package sim

import (
	"fmt"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// ConsolidationGenerator produces proposals that remove a node. It is the
// single-node path's move generator, upstream's computeConsolidation for one
// candidate, and SingleNodeEvaluation is what calls it. Emptiness and multi-node
// are separate evaluations in paths.go.
//
// A move deletes the source and places its pods somewhere. Savings is the
// source's full price, because a destination that already exists keeps costing
// what it cost. Disruption is the eviction cost of the source's pods, because
// those are the pods that restart. One score for the whole move.
type ConsolidationGenerator struct {
	// Threshold is 1/k from balanced consolidation. A move is approved at or
	// above it.
	Threshold float64
}

func (g ConsolidationGenerator) Name() string { return "consolidation" }

// Generate returns proposals for removing one node.
//
// dests is the eligible destination set, computed once per pass by
// EligibleDestinations and passed in. The generator takes no cluster handle, so
// it cannot widen that set or consult anything outside its arguments. dests is a
// snapshot, so two proposals can plan against the same slack. Stage three
// resolves that between proposals, and the PodsStillFit precondition catches
// what survives to apply time.
func (g ConsolidationGenerator) Generate(n NodeSnapshot, dests []NodeSnapshot, cat *Catalog, totals PoolTotals) []Proposal {
	// A node holding a do-not-disrupt pod cannot be drained, so there is no move
	// to generate. Stage 1 already filters these; repeating the test here keeps
	// the generator correct for a caller that invokes it directly.
	if !Disruptable(n) {
		return nil
	}

	// An empty node belongs to the Emptiness path, which never scores it and
	// charges a different budget. Upstream routes it away here, in
	// consolidation.ShouldDisrupt: "Empty nodes are handled by Emptiness (reason
	// \"Empty\") for correct budget accounting." Emitting a scored empty delete
	// from this generator, which is what it used to do, put empty nodes in
	// competition with underutilized ones for one budget.
	if IsEffectivelyEmpty(n.DisruptionCost) {
		return nil
	}

	// The delete ratio bounds every single-node move from this node. Below the
	// threshold, no move from here can pass, so skip generation. This is
	// upstream's CanPassThreshold, which lives in the single-node loop and has no
	// multi-node counterpart, because a node that cannot pass alone can still
	// ride in a batch that does.
	if DeleteRatio(n, totals) < g.Threshold {
		return nil
	}

	var out []Proposal

	// One move per candidate, delete preferred.
	//
	// A delete recovers the node's full price and a replace only the difference,
	// and both divide by the same disruption cost, so whenever both are available
	// the delete scores higher. Emitting both was dead work: 3031 pairs across 75
	// runs, every replace generated, ranked, and discarded. It also inflated the
	// conflict count with contention that was structurally certain rather than
	// real, and it meant the replace-without-delete case never occurred, which
	// hid a defect in PodsStillFit for as long as the code existed.
	//
	// Upstream orders it the same way. computeConsolidation returns a delete when
	// no new NodeClaims are needed and only then falls through to replacement.
	if p, ok := g.deleteOnto(n, dests, cat, totals); ok {
		return append(out, p)
	}

	// Replace: buy the cheapest node that holds these pods. This is the only
	// cost-saving move on a tightly packed cluster, where the pods have nowhere
	// else to go.
	if p, ok := g.replaceWith(n, cat, totals); ok {
		out = append(out, p)
	}
	return out
}

// deleteOnto proposes removing the source and scattering its pods onto dests.
// It checks only that the pods fit in aggregate. Which pod lands where is the
// scheduler's decision, and predicting it is what the precondition avoids.
func (g ConsolidationGenerator) deleteOnto(n NodeSnapshot, dests []NodeSnapshot, cat *Catalog, totals PoolTotals) (Proposal, bool) {
	required := Requests(n.ReschedulablePods)
	if !aggregateRoom(dests, cat, required) {
		return Proposal{}, false
	}
	score := Score(n.Price, n.DisruptionCost, totals)
	if score < g.Threshold {
		return Proposal{}, false
	}

	actions := []Action{{Kind: Taint, ProviderID: n.ProviderID}}
	for range n.ReschedulablePods {
		actions = append(actions, Action{Kind: Evict, ProviderID: n.ProviderID, DependsOn: []int{0}})
	}
	actions = append(actions, Action{
		Kind:       Delete,
		ProviderID: n.ProviderID,
		DependsOn:  dependsOnAll(1, len(n.ReschedulablePods)),
	})

	return Proposal{
		ID:            fmt.Sprintf("del/%s", n.ProviderID),
		Candidates:    []NodeSnapshot{n},
		Savings:       n.Price,
		Score:         score,
		Actions:       actions,
		Preconditions: consolidationPreconditions(n),
		Reason:        ReasonUnderutilized,
	}, true
}

// replaceWith proposes swapping the source for the cheapest instance type that
// holds its pods. The launch must confirm Ready before any eviction fires, which
// is the ordering guarantee the log exists to enforce.
func (g ConsolidationGenerator) replaceWith(n NodeSnapshot, cat *Catalog, totals PoolTotals) (Proposal, bool) {
	required := Requests(n.ReschedulablePods)
	it, price, ok := cat.Cheapest(cat.Fits(required))
	if !ok || price >= n.Price {
		return Proposal{}, false
	}
	savings := n.Price - price
	score := Score(savings, n.DisruptionCost, totals)
	if score < g.Threshold {
		return Proposal{}, false
	}

	actions := []Action{
		{Kind: Taint, ProviderID: n.ProviderID},
		{Kind: Launch, InstanceType: it},
	}
	for range n.ReschedulablePods {
		actions = append(actions, Action{Kind: Evict, ProviderID: n.ProviderID, DependsOn: []int{1}})
	}
	actions = append(actions, Action{
		Kind:       Delete,
		ProviderID: n.ProviderID,
		DependsOn:  dependsOnAll(2, len(n.ReschedulablePods)),
	})

	return Proposal{
		ID:            fmt.Sprintf("rep/%s/%s", n.ProviderID, it.Name),
		Candidates:    []NodeSnapshot{n},
		Replacements:  []*cloudprovider.InstanceType{it},
		Savings:       savings,
		Score:         score,
		Actions:       actions,
		Preconditions: consolidationPreconditions(n),
		Reason:        ReasonUnderutilized,
	}, true
}

// EligibleDestinations filters nodes that may receive evicted pods.
//
// Two exclusions matter. A node that is itself a consolidation candidate cannot
// be a destination, because we are proposing to drain it. And an empty node is
// excluded unless the arriving pods bring it above the utilization threshold,
// because otherwise the move relocates waste instead of removing it. The score
// would rate such a move well, since savings is the full source price and the
// destination's emptiness never enters the arithmetic. This filter is what
// closes that gap.
//
// Every test here reads one node plus the catalog, so eligibility is decided per
// node and the set can be built in parallel.
func EligibleDestinations(all []NodeSnapshot, cat *Catalog, totals PoolTotals, cfg BandConfig) []NodeSnapshot {
	var out []NodeSnapshot
	for _, n := range all {
		switch cfg.Classify(n, cat, totals) {
		case Destination, Full:
			// Full nodes stay in the set. Whether a specific pod fits is the
			// feasibility check, which flips, so it is gated at apply time
			// rather than estimated here.
			out = append(out, n)
		}
	}
	return out
}

// Band is where a node sits on the utilization range.
type Band int

const (
	// Underutilized: low enough to be a consolidation source. Empty nodes and
	// non-empty candidates both land here.
	Underutilized Band = iota
	// Ambiguous: the middle, where classification could go either way. Neither
	// a source nor a destination.
	Ambiguous
	// Destination: utilized enough that adding pods does not relocate waste.
	Destination
	// Full: no room for another pod. This is a feasibility fact rather than a
	// threshold, which is why it is a separate band.
	Full
)

// BandConfig holds the two thresholds separating the bands. They are estimated
// from observed cluster behavior rather than fixed. The error signal is
// observable: place a pod on a node, then consolidate that same node shortly
// after, and the destination was misclassified. A threshold set too low produces
// exactly that sequence.
//
// The estimation loop is unmeasured and may not damp. The thresholds decide
// destinations, destinations shape utilization, and utilization is what the
// thresholds are estimated from.
type BandConfig struct {
	// SourceCeiling is the top of Underutilized.
	SourceCeiling float64
	// DestinationFloor is the bottom of Destination.
	DestinationFloor float64
	// FullCeiling is where no further pod fits in practice.
	FullCeiling float64
}

func (c BandConfig) Classify(n NodeSnapshot, cat *Catalog, totals PoolTotals) Band {
	u := Utilization(n, cat)
	switch {
	case u >= c.FullCeiling:
		return Full
	case u >= c.DestinationFloor:
		return Destination
	case u > c.SourceCeiling:
		return Ambiguous
	default:
		return Underutilized
	}
}

// consolidationPreconditions is what the proposal declares stage 4 must check.
//
// A relational constraint is declared only when a pod on one of these nodes
// carries it. Declaring all four on every proposal would charge every move for
// constraints nobody in it has, and the read counts are what the measurement is
// about.
//
// It takes a batch rather than one node, because multi-node proposals remove
// several. A batch declares the union of its members' constraints: one pod
// carrying topology spread makes the whole batch's removal depend on the
// distribution, and checking a proper subset would approve a move nobody checked.
//
// Every test here reads the nodes' own pods, which is what Disruptable already
// does, so stage 2 stays local. Deciding a constraint takes the cluster; noticing
// that a pod carries one does not.
func consolidationPreconditions(batch ...NodeSnapshot) []PreconditionKind {
	// ScoreStillClears is on every consolidation proposal, DELETE and REPLACE
	// alike, because both are approved by the same comparison and either can decay
	// past it between generation and apply. A provisioning proposal carries no
	// score, so it does not declare this, and neither does an empty delete, which
	// was never gated on a threshold.
	pre := []PreconditionKind{ScoreStillClears, PodsStillFit, NoDoNotDisrupt, BudgetHasRoom, NotNominated}
	pods := 0
	for _, n := range batch {
		pods += len(n.ReschedulablePods)
	}
	if pods > 0 {
		pre = append(pre, PDBAllows)
	}
	for _, kind := range []struct {
		kind    PreconditionKind
		applies func(*corev1.Pod) bool
	}{
		{AntiAffinityHolds, AntiAffinityConstraint{}.Applies},
		{AffinityHolds, AffinityConstraint{}.Applies},
		{TopologyHolds, TopologyConstraint{}.Applies},
	} {
		if anyPodCarries(batch, kind.applies) {
			pre = append(pre, kind.kind)
		}
	}
	return pre
}

func anyPodCarries(batch []NodeSnapshot, applies func(*corev1.Pod) bool) bool {
	for _, n := range batch {
		for _, p := range n.ReschedulablePods {
			if applies(p) {
				return true
			}
		}
	}
	return false
}

// aggregateRoom reports whether the destinations have room for required in
// total. It sums free capacity across dests and compares.
//
// This is a necessary condition and not a sufficient one. Aggregate room does not
// mean the pods can actually be assigned, since a single large pod may not fit on
// any one node even when the total is ample. Treating it as sufficient is what
// makes the generator cheap and what makes PodsStillFit a precondition rather
// than a guarantee.
func aggregateRoom(dests []NodeSnapshot, cat *Catalog, required corev1.ResourceList) bool {
	free := corev1.ResourceList{}
	for _, d := range dests {
		if d.InstanceType == nil {
			continue
		}
		eff := cat.Effective(d.InstanceType)
		used := Requests(d.ReschedulablePods)
		for name, have := range eff {
			remaining := have.DeepCopy()
			if u, ok := used[name]; ok {
				remaining.Sub(u)
			}
			if remaining.Sign() < 0 {
				continue
			}
			if cur, ok := free[name]; ok {
				cur.Add(remaining)
				free[name] = cur
			} else {
				free[name] = remaining
			}
		}
	}
	for name, want := range required {
		have, ok := free[name]
		if !ok || have.Cmp(want) < 0 {
			return false
		}
	}
	return true
}

func dependsOnAll(start, count int) []int {
	out := make([]int, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, start+i)
	}
	return out
}
