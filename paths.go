package sim

import (
	"math"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// Upstream runs three distinct disruption methods over the same cluster, and the
// interaction between them decides what happens. NewMethods lists them in this
// order, and Reconcile stops at the first one that acts:
//
//	Emptiness            reason Empty, never scores
//	StaticDrift, Drift   not modeled here
//	MultiNodeConsolidation   reason Underutilized, binary search over a batch
//	SingleNodeConsolidation  reason Underutilized, the fallback
//
// The three were one function here, a single ConsolidationGenerator.Generate that
// tried a delete and then a replace, with emptiness as a branch inside it. That
// collapsed two boundaries that upstream keeps apart. Emptiness has its own
// reason and therefore its own budget, and it does not score, so an empty node
// that a score would reject still gets deleted. And multi-node runs first, so a
// node upstream removes as part of a batch is a node this simulator would have
// evaluated alone or not at all.
//
// Every function in this file takes snapshots, a destination set, a catalog and
// pool totals. None takes a cluster handle, a context, or a client. That is the
// stage boundary from proposal.go, and multi-node did not need it broken: the
// binary search probes batches of snapshots against the destination set it was
// handed. What it does need that the single-node path did not is a packing test
// rather than a sum of free capacity, because a batch's pods against a batch's
// worth of holes passes the sum test almost always. See batchFits.

// Path names one of the three evaluations.
type Path int

const (
	// PathEmpty is upstream's Emptiness. It never scores.
	PathEmpty Path = iota
	// PathMultiNode is upstream's MultiNodeConsolidation, attempted before
	// single-node.
	PathMultiNode
	// PathSingleNode is upstream's SingleNodeConsolidation, the fallback.
	PathSingleNode
)

func (p Path) String() string {
	switch p {
	case PathEmpty:
		return "empty"
	case PathMultiNode:
		return "multi"
	case PathSingleNode:
		return "single"
	}
	return "unknown"
}

// MultiNodeMaxParallel is upstream's clamp, "Only consider a maximum batch of 100
// NodeClaims to save on computation. This could be further configurable in the
// future."
//
// The batch it actually produces can be 101. maxParallel clamps to 100, then
// firstNConsolidationOption narrows max only when `len(candidates) <= max`, which
// is false above 100 candidates, so max stays 100, mid reaches 100, and the batch
// is candidates[0 : 101]. Reproduced rather than corrected, and pinned in
// TestBinarySearchWindow.
const MultiNodeMaxParallel = 100

// SavingsRatio is price per unit of disruption, unnormalized. This is what
// upstream sorts candidates by, in consolidation.sortCandidates:
//
//	func (c *Candidate) SavingsRatio() float64 { return c.Price / c.RescheduleDisruptionCost }
//
// Two things about it are load-bearing and neither is obvious.
//
// It is unnormalized. Score divides both terms by the pool totals, so it answers
// "what share of the pool's cost does this recover per share of its disruption."
// SavingsRatio answers "how many dollars per unit of disruption." Within one pool
// the two order candidates identically, because the pool totals are a constant
// factor. Across pools they do not, and upstream sorts one global list.
//
// It is always the node's full price, whatever move eventually follows. A REPLACE
// recovers only the price difference, so ranking proposals by Score puts a
// replace far below where ranking candidates by SavingsRatio puts its node. On
// the tight fixture the oversized node's full price is $0.5292 and its replace
// saves $0.1724, a factor of 3.07. Upstream ranks first and decides the move
// second, so it never sees that gap; RankingCompactor ranks the move, so it does.
// See TestSavingsRatioAndScoreDisagree.
func SavingsRatio(n NodeSnapshot) float64 {
	if n.DisruptionCost == 0 {
		return 0
	}
	return n.Price / n.DisruptionCost
}

// sortBySavingsRatio orders candidates by SavingsRatio descending, upstream's
// consolidation.sortCandidates.
//
// Upstream's comment says why the sort matters to the binary search: "The binary
// search in multi-node consolidation tries the first N candidates as a batch.
// Ratio sort means the batch contains the highest-value nodes, so budget-limited
// cycles execute the most impactful moves first."
//
// Stable, where upstream's sort.Slice is not. Ties are common (every node of one
// instance type carrying the same pod count ties exactly), and an unstable sort
// would let the ties decide the batch differently between runs of the same seed.
func sortBySavingsRatio(candidates []NodeSnapshot) []NodeSnapshot {
	out := make([]NodeSnapshot, len(candidates))
	copy(out, candidates)
	sort.SliceStable(out, func(i, j int) bool {
		if r1, r2 := SavingsRatio(out[i]), SavingsRatio(out[j]); r1 != r2 {
			return r1 > r2
		}
		// The provider ID is the only remaining discriminator, and it is fixed by
		// the fixture rather than drawn, so a seeded run reproduces.
		return out[i].ProviderID < out[j].ProviderID
	})
	return out
}

// poolName is NodePool.Name, tolerating a snapshot built without one. Hand-built
// fixtures leave NodePool nil, and a budget map keyed by a nil dereference is a
// panic in a helper nobody was thinking about.
func poolName(n NodeSnapshot) string {
	if n.NodePool == nil {
		return ""
	}
	return n.NodePool.Name
}

// poolBudgets seeds one allowance per NodePool present in nodes.
//
// Upstream's BuildDisruptionBudgetMapping runs inside disrupt(), once per method,
// so each method gets a fresh map keyed by NodePool name and its own reason's
// allowance. Two consolidation methods therefore never share a decremented
// counter, which is consistent upstream because only one of them acts per
// reconcile. This simulator collapses those reconciles into one pass, so the
// fresh-per-path map is kept for fidelity and the compactor's per-reason
// accounting is what actually binds. See selectWithBudgets.
func poolBudgets(nodes []NodeSnapshot, budget int) map[string]int {
	out := map[string]int{}
	for _, n := range nodes {
		out[poolName(n)] = budget
	}
	return out
}

// -----------------------------------------------------------------------------
// Path 1: Emptiness
// -----------------------------------------------------------------------------

// EmptinessEvaluation is upstream's Emptiness, reason Empty.
//
// It carries no threshold, and that is the point. consolidation.ShouldDisrupt
// routes empty nodes away with the comment "Empty nodes are handled by Emptiness
// (reason \"Empty\") for correct budget accounting," and Emptiness never calls
// the evaluator. So an empty node whose score would fail is deleted anyway,
// against a separate budget. Scoring empty nodes alongside underutilized ones,
// which is what this simulator did, lets a cheap empty delete take a slot an
// expensive underutilized move needed.
type EmptinessEvaluation struct{}

func (EmptinessEvaluation) Path() Path     { return PathEmpty }
func (EmptinessEvaluation) Reason() Reason { return ReasonEmpty }

// ShouldDisrupt is upstream's Emptiness.ShouldDisrupt reduced to what a snapshot
// can answer: the node is empty.
//
// Emptiness is disruption cost carrying only the per-node baseline, upstream's
// IsEmpty, and not a pod count. A node holding one pod whose eviction cost clamps
// to zero is empty under that definition.
//
// Three of upstream's tests are absent because they need the cluster: the
// Consolidatable status condition, ConsolidateAfter being set, and
// cluster.HasBufferPods. Disruptable, which is upstream's ValidateNodeDisruptable
// and applies to every method, is applied by the caller.
func (EmptinessEvaluation) ShouldDisrupt(n NodeSnapshot, _ PoolTotals, _ float64) bool {
	return IsEffectivelyEmpty(n.DisruptionCost)
}

// Evaluate returns at most one proposal, batching every empty node that the
// budget allows.
//
// One proposal for the whole batch is upstream's shape: Emptiness.ComputeCommands
// builds a single Command whose Candidates are all the empty nodes it found. The
// consequence travels with it. The batch commits or it does not, so one node in it
// failing a precondition stops the deletion of all of them.
func (e EmptinessEvaluation) Evaluate(candidates, _ []NodeSnapshot, _ *Catalog, _ PoolTotals, budgets map[string]int) []Proposal {
	// Sorted even though nothing here scores, because upstream sorts before the
	// budget filter and the budget filter is what decides which nodes make the
	// batch when the allowance is smaller than the number of empty nodes.
	candidates = sortBySavingsRatio(candidates)

	var empty []NodeSnapshot
	for _, n := range candidates {
		if !IsEffectivelyEmpty(n.DisruptionCost) {
			continue
		}
		if budgets[poolName(n)] == 0 {
			continue
		}
		empty = append(empty, n)
		budgets[poolName(n)]--
	}
	if len(empty) == 0 {
		return nil
	}

	// Taint every node, then delete each behind its own taint. No Evict actions:
	// there is nothing whose restart the plan has to wait for, which is what
	// emptiness means.
	var actions []Action
	for _, n := range empty {
		actions = append(actions, Action{Kind: Taint, ProviderID: n.ProviderID})
	}
	for i, n := range empty {
		actions = append(actions, Action{Kind: Delete, ProviderID: n.ProviderID, DependsOn: []int{i}})
	}

	savings := 0.0
	for _, n := range empty {
		savings += n.Price
	}

	return []Proposal{{
		ID:         emptyBatchID(empty),
		Candidates: empty,
		Savings:    savings,
		// No Score. Nothing on this path was approved by a comparison, so
		// carrying a number that a threshold could be applied to would invite
		// exactly the gate upstream removed.
		Actions: actions,
		// No ScoreStillClears for the same reason, and no PodsStillFit because
		// there are no pods to fit. A node whose pods came back is caught by
		// NoDoNotDisrupt on the next pass, where it is no longer empty and takes
		// a consolidation path.
		Preconditions: []PreconditionKind{NoDoNotDisrupt, BudgetHasRoom, NotNominated},
		Reason:        ReasonEmpty,
	}}
}

// emptyBatchID names the batch by its members. Content-derived, because the
// compactor's tie-break is the ID and a generated one would make a seeded run
// unreproducible.
func emptyBatchID(batch []NodeSnapshot) string {
	id := "del-empty"
	for _, n := range batch {
		id += "/" + n.ProviderID
	}
	return id
}

// -----------------------------------------------------------------------------
// Path 2: multi-node consolidation
// -----------------------------------------------------------------------------

// MultiNodeEvaluation is upstream's MultiNodeConsolidation. It is attempted
// before single-node, and single-node is the fallback when it finds nothing.
type MultiNodeEvaluation struct {
	// Threshold is 1/k. Upstream applies it through evaluator.ApproveCommand on
	// each probe of the binary search.
	Threshold float64
}

func (MultiNodeEvaluation) Path() Path     { return PathMultiNode }
func (MultiNodeEvaluation) Reason() Reason { return ReasonUnderutilized }

// ShouldDisrupt is upstream's consolidation.ShouldDisrupt: not empty.
//
// Note what is absent. Upstream's ShouldDisrupt carries no score test at all. The
// delete-ratio prune lives in single-node's loop, as CanPassThreshold, and
// multi-node has no equivalent, because a node that cannot pass alone can still
// ride in a batch that does. Identify applies that prune to every node, so a
// simulator that fed Identify's output to multi-node would deny it candidates
// upstream gives it.
func (MultiNodeEvaluation) ShouldDisrupt(n NodeSnapshot, _ PoolTotals, _ float64) bool {
	return !IsEffectivelyEmpty(n.DisruptionCost)
}

// Evaluate returns at most one proposal, the batch the binary search settled on.
func (m MultiNodeEvaluation) Evaluate(candidates, dests []NodeSnapshot, cat *Catalog, totals PoolTotals, budgets map[string]int) []Proposal {
	p, _, ok := m.EvaluateCounting(candidates, dests, cat, totals, budgets)
	if !ok {
		return nil
	}
	return []Proposal{p}
}

// EvaluateCounting is Evaluate with the search's probe count returned, which is
// the number a fidelity claim about the binary search is made of.
func (m MultiNodeEvaluation) EvaluateCounting(candidates, dests []NodeSnapshot, cat *Catalog, totals PoolTotals, budgets map[string]int) (Proposal, int, bool) {
	candidates = sortBySavingsRatio(candidates)

	// Upstream pre-filters by budget rather than checking inside the search:
	// "since multi-node consolidation relies on the ordering of these candidates,
	// and does computation in batches of these nodes by simulateScheduling(nodes[0,
	// n]), doing a binary search on n to find the optimal consolidation command,
	// this pre-filters out nodes that would have violated the budget anyway,
	// preserving the ordering and only considering a number of nodes that can be
	// disrupted."
	disruptable := make([]NodeSnapshot, 0, len(candidates))
	for _, n := range candidates {
		if budgets[poolName(n)] == 0 {
			continue
		}
		disruptable = append(disruptable, n)
		budgets[poolName(n)]--
	}

	maxParallel := len(disruptable)
	if maxParallel > MultiNodeMaxParallel {
		maxParallel = MultiNodeMaxParallel
	}
	return m.firstNConsolidationOption(disruptable, dests, cat, totals, maxParallel)
}

// firstNConsolidationOption is upstream's binary search, reproduced.
//
// The structure is upstream's, including the two things about it that are easy to
// get wrong. The window is [min, max] over an index, and the batch probed is
// candidates[0 : mid+1], so a probe at mid tests a batch of mid+1 nodes and the
// smallest batch ever probed is two. A valid probe moves min up and keeps the
// command; an invalid probe moves max down. The last valid command is returned,
// which is what upstream means by "we can consolidate NodeClaims [0,mid]".
//
// The assumption underneath it is monotonicity in the count: if removing N nodes
// is feasible then removing N-1 is too, so the predicate is a step function the
// search can bisect. Upstream states the weaker version of this in a comment on
// sortCandidates, that "the binary search still converges because it shrinks the
// window until scheduling succeeds," which is true of the feasibility term and
// not of the approval term.
//
// The assumption holds for a DELETE and fails for a REPLACE, and the reason is in
// the arithmetic rather than in the code. A delete-only batch's score is the
// batch's aggregate savings ratio scaled by a constant, and the aggregate ratio
// of a prefix of a list sorted by individual ratio is non-increasing in the prefix
// length, by the mediant inequality. So if a prefix of length N clears the
// threshold, every shorter prefix does. A replace subtracts one replacement price
// from the whole batch's savings, and that price is shared: it is amortized over
// however many nodes the batch removes. Dropping a node therefore moves the
// numerator and the denominator independently, the prefix ratios stop being
// ordered, and a batch of three can clear a threshold a batch of two fails. The
// search then probes two, fails, shrinks the window away from three, and returns
// nothing. TestMultiNodeMonotonicityViolation constructs exactly that.
func (m MultiNodeEvaluation) firstNConsolidationOption(candidates, dests []NodeSnapshot, cat *Catalog, totals PoolTotals, max int) (Proposal, int, bool) {
	// "we always operate on at least two NodeClaims at once, for single
	// NodeClaims standard consolidation will find all solutions"
	if len(candidates) < 2 {
		return Proposal{}, 0, false
	}
	min := 1
	if len(candidates) <= max {
		max = len(candidates) - 1
	}

	var lastSaved Proposal
	found := false
	probes := 0
	for min <= max {
		mid := (min + max) / 2
		batch := candidates[0 : mid+1]
		probes++

		p, valid := computeBatchConsolidation(batch, dests, cat, totals, m.Threshold)
		if valid {
			// We can consolidate candidates [0,mid].
			lastSaved, found = p, true
			min = mid + 1
		} else {
			max = mid - 1
		}
	}
	return lastSaved, probes, found
}

// ExhaustiveOption scans every prefix from two up to max and returns the largest
// valid one. It is not upstream's algorithm and is not used by a run.
//
// It exists to make the monotonicity assumption testable. The binary search and
// the scan agree exactly when the predicate is monotone in the count, so a
// disagreement between them is a violation, located and sized rather than argued
// for.
func (m MultiNodeEvaluation) ExhaustiveOption(candidates, dests []NodeSnapshot, cat *Catalog, totals PoolTotals) (Proposal, bool) {
	var best Proposal
	found := false
	for n := 2; n <= len(candidates); n++ {
		if p, valid := computeBatchConsolidation(candidates[0:n], dests, cat, totals, m.Threshold); valid {
			best, found = p, true
		}
	}
	return best, found
}

// computeBatchConsolidation is upstream's consolidation.computeConsolidation for
// a batch of candidates, minus the scheduling simulation it cannot run here.
//
// The order of the tests is upstream's. Delete first: "were we able to schedule
// all the pods on the inflight candidates?" returns a Command with no
// Replacements. Only if that fails does it price a replacement, and the
// replacement must be strictly cheaper than the batch. Then the score decides,
// which is upstream's evaluator.ApproveCommand.
func computeBatchConsolidation(batch, dests []NodeSnapshot, cat *Catalog, totals PoolTotals, threshold float64) (Proposal, bool) {
	if len(batch) == 0 {
		return Proposal{}, false
	}

	var pods []*corev1.Pod
	sumPrice := 0.0
	for _, c := range batch {
		pods = append(pods, c.ReschedulablePods...)
		sumPrice += c.Price
	}
	// Priced over the batch's whole evicted set rather than summed per node, so a
	// gang with members on two of these nodes is charged once instead of twice.
	// Summing c.DisruptionCost, which is what this did, charged it per node and
	// made a batch that takes a gang whole look dearer than one that breaks it and
	// leaves the rest running. With no gangs in the batch the two are equal, term
	// for term, apart from the baseline both count once per node.
	sumDisruption := MoveDisruptionCost(len(batch), pods)

	// A node in the batch is being drained, so it cannot receive the batch's own
	// pods. withoutNode per member rather than a set test, because dests is short
	// and the batch shorter.
	room := dests
	for _, c := range batch {
		room = withoutNode(room, c.ProviderID)
	}

	if batchFits(pods, room, cat) {
		score := Score(sumPrice, sumDisruption, totals)
		if score < threshold {
			return Proposal{}, false
		}
		return Proposal{
			ID:            batchID("multi-del", batch, ""),
			Candidates:    batch,
			Savings:       sumPrice,
			Score:         score,
			Actions:       batchActions(batch, nil),
			Preconditions: consolidationPreconditions(batch...),
			Reason:        ReasonUnderutilized,
		}, true
	}

	it, price, ok := batchReplacement(batch, pods, sumPrice, cat)
	if !ok {
		return Proposal{}, false
	}
	savings := sumPrice - price
	score := Score(savings, sumDisruption, totals)
	if score < threshold {
		return Proposal{}, false
	}
	return Proposal{
		ID:            batchID("multi-rep", batch, it.Name),
		Candidates:    batch,
		Replacements:  []*cloudprovider.InstanceType{it},
		Savings:       savings,
		Score:         score,
		Actions:       batchActions(batch, it),
		Preconditions: consolidationPreconditions(batch...),
		Reason:        ReasonUnderutilized,
	}, true
}

// batchFits is first-fit-decreasing of the batch's pods over the destinations'
// free capacity. Aggregate room is a necessary condition and not a sufficient
// one, and the gap widens with the batch: N nodes' pods against N nodes' worth of
// holes passes the sum test almost always and the packing test rarely.
//
// The single-node path deliberately uses the sum test, and leaves the difference
// to the PodsStillFit precondition. Multi-node cannot afford that. The binary
// search would find a batch of forty feasible on the sum test, commit it, and stop
// on the first pod, so the search would report a batch size that never happens.
// The cost is that this is stage 2 doing more arithmetic; it is still local, since
// dests came in as an argument.
func batchFits(pods []*corev1.Pod, dests []NodeSnapshot, cat *Catalog) bool {
	if len(pods) == 0 {
		return true
	}
	free := make([]corev1.ResourceList, 0, len(dests))
	for _, d := range dests {
		if d.InstanceType == nil {
			continue
		}
		eff := cat.Effective(d.InstanceType)
		used := Requests(d.ReschedulablePods)
		remaining := corev1.ResourceList{}
		for name, have := range eff {
			q := have.DeepCopy()
			if u, ok := used[name]; ok {
				q.Sub(u)
			}
			if q.Sign() < 0 {
				continue
			}
			remaining[name] = q
		}
		slots := remaining[corev1.ResourcePods]
		slots.Sub(*resource.NewQuantity(int64(len(d.ReschedulablePods)), resource.DecimalSI))
		remaining[corev1.ResourcePods] = slots
		free = append(free, remaining)
	}

	// Largest first. First-fit over an arbitrary order can strand a large pod that
	// a descending order would have placed, which would make the search's verdict
	// depend on the pod ordering the fixture happened to produce.
	order := make([]*corev1.Pod, len(pods))
	copy(order, pods)
	sort.SliceStable(order, func(i, j int) bool { return footprint(order[i]) > footprint(order[j]) })

	for _, p := range order {
		need := podNeeds(p)
		placed := false
		for _, f := range free {
			if !fitsInto(f, need) {
				continue
			}
			for name, want := range need {
				have := f[name]
				have.Sub(want)
				f[name] = have
			}
			placed = true
			break
		}
		if !placed {
			return false
		}
	}
	return true
}

// batchReplacement picks the single instance type that replaces a whole batch.
//
// Two filters, both upstream's. RemoveInstanceTypeOptionsByPriceAndMinValues
// keeps only options strictly cheaper than the batch's total price, since
// replacing N nodes with something costing more than all of them saves nothing.
// filterOutSameInstanceType is the second and it is specific to multi-node: if an
// option is the same type as a node the batch is removing, cap the price at that
// node's own price, because "this is equivalent to just deleting the [other]
// NodeClaims" and the deletion is the cheaper disruption.
//
// The pod count enters the requirement here, where the single-node path leaves it
// out. A single node's pods already fit inside one node's pod limit; a batch's
// pods need not, and a replacement chosen without the limit would be a node that
// cannot hold what it was bought for.
func batchReplacement(batch []NodeSnapshot, pods []*corev1.Pod, sumPrice float64, cat *Catalog) (*cloudprovider.InstanceType, float64, bool) {
	required := withPodSlots(Requests(pods), int64(len(pods)))
	fits := cat.Fits(required)

	// The cheapest price at which the batch already owns each instance type.
	ownPrice := map[string]float64{}
	for _, c := range batch {
		if c.InstanceType == nil {
			continue
		}
		if p, seen := ownPrice[c.InstanceType.Name]; !seen || c.Price < p {
			ownPrice[c.InstanceType.Name] = c.Price
		}
	}

	// filterOutSameInstanceType's ceiling, taken over the options rather than
	// over the batch: only a type that is both an option and already in the batch
	// constrains anything.
	ceiling := math.MaxFloat64
	for _, it := range cat.InstanceTypes {
		if !fits(it) {
			continue
		}
		if p, owned := ownPrice[it.Name]; owned && p < ceiling {
			ceiling = p
		}
	}
	maxPrice := sumPrice
	if ceiling < maxPrice {
		maxPrice = ceiling
	}

	var best *cloudprovider.InstanceType
	bestPrice := 0.0
	for _, it := range cat.InstanceTypes {
		if !fits(it) {
			continue
		}
		p, available := cheapestOffering(it)
		if !available || p >= maxPrice {
			continue
		}
		if best == nil || p < bestPrice {
			best, bestPrice = it, p
		}
	}
	return best, bestPrice, best != nil
}

// batchID is derived from the batch's members and the replacement type, for the
// same reason emptyBatchID is.
func batchID(prefix string, batch []NodeSnapshot, replacement string) string {
	id := prefix
	for _, n := range batch {
		id += "/" + n.ProviderID
	}
	if replacement != "" {
		id += "->" + replacement
	}
	return id
}

// batchActions builds the DAG for removing a batch, with one shared replacement
// or none.
//
// The ordering guarantee is the same one the single-node replace exists to
// enforce, applied across the batch: no pod leaves any candidate until the
// replacement confirms Ready. A delete waits on its own node's evictions and not
// on the other candidates', because a node with an empty pod list is safe to
// remove whatever is still running elsewhere.
func batchActions(batch []NodeSnapshot, replacement *cloudprovider.InstanceType) []Action {
	var actions []Action
	taintAt := make([]int, len(batch))
	for i, c := range batch {
		taintAt[i] = len(actions)
		actions = append(actions, Action{Kind: Taint, ProviderID: c.ProviderID})
	}

	launchAt := -1
	if replacement != nil {
		launchAt = len(actions)
		actions = append(actions, Action{Kind: Launch, InstanceType: replacement})
	}

	evictAt := make([][]int, len(batch))
	for i, c := range batch {
		gate := taintAt[i]
		if launchAt >= 0 {
			gate = launchAt
		}
		for range c.ReschedulablePods {
			evictAt[i] = append(evictAt[i], len(actions))
			actions = append(actions, Action{Kind: Evict, ProviderID: c.ProviderID, DependsOn: []int{gate}})
		}
	}

	for i, c := range batch {
		deps := evictAt[i]
		if len(deps) == 0 {
			// Nothing to evict, so the taint is the only thing to wait for.
			deps = []int{taintAt[i]}
		}
		actions = append(actions, Action{Kind: Delete, ProviderID: c.ProviderID, DependsOn: deps})
	}
	return actions
}

// -----------------------------------------------------------------------------
// Path 3: single-node consolidation
// -----------------------------------------------------------------------------

// SingleNodeEvaluation is upstream's SingleNodeConsolidation, the fallback when
// multi-node finds nothing.
type SingleNodeEvaluation struct {
	Threshold float64

	// MaxProposals caps how many candidates yield a proposal. Upstream returns
	// after the first valid command, because Reconcile requeues immediately and
	// the next reconcile starts the walk again. This simulator's pass is one
	// reconcile that commits up to the budget, so a cap of one would collapse a
	// pass to a single move and every measurement built on it. Zero means no cap,
	// which is what a pass uses; one reproduces upstream's per-reconcile
	// behavior, and TestSingleNodeReturnsFirstValidCommand pins it.
	MaxProposals int
}

func (SingleNodeEvaluation) Path() Path     { return PathSingleNode }
func (SingleNodeEvaluation) Reason() Reason { return ReasonUnderutilized }

// ShouldDisrupt is upstream's consolidation.ShouldDisrupt, the same predicate
// multi-node uses. The delete-ratio prune is applied inside Evaluate, where
// upstream applies it.
func (SingleNodeEvaluation) ShouldDisrupt(n NodeSnapshot, _ PoolTotals, _ float64) bool {
	return !IsEffectivelyEmpty(n.DisruptionCost)
}

// SortCandidates is upstream's SingleNodeConsolidation.SortCandidates: the ratio
// sort, then interweaved by NodePool.
//
// The interweave is what stops one large pool from starving the others when the
// walk is cut short by a timeout. Upstream orders the pools with lo.Keys over a
// map, which is randomized; this sorts the names, because a simulator whose
// candidate order changes between runs of one seed cannot report a number.
func (SingleNodeEvaluation) SortCandidates(candidates []NodeSnapshot) []NodeSnapshot {
	sorted := sortBySavingsRatio(candidates)

	byPool := map[string][]NodeSnapshot{}
	var pools []string
	for _, n := range sorted {
		p := poolName(n)
		if _, seen := byPool[p]; !seen {
			pools = append(pools, p)
		}
		byPool[p] = append(byPool[p], n)
	}
	sort.Strings(pools)

	deepest := 0
	for _, p := range pools {
		if len(byPool[p]) > deepest {
			deepest = len(byPool[p])
		}
	}

	out := make([]NodeSnapshot, 0, len(sorted))
	for i := 0; i < deepest; i++ {
		for _, p := range pools {
			if i < len(byPool[p]) {
				out = append(out, byPool[p][i])
			}
		}
	}
	return out
}

// Evaluate walks candidates in sorted order and returns a proposal per candidate
// that yields one, capped by MaxProposals.
//
// The budget is checked and not decremented, matching upstream's comment: "We
// don't need to decrement any budget counter since single node consolidation
// commands can only have one candidate."
func (s SingleNodeEvaluation) Evaluate(candidates, dests []NodeSnapshot, cat *Catalog, totals PoolTotals, budgets map[string]int) []Proposal {
	gen := ConsolidationGenerator{Threshold: s.Threshold}
	var out []Proposal
	for _, n := range s.SortCandidates(candidates) {
		if budgets[poolName(n)] == 0 {
			continue
		}
		// CanPassThreshold. "A DELETE is the upper bound; if it fails, no REPLACE
		// will pass." This is the delete-ratio prune, and it belongs here rather
		// than in a shared candidate filter, because multi-node does not apply it.
		if DeleteRatio(n, totals) < s.Threshold {
			continue
		}
		props := gen.Generate(n, withoutNode(dests, n.ProviderID), cat, totals)
		if len(props) == 0 {
			continue
		}
		out = append(out, props...)
		if s.MaxProposals > 0 && len(out) >= s.MaxProposals {
			return out[:s.MaxProposals]
		}
	}
	return out
}

// -----------------------------------------------------------------------------
// The ordered evaluation
// -----------------------------------------------------------------------------

// PathStats is what one path contributed to a pass.
type PathStats struct {
	// Candidates is how many nodes the path's ShouldDisrupt accepted, after the
	// nodes an earlier path claimed were removed.
	Candidates int
	Proposals  int
	// Nodes is candidate nodes across the path's proposals. It differs from
	// Proposals on the two batching paths.
	Nodes   int
	Savings float64

	// Probes is how many batches the binary search tested. Zero elsewhere.
	Probes int
	// BatchSize is the batch multi-node settled on. Zero elsewhere.
	BatchSize int
}

// EvaluateInOrder runs the three paths in upstream's order and returns every
// proposal they produced, with per-path attribution.
//
// Two rules govern the interaction, and both are upstream's.
//
// A node an earlier path claimed is withheld from later paths. Emptiness claims
// the empty nodes, which is upstream's routing rather than a scheduling nicety:
// consolidation.ShouldDisrupt returns false for IsEmpty. Multi-node claims its
// batch before single-node walks, so single-node never proposes a node that is
// already in a batch. Without this the compactor would drop the duplicate anyway
// and the attribution would be wrong, which is the number this function exists
// to produce.
//
// Each path gets a fresh budget map, because upstream builds one per method.
//
// Where this diverges from upstream, and it is the one place it does: upstream's
// Reconcile returns after the first method that acts, and relies on requeueing
// immediately for the rest. A pass here runs all three, so a single pass reports
// what three consecutive reconciles would do. StopAfterFirstPath reproduces the
// per-reconcile behavior exactly, and TestStopAfterFirstPath pins it.
func EvaluateInOrder(nodes []NodeSnapshot, dests []NodeSnapshot, cat *Catalog, totals PoolTotals, cfg RunConfig) ([]Proposal, map[Path]PathStats) {
	empty := EmptinessEvaluation{}
	multi := MultiNodeEvaluation{Threshold: cfg.Threshold}
	single := SingleNodeEvaluation{Threshold: cfg.Threshold, MaxProposals: cfg.SingleNodeMaxProposals}

	stats := map[Path]PathStats{}
	claimed := map[string]bool{}
	var all []Proposal

	// Disruptable is upstream's ValidateNodeDisruptable, applied in NewCandidate
	// before any method sees a candidate, so it is applied once here rather than
	// repeated in each path's ShouldDisrupt.
	candidatesFor := func(accept func(NodeSnapshot) bool) []NodeSnapshot {
		var out []NodeSnapshot
		for _, n := range nodes {
			if claimed[n.ProviderID] || !Disruptable(n) || !accept(n) {
				continue
			}
			out = append(out, n)
		}
		return out
	}
	record := func(p Path, cands int, props []Proposal, extra PathStats) {
		s := extra
		s.Candidates = cands
		s.Proposals = len(props)
		for _, prop := range props {
			s.Nodes += len(prop.Candidates)
			s.Savings += prop.Savings
			for _, c := range prop.Candidates {
				claimed[c.ProviderID] = true
			}
		}
		stats[p] = s
		all = append(all, props...)
	}

	emptyCands := candidatesFor(func(n NodeSnapshot) bool { return empty.ShouldDisrupt(n, totals, cfg.Threshold) })
	emptyProps := empty.Evaluate(emptyCands, dests, cat, totals, poolBudgets(nodes, cfg.Budget))
	record(PathEmpty, len(emptyCands), emptyProps, PathStats{})
	if cfg.StopAfterFirstPath && len(emptyProps) > 0 {
		return all, stats
	}

	multiCands := candidatesFor(func(n NodeSnapshot) bool { return multi.ShouldDisrupt(n, totals, cfg.Threshold) })
	multiProp, probes, ok := multi.EvaluateCounting(multiCands, dests, cat, totals, poolBudgets(nodes, cfg.Budget))
	var multiProps []Proposal
	batchSize := 0
	if ok {
		multiProps = []Proposal{multiProp}
		batchSize = len(multiProp.Candidates)
	}
	record(PathMultiNode, len(multiCands), multiProps, PathStats{Probes: probes, BatchSize: batchSize})
	if cfg.StopAfterFirstPath && len(multiProps) > 0 {
		return all, stats
	}

	singleCands := candidatesFor(func(n NodeSnapshot) bool { return single.ShouldDisrupt(n, totals, cfg.Threshold) })
	singleProps := single.Evaluate(singleCands, dests, cat, totals, poolBudgets(nodes, cfg.Budget))
	record(PathSingleNode, len(singleCands), singleProps, PathStats{})

	return all, stats
}
