package sim

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// A precondition is a predicate that flips rather than drifts. The score itself
// drifts, so it grades and ranks; its comparison against the approval threshold
// flips, so that comparison is gated here alongside the hard constraints.
//
// The list is not arbitrary. Every entry is something whose value jumps, which is
// why re-checking it at apply time can change a verdict and re-reading a smooth
// quantity cannot.
type PreconditionKind int

const (
	// PodsStillFit: the candidate's pods can land somewhere. Capacity is
	// continuous, fitting is binary. Local.
	PodsStillFit PreconditionKind = iota

	// NoDoNotDisrupt: no karpenter.sh/do-not-disrupt pod arrived on the
	// candidate. Forbidden or not. Local.
	NoDoNotDisrupt

	// BudgetHasRoom: the NodePool's disruption budget still permits this move.
	// A step function at the limit. Pool-scoped.
	BudgetHasRoom

	// NotNominated: the candidate has not been nominated for a pending pod.
	// Karpenter's own validator already checks this. Local.
	NotNominated

	// PDBAllows: the pods' PodDisruptionBudgets permit eviction. minAvailable is
	// a step, and PDBs are shared across workloads, so the change can arrive from
	// a workload unrelated to this candidate.
	//
	// Measured: local, and bounded by nothing at all. The PDB object carries
	// DisruptionsAllowed in its status, so the check reads the candidate node to
	// see which pods are leaving and then reads one object per PDB. It does not
	// read the workload's other nodes, and it does not grow with replica count.
	// Deriving availability instead of reading it costs the cluster, which is
	// what PDBChecker.Recompute measures and readcost_test.go reports.
	PDBAllows

	// TopologyHolds: evicting these pods does not push any topology domain past
	// maxSkew. Skew 2 is legal at maxSkew 2, skew 3 is not. Moving a pod in
	// zone A can invalidate a move in zone B. NOT LOCAL, and unlike the PDB there
	// is no status field standing in for the distribution.
	TopologyHolds

	// AntiAffinityHolds: the destination is still permitted by pod anti-affinity.
	//
	// Measured: local at hostname scope, one node read per destination tried.
	// "Do not place me where pods matching X run" is a question about the
	// destination's own pod list. At a wider topology key the same term is a
	// question about a whole domain and the cost becomes the cluster, so the
	// locality belongs to the term rather than to the constraint. See
	// LocalForTopologyKey.
	AntiAffinityHolds

	// GangIntact: for all-or-nothing workloads, the full set is still
	// satisfiable. Worth nothing below N. NOT LOCAL.
	GangIntact

	// AffinityHolds: the destination is still permitted by pod affinity, the
	// same relation with the sign flipped. Added last so the constants above keep
	// their values.
	AffinityHolds

	// ScoreStillClears: the move's score, recomputed against current inputs,
	// still clears the approval threshold.
	//
	// The score itself drifts, so it grades. The comparison against the threshold
	// does not: a move at 0.51 with a floor of 0.5 is approved and the same move at
	// 0.49 is not. That comparison is a predicate that flips, which is what puts it
	// here rather than leaving it in stage 2 alone.
	//
	// It is also what removes the need for a forward path. A move whose value
	// decayed below the floor does not start, so there is no case where a proposal
	// has to be finished as an obligation after its value went away.
	//
	// Measured: one node read per candidate per check, 4.8 reads per applied move
	// at 1000 nodes against 4.7 for the PDB and 2156 for topology spread, and flat
	// from 10 nodes to 1000. The inputs that move are the candidate's pods and its
	// price, both on the node. The pool denominators are the cluster, and this
	// check does not derive them; it carries the totals stage 3 was handed. See
	// ScoreStillClearsChecker.
	ScoreStillClears
)

// Local reports whether this precondition can be evaluated from one node's own
// state plus its NodePool configuration.
//
// The list was written from the shape of each constraint and then measured, and
// the measurement moved three of them. readcost_test.go reports nodes read per
// decision at 10, 100, and 1000 nodes.
//
// Anti-affinity and affinity are local at hostname scope. Both ask about the pods
// on one node, so one node is what they read, and the count does not move with
// cluster size. At a wider topology key both ask about a domain and read the
// cluster, which is why LocalForTopologyKey exists and this method cannot answer
// for them alone.
//
// PDB is local because the object carries its own availability. The count that
// makes the answer is maintained by the PDB controller, and the eviction path
// reads it rather than deriving it.
//
// The score is local for the same reason with a different supplier. Its numerator
// is one node's pods and one node's price; its denominators are the pool, and the
// pass computes those once rather than per check. So the answer is local only as
// long as something else holds the totals, and that is stated rather than assumed:
// carrying stale denominators across a pass is a real cost, and it is named in
// ScoreStillClearsChecker.
//
// Topology spread is the one that stays non-local, and there is no status field to
// read in place of the distribution. maxSkew is defined against the minimum count
// across domains, so the answer is spread over every node the constraint governs.
// Gang scheduling is the same shape and is not implemented here.
//
// So the boundary is real and it is narrower than it looked. Either the check
// grows to cover the relation, which costs the locality that made stages 1 and 2
// parallel, or workloads carrying it do not get local proposals.
func (p PreconditionKind) Local() bool {
	switch p {
	case PodsStillFit, NoDoNotDisrupt, NotNominated:
		return true
	case BudgetHasRoom:
		// Pool-scoped: needs the NodePool's counter, not the whole cluster.
		return true
	case PDBAllows:
		// The PDB's status carries DisruptionsAllowed, so the check reads the
		// candidate and one object. Measured flat across three cluster sizes.
		return true
	case AntiAffinityHolds, AffinityHolds:
		// At hostname scope. See LocalForTopologyKey, which is the honest answer.
		return true
	case ScoreStillClears:
		// One node per candidate. The pool denominators are cluster-wide and are
		// carried forward from stage 3 rather than re-derived, so no read pays for
		// them here. Deriving them instead would cost the cluster, which is the
		// distinction ScoreStillClearsChecker's comment records.
		return true
	default:
		return false
	}
}

// LocalForTopologyKey is Local() for the two constraints whose cost depends on
// their topology key rather than on their kind.
//
// A hostname-scoped term is answered by the destination node. Any wider key means
// the term governs a domain, and finding a domain's members means reading every
// node, because a node's domain is a label on the node.
func (p PreconditionKind) LocalForTopologyKey(topologyKey string) bool {
	switch p {
	case AntiAffinityHolds, AffinityHolds:
		return topologyKey == corev1.LabelHostname
	default:
		return p.Local()
	}
}

func (p PreconditionKind) String() string {
	switch p {
	case PodsStillFit:
		return "PodsStillFit"
	case NoDoNotDisrupt:
		return "NoDoNotDisrupt"
	case BudgetHasRoom:
		return "BudgetHasRoom"
	case NotNominated:
		return "NotNominated"
	case PDBAllows:
		return "PDBAllows"
	case TopologyHolds:
		return "TopologyHolds"
	case AntiAffinityHolds:
		return "AntiAffinityHolds"
	case AffinityHolds:
		return "AffinityHolds"
	case GangIntact:
		return "GangIntact"
	case ScoreStillClears:
		return "ScoreStillClears"
	}
	return "unknown"
}

// Checker evaluates one precondition against live cluster state. Unlike stages
// 1 and 2, a Checker sees whatever it needs. That is the point of putting the
// cliffs in stage 4.
type Checker interface {
	Check(ctx context.Context, p Proposal) error
	Kind() PreconditionKind
}

// CheckAll evaluates every precondition a proposal declares, returning the
// first violation. A violation stops the move where it is. Actions already
// applied stay applied, because each one leaves a state some controller already
// handles: a tainted node is a node someone means to drain, an extra node is an
// underutilized node consolidation reclaims, an evicted pod is a pending pod
// provisioning places.
func CheckAll(ctx context.Context, p Proposal, checkers map[PreconditionKind]Checker) error {
	return CheckAllCounting(ctx, p, checkers, nil)
}

// CheckAllCounting is CheckAll with the node reads accumulated into reads, keyed
// by precondition.
//
// The accumulation happens as each checker returns rather than after the loop,
// because the loop stops at the first violation and a checker that did not run
// this time still carries last time's count.
func CheckAllCounting(ctx context.Context, p Proposal, checkers map[PreconditionKind]Checker, reads map[PreconditionKind]int) error {
	for _, kind := range p.Preconditions {
		c, ok := checkers[kind]
		if !ok {
			return fmt.Errorf("no checker registered for %s", kind)
		}
		err := c.Check(ctx, p)
		if counting, ok := c.(CountingChecker); ok && reads != nil {
			reads[kind] += counting.Reads()
		}
		if err != nil {
			return &PreconditionViolation{Kind: kind, Err: err}
		}
	}
	return nil
}

// PreconditionViolation names which precondition stopped a move. The kind is
// carried in a field rather than only in the message, so a pass can report what
// its stops were caused by without parsing text.
type PreconditionViolation struct {
	Kind PreconditionKind
	Err  error
}

func (v *PreconditionViolation) Error() string {
	return fmt.Sprintf("precondition %s: %v", v.Kind, v.Err)
}

func (v *PreconditionViolation) Unwrap() error { return v.Err }
