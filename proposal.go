// Package sim implements the four-stage proposal pipeline against Karpenter's
// own types, so the stage boundaries can be checked against the real codebase.
//
// The load-bearing property is in the signatures. Stages 1 and 2 accept a
// candidate and its configuration. Neither accepts a cluster handle, a
// client.Client, or a state.Cluster. That is what makes them local: they cannot
// read global state because they were not given any.
//
// Stages 3 and 4 take the cluster. Every hard constraint that can flip a good
// move into an illegal one is checked there.
package sim

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// Catalog is the set of instance types a NodePool may launch, with prices.
// Prices come from Offering.Price. A catalog is fixed for the duration of a
// reconcile, so stages 1 and 2 may hold one without reading the cluster.
type Catalog struct {
	InstanceTypes []*cloudprovider.InstanceType

	// Overhead is deducted from every instance type's allocatable capacity
	// before anything is placed. See catalog.go.
	Overhead DaemonSetOverhead
}

// Cheapest returns the lowest-priced available offering across the catalog that
// satisfies fits. It is the local provisioning primitive: given requirements,
// return capacity, with no reference to what else is running.
//
// This is alone_cost from the pricing work. Relational constraints cannot be
// expressed here. Topology spread, pod anti-affinity, and volume affinity all
// depend on where other pods already are, so they belong to stage 3.
func (c *Catalog) Cheapest(fits func(*cloudprovider.InstanceType) bool) (*cloudprovider.InstanceType, float64, bool) {
	var best *cloudprovider.InstanceType
	bestPrice := 0.0
	for _, it := range c.InstanceTypes {
		if !fits(it) {
			continue
		}
		for _, o := range it.Offerings {
			if !o.Available {
				continue
			}
			if best == nil || o.Price < bestPrice {
				best, bestPrice = it, o.Price
			}
		}
	}
	return best, bestPrice, best != nil
}

// -----------------------------------------------------------------------------
// Stage 1: candidate identification
// -----------------------------------------------------------------------------

// NodeSnapshot is everything stage 1 and stage 2 may see about one node. It is
// deliberately small. Building one requires the cluster; using one does not.
type NodeSnapshot struct {
	ProviderID   string
	NodePool     *v1.NodePool
	InstanceType *cloudprovider.InstanceType
	Price        float64

	// Pods that would have to be rescheduled if this node went away.
	ReschedulablePods []*corev1.Pod

	// Sum of per-pod eviction costs, plus the per-node baseline.
	DisruptionCost float64
}

// PoolTotals are the denominators in the balanced consolidation score. They are
// snapshotted once per reconcile, so a stage-1 decision is a pure function of a
// NodeSnapshot and a PoolTotals.
type PoolTotals struct {
	Cost           float64
	DisruptionCost float64
}

// DeleteRatio is the best score any single-node move from this node could reach.
// A DELETE recovers the node's full price. A REPLACE recovers strictly less,
// because the replacement costs something. So this bound never understates a
// branch's value, which is what makes pruning on it safe.
//
// One caveat travels with it: a node that fails alone could still join a passing
// multi-node batch. The nodes compensating for it would already be strong
// single-node candidates.
func DeleteRatio(n NodeSnapshot, totals PoolTotals) float64 {
	if totals.Cost == 0 || totals.DisruptionCost == 0 || n.DisruptionCost == 0 {
		return 0
	}
	savingsFraction := n.Price / totals.Cost
	disruptionFraction := n.DisruptionCost / totals.DisruptionCost
	return savingsFraction / disruptionFraction
}

// Identify is stage 1. One bit per node.
//
// Note what it does not take: no context, no client, no cluster. The filter is
// a pure function, so it can be evaluated for every node in parallel, cached
// against a snapshot, and tested with no API server.
//
// Two tests, in this order. A node holding a do-not-disrupt pod is not a
// candidate at all, and a node whose delete ratio cannot reach the approval
// threshold cannot produce a passing move.
//
// Order matters for the denominators. Upstream rejects undisruptable nodes in
// NewCandidate, before pool totals are computed, so they never appear in either
// denominator. Scoring them and rejecting later inflates the candidate count and
// generates work that stage 4 will refuse.
//
// The measurement this exists to support: how much of the oracle's achievable
// savings survives the filter, and how small k/N gets.
func Identify(nodes []NodeSnapshot, totals PoolTotals, k float64) []bool {
	bits := make([]bool, len(nodes))
	for i, n := range nodes {
		bits[i] = Disruptable(n) && DeleteRatio(n, totals) >= 1/k
	}
	return bits
}

// Disruptable reports whether a node may be drained at all.
//
// A karpenter.sh/do-not-disrupt pod blocks eviction, so the node cannot be a
// consolidation candidate whatever it scores. Upstream checks this in
// ValidatePodsDisruptable and returns no candidate for the graceful class, which
// keeps the node out of the pool totals as well.
//
// This reads one node's own pods, so stage 1 stays local.
func Disruptable(n NodeSnapshot) bool {
	for _, p := range n.ReschedulablePods {
		if p.Annotations[v1.DoNotDisruptAnnotationKey] == "true" {
			return false
		}
	}
	return true
}

// -----------------------------------------------------------------------------
// Stage 2: move generation
// -----------------------------------------------------------------------------

// Action is one atomic write against the API server. Every action's partial
// application must leave a valid cluster, which is what makes an ordering
// mechanism sufficient and a transaction unnecessary.
type Action struct {
	Kind ActionKind

	// ProviderID for Taint and Delete. Empty for Launch.
	ProviderID string

	// InstanceType for Launch. Nil otherwise.
	InstanceType *cloudprovider.InstanceType

	// Indices into Proposal.Actions that must confirm before this fires.
	DependsOn []int
}

type ActionKind int

const (
	// Taint cordons a node. Reversible: untaint restores the prior state.
	Taint ActionKind = iota
	// Launch creates a NodeClaim. Reversible before any pod moves.
	Launch
	// Evict removes a pod. THE FIRST IRREVERSIBLE ACTION. Once an Evict has
	// fired, the disruption is paid and rollback recovers nothing.
	Evict
	// Delete removes a node. Irreversible.
	Delete
)

// Reversible reports whether rolling back this action restores the prior state.
// The rollback-or-forward boundary is the first action for which this is false.
func (k ActionKind) Reversible() bool {
	return k == Taint || k == Launch
}

// Proposal is a move that has not been applied. It carries its own score, its
// preconditions, and a DAG of actions with dependency edges.
type Proposal struct {
	// ID is derived from the proposal's content and never randomly generated. It
	// is the compactor's tie-break, and most proposals in a run sit in a score
	// tie, so a random ID would let the tie-break decide most of the ordering.
	// A fixed seed would then stop reproducing the same result.
	ID        string
	CreatedAt time.Time

	// Nodes this proposal removes.
	Candidates []NodeSnapshot

	// Instance types this proposal launches. Empty for a pure DELETE.
	Replacements []*cloudprovider.InstanceType

	// Consolidation ranks by Score, savings over disruption, both as fractions
	// of pool totals. Zero on a provisioning proposal, where the ratio has no
	// meaning: nothing is deleted and nothing is evicted.
	Score   float64
	Savings float64

	// Provisioning ranks by Cost, the price of the capacity bought, and by how
	// many pending pods the proposal covers. Zero on a consolidation proposal.
	Cost        float64
	PodsCovered int

	// Actions in dependency order. Index i may depend only on indices < i.
	Actions []Action

	// Preconditions are the hard constraints whose violation makes this move
	// illegal rather than merely worse. See precondition.go.
	Preconditions []PreconditionKind

	// Reason is which disruption budget this proposal charges. Upstream keeps
	// empty-node deletion in a separate controller under reason Empty, with the
	// comment "Empty nodes are handled by Emptiness (reason \"Empty\") for correct
	// budget accounting." Scoring both against one budget lets a cheap empty
	// delete displace an expensive underutilized move.
	Reason Reason
}

// Reason is the disruption reason a proposal charges its budget against.
type Reason int

const (
	// ReasonEmpty deletes a node with no reschedulable pods. Upstream never
	// scores these; they are always worth doing and cost nothing to disrupt.
	ReasonEmpty Reason = iota
	// ReasonUnderutilized removes a node whose pods have to move. These are what
	// balanced scoring gates.
	ReasonUnderutilized
	// ReasonProvisioning adds capacity for pending pods.
	ReasonProvisioning
)

func (r Reason) String() string {
	switch r {
	case ReasonEmpty:
		return "Empty"
	case ReasonUnderutilized:
		return "Underutilized"
	case ReasonProvisioning:
		return "Provisioning"
	}
	return "unknown"
}

// Score is the balanced consolidation score. Every input drifts continuously:
// prices are fixed, pool cost moves when the node set changes, and disruption
// cost moves by one pod's eviction cost when one pod binds. Nothing here has a
// cliff, which is why the score grades rather than gates.
func Score(savings, disruptionCost float64, totals PoolTotals) float64 {
	if totals.Cost == 0 || totals.DisruptionCost == 0 || disruptionCost == 0 {
		return 0
	}
	savingsFraction := savings / totals.Cost
	disruptionFraction := disruptionCost / totals.DisruptionCost
	return savingsFraction / disruptionFraction
}

// The two generators do not share a Generate signature, and the difference is
// the finding rather than an inconvenience.
//
// ConsolidationGenerator.Generate takes a source node, a pre-filtered
// destination set, a catalog, and pool totals. ProvisioningGenerator.Generate
// takes pending pods and a catalog. Neither takes a cluster handle, so both are
// pure functions of their arguments, but they need different arguments because
// they answer different questions. Forcing one interface over both would mean
// passing nil for half the parameters at every call site.
//
// What they do share is Catalog.Cheapest. Both ask the same question of the same
// primitive: given a resource requirement, what is the cheapest instance type
// that satisfies it. That is the unification claim, and it holds at the level of
// the primitive rather than at the level of the generator.

// -----------------------------------------------------------------------------
// Stage 3: planning and budgets
// -----------------------------------------------------------------------------

// Compactor selects which proposals commit. This is where cluster state enters.
// It holds the budget, sees other in-flight proposals, and resolves conflicts
// between proposals touching the same node.
type Compactor interface {
	// Select ranks proposals and returns those that fit the budget.
	Select(ctx context.Context, proposals []Proposal, budget int) []Proposal
}

// -----------------------------------------------------------------------------
// Stage 4: execution
// -----------------------------------------------------------------------------

// Outcome is the three-way result of a postcondition check. A move can fail as
// an optimization and still have to complete as an obligation.
type Outcome int

const (
	// Proceed: postconditions hold, continue to the next action.
	Proceed Outcome = iota
	// Rollback: undo applied actions. Only available while every applied
	// action is reversible.
	Rollback
	// Forward: the disruption is paid, so finish getting the pods running even
	// though the move no longer delivers what it claimed.
	Forward
)

// Executor applies a proposal one action at a time. Its single guarantee is
// ordering: release no action whose prerequisites are unconfirmed.
//
// It does not build compensation paths. A replacement node whose move stalled
// halfway is an underutilized node, and consolidation already reclaims those.
type Executor interface {
	// Apply walks the action DAG, checking preconditions before each action and
	// postconditions after. It returns the outcome of the last decision made.
	Apply(ctx context.Context, p Proposal) (Outcome, error)
}
