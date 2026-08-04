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

	// What draining this node alone costs: the per-node baseline, the per-pod
	// eviction costs of pods in no gang, and one full outage per gang holding a
	// member here. NodeDisruptionCost computes it.
	//
	// It is a bound rather than an exact figure when a gang spans nodes, because a
	// node cannot see members it does not hold. Nothing scores a gang from this
	// figure: a gang candidate prices its own nodes together, which is what makes
	// the charge exact. See Candidate in candidate.go and NodeDisruptionCost.
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
//
// This is the node form. Candidate.DeleteRatio is the same arithmetic at the
// decision unit, and the two agree term for term on a node holding no gang member.
func DeleteRatio(n NodeSnapshot, totals PoolTotals) float64 {
	if totals.Cost == 0 || totals.DisruptionCost == 0 || n.DisruptionCost == 0 {
		return 0
	}
	savingsFraction := n.Price / totals.Cost
	disruptionFraction := n.DisruptionCost / totals.DisruptionCost
	return savingsFraction / disruptionFraction
}

// Identify is stage 1, projected back onto the node list: one bit per node,
// saying whether that node belongs to a decision unit worth generating a move
// for.
//
// The unit is the workload scope, not the node. A node carrying no gang member is
// its own candidate and its bit is the node's own test. A node carrying gang
// members belongs to one candidate per gang on it, each scored once over that
// gang's nodes, and the node's bit is true when any of them passed. See
// candidate.go for the unit and CandidateBits for stage 1 at that unit, which is
// the form with one bit per candidate rather than one per node.
//
// The projection exists because the number this function reports is a share of
// the node list, and because every existing caller asks about nodes. It is not
// where the decision is made. What changed is the arithmetic behind the bit: a
// gang is charged one outage over its own nodes instead of a full outage on each
// node holding a member, so five nodes holding one gang are no longer each priced
// as though the gang were theirs alone.
//
// Note what it still does not take: no context, no client, no cluster. Grouping
// the nodes by gang label reads the argument it was given, so the unit change cost
// it nothing in reach. The filter stays a pure function, evaluable in parallel,
// cacheable against a snapshot, and testable with no API server.
//
// Two tests, in this order. A candidate any of whose nodes holds a do-not-disrupt
// pod is not a candidate at all, and a candidate whose delete ratio cannot reach
// the approval threshold cannot produce a passing move.
//
// Order matters for the denominators. Upstream rejects undisruptable nodes in
// NewCandidate, before pool totals are computed, so they never appear in either
// denominator. Scoring them and rejecting later inflates the candidate count and
// generates work that stage 4 will refuse.
//
// The measurement this exists to support: how much of the oracle's achievable
// savings survives the filter, and how small k/N gets.
func Identify(nodes []NodeSnapshot, totals PoolTotals, k float64) []bool {
	cands := Candidates(nodes)
	passed := CandidateBits(cands, totals, k)

	// A node's bit is true when some candidate covering it passed. Two gangs sharing
	// a node give it two candidates, and the node is worth a move if either is.
	keep := make(map[string]bool, len(nodes))
	for i, c := range cands {
		if !passed[i] {
			continue
		}
		for _, n := range c.Nodes {
			keep[n.ProviderID] = true
		}
	}

	bits := make([]bool, len(nodes))
	for i, n := range nodes {
		bits[i] = keep[n.ProviderID]
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
// This reads one node's own pods. A gang candidate applies it to every node its
// move acts on, which is Candidate.Disruptable, and stays local for the same
// reason: the read set is the action set.
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
	// Taint cordons a node. Stopping here leaves a node someone means to drain.
	Taint ActionKind = iota
	// Launch creates a NodeClaim. Stopping here leaves an underutilized node,
	// which is what consolidation reclaims.
	Launch
	// Evict removes a pod. Stopping here leaves a pending pod, which is what
	// provisioning places.
	Evict
	// Delete removes a node. Stopping here leaves a smaller cluster.
	Delete
)

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

	// GangID names the gang this proposal is a move over, empty on a node-unit
	// move. It says the proposal's Candidates are one decision unit rather than a
	// batch of independent ones, which is what makes the proposal indivisible: a
	// gang cannot be half-consolidated, so committing some of these nodes and not
	// the rest takes the workload down and recovers part of the price.
	//
	// The compactor needs no special case for it, and that is worth stating rather
	// than discovering. Its accounting is already per proposal: overlap is checked
	// across every candidate node, and the budget must cover the whole candidate
	// count or the proposal is skipped entirely. So a gang move commits whole or not
	// at all under the rule that was already there. The field is what lets a caller
	// tell the two units apart, and what a test asserts on.
	GangID string

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

	// AtomicLaunch makes this proposal's launch set all-or-nothing: either every
	// Launch lands or none do and the ones that landed are released. False is
	// Omega's incremental transaction, which "accepts all but the conflicting
	// changes," and it is the default because every existing proposal wants it.
	//
	// It is a property of the launch set and not of the whole move. The eviction
	// half has no undo, so nothing here promises anything about it. What the flag
	// buys is that the eviction half never begins from a partial acquisition.
	//
	// The flag only says something for a proposal with two or more launches. A
	// single-launch REPLACE is trivially all-or-nothing already, since one launch
	// either happens or does not, and setting the flag on one changes nothing.
	//
	// See atomic.go for the action shape this requires and the executor's Apply
	// for what it does.
	AtomicLaunch bool

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

// Outcome is what became of one proposal.
type Outcome int

const (
	// Proceed: every precondition held through the last action.
	Proceed Outcome = iota
	// Stopped: a precondition failed. Whatever had already fired stays.
	Stopped
	// Abandoned: an AtomicLaunch proposal could not complete its launch set, so
	// the launches that did land were released and the proposal holds none of the
	// capacity it asked for. It is a separate outcome from Stopped because a
	// Stopped proposal keeps whatever it acquired and an Abandoned one does not.
	//
	// It says nothing about the proposal's non-launch actions. A taint that fired
	// before the launch set stays tainted, which is the same state a Stopped
	// proposal leaves and one the cluster already handles.
	Abandoned
)

func (o Outcome) String() string {
	switch o {
	case Stopped:
		return "Stopped"
	case Abandoned:
		return "Abandoned"
	}
	return "Proceed"
}

// Executor applies a proposal one action at a time. Its single guarantee is
// ordering: release no action whose prerequisites are unconfirmed.
//
// It builds no unwind path and no compensation path, because every state an
// action leaves behind is one the system already handles continuously. Ordering
// is what survives: a later pass cannot recover the gap where pods are pending
// because a node was deleted before its replacement was ready.
//
// AtomicLaunch does not change that. Releasing a node nobody put a pod on is not
// an unwind of a committed change, because a launch that no eviction has depended
// on yet has had no effect anyone can observe: the state where four of five nodes
// are up is never committed rather than committed and reversed. No eviction is
// ever compensated for, which is the thing that cannot be done.
type Executor interface {
	// Apply walks the action DAG, checking preconditions before each action. It
	// returns whether the proposal ran out of actions or out of preconditions.
	Apply(ctx context.Context, p Proposal) (Outcome, error)
}
