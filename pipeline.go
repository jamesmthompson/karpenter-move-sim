package sim

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// -----------------------------------------------------------------------------
// Stage 3: the compactor
// -----------------------------------------------------------------------------

// RankingCompactor commits proposals by score until the budget is spent,
// dropping any proposal that touches a node an already-committed proposal
// claimed.
//
// Overlap is a conflict rather than a merge opportunity. Two proposals removing
// the same node cannot both be right, and the one that scored lower is the one
// with less value per unit of disruption. The common case is the same node's
// DELETE and REPLACE, which stage 2 emits as alternatives without knowing which
// will win.
//
// Overlap here means candidates only. Two proposals planning against the same
// free capacity on the same destination are NOT detected, and this compactor
// cannot detect them: a Proposal records the nodes it removes and the instance
// types it launches, and nothing about the existing destinations its generator
// counted on. The information is not in the type. Everything that survives to
// apply time therefore falls to PodsStillFit, and a run where most committed
// proposals roll back on PodsStillFit is that gap being measured rather than a
// bug in the checker.
type RankingCompactor struct{}

// Select satisfies Compactor. The conflict count is dropped, which is why
// SelectCounting exists: the count is the interesting number and the interface
// predates it.
func (c RankingCompactor) Select(ctx context.Context, proposals []Proposal, budget int) []Proposal {
	committed, _ := c.SelectCounting(ctx, proposals, budget)
	return committed
}

// SelectCounting returns the committed proposals and how many were dropped for
// overlapping a commitment.
//
// A proposal skipped for want of budget is not counted as a conflict. Budget
// exhaustion means the work was legal and deferred; a conflict means the work
// was never going to happen this pass.
func (c RankingCompactor) SelectCounting(_ context.Context, proposals []Proposal, budget int) ([]Proposal, int) {
	return c.selectWithBudgets(proposals, map[Reason]int{
		ReasonEmpty:         budget,
		ReasonUnderutilized: budget,
		ReasonProvisioning:  budget,
	})
}

// selectWithBudgets accounts each reason against its own allowance.
//
// Upstream keeps empty-node deletion in a separate controller under reason Empty
// with its own budget, and says why: "Empty nodes are handled by Emptiness
// (reason \"Empty\") for correct budget accounting." Sharing one budget lets a
// cheap empty delete take a slot an expensive underutilized move needed, which
// inflates savings per budget slot and hides the moves that actually matter.
//
// Ranking still runs across the whole set, so the ordering is global and only the
// accounting is per reason.
func (c RankingCompactor) selectWithBudgets(proposals []Proposal, budgets map[Reason]int) ([]Proposal, int) {
	ranked := make([]Proposal, len(proposals))
	copy(ranked, proposals)
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].Score != ranked[j].Score {
			return ranked[i].Score > ranked[j].Score
		}
		// ID breaks ties. It is derived from the proposal's content, so the same
		// seed produces the same ordering. Most proposals in a run sit in a score
		// tie, so this comparison decides most of the order and a random ID would
		// make a seeded run unreproducible.
		return ranked[i].ID < ranked[j].ID
	})

	claimed := map[string]bool{}
	var committed []Proposal
	conflicts := 0
	spent := map[Reason]int{}

	for _, p := range ranked {
		if overlaps(p, claimed) {
			conflicts++
			continue
		}
		cost := len(p.Candidates)
		if cost == 0 {
			// A pure launch destroys nothing, so it does not draw on the
			// disruption budget.
			committed = append(committed, p)
			continue
		}
		if spent[p.Reason]+cost > budgets[p.Reason] {
			continue
		}
		for _, n := range p.Candidates {
			claimed[n.ProviderID] = true
		}
		spent[p.Reason] += cost
		committed = append(committed, p)
	}
	return committed, conflicts
}

func overlaps(p Proposal, claimed map[string]bool) bool {
	for _, n := range p.Candidates {
		if claimed[n.ProviderID] {
			return true
		}
	}
	return false
}

// -----------------------------------------------------------------------------
// The in-memory cluster model stage 4 writes against
// -----------------------------------------------------------------------------

// ClusterNode is one node as the executor sees it. Unlike a NodeSnapshot this is
// mutable, because stage 4 is the only stage that changes anything.
type ClusterNode struct {
	ProviderID   string
	InstanceType *cloudprovider.InstanceType
	Price        float64
	Pods         []*corev1.Pod

	// Labels are the node's topology labels, which is where a topology key is
	// resolved. They live on the node and not on the NodeSnapshot, because
	// resolving a key means reading the node, and that read is the cost the
	// locality measurement is about.
	Labels map[string]string

	Tainted bool
	Deleted bool

	// Launched marks a node this pass created. Rollback removes it; a node that
	// was already there is not ours to delete.
	Launched bool
}

// Cluster is the state stages 1 and 2 were denied and stages 3 and 4 are given.
// It is a map and two counters, which is the point: the state that flips a good
// move into an illegal one is small, and gating on it does not require
// re-deriving the cluster's scheduling.
type Cluster struct {
	Catalog *Catalog
	Nodes   map[string]*ClusterNode

	// Nominated nodes are reserved for a pending pod the scheduler already
	// aimed at them. Draining one strands that pod.
	Nominated map[string]bool

	// Budget is nodes still disruptable this window.
	Budget int

	// PDBs are the PodDisruptionBudget objects, keyed by the label value their
	// selector matches. Built by IndexPDBs, which is the PDB controller's work and
	// not the eviction path's.
	PDBs map[string]*PodDisruptionBudget

	// LaunchesInFlight is how many of the proposal being applied right now have
	// already fired. PodsStillFit needs it to tell a replacement that is still a
	// plan from one that is already a node, and counting both would approve a move
	// against capacity twice over. The executor sets and clears it.
	LaunchesInFlight int

	// Pending are pods evicted with nowhere to land. In a real cluster the
	// provisioner picks these up; here they are the record of what a move cost
	// that its score never charged it for.
	Pending []*corev1.Pod

	launchSeq int
}

// NewCluster materializes the model from the same snapshots stages 1 and 2 read.
func NewCluster(nodes []NodeSnapshot, cat *Catalog, budget int) *Cluster {
	c := &Cluster{
		Catalog:   cat,
		Nodes:     make(map[string]*ClusterNode, len(nodes)),
		Nominated: map[string]bool{},
		Budget:    budget,
	}
	for _, n := range nodes {
		pods := make([]*corev1.Pod, len(n.ReschedulablePods))
		copy(pods, n.ReschedulablePods)
		c.Nodes[n.ProviderID] = &ClusterNode{
			ProviderID:   n.ProviderID,
			InstanceType: n.InstanceType,
			Price:        n.Price,
			Pods:         pods,
			Labels:       nodeLabels(n.ProviderID),
		}
	}
	return c
}

// Cost is the hourly price of every node still standing.
func (c *Cluster) Cost() float64 {
	total := 0.0
	for _, n := range c.Nodes {
		if !n.Deleted {
			total += n.Price
		}
	}
	return total
}

// free is what a node has left, including pod slots. Pod slots are a real
// capacity dimension and the only one Requests cannot see, because a pod does
// not request itself.
func (c *Cluster) free(n *ClusterNode) corev1.ResourceList {
	out := corev1.ResourceList{}
	if n.InstanceType == nil {
		return out
	}
	used := Requests(n.Pods)
	for name, have := range c.Catalog.Effective(n.InstanceType) {
		remaining := have.DeepCopy()
		if u, ok := used[name]; ok {
			remaining.Sub(u)
		}
		out[name] = remaining
	}
	slots := out[corev1.ResourcePods]
	slots.Sub(*resource.NewQuantity(int64(len(n.Pods)), resource.DecimalSI))
	out[corev1.ResourcePods] = slots
	return out
}

func podNeeds(p *corev1.Pod) corev1.ResourceList {
	return withPodSlots(Requests([]*corev1.Pod{p}), 1)
}

func fitsInto(free, need corev1.ResourceList) bool {
	for name, want := range need {
		have, ok := free[name]
		if !ok || have.Cmp(want) < 0 {
			return false
		}
	}
	return true
}

// Taint cordons a node. Reversible.
func (c *Cluster) Taint(providerID string) error {
	n, ok := c.Nodes[providerID]
	if !ok || n.Deleted {
		return fmt.Errorf("taint %s: node is gone", providerID)
	}
	n.Tainted = true
	return nil
}

func (c *Cluster) untaint(providerID string) {
	if n, ok := c.Nodes[providerID]; ok {
		n.Tainted = false
	}
}

// Launch adds a node of the given type. Reversible until a pod lands on it.
func (c *Cluster) Launch(it *cloudprovider.InstanceType) (string, error) {
	if it == nil {
		return "", errors.New("launch: nil instance type")
	}
	price, ok := cheapestOffering(it)
	if !ok {
		return "", fmt.Errorf("launch %s: no available offering", it.Name)
	}
	c.launchSeq++
	id := fmt.Sprintf("i-new-%04d", c.launchSeq)
	c.Nodes[id] = &ClusterNode{
		ProviderID:   id,
		InstanceType: it,
		Price:        price,
		Labels:       nodeLabels(id),
		Launched:     true,
	}
	return id, nil
}

// Evict removes one pod from a node and places it wherever it fits. This is the
// first irreversible action, and the reason is visible here: the pod's identity
// on its old node is gone whether or not the placement succeeds.
func (c *Cluster) Evict(providerID string, prefer string) error {
	n, ok := c.Nodes[providerID]
	if !ok || n.Deleted {
		return fmt.Errorf("evict from %s: node is gone", providerID)
	}
	if len(n.Pods) == 0 {
		// The generator emitted one Evict per pod it saw. Fewer pods now means
		// the workload finished on its own, which is not a failure.
		return nil
	}
	pod := n.Pods[0]
	n.Pods = n.Pods[1:]
	if dest, ok := c.placement(pod, providerID, prefer); ok {
		dest.Pods = append(dest.Pods, pod)
		return nil
	}
	c.Pending = append(c.Pending, pod)
	return nil
}

// placement is the scheduler stand-in: first fit over untainted nodes, with the
// proposal's own replacement preferred so a REPLACE lands its pods where it
// planned to. Iteration is over a sorted key list because Go map order is
// random and a simulator that reorders placements between runs is not
// reproducible.
func (c *Cluster) placement(pod *corev1.Pod, exclude, prefer string) (*ClusterNode, bool) {
	need := podNeeds(pod)
	if prefer != "" {
		if n, ok := c.Nodes[prefer]; ok && !n.Deleted && !n.Tainted && fitsInto(c.free(n), need) {
			return n, true
		}
	}
	for _, id := range c.sortedIDs() {
		n := c.Nodes[id]
		if id == exclude || n.Deleted || n.Tainted {
			continue
		}
		if fitsInto(c.free(n), need) {
			return n, true
		}
	}
	return nil, false
}

func (c *Cluster) sortedIDs() []string {
	ids := make([]string, 0, len(c.Nodes))
	for id := range c.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Delete removes a node. A pod still on it is rescheduled by its controller,
// which the narrative relies on for partial application to be safe, so the same
// placement runs here. A pod with nowhere to go is stranded.
//
// A node reaching Delete with pods on it means the plan miscounted. Stage 2
// emitted one Evict per pod it saw in the snapshot, so extra pods arrived after
// the snapshot was taken.
func (c *Cluster) Delete(providerID string) error {
	n, ok := c.Nodes[providerID]
	if !ok || n.Deleted {
		return fmt.Errorf("delete %s: node is gone", providerID)
	}
	left := n.Pods
	n.Pods = nil
	n.Deleted = true
	for _, pod := range left {
		if dest, ok := c.placement(pod, providerID, ""); ok {
			dest.Pods = append(dest.Pods, pod)
			continue
		}
		c.Pending = append(c.Pending, pod)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Checkers
// -----------------------------------------------------------------------------

// LocalCheckers returns a checker for every precondition a proposal can declare.
//
// Four read one node and its NodePool. Four are the relational constraints, and
// they are implemented rather than stubbed, because the claim about their cost is
// only worth something if the code that pays it is here to be measured. Each
// counts the nodes it reads. GangIntact is the exception and stays a stub, since
// nothing in the model carries a gang's minimum member count.
//
// PDB defaults to reading the object's status. Recompute is the other path and
// costs the cluster; readcost_test.go runs both.
func LocalCheckers(c *Cluster) map[PreconditionKind]Checker {
	return map[PreconditionKind]Checker{
		PodsStillFit:      PodsStillFitChecker{Cluster: c},
		NoDoNotDisrupt:    NoDoNotDisruptChecker{Cluster: c},
		BudgetHasRoom:     BudgetHasRoomChecker{Cluster: c},
		NotNominated:      NotNominatedChecker{Cluster: c},
		PDBAllows:         &PDBChecker{Cluster: c},
		TopologyHolds:     &PlacementChecker{Cluster: c, Constraint: TopologyConstraint{}},
		AntiAffinityHolds: &PlacementChecker{Cluster: c, Constraint: AntiAffinityConstraint{}},
		AffinityHolds:     &PlacementChecker{Cluster: c, Constraint: AffinityConstraint{}},
		GangIntact:        unimplementedChecker{kind: GangIntact},
	}
}

// PodsStillFitChecker asks whether the candidates' remaining pods can be placed
// on nodes that are not being drained, plus the capacity the proposal launches.
//
// It re-runs the placement rather than trusting the aggregate-room test stage 2
// used, because aggregate room is necessary and not sufficient. A single large
// pod can fail to fit on any one node while the total free capacity is ample.
//
// The proposal's own replacements count as destinations. Without them a REPLACE
// is checked against a cluster that does not contain the node it exists to buy,
// so it fails whenever its pods do not fit elsewhere, which is the only condition
// under which a REPLACE is generated instead of a DELETE. Upstream's
// validateCommand runs the same simulation over Replacements for the same reason.
type PodsStillFitChecker struct{ Cluster *Cluster }

func (k PodsStillFitChecker) Kind() PreconditionKind { return PodsStillFit }

func (k PodsStillFitChecker) Check(_ context.Context, p Proposal) error {
	draining := map[string]bool{}
	for _, n := range p.Candidates {
		draining[n.ProviderID] = true
	}

	// Free capacity is tracked in a scratch list so placing one pod is visible to
	// the next. Checking each pod against untouched capacity would pass a set of
	// pods that all fit the same hole. It is a list rather than a map because the
	// order pods are tried in decides the verdict, and Go map order is random.
	//
	// The proposal's own replacements come first, matching the order Cluster.Evict
	// actually places pods in. A checker that preferred existing slack would
	// answer a different question than the executor asks.
	var scratch []corev1.ResourceList
	// Replacements the executor has already launched are standing nodes, picked up
	// by the loop below. Counting them here as well would approve a move against
	// capacity the cluster does not have. Launch actions fire in DAG order, so the
	// ones already up are a prefix of Replacements.
	for i, it := range p.Replacements {
		if it == nil || i < k.Cluster.LaunchesInFlight {
			continue
		}
		scratch = append(scratch, k.Cluster.Catalog.Effective(it))
	}
	for _, id := range k.Cluster.sortedIDs() {
		n := k.Cluster.Nodes[id]
		if n.Deleted || n.Tainted || draining[id] {
			continue
		}
		scratch = append(scratch, k.Cluster.free(n))
	}

	for _, cand := range p.Candidates {
		n, ok := k.Cluster.Nodes[cand.ProviderID]
		if !ok {
			continue
		}
		for _, pod := range n.Pods {
			need := podNeeds(pod)
			placed := false
			for _, free := range scratch {
				if !fitsInto(free, need) {
					continue
				}
				for name, want := range need {
					have := free[name]
					have.Sub(want)
					free[name] = have
				}
				placed = true
				break
			}
			if !placed {
				return fmt.Errorf("pod %s has nowhere to go", pod.Name)
			}
		}
	}
	return nil
}

// NoDoNotDisruptChecker rejects a move over a pod that opted out. The annotation
// can arrive after the proposal was generated, which is why this is a gate and
// not a filter in stage 1.
type NoDoNotDisruptChecker struct{ Cluster *Cluster }

func (k NoDoNotDisruptChecker) Kind() PreconditionKind { return NoDoNotDisrupt }

func (k NoDoNotDisruptChecker) Check(_ context.Context, p Proposal) error {
	for _, cand := range p.Candidates {
		n, ok := k.Cluster.Nodes[cand.ProviderID]
		if !ok {
			continue
		}
		for _, pod := range n.Pods {
			if pod.Annotations[v1.DoNotDisruptAnnotationKey] == "true" {
				return fmt.Errorf("pod %s carries %s", pod.Name, v1.DoNotDisruptAnnotationKey)
			}
		}
	}
	return nil
}

// BudgetHasRoomChecker gates on the NodePool's remaining disruption allowance.
type BudgetHasRoomChecker struct{ Cluster *Cluster }

func (k BudgetHasRoomChecker) Kind() PreconditionKind { return BudgetHasRoom }

func (k BudgetHasRoomChecker) Check(_ context.Context, p Proposal) error {
	need := len(p.Candidates)
	if need == 0 {
		// A launch removes nothing, so it spends no budget.
		return nil
	}
	if k.Cluster.Budget < need {
		return fmt.Errorf("budget has %d, move needs %d", k.Cluster.Budget, need)
	}
	return nil
}

// NotNominatedChecker rejects draining a node the scheduler already aimed a
// pending pod at.
type NotNominatedChecker struct{ Cluster *Cluster }

func (k NotNominatedChecker) Kind() PreconditionKind { return NotNominated }

func (k NotNominatedChecker) Check(_ context.Context, p Proposal) error {
	for _, cand := range p.Candidates {
		if k.Cluster.Nominated[cand.ProviderID] {
			return fmt.Errorf("node %s is nominated for a pending pod", cand.ProviderID)
		}
	}
	return nil
}

// unimplementedChecker always passes. It now stands for GangIntact alone.
//
// A gang is all-or-nothing below a member count, and nothing in this model
// carries that count: a Proposal knows its candidates and a pod knows its labels,
// and neither knows how many members the workload needs simultaneously. Adding
// the count is a fixture change and not a checker change, so the stub is honest
// about what is missing rather than approximating it.
//
// A number produced by this pipeline is therefore an upper bound on what the
// architecture delivers for gang-scheduled workloads. The other three relational
// constraints are implemented and measured; see constraints.go.
type unimplementedChecker struct{ kind PreconditionKind }

func (k unimplementedChecker) Kind() PreconditionKind { return k.kind }

func (k unimplementedChecker) Check(context.Context, Proposal) error { return nil }

// -----------------------------------------------------------------------------
// Stage 4: the executor
// -----------------------------------------------------------------------------

// MemoryExecutor applies proposals against a Cluster.
//
// It carries counters because the numbers the paper needs are properties of the
// apply loop and not of any one proposal: how often a move proceeds, how often
// it rolls back for free, and how often it has to finish work that no longer
// pays.
type MemoryExecutor struct {
	Cluster  *Cluster
	Checkers map[PreconditionKind]Checker

	Proceeded  int
	RolledBack int
	Forwarded  int

	// Forward is unplanned disruption, charged against the budget. See
	// ForwardCost.
	Forward ForwardCost

	// Failures records the precondition error behind each non-Proceed outcome,
	// in apply order.
	Failures []error

	// NodeReads is nodes read per precondition, summed over every Check the pass
	// ran. It accumulates across proposals, so it is the pass's total cost for that
	// constraint and not one proposal's.
	NodeReads map[PreconditionKind]int
}

func NewMemoryExecutor(c *Cluster) *MemoryExecutor {
	return &MemoryExecutor{
		Cluster:   c,
		Checkers:  LocalCheckers(c),
		NodeReads: map[PreconditionKind]int{},
	}
}

// applied records one action that fired, so rollback knows what to undo.
type applied struct {
	kind       ActionKind
	providerID string
}

// Apply walks the action DAG in order.
//
// A precondition failure is an outcome and not an error. The error return is for
// faults in the mechanism itself: an action released before its prerequisites
// confirmed, a missing checker, a write against a node that vanished. Conflating
// the two would make a legal rollback indistinguishable from a bug.
func (e *MemoryExecutor) Apply(ctx context.Context, p Proposal) (Outcome, error) {
	confirmed := map[int]bool{}
	var log []applied
	appliedIrreversible := false
	evicted := 0
	// replacement is the node this proposal launched, so its own evictions land
	// on the capacity it bought.
	replacement := ""

	// PodsStillFit counts a replacement that has not launched yet as capacity,
	// since the proposal is about to buy it. Once the Launch fires the node is
	// real and counting it twice would approve a move against capacity that is
	// not there. Cleared on the way out so the next proposal starts at zero.
	e.Cluster.LaunchesInFlight = 0
	defer func() { e.Cluster.LaunchesInFlight = 0 }()

	for i, a := range p.Actions {
		for _, dep := range a.DependsOn {
			if dep >= i {
				return Proceed, fmt.Errorf("action %d depends on %d, which is not earlier", i, dep)
			}
			if !confirmed[dep] {
				return Proceed, fmt.Errorf("action %d released before %d confirmed", i, dep)
			}
		}

		if err := CheckAllCounting(ctx, p, e.Checkers, e.NodeReads); err != nil {
			return e.resolve(p, err, log, appliedIrreversible, evicted)
		}

		switch a.Kind {
		case Taint:
			if err := e.Cluster.Taint(a.ProviderID); err != nil {
				return Proceed, err
			}
			log = append(log, applied{kind: Taint, providerID: a.ProviderID})
		case Launch:
			id, err := e.Cluster.Launch(a.InstanceType)
			if err != nil {
				return Proceed, err
			}
			replacement = id
			e.Cluster.LaunchesInFlight++
			log = append(log, applied{kind: Launch, providerID: id})
		case Evict:
			if err := e.Cluster.Evict(a.ProviderID, replacement); err != nil {
				return Proceed, err
			}
			evicted++
			appliedIrreversible = true
			log = append(log, applied{kind: Evict, providerID: a.ProviderID})
		case Delete:
			if err := e.Cluster.Delete(a.ProviderID); err != nil {
				return Proceed, err
			}
			e.Cluster.Budget--
			appliedIrreversible = true
			log = append(log, applied{kind: Delete, providerID: a.ProviderID})
		}
		confirmed[i] = true
	}

	e.Proceeded++
	return Proceed, nil
}

// resolve turns a precondition failure into an outcome and carries it out. The
// cut lands at the first irreversible action, which Decide reads off
// appliedIrreversible.
func (e *MemoryExecutor) resolve(p Proposal, cause error, log []applied, appliedIrreversible bool, evicted int) (Outcome, error) {
	e.Failures = append(e.Failures, cause)
	switch Decide(appliedIrreversible) {
	case Rollback:
		// Undo in reverse, which matters for a taint-then-launch pair only in
		// that it is the order that reads correctly in the log.
		for i := len(log) - 1; i >= 0; i-- {
			switch log[i].kind {
			case Taint:
				e.Cluster.untaint(log[i].providerID)
			case Launch:
				delete(e.Cluster.Nodes, log[i].providerID)
			}
		}
		e.RolledBack++
		return Rollback, nil
	default:
		// Forward. Finish the actions that get the pods running and skip the
		// rest. Deleting the source is what releases its remaining pods to be
		// rescheduled, so it stays; a further Launch would buy capacity the
		// proposal never priced, so it does not.
		for _, a := range p.Actions {
			switch a.Kind {
			case Evict:
				_ = e.Cluster.Evict(a.ProviderID, "")
				evicted++
			case Delete:
				if err := e.Cluster.Delete(a.ProviderID); err == nil {
					e.Cluster.Budget--
				}
			}
		}
		e.Forwarded++
		e.Forward.Nodes += len(p.Candidates)
		e.Forward.Pods += evicted
		// Charge the unplanned work. A budget that does not see forward work
		// lets a burst of failures exceed the limit with every single decision
		// inside it.
		e.Cluster.Budget -= len(p.Candidates)
		return Forward, nil
	}
}

// -----------------------------------------------------------------------------
// The pipeline
// -----------------------------------------------------------------------------

// RunConfig is every knob the four stages take. Nothing here is read from the
// cluster, so a configuration is reproducible against a seed.
type RunConfig struct {
	// K is the stage-1 threshold denominator. Stage 1 keeps a node whose delete
	// ratio is at least 1/K.
	K float64

	// Threshold is the stage-2 approval floor on Score. It is 1/K in the
	// balanced formulation, and separating them is what lets a run measure how
	// much stage 1 discards that stage 2 would have rejected anyway.
	Threshold float64

	// Budget is nodes disruptable this pass.
	Budget int

	Bands BandConfig

	// NominateEvery marks every nth node as nominated for a pending pod. This is
	// cluster state a proposal cannot anticipate, and it is the cheapest way to
	// make stage 4 reject work stages 1 and 2 thought was legal. Zero disables
	// it.
	NominateEvery int

	// PDBMinAvailableFraction is the share of a workload's pods its PDB requires
	// healthy. It is a stage-3 input because a PDB is cluster state, and indexing
	// one means walking the pods.
	PDBMinAvailableFraction float64

	// RecomputePDB makes the PDB check derive current availability from the nodes
	// instead of reading the object's status. Both answers agree on a fresh index
	// and their costs differ by the cluster size, which is what readcost_test.go
	// reports.
	RecomputePDB bool
}

func DefaultRunConfig(k float64, budget int) RunConfig {
	return RunConfig{
		K:         k,
		Threshold: 1 / k,
		Budget:    budget,
		Bands: BandConfig{
			SourceCeiling:    0.50,
			DestinationFloor: 0.60,
			FullCeiling:      0.95,
		},
		NominateEvery: 50,
		// Half a workload must stay up. High enough that a PDB refuses something
		// and low enough that it does not refuse everything.
		PDBMinAvailableFraction: 0.5,
	}
}

// Result is one pass through the pipeline. The first number the paper needs is
// CandidatesOut over NodesIn.
type Result struct {
	NodesIn       int
	CandidatesOut int
	Destinations  int

	ProposalsGenerated int
	Committed          int
	Conflicts          int

	Applied     int
	Proceeded   int
	RolledBack  int
	Forward     int
	ForwardCost ForwardCost

	// SavingsProposed is the claimed hourly savings of the committed proposals.
	// Stage 2 emits one move per candidate, so summing generated proposals would
	// not double count, but it would count work the budget refused.
	SavingsProposed float64

	// SavingsRealized is measured from the cluster: pool cost before minus pool
	// cost after. It charges for launched replacements and gives no credit for a
	// move that went forward without delivering, which is what makes it differ
	// from SavingsProposed.
	SavingsRealized float64

	PoolCostBefore float64
	PoolCostAfter  float64

	// PendingAfter are pods a move stranded. A pass that realizes savings by
	// leaving pods homeless has not saved anything.
	PendingAfter int

	BudgetLeft int

	// NodeReads is nodes read per precondition across the whole pass. It is the
	// cost the locality split is about, measured rather than asserted.
	NodeReads map[PreconditionKind]int

	IdentifyNanos int64
	GenerateNanos int64
	CompactNanos  int64
	ExecuteNanos  int64
}

// Run executes all four stages once and returns what each one did.
//
// The stage boundary is visible in the argument lists below. Stages 1 and 2 get
// nodes, totals, and the catalog. The Cluster is not built until stage 3.
func Run(ctx context.Context, nodes []NodeSnapshot, totals PoolTotals, cat *Catalog, cfg RunConfig) Result {
	res := Result{NodesIn: len(nodes), PoolCostBefore: totals.Cost}

	// Stage 1. One bit per node, no cluster.
	t0 := time.Now()
	bits := Identify(nodes, totals, cfg.K)
	res.IdentifyNanos = time.Since(t0).Nanoseconds()
	for _, b := range bits {
		if b {
			res.CandidatesOut++
		}
	}

	// Stage 2. Destination set once, then one generator call per candidate.
	t1 := time.Now()
	dests := EligibleDestinations(nodes, cat, totals, cfg.Bands)
	res.Destinations = len(dests)
	gen := ConsolidationGenerator{Threshold: cfg.Threshold}
	var proposals []Proposal
	for i, n := range nodes {
		if !bits[i] {
			continue
		}
		proposals = append(proposals, gen.Generate(n, withoutNode(dests, n.ProviderID), cat, totals)...)
	}
	res.GenerateNanos = time.Since(t1).Nanoseconds()
	res.ProposalsGenerated = len(proposals)

	// Stage 3. Cluster state enters here.
	cluster := NewCluster(nodes, cat, cfg.Budget)
	markNominated(cluster, nodes, cfg.NominateEvery)
	// Indexing walks every pod, which is the PDB controller's cost. It runs here
	// rather than inside a checker so the checker's read count is the eviction
	// path's cost and not the controller's.
	cluster.IndexPDBs(cfg.PDBMinAvailableFraction)
	t2 := time.Now()
	committed, conflicts := RankingCompactor{}.SelectCounting(ctx, proposals, cfg.Budget)
	res.CompactNanos = time.Since(t2).Nanoseconds()
	res.Committed = len(committed)
	res.Conflicts = conflicts
	for _, p := range committed {
		res.SavingsProposed += p.Savings
	}

	// Stage 4.
	exec := NewMemoryExecutor(cluster)
	if pdb, ok := exec.Checkers[PDBAllows].(*PDBChecker); ok {
		pdb.Recompute = cfg.RecomputePDB
	}
	t3 := time.Now()
	for _, p := range committed {
		if _, err := exec.Apply(ctx, p); err != nil {
			// A mechanism fault, not a precondition failure. Stop rather than
			// average it away.
			break
		}
		res.Applied++
	}
	res.ExecuteNanos = time.Since(t3).Nanoseconds()
	res.NodeReads = exec.NodeReads

	res.Proceeded = exec.Proceeded
	res.RolledBack = exec.RolledBack
	res.Forward = exec.Forwarded
	res.ForwardCost = exec.Forward
	res.PoolCostAfter = cluster.Cost()
	res.SavingsRealized = res.PoolCostBefore - res.PoolCostAfter
	res.PendingAfter = len(cluster.Pending)
	res.BudgetLeft = cluster.Budget
	return res
}

// withoutNode drops the source from its own destination set. A node cannot
// receive the pods it is being drained of, and EligibleDestinations has no way
// to know which source it is being asked about.
func withoutNode(dests []NodeSnapshot, providerID string) []NodeSnapshot {
	out := make([]NodeSnapshot, 0, len(dests))
	for _, d := range dests {
		if d.ProviderID != providerID {
			out = append(out, d)
		}
	}
	return out
}

// markNominated seeds the model's nominations. It runs in stage 3 because
// nomination is cluster state, and putting it on a NodeSnapshot would hand
// stages 1 and 2 something they must not see.
//
// The pattern is positional rather than random, so it needs no second random
// stream and reproduces from the node ordering alone.
func markNominated(c *Cluster, nodes []NodeSnapshot, every int) {
	if every <= 0 {
		return
	}
	for i, n := range nodes {
		if i%every == every-1 {
			c.Nominated[n.ProviderID] = true
		}
	}
}
