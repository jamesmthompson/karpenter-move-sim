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
// proposals stop on PodsStillFit is that gap being measured rather than a bug in
// the checker.
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
		// The whole candidate set or none of it. That is what a gang move requires,
		// since committing four of its five nodes takes the workload down and
		// recovers four nodes' price for a five-node outage, and it is what this
		// comparison already did for every batching path. Charging the budget per
		// node and committing what fits would half-consolidate the gang.
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

	// Launched marks a node this pass created, which is what distinguishes an
	// extra node the pass is answerable for from one it inherited.
	Launched bool
}

// Cluster is the state stages 1 and 2 were denied and stages 3 and 4 are given.
// It is a map and two counters, which is the point: the state that flips a good
// move into an illegal one is small, and gating on it does not require
// re-deriving the cluster's scheduling.
type Cluster struct {
	Catalog *Catalog
	Nodes   map[string]*ClusterNode

	// Placement is the scheduler scoring strategy that decides which of the nodes
	// a pod fits on it actually lands on. It is one field on the cluster rather
	// than an argument to each placement, because it is a configuration of the
	// scheduler and not a property of a pod: a cluster where half the pods spread
	// and half pack is not a cluster anyone runs, and a per-call choice would let
	// the feasibility check and the executor answer differently about the same pod.
	//
	// Nil is first-fit, so a Cluster built by NewCluster and never configured
	// places pods the way this simulator always did. See placement.go.
	Placement PlacementStrategy

	// Nominated nodes are reserved for a pending pod the scheduler already
	// aimed at them. Draining one strands that pod.
	Nominated map[string]bool

	// Budget is nodes still disruptable this window under reason Underutilized.
	// It is the field every existing caller reads, so it stays the underutilized
	// counter rather than becoming an aggregate.
	Budget int

	// EmptyBudget is the same allowance for reason Empty. Upstream builds a
	// disruption budget mapping per method, inside disrupt(), so Emptiness and the
	// two consolidation methods never share a decremented counter. That is the
	// accounting the comment in consolidation.ShouldDisrupt is about: "Empty nodes
	// are handled by Emptiness (reason \"Empty\") for correct budget accounting."
	//
	// Stage 3 has accounted per reason since selectWithBudgets. Stage 4 did not,
	// and with only single-node proposals in existence nothing ever showed it: one
	// empty delete and one underutilized delete each cost 1 and the shared counter
	// was 20. A 20-node multi-node batch spends the whole allowance, and then the
	// empty batch stopped on a budget upstream would not have charged it.
	EmptyBudget int

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

	// LaunchFails decides which launches the cloud provider refuses. Nil is a
	// provider with capacity for everything, which is what every existing run
	// assumes.
	//
	// It is a predicate on the cluster rather than a property of the catalog,
	// because launch failure is not a property of an instance type. The same type
	// launches now and fails in a minute when the zone runs out, and the thing the
	// atomic path has to be tested against is exactly that: a set of N launches
	// where some succeed and some do not. See FailLaunchesOf and FailLaunchAfter.
	LaunchFails func(*cloudprovider.InstanceType) bool

	// Totals and Threshold are what ScoreStillClears compares against. They enter
	// with the cluster because stage 3 is the first stage holding both: the pass's
	// denominators and the approval floor stage 2 used.
	//
	// A zero Threshold is no floor, so a cluster built without these has an inert
	// score check rather than a wrong one. Setting Threshold without Totals is the
	// combination that fails loudly, which is the right way round.
	Totals    PoolTotals
	Threshold float64

	launchSeq int
}

// NewCluster materializes the model from the same snapshots stages 1 and 2 read.
//
// It does not fill in Totals or Threshold. Both are stage-3 inputs the caller
// already holds, and summing the pool here would put Totals' adversarial price
// guard on a constructor that has no other reason to reject a cluster.
// EmptyBudget is seeded to the same allowance, which is what a per-method mapping
// gives each method when the NodePool declares one budget for all reasons.
func NewCluster(nodes []NodeSnapshot, cat *Catalog, budget int) *Cluster {
	c := &Cluster{
		Catalog:     cat,
		Nodes:       make(map[string]*ClusterNode, len(nodes)),
		Nominated:   map[string]bool{},
		Budget:      budget,
		EmptyBudget: budget,
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

// Taint cordons a node.
func (c *Cluster) Taint(providerID string) error {
	n, ok := c.Nodes[providerID]
	if !ok || n.Deleted {
		return fmt.Errorf("taint %s: node is gone", providerID)
	}
	n.Tainted = true
	return nil
}

// Launch adds a node of the given type.
func (c *Cluster) Launch(it *cloudprovider.InstanceType) (string, error) {
	if it == nil {
		return "", errors.New("launch: nil instance type")
	}
	price, ok := cheapestOffering(it)
	if !ok {
		return "", fmt.Errorf("launch %s: no available offering", it.Name)
	}
	if c.LaunchFails != nil && c.LaunchFails(it) {
		return "", &LaunchFailure{InstanceType: it.Name}
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

// Release gives back a node this pass launched and nothing has used yet.
//
// It removes the entry instead of marking it Deleted, because a released node is
// not a node the cluster once had. Nothing was scheduled onto it, so leaving a
// tombstone behind would report a deletion that no workload experienced and would
// make an abandoned acquisition look like a disruption.
//
// It refuses a node holding pods, which is the boundary between this and the
// eviction rollback this simulator does not have. A node with a pod on it has been
// used, and giving it back means moving that pod, which is a disruption and not a
// release. Reaching that state is a mechanism fault: the executor only releases
// launches whose dependent evictions have not fired.
//
// launchSeq is not rewound. A later launch reusing a released ID would make two
// different nodes indistinguishable in a trace.
func (c *Cluster) Release(providerID string) error {
	n, ok := c.Nodes[providerID]
	if !ok {
		return fmt.Errorf("release %s: node is gone", providerID)
	}
	if !n.Launched {
		return fmt.Errorf("release %s: node was not launched by this pass", providerID)
	}
	if len(n.Pods) > 0 {
		return fmt.Errorf("release %s: node holds %d pods, releasing it would be a disruption", providerID, len(n.Pods))
	}
	delete(c.Nodes, providerID)
	return nil
}

// Evict removes one pod from a node and places it wherever it fits. A pod with
// nowhere to land becomes pending, which is the state provisioning already exists
// to resolve.
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

// placement is the scheduler stand-in: score every untainted node the pod fits
// on and take the best, with the proposal's own replacement preferred so a
// REPLACE lands its pods where it planned to.
//
// Iteration is over a sorted key list because Go map order is random, and the
// order still matters even with scoring: the scorers truncate to integers, so
// most candidates tie, and Pick keeps the lowest index on a tie. A simulator that
// reorders placements between runs is not reproducible.
//
// The preference is checked before scoring rather than folded into it. A REPLACE
// bought a specific node for these specific pods, and a strategy that sent them
// elsewhere would leave the node it just launched empty and make the proposal's
// own savings arithmetic wrong. That is a plan the executor is carrying out, not
// a placement decision it is making.
func (c *Cluster) placement(pod *corev1.Pod, exclude, prefer string) (*ClusterNode, bool) {
	need := podNeeds(pod)
	if prefer != "" {
		if n, ok := c.Nodes[prefer]; ok && !n.Deleted && !n.Tainted && fitsInto(c.free(n), need) {
			return n, true
		}
	}
	candidates := make([]*ClusterNode, 0, len(c.Nodes))
	slots := make([]Slot, 0, len(c.Nodes))
	for _, id := range c.sortedIDs() {
		n := c.Nodes[id]
		if id == exclude || n.Deleted || n.Tainted {
			continue
		}
		candidates = append(candidates, n)
		slots = append(slots, c.slotFor(n))
	}
	i, ok := Pick(c.strategy(), slots, need)
	if !ok {
		return nil, false
	}
	return candidates[i], true
}

// strategy is the cluster's placement strategy, defaulting to first fit.
func (c *Cluster) strategy() PlacementStrategy {
	if c.Placement == nil {
		return FirstFit()
	}
	return c.Placement
}

// slotFor reduces a standing node to what scoring needs.
func (c *Cluster) slotFor(n *ClusterNode) Slot {
	alloc := corev1.ResourceList{}
	if n.InstanceType != nil {
		alloc = c.Catalog.Effective(n.InstanceType)
	}
	return Slot{Free: c.free(n), Allocatable: alloc}
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
		ScoreStillClears:  &ScoreStillClearsChecker{Cluster: c},
	}
}

// ScoreStillClearsChecker recomputes the proposal's score against current inputs
// and rejects it if the score no longer clears the threshold.
//
// This is the check that lets stage 4 stop a move instead of finishing one that
// stopped paying. A move whose value decayed below the floor never fires its first
// action, so there is no partly-applied move that has to be completed as an
// obligation.
//
// What it reads is the point. The numerator's inputs are local: this node's pods,
// which decide its disruption cost, and its price, which is a label on the node.
// One node read per candidate, flat in cluster size.
//
// The denominators are not local. Pool cost and pool disruption cost are sums over
// every node in the pool, and no status field carries either. This checker does not
// derive them. It uses Cluster.Totals, which stage 3 was handed and which stages 1
// and 2 already scored against, so the recheck compares a fresh numerator against
// the same denominators the original approval used. That is deliberate and it is
// not free of consequence: a pass that deletes nodes shrinks the real pool cost,
// so late in a pass the denominators are stale by the savings the pass has already
// realized. Refreshing them each time would cost one read of every node in the
// pool per proposal, which is the cost this design exists to avoid, and it would
// answer a question about one node by re-reading the cluster.
type ScoreStillClearsChecker struct {
	Cluster *Cluster

	reads
}

func (k *ScoreStillClearsChecker) Kind() PreconditionKind { return ScoreStillClears }

func (k *ScoreStillClearsChecker) Check(_ context.Context, p Proposal) error {
	if p.Score == 0 && len(p.Candidates) == 0 {
		// A pure launch carries no score. Nothing to compare.
		return nil
	}
	r := k.begin(k.Cluster)

	savings := 0.0
	live := 0
	var evicted []*corev1.Pod
	for _, cand := range p.Candidates {
		n, ok := r.Node(cand.ProviderID)
		if !ok || n.Deleted {
			// The node this move was about is gone, so the move is not worth
			// anything. Whether that is a violation is decided by the total below,
			// which is zero if every candidate vanished.
			continue
		}
		k.evals++
		savings += n.Price
		live++
		evicted = append(evicted, n.Pods...)
	}
	// The recheck prices the whole evicted set at once, which is how stage 2 priced
	// it. Summing per-node costs here would charge a gang spanning two candidates
	// twice, so a move stage 2 approved could fail its own recheck with nothing in
	// the cluster having changed.
	disruption := MoveDisruptionCost(live, evicted)
	// A REPLACE recovers the price difference, not the full price. Stage 2 priced
	// the replacement from the catalog and prices do not move within a pass, so the
	// replacement's price is subtracted from what the candidates give back.
	for _, it := range p.Replacements {
		if it == nil {
			continue
		}
		if price, ok := cheapestOffering(it); ok {
			savings -= price
		}
	}

	score := Score(savings, disruption, k.Cluster.Totals)
	if score < k.Cluster.Threshold {
		return fmt.Errorf("score fell to %.4f, below the %.4f floor (was %.4f)",
			score, k.Cluster.Threshold, p.Score)
	}
	return nil
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
	var scratch []Slot
	// Replacements the executor has already launched are standing nodes, picked up
	// by the loop below. Counting them here as well would approve a move against
	// capacity the cluster does not have. Launch actions fire in DAG order, so the
	// ones already up are a prefix of Replacements.
	for i, it := range p.Replacements {
		if it == nil || i < k.Cluster.LaunchesInFlight {
			continue
		}
		eff := k.Cluster.Catalog.Effective(it)
		// A replacement is empty, so free is its whole effective capacity. Free is
		// a copy, because the scratch list is mutated as pods are placed and the
		// allocatable denominator must not move with it.
		scratch = append(scratch, Slot{Free: eff.DeepCopy(), Allocatable: eff})
	}
	for _, id := range k.Cluster.sortedIDs() {
		n := k.Cluster.Nodes[id]
		if n.Deleted || n.Tainted || draining[id] {
			continue
		}
		scratch = append(scratch, k.Cluster.slotFor(n))
	}

	// The same strategy the executor will use. These two used to be one policy by
	// accident, both being first fit over sorted IDs. Now that placement is a
	// choice, a checker asking a different question than the executor answers
	// would approve moves that then strand pods, and refuse moves that would have
	// worked.
	strategy := k.Cluster.strategy()

	for _, cand := range p.Candidates {
		n, ok := k.Cluster.Nodes[cand.ProviderID]
		if !ok {
			continue
		}
		for _, pod := range n.Pods {
			need := podNeeds(pod)
			i, ok := Pick(strategy, scratch, need)
			if !ok {
				return fmt.Errorf("pod %s has nowhere to go", pod.Name)
			}
			scratch[i].claim(need)
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
//
// What it counts is the deletions still to come, not the proposal's whole
// candidate list. The two are the same for a single-node move and they are not for
// a batch, because the executor decrements the budget on each Delete and this runs
// again before the next action. Counting the whole batch made a batch of N stop on
// its own first Delete, every time, as soon as the budget was exactly N: 20 taints
// and 49 evictions fired, then the budget fell to 19, then the check refused the
// remaining 19 deletions of a move it had already approved. The emptiness and
// multi-node paths both emit batches, so this only became reachable when they did.
type BudgetHasRoomChecker struct{ Cluster *Cluster }

func (k BudgetHasRoomChecker) Kind() PreconditionKind { return BudgetHasRoom }

func (k BudgetHasRoomChecker) Check(_ context.Context, p Proposal) error {
	need := 0
	for _, cand := range p.Candidates {
		n, ok := k.Cluster.Nodes[cand.ProviderID]
		if !ok || n.Deleted {
			continue
		}
		need++
	}
	if need == 0 {
		// A launch removes nothing, and neither does a batch already applied.
		return nil
	}
	if have := k.Cluster.budgetFor(p.Reason); have < need {
		return fmt.Errorf("%s budget has %d, move needs %d more deletions", p.Reason, have, need)
	}
	return nil
}

// budgetFor returns the counter a proposal's reason draws on.
func (c *Cluster) budgetFor(r Reason) int {
	if r == ReasonEmpty {
		return c.EmptyBudget
	}
	return c.Budget
}

// spendBudget charges one deletion to the reason's counter.
func (c *Cluster) spendBudget(r Reason) {
	if r == ReasonEmpty {
		c.EmptyBudget--
		return
	}
	c.Budget--
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
// apply loop and not of any one proposal: how often a move runs to the end, and
// how often a precondition stopped it partway.
type MemoryExecutor struct {
	Cluster  *Cluster
	Checkers map[PreconditionKind]Checker

	Proceeded int
	Stopped   int

	// ActionsApplied is how many actions fired inside proposals that stopped. A
	// stop after zero actions costs nothing; a stop after an Evict left a pending
	// pod. The two are different events and one counter cannot tell them apart.
	ActionsApplied int

	// Failures records the violation behind each stop, in apply order. StopCauses
	// aggregates them, since a list of errors is not a number anyone reports.
	Failures []error

	// StopCauses is stops per precondition. A pass where every stop is
	// do-not-disrupt is a different pass from one where every stop is a capacity
	// shortfall, and without this they print identically.
	StopCauses map[PreconditionKind]int

	// NodeReads is nodes read per precondition, summed over every Check the pass
	// ran. It accumulates across proposals, so it is the pass's total cost for that
	// constraint and not one proposal's.
	NodeReads map[PreconditionKind]int

	// Abandoned is AtomicLaunch proposals whose launch set could not be completed.
	// It is separate from Stopped because it is a different event: no precondition
	// failed and nothing was left behind, the provider just did not have the
	// capacity the set asked for.
	Abandoned int

	// LaunchesReleased is nodes given back by abandoned proposals, summed over the
	// pass. It is the size of the partial acquisition all-or-nothing prevented, and
	// it is the number that says whether the mechanism ever fires.
	LaunchesReleased int
}

func NewMemoryExecutor(c *Cluster) *MemoryExecutor {
	return &MemoryExecutor{
		Cluster:    c,
		Checkers:   LocalCheckers(c),
		StopCauses: map[PreconditionKind]int{},
		NodeReads:  map[PreconditionKind]int{},
	}
}

// Apply walks the action DAG in order.
//
// A precondition failure is an outcome and not an error. The error return is for
// faults in the mechanism itself: an action released before its prerequisites
// confirmed, a missing checker, a write against a node that vanished. Conflating
// the two would make a legal stop indistinguishable from a bug.
//
// A launch the provider refuses is the third case, and what happens next is the
// one thing AtomicLaunch changes.
//
// Incremental, which is the default and what every proposal did before the flag
// existed: the refusal comes back as an error and whatever launched stays. A
// partial launch set is capacity the pass bought and the next pass reclaims.
//
// All-or-nothing: release every node this proposal launched and return Abandoned.
// The proposal delivers nothing and evicts nothing. That the release is safe rests
// on the DAG, not on timing: validateAtomicLaunch has already established that
// every Evict and every Delete waits on every Launch, so a refusal reaches this
// point with the launched nodes empty and nothing depending on them. This is the
// four-node state never being committed. It is not the eviction rollback this
// simulator does not have, which would have had to move pods that were already
// somewhere.
func (e *MemoryExecutor) Apply(ctx context.Context, p Proposal) (Outcome, error) {
	if p.AtomicLaunch {
		if err := validateAtomicLaunch(p); err != nil {
			return Proceed, err
		}
	}

	confirmed := map[int]bool{}
	fired := 0
	// replacement is the node this proposal launched, so its own evictions land
	// on the capacity it bought.
	replacement := ""
	// launched is every node this proposal created, in launch order. It is the
	// release list for an atomic proposal and nothing reads it otherwise.
	var launched []string

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
			// Stop here. Nothing is undone, because each action already applied
			// left a state the cluster handles on its own terms, and unwinding
			// would be a second mechanism doing what the first does next pass.
			//
			// This is a stop and not an abandonment even on an atomic proposal. A
			// precondition that flipped is the cluster saying the rest of the move
			// is illegal, which is a statement about the move and not about whether
			// the launch set could be completed. The launches that fired stay, for
			// the same reason they stay on any other stop.
			e.Failures = append(e.Failures, err)
			var v *PreconditionViolation
			if errors.As(err, &v) {
				e.StopCauses[v.Kind]++
			}
			e.Stopped++
			e.ActionsApplied += fired
			return Stopped, nil
		}

		switch a.Kind {
		case Taint:
			if err := e.Cluster.Taint(a.ProviderID); err != nil {
				return Proceed, err
			}
		case Launch:
			id, err := e.Cluster.Launch(a.InstanceType)
			if err != nil {
				var refused *LaunchFailure
				if p.AtomicLaunch && errors.As(err, &refused) {
					return e.abandon(p, launched)
				}
				return Proceed, err
			}
			launched = append(launched, id)
			replacement = id
			e.Cluster.LaunchesInFlight++
		case Evict:
			if err := e.Cluster.Evict(a.ProviderID, replacement); err != nil {
				return Proceed, err
			}
		case Delete:
			if err := e.Cluster.Delete(a.ProviderID); err != nil {
				return Proceed, err
			}
			e.Cluster.spendBudget(p.Reason)
		}
		fired++
		confirmed[i] = true
	}

	e.Proceeded++
	return Proceed, nil
}

// abandon gives back the launches an atomic proposal could not complete and
// reports the outcome.
//
// Nothing is refunded to the disruption budget, because nothing was charged. The
// budget is spent on Delete, and validateAtomicLaunch has established that every
// Delete in an atomic proposal waits on every Launch, so a proposal that failed a
// launch has deleted nothing. A refund path here would be dead code that looked
// like it was doing something.
//
// A release that fails is a mechanism fault. Release refuses a node holding pods
// or a node the pass did not launch, and neither can be true of a node reached
// this way, so hitting one means the DAG validation or the launch bookkeeping is
// wrong and averaging it into an outcome would hide that.
func (e *MemoryExecutor) abandon(p Proposal, launched []string) (Outcome, error) {
	for _, id := range launched {
		if err := e.Cluster.Release(id); err != nil {
			return Proceed, fmt.Errorf("abandoning %s: %w", p.ID, err)
		}
		e.LaunchesReleased++
	}
	e.Cluster.LaunchesInFlight = 0
	e.Abandoned++
	return Abandoned, nil
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

	// StopAfterFirstPath returns after the first of the three paths that produces
	// anything, which is upstream's Reconcile: "We'll only let one method perform
	// an action," then requeue immediately. A pass with this off runs all three,
	// so one pass reports what three consecutive reconciles would do. That is the
	// form a savings-per-pass measurement needs, and it is the divergence.
	StopAfterFirstPath bool

	// SingleNodeMaxProposals caps how many proposals the single-node path emits.
	// Upstream returns after the first valid command; zero here means no cap. See
	// SingleNodeEvaluation.MaxProposals.
	SingleNodeMaxProposals int

	// Placement is the scheduler scoring strategy evicted pods are placed under,
	// and the one PodsStillFit checks feasibility against.
	//
	// It is a stage-3 input, because placement takes the cluster and stages 1 and 2
	// do not have it. What it changes is where waste ends up, which decides how much
	// of it a later pass can reclaim. The empty value is first-fit, which is what
	// this simulator did before the choice existed. See placement.go.
	Placement PlacementStrategyName
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
		// First fit, named rather than inherited.
		//
		// It is the default on fidelity grounds and not for continuity. Placement
		// here is the consolidation reschedule, and Karpenter does that itself in
		// addToExistingNode: the lowest-indexed existing node the pod can be added
		// to, over a list sorted initialized-first then by node name. It does not
		// run the kube scheduler's scoring plugins over those nodes. So first fit
		// over sorted IDs is what the controller under study does, and the two
		// allocation strategies are what the scheduler did to the pods before
		// consolidation ever saw them, which is the upstream condition the entropy
		// measurement varies.
		Placement: PlacementFirstFit,
	}
}

// Result is one pass through the pipeline. The first number the paper needs is
// CandidatesOut over CandidatesIn.
type Result struct {
	NodesIn int

	// CandidatesIn is the decision units the node list grouped into, and
	// CandidatesOut how many survived stage 1. They are units and not nodes: a gang
	// is one candidate however many nodes hold its members, which is why
	// CandidatesOut can exceed neither CandidatesIn nor, on a gangless fleet, NodesIn.
	CandidatesIn  int
	CandidatesOut int

	// GangCandidatesOut is the share of CandidatesOut that are gangs. Zero on a
	// fleet with no gang labels, which is every fixture that does not ask for them.
	GangCandidatesOut int

	Destinations int

	ProposalsGenerated int
	Committed          int
	Conflicts          int

	Applied   int
	Proceeded int

	// Stopped is committed proposals a precondition halted partway. What was
	// already applied stayed applied, so a stop is not a no-op and the actions
	// behind it are counted separately.
	Stopped int

	// StoppedActions is how many actions fired inside the stopped proposals.
	StoppedActions int

	// Abandoned is AtomicLaunch proposals that could not complete their launch
	// set, and LaunchesReleased is the nodes they gave back. Both are zero in a
	// pass with no atomic proposals and no launch failures, which is every pass
	// the consolidation paths produce today.
	Abandoned        int
	LaunchesReleased int

	// StopCauses is which precondition did the stopping, counted per kind. A pass
	// that stops ten moves on capacity is a different pass from one that stops ten
	// on do-not-disrupt.
	StopCauses map[PreconditionKind]int

	// SavingsProposed is the claimed hourly savings of the committed proposals.
	// Stage 2 emits one move per candidate, so summing generated proposals would
	// not double count, but it would count work the budget refused.
	SavingsProposed float64

	// SavingsRealized is measured from the cluster: pool cost before minus pool
	// cost after. It charges for launched replacements and gives no credit for a
	// move that stopped before deleting its candidate, which is what makes it
	// differ from SavingsProposed.
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

	// Paths is what each of the three evaluations contributed. The interaction
	// between them is what decides a cluster's outcome, so a pass that reports
	// only a total cannot say which path produced it.
	Paths map[Path]PathStats

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

	// Stage 1. One bit per candidate, no cluster.
	//
	// The candidate is the decision unit: a node for ordinary pods, a gang for gang
	// pods. Candidates() builds them by grouping the same []NodeSnapshot on the gang
	// label, so stage 1 reads its own argument and gains no handle on anything else.
	//
	// The bit is the single-node path's filter at the candidate's scale: Disruptable
	// over every node the move acts on, and the delete ratio computed once. It is
	// reported rather than used to gate stage 2, because two of the three node paths
	// do not apply it. Emptiness never scores at all, and multi-node has no
	// CanPassThreshold, since a node that cannot pass alone can still ride in a batch
	// that does. Feeding this output to all the paths would deny multi-node
	// candidates upstream gives it, and would drop empty nodes whose delete ratio is
	// high but whose pool has a large denominator. The gang path does apply it, in
	// gangMove.
	t0 := time.Now()
	cands := Candidates(nodes)
	bits := CandidateBits(cands, totals, cfg.K)
	res.IdentifyNanos = time.Since(t0).Nanoseconds()
	res.CandidatesIn = len(cands)
	for i, b := range bits {
		if !b {
			continue
		}
		res.CandidatesOut++
		if cands[i].IsGang() {
			res.GangCandidatesOut++
		}
	}

	// Stage 2. Destination set once, then the three paths in upstream's order.
	t1 := time.Now()
	dests := EligibleDestinations(nodes, cat, totals, cfg.Bands)
	res.Destinations = len(dests)
	proposals, paths := EvaluateInOrder(nodes, dests, cat, totals, cfg)
	res.GenerateNanos = time.Since(t1).Nanoseconds()
	res.ProposalsGenerated = len(proposals)
	res.Paths = paths

	// Stage 3. Cluster state enters here.
	cluster := NewCluster(nodes, cat, cfg.Budget)
	// The denominators and the floor stage 2 approved against, carried forward so
	// ScoreStillClears compares a fresh numerator against the same scale rather
	// than re-summing the pool per proposal.
	cluster.Totals = totals
	cluster.Threshold = cfg.Threshold
	// One strategy for the pass, read by both the executor and PodsStillFit, so
	// feasibility is decided against the placement that will actually happen.
	cluster.Placement = cfg.Placement.MustStrategy()
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
	res.Stopped = exec.Stopped
	res.StoppedActions = exec.ActionsApplied
	res.Abandoned = exec.Abandoned
	res.LaunchesReleased = exec.LaunchesReleased
	res.StopCauses = exec.StopCauses
	res.PoolCostAfter = cluster.Cost()
	res.SavingsRealized = res.PoolCostBefore - res.PoolCostAfter
	res.PendingAfter = len(cluster.Pending)
	res.BudgetLeft = cluster.Budget
	return res
}

// StopCausesSorted lists the preconditions that stopped something, ordered by
// kind. Go map order is random, so a report built by ranging StopCauses would not
// reproduce between runs.
func (r Result) StopCausesSorted() []PreconditionKind {
	kinds := make([]PreconditionKind, 0, len(r.StopCauses))
	for k := range r.StopCauses {
		kinds = append(kinds, k)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	return kinds
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
