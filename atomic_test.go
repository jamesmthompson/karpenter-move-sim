package sim

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// twoLaunchReplace is a batch of two nodes replaced by two launches, with every
// eviction gated on both launches.
//
// It is built by hand because nothing generates this shape. See
// TestNoGeneratorEmitsAMultiLaunchProposal: the multi-node path launches zero or
// one node, so the flag has no producer yet and the mechanism is ahead of its use.
func twoLaunchReplace(batch []NodeSnapshot, its ...*cloudprovider.InstanceType) Proposal {
	var actions []Action
	taintAt := make([]int, len(batch))
	for i, c := range batch {
		taintAt[i] = len(actions)
		actions = append(actions, Action{Kind: Taint, ProviderID: c.ProviderID})
	}

	var launchAt []int
	for _, it := range its {
		launchAt = append(launchAt, len(actions))
		actions = append(actions, Action{Kind: Launch, InstanceType: it})
	}

	evictAt := make([][]int, len(batch))
	for i, c := range batch {
		for range c.ReschedulablePods {
			evictAt[i] = append(evictAt[i], len(actions))
			// Every launch, not just the last. That is what makes the launch set
			// releasable: no pod can have moved onto any of them yet.
			actions = append(actions, Action{Kind: Evict, ProviderID: c.ProviderID, DependsOn: launchAt})
		}
	}

	for i, c := range batch {
		deps := evictAt[i]
		if len(deps) == 0 {
			deps = []int{taintAt[i]}
		}
		actions = append(actions, Action{Kind: Delete, ProviderID: c.ProviderID, DependsOn: deps})
	}

	return Proposal{
		ID:            "atomic-test",
		Candidates:    batch,
		Replacements:  its,
		Savings:       1,
		Actions:       actions,
		Preconditions: []PreconditionKind{BudgetHasRoom},
		Reason:        ReasonUnderutilized,
	}
}

// atomicFixture is two loaded source nodes and the two nodes a move would buy.
func atomicFixture(t *testing.T) ([]NodeSnapshot, *Catalog, []*cloudprovider.InstanceType) {
	t.Helper()
	cat := BuildCatalog()
	batch := []NodeSnapshot{
		node(t, cat, "i-src1", "m7i.2xlarge", testPod("a1", 500, 1024), testPod("a2", 500, 1024)),
		node(t, cat, "i-src2", "m7i.2xlarge", testPod("b1", 500, 1024), testPod("b2", 500, 1024)),
	}
	its := []*cloudprovider.InstanceType{
		instanceType(t, cat, "m7i.large"),
		instanceType(t, cat, "m7i.xlarge"),
	}
	return batch, cat, its
}

func standingLaunched(c *Cluster) int {
	n := 0
	for _, node := range c.Nodes {
		if node.Launched && !node.Deleted {
			n++
		}
	}
	return n
}

func podsOn(c *Cluster, id string) int {
	if n, ok := c.Nodes[id]; ok {
		return len(n.Pods)
	}
	return 0
}

// The case the issue is about. An all-or-nothing proposal whose second launch is
// refused holds none of the capacity it asked for and has evicted nothing.
//
// The four-node state is never committed. The released node is a node no eviction
// was allowed to target, which is what distinguishes this from unwinding an
// eviction after the fact.
func TestAtomicLaunchReleasesAPartialAcquisition(t *testing.T) {
	batch, cat, its := atomicFixture(t)
	cluster := NewCluster(batch, cat, 10)
	// The first launch lands and the second is refused, which is the partial
	// acquisition. A predicate on the instance type name cannot produce this when a
	// set asks for several nodes of one type.
	cluster.LaunchFails = FailLaunchAfter(1)

	p := twoLaunchReplace(batch, its...)
	p.AtomicLaunch = true

	exec := NewMemoryExecutor(cluster)
	outcome, err := exec.Apply(context.Background(), p)
	if err != nil {
		t.Fatalf("abandoning a launch set is an outcome, not an error: %v", err)
	}
	if outcome != Abandoned {
		t.Fatalf("want Abandoned, got %v", outcome)
	}
	if exec.Abandoned != 1 {
		t.Errorf("want Abandoned=1, got %d", exec.Abandoned)
	}
	if exec.LaunchesReleased != 1 {
		t.Errorf("want 1 launch released, got %d", exec.LaunchesReleased)
	}
	// Zero nodes standing from the launch set. This is the assertion the issue
	// asks for: a partial provisioning failure leaves no nodes standing.
	if got := standingLaunched(cluster); got != 0 {
		t.Errorf("%d launched nodes still standing after abandonment, want 0", got)
	}
	// Nothing evicted. Every pod is where it started and nothing is pending.
	if got := podsOn(cluster, "i-src1"); got != 2 {
		t.Errorf("i-src1 holds %d pods, want 2", got)
	}
	if got := podsOn(cluster, "i-src2"); got != 2 {
		t.Errorf("i-src2 holds %d pods, want 2", got)
	}
	if len(cluster.Pending) != 0 {
		t.Errorf("%d pods pending after an abandonment that evicted nothing", len(cluster.Pending))
	}
	for _, id := range []string{"i-src1", "i-src2"} {
		if cluster.Nodes[id].Deleted {
			t.Errorf("%s was deleted by an abandoned proposal", id)
		}
	}
	// The taints fired before the launch set and they stay. A tainted node is a
	// node someone means to drain, which the cluster handles on its own terms, and
	// unwinding it would be the compensation path this simulator does not have.
	if !cluster.Nodes["i-src1"].Tainted {
		t.Error("the taint that fired was undone, which is not what abandonment does")
	}
	// A released node is not a node the cluster ever had, so it leaves no
	// tombstone and the pool cost is what it was.
	if got, want := cluster.Cost(), batch[0].Price+batch[1].Price; got != want {
		t.Errorf("pool cost %.4f after abandonment, want %.4f", got, want)
	}
}

// The same proposal without the flag. This is the incremental transaction, which
// is Omega's default and this simulator's: the launch that landed stays, because
// an extra node is an underutilized node and consolidation reclaims it.
func TestIncrementalLaunchKeepsAPartialAcquisition(t *testing.T) {
	batch, cat, its := atomicFixture(t)
	cluster := NewCluster(batch, cat, 10)
	cluster.LaunchFails = FailLaunchAfter(1)

	p := twoLaunchReplace(batch, its...)
	// AtomicLaunch left false, which is what every proposal in the simulator
	// carries today.

	exec := NewMemoryExecutor(cluster)
	outcome, err := exec.Apply(context.Background(), p)
	// Today's behavior: a refused launch on a non-atomic proposal comes back as an
	// error and the pass stops rather than being averaged away.
	if err == nil {
		t.Fatal("want the refused launch to surface as an error on a non-atomic proposal")
	}
	var refused *LaunchFailure
	if !errors.As(err, &refused) {
		t.Errorf("want a LaunchFailure, got %T: %v", err, err)
	}
	if outcome != Proceed {
		t.Errorf("a mechanism error carries no outcome, got %v", outcome)
	}
	// The partial acquisition stands. That is the difference the flag makes.
	if got := standingLaunched(cluster); got != 1 {
		t.Errorf("%d launched nodes standing, want the 1 that succeeded", got)
	}
	if exec.Abandoned != 0 || exec.LaunchesReleased != 0 {
		t.Errorf("a non-atomic proposal released %d launches over %d abandonments, want none",
			exec.LaunchesReleased, exec.Abandoned)
	}
}

// An atomic proposal whose launch set completes is an ordinary proposal. The flag
// costs nothing when the provider has capacity, and the evictions and deletions
// run the way they do without it.
func TestAtomicLaunchProceedsWhenEveryLaunchLands(t *testing.T) {
	batch, cat, its := atomicFixture(t)
	cluster := NewCluster(batch, cat, 10)
	// No LaunchFails, which is every existing run.

	p := twoLaunchReplace(batch, its...)
	p.AtomicLaunch = true

	exec := NewMemoryExecutor(cluster)
	outcome, err := exec.Apply(context.Background(), p)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if outcome != Proceed {
		t.Fatalf("want Proceed, got %v (stops: %v)", outcome, exec.StopCauses)
	}
	if exec.Abandoned != 0 || exec.LaunchesReleased != 0 {
		t.Errorf("a completed launch set released %d launches, want none", exec.LaunchesReleased)
	}
	if got := standingLaunched(cluster); got != 2 {
		t.Errorf("%d launched nodes standing, want 2", got)
	}
	for _, id := range []string{"i-src1", "i-src2"} {
		if !cluster.Nodes[id].Deleted {
			t.Errorf("%s survived a move that proceeded", id)
		}
	}
	if len(cluster.Pending) != 0 {
		t.Errorf("%d pods stranded by a move that proceeded", len(cluster.Pending))
	}
	// Two deletions against the underutilized allowance, which is the accounting a
	// completed move does and the abandoned one below does not.
	if cluster.Budget != 8 {
		t.Errorf("budget left %d after two deletions from 10, want 8", cluster.Budget)
	}
}

// Budget accounting on abandonment. Nothing is refunded because nothing is
// charged: the budget is spent on Delete, and an atomic proposal's Deletes all
// wait on its launch set, so a proposal that failed a launch has deleted nothing.
func TestAbandonedProposalChargesNoBudget(t *testing.T) {
	batch, cat, its := atomicFixture(t)
	cluster := NewCluster(batch, cat, 10)
	cluster.LaunchFails = FailLaunchAfter(1)

	p := twoLaunchReplace(batch, its...)
	p.AtomicLaunch = true

	exec := NewMemoryExecutor(cluster)
	if _, err := exec.Apply(context.Background(), p); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if cluster.Budget != 10 {
		t.Errorf("underutilized budget left %d after an abandonment, want the full 10", cluster.Budget)
	}
	if cluster.EmptyBudget != 10 {
		t.Errorf("empty budget left %d after an abandonment, want the full 10", cluster.EmptyBudget)
	}
	// The budget is intact, so the next proposal in the same pass gets the whole
	// allowance. Applying the same move against a provider with capacity proves it.
	cluster.LaunchFails = nil
	if _, err := exec.Apply(context.Background(), p); err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if cluster.Budget != 8 {
		t.Errorf("budget left %d after the retry deleted two nodes, want 8", cluster.Budget)
	}
	// LaunchesInFlight is cleared by the abandonment, or PodsStillFit on the next
	// proposal would discount capacity the released nodes no longer offer.
	if cluster.LaunchesInFlight != 0 {
		t.Errorf("LaunchesInFlight left at %d", cluster.LaunchesInFlight)
	}
}

// AcquireNodes is the shape #5's gang workload needs: N nodes, all or none, with
// no candidate to disrupt. A refusal partway leaves the cluster as it was found.
func TestAcquireNodesIsAllOrNothing(t *testing.T) {
	cat := BuildCatalog()
	standing := []NodeSnapshot{node(t, cat, "i-keep", "m7i.large", testPod("k1", 500, 1024))}
	cluster := NewCluster(standing, cat, 10)
	before := cluster.Cost()

	it := instanceType(t, cat, "m7i.2xlarge")
	// Five members, and the provider runs out after three.
	p := AcquireNodes("gang/five", it, it, it, it, it)
	cluster.LaunchFails = FailLaunchAfter(3)

	exec := NewMemoryExecutor(cluster)
	outcome, err := exec.Apply(context.Background(), p)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if outcome != Abandoned {
		t.Fatalf("want Abandoned, got %v", outcome)
	}
	if exec.LaunchesReleased != 3 {
		t.Errorf("released %d of the 3 nodes that landed", exec.LaunchesReleased)
	}
	if got := standingLaunched(cluster); got != 0 {
		t.Errorf("%d nodes standing from an abandoned acquisition, want 0", got)
	}
	if len(cluster.Nodes) != 1 {
		t.Errorf("cluster holds %d nodes, want the 1 it started with", len(cluster.Nodes))
	}
	if got := cluster.Cost(); got != before {
		t.Errorf("pool cost %.4f after an abandoned acquisition, want %.4f", got, before)
	}
	if podsOn(cluster, "i-keep") != 1 {
		t.Error("an acquisition touched a pod, which it has no action for")
	}

	// The whole set landing is the other half. Five nodes, and the value of them is
	// #5's question rather than this one's.
	cluster.LaunchFails = nil
	if outcome, err := exec.Apply(context.Background(), p); err != nil || outcome != Proceed {
		t.Fatalf("want Proceed with capacity available, got %v (%v)", outcome, err)
	}
	if got := standingLaunched(cluster); got != 5 {
		t.Errorf("%d nodes standing from a completed acquisition, want 5", got)
	}
	// An acquisition deletes nothing, so it draws on no disruption budget.
	if cluster.Budget != 10 {
		t.Errorf("an acquisition charged the disruption budget, leaving %d", cluster.Budget)
	}
}

// A single-launch proposal is trivially all-or-nothing already, so the flag is
// inert on one. Both settings produce the same cluster.
func TestAtomicLaunchIsInertOnASingleLaunch(t *testing.T) {
	cat := BuildCatalog()
	it := instanceType(t, cat, "m7i.large")

	for _, atomic := range []bool{false, true} {
		src := node(t, cat, "i-src", "m7i.2xlarge", testPod("p1", 500, 1024))
		cluster := NewCluster([]NodeSnapshot{src}, cat, 10)
		p := twoLaunchReplace([]NodeSnapshot{src}, it)
		p.AtomicLaunch = atomic

		exec := NewMemoryExecutor(cluster)
		outcome, err := exec.Apply(context.Background(), p)
		if err != nil {
			t.Fatalf("atomic=%v: apply: %v", atomic, err)
		}
		if outcome != Proceed {
			t.Fatalf("atomic=%v: want Proceed, got %v", atomic, outcome)
		}
		if got := standingLaunched(cluster); got != 1 {
			t.Errorf("atomic=%v: %d launched nodes standing, want 1", atomic, got)
		}
		if !cluster.Nodes["i-src"].Deleted {
			t.Errorf("atomic=%v: source survived", atomic)
		}
	}
}

// An atomic proposal whose evictions do not wait on its whole launch set cannot
// be delivered: once a pod has moved there is no releasing the launches. That is a
// generator bug, so it is a mechanism error rather than an outcome, and it is
// rejected before any action fires.
func TestAtomicLaunchRejectsAnUngatedEviction(t *testing.T) {
	batch, cat, its := atomicFixture(t)
	cluster := NewCluster(batch, cat, 10)

	p := twoLaunchReplace(batch, its...)
	p.AtomicLaunch = true
	// Re-gate the first eviction on the first launch alone, which is what
	// batchActions would produce if it gated on one launch out of several.
	for i := range p.Actions {
		if p.Actions[i].Kind == Evict {
			p.Actions[i].DependsOn = []int{2}
			break
		}
	}

	exec := NewMemoryExecutor(cluster)
	if _, err := exec.Apply(context.Background(), p); err == nil {
		t.Fatal("want an error for an eviction that does not wait on the whole launch set")
	}
	if cluster.Nodes["i-src1"].Tainted {
		t.Error("a rejected atomic proposal fired an action")
	}
	if got := standingLaunched(cluster); got != 0 {
		t.Errorf("%d nodes launched by a rejected atomic proposal", got)
	}

	// batchActions gates every eviction on the launch, so the batch shape the
	// multi-node path produces passes as it stands.
	ok := twoLaunchReplace(batch, its[0])
	ok.Actions = batchActions(batch, its[0])
	ok.AtomicLaunch = true
	if err := validateAtomicLaunch(ok); err != nil {
		t.Errorf("batchActions does not satisfy the atomic gating: %v", err)
	}
}

// FailLaunchesOf is the other injection, a type the provider has no capacity for.
// It is what a test wants when the failing launch is identified by what it asks
// for rather than by when it is asked.
func TestFailLaunchesOfNamedType(t *testing.T) {
	batch, cat, its := atomicFixture(t)
	cluster := NewCluster(batch, cat, 10)
	cluster.LaunchFails = FailLaunchesOf(its[1].Name)

	p := twoLaunchReplace(batch, its...)
	p.AtomicLaunch = true

	exec := NewMemoryExecutor(cluster)
	outcome, err := exec.Apply(context.Background(), p)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if outcome != Abandoned {
		t.Fatalf("want Abandoned when %s has no capacity, got %v", its[1].Name, outcome)
	}
	if got := standingLaunched(cluster); got != 0 {
		t.Errorf("%d nodes standing, want 0", got)
	}
	// The type that was available is not what failed, so the injection is
	// selective rather than a switch that fails everything.
	cluster2 := NewCluster(batch, cat, 10)
	cluster2.LaunchFails = FailLaunchesOf(its[1].Name)
	if _, err := cluster2.Launch(its[0]); err != nil {
		t.Errorf("the available type was refused: %v", err)
	}
}

// A precondition flipping under an atomic proposal is a stop and not an
// abandonment. The two say different things: a precondition failure is the cluster
// refusing the rest of the move, and abandonment is the provider not having the
// capacity the launch set asked for. Only the second is answered by releasing.
func TestAtomicPreconditionFailureStopsRatherThanAbandons(t *testing.T) {
	batch, cat, its := atomicFixture(t)
	cluster := NewCluster(batch, cat, 10)

	p := twoLaunchReplace(batch, its...)
	p.AtomicLaunch = true
	p.Preconditions = []PreconditionKind{NotNominated}

	exec := NewMemoryExecutor(cluster)
	// Pass through the taints and both launches, then fail.
	exec.Checkers[NotNominated] = &countingChecker{kind: NotNominated, passes: 4}

	outcome, err := exec.Apply(context.Background(), p)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if outcome != Stopped {
		t.Fatalf("want Stopped, got %v", outcome)
	}
	if exec.Abandoned != 0 || exec.LaunchesReleased != 0 {
		t.Errorf("a precondition stop released %d launches, want none", exec.LaunchesReleased)
	}
	if got := standingLaunched(cluster); got != 2 {
		t.Errorf("%d launched nodes standing after a stop, want the 2 that fired", got)
	}
}

// Whether the mechanism has a producer yet. It does not: every proposal the three
// consolidation paths and the provisioning generator emit carries at most one
// Launch, so nothing today needs AtomicLaunch and nothing sets it.
//
// The multi-node path is the one that looked like a candidate, because its
// Candidates is a list and its actions are the whole batch's. But
// computeBatchConsolidation replaces a batch with one instance type or with none,
// so its launch count is zero or one and its single launch is trivially atomic
// already. The mechanism is ahead of its use, and #5's gang workload is what will
// use it: AcquireNodes is the shape that needs it.
func TestNoGeneratorEmitsAMultiLaunchProposal(t *testing.T) {
	cat := BuildCatalog()
	opts := DefaultGenClusterOpts(150, 7)
	opts.Catalog = cat
	nodes, totals := GenCluster(opts)
	cfg := DefaultRunConfig(2, 40)

	dests := EligibleDestinations(nodes, cat, totals, cfg.Bands)
	proposals, _ := EvaluateInOrder(nodes, dests, cat, totals, cfg)

	var pending []*corev1.Pod
	for _, n := range nodes {
		pending = append(pending, n.ReschedulablePods...)
	}
	proposals = append(proposals, ProvisioningGenerator{}.Generate(pending, cat)...)

	if len(proposals) == 0 {
		t.Fatal("no proposals to check")
	}
	for _, p := range proposals {
		launches := 0
		for _, a := range p.Actions {
			if a.Kind == Launch {
				launches++
			}
		}
		if launches > 1 {
			t.Errorf("proposal %s launches %d nodes; it needs AtomicLaunch and this test's premise is stale",
				p.ID, launches)
		}
		if p.AtomicLaunch {
			t.Errorf("proposal %s sets AtomicLaunch; no generator should, since none launches more than one node", p.ID)
		}
	}
}
