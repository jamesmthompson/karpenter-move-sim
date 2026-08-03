package sim

import (
	"context"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// testPod builds a pod with the given requests. Tests construct their own pods
// rather than drawing from PodShape, because a test that depends on a
// distribution fails for reasons that have nothing to do with the code.
func testPod(name string, cpuMilli, memMiB int64) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "app",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    *resource.NewMilliQuantity(cpuMilli, resource.DecimalSI),
						corev1.ResourceMemory: *resource.NewQuantity(memMiB*(1<<20), resource.BinarySI),
					},
				},
			}},
		},
	}
}

func instanceType(t *testing.T, cat *Catalog, name string) *cloudprovider.InstanceType {
	t.Helper()
	for _, it := range cat.InstanceTypes {
		if it.Name == name {
			return it
		}
	}
	t.Fatalf("instance type %s not in catalog", name)
	return nil
}

func node(t *testing.T, cat *Catalog, id, itName string, pods ...*corev1.Pod) NodeSnapshot {
	t.Helper()
	it := instanceType(t, cat, itName)
	price, ok := cheapestOffering(it)
	if !ok {
		t.Fatalf("instance type %s has no available offering", itName)
	}
	return NodeSnapshot{
		ProviderID:        id,
		NodePool:          &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
		InstanceType:      it,
		Price:             price,
		ReschedulablePods: pods,
		DisruptionCost:    disruptionCost(pods),
	}
}

// -----------------------------------------------------------------------------
// Stage 2
// -----------------------------------------------------------------------------

func TestGenerate(t *testing.T) {
	cat := BuildCatalog()

	// An empty m7i.large alongside two nodes carrying load. Pool totals come
	// from the whole set, because the score is a share of the pool.
	empty := node(t, cat, "i-empty", "m7i.large")
	busyA := node(t, cat, "i-busy-a", "m7i.2xlarge",
		testPod("a1", 2000, 6144), testPod("a2", 2000, 6144), testPod("a3", 2000, 6144))
	busyB := node(t, cat, "i-busy-b", "m7i.2xlarge",
		testPod("b1", 2000, 6144), testPod("b2", 2000, 6144), testPod("b3", 2000, 6144))

	// A node holding one small pod on capacity sized for far more. Its delete
	// ratio is high, so it survives any reasonable threshold.
	thin := node(t, cat, "i-thin", "m7i.2xlarge", testPod("t1", 100, 256))

	cases := []struct {
		name      string
		source    NodeSnapshot
		all       []NodeSnapshot
		threshold float64
		wantAny   bool
		wantKinds []ActionKind
	}{
		{
			// An empty node is a delete with nothing to place. No Launch,
			// no Evict, and PodsStillFit is not among the preconditions
			// because there are no pods to fit.
			name:      "empty node produces a delete proposal",
			source:    empty,
			all:       []NodeSnapshot{empty, busyA, busyB},
			threshold: 0.5,
			wantAny:   true,
			wantKinds: []ActionKind{Taint, Delete},
		},
		{
			// A node at pool-average price per unit of disruption sits at a
			// delete ratio near 1. Raise the threshold above 1 and it is
			// pruned before any move is costed.
			name:      "node below the delete ratio threshold produces nothing",
			source:    busyA,
			all:       []NodeSnapshot{empty, busyA, busyB},
			threshold: 3.0,
			wantAny:   false,
		},
		{
			name:      "underloaded node produces at least one proposal",
			source:    thin,
			all:       []NodeSnapshot{thin, busyA, busyB},
			threshold: 0.5,
			wantAny:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			totals := Totals(tc.all)
			dests := withoutNode(
				EligibleDestinations(tc.all, cat, totals, DefaultRunConfig(2, 10).Bands),
				tc.source.ProviderID)
			got := ConsolidationGenerator{Threshold: tc.threshold}.
				Generate(tc.source, dests, cat, totals)

			if tc.wantAny != (len(got) > 0) {
				t.Fatalf("wantAny=%v, got %d proposals", tc.wantAny, len(got))
			}
			if tc.wantKinds == nil {
				return
			}
			if len(got) != 1 {
				t.Fatalf("want exactly 1 proposal, got %d", len(got))
			}
			var kinds []ActionKind
			for _, a := range got[0].Actions {
				kinds = append(kinds, a.Kind)
			}
			if len(kinds) != len(tc.wantKinds) {
				t.Fatalf("want actions %v, got %v", tc.wantKinds, kinds)
			}
			for i := range kinds {
				if kinds[i] != tc.wantKinds[i] {
					t.Fatalf("want actions %v, got %v", tc.wantKinds, kinds)
				}
			}
			for _, pre := range got[0].Preconditions {
				if pre == PodsStillFit {
					t.Errorf("empty node should not gate on PodsStillFit")
				}
			}
		})
	}
}

// TestSourceIsNotItsOwnDestination pins the one thing Run has to do that
// EligibleDestinations cannot. Eligibility is decided per node, so the filter has
// no idea which source it is being asked about, and a node in the Destination or
// Full band with a high delete ratio is both a candidate and eligible. Left in
// its own destination set, such a node satisfies aggregateRoom against the
// capacity it is about to give up.
func TestSourceIsNotItsOwnDestination(t *testing.T) {
	cat := BuildCatalog()
	// Both nodes sit in the Destination band, so both pass the filter. The
	// filler is loaded to just under full, leaving less room than the source's
	// pod needs. The only free capacity that could hold that pod belongs to the
	// source itself.
	source := node(t, cat, "i-src", "m7i.large", testPod("p1", 1500, 5120))
	filler := node(t, cat, "i-filler", "m7i.xlarge", testPod("f1", 2620, 12191))
	all := []NodeSnapshot{source, filler}
	totals := Totals(all)

	dests := EligibleDestinations(all, cat, totals, DefaultRunConfig(2, 10).Bands)
	found := false
	for _, d := range dests {
		if d.ProviderID == source.ProviderID {
			found = true
		}
	}
	if !found {
		t.Fatalf("premise broken: the source is not in the destination set (%d dests)", len(dests))
	}

	required := Requests(source.ReschedulablePods)
	if !aggregateRoom(dests, cat, required) {
		t.Fatal("premise broken: the unfiltered set should claim room, since it contains the source")
	}
	if aggregateRoom(withoutNode(dests, source.ProviderID), cat, required) {
		t.Error("withoutNode did not remove the source from its own destination set")
	}
}

// TestIdentifyPrunes checks that stage 1 emits one bit per node and that the bit
// tracks the delete ratio rather than the node's own utilization. The empty node
// scores highest because its disruption cost is the bare per-node baseline.
func TestIdentifyPrunes(t *testing.T) {
	cat := BuildCatalog()
	empty := node(t, cat, "i-empty", "m7i.large")
	busy := node(t, cat, "i-busy", "m7i.large",
		testPod("b1", 500, 1024), testPod("b2", 500, 1024), testPod("b3", 500, 1024))
	nodes := []NodeSnapshot{empty, busy}
	totals := Totals(nodes)

	if got := DeleteRatio(empty, totals); got <= DeleteRatio(busy, totals) {
		t.Fatalf("empty node ratio %v should exceed busy node ratio %v",
			got, DeleteRatio(busy, totals))
	}

	bits := Identify(nodes, totals, 1.0)
	if len(bits) != 2 {
		t.Fatalf("want one bit per node, got %d", len(bits))
	}
	if !bits[0] {
		t.Error("empty node should survive the filter at k=1")
	}
	if bits[1] {
		t.Error("busy node should be pruned at k=1")
	}
}

// -----------------------------------------------------------------------------
// Stage 3
// -----------------------------------------------------------------------------

func TestCompactorConflictsAndBudget(t *testing.T) {
	cat := BuildCatalog()
	a := node(t, cat, "i-a", "m7i.large")
	b := node(t, cat, "i-b", "m7i.large")

	mk := func(score float64, cands ...NodeSnapshot) Proposal {
		return Proposal{Candidates: cands, Score: score, Savings: score}
	}

	cases := []struct {
		name          string
		proposals     []Proposal
		budget        int
		wantCommitted int
		wantConflicts int
	}{
		{
			// Stage 2 emits a DELETE and a REPLACE for the same node as
			// alternatives. The compactor is where one of them dies.
			name:          "overlapping candidates conflict",
			proposals:     []Proposal{mk(2.0, a), mk(1.0, a)},
			budget:        10,
			wantCommitted: 1,
			wantConflicts: 1,
		},
		{
			name:          "disjoint candidates both commit",
			proposals:     []Proposal{mk(2.0, a), mk(1.0, b)},
			budget:        10,
			wantCommitted: 2,
			wantConflicts: 0,
		},
		{
			// Budget exhaustion is not a conflict. The work was legal and
			// deferred, and counting it as a conflict would hide that.
			name:          "budget stops commitment without counting a conflict",
			proposals:     []Proposal{mk(2.0, a), mk(1.0, b)},
			budget:        1,
			wantCommitted: 1,
			wantConflicts: 0,
		},
		{
			// A launch removes nothing, so it draws no budget.
			name:          "pure launch commits with no budget",
			proposals:     []Proposal{{Cost: 1.0, PodsCovered: 3}},
			budget:        0,
			wantCommitted: 1,
			wantConflicts: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			committed, conflicts := RankingCompactor{}.
				SelectCounting(context.Background(), tc.proposals, tc.budget)
			if len(committed) != tc.wantCommitted {
				t.Errorf("want %d committed, got %d", tc.wantCommitted, len(committed))
			}
			if conflicts != tc.wantConflicts {
				t.Errorf("want %d conflicts, got %d", tc.wantConflicts, conflicts)
			}
			// Ranking is by score descending, so the winner of a conflict is
			// the higher-scoring proposal.
			if tc.wantCommitted > 0 && len(committed) > 0 && len(tc.proposals) > 1 {
				if committed[0].Score < tc.proposals[len(tc.proposals)-1].Score {
					t.Error("committed the lower-scoring proposal first")
				}
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Stage 4
// -----------------------------------------------------------------------------

// TestExecutorOutcomes drives the rollback-or-forward cut. Both cases use the
// same proposal shape and differ only in where the failing precondition sits
// relative to the first Evict, which is the whole of what Decide reads.
func TestExecutorOutcomes(t *testing.T) {
	cat := BuildCatalog()

	// failAfter is a checker that passes a set number of times and then fails.
	// A real precondition flips because the cluster changed under it; counting
	// calls puts the flip at an exact point in the action sequence, which a
	// cluster mutation cannot do reliably.
	newFailAfter := func(passes int) *countingChecker {
		return &countingChecker{kind: NotNominated, passes: passes}
	}

	cases := []struct {
		name string
		// passes before the checker starts failing.
		passes      int
		wantOutcome Outcome
		wantEvicted bool
		// wantLaunched is whether the Launch fired before the failure, so the
		// rollback has a node to remove. At passes=0 nothing has been applied
		// and the launched-node assertion below would hold vacuously.
		wantLaunched bool
	}{
		{
			// The check runs before action 0, so nothing has been applied and
			// nothing is destroyed. Rollback is free and correct.
			name:        "rollback when a precondition fails before any eviction",
			passes:      0,
			wantOutcome: Rollback,
			wantEvicted: false,
		},
		{
			// Taint and Launch applied, then the check fails before the first
			// Evict. This is the case that costs something to roll back: a
			// wasted CreateFleet and a node that has to go away again.
			name:         "rollback undoes a launch that fired before the failure",
			passes:       2,
			wantOutcome:  Rollback,
			wantEvicted:  false,
			wantLaunched: true,
		},
		{
			// Taint, Launch, Evict, then the check fails. The pod is off its
			// node and rolling back does not put it back, so the move has to
			// finish as an obligation.
			name:         "forward when a precondition fails after an eviction",
			passes:       3,
			wantOutcome:  Forward,
			wantEvicted:  true,
			wantLaunched: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := node(t, cat, "i-src", "m7i.2xlarge",
				testPod("p1", 500, 1024), testPod("p2", 500, 1024))
			sink := node(t, cat, "i-sink", "m7i.4xlarge")
			nodes := []NodeSnapshot{source, sink}

			cluster := NewCluster(nodes, cat, 10)
			exec := NewMemoryExecutor(cluster)
			check := newFailAfter(tc.passes)
			exec.Checkers[NotNominated] = check

			// A replace: taint, launch, evict, evict, delete. The launch is
			// what makes a pre-eviction rollback have something to undo.
			replacement := instanceType(t, cat, "m7i.large")
			p := Proposal{
				Candidates:   []NodeSnapshot{source},
				Replacements: []*cloudprovider.InstanceType{replacement},
				Savings:      source.Price,
				Actions: []Action{
					{Kind: Taint, ProviderID: source.ProviderID},
					{Kind: Launch, InstanceType: replacement},
					{Kind: Evict, ProviderID: source.ProviderID, DependsOn: []int{1}},
					{Kind: Evict, ProviderID: source.ProviderID, DependsOn: []int{1}},
					{Kind: Delete, ProviderID: source.ProviderID, DependsOn: []int{2, 3}},
				},
				Preconditions: []PreconditionKind{NotNominated},
			}

			got, err := exec.Apply(context.Background(), p)
			if err != nil {
				t.Fatalf("Apply returned a mechanism error: %v", err)
			}
			if got != tc.wantOutcome {
				t.Fatalf("want outcome %v, got %v", tc.wantOutcome, got)
			}

			// The failure has to land where the case intends, or the assertions
			// below are about a different point in the sequence than the name
			// claims.
			if check.calls != tc.passes+1 {
				t.Fatalf("want the check to fail on call %d, it was called %d times",
					tc.passes+1, check.calls)
			}
			if tc.wantLaunched && cluster.launchSeq != 1 {
				t.Fatalf("case expects the launch to have fired, launchSeq=%d", cluster.launchSeq)
			}

			src := cluster.Nodes[source.ProviderID]
			switch tc.wantOutcome {
			case Rollback:
				if src.Tainted {
					t.Error("rollback left the source tainted")
				}
				if src.Deleted {
					t.Error("rollback deleted the source")
				}
				if len(src.Pods) != 2 {
					t.Errorf("rollback lost pods: want 2 on the source, got %d", len(src.Pods))
				}
				// The launched node is the one reversible write that costs
				// money, so a rollback that leaves it standing has not rolled
				// back. Counting nodes catches it even if Launched were wrong.
				if len(cluster.Nodes) != len(nodes) {
					t.Errorf("rollback left %d nodes, want %d", len(cluster.Nodes), len(nodes))
				}
				for id, n := range cluster.Nodes {
					if n.Launched {
						t.Errorf("rollback left launched node %s standing", id)
					}
				}
				if exec.RolledBack != 1 {
					t.Errorf("want RolledBack=1, got %d", exec.RolledBack)
				}
				if exec.Forward.Pods != 0 {
					t.Errorf("rollback should charge no forward cost, got %d pods", exec.Forward.Pods)
				}
			case Forward:
				if !src.Deleted {
					t.Error("forward left the source standing")
				}
				if len(src.Pods) != 0 {
					t.Errorf("forward left %d pods on a deleted node", len(src.Pods))
				}
				if exec.Forwarded != 1 {
					t.Errorf("want Forwarded=1, got %d", exec.Forwarded)
				}
				if exec.Forward.Pods == 0 {
					t.Error("forward charged no pods against the budget")
				}
				if exec.Forward.Nodes != 1 {
					t.Errorf("want 1 forward node charged, got %d", exec.Forward.Nodes)
				}
			}
			if tc.wantEvicted && exec.Forward.Pods == 0 {
				t.Error("expected evictions to have been charged")
			}
		})
	}
}

// countingChecker passes a fixed number of times, then fails. It stands in for a
// precondition the cluster flipped, with the flip landing at a chosen action.
type countingChecker struct {
	kind   PreconditionKind
	passes int
	calls  int
}

func (c *countingChecker) Kind() PreconditionKind { return c.kind }

func (c *countingChecker) Check(context.Context, Proposal) error {
	c.calls++
	if c.calls <= c.passes {
		return nil
	}
	return errCheckFailed
}

var errCheckFailed = &checkError{}

type checkError struct{}

func (e *checkError) Error() string { return "precondition flipped" }

// TestExecutorOrdering checks the one guarantee the executor claims. An action
// released before its prerequisite confirmed is a mechanism fault, not a
// precondition failure, so it comes back as an error.
func TestExecutorOrdering(t *testing.T) {
	cat := BuildCatalog()
	source := node(t, cat, "i-src", "m7i.large", testPod("p1", 500, 1024))
	cluster := NewCluster([]NodeSnapshot{source}, cat, 10)
	exec := NewMemoryExecutor(cluster)

	p := Proposal{
		Candidates: []NodeSnapshot{source},
		Actions: []Action{
			// Action 0 depends on action 1, which cannot have confirmed yet.
			{Kind: Taint, ProviderID: source.ProviderID, DependsOn: []int{1}},
			{Kind: Delete, ProviderID: source.ProviderID},
		},
	}
	if _, err := exec.Apply(context.Background(), p); err == nil {
		t.Fatal("want an error for a forward dependency, got nil")
	}
	if cluster.Nodes[source.ProviderID].Deleted {
		t.Error("a rejected DAG should not have deleted anything")
	}
}

// TestCheckers pins each local checker to the one piece of state it reads. The
// four non-local kinds are not tested for behavior because they have none.
func TestCheckers(t *testing.T) {
	cat := BuildCatalog()

	t.Run("NoDoNotDisrupt rejects an opted-out pod", func(t *testing.T) {
		pod := testPod("p1", 500, 1024)
		pod.Annotations = map[string]string{v1.DoNotDisruptAnnotationKey: "true"}
		source := node(t, cat, "i-src", "m7i.large", pod)
		cluster := NewCluster([]NodeSnapshot{source}, cat, 10)
		c := NoDoNotDisruptChecker{Cluster: cluster}
		if err := c.Check(context.Background(), Proposal{Candidates: []NodeSnapshot{source}}); err == nil {
			t.Error("want a violation, got nil")
		}
	})

	t.Run("NotNominated rejects a nominated node", func(t *testing.T) {
		source := node(t, cat, "i-src", "m7i.large")
		cluster := NewCluster([]NodeSnapshot{source}, cat, 10)
		cluster.Nominated[source.ProviderID] = true
		c := NotNominatedChecker{Cluster: cluster}
		if err := c.Check(context.Background(), Proposal{Candidates: []NodeSnapshot{source}}); err == nil {
			t.Error("want a violation, got nil")
		}
	})

	t.Run("BudgetHasRoom rejects when the budget is short", func(t *testing.T) {
		source := node(t, cat, "i-src", "m7i.large")
		cluster := NewCluster([]NodeSnapshot{source}, cat, 0)
		c := BudgetHasRoomChecker{Cluster: cluster}
		p := Proposal{Candidates: []NodeSnapshot{source}}
		if err := c.Check(context.Background(), p); err == nil {
			t.Error("want a violation, got nil")
		}
		// A launch spends no budget, so it passes an exhausted one.
		if err := c.Check(context.Background(), Proposal{}); err != nil {
			t.Errorf("a launch should pass an exhausted budget, got %v", err)
		}
	})

	t.Run("PodsStillFit rejects a pod no single node holds", func(t *testing.T) {
		// Aggregate room is ample: two large nodes have more total free CPU
		// than the pod wants. Neither one holds it. This is the case
		// aggregateRoom passes and the precondition has to catch.
		big := testPod("big", 3000, 8192)
		source := node(t, cat, "i-src", "m7i.2xlarge", big)
		sinkA := node(t, cat, "i-sink-a", "m7i.large")
		sinkB := node(t, cat, "i-sink-b", "m7i.large")
		nodes := []NodeSnapshot{source, sinkA, sinkB}

		if !aggregateRoom([]NodeSnapshot{sinkA, sinkB}, cat, Requests([]*corev1.Pod{big})) {
			t.Skip("catalog changed: the two sinks no longer have aggregate room")
		}
		cluster := NewCluster(nodes, cat, 10)
		c := PodsStillFitChecker{Cluster: cluster}
		if err := c.Check(context.Background(), Proposal{Candidates: []NodeSnapshot{source}}); err == nil {
			t.Error("want a violation for a pod that fits nowhere, got nil")
		}
	})

	t.Run("PodsStillFit does not double-book one hole", func(t *testing.T) {
		// Two pods, and room for exactly one. Checking each against untouched
		// free capacity would pass both.
		p1 := testPod("p1", 1500, 4096)
		p2 := testPod("p2", 1500, 4096)
		source := node(t, cat, "i-src", "m7i.2xlarge", p1, p2)
		sink := node(t, cat, "i-sink", "m7i.large")
		cluster := NewCluster([]NodeSnapshot{source, sink}, cat, 10)
		c := PodsStillFitChecker{Cluster: cluster}
		if err := c.Check(context.Background(), Proposal{Candidates: []NodeSnapshot{source}}); err == nil {
			t.Error("want a violation, got nil: one hole was booked twice")
		}
	})

	// The classification was written from each constraint's shape and then
	// measured, and the measurement moved three of the four. What is asserted here
	// is the classification readcost_test.go measures, so the two cannot drift
	// apart without one of them failing.
	t.Run("relational constraints are classified as measured", func(t *testing.T) {
		for _, kind := range []PreconditionKind{PDBAllows, AntiAffinityHolds, AffinityHolds} {
			if !kind.Local() {
				t.Errorf("%s measures local; see TestNodeReadsPerCheck", kind)
			}
		}
		for _, kind := range []PreconditionKind{TopologyHolds, GangIntact} {
			if kind.Local() {
				t.Errorf("%s measures non-local; see TestNodeReadsPerCheck", kind)
			}
		}
		// The two affinity kinds are local because of their topology key and not
		// because of their kind, which is what LocalForTopologyKey says out loud.
		for _, kind := range []PreconditionKind{AntiAffinityHolds, AffinityHolds} {
			if !kind.LocalForTopologyKey(corev1.LabelHostname) {
				t.Errorf("%s at hostname scope should be local", kind)
			}
			if kind.LocalForTopologyKey(corev1.LabelTopologyZone) {
				t.Errorf("%s at zone scope reads every node and is not local", kind)
			}
		}
	})

	// GangIntact is the one still stubbed. Nothing in the model carries a gang's
	// minimum member count, so a checker here would be an approximation dressed as
	// a check.
	t.Run("GangIntact is still a stub", func(t *testing.T) {
		cluster := NewCluster(nil, cat, 10)
		if err := LocalCheckers(cluster)[GangIntact].Check(context.Background(), Proposal{}); err != nil {
			t.Errorf("GangIntact is a stub and must not gate, got %v", err)
		}
	})
}

// -----------------------------------------------------------------------------
// Generator and end to end
// -----------------------------------------------------------------------------

// TestGenClusterDeterministic is the property the reported numbers rest on. A
// seed and a node count fix the cluster, so a reduction figure reproduces.
func TestGenClusterDeterministic(t *testing.T) {
	opts := DefaultGenClusterOpts(50, 42)
	a, ta := GenCluster(opts)
	b, tb := GenCluster(opts)

	if len(a) != len(b) || len(a) != 50 {
		t.Fatalf("want 50 nodes both times, got %d and %d", len(a), len(b))
	}
	if ta != tb {
		t.Fatalf("totals differ between runs: %+v vs %+v", ta, tb)
	}
	for i := range a {
		if a[i].ProviderID != b[i].ProviderID ||
			a[i].InstanceType.Name != b[i].InstanceType.Name ||
			a[i].Price != b[i].Price ||
			len(a[i].ReschedulablePods) != len(b[i].ReschedulablePods) {
			t.Fatalf("node %d differs between runs", i)
		}
	}
	if ta.Cost <= 0 || ta.DisruptionCost <= 0 {
		t.Fatalf("degenerate totals: %+v", ta)
	}
}

// TestCatalogShapes checks the three memory ratios and the price doubling,
// because a wrong ratio makes every packing result meaningless in a way no
// other test would catch.
func TestCatalogShapes(t *testing.T) {
	cat := BuildCatalog()
	if len(cat.InstanceTypes) < 15 || len(cat.InstanceTypes) > 25 {
		t.Errorf("want 15 to 25 instance types, got %d", len(cat.InstanceTypes))
	}

	want := map[string]struct {
		cpu   int64
		memGi int64
		price float64
	}{
		"c7i.large":    {2, 4, 0.0892},
		"m7i.large":    {2, 8, 0.1008},
		"r7i.large":    {2, 16, 0.1323},
		"m7i.xlarge":   {4, 16, 0.2016},
		"m7i.8xlarge":  {32, 128, 0.1008 * 16},
		"c7i.16xlarge": {64, 128, 0.0892 * 32},
	}
	for name, w := range want {
		it := instanceType(t, cat, name)
		cpu := it.Capacity[corev1.ResourceCPU]
		if cpu.Value() != w.cpu {
			t.Errorf("%s: want %d vCPU, got %d", name, w.cpu, cpu.Value())
		}
		mem := it.Capacity[corev1.ResourceMemory]
		if mem.Value() != w.memGi*(1<<30) {
			t.Errorf("%s: want %d GiB, got %d bytes", name, w.memGi, mem.Value())
		}
		price, ok := cheapestOffering(it)
		if !ok {
			t.Fatalf("%s: no available offering", name)
		}
		if diff := price - w.price; diff > 1e-9 || diff < -1e-9 {
			t.Errorf("%s: want price %.4f, got %.4f", name, w.price, price)
		}
		// Effective capacity has to be positive after both the kubelet reserve
		// and the daemonset draw, or every fit test on this type is a no.
		for _, res := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourcePods} {
			q := cat.Effective(it)[res]
			if q.Sign() <= 0 {
				t.Errorf("%s: effective %s is %s", name, res, q.String())
			}
		}
	}
}

// TestRunEndToEnd checks the invariants a whole pass has to satisfy. It asserts
// no specific savings figure, because the figure is what the simulator exists to
// discover and pinning it would turn a measurement into a fixture.
func TestRunEndToEnd(t *testing.T) {
	cat := BuildCatalog()
	opts := DefaultGenClusterOpts(120, 3)
	opts.Catalog = cat
	nodes, totals := GenCluster(opts)

	cfg := DefaultRunConfig(2, 15)
	res := Run(context.Background(), nodes, totals, cat, cfg)

	if res.NodesIn != 120 {
		t.Errorf("want 120 nodes in, got %d", res.NodesIn)
	}
	if res.CandidatesOut > res.NodesIn {
		t.Errorf("stage 1 emitted %d candidates from %d nodes", res.CandidatesOut, res.NodesIn)
	}
	if res.Applied != res.Proceeded+res.RolledBack+res.Forward {
		t.Errorf("applied %d != proceed %d + rollback %d + forward %d",
			res.Applied, res.Proceeded, res.RolledBack, res.Forward)
	}
	if res.Applied > res.Committed {
		t.Errorf("applied %d exceeds committed %d", res.Applied, res.Committed)
	}
	if res.SavingsRealized > res.SavingsProposed+1e-9 {
		t.Errorf("realized %.4f exceeds proposed %.4f", res.SavingsRealized, res.SavingsProposed)
	}
	if res.PoolCostAfter > res.PoolCostBefore+1e-9 {
		t.Errorf("a consolidation pass raised pool cost: %.4f -> %.4f",
			res.PoolCostBefore, res.PoolCostAfter)
	}
	// A pass that realizes savings by leaving pods homeless has saved nothing,
	// and this is where that would show up.
	if res.PendingAfter != 0 {
		t.Errorf("pass stranded %d pods", res.PendingAfter)
	}
	// The budget is the constraint that has to bind, or the pass is not
	// measuring what it claims.
	if res.BudgetLeft < 0 {
		t.Errorf("budget went negative: %d", res.BudgetLeft)
	}
}

// TestStagesOneAndTwoParallel is the locality claim as an executable check
// rather than a comment. Stages 1 and 2 were given no cluster handle, so two
// workers examining two different nodes should share nothing. Run every
// candidate concurrently and compare against the serial result.
//
// This test is worth little without -race, which is what actually detects the
// shared write it is looking for.
func TestStagesOneAndTwoParallel(t *testing.T) {
	cat := BuildCatalog()
	opts := DefaultGenClusterOpts(150, 11)
	opts.Catalog = cat
	nodes, totals := GenCluster(opts)
	cfg := DefaultRunConfig(2, 50)

	bits := Identify(nodes, totals, cfg.K)
	dests := EligibleDestinations(nodes, cat, totals, cfg.Bands)
	gen := ConsolidationGenerator{Threshold: cfg.Threshold}

	serial := map[string]int{}
	for i, n := range nodes {
		if !bits[i] {
			continue
		}
		serial[n.ProviderID] = len(gen.Generate(n, withoutNode(dests, n.ProviderID), cat, totals))
	}
	if len(serial) == 0 {
		t.Fatal("premise broken: no candidates to generate for")
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	par := map[string]int{}
	for i, n := range nodes {
		if !bits[i] {
			continue
		}
		wg.Add(1)
		go func(n NodeSnapshot) {
			defer wg.Done()
			got := len(gen.Generate(n, withoutNode(dests, n.ProviderID), cat, totals))
			mu.Lock()
			par[n.ProviderID] = got
			mu.Unlock()
		}(n)
	}
	wg.Wait()

	if len(serial) != len(par) {
		t.Fatalf("serial produced %d results, parallel %d", len(serial), len(par))
	}
	for id, want := range serial {
		if par[id] != want {
			t.Errorf("%s: serial %d proposals, parallel %d", id, want, par[id])
		}
	}
}
