package sim

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// clusterWith builds a Cluster from hand-built nodes under one strategy. Tests
// here need the executor's placement, so they need a Cluster and not snapshots.
func clusterWith(t *testing.T, cat *Catalog, name PlacementStrategyName, nodes ...NodeSnapshot) *Cluster {
	t.Helper()
	c := NewCluster(nodes, cat, len(nodes))
	s, err := name.Strategy()
	if err != nil {
		t.Fatalf("strategy %q: %v", name, err)
	}
	c.Placement = s
	return c
}

// -----------------------------------------------------------------------------
// The two formulas, against the numbers upstream's own tests use
// -----------------------------------------------------------------------------

// TestAllocationScorersMatchUpstream pins the arithmetic rather than the
// preference. A test that only checks which node wins passes for a scorer that is
// monotone in the right direction and wrong everywhere else, and the formula is
// what the paper cites.
//
// Requested is after the pod lands, capacity is allocatable, the fraction is
// scaled to 0..100, and the resources are averaged by weight. cpu and memory at
// weight 1 each, which is kube's defaultResourceSpec.
func TestAllocationScorersMatchUpstream(t *testing.T) {
	for _, tc := range []struct {
		requested, capacity int64
		least, most         int64
	}{
		// Empty: all capacity unused.
		{0, 4000, 100, 0},
		// Half full: both strategies agree, which is the crossover.
		{2000, 4000, 50, 50},
		// Full.
		{4000, 4000, 0, 100},
		// A quarter used.
		{1000, 4000, 75, 25},
		// Integer truncation, which upstream has and a float port would not: a
		// third of the capacity scores 66 and not 66.67.
		{1000, 3000, 66, 33},
		// Over capacity. The two differ here and it is not symmetry: least
		// returns zero, most clamps requested, because a pod with no requests is
		// scored at a nonzero minimum upstream and can push the sum over.
		{5000, 4000, 0, 100},
		// No capacity for the resource at all.
		{0, 0, 0, 0},
	} {
		if got := leastRequestedScore(tc.requested, tc.capacity); got != tc.least {
			t.Errorf("leastRequestedScore(%d, %d) = %d, want %d",
				tc.requested, tc.capacity, got, tc.least)
		}
		if got := mostRequestedScore(tc.requested, tc.capacity); got != tc.most {
			t.Errorf("mostRequestedScore(%d, %d) = %d, want %d",
				tc.requested, tc.capacity, got, tc.most)
		}
	}
}

// TestStrategyScoreAveragesResources checks the weighted average across cpu and
// memory, using a slot where the two fractions differ. A scorer that returned one
// resource's fraction, or that summed without dividing by the weight sum, would
// pass every single-resource test and fail this.
func TestStrategyScoreAveragesResources(t *testing.T) {
	// 4 cores and 8 GiB allocatable. After the pod lands, cpu is at 1/4 and
	// memory at 3/4, so least averages (75 + 25) / 2 and most averages (25 + 75) / 2.
	// Both come to 50, which is the wrong pair to assert on, so memory is put at
	// 1/2 instead: least is (75 + 50) / 2 = 62, most is (25 + 50) / 2 = 37.
	slot := Slot{
		Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:    *resource.NewMilliQuantity(4000, resource.DecimalSI),
			corev1.ResourceMemory: *resource.NewQuantity(8<<30, resource.BinarySI),
		},
		Free: corev1.ResourceList{
			corev1.ResourceCPU:    *resource.NewMilliQuantity(3500, resource.DecimalSI),
			corev1.ResourceMemory: *resource.NewQuantity(5<<30, resource.BinarySI),
		},
	}
	// The pod takes cpu to 1000/4000 and memory to 4 GiB of 8.
	need := corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(1<<30, resource.BinarySI),
	}

	if got := LeastAllocated().Score(slot, need); got != 62 {
		t.Errorf("LeastAllocated score = %d, want 62 (cpu 75, memory 50, averaged)", got)
	}
	if got := MostAllocated().Score(slot, need); got != 37 {
		t.Errorf("MostAllocated score = %d, want 37 (cpu 25, memory 50, averaged)", got)
	}
}

// TestScoreIgnoresPodSlots keeps the scored resources to cpu and memory. Pod
// slots are in Slot.Free because fitsInto tests them, so a scorer that averaged
// over everything in the list would let a node's max-pods figure outvote its
// utilization: an m7i.large at 29 slots and an m7i.4xlarge at 110 differ by more
// on that dimension than on either real resource.
func TestScoreIgnoresPodSlots(t *testing.T) {
	base := func(slots int64) Slot {
		return Slot{
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewMilliQuantity(4000, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(8<<30, resource.BinarySI),
				corev1.ResourcePods:   *resource.NewQuantity(slots, resource.DecimalSI),
			},
			Free: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(4<<30, resource.BinarySI),
				corev1.ResourcePods:   *resource.NewQuantity(slots, resource.DecimalSI),
			},
		}
	}
	need := podNeeds(testPod("p", 100, 128))
	for _, s := range []PlacementStrategy{LeastAllocated(), MostAllocated()} {
		few, many := s.Score(base(29), need), s.Score(base(110), need)
		if few != many {
			t.Errorf("%s scored a 29-slot node %d and a 110-slot node %d on identical "+
				"cpu and memory; pod slots are feasibility, not score", s.Name(), few, many)
		}
	}
}

// -----------------------------------------------------------------------------
// Which node gets the pod
// -----------------------------------------------------------------------------

// TestLeastAllocatedPicksTheEmptiest and its Most counterpart are the behavior
// claim. Three same-typed nodes at different fill, one pod to place, and the two
// strategies must disagree about where it goes.
//
// The nodes are named so that sorted provider-ID order is neither the emptiest
// nor the fullest first. Otherwise a strategy that had no effect at all would
// pass one of the two tests by accident, which is how the first-fit behavior went
// unnoticed.
func TestLeastAllocatedPicksTheEmptiest(t *testing.T) {
	cat := BuildCatalog()
	// m7i.2xlarge: 8 cores, 32 GiB.
	half := node(t, cat, "i-a-half", "m7i.2xlarge", testPod("h1", 4000, 12288))
	full := node(t, cat, "i-b-full", "m7i.2xlarge",
		testPod("f1", 3000, 9216), testPod("f2", 3000, 9216))
	empty := node(t, cat, "i-c-empty", "m7i.2xlarge")

	c := clusterWith(t, cat, PlacementLeastAllocated, half, full, empty)
	dest, ok := c.placement(testPod("new", 500, 1024), "", "")
	if !ok {
		t.Fatal("nothing fit; the fixture has no room")
	}
	if dest.ProviderID != "i-c-empty" {
		t.Errorf("LeastAllocated placed on %s, want i-c-empty", dest.ProviderID)
	}
}

func TestMostAllocatedPicksTheFullest(t *testing.T) {
	cat := BuildCatalog()
	half := node(t, cat, "i-a-half", "m7i.2xlarge", testPod("h1", 4000, 12288))
	full := node(t, cat, "i-b-full", "m7i.2xlarge",
		testPod("f1", 3000, 9216), testPod("f2", 3000, 9216))
	empty := node(t, cat, "i-c-empty", "m7i.2xlarge")

	c := clusterWith(t, cat, PlacementMostAllocated, half, full, empty)
	dest, ok := c.placement(testPod("new", 500, 1024), "", "")
	if !ok {
		t.Fatal("nothing fit; the fixture has no room")
	}
	if dest.ProviderID != "i-b-full" {
		t.Errorf("MostAllocated placed on %s, want i-b-full", dest.ProviderID)
	}
}

// TestFirstFitIsTheOldBehavior pins the baseline explicitly. It used to be what
// every caller got without asking, and a test that depended on it depended on
// node naming without saying so.
func TestFirstFitIsTheOldBehavior(t *testing.T) {
	cat := BuildCatalog()
	half := node(t, cat, "i-a-half", "m7i.2xlarge", testPod("h1", 4000, 12288))
	full := node(t, cat, "i-b-full", "m7i.2xlarge",
		testPod("f1", 3000, 9216), testPod("f2", 3000, 9216))
	empty := node(t, cat, "i-c-empty", "m7i.2xlarge")

	c := clusterWith(t, cat, PlacementFirstFit, half, full, empty)
	dest, ok := c.placement(testPod("new", 500, 1024), "", "")
	if !ok {
		t.Fatal("nothing fit; the fixture has no room")
	}
	// The lowest provider ID that fits, which is neither the emptiest nor the
	// fullest. That is the arbitrariness the two real strategies exist to replace.
	if dest.ProviderID != "i-a-half" {
		t.Errorf("FirstFit placed on %s, want i-a-half, the lowest ID that fits", dest.ProviderID)
	}
}

// TestNilStrategyIsFirstFit covers a Cluster built by NewCluster and handed to a
// test that never sets the field. Nil has to mean the old behavior, or every
// hand-built fixture in the package starts placing pods somewhere new.
func TestNilStrategyIsFirstFit(t *testing.T) {
	cat := BuildCatalog()
	c := NewCluster([]NodeSnapshot{
		node(t, cat, "i-a-half", "m7i.2xlarge", testPod("h1", 4000, 12288)),
		node(t, cat, "i-c-empty", "m7i.2xlarge"),
	}, cat, 2)
	if c.Placement != nil {
		t.Fatal("NewCluster set a strategy; the test is measuring something else")
	}
	dest, ok := c.placement(testPod("new", 500, 1024), "", "")
	if !ok || dest.ProviderID != "i-a-half" {
		t.Errorf("nil strategy placed on %v (ok=%v), want i-a-half", dest, ok)
	}
}

// -----------------------------------------------------------------------------
// Capacity and pod slots still decide feasibility
// -----------------------------------------------------------------------------

// TestBothStrategiesRespectCapacity gives each strategy a pod that fits on
// exactly one node, and it is the node the strategy would rank last. Scoring must
// not be able to override a capacity verdict, and ranking last is the only case
// where a filter-then-sort implementation and a sort-then-filter one differ.
func TestBothStrategiesRespectCapacity(t *testing.T) {
	cat := BuildCatalog()
	// The big pod needs 6 cores. Only the empty 2xlarge has them, and it is
	// MostAllocated's least favorite node of the three.
	pod := testPod("big", 6000, 12288)

	for _, name := range []PlacementStrategyName{PlacementLeastAllocated, PlacementMostAllocated} {
		c := clusterWith(t, cat, name,
			node(t, cat, "i-a-full", "m7i.2xlarge",
				testPod("f1", 3500, 12288), testPod("f2", 3500, 12288)),
			node(t, cat, "i-b-part", "m7i.2xlarge", testPod("p1", 5000, 20480)),
			node(t, cat, "i-c-empty", "m7i.2xlarge"),
		)
		dest, ok := c.placement(pod, "", "")
		if !ok {
			t.Fatalf("%s: the only node with room was refused", name)
		}
		if dest.ProviderID != "i-c-empty" {
			t.Errorf("%s placed a 6-core pod on %s, which does not have 6 cores free",
				name, dest.ProviderID)
		}
	}
}

// TestBothStrategiesRespectPodSlots is the same test on the dimension a resource
// request cannot see. A node with cores and memory to spare and no pod slots left
// is full, and Cluster.free is the only thing that knows it.
//
// m7i.large carries 29 slots. Filling it with 29 tiny pods leaves plenty of cpu
// and memory, so a strategy scoring on cpu and memory alone ranks it highly under
// MostAllocated and must still refuse it.
func TestBothStrategiesRespectPodSlots(t *testing.T) {
	cat := BuildCatalog()
	packed := make([]*corev1.Pod, 0, 29)
	for i := 0; i < 29; i++ {
		packed = append(packed, testPod(fmt.Sprintf("tiny-%d", i), 10, 16))
	}

	for _, name := range []PlacementStrategyName{PlacementLeastAllocated, PlacementMostAllocated} {
		c := clusterWith(t, cat, name,
			node(t, cat, "i-a-slotfull", "m7i.large", packed...),
			node(t, cat, "i-b-room", "m7i.large", testPod("one", 200, 512)),
		)
		dest, ok := c.placement(testPod("new", 10, 16), "", "")
		if !ok {
			t.Fatalf("%s: nothing fit, though one node has 28 slots free", name)
		}
		if dest.ProviderID != "i-a-slotfull" {
			continue
		}
		t.Errorf("%s placed a pod on a node with 29 of 29 slots used", name)
	}
}

// TestNoFitReturnsFalse is the negative case for all three strategies. A pod
// bigger than anything standing has nowhere to go, and placement has to say so
// rather than return the best of a bad set.
func TestNoFitReturnsFalse(t *testing.T) {
	cat := BuildCatalog()
	// 64 cores, more than the m7i.large pair below has in total.
	huge := testPod("huge", 64000, 131072)
	for _, name := range PlacementStrategyNames() {
		c := clusterWith(t, cat, name,
			node(t, cat, "i-a", "m7i.large", testPod("a1", 500, 1024)),
			node(t, cat, "i-b", "m7i.large"),
		)
		if dest, ok := c.placement(huge, "", ""); ok {
			t.Errorf("%s placed a 64-core pod on %s", name, dest.ProviderID)
		}
	}
}

// TestPlacementSkipsTaintedDeletedAndExcluded keeps the filters that were there
// before scoring existed. A tainted node is being drained, a deleted one is gone,
// and the excluded one is the node the pod is leaving. Under MostAllocated all
// three are attractive, since a node about to be drained is the fullest thing
// around, so this is exactly where a scoring rewrite drops a filter.
func TestPlacementSkipsTaintedDeletedAndExcluded(t *testing.T) {
	cat := BuildCatalog()
	c := clusterWith(t, cat, PlacementMostAllocated,
		node(t, cat, "i-a-source", "m7i.2xlarge", testPod("s1", 6000, 20480)),
		node(t, cat, "i-b-tainted", "m7i.2xlarge", testPod("t1", 6000, 20480)),
		node(t, cat, "i-c-deleted", "m7i.2xlarge", testPod("d1", 6000, 20480)),
		node(t, cat, "i-d-open", "m7i.2xlarge"),
	)
	c.Nodes["i-b-tainted"].Tainted = true
	c.Nodes["i-c-deleted"].Deleted = true

	dest, ok := c.placement(testPod("new", 500, 1024), "i-a-source", "")
	if !ok {
		t.Fatal("nothing fit; the open node has room")
	}
	if dest.ProviderID != "i-d-open" {
		t.Errorf("placed on %s; tainted, deleted, and excluded nodes are not destinations",
			dest.ProviderID)
	}
}

// TestPreferBeatsTheStrategy pins the one case where the strategy does not
// decide. A REPLACE bought a node for these pods; sending them elsewhere would
// leave the launched node empty and make the proposal's savings arithmetic wrong.
// Under MostAllocated a fresh empty replacement is the worst-scoring node in the
// cluster, so without the preference every REPLACE would misplace its own pods.
func TestPreferBeatsTheStrategy(t *testing.T) {
	cat := BuildCatalog()
	c := clusterWith(t, cat, PlacementMostAllocated,
		node(t, cat, "i-a-full", "m7i.2xlarge", testPod("f1", 6000, 20480)),
	)
	id, err := c.Launch(instanceType(t, cat, "m7i.2xlarge"))
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	dest, ok := c.placement(testPod("new", 500, 1024), "", id)
	if !ok {
		t.Fatal("nothing fit")
	}
	if dest.ProviderID != id {
		t.Errorf("placed on %s, want the preferred replacement %s", dest.ProviderID, id)
	}
}

// -----------------------------------------------------------------------------
// Feasibility and the executor agree
// -----------------------------------------------------------------------------

// TestPodsStillFitUsesTheClusterStrategy is the wiring claim. Feasibility and the
// executor read one field, so a proposal PodsStillFit approves is one the executor
// can carry out.
//
// The fixture is a source whose pods fit only if they are packed. Three pods of 3
// cores each, two destinations with 6 cores free and one with 3. LeastAllocated
// spreads the first two pods across the two big nodes and then the third has 3
// cores available on the small one, so it fits either way here; what the test
// pins is that the checker's verdict and the executor's outcome match, which is
// the property that used to hold by coincidence.
func TestPodsStillFitUsesTheClusterStrategy(t *testing.T) {
	cat := BuildCatalog()
	for _, name := range PlacementStrategyNames() {
		src := node(t, cat, "i-a-source", "m7i.2xlarge",
			testPod("s1", 2000, 4096), testPod("s2", 2000, 4096), testPod("s3", 2000, 4096))
		nodes := []NodeSnapshot{
			src,
			node(t, cat, "i-b-dest", "m7i.2xlarge", testPod("b1", 2000, 8192)),
			node(t, cat, "i-c-dest", "m7i.2xlarge", testPod("c1", 4000, 16384)),
		}

		c := clusterWith(t, cat, name, nodes...)
		p := Proposal{Candidates: []NodeSnapshot{src}, Preconditions: []PreconditionKind{PodsStillFit}}
		checkErr := PodsStillFitChecker{Cluster: c}.Check(context.Background(), p)

		// Now actually drain it under the same strategy and see whether anything
		// stranded. The checker's verdict has to match the outcome.
		exec := clusterWith(t, cat, name, nodes...)
		if err := exec.Taint("i-a-source"); err != nil {
			t.Fatalf("%s: taint: %v", name, err)
		}
		for range src.ReschedulablePods {
			if err := exec.Evict("i-a-source", ""); err != nil {
				t.Fatalf("%s: evict: %v", name, err)
			}
		}
		stranded := len(exec.Pending)

		switch {
		case checkErr == nil && stranded > 0:
			t.Errorf("%s: PodsStillFit approved the move and the executor stranded %d pods",
				name, stranded)
		case checkErr != nil && stranded == 0:
			t.Errorf("%s: PodsStillFit refused the move (%v) and the executor placed every pod",
				name, checkErr)
		}
	}
}

// TestPodsStillFitStrandsUnderSpreadNotUnderPack is the disagreement the shared
// field prevents, shown by pointing the two strategies at the same cluster.
//
// Two empty destinations and a source holding two small pods and one that takes
// nearly a whole destination. Packing puts both small pods on the first
// destination and the big pod on the second, and everything fits. Spreading puts
// one small pod on each, leaving under half a node on each, and the big pod has
// nowhere to go. Same cluster, same pods, opposite verdict, which is why the
// checker cannot have its own policy.
func TestPodsStillFitStrandsUnderSpreadNotUnderPack(t *testing.T) {
	cat := BuildCatalog()
	// An m7i.xlarge is nominally 4 cores, and 3820m of it is available to workload
	// pods: the kubelet reserve and the daemonset overhead come off first, in
	// Catalog.Effective. The pod sizes are against that number and not against the
	// nominal one, which is what a 4000m "big" pod got wrong: it fit nowhere under
	// either strategy, so both verdicts were refusal and the test proved nothing.
	src := node(t, cat, "i-a-source", "m7i.2xlarge",
		testPod("small1", 1800, 2048), testPod("small2", 1800, 2048), testPod("big", 3800, 4096))
	nodes := []NodeSnapshot{
		src,
		node(t, cat, "i-b-dest", "m7i.xlarge"),
		node(t, cat, "i-c-dest", "m7i.xlarge"),
	}

	verdict := map[PlacementStrategyName]bool{}
	for _, name := range []PlacementStrategyName{PlacementLeastAllocated, PlacementMostAllocated} {
		c := clusterWith(t, cat, name, nodes...)
		p := Proposal{Candidates: []NodeSnapshot{src}, Preconditions: []PreconditionKind{PodsStillFit}}
		verdict[name] = PodsStillFitChecker{Cluster: c}.Check(context.Background(), p) == nil
	}

	if !verdict[PlacementMostAllocated] {
		t.Error("MostAllocated: packing places all three pods on two empty xlarges, but the check refused")
	}
	if verdict[PlacementLeastAllocated] {
		t.Error("LeastAllocated: spreading the two small pods leaves 2 cores on each node " +
			"and the 4-core pod nowhere to go, but the check approved")
	}
}

// -----------------------------------------------------------------------------
// Naming
// -----------------------------------------------------------------------------

func TestStrategyNameResolution(t *testing.T) {
	for name, want := range map[PlacementStrategyName]string{
		"":                      string(PlacementFirstFit),
		PlacementFirstFit:       string(PlacementFirstFit),
		PlacementLeastAllocated: string(PlacementLeastAllocated),
		PlacementMostAllocated:  string(PlacementMostAllocated),
	} {
		s, err := name.Strategy()
		if err != nil {
			t.Fatalf("%q: %v", name, err)
		}
		if s.Name() != want {
			t.Errorf("%q resolved to %s, want %s", name, s.Name(), want)
		}
	}
	if _, err := PlacementStrategyName("bin-packing").Strategy(); err == nil {
		t.Error("an unknown strategy name was accepted; a typo in a sweep would run the default silently")
	}
}
