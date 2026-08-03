package sim

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// gangPod builds a member of gang id with the given outage cost. outage of zero
// leaves the annotation off, which is the case where the cost has to be inferred
// from the members present.
func gangPod(name, id string, outage float64) *corev1.Pod {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: "default",
		Labels:    map[string]string{GangLabel: id},
	}}
	if outage > 0 {
		p.Annotations = map[string]string{
			GangOutageCostAnnotation: strconv.FormatFloat(outage, 'f', -1, 64),
		}
	}
	return p
}

// gangMembers returns n members of one gang.
func gangMembers(id string, n int, outage float64) []*corev1.Pod {
	out := make([]*corev1.Pod, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, gangPod(fmt.Sprintf("%s-m%d", id, i), id, outage))
	}
	return out
}

// The step is the whole claim. Breaking one member of a five-member gang costs
// the same as breaking three and the same as breaking all five, because the
// workload is all-or-nothing in value and the first eviction is what takes it
// down. Everything after that is free, so the marginal cost of the remaining
// members is zero.
func TestGangDisruptionIsAStepNotASum(t *testing.T) {
	const outage = 8.0
	all := gangMembers("g", 5, outage)

	want := PerNodeBaseDisruptionCost + outage
	for _, broken := range []int{1, 2, 3, 4, 5} {
		got := MoveDisruptionCost(1, all[:broken])
		if math.Abs(got-want) > 1e-9 {
			t.Errorf("breaking %d of 5: got %v want %v", broken, got, want)
		}
	}

	// The marginal cost of each member after the first is zero, stated as the
	// difference rather than inferred from the equalities above.
	for broken := 2; broken <= 5; broken++ {
		marginal := MoveDisruptionCost(1, all[:broken]) - MoveDisruptionCost(1, all[:broken-1])
		if marginal != 0 {
			t.Errorf("member %d cost %v at the margin, want 0", broken, marginal)
		}
	}

	// A sum is what this replaces. RescheduleDisruptionCost prices the same five
	// pods at five restarts, which is the defect.
	if summed := RescheduleDisruptionCost(all); summed != PerNodeBaseDisruptionCost+5 {
		t.Fatalf("premise broken: the sum priced 5 gang members at %v", summed)
	}
}

// The behavioral consequence, and the thing worth demonstrating: a move that
// takes a gang whole is no dearer than one that takes a single member of it.
// Every other pod in this simulator prefers fewer, and a gang inverts that.
func TestTakingAGangWholeIsNotDearerThanTakingOneMember(t *testing.T) {
	const outage = 8.0
	all := gangMembers("g", 5, outage)

	one := MoveDisruptionCost(1, all[:1])
	whole := MoveDisruptionCost(1, all)
	if whole > one {
		t.Errorf("taking 5 members cost %v against %v for one; a gang must not get dearer", whole, one)
	}

	// The same five pods without gang labels do get dearer, which is what the
	// inversion is against.
	loose := make([]*corev1.Pod, 0, 5)
	for i := 0; i < 5; i++ {
		loose = append(loose, pod(fmt.Sprintf("p%d", i), ""))
	}
	if MoveDisruptionCost(1, loose) <= MoveDisruptionCost(1, loose[:1]) {
		t.Error("non-gang pods should still cost more the more of them a move evicts")
	}
}

// A node of the same pod count carrying no gang costs the per-pod sum, so the
// step applies to gangs and nothing else. This is the control for the test above.
func TestNonGangNodeOfEqualCountCostsThePerPodSum(t *testing.T) {
	loose := make([]*corev1.Pod, 0, 5)
	for i := 0; i < 5; i++ {
		loose = append(loose, pod(fmt.Sprintf("p%d", i), ""))
	}
	want := PerNodeBaseDisruptionCost + 5
	if got := NodeDisruptionCost(loose); got != want {
		t.Errorf("five default-cost pods: got %v want %v", got, want)
	}
	// And it agrees with the sum term for term, so no existing number moved.
	if got, sum := NodeDisruptionCost(loose), RescheduleDisruptionCost(loose); got != sum {
		t.Errorf("gang-aware cost %v diverged from the sum %v on a node with no gangs", got, sum)
	}
}

// Two gangs are two outages. The step is per gang, not per move: a move that
// breaks two distinct gangs pays both, and only the repeats within one gang
// collapse.
func TestTwoGangsCostBoth(t *testing.T) {
	a := gangMembers("ga", 3, 8)
	b := gangMembers("gb", 4, 5)

	both := append(append([]*corev1.Pod{}, a...), b...)
	want := PerNodeBaseDisruptionCost + 8 + 5
	if got := MoveDisruptionCost(1, both); math.Abs(got-want) > 1e-9 {
		t.Errorf("two gangs: got %v want %v", got, want)
	}
	if got := GangsBroken(both); len(got) != 2 || got[0] != "ga" || got[1] != "gb" {
		t.Errorf("gangs broken: got %v want [ga gb]", got)
	}

	// Non-gang pods on the same move still sum on top of the two steps.
	mixed := append(append([]*corev1.Pod{}, both...), pod("loose1", ""), pod("loose2", ""))
	if got := MoveDisruptionCost(1, mixed); math.Abs(got-(want+2)) > 1e-9 {
		t.Errorf("two gangs plus two loose pods: got %v want %v", got, want+2)
	}
}

// The double-counting rule is scoped to the move. Members of one gang arriving
// from two different nodes are one charge, which is what makes a batch cheaper
// than the sum of its members and is the only place the rule bites.
func TestOneGangSpanningNodesIsChargedOncePerMove(t *testing.T) {
	const outage = 8.0
	nodeA := gangMembers("g", 5, outage)[:2]
	nodeB := gangMembers("g", 5, outage)[2:]

	alone := NodeDisruptionCost(nodeA) + NodeDisruptionCost(nodeB)
	together := MoveDisruptionCost(2, append(append([]*corev1.Pod{}, nodeA...), nodeB...))

	// Two baselines either way. The difference is the second outage charge.
	if want := 2*PerNodeBaseDisruptionCost + outage; math.Abs(together-want) > 1e-9 {
		t.Errorf("both nodes in one move: got %v want %v", together, want)
	}
	if alone <= together {
		t.Fatalf("draining the nodes separately cost %v, together %v; the cost must be subadditive", alone, together)
	}
	if math.Abs((alone-together)-outage) > 1e-9 {
		t.Errorf("the per-node sum over-charges by %v, want exactly one outage of %v", alone-together, outage)
	}
}

// The finding this test exists to record: a gang spanning nodes cannot be priced
// exactly from one node's own pods, and a node holding one of five members is
// charged the whole outage. Stage 1 stays local at the price of over-pricing such
// a node, which is safe in direction and not free in consequence.
func TestNodeLocalGangPricingOverchargesAndIsRecorded(t *testing.T) {
	const outage = 8.0
	// The same gang, once confined to one node and once split across two.
	confined := gangMembers("g", 2, outage)
	splitA := gangMembers("g", 2, outage)[:1]
	splitB := gangMembers("g", 2, outage)[1:]

	// Confined, the node's own cost is exact.
	if got, want := NodeDisruptionCost(confined), PerNodeBaseDisruptionCost+outage; got != want {
		t.Errorf("gang confined to one node: got %v want %v", got, want)
	}
	// Split, each node is charged the whole outage, so the two together read as two
	// outages when the truth is one.
	if NodeDisruptionCost(splitA) != NodeDisruptionCost(confined) {
		t.Error("a node holding one of two members should be charged the same outage as a node holding both")
	}
	perNode := NodeDisruptionCost(splitA) + NodeDisruptionCost(splitB)
	if want := 2*PerNodeBaseDisruptionCost + 2*outage; math.Abs(perNode-want) > 1e-9 {
		t.Errorf("per-node sum over a split gang: got %v want %v", perNode, want)
	}

	// The pool denominator corrects it, because disrupting the whole pool takes
	// each gang down once. Correcting it needs a read over every node in the pool,
	// which one node cannot do.
	nodes := []NodeSnapshot{
		{ProviderID: "i-a", Price: 1, ReschedulablePods: splitA, DisruptionCost: NodeDisruptionCost(splitA)},
		{ProviderID: "i-b", Price: 1, ReschedulablePods: splitB, DisruptionCost: NodeDisruptionCost(splitB)},
	}
	totals := Totals(nodes)
	if want := 2*PerNodeBaseDisruptionCost + outage; math.Abs(totals.DisruptionCost-want) > 1e-9 {
		t.Errorf("pool disruption cost: got %v want %v", totals.DisruptionCost, want)
	}
	// The per-node costs stages 1 and 2 read do not sum to the denominator they
	// divide by. That is the discrepancy, stated rather than hidden.
	if nodes[0].DisruptionCost+nodes[1].DisruptionCost == totals.DisruptionCost {
		t.Error("the sum of per-node costs equals the pool total; the over-count correction did nothing")
	}
}

// Confining a gang to one node must not change the pool total. The denominator is
// meant to measure how much disruptable work the pool holds, not how scattered
// its gangs happen to be, and an uncorrected sum would shrink every score in the
// pool because a gang got spread over two nodes instead of one.
func TestPoolTotalIsIndifferentToHowAGangIsScattered(t *testing.T) {
	const outage = 8.0
	four := gangMembers("g", 4, outage)

	confined := Totals([]NodeSnapshot{
		{ProviderID: "i-a", Price: 1, ReschedulablePods: four, DisruptionCost: NodeDisruptionCost(four)},
		{ProviderID: "i-b", Price: 1, ReschedulablePods: nil, DisruptionCost: NodeDisruptionCost(nil)},
	})
	scattered := Totals([]NodeSnapshot{
		{ProviderID: "i-a", Price: 1, ReschedulablePods: four[:2], DisruptionCost: NodeDisruptionCost(four[:2])},
		{ProviderID: "i-b", Price: 1, ReschedulablePods: four[2:], DisruptionCost: NodeDisruptionCost(four[2:])},
	})
	if math.Abs(confined.DisruptionCost-scattered.DisruptionCost) > 1e-9 {
		t.Errorf("pool total moved with the layout: confined %v, scattered %v",
			confined.DisruptionCost, scattered.DisruptionCost)
	}
}

// A gang member with no declared outage cost falls back to the largest eviction
// cost among the members present. That fall-back is degenerate and pinned as
// such: the step's height then depends on which members the move touches, which
// is the reason the annotation exists.
func TestUndeclaredGangCostFallsBackToTheLargestMember(t *testing.T) {
	cheap := gangPod("cheap", "g", 0)
	dear := gangPod("dear", "g", 0)
	dear.Annotations = map[string]string{corev1.PodDeletionCost: "134217728"} // +1, so 2.0

	if got, want := MoveDisruptionCost(1, []*corev1.Pod{cheap}), PerNodeBaseDisruptionCost+1; got != want {
		t.Errorf("cheap member alone: got %v want %v", got, want)
	}
	if got, want := MoveDisruptionCost(1, []*corev1.Pod{dear}), PerNodeBaseDisruptionCost+2; got != want {
		t.Errorf("dear member alone: got %v want %v", got, want)
	}
	// Both together still charge once, at the larger of the two.
	if got, want := MoveDisruptionCost(1, []*corev1.Pod{cheap, dear}), PerNodeBaseDisruptionCost+2; got != want {
		t.Errorf("both members: got %v want %v", got, want)
	}

	// A declared cost on any member overrides the fall-back for the whole gang,
	// and members disagreeing resolve to the largest declared value rather than to
	// whichever the caller enumerated first.
	declared := gangPod("declared", "g", 20)
	quieter := gangPod("quieter", "g", 5)
	if got, want := MoveDisruptionCost(1, []*corev1.Pod{cheap, declared}), PerNodeBaseDisruptionCost+20; got != want {
		t.Errorf("one declared member: got %v want %v", got, want)
	}
	if got, want := MoveDisruptionCost(1, []*corev1.Pod{quieter, declared}), PerNodeBaseDisruptionCost+20; got != want {
		t.Errorf("disagreeing declarations: got %v want %v", got, want)
	}
	if got, want := MoveDisruptionCost(1, []*corev1.Pod{declared, quieter}), PerNodeBaseDisruptionCost+20; got != want {
		t.Errorf("declaration order changed the answer: got %v want %v", got, want)
	}
}

// A gang priced at zero would be free to break, which is the one answer that
// cannot be right for a workload whose members must run together. A malformed,
// zero, negative or non-finite annotation is treated as absent, so the fall-back
// applies instead.
func TestMalformedGangCostFallsBackRatherThanPricingAtZero(t *testing.T) {
	for _, raw := range []string{"", "not-a-number", "0", "-4", "NaN", "+Inf"} {
		p := gangPod("m", "g", 0)
		p.Annotations = map[string]string{GangOutageCostAnnotation: raw}
		got := MoveDisruptionCost(1, []*corev1.Pod{p})
		if got <= PerNodeBaseDisruptionCost {
			t.Errorf("annotation %q priced the gang at %v, at or below the baseline", raw, got)
		}
	}
}

// An empty gang label is not a gang. Otherwise every pod a fixture labelled by
// accident would collapse into one shared workload and price as a single outage.
func TestEmptyGangLabelIsNotAGang(t *testing.T) {
	a := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: map[string]string{GangLabel: ""}}}
	b := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "b", Labels: map[string]string{GangLabel: ""}}}
	if got, want := MoveDisruptionCost(1, []*corev1.Pod{a, b}), PerNodeBaseDisruptionCost+2; got != want {
		t.Errorf("two empty-labelled pods: got %v want %v", got, want)
	}
	if got := GangsBroken([]*corev1.Pod{a, b}); len(got) != 0 {
		t.Errorf("gangs broken: got %v want none", got)
	}
}

// Totals must refuse a snapshot whose DisruptionCost came from the sum while its
// pods carry gang labels. The correction subtracts charges the sum never
// contained, so the denominator would come out understated and every score in the
// pool inflated, with the run still reporting success.
func TestTotalsRefusesGangPodsPricedBySum(t *testing.T) {
	four := gangMembers("g", 4, 40)
	nodes := []NodeSnapshot{
		{ProviderID: "i-a", Price: 1, ReschedulablePods: four[:2],
			DisruptionCost: RescheduleDisruptionCost(four[:2])},
		{ProviderID: "i-b", Price: 1, ReschedulablePods: four[2:],
			DisruptionCost: RescheduleDisruptionCost(four[2:])},
	}
	defer func() {
		if recover() == nil {
			t.Error("Totals accepted per-node costs that omitted the gang charge it then subtracted")
		}
	}()
	Totals(nodes)
}

// Raising gang prevalence must shrink the candidate set, because a gang member
// makes its node dearer to disrupt and the delete ratio is what stage 1 filters
// on. This is the run-level consequence of the step, and it is the number a claim
// about gang pricing would be made of.
func TestGangPrevalenceShrinksTheCandidateSet(t *testing.T) {
	var counts []int
	for _, frac := range []float64{0, 0.1, 0.3} {
		cat := BuildCatalog()
		opts := DefaultGenClusterOpts(200, 7)
		opts.Catalog = cat
		opts.Shape.GangFraction = frac
		opts.Shape.GangCount = 6
		nodes, totals := GenCluster(opts)
		bits := Identify(nodes, totals, 2)
		n := 0
		for _, b := range bits {
			if b {
				n++
			}
		}
		counts = append(counts, n)
		fmt.Printf("gangFraction=%.1f poolDisruption=%.1f candidates=%d/%d\n",
			frac, totals.DisruptionCost, n, len(nodes))
	}
	if !(counts[0] > counts[1] && counts[1] > counts[2]) {
		t.Errorf("candidates went %v as gang prevalence rose; more gangs must mean fewer candidates", counts)
	}
}

// The generator produces gangs at the prevalence asked for, spread over the
// number of gangs asked for, and lands members on more than one node. A fixture
// where every gang sits on a single node would never exercise the case the
// pricing exists for.
func TestGeneratedGangsSpanNodes(t *testing.T) {
	opts := DefaultGenClusterOpts(200, 9)
	opts.Shape.GangFraction = 0.15
	opts.Shape.GangCount = 6
	opts.Shape.GangOutageCost = 12
	nodes, _ := GenCluster(opts)

	members := 0
	total := 0
	nodesPerGang := map[string]map[string]bool{}
	for _, n := range nodes {
		for _, p := range n.ReschedulablePods {
			total++
			id, ok := GangID(p)
			if !ok {
				continue
			}
			members++
			if nodesPerGang[id] == nil {
				nodesPerGang[id] = map[string]bool{}
			}
			nodesPerGang[id][n.ProviderID] = true
			if c, ok := declaredGangOutageCost(p); !ok || c != 12 {
				t.Fatalf("member %s carries outage cost %v, want 12", p.Name, c)
			}
		}
	}
	if len(nodesPerGang) != 6 {
		t.Errorf("generated %d distinct gangs, want 6", len(nodesPerGang))
	}
	share := float64(members) / float64(total)
	if math.Abs(share-0.15) > 0.03 {
		t.Errorf("gang share %.3f, want ~0.15", share)
	}
	spanning := 0
	for _, ns := range nodesPerGang {
		if len(ns) > 1 {
			spanning++
		}
	}
	if spanning != len(nodesPerGang) {
		t.Errorf("%d of %d gangs span more than one node; the fixture needs spanning gangs",
			spanning, len(nodesPerGang))
	}
	fmt.Printf("gangs=%d members=%d/%d share=%.3f spanning=%d\n",
		len(nodesPerGang), members, total, share, spanning)
}

// Adding gangs must not move which pods carry anything else. The gang draws come
// off a substream keyed on the pod's name for exactly this reason: the seven
// per-pod flags above them all take a position in the shared stream, so an eighth
// would shift every draw after it and change every generated cluster.
func TestGangDrawDoesNotShiftTheRandomStream(t *testing.T) {
	base := DefaultGenClusterOpts(80, 5)
	base.Shape.ExpensiveFraction = 0.2
	base.Shape.ExpensiveDeletionCost = 134217728
	base.Shape.AntiAffinityFraction = 0.25
	base.Shape.PDBFraction = 0.25

	withGangs := base
	withGangs.Shape.GangFraction = 0.3
	withGangs.Shape.GangCount = 5

	describe := func(opts GenClusterOpts) []string {
		nodes, _ := GenCluster(opts)
		var out []string
		for _, n := range nodes {
			for _, p := range n.ReschedulablePods {
				out = append(out, fmt.Sprintf("%s/%s/%s/%v/%v",
					n.ProviderID, p.Name,
					p.Spec.Containers[0].Resources.Requests.Cpu().String(),
					p.Annotations[corev1.PodDeletionCost],
					(AntiAffinityConstraint{}).Applies(p)))
			}
		}
		return out
	}

	a, b := describe(base), describe(withGangs)
	if len(a) != len(b) {
		t.Fatalf("turning gangs on changed the pod set: %d then %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("turning gangs on changed pod %d: %s then %s", i, a[i], b[i])
		}
	}

	// And the gang assignment itself reproduces under a fixed seed. Membership per
	// pod name, not just the set of gang ids, because a stable id set with the
	// members shuffled between them is not a reproducible fixture.
	membership := func() []string {
		var out []string
		for _, p := range allPods(withGangs) {
			if id, ok := GangID(p); ok {
				out = append(out, p.Name+"="+id)
			}
		}
		return out
	}
	first, second := membership(), membership()
	if len(first) == 0 {
		t.Fatal("no gangs generated at fraction 0.3")
	}
	if fmt.Sprint(first) != fmt.Sprint(second) {
		t.Error("gang assignment did not reproduce under a fixed seed")
	}
}

// allPods flattens a generated cluster's pods, for tests that care about the pods
// and not the nodes.
func allPods(opts GenClusterOpts) []*corev1.Pod {
	nodes, _ := GenCluster(opts)
	var out []*corev1.Pod
	for _, n := range nodes {
		out = append(out, n.ReschedulablePods...)
	}
	return out
}

// The step has to survive into the batch scorer, or the pricing is right in the
// cost function and wrong everywhere it is used. A batch taking both halves of one
// gang scores strictly better than the same batch would if the gang were charged
// per node, which is what computeBatchConsolidation did when it summed
// c.DisruptionCost.
func TestBatchScoringChargesASpanningGangOnce(t *testing.T) {
	cat := BuildCatalog()
	const outage = 8.0
	four := gangMembers("g", 4, outage)

	// Two small nodes, each holding two members of one gang, and one large empty
	// destination with room for all four.
	a := typedNode(t, cat, "i-a", "c7i.large", four[:2]...)
	a.DisruptionCost = NodeDisruptionCost(a.ReschedulablePods)
	b := typedNode(t, cat, "i-b", "c7i.large", four[2:]...)
	b.DisruptionCost = NodeDisruptionCost(b.ReschedulablePods)
	dest := typedNode(t, cat, "i-dest", "c7i.4xlarge")
	dest.DisruptionCost = NodeDisruptionCost(nil)

	nodes := []NodeSnapshot{a, b, dest}
	totals := Totals(nodes)

	got, ok := computeBatchConsolidation([]NodeSnapshot{a, b}, []NodeSnapshot{dest}, cat, totals, 0)
	if !ok {
		t.Fatal("the batch produced no proposal")
	}

	// The batch's disruption cost, recovered from the score it was given.
	priced := MoveDisruptionCost(2, four)
	want := Score(a.Price+b.Price, priced, totals)
	if math.Abs(got.Score-want) > 1e-9 {
		t.Errorf("batch score %.6f, want %.6f from a single outage charge", got.Score, want)
	}
	perNode := Score(a.Price+b.Price, a.DisruptionCost+b.DisruptionCost, totals)
	if got.Score <= perNode {
		t.Errorf("batch scored %.6f, no better than the per-node sum's %.6f; the gang was charged twice",
			got.Score, perNode)
	}
}

// A gang member makes its node expensive enough to disrupt that the delete ratio
// can fall below the floor, which is the consequence of the step travelling into
// stage 1. Two nodes of identical price and pod count, one holding a gang member,
// and only the gang node is filtered out.
func TestGangMemberCanFilterItsNodeOutAtStageOne(t *testing.T) {
	loose := []*corev1.Pod{pod("l1", ""), pod("l2", "")}
	withGang := []*corev1.Pod{gangPod("g1", "g", 40), pod("l3", "")}

	nodes := []NodeSnapshot{
		{ProviderID: "i-loose", Price: 0.8064, ReschedulablePods: loose,
			DisruptionCost: NodeDisruptionCost(loose)},
		{ProviderID: "i-gang", Price: 0.8064, ReschedulablePods: withGang,
			DisruptionCost: NodeDisruptionCost(withGang)},
	}
	// Three more loose nodes, so no single node is a large share of the pool's
	// price. With only two nodes each is half the pool cost, and half the savings
	// carries the gang node over the floor whatever it costs to disrupt.
	for i := 0; i < 3; i++ {
		ps := []*corev1.Pod{pod(fmt.Sprintf("f%da", i), ""), pod(fmt.Sprintf("f%db", i), "")}
		nodes = append(nodes, NodeSnapshot{
			ProviderID:        fmt.Sprintf("i-filler-%d", i),
			Price:             0.8064,
			ReschedulablePods: ps,
			DisruptionCost:    NodeDisruptionCost(ps),
		})
	}
	totals := Totals(nodes)

	bits := Identify(nodes, totals, 2)
	if !bits[0] {
		t.Error("the node with two loose pods should be a candidate")
	}
	if bits[1] {
		t.Errorf("the node holding a gang member scored %.4f and stayed a candidate",
			DeleteRatio(nodes[1], totals))
	}
	// Identical price and pod count, so only the gang separates them.
	if len(loose) != len(withGang) || nodes[0].Price != nodes[1].Price {
		t.Fatal("fixture broken: the two nodes should differ only in the gang")
	}
}
