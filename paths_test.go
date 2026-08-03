package sim

import (
	"context"
	"fmt"
	"math"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// -----------------------------------------------------------------------------
// Fixture helpers
// -----------------------------------------------------------------------------

var testPool = &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}

// sizedPod requests an exact CPU and memory, with an optional deletion cost. The
// exact numbers are what the monotonicity fixture rests on, so a pod drawn from a
// distribution will not do.
func sizedPod(name string, cpuMilli, memMiB int64, deletionCost string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "app",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewMilliQuantity(cpuMilli, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(memMiB*(1<<20), resource.BinarySI),
			}},
		}}},
	}
	if deletionCost != "" {
		p.Annotations = map[string]string{corev1.PodDeletionCost: deletionCost}
	}
	return p
}

// typedNode builds a snapshot on a named catalog type at its catalog price.
func typedNode(t *testing.T, cat *Catalog, id, itName string, pods ...*corev1.Pod) NodeSnapshot {
	t.Helper()
	it := instanceTypeNamed(cat, itName)
	price, ok := cheapestOffering(it)
	if !ok {
		t.Fatalf("%s has no available offering", itName)
	}
	return NodeSnapshot{
		ProviderID:        id,
		NodePool:          testPool,
		InstanceType:      it,
		Price:             price,
		ReschedulablePods: pods,
		DisruptionCost:    RescheduleDisruptionCost(pods),
	}
}

// -----------------------------------------------------------------------------
// The ordering of the three paths
// -----------------------------------------------------------------------------

// Upstream's NewMethods orders the methods and Reconcile walks them in that
// order. This pins the order and, more usefully, the claim-and-withhold rule
// between them: emptiness takes the empty nodes and multi-node takes its batch
// before single-node walks.
func TestPathsRunInUpstreamOrder(t *testing.T) {
	cat := BuildCatalog()
	opts := DefaultGenClusterOpts(200, 1)
	opts.Catalog = cat
	nodes, totals := GenCluster(opts)
	cfg := DefaultRunConfig(2, 20)
	dests := EligibleDestinations(nodes, cat, totals, cfg.Bands)

	props, stats := EvaluateInOrder(nodes, dests, cat, totals, cfg)

	for _, p := range []Path{PathEmpty, PathMultiNode, PathSingleNode} {
		s := stats[p]
		fmt.Printf("%-7s candidates=%3d proposals=%3d nodes=%3d savings=$%.4f probes=%d batch=%d\n",
			p, s.Candidates, s.Proposals, s.Nodes, s.Savings, s.Probes, s.BatchSize)
	}

	// Every path contributed, or the fixture is not exercising the split.
	for _, p := range []Path{PathEmpty, PathMultiNode, PathSingleNode} {
		if stats[p].Proposals == 0 {
			t.Errorf("the %s path produced nothing on the default fixture", p)
		}
	}

	// No node appears in two proposals. This is the withholding rule, and without
	// it the compactor would drop the duplicates and the per-path attribution
	// above would be counting work that never happens.
	seen := map[string]string{}
	for _, p := range props {
		for _, c := range p.Candidates {
			if prior, dup := seen[c.ProviderID]; dup {
				t.Errorf("node %s is in both %s and %s", c.ProviderID, prior, p.ID)
			}
			seen[c.ProviderID] = p.ID
		}
	}

	// Emptiness owns every empty node and no other path may see one.
	emptyIDs := map[string]bool{}
	for _, n := range nodes {
		if Disruptable(n) && IsEffectivelyEmpty(n.DisruptionCost) {
			emptyIDs[n.ProviderID] = true
		}
	}
	if len(emptyIDs) == 0 {
		t.Fatal("premise broken: the fixture has no empty nodes")
	}
	for _, p := range props {
		for _, c := range p.Candidates {
			if emptyIDs[c.ProviderID] && p.Reason != ReasonEmpty {
				t.Errorf("empty node %s was claimed by a %s proposal", c.ProviderID, p.Reason)
			}
			if !emptyIDs[c.ProviderID] && p.Reason == ReasonEmpty {
				t.Errorf("non-empty node %s was claimed by the emptiness path", c.ProviderID)
			}
		}
	}
}

// StopAfterFirstPath is upstream's Reconcile: "We'll only let one method perform
// an action," then requeue immediately. A pass with it off runs all three, which
// is the divergence a per-pass savings number rests on.
func TestStopAfterFirstPath(t *testing.T) {
	cat := BuildCatalog()
	opts := DefaultGenClusterOpts(200, 1)
	opts.Catalog = cat
	nodes, totals := GenCluster(opts)
	cfg := DefaultRunConfig(2, 20)
	cfg.StopAfterFirstPath = true
	dests := EligibleDestinations(nodes, cat, totals, cfg.Bands)

	props, stats := EvaluateInOrder(nodes, dests, cat, totals, cfg)

	// The fixture has empty nodes, so emptiness acts and the other two never run.
	if len(props) != 1 || props[0].Reason != ReasonEmpty {
		t.Fatalf("want one emptiness proposal, got %d", len(props))
	}
	if stats[PathMultiNode].Candidates != 0 || stats[PathSingleNode].Candidates != 0 {
		t.Errorf("a later path ran after the first one acted: multi=%d single=%d",
			stats[PathMultiNode].Candidates, stats[PathSingleNode].Candidates)
	}
}

// Single-node is the fallback. When multi-node finds a batch, the nodes in it are
// gone from single-node's candidate list.
func TestSingleNodeIsTheFallback(t *testing.T) {
	cat := BuildCatalog()
	opts := DefaultGenClusterOpts(200, 1)
	opts.Catalog = cat
	nodes, totals := GenCluster(opts)
	cfg := DefaultRunConfig(2, 20)
	dests := EligibleDestinations(nodes, cat, totals, cfg.Bands)

	// Multi-node alone, to learn its batch.
	multiCands := make([]NodeSnapshot, 0, len(nodes))
	for _, n := range nodes {
		if Disruptable(n) && !IsEffectivelyEmpty(n.DisruptionCost) {
			multiCands = append(multiCands, n)
		}
	}
	batch, _, ok := MultiNodeEvaluation{Threshold: cfg.Threshold}.
		EvaluateCounting(multiCands, dests, cat, totals, poolBudgets(nodes, cfg.Budget))
	if !ok {
		t.Fatal("premise broken: multi-node found no batch on the default fixture")
	}
	inBatch := map[string]bool{}
	for _, c := range batch.Candidates {
		inBatch[c.ProviderID] = true
	}

	props, _ := EvaluateInOrder(nodes, dests, cat, totals, cfg)
	for _, p := range props {
		if len(p.Candidates) > 1 {
			continue
		}
		if inBatch[p.Candidates[0].ProviderID] {
			t.Errorf("single-node proposed %s, which multi-node already batched", p.Candidates[0].ProviderID)
		}
	}
}

// MaxProposals of one reproduces upstream's per-reconcile behavior:
// SingleNodeConsolidation returns after the first valid command it finds, walking
// candidates in sorted order.
func TestSingleNodeReturnsFirstValidCommand(t *testing.T) {
	cat := BuildCatalog()
	opts := DefaultGenClusterOpts(120, 5)
	opts.Catalog = cat
	nodes, totals := GenCluster(opts)
	cfg := DefaultRunConfig(2, 20)
	dests := EligibleDestinations(nodes, cat, totals, cfg.Bands)

	cands := make([]NodeSnapshot, 0, len(nodes))
	for _, n := range nodes {
		if Disruptable(n) && !IsEffectivelyEmpty(n.DisruptionCost) {
			cands = append(cands, n)
		}
	}

	uncapped := SingleNodeEvaluation{Threshold: cfg.Threshold}.
		Evaluate(cands, dests, cat, totals, poolBudgets(nodes, cfg.Budget))
	capped := SingleNodeEvaluation{Threshold: cfg.Threshold, MaxProposals: 1}.
		Evaluate(cands, dests, cat, totals, poolBudgets(nodes, cfg.Budget))

	if len(uncapped) < 2 {
		t.Fatalf("premise broken: the uncapped walk found %d proposals", len(uncapped))
	}
	if len(capped) != 1 {
		t.Fatalf("capped walk returned %d proposals, want 1", len(capped))
	}
	// The capped walk must return the first the uncapped one found, or the two are
	// walking different orders and only one of them is upstream's.
	if capped[0].ID != uncapped[0].ID {
		t.Errorf("capped returned %s, uncapped's first was %s", capped[0].ID, uncapped[0].ID)
	}
}

// -----------------------------------------------------------------------------
// Candidate ranking
// -----------------------------------------------------------------------------

// Upstream's own sortCandidates cases, from balanced_scoring_test.go, against
// this implementation. Its numbers are asserted rather than recomputed, so a
// change to SavingsRatio here fails against upstream's expectations and not
// against ours.
func TestSortCandidatesMatchesUpstream(t *testing.T) {
	// "should sort Balanced candidates by savings ratio (highest first)".
	// Upstream sets Candidate.DisruptionCost to 100, 1 and 50 on A, B and C to
	// prove the sort reads RescheduleDisruptionCost and not DisruptionCost. This
	// simulator carries only the reschedule cost, so that half of the case cannot
	// be reproduced; the ratios and the resulting order can.
	//
	// A: $10 over 1 pod  -> 10 / 2.0  = 5.0
	// B: $1  over 8 pods -> 1  / 9.0  = 0.11
	// C: $5  over 3 pods -> 5  / 4.0  = 1.25
	mk := func(id string, price float64, pods int) NodeSnapshot {
		ps := make([]*corev1.Pod, pods)
		for i := range ps {
			ps[i] = pod(fmt.Sprintf("%s-p%d", id, i), "")
		}
		return NodeSnapshot{
			ProviderID:        id,
			NodePool:          testPool,
			Price:             price,
			ReschedulablePods: ps,
			DisruptionCost:    RescheduleDisruptionCost(ps),
		}
	}
	candA, candB, candC := mk("node-a", 10.0, 1), mk("node-b", 1.0, 8), mk("node-c", 5.0, 3)
	for _, tc := range []struct {
		n    NodeSnapshot
		want float64
	}{{candA, 5.0}, {candB, 1.0 / 9.0}, {candC, 1.25}} {
		if got := SavingsRatio(tc.n); math.Abs(got-tc.want) > 0.01 {
			t.Errorf("%s ratio: got %.4f want %.4f", tc.n.ProviderID, got, tc.want)
		}
	}
	sorted := sortBySavingsRatio([]NodeSnapshot{candB, candC, candA})
	for i, want := range []string{"node-a", "node-c", "node-b"} {
		if sorted[i].ProviderID != want {
			t.Errorf("position %d: got %s want %s", i, sorted[i].ProviderID, want)
		}
	}

	// "should sort non-Balanced candidates by savings ratio descending". Same
	// price, different disruption costs, so the ratio is the only discriminator.
	// Upstream sets RescheduleDisruptionCost directly; here it comes from pods
	// whose deletion costs produce the same totals.
	//
	// A: 4.84 / 10 = 0.484, B: 4.84 / 1 = 4.84, C: 4.84 / 5 = 0.968
	sameA := NodeSnapshot{ProviderID: "node-a", NodePool: testPool, Price: 4.84, DisruptionCost: 10.0}
	sameB := NodeSnapshot{ProviderID: "node-b", NodePool: testPool, Price: 4.84, DisruptionCost: 1.0}
	sameC := NodeSnapshot{ProviderID: "node-c", NodePool: testPool, Price: 4.84, DisruptionCost: 5.0}
	sorted = sortBySavingsRatio([]NodeSnapshot{sameA, sameB, sameC})
	for i, want := range []string{"node-b", "node-c", "node-a"} {
		if sorted[i].ProviderID != want {
			t.Errorf("uniform-price position %d: got %s want %s", i, sorted[i].ProviderID, want)
		}
	}

	// "should compute expected ratios for different configurations", upstream's
	// Candidate.SavingsRatio cases. A node with no pods carries the per-node base
	// alone, so its ratio is its price.
	noPods := NodeSnapshot{ProviderID: "n", Price: 4.84, DisruptionCost: RescheduleDisruptionCost(nil)}
	if got := SavingsRatio(noPods); math.Abs(got-4.84) > 0.01 {
		t.Errorf("no pods: got %.4f want 4.84", got)
	}
	threePods := mk("n3", 4.84, 3)
	if got := SavingsRatio(threePods); math.Abs(got-1.21) > 0.01 {
		t.Errorf("three pods: got %.4f want 1.21", got)
	}
	// "Nil instanceType: ratio = 0". Upstream's resolveNodePrice returns 0 for a
	// nil instance type, so the ratio is 0. Totals rejects a zero-priced snapshot,
	// which is why this is asserted on SavingsRatio directly.
	if got := SavingsRatio(NodeSnapshot{Price: 0, DisruptionCost: 4.0}); got != 0 {
		t.Errorf("zero price: got %.4f want 0", got)
	}
}

// The ranking divergence, as an assertion rather than a printed aside.
//
// Upstream ranks candidates by SavingsRatio, always the node's full price. This
// simulator's compactor ranks proposals by Score, which for a REPLACE uses the
// price difference. On the tight fixture the two disagree by a factor of three on
// the same node.
func TestSavingsRatioAndScoreDisagree(t *testing.T) {
	nodes, totals, cat, cfg := tightRun(t)
	over := nodeByID(t, nodes, OversizedProviderID)

	_, replaces := splitByKind(generateFor(nodes, totals, cat, cfg, OversizedProviderID))
	if len(replaces) != 1 {
		t.Fatalf("want one REPLACE from the tight fixture, got %d", len(replaces))
	}
	rep := replaces[0]

	// Upstream's number: the full price over the disruption cost, unnormalized.
	upstream := SavingsRatio(over)
	// What the move is actually worth, on the same unnormalized scale.
	move := rep.Savings / over.DisruptionCost

	fmt.Printf("ranking divergence on %s: SavingsRatio %.5f, move value %.5f, %.2fx\n",
		over.ProviderID, upstream, move, upstream/move)

	if upstream <= move {
		t.Fatalf("premise broken: the full price %.5f should exceed the replace's %.5f", upstream, move)
	}
	// The factor is the node price over the replace savings, which is what the
	// number reduces to. Pinned loosely, because it is a property of the catalog.
	if got := upstream / move; got < 2.5 || got > 4.0 {
		t.Errorf("divergence factor %.2fx is outside the 2.5-4.0 range the fixture produces", got)
	}
}

// Score and SavingsRatio order candidates identically within one pool, because
// the pool totals are a constant factor on both terms. That is why the divergence
// above is about the DELETE-versus-REPLACE numerator and not about normalization.
func TestNormalizationDoesNotReorderWithinAPool(t *testing.T) {
	cat := BuildCatalog()
	opts := DefaultGenClusterOpts(150, 13)
	opts.Catalog = cat
	nodes, totals := GenCluster(opts)

	byRatio := sortBySavingsRatio(nodes)
	for i := 1; i < len(byRatio); i++ {
		a, b := byRatio[i-1], byRatio[i]
		// The delete score is the ratio scaled by PoolDisruption/PoolCost, so the
		// comparison must agree wherever the ratios differ.
		if SavingsRatio(a) == SavingsRatio(b) {
			continue
		}
		sa := Score(a.Price, a.DisruptionCost, totals)
		sb := Score(b.Price, b.DisruptionCost, totals)
		if sa < sb {
			t.Fatalf("%s ranks above %s by ratio but below by delete score (%.6f vs %.6f)",
				a.ProviderID, b.ProviderID, sa, sb)
		}
	}
}

// -----------------------------------------------------------------------------
// The binary search
// -----------------------------------------------------------------------------

// The window arithmetic, which is easy to get wrong by one. A probe at mid tests
// candidates[0 : mid+1], so the smallest batch ever probed is two, and a search
// over N candidates probes at most floor(log2(N)) + 1 times.
func TestBinarySearchWindow(t *testing.T) {
	cat := BuildCatalog()
	opts := DefaultGenClusterOpts(400, 21)
	opts.Catalog = cat
	nodes, totals := GenCluster(opts)
	cfg := DefaultRunConfig(2, 400)
	dests := EligibleDestinations(nodes, cat, totals, cfg.Bands)

	cands := make([]NodeSnapshot, 0, len(nodes))
	for _, n := range nodes {
		if Disruptable(n) && !IsEffectivelyEmpty(n.DisruptionCost) {
			cands = append(cands, n)
		}
	}
	p, probes, ok := MultiNodeEvaluation{Threshold: cfg.Threshold}.
		EvaluateCounting(cands, dests, cat, totals, poolBudgets(nodes, cfg.Budget))
	if !ok {
		t.Fatal("no batch found")
	}
	fmt.Printf("binary search over %d candidates: %d probes, batch %d\n", len(cands), probes, len(p.Candidates))

	if len(p.Candidates) < 2 {
		t.Errorf("batch of %d; multi-node always operates on at least two", len(p.Candidates))
	}
	// The window is over an index in [1, min(len-1, 100-1)], so the bound is the
	// log of the window and not of the candidate count.
	window := len(cands) - 1
	if window > MultiNodeMaxParallel-1 {
		window = MultiNodeMaxParallel - 1
	}
	if maxProbes := int(math.Log2(float64(window))) + 1; probes > maxProbes {
		t.Errorf("%d probes over a window of %d; a bisection takes at most %d", probes, window, maxProbes)
	}
	// The cap is 101 and not 100, and that is upstream's, reproduced. maxParallel
	// clamps to 100, but the comparison that narrows it is `len(candidates) <=
	// max`, which is false once there are more than 100 candidates, so max stays
	// 100. mid then reaches 100 and the batch is candidates[0 : 101]. Upstream's
	// comment says "Only consider a maximum batch of 100 NodeClaims"; the code
	// considers 101. Below 101 candidates the branch does fire and the cap is
	// exact, which is why the off-by-one only appears on large clusters.
	if len(p.Candidates) > MultiNodeMaxParallel+1 {
		t.Errorf("batch of %d exceeds even the off-by-one cap of %d",
			len(p.Candidates), MultiNodeMaxParallel+1)
	}
	if len(cands) > MultiNodeMaxParallel && len(p.Candidates) == MultiNodeMaxParallel+1 {
		fmt.Printf("batch reached %d, upstream's off-by-one on a documented cap of %d\n",
			len(p.Candidates), MultiNodeMaxParallel)
	}
	// Fewer than two candidates is upstream's early return: "for single NodeClaims
	// standard consolidation will find all solutions."
	if _, _, single := (MultiNodeEvaluation{Threshold: cfg.Threshold}).
		EvaluateCounting(cands[:1], dests, cat, totals, poolBudgets(nodes, cfg.Budget)); single {
		t.Error("multi-node returned a command for one candidate")
	}
}

// TestMultiNodeMonotonicityViolation is the finding.
//
// Upstream's binary search assumes the predicate is monotone in the batch size:
// if removing N nodes is allowed, removing N-1 is too. That is what lets a
// bisection work at all, and upstream states the weak form of it in a comment on
// sortCandidates, that the search "still converges because it shrinks the window
// until scheduling succeeds."
//
// The assumption holds for a DELETE. A delete-only batch's savings and disruption
// are both sums over its members, so the batch's ratio is the mediant of its
// members' ratios, and the mediant of a prefix of a list sorted by individual
// ratio is non-increasing in the prefix length. Every shorter prefix of a passing
// prefix therefore passes.
//
// It fails for a REPLACE, and the reason is the shared replacement. One
// replacement price is subtracted from the whole batch's savings, so it is
// amortized over however many nodes the batch removes. Dropping a node removes
// its price from the numerator and its disruption from the denominator while the
// replacement price stays put, and the two move independently. A batch of three
// can clear a threshold that the batch of two inside it fails.
//
// The fixture below is that case, built to the arithmetic rather than searched
// for. Three candidates, sorted correctly by SavingsRatio, in a cluster with no
// slack, so every prefix takes the REPLACE path and they all buy the same
// replacement type:
//
//	prefix 2: savings = p1 + p2 - r,      disruption = d1 + d2
//	prefix 3: savings = p1 + p2 + p3 - r, disruption = d1 + d2 + d3
//
// The third node is priced so that p3/d3 lands above prefix 2's net ratio and
// below node 2's gross ratio. The first keeps the score rising; the second keeps
// the sort order upstream's.
//
// The search then does this: window [1,2], probe mid=1, batch = prefix 2, fails,
// max drops to 0, loop ends, nothing returned. It never probes prefix 3, which
// passes. An exhaustive scan finds it. What upstream loses on this cluster is the
// whole move.
func TestMultiNodeMonotonicityViolation(t *testing.T) {
	cat := BuildCatalog()

	// Nodes 1 and 2: m7i.8xlarge at $1.6128, each holding one large bare pod. One
	// pod means the disruption cost is the baseline plus one, which is the smallest
	// a non-empty node can carry, so the gross ratio is as high as the catalog
	// allows.
	n1 := typedNode(t, cat, "i-heavy-1", "m7i.8xlarge", sizedPod("h1", 7000, 61440, ""))
	n2 := typedNode(t, cat, "i-heavy-2", "m7i.8xlarge", sizedPod("h2", 7000, 61440, ""))
	// Node 3: c7i.8xlarge at $1.4272 holding one tiny pod. Cheaper than the first
	// two, so it sorts last, and its pod is small enough to fit in the slack the
	// replacement already has.
	n3 := typedNode(t, cat, "i-light", "c7i.8xlarge", sizedPod("l1", 1000, 4096, ""))
	// A node in the same pool that is not a candidate this pass, present only in
	// the denominators. Upstream's computeNodePoolTotals is explicit that
	// "non-candidate nodes still contribute to the denominators," and its
	// ConsolidateAfter and Consolidatable tests are what exclude a node from the
	// candidate list after the totals are computed. NodeSnapshot carries neither
	// field, which is why this is expressed by leaving the node out of the
	// candidate slice rather than by a predicate.
	filler := typedNode(t, cat, "i-filler", "r7i.16xlarge", sizedPod("f1", 1000, 4096, ""))

	candidates := []NodeSnapshot{n1, n2, n3}
	totals := Totals(append(append([]NodeSnapshot{}, candidates...), filler))
	// No destinations. A cluster with no slack anywhere is the only regime in which
	// a REPLACE happens at all, which is the same premise GenTightCluster supplies.
	var dests []NodeSnapshot

	// The sort order is upstream's, or the prefixes are not the ones it would test.
	sorted := sortBySavingsRatio(candidates)
	for i := range sorted {
		if sorted[i].ProviderID != candidates[i].ProviderID {
			t.Fatalf("fixture broken: sorted order is %s..., want %s...",
				sorted[0].ProviderID, candidates[0].ProviderID)
		}
	}
	fmt.Printf("ratios: %s=%.4f %s=%.4f %s=%.4f\n",
		n1.ProviderID, SavingsRatio(n1), n2.ProviderID, SavingsRatio(n2), n3.ProviderID, SavingsRatio(n3))

	const threshold = 0.5
	_, ok2 := computeBatchConsolidation(candidates[:2], dests, cat, totals, threshold)
	_, ok3 := computeBatchConsolidation(candidates[:3], dests, cat, totals, threshold)

	// Both prefixes must be on the REPLACE path and must buy the same type. A
	// prefix that fell back to a DELETE, or that bought something different, would
	// be a violation of a different mechanism than the shared-replacement one.
	p2, _ := computeBatchConsolidation(candidates[:2], dests, cat, totals, 0)
	p3, _ := computeBatchConsolidation(candidates[:3], dests, cat, totals, 0)
	if len(p2.Replacements) != 1 || len(p3.Replacements) != 1 {
		t.Fatalf("fixture broken: prefix 2 has %d replacements, prefix 3 has %d",
			len(p2.Replacements), len(p3.Replacements))
	}
	if p2.Replacements[0].Name != p3.Replacements[0].Name {
		t.Fatalf("fixture broken: prefix 2 buys %s, prefix 3 buys %s",
			p2.Replacements[0].Name, p3.Replacements[0].Name)
	}
	fmt.Printf("both prefixes replace with %s; prefix2 score=%.4f savings=$%.4f, prefix3 score=%.4f savings=$%.4f\n",
		p2.Replacements[0].Name, p2.Score, p2.Savings, p3.Score, p3.Savings)

	// The violation itself: the larger batch passes and the smaller one inside it
	// does not.
	if ok2 {
		t.Fatalf("prefix 2 passed at %.4f; the fixture needs it to fail", p2.Score)
	}
	if !ok3 {
		t.Fatalf("prefix 3 failed at %.4f; the fixture needs it to pass", p3.Score)
	}

	// What the search does with it. It probes prefix 2 alone and returns nothing.
	m := MultiNodeEvaluation{Threshold: threshold}
	got, probes, found := m.firstNConsolidationOption(candidates, dests, cat, totals, MultiNodeMaxParallel)
	if probes != 1 {
		t.Errorf("the search took %d probes over three candidates, want 1", probes)
	}
	if found {
		t.Errorf("the search found a batch of %d; upstream's bisection cannot reach prefix 3 from a failing prefix 2",
			len(got.Candidates))
	}

	// The exhaustive scan finds what the search missed, which is what sizes the
	// loss. Same predicate, every prefix tried.
	best, exhaustive := m.ExhaustiveOption(candidates, dests, cat, totals)
	if !exhaustive {
		t.Fatal("the exhaustive scan found nothing either; then this is not a search defect")
	}
	if len(best.Candidates) != 3 {
		t.Errorf("the scan found a batch of %d, want 3", len(best.Candidates))
	}
	fmt.Printf("MONOTONICITY VIOLATION: bisection finds nothing, exhaustive finds a batch of %d worth $%.4f/hr at score %.4f\n",
		len(best.Candidates), best.Savings, best.Score)

	// And the whole path returns nothing on this cluster, which is the consequence
	// rather than the mechanism.
	props := m.Evaluate(candidates, dests, cat, totals, poolBudgets(candidates, 10))
	if len(props) != 0 {
		t.Errorf("Evaluate returned %d proposals; the search inside it found nothing", len(props))
	}
}

// The other half of the finding: a DELETE-only batch cannot violate
// monotonicity, because the batch ratio is the mediant of its members' and the
// mediant of a prefix sorted by individual ratio is non-increasing. Checked over
// the default fixture rather than argued, since a mediant argument is only worth
// something if the code agrees with it.
func TestDeleteBatchesAreMonotone(t *testing.T) {
	cat := BuildCatalog()
	opts := DefaultGenClusterOpts(300, 17)
	opts.Catalog = cat
	nodes, totals := GenCluster(opts)
	cands := make([]NodeSnapshot, 0, len(nodes))
	for _, n := range nodes {
		if Disruptable(n) && !IsEffectivelyEmpty(n.DisruptionCost) {
			cands = append(cands, n)
		}
	}
	cands = sortBySavingsRatio(cands)
	if len(cands) > 60 {
		cands = cands[:60]
	}

	// The delete score over each prefix, ignoring feasibility, which is the term
	// the mediant argument is about.
	prev := math.Inf(1)
	checked := 0
	for n := 2; n <= len(cands); n++ {
		sumP, sumD := 0.0, 0.0
		for _, c := range cands[:n] {
			sumP += c.Price
			sumD += c.DisruptionCost
		}
		score := Score(sumP, sumD, totals)
		if score > prev+1e-12 {
			t.Errorf("prefix %d scores %.6f, above prefix %d's %.6f; a delete batch's score must not rise with the prefix",
				n, score, n-1, prev)
		}
		prev = score
		checked++
	}
	if checked < 10 {
		t.Fatalf("only %d prefixes checked", checked)
	}
	fmt.Printf("delete prefixes over %d candidates: score falls monotonically to %.6f\n", len(cands), prev)
}

// The batch's action DAG has to carry the same ordering guarantee the single-node
// REPLACE does: no pod leaves any candidate until the shared replacement is up,
// and no node is deleted until its own pods are gone.
func TestBatchActionDAGOrders(t *testing.T) {
	cat := BuildCatalog()
	n1 := typedNode(t, cat, "i-1", "m7i.2xlarge", sizedPod("a1", 1000, 4096, ""), sizedPod("a2", 1000, 4096, ""))
	n2 := typedNode(t, cat, "i-2", "m7i.2xlarge", sizedPod("b1", 1000, 4096, ""))
	batch := []NodeSnapshot{n1, n2}
	it := instanceTypeNamed(cat, "c7i.2xlarge")

	actions := batchActions(batch, it)

	// Every index depends only on earlier indices, which is what the executor
	// requires and what it errors on rather than stopping for.
	for i, a := range actions {
		for _, dep := range a.DependsOn {
			if dep >= i {
				t.Errorf("action %d depends on %d, which is not earlier", i, dep)
			}
		}
	}

	launch := -1
	for i, a := range actions {
		if a.Kind == Launch {
			if launch >= 0 {
				t.Fatalf("a batch launched twice, at %d and %d", launch, i)
			}
			launch = i
		}
	}
	if launch < 0 {
		t.Fatal("no Launch in a batch REPLACE")
	}
	// Position is not the guarantee. An Evict that sits after the Launch but does
	// not depend on it can still fire first.
	evictsByNode := map[string][]int{}
	for i, a := range actions {
		if a.Kind != Evict {
			continue
		}
		evictsByNode[a.ProviderID] = append(evictsByNode[a.ProviderID], i)
		found := false
		for _, dep := range a.DependsOn {
			if dep == launch {
				found = true
			}
		}
		if !found {
			t.Errorf("Evict at %d does not depend on the Launch at %d", i, launch)
		}
	}
	for _, n := range batch {
		if got := len(evictsByNode[n.ProviderID]); got != len(n.ReschedulablePods) {
			t.Errorf("%s: %d Evicts for %d pods", n.ProviderID, got, len(n.ReschedulablePods))
		}
	}
	// Each Delete waits on its own node's Evicts and nothing else's. A Delete that
	// fires early strands whatever is still on the node; one that waits on another
	// node's evictions serializes work that need not be.
	for i, a := range actions {
		if a.Kind != Delete {
			continue
		}
		want := map[int]bool{}
		for _, e := range evictsByNode[a.ProviderID] {
			want[e] = true
		}
		got := map[int]bool{}
		for _, dep := range a.DependsOn {
			got[dep] = true
		}
		for e := range want {
			if !got[e] {
				t.Errorf("Delete at %d for %s does not wait on its Evict at %d", i, a.ProviderID, e)
			}
		}
		for dep := range got {
			if actions[dep].Kind == Evict && actions[dep].ProviderID != a.ProviderID {
				t.Errorf("Delete at %d for %s waits on %s's Evict at %d",
					i, a.ProviderID, actions[dep].ProviderID, dep)
			}
		}
	}
}

// A batch of empty nodes evicts nothing and deletes each node behind its own
// taint. It also declares neither ScoreStillClears nor PodsStillFit, for the same
// reason: nothing was scored and there are no pods to place.
func TestEmptyBatchShape(t *testing.T) {
	cat := BuildCatalog()
	nodes := []NodeSnapshot{
		typedNode(t, cat, "i-e1", "m7i.large"),
		typedNode(t, cat, "i-e2", "m7i.xlarge"),
		typedNode(t, cat, "i-e3", "c7i.large"),
	}
	totals := Totals(nodes)

	props := EmptinessEvaluation{}.Evaluate(nodes, nil, cat, totals, poolBudgets(nodes, 10))
	if len(props) != 1 {
		t.Fatalf("want one batched proposal, got %d", len(props))
	}
	p := props[0]
	if len(p.Candidates) != 3 {
		t.Errorf("want all three nodes in one batch, got %d", len(p.Candidates))
	}
	want := 0.0
	for _, n := range nodes {
		want += n.Price
	}
	if math.Abs(p.Savings-want) > 1e-9 {
		t.Errorf("savings %.4f want %.4f", p.Savings, want)
	}
	for _, a := range p.Actions {
		if a.Kind == Evict || a.Kind == Launch {
			t.Errorf("an empty batch produced a %v action", a.Kind)
		}
	}
	for _, pre := range p.Preconditions {
		if pre == ScoreStillClears || pre == PodsStillFit {
			t.Errorf("an empty batch declares %s", pre)
		}
	}

	// The budget caps the batch, and the cap takes the highest-ratio nodes,
	// because the path sorts before it filters.
	capped := EmptinessEvaluation{}.Evaluate(nodes, nil, cat, totals, poolBudgets(nodes, 2))
	if len(capped) != 1 || len(capped[0].Candidates) != 2 {
		t.Fatalf("a budget of 2 should batch 2 nodes, got %v", capped)
	}
	best := sortBySavingsRatio(nodes)[:2]
	for i, c := range capped[0].Candidates {
		if c.ProviderID != best[i].ProviderID {
			t.Errorf("budget-capped batch position %d: got %s want %s",
				i, c.ProviderID, best[i].ProviderID)
		}
	}
	// Zero budget yields nothing rather than an empty proposal.
	if got := (EmptinessEvaluation{}).Evaluate(nodes, nil, cat, totals, poolBudgets(nodes, 0)); len(got) != 0 {
		t.Errorf("a zero budget produced %d proposals", len(got))
	}
}

// A batch commits or it does not. One node in it failing a precondition stops the
// deletion of all of them, which is the cost of upstream's one-Command-per-batch
// shape and is worth pinning rather than discovering.
func TestBatchIsAllOrNothing(t *testing.T) {
	cat := BuildCatalog()
	nodes := []NodeSnapshot{
		typedNode(t, cat, "i-e1", "m7i.large"),
		typedNode(t, cat, "i-e2", "m7i.xlarge"),
	}
	totals := Totals(nodes)
	props := EmptinessEvaluation{}.Evaluate(nodes, nil, cat, totals, poolBudgets(nodes, 10))
	if len(props) != 1 {
		t.Fatalf("want one proposal, got %d", len(props))
	}

	cluster := NewCluster(nodes, cat, 10)
	cluster.Totals, cluster.Threshold = totals, 0.5
	// Nominate the second node. The batch's first Delete has already fired by the
	// time the check runs again, so the stop leaves one node deleted and one not.
	cluster.Nominated["i-e2"] = true

	exec := NewMemoryExecutor(cluster)
	outcome, err := exec.Apply(context.Background(), props[0])
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if outcome != Stopped {
		t.Fatalf("want the batch to stop on the nomination, got %v", outcome)
	}
	if exec.StopCauses[NotNominated] != 1 {
		t.Errorf("stop causes %v, want one NotNominated", exec.StopCauses)
	}
	// Nothing fired, because the check runs before the first action and the
	// nominated node was in the batch from the start.
	if exec.ActionsApplied != 0 {
		t.Errorf("%d actions fired inside a batch rejected before its first action", exec.ActionsApplied)
	}
	if cluster.Nodes["i-e1"].Deleted {
		t.Error("i-e1 was deleted by a batch that never started")
	}
}

// Empty and underutilized draw on separate counters in stage 4, not only in stage
// 3. A multi-node batch spends the whole underutilized allowance, and with one
// shared counter the emptiness batch behind it stopped on a budget upstream would
// not have charged it.
func TestStageFourBudgetsAreSeparatePerReason(t *testing.T) {
	cat := BuildCatalog()
	nodes := []NodeSnapshot{
		typedNode(t, cat, "i-e1", "m7i.large"),
		typedNode(t, cat, "i-e2", "m7i.large"),
	}
	totals := Totals(nodes)
	cluster := NewCluster(nodes, cat, 2)
	cluster.Totals, cluster.Threshold = totals, 0.5
	// Spend the underutilized allowance, as a committed batch of two would.
	cluster.Budget = 0

	props := EmptinessEvaluation{}.Evaluate(nodes, nil, cat, totals, poolBudgets(nodes, 2))
	exec := NewMemoryExecutor(cluster)
	outcome, err := exec.Apply(context.Background(), props[0])
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if outcome != Proceed {
		t.Fatalf("the empty batch stopped with the underutilized budget spent: %v", exec.StopCauses)
	}
	if cluster.EmptyBudget != 0 {
		t.Errorf("empty budget left %d after two deletions from an allowance of 2", cluster.EmptyBudget)
	}
	if cluster.Budget != 0 {
		t.Errorf("the empty batch charged the underutilized budget, leaving %d", cluster.Budget)
	}
}
