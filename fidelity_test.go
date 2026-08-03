package sim

import (
	"context"
	"fmt"
	"math"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// A NaN price must stop the run rather than silently poison every score. One NaN
// makes the pool total NaN, then every score NaN, and NaN >= threshold is false,
// so the pass commits nothing and reports success. Upstream guards this in
// resolveNodePrice and pins it with an adversarial test.
func TestNonFinitePriceRejected(t *testing.T) {
	for _, tc := range []struct {
		name  string
		price float64
	}{
		{"NaN", math.NaN()},
		{"positive infinity", math.Inf(1)},
		{"zero", 0},
		{"negative", -1.0},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s price: Totals accepted it", tc.name)
				}
			}()
			Totals([]NodeSnapshot{{ProviderID: "i-1", Price: tc.price, DisruptionCost: 2}})
		}()
	}
}

// Emptiness is disruption cost carrying only the baseline, matching upstream's
// IsEmpty. A pod with a maximally negative deletion cost clamps to zero, so a
// node holding only that pod is empty even though its pod count is one.
//
// Rewritten when the three paths were split apart. The empty delete used to come
// out of ConsolidationGenerator.Generate, which is where upstream does not put it:
// consolidation.ShouldDisrupt routes empty nodes away and Emptiness handles them
// under its own reason. So the generator must now decline and the emptiness path
// must claim it.
func TestEmptinessMatchesUpstreamDefinition(t *testing.T) {
	free := pod("free", "-2147483647")
	nodes := []NodeSnapshot{{
		ProviderID:        "i-1",
		Price:             0.1008,
		ReschedulablePods: []*corev1.Pod{free},
		DisruptionCost:    RescheduleDisruptionCost([]*corev1.Pod{free}),
	}}
	if nodes[0].DisruptionCost != PerNodeBaseDisruptionCost {
		t.Fatalf("disruption cost: got %v want %v", nodes[0].DisruptionCost, PerNodeBaseDisruptionCost)
	}
	if !IsEffectivelyEmpty(nodes[0].DisruptionCost) {
		t.Fatal("node with one zero-cost pod should count as empty")
	}
	totals := Totals(nodes)

	// The consolidation generator declines, which is consolidation.ShouldDisrupt.
	if got := (ConsolidationGenerator{Threshold: 0.5}).Generate(nodes[0], nil, BuildCatalog(), totals); len(got) != 0 {
		t.Errorf("the consolidation generator produced %d proposals for an empty node; Emptiness owns it", len(got))
	}
	if (MultiNodeEvaluation{}).ShouldDisrupt(nodes[0], totals, 0.5) {
		t.Error("multi-node accepted an empty candidate")
	}
	if (SingleNodeEvaluation{}).ShouldDisrupt(nodes[0], totals, 0.5) {
		t.Error("single-node accepted an empty candidate")
	}
	if !(EmptinessEvaluation{}).ShouldDisrupt(nodes[0], totals, 0.5) {
		t.Fatal("the emptiness path rejected an empty candidate")
	}

	got := EmptinessEvaluation{}.Evaluate(nodes, nil, BuildCatalog(), totals, poolBudgets(nodes, 10))
	if len(got) != 1 {
		t.Fatalf("want one proposal, got %d", len(got))
	}
	for _, a := range got[0].Actions {
		if a.Kind == Evict {
			t.Error("empty-node proposal must not evict")
		}
	}
	if got[0].Reason != ReasonEmpty {
		t.Errorf("reason: got %s want Empty", got[0].Reason)
	}
	// Emptiness never scores. A number here would invite a threshold that upstream
	// does not apply, and a score of zero is what a Proposal carries when nothing
	// compared it against one.
	if got[0].Score != 0 {
		t.Errorf("the emptiness path scored the move at %v; upstream never scores it", got[0].Score)
	}
}

// A high enough threshold rejects every scored move and must not touch the
// emptiness path. This is the behavior the routing exists for: an empty node is
// deleted whatever it would have scored.
func TestEmptinessIgnoresTheThreshold(t *testing.T) {
	// Two loaded nodes make the pool cost large, so the empty node's savings
	// fraction is small and its score would fail any reasonable floor.
	loaded := []*corev1.Pod{pod("p1", ""), pod("p2", ""), pod("p3", "")}
	nodes := []NodeSnapshot{
		{ProviderID: "i-empty", Price: 0.0892, DisruptionCost: PerNodeBaseDisruptionCost},
		{ProviderID: "i-a", Price: 3.2256, ReschedulablePods: loaded, DisruptionCost: RescheduleDisruptionCost(loaded)},
		{ProviderID: "i-b", Price: 3.2256, ReschedulablePods: loaded, DisruptionCost: RescheduleDisruptionCost(loaded)},
	}
	totals := Totals(nodes)

	// The premise: scored, this node fails.
	if score := Score(nodes[0].Price, nodes[0].DisruptionCost, totals); score >= 0.5 {
		t.Fatalf("premise broken: the empty node scores %.4f, which passes", score)
	}
	got := EmptinessEvaluation{}.Evaluate(nodes[:1], nil, BuildCatalog(), totals, poolBudgets(nodes, 10))
	if len(got) != 1 {
		t.Fatalf("the emptiness path refused a move a score would have refused; got %d proposals", len(got))
	}
}

// withDefaults must supply churn. Without it every node is perfectly packed, no
// REPLACE is ever generated, and the run still reports success.
func TestDefaultsProduceConsolidationWork(t *testing.T) {
	cat := BuildCatalog()
	nodes, totals := GenCluster(GenClusterOpts{NodeCount: 200, Seed: 7, Catalog: cat})
	gen := ConsolidationGenerator{Threshold: 0.5}
	dests := EligibleDestinations(nodes, cat, totals, DefaultRunConfig(2, 20).Bands)
	replaces, deletes := 0, 0
	for _, n := range nodes {
		for _, p := range gen.Generate(n, dests, cat, totals) {
			if len(p.Replacements) > 0 {
				replaces++
			} else {
				deletes++
			}
		}
	}
	fmt.Printf("deletes=%d replaces=%d\n", deletes, replaces)
	// A loose cluster has room to scatter pods, so every candidate gets a delete
	// and the generator stops there. REPLACE coverage comes from the tight
	// fixture, where no delete is feasible. See TestTightClusterYieldsReplaceNotDelete.
	if deletes == 0 {
		t.Error("no DELETE proposals; the fixture has no consolidation work in it")
	}
	// One move per candidate. A node yielding two proposals means the generator
	// stopped preferring delete.
	byNode := map[string]int{}
	for _, n := range nodes {
		byNode[n.ProviderID] = len(gen.Generate(n, dests, cat, totals))
	}
	for id, count := range byNode {
		if count > 1 {
			t.Errorf("node %s produced %d proposals, want at most 1", id, count)
		}
	}
	_ = context.Background()
}

// A node holding a do-not-disrupt pod is not a candidate at any score. Upstream
// rejects it in NewCandidate, before pool totals are computed, so it never
// appears in a denominator either.
func TestDoNotDisruptNodeIsNotACandidate(t *testing.T) {
	blocked := pod("blocked", "")
	blocked.Annotations = map[string]string{"karpenter.sh/do-not-disrupt": "true"}
	free := pod("free", "")

	nodes := []NodeSnapshot{
		{ProviderID: "i-blocked", Price: 0.8064, ReschedulablePods: []*corev1.Pod{blocked},
			DisruptionCost: RescheduleDisruptionCost([]*corev1.Pod{blocked})},
		{ProviderID: "i-free", Price: 0.8064, ReschedulablePods: []*corev1.Pod{free},
			DisruptionCost: RescheduleDisruptionCost([]*corev1.Pod{free})},
	}
	totals := Totals(nodes)

	// Identical price and identical disruption cost, so identical delete ratio.
	// Only the annotation separates them.
	if DeleteRatio(nodes[0], totals) != DeleteRatio(nodes[1], totals) {
		t.Fatal("fixture broken: the two nodes should score the same")
	}
	bits := Identify(nodes, totals, 2)
	if bits[0] {
		t.Error("do-not-disrupt node was kept as a candidate")
	}
	if !bits[1] {
		t.Error("free node should be a candidate")
	}
	gen := ConsolidationGenerator{Threshold: 0.5}
	if got := gen.Generate(nodes[0], nodes[1:], BuildCatalog(), totals); got != nil {
		t.Errorf("generator produced %d proposals for an undisruptable node", len(got))
	}
}

// Empty and underutilized draw on separate budgets. Sharing one lets a cheap
// empty delete take a slot an expensive underutilized move needed.
func TestBudgetsAreSeparatePerReason(t *testing.T) {
	mk := func(id string, reason Reason, score float64) Proposal {
		return Proposal{
			ID:         id,
			Score:      score,
			Reason:     reason,
			Candidates: []NodeSnapshot{{ProviderID: id}},
		}
	}
	// Two empties outscore the underutilized move. With one shared budget of 2
	// the underutilized move would be starved.
	proposals := []Proposal{
		mk("e1", ReasonEmpty, 9.0),
		mk("e2", ReasonEmpty, 8.0),
		mk("u1", ReasonUnderutilized, 1.0),
	}
	committed, _ := RankingCompactor{}.SelectCounting(context.Background(), proposals, 2)
	byReason := map[Reason]int{}
	for _, p := range committed {
		byReason[p.Reason]++
	}
	if byReason[ReasonEmpty] != 2 {
		t.Errorf("empty committed: got %d want 2", byReason[ReasonEmpty])
	}
	if byReason[ReasonUnderutilized] != 1 {
		t.Errorf("underutilized committed: got %d want 1; empties consumed its budget",
			byReason[ReasonUnderutilized])
	}
}
