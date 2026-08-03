package sim

import (
	"fmt"
	"math"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// pod builds a pod with an optional pod-deletion-cost annotation.
func pod(name string, deletionCost string) *corev1.Pod {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
	if deletionCost != "" {
		p.Annotations = map[string]string{corev1.PodDeletionCost: deletionCost}
	}
	return p
}

// TestDisruptionCostDerivation pins how disruption cost is currently derived:
// the two terms and the clamp from upstream's pkg/utils/disruption.EvictionCost.
// The RFC allows this derivation to be replaced, so these assertions cover the
// derivation and not the concept.
func TestDisruptionCostDerivation(t *testing.T) {
	for _, tc := range []struct {
		name string
		pod  *corev1.Pod
		want float64
	}{
		{"bare pod is 1.0", pod("p", ""), 1.0},
		{"max deletion cost clamps to 10", pod("p", "2147483647"), 10.0},
		{"min deletion cost clamps to -10", pod("p", "-2147483647"), -10.0},
		{"malformed annotation is ignored", pod("p", "not-a-number"), 1.0},
		{"deletion cost 2^27 adds exactly 1", pod("p", "134217728"), 2.0},
	} {
		if got := EvictionCost(tc.pod); math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
	// Scoring clamps negatives to zero, because a negative disruption cost would
	// invert the ratio and make a move look better the more it hurts. A node
	// holding one maximally-negative pod therefore costs only the baseline.
	if got := RescheduleDisruptionCost([]*corev1.Pod{pod("p", "-2147483647")}); got != PerNodeBaseDisruptionCost {
		t.Errorf("node with one negative-cost pod: got %v want %v", got, PerNodeBaseDisruptionCost)
	}
	// Eviction ordering still sees the signed value.
	if got := EvictionCost(pod("p", "-2147483647")); got != -10 {
		t.Errorf("eviction ordering: got %v want -10", got)
	}
}

// TestRFCHeterogeneousDisruption reproduces the RFC's central example, the one
// that shows the score doing something savings-versus-pod-count cannot. Two
// m7i.xlarge nodes, both saving $4.84/day, both running 4 pods. Node A holds
// stateless proxies. Node B holds three model servers with max deletion cost.
// The RFC says A scores 2.24 and is approved, B scores 0.29 and is rejected.
//
// Before disruption cost was derived from the annotation, every pod cost 1.0,
// both nodes scored identically, and this example was unreproducible.
func TestRFCHeterogeneousDisruption(t *testing.T) {
	const maxCost = "2147483647"

	nodeA := []*corev1.Pod{pod("a1", ""), pod("a2", ""), pod("a3", ""), pod("a4", "")}
	nodeB := []*corev1.Pod{pod("b1", ""), pod("b2", maxCost), pod("b3", maxCost), pod("b4", maxCost)}

	// Neither node holds a gang, so the step in MoveDisruptionCost does not apply
	// here and the RFC's arithmetic is reproduced by the sum. Stated because the
	// gang path is the one thing that could move these two numbers without
	// anything in the example changing.
	for _, pods := range [][]*corev1.Pod{nodeA, nodeB} {
		if got := GangsBroken(pods); len(got) != 0 {
			t.Fatalf("the RFC example acquired gangs %v; its numbers assume none", got)
		}
		if NodeDisruptionCost(pods) != RescheduleDisruptionCost(pods) {
			t.Fatalf("the gang-aware cost diverged from the sum on a gangless node")
		}
	}

	// The RFC's pool: 8 m7i.xlarge at $4.84/day and 2 m7i.2xlarge at $9.68,
	// totalling $58.08/day with 80 pods. Its stated total disruption cost of 107
	// counts 76 default-cost pods plus node B's 31, and omits the per-node
	// baseline its own formula adds. We reproduce the RFC's number to compare
	// against its arithmetic, then note the divergence.
	const poolCost = 58.08
	const rfcPoolDisruption = 107.0
	totals := PoolTotals{Cost: poolCost, DisruptionCost: rfcPoolDisruption}

	dispA := 0.0
	for _, p := range nodeA {
		dispA += EvictionCost(p)
	}
	dispB := 0.0
	for _, p := range nodeB {
		dispB += EvictionCost(p)
	}
	if dispA != 4 {
		t.Fatalf("node A pod disruption: got %v want 4", dispA)
	}
	if math.Abs(dispB-31) > 0.01 {
		t.Fatalf("node B pod disruption: got %v want ~31", dispB)
	}

	scoreA := Score(4.84, dispA, totals)
	scoreB := Score(4.84, dispB, totals)
	fmt.Printf("node A score=%.4f (RFC 2.24)  node B score=%.4f (RFC 0.29)\n", scoreA, scoreB)

	if math.Abs(scoreA-2.23) > 0.02 {
		t.Errorf("node A: got %.4f want ~2.24", scoreA)
	}
	if math.Abs(scoreB-0.288) > 0.02 {
		t.Errorf("node B: got %.4f want ~0.29", scoreB)
	}

	const threshold = 0.5
	if scoreA < threshold {
		t.Errorf("node A should be approved at k=2, scored %.4f", scoreA)
	}
	if scoreB >= threshold {
		t.Errorf("node B should be rejected at k=2, scored %.4f", scoreB)
	}

	// The point of the example: identical savings and pod counts, opposite
	// verdicts. Savings-versus-pod-count cannot produce this.
	if scoreA <= scoreB {
		t.Errorf("cheap-to-disrupt node must outscore expensive one: %.4f vs %.4f", scoreA, scoreB)
	}
}

// TestRescheduleDisruptionCostIncludesBaseline pins the per-node term the RFC's worked
// examples omit from their pool totals but its formula includes.
func TestRescheduleDisruptionCostIncludesBaseline(t *testing.T) {
	if got := RescheduleDisruptionCost(nil); got != PerNodeBaseDisruptionCost {
		t.Errorf("empty node: got %v want %v", got, PerNodeBaseDisruptionCost)
	}
	tenPods := make([]*corev1.Pod, 10)
	for i := range tenPods {
		tenPods[i] = pod(fmt.Sprintf("p%d", i), "")
	}
	// The RFC: "A 10-pod node has disruption cost 11; the per-node cost is 9% of
	// the total."
	if got := RescheduleDisruptionCost(tenPods); got != 11 {
		t.Errorf("10-pod node: got %v want 11", got)
	}
	if share := PerNodeBaseDisruptionCost / 11; math.Abs(share-0.0909) > 0.001 {
		t.Errorf("per-node share: got %.4f want ~0.09", share)
	}
}
