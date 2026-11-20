package sim

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// TestStage4StopsSecondProposalOnSharedDestination shows how stage 4 handles two
// proposals that need the same free capacity. Two DELETE proposals each plan to
// move one pod onto the same destination, which has room for only one of the
// pods. The proposals delete different nodes, so RankingCompactor commits both.
// The first proposal applies, and its pod uses the free capacity. Before the
// second proposal's first action, the executor finds its PodsStillFit
// precondition false and stops it.
func TestStage4StopsSecondProposalOnSharedDestination(t *testing.T) {
	cat := BuildCatalog()
	destType := instanceType(t, cat, "m7i.large")
	effCPU := cat.Effective(destType)[corev1.ResourceCPU]

	// One pod per source, each taking 60% of the destination's effective CPU: one
	// fits, two do not.
	podMilli := effCPU.MilliValue() * 6 / 10
	// IDs sort destination first, so first fit places a pod there before it would
	// consider the other source, which is still standing when the first applies.
	dest := node(t, cat, "i-a-dest", "m7i.large")
	srcB := node(t, cat, "i-b-src", "m7i.large", testPod("pb", podMilli, 512))
	srcC := node(t, cat, "i-c-src", "m7i.large", testPod("pc", podMilli, 512))
	nodes := []NodeSnapshot{dest, srcB, srcC}
	totals := Totals(nodes)

	// Premise: the destination holds either pod, not both.
	one := Requests(srcB.ReschedulablePods)
	both := Requests(append(append([]*corev1.Pod{}, srcB.ReschedulablePods...), srcC.ReschedulablePods...))
	if !aggregateRoom([]NodeSnapshot{dest}, cat, one) || aggregateRoom([]NodeSnapshot{dest}, cat, both) {
		t.Fatal("premise broken: the destination must hold exactly one of the two pods")
	}

	// Stage 2, generated as Run does: each source against the destination set.
	gen := ConsolidationGenerator{Threshold: 0.5}
	var proposals []Proposal
	for _, src := range []NodeSnapshot{srcB, srcC} {
		ps := gen.Generate(src, []NodeSnapshot{dest}, cat, totals)
		if len(ps) != 1 || len(ps[0].Replacements) != 0 {
			t.Fatalf("%s: want one DELETE onto the destination, got %d proposals", src.ProviderID, len(ps))
		}
		proposals = append(proposals, ps[0])
	}

	// Stage 3. Disjoint candidates, so no conflict, and both commit.
	committed, conflicts := RankingCompactor{}.SelectCounting(context.Background(), proposals, 10)
	if len(committed) != 2 || conflicts != 0 {
		t.Fatalf("compactor committed %d with %d conflicts, want both committed with none",
			len(committed), conflicts)
	}

	// Stage 4. The first proposal takes the hole; the second finds it gone.
	cluster := NewCluster(nodes, cat, 10)
	cluster.Totals, cluster.Threshold = totals, gen.Threshold
	exec := NewMemoryExecutor(cluster)
	first, err := exec.Apply(context.Background(), committed[0])
	if err != nil || first != Proceed {
		t.Fatalf("first proposal %s: %v, %v; want Proceed", committed[0].ID, first, err)
	}
	second, err := exec.Apply(context.Background(), committed[1])
	if err != nil {
		t.Fatalf("second proposal %s: %v", committed[1].ID, err)
	}
	if second != Stopped {
		t.Fatalf("second proposal %s: %v; want Stopped on PodsStillFit", committed[1].ID, second)
	}
	if exec.StopCauses[PodsStillFit] != 1 || len(exec.StopCauses) != 1 {
		t.Errorf("stop causes %v; want exactly one PodsStillFit", exec.StopCauses)
	}
	// Stopped before any action, so the second source still holds its pod.
	if n := cluster.Nodes[committed[1].Candidates[0].ProviderID]; n.Deleted || len(n.Pods) != 1 {
		t.Errorf("second source was touched: deleted=%v pods=%d", n.Deleted, len(n.Pods))
	}
}
