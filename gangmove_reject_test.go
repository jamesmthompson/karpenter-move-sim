package sim

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// sizedGangPod is a gang member that requests capacity, so the move has
// something to place.
func sizedGangPod(name, gang string, cpuMilli, memMiB int64) *corev1.Pod {
	p := sizedPod(name, cpuMilli, memMiB, "")
	if p.Labels == nil {
		p.Labels = map[string]string{}
	}
	p.Labels[GangLabel] = gang
	return p
}

// gangOn builds one node per pod list, each of the given type, and returns them
// with the gang candidate Candidates() forms over them.
func gangOn(t *testing.T, cat *Catalog, itName string, podsPerNode ...[]*corev1.Pod) ([]NodeSnapshot, Candidate) {
	t.Helper()
	var nodes []NodeSnapshot
	for i, pods := range podsPerNode {
		n := typedNode(t, cat, fmt.Sprintf("i-g%d", i), itName, pods...)
		n.DisruptionCost = NodeDisruptionCost(pods)
		nodes = append(nodes, n)
	}
	for _, c := range Candidates(nodes) {
		if c.IsGang() {
			return nodes, c
		}
	}
	t.Fatal("no gang candidate formed")
	return nil, Candidate{}
}

// Each way a gang candidate yields no move. A rejected gang must also leave the
// budget untouched, because Evaluate charges the budget only for a move it emits.
func TestGangMoveRejections(t *testing.T) {
	cat := BuildCatalog()

	// A gang with a destination that holds all of it: the delete path.
	deletable := func(t *testing.T) ([]NodeSnapshot, Candidate, []NodeSnapshot) {
		nodes, c := gangOn(t, cat, "c7i.large",
			[]*corev1.Pod{sizedGangPod("m0", "g", 500, 512)},
			[]*corev1.Pod{sizedGangPod("m1", "g", 500, 512)})
		dest := typedNode(t, cat, "i-dest", "c7i.4xlarge")
		dest.DisruptionCost = NodeDisruptionCost(nil)
		return nodes, c, []NodeSnapshot{dest}
	}

	// A gang that must be replaced: no destination, and a cheaper node holds it.
	replaceable := func(t *testing.T) ([]NodeSnapshot, Candidate, []NodeSnapshot) {
		nodes, c := gangOn(t, cat, "r7i.2xlarge",
			[]*corev1.Pod{sizedGangPod("m0", "g", 500, 512), sizedPod("bulk0", 3000, 8192, "")},
			[]*corev1.Pod{sizedGangPod("m1", "g", 500, 512), sizedPod("bulk1", 3000, 8192, "")})
		return nodes, c, nil
	}

	cases := []struct {
		name      string
		build     func(t *testing.T) ([]NodeSnapshot, Candidate, []NodeSnapshot)
		threshold func(c Candidate, totals PoolTotals, replaceScore float64) float64
	}{
		{
			// A do-not-disrupt member blocks the whole gang, even with room for it.
			name: "a do-not-disrupt pod on one gang node",
			build: func(t *testing.T) ([]NodeSnapshot, Candidate, []NodeSnapshot) {
				blocked := sizedGangPod("m1", "g", 500, 512)
				blocked.Annotations = map[string]string{v1.DoNotDisruptAnnotationKey: "true"}
				nodes, c := gangOn(t, cat, "c7i.large",
					[]*corev1.Pod{sizedGangPod("m0", "g", 500, 512)},
					[]*corev1.Pod{blocked})
				dest := typedNode(t, cat, "i-dest", "c7i.4xlarge")
				return nodes, c, []NodeSnapshot{dest}
			},
			threshold: func(Candidate, PoolTotals, float64) float64 { return 0 },
		},
		{
			// The delete ratio bounds every move, so a gang below it on delete
			// is rejected before any packing.
			name:  "a delete ratio below the threshold",
			build: deletable,
			threshold: func(c Candidate, totals PoolTotals, _ float64) float64 {
				return c.DeleteRatio(totals) * 1.01
			},
		},
		{
			// Two full c7i.large nodes need a c7i.xlarge, which costs exactly the
			// two of them. A replacement has to be strictly cheaper, and there is
			// no destination, so the gang has nowhere to go.
			name: "no destination and no cheaper replacement",
			build: func(t *testing.T) ([]NodeSnapshot, Candidate, []NodeSnapshot) {
				nodes, c := gangOn(t, cat, "c7i.large",
					[]*corev1.Pod{sizedGangPod("m0", "g", 1500, 1024)},
					[]*corev1.Pod{sizedGangPod("m1", "g", 1500, 1024)})
				return nodes, c, nil
			},
			threshold: func(Candidate, PoolTotals, float64) float64 { return 0 },
		},
		{
			// The gang clears on its delete ratio but the replace recovers less
			// than the delete would, and the replace score falls under the floor.
			name:  "a replace score below the threshold",
			build: replaceable,
			threshold: func(c Candidate, totals PoolTotals, replaceScore float64) float64 {
				return (replaceScore + c.DeleteRatio(totals)) / 2
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nodes, c, dests := tc.build(t)
			totals := Totals(append(append([]NodeSnapshot{}, nodes...), dests...))

			// The replace score at a zero floor, which the replace case needs to
			// place its threshold between the two scores.
			replaceScore := 0.0
			if p, ok := (GangEvaluation{}).gangMove(c, dests, cat, totals); ok {
				replaceScore = p.Score
			}
			threshold := tc.threshold(c, totals, replaceScore)
			if tc.name == "a replace score below the threshold" &&
				!(replaceScore > 0 && replaceScore < threshold && threshold < c.DeleteRatio(totals)) {
				t.Fatalf("premise broken: replace score %v, threshold %v, delete ratio %v",
					replaceScore, threshold, c.DeleteRatio(totals))
			}

			budgets := map[string]int{c.Pool(): 10}
			// A plain node candidate rides along to show the gang path skips it.
			plainNode := typedNode(t, cat, "i-plain", "c7i.large", sizedPod("p", 100, 128, ""))
			plainNode.DisruptionCost = NodeDisruptionCost(plainNode.ReschedulablePods)
			plain := Candidates([]NodeSnapshot{plainNode})[0]
			got := GangEvaluation{Threshold: threshold}.Evaluate(
				[]Candidate{plain, c}, dests, cat, totals, budgets)
			if len(got) != 0 {
				t.Fatalf("got %d proposals, want none: %s", len(got), got[0].ID)
			}
			if budgets[c.Pool()] != 10 {
				t.Errorf("a rejected gang charged the budget down to %d", budgets[c.Pool()])
			}
		})
	}
}

// A multi-node batch whose pods fit the destinations but whose delete score is
// under the floor yields nothing, and the empty batch yields nothing.
func TestMultiNodeBatchRejections(t *testing.T) {
	cat := BuildCatalog()
	a := typedNode(t, cat, "i-a", "c7i.large", sizedPod("a1", 500, 512, ""))
	a.DisruptionCost = NodeDisruptionCost(a.ReschedulablePods)
	b := typedNode(t, cat, "i-b", "c7i.large", sizedPod("b1", 500, 512, ""))
	b.DisruptionCost = NodeDisruptionCost(b.ReschedulablePods)
	dest := typedNode(t, cat, "i-dest", "c7i.4xlarge")
	dest.DisruptionCost = NodeDisruptionCost(nil)
	totals := Totals([]NodeSnapshot{a, b, dest})
	batch, dests := []NodeSnapshot{a, b}, []NodeSnapshot{dest}

	p, ok := computeBatchConsolidation(batch, dests, cat, totals, 0)
	if !ok || len(p.Replacements) != 0 {
		t.Fatalf("premise broken: want a delete at a zero floor, got ok=%v %+v", ok, p.Replacements)
	}
	if _, ok := computeBatchConsolidation(batch, dests, cat, totals, p.Score*1.01); ok {
		t.Errorf("a batch scoring %v cleared a floor of %v", p.Score, p.Score*1.01)
	}
	if _, ok := computeBatchConsolidation(nil, dests, cat, totals, 0); ok {
		t.Error("an empty batch produced a proposal")
	}
}
