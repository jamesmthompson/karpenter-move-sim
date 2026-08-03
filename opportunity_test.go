package sim

import (
	"context"
	"fmt"
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// How much consolidation work a placement strategy leaves behind.
//
// The claim placement.go is built for is that where waste sits decides whether it
// is reclaimable. MostAllocated concentrates pods, so free capacity collects in
// whole nodes and a node emptied enough to delete is common. LeastAllocated
// spreads pods, so every node sits partly full and the same idle total is spread
// too thin to remove. Same fleet, same pods, different opportunity.
//
// These tests measure that rather than assert it. The comparison is a full Run on
// each layout, so what comes out is candidate count and realized savings and not a
// proxy.

// relayout re-places one fleet's pods under one strategy and returns the
// resulting snapshots, plus the pods that would not fit anywhere.
//
// The fleet is fixed: same instance types, same provider IDs, same count as the
// input. Only where each pod lands changes, which is the one variable the
// comparison is about. Holding the fleet fixed also holds pool cost fixed, so a
// difference in realized savings is a difference in what was reclaimable and not
// in what was bought.
//
// Pods are placed one at a time in a deterministic order, node by node in
// provider-ID order. Sorting them largest-first would be first-fit-decreasing,
// which is a third policy on top of the strategy under test, and it would flatter
// whichever strategy happens to agree with it.
func relayout(t *testing.T, in []NodeSnapshot, cat *Catalog, name PlacementStrategyName) ([]NodeSnapshot, []*corev1.Pod) {
	t.Helper()

	// An empty copy of the fleet. NewCluster takes snapshots, so the pods are
	// stripped first rather than deleted after.
	bare := make([]NodeSnapshot, 0, len(in))
	var pods []*corev1.Pod
	for _, n := range in {
		bare = append(bare, NodeSnapshot{
			ProviderID:   n.ProviderID,
			NodePool:     n.NodePool,
			InstanceType: n.InstanceType,
			Price:        n.Price,
		})
		pods = append(pods, n.ReschedulablePods...)
	}

	c := NewCluster(bare, cat, len(bare))
	s, err := name.Strategy()
	if err != nil {
		t.Fatalf("strategy %q: %v", name, err)
	}
	c.Placement = s

	var stranded []*corev1.Pod
	for _, pod := range pods {
		dest, ok := c.placement(pod, "", "")
		if !ok {
			stranded = append(stranded, pod)
			continue
		}
		dest.Pods = append(dest.Pods, pod)
	}

	// Back to snapshots. DisruptionCost has to be recomputed, since the pods on
	// each node are not the pods the generator put there.
	pool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	if len(in) > 0 && in[0].NodePool != nil {
		pool = in[0].NodePool
	}
	ids := make([]string, 0, len(c.Nodes))
	for id := range c.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	out := make([]NodeSnapshot, 0, len(ids))
	for _, id := range ids {
		n := c.Nodes[id]
		out = append(out, NodeSnapshot{
			ProviderID:        id,
			NodePool:          pool,
			InstanceType:      n.InstanceType,
			Price:             n.Price,
			ReschedulablePods: n.Pods,
			DisruptionCost:    disruptionCost(n.Pods),
		})
	}
	return out, stranded
}

// emptyNodes counts nodes carrying nothing, which is the shape the entropy claim
// is about. Concentrated waste means whole idle nodes; spread waste means none.
func emptyNodes(nodes []NodeSnapshot) int {
	n := 0
	for _, s := range nodes {
		if len(s.ReschedulablePods) == 0 {
			n++
		}
	}
	return n
}

// TestPlacementDecidesConsolidationOpportunity is the measurement. One generated
// fleet, laid out three ways, then a full pass over each.
//
// It prints the comparison and asserts only the direction, because the magnitude
// is a property of the fixture and the direction is the claim. If packing ever
// stops leaving more opportunity than spreading, the assertion is what says so.
func TestPlacementDecidesConsolidationOpportunity(t *testing.T) {
	const (
		seed  = 7
		count = 400
	)
	cat := BuildCatalog()
	opts := DefaultGenClusterOpts(count, seed)
	opts.Catalog = cat
	base, _ := GenCluster(opts)

	cfg := DefaultRunConfig(2.0, 20)

	type row struct {
		name      PlacementStrategyName
		empty     int
		stranded  int
		res       Result
		poolStart float64
	}
	var rows []row

	fmt.Printf("consolidation opportunity by placement, seed=%d nodes=%d k=%.1f budget=%d\n",
		seed, count, cfg.K, cfg.Budget)
	for _, name := range PlacementStrategyNames() {
		laid, stranded := relayout(t, base, cat, name)
		totals := Totals(laid)

		runCfg := cfg
		// The layout and the run use the same strategy. Measuring a packed layout
		// under a spreading executor would mix the two effects.
		runCfg.Placement = name
		res := Run(context.Background(), laid, totals, cat, runCfg)

		rows = append(rows, row{
			name: name, empty: emptyNodes(laid), stranded: len(stranded),
			res: res, poolStart: totals.Cost,
		})
		fmt.Printf("  %-16s empty=%3d/%d stranded=%2d candidates=%3d proposals=%3d "+
			"committed=%3d realized=$%.4f/hr of $%.4f (%.2f%%)\n",
			name, emptyNodes(laid), len(laid), len(stranded),
			res.CandidatesOut, res.ProposalsGenerated, res.Committed,
			res.SavingsRealized, totals.Cost, 100*res.SavingsRealized/totals.Cost)
	}

	byName := map[PlacementStrategyName]row{}
	for _, r := range rows {
		byName[r.name] = r
	}
	pack, spread := byName[PlacementMostAllocated], byName[PlacementLeastAllocated]

	// The fleet is the same in every row, so a pool cost that moved means the
	// re-layout changed what was bought and the comparison is not about placement.
	for _, r := range rows {
		if r.poolStart != rows[0].poolStart {
			t.Fatalf("%s started from $%.4f and %s from $%.4f; the fleet must be identical",
				r.name, r.poolStart, rows[0].name, rows[0].poolStart)
		}
	}

	if pack.empty <= spread.empty {
		t.Errorf("packing left %d empty nodes and spreading left %d; packing is supposed to "+
			"concentrate waste into whole nodes", pack.empty, spread.empty)
	}
	if pack.res.SavingsRealized <= spread.res.SavingsRealized {
		t.Errorf("packing realized $%.4f/hr and spreading $%.4f/hr; packing did NOT leave more "+
			"consolidation opportunity on this fixture",
			pack.res.SavingsRealized, spread.res.SavingsRealized)
	}
}
