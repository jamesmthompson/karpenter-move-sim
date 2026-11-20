package sim

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	updisruption "sigs.k8s.io/karpenter/pkg/controllers/disruption"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	disruptionutils "sigs.k8s.io/karpenter/pkg/utils/disruption"
)

// This file runs every scenario in upstream's
// pkg/controllers/disruption/balanced_scoring_test.go (26, counting each
// DescribeTable entry) and balanced_adversarial_test.go (5) through the
// simulator's scoring and through upstream's own ScoreMove / EvaluateBalancedMove,
// imported directly, and requires agreement to 1e-9 on the score and exactly on
// approve/reject. Where a scenario states an expected value, that value is
// copied verbatim and checked too, so the table cannot drift from the file it
// cites. Line numbers are against sigs.k8s.io/karpenter@v1.15.1.
//
// Three kinds of scenario are not pure score comparisons, and each says which
// it is:
//
//   - Intended difference. A NaN, zero or missing source price. Upstream's
//     ResolveOfferingPrice maps it to 0 and scores on; the simulator's Totals
//     panics (TestNonFinitePriceRejected). The case asserts the panic and checks
//     upstream's numbers for the well-priced remainder.
//   - Out of model. The simulator has no ConsolidationPolicy: every pool is
//     scored as Balanced. Scenarios about non-Balanced pools check the Balanced
//     part and state what is skipped.
//   - Unexported upstream. ShouldDisrupt and sortCandidates live on an
//     unexported type, so the simulator is checked against upstream's expected
//     value copied verbatim.

const scoreTol = 1e-9

// upCtx is what upstream's makeCandidate passes ComputeRescheduleDisruptionCost.
// It carries the same default gate evictionCostCtx does.
var upCtx = evictionCostCtx

// upTotals sums a pool the way upstream's computeNodePoolTotals does when every
// node is a candidate and there is no ClusterCost: Price and
// RescheduleDisruptionCost over the pool. computeNodePoolTotals is unexported.
func upTotals(cands []*updisruption.Candidate) updisruption.NodePoolTotals {
	var t updisruption.NodePoolTotals
	for _, c := range cands {
		t.TotalCost += c.Price
		t.TotalDisruptionCost += c.RescheduleDisruptionCost
	}
	return t
}

// upCandidate is upstream's makeCandidate (balanced_scoring_test.go:68) built
// from exported fields only.
func upCandidate(name string, np *v1.NodePool, it *cloudprovider.InstanceType, pods []*corev1.Pod) *updisruption.Candidate {
	labels := map[string]string{
		v1.NodePoolLabelKey:      np.Name,
		corev1.LabelTopologyZone: "test-zone-1",
		v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
	}
	sn := &state.StateNode{Node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}}
	return &updisruption.Candidate{
		StateNode:                sn,
		NodePool:                 np,
		Price:                    disruptionutils.ResolveOfferingPrice(sn.Labels(), it),
		RescheduleDisruptionCost: disruptionutils.ComputeRescheduleDisruptionCost(upCtx, pods),
	}
}

// simNode is the simulator's snapshot of the same node: priced at launch the
// way the fixture generators price one, costed the way GenCluster costs one.
func simNode(t *testing.T, name string, np *v1.NodePool, it *cloudprovider.InstanceType, pods []*corev1.Pod) NodeSnapshot {
	t.Helper()
	price, ok := cheapestOffering(it)
	if !ok {
		t.Fatalf("%s: no launchable offering", name)
	}
	return NodeSnapshot{
		ProviderID:        name,
		NodePool:          np,
		InstanceType:      it,
		Price:             price,
		ReschedulablePods: pods,
		DisruptionCost:    disruptionCost(pods),
	}
}

// Upstream's fixture helpers, balanced_scoring_test.go:46-123.
func upInstanceType(name string, price float64) *cloudprovider.InstanceType {
	return &cloudprovider.InstanceType{
		Name: name,
		Offerings: cloudprovider.Offerings{{
			Requirements: scheduling.NewRequirements(
				scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "test-zone-1"),
				scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
			),
			Price:     price,
			Available: true,
		}},
	}
}

func upNodePool(name string, policy v1.ConsolidationPolicy) *v1.NodePool {
	np := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name}}
	np.Spec.Disruption.ConsolidationPolicy = policy
	return np
}

func balancedPool(name string) *v1.NodePool { return upNodePool(name, v1.ConsolidationPolicyBalanced) }

func pods(prefix string, n int, deletionCost string) []*corev1.Pod {
	out := make([]*corev1.Pod, n)
	for i := range out {
		out[i] = pod(fmt.Sprintf("%s-%d", prefix, i), deletionCost)
	}
	return out
}

// pool builds n identical nodes on both sides.
func pool(t *testing.T, prefix string, np *v1.NodePool, it *cloudprovider.InstanceType, n int, podsPer int) ([]NodeSnapshot, []*updisruption.Candidate) {
	var sim []NodeSnapshot
	var up []*updisruption.Candidate
	for i := 0; i < n; i++ {
		ps := pods(fmt.Sprintf("%s-%d", prefix, i), podsPer, "")
		name := fmt.Sprintf("%s-%d", prefix, i)
		sim = append(sim, simNode(t, name, np, it, ps))
		up = append(up, upCandidate(name, np, it, ps))
	}
	return sim, up
}

// agree is the core assertion: the simulator's Score at threshold 1/k against
// upstream's ScoreResult, to 1e-9 on the score and exactly on the verdict.
func agree(t *testing.T, label string, simScore float64, k int32, up updisruption.ScoreResult) {
	t.Helper()
	upScore := up.Score()
	switch {
	case math.IsInf(upScore, 1) || math.IsInf(simScore, 1):
		if !(math.IsInf(upScore, 1) && math.IsInf(simScore, 1)) {
			t.Errorf("%s: score sim=%v upstream=%v", label, simScore, upScore)
		}
	case math.IsNaN(upScore) || math.IsNaN(simScore) || math.Abs(simScore-upScore) > scoreTol:
		t.Errorf("%s: score sim=%.12f upstream=%.12f", label, simScore, upScore)
	}
	simApproved := simScore >= 1.0/float64(k)
	if simApproved != up.Approved() {
		t.Errorf("%s: approved sim=%v upstream=%v (score %.6f, threshold %.6f)",
			label, simApproved, up.Approved(), simScore, up.Threshold())
	}
}

// wantScore checks upstream's own stated expectation, copied verbatim with its
// stated tolerance, so a row cannot quietly test something else.
func wantScore(t *testing.T, label string, got, want, tol float64) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Errorf("%s: upstream states %.4f±%g, upstream computes %.12f", label, want, tol, got)
	}
}

func approxEq(a, b float64) bool { return math.Abs(a-b) <= scoreTol }

func mustPanic(t *testing.T, label string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s: want the simulator's price guard to panic", label)
		}
	}()
	fn()
}

// totalsAgree compares the simulator's Totals to upstream's pool sums.
func totalsAgree(t *testing.T, label string, sim PoolTotals, up updisruption.NodePoolTotals) {
	t.Helper()
	if !approxEq(sim.Cost, up.TotalCost) || !approxEq(sim.DisruptionCost, up.TotalDisruptionCost) {
		t.Errorf("%s: totals sim={%v %v} upstream={%v %v}", label,
			sim.Cost, sim.DisruptionCost, up.TotalCost, up.TotalDisruptionCost)
	}
}

type upstreamScenario struct {
	ref  string // upstream file:line of the It or Entry
	name string
	run  func(t *testing.T)
}

func upstreamScenarios() []upstreamScenario {
	const bs = "balanced_scoring_test.go"
	const ba = "balanced_adversarial_test.go"
	const k = v1.BalancedK
	ctx := context.Background()

	// scoreMoveTable is the DescribeTable at balanced_scoring_test.go:457-474,
	// entries verbatim: savings, disruptionCost, wantScore, wantApproved, wantInf.
	tableTotals := updisruption.NodePoolTotals{TotalCost: 100.0, TotalDisruptionCost: 100.0}
	tableEntry := func(line int, name string, savings, disruption, want float64, wantApproved, wantInf bool) upstreamScenario {
		return upstreamScenario{fmt.Sprintf("%s:%d", bs, line), "ScoreMove/" + name, func(t *testing.T) {
			up := updisruption.ScoreMove(savings, disruption, tableTotals, 2)
			if wantInf {
				if !math.IsInf(up.Score(), 1) {
					t.Errorf("upstream states +Inf, computes %v", up.Score())
				}
			} else {
				wantScore(t, name, up.Score(), want, 0.01)
			}
			if up.Approved() != wantApproved {
				t.Errorf("upstream states approved=%v", wantApproved)
			}
			agree(t, name, Score(savings, disruption, PoolTotals{Cost: 100, DisruptionCost: 100}), 2, up)
		}}
	}

	return []upstreamScenario{
		{bs + ":189", "computeNodePoolTotals/all candidates", func(t *testing.T) {
			sim, up := pool(t, "n", balancedPool("pool-a"), upInstanceType("m7i.xlarge", 4.84), 5, 1)
			ut := upTotals(up)
			wantScore(t, "TotalCost", ut.TotalCost, 5*4.84, 0.01)
			wantScore(t, "TotalDisruptionCost", ut.TotalDisruptionCost, 10.0, 0.01)
			st := Totals(sim)
			totalsAgree(t, "totals", st, ut)
			agree(t, "delete one", Score(sim[0].Price, sim[0].DisruptionCost, st), k,
				updisruption.ScoreMove(up[0].Price, up[0].RescheduleDisruptionCost, ut, k))
		}},
		{bs + ":212", "computeNodePoolTotals/zero-priced candidate (intended difference)", func(t *testing.T) {
			np := balancedPool("pool-b")
			it := upInstanceType("m7i.xlarge", 4.84)
			p := []*corev1.Pod{pod("pod", "")}
			up := []*updisruption.Candidate{
				upCandidate("node-good-1", np, it, p), upCandidate("node-nil", np, nil, p), upCandidate("node-good-2", np, it, p),
			}
			ut := upTotals(up)
			wantScore(t, "TotalCost", ut.TotalCost, 2*4.84, 0.01)
			wantScore(t, "TotalDisruptionCost", ut.TotalDisruptionCost, 6.0, 0.01)
			good := []NodeSnapshot{simNode(t, "node-good-1", np, it, p), simNode(t, "node-good-2", np, it, p)}
			mustPanic(t, "nil instance type", func() {
				Totals(append(good, NodeSnapshot{ProviderID: "node-nil", NodePool: np, Price: 0, DisruptionCost: disruptionCost(p)}))
			})
			// Without the unpriceable node the cost sum agrees; upstream's disruption
			// sum still carries the zero-priced node's 2.0.
			st := Totals(good)
			if !approxEq(st.Cost, ut.TotalCost) || !approxEq(st.DisruptionCost+up[1].RescheduleDisruptionCost, ut.TotalDisruptionCost) {
				t.Errorf("well-priced remainder: sim=%+v upstream=%+v", st, ut)
			}
		}},
		{bs + ":236", "EvaluateBalancedMove/zero candidates rejects", func(t *testing.T) {
			totals := map[string]updisruption.NodePoolTotals{"pool-a": {TotalCost: 100.0, TotalDisruptionCost: 50.0}}
			approved, perPool := updisruption.EvaluateBalancedMove(ctx, updisruption.Command{Candidates: []*updisruption.Candidate{}}, totals)
			if approved || perPool != nil {
				t.Errorf("upstream states rejected with nil perPool")
			}
			// An empty move saves and disrupts nothing. Upstream's ScoreMove scores it 0.
			agree(t, "empty move", Score(0, 0, PoolTotals{Cost: 100, DisruptionCost: 50}), k,
				updisruption.ScoreMove(0, 0, totals["pool-a"], k))
		}},
		{bs + ":247", "EvaluateBalancedMove/non-Balanced pool skipped (out of model)", func(t *testing.T) {
			// The simulator scores every pool as Balanced. What both sides share is the
			// score the move would get under Balanced: 2 of 5 nodes, 2/5 over 4/10.
			np := upNodePool("pool-nobalanced", v1.ConsolidationPolicyWhenEmptyOrUnderutilized)
			sim, up := pool(t, "n", np, upInstanceType("m7i.xlarge", 4.84), 5, 1)
			ut := upTotals(up)
			approved, perPool := updisruption.EvaluateBalancedMove(ctx, updisruption.Command{Candidates: up[:2]}, map[string]updisruption.NodePoolTotals{np.Name: ut})
			if _, ok := perPool[np.Name]; ok || !approved {
				t.Errorf("upstream states non-Balanced pool absent from perPool and approved")
			}
			st := Totals(sim)
			agree(t, "as Balanced", Score(sim[0].Price+sim[1].Price, sim[0].DisruptionCost+sim[1].DisruptionCost, st), k,
				updisruption.ScoreMove(up[0].Price+up[1].Price, up[0].RescheduleDisruptionCost+up[1].RescheduleDisruptionCost, ut, k))
		}},
		{bs + ":267", "EvaluateBalancedMove/pool missing from totals", func(t *testing.T) {
			np := balancedPool("pool-missing")
			sim, up := pool(t, "n", np, upInstanceType("m7i.xlarge", 4.84), 1, 1)
			approved, perPool := updisruption.EvaluateBalancedMove(ctx, updisruption.Command{Candidates: up}, map[string]updisruption.NodePoolTotals{})
			r := perPool[np.Name]
			if r.Score() != 0.0 || r.Approved() || approved {
				t.Errorf("upstream states score 0, rejected")
			}
			agree(t, "zero totals", Score(sim[0].Price, sim[0].DisruptionCost, PoolTotals{}), k, r)
		}},
		{bs + ":286", "EvaluateBalancedMove/single-pool delete approves", func(t *testing.T) {
			np := balancedPool("pool-single")
			sim, up := pool(t, "n", np, upInstanceType("m7i.xlarge", 4.84), 10, 1)
			ut := upTotals(up)
			approved, perPool := updisruption.EvaluateBalancedMove(ctx, updisruption.Command{Candidates: up[:1]}, map[string]updisruption.NodePoolTotals{np.Name: ut})
			r := perPool[np.Name]
			wantScore(t, "score", r.Score(), 1.0, 0.05)
			if !approved {
				t.Error("upstream states approved")
			}
			agree(t, "delete", Score(sim[0].Price, sim[0].DisruptionCost, Totals(sim)), k, r)
		}},
		{bs + ":313", "EvaluateBalancedMove/cross-pool proportional savings", func(t *testing.T) {
			// The simulator has one PoolTotals per run and no cross-pool attribution.
			// For a delete, upstream's attributed savings is the pool's own source
			// cost, so scoring the Balanced pool's share against its own totals is the
			// same arithmetic. The second pool is non-Balanced and out of model.
			bnp := balancedPool("pool-balanced")
			enp := upNodePool("pool-empty", v1.ConsolidationPolicyWhenEmptyOrUnderutilized)
			it := upInstanceType("m7i.xlarge", 4.84)
			bsim, bup := pool(t, "b", bnp, it, 5, 1)
			_, eup := pool(t, "e", enp, it, 5, 1)
			totals := map[string]updisruption.NodePoolTotals{bnp.Name: upTotals(bup), enp.Name: upTotals(eup)}
			approved, perPool := updisruption.EvaluateBalancedMove(ctx, updisruption.Command{Candidates: []*updisruption.Candidate{bup[0], eup[0]}}, totals)
			r := perPool[bnp.Name]
			wantScore(t, "score", r.Score(), 1.0, 0.05)
			if !approved {
				t.Error("upstream states approved")
			}
			agree(t, "balanced pool", Score(bsim[0].Price, bsim[0].DisruptionCost, Totals(bsim)), k, r)
		}},
		{bs + ":346", "EvaluateBalancedMove/all pools must approve", func(t *testing.T) {
			poolA, poolB := balancedPool("pool-a"), balancedPool("pool-b")
			itA, itB := upInstanceType("m7i.xlarge", 4.84), upInstanceType("tiny", 1.0)
			asim, aup := pool(t, "a", poolA, itA, 10, 1)
			for _, heavy := range []int{20, 40} {
				hp, lp := pods(fmt.Sprintf("heavy%d", heavy), heavy, ""), []*corev1.Pod{pod("light", "")}
				bsim := []NodeSnapshot{simNode(t, "b-0", poolB, itB, hp), simNode(t, "b-1", poolB, itB, lp)}
				bup := []*updisruption.Candidate{upCandidate("b-0", poolB, itB, hp), upCandidate("b-1", poolB, itB, lp)}
				totals := map[string]updisruption.NodePoolTotals{poolA.Name: upTotals(aup), poolB.Name: upTotals(bup)}
				approved, perPool := updisruption.EvaluateBalancedMove(ctx, updisruption.Command{Candidates: []*updisruption.Candidate{aup[0], bup[0]}}, totals)
				// Upstream's comments: pool B 0.5/(21/23) ~= 0.548 and 0.5/(41/43) ~= 0.524.
				want := map[int]float64{20: 0.548, 40: 0.524}[heavy]
				wantScore(t, "pool B", perPool[poolB.Name].Score(), want, 0.001)
				wantScore(t, "pool A", perPool[poolA.Name].Score(), 1.0, 0.001)
				agree(t, fmt.Sprintf("pool A (B has %d pods)", heavy), Score(asim[0].Price, asim[0].DisruptionCost, Totals(asim)), k, perPool[poolA.Name])
				agree(t, fmt.Sprintf("pool B with %d pods", heavy), Score(bsim[0].Price, bsim[0].DisruptionCost, Totals(bsim)), k, perPool[poolB.Name])
				simAll := Score(asim[0].Price, asim[0].DisruptionCost, Totals(asim)) >= 0.5 &&
					Score(bsim[0].Price, bsim[0].DisruptionCost, Totals(bsim)) >= 0.5
				if simAll != approved {
					t.Errorf("all-pools verdict: sim=%v upstream=%v", simAll, approved)
				}
			}
		}},
		{bs + ":413", "sumCandidatePrices/nil instance type (intended difference)", func(t *testing.T) {
			c := upCandidate("node", balancedPool("pool"), nil, nil)
			if c.Price != 0.0 {
				t.Errorf("upstream states 0, got %v", c.Price)
			}
			if _, ok := cheapestOffering(nil); ok {
				t.Error("the simulator priced a nil instance type")
			}
			mustPanic(t, "zero price", func() { Totals([]NodeSnapshot{{ProviderID: "node", Price: c.Price, DisruptionCost: 1}}) })
		}},
		{bs + ":419", "sumCandidatePrices/valid instance type", func(t *testing.T) {
			it := upInstanceType("m7i.xlarge", 4.84)
			c := upCandidate("node2", balancedPool("pool"), it, nil)
			wantScore(t, "price", c.Price, 4.84, 0.001)
			if s := simNode(t, "node2", nil, it, nil); !approxEq(s.Price, c.Price) {
				t.Errorf("price sim=%v upstream=%v", s.Price, c.Price)
			}
		}},
		{bs + ":428", "Candidate.SavingsRatio", func(t *testing.T) {
			np := balancedPool("pool")
			it := upInstanceType("m7i.xlarge", 4.84)
			three := []*corev1.Pod{pod("p1", ""), pod("p2", ""), pod("p3", "")}
			for _, tc := range []struct {
				name string
				it   *cloudprovider.InstanceType
				pods []*corev1.Pod
				want float64
			}{{"no pods", it, nil, 4.84}, {"three pods", it, three, 1.21}, {"nil instance type", nil, three, 0.0}} {
				up := upCandidate("n", np, tc.it, tc.pods)
				wantScore(t, tc.name, up.SavingsRatio(), tc.want, 0.01)
				// The nil-type node is unpriceable, so build its snapshot by hand at the
				// price upstream resolved; SavingsRatio itself has no price guard.
				s := NodeSnapshot{Price: up.Price, DisruptionCost: disruptionCost(tc.pods)}
				if tc.it != nil {
					s = simNode(t, "n", np, tc.it, tc.pods)
				}
				if !approxEq(SavingsRatio(s), up.SavingsRatio()) {
					t.Errorf("%s: sim=%v upstream=%v", tc.name, SavingsRatio(s), up.SavingsRatio())
				}
			}
		}},
		tableEntry(468, "high savings low disruption", 20.0, 5.0, 4.0, true, false),
		tableEntry(469, "equal fractions", 10.0, 10.0, 1.0, true, false),
		tableEntry(470, "low savings high disruption", 5.0, 50.0, 0.10, false, false),
		tableEntry(471, "zero disruption cost", 10.0, 0.0, 0.0, true, true),
		tableEntry(472, "zero savings", 0.0, 10.0, 0.0, false, false),
		tableEntry(473, "negative savings", -5.0, 10.0, 0.0, false, false),
		{bs + ":476", "ScoreMove/spot offering must not price an on-demand replacement", func(t *testing.T) {
			np := balancedPool("pool-onDemand-only")
			lbl := func(ct string) scheduling.Requirements {
				return scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: ct, corev1.LabelTopologyZone: "test-zone-1"})
			}
			// Upstream builds these with fake.NewInstanceType; only the offerings
			// matter to pricing, and the fake package would pull testify into go.mod.
			sourceIT := &cloudprovider.InstanceType{Name: "source-type", Offerings: cloudprovider.Offerings{
				{Available: true, Price: 0.50, Requirements: lbl(v1.CapacityTypeOnDemand)},
			}}
			destIT := &cloudprovider.InstanceType{Name: "dest-type", Offerings: cloudprovider.Offerings{
				{Available: true, Price: 0.10, Requirements: lbl(v1.CapacityTypeSpot)},
				{Available: true, Price: 0.40, Requirements: lbl(v1.CapacityTypeOnDemand)},
			}}
			three := []*corev1.Pod{pod("p1", ""), pod("p2", ""), pod("p3", "")}
			cand := upCandidate("source-node", np, sourceIT, three)
			onDemand := scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand))
			cmd := updisruption.Command{
				Candidates:   []*updisruption.Candidate{cand},
				Replacements: []*updisruption.Replacement{{}},
				Results: pscheduling.Results{NewNodeClaims: []*pscheduling.NodeClaim{{
					NodeClaimTemplate: pscheduling.NodeClaimTemplate{InstanceTypeOptions: []*cloudprovider.InstanceType{destIT}, Requirements: onDemand},
				}}},
			}
			poolTotals := updisruption.NodePoolTotals{TotalCost: 1.0, TotalDisruptionCost: 10.0}
			upSavings := cmd.EstimatedSavings()
			up := updisruption.ScoreMove(upSavings, updisruption.ComputeMoveDisruptionCost(cmd.Candidates), poolTotals, k)
			wantScore(t, "savings", upSavings, 0.10, 0.001)
			if up.Approved() {
				t.Error("upstream states rejected")
			}

			// The simulator prices a replacement with Catalog.Cheapest, under the
			// NodePool's requirements.
			src := simNode(t, "source-node", np, sourceIT, three)
			cat := &Catalog{InstanceTypes: []*cloudprovider.InstanceType{destIT}, Requirements: onDemand}
			_, price, ok := cat.Cheapest(func(*cloudprovider.InstanceType) bool { return true })
			if !ok {
				t.Fatal("no replacement")
			}
			simSavings := src.Price - price
			if !approxEq(simSavings, upSavings) {
				t.Errorf("savings sim=%v upstream=%v", simSavings, upSavings)
			}
			agree(t, "replace", Score(simSavings, src.DisruptionCost, PoolTotals{Cost: 1, DisruptionCost: 10}), k, up)
		}},
		{bs + ":540", "ShouldDisrupt/Balanced accepted (unexported upstream)", func(t *testing.T) {
			// makeShouldDisruptCandidate: RescheduleDisruptionCost = base + 1. Upstream
			// states ShouldDisrupt is true.
			n := NodeSnapshot{ProviderID: "test-node", NodePool: balancedPool("test-pool"), DisruptionCost: PerNodeBaseDisruptionCost + 1}
			if !(SingleNodeEvaluation{}).ShouldDisrupt(n, PoolTotals{}, 0.5) || !(MultiNodeEvaluation{}).ShouldDisrupt(n, PoolTotals{}, 0.5) {
				t.Error("upstream states true")
			}
		}},
		{bs + ":552", "ShouldDisrupt/WhenEmptyOrUnderutilized accepted (unexported upstream, out of model)", func(t *testing.T) {
			n := NodeSnapshot{ProviderID: "test-node", NodePool: upNodePool("test-pool", v1.ConsolidationPolicyWhenEmptyOrUnderutilized), DisruptionCost: PerNodeBaseDisruptionCost + 1}
			if !(SingleNodeEvaluation{}).ShouldDisrupt(n, PoolTotals{}, 0.5) {
				t.Error("upstream states true")
			}
		}},
		{bs + ":566", "sortCandidates/Balanced by savings ratio (unexported upstream)", func(t *testing.T) {
			np := balancedPool("balanced")
			a := simNode(t, "node-a", np, upInstanceType("expensive", 10.0), []*corev1.Pod{pod("pod-a", "")})
			b := simNode(t, "node-b", np, upInstanceType("cheap", 1.0), pods("pod-b", 8, ""))
			c := simNode(t, "node-c", np, upInstanceType("medium", 5.0), pods("pod-c", 3, ""))
			assertOrder(t, sortBySavingsRatio([]NodeSnapshot{b, c, a}), "node-a", "node-c", "node-b")
			for _, tc := range []struct {
				n    NodeSnapshot
				want float64
			}{{a, 5.0}, {b, 1.0 / 9}, {c, 1.25}} {
				up := upCandidate(tc.n.ProviderID, np, tc.n.InstanceType, tc.n.ReschedulablePods)
				if !approxEq(SavingsRatio(tc.n), up.SavingsRatio()) {
					t.Errorf("%s ratio sim=%v upstream=%v", tc.n.ProviderID, SavingsRatio(tc.n), up.SavingsRatio())
				}
			}
		}},
		{bs + ":601", "sortCandidates/non-Balanced by savings ratio (unexported upstream)", func(t *testing.T) {
			np := upNodePool("default", v1.ConsolidationPolicyWhenEmptyOrUnderutilized)
			mk := func(name string, cost float64) NodeSnapshot {
				n := simNode(t, name, np, upInstanceType(name, 4.84), nil)
				n.DisruptionCost = cost
				return n
			}
			assertOrder(t, sortBySavingsRatio([]NodeSnapshot{mk("node-a", 10), mk("node-b", 1), mk("node-c", 5)}), "node-b", "node-c", "node-a")
		}},
		{bs + ":626", "sortCandidates/mixed policies (unexported upstream)", func(t *testing.T) {
			bal := simNode(t, "node-balanced", balancedPool("balanced"), upInstanceType("expensive", 10.0), []*corev1.Pod{pod("p1", "")})
			def := simNode(t, "node-default", upNodePool("default", v1.ConsolidationPolicyWhenEmptyOrUnderutilized), upInstanceType("cheap", 1.0), []*corev1.Pod{pod("p2", "")})
			if got := sortBySavingsRatio([]NodeSnapshot{def, bal}); got[0].ProviderID != "node-balanced" {
				t.Errorf("upstream states balanced first, got %s", got[0].ProviderID)
			}
		}},
		{bs + ":645", "EmitMultiNodeEvents/approved result", func(t *testing.T) {
			r := updisruption.ScoreResult{SavingsFraction: 0.15, DisruptionFraction: 0.10, K: 2}
			if !r.Approved() {
				t.Error("upstream emits the approved event, so the result approves")
			}
			agree(t, "0.15/0.10 k=2", Score(0.15, 0.10, PoolTotals{Cost: 1, DisruptionCost: 1}), 2, r)
		}},
		{bs + ":664", "EmitMultiNodeEvents/rejected result", func(t *testing.T) {
			r := updisruption.ScoreResult{SavingsFraction: 0.03, DisruptionFraction: 0.10, K: 1}
			if r.Approved() {
				t.Error("upstream emits nothing for this result, so it rejects")
			}
			agree(t, "0.03/0.10 k=1", Score(0.03, 0.10, PoolTotals{Cost: 1, DisruptionCost: 1}), 1, r)
		}},
		{bs + ":681", "EmitMultiNodeEvents/pool missing from results", func(t *testing.T) {
			r := updisruption.ScoreResult{SavingsFraction: 0.10, DisruptionFraction: 0.05, K: v1.BalancedK}
			if !r.Approved() {
				t.Error("upstream emits one approved event for pool-a")
			}
			agree(t, "0.10/0.05", Score(0.10, 0.05, PoolTotals{Cost: 1, DisruptionCost: 1}), v1.BalancedK, r)
		}},

		{ba + ":34", "NaN price must not poison totals (intended difference)", func(t *testing.T) {
			np := balancedPool("pool-nan")
			p := []*corev1.Pod{pod("pod", "")}
			normal := upInstanceType("m7i.xlarge", 4.84)
			nan := upInstanceType("nan-instance", math.NaN())
			up := []*updisruption.Candidate{upCandidate("node-good-1", np, normal, p), upCandidate("node-nan", np, nan, p), upCandidate("node-good-2", np, normal, p)}
			ut := upTotals(up)
			r := updisruption.ScoreMove(4.84, 2.0, ut, 2)
			if math.IsNaN(ut.TotalCost) || math.IsNaN(r.Score()) {
				t.Error("upstream states no NaN in totals or score")
			}
			// Upstream maps NaN to 0 and scores on. The simulator refuses the node.
			mustPanic(t, "NaN price", func() {
				Totals([]NodeSnapshot{simNode(t, "node-good-1", np, normal, p), {ProviderID: "node-nan", Price: math.NaN(), DisruptionCost: 2}})
			})
			// Given upstream's totals, the two score the move identically.
			agree(t, "score on upstream totals", Score(4.84, 2.0, PoolTotals{Cost: ut.TotalCost, DisruptionCost: ut.TotalDisruptionCost}), 2, r)
		}},
		{ba + ":70", "perPool populated for every pool", func(t *testing.T) {
			poolA, poolB := balancedPool("pool-a"), balancedPool("pool-b")
			itA, itB := upInstanceType("a-type", 1.0), upInstanceType("b-type", 1.0)
			heavy := pods("heavy", 20, "1000")
			light := []*corev1.Pod{pod("light", "")}
			asim := []NodeSnapshot{simNode(t, "a-0", poolA, itA, heavy), simNode(t, "a-1", poolA, itA, light)}
			aup := []*updisruption.Candidate{upCandidate("a-0", poolA, itA, heavy), upCandidate("a-1", poolA, itA, light)}
			bsim, bup := pool(t, "b", poolB, itB, 10, 1)
			totals := map[string]updisruption.NodePoolTotals{poolA.Name: upTotals(aup), poolB.Name: upTotals(bup)}
			_, perPool := updisruption.EvaluateBalancedMove(upCtx, updisruption.Command{Candidates: []*updisruption.Candidate{aup[0], bup[0]}}, totals)
			for _, name := range []string{poolA.Name, poolB.Name} {
				if r, ok := perPool[name]; !ok || r.K == 0 {
					t.Errorf("upstream states %s present with K != 0", name)
				}
			}
			agree(t, "pool A", Score(asim[0].Price, asim[0].DisruptionCost, Totals(asim)), k, perPool[poolA.Name])
			agree(t, "pool B", Score(bsim[0].Price, bsim[0].DisruptionCost, Totals(bsim)), k, perPool[poolB.Name])
		}},
		{ba + ":117", "sum with nil instance type (intended difference)", func(t *testing.T) {
			np := balancedPool("pool-mixed")
			it := upInstanceType("m7i.xlarge", 4.84)
			p := []*corev1.Pod{pod("pod", "")}
			up := []*updisruption.Candidate{upCandidate("node-good-1", np, it, p), upCandidate("node-nil", np, nil, p), upCandidate("node-good-2", np, it, p)}
			wantScore(t, "sum", upTotals(up).TotalCost, 9.68, 0.01)
			good := []NodeSnapshot{simNode(t, "node-good-1", np, it, p), simNode(t, "node-good-2", np, it, p)}
			if st := Totals(good); !approxEq(st.Cost, upTotals(up).TotalCost) {
				t.Errorf("well-priced sum sim=%v upstream=%v", st.Cost, upTotals(up).TotalCost)
			}
			mustPanic(t, "nil instance type", func() {
				Totals(append(good, NodeSnapshot{ProviderID: "node-nil", Price: 0, DisruptionCost: 2}))
			})
		}},
		{ba + ":133", "source pricing ignores availability", func(t *testing.T) {
			// Upstream resolves a running node's price by its zone and capacity type,
			// ignoring Available. The simulator never re-resolves a running node: the
			// price is fixed on the snapshot at launch, so an offering going
			// unavailable afterwards cannot move it either. The node ran at $1.00 in
			// test-zone-1.
			np := balancedPool("pool-avail")
			p := []*corev1.Pod{pod("pod", "")}
			sourceIT := &cloudprovider.InstanceType{Name: "source-type", Offerings: cloudprovider.Offerings{
				{Requirements: scheduling.NewRequirements(
					scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "test-zone-1"),
					scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
				), Price: 1.00, Available: true},
				{Requirements: scheduling.NewRequirements(
					scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "test-zone-2"),
					scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
				), Price: 5.00, Available: true},
			}}
			launched := simNode(t, "node-src", np, sourceIT, p)
			sourceIT.Offerings[0].Available = false // ICE'd after launch
			up := upCandidate("node-src", np, sourceIT, p)
			if up.Price != 1.00 {
				t.Errorf("upstream states $1.00, got %v", up.Price)
			}
			if st := Totals([]NodeSnapshot{launched}); st.Cost != up.Price {
				t.Errorf("source cost sim=%v upstream=%v", st.Cost, up.Price)
			}
		}},
		{ba + ":173", "single-node pool delete", func(t *testing.T) {
			np := balancedPool("pool-single")
			sim, up := pool(t, "only", np, upInstanceType("m7i.xlarge", 4.84), 1, 1)
			ut := upTotals(up)
			approved, perPool := updisruption.EvaluateBalancedMove(ctx, updisruption.Command{Candidates: up}, map[string]updisruption.NodePoolTotals{np.Name: ut})
			r := perPool[np.Name]
			if !approved || r.SavingsFraction < 1.0 {
				t.Errorf("upstream states approved with savings fraction 1.0, got %v %v", approved, r.SavingsFraction)
			}
			agree(t, "delete", Score(sim[0].Price, sim[0].DisruptionCost, Totals(sim)), k, r)
		}},
	}
}

func assertOrder(t *testing.T, got []NodeSnapshot, want ...string) {
	t.Helper()
	for i, id := range want {
		if got[i].ProviderID != id {
			t.Errorf("position %d: got %s want %s", i, got[i].ProviderID, id)
		}
	}
}

// TestScoreMatchesUpstream is the README's fidelity claim. Every scenario from
// both upstream files, each citing the upstream line it reproduces.
func TestScoreMatchesUpstream(t *testing.T) {
	scenarios := upstreamScenarios()
	// 26 in balanced_scoring_test.go (20 Its plus 6 table entries) and 5 in
	// balanced_adversarial_test.go. A scenario added upstream should fail this
	// count when the pin moves, rather than go unported.
	byFile := map[string]int{}
	seen := map[string]bool{}
	for _, s := range scenarios {
		if seen[s.ref] {
			t.Fatalf("duplicate scenario %s", s.ref)
		}
		seen[s.ref] = true
		byFile[strings.SplitN(s.ref, ":", 2)[0]]++
	}
	if byFile["balanced_scoring_test.go"] != 26 || byFile["balanced_adversarial_test.go"] != 5 {
		t.Fatalf("want 26 + 5 upstream scenarios, have %v", byFile)
	}
	for _, s := range scenarios {
		t.Run(s.ref+" "+s.name, s.run)
	}
}
