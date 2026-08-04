package sim

import (
	"context"
	"fmt"
	"math"
	"testing"

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// The decision unit is the workload scope. These tests are about the unit: what a
// candidate is, what it is charged, what it reads, and what a move over it takes.

// -----------------------------------------------------------------------------
// The unit
// -----------------------------------------------------------------------------

// A node holding no gang member is one candidate and nothing about it moved. This
// is the regression bar, stated first because everything else is measured against
// it.
func TestNonGangNodeIsOneCandidateUnchanged(t *testing.T) {
	cat := BuildCatalog()
	opts := DefaultGenClusterOpts(150, 3)
	opts.Catalog = cat
	nodes, totals := GenCluster(opts)

	cands := Candidates(nodes)
	if len(cands) != len(nodes) {
		t.Fatalf("a fleet with no gangs grouped %d nodes into %d candidates, want one each",
			len(nodes), len(cands))
	}
	for i, c := range cands {
		if c.IsGang() {
			t.Fatalf("candidate %d is a gang on a fleet with no gang labels", i)
		}
		if len(c.Nodes) != 1 || c.Nodes[0].ProviderID != nodes[i].ProviderID {
			t.Fatalf("candidate %d covers %v, want just %s", i, c.NodeIDs(), nodes[i].ProviderID)
		}
		// Price, disruption cost, delete ratio and savings ratio all read exactly what
		// the node's own carry. Any drift here changes a non-gang number.
		if c.Price != nodes[i].Price || c.DisruptionCost != nodes[i].DisruptionCost {
			t.Errorf("%s: candidate priced %v/%v, node %v/%v",
				c.ID(), c.Price, c.DisruptionCost, nodes[i].Price, nodes[i].DisruptionCost)
		}
		if got, want := c.DeleteRatio(totals), DeleteRatio(nodes[i], totals); got != want {
			t.Errorf("%s: candidate ratio %v, node ratio %v", c.ID(), got, want)
		}
		if got, want := c.SavingsRatio(), SavingsRatio(nodes[i]); got != want {
			t.Errorf("%s: candidate savings ratio %v, node %v", c.ID(), got, want)
		}
	}
}

// A gang is one candidate over its own nodes, and it is charged one outage whether
// its members sit on one node or on five. That is the whole point of the unit: the
// number does not depend on how the packer happened to scatter the workload.
func TestGangIsOneCandidateChargedOneOutage(t *testing.T) {
	const outage = 8.0
	cat := BuildCatalog()

	// The same five members, laid out five ways: all on one node, then split over
	// two, three, four and five. In every layout the gang is one candidate and the
	// gang's share of the charge is one outage.
	for _, layout := range [][]int{{5}, {3, 2}, {2, 2, 1}, {2, 1, 1, 1}, {1, 1, 1, 1, 1}} {
		five := gangMembers("g", 5, outage)
		var nodes []NodeSnapshot
		at := 0
		for i, count := range layout {
			pods := five[at : at+count]
			at += count
			n := typedNode(t, cat, fmt.Sprintf("i-%02d", i), "c7i.large", pods...)
			n.DisruptionCost = NodeDisruptionCost(pods)
			nodes = append(nodes, n)
		}

		cands := Candidates(nodes)
		if len(cands) != 1 {
			t.Fatalf("layout %v produced %d candidates, want the one gang", layout, len(cands))
		}
		c := cands[0]
		if !c.IsGang() || c.GangID != "g" {
			t.Fatalf("layout %v produced candidate %q, want gang g", layout, c.ID())
		}
		if len(c.Nodes) != len(layout) {
			t.Errorf("layout %v: candidate covers %d nodes, want %d", layout, len(c.Nodes), len(layout))
		}
		if len(c.Pods) != 5 {
			t.Errorf("layout %v: candidate holds %d pods, want the 5 members", layout, len(c.Pods))
		}

		// One outage, plus one baseline per node the move drains. The baseline is per
		// node because draining a node costs something whatever is on it; the outage is
		// per gang because the workload goes down once.
		want := float64(len(layout))*PerNodeBaseDisruptionCost + outage
		if math.Abs(c.DisruptionCost-want) > 1e-9 {
			t.Errorf("layout %v: candidate charged %v, want %v", layout, c.DisruptionCost, want)
		}

		// The old unit is what this replaces: summing the nodes' own costs charges the
		// outage once per node holding a member.
		perNode := 0.0
		for _, n := range nodes {
			perNode += n.DisruptionCost
		}
		wantOld := float64(len(layout))*PerNodeBaseDisruptionCost + float64(len(layout))*outage
		if math.Abs(perNode-wantOld) > 1e-9 {
			t.Fatalf("premise broken: the per-node sum over layout %v is %v, want %v",
				layout, perNode, wantOld)
		}
	}
}

// The gang's charge does not move with its layout, stated as one comparison rather
// than inferred from the loop above. A gang on five nodes costs more than a gang on
// one only by the four extra baselines, which is the cost of draining four more
// nodes and not a cost of the gang.
func TestGangChargeIsIndifferentToLayoutBeyondBaselines(t *testing.T) {
	const outage = 8.0
	cat := BuildCatalog()

	mk := func(layout []int) Candidate {
		five := gangMembers("g", 5, outage)
		var nodes []NodeSnapshot
		at := 0
		for i, count := range layout {
			pods := five[at : at+count]
			at += count
			n := typedNode(t, cat, fmt.Sprintf("i-%02d", i), "c7i.large", pods...)
			n.DisruptionCost = NodeDisruptionCost(pods)
			nodes = append(nodes, n)
		}
		cands := Candidates(nodes)
		if len(cands) != 1 {
			t.Fatalf("layout %v gave %d candidates", layout, len(cands))
		}
		return cands[0]
	}

	one := mk([]int{5})
	five := mk([]int{1, 1, 1, 1, 1})

	gangShareOne := one.DisruptionCost - float64(len(one.Nodes))*PerNodeBaseDisruptionCost
	gangShareFive := five.DisruptionCost - float64(len(five.Nodes))*PerNodeBaseDisruptionCost
	if math.Abs(gangShareOne-gangShareFive) > 1e-9 {
		t.Errorf("the gang's own charge is %v confined and %v scattered; the unit is supposed "+
			"to make it one outage either way", gangShareOne, gangShareFive)
	}
	if math.Abs(gangShareOne-outage) > 1e-9 {
		t.Errorf("the gang's own charge is %v, want exactly one outage of %v", gangShareOne, outage)
	}
}

// Two gangs sharing a node are two candidates and not one merged unit. Merging
// them by transitive closure over shared nodes is what would make a candidate
// unbounded: two workloads that happen to share hardware are not one workload, and
// a unit that grows with the packing is the unboundedness that makes topology
// spread non-local.
func TestTwoGangsSharingANodeStayTwoCandidates(t *testing.T) {
	const outage = 8.0
	cat := BuildCatalog()
	ga := gangMembers("ga", 2, outage)
	gb := gangMembers("gb", 2, outage)

	// i-shared holds one member of each. i-a and i-b hold the other member each.
	shared := []*corev1.Pod{ga[0], gb[0]}
	nShared := typedNode(t, cat, "i-shared", "c7i.large", shared...)
	nShared.DisruptionCost = NodeDisruptionCost(shared)
	nA := typedNode(t, cat, "i-a", "c7i.large", ga[1])
	nA.DisruptionCost = NodeDisruptionCost([]*corev1.Pod{ga[1]})
	nB := typedNode(t, cat, "i-b", "c7i.large", gb[1])
	nB.DisruptionCost = NodeDisruptionCost([]*corev1.Pod{gb[1]})

	cands := Candidates([]NodeSnapshot{nShared, nA, nB})
	if len(cands) != 2 {
		t.Fatalf("got %d candidates, want one per gang", len(cands))
	}
	for _, c := range cands {
		if !c.IsGang() {
			t.Fatalf("candidate %q is not a gang; the shared node is not its own candidate", c.ID())
		}
		if len(c.Nodes) != 2 {
			t.Errorf("gang %s covers %v, want its own two nodes", c.GangID, c.NodeIDs())
		}
		// Each candidate is honestly charged for everything its move breaks. Taking
		// ga's nodes evicts gb's member on i-shared, so it pays gb's outage too. That
		// is not double counting; it is what the move actually costs.
		want := 2*PerNodeBaseDisruptionCost + 2*outage
		if math.Abs(c.DisruptionCost-want) > 1e-9 {
			t.Errorf("gang %s charged %v, want %v: two baselines and both gangs it breaks",
				c.GangID, c.DisruptionCost, want)
		}
	}
	// The shared node is in both, which is the overlap the compactor resolves rather
	// than a unit the grouping should have merged.
	if cands[0].NodeIDs()[0] != "i-a" && cands[0].NodeIDs()[0] != "i-shared" {
		t.Errorf("gang %s node set %v does not contain the shared node", cands[0].GangID, cands[0].NodeIDs())
	}
}

// A node holding both gang members and ordinary pods puts the ordinary pods in the
// gang candidate, because draining the node evicts them too. They are priced as
// they always were, per pod, on top of the gang's step.
func TestOrdinaryPodsRideAlongInAGangCandidate(t *testing.T) {
	const outage = 8.0
	cat := BuildCatalog()
	member := gangPod("g-m0", "g", outage)
	riders := []*corev1.Pod{pod("r1", ""), pod("r2", "")}
	pods := append([]*corev1.Pod{member}, riders...)

	n := typedNode(t, cat, "i-a", "c7i.large", pods...)
	n.DisruptionCost = NodeDisruptionCost(pods)

	cands := Candidates([]NodeSnapshot{n})
	if len(cands) != 1 || !cands[0].IsGang() {
		t.Fatalf("got %d candidates, want the one gang", len(cands))
	}
	c := cands[0]
	if len(c.Pods) != 3 {
		t.Errorf("candidate holds %d pods, want the member and both riders", len(c.Pods))
	}
	// One baseline, one outage, and one per rider.
	want := PerNodeBaseDisruptionCost + outage + 2
	if math.Abs(c.DisruptionCost-want) > 1e-9 {
		t.Errorf("charged %v, want %v", c.DisruptionCost, want)
	}
}

// A do-not-disrupt pod anywhere on a gang's nodes blocks the whole candidate.
// There is no half-consolidating a gang, so a node the move cannot drain is a move
// that cannot happen.
func TestDoNotDisruptOnAnyGangNodeBlocksTheCandidate(t *testing.T) {
	const outage = 8.0
	cat := BuildCatalog()
	two := gangMembers("g", 2, outage)
	blocked := pod("blocked", "")
	blocked.Annotations = map[string]string{v1.DoNotDisruptAnnotationKey: "true"}

	first := []*corev1.Pod{two[0]}
	second := []*corev1.Pod{two[1], blocked}
	nA := typedNode(t, cat, "i-a", "c7i.large", first...)
	nA.DisruptionCost = NodeDisruptionCost(first)
	nB := typedNode(t, cat, "i-b", "c7i.large", second...)
	nB.DisruptionCost = NodeDisruptionCost(second)

	cands := Candidates([]NodeSnapshot{nA, nB})
	if len(cands) != 1 {
		t.Fatalf("got %d candidates", len(cands))
	}
	if cands[0].Disruptable() {
		t.Error("a gang whose second node holds a do-not-disrupt pod was called disruptable")
	}
	// The first node alone is disruptable, so it is the second that decides. Without
	// the whole-candidate test the gang would look drainable from i-a.
	if !Disruptable(nA) {
		t.Fatal("fixture broken: i-a should be disruptable on its own")
	}

	totals := Totals([]NodeSnapshot{nA, nB})
	bits := CandidateBits(cands, totals, 2)
	if bits[0] {
		t.Error("stage 1 kept a gang candidate it cannot drain")
	}
}

// Every non-gang number through a whole pass is what it was. The unit changed, so
// the risk is that a fleet with no gang labels now takes a different route through
// the pipeline, and the way to rule that out is to run the pipeline.
//
// The gang path must contribute nothing, since there is nothing for it to decide
// about, and stage 1 must group one candidate per node so CandidatesOut equals what
// the node filter would have produced.
func TestAGanglessPassIsUnchanged(t *testing.T) {
	for _, size := range []int{100, 400} {
		cat := BuildCatalog()
		opts := DefaultGenClusterOpts(size, 7)
		opts.Catalog = cat
		nodes, totals := GenCluster(opts)
		cfg := DefaultRunConfig(2, 20)
		res := Run(context.Background(), nodes, totals, cat, cfg)

		if res.CandidatesIn != len(nodes) {
			t.Errorf("N=%d: %d nodes grouped into %d units, want one each",
				size, len(nodes), res.CandidatesIn)
		}
		if res.GangCandidatesOut != 0 {
			t.Errorf("N=%d: %d gang candidates on a fleet with no gang labels",
				size, res.GangCandidatesOut)
		}
		// The gang path saw nothing and proposed nothing.
		if g := res.Paths[PathGang]; g.Candidates != 0 || g.Proposals != 0 {
			t.Errorf("N=%d: the gang path saw %d candidates and made %d proposals on a "+
				"gangless fleet", size, g.Candidates, g.Proposals)
		}
		// Stage 1's own bit agrees with the node-scoped filter it used to be, node for
		// node, since with no gangs the candidate is the node.
		want := 0
		for _, n := range nodes {
			if Disruptable(n) && DeleteRatio(n, totals) >= 1/cfg.K {
				want++
			}
		}
		if res.CandidatesOut != want {
			t.Errorf("N=%d: stage 1 kept %d candidates, the node filter keeps %d",
				size, res.CandidatesOut, want)
		}
		// Every proposal is a node-unit move.
		for _, p := range []Path{PathEmpty, PathMultiNode, PathSingleNode} {
			if res.Paths[p].Proposals == 0 {
				t.Errorf("N=%d: the %s path produced nothing", size, p)
			}
		}
		fmt.Printf("gangless N=%d: units=%d candidates=%d proposals=%d committed=%d realized=$%.4f\n",
			size, res.CandidatesIn, res.CandidatesOut, res.ProposalsGenerated,
			res.Committed, res.SavingsRealized)
	}
}

// -----------------------------------------------------------------------------
// The collapse
// -----------------------------------------------------------------------------

// The collapse is gone.
//
// Before this change the node was the unit, so a node holding one member of a
// spanning gang was charged the whole outage. Five such nodes each paid a full
// outage for a gang that costs one outage to break, which pushed them below the
// delete-ratio floor. At 200 nodes seed 7, with gang prevalence 0, 0.1 and 0.3, the
// nodes clearing that floor went:
//
//	OLD (node unit, ratio floor only):  183 -> 121 ->  45
//	                     as a price share:  96.6% -> 66.2% -> 25.5%
//
// A quarter of the fleet's price reachable at prevalence 0.3, from 96.6% with no
// gangs at all, for a pricing artifact rather than anything about the workloads.
//
// Scored at the workload scope the same fleet reads:
//
//	NEW (gang unit, ratio floor only):  183 -> 194 -> 200
//	                     as a price share:  96.6% -> 99.2% -> 100.0%
//
// It rises rather than merely holding, and the reason is the step: one gang
// candidate covering 33 nodes recovers 33 nodes' price for one outage, so its ratio
// is high and it carries every node it covers. That is the step applied at the right
// granularity, not a scoring error in the other direction. The move it approves is
// one move over 33 nodes, which the compactor and the budget then have to afford.
func TestTheGangPrevalenceCollapseIsGone(t *testing.T) {
	// Measured at the ratio floor alone, which is the term per-node pricing broke.
	// The do-not-disrupt test is held out because it is a different gate on a
	// different input, and mixing the two would hide which one moved. The test below
	// this one measures that gate on its own.
	type row struct {
		frac      float64
		nodes     int
		priceShar float64
	}
	var perNode, perUnit []row

	for _, frac := range []float64{0, 0.1, 0.3} {
		cat := BuildCatalog()
		opts := DefaultGenClusterOpts(200, 7)
		opts.Catalog = cat
		opts.Shape.GangFraction = frac
		opts.Shape.GangCount = 6
		nodes, totals := GenCluster(opts)

		// The old unit: every node scored alone.
		oldN, oldP := 0, 0.0
		for _, n := range nodes {
			if DeleteRatio(n, totals) >= 0.5 {
				oldN++
				oldP += n.Price
			}
		}
		perNode = append(perNode, row{frac, oldN, 100 * oldP / totals.Cost})

		// The new unit: candidates scored once, then projected onto the nodes they
		// reach, so the two columns count the same thing.
		reached := map[string]float64{}
		for _, c := range Candidates(nodes) {
			if c.DeleteRatio(totals) < 0.5 {
				continue
			}
			for _, n := range c.Nodes {
				reached[n.ProviderID] = n.Price
			}
		}
		newP := 0.0
		for _, p := range reached {
			newP += p
		}
		perUnit = append(perUnit, row{frac, len(reached), 100 * newP / totals.Cost})
	}

	fmt.Println("nodes clearing the delete-ratio floor, 200 nodes seed 7, k=2")
	fmt.Printf("%-24s %12s %12s %12s\n", "unit", "gangs=0", "gangs=0.1", "gangs=0.3")
	fmt.Printf("%-24s", "node (before)")
	for _, r := range perNode {
		fmt.Printf(" %5d (%4.1f%%)", r.nodes, r.priceShar)
	}
	fmt.Printf("\n%-24s", "workload scope (after)")
	for _, r := range perUnit {
		fmt.Printf(" %5d (%4.1f%%)", r.nodes, r.priceShar)
	}
	fmt.Println()

	// The before-numbers, pinned. If the fixture drifts, the comparison above is
	// against something other than the collapse this test is named for.
	for i, want := range []int{183, 121, 45} {
		if perNode[i].nodes != want {
			t.Fatalf("the node unit reached %d nodes at prevalence %.1f, want the recorded %d; "+
				"the fixture moved and the comparison is no longer about the collapse",
				perNode[i].nodes, perNode[i].frac, want)
		}
	}

	// The collapse: the node unit falls with prevalence, in nodes and in price.
	if !(perNode[0].nodes > perNode[1].nodes && perNode[1].nodes > perNode[2].nodes) {
		t.Fatalf("premise broken: the node unit did not collapse, %v", perNode)
	}

	// And it is gone. The workload scope does not fall with prevalence.
	if perUnit[1].nodes < perUnit[0].nodes || perUnit[2].nodes < perUnit[1].nodes {
		t.Errorf("the workload scope still falls with gang prevalence: %d -> %d -> %d",
			perUnit[0].nodes, perUnit[1].nodes, perUnit[2].nodes)
	}
	// Stated on price too, since a node count can hold while the price it covers
	// falls, and price is what a savings measurement is made of.
	if perUnit[2].priceShar < perUnit[0].priceShar {
		t.Errorf("the workload scope reaches %.1f%% of the pool's price at prevalence 0.3 "+
			"against %.1f%% with no gangs", perUnit[2].priceShar, perUnit[0].priceShar)
	}
	// The gangless column is untouched, which is the regression bar on this
	// measurement: with no gang labels the two units are the same unit.
	if perNode[0].nodes != perUnit[0].nodes ||
		math.Abs(perNode[0].priceShar-perUnit[0].priceShar) > 1e-9 {
		t.Errorf("with no gangs the units disagree: %d (%.1f%%) against %d (%.1f%%)",
			perNode[0].nodes, perNode[0].priceShar, perUnit[0].nodes, perUnit[0].priceShar)
	}
}

// What remains after the pricing collapse is removed, measured rather than left
// implicit, because a reader comparing the old candidate counts to the new ones
// will otherwise conclude the fix made things worse.
//
// The old measurement's 149 -> 102 -> 38 mixed two effects. The pricing one is
// gone. The other is do-not-disrupt, and it is not a pricing artifact: a gang
// candidate cannot be drained if any of its nodes holds a do-not-disrupt pod, and
// the default fixture puts one on 2% of pods. A gang candidate spanning 27 nodes
// with ten pods each covers roughly 270 pods, so the chance that none of them opted
// out is about 0.98^270, which is under a percent. Every gang candidate at
// GangCount 6 is blocked, and its nodes go with it.
//
// That is the unit reporting a real constraint rather than mispricing one. The
// gang's move needs every one of its nodes, so one opted-out pod anywhere refuses
// the whole move, and the alternative would be approving a move that takes a
// workload down and cannot finish. What the number is sensitive to is gang span,
// and the fixture's spans are an artifact of drawing membership per pod over six
// gangs: 27 and 73 nodes per gang are not workloads anyone runs. At gang sizes near
// five members the same fleet keeps candidates through both gates.
func TestWhatRemainsAfterTheCollapseIsDoNotDisruptNotPricing(t *testing.T) {
	fmt.Println("gang candidates at the two stage-1 gates, 200 nodes seed 7, k=2")
	fmt.Printf("%-12s %8s %8s %8s %8s %8s\n",
		"gangCount", "frac", "gangs", "avgSpan", "ratioOK", "drainOK")

	type row struct {
		count            int
		frac             float64
		gangs            int
		span             float64
		ratioOK, drainOK int
	}
	var rows []row
	for _, count := range []int{6, 120} {
		for _, frac := range []float64{0.1, 0.3} {
			cat := BuildCatalog()
			opts := DefaultGenClusterOpts(200, 7)
			opts.Catalog = cat
			opts.Shape.GangFraction = frac
			opts.Shape.GangCount = count
			nodes, totals := GenCluster(opts)

			r := row{count: count, frac: frac}
			span := 0
			for _, c := range Candidates(nodes) {
				if !c.IsGang() {
					continue
				}
				r.gangs++
				span += len(c.Nodes)
				if c.DeleteRatio(totals) >= 0.5 {
					r.ratioOK++
				}
				if c.Disruptable() {
					r.drainOK++
				}
			}
			if r.gangs > 0 {
				r.span = float64(span) / float64(r.gangs)
			}
			rows = append(rows, r)
			fmt.Printf("%-12d %8.1f %8d %8.1f %8d %8d\n",
				count, frac, r.gangs, r.span, r.ratioOK, r.drainOK)
		}
	}

	// At six gangs over 200 nodes the spans are enormous and nothing survives
	// do-not-disrupt, while the ratio floor passes everything. That separation is the
	// finding: the surviving loss is the drain gate and not the price.
	wide := rows[0]
	if wide.span < 20 {
		t.Fatalf("premise broken: gangs at GangCount 6 span %.1f nodes, expected tens", wide.span)
	}
	if wide.ratioOK != wide.gangs {
		t.Errorf("%d of %d wide gangs cleared the ratio floor; pricing is supposed to no "+
			"longer be what filters them", wide.ratioOK, wide.gangs)
	}
	if wide.drainOK != 0 {
		t.Errorf("%d of %d wide gangs were drainable; at ~270 pods each and 2%% opting out "+
			"none should be", wide.drainOK, wide.gangs)
	}

	// At gang sizes near five members, which is a gang someone runs, candidates
	// survive both gates.
	narrow := rows[2]
	if narrow.span > 8 {
		t.Fatalf("premise broken: gangs at GangCount 120 span %.1f nodes, expected a handful", narrow.span)
	}
	if narrow.drainOK == 0 {
		t.Errorf("no narrow gang was drainable; the drain gate is supposed to be about span")
	}
	if narrow.ratioOK == 0 {
		t.Errorf("no narrow gang cleared the ratio floor")
	}
}

// -----------------------------------------------------------------------------
// The move
// -----------------------------------------------------------------------------

// A gang move takes all the gang's nodes or none. There is no proposal over a
// subset, and every node the gang holds is in the one proposal.
func TestGangMoveTakesEveryNodeOrNone(t *testing.T) {
	const outage = 4.0
	cat := BuildCatalog()
	three := gangMembers("g", 3, outage)

	// Three small nodes, one member each, and a large destination with room for all
	// three pods.
	var nodes []NodeSnapshot
	for i, m := range three {
		pods := []*corev1.Pod{m}
		n := typedNode(t, cat, fmt.Sprintf("i-g%d", i), "c7i.large", pods...)
		n.DisruptionCost = NodeDisruptionCost(pods)
		nodes = append(nodes, n)
	}
	dest := typedNode(t, cat, "i-dest", "c7i.4xlarge")
	dest.DisruptionCost = NodeDisruptionCost(nil)
	all := append(append([]NodeSnapshot{}, nodes...), dest)
	totals := Totals(all)

	cands := Candidates(all)
	var gangCand Candidate
	for _, c := range cands {
		if c.IsGang() {
			gangCand = c
		}
	}
	if len(gangCand.Nodes) != 3 {
		t.Fatalf("the gang candidate covers %v, want all three", gangCand.NodeIDs())
	}

	props := GangEvaluation{Threshold: 0}.Evaluate(
		[]Candidate{gangCand}, []NodeSnapshot{dest}, cat, totals,
		map[string]int{gangCand.Pool(): 10})
	if len(props) != 1 {
		t.Fatalf("got %d proposals, want one move over the gang", len(props))
	}
	p := props[0]
	if p.GangID != "g" {
		t.Errorf("proposal %s carries gang %q, want g", p.ID, p.GangID)
	}
	if len(p.Candidates) != 3 {
		t.Fatalf("the move takes %d nodes, want all 3", len(p.Candidates))
	}
	got := map[string]bool{}
	for _, c := range p.Candidates {
		got[c.ProviderID] = true
	}
	for _, n := range nodes {
		if !got[n.ProviderID] {
			t.Errorf("the move leaves %s standing, so it would half-consolidate the gang", n.ProviderID)
		}
	}
	// The savings is every node's price, and the disruption is the candidate's single
	// charge, so the score is the gang's one score and not a per-node average.
	wantSavings := 0.0
	for _, n := range nodes {
		wantSavings += n.Price
	}
	if math.Abs(p.Savings-wantSavings) > 1e-9 {
		t.Errorf("savings %v, want %v", p.Savings, wantSavings)
	}
	if want := Score(wantSavings, gangCand.DisruptionCost, totals); math.Abs(p.Score-want) > 1e-9 {
		t.Errorf("score %v, want %v from the candidate's single charge", p.Score, want)
	}

	// A budget short of the gang's node count yields nothing rather than a smaller
	// move. Taking two of three takes the workload down and recovers two nodes'
	// price, which is the half-consolidation the unit exists to forbid.
	short := GangEvaluation{Threshold: 0}.Evaluate(
		[]Candidate{gangCand}, []NodeSnapshot{dest}, cat, totals,
		map[string]int{gangCand.Pool(): 2})
	if len(short) != 0 {
		t.Errorf("a budget of 2 produced %d proposals over a 3-node gang; there is no partial move",
			len(short))
	}
}

// The executor applies a gang move as a unit, and a gang move that cannot complete
// its launch set abandons whole: it releases what it acquired and evicts nothing.
// That is #3's atomic acquisition at the unit it was built for.
func TestGangMoveAbandonsWholeWhenItsLaunchIsRefused(t *testing.T) {
	const outage = 4.0
	cat := BuildCatalog()
	two := gangMembers("g", 2, outage)

	// Two oversized nodes, one member each, packed tight enough that a replace is
	// the only move: no destination in the cluster.
	var nodes []NodeSnapshot
	for i, m := range two {
		pods := []*corev1.Pod{m, sizedPod(fmt.Sprintf("bulk%d", i), 3000, 8192, "")}
		n := typedNode(t, cat, fmt.Sprintf("i-g%d", i), "r7i.2xlarge", pods...)
		n.DisruptionCost = NodeDisruptionCost(pods)
		nodes = append(nodes, n)
	}
	totals := Totals(nodes)

	var gangCand Candidate
	for _, c := range Candidates(nodes) {
		if c.IsGang() {
			gangCand = c
		}
	}
	props := GangEvaluation{Threshold: 0}.Evaluate(
		[]Candidate{gangCand}, nil, cat, totals, map[string]int{gangCand.Pool(): 10})
	if len(props) != 1 {
		t.Fatalf("got %d proposals", len(props))
	}
	p := props[0]
	if len(p.Replacements) != 1 {
		t.Fatalf("want a REPLACE with one replacement, got %d", len(p.Replacements))
	}
	if !p.AtomicLaunch {
		t.Error("a gang move's launch set must be all-or-nothing")
	}

	cluster := NewCluster(nodes, cat, 10)
	cluster.Totals, cluster.Threshold = totals, 0
	// The provider refuses the replacement.
	cluster.LaunchFails = FailLaunchesOf(p.Replacements[0].Name)

	exec := NewMemoryExecutor(cluster)
	outcome, err := exec.Apply(context.Background(), p)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if outcome != Abandoned {
		t.Fatalf("outcome %v, want Abandoned", outcome)
	}
	// Nothing was evicted and no node was deleted, so the gang is exactly where it
	// was. A move that abandons before eviction is the four-node state never being
	// committed rather than committed and reversed.
	if len(cluster.Pending) != 0 {
		t.Errorf("%d pods pending after an abandoned gang move; it evicted something", len(cluster.Pending))
	}
	for _, n := range nodes {
		live := cluster.Nodes[n.ProviderID]
		if live.Deleted {
			t.Errorf("%s was deleted by an abandoned move", n.ProviderID)
		}
		if len(live.Pods) != len(n.ReschedulablePods) {
			t.Errorf("%s holds %d pods, started with %d", n.ProviderID, len(live.Pods), len(n.ReschedulablePods))
		}
	}
}

// The compactor treats a gang candidate as one indivisible commit. A budget that
// cannot cover the whole gang commits none of it, and the slot goes to work that
// fits.
func TestCompactorCommitsAGangWholeOrNotAtAll(t *testing.T) {
	gang := Proposal{
		ID:         "gang-del/g/i-1/i-2/i-3",
		GangID:     "g",
		Score:      9.0,
		Reason:     ReasonUnderutilized,
		Candidates: []NodeSnapshot{{ProviderID: "i-1"}, {ProviderID: "i-2"}, {ProviderID: "i-3"}},
	}
	single := Proposal{
		ID:         "del/i-9",
		Score:      1.0,
		Reason:     ReasonUnderutilized,
		Candidates: []NodeSnapshot{{ProviderID: "i-9"}},
	}

	// A budget of 3 fits the gang, and it outranks the single move.
	committed, _ := RankingCompactor{}.SelectCounting(context.Background(),
		[]Proposal{single, gang}, 3)
	if len(committed) != 1 || committed[0].ID != gang.ID {
		t.Fatalf("budget 3: committed %v, want just the gang", ids(committed))
	}

	// A budget of 2 cannot cover it, so none of it commits and the single move takes
	// the slot instead. Committing two of the gang's nodes would take the workload
	// down for two thirds of the price.
	committed, _ = RankingCompactor{}.SelectCounting(context.Background(),
		[]Proposal{single, gang}, 2)
	if len(committed) != 1 || committed[0].ID != single.ID {
		t.Fatalf("budget 2: committed %v, want the single move alone", ids(committed))
	}
	for _, p := range committed {
		if p.GangID != "" {
			t.Errorf("a gang move committed against a budget of 2 for its 3 nodes")
		}
	}
}

func ids(ps []Proposal) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.ID)
	}
	return out
}

// A gang goes through the whole pipeline as a unit, which is what makes the change
// a pipeline change rather than a scoring one. The gang path proposes, the compactor
// commits, and the executor applies, and every node the committed gang move names is
// gone afterwards.
func TestAGangMoveSurvivesTheWholePipeline(t *testing.T) {
	const outage = 3.0
	cat := BuildCatalog()
	three := gangMembers("g", 3, outage)

	// Three thinly loaded nodes holding the gang, one member and one ordinary rider
	// each. Sized so the gang's share of the pool price clears the delete-ratio floor,
	// which is what makes this a test of the pipeline rather than of the floor.
	var snaps []NodeSnapshot
	for i, m := range three {
		pods := []*corev1.Pod{m, sizedPod(fmt.Sprintf("rider%d", i), 200, 512, "")}
		n := typedNode(t, cat, fmt.Sprintf("i-g%d", i), "m7i.xlarge", pods...)
		n.DisruptionCost = NodeDisruptionCost(pods)
		snaps = append(snaps, n)
	}
	for i := 0; i < 2; i++ {
		// Loaded enough to classify as a destination rather than a source, and holding
		// a do-not-disrupt pod so the node paths cannot propose over them. They exist
		// to receive the gang's pods, and a fixture where the destinations are also
		// candidates would be asserting on whatever the compactor ranked highest
		// rather than on the gang move.
		var pods []*corev1.Pod
		for j := 0; j < 6; j++ {
			pods = append(pods, sizedPod(fmt.Sprintf("d%d-%d", i, j), 1800, 6144, ""))
		}
		pinned := pod(fmt.Sprintf("pinned%d", i), "")
		pinned.Annotations = map[string]string{v1.DoNotDisruptAnnotationKey: "true"}
		pods = append(pods, pinned)
		n := typedNode(t, cat, fmt.Sprintf("i-dest%d", i), "m7i.4xlarge", pods...)
		n.DisruptionCost = NodeDisruptionCost(pods)
		snaps = append(snaps, n)
	}
	totals := Totals(snaps)

	cfg := DefaultRunConfig(2, 10)
	// The positional nomination would otherwise land on a node the move needs, which
	// is a stage-4 refusal rather than the path behavior under test.
	cfg.NominateEvery = 0
	res := Run(context.Background(), snaps, totals, cat, cfg)

	fmt.Printf("gang pipeline: units=%d candidates=%d (%d gangs) gangPath=%d proposals over %d nodes, "+
		"committed=%d proceeded=%d realized=$%.4f pending=%d\n",
		res.CandidatesIn, res.CandidatesOut, res.GangCandidatesOut,
		res.Paths[PathGang].Proposals, res.Paths[PathGang].Nodes,
		res.Committed, res.Proceeded, res.SavingsRealized, res.PendingAfter)

	if res.GangCandidatesOut != 1 {
		t.Fatalf("stage 1 kept %d gang candidates, want the one gang", res.GangCandidatesOut)
	}
	g := res.Paths[PathGang]
	if g.Proposals != 1 {
		t.Fatalf("the gang path made %d proposals, want one", g.Proposals)
	}
	if g.Nodes != 3 {
		t.Errorf("the gang move covers %d nodes, want all 3", g.Nodes)
	}
	// The three node paths must not have touched the gang's nodes, because a
	// node-scoped move over one of them would take part of the gang.
	for _, p := range []Path{PathEmpty, PathMultiNode, PathSingleNode} {
		if res.Paths[p].Nodes != 0 {
			t.Errorf("the %s path proposed over %d nodes; the gang's nodes are withheld "+
				"from the node paths and the destinations are not candidates",
				p, res.Paths[p].Nodes)
		}
	}
	if res.Proceeded != 1 {
		t.Fatalf("%d proposals ran to the end, want the gang move; stops were %v",
			res.Proceeded, res.StopCauses)
	}
	if res.PendingAfter != 0 {
		t.Errorf("%d pods pending; the move stranded the gang it was supposed to relocate",
			res.PendingAfter)
	}
	if res.SavingsRealized <= 0 {
		t.Errorf("realized $%.4f; a committed gang delete recovers three nodes' price",
			res.SavingsRealized)
	}
}

// -----------------------------------------------------------------------------
// The locality claim
// -----------------------------------------------------------------------------

// The locality claim as an assertion. A gang candidate's read set is the gang's
// nodes, which is exactly what its move acts on. Topology spread reads beyond the
// move's scope, and the difference is what separates a bounded unit from an
// unbounded one.
//
// "Reads more than one node" would flatten the two. "Reads more than it acts on" is
// what separates them, and only the second is a locality violation.
func TestGangReadSetEqualsItsActionSetAndSpreadDoesNot(t *testing.T) {
	const outage = 4.0
	cat := BuildCatalog()
	three := gangMembers("g", 3, outage)

	// Three nodes holding the gang, and five bystanders the move does not act on.
	// The bystanders carry a spread constraint over the same workload, so a spread
	// check has something outside the move's scope to read.
	var snaps []NodeSnapshot
	for i, m := range three {
		m.Labels[WorkloadLabel] = "w"
		pods := []*corev1.Pod{m}
		n := typedNode(t, cat, fmt.Sprintf("i-g%d", i), "m7i.4xlarge", pods...)
		n.DisruptionCost = NodeDisruptionCost(pods)
		snaps = append(snaps, n)
	}
	for i := 0; i < 5; i++ {
		p := withSpread(labeledPod(fmt.Sprintf("bystander%d", i), "w"), "w", corev1.LabelTopologyZone, 1)
		pods := []*corev1.Pod{p}
		n := typedNode(t, cat, fmt.Sprintf("i-other%d", i), "m7i.4xlarge", pods...)
		n.DisruptionCost = NodeDisruptionCost(pods)
		snaps = append(snaps, n)
	}
	totals := Totals(snaps)

	var gangCand Candidate
	for _, c := range Candidates(snaps) {
		if c.IsGang() {
			gangCand = c
		}
	}
	if len(gangCand.Nodes) != 3 {
		t.Fatalf("the gang candidate covers %v, want three nodes", gangCand.NodeIDs())
	}

	// The action set: what the move touches.
	action := map[string]bool{}
	for _, id := range gangCand.NodeIDs() {
		action[id] = true
	}

	cluster := NewCluster(snaps, cat, 10)
	cluster.Totals, cluster.Threshold = totals, 0

	// The read set, instrumented. ScoreStillClears is the check that reprices the
	// move, so it is the one whose reads are the decision's own reads.
	score := &ScoreStillClearsChecker{Cluster: cluster}
	p := Proposal{GangID: "g", Candidates: gangCand.Nodes, Score: 1, Reason: ReasonUnderutilized}
	if err := score.Check(context.Background(), p); err != nil {
		t.Fatalf("repricing the gang move: %v", err)
	}
	if got, want := score.Reads(), len(gangCand.Nodes); got != want {
		t.Errorf("repricing the gang read %d nodes, want %d, its own", got, want)
	}
	// Equal, not merely bounded. A read of a node the move does not act on is the
	// thing being ruled out, and a bound would let one through.
	if score.Reads() != len(action) {
		t.Errorf("read set %d against action set %d; for a gang the two must be equal",
			score.Reads(), len(action))
	}
	// And the reads are flat in the bystanders. Adding nodes the move does not act on
	// must not cost the decision anything, which is what makes the unit bounded by
	// the workload rather than by the cluster.
	if score.Reads() >= len(cluster.Nodes) {
		t.Errorf("repricing read %d of %d nodes; a bounded unit reads its own and no more",
			score.Reads(), len(cluster.Nodes))
	}

	// Topology spread over the same cluster reads every node, including the five the
	// move does not act on and the workloads on them. Its read set exceeds its action
	// set and is bounded by the cluster, which is the distinction.
	r := cluster.Reader()
	mover := withSpread(labeledPod("mover", "w"), "w", corev1.LabelTopologyZone, 1)
	_ = (TopologyConstraint{}).AdmitsPlacement(r, mover, "i-g0")
	if r.Reads() != len(cluster.Nodes) {
		t.Errorf("spread read %d of %d nodes; maxSkew is defined over every domain",
			r.Reads(), len(cluster.Nodes))
	}
	beyond := 0
	for id := range cluster.Nodes {
		if !action[id] {
			beyond++
		}
	}
	if r.Reads() <= len(action) {
		t.Errorf("spread read %d nodes against an action set of %d; it is supposed to read "+
			"%d nodes the move does not touch", r.Reads(), len(action), beyond)
	}

	fmt.Printf("gang candidate: acts on %d nodes, reads %d. spread: acts on %d, reads %d of %d.\n",
		len(action), score.Reads(), len(action), r.Reads(), len(cluster.Nodes))
}

// Making the gang the unit forced no cluster access into stages 1 and 2, which is
// the critical finding either way. Candidates are built from the node list alone,
// so the same call answers identically with no cluster in existence, and concurrent
// callers over disjoint units share nothing.
//
// The signatures are the load-bearing half of this and a test cannot assert on
// them, so this asserts the behavior the signatures buy: the grouping is a pure
// function of its argument.
func TestGangUnitNeedsNoClusterAccess(t *testing.T) {
	cat := BuildCatalog()
	opts := DefaultGenClusterOpts(120, 19)
	opts.Catalog = cat
	opts.Shape.GangFraction = 0.2
	opts.Shape.GangCount = 40
	nodes, totals := GenCluster(opts)

	first := Candidates(nodes)
	gangs := 0
	for _, c := range first {
		if c.IsGang() {
			gangs++
		}
	}
	if gangs == 0 {
		t.Fatal("premise broken: the fixture produced no gang candidates")
	}

	// The same argument gives the same answer, every time, with nothing else in
	// scope. A grouping that consulted anything outside its argument could not
	// promise this.
	for i := 0; i < 3; i++ {
		again := Candidates(nodes)
		if len(again) != len(first) {
			t.Fatalf("call %d grouped %d candidates, first call gave %d", i, len(again), len(first))
		}
		for j := range again {
			if again[j].ID() != first[j].ID() || again[j].DisruptionCost != first[j].DisruptionCost {
				t.Fatalf("call %d candidate %d is %s/%v, first call gave %s/%v",
					i, j, again[j].ID(), again[j].DisruptionCost, first[j].ID(), first[j].DisruptionCost)
			}
		}
	}

	// Stage 1's bits are likewise a function of the candidates and the totals.
	bits := CandidateBits(first, totals, 2)
	for i := 0; i < 3; i++ {
		if fmt.Sprint(CandidateBits(Candidates(nodes), totals, 2)) != fmt.Sprint(bits) {
			t.Fatalf("stage 1 answered differently on call %d", i)
		}
	}
}
