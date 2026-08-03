package sim

import (
	"context"
	"fmt"
	"math"
	"testing"
)

// The REPLACE path had no coverage, and an audit found why: 4632 REPLACE
// proposals generated across 75 runs of the default fixture and zero committed.
// Every test that touched consolidation used the loose default fixture, where a
// node's pods always fit somewhere else, so a DELETE was always feasible. The two
// proposals share a candidate, a DELETE recovers the full node price where a
// REPLACE recovers only the difference, so the compactor's overlap check dropped
// the REPLACE every time.
//
// These tests run the tight fixture, where a REPLACE is the only move, and follow
// it through to a realized saving. TestLooseClusterPrefersDelete keeps the loose
// behavior documented rather than quietly changed.

const tightNodeCount = 12

// tightRun is the fixture plus the config every test here uses, so a change to
// one of them moves all of them together.
func tightRun(t *testing.T) ([]NodeSnapshot, PoolTotals, *Catalog, RunConfig) {
	t.Helper()
	cat := BuildCatalog()
	nodes, totals := GenTightCluster(TightClusterOpts{NodeCount: tightNodeCount, Seed: 3, Catalog: cat})
	return nodes, totals, cat, DefaultRunConfig(2, 20)
}

// nodeByID returns the snapshot with this provider ID. GenTightCluster shuffles,
// so the oversized node's index is not fixed.
func nodeByID(t *testing.T, nodes []NodeSnapshot, providerID string) NodeSnapshot {
	t.Helper()
	for _, n := range nodes {
		if n.ProviderID == providerID {
			return n
		}
	}
	t.Fatalf("node %s is not in the fixture", providerID)
	return NodeSnapshot{}
}

// generateFor runs stages 1 and 2 for one node the way Run does.
func generateFor(nodes []NodeSnapshot, totals PoolTotals, cat *Catalog, cfg RunConfig, providerID string) []Proposal {
	dests := EligibleDestinations(nodes, cat, totals, cfg.Bands)
	gen := ConsolidationGenerator{Threshold: cfg.Threshold}
	bits := Identify(nodes, totals, cfg.K)
	for i, n := range nodes {
		if n.ProviderID != providerID || !bits[i] {
			continue
		}
		return gen.Generate(n, withoutNode(dests, providerID), cat, totals)
	}
	return nil
}

// splitByKind separates a proposal set into the DELETE and the REPLACE. A pure
// DELETE launches nothing, which is what Replacements being empty means.
func splitByKind(props []Proposal) (deletes, replaces []Proposal) {
	for _, p := range props {
		if len(p.Replacements) > 0 {
			replaces = append(replaces, p)
		} else {
			deletes = append(deletes, p)
		}
	}
	return deletes, replaces
}

// TestTightClusterYieldsReplaceNotDelete is the fixture's premise as an
// assertion. The oversized node's pods do not fit on the rest of the cluster, so
// aggregateRoom fails and deleteOnto declines. A cheaper instance type holds
// them, so replaceWith does not.
func TestTightClusterYieldsReplaceNotDelete(t *testing.T) {
	nodes, totals, cat, cfg := tightRun(t)
	over := nodeByID(t, nodes, OversizedProviderID)

	// The arithmetic behind the premise, printed so a catalog change that breaks
	// the fixture says which number moved.
	rest := withoutNode(nodes, OversizedProviderID)
	required := Requests(over.ReschedulablePods)
	cheaper, cheaperPrice, found := cat.Cheapest(cat.Fits(required))
	if !found {
		t.Fatal("no instance type in the catalog holds the oversized node's pods")
	}
	fmt.Printf("oversized: %s at $%.4f, utilization %.3f, %d pods, delete ratio %.3f\n",
		over.InstanceType.Name, over.Price, Utilization(over, cat),
		len(over.ReschedulablePods), DeleteRatio(over, totals))
	fmt.Printf("cheapest fit: %s at $%.4f, so a REPLACE saves $%.4f/hr\n",
		cheaper.Name, cheaperPrice, over.Price-cheaperPrice)

	// A known divergence this fixture makes visible and does not resolve. Upstream
	// ranks candidates by Price / RescheduleDisruptionCost, always the node's full
	// price whatever move follows. The simulator ranks proposals by Score, which
	// for a REPLACE uses the price delta. On this node that is $0.5292 against
	// $0.1724, so the two rankings differ by a factor of three. Printed rather than
	// asserted, because the fixture has one node that generates anything and the
	// divergence therefore cannot reorder it here.
	fmt.Printf("ranking divergence: upstream %.5f, sim unnormalized %.5f, %.2fx\n",
		over.Price/over.DisruptionCost,
		(over.Price-cheaperPrice)/over.DisruptionCost,
		over.Price/(over.Price-cheaperPrice))

	if aggregateRoom(rest, cat, required) {
		t.Fatal("the rest of the cluster has aggregate room, so a DELETE is feasible and the fixture is not tight")
	}
	if cheaperPrice >= over.Price {
		t.Fatalf("cheapest fit $%.4f is not below the node's $%.4f, so no REPLACE exists", cheaperPrice, over.Price)
	}

	deletes, replaces := splitByKind(generateFor(nodes, totals, cat, cfg, OversizedProviderID))
	if len(deletes) != 0 {
		t.Errorf("want no DELETE for the oversized node, got %d", len(deletes))
	}
	if len(replaces) != 1 {
		t.Fatalf("want exactly one REPLACE, got %d", len(replaces))
	}
	if got := replaces[0].Replacements[0].Name; got != cheaper.Name {
		t.Errorf("replacement type: got %s want %s", got, cheaper.Name)
	}
	// Savings is the price difference and not the full node price. A REPLACE that
	// claimed the full price would outrank a DELETE it should lose to.
	if want := over.Price - cheaperPrice; replaces[0].Savings != want {
		t.Errorf("savings: got %.4f want %.4f", replaces[0].Savings, want)
	}
}

// TestReplaceCommitsAndRealizesSavings is the assertion that would have caught
// 0-of-4632. Generation is not the claim. The claim is that the move survives the
// compactor, applies without rolling back, strands no pods, and leaves the pool
// measurably cheaper.
func TestReplaceCommitsAndRealizesSavings(t *testing.T) {
	nodes, totals, cat, cfg := tightRun(t)
	res := Run(context.Background(), nodes, totals, cat, cfg)

	fmt.Printf("tight run: proposals=%d committed=%d proceeded=%d rolled=%d forward=%d\n",
		res.ProposalsGenerated, res.Committed, res.Proceeded, res.RolledBack, res.Forward)
	fmt.Printf("pool cost $%.4f -> $%.4f, realized $%.4f/hr\n",
		res.PoolCostBefore, res.PoolCostAfter, res.SavingsRealized)

	if res.Committed != 1 {
		t.Fatalf("want one committed proposal, got %d", res.Committed)
	}
	if res.Proceeded != 1 {
		t.Errorf("want the REPLACE to proceed, got proceeded=%d rolled=%d forward=%d",
			res.Proceeded, res.RolledBack, res.Forward)
	}
	// Realized savings is measured from the cluster, so it charges for the node
	// the REPLACE launched. Proposed savings is what stage 2 claimed. A REPLACE
	// that launches what it planned to delivers exactly its claim.
	if res.SavingsRealized <= 0 {
		t.Errorf("realized savings %.4f; a committed REPLACE that saves nothing has not replaced anything",
			res.SavingsRealized)
	}
	// Compared with a tolerance because Cluster.Cost sums over a map, so summation
	// order varies and the last bits of the mantissa move.
	if math.Abs(res.SavingsRealized-res.SavingsProposed) > 1e-9 {
		t.Errorf("realized %.12f does not match proposed %.12f", res.SavingsRealized, res.SavingsProposed)
	}
	// A pass that realizes savings by leaving pods homeless has not saved
	// anything. In a tight cluster this is the failure mode to worry about.
	if res.PendingAfter != 0 {
		t.Errorf("the move stranded %d pods", res.PendingAfter)
	}
}

// TestReplaceActionDAGOrders pins the ordering the REPLACE path exists to
// enforce. The replacement must be up before any pod leaves the source, or the
// evicted pods have nowhere to land in a cluster that by construction has no
// slack. The executor only enforces the edges the proposal declares, so a missing
// edge is a silent loss of that guarantee.
func TestReplaceActionDAGOrders(t *testing.T) {
	nodes, totals, cat, cfg := tightRun(t)
	_, replaces := splitByKind(generateFor(nodes, totals, cat, cfg, OversizedProviderID))
	if len(replaces) != 1 {
		t.Fatalf("want one REPLACE, got %d", len(replaces))
	}
	actions := replaces[0].Actions

	launch, delete, evicts := -1, -1, 0
	for i, a := range actions {
		switch a.Kind {
		case Launch:
			if launch >= 0 {
				t.Fatalf("a single-node REPLACE launched twice, at %d and %d", launch, i)
			}
			launch = i
		case Delete:
			delete = i
		case Evict:
			evicts++
			if launch < 0 {
				t.Errorf("Evict at %d fires before the Launch", i)
			}
			// Position is not the guarantee. The executor releases an action once
			// its declared dependencies confirm, so an Evict that sits after the
			// Launch but does not depend on it can still fire first.
			depends := false
			for _, dep := range a.DependsOn {
				if dep == launch {
					depends = true
				}
			}
			if !depends {
				t.Errorf("Evict at %d does not depend on the Launch at %d, so nothing holds it back",
					i, launch)
			}
		}
	}

	if launch < 0 {
		t.Fatal("a REPLACE with no Launch is a DELETE")
	}
	if delete != len(actions)-1 {
		t.Errorf("Delete is at %d of %d actions; it must be last", delete, len(actions))
	}
	if evicts != len(nodeByID(t, nodes, OversizedProviderID).ReschedulablePods) {
		t.Errorf("got %d Evict actions for %d pods", evicts,
			len(nodeByID(t, nodes, OversizedProviderID).ReschedulablePods))
	}
	// The Delete must wait on every Evict. A Delete that fires early strands
	// whatever pods are still on the node.
	deps := map[int]bool{}
	for _, dep := range actions[delete].DependsOn {
		deps[dep] = true
	}
	for i, a := range actions {
		if a.Kind == Evict && !deps[i] {
			t.Errorf("Delete does not depend on the Evict at %d", i)
		}
	}
}

// A loose cluster gives every candidate a delete, so the generator never reaches
// replaceWith. That is the point of preferring delete: when the pods have
// somewhere to go, removing the node outright saves more than swapping it, and
// generating the losing alternative is dead work.
//
// This pins the preference by calling both halves directly, since Generate now
// returns after the delete.
func TestLooseClusterPrefersDelete(t *testing.T) {
	cat := BuildCatalog()
	nodes, totals := GenCluster(GenClusterOpts{NodeCount: 200, Seed: 7, Catalog: cat})
	cfg := DefaultRunConfig(2, 20)
	gen := ConsolidationGenerator{Threshold: cfg.Threshold}
	dests := EligibleDestinations(nodes, cat, totals, cfg.Bands)

	// Find a node where both moves are individually available, then confirm the
	// generator emits only the delete.
	checked := 0
	for _, n := range nodes {
		if !Disruptable(n) || DeleteRatio(n, totals) < cfg.Threshold {
			continue
		}
		del, delOK := gen.deleteOnto(n, withoutNode(dests, n.ProviderID), cat, totals)
		rep, repOK := gen.replaceWith(n, cat, totals)
		if !delOK || !repOK {
			continue
		}
		checked++

		if del.Score <= rep.Score {
			t.Errorf("node %s: DELETE score %.4f should exceed REPLACE %.4f; a DELETE recovers the full node price",
				n.ProviderID, del.Score, rep.Score)
		}
		if del.Savings <= rep.Savings {
			t.Errorf("node %s: DELETE saves $%.4f, REPLACE $%.4f", n.ProviderID, del.Savings, rep.Savings)
		}

		got := gen.Generate(n, withoutNode(dests, n.ProviderID), cat, totals)
		if len(got) != 1 {
			t.Fatalf("node %s: want one proposal, got %d", n.ProviderID, len(got))
		}
		if len(got[0].Replacements) != 0 {
			t.Errorf("node %s: generator emitted a REPLACE when a DELETE was available", n.ProviderID)
		}
		if checked == 1 {
			fmt.Printf("loose node %s: DELETE score %.4f saves $%.4f, REPLACE score %.4f saves $%.4f, generator emits DELETE\n",
				n.ProviderID, del.Score, del.Savings, rep.Score, rep.Savings)
		}
		if checked >= 20 {
			break
		}
	}
	if checked == 0 {
		t.Fatal("no node in the loose fixture had both moves available")
	}
}

// TestPodsStillFitCountsTheReplacement pins the checker fix the tight fixture
// exposed. PodsStillFit re-ran placement over standing nodes only, so a REPLACE
// was gated against a cluster missing the node it exists to buy. That fails
// exactly when a REPLACE is the only move available, which is why the loose
// fixture never showed it: there, a REPLACE always had a DELETE outranking it and
// never reached the executor.
func TestPodsStillFitCountsTheReplacement(t *testing.T) {
	nodes, totals, cat, cfg := tightRun(t)
	_, replaces := splitByKind(generateFor(nodes, totals, cat, cfg, OversizedProviderID))
	if len(replaces) != 1 {
		t.Fatalf("want one REPLACE, got %d", len(replaces))
	}
	p := replaces[0]

	cluster := NewCluster(nodes, cat, cfg.Budget)
	check := PodsStillFitChecker{Cluster: cluster}
	if err := check.Check(context.Background(), p); err != nil {
		t.Errorf("the check rejected a REPLACE whose own replacement holds the pods: %v", err)
	}

	// The same pods with the replacement taken away must fail, or the check passed
	// for a reason other than the replacement.
	bare := p
	bare.Replacements = nil
	if err := check.Check(context.Background(), bare); err == nil {
		t.Error("without the replacement the pods have nowhere to go, but the check passed")
	}

	// Once the Launch has fired the node is in the cluster, and counting it as a
	// plan as well would approve a move against capacity twice over.
	if _, err := cluster.Launch(p.Replacements[0]); err != nil {
		t.Fatalf("launch: %v", err)
	}
	cluster.LaunchesInFlight = 1
	if err := check.Check(context.Background(), p); err != nil {
		t.Errorf("after the launch the standing node should hold the pods: %v", err)
	}
	// Two of these pod sets do not fit one replacement, which is what catches a
	// double count.
	doubled := p
	doubled.Candidates = append(append([]NodeSnapshot{}, p.Candidates...), p.Candidates[0])
	if err := check.Check(context.Background(), doubled); err == nil {
		t.Error("the check approved twice the pods against one replacement")
	}
}

// TestTightFixtureGuardsItsPremises checks that the fixture refuses to build when
// it can no longer guarantee what it claims. Fillers each carry a little unused
// capacity, so a large enough node count adds up to room for the oversized node's
// pods, a DELETE becomes feasible, and every assertion above passes for the wrong
// reason. The constructor panics there rather than returning that cluster.
func TestTightFixtureGuardsItsPremises(t *testing.T) {
	// Find the boundary by construction rather than pinning a number, because the
	// number is a function of the catalog's capacities and the load constants.
	ceiling := 0
	for n := 2; n <= 200; n++ {
		if !buildsTight(n) {
			ceiling = n
			break
		}
	}
	if ceiling == 0 {
		t.Fatal("the fixture never stops being tight, so its guard is not reachable")
	}
	fmt.Printf("tight up to %d nodes, panics at %d\n", ceiling-1, ceiling)
	if ceiling <= 30 {
		t.Errorf("the fixture stops being tight at %d nodes, below the 10-to-30 range it documents", ceiling)
	}
	// One filler and the oversized node is the smallest thing the shape means.
	if buildsTight(1) {
		t.Error("a one-node cluster has no filler and should be refused")
	}
}

func buildsTight(nodeCount int) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	GenTightCluster(TightClusterOpts{NodeCount: nodeCount, Seed: 3})
	return true
}

// TestTightClusterDeterministic is the property every reported number rests on.
// The fixture is seeded, reads no clock, and its per-node loads do not move with
// the seed at all: only the pod counts and the node ordering do.
func TestTightClusterDeterministic(t *testing.T) {
	first, firstTotals := GenTightCluster(TightClusterOpts{NodeCount: tightNodeCount, Seed: 3})
	for i := 0; i < 3; i++ {
		got, totals := GenTightCluster(TightClusterOpts{NodeCount: tightNodeCount, Seed: 3})
		if totals != firstTotals {
			t.Fatalf("repeat %d: totals %+v want %+v", i, totals, firstTotals)
		}
		if len(got) != len(first) {
			t.Fatalf("repeat %d: %d nodes want %d", i, len(got), len(first))
		}
		for j := range got {
			if got[j].ProviderID != first[j].ProviderID ||
				got[j].Price != first[j].Price ||
				got[j].DisruptionCost != first[j].DisruptionCost ||
				len(got[j].ReschedulablePods) != len(first[j].ReschedulablePods) {
				t.Fatalf("repeat %d node %d diverged: %s vs %s", i, j,
					got[j].ProviderID, first[j].ProviderID)
			}
		}
	}
	// The oversized node's price and pod totals are what the fixture guarantees,
	// so they must not move with the seed even though the pod count may.
	for _, seed := range []int64{1, 2, 9, 41} {
		nodes, _ := GenTightCluster(TightClusterOpts{NodeCount: tightNodeCount, Seed: seed})
		over := nodeByID(t, nodes, OversizedProviderID)
		req := Requests(over.ReschedulablePods)
		cpu, mem := req.Cpu(), req.Memory()
		if cpu.MilliValue() != tightOversizedCPU {
			t.Errorf("seed %d: oversized CPU %dm want %dm", seed, cpu.MilliValue(), tightOversizedCPU)
		}
		if got := mem.Value() / (1 << 20); got != tightOversizedMiB {
			t.Errorf("seed %d: oversized memory %dMiB want %dMiB", seed, got, tightOversizedMiB)
		}
	}
}
