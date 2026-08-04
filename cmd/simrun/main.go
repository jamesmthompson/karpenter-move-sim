// Command simrun runs one pass of the four-stage pipeline over a generated
// cluster and reports what each stage did.
//
// The number the paper needs first is stage 1's reduction: candidates over nodes,
// which the report prints as a ratio because that is the form the claim takes.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/jamesmthompson/karpenter-move-sim"
)

func main() {
	nodes := flag.Int("nodes", 200, "number of nodes to generate")
	seed := flag.Int64("seed", 1, "generator seed")
	k := flag.Float64("k", 2.0, "stage 1 threshold denominator: keep a node whose delete ratio is at least 1/k")
	threshold := flag.Float64("threshold", -1, "stage 2 approval floor on Score; defaults to 1/k")
	budget := flag.Int("budget", 20, "nodes disruptable this pass")
	sourceCeiling := flag.Float64("source-ceiling", 0.50, "top of the Underutilized band")
	destFloor := flag.Float64("destination-floor", 0.60, "bottom of the Destination band")
	fullCeiling := flag.Float64("full-ceiling", 0.95, "utilization at which no further pod fits")
	placement := flag.String("placement", string(sim.PlacementFirstFit),
		"where evicted pods land: first-fit, least-allocated (spread), or most-allocated (pack)")
	csv := flag.Bool("csv", false, "emit one CSV row instead of a report")
	header := flag.Bool("csv-header", false, "print the CSV header and exit")
	flag.Parse()

	if *header {
		fmt.Println(csvHeader)
		return
	}

	cfg := sim.DefaultRunConfig(*k, *budget)
	if *threshold >= 0 {
		cfg.Threshold = *threshold
	}
	cfg.Placement = sim.PlacementStrategyName(*placement)
	// Reject an unknown name here rather than letting the pipeline panic on it. A
	// typo in a sweep script is a usage error and should read like one.
	if _, err := cfg.Placement.Strategy(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	cfg.Bands = sim.BandConfig{
		SourceCeiling:    *sourceCeiling,
		DestinationFloor: *destFloor,
		FullCeiling:      *fullCeiling,
	}

	cat := sim.BuildCatalog()
	opts := sim.DefaultGenClusterOpts(*nodes, *seed)
	opts.Catalog = cat
	snaps, totals := sim.GenCluster(opts)

	res := sim.Run(context.Background(), snaps, totals, cat, cfg)

	if *csv {
		fmt.Println(csvRow(*nodes, *seed, *k, cfg, res))
		return
	}
	if _, err := os.Stdout.WriteString(report(*seed, *k, cfg, res)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func report(seed int64, k float64, cfg sim.RunConfig, r sim.Result) string {
	var b strings.Builder
	f := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }

	f("seed=%d k=%.2f threshold=%.3f budget=%d bands=%.2f/%.2f/%.2f placement=%s\n",
		seed, k, cfg.Threshold, cfg.Budget,
		cfg.Bands.SourceCeiling, cfg.Bands.DestinationFloor, cfg.Bands.FullCeiling,
		cfg.Placement)
	f("\n")
	// Units and not nodes. A gang is one candidate however many nodes hold its
	// members, so the denominator is the units the fleet grouped into.
	f("stage 1  identify     %5d nodes -> %d units -> %d candidates (%.1f%%, %d gangs)   %s\n",
		r.NodesIn, r.CandidatesIn, r.CandidatesOut,
		pct(r.CandidatesOut, r.CandidatesIn), r.GangCandidatesOut, dur(r.IdentifyNanos))
	f("stage 2  generate     %5d destinations, %d proposals        %s\n",
		r.Destinations, r.ProposalsGenerated, dur(r.GenerateNanos))
	// Per path, in the order upstream runs them. A total cannot say which of the
	// three produced it, and the interaction between them is what decides an
	// outcome: emptiness claims the empty nodes, multi-node claims its batch
	// before single-node walks.
	for _, p := range []sim.Path{sim.PathGang, sim.PathEmpty, sim.PathMultiNode, sim.PathSingleNode} {
		s := r.Paths[p]
		f("         %-12s %5d candidates, %d proposals over %d nodes, $%.4f/hr",
			p, s.Candidates, s.Proposals, s.Nodes, s.Savings)
		if p == sim.PathMultiNode {
			f("  (%d probes, batch %d)", s.Probes, s.BatchSize)
		}
		f("\n")
	}
	f("stage 3  compact      %5d committed, %d conflicts dropped   %s\n",
		r.Committed, r.Conflicts, dur(r.CompactNanos))
	f("stage 4  execute      %5d applied: %d proceed, %d stopped early  %s\n",
		r.Applied, r.Proceeded, r.Stopped, dur(r.ExecuteNanos))
	// Printed only when it happened. No generator sets AtomicLaunch, so a pass
	// abandons nothing, and a column of zeros in every report would be noise. The
	// line appearing at all means an all-or-nothing launch set could not be
	// completed.
	if r.Abandoned > 0 {
		f("         %5d abandoned: %d launches released\n", r.Abandoned, r.LaunchesReleased)
	}
	// Which precondition stopped them, because a pass stopping on capacity and a
	// pass stopping on do-not-disrupt otherwise print the same.
	for _, kind := range r.StopCausesSorted() {
		f("         stopped by %-18s %d\n", kind, r.StopCauses[kind])
	}
	f("\n")
	f("savings proposed  $%8.4f/hr\n", r.SavingsProposed)
	f("savings realized  $%8.4f/hr  (%.1f%% of proposed)\n",
		r.SavingsRealized, ratioPct(r.SavingsRealized, r.SavingsProposed))
	f("pool cost         $%8.4f/hr -> $%.4f/hr\n", r.PoolCostBefore, r.PoolCostAfter)
	f("stopped work       %d actions applied inside stopped proposals\n", r.StoppedActions)
	f("pods stranded      %d\n", r.PendingAfter)
	f("budget left        %d of %d\n", r.BudgetLeft, cfg.Budget)
	return b.String()
}

// stopped_score is broken out of the cause histogram because it is the column a
// sweep is about: how often a committed move had stopped paying by the time it
// reached the executor.
// The per-path columns are what let a sweep say which of the three produced a
// savings figure. multi_batch and multi_probes are the binary search's own
// numbers, which no aggregate can recover.
// placement is a column because two rows with the same seed and different
// placement are different runs, and a sweep that omits it cannot tell them apart.
const csvHeader = "nodes,seed,k,threshold,budget,placement,candidates,destinations,proposals," +
	"committed,conflicts,applied,proceeded,stopped,stopped_actions,stopped_score," +
	"savings_proposed,savings_realized,cost_before,cost_after," +
	"pending,budget_left,identify_ns,generate_ns,compact_ns,execute_ns," +
	"empty_props,empty_nodes,empty_savings," +
	"multi_props,multi_nodes,multi_savings,multi_probes,multi_batch," +
	"single_props,single_nodes,single_savings"

func csvRow(nodes int, seed int64, k float64, cfg sim.RunConfig, r sim.Result) string {
	e, m, s := r.Paths[sim.PathEmpty], r.Paths[sim.PathMultiNode], r.Paths[sim.PathSingleNode]
	return fmt.Sprintf("%d,%d,%g,%g,%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.6f,%.6f,%.6f,%.6f,%d,%d,%d,%d,%d,%d"+
		",%d,%d,%.6f,%d,%d,%.6f,%d,%d,%d,%d,%.6f",
		nodes, seed, k, cfg.Threshold, cfg.Budget, cfg.Placement,
		r.CandidatesOut, r.Destinations, r.ProposalsGenerated,
		r.Committed, r.Conflicts, r.Applied, r.Proceeded,
		r.Stopped, r.StoppedActions, r.StopCauses[sim.ScoreStillClears],
		r.SavingsProposed, r.SavingsRealized, r.PoolCostBefore, r.PoolCostAfter,
		r.PendingAfter, r.BudgetLeft,
		r.IdentifyNanos, r.GenerateNanos, r.CompactNanos, r.ExecuteNanos,
		e.Proposals, e.Nodes, e.Savings,
		m.Proposals, m.Nodes, m.Savings, m.Probes, m.BatchSize,
		s.Proposals, s.Nodes, s.Savings)
}

func pct(n, d int) float64 {
	if d == 0 {
		return 0
	}
	return 100 * float64(n) / float64(d)
}

func ratioPct(n, d float64) float64 {
	if d == 0 {
		return 0
	}
	return 100 * n / d
}

// dur prints microseconds, which is the scale a stage takes over a few hundred
// nodes. Milliseconds would round every stage to zero.
func dur(ns int64) string {
	return fmt.Sprintf("%8.0fus", float64(ns)/1000)
}
