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

	f("seed=%d k=%.2f threshold=%.3f budget=%d bands=%.2f/%.2f/%.2f\n",
		seed, k, cfg.Threshold, cfg.Budget,
		cfg.Bands.SourceCeiling, cfg.Bands.DestinationFloor, cfg.Bands.FullCeiling)
	f("\n")
	f("stage 1  identify     %5d nodes -> %d candidates (%.1f%%)   %s\n",
		r.NodesIn, r.CandidatesOut, pct(r.CandidatesOut, r.NodesIn), dur(r.IdentifyNanos))
	f("stage 2  generate     %5d destinations, %d proposals        %s\n",
		r.Destinations, r.ProposalsGenerated, dur(r.GenerateNanos))
	f("stage 3  compact      %5d committed, %d conflicts dropped   %s\n",
		r.Committed, r.Conflicts, dur(r.CompactNanos))
	f("stage 4  execute      %5d applied: %d proceed, %d rollback, %d forward  %s\n",
		r.Applied, r.Proceeded, r.RolledBack, r.Forward, dur(r.ExecuteNanos))
	f("\n")
	f("savings proposed  $%8.4f/hr\n", r.SavingsProposed)
	f("savings realized  $%8.4f/hr  (%.1f%% of proposed)\n",
		r.SavingsRealized, ratioPct(r.SavingsRealized, r.SavingsProposed))
	f("pool cost         $%8.4f/hr -> $%.4f/hr\n", r.PoolCostBefore, r.PoolCostAfter)
	f("forward cost       %d nodes, %d pods unplanned\n", r.ForwardCost.Nodes, r.ForwardCost.Pods)
	f("pods stranded      %d\n", r.PendingAfter)
	f("budget left        %d of %d\n", r.BudgetLeft, cfg.Budget)
	return b.String()
}

const csvHeader = "nodes,seed,k,threshold,budget,candidates,destinations,proposals," +
	"committed,conflicts,applied,proceeded,rolled_back,forward," +
	"savings_proposed,savings_realized,cost_before,cost_after,forward_nodes,forward_pods," +
	"pending,budget_left,identify_ns,generate_ns,compact_ns,execute_ns"

func csvRow(nodes int, seed int64, k float64, cfg sim.RunConfig, r sim.Result) string {
	return fmt.Sprintf("%d,%d,%g,%g,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.6f,%.6f,%.6f,%.6f,%d,%d,%d,%d,%d,%d,%d,%d",
		nodes, seed, k, cfg.Threshold, cfg.Budget,
		r.CandidatesOut, r.Destinations, r.ProposalsGenerated,
		r.Committed, r.Conflicts, r.Applied, r.Proceeded, r.RolledBack, r.Forward,
		r.SavingsProposed, r.SavingsRealized, r.PoolCostBefore, r.PoolCostAfter,
		r.ForwardCost.Nodes, r.ForwardCost.Pods,
		r.PendingAfter, r.BudgetLeft,
		r.IdentifyNanos, r.GenerateNanos, r.CompactNanos, r.ExecuteNanos)
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
