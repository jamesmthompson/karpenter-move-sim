package sim

import (
	"context"
	"fmt"
	"math"
	"testing"
)

// Four repeats of one seed must produce identical results. Before content-derived
// IDs, crypto-random UUIDs broke the compactor's tie-break and seed 1 gave three
// different savings figures across four runs.
func TestRunReproducibleUnderFixedSeed(t *testing.T) {
	var first Result
	for i := 0; i < 4; i++ {
		cat := BuildCatalog()
		nodes, totals := GenCluster(GenClusterOpts{NodeCount: 200, Seed: 1, Catalog: cat})
		got := Run(context.Background(), nodes, totals, cat, DefaultRunConfig(2, 20))
		if i == 0 {
			first = got
			fmt.Printf("seed=1 committed=%d proceeded=%d rolled=%d proposed=%.4f realized=%.4f\n",
				got.Committed, got.Proceeded, got.RolledBack, got.SavingsProposed, got.SavingsRealized)
			continue
		}
		// Compare decisions only. Timing fields measure wall clock, so they
		// differ by design.
		type decisions struct {
			candidates, proposals, committed, conflicts int
			applied, proceeded, rolled, forward         int
			pending                                     int
		}
		d := func(r Result) decisions {
			return decisions{r.CandidatesOut, r.ProposalsGenerated, r.Committed, r.Conflicts,
				r.Applied, r.Proceeded, r.RolledBack, r.Forward, r.PendingAfter}
		}
		if d(got) != d(first) {
			t.Fatalf("run %d diverged\n got:   %+v\n first: %+v", i, d(got), d(first))
		}
		// Money is compared with a tolerance. Cluster.Cost sums over a map, so
		// summation order varies and the last few bits of the mantissa move.
		// A dollar figure agreeing to a nanocent is the same decision.
		const eps = 1e-9
		if math.Abs(got.SavingsProposed-first.SavingsProposed) > eps ||
			math.Abs(got.SavingsRealized-first.SavingsRealized) > eps {
			t.Fatalf("run %d savings diverged: proposed %.12f vs %.12f, realized %.12f vs %.12f",
				i, got.SavingsProposed, first.SavingsProposed, got.SavingsRealized, first.SavingsRealized)
		}
	}
}
