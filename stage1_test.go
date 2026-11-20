package sim

import (
	"context"
	"testing"
)

// TestStage1Reduction pins the README's stage-1 numbers on the fixture the
// README's command line builds: go run ./cmd/simrun -nodes 200 -seed 7. The
// README quotes these; if the fixture or the filter moves, this fails and the
// README has to move with it.
func TestStage1Reduction(t *testing.T) {
	cat := BuildCatalog()
	opts := DefaultGenClusterOpts(200, 7)
	opts.Catalog = cat
	nodes, totals := GenCluster(opts)
	if len(nodes) != 200 {
		t.Fatalf("fixture built %d nodes, want 200", len(nodes))
	}

	for _, tc := range []struct {
		k    float64
		kept int
	}{
		{1, 61},    // 30.5%
		{1.5, 130}, // 65%
		{2, 149},   // 74.5%, simrun's default k
		{3, 162},   // 81%
	} {
		kept := 0
		for _, b := range Identify(nodes, totals, tc.k) {
			if b {
				kept++
			}
		}
		if kept != tc.kept {
			t.Errorf("k=%g: Identify kept %d of %d (%.1f%%), README says %d",
				tc.k, kept, len(nodes), 100*float64(kept)/float64(len(nodes)), tc.kept)
		}
	}

	// simrun reports the candidate-unit count from Run, not Identify's node bits.
	// The fixture has no gangs, so the two agree, and the README's number is the
	// one simrun prints.
	res := Run(context.Background(), nodes, totals, cat, DefaultRunConfig(2, 20))
	if res.CandidatesIn != 200 || res.CandidatesOut != 149 {
		t.Errorf("simrun at k=2: %d units -> %d candidates, README says 200 -> 149",
			res.CandidatesIn, res.CandidatesOut)
	}
}
