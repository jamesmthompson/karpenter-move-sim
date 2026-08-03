package sim

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// -----------------------------------------------------------------------------
// Where a pod lands
// -----------------------------------------------------------------------------

// A placement strategy is the scheduler's scoring step, which decides which of
// the nodes a pod fits on it actually gets. Consolidation depends on it more than
// on anything else in the scheduler: the strategy decides how waste is
// distributed, and how waste is distributed decides whether any of it is
// reclaimable.
//
// MostAllocated concentrates pods, so free capacity collects in a few nodes and a
// node emptied enough to delete is a common event. LeastAllocated spreads pods,
// so every node sits partly full and nothing is cleanly removable even though the
// same total capacity is idle. Same cluster, same workload, different consolidation
// opportunity. That is the entropy claim, and having both strategies is what makes
// it a measurement instead of an argument.
//
// The simulator used to do neither. It took the first node in sorted provider-ID
// order that the pod fit on, which is a third policy nobody runs, and it made a
// proposal's feasibility depend on node naming.

// maxNodeScore is the top of the scheduler's score range. Kubernetes fixes it at
// 100 (framework.MaxNodeScore), and both allocation scorers scale into it.
const maxNodeScore int64 = 100

// Slot is one candidate destination reduced to what scoring needs: what it has
// left and what it started with.
//
// Two things need scoring and they hold different types. The executor scores
// live ClusterNodes. PodsStillFit scores those plus the proposal's replacements,
// which are instance types and not nodes yet. A Slot is what both can produce,
// so one strategy serves both and feasibility cannot disagree with the executor
// about where a pod would go.
//
// Free is what fitsInto is tested against, so it carries pod slots. Allocatable
// is the denominator in the score.
type Slot struct {
	Free        corev1.ResourceList
	Allocatable corev1.ResourceList
}

// PlacementStrategy scores one candidate destination for one pod. Higher wins.
//
// Score is called only for slots the pod already fits on, so it never has to
// answer feasibility. Filtering is Pick's job, which keeps the two questions
// separate: capacity decides which nodes are legal, the strategy decides which
// legal node is chosen.
type PlacementStrategy interface {
	Name() string

	// Score returns 0..100 for placing a pod needing need onto slot.
	Score(slot Slot, need corev1.ResourceList) int64
}

// Pick returns the index of the highest-scoring slot the pod fits in.
//
// Ties keep the lowest index. Every caller builds its slot list in sorted
// provider-ID order, so a tie resolves the way first-fit used to and a seeded run
// stays reproducible. Most scores tie at these cluster sizes, since the scorers
// truncate to integers, so this comparison decides a lot of placements.
func Pick(s PlacementStrategy, slots []Slot, need corev1.ResourceList) (int, bool) {
	best := -1
	var bestScore int64
	for i, slot := range slots {
		if !fitsInto(slot.Free, need) {
			continue
		}
		score := s.Score(slot, need)
		if best < 0 || score > bestScore {
			best, bestScore = i, score
		}
	}
	if best < 0 {
		return 0, false
	}
	return best, true
}

// claim deducts a placed pod's requests from a slot's free capacity, so placing
// one pod is visible to the next.
func (s Slot) claim(need corev1.ResourceList) {
	for name, want := range need {
		have := s.Free[name]
		have.Sub(want)
		s.Free[name] = have
	}
}

// -----------------------------------------------------------------------------
// The two real strategies
// -----------------------------------------------------------------------------

// allocationResource is one resource the score averages over, with the weight it
// carries.
type allocationResource struct {
	name   corev1.ResourceName
	weight int64

	// quantize turns a Quantity into the int64 the upstream scorers take. The
	// scheduler's framework.Resource holds CPU in milli and memory in bytes, and
	// integer truncation makes the unit visible in the result, so the units are
	// matched rather than normalized.
	quantize func(resource.Quantity) int64
}

// allocationResources is the kube-scheduler default, defaultResourceSpec in
// pkg/scheduler/apis/config/v1/defaults.go: cpu and memory, weight 1 each.
//
// Pod slots are deliberately absent, as they are upstream. A pod slot is a
// feasibility dimension, which is why Slot.Free carries it and fitsInto tests it,
// but scoring on it would let a node's max-pods figure outvote its actual
// utilization.
var allocationResources = []allocationResource{
	{name: corev1.ResourceCPU, weight: 1, quantize: func(q resource.Quantity) int64 { return q.MilliValue() }},
	{name: corev1.ResourceMemory, weight: 1, quantize: func(q resource.Quantity) int64 { return q.Value() }},
}

// allocationStrategy is the NodeResourcesFit scoring plugin: a per-resource
// fraction of requested to allocatable after the pod lands, weighted-averaged
// across resources.
//
// The two strategies differ only in the per-resource scorer, which is how
// upstream splits them too: one resourceAllocationScorer parameterized by a
// scorer func, in pkg/scheduler/framework/plugins/noderesources.
type allocationStrategy struct {
	name string

	// scorer is leastRequestedScore or mostRequestedScore.
	scorer func(requested, capacity int64) int64
}

func (s allocationStrategy) Name() string { return s.name }

func (s allocationStrategy) Score(slot Slot, need corev1.ResourceList) int64 {
	var nodeScore, weightSum int64
	for _, r := range allocationResources {
		alloc, ok := slot.Allocatable[r.name]
		if !ok {
			continue
		}
		capacity := r.quantize(alloc)
		if capacity == 0 {
			// Upstream skips a resource the node does not have, rather than
			// scoring it zero, so a node missing a scored resource is not
			// penalized on it.
			continue
		}
		var free int64
		if f, ok := slot.Free[r.name]; ok {
			free = r.quantize(f)
		}
		var want int64
		if w, ok := need[r.name]; ok {
			want = r.quantize(w)
		}
		// Requested after the pod lands. We hold free rather than requested, and
		// the scorers take requested, so it is recovered from the capacity. A node
		// oversubscribed past its allocatable would give a negative here; clamp,
		// because both scorers are defined on non-negative requests.
		requested := capacity - free + want
		if requested < 0 {
			requested = 0
		}
		nodeScore += s.scorer(requested, capacity) * r.weight
		weightSum += r.weight
	}
	if weightSum == 0 {
		return 0
	}
	return nodeScore / weightSum
}

// leastRequestedScore is upstream's function of the same name, verbatim from
// pkg/scheduler/framework/plugins/noderesources/least_allocated.go:
//
//	((capacity - requested) * MaxNodeScore) / capacity
//
// The more unused capacity, the higher the score, so the emptiest fitting node
// wins and pods spread.
func leastRequestedScore(requested, capacity int64) int64 {
	if capacity == 0 {
		return 0
	}
	if requested > capacity {
		return 0
	}
	return ((capacity - requested) * maxNodeScore) / capacity
}

// mostRequestedScore is upstream's function of the same name, from
// pkg/scheduler/framework/plugins/noderesources/most_allocated.go:
//
//	(requested * MaxNodeScore) / capacity
//
// The more used capacity, the higher the score, so the fullest fitting node wins
// and pods pack. Upstream clamps requested rather than returning zero, because a
// pod with no requests is scored at a nonzero minimum and can push the total over
// capacity; the same clamp is kept so the two functions stay each other's mirror
// only where upstream makes them so.
func mostRequestedScore(requested, capacity int64) int64 {
	if capacity == 0 {
		return 0
	}
	if requested > capacity {
		requested = capacity
	}
	return (requested * maxNodeScore) / capacity
}

// LeastAllocated favors the emptiest fitting node. It is the kube-scheduler
// default for NodeResourcesFit, set in SetDefaults_NodeResourcesFitArgs.
func LeastAllocated() PlacementStrategy {
	return allocationStrategy{name: string(PlacementLeastAllocated), scorer: leastRequestedScore}
}

// MostAllocated favors the fullest fitting node. This is bin-packing, and it is
// what a cluster run for cost rather than for headroom configures.
func MostAllocated() PlacementStrategy {
	return allocationStrategy{name: string(PlacementMostAllocated), scorer: mostRequestedScore}
}

// -----------------------------------------------------------------------------
// First fit, kept as a named baseline
// -----------------------------------------------------------------------------

// firstFitStrategy takes the first node the pod fits on. Every slot scores the
// same, so Pick's lowest-index tiebreak decides, and every caller orders its
// slots by provider ID.
//
// It is not a scheduler scoring strategy and it is here anyway, for two reasons.
// It is what this simulator did before there was a choice, so a test that depends
// on it now says so. And it is what Karpenter itself does when it reschedules a
// candidate's pods: addToExistingNode takes the lowest-indexed existing node the
// pod can be added to, over a list sortExistingNodes ordered by initialized-first
// then node name. So first-fit-over-sorted-names is the faithful model of the
// consolidation reschedule, and the two allocation strategies are the faithful
// model of what the scheduler does to the pods that go through it.
type firstFitStrategy struct{}

func (firstFitStrategy) Name() string { return string(PlacementFirstFit) }

func (firstFitStrategy) Score(Slot, corev1.ResourceList) int64 { return 0 }

// FirstFit returns the baseline strategy.
func FirstFit() PlacementStrategy { return firstFitStrategy{} }

// -----------------------------------------------------------------------------
// Selecting one by name
// -----------------------------------------------------------------------------

// PlacementStrategyName is the config-level choice. It is a string so a run is
// selected by flag and recorded in a CSV column without a lookup table.
type PlacementStrategyName string

const (
	// PlacementFirstFit is the pre-existing behavior, named so it can be asked
	// for rather than inherited.
	PlacementFirstFit PlacementStrategyName = "first-fit"

	// PlacementLeastAllocated spreads pods. The kube-scheduler default.
	PlacementLeastAllocated PlacementStrategyName = "least-allocated"

	// PlacementMostAllocated packs pods.
	PlacementMostAllocated PlacementStrategyName = "most-allocated"
)

// Strategy resolves a name. The empty name is first-fit, so a Cluster or a
// RunConfig built without the field still places pods the way it always did
// rather than failing at the first eviction.
func (n PlacementStrategyName) Strategy() (PlacementStrategy, error) {
	switch n {
	case "", PlacementFirstFit:
		return FirstFit(), nil
	case PlacementLeastAllocated:
		return LeastAllocated(), nil
	case PlacementMostAllocated:
		return MostAllocated(), nil
	}
	return nil, fmt.Errorf("unknown placement strategy %q; want one of %s, %s, %s",
		n, PlacementFirstFit, PlacementLeastAllocated, PlacementMostAllocated)
}

// MustStrategy is Strategy for a caller that has no error path, which is every
// caller inside the pipeline. A bad name is a configuration mistake and the run
// that follows it would be measuring the wrong thing.
func (n PlacementStrategyName) MustStrategy() PlacementStrategy {
	s, err := n.Strategy()
	if err != nil {
		panic(err.Error())
	}
	return s
}

// PlacementStrategyNames is every choice, in report order.
func PlacementStrategyNames() []PlacementStrategyName {
	return []PlacementStrategyName{PlacementFirstFit, PlacementLeastAllocated, PlacementMostAllocated}
}
