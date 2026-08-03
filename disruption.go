package sim

import (
	"context"
	"math"

	corev1 "k8s.io/api/core/v1"

	disruptionutils "sigs.k8s.io/karpenter/pkg/utils/disruption"
)

// Karpenter carries two disruption costs per candidate, and the difference is
// the one drawn in the February proposal: eviction cost determines the order
// pods are evicted in, disruption cost determines whether consolidating is worth
// doing at all. The two are often correlated and sometimes not. A low-priority
// batch job that is expensive to checkpoint is cheap to evict and expensive to
// disrupt.
//
// Upstream keeps them as separate fields on Candidate:
//
//	DisruptionCost           = ReschedulingCost(pods) * LifetimeRemaining(...)
//	RescheduleDisruptionCost = PerNodeBaseDisruptionCost + sum(max(0, EvictionCost(pod)))
//
// The first carries a lifetime term, so a node near its expireAfter is cheap to
// disrupt because it is leaving anyway, and it drives eviction-order sorting. The
// second is what balanced scoring divides by, and it has no lifetime term.
//
// The simulator scores moves, so it needs the second. The merged RFC declined to
// add a `karpenter.sh/disruption-cost` annotation and reuses pod-deletion-cost
// and pod priority instead, noting that "a separate annotation could be
// introduced later if eviction ordering and consolidation gating need to
// diverge." So this derivation is a policy choice with a stated escape hatch.

// PerNodeBaseDisruptionCost is the inherent cost of draining a node: cordon,
// drain, API calls, replacement latency. Upstream names the same constant in
// pkg/controllers/disruption/types.go.
//
// The RFC calls 1.0 a placeholder. It keeps an empty node's cost above zero, so
// the score stays finite and empty deletes need no special case, and it leaves
// room to refine, for instance if a GPU node should carry more inherent cost
// than a CPU node.
//
// The consequence shapes every ratio. A 10-pod node has disruption cost 11, so
// the baseline is 9% of its total, while on a 1-pod node the same 1.0 is half the
// total. It therefore weighs against consolidating nodes with few pods, which the
// RFC acknowledges it has not modeled precisely.
const PerNodeBaseDisruptionCost = 1.0

// RescheduleDisruptionCost is what balanced scoring divides by: the per-node
// baseline plus the eviction cost of every pod that would have to move, with
// negatives clamped to zero.
//
// A negative disruption cost would invert the ratio and make a move look better
// the more it hurts, which is why the clamp is here and not in EvictionCost.
// Eviction ordering still sees the signed value.
//
// This mirrors upstream's computeRescheduleDisruptionCost.
//
// It is a sum, so it cannot price a workload whose value is all-or-nothing: a
// gang of five members costs five restarts here, when the truth is one outage.
// MoveDisruptionCost in gang.go is the gang-aware form, and the two agree term
// for term on pods carrying no gang label. This one is kept because it is what
// upstream computes, and the divergence should be visible rather than absorbed.
func RescheduleDisruptionCost(pods []*corev1.Pod) float64 {
	cost := PerNodeBaseDisruptionCost
	for _, p := range pods {
		cost += math.Max(0, EvictionCost(p))
	}
	return cost
}

// EvictionCost calls upstream's pkg/utils/disruption.EvictionCost, so the
// simulator derives cost the way the shipped controller does rather than from a
// port that can drift.
//
// It starts at 1.0 and adds the pod deletion cost annotation over 2^27, spanning
// roughly -16 to +16 across the annotation's int32 range, plus pod priority over
// 2^25, spanning roughly -64 to +30. The result clamps to [-10, 10].
//
// The divisors are not economic ratios. This function was built to order pods,
// and the score divides by its output as though it were a magnitude. Nothing
// establishes that a pod at 10 is ten times as expensive to restart as a pod at
// 1, or that real restart costs fit inside a 10x spread. That is an open question
// about the denominator, and it is why the RFC leaves room for a separate
// annotation.
func EvictionCost(p *corev1.Pod) float64 {
	// Upstream takes a context only to log annotation parse failures.
	return disruptionutils.EvictionCost(context.Background(), p)
}

// IsEffectivelyEmpty reports whether a node carries no reschedulable pods, which
// upstream expresses as its cost having only the baseline in it.
func IsEffectivelyEmpty(disruptionCost float64) bool {
	return disruptionCost <= PerNodeBaseDisruptionCost
}
