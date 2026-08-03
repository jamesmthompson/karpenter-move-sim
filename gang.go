package sim

import (
	"math"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
)

// A gang-scheduled workload is all-or-nothing in value. Its members have to run
// together, so losing one member is losing all of them. That is a property of the
// workload and it is true while the cluster sits still, before anything proposes
// to change it. It is not a transaction: nothing here is about acquiring several
// things atomically, only about what a workload is worth once a member is gone.
//
// The consequence for consolidation is that the price of touching a gang is a
// step and not a sum. Evicting one member of a five-member gang takes the whole
// workload down, so it costs the whole outage. Evicting three costs the same.
// Evicting all five costs the same again, because after the first member the
// remaining four are already worthless and their restarts add nothing.
//
// RescheduleDisruptionCost cannot express that. It sums max(0, EvictionCost)
// over pods, and a sum has no step in it: it prices five members at five times
// one member, which is the opposite of the truth. So gang pricing needs a cost
// function over the set of pods a move evicts rather than over one pod at a time,
// which is MoveDisruptionCost.

// GangLabel carries the gang a pod belongs to. Every member of one gang carries
// the same value. A gang is a group over pods, not over nodes or over moves: its
// members may sit on any number of nodes, including nodes no move touches.
const GangLabel = "sim.karpenter.sh/gang"

// GangOutageCostAnnotation is what one gang going down costs, in the same units
// EvictionCost produces, replicated onto every member.
//
// It is replicated rather than held once per gang because of where it has to be
// read. A node holds some of a gang's members and cannot see the rest, so a cost
// stored per gang somewhere else would make pricing a non-local read. Replicated
// onto each member, any single member is enough to price the whole outage, which
// is what keeps the per-node cost computable from the node's own pods.
const GangOutageCostAnnotation = "sim.karpenter.sh/gang-outage-cost"

// DefaultGangOutageCost is what the generator writes when a fixture asks for
// gangs without saying what one is worth. Eight is a gang of roughly eight
// default-cost pods, high enough that the step is visible against the per-pod sum
// and low enough that a gang member does not make its node undisruptable in
// practice.
const DefaultGangOutageCost = 8.0

// GangID reports the gang a pod belongs to, and whether it belongs to one.
func GangID(p *corev1.Pod) (string, bool) {
	if p == nil {
		return "", false
	}
	id, ok := p.Labels[GangLabel]
	return id, ok && id != ""
}

// declaredGangOutageCost reads the annotation. A malformed, negative, or
// non-finite value is treated as absent rather than as zero, because a gang
// priced at zero would be free to break, which is the one answer that cannot be
// right for a workload whose members must run together.
func declaredGangOutageCost(p *corev1.Pod) (float64, bool) {
	if p == nil {
		return 0, false
	}
	raw, ok := p.Annotations[GangOutageCostAnnotation]
	if !ok {
		return 0, false
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
		return 0, false
	}
	return v, true
}

// gangCharge is one gang and what breaking it costs.
type gangCharge struct {
	id   string
	cost float64
}

// gangCharges resolves what each gang represented in pods costs, one entry per
// distinct gang, sorted by id.
//
// Sorted so that summing the charges is order-independent. A batch's pods arrive
// in whatever order its nodes were listed in, and float addition is not
// associative, so an unsorted sum would make the same batch score differently
// depending on how its members were enumerated.
//
// Two ways to price a gang, in this order. A declared cost wins, and where
// members disagree the largest declared value wins, so a fixture that writes the
// annotation inconsistently still gets one answer rather than one per caller.
// Failing that, the fall-back is the largest clamped eviction cost among the
// members present, which is the tightest bound a caller holding only some of the
// gang can put on the whole workload going down. That fall-back is degenerate:
// with members of differing eviction cost it makes the step's height depend on
// which members the move happens to touch, which is exactly what the declared
// cost exists to avoid.
func gangCharges(pods []*corev1.Pod) []gangCharge {
	var order []string
	cost := map[string]float64{}
	declared := map[string]bool{}
	for _, p := range pods {
		id, ok := GangID(p)
		if !ok {
			continue
		}
		if _, seen := cost[id]; !seen {
			order = append(order, id)
			cost[id] = 0
		}
		if c, ok := declaredGangOutageCost(p); ok {
			if !declared[id] || c > cost[id] {
				cost[id] = c
			}
			declared[id] = true
			continue
		}
		if declared[id] {
			continue
		}
		if c := math.Max(0, EvictionCost(p)); c > cost[id] {
			cost[id] = c
		}
	}
	if len(order) == 0 {
		return nil
	}
	sort.Strings(order)
	out := make([]gangCharge, 0, len(order))
	for _, id := range order {
		out = append(out, gangCharge{id: id, cost: cost[id]})
	}
	return out
}

// MoveDisruptionCost is what one move costs to apply: a baseline per node it
// drains, the ordinary per-pod sum over the evicted pods that belong to no gang,
// and one full outage per gang the move breaks.
//
// Each broken gang is charged once over the whole evicted set. A move evicting
// three of one gang's five members charges that gang once, not three times, and
// a move evicting members of the same gang from two different nodes charges it
// once across both. That is the step, and charging once is what makes taking a
// gang whole no dearer than taking one member of it.
//
// The double-counting rule is scoped to the move and nothing wider. A gang some
// other concurrent eviction already broke is charged here as though it were
// intact, because whether it is intact is not knowable from this function's
// inputs: the move's own pods are all it is given, and finding out what else is
// being evicted right now means reading the cluster, which stages 1 and 2 are
// built not to do. The error is one-directional. Charging for a gang that is
// already down over-prices the move, so the mistake is refusing a disruption
// rather than approving one, and it corrects itself on the next pass once the
// broken gang's members are back or gone.
//
// The node count is a parameter rather than derived from the pods because the
// baseline is per node and a gang spans nodes. Summing per-node costs would
// charge the gang once per node holding a member; taking the whole evicted set at
// once charges it once. So this is subadditive across nodes: the cost of draining
// two nodes together is at most, and sometimes strictly less than, the sum of
// draining each alone.
func MoveDisruptionCost(nodes int, evicted []*corev1.Pod) float64 {
	cost := float64(nodes) * PerNodeBaseDisruptionCost
	for _, p := range evicted {
		if _, inGang := GangID(p); inGang {
			continue
		}
		cost += math.Max(0, EvictionCost(p))
	}
	for _, g := range gangCharges(evicted) {
		cost += g.cost
	}
	return cost
}

// NodeDisruptionCost is MoveDisruptionCost for a single node's pods, and it is
// what NodeSnapshot.DisruptionCost holds.
//
// With no gang pods on the node it equals RescheduleDisruptionCost term for term,
// so every non-gang number in this simulator is unchanged.
//
// With a gang spanning nodes it over-prices, and that is the architectural cost
// of keeping stage 1 local. A node holding one member of a five-member gang is
// charged the whole outage, because from that node's own pods there is no way to
// tell the difference between holding the only member and holding one of five.
// Two nodes each holding members of the same gang are therefore each charged the
// full outage, while a move taking both is charged once. The over-pricing is
// safe in the direction that matters, since it makes a node look more expensive
// to disrupt than it is, but it is not free: it pushes such nodes below stage 1's
// delete-ratio floor, and the batch that would have priced them correctly is
// exactly what stage 1 has already pruned. See the note on PoolTotals in Totals.
func NodeDisruptionCost(pods []*corev1.Pod) float64 {
	return MoveDisruptionCost(1, pods)
}

// GangsBroken names the gangs a move breaks, sorted. It is what a test asserts on
// when the number is not the point.
func GangsBroken(evicted []*corev1.Pod) []string {
	charges := gangCharges(evicted)
	out := make([]string, 0, len(charges))
	for _, g := range charges {
		out = append(out, g.id)
	}
	return out
}

// poolGangOverCount is what summing per-node disruption costs over-charges for
// gangs spanning more than one node.
//
// The pool denominator counts each gang once, for the same reason a move does:
// the pool's total disruption cost is meant to be what disrupting everything in
// it would cost, and disrupting everything takes each gang down once. Summing
// NodeDisruptionCost over nodes charges a gang once per node holding a member,
// so Totals subtracts this difference.
//
// This read is not local and cannot be made local. Deciding how many nodes a gang
// spans requires looking at every node in the pool. That is not a new
// non-locality, since PoolTotals is already a sum over the pool computed once per
// reconcile and handed down, but it is a real one: the per-node costs stages 1 and
// 2 read do not sum to the denominator they divide by, and no local read can fix
// the discrepancy.
func poolGangOverCount(nodes []NodeSnapshot) float64 {
	perNode := 0.0
	var all []*corev1.Pod
	for _, n := range nodes {
		for _, g := range gangCharges(n.ReschedulablePods) {
			perNode += g.cost
		}
		all = append(all, n.ReschedulablePods...)
	}
	pool := 0.0
	for _, g := range gangCharges(all) {
		pool += g.cost
	}
	return perNode - pool
}
