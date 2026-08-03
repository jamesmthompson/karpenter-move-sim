package sim

import (
	"sort"

	"fmt"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// ProvisioningGenerator produces proposals that add capacity for pending pods.
//
// It ranks by the price of the capacity it buys and carries no score, because
// the savings-over-disruption ratio has no meaning here. Nothing is deleted, so
// there are no savings, and nothing is evicted, so there is no disruption. More
// to the point, doing nothing is not an available option. Pods are pending, so
// something launches, and the only question is what.
//
// The two directions differ in what they can spend as well as in what they
// optimize. A node wasting money for another thirty seconds costs cents, so a
// consolidation decision can afford to be slow. Every millisecond of provisioning
// decision latency is a millisecond a pod is not running. Decision latency is a
// property of this code path rather than of the proposals it emits, so it is
// measured on the pipeline and not carried in a field.
type ProvisioningGenerator struct{}

func (g ProvisioningGenerator) Name() string { return "provisioning" }

// Generate bins pending pods and buys the cheapest instance type per bin.
//
// It takes pods and a catalog. No cluster, no existing nodes. A pod that would
// have fit in slack somewhere is the existing-capacity path's business, and
// leaving it out is what keeps this function local and parallel: two workers
// proposing two new nodes cannot contend for a destination that does not exist
// yet.
func (g ProvisioningGenerator) Generate(pending []*corev1.Pod, cat *Catalog) []Proposal {
	var out []Proposal
	for binIdx, bin := range firstFitDecreasing(pending, cat) {
		it, price, ok := cat.Cheapest(cat.Fits(Requests(bin)))
		if !ok {
			// No instance type in the catalog holds this pod. The pod stays
			// pending and the reason belongs in an event, not a proposal.
			continue
		}
		out = append(out, Proposal{
			ID:           fmt.Sprintf("prov/%d/%s", binIdx, it.Name),
			Replacements: []*cloudprovider.InstanceType{it},
			Cost:         price,
			PodsCovered:  len(bin),
			Actions:      []Action{{Kind: Launch, InstanceType: it}},
			// A launch destroys nothing, so there is nothing to gate but the
			// budget. Capacity may have vanished by apply time, which surfaces
			// as a launch failure rather than a precondition.
			Preconditions: []PreconditionKind{BudgetHasRoom},
		})
	}
	return out
}

// firstFitDecreasing packs pods into bins, each bin becoming one prospective
// node. Largest first, which is what Karpenter's own scheduler does with its pod
// queue.
//
// Bin capacity is the largest effective capacity in the catalog, so a bin is
// feasible for something before Cheapest is asked to price it.
func firstFitDecreasing(pods []*corev1.Pod, cat *Catalog) [][]*corev1.Pod {
	sorted := make([]*corev1.Pod, len(pods))
	copy(sorted, pods)
	sort.SliceStable(sorted, func(i, j int) bool {
		return footprint(sorted[i]) > footprint(sorted[j])
	})

	limit := largestEffective(cat)
	var bins [][]*corev1.Pod
	for _, p := range sorted {
		placed := false
		for i := range bins {
			if fitsWithin(append(bins[i], p), limit) {
				bins[i] = append(bins[i], p)
				placed = true
				break
			}
		}
		if !placed {
			bins = append(bins, []*corev1.Pod{p})
		}
	}
	return bins
}

// footprint orders pods for packing. CPU millicores times memory mebibytes,
// which keeps a pod that is large in either dimension near the front.
func footprint(p *corev1.Pod) int64 {
	r := Requests([]*corev1.Pod{p})
	cpu := r[corev1.ResourceCPU]
	mem := r[corev1.ResourceMemory]
	return (cpu.MilliValue() + 1) * (mem.Value()/(1<<20) + 1)
}

func fitsWithin(pods []*corev1.Pod, limit corev1.ResourceList) bool {
	req := Requests(pods)
	for name, want := range req {
		have, ok := limit[name]
		if !ok || have.Cmp(want) < 0 {
			return false
		}
	}
	return true
}

func largestEffective(cat *Catalog) corev1.ResourceList {
	out := corev1.ResourceList{}
	for _, it := range cat.InstanceTypes {
		for name, q := range cat.Effective(it) {
			if cur, ok := out[name]; !ok || q.Cmp(cur) > 0 {
				out[name] = q.DeepCopy()
			}
		}
	}
	return out
}
