package sim

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// DaemonSetOverhead is the resource a node gives up to daemonsets before any
// workload pod lands on it. We model it as a discount on capacity, known up
// front, rather than as pods that participate in placement. That keeps both
// generators ignorant of daemonsets: each asks for capacity that fits its
// requirement, and the catalog has already deducted the overhead.
type DaemonSetOverhead corev1.ResourceList

// Effective returns what an instance type has left for workload pods. Karpenter
// tracks the same quantity per node as daemonSetRequests against Allocatable.
// Here it is a property of the instance type, so it is the same for every node
// of that type and can be computed once.
func (c *Catalog) Effective(it *cloudprovider.InstanceType) corev1.ResourceList {
	out := corev1.ResourceList{}
	for name, q := range it.Allocatable() {
		remaining := q.DeepCopy()
		if overhead, ok := c.Overhead[name]; ok {
			remaining.Sub(overhead)
		}
		if remaining.Sign() < 0 {
			remaining = *resource.NewQuantity(0, remaining.Format)
		}
		out[name] = remaining
	}
	return out
}

// Fits builds the predicate Cheapest takes. An instance type fits a requirement
// when its effective capacity covers every requested resource.
func (c *Catalog) Fits(required corev1.ResourceList) func(*cloudprovider.InstanceType) bool {
	return func(it *cloudprovider.InstanceType) bool {
		eff := c.Effective(it)
		for name, want := range required {
			have, ok := eff[name]
			if !ok || have.Cmp(want) < 0 {
				return false
			}
		}
		return true
	}
}

// Requests sums the resource requests of a set of pods. Limits are ignored,
// matching how the scheduler reasons about placement.
func Requests(pods []*corev1.Pod) corev1.ResourceList {
	total := corev1.ResourceList{}
	for _, p := range pods {
		for _, ctr := range p.Spec.Containers {
			for name, q := range ctr.Resources.Requests {
				if cur, ok := total[name]; ok {
					cur.Add(q)
					total[name] = cur
				} else {
					total[name] = q.DeepCopy()
				}
			}
		}
	}
	return total
}

// Utilization is the fraction of a node's effective capacity that its pods
// request, taken as the maximum over resources. A node at 0.9 on memory and 0.2
// on CPU is nearly full, because memory is what will stop the next pod.
func Utilization(n NodeSnapshot, cat *Catalog) float64 {
	if n.InstanceType == nil {
		return 0
	}
	eff := cat.Effective(n.InstanceType)
	req := Requests(n.ReschedulablePods)
	worst := 0.0
	for name, want := range req {
		have, ok := eff[name]
		if !ok || have.IsZero() {
			continue
		}
		frac := float64(want.MilliValue()) / float64(have.MilliValue())
		if frac > worst {
			worst = frac
		}
	}
	return worst
}
