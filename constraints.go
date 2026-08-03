package sim

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// Four hard constraints, one question each, and the measurement is how much of
// the cluster answering it costs.
//
// Every constraint here rides on the pod, which is where Kubernetes puts them:
// anti-affinity and affinity in Spec.Affinity, spread in
// Spec.TopologySpreadConstraints, PDB coverage as a label the PDB's selector
// matches. So a checker reads the constraint off the pod it is deciding about
// and then reads whatever nodes it needs to decide. The nodes go through
// NodeReader, which counts them, and the checkers hold no other path to the node
// set. A read that is not counted is a read that did not happen.

// WorkloadLabel groups pods that constrain each other. One label carries all
// four constraints: an anti-affinity term selects on it, so does an affinity
// term, so does a spread constraint, and a PDB covers a workload.
const WorkloadLabel = "sim.karpenter.sh/workload"

// PDBLabel names the PodDisruptionBudget covering a pod. A real PDB has a
// selector and the pod knows nothing about it; the label is the selector's other
// half, so matching it is the same operation a real selector performs.
const PDBLabel = "sim.karpenter.sh/pdb"

// SimZones are the topology domains node labels are drawn from.
//
// The catalog sells one zone, because pricing is what the catalog exists for and
// one zone keeps it honest. Topology spread needs more than one domain to mean
// anything, so the domain label is derived from the provider ID instead of coming
// from the offering. The derivation is a fixture device. It does not change the
// read count, because a node's domain is on the node either way and learning it
// costs a node read.
var SimZones = []string{"us-east-1a", "us-east-1b", "us-east-1c"}

// derivedZone assigns a node to a domain from its provider ID alone, so the
// assignment is reproducible and needs no second random stream.
func derivedZone(providerID string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(providerID))
	return SimZones[int(h.Sum32()%uint32(len(SimZones)))]
}

// nodeLabels are the labels a topology key is resolved against. Hostname is the
// node itself, which is what makes a hostname-scoped term answerable from one
// node.
func nodeLabels(providerID string) map[string]string {
	return map[string]string{
		corev1.LabelHostname:     providerID,
		corev1.LabelTopologyZone: derivedZone(providerID),
	}
}

// -----------------------------------------------------------------------------
// PodDisruptionBudget
// -----------------------------------------------------------------------------

// PodDisruptionBudget is the object a PDB check consults: a selector, a count,
// and a status.
//
// The status fields are what the PDB controller maintains. They exist in the real
// object for the same reason they matter here: something has to walk the
// workload's pods to know how many are healthy, and the eviction path does not
// want to be the thing that walks them.
type PodDisruptionBudget struct {
	Name string

	// Selector is the label value pods carry under PDBLabel. A real PDB carries a
	// LabelSelector; this is the same match narrowed to the one key the fixture
	// uses.
	Selector string

	// MinAvailable is the floor. Eviction is refused below it.
	MinAvailable int

	// CurrentHealthy and DisruptionsAllowed are status, computed once by
	// IndexPDBs. They go stale as soon as a pod moves, which is true of the real
	// object too.
	CurrentHealthy     int
	DisruptionsAllowed int
}

// IndexPDBs builds one PDB per distinct PDBLabel value and fills its status,
// which requires walking every node.
//
// This walk is the PDB controller's cost and not the checker's, so it takes the
// Cluster directly rather than a NodeReader. Charging it to the checker would say
// the eviction path pays it, which is the thing under test.
func (c *Cluster) IndexPDBs(minAvailableFraction float64) {
	healthy := map[string]int{}
	for _, id := range c.sortedIDs() {
		n := c.Nodes[id]
		if n.Deleted {
			continue
		}
		for _, p := range n.Pods {
			if key := p.Labels[PDBLabel]; key != "" {
				healthy[key]++
			}
		}
	}
	c.PDBs = make(map[string]*PodDisruptionBudget, len(healthy))
	for key, count := range healthy {
		minAvail := int(math.Ceil(minAvailableFraction * float64(count)))
		allowed := count - minAvail
		if allowed < 0 {
			allowed = 0
		}
		c.PDBs[key] = &PodDisruptionBudget{
			Name:               "pdb-" + key,
			Selector:           key,
			MinAvailable:       minAvail,
			CurrentHealthy:     count,
			DisruptionsAllowed: allowed,
		}
	}
}

// -----------------------------------------------------------------------------
// One constraint decision
// -----------------------------------------------------------------------------

// PlacementConstraint answers what a scheduler Filter plugin answers: may this
// pod run on this node. Reads through r are the cost of that one answer, which is
// the number the local/non-local split is about.
type PlacementConstraint interface {
	Kind() PreconditionKind

	// Applies reports whether the pod carries this constraint at all. Pods
	// without it are free.
	Applies(pod *corev1.Pod) bool

	// AdmitsPlacement returns nil when the pod may run on destID.
	AdmitsPlacement(r *NodeReader, pod *corev1.Pod, destID string) error
}

// domainMembers returns the provider IDs sharing dest's value for topologyKey.
//
// Hostname resolves to the destination alone, which is why a hostname-scoped term
// costs one read: the answer is on the node being decided. Any wider key has to
// read every node, because a node's domain is a label on the node and there is no
// way to know which nodes share a domain without looking at them.
func domainMembers(r *NodeReader, dest *ClusterNode, topologyKey string) []string {
	if topologyKey == corev1.LabelHostname || topologyKey == "" {
		return []string{dest.ProviderID}
	}
	want := dest.Labels[topologyKey]
	var out []string
	for _, id := range r.IDs() {
		n, ok := r.Node(id)
		if !ok || n.Deleted {
			continue
		}
		if n.Labels[topologyKey] == want {
			out = append(out, id)
		}
	}
	return out
}

func selectorFor(ls *metav1.LabelSelector) (labels.Selector, error) {
	if ls == nil {
		// A nil selector matches nothing, matching the API's own reading of an
		// absent selector in an affinity term.
		return labels.Nothing(), nil
	}
	return metav1.LabelSelectorAsSelector(ls)
}

// AntiAffinityConstraint is "do not place me where pods matching X run".
//
// The claim under test is that this is local. The term names a topology key, and
// at hostname the domain is the destination node, so the destination's own pod
// list decides it.
type AntiAffinityConstraint struct{}

func (AntiAffinityConstraint) Kind() PreconditionKind { return AntiAffinityHolds }

func (AntiAffinityConstraint) Applies(pod *corev1.Pod) bool {
	return len(antiAffinityTerms(pod)) > 0
}

func (AntiAffinityConstraint) AdmitsPlacement(r *NodeReader, pod *corev1.Pod, destID string) error {
	terms := antiAffinityTerms(pod)
	if len(terms) == 0 {
		return nil
	}
	dest, ok := r.Node(destID)
	if !ok || dest.Deleted {
		return fmt.Errorf("destination %s is gone", destID)
	}
	for _, t := range terms {
		sel, err := selectorFor(t.LabelSelector)
		if err != nil {
			return fmt.Errorf("anti-affinity selector: %w", err)
		}
		for _, id := range domainMembers(r, dest, t.TopologyKey) {
			n, ok := r.Node(id)
			if !ok || n.Deleted {
				continue
			}
			for _, other := range n.Pods {
				if other.Name == pod.Name {
					continue
				}
				if sel.Matches(labels.Set(other.Labels)) {
					return fmt.Errorf("pod %s excludes %s: %s already runs in %s=%s",
						pod.Name, destID, other.Name, t.TopologyKey, n.Labels[t.TopologyKey])
				}
			}
		}
	}
	return nil
}

// AffinityConstraint is the same shape with the sign flipped: place me only where
// a pod matching X already runs. Symmetric, so it is local under the same
// condition and for the same reason.
type AffinityConstraint struct{}

func (AffinityConstraint) Kind() PreconditionKind { return AffinityHolds }

func (AffinityConstraint) Applies(pod *corev1.Pod) bool {
	return len(affinityTerms(pod)) > 0
}

func (AffinityConstraint) AdmitsPlacement(r *NodeReader, pod *corev1.Pod, destID string) error {
	terms := affinityTerms(pod)
	if len(terms) == 0 {
		return nil
	}
	dest, ok := r.Node(destID)
	if !ok || dest.Deleted {
		return fmt.Errorf("destination %s is gone", destID)
	}
	for _, t := range terms {
		sel, err := selectorFor(t.LabelSelector)
		if err != nil {
			return fmt.Errorf("affinity selector: %w", err)
		}
		found := false
		for _, id := range domainMembers(r, dest, t.TopologyKey) {
			n, ok := r.Node(id)
			if !ok || n.Deleted {
				continue
			}
			for _, other := range n.Pods {
				if other.Name == pod.Name {
					continue
				}
				if sel.Matches(labels.Set(other.Labels)) {
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			return fmt.Errorf("pod %s requires a matching pod in %s of %s and finds none",
				pod.Name, t.TopologyKey, destID)
		}
	}
	return nil
}

// TopologyConstraint is maxSkew over topology domains.
//
// maxSkew constrains the distribution, so a correct answer needs the
// distribution. There is no version of this that reads one node: the destination
// domain's count is meaningless without the minimum across the other domains, and
// the minimum is spread over every node in the cluster. The rule applied is the
// scheduler's own, count(destination domain) + 1 - min(count) <= maxSkew.
//
// A check that read only the destination would answer with the destination's own
// count against maxSkew, which is wrong in both directions: it rejects a
// destination holding maxSkew+1 matching pods when every other domain holds as
// many, and it admits a destination holding one when another domain holds none and
// maxSkew is zero. Both are pinned in the tests.
type TopologyConstraint struct{}

func (TopologyConstraint) Kind() PreconditionKind { return TopologyHolds }

func (TopologyConstraint) Applies(pod *corev1.Pod) bool {
	return len(spreadConstraints(pod)) > 0
}

func (TopologyConstraint) AdmitsPlacement(r *NodeReader, pod *corev1.Pod, destID string) error {
	tscs := spreadConstraints(pod)
	if len(tscs) == 0 {
		return nil
	}
	dest, ok := r.Node(destID)
	if !ok || dest.Deleted {
		return fmt.Errorf("destination %s is gone", destID)
	}
	for _, tsc := range tscs {
		sel, err := selectorFor(tsc.LabelSelector)
		if err != nil {
			return fmt.Errorf("spread selector: %w", err)
		}
		counts := domainCounts(r, tsc.TopologyKey, sel, pod.Name)
		domain, ok := dest.Labels[tsc.TopologyKey]
		if !ok {
			// A node without the key is not in any domain, so it cannot host a pod
			// the constraint governs. The scheduler skips such nodes.
			return fmt.Errorf("destination %s carries no %s label", destID, tsc.TopologyKey)
		}
		if _, seen := counts[domain]; !seen {
			counts[domain] = 0
		}
		low := math.MaxInt32
		for _, c := range counts {
			if c < low {
				low = c
			}
		}
		skew := counts[domain] + 1 - low
		if int32(skew) > tsc.MaxSkew {
			return fmt.Errorf("pod %s on %s puts %s=%s at %d against minimum %d, skew %d exceeds maxSkew %d",
				pod.Name, destID, tsc.TopologyKey, domain, counts[domain]+1, low, skew, tsc.MaxSkew)
		}
	}
	return nil
}

// domainCounts is the distribution: matching pods per domain, with every domain
// the cluster has present even at zero. A domain missing from the map would be
// read as having no minimum, and the minimum is what maxSkew measures against.
//
// Every node is read. That is the cost, and it is the whole point.
func domainCounts(r *NodeReader, topologyKey string, sel labels.Selector, excludePod string) map[string]int {
	counts := map[string]int{}
	for _, id := range r.IDs() {
		n, ok := r.Node(id)
		if !ok || n.Deleted {
			continue
		}
		domain, ok := n.Labels[topologyKey]
		if !ok {
			continue
		}
		if _, seen := counts[domain]; !seen {
			counts[domain] = 0
		}
		for _, p := range n.Pods {
			if p.Name == excludePod {
				continue
			}
			if sel.Matches(labels.Set(p.Labels)) {
				counts[domain]++
			}
		}
	}
	return counts
}

func antiAffinityTerms(pod *corev1.Pod) []corev1.PodAffinityTerm {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.PodAntiAffinity == nil {
		return nil
	}
	return pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
}

func affinityTerms(pod *corev1.Pod) []corev1.PodAffinityTerm {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.PodAffinity == nil {
		return nil
	}
	return pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
}

// spreadConstraints returns only the ones that gate. ScheduleAnyway is a
// preference, and a preference cannot make a move illegal.
func spreadConstraints(pod *corev1.Pod) []corev1.TopologySpreadConstraint {
	var out []corev1.TopologySpreadConstraint
	for _, tsc := range pod.Spec.TopologySpreadConstraints {
		if tsc.WhenUnsatisfiable == corev1.DoNotSchedule {
			out = append(out, tsc)
		}
	}
	return out
}

// -----------------------------------------------------------------------------
// The proposal-level checkers stage 4 registers
// -----------------------------------------------------------------------------

// PlacementChecker lifts one PlacementConstraint to a Proposal.
//
// The move is illegal when a pod being evicted has no legal destination left. Not
// when some particular destination refuses it: the executor tries nodes in order
// and one refusal is not a violation.
//
// The search over destinations is what makes the proposal-level total larger than
// the per-decision cost, and the search is O(N) for every placement constraint
// including the local ones. Finding a destination costs a walk whatever the
// constraint is. What the constraint decides is how much each step of that walk
// costs: one node for anti-affinity, the whole cluster for spread.
type PlacementChecker struct {
	Cluster    *Cluster
	Constraint PlacementConstraint
	reads
}

func (k *PlacementChecker) Kind() PreconditionKind { return k.Constraint.Kind() }

func (k *PlacementChecker) Check(_ context.Context, p Proposal) error {
	r := k.begin(k.Cluster)
	draining := map[string]bool{}
	for _, cand := range p.Candidates {
		draining[cand.ProviderID] = true
	}
	for _, cand := range p.Candidates {
		// The candidate has to be read to know which pods are leaving. Stage 2 wrote
		// down what it saw, and pods arrive and finish after that.
		n, ok := r.Node(cand.ProviderID)
		if !ok || n.Deleted {
			continue
		}
		pods := make([]*corev1.Pod, len(n.Pods))
		copy(pods, n.Pods)
		for _, pod := range pods {
			if !k.Constraint.Applies(pod) {
				continue
			}
			k.evals++
			if err := k.anyDestination(r, pod, draining); err != nil {
				return err
			}
		}
	}
	return nil
}

func (k *PlacementChecker) anyDestination(r *NodeReader, pod *corev1.Pod, draining map[string]bool) error {
	var last error
	for _, id := range r.IDs() {
		if draining[id] {
			continue
		}
		n, ok := r.Node(id)
		if !ok || n.Deleted || n.Tainted {
			continue
		}
		if err := k.Constraint.AdmitsPlacement(r, pod, id); err != nil {
			last = err
			continue
		}
		return nil
	}
	if last == nil {
		last = fmt.Errorf("no standing destination for pod %s", pod.Name)
	}
	return fmt.Errorf("%s: %w", k.Kind(), last)
}

// PDBChecker gates eviction on the PodDisruptionBudget.
//
// Two ways to answer, and which one is right is the measurement. Recompute walks
// the workload's pods and derives current availability, which costs the cluster.
// The status path reads the count the PDB object already carries, which costs
// nothing beyond the candidate node the pods are on. The tests assert the two
// agree on a fresh index, because a cheap answer that is a different answer is
// not the same check.
//
// The status is not free in the system, only free here. Something walked the pods
// to produce it, and that something is the PDB controller. What the split shows is
// that the eviction path does not have to be the thing that walks them.
type PDBChecker struct {
	Cluster *Cluster

	// Recompute derives current availability from the nodes instead of reading the
	// PDB's status.
	Recompute bool

	reads
}

func (k *PDBChecker) Kind() PreconditionKind { return PDBAllows }

func (k *PDBChecker) Check(_ context.Context, p Proposal) error {
	r := k.begin(k.Cluster)
	// Evictions inside one proposal draw on the same allowance, so they are
	// counted. A PDB permitting one disruption does not permit three pods of the
	// same workload to leave together.
	spent := map[string]int{}
	for _, cand := range p.Candidates {
		n, ok := r.Node(cand.ProviderID)
		if !ok || n.Deleted {
			continue
		}
		for _, pod := range n.Pods {
			key := pod.Labels[PDBLabel]
			if key == "" {
				continue
			}
			k.evals++
			allowed, err := k.allowed(r, key)
			if err != nil {
				return err
			}
			spent[key]++
			if spent[key] > allowed {
				return fmt.Errorf("%s allows %d disruptions, this move takes %d (pod %s)",
					"pdb-"+key, allowed, spent[key], pod.Name)
			}
		}
	}
	return nil
}

// allowed is the two paths side by side. Both end at the same number; only the
// reads differ.
func (k *PDBChecker) allowed(r *NodeReader, key string) (int, error) {
	pdb, ok := k.Cluster.PDBs[key]
	if !ok {
		return 0, fmt.Errorf("no PodDisruptionBudget indexed for %s", key)
	}
	if !k.Recompute {
		// The object carries the answer. No node is touched.
		return pdb.DisruptionsAllowed, nil
	}
	healthy := 0
	for _, id := range r.IDs() {
		n, ok := r.Node(id)
		if !ok || n.Deleted {
			continue
		}
		for _, pod := range n.Pods {
			if pod.Labels[PDBLabel] == key {
				healthy++
			}
		}
	}
	allowed := healthy - pdb.MinAvailable
	if allowed < 0 {
		allowed = 0
	}
	return allowed, nil
}
