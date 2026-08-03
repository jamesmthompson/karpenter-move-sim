package sim

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// The generator is seeded and reads no clock. Two runs with the same seed
// produce the same cluster, which is what makes a reported reduction number
// something a reader can reproduce rather than something they have to trust.

// Zone is the single availability zone every offering lives in. One zone keeps
// the catalog honest about price while leaving zonal topology out, which the
// pipeline does not model anyway.
const Zone = "us-east-1a"

// instanceFamily is a vCPU-to-memory ratio and the price of its smallest size.
// Prices are us-east-1 on-demand Linux, and they double with each size step,
// which is true of these families to the cent.
type instanceFamily struct {
	name       string
	memPerCPU  int64   // GiB of memory per vCPU
	largePrice float64 // hourly price of the .large size
}

// instanceSize is a vCPU count and the price multiplier that goes with it.
type instanceSize struct {
	suffix string
	vcpu   int64
	// maxPods is the ENI-derived pod limit, capped at the kubelet default of
	// 110. The cap matters: it is what makes a 16xlarge run out of pod slots
	// long before it runs out of memory.
	maxPods int64
}

// BuildCatalog returns eighteen instance types across three memory ratios.
//
// Three ratios rather than one because the ratio is what makes a packing
// decision interesting. A pod set that is CPU heavy and a pod set that is
// memory heavy pick different families, and a catalog with one ratio cannot
// show that.
func BuildCatalog() *Catalog {
	families := []instanceFamily{
		{name: "c7i", memPerCPU: 2, largePrice: 0.0892},
		{name: "m7i", memPerCPU: 4, largePrice: 0.1008},
		{name: "r7i", memPerCPU: 8, largePrice: 0.1323},
	}
	sizes := []instanceSize{
		{suffix: "large", vcpu: 2, maxPods: 29},
		{suffix: "xlarge", vcpu: 4, maxPods: 58},
		{suffix: "2xlarge", vcpu: 8, maxPods: 58},
		{suffix: "4xlarge", vcpu: 16, maxPods: 110},
		{suffix: "8xlarge", vcpu: 32, maxPods: 110},
		{suffix: "16xlarge", vcpu: 64, maxPods: 110},
	}

	var out []*cloudprovider.InstanceType
	for _, f := range families {
		for step, s := range sizes {
			name := fmt.Sprintf("%s.%s", f.name, s.suffix)
			price := f.largePrice * math.Pow(2, float64(step))
			memGiB := s.vcpu * f.memPerCPU

			offeringReqs := scheduling.NewRequirements(
				scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
				scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, Zone),
			)
			out = append(out, &cloudprovider.InstanceType{
				Name: name,
				Requirements: scheduling.NewRequirements(
					scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, name),
					scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
					scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, Zone),
				),
				Offerings: cloudprovider.Offerings{
					{Requirements: offeringReqs, Price: price, Available: true},
				},
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:    *resource.NewQuantity(s.vcpu, resource.DecimalSI),
					corev1.ResourceMemory: *resource.NewQuantity(memGiB*(1<<30), resource.BinarySI),
					corev1.ResourcePods:   *resource.NewQuantity(s.maxPods, resource.DecimalSI),
				},
				// Kubelet and system reserve, not daemonsets. Karpenter subtracts
				// this inside Allocatable(), and leaving it nil panics there.
				Overhead: &cloudprovider.InstanceTypeOverhead{
					KubeReserved: corev1.ResourceList{
						corev1.ResourceCPU:    *resource.NewMilliQuantity(cpuReserveMilli(s.vcpu), resource.DecimalSI),
						corev1.ResourceMemory: *resource.NewQuantity(memReserveBytes(s.maxPods), resource.BinarySI),
					},
					EvictionThreshold: corev1.ResourceList{
						corev1.ResourceMemory: *resource.NewQuantity(100*(1<<20), resource.BinarySI),
					},
				},
			})
		}
	}

	return &Catalog{
		InstanceTypes: out,
		// A daemonset draw every node pays before any workload lands. Node
		// exporter, a log shipper, and a CNI agent add up to about this.
		Overhead: DaemonSetOverhead{
			corev1.ResourceCPU:    *resource.NewMilliQuantity(100, resource.DecimalSI),
			corev1.ResourceMemory: *resource.NewQuantity(200*(1<<20), resource.BinarySI),
		},
	}
}

// cpuReserveMilli is the EKS kubelet CPU reservation: 6% of the first core, 1%
// of the second, 0.5% of cores three and four, 0.25% of the rest.
func cpuReserveMilli(vcpu int64) int64 {
	milli := 60.0
	if vcpu >= 2 {
		milli += 10
	}
	for c := int64(3); c <= vcpu && c <= 4; c++ {
		milli += 5
	}
	if vcpu > 4 {
		milli += 2.5 * float64(vcpu-4)
	}
	return int64(milli)
}

// memReserveBytes is the EKS kubelet memory reservation, which scales with the
// pod limit rather than with memory size.
func memReserveBytes(maxPods int64) int64 {
	return (255 + 11*maxPods) * (1 << 20)
}

// cheapestOffering returns the lowest available price for one instance type.
func cheapestOffering(it *cloudprovider.InstanceType) (float64, bool) {
	price, found := 0.0, false
	for _, o := range it.Offerings {
		if !o.Available {
			continue
		}
		if !found || o.Price < price {
			price, found = o.Price, true
		}
	}
	return price, found
}

// -----------------------------------------------------------------------------
// Cluster generation
// -----------------------------------------------------------------------------

// PodShape is a lognormal distribution over pod requests. CPU is drawn first,
// then a memory-per-core ratio, so a pod's shape and not just its size varies.
// Drawing memory independently of CPU would produce pods no family fits well.
type PodShape struct {
	CPUMedianMilli float64
	CPUSigma       float64

	MemPerCoreMedianMiB float64
	MemPerCoreSigma     float64

	// DoNotDisruptFraction is the share of pods carrying
	// karpenter.sh/do-not-disrupt. These pods make stage 4 reject proposals
	// stages 1 and 2 had no way to know were illegal.
	DoNotDisruptFraction float64

	// ExpensiveFraction is the share of pods that are costly to restart: a model
	// server with a warm cache, a database replica, anything with a long startup.
	// They carry controller.kubernetes.io/pod-deletion-cost. The disruption cost
	// derivation divides it by 2^27, so ExpensiveDeletionCost is the raw
	// annotation value.
	//
	// Without these, every pod costs 1.0 and the score reduces to savings versus
	// pod count. The RFC names that the degenerate default and it is what most
	// clusters run today, so a fixture needs both regimes.
	ExpensiveFraction     float64
	ExpensiveDeletionCost int64

	// PriorityFraction is the share of pods carrying a scheduling priority, and
	// PriorityValue what they carry. The derivation divides priority by 2^25.
	PriorityFraction float64
	PriorityValue    int32

	// The four relational constraints, one fraction each. A pod carrying one makes
	// its node's consolidation depend on where other pods are, which is what the
	// read-cost measurement is about. See constraints.go.
	//
	// Each fraction is drawn unconditionally, like the three flags above, so
	// setting one does not move which pods the others select.
	AntiAffinityFraction float64
	AffinityFraction     float64
	PDBFraction          float64
	SpreadFraction       float64

	// GangFraction is the share of pods belonging to a gang-scheduled workload,
	// and GangCount how many distinct gangs those pods are spread over. A gang's
	// members are all-or-nothing in value, so breaking one member costs the whole
	// workload: see gang.go.
	//
	// Gang membership is drawn per pod rather than per gang, which means a gang's
	// realized size is whatever the draw gave it and its members land on whatever
	// nodes the packer put them on. That is the case worth generating, because a
	// gang confined to one node is the easy case and a gang spread over several is
	// where per-node pricing stops being able to see the whole workload.
	//
	// GangOutageCost is what one gang going down is worth, in EvictionCost units.
	// Zero means DefaultGangOutageCost.
	GangFraction   float64
	GangCount      int
	GangOutageCost float64

	// WorkloadCount is how many distinct workloads pods are drawn from. Every
	// relational constraint selects on WorkloadLabel, so this is the size of the
	// group a constraint relates. One workload means every constrained pod
	// constrains every other, which is the worst case and not the common one.
	// Defaults to 8.
	WorkloadCount int

	// AntiAffinityTopologyKey and AffinityTopologyKey are the scope of the two
	// affinity terms. Both default to hostname, which is where the locality claim
	// holds. Setting either to zone is what makes the same constraint cost the
	// cluster, and readcost_test.go measures both.
	AntiAffinityTopologyKey string
	AffinityTopologyKey     string

	// SpreadTopologyKey and SpreadMaxSkew configure the spread constraint. The key
	// defaults to zone, because spreading over hostnames in a simulator with one
	// pod per workload per node is trivially satisfied and measures nothing.
	SpreadTopologyKey string
	SpreadMaxSkew     int32
}

// AnyRelationalConstraint reports whether any of the four is switched on. A
// fixture with all four at zero produces no constrained pods, so a read-cost
// measurement over it reports zero and looks like a fast answer rather than an
// absent one.
func (s PodShape) AnyRelationalConstraint() bool {
	return s.AntiAffinityFraction > 0 || s.AffinityFraction > 0 ||
		s.PDBFraction > 0 || s.SpreadFraction > 0
}

// PodCountDist is the per-node pod count, lognormal and clamped. A node's pods
// are drawn before its instance type is chosen, so the count drives the size
// rather than the other way around.
type PodCountDist struct {
	Median float64
	Sigma  float64
	Min    int
	Max    int
}

type GenClusterOpts struct {
	NodeCount int
	Seed      int64

	PodsPerNode PodCountDist
	Shape       PodShape

	// Catalog the nodes are drawn from. BuildCatalog() when nil.
	Catalog *Catalog

	// ThinNodeFraction is the share of nodes that lose pods after packing, and
	// ThinPodFractionMin the floor on how much of a node is emptied. Packing
	// alone produces a tightly packed cluster with no consolidation work in it.
	// Churn is what creates the work, and this is the churn model: pods
	// finished and nothing refilled the gap.
	ThinNodeFraction   float64
	ThinPodFractionMin float64
}

// DefaultGenClusterOpts is a mid-size general-purpose cluster: most pods under
// half a core, a long tail up to several cores, and a third of the fleet
// carrying holes left by finished work.
func DefaultGenClusterOpts(nodeCount int, seed int64) GenClusterOpts {
	return GenClusterOpts{
		NodeCount: nodeCount,
		Seed:      seed,
		PodsPerNode: PodCountDist{
			Median: 10,
			Sigma:  0.6,
			Min:    1,
			Max:    40,
		},
		Shape: PodShape{
			CPUMedianMilli:       250,
			CPUSigma:             0.9,
			MemPerCoreMedianMiB:  2048,
			MemPerCoreSigma:      0.7,
			DoNotDisruptFraction: 0.02,
		},
		ThinNodeFraction:   0.30,
		ThinPodFractionMin: 0.40,
	}
}

// GenCluster builds a cluster from opts.Seed alone.
func GenCluster(opts GenClusterOpts) ([]NodeSnapshot, PoolTotals) {
	return GenClusterWith(rand.New(rand.NewSource(opts.Seed)), opts.withDefaults())
}

// withDefaults fills in the pod distributions when a caller leaves them out.
//
// A caller who sets NodeCount and Seed and nothing else gets a median pod count
// of zero, which is Go's zero value for the field, and every node comes out
// empty. The run then succeeds and reports a cluster that has no workload in it,
// which is a fixture that quietly answers every question with the empty-node
// case. Defaults here are a working cluster: eight pods per node, a quarter core
// and 2 GiB per core, drawn log-normally.
func (o GenClusterOpts) withDefaults() GenClusterOpts {
	if o.PodsPerNode.Median <= 0 {
		o.PodsPerNode = PodCountDist{Median: 8, Sigma: 0.7, Min: 0, Max: 40}
	}
	if o.PodsPerNode.Max <= 0 {
		o.PodsPerNode.Max = 40
	}
	if o.Shape.CPUMedianMilli <= 0 {
		o.Shape.CPUMedianMilli = 250
		o.Shape.CPUSigma = 1.0
	}
	if o.Shape.MemPerCoreMedianMiB <= 0 {
		o.Shape.MemPerCoreMedianMiB = 2048
		o.Shape.MemPerCoreSigma = 0.6
	}
	// Churn is what creates consolidation work. A caller who omits it gets a
	// perfectly packed cluster where no node is worth removing and no REPLACE is
	// ever generated, and the run still reports success. Omitting this default
	// silently invalidated two measurements.
	if o.ThinNodeFraction <= 0 {
		o.ThinNodeFraction = 0.4
		o.ThinPodFractionMin = 0.2
	}
	return o
}

// GenClusterWith takes the source explicitly so a caller running several
// clusters in one process controls the stream rather than inheriting a global.
func GenClusterWith(r *rand.Rand, opts GenClusterOpts) ([]NodeSnapshot, PoolTotals) {
	cat := opts.Catalog
	if cat == nil {
		cat = BuildCatalog()
	}
	pool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	limit := largestEffective(cat)

	nodes := make([]NodeSnapshot, 0, opts.NodeCount)
	for i := 0; i < opts.NodeCount; i++ {
		want := opts.PodsPerNode.draw(r)
		batch := make([]*corev1.Pod, 0, want)
		for j := 0; j < want; j++ {
			batch = append(batch, opts.Shape.draw(r, fmt.Sprintf("pod-%d-%d", i, j)))
		}

		// First-fit-decreasing against the largest node the catalog sells, so a
		// node's pod set is feasible for something before it is priced. A pod
		// that does not fit is dropped rather than carried, which keeps the node
		// count exactly what was asked for.
		sort.SliceStable(batch, func(a, b int) bool { return footprint(batch[a]) > footprint(batch[b]) })
		placed := make([]*corev1.Pod, 0, len(batch))
		for _, p := range batch {
			if fitsWithin(append(placed, p), limit) {
				placed = append(placed, p)
			}
		}

		it, price, ok := cat.Cheapest(cat.Fits(withPodSlots(Requests(placed), int64(len(placed)))))
		if !ok {
			continue
		}
		// A zero price makes every score and every delete ratio degenerate: the
		// numerator vanishes, the pool total is understated, and the fixture
		// answers questions it was never asked. Refuse rather than emit one. The
		// RFC's near-zero ODCR case is on-demand price divided by a large
		// constant, which is small but positive, so this catches only a broken
		// catalog.
		// NaN is the case a positivity check misses, since NaN <= 0 is false. One
		// NaN price makes the pool total NaN, then every score NaN, and since
		// NaN >= threshold is false the whole pass silently commits nothing.
		// Upstream guards this explicitly in resolveNodePrice and pins it with an
		// adversarial test.
		if math.IsNaN(price) || math.IsInf(price, 0) || price <= 0 {
			panic(fmt.Sprintf("instance type %s priced at %g; catalog offerings must be finite and positive", it.Name, price))
		}
		nodes = append(nodes, NodeSnapshot{
			ProviderID:        fmt.Sprintf("i-%06d", i),
			NodePool:          pool,
			InstanceType:      it,
			Price:             price,
			ReschedulablePods: placed,
		})
	}

	thin(r, nodes, opts)
	for i := range nodes {
		nodes[i].DisruptionCost = disruptionCost(nodes[i].ReschedulablePods)
	}
	return nodes, Totals(nodes)
}

// Totals sums the two denominators the score divides by.
//
// Disruption cost is a sum over nodes with one correction: a gang spanning
// several nodes is charged to each of them, and the pool total counts each gang
// once, so the duplicate charges are subtracted. Without the correction the
// denominator grows with how scattered the gangs are rather than with how much
// disruptable work the pool holds, and every score in the pool shrinks because a
// gang got spread over two nodes instead of one. See poolGangOverCount.
func Totals(nodes []NodeSnapshot) PoolTotals {
	var t PoolTotals
	for _, n := range nodes {
		// Hand-built snapshots bypass the generator's guard. A non-finite or
		// non-positive price, or a zero disruption cost on a priced node, makes
		// every downstream score meaningless while the run still succeeds.
		if math.IsNaN(n.Price) || math.IsInf(n.Price, 0) || n.Price <= 0 {
			panic(fmt.Sprintf("node %s priced at %g; prices must be finite and positive", n.ProviderID, n.Price))
		}
		if n.DisruptionCost < PerNodeBaseDisruptionCost {
			panic(fmt.Sprintf("node %s disruption cost %g is below the per-node baseline %g",
				n.ProviderID, n.DisruptionCost, PerNodeBaseDisruptionCost))
		}
		t.Cost += n.Price
		t.DisruptionCost += n.DisruptionCost
	}
	t.DisruptionCost -= poolGangOverCount(nodes)
	// The correction subtracts gang charges the per-node sum contained. If a caller
	// built DisruptionCost from RescheduleDisruptionCost on nodes whose pods carry
	// gang labels, the sum never contained them and the subtraction overshoots. The
	// symptom would be a quietly understated denominator inflating every score in
	// the pool, so refuse rather than return one.
	if floor := float64(len(nodes)) * PerNodeBaseDisruptionCost; t.DisruptionCost < floor {
		panic(fmt.Sprintf("pool disruption cost %g fell below %d nodes' baselines; "+
			"snapshot costs must come from NodeDisruptionCost when pods carry gang labels",
			t.DisruptionCost, len(nodes)))
	}
	return t
}

// disruptionCost is what draining this node alone costs.
//
// It was float64(len(pods)) + 1.0, which hardcoded every pod at 1.0 and made
// pod-deletion-cost and pod priority inert. That is the RFC's degenerate default
// case, and scoring it as though it were the general case meant the simulator
// could not reproduce the RFC's own heterogeneous example: two nodes with equal
// savings and equal pod counts scoring 2.24 and 0.29.
//
// It is NodeDisruptionCost rather than RescheduleDisruptionCost, so a gang member
// on the node prices the gang's outage instead of one pod's restart. On a fixture
// with no gangs the two are identical.
func disruptionCost(pods []*corev1.Pod) float64 {
	return NodeDisruptionCost(pods)
}

// thin removes pods from a random subset of nodes. The instance type stays what
// it was, because a node launched for a pod set that has since shrunk is exactly
// the node consolidation exists to reclaim.
func thin(r *rand.Rand, nodes []NodeSnapshot, opts GenClusterOpts) {
	for i := range nodes {
		if r.Float64() >= opts.ThinNodeFraction {
			continue
		}
		pods := nodes[i].ReschedulablePods
		if len(pods) == 0 {
			continue
		}
		frac := opts.ThinPodFractionMin + r.Float64()*(1-opts.ThinPodFractionMin)
		remove := int(math.Round(frac * float64(len(pods))))
		if remove > len(pods) {
			remove = len(pods)
		}
		// Shuffle then truncate, so which pods survive is not a function of the
		// packing order.
		r.Shuffle(len(pods), func(a, b int) { pods[a], pods[b] = pods[b], pods[a] })
		nodes[i].ReschedulablePods = pods[:len(pods)-remove]
	}
}

// -----------------------------------------------------------------------------
// A tightly packed cluster, where REPLACE is the only move available
// -----------------------------------------------------------------------------

// The default fixture is loosely packed, so every node's pods fit somewhere else
// and a DELETE is always feasible. A DELETE recovers the source's full price and
// a REPLACE only the price difference, so the DELETE outscores the REPLACE, the
// two share a candidate, and the compactor drops the REPLACE. Across 75 runs of
// the default fixture that produced 4632 REPLACE proposals and zero commits.
//
// A tight cluster is the regime where the REPLACE is the only move. Every node
// sits against its capacity limit, so the aggregate free capacity is smaller
// than the oversized node's pods need and there is nowhere to delete to. The
// node is still oversized for what it runs, so a cheaper instance type holds the
// same pods.

// OversizedProviderID names the one node in a tight cluster that is larger than
// its pods need. Callers assert against it by name, because a fixture whose
// interesting node has to be found by search stops being about that node as soon
// as the search drifts.
const OversizedProviderID = "i-oversized"

// The two instance types a tight cluster is built from, and the load each
// carries. The numbers are chosen against BuildCatalog's effective capacities
// and are what make the fixture's premises hold, so they are named rather than
// inlined.
//
// A c7i.large has 1830m of CPU and 3222 MiB of memory left after overhead, and
// is the cheapest type in the catalog. Loading it to 1810m leaves 20m free, and
// being the cheapest type means no REPLACE exists for it at any utilization. A
// filler therefore contributes almost nothing to the aggregate free pool and
// generates no proposals of its own.
//
// An r7i.2xlarge has 7810m and 64343 MiB. Loading it to 7000m and 12000 MiB uses
// most of its CPU and a fifth of its memory, which is what oversized means here:
// the node was sized for a memory ratio its pods do not have. A c7i.2xlarge
// holds the same pods at $0.3568 against $0.5292.
const (
	tightFillerType    = "c7i.large"
	tightFillerCPU     = 1810
	tightFillerMemMiB  = 3100
	tightOversizedType = "r7i.2xlarge"
	tightOversizedCPU  = 7000
	tightOversizedMiB  = 12000
)

// TightClusterOpts configures GenTightCluster.
type TightClusterOpts struct {
	// NodeCount includes the oversized node. Ten to thirty is the intended range.
	// Fillers each carry a little unused capacity, so a large enough count adds up
	// to room for the oversized node's pods and the cluster stops being tight.
	// GenTightCluster panics there rather than returning a fixture that answers
	// the wrong question.
	NodeCount int

	// Seed fixes the pod counts and the node ordering. It does not move the
	// per-node CPU and memory totals, because those totals are the whole of what
	// the fixture guarantees.
	Seed int64

	// Catalog the nodes are drawn from. BuildCatalog() when nil.
	Catalog *Catalog
}

// GenTightCluster builds a cluster where the only cost-saving move is a REPLACE.
//
// One node is oversized for its pods. Every other node is a filler pressed
// against its CPU limit. The fillers' free capacity in aggregate is far short of
// what the oversized node's pods need, so aggregateRoom fails and no DELETE is
// generated for it. A cheaper instance type in the catalog holds those same pods,
// so replaceWith finds one.
//
// The fillers run the catalog's cheapest instance type, so no REPLACE exists for
// them either, and their own pods do not fit in what is left elsewhere. The
// oversized node's REPLACE is therefore the only proposal the fixture produces,
// which is what makes an assertion about it an assertion about that move and not
// about whatever else the pass happened to commit.
func GenTightCluster(opts TightClusterOpts) ([]NodeSnapshot, PoolTotals) {
	cat := opts.Catalog
	if cat == nil {
		cat = BuildCatalog()
	}
	if opts.NodeCount < 2 {
		panic(fmt.Sprintf("tight cluster needs at least one filler and the oversized node, got NodeCount=%d", opts.NodeCount))
	}
	r := rand.New(rand.NewSource(opts.Seed))
	pool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}

	filler := instanceTypeNamed(cat, tightFillerType)
	fillerPrice, ok := cheapestOffering(filler)
	if !ok {
		panic(fmt.Sprintf("instance type %s has no available offering", tightFillerType))
	}
	oversized := instanceTypeNamed(cat, tightOversizedType)
	oversizedPrice, ok := cheapestOffering(oversized)
	if !ok {
		panic(fmt.Sprintf("instance type %s has no available offering", tightOversizedType))
	}

	nodes := make([]NodeSnapshot, 0, opts.NodeCount)
	for i := 0; i < opts.NodeCount-1; i++ {
		id := fmt.Sprintf("i-tight-%02d", i)
		pods := splitLoad(id, 3+r.Intn(4), tightFillerCPU, tightFillerMemMiB)
		nodes = append(nodes, NodeSnapshot{
			ProviderID:        id,
			NodePool:          pool,
			InstanceType:      filler,
			Price:             fillerPrice,
			ReschedulablePods: pods,
			DisruptionCost:    disruptionCost(pods),
		})
	}
	overPods := splitLoad(OversizedProviderID, 3+r.Intn(4), tightOversizedCPU, tightOversizedMiB)
	nodes = append(nodes, NodeSnapshot{
		ProviderID:        OversizedProviderID,
		NodePool:          pool,
		InstanceType:      oversized,
		Price:             oversizedPrice,
		ReschedulablePods: overPods,
		DisruptionCost:    disruptionCost(overPods),
	})

	// The two premises the fixture exists to supply, checked against the same
	// aggregateRoom the generator calls rather than argued for in a comment.
	//
	// First: the oversized node's pods do not fit on the rest of the cluster, so
	// deleteOnto declines and a REPLACE is the only move from it.
	if aggregateRoom(withoutNode(nodes, OversizedProviderID), cat, Requests(overPods)) {
		panic(fmt.Sprintf("tight cluster with %d nodes has aggregate room for the oversized node's pods; a DELETE would be generated and the fixture is not tight",
			opts.NodeCount))
	}
	// Second: a cheaper instance type holds those pods, so replaceWith finds one.
	if _, price, ok := cat.Cheapest(cat.Fits(Requests(overPods))); !ok || price >= oversizedPrice {
		panic(fmt.Sprintf("no instance type cheaper than %s at $%g holds the oversized node's pods; no REPLACE would be generated",
			tightOversizedType, oversizedPrice))
	}
	// The fillers must generate nothing, or a pass asserting on the oversized
	// node's REPLACE is really asserting on whatever the compactor ranked above
	// it. A filler runs the cheapest type in the catalog so it has no REPLACE, and
	// this is the DELETE half. Each filler leaves about 20m of CPU unused, and the
	// oversized node leaves memory, so enough fillers add up to one filler's worth
	// of room. That is the ceiling on NodeCount.
	if aggregateRoom(withoutNode(nodes, nodes[0].ProviderID), cat, Requests(nodes[0].ReschedulablePods)) {
		panic(fmt.Sprintf("tight cluster with %d nodes has aggregate room to delete a filler; lower NodeCount",
			opts.NodeCount))
	}

	// Shuffle so nothing downstream can depend on the oversized node sitting
	// last. markNominated walks the slice positionally, and a fixture that always
	// puts the interesting node at a fixed index hides that.
	r.Shuffle(len(nodes), func(a, b int) { nodes[a], nodes[b] = nodes[b], nodes[a] })
	return nodes, Totals(nodes)
}

// splitLoad divides a CPU and memory total across count pods, with the remainder
// on the last pod so the sum is exactly the total. The pod count may vary with
// the seed; the total may not, because the total is what decides whether the
// cluster is tight.
func splitLoad(prefix string, count int, cpuMilli, memMiB int64) []*corev1.Pod {
	if count < 1 {
		count = 1
	}
	pods := make([]*corev1.Pod, 0, count)
	for i := 0; i < count; i++ {
		cpu, mem := cpuMilli/int64(count), memMiB/int64(count)
		if i == count-1 {
			cpu += cpuMilli - cpu*int64(count)
			mem += memMiB - mem*int64(count)
		}
		pods = append(pods, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-pod-%d", prefix, i), Namespace: "default"},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    *resource.NewMilliQuantity(cpu, resource.DecimalSI),
							corev1.ResourceMemory: *resource.NewQuantity(mem*(1<<20), resource.BinarySI),
						},
					},
				}},
			},
		})
	}
	return pods
}

// instanceTypeNamed returns the catalog entry with this name. A fixture naming a
// type the catalog does not sell is broken, so this panics rather than handing
// every caller a second error to thread.
func instanceTypeNamed(cat *Catalog, name string) *cloudprovider.InstanceType {
	for _, it := range cat.InstanceTypes {
		if it.Name == name {
			return it
		}
	}
	panic(fmt.Sprintf("instance type %s is not in the catalog", name))
}

func (d PodCountDist) draw(r *rand.Rand) int {
	n := int(math.Round(d.Median * math.Exp(d.Sigma*r.NormFloat64())))
	if n < d.Min {
		n = d.Min
	}
	if n > d.Max {
		n = d.Max
	}
	return n
}

func (s PodShape) draw(r *rand.Rand, name string) *corev1.Pod {
	cpuMilli := s.CPUMedianMilli * math.Exp(s.CPUSigma*r.NormFloat64())
	cpuMilli = math.Round(clampFloat(cpuMilli, 10, 8000)/10) * 10

	perCore := s.MemPerCoreMedianMiB * math.Exp(s.MemPerCoreSigma*r.NormFloat64())
	memMiB := math.Round(clampFloat(perCore*cpuMilli/1000, 16, 240*1024))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "app",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    *resource.NewMilliQuantity(int64(cpuMilli), resource.DecimalSI),
						corev1.ResourceMemory: *resource.NewQuantity(int64(memMiB)*(1<<20), resource.BinarySI),
					},
				},
			}},
		},
	}
	// Draw every flag unconditionally so adding one does not shift the random
	// stream for the others. A fixture that changes ExpensiveFraction should not
	// silently change which pods are do-not-disrupt.
	doNotDisrupt := r.Float64() < s.DoNotDisruptFraction
	expensive := r.Float64() < s.ExpensiveFraction
	prioritized := r.Float64() < s.PriorityFraction
	// The four relational flags, and the workload draw that goes with them. The
	// workload is drawn whether or not any flag came up, for the same reason: it is
	// one position in the stream and skipping it conditionally would make
	// SpreadFraction move which pods are anti-affine.
	antiAffine := r.Float64() < s.AntiAffinityFraction
	affine := r.Float64() < s.AffinityFraction
	inPDB := r.Float64() < s.PDBFraction
	spread := r.Float64() < s.SpreadFraction
	workloads := s.WorkloadCount
	if workloads < 1 {
		workloads = 8
	}
	workload := fmt.Sprintf("w%d", r.Intn(workloads))
	// The gang draws come off a substream seeded from the pod's name rather than
	// off r, and both are drawn unconditionally.
	//
	// Unconditional for the reason the seven flags above are: consuming a position
	// only when the flag comes up would make GangCount move which pods are members.
	//
	// Off a substream because the flags above cannot manage it. Every one of them
	// takes a position in r, so adding an eighth shifts every draw after it and
	// every generated cluster changes. The read-cost measurement is pinned to that
	// stream at three cluster sizes, and moving it broke all three assertions with
	// GangFraction still at zero. A substream keyed on the pod's name is a pure
	// function of the fixture, so it reproduces under a fixed seed while leaving
	// every existing number where it was.
	gr := rand.New(rand.NewSource(int64(nameHash(name))))
	inGang := gr.Float64() < s.GangFraction
	gangs := s.GangCount
	if gangs < 1 {
		gangs = 4
	}
	gang := fmt.Sprintf("g%d", gr.Intn(gangs))

	if doNotDisrupt || expensive || inGang {
		pod.Annotations = map[string]string{}
	}
	if doNotDisrupt {
		pod.Annotations[v1.DoNotDisruptAnnotationKey] = "true"
	}
	if expensive {
		pod.Annotations[corev1.PodDeletionCost] = strconv.FormatInt(s.ExpensiveDeletionCost, 10)
	}
	if prioritized {
		v := s.PriorityValue
		pod.Spec.Priority = &v
	}

	// Every relational constraint selects on WorkloadLabel, so a pod carrying any
	// of them needs the label for the others' selectors to find it.
	if antiAffine || affine || inPDB || spread || inGang {
		pod.Labels = map[string]string{WorkloadLabel: workload}
	}
	if inGang {
		outage := s.GangOutageCost
		if outage <= 0 {
			outage = DefaultGangOutageCost
		}
		pod.Labels[GangLabel] = gang
		// Replicated onto every member, because the node holding one member has to
		// be able to price the whole outage without reading the others.
		pod.Annotations[GangOutageCostAnnotation] = strconv.FormatFloat(outage, 'f', -1, 64)
	}
	if inPDB {
		// A real PDB has a selector and the pod carries whatever it matches. One
		// PDB per workload.
		pod.Labels[PDBLabel] = workload
	}
	sel := &metav1.LabelSelector{MatchLabels: map[string]string{WorkloadLabel: workload}}
	if antiAffine {
		key := s.AntiAffinityTopologyKey
		if key == "" {
			key = corev1.LabelHostname
		}
		affinityOf(pod).PodAntiAffinity = &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
				{LabelSelector: sel, TopologyKey: key},
			},
		}
	}
	if affine {
		key := s.AffinityTopologyKey
		if key == "" {
			key = corev1.LabelHostname
		}
		affinityOf(pod).PodAffinity = &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
				{LabelSelector: sel, TopologyKey: key},
			},
		}
	}
	if spread {
		key := s.SpreadTopologyKey
		if key == "" {
			key = corev1.LabelTopologyZone
		}
		skew := s.SpreadMaxSkew
		if skew < 1 {
			skew = 1
		}
		pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
			MaxSkew:           skew,
			TopologyKey:       key,
			WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector:     sel,
		}}
	}
	return pod
}

// nameHash is FNV-1a over the pod's name, the seed for the gang substream.
//
// Written out rather than taken from hash/fnv so the constants are visible: a
// substream seed that changes silently changes which pods are gang members, and
// that is the kind of thing a stdlib version bump could do without anyone
// noticing which measurement moved.
func nameHash(name string) uint64 {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211
	h := uint64(offset64)
	for i := 0; i < len(name); i++ {
		h ^= uint64(name[i])
		h *= prime64
	}
	return h
}

func affinityOf(pod *corev1.Pod) *corev1.Affinity {
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	return pod.Spec.Affinity
}

func clampFloat(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// withPodSlots adds the pod count to a resource requirement. The pods resource
// is a real capacity dimension and the only one Requests cannot see, because a
// pod does not request itself.
func withPodSlots(req corev1.ResourceList, count int64) corev1.ResourceList {
	out := corev1.ResourceList{}
	for name, q := range req {
		out[name] = q.DeepCopy()
	}
	out[corev1.ResourcePods] = *resource.NewQuantity(count, resource.DecimalSI)
	return out
}
