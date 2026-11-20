package sim

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// newSource is the explicit stream GenClusterWith takes, so a size sweep in one
// process does not inherit a shared global.
func newSource(seed int64) *rand.Rand { return rand.New(rand.NewSource(seed)) }

// The classification in precondition.go is a claim about how many nodes a check
// reads. These tests measure it rather than assert it, and the correctness tests
// below come first, because a cheap check that returns the wrong answer is cheap
// for the wrong reason.

// -----------------------------------------------------------------------------
// Correctness
// -----------------------------------------------------------------------------

// constraintCluster builds nodes with explicit zone labels. The generator derives
// a zone from the provider ID, which is fine for a distribution and useless for a
// fixture that has to put a known count in a known domain.
func constraintCluster(t *testing.T, zones map[string]string, pods map[string][]*corev1.Pod) *Cluster {
	t.Helper()
	cat := BuildCatalog()
	var snaps []NodeSnapshot
	for id := range zones {
		snaps = append(snaps, node(t, cat, id, "m7i.4xlarge", pods[id]...))
	}
	c := NewCluster(snaps, cat, 100)
	for id, zone := range zones {
		c.Nodes[id].Labels[corev1.LabelTopologyZone] = zone
	}
	return c
}

// labeledPod is a pod in a workload, sized small enough to fit anywhere.
func labeledPod(name, workload string) *corev1.Pod {
	p := testPod(name, 100, 256)
	p.Labels = map[string]string{WorkloadLabel: workload}
	return p
}

func workloadSelector(workload string) *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: map[string]string{WorkloadLabel: workload}}
}

func withAntiAffinity(p *corev1.Pod, workload, key string) *corev1.Pod {
	affinityOf(p).PodAntiAffinity = &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
			{LabelSelector: workloadSelector(workload), TopologyKey: key},
		},
	}
	return p
}

func withAffinity(p *corev1.Pod, workload, key string) *corev1.Pod {
	affinityOf(p).PodAffinity = &corev1.PodAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
			{LabelSelector: workloadSelector(workload), TopologyKey: key},
		},
	}
	return p
}

func withSpread(p *corev1.Pod, workload, key string, maxSkew int32) *corev1.Pod {
	p.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		MaxSkew:           maxSkew,
		TopologyKey:       key,
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector:     workloadSelector(workload),
	}}
	return p
}

// Anti-affinity is a question about the destination's own pod list. Both answers
// have to be right, or "reads one node" is just "reads nothing useful".
func TestAntiAffinityAdmitsAndRefuses(t *testing.T) {
	occupied := labeledPod("w-incumbent", "w")
	c := constraintCluster(t,
		map[string]string{"i-taken": "us-east-1a", "i-free": "us-east-1b"},
		map[string][]*corev1.Pod{"i-taken": {occupied}})

	mover := withAntiAffinity(labeledPod("mover", "w"), "w", corev1.LabelHostname)

	if err := (AntiAffinityConstraint{}).AdmitsPlacement(c.Reader(), mover, "i-taken"); err == nil {
		t.Error("a node already running a matching pod must be refused")
	}
	if err := (AntiAffinityConstraint{}).AdmitsPlacement(c.Reader(), mover, "i-free"); err != nil {
		t.Errorf("an empty node must be admitted, got %v", err)
	}

	// One node read, and the same one either way. This is the locality claim at its
	// smallest.
	r := c.Reader()
	_ = (AntiAffinityConstraint{}).AdmitsPlacement(r, mover, "i-free")
	if r.Reads() != 1 {
		t.Errorf("hostname-scoped anti-affinity read %d nodes, want 1", r.Reads())
	}
}

// Affinity is the same relation with the sign flipped, so the two answers swap.
func TestAffinityAdmitsAndRefuses(t *testing.T) {
	occupied := labeledPod("w-incumbent", "w")
	c := constraintCluster(t,
		map[string]string{"i-taken": "us-east-1a", "i-free": "us-east-1b"},
		map[string][]*corev1.Pod{"i-taken": {occupied}})

	mover := withAffinity(labeledPod("mover", "w"), "w", corev1.LabelHostname)

	if err := (AffinityConstraint{}).AdmitsPlacement(c.Reader(), mover, "i-taken"); err != nil {
		t.Errorf("a node running a matching pod must be admitted, got %v", err)
	}
	if err := (AffinityConstraint{}).AdmitsPlacement(c.Reader(), mover, "i-free"); err == nil {
		t.Error("a node with no matching pod must be refused")
	}

	r := c.Reader()
	_ = (AffinityConstraint{}).AdmitsPlacement(r, mover, "i-taken")
	if r.Reads() != 1 {
		t.Errorf("hostname-scoped affinity read %d nodes, want 1", r.Reads())
	}
}

// A zone-scoped term is the same constraint costing the whole cluster. The
// locality belongs to the topology key and not to the constraint, which is what
// LocalForTopologyKey says and this measures.
func TestAffinityScopeDecidesTheCost(t *testing.T) {
	incumbent := labeledPod("w-incumbent", "w")
	c := constraintCluster(t,
		map[string]string{
			"i-a1": "us-east-1a", "i-a2": "us-east-1a",
			"i-b1": "us-east-1b", "i-b2": "us-east-1b",
		},
		map[string][]*corev1.Pod{"i-a1": {incumbent}})

	// Zone-scoped anti-affinity refuses i-a2 even though i-a2 itself is empty,
	// because the incumbent shares its zone. A one-node check would admit it.
	mover := withAntiAffinity(labeledPod("mover", "w"), "w", corev1.LabelTopologyZone)
	if err := (AntiAffinityConstraint{}).AdmitsPlacement(c.Reader(), mover, "i-a2"); err == nil {
		t.Error("zone-scoped anti-affinity must refuse an empty node whose zone holds a match")
	}
	r := c.Reader()
	_ = (AntiAffinityConstraint{}).AdmitsPlacement(r, mover, "i-b1")
	if r.Reads() != len(c.Nodes) {
		t.Errorf("zone-scoped anti-affinity read %d of %d nodes; finding a zone's members means reading every node",
			r.Reads(), len(c.Nodes))
	}
}

// The bug we are demonstrating the absence of. A one-node spread check is wrong in
// both directions, so each direction is pinned against the correct answer.
func TestTopologyNeedsTheDistribution(t *testing.T) {
	t.Run("admits at maxSkew when every domain is equally loaded", func(t *testing.T) {
		// Three domains, two matching pods each. Adding one makes the destination
		// domain 3 against a minimum of 2, so skew is 1 and maxSkew 2 permits it.
		// A check reading only the destination sees 2 pods, computes 3 against
		// maxSkew 2, and refuses a legal move.
		c := constraintCluster(t,
			map[string]string{"i-a": "us-east-1a", "i-b": "us-east-1b", "i-c": "us-east-1c"},
			map[string][]*corev1.Pod{
				"i-a": {labeledPod("a1", "w"), labeledPod("a2", "w")},
				"i-b": {labeledPod("b1", "w"), labeledPod("b2", "w")},
				"i-c": {labeledPod("c1", "w"), labeledPod("c2", "w")},
			})
		mover := withSpread(labeledPod("mover", "w"), "w", corev1.LabelTopologyZone, 2)
		if err := (TopologyConstraint{}).AdmitsPlacement(c.Reader(), mover, "i-a"); err != nil {
			t.Errorf("skew 1 at maxSkew 2 is legal, got %v", err)
		}
	})

	t.Run("refuses at maxSkew when a domain is empty", func(t *testing.T) {
		// One matching pod in the destination domain, none in another. Adding one
		// makes the destination 2 against a minimum of 0, so skew is 2 and maxSkew 1
		// forbids it. A check reading only the destination sees 1 pod against
		// maxSkew 1 and admits an illegal move.
		c := constraintCluster(t,
			map[string]string{"i-a": "us-east-1a", "i-b": "us-east-1b"},
			map[string][]*corev1.Pod{"i-a": {labeledPod("a1", "w")}})
		mover := withSpread(labeledPod("mover", "w"), "w", corev1.LabelTopologyZone, 1)
		if err := (TopologyConstraint{}).AdmitsPlacement(c.Reader(), mover, "i-a"); err == nil {
			t.Error("skew 2 at maxSkew 1 is illegal and was admitted")
		}
		// The empty domain is what makes the answer no, and it is the one node the
		// destination cannot see.
		if err := (TopologyConstraint{}).AdmitsPlacement(c.Reader(), mover, "i-b"); err != nil {
			t.Errorf("the empty domain is where the pod belongs, got %v", err)
		}
	})

	t.Run("a domain with no nodes read is a domain with no minimum", func(t *testing.T) {
		// Same cluster, and the distribution is read from the nodes. Restricting the
		// read to the destination is what loses the minimum, so this asserts the
		// count directly.
		c := constraintCluster(t,
			map[string]string{"i-a": "us-east-1a", "i-b": "us-east-1b", "i-c": "us-east-1c"},
			map[string][]*corev1.Pod{"i-a": {labeledPod("a1", "w")}})
		r := c.Reader()
		counts := domainCounts(r, corev1.LabelTopologyZone,
			labels.SelectorFromSet(labels.Set{WorkloadLabel: "w"}), "")
		if len(counts) != 3 {
			t.Errorf("want all 3 domains present even at zero, got %v", counts)
		}
		if r.Reads() != 3 {
			t.Errorf("the distribution cost %d node reads, want 3", r.Reads())
		}
	})
}

// The PDB verdict. The status path and the derivation have to agree, or the cheap
// answer is a different answer.
func TestPDBStatusMatchesDerivation(t *testing.T) {
	pods := func(n int, node string) []*corev1.Pod {
		var out []*corev1.Pod
		for i := 0; i < n; i++ {
			p := labeledPod(fmt.Sprintf("%s-p%d", node, i), "w")
			p.Labels[PDBLabel] = "w"
			out = append(out, p)
		}
		return out
	}
	// Nine replicas over three nodes, minAvailable two thirds, so six must stay and
	// three disruptions are allowed.
	c := constraintCluster(t,
		map[string]string{"i-a": "us-east-1a", "i-b": "us-east-1b", "i-c": "us-east-1c"},
		map[string][]*corev1.Pod{"i-a": pods(3, "a"), "i-b": pods(3, "b"), "i-c": pods(3, "c")})
	c.IndexPDBs(2.0 / 3.0)

	pdb := c.PDBs["w"]
	if pdb == nil {
		t.Fatal("no PDB indexed")
	}
	if pdb.CurrentHealthy != 9 || pdb.MinAvailable != 6 || pdb.DisruptionsAllowed != 3 {
		t.Fatalf("PDB status: healthy=%d minAvailable=%d allowed=%d, want 9/6/3",
			pdb.CurrentHealthy, pdb.MinAvailable, pdb.DisruptionsAllowed)
	}

	src := NodeSnapshot{ProviderID: "i-a"}
	p := Proposal{Candidates: []NodeSnapshot{src}}

	// Three pods leaving against three allowed. Both paths permit it.
	status := &PDBChecker{Cluster: c}
	derive := &PDBChecker{Cluster: c, Recompute: true}
	if err := status.Check(context.Background(), p); err != nil {
		t.Errorf("status path refused a legal move: %v", err)
	}
	if err := derive.Check(context.Background(), p); err != nil {
		t.Errorf("derivation refused a legal move: %v", err)
	}
	// The status path read the candidate. The derivation read the cluster. Same
	// answer, different cost, and that difference is the finding.
	if status.Reads() != 1 {
		t.Errorf("status path read %d nodes, want 1 (the candidate)", status.Reads())
	}
	if derive.Reads() != len(c.Nodes) {
		t.Errorf("derivation read %d of %d nodes", derive.Reads(), len(c.Nodes))
	}

	// A fourth pod on the candidate exceeds the allowance, and both paths have to
	// say so. The derivation now sees ten healthy against minAvailable six, so it
	// allows four, which is the staleness the status field carries: the index was
	// built before the pod arrived.
	c.Nodes["i-a"].Pods = append(c.Nodes["i-a"].Pods, pods(1, "a-extra")...)
	if err := (&PDBChecker{Cluster: c}).Check(context.Background(), p); err == nil {
		t.Error("four evictions against three allowed must be refused")
	}
	if err := (&PDBChecker{Cluster: c, Recompute: true}).Check(context.Background(), p); err != nil {
		t.Errorf("the derivation sees the new pod as healthy and allows four, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// The measurement
// -----------------------------------------------------------------------------

// measureShape puts every relational constraint on every pod, so a candidate's
// pods each force a decision and the per-decision cost is what the size sweep
// varies. One workload, because a constraint relating eight workloads relates
// each pod to an eighth of the cluster and the number to report is the cost of
// the relation rather than of the fixture's fan-out.
func measureShape() PodShape {
	return PodShape{
		CPUMedianMilli:       250,
		CPUSigma:             0.9,
		MemPerCoreMedianMiB:  2048,
		MemPerCoreSigma:      0.7,
		AntiAffinityFraction: 1.0,
		AffinityFraction:     1.0,
		PDBFraction:          1.0,
		SpreadFraction:       1.0,
		WorkloadCount:        1,
		SpreadMaxSkew:        50,
	}
}

// readCost is one row of the table.
type readCost struct {
	constraint  string
	perDecision int
	nodes       int
}

// TestNodeReadsPerCheck is the table the classification rests on: nodes read to
// decide one constraint, at three cluster sizes.
//
// Per decision means one (pod, destination) pair, which is what a scheduler Filter
// plugin answers and what local means. The proposal-level cost is larger for every
// placement constraint including the local ones, because finding a destination is
// a walk over the cluster whatever the constraint is. The walk is the executor's
// cost. What the constraint decides is how much each step of the walk costs, and
// that is this number.
func TestNodeReadsPerCheck(t *testing.T) {
	sizes := []int{10, 100, 1000}
	rows := map[string][]readCost{}

	for _, size := range sizes {
		cat := BuildCatalog()
		opts := DefaultGenClusterOpts(size, 17)
		opts.Catalog = cat
		opts.Shape = measureShape()
		nodes, _ := GenClusterWith(newSource(17), opts)
		if len(nodes) < size/2 {
			t.Fatalf("fixture produced %d of %d nodes", len(nodes), size)
		}
		c := NewCluster(nodes, cat, size)
		c.IndexPDBs(0.5)

		// One pod, carrying all four constraints, and one destination that is not
		// its own node. Every row measures the same pair, so the rows differ only in
		// the constraint.
		pod, srcID := firstConstrainedPod(t, c)
		destID := ""
		for _, id := range c.sortedIDs() {
			if id != srcID {
				destID = id
				break
			}
		}

		measure := func(name string, fn func(*NodeReader)) {
			r := c.Reader()
			fn(r)
			rows[name] = append(rows[name], readCost{name, r.Reads(), len(c.Nodes)})
		}

		measure("AntiAffinityHolds (hostname)", func(r *NodeReader) {
			_ = (AntiAffinityConstraint{}).AdmitsPlacement(r, pod, destID)
		})
		measure("AffinityHolds (hostname)", func(r *NodeReader) {
			_ = (AffinityConstraint{}).AdmitsPlacement(r, pod, destID)
		})
		measure("TopologyHolds (zone)", func(r *NodeReader) {
			_ = (TopologyConstraint{}).AdmitsPlacement(r, pod, destID)
		})

		// The same anti-affinity term widened to zone. Same constraint, same pod,
		// different key.
		zoneScoped := withAntiAffinity(labeledPod("probe", pod.Labels[WorkloadLabel]),
			pod.Labels[WorkloadLabel], corev1.LabelTopologyZone)
		measure("AntiAffinityHolds (zone)", func(r *NodeReader) {
			_ = (AntiAffinityConstraint{}).AdmitsPlacement(r, zoneScoped, destID)
		})

		// PDB has no per-destination question: eviction is refused or permitted
		// wherever the pod lands. So the unit is one pod's allowance, measured
		// through the checker on a candidate holding pods of one workload.
		src := NodeSnapshot{ProviderID: srcID}
		prop := Proposal{Candidates: []NodeSnapshot{src}}

		status := &PDBChecker{Cluster: c}
		_ = status.Check(context.Background(), prop)
		rows["PDBAllows (status)"] = append(rows["PDBAllows (status)"],
			readCost{"PDBAllows (status)", status.Reads(), len(c.Nodes)})

		derive := &PDBChecker{Cluster: c, Recompute: true}
		_ = derive.Check(context.Background(), prop)
		rows["PDBAllows (recomputed)"] = append(rows["PDBAllows (recomputed)"],
			readCost{"PDBAllows (recomputed)", derive.Reads(), len(c.Nodes)})

		// The two paths must agree on a freshly indexed cluster at every size, or
		// the cheap column is measuring a different check.
		agreeStatus := status.Check(context.Background(), prop)
		agreeDerive := derive.Check(context.Background(), prop)
		if (agreeStatus == nil) != (agreeDerive == nil) {
			t.Errorf("size %d: PDB paths disagree: status=%v derived=%v", size, agreeStatus, agreeDerive)
		}
	}

	order := []string{
		"AntiAffinityHolds (hostname)",
		"AffinityHolds (hostname)",
		"PDBAllows (status)",
		"PDBAllows (recomputed)",
		"AntiAffinityHolds (zone)",
		"TopologyHolds (zone)",
	}
	fmt.Println("nodes read to decide one constraint, by cluster size")
	fmt.Printf("%-30s %8s %8s %8s\n", "constraint", "N=10", "N=100", "N=1000")
	for _, name := range order {
		line := fmt.Sprintf("%-30s", name)
		for _, rc := range rows[name] {
			line += fmt.Sprintf(" %8d", rc.perDecision)
		}
		fmt.Println(line)
	}
	for _, name := range order {
		r := rows[name]
		fmt.Printf("%-30s reads/nodes: %.3f %.3f %.3f\n", name,
			float64(r[0].perDecision)/float64(r[0].nodes),
			float64(r[1].perDecision)/float64(r[1].nodes),
			float64(r[2].perDecision)/float64(r[2].nodes))
	}

	// Flat means the number does not move with cluster size. Asserting equality
	// rather than a bound, because a local check that reads two nodes at N=1000 and
	// one at N=10 is not local, it is slowly growing.
	for _, name := range []string{
		"AntiAffinityHolds (hostname)",
		"AffinityHolds (hostname)",
		"PDBAllows (status)",
	} {
		r := rows[name]
		if r[0].perDecision != r[1].perDecision || r[1].perDecision != r[2].perDecision {
			t.Errorf("%s claims local but reads %d/%d/%d across N=10/100/1000",
				name, r[0].perDecision, r[1].perDecision, r[2].perDecision)
		}
		// And the constant is the README table's: one node, the destination for the
		// hostname terms and the PDB's own status for the PDB row. Flat at 2 would
		// still be local and would make the table wrong.
		for _, rc := range r {
			if rc.perDecision != 1 {
				t.Errorf("%s at N=%d read %d nodes, README table says 1", name, rc.nodes, rc.perDecision)
			}
		}
	}

	// Non-local means the number tracks the cluster. Spread reads every standing
	// node, so the ratio is one and it holds at all three sizes.
	for _, name := range []string{"TopologyHolds (zone)", "AntiAffinityHolds (zone)", "PDBAllows (recomputed)"} {
		for _, rc := range rows[name] {
			if rc.perDecision != rc.nodes {
				t.Errorf("%s at N=%d read %d nodes, want all %d",
					name, rc.nodes, rc.perDecision, rc.nodes)
			}
		}
	}

	// The gap between the two columns is the claim. If the non-local check were only
	// a constant factor worse the classification would not be worth drawing.
	local := rows["AntiAffinityHolds (hostname)"][2].perDecision
	nonLocal := rows["TopologyHolds (zone)"][2].perDecision
	if nonLocal < 100*local {
		t.Errorf("at N=1000 the non-local check read %d nodes against the local check's %d; "+
			"the classification needs the gap to grow with N", nonLocal, local)
	}
}

// TestProposalReadCost is the same measurement one level up: what a whole Check
// costs, which is the number stage 4 actually pays.
//
// It is larger than the per-decision cost for every placement constraint, local
// ones included, and the reason is the destination search rather than the
// constraint. A pod has to be placed somewhere and finding somewhere means walking
// the cluster. The proposal-level number therefore does not separate local from
// non-local, which is why the per-decision table above is the one the
// classification rests on.
func TestProposalReadCost(t *testing.T) {
	cat := BuildCatalog()
	opts := DefaultGenClusterOpts(100, 23)
	opts.Catalog = cat
	opts.Shape = measureShape()
	nodes, _ := GenClusterWith(newSource(23), opts)
	c := NewCluster(nodes, cat, 100)
	c.IndexPDBs(0.5)

	_, srcID := firstConstrainedPod(t, c)
	if !opts.Shape.AnyRelationalConstraint() {
		t.Fatal("the shape carries no relational constraint; the measurement would report zero")
	}
	prop := Proposal{Candidates: []NodeSnapshot{{ProviderID: srcID}}}
	constrainedPods := 0
	for _, p := range c.Nodes[srcID].Pods {
		if (AntiAffinityConstraint{}).Applies(p) {
			constrainedPods++
		}
	}

	fmt.Printf("proposal-level reads, N=%d nodes, candidate %s with %d constrained pods\n",
		len(c.Nodes), srcID, constrainedPods)
	for _, tc := range []struct {
		name    string
		checker CountingChecker
	}{
		{"AntiAffinityHolds", &PlacementChecker{Cluster: c, Constraint: AntiAffinityConstraint{}}},
		{"AffinityHolds", &PlacementChecker{Cluster: c, Constraint: AffinityConstraint{}}},
		{"TopologyHolds", &PlacementChecker{Cluster: c, Constraint: TopologyConstraint{}}},
		{"PDBAllows (status)", &PDBChecker{Cluster: c}},
		{"PDBAllows (recomputed)", &PDBChecker{Cluster: c, Recompute: true}},
	} {
		err := tc.checker.Check(context.Background(), prop)
		fmt.Printf("  %-24s reads=%4d evaluations=%3d verdict=%v\n",
			tc.name, tc.checker.Reads(), tc.checker.Evaluations(), err == nil)
		if tc.checker.Evaluations() == 0 {
			t.Errorf("%s decided nothing; the fixture carries no such constraint", tc.name)
		}
	}
}

// TestRunPaysTheReadCost runs the whole pipeline over a constrained fleet, so the
// number reported is what a pass costs rather than what a checker costs in
// isolation. It also closes the loop on the declarations: a proposal that does not
// declare a constraint never has it checked, and a pass reporting zero reads for a
// constraint the fixture carries would be measuring nothing.
func TestRunPaysTheReadCost(t *testing.T) {
	// Two normalizations, because one is not enough. Reads per applied proposal
	// makes the sizes comparable, and that number over N is what separates local
	// from non-local: a local check's share of the cluster shrinks as the cluster
	// grows, a non-local check's does not.
	perMove := map[PreconditionKind][]float64{}
	perMoveOverN := map[PreconditionKind][]float64{}
	for _, size := range []int{10, 100, 1000} {
		cat := BuildCatalog()
		opts := DefaultGenClusterOpts(size, 31)
		opts.Catalog = cat
		// A quarter of pods constrained, and eight workloads, which is closer to a
		// real fleet than the all-on shape the per-decision table uses.
		opts.Shape.AntiAffinityFraction = 0.25
		opts.Shape.AffinityFraction = 0.25
		opts.Shape.PDBFraction = 0.25
		opts.Shape.SpreadFraction = 0.25
		opts.Shape.WorkloadCount = 8
		opts.Shape.SpreadMaxSkew = 20
		nodes, totals := GenClusterWith(newSource(31), opts)

		cfg := DefaultRunConfig(2, size/4+1)
		res := Run(context.Background(), nodes, totals, cat, cfg)

		if res.Applied == 0 {
			t.Fatalf("N=%d applied nothing; the pass measured no checks", size)
		}
		fmt.Printf("N=%d nodes, committed=%d applied=%d proceeded=%d stopped=%d causes=%v\n",
			size, res.Committed, res.Applied, res.Proceeded, res.Stopped, res.StopCauses)
		for _, kind := range []PreconditionKind{ScoreStillClears, PDBAllows, AntiAffinityHolds, AffinityHolds, TopologyHolds} {
			per := float64(res.NodeReads[kind]) / float64(res.Applied)
			perMove[kind] = append(perMove[kind], per)
			perMoveOverN[kind] = append(perMoveOverN[kind], per/float64(len(nodes)))
			fmt.Printf("  %-20s reads=%7d per move=%8.1f per move over N=%.3f local=%v\n",
				kind, res.NodeReads[kind], per, per/float64(len(nodes)), kind.Local())
		}
	}

	// The PDB stays flat per move at every size, which is the verdict. Growth here
	// would mean the status field is not enough and the check has to walk the
	// workload after all.
	pdb := perMove[PDBAllows]
	if pdb[2] > 2*pdb[0] {
		t.Errorf("PDB reads per move went %.1f -> %.1f -> %.1f across N=10/100/1000; "+
			"the status path is supposed to be flat", pdb[0], pdb[1], pdb[2])
	}

	// Spread's cost per move tracks the cluster. It is 400 reads at N=100 and 2000
	// at N=1000, which is a fifth of linear rather than linear, and the shortfall is
	// the compactor and not the checker: a pass applies proportionally more
	// proposals at 1000 nodes, and each is cheaper on average because more of them
	// carry few constrained pods. The number that survives the normalization is the
	// ratio against the local checks, so that is what is asserted.
	tsc := perMove[TopologyHolds]
	if tsc[2] < 5*tsc[1] {
		t.Errorf("spread reads per move went %.1f -> %.1f across N=100/1000; "+
			"a non-local check should track cluster size", tsc[1], tsc[2])
	}
	// The score is the cheapest check in the set and the one Karpenter today pays
	// the most for, because upstream answers it by re-deriving every candidate in
	// the cluster. Here it is one read per candidate per check, so a couple of reads
	// per move at any size. Asserted against the PDB rather than as an absolute,
	// since the PDB is the check already established as flat.
	score := perMove[ScoreStillClears]
	if score[2] > 2*score[0] {
		t.Errorf("score reads per move went %.1f -> %.1f -> %.1f across N=10/100/1000; "+
			"recomputing one node's score is supposed to be flat", score[0], score[1], score[2])
	}
	if score[2] > 2*pdb[2] {
		t.Errorf("the score costs %.1f reads per move against the PDB's %.1f at N=1000; "+
			"it reads strictly less state than the PDB does", score[2], pdb[2])
	}

	// The gap is the claim. At N=1000 the non-local check costs two orders of
	// magnitude more per move than any local one, and the local ones' share of the
	// cluster fell by 10x from N=100 while spread's held.
	for _, kind := range []PreconditionKind{ScoreStillClears, PDBAllows, AntiAffinityHolds, AffinityHolds} {
		if perMove[kind][2] > tsc[2]/50 {
			t.Errorf("%s costs %.1f reads per move against spread's %.1f at N=1000; "+
				"the classification needs the gap", kind, perMove[kind][2], tsc[2])
		}
		// A local check's share of the cluster has to shrink as the cluster grows.
		if perMoveOverN[kind][2] > perMoveOverN[kind][1]/2 {
			t.Errorf("%s reads %.4f of the cluster per move at N=1000 against %.4f at N=100; "+
				"a local check's share should fall with N", kind,
				perMoveOverN[kind][2], perMoveOverN[kind][1])
		}
	}
}

// A pod carrying the constraints and the node it sits on. The fixture puts all
// four on every pod, so the first one found carries all four.
func firstConstrainedPod(t *testing.T, c *Cluster) (*corev1.Pod, string) {
	t.Helper()
	for _, id := range c.sortedIDs() {
		for _, p := range c.Nodes[id].Pods {
			if (AntiAffinityConstraint{}).Applies(p) && (TopologyConstraint{}).Applies(p) &&
				p.Labels[PDBLabel] != "" {
				return p, id
			}
		}
	}
	t.Fatal("fixture produced no pod carrying the relational constraints")
	return nil, ""
}

// The unconditional draw, stated as a test. Turning one constraint on must not
// move which pods carry another, or a measurement that varies a fraction is
// varying the fixture underneath itself.
func TestConstraintFlagsDoNotShiftTheStream(t *testing.T) {
	base := measureShape()
	base.AntiAffinityFraction = 0.5
	base.AffinityFraction = 0
	base.PDBFraction = 0.5
	base.SpreadFraction = 0
	base.WorkloadCount = 8

	withAffinityOn := base
	withAffinityOn.AffinityFraction = 0.5

	names := func(s PodShape) []string {
		opts := DefaultGenClusterOpts(60, 5)
		opts.Shape = s
		nodes, _ := GenClusterWith(newSource(5), opts)
		var out []string
		for _, n := range nodes {
			for _, p := range n.ReschedulablePods {
				if (AntiAffinityConstraint{}).Applies(p) {
					out = append(out, p.Name)
				}
			}
		}
		return out
	}

	a, b := names(base), names(withAffinityOn)
	if len(a) == 0 {
		t.Fatal("no anti-affine pods at fraction 0.5")
	}
	if len(a) != len(b) {
		t.Fatalf("turning AffinityFraction on changed the anti-affine set: %d then %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("anti-affine pod %d: %s then %s", i, a[i], b[i])
		}
	}
}
