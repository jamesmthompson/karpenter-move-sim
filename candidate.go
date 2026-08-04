package sim

import (
	"sort"

	corev1 "k8s.io/api/core/v1"
)

// The decision unit is the workload scope, not the node.
//
// Locality is "a decision reads only what it acts on." It is not "a decision
// reads one node." The node is the unit for an ordinary pod because an ordinary
// pod's move acts on one node, so one node is both the read set and the action
// set. A gang's move acts on every node holding a member, because breaking one
// member breaks the workload and the only move worth pricing is the one that
// takes it whole. So the gang is the unit, and reading its nodes is as local as
// reading one node is for a single pod: the read set equals the action set.
//
// Topology spread is the contrast, and the distinction is worth keeping sharp. A
// spread check reads pods on nodes the move does not act on, belonging to
// workloads the move has nothing to do with, because maxSkew is defined against a
// minimum over every domain the constraint governs. Its read set exceeds its
// action set and is bounded by the cluster. A gang's read set is bounded by the
// declared workload and equals its action set. "Reads more than one node" flattens
// the two. "Reads more than it acts on" separates them, and only the second is a
// locality violation.
//
// What was wrong before: the node was the unit everywhere. NodeDisruptionCost
// received one node's pod list, which carries no information about members on
// other nodes, so it charged the full gang outage whether the node held the only
// member or one of five. Five such nodes were each charged a full outage for a
// gang that costs one outage to break, which pushed them below stage 1's
// delete-ratio floor and pruned them. That was pricing at the wrong unit rather
// than a cost of locality.

// Candidate is one decision unit: a single node for ordinary pods, a gang for
// gang pods.
//
// Nodes is both what the candidate's move acts on and the whole of what deciding
// about it reads. Pods is every pod on those nodes, gang members and the ordinary
// pods riding along with them, because draining a node takes everything on it.
type Candidate struct {
	// GangID is the gang this candidate is, or empty for a single node.
	GangID string

	// Nodes the move acts on. One for a node candidate; every node holding a
	// member for a gang candidate.
	Nodes []NodeSnapshot

	// Pods is the evicted set: every pod on Nodes, in node order.
	Pods []*corev1.Pod

	// Price is what removing the candidate's nodes recovers.
	Price float64

	// DisruptionCost is what one move over the whole candidate costs. For a node
	// candidate it is the snapshot's own declared cost, unchanged. For a gang it is
	// MoveDisruptionCost over the candidate's nodes and pods, so the gang is charged
	// one outage rather than one per node holding a member.
	DisruptionCost float64
}

// IsGang reports whether this candidate is a gang rather than a single node.
func (c Candidate) IsGang() bool { return c.GangID != "" }

// Disruptable is Disruptable over every node the move acts on. One node carrying
// a do-not-disrupt pod blocks the whole candidate, because a gang move that
// cannot drain one of its nodes cannot take the gang whole and there is no
// half-consolidating it.
func (c Candidate) Disruptable() bool {
	for _, n := range c.Nodes {
		if !Disruptable(n) {
			return false
		}
	}
	return true
}

// DeleteRatio is DeleteRatio at the candidate's own scale: the aggregate price of
// its nodes over its single step disruption cost. For a node candidate it reduces
// to DeleteRatio of that node, term for term.
func (c Candidate) DeleteRatio(totals PoolTotals) float64 {
	if totals.Cost == 0 || totals.DisruptionCost == 0 || c.DisruptionCost == 0 {
		return 0
	}
	return (c.Price / totals.Cost) / (c.DisruptionCost / totals.DisruptionCost)
}

// SavingsRatio is price per unit of disruption at the candidate's scale, the
// unnormalized form the paths rank by.
func (c Candidate) SavingsRatio() float64 {
	if c.DisruptionCost == 0 {
		return 0
	}
	return c.Price / c.DisruptionCost
}

// NodeIDs names the nodes the move acts on, which is the read set the locality
// claim is about.
func (c Candidate) NodeIDs() []string {
	out := make([]string, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		out = append(out, n.ProviderID)
	}
	return out
}

// Pool is the NodePool the candidate charges its disruption budget against. A
// gang spanning pools charges the pool of its first node, which is the same
// approximation upstream makes when one Command spans NodePools: a Command draws
// on one allowance and the mapping is keyed by name.
func (c Candidate) Pool() string {
	if len(c.Nodes) == 0 {
		return ""
	}
	return poolName(c.Nodes[0])
}

// ID is derived from the candidate's content, so a seeded run reproduces. The
// compactor's tie-break is the proposal ID and most proposals sit in a score tie,
// so a generated one would let the tie-break decide most of the ordering.
func (c Candidate) ID() string {
	if c.IsGang() {
		return "gang:" + c.GangID
	}
	if len(c.Nodes) == 0 {
		return ""
	}
	return c.Nodes[0].ProviderID
}

// Candidates groups a node list into decision units.
//
// It reads its own argument and nothing else. Grouping by gang means reading the
// gang label off pods already on the snapshots the caller handed in, so no
// signature here gains a cluster handle, a context, or a client. That is the
// property the locality claim rests on: making the gang the unit did not force
// cluster access into stage 1.
//
// A node holding no gang member is one node candidate. A node holding gang
// members is not a node candidate at all, because draining it is a decision about
// the workloads whose members are on it and not about the node. It appears in one
// candidate per gang it holds.
//
// Two gangs sharing a node therefore produce two overlapping candidates, and that
// is deliberate. The alternative is to merge every gang that shares a node into
// one transitive unit, which is what makes a candidate unbounded: on the default
// fixture at gang prevalence 0.3 the closure swallows 172 of 200 nodes, and a
// unit that grows with how workloads happen to be packed is exactly the
// unboundedness that makes topology spread non-local. Two workloads sharing
// hardware are not one workload. Overlap between candidates is what the compactor
// already resolves, and each gang candidate is honestly priced for everything it
// breaks, because MoveDisruptionCost charges every distinct gang in its evicted
// set once.
func Candidates(nodes []NodeSnapshot) []Candidate {
	var out []Candidate

	gangNodes := map[string][]NodeSnapshot{}
	var gangOrder []string

	for _, n := range nodes {
		ids := gangIDsOn(n)
		if len(ids) == 0 {
			out = append(out, Candidate{
				Nodes: []NodeSnapshot{n},
				Pods:  n.ReschedulablePods,
				Price: n.Price,
				// The snapshot's own declared cost, not a recomputation. A node
				// candidate must score exactly what it scored before the unit changed,
				// and a fixture that declares a cost is entitled to have it used.
				DisruptionCost: n.DisruptionCost,
			})
			continue
		}
		for _, id := range ids {
			if _, seen := gangNodes[id]; !seen {
				gangOrder = append(gangOrder, id)
			}
			gangNodes[id] = append(gangNodes[id], n)
		}
	}

	// Sorted by gang id so the candidate list is a function of the node list and
	// not of map iteration order.
	sort.Strings(gangOrder)
	for _, id := range gangOrder {
		members := gangNodes[id]
		var pods []*corev1.Pod
		price := 0.0
		for _, n := range members {
			pods = append(pods, n.ReschedulablePods...)
			price += n.Price
		}
		out = append(out, Candidate{
			GangID: id,
			Nodes:  members,
			Pods:   pods,
			Price:  price,
			// Priced once over the whole candidate. This is the fix: the gang is
			// charged one outage for the one outage its move causes, rather than one
			// per node holding a member.
			DisruptionCost: MoveDisruptionCost(len(members), pods),
		})
	}
	return out
}

// gangIDsOn names the gangs with a member on this node, sorted and deduplicated.
func gangIDsOn(n NodeSnapshot) []string {
	var ids []string
	seen := map[string]bool{}
	for _, p := range n.ReschedulablePods {
		id, ok := GangID(p)
		if !ok || seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// GangHeldNodes names every node holding a gang member. Those nodes belong to a
// gang decision, so the node-unit paths must not see them: a single-node or
// multi-node move over one of them would take part of a gang and leave the rest
// running, which is the half-consolidation a gang cannot survive.
func GangHeldNodes(nodes []NodeSnapshot) map[string]bool {
	held := map[string]bool{}
	for _, n := range nodes {
		if len(gangIDsOn(n)) > 0 {
			held[n.ProviderID] = true
		}
	}
	return held
}

// CandidateBits is stage 1 at the unit the decision is about: one bit per
// candidate.
//
// The two tests are Identify's, applied to the candidate rather than to a node. A
// candidate any of whose nodes holds a do-not-disrupt pod is no candidate, and a
// candidate whose delete ratio cannot reach the approval threshold cannot produce
// a passing move.
//
// A gang's ratio is computed once, over its nodes and its single step disruption
// cost. Charging it per member is what collapsed the candidate set as gang
// prevalence rose.
func CandidateBits(cands []Candidate, totals PoolTotals, k float64) []bool {
	bits := make([]bool, len(cands))
	for i, c := range cands {
		bits[i] = c.Disruptable() && c.DeleteRatio(totals) >= 1/k
	}
	return bits
}

// IdentifiedCandidates is the candidates that survived stage 1.
func IdentifiedCandidates(nodes []NodeSnapshot, totals PoolTotals, k float64) []Candidate {
	cands := Candidates(nodes)
	bits := CandidateBits(cands, totals, k)
	out := make([]Candidate, 0, len(cands))
	for i, c := range cands {
		if bits[i] {
			out = append(out, c)
		}
	}
	return out
}
