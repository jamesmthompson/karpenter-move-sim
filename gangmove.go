package sim

import (
	"sort"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// GangEvaluation is move generation at the gang unit. One proposal per gang
// candidate, taking the whole gang.
//
// It sits alongside the three node-unit paths rather than inside one of them,
// because the unit is what differs and not the algorithm. Emptiness, multi-node
// and single-node all decide about nodes, and a gang-held node is not a node
// decision: a move over one of its nodes evicts some of the gang's members and
// leaves the rest running, which costs the whole workload and recovers one node's
// price. So the gang-held nodes are withheld from the three node paths and
// answered here, once, at the scope the move acts on.
//
// It takes candidates, a destination set, a catalog and pool totals. No cluster
// handle, no context, no client. Candidates were built by Candidates() from the
// same []NodeSnapshot the other paths read, so the gang unit cost stage 2 nothing
// in reach.
type GangEvaluation struct {
	// Threshold is 1/k, the approval floor, applied to the gang's single score.
	Threshold float64
}

func (GangEvaluation) Path() Path     { return PathGang }
func (GangEvaluation) Reason() Reason { return ReasonUnderutilized }

// ShouldDisrupt is the node paths' predicate at the gang's scale: the candidate
// is not effectively empty.
//
// A gang candidate carrying only baselines holds no member worth anything, which
// cannot happen while a member is present, so this is a guard rather than a
// filter. It is stated because every other path states it.
func (GangEvaluation) ShouldDisrupt(c Candidate, _ PoolTotals, _ float64) bool {
	return c.DisruptionCost > float64(len(c.Nodes))*PerNodeBaseDisruptionCost
}

// Evaluate returns one proposal per gang candidate that yields one.
//
// The budget is charged per node the move deletes, which is what the move
// destroys and what stage 4 will decrement. A gang that does not fit the remaining
// allowance yields nothing rather than a smaller move, because there is no smaller
// move: taking four of five nodes takes the workload down and leaves a fifth of it
// running.
func (g GangEvaluation) Evaluate(cands []Candidate, dests []NodeSnapshot, cat *Catalog, totals PoolTotals, budgets map[string]int) []Proposal {
	// Sorted for the reason the node paths sort: the budget filter decides which
	// candidates make it when the allowance is smaller than the work available, so
	// the highest-value candidates have to come first.
	sorted := sortCandidatesByRatio(cands)

	var out []Proposal
	for _, c := range sorted {
		if !c.IsGang() {
			continue
		}
		need := len(c.Nodes)
		if budgets[c.Pool()] < need {
			continue
		}
		p, ok := g.gangMove(c, dests, cat, totals)
		if !ok {
			continue
		}
		budgets[c.Pool()] -= need
		out = append(out, p)
	}
	return out
}

// gangMove prices and builds the one move a gang candidate has.
//
// The order of the tests is the node paths' order, applied once at the gang's
// scale. Delete first, since it recovers every node's full price and a replacement
// recovers strictly less. Then the score, which is the same approval comparison
// stage 2 applies to every consolidation proposal.
//
// Feasibility is the packing test rather than the sum of free capacity, for the
// same reason multi-node needs it: a gang's pods against a gang's worth of holes
// passes the sum test almost always and the packing test rarely, and a move that
// reports a gang consolidated and then strands its members on the first pod has
// told the measurement a number that never happens.
func (g GangEvaluation) gangMove(c Candidate, dests []NodeSnapshot, cat *Catalog, totals PoolTotals) (Proposal, bool) {
	if !c.Disruptable() {
		return Proposal{}, false
	}
	// The delete ratio bounds every move over this candidate, so a candidate that
	// cannot pass it cannot produce a passing move. Repeated here for a caller that
	// invokes the path directly, the way ConsolidationGenerator repeats it.
	if c.DeleteRatio(totals) < g.Threshold {
		return Proposal{}, false
	}

	// None of the gang's own nodes can receive its pods, because every one of them
	// is being drained.
	room := dests
	for _, n := range c.Nodes {
		room = withoutNode(room, n.ProviderID)
	}

	if batchFits(c.Pods, room, cat) {
		score := Score(c.Price, c.DisruptionCost, totals)
		if score < g.Threshold {
			return Proposal{}, false
		}
		return Proposal{
			ID:            "gang-del/" + c.GangID + batchSuffix(c.Nodes),
			GangID:        c.GangID,
			Candidates:    c.Nodes,
			Savings:       c.Price,
			Score:         score,
			Actions:       batchActions(c.Nodes, nil),
			Preconditions: consolidationPreconditions(c.Nodes...),
			Reason:        ReasonUnderutilized,
		}, true
	}

	it, price, ok := batchReplacement(c.Nodes, c.Pods, c.Price, cat)
	if !ok {
		return Proposal{}, false
	}
	savings := c.Price - price
	score := Score(savings, c.DisruptionCost, totals)
	if score < g.Threshold {
		return Proposal{}, false
	}
	return Proposal{
		ID:           "gang-rep/" + c.GangID + batchSuffix(c.Nodes) + "->" + it.Name,
		GangID:       c.GangID,
		Candidates:   c.Nodes,
		Replacements: []*cloudprovider.InstanceType{it},
		Savings:      savings,
		Score:        score,
		Actions:      batchActions(c.Nodes, it),
		// All-or-nothing on the launch set, which is #3's machinery at the unit it
		// was built for. A gang move that cannot acquire the capacity it planned on
		// releases what it got and evicts nothing, rather than holding a partial
		// acquisition while the workload it was for stays where it is.
		//
		// batchActions already gates every Evict and every Delete on the Launch, so
		// validateAtomicLaunch is satisfied as this stands. With one replacement the
		// flag is trivially true, and it is set rather than omitted because the
		// property it declares is the gang's requirement and not an artifact of the
		// launch count.
		AtomicLaunch:  true,
		Preconditions: consolidationPreconditions(c.Nodes...),
		Reason:        ReasonUnderutilized,
	}, true
}

// batchSuffix names the nodes a gang move acts on. Content-derived, because the
// compactor's tie-break is the ID and a generated one would make a seeded run
// unreproducible. The gang id alone would not do: a gang's node set changes
// between passes as its members move, and two passes proposing different moves
// must not share an ID.
func batchSuffix(nodes []NodeSnapshot) string {
	s := ""
	for _, n := range nodes {
		s += "/" + n.ProviderID
	}
	return s
}

// sortCandidatesByRatio orders candidates by savings ratio descending, the node
// paths' sortBySavingsRatio at the candidate unit.
//
// Stable, and tie-broken on the content-derived ID, for the reason
// sortBySavingsRatio is: ties are common and an unstable sort would let them
// decide the order differently between runs of one seed.
func sortCandidatesByRatio(cands []Candidate) []Candidate {
	out := make([]Candidate, len(cands))
	copy(out, cands)
	sort.SliceStable(out, func(i, j int) bool {
		if r1, r2 := out[i].SavingsRatio(), out[j].SavingsRatio(); r1 != r2 {
			return r1 > r2
		}
		return out[i].ID() < out[j].ID()
	})
	return out
}
