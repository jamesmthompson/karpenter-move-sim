package sim

import "sort"

// A checker's cost is the state it has to read. Every claim about locality in
// precondition.go is a claim about that number, so the number is measured here
// rather than argued for in a comment.
//
// NodeReader is the only handle the constraint checkers hold on the node set.
// They cannot reach Cluster.Nodes directly, so a read that is not counted is a
// read that did not happen.
type NodeReader struct {
	c    *Cluster
	seen map[string]bool
}

// Reader returns a fresh reader with an empty account.
func (c *Cluster) Reader() *NodeReader {
	return &NodeReader{c: c, seen: map[string]bool{}}
}

// Node charges one node read and returns the live node.
//
// The charge is per distinct node, not per call, because an API server read is
// cached and a controller that touches the same node twice has not paid twice.
// What we are measuring is how much of the cluster a check has to see.
func (r *NodeReader) Node(providerID string) (*ClusterNode, bool) {
	r.seen[providerID] = true
	n, ok := r.c.Nodes[providerID]
	return n, ok
}

// IDs names every node without charging for any of them. A list of names is one
// call, and every caller here goes on to read the nodes it names, so the reads
// land on Node where they belong. Sorted, because Go map order is random and a
// measurement that changes between runs is not a measurement.
func (r *NodeReader) IDs() []string {
	ids := make([]string, 0, len(r.c.Nodes))
	for id := range r.c.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Reads is the number of distinct nodes read since the reader was made.
func (r *NodeReader) Reads() int { return len(r.seen) }

// CountingChecker is a Checker that reports what its last Check cost.
//
// Evaluations is how many constraint instances that Check decided, so a proposal
// carrying twenty constrained pods and one carrying one can be compared. Reads
// over Evaluations is the per-constraint cost; Reads alone is how much of the
// cluster the check had to see, which is the number locality is about.
type CountingChecker interface {
	Checker

	// Reads is the distinct nodes the last Check read.
	Reads() int

	// Evaluations is the constraint instances the last Check decided.
	Evaluations() int
}

// reads is the accounting every constraint checker embeds.
type reads struct {
	reader *NodeReader
	evals  int
}

func (a *reads) Reads() int {
	if a.reader == nil {
		return 0
	}
	return a.reader.Reads()
}

func (a *reads) Evaluations() int { return a.evals }

// begin starts a fresh account. Every Check calls it first, so Reads() answers
// for one check rather than for the checker's lifetime.
func (a *reads) begin(c *Cluster) *NodeReader {
	a.reader = c.Reader()
	a.evals = 0
	return a.reader
}
