package sim

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// Mechanism faults partway through a proposal.
//
// A precondition failure stops a move and is counted as a stop. A write the
// cluster refuses is a different event: Apply returns it as an error, Run ends the
// pass on it, and nothing is undone. These tests pin what the executor leaves
// behind when each kind of write fails after earlier writes in the same proposal
// have already landed.

// writeLog records every node write the executor attempted, in order, and refuses
// the nth write of one kind.
type writeLog struct {
	failKind ActionKind
	failAt   int
	seen     int
	attempts []string
}

func (w *writeLog) hook(kind ActionKind, id string) bool {
	w.attempts = append(w.attempts, (&WriteFailure{Kind: kind, ProviderID: id}).Error())
	if kind != w.failKind {
		return false
	}
	w.seen++
	return w.seen == w.failAt
}

// twoNodeReplace drains two loaded nodes onto one replacement:
//
//	0 taint src1, 1 taint src2, 2 launch,
//	3-4 evict src1, 5-6 evict src2 (each on the launch),
//	7 delete src1 (on 3, 4), 8 delete src2 (on 5, 6).
func twoNodeReplace(t *testing.T) ([]NodeSnapshot, *Catalog, Proposal) {
	t.Helper()
	cat := BuildCatalog()
	batch := []NodeSnapshot{
		node(t, cat, "i-src1", "m7i.2xlarge", testPod("a1", 500, 1024), testPod("a2", 500, 1024)),
		node(t, cat, "i-src2", "m7i.2xlarge", testPod("b1", 500, 1024), testPod("b2", 500, 1024)),
	}
	it := instanceType(t, cat, "m7i.xlarge")
	p := Proposal{
		ID:           "fault-test",
		Candidates:   batch,
		Replacements: []*cloudprovider.InstanceType{it},
		Savings:      1,
		Actions: []Action{
			{Kind: Taint, ProviderID: "i-src1"},
			{Kind: Taint, ProviderID: "i-src2"},
			{Kind: Launch, InstanceType: it},
			{Kind: Evict, ProviderID: "i-src1", DependsOn: []int{2}},
			{Kind: Evict, ProviderID: "i-src1", DependsOn: []int{2}},
			{Kind: Evict, ProviderID: "i-src2", DependsOn: []int{2}},
			{Kind: Evict, ProviderID: "i-src2", DependsOn: []int{2}},
			{Kind: Delete, ProviderID: "i-src1", DependsOn: []int{3, 4}},
			{Kind: Delete, ProviderID: "i-src2", DependsOn: []int{5, 6}},
		},
		Preconditions: []PreconditionKind{BudgetHasRoom},
		Reason:        ReasonUnderutilized,
	}
	return batch, cat, p
}

func launchedNode(c *Cluster) *ClusterNode {
	for _, n := range c.Nodes {
		if n.Launched && !n.Deleted {
			return n
		}
	}
	return nil
}

func TestExecutorWriteFailurePartway(t *testing.T) {
	type state struct {
		tainted1, tainted2 bool
		deleted1, deleted2 bool
		pods1, pods2       int
		launched           bool
		replacementPods    int
		budget             int
	}
	cases := []struct {
		name     string
		kind     ActionKind
		failAt   int
		failNode string
		// attempts is how many node writes the executor tried, the refused one
		// included. Nothing after the refusal may be attempted.
		attempts int
		want     state
	}{
		{
			// src1 is cordoned and src2 is not. The launch never fires, so the
			// proposal bought nothing and moved nothing.
			name: "second taint refused", kind: Taint, failAt: 2, failNode: "i-src2",
			attempts: 2,
			want:     state{tainted1: true, pods1: 2, pods2: 2, budget: 10},
		},
		{
			// src1 drained onto the replacement. src2 is cordoned and still holds
			// both pods. No node is deleted, so no budget is spent.
			name: "first eviction from the second node refused", kind: Evict, failAt: 3, failNode: "i-src2",
			attempts: 5,
			want: state{tainted1: true, tainted2: true, pods1: 0, pods2: 2,
				launched: true, replacementPods: 2, budget: 10},
		},
		{
			// src1 is deleted and charged. src2 is drained and cordoned but stands,
			// and the budget is charged for the one deletion that happened.
			name: "second NodeClaim delete refused", kind: Delete, failAt: 2, failNode: "i-src2",
			attempts: 8,
			want: state{tainted1: true, tainted2: true, deleted1: true,
				launched: true, replacementPods: 4, budget: 9},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			batch, cat, p := twoNodeReplace(t)
			cluster := NewCluster(batch, cat, 10)
			log := &writeLog{failKind: tc.kind, failAt: tc.failAt}
			cluster.WriteFails = log.hook
			exec := NewMemoryExecutor(cluster)

			_, err := exec.Apply(context.Background(), p)

			// A refused write comes back as an error naming the write, not as a
			// precondition stop.
			var wf *WriteFailure
			if !errors.As(err, &wf) {
				t.Fatalf("want a *WriteFailure, got %v", err)
			}
			if wf.Kind != tc.kind || wf.ProviderID != tc.failNode {
				t.Errorf("error names %v on %s, want %v on %s", wf.Kind, wf.ProviderID, tc.kind, tc.failNode)
			}
			var v *PreconditionViolation
			if errors.As(err, &v) {
				t.Errorf("a refused write was reported as a precondition violation: %v", err)
			}
			if exec.Stopped != 0 || exec.Proceeded != 0 || exec.Abandoned != 0 {
				t.Errorf("counters Proceeded=%d Stopped=%d Abandoned=%d, want all 0 on a fault",
					exec.Proceeded, exec.Stopped, exec.Abandoned)
			}
			if exec.ActionsApplied != 0 || len(exec.Failures) != 0 || len(exec.StopCauses) != 0 {
				t.Errorf("a fault was recorded as a stop: ActionsApplied=%d Failures=%v StopCauses=%v",
					exec.ActionsApplied, exec.Failures, exec.StopCauses)
			}

			// The executor stopped at the refusal and did not try the rest.
			if len(log.attempts) != tc.attempts {
				t.Errorf("executor attempted %d writes, want %d: %v", len(log.attempts), tc.attempts, log.attempts)
			}
			if last := log.attempts[len(log.attempts)-1]; last != wf.Error() {
				t.Errorf("last write attempted was %q, want the refused one %q", last, wf.Error())
			}

			src1, src2 := cluster.Nodes["i-src1"], cluster.Nodes["i-src2"]
			got := state{
				tainted1: src1.Tainted, tainted2: src2.Tainted,
				deleted1: src1.Deleted, deleted2: src2.Deleted,
				pods1: len(src1.Pods), pods2: len(src2.Pods),
				budget: cluster.Budget,
			}
			if r := launchedNode(cluster); r != nil {
				got.launched = true
				got.replacementPods = len(r.Pods)
			}
			if got != tc.want {
				t.Errorf("cluster after the fault:\n got %+v\nwant %+v", got, tc.want)
			}
			if cluster.EmptyBudget != 10 {
				t.Errorf("empty budget charged to %d by an underutilized move", cluster.EmptyBudget)
			}
			if cluster.LaunchesInFlight != 0 {
				t.Errorf("LaunchesInFlight left at %d", cluster.LaunchesInFlight)
			}
			// Every pod is somewhere. The refused write moved nothing.
			total := len(cluster.Pending)
			for _, n := range cluster.Nodes {
				if !n.Deleted {
					total += len(n.Pods)
				}
			}
			if total != 4 || len(cluster.Pending) != 0 {
				t.Errorf("%d pods accounted for with %d pending, want 4 placed", total, len(cluster.Pending))
			}
		})
	}
}

// stranger adds an unrelated pod to the second node this pass launched, during
// check number onCall. It stands in for kube-scheduler binding a pending pod to a
// new, untainted node while the executor is still launching the rest of a
// launch set.
type stranger struct {
	cluster *Cluster
	onCall  int
	calls   int
}

func (s *stranger) Kind() PreconditionKind { return NotNominated }

func (s *stranger) Check(context.Context, Proposal) error {
	s.calls++
	if s.calls != s.onCall {
		return nil
	}
	// Launched node IDs are sequential, so sorted order is launch order.
	var launched []string
	for _, id := range s.cluster.sortedIDs() {
		if s.cluster.Nodes[id].Launched {
			launched = append(launched, id)
		}
	}
	n := s.cluster.Nodes[launched[1]]
	n.Pods = append(n.Pods, testPod("stranger", 100, 128))
	return nil
}

// An atomic proposal whose third launch is refused releases its first two. If the
// second has picked up a pod in the meantime, Release refuses it, and Apply
// reports a mechanism fault instead of an abandonment. The first release has
// already happened and stays, the second node stays with its pod, and nothing is
// evicted, deleted, or charged.
func TestAbandonStopsOnARefusedRelease(t *testing.T) {
	batch, cat, its := atomicFixture(t)
	its = append(its, its[0])
	cluster := NewCluster(batch, cat, 10)
	cluster.LaunchFails = FailLaunchAfter(2)

	p := twoLaunchReplace(batch, its...)
	p.AtomicLaunch = true
	p.Preconditions = append(p.Preconditions, NotNominated)

	exec := NewMemoryExecutor(cluster)
	// Actions 0-1 taint, 2-4 launch. The check before action 4, the refused
	// launch, is the fifth.
	exec.Checkers[NotNominated] = &stranger{cluster: cluster, onCall: 5}

	_, err := exec.Apply(context.Background(), p)
	if err == nil {
		t.Fatal("a refused release was not reported")
	}
	if !strings.Contains(err.Error(), "abandoning atomic-test") || !strings.Contains(err.Error(), "holds 1 pods") {
		t.Errorf("error %q does not name the abandonment and the occupied node", err)
	}
	var refused *LaunchFailure
	if errors.As(err, &refused) {
		t.Errorf("the release fault was reported as the launch refusal: %v", err)
	}

	if exec.Abandoned != 0 || exec.Stopped != 0 || exec.Proceeded != 0 {
		t.Errorf("counters Abandoned=%d Stopped=%d Proceeded=%d, want all 0 on a fault",
			exec.Abandoned, exec.Stopped, exec.Proceeded)
	}
	if exec.LaunchesReleased != 1 {
		t.Errorf("LaunchesReleased=%d, want the 1 that succeeded before the refusal", exec.LaunchesReleased)
	}
	if _, ok := cluster.Nodes["i-new-0001"]; ok {
		t.Error("the first launch was not released")
	}
	second, ok := cluster.Nodes["i-new-0002"]
	if !ok || len(second.Pods) != 1 {
		t.Fatalf("the second launch should stand with the stranger pod, got %+v", second)
	}
	if got := standingLaunched(cluster); got != 1 {
		t.Errorf("%d launched nodes standing, want 1", got)
	}
	for _, c := range batch {
		n := cluster.Nodes[c.ProviderID]
		if !n.Tainted || n.Deleted || len(n.Pods) != 2 {
			t.Errorf("%s tainted=%v deleted=%v pods=%d, want tainted, standing, 2 pods",
				c.ProviderID, n.Tainted, n.Deleted, len(n.Pods))
		}
	}
	if cluster.Budget != 10 || len(cluster.Pending) != 0 {
		t.Errorf("budget %d pending %d, want 10 and 0", cluster.Budget, len(cluster.Pending))
	}
	if cluster.LaunchesInFlight != 0 {
		t.Errorf("LaunchesInFlight left at %d", cluster.LaunchesInFlight)
	}
}

// The cluster's own write guards, called directly. Each refuses a write that has
// no valid target and changes nothing.
func TestClusterWritesRefuseAMissingTarget(t *testing.T) {
	cat := BuildCatalog()
	standing := node(t, cat, "i-keep", "m7i.large", testPod("k1", 500, 1024))
	gone := node(t, cat, "i-gone", "m7i.large")

	cluster := NewCluster([]NodeSnapshot{standing, gone}, cat, 10)
	if err := cluster.Delete("i-gone"); err != nil {
		t.Fatalf("setup delete: %v", err)
	}
	launched, err := cluster.Launch(instanceType(t, cat, "m7i.large"))
	if err != nil {
		t.Fatalf("setup launch: %v", err)
	}
	if err := cluster.Release(launched); err != nil {
		t.Fatalf("setup release: %v", err)
	}

	cases := []struct {
		name string
		call func() error
		want string
	}{
		{"taint a deleted node", func() error { return cluster.Taint("i-gone") }, "node is gone"},
		{"evict from a deleted node", func() error { return cluster.Evict("i-gone", "") }, "node is gone"},
		{"delete a deleted node", func() error { return cluster.Delete("i-gone") }, "node is gone"},
		{"release a released node", func() error { return cluster.Release(launched) }, "node is gone"},
		{"release a node the pass did not launch", func() error { return cluster.Release("i-keep") }, "not launched by this pass"},
		{"launch a nil type", func() error { _, err := cluster.Launch(nil); return err }, "nil instance type"},
		{"launch a type with no offering", func() error {
			_, err := cluster.Launch(&cloudprovider.InstanceType{Name: "x.none"})
			return err
		}, "no available offering"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := len(cluster.Nodes)
			err := tc.call()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error containing %q", err, tc.want)
			}
			if len(cluster.Nodes) != before {
				t.Errorf("a refused write changed the node count from %d to %d", before, len(cluster.Nodes))
			}
		})
	}
	keep := cluster.Nodes["i-keep"]
	if keep.Tainted || len(keep.Pods) != 1 || len(cluster.Pending) != 0 {
		t.Errorf("the standing node changed: tainted=%v pods=%d pending=%d",
			keep.Tainted, len(keep.Pods), len(cluster.Pending))
	}
}

// The two Evict outcomes that are not failures, and Delete stranding a pod. An
// Evict against a node whose pods already left does nothing. A pod with nowhere
// to land, from either an Evict or a Delete, goes to Pending instead of being
// lost.
func TestClusterEvictAndDeleteWithNowhereToLand(t *testing.T) {
	cat := BuildCatalog()
	empty := node(t, cat, "i-empty", "m7i.large")
	full := node(t, cat, "i-full", "m7i.large", testPod("f1", 500, 1024), testPod("f2", 500, 1024))
	cluster := NewCluster([]NodeSnapshot{empty, full}, cat, 10)
	// The only other node is cordoned, so nothing can receive a pod.
	if err := cluster.Taint("i-empty"); err != nil {
		t.Fatal(err)
	}

	if err := cluster.Evict("i-empty", ""); err != nil {
		t.Errorf("evicting from an empty node: %v, want a no-op", err)
	}
	if err := cluster.Evict("i-full", ""); err != nil {
		t.Fatalf("evict: %v", err)
	}
	if len(cluster.Pending) != 1 || len(cluster.Nodes["i-full"].Pods) != 1 {
		t.Errorf("after one eviction with no room: %d pending, %d on the source; want 1 and 1",
			len(cluster.Pending), len(cluster.Nodes["i-full"].Pods))
	}
	if err := cluster.Delete("i-full"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(cluster.Pending) != 2 {
		t.Errorf("after deleting a node with one pod left and no room: %d pending, want 2", len(cluster.Pending))
	}
}
