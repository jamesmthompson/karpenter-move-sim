package sim

import (
	"fmt"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// All-or-nothing apply semantics for a proposal's launch set.
//
// Omega (Schwarzkopf et al., EuroSys 2013) offers two transaction modes over
// resource claims and names the second's use case: "schedulers typically choose
// to use incremental transactions, which accept all but the conflicting changes
// (i.e., the transaction provides atomicity but not independence). A scheduler
// can instead use an all-or-nothing transaction to achieve gang scheduling."
//
// Incremental is what this simulator has always done and stays the default.
// All-or-nothing is Proposal.AtomicLaunch, opt in per proposal, for a launch set
// where a partial result is worse than no result: four of five nodes holds five
// nodes' worth of intent and delivers nothing.
//
// Upstream cannot express it. Provisioner.CreateNodeClaims takes a slice and
// creates each claim independently, so a partial result holds capacity with no
// way to say the five belong together and no way to release the ones that landed.
//
// What makes this a transaction and not the rollback model that was removed from
// this simulator: the released nodes are nodes no eviction was ever allowed to
// target. The launch set completes before the first Evict fires, so a launch that
// has to be released has had no observable effect. The four-node state is never
// committed rather than committed and reversed. Nothing compensates for an
// eviction, because an eviction cannot be compensated for.

// LaunchFailure is the cloud provider refusing a launch. It is a distinct type so
// the executor can tell a provider refusing capacity, which an atomic proposal
// answers by releasing what it got, from a fault in the mechanism, which it
// answers by stopping the pass.
type LaunchFailure struct {
	InstanceType string
}

func (e *LaunchFailure) Error() string {
	return fmt.Sprintf("launch %s: no capacity", e.InstanceType)
}

// AcquireNodes builds an all-or-nothing acquisition: N nodes with the given
// instance types, all or none, evicting nothing and deleting nothing.
//
// This is the shape a workload needing N simultaneous members asks for. It
// carries no Candidates, so it takes no disruption budget and charges nothing to
// release, which is the accounting that makes abandonment free. Cost is the sum
// of the offerings' prices, so the compactor ranks it against other provisioning
// proposals the way it ranks a single launch.
func AcquireNodes(id string, its ...*cloudprovider.InstanceType) Proposal {
	actions := make([]Action, 0, len(its))
	cost := 0.0
	for _, it := range its {
		actions = append(actions, Action{Kind: Launch, InstanceType: it})
		if price, ok := cheapestOffering(it); ok {
			cost += price
		}
	}
	return Proposal{
		ID:           id,
		Replacements: its,
		Cost:         cost,
		Actions:      actions,
		AtomicLaunch: true,
		// Same preconditions a single-launch provisioning proposal declares. A
		// launch destroys nothing, so there is nothing to gate but the budget, and
		// capacity vanishing by apply time surfaces as a launch failure.
		Preconditions: []PreconditionKind{BudgetHasRoom},
		Reason:        ReasonProvisioning,
	}
}

// validateAtomicLaunch checks that the proposal's DAG lets the launch set finish
// before anything irreversible starts. Every Evict and every Delete must depend,
// transitively, on every Launch.
//
// This is a mechanism check and not a precondition. A proposal that asks for
// all-or-nothing while permitting an eviction to fire alongside its launches is
// asking for something the executor cannot deliver: once a pod is evicted there
// is no releasing the launches, so an atomic proposal built that way would be
// silently incremental. A generator that emits one has a bug, so this returns an
// error rather than an outcome.
//
// batchActions already gates every eviction on the launch, for the ordering
// reason a REPLACE needs, so a batch proposal satisfies this without changing
// shape. The check exists to keep it that way.
func validateAtomicLaunch(p Proposal) error {
	launches := map[int]bool{}
	for i, a := range p.Actions {
		if a.Kind == Launch {
			launches[i] = true
		}
	}
	if len(launches) == 0 {
		return nil
	}

	// Transitive dependencies, built forward. Index i may depend only on indices
	// below it, which Apply enforces, so one pass in order is enough.
	deps := make([]map[int]bool, len(p.Actions))
	for i, a := range p.Actions {
		deps[i] = map[int]bool{}
		for _, d := range a.DependsOn {
			if d < 0 || d >= i {
				// Apply reports this as a mechanism fault with a better message.
				continue
			}
			deps[i][d] = true
			for t := range deps[d] {
				deps[i][t] = true
			}
		}
	}

	for i, a := range p.Actions {
		if a.Kind != Evict && a.Kind != Delete {
			continue
		}
		for l := range launches {
			if !deps[i][l] {
				return fmt.Errorf("atomic proposal %s: action %d (%v) does not wait on launch %d, so its launch set cannot be released",
					p.ID, i, a.Kind, l)
			}
		}
	}
	return nil
}

// FailLaunchesOf fails every launch of the named instance types. It is the
// simple form of the injection: a type the cloud provider has no capacity for.
func FailLaunchesOf(names ...string) func(*cloudprovider.InstanceType) bool {
	deny := map[string]bool{}
	for _, n := range names {
		deny[n] = true
	}
	return func(it *cloudprovider.InstanceType) bool {
		return it != nil && deny[it.Name]
	}
}

// FailLaunchAfter succeeds for the first n launch attempts against a cluster and
// fails every one after. It is what forces a partial acquisition, which a
// name-based predicate cannot do when a set asks for N nodes of one type.
//
// The counter is per predicate and not synchronized. Stage 4 applies one proposal
// at a time on one goroutine, and this is a test injection point rather than a
// model of the cloud provider, so a mutex here would be dressing.
func FailLaunchAfter(n int) func(*cloudprovider.InstanceType) bool {
	seen := 0
	return func(*cloudprovider.InstanceType) bool {
		seen++
		return seen > n
	}
}
