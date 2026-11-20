# karpenter-move-sim

A simulator for Karpenter consolidation decisions, structured as four stages, built to test whether the first two can work without reading cluster state.

Compiles against upstream Karpenter's own types, pinned to release v1.15.1. Balanced consolidation ([#2962](https://github.com/kubernetes-sigs/karpenter/pull/2962)) ships in tagged releases from v1.14.0 on.

```
go test ./...
go run ./cmd/simrun -nodes 200 -seed 7
```

## What it is for

Karpenter holds a consolidation decision for fifteen seconds, then asks whether it is still good by recomputing the cluster's scheduling from scratch and requiring the same answer. On a cluster with several NodePools, churn in one pool changes what that recomputation returns, so a correct move about a node in another pool is discarded and recomputed, indefinitely. An investigation of a production cluster recorded that loop running thirty to sixty minutes while the target nodes stayed consolidatable throughout.

The observation this simulator explores: cost is smooth and constraints are not. A move's savings drift continuously under pod churn. Whether pods fit on a node is yes or no, `maxSkew` is a cliff, a PodDisruptionBudget's `minAvailable` is a step. Today's validator represents neither separately, so a change worth cents and a change that makes the move illegal are equally fatal.

So grade the smooth quantity and gate the sharp ones. A move becomes a proposal carrying its score and its preconditions, and the preconditions include the score, because a move that got worse should not fire.

## The stages

| Stage | Reads | Cluster access |
|---|---|---|
| 1. Candidate identification | one node's pods, its price, its NodePool config | no |
| 2. Move generation | the candidate, the instance catalog, pool totals | no |
| 3. Planning and budgets | budget consumed, in-flight proposals | yes |
| 4. Execution | live cluster at apply time | yes |

The claim is in the signatures. Neither `Identify` nor either `Generate` accepts a `context.Context`, a `client.Client`, or a cluster handle, so they cannot read global state and the compiler enforces it. `TestStageSignaturesTakeNoClusterHandle` pins the three parameter lists by reflection and rejects a context, client, `*Cluster`, `*NodeReader`, `state.Cluster` or function anywhere in an argument's own fields.

One package-level value is reachable from stages 1 and 2: `evictionCostCtx` in `disruption.go`, the `context.Context` upstream's `EvictionCost` requires since v1.15. It carries only upstream's default feature gates (`options.DefaultFeatureGates()`), which decide whether `pod-deletion-cost` counts, and no cluster state.

```go
func Identify(nodes []NodeSnapshot, totals PoolTotals, k float64) []bool
func (g ConsolidationGenerator) Generate(n NodeSnapshot, dests []NodeSnapshot, cat *Catalog, totals PoolTotals) []Proposal
func (g ProvisioningGenerator) Generate(pending []*corev1.Pod, cat *Catalog) []Proposal
```

Stages 3 and 4 hold every time-varying input. That is where the constraint checks live, because a constraint is what flips rather than drifting.

## What it reproduces

The scoring matches upstream's `ScoreMove` and `ComputeRescheduleDisruptionCost` to 1e-9 across every scenario in upstream's own `balanced_scoring_test.go` and `balanced_adversarial_test.go`. `TestScoreMatchesUpstream` imports `sigs.k8s.io/karpenter/pkg/controllers/disruption` and runs all 31 scenarios (26 and 5, counting each `DescribeTable` entry) through both, asserting agreement on the score to 1e-9 and on approve/reject exactly. Each row cites the upstream line it reproduces. That includes the guards: a move with zero disruption scores +Inf and is approved, and a move with negative savings scores 0.

There is one intended difference. Upstream's `ResolveOfferingPrice` maps a NaN or unresolvable node price to 0 and scores on. The simulator refuses a NaN, infinite, zero or negative price outright (`Totals` and the fixture generators panic, `TestNonFinitePriceRejected`), because a silently zero-priced node understates the pool total and inflates every score in it. The three scenarios that feed upstream such a price assert the panic and check the rest of upstream's numbers.

The simulator has no `ConsolidationPolicy` and scores every pool as Balanced. Upstream's scenarios about non-Balanced pools check the Balanced part and say what they skip.

It reproduces the design's central worked example. Two nodes, identical savings, identical pod counts. One holds stateless proxies, the other holds model servers carrying maximum `pod-deletion-cost`. Node A scores 2.2292 against the design's 2.24 and is approved; node B scores 0.2876 against 0.29 and is rejected. Savings-versus-pod-count cannot produce that.

## What it measures

**Constraint locality, in nodes read per decision.** From `TestNodeReadsPerCheck`:

```
constraint                      N=10  N=100  N=1000
anti-affinity (hostname)           1      1       1
affinity (hostname)                1      1       1
PDB (status field)                 1      1       1
anti-affinity (zone key)          10    100    1000
topology spread (zone)            10    100    1000
PDB (availability recomputed)     10    100    1000
```

Locality belongs to the topology key rather than the constraint kind. A hostname-scoped anti-affinity term reads the destination node. The same term at a zone key reads every node, because a node's domain is a label and there is no way to know which nodes share a domain without looking. PodDisruptionBudget is local because `DisruptionsAllowed` is already in its status; the recompute path costs N reads and the PDB controller pays it, not the eviction path.

Topology spread is the only one of the four with no status field standing in for the distribution.

**Stage 1's reduction.** On the fixture `simrun -nodes 200 -seed 7` builds, at the default k=2 it keeps 149 of 200 nodes. Across k: 30.5% at k=1, 65% at 1.5, 74.5% at 2, 81% at 3. `TestStage1Reduction` asserts these counts. This is a soundness filter and not a search-space reduction. The delete ratio bounds a node's best possible move, so a node below the threshold cannot produce a passing one, and the rest genuinely could.

## Fixtures

`GenCluster` builds a loose cluster: pods drawn log-normally, packed first-fit-decreasing onto a real instance catalog, then a fraction of nodes thinned to create consolidation work. Seeded, reads no clock, same seed gives the same cluster.

`GenTightCluster` builds the case the loose fixture cannot reach. Eleven nodes at 99% CPU plus one oversized node, where aggregate free capacity across the others is 220m CPU against the 7000m the oversized node's pods need. No delete is feasible, so replace is the only cost-saving move. The constructor calls `aggregateRoom` itself and panics if a delete would be possible, so the premise is checked rather than assumed.

## Known gaps

`GangIntact` is unimplemented, because nothing in the model carries a workload's minimum simultaneous member count. It is excluded from the locality table rather than counted as cheap.

Multi-node consolidation is not built. Upstream attempts multi-node before single-node, and that is where a search-space reduction would come from.

The compactor detects two proposals removing the same node. It cannot detect two proposals planning against the same free capacity on the same destination, because a proposal records what it removes and what it launches but nothing about the destinations its generator counted on. Those surface at apply time as a `PodsStillFit` failure.

Placement is first-fit over sorted node IDs, not the Kubernetes scheduler. Adequate for measuring stage costs, wrong for anything claiming placement fidelity.

## Reproducibility

Proposal IDs are derived from content (`del/i-000042`, `rep/i-000042/c7i.large`) rather than generated randomly. They are the compactor's tie-break, and most proposals in a run sit in a score tie, so random IDs made a seeded run produce different results. `TestRunReproducibleUnderFixedSeed` runs one seed four times and compares decisions exactly and money to 1e-9, since summing over a map moves the last bits.
