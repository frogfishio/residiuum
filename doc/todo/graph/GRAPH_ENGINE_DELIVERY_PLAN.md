# Residiuum Graph Engine delivery plan

Status: **staged destination plan v0.1; implementation held by critical path**

Date: 2026-08-12

Normative destination:
[GRAPH_ENGINE_SPEC.md](./GRAPH_ENGINE_SPEC.md)

GRF-0/GRF-1 execution contract:
[GRF01_DEVELOPER_HANDOFF.md](./GRF01_DEVELOPER_HANDOFF.md)

Execution authority:
[CRITICAL_PATH.md](../../../CRITICAL_PATH.md)

## 1. Delivery objective

Deliver useful graph capability early while every stage remains a strict subset
of the final architecture.

The first consumer is a package/module/source analyzer. It must be able to
create a durable graph, publish immutable analysis generations, inspect exact
incoming/outgoing relationships and then gain native bounded traversal without
migrating its data model or replacing its client.

The destination is the complete property-graph engine in the normative spec.
The programme does not wait for every analytic, distributed and standards
feature before the first application uses it.

## 2. Priority and admission

This plan does **not** amend the current critical path. As of 2026-08-12:

```text
RQL checkpoint preserved
        |
        v
Atomics active
        |
        v
remaining RQL gate
        |
        v
Cluster
```

Graph implementation is not admitted by the existence of this plan. The
principal may later admit a bounded graph package by dated amendment. Until
then, only specification correction and non-invasive design validation are
allowed.

No graph package may quietly consume the Atomics, RQL or Cluster delivery lane.
In particular:

- `GRF-2+` changes the query surface and requires explicit RQL admission;
- `GRF-1` record-state publication and all later graph mutation require
  accepted LocalHeap Atomics;
- `GRF-6` distributed execution requires the relevant accepted Cluster
  snapshot/coverage profile; and
- Graph work cannot be used to declare the parked RQL gate accepted.

## 3. Current baseline and total delta

### 3.1 Reusable baseline

| Existing capability | Graph use |
|---|---|
| one physical async `Client`, multiple `HeapClient`s | correct graph client ownership and authorization topology |
| authoritative versioned collection records | vertex, edge, definition and generation authority |
| version-bearing point reads and scans | CAS-safe graph administration and mutation inputs |
| exact scan/index building primitives | inputs to graph adjacency construction, not graph coverage authority |
| RQL/SDA total value semantics | graph property predicates and result shaping |
| canonical QVM and one runtime law | sole home for traversal and graph algorithms |
| RQL coverage, budgets, explain and pages | foundation for graph honesty and bounded operation |
| immutable Heap identity and capabilities | graph confinement |
| Recovery Shadow and salvage scanner | authoritative vertex/edge survival |
| key-local CAS and operation replay | reservation and member retry primitives |
| Atomics developer-ready design | GRF-1 entry dependency for graph publication; later strict compound mutation |
| Direct Access/rank-select designs | high-degree adjacency and materialized result navigation |
| cluster partition/coverage design | future distributed graph execution |

### 3.2 Missing work

| Area | Missing |
|---|---|
| Model | graph definitions, typed identities, canonical vertex/edge mapping, path values |
| Client | `HeapClient::graph`, graph admin, point/adjacency/bulk APIs, typed results |
| Query | graph logical algebra, RQL grammar, bytecode instructions, runtime state |
| Correctness | independent graph oracle, corpus, formal claims and differential suite |
| Physical | adjacency lifecycle, graph statistics, GraphPack, deltas and compaction |
| Paths | path modes, regular patterns, shortest/weighted/k-shortest semantics |
| Mutation | strict endpoint assertions, graph constraints, deletion policies, cascades |
| Scale | bounded spill, resumable workspaces, materialized graph views |
| Analytics | accepted exact/approximate algorithm profiles |
| Distribution | partitioned frontier exchange, snapshot binding and coverage merge |
| Ecosystem | wire negotiation, import/export, optional GQL/PGQ frontend, tooling |
| Proof | graph-specific recovery, damage, concurrency, fuzz and benchmark evidence |

The difference between “we have records and adjacency access paths” and the gold-standard
engine is therefore substantial. The first useful slice is small because it
reuses the baseline; the complete destination remains a multi-release,
multi-engineer programme.

## 4. Non-negotiable staging laws

Every package obeys these laws:

1. **Final identities from day one.** Early vertices, edges and graph
   definitions use the canonical final model.
2. **One query engine.** Native traversal starts only as QVM instructions; no
   temporary SDK/server executor.
3. **One authority system.** Records are authoritative; all adjacency and
   analytics structures are rebuildable.
4. **One connection topology.** `Client -> HeapClient -> GraphClient`; a graph
   never opens another physical store or scheduler.
5. **Async only.** No synchronous compatibility surface.
6. **Bounded from the first recursive query.** Depth, examined vertices/edges,
   output, workspace and time have effective ceilings.
7. **Coverage is part of the answer.** Index damage, lag or budget exhaustion
   cannot become false absence.
8. **Capability honesty.** Negotiation advertises only accepted profiles.
9. **Strict refusal.** Later syntax/opcodes received by an earlier server return
   a stable unsupported-profile error.
10. **No premature optimization dependency.** Semantic profiles work through a
    qualified baseline before GraphPack or distributed accelerators are needed.

## 5. Package and profile map

```text
GRF-0  model/protocol/oracle freeze
  |
  v
GRF-1  early GraphClient + exact adjacency + immutable generations
  |
  v
GRF-2  native bounded traversal in QVM             <-- first graph-engine use
  |
  +-----------> GRF-3 patterns + paths
  |                    |
  +-----------> GRF-4 GraphPack + optimizer + spill
                       |
                       v
             GRF-5 Atomics-backed integrity/mutation
                       |
                       v
             GRF-6 analytics + materialized views
                       |
                       v
             GRF-7 distributed graph execution
                       |
                       v
             GRF-8 standards/ecosystem/gold qualification
```

`GRF-3`, `GRF-4` and selected `GRF-5` preparation can overlap after `GRF-2`
when dependencies and review capacity permit. Acceptance order remains explicit.

| Package | Accepted capability profile | Rough effort* | First consumer value |
|---|---|---:|---|
| GRF-0 | `residiuum-graph-model-v1` | 3–5 person-weeks | stable schema/codec and test oracle |
| GRF-1 | `residiuum-graph-client-v0.1` | 5–8 person-weeks | ingest, publish, point and one-hop code graph |
| GRF-2 | `residiuum-graph-traversal-v0.1` | 8–12 person-weeks | native dependency closure and impact traversal |
| GRF-3 | `residiuum-graph-path-v0.1` | 10–16 person-weeks | patterns, cycles, explanations, shortest paths |
| GRF-4 | `residiuum-graph-physical-v1` | 10–18 person-weeks | GraphPack, spill and competitive traversal cost |
| GRF-5 | `residiuum-graph-integrity-v1` | 8–14 person-weeks | safe incremental graph maintenance |
| GRF-6 | `residiuum-graph-analytics-v1` | 18–30 person-weeks | algorithms and durable analytic projections |
| GRF-7 | `residiuum-graph-cluster-v1` | 20–36 person-weeks | partitioned traversal and analytics |
| GRF-8 | `residiuum-graph-gold-v1` | 12–24 person-weeks | ecosystem surface and complete qualification |

\*Planning ranges for graph-specific work by engineers already familiar with
the codebase. They exclude unfinished Atomics/RQL/Cluster prerequisites,
product interrupts, external audit and broad bindings. They are sizing tools,
not delivery promises. The full destination is intentionally a multi-person-
year class of product work; useful source-analysis capability is not.

## 6. Ownership boundaries

Target module ownership:

| Owner | Responsibility | Forbidden responsibility |
|---|---|---|
| `residiuum-graph` pure crate | identities, definitions, canonical plans, path/algorithm profiles, codecs, oracle model | files, network, SDK handles, store IO |
| `residiuum-sda` | property values, paths and total predicates | graph IO or traversal scheduling |
| QVM module | graph bytecode compile/runtime semantics | physical file layout |
| `residiuum-store` | exact adjacency host ops, builds, GraphPack, deltas, recovery | graph query meaning |
| `residiuum-sdk::driver::graph` | async typed client, admission, streams, jobs | alternate traversal executor |
| `residiuum-server` | authorized wire adapter and resource policy | second planner/runtime |
| `residiuum-examine` | graph salvage/evidence projections | declaring uncertain data complete |
| `residiuum-cluster` | partition routing/exchange and distributed coverage | alternate graph semantics |
| independent graph model | slow oracle and history/result checker | product execution path |

Crate placement for GRF-0/GRF-1 is frozen by the developer handoff. Later
package placement remains subject to dependency review. The ownership law is
fixed even if a later internal module name changes.

## 7. GRF-0 — model, protocol and oracle freeze

### Purpose

Freeze the final conceptual model before an application writes graph data.

### Deliverables

- canonical `GraphId`, definition revision, vertex/edge type and logical
  reference types;
- deterministic GraphDefinition codec and domain-separated hashes;
- canonical property-graph and path values;
- final authoritative edge endpoint representation;
- semantic definitions for direction, multiplicity, path modes and coverage;
- graph capability/profile negotiation types;
- stable result/evidence/error envelopes;
- independent in-memory reference model for vertex/edge/adjacency operations;
- initial chain/star/cycle/parallel-edge/damage corpus;
- hostile decode corpus and size/depth/count limits;
- graph claim registry entries `G-AUTH-1` through `G-COV-1`; and
- schema examples for the source-analysis journey.

### Required decisions closed here

- exact canonical field numbers and maximum encoded sizes;
- edge endpoint format and label normalization;
- logical identity ordering;
- whether labels are static, derived or both in the first profile;
- graph definition authorization and revision selection;
- adjacency coverage-domain representation; and
- public semver/capability names.

### Exit

- accepted/rejected golden byte fixtures are stable;
- alternate insertion orders produce the same definition/plan hash;
- cross-Heap identities are rejected;
- oracle handles the complete GRF-1 model;
- every semantic term used by GRF-1 is defined once; and
- architecture review confirms no migration is required for GRF-2+.

### Non-claim

No graph API or query exists at GRF-0.

## 8. GRF-1 — early GraphClient and exact adjacency

Accepted profile: `residiuum-graph-client-v0.1`

Entry prerequisite: embedded LocalHeap `residiuum-atomic-v1` has passed ATM-5
and the driver truthfully advertises `Capabilities::atomics`.

### Purpose

Give the package analyzer a real, final-shape client as early as possible,
without pretending that one-hop access is a native traversal engine.

### Product surface

```text
HeapClient::graph(name/revision) -> GraphClient

GraphClient:
  definition()
  vertex(id)
  edge(id)
  vertices(page/filter subset)
  edges(page/filter subset)
  neighbors(vertex, direction, edge_types, page)
  bulk_generation(job_id, manifest)
  validate_generation(job_id)
  activate_generation(expected_root_version, generation)
  generation_status(job_id)
```

All methods are async, bounded and coverage-bearing. Values include establishing
record versions. `neighbors` is one hop only and pages in canonical edge
identity order.

### Storage shape

- graph definition registry in the Heap;
- authoritative vertex and edge collections under the final model;
- exact outbound and inbound generation-scoped adjacency artifacts;
- immutable application generation field/root;
- stable bulk job manifest, restart cursor and rejection report; and
- no GraphPack dependency.

### Source-analysis acceptance journey

1. Bind one physical `Client` to the authorized Heap.
2. Register the `code` graph definition.
3. Stream one workspace analysis into immutable generation `G42`.
4. Resume correctly after cancellation/restart with the same job/manifest.
5. Validate endpoints and index coverage.
6. CAS `active_generation` from `G41` to `G42`.
7. Read a module, its imports and its direct dependants.
8. Reopen the deployment and receive identical results and versions.
9. Demonstrate that another Heap or graph binding cannot observe them.
10. Damage one derived adjacency region and receive explicit incomplete
    coverage or an authoritative fallback, never an empty-complete answer.

### Tests and evidence

- embedded/remote contract tests where the remote profile is in scope;
- pagination concatenation for high-degree vertices;
- duplicate edge, parallel edge and self-loop cases;
- absent endpoint and generation mismatch refusal;
- lost-reply/retry for generation activation;
- bulk load crash at every persisted phase;
- delete/rebuild both adjacency artifacts from authority;
- memory/queue bounds under producer and consumer pressure; and
- API documentation with no raw store handle escape.

### Exit and claim

Allowed claim:

> Residiuum can durably store a property graph and provide exact, bounded point
> and one-hop navigation through an async Heap-bound client.

Forbidden claims: recursive traversal, path queries, cycle detection, graph
algorithms, strict incremental referential integrity or GQL compatibility.

### First-use rule

The source analyzer should begin production feedback here if point and one-hop
navigation are useful. Its own domain code may compose calls temporarily, but
the SDK MUST NOT publish that loop as `traverse`, `reachability` or a complete
graph answer. Native recursive semantics arrive in GRF-2.

## 9. GRF-2 — native bounded traversal in QVM

Accepted profile: `residiuum-graph-traversal-v0.1`

### Purpose

Answer the first decisive graph questions inside Residiuum:

- what does this module depend on within `N` hops?
- what depends on this package within `N` hops?
- which files/symbols are affected by this change?
- return one canonical explanation path for each reached target.

### Semantic subset

- one seed set from IDs or an ordinary RQL vertex filter;
- outgoing, incoming and both-direction expansion;
- edge type/label and SDA property predicates;
- finite depth range with policy ceiling;
- `walk`, `trail` and `simple` modes;
- BFS visitation and deterministic canonical result order;
- emit reached vertices, traversed edges, depth and one canonical discovery
  path;
- complete/allow-incomplete coverage;
- full graph budgets, deadline and cancellation;
- explain and execution counters; and
- bounded pages/streaming using a resumable workspace where necessary.

### Implementation

- extend the canonical logical plan;
- freeze the first graph QVM instruction bytes and golden vectors;
- add data-only adjacency/get host capabilities;
- implement visited/frontier/path state in the one QVM runtime;
- add a deliberately slow independent traversal oracle;
- add RQL text grammar and Rust builder lowering to the same plan;
- add embedded SDK and negotiated remote execution;
- enforce Heap/read-view binding and authoritative candidate revalidation; and
- add the QVM architecture gate to detect SDK/store traversal semantics.

### Exit

- exhaustive small-graph equivalence for all path modes and directions;
- randomized differential tests against the oracle;
- metamorphic direction reversal, label renaming and disconnected-union laws;
- identical result for scan adjacency and exact-index adjacency;
- page concatenation equals uninterrupted execution;
- cancellation/restart and workspace expiry are honest;
- every budget counter has positive and negative controls;
- damage at any frontier cannot prove false unreachable; and
- source-analysis dependency closure passes on a larger-than-memory corpus.

### Claim

> Residiuum supports deterministic, resource-bounded native graph traversal
> with exact read-view and coverage semantics.

This is the first release that may call itself a graph query engine.

## 10. GRF-3 — patterns, regular paths and shortest paths

Accepted profile: `residiuum-graph-path-v0.1`

### Deliverables

- multi-variable fixed graph patterns;
- shared-variable joins, existence, anti-existence and optional match;
- regular path expressions compiled to finite automata;
- named path variables and path predicates;
- `any shortest`, bounded `all shortest`, bounded `k shortest`;
- unweighted and non-negative weighted shortest path;
- deterministic tie-breaking and exact numeric weight profile;
- cycle witnesses and topological-order query forms;
- graph aggregation over bindings and paths;
- optimizer start-node/join-direction selection; and
- explain of normalized automaton and path proof obligations.

### Source-analysis value

- detect and explain dependency cycles;
- find shortest import/reference chain;
- match package → module → file → symbol patterns;
- prove absence of a forbidden dependency when coverage is complete;
- return deterministic impact explanations; and
- group dependency relationships by package, owner or edge type.

### Exit

- automaton/product-graph oracle equivalence;
- equal-cost and parallel-edge shortest-path corpus;
- anti-match refuses incomplete absence proof;
- fixed-pattern join reordering equivalence;
- weighted-path invalid/overflow cases;
- stable plans and results across parallel schedules; and
- controlled comparisons against at least one established graph system with
  fully disclosed semantics/indexes.

### Non-claim

GRF-3 does not yet promise competitive whole-graph analytics or distributed
execution. Baseline exact indexes may remain the physical path.

## 11. GRF-4 — GraphPack, optimizer and external memory

Accepted profile: `residiuum-graph-physical-v1`

### Purpose

Make the accepted semantics competitive without making performance structures
authoritative.

### Deliverables

- immutable checksummed outbound/inbound GraphPacks;
- dense build-local ordinals with logical-ID revalidation;
- chunked high-degree adjacency and rank/select navigation;
- bounded delta runs, catch-up frontier and pack compaction;
- degree/label/property/correlation statistics;
- cost-based seed, direction, join and expansion planning;
- late property/document materialization;
- deterministic spill/frontier/priority-queue formats;
- resumable workspace publication and quota/expiry;
- at least one advanced accelerator with a full coverage proof (candidate:
  SCC condensation or landmark distances); and
- build/rebuild/verify/retire admin surfaces and OpenReport phases.

### Exit

- delete-all-derived-and-rebuild equivalence;
- crash matrix through build/catch-up/publish/compact/retire;
- bit flip and missing block cause local honest degradation;
- GraphPack/index/authoritative paths equal the oracle;
- in-memory and external-memory runs are identical;
- bounded high-degree pages avoid one unbounded allocation;
- open with current metadata does no full-graph scan;
- build and catch-up debt remain bounded under mixed updates; and
- published performance envelopes show where GraphPack wins or is refused.

### Performance gate

Use chain, star, scale-free, cyclic, dense and high-diameter shapes at multiple
scales. Measure cold/warm p50/p95/p99, examined edges per result, physical bytes,
CPU, memory, spill, build cost and update catch-up. Do not compare a warm
GraphPack to an unindexed competitor or hide durability/snapshot differences.

## 12. GRF-5 — Atomics-backed graph integrity and mutation

Accepted profile: `residiuum-graph-integrity-v1`

Prerequisite: accepted one-Heap Atomics profile adequate for the declared
transition.

### Deliverables

- async typed create/replace/delete vertex/edge builders;
- strict endpoint assertions in one Atomic;
- exact operation/Atomic status and retry semantics;
- unique edge and bounded cardinality constraints;
- `Restrict`, `Detach` and durable `CascadeJob` deletion profiles;
- adjacency/materialization obligations recorded with authoritative mutation;
- `current` wait/fallback for derived catch-up;
- bounded incremental generation changes;
- constraint validation jobs and activation gates; and
- graph mutation history checker linked to Atomics evidence.

### Exit

- concurrent edge-create/delete/endpoint-change histories are serializable;
- stale CAS creates no partial graph mutation;
- lost reply resolves by stable Atomic identity;
- crash before/after decision exposes old or new whole transition;
- incomplete adjacency cannot authorize `Restrict` or uniqueness proof;
- cascade restarts without double delete or premature vertex removal;
- graph queries observe one whole Heap publication generation; and
- compaction/salvage preserve Atomic graph decision evidence.

### Claim

> Residiuum can maintain declared property-graph integrity through bounded
> serializable async transitions and durable managed jobs.

## 13. GRF-6 — analytics and materialized graph views

Accepted profile: `residiuum-graph-analytics-v1`

### Delivery waves

#### GRF-6A — structural exact algorithms

- weak/strong connected components;
- cycle witnesses and topological ordering;
- degree, bridges and articulation vertices;
- minimum spanning forest; and
- durable component/condensation materialized views.

#### GRF-6B — paths and networks

- single-source and admitted all-pairs shortest paths;
- `k` shortest simple paths;
- dominators;
- maximum flow/minimum cut; and
- bipartite matching.

#### GRF-6C — graph analytics

- PageRank and personalized PageRank;
- exact/approximate centralities;
- triangle/clustering metrics;
- `k`-core;
- deterministic label propagation and community profile; and
- incremental refresh where the proof and cost justify it.

### Acceptance per algorithm

No family is accepted en bloc. Every algorithm has:

- versioned mathematical definition;
- input/type/weight constraints;
- exact or approximation declaration;
- deterministic tie/convergence rules;
- independent oracle or trusted reference vectors;
- in-memory/external-memory equivalence;
- damage/coverage semantics;
- resource envelope and adversarial shapes; and
- result/materialized-view publication recovery tests.

## 14. GRF-7 — distributed graph execution

Accepted profile: `residiuum-graph-cluster-v1`

Prerequisite: accepted Cluster partition, read-view, continuation and coverage
semantics for every graph operation in scope.

### Deliverables

- graph-aware partition statistics and placement;
- derived inbound/outbound adjacency obligations across partitions;
- deterministic frontier routing and deduplication;
- bulk-synchronous traversal/algorithm supersteps;
- authenticated coordinator/workspace failover;
- partition-local GraphPacks and remote block avoidance;
- deterministic global shortest/aggregate/convergence merge;
- rebalance interaction with pinned graph read views;
- complete partition/frontier/derived coverage evidence; and
- bounded network, message, superstep and straggler admission.

### Exit

- complete distributed results equal the single-node oracle;
- missing partitions never become empty adjacency;
- coordinator failure resumes or honestly restarts at the same view;
- rebalance does not mix epochs or duplicate/lose graph members;
- network partitions cannot fabricate convergence/shortest/absence;
- load-skew/supernode cells have bounded queue and memory behavior; and
- scale-out gains are published together with coordination/network costs.

## 15. GRF-8 — standards, ecosystem and gold qualification

Accepted profile: `residiuum-graph-gold-v1`

### Deliverables

- published graph semantic/capability matrix;
- optional GQL and/or SQL/PGQ frontend subset with strict diagnostics;
- stable import/export for graph definitions and data;
- administration, inspect, doctor, salvage and visualization integration;
- remote protocol compatibility matrix and rolling-upgrade behavior;
- language bindings selected by actual consumer demand;
- complete security and multi-tenant side-channel review;
- long soak, chaos, crash, corruption and upgrade campaigns;
- LDBC-shaped Interactive and BI qualification with full disclosures;
- formal-claim closure for accepted profiles; and
- operator runbooks, limits, sizing and incident diagnostics.

### Gold exit

The complete product claim in the normative spec is permitted only when:

1. all capabilities advertised as `graph-gold-v1` are accepted;
2. unsupported standard features have stable refusals;
3. point, traversal, path, analytics, mutation, recovery and distributed
   evidence are reproducible;
4. no derived graph structure is required for authority or salvage;
5. exactness, approximation and incomplete coverage are distinct end to end;
6. production SDKs expose the same connection/Heap/graph topology;
7. performance and cost envelopes are published, including weak cells; and
8. architects sign the conformance and non-claim statement.

## 16. Earliest source-analysis product slice

The recommended cut is deliberately narrow.

### At GRF-1

The application can:

- declare the code graph once;
- ingest packages/modules/files/symbols and typed edges;
- publish a whole immutable scan generation;
- fetch versioned vertices/edges;
- page exact direct imports and reverse dependants;
- inspect rejected/dangling input and coverage; and
- reopen safely after kill/crash.

This is enough to begin collecting real graph shape, fan-out, update and query
telemetry and to harden the client.

### At GRF-2

It additionally gains:

- bounded transitive dependency closure;
- reverse impact traversal;
- canonical explanation paths;
- server-enforced budgets and coverage; and
- native streaming rather than application orchestration.

This is the recommended first end-user graph feature release.

### At GRF-3

It gains cycles, shortest explanations and multi-entity patterns. GRF-4 then
makes those operations economical on much larger graphs. GRF-5 permits safe
fine-grained incremental graph maintenance instead of generation replacement.

## 17. Pull order and PR discipline

Within an admitted package, pull requests are ordered:

1. semantic types, limits and stable errors;
2. independent oracle and accepted/rejected fixtures;
3. canonical plan/bytecode vectors;
4. baseline host/storage path;
5. one runtime integration;
6. async SDK and wire adapter;
7. recovery/damage/failpoint evidence;
8. performance/operational evidence; and
9. capability flag plus documentation last.

The capability flag is false until the package exit review. A performance
optimization cannot merge before baseline oracle equivalence exists. A public
API cannot merge before its stable error, cancellation, retry, version and
coverage behavior are specified.

## 18. Review gates

Every package receives four independent review decisions:

| Gate | Question |
|---|---|
| Architecture | Does it remain inside the final model, one QVM and correct ownership boundaries? |
| Correctness | Does oracle/differential/formal evidence establish the claimed semantics and negative controls? |
| Operations | Are bounds, cancellation, recovery, damage, telemetry and upgrade behavior explicit? |
| Product | Is the capability useful through the async client and described without overclaim? |

Failure of any gate keeps the capability unadvertised. A waiver must be a dated
principal amendment naming the weakened claim; silence is not a waiver.

## 19. Stop/go points

The programme deliberately checks value before funding the entire destination:

- after `GRF-1`: is the source graph model/client stable under real ingestion?
- after `GRF-2`: are bounded graph queries useful and within the performance
  ballpark?
- after `GRF-3`: do native patterns/paths remove meaningful application work?
- after `GRF-4`: does physical specialization justify analytics investment?
- after `GRF-6`: is distributed graph execution demanded by real graph size or
  deployment shape?

A stop decision parks later packages; it does not invalidate the earlier
accepted profile. That is the principal benefit of strict conformance staging.

## 20. Immediate next action when Graph is admitted

Do not begin with BFS code. Claim `GRF-0` and produce:

1. the canonical graph-definition and identity codec;
2. the final source-analysis edge/vertex record examples;
3. the independent graph oracle and tiny adversarial corpus;
4. capability/profile and error types; and
5. an architect review pack proving GRF-1 data will remain valid through the
   gold destination.

Only after that review should developers expose `HeapClient::graph`.
