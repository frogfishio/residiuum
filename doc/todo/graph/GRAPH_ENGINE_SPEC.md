# Residiuum Graph Engine specification

Status: **normative destination v0.1; no implementation authority**

Date: 2026-08-12

Companion delivery plan:
[GRAPH_ENGINE_DELIVERY_PLAN.md](./GRAPH_ENGINE_DELIVERY_PLAN.md)

Governing specifications:
[RQL_SPEC.md](../../wip/query/RQL_SPEC.md),
[QUERY_BYTECODE_V1.md](../rql/QUERY_BYTECODE_V1.md),
[SDA_SPEC.md](../../reference/query/SDA_SPEC.md),
[HEAP_SPEC.md](../../wip/heap/HEAP_SPEC.md),
[ATOMICS_SPEC.md](../atomics/ATOMICS_SPEC.md),
[RRE_SPEC.md](../rre/RRE_SPEC.md),
[DIRECT_ACCESS_SPEC.md](../direct-access/DIRECT_ACCESS_SPEC.md), and
[CLUSTER_SPEC.md](../cluster/CLUSTER_SPEC.md).

## 1. Purpose and product claim

This specification defines the complete intended graph capability of
Residiuum: a transactional, recoverable, bounded, property-graph engine whose
queries are part of RQL and whose authority remains ordinary Residiuum data.

The eventual product claim is:

> Within one authorized Heap, Residiuum can store and maintain a directed
> property multigraph, execute deterministic pattern, traversal, path and
> analytic queries over a declared read view, report exact coverage and damage,
> and recover authoritative graph data even when every derived graph structure
> is missing.

This is the destination, not the first release. A conforming partial release
implements one named profile from the delivery plan and refuses everything
outside it. A partial implementation MUST NOT weaken semantics, return an
unlabelled approximation or create an incompatible graph representation.

## 2. Governing architecture decisions

The following decisions are closed unless this specification is explicitly
amended.

| Topic | Decision |
|---|---|
| Logical model | directed, labelled property multigraph with stable vertex and edge identities |
| Authority | ordinary authoritative Residiuum records; graph structures are derived accelerators |
| Isolation | exactly one Heap per graph; no traversal can cross a Heap boundary |
| Connection topology | one physical async `Client`, many capability-bound `HeapClient`s, many graph bindings per Heap |
| Query language | graph constructs extend RQL; they do not create a separate product language |
| Execution owner | the one canonical RQL logical-plan → QVM bytecode → QVM runtime path |
| Value semantics | SDA missing/null/value and total predicate semantics |
| Mutation owner | async graph mutation APIs and Atomics; RQL remains read-only |
| Consistency | every query binds a declared Heap read view/frontier and graph-definition revision |
| Damage | absence and completeness are claims supported by explicit coverage evidence |
| Derived state | disposable, versioned, checksummed, rebuildable and never sole authority |
| Recursion | variable-length traversal is permitted only with finite semantic and resource bounds |
| Algorithms | built-in, versioned deterministic kernels invoked by QVM; no user code in the engine |
| Distribution | one logical semantics; partitioning may change execution, never meaning |
| Compatibility | future GQL/PGQ frontends may lower to the same plan; syntax compatibility is not semantic ownership |

### 2.1 One engine, not a graph-shaped sidecar

The only legal shape is:

```text
RQL graph syntax | Rust graph builder | future GQL/PGQ subset
                         |
                         v
              canonical RQL graph plan
                         |
                         v
                     QVM bytecode
                         |
                         v
                  ONE QVM runtime
                    /          \
          graph host data      graph algorithm kernels
          capabilities         called by QVM opcodes
```

The following are forbidden:

- a graph traversal executor in the SDK;
- a separate graph query service with different missing/null or coverage
  semantics;
- storage code that decides traversal, path uniqueness or shortest-path
  meaning;
- an index whose miss is treated as authoritative absence without exact
  coverage;
- an application-only edge format that the future native engine must migrate
  to understand; and
- graph mutation through synchronous or interactive transaction handles.

### 2.2 Standards position

The model and terminology should track the useful common ground of
[ISO/IEC 39075:2024 GQL](https://www.iso.org/standard/76120.html) and
[ISO/IEC 9075-16:2023 SQL/PGQ](https://www.iso.org/standard/79473.html), while
preserving Residiuum's Heap, SDA, recovery and coverage laws. The first
profiles make no standards-conformance claim. A standards frontend is accepted
only through a published feature matrix and strict refusal of unsupported
constructs.

## 3. Scope and deliberate boundaries

The complete destination includes:

- graph definitions over one or more collections in one Heap;
- multiple vertex and edge types, labels and property constraints;
- parallel edges, self-loops and directed/undirected query interpretation;
- fixed and variable-length pattern matching;
- bounded reachability and neighbourhood traversal;
- path selection, enumeration, shortest paths and weighted paths;
- graph aggregation and a versioned algorithm library;
- transactional graph mutation and referential policies;
- bulk generation publication and incremental maintenance;
- exact adjacency structures, path-oriented accelerators and materialized
  graph views;
- bounded streaming, continuation and resumable workspaces;
- single-node, external-memory and distributed execution;
- recovery, salvage, coverage and formal qualification; and
- an async typed SDK, remote protocol and administration surface.

The destination deliberately excludes:

- cross-Heap edges or traversal;
- unbounded server work;
- arbitrary application callbacks inside query execution;
- user-provided native algorithm code in the database process;
- silent best-effort answers;
- graph indexes as sole authority;
- RDF entailment, OWL reasoning and SPARQL compatibility as implicit graph
  features;
- hyperedges as a primitive in v1 of the model; and
- a promise of identical syntax or planner behaviour to another graph product.

An n-ary relation is represented as a vertex plus binary role edges. This
keeps identity, traversal and referential semantics unambiguous and does not
prevent higher-level tooling from presenting a hyperedge abstraction.

## 4. Core data model

### 4.1 Graph definition

A graph is an immutable-versioned interpretation of authoritative collections:

```text
GraphDefinition {
  heap_id: HeapId
  graph_id: GraphId
  revision: GraphDefinitionRevision
  name: String
  vertex_types: Map<VertexTypeName, VertexTypeDefinition>
  edge_types: Map<EdgeTypeName, EdgeTypeDefinition>
  constraints: Seq<GraphConstraint>
  temporal_profile: Optional<TemporalProfile>
  physical_hints: PhysicalHints
  semantic_profile: "residiuum-graph-v1"
}
```

The definition is authoritative administrative data. Revisions are immutable.
Changing label derivation, endpoint fields, property interpretation,
constraints or temporal meaning creates a new revision and a new derived-build
obligation.

### 4.2 Vertex type

```text
VertexTypeDefinition {
  type_id: VertexTypeId
  source_collection: CollectionId
  labels: StaticLabels | LabelsFrom(path)
  identity: DocumentKey
  properties: WholeDocument | Projection
  membership_predicate: Optional<SdaPredicate>
  rre_contract: Optional<ContractRevision>
}
```

A source record is a vertex in a graph read view exactly when:

1. it is live in the bound collection and read view;
2. the vertex-type membership predicate evaluates `true` under SDA total
   semantics; and
3. the required graph-definition and contract revisions accept it.

A record may participate in more than one graph and more than one vertex type.
Its graph identity includes the vertex-type identity, so overlapping mappings
do not alias accidentally.

### 4.3 Edge type

```text
EdgeTypeDefinition {
  type_id: EdgeTypeId
  source_collection: CollectionId
  label: EdgeLabel
  identity: DocumentKey
  from: EndpointDefinition
  to: EndpointDefinition
  properties: WholeDocument | Projection
  membership_predicate: Optional<SdaPredicate>
  endpoint_policy: Strict | Deferred
  deletion_policy: Restrict | Detach | CascadeJob
}
```

An edge record is authoritative. Its source and target are exact canonical
vertex references, either stored directly or derived by a frozen deterministic
projection. Multiple edge records may join the same ordered pair. Self-loops
are legal. Edge identity never collapses merely because endpoints, label or
properties compare equal.

`Strict` endpoint policy requires both endpoints to exist at the Atomic
validation frontier. `Deferred` permits a temporarily unresolved endpoint but
requires it to be surfaced as a graph integrity violation and excludes that
edge from exact traversals unless the query explicitly admits incomplete
coverage.

### 4.4 Identity

Canonical logical identities are:

```text
VertexRef {
  heap_id: HeapId
  graph_id: GraphId
  vertex_type_id: VertexTypeId
  collection_id: CollectionId
  key: DocumentKey
}

EdgeRef {
  heap_id: HeapId
  graph_id: GraphId
  edge_type_id: EdgeTypeId
  collection_id: CollectionId
  key: DocumentKey
}
```

Display names never participate in identity. Canonical encoding is bounded,
deterministic and domain-separated. A reference with a different `HeapId` or
`GraphId` is rejected before access. Physical ordinal IDs may accelerate one
derived build but MUST NOT escape as durable logical IDs.

### 4.5 Labels and properties

- Vertex labels form a finite set of normalized strings for the graph-definition
  revision.
- Every edge has exactly one type identity and one primary label. Alias labels
  may be metadata but do not alter identity.
- Properties are SDA values. `Absent`, `Null`, scalar, sequence, bag and object
  remain distinct.
- A property read from an absent path is `Absent`; it is never coerced to
  `Null`, zero, false or an empty collection.
- Labels and property names are compared under a frozen normalization profile.
- Property indexes are derived and inherit ordinary Residiuum index coverage
  rules.

### 4.6 Path value

A path is an immutable alternating sequence:

```text
Path {
  start: VertexRef
  steps: Seq<PathStep>
  total_cost: Optional<CanonicalNumber>
}

PathStep {
  edge: EdgeRef
  direction: Outgoing | Incoming
  vertex: VertexRef
}
```

Zero-length paths contain a start vertex and no steps. Path equality compares
the entire canonical identity sequence and direction sequence, not rendered
properties. Path properties are projected separately.

## 5. Authority, lifecycle and derived state

### 5.1 Authority law

For any accepted graph answer, the chain of authority is:

```text
verified authoritative vertex/edge revisions
        -> graph-definition revision
        -> read-view/frontier
        -> candidate-producing derived structures
        -> QVM revalidation and graph semantics
        -> result plus coverage evidence
```

Deleting every adjacency structure, statistic, materialized view and algorithm
artifact MUST leave enough surviving authority to rebuild them. A derived
structure may accelerate discovery; it never resurrects a deleted record or
overrides the live authoritative revision.

### 5.2 Graph generations

Residiuum supports two distinct notions which MUST NOT be conflated:

- a **Heap read view** is a database consistency boundary;
- an **application graph generation** is an optional application-owned field
  or root pointer used to publish a coherent imported graph.

Source-analysis tools SHOULD bulk-write an immutable generation, validate it,
then use one Atomic CAS to change `active_generation`. This is available before
general multi-record replacement is economical and provides deterministic
whole-analysis publication. Native graph queries may bind that generation as
an ordinary predicate.

### 5.3 Derived-build lifecycle

Every graph projection follows:

```text
Declared -> Building -> CatchingUp -> Current -> Retiring -> Collected
                      \-> Damaged
                      \-> Refused
```

Each build records:

- Heap, graph and graph-definition revision;
- authoritative source frontiers;
- covered vertex and edge domains;
- encoding and algorithm version;
- immutable segment hashes;
- known holes and rejected source records;
- build/catch-up cursor;
- publication evidence; and
- supersession and collection eligibility.

Publication is atomic. An interrupted build is either unpublished work or a
fully identified published generation; no half-published directory is current.

## 6. Read views, consistency and time

### 6.1 Query binding

Every graph query binds:

```text
GraphReadBinding {
  heap_id
  graph_id
  graph_definition_revision
  heap_read_view
  optional_application_generation
  consistency
  coverage_requirement
}
```

All vertices, edges, property values, index candidates and algorithm inputs in
one answer are interpreted against that binding. A query MUST NOT mix current
vertices with an older adjacency build without authoritative validation and
explicit coverage reconciliation.

### 6.2 Consistency modes

Graph queries inherit RQL's `available` and `current` modes.

- `available` uses currently published sources and derived frontiers and
  reports them.
- `current` waits for required exact graph structures to observe the admission
  frontier, or uses a qualified authoritative fallback, or refuses.

The gold-standard engine also supports a named immutable `read view` for
multi-page and long-running graph work. Read-view lifetime is bounded by policy.
Expiry returns a stable error; it never silently restarts against newer data.

### 6.3 Temporal graph profile

The optional temporal profile interprets frozen property paths as:

- valid-time interval `[valid_from, valid_to)`; and/or
- application transaction-time metadata distinct from Residiuum's physical
  commit frontier.

Temporal predicates become ordinary graph-plan constraints. The database does
not infer missing history. An `as of` query is exact only when authoritative
history and all required temporal index coverage are complete for the requested
interval.

## 7. Graph algebra

The canonical graph plan extends, rather than replaces, `RqlPlanV1` with these
logical operators:

| Operator | Meaning |
|---|---|
| `GraphBind` | bind graph definition, read view and aliases |
| `VertexScan` | enumerate vertices of declared types/labels |
| `EdgeScan` | enumerate edges of declared types/labels |
| `PatternMatch` | match a fixed graph pattern |
| `Expand` | traverse one edge step in a declared direction |
| `Repeat` | apply a subpattern for a finite repetition interval |
| `PathFilter` | apply predicates to path, edge or vertex bindings |
| `PathMode` | enforce walk/trail/simple semantics |
| `Shortest` | select path(s) by hop or declared cost |
| `GraphSemiJoin` | keep bindings for which a subpattern exists |
| `GraphAntiJoin` | keep bindings for which a complete subpattern proves absence |
| `GraphOptional` | preserve the outer binding when a subpattern has no match |
| `GraphAggregate` | aggregate over vertices, edges, paths or groups |
| `GraphAlgorithm` | invoke one versioned built-in algorithm kernel |
| `MaterializeGraph` | produce a derived graph view or bounded workspace |

Ordinary RQL filter, enrich, project, group, order, page, coverage and budget
operators compose with graph operators. Graph bindings are typed values in the
same plan; they are not opaque rows evaluated by another runtime.

### 7.1 Result multiplicity

Pattern matching has bag semantics by default: each distinct complete binding
contributes one row, even when projected values compare equal. `distinct`
performs explicit canonical deduplication. A path variable distinguishes paths
by its identity sequence. An unbound optional variable is `Absent`, not `Null`.

### 7.2 Direction

- `outgoing` follows stored source → target.
- `incoming` follows target → source.
- `both` is the union of both directions and carries the actual direction in
  each `PathStep`.
- An undirected pattern is query syntax for `both`; it does not erase stored
  direction or edge identity.

### 7.3 Path modes

Residiuum defines these exact modes:

| Mode | Repetition rule |
|---|---|
| `walk` | vertices and edges may repeat |
| `trail` | an edge identity may appear at most once |
| `simple` | a vertex identity may appear at most once, except an explicitly requested cycle may close at its start |

No mode is implicit for variable-length traversal. A frontend must insert the
declared profile default into the canonical plan. The initial profile default
is `trail`, chosen to terminate finite graph walks without silently excluding
cycles or parallel edges.

## 8. RQL graph surface

The exact token-level grammar is frozen by `GRF-2`; this section freezes the
semantic surface. The intended form is:

```text
from graph code_graph revision current
match path p =
  (root:Module where .name == $module)
  -[dep:DEPENDS_ON where .scope != "dev" * 1..20]->
  (downstream:Module)
path mode simple
where downstream.package != null
project {
  module: downstream.name,
  depth: length(p),
  path: identities(p)
}
order by depth asc, module asc
limit 1000
coverage complete
budget graph {
  depth: 20,
  vertices: 100000,
  edges: 1000000,
  paths: 10000,
  workspace_bytes: 268435456
}
```

### 8.1 Pattern features

The complete surface supports:

- vertex and edge variables;
- type/label alternatives;
- property predicates at every binding;
- direction and undirected matching;
- fixed-length and finite variable-length patterns;
- concatenation, alternation and optional pattern groups;
- correlated existence, anti-existence and optional matches;
- multiple comma-separated patterns joined by shared variables;
- path variables and path predicates;
- named reusable subplans whose recursion is statically bounded;
- graph-aware grouping, projection and ordering; and
- subgraph construction into derived materialized views.

### 8.2 Regular path queries

A repeated edge/vertex pattern may express a regular language over labels and
directions. The compiler normalizes it to a finite automaton and the QVM
evaluates the product of automaton state and graph vertex. Epsilon transitions,
alternation and finite repetition are legal. Complement over an unbounded label
universe and back-references that make the language non-regular are refused.

Every regular path query still declares a maximum depth or a finite path mode
plus a finite explored-state budget. The planner cannot turn an infinite walk
language into infinite work.

### 8.3 Shortest-path forms

The semantic forms are:

- `any shortest` — one canonical minimum path;
- `all shortest` — every equal-minimum path up to a mandatory result bound;
- `k shortest` — the first `k` canonical paths, with finite `k`;
- `shortest by hops`; and
- `shortest by weight <numeric-expression>`.

Canonical tie order is total path identity order after total cost and hop
count. Non-negative finite weights use Dijkstra-class semantics; an admissible
heuristic may accelerate A* without changing the result. Negative weights
require the explicitly selected `signed_weight` profile, a finite explored
subgraph and negative-cycle detection. NaN, infinity, `Null`, `Absent` or a
non-number weight is a stable query error unless a declared mapping handles it.

## 9. Resource bounds and admission

Graph recursion changes the shape of work, not Residiuum's bounded-work law.
Every graph query has effective ceilings for:

```text
GraphBudget {
  depth
  vertices_examined
  edges_examined
  frontier_entries
  paths_emitted
  path_steps_emitted
  source_bytes
  result_bytes
  workspace_bytes
  spill_bytes
  wall_time
  cpu_time
  network_bytes
  partition_supersteps
}
```

The caller may request tighter bounds. Server policy may tighten them further.
Explain and results disclose requested and effective bounds.

Admission estimates cost from graph statistics, fan-out, selectivity, path
mode, depth, workspace and concurrent load. It may refuse before execution.
During execution every host read, edge examination, automaton state, queued
frontier item, emitted step, spill byte and network exchange is charged once to
the relevant budget counter.

Budget exhaustion:

- under `coverage complete`, fails the query without a complete result claim;
- under `coverage allow incomplete`, may return a bounded partial result plus
  exact termination reason and explored frontier; and
- never becomes an ordinary empty result or silent limit.

## 10. Determinism

For identical canonical plan, parameters, read binding, effective policy and
coverage, a conforming engine produces identical logical results and evidence.

Determinism requires:

- canonical vertex, edge and path identity order;
- stable tie-breaking for frontiers and shortest paths;
- exact numeric profiles for weights and algorithm convergence;
- deterministic partition merge;
- deterministic spill runs and resumed workspace state;
- versioned algorithm implementations; and
- separation of unordered result semantics from presentation order.

Parallel scheduling may differ. It MUST NOT change result membership, selected
canonical shortest paths, aggregate values, coverage or continuation order.

## 11. Graph algorithms

Algorithms are named semantic profiles, not arbitrary procedures. Each profile
freezes accepted graph shape, directedness, weights, exactness, convergence,
tie-breaking, output, bounds and damage behavior.

### 11.1 Traversal and structure

- breadth-first and depth-first visitation;
- bounded reachability and neighbourhoods;
- degree and degree distribution;
- cycle detection and canonical cycle witnesses;
- topological order with cycle refusal/witness;
- weakly and strongly connected components;
- articulation vertices and bridges;
- minimum spanning forest for a declared undirected projection; and
- dominators for a rooted directed graph.

### 11.2 Paths and networks

- unweighted and weighted single-source shortest paths;
- bidirectional shortest path where legal;
- `k` shortest simple paths;
- all-pairs shortest paths only for an admitted finite projection;
- A* with a declared admissible heuristic profile;
- maximum flow/minimum cut with canonical residual ordering; and
- bipartite matching for a declared bipartite projection.

### 11.3 Analytics

- PageRank with frozen damping, initialization, tolerance and iteration cap;
- personalized PageRank;
- closeness and betweenness centrality, exact or explicitly approximate;
- triangle counting and local/global clustering coefficient;
- label propagation with deterministic update profile;
- modularity-based community detection with a versioned deterministic profile;
- `k`-core decomposition; and
- weak/strong component condensation graphs.

Approximate variants carry `approximation_profile`, seed where applicable,
error/confidence information where meaningful, iteration count and coverage.
They never share the result type or claim of their exact counterpart without an
explicit `exactness` field.

### 11.4 Algorithm output

Small results stream as rows. Large vertex/edge annotations are written only to
a new derived materialized graph view or artifact. Publication occurs after
successful completion and validation; a failed algorithm cannot partially
replace the previous artifact.

## 12. QVM and host boundary

### 12.1 Canonical plan additions

The graph plan contains no storage path or physical ordinal. It includes:

```text
GraphPlanV1 {
  semantic_profile
  graph_binding
  typed_variables
  normalized_patterns
  path_modes
  graph_operators
  ordinary_rql_operators
  result_shape
  consistency
  coverage
  budgets
  algorithm_profiles
}
```

The compiler statically rejects missing bounds, illegal type joins,
cross-Heap references, incompatible temporal scopes, unsupported algorithm
profiles and query forms whose exactness cannot be stated.

### 12.2 QVM instruction families

The bytecode ISA grows through versioned instruction families, conceptually:

```text
G_BIND_GRAPH
G_SEED_VERTICES
G_EXPAND_OUT | G_EXPAND_IN | G_EXPAND_BOTH
G_TEST_VERTEX | G_TEST_EDGE
G_VISIT_WALK | G_VISIT_TRAIL | G_VISIT_SIMPLE
G_AUTOMATON_STEP
G_PATH_EMIT
G_SHORTEST_INIT | G_SHORTEST_STEP | G_SHORTEST_EMIT
G_ALGORITHM_CALL
G_WORKSPACE_CHECKPOINT
G_MATERIALIZE
```

Actual encoding is frozen with golden byte vectors before implementation. An
instruction cannot hide an unbounded loop. Every state transition charges a
budget and remains interruptible at declared safe points.

### 12.3 Admitted host capabilities

The graph host may expose data only:

| Capability | Meaning |
|---|---|
| `resolve_graph_definition` | return the authorized immutable definition revision |
| `lookup_vertex_candidates` | exact/covered candidate vertex identities |
| `lookup_edge_candidates` | exact/covered candidate edge identities |
| `lookup_adjacency` | covered edge identities adjacent to one vertex and direction |
| `get_vertex` / `get_edge` | authoritative value and revision in the read view |
| `read_graph_block` | checksummed derived adjacency/property block |
| `open_spill` | bounded Heap-confined workspace storage |
| `coverage_evidence` | frontiers, domains, holes and damage |
| budget/deadline/cancel signal | cooperative stop only |

The host may return candidates in a useful physical order. The QVM owns
filtering, repetition, visited-state semantics, path construction, shortest
selection, aggregation, completeness and result order.

### 12.4 Algorithm kernels

An algorithm kernel is a versioned pure state-transition component called by a
QVM opcode. It consumes only QVM graph access, arithmetic and workspace
interfaces. It cannot open collections, bypass Heap authority, mint coverage,
or publish output. A slow independent oracle exists for every accepted kernel.

## 13. Physical graph structures

### 13.1 Baseline exact indexes

Every edge type requires exact compound adjacency indexes equivalent to:

```text
(graph_revision, generation?, from_vertex, edge_label, edge_id)
(graph_revision, generation?, to_vertex,   edge_label, edge_id)
```

These indexes produce candidates; authoritative edge records are revalidated.
Vertex label/type/property indexes use the generic derived-index substrate.

### 13.2 GraphPack

`GraphPack` is the preferred derived traversal representation for a stable
graph region. A pack contains:

- a dictionary from logical identities to build-local dense ordinals;
- outbound compressed adjacency (CSR-like);
- inbound compressed adjacency (CSC-like);
- edge identity/type streams;
- optional selected immutable property columns;
- rank/select metadata for high-degree adjacency slices;
- source frontier and exact coverage domain;
- per-block checksums and an immutable Merkle root; and
- enough provenance to revalidate ordinals against authority.

Dense ordinals never become application identity. Packs are immutable. Updates
enter bounded delta runs; compaction merges base plus deltas into a new pack and
atomically publishes it. Hydra may choose an encoding per block and Chimera may
place blocks across media, but neither changes graph semantics or authority.

### 13.3 High-degree vertices

Adjacency is chunked by a declared maximum block size. A hub lookup can page by
canonical `(edge_type, edge_id)` order without decoding unrelated blocks.
Heavy-hitter statistics allow admission to price hubs honestly. No single
vertex may force one unbounded allocation.

### 13.4 Late materialization

Traversal carries compact logical references and only properties demanded by
predicates. Full documents are fetched for projection after candidate and path
selection whenever semantics permit. A planner may push predicates into exact
property indexes, but the QVM re-evaluates them against authoritative values.

### 13.5 Advanced accelerators

The engine may build versioned, coverage-bearing accelerators:

- reachability labels or two-hop indexes;
- SCC condensation graphs;
- landmark distances for admissible A* heuristics;
- transitive closure for explicitly bounded stable subgraphs;
- path-prefix/materialized pattern views;
- degree, label and joint selectivity sketches;
- precomputed community/component artifacts; and
- Bloom/Xor filters as rejection candidates only.

An accelerator is used only where its build profile proves it preserves the
requested semantics. Probabilistic structures cannot prove positive membership
or completeness by themselves.

## 14. Optimizer

The optimizer considers:

- vertex/edge start selectivity;
- direction-specific degree distributions and heavy hitters;
- label and property selectivity, including correlations;
- fixed-pattern join order;
- automaton state selectivity for regular paths;
- bidirectional vs unidirectional expansion;
- adjacency index vs GraphPack vs authoritative fallback;
- late property/document materialization;
- shortest-path strategy and admissible heuristics;
- memory, spill, network and partition costs;
- read-view lifetime and derived-index lag; and
- exact coverage required by anti-match and absence claims.

Adaptive execution may change physical strategy at a deterministic checkpoint
when estimates are wrong. It may not change path mode, result semantics,
tie-breaking or coverage requirement. Explain records the original choice,
observed divergence and every adaptation.

## 15. Mutation and integrity

### 15.1 Mutation surface

RQL graph queries are read-only. Mutations use async SDK operations:

- create/replace/delete vertex;
- create/replace/delete edge;
- compare-and-swap property update;
- bounded compound graph transition through Atomics;
- bulk import into an unpublished application generation; and
- managed cascade/materialization jobs.

### 15.2 Atomic graph transition

A strict edge creation is one Heap-local Atomic:

```text
assert source vertex revision/existence
assert target vertex revision/existence
create edge if absent
record derived-index obligations
```

Replacing an edge endpoint asserts the old edge revision and both new
endpoints. A successful Atomic publishes all authoritative changes together.
Adjacency projections may catch up asynchronously under exact frontier and
coverage rules; `current` waits or falls back.

### 15.3 Vertex deletion

- `Restrict` proves no live incident strict edge at the Atomic frontier or
  refuses. Incomplete adjacency coverage cannot prove absence.
- `Detach` permits dangling deferred edges and records integrity violations.
- `CascadeJob` is a durable bounded job which deletes incident edges in Atomic
  batches, then conditionally deletes the vertex. Until completion the vertex
  remains in an explicit `deleting` state defined by its collection contract.

An unbounded cascade is never disguised as one Atomic.

### 15.4 Uniqueness and cardinality

Constraints may require unique edge keys, unique ordered endpoint pairs,
maximum cardinality, acyclicity for a declared edge subgraph, or label/property
rules. Local bounded constraints execute inside Atomics. Constraints requiring
unbounded traversal use certified derived artifacts or managed validation jobs;
if the proof frontier is incomplete, mutation refuses or remains pending.

### 15.5 Bulk loading

Bulk loading writes immutable vertices/edges in bounded durable batches,
records rejected rows, builds exact indexes/GraphPacks, validates constraints,
then atomically activates the new generation root. It is restartable by stable
job identity. Repeating the same manifest is idempotent; a different manifest
under the same identity refuses.

## 16. Async client and wire contract

### 16.1 Object topology

```text
Client                         // one physical deployment connection/domain
  -> HeapClient                // one capability-bound Heap
       -> GraphClient          // graph definition within that Heap
            -> query/traverse/algorithms/mutate/admin
```

`GraphClient` never owns another physical writer, scheduler, queue, inspection
state or shutdown domain. More than one graph and Heap binding may coexist on
one `Client` when separately authorized.

### 16.2 Core Rust shape

The intended async shape is:

```rust
let heap = client.heap(heap_cap).await?;
let graph = heap.graph("code", GraphRevision::Current).await?;

let mut rows = graph
    .traverse(
        Traversal::from(module_id)
            .out("DEPENDS_ON")
            .depth(1..=20)
            .path_mode(PathMode::Simple)
            .budget(budget),
    )
    .await?;

while let Some(row) = rows.try_next().await? {
    // bounded streamed result with version, read binding and coverage
}
```

Public result values include stable logical IDs and establishing revisions:

```text
VersionedVertex<T> { id, value, version, labels, read_binding }
VersionedEdge<T>   { id, from, to, value, version, label, read_binding }
GraphPage<T>       { rows, continuation, coverage, statistics, read_binding }
```

### 16.3 Required client behavior

- async-only database work;
- bounded count/byte admission before queueing;
- stream/page backpressure;
- deadline and cancellation propagation;
- stable operation/job/query IDs;
- no automatic retry after ambiguous mutation admission without status
  resolution;
- no client-side decoding or manufacture of continuation tokens;
- explicit exactness and coverage in types; and
- no raw store handles escaping the SDK/DAL boundary.

### 16.4 Wire

Remote execution carries a versioned canonical plan or an authorized source
plus bindings that the server compiles to the same canonical plan. Handshake
negotiates semantic profile, QVM graph ISA, algorithms and resource ceilings.
Unsupported profiles are refused before execution. The server does not accept
an arbitrary executable program or host callback.

## 17. Streaming, continuation and workspaces

Graph queries use three continuation classes:

1. **seek continuation** for plans whose next position is completely described
   by bounded authenticated state;
2. **materialized-result continuation** for a completed result artifact; and
3. **resumable-workspace continuation** for long traversal/algorithm state.

Every token binds at least:

- Heap, graph and definition revision;
- plan and parameter hash;
- read view/frontier and application generation;
- algorithm/path semantic profile;
- effective budgets and coverage requirement;
- workspace/result artifact identity and generation;
- next canonical position; and
- expiry plus authentication domain.

Workspace state is Heap-confined, encrypted where database data is encrypted,
checksummed, quota-accounted, cancellable and garbage-collected after expiry.
Publication/checkpoint is atomic. Recovery either resumes the last verified
checkpoint or marks the workspace unusable; it never invents frontier state.

Snapshot pinning, spill and workspace retention consume explicit quotas. A
client that stops reading cannot hold unbounded memory or an immortal read view.

## 18. External-memory execution

When the graph exceeds memory, the QVM may use:

- sorted frontier runs;
- partitioned visited sets;
- disk-backed priority queues;
- compressed predecessor/path stores;
- block-local GraphPack scans; and
- deterministic merge/reduction passes.

All spill belongs to a query/workspace identity and Heap. Spill files use
checksummed framed formats, are never authority, and are recoverably deleted.
Admission accounts for worst-case spill and free-space reserve. Disk-full
returns a stable resource outcome with no complete result claim.

## 19. Distributed execution

Distributed graph work is admitted only after the Cluster gate defines the
required snapshot/frontier profile. A graph remains one Heap even when its data
is partitioned.

### 19.1 Partitioning

Default vertex ownership is a stable partition of `VertexRef`. Edges are
authoritative in their source collection partition. Derived inbound/outbound
adjacency may be replicated by obligation. Replicas remain derived and
coverage-bearing.

### 19.2 Execution model

The coordinator executes the same QVM semantics using partition-local frontier
steps and deterministic exchanges. Bulk-synchronous supersteps are the baseline
for recursive and iterative algorithms because they expose frontier, failure
and convergence boundaries. Optimized asynchronous transport is legal only
when equivalence to the algorithm profile is proved.

### 19.3 Failure and coverage

Distributed evidence records:

- participating partitions and epochs;
- read frontier per partition;
- adjacency/index frontier per partition;
- messages/supersteps completed;
- missing or retried partitions;
- coordinator/workspace identity; and
- whether exact convergence and complete coverage were established.

A missing partition cannot become an empty adjacency list. Anti-match,
reachability absence and completed analytics require exact participating-domain
coverage. Coordinator replacement resumes authenticated state or restarts at
the same read view; it cannot silently change snapshots.

## 20. Damage, recovery and salvage

### 20.1 Ordinary recovery

Opening a Heap does not rebuild the entire graph. Ordinary open inventories
published graph metadata and resumes bounded jobs; derived graph rebuild is a
background/admin operation. A missing GraphPack degrades qualified plans to
exact base indexes or authoritative scan, or causes an honest refusal.

### 20.2 Coverage model

Graph coverage is multidimensional:

```text
GraphCoverage {
  vertex_types_and_domains
  edge_types_and_domains
  directions
  source_frontiers
  index_or_pack_frontiers
  read_view
  partitions
  known_holes
  rejected_integrity_records
  algorithm_exactness
  terminated_by
}
```

An adjacency lookup proves “no incident edge” only for its exact covered
domain. A traversal proves “unreachable” only when every frontier expansion
needed by the semantic search was complete. A negative pattern or `Restrict`
delete requires the corresponding absence proof.

### 20.3 Damaged paths and analytics

- A path composed entirely of verified surviving records is valid surviving
  data.
- Damage elsewhere may make the path set or shortest-path claim incomplete.
- A returned path MUST NOT contain an unverified or missing member.
- `no path` is never returned as complete when a relevant frontier has a hole.
- Exact whole-graph algorithms refuse or report incomplete coverage when their
  input domain is incomplete.
- Approximate algorithms report damage separately from statistical
  approximation; the two are not interchangeable.

### 20.4 Salvage

SDA examination exposes surviving graph definitions, vertex/edge records,
Atomic decisions, derived-build manifests and known holes. A graph salvage
projection emits:

- verified vertices and edges;
- unresolved endpoint references;
- conflicting identities/revisions;
- damaged or missing source ranges;
- derived artifacts that can be trusted only as provenance clues; and
- graph coverage, never a fabricated complete graph.

## 21. Security and multi-tenancy

- Heap authority is checked before graph definition resolution.
- Graph admin, read, mutate, algorithm and materialize rights are separately
  attenuable capabilities.
- Authorization is rechecked for every source collection bound by the graph
  definition; graph authority cannot amplify collection rights.
- Hidden labels, properties, vertices and edges must not leak through counts,
  degree, path existence, timing-sensitive explain detail or statistics.
- Query/result/workspace caches are scoped by authorization digest in addition
  to Heap and plan.
- Budgets, concurrency and workspace quotas protect high-fan-out denial of
  service.
- Spill and materialized artifacts inherit Heap encryption, jurisdiction,
  retention and evidence policy.
- No query syntax admits file, network, process, dynamic library or application
  callback access.

Where authorization removes graph elements, the authorized view is itself the
query graph. Completeness claims apply only to that view and must not disclose
that a hidden element exists.

## 22. Explain, telemetry and administration

### 22.1 Explain

Structured explain adds:

- graph and definition revision;
- normalized pattern/automaton;
- path mode, shortest/algorithm profile and exactness;
- chosen start binding and join/expansion order;
- degree/selectivity estimates and evidence age;
- selected adjacency, GraphPack and advanced accelerators;
- authoritative revalidation points;
- projected frontier, visited, memory, spill and network cost;
- requested/effective graph budgets;
- read view and coverage proof obligations;
- rejected plans and refusal reasons; and
- continuation/workspace class.

`explain analyze` executes only when explicitly requested and returns actual
counters without dumping sensitive values.

### 22.2 Telemetry

Bounded telemetry includes query/plan profile, admission outcome, vertices and
edges examined, frontier peaks, property fetches, cache/block activity, spill,
network exchange, QVM/algorithm time, coverage, cancellations and tail latency.
No record payload, path identity or secret parameter is logged by default.

### 22.3 Administration

Operators can inspect graph definitions, builds, frontiers, coverage, damage,
statistics, jobs, workspaces and quotas; trigger build/verify/rebuild/retire;
and compare derived state to authoritative samples or a full bounded audit.
Every mutating admin operation has stable identity and durable evidence.

## 23. Stable error taxonomy

At minimum:

```text
graph_not_found
graph_definition_revision_not_found
graph_definition_invalid
graph_heap_mismatch
graph_source_unauthorized
graph_vertex_not_found
graph_edge_not_found
graph_endpoint_missing
graph_integrity_violation
graph_pattern_invalid
graph_path_mode_required
graph_bound_required
graph_weight_invalid
graph_negative_cycle
graph_algorithm_unsupported
graph_profile_unsupported
graph_budget_exhausted
graph_admission_refused
graph_coverage_incomplete
graph_read_view_expired
graph_continuation_invalid
graph_workspace_expired
graph_workspace_damaged
graph_index_lagging
graph_derived_state_damaged
graph_partition_unavailable
graph_exactness_unavailable
```

Errors distinguish invalid request, definite absence, incomplete knowledge,
resource refusal, transient availability and damaged state. Human messages may
change; stable codes and structured fields do not.

## 24. Formal obligations

The programme registers claims before implementation. At minimum:

| Claim | Obligation |
|---|---|
| `G-AUTH-1` | derived deletion cannot delete or redefine authoritative graph data |
| `G-ISO-1` | no plan, cache, spill, index or traversal crosses Heap authority |
| `G-QVM-1` | optimized QVM graph execution is equivalent to the independent bounded oracle |
| `G-ADJ-1` | exact adjacency candidates are complete for the declared covered domain |
| `G-PATH-1` | emitted paths contain only valid adjacent authoritative members in the read view |
| `G-MODE-1` | walk/trail/simple visited-state rules match the mathematical definitions |
| `G-REACH-1` | complete bounded traversal returns every and only reachable binding within its pattern language |
| `G-SP-1` | canonical shortest result has minimum declared cost and stable tie order |
| `G-COV-1` | a hole cannot produce false absence, false shortest, false anti-match or false convergence |
| `G-SNAP-1` | every result is interpretable against one declared read binding |
| `G-CONT-1` | page concatenation/resume equals one uninterrupted execution at the same read binding |
| `G-SPILL-1` | in-memory and external-memory executions are result/evidence equivalent |
| `G-ATM-1` | strict graph mutations preserve endpoint and declared bounded constraints atomically |
| `G-REC-1` | crash recovery exposes either the old or new published derived generation, never a mixture |
| `G-DIST-1` | partitioned execution and deterministic merge equal the single-node oracle for complete coverage |
| `G-ALG-1` | each accepted algorithm kernel meets its versioned mathematical postcondition |

The proof stack uses the appropriate tool rather than one ceremonial formalism:

- executable reference model and property/differential testing for all plans;
- TLA+/state-machine exploration for publication, workspace and distributed
  recovery;
- Verus/Lean-style proofs for selected traversal/shortest/coverage kernels;
- Kani/loom for bounded implementation state, cancellation and concurrency;
- hostile codecs and fuzzing for plans, tokens, graph blocks and workspaces;
- crash/failpoint matrices for builds, mutation and continuation; and
- model-based history checking for Atomic graph transitions.

## 25. Conformance and qualification

### 25.1 Semantic corpus

The independent corpus contains at least:

- empty, singleton, chain, tree, diamond, lattice and disconnected graphs;
- directed cycles and self-loops;
- parallel edges with equal and unequal properties;
- high-degree stars and power-law hubs;
- overlapping labels and graph mappings;
- absent/null/wrong-type properties;
- dangling deferred endpoints;
- every path mode and depth boundary;
- regular-pattern alternation and epsilon cases;
- equal-cost path ties, zero weights, invalid/negative weights and cycles;
- concurrent graph updates and read-view pinning;
- damaged vertex/edge/index/GraphPack/workspace regions;
- incomplete partitions and lagging derived frontiers; and
- continuation at every page and failure boundary.

Every optimized plan is differentially checked against a deliberately slow
oracle operating on an in-memory canonical graph extracted from the same read
view.

### 25.2 Performance shapes

Qualification measures:

- point vertex/edge and one-hop adjacency;
- bounded 2–20 hop reachability;
- selective and non-selective fixed patterns;
- shortest path in sparse, dense and high-diameter graphs;
- high fan-out and supernode queries;
- cycles/SCC/PageRank on memory-resident and external-memory datasets;
- cold/warm cache, build/catch-up and mixed updates;
- restart with intact/missing/damaged derived graph state;
- concurrent readers and bounded admission fairness; and
- single-node versus partitioned equivalence and scaling when Cluster is
  admitted.

Record p50/p95/p99 latency, throughput, CPU, memory, logical/physical bytes,
read amplification, GraphPack/index size, spill, network, build/catch-up time,
coverage and tail behavior.

The industry comparison harness adopts the relevant shapes from the
[Graph Data Council LDBC SNB Interactive and BI workloads](https://ldbcouncil.org/benchmarks/snb/),
with full disclosure of semantic, hardware, cache, durability, index and
concurrency differences. Passing a private microbenchmark is not graph-engine
qualification.

### 25.3 Acceptance law

A delivery profile is accepted only when:

1. its exact semantic subset and refusals are frozen;
2. builder and text frontends lower to the same canonical plan;
3. the QVM is the only product semantic executor;
4. oracle, differential, fuzz, crash and recovery evidence pass;
5. coverage/damage negative controls prove no false answer;
6. async SDK and remote/embedded paths agree where both are in scope;
7. resource ceilings and performance envelopes are published; and
8. capability negotiation reports only the accepted profile.

## 26. Versioning and compatibility

Version independently:

- graph semantic profile;
- graph-definition schema;
- canonical graph plan;
- QVM graph ISA;
- GraphPack and workspace formats;
- each algorithm profile;
- SDK/wire capability profile; and
- optional GQL/PGQ frontend profile.

Readers reject unknown required semantics. Writers never silently rewrite an
old graph definition or derived build under new interpretation. Migration
builds a new revision side by side, validates it, then atomically changes the
selected definition revision.

## 27. Source-analysis reference journey

The first real consumer is a package/module/source graph. A recommended model
is:

```text
vertices:
  Workspace, Package, Module, File, Symbol

edges:
  CONTAINS, DECLARES, IMPORTS, REQUIRES, REFERENCES, CALLS, EXPORTS

authoritative collections:
  analysis_generations
  graph_vertices
  graph_edges
  graph_diagnostics
  graph_roots
```

Each scan creates immutable generation `G`. Vertex and edge keys are stable
content-derived or caller-generated identities whose collision policy is
frozen. After validation:

```text
replace graph_roots/code.active_generation if version V: G_old -> G
```

The early client must answer one-hop imports/dependants and bounded dependency
closure. Later profiles add cycle witnesses, shortest dependency explanations,
impact paths, symbol-reference patterns, SCC condensation, centrality and
incremental Atomic replacement. No early record needs migration because it was
written under the final graph definition and identity model.

## 28. Non-claims at v0.1

This document does not claim that:

- native graph syntax, QVM opcodes, GraphPack or `GraphClient` exist;
- Residiuum currently conforms to ISO GQL or SQL/PGQ;
- all listed algorithms will ship in one release;
- cross-partition graph snapshots are already available;
- unaccepted Atomics can enforce graph constraints; or
- graph work is admitted ahead of the current critical path.

It freezes the destination so staged work can be useful immediately without
creating an architectural dead end.
