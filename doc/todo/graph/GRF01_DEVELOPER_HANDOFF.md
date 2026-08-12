# GRF-0 / GRF-1 developer handoff

Status: **developer-ready specification v1.0; implementation not admitted by
critical path**

Date: 2026-08-12

Package IDs: `GRF-0`, `GRF-1`

Inspected implementation baseline: clean `main` at `fb8942ecce15` before this
specification change.

Normative parent:
[GRAPH_ENGINE_SPEC.md](./GRAPH_ENGINE_SPEC.md)

Delivery authority:
[GRAPH_ENGINE_DELIVERY_PLAN.md](./GRAPH_ENGINE_DELIVERY_PLAN.md)

Machine-readable contracts: [spec/graph/](../../../spec/graph/)

## 1. Handoff decision

This document closes the choices needed to implement the first two graph
packages. Developers may choose ordinary internal Rust structure and optimize
within measured bounds. They may not change public meaning, durable encoding,
identity, collection/index layout, authority, error outcomes, package exits or
the one-runtime boundary without an architect amendment.

The implementation target is:

```text
GRF-0: pure model + codecs + oracle + fixtures
    |
    v
GRF-1: embedded async GraphClient
       + immutable-generation loading
       + exact point and one-hop adjacency
```

GRF-1 is not recursive traversal. It must not add traversal loops to the SDK,
store or server. `GRF-2` will add traversal through QVM under a separate
developer handoff.

The GRF-1 point, source-page and one-hop methods are fixed-shape data-access
primitives, not an alternate query algebra: they accept no predicate AST,
callbacks, joins, recursion, path state or user plan. GRF-2 may expose these
same bounded host operations to QVM; it may not wrap an SDK traversal loop and
call that native execution.

Precedence for GRF-0/1 is: machine contracts for bytes/enums/limits, this
handoff for observable behavior and API, the destination specification for
unrestated architecture, then the delivery plan for sequencing. A detected
conflict is a specification defect and stops the affected package; developers
do not choose whichever text is convenient. Only an architect-reviewed
amendment may resolve it.

## 2. Closed scope

### 2.1 Included

- each graph belongs to exactly one Heap; one Heap may contain multiple graphs;
- multiple canonical-record vertex and edge types;
- multiple types sharing a collection;
- directed edges, parallel edges and self-loops;
- static vertex labels and one edge label per edge type;
- immutable application generations;
- graph registration and lookup by name or `GraphId`;
- exact point vertex/edge reads with establishing versions;
- exact one-hop outgoing/incoming/both adjacency pages;
- optional edge-type restriction;
- bulk generation ingestion, validation and one-key CAS activation;
- graph-owned canonical source collections and sealed generations;
- complete or explicitly incomplete coverage;
- generation-scoped adjacency artifacts with authoritative edge revalidation;
- embedded async driver support; and
- restart, retry, damage and rebuild qualification.

### 2.2 Excluded and refused

- recursive traversal, paths, patterns, shortest paths and algorithms;
- remote graph RPC;
- graph definition update, rename or delete;
- active-generation removal;
- fine-grained mutation of an active generation;
- endpoint cascade;
- temporal graph interpretation;
- derived labels, membership predicates or property projections;
- cross-Heap or cross-graph endpoint references;
- GraphPack;
- standards frontend claims; and
- a synchronous or legacy-flat graph API.

Excluded operations return `graph_profile_unsupported`; they are never
approximated in the client.

## 3. Required repository shape

GRF-0 creates:

```text
crates/residiuum-graph/
  Cargo.toml
  src/
    lib.rs
    ids.rs
    names.rs
    definition.rs
    record.rs
    canonical.rs
    coverage.rs
    error.rs
    oracle.rs
  tests/
    vectors.rs
    hostile_decode.rs
    model_properties.rs
    source_analysis.rs
```

The crate is pure:

- allowed dependencies: `serde`, `serde_json`, `thiserror`, `blake3`, and the
  minimal deterministic CBOR facility already used by the workspace;
- it may depend on `residiuum-heap` for `HeapId` and `CollectionId`;
- it must not depend on store, SDK, server, cluster, filesystem or async
  runtime crates; and
- no production IO or thread creation is permitted.

GRF-1 adds:

```text
crates/residiuum-sdk/src/driver/graph.rs     // or driver_graph.rs if module
                                             // extraction is deferred
crates/residiuum-store/src/heap/graph.rs
crates/residiuum-sdk/tests/grf1_*.rs
```

If `driver.rs` is split during the work, behavior and public paths remain
`residiuum_sdk::driver::*`. Moving unrelated driver behavior is forbidden.

GRF-0 has no Atomics dependency and may be delivered independently. GRF-1 has
a hard entry gate: embedded LocalHeap `residiuum-atomic-v1` must have passed its
ATM-5 qualification and `driver::Capabilities::atomics` must be true. GRF-1
uses those Atomics for graph record-state publication; it must not implement a
second transaction coordinator. The collection descriptor/ownership pair in
§6.1 remains a narrow Heap metadata publication primitive because collection
creation is outside the Atomics v1 record-mutation vocabulary.

## 4. Profiles and identifiers

The exact profile strings are:

```text
residiuum-graph-model-v1
residiuum-graph-definition-cbor-v1
residiuum-graph-record-json-v1
residiuum-graph-client-v0.1
residiuum-graph-adjacency-v1
residiuum-graph-bulk-generation-v1
```

The following capability profile strings are reserved by the destination but
must remain false in GRF-1:

```text
residiuum-graph-traversal-v0.1
residiuum-graph-path-v0.1
residiuum-graph-physical-v1
residiuum-graph-integrity-v1
residiuum-graph-analytics-v1
residiuum-graph-cluster-v1
residiuum-graph-gold-v1
```

Corresponding wire feature identifiers are reserved now, but no remote graph
feature is activated in GRF-1:

```text
graph-model-v1
graph-client-v0.1
graph-adjacency-page-v1
graph-bulk-generation-v1
```

No remote endpoint advertises these features in GRF-1.

### 4.1 Identity widths

| Type | Width | Validation |
|---|---:|---|
| `GraphId` | 16 bytes | non-zero RFC 4122 UUIDv4 |
| `VertexTypeId` | 16 bytes | non-zero RFC 4122 UUIDv4 |
| `EdgeTypeId` | 16 bytes | non-zero RFC 4122 UUIDv4 |
| `GraphGenerationId` | 16 bytes | non-zero RFC 4122 UUIDv4 |
| `GraphJobId` | 16 bytes | non-zero RFC 4122 UUIDv4 |
| `GraphDefinitionRevision` | 32 bytes | BLAKE3-256 rule in §8.2 |
| document version | 16 bytes | existing establishing event ID |
| `OperationId` | 16 bytes | existing driver identity |

New UUID identities follow the implementation and formatting behavior of
`residiuum_heap::HeapId`: OS CSPRNG, canonical lowercase hyphenated display,
strict parsing and no unchecked public constructor.

### 4.2 Logical references

```rust
pub struct VertexRef {
    pub heap_id: HeapId,
    pub graph_id: GraphId,
    pub vertex_type_id: VertexTypeId,
    pub collection_id: CollectionId,
    pub key: String,
}

pub struct EdgeRef {
    pub heap_id: HeapId,
    pub graph_id: GraphId,
    pub edge_type_id: EdgeTypeId,
    pub collection_id: CollectionId,
    pub key: String,
}
```

Ordering is lexicographic over the fields in displayed order, comparing fixed
bytes unsigned and `key` by UTF-8 bytes. All fields participate in equality and
hashing. Logical graph keys are 1–2,015 UTF-8 bytes and contain no NUL.

An endpoint stored inside an edge omits Heap/graph because its enclosing edge
already binds both:

```rust
pub struct VertexLocator {
    pub vertex_type_id: VertexTypeId,
    pub collection_id: CollectionId,
    pub key: String,
}
```

Expanding it into a public `VertexRef` inserts the bound Heap and graph IDs.

### 4.3 Generation-scoped physical keys

Graph collections retain several immutable generations simultaneously. A
logical graph key is therefore never used directly as the underlying collection
key. The only GRF-1 codec is:

```text
physical_key = lower_hex(generation_id_raw_16_bytes) || "/" || logical_key
```

The prefix is exactly 33 ASCII bytes, so the 2,015-byte logical limit produces
at most the existing 2,048-byte collection-key limit. There is no escaping:
the decoder consumes exactly 32 lowercase hex digits and `/`, and every
remaining byte is the non-empty UTF-8 logical key. A generation scan is the
exact fixed-prefix range for `<generation_hex>/`; byte order within it is
logical-key UTF-8 order. Non-canonical prefixes, uppercase hex, invalid UTF-8,
NUL, empty suffix or body-generation mismatch are `graph_record_mismatch`.
Public references, receipts, adjacency tuples, continuation positions and
content hashes always contain the logical key. Only the store boundary encodes
the physical key.

## 5. Portable naming profile

Graph names:

- 1–63 bytes;
- lowercase ASCII letters, digits, `-`, `_`, and `.`;
- begin and end with a letter or digit;
- no consecutive `..`;
- case-sensitive; and
- `system`, `admin`, `default` and names beginning `_residiuum` are reserved.

Type names and labels:

- 1–63 ASCII bytes;
- match `[A-Za-z_][A-Za-z0-9_]{0,62}`;
- case-sensitive;
- names beginning `_residiuum` are reserved; and
- static label arrays are sorted by unsigned UTF-8 bytes and contain no
  duplicates.

Graph display/name profiles are deliberately narrower than arbitrary JSON
property names. A later Unicode naming profile requires a new semantic profile.

## 6. System collection layout

The following collection names are reserved exactly:

```text
_residiuum.graph.definitions.v1
_residiuum.graph.names.v1
_residiuum.graph.heads.v1
_residiuum.graph.jobs.v1
_residiuum.graph.generations.v1
_residiuum.graph.collection-bindings.v1
```

Starting with GRF-1, public `create_collection` rejects every name beginning
`_residiuum.` with the general driver code `reserved_collection`. An existing
store containing
one of the six exact names but not a valid internally created collection still
opens normally, but graph bootstrap and every graph call refuse
`graph_reserved_collection_conflict`. Ordinary non-graph collection access is
unaffected. No automatic rename or deletion occurs.

Internal collection creation uses Heap administration code, not a bypass of
Heap authority. The operation IDs are derived as specified in §16.2. The
internal collection IDs remain ordinary random `CollectionId`s recorded in the
Heap catalog.

| Collection | Key | Body | Authority |
|---|---|---|---|
| definitions | `<graph_uuid>/<revision_hex>` | deterministic CBOR definition document | immutable graph definition |
| names | `<graph_name>` | deterministic CBOR name binding | visible name → graph identity |
| heads | `<graph_uuid>` | deterministic CBOR graph head | current definition + active generation |
| jobs | `<job_uuid>` | deterministic CBOR job/manifest state | bulk job identity and validation evidence |
| generations | `<graph_uuid>/<generation_uuid>` | deterministic CBOR generation binding | validated generation → job/manifest/coverage |
| collection-bindings | `<collection_uuid>` | deterministic CBOR graph ownership binding | exclusive canonical graph-write guard |

System records use the same authoritative store, versions, Recovery Shadow and
salvage rules as ordinary records. They are not derived index files.

### 6.1 Canonical collection ownership

Every source collection in a GRF-1 definition is created by graph registration
as exclusively graph-owned. GRF-1 does not adopt an existing collection. One
new collection may serve several types in the same graph, but cannot mix vertex
and edge record kinds, belong to another graph or accept ordinary collection
mutations.

The Heap store exposes an internal-only bounded administrative primitive
`create_graph_collection_idempotent`. Under the Heap writer/metadata lock it
creates the collection descriptor and its collection-binding record before the
collection becomes listable. Crash recovery exposes neither object, or the
same paired object identities; it never exposes an ordinary writable
collection. The primitive accepts the graph parent operation identity and has
golden/failpoint coverage. It is administrative metadata publication, not a
general multi-record data Atomic and is not public SDK surface.

The Heap mutation boundary checks `_residiuum.graph.collection-bindings.v1`
before accepting a put, replace or delete. A bound collection requires an
unforgeable internal `GraphWritePermit` containing Heap, graph, definition,
generation, job and target identity. Only `GraphGenerationWriter` can obtain
one after validating a durable `Loading` job. Public `Collection<T>` and raw
wire data operations return `graph_collection_owned` before admission.

Legacy raw-store mutation bypass is outside the graph profile. A build exposing
that bypass cannot advertise any GRF-1 graph capability. Reads through ordinary
collection APIs remain allowed and coverage-honest.

When validation changes the job from `Loading` to `Validating`, no further
permit can be minted. The generation is thereby sealed. `Rejected`, `Validated`,
`Activated` and `Damaged` generations are also immutable. A failed generation
is corrected under a new `GraphJobId` and `GraphGenerationId`, never by changing
a previously submitted record under the same derived operation identity.

## 7. GraphDefinition v1

Public semantic types:

```rust
pub struct GraphDefinitionDraft {
    pub graph_id: GraphId,
    pub name: GraphName,
    pub vertex_types: Vec<VertexTypeDraft>,
    pub edge_types: Vec<EdgeTypeDraft>,
}

pub struct VertexTypeDraft {
    pub id: VertexTypeId,
    pub name: TypeName,
    pub collection_name: String,
    pub labels: Vec<Label>,
}

pub struct EdgeTypeDraft {
    pub id: EdgeTypeId,
    pub name: TypeName,
    pub collection_name: String,
    pub label: Label,
    pub from_types: Vec<VertexTypeId>,
    pub to_types: Vec<VertexTypeId>,
    pub endpoint_policy: EndpointPolicy,
}

pub struct GraphDefinition {
    pub heap_id: HeapId,
    pub graph_id: GraphId,
    pub name: GraphName,
    pub vertex_types: Vec<VertexTypeDefinition>,
    pub edge_types: Vec<EdgeTypeDefinition>,
}

pub struct VertexTypeDefinition {
    pub id: VertexTypeId,
    pub name: TypeName,
    pub collection_id: CollectionId,
    pub labels: Vec<Label>,
}

pub struct EdgeTypeDefinition {
    pub id: EdgeTypeId,
    pub name: TypeName,
    pub collection_id: CollectionId,
    pub label: Label,
    pub from_types: Vec<VertexTypeId>,
    pub to_types: Vec<VertexTypeId>,
    pub endpoint_policy: EndpointPolicy,
}

pub enum EndpointPolicy {
    Strict,
    Deferred,
}
```

`GraphDefinitionDraft` is the create input. The Heap is inferred from
`GraphCatalogClient`; collection names use the existing 1–256 byte collection
name profile and cannot begin `_residiuum.`. Graph creation resolves or creates
each distinct draft collection name exactly once and returns the finalized
`GraphDefinition` containing immutable `CollectionId`s.

GRF-1 interpretation is frozen:

- source profile is `CanonicalRecordV1` for every type;
- identity is the logical graph key; its source document key is the §4.3
  generation-scoped physical encoding;
- properties are the whole `properties` member;
- labels are static from the vertex definition and must equal the stored
  canonical record labels;
- edge label is static from the edge definition and must equal the record;
- membership predicates, temporal profile, user constraints and physical hints
  are absent;
- `from_types` and `to_types` are non-empty sorted unique sets;
- every referenced endpoint type exists in `vertex_types`; and
- every collection ID belongs to the bound Heap at registration time.

Limits:

| Item | Limit |
|---|---:|
| encoded definition document | 262,144 bytes |
| vertex types | 256 |
| edge types | 512 |
| labels per vertex type | 16 |
| endpoint type alternatives per side | 64 |
| decoded nesting | 8 |
| name/label | 63 bytes |

Types are canonically sorted by ID. Duplicate IDs or names are invalid. Sharing
a collection across types of the same record kind is allowed. A collection may
not be both a vertex and edge source within one definition in GRF-1; this avoids
ambiguous canonical record validation and may be relaxed only by a later
profile.

## 8. Canonical definition encoding

### 8.1 CBOR rules

Use the repository deterministic-CBOR profile:

- one definite top-level unsigned-integer-keyed map;
- shortest integer/length encoding;
- unsigned integer keys sorted by encoded bytes;
- no duplicate keys, floats, tags or indefinite values;
- valid UTF-8;
- sorted arrays where this document declares set semantics; and
- unknown keys rejected in v1.

Exact label maps are in
[cbor-v1.json](../../../spec/graph/cbor-v1.json).

### 8.2 Revision

First encode the `graph_definition_content` map exactly. Then:

```text
definition_revision = BLAKE3-256(
    UTF8("RESIDIUUM-GRAPH-DEFINITION-REVISION-V1")
    || graph_definition_content_cbor
)
```

The stored definition document is the map:

```text
1 -> uint 1
2 -> bstr(graph_definition_content_cbor)
3 -> bstr(32, definition_revision)
```

Decode re-encodes content and verifies field 3. Hash mismatch is
`graph_definition_damaged`, not a validation error.

### 8.3 Semantic content map

```text
1 -> text "residiuum-graph-model-v1"
2 -> bstr(16) heap_id
3 -> bstr(16) graph_id
4 -> text graph_name
5 -> array vertex_type
6 -> array edge_type
7 -> empty array constraints
8 -> null temporal_profile
9 -> empty map physical_hints
```

Vertex type map:

```text
1 -> bstr(16) vertex_type_id
2 -> text type_name
3 -> bstr(16) collection_id
4 -> uint 1              // CanonicalRecordV1
5 -> array text labels   // canonical sorted unique
6 -> uint 1              // WholePropertiesMember
7 -> null                // no membership predicate
```

Edge type map:

```text
1  -> bstr(16) edge_type_id
2  -> text type_name
3  -> bstr(16) collection_id
4  -> uint 1             // CanonicalRecordV1
5  -> text edge_label
6  -> array bstr(16) from_type_ids
7  -> array bstr(16) to_type_ids
8  -> uint 1             // WholePropertiesMember
9  -> null               // no membership predicate
10 -> uint 1 | 2         // Strict | Deferred
11 -> uint 1             // Restrict; mutation not exposed in GRF-1
```

## 9. Authoritative record JSON

The normative schema is
[records-v1.schema.json](../../../spec/graph/records-v1.schema.json).
The SDK constructs metadata; callers provide `properties` only.

### 9.1 Vertex

```json
{
  "_rgraph": {
    "v": 1,
    "kind": "vertex",
    "graph": "20212223-2425-4627-a829-2a2b2c2d2e2f",
    "definition": "<64 lowercase hex>",
    "generation": "80818283-8485-4687-8889-8a8b8c8d8e8f",
    "type": "30313233-3435-4637-b839-3a3b3c3d3e3f",
    "labels": ["Module"]
  },
  "properties": {}
}
```

### 9.2 Edge

```json
{
  "_rgraph": {
    "v": 1,
    "kind": "edge",
    "graph": "20212223-2425-4627-a829-2a2b2c2d2e2f",
    "definition": "<64 lowercase hex>",
    "generation": "80818283-8485-4687-8889-8a8b8c8d8e8f",
    "type": "50515253-5455-4657-9859-5a5b5c5d5e5f",
    "label": "DEPENDS_ON",
    "from": {
      "type": "30313233-3435-4637-b839-3a3b3c3d3e3f",
      "collection": "60616263-6465-4667-a869-6a6b6c6d6e6f",
      "key": "module/a"
    },
    "to": {
      "type": "30313233-3435-4637-b839-3a3b3c3d3e3f",
      "collection": "60616263-6465-4667-a869-6a6b6c6d6e6f",
      "key": "module/b"
    }
  },
  "properties": {"scope": "runtime"}
}
```

Rules:

- top-level members are exactly `_rgraph` and `properties`;
- `_rgraph` has exactly the fields shown for its kind;
- unknown metadata fields are rejected in v1;
- `properties` may be any JSON value, including `null`; absence is invalid;
- metadata strings use the exact canonical formats;
- vertex labels exactly equal the sorted definition labels;
- endpoint collection/type pairing matches the definition;
- record body remains within the existing 16 MiB value limit; and
- user properties named `_rgraph` are legal inside `properties` and have no
  metadata meaning.

The record's decoded logical collection key completes its identity. It is not
duplicated in the JSON body; the physical collection key is derived by §4.3.

## 10. Exact generation-scoped adjacency

The existing generic secondary index is not the GRF-1 adjacency authority. It
becomes globally stale when any record in its collection changes, so loading a
new generation would unnecessarily invalidate the active generation. GRF-1
therefore introduces one narrow derived structure:
`residiuum-graph-adjacency-v1`.

For every `(Heap, graph, definition revision, generation, edge collection)`
validation builds two immutable artifacts: outgoing and incoming. Each entry
is the tuple:

```text
endpoint_vertex_type_id : 16 bytes
endpoint_collection_id  : 16 bytes
endpoint_key             : u16be length + UTF-8 bytes
edge_type_id             : 16 bytes
edge_key                 : u16be length + UTF-8 bytes
```

Outgoing uses the stored `from` endpoint; incoming uses `to`. Entries are
strictly sorted by the tuple above and duplicate tuples are rejected. Edge keys
remain authoritative logical keys; no edge body or property is stored in the
artifact.

### 10.1 Artifact format

The immutable file name is derived from the full identity hash, never a display
name. Paths below are relative to the deployment's derived-data root; they do
not enter authoritative segment or Recovery Shadow namespaces:

```text
graph/adjacency/<artifact_id_first_byte_lower_hex>/<artifact_id_lower_hex>.gai

artifact_id = BLAKE3-256(
  "RESIDIUUM-GRAPH-ADJACENCY-ARTIFACT-V1"
  || heap_id || graph_id || definition_revision || generation_id
  || edge_collection_id || direction_byte
)
```

The file contains:

```text
header:
  magic                  8 bytes = "RGADJ001"
  version                u32le = 1
  header_len             u32le
  heap_id                16 bytes
  graph_id               16 bytes
  definition_revision    32 bytes
  generation_id          16 bytes
  edge_collection_id     16 bytes
  direction              u8: 1 outgoing, 2 incoming
  reserved               7 zero bytes
  entry_count            u64le
  block_count            u32le
  block_entry_limit      u32le = 4096
  generation_content_root 32 bytes
  payload_root           32 bytes
  header_crc32c           u32le over preceding header bytes

block directory, block_count entries:
  file_offset            u64le
  encoded_len            u32le
  entry_count            u32le, 1..=4096
  first_tuple_len        u32le
  first_tuple            canonical tuple bytes
  last_tuple_len         u32le
  last_tuple             canonical tuple bytes
  block_hash             BLAKE3-256(encoded block)

blocks:
  encoded_len            u32le
  entry_count            u32le
  repeated tuple_len u32le + tuple bytes
  block_crc32c            u32le over block prefix and tuples
```

Directory tuples are bounded by the 2,015-byte logical graph-key limit. Offsets and
lengths use checked arithmetic. Directory ranges are strictly increasing,
non-overlapping and cover every block. `payload_root` is BLAKE3-256 of the
concatenated directory `block_hash` values with domain
`RESIDIUUM-GRAPH-ADJACENCY-PAYLOAD-V1`. Trailing bytes, unknown version,
non-zero reserved bytes, unsorted/duplicate tuples or any checksum/hash mismatch
damage the affected artifact.

Publication writes both direction files and a deterministic-CBOR adjacency
manifest to a temporary build directory, syncs them, then atomically publishes
the manifest last. The manifest names every required edge collection/direction,
artifact ID, payload root, entry count, build ID and generation content root.
An unpublished or incomplete build is never current.

Every temporary file is created in its final parent filesystem. If an immutable
target already exists and fully verifies against the same identity/root it is
reused; if it is missing or damaged, a complete synced temporary replacement
is atomically installed and the parent directory is synced. The manifest is
subject to the same rule only after every named artifact verifies. Abandoned
temporary files are derived garbage and may be removed during graph validation
or maintenance, never during ordinary store/graph open.

The manifest identity is:

```text
manifest_id = BLAKE3-256(
  "RESIDIUUM-GRAPH-ADJACENCY-MANIFEST-V1" || canonical_manifest_cbor
)
```

The generation binding names that manifest identity. Exact byte layouts,
integer endianness, bounds, manifest map and sync/rename order are frozen in
[adjacency-v1.json](../../../spec/graph/adjacency-v1.json). Prose and that
machine contract must agree; an implementation may not substitute the generic
secondary-index format.

### 10.2 Host capability

```rust
fn lookup_graph_adjacency_page(
    lookup: GraphAdjacencyLookup,
) -> Result<GraphAdjacencyPage, Error>;

pub struct GraphAdjacencyLookup {
    pub heap_id: HeapId,
    pub graph_id: GraphId,
    pub definition_revision: GraphDefinitionRevision,
    pub generation_id: GraphGenerationId,
    pub edge_collection_id: CollectionId,
    pub direction: Direction,            // Outgoing or Incoming only; Both refuses
    pub endpoint: VertexLocator,
    pub edge_types: Vec<EdgeTypeId>,     // sorted unique; empty means all
    pub after: Option<GraphAdjacencyPosition>,
    pub limit: usize,                    // 1..=1000
}

pub struct GraphAdjacencyPage {
    pub candidates: Vec<GraphAdjacencyCandidate>,
    pub next: Option<GraphAdjacencyPosition>,
    pub exhausted: bool,
    pub artifact: GraphAdjacencyCoverage,
}

pub struct GraphAdjacencyCandidate {
    pub edge_type_id: EdgeTypeId,
    pub edge_collection_id: CollectionId,
    pub edge_key: String,                // logical key
}

pub struct GraphAdjacencyPosition {
    pub artifact_id: [u8; 32],
    pub block_index: u32,
    pub entry_index: u32,
    pub after_tuple: Vec<u8>,            // canonical tuple, at most 4,082 bytes
}
```

The host performs prefix/range selection only. The graph client/QVM owns
direction merge, record validation, coverage and result semantics.
Positions are host-internal, exclusive and untrusted on re-entry: artifact,
indices and tuple must agree with the verified file or the lookup refuses as
damage. They are never serialized directly as the public continuation.
`exhausted=true` requires `next=None`; otherwise a non-empty page has the
exclusive position of its last candidate. A zero-candidate non-exhausted host
page is forbidden.

An empty `edge_types` filter scans the endpoint prefix; a non-empty filter opens
one range per compatible type. The executor performs a bounded k-way merge
across `(edge collection, edge type range, requested direction)` in canonical
`(EdgeRef, actual_direction, adjacent_ref)` order. It holds at most one 1,000-key
page per source and never materializes full adjacency.

Only a verified published manifest covering every required source artifact for
the sealed generation can prove an empty neighbor set. Otherwise:

- `GraphCoveragePolicy::Complete` performs a bounded authoritative edge-collection
  fallback when its budget can cover the source, otherwise returns
  `graph_adjacency_unavailable` or `coverage_incomplete`;
- `GraphCoveragePolicy::IncompleteAllowed` may use verified surviving artifacts and returns explicit
  holes; and
- every candidate edge is loaded and revalidated against graph, definition,
  generation, endpoint, type and authoritative live version.

Loading another generation cannot stale or alter an already published artifact.
Deleting all `.gai` files loses no authority and validation/rebuild recreates
them from sealed canonical records.

## 11. Graph head and binding

The authoritative graph head contains:

```text
graph_id
name
current_definition_revision
active_generation: optional GraphGenerationId
state: Active | Disabled
sequence: non-zero u64
```

GRF-1 only creates an `Active` head at sequence 1 and replaces it during active
generation activation. It does not expose disable, rename, definition update or
delete.

Opening a graph resolves in this order:

```text
name selector: names[name] -> graph_id
id selector: graph_id directly
head[graph_id]
names[head.name] -> the same graph_id and definition revision
definition[graph_id/current_definition_revision]
requested generation or head.active_generation
generations[graph_id/generation_id] -> validated job/manifest
validate Heap, names, IDs, revision hash and state
```

A non-`Active`/absent name is `graph_not_found`, including an abandoned
`Creating` reservation and ID lookup before name publication. `Active` with no
active generation, or a missing exact generation, is
`graph_generation_not_found`. A generation record that exists without the
required `Validated` binding is `graph_generation_not_validated`; inconsistent
published identities or revision hashes are damage. Open reads only catalog,
head, definition, generation binding and adjacency manifest metadata; it never
scans source records or adjacency blocks.
Missing adjacency files/manifest do not erase the binding or make the graph
absent: open succeeds with derived availability recorded internally, and each
read applies its requested coverage policy. Structurally invalid authoritative
binding/job records still fail as damage.

The returned immutable binding is:

```rust
pub struct GraphBinding {
    pub heap_id: HeapId,
    pub graph_id: GraphId,
    pub name: GraphName,
    pub definition_revision: GraphDefinitionRevision,
    pub generation_id: GraphGenerationId,
    pub head_version: [u8; 16],
}
```

Every point and adjacency result reports this binding. A handle never silently
follows a later active-generation change. The application reopens or explicitly
refreshes to observe a newer binding.

## 12. Public async Rust API

Public types live under `residiuum_sdk::driver::graph` and are re-exported from
`residiuum_sdk::driver` where noted.

`GraphCatalogClient`, `GraphClient`, `GraphBulkClient` and
`GraphGenerationWriter` are cheap `Clone + Send + Sync` handles over the same
physical driver connection. Cloning creates no scheduler, writer, lock file or
shutdown domain. Dropping a graph handle does not close the shared client.

### 12.1 Capability surface

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphProfile {
    ModelV1,
    ClientV0_1,
    AdjacencyV1,
    BulkGenerationV1,
    TraversalV0_1,
    PathV0_1,
    PhysicalV1,
    IntegrityV1,
    AnalyticsV1,
    ClusterV1,
    GoldV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphCapabilities {
    bits: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GraphCoveragePolicy {
    #[default]
    Complete,
    IncompleteAllowed,
}

impl GraphCapabilities {
    pub fn supports(self, profile: GraphProfile) -> bool;
}
```

Bit allocation is fixed: bits 0–10 correspond to enum variants in declaration
order; bits 11–63 are reserved and zero. `GraphProfile` maps to the exact
profile strings in §4. Unknown wire profile strings are preserved for
diagnostics but evaluate unsupported.

`driver::Capabilities` gains:

```rust
pub graph: GraphCapabilities
```

At GRF-1 embedded clients advertise exactly ModelV1, ClientV0_1,
AdjacencyV1 and BulkGenerationV1. Every other graph profile is false.
Those four bits remain false when `Capabilities::atomics` is false or before
GRF-1 acceptance; graph entry points then return `graph_profile_unsupported`
without bootstrapping or mutating storage.

### 12.2 Catalog

```rust
impl HeapClient {
    pub fn graphs(&self) -> GraphCatalogClient;
}

#[derive(Clone)]
pub struct GraphCatalogClient { /* owns HeapClient clone */ }

impl GraphCatalogClient {
    pub async fn bootstrap(
        &self,
        options: GraphBootstrapOptions,
    ) -> Result<GraphBootstrapReceipt, Error>;

    pub async fn create(
        &self,
        definition: GraphDefinitionDraft,
        options: CreateGraphOptions,
    ) -> Result<GraphCreateReceipt, Error>;

    pub async fn open(
        &self,
        selector: GraphSelector,
        options: OpenGraphOptions,
    ) -> Result<GraphClient, Error>;

    pub async fn list_page(
        &self,
        options: GraphListOptions,
    ) -> Result<GraphListPage, Error>;

    pub fn bulk(&self, selector: GraphSelector) -> GraphBulkClient;
}
```

Selectors/options:

```rust
pub enum GraphSelector {
    Name(GraphName),
    Id(GraphId),
}

pub enum GenerationSelector {
    Active,
    Exact(GraphGenerationId),
}

pub struct OpenGraphOptions {
    pub generation: GenerationSelector,
    pub context: OperationContext,       // operation_id must be absent
}

pub struct GraphBootstrapOptions {
    pub context: OperationContext,       // operation_id required or minted once
}

pub struct CreateGraphOptions {
    pub context: OperationContext,       // operation_id required or minted once
}

pub struct GraphListOptions {
    pub page_size: usize,                // 1..=1000, default 64
    pub continuation: Option<GraphCatalogContinuation>,
    pub coverage: GraphCoveragePolicy,
    pub context: OperationContext,       // operation_id must be absent
}

pub struct GraphInfo {
    pub graph_id: GraphId,
    pub name: GraphName,
    pub definition_revision: GraphDefinitionRevision,
    pub active_generation: Option<GraphGenerationId>,
    pub head_version: [u8; 16],
}

pub struct GraphListPage {
    pub graphs: Vec<GraphInfo>,
    pub continuation: Option<GraphCatalogContinuation>,
    pub exhausted: bool,
    pub coverage: GraphCoverageEvidence,
}

pub struct GraphBootstrapReceipt {
    pub heap_id: HeapId,
    pub definitions_collection_id: CollectionId,
    pub names_collection_id: CollectionId,
    pub heads_collection_id: CollectionId,
    pub jobs_collection_id: CollectionId,
    pub generations_collection_id: CollectionId,
    pub collection_bindings_collection_id: CollectionId,
    pub request_id: RequestId,
    pub operation_id: OperationId,
    pub replayed: bool,
}

pub struct GraphCreateReceipt {
    pub heap_id: HeapId,
    pub graph_id: GraphId,
    pub name: GraphName,
    pub definition_revision: GraphDefinitionRevision,
    pub definition: GraphDefinition,
    pub definition_version: [u8; 16],
    pub head_version: [u8; 16],
    pub name_binding_version: [u8; 16],
    pub request_id: RequestId,
    pub operation_id: OperationId,
    pub replayed: bool,
}
```

`create` requires graph system bootstrap, creates graph-owned mapped
collections, writes definition/head/name in the safe order from §16.3 and
returns only after the name binding is active and durable. Required adjacency
artifacts are built by generation validation, not graph registration.

### 12.3 Point reads

```rust
#[derive(Clone)]
pub struct GraphClient { /* owns HeapClient clone + immutable GraphBinding */ }

impl GraphClient {
    pub fn binding(&self) -> &GraphBinding;
    pub fn definition(&self) -> &GraphDefinition;

    pub async fn vertex<P>(
        &self,
        id: VertexRef,
        options: GraphPointOptions,
    ) -> Result<GraphPointResult<VersionedVertex<P>>, Error>
    where P: DeserializeOwned + Send + Sync + 'static;

    pub async fn edge<P>(
        &self,
        id: EdgeRef,
        options: GraphPointOptions,
    ) -> Result<GraphPointResult<VersionedEdge<P>>, Error>
    where P: DeserializeOwned + Send + Sync + 'static;

    pub async fn vertices_page<P>(
        &self,
        options: VertexPageOptions,
    ) -> Result<VertexPage<P>, Error>
    where P: DeserializeOwned + Send + Sync + 'static;

    pub async fn edges_page<P>(
        &self,
        options: EdgePageOptions,
    ) -> Result<EdgePage<P>, Error>
    where P: DeserializeOwned + Send + Sync + 'static;

    pub async fn neighbors<P>(
        &self,
        vertex: VertexRef,
        options: NeighborOptions,
    ) -> Result<NeighborPage<P>, Error>
    where P: DeserializeOwned + Send + Sync + 'static;
}
```

Point reads return `GraphPointResult { value: None, .. }` only when the exact
authoritative key is absent and coverage proves that absence. A record at the
key with wrong metadata is `graph_record_mismatch`, not `None`. Under
`GraphCoveragePolicy::Complete`, an inability to prove absence is an error;
under `IncompleteAllowed`, it is `value: None` with `coverage.complete=false`
and a typed hole.

### 12.4 Values and pages

```rust
pub struct GraphPointResult<T> {
    pub value: Option<T>,
    pub coverage: GraphCoverageEvidence,
}

pub struct GraphPointOptions {
    pub coverage: GraphCoveragePolicy,
    pub context: OperationContext,       // operation_id must be absent
}

pub struct VertexPageOptions {
    pub vertex_types: Vec<VertexTypeId>, // sorted unique; empty means all
    pub page_size: usize,                // 1..=1000
    pub continuation: Option<GraphScanContinuation>,
    pub coverage: GraphCoveragePolicy,
    pub budget: GraphScanBudget,
    pub context: OperationContext,       // operation_id must be absent
}

pub struct EdgePageOptions {
    pub edge_types: Vec<EdgeTypeId>,     // sorted unique; empty means all
    pub page_size: usize,                // 1..=1000
    pub continuation: Option<GraphScanContinuation>,
    pub coverage: GraphCoveragePolicy,
    pub budget: GraphScanBudget,
    pub context: OperationContext,       // operation_id must be absent
}

pub struct GraphScanBudget {
    pub max_candidate_records: u64,
    pub max_source_bytes: u64,
    pub max_result_bytes: u64,
}

pub struct VertexPage<P> {
    pub rows: Vec<VersionedVertex<P>>,
    pub continuation: Option<GraphScanContinuation>,
    pub exhausted: bool,
    pub coverage: GraphCoverageEvidence,
}

pub struct EdgePage<P> {
    pub rows: Vec<VersionedEdge<P>>,
    pub continuation: Option<GraphScanContinuation>,
    pub exhausted: bool,
    pub coverage: GraphCoverageEvidence,
}

pub struct VersionedVertex<P> {
    pub id: VertexRef,
    pub labels: Vec<Label>,
    pub properties: P,
    pub version: [u8; 16],
    pub binding: GraphBinding,
}

pub struct VersionedEdge<P> {
    pub id: EdgeRef,
    pub label: Label,
    pub from: VertexRef,
    pub to: VertexRef,
    pub properties: P,
    pub version: [u8; 16],
    pub binding: GraphBinding,
}

pub enum Direction { Outgoing, Incoming, Both }

pub struct NeighborRow<P> {
    pub edge: VersionedEdge<P>,
    pub adjacent: VertexRef,
    pub direction: Direction,            // never Both in a row
}

pub struct NeighborOptions {
    pub direction: Direction,
    pub edge_types: Vec<EdgeTypeId>,     // empty means all declared types
    pub page_size: usize,                // 1..=1000
    pub continuation: Option<GraphContinuation>,
    pub coverage: GraphCoveragePolicy,
    pub budget: GraphAdjacencyBudget,
    pub context: OperationContext,       // operation_id must be absent
}

pub struct GraphAdjacencyBudget {
    pub max_candidate_edges: u64,
    pub max_source_bytes: u64,
    pub max_result_bytes: u64,
}

pub struct NeighborPage<P> {
    pub rows: Vec<NeighborRow<P>>,
    pub continuation: Option<GraphContinuation>,
    pub exhausted: bool,
    pub coverage: GraphCoverageEvidence,
    pub binding: GraphBinding,
    pub examined_edges: u64,
    pub examined_bytes: u64,
}
```

Vertex and edge scans visit distinct mapped collections in unsigned
`collection_id` order and the exact generation prefix within each collection in
logical-key UTF-8 order. Their stable row order is
`(collection_id, logical_key, type_id)`. Each body is authoritatively loaded and
validated before filtering or return. A nonmatching type consumes candidate and
byte budget. Unknown requested type IDs are validation errors. These APIs do
not accept property predicates, sort expressions or projection programs; those
belong to the QVM-backed GRF-2/RQL surface.

Default scan budget is 100,000 candidate records, 64 MiB source bytes, 16 MiB
result bytes and page size 128. An exhausted complete scan proves no further
matching row. Budget termination under `Complete` is `coverage_incomplete`;
under `IncompleteAllowed` it returns the bounded prefix, a continuation and an
explicit hole. No scan buffers more than one host page and one result page.

Default adjacency budget:

```text
max_candidate_edges = 100,000
max_source_bytes     = 64 MiB
max_result_bytes     = 16 MiB
page_size            = 128
```

`Both` merges outbound and inbound rows by canonical tuple
`(edge_ref, actual_direction, adjacent_ref)`. A self-loop therefore produces
two rows, one outgoing and one incoming. Canonical direction order is
`Outgoing < Incoming`; `Both` is a selector and never a row direction. Exact
duplicate tuples are removed.

### 12.5 Continuation

`GraphContinuation` is opaque authenticated bytes. Its payload binds:

```text
profile
heap_id
graph_id
definition_revision
generation_id
head_version
vertex_ref
direction
sorted edge_type filter
coverage policy
effective budget
last examined (edge_ref, actual_direction, adjacent_ref)
expiry
key_id
```

It uses the existing Heap-confined cursor keyring and MAC profile with a new
domain separator `RESIDIUUM-GRAPH-ADJACENCY-CURSOR-V1`. Default lifetime is 15
minutes and maximum lifetime is 24 hours. Any binding/filter/budget mismatch,
expiry or MAC failure returns `graph_continuation_invalid`. Clients never
decode it.

`GraphScanContinuation` uses the same keyring, lifetime, MAC rules and error,
with the domain `RESIDIUUM-GRAPH-SCAN-CURSOR-V1`. Its payload binds profile,
Heap, graph, definition, generation, head version, record kind, sorted type
filter, coverage policy, effective budget, current collection ID, last examined
logical key, expiry and key ID. A cursor resumes strictly after that key; it
never converts an incomplete prior prefix into complete coverage.

## 13. Bulk generation API

### 13.1 Manifest

```rust
pub struct GenerationManifest {
    pub job_id: GraphJobId,
    pub generation_id: GraphGenerationId,
    pub graph_id: GraphId,
    pub definition_revision: GraphDefinitionRevision,
    pub source_fingerprint: Vec<u8>,       // 1..=1024 opaque bytes
    pub expected_vertex_counts: Vec<(VertexTypeId, u64)>,
    pub expected_edge_counts: Vec<(EdgeTypeId, u64)>,
    pub expected_content_root: [u8; 32],
}
```

Count arrays contain every declared type exactly once, sorted by ID. The
manifest canonical root is:

```text
BLAKE3-256("RESIDIUUM-GRAPH-GENERATION-MANIFEST-V1" || canonical_manifest_cbor)
```

### 13.2 Content root

For each accepted canonical record compute:

```text
record_digest = BLAKE3-256(
  "RESIDIUUM-GRAPH-GENERATION-RECORD-V1"
  || kind_byte                         // 1 vertex, 2 edge
  || collection_id
  || u32be(key_utf8_length)
  || key_utf8
  || u64be(canonical_json_length)
  || canonical_json_bytes
)
```

Canonical JSON is exact in GRF-1:

- bytes are UTF-8 without BOM and strings are not Unicode-normalized;
- a decoder rejects duplicate object names before materializing a map;
- object names sort by their unescaped UTF-8 bytes and array order is retained;
- output has no insignificant whitespace and object separators are `,` and `:`;
- `"`, `\\`, backspace, tab, LF, form-feed and CR use `\"`, `\\`, `\b`,
  `\t`, `\n`, `\f` and `\r`; other U+0000..U+001F scalars use lowercase
  `\u00xx`; `/` and all other Unicode scalar values remain unescaped UTF-8;
- numbers are integers only, in `i64` or `u64` range, encoded as minimal base-10
  with no leading plus or zeroes and with zero encoded as `0`; and
- `true`, `false` and `null` use those exact lowercase spellings.

Stored bytes must equal re-encoding byte-for-byte. Floating numbers, exponents,
out-of-range integers, unpaired UTF-16 escapes, non-shortest escapes, unknown
wrapper fields and non-canonical bytes are refused. Callers encode exact decimal
values as strings under an application contract.

Sort `record_digest` values unsigned and compute:

```text
content_root = BLAKE3-256(
  "RESIDIUUM-GRAPH-GENERATION-CONTENT-V1"
  || u64be(record_count)
  || concat(sorted_record_digests)
)
```

For each mapped source collection, sort that collection's accepted record
digests unsigned and compute its generation-scoped frontier:

```text
source_frontier = BLAKE3-256(
  "RESIDIUUM-GRAPH-SOURCE-FRONTIER-V1"
  || collection_id
  || generation_id
  || u64be(record_count)
  || concat(sorted_collection_record_digests)
)
```

This frontier covers only the §4.3 generation prefix. Loading another
generation in the same physical collection cannot move it. Resume evidence and
adjacency publication bind these frontier values; a whole-collection sequence
or the generic secondary-index state is not a substitute.

### 13.3 Client

```rust
#[derive(Clone)]
pub struct GraphBulkClient {
    /* owns HeapClient clone + GraphSelector; no active generation required */
}

#[derive(Clone)]
pub struct GraphGenerationWriter {
    /* owns HeapClient clone + immutable graph/definition/generation/job binding */
}

pub struct GraphGenerationWriteBinding {
    pub heap_id: HeapId,
    pub graph_id: GraphId,
    pub definition_revision: GraphDefinitionRevision,
    pub generation_id: GraphGenerationId,
    pub job_id: GraphJobId,
    pub manifest_root: [u8; 32],
}

impl GraphBulkClient {
    pub async fn begin(
        &self,
        manifest: GenerationManifest,
        context: OperationContext,
    ) -> Result<GraphGenerationWriter, Error>;

    pub async fn status(
        &self,
        job_id: GraphJobId,
        context: OperationContext,
    ) -> Result<GraphPointResult<GenerationStatus>, Error>;

    pub async fn resume(
        &self,
        job_id: GraphJobId,
        context: OperationContext,
    ) -> Result<GraphGenerationWriter, Error>;

    pub async fn rebuild_adjacency(
        &self,
        job_id: GraphJobId,
        options: RebuildAdjacencyOptions,
    ) -> Result<GraphAdjacencyRebuildReport, Error>;
}

impl GraphGenerationWriter {
    pub fn binding(&self) -> &GraphGenerationWriteBinding;

    pub async fn put_vertex<P>(
        &self,
        id: VertexRef,
        properties: &P,
        context: OperationContext,
    ) -> Result<WriteReceipt, Error>
    where P: Serialize + Send + Sync;

    pub async fn put_edge<P>(
        &self,
        id: EdgeRef,
        from: VertexRef,
        to: VertexRef,
        properties: &P,
        context: OperationContext,
    ) -> Result<WriteReceipt, Error>
    where P: Serialize + Send + Sync;

    pub async fn put_vertices<P>(
        &self,
        values: Vec<VertexWrite<P>>,
        context: OperationContext,
    ) -> Result<Vec<PutManyOutcome>, Error>;

    pub async fn put_edges<P>(
        &self,
        values: Vec<EdgeWrite<P>>,
        context: OperationContext,
    ) -> Result<Vec<PutManyOutcome>, Error>;

    pub async fn validate(
        &self,
        options: ValidateGenerationOptions,
    ) -> Result<GenerationValidationReport, Error>;

    pub async fn activate(
        &self,
        options: ActivateGenerationOptions,
    ) -> Result<GraphActivationReceipt, Error>;
}

pub struct VertexWrite<P> {
    pub id: VertexRef,
    pub properties: P,
}

pub struct EdgeWrite<P> {
    pub id: EdgeRef,
    pub from: VertexRef,
    pub to: VertexRef,
    pub properties: P,
}

pub struct ValidateGenerationOptions {
    pub max_documents: u64,
    pub max_bytes: u64,
    pub max_memory_bytes: usize,          // 8 MiB..=1 GiB; default 64 MiB
    pub max_temporary_bytes: u64,         // 16 MiB..=1 TiB; default 4 GiB
    pub max_violations: usize,           // 1..=1000
    pub context: OperationContext,       // operation_id must be absent
}

pub struct ActivateGenerationOptions {
    pub if_head_version: [u8; 16],
    pub context: OperationContext,       // operation_id required or minted once
}

pub struct RebuildAdjacencyOptions {
    pub max_memory_bytes: usize,          // same bounds/default as validation
    pub max_temporary_bytes: u64,         // same bounds/default as validation
    pub context: OperationContext,       // operation_id must be absent
}

pub struct GraphAdjacencyRebuildReport {
    pub binding: GraphBinding,
    pub adjacency_manifest_id: [u8; 32],
    pub artifacts: Vec<GraphAdjacencyCoverage>,
    pub replaced_files: u64,
    pub reused_files: u64,
    pub coverage: GraphCoverageEvidence,
}

pub enum GenerationState {
    Declared,
    Loading,
    Validating,
    Validated,
    Activated,
    Rejected,
    Damaged,
}

pub struct GenerationStatus {
    pub manifest: GenerationManifest,
    pub manifest_root: [u8; 32],
    pub state: GenerationState,
    pub computed_vertex_counts: Vec<(VertexTypeId, u64)>,
    pub computed_edge_counts: Vec<(EdgeTypeId, u64)>,
    pub computed_content_root: Option<[u8; 32]>,
    pub violation_count: u64,
    pub reported_violations: Vec<GraphViolation>,
    pub failure_code: Option<ErrorCode>,
    pub sequence: u64,
}

pub struct GenerationValidationReport {
    pub status: GenerationStatus,
    pub source_coverage: GraphCoverageEvidence,
    pub adjacency_artifacts: Vec<GraphAdjacencyCoverage>,
}

pub struct GraphActivationReceipt {
    pub graph_id: GraphId,
    pub previous_generation: Option<GraphGenerationId>,
    pub generation_id: GraphGenerationId,
    pub previous_head_version: [u8; 16],
    pub head_version: [u8; 16],
    pub request_id: RequestId,
    pub operation_id: OperationId,
    pub replayed: bool,
}
```

`begin` resolves an `Active` graph name/definition, validates and roots the
manifest, then creates the job as `Declared` and version-CAS advances it to
`Loading` before returning a writer. A crash between those steps is resumed by
the same `job_id`; the identical manifest advances/replays, while changed
manifest bytes return `operation_identity_conflict`. `resume` returns a bound
writer for `Loading`, `Validating`, `Validated` and `Activated`: methods outside
the state's legal set refuse without mutation. This permits validation resume
and same-operation activation outcome replay. `Rejected` and `Damaged` expose
status but cannot resume. `resume` on an absent job is `graph_job_not_found`;
`status` reports covered `None`. A method incompatible with an existing job
state is `graph_job_state_conflict`.

`put_vertices` and `put_edges` are pipelined, not atomic. Each accepts
`1..=min(1000, client_queue_capacity)` entries and at most
`min(16 MiB, client_queue_byte_capacity)` aggregate canonical key/body bytes.
The outer error is pre-admission only. After admission it returns one outcome
per input in input order; every member has its own job-derived operation ID,
durable receipt or terminal error. Mixed success is explicit and the caller
retries only failed members with the same job and identities. No method
materializes a generation-sized vector. `PutManyOutcome.key` and any public
receipt/error key field expose the logical key, never the §4.3 physical
encoding.

The writer always inserts its manifest generation/definition/graph metadata.
Caller-supplied IDs with another binding refuse before admission.
`begin`, `resume`, `status`, record writes, `validate` and
`rebuild_adjacency` require an absent
`context.operation_id`; their durable member identities derive from `job_id`.
Only `activate` uses its caller-visible parent `OperationId`.

`validate`:

1. inventories exactly the mapped collections for the generation;
2. rejects malformed/mismatched records;
3. computes counts and content root;
4. verifies every strict edge endpoint exists as a matching authoritative
   vertex in the same generation;
5. records deferred missing endpoints as violations;
6. builds all required generation-scoped adjacency artifacts;
7. proves their exact coverage at the validation frontier;
8. rechecks source frontier drift;
9. publishes the verified adjacency manifest file; and
10. submits one LocalHeap Atomic that creates the immutable generation binding
    and version-CAS replaces the job as `Validated`; its committed decision is
    the validation visibility point.

`max_documents` and `max_bytes` are positive per-invocation work limits, not
claims that validation finished. At a clean record/block boundary, exhausted
work budget durably stores the exact validation frontier, accumulators and
bounded job-owned adjacency work fragments, keeps the job `Validating`, and
returns driver `CoverageIncomplete` with `SafeSameRequest`. Work fragments are
never referenced by a manifest and prove no coverage; restart reuses them only
after their job, source-frontier and hash checks pass, otherwise it discards
them. A later `validate` resumes from that frontier under a new request ID.
`max_violations` only limits the returned sorted sample; the durable
`violation_count` is exact and the job retains the first 1,000 violations in
canonical order. No partial validation report or artifact can be mistaken for
`Validated`.

The validator uses external sorted runs for record digests, violations and
adjacency tuples. A run, merge fan-in and output buffers together stay within
`max_memory_bytes`; merge fan-in is at most 16. Job-owned temporary bytes stay
within `min(max_temporary_bytes, deployment_derived_work_quota)`. Exceeding
either limit before publication returns `resource_limit`, keeps the job
`Validating` and preserves only already verified bounded work fragments. It
never falls back to generation-sized RAM or unaccounted temporary storage.

Work paths are deterministic
`graph/work/<job_id_lower_hex>/<kind_u8_decimal>/<collection_id_or_zero_lower_hex>/<ordinal_u64_lower_hex_16>.grw`.
The job's exact `validation_frontier` and at most 2,048 `work_fragment` records
are defined in `cbor-v1.json`; progressive merges keep that cap. A fragment is
reusable only when its complete-file BLAKE3 hash, length, count and job/source
binding verify. Missing or invalid work is disposable: validation atomically
rewinds its derived frontier to the earliest affected phase and recomputes from
the sealed authoritative generation. Work files are never consulted by graph
reads and are removed after `Validated`, `Rejected` or `Damaged` publication.

`rebuild_adjacency` is permitted only for a `Validated` or `Activated` sealed
job and requires `Read + IndexAdmin`. It reconstructs the exact artifact bytes,
payload roots, artifact IDs, build ID, manifest bytes and manifest ID already
named by the immutable generation binding. It atomically replaces only missing
or damaged derived files and does not rewrite the job, binding, head or source
records. If sealed authority no longer reproduces every recorded root/count,
it returns `data_damaged` and publishes nothing. It is explicit maintenance:
ordinary deployment open, graph open and reads never invoke it.

The `Loading -> Validating` transition is serialized at the graph-write
admission boundary after all earlier admitted member writes have reached a
terminal outcome. Later member writes return `graph_generation_changed` before
admission. Active generations are immutable. Graph-owned collection bindings
reject ordinary `Collection<T>` mutation before admission; only the internal
generation writer holding a valid `GraphWritePermit` can mutate a Loading
generation. If a sealed source frontier nevertheless moves, validation marks
the job `Damaged` and returns `data_damaged`; a bypass is a storage defect, not
a supported degraded mode.

`activate` submits one LocalHeap Atomic containing a version-CAS replacement of
the graph head and a version-CAS replacement of the job from `Validated` to
`Activated`. It requires:

- job `Validated`;
- matching immutable generation binding;
- matching graph/definition/generation/manifest;
- a complete, verified adjacency manifest naming every required artifact;
- `if_head_version` equal to the binding used by the caller; and
- a stable `OperationId`.

Both records become visible at one Heap commit position. On success, the old
generation remains authoritative but inactive. Collection of old generations
is excluded from GRF-1. Lost reply uses Atomic outcome lookup; the same parent
operation returns the original activation receipt.

## 14. Bulk job states

```text
Declared
  -> Loading
  -> Validating
  -> Validated
  -> Activated

Validating -> Rejected
Declared | Loading | Validating | Validated -> Damaged
```

The durable job record stores manifest/root, state, counts, computed root,
validation frontier, adjacency build/artifact IDs, violations, last phase,
failure code and activation head version. State transitions are monotonic. `Rejected` and
`Damaged` are terminal: correction or revalidation uses a new job and
generation identity. An administrator may verify authority before doing so,
but no terminal job self-repairs.

`Rejected` means complete validation proved caller-authored content violates
the manifest or graph definition. `Damaged` means required durable bytes,
metadata, hashes or recovery evidence are internally inconsistent or unreadable.
Budget/deadline/cancellation and retriable I/O failures are neither state.

## 15. Authorization

GRF-1 uses existing Heap rights; it does not allocate a new certificate bit.

| Operation | Required rights |
|---|---|
| bootstrap internal collections | `HeapAdmin + Write + Read` |
| create graph definition/head/name | `HeapAdmin + Write + Read` |
| list/open graph | `Read` |
| point vertex/edge read | `Read` |
| neighbors | `Read` |
| begin/load generation | `Write + Read` |
| validate generation | `Read + IndexAdmin` |
| rebuild adjacency | `Read + IndexAdmin` |
| activate generation | `Write + Read` |

Every mapped collection remains confined by the same Heap capability. GRF-1
does not support a graph cap narrower than its Heap cap. Separate attenuable
graph rights remain a later gold-profile requirement and must not be claimed.
Graph-internal Atomics recheck the caller rights in this table, then execute
with a non-serializable engine capability bound to the exact graph/job plan;
they do not grant callers general system-collection write access.

## 16. Idempotency and safe publication protocols

### 16.1 Parent operation

Graph bootstrap, create and activate accept one parent `OperationId`. The SDK
mints it once when absent and reports it in receipts/errors. A final LocalHeap
Atomic uses the parent as its Atomic identity. The parent is never reused as
the identity of several independently committed ordinary mutations.

### 16.2 Derived member operation IDs

Independently committed bootstrap, reservation, collection-metadata and bulk
member stages derive identities as follows. Members inside a LocalHeap Atomic
use the Atomics v1 member identity rules instead and are not separately
submitted. For a stage tag and target:

```text
digest = BLAKE3-256(
  "RESIDIUUM-GRAPH-MEMBER-OPERATION-V1"
  || parent_operation_id
  || u16be(stage_tag_length)
  || stage_tag_utf8
  || collection_id
  || u16be(key_length)
  || key_utf8
)
member_operation_id = first_16_bytes(digest)
```

If all 16 bytes are zero, set the final byte to 1. Member IDs never escape as
the caller's operation identity.

Bulk record member IDs use `job_id` in place of parent operation ID and stage
tag `vertex` or `edge`. Reusing job/id/target with changed canonical content
therefore produces the existing operation identity conflict.

Bootstrap collection IDs do not exist before their create operations. Their
member identity is derived separately as:

```text
first_16_bytes(BLAKE3-256(
  "RESIDIUUM-GRAPH-BOOTSTRAP-OPERATION-V1"
  || parent_operation_id
  || heap_id
  || u16be(reserved_collection_name_length)
  || reserved_collection_name_utf8
))
```

The all-zero correction is identical. No other operation uses this special
domain.

Validation's final two-member Atomic has no caller mutation identity. Its
stable Atomic `OperationId` is the first 16 bytes of:

```text
BLAKE3-256(
  "RESIDIUUM-GRAPH-VALIDATION-ATOMIC-OPERATION-V1"
  || heap_id || graph_id || generation_id || job_id || manifest_root
)
```

The same all-zero correction applies. Reuse with changed content is an
operation identity conflict and marks the job `Damaged`.

### 16.3 Create graph ordering

GRF-1 reserves the name first, prepares graph-owned collections, then uses one
LocalHeap Atomic as the visibility point:

1. create name reservation in `Creating`, bound to graph and parent operation;
2. create each graph-owned collection plus collection-binding atomically at the
   Heap administrative metadata boundary;
3. finalize collection IDs and encode the immutable definition record;
4. submit one LocalHeap Atomic containing definition `Create`, initial-head
   `Create`, and name-reservation version-CAS `Replace` from `Creating` to
   `Active`;
5. require one committed Atomic decision before any of those three records is
   visible;
6. return receipt.

Only `Active` name bindings are enumerated or openable, including open by
`GraphId`. Collections before step 4 are not a published graph and are reusable
only by the same parent operation. If step 1 finds the
same name bound to another graph or parent operation, return
`graph_name_conflict`; never overwrite. A retry with the same parent operation
revalidates every completed stage and returns the original logical receipt.

A permanently abandoned `Creating` reservation requires an explicit future
administrative cleanup operation; GRF-1 reports it but does not guess that the
creator is dead. It cannot be opened or mistaken for an active graph.

The Atomic plan has exactly three members in three system collections and binds
the parent operation identity through the normal Atomics plan identity rules.
Lost reply is resolved through Atomic outcome lookup; replay returns the same
logical graph receipt. This protocol is not precedent for arbitrary graph
transactions.

## 17. Error contract

`driver::ErrorCode` gains these variants and exact strings:

| Variant | String | Class | Retry |
|---|---|---|---|
| `GraphNotFound` | `graph_not_found` | Request | Never |
| `GraphProfileUnsupported` | `graph_profile_unsupported` | Request | Never |
| `GraphDefinitionInvalid` | `graph_definition_invalid` | Request | Never |
| `GraphDefinitionDamaged` | `graph_definition_damaged` | Service | Never |
| `GraphNameConflict` | `graph_name_conflict` | Request | Never |
| `GraphHeapMismatch` | `graph_heap_mismatch` | Request | Never |
| `ReservedCollection` | `reserved_collection` | Request | Never |
| `GraphReservedCollectionConflict` | `graph_reserved_collection_conflict` | Service | Never |
| `GraphCollectionOwned` | `graph_collection_owned` | Request | Never |
| `GraphJobNotFound` | `graph_job_not_found` | Request | Never |
| `GraphJobStateConflict` | `graph_job_state_conflict` | Request | Never |
| `GraphGenerationNotFound` | `graph_generation_not_found` | Request | Never |
| `GraphGenerationChanged` | `graph_generation_changed` | Request | Never |
| `GraphGenerationNotValidated` | `graph_generation_not_validated` | Request | Never |
| `GraphHeadConflict` | `graph_head_conflict` | Request | Never |
| `GraphRecordMismatch` | `graph_record_mismatch` | Service | Never |
| `GraphEndpointMissing` | `graph_endpoint_missing` | Request | Never |
| `GraphAdjacencyUnavailable` | `graph_adjacency_unavailable` | Service | SafeSameRequest |
| `GraphContinuationInvalid` | `graph_continuation_invalid` | Request | Never |

Existing codes remain authoritative for `PermissionDenied`, `ResourceLimit`,
`DataDamaged`, `Overloaded`, cancellation, deadline and operation identity
conflict. Coverage failure gains a distinct driver-level `CoverageIncomplete`
variant rather than mapping to `DataDamaged`; this correction applies to all
driver reads, not only graph calls.

English messages are not an application interface. Errors and receipts carry
request ID and relevant operation ID under existing driver rules.

## 18. Coverage contract

```rust
pub struct GraphCoverageEvidence {
    pub complete: bool,
    pub policy: GraphCoveragePolicy,
    pub adjacency_manifest_id: Option<[u8; 32]>,
    pub metadata_sources: Vec<GraphSourceCoverage>,
    pub vertex_sources: Vec<GraphSourceCoverage>,
    pub edge_sources: Vec<GraphSourceCoverage>,
    pub adjacency: Vec<GraphAdjacencyCoverage>,
    pub known_holes: Vec<GraphHole>,
    pub terminated_by: GraphTermination,
}

pub struct GraphSourceCoverage {
    pub collection_id: CollectionId,
    pub frontier: Option<[u8; 32]>,
    pub complete: bool,
    pub examined_records: u64,
    pub examined_bytes: u64,
}

pub struct GraphAdjacencyCoverage {
    pub collection_id: CollectionId,
    pub artifact_id: [u8; 32],
    pub build_id: [u8; 16],
    pub direction: Direction, // Outgoing or Incoming only
    pub generation_content_root: [u8; 32],
    pub payload_root: [u8; 32],
    pub entry_count: u64,
    pub complete: bool,
}

pub struct GraphHole {
    pub code: String,
    pub collection_id: Option<CollectionId>,
    pub key: Option<String>,
}

pub enum GraphTermination {
    Exhausted,
    PageBoundary,
    Budget,
    Deadline,
    Cancelled,
}

pub struct GraphViolation {
    pub code: String,
    pub kind: GraphRecordKind,
    pub collection_id: CollectionId,
    pub key: Option<String>,
    pub endpoint: Option<VertexLocator>,
}
```

Sources/adjacency artifacts sort by immutable collection/artifact identity. A
mapped-generation source supplies the §13.2 frontier; a metadata point/page
whose underlying host surface has no stable frontier reports `None` and cannot
use it as cross-request snapshot evidence. Unused metadata/vertex/edge source
vectors are empty. Holes include a
stable code and optional redacted collection/key. GRF-1 termination is
`Exhausted`, `PageBoundary`, `Budget`, `Deadline` or `Cancelled`.

Violation order is `(kind_byte, collection_id, key, code, endpoint)`; hole
order is `(collection_id, key, code)`. `None` sorts before `Some`, strings sort
by UTF-8 bytes and endpoints use `VertexLocator` canonical order. Exact
duplicates collapse only after their total `violation_count` contribution has
been recorded.

Rules:

- a page boundary is complete for the page and does not claim exhaustion;
- `exhausted=true` plus `complete=true` proves the neighbor set complete for
  the binding/filter;
- an empty page with incomplete coverage is not proof of no neighbor;
- a missing edge body is a hole, never silently skipped;
- a malformed edge candidate is `graph_record_mismatch` under complete policy
  and a typed hole under incomplete policy; and
- candidate adjacency coverage and authoritative record coverage are reported
  separately.

## 19. Reference source-analysis corpus

The machine-readable corpus is
[source-analysis-v1.json](../../../spec/graph/source-analysis-v1.json).
It contains:

- one Heap and graph;
- `Package` and `Module` vertex types;
- `CONTAINS` and `DEPENDS_ON` edge types;
- two packages, four modules;
- a directed dependency cycle;
- one parallel dependency edge;
- one self-loop;
- expected outgoing, incoming and both pages;
- strict missing-endpoint rejection;
- deferred endpoint violation;
- wrong graph/generation/type cases; and
- exact empty-neighbor and incomplete-empty negative controls.

GRF-0's oracle consumes this file. GRF-1 embedded tests consume the same file;
they may not duplicate expected answers in Rust literals.

## 20. Package work breakdown

### GRF-0.1 — crate and identity kernel

- add workspace crate and public IDs/names/references;
- enforce widths, UUID, name and key constraints;
- canonical ordering and serde display forms;
- compile-time `Send + Sync` assertions; and
- property tests for parse/display/order.

### GRF-0.2 — definition codec

- implement exact CBOR maps and limits;
- definition revision hashing;
- accepted/rejected golden vectors;
- hostile length/depth/duplicate/unknown-key corpus; and
- determinism across input order.

### GRF-0.3 — record codec

- canonical JSON graph wrappers;
- exact numeric/refusal profile;
- schema conformance tests;
- content/manifest hashing; and
- record mismatch diagnostics.

### GRF-0.4 — independent oracle

- in-memory authority maps by collection/key;
- point and one-hop direction/type semantics;
- coverage and damage outcomes;
- canonical pagination; and
- source-analysis corpus runner.

### GRF-0 exit review

- machine artifacts validate;
- all golden vectors pass;
- oracle is pure and cannot import SDK/store;
- API docs deny recursive traversal; and
- architects sign `GRF-0` manifest.

### GRF-1.1 — bootstrap and reservation

- reserve internal collection namespace;
- idempotent internal bootstrap;
- existing collision refusal/OpenReport observation;
- graph capability fields; and
- authority tests.

### GRF-1.2 — catalog and bindings

- definition/head/name codecs in system collections;
- reserved-name plus three-member Atomic graph publication;
- list/open by name/id;
- immutable binding; and
- crash/retry matrix.

### GRF-1.3 — canonical point records

- GraphClient point vertex/edge reads;
- metadata validation and typed properties;
- versions and exact absence;
- cross-binding refusal; and
- damage cases.

This package also delivers bounded vertex/edge source pages, their authenticated
scan continuation, generation-prefix isolation, type filters and exhaustive vs
budget-limited coverage tests.

### GRF-1.4 — generation adjacency artifacts

- immutable file/manifest codec and structural validation;
- canonical sorted/deduplicated paging;
- cursor and coverage evidence;
- missing/unpublished/partial/damaged artifact behavior; and
- high-degree allocation bounds.

### GRF-1.5 — neighbors

- outgoing/incoming/both and type filter;
- authoritative edge revalidation;
- self-loop and parallel-edge rules;
- opaque continuation; and
- oracle differential tests.

### GRF-1.6 — bulk generation

- job/manifest records and derived operation IDs;
- canonical vertex/edge writers and bounded batches;
- restart/status;
- validation/count/root/endpoint checks;
- adjacency build/frontier check; and
- activation CAS.

### GRF-1.7 — recovery and qualification

- failpoints for every visibility/job transition;
- reopen after kill at each phase;
- wipe-derived rebuild;
- corrupt adjacency/body/job/definition cases;
- mixed non-graph collection regression suite;
- source-analysis journey and performance envelope; and
- capability flag last.

## 21. Required failpoints

```text
graph.bootstrap.after_collection.N
graph.create.after_name_reservation
graph.create.after_collection.N
graph.create.before_publication_atomic
graph.create.after_publication_submit
graph.create.after_publication_decision
graph.bulk.after_manifest
graph.bulk.after_record.N
graph.validate.after_inventory
graph.validate.after_content_root
graph.validate.after_endpoint_check
graph.validate.after_adjacency.N
graph.validate.after_manifest_publish
graph.validate.before_validated_atomic
graph.validate.after_validated_submit
graph.rebuild.after_artifact.N
graph.rebuild.before_manifest
graph.rebuild.after_manifest
graph.activate.before_atomic
graph.activate.after_submit
graph.activate.after_decision
graph.adjacency_page.after_candidates
graph.neighbors.after_edge.N
```

Test-only `N` controls use existing failpoint conventions and do not enter
production configuration.

## 22. Acceptance commands and artifacts

Developers add these stable entry points:

```text
cargo test -p residiuum-graph
cargo test -p residiuum-sdk --test grf1_catalog
cargo test -p residiuum-sdk --test grf1_point
cargo test -p residiuum-sdk --test grf1_scan
cargo test -p residiuum-sdk --test grf1_adjacency
cargo test -p residiuum-sdk --test grf1_bulk_generation
cargo test -p residiuum-sdk --test grf1_recovery
cargo test -p residiuum-sdk --test grf1_authority
bash scripts/check_query_runtime_architecture.sh
bash scripts/verify-graph-grf01.sh
```

`verify-graph-grf01.sh` runs the accepted subset and emits:

```text
out/graph/grf01/report.json
out/graph/grf01/junit.xml
out/graph/grf01/corpus-results.json
out/graph/grf01/crash-matrix.json
out/graph/grf01/performance.json
out/graph/grf01/manifest.json
```

The exact suite/claim requirements are in
[acceptance-v1.json](../../../spec/graph/acceptance-v1.json).

## 23. Minimum performance envelope

GRF-1 is primarily a correctness package, but it must reject pathological
implementation shape. On the controlled local benchmark host, publish:

- point vertex/edge p50/p95/p99;
- 1, 10, 100, 1,000 and 100,000 degree adjacency pages;
- cold/warm cache;
- page sizes 1, 16, 128 and 1,000;
- direct/incoming/both and one/four/all type filters;
- bulk ingestion at 100K and 1M total records;
- validation and adjacency-build duration;
- reopen with intact and deleted adjacency artifacts;
- CPU, peak RSS, physical bytes read/written and adjacency size.

Hard correctness/resource gates:

- no allocation proportional to the entire posting list for a page of 1,000;
- client buffering at most two result pages;
- no full collection scan on ordinary graph open;
- no graph adjacency rebuild on ordinary deployment open;
- adjacency work respects all effective budgets; and
- a 100K-degree first page begins without materializing 100K edge bodies.

No fixed latency/throughput competitive claim is frozen in GRF-1. The report
establishes the baseline that GRF-2/GRF-4 will improve. A regression against
ordinary equivalent indexed collection lookup must be explained and accepted.

## 24. Definition of done

GRF-0 is done only when its pure artifacts, oracle and vectors are accepted.

GRF-1 is done only when:

1. the full source-analysis journey passes through `driver::GraphClient`;
2. one `Client` can host two authorized Heaps and multiple graphs without a
   second writer/scheduler;
3. point/adjacency results carry versions, binding and coverage;
4. immutable generation publication survives crash and lost reply;
5. the same logical key coexists in active and loading generations without
   overwrite or query interference;
6. artifact/fallback/rebuilt paths match the independent oracle;
7. missing/unpublished/partial/damaged artifacts cannot prove empty adjacency;
8. reserved system collection collisions are contained and non-destructive;
9. all queues, pages, fallback scans and bulk writes are bounded;
10. capability negotiation exposes only the four accepted GRF-1 profiles;
11. recursive/path/algorithm calls do not exist or refuse stably;
12. the architecture gate finds no second query/traversal executor; and
13. the architects accept the machine manifest.

## 25. Implementation freedom

Developers may choose:

- internal module splitting;
- allocation strategy below declared limits;
- iterator and async channel implementation;
- in-memory representation beneath the frozen adjacency file/manifest codec;
- batching and group-commit tuning without changing per-record receipts;
- caching keyed by full graph binding and authorization; and
- additional tests and telemetry.

Developers must ask for an amendment before changing:

- any public/durable field, ID, domain separator, profile or error string;
- graph/system collection or adjacency artifact names;
- identity/order/path semantics;
- safe publication order;
- authority or coverage meaning;
- public API topology;
- GRF-1 scope or non-claims; or
- the rule that recursive graph semantics belong only to QVM.
