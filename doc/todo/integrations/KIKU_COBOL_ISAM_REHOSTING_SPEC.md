# Kiku / COBOL / ISAM lateral-rehosting specification

Status: **provisional architecture v0.1; refinement required before developer
handoff or product claim**

Date: 2026-08-12

Programme ID: `KCR`

Owners: Residiuum architecture/governance and Kiku language architecture

Critical-path status: **not admitted**. This document records a future product
direction. It does not authorize implementation or displace Atomics, remaining
RQL qualification, Graph, or Cluster work.

## 1. Decision

Residiuum and Kiku will be designed to support a lateral shift of eligible
COBOL indexed-file applications:

```text
COBOL source + copybooks                  ISAM data export
              |                                  |
              v                                  v
     COBOL -> Kiku cross-compiler       bounded record inventory
              |                                  |
              v                                  v
       Kiku program + compiled Hopper record/schema descriptors
                         |
                         v
          Kiku indexed-file compatibility runtime
                         |
                         v
                Residiuum Heap/collections
```

The target is not relational migration. It does not require SQL, table design,
normalization, an ORM, floating-point conversion, JSON conversion, or manual
business-logic rewriting.

For an explicitly admitted source dialect and application envelope, the
customer supplies COBOL source, copybooks, file declarations and a consistent
ISAM data export. The toolchain:

1. cross-compiles supported COBOL meaning into Kiku;
2. compiles PIC and record declarations into bounded Hopper descriptors;
3. derives Residiuum collection and ordered-index definitions;
4. imports authoritative record bodies without logical field conversion;
5. binds generated Kiku file operations to Residiuum; and
6. produces differential evidence against the source COBOL runtime.

The business records retain their field layouts and decimal meaning. The
program retains its indexed-file operating model. Residiuum replaces the
physical ISAM implementation, not the application's business ontology.

## 2. Product thesis

The intended product statement is:

> Give us an eligible COBOL application, its copybooks and a consistent ISAM
> export. Kiku preserves the admitted program, record and base-10 arithmetic
> semantics; Residiuum preserves and serves the authoritative records with
> explicit durability, concurrency, recovery and damage evidence. No
> relational redesign or manual business-logic rewrite is required. RQL is an
> optional additional view over the same records.

The word **eligible** is mandatory. “COBOL,” “ISAM,” and file semantics are
families of dialects and implementations, not single universal formats. A
compatibility scanner must identify the admitted envelope before any promise.

## 3. Meaning of lateral shift

For this programme, lateral shift means:

- COBOL source becomes generated Kiku source or Kiku IR;
- COBOL file operations become calls to a Kiku compatibility runtime;
- raw business-record representation remains governed by the same admitted
  layout and physical field codecs;
- decimal values retain coefficient, precision, scale, sign and declared
  rounding/truncation meaning;
- primary and alternate key behavior remains observationally equivalent;
- the source program's file-status and control-flow decisions remain
  equivalent; and
- Residiuum becomes the physical authority and recovery system.

It does **not** mean that an unchanged COBOL binary continues opening the
vendor's original ISAM files. Binary ABI emulation, filesystem interception and
vendor-runtime replacement are outside the initial programme.

It also does not require byte-identical ISAM container files or indexes after
cutover. The authoritative record bytes and their observable file behavior
matter; proprietary pages, free lists and tree layouts do not.

## 4. Fundamental invariants

### KCR-I1 — one record interpretation

Kiku execution, import validation, RQL projection and index construction use
the same compiled Hopper descriptor identity. Residiuum does not contain an
independent PIC/copybook interpreter.

### KCR-I2 — authoritative record bytes

The admitted external record bytes are the authoritative application body.
Decoded objects, SDA values, JSON renderings, primary keys and alternate keys
are views or derived state unless a later profile explicitly declares
otherwise.

### KCR-I3 — no hidden decimal conversion

An admitted decimal never passes through IEEE binary floating point. Its
logical coefficient, scale, precision, sign, size-error and rounding behavior
are explicit from source semantics through Hopper, SDA/RQL and re-encoding.

### KCR-I4 — no semantic approximation

An unsupported statement, representation, collation, locking behavior,
overlay, status outcome or arithmetic case is a compatibility refusal. It is
never silently translated into a “close enough” operation.

### KCR-I5 — database authority remains Residiuum

Residiuum's ordinary Heap confinement, version identity, durable operation
identity, Atomics, Recovery Shadow, damage containment, coverage and salvage
rules apply. The compatibility layer does not add a second WAL, recovery
system, transaction coordinator or writer domain.

### KCR-I6 — derived deletion is harmless to authority

Deleting indexes, projections, query caches or import work files cannot delete
or redefine authoritative records. Rebuild either reproduces exact derived
meaning from the bound descriptor and record versions or reports damage.

### KCR-I7 — bounded execution

Compilation, import, projection, index construction, sequential reads,
arithmetic and result production have declared limits. No application-sized
buffer, unbounded cursor session, hidden heap decimal, runtime PIC parser or
unbounded recovery pass is permitted.

### KCR-I8 — proof precedes compatibility claim

An application is compatible only after static coverage and differential
execution evidence pass for its exact source dialect, compiler/runtime
versions, schemas, collations and workload envelope.

## 5. Existing foundations

### 5.1 Kiku/Hopper foundation observed at v0.1

The design assumes the experimental Kiku capabilities described to architecture
on 2026-08-12:

- compile-time PIC literals with canonical descriptors and interpreter identity;
- fixed byte-record layouts folded from typed Shapes;
- stable Symbol field identity;
- borrowed, length-bearing `ByteView` projections;
- DISPLAY and COMP-3 record codecs;
- exact scaled `i64` and arena-backed wide decimals;
- decimal comparison and bounded arithmetic;
- explicit rounding modes and `RoundingRequired`;
- caller-bounded scratch;
- failure-atomic encoding;
- equal-width overlays; and
- algebraic bounds, representation, overlay and allocation failures.

These are inputs to the architecture, not yet a production compatibility
claim. Edited signs, COMP, COMP-2, wide-decimal record adapters,
discriminator-governed alternatives and complete source-dialect semantics must
be tracked by profile.

### 5.2 Residiuum foundation

Residiuum already provides or is separately delivering:

- opaque byte storage;
- JSON and byte body tags in the legacy SDK;
- one bounded async driver connection with multiple Heap bindings;
- durable mutation identities and version-bearing reads;
- create-if-absent and version-CAS mutation;
- bounded scans and ordered derived-index foundations;
- LocalHeap Atomics under its independent qualification programme;
- Recovery Shadow, salvage and explicit payload completeness;
- RQL/SDA/QVM structured query semantics; and
- explicit complete/incomplete coverage.

The modern async typed driver presently serializes `Collection<T>` through
JSON. First-class codec-bound binary records are therefore a real but narrow
Residiuum delta.

## 6. Architecture boundary

```text
┌────────────────────────────────────────────────────────────┐
│ Cross-compiled Kiku application                            │
│ COBOL-shaped control flow, file statuses, decimal behavior │
└─────────────────────────────┬──────────────────────────────┘
                              │ indexed-file operations
┌─────────────────────────────▼──────────────────────────────┐
│ Kiku COBOL compatibility runtime                           │
│ OPEN/READ/START/NEXT/PREVIOUS/WRITE/REWRITE/DELETE         │
└──────────────┬─────────────────────────────┬───────────────┘
               │ record codec               │ async data API
┌──────────────▼──────────────┐  ┌───────────▼───────────────┐
│ Hopper compiled descriptor │  │ Residiuum codec-bound SDK │
│ PIC/layout/decimal/keys     │  │ versions/cursors/Atomics  │
└──────────────┬──────────────┘  └───────────┬───────────────┘
               │ projections                 │ opaque bodies
┌──────────────▼──────────────────────────────▼───────────────┐
│ Residiuum Heap                                             │
│ authoritative records + schema bindings + derived indexes  │
└─────────────────────────────┬──────────────────────────────┘
                              │ bounded projection
┌─────────────────────────────▼──────────────────────────────┐
│ SDA/QVM/RQL                                                │
│ optional queries over the same compiled record meaning     │
└────────────────────────────────────────────────────────────┘
```

### 6.1 Ownership

| Component | Owns | Must not own |
|---|---|---|
| COBOL-to-Kiku compiler | source parsing, dialect lowering, coverage report, generated runtime calls | record persistence, index files, recovery |
| Hopper | compiled layouts, physical codecs, exact decimal, bounded field projection/encoding | database authority, queries, transactions |
| Kiku compatibility runtime | COBOL indexed-file observable behavior and file-status mapping | a private store, WAL or transaction system |
| Residiuum codec SDK | schema-bound exact-byte records, versions, admission, cursor and Atomic calls | COBOL language interpretation |
| Residiuum store | authoritative bytes, versions, ordered derived access, recovery and damage evidence | PIC or business semantics |
| SDA/QVM/RQL | typed predicates, ordering, projections and aggregates | re-encoding application records or COBOL control flow |

## 7. Schema authority

The cross-compiler produces an immutable `HopperSchemaPack` for every admitted
record layout. Its eventual binary encoding is not frozen in v0.1, but its
semantic content must include:

```text
schema_profile
schema_id
schema_revision_hash
source_dialect_profile
source_copybook/source-location evidence
Kiku compiler identity
Hopper descriptor/interpreter identity
record framing: fixed or admitted variable profile
minimum/maximum/exact byte width
field symbols and canonical paths
field offsets/widths
physical codecs and codec parameters
logical type, precision, scale and signedness
character repertoire/code page
overlay sets and selection rules
occurrence dimensions and bounds
primary key projection
alternate key projections
duplicate-key policies
collation identities
record validation programme
encode/decode resource bounds
```

`schema_revision_hash` is domain-separated over a future deterministic pack
encoding plus every identity that can affect interpretation. Recompiling
semantically equivalent source may reuse a revision only when it produces the
same canonical pack. Compiler version alone never changes meaning silently.

Schema packs are immutable. Changing record meaning creates a new revision and
requires an explicit compatibility/transition plan. A collection binding names
one admitted schema revision; it never follows “latest.”

## 8. Authoritative record profile

The logical body is conceptually:

```text
profile_version
schema_id
schema_revision_hash
record_bytes
```

The exact envelope and type tag remain open for refinement. Requirements:

- `record_bytes` are retained exactly as admitted;
- schema identity is authenticated by the containing durable record encoding;
- lengths are checked before allocation;
- unknown profiles and schema revisions remain preservable but not interpretable;
- corrupt or incomplete bodies never decode as valid records;
- a raw-byte read remains possible for authorized diagnostics/export;
- ordinary writes require the collection's exact schema binding;
- key/body mismatch is rejected before mutation; and
- descriptor metadata is not copied into every user-visible object.

Whether the profile uses a new body tag, a general codec envelope, or
collection-level schema binding plus raw bytes is deliberately unresolved. It
must be decided as part of the general Residiuum codec architecture rather than
as an ISAM-only special case.

## 9. Record identity and keys

### 9.1 Primary identity

The imported record's Residiuum key is derived from the compiled primary-key
projection and its declared collation/encoding profile. It must be injective
over admitted primary-key values and stable across processes and rebuilds.

The storage-key encoding must distinguish values that the source runtime
distinguishes and equate only values the source runtime equates. It cannot
assume UTF-8 lexical order or JSON scalar order.

### 9.2 Alternate keys

Alternate keys are derived ordered access paths. Each definition includes:

- field projection or concatenated key shape;
- physical-to-logical normalization rules;
- collation identity;
- uniqueness or duplicates policy;
- treatment of spaces, low/high values and invalid representations;
- tie-breaking order for duplicate keys; and
- source-record version/frontier coverage.

Duplicate alternate-key order must be deterministic. If the source runtime
specifies only an implementation-dependent order, the compatibility profile
must either reproduce it or state and prove that the program cannot observe
the difference.

### 9.3 Index authority

Primary and alternate indexes are derived from authoritative records using the
bound Hopper descriptor. Their loss cannot lose records. A complete empty or
end-of-range result requires verified complete coverage; otherwise the runtime
returns a mapped failure rather than fabricating `NOT FOUND` or end-of-file.

## 10. Indexed-file compatibility surface

The generated Kiku application targets a bounded async runtime. The source
compiler may lower synchronous-looking COBOL control flow to state-machine
awaits, but Residiuum does not add synchronous database writes.

Minimum eventual operation set:

```text
OPEN INPUT | OUTPUT | I-O | EXTEND
CLOSE
READ primary-key
READ alternate-key
START relation key
READ NEXT
READ PREVIOUS
WRITE
REWRITE
DELETE
COMMIT / ROLLBACK where the admitted source profile defines them
file status and declarative error handling
```

Provisional mapping:

| Source operation | Residiuum mechanism |
|---|---|
| keyed `READ` | version-bearing point read or exact derived-index lookup plus authoritative revalidation |
| `START` | authenticated ordered seek cursor bound to schema/index/collation/snapshot |
| `READ NEXT/PREVIOUS` | bounded ordered continuation |
| `WRITE` | create-if-absent with stable operation identity |
| `REWRITE` | version-CAS replacement, optionally inside LocalHeap Atomic |
| `DELETE` | version-CAS deletion, optionally inside LocalHeap Atomic |
| compound business mutation | qualified LocalHeap Atomic |
| file status | stable compatibility mapping from typed SDK/storage outcomes |

Every successful read returns its establishing version to the compatibility
runtime even when the source language does not expose it. A later `REWRITE` or
`DELETE` uses that version according to the admitted locking/concurrency
profile. The runtime must not implement a read-then-unconditional-write race.

## 11. Cursor and sequential semantics

A compatibility cursor binds at least:

```text
Heap and collection identity
schema revision
key/index definition revision
collation identity
direction
START relation and key
duplicate-key position/tie breaker
consistency/snapshot profile
coverage policy and effective bounds
expiry and authentication identity
```

Cursors are bounded, authenticated and resumable according to the admitted
profile. They are not server-held transactions and do not hold database locks
while application code runs.

The refinement phase must close:

- exact `START =`, `>`, `>=`, `<`, `<=` behavior by dialect;
- what happens when the current record is deleted or rewritten;
- whether insertions become visible during sequential traversal;
- alternate-key duplicates and reverse traversal;
- end-of-file and invalid-key status mapping;
- cursor lifetime and restart behavior; and
- snapshot versus available-read profiles.

Until those are closed and differentially proved, no “unchanged indexed-file
behavior” claim is allowed.

## 12. Concurrency and transaction semantics

The programme consumes Residiuum Atomics; it does not bypass them.

The compatibility scanner classifies source behavior such as:

- no observable concurrent mutation;
- record lock retained between `READ` and `REWRITE`;
- automatic lock release rules;
- explicit commit units;
- multi-file commit units;
- declarative error/rollback behavior; and
- vendor extensions.

Profiles may implement pessimistic source behavior using optimistic versions
only when observable equivalence is proved. Otherwise the Kiku runtime needs a
bounded lease/coordination profile explicitly backed by Residiuum facilities.
It may never retain an in-process mutex and call that a distributed record
lock.

Cross-Heap or unbounded transactions are refused. File groups requiring atomic
commit must be placed within one admitted LocalHeap scope and remain within the
Atomics member/byte/collection ceilings, or the application is not eligible for
the first profile.

## 13. Decimal and data semantics

Base-10 preservation is a core compatibility boundary, not a serialization
detail.

For every admitted numeric operation the compiler/runtime must preserve:

- source operand pictures;
- intermediate precision and scale rules;
- signed representation and negative zero behavior if observable;
- truncation and rounding policy;
- overflow and `ON SIZE ERROR` behavior;
- division quotient/remainder semantics;
- move/alignment/padding behavior;
- edited versus computational representations; and
- exact encode failure without partial record mutation.

Hopper decimal values projected into SDA require a first-class exact decimal
carrier. They must not become JSON numbers or `f64`. Until SDA/RQL admits that
carrier, such fields may be selected as exact typed values through the Hopper
adapter but cannot be advertised as generally queryable numeric RQL fields.

Character fields similarly bind an explicit repertoire and collation.
EBCDIC-to-Unicode rendering may be offered as a view; it is not authority and
must not redefine key equality or ordering.

## 14. RQL/SDA integration

RQL is additive. Generated Kiku applications do not require it.

For eligible fields, QVM host projection asks the bound Hopper descriptor for
only the required fields:

```text
record version + authoritative bytes
        |
        v
bounded Hopper projection
        |
        v
SDA value with provenance/schema/field identity
        |
        v
RQL predicate, projection, order or aggregate
```

The adapter must provide:

- presence versus invalid-representation distinction;
- exact scalar type and declared bounds;
- borrowed-field lifetime confinement;
- byte and CPU accounting;
- stable algebraic error mapping;
- exact decimal and character collation identities;
- covering-index eligibility rules; and
- coverage evidence tied to record versions and schema revision.

There is no whole-record JSON tree unless a caller explicitly requests a JSON
rendering. JSON export is a lossy/convenience view unless its profile proves a
round trip for the particular schema.

## 15. Import pipeline

An import is a restartable, bounded job:

1. bind exact source file, schema pack and import profile identities;
2. inventory framing and record counts/bytes without interpreting past bounds;
3. extract records sequentially from the consistent export;
4. validate width, representation and schema constraints with Hopper;
5. derive and validate primary keys;
6. write exact authoritative record bytes with deterministic member operation
   identities;
7. record rejected/damaged source regions without filling or guessing;
8. build primary/alternate access paths from accepted authority;
9. compare counts, roots and key coverage;
10. publish the imported generation/catalog binding atomically; and
11. emit a complete import evidence report.

The first profile imports from a quiesced or otherwise externally consistent
export. Live source capture, dual writes, change-data capture and online cutover
are later profiles.

Source index pages need not be imported. They may be used as independent
evidence, but Residiuum rebuilds its own access paths from authoritative record
bodies and declarations.

Malformed records are never silently repaired. Policy choices are explicit:

```text
reject entire import
admit sound records with incomplete coverage evidence
preserve undecodable record bytes in a quarantine collection
```

Only the first is eligible for an exact application cutover claim unless the
original runtime demonstrably treated the same bytes as unreachable.

## 16. Cross-compiler compatibility report

Before conversion, the toolchain emits a machine-readable and human-readable
estate report classifying every relevant construct:

```text
mechanically supported and proved
supported by a named runtime-emulation profile
supported only under a documented non-observability assumption
requires external integration
unsupported
```

It covers at least:

- COBOL dialect/compiler/runtime and options;
- file organizations and access modes;
- copybooks and conditional compilation;
- PIC forms and physical representations;
- overlays, `REDEFINES`, `OCCURS` and depending-on forms;
- key and collation declarations;
- file statuses and declaratives;
- locking and transaction usage;
- arithmetic statements and options;
- CICS/JCL/vendor calls and external effects;
- dynamic calls and generated code;
- encoding/code pages; and
- any semantic construct whose lowering is not mechanically certified.

No import/cutover tool may hide an unsupported result behind generated stub
behavior.

## 17. Assurance and differential proof

### 17.1 Required claim families

Provisional claims:

| Claim | Obligation |
|---|---|
| `KCR-RAW-1` | admitted record bytes round-trip exactly |
| `KCR-SCHEMA-1` | schema identity cannot silently change interpretation |
| `KCR-BOUND-1` | projection/encoding stays within record and declared resources |
| `KCR-DEC-1` | admitted decimal operations match source results/statuses |
| `KCR-KEY-1` | primary/alternate equality and ordering match source semantics |
| `KCR-FILE-1` | admitted file operations and statuses are trace-equivalent |
| `KCR-CONC-1` | admitted concurrency/commit behavior is observationally equivalent |
| `KCR-REC-1` | crash/retry exposes valid committed states without invented bytes |
| `KCR-DER-1` | loss of derived state cannot destroy or redefine authority |
| `KCR-ISO-1` | no file/program crosses Heap, schema or capability authority |
| `KCR-QRY-1` | RQL results reflect the same descriptor semantics and honest coverage |

### 17.2 Differential harness

For each admitted application/profile:

1. run the original COBOL binary/runtime on a controlled copy of source data;
2. capture input events, external dependencies and file-operation traces;
3. cross-compile without manual business-logic edits;
4. import the same authoritative record snapshot;
5. run generated Kiku against the same controlled inputs;
6. compare operation-by-operation and final-state evidence.

Comparison includes:

- returned record bytes;
- file status and declarative branch;
- primary and alternate-key traversal order;
- arithmetic values and size/rounding outcomes;
- created/rewritten/deleted record bytes;
- commit/rollback visibility;
- output files/reports where in scope;
- final logical record sets and content roots; and
- crash/restart/lost-reply outcomes.

The harness must contain both recorded estate workloads and generated boundary
programs for every admitted language/runtime rule. Passing happy-path business
tests alone is insufficient.

### 17.3 Non-observability assumptions

If exact source behavior is unspecified or unstable, an application may still
qualify only when static and dynamic evidence proves it cannot observe the
difference. Every such assumption is named, reviewed and included in the
compatibility certificate. There is no global “implementation-defined means
anything is acceptable” escape.

## 18. Security and authority

- Every imported file/collection belongs to one Heap.
- Schema registration requires Heap administration authority.
- Generated programs receive attenuated capabilities for only their declared
  files and operations.
- RQL access requires independent read/query authority; the COBOL compatibility
  cap does not automatically grant analytics access.
- Raw diagnostic/export reads are separately authorizable because records may
  contain fields not exposed by application logic.
- Error, coverage and status mapping must not reveal other records, files,
  schemas or Heaps.
- Schema packs and generated binaries are supply-chain artifacts whose hashes
  enter the compatibility certificate.

## 19. Recovery and damage behavior

Residiuum remains the only persistence/recovery authority after cutover.

On damage or incomplete evidence:

- no missing record or field is filled with spaces, zeroes or neighboring bytes;
- no broken derived index proves `INVALID KEY`, `NOT FOUND` or end-of-file;
- a point record with incomplete payload maps to a distinct compatibility
  failure, not absence;
- derived indexes may be rebuilt explicitly from sound authoritative records;
- schema-unavailable records remain preserved but uninterpretable;
- import work can resume only from verified source/job checkpoints; and
- salvage reports retain physical and logical provenance.

Exact COBOL file-status mappings are profile-specific and remain open. Operator
diagnostics always retain the richer structured Residiuum/Kiku cause even when
the application sees a two-character status.

## 20. Performance hypothesis and qualification

The design should be competitive with indexed-file workloads because:

- point reads map to primary-key lookup;
- fixed-layout field projection avoids parsing a complete JSON tree;
- primary/alternate keys can be projected from bounded byte ranges;
- sequential reads use ordered bounded pages;
- exact decimal decode touches only required fields;
- generated Kiku can reuse arenas and caller-owned scratch; and
- Residiuum can pipeline independent writes and share stable-media boundaries.

These are hypotheses, not claims. Qualification must compare original runtime
and Kiku/Residiuum on the same hardware class and dataset for:

- keyed reads;
- `START` plus forward/reverse traversal;
- unique and duplicate alternate keys;
- write/rewrite/delete;
- batch import and batch processing;
- cold/warm cache;
- crash/restart;
- storage amplification;
- CPU, memory, read/write bytes and stable boundaries; and
- high-contention record updates where admitted.

Benchmarks report records/second, operations/second, logical and physical
bytes/second, latency distributions and durability profile. They do not compare
durable Residiuum against unsafe source settings without disclosing the
difference.

## 21. Initial delivery stages

These stages are planning placeholders and require refinement before admission.

### KCR-0 — corpus and compatibility taxonomy

- select public CBL/copybook/DATA examples and at least one executable source
  runtime;
- inventory dialects, file organizations, PIC forms and operations;
- define compatibility-report schema;
- build trace vocabulary and original-runtime capture harness; and
- choose one deliberately narrow first profile.

Exit: one representative application and adversarial micro-corpus execute under
the source runtime with reproducible traces.

### KCR-1 — Hopper schema/record conformance

- deterministic portable schema pack;
- fixed-record DISPLAY and COMP-3 codecs;
- exact primary/alternate key projection;
- hostile record and schema corpus;
- exact byte round trips and wide-decimal integration; and
- independent descriptor/decimal oracle comparisons.

Exit: record and key claims pass without Residiuum.

### KCR-2 — Residiuum codec-bound records

- general async codec-bound collection surface;
- first-class exact bytes and schema binding;
- version-bearing point reads and bounded pages;
- projection/index host contract;
- schema registration and authority; and
- damage/rebuild qualification.

Exit: no JSON conversion occurs, and exact records survive write/read/reopen,
crash, lost reply, damage tests and derived-index deletion.

### KCR-3 — read-only indexed-file runtime

- `OPEN INPUT`, keyed `READ`, `START`, `NEXT`, `PREVIOUS`, `CLOSE`;
- primary and alternate ordered access;
- duplicate-key behavior;
- file statuses and declaratives; and
- bounded authenticated cursor profile.

Exit: read-only source and Kiku traces are equivalent for the first profile.

### KCR-4 — mutation and commit behavior

- `OPEN I-O/OUTPUT/EXTEND` as admitted;
- create, rewrite and delete;
- establishing-version discipline;
- LocalHeap Atomics and commit units;
- concurrency/lock compatibility profile; and
- crash, retry and uncertain-outcome qualification.

Exit: mutation traces and final record states are equivalent.

### KCR-5 — first application lateral shift

- automatic cross-compilation without business-logic edits;
- consistent import and cutover tooling;
- full differential run;
- performance qualification;
- operator runbook and rollback/export plan; and
- signed compatibility/evidence certificate.

Exit: the named application and only its named profile may claim lateral
rehosting compatibility.

### KCR-6 — RQL/SDA additive access

- exact decimal SDA carrier where not already admitted;
- bounded Hopper projection host;
- field predicates/projections/order/aggregates;
- query/index equivalence and coverage; and
- coexistence with generated Kiku workload.

Exit: RQL is a verified additional view and cannot change Kiku application
semantics or authoritative bytes.

Later stages may add more dialects, variable records, discriminator-governed
overlays, EBCDIC profiles, online capture, external integration adapters and
application portfolios. None is implied by KCR-5.

## 22. First proof candidate

The preferred first public proof contains:

- one indexed fixed-record file;
- one primary key;
- one alternate key with duplicates;
- DISPLAY character/numeric and signed COMP-3 fields;
- point primary and alternate reads;
- `START`, `READ NEXT` and `READ PREVIOUS`;
- write, rewrite and delete;
- one multi-record commit if supported by the source example;
- exact decimal add/multiply/divide with a rounding/size boundary;
- one equal-width overlay if discriminator semantics are unambiguous;
- restart after forced termination; and
- a stable expected operation/final-record trace.

The proof deliberately excludes CICS, live source synchronization, variable
records, COMP-2, dynamic calls and vendor-native external effects unless the
chosen corpus makes one unavoidable and explicitly admitted.

## 23. Open decisions requiring refinement

Before a developer-ready handoff, architecture must close:

1. the first exact COBOL compiler/runtime/dialect profile;
2. the first ISAM/export framing format and consistency procedure;
3. canonical `HopperSchemaPack` bytes and hash domains;
4. general Residiuum codec-envelope versus collection-binding design;
5. schema catalog, revision and compatibility-transition protocol;
6. storage-key encoding for each admitted key/collation type;
7. duplicate alternate-key tie-breaking;
8. cursor snapshot and mutation visibility semantics;
9. record-lock and commit-unit equivalence;
10. exact file-status/error mapping;
11. SDA exact decimal carrier and ordering/aggregation laws;
12. character repertoire, EBCDIC and national-character profiles;
13. `REDEFINES`, `OCCURS DEPENDING ON` and variable-record policy;
14. rejected-record/import atomicity policy;
15. cutover consistency, verification and rollback/export procedure;
16. schema/binary signing and supply-chain trust;
17. limits for records, schemas, fields, keys, cursors and import jobs;
18. remote protocol and capability negotiation, if any;
19. public corpus licensing and reproducibility; and
20. the exact claim language legal/product teams may use.

Real CBL + copybook + DATA examples must drive these decisions. Architecture
should resist freezing elegant abstractions that fail to reproduce an actual
runtime's edge behavior.

## 24. Explicit non-goals for the first profile

- unchanged original COBOL binaries;
- preserving proprietary ISAM page/index file layouts;
- SQL compatibility;
- relational schema generation;
- automatic business-domain redesign;
- all COBOL dialects or all indexed-file products;
- CICS/JCL/mainframe operating-environment emulation;
- cross-Heap transactions;
- live bidirectional synchronization;
- silent repair of malformed source data;
- replacing Kiku/Hopper semantics inside Residiuum;
- synchronous Residiuum mutation APIs; and
- claiming equivalence from compilation success alone.

## 25. Governance

This specification deliberately records a strong architecture with unresolved
profiles. It is ready for corpus selection and design experiments, not product
delivery.

Advancement to a normative developer contract requires:

1. one pinned public or authorized private estate corpus;
2. source-runtime executable traces;
3. a Kiku compatibility coverage report over that corpus;
4. Hopper descriptor/record prototype evidence;
5. a Residiuum codec-bound API proposal measured against current driver/store
   boundaries;
6. closed decisions from §23 for the first profile;
7. an implementation and qualification plan with dependency gates; and
8. explicit principal admission under `CRITICAL_PATH.md`.

Until then, permitted statements are:

> Residiuum and Kiku have a provisional architecture for behavior-preserving
> rehosting of eligible COBOL indexed-file applications without relational or
> JSON conversion.

Forbidden statements include:

- “Residiuum runs COBOL applications unchanged”;
- “all ISAM data can be imported without conversion risk”;
- “Kiku is COBOL-compatible” without a named accepted profile;
- “ISAM-speed” without disclosed comparative evidence; and
- “zero migration” without an application-specific compatibility certificate.

