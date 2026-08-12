# Residiuum technical sales master copy

Status: **visionary landing-page source copy**

Audience: web designers, product writers, technical buyers, investors,
architects and engineering leaders

Voice: confident, cinematic, technically literate, category-defining

This document deliberately describes the complete Residiuum universe in its
north-star product voice. The designers should use it as source material, not
place every paragraph on one page. Performance figures retain their workload
context in the claims appendix. Features still passing a delivery gate should
be labelled appropriately when the public site is published.

---

## The shortest possible pitch

> **Residiuum is the database that refuses to die.**

Residiuum is a high-performance, damage-tolerant database for arbitrary,
massive and long-lived data. It combines an embedded document database, an
append-optimized event store, a deterministic query and transformation engine,
bounded serializable Atomics, independently recoverable storage, adaptive
indexing and mathematically specified distributed durability in one coherent
system.

Put anything in. Keep it at scale. Query it now. Reinterpret it decades later.
Damage the system—and recover every intact piece that still exists.

---

## Hero section

### Headline option A

> **The database that refuses to die.**

### Headline option B

> **Damage the database. Keep the data you did not destroy.**

### Headline option C

> **Your data should outlive the software that created it.**

### Hero body

Most databases are built to protect one healthy present. Residiuum is built to
preserve data across time, scale, software change and physical damage.

Every authoritative storage frame is independently identifiable,
self-describing and verifiable. Indexes accelerate truth; they do not own it.
Catalogues organize truth; they do not imprison it. If part of the system is
destroyed, Residiuum does not turn surviving data into collateral damage. It
finds the intact islands, identifies the holes and lets every surviving byte
speak for itself.

All of this sits beneath an ordinary application experience: connect once,
bind the Heaps you are authorized to use, open a collection, write JSON or
bytes, run RQL, stream results and commit compound changes asynchronously.

### Hero proof strip

```text
459.18 MiB/s       58,775 durable writes/s       524,288/524,288 recovered
```

Controlled Apple-silicon campaign, 4 GiB logical payload, 8 KiB records,
durable acknowledgements, 32 MiB bounded group-commit knee; clean reopen in
0.469 seconds and complete post-restart byte validation.

### Primary call to action

> **Build for the next failure—and the next decade.**

### Secondary call to action

> Explore the architecture

---

## The category

### A damage-tolerant universal data fabric

Residiuum is not merely a document store, key-value engine, event log, archive
or recovery utility. It is a database designed around a more durable idea:

> The database may be damaged. Surviving data must remain meaningful.

Traditional systems often depend on a healthy chain of global metadata: a
catalogue locates a table, a tree locates a page, a page locates a record. Break
the wrong link and perfectly healthy bytes can become unreachable.

Residiuum reverses the dependency. Authoritative frames retain their own
identity, integrity and interpretation envelope. Global structures are
replaceable accelerators. The system can rebuild its maps from the territory.

```text
Conventional failure

critical metadata damage  ->  database unavailable

Residiuum failure

DATA | DATA | HOLE | DATA | HOLE | DATA
  ✓      ✓      ✗      ✓      ✗      ✓
```

What is gone is gone. What remains still lives.

---

## The three engines

### Hydra finds. Chimera stores. Medusa survives.

Residiuum separates three jobs that conventional storage engines frequently
entangle. Each layer can evolve independently because none is allowed to
silently redefine authoritative truth.

### Hydra — adaptive intelligence for finding immutable data

Most databases choose an index structure globally and force every dataset to
fit it. Hydra compiles the index to the shape of each immutable segment.

Dense ordered keys, sparse ranges, prefix-heavy identifiers and point-lookup
workloads do not have the same geometry. Hydra classifies the segment and can
select from structures such as:

- cache-efficient Eytzinger search layouts;
- piecewise geometric models;
- radix splines;
- compressed radix structures; and
- minimal perfect hashing for static point lookup.

Hydra indexes are derived, rebuildable and disposable. A missing Hydra index
may cost time; it does not erase data. A corrupt index cannot overrule a
verified authoritative frame. Query planning can use Hydra when its coverage
proves the requested answer and fall back honestly when it cannot.

> **Hydra does not make one index faster. It chooses the right index for the
> data that actually exists.**

### Chimera — workload-compiled physical representation

Hydra decides how a value is found. Chimera decides how that value should be
represented locally.

Not every value deserves the same physical layout. Tiny values can live near
their locator. Point-oriented data can use compact micro-pages. Range-heavy
material can be clustered. Large payloads can remain in independently
verifiable extents. Chimera compiles these choices from the workload and value
shape without making the derived layout the sole source of truth.

Compact Chimera transformed the enrichment economics: measured derived
Chimera output fell from approximately 98% of authoritative bytes to roughly
0.74% in the performance campaign, while complete-lifecycle throughput rose
about threefold relative to the former materialized-Chimera architecture.

> **Chimera moves representation intelligence out of the application and into
> a rebuildable storage compiler.**

### Medusa — distributed durability as verifiable evidence

Medusa is Residiuum's distributed durability fabric. It separates the data
plane, evidence plane, ordering plane and repair plane:

```text
data plane       disperse and verify authoritative frames or coded regions
evidence plane   prove that a recoverable set is durably present
ordering plane   order a compact commitment through partition consensus
repair plane     continuously preserve the declared failure envelope
```

Consensus decides the logical order. Medusa proves that the bytes required to
survive that decision are durably available.

Every protection profile declares its coding, fragment threshold, placement
rules, failure domains, verification policy, witnesses and repair policy. A
commit receipt can bind the payload commitment, availability certificate,
partition position, leadership term and placement epoch into portable
evidence.

Medusa reasons about real correlated failure domains—not merely node counts:
device, host, rack, zone, region, provider, credential cohort and software
cohort can all form part of the declared survival envelope.

For a coding matrix `G`, reconstruction threshold `k`, certified fragment set
`A` and every admitted failure set `S`, a Medusa protection profile is valid
only when:

```text
for every S in FailureEnvelope(P): rank(G[A \ S]) >= k
```

And commitment requires both order and certified availability:

```text
Committed(x, epoch)
    => Ordered(Hash(x), epoch)
       AND CertifiedAvailable(Hash(x), profile, epoch)
```

> **Consensus decides. Medusa proves the bytes can survive the decision.**

---

## Survival is a first-class data model

### Immutable, self-verifying frames

Residiuum stores events in versioned frames with bounded lengths, deterministic
CBOR envelopes, content hashes, event identity and frame integrity. Segments
are immutable after publication. Large payloads are chunked into independently
verifiable material with manifests that describe the complete logical value.

The scanner does not require the entire database to be healthy before it can
recognize one healthy frame. It can resynchronize after corruption, reject
false positives and report exact holes rather than turning uncertainty into an
empty result.

### Recovery Shadow and the P-star frontier

Recovery Shadow gives sealed data a second recovery-oriented representation.
It is not a query cache pretending to be a backup. It advances an explicit
protection frontier—P-star—only when the corresponding recovery material is
complete, verified and published.

The acknowledgement frontier and the P-star frontier are deliberately
distinct. Residiuum can optimize the foreground write path without lying about
which material has acquired the stronger recovery representation. If
protection work lags, the lag is observable. If a crash interrupts it,
recovery resumes the declared intent rather than guessing.

### Honest partial recovery

Residiuum does not manufacture completeness. A damaged read can be:

```text
complete       verified and readable
reconstructed  complete after verified reconstruction
partial        healthy regions plus explicit holes
unavailable    identity survives; required material does not
conflicting    incompatible verified evidence survives
unknown        evidence cannot prove the outcome
```

Missing data never becomes `null`. Corruption never becomes “no rows.” An
offline tier never becomes an empty collection. This is not just an operations
feature; it is part of query truth.

---

## Query as algebra, not string interpretation

### SDA — Structured Data Algebra

SDA is Residiuum's small deterministic algebra for filtering, projection,
normalization, validation, transformation and reduction.

It is pure: storage access, decryption, tier staging and resource control occur
outside the evaluator. The same semantics can operate over ordinary documents,
recovered frames, partial payloads and recovery evidence.

SDA preserves distinctions other systems frequently blur:

```text
Absent != Null != Value(x) != Failure(reason)
```

This matters. “The field is not present,” “the application stored null,” “the
value exists,” and “the bytes could not be decoded” are different facts. A
database that collapses them can produce convenient answers that are simply
wrong.

> **If Residiuum can recover it, SDA can examine it.**

### RQL — Residiuum Query Language

RQL is the human query surface over the same canonical semantics. It provides
document-native filtering and projection, deterministic ordering, authenticated
continuation, enrichment, grouping, aggregation, conditional result shaping,
budgets, coverage requirements and explainable plan identity.

RQL source, typed builders and supported compatibility dialects compile toward
one canonical bytecode runtime. There is no “fast evaluator” with slightly
different truth from the reference path.

For a predicate `P`, document `d` and compiled SDA program `C(P)`, the semantic
obligation is:

```text
for every P and d:
    EvaluateSDA(C(P), d) = EvaluatePredicate(P, d)
```

Indexes may accelerate the computation only when their recorded coverage can
prove the answer. Explain output describes the plan that actually executed,
not the plan the optimizer hoped to execute.

### RRE — invariants that belong to the data

RRE, the Residiuum Rule Expression language, is the stored-invariant companion
to RQL. It describes document rules, transitions, relationships, uniqueness
and bounded cardinality in canonical, versioned form.

Rules belong to the Heap and travel with its evidence. Applications do not
have to recreate fifteen-year-old validation logic merely to understand why a
historical value was admitted.

---

## Atomics — compound truth without hand-waving

Applications rarely change one record in isolation. A conversation update may
need to replace authoritative state, create a turn and create a turn-ID locator.
A financial action may update an account and append a ledger entry. A durable
message may need state plus an outbox record.

Residiuum Atomics make these bounded state transitions one serializable
decision inside exactly one Heap.

An Atomic has:

- a stable 256-bit identity;
- a canonical closed plan;
- declared reads, predicates and mutations;
- one serialization point;
- authoritative prepare, member and decision evidence;
- exact replay for the same identity and content;
- explicit committed, not-committed and unknown observation states; and
- independently examinable crash recovery.

```text
prepare accepted plan
        |
validate at Heap serialization frontier
        |
persist every invisible member
        |
durable member boundary
        |
persist one decision
        |
durable decision = linearization
        |
publish one complete read-view delta
```

Prepared members are never ordinary data. Readers see the state before the
Atomic or the state after it—never a proper subset. The same Atomic identity
and content resolves the original outcome after a timeout, disconnect, crash,
restart or compaction. Reusing the identity for different content is refused.

Atomics are deliberately bounded and asynchronous. They do not hold an
interactive transaction open while application code waits on a network. They
do not disguise a distributed saga as ACID. They make one strong promise
inside one declared coordination scope and preserve the evidence required to
verify it.

> **One plan. One decision. All visible—or none visible.**

---

## Heaps — security and isolation by construction

A Residiuum deployment can host many logical Heaps. One physical client
connection owns the writer, scheduler, queues, observability and shutdown
domain; separately authorized Heap bindings share that connection safely.

Connection is not authority.

Every Heap has immutable identity. Collections, indexes, cursors, capabilities,
Atomic plans, Medusa fragments, repair evidence and derived artifacts bind to
that identity. A capability for Heap A cannot be widened into Heap B. The same
human collection name or record key in two Heaps remains two unrelated
objects.

This creates an operationally simple multi-tenant shape without making a
socket, process or filesystem path the security boundary.

---

## Performance without benchmark theatre

Residiuum is built around sequential append, bounded group commit, parallel
frame cooking, immutable segments, memory-resident live indexes, asynchronous
seal/enrichment and explicit durability boundaries.

The database does not pretend that buffering is cheating. Modern databases
amortize system calls and durability barriers. Residiuum does the same while
preserving an individual logical receipt and exact retry identity for every
operation.

### Controlled durable-ingest result

On the measured Apple-silicon host, a 4 GiB controlled campaign using 8 KiB
records and durable asynchronous acknowledgements achieved:

| Measurement | Result |
|---|---:|
| Durable writes | **58,775 operations/s** |
| Logical payload throughput | **459.18 MiB/s** |
| Records | **524,288** |
| Physical durability cohorts | **130** |
| Authoritative writes/barriers | **130 / 130** |
| Maximum cohort | **4,058 records / 33.54 MB encoded** |
| Operation failures | **0** |
| Admission waits/refusals | **0 / 0** |
| Clean reopen | **0.469 s** |
| Post-restart validation | **524,288 records and 4 GiB, complete** |

The measured knee was 32 MiB. Doubling the cohort to 64 MiB reduced barrier
count but damaged sustained throughput and tail latency, so Residiuum rejected
the apparently “bigger” optimization. That is how the engineering programme
works: optimize the system, not the graph.

Earlier complete-lifecycle Compact Chimera campaigns demonstrated roughly
37,900 8 KiB writes/s—about 296 MiB/s—and around three times the throughput of
the former materialized-Chimera architecture. The subsequent durable smart
client and group-commit work moved the controlled result substantially higher.

Hot indexed reads in a separate 10 GiB diagnostic campaign were measured in
the microsecond class, including approximately 18 µs p50 and 284 µs p99 on the
measured host. The damage campaign then punched sealed media, reported holes
and retained healthy sampled reads rather than invalidating the store.

These are measured campaign results, not universal hardware promises. The
public performance page should always disclose payload size, acknowledgement
mode, concurrency, cache state, lifecycle work, dataset size, host and exact
release.

---

## Formal assurance that names its assumptions

Residiuum does not use mathematics as decoration. Its Formal Assurance Spine
connects product claims to precise statements, explicit assumptions,
machine-checkable artifacts, production-code refinement and adversarial
physical evidence.

The toolchain assigns different jobs to different instruments:

- **Lean 4** for abstract state, invariant preservation and compositional
  theorem work;
- **Verus** for Rust-connected pure safety and refinement obligations;
- **TLA+ / TLC / TLAPS** for temporal protocols, crash ordering, concurrency,
  authority epochs, Atomics and cluster agreement;
- **Kani** for bounded concrete Rust state spaces, parser bounds and control
  reachability;
- independent executable models for differential testing and counterexample
  replay; and
- real filesystems, real process death, corruption, ENOSPC, fuzzing, mutation
  and soak campaigns for the physical world no abstract theorem can replace.

Every theorem receives an evidence status:

```text
proposed
specified
model_checked_bounded
machine_proved
implementation_connected
physically_qualified
revoked
```

The release manifest identifies the theorem, assumptions, source revision,
toolchain, connection to production Rust, negative controls and every excluded
obligation.

The principle is simple:

> **Do not trust the claim. Read the theorem, inspect its assumptions, run the
> prover and torture the connected implementation.**

Representative invariant families include:

```text
Heap noninterference
    authority(A) cannot observe or mutate Heap B

Atomic publication
    VisibleMembers(a) is either empty or CompleteMembers(a)

Atomic decision uniqueness
    Decision(a) cannot be both committed and not_committed

Query compilation
    compiled and reference semantics are observationally equal

Medusa availability
    every admitted failure set leaves reconstruction rank >= k

Coverage honesty
    incomplete evidence cannot prove absence or completeness
```

Residiuum is not marketed with the empty phrase “formally verified database.”
It publishes named, inspectable proof obligations and their achieved level.

---

## One database, three deployment shapes

### Embedded

Link Residiuum directly into the application. One asynchronous client owns the
physical deployment resources; cloneable collection and Heap handles provide
bounded concurrency without forcing the application to invent its own lock,
session or writer lifecycle.

Ideal for agent memory, desktop applications, edge systems, local-first
software, developer tools and appliances.

### Server

Use the same Heap, collection, RQL, version, receipt and Atomic semantics across
an authenticated network boundary. Requests are admitted by count and bytes.
Deadlines, cancellation stage, overload and unknown commit outcome are typed;
applications never parse error strings.

### Cluster

Partition-local consensus controls the right to order strong writes. Any-node
ingress, direct-route learning, bounded forwarding, anti-entropy, rebalancing
and explicit coverage keep the system operable without turning the control
plane into payload authority.

Medusa supplies the durability evidence and repair fabric beneath the
partition order. Lose a catalogue and surviving frames still identify
themselves. Lose quorum and strong writes pause rather than split-brain. Exceed
the declared failure envelope and the system reports degradation rather than
inventing success.

---

## Data through its entire life

Residiuum treats hot, warm, cold and archival placement as locations of one
logical identity—not separate products joined by fragile export pipelines.

- append high-volume operational data;
- query the hot working set;
- seal immutable segments;
- build Hydra and Chimera accelerators asynchronously;
- move segments across storage tiers;
- preserve history and decision evidence;
- scrub and repair verified regions;
- reconstruct indexes and catalogues;
- salvage after partial destruction; and
- examine old or malformed material with SDA even when its original
  application no longer exists.

The same item remains itself while its physical location and acceleration
structures change.

---

## Ordinary database ergonomics over extraordinary machinery

The survival architecture stays out of the way until it has something
important to say.

Applications work with familiar primitives:

```text
open deployment
    -> bind authorized Heap
    -> open typed collection
    -> create / get / replace / delete
    -> query / page / stream
    -> inspect history
    -> commit Atomic
```

Version-bearing reads supply exact optimistic-concurrency tokens after restart.
Stable operation identities make an ambiguous reply safely resolvable instead
of encouraging a blind retry. Bounded scans carry continuations rather than
materializing an unbounded collection. One connection can speak to many
authorized Heaps while retaining one scheduler, writer and shutdown domain.

The async driver owns the concurrency mechanics applications should not have
to reinvent:

- bounded admission by operation count and encoded bytes;
- cloneable, thread-safe collection handles;
- shared durable group commit;
- explicit overload rather than runaway memory;
- typed deadline and cancellation stages;
- exact commit-outcome uncertainty;
- orderly drain and shutdown; and
- redacted, bounded inspection of queue and write-path state.

Errors are stable machine-readable codes with declared retry dispositions.
Applications never need to parse a storage-engine sentence to decide whether a
request is safe to repeat.

> **The application describes intent. Residiuum owns scheduling, durability,
> recovery and retry truth.**

---

## Operations designed around evidence

Residiuum treats operability as part of correctness.

### Doctor, scrub and salvage

Doctor reports structural and coverage state. Scrub verifies authoritative and
protection material. Salvage emits every independently valid island plus holes,
conflicts and unsupported encodings. None of these tools needs permission to
turn damaged data into a clean fiction.

### Backup, restore and migration

Backups preserve authoritative media and required recovery evidence. Restore
distinguishes preservation of historical provenance from creation of a new
Heap identity and new authority. Versioned readers preserve unknown future
fields for lossless tooling, while execution refuses semantics it cannot
safely interpret.

### Tiering and retention

Immutable segments can move between filesystem or storage tiers without
changing logical identity. Retention and reclamation respect history,
protection frontiers, Atomic decisions and legal/operational evidence rather
than deleting bytes merely because one cache no longer references them.

### Encryption, compression and chunks

Transforms are declared in the frame envelope and applied under bounded,
ordered profiles. Large values use chunk manifests so damage is localized and
completeness remains provable. Unsupported transforms remain identifiable and
preservable rather than becoming anonymous corruption.

### Evidence Ledger

Administrative and security decisions can be recorded as immutable,
Heap-confined evidence: rule changes, authority transitions, repair actions,
retention cuts, checkpoints and future Atomic/Medusa decisions form an
independently inspectable operational history.

### Ratatouille telemetry

Residiuum's telemetry architecture favors a bounded operational firehose over
unbounded request-path logging. Metrics and traces expose queueing, media
boundaries, recovery, coverage, repair debt and lifecycle work without turning
Heap IDs, keys, credentials or query text into uncontrolled labels.

> **If the database made a consequential decision, the operator should be able
> to inspect the evidence—not reverse-engineer it from log poetry.**

---

## Built in Rust, designed as replaceable kernels

Residiuum is implemented as a set of explicit responsibility boundaries rather
than one inseparable server binary:

- survival format and scanners;
- single-node store and recovery;
- pure SDA evaluator;
- application SDK and async driver;
- authenticated wire client and server;
- evidence examination and salvage;
- cluster coordination; and
- formal and executable reference models.

The separation supports embedded use without a network stack, server use
without duplicating storage semantics and formal analysis without pretending a
second demonstration engine is the production path.

The public Rust ecosystem follows a progressive-disclosure model: ordinary
applications begin with typed collections and RQL; operators reach for
inspection, salvage, scrub, migration and protection evidence only when the
situation calls for them.

---

## Concrete application stories

### Agent memory that survives the agent

Store conversations, turns, tool traces, intermediate reasoning artifacts and
large model outputs without flattening them into one fragile document.

RQL retrieves and reshapes operational context. SDA filters the incoming
telemetry stream. Atomics replace the authoritative conversation state while
creating its turn and locator together. History remains inspectable. If the
application crashes after the durable decision but before receiving the reply,
the stable Atomic identity resolves the original outcome after restart.

### Telemetry at wire speed

Use SDA as a deterministic in-memory filter before persistence or feed selected
events into Residiuum's append path. Preserve raw events, structured envelopes
or opaque payloads. Build new interpretations later without having required
the future schema at ingest time.

Hydra adapts immutable indexes to key shape. Chimera compiles physical
representation to workload. Tier old segments without losing logical
identity. Medusa disperses and protects the regions that must survive machine
or site loss.

### Edge and local-first applications

Embed one database rather than combining SQLite, loose files, a search sidecar
and a custom recovery protocol. Work offline. Keep the same logical API when
the application later connects to a server or cluster. Treat missing media and
partial synchronization as explicit coverage states rather than empty results.

### Long-lived regulated evidence

Preserve immutable events, rule revisions, decision receipts, repair actions
and retention evidence. Keep payload authority separate from derived indexes.
Bind administrative evidence to Heap identity. Verify what survives without
requiring the original catalogue or application binary.

### Financial and operational invariants

Commit an account version and ledger entry in one bounded serializable Atomic.
Create state and outbox together. Enforce uniqueness and parent existence with
the same predicate algebra used by query. Resolve timeouts by identity rather
than issuing a dangerous blind retry.

### Scientific, industrial and AI archives

Store the source bytes before the perfect schema exists. Add structured
projections and indexes later. Preserve partial datasets with explicit holes.
Re-examine years of retained material using a new SDA program without
requiring the software that originally produced it.

---

## Competitive framing

### More than a document database

Residiuum offers flexible documents and deep queries, then adds arbitrary
bytes, immutable event history, explicit partial recovery, tiered survival and
catalog-independent examination.

### More than an object store

Residiuum adds an embedded hot path, versioned collections, indexed access,
queries, Atomics, history and recovery semantics across objects and holes.

### More than an event log

Residiuum adds current-state collections, direct lookup, adaptive indexes,
document-native queries, derived layouts and bounded compound transitions.

### More than a cache

Residiuum is designed for durable, independently examinable data and explicit
recovery—not merely reconstruction from some other system of record.

### More than backup

Backups restore a known intact copy. Residiuum can also locate independently
valid islands when no complete copy, catalogue or control plane remains.

### More than “ACID” as a checkbox

Residiuum gives an Atomic a durable identity, canonical content, one decision,
exact retry behavior and examination evidence. Unknown outcome is represented
as unknown; it is not guessed from a broken connection.

---

## Architecture montage copy

This section is suitable for a scrolling animated diagram.

```text
APPLICATIONS
    |
    |  async typed collections • JSON • bytes • events
    v
HEAPS
    |  immutable identity • capabilities • noninterference
    v
RQL + SDA + RRE
    |  query • algebra • invariants • canonical bytecode
    v
ATOMICS
    |  one bounded plan • serial validation • one durable decision
    v
AUTHORITATIVE FRAMES
    |  append • hash • verify • chunk • seal • salvage
    |
    +---- HYDRA -------- finds the data
    |
    +---- CHIMERA ------ compiles its local representation
    |
    +---- RECOVERY SHADOW protects the P-star frontier
    |
    +---- MEDUSA ------- disperses, certifies and repairs survival
    v
HOT • WARM • COLD • ARCHIVE • EDGE • SERVER • CLUSTER
```

Overlay phrases:

```text
Truth is immutable.
Acceleration is replaceable.
Coverage is explicit.
Damage is local.
Decisions are examinable.
Recovery never invents data.
```

---

## Landing-page section headlines

Designers can use these verbatim or as direction:

1. **A database should not become silent just because it is wounded.**
2. **The map can burn. The territory still knows what it is.**
3. **Hydra finds. Chimera stores. Medusa survives.**
4. **459 MiB/s of durable intent. Zero ambiguity about the boundary.**
5. **Absent is not null. Damaged is not empty. Unknown is not success.**
6. **One Heap. One plan. One serializable decision.**
7. **Indexes accelerate truth. They do not own it.**
8. **Query recovered data with the same algebra that queries healthy data.**
9. **Consensus orders. Medusa protects. Evidence proves.**
10. **Mathematics with build artifacts, not mathematics as decoration.**
11. **Embedded simplicity. Server reach. Cluster survival.**
12. **Your next schema can arrive years after your data.**
13. **Extraordinary internals. Ordinary database experience.**

---

## Short feature cards

### Arbitrary data

JSON, bytes, events, documents, chunks, model output and formats that have not
been invented yet.

### Durable async ingestion

Bounded group commit amortizes real durability barriers while every logical
operation keeps its own identity and receipt.

### Independent survival

Self-verifying immutable frames keep healthy data meaningful after local
damage.

### Exact recovery truth

Complete, partial, missing, conflicting and unknown are different outcomes.

### Hydra adaptive indexing

Compile a segment's lookup structure to its actual key geometry.

### Chimera physical compilation

Choose value placement by size and workload without making a derived layout
authoritative.

### Medusa durability fabric

Separate payload dispersal, availability evidence, logical order and repair.

### RQL

Document-native querying, deterministic ordering, aggregation, enrichment,
budgets and authenticated continuation.

### SDA

A pure deterministic algebra for healthy, historical and damaged data.

### Atomics

Bounded serializable multi-record change with exact replay and examinable
decisions.

### Heap isolation

Identity and capability confinement across every data, query, cursor, repair
and durability artifact.

### Formal Assurance Spine

Named theorems, disclosed assumptions, machine checks, Rust connections,
negative controls and physical torture tests.

---

## Suggested final page close

### Headline

> **Most databases protect your data while everything works. Residiuum is
> designed for what happens next.**

### Body

Applications disappear. Schemas change. Disks fail. Networks split. Catalogues
corrupt. Operators make mistakes. Data outlives every layer built around it.

Residiuum starts from that reality. It makes authoritative data independently
verifiable, acceleration rebuildable, coverage explicit, compound decisions
examinable and distributed survival a mathematical contract.

Build the hot application you need today—without making today's application
the only thing that can understand your data tomorrow.

### Final call to action

> **Put anything in. Keep what survives. Understand it forever.**

---

## Vocabulary and buzzword bank

Use consistently:

| Name | Public meaning |
|---|---|
| **Residiuum** | the damage-tolerant universal database |
| **Hydra** | adaptive per-segment indexing: how immutable data is found |
| **Chimera** | workload-compiled local representation and value placement |
| **Medusa** | distributed dispersal, availability evidence, protection and repair |
| **Recovery Shadow** | recovery-oriented sealed-data representation |
| **P-star** | explicit frontier covered by verified Recovery Shadow protection |
| **SDA** | Structured Data Algebra for deterministic examination/transformation |
| **RQL** | Residiuum Query Language |
| **RRE** | Residiuum Rule Expression language for stored invariants |
| **Atomic** | one bounded serializable state transition with durable evidence |
| **Heap** | immutable capability-confined logical data/security domain |
| **Survival frame** | independently identifiable and verifiable authoritative unit |
| **Formal Assurance Spine** | reproducible theorem/model/code/physical evidence system |
| **Medusa Protection Profile** | exact declared durability and failure envelope |
| **MACert** | Medusa Availability Certificate |
| **MCR** | Medusa Commit Receipt |
| **coverage** | what the system can prove it examined completely |
| **hole** | identified missing or unreadable material, never silently omitted |

Avoid:

- “indestructible” without “inside the declared failure envelope”;
- “formally verified database” as an unqualified system-wide claim;
- universal throughput or latency language based on one host;
- claiming an index is authoritative;
- treating `unknown` as failure or success;
- describing Medusa as a replacement for consensus; or
- describing a physical write cohort as one logical Atomic.

---

## Claims appendix for the eventual public site

The main creative copy above is intentionally rosy. Before publication, attach
each strong claim to one of these labels:

```text
Available        shipped in the named public release
Preview          implemented; qualification or API stability incomplete
Architecture     frozen design; delivery incomplete
Measured         exact campaign result with reproducible report
Target           intended result; not yet demonstrated
```

### Performance claim source

The 459.18 MiB/s / 58,775 durable operations/s result comes from the controlled
4 GiB Apple-silicon campaign documented in
`DURABLE_GROUP_COMMIT_BASELINE_2026_08_09.md`:

- 524,288 records;
- 8 KiB logical payload each;
- asynchronous smart-client path;
- durable acknowledgement;
- bounded 32 MiB physical cohorts;
- 130 authoritative writes and barriers;
- no operation failures, admission waits or scheduler refusals;
- clean reopen in 0.469 seconds; and
- all records and all 4 GiB revalidated after restart.

It is a measured campaign result, not an SLO or a promise for arbitrary
hardware, record sizes, contention, query load or Atomic member count.

### Architecture claim status at time of writing

The web team should verify the current release manifest immediately before
publication. In particular:

- core storage, recovery, async-client and measured ingest claims can cite
  implementation evidence;
- RQL has substantial implementation and qualification evidence but its full
  critical-path gate remains separately governed;
- LocalHeap Atomics have a developer-ready normative contract and delivery
  plan; capability advertisement waits for ATM-5 acceptance;
- Medusa is the complete distributed durability architecture and formal target;
  public availability language must follow its delivered profile; and
- the Formal Assurance Spine must describe the achieved status of each named
  theorem rather than imply one universal proof.

The ideal site can present the full universe as the destination, provided its
release badge, feature matrix or footnotes distinguish what users can install
today from what is in preview or architecture.
