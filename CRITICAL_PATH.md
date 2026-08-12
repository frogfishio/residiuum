# Residiuum critical path

Status: **principal-locked execution authority**

Effective: 2026-08-04

Owner: Residiuum product and engineering programme

## 1. Decision

Residiuum has proved that it is a serious, recoverable, high-throughput
**datastore**. It has not yet proved that it is a complete database.

The remaining existential risks are exactly three:

1. **RQL** — can Residiuum express, execute and sustain the queries ordinary
   database users need?
2. **Atomics** — can Residiuum make bounded compound state transitions with
   precise, provable semantics at an acceptable cost?
3. **Cluster** — can those semantics survive distribution, load and failure?

They are executed in this order:

```text
RQL  ->  Atomics  ->  Cluster  ->  broad product development
```

This file decides what engineering work is admitted. If another roadmap,
status page, archived plan or Kanban card conflicts with this order, this file
wins. Normative specifications still own technical semantics; the scoreboard
still records package state; Kanban still assigns labour. Neither the
scoreboard nor Kanban may silently change this priority.

Changing this sequence requires a dated principal amendment in this file.
Chat, an attractive prototype, an existing card or code already in the tree
does not amend it.

### 1.1 Principal execution amendment — 2026-08-10

RQL has reached a useful proof and implementation checkpoint at Q1--Q4, but
Gate 1 has **not** been accepted. Q5--Q7 and the remaining controlled
comparison/qualification work are parked with their state and residuals
preserved.

Atomics is now the active critical-path programme. This is a deliberate
risk-order amendment: compound state transition correctness is an independent
RED risk and is required by real application invariants now. It does not turn
the RQL checkpoint into a pass, waive any RQL exit criterion, admit Cluster,
or authorize broad product work.

Active execution is therefore:

```text
RQL checkpoint preserved
        |
        v
Atomics specification/governance -> ATM-0 -> ... -> ATM-5
        |
        v
resume/accept remaining RQL gate work before the combined database claim
        |
        v
Cluster
```

Atomics delivery authority is
[ATOMICS_SPEC.md](./doc/todo/atomics/ATOMICS_SPEC.md) plus
[ATOMICS_IMPLEMENTATION_PLAN.md](./doc/todo/atomics/ATOMICS_IMPLEMENTATION_PLAN.md).
No developer may substitute the older proposal or transaction compatibility
sketch for those two documents.

## 2. Proven baseline

The critical path begins from evidence already earned, not from zero.

- The core storage engine stores, seals, reopens, scans and recovers real data.
- Compact Chimera plus Recovery Shadow is the fresh-store recovery profile.
- Recovery Shadow preserves the named P-star live-value salvage guarantee
  without using query-shaped Materialized Chimera as the hot product path.
- The segment-ID reuse defect was reproduced independently and fixed by
  construction: durable allocation, collision refusal, immutable publication,
  media inventory and locator-identity checks.
- Residiuum 0.2.2 is the corrected release; 0.2.0 and 0.2.1 are unsafe/yanked.
- The controlled CompactShadow campaign established approximately **21--23K
  sustained 8 KiB writes/s**, with complete lifecycle work and no growing
  deferred debt, on the measured Apple-silicon host. This is approximately
  164--180 MiB/s of logical payload and is a campaign result, not a universal
  hardware floor.
- The measured path is substantially a single foreground writer lane. Future
  multi-lane work has headroom, but it is not the present product priority.

These facts establish a dependable datastore. They do not establish query
completeness, atomic state transitions or distributed correctness.

## 3. Admission law

Until all three critical-path gates pass, active work is limited to:

- the currently admitted critical-path package;
- a P0 correctness or security defect;
- infrastructure strictly necessary to execute or qualify that package;
- formal models, adversarial tests and measurement harnesses required by its
  acceptance gate; and
- corrections needed to keep specifications and claims honest.

The following do **not** enter the active lane merely because they are useful:

- Studio and other management UI work;
- website or marketing expansion;
- text, vector or geospatial search;
- native graph traversal, path, analytics or graph-storage implementation
  (destination and staged profiles are recorded in
  [doc/todo/graph/](./doc/todo/graph/));
- Kiku/COBOL/ISAM rehosting implementation (the provisional architecture is
  recorded in
  [KIKU_COBOL_ISAM_REHOSTING_SPEC.md](./doc/todo/integrations/KIKU_COBOL_ISAM_REHOSTING_SPEC.md));
- broad SDK proliferation;
- general product polish;
- speculative storage optimisation;
- new query-adjacent features outside the frozen RQL conformance target; or
- embedded-product implementation beyond bounded discovery work.

Such work goes to backlog. It must not consume the critical-path capacity or
create a competing priority document.

One package may be active at a time unless the principal explicitly permits a
non-blocking parallel package. “Parallel” never means “quietly promoted.”

## 4. Gate 1 — RQL

### 4.1 Question to answer

RQL must be an adequate document-native replacement for the useful
computational surface for which users reach for SQL, while retaining
Residiuum's explicit coverage, ordering, cursor and damage semantics.

The goal is not SQL syntax parity. The goal is that an application developer
can obtain the answer they need without escaping into collection-wide
application code, and that unsupported operations are refused explicitly.

### 4.2 Required semantic surface

The RQL conformance decision must explicitly cover:

- selection and nested projection;
- total missing/null/value semantics;
- scalar, object and array predicates;
- deterministic ordering with immutable tie-breaking;
- cursor continuation without offset-style prefix discard;
- indexed point, range and compound predicates;
- relationship enrichment with explicit cardinality;
- composition and reusable subplans;
- grouping and aggregation, or a documented equivalent mechanism;
- conditional expressions needed for practical result shaping;
- query budgets, consistency and coverage requirements;
- index declarations and honest index eligibility;
- explain output that reports the executed plan and refusal reasons; and
- deterministic SQL-to-RQL translation for the declared SQL subset.

Every SQL construct outside that subset must produce a stable diagnostic. It
must never be ignored, weakened or interpreted approximately.

### 4.3 Correctness programme

RQL requires one executable semantic oracle independent of the optimiser.
Every optimised plan is checked against that oracle using:

- golden examples and SQL cross-compiler vectors;
- property and metamorphic testing;
- differential execution across scan and index plans;
- fuzzed documents, predicates, plans and continuation tokens;
- missing/null/type/cardinality edge cases;
- deterministic pagination across updates and ties; and
- damaged-media and incomplete-coverage cases.

A hole must never become an empty result, a false absence proof or a complete
page. Indexes may accelerate only what their recorded coverage permits them to
prove.

### 4.4 Read qualification

The query engine is measured under:

- point and indexed equality lookup;
- range and compound predicates;
- nested fields and arrays;
- order/top-k and cursor pagination;
- enrichment and aggregation;
- cold and warm cache;
- datasets larger than memory;
- concurrent readers;
- mixed reads and writes;
- segment rotation, reopen and compaction; and
- declared damage with surviving data.

Record throughput, p50/p95/p99 latency, CPU, resident memory, physical I/O,
read amplification, index size, build cost and coverage status. Tail latency
and correctness are first-class results; a median-only win is insufficient.

Comparison with MongoDB and Couchbase Lite must use the same machine, logical
dataset, query result, index entitlement, durability posture, cache state and
concurrency. The report must disclose every difference. The purpose is to
locate weaknesses and establish fitness, not manufacture a favourable chart.

### 4.5 RQL exit gate

RQL is de-risked only when all of the following are true:

1. The v1 semantic profile and its deliberate exclusions are frozen.
2. The parser, builder and SQL frontend compile to one canonical plan.
3. The reference oracle and conformance corpus are independently executable.
4. Scan and every admitted index plan are result-equivalent, including
   coverage and continuation semantics.
5. The required practical query corpus is expressible without application-side
   collection scans.
6. Read qualification passes its declared correctness and resource gates under
   strain.
7. MongoDB and Couchbase Lite comparisons are reproducible and honestly
   disclosed.
8. Formal claims, tests and implementation are connected; no proof refers only
   to an unused model.

Except for the dated principal amendment in §1.1, this gate would restrict
Atomics to specification corrections and small feasibility spikes. The
amendment explicitly admits Atomics while preserving every unaccepted RQL
residual.

### 4.6 Immediate RQL package

The inventory package **RQL-0** produced the gap ledger and package sequence
([RQL0_GAP_LEDGER.md](./doc/todo/rql/RQL0_GAP_LEDGER.md)). **Decision 0**
(2026-08-05) amended that ledger: parallel semantic executors are an
architectural violation. **RQL-X1** freezes the bytecode + host boundary
([QUERY_BYTECODE_V1.md](./doc/todo/rql/QUERY_BYTECODE_V1.md)). The active
convergence package is **RQL-X2** (one runtime; delete frozen Rust executors).

Living short status:
[RQL_WHAT_IS_LEFT.md](./doc/todo/rql/RQL_WHAT_IS_LEFT.md).

Gate-1 implementation and qualification authority:
[RQL_QUERY_QUALIFICATION_PROGRAM.md](./doc/todo/rql/RQL_QUERY_QUALIFICATION_PROGRAM.md).
This programme owns the capability corpus, independent semantic qualification,
cross-engine harness, controlled baseline, optimisation sequence and final RQL
exit decision. `RQL0_GAP_LEDGER.md` remains the implementation inventory; it is
not a competing qualification roadmap.

Application-path qualification authority:
[ASYNC_DRIVER_SPINE_SPEC.md](./doc/todo/application-driver/ASYNC_DRIVER_SPINE_SPEC.md).
The Driver Spine is admitted only as infrastructure required to exercise RQL
through a realistic async application path: bounded pooling, streamed query
results, deadlines/cancellation, retry and receipt truth, embedded scheduling,
and server read concurrency. This does not admit broad SDK proliferation,
Atomics implementation, cluster routing, or non-Rust bindings before their
critical-path gates.

RQL-0 still forbids adding syntax until its semantics and execution owner are
named. Execution owner under Decision 0 is the **one bytecode runtime**, not a
new Rust façade.
## 5. Gate 2 — Atomics

### 5.1 Question to answer

Atomics must prove that Residiuum can perform one bounded, serializable state
transition with durable identity and an independently examinable outcome. It
must also quantify what that guarantee costs.

### 5.2 Required proof surface

The programme must cover:

- create-if-absent and compare-version replacement/deletion;
- multi-record all-or-nothing visibility inside the declared scope;
- read, absence, range and uniqueness predicates;
- lost-update, write-skew and conflicting-writer behaviour;
- retry and idempotency identity;
- exactly one terminal decision for every issued Atomic;
- crash before prepare, during member persistence, before decision, after
  decision and during recovery;
- RRE and relationship consequences in the closed plan;
- cancellation, timeout and unknown-outcome semantics; and
- examination sufficient to distinguish aborted, committed, incomplete and
  damaged evidence.

### 5.3 Cost qualification

Measure ordinary writes against Key and LocalHeap Atomics across member count,
payload size, contention and durability. Report throughput, tail latency,
write amplification, memory, recovery time and the precise serialization
bottleneck. A safety proof without a cost model is not product qualification.

### 5.4 Atomics exit gate

Atomics is de-risked only when:

1. The admitted isolation and visibility model is unambiguous.
2. A formal model states the safety properties and assumptions.
3. Machine-checked obligations are connected to the actual decision kernel.
4. The crash/concurrency matrix agrees with the model.
5. Recovery is deterministic and independently examinable.
6. RQL predicates used by Atomics retain their frozen meaning.
7. Performance and contention costs are measured and acceptable for the named
   profiles.

Cluster implementation is not admitted before this gate because distribution
must preserve a known local atomic contract, not invent one.

## 6. Gate 3 — Cluster

### 6.1 Question to answer

The cluster must preserve Residiuum's storage, query and Atomic semantics while
machines, networks and media fail. It must remain operable without turning
deployment into an expert-only exercise.

### 6.2 Required system surface

The programme must cover:

- authenticated zero-configuration discovery, founding and joining;
- durable membership and fenced writers;
- partition placement, replication, split, migration and rebalance;
- quorum, leader loss, stale leader and network-partition behaviour;
- node, disk, rack/zone and correlated failure;
- Medusa durability and repair evidence;
- deterministic distributed RQL merge, ordering and continuation;
- partition-local Atomics and explicit refusal/semantics across partitions;
- overload, hot partitions, admission control and backpressure;
- rolling upgrade and mixed-version refusal boundaries;
- disaster reconstruction without the control plane; and
- the restricted client ingress path without management authority.

### 6.3 Cluster qualification

Qualification uses real multi-process/multi-host execution and a deterministic
nemesis campaign: kill, pause, partition, reorder, corrupt, fill disks, skew
clocks within the declared model, replace nodes and interrupt membership
changes. Histories are checked against the RQL and Atomic contracts, not merely
against node availability.

### 6.4 Cluster exit gate

Cluster is de-risked only when:

1. No admitted failure creates split-brain authority or silent committed-data
   loss.
2. Query coverage, ordering and continuation remain honest during movement and
   failure.
3. Qualified Atomics preserve their isolation contract.
4. Recovery and repair are bounded, observable and independently verifiable.
5. Zero-configuration three-node deployment succeeds from declared inputs and
   fails safely under ambiguity.
6. Load, rebalance and failure qualification meets declared availability,
   latency and durability gates.
7. Formal safety obligations are connected to the shipped protocol kernels.

Passing this gate establishes the database claim targeted by this programme.

## 7. Embedded is a deployment profile, not a fourth gate

Embedded Residiuum is a serious market opportunity: a document-native,
recoverable alternative to SQLite for mobile and local applications. It must
share the same storage, RQL and Atomic kernels. There will not be an embedded
fork with weaker accidental semantics.

The current observed resident-memory class of roughly 300--500 MiB may be
acceptable for a server but is not a mobile profile. Before implementation,
attribute memory to active buffers, indexes, caches, Recovery Shadow, queues,
worker stacks, memory maps and the operating-system page cache.

Provisional design targets, not current claims:

```text
idle resident memory       <= 32 MiB
ordinary working set       <= 64 MiB
configured hard envelope   <= 128 MiB
idle background threads    0 or 1
```

The intended `Embedded` profile uses the same on-disk truth with one writer
shard, bounded workers and queues, smaller segments, strict cache budgets,
streaming scans, lazy indexes, bounded RQL workspace, minimal telemetry, and no
server/TLS/cluster/ingress components. CompactShadow remains the recovery
default unless a separately named and explicitly weaker profile is ever
approved.

Product readiness additionally requires a stable C ABI, Swift package /
XCFramework, Kotlin AAR/JNI, cross-language golden fixtures, and mobile tests
for kill-during-write/seal, low-memory pressure, background/foreground,
disk-full, encryption/key storage, backup policy, upgrade, battery/thermal and
data larger than RAM.

Embedded discovery may measure and allocate memory while RQL is active, but it
must not take critical-path capacity without principal promotion. Its product
implementation is admitted only when the shared local RQL and Atomic contracts
it depends on are stable.

## 8. Evidence law

Every critical-path claim must identify:

- the exact semantic profile and build;
- workload, dataset and seed;
- hardware, filesystem and configuration;
- durability, recovery and index settings;
- correctness oracle and coverage result;
- repetitions, distribution and raw artifacts;
- resource measurements and known observer cost; and
- explicit non-claims.

Smoke proves wiring. Diagnostic runs locate causes. Qualification supports a
bounded product claim. Formal proof establishes only the theorem and
assumptions actually connected to shipped code. None may impersonate another.

A package is not complete because code exists, tests are numerous, a board card
is in review or one attractive number appeared. Completion requires its frozen
gate and principal acceptance.

## 9. Exit condition

We may relax this freeze and return to broad product development only after the
RQL, Atomics and Cluster gates above are each recorded as accepted with linked
evidence.

At that point Residiuum will have proved, in order:

```text
data survives
queries mean what they say and perform under strain
compound state changes are atomic and examinable
the same guarantees survive distribution and failure
```

That is the transition from a proven datastore to a proven database.
