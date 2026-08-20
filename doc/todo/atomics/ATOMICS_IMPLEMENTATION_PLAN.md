# Atomics implementation and delivery plan

Status: **developer-ready v1.1; active critical-path programme**

Initial inspected implementation baseline: clean `main` at `ec80380`
(2026-08-10). Current continuation record:
[ATM5B_OUTCOME_AND_JOURNEY_BASELINE_2026-08-20.md](./ATM5B_OUTCOME_AND_JOURNEY_BASELINE_2026-08-20.md).

Normative semantics: [ATOMICS_SPEC.md](./ATOMICS_SPEC.md)

Programme authority: [CRITICAL_PATH.md](../../../CRITICAL_PATH.md) §1.1 and §5

Package IDs: `ATM-0` through `ATM-5` as defined here and in the living
scoreboard. Older proposal/package meanings are superseded.

## 1. Delivery objective

Deliver and qualify this product statement:

> Within one Heap, Residiuum commits one bounded serializable transition with
> stable identity, all-or-nothing visibility, durable decision evidence, safe
> retry, and independently examinable recovery.

The immediate application journey is Gremlin's authoritative state update:

```text
replace conversation state if version V
create turn record if absent
create turn-id locator if absent
```

All three commit or none commit. A stale state version creates neither
projection. The result survives restart, lost reply, compaction, and repeated
submission with the same `AtomicId`.

This programme does not deliver cross-Heap, cross-partition, synchronous,
interactive, long-running, or external-side-effect transactions.

## 2. Current baseline and delta

The codebase is not starting from zero, but none of the precursors below is a
multi-record Atomic.

| Area | Landed baseline | Missing Atomic work |
|---|---|---|
| Identity | immutable `HeapId`, `CollectionId`, version/event IDs; `OperationId` replay | `AtomicId`, content root, Heap-bound canonical plan |
| Authority | one physical async `Client`, many capability-bound `HeapClient`s | union-of-rights validation for every plan member; cross-Heap refusal |
| Key concurrency | create-if-absent, present/version CAS put/delete | one validation point for multiple targets and predicates |
| Async scheduling | bounded count/byte admission, deadlines, cancellation stages | Atomic-weighted admission; post-admission outcome resolution |
| Physical write | parallel cooking, multi-shard append, durable group commit | staged invisible members, prepare/decision order, one logical publication |
| Idempotency | authoritative per-item operation identity plus derived lookup journal | lifetime Atomic decision tombstone and status recovery |
| Format | reserved `BatchPrepare`/`BatchCommit` frame kinds | frozen Atomic envelopes, member linkage, vectors, hostile parser |
| Reads | version-bearing point reads and scans; bounded RQL | immutable Heap publication generation and predicate witnesses |
| Recovery | active-tail recovery, OpenReport, failpoints, Recovery Shadow | Atomic decision recovery, prepared-member suppression, bounded status rebuild |
| SDK | `Capabilities::atomics = false` | async builder/commit/status/receipts and conformance journey |

Two existing mechanisms MUST NOT be relabelled:

1. `put_many` and operation commit cohorts are physical efficiency mechanisms;
   their entries retain independent outcomes.
2. Key-local CAS is a one-member Atomic precursor; it does not make a sequence
   of calls atomic.

### 2.1 Architectural seams to preserve

- one physical deployment writer, scheduler, inspection state, and shutdown
  domain per `driver::Client`;
- any number of separately authorized Heap bindings per connection;
- async-only product mutation APIs;
- one canonical QVM/RQL predicate meaning;
- persist-before-publish;
- no full-store work on ordinary open or commit;
- coverage-honest reads and indexes; and
- Recovery Shadow and salvage evidence remain at least as strong for Atomic
  decisions as for ordinary values.

## 3. Closed architecture decisions

These decisions are not left to implementation PRs.

| Topic | Decision |
|---|---|
| Product scope | one `LocalHeap` |
| Isolation | serializable |
| Mutation API | immutable bounded plan; create, explicit blind upsert, version-replace, version-delete, assertions |
| Unconditional write | explicit `put_unconditional`; computed-from-read callers must also assert the read version |
| Product surface | async builder + `commit_atomic().await` + `atomic_status().await` |
| Server interaction | one-shot plan; no held interactive session |
| Ordering | every predicate-affecting write participates in one Heap commit order |
| Linearization | durable valid committed decision |
| Visibility | one whole Heap read-view delta; prepared members invisible |
| Durability | durable acknowledgement only; buffering/group commit permitted internally |
| Retry | same ID/root returns original decision; different root refuses forever |
| Cancellation | definite before admission; outcome lookup required after admission |
| Evidence | prepare + linked members + decision; derived status cache is not authority |
| Recovery | deterministic from verified evidence; no guessed commit |
| Distribution | deferred; no cross-partition implication |
| Compatibility name | Atomic is primary; transaction names may later adapt it |

Any proposed change to this table is a specification amendment reviewed by the
architects before code is merged.

## 4. Ownership and dependency boundaries

Create one pure protocol crate:

```text
crates/residiuum-atomics/
  src/
    id.rs
    limits.rs
    plan.rs
    canonical.rs
    evidence.rs
    outcome.rs
    oracle.rs
  tests/
    vectors.rs
    canonical_properties.rs
    hostile_decode.rs
    oracle_histories.rs
```

Module ownership:

| Owner | Responsibility | Forbidden responsibility |
|---|---|---|
| `residiuum-atomics` | pure types, canonical codec, hashes, semantic oracle | files, threads, SDK handles, RPC, store internals |
| `residiuum-format` | frame/envelope encoding and bounded decode | commit semantics |
| `residiuum-store` | sequencer, staged members, decision, publication, recovery | public application ergonomics |
| `residiuum-sdk::driver::atomics` | typed builder, admission, async submit/status, receipts | alternate storage or query engine |
| `residiuum-server` | later one-shot authorized RPC adapter | interactive transaction state |
| `residiuum-examine` | evidence/status projection | deciding or repairing without authority |
| `residiuum-store-model` / formal harness | independent state machine and history checker | importing the production decision kernel |

The first release target is embedded LocalHeap. Remote plumbing may be
implemented behind the same plan codec after embedded ATM-5; it does not block
the Gremlin handoff. Cluster work is not part of ATM-0…ATM-5.

## 5. Package dependency graph

```text
ATM-0 protocol + oracle
  |
  v
ATM-1 canonical plans and validation
  |
  v
ATM-2 prepare/member evidence and invisible staging
  |
  v
ATM-3 durable decision, universal Heap order, atomic publication
  |\
  | +--> formal decision-kernel obligations
  v
ATM-4 recovery, status, maintenance, serial histories
  |
  v
ATM-5 async SDK, Gremlin journey, performance and release gate
```

No package is accepted from code existence. Its tests, fixtures, evidence
manifest, review checklist, and negative controls must all pass.

## 6. ATM-0 — protocol freeze and independent oracle

### Purpose

Make every later byte and outcome testable without the production store.

### Deliverables

- add `residiuum-atomics` to the workspace;
- freeze `AtomicId`, scope, limits, mutation/predicate vocabulary, outcomes,
  material status, and abort reasons;
- add `spec/atomics/cbor-v1.json` with exact deterministic-CBOR field numbers,
  required fields, widths, limits, and domain separators;
- add accepted and rejected byte fixtures plus stable content-root hashes;
- implement canonical target ordering and duplicate-target refusal;
- implement a deliberately slow serial in-memory oracle;
- implement a history format consumed by both the oracle and later store tests;
- add a hostile decoder corpus for depth, count, byte, duplicate-key, integer,
  unknown-kind, and trailing-data attacks; and
- create the initial formal state-machine definition for prepare/member/
  decision/publication states.

### Required properties

- equivalent builder order produces identical canonical bytes and root;
- any semantic change changes the root;
- Heap or collection substitution changes/refuses the plan;
- same ID/same root replays; same ID/different root conflicts;
- no decoder allocates before enforcing its declared bound;
- the oracle never produces a partially visible state; and
- unknown profiles are preserved by examination but refused by execution.

### Exit evidence

```text
target/atomics-evidence/atm-0/
  manifest.json
  protocol-vectors.json
  rejected-vectors.json
  property-summary.json
  hostile-corpus-summary.json
  model-check-summary.json
```

ATM-0 acceptance freezes the semantic and byte contract. Later changes require
new fixtures and an explicit compatibility decision.

## 7. ATM-1 — canonical plan compiler and validation kernel

### Purpose

Turn SDK intents into one immutable, authority-bound, fully costed plan before
any authoritative write.

### Deliverables

- internal typed builder using collection IDs from one `HeapClient`;
- immutable `AtomicPlan` with no public field construction;
- encoding of create, explicit blind upsert, version-replace, version-delete,
  `assert_absent`, `assert_present`, and `assert_version`;
- canonical serialization of values before admission;
- exact requested and worst-case generated-member byte accounting;
- rights union and authority-revision binding;
- deadline and configured/hard-limit validation;
- read witness types for exact version and absence;
- pure closed-plan validator shared by embedded and future remote execution;
- differential tests against ATM-0 oracle; and
- mutation tests that prove validators are sensitive, not ceremonial.

### Refusal contract

Structural validation before acceptance returns a typed request error for
duplicate target, cross-Heap collection, stale/foreign capability, invalid
value, unsupported predicate, deadline, or limit. It appends no Atomic
evidence and guarantees non-acceptance. Data preconditions and rule predicates
are evaluated after acceptance under the sequencer; failure writes a durable
`not committed` decision so the accepted ID can never execute differently.

### Exit gate

At least the following plans agree with the oracle: one member, 2/10/64/256
members, 1/16/64 collections, create+blind-put+replace+delete mixes, assertion-only
guards, same key across distinct collections, same human names across two
Heaps, maximum accepted values, and every one-unit-over-limit case.

## 8. ATM-2 — authoritative evidence and invisible staging

### Purpose

Persist enough verified material to make a later decision meaningful without
ever exposing prepared state.

### Deliverables

- exact `BatchPrepare` and `BatchCommit` Atomic envelopes in
  `residiuum-format`;
- Atomic linkage fields on item-event members;
- designated per-Heap coordinator stream and allocator;
- member placement manifest across writer shards;
- staged append path that does not update the ordinary primary index;
- chunked-value member support with complete manifest commitment;
- first stable boundary covering prepare and every member;
- examination projection for valid, partial, corrupt, and unsupported
  evidence;
- failpoints `before_prepare`, `after_prepare`, and `after_member_n`; and
- byte-level recovery-reader tests independent of the write path.

### Invariants

- no staged member is visible through point get, scan, RQL, history, watch, or
  secondary index;
- member order and target/payload identity are committed by the prepare root;
- all member bytes named by a decision can be proven durable before decision;
- shard rotation cannot orphan or publish a staged member;
- a second Heap cannot discover or resolve the first Heap's Atomic; and
- physical cohort neighbours cannot acquire each other's Atomic identity.

### Exit gate

Crash or injected I/O failure at every byte/phase before decision leaves no
ordinary visible mutation after reopen. Examination reports the surviving
prepare/material accurately. Negative-control tests that intentionally publish
one staged member must fail.

## 9. ATM-3 — durable decision and atomic publication

### Purpose

Create exactly one serial decision and make all committed members visible as
one logical state transition.

### Deliverables

- one per-Heap commit sequencer/frontier;
- migration of ordinary writes and lifecycle mutations that affect predicates
  into that commit order;
- validation under the sequencer immediately before decision;
- monotonic nonzero `HeapCommitPosition` allocation and durable high-water
  reconstruction;
- decision append only after the member stable boundary;
- decision stable boundary and exact linearization instrumentation;
- immutable read-view generation or equivalent guarded multi-index publish;
- one-delta primary/history publication;
- group commit support that can share boundaries without sharing outcomes;
- exact committed and not-committed receipts; and
- failpoints `before_decision`, `after_decision`, `before_publish`,
  `after_publish`, and `before_ack`.

### Mandatory design review

Before merging the first store implementation, developers submit a short
decision record showing:

1. which files carry prepare, members, and decision;
2. which write/sync establishes each ordering boundary;
3. how multi-shard members are fenced from rotation and publication;
4. how ordinary writes join the same Heap order;
5. how readers bind before/after generations;
6. how a committed-but-unpublished Atomic is recovered; and
7. why no lock is held across file cooking or application/network waits.

### Exit gate

- all-or-none visibility under concurrent point, scan, and RQL readers;
- exactly one commit position for every committed Atomic;
- stale precondition means no committed member;
- crash after durable decision and before publish recovers all members;
- same-ID replay returns byte-equivalent logical receipt;
- no per-member fsync; at most the two ordered Atomic boundaries per physical
  decision cohort; and
- ordinary Key Atomic histories remain serial with LocalHeap Atomics.

ATM-3 is the earliest package on which relationship implementation may build.
It is not a product release.

## 10. ATM-4 — recovery, convergence, status, and serial histories

### Purpose

Prove that the decision remains exact after process death, media damage,
maintenance, and conflicting concurrency.

### Deliverables

- bounded Atomic status index/checkpoint plus authoritative reconstruction;
- dirty-tail recovery with explicit `OpenReport` phases and reasons;
- deterministic prepared/no-decision cleanup classification;
- lifetime decision tombstones and same-ID receipt replay;
- status response with independent logical and material axes;
- compaction, Recovery Shadow, backup, restore, clone, salvage, scrub, and tier
  rules for Atomic evidence;
- exact version, absence, and bounded exact-range predicate validation;
- read-your-plan overlay used during plan construction/oracle comparison;
- randomized concurrent history recorder and serializability checker;
- formal obligations connected to the production decision kernel; and
- damaged-index refusal when absence or uniqueness cannot be proven.

### Crash matrix

Each cell runs clean, `SIGKILL`, process abort, short write, torn final frame,
ENOSPC, sync failure, and selected bit corruption where meaningful:

```text
before prepare
after prepare
after each member ordinal
before first stable boundary
after member boundary
during decision append
after decision boundary
before publication
during publication
after publication
before reply
during status checkpoint
during compaction/backup/restore
```

### Concurrency histories

- lost update;
- write skew;
- create/create same key;
- replace/delete same version;
- delete/recreate ABA attempt;
- absence phantom;
- bounded range phantom;
- unique-value contention;
- ordinary write versus Atomic;
- two LocalHeap Atomics with overlapping and disjoint sets;
- rule/authority revision change versus commit; and
- two Heaps with identical user keys and Atomic IDs.

Every committed history must have a checker-produced serial order. Every known
anomaly must be rejected by at least one mutation-sensitive test.

### Recovery performance gate

Clean open and normal unclean open are proportional to control metadata plus
dirty active tails, not total database size. The campaign records segments and
bytes examined, prepares/decisions resolved, repairs performed, and time per
phase. A full-store fallback is acceptable only as an explicit degraded rebuild
with a reason and separate measurement.

## 11. ATM-5 — async SDK, application journey, and qualification

### Purpose

Expose only the proven contract and decide whether its cost is product-worthy.

### SDK deliverables

- `residiuum_sdk::driver::atomics` exact types from the specification;
- `HeapClient::atomic(options)` builder;
- `HeapClient::commit_atomic(plan).await`;
- `HeapClient::atomic_status(id).await`;
- per-member before/after versions in committed receipts;
- weighted bounded admission as one indivisible scheduler job;
- cancellation/deadline handling before and after admission;
- `Client::inspect()` Atomic queue, conflict, outcome, latency, member, byte,
  recovery, and group-commit counters with bounded/redacted labels;
- no raw store handles and no synchronous mutation equivalent;
- embedded backend conformance; and
- future remote feature negotiation that fails closed when absent.

`Capabilities::atomics` remains `false` until this complete package is
accepted. It changes only for the backend/profile that passed.

### Mandatory Gremlin journey

1. Open one physical embedded `Client`.
2. Bind authorized `tinker` and `gremlin` Heaps.
3. Build the three-member state/turn/locator plan on `gremlin`.
4. Commit and verify all three values and receipt versions.
5. Repeat the same ID and prove no new events.
6. Reopen and repeat the same ID; receive the same decision.
7. Submit the same ID with changed content; receive identity conflict.
8. Race two replacements from one establishing version; exactly one commits.
9. Kill after decision/before reply; status and retry resolve committed.
10. Use stale state version; neither turn nor locator is created.
11. Attempt to include a `tinker` collection; reject before prepare.
12. Run compaction, reopen, and resolve the original status again.

### Qualification matrix

Run member counts `1, 2, 3, 10, 64, 256`; payload bands `0, 256 B, 8 KiB,
128 KiB, 1 MiB` within total limits; collection counts `1, 2, 8, 16, 64`;
contention `0%, 1%, 10%, 50%, 100%`; and cold/warm/reopen states.

Record:

- commits/s and member mutations/s;
- p50/p95/p99 admission, validation, member-boundary, decision-boundary,
  publication, and end-to-end latency;
- logical bytes, physical bytes, write amplification, write calls, sync calls;
- CPU, RSS, queue credit, cooker utilization, and sequencer wait;
- conflict/not-committed/unknown rates;
- clean and unclean reopen cost; and
- status lookup latency before/after compaction.

### Absolute performance rules

- no per-member fsync;
- no database-wide commit or ordinary-open scan;
- enabling Atomics with an all-ordinary-write workload adds no more than 5%
  throughput regression at the controlled median and no unexplained p99 cliff;
- one-member Atomic overhead is disclosed against the current Key Atomic path;
- 10-member Atomic is compared with ten independently acknowledged writes and
  must demonstrate the expected stable-boundary amortization;
- memory/admission remains within declared bounds at maximum plan size; and
- performance never weakens durability, visibility, validation, or recovery.

Hardware-independent correctness rules are pass/fail. Performance results are
reviewed by the architects against the controlled baseline; developers do not
self-accept an unexplained regression by changing the threshold.

### ATM-5 exit

- all ATM-0…ATM-4 evidence manifests verify from a clean checkout;
- Gremlin journey passes;
- randomized/soak campaign passes for the declared duration and seed corpus;
- crash, damage, authority, hostile-input, and performance reports are linked;
- public docs state exact scope and exclusions;
- package/version compatibility review passes; and
- architects record the acceptance decision before capability advertisement.

## 12. Pull-request delivery sequence

Keep PRs reviewable and never expose a partial public promise.

| PR | Package | Content | Merge gate |
|---:|---|---|---|
| 1 | ATM-0 | pure types, IDs, limits, model skeleton | no store dependency |
| 2 | ATM-0 | canonical codec, vectors, hostile corpus, oracle | byte/profile review |
| 3 | ATM-1 | immutable plan and validation kernel | oracle differential green |
| 4 | ATM-1 | authority/rights and SDK-internal typed builder | cross-Heap negatives green |
| 5 | ATM-2 | format amendment and recovery reader | independent byte fixtures green |
| 6 | ATM-2 | invisible staging across shards/chunks | prepared visibility negatives green |
| 7 | ATM-3 | Heap sequencer/frontier and universal write participation | concurrency design review |
| 8 | ATM-3 | durable decision and whole-delta publication | complete crash prefix green |
| 9 | ATM-4 | status/tombstone/reopen/recovery | bounded-open evidence green |
| 10 | ATM-4 | maintenance/damage + serial history checker | anomaly corpus green |
| 11 | ATM-5 | async public API and scheduler integration | capability remains false |
| 12 | ATM-5 | Gremlin journey, soak, perf, docs, release decision | architect acceptance |

PRs 5--10 should use internal feature gating if needed. The public API must not
be released in a state where it can create evidence that the next release
cannot reopen.

## 13. Evidence and CI contract

Add one top-level verifier:

```text
scripts/verify-atomics.sh quick
scripts/verify-atomics.sh crash
scripts/verify-atomics.sh model
scripts/verify-atomics.sh full
```

The script writes a machine-readable manifest containing commit SHA, dirty
state, toolchain, platform, seed, suite/profile versions, commands, durations,
results, and artifact hashes. `quick` runs in normal CI. Crash/model/full run in
scheduled or controlled lanes and their latest accepted manifests are checked
for profile/commit compatibility.

Required evidence families:

| Family | Proves |
|---|---|
| `ATM-ENC` | canonical and hostile encoding |
| `ATM-ORA` | implementation/oracle agreement |
| `ATM-ISO` | serializable histories and noninterference |
| `ATM-CRS` | crash-prefix outcomes |
| `ATM-DMG` | damage/material truth |
| `ATM-RET` | retry, identity, tombstone retention |
| `ATM-MNT` | compaction/backup/restore/salvage |
| `ATM-AUT` | Heap/collection rights and isolation |
| `ATM-RES` | hard resource bounds and admission |
| `ATM-APP` | async SDK and Gremlin journey |
| `ATM-PERF` | cost and regression disclosure |

Every positive test family needs at least one negative control or mutant known
to fail when the protected rule is removed.

## 14. Governance and change control

### Architects/governance

The architects own semantics, profile/version decisions, scope, acceptance,
and claims. They review package evidence and adjudicate deviations. They do not
accept “the tests pass” without a traceable invariant and sensitive test.

### Developers

Developers own implementation, local design records, tests, fixtures,
instrumentation, and evidence generation. When code pressure conflicts with
the specification, they stop and raise a deviation; they do not silently make
the contract easier.

### Required package handoff

Each package handoff contains:

```text
package and commit
implemented requirements
changed durable/public formats
tests and evidence manifest
negative controls/mutants
known residuals
performance change
recovery/compatibility impact
requested architecture decisions (if any)
```

### Severity

- **RED**: partial visibility, guessed commit, same-ID double execution,
  cross-Heap access, acknowledged decision loss, non-serial history, or
  unbounded input/recovery. Blocks merge/release.
- **AMBER**: complete but materially slow path, incomplete observability,
  unsupported maintenance journey, or missing negative control. Blocks package
  acceptance unless explicitly dispositioned.
- **GREEN residual**: documented ergonomics or optimization outside the frozen
  claim, with no weakening of correctness or operability.

### Specification amendment rule

A durable-format, outcome, isolation, scope, limit-ceiling, retention,
authority, or async-API semantic change requires:

1. proposed spec diff;
2. compatibility and recovery analysis;
3. new/changed fixtures and oracle behavior;
4. architect approval; and
5. implementation only after approval.

## 15. Explicit deferrals

- Partition and cross-partition Atomics;
- cross-Heap Atomics;
- distributed two-phase commit;
- interactive or long-lived transaction sessions;
- synchronous product mutations;
- read-only snapshot sessions;
- arbitrary user code/triggers inside commit;
- external-effect exactly-once claims;
- unbounded cascade;
- sagas disguised as Atomics; and
- performance work that changes the two-boundary safety contract without a new
  proof and profile decision.

## 16. Developer start instruction

Start only `ATM-0`. Before writing store code, produce the pure crate, exact
profile vectors, oracle, hostile corpus, and formal state skeleton. ATM-1 may
start after ATM-0 byte/semantic review. ATM-2 may not invent fields ahead of
that freeze. No team should begin a transaction façade or Gremlin-specific
workaround in parallel.
