# Residiuum scoped transaction compatibility profile

Status: compatibility note v0.3; subordinate to `ATOMICS_SPEC.md`; not an
implementation authority

Target: familiar transaction API over qualified LocalHeap and Partition
Atomics

Normative impact: SDK compatibility policy and transaction terminology;
Atomic semantics and evidence belong to `ATOMICS_SPEC.md`

## 1. Summary

Residiuum should expose familiar transaction terminology only as a compatibility
interpretation of bounded Atomics. It must not present an unbounded “ACID
everywhere” API that the storage and cluster models cannot honestly support.

The compatibility mapping is:

1. **Key Atomic** — single-key conditional operations on every backend;
2. **LocalHeap Atomic** — serializable transaction compatibility inside one
   embedded or single-server heap;
3. **Partition Atomic** — serializable transaction compatibility inside one
   cluster partition;
4. **Workflow** — explicit durable steps and compensation across scopes;
5. no arbitrary distributed transaction claim.

`AtomicId`, scope, logical outcomes, two-dimensional truth, retry identity,
prepare/member/decision evidence, and recovery classification are governed by
`ATOMICS_SPEC.md`. This document may provide transaction-shaped names, but
it cannot broaden or weaken those semantics.

This gives ordinary applications familiar transactional ergonomics while
preserving Residiuum’s larger promise:

> Most databases manage the current state of an application. Residiuum manages
> the lifetime of the data itself.

### 1.1 Terminology rule

In this document:

```text
TransactionId      = AtomicId
transaction commit = Atomic committed decision
transaction abort  = Atomic not-committed outcome
local transaction  = LocalHeap Atomic compatibility
partition transaction = Partition Atomic compatibility
```

“Rollback” means no Atomic members became logically visible. It never means
erasing immutable evidence or reversing external effects. Compensation is a
new workflow step.

If this document conflicts with `ATOMICS_SPEC.md`, the Atomic contract
wins.

## 2. Motivation

Residiuum already provides immutable events, single-key writes, durability
receipts, history, partition-local ordering, and wire kinds for batch prepare
and batch commit. The specifications also describe version-conditional writes
and partition batches.

What the compatibility surface must translate coherently is:

- scope;
- isolation;
- durability;
- idempotent retries;
- conflict handling;
- crash recovery;
- damage and partial survival;
- indexes and history;
- cluster commitment;
- operator examination.

Without that mapping, adding a generic `transaction()` method would create
false expectations. Callers may assume arbitrary cross-collection,
cross-partition serializability, while the implementation may only provide a
best-effort batch.

The adapter must therefore make the Atomic coordination boundary visible and
enforceable.

## 3. Goals

The initial transaction profile MUST provide:

- atomic create, put, replace, and delete for one key;
- optimistic concurrency through stable versions;
- atomic multi-key transaction compatibility in one embedded/single-server
  heap;
- atomic multi-key transactions when every clustered key maps to one
  partition;
- serializable execution within the declared scope;
- stable transaction identity and idempotent commit;
- bounded transaction size, duration, and resource use;
- explicit requested and achieved durability;
- deterministic recovery after process interruption;
- evidence-preserving salvage after physical damage;
- history and examination that retain transaction boundaries;
- equivalent logical semantics across supported backends.

## 4. Non-goals

The initial profile does not provide:

- atomic writes across arbitrary cluster partitions;
- one global serial order over a cluster;
- a globally consistent snapshot across unrelated partitions;
- SQL transactions or relational constraint semantics;
- long-running interactive transactions with unbounded locks;
- external side-effect atomicity;
- exactly-once application effects;
- automatic conversion of a cross-partition transaction into a hidden saga;
- exposure of physically prepared members as committed values.

A future distributed transaction profile may be proposed separately. It must
not weaken independent recovery or hide uncertain outcomes.

## 5. Governing principles

### 5.1 Scope is part of the guarantee

Every transaction declares its coordination scope before mutation:

- one key;
- one local heap;
- one cluster partition.

If an operation falls outside the scope, Residiuum fails before recording any
member. It never silently weakens atomicity or splits the transaction.

### 5.2 Transaction compatibility is serializable within scope

The first profile exposes one isolation level for read-write transactions:
`serializable`.

Avoiding a menu of weaker isolation levels keeps the ordinary contract clear.
Optimizations may use optimistic validation internally, but observable results
must be equivalent to a serial order within the scope.

Read-only snapshot transactions may be added as a separate bounded API. A
multi-partition read never claims one global snapshot unless a future protocol
proves it.

### 5.3 Physical survival is not logical commitment

A prepare frame or transaction member may survive without a valid commit.
That frame remains recoverable evidence, but it is not visible through the
ordinary committed-state API.

### 5.4 No silent uncertainty

After a timeout, disconnect, damaged commit frame, or missing consensus
evidence, the result is not guessed.

The API returns one of:

- committed;
- definitely not committed;
- conflict before commit;
- outcome unknown, with a stable transaction ID and resolution handle.

### 5.5 Retry identity is the Atomic identity

Every transaction-shaped handle carries the governing caller-stable
`AtomicId`, exposed compatibly as `transaction_id`. Retrying the same ID and
content returns the original outcome. Reusing the ID with different content
is `AtomicIdConflict`.

### 5.6 Authority remains in Atomic evidence

Indexes, catalogs, lock tables, and transaction-status caches are derived.
The authoritative outcome is reconstructed from verified Atomic
prepare/member/decision evidence and, for clusters, durable consensus evidence.

### 5.7 Transactions are bounded

The server enforces limits for:

- member count;
- encoded bytes;
- read-set size;
- duration;
- buffered memory;
- touched collections;
- generated history and index work.

Limits fail before commit with typed errors. A transaction is never allowed to
grow until it destabilizes the process.

## 6. Transaction compatibility scopes

### 6.1 Single-key atomic operation

This is the baseline available everywhere.

Supported preconditions:

- key must not exist;
- key must exist;
- visible version must equal `if_version`;
- value hash must equal an expected hash;
- no precondition.

The operation and precondition are evaluated atomically under the key’s
coordination scope.

Example:

```rust
let current = users.inspect("user-42")?;

users.replace(
    "user-42",
    &next_value,
    ReplaceOptions::new().if_version(current.version),
)?;
```

### 6.2 LocalHeap transaction compatibility

A local transaction adapter may touch multiple keys and collections within one
embedded or single-server heap.

Properties:

- serializable isolation;
- one heap identity, authority epoch, and serving generation;
- one durable transaction ID;
- one atomic logical commit;
- no dependency on a cluster partition map;
- same damage-evidence model as partition transactions.

This is the ordinary transaction profile for embedded applications.

Example shape:

```rust
let account = accounts.get_versioned("account-42").await?.unwrap();
let mut atomic = heap.atomic(AtomicOptions::new(atomic_id));
atomic.replace(
    &accounts,
    "account-42",
    account.version,
    &debit(&account.value, 100)?,
)?;
atomic.create(&ledger, "entry-901", &entry)?;
let outcome = heap.commit_atomic(atomic.build()?).await?;
```

### 6.3 Partition transaction compatibility

A partition transaction may touch multiple keys and collections only when
every operation maps to the same cluster partition.

The caller declares a partition key or receives a transaction handle already
bound to a partition:

```rust
// Future qualified Partition profile; not part of LocalHeap v1 delivery.
let mut atomic = heap.partition_atomic("account-42", options)?;
atomic.replace(&accounts, "account-42", version, &account)?;
atomic.create(&ledger, "account-42/entry-901", &entry)?;
let outcome = heap.commit_atomic(atomic.build()?).await?;
```

The SDK computes the partition for every member before submission where
possible. The leader validates scope again. A stale client map cannot authorize
a cross-partition commit.

Properties:

- serializable ordering within one partition;
- one Raft log command or equivalent consensus decision;
- quorum commitment under the strong profile;
- no ordering promise relative to unrelated partitions.

### 6.4 Cross-partition workflow

Cross-partition work uses explicit workflow records, idempotent steps, and
compensation.

Residiuum may provide a saga helper, but it must expose:

- workflow identity;
- completed and pending steps;
- retries and deduplication;
- compensation attempts;
- uncertain outcomes;
- coverage and unavailable partitions.

The helper is not named `transaction` and does not claim atomic rollback.

Example shape:

```rust
let workflow = heap.workflow("transfer-901")?;
workflow.step("debit", debit_command)?;
workflow.step("credit", credit_command)?;
workflow.compensation("refund", refund_command)?;
workflow.run()?;
```

## 7. Isolation and concurrency model

### 7.1 Serializable optimistic execution

The recommended initial implementation is optimistic:

1. Begin at a stable store or partition frontier.
2. Record every read version in the transaction read set.
3. Buffer writes without publishing them.
4. At commit, acquire the scope’s commit sequencer.
5. Validate that read and write preconditions still hold.
6. Assign one commit position.
7. append and persist transaction evidence;
8. publish all members together.

If validation fails, no member becomes committed and the caller receives
`TransactionConflict` or `VersionConflict`.

### 7.2 Why serializable rather than snapshot isolation

Snapshot isolation permits write skew and requires users to understand which
invariants are safe. Residiuum should not advertise an ordinary transaction API
while leaving common multi-key invariants vulnerable by default.

Because the initial coordination scopes are one local heap or one partition,
a serializable commit sequencer is practical. Concurrency can be recovered
through sharded partitions and optimistic execution rather than weaker
semantics.

### 7.3 Read-only snapshots

A read-only snapshot binds to:

- a local durable frontier; or
- one partition term and committed position.

It is bounded by timeout and retention. If required history has been compacted
or damaged, the snapshot reports incomplete coverage rather than silently
reading a newer state.

### 7.4 Locking

The initial profile should avoid long-lived user locks.

Short internal locks are permitted during validation and publication.
Transactions that exceed duration or resource limits expire before commit.
Deadlock detection is unnecessary if the implementation uses one ordered
commit sequencer per scope and does not hold user locks during transaction
construction.

Construction is entirely client-side builder buffering. `build()` freezes one
immutable Atomic plan and that plan is submitted once. The remote API accepts
only a complete plan. No server-side transaction session remains open across
arbitrary network pauses.

## 8. API model

This API is an adapter over Atomics, not a second execution engine. Implementors
MUST compile every transaction request to the corresponding Atomic plan and
return a projection of the Atomic outcome.

### 8.1 Core types

Conceptual API:

```rust
pub type TransactionId = AtomicId;

pub enum TransactionScope {
    LocalHeap,
    Partition {
        partition_key: Vec<u8>,
    },
}

pub enum IsolationLevel {
    Serializable,
}

pub struct TransactionOptions {
    pub transaction_id: Option<TransactionId>,
    pub scope: TransactionScope,
    pub isolation: IsolationLevel,
    pub durability: AtomicDurability, // LocalHeap v1 accepts Durable only
    pub timeout: Duration,
    pub max_operations: u32,
    pub max_bytes: u64,
}

pub struct TransactionReceipt {
    pub transaction_id: TransactionId,
    pub atomic_receipt: AtomicReceipt,
}
```

The SDK may provide convenience accessors for scope, isolation, operation
count, commit position, durability, and evidence, but their sole source is
`atomic_receipt`; it must not maintain a second receipt truth. Exact public
Rust types are frozen by `ATOMICS_SPEC.md` §15. The product API is async-only
and builder-based. This compatibility projection may not add a synchronous
mutation path.

### 8.2 Transaction operations

The LocalHeap v1 product profile supports pre-Atomic version-bearing reads on
ordinary collection handles, then `create`, explicit `put_unconditional`,
`replace(if_version)`, `delete(if_version)`, and assertions in the immutable
plan. A blind upsert derived from a read must also assert that read's version.
The profile does not expose generated-key allocation or an interactive read
method on the builder. Familiar adapters must preserve that closed vocabulary.

Index creation, tier movement, schema changes, compaction, purge, and cluster
membership changes are administrative operations and cannot be transaction
members.

### 8.3 Commit outcomes

Conceptual result:

```rust
pub enum CommitOutcome {
    Committed(TransactionReceipt),
    NotCommitted {
        transaction_id: TransactionId,
        reason: TransactionAbortReason,
    },
    Unknown {
        transaction_id: TransactionId,
        recovery_handle: String,
        last_observed: Option<CommitPosition>,
    },
}
```

An SDK may expose definitely-not-committed conflicts as typed errors, but it
must preserve the distinction from unknown outcome.

### 8.4 Status resolution

Every qualified backend supports the governing async Atomic status call:

```rust
heap.atomic_status(transaction_id).await?
```

The compatibility projection does not invent states. It exposes the logical
and material axes from `ATOMICS_SPEC.md`:

- `not_found` — no evidence within complete declared coverage;
- `committed`;
- `not_committed`;
- `unknown_commit`;
- `conflicting_decision_evidence`; and
- material `complete`, `partial`, `missing`, `conflicting`, or
  `coverage_incomplete`.

`not_found` is legal only when the relevant scope and retention window have
complete coverage.

## 9. Wire representation

`FORMAT_SPEC.md` already reserves core frame kinds:

- `5` — batch prepare;
- `6` — batch commit.

Those historical names may remain as format aliases, but the frames encode
`AtomicPrepare` and `AtomicDecision` evidence governed by
`ATOMICS_SPEC.md`. This compatibility profile does not independently
freeze wire semantics.

### 9.1 Transaction identity

Every Atomic evidence frame and member carries or derives:

- Atomic ID, exposed here as transaction ID;
- store and segment identity;
- scope kind;
- partition ID when clustered;
- transaction ordinal;
- operation count;
- isolation profile;
- snapshot/read frontier;
- transaction content hash;
- created timestamp as diagnostic evidence only.

Wall-clock time never establishes ordering or commitment.

### 9.2 Prepare frame

The Atomic prepare frame contains a deterministic manifest:

- Atomic ID;
- protocol/profile version;
- scope;
- expected member count;
- ordered operation descriptors;
- collection/key identity;
- operation kind;
- member event IDs;
- preconditions and observed versions;
- payload/content hashes;
- total logical and encoded bytes;
- snapshot frontier;
- isolation level.

The prepare frame does not make members visible.

### 9.3 Member frames

Atomic members use ordinary item-event and payload-chunk frames tagged
with:

- Atomic ID;
- operation ordinal;
- member event ID;
- item identity;
- content hash.

They remain independently verifiable and salvageable. Transaction-shaped
readers MAY project the Atomic ID as `transaction_id`.

An item event carrying an Atomic ID is never applied to ordinary current state
unless the complete Atomic decision validates.

### 9.4 Commit frame

The Atomic committed-decision frame contains:

- Atomic ID;
- hash of the prepare frame;
- hash/root covering the ordered member set;
- member count;
- local commit position;
- achieved durability;
- partition term/position and placement epoch when clustered;
- portable commit evidence when available.

A committed decision is valid only when:

1. prepare verifies;
2. every required member verifies;
3. member identities and hashes match the prepare manifest;
4. the commit references the exact prepare/member set;
5. scope and preconditions were valid at the assigned commit position;
6. clustered commitment is supported by durable consensus evidence.

### 9.5 Abort evidence

Structural rejection before durable prepare is a request error, not an abort
of an issued Atomic. Once a valid prepare exists, precondition/rule failure or
recovery abort requires a durable `AtomicDecision(not_committed)` and lifetime
decision tombstone. Absence of a valid decision during recovery is resolved to
that not-committed evidence before the ID is reused.

### 9.6 Recovery classification

Recovery groups frames by Atomic ID and classifies:

- `verified-committed`;
- `verified-aborted`;
- `prepared-uncommitted`;
- `unknown-commit`;
- `incomplete-prepare`;
- `incomplete-members`;
- `conflicting`;
- `unsupported-profile`.

Only `verified-committed` enters ordinary logical state.

All other verified material remains available to examination and salvage.

## 10. Local commit protocol

The normative state machine is the LocalHeap Atomic protocol in
`ATOMICS_SPEC.md`. Its initial transaction-compatible append sequence is:

```text
append AtomicPrepare
    ↓
validate read/write set under Heap sequencer
    ├── conflict → append/sync AtomicDecision(not_committed) → return
    ↓ success
append AtomicMember frames
    ↓
persist prepare + all members (boundary 1)
    ↓
append AtomicDecision(committed)
    ↓
persist decision (boundary 2 / linearization)
    ↓
publish all index changes atomically
    ↓
return receipt
```

Rules:

- Physical contiguity is not required and is not evidence; recovery uses
  identity, manifest roots, hashes, and the decision.
- Visibility is published only after the committed decision is durable.
- LocalHeap v1 has no memory/buffered acknowledgement mode.
- Index/catalog publication must install one Atomic delta indivisibly.
- A crash before valid commit leaves prepared evidence but no committed
  logical mutation.
- A crash after durable commit but before response resolves to the same receipt
  by transaction ID.

## 11. Cluster commit protocol

### 11.1 Partition-linearizable mode

One Partition Atomic, exposed compatibly as a partition transaction, is one
deterministic Raft state-machine command containing or referencing the complete
operation manifest.

Sequence:

1. Client sends stable transaction ID, manifest, and preconditions to the
   partition leader.
2. Leader verifies scope and limits.
3. Leader proposes the transaction command through Raft.
4. A quorum persists the log entry under the consensus durability contract.
5. After commitment, each replica applies the Atomic by writing local Atomic
   evidence idempotently.
6. The leader returns a receipt with term, position, placement epoch, replica
   acknowledgements, and commit evidence.

The exact boundary between Raft-log persistence and Residiuum segment persistence
must be specified before implementation. A “replicated durable”
acknowledgement cannot be returned unless the configured number of replicas
has durable evidence sufficient for recovery.

### 11.2 Leader failure

- Before proposal: definitely not committed.
- After local proposal but before quorum: prepared or outcome unknown.
- After quorum commit: committed even if the client never receives the
  response.
- Retry with the same transaction ID resolves or completes the original
  command; it never proposes altered content.

### 11.3 Convergent-append mode

Mutable multi-key transactions are not supported in convergent-append mode.

An append group may preserve a shared workflow identity, but it
does not claim atomic visibility across split sides. The API must name this a
group or workflow, not a transaction.

## 12. History, indexes, watches, and queries

### 12.1 History

Every committed member records:

- transaction ID;
- transaction ordinal;
- commit position;
- transaction member count.

History can be viewed as individual events or grouped transactions.
Prepared/uncommitted members appear only in examination/salvage views.

### 12.2 Primary index

The primary index applies all transaction mutations as one publication step.
Rebuild groups transaction evidence and ignores unproven members.

### 12.3 Secondary indexes

Secondary index updates are derived from the committed transaction frontier.
They may lag, but:

- all members share one source commit position;
- partial index application cannot prove absence;
- queries fall back to authoritative scan or return incomplete coverage.

### 12.4 Watches

A watch may expose:

- one transaction envelope containing ordered members; or
- member events carrying one transaction boundary.

It must not expose the first member as committed while later members remain
unpublished.

### 12.5 Queries

A transaction reads from its declared snapshot/frontier plus its own buffered
writes. It does not observe another transaction’s prepared members.

## 13. Chunks and large values

Chunked transaction members remain invisible until:

- every required chunk verifies;
- the member manifest verifies;
- the complete transaction commits.

If a commit survives but a chunk is later destroyed, the transaction remains
historically committed while the current payload becomes partial. Residiuum must
distinguish:

- commitment of the logical event;
- present completeness of its payload.

Large transaction limits should prevent one transaction from monopolizing an
active segment or Raft proposal. Oversized workflows should use staged objects
plus a small atomic reference update.

## 14. Compaction, tiering, and salvage

### 14.1 Compaction

Compaction must preserve:

- transaction ID and member ordering;
- commit decision and content root;
- event and item identities;
- enough evidence to prevent prepared members becoming committed;
- history required by active snapshots and deduplication retention.

It may emit a transaction checkpoint only when coverage and source frontiers
are explicit.

### 14.2 Tiering

Transaction evidence may span segments or media after migration. Tier
placement cannot become the only map from a commit to its members.

An offline tier may make transaction status or payload completeness uncertain.
It cannot be represented as abort or absence.

### 14.3 Salvage

Evidence-preserving salvage copies:

- prepare;
- surviving members and chunks;
- commit/abort evidence;
- holes and missing member identities;
- consensus evidence;
- recovery classification and provenance.

Live-state export includes only verified committed transactions whose required
current payloads are complete under the requested export policy.

## 15. Errors

The Atomic layer owns the canonical failure taxonomy. Transaction-shaped SDKs
MAY expose these stable compatibility aliases:

- `version_conflict`;
- `transaction_conflict`;
- `transaction_scope_violation`;
- `transaction_too_large`;
- `transaction_expired`;
- `transaction_not_supported`;
- `transaction_id_reused`;
- `transaction_outcome_unknown`;
- `transaction_incomplete`;
- `durability_unavailable`;
- `partition_unavailable`;
- `coverage_incomplete`;
- `protocol_violation`.

Each alias maps one-to-one to a documented Atomic outcome or error; it may not
collapse `unknown` into `not committed`. Every error states:

- transaction ID when assigned;
- whether any authoritative evidence may exist;
- whether retry is safe;
- requested and achieved guarantees;
- a status/recovery handle when outcome is unknown.

## 16. Security and resource controls

Transactions add denial-of-service and contention risks.

Required controls:

- authenticate before allocating transaction buffers;
- authorize every collection and operation;
- cap concurrent transaction builders per capability and heap;
- cap members, bytes, duration, read set, and response size;
- avoid logging values or secrets;
- audit administrative overrides;
- bind Atomic IDs to the authenticated heap capability where policy requires;
- reject malformed manifests before expensive payload work.

## 17. Observability

Core telemetry uses Atomic names and identity. The compatibility layer MAY
project these transaction-shaped metrics:

- begun, committed, aborted, conflicted, expired, and unknown transactions;
- commit latency by scope and durable-boundary phase;
- validation failures;
- operations and bytes per transaction;
- open transaction count and age;
- prepared/uncommitted evidence discovered;
- deduplication hits and ID-reuse violations;
- partition transaction quorum and apply latency;
- index lag from transaction commit frontier.

Logs and traces include Atomic ID (projected as transaction ID), scope,
partition, term/position,
achieved durable profile, and stable error code. Payloads are excluded by
default.

## 18. Compatibility implementation phases

These phases describe an optional later naming adapter. They depend on, and
cannot replace, `ATM-0`–`ATM-5` in
`ATOMICS_IMPLEMENTATION_PLAN.md`.

### Phase T0 — Compatibility naming and fixtures

- Complete Atomic Phase A0 first.
- Freeze transaction-to-Atomic names, projections, and error aliases.
- Reuse the Atomic ID, outcome, recovery, and wire fixtures.
- Add adapter conformance fixtures.

Exit:

- Every transaction-visible result maps unambiguously to one Atomic result.

### Phase T1 — Single-key preconditions

- Depend on Atomic Phase A1.
- Implement create-if-absent and replace-if-version.
- Make remote retries idempotent by stable operation ID.
- Add committed/not-committed/unknown outcomes.

Exit:

- Single-key ambiguity and version races pass crash and retry tests.

### Phase T2 — Local write-only batches

- Depend on Atomic Phase A2.
- Implement bounded create/put/replace/delete batches.
- Compile each batch to one LocalHeap Atomic plan.
- Rebuild indexes transaction-aware.
- Preserve evidence through salvage.

Exit:

- No partial logical visibility under every injected crash and damage point.

### Phase T3 — Serializable local transactions

- Depend on Atomic Phase A3.
- Add snapshot/read sets and optimistic validation.
- Add read-your-writes.
- Add deterministic conflict handling.
- Integrate secondary indexes, history, and watches.

Exit:

- A serializability checker validates randomized concurrent histories.

### Phase T4 — Remote transaction protocol

- Depend on Atomic Phase A4.
- Add a versioned RPC adapter that submits one complete bounded Atomic plan.
- Preserve transaction ID through timeout and reconnect.
- Add transaction-status resolution.

Exit:

- Embedded and remote backend conformance suites are equivalent.

### Phase T5 — Partition transactions

- Depend on Atomic Phase A5.
- Encode one batch as one Raft command.
- Persist consensus and state-machine evidence.
- Add leader failure, retry, fencing, and quorum tests.

Exit:

- Multi-process network cluster histories are serializable per partition.

### Phase T6 — Workflow helpers

- Add explicit saga/workflow records.
- Preserve retries, compensation, and uncertain steps.
- Keep naming and receipts distinct from transactions.

Exit:

- Cross-partition examples cannot be mistaken for atomic transactions.

## 19. Conformance tests

### 19.1 Atomicity

- crash before prepare;
- crash during prepare;
- crash after prepare;
- crash between every member;
- crash during chunk write;
- crash before commit;
- crash during commit;
- crash after durable commit before publication;
- crash after publication before response;
- damaged prepare, member, chunk, or commit;
- reordered and duplicated segment copies.

Expected: all members are visible or none are visible; noncommitted evidence
remains examinable.

### 19.2 Isolation

- lost-update race;
- write skew attempt;
- read/write conflict;
- phantom over indexed and scan paths;
- read-your-writes;
- concurrent create-if-absent;
- delete/recreate version race;
- transaction expiry during validation.

Expected: observed histories are serializable within scope.

### 19.3 Retry and identity

- response loss before and after commit;
- reconnect to another server/leader;
- duplicate request;
- same ID with different content;
- status lookup after restart and compaction;
- deduplication horizon expiry.

### 19.4 Scope

- two collections in one local heap;
- all keys in one partition;
- accidental second partition;
- stale partition map;
- placement epoch change during commit;
- unsupported convergent mode.

### 19.5 Coverage and recovery

- offline tier containing a member;
- missing consensus evidence;
- control-plane loss;
- salvage without catalogs;
- partial payload after historical commit;
- unsupported future Atomic profile.

### 19.6 Backend parity

Run the same logical corpus against:

- embedded local heap;
- single-node remote server;
- in-process partition harness;
- multi-process network cluster.

Unsupported scopes must fail explicitly rather than degrade.

## 20. Performance requirements

Transactions must not reintroduce full-store work on commit.

Benchmark:

- one-key transaction overhead versus ordinary put;
- 2, 10, 100, and maximum-member batches;
- read-write contention;
- conflict-heavy workload;
- durable group-commit width and member counts;
- remote transaction latency;
- partition quorum latency;
- recovery scan with prepared transactions;
- index and watch publication;
- large chunked member behavior.

Reports disclose p50/p95/p99, throughput, durability, verification, member
count, bytes, contention, abort rate, replication, and hardware.

## 21. Compatibility and versioning

Atomics own the semantic, wire, recovery, and cluster profile versions.
Transaction compatibility independently versions only:

- SDK transaction API;
- RPC adapter surface;
- transaction-shaped examination projection.

Readers must preserve unknown future Atomic evidence losslessly. They must not
apply an unsupported Atomic profile as committed state.

The Atomic wire profile must remain draft until crash, damage, retry, and
interoperability suites pass.

## 22. Required specification changes

If this proposal is accepted:

1. Implement and qualify the governing Atomic contract in `ATOMICS_SPEC.md`.
2. Amend `OVERVIEW.md` §7.3 with Atomic invariants and recovery
   classification.
3. Freeze `FORMAT_SPEC.md` Atomic prepare/member/decision envelopes.
4. Align `DX_SPEC.md` §9 with Atomic scopes and this compatibility API.
5. Amend `CLUSTER_SPEC.md` with Partition Atomic Raft and durability rules.
6. Add Atomic and adapter cases to destructive and cluster conformance suites.
7. Add implementation tasks and release gates to `DEFECTS.md`.
8. Do not increment stable API/profile labels until compatibility review.

## 23. Closed compatibility decisions

1. The product exposes the explicit async Atomic builder and one-shot commit;
   no transaction closure or synchronous mutation path is added.
2. Atomic names are primary. Transaction-shaped names are optional adapters and
   project the same outcome/error types without a second truth.
3. Read-only snapshots, if added, use a separate future API.
4. V1 exposes `atomic_status`; it does not add `transaction_status` merely as an
   alias.
5. Ecosystem adapters may use transaction terminology only after the backend's
   corresponding Atomic capability is qualified.

## 24. Recommendation

Adopt this scoped model rather than a generic distributed transaction API.

The first customer-meaningful compatibility target should be:

> Serializable transaction ergonomics across collections in one embedded
> heap, and the same ergonomics for keys colocated in one cluster partition.

That is enough for account-and-ledger, metadata-and-object, state-and-outbox,
and other ordinary application invariants. It avoids claiming a global
transaction system before one exists.

The implementation remains one system: Atomic is the primitive; transaction is
an adapter that neither widens the scope nor invents a stronger outcome.

Most importantly, it preserves Residiuum’s core distinction:

> A transaction can lose evidence without Residiuum lying about its outcome.
> Whatever survives remains independently verifiable and examinable.
