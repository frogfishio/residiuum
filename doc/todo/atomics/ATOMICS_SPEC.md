# Residiuum Atomics v1 specification

Formal assurance companion:
[FORMAL_ASSURANCE_SPEC.md](../formal-assurance/FORMAL_ASSURANCE_SPEC.md),
theorem families `FAS-6` and `FAS-7`. Atomic safety and isolation proofs are
developed with the implementation, not retrofitted after it.

Status: **normative developer contract v1.1; implementation not yet qualified**

Execution scope: LocalHeap Atomics. Partition Atomics are a separately gated
future profile. Relationship enforcement consumes this contract but does not
delay the core LocalHeap API.

Profiles:

```text
residiuum-atomic-v1
residiuum-atomic-plan-v1
residiuum-atomic-evidence-v1
residiuum-relationship-v1
```

Source proposal: [ATOMICS_PROPOSAL.md](../../done/proposals/ATOMICS_PROPOSAL.md)

Normative companions:
[HEAP_SPEC.md](../../wip/heap/HEAP_SPEC.md),
[RRE_SPEC.md](../rre/RRE_SPEC.md),
[COLLECTION_CONTRACT_SPEC.md](../rre/COLLECTION_CONTRACT_SPEC.md),
[RESIDIUUM_PREDICATE_SPEC.md](../../reference/query/RESIDIUUM_PREDICATE_SPEC.md),
[FORMAT_SPEC.md](../../reference/storage/FORMAT_SPEC.md), and
[doc/todo/atomics/ATOMICS_IMPLEMENTATION_PLAN.md](./ATOMICS_IMPLEMENTATION_PLAN.md)

## 1. Decision

An Atomic is one bounded, serializable state transition inside exactly one
declared coordination scope and exactly one Heap.

It has:

- a stable identity;
- a canonical closed plan;
- one serialization point;
- authoritative prepare/member/decision evidence;
- explicit retry behavior;
- explicit outcome uncertainty;
- independently examinable recovery semantics.

An Atomic is not defined by an API closure, adjacent writes, a process mutex,
or transaction-shaped syntax.

Physical batching is not logical atomicity. The existing operation commit
coordinator may share cooking, append, and stable-media boundaries among
independent writes; each member still succeeds or fails independently. An
Atomic instead has one plan identity, one validation result, one decision, and
one all-or-nothing publication.

The product statement is:

> Within one Key, LocalHeap, or qualified Partition scope, Residiuum can commit
> one bounded serializable transition with durable identity and independently
> examinable outcome evidence.

## 2. Requirement language

MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are normative.

## 3. Scope

V1 defines:

- Key Atomics on every backend;
- LocalHeap Atomics for embedded and qualified single-server Heaps;
- the semantic shape of Partition Atomics, disabled until a separate profile
  and qualification gate passes;
- create-if-absent, compare-version replace/delete, and bounded mutation plans;
- serializable read/write and absence predicates;
- RRE enforcement;
- uniqueness and scalar relationships;
- exact outcomes and recovery evidence;
- remote submit/status without interactive server-held transactions.

V1 excludes:

- cross-Heap Atomics;
- cross-partition Atomics;
- interactive sessions holding server locks;
- unbounded member generation;
- external services/effects;
- triggers and user code;
- arbitrary rollback of history;
- cascade delete;
- read-only snapshot sessions;
- distributed sagas presented as Atomics.

Cross-scope work is an explicit workflow with idempotent steps and compensation.

The first shipped capability is `LocalHeap`. A build MUST NOT advertise
`Capabilities::atomics = true` for a backend until the complete ATM-5 gate for
that backend passes. Key-local CAS already present in the product is a
precursor, not evidence that LocalHeap Atomics exist.

## 4. Coordination scopes

| Code | Scope | Boundary |
|---:|---|---|
| 1 | `Key` | one immutable collection/key identity |
| 2 | `LocalHeap` | bounded identities in one embedded/single-server Heap |
| 3 | `Partition` | bounded identities in one qualified strong partition |

Every scope contains exactly one `HeapId`.

There is no wildcard Heap, Heap set, deployment scope, or implicit scope.

Before prepare, the engine MUST close the plan over:

- caller mutations;
- read versions;
- absence/range predicates;
- active RRE revisions;
- relationship/unique consequences;
- history events;
- index invalidation/publication consequences;
- idempotency and decision evidence.

If closure escapes the declared scope, execution fails before prepare.

## 5. Atomic identity

```text
AtomicId = 32 opaque bytes
```

`AtomicId` is distinct from the driver's 16-byte `OperationId` and 16-byte
`RequestId`. It MUST NOT be truncated into either namespace. Atomic members
have event/version IDs for history but are not independently replayable client
operations; retry resolves the enclosing Atomic decision only.

Caller-generated IDs MUST come from a cryptographically secure random source
or a caller-owned stable idempotency derivation.

Engine-generated IDs are:

```text
BLAKE3-256(
  "RESIDIUUM-ATOMIC-ID-V1"
  || heap_id
  || source_operation_id
  || invariant_or_job_id
)
```

An Atomic plan has:

```text
content_root = BLAKE3-256(
  "RESIDIUUM-ATOMIC-CONTENT-V1"
  || canonical_plan_bytes
)
```

Rules:

- same `AtomicId` + same `content_root` resolves the original outcome;
- same `AtomicId` + different root returns `atomic_id_conflict`;
- an expired detailed receipt never permits re-execution;
- a minimal decision tombstone remains until Heap purge.

## 6. Canonical plan

Logical plan:

```text
AtomicPlan {
    profile
    atomic_id
    heap_id
    scope
    read_frontier?
    reads[]
    predicates[]
    mutations[]
    active_rule_revisions[]
    limits
}
```

`read_frontier` is present when the plan contains a witness produced by a
prior read; write-only create plans may omit it. A plan that contains any
prior-read witness MUST contain `read_frontier`.

Canonical close is order-independent. Reads, predicates, mutations, and active
rule revisions are sorted by a total key that includes every semantic field.
A plan MUST NOT name the same `(collection_id, canonical_key)` twice as a read
witness, even when observed version or projection hash differs. A plan MUST NOT
name the same `(predicate_kind, collection_id, canonical_key)` twice.
Request ID, transport attempt, deadline, trace context, bearer capability bytes,
and connection identity are
submission metadata and MUST NOT enter `canonical_plan_bytes`. This permits an
identical plan to be retried over a new request or renewed connection without
an identity conflict. The plan still binds immutable Heap/collection identity,
semantic authority/rule revisions, and the applied semantic limits.

The LocalHeap v1 mutation vocabulary is closed:

```text
Create       collection_id, key, encoded_value
Put          collection_id, key, encoded_value
Replace      collection_id, key, if_version, encoded_value
Delete       collection_id, key, if_version
AssertAbsent collection_id, key
AssertPresent collection_id, key
AssertVersion collection_id, key, version
```

`Put` is an explicit blind upsert and is serializable at the Atomic commit
position. It is not CAS. If its value was computed from a prior read, the
caller MUST also declare that read with `assert_version`; otherwise the plan
has intentionally declared no dependency on the overwritten version. The SDK
names the method `put_unconditional` so a blind write is not confused with
`replace(if_version)`.

A plan MUST reject before prepare:

- the same `(collection_id, canonical_key)` appearing more than once as a
  mutation target;
- a collection belonging to another Heap;
- a collection capability lacking the required right;
- a key/value that fails the collection's frozen encoding contract;
- an unknown mutation or predicate kind; or
- any configured or hard resource ceiling.

Assertions may share a target with its one mutation; they are folded into that
member's precondition during canonicalization. Canonicalization is pure and
must produce byte-identical output independent of builder call order.

Members are canonically ordered by:

```text
(heap_id, collection_id, canonical_key_bytes, member_kind, ordinal)
```

The first component is constant inside one plan but remains in the definition
to prohibit accidental cross-Heap reuse.

Canonical key bytes use the Heap key profile:

```text
string UTF-8 bytes
opaque byte string
mathematical integer canonical signed encoding
exact decimal canonical coefficient + scale
```

Boolean, Null, products, sequences, and floating point are not relationship or
ordered-lock keys in v1.

Paths use the exact canonical RRE path profile. No host-language path syntax is
accepted after compilation.

## 7. Predicates and reads

V1 supports:

- exact version equality;
- key absence;
- key presence;
- exact scalar equality;
- bounded key-range absence/presence when the index declares exact coverage;
- active rule revision equality;
- collection/object lifecycle state;
- Heap authority/security revision.

Every read records:

```text
ReadWitness {
    object_identity
    observed_version_or_absent
    projection_hash
}
```

Every absence/range predicate records the exact index/order domain and frontier
under which absence was observed.

A candidate or damaged index cannot prove absence.

The public builder exposes `assert_absent`, `assert_present`, and
`assert_version`. Exact scalar and range predicates are compiled through the
canonical RQL/RRE predicate representation; they are not host-language
closures. A caller whose computation read a record that it does not mutate
MUST add that record's exact version witness. Otherwise Residiuum makes no
claim about a dependency the plan did not declare.

## 8. Serialization and isolation

Read/write Atomics are serializable.

The LocalHeap reference algorithm is:

1. bind current Heap commit frontier;
2. read and record versions/predicates;
3. build the closed mutation plan;
4. acquire the LocalHeap commit sequencer;
5. resolve existing identity or durably prepare the accepted plan;
6. validate Heap authority, rights, lifecycle, reads, predicates, and active
   invariant revisions at the serialization frontier;
7. on validation failure, durably record `not committed` and release;
8. on success, allocate one Heap commit position and persist members plus the
   committed decision under the crash protocol;
9. publish one logical committed delta;
10. release sequencer;
11. return receipt only after the durable decision boundary.

The sequencer is an implementation mechanism, not the semantics. An optimistic
or parallel implementation is conforming only if histories are equivalent to
the same serial contract.

### 8.1 One Heap commit order

Once LocalHeap Atomics are enabled, **every** ordinary write, Key Atomic,
LocalHeap Atomic, internal RRE consequence, collection lifecycle mutation, and
derived publication that can affect an Atomic predicate MUST participate in
the same per-Heap commit order. Routing only multi-record requests through a
new lock is non-conforming: an ordinary write could otherwise pass validation
and break serializability.

The existing physical group-commit coordinator remains usable below this
order. It may coalesce independent Key Atomics and LocalHeap Atomics into one
I/O cohort, but it MUST preserve each logical decision and commit position.

### 8.2 Visibility generation

Publication replaces one immutable Heap read view (or applies one delta while
holding its equivalent publication guard). Point reads, scans, RQL, history,
and index consumers bind either the generation before the Atomic or the
generation after it. No reader may bind a generation containing a proper
subset of committed members.

Prepared member frames are not inserted into the ordinary primary index.
Secondary indexes may lag only under their existing honest-coverage rules;
they may not expose a prepared member or use partial Atomic application to
prove absence.

### 8.3 Linearization and durability

For LocalHeap v1, the linearization point is the successful durable write of
the valid committed `AtomicDecision`. Publication follows that decision. A
crash after decision durability and before in-memory publication therefore
recovers to committed and publishes the whole delta.

LocalHeap v1 exposes only durable commit acknowledgements. Buffering and group
commit are allowed before the acknowledgement; one fsync per member is neither
required nor desired. `Memory` or unqualified `Buffered` Atomic acknowledgement
requires a different future profile because it cannot satisfy stable retry and
crash outcome promises.

The commit position is:

```text
HeapCommitPosition = monotonically increasing nonzero u64
```

It is allocated per Heap. Exhaustion fails closed and requires a new profile.
Positions are never reused, including after compaction or restore retaining
identity.

## 9. Evidence

Canonical authoritative evidence:

```text
AtomicPrepare {
    atomic_id
    heap_id
    scope
    content_root
    frontier
    ordered_member_manifest_root
    read_set_root
    predicate_set_root
    active_rule_revision_root
    limits
}

AtomicMember {
    atomic_id
    ordinal
    object_identity
    member_kind
    before_version?
    after_content_hash?
    event_id
}

```

Member field presence is determined by `member_kind` and MUST match the closed
plan mutation shapes. Encode and decode MUST refuse any other combination:

| `member_kind` | `before_version` | `after_content_hash` |
|---|---|---|
| create | omitted | required |
| put | optional (present on overwrite) | required |
| replace | required | required |
| delete | required | omitted |

`object_identity` is the pair `(collection_id, canonical_key)`. It is bound
into the member hash and the ordered member manifest. Recovery MUST be able to
verify which object a member represents from the member record alone.

```text
AtomicDecision {
    atomic_id
    prepare_hash
    member_root
    member_count
    decision
    commit_position?
    durability
    abort_reason?
}
```

Decision codes:

| Code | Decision |
|---:|---|
| 1 | committed |
| 2 | not committed |

`unknown_commit` and `conflicting_decision_evidence` are examination outcomes,
not decisions an engine intentionally writes.

Domain separators:

```text
RESIDIUUM-ATOMIC-PREPARE-V1
RESIDIUUM-ATOMIC-MEMBER-V1
RESIDIUUM-ATOMIC-DECISION-V1
RESIDIUUM-ATOMIC-TOMBSTONE-V1
RESIDIUUM-ATOMIC-MANIFEST-V1
RESIDIUUM-ATOMIC-READSET-V1
RESIDIUUM-ATOMIC-PREDICATES-V1
```

A durable `not_committed` decision MUST record `abort_reason` as one of the
closed `AtomicAbortReason` codes (`precondition_conflict`, `rule_rejected`,
`recovery_abort`, `coverage_incomplete`). The field is omitted on a committed
decision and MUST NOT appear together with `commit_position`. The lifetime
tombstone copies `abort_reason` so same-ID replay after detail removal still
reconstructs `AtomicOutcome::NotCommitted { reason }`.

Persistent v1 uses deterministic CBOR under the repository canonical-CBOR
profile. Exact numeric field assignments MUST land in
`spec/atomics/cbor-v1.json` before `ATM-1`.

LocalHeap storage uses the existing core `BatchPrepare` and `BatchCommit` frame
kinds. Atomic member item events carry `atomic_id`, ordinal, content root, and
Heap commit position under the frozen Atomic envelope extension. The legacy
presence of a `batch_id` field or physical `put_many` call MUST NOT be
interpreted as Atomic evidence.

Format envelope keys (FORMAT_SPEC §4.4 uint map; 1–36 keep existing
format/ownership meanings and are never Atomic identity). Atomic writers MUST
also emit keys 31 (`heap_id`) and 34 (`ownership_profile` = 1) so the frame
admits through the live ownership path:

| Key | Field |
|---:|---|
| 31 | `heap_id` (`bstr` 16; required on Atomic frames) |
| 34 | `ownership_profile` (uint `1`; required on Atomic frames) |
| 37 | `atomic_id` (`bstr` 32) |
| 38 | `ordinal` (uint; `ItemEvent` only) |
| 39 | `content_root` (`bstr` 32) |
| 40 | `commit_position` (nonzero uint; omitted until a committed decision names the member) |

The decision is stored in a designated per-Heap coordinator stream; member
events may reside on any writer shard. The prepare manifest commits to every
member's target, operation, payload hash, and intended shard before any member
is installed. The decision commits to the verified prepare and ordered member
root. Recovery never infers a decision from adjacency, member count, or a
shared fsync.

### 9.1 Local durable protocol

Under the Heap commit sequencer, the implementation performs:

1. resolve prior identity, validate the closed structural plan, and reserve one
   commit position;
2. append the prepare to the Heap coordinator stream;
3. validate data/rule predicates at the serialization frontier;
4. when validation fails, append `not committed`, make prepare plus decision
   durable, publish no member, and return that stable outcome;
5. otherwise append every prepared member to its selected authoritative segment without
   ordinary index publication;
6. make the prepare and all member bytes durable on every touched file;
7. append the committed decision to the coordinator stream;
8. make that decision durable;
9. publish one complete read-view delta;
10. append/update derived status acceleration without adding another required
   acknowledgement barrier; and
11. return the receipt.

Steps 6 and 8 are separate ordering boundaries. An implementation may combine
multiple Atomics at each corresponding boundary, but MUST NOT make a decision
durable before all bytes it names are durable. Failure before the durable
committed-decision boundary in step 8 yields no committed Atomic. Failure after
step 8 yields committed, even if publication or the reply did not occur.

Recovery Shadow, backup, compaction, and salvage MUST preserve Atomic evidence
with the same or stronger survival posture as the member item events. A
recovery representation that preserves values but discards their decision
boundary is not qualified.

## 10. Outcomes

```rust
pub enum AtomicOutcome {
    Committed(AtomicReceipt),
    NotCommitted {
        atomic_id: AtomicId,
        reason: AtomicAbortReason,
    },
    Unknown {
        atomic_id: AtomicId,
        resolution: AtomicResolutionHandle,
    },
}
```

Status returns two independent axes.

Logical:

```text
committed
not_committed
unknown_commit
conflicting_decision_evidence
```

Material:

```text
complete
partial
missing
conflicting
coverage_incomplete
```

`AtomicStatus::NotFound` is a lookup result, not a logical decision. It is
returned only when the status index/evidence search has complete coverage for
that Heap and no valid prepare or decision exists for the ID. In incomplete
coverage, the answer is `unknown_commit + coverage_incomplete`, never
`NotFound`. Every **issued Atomic**—defined as one with a valid durable
prepare—eventually has exactly one durable committed or not-committed decision,
or is reported as conflicting/degraded evidence rather than guessed.

`committed + partial` means the transition committed but later damage removed
material. Healthy members remain examinable; current logical materialization
MUST NOT invent missing values.

Conflict, failed data precondition, and rule rejection after durable acceptance
are `NotCommitted`, not transport errors and not `Unknown`. Post-admission
cancellation does not abort execution; it may make the caller's observation
`Unknown` until status resolution. `Unknown` describes the observer's current
knowledge, never a third decision written by the engine.

There is one sharp admission boundary:

- malformed input, unsupported profile, local builder error, authorization
  failure, hard-limit failure, deadline, or cancellation **before acceptance**
  is a request error; no Atomic was issued and no retry promise is created;
- durable acceptance occurs when the valid prepare for
  `(heap_id, atomic_id, content_root)` is recorded; after that point every
  precondition conflict, rule rejection, or recovery abort produces durable
  `not committed` decision evidence and the lifetime decision tombstone.

An accepted ID never becomes eligible for later re-execution merely because
its first decision was not committed.

## 11. Crash protocol

The implementation MUST provide failpoints:

```text
before_prepare
after_prepare
after_member_n
before_decision
after_decision
before_publish
after_publish
before_ack
```

Allowed restart outcomes:

- no valid prepare: not committed;
- valid prepare and complete members, no valid decision: recovery writes the
  deterministic `not committed` decision/tombstone before the ID can be
  resolved or reused; prepared material remains invisible and may later be
  reclaimed;
- evidence coverage that cannot establish whether a decision existed:
  unknown commit and degraded status;
- one valid commit decision with complete manifest: committed;
- conflicting valid decisions: conflicting evidence, Heap degraded;
- commit decision with damaged members: committed + partial/missing.

Prepared members are never visible to ordinary reads.

Recovery MUST be bounded by Atomic evidence indexes/checkpoints and the tails
opened since their frontier. Normal open MUST NOT scan the full database merely
to rediscover old decisions. Any fallback full scan is a measured degraded
rebuild path with an `OpenReport` reason.

## 12. Retry and retention

A compact exact tombstone:

```text
(atomic_id, content_root, decision, commit_position?, decision_hash, abort_reason?)
```

is retained for the lifetime of the Heap identity and removed only by complete
Heap purge.

Detailed prepare/member/violation evidence is retained for at least:

```text
max(
  Heap history retention,
  active/retained RRE evidence requirement,
  configured Atomic detail retention
)
```

The default Atomic detail retention is 90 days. Heap policy MAY increase it.
It MAY lower it only when no active rule, legal hold, backup contract, or
history policy requires the evidence.

After detail removal, same-ID/same-root retry returns the retained decision
summary. It does not execute again.

## 13. Resource limits

V1 hard ceilings:

| Quantity | Key | LocalHeap / Partition |
|---|---:|---:|
| caller mutations | 1 | 256 |
| total generated members | 32 | 4,096 |
| canonical plan bytes | 256 KiB | 1 MiB |
| total proposed value bytes | 4 MiB | 8 MiB |
| read witnesses | 64 | 4,096 |
| predicates | 32 | 1,024 |
| affected collections | 1 | 64 |
| active rule revisions | 64 | 1,024 |
| construction deadline | 2 s | 5 s |
| emitted violations | 1,024 | 1,024 |

The public LocalHeap v1 builder defaults are stricter than the hard ceilings:

| Quantity | Default |
|---|---:|
| caller mutations | 64 |
| canonical plan bytes | 512 KiB |
| proposed value bytes | 4 MiB |
| affected collections | 16 |
| end-to-end timeout | 5 s |

Admission charges the complete encoded plan, proposed values, generated-member
reserve, and reply reserve against the shared async driver's count and byte
windows. An Atomic is one admission unit and is never split to fit a cohort.

Heap policy MAY lower these ceilings and records the applied limits in the
prepare. Raising one requires a new Atomic profile.

Limit failure occurs before prepare whenever possible and never truncates
members, predicates, violations, or evidence silently.

## 14. Rights

Ordinary execution requires the union of rights for every proposed ordinary
data operation.

Administrative rights:

```text
RuleAdmin
AtomicAdmin
```

- `RuleAdmin`: create, validate, activate, replace, retire RRE rulesets.
- `AtomicAdmin`: create/change/validate/retire relationship, uniqueness, and
  other cross-document Atomic definitions.

These are independent HeapKey right bits and require a rights-registry version
amendment before network use.

Protected recovery modes require a non-serializable local `RecoveryCap`; they
are never granted by an ordinary application key.

Internal enforcement Atomics use a non-serializable engine capability derived
from the active rule/definition and still check the caller's ordinary data
rights.

## 15. Async product API

The Rust product surface is builder plus one-shot asynchronous submission. It
does not expose a synchronous mutation method, an interactive server-held
transaction, or an async closure that performs arbitrary user code while a
database resource is held.

```rust
let mut atomic = heap.atomic(AtomicOptions::new(atomic_id));
atomic.replace(&state, STATE_KEY, state_version, &next_state)?;
atomic.create(&turns, turn_key, &turn)?;
atomic.create(&turn_ids, turn_id_key, &locator)?;
let plan = atomic.build()?;

match heap.commit_atomic(plan).await? {
    AtomicOutcome::Committed(receipt) => { /* all three visible */ }
    AtomicOutcome::NotCommitted { reason, .. } => { /* none visible */ }
    AtomicOutcome::Unknown { atomic_id, .. } => {
        let status = heap.atomic_status(atomic_id).await?;
    }
}
```

Remote API submits one immutable canonical plan. It does not open a transaction
session or hold a lock between client calls.

```rust
heap.atomic_status(atomic_id).await?
```

Required public types live under `residiuum_sdk::driver::atomics` and are:

```text
AtomicId([u8; 32])
AtomicOptions { atomic_id, deadline, limits }
AtomicBuilder
AtomicPlan                    // immutable, Heap-bound, not user-constructible
AtomicOutcome
AtomicReceipt
AtomicMemberReceipt { collection_id, key, before_version?, after_version? }
AtomicAbortReason
AtomicStatus
AtomicResolutionHandle
```

Required method shapes are:

```rust
impl HeapClient {
    pub fn atomic(&self, options: AtomicOptions) -> AtomicBuilder;
    pub async fn commit_atomic(
        &self,
        plan: AtomicPlan,
    ) -> Result<AtomicOutcome, Error>;
    pub async fn atomic_status(
        &self,
        atomic_id: AtomicId,
    ) -> Result<AtomicStatus, Error>;
}

impl AtomicBuilder {
    pub fn create<T: Serialize>(
        &mut self, collection: &Collection<T>, key: impl Into<String>, value: &T,
    ) -> Result<&mut Self, Error>;
    pub fn put_unconditional<T: Serialize>(
        &mut self, collection: &Collection<T>, key: impl Into<String>, value: &T,
    ) -> Result<&mut Self, Error>;
    pub fn replace<T: Serialize>(
        &mut self, collection: &Collection<T>, key: impl Into<String>,
        if_version: [u8; 16], value: &T,
    ) -> Result<&mut Self, Error>;
    pub fn delete<T>(
        &mut self, collection: &Collection<T>, key: impl Into<String>,
        if_version: [u8; 16],
    ) -> Result<&mut Self, Error>;
    pub fn assert_absent<T>(
        &mut self, collection: &Collection<T>, key: impl Into<String>,
    ) -> Result<&mut Self, Error>;
    pub fn assert_present<T>(
        &mut self, collection: &Collection<T>, key: impl Into<String>,
    ) -> Result<&mut Self, Error>;
    pub fn assert_version<T>(
        &mut self, collection: &Collection<T>, key: impl Into<String>,
        version: [u8; 16],
    ) -> Result<&mut Self, Error>;
    pub fn build(self) -> Result<AtomicPlan, Error>;
}
```

`AtomicOptions::new(atomic_id)` requires the stable ID; the SDK does not hide
it in an unobservable auto-generated request. `AtomicId::random()` is provided,
but the application retains it until a terminal outcome is known.

The committed receipt contains exactly:

```text
AtomicReceipt {
    atomic_id
    heap_id
    content_root
    commit_position
    durability = durable
    members[]       // canonical target order
    decision_hash
    replayed
}

AtomicMemberReceipt {
    collection_id
    key
    before_version?
    after_version?  // absent for delete
    event_id
}
```

`AtomicStatus` returns the logical and material axes from §10, the content root
when known, and the receipt when committed material/evidence permits it. It
never returns payload values by default.

`AtomicPlan` captures the `HeapId`, collection IDs, capability/authority
revision, canonical encoded values, preconditions, limits, and content root.
A plan built from collections belonging to different `HeapClient` bindings is
rejected locally and again by the kernel. One physical `Client` may own many
Heap bindings, but one Atomic belongs to exactly one of them.

Dropping the future or crossing its deadline before admission returns a request
error and guarantees that the Atomic was not accepted. Once admitted to the
commit sequencer, execution continues to a safe terminal decision. A deadline,
cancellation, transport loss, or dropped future after admission is represented
as `Ok(AtomicOutcome::Unknown { .. })` whenever an observer remains available
to receive it; the resolution carries the `AtomicId`. It is not also encoded as
a second error truth. A dropped observer resolves later with `atomic_status`.
Retrying the same canonical plan and ID is always safe.

resolves using evidence and explicit coverage.

Read-only snapshot sessions are deferred from v1.

## 16. RRE integration

At the serialization point:

1. load the exact active RRE revisions named by Heap state;
2. verify the plan's recorded revision root;
3. compute the complete affected projection;
4. evaluate every applicable invariant;
5. add derived consequences to the closed member set;
6. re-check package limits;
7. commit only when violations are empty.

There is no ordinary-write bypass.

Document-local rules use Key Atomic scope.
Reference, uniqueness, and bounded-cardinality rules require LocalHeap or
qualified Partition scope.

## 17. Relationship profile

V1 relationships support:

- required scalar reference;
- optional scalar reference;
- parent exists;
- `on delete restrict`;
- same-collection references;
- bounded sequence references when RRE declares a maximum;
- exact scalar key equality.

V1 permits relationship cycles because it has no cascade. Self-reference is
permitted only when the parent exists in the pre-state or is created in the
same Atomic and the final state satisfies the rule.

Relationship graphs need not be acyclic.

Parent/child conflicts use the canonical member ordering from §6 and
serializable validation. A concurrent parent deletion invalidates a child's
parent-exists predicate; a concurrent child insertion invalidates the parent's
no-children predicate. At most one conflicting transition commits.

The reverse-reference index is derived. It may nominate children, but absence
proves safety only when its declared coverage is complete and exact for the
Atomic frontier. Otherwise deletion refuses with `coverage_incomplete`.

## 18. Relationship activation

Activation over existing data:

1. create immutable definition;
2. install prospective enforcement barrier;
3. capture frontier;
4. scan complete parent/child scope;
5. build reverse index and report violations;
6. replay changes after frontier;
7. obtain serialization point;
8. validate no uncovered gap;
9. activate only with complete coverage and zero unaccepted violations.

Concurrent parent deletion conflicts with validation through the same
predicate/read-set mechanism. It cannot pass between scan and activation
unobserved.

Integrity status is exposed through dedicated rule/relationship inspection and
optional `read_with_integrity`. Ordinary reads do not automatically claim
relationship completeness and need not carry the full status payload.

## 19. Uniqueness

Unique values use:

- RRE canonical path;
- frozen normalization/comparison profile;
- Heap-bound exact reverse map;
- absence predicate at the Atomic frontier;
- canonical member ordering.

Null/Absent participation is declared by the rule. It is never inferred from
SQL convention.

Damage or incomplete coverage cannot prove uniqueness and causes refusal or
explicit degraded status.

## 20. Backup, restore, import, and salvage

Every data-entry mode is one of:

```text
enforce
validate_then_commit
quarantine_violations
trusted_rebuild
```

Ordinary import uses `enforce` or `validate_then_commit`.

`quarantine_violations` and `trusted_rebuild` require local `RecoveryCap` and
produce explicit evidence. They cannot publish violating records into ordinary
active state.

Payload restore to a new Heap:

- rewrites Heap identity;
- does not preserve source Atomic authority/cursors/capabilities;
- preserves historical decision/material evidence with provenance;
- revalidates active rules before ordinary service.

Salvage reports decisions, members, holes, conflicts, and coverage without
manufacturing a clean current state.

## 21. Partition profile

Partition Atomic v1 requires explicit co-partitioning of every read, predicate,
mutation, rule dependency, and generated consequence.

The partition consensus decision is authoritative for logical commit.
`Committed` acknowledgement requires:

- quorum commit of the canonical Atomic command;
- local application of the decision and members;
- requested durability evidence.

Follower material may apply later but cannot contradict the committed decision.
Cluster relationship rules remain disabled until placement and partition
qualification pass.

## 22. Error codes

Minimum stable codes:

```text
atomic_id_conflict
atomic_id_invalid
atomic_scope_escape
atomic_scope_unavailable
atomic_limit_exceeded
atomic_deadline_exceeded
atomic_read_conflict
atomic_predicate_conflict
atomic_rule_changed
atomic_rule_violation
atomic_not_committed
atomic_outcome_unknown
atomic_evidence_conflicting
atomic_coverage_incomplete
atomic_material_partial
atomic_right_denied
relationship_parent_missing
relationship_children_exist
relationship_degraded
unique_value_exists
```

Errors reveal nothing outside the caller's Heap and collection constraints.

## 23. Conformance

V1 requires:

- canonical encoding corpus;
- ID/content-root retry corpus;
- serial history model check;
- write-skew and phantom tests;
- crash at every §11 failpoint;
- two-Heap noninterference;
- rights matrix;
- RRE enforcement;
- parent insert/update/delete races;
- relationship activation with concurrent mutation;
- unique contention;
- damage to prepare/member/decision/index;
- backup/restore/salvage;
- remote timeout/reconnect/status;
- limit and hostile-plan corpus.

No capability is advertised beyond the scopes that pass.

The Gremlin acceptance journey is mandatory, not illustrative: conditional
replacement of the authoritative conversation state plus creation of the turn
record and turn-id locator must commit together, survive restart, replay by
the same `AtomicId`, reject a stale state version with no projection writes,
and remain isolated from a second authorized Heap sharing the same physical
`Client`.

## 24. Closed decisions

This specification resolves every open question from
`ATOMICS_PROPOSAL.md` §31:

1. IDs/evidence use §5 and §9 canonical profiles.
2. Limits are fixed by §13.
3. Retention is fixed by §12.
4. Heap commit positions and predicate witnesses are fixed by §7–§8.
5. Partition decision/material durability is fixed by §21.
6. Read-only snapshots are deferred.
7. Required and optional scalar relationships are included.
8. Paths and scalar key profiles are fixed by §6.
9. Same-collection references are allowed.
10. Acyclic graphs are not required without cascade.
11. Conflict ordering is fixed by §6.
12. Clustered v1 requires explicit co-partitioning.
13. Validation concurrency is fixed by §18.
14. Evidence retention is fixed by §12.
15. Integrity status uses dedicated/optional read surfaces.
16. Administration and recovery authority are fixed by §14.

An implementer has no remaining semantic choice in this list. Exact Rust names,
async-only submission, LocalHeap durable acknowledgement, ordinary-write
participation, publication generation, and the local durable protocol are also
closed by §§8--15.