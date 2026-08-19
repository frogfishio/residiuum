# ATM-1 / ATM-2 deep acceptance review — ATMR5

Date: 2026-08-19

Review baseline: clean `00a06aed6deab5608ad5ebf92fe35e89e73db4be`
(`origin/main` at review start)

Normative authority:

- `doc/todo/atomics/ATOMICS_SPEC.md`;
- `doc/todo/atomics/ATOMICS_IMPLEMENTATION_PLAN.md`;
- `spec/atomics/cbor-v1.json`; and
- `doc/reference/storage/FORMAT_SPEC.md`.

This is a new review round. Its active findings use only the independent
`CR-ATMR5-*` namespace. ATMR4 identifiers remain historical evidence and must
not be reused for this repair pass.

## Acceptance decision

### ATM-1

**Technically accepted at `00a06ae` for the ATM-1 compiler/validation scope.**

The ATMR4 exact-closure defect is closed: the plan-to-prepare conversion now
requires a one-to-one set of plan mutations and members, refuses duplicate
targets, and refuses leftovers. The canonical plan, oracle, authority,
encoding, hostile-input, and resource-limit suites remain green. The clean
full verifier records ATM-1 as `acceptance_candidate` with no ATM-1 blockers.

This acceptance does not advertise Atomics, accept store durability, or approve
ATM-2. The envelope namespace amendment should still be recorded in
`FORMAT_SPEC.md`, but it does not invalidate the accepted ATM-1 semantic core.

### ATM-2

**Not accepted.**

The peer lane has improved substantially and several ATMR4 defects are genuinely
closed there. The new store path also removes `DurableLane` as a second storage
authority. However, the replacement is not yet an authoritative recovery
protocol:

- it recursively full-scans and loads every store segment whenever the stage
  opens and repeatedly during operations;
- it silently discards damaged evidence and collapses conflicting records;
- it loses partial chunks and coordinator order on stage reopen;
- exact unchunked retry fails;
- its seal and prepare side-record protocols are not identity-complete; and
- the green verifier does not test those store contracts.

`Capabilities::atomics` must remain `false`. ATM-3 must not build durable
decisions or publication on the current `StoreAtomicStage` catalogue.

## Verification performed

The following passed at the clean baseline:

- `bash scripts/verify-atomics.sh full`;
- all scoped Atomics, format, and peer-lane targets in that verifier;
- store legacy envelope migration tests;
- the current store `atomic_stage_invisibility` integration test;
- formatting and Clippy with warnings denied for Atomics, format, and peer
  lane; and
- detached run-record hash generation.

The full run correctly recorded:

- overall result `pass`;
- ATM-1 `acceptance_candidate`;
- ATM-2 `partial`;
- `not_store = true`; and
- `Capabilities::atomics == false`.

Three temporary store probes were added, run, and removed:

1. repeating the exact same unchunked `append_staged` returned
   `DuplicateTarget` rather than idempotent success;
2. after persisting the first of two chunks, dropping and reopening only the
   staging handle made the second chunk fail `MalformedInput`; and
3. preparing Atomic ID 2 and then ID 1 assigned sequences `(1, 2)`, but after
   store reopen the same IDs reported `(2, 1)`.

All three probes reproduced on `00a06ae`. No probe code remains in the tree.

An additional reviewer command,
`cargo clippy -p residiuum-store --offline --all-targets --no-deps -- -D warnings`,
failed on the wider store crate. Most reported warnings predate this delivery,
so they are not individually raised as Atomics defects. The relevant evidence
gap is that the authoritative store implementation is excluded from the full
Atomics formatting/Clippy package gate.

## ATMR4 repair-theme status

This table is informational. Only ATMR5 identifiers below are active.

| ATMR4 theme | ATMR5 result |
| --- | --- |
| Exact closed member set | Closed. Extra, missing, duplicate, and mismatched members are covered. |
| Authenticated checkpoint / forged seal | Original forged-seal defect closed in the peer lane. |
| Genuine checkpoint tails | Not closed: prefix hashes reread the full historical prefix outside recovery accounting. |
| Chunk member authority | Closed in the peer lane; not delivered on the store path for partial chunks/manifest commitment. |
| One store-owned authority | Dual lane removed, but the replacement raw-segment catalogue is not safe or bounded. |
| Exclusive publication | Direct-final short-write poison closed; prefix-based torn classification can overwrite a different identity. |
| Sidecar allocation limits | Closed for peer-lane recovery; store catalogue still reads complete segments without an Atomics budget. |
| Deterministic I/O matrix | Parallel race closed; store authority and most chunk phases are absent, and mutants remain weak. |
| Visibility/examination | Narrow store test improved; RQL/watch/examine/recovery-shadow/backup remain open. |
| Clean package gate | Green for its declared commands; declared commands are insufficient for the new store authority. |

## New change requests

### CR-ATMR5-001 — RED — Store Atomic recovery is an unbounded repeated full-database scan

Evidence:

- `Store::atomic_stage` calls `scan_stage_catalog` on every staging-handle open.
- `begin_prepare`, `append_staged`, `append_chunk`, and
  `seal_member_boundary` call it again during normal operations.
- `segment_files` recursively walks active, sealed-segment, and pending-seal
  trees without entry, depth, file-count, or byte ceilings.
- `scan_stage_catalog` calls `fs::read` for every discovered file, allocating
  the entire file before frame scanning.
- It retains every prepare, member, payload body, and seal in `BTreeMap` /
  `BTreeSet` collections. There is no checkpoint, frontier, tail index,
  work-memory budget, or OpenReport.
- Large payloads are cloned again while rebuilding `StagingHeap`.

Impact:

Opening or using Atomics becomes proportional to the complete physical store,
not the Atomic tail. A large or hostile store can force unbounded allocation
and repeated I/O. This recreates the startup pathology that the database's
normal index/recovery work was designed to eliminate and violates the explicit
RED rule for unbounded recovery.

Required fix:

1. Integrate Atomic status/evidence into a store-owned bounded catalogue or
   authenticated checkpoint maintained by the store writer.
2. Recover from that frontier plus bounded segment tails.
3. Stream frame reads; never `fs::read` an unbounded segment.
4. Apply aggregate ceilings to bytes, frames, Atomics, members, payloads,
   directory entries, depth, and retained work memory.
5. Expose normal/checkpoint/rebuild disposition and measured phase costs in the
   store OpenReport.
6. Do not rescan the database for every Atomic method call.

Acceptance proof:

- fixed-size Atomic tails have approximately constant open cost as historical
  store size grows from MiB to GiB;
- oversized/sparse segments and directory populations refuse or enter an
  explicit bounded rebuild before proportional allocation;
- per-operation staging does not rescan settled history; and
- reported bytes and objects reconcile all recovery work.

### CR-ATMR5-002 — RED — The store catalogue converts damage, conflicts, and foreign evidence into guessed truth

Evidence:

- `scan_forward` holes and corrupt/partial/unsupported Atomic evidence are
  ignored; only verified frames are considered.
- malformed `ATPREP1`, `ATPAY1`, and `ATSEAL1` bodies return `None` and silently
  disappear.
- duplicate prepares and payloads use `BTreeMap::insert`, so whichever file is
  visited last silently wins; filesystem traversal order is not canonical.
- duplicate member ordinals keep the first record and silently ignore a
  different later record.
- `rebuild_heap` silently `continue`s when members do not match a prepare,
  converting damaged accepted evidence into an absent Atomic.
- recovered prepares are not checked against the current store/Heap ID before
  being installed in the local `StagingHeap`.
- `decode_stage_seal` discards the encoded content root and returns only the
  Atomic ID. The catalogue therefore accepts a seal whose root does not match
  the prepare, and the seal record commits neither manifest root nor member
  count.

Impact:

The recovery result depends on directory iteration order and can silently
erase, substitute, or cross-bind evidence. A missing/damaged prepare may make
an accepted ID appear unused; a mismatched seal can invent the first stable
member boundary. These are guessed recovery outcomes and Heap-isolation
violations.

Required fix:

1. Use the format admission path bound to the current Heap, not generic
   `examine_atomic_frame` alone.
2. Preserve and report all physical coverage holes and every unsupported,
   partial, corrupt, duplicate, and conflicting record.
3. Aggregate by `(heap_id, atomic_id, content_root)` with deterministic conflict
   classification; never overwrite or first-win silently.
4. Validate store side-record bodies with version, Heap ID, Atomic ID, content
   root, prepare/member/manifest hash, count, and role.
5. Make store recovery and independent examination share the same classifier.
6. Never translate damaged accepted evidence into absence or reusable identity.

Acceptance proof:

- pairwise conflicting prepare/member/payload/seal fixtures always produce the
  same result regardless of file/segment order;
- every one-byte mutation and truncation is corrupt/incomplete, not absent;
- foreign-Heap evidence cannot be resolved locally; and
- mismatched seal root, manifest, count, or member durability cannot seal.

### CR-ATMR5-003 — RED — Store staging violates exact same-ID retry semantics

Evidence:

- `StoreAtomicStage::append_staged` calls
  `StagingHeap::check_append_staged` before testing whether the same member and
  payload are already staged.
- the kernel check returns `DuplicateTarget` whenever that ordinal already
  exists, including an exact retry.
- The temporary probe confirmed that a second byte-identical
  `append_staged(member, payload)` fails.
- The completed-chunk path has the opposite problem: once `already` is true,
  `append_chunk` skips member, index, chunk-body, and hash validation. A
  conflicting retry can return success merely because some payload is already
  complete.

Impact:

The unchunked path rejects legitimate transport/client replay, while the
chunked path can acknowledge a request it did not validate. This makes retry
behavior dependent on transport form and breaks the lifetime Atomic identity
contract.

Required fix:

1. Perform an exact existing-state comparison before duplicate refusal.
2. Exact member/payload/chunk retries return the original successful result
   without additional media.
3. Any changed member, ordinal, index, body, chunk plan, payload, or content
   root returns the correct identity/duplicate conflict.
4. Apply the same logic before and after process restart.

Acceptance proof:

- a retry matrix covers every field for unchunked and chunked members;
- exact retry is byte-idempotent in-process and after reopen;
- every one-field mutation refuses; and
- receipts/status are identical across transport forms.

### CR-ATMR5-004 — RED — Coordinator sequence/order is reconstructed from Atomic ID sort order

Evidence:

- `StageCatalog.prepares` is a `BTreeMap<AtomicId, AtomicPrepare>`.
- `rebuild_heap` iterates that map and calls `StagingHeap::begin_prepare`, which
  allocates a fresh `CoordinatorSeq` in iteration order.
- no durable store-stage record retains the issued coordinator sequence or a
  canonical coordinator log position.
- The temporary probe prepared ID 2 then ID 1. Before restart their sequences
  were `(1, 2)`; after restart they became `(2, 1)`.

Impact:

The designated per-Heap coordinator stream is not durable. Retry receipts,
serialization order, future decision ordering, frontier witnesses, and recovery
can disagree before and after restart.

Required fix:

1. Allocate coordinator sequence under the store writer before durable prepare
   acceptance.
2. Persist that sequence in the authoritative prepare/coordinator record.
3. Rebuild exact order and high-water state from the checkpoint plus tails.
4. Refuse duplicate sequence ownership and sequence regression.

Acceptance proof:

- arbitrary Atomic-ID insertion orders retain identical sequences across
  reopen, rotation, compaction, backup/restore, and checkpoint rebuild;
- same-ID retry returns the original sequence; and
- the next allocated sequence is strictly above every durable issued sequence.

### CR-ATMR5-005 — RED — Partial store chunks and their frozen manifest are not durable

Evidence:

- `StoreAtomicStage::commit_chunk_manifest` explicitly records the `ChunkPlan`
  in the in-memory `StagingHeap` only.
- `append_chunk` persists nothing until the complete assembled payload exists.
- dropping and reopening only the staging handle loses both the manifest and
  all partial chunk bodies.
- The temporary probe persisted chunk 0, reopened the stage, and then chunk 1
  failed `MalformedInput`.
- A completed store chunk operation is represented only as the assembled
  `ATPAY1` payload. Chunk count/order/hashes/profile are absent from store
  authority.
- Member frames are currently written during `begin_prepare`, before chunk
  bodies exist, unlike the peer lane's corrected complete-member boundary.

Impact:

The store path does not provide durable chunked-value staging or complete
manifest commitment. A process interruption discards acknowledged chunk work,
and independent evidence cannot prove which frozen chunk manifest produced the
payload.

Required fix:

1. Persist a versioned chunk manifest committed by the prepare/member evidence.
2. Persist each verified chunk under store authority before acknowledging it.
3. Recover partial chunk state and accept exact continuation/retry after reopen.
4. Emit authoritative member evidence only at the protocol point defined for
   complete member durability, or explicitly define and test a separate
   metadata-before-payload state.
5. Make store and peer-lane oracle outcomes identical at every chunk prefix.

Acceptance proof:

- interruption after every chunk retains the exact prefix;
- continuation and exact retry succeed after reopen;
- changed manifest/body/order/count refuses;
- seal requires complete committed chunks and authoritative member evidence;
  and
- independent examiner reconstructs the same partial/complete state.

### CR-ATMR5-006 — RED — Prepare authority is split across two non-atomic store records

Evidence:

- `persist_prepare` first appends a canonical `BatchPrepare`, then appends a
  second `ATPREP1` copy inside a generic `PayloadChunk` frame.
- the comment states that the second copy is required because store open/rebuild
  may not retain `BatchPrepare` as the reopen authority.
- a crash or I/O error between the two durable appends leaves only
  `BatchPrepare`.
- retry scans that first record into `catalog.prepares`, concludes the prepare
  already exists, and does not call `persist_prepare`; the required `ATPREP1`
  copy is therefore never repaired.
- the catalogue merges both sources into one map and cannot report which
  required representation is missing or conflicting.

Impact:

A successfully recoverable prepare can later disappear through rotation or
compaction because the representation intended to survive was never written.
The two records are independently durable but have no joined completion marker
or recovery protocol.

Required fix:

1. Use one authoritative versioned prepare representation that the store's
   normal rotation/compaction/recovery preserves.
2. If a migration mirror is temporarily unavoidable, represent its completion
   explicitly and make retry/recovery repair every incomplete prefix.
3. Do not encode private Atomic authority by masquerading as an unrelated
   generic `PayloadChunk` without a frozen format/admission contract.
4. Add I/O failpoints between every store append and maintenance transition.

Acceptance proof:

- every crash prefix during prepare has one durable/retryable outcome;
- rotation and compaction preserve exactly one authoritative prepare;
- missing/conflicting mirrors are detected and repaired or degraded; and
- same-ID/different-root can never exploit a missing representation.

### CR-ATMR5-007 — RED — Exclusive retry guesses that any strict prefix is torn and overwrites it

Evidence:

- `classify_existing` labels an existing final file `Torn` whenever it is empty
  or a strict byte prefix of the intended bytes.
- no checksum, authenticated incomplete marker, prior length, operation journal,
  or acknowledged publish state proves that the file is a torn write.
- `write_exclusive` quarantines that final file and publishes the intended one.
- a different valid short payload can naturally be a prefix of a longer
  payload. Corruption can also turn durable evidence into such a prefix.
- `classify_existing` itself uses unbounded `fs::read` before checking the
  existing file against the role ceiling.
- The new temp-file protocol never publishes a partial destination, so broad
  prefix healing is primarily compensating for legacy/direct-final files
  without proving their provenance.

Impact:

Retry can destroy or hide conflicting/damaged evidence and replace it with the
caller's desired bytes. This changes an honest conflict/damage result into
success and undermines same-ID evidence preservation.

Required fix:

1. New writes must rely only on the temp/no-replace protocol; a partial temp is
   never final authority.
2. Legacy torn finals require an authenticated migration/recovery classifier,
   not byte-prefix inference.
3. Bound existing-file size before allocation.
4. Preserve conflicting/damaged bytes for examination and return a typed
   damage/conflict result unless recovery evidence proves safe completion.

Acceptance proof:

- valid shorter-prefix identities are never overwritten;
- every corrupt/truncated legacy final has an explicit classification;
- exact retry after each new-protocol prefix succeeds without final-path
  guessing; and
- oversized existing files refuse before proportional allocation.

### CR-ATMR5-008 — AMBER — Checkpoint prefix authentication still rereads all history and hides the work

Evidence:

- `verify_checkpoint_prefixes` calls `prefix_digest` from byte zero to every
  covered coordinator/shard offset on every open.
- `persist_recovery_checkpoint` recomputes the same complete prefix hashes after
  each prepare/member/chunk/seal checkpoint write.
- `prefix_digest` streams in 8 KiB buffers, so memory is bounded, but I/O and CPU
  remain proportional to historical log size and repeated writes become
  quadratic over growth.
- prefix hashing is not charged to `RecoveryBudget` or
  `RecoveryStats.bytes_scanned`.
- `covered_prefix_larger_than_budget_opens_from_tails` actually reads the large
  covered prefix to verify its hash, then asserts only that the uncharged
  `bytes_scanned` counter is small.

Impact:

The implementation avoids a large allocation but does not deliver snappy tail
recovery. Metrics claim bounded scanning while substantial historical I/O is
performed outside accounting.

Required fix:

1. Maintain an incremental authenticated accumulator/Merkle frontier or reuse
   store segment hashes so prefix trust does not require rereading bytes.
2. Persist/check accumulator state at segment/checkpoint boundaries.
3. Charge every verification byte and CPU phase honestly in OpenReport.
4. Avoid rewriting and rehashing all historical Atomic summaries after each
   operation.

Acceptance proof:

- fixed-tail open and checkpoint cost stay approximately constant as covered
  history grows;
- total physical reads measured externally agree with reported verification
  bytes; and
- any changed covered byte is still detected through the retained accumulator
  hierarchy.

### CR-ATMR5-009 — AMBER — Fault-injection proof does not cover the authoritative store protocol

Evidence:

- the deterministic serialization guard closes the ATMR4 parallel-state race.
- the main peer-lane matrix declares `Scenario::Chunk`, but does not add chunk
  cases to its generated case list; only one `Chunk/BeforeWrite` sentinel runs.
- omit-file-sync and omit-dir-sync mutants still verify missing instrumentation
  visits rather than simulating loss of unsynced media.
- the new store `BatchPrepare`/`ATPREP1`/member/`ATPAY1`/`ATSEAL1` protocol has
  no phase-indexed I/O failpoints or directory-image crash matrix.
- store rotation, compaction, pending seal, and recovery-shadow transitions are
  absent.

Impact:

The strongest fault suite tests the peer oracle, not the storage authority that
ATM-3 would consume. Store ordering bugs can pass a completely green verifier.

Required fix:

1. Add store-authority failpoints at every write, sync, rotation, seal,
   compaction, checkpoint, and publish boundary.
2. Generate all chunk cells rather than a single sentinel.
3. Use a crash-media/directory-image model that removes unsynced writes and
   proves mutants are semantically detected.
4. Require exact operation result and exact reopen class for every cell.

Acceptance proof:

- each required store durability edge is killed by at least one mutant;
- all prepare/member/chunk/seal prefixes have reviewed expected outcomes;
- repeated default-parallel runs are deterministic; and
- no staged material reaches any ordinary surface.

### CR-ATMR5-010 — AMBER — The green verifier and handoff overstate store coverage

Evidence:

- the full verifier runs one store integration test and two legacy envelope
  unit tests; it does not run store all-targets, store formatting/Clippy as an
  Atomics package, store crash prefixes, or bounded store recovery tests.
- ATM-2 manifest has only `not_store=true` as a blocker even though exact retry,
  partial chunks, coordinator sequence, recovery honesty, and boundedness are
  failing.
- the handoff claims checkpoint tail opening without disclosing the full-prefix
  hash read, and claims durable/store chunk improvements without distinguishing
  the store's in-memory-only partial chunk path.
- RQL, watch, `residiuum-examine`, Recovery Shadow, backup/restore/clone, and
  full rotation/cohort isolation are acknowledged residuals.

Impact:

The clean green result is valid for the commands run, but can be misread as
evidence that the current store staging contract is nearly acceptable. It does
not detect three independently reproduced contract failures.

Required fix:

1. Add each ATMR5 acceptance proof to the mandatory ATM-2 matrix.
2. Give the store-owned staging code its own scoped formatting, Clippy, unit,
   integration, crash, hostile-media, and performance gates.
3. Derive manifest blockers from missing/failing required families, not only
   `not_store=true`.
4. Regenerate the handoff with exact distinctions between peer-lane proof,
   store proof, implemented behavior, and residual work.

Acceptance proof:

- deliberately reintroducing each ATMR5 defect makes the verifier fail;
- ATM-2 manifest lists all unresolved mandatory deliverables;
- handoff claims match measured physical work and tested surfaces; and
- all required store families pass at one clean commit.

## Architecture dispositions

### ATM-1

Record ATM-1 as technically accepted at `00a06ae`. Future changes to canonical
plan semantics, authority, encoding, or resource limits reopen ATM-1 review.
This does not enable the public capability.

### Store authority

The decision to remove the peer lane from `StoreAtomicStage` is correct. Do not
reintroduce dual-write. Replace the raw recursive catalogue with a genuine
store-owned coordinator/evidence index integrated into segment rotation,
compaction, recovery, and examination.

### Peer lane

Keep `residiuum-atomic-lane` as a mechanics and crash-model oracle. Its recent
checkpoint, chunk-member, exclusive-temp, limits, and deterministic-test work
is useful, but peer-lane acceptance cannot substitute for store proof.

### Capability

Keep `Capabilities::atomics == false` until ATM-5 acceptance.

## Required delivery order

1. Define the one authoritative store record/catalogue and honest recovery
   classifier under CR-ATMR5-001/002/006.
2. Persist coordinator identity/order under CR-ATMR5-004.
3. Close exact retry and durable chunk prefixes under CR-ATMR5-003/005.
4. Remove guessed prefix healing under CR-ATMR5-007.
5. Replace historical prefix rehashing with an incremental authenticated
   frontier under CR-ATMR5-008.
6. Prove the authoritative store path with CR-ATMR5-009.
7. Expand and regenerate package evidence/handoff under CR-ATMR5-010.

## ATM-2 acceptance gate

ATM-2 may be accepted only when:

- CR-ATMR5-001 through CR-ATMR5-010 are closed or explicitly dispositioned by
  architecture without weakening correctness;
- one store-owned coordinator/evidence authority survives restart, rotation,
  compaction, recovery, backup, and examination;
- normal open and operation cost are bounded by checkpoint plus tails rather
  than complete store history;
- damage/conflicts never become absence, last-wins, or first-wins;
- coordinator sequence and exact retry are stable for the Heap lifetime;
- partial and complete chunks share the frozen manifest and survive every
  crash prefix;
- every ordinary visibility surface remains empty before decision; and
- a clean package-specific verifier and current handoff prove the complete
  store contract.
