# ATM-1 / ATM-2 deep acceptance review — ATMR6

Date: 2026-08-20

Review baseline: clean `7f3db25ffda83832b46d3adf1ff5a1539cc93f6c`
(`origin/main` at review start)

Compared with: `00a06aed6deab5608ad5ebf92fe35e89e73db4be`
(ATMR5 review baseline)

Normative authority:

- `doc/todo/atomics/ATOMICS_SPEC.md`;
- `doc/todo/atomics/ATOMICS_IMPLEMENTATION_PLAN.md`;
- `spec/atomics/cbor-v1.json`; and
- `doc/reference/storage/FORMAT_SPEC.md`.

This is an independent review round. Active changes below use only the new
`CR-ATMR6-*` namespace. Earlier identifiers remain historical evidence and
must not be reused for this repair pass.

## Acceptance decision

### ATM-1

**The semantic ATM-1 acceptance at `00a06ae` is not revoked.**

No reviewed change reopens the canonical plan, exact member closure, oracle,
authority, encoding, or resource-limit semantics accepted in ATMR5. However,
the current full package run at `7f3db25` is diagnostic rather than an accepted
manifest because shared peer-lane damage tests, formatting, and Clippy fail.
The delivery therefore cannot claim a new all-green ATM-1/ATM-2 package
baseline yet.

### ATM-2

**Not accepted. `Capabilities::atomics` must remain `false`, and ATM-3 must not
consume either store staging or the peer lane as accepted durability.**

The delivery is a substantial improvement, not a failed rewrite. It closes the
basic store retry/chunk/prepare problems identified in ATMR5. The remaining
RED defects are narrower but fundamental:

- covered-prefix checkpoints can silently miss interior corruption;
- store damage, orphan, and conflict evidence is still forgotten or reused;
- the seal API changes the live state before its durable append;
- normal valid store growth can exceed hard-coded prototype limits and make
  staging unreopenable;
- a durable prepare without all members is projected as absence; and
- existing compaction can retire the only authoritative segment copies of
  private Atomic records.

The full acceptance verifier is also red at this baseline.

## What was reviewed

The review covered the ten commits after the ATMR5 baseline, including:

- bounded store catalogue/checkpoint recovery;
- store evidence classification and findings;
- durable coordinator sequencing;
- exact retry and prepared-state installation;
- durable chunk plans and partial chunk bodies;
- exclusive sidecar conflict handling;
- peer-lane incremental prefix marks;
- the store I/O prefix matrix; and
- the regenerated verifier/handoff.

The full verifier was run from the clean baseline:

```text
bash scripts/verify-atomics.sh full
```

It wrote
`target/atomics-evidence/runs/7f3db25ffda8-full.json` and returned exit code 1:

- result: `fail`;
- acceptance: `diagnostic`;
- `ATM-CRS`: fail;
- `ATM-ENC`: fail;
- three `honest_damage` tests failed;
- peer-lane formatting failed; and
- peer-lane Clippy with warnings denied failed.

All newly listed store integration targets passed, including retry, chunks,
classification, coordinator, bounded recovery, prepare authority, I/O matrix,
and store invisibility. Those greens are useful evidence, but the tests do not
yet exercise the defects below.

## ATMR5 closure map

This table is informational. Only ATMR6 identifiers are active.

| ATMR5 item | ATMR6 result |
| --- | --- |
| 001 bounded store catalogue | Partly closed. Per-operation full scans are gone, but whole-file size ceilings, full-state checkpoint rewrites, and weak covered-prefix trust are not an operable bounded design. |
| 002 honest damage classifier | Partly closed. Conflicts are now represented, but seal conflicts, orphan records, unbound holes, and checkpointed findings can still become forgotten/reusable evidence. |
| 003 exact same-ID retry | Closed for the reviewed unchunked and chunked store paths. |
| 004 durable coordinator sequence | Closed for ordinary reopen and opposite-ID order. No new ATMR6 request is raised for the normal sequence path. |
| 005 durable partial chunks | Closed for the reviewed store-handle reopen path. Maintenance survival remains under ATMR6-006. |
| 006 one prepare authority | Closed for new writes: `BatchPrepare` is authoritative and legacy `ATPREP1` is repair-only. |
| 007 no prefix-guess overwrite | Closed. Existing nonidentical finals are preserved as conflict/damage and size-checked before read. |
| 008 incremental checkpoint frontier | Not closed. Incremental hashes are built, but normal open verifies only the first and last 32 bytes; the existing damage tests catch the regression. |
| 009 store I/O matrix | Partly closed. Store cases exist, but the matrix omits the actual member-frame operation and blesses durable prepare prefixes as absence. |
| 010 honest verifier/handoff | Not closed. The full verifier fails and its static residual list contradicts commands/tests now present. |

## New change requests

### CR-ATMR6-001 — RED — Covered-prefix recovery is fast because it does not verify the covered prefix

Evidence:

- Peer-lane `PrefixMarks` stores per-64-KiB block hashes, but
  `recover::verify_checkpoint_prefixes` calls only `verify_prefix_marks`.
- `verify_prefix_marks` reads file length, the first 32 bytes, and the last 32
  bytes. `verify_prefix_blocks`, which detects an arbitrary changed covered
  byte, is dead code in production recovery.
- The full verifier reproduces the consequence. The sole-prepare mutation test
  and exhaustive prepare/member mutation tests accept an interior byte flip at
  offset 32 as if the acknowledged log were intact.
- Store recovery uses the same weaker rule: a same-sized file is skipped when
  only its first and last 32 bytes equal the checkpoint fingerprint. Interior
  frames are never read or verified.
- The store also permits a path rename match using size plus those two 32-byte
  samples, so an unrelated same-size file can inherit another file's coverage.

Impact:

Acknowledged Atomic evidence can be changed inside a covered prefix while
recovery applies the old checkpoint facts and reports a healthy fast path. This
is guessed material truth and directly contradicts the byte-damage proof.

Required fix:

1. Until a stronger store-owned immutable-media trust model is approved, use
   the stored block frontier to verify every covered block and charge all bytes
   honestly.
2. If constant-I/O open is required, submit an architecture/spec amendment
   defining which durable object is authoritative, how immutable segment
   identity is established, and when arbitrary offline media damage becomes
   `coverage_incomplete` rather than silently healthy.
3. Do not use head/tail samples to transfer coverage across path changes.
4. Apply the same trust rule to store and peer-lane recovery; do not keep one
   strict test oracle and a weaker production verifier.

Acceptance proof:

- every byte flip and truncation in each covered prepare/member/chunk/seal log
  is detected or explicitly reported as incomplete coverage;
- the exhaustive existing `honest_damage` target is green;
- store tests mutate interior bytes while preserving length/head/tail and must
  not return a healthy checkpoint disposition; and
- reported verification I/O reconciles actual bytes read.

### CR-ATMR6-002 — RED — Store classification still forgets damage and permits impossible orphan evidence to attach later

Evidence:

- A conflicting seal removes `catalog.seals[atomic_id]` but does not add the ID
  to `catalog.blocked`.
- A seal whose root disagrees with the prepare is likewise removed during
  `finalize_catalog` without blocking the identity.
- Members, payloads, chunk plans, chunk bodies, and seals are admitted before a
  prepare exists. Recovery does not classify those impossible write-order
  orphans as blocking evidence. A later prepare with the same ID can attach to
  the retained orphan data.
- Physical holes and partial sidecars without a decodable ID are recorded only
  in the transient `StageFindings`. The checkpoint persists catalogue maps and
  blocked IDs, but not these findings or a global degraded-coverage state.
- On the next open the file is skipped as covered, so the same hole/partial
  record disappears from `findings`, report counts return to zero, and new IDs
  remain admissible.
- `open_catalog` retains `unmatched` checkpoint files after media discovery but
  never treats a missing covered file as damage or incomplete coverage.

Impact:

Durable contradictory or damaged evidence can be converted into absence, then
repaired according to a later caller's desired bytes. Recovery outcome changes
across consecutive opens even though media did not improve.

Required fix:

1. A conflicting or mismatched seal must block the Atomic identity; never drop
   the conflict and continue.
2. Classify side records that cannot legally precede prepare/member authority
   as orphan damage and block the named identity.
3. Persist identity-bound findings and a store/coverage degradation state for
   unbound holes or undecodable evidence.
4. Missing or replaced covered media must invalidate coverage, not be ignored.
5. Define the explicit repair/scrub ceremony that can clear degradation; an
   ordinary reopen or retry must not clear it.

Acceptance proof:

- conflicting/mismatched seals remain blocked across repeated opens;
- each orphan record role is tested before prepare, followed by a reuse attempt;
- holes and undecodable partial records remain visible after two checkpoint
  reopens; and
- deleting or replacing a covered file produces degraded/incomplete coverage.

### CR-ATMR6-003 — RED — Seal changes live lifecycle before the durable seal exists

Evidence:

- `StoreAtomicStage::seal_member_boundary` first calls the in-memory
  `StagingHeap::seal_member_boundary`.
- Only afterwards does it call `persist_seal`.
- The `store.atomic.seal.before_append` failpoint can therefore return an error
  while the same live handle already reports `MemberPhase::DurableInvisible`.
- The method comment claims the opposite order: persist the store seal, then
  apply the model.
- Current I/O tests drop and reopen the store before classification, so they do
  not inspect the poisoned live handle.

Impact:

A failed pre-write call can make nondurable material appear to have crossed the
first stable boundary. Any later ATM-3 logic using that handle could decide from
a durability fact that exists only in memory.

Required fix:

1. Validate seal readiness without mutating the kernel.
2. Append and durably acknowledge the authoritative seal.
3. Apply `DurableInvisible` to the kernel only after persistence succeeds.
4. Make exact retry repair the two legitimate states: durable seal/not applied,
   and seal already applied.

Acceptance proof:

- every failure before the durable append leaves the same live handle staged,
  not durable;
- every failure after durable append recovers/retries to durable without a
  second conflicting record; and
- tests inspect both the live handle and a fresh process/reopen outcome.

### CR-ATMR6-004 — RED — Prototype limits and full-state checkpoint rewrites make valid stores eventually unreopenable

Evidence:

- `Store::atomic_stage` always uses the non-configurable
  `AtomicStageLimits::prototype()`.
- The prototype caps `max_segment_bytes`, aggregate scan, retained work,
  payloads, and the checkpoint at 16 MiB.
- `open_catalog` rejects any media file whose total length exceeds 16 MiB
  before checking whether the checkpoint already covers it. The normal store
  segment-growth defaults are 64 MiB.
- The Atomic protocol permits up to 8 MiB proposed value bytes per LocalHeap
  Atomic and up to 4096 generated members. A small number of legitimate
  outstanding Atomics can exceed the cumulative 16-MiB catalogue/checkpoint
  limit.
- The checkpoint serializes all prepares, members, complete payloads, chunk
  plans, and every partial chunk body. It is rewritten after every stage
  append, producing full-history write amplification rather than incremental
  catalogue maintenance.
- Once a durable append grows active media or catalogue state beyond a limit,
  the API can return an error after the append and subsequent opens can refuse
  the store indefinitely.

Impact:

This is bounded in the narrow allocation sense but not an operable bounded
recovery design. Normal valid use can strand already accepted staging evidence
behind an unconditional prototype ceiling.

Required fix:

1. Bound bytes actually scanned/tail-read, not the total length of a covered
   immutable segment.
2. Derive configured cumulative staging limits from the frozen per-Atomic
   ceilings, concurrency/admission policy, and reclaim policy; expose them in
   configuration and OpenReport.
3. Replace full catalogue/payload checkpoint rewrites with an incremental
   store-owned index/frontier whose checkpoint contains summaries and locators,
   not duplicate payload history.
4. Refuse admission before durable append when capacity is unavailable; never
   discover an ordinary capacity limit only after accepting bytes.
5. Provide bounded rebuild/degraded behavior for large historical stores.

Acceptance proof:

- a covered 64-MiB-or-larger segment plus a small dirty tail opens normally;
- multiple maximum-size valid Atomics remain recoverable within declared
  admission capacity;
- one-unit-over aggregate capacity refuses before append;
- checkpoint bytes written per small append do not grow linearly with all prior
  staged payload bytes; and
- an injected checkpoint-capacity error cannot strand acknowledged evidence.

### CR-ATMR6-005 — RED — A surviving durable prepare is publicly classified as absence

Evidence:

- `rebuild_heap` installs a prepare only when all intended members already
  match. A legitimate crash after durable prepare but before all members leaves
  no kernel placement/lifecycle.
- The authoritative `StageCatalog` is private. The public stage exposes only
  the reconstructed kernel, aggregate OpenReport, and transient damage
  findings; it has no prepared/partial evidence projection.
- The store I/O matrix classifies `lifecycle == None` as `Outcome::Absence`.
- For `Scenario::Prepare`, that matrix permits `Absence` at every phase,
  including `AfterAppend` and `AfterCheckpoint`.
- The matrix therefore passes while calling a valid surviving durable prepare
  absent, contrary to the ATM-2 exit gate requiring accurate examination of
  surviving prepare/material.

Impact:

The internal catalogue may prevent immediate ID reuse, but the only exposed
recovery view says no Atomic exists. ATM-3 status built on this surface would
be capable of returning `NotFound` where the spec requires issued/degraded
evidence and eventual deterministic resolution.

Required fix:

1. Expose one store-authoritative, typed examination/status projection for
   valid prepare, intended members, present members/payload/chunks, seal state,
   conflicts, and coverage.
2. Distinguish `no valid prepare` from `valid prepare + incomplete members`.
3. Make checkpoint and rebuild produce the same projection.
4. Correct the I/O matrix vocabulary and expected states; `AfterAppend` cannot
   be called absence when a valid durable prepare survives.

Acceptance proof:

- interruption after prepare and after each member reports exact surviving
  evidence before and after checkpoint reopen;
- no valid prepare is the only path to true absence;
- an incomplete accepted ID cannot be reused; and
- independent byte examination agrees with store recovery.

### CR-ATMR6-006 — RED — Private Atomic records are not integrated with maintenance or a frozen durable format

Evidence:

- Payload, seal, chunk-plan, and chunk-body authority is encoded as private
  `ATPAY1`, `ATSEAL1`, `ATMAP1`, and `ATCHK1` bodies inside generic
  `PayloadChunk` frames.
- Store checkpoint version 5 and the separate coordinator checkpoint are also
  durable semantic formats. None is frozen in `FORMAT_SPEC.md` or the Atomic
  CBOR registry with compatibility/recovery rules.
- Existing compaction builds a live-projection output from ordinary indexed
  `ItemEvent` puts. Staged Atomic members are deliberately absent from that
  index, while `BatchPrepare` and private `PayloadChunk` roles are not copied.
- Source reclaim can then delete the sealed segments holding those records.
- The stage checkpoint masks this initially because it duplicates the full
  catalogue and ignores missing covered files, leaving one mutable sidecar as
  the only remaining copy.
- No compaction, rotation, pending-seal, Recovery Shadow, backup/restore, clone,
  salvage, or scrub proof establishes equal-or-stronger survival posture.

Impact:

An existing supported maintenance operation can retire authoritative staging
media without understanding it. The replacement sidecar has no approved
durable-format or survival contract. This violates both the ATM-2 rotation
invariant and the specification amendment rule.

Required fix:

1. Freeze every durable Atomic staging/index record, ownership rule, version,
   limit, and compatibility behavior before treating it as authority.
2. Integrate Atomic records into compaction/rotation/recovery preservation, or
   fail those operations closed while outstanding evidence exists.
3. Ensure Recovery Shadow and backup/restore carry the same or stronger Atomic
   evidence, or explicitly gate them until ATM-4 delivery.
4. Add maintenance-aware checkpoint invalidation/relocation; never silently
   trust references to retired files.

Acceptance proof:

- prepare/member/chunk/seal prefixes survive actual rotation and compaction;
- a one-copy checkpoint loss after compaction cannot erase accepted evidence;
- backup/restore/clone behavior for Heap identity is specified and tested; and
- `FORMAT_SPEC.md` plus fixtures cover all shipped durable representations.

### CR-ATMR6-007 — AMBER — The store I/O matrix is an API-error matrix, not the required byte/phase crash proof

Evidence:

- Store failpoints exist only before append, after append, and after checkpoint.
  They do not enumerate low-level write, short-write, file-sync, directory-sync,
  rename/publish, ENOSPC, rotation, or compaction boundaries.
- The test injects an ordinary returned error, then cleanly drops and reopens
  the store. It does not model process death or removal of unsynced media.
- `Scenario::Member` arms payload-sidecar failpoints after `setup` has already
  called `begin_prepare`; the actual authoritative Atomic member-frame append
  is not the operation under test.
- `Scenario::Prepare` permits the inaccurate absence result described in
  CR-ATMR6-005.
- Seal-after-append permits either staged or durable rather than proving one
  exact outcome for a defined persistence boundary.

Impact:

Ordering, durability, and examiner bugs can pass the new matrix. The suite is
useful failpoint plumbing evidence, but it does not meet the ATM-2 exit gate of
failure at every byte/phase with exact reopen classification.

Required fix:

1. Generate cells for every authoritative prepare, member, payload, chunk-plan,
   chunk-body, seal, checkpoint, and coordinator write/sync/publish boundary.
2. Add true process-kill or crash-media images and remove unsynced effects.
3. Exercise the member frame itself rather than naming a payload scenario
   `Member`.
4. Give every cell one reviewed semantic result (or a rigorously justified set
   tied to an actual OS boundary), including exact examination projection.
5. Include sensitive mutants proving each ordering edge is necessary.

Acceptance proof:

- all required prefix cells execute and reconcile with the model;
- omit-file-sync/omit-dir-sync/short-write mutants fail semantically;
- repeated default-parallel runs are deterministic; and
- staged bytes remain absent from every ordinary surface in every cell.

### CR-ATMR6-008 — AMBER — The acceptance gate and handoff are not self-consistent

Evidence:

- `scripts/verify-atomics.sh full` exits 1 at the clean review baseline.
- `residiuum-atomic-lane --all-targets` fails three acknowledged-damage tests.
- package formatting fails because `checkpoint.rs` is not rustfmt-clean.
- strict peer-lane Clippy fails `large_enum_variant` for `CheckpointLoad`.
- The generated ATM-2 blocker list still says peer-lane honest-damage tests and
  scoped store rustfmt are missing, although both are now commands in the run.
- The manifest consequently labels both packages `diagnostic`; no clean
  acceptance-candidate evidence exists for this delivery.

Impact:

Humans and automation cannot use the manifest as a reliable statement of what
was executed or what remains. A handoff cannot supersede the prior review while
its declared full gate is failing.

Required fix:

1. Fix the behavioral failures, formatting, and strict lint failure.
2. Generate blocker/residual claims from actual command/evidence state or keep
   the static list accurately maintained.
3. Separate future deferred families from falsely reported missing tests.
4. Regenerate the handoff only after the exact clean commit passes the declared
   gate; preserve the failing manifest as diagnostic evidence.

Acceptance proof:

- `scripts/verify-atomics.sh full` exits 0 on a clean tree;
- every manifest family/result agrees with its command exit codes;
- the residual list names only genuinely absent/deferred proof; and
- artifact hashes and the handoff reference the exact accepted commit.

## Required delivery order

1. **CR-ATMR6-001 and CR-ATMR6-002:** restore honest recovery truth before
   optimizing the checkpoint again.
2. **CR-ATMR6-003:** correct persist-before-apply seal ordering.
3. **CR-ATMR6-004:** replace prototype whole-file/full-state limits with an
   operable admission and incremental catalogue design.
4. **CR-ATMR6-005:** expose and test exact surviving prepare/material states.
5. **CR-ATMR6-006:** freeze the durable records and fence/integrate maintenance.
6. **CR-ATMR6-007:** rebuild the crash proof around the resulting authority.
7. **CR-ATMR6-008:** make the final clean verifier and handoff truthful.

Do not spend time reopening ATMR5-003, ATMR5-005, ATMR5-006, or ATMR5-007
unless the above repairs change their underlying record protocol.

## Acceptance boundary for the next review

The next review should be performed on one clean commit and include:

- a response matrix mapping every `CR-ATMR6-*` item to code, tests, and durable
  format changes;
- an approved architecture note if the design chooses authoritative checkpoint
  summaries instead of full covered-prefix verification;
- green exhaustive peer and store damage tests;
- same-handle plus reopen seal failpoint tests;
- valid large-store/capacity tests;
- prepared/partial examination fixtures;
- actual maintenance survival or explicit fail-closed gates; and
- a green `scripts/verify-atomics.sh full` manifest from that exact commit.

Until then ATM-2 remains a promising prototype implementation, not accepted
store durability.
