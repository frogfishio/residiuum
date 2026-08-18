# ATM-1 / ATM-2 deep acceptance review — ATMR4

Date: 2026-08-18

Review baseline: clean `9de99157e2ee04ed17472cacf57e3f11578dd176`
(`origin/main` at review start)

Normative authority:

- `doc/todo/atomics/ATOMICS_SPEC.md`;
- `doc/todo/atomics/ATOMICS_IMPLEMENTATION_PLAN.md`;
- `spec/atomics/cbor-v1.json`; and
- `doc/reference/storage/FORMAT_SPEC.md`.

This is a new review round. Its findings use only the independent
`CR-ATMR4-*` namespace. `CR-ATMR3-*` remains historical evidence and must not
be reused to track this repair pass.

## Acceptance decision

### ATM-1

**The compiler/validator core remains technically sound, but the current
package is not acceptance-ready.**

The canonical plan, oracle, authority, encoding, and resource-limit work found
acceptable in ATMR3 remains intact. However, the new plan-to-prepare seam
accepts arbitrary additional members as if they were generated consequences.
That means the durable closure is not yet proven to be the exact semantic
closure of the admitted plan. The clean full verifier also fails formatting,
Clippy, and the I/O test target, so there is no green package record at this
baseline.

### ATM-2

**Not accepted.**

The delivery is materially better than ATMR3. In particular, it now has:

- plan-derived prepares;
- correct `not_committed` classification;
- acknowledged log coverage checks;
- explicit recovery ceilings and statistics;
- a same-process and cross-process lane writer lock;
- durable chunk sidecars;
- a first store staging slice; and
- a phase-indexed I/O harness.

Those are useful building blocks, not yet one authoritative storage protocol.
The checkpoint can invent state, normal checkpoint recovery still scans whole
logs, chunked members bypass the lane member log, the store and peer lane are
two independently durable copies, and exclusive short writes poison retry.
The required visibility and examination surface matrix also remains incomplete.

`Capabilities::atomics` must remain `false`. ATM-3 must not treat the current
lane or `StoreAtomicStage` as an accepted decision/publication foundation.

## Verification performed

At the clean baseline:

- the scoped Atomics, format, store staging, damage, chunk, writer-lock, and
  recovery tests executed by `bash scripts/verify-atomics.sh full` passed until
  the lane all-target run;
- `cargo test -p residiuum-atomic-lane --offline --all-targets` failed in
  `io_prefix_matrix` under normal parallel test execution;
- the same I/O test binary passed with `--test-threads=1`, proving shared
  failpoint state leaks between tests;
- formatting check failed on the changed Atomics/lane sources;
- Clippy with warnings denied failed in `io_fail.rs` and `lane.rs`;
- the verifier correctly wrote a `diagnostic`/failed record rather than an
  acceptance candidate; and
- `git diff --check` was clean.

Two temporary review probes were added, run, and removed:

1. changing only the checkpoint's final `sealed` byte from `0` to `1` made
   reopen report `DurableInvisible` even though no `sealed/<atomic_id>` record
   existed; and
2. injecting a short write into the plan sidecar made both immediate and
   post-reopen retries of the exact same plan fail permanently.

Both probes passed as reproductions on `9de9915`; neither probe remains in the
working tree.

## ATMR3 repair-theme status

This table is informational. Only the new ATMR4 identifiers below are active.

| ATMR3 theme | ATMR4 result |
| --- | --- |
| Plan-derived prepare | Improved, but arbitrary unproven extra members remain admissible. |
| Not-committed classification | Closed for the reviewed format classifier and tests. |
| Acknowledged log damage | Closed for direct log corruption; checkpoint state can still bypass the log truth. |
| Bounded recovery | Ceilings exist, but the checkpoint path full-scans logs and several sidecars are read before size checks. |
| Durable chunks | Sidecars survive reopen, but chunked members are absent from the lane member log and the chunk manifest is not part of the closed commitment. |
| Store integration | A first slice exists, but it is a dual-write wrapper over the peer lane, not one store-owned authority. |
| Writer ownership | Closed for one peer-lane root; the store/lane authority split remains. |
| I/O prefix matrix | Implemented partially; parallel execution is unsafe and the permitted outcomes/tested sites are too weak. |
| Handoff/evidence | Much more honest; current full run is correctly diagnostic, but not green. |

## New change requests

### CR-ATMR4-001 — RED — The admitted member set is not the exact closed semantic plan

Evidence:

- `prepare_from_closed_plan` calls `bind_members_to_plan`, then commits the
  supplied member slice into `ordered_member_manifest_root`.
- `bind_members_to_plan` consumes one matching member per user mutation but
  explicitly ignores every member left in `unused`.
- A test named `generated_extra_member_enters_the_same_manifest` blesses an
  arbitrary extra record. There is no generated-consequence type, rule
  provenance, active revision binding, or deterministic derivation proving
  that the record is history, index, RRE, uniqueness, or relationship work.
- `plan_content_root(plan)` does not commit those extra semantics. The prepare
  can therefore pair one plan root with multiple independently chosen member
  closures.

Impact:

Any caller able to supply `members` can smuggle an otherwise valid same-Atomic
write into the durable manifest. The prepare authenticates the bytes but does
not prove they are the exact effects authorized by the admitted plan and its
active rules. This breaks the central closed-plan boundary before decisions
exist.

Required fix:

1. Reject all leftover members until generated consequences have a normative,
   typed closed-plan representation.
2. When generated consequences are introduced, derive them inside the trusted
   compiler from the plan plus frozen rule/schema revisions.
3. Commit the generated-consequence set and provenance into the canonical
   content root as well as the member manifest.
4. Do not accept a caller-authored label saying that an extra member is
   generated.

Acceptance proof:

- an arbitrary extra valid member refuses before any file or log append;
- every missing, duplicate, reordered, substituted, or additional member has a
  specified result; and
- independently deriving plan plus consequences produces byte-identical plan,
  prepare, and manifest roots after reopen.

### CR-ATMR4-002 — RED — The checkpoint can invent an authenticated prefix and stable boundary

Evidence:

- checkpoint format `R2CKP1` contains offsets, identity summaries, and a plain
  `sealed` Boolean, with no digest, frame hash frontier, or authenticated link
  to the coordinator/shard logs.
- `apply_checkpoint` reconstructs prepares and members from mutable
  `plan/`, `intent/`, `payload/`, and `chunk-*` sidecars, not from the skipped
  log prefix.
- it then calls `heap.seal_member_boundary` directly for every checkpoint item
  whose Boolean is true.
- `replay_seals` only applies seals that exist; it never disproves a seal that
  the checkpoint already invented.
- The review probe changed the final checkpoint byte from `0` to `1`. Reopen
  succeeded as `DurableInvisible` while `sealed/<atomic_id>` did not exist.
- `verify_log_coverage` verifies framing coverage, but does not prove that the
  checkpoint summaries or seal bits equal the records in that prefix.

Impact:

Recovery can promote staged material across the first stable boundary without
the durable evidence required by the protocol. A later decision layer could
then rely on a member-durability fact that never existed. This is guessed
recovery state and a direct RED violation.

Required fix:

1. Define a versioned checkpoint whose body is checksum-protected and whose
   frontier commits to exact coordinator/shard prefix hashes or equivalent
   authenticated accumulator roots.
2. Never reconstruct `sealed` solely from a checkpoint Boolean. Bind the seal
   record/hash into the checkpoint or validate the authoritative seal record
   before changing member phase.
3. Treat a checkpoint mismatch as explicit damaged/incomplete coverage. Do not
   silently use checkpoint facts and do not silently downgrade to absence.
4. Add checkpoint upgrade/rebuild semantics and report the disposition in
   recovery statistics/OpenReport.

Acceptance proof:

- every single-byte mutation/truncation of checkpoint fields has an explicit
  corrupt, incomplete, or safe rebuild result;
- checkpoint offsets and summaries are independently cross-checked against the
  authenticated log frontier;
- a forged seal bit cannot change lifecycle; and
- a valid checkpoint plus bounded tails recreates byte-identical status.

### CR-ATMR4-003 — AMBER — The checkpoint path still full-scans logs and cannot open a normally grown lane

Evidence:

- after applying a checkpoint, `recover_heap` calls `verify_log_coverage` for
  the coordinator and every shard;
- `verify_log_coverage` calls `read_log_tail(path, 0, budget)`, so the alleged
  tail path starts at byte zero and reads the whole log;
- only afterwards does recovery request the tail from the checkpoint offsets;
- one global prototype budget is 16 MiB, 4,096 Atomics, 4,096 members, and
  8,192 directory entries;
- crossing the log ceiling makes ordinary open return `Incomplete`, even with
  a current valid checkpoint. The checkpoint does not make open independent of
  historical log size as required by `ATOMICS_SPEC` §11.

Impact:

Memory is capped, which closes the literal unbounded-allocation defect for log
bytes, but a healthy lane eventually becomes unopenable and the normal path
still pays work proportional to history. This is not an acceptable bounded
recovery architecture.

Required fix:

1. Validate prefix authenticity from the checkpoint commitment without reading
   the prefix on every open.
2. Stream and bound only bytes after the frontier on the normal path.
3. Make full scan an explicit degraded rebuild mode with reason, progress,
   ceilings, and an OpenReport—not an implicit normal-open step.
4. Define pruning/decision retention so the live status catalogue is not
   capped permanently at 4,096 historical Atomics.

Acceptance proof:

- open work remains approximately constant when historical logs grow from MiB
  to GiB while the post-checkpoint tail is held fixed;
- a log larger than 16 MiB opens normally with a valid current checkpoint;
- corrupt checkpoint triggers a labelled bounded rebuild/incomplete result; and
- reported bytes scanned distinguish prefix, tail, sidecars, and rebuild work.

### CR-ATMR4-004 — RED — Chunked members bypass authoritative member evidence and closure

Evidence:

- `DurableLane::append_staged` persists an `ItemEvent` member frame before
  applying the member to the kernel.
- `DurableLane::append_chunk` persists chunk sidecars and an assembled payload,
  but never calls `persist_member_frame` when the member becomes complete.
- `seal_member_boundary` syncs coordinator and shard logs even though the
  chunked member is absent from the shard log, then writes a seal claiming the
  complete member boundary.
- recovery explicitly reconstructs that member from `chunk-manifest/` and
  `chunk/` sidecars, bypassing member-log evidence.
- the `ChunkPlan` is installed after prepare and is not included in
  `AtomicMember`, `AtomicPrepare`, or the plan content root. The final payload
  hash is committed, but the supposedly frozen chunk manifest is not.

Impact:

The first stable boundary does not cover the same evidence for chunked and
unchunked values. Independent examination of coordinator/shard logs cannot
prove the chunked member exists, and a post-prepare sidecar chooses physical
manifest semantics that the closed prepare did not commit.

Required fix:

1. Persist the canonical member frame on the authoritative shard path when the
   chunked payload becomes complete, before seal eligibility.
2. Bind the chunk manifest (count, order, hashes, assembled length/hash, profile)
   into the closed generated member/prepare commitment, or formally reduce it
   to a non-authoritative transport detail and prove the authoritative frame
   plus payload commitment is sufficient.
3. Make recovery and examination derive the same member state from the same
   authority.
4. Include chunk manifest/body/payload/member/seal phases in the crash matrix.

Acceptance proof:

- a chunk-complete lane contains the exact member frame expected by independent
  format examination;
- seal refuses if that frame is absent or unacknowledged;
- mutation/substitution of chunk plan, body, order, count, or assembled payload
  cannot produce a valid boundary; and
- chunked and unchunked forms have equivalent durability and visibility
  outcomes at every prefix.

### CR-ATMR4-005 — RED — `StoreAtomicStage` creates two durable authorities with no atomic join

Evidence:

- `StoreAtomicStage` owns both a mutable `Store` and an independently opened
  `DurableLane` below `store-info/atomic-lane`.
- prepare and unchunked member operations first commit to the lane, then append
  a second copy to the store active segment. A crash/error may occur between
  those independently durable operations.
- store reopen does not reconstruct staging state from its segment frames;
  `atomic_stage()` opens or creates the peer lane and treats that as semantic
  truth.
- lane reopen does not reconcile its state with the duplicated store frames.
- sealing exists only in the lane. The store segment has no joined durable
  boundary covering its copied prepare/member frames.
- on the chunk path, if the lane completes the final chunk but the store member
  append fails, retry observes `was_complete == true` and never retries the
  missing store member frame.

Impact:

The system can contain lane-only evidence, store-only/corrupt copies, or a
complete lane member with a permanently absent store member. Which copy is
authoritative is undefined. Compaction, rotation, backup, salvage, and future
decision publication cannot safely reason about this split.

Required fix:

1. Choose one store-owned authoritative append protocol under the existing
   store writer/sequencer; keep the peer lane only as a model/test oracle.
2. Stage prepare/member/chunk material in store media once, with one durable
   order and one recovery reader.
3. If a temporary migration mirror is unavoidable, define a durable journaled
   reconciliation protocol and prove every crash prefix. Do not call both
   copies authoritative.
4. Make idempotent retry close a partially completed store append, including
   the final-chunk case.

Acceptance proof:

- deleting or corrupting a non-authoritative test mirror cannot change store
  status;
- store reopen, format examination, and Atomic status agree using store media;
- crash at every boundary between prepare/member/chunk/seal store operations
  has one specified outcome; and
- rotation/compaction cannot orphan, duplicate, or publish staged evidence.

### CR-ATMR4-006 — RED — Short exclusive writes permanently poison same-ID retry

Evidence:

- `write_exclusive` creates the final path with `create_new(true)` before the
  `BeforeWrite` failpoint and writes directly into it.
- an injected pre-write error leaves an empty final file; a short write leaves
  a truncated final file.
- retry sees `AlreadyExists`, compares the bad bytes with the intended bytes,
  and returns `AtomicIdConflict`.
- there is no authenticated incomplete marker, cleanup, quarantine, or
  resumable exclusive publish protocol.
- The review probe demonstrated that an exact retry fails both in-process and
  after reopen following a short plan-sidecar write.

Impact:

A pre-admission storage error can consume an Atomic ID permanently without a
valid prepare or retry promise. The client cannot distinguish a genuine
same-ID/different-content conflict from an engine-created torn sidecar.

Required fix:

1. Publish exclusive immutable files through a unique temp file, file sync,
   no-replace/identity-safe publication, and directory sync.
2. Define how abandoned temp files and torn legacy final files are detected,
   quarantined, rebuilt, or reported without converting them into ID conflict.
3. Preserve exact same-ID retry; never overwrite a different valid identity.
4. Avoid one shared `.tmp` filename that lets concurrent or stale operations
   overwrite one another.

Acceptance proof:

- exact retry succeeds or returns a typed recoverable outcome after every
  pre-write, short-write, post-write, sync, publish, and directory-sync prefix;
- a valid different existing identity still conflicts;
- stale temp files do not block or replace valid material; and
- process-kill tests cover plan, intent, payload, chunk manifest, and chunk
  body exclusive files.

### CR-ATMR4-007 — RED — Several recovery inputs are allocated before their limits are checked

Evidence:

- checkpoint, plan, intent, chunk-manifest, chunk-body, and seal readers use
  `fs::read` before checking a file-size ceiling;
- payload is the exception: it checks metadata length first;
- the checkpoint decoder bounds declared counts only after the whole file is
  resident;
- chunk manifest validation and chunk-body/hash validation likewise happen
  after allocation;
- the recovery byte budget charges log reads, but not all sidecar bytes and
  working allocations.

Impact:

A hostile or damaged sidecar can allocate memory proportional to file size
before any semantic limit fires. This leaves the ATMR3 unbounded-recovery RED
open through non-log inputs.

Required fix:

1. Define maximum encoded bytes for every durable file role.
2. Check metadata length before allocation/open-ended read.
3. Stream where the valid maximum is not intentionally small.
4. Charge all sidecar bytes, decoded objects, chunk bodies, directory entries,
   sorting buffers, and retained state to a single explicit recovery budget.
5. Distinguish hostile limit exhaustion from corruption and incomplete
   coverage in the public recovery report.

Acceptance proof:

- sparse and real oversized files for every role refuse before proportional
  allocation;
- one-unit-at-limit and one-unit-over tests exist for each role;
- aggregate small files cannot evade total byte/work-memory ceilings; and
- recovery statistics reconcile all bytes and objects visited.

### CR-ATMR4-008 — AMBER — The I/O proof matrix is nondeterministic and admits almost any outcome

Evidence:

- all failpoint registries, visit maps, and mutants are process-global;
  `io_prefix_matrix` tests mutate them concurrently without serialization;
- the all-target run failed two I/O tests under default parallel execution,
  while the same binary passed with `--test-threads=1`;
- the broad matrix does not call `require_visited(point)` per injected cell, so
  an unreachable injection can pass;
- many rows permit `Damage`, and the default row permits every outcome;
- `run_op` discards the operation result, reducing diagnostic precision;
- omit-sync mutants only prove that an instrumentation counter was not visited;
  they do not model loss of unsynced bytes and prove that reopen detects the
  protocol violation;
- chunk, store-segment, rotation, and store/lane join phases are absent from
  the matrix.

Impact:

The suite is not deterministic in normal CI and is too permissive to establish
the claimed crash contract. A broken write edge can survive because `Damage`
is accepted or because the intended injection was consumed/reset by another
test.

Required fix:

1. Give each test an isolated failpoint context or serialize the complete
   binary explicitly and enforce that mode in the verifier.
2. Require every armed point to be visited exactly as intended.
3. Specify the exact allowed result set for every cell; use `Damage` only where
   the protocol genuinely declares damaged coverage.
4. Assert operation return class as well as reopened state.
5. Add a crash-media model or directory-image harness that actually removes
   unsynced writes/renames for mutants.
6. Cover all chunk and authoritative store phases.

Acceptance proof:

- repeated default-parallel and serial runs are deterministic;
- deliberately unreachable failpoints fail the suite;
- each omitted required durability edge causes at least one semantic test to
  fail; and
- the matrix publishes a reviewed cell-by-cell expected-outcome table.

### CR-ATMR4-009 — AMBER — ATM-2 visibility and independent examination remain incomplete

Evidence:

- store tests cover point get, logical scan, history, and secondary-index path
  inspection for a small unrotated store;
- no store-backed tests cover RQL, watch/change streams, ordinary catalogue
  APIs, backup/restore, compaction, segment rotation, or recovery shadow;
- there is no `residiuum-examine` projection for valid, partial, corrupt,
  conflicting, and unsupported Atomic groups;
- the handoff itself lists RQL, watch, examine, and rotation/cohort isolation as
  residuals;
- `ATOMICS_IMPLEMENTATION_PLAN` §8 makes examination projection and invisibility
  across point/scan/RQL/history/watch/secondary mandatory ATM-2 work.

Impact:

The current tests prove a valuable narrow negative, not the ATM-2 visibility
contract. A staged frame may still leak or be mishandled through an untested
reader, maintenance path, or rotated segment.

Required fix:

1. Build a single store-backed surface matrix covering every ordinary read and
   observation API.
2. Add rotation, reopen, compaction, backup/restore, and protected-recovery
   prefixes while staged material exists.
3. Implement independent examine/status projection over the same authoritative
   store evidence and coverage model.
4. Include negative controls that deliberately expose one staged record through
   each surface and prove the test detects it.

Acceptance proof:

- all named surfaces remain empty/invisible before decision at every crash and
  maintenance prefix;
- examiner and recovery agree on healthy, partial, corrupt, conflicting,
  unsupported, and coverage-incomplete fixtures; and
- multi-Heap and cohort-neighbour tests survive rotation and compaction.

### CR-ATMR4-010 — AMBER — The clean full package gate is red

Evidence:

- the clean full verifier recorded `result: fail` and
  `acceptance: diagnostic`;
- the lane all-target test failed because of the shared failpoint-state race;
- formatting check reports diffs in the changed Atomics/lane files; and
- Clippy with `-D warnings` reports `io_other_error` and
  `redundant_closure` failures.

Impact:

There is no clean acceptance evidence for the reviewed commit. The handoff is
accurately cautious, but cannot be promoted or signed off.

Required fix:

1. Close the substantive CRs before regenerating evidence.
2. Make default verifier execution deterministic and green; do not rely on a
   reviewer-only serial flag.
3. Apply repository formatting and clear Clippy warnings.
4. Regenerate the package handoff and both manifests at one clean commit, with
   current commit/toolchain/suite identity and explicit remaining blockers.

Acceptance proof:

- `bash scripts/verify-atomics.sh full` exits zero at a clean tree;
- every command and family in the run record passes;
- detached hashes verify; and
- ATM-1 and ATM-2 labels remain independently honest.

## Architecture dispositions

### Envelope namespace

The ATMR3 recommendation stands: ownership keys 31–36, Atomic keys 37–40, and
operation identity keys 41/42 are coherent in the reviewed code. Record the
amendment in `FORMAT_SPEC.md` with the legacy dual-read lifetime. This is an
architectural recording action, not permission to advertise Atomics.

### Peer lane

Retain `residiuum-atomic-lane` as a mechanics/fault-injection oracle. Do not
evolve `StoreAtomicStage` by continuing to dual-write more state. The next
storage design should place authoritative evidence directly under the store's
writer, segment, rotation, compaction, recovery, and examination domains.

### Capability

Keep `Capabilities::atomics == false` until ATM-5 acceptance. No provisional
SDK Atomic surface should be published from this delivery.

## Required delivery order

1. Close CR-ATMR4-001 before any decision commits to the wrong semantic
   closure.
2. Redesign the authoritative store boundary under CR-ATMR4-005; do not build
   ATM-3 on dual authorities.
3. Fix exclusive publication and bounded sidecar reads under CR-ATMR4-006/007.
4. Define the authenticated checkpoint and genuine tail recovery under
   CR-ATMR4-002/003.
5. Re-home chunk evidence on that authority and close CR-ATMR4-004.
6. Run the exact I/O and visibility/examination matrices under
   CR-ATMR4-008/009.
7. Clear the package gate and regenerate evidence under CR-ATMR4-010.

## Acceptance gates

ATM-1 may be accepted when:

- CR-ATMR4-001 is closed at the plan/prepare seam;
- canonical plan/oracle/authority/encoding/limit suites remain green;
- the format amendment disposition is recorded; and
- a clean full package record passes formatting and Clippy.

ATM-2 may be accepted only when:

- CR-ATMR4-001 through CR-ATMR4-010 are closed or explicitly dispositioned by
  architecture without weakening correctness;
- one store-owned authority covers prepare, all members/chunks, and the first
  stable boundary;
- normal recovery authenticates a checkpoint and reads bounded tails rather
  than historical prefixes;
- no durable input allocates before its hard and aggregate limits;
- exact same-ID retry survives every legal failure prefix;
- the complete store visibility and independent examination matrices pass;
- default crash/fault tests are deterministic and mutation-sensitive; and
- a clean, commit-scoped, package-specific handoff and evidence record is
  reviewed and signed off.
