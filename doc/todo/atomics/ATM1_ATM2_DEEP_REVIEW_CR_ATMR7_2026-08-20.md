# ATM-1 / ATM-2 deep acceptance review — ATMR7

Date: 2026-08-20

Review baseline: clean `5f90d59b1b15ace474bf418cf46857b0042ecbf7`
(`origin/main` at review start)

Compared with: `7f3db25ffda83832b46d3adf1ff5a1539cc93f6c`
(ATMR6 implementation baseline)

Normative authority:

- `doc/todo/atomics/ATOMICS_SPEC.md`;
- `doc/todo/atomics/ATOMICS_IMPLEMENTATION_PLAN.md`;
- `spec/atomics/cbor-v1.json`;
- `crates/residiuum-atomics/spec/cbor-v1.json`; and
- `doc/reference/storage/FORMAT_SPEC.md`.

This is an independent review round. Active findings use only the new
`CR-ATMR7-*` namespace. ATMR6 identifiers are historical closure evidence and
must not be reused for this repair pass.

## Acceptance decision

### ATM-1

**Accepted for the ATM-1 compiler/validation scope.**

The semantic acceptance at `00a06ae` remains valid, and the clean full verifier
at `5f90d59` now records ATM-1 as `acceptance_candidate`. Encoding, oracle,
authority, isolation, resource limits, hostile inputs, formatting, and strict
linting are green.

### ATM-2

**Not accepted. `Capabilities::atomics` must remain `false`; ATM-3 must not
consume the current store stage as accepted durability.**

This delivery closes most ATMR6 correctness defects. The remaining blockers
are concentrated and actionable:

- honest covered-prefix verification has reintroduced a full-database read on
  every Atomic-stage open;
- incomplete coverage is observable but does not prevent new Atomic issuance,
  and its scrub operation does not actually verify restored media;
- merely opening an empty stage permanently disables seal/compaction because
  all covered ordinary media is treated as outstanding Atomic evidence;
- aggregate chunk-plan/checkpoint metadata has no pre-append admission bound;
  checkpoint overflow is silently ignored; and
- much of the crash matrix labels one post-append failpoint as both
  `AfterWrite` and `AfterFileSync`, so those are not distinct tested boundaries.

These are 4 RED and 2 AMBER requests below.

## Verification performed

The full verifier was run on the clean baseline:

```text
bash scripts/verify-atomics.sh full
```

Result:

- exit code: `0`;
- run: `target/atomics-evidence/runs/5f90d59b1b15-full.json`;
- result: `pass`;
- acceptance: `partial`;
- ATM-1: `acceptance_candidate`;
- ATM-2: `partial`;
- failed commands: none;
- formatting and strict peer-package Clippy: green; and
- exhaustive peer-lane acknowledged-damage tests: green.

All newly declared store targets passed, including classification, seal order,
limits, status, maintenance, I/O matrix, retry, chunk recovery, coordinator,
prepare authority, and invisibility.

One temporary reviewer probe was added, executed, and removed. It performed an
ordinary put, opened and dropped an otherwise empty `atomic_stage`, and asserted
that no outstanding Atomic evidence existed. The assertion failed:

```text
reviewer_probe_empty_stage_checkpoint_must_not_disable_maintenance ... FAILED
assertion failed: !outstanding_atomic_evidence(store.paths()).unwrap()
```

No probe code remains in the tree.

## ATMR6 closure map

This table is informational. Only ATMR7 identifiers below are active.

| ATMR6 item | ATMR7 result |
| --- | --- |
| 001 covered-prefix integrity | Correctness closed: production recovery now verifies all stored blocks and exhaustive interior mutation tests pass. Operability remains open as ATMR7-001 because every open rereads all covered store bytes. |
| 002 persistent damage/orphans | Partly closed: conflicts, orphans, findings, and missing paths persist. Incomplete coverage still permits issuance and scrub can clear it by path existence alone (ATMR7-002). |
| 003 persist-before-apply seal | Closed. Same-handle and reopen tests cover pre/post durable seal states. |
| 004 operable limits/catalogue | Partly closed: payloads/chunks use locators and per-payload admission is pre-append. Aggregate chunk-plan/checkpoint capacity remains unbounded/silent (ATMR7-004). |
| 005 surviving prepare projection | Closed. `AtomicStageStatus` distinguishes Prepared/Staged/Sealed/Blocked/Absent. |
| 006 format and maintenance | Durable formats are now frozen and maintenance is fenced. The fence has a false-positive lifetime bug (ATMR7-003). |
| 007 crash-media matrix | Substantially improved, but several named boundaries are aliases rather than distinct I/O phases (ATMR7-005). |
| 008 clean verifier/handoff | Build and verifier closed. The committed handoff still names an older dirty run rather than the final clean evidence (ATMR7-006). |

## New change requests

### CR-ATMR7-001 — RED — Correct covered-prefix verification performs a full-database read on every stage open

Evidence:

- Store `open_catalog` enumerates active, sealed-segment, and pending-seal
  media.
- For every checkpoint-covered file it calls `verify_covered_blocks`, which
  reads and hashes every 64-KiB block plus the leftover from byte zero to
  `covered_len`.
- This happens even when the file length is unchanged and the dirty Atomic tail
  is zero bytes.
- `bytes_verified` records the work but is not charged to `max_scan_bytes`; the
  configured 64-MiB scan ceiling therefore does not bound total physical reads.
- The `files_skipped` label is misleading: a skipped file is not frame-decoded,
  but all its bytes were still read and hashed.
- The stage checkpoint covers every discovered store file, including ordinary
  historical segments with no Atomic evidence.
- Peer-lane recovery makes the same conservative choice and now refuses once
  full covered-prefix verification exceeds its recovery byte budget.

Impact:

The corruption bug is fixed, but normal Atomic-stage open is once again
proportional to the complete retained database. On the multi-gigabyte stores
that motivated bounded startup work, this recreates the full-scan latency and
violates `ATOMICS_SPEC` §11: normal open must use evidence indexes/checkpoints
plus tails, not scan the full database to rediscover old evidence.

Required fix:

1. Stop checkpointing/verifying ordinary media that contains no Atomic staging
   evidence.
2. Anchor Atomic locators to the store's existing immutable sealed-segment
   identities/whole-segment commitments, or use a dedicated store-owned Atomic
   evidence stream/catalogue with an approved trust boundary.
3. Verify active/unsettled tails directly; reuse already qualified immutable
   segment commitments without rereading their payload bytes on every open.
4. Charge every remaining verification byte and expose separate catalogue,
   commitment, tail, and degraded-rebuild costs.
5. If arbitrary offline corruption cannot be detected on every constant-I/O
   open, specify when the status is `coverage_incomplete` and how scrub proves
   it complete; do not silently claim both constant I/O and full-byte checking.

Acceptance proof:

- with a fixed Atomic tail, physical read bytes and open latency remain
  approximately constant as ordinary retained store size grows MiB → GiB;
- interior mutation of an active/unsettled Atomic record is detected;
- mutation of immutable media is detected by its qualified store commitment or
  yields explicit incomplete coverage;
- external I/O measurements reconcile `OpenReport`; and
- normal open does not become incomplete merely because settled history exceeds
  a tail budget.

### CR-ATMR7-002 — RED — Incomplete coverage is reported but does not close the issuance/repair safety boundary

Evidence:

- Missing checkpoint-covered files set `catalog.coverage_degraded` and persist
  a finding.
- Physical scan holes and undecodable unbound evidence persist as findings but
  do not set `coverage_degraded`.
- `StoreAtomicStage::begin_prepare` checks only whether the requested ID is in
  `catalog.blocked`; it does not refuse while global coverage is degraded or an
  unbound corruption finding exists.
- An Atomic accepted in missing/unreadable media may therefore be retried under
  the same ID as a new prepare because recovery cannot bind the lost bytes to a
  specific `blocked` identity.
- `scrub_coverage` clears degradation after checking only that every missing
  relative path now names a regular file. It does not read, hash, classify, or
  reconcile the restored file against the lost covered frontier.
- It immediately persists the cleared state, so the live handle can advertise
  complete coverage before a later reopen happens to scan the restored file.

Impact:

The implementation has an honest warning flag but does not enforce the rule
that incomplete coverage cannot prove identity absence. The operator repair
API can also clear that flag without proving that the missing evidence was
restored. This leaves a same-ID double-execution/guessed-absence path.

Required fix:

1. Treat every unbound hole/corrupt record as global incomplete coverage unless
   a stronger classifier proves it unrelated to Atomic authority.
2. Refuse new prepare issuance while coverage is incomplete. Exact retries of
   already catalogued identities may be allowed only when their evidence is
   complete enough to prove the exact retry.
3. Make scrub perform a bounded authenticated rescan/reconciliation and clear
   degradation only after facts, locators, findings, and coverage frontier are
   rebuilt consistently.
4. Do not clear degradation by path existence.

Acceptance proof:

- a missing covered file prevents issuance of a previously unseen ID;
- an unbound corrupt/hole fixture prevents absence-dependent issuance;
- replacing a missing path with arbitrary bytes cannot pass scrub;
- restoring the exact authenticated media can pass scrub; and
- status and admission agree before and after process reopen.

### CR-ATMR7-003 — RED — An empty stage checkpoint permanently disables ordinary maintenance

Evidence:

- `checkpoint_indicates_outstanding` returns true when either the catalogue has
  evidence **or `covered` is nonempty**.
- Opening `Store::atomic_stage` on an ordinary store inventories and checkpoints
  all active/segment media even when no prepare, member, payload, chunk, seal,
  blocked identity, finding, or coordinator sequence exists.
- The resulting nonempty covered-file vector is therefore classified as
  outstanding Atomic staging evidence.
- `seal_active`, compaction/reclaim paths, and identity-reassign clone call the
  maintenance fence and are refused thereafter.
- The reviewer probe reproduced this with one ordinary put and an empty stage
  open; `outstanding_atomic_evidence` unexpectedly returned true.
- There is no operation that removes ordinary covered paths from the checkpoint
  or declares an empty stage quiescent.

Impact:

Merely initializing or inspecting Atomics can permanently prevent segment
rotation, compaction, reclaim, and clone on a store that has never issued an
Atomic. In an active deployment this eventually prevents normal write growth.

Required fix:

1. Define outstanding evidence from actual Atomic facts/degradation, not the
   presence of an acceleration frontier over ordinary media.
2. An empty/quiescent stage must permit normal seal/compact/reclaim/clone.
3. Preserve fail-closed behavior for unreadable checkpoints only when they may
   contain Atomic facts; use a small explicit `has_outstanding`/generation
   commitment if that distinction cannot be recovered cheaply.
4. Define how ATM-3 completion/reclaim eventually returns the stage to quiescent
   maintenance-allowed state.

Acceptance proof:

- ordinary put → empty stage open/reopen → seal/compact/reclaim succeeds;
- a real prepare at every prefix keeps maintenance refused;
- loss/corruption of a checkpoint that may contain evidence remains fail-closed;
  and
- after qualified Atomic evidence retirement, maintenance becomes available
  without deleting safety metadata by hand.

### CR-ATMR7-004 — RED — Aggregate chunk-plan/checkpoint capacity is not admitted before durable append

Evidence:

- `admit_new_atomic` bounds only the number of prepares.
- `admit_payload_bytes` bounds payload/chunk body bytes, but
  `commit_chunk_manifest` has no aggregate work/checkpoint-byte admission.
- Each member may legally name up to 4096 chunk hashes. One maximum plan is
  approximately 128 KiB of hash metadata; many such plans remain within member
  and proposed-payload ceilings while exceeding the 16-MiB checkpoint ceiling.
- `persist_chunk_plan` appends the plan frame durably and inserts it into memory
  before `persist_live_checkpoint` discovers the resulting checkpoint size.
- `persist_checkpoint` does not return capacity failure when encoded bytes
  exceed `max_checkpoint_bytes`; it silently returns `Ok(())` without updating
  the checkpoint or any OpenReport/degraded-state field.
- The same function always uses `AtomicStageLimits::operable()` rather than the
  limits attached to the current stage, so `atomic_stage_with_limits` cannot
  enforce a lower checkpoint ceiling on writes.
- Subsequent operations can continue acknowledging media against a stale
  checkpoint until the dirty tail, active segment, or retained work crosses a
  different limit.

Impact:

A valid bounded protocol input can create unbounded aggregate chunk-map memory
and checkpoint write amplification, silently disable checkpoint progress after
durable acceptance, and eventually strand the stage behind tail/maintenance
limits. This violates pre-allocation/pre-append resource admission.

Required fix:

1. Add aggregate chunk-count/hash-metadata/work/checkpoint budgets derived from
   the frozen per-Atomic and outstanding-concurrency limits.
2. Estimate the exact incremental durable catalogue cost and refuse before the
   chunk-plan append if it would exceed policy.
3. Use the stage's configured limits consistently for both load and persist.
4. If checkpoint publication is optional acceleration, record stale/degraded
   disposition and ensure bounded tail recovery remains possible; never silently
   return success as though the frontier advanced.
5. Add a reclaim/rotation strategy before accepted tails can exceed their
   bounded recovery window.

Acceptance proof:

- many maximum-hash chunk plans remain within one declared aggregate budget;
- one unit over refuses before active media grows;
- custom lower checkpoint/work limits are honored on write and reopen;
- forced checkpoint non-publication is visible and bounded; and
- no sequence of accepted plan metadata makes the stage unreopenable.

### CR-ATMR7-005 — AMBER — Several crash-matrix I/O boundaries are aliases, not independently exercised phases

Evidence:

- The matrix declares `BeforeWrite`, `AfterWrite`, `AfterFileSync`, and
  `AfterCheckpoint` for every scenario.
- For prepare, member, chunk plan, chunk body, and seal, both `AfterWrite` and
  `AfterFileSync` map to the same `store.atomic.<role>.after_append` failpoint.
- Those two cells therefore execute the same code boundary and cannot prove
  loss of an unsynced write versus survival of a synced write.
- Only the payload scenario maps `AfterWrite` to the lower-level
  `store.active.write_tail.after_write` point.
- Checkpoint/coordinator `AfterWrite`, `AfterFileSync`, and in places
  `AfterCheckpoint` similarly collapse onto one post-persist point.
- The test uses caught in-process panic plus `abandon_for_crash_test`; the
  handoff correctly acknowledges that no multiprocess abort cell exists.

Impact:

The suite is substantially better and now tests exact projections, real member
frames, short checkpoint media, and tail removal. But its matrix shape
overstates phase coverage and cannot yet prove several write-before-sync safety
edges.

Required fix:

1. Add distinct lower-level failpoints for after bytes written, after file sync,
   after publish/rename, and after directory sync for each authoritative role.
2. Remove cells whose boundaries do not exist rather than aliasing them.
3. Mutate crash media according to the actual boundary and prove the expected
   projection differs when durability differs.
4. Add at least one subprocess abort/kill sentinel to validate that the
   in-process abandonment model matches real process death.

Acceptance proof:

- every named matrix phase maps to a unique visited failpoint;
- omit-sync mutants change semantic outcomes at the intended edge;
- a subprocess crash produces the same image/projection as the model; and
- no ordinary surface exposes staged material in any cell.

### CR-ATMR7-006 — AMBER — The committed handoff does not name the final clean evidence run

Evidence:

- The actual clean run at review baseline is
  `target/atomics-evidence/runs/5f90d59b1b15-full.json`, exit 0, with no failed
  commands.
- `ATM1_ATM2_HANDOFF_ATMR6_2026-08-20.md` still records the earlier dirty
  `11d6f2e5060e` run, labels it diagnostic, and says to rerun after commit.
- The final boxing/format/lint commit is `5f90d59`, so the handoff predates the
  evidence that actually closes ATMR6-008.

Impact:

The machine manifest is truthful, but the human delivery record does not point
to the exact clean commit/run it asks governance to review.

Required fix and proof:

Regenerate the handoff from the accepted clean run, including exact commit,
manifest path/hash, command count, package labels, and the genuinely remaining
ATMR7 residuals. Preserve the older dirty run as diagnostic history.

## Required delivery order

1. **CR-ATMR7-002 and CR-ATMR7-003:** close issuance safety and the maintenance
   false positive first.
2. **CR-ATMR7-004:** make every aggregate resource refusal pre-append and keep
   checkpoint/tail recovery operable.
3. **CR-ATMR7-001:** establish the final store-trust architecture that is both
   honest and tail-bounded.
4. **CR-ATMR7-005:** align the crash proof with the resulting actual I/O edges.
5. **CR-ATMR7-006:** regenerate the clean final evidence/handoff.

Do not reopen the accepted retry, chunk continuation, coordinator-order,
prepare-authority, persist-before-apply seal, exclusive-publication, or typed
status work unless the recovery architecture changes their durable format.

## Acceptance boundary for the next review

The next package should contain:

- a response matrix for every `CR-ATMR7-*` item;
- an architecture note for immutable segment/catalogue trust and bounded open;
- degraded-coverage admission and authenticated scrub tests;
- the empty-stage maintenance regression test reproduced above;
- aggregate maximum chunk-map/checkpoint pre-admission tests;
- uniquely mapped crash I/O phases plus one subprocess crash sentinel; and
- a clean full manifest and regenerated handoff naming the exact same commit.

Until those are delivered, ATM-2 remains a strong but partial staging
implementation, not accepted store durability.
