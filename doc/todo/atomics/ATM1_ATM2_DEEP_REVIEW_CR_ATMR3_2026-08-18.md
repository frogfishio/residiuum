# ATM-1 / ATM-2 deep acceptance review — ATMR3

Date: 2026-08-18

Review baseline: clean `bcafefc60de06be8b948360356bec978fda8b97f`
(`origin/main`)

Normative authority:

- `doc/todo/atomics/ATOMICS_SPEC.md`;
- `doc/todo/atomics/ATOMICS_IMPLEMENTATION_PLAN.md`;
- `spec/atomics/cbor-v1.json`; and
- `doc/reference/storage/FORMAT_SPEC.md`.

This is a new review document. Its change requests use the independent
`CR-ATMR3-*` namespace. Earlier `CR-ATM*` and `CR-R2-*` numbers are historical
evidence only and must not be used to track this delivery.

## Acceptance decision

### ATM-1

**Technically acceptable at `bcafefc`, subject to the package handoff being
regenerated.**

The reviewed implementation now has:

- immutable/canonical closed plans;
- oracle differential coverage across the required shapes and boundaries;
- authority revision separated from active rule revisions;
- current-authority and rights revalidation at admission;
- a non-forgeable public trust boundary for `TrustedAuthorityView`;
- frozen encoding profiles with canonical integer, decimal, UTF-8, and value
  validation;
- hard/configured limit checks and one-unit-over tests; and
- compile-fail proof that downstream crates cannot mint or elevate authority.

The absence of a production authority-view minting bridge is not an ATM-1
security defect: the constructor is deliberately unavailable until the
capability/SDK integration package. It must remain unavailable rather than be
made public for convenience.

### ATM-2

**Not accepted.**

The peer lane is a useful and significantly improved durability prototype. It
now validates before semantic persistence, authenticates intent/member/seal
linkage, bounds several hostile metadata fields, migrates the envelope key
registry, and has real directory reopen tests. It still does not implement the
ATM-2 contract:

- the durable prepare is synthetic and is not the prepare for an accepted
  `AtomicPlan`;
- valid not-committed evidence is misclassified;
- damaged evidence can be silently converted into clean absence;
- open is globally unbounded and scans entire logs into memory;
- durable chunked members are absent;
- there is no authoritative store/read-surface/examine integration;
- the peer lane has no exclusive writer ownership; and
- the required byte/phase I/O-failure matrix has not been run.

`Capabilities::atomics` must remain `false`. ATM-3 must not build publication or
decision semantics on this lane as an accepted storage contract.

## Verification performed

The following passed at the clean review baseline:

- `bash scripts/verify-atomics.sh full`;
- all targets in `residiuum-atomics`;
- all targets in `residiuum-format`;
- all targets in `residiuum-atomic-lane`;
- the store envelope migration unit tests;
- Clippy with warnings denied for atomics, format, and lane;
- full workspace formatting check;
- whitespace check; and
- detached evidence-run SHA-256 verification.

The full verifier generated an `acceptance_candidate` record. That label is not
an architecture acceptance decision and is currently too broad for ATM-2; see
CR-ATMR3-009.

Two temporary diagnostic tests were run and then removed:

1. corrupting the only persisted prepare frame allowed `DurableLane::open` to
   succeed with the Atomic absent; and
2. a canonical prepare plus canonical `not_committed` decision with the frozen
   vector semantics (`member_root = intended manifest`, `member_count = 0`, no
   staged member frames) was classified `Corrupt(BodyMismatch)`.

Both probes reproduced on `bcafefc`.

## Status of the previous repair themes

This table is informational only. The active work is identified exclusively by
the new CRs below.

| Repair theme | Current result |
| --- | --- |
| Validate before semantic persistence | Closed for the tested request-refusal cases; crash/I/O partial writes remain part of CR-ATMR3-008. |
| Intent/member/seal authentication | Improved, but the prepare is not derived from the plan and damaged-log coverage is unsafe. |
| Metadata and intent limits | Locally closed; total log, group, directory, and reopen work remain unbounded. |
| Public authority forgery | Closed with crate-private mint/elevation and downstream compile-fail tests. |
| Same-ID/different-content examination | Closed; frames now aggregate first by Heap and Atomic ID. |
| Envelope namespace collision | Implementation migrated to 41/42 with legacy reads; formal architect approval remains required. |
| Evidence self-hash and cross-commit inheritance | Self-hash fixed and inheritance is commit/toolchain scoped; package acceptance semantics and handoff remain weak. |
| Formatting and Clippy | Closed at the reviewed baseline. |

## New change requests

### CR-ATMR3-001 — RED — The persisted prepare is not derived from the accepted closed plan

Evidence:

- `DurableLane::begin_prepare` accepts an arbitrary `AtomicId`, arbitrary
  `ContentRoot`, and a slice of `AtomicMember`; it does not accept or validate an
  `AtomicPlan` or canonical `AtomicPrepare`.
- `build_lane_prepare` manufactures a frontier from Heap/content/manifest
  hashes, always uses empty read and predicate roots, always uses an empty rule
  root, and always records the hard LocalHeap limits.
- Those values are deterministic, but they are not the actual serialization
  frontier, read set, predicate set, active-rule revisions, or applied limits
  of the accepted plan.
- Nothing proves that the supplied `content_root` is the content root of the
  supplied members or of any plan at all.
- The test named `sealed_reopen_recovers_derived_prepare_not_placeholders`
  proves only that the new constants differ from the old byte-pattern
  placeholders. It does not prove derivation from a closed plan.

Impact:

The durable prepare can truthfully authenticate the lane's own files while
making false claims about the Atomic request it allegedly represents. A later
decision would commit to synthetic evidence rather than the accepted plan.
This breaks identity retry, authority/rule validation, read/predicate evidence,
limit disclosure, and serializability.

Required fix:

1. Define one internal conversion from an admitted `AtomicPlan` plus the bound
   serialization frontier and closed generated members to the exact
   `AtomicPrepare` and placement manifest.
2. Make the durable lane accept that closed/admitted object, not independently
   supplied IDs, roots, and member slices.
3. Recompute and verify `plan_content_root(plan)` at the persistence frontier.
4. Derive the read-set, predicate-set, active-rule-revision, manifest, and
   applied-limit roots/fields from the plan's canonical data.
5. Ensure generated history/index/RRE/relationship members eventually enter
   the same closed manifest rather than being appended outside the plan.
6. Remove `build_lane_prepare`'s synthetic frontier and empty-root assumptions
   from the authoritative path. A reduced prototype helper may remain only if
   its evidence is explicitly non-authoritative and cannot reach store media.

Acceptance proof:

- Independent recomputation from the original plan produces byte-identical
  prepare evidence after reopen.
- Mutating any plan field, frontier, read, predicate, rule revision, applied
  limit, member, or content root refuses before prepare or makes recovery report
  corruption.
- Assertion-only plans and mutation plans both retain their exact semantic
  roots.

### CR-ATMR3-002 — RED — Valid not-committed evidence is classified as corrupt

Evidence:

- The canonical evidence vector creates `not_committed` with the intended
  manifest root and `member_count = 0`.
- The durable protocol explicitly writes prepare plus not-committed decision
  after a precondition/rule failure and publishes no member.
- `classify_group` applies committed-material checks to both decision codes. It
  compares decision member count with observed member-frame count and then
  recomputes the manifest from the observed members.
- With no staged members, it hashes the empty observed list and compares that
  root with the prepare's non-empty intended manifest, producing
  `Corrupt(BodyMismatch)`.
- This exact outcome was reproduced by the temporary review probe.

Impact:

The examiner cannot represent one of the protocol's normal terminal outcomes.
A legitimate conflict or rule rejection becomes apparent damage, making retry,
administration, recovery, and future tombstone materialization unreliable.

Required fix:

Branch aggregation by `DecisionCode`:

- committed must have the complete durable member set matching the prepare and
  decision;
- not committed must validate prepare hash, intended manifest root, zero/no
  committed member requirement, absence of commit position, durable class, and
  required abort reason without requiring the intended members to exist; and
- any surviving staged members beside a not-committed decision must remain
  invisible and be reported as reclaimable material, not as committed data.

Acceptance proof:

- Golden tests for every `AtomicAbortReason` with prepare + decision and no
  member frames classify consistently as not committed.
- A not-committed decision with commit position, missing abort reason, wrong
  prepare hash, wrong intended root, or illegally published member fails the
  appropriate invariant.

### CR-ATMR3-003 — RED — Damaged logs can be silently converted into clean absence

Evidence:

- `refuse_midstream_damage` rejects a hole only when a later verified frame
  exists.
- A hole occupying the tail, or the whole file when no frame verifies, is
  accepted regardless of why verification failed.
- Reopen then reconstructs only verified frames and returns success.
- Bit-flipping the middle of the sole prepare frame reproduced a successful
  reopen in which `can_resolve(atomic_id)` was false.
- The current truncated-tail test proves only the desired torn-last-append
  case; it does not distinguish truncation at a known append frontier from
  checksum corruption or arbitrary lost evidence.

Impact:

Evidence damage is converted into "no valid prepare" without an honest
coverage/degraded outcome. Once decisions land, the same pattern could turn
damaged terminal evidence into an apparently unused ID and enable a different
execution attempt. This is a RED guessed-outcome/same-ID risk.

Required fix:

1. Define an authenticated log/checkpoint frontier that distinguishes a legal
   incomplete final append from damage to previously durable coverage.
2. Preserve and surface all scan holes with physical range and reason.
3. Treat checksum/body/prefix corruption inside authenticated coverage as
   damage, even when no later frame survives.
4. Permit truncation only where the durable append protocol proves the bytes
   were never part of an acknowledged boundary.
5. Feed the coverage state into examination and eventual Atomic status; never
   translate incomplete coverage into `not_found` or reusable identity.

Acceptance proof:

- Every one-byte corruption/truncation position in prepare, member, and seal
  material has an explicit expected outcome.
- Corruption of a previously stable sole frame is degraded/corrupt, not empty.
- A genuinely torn unacknowledged tail is ignored without inventing evidence.

### CR-ATMR3-004 — RED — Reopen remains globally unbounded

Evidence:

- `replay_prepares` uses `fs::read` on the entire coordinator log.
- `replay_members` uses `fs::read` on every shard log and collects every
  recovered member/payload in one `Vec` before applying it.
- The staging heap retains every recovered Atomic, manifest, and member in
  memory. There is no retained checkpoint/frontier or evidence index.
- `replay_seals` collects every seal in a `Vec`; directory entry counts are not
  bounded.
- Per-frame `SafetyLimits` and per-intent limits do not bound total media bytes,
  number of Atomics, total recovered members, directory entries, or recovery
  working memory.

Impact:

A large, corrupt, or hostile lane can force memory proportional to the entire
history and total payload population during open. This violates the explicit
RED rule for unbounded recovery and repeats the full-startup-scan failure mode
that the wider store work already had to eliminate.

Required fix:

1. Recover from a durable Atomic evidence checkpoint/index plus bounded tails.
2. Stream frame verification rather than reading complete logs into memory.
3. Apply total byte/frame/Atomic/member/directory/work-memory ceilings before
   allocation and report incomplete coverage honestly when a diagnostic limit
   is reached.
4. Do not load every payload merely to establish catalogue/status state;
   authenticate on demand or through retained verified summaries.
5. Add scale tests with many Atomics and large logs, not only hostile `u32`
   fields in tiny files.

Acceptance proof:

- Peak recovery memory and bytes scanned are reported and bounded independently
  of total database size on the normal path.
- A hostile multi-gigabyte sparse/log image cannot cause equivalent allocation.
- Recovery checkpoint corruption falls back to a bounded, explicitly reported
  path rather than an unbounded scan.

### CR-ATMR3-005 — RED — Chunked members have no durable lane

Evidence:

- `StagingHeap` models `commit_chunk_manifest` and `append_chunk`.
- `DurableLane` exposes only unchunked `append_staged` and stores one payload
  file per completed member.
- No durable chunk manifest, chunk frames/files, chunk failpoints, or chunk
  recovery path exists in `residiuum-atomic-lane`.
- The verifier's chunk tests exercise only the in-memory model.

Impact:

ATM-2 explicitly requires chunked-value member support with complete manifest
commitment. Large/chunked values cannot use the delivered durable protocol, and
the first stable boundary has no proof covering chunk durability.

Required fix:

Implement durable chunk-manifest and chunk append/reopen semantics using the
same closed member commitment. Validate per-chunk and assembled hashes, exact
member identity, ordering, total count/bytes, crash prefixes, and first stable
boundary. Alternatively, remove the peer lane from the acceptance claim and
deliver chunking directly in the authoritative store adapter.

### CR-ATMR3-006 — RED — ATM-2 is not connected to authoritative storage or read surfaces

Evidence:

- `residiuum-atomic-lane` deliberately has no dependency on
  `residiuum-store`, `residiuum-sdk`, `residiuum-examine`, or the server.
- No store code uses `DurableLane` or `StagingHeap`.
- No prepared frame is appended through real segment allocation/rotation,
  Recovery Shadow, backup, compaction, or store recovery.
- Only the model's ordinary map supplies `get` and `scan` visibility tests.
- RQL, history, watch, secondary-index, and real point-read invisibility are
  absent.
- No `residiuum-examine` projection consumes `AtomicRecoveryReport`.

Impact:

The prototype cannot prove the central ATM-2 claim: prepared state is invisible
through every real database surface while its evidence remains authoritative
through storage lifecycle operations.

Required fix:

1. Decide the integration boundary: the peer crate may provide mechanics, but
   `residiuum-store` must own authoritative paths, allocation, locks, recovery,
   and visibility.
2. Append prepares/members through real store segments and ownership admission.
3. Keep staged material out of every ordinary primary/secondary/history/watch
   projection.
4. Add a real examination projection with valid, partial, corrupt, conflict,
   coverage, and material-state reporting.
5. Exercise segment rotation and physical-cohort isolation.

Acceptance proof:

- Reopen of a real store image after every pre-decision prefix shows no staged
  value through point, scan, RQL, history, watch, or secondary index.
- The deliberate leak negative control fails each affected surface.
- Examination and store recovery agree on the same authoritative evidence.

### CR-ATMR3-007 — RED — The durable peer lane has no exclusive writer ownership

Evidence:

- `DurableLane::open` can be called repeatedly for the same directory.
- The crate acquires no process writer lock and receives no unforgeable proof
  that the store writer lock is held.
- Each opened instance reconstructs an independent in-memory coordinator and
  staged map, then appends to shared files.
- `write_exclusive` uses `exists/read` followed by a shared deterministic temp
  filename and rename, which is not an inter-process compare-and-create
  primitive.

Impact:

Two processes or two accidental lane instances can both validate against stale
state, race intent/payload publication, append duplicate or conflicting
prepares, and corrupt the evidence set. The architecture requires one physical
writer/scheduler domain per deployment, not a convention callers must remember.

Required fix:

Either acquire the authoritative store writer lock inside the lane lifetime or
make lane construction private to a store-owned writer token that cannot be
duplicated. Use crash-atomic exclusive creation primitives for identity-bound
files. Add two-process contention and stale-open tests for same-ID/same-root and
same-ID/different-root cases.

### CR-ATMR3-008 — AMBER — The ATM-2 crash matrix covers operations, not every I/O phase

Evidence:

- Public failpoints are only `before_prepare`, `after_prepare`, and
  `after_member_n` for this package.
- Tests reopen after completed, synced operations or manually mutate a small
  number of files.
- There is no injected short write, write error, sync error, rename error,
  directory-sync error, or process kill between intent/payload temp creation,
  file sync, rename, directory sync, log append, log sync, and seal publication.
- The existing store already has richer atomic-file failure machinery, but the
  peer lane does not use it.

Impact:

The ATM-2 exit requirement—crash or injected I/O failure at every byte/phase
before decision—is not evidenced. The implementation may be correct for the
tested prefixes, but package acceptance would be unsupported.

Required fix:

Create a phase-indexed failure harness around every durable operation and run
the full prefix matrix against real reopened media. Record whether each prefix
produces absence, prepared-invisible material, durable-invisible material, or
explicit damage. Include a mutant that omits each required sync/rename ordering
edge and prove the suite detects it.

### CR-ATMR3-009 — AMBER — Evidence and handoff can label an incomplete ATM-2 package as an acceptance candidate

Evidence:

- A clean `full` run writes `acceptance_candidate`, while the ATM-2 manifest
  itself says `prototype / peer crate` and `not_store = true`.
- Acceptance-family calculation is run-global, not package-specific, and does
  not check the ATM-2 deliverables listed in the implementation plan.
- It cannot detect missing durable chunks, store visibility surfaces,
  examination integration, writer ownership, or byte-phase crash coverage.
- The advertised direct command `scripts/verify-atomics.sh full` fails because
  the script is not executable; only `bash scripts/verify-atomics.sh full`
  works.
- `ATM1_ATM2_HANDOFF_2026-08-16.md` is stale: it still says store writers may
  emit operation identity at 31/32, describes the old verifier behaviour, and
  omits the new recovery/authentication changes and remaining findings.
- The verifier does not run all `residiuum-format` targets or the store envelope
  migration tests even though the package claims the global envelope amendment.

Impact:

Machine evidence can be internally green while the package is architecturally
incomplete. Developers and downstream governance may mistake a test-run label
for acceptance.

Required fix:

1. Give ATM-1 and ATM-2 independent acceptance matrices.
2. Make ATM-2 remain `partial` while `not_store = true` or any mandatory
   deliverable is declared absent.
3. Add format all-targets, store migration tests, durable chunk tests, damage
   tests, writer-lock tests, and real visibility tests to the relevant package
   matrix as they land.
4. Make the verifier executable or document invocation through `bash`
   consistently.
5. Regenerate the handoff from the accepted baseline with exact commit,
   formats, residuals, recovery effects, performance effects, and architecture
   decisions.

## Architecture dispositions required

These are governance decisions, not excuses for developers to weaken tests.

### Envelope key amendment

The reviewed implementation of ownership 31–36, Atomic 37–40, and operation
identity 41/42 is coherent. New writers use 41/42; readers distinguish legacy
31/32 operation identity from 16-byte collection ownership; Atomic ownership
admission and cross-Heap tests pass.

**Recommendation: approve this amendment**, subject to recording the approval
in `FORMAT_SPEC.md` and the new package handoff. The legacy dual-read path must
remain tested for the declared compatibility lifetime.

### Peer-lane boundary

**Recommendation: retain `residiuum-atomic-lane` as a prototype/mechanics and
test-oracle crate, but do not accept it as the final ATM-2 authoritative storage
home.** Store-owned paths, writer ownership, lifecycle integration, and all
ordinary visibility surfaces remain mandatory.

### Capability advertisement

Keep `Capabilities::atomics == false` until ATM-5 acceptance. No current finding
requires exposing a provisional public API.

## Delivery order

1. Fix CR-ATMR3-001 before adding decisions. A decision over synthetic prepare
   evidence would freeze the wrong contract.
2. Fix CR-ATMR3-002 and CR-ATMR3-003 before recovery is allowed to resolve an
   Atomic outcome.
3. Design the bounded checkpoint/tail recovery required by CR-ATMR3-004 before
   scaling or integrating the lane.
4. Resolve writer ownership and the store boundary under CR-ATMR3-006/007.
5. Implement durable chunks under CR-ATMR3-005 on that accepted boundary.
6. Run the complete I/O-prefix and visibility matrices under CR-ATMR3-008.
7. Regenerate honest, package-specific evidence and handoff under
   CR-ATMR3-009.

## Acceptance gates

ATM-1 acceptance record may be completed when:

- the envelope amendment disposition is recorded if ATM-1 evidence references
  it;
- the handoff and ATM-1 manifest are regenerated at one clean commit; and
- the compile-fail authority boundary, oracle differential, encoding corpus,
  limits, Clippy, and formatting remain green.

ATM-2 may be accepted only when:

- CR-ATMR3-001 through CR-ATMR3-009 are closed or explicitly dispositioned by
  architecture with no correctness weakening;
- exact plan-derived prepare bytes and the envelope registry are frozen;
- normal reopen is bounded and damage coverage is honest;
- durable unchunked and chunked material pass every pre-decision I/O prefix;
- real store point/scan/RQL/history/watch/index paths prove prepared
  invisibility;
- writer ownership prevents concurrent physical coordinators;
- examination agrees with store recovery for valid, partial, corrupt,
  conflicting, and incomplete-coverage material; and
- a clean, package-specific evidence manifest and current handoff are reviewed
  and signed off.

