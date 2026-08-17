# ATM-1 / ATM-2 delivery acceptance review

Date: 2026-08-16  
Review baseline: `9eeae8c` plus the uncommitted ATM-2.4 failpoint delivery  
Normative authority: `ATOMICS_SPEC.md` and `ATOMICS_IMPLEMENTATION_PLAN.md`

## Re-review addendum — 2026-08-18

Review baseline: clean `08f3483` (`origin/main`)
Reviewed fixes: `e53f0e8..08f3483`

### Updated decision

**ATM-1: changes still required.** The encoding-profile work closes
CR-ATM1-002, and authority revision is now represented separately from active
rule revisions. However, the alleged trusted authority source is a fully public,
caller-constructible Rust value. An application can still invent a Heap,
revision, collection grant, rights, and encoding profile and then use that value
to manufacture a `BoundCollection` or pass admission. CR-ATM1-001 is therefore
only partially closed.

**ATM-2: not accepted.** The delivery is substantially better than the first
submission: Atomic bodies are decoded, the in-memory manifest binds exact
members and payloads, chunk limits exist, and a file-backed peer lane now
exercises reopen. It is still a prototype rather than an authoritative storage
implementation, and the prototype contains write-order and recovery-binding
defects that can turn refused or altered input into durable Atomic evidence.

`Capabilities::atomics` correctly remains `false`. ATM-3 publication must not
consume this lane as an accepted durability contract yet.

### Gate results

Passed:

- `scripts/verify-atomics.sh full`;
- all targets for `residiuum-atomics`, `residiuum-format`, and
  `residiuum-atomic-lane`;
- whitespace/error-marker check; and
- clean-tree check before this review document was edited.

Failed:

- Clippy with warnings denied: `residiuum-atomics/src/encoding.rs` has a
  needless explicit lifetime in `field`;
- workspace formatting check: committed trailing/line-layout differences in
  `residiuum-format/src/atomic.rs` and `tests/atomic_admit.rs`; and
- the generated `runs/full.json` does not match the SHA-256 that the file
  records for itself.

Passing the project verifier does not override the substantive RED findings
below because several of the failing behaviours are outside its current test
oracle.

### Original CR disposition

| CR | Re-review status | Disposition |
| --- | --- | --- |
| CR-ATM1-001 | **PARTIAL / RED** | Authority/rule semantics are separated and stale/revoked tests exist, but the trusted authority view remains forgeable through the public API. |
| CR-ATM1-002 | **CLOSED** | Encoding profiles, canonical integer/decimal checks, invalid UTF-8/value refusal, and negative tests now exist. |
| CR-ATM2-001 | **PARTIAL / RED** | A durable peer lane exists, but it is not authoritative store staging and its validate/persist order is unsafe. |
| CR-ATM2-002 | **PARTIAL / RED** | Atomic ownership admission is composable, but the global key migration is not complete or accepted; live store still emits operation identity at 31/32. |
| CR-ATM2-003 | **PARTIAL / RED** | Typed bodies are decoded and cross-checked, but malformed ownership and same-ID/different-root conflicts are misclassified. |
| CR-ATM2-004 | **CLOSED IN MODEL** | Exact member, member hash, payload hash, ordinals, and manifest root are bound in `StagingHeap`; durable replay binding remains open under CR-R2-002. |
| CR-ATM2-005 | **CLOSED IN MODEL** | Chunk allocation and empty-value defects are fixed in `StagingHeap`; durable file parsing remains unbounded under CR-R2-003. |
| CR-ATM2-006 | **PARTIAL / AMBER** | Verifier and handoff exist, but their acceptance and hash accounting are not reliable enough for package evidence. |

### CR-R2-001 — RED — The durable lane persists before validation

Evidence:

- `DurableLane::begin_prepare` writes/replaces the intent file and appends the
  prepare frame before calling `StagingHeap::begin_prepare`.
- Therefore a duplicate Atomic ID, conflicting content root, invalid member
  set, or other kernel refusal can occur only after authoritative-looking bytes
  have already been synced.
- `DurableLane::append_staged` similarly writes/replaces the payload and
  appends/syncs the member frame before `StagingHeap::append_staged` validates
  exact member equality, duplicate ordinal, and payload hash.
- A duplicate or malformed append can consequently return an error while
  leaving a new payload and member frame on disk. The payload filename is only
  `(atomic_id, ordinal)`, so the rejected call can overwrite the prior payload.

Impact:

Structural refusal is required to produce no evidence. The current ordering
does the reverse: it makes validation failure a post-persistence event. This
also permits a rejected retry to damage previously valid staged state.

Required fix:

1. Split validation/reservation from mutation in the kernel/lane contract.
2. Validate the complete prepare or member, including duplicate-ID and
   duplicate-ordinal state, before touching a durable path.
3. Make durable creation non-overwriting for an existing Atomic identity unless
   byte-for-byte idempotence has been established.
4. If a failure can occur after the first write, introduce an explicit abort or
   repair protocol whose evidence is authoritative; do not silently leave a
   refused prepare/member behind.
5. Add injected failures and semantic-refusal tests that compare the complete
   directory image before and after the refused operation.

Acceptance proof:

- Every prepare/member mutation mutant returns refusal with an identical
  pre/post directory image.
- Same ID/same root is either proven idempotent or explicitly refused without
  new bytes; same ID/different root is permanently conflicting and never
  overwrites the original intent or payload.

### CR-R2-002 — RED — Reopen does not authenticate intent, members, or seal evidence

Evidence:

- `replay_prepares` decodes `AtomicPrepare`, then loads `intent/<atomic_id>` and
  calls `begin_prepare`; it never recomputes the intent's ordered member root or
  compares it with `AtomicPrepare.ordered_member_manifest_root`.
- The durable prepare is not compiled from an `AtomicPlan`. `persist_prepare`
  fills `frontier`, read-set root, predicate-set root, and active-rule root with
  fixed placeholder byte arrays.
- `replay_members` does not explicitly require the examined envelope Heap,
  Atomic ID, content root, and ordinal to agree with the decoded member and the
  recovered prepare before reading the payload. Some mismatches are caught by
  the model later, but the durable recovery boundary itself is not closed.
- `sealed/<atomic_id>` contains only `boundary\n`. Reopen trusts the filename
  and presence, without a content root, manifest root, checksum, version, or
  validation of the marker contents.
- Crash tests cover directory-image reopen after whole synced operations, not
  torn writes or every byte/phase of intent, prepare, payload, member, and seal
  publication.

Impact:

Replacing or corrupting an intent can redefine the member cohort recovered for
an otherwise valid prepare. A stray or damaged seal filename can promote that
cohort to the first stable boundary. The placeholder roots also mean the
persisted prepare is not evidence for the actual closed plan.

Required fix:

1. Persist the canonical `AtomicPrepare` derived from the accepted closed plan;
   remove all placeholder roots.
2. On reopen, recompute and compare the ordered member manifest, count, Heap,
   Atomic ID, content root, ordinals, exact member records, payload hashes, and
   shard placement before reconstructing the lane.
3. Make the stable-boundary record a versioned, checksummed record bound to the
   Heap, Atomic ID, content root, and manifest root, or derive the boundary from
   equally strong retained evidence.
4. Classify malformed, partial, unsupported, and conflicting evidence without
   silently skipping it or promoting it.
5. Test byte truncation/corruption/replacement at every durable file and append
   phase, followed by actual reopen.

Acceptance proof:

- Mutating any intent member or the seal contents cannot reconstruct a sealed
  prepare.
- Recovery of accepted material produces the exact original plan/prepare and
  member manifest; recovery never invents placeholder state.

### CR-R2-003 — RED — Durable recovery accepts unbounded metadata and intent lengths

Evidence:

- `read_intent` trusts a media-supplied `u32` member count for
  `Vec::with_capacity` and trusts each `u32` member length without applying hard
  resource limits.
- It does not reject trailing bytes after the declared members.
- `parse_shard_count` accepts any `u32`; create/open/replay then create or scan
  paths in a loop up to that value.

Impact:

A small corrupt or hostile store image can cause excessive allocation, file
creation, or path scanning during open. This restores the unbounded-input class
that CR-ATM2-005 closed only in the in-memory chunk API.

Required fix:

Apply frozen hard limits before allocation or iteration, use checked arithmetic,
require exact input consumption, and add hostile `u32::MAX`, one-unit-over,
trailing-byte, and truncated-field reopen fixtures.

### CR-R2-004 — RED — Trusted authority remains publicly forgeable

Evidence:

- `TrustedAuthorityView` is publicly re-exported.
- Its public `new`, `grant`, and `grant_with_encoding` methods accept arbitrary
  Heap IDs, authority revisions, rights, and encoding profiles.
- `BoundCollection::from_trusted` and `admit_closed_plan` accept that same
  caller-created value.
- This contradicts the type documentation that application code cannot mint a
  bound collection except through trusted capability state.

Impact:

The type shape prevents accidentally mixing a grant and a handle, but it does
not establish a trust boundary. Any dependent application can fabricate the
authority that both compilation and admission trust.

Required fix:

Construction of authority state must be crate-private/SDK-internal or require
an unforgeable authority token supplied by the actual capability subsystem.
Keep a clearly marked test-only constructor behind `cfg(test)` or a dedicated
non-production feature. Test the public consumer surface with a compile-fail
case proving it cannot mint or elevate trusted grants.

### CR-R2-005 — RED — Examination can split conflicts into valid groups

Evidence:

- `parse_fields` maps every ownership parse result other than `Known` to
  `heap_id = None`; malformed or missing mandatory ownership can therefore
  reach `AtomicEvidenceClass::Valid`.
- `aggregate_groups` keys groups by `(atomic_id, content_root, heap_id)`. Two
  records with the same Heap/Atomic ID but different content roots are placed
  into separate groups, so `classify_group` never sees the conflict.

Impact:

The examiner can report two individually plausible groups instead of the
protocol's permanent same-ID/different-content conflict, and can label an
unowned Atomic frame valid. That is unsafe for damage reporting and future
recovery decisions.

Required fix:

Require valid mandatory ownership on every Atomic frame. Detect conflicts first
by `(heap_id, atomic_id)`, then validate the single allowed content root and
role evidence within that identity. Add mutants for missing/malformed ownership
and same ID with different roots across prepare, member, and decision frames.

### CR-R2-006 — RED — The envelope namespace remains contradictory in live store code

Evidence:

- `FORMAT_SPEC.md` assigns keys 31/32 to Heap/collection ownership and proposes
  operation identity at 41/42.
- `residiuum-store/src/envelope.rs` still documents, encodes, and decodes
  operation ID/content hash at 31/32.
- The amendment is explicitly labelled proposed and architect-unaccepted in
  the handoff.

Impact:

The same durable key bytes still have two meanings in the repository. Atomic
format acceptance would freeze a namespace that the current store contradicts.

Required fix:

Approve one migration policy, move the store writer to the accepted keys, and
implement explicit legacy read discrimination. Golden bytes and reopen tests
must cover old ordinary frames, new ordinary frames, and Heap-owned Atomic
frames without ambiguous interpretation.

### CR-R2-007 — AMBER — Evidence manifests can certify stale or self-inconsistent results

Evidence:

- `merge_pack` carries forward prior families and verifier profiles without
  requiring their commit, dirty state, toolchain, suite version, or artifact
  hashes to match the current run, while replacing the package's top-level
  metadata with the current values.
- Every clean profile, including partial `quick`, is labelled
  `accepted_candidate`; required family coverage is not enforced before that
  label is written.
- The run manifest is written, hashed, has that hash appended to itself, and is
  written again. The recorded hash for `runs/full.json` therefore cannot equal
  the final file. This was reproduced at `08f3483`.
- The verifier checks for negative controls by source-code name, not by proving
  each control actually failed under a deliberately broken implementation.

Impact:

A clean partial run can inherit results from another commit and present them as
one accepted candidate. The manifest's self-integrity claim is false by
construction.

Required fix:

1. Make every run immutable and commit-scoped.
2. Build an acceptance package only from runs whose commit, dirty state,
   toolchain/suite identity, and required artifact hashes agree.
3. Distinguish `diagnostic`, `partial`, and `acceptance_candidate`; require the
   complete acceptance family matrix for the last state.
4. Remove the self-hash or hash a detached/canonical payload and store the hash
   outside that payload.
5. Execute negative controls or mutants and record their expected failing
   outcome; do not use symbol-name presence as evidence.

### CR-R2-008 — AMBER — Mechanical quality gates are not clean

Fix the Clippy lifetime warning and run `cargo fmt --all`. Acceptance evidence
must include Clippy with warnings denied, formatting, and whitespace gates, not
only the scoped test profiles.

### Re-review acceptance order

1. Fix CR-R2-001 first; no further durability proof is meaningful while a
   refused operation can write or overwrite evidence.
2. Close CR-R2-002 and CR-R2-003, then run the full corrupt/torn/reopen matrix.
3. Freeze and migrate the envelope namespace under CR-R2-006.
4. Close the authority boundary under CR-R2-004; then ATM-1 can be accepted.
5. Close examination and evidence-accounting under CR-R2-005/007/008.
6. Integrate the accepted lane with the authoritative store and prove prepared
   invisibility through point, scan, RQL, history, watch, and index surfaces.

The current delivery should continue to be described as **ATM-1 nearly
complete; ATM-2 durable prototype under review**, not ATM-1/ATM-2 accepted.

## Decision

**ATM-1: changes required before acceptance.** The canonical plan, accounting,
limit differential, typed operation vocabulary, and basic Heap-bound builder
are a strong implementation start. Authority binding and collection encoding
validation are not yet sound enough to freeze the package.

**ATM-2: not accepted.** The delivered staging code is explicitly an in-memory
model. It is useful as a model/test fixture, but it is not the authoritative
store staging, durable evidence, recovery, or examination implementation
required by ATM-2. There are also protocol-integrity defects inside the model
and an incompatibility between the new format envelopes and existing live-store
ownership admission.

The package must not be represented as ATM-2 complete, and ATM-3 store work must
not build on the current staging contract until the RED items below are closed.

## Review scope and gates run

Reviewed:

- ATM-1 commits `74f5127`, `02171e2`, and `e65626f`;
- ATM-2 commits `c74109c`, `5035107`, and `9eeae8c`;
- the current uncommitted ATM-2.4 failpoint changes;
- the normative Atomic specification and delivery plan;
- the format ownership/admission path and wire-format specification; and
- absence/presence of store, SDK, examination, CI, and evidence integration.

Mechanical gates passed:

- `cargo test -p residiuum-atomics --all-targets`;
- `cargo test -p residiuum-format --all-targets`;
- Clippy for both crates with warnings denied;
- workspace formatting check; and
- whitespace/error-marker check.

Passing these gates does not establish ATM-2 acceptance because the tests run
against an in-memory map/clone rather than authoritative files, indexes,
reopen, or recovery.

## Required changes

### CR-ATM2-001 — RED — ATM-2 authoritative staging and durability were not delivered

Evidence:

- `crates/residiuum-atomics/src/staging.rs:3-6` explicitly describes a pure
  in-memory kernel with no file, store, SDK, or index publication.
- `crates/residiuum-atomics/src/failpoints.rs:5-7` explicitly says reopen is a
  clone and that this is not a store I/O simulator.
- There is no use of `StagingHeap`, the new envelope encoders, or the Atomic
  staging API in `residiuum-store`, `residiuum-sdk`, or `residiuum-examine`.
- `seal_member_boundary` changes an enum to `DurableInvisible`; it performs no
  write, flush, sync, stable-boundary coordination, or reopen verification.

Impact:

- No authoritative `BatchPrepare` or member is written to media.
- No writer-shard placement is exercised against actual segment rotation.
- No existing point, scan, RQL, history, watch, or secondary-index path is
  proven to suppress prepared members.
- The crash gate has not been exercised: process loss discards this model, and
  cloning memory is not reopen or recovery.

Required fix:

1. Keep the current kernel only as a reference/model if useful; move store
   ownership out of the pure protocol crate.
2. Implement the designated per-Heap coordinator and staged member append lane
   in `residiuum-store` using authoritative segment/file abstractions.
3. Append canonical `AtomicPrepare` and `AtomicMember` bodies and establish a
   real first stable boundary across every touched file.
4. Route all ordinary read surfaces through the real visibility rule and add
   negative tests for point get, scan, RQL, history, watch, and secondary
   indexes.
5. Replace clone-based failpoints with process/reopen or faithful store-image
   crash prefixes at `before_prepare`, `after_prepare`, and every
   `after_member_n` position.

Acceptance proof:

- Tests must reopen a real store image and show zero ordinary-visible mutation
  after every pre-decision crash prefix.
- The negative-control build/test that publishes one prepared member must fail.
- Evidence must identify the authoritative files and sync operation forming the
  first stable boundary.

### CR-ATM2-002 — RED — Atomic envelopes are incompatible with Heap ownership admission

Evidence:

- `crates/residiuum-format/src/atomic.rs:136-194` emits envelopes containing
  only keys 37-40.
- `crates/residiuum-format/src/ownership.rs:70-86` rejects every key above 36
  as `UnknownKey`.
- `crates/residiuum-format/src/admit.rs:56-60` converts that parse failure to
  `RejectMalformed`.
- The Atomic envelope helpers do not include keys 31/34 (Heap identity and
  ownership profile in the current ownership implementation), so even a parser
  that ignored keys 37-40 would not obtain frame-level ownership from them.
- `doc/reference/storage/FORMAT_SPEC.md:180-181` requires unknown keys to be
  ignored by readers, while the ownership parser rejects them.
- The format specification's published key table does not contain the new
  Atomic keys and currently assigns keys 31/32 to operation identity, while the
  ownership implementation uses 31-36 for a different registry.

Impact:

Any frame produced by the new Atomic envelope encoders is rejected by the
existing live-store admission path. The durable format registry is internally
contradictory, and a recovery-only test bypasses the exact integration point
that fails.

Required fix:

1. Freeze one authoritative envelope-key registry across `FORMAT_SPEC`, format
   constants, ownership, operation identity, and Atomics.
2. Amend the format through the specification-amendment procedure: proposed
   diff, compatibility/recovery analysis, fixtures, and architect approval.
3. Provide composable envelope construction so ownership plus Atomic linkage
   live in one deterministic CBOR map without duplicate/colliding keys.
4. Make ownership parsing retain/ignore understood extension namespaces as the
   format contract requires while still rejecting malformed ownership fields.
5. Add live-admission tests proving prepare, member, and decision frames are
   admitted only to their bound Heap and rejected cross-Heap.

Acceptance proof:

- Independent golden bytes for all three frame roles.
- Old-reader/new-reader compatibility disposition.
- A test that passes each golden frame through `admit_frame_to_heap`, segment
  append, scan, and recovery examination.

### CR-ATM2-003 — RED — Recovery examination labels unverified bodies as valid evidence

Evidence:

- `crates/residiuum-format/src/atomic.rs:265-270` checks coordinator bodies only
  for “non-empty deterministic CBOR”.
- `classify_linkage` then reports `AtomicEvidenceClass::Valid` without decoding
  an `AtomicPrepare` or `AtomicDecision` and without checking its required
  fields, decision shape, hashes, Heap, manifest root, member count, durability,
  or abort reason.
- The positive fixture uses the unrelated body `{1: 1}` for both prepare and
  commit and expects both to be valid.
- Member bodies are not decoded as canonical `AtomicMember` records at all.

Impact:

Malformed or forged evidence can be reported as valid. Recovery cannot safely
distinguish committed, not committed, partial, corrupt, conflicting, or
unsupported material and therefore cannot use this reader as authority.

Required fix:

1. Decode frame bodies using the frozen `AtomicPrepare`, `AtomicMember`, and
   `AtomicDecision` codecs.
2. Cross-check envelope linkage against the decoded body's ID, root/prepare,
   ordinal, decision, and commit-position rules.
3. Aggregate records by `(heap_id, atomic_id, content_root)` and verify prepare
   hash, ordered member root/count, role uniqueness, and decision consistency.
4. Preserve partial/corrupt/unsupported evidence without promoting it to
   `Valid`.
5. Add independent byte fixtures, not encoder-to-decoder round trips using the
   same implementation.

Acceptance proof:

- Mutating every required body field makes the expected record corrupt or
  partial.
- `{1: 1}` and other merely-valid CBOR bodies are never valid Atomic evidence.
- Conflicting decisions and cross-linked members produce explicit degraded
  examination outcomes, never a guessed decision.

### CR-ATM2-004 — RED — Staged material is not bound to the frozen manifest

Evidence:

- `PlacementEntry` records only ordinal, shard, collection, key bytes, and an
  after-content hash; it does not commit member kind, before-version, event ID,
  or the canonical member hash.
- `append_staged` checks ordinal/collection/key but does not compare the
  incoming member's after hash with the manifest entry and does not hash the
  supplied payload at all.
- `seal_member_boundary` checks only member count and a weak completeness
  predicate; it never recomputes payload hashes, member hashes, or the ordered
  manifest root.
- The existing test deliberately stages `b"secret"` against an unrelated fake
  hash and passes.
- `append_chunk` finds placement by ordinal but does not compare the incoming
  member's object identity or full member record with that placement. On later
  chunks it finds the stored ordinal and ignores semantic differences in the
  newly supplied member.

Impact:

A prepare can name one payload/member while the staged lane installs another.
A later decision would then attest to bytes that were never proven durable,
breaking the central Atomic evidence invariant.

Required fix:

1. Freeze a manifest entry containing or deriving the canonical
   `member_hash(member)`, payload/content commitment, intended shard, and all
   identity necessary for recovery.
2. Refuse duplicate/non-contiguous ordinals and duplicate object identities.
3. On every unchunked append, require exact member equality with the frozen
   entry and verify `BLAKE3(payload) == after_content_hash`; delete must have no
   payload.
4. On every chunk append, require exact member equality, verify each chunk and
   full reassembly, and bind the chunk manifest into the prepare/member
   commitment.
5. At the stable boundary recompute the ordered member/placement commitment and
   compare it with the canonical prepare before declaring durability.

Acceptance proof:

- Mutants for payload, kind, before-version, after hash, event ID, target,
  ordinal, shard, chunk order, chunk count, and full-payload hash all refuse.
- The current fake-hash positive test must be replaced with a real matching
  content hash.

### CR-ATM1-001 — RED — Authority revision is encoded as an active rule revision

Evidence:

- `AtomicBuilder` collects collection authority revisions in
  `authority_revisions`.
- `AtomicBuilder::build` writes that vector into
  `AtomicPlanParts.active_rule_revisions`.
- The test asserts this substitution as desired behavior.
- `active_rule_revisions` has a distinct normative meaning and feeds the
  prepare's active-rule-revision root. `PredicateKind::HeapAuthorityRevision`
  already exists for the authority/security meaning.
- `validate_closed_plan` receives only a Heap ID. It has no execution-time
  authority revision or per-collection capability context and cannot detect a
  capability that became stale after plan construction.

Impact:

The content root mislabels security state as rule state, corrupting semantic
identity and future RRE checks. More importantly, authority is checked only
against caller-constructible builder handles, not revalidated at the admission
or serialization frontier as required.

Required fix:

1. Give authority revision an explicit canonical representation, normally the
   frozen `HeapAuthorityRevision` predicate or an approved plan field.
2. Keep active RRE revision hashes semantically separate.
3. Carry a per-collection rights requirement or equivalent closed authority
   requirement so execution can validate the union against trusted capability
   state.
4. Revalidate Heap authority, collection rights, and lifecycle using trusted
   current state before durable acceptance and again at the specified
   serialization point where required.
5. Prevent application code from manufacturing trusted `BoundCollection`
   rights/revisions; construction must be SDK-internal or require a trusted
   authority token.

Acceptance proof:

- Revoking or changing authority after build but before admission refuses with
  no prepare.
- A forged/stale collection handle cannot grant rights.
- Changing authority changes the plan root through the authority meaning, while
  active rule revisions remain unchanged.

### CR-ATM1-002 — AMBER — Collection key/value encoding contracts are not validated

Evidence:

- `CanonicalValue::serialize` accepts every byte string and cannot return
  `InvalidValue`.
- `BoundCollection` carries no frozen encoding/schema contract or trusted
  encoder.
- Integer and decimal keys accept caller-supplied “already canonical” byte
  vectors without a canonicality check in the builder.
- The ATM-1 refusal contract requires invalid key/value encodings to fail before
  prepare.

Impact:

Different encodings of the same logical key/value can enter different content
roots or evade collection encoding rules. The builder cannot currently satisfy
its invalid-value refusal claim.

Required fix:

1. Bind the collection's frozen key/value encoding profile to the trusted
   collection handle.
2. Make serialization/validation fallible and canonicalize from typed input or
   verify already-encoded bytes under that profile.
3. Validate canonical mathematical integer and decimal encodings, including
   zero/sign/minimal-width rules.
4. Add alias/noncanonical and wrong-schema negative fixtures.

Acceptance proof:

- Equivalent logical input produces one byte representation and one root.
- Noncanonical integer/decimal and collection-invalid payloads refuse before
  prepare with no evidence.

### CR-ATM2-005 — RED — Chunk admission permits unbounded allocation and mishandles empty values

Evidence:

- `ChunkPlan::validate` checks only `total >= 2` and vector-length equality; it
  does not apply plan/store byte or member/chunk limits.
- `append_chunk` allocates `vec![None; total]` directly from that value.
- `member_payload_complete` treats a chunked member as complete only when the
  assembled payload is non-empty, so a valid zero-length value represented by
  empty chunks can never seal.

Impact:

Hostile input can cause memory exhaustion before bounded admission. The
programme classifies unbounded input as RED. Empty values also have inconsistent
behavior between unchunked and chunked paths.

Required fix:

1. Add hard and configured limits for chunk count, per-chunk bytes, total
   reassembled bytes, and allocation before allocating any vector/body.
2. Use an explicit completeness state rather than non-empty payload as the
   marker.
3. Charge chunk manifests, bodies, generated members, and recovery working
   memory to the closed plan/admission budget.
4. Add one-unit-over and hostile `u32::MAX` tests that terminate without large
   allocation.

Acceptance proof:

- Bounded-memory hostile corpus and zero-length-value round trip.

### CR-ATM2-006 — AMBER — Required package evidence and handoff are absent

Evidence:

- Only `target/atomics-evidence/atm-0` exists.
- There is no top-level `scripts/verify-atomics.sh` with quick/crash/model/full
  profiles.
- No ATM-1/ATM-2 manifest records commit, dirty state, toolchain, platform,
  seeds, commands, durations, results, or artifact hashes.
- No handoff records changed durable formats, compatibility/recovery impact,
  residuals, performance, or requested architecture decisions.
- The current ATM-2.4 delivery is uncommitted.

Required fix:

Implement the evidence/CI contract in implementation-plan section 13 and
provide the package handoff required by section 14. Evidence generated from a
dirty tree is diagnostic only and cannot be the accepted package record.

## Additional required ATM-2 deliverables not yet present

These are not duplicates of the defects above; they are missing scope from the
ATM-2 delivery definition:

- a real examination projection in `residiuum-examine` for valid, partial,
  corrupt, and unsupported Atomic evidence;
- byte-level recovery-reader fixtures independent of the production writer;
- complete manifest commitment across actual writer shards and rotations;
- proof that physical cohort neighbours cannot acquire another Atomic's
  identity in the real writer;
- cross-Heap negative tests through actual ownership/admission, not two separate
  in-memory maps; and
- crash or injected I/O failure at every byte/phase before decision with reopen
  against authoritative media.

## What is accepted as useful work

The following work should be retained, subject to the CRs above:

- immutable closed plan representation and canonical ordering;
- mutation and public-assert typed encoders;
- limit/accounting differential tests covering 1/2/10/64/256 members,
  1/16/64 collections, mixed mutations, assertion-only plans, maximum values,
  and one-unit-over cases;
- same-key/different-collection and same-name/different-Heap oracle tests;
- the staging kernel as a non-authoritative model/test oracle;
- initial failpoint names and visibility-negative test structure; and
- reservation of an Atomic envelope extension, once the global key registry
  and compatibility procedure are completed.

## Re-review acceptance checklist

ATM-1 may be accepted when:

- CR-ATM1-001 and CR-ATM1-002 are closed;
- the authority and encoding mutants fail as specified; and
- an ATM-1 evidence manifest and clean package handoff are supplied.

ATM-2 may be accepted when:

- CR-ATM2-001 through CR-ATM2-006 are closed;
- exact persistent bytes and the envelope namespace are approved and frozen;
- real store staging and stable-boundary recovery pass the complete pre-decision
  crash matrix;
- every ordinary read/index surface proves prepared invisibility;
- independent examination agrees with authoritative evidence; and
- a clean ATM-2 evidence manifest and compatibility/recovery handoff are
  supplied.

Until then, `Capabilities::atomics` must remain `false`, no durable Atomic
format should be published, and ATM-2 should be described as a prototype/model
rather than a delivered storage package.
