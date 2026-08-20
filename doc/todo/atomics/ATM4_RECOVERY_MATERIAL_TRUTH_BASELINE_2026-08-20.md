# ATM-4 recovery and material-truth baseline — 2026-08-20

Status: `in_progress`

ATM-3 is an accepted implementation candidate. Public Atomics remain disabled.
ATM-4 proves that its decision remains exact after dirty shutdown, media damage,
evidence retirement, maintenance, restore and conflicting concurrency.

## Acceptance rule

ATM-4 is accepted only when `ATM-DMG`, `ATM-RET`, `ATM-MNT` and the ATM-4
serial-history portion of `ATM-ISO` pass from a clean full verifier run. A known
decision may never be guessed from material, and material availability may
never be inferred from a decision.

## Delivered in the first ATM-4 recovery push

1. `AtomicPrepare.member_count` is authoritative durable evidence.
   The prior format authenticated the manifest root but omitted its cardinality,
   making an exact recovery decision impossible after a crash between prepare
   and the first member/checkpoint. CBOR prepare label 11 closes that hole and
   the evidence vectors have been regenerated.
2. A normal writer open deterministically closes every complete-coverage,
   unblocked prepare without a decision as durable
   `not_committed/recovery_abort`. It never resumes caller intent and never
   makes the ID reusable. Read-only inspection remains non-mutating.
3. Dirty-open resolution is bounded by the existing authenticated Atomic
   checkpoint plus dirty tails and reports `atomic_stage_recovery_aborts`.
4. The Store now projects the independent `AtomicStatus` logical and material
   axes. `NotFound` requires complete coverage; incomplete coverage is
   `unknown_commit/coverage_incomplete`; committed receipts are attached only
   when exact member material is reconstructible.
5. Existing crash-prefix qualification now reflects the frozen ATM-4 rule:
   before the decision boundary recovers not committed; at or after a durable
   committed decision recovers committed.
6. Physical Atomic identity is now `(HeapId, AtomicId)` throughout the
   catalogue, coordinator, checkpoint, recovery, status, retry and publication
   paths. Payload, seal, chunk and order-frontier sidecars carry the Heap ID,
   and their derived event IDs bind it. Two named Heaps can issue and commit the
   same caller-selected Atomic ID independently across restart. Conflicting
   material in one Heap blocks only that composite identity and does not poison
   the same Atomic ID in another Heap.
7. Every new terminal decision now carries an `ATTOMB2` lifetime tombstone in
   the same durable prefix. Recovery backfills a missing tombstone from an
   exact surviving decision. The authenticated v17 checkpoint binds a derived,
   fixed-page Merkle index over the composite-key summaries. Tombstone-only
   status and same-root replay work after
   detailed not-committed evidence is lawfully retired and after restart.
8. `AtomicDetailRetentionPolicy` freezes the 90-day minimum and computes the
   maximum of configured detail, Heap-history, RRE-evidence and backup-contract
   horizons. Active legal hold refuses retirement. Tombstone deletion is not
   exposed outside complete Heap purge.

## Format amendment

This is a deliberate pre-publication amendment to the private Atomic evidence
profile. The product capability is still false, so no released SDK could have
created supported Atomic data. New prepares require CBOR field 11
`member_count: uint`; old experimental prepares that omit it are not silently
upgraded because their exact intended cardinality is unknowable. The same
pre-publication rule applies to `ATCKP1` v18, `ATCRD1` v2 and the heap-qualified
sidecars: older experimental private layouts are rebuilt where authoritative
media permits and are never interpreted as the new composite-key format.

## Remaining delivery blocks

### ATM-4A — lifetime identity and detail retention — delivered except ATM-4C dependency

- **Delivered:** compact Heap-qualified tombstone beside every terminal
  decision; recovery backfill; tombstone-only status/replay; 90-day and
  stronger-obligation calculation; legal-hold refusal; restart proof for
  lawfully retired not-committed detail; authenticated copy-on-write 4-KiB
  B+tree with bounded point lookup and a constant-size checkpoint descriptor;
  O(tree-height) insertion; crash-suffix rollback to the prior root; linear
  bulk reconstruction; one-million-identity scale proof.
- **Remaining:** committed-detail reclamation must wait for ATM-4C to provide a
  qualified material/publication representation. The current staging
  payload/member locators are still live database material and MUST NOT be
  deleted merely because decision detail expires.
- **Remaining with ATM-4C:** reclaim superseded copy-on-write index pages using
  a crash-qualified generation swap. This is physical space reclamation only;
  lookup, insertion and restart costs do not grow with obsolete-page count.

### ATM-4B — material truth and damage — delivered and qualified

- **Delivered:** frozen two-axis truth table in
  `ATM4B_MATERIAL_DAMAGE_TRUTH_SPEC_2026-08-20.md`; an exact durable decision is
  never rewritten by later material damage, and complete material never
  invents a decision.
- **Delivered:** Heap-qualified persisted findings with distinct prepare,
  member, payload, chunk-plan, chunk-body, seal, order-frontier, decision and
  tombstone roles. Unattributable damage degrades global coverage rather than
  a guessed identity.
- **Delivered:** format-level `AttributedCorrupt` retains authenticated
  Heap/Atomic linkage when the envelope survives a damaged role body.
- **Delivered:** `ATTOMB2` repeats the Atomic ID in its fixed header and checks
  it against canonical tombstone CBOR, so a damaged lifetime body remains
  attributable. `ATCKP1` v18 persists the Heap-qualified finding.
- **Delivered:** committed status survives prepare/member/payload/chunk/seal,
  order, decision and tombstone damage with an independently exact logical
  result and honest `complete`, `partial`, `missing`, `conflicting` or
  `coverage_incomplete` material result.
- **Delivered:** a damaged detailed decision is reconstructed only when the
  surviving prepare and lifetime tombstone reproduce its exact decision hash;
  complete detail returns the byte-equivalent replay receipt.
- **Delivered:** a one-member cut in a two-member commit retains the healthy
  sibling for examination across checkpoint reopen and full `Store::open`.
  Degraded Atomic publication is skipped and counted rather than preventing
  access to the entire healthy store; missing values are never fabricated.
- **Delivered:** damaged lifetime-index lookup cannot prove absence or hide a
  detailed decision. Missing authenticated coverage returns
  `unknown_commit/coverage_incomplete`, never `not_found`.
- **Qualified:** head/middle/tail physical flips, tombstone
  truncations, missing covered media, conflicting payload/seal/order/chunk
  material, damaged prepare/member/decision/tombstone, conflicting decisions
  in both file orders, checkpoint persistence and an executed negative
  control. The delivery commit's clean full verifier run is the acceptance
  record.

### ATM-4C — maintenance journeys

- **Delivered in the first push:** the frozen journey/admission matrix in
  `ATM4C_MAINTENANCE_JOURNEYS_SPEC_2026-08-20.md`.
- **Delivered:** healthy terminal Atomics may seal, relocate and run
  source-retaining live compaction. Undecided, damaged, conflicting or
  incomplete evidence still refuses before relocation.
- **Delivered:** stale active/sealed/tier checkpoint paths heal only when one
  discovered candidate authenticates the complete covered prefix. Payload and
  chunk locators are rebound to that exact physical replacement; ambiguous or
  missing candidates remain incomplete.
- **Delivered:** locally managed tier moves discover Atomic-bearing segment
  media without trusting the derived placement catalogue. Whole-object hashes,
  status, receipt and value survive hot-to-tier move and restart.
- **Delivered:** same-identity full backup/restore preserves the decision,
  commit position, complete material, binary Heap SubjectV2 values and
  same-root replay. Restore verification now uses the binary-key path rather
  than the legacy UTF-8 scan façade.
- **Delivered:** evidence salvage retains source Heap authority as inactive
  foreign-Heap evidence; it is examinable but is not activated in the new
  lineage. Scrub is qualified read-only for healthy terminal evidence.
- **Delivered:** destructive source reclaim publishes a dedicated byte-exact
  Atomic authority generation, reconstructs and materially verifies it, then
  checkpoint-swaps before deleting a source. Superseded paths close the
  mixed-generation crash window and are pruned after deletion. Restart, retry,
  receipt, backup/restore, salvage, repeated reclaim, omission negative control
  and three destructive crash cuts are green.
- **Delivered:** the expanded reclaim matrix covers exact not-committed
  rejection/no-position semantics, chunk maps and bodies, attributable partial
  damage refusing before mutation, and multiple sealed source segments while
  preserving the global Heap commit frontier. Benign valid scan observations
  are normalized out of replacement comparison; adverse findings remain an
  exact order-independent multiset.
- **Qualified fail-closed:** identity-reassign clone and CompactShadow
  transition refuse before mutation while any issued Atomic identity exists.
  The current value-only Recovery Shadow loses Atomic authority, so the
  transition remains explicitly fenced.
- **Remaining:** Recovery Shadow authority carriage and rebuild; external
  tier-root discovery/offline truth; tombstone-index page generation reclaim;
  and expanded multi-source deletion crash cuts.

### ATM-4D — serial histories and predicates

- exact version, absence and bounded exact-range predicates;
- read-your-plan construction overlay;
- randomized concurrent history recorder plus independent serial checker;
- lost update, write skew, ABA, phantom, uniqueness, ordinary/Atomic,
  authority-change and disjoint/overlapping Atomic anomaly corpus.

### ATM-4E — Heap-qualified identity key — delivered

The catalogue and all dependent physical keys use the exact composite
`(HeapId, AtomicId)`. `ATCKP1` v18, `ATCRD1` v2 and every non-prepare staging
sidecar carry the Heap component. No hash-derived naming convention is used as
an isolation boundary. Tombstones introduced by ATM-4A MUST use the same
composite key.

## Red lines

- no `NotFound` under incomplete coverage;
- no retry execution after any durable prepare has been issued;
- no inferred commit from complete members and no inferred abort from missing
  members;
- no detail pruning before a durable exact tombstone exists;
- no maintenance operation may discard or alias an Atomic identity;
- no public capability until ATM-5 completes against accepted ATM-4 evidence.
