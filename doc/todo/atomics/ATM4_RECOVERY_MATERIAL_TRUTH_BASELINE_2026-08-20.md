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
7. Every new terminal decision now carries an `ATTOMB1` lifetime tombstone in
   the same durable prefix. Recovery backfills a missing tombstone from an
   exact surviving decision. The authenticated v16 checkpoint binds a derived,
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
pre-publication rule applies to `ATCKP1` v16, `ATCRD1` v2 and the heap-qualified
sidecars: older experimental private layouts are rebuilt where authoritative
media permits and are never interpreted as the new composite-key format.

## Remaining delivery blocks

### ATM-4A — lifetime identity and detail retention — in progress

- **Delivered:** compact Heap-qualified tombstone beside every terminal
  decision; recovery backfill; tombstone-only status/replay; 90-day and
  stronger-obligation calculation; legal-hold refusal; restart proof for
  lawfully retired not-committed detail; authenticated 64-KiB paged lifetime
  index with bounded point lookup and a constant-size checkpoint descriptor.
- **Remaining:** committed-detail reclamation must wait for ATM-4C to provide a
  qualified material/publication representation. The current staging
  payload/member locators are still live database material and MUST NOT be
  deleted merely because decision detail expires.
- **Remaining:** qualify incremental index maintenance at production lifetime
  cardinality. Point reads and ordinary unchanged checkpoints are bounded;
  adding a new key currently rebuilds the derived sorted index. This is an
  update-cost delta, not a status-correctness or checkpoint-growth gap.

### ATM-4B — material truth and damage

- distinguish complete, partial, missing, conflicting and coverage-incomplete
  material for every prepare/member/payload/seal/decision/tombstone cut;
- retain healthy-member examination under partial committed damage;
- refuse damaged-index absence and uniqueness claims;
- add byte-flip, truncation, hole and conflicting-decision mutants.

### ATM-4C — maintenance journeys

- copy through or reconstruct Atomic authority across compaction, Recovery
  Shadow, backup, restore, clone, salvage, scrub and tier movement;
- replace the temporary maintenance fences only where a qualified journey
  exists;
- retain identity, commit position and tombstones across same-identity restore.

### ATM-4D — serial histories and predicates

- exact version, absence and bounded exact-range predicates;
- read-your-plan construction overlay;
- randomized concurrent history recorder plus independent serial checker;
- lost update, write skew, ABA, phantom, uniqueness, ordinary/Atomic,
  authority-change and disjoint/overlapping Atomic anomaly corpus.

### ATM-4E — Heap-qualified identity key — delivered

The catalogue and all dependent physical keys use the exact composite
`(HeapId, AtomicId)`. `ATCKP1` v16, `ATCRD1` v2 and every non-prepare staging
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
