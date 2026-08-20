# ATM-4C maintenance journeys specification — 2026-08-20

Status: `implementation_in_progress`

ATM-4C proves that a maintenance operation cannot erase, alias, duplicate or
invent Atomic authority. Maintenance may change physical placement and derived
representations. It may not change the Heap-qualified logical history.

## 1. Preservation invariant

For every issued identity `(heap_id, atomic_id)`, an accepted maintenance
journey preserves:

```text
logical decision
content root
decision hash
commit position
lifetime tombstone
material truth (or an honestly weaker result)
healthy-member examination
same-root retry result
```

Physical paths, segment identities, indexes and derived catalogues may change.
An exact move is recognized only by authenticated prefix or whole-object
identity; filenames and operator intent are not evidence.

## 2. Admission classes

| Source state | Read-only maintenance | Copy/backup | Relocation | Reclaim/rewrite |
|---|---:|---:|---:|---:|
| no Atomic evidence | allow | allow | allow | allow |
| healthy terminal decision and complete coverage | allow | allow | allow after proof | allow only with an authenticated replacement generation |
| issued prepare without a decision | allow examination | allow exact copy | refuse | refuse |
| damaged/conflicting attributable evidence | allow examination | allow exact copy | refuse unless byte-identical | refuse |
| incomplete/unattributable coverage | allow examination | allow exact copy | refuse | refuse |

Opening a normal writer may first resolve a complete-coverage undecided prepare
to the specified durable recovery-abort. A maintenance call on an already-open
handle does not silently make that decision merely to bypass a fence.

## 3. Journey matrix

### 3.1 Seal and physical relocation

- Moving an authenticated active prefix into a sealed or tier path preserves
  the exact bytes and their coverage frontier.
- Recovery may heal a stale physical path only when exactly one discovered
  candidate authenticates the complete covered prefix.
- Zero or multiple candidates are not guessed; coverage remains incomplete.
- Every payload/chunk locator is rebound to the authenticated replacement path.

### 3.2 Compaction

- Non-reclaiming compaction may run with healthy terminal Atomics because the
  authoritative sources remain present.
- Source reclaim requires a verified replacement containing everything needed
  for ordinary values, Atomic status/retry, index rebuild, examination and
  same-identity restore.
- The replacement generation becomes authoritative before any source is
  removed. Every crash cut exposes either the old generation or the complete
  new generation.
- Damaged, conflicted, undecided or incomplete Atomic evidence blocks reclaim.
- The delivered v1 replacement is a dedicated immutable stream under
  `store-info/atomic-authority/`, not a value-only compact segment. It contains
  byte-identical verified Atomic frames and is reconstructed into an independent
  catalogue before checkpoint publication.
- The checkpoint swap is the authority linearization point. Its superseded-path
  set prevents mixed old/new ingestion while deletion is incomplete; completed
  deletion prunes obsolete generations and markers so repeated compaction stays
  bounded.

### 3.3 Recovery Shadow

- A protected Shadow carries Atomic frames byte-identically or through a
  versioned representation that authenticates the same identities and hashes.
- Mode activation requires gap-free coverage including Atomic-bearing
  segments. A value-only Shadow is not sufficient.
- Recovery from Shadow must reproduce the same two-axis statuses and receipts.

### 3.4 Backup and restore

- A same-identity full backup contains Atomic media, checkpoint, coordinator,
  tombstone index and maintenance-generation descriptors.
- Restore verifies every package object before publication and preserves Heap
  identity, decisions, positions, tombstones, values and retry results.
- A crash or failed verification cannot publish a partial destination as a
  valid restored store.

### 3.5 Clone and new identity

- Identity-reassign clone never aliases source Atomic authority into the new
  Heap.
- Until a provenance-bearing historical-evidence profile is implemented, a
  package containing any issued Atomic identity is refused before destination
  publication. This refusal is the qualified v1 behavior.

### 3.6 Salvage

- Evidence salvage copies every verified Atomic frame byte-identically and
  records holes and corrupt regions.
- A new-lineage salvage destination does not activate source Heap authority.
  Source evidence remains examinable as provenance/foreign-Heap evidence.
- Salvage never converts partial evidence into a complete decision or clean
  current state.

### 3.7 Scrub

- Scrub is read-only with respect to authoritative Atomic bytes.
- Findings cover Atomic media and remain attributable where authenticated
  linkage survives.
- Repair/coverage clearing requires exact authenticated replacement media; a
  coincidental pathname or value is insufficient.

### 3.8 Tier movement

- Segment identity and whole-object hash remain stable across copy/move.
- Atomic recovery discovers every online configured tier and heals checkpoint
  paths by authenticated identity, including after catalog rebuild.
- An offline Atomic-bearing tier yields `coverage_incomplete`, never absence.

### 3.9 Tombstone-index page reclamation

- Superseded copy-on-write pages are reclaimed only by a generation swap:
  build complete new pages, authenticate the root, durably publish the new
  descriptor, then retire old pages.
- Crashes before descriptor publication use the old root; crashes after it use
  the new root. Neither path scans obsolete-page count.

## 4. Required qualification

The `ATM-MNT` family must cover:

1. committed and not-committed decisions through every supported journey;
2. same-root retry and next commit position after each same-identity journey;
3. ordinary value visibility and healthy-member examination;
4. unresolved, damaged, conflicted and incomplete sources refusing every
   destructive journey before mutation;
5. active-to-sealed and hot-to-warm/cold/archive path healing, including stale
   checkpoints and deleted placement catalogues;
6. every failpoint around replacement creation, verification, activation,
   descriptor publication and source retirement;
7. same-identity backup/restore and new-identity clone refusal;
8. salvage with clean frames, holes and foreign-Heap provenance;
9. scrub before/after exact media return and a non-matching negative control;
10. Recovery Shadow rebuild equivalence;
11. tombstone-index generation-swap head/middle/tail corruption and crash cuts;
12. negative controls proving that omitted authority, locator rebinding or a
    premature source delete is detected.

The clean full verifier must label ATM-MNT as an acceptance candidate. Public
Atomics remain disabled until ATM-5.

## 5. Delivery status at 2026-08-20

Delivered and locally qualified:

- terminal complete Atomics survive destructive live-projection source reclaim;
- replacement material is compared across decisions, roots, members,
  payload/chunk hashes, positions, order frontiers, coordinator sequence,
  findings and lifetime-tombstone cardinality before source deletion;
- restart, status, receipt, value, same-root retry and repeated reclaim remain
  exact;
- failpoints immediately before/after checkpoint swap and after a source delete
  reopen with complete authority and can resume reclaim;
- an injected omitted Atomic frame refuses before source deletion;
- same-identity backup/restore and evidence salvage include authority-only media;
- superseded authority generations are pruned after successful reclaim.
- not-committed authority survives reclaim with its exact abort reason, no
  receipt, exact same-root retry and no consumed commit position;
- chunk plans and every chunk-body hash survive replacement reconstruction,
  source deletion, restart, value publication and same-root retry;
- attributable member damage with a healthy sibling is classified as partial
  and refuses reclaim before a compaction job or source mutation exists;
- Atomics spread across multiple sealed source segments are collected into one
  verified generation without collapsing identity or commit order; the next
  commit resumes at the exact global Heap frontier;
- replacement comparison ignores only benign `Valid` scan observations, which
  are reconstruction provenance rather than durable damage. Every adverse
  finding is compared as an order-independent multiset.
- CompactShadow publishes an authenticated `RSHATM01` bundle containing the
  exact consolidated Atomic generation, checkpoint, coordinator and optional
  tombstone index before retiring an Atomic-bearing source Shadow;
- missing sealed segments are restored byte-identically from verified
  RSHD0003/RSHD0004 images before authoritative inventory, while Atomic bundle
  restoration verifies first and publishes its checkpoint last;
- full Shadow-only rebuild preserves ordinary values, committed and
  not-committed status, receipts, chunk maps/bodies, same-root retry and the
  next global Heap commit position;
- transition/activation/rollback, corrupt-needed versus corrupt-unused bundle
  behavior, four restoration crash cuts and both sides of bundle publication
  are qualified. Replacement coverage retires old sealed identities without
  leaving a false frontier gap;
- online external tier roots participate in pre-mutation collision inventory
  and Atomic media discovery. Checkpoint deletion followed by reopen rebuilds
  the exact Atomic from its externally moved segment;
- a missing online external segment is restored from its exact Shadow at the
  configured tier path, never duplicated into hot. An offline external tier is
  not read or repaired, remains explicit incomplete coverage, and cannot prove
  ordinary absence. With independent authority, the Atomic remains materially
  complete; without it, the checkpoint preserves `Committed` while material is
  `CoverageIncomplete`, emits no receipt and never leaks raw `ENOENT`;
- destructive compaction resolves external sources through the current
  canonical tier path, physically removes them before reporting reclaimed,
  preserves authenticated absolute supersession markers through the swap, and
  reopens on the replacement generation.

Still open in ATM-4C:

- none. ATM-4C implementation and focused qualification are complete; final
  acceptance remains subject to the clean full Atomics verifier.

Delivered in the closing slice:

- the append-only `ATTOMB2` index now reclaims obsolete COW pages only through
  a complete streamed generation rewrite. It does not collect the lifetime
  table in memory and does not scan obsolete pages;
- the existing checkpoint root is also the physical generation identity. The
  prior file is retained under a root-derived authenticated name, the new file
  is synced and published, the checkpoint descriptor is then swapped, and only
  non-selected generations are retired;
- old checkpoints remain wire-compatible. A crash before descriptor
  publication resolves the old root from either the primary or root-derived
  path; a crash after publication resolves the new root;
- head, middle and tail corruption of an unpublished generation cannot erase
  the checkpoint-selected old authority;
- canonical crash boundaries cover new-generation sync, old-generation
  archival, new-generation publication and both sides of retirement;
- three-source destructive reclaim now has distinct middle-source and
  last-source deletion cuts. Both reopen on the complete replacement, preserve
  all decisions/values/receipts, and resume to `Reclaimed` exactly.
