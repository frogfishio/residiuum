# ATM-3 publication architecture

Status: implementation baseline, 2026-08-20

Implementation checkpoint (2026-08-20): ATM-3A and ATM-3B are committed.
ATM-3C now has locator-backed whole-generation primary and history publication,
plus writer-open and strictly read-only inspection reconstruction. Published
puts resolve directly from authenticated `ATPAY1` frame locators; payload
bodies are not retained in either derived projection. History merges committed
members at the ordinary-write gap named by `ATORD1`, retains overwritten Atomic
payload locators, and excludes prepared, aborted, and decision-less evidence.
Multi-member publication, decision-before-publish, publish-before-ack, ordered
history across reopen, and inspection-without-checkpoint-write cases are
covered. This is not yet ATM-3 acceptance: the complete concurrent/crash proof
and grouped member-boundary optimization remain open. Universal ordinary-write
ordering now has its first implemented and tested witness profile, described
below.

The guarded-reader proof now runs point, full logical scan, and history readers
concurrently with a two-member commit. Every sampled view is either the prior
generation or the complete committed generation. The SDK/RQL façade proof also
executes against SDK-created documents through a capability-bound Heap. Embedded
Core RQL now obtains a complete collection page under one physical-store lock
instead of composing a key inventory with independently locked point reads; a
two-record replacement therefore yields either both old documents or both new
documents, never a mixed page.

That integration proof also froze the distinction between two key encodings:
Atomic manifest identity/order bytes retain their leading kind tag, while the
SubjectV2 application-key field receives the collection's Heap-profile payload
bytes. Thus an SDK UTF-8 key and an Atomic `CanonicalKey::String` address the
same physical record without weakening cross-kind manifest identity.

Universal-order amendment (2026-08-20): checkpoint profile v13 adds an
`ATORD1` decision witness. Immediately before a committed decision, the Store
captures `(shard, active segment id, next writer sequence)` for every physical
writer shard and appends the witness on coordinator shard zero without a
barrier. The following durable `BatchCommit` flushes both witness and decision
in one ordered decision boundary. This is not a timestamp and does not add a
third sync.

During reconstruction, a current ordinary event whose segment/sequence is
beyond the witnessed frontier for its subject's home shard outranks that
Atomic member. Otherwise the committed member is applied. Atomics themselves
are replayed in per-Heap commit-position order. Consequently Atomic → ordinary
put, Atomic → ordinary delete, and overlapping Atomic → Atomic histories retain
their order even after deleting the derived primary index. An `ATORD1` without
a decision publishes nothing; a committed decision without its complete order
witness is damaged evidence and cannot be guessed into visibility.

This record satisfies the mandatory ATM-3 design review in
`ATOMICS_IMPLEMENTATION_PLAN.md` section 9. It is deliberately narrower than
ATM-4 recovery/status and does not enable the product capability.

## Decision

The valid durable `AtomicDecision` is the sole linearization point. Prepared
members and payload sidecars are never ordinary item events and never become
visible merely because their bytes survive. A committed decision names the
exact prepare hash, ordered member root, member count, and one non-zero Heap
commit position.

Publication is a derived, whole-Atomic projection. The store constructs a
complete delta from authenticated prepare/member/payload evidence and
preflights every fallible semantic condition before publication. It then
applies the O(member-count) delta to primary, history, locator, and derived
projections while holding the physical publication guard. It does not clone
the O(database-size) primary indexes or retained history. No reader can
interleave with a partly applied delta. A crash discards the derived state;
reopen reconstructs it from the decision and its complete evidence.

We explicitly reject a design that writes ordinary member events after the
decision. A crash between such events would let ordinary index rebuild expose
a committed subset. We also reject retaining every committed value only as an
in-memory body: that makes RSS proportional to dataset size and defeats the
existing locator model. ATM-3 publication therefore uses authenticated Atomic
payload locators as a first-class durable locator class.

## Media and ordered boundaries

1. `BatchPrepare` carries the canonical `AtomicPrepare` in the coordinator
   stream on store segment media.
2. Atomic `ItemEvent` frames carry canonical `AtomicMember` records. `ATPAY1`,
   `ATMAP1`, and `ATCHK1` payload frames carry value bytes or frozen chunks.
   None participates in ordinary indexes before a valid committed decision.
3. On the whole-plan commit path, prepare/member/payload frames are submitted
   with buffered durability and no checkpoint refresh. `ATSEAL1` is appended
   after the complete member set and payload bytes; its durable append flushes
   that entire prefix and is the first stable boundary. There is no per-member
   sync. The separately exposed low-level/manual staging surface deliberately
   retains record-by-record durability for forensic qualification and is not
   the product commit path.
4. `ATORD1` captures every writer shard's ordinary-event frontier and is
   appended without a separate stable boundary.
5. `BatchCommit` carries canonical `AtomicDecision`. The store appends it only
   after revalidating the sealed evidence at the serialization frontier. Its
   durable append covers `ATORD1`, is the decision stable boundary, and is the
   linearization point.
6. The catalogue checkpoint is an authenticated acceleration structure, never
   authority. The whole-plan path does not refresh it between members, seal,
   decision, or acknowledgement; a stale checkpoint is recovered from the
   segment tail. A later catalogue open may checkpoint that tail independently
   of the decision's two authoritative boundaries.

The current embedded writer has one exclusive physical `Store` mutation guard.
ATM-3 assigns Heap commit positions while that guard is held. Ordinary writes,
Atomic decisions, and lifecycle changes that affect predicates must use the
same monotonically ordered frontier before the capability can be enabled.
Commit position allocation is reconstructed as one greater than the maximum
valid durable committed decision; an allocation is never acknowledged before
its decision is durable.

The catalogue may cover evidence for many named Heaps in one physical store,
but commit-position high-water marks are partitioned by `HeapId`. A physical
connection, writer, and sync cohort are shared infrastructure; they are not a
logical Heap identity. Authenticated decision-envelope ownership is retained
even when related evidence is damaged so a position named by damaged evidence
cannot later be reused in that Heap.

## Rotation and multi-shard fencing

Prepare, member evidence, payload sidecars, seal, and decision are admitted by
the store-owned Atomic stage. Rotation, compaction, backup, restore, and other
maintenance remain fenced while outstanding evidence exists. ATM-3 may relax
that conservative fence only after all shards can prove the same stable member
boundary and their media are retained by the decision manifest. Publication
never depends on an unsealed active tail.

## Reader generations

Point reads, scans, RQL, history, and derived-index readers bind one generation.
The first implementation serializes publication and readers on the physical
store guard. A reader therefore completes against the prior state or begins
against the committed state; it cannot retain one projection from each. SDK
Core RQL materializes an embedded collection page under that same guard. Later
RCU or `Arc` generations may remove the brief serialization without changing
semantics.

## Committed but unpublished recovery

On open, normal item reconstruction runs first. Atomic catalogue recovery then
classifies every decision. A committed decision is publishable only when its
prepare hash, member root/count, seal, and every required payload verify. The
store applies publishable decisions in Heap commit-position order as complete
deltas. A valid commit with damaged or missing member material is reported as
committed-partial and the Heap is degraded; it is never translated to
not-committed and no subset is published. Conflicting decisions or positions
also degrade the Heap.

The same path repairs a crash after decision and before in-memory publication.
Publication has no second authority and therefore needs no "published" durable
bit.

The implementation exposes the mandatory crash cuts at `before_decision`,
`after_decision`, `before_publish`, `after_publish`, and `before_ack`. Every cut
before the durable decision may recover as not committed; every cut after that
linearization point must reconstruct the entire committed generation. An
acknowledgement failure never authorizes rollback, and exact retry returns the
stored terminal decision.

## Lock and latency rule

Canonical plan construction, value cooking, compression, rule compilation,
and application/network waits occur before entering the store publication
guard. Under the guard the engine performs bounded current-state validation,
commit-position allocation, ordered durable appends, complete-delta preflight,
and guarded O(member-count) publication. Group commit may share the member and decision
stable boundaries across independent outcomes, but each decision and receipt
remains independently authenticated.

## Delivery slices

- ATM-3A: admit and checkpoint decisions; durable commit-position high-water;
  exact decision replay/conflict classification; decision failpoints.
- ATM-3B: authoritative validation against the live Heap frontier; durable
  committed/not-committed outcomes and byte-equivalent receipts.
- ATM-3C: Atomic payload locator support and whole-generation primary/history
  publication; committed-unpublished reopen repair.
- ATM-3D: ordinary-write participation in the Heap order, scan/RQL concurrency
  proof, grouped stable boundaries, crash-prefix and resource qualification.

The primary and history projections now share the same durable order witness,
including later ordinary puts/deletes and overlapping Atomics after a full
derived-index rebuild. The five mandated decision/publication/ack crash cuts
are green, as is guarded concurrent point/scan/history observation. ATM-3D must
still close the resource-bound qualification before the hidden qualification
path can become a product capability. SDK/RQL generation binding is now green
on the real embedded façade. Whole-plan I/O has an executable invariant: one-
and 256-member commits both complete with exactly two authoritative syncs,
proving sync count is independent of member count.

`Capabilities::atomics` remains `false` until all slices and the ATM-3 exit gate
are green.
