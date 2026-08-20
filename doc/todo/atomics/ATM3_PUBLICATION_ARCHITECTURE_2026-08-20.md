# ATM-3 publication architecture

Status: implementation baseline, 2026-08-20

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
complete delta from authenticated prepare/member/payload evidence, applies it
to a private clone of every affected read projection, and swaps the complete
generation while holding the store publication guard. No reader can observe a
partly mutated live index. A crash discards the derived generation; reopen
reconstructs it from the decision and its complete evidence.

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
3. `ATSEAL1` is appended after the complete member set and payload bytes. Its
   durable append is the member stable boundary. There is no per-member sync.
4. `BatchCommit` carries canonical `AtomicDecision`. The store appends it only
   after revalidating the sealed evidence at the serialization frontier. Its
   durable append is the decision stable boundary and linearization point.
5. The catalogue checkpoint is an authenticated acceleration structure, never
   authority. A crash before it is replaced is recovered from the segment
   tail.

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
The writer prepares replacement primary/history/derived projections privately,
then installs all of them under one publication guard. A reader sees the prior
generation or the replacement generation. It cannot retain one index from each
generation. The first implementation may serialize readers briefly; later RCU
or `Arc` generations may remove that contention without changing semantics.

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

## Lock and latency rule

Canonical plan construction, value cooking, compression, rule compilation,
and application/network waits occur before entering the store publication
guard. Under the guard the engine performs bounded current-state validation,
commit-position allocation, ordered durable appends, private delta assembly,
and one generation install. Group commit may share the member and decision
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

`Capabilities::atomics` remains `false` until all slices and the ATM-3 exit gate
are green.
