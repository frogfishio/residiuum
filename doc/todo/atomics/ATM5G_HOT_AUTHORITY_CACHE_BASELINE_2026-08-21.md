# ATM-5G hot Atomic authority cache baseline — 2026-08-21

Status: **implemented and locally qualified; product capability remains gated**

This checkpoint removes catalogue reconstruction from the normal consecutive
Atomic execution path. It does not replace or weaken durable authority. The
authenticated, copy-on-write, 4 KiB paged Atomic tombstone index and canonical
segment/checkpoint media remain the lifetime truth; the Store writer handle now
retains an authenticated in-memory projection as a bounded hot accelerator.

## Architecture

One physical writer Store owns one hot Atomic authority cache containing:

- the authenticated `StageCatalog` and its covered-file frontier;
- recovery findings and the last authenticated open report; and
- a separate `StagingHeap` kernel for every Heap used through that connection.

Product operations borrow the requested Heap kernel and explicitly return it
to the Store after every success or terminal error. This explicit lifecycle is
intentional: raw/qualification stage handles preserve their previous drop and
borrow semantics, while the smart-client product path gets stable reuse.

The cache is never accepted as independent authority:

- a new store performs one real Atomic bootstrap open so durable control and
  checkpoint media exist before restart;
- writer startup recovery authenticates media, repairs as required, and seeds
  the cache from that already-opened catalogue;
- an ordinary put, delete, batch, or operation cohort invalidates the cache so
  the next Atomic tails and authenticates the new ordinary history before
  evaluating conditions;
- compaction relocation invalidates it because covered paths/frontiers change;
- custom recovery-limit stages bypass and discard it, so a hot cache cannot
  evade a qualification or hostile-media ceiling; and
- an operation-level `Err` discards the borrowed hot state so the next attempt
  reconstructs uncertain or partially written media instead of trusting it;
- read-only recovery does not install mutable hot state.

Alternating `tinker -> gremlin -> tinker` Atomics through one physical client
is covered by a driver journey. It records one catalogue miss and two hits,
while proving identical application keys remain Heap-isolated. A separate
ordinary-write/Atomic interleave proves a stale Atomic condition is refused
after the ordinary write rather than evaluated against the cached projection.

## Observability

`AtomicStoreStats` now reports exact cumulative:

- `catalog_cache_hits`; and
- `catalog_cache_misses`.

The qualification evidence records their per-cell deltas alongside catalogue
latency. A zero-load `AtomicStageOpenReport` identifies a hit; a media open or
reconstruction is a miss.

## Local dipstick evidence

Evidence file:
`/tmp/residiuum-atomic-qual-hot-cache-20260821-e.atomic-qual.json`

Release build, five iterations per cell, local development machine:

| Members | Value bytes/member | Commits/s | Member mutations/s | p50 end-to-end | p50 catalogue | Cache hit/miss | Writes/commit | Syncs/commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0 | 85.11 | 85.11 | 9.12 ms | 1.291 us | 4 / 1 | 2 | 2 |
| 1 | 8,192 | 131.99 | 131.99 | 6.99 ms | 0.917 us | 5 / 0 | 2 | 2 |
| 3 | 256 | 124.07 | 372.20 | 8.36 ms | 0.458 us | 5 / 0 | 2 | 2 |
| 10 | 8,192 | 100.51 | 1,005.09 | 9.89 ms | 0.500 us | 5 / 0 | 2 | 2 |
| 64 | 256 | 52.80 | 3,379.45 | 18.61 ms | 0.833 us | 5 / 0 | 2 | 2 |
| 256 | 0 | 17.19 | 4,400.67 | 56.83 ms | 1.042 us | 5 / 0 | 2 | 2 |

The first cell deliberately contains the one cold bootstrap: its catalogue p95
is 16.71 ms while four warm opens are approximately one microsecond. Every
later completed cell is entirely cache-served. The structurally impossible
`1 x 1 MiB` application-payload cell remains honestly skipped because the
canonical plan envelope also has a 1 MiB ceiling.

Against ATM-5F, the 256-member path rises from 9.38 to 17.19 commits/s and from
2,400 to 4,401 member mutations/s, while p50 falls from 104.82 to 56.83 ms.
Catalogue p50 falls from 58.02 ms to 0.00104 ms. The durability shape remains
exactly two gathered writes and two sync boundaries per commit.

These are local diagnostic figures, not controlled product benchmarks.

## What this does not claim

This checkpoint does not make all committed Atomic detail permanently resident
or claim that every retained-detail structure has been converted to a paged
index. The durable terminal-identity authority already uses the authenticated
paged B+tree; further retained-detail paging remains a future scale package.

It also does not make an ordinary-write/Atomic mixed workload cache-hit by
magic. Ordinary mutation invalidation deliberately chooses correctness first;
the subsequent Atomic performs an authenticated incremental tail. A later
optimization may update the hot kernel directly, but only with equivalent
serial-history and damage proofs.

## Remaining measured work

With catalogue reconstruction removed, the 256-member median is now dominated
by plan validation, member-boundary construction/submission, decision-boundary
work, and publication. Those phases are the next optimization targets. They
must be attacked independently without changing the two-boundary protocol or
turning the in-memory cache into recovery authority.

## Verification

The checkpoint passes:

- 24 Atomic frontier/decision crash, recovery, history and durability tests;
- 7 bounded Atomic-stage checkpoint, tailing and damage tests;
- 7 authenticated paged tombstone-index tests (the million-identity test stays
  explicitly ignored as a large qualification run);
- 14 embedded smart-driver tests, including restart, external signal-9,
  multi-Heap isolation and ordinary/Atomic interleaving;
- 2 qualification-runner unit tests; and
- `cargo check --workspace --all-targets --all-features`.

`Capabilities::atomics` remains `false`. ATM-5G is a performance architecture
and evidence checkpoint, not the final ATM-5 acceptance decision.
