# ATM-5F qualification dipstick baseline — 2026-08-21

Status: **implemented and locally qualified; product capability remains gated**

This checkpoint adds a reproducible public-driver qualification runner and
records the first measured Atomic member/payload dipstick. It is diagnostic
evidence, not a published product benchmark or ATM-5 acceptance.

## Reproduce

Build and run against a new path. The runner refuses an existing store or
evidence path and retains both for inspection.

```text
cargo run --release -p residiuum-testrig --bin residiuum-atomic-qual -- \
  --root /tmp/residiuum-atomic-qual \
  --iterations 3 \
  --profile dipstick
```

Use `--profile member-payload` for the declared 6 × 5 matrix. Structurally
invalid cells are reported as skipped rather than silently changed.

The runner uses the async smart driver and a capability-bound named Heap. It
reports commits and member mutations per second, end-to-end and store phase
percentiles, exact authoritative write bytes/calls, sync boundaries, durability
cohorts and write amplification as JSON.

## Correctness fixes found by measurement

1. The eight-Atomic admission limit was accidentally applied to all historical
   Atomics instead of outstanding Atomics. A store therefore failed after its
   eighth lifetime commit. Admission and recovery budgets now count only
   non-terminal work; a restart test commits and resolves twelve identities.
2. The driver converted every non-preacceptance-classified store error into
   `Unknown`. It now performs authoritative status resolution: complete
   `NotFound` returns the real error, while accepted, committed, incomplete or
   uncertain evidence remains `Unknown`.
3. Buffered Atomic evidence submitted every frame separately, producing
   `2 × member_count + 5` physical writes per commit. The single-plan product
   path now gathers independently checksummed frames and submits one contiguous
   tail at each of the two existing durable boundaries. This changes syscall
   granularity, not the logical failure unit, frame authentication, ordering or
   recovery prefixes.
4. Payload locators previously assumed file length equalled logical segment
   length. Gathering made that assumption visibly false. Locators are now
   established from the canonical retained segment suffix, including its
   digest, before submission.
5. Member admission cloned and encoded the complete catalogue once per member
   and once per payload, making the 256-member path quadratic. The frozen plan
   is now admitted once, in full, before member media is appended. Per-frame
   checks remain on the incremental/qualification APIs.
6. Work admission now measures outstanding work, matching the stated bounded
   in-flight contract, rather than charging all terminal history forever.
   Recovery separately enforces explicit retained-identity, retained-member,
   retained-payload and retained-work ceilings; fixing the in-flight limit does
   not make hostile-store reconstruction unbounded.

## Local dipstick evidence

Evidence file:
`/tmp/residiuum-atomic-qual-batch-admit-20260821-a.atomic-qual.json`

Release build, three iterations per cell, local development machine:

| Members | Value bytes/member | Commits/s | Member mutations/s | p50 end-to-end | p50 catalogue open | p50 member boundary | Writes/commit | Syncs/commit |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0 | 40.08 | 40.08 | 24.98 ms | 16.96 ms | 3.64 ms | 2 | 2 |
| 1 | 8,192 | 32.99 | 32.99 | 29.93 ms | 21.02 ms | 4.49 ms | 2 | 2 |
| 3 | 256 | 36.14 | 108.41 | 27.01 ms | 19.03 ms | 4.26 ms | 2 | 2 |
| 10 | 8,192 | 30.38 | 303.79 | 29.00 ms | 20.02 ms | 3.69 ms | 2 | 2 |
| 64 | 256 | 25.31 | 1,620.11 | 39.68 ms | 23.73 ms | 7.06 ms | 2 | 2 |
| 256 | 0 | 9.38 | 2,400.38 | 104.82 ms | 58.02 ms | 21.56 ms | 2 | 2 |

The pre-fix 256-member dipstick was approximately 0.917 commits/s, 235 member
mutations/s, 1.09 s p50 end-to-end and 1.02 s in the member boundary, with 517
writes per commit. The current result is roughly a tenfold throughput/latency
improvement while retaining exactly two durability boundaries.

These figures are useful for bottleneck location only. They are not controlled
hardware results and must not be used as product claims.

## Explicit gap: nominal 1 MiB payload band

The `1 member × 1 MiB application payload` cell is structurally refused with
`LimitExceeded`. The canonical plan ceiling is itself 1 MiB, so a full 1 MiB
value plus key, mutation and plan envelopes cannot fit. The qualification spec
must distinguish:

- maximum canonical plan bytes; and
- maximum application value bytes within that plan.

Do not relabel this cell as passing. Either define the payload band below the
envelope-adjusted maximum or deliberately raise the canonical plan limit with a
new bounded-resource proof.

## Next optimization target

Catalogue open/reconstruction is now the largest measured component: about
58 ms of the 105 ms 256-member median, and roughly 17–24 ms even for small
plans. The next package should make terminal Atomic authority persistent and
paged so execution does not reconstruct and clone the complete retained
catalogue per commit. That work must preserve lifetime status resolution,
damage truth, bounded open and read-only recovery.

## Acceptance state

The Atomic frontier crash matrix and embedded driver journeys pass with the
gathered path, including external signal-9 resolution and restart. A dedicated
invariant asserts both one-member and 256-member commits use exactly two
authoritative writes and two authoritative syncs.

`Capabilities::atomics` remains `false`. ATM-5F improves and measures the
implementation; it does not waive the remaining qualification and governance
gates.
