# Q4 §7.2 faithfulness findings (block Q5 competitive)

Status: **recovered 2026-08-09** · **F10–F16 labor implemented/in review with product residuals below**
Feature: `019fe054-1091-7c43-8db0-25394545d377`  
Authority: principal review wave 2 (same day); programme §7–§8  
Prior wave: [RQL_Q34_PREACCEPT_FINDINGS.md](./RQL_Q34_PREACCEPT_FINDINGS.md) F1–F9 labor `in_review`

## Gate

**Do not accept Q4 as competitive-ready** and **do not run Q5 baseline** until all
**P1** cards on this feature are labor-complete (`in_review`) and principal
re-accepts harness honesty. Prefer P2 before package accept; P3 hygiene for
campaign cleanliness.

F1–F9 fixed mechanical defects; verifiers PASS as **scaffold only**. This wave
addresses **specification faithfulness** for §7.2 / §7.4.

## Priority queue (claim order)

| ID | Pri | Board title | Primary paths |
|---|---|---|---|
| F10 | P1 | Execute real concurrency (not metadata-only) | **labor implemented/in review**: logical dual-barrier workers plus all 12 embedded product cells through one physical smart-client connection per cell; requested/observed concurrency, results and metrics are verifier-gated |
| F11 | P1 | Complete mandatory §7.2 plan variants | **labor `in_review`**: nested/array, covered/non-covered, low/high group, all-cell concurrency |
| F12 | P1 | 1:N enrich must produce multiple matches | **labor implemented**: shared fixture has duplicate join values; logical `many` returns genuine fanout |
| F13 | P1 | Deep cursor must drive multi-page product API | **labor implemented for embedded adapter**: drains authenticated `QueryRunOptions.after`; comparator/server residual |
| F14 | P1 | Evidence model for §7.4 campaign fields | **labor implemented model/gate**: protocol floors + raw repetitions + fail-closed Q5 validation; collectors/adapters must populate |
| F15 | P2 | explain_plan_digest must not be result digest | **labor implemented**: logical plan digest hashes serialized plan; product records full plan hash |
| F16 | P2 | Explicit Refused/Unsupported adapter status | **labor implemented**: explicit outcome states; product refusal no longer `Ready` |
| F17 | P3 | Remove `ringtail-sda-starter.zip` from repo tree | workspace root untracked zip |

## Board task ids

| ID | kanban_task_id |
|---|---|
| F10 | `019fe054-937e-7083-b680-503860cf7766` |
| F11 | `019fe054-95b9-73c1-9da9-db3899198521` |
| F12 | `019fe054-9895-7520-9bab-c008f693af2b` |
| F13 | `019fe054-9bc1-7b73-9df7-8593eaf24b35` |
| F14 | `019fe054-9e58-7811-82e8-011cdd9313d0` |
| F15 | `019fe054-a1ab-7701-83e2-eddbe1fd17eb` |
| F16 | `019fe054-a493-7f11-8849-782829ef928d` |
| F17 | `019fe054-a733-7610-87e9-03cb1e71c40d` |

## What still passes (scaffold)

- Q3 verifier PASS (oracle/differential/adversarial/page-concat)
- Q4 verifier PASS as scaffold (default + residiuum-embedded feature)
- F1–F9 labor residual honesty items (see prior findings pack)

## Non-claims

F10–F16 mechanical/model labor is implemented, but Q5 remains HOLD. Embedded
Full enrich and deterministic 90/10 + 70/30 mixed R/W perform real product
operations. All 12 mandatory cells now run through bounded Core/Full pages on
one shared writer/scheduler and must report exact simultaneous jobs. The full
60-row 1/2/4/8/host-oversubscribed product matrix is published with synchronized
aggregate throughput; 60/60 are Product Ready with exact concurrency. Remaining:
the product rehearsal now also carries 84/84 valid raw repetitions with stable
result/query/QVM/index identity across same-deployment reopen, plus 12/12 Product
Ready 4× fixtures. Remaining: configured comparators/server; true
memory-saturating and evidenced device-cold lifecycle; and comparator/server
probes. Accumulated process CPU time, RSS, 1 ms sampled peak RSS, physical-I/O
deltas, VM-boundary logical bytes, and physical/logical read amplification are
now populated for all 24 product resource rows. CPU includes the sampler cost.
The R400 path itself is now executable through a constant-memory streaming
loader with rolling identity, full-scan proof, and a live disk floor. This host
refuses the 64 GiB fixture because fewer than 8 GiB are free. Its privileged
page-cache purge also fails with `Operation not permitted`; both refusals are
published without upgrading either lifecycle claim.
Rotation/compaction is exercised
across all 12 product cells with
stable result digests. Declared physical damage now returns 51/64 healthy rows
under explicit incomplete coverage and fails closed under strict coverage; the
verifier rejects empty survivors, false completeness, and missing compaction I/O.

## Controlled-host R400 result (2026-08-09)

The first admitted run on a fixed 16 GiB M2 host **failed during the unwarmed
full scan**. The 64 GiB logical fixture loaded through the real product path,
but macOS killed `residiuum_rql_qu` under low-swap protection at 46,893 MB of
compressed process memory. The sampled resident set peaked far lower and was
therefore misleading. No R400 claim is admitted.

The operator simultaneously observed approximately 5,500 filesystem writes/s
at a 55 MB/s peak, about 50% CPU, and 2.59 GB visible process memory at 128 GB
written. Read activity alternated between bursts near 2,500 reads/s and drains
near 56 reads/s. These non-normalised host observations align with the preserved
135.3 GiB dual-media footprint and expose why an RSS-only dashboard missed the
kernel's 46,893 MB compressed-process high-water condition.

Two P1 defects are now established:

1. The key-ordered paged scan of large documents retains or churns allocations
   until compressed process memory exceeds the host. It must be made bounded
   by bytes across repeated cursor pages, not merely bounded by row count.
2. The campaign's 1.5x disk estimate was unsafe: the preserved deployment is
   135.3 GiB for the 64 GiB logical target. Admission now uses 2.25x, and the
   final design must retain a live floor through scan/finalization as well as
   loading.

The evidence publisher must also supervise the product worker out of process;
otherwise an OS kill prevents the failure report itself from being written.
