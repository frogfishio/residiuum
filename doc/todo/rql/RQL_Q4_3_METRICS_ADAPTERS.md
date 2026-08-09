# RQL-Q4.3 — Metrics, engine adapters, evidence publication

Status: **labor complete → board `in_review`** (2026-08-08) · package **not accepted**  
Package: RQL-Q4 · Feature `019fda4c-59bf-7320-a0cb-35f92c50fc45` · Task Q4.3  
Depends: Q4.1 architecture · Q4.2 dataset/cells  
Authority: [RQL_QUERY_QUALIFICATION_PROGRAM.md](./RQL_QUERY_QUALIFICATION_PROGRAM.md) §7.4  
Prior: [RQL_Q4_1_HARNESS_ARCHITECTURE.md](./RQL_Q4_1_HARNESS_ARCHITECTURE.md) ·
[RQL_Q4_2_DATASET_CELLS.md](./RQL_Q4_2_DATASET_CELLS.md)

## 1. Goal

Complete **metrics collection**, wire **engine adapters** for **shared logical work**,
and **publish evidence bundles** (versions, seeds, hashes). Harness is Q5-ready as
**structure** — no competitive claims until principal accepts design and Q3 families
are green for measured cells.

## 2. Ownership

| Module | Role |
|---|---|
| `metrics` | Latency collector, quantiles, assemble §7.4 envelope, structured presence (F6) |
| `shared_work` | Same `LogicalDataset` content_hash for all engines |
| `engine` | Adapters: logical Ready, Mongo/CBL/server NotConfigured after load, Residiuum embedded feature |
| `run` | Smoke portfolio runner + evidence publish |
| `residiuum_embedded` | Product Core/Full path, including shared smart-client concurrency |

## 3. Metrics (§7.4)

Collectors fill:

- result digest + coverage + validity
- queries/s (from mean latency) + p50/p95/p99/max
- RSS best-effort (optional; Linux `/proc`; macOS residual)
- documents examined (path)
- explain plan digest (logical or product plan hash echo)
- lifecycle + cold method
- deferred work drain flags

### 3.1 Presence honesty (F6)

`metric_key_presence` returns structured states — **never** unconditional present:

| State | Meaning |
|---|---|
| `present` | Measured value populated |
| `residual` | Known instrumentation gap (store/host probes not wired) |
| `not_supported` | Platform/engine cannot provide the metric |

**Competitive completeness** (`metrics_competitive_complete`) is true only when
every required §7.4 key is `present`. Residual keys without a principal waiver
**fail** competitive validation.

**Scaffold smoke** may still publish when residual-class keys are `residual` and
core measured keys (digest, coverage, validity, latency, docs examined,
lifecycle, deferred drain) are `present` (`metrics_scaffold_publishable`).

### 3.2 Residual until probes (documented)

These keys stay `residual` when empty (envelope fields remain `None`):

| Key | Residual until |
|---|---|
| `cpu_rss` | CPU time collection; RSS on platforms without a probe (macOS currently None) |
| `physical_bytes_rw_amplification` | Store physical I/O + amplification probes |
| `index_size_build_write_penalty` | Index size/build/write-penalty accounting |
| `explain_plan` | Adapters echo executed plan digest |

Constants: `RESIDUAL_UNTIL_PROBES_KEYS`, `RESIDUAL_METRIC_NOTES` in `metrics.rs`.

## 4. Adapters

| Engine | Shared work | Execute |
|---|---|---|
| Logical harness | load | **Ready** pure digests (not product) |
| Residiuum embedded | load | Core + bounded Full RQL, deep cursor, declared indexes, and real mixed R/W |
| Residiuum server | load | `adapter_not_configured` (op 118 residual) |
| Mongo local | load (hash identity) | `adapter_not_configured` (driver 3.8.0 residual) |
| CBL embedded | load (hash identity) | `adapter_not_configured` (native residual) |

**Law:** stubs never invent result digests. They record `shared_work_hash` for
fixture identity proofs across lanes.

## 5. Evidence publication

| Artefact | Path |
|---|---|
| Smoke evidence bundle (default write) | `target/rql-q4/q4_3_smoke_evidence_bundle.json` |
| Labor report (default write) | `target/rql-q4/q4_3_metrics_adapters_report.json` |
| Product concurrency proof | `target/rql-q4/q4_product_concurrency_report.json` |
| Product scaling campaign | `target/rql-q4/q4_product_scaling_report.json` |
| Product repetition/lifecycle rehearsal | `target/rql-q4/q4_product_repetition_lifecycle_report.json` |
| Product maintenance/damage rehearsal | `target/rql-q4/q4_product_maintenance_damage_report.json` |
| Product R400/device-cold admission and execution | `target/rql-q4/q4_product_r400_cold_report.json` |
| Checked-in snapshots | `spec/rql/qualification/harness-v1/q4_*.json` via publish only |
| Default verify | `bash scripts/verify-rql-q4-harness.sh` (reads target/ then spec/; no spec churn) |
| Explicit publish (F8) | `bash scripts/publish-rql-q4-evidence.sh` or `RESIDIUUM_WRITE_SPEC_EVIDENCE=1` |

Bundle includes env fingerprint (Q0 pins), 12 smoke cells, content_hash, notes
that CBL/Mongo are not competitive Ready.

F14 recovery tranche adds a mandatory `CampaignProtocol`, per-cell raw
repetition records (operations, duration, result/query/QVM/index identity and
cache state), and `validate_qualification_ready`. Scaffold bundles fail that
gate by construction. Q5 requires at least seven valid repetitions plus
non-zero warm-up, duration and operation floors with alternating/seeded engine
order. Adapters and campaign collectors must still populate those records. The
smart client admits cloneable Core/Full query pages through one bounded
scheduler and exposes `peak_running`. The product concurrency proof executes all
12 mandatory cells at two workers, requires `peak_running == requested`, and
records result and metric envelopes for each cell.

The explicit engineering campaign executes the complete **60-row** product
matrix: 12 cells at concurrency **1/2/4/8/20**, where 20 is the recorded host
oversubscription (`available_parallelism=10`, rule `2×`). All 60 rows are Product
Ready, achieved exact concurrency, and carry synchronized workload wall time,
operation count, and aggregate operations/second. Setup, fixture loading, and
index construction are outside that wall interval. It remains a single
repetition smoke-scale campaign, not campaign-grade raw-repetition evidence.

Observed contention signals—not verdicts—peak around four workers for
nested/array predicates, Full enrich, conditional projection, and mixed R/W;
most other smoke cells continue improving or flatten later. Repetition and
larger datasets are required before treating this as performance evidence.

The repetition/lifecycle rehearsal adds **84 raw records**: seven repetitions
for every mandatory cell against one prepared deployment. Every repetition
closes/reopens the physical client, performs an unmeasured warm-up on that same
client, then measures. Result, query, QVM, and index-configuration identities
remain stable for all 12 cells. A separate 256-document fixture executes all 12
cells at 4× smoke size. It explicitly does **not** claim larger-than-memory or
device-cold coverage.

Resource honesty: the harness no longer shells out to the denied macOS process
table. A safe in-process sampler records interval-end RSS, a 1 ms sampled RSS
peak, and physical read/write byte deltas for all 24 repetition/larger-fixture
resource rows. The QVM now accounts serialized logical JSON bytes at its host
boundary across Core, indexed candidates, root enrich, and nested foreign
loads. Read amplification is therefore the measured physical-read delta divided
by those logical bytes for the same post-warm-up interval. Zero physical bytes
is retained as a valid observed delta (for example, a cache-served query), not
converted to missing evidence. Accumulated CPU time is the delta from the safe
POSIX process CPU clock over that same interval. It includes the in-process RSS
sampler's cost, which is stated explicitly rather than silently subtracted or
inferred from wall time.

The maintenance/damage rehearsal now exercises explicit seal plus live
compaction for all 12 mandatory product cells. All 12 retain the exact result
digest and record non-zero compaction reads/writes through the public store
operator boundary; queries before and after maintenance remain smart-client
queries. A declared-damage fixture then corrupts 17 verified ItemEvent
frames. `coverage allow_incomplete` returns 51 of 64 healthy rows with incomplete
coverage, while the same query under strict coverage fails closed with 13 typed
`locator_frame_verify_failed` holes. This is product damage-survivor evidence,
not a simulated fault. R400 now has a constant-memory deterministic loader,
rolling fixture identity, full-scan logical-byte proof, and live filesystem
floor checks. This host correctly refuses execution: 16 GiB fixed memory means
a 64 GiB R400 fixture with a 144 GiB physical-write admission (raised after the
first controlled run measured a 135.3 GiB deployment),
while the workspace volume has under 8 GiB free. A real
macOS page-cache purge was also attempted for the unwarmed 128 MiB cold scan;
the host denied it with `Operation not permitted`, so no device-cold claim is
made.

## 6. Evidence (labor)

```
cargo test -p residiuum-rql-qual
bash scripts/verify-rql-q4-harness.sh
```

Smoke: 12/12 logical Ready with results; 12/12 embedded product cells Ready at
requested/achieved concurrency 2 on one physical client per cell; CBL
shared_work loaded; lane S fixture identity true. Explicit scaling campaign:
60/60 Product Ready and exact concurrency at 1/2/4/8/20. Repetition/lifecycle:
84/84 valid raw repetitions with stable identities; 12/12 4× larger fixtures
Product Ready. Maintenance/damage: 12/12 stable through seal+compaction; 51/64
healthy survivors under incomplete coverage; strict coverage fails closed.

## 7. Non-claims

- **Not Gate-1**; **not RQL-Q4 package accept**; **not competitive baseline (Q5)**.
- Logical harness digests ≠ Residiuum product competitiveness.
- Mongo/CBL drivers not shipped; execute refuse is honest.
- Principal still accepts harness design before Q5 campaign.

## 8. Exit checklist (Q4.3)

- [x] Metrics collectors + §7.4 envelope assembly
- [x] Shared logical work across adapters
- [x] Mongo + CBL + server adapters (load + honest refuse)
- [x] Logical smoke execute + digests + metrics
- [x] Shared-client product concurrency smoke across all 12 mandatory cells
- [x] Full 12 × 1/2/4/8/host-oversubscribed product scaling matrix
- [x] Synchronized aggregate-throughput observation per scaling row
- [x] Seven raw product repetitions per mandatory cell
- [x] Same-deployment close/reopen + same-client warm-up mechanics
- [x] 4× larger smoke fixture across all mandatory cells (not memory-saturating)
- [x] Rotation/seal/compaction rehearsal across all mandatory product cells
- [x] Declared physical damage returns healthy survivors with explicit incomplete coverage
- [x] Strict coverage fails closed on the same declared-damage fixture
- [x] In-process RSS, 1 ms sampled peak RSS and physical-I/O interval deltas
- [ ] Controlled-host R400/larger-than-memory campaign
- [ ] Controlled device-cold campaign with an evidenced page-cache drop
- [ ] Accumulated process CPU time and logical-byte read amplification
- [x] Evidence bundle publication (hashes, seeds, versions)
- [x] One-command verify floors
- [ ] Principal harness accept (not labor)

## 9. Q4 package residual for principal

Q4.1–Q4.3 labor is on the board as `in_review`. Package `accept` requires principal
review of design + evidence format before Q5 admits competitive runs.
