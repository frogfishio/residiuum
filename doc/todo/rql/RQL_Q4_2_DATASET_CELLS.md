# RQL-Q4.2 — Dataset generators + mandatory measured cells

Status: **labor complete → board `in_review`** (2026-08-08) · package **not accepted**  
Package: RQL-Q4 · Feature `019fda4c-59bf-7320-a0cb-35f92c50fc45` · Task Q4.2  
Depends: Q4.1 harness architecture  
Authority: [RQL_QUERY_QUALIFICATION_PROGRAM.md](./RQL_QUERY_QUALIFICATION_PROGRAM.md) §7.1–7.3  
Design: [RQL_Q4_1_HARNESS_ARCHITECTURE.md](./RQL_Q4_1_HARNESS_ARCHITECTURE.md)

## 1. Goal

Implement **dataset axes**, **deterministic logical generators**, **mandatory cell
plans 1–12** with concurrency matrix, and **cache/lifecycle classes** with honest
cold/reopen definitions. Shared logical work across engines; host only scales
document counts.

## 2. Ownership

| Module | Role |
|---|---|
| `residiuum_rql_qual::dataset` | §7.1 axes: shape, payload, memory ratio, distribution, cardinality, selectivity |
| `residiuum_rql_qual::generator` | SplitMix64 logical docs (`docs` + `customers`); content hash |
| `residiuum_rql_qual::lifecycle` | §7.3 classes; cold method honesty (reopen ≠ device cold) |
| `residiuum_rql_qual::cell_plan` | Measured cell plans, smoke portfolio, concurrency / selectivity / lifecycle matrices |

## 3. Dataset axes (§7.1)

| Axis | Values |
|---|---|
| Shape | flat, deeply_nested, sparse_heterogeneous, array_heavy |
| Payload | ≈1 / 8 / 64 KiB + seeded heavy tail |
| Memory ratio | 25% / 100% / 400% (scales `doc_count` only) |
| Distribution | uniform, zipf_hot_key, time_ordered |
| Cardinality | low / medium / high (distinct-ratio) |
| Selectivity | point, 0.01%, 1%, 10%, broad |

Smoke default: 64 docs, flat, 1 KiB, R25, uniform, medium card, 10% sel.

## 4. Mandatory cells (§7.2)

Twelve plans in `smoke_portfolio` with RQL intention, indexes, order flag,
page_size / R/W mix where relevant:

1. Key get (`_key`)
2. Indexed eq multi-selectivity (`sel_bucket`) + selectivity matrix ×5
3. Range + compound
4. Nested / array preds
5. Covered / non-covered project
6. Deterministic top-k
7. First + deep cursor (`page_size=8`)
8. Enrich cardinalities (**server_lane_ineligible** — Q0.A4)
9. Group low/high card
10. Agg count/sum/min/max
11. Conditional/computed project intention
12. Mixed R/W 90/10 (70/30 via `ReadWriteMix`)

**Concurrency:** 1, 2, 4, 8 + one host-declared oversubscribed slot
(`concurrency_matrix`).

## 5. Lifecycle honesty (§7.3)

| Class | Cold method | Device cold? |
|---|---|---|
| warm_steady | not_cold_warm_steady | no |
| fresh_reopen | store_reopen | **no** (page cache may stay warm) |
| larger_than_memory | not_cold_warm_steady | no |
| read_only | not_cold_warm_steady | no |
| concurrent_writes | not_cold_warm_steady | no |
| rotation_compaction | store_reopen | no |
| declared_damage | declared_damage_survivors | no |

Device-cold claims require `attempted_page_cache_drop` only.

Memory ratios bind to a fixed, evidenced controlled-host capacity. Transient
free-memory readings are forbidden because memory pressure could otherwise make
a fixture smaller than physical RAM falsely satisfy R400.

## 6. Evidence

| Artefact | Path |
|---|---|
| Crate tests | `cargo test -p residiuum-rql-qual` (**26**/26) |
| Machine report | `spec/rql/qualification/harness-v1/q4_2_dataset_cells_report.json` |
| Verify | `bash scripts/verify-rql-q4-harness.sh` |

## 7. Non-claims

- Not Gate-1; not RQL-Q4 package accept; not competitive Q5 baseline.
- Product execution and metrics live in Q4.3; this task owns plans and logical datasets.
- Enrich uses the explicit Full profile. Group and aggregate plans use the current
  Core grammar (`group by … project … count() as …`) and are exercised by the
  embedded product concurrency smoke.

## 8. Exit checklist (Q4.2)

- [x] Dataset axes closed sets + scale-by-memory-ratio
- [x] Deterministic generator + content hash
- [x] Mandatory cells 1–12 smoke plans
- [x] Concurrency matrix (1/2/4/8 + oversub)
- [x] Selectivity + lifecycle matrices
- [x] Honest cold/reopen definitions
- [x] Machine report + verify script floors
- [ ] Principal design accept (not labor)

## 9. F2 real §7.2 variants (pre-accept)

Logical harness now runs **genuine** multipage cursor (first+deep, full concat),
mixed R/W with writes (90/10 and 70/30), conditional `high_band`, agg **avg**,
enrich optional/exactly_one/many, and **executes** concurrency matrix in
`section_7_2_expanded_portfolio` / `run_section_7_2_expanded`.

Product rehearsal now exercises all mandatory cells at 256 documents (4× the
64-document smoke fixture). This proves larger-fixture plumbing only; it does
not satisfy the R400/larger-than-host-memory lifecycle class.

## 10. Next

**Q4.3** — metrics collectors, Mongo/CBL/server adapters, evidence publication — see [RQL_Q4_3_METRICS_ADAPTERS.md](./RQL_Q4_3_METRICS_ADAPTERS.md) (labor `in_review`).
