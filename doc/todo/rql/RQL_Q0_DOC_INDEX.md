# RQL-Q0 — Document index (normative vs process)

Status: **Q0.A8** · 2026-08-07  
Authority: principal review finding #8 (doc proliferation)

## Normative freeze set (must accept or amend together)

These four artefacts plus the principal accept pack are the **only** Q0 freeze
surface for Gate-1 target/profile:

| # | Artefact | Role |
|---|---|---|
| 1 | [RQL_Q0_ENV_MANIFEST.md](./RQL_Q0_ENV_MANIFEST.md) | Engines, drivers, durability, fingerprint |
| 2 | [RQL_Q0_CAPABILITY_MATRIX.md](./RQL_Q0_CAPABILITY_MATRIX.md) | Tier A/B/C classes + blockers |
| 3 | [RQL_Q0_RESULT_EQUIVALENCE.md](./RQL_Q0_RESULT_EQUIVALENCE.md) | Equivalent-result laws |
| 4 | [RQL_Q0_LANES_EXCLUSIONS.md](./RQL_Q0_LANES_EXCLUSIONS.md) | Lanes E/S, refusals, **Q2-BLOCK-FULL-WIRE** |
| 5 | [RQL_Q0_PRINCIPAL_ACCEPT.md](./RQL_Q0_PRINCIPAL_ACCEPT.md) | Principal sign-off pack (§5 human only) |

Strategy (not freeze exit): [RQL_QUERY_QUALIFICATION_PROGRAM.md](./RQL_QUERY_QUALIFICATION_PROGRAM.md).

## Current performance campaign restart

The parked RQL/Mongo performance effort resumes from
[RQL_PERFORMANCE_PROOF_CONTINUATION_2026_08_10.md](./RQL_PERFORMANCE_PROOF_CONTINUATION_2026_08_10.md).
It contains the frozen local baseline, remaining scale/remote/join work,
Bonzo procedure, validation commands and evidence-admission rules. Do not
restart that work from the historical chat or the original weak-cell baseline.

## Process / honesty (temporary; archive after Q0 ACCEPT)

| Artefact | Role | After principal Q0 ACCEPT |
|---|---|---|
| [RQL_LABOR_HOLD.md](./RQL_LABOR_HOLD.md) | Amendment admit / Q1 claim policy | Move to `doc/done/` or archive section |
| [RQL_D0_RESIDUAL_INVENTORY.md](./RQL_D0_RESIDUAL_INVENTORY.md) | Decision 0 IR honesty | Keep until D0 principal disposition |
| [RQL_D0_CLOSE_READINESS.md](./RQL_D0_CLOSE_READINESS.md) | Principal D0 checklist | Keep until D0 disposition |
| This index | Map normative vs process | Keep as pointer or fold into programme §11 |

**Law:** do not grow a parallel Q0 essay series. Amendments edit the four freeze
files + accept pack; process notes stay thin.

## Q2.1 capability audit (after Q1 corpus ready)

Machine gap report: [`spec/rql/qualification/corpus-v1/q2_1_capability_audit.json`](../../../spec/rql/qualification/corpus-v1/q2_1_capability_audit.json)  
Human: [RQL_Q2_1_CAPABILITY_AUDIT.md](./RQL_Q2_1_CAPABILITY_AUDIT.md)  
Harness: `cargo test -p residiuum-sdk --test rql_q2_capability_audit`  
**Not** Q2 package accept; Decision 0 still OPEN.

## Q1 delivery shape (after Q0 ACCEPT)

Programme package **RQL-Q1 Practical corpus** is:

1. **Machine-readable corpus data** (schema + versioned cases + fixtures)
2. **One** short human report (floors, comparator review, amendment process)

Not: another multi-document freeze family named Q0-style.

**Q1.1 scaffold (2026-08-07):**  
Machine: [`spec/rql/qualification/corpus-v1/`](../../../spec/rql/qualification/corpus-v1/)  
Report: [RQL_Q1_CORPUS.md](./RQL_Q1_CORPUS.md)  
Validate: `bash scripts/verify-rql-q1-corpus.sh`  
Live cases still empty until Q1.2–Q1.3; package **not** accepted.

**Name collision:** gap-ledger historical row is **`RQL-PERF-1`** (query perf
campaign). Programme **RQL-Q1** = practical corpus only.

## Amendment Feature (Kanban)

Product project `019fda36-f8f4-7f40-9a9b-a86cfae1466e` · Feature
`019fdac4-1408-7321-8edc-a09851c9e656` · tasks Q0.A1–Q0.A9.
