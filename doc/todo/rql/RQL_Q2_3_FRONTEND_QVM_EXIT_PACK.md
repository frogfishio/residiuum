# RQL-Q2.3 — Frontend QVM identity + one-runtime exit pack

Status: **labor complete** (2026-08-08) · **Q2 package exit BLOCKED** (not accepted)  
Task: Q2.3 · Feature `019fda4c-1227-7c93-b7e6-292141ec7a78`  
Authority: [RQL_QUERY_QUALIFICATION_PROGRAM.md](./RQL_QUERY_QUALIFICATION_PROGRAM.md) §5 exit  
Evidence tip (this labor): `7fdf156caabb49d51c0b120b2e5bb2082d5c289c` + uncommitted identity test/doc  
Machine audit: [`spec/rql/qualification/corpus-v1/q2_1_capability_audit.json`](../../../spec/rql/qualification/corpus-v1/q2_1_capability_audit.json)

## 0. Expert review revise (2026-08-08)

Expert review findings on this pack:

| Finding | Severity | Disposition |
|---|---|---|
| Insufficient Tier A expressibility (was 129/147) | high · blocking for **Q2 package accept** | **Acknowledged.** Not a Q2.3 task-accept failure: Q2.3 owns identity + exit packaging honesty. **Labor revise:** closed `pkg_budget_partial_coverage` → **134**/147 (gap 11). Residual packages remain (§4). |
| Decision 0 OPEN blocks one-runtime exit claim | high · blocking for **Q2 package accept** | **Acknowledged; principal-only.** Labor must not close D0 or accept RQL-C1. Exit pack continues to **block** package exit until principal D0 disposition. |

**Q2.3 task criteria (this card):** multi-frontend Core QVM identity proven; sole QVM product path; blockers recorded.  
**Q2 package §5 exit:** still **NOT READY** (Tier-A gaps + D0 OPEN).

## 1. What Q2.3 owns

Programme §5 exit requires **all** of:

1. **100% of Tier A** expressible without application collection scans
2. Tier B/C deliberately classified
3. **Equivalent SQL / Rust-builder / RQL → identical canonical QVM**
4. **Canonical QVM is the only production execution authority**

Q2.3 packages identity proof + honesty for (3)–(4), and records blockers for (1) and Decision 0.

## 2. Verdict (principal-facing)

| Sub-claim | Status | Notes |
|---|---|---|
| Multi-frontend Core → same `plan_hash` + same QVM bytes/hash | **proven (labor)** | Integration test green (§3) |
| Product query path = QVM1 + `run_vm` only (no RQB1) | **proven (labor)** | Q0.A10 delete + public API guard |
| No second **product semantic** executor for RQL | **labor_closed architecture** | IR residual is tech-debt, not a second public path |
| Tier A 100% expressible | **OPEN** | **134**/147 execute_ok; **11** gaps (§4) — post Q2.2d |
| Tier B/C classified | **yes** (Q1 corpus) | A=147 / B=2 / C=4 |
| Decision 0 / one-runtime **exit claim** | **BLOCKED** | D0 **OPEN**; RQL-C1 **FORBIDDEN** |
| **Q2 package accept** | **BLOCKED** | Do not accept until §4 gaps closed **and** principal D0 disposition |

```text
Q2.3 labor     = complete (identity pack + blockers recorded)
Q2 exit claim  = NOT READY
RQL-C1         = FORBIDDEN
NEXT           = residual Q2.2 packages (conditional project / cursor after / enrich semantics)
                 then principal D0 disposition before any one-runtime exit wording
```

## 3. Multi-frontend identity evidence

### 3.1 Test

```sh
cargo test -p residiuum-sdk --test rql_q2_frontend_qvm_identity
```

**Result (2026-08-08):** `7 passed; 0 failed`.

| Test | What it locks |
|---|---|
| `q23_builder_and_rql_identical_plan_and_qvm` | `PlanBuilder` ≡ Core RQL → same plan_hash + QVM bytes |
| `q23_sql_emit_and_hand_rql_identical_qvm` | SQL-ish+ emit ≡ hand Core RQL → same plan + QVM |
| `q23_sql_param_form_matches_rql_param_form` | `:param` SQL ≡ `$param` RQL |
| `q23_three_frontends_same_simple_eq` | Builder + RQL + SQL on one selection cell |
| `q23_lower_core_source_matches_from_core_plan` | Public `lower_core_source` ≡ `QueryBytecodeV1::from_core_plan` |
| `q23_predicate_builder_path_matches_rql_cmp` | Predicate builder cmp ≡ RQL cmp |
| `q23_product_qvm_path_no_rqb1_symbols` | No public RQB1 execute surface; QVM path named |

Identity is measured as:

1. `RqlPlanV1::plan_hash` (canonical plan encoding domain)
2. durable QVM1 bytes after encode
3. `qvm_hash` of those bytes

### 3.2 Frontend map (honest)

| Frontend | Product role | Identity surface |
|---|---|---|
| Core RQL (`compile_app_core`) | Primary language | Authority for Core plans |
| Rust `PlanBuilder` / predicate builders | Typed builder | Must lower to same plan/QVM as equivalent RQL |
| SQL-ish+ (`compile_sql_to_rql`) | Declared SQL subset → Core RQL | Must emit Core RQL then share lower; refuse outside subset |
| Full RQL (`compile_rql_full`) | Enrich / within / brace | **RQL-only** today — no SQL/builder Full multi-frontend claim |
| Comfort `Filter` / dialect find | Non-RQL scan helpers | **Not** product RQL query authority |

Corpus CBL SQL++ / Mongo pipelines are **comparators** (Q0/Q4), not Residiuum frontends that must hash-equal QVM.

### 3.3 Prior unit locks (still valid)

- `rql_app_core` unit: builder ↔ RQL plan_hash (APP-5)
- `app4_predicate_plan`: plan vector hash lock
- `sql_plus_corpus`: SQL emit/refuse corpus

Q2.3 elevates these to **QVM byte identity**, not plan_hash alone.

## 4. Tier-A expressibility residual (blocks Q2 exit)

From Q2.1 re-audit after computed projection, textual continuation and the
versioned within corpus correction (2026-08-09):

| Metric | Value |
|---|---:|
| Tier-A cases | 147 |
| Execute ok (product expressible) | **145** |
| Expected stable refusal ok | **2** |
| Gap cases | **0** |

All case-count packages are closed on the embedded product path. The explicit
Full op-118 profile and backend-neutral `HeapClient::rql_full` close the
Full-over-wire labor blocker with qualified TLS parity/refusal/isolation tests.
Package exit still requires the principal's corpus/Q1 and Decision-0
dispositions and complete Q3 semantics; 145 successful executions plus two
deliberate refusals are not by themselves a package acceptance.

## 5. Sole production authority (QVM)

| Check | Evidence |
|---|---|
| Public product path is QVM1 only | `crates/residiuum-sdk/src/lib.rs` comment + exports; Q0.A10 deleted RQB1 |
| Execute = decode QVM → verify → `run_vm` | `execute_qvm_bytes` / `execute_bytecode` |
| Core + Full share one VM loop | `vm_exec::run_vm` (RQL-VM1R) |
| IR residual honesty | [QUERY_IR_RESIDUAL.md](./QUERY_IR_RESIDUAL.md); [RQL_D0_RESIDUAL_INVENTORY.md](./RQL_D0_RESIDUAL_INVENTORY.md) |

**Not claimed:** micro-op purity of every phase body (Decision 0 residual; principal disposition required for C1).

## 6. Decision 0 gate (hard)

From [RQL_D0_CLOSE_READINESS.md](./RQL_D0_CLOSE_READINESS.md) §D:

> Do **not** mark Q2 Feature/package exit "one QVM runtime" while Decision 0 is OPEN
> unless principal has chosen a written intermediate disposition.

| Item | State |
|---|---|
| Decision 0 | **OPEN** |
| RQL-C1 | **FORBIDDEN** |
| D0.1 / D0.2 labor cards | board `done` (inventory only ≠ close) |
| Kanban Feature (honesty) | `019fda4c-a6f2-7932-a9d7-6e04400fd3df` |

Labor must **not** fill the principal sign-off block in D0.2.

## 7. What principal must still do for Q2 accept

1. Residual Q2.2 packages land → re-run `rql_q2_capability_audit` → **147/147** expressible (plus expected refusals).
2. Principal D0 disposition (OPEN | DOCUMENTED_INTERMEDIATE | CLOSED) recorded in D0.2 §F.
3. Only then may Q2 exit language claim one-runtime + multi-frontend identity as a **package accept**.
4. Human accept on scoreboard (`NEXT_BUILD_STATUS.md` RQL-Q2 → `accept`) and board `done` — not labor-driven.

## 8. Non-claims

- Not Gate-1 / Q3 oracle / Q4 harness / performance
- Not Full multi-frontend identity (SQL/builder do not express Full)
- Not wire Full (Q2-BLOCK-FULL-WIRE)
- Not corpus `canonical_qvm_hash_hex` backfill for all 147 cases (optional Q2/Q3 follow-on)
- Board `in_review` ≠ package accept

## 9. Related paths

| Path | Role |
|---|---|
| `crates/residiuum-sdk/tests/rql_q2_frontend_qvm_identity.rs` | Identity suite |
| [RQL_Q2_1_CAPABILITY_AUDIT.md](./RQL_Q2_1_CAPABILITY_AUDIT.md) | Expressibility audit |
| [RQL_Q2_2_WAVE1_GROUP_AGG.md](./RQL_Q2_2_WAVE1_GROUP_AGG.md) | Wave-1 |
| [RQL_Q2_2B_ENRICH_DIALECT.md](./RQL_Q2_2B_ENRICH_DIALECT.md) | Enrich dialect |
| [RQL_Q2_2C_ARRAY_PREDICATE.md](./RQL_Q2_2C_ARRAY_PREDICATE.md) | Array preds |
| [RQL_D0_CLOSE_READINESS.md](./RQL_D0_CLOSE_READINESS.md) | One-runtime honesty |
| [QUERY_VM_V1.md](./QUERY_VM_V1.md) | Opcode map |
