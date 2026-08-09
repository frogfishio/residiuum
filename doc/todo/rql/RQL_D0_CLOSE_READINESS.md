# RQL Decision-0 close readiness checklist (D0.2)

Status: **2026-08-07 · labor checklist · Decision 0 remains OPEN · RQL-C1 FORBIDDEN**
Audience: **principal** (human accept only)
Authority: hard invariant below · [RQL_D0_RESIDUAL_INVENTORY.md](./RQL_D0_RESIDUAL_INVENTORY.md) (D0.1) ·
[QUERY_RUNTIME_CONVERGENCE.md](./QUERY_RUNTIME_CONVERGENCE.md) ·
[RQL_QUERY_QUALIFICATION_PROGRAM.md](./RQL_QUERY_QUALIFICATION_PROGRAM.md) §5 + §11
Tree baseline for companion inventory: git `ef4e825` (post D0.1 land)

**This document does not close Decision 0.** Labor marks criteria
`labor_closed` / `open` / `principal_only` / `out_of_scope_for_D0_close`.
Only principal may flip Decision 0 or accept RQL-C1.

**Q0.A7 (2026-08-07, principal review finding #7):** The Decision 0 close test is
*all product frontends → canonical QVM → one `run_vm`*, not rewriting every
opcode body as a pure stack micro-op. Criterion **A9** no longer blocks D0 close.

---

## Hard invariant (must hold before any close)

```text
RQL / SQL-ish+ / JSON / Mongo / Builder / Wire
        → canonical QVM bytecode → one run_vm → HostCapabilities

Raw SDA → explicitly raw SDA APIs only (dialect `sda` / Collection::sda)
```

| Forbidden until principal says otherwise |
|---|
| Decision 0 closed |
| RQL-C1 accept |
| Prior VM1 / P1c "converged" re-claimed as accept |
| Q2 "one QVM runtime" exit claim while D0 residual open |
| Treating board `in_review` as package accept |

---

## How to read this checklist

| Mark | Meaning |
|---|---|
| **labor_closed** | Implementer evidence exists; principal may still reject |
| **open** | Residual honesty or missing product bar; blocks principal close if required by bar |
| **principal_only** | Human decision; labor must not self-certify |
| **out_of_scope_for_D0** | Programme/capability work (Q1–Q7); must not be silently required as if D0 alone |

---

## A. Architectural close criteria (Decision 0 bar)

These answer: *is there one product semantic executor?*

| ID | Criterion | State | Evidence / path |
|---|---|---|---|
| A1 | Single durable public executable form is **QVM1** | **labor_closed** | `query_bytecode_v1/qvm.rs`; `QueryBytecodeV1`; [QUERY_VM_V1.md](./QUERY_VM_V1.md); `evidence/rql_qvm1_durable_bytecode.log` |
| A2 | One dispatch loop: Core + Full enter `run_vm` | **labor_closed** | `vm_exec::run_vm`; `execute_qvm_bytes` / `execute_full_qvm_with`; `evidence/rql_vm1r_one_run_vm.log` |
| A3 | Fused orchestrators deleted (`run_core_page`, `execute_plan`, `run_attach_pipeline`) | **labor_closed** | `scripts/check_query_runtime_architecture.sh` forbids symbols; DEL1 |
| A4 | Dialect `rql` no longer compiles to SDA (parallel story retired) | **labor_closed** | R1 refuse; `dialects` + arch gate; `evidence/rql_r1_dialect_cache_arch.log` |
| A5 | sql / json / mongo → portable → QVM (not SDA text product path) | **labor_closed** | DQ1; `Collection::find_portable_with` → `execute_bytecode`; dialects banner |
| A6 | Host is collection-qualified scan/index/get only (`HostCapabilities`) | **labor_closed** | P1b; trait in `query_bytecode_v1/mod.rs`; `evidence/rql_p0b_private_api.log` / `rql_p1b_host_by_id.log` |
| A7 | Raw SDA only via explicit SDA APIs | **labor_closed** | `Collection::sda`; dialect `sda`; custom dialects portable-only |
| A8 | Architecture gate green (Decision 0 honesty + C1 forbid text) | **labor_closed** | `bash scripts/check_query_runtime_architecture.sh` → OK |
| A9 | Opcode **bodies** are pure stack micro-ops (no large Rust phase interpreters) | **out_of_scope_for_D0_close** (Q0.A7) | Principal review: Rust phase helpers inside QVM opcodes are normal; **not** a second executor. IR residual = optional tech-debt. Close test = one product QVM path (A1–A8), not micro-op purity. See D0.1 §1 |
| A10 | Full language on same **wire** as Core (op 118) | **labor_closed** | Explicit `profile: "full"`; server Full QVM host; `HeapClient::rql_full`; qualified TLS parity/refusal/isolation coverage. Core profile still refuses Full constructs. |
| A11 | Portable dialect path uses durable host identity (not free name-only synthetic) | **labor_closed** (store-scoped, Q0.A6) | DX: `store_id` + name; Heap catalog ids remain `CollectionClient`. Residual: not Heap-catalog UUID on flat Collection |
| A12 | Principal accepts residual inventory + this checklist | **principal_only** | D0.1 + this file; board Feature `019fda4c-a6f2-…` |
| A13 | Principal Decision 0 close / RQL-C1 | **principal_only** | **Do not mark closed in docs without principal** |

### Principal decision options (suggested)

Labor must not pick these. For principal use only:

1. **Keep OPEN** — default. Q2 may not claim one-runtime exit. Continue Q0/Q1 programme.
2. **Accept D0 residual as *documented* intermediate** — still not C1; explicitly allow Q2
   work while IR residual remains, with scoreboard honesty that "one runtime" means
   one `run_vm` + QVM authority, not pure stack purity. Requires dated principal note.
3. **Close Decision 0 / accept C1** — only if A1–A8 hold *and* principal accepts
   residual inventory (A12). **A9 pure micro-op purity is not required** (Q0.A7).
   A10 Full-wire is labor-closed; it remains evidence rather than a substitute
   for the principal A12/A13 disposition.
   Record profile, exclusions, evidence digest.

---

## B. Evidence commands (re-run before principal review)

| Check | Command / artefact | Expected |
|---|---|---|
| Architecture gate | `bash scripts/check_query_runtime_architecture.sh` | exit 0; Decision 0 OPEN; C1 forbidden |
| QVM / VM unit surface | `cargo test -p residiuum-sdk --lib query_bytecode_v1` (or focused module tests) | pass |
| App Core compile | `cargo test -p residiuum-sdk --lib rql_app_core` | pass (APP-5 labor; not C1) |
| App Core integration | `cargo test -p residiuum-sdk --test app5_rql_app_core` | pass |
| Dual pack / APB-7 query | `cargo test -p residiuum-sdk --test apb7_query_dual_pack` | pass (product path evidence; package accept separate) |
| Full corpus (embedded Full, not C1) | `cargo test -p residiuum-sdk --test rql_full_corpus` | pass where admitted |
| Full product wire | `cargo test -p residiuum-server --features dangerous-key-export --test hp007_connect_heap apb7_query_from_remote_collection_plane` | qualified TLS Full parity/refusal/isolation pass |
| Dialect portable path | `cargo test -p residiuum-sdk --test dialects_query` | pass |
| Labor evidence logs | `doc/todo/rql/evidence/rql_{r1,qvm1,vm1r,*}.log` | present; historical labor |
| Residual inventory | [RQL_D0_RESIDUAL_INVENTORY.md](./RQL_D0_RESIDUAL_INVENTORY.md) | current honesty |

**Note:** Green tests ≠ Decision 0 close ≠ RQL-C1 accept ≠ Gate-1 pass.

---

## C. Corpora and dual packs (honesty map)

| Artefact | Role | Decision 0? | Programme |
|---|---|---|---|
| `spec/app/v1/rql_app_core_corpus_v1.json` + APP-5 tests | Application Core compile surface | Supports QVM lower path; **not** D0 close | APP-5 labor |
| `rql_full_*` + qualified TLS op-118 scenario | Full language embedded and remote execute via QVM | Proves Full → `run_vm` plus bounded wire parity; package accept remains separate | Phase 3 / APP-7 |
| `apb7_query_dual_pack` | Embedded + remote Core query | Product dual-pack evidence; package accept open | APB-7 / APP-7 |
| `apb7_multipage_oracle_matrix` | Page / oracle matrix | Test honesty; not Q3 independent oracle | APB-7 |
| Tier-A practical corpus (Q1) | ~100–150 intentions | **Not landed** | **out_of_scope_for_D0** until Q0 accept → Q1 |
| Q3 independent oracle | Unoptimised test-only oracle | **Not landed** | **out_of_scope_for_D0** (Q3) |

Do not claim D0 closed because Full/Core unit tests are green.

---

## D. Cross-link: Q2 one-runtime exit criterion

From [RQL_QUERY_QUALIFICATION_PROGRAM.md](./RQL_QUERY_QUALIFICATION_PROGRAM.md) §5 exit:

> Canonical QVM is the only production execution authority.  
> Equivalent SQL/Rust-builder/RQL inputs produce the same canonical QVM.  
> 100% of Tier A expressible without application collection scans.

Programme §11: *"Current QVM convergence blockers must close before Q2 can
claim its one-runtime exit."*

| Q2 one-runtime sub-claim | Depends on | State today |
|---|---|---|
| Sole production authority = QVM + `run_vm` | A1–A8 | **labor_closed** (architecture) |
| No second product executor | A3–A5, A7 | **labor_closed** |
| Principal honesty that residual IR is documented tech-debt (not second executor) | A12–A13; A9 = out_of_scope_for_D0_close (Q0.A7) | **principal_only** for close |
| Frontend → identical canonical QVM (SQL/builder/RQL) | Q2.3 + corpus | **labor_closed for Core multi-frontend identity** ([RQL_Q2_3_FRONTEND_QVM_EXIT_PACK.md](./RQL_Q2_3_FRONTEND_QVM_EXIT_PACK.md)); **exit claim still blocked** by D0 OPEN + Tier-A gaps |
| Tier A 100% expressible | Q1 corpus + Q2 implement | **open** (**out_of_scope_for_D0** capability) |

**Law for implementers:** Do **not** mark Q2 Feature/package exit "one QVM runtime"
while Decision 0 is OPEN unless principal has chosen option 2 above in writing.
Capability gaps (aggregates, etc.) are Q2 work packages; they do not substitute
for D0 honesty, and D0 honesty does not substitute for Tier-A completeness.

Kanban Feature (blocks Q2 exit claim): `019fda4c-a6f2-7932-a9d7-6e04400fd3df`
(*RQL Decision-0 residual (pre-Q2 honesty)*).

---

## E. Explicit open residuals (do not paper over)

1. **IR residual** — opcode dispatch is real; phase bodies are still Rust (D0.1 §1).
2. **Dialect identity** — name-derived ids on comfort `Collection` dialect path.
3. **Prior principal rejects** — VM1, P1c, prior D0/C1 closure claims stay rejected.
4. **Principal disposition remains open** — A10 labor closure does not close D0/C1.

---

## F. Principal sign-off block (leave blank until human)

```text
Decision 0 status after review:  OPEN | DOCUMENTED_INTERMEDIATE | CLOSED
RQL-C1:                          FORBIDDEN | ACCEPTED (date/profile: ________)
Accepted residuals (if any):     _______________________________________________
Evidence digest / git SHA:       _______________________________________________
Date / principal:                _______________________________________________
```

Labor must not fill this block as if accepted.

---

## G. Related docs

| Doc | Role |
|---|---|
| [RQL_D0_RESIDUAL_INVENTORY.md](./RQL_D0_RESIDUAL_INVENTORY.md) | D0.1 detailed inventory |
| [RQL_WHAT_IS_LEFT.md](./RQL_WHAT_IS_LEFT.md) | Short residual order |
| [QUERY_IR_RESIDUAL.md](./QUERY_IR_RESIDUAL.md) | Phase ledger |
| [QUERY_VM_V1.md](./QUERY_VM_V1.md) | Opcode map |
| [RQL_QUERY_QUALIFICATION_PROGRAM.md](./RQL_QUERY_QUALIFICATION_PROGRAM.md) | Q0–Q7 programme; Q2 exit |
| [RQL0_GAP_LEDGER.md](./RQL0_GAP_LEDGER.md) | Capability gap ledger (not D0 close) |

---

## One-line verdict

```text
D0.2 checklist = ready for principal review
Decision 0     = OPEN (labor must not close)
RQL-C1         = FORBIDDEN
Q2 one-runtime exit claim = blocked until principal D0 disposition
NEXT           = principal review of D0.1+D0.2; Q0 package accept; no false C1
```
