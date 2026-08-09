# RQL Decision-0 residual inventory (D0.1)

Status: **2026-08-07 · labor inventory · Decision 0 remains OPEN**
Authority: [RQL_WHAT_IS_LEFT.md](./RQL_WHAT_IS_LEFT.md) ·
[QUERY_RUNTIME_CONVERGENCE.md](./QUERY_RUNTIME_CONVERGENCE.md) ·
[QUERY_IR_RESIDUAL.md](./QUERY_IR_RESIDUAL.md) · [QUERY_VM_V1.md](./QUERY_VM_V1.md)
Tree snapshot: git `d8fc0f8ace6e65a59e04ccd9fd70dd33546753a4`
Architecture gate: `bash scripts/check_query_runtime_architecture.sh` → **OK**
  (R1+QVM1+WIRE1+typed ops+DEL1; Decision 0 OPEN; C1 forbidden)

This is the **single citable honesty inventory** for Decision 0 / RQL-C1 close
readiness. It is **not** package accept and **not** a claim that prior VM1 / P1c
convergence is accepted.

---


### Q0.A7 amendment (principal review, 2026-08-07)

**Pure stack micro-op rewrite is not required for Decision 0 close.**
Rust phase helpers (`core_phases`, `ir_*`, Full attach) behind one `run_vm`
are **implementation detail / optional tech-debt**, not a second product
executor. The hard close test remains:

```text
all product frontends → canonical QVM → one run_vm → HostCapabilities
```

IR residual inventory below remains for honesty; it must not be treated as
a mandatory purity campaign before principal D0 disposition.

## FORBIDDEN claims (restate — do not weaken)

| Claim | Status |
|---|---|
| Decision 0 closed | **OPEN** |
| RQL-C1 accept | **must not be accepted** |
| Prior VM1 / P1c "converged" | **rejected** (stands) |
| Intermediate IR1–IR4 / VM2–VM4 labor = Decision 0 | **false** |
| QVM1 + one `run_vm` alone = every frontend → pure opcode machine | **false** |
| Board `in_review` = principal accept | **false** |

Hard product invariant (target, not fully "pure stack" today):

```text
RQL / SQL-ish+ / JSON / Mongo / Builder / Wire
        → canonical QVM bytecode → one run_vm → HostCapabilities

Raw SDA → explicitly raw SDA APIs only (dialect `sda` / Collection::sda)
```

---

## 1. Opcode vocabulary vs Rust phase bodies

### 1.1 True QVM opcodes (vocabulary frozen)

Source: `crates/residiuum-sdk/src/query_bytecode_v1/vm.rs` (`OpCode`, `VM_PROFILE`).

| Byte | Name | Dispatch | Body implementation (honest) |
|---|---|---|---|
| `0x01` | `BindCollection` | `vm_exec::run_vm` | Opens `CoreFrame::begin` (Rust) |
| `0x10` | `Scan` | `run_vm` | `CoreFrame::scan` → `core_phases` |
| `0x11` | `IndexEq` | `run_vm` | `CoreFrame::index_eq` → host probe |
| `0x20` | `Filter` | `run_vm` | `CoreFrame::filter` → `kernel` SDA eval |
| `0x30` | `ProjectPaths` | `run_vm` | `CoreFrame` → `ir_project::apply_project_paths` |
| `0x40` | `Order` | `run_vm` | `CoreFrame` → `ir_order` |
| `0x50` | `Page` | `run_vm` | `CoreFrame` → `ir_page` |
| `0x60` | `Enrich` | `run_vm` | Attach helpers in `full_attach` / `vm_exec` |
| `0x61` | `Within` | `run_vm` | Within stack + bag map (Rust) |
| `0x62` | `WithinEnd` | `run_vm` | Leave within (Rust) |
| `0x63` | `FilterAttach` | `run_vm` | Post-attach kernel filter (Rust) |
| `0x64` | `ProjectBrace` | `run_vm` | Brace project (Rust) |
| `0xFF` | `Halt` | `run_vm` | Yield page |

**Residual:** opcodes are a **dispatch and encoding boundary**, not a finished
micro-VM whose semantics live only in stack ops. Each Core opcode owns a real
`CoreFrame` phase body (`core_phases.rs`); Full attach lives as large Rust
helpers behind the same loop. That is **IR residual honesty**, not a second
product executor.

### 1.2 Named IR residual modules (still Rust helpers)

| Module | Profile stamp | Role |
|---|---|---|
| `ir_project.rs` | `residiuum-query-ir-project-v1` | Path-project pure helper |
| `ir_order.rs` | `residiuum-query-ir-order-v1` | Sort-tuple compare |
| `ir_page.rs` | `residiuum-query-ir-page-v1` | Page size, coverage, cursor mint |
| `ir_attach.rs` | `residiuum-query-ir-attach-v1` | **Profile stamp only** — `run_attach_pipeline` deleted |
| `core_phases.rs` | (VM2–VM4 intermediate) | Opcode-owned Core phase bodies |
| `kernel.rs` | `residiuum-query-kernel-sda-v1` | `where` meaning via SDA boolean programs |
| `core_page.rs` | (shared) | Scan/index scaffolding residual |
| `full_attach.rs` | Full profile | Compile Full + execute entry + attach bodies |

### 1.3 Durable carriers

| Carrier | Magic / role | Residual |
|---|---|---|
| **QVM1** | Public durable executable identity (`qvm.rs`) | Sole public authority on product path |
| **RQB1** | **Deleted (Q0.A10)** — former `isa.rs` / magic `RQB1` | **Not in tree.** No encode, decode, import, or execute path. Architecture gate forbids restoration. Historical only: [QUERY_ISA_V1.md](./QUERY_ISA_V1.md) (retired) |
| `VmProgram` / `VmPool` | In-memory ops + pool | No `RqlPlanV1` sidecar on pool (QVM1 labor) |
| `QueryBytecodeV1` | Envelope of QVM1 bytes | Public stored bytes are **QVM1** only; `from_isa_bytes` **removed** |

---

## 2. Product paths: compile → QVM → `run_vm`?

### 2.1 Paths that **do** enter one `run_vm` after QVM

| Surface | Entry | Path summary |
|---|---|---|
| Application Core RQL | `CollectionClient::rql` → `execute_core_rql` | compile App Core → `encode_qvm` → `execute_qvm_bytes` → `run_vm` |
| Bytecode envelope | `execute_bytecode` / `execute_qvm_bytes` | decode QVM → verify → `run_vm` |
| Full RQL (embedded) | `HeapClient::rql_full` / `execute_rql_full` | `compile_rql_full` → `lower_full` → `encode_qvm` → `execute_full_qvm_with` → `run_vm` |
| Full QVM bytes | `execute_full_qvm_with` | decode QVM → `run_vm` |
| Portable dialects | `Collection::find_dialect` sql/json/mongo | `CompiledPortable` → plan → `QueryBytecodeV1` → `execute_bytecode` → `run_vm` |
| ~~Legacy Core ISA import~~ | ~~`execute_isa_bytes`~~ | **Removed (Q0.A10)** — not a product path |
| ~~Legacy Full ISA import~~ | ~~`execute_full_isa_with`~~ | **Removed (Q0.A10)** — not a product path |
| Remote Core + bounded Full | op **118** `rql_query` | Omitted/`core` uses Core QVM; explicit `full` uses the same Full QVM executor over authorised collection-qualified host capabilities |

### 2.2 Paths that are **not** product QVM (allowed under invariant)

| Surface | Entry | Why allowed |
|---|---|---|
| Raw SDA dialect | `compile_dialect("sda")` / `Collection::find_dialect` SDA | Explicit raw SDA surface |
| Raw SDA API | `Collection::sda` / `sda_with` | Explicit SDA only |
| Document-predicate SDA filter | `filter_sda_with` via dialect SDA shape | SDA filter lane, not RQL product claim |

### 2.3 Honesty residuals on "product" paths (still not Decision 0 close)

| Residual | Detail |
|---|---|
| Dialect id binding | `Collection::find_portable_with` derives **synthetic** `CollectionId` / `HeapId` from collection **name** (stable, not Heap-durable). Official Heap product path is `CollectionClient::rql` with real ids. |
| Full vs Core profile | Application Core op-118 still refuses Full constructs; callers must explicitly select the Heap-bound Full profile through `HeapClient::rql_full`. This preserves fail-closed Core semantics while sharing the product wire and QVM executor. |
| Response diagnostics | `execute_full_qvm_with` may `reconstruct_attach_from_ops` for page metadata — **not** execute authority (QVM bytes are). |
| Public API surface | `execute_rql_full` remains a named entry; it is **not** a parallel semantic executor — it lowers to QVM first. |
| Kernel substrate | Filter meaning is SDA-evaluated text programs (`kernel.rs`), not a QVM micro-op for each compare. Host is still scan/index/get only. |

### 2.4 Deleted / forbidden product bypasses (gate-enforced)

| Symbol | Status |
|---|---|
| `fn run_core_page` | **deleted** (RQL-DEL1) |
| `fn execute_plan` | **deleted** (RQL-DEL1) |
| `fn run_attach_pipeline` | **deleted** (one executor) |
| Dialect id `rql` → SDA | **refuses** (RQL-R1) |
| `run_vm` calling `execute_plan` | **forbidden** by architecture gate |

Gate: `scripts/check_query_runtime_architecture.sh`.

---

## 3. Test-only oracles vs product paths

| Oracle / helper | Location | Product? |
|---|---|---|
| `Predicate::eval` | predicate module / kernel tests | **Test oracle only** — not product page path |
| Kernel `eq_oracle` tests | `kernel.rs` `#[cfg(test)]` | Test |
| `force_enrich_scan` | `RqlFullExecuteOptions` | Differential / oracle control on product entry; default false |
| Recursive within pre-pass helpers | `full_attach.rs` (marked test-only / DEL1) | Not product `Within` path |
| `dialects/rql` module | legacy RQL→SDA | **Test-only**; product refuses dialect `rql` |
| `Filter::to_sda` | filter comfort / oracles | Not the sql/json/mongo product path (those use portable→QVM) |
| Architecture script checks | `check_query_runtime_architecture.sh` | Meta gate, not runtime |

Independent **Q3** corpus oracle (deliberately unoptimised, test-only) is **not
yet** a landed package — see qualification programme Q3. Do not invent it here.

---

## 4. What still blocks principal Decision 0 / RQL-C1 close

Decision 0 is **architectural**: one semantic meaning path through canonical QVM
and one runtime. Labor has closed many sub-packages; principal has **not**
accepted C1. Blockers for a principal close (honest list):

1. **IR residual remains** — Core/Full semantics are still large Rust phase
   bodies interpreting typed QVM immediates. "Opcode vocabulary + one loop" is
   intermediate, not a pure stack machine. Principal previously rejected claims
   that overstated this.
2. **Prior reject stands** — VM1 / P1c / prior D0-closure claims remain
   **rejected**; re-labeling labor as accept is forbidden.
3. **Principal review of this inventory (D0.1) and readiness checklist (D0.2)**
   — human gate; labor cannot self-accept.
4. **Wire completeness residual** — Full language not on op-118; Core wire only.
   Does not alone open Decision 0 if Core path is one runtime, but blocks any
   claim that "all product frontends including Full wire" are identical.
5. **Dialect Heap-id honesty** — portable dialect execute on `Collection` uses
   store-scoped durable ids (store_id + name, Q0.A6); product Heap RQL uses catalog CollectionId. Same `run_vm`, different
   host identity story.
6. **Capability gaps are separate** (aggregates, nested within-index, Tier-A
   corpus, etc.) — those are **Q2/Q3 programme** work, not substitutes for
   Decision 0 honesty. Closing C1 while Tier-A is incomplete would still be
   wrong if the one-runtime invariant is the C1 bar; today both are open.

**What does *not* need to re-open as "second executor" claims:**

- Deleted `query_exec_v1` / fused orchestrators (gone, gated).
- sql/json/mongo compiling to SDA text (DQ1 closed — portable → QVM).
- Two dispatch loops for Core vs Full (VM1R closed — one `run_vm`).

---

## 5. Companion docs to keep honest

| Doc | Note after this inventory |
|---|---|
| [QUERY_IR_RESIDUAL.md](./QUERY_IR_RESIDUAL.md) | Phase ledger; must not claim sql/json/mongo still → SDA |
| [QUERY_VM_V1.md](./QUERY_VM_V1.md) | Opcode map; NEXT labor is residual IR honesty / principal, not "dialect→QVM" as if DQ1 open |
| [RQL_WHAT_IS_LEFT.md](./RQL_WHAT_IS_LEFT.md) | Programme residual order; cites this inventory for D0 |
| [QUERY_RUNTIME_CONVERGENCE.md](./QUERY_RUNTIME_CONVERGENCE.md) | Charter; Decision 0 still open |

---

## 6. Evidence anchors

- `bash scripts/check_query_runtime_architecture.sh` exit 0 @ inventory date
- Labor evidence logs under `doc/todo/rql/evidence/` (R1, QVM1, VM1R, DQ1, WIRE1, DEL1, VM0–VM4)
- Sources: `query_bytecode_v1/{vm,vm_exec,qvm,core_phases,ir_*,kernel,full_attach,mod}.rs`
- Dialects: `crates/residiuum-sdk/src/dialects/mod.rs` (portable path banner)
- Collection portable execute: `collection.rs` `find_portable_with`
- Façade Core: `app_v1.rs` `CollectionClient::rql` → `execute_core_rql`

---

## 7. One-line verdict

```text
Decision 0 = OPEN
RQL-C1     = FORBIDDEN
Landed     = QVM1 public path; one run_vm; dialects portable→QVM; DEL1 orchestrators gone
Residual   = IR = Rust phase bodies; Full not on Core wire; dialect store-scoped ids (not Heap catalog UUID residual); principal reject stands
Next labor = principal review (D0.2 checklist: RQL_D0_CLOSE_READINESS.md); no false C1 accept
```
