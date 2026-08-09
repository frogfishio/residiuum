# RQL-Q2.1 — Corpus compile/execute capability audit

Status: **labor complete** (2026-08-07) · package RQL-Q2 **not accepted**  
**Re-audit after Q2.2 wave-1 (`pkg_group_aggregate`, 2026-08-08):** execute_ok **107**/147 (was 91); gap **38** (was 54); `pkg_group_aggregate` **0 blocked** (was 16). Machine report refreshed.
**Re-audit after Q2.2b enrich dialect (2026-08-08):** execute_ok **122**/147; gap **23**; `pkg_enrich_corpus_dialect` **0**.  
**Re-audit after Q2.2c array predicates (2026-08-08):** execute_ok **129**/147; gap **16**; `pkg_array_predicate_surface` **0**.
**Re-audit after Q2.2d budget partial coverage (2026-08-08):** execute_ok **134**/147; gap **11**; `pkg_budget_partial_coverage` **0**.
**Re-audit after Q2 closure tranche (2026-08-09):** execute_ok **145**/147;
expected stable refusal **2**/147; **gap 0**. Computed conditional projection,
textual authenticated continuation, and the grammar-aligned unread-within case
all compile and execute through QVM. Full server wire remains separate.
Task: Q2.1 · Feature `019fda4c-1227-7c93-b7e6-292141ec7a78`  
Authority: [RQL_QUERY_QUALIFICATION_PROGRAM.md](./RQL_QUERY_QUALIFICATION_PROGRAM.md) §5  
Machine report: [`spec/rql/qualification/corpus-v1/q2_1_capability_audit.json`](../../../spec/rql/qualification/corpus-v1/q2_1_capability_audit.json)  
Harness: `cargo test -p residiuum-sdk --test rql_q2_capability_audit` (writes
`target/rql-q2/`); publish the reviewed snapshot with
`RESIDIUUM_WRITE_SPEC_EVIDENCE=1 cargo test -p residiuum-sdk --test rql_q2_capability_audit`

## 1. What was audited

Every **Tier-A** case in corpus `rql-q1-corpus-v0.4.2` (147 cases) was run on the
**product** path only:

1. **Compile:** `compile_rql_full` (Full surface; Application Core base via the same stack)
2. **Execute:** `execute_rql_full` → QVM1 (`run_vm`) on an embedded heap seeded from
   `tools/rql_q1/materialise_fixture.py`

No silent skips. Unsupported outcomes are **error/refusal classification**, never empty success.

### Non-claims

- Not Q3 oracle / differential correctness
- Not Gate-1 pass
- Not Decision 0 / RQL-C1 close
- Execute **ok** means the product path returned a page; row correctness is Q3

## 2. Initial-audit numbers (historical)

| Metric | Value |
|---|---:|
| Tier-A cases | **147** |
| Compile ok | **96** |
| Execute ok (product expressible) | **91** |
| Expected stable refusal (offset discard) ok | **2** |
| Gap cases (need Q2.2 packages) | **54** |
| Failure classes (gaps) | semantic **31**, syntax **23** |
| Corpus tip at audit | `rql-q1-corpus-v0.4.0` |
| Workspace tip at audit | `96b526fdb3ed55959db77abd5ee41b0d02f45dc1` |

**Product expressible ≈ 62% of Tier A** (91/147).  
With the two expected refusals counted as closed-for-audit, **93/147** cases have a
stable product outcome; **54** remain implementation gaps for Q2.2.

## 3. Failure classification (programme §5 step 1)

| Class | Count | Meaning in this audit |
|---|---:|---|
| `semantic` | 31 | Feature missing or wrong product behaviour (group/agg, `if/then` project, `after` clause, budget hard-fail vs partial coverage) |
| `syntax` | 23 | Source does not parse on product grammar (corpus enrich dialect, array `[]` / `contains`, `within` shape) |
| `expected_refusal` | 2 | Offset-discard cases correctly refused at compile |
| `compiler` / `qvm` / `index` / `wire` / `host` | 0 | No residual in this run after param binding fix |

Wire path was **not** in scope (embedded Full path only). Full-over-wire remains the
separate Q2-BLOCK-FULL-WIRE residual from Q0.

## 4. Initial implementation package order (historical; now closed embedded)

Ordered by **number of Tier-A cases blocked**. Q2.2 must freeze semantics in
`RQL_SPEC.md` before implementing each package.

| Rank | Package id | Cases blocked | What to do |
|---:|---|---:|---|
| 1 | `pkg_group_aggregate` | **16** | Freeze group-by + count/sum/min/max/avg; compile → QVM → one runtime; oracle/mutation tests |
| 2 | `pkg_enrich_corpus_dialect` | **15** | Align corpus RQL with product enrich grammar (`using … matching … expect`) **or** accept corpus dialect in compiler; then re-audit enrich cardinality |
| 3 | `pkg_array_predicate_surface` | **7** | Empty-array literal `= []`, bag `contains`, related array predicates |
| 4 | `pkg_budget_partial_coverage` | **5** | Budget exhaust must yield **partial page + incomplete coverage**, not hard `ResourceLimit` abort (oracle already requires this) |
| 5 | `pkg_computed_conditional_project` | **5** | `project x = if … then … else …` (computed/conditional projection) |
| 6 | `pkg_cursor_after_clause` | **5** | Source `after $cursor` vs product `QueryRunOptions.after` — freeze one surface; multipage resume expressible in corpus RQL |
| 7 | `pkg_enrich_semantics` | **1** | Residual after dialect align: `within` / nested unread shape (`messaging.messages.within_conversation_unread`) |

### Package notes

**`pkg_enrich_corpus_dialect` (rank 2)** is not “enrich is missing” alone. Product
already executes product-shaped enrich (see Phase-3 enrich tests). Corpus sources use:

```text
enrich customer from customers on customer.id = customers._key exactly_one
```

Product grammar is:

```text
enrich customer using customers matching customer_id = id expect exactly_one
```

Fixing dialect unlocks re-measurement of true enrich cardinality gaps (exactly_one /
optional / many) without conflating syntax with semantics.

**`pkg_budget_partial_coverage`:** compile succeeds; execute fails closed with resource
limit even under `CoveragePolicy::IncompleteAllowed`. Oracle text wants incomplete
coverage when budgets exhaust — this is a semantic product gap, not a harness issue.

**Closure:** textual `after $cursor` now accepts only an opaque string or byte
parameter, removes that cursor binding from semantic parameter hashing, and
executes the same authenticated continuation verifier as `QueryRunOptions.after`.
The audit obtains a real first-page token and resumes it; it does not use a
synthetic cursor.

## 5. What already works (no gap package)

Roughly **selection / projection-flat / order+limit / missing-null / basic predicates**
across all five domains compile and execute on QVM. Family-tag hits among expressible
cases (overlap OK):

| Family tag | Expressible cases (approx.) |
|---|---:|
| `selection_key_eq_range_compound` | 48 |
| `predicate_missing_null_type_nested_array` | 21 |
| `order_topk_cursor` | 15 |
| `projection_computed_conditional` | 10 (flat project only; computed `if` still gap) |
| `budget_coverage_damage_refusal` | 6 (non-budget or non-exhausting cells) |

Stable refusals confirmed at compile for:

- `commerce.orders.refuse_offset_discard`
- `project_management.projects.refuse_offset_discard`

## 6. How to re-run

```sh
cargo test -p residiuum-sdk --test rql_q2_capability_audit -- --nocapture
```

Rewrites `spec/rql/qualification/corpus-v1/q2_1_capability_audit.json`.
Exit 0 = **audit completed** (all Tier-A cases classified), **not** 100% expressible.

## 7. Exit for Q2.1 (this task)

| Criterion | Status |
|---|---|
| Every Tier-A case through compile + product execute | **yes** |
| Failures classified (semantic / syntax / …) | **yes** |
| Machine-readable gap report | **yes** (`q2_1_capability_audit.json`) |
| Recommended implementation package order | **yes** (§4) |
| No silent skips / empty success for unsupported | **yes** |
| Q2 package accept / 100% Tier-A expressible | **no** (Q2.2+) |

## 8. Decision-0 honesty

Decision 0 remains **OPEN**. This audit uses the public Full → QVM path but does
**not** claim one-runtime exit or RQL-C1 accept. Q2.3 still owns frontend QVM identity
and one-runtime exit packaging — see [RQL_Q2_3_FRONTEND_QVM_EXIT_PACK.md](./RQL_Q2_3_FRONTEND_QVM_EXIT_PACK.md)
(labor complete; **Q2 package exit still blocked**).
