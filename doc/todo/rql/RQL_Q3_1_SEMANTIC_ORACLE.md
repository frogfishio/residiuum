# RQL-Q3.1 — Independent semantic oracle (test-only)

Status: **labor exit ready → principal package review** (2026-08-09) · package **not yet accepted**
Package: RQL-Q3 · Feature `019fda4c-5994-77e2-a2c9-aaa0c3097b29` · Task Q3.1  
Authority: [RQL_QUERY_QUALIFICATION_PROGRAM.md](./RQL_QUERY_QUALIFICATION_PROGRAM.md) §6.1  
Equivalence profile: [RQL_Q0_RESULT_EQUIVALENCE.md](./RQL_Q0_RESULT_EQUIVALENCE.md)  
Corpus: [`spec/rql/qualification/corpus-v1/`](../../../spec/rql/qualification/corpus-v1/)

## 1. Goal

Provide one **deliberately unoptimised**, **test-only** semantic oracle that can
check corpus expected results **independently of Residiuum plan selection /
index pushdown**.

## 2. Boundary (hard firewall)

| May | Must not |
|---|---|
| Read complete **logical** fixtures (`generator_id` + `seed` via `tools/rql_q1/materialise_fixture.py`) | Be callable as a product query path |
| Use [`compile_app_core`](../../../crates/residiuum-sdk/src/rql_app_core.rs) for **plan structure** (language meaning) | Call `CollectionClient::rql`, `execute_rql_full`, QVM `run_vm` |
| Evaluate `where` with [`Predicate::eval`](../../../crates/residiuum-sdk/src/predicate.rs) (total predicate profile / APP-4) | Share **optimiser / index-selection** with the IUT |
| Full-scan every document in the root collection | Depend on store media, equality indexes, or host index probes |
| Project / enrich / within / group / order / limit in pure Rust over the working set | Claim Gate-1 or self-accept Q3 |

**Profile stamp:** `residiuum-rql-q3-semantic-oracle-v1`  
(distinct from product `residiuum-query-bytecode-v1`, `residiuum-app-core-exec-v1`, `rql-full-v1`)

Location: **integration test only** —
`crates/residiuum-sdk/tests/rql_q3_semantic_oracle.rs`  
(not exported from `residiuum-sdk` lib surface).

## 3. Evaluation model

```text
logical fixture (all collections for case)
  → compile_app_core(source) → RqlPlanV1
  → full scan root collection
  → Predicate::eval (with _key / $key bound to immutable key)
  → optional group/aggregate (pure)
  → optional order (pure; _key/$key = key)
  → limit + first page_size truncate
  → enrich/within attach (pure complete-foreign-collection evaluation)
  → path/brace/computed project (pure; missing omitted)
  → digests over keys + values + coverage
```

`_key` / `$key` are treated as the **immutable document key** on the logical
fixture. Product store bodies strip `_key` after put; Q2 capability audit can
report `row_count=0` for key-get while still “execute ok”. The oracle is the
independent expected-result authority for those cases.

## 4. Evidence

| Artefact | Path |
|---|---|
| Suite | `cargo test -p residiuum-sdk --test rql_q3_semantic_oracle` |
| Verify script | `bash scripts/verify-rql-q3-oracle.sh` |
| Machine report | `spec/rql/qualification/corpus-v1/q3_1_oracle_report.json` |
| Runtime copy | `target/rql-q3/q3_1_oracle_report.json` |

### Latest run (labor)

| Metric | Value |
|---:|---:|
| Tier-A semantic sources considered | **144** |
| `oracle_ok` (deterministic dual-run digests) | **144** |
| Required stable refusals / explain contract | **2 / 1** |
| `oracle_unsupported` | **0** |
| Complete Tier-A package denominator | **147/147 green; 0 residual — LABOR EXIT READY** |
| compile / eval / fixture fail | **0** |
| Hand unit checks | **6** (eq, key, missing/null, order/limit, project, profile firewall) |

The complete Tier-A denominator is explicitly classified: 144 semantic-result
cases, two required stable offset refusals, and one explain-without-rows
contract. The pure evaluator covers group/aggregate, computed conditional and
nested brace projection, cardinality-aware enrich, and nested `within` filter.
Aggregate comparison ignores only executor-internal synthetic group row keys;
all values, multiplicity, ordering declarations and coverage remain compared.

## 5. Non-claims

- Not Gate-1 pass; not RQL-Q3 package accept.
- Not differential matrix (Q3.2) or adversarial/damage one-command suite (Q3.3).
- Does not close Decision 0 / RQL-C1.
- Does not replace product execution; digests are oracle-side expected anchors
  for later IUT comparison.
- Historical corpus `deferred_q2` labels are not rewritten here; Q1 amendment
  remains a principal-controlled package action.

## 6. Exit checklist (Q3.1)

- [x] Test-only module with documented product-path firewall
- [x] Full logical-fixture scan; no index selection
- [x] Corpus `oracle_rule` cases run expected-result checks (digests + determinism)
- [x] Hand fixtures with known answers (including missing≠null and key get)
- [x] One-command verify script + machine report
- [ ] Principal package accept (not labor)
- [x] Complete Tier-A denominator green (147/147)

## 7. Next (Q3.2)

Wire differential equality:

```text
reference_oracle(Q) == forced_scan_QVM(Q) == admitted_index_plans(Q) == …
```

using this oracle as `reference_oracle`.

## Evidence write policy (F8)

Default tests write under `target/rql-q3/` only. Checked-in `spec/` snapshots update only with `RESIDIUUM_WRITE_SPEC_EVIDENCE=1` or `scripts/publish-rql-q3-evidence.sh`.
