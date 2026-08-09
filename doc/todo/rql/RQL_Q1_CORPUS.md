# RQL-Q1 — Practical query corpus (human report)

Status: **Q1.4 floors + comparator review landed** (2026-08-07) · package **not accepted**  
Package: RQL-Q1 · Feature `019fda4c-11fd-7102-bd55-10a347802144`  
Authority: [RQL_QUERY_QUALIFICATION_PROGRAM.md](./RQL_QUERY_QUALIFICATION_PROGRAM.md) §4  
Machine corpus: [`spec/rql/qualification/corpus-v1/`](../../../spec/rql/qualification/corpus-v1/)

## 1. Delivery shape (fixed)

Programme + Q0 index require **exactly**:

1. Machine-readable corpus data (schema + versioned cases + fixtures)
2. **One** short human report (this file)

Not another multi-document Q0-style freeze family.

## 2. Version identity

| Field | Value |
|---|---|
| Format | `residiuum-rql-q1-corpus-v1` |
| Profile | `rql-gate1-practical-corpus-v1` |
| Corpus version | **`rql-q1-corpus-v0.4.2`** (Q2 within case aligned to frozen enrich/within grammar; amendment pending principal disposition) |
| Equivalence profile | `rql-q0-result-equivalence-v1` |
| Q0 freeze tip (authority) | `e1f5c670a99dc54da477c531c83bca4985199a42` |
| Live cases | **153** (A=147 / B=2 / C=4); status `ready` (not `frozen`) |
| Generators | [`generators/`](../../../spec/rql/qualification/corpus-v1/generators/) + `tools/rql_q1/materialise_fixture.py` |
| Floor enforcement | **`enforce_floors=true`** |

## 3. Case record contract

Every case must carry programme §4.2 fields. Normative JSON Schema:

- Wrapper: `spec/rql/qualification/corpus-v1/corpus-v1.schema.json`
- Case: `spec/rql/qualification/corpus-v1/corpus-case-v1.schema.json`

| Group | Fields |
|---|---|
| Identity | `case_id`, `tier` (A/B/C), `domain` (five required domains) |
| Intent | `plain_english_intent` |
| Fixture | `fixture.generator_id` + `fixture.seed` (+ optional params) |
| Expected | `expected.kind` ∈ literal / oracle_rule / stable_refusal / deferred_q2 |
| Order | `ordering_and_multiplicity` |
| Engines | `implementations.rql` + `.mongo` + `.cbl` (source, pipeline/find, sqlpp/builder, or refusal) |
| Indexes | `indexes.required` / `optional` |
| Classes | `selectivity_class`, `cardinality_class` |
| Variants | `variants.missing_null_type`, `variants.cursor_page` |
| Exclusion | `exclusion_or_refusal` |
| Floors | `family_tags` (see §4) |
| Optional | `capability_ids` (Q0 matrix links), `dogfood`, `status` |

Intention + expected result are **authority**. RQL / Mongo / CBL are implementations of that intention. Expected results must not depend on Residiuum optimiser choices.

## 4. Distribution floors (§4.3)

Floor measurement = **count of cases listing each `family_tags` entry** (overlap OK).

| Family tag | Floor | Count after Q1.4 |
|---|---:|---:|
| `selection_key_eq_range_compound` | 20 | **53** (OK) |
| `predicate_missing_null_type_nested_array` | 20 | **31** (OK) |
| `projection_computed_conditional` | 15 | **16** (OK) |
| `order_topk_cursor` | 15 | **23** (OK) |
| `enrichment_cardinality` | 15 | **16** (OK) |
| `group_aggregate` | 15 | **17** (OK) |
| `budget_coverage_damage_refusal` | 10 | **17** (OK) |

`floor_policy.enforce_floors` is **`true`**. `bash scripts/verify-rql-q1-corpus.sh` fails if any family falls below its floor.

## 5. Amendment process (principal-reviewed)

### 5.1 When an amendment is required

Any change that alters:

- case intention, expected result, oracle rule, ordering/multiplicity;
- Tier A inclusion or deliberate exclusion / refusal code;
- floor policy constants or measurement rule;
- RQL/Mongo/CBL forms in a way that changes comparable answer semantics;

requires a **versioned, principal-reviewed** amendment. Typos in notes alone may be PATCH with explicit log entry.

### 5.2 Procedure

1. Open labor against Feature RQL-Q1 (or a named amendment card).
2. Bump `corpus_version` (`rql-q1-corpus-vMAJOR.MINOR.PATCH`).
3. Append `amendment_log` with date, summary, case id adds/changes/archives, disposition `pending`.
4. Prefer **archive + new `case_id`** over in-place redefinition of a previously frozen id.
5. Run `bash scripts/verify-rql-q1-corpus.sh` (must stay green; floors enforced).
6. Principal sets disposition `accepted` / `accepted_with_amendments` / `rejected` on the log entry.
7. Only after principal package accept may scoreboard `RQL-Q1` move to `accept` and cases to `frozen`.

### 5.3 Semver rules (also in corpus JSON)

- **MAJOR** — remove or redefine frozen case meaning; change floor policy.
- **MINOR** — add cases or non-breaking optional fields after principal accept.
- **PATCH** — notes, generator params that do not change intention/result.

### 5.4 Forbidden

- Post-hoc exclusion of a diverging cell after measurement (equivalence anti-escape).
- Silent edit of a frozen `case_id` without log + version bump.
- Claiming package floors while `enforce_floors` is false (no longer applicable after Q1.4).
- Treating APP-5 / full-v1 surface corpora as this Tier-A corpus.

## 6. Task plan

| Task | State (labor) | Deliverable |
|---|---|---|
| Q1.1 schema + versioning + amendment | `in_review` | Scaffold |
| Q1.2 Commerce + Messaging | `in_review` | domain bulk + generators |
| Q1.3 Directory + Telemetry + Project | `in_review` | five domains + floor tag fill |
| Q1.4 floors + comparator review | **this delivery** → `in_review` | enforce floors; comparator honesty; B/C tags; §2.1 gap fill |

## 7. Domain bulk + Q1.4 adds

| Domain | Cases (v0.4.0) | Collections (generators) |
|---|---:|---|
| Commerce | 35 | orders, products, customers, line_items, inventory |
| Messaging | 23 | conversations, messages, participants |
| Directory | 29 | entries, categories, locations |
| Telemetry | 32 | devices, events, metrics |
| Project management | 34 | projects, tasks, revisions, memberships |
| **Total** | **153** | |

### Dogfood honesty

No in-tree Residiuum dogfood datasets for the five domains were found (2026-08-07).
All cases use `dogfood.origin = invented_honest_label` with shapes aligned to programme
§4.1. Real dogfood can replace generators under a versioned amendment (archive + new
case ids if meanings change). Programme “≥2 domains dogfood-derived” remains residual
until real datasets exist.

### Expected results

- Optimiser-independent `oracle_rule` or `deferred_q2` / `stable_refusal` text on every case.
- RQL forms present as `source` where app-core surface allows; `pending` for Q0 blockers
  (enrich wire, aggregates, computed projection, explain, within, prefix) with Mongo/CBL
  comparator forms retained where equivalence holds.
- Native Residiuum surfaces (budget, coverage, consistency, explain shape) marked
  `predeclared_native_diff`; Mongo/CBL use `lane_local_only` or `deliberate_exclusion`
  — never competitive `find`/`pipeline`/`sqlpp` under that exclusion kind.

### Generators

Documented under `spec/rql/qualification/corpus-v1/generators/`.
Executable materialiser:

```sh
python3 tools/rql_q1/materialise_fixture.py --generator directory.entries_v1 --seed 20 --params '{"n_entries":40}'
python3 tools/rql_q1/materialise_fixture.py --generator telemetry.events_v1 --seed 30 --params '{"n_events":128}'
python3 tools/rql_q1/materialise_fixture.py --generator project_management.tasks_v1 --seed 40 --params '{"n_tasks":80}'
```

Same seed ⇒ identical JSON (determinism smoke checked in labor).

## 8. Q1.4 comparator review (semantic equivalence)

Review law: [RQL_Q0_RESULT_EQUIVALENCE.md](./RQL_Q0_RESULT_EQUIVALENCE.md) + Q0 matrix classes.

### 8.1 Competitive path (Tier A, exclusion kind `none`)

- Mongo `find` / `pipeline` and CBL `sqlpp` must express the **same intention** as RQL
  (values, keys, multiplicity, order, continuation where declared).
- Document-native differences allowed only where Q0 equivalence names them
  (e.g. Mongo `$regex: ^Acme` ≡ binary prefix; CBL `LIKE 'Acme%'`).
- RQL `pending` + expected `deferred_q2` is honest for Q0 blockers (enrich, group/agg,
  computed project, within, explain) — comparator forms still document target semantics.

### 8.2 Predeclared native diffs (not competitive equality)

| Code | Surface | Mongo/CBL status after Q1.4 |
|---|---|---|
| `Q0-BUDGET-NATIVE` | document/byte/result budgets | `lane_local_only` (limit ≠ budget) |
| `Q0-COVERAGE-NATIVE` | coverage incomplete / damage honesty | `deliberate_exclusion` |
| `Q0-CONSISTENCY-MODE` | consistency mode labels | `lane_local_only` |
| `Q0-EXPLAIN-SHAPE` | explain programme honesty | `lane_local_only` |

Fixed in v0.4.0 (were competitive `find`/`sqlpp` under `predeclared_native_diff`):

- `commerce.orders.budget_documents`
- `directory.entries.budget_documents`
- `project_management.tasks.budget_documents`
- `messaging.messages.consistency_available`
- `telemetry.events.consistency_available`

Validator now fails if `predeclared_native_diff` cases keep competitive engine statuses.

### 8.3 Stable refusals

- Offset discard remains Tier A **refusal** cases (`rql_offset_discard_unsupported`) —
  competitors may implement skip/offset; that is not Residiuum-equivalent product path.
- Tier C full-text / vector / geo / change-stream cases are stable refusals with
  deliberate exclusions on comparators (named backlog, not silent holes).

### 8.4 Tier B/C (non-blocking)

| Tier | Cases (v0.4.0) | Role |
|---|---|---|
| B | `telemetry.events.array_transform_tag_upper`, `project_management.tasks.named_component_open_by_owner` | Expansion; RQL deliberate exclusion until promoted |
| C | FTS, vector, geo, change stream | Explicit deferred; stable refusal |

Tier B/C do **not** block Q1 exit or Gate-1 first pass (programme §2.2–§2.3).

### 8.5 §2.1 coverage honesty (Tier A)

Family floors + intentional gap fill cover programme §2.1 surfaces at corpus-intention
level (selection, null/type/nested/array, project, order/page, enrich, group/agg,
budget/coverage/consistency, explain, within, string prefix, SQL-offset refuse).

Many remain `deferred_q2` / RQL `pending` until Q2 capability closure — that is corpus
**intention** completeness, not product expressibility.

## 9. Validation evidence

```sh
bash scripts/verify-rql-q1-corpus.sh
```

Expected: exit 0; live cases validate; **`enforce_floors=true`**; all family tags ≥ floors;
tier counts printed; native-diff competitive-status check green.

## 10. Residual (package exit)

- Principal disposition on amendment log v0.4.0 + scoreboard `RQL-Q1` → `accept`.
- Dogfood promotion when real datasets exist (≥2 domains programme law).
- Freeze canonical QVM hashes (many RQL `pending` until Q2).
- Case status `ready` → `frozen` only on principal package accept.
- Labor must not mark scoreboard or board `done` without principal accept.
