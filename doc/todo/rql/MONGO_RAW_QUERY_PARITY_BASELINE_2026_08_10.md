# Mongo raw-query parity baseline — 2026-08-10

Status: **proof programme active** · **Mongo parity not yet proven**

This baseline answers two distinct questions:

1. Can RQL express the ordinary raw-document questions expected of MongoDB?
2. Do Residiuum and MongoDB return equivalent answers for those questions, and
   are their query and aggregate execution characteristics at least comparable?

The answer at this baseline is:

- **RQL is already adequate for a substantial application-query core.**
- **RQL is not yet feature-complete against ordinary Mongo query/aggregation.**
- **No cross-engine semantic or performance parity claim is currently valid.**

The existing 147-case Tier-A corpus is strong Residiuum qualification evidence,
but it is not a complete Mongo capability denominator and Mongo has not executed
the comparator forms in that corpus.

## 1. Evidence recovered and rerun

On 2026-08-10:

| Evidence | Result | What it proves |
|---|---:|---|
| Q1 corpus verifier | A=147, B=2, C=4; all family floors green | The frozen corpus is structurally complete against its declared floors. |
| Q2 product capability audit | 143 execute, 2 stable refusals, 2 semantic gaps | 145 cases have a closed embedded-product outcome; two result-byte-budget cases currently fail the intended partial-coverage contract. |
| Q3 independent oracle | 147/147 green | Product results agree with the repository's independent oracle for the corpus. |
| Q3 differential matrix | 147/147 green | Scan/index/frontend paths agree for the corpus. |
| Q3 adversarial suite | 17/17 green | Named missing/null/type, damage, budget, cursor, reopen and mutation hazards are covered. |
| Q3 page-concat suite | 6/6 green | Authenticated pages concatenate to the unpaged answer for admitted cases. |
| Mongo comparator | deferred for every Q3 case | Mongo equivalence is **not** proven. |

The corpus contains 136 Tier-A cases with a declared competitive Mongo form
(99 `find`, 37 aggregation pipelines). Eleven Tier-A cases are intentionally
lane-local or excluded because they test Residiuum-native budget, coverage,
damage or explain contracts. Declaring a Mongo form is not the same as executing
it.

The two live Q2 gaps are
`messaging.messages.budget_cancel_surface` and
`telemetry.events.budget_result_bytes`. Both compile but return
`QueryBudgetRequired` when result materialisation exceeds 4096 bytes; their
declared `pkg_budget_partial_coverage` outcome expects a bounded incomplete
result. These cases are Residiuum-native rather than Mongo parity cells, but the
regression invalidates the stale repository claim of 145 successful executions.

## 2. Frozen parity boundary

“Ordinary raw-data parity” includes:

- point lookup and document selection;
- equality, inequality, range, set-membership and boolean predicates;
- missing, null, type, nested-object and array semantics;
- flat, nested, computed and conditional projection;
- deterministic ordering, top-k and bounded pagination;
- equality joins / lookups and one-to-one, optional and one-to-many results;
- grouping, distinct, count, sum, minimum, maximum and average;
- unwinding arrays into rows;
- useful multi-stage composition, including filtering and reshaping aggregate
  output; and
- ordinary string, arithmetic and date expressions needed in filters and
  projections.

Excluded from the first parity claim: geospatial, full-text/search indexes,
vector search, graph traversal, change streams, write pipelines, server-side
JavaScript/custom accumulators, sharding and analytics-scale external spill.

## 3. Current capability verdict

| Family | Residiuum state | Proof state | Baseline verdict |
|---|---|---|---|
| Point get; equality/range/compound selection; AND/OR/NOT; IN | Implemented | Q2/Q3 corpus green | **Core adequate; Mongo differential pending** |
| Missing versus stored null | Implemented with an explicit distinction | Q3 adversarial green | **Core adequate; translation must account for Mongo's different null shorthand** |
| Nested paths | Implemented | Q2/Q3 corpus green | **Core adequate; Mongo differential pending** |
| Arrays | Empty-array equality and containment implemented | Limited Q2/Q3 coverage | **Partial** — no proved equivalents for Mongo `$all`, `$elemMatch` and `$size` |
| String predicates | Prefix implemented | Corpus proof for prefix | **Partial** — regex and the ordinary string-expression family are absent/unproved |
| Projection | Flat, nested, path/literal and bounded conditional projection implemented | Q2/Q3 corpus green | **Partial** — arbitrary arithmetic/function expressions are outside v1 |
| Ordering/top-k | Deterministic multi-field order and immutable tie-break implemented | Q3/Q4 product green | **Core adequate** |
| Pagination | Authenticated continuation and exact page-concat implemented; offset discard refused | Q3 green | **Equivalent practical result, deliberately different mechanism** |
| Lookup/enrichment | Exactly-one, optional and many cardinalities implemented | Q3/Q4 product green | **Core adequate; Mongo `$lookup` differential pending** |
| Group + count/sum/min/max/avg | Implemented through QVM | Q2/Q3 corpus green | **Basic aggregate core adequate; Mongo differential pending** |
| Distinct / count-distinct | No accepted product proof | Missing from effective denominator | **Gap** |
| Array unwind / unnest | No RQL row-expansion operator | Not covered as an RQL capability | **Gap** |
| Post-group filtering (`HAVING`) | Explicitly outside current slice | Not covered | **Gap** |
| Rich accumulators (`push`, set, first/last/top-N, deviation, percentile) | Not in current Tier-A runtime | Not covered | **Gap**, except those deliberately excluded from the first profile |
| Multi-stage/faceted pipelines | Only the ordered RQL attach/filter/project path and basic group phase | No general pipeline-composition proof | **Gap/partial** |
| Arithmetic/date expression families | Outside bounded computed v1 | Not covered | **Gap** |
| Explain/index use | Product explain and admitted indexes exist | Product-local evidence only | **Not cross-engine comparable yet** |

## 4. Why 147/147 is not Mongo parity

The corpus denominator was designed around practical application domains and
minimum family floors. It successfully drove the implemented core to 147/147,
but it does not contain a one-to-one case for every capability frozen in the Q0
matrix. In particular:

- Mongo `$unwind` appears in comparator pipelines used to model enrichment; no
  RQL case proves general array-to-row expansion.
- the current group slice explicitly excludes count-distinct and `HAVING`;
- Mongo's wider array predicate and expression families are not represented;
- regex, general string expressions, arithmetic and date expressions were
  named as Tier-A blockers but were not forced into the effective 147-case
  denominator; and
- Q3's comparator field is `deferred_q4` for all cases.

Therefore “147/147 green” means **the current RQL corpus is correct**, not **RQL
is on par with MongoDB**.

## 5. Proof programme from this baseline

### P1 — capability-denominator repair

Create a Mongo raw-query capability matrix independent of the existing corpus.
Every included family must end in exactly one state:

- equivalent and cross-engine green;
- equivalent but not yet measured;
- deliberate first-profile exclusion; or
- Residiuum implementation gap.

Add missing executable cases for array quantifiers/element matching/size,
distinct, unwind, regex/string expressions, arithmetic/date expressions,
post-group filtering and pipeline composition.

### P2 — real Mongo semantic adapter

Replace `MongoLocalAdapter`'s `adapter_not_configured` outcome. Load identical
canonical JSON fixtures into a pinned local MongoDB, execute the corpus's
declared `find`/pipeline form, canonicalize values under the Q0 equivalence
profile, and compare result values, multiplicity, ordering and explicit
missing/null behavior.

Start with the 136 already-declared comparable Tier-A cases. A case is not
green when either engine uses a simulator, an invented digest or an
application-side scan to manufacture the answer.

### P3 — aggregate semantic edge matrix

For count/sum/min/max/avg, compare at least:

- global and multi-key groups;
- empty input and empty contributing sets;
- missing, null, wrong-type and mixed numeric inputs;
- integer/decimal behavior and overflow/refusal;
- duplicate values and group-key multiplicity;
- order and page behavior after grouping; and
- deterministic canonical output independent of physical plan.

### P4 — query-plan and performance qualification

Only cross-engine-green families enter performance work. Use identical logical
fixtures and equivalent indexes, record chosen plans and documents/index entries
examined, then measure warm, reopen and device-cold classes with raw repetitions.
Latency or throughput without result-digest equality is invalid evidence.

## 6. Acceptance bars

The first ordinary Mongo parity claim requires:

1. every included capability classified with no hidden denominator gaps;
2. 100% of equivalent cases executed by real Residiuum and Mongo product paths;
3. exact canonical result equality, including multiplicity and ordering where
   observable;
4. explicit accounting for null/missing and numeric semantic differences;
5. zero application-side answer construction;
6. stable refusals for deliberate exclusions; and
7. query/aggregate performance compared only after semantic equality passes.

Until those bars are met, the accurate product statement is:

> Residiuum has a qualified, deep application-query core with nested document
> filtering, deterministic paging, enrichment and basic grouped aggregates.
> Full ordinary Mongo query and aggregation parity remains under active proof
> and has known gaps.
