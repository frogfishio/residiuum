# RQL-Q0 — Tier A/B/C capability matrix

Status: **Q0.A3 amendment · principal freeze re-accept pending**

Package: RQL-Q0 deliverable 2
Authority: [RQL_QUERY_QUALIFICATION_PROGRAM.md](./RQL_QUERY_QUALIFICATION_PROGRAM.md) §2–§3
Grounding: [RQL_SPEC.md](../../wip/query/RQL_SPEC.md), [RQL0_GAP_LEDGER.md](./RQL0_GAP_LEDGER.md)
Board task: Q0.2
Effective: 2026-08-07

## 0. Classification legend

| Class | Meaning |
|---|---|
| `exact` | RQL exposes the same result semantics directly |
| `document-native-equivalent` | Different expression, same practical result |
| `deliberate-exclusion` | Outside the frozen Gate-1 profile; stable refusal required |
| `blocker` | Required by frozen Tier A but not yet product-complete |

**Impl state** (honesty, not Gate-1 accept): `implemented` · `partial` · `absent`
from the gap ledger. Impl state ≠ matrix class.

**Owner** names the normative section or package that must freeze meaning before
behavior changes.

**Law:** no mandatory Tier-A semantic may remain `TBD`. Aggregates are Tier A in
the qualification programme even though RQL_SPEC v1 currently excludes them —
they are frozen here as **`blocker`** (SPEC must be amended in Q2 before product
syntax lands).

---

## 1. Tier A — mandatory Gate-1 surface

| id | Capability | Class | Impl | Owner | Residual / notes |
|---|---|---|---|---|---|
| TA-KEY | Key lookup / point get | `exact` | implemented | RQL_SPEC §3; Core path | Product Core path green |
| TA-SEL-EQ | Equality selection (root) | `exact` | partial | RQL_SPEC; gap ledger root `where` | Range/multi-field multipage residual |
| TA-SEL-RANGE | Range predicates | `exact` | partial | RQL_SPEC; index pushdown | Compound/range multipage residual |
| TA-SEL-COMPOUND | Compound predicates | `exact` | partial | RQL_SPEC; planner | Partial pushdown |
| TA-NULL | Total absent / null / value semantics | `exact` | partial | RESIDIUUM_PREDICATE_SPEC; SDA | Must not collapse holes to empty complete pages |
| TA-TYPE | Type-aware predicates | `exact` | partial | Predicate profile | Wrong-type adversarial cases required in Q3 |
| TA-NESTED | Nested-field predicates | `exact` | partial | RQL_SPEC; SDA path | |
| TA-ARRAY | Array predicates (bucket — prefer specific rows below) | `exact` | partial | RQL_SPEC; ENR/SDA | Prefer TA-ARR-* for corpus; residual completeness → Q2 |
| TA-ARR-ELEM | Array element match (any/all quantifier frozen on case) | `exact` | partial | RQL_SPEC; §2.5 equivalence | Quantifier must be case-frozen (A2 `arr.bag_pred`) |
| TA-ARR-NEST | Nested array predicates (no implicit flatten) | `exact` | partial | RQL_SPEC; A2 `arr.nested` | Flatten only if case freezes flatten |
| TA-ARR-DUP | Duplicate array elements / multikey bag semantics | `exact` | partial | Index/multikey; A2 `arr.dupes`/`arr.multikey` | Dropping dupes is fail |
| TA-UNWIND | Unwind / unnest array to rows | `blocker` | absent | RQL_SPEC amend + Q2 | Ordinary Mongo `$unwind` / SQL++ UNNEST class |
| TA-IN | `IN` / set membership | `exact` | partial | Predicate / Core | Parameterised lists; empty-set law on case |
| TA-DISTINCT | `DISTINCT` result rows | `exact` | absent | RQL_SPEC amend + Q2 | **Promoted from Tier B** (principal: ordinary Mongo/SQL++ work) |
| TA-STR-PREFIX | String prefix / starts-with | `exact` | partial | Predicate / index | Binary string profile (A2 `str.*`) |
| TA-STR-REGEX | Regex / pattern match | `blocker` | absent | RQL_SPEC amend + Q2 | Engine regex dialects differ — freeze dialect on case when implemented |
| TA-STR-EXPR | String expressions (concat, lower/upper, length, …) | `blocker` | absent | RQL_SPEC amend + Q2 | Case-fold algorithm frozen when case-insensitive |
| TA-ARITH | Arithmetic expressions in filter/project | `blocker` | absent | RQL_SPEC amend + Q2 | i64 overflow = refuse (A2 `int.overflow.*`) |
| TA-DATE | Date/time expressions and comparisons | `blocker` | absent | RQL_SPEC amend + Q2 | Timezone policy must freeze on case |
| TA-AGG-COUNT-DISTINCT | `COUNT DISTINCT` | `blocker` | absent | RQL_SPEC amend; APB-8 | Distinct from TA-AGG-COUNT |
| TA-COLLATION | Explicit collation on order/compare | `deliberate-exclusion` | absent | A2 `str.collation`; env | Primary profile = binary; locale collations out unless case freezes all engines |
| TA-PIPE-COMPOSE | Aggregation / query pipeline composition (multi-stage) | `blocker` | partial | Plan compose; Mongo pipeline class | Not only SQL GROUP BY — staged pipeline shape |
| TA-BOOL | Boolean composition (and/or/not) | `exact` | implemented | Predicate / Core where | |
| TA-PARAM | Named parameter binding `$` | `exact` | implemented | Core | Cursor param MAC residual separate |
| TA-PROJ-FLAT | Flat projection | `exact` | implemented | Core project | On wire op 118 |
| TA-PROJ-NEST | Nested / brace projection | `exact` | partial | Full RQL project | Local façade; wire refuse; **not lane-S pass** until Q2-BLOCK-FULL-WIRE |
| TA-PROJ-COMP | Computed projection | `blocker` | absent | RQL_SPEC amend + Q2 | SPEC v1 excludes arbitrary computed proj; programme requires practical shaping |
| TA-PROJ-COND | Conditional projection / shaping | `blocker` | absent | RQL_SPEC amend + Q2 | Same as computed for Gate-1 practical surface |
| TA-ORDER | Deterministic multi-field order + immutable key tie-break | `exact` | implemented | Core order | |
| TA-TOPK | Top-k / limit | `exact` | implemented | Core limit/page | |
| TA-CURSOR | Cursor continuation without offset-prefix discard | `exact` | partial | APP-6 cursor-v1 | Heap-confined secrets residual; offset deliberately refused |
| TA-IDX-EQ | Equality index eligibility | `exact` | partial | APB-7 index pushdown | Admitted paths only; scan fallback honest |
| TA-IDX-RANGE | Range index eligibility | `exact` | partial | Planner / index | Residual multipage |
| TA-IDX-COMPOUND | Compound index eligibility | `exact` | partial | Planner / index | |
| TA-ENRICH-1 | Enrich `exactly_one` | `exact` | partial | Full enrich; ENR | Local path; **op 118 refuse**; **lane S competitive pass forbidden** until Q2-BLOCK-FULL-WIRE (Q0.A4) |
| TA-ENRICH-OPT | Enrich `optional` | `exact` | partial | Full enrich | Same; **not lane-S pass** until Q2-BLOCK-FULL-WIRE |
| TA-ENRICH-MANY | Enrich `many` | `exact` | partial | Full enrich | Same; **not lane-S pass** until Q2-BLOCK-FULL-WIRE |
| TA-WITHIN | Nested `within` carrier | `exact` | partial | Full within | Depth bound; wire refuse; **not lane-S pass** until Q2-BLOCK-FULL-WIRE |
| TA-GROUP | Grouping | `blocker` | absent | RQL_SPEC amend; APB-8 lane | SPEC v1 excludes GROUP BY; programme Tier A requires it |
| TA-AGG-COUNT | Count accumulator | `blocker` | absent | RQL_SPEC amend | sql+ refuses aggregates today |
| TA-AGG-SUM | Sum accumulator | `blocker` | absent | RQL_SPEC amend | |
| TA-AGG-MIN | Min accumulator | `blocker` | absent | RQL_SPEC amend | |
| TA-AGG-MAX | Max accumulator | `blocker` | absent | RQL_SPEC amend | |
| TA-AGG-AVG | Average accumulator | `blocker` | absent | RQL_SPEC amend | |
| TA-COMPOSE | Reusable composition / subplans required by corpus | `blocker` | partial | RQL_SPEC; plan reuse | Named reusable components incomplete |
| TA-BUDGET | Query budgets (docs/bytes/result) | `exact` | implemented | Core budget | |
| TA-CANCEL | Cancellation / deadline | `exact` | partial | Resource / deadline codes | Cooperative cancel residual honesty |
| TA-CONSIST | Consistency modes | `exact` | implemented | Core consistency | |
| TA-COVER | Coverage policy + incomplete honesty | `exact` | implemented | Core coverage | Incomplete fail-closed |
| TA-EXPLAIN | Explain of programme actually executed | `exact` | partial | Core + full explain | Full explain not on op 118; must describe physical strategy honestly |
| TA-SQL-SUBSET | Deterministic SQL subset → RQL/QVM | `document-native-equivalent` | partial | SQL_TO_RQL_SPEC; sql+ scaffold | Emit or refuse; never guess; joins currently refuse |

**Tier A blocker summary (must close before Q2 exit):** TA-PROJ-COMP, TA-PROJ-COND,
TA-GROUP, TA-AGG-*, TA-AGG-COUNT-DISTINCT, TA-UNWIND, TA-STR-REGEX, TA-STR-EXPR,
TA-ARITH, TA-DATE, TA-PIPE-COMPOSE, TA-DISTINCT (impl), TA-COMPOSE (to corpus bar),
plus elevating all `partial` rows to expressible-without-app-scan for their Tier-A cases.
**Full-over-wire** for enrich/within/full project on lane S is a separate Q2 blocker (Q0.A4).

---

## 2. Tier B — important expansion (non-blocking unless promoted pre-Q1 freeze)

| id | Capability | Class | Impl | Owner | Notes |
|---|---|---|---|---|---|
| TB-AGG-RICH | Richer accumulators beyond count/sum/min/max/avg | `deliberate-exclusion` until promoted | absent | Future SPEC | Measured only if promoted |
| TB-ARRAY-XFORM | Array transformation pipelines | `deliberate-exclusion` until promoted | partial | ENR/SDA | |
| TB-ENRICH-FANOUT | Larger / multi-hop enrich fan-out | `deliberate-exclusion` until promoted | partial | Full attach | |
| TB-DISTINCT | *(promoted → TA-DISTINCT)* | — | — | — | See Tier A |
| TB-NAMED-COMP | Named reusable query components (library) | `deliberate-exclusion` until promoted | absent | DX / plan | |
| TB-COVERING-IDX | Partial/covering index improvements | `deliberate-exclusion` until promoted | partial | Index planner | |
| TB-SQL-AGG | SQL++/Mongo aggregation conveniences beyond subset | `deliberate-exclusion` until promoted | absent | SQL_TO_RQL | |

Promotion of any Tier B row into Tier A before Q1 corpus freeze requires principal
amendment of this matrix and the programme §2 tables.

---

## 3. Tier C — explicitly deferred

| id | Capability | Class | Owner |
|---|---|---|---|
| TC-FTS | Full-text search | `deliberate-exclusion` | FUTURE_ROADMAP |
| TC-VEC | Vector search | `deliberate-exclusion` | FUTURE_ROADMAP |
| TC-GEO | Geospatial search | `deliberate-exclusion` | FUTURE_ROADMAP |
| TC-GRAPH | Native graph pattern, traversal, path and analytics | `deliberate-exclusion` | [GRAPH_ENGINE_SPEC](../graph/GRAPH_ENGINE_SPEC.md) + staged plan |
| TC-CHANGE | Change streams / live queries | `deliberate-exclusion` | FUTURE_ROADMAP |
| TC-SPILL | Analytics-scale external-spill pipelines | `deliberate-exclusion` | FUTURE_ROADMAP |
| TC-WRITE-Q | Server-side write/update query pipelines | `deliberate-exclusion` | RQL read-only doctrine |
| TC-ML | Predictive / ML query operators | `deliberate-exclusion` | FUTURE_ROADMAP |
| TC-OFFSET | SQL OFFSET silent prefix discard | `deliberate-exclusion` | RQL_SPEC §3 deliberate |
| TC-DDA | Ranked `at rank` / direct access policies | `deliberate-exclusion` for Gate-1 unless promoted | DIRECT_ACCESS_SPEC |
| TC-ACCESS-POL | sequential/direct/build access policies | `deliberate-exclusion` | DDA-linked |

Tier C is named product backlog, not an unspoken deficiency.

---

## 4. Frontend and runtime surfaces (profile freeze)

| Surface | Gate-1 role | Notes |
|---|---|---|
| Application Core RQL | Primary product syntax | op 118 + embedded |
| Full RQL (enrich/within/brace project) | Tier A semantics required; wire may lag | Local path until wire parity |
| SQL-ish+ (`sql+`) | Declared subset only | refuse outside subset |
| JSON / Mongo dialect → QVM | Portable filter path | Not full Mongo aggregation |
| Rust builder | Equivalent frontend → same QVM | Q2 identity exit |
| Raw SDA / dialect `sda` | **Not** Gate-1 product query path | Explicit raw API only |
| Test-only semantic oracle | Q3 only | Never product path |

---

## 5. Exit

### Q0.2 (first freeze)

- [x] Every §2.1 programme surface has a row and class
- [x] Tier B and C named
- [x] No Tier A `TBD`
- [x] Blockers called out for Q2 ordering

### Q0.A3 (this amendment)

- [x] DISTINCT as Tier A (`TA-DISTINCT`), not Tier B bucket
- [x] IN / set membership (`TA-IN`)
- [x] Prefix, regex, string expressions (split rows)
- [x] Array element / nested / dupes / unwind rows (not only TA-ARRAY bucket)
- [x] Arithmetic and date expression rows
- [x] COUNT DISTINCT row
- [x] Explicit collation policy row
- [x] Pipeline composition row
- [ ] Principal accept of classifications (especially aggregate + DISTINCT + computed blockers vs SPEC v1)

**Principal decision needed:** confirm Tier A includes grouping/aggregates and
computed/conditional projection (programme text) despite RQL_SPEC v1 exclusion
— labor treats them as **blockers requiring SPEC amendment**, not silent demotion
to Tier C.
