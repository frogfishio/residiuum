# RQL / SDA ordinary Mongo capability delta

Status: **growth baseline** · 2026-08-10 · specialist search classes excluded

## 1. Objective

Grow Residiuum until an application can ask at least the ordinary raw-document
questions it can ask MongoDB. Syntax need not match MongoDB. The practical
answer, multiplicity and observable ordering must be expressible without an
application-side collection scan.

This is an algebra and execution target, not a request to reproduce MongoDB's
mistakes. In particular, Residiuum's explicit distinction between an absent
field and a stored `null` is retained. A Mongo equality shorthand that collapses
those states is translated into an explicit RQL disjunction when that is the
intended question.

Excluded from this first target: geospatial, full-text/search indexes, vector
search, graph traversal, change streams, write/update pipelines, server-side
JavaScript and cluster/sharding operators.

## 2. Ownership rule

| Layer | Owns |
|---|---|
| SDA | Total value algebra: typed scalar operations, array quantifiers, string/regex, arithmetic and date functions; explicit absent/null/error behavior. |
| RQL frontend | Readable syntax, names/scopes, static checks and lowering to canonical QVM. It must not invent a second evaluator. |
| Query plan / QVM | Row cardinality and pipeline operations: unwind, distinct, group, post-group filter, joins, facets/subpipelines, order and page. |
| Index/planner | Acceleration and honest explain. Missing acceleration may make a query slow; it must not change its answer. |

## 3. Delta ledger

### D0 — already adequate; cross-engine proof required

| Capability | Current state | Remaining work |
|---|---|---|
| Equality/range/compound and boolean predicates | Implemented | Execute real Mongo differential cases. |
| IN/set membership | Implemented | Differential empty, duplicate and mixed-type lists. |
| Absent/null/type distinction | Better, explicit semantics | Freeze Mongo translation cases; retain RQL algebra. |
| Nested paths | Implemented | Differential sparse/wrong-type paths. |
| Flat/nested projection | Implemented | Differential absent/null projection. |
| Deterministic order/top-k/cursor | Implemented; exact Mongo dipstick equality and admitted single-field ordered-index path | Extend differential matrix beyond the proven scalar order and multipage cases; replace the cold semantic projection bridge with a versioned order-preserving durable encoding. |
| Equality enrichment: exactly-one/optional/many | Implemented | Compare with `$lookup` pipelines and multiplicity. |
| Group + count/sum/min/max/avg | Implemented | Full mixed-type/empty/decimal differential matrix. |

### D1 — required ordinary-query growth

| ID | Capability | SDA delta | RQL/QVM delta | Exit example |
|---|---|---|---|---|
| D1-ARRAY-ALL | Array contains all requested values | Total `all`/membership primitive | Predicate syntax/lower | Tags contain both `red` and `sale`. |
| D1-ARRAY-ELEM | One array element satisfies a compound predicate | Scoped element quantifier | Element alias/scope lowering | One line item has `qty > 2` and `price < 10`. |
| D1-ARRAY-SIZE | Array cardinality predicate/expression | Total length over array-like values | Predicate/expression surface | Exactly three attachments. |
| D1-REGEX | Bounded regular-expression match | Frozen safe regex dialect and resource bound | Literal/options syntax | Codes matching `^ERR-[0-9]+$`. |
| D1-STRING | Lower/upper/length/concat/substr | Typed string primitives and Unicode policy | Expression grammar/lower | Case-normalised grouping or calculated label. |
| D1-ARITH | Add/subtract/multiply/divide/modulo | Numeric promotion, divide-by-zero and overflow laws | Expression grammar/lower | `price * quantity > 100`. |
| D1-DATE | Compare/extract/add/subtract dates | Date instant/duration/timezone algebra | Date expression surface | Events in hour bucket; overdue duration. |
| D1-UNWIND | Expand an array to one row per member | None beyond value iteration | Cardinality-changing QVM phase | One output row per order line. |
| D1-DISTINCT | Distinct values/rows | Canonical value equality/hash | QVM distinct phase and bounded memory | Unique customer regions. |
| D1-COUNT-DISTINCT | Count unique contributing values | Same canonical equality | Aggregate state | Unique users per conversation. |
| D1-HAVING | Filter aggregate output | Existing predicate algebra over group rows | Post-group filter phase | Regions with total sales over 1000. |
| D1-PIPELINE | Compose multiple filter/project/unwind/group stages | Existing expressions where sufficient | General ordered stage plan | Unwind lines, filter, group SKU, order top ten. |

### D2 — ordinary aggregate breadth after D1

| ID | Capability | Owner | Initial admitted subset |
|---|---|---|---|
| D2-SET | Collect unique values | Aggregate/QVM | `add_to_set(path)` with deterministic canonical output. |
| D2-PUSH | Collect values preserving admitted order | Aggregate/QVM | Bounded `push(path)`. |
| D2-FIRST-LAST | First/last under explicit order | Planner + aggregate | Require deterministic upstream order. |
| D2-TOPN | Top/bottom N per group | Planner + aggregate | Bounded N with stable tie-break. |
| D2-MERGE | Merge projected objects | SDA object algebra + aggregate | Explicit collision policy. |
| D2-FACET | Several subpipelines over one input | Plan/QVM | Bounded named branches; no invented shared-state semantics. |

Standard deviation, percentile/median and window functions are useful but do
not block the first ordinary application-query parity statement unless real
dogfood promotes them.

## 4. Implementation order

1. Build and run the real Mongo differential adapter for current D0 cases.
2. Implement D1 array predicates because they close common document selection
   without changing result cardinality.
3. Add the bounded SDA expression algebra: regex/string, arithmetic, then date.
4. Add QVM `unwind` and `distinct`.
5. Generalise group output into post-group filtering and ordered stage
   composition.
6. Add the D2 accumulators demanded by dogfood cases.
7. Add index eligibility only after each semantic family is oracle-green.

Every feature tranche must include source syntax, canonical plan/QVM encoding,
SDA or QVM execution authority, stable refusal/resource bounds, independent
oracle cases and scan/index equivalence where an index is admitted.

## 5. Dipstick boundary

The quick “are we in the game?” benchmark is deliberately narrower than this
ledger. It measures current D0 product work only:

- indexed equality;
- compound range;
- nested/array scan;
- deterministic top-k;
- grouped count; and
- count/sum/min/max/avg.

It uses identical generated documents and equivalent indexes, excludes load and
index-build time, warms both engines, records result counts/digests, and reports
Mongo end-to-end latency alongside a separately measured localhost command
floor. It is an order-of-magnitude dipstick, not a competitive qualification
claim.

## 6. Exit claim

“At least ordinary Mongo raw-query capability” becomes valid only when every D1
row is either product-green or explicitly removed from the target by written
decision, current D0+D1 cross-engine cases return equivalent answers, and no
answer is constructed by application-side scans.
