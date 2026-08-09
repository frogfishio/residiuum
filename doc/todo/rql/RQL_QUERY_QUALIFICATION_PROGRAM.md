# RQL Query Qualification Programme

Status: **principal-approved strategy; delivery plan accepted; Q0 freeze labor complete (principal package accept pending)**

Effective: 2026-08-05

Plan accept: **2026-08-07** — principal accepted Features + tasks materialisation for
RQL-Q0…Q7 and Decision-0 residual (see §11.1). This is **not** package exit for
Q0–Q7 and **not** Gate-1 pass.

Authority: [CRITICAL_PATH.md](../../../CRITICAL_PATH.md) §4

Q0 freeze artefacts (labor):

- [RQL_Q0_ENV_MANIFEST.md](./RQL_Q0_ENV_MANIFEST.md)
- [RQL_Q0_CAPABILITY_MATRIX.md](./RQL_Q0_CAPABILITY_MATRIX.md)
- [RQL_Q0_RESULT_EQUIVALENCE.md](./RQL_Q0_RESULT_EQUIVALENCE.md)
- [RQL_Q0_LANES_EXCLUSIONS.md](./RQL_Q0_LANES_EXCLUSIONS.md)
- [RQL_Q0_PRINCIPAL_ACCEPT.md](./RQL_Q0_PRINCIPAL_ACCEPT.md) — **principal accept pack**

Semantic specification: [RQL_SPEC.md](../../wip/query/RQL_SPEC.md)

Implementation inventory: [RQL0_GAP_LEDGER.md](./RQL0_GAP_LEDGER.md)

Runtime convergence status: [RQL_WHAT_IS_LEFT.md](./RQL_WHAT_IS_LEFT.md)

## 0. Decision

This document is the single implementation and qualification programme for
Residiuum Critical Path Gate 1: **RQL**.

It must answer two independent questions:

1. **Capability:** can RQL express the practical document retrieval and result
   shaping for which developers choose MongoDB or Couchbase Lite?
2. **Execution:** can Residiuum return those answers correctly and at
   competitive throughput, latency and resource cost under realistic strain?

Passing only one question is failure. A fast incomplete language is not a
database query system. A complete language whose implementation collapses
under load is not a viable product.

```text
RQL-Q0 target freeze
  -> RQL-Q1 practical query corpus
  -> RQL-Q2 capability closure
  -> RQL-Q3 semantic qualification
  -> RQL-Q4 cross-engine harness
  -> RQL-Q5 controlled baseline
  -> RQL-Q6 evidence-led optimisation
  -> RQL-Q7 final qualification and Gate-1 decision
```

There is one controlling document (this file), one scoreboard and one harness.
Run evidence belongs under an archive/evidence directory. It must not create
another roadmap family.

## 1. Hard laws

1. **One product executor.** Every supported frontend compiles to one canonical
   QVM programme and executes through one runtime. A test oracle is permitted;
   another product semantic executor is not.
2. **Correctness precedes speed.** A query family cannot enter comparative
   performance qualification until its semantics and scan/index equivalence
   are green.
3. **Equivalent work only.** Engines receive the same logical documents,
   result requirements, index entitlement, durability posture, cache state and
   concurrency.
4. **No application escape hatch.** A mandatory query is not supported if the
   caller must scan the collection or reconstruct the answer in application
   code.
5. **No silent weakening.** Unsupported semantics return a stable refusal.
6. **Coverage is part of the answer.** A hole must never become an empty
   result, false absence proof or complete page.
7. **Tails count.** Median throughput cannot conceal p99 latency, memory
   growth, read amplification, deferred work or incompleteness.
8. **Baseline before optimisation.** Q5 records the unoptimised truth. Q6 may
   change code only against a reproduced, measured bottleneck.
9. **No moving gates.** Numeric gates freeze after Q5 and before Q6.
10. **Board state is not acceptance.** Labor stops at `in_review`; principal
    acceptance advances packages and closes Gate 1.

## 2. Competitive target

The target is practical document-query equivalence, not syntax parity with
every historical MongoDB or Couchbase operator. Every comparator construct is
classified as exactly one of:

| Class | Meaning |
|---|---|
| `exact` | RQL exposes the same result semantics directly |
| `document-native-equivalent` | Different expression, same practical result |
| `deliberate-exclusion` | Outside the frozen profile with stable refusal |
| `blocker` | Required by the frozen profile but not implemented |

### 2.1 Tier A — mandatory database surface

Every Tier-A case must be expressible and qualified:

- key lookup and selection;
- total absent/null/value and type semantics;
- scalar, object, nested-field and array predicates;
- boolean composition and parameter binding;
- flat, nested, computed and conditional projection;
- deterministic multi-field ordering with immutable tie-breaking;
- top-k and cursor continuation without offset-prefix discard;
- equality, range and compound index eligibility;
- relationship enrichment with explicit `exactly_one`, `optional` and `many`
  cardinality;
- grouping and admitted count/sum/min/max/average accumulators;
- reusable composition/subplans required by the corpus;
- budgets, cancellation, consistency and coverage;
- explain output describing the programme actually executed; and
- deterministic translation for the declared SQL subset.

### 2.2 Tier B — important expansion

Tier B is measured and classified, but does not block the first Gate-1 pass
unless promoted before Q1 freezes:

- richer accumulators and array transformation;
- larger/fan-out enrichment pipelines;
- distinct and richer computed objects;
- reusable named query components;
- partial/covering index improvements; and
- additional SQL++/Mongo aggregation conveniences.

### 2.3 Tier C — explicitly deferred

Unless principal-amended before corpus freeze:

- full-text, vector and geospatial search;
- recursive graph traversal;
- change streams/live queries;
- analytics-scale external-spill pipelines;
- server-side write/update pipelines; and
- predictive/ML query operators.

Tier C remains named product backlog, not an unspoken deficiency.

## 3. RQL-Q0 — target and profile freeze

### Deliverables

1. Version-pinned manifest for Residiuum, MongoDB Community, Couchbase Lite,
   operating system, filesystem and hardware.
2. Tier-A/B/C capability matrix.
3. Stable definition of equivalent result for every query family.
4. Explicit embedded and local client/server comparison lanes.
5. Declared exclusions and stable refusal codes.

### Exit

- Every capability has one classification and owner.
- No mandatory semantic is `TBD`.
- Principal accepts the target before corpus implementation begins.

## 4. RQL-Q1 — practical query corpus

Build one immutable, versioned corpus of approximately **100–150 query
intentions**. The intention and expected result are authority; each language is
an implementation of that intention.

### 4.1 Required domains

| Domain | Representative work |
|---|---|
| Commerce | products, inventory, customers, orders, line items |
| Messaging | conversations, participants, unread/recent messages |
| Directory | scoped listings, discovery, category/location filters |
| Telemetry | devices, ranges, recent events, aggregates |
| Project management | ownership, status, revisions, cross-owner reporting |

At least two domains must come from real dogfood use rather than invented
microbenchmarks.

### 4.2 Corpus record contract

Every case contains:

```text
case_id, tier, domain
plain_english_intent
fixture_generator + seed
expected_result_or_oracle_rule
ordering_and_multiplicity
RQL source + canonical QVM hash/vector
Mongo query/aggregation pipeline
Couchbase Lite SQL++/QueryBuilder form
required and optional indexes
selectivity and cardinality class
missing/null/type variants
cursor/page variants
declared exclusion or stable refusal
```

### 4.3 Distribution floor

| Family | Minimum cases |
|---|---:|
| Key/equality/range/compound selection | 20 |
| Missing/null/type/nested/array predicates | 20 |
| Projection/computed/conditional shaping | 15 |
| Ordering/top-k/cursor pagination | 15 |
| Enrichment/cardinality | 15 |
| Grouping/aggregation | 15 |
| Budgets/coverage/damage/refusal | 10 |

Cases may overlap families, but the corpus must not become a hundred variants
of indexed equality.

### Exit

- Tier A covers every §2.1 requirement.
- Expected results are independent of the Residiuum optimiser.
- Comparator forms have been reviewed for semantic equivalence.
- Corpus changes require a versioned, principal-reviewed amendment.

## 5. RQL-Q2 — capability closure

Run the corpus as a compile/execution audit before benchmarking. For every
failing Tier-A case:

1. classify the missing semantic, syntax, compiler, QVM, host or index feature;
2. freeze its meaning in `RQL_SPEC.md`;
3. implement it through canonical-plan -> canonical-QVM -> one runtime;
4. add stable refusals for unsupported variants; and
5. add oracle and mutation tests.

Likely packages include grouping/aggregation, computed conditional projection,
complete array semantics, composition, compound/range planning, executable
explain and direct Full-RQL-to-QVM compilation. The corpus decides their order.

### Exit

- **100% of Tier A** is expressible without application collection scans.
- Tier B/C cases are deliberately classified.
- Equivalent SQL/Rust-builder/RQL inputs produce the same canonical QVM.
- Canonical QVM is the only production execution authority.

## 6. RQL-Q3 — semantic qualification

### 6.1 Independent oracle

Provide one deliberately unoptimised, test-only semantic oracle. It may read a
complete logical fixture, but it must not be callable as a product query path
or share optimiser/index-selection code with the implementation under test.

### 6.2 Differential matrix

```text
reference_oracle(Q)
  == forced_scan_QVM(Q)
  == every_admitted_index_plan(Q)
  == reopened_store(Q)
  == comparator_result(Q) where semantics overlap
```

Compare values, keys, multiplicity, order, continuation and coverage—not only
row count.

### 6.3 Metamorphic laws

```text
indexed(Q) = forced_scan(Q)
page_1 ++ page_2 ++ ... = unpaged(Q)
filter(A and B) = filter(B and A)
project(identity, Q) = Q
reopen(Q) = pre_close(Q)
equivalent_frontends(Q) produce identical canonical QVM
complete_coverage(Q) implies zero hidden holes
```

Add laws for every aggregate and enrichment cardinality.

### 6.4 Adversarial dimensions

- heterogeneous, sparse and generated documents;
- absent/null/wrong-type/empty values;
- nested, empty and duplicate arrays;
- duplicate ordering values and immutable-key ties;
- zero/one/many and violated enrichment cardinalities;
- index create/rebuild/partial coverage/stale candidates;
- mutated QVM structure, operands and identity;
- mutated/replayed continuation tokens;
- reopen, rotation and compaction;
- writes between pages under declared consistency;
- missing/corrupt authoritative or derived media; and
- budget, cancellation and timeout boundaries.

### Exit

- No unresolved result divergence.
- No false absence or false completeness.
- Forced scan equals every admitted index path.
- Corpus, property, fuzz and damage suites run from one command.

## 7. RQL-Q4 — cross-engine qualification harness

The harness owns fixtures, loading, indexes, execution, telemetry, result
canonicalisation and evidence publication.

| Lane | Engines |
|---|---|
| Embedded | Residiuum embedded vs Couchbase Lite embedded |
| Local client/server | Residiuum server protocol vs local MongoDB |

Do not conceal the transport difference by presenting embedded Residiuum
against MongoDB TCP latency as one undifferentiated contest.

### 7.1 Dataset axes

- flat, deeply nested, sparse heterogeneous and array-heavy shapes;
- approximately 1 KiB, 8 KiB and 64 KiB payloads plus a seeded heavy tail;
- working sets near 25%, 100% and 400% of the controlled host's fixed memory
  capacity (physical RAM on bare metal, or an evidenced container/VM limit),
  never a transient free/available-memory reading;
- uniform, Zipf/hot-key and time-ordered distributions;
- low, medium and high cardinality; and
- point, 0.01%, 1%, 10% and broad selectivity.

Dataset size may scale to the host, but memory ratios and logical generators
remain identical across engines.

### 7.2 Mandatory measured cells

1. Key get.
2. Indexed equality at multiple selectivities.
3. Range and compound equality/range.
4. Nested-field and array predicates.
5. Covered and non-covered projection.
6. Deterministic top-k.
7. First-page and deep cursor continuation.
8. One-to-one, optional and one-to-many enrichment.
9. Low- and high-cardinality grouping.
10. Count/sum/min/max/average.
11. Conditional/computed shaping.
12. Mixed 90/10 and 70/30 read/write workloads.

Run at concurrency 1, 2, 4, 8 and one declared oversubscribed level to expose
single-core latency, four-core scaling and contention separately.

### 7.3 Cache and lifecycle classes

- warmed steady state;
- fresh process/reopen;
- dataset larger than memory;
- read-only and concurrent writes;
- segment rotation and compaction/rebuild; and
- declared damage with surviving readable data.

“Cold” must state how it was obtained. Reopen is not automatically a true
device-cache cold start.

### 7.4 Required metrics

```text
result digest + coverage + validity
queries/s and p50/p95/p99/max latency
CPU time/utilisation and RSS/peak memory
physical bytes read/written and read amplification
documents/index entries examined
index size/build time and indexed-write penalty
actual execution/explain plan
cache/lifecycle state
deferred work and final drain
```

Bundles include raw repetitions, environment fingerprint, configurations,
versions, seed, query/QVM hashes and content hashes.

## 8. RQL-Q5 — controlled baseline

Run the complete qualified Tier-A corpus without product optimisation:

- same host/filesystem;
- release builds;
- alternating/randomised engine order;
- at least seven valid repetitions per primary cell;
- separate warm-up and measurement;
- duration and operation floors;
- no benchmark-only product path;
- correctness digest every repetition; and
- medians plus dispersion/confidence evidence, never best-run selection.

The baseline produces per-query comparisons, measured bottlenecks, correctness
and resource failures, the Q6 queue and proposed numeric gates. Smoke or
failed/inconclusive cells cannot support performance claims.

## 9. RQL-Q6 — evidence-led optimisation

Only reproduced Q5 bottlenecks enter Q6. Every change records:

```text
failing cells + run ids
measured causal hypothesis
implementation change
semantic equivalence result
controlled before/after repetitions
resource/write-cost regression check
accept/reject/revert decision
```

Possible work includes index selection, range traversal, covered projection,
top-k, cursor resume, join strategy, aggregation layout, parallel readers,
caching and read-ahead. None is commissioned by fashion or guesswork.

## 10. RQL-Q7 — final qualification

Run a fresh controlled campaign from the frozen corpus and gates.

### 10.1 Capability gate

- 100% of Tier A compiles and executes.
- No mandatory case requires an application scan.
- Unsupported constructs refuse stably.
- Equivalent frontends produce identical canonical QVM.
- Explain reports the programme and physical strategy actually executed.

### 10.2 Correctness gate

- 100% agreement with the independent oracle.
- Forced scan equals every admitted index plan.
- Pagination equivalence holds under ties and declared updates.
- Zero false absence or false completeness under damage.
- Fuzz/property/mutation suites have no unresolved defect.

### 10.3 Competitive performance gate

Portfolio gates freeze after Q5 and before Q6. Initial hard floor:

- no critical indexed Tier-A cell is more than **2x slower at p95** than the
  faster relevant comparator without principal-accepted explanation;
- no win depends on weaker durability, omitted index maintenance, hidden debt
  or warmer cache;
- aggregate/geometric-mean throughput and p95 are competitive on the named
  primary profile;
- p99, memory, amplification, index build and write costs pass separately
  frozen budgets; and
- mixed read/write execution remains bounded, correct and drainable through
  rotation and compaction.

Portfolio reporting must expose per-cell failures. A geometric mean cannot
erase a catastrophic practical query.

### 10.4 Gate-1 decision

Gate 1 passes only when capability, correctness and performance all pass.
Principal acceptance records the exact proven profile, exclusions, controlled
performance envelope, known weaknesses and evidence-bundle digest. Only then
does Atomics become the active critical-path programme.

## 11. Package scoreboard

Recovery note (2026-08-09): the lost Kanban state is reconstructed in
[RQL_RECOVERY_BASELINE_2026_08_09.md](./RQL_RECOVERY_BASELINE_2026_08_09.md).
Where the older delivery rows below conflict with that dated baseline, the
baseline records current delivery truth; this programme remains target authority.

| Package | State | Principal exit decision |
|---|---|---|
| RQL-Q0 Target freeze | **`accept`** (2026-08-07; SHA e1f5c670…) | Tier and comparator profile accepted |
| RQL-Q1 Practical corpus | `active`; Q1.1–Q1.4 labor landed; not frozen | Immutable corpus accepted |
| RQL-Q2 Capability closure | `in_review`; 147/147 explicit closed outcomes (145 execute + 2 stable refusals) | principal accept; Decision 0 remains separate |
| RQL-Q3 Semantic qualification | Q3.1–Q3.4 labor exit ready; 147/147 green denominator | principal accept |
| RQL-Q4 Harness | Q4.1–Q4.3 `in_review`; product scaling/repetition/maintenance/damage rehearsals verifier-green | R400 memory + resource probes + real comparator/server campaign + principal accept |
| RQL-Q5 Baseline | `backlog` | Baseline accepted; gates frozen |
| RQL-Q6 Optimisation | `backlog` | Bottlenecks closed or accepted |
| RQL-Q7 Final qualification | `backlog` | Gate 1 pass/fail |

Only the next dependency-satisfied package may move to `todo`. Current QVM
convergence blockers must close before Q2 can claim its one-runtime exit.

### 11.1 Kanban delivery plan (accepted)

Product project: `019fda36-f8f4-7f40-9a9b-a86cfae1466e`

| Package | Feature id | Task stage policy |
|---|---|---|
| Decision-0 residual (pre-Q2 honesty) | `019fda4c-a6f2-7932-a9d7-6e04400fd3df` | D0.1–D0.2 labor cards board `done`; **Decision 0 still OPEN** (principal disposition required); blocks Q2 one-runtime exit claim |
| RQL-Q0 Target freeze (first freeze) | `019fda4b-d981-7980-a283-549a7312f2a9` | Q0.1–Q0.8 labor complete/`in_review`; **not package accept** |
| RQL-Q0 Amendment package | `019fdac4-1408-7321-8edc-a09851c9e656` | A1–A15 complete; package ACCEPT recorded |
| RQL-Q1 Practical corpus | `019fda4c-11fd-7102-bd55-10a347802144` | Q1.2 Commerce+Messaging `in_review`; claim Q1.3 next |
| RQL-Q2 Capability closure | `019fda4c-1227-7c93-b7e6-292141ec7a78` | backlog; spawn gap packages after audit |
| RQL-Q3 Semantic qualification | `019fda4c-5994-77e2-a2c9-aaa0c3097b29` | active (Q3.1–Q3.3 in_review; one-command `verify-rql-q3.sh`) |
| RQL-Q4 Harness | `019fda4c-59bf-7320-a0cb-35f92c50fc45` | active (Q4.1–Q4.3 scaffold `in_review`; no competitive claims) |
| RQL-Q5 Baseline | `019fda4c-59e4-76c3-9f24-ce13fbdbbd4e` | backlog |
| RQL-Q6 Optimisation | `019fda4c-a695-7ff1-8fbf-f4d407b0ba87` | backlog; concrete opts only from Q5 queue |
| RQL-Q7 Final / Gate-1 | `019fda4c-a6c4-7b40-b15a-f8190ca62d03` | backlog |

**Q0 labor (2026-08-07):** Q0.1–Q0.4 freeze docs + Q0.5 principal accept pack
([RQL_Q0_PRINCIPAL_ACCEPT.md](./RQL_Q0_PRINCIPAL_ACCEPT.md)) + Q0.6 hold + Q0.7
scoreboard honesty landed. Principal advanced those **labor cards** to board
`done`. **That is not package accept.** Package exit still requires principal
to fill accept pack **§5**. Do not implement Q1 until §5 records ACCEPT.

**Principal ACCEPT (2026-08-07):** Q0 freeze package **accepted** after A11–A14
closeout. Clean tip `e1f5c670a99dc54da477c531c83bca4985199a42`. Q1 corpus **admitted**.
RQB1 remains **deleted**; do not restore. Not Gate-1 pass; Decision 0 still OPEN.

**Labor hold (updated):** [RQL_LABOR_HOLD.md](./RQL_LABOR_HOLD.md) — Q0 hold
**lifted** for Q1; claim Q1.1 next.

**D0 residual labor (2026-08-07):** [RQL_D0_RESIDUAL_INVENTORY.md](./RQL_D0_RESIDUAL_INVENTORY.md)
(D0.1) + [RQL_D0_CLOSE_READINESS.md](./RQL_D0_CLOSE_READINESS.md) (D0.2) labor
cards board `done`. Decision 0 remains OPEN; RQL-C1 forbidden; Q2 must not
claim one-runtime exit until principal D0 disposition (see D0.2 §D).

**Honesty law:** Kanban `done` on implementer cards ≠ package `accept` on the
scoreboard. Only principal §5 (Q0) or explicit Decision 0 disposition (D0)
moves those package states.

Implementers claim from `todo` only → `doing` → `in_review`. Principal accepts
package exits.

Labor SoT is the Kanban board, not this table alone. Amend this section when
features are archived or package stages change after principal accept.

## 12. Immediate marching order

1. **Done:** Q0 ACCEPT (2026-08-07); Q1.1 corpus schema scaffold landed
   (`spec/rql/qualification/corpus-v1/` + [RQL_Q1_CORPUS.md](./RQL_Q1_CORPUS.md)).
2. **Labor next:** Q1.3 Directory + Telemetry + Project management fixtures (then Q1.4 floors).
3. After Q1 package accept: capability audit → Q2 from actual failures (include
   Full-over-wire blocker from Q0.A4).
4. Decision 0 residual: one product QVM path is the close test; still
   principal-only for C1; blocks Q2 one-runtime *exit claim*.
5. Do not benchmark a semantic family until it passes Q3.

## 13. Non-claims

- Existing examples are not the Tier-A corpus.
- A green compiler does not prove query correctness.
- A scan answer does not prove index competitiveness.
- Comparator agreement does not replace the independent oracle.
- A microbenchmark does not qualify the product query path.
- Storage write qualification does not imply query qualification.
- RQL-Q7 does not qualify Atomics or Cluster.
