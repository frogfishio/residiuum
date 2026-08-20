# ATM-4D serial histories and predicates — 2026-08-20

Status: `implementation_in_progress`

ATM-4D proves that every accepted LocalHeap history is equivalent to one
serial execution at the Heap serialization frontier. It also closes the
predicate delta needed to make dependencies wider than a single key exact.

## 1. Acceptance invariant

For every recorded history there must be an independently checked total order
such that:

1. committed plans validate every declared read, predicate and mutation
   precondition against the state immediately before that plan;
2. a not-committed precondition conflict is invalid at its position and
   publishes no member;
3. committed receipts form one strictly increasing Heap commit order;
4. applying receipt versions produces the exact visible final state;
5. no undeclared dependency is claimed by the checker or engine; and
6. histories from another Heap cannot satisfy or invalidate this Heap's
   witnesses.

The checker must consume plans, initial observations and returned outcomes. It
must not call the production validation kernel or assume submission order.

## 2. Frozen point semantics

The current production kernel already implements these serialization-frontier
conditions:

- `ReadWitness(Some(version))`: the exact version must still be current;
- `ReadWitness(None)`: the key must still be absent;
- `assert_absent`, `assert_present`, `assert_version`;
- create-if-absent and version-bound replace/delete;
- read-your-earlier-plan validation inside a physical cohort;
- one monotonically increasing commit position per committed plan;
- authority revision equality when the stage is capability-bound.

Versions are immutable event identities, so delete/recreate cannot reproduce a
prior version. Projection hashes remain content bindings in the closed plan;
version equality is the concurrency witness because an event identity cannot
legally acquire different content.

## 3. Predicate delta

The following closed `PredicateKind`s exist in the canonical vocabulary but
are not executable yet and currently produce `RuleRejected` or structural
refusal:

- exact scalar equality;
- bounded key-range absence;
- bounded key-range presence;
- active rule revision equality;
- collection lifecycle state.

ATM-4D will define a versioned canonical payload inside `PlanPredicate.encoded`
without changing the frozen outer plan/checkpoint shapes. Range payloads must
bind:

```text
collection_id
canonical key-order profile
lower bound + inclusivity
upper bound + inclusivity
exact coverage generation/frontier
expected absence or authenticated result-set commitment
```

A partial, stale, damaged or non-exact index can never prove range absence.
Index and forced-scan execution must agree on the same canonical predicate.

## 4. Delivery slices

### ATM-4D.1 — point serial foundation

- independent bounded serial-order checker;
- lost update, write skew, create/create uniqueness, delete/recreate ABA,
  point absence phantom, disjoint and overlapping cohorts;
- ordinary-write-versus-Atomic stale witness;
- deterministic randomized point histories;
- restart/replay preserves the same terminal outcomes.

### ATM-4D.2 — construction overlay

- **Delivered core:** `AtomicBuilder::read_your_plan` resolves earlier planned
  create/put/replace values and planned deletes by canonical object identity.
  It returns `External` only when a version-bearing host read is required.
  Planned reads emit no external witness and payload bytes are not duplicated
  in the overlay; Heap, rights, encoding and authority checks still apply.
- **Open host bridge:** a store/SDK plan-construction session must consume
  `External`, perform a version-bearing read, record the exact version/absence
  witness, decode planned bytes under the collection codec, and enforce bounded
  memory/cancellation. Capability-to-`TrustedAuthorityView` minting remains an
  ATM-5 composition boundary and must not be bypassed here.

### ATM-4D.3 — exact scalar predicate

- **Delivered execution core:** `predicates-v1.json` freezes a deterministic,
  typed exact-scalar payload. Canonical byte, UTF-8, signed-integer and decimal
  encodings are validated before prepare; absence is false. The store evaluates
  at the serialization frontier and the cohort overlay carries both version and
  value, so an earlier plan's mutation participates exactly.
- **Delivered proof:** match, mismatch, absence, ordinary-write invalidation,
  same-cohort planned-value visibility, canonical hostile payloads, rights and
  encoding checks, plus an independent serial-checker negative control.
- **Open compiler bridge:** canonical RQL/RRE lowering must call the typed
  `compiled_exact_scalar_equality` hook. There is no host closure or arbitrary
  executor bytecode path.
- **Open query differential:** heterogeneous SDA/JSON query equality remains a
  query-engine differential task; this scalar predicate deliberately covers
  collections whose frozen value contract is scalar.

### ATM-4D.4 — bounded exact ranges

- **Delivered canonical contract:** `predicates-v1.json` freezes collection,
  key kind, mathematical order profile, inclusive/exclusive bounds, exact
  coverage domain, examination ceiling, cardinality, ordered `(key, version)`
  commitment and semantic range identity. Empty or backwards geometry,
  duplicate observations, mixed key kinds and unbounded work are refused.
- **Delivered mathematical order:** UTF-8 strings, opaque bytes, arbitrary-width
  signed integers and exact decimals (`coefficient × 10^-scale`, then declared
  scale as the distinct-representation tie-break) compare by a total semantic
  order rather than SubjectV2 bytes. Exhaustive bounded decimal
  cross-multiplication checks protect the comparator.
- **Delivered authoritative execution:** the LocalHeap executor streams the
  complete primary key/version domain under the writer frontier, decodes every
  SubjectV2 key under the collection profile, applies prior-cohort overlay
  versions/creates/deletes, sorts only bounded matching results, and compares
  the exact commitment. Writes outside the range do not conflict.
- **Delivered fail-closed coverage:** offline key authority, malformed stored
  identities and an exceeded examination ceiling produce
  `coverage_incomplete`; they never prove absence. Result cardinality is capped
  at 4,096 and collection examination at 1,000,000 identities per predicate.
- **Delivered proof:** in-cohort phantom, exact version drift, outside-range
  non-conflict, integer physical-order negative, work/tier coverage refusal,
  predicate-only commit/retry mechanics, and an independent serial-history
  mutation-sensitive negative control.
- **Delivered acceleration differential:** a bounded 64 MiB process-local
  projection carries the exact heap, collection, key kind, mathematical order
  profile, coverage-domain identity, complete examined count and ordered
  `(key, version)` rows. Cache hits use binary semantic bounds and then apply
  the same cohort overlay. Every primary publication invalidates the cache;
  restart begins empty; offline authority refuses before lookup; oversized
  projections transparently remain on the forced-scan path. Observable
  hit/miss/build/invalidation/bypass counters prove which path qualification
  exercised. Forced and cached execution agree for string, opaque, integer and
  decimal commits and stale conflicts, including cohort create/delete and
  unavailable-tier negative controls. Candidate, stale, partial, damaged or
  identity-mismatched indexes remain ineligible.

### ATM-4D.5 — remaining shared-frontier predicates

- active rule revision;
- collection/object lifecycle state;
- authority/lifecycle/ordinary/Atomic races in the independent checker.

## 5. Required anomaly corpus

The accepted corpus contains:

- lost update;
- write skew;
- create/create on one key;
- replace/delete on one version;
- delete/recreate ABA;
- point and bounded-range phantoms;
- unique scalar contention;
- ordinary write versus Atomic;
- overlapping and disjoint LocalHeap Atomics;
- active-rule, collection-lifecycle and Heap-authority revision changes;
- identical user keys and Atomic IDs in two Heaps;
- crash/restart and exact retry for every terminal result.

Every all-committed result needs a checker-produced serial order. Each known
anomaly needs a mutation-sensitive negative control which fails if validation
or overlay participation is removed.

## 6. Current delta at start

Delivered before ATM-4D:

- production point validation and cohort overlay;
- exact terminal decisions, receipts, retry and recovery;
- one Heap commit position for each committed Atomic;
- authority revision participation for capability-bound stages.

Open:

- independent serial checker and complete point anomaly corpus;
- deterministic randomized histories;
- public construction overlay;
- scalar/range/lifecycle/rule predicate payloads and execution;
- proof that every ordinary predicate-affecting mutation participates in the
  same Heap order through the final public API.
