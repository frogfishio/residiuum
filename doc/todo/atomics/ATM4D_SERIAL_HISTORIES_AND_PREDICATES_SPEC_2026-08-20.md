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

Every closed LocalHeap v1 `PredicateKind` is now executable. Exact scalar,
bounded key-range, active-rule revision and collection-lifecycle predicates use
the frozen contracts below; no compiled predicate is a host closure.

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
- **Delivered in ATM-5A:** the SDK plan-construction session consumes
  `External`, performs a bounded version-bearing read, records the exact
  version/absence witness, binds the store frontier and decodes planned bytes
  under the collection codec. Capability authority is sampled from the exact
  live Heap binding under its authority guard; application fields cannot mint
  it. Submission deadline/cancellation qualification remains an ATM-5 gate.

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

- **Delivered active-rule contract:** kind 7 carries one canonical nonzero
  32-byte invariant revision, while `active_rule_revisions` binds the complete
  strictly sorted active set. The executor requires both membership of every
  declared revision and exact equality with the authoritative Heap set;
  membership alone cannot let a plan compiled before a new invariant was
  activated commit afterward.
- **Delivered active-rule authority:** the exact set is deterministic Heap
  metadata, missing means empty, and explicit empty sets are not deletion
  aliases. Activation/deactivation and Atomic validation serialize through the
  physical writer order. Unreadable authority produces `coverage_incomplete`;
  a stale set produces `precondition_conflict`.
- **Delivered active-rule proof:** canonical hostile cases, multiple distinct
  revisions in one plan, stale membership-only and activation-before-decision
  conflicts, exact current-set commit, restart/terminal retry, and cross-Heap
  isolation.
- **Open public bridge:** capability-bound Heap/SDK rule administration must
  expose activation/deactivation without leaking raw Store access, and must use
  the same physical writer mutex. The raw Store mechanism is qualification
  infrastructure, not the final client authority ceremony.
- **Delivered lifecycle contract:** kind 8 binds one immutable collection ID to
  exactly `absent`, `active` or `retired`. Authority comes from the verified
  descriptor chain, never its rebuildable catalogue. IDs are never reused and
  retirement is monotonic, so state equality has no ABA alias. Rename preserves
  state and deliberately does not cause a false conflict.
- **Delivered lifecycle order:** ordered create, rename and retire primitives
  execute while the physical Store order is owned. Embedded SDK collection
  creation now enters through `HeapStore` and the same physical mutex as Atomic
  decisions instead of writing descriptor files beside that order. Qualified
  remote op 106 now uses the same `HeapStore` entry rather than its former raw
  layout call.
- **Delivered lifecycle proof:** create-versus-absent and
  retire-versus-active races conflict; rename-versus-active commits; descriptor
  state and terminal conflict survive restart; the same collection ID remains
  isolated across Heaps. A damaged descriptor suffix is
  `coverage_incomplete`, never a valid active prefix. An independent
  state-machine checker plus mutation controls proves the recorded
  create/retire aborts become unjustifiable when the winning lifecycle
  transition is removed. Concurrent embedded same-name creation has exactly one
  winner, demonstrating the administration path actually enters the shared
  writer order.
- **Delivered construction binding:** every typed collection admission now adds
  exactly one canonical `active` lifecycle predicate automatically. Repeated
  operations deduplicate it, and an active handle cannot claim `absent` or
  `retired`. Because this is an observation, typed admission requires `READ`
  alongside the requested mutation right and refuses early if it is absent.
  The pure serial oracle models lifecycle state and proves a retired collection
  rejects without publishing. Public capability-gated rename/retire APIs do not
  exist yet; when introduced they must call the ordered Store primitives rather
  than the raw descriptor functions.
- **Delivered combined corpus:** all 32 subsets of authority revision, lifecycle
  rename/retire, ordinary data mutation, rule-set mutation and Atomic decision
  run against the real capability-bound store. The independent classifier
  distinguishes committed, durably not-committed and capability refusal before
  issuance. Every campaign reopens the store, checks exact terminal status and
  visible value, and includes mutation-sensitive controls for every semantic
  transition; rename remains the deliberate non-conflicting control.

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
