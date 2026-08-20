# ATM-5E store-derived telemetry baseline — 2026-08-20

Status: **public embedded telemetry bridge complete; capability remains closed**

Predecessor:
[ATM5D_STABLE_VOCABULARY_BASELINE_2026-08-20.md](./ATM5D_STABLE_VOCABULARY_BASELINE_2026-08-20.md).

This record closes the ATM-5 requirement to expose bounded store-derived
Atomic recovery, physical durability and execution-phase telemetry through the
smart client. It is not a performance acceptance report.

## 1. Ownership and attribution

`AtomicStoreStats` is owned by `StoreHost`, shared by every capability-bound
Heap opened from one physical deployment and surfaced at:

```text
Client::inspect().atomics.store
```

The snapshot is constant-space and performs no media, key, Heap, collection or
record scan. It contains cumulative counters plus totals/maxima only; no
unbounded labels, samples or per-identity state are retained.

The public Heap Atomic path measures authoritative I/O before and after the
decision while it holds the physical writer mutex. Therefore write operations,
bytes, write time, sync operations and sync time are exact Atomic deltas;
unrelated ordinary writes cannot enter the interval.

## 2. Physical execution counters

The deployment snapshot reports:

- executions, committed, not-committed, replayed and failed;
- submitted member total;
- physical durability cohorts and maximum members in one cohort;
- authoritative write operations, bytes and nanoseconds; and
- authoritative durability barriers and barrier nanoseconds.

`durability_cohorts` advances only when the store observes at least one new
physical sync. A durable replay therefore advances `executions`, `committed`
and `replayed`, but not durability cohorts, writes or syncs.

The initial public proof establishes:

```text
three-member newly issued Atomic     1 durability cohort, exactly 2 syncs
same ID/root durable replay          0 additional cohorts, writes or syncs
```

This proves that sync count is independent of member count for the product
journey; it does not yet claim cross-Atomic public batching.

## 3. Bounded phase timing

The snapshot separates total and maximum nanoseconds for:

1. physical writer-lock wait;
2. Atomic catalogue open/reconstruction;
3. complete decision-plus-publication execution;
4. closed-plan and serialization-frontier validation;
5. member append and member stable boundary;
6. terminal-decision append and stable boundary; and
7. whole-delta visibility publication.

The complete execution timer intentionally envelopes the four internal phases;
their totals are diagnostic components, not assumed to be perfectly additive
under replay, refusal, failure or future overlap.

Separating catalogue-open time is deliberate. The current product path opens a
Heap-bound stage for each execution. The qualification matrix can now prove
whether this is negligible or a dominant optimization target instead of
hiding it inside SDK wall time.

## 4. Recovery telemetry

The same snapshot projects authoritative values captured during physical store
open:

- Atomics reconstructed;
- deterministic dirty-open recovery aborts;
- publications skipped due to degraded authority/material;
- bytes scanned and frames verified; and
- Atomic catalogue recovery plus committed-publication rebuild nanoseconds.

These are store-open facts, not SDK estimates. The external SIGKILL journey
asserts non-zero reconstructed Atomic, frame and recovery-time evidence after
the unclean reopen.

## 5. Failure and reset semantics

- Runtime counters begin at zero for each `StoreHost` open and are shared
  across its Heap bindings.
- Recovery fields describe that host's successful physical open.
- `atomics.store == None` only if inspection cannot acquire the store mutex
  because it is poisoned.
- Store errors are counted after the authority boundary and after a stage was
  successfully opened. Pre-authority driver refusals remain in the existing
  connection-level Atomic counters.
- `Unknown` remains an SDK observer outcome; the synchronous store never
  invents it as a physical decision.

## 6. Evidence

The public Gremlin journey pins the two-sync three-member boundary, replay with
zero additional sync, member/cohort counts and non-zero phase timings. The
external SIGKILL journey pins store-derived recovery evidence.

The checkpoint runs:

```text
focused Gremlin physical telemetry journey       green
external SIGKILL recovery telemetry journey      green
all-feature Atomic frontier decision suite       24/24 green
complete embedded driver integration             13/13 green
workspace all targets + all features             check green
```

## 7. Remaining ATM-5 release delta

1. Execute the declared member/payload/collection/contention matrix and turn
   these counters into controlled performance evidence.
2. Run randomized/soak and the wider crash/damage/authority corpus.
3. Prove no per-member fsync across every member band, maximum-plan memory,
   ordinary-write regression and absence of full-store ordinary commit/open
   scans.
4. Decide from measured catalogue-open and phase costs whether the product
   path needs a persistent in-memory stage catalogue and/or public cross-Atomic
   cohorting before acceptance.
5. Complete package/API compatibility, public documentation and the clean
   top-level evidence manifest.
6. Record architect acceptance before capability advertisement.

`Capabilities::atomics` remains `false`.
