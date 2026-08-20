# ATM-5D stable Atomic vocabulary baseline — 2026-08-20

Status: **public vocabulary complete; evidence-limited emission enforced**

Predecessor:
[ATM5C_EXTERNAL_SIGKILL_BASELINE_2026-08-20.md](./ATM5C_EXTERNAL_SIGKILL_BASELINE_2026-08-20.md).

This record closes the minimum stable Atomic error/outcome vocabulary item in
the ATM-5 release delta. It does not close the complete ATM-5 release gate.

## 1. Public contract

`residiuum_sdk::driver::atomics::AtomicCode` is a closed public vocabulary for
all twenty normative codes in `ATOMICS_SPEC` §22. `AtomicCode::as_str()` and
`Display` return the exact frozen snake-case spelling.

Driver failures retain their general `ErrorCode`, handling class and retry
disposition. When the failure has a proven Atomic meaning, public
`driver::Error::atomic_code` carries the additional `AtomicCode`. Applications
therefore never parse an operator message and do not lose generic scheduler or
transport handling.

Structural mappings currently proven include:

| Source | General driver code | Atomic code |
|---|---|---|
| illegal Atomic identity | `Validation` | `atomic_id_invalid` |
| same ID, different content root | `AtomicIdConflict` | `atomic_id_conflict` |
| cross-Heap plan/member | `PermissionDenied` | `atomic_scope_escape` |
| unavailable scope/profile | `Validation` | `atomic_scope_unavailable` |
| hard plan/admission byte limit | `ResourceLimit` | `atomic_limit_exceeded` |
| pre-accept/dequeue deadline | `AtomicDeadlineExceeded` | `atomic_deadline_exceeded` |
| missing/stale/foreign authority | `PermissionDenied` | `atomic_right_denied` |

Ordinary queue overload and cancellation before dispatch remain generic driver
conditions. They are not falsely relabelled as a normative Atomic decision.

## 2. Outcome truth remains singular

`AtomicOutcome::NotCommitted` and `AtomicOutcome::Unknown` remain successful
return values from `commit_atomic`; they are not duplicated as transport
errors.

`AtomicCode::from_outcome` provides the semantic classification:

- committed → no failure code;
- unknown observer knowledge → `atomic_outcome_unknown`;
- durable coverage abort → `atomic_coverage_incomplete`; and
- other durable v1 aborts → `atomic_not_committed`.

This preserves one authoritative truth for each submission.

## 3. Status classification

`AtomicCode::from_status` classifies the independent logical and material axes
with proof-safety precedence:

1. conflicting logical or material evidence → `atomic_evidence_conflicting`;
2. incomplete coverage → `atomic_coverage_incomplete`;
3. committed but incomplete named material → `atomic_material_partial`;
4. unknown logical outcome → `atomic_outcome_unknown`;
5. durable not-committed → `atomic_not_committed`; and
6. committed-complete or complete-coverage not-found → no failure code.

The logical and material status types are re-exported from the driver Atomics
module so applications need no direct kernel dependency to inspect them.

## 4. Deliberately reserved distinctions

The complete §22 vocabulary includes `atomic_read_conflict`,
`atomic_predicate_conflict`, `atomic_rule_changed`, `atomic_rule_violation`,
the three relationship codes and `unique_value_exists`.

The v1 durable abort record freezes only four coarse wire reasons:
precondition conflict, rule rejected, recovery abort and coverage incomplete.
It cannot retrospectively prove every finer distinction. This SDK therefore
reserves the exact public codes but does not manufacture them. Emitting the
read/predicate or changed/violation split requires a separately reviewed,
forward-compatible durable-detail amendment. Relationship and uniqueness
codes become emit-capable with the corresponding RRE delivery.

## 5. Evidence

The focused tests pin:

- every normative spelling;
- structural refusal and identity mappings;
- the absence of an invented Atomic code for generic malformed input;
- coarse versus coverage-specific not-committed classification;
- status conflict/coverage precedence;
- public deadline, identity-conflict and cross-Heap error attachment; and
- continued successful `AtomicOutcome` handling.

```text
Atomic vocabulary/classification unit tests       3/3 green
complete embedded driver integration             13/13 green
workspace all targets + all features              check green
```

The unrelated APP-5 RQL corpus mismatch remains outside Atomics.

## 6. Remaining ATM-5 release delta

1. Join store-derived recovery, physical sync/group-commit and bounded
   phase-latency telemetry into `Client::inspect().atomics`.
2. Execute the member/payload/collection/contention matrix, randomized/soak
   campaign and wider crash/damage corpus.
3. Prove all absolute performance rules and ordinary-write regression bounds.
4. Complete package/API compatibility, public documentation and the clean
   top-level evidence manifest.
5. Record architect acceptance before capability advertisement.

`Capabilities::atomics` remains `false`.
