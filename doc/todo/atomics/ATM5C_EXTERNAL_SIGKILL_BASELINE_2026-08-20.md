# ATM-5C external SIGKILL baseline — 2026-08-20

Status: **external decision/before-ack process-death gate passed**

Predecessor:
[ATM5B_OUTCOME_AND_JOURNEY_BASELINE_2026-08-20.md](./ATM5B_OUTCOME_AND_JOURNEY_BASELINE_2026-08-20.md).

This record closes the remaining process-death qualification item in the
mandatory Gremlin journey. It does not close the complete ATM-5 release gate.

## 1. Exact crash boundary

The child process performs the following through the public embedded smart
driver:

1. create one physical deployment and capability-bound Heap;
2. create state, turn and locator collections;
3. build one deterministic three-member create/create/create Atomic;
4. submit it through `HeapClient::commit_atomic`; and
5. pause inside the store at `store.atomic.before_ack`.

That failpoint is reached only after:

- durable decision evidence;
- whole-delta publication; and
- receipt determination;

but before the caller acknowledgement boundary returns.

The failpoint registry now provides `Action::Pause`. It parks the executing
thread indefinitely and has no cooperative release. A separate child watchdog
observes failpoint hit-proof state and writes only a boundary marker. It cannot
complete, acknowledge or close the database.

## 2. External kill proof

The parent process:

1. spawns a fresh test process running only the crash child path;
2. waits for the exact before-ack marker;
3. invokes the operating-system child kill operation;
4. waits for termination; and
5. asserts Unix termination signal **9**.

This is not `panic`, `catch_unwind`, injected I/O failure, cooperative
cancellation or child-selected `abort`. No child destructor, orderly-close
barrier or acknowledgement executes after the marked boundary.

## 3. Unclean reopen proof

A new public `Client` then opens the killed deployment and proves:

- `atomic_status(id).logical == Committed`;
- a committed receipt is reconstructible;
- state, turn and locator values are all visible;
- no partial result is observed; and
- rebuilding and submitting the identical plan returns a committed replay.

The plan is rebuilt from persisted collection identities and the same fixed
Atomic identity/content. `receipt.replayed == true` proves that retry did not
manufacture a second transition.

## 4. Evidence

```text
external SIGKILL focused journey              1/1 green, signal == 9
complete embedded driver integration         13/13 green
store failpoint library tests                10/10 green
workspace all targets + all features         check green
```

The unrelated APP-5 RQL corpus mismatch recorded by ATM-5B remains outside
this slice and prevents a claim that the entire SDK test universe is clean.

## 5. Remaining ATM-5 release delta

The mandatory twelve-step Gremlin correctness journey is now covered,
including real process death. Remaining release blockers are:

1. complete the minimum stable Atomic error-code vocabulary from
   `ATOMICS_SPEC` §22 while preserving `NotCommitted` and `Unknown` as outcome
   truth rather than contradictory transport errors;
2. join store-derived recovery, physical sync/group-commit and bounded
   phase-latency telemetry into `Client::inspect().atomics`;
3. execute the declared member/payload/collection/contention matrix,
   randomized/soak campaign and wider crash/damage corpus;
4. prove every absolute performance rule, including no per-member fsync, no
   full-store ordinary commit/open scan, bounded maximum-plan memory and the
   ordinary-write regression ceiling;
5. complete package/API compatibility, public documentation and the clean
   top-level evidence manifest; and
6. record architect acceptance before advertising the capability.

`Capabilities::atomics` remains `false`.
