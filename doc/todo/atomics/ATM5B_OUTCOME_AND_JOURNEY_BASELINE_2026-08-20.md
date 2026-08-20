# ATM-5B outcome and Gremlin journey baseline — 2026-08-20

Status: **ATM-5B implemented; capability advertisement remains closed**

Predecessor:
[ATM5_ASYNC_SDK_BASELINE_2026-08-20.md](./ATM5_ASYNC_SDK_BASELINE_2026-08-20.md).

This is the continuation point after the first public async composition slice.
It records product-surface correctness delivered in ATM-5B and the exact work
still required for release acceptance.

## 1. Public plan and submission metadata

`driver::atomics::AtomicPlan` is now the non-constructible public plan type. It
contains:

- one private immutable canonical `residiuum_atomics::AtomicPlan`; and
- an optional submission deadline which is explicitly outside canonical bytes.

Changing or removing the submission deadline cannot change the Atomic content
root. `AtomicPlan::with_deadline` permits a safe retry on a renewed connection;
`without_deadline` makes an explicit unbounded retry. Application code cannot
replace the canonical Heap, identity, members, predicates or authority fields.

The builder carries `AtomicOptions::deadline` through to submission. The pure
protocol plan remains transport-independent.

## 2. Cancellation and unknown-outcome polarity

Atomic submission uses the existing bounded scheduler state machine, not a
parallel transaction scheduler:

```text
queued + dropped/deadline
    -> no kernel entry, no prepare, definite request error

dispatched + deadline
    -> AtomicOutcome::Unknown { atomic_id, resolution }
    -> kernel continues to a safe terminal result
    -> whole scheduler byte credit remains held until completion

non-provably-pre-acceptance storage failure
    -> AtomicOutcome::Unknown
    -> resolve with atomic_status or same-ID/same-root retry
```

Only typed protocol/capability refusals are returned as definite errors. A
generic I/O or internal failure is never mislabeled as proof that no Atomic was
issued. Dropping a queued Atomic is proved not to enter the kernel and releases
its indivisible byte credit. Deadline expiry before dispatch is proved to leave
status `NotFound`; renewing the unchanged plan then commits normally.

The driver now exposes stable `AtomicIdConflict` and
`AtomicDeadlineExceeded` machine codes. The store preserves typed
`AtomicsError` refusals instead of flattening them into a string.

## 3. Mandatory Gremlin journey now exercised

The public embedded driver test now covers:

1. one physical `Client` and a capability-bound Heap;
2. state/turn/locator collections;
3. a three-member replace/create/create plan;
4. exact whole visibility and member receipt;
5. same-ID/same-root replay without a new decision;
6. reopen and same decision/replay;
7. same-ID/different-root typed refusal with no new value;
8. two replacements racing from one establishing version: exactly one commits
   and exactly one projection appears;
9. failure at the decision/publish/ack boundary resolving as `Unknown`, then
   committed status and replay;
10. stale state version producing durable `NotCommitted` and no turn;
11. cross-Heap collection refusal before plan close/prepare; and
12. compaction, reopen and resolution of the original committed status.

Step 9 is an in-process injected lost-reply boundary. The mandatory external
process `kill -9` campaign remains open; this test must not be relabeled as the
real process-death proof.

## 4. Bounded inspection delivered

`Client::inspect().atomics` is constant-space and redacted. It reports:

- submitted and in-flight plans;
- committed, not-committed, unknown and refused outcomes;
- identity conflicts and status lookups;
- submitted/max member counts and canonical plan bytes; and
- total/max engine-call latency.

No Heap, collection, key, Atomic identity or payload label is retained.
Counters distinguish engine terminal outcomes from observer `Unknown`.

This is not yet the full ATM-5 telemetry exit. Store-side recovery counts,
physical Atomic group-commit counts, and bounded latency phase histograms must
still be joined into inspection.

## 5. Evidence green at this checkpoint

```text
embedded driver integration                  12/12
focused Atomic scheduler polarity             3/3
pure Atomics unit tests                      108/108
pure Atomics integration suites               all green
store Atomic library tests                    33 green, 1 declared qualification ignore
workspace all targets + all features          check green
```

The declared ignored million-identity qualification is not counted as passed.
Warnings reported by the workspace check predate this slice.

The complete `cargo test -p residiuum-sdk` command is not currently globally
green because the unrelated APP-5 RQL corpus still expects
`from orders after $cursor` to be rejected while the current compiler accepts
it. Atomics did not alter that parser or fixture. This checkpoint therefore
does not claim a clean whole-SDK test gate; the exact Atomic and embedded-driver
suites above are the accepted evidence for this slice.

## 6. Remaining ATM-5 release delta

Before `Capabilities::atomics` may become true:

1. Run the real external-process kill campaign at decision/before-reply and
   prove status/retry after unclean reopen.
2. Complete stable Atomic error-code mapping for every minimum code in
   `ATOMICS_SPEC` §22; do not collapse semantic outcomes into generic conflict.
3. Add store-derived recovery, physical sync/group-commit and phase-latency
   counters without scans or unbounded labels.
4. Execute the member/payload/collection/contention qualification matrix,
   randomized histories, soak and the declared crash/damage campaign.
5. Prove the performance red lines: no per-member fsync, no full-store ordinary
   open/commit scan, bounded maximum-plan memory, ordinary-write regression at
   or below 5%, and disclosed one-/ten-member comparisons.
6. Run the clean-checkout top-level evidence verifier, package/API compatibility
   review, documentation review and architect acceptance record.
7. Add remote feature negotiation only when a remote backend exists; it must
   fail closed when Atomics are absent.

Canonical RQL/RRE lowering and public rule/lifecycle administration remain
separate integrations and may not bypass this submission contract.

## 7. Release gate

`Capabilities::atomics` remains `false`. ATM-5B materially closes the public
correctness journey, but process-death, full telemetry and qualification/perf
evidence remain release blockers.
