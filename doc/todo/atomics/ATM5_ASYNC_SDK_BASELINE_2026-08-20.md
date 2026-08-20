# ATM-5 async SDK baseline — 2026-08-20

Status: **ATM-5A implemented; public capability remains deliberately false**

Normative authorities:

- [ATOMICS_SPEC.md](./ATOMICS_SPEC.md) §15;
- [ATOMICS_IMPLEMENTATION_PLAN.md](./ATOMICS_IMPLEMENTATION_PLAN.md) §11; and
- the accepted ATM-0…ATM-4 evidence named by this programme.

This document records the first product-composition slice. It is a continuation
point, not an ATM-5 acceptance record and not permission to advertise Atomics.

## 1. Delivered product seam

The embedded smart driver now exposes a Heap-bound, typed, async-only Atomic
path:

```text
HeapClient::atomic(options) -> Result<AtomicBuilder, Error>
AtomicBuilder::{create, put_unconditional, replace, delete}
AtomicBuilder::{assert_absent, assert_present, assert_version}
AtomicBuilder::read(...).await
AtomicBuilder::build() -> AtomicPlan
HeapClient::commit_atomic(plan).await -> AtomicOutcome
HeapClient::atomic_status(id).await -> AtomicStatus
```

The builder:

- accepts only typed `Collection<T>` handles from the exact same live
  capability instance as its Heap binding;
- samples Heap identity, collection identity, rights and authority revision
  from the guarded capability kernel, never from application-supplied fields;
- maps ordinary Read/Write rights to the closed Atomic rights vocabulary;
- uses the existing canonical JSON/SDA encoding path;
- resolves planned values and deletes locally through the bounded construction
  overlay; and
- turns external version-bearing point reads into exact present/absence
  witnesses and binds their store frontier.

The pure protocol crate remains free of store, Heap and SDK dependencies. The
temporary composition seam is a hidden unsafe constructor whose contract
requires every field to be sampled from one live capability under its authority
guard. Only the safe SDK bridge calls it. This is a deliberate crate-cycle
avoidance boundary, not a public application construction mechanism. A later
crate-boundary cleanup may replace it, but must not weaken this invariant.

## 2. Submission, status and admission

An immutable plan is submitted as one worker job. Canonical plan bytes are
charged as one indivisible scheduler byte credit. Members are never scheduled
or admitted independently. Oversized plans receive `ResourceLimit`; aggregate
byte pressure receives `Overloaded`; RAII releases credit on completion,
cancellation, queue refusal or worker shutdown.

Commit rechecks the plan Heap against the bound `HeapClient`. The store then
rechecks live Write authority, the authority revision, the Heap identity and
all plan predicates while holding the existing Heap serialization boundary.
Status requires live Read authority and resolves the exact Heap-qualified
`AtomicId` from durable evidence.

There is no synchronous mutation equivalent, no raw store handle and no
server-held interactive transaction.

## 3. Integration defect found and fixed

The first Gremlin journey exposed a pre-existing media-classification defect.
A valid ordinary operation-replay `BatchCommit` carrying envelope keys 41/42
was being treated as corrupt Atomic evidence. This made Atomic preparation
refuse coverage on a healthy store that already contained ordinary SDK writes.

The correction is fail-closed and namespace-specific:

- operation identity/content-hash keys are recognized as the established
  client-operation namespace by Heap ownership parsing;
- a batch frame whose examination result is `Unsupported / NotAtomicEvidence`
  no longer makes an ordinary file Atomic-related; and
- partial, corrupt or valid Atomic evidence still makes the file Atomic-related
  and remains subject to the strict recovery catalogue.

A direct format regression and the real SDK journey pin this distinction.

## 4. Evidence currently green

The current slice proves:

- typed three-record state/turn/locator commit with exact whole visibility;
- external version witness and read-your-plan overlay;
- same ID/root replay in-process and after orderly restart;
- committed status in-process and after restart;
- stale-version `NotCommitted` with no partial projection;
- cross-Heap collection refusal before build/prepare;
- valid operation-replay media does not poison the Atomic catalogue; and
- indivisible weighted admission and byte-credit release.

Focused evidence at this baseline:

```text
residiuum-atomics                       108 unit tests + all integration suites green
residiuum-sdk driver Atomic journey    2/2 focused tests green
residiuum-format operation namespace   direct recovery-classification regression green
```

The clean full-workspace verifier is still required before acceptance.

## 5. Deliberate API variance requiring architect closure

The frozen specification shows `HeapClient::atomic(options) -> AtomicBuilder`.
The implementation returns `Result<AtomicBuilder, Error>` because an expired
construction deadline, unsupported scope or limits above the hard ceiling can
already make the requested builder invalid. This is honest early refusal and
avoids a panic or a builder containing a latent initialization error. Before
release, either amend the normative signature to this result-bearing form or
introduce a validated public options type that makes construction infallible.
Do not silently defer the error.

## 6. Remaining ATM-5 delta

The following work is mandatory before `Capabilities::atomics` may become
true:

1. Complete the twelve-step Gremlin journey: changed-content identity
   conflict, two-writer CAS race, kill after decision/before reply, and status
   after compaction remain to be exercised through this public driver surface.
2. Carry submission deadline/cancellation metadata through admission. Prove
   definite pre-admission cancellation and `Unknown` plus resolution after the
   commit sequencer accepts work. The canonical plan must remain independent of
   transport metadata.
3. Add bounded/redacted Atomic inspection counters for queueing, outcomes,
   conflicts, latency phases, members, bytes, recovery and group commit.
4. Run the declared member/payload/collection/contention qualification matrix,
   crash campaign, randomized histories, soak and performance comparison.
5. Prove no per-member fsync, no database-wide ordinary-open/commit scan, the
   ordinary-write regression ceiling, and bounded memory at maximum plan size.
6. Complete public documentation, package/compatibility review, and remote
   fail-closed feature negotiation when a remote backend exists.
7. Run the top-level clean-checkout evidence verifier and record the architect
   acceptance decision.

Canonical RQL/RRE lowering and public rule/lifecycle administration remain
separate integrations. They do not justify weakening or bypassing the point
Atomic contract.

## 7. Release gate

`Capabilities::atomics` remains `false`. Applications may exercise this API in
qualification builds, but its existence is not an advertised product promise
until every item in §6 and the ATM-5 exit gate passes.
