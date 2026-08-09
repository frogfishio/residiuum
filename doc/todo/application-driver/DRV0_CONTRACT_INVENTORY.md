# DRV-0 — contract and current-runtime inventory

Status: **DRV-0 closed; embedded DRV-1/DRV-5 candidate implemented**  
Authority: [ASYNC_DRIVER_SPINE_SPEC.md](./ASYNC_DRIVER_SPINE_SPEC.md) §17

## Incident motivating the start

An embedded application currently exposes an internal failure shaped as:

```text
session_send ... NOT_FOUND: session missing
```

That diagnostic is not present in Residiuum's source tree. Inspection of the
calling Gremlin/Tinker code identifies the direct cause as a split domain
lifecycle: a runtime agent session may exist without its durable Gremlin
`SessionControl` aggregate. Dialogue sealing later expects that aggregate and
reports the missing half.

This P0 is owned by Gremlin's `SessionManager`, not by the Residiuum driver.
The driver must still remove database connection/scheduling coordination from
applications, but it cannot create or reconcile application-domain aggregates.
DRV-0 therefore makes no claim about fixing this incident.

The current Gremlin CR partially opens `SessionControl` during session creation,
but still needs structural identity/ownership verification, compensation or
recovery when backend creation fails, and an idempotent projection outcome after
the authoritative dialogue commit.

## Landed implementation

- transport-independent request and operation identifiers;
- closed request-stage and terminal-outcome vocabulary;
- typed retry dispositions;
- frozen v1 resource defaults and required wire-feature identifiers;
- machine-readable current blocking/synchronization inventory; and
- a verification gate that rejects `Arc<Mutex<RemoteHeap>>` in new driver code.
- deployment-level `driver::Client`, capability-bound `driver::HeapClient`, and
  `driver::Collection<T>` handles that are `Clone + Send + Sync` and use
  `&self` operations;
- simultaneous authorized multi-Heap bindings sharing one physical writer,
  bounded scheduler, inspection surface, and shutdown domain;
- bounded embedded admission and dedicated synchronous-kernel workers;
- overload, queued cancellation, pre-dispatch deadline, and shared-close behavior;
- collection create/open/list plus get, put, create-if-absent,
  version-conditional replace, and version/presence-conditional delete;
- atomic typed value-plus-version point reads and version-bearing scan rows, so
  every public CAS precondition can be obtained after restart;
- SDK-reexported `HeapCap`, authoritative named-Heap/capability matching, and
  bounded typed collection scan pages with collection-bound continuations;
- bounded Application Core pages on cloneable `Collection<T>` handles and
  bounded Full RQL pages on cloneable Heap bindings, both dispatched through
  the one shared scheduler and canonical QVM runtime;
- persistent `OperationId` deduplication, canonical request hashing, exact
  receipt replay, and typed conflicting-ID refusal;
- structured client errors, retry dispositions, receipts, scheduler inspection,
  and store `OpenReport`; and
- compile, concurrency, replay, conflict, overload, deadline, cancellation, and
  shutdown evidence in `driver_embedded.rs` and the driver unit tests.

## Deliberate non-claims

- No remote pool, reconnect, or non-blocking transport exists yet.
- Bounded ordered scans and one-page Core/Full RQL calls are implemented. The
  ergonomic lazy `QueryCursor<T>` / `Stream` surface is not yet claimed.
- `Capabilities::atomics` is false. No multi-record transaction or Atomic
  behavior is implemented; applications must not infer it from key-level CAS.
- Queued deadlines actively wake through one bounded scheduler timer. An
  already-running synchronous kernel call continues to completion; a mutation
  crossing its deadline reports `CommitOutcomeUnknown` and preserves its
  operation identity for exact replay/outcome resolution.
- New idempotent put/delete frames carry their operation identity and canonical
  request hash in authoritative media. If the derived dedup update is
  interrupted, retry reconstructs the original receipt without another append.
- Receipt-stable idempotent delete currently requires `if_present=true`.
- The existing synchronous façade remains unchanged and legacy-only.
- No second RQL executor is introduced.

## Recommended driver order

Keep the package gates, but execute them in this order after DRV-0 review:

1. DRV-1 mutation identity, request binding, receipts, and structured errors.
2. DRV-2 bounded remote pool and cloneable handles.
3. DRV-5 bounded embedded scheduler, promoted ahead of streamed RQL because the
   shared physical-store contention is independently real.
4. DRV-3 streamed RQL over the now-safe dispatch boundary.
5. DRV-4 deadline, cancellation, retry, and ambiguous-outcome resolution.
6. DRV-6/7 server concurrency and qualification.

Promoting DRV-5 addresses database scheduling and contention pressure. It does
not replace the separate P0 Gremlin session-lifecycle correction.

## Remaining full-driver work

- Freeze sync migration naming and module placement.
- Implement the bounded remote pool and negotiated wire features.
- Build the lazy typed query/cursor façade over the now-dispatched canonical
  Core/Full QVM page calls.
- Implement Heap-local Atomics before exposing any transaction-shaped API.
- Add dispatched cancellation, outcome lookup, and crash-window closure.
- Complete bulk/history/raw/prepared-query surfaces and embedded/remote parity.
