# Gremlin embedded-driver handoff

Status: **qualified integration candidate; use the bounded embedded slice only**

Minimum SDK package for one physical connection with simultaneous Tinker and
Gremlin Heap bindings: **0.3.0**.

Authority: [Async Driver Spine Specification](./ASYNC_DRIVER_SPINE_SPEC.md)

## What Gremlin should adopt now

Use one `residiuum_sdk::driver::Client` as the physical deployment connection
inside one application process. It owns the sole writer and bounded scheduler.
Bind both the Tinker and Gremlin capabilities through that connection to obtain
separate `HeapClient`s. Clone the connection, a `HeapClient`, or a typed
`Collection<T>` into concurrent tasks, and close the connection once during
application shutdown.

The application must not put a mutex or semaphore around the client, open the
deployment for each request, call the synchronous Heap API from async tasks, or
create its own blocking worker pool. The client already provides a bounded
queue and dedicated synchronous-kernel workers.

```rust
use residiuum_sdk::driver::{
    Client, Collection, EmbeddedOptions, ErrorCode, HeapCap, OperationContext,
    OperationId, ReplaceOptions, ScanOptions,
};
use serde_json::Value;

// Both capabilities were validated by Residiuum authority handling.
let connection = Client::open_embedded(
    EmbeddedOptions::new(database_path)
        .workers(4)
        .queue_capacity(2048),
).await?;
let tinker = connection.open_named_heap("tinker", tinker_capability).await?;
let gremlin = connection.open_named_heap("gremlin", gremlin_capability).await?;

let sessions: Collection<Value> = gremlin.open_collection("sessions").await?;

// Collection handles are cheap Clone + Send + Sync values.
let sessions_for_task = sessions.clone();

// Version and value are read atomically and survive process restart.
let current = sessions_for_task
    .get_versioned(session_key)
    .await?
    .expect("session exists");

// Mint this before the first attempt and retain it if this logical mutation is
// retried. Reusing it with different content is refused.
let operation_id = OperationId(existing_or_new_128_bit_id);
let receipt = sessions_for_task.replace(
    session_key,
    &document,
    ReplaceOptions {
        if_version: current.version,
        context: OperationContext {
            operation_id: Some(operation_id),
            ..OperationContext::default()
        },
    },
).await?;

connection.close().await?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Use unconditional `Collection::put` only where last-writer-wins overwrite is
an intentional application rule. Authoritative conversation/session updates
must use `get_versioned` plus `replace`. When Gremlin owns a retry loop or
persists a command identity, it must supply the same non-zero `OperationId` on
every attempt for that same logical mutation.

## Failure handling

Decisions use `Error.code` and `Error.retry`; Gremlin must not parse error
messages.

- `Overloaded` means the hard queue bound protected the process. Respect the
  `RetryDisposition`; do not create an unbounded retry task set.
- `Conflict` means a create/replace/delete precondition lost a race. Re-read
  application state before deciding on a new logical operation.
- `OperationIdentityConflict` means one operation ID was reused for different
  content. This is an application identity bug and must not be retried.
- `CommitOutcomeUnknown` means the deadline crossed after kernel dispatch.
  Resubmit the identical mutation with the same `OperationId`; authoritative
  media resolves the original receipt without appending the command twice.
- `Unavailable` on a mutation may require the same operation-identity
  resolution. Never report failure as proof that the mutation did not commit.
- `Closed` means shared shutdown has started; every clone observes it.

## Supported handoff surface

- one cloneable deployment-level `Client`, multiple capability-bound
  `HeapClient`s, and typed `Collection<T>` handles;
- simultaneous Tinker and Gremlin Heap access through one physical writer and
  one bounded scheduler;
- bounded embedded workers and admission queue;
- collection create/open/list;
- typed JSON get, atomic value-plus-version get, and version-bearing bounded
  ordered scan pages;
- bounded Application Core query pages on `Collection<T>` and bounded Full RQL
  pages on a Heap binding, using the shared scheduler and canonical QVM;
- durable idempotent put and create-if-absent;
- version-conditional replace and delete;
- stable structured errors, retry dispositions, and receipts;
- active queued deadlines, truthful dispatched-mutation outcomes, and
  queued-cancellation enforcement;
- authoritative operation identity recovery across the append-to-dedup update
  crash window and history-loss compaction; and
- redacted scheduler and store-open inspection.

## Explicit residuals

This handoff does not claim remote pooling, a lazy typed RQL stream/cursor,
cancellation of a running synchronous kernel call, automatic retry, a separate
status-only outcome API, bulk calls, or multi-record Atomics. Those remain
driver work and must not be recreated in Gremlin.

`HeapCap` is re-exported from `residiuum_sdk::driver`, and
`Client::open_named_heap(...)` verifies the supplied capability against the
authoritative published Heap descriptor. The SDK still does not create
authority or mint a Heap capability:
new-Heap creation remains an explicit authority ceremony, not a name-only
database open that bypasses isolation.

For bounded migration or projection repair, use `Collection::scan_page` with
`ScanOptions`. Page size is hard-limited to 1,000 complete rows. Continue only
with the opaque continuation returned by the preceding page. Check `complete`
and `incomplete`; an empty `rows` vector alone is not proof that no live keys
exist when the page reports holes. Every complete `ScanRow` carries the
establishing event ID in `version`, obtained with its body under the same store
lock and usable as a `ReplaceOptions::if_version` CAS token.

Use `Collection::get_versioned` whenever a point read will feed a conditional
replace or delete. Its `VersionedValue::version` remains available after a
process restart because it is reconstructed from authoritative store state;
applications must not rely on an earlier in-memory write receipt.

## Canonical Gremlin persistence profile

Do not reproduce Koderra's content-addressed persistent tree through the new
client. For the immediate migration, use one collection key per conversation
and store the complete conversation document. Each completed command is one
version-conditional `replace` using the command's stable `OperationId`.

This gives Gremlin one authoritative document mutation, one event, one index
entry, exact retry, history, and optimistic concurrency. Residiuum supports
large values directly; Koderra branch nodes, commit maps, turn locators, and
application-owned publication machinery are unnecessary on this path.

If full-document rewriting later becomes measurably expensive, the supported
next profile is one authoritative `TurnCommit` record per ordinal with derived
current-state and turn-ID indexes. Do not split one command into several writes
and call it atomic: LocalHeap multi-key Atomics are a separate unimplemented
product capability.

### Multi-record recovery protocol

Until Heap-local Atomics exist, every hard invariant must linearize in one
authoritative record. Gremlin should use the conversation document as that
record and commit it with version CAS plus the command `OperationId`.

Anything requiring another key—session directory entries, turn-ID lookup,
search material, summaries, and caches—is a derived projection. Projection
writers must be deterministic and idempotent from `(aggregate_id, version)`,
may lag the authoritative record, and must be rebuildable by bounded scans.
Write projection progress only after the derived write succeeds. On restart,
resume after the recorded progress and safely repeat the last projection.

Reads that enforce a hard invariant must consult the authoritative aggregate,
not treat a missing or stale projection as proof of absence. If Gremlin finds
an invariant that genuinely requires two authoritative keys to change as one
decision, stop and retain the current schema until Residiuum Atomics ship.

Gremlin's runtime-agent/session-domain consistency remains an application
invariant. The driver protects database access and mutation identity; it cannot
invent or repair a missing Gremlin aggregate.

## Qualification evidence

The embedded contract suite exercises concurrent cloned handles, hard admission
bounds, exact replay, identity conflicts, version-conditional replace/delete,
shared close, active queued deadlines, dispatched-mutation outcome uncertainty,
and release of completed deadline registrations.

The Gremlin profile test writes and version-conditionally replaces one roughly
half-megabyte conversation document, proves same-operation replay produces no
second event, and proves a stale writer cannot replace the current document.
Store crash tests prove put and delete recovery after an authoritative append
whose ledger update failed, including compaction with source reclaim before the
retry.

The supplied 2.9 GiB Gremlin/Tinker fixture opened successfully with 1,203,046
live records and no integrity findings. Its existing Koderra collections are
ordinary Heap collections and remain readable, but their content-addressed
schema is application data: adopting this client does not automatically fold
those 1.2 million records into conversation documents. That is a separate,
explicit migration.
