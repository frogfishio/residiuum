# Residiuum Async Driver Spine Specification

Status: **developer-ready v1.0**  
Program: `DRV`  
Priority: **RQL critical-path infrastructure**

Audience: SDK, client, server, protocol, store, query, telemetry, and test
implementers.

Normative companions: [critical path](../../../CRITICAL_PATH.md),
[DX doctrine](../../reference/product/DX_SPEC.md),
[application API plan](../application-baseline/CORE_APPLICATION_API_IMPLEMENTATION_PLAN.md),
[RQL qualification](../rql/RQL_QUERY_QUALIFICATION_PROGRAM.md),
[Heap specification](../../wip/heap/HEAP_SPEC.md), and
[testing strategy](../../reference/engineering/TESTING_STRATEGY.md).

This is the sole implementation specification for the async application
driver. It refines `DX_SPEC.md`; it does not replace it or create another
roadmap.

## 1. Product decision

Residiuum SHALL expose an **async-first deployment connection** with separate
capability-bound Heap handles. One physical connection owns pooling,
scheduling, backpressure, deadlines, cancellation, retry classification,
mutation identity, cursor paging, and connection recovery. Any number of
authorized Heap handles may share that connection and are cheap to clone and
safe to use concurrently.

Applications MUST NOT need a database mutex, semaphore, `spawn_blocking`,
socket manager, retry loop, or pagination loop merely to use Residiuum safely
from an async runtime.

The synchronous storage kernel remains synchronous. Async is a boundary and
scheduling concern; it SHALL NOT fork or infect verified storage semantics.

```text
application futures / streams
          |
          v
deployment Client (one physical connection)
          |
          v
authorized HeapClient handles (Clone + Send + Sync)
          |
          v
driver: pool | admission | deadline | cancellation | retry | telemetry
          |
          +-------------------------+
          |                         |
          v                         v
remote async transport      embedded bounded scheduler
          |                         |
          v                         v
server execution            synchronous storage kernel
          +------------+------------+
                       v
              one semantic engine
```

Embedded, server, and future cluster deployment SHALL expose one public
semantic contract: the same options, values, receipts, errors, coverage,
continuations, and retry rules.

## 2. Current deficiency

The present API is a synchronous façade, not a competitive driver:

1. remote collections share `Arc<Mutex<RemoteHeap>>`;
2. `RemoteHeap` owns one blocking stream and permits one in-flight RPC;
3. many operations require `&mut self`;
4. there is no network connection pool;
5. queries return pages/vectors rather than async streams;
6. cancellation/deadlines are not carried end-to-end remotely;
7. not every mutation has an idempotency identity;
8. some remote options and receipts are weaker than embedded equivalents; and
9. broad store locks can serialize otherwise independent work; and
10. a physical client connection is incorrectly conflated with one Heap
    authorization, preventing simultaneous multi-Heap use of one deployment.

Putting `async fn` around the current mutex is explicitly forbidden as the
final design. It only makes the same one-socket queue asynchronous.

## 3. Scope and priority boundary

Included now because RQL cannot be qualified honestly without it:

- async Rust API and concurrent handles;
- bounded remote pool and non-blocking I/O;
- streamed RQL;
- deadlines, cancellation, safe retries, and complete receipts;
- driver admission/backpressure;
- bounded embedded scheduler;
- server read concurrency; and
- qualification, observability, and sync compatibility.

The API reserves extension points for Atomics and cluster routing, but does not
implement them. Non-Rust bindings, watches, multiplexing, offset pagination,
and unrelated SDK breadth are excluded. No client-side query executor or
hidden fallback scan may be added.

## 4. Normative laws

### DRV-L1 — one semantic universe

For every admitted operation `op` and logical state `S`:

```text
observe(embedded_async(op, S)) == observe(remote_async(op, S))
```

Observation includes value, receipt, stable error, coverage, continuation,
achieved durability, and retry disposition.

### DRV-L2 — concurrent handles

`Client`, `HeapClient`, `Collection<T>`, `RawCollection`, and prepared queries
MUST be `Clone + Send + Sync`. Ordinary operations use `&self`, never `&mut
self`.

### DRV-L3 — bounded resources

For configured bounds `Cmax`, `Qmax`, `Wmax`, `Pmax`:

```text
connections <= Cmax
queued operations <= Qmax
embedded running operations <= Wmax
prefetched pages per cursor <= Pmax
```

Excess load waits within deadline or receives a typed refusal.

### DRV-L4 — one terminal outcome

Every request reaches exactly one driver terminal class:

```text
Completed | Refused | CancelledBeforeDispatch |
CancelledAfterDispatch | DeadlineExceeded | CommitOutcomeUnknown
```

A cancelled dispatched mutation is never called aborted without evidence.

### DRV-L5 — mutation identity

Every mutation has one 128-bit `OperationId`, minted before first dispatch.
Retry, redirect, reconnect, and outcome lookup preserve it. Reuse with a
different canonical request hash is `OperationIdentityConflict`.

### DRV-L6 — no weakening

Unsupported durability, consistency, coverage, deadline, cancellation,
receipt, or idempotency requirements cause typed refusal. Options are never
ignored or silently weakened.

### DRV-L7 — cursor honesty

Cursors bind Heap, collection, QVM/query, parameters, order, read view,
coverage, and continuation. Resume uses the server continuation/last-examined
position, never merely the last successful row.

### DRV-L8 — one query runtime

The driver invokes the canonical QVM path. It SHALL NOT evaluate a parallel
Rust predicate or invent an async query executor.

### DRV-L9 — backpressure before collapse

Capacity exhaustion becomes bounded queue wait and typed overload, never an
unbounded socket backlog, thread pool, task set, or buffer.

### DRV-L10 — cancellation is not rollback

Cancellation stops waiting and requests cooperative termination. A dispatched
mutation requires a deduplicated outcome or `CommitOutcomeUnknown`.

### DRV-L11 — connection is not Heap

One `Client` represents one physical deployment connection and owns its writer,
pool/scheduler, and shutdown state. Authorization is introduced only by
creating a `HeapClient` from a validated `HeapCap`. Opening another authorized
Heap MUST reuse the existing physical connection and MUST NOT acquire another
writer lock. Closing the `Client` closes every derived Heap and collection
handle; dropping one `HeapClient` does not close its siblings.

## 5. Public Rust contract

Names and observable shapes below are normative for v1. Tokio is the initial
runtime.

```rust
pub struct Client { /* one physical deployment connection */ }
pub struct HeapClient { /* Client + one validated HeapCap */ }
pub struct Collection<T = serde_json::Value> { /* HeapClient + CollectionId */ }
pub struct RawCollection;

impl Client {
    pub async fn connect(options: ConnectOptions) -> Result<Self, Error>;
    pub async fn open_embedded(options: EmbeddedOptions) -> Result<Self, Error>;
    pub async fn open_heap(&self, capability: HeapCap) -> Result<HeapClient, Error>;
    pub async fn open_named_heap(&self, name: &str, capability: HeapCap)
        -> Result<HeapClient, Error>;
    pub fn capabilities(&self) -> &Capabilities;
    pub async fn close(&self) -> Result<(), Error>;
}

impl HeapClient {
    pub fn connection(&self) -> &Client;
    pub fn heap_id(&self) -> HeapId;
    pub fn capabilities(&self) -> &Capabilities;
    pub async fn create_collection<T>(&self, name: &str, options: CreateCollectionOptions)
        -> Result<Collection<T>, Error>;
    pub async fn open_collection<T>(&self, name: &str)
        -> Result<Collection<T>, Error>;
    pub async fn list_collections(&self) -> Result<Vec<CollectionInfo>, Error>;
}

impl<T> Collection<T>
where T: Serialize + DeserializeOwned + Send + Sync + 'static {
    pub fn heap_id(&self) -> HeapId;
    pub fn id(&self) -> CollectionId;
    pub fn name(&self) -> &str;
    pub async fn put(&self, key: impl Into<Key>, value: &T)
        -> Result<WriteReceipt, Error>;
    pub async fn put_with(&self, key: impl Into<Key>, value: &T, options: PutOptions)
        -> Result<WriteReceipt, Error>;
    pub async fn create(&self, key: impl Into<Key>, value: &T, options: CreateOptions)
        -> Result<WriteReceipt, Error>;
    pub async fn replace(&self, key: impl Into<Key>, value: &T, options: ReplaceOptions)
        -> Result<WriteReceipt, Error>;
    pub async fn get(&self, key: impl Into<Key>) -> Result<Option<T>, Error>;
    pub async fn get_item(&self, key: impl Into<Key>) -> Result<Option<Item<T>>, Error>;
    pub async fn delete(&self, key: impl Into<Key>, options: DeleteOptions)
        -> Result<DeleteReceipt, Error>;
    pub async fn history(&self, key: impl Into<Key>, options: HistoryOptions)
        -> Result<History<T>, Error>;
    pub fn query(&self, source: impl Into<String>) -> Query<T>;
    pub fn raw(&self) -> RawCollection;
}
```

The `Client` is bound to exactly one physical deployment, writer ownership
domain, and resource scheduler. A `HeapClient` is bound to exactly one
authenticated Heap. Multiple `HeapClient`s may share one `Client`; they MUST
not share capabilities, cursor authority, collection identities, or data.
Ordinary data methods that accept a caller-supplied `HeapId` are forbidden.
Typed decode failures never alter stored data and report a stable code, key,
and expected host type.

### 5.1 Queries

```rust
let mut rows = products
    .query("from products where price < $max order by rating desc")
    .bind("max", 100)?
    .page_size(256)?
    .coverage(CoveragePolicy::Complete)
    .consistency(Consistency::Current)
    .deadline(Duration::from_secs(5))
    .stream().await?;

while let Some(row) = rows.try_next().await? { consume(row); }
```

```rust
pub struct Query<T>;       // owns a Collection clone and QVM/source/options
pub struct QueryCursor<T>; // bounded lazy page state
pub struct QueryPage<T>;   // rows + continuation + coverage + evidence

impl<T> Query<T> {
    pub fn bind<V: Serialize>(self, name: &str, value: V) -> Result<Self, Error>;
    pub fn page_size(self, value: NonZeroU32) -> Result<Self, Error>;
    pub fn limit(self, value: NonZeroU64) -> Self;
    pub fn coverage(self, value: CoveragePolicy) -> Self;
    pub fn consistency(self, value: Consistency) -> Self;
    pub fn budget(self, value: QueryBudget) -> Self;
    pub fn deadline(self, value: Duration) -> Self;
    pub fn operation_context(self, value: OperationContext) -> Self;
    pub async fn page(self) -> Result<QueryPage<T>, Error>;
    pub async fn stream(self) -> Result<QueryCursor<T>, Error>;
    pub async fn explain(self) -> Result<QueryExplanation, Error>;
}
```

`QueryCursor<T>` implements `Stream<Item = Result<T, Error>>` and explicit
`next_page()`. It fetches lazily and prefetches one page by default.
Materialization requires a query limit, configured cap, or explicit budget.
Dropping a cursor releases connections/buffers and best-effort cancels
remaining work. A v1 cursor never pins a socket between pages.

### 5.2 Bulk

The driver provides bounded bulk calls so applications do not spawn one future
per document. Options declare ordered/unordered behavior, item/byte caps,
durability, deadline, and identity policy. Results contain one terminal result
per admitted input sequence; known item outcomes survive transport failure and
unknown items are explicitly `OutcomeUnknown`.

### 5.3 Operation context

```rust
pub struct OperationContext {
    pub request_id: Option<RequestId>,
    pub operation_id: Option<OperationId>,
    pub deadline: Option<Deadline>,
    pub consistency: Option<Consistency>,
    pub durability: Option<Durability>,
    pub coverage: Option<CoveragePolicy>,
    pub retry: RetryPolicy,
    pub trace: TraceContext,
}
```

Precedence is `operation > collection > client > negotiated profile`.
Merging is deterministic and inspectable. A future Atomic binding is reserved
but not implemented here.

### 5.4 Sync compatibility

The sync API remains in an explicit module during migration and uses the same
types, wire, dispatcher semantics, and errors. It SHALL NOT retain a second
remote executor or create a Tokio runtime per call.

## 6. Remote driver and pool

`ClientInner` owns a pool per endpoint/topology member. Under RPC v1 each
connection has at most one in-flight request; concurrency comes from bounded
pooled connections. Multiplexing is later and requires a negotiated feature,
reader/writer tasks, bounded request demultiplexing, out-of-order and
cancellation tests, and control-frame fairness.

| Setting | v1 default | Rule |
|---|---:|---|
| `min_connections` | 0 | lazy open allowed |
| `max_connections` | 10 | hard per endpoint |
| `max_connecting` | 2 | hard handshake bound |
| `max_waiters` | 1024 | hard checkout queue |
| connect timeout | 5 s | per attempt |
| checkout timeout | 5 s | shortened by operation deadline |
| request timeout | 30 s | shortened by operation deadline |
| idle timeout | 60 s | preserve minimum |
| max lifetime | 30 min | retire after request |
| cursor prefetch | 1 page | hard per cursor |

Checkout is bounded FIFO. Cancel, outcome lookup, route refresh, and credential
refresh have a small bounded control reserve. There is no user priority in v1.
One deadline covers queue, checkout, network, execution, decode, backoff, and
retry; it never restarts by phase.

Every connection performs TLS and Heap-key authentication. Credentials are
zeroizable and absent from logs/debug. Credential generation change drains
idle old connections immediately and busy ones after their request.

## 7. Deadline and cancellation contract

The client uses monotonic local time. Wire requests carry remaining duration
and, where negotiated, a server-evaluable deadline. The server applies the
earlier valid bound. Checks occur before queueing/dispatch, during checkout,
at server admission/QVM checkpoints, before expensive tier/index work, before
response serialization, and during backoff.

```text
Created -> Queued -> CancelledBeforeDispatch
                  -> Dispatched -> Completed | Refused
                                -> CancelRequested
                                     -> CancelledAfterDispatch
                                     -> Completed
                                     -> CommitOutcomeUnknown
                                -> DeadlineExceeded
```

Queued cancellation never reaches the server. A dispatched v1 read may retire
its connection when cooperative cancellation cannot be confirmed. A mutation
resolves by `OperationId`; socket closure is not an abort proof.

Required negotiated features:

```text
request-deadline-v1
cancel-request-v1
operation-outcome-v1
complete-receipts-v2
```

The envelope adds `request_id`, `operation_id`, `deadline_remaining_ms`, and
`trace_context`. Mutation identity is mandatory in the async profile. Cancel
and outcome lookup are control operations.

## 8. Retry, receipts, and errors

| Outcome | Automatic retry |
|---|---|
| transient read failure | yes, within deadline |
| page with authenticated continuation | yes, same query identity |
| mutation with dedup/outcome support | yes, same OperationId |
| mutation without dedup support | no; refuse before dispatch |
| validation/auth/permission/conflict | no |
| incomplete coverage/data damage | no |
| overload with retry hint | bounded within deadline |
| outcome retention expired | no; unknown commit |

Backoff is bounded exponential with jitter and one deadline. The server binds
OperationId to a canonical hash of Heap, collection, operation, keys/members,
payload, conditions, durability, and semantic options. Dedup returns the
original receipt.

Every receipt includes operation/request/Heap/collection/key/event/version,
requested durability, achieved acknowledgement, committed, deduplicated,
partition when relevant, and evidence. Remote code MUST NOT insert zero IDs,
inferred durability, or placeholders. Missing required fields are protocol
violations; inability to achieve durability is refusal.

Errors are structured:

```rust
pub struct Error {
    pub code: ErrorCode,
    pub class: ErrorClass,
    pub message: String,
    pub request_id: Option<RequestId>,
    pub operation_id: Option<OperationId>,
    pub retry: RetryDisposition,
    pub context: ErrorContext,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}
```

Stable codes distinguish validation, authentication, permission, capability,
not-found, already-exists, conflict, incomplete coverage, damage, deadline,
both cancellation stages, pool wait/exhaustion, overload, transient network,
protocol violation, identity conflict, unknown commit, unavailable, and
internal. Retry disposition is `Never`, `SafeSameRequest`,
`SafeSameOperationId`, `After(Duration)`, or `OutcomeLookupRequired`.
Applications never parse error text.

## 9. Embedded adapter

Embedded mode dispatches the synchronous kernel through dedicated bounded
workers, not a global unbounded blocking pool.

| Setting | v1 default |
|---|---:|
| workers | `min(4, available_parallelism)`, at least 1 |
| queue | 1024 operations |
| cursor prefetch | 1 page |

Queued cancellation removes work. Once kernel work begins it completes to a
safe boundary; mutation outcome stays resolvable by OperationId. Worker count
does not imply storage parallelism: qualification records queue, lock wait,
kernel time, and actual overlap.

## 10. Server execution contract

The server SHALL stop holding one process-wide exclusive store lock for an
entire read/query. It separates frame handling, authentication/admission,
read/query execution, controlled mutation admission, lifecycle, and response
serialization. Network I/O never occurs under a store critical-section lock.

Safe point reads and RQL proceed concurrently under the frozen read-view
model. A simple `Mutex` to `RwLock` substitution is not acceptance: barrier
tests must prove overlap without writer starvation, locator races, or false
coverage.

Mutations remain under qualified store/AWO authority. Driver concurrency never
bypasses persist-before-publish, barriers, Heap isolation, or fencing. Mixed
query/write qualification covers sealing, index publication, reopen,
compaction, and damage.

## 11. Future integration boundaries

RQL uses the one canonical QVM, executes remotely on the server, and explains
the executed plan. Qualification records engine and application-observed time.

The future Atomic shape is reserved:

```rust
client.atomic(options, |atomic| async move {
    let users = atomic.collection::<User>("users");
}).await
```

No Atomic behavior is implemented by DRV. `ClientInner` also uses an endpoint/
topology abstraction now so future route refresh does not change collection
semantics; it contains logical identity, not socket/leader ownership.

## 12. Observability

Metrics cover pool state/checkouts/wait, queue depth/wait, requests in flight,
operation latency/attempts/retries, cancellation/deadline stage, unknown
outcomes, cursors/pages/prefetch bytes, embedded workers, and lock wait.

Heap IDs, keys, query text/parameters, credentials, error text, and operation
IDs are forbidden as unbounded labels. Traces separate queue, checkout,
connect/auth, request write, server admission, query/store, durability, and
response decode. `Client::inspect()` returns redacted bounded state without a
scan or mutation.

## 13. Compatibility and ownership

Pooling works with framed RPC v1; new semantics are feature-negotiated and fail
closed. Unknown optional fields are ignored; missing mandatory fields fail.
Old `HeapClient`, `CollectionClient`, and `RemoteHeap` have a declared
deprecation window, but new handles MUST NOT hide their current mutex path.

Preferred ownership:

```text
residiuum-client   async framing/TLS/pool/dispatcher
residiuum-sdk      public Client/Collection/Query and semantic adapters
residiuum-server   control operations and concurrent execution
residiuum-store    synchronous kernel and qualified read facilities
```

A new crate requires a demonstrated dependency or release-boundary need.

## 14. Verification

Compile tests prove handle traits and no mutable ordinary API. A scripted
server controls delays, disconnects, partial frames, overload, cancellation,
and committed-response loss. It must prove:

1. one slow connection does not block work on other available connections;
2. connections/waiters never exceed bounds;
3. cancelled queued work is not dispatched;
4. cursor drop releases resources;
5. expired work does not retry;
6. reconnect preserves OperationId;
7. response loss yields original receipt or unknown commit;
8. conflicting identity reuse refuses;
9. connection poison is isolated;
10. credential rotation drains old authority; and
11. shutdown leaks nothing.

Property/concurrency tests generate issue, queue, checkout, dispatch,
response, disconnect, cancel, deadline, retry, dedup, close, and recycle
interleavings and check DRV-L3..L5. Loom or equivalent covers pool and terminal
delivery races.

The same corpus runs on embedded async and remote async and compares values,
conditional outcomes, receipts, durability, history/holes, RQL ordering/
coverage/continuation/explain identity, and error/retry semantics.

Barrier tests prove concurrent server reads (including a slow query beside
short reads), mixed reads/writes, seal/index publication, cancellation,
saturation, and damage. Sustained churn bounds FDs, sockets, tasks/threads,
queues, buffers, RSS, and outcome retention.

Performance reports application latency, queue, checkout, wire, admission,
query/store, decode, p50/p95/p99, throughput, CPU, RSS, bytes, and timeouts at
concurrency 1,2,4,8,16,32 and saturation. Comparisons use identical
connections, durability, payloads, operations, and validated results.

## 15. Delivery packages

1. **DRV-0 — contract/inventory:** compile fixtures; machine-readable blocking,
   lock, option, receipt, and retry inventory; registries; architecture gates.
2. **DRV-1 — parity/identity:** common types, OperationId on every mutation,
   request binding/outcome lookup, full remote options/receipts, conformance.
3. **DRV-2 — async pool:** Tokio transport/TLS/auth, bounded FIFO pool,
   concurrent handles, reconnect/shutdown, telemetry, HOL isolation.
4. **DRV-3 — RQL stream:** owned query, cursor, exact continuation, bounded
   prefetch/materialization, drop cleanup, async corpus.
5. **DRV-4 — deadline/cancel/retry:** one deadline, control operations, server
   checkpoints, retry machine, ambiguous commit, race matrix.
6. **DRV-5 — embedded scheduler:** bounded workers/queue, safe cancellation,
   telemetry, dual-backend conformance.
7. **DRV-6 — server reads:** narrow global locking, qualified read view,
   concurrent point/RQL execution, writer/lifecycle correctness.
8. **DRV-7 — qualification/sync:** leak and performance campaigns, common sync
   façade, deprecation map, capabilities.
9. **DRV-8 — docs/examples:** CI-run remote/embedded, streamed RQL,
   cancellation, retry/unknown outcome, bulk, and test utilities.

Each package's gate is the matching law/verification above. Later packages may
scaffold types but may not claim predecessors.

## 16. Product acceptance gates

All must pass:

1. concurrent `&self` handles and async-first docs;
2. enforced bounds for pool, queues, workers, prefetch, frames, materializing,
   and retries;
3. no client-wide head-of-line blocking when capacity exists;
4. embedded async, remote async, and sync semantic conformance;
5. honest one-QVM streamed RQL;
6. distinct, tested queued/read/query/mutation cancellation;
7. mutation identity and safe response-loss handling;
8. complete remote receipts with no ignored options/placeholders;
9. typed decisions without text parsing;
10. proven server read overlap with safe writers/lifecycle;
11. no unbounded resource growth;
12. attributed driver overhead using application-observed RQL;
13. per-connection Heap authentication and authority rotation;
14. observable saturation; and
15. CI examples containing no application connection machinery.

Principal acceptance records commit, wire/runtime/server profiles, platform,
configuration, and evidence. No subpackage claims the whole spine.

## 17. First developer instruction

Start **DRV-0 only**. Its first pull request contains no Tokio rewrite. It
delivers:

1. exact compile fixtures from section 5;
2. machine-readable current synchronization/blocking inventory;
3. error, receipt, option, request-state, and wire-feature registries;
4. gates forbidding `Arc<Mutex<RemoteHeap>>` behind new async types and a
   second RQL executor;
5. test/evidence paths for DRV-1..4;
6. sync naming/module compatibility decision; and
7. explicit unresolved residuals.

DRV-1/2 begin only after DRV-0 principal review. This prevents an attractive
async façade from freezing the current serialized architecture underneath it.
