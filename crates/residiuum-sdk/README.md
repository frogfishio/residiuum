# residiuum-sdk

**Collection SDK** for Residiuum: the ordinary application surface.

Open a local store, connect to a remote server, or (optionally) open an
in-process multi-node cluster. Name a collection; put/get/delete JSON or bytes;
filter JSON documents; use pluggable query dialects that compile to pure SDA
(`rql` official human surface — [USER_GUIDE](../../doc/RQL/USER_GUIDE.md); also
`json` / `mongo` / `sql` mimicry / raw `sda`); manage secondary indexes;
inspect per-key history — without learning frames or segments for common paths.

Freeze label: `SDK_API_VERSION` = `1.0`.

## When to use this crate

| You want… | Use |
|-----------|-----|
| Application put/get/find on a local file | **`residiuum-sdk`** (this crate) |
| Same collection API over product remote (HeapKey) | **`residiuum-sdk`** (`Residiuum::connect_heap`) |
| Legacy Stage-7 token remote | **`residiuum-sdk`** (`Residiuum::connect` / `connect_with`) |
| CLI | [`residiuum-cli`](https://crates.io/crates/residiuum-cli) |
| Raw subject store / salvage | [`residiuum-store`](https://crates.io/crates/residiuum-store) |
| Embed a TCP server | [`residiuum-server`](https://crates.io/crates/residiuum-server) |

## Install

```toml
[dependencies]
residiuum-sdk = "0.2.5"   # MPL-2.0: bounded embedded client + Heap collection SDK
```

Optional in-process multi-node cluster (pulls AGPL `residiuum-cluster`):

```toml
residiuum-sdk = { version = "0.2", features = ["cluster"] }
```

Or: `cargo add residiuum-sdk`

### License

| Feature set | Effective license of your dependency graph |
|-------------|--------------------------------------------|
| Default (embedded + remote client) | **MPL-2.0** (+ MIT `residiuum-client` / `residiuum-format` / `residiuum-sda`) |
| `features = ["cluster"]` | Adds **AGPL-3.0-or-later** `residiuum-cluster` |

Network **serve** is a separate AGPL crate (`residiuum-server`), not a default
dependency of this SDK.

## Quick examples

### Bounded embedded client

Use one shared client per process. Handles are cheap to clone across tasks; the
client owns the bounded blocking workers and admission queue.

```rust,no_run
use residiuum_sdk::driver::{
    Client, Collection, EmbeddedOptions, OperationContext, OperationId,
    ReplaceOptions, ScanOptions,
};
use serde_json::Value;

# async fn example(
#     database_path: &std::path::Path,
#     document: Value,
#     command_id: [u8; 16],
# ) -> Result<(), residiuum_sdk::driver::Error> {
# let capability = todo!("load Gremlin's validated Heap capability");
let client = Client::open_embedded(
    EmbeddedOptions::new(database_path, capability)
        .heap_name("gremlin")
        .workers(4)
        .queue_capacity(1024),
).await?;
let conversations: Collection<Value> =
    client.open_collection("conversations").await?;
let current = conversations
    .get_versioned("conversation-7")
    .await?
    .expect("conversation exists");
conversations.replace(
    "conversation-7",
    &document,
    ReplaceOptions {
        if_version: current.version,
        context: OperationContext {
            // Retain and reuse this ID when resolving an uncertain outcome.
            operation_id: Some(OperationId(command_id)),
            ..OperationContext::default()
        },
    },
).await?;
client.close().await?;
# Ok(())
# }
```

This slice provides exact operation replay, version-conditional replacement,
hard queue bounds, active queued deadlines, and an honest
`CommitOutcomeUnknown` result when a mutation deadline crosses after dispatch.
`HeapCap` is re-exported from `residiuum_sdk::driver`; applications do not need
to name the lower-level heap crate merely to open the driver. A new Heap still
requires an authority ceremony—the SDK will not mint authority from a name.

Bounded record traversal uses typed pages rather than an unbounded vector:

```rust,no_run
# use residiuum_sdk::driver::{Collection, ScanOptions};
# use serde_json::Value;
# async fn scan(conversations: Collection<Value>) -> Result<(), residiuum_sdk::driver::Error> {
let mut continuation = None;
loop {
    let page = conversations.scan_page(ScanOptions {
        page_size: 256,
        continuation,
        ..ScanOptions::default()
    }).await?;
    if !page.complete {
        // Inspect page.incomplete; do not infer absence from page.rows.
    }
    for row in &page.rows {
        // row.version is the CAS token for row.value; no point reread required.
    }
    continuation = page.continuation;
    if continuation.is_none() {
        break;
    }
}
# Ok(())
# }
```

Heap-local multi-record Atomics are not implemented in this release. Keep hard
invariants in one version-CAS aggregate record and treat additional keys as
idempotently rebuildable projections.

### Legacy flat embedded API

The following older flat surface requires
`residiuum-sdk = { version = "0.2.5", features = ["legacy-flat-sdk"] }`.

```rust
use residiuum_sdk::{json, Residiuum, Filter};

# let dir = tempfile::tempdir().unwrap();
# let path = dir.path().join("app.residiuum");
let mut db = Residiuum::open(&path)?;
{
    let mut users = db.collection("users")?;
    users.put(
        "user-42",
        &json!({ "name": "Alice", "status": "active", "age": 30 }),
    )?;
    users.indexes()?.create("by-status", &["status"])?;

    let rows = users.find(&Filter::field("status").eq("active"))?;
    // SQL mimicry dialect → pure SDA predicate (doc/SDA/DIALECTS.md)
    let via_sql = users.find_dialect(
        "sql",
        "SELECT * WHERE status = 'active' AND age >= 18",
    )?;
    let hist = users.history("user-42")?;
    let _ = (rows, via_sql, hist);
}
# Ok::<(), residiuum_sdk::Error>(())
```

### Remote (product — HeapKey)

**HAR-4:** product remote is `Residiuum::connect_heap` against a **qualified**
server (`residiuum serve … --qualified-heap-key` + TLS + `--deployment-id`).
There is no shared token on this path.

```rust
use residiuum_sdk::{
    HeapCredential, RemoteHeapOptions, Residiuum, TlsClientOptions,
};

// certificate_cose + HolderSigner from local authority ceremony (HAR-2/HAR-3).
let credential = HeapCredential::new(&certificate_cose, holder)?;
let options = RemoteHeapOptions::new(
    TlsClientOptions::new("localhost").ca_path(ca_path),
    credential,
)
.expected_heap_name("accounts");

let mut heap = Residiuum::connect_heap(
    "residiuum://127.0.0.1:7434/accounts",
    options,
)?;
// RemoteHeap: process ops + collection plane (§32.4). Prefer HeapClient::from(remote)
// for CollectionClient façade when that path is in use.
# let _ = heap;
# Ok::<(), residiuum_sdk::Error>(())
```

Journey:  
[HAR4_T4_CONNECT_HEAP_JOURNEY.md](../../doc/todo/heap-application-ready/HAR4_T4_CONNECT_HEAP_JOURNEY.md).

#### Appendix — legacy token remote (non-product)

Requires a server started with `--legacy-token-server` (Stage-7 / diagnostics).
Not the product remote path.

```rust
use residiuum_sdk::{json, ConnectOptions, Residiuum};
use std::time::Duration;

let mut db = Residiuum::connect_with(
    "residiuum://127.0.0.1:7434/app",
    ConnectOptions::new()
        .auth_token("SECRET")
        .request_timeout(Duration::from_secs(10))
        .max_connect_attempts(5),
)?;
db.collection("users")?
    .put("user-42", &json!({ "name": "Alice" }))?;
# Ok::<(), residiuum_sdk::Error>(())
```

### In-process cluster

Requires `features = ["cluster"]`:

```rust
use residiuum_sdk::{json, ClusterConfig, Residiuum, Filter, QueryOptions};

# let dir = tempfile::tempdir().unwrap();
let mut db = Residiuum::create_cluster(
    ClusterConfig::development(dir.path().join("cluster")).with_virtual_partitions(16),
)?;
{
    let mut users = db.collection("users")?;
    users.put("user-42", &json!({ "status": "active" }))?;
    let covered = users.find_with_coverage(
        &Filter::field("status").eq("active"),
        QueryOptions::new().allow_partial_coverage(),
    )?;
    let _ = covered.coverage.is_complete();
}
# Ok::<(), residiuum_sdk::Error>(())
```

## What you get

| Area | Capability |
|------|------------|
| Embedded | `Residiuum::open`, JSON/bytes put/get/delete, scan + streaming iter |
| Filters | SDK-native `Filter` / `find` / `query`, secondary field indexes, budgets |
| Multi-collection join | `Residiuum::query().from(..).join(..).on(X,Y).collect()`; `.map_sda(..)` normalises |
| SDA/ENR text queries | `Collection::sda` / `filter_sda` (DX §7.6); multi-collection `Residiuum::enr_query().bind(..).run` (`Match`/`enrich`) or `Residiuum::sda(&[…], program)` |
| History | Per-key immutable event stream |
| Chunks | Completeness-aware `get_payload` for large bodies |
| Remote (product) | `Residiuum::connect_heap` — TLS 1.3 + HeapKey; no token |
| Remote (legacy) | `Residiuum::connect` / `connect_with` — Stage-7 token/open path only |
| Parity | Remote put/get/delete/scan, history, indexes, `get_payload`, server-side find, `directory` |
| Cluster | Feature `cluster`: `create_cluster` / `open_cluster`, directory cache, `find_with_coverage` |

Application developers do not need to know about frames or segments.

## API surface

| API | Role |
|-----|------|
| `Residiuum::open` | Create-or-open store directory with safe defaults |
| `Residiuum::connect_heap` | Product remote: TLS + HeapKey (`RemoteHeapOptions`) |
| `Residiuum::connect` / `connect_with` | Legacy Stage-7 token/open remote only |
| `Residiuum::create_cluster` / `open_cluster` | In-process multi-node (`cluster` feature) |
| `Residiuum::collection` | Lazy named collection handle |
| `Collection::put` / `get` / `delete` | JSON values (serde) |
| `Collection::put_bytes` / `get_bytes` | Opaque byte payloads |
| `Collection::get_payload` | Completeness-aware chunked read |
| `Collection::scan_keys` / `scan_json` / `scan_json_iter` / `scan_json_page` | Live scan |
| `Collection::find` / `find_json` / `query` | Filters + index acceleration |
| `Residiuum::query` | Multi-collection equijoin (`from` / `join` / `on`) + optional SDA map |
| `Collection::sda` / `filter_sda` | Raw SDA/ENR1 text over one collection (DX §7.6) |
| `Residiuum::enr_query` / `sda_query` / `sda` | Bind collections → free names + pure SDA/ENR1 text (`Match`/`enrich`) |
| `Collection::find_with_coverage` | Cluster find with explicit partition coverage |
| `Collection::indexes` | Create / drop / rebuild / list secondary indexes |
| `Collection::history` | Immutable event stream for one key |
| `Filter` / `QueryOptions` / `QueryBudget` | Predicates + limit/order/budget |
| `MultiQuery` / `map_joined_sda` | Join bag then pure SDA normalisation |
| `SdaTextQuery` / `eval_sda_program` | Text-program axis (ENR1 match bags + cardinality) |
| `WriteReceipt` / `DeleteReceipt` | Event identity + achieved durability |
| `Error::code` / `ErrorCode` | Stable machine codes |
| `SDK_API_VERSION` | Product freeze label |

## Subject encoding

Logical `(collection, key)` pairs map to store subjects as:

```text
0x01 || coll_len:u16 LE || collection UTF-8 || key UTF-8
```

Payloads are typed:

```text
0x01 || JSON UTF-8 text
0x02 || raw bytes
```

Large bodies may be stored as chunked payloads; ordinary get returns complete
data only when every chunk verifies.

## Out of scope (this crate)

- TCP accept loop / authz / admission — [`residiuum-server`](https://crates.io/crates/residiuum-server)
- SDA examination of holes / recovery units — [`residiuum-examine`](https://crates.io/crates/residiuum-examine)
- Network Raft log shipping as a default path (quorum writes remain in-process
  `open_cluster`; experimental multi-process serve is separate)

## Related crates

| Crate | License | Role |
|-------|---------|------|
| [`residiuum-store`](https://crates.io/crates/residiuum-store) | MPL-2.0 | Single-node store |
| [`residiuum-client`](https://crates.io/crates/residiuum-client) | MIT | Wire framing (re-exported) |
| [`residiuum-server`](https://crates.io/crates/residiuum-server) | AGPL-3.0-or-later | TCP serve |
| [`residiuum-cluster`](https://crates.io/crates/residiuum-cluster) | AGPL-3.0-or-later | Partitions / Raft (`cluster` feature) |
| [`residiuum-cli`](https://crates.io/crates/residiuum-cli) | AGPL-3.0-or-later | Operator CLI |

## Documentation

- DX / product surface: [DX_SPEC.md](https://github.com/frogfishio/dingodb/blob/main/doc/reference/product/DX_SPEC.md)
- Project overview: [README.md](https://github.com/frogfishio/dingodb/blob/main/README.md)
- Licensing: [doc/reference/operations/LICENSING.md](https://github.com/frogfishio/dingodb/blob/main/doc/reference/operations/LICENSING.md)

## License

MPL-2.0 for this crate's sources (default features). Enabling `cluster` adds
AGPL dependencies — see the install section above.

Part of [Residiuum](https://github.com/frogfishio/dingodb).
