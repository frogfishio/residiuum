# residiuum-server

**AGPL-3.0-or-later** networked serve path for Residiuum: bounded accept loop,
authorization, protocol admission, TLS bind policy, and network Raft control /
data-plane glue.

Use this crate when you embed a Residiuum TCP server in a Rust binary (the
`residiuum` CLI does exactly that). Application collection APIs and remote clients
live in [`residiuum-sdk`](https://crates.io/crates/residiuum-sdk). Wire framing is MIT
[`residiuum-client`](https://crates.io/crates/residiuum-client).

## When to use this crate

| You want… | Use |
|-----------|-----|
| Embedded local store, no network | [`residiuum-sdk`](https://crates.io/crates/residiuum-sdk) only |
| CLI (`residiuum serve`, `residiuum doctor`, …) | [`residiuum-cli`](https://crates.io/crates/residiuum-cli) |
| Programmatic TCP serve from Rust | **`residiuum-server`** (this crate) |
| Wire framing / handshake only | [`residiuum-client`](https://crates.io/crates/residiuum-client) |

## Install

```toml
[dependencies]
residiuum-server = "0.2.3"
residiuum-store = "0.2.5"   # open/create the store path you serve
```

Or: `cargo add residiuum-server`

`0.2.3` serves the `residiuum-sdk 0.4` Full-RQL response shape: attached rows
are sent once, while pre-attach cardinality is reported as `base_row_count`.
The former duplicate `base_rows` payload is no longer emitted.

> **License note:** This crate is AGPL-3.0-or-later. Network use of a modified
> version triggers the AGPL source-offer obligation. Prefer MIT
> `residiuum-client` + MPL `residiuum-sdk` (embedded) if you only need a local store.

## Quick example (product path — HeapKey)

**HAR-4:** product serve is the **qualified HeapKey** listener.  
`ServeOptions::new()` / `default()` sets `qualified_heap_key=true`. That path
requires TLS 1.3, a resident heap registry, and a deployment id. Clients use
`Residiuum::connect_heap` — not a shared token.

Single-node serve on loopback (shape; registry/credential from authority install):

```rust
use residiuum_server::{serve_store_with, ServeOptions};
use residiuum_sdk::TlsServerOptions;
use residiuum_store::Store;
use std::path::Path;
use std::sync::Arc;

fn run(
    store_path: &Path,
    cert: &Path,
    key: &Path,
    deployment_id: &str,
    registry: Arc<residiuum_server::ResidentHeapRegistry>,
) -> Result<(), residiuum_sdk::Error> {
    let _ = Store::create(store_path)?;

    // Product default: qualified_heap_key=true (HAR-4).
    let opts = ServeOptions::new()
        .tls(TlsServerOptions::new(cert, key))
        .deployment_id(deployment_id)
        .heap_registry(registry);

    // Blocks the current thread in the accept loop.
    serve_store_with(store_path, "127.0.0.1:7434", opts)
}
```

Product clients connect with HeapKey + TLS:

```rust
use residiuum_sdk::{
    HeapCredential, RemoteHeapOptions, Residiuum, TlsClientOptions,
};

// certificate_cose + HolderSigner from local authority (HAR-2/HAR-3).
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
# let _ = heap;
# Ok::<(), residiuum_sdk::Error>(())
```

Journey evidence:  
[HAR4_T4_CONNECT_HEAP_JOURNEY.md](../../doc/todo/heap-application-ready/HAR4_T4_CONNECT_HEAP_JOURNEY.md).

### Appendix — legacy open/token (non-product)

Stage-7 diagnostics and shared-token tests must opt in explicitly.
**Do not** co-host with qualified HeapKey on one process.

```rust
use residiuum_server::{serve_store_with, ServeOptions};
use residiuum_sdk::{ConnectOptions, Residiuum};
use residiuum_store::Store;
use std::path::Path;

fn run_legacy(store_path: &Path) -> Result<(), residiuum_sdk::Error> {
    let _ = Store::create(store_path)?;
    let opts = ServeOptions::new()
        .legacy_token_server() // HAR-4: explicit non-product path
        .auth_token("SECRET");
    serve_store_with(store_path, "127.0.0.1:7434", opts)
}

// Legacy client (token path — not product remote):
let mut db = Residiuum::connect_with(
    "residiuum://127.0.0.1:7434/app",
    ConnectOptions::new().auth_token("SECRET"),
)?;
# let _ = db;
# Ok::<(), residiuum_sdk::Error>(())
```

## API surface

| API | Role |
|-----|------|
| `serve_store` / `serve_store_with` | Single-node TCP serve over a store directory |
| `serve_cluster_node` | Multi-node node process (experimental network cluster) |
| `ServeOptions` | Auth token, TLS, bind policy, admission, Raft, logger, metrics, shutdown |
| `handle_connection` / `handle_connection_with` | Per-connection RPC dispatch |
| `ServerRuntime` / `ServerLimits` | Connection caps, idle timeout, drain |
| `AdmissionController` / `AdmissionLimits` | Rate limits, auth lockout, replay window |
| `AuthzPolicy` / `Principal` / `Privilege` | Principal privileges + audit chain |
| `validate_bind` / `ServeStartupReport` | Loopback-default bind policy (DEF-002) |
| `load_and_validate` / `ResidiuumConfigFile` / `ValidatedConfig` | Versioned process config (DEF-054, `residiuum-config-v1`) |
| `Logger` / `LogEvent` / `MemorySink` / `log_rpc_complete` | Structured NDJSON process logs (DEF-060, `residiuum-log-v1`) |
| `MetricsRegistry` / `HealthReport` / `evaluate_health` | Process metrics + health probes (DEF-061) |
| `RaftServerState` / `TcpRaftTransport` | Network Raft RPC (vote / append / snapshot / read-index) |

## Process configuration (DEF-054)

```rust
use residiuum_server::{load_and_validate, ConfigMode, ConfigOverrides, ServeOptions};
use std::path::Path;

let validated = load_and_validate(
    Some(Path::new("residiuum.json")),
    ConfigMode::Serve,
    ConfigOverrides {
        store_path: Some(Path::new("/data/store").into()),
        ..Default::default()
    },
)?;
// Secrets never appear in effective reports:
let report = validated.effective_report(ConfigMode::Serve);
let opts: ServeOptions = validated.apply_to_serve_options(ServeOptions::new());
# Ok::<(), residiuum_server::ConfigError>(())
```

JSON documents use profile `residiuum-config-v1`. Put only secret *references*
(`serve.token_env`, `serve.token_secret_ref` as `env:NAME` or `file:PATH`) in
the file — never token values. Validation refuses unsafe combinations such as
`cluster.claim_replication=true` with fewer than three expected nodes.

## Structured logging (DEF-060)

Serve paths emit versioned NDJSON (`profile: residiuum-log-v1`) on stderr by
default. Events use stable names (`rpc.complete`, `guarantee.failed`,
`connection.rejected`, …) and bounded correlation fields (`request_id`,
`operation_id`, `principal_id`, requested/achieved guarantees, latency).
Credentials and request/response bodies are never logged.

```rust
use residiuum_server::{log_events, Logger, MemorySink, ServeOptions};
use std::sync::Arc;

// Tests: capture lines without touching stderr.
let sink = Arc::new(MemorySink::new(64));
let log = Logger::with_sink(sink).store("/data/store").mode("serve").shared();
let opts = ServeOptions::new().logger(log);
// Production: omit `.logger(...)` — serve installs stderr NDJSON automatically.
# let _ = opts;
```

## Metrics and health (DEF-061)

Serve installs an in-process `MetricsRegistry` (`profile: residiuum-metrics-v1`)
and answers:

| RPC | Auth | Role |
|-----|------|------|
| `health_live` | public | Liveness — process is handling RPCs |
| `health_ready` | public | Readiness — fails when draining, store unavailable, or replication claimed without Raft |
| `health` | Read | Detailed operator health (`residiuum-health-v1`) |
| `metrics` | Admin | Bounded scrape: per-op counters + latency histograms, guarantee/admission edges |

Op labels are a fixed known set plus `other` (no collection/key/token labels).
Public probes work without a token even when the data plane requires auth.

```rust
use residiuum_server::{MetricsRegistry, ServeOptions};
use std::sync::Arc;

let metrics = MetricsRegistry::new().shared();
let opts = ServeOptions::new().metrics(Arc::clone(&metrics));
// Production: omit `.metrics(...)` — serve installs a default registry.
# let _ = opts;
```

## Serve options (highlights)

```rust
use residiuum_server::ServeOptions;
use residiuum_sdk::TlsServerOptions;

// Product: qualified HeapKey (default). Requires TLS + deployment_id + registry.
let opts = ServeOptions::new()
    .tls(TlsServerOptions::new(cert_path, key_path))
    .deployment_id("00000000-0000-4000-8000-000000000001")
    // .heap_registry(registry)
    .allow_insecure_bind(false) // refuse non-loopback plaintext
    .experimental_network_cluster(false);

// Non-product Stage-7 open/token: chain legacy_token_server() first.
// let opts = ServeOptions::new().legacy_token_server().auth_token("SECRET");
```

**Auth path (HAR-4):** product = `qualified_heap_key` (default on `ServeOptions`).
Legacy open/token = `legacy_token_server()`. Co-host is refuse-closed. Config
keys: `serve.qualified_heap_key`, `serve.legacy_token_server`,
`serve.deployment_id`.

**Bind policy:** plaintext binds default to loopback. Non-loopback plaintext
requires `allow_insecure_bind(true)`. Prefer TLS (`ServeOptions::tls`) for
public binds.

**`serve_cluster_node`:** experimental multi-process cluster serve. Requires
`experimental_network_cluster(true)`. When Raft attaches, data-plane put/delete
use partition propose (DEF-037) and control-plane `raft_*` RPCs (DEF-036).
Directory-only fallback if attach fails. Not production-ready; durable
rebalance, repair, and Jepsen gates remain open (DEF-038+).

## Related crates

| Crate | License | Role |
|-------|---------|------|
| [`residiuum-sdk`](https://crates.io/crates/residiuum-sdk) | MPL-2.0 | Collection + remote client API |
| [`residiuum-client`](https://crates.io/crates/residiuum-client) | MIT | Framed RPC + handshake |
| [`residiuum-cluster`](https://crates.io/crates/residiuum-cluster) | AGPL-3.0-or-later | Partitions, Raft, rebalance |
| [`residiuum-cli`](https://crates.io/crates/residiuum-cli) | AGPL-3.0-or-later | `residiuum serve` binary |

## License

AGPL-3.0-or-later.

Part of [Residiuum](https://github.com/frogfishio/dingodb). Multi-tier license map:
[doc/reference/operations/LICENSING.md](https://github.com/frogfishio/dingodb/blob/main/doc/reference/operations/LICENSING.md).
