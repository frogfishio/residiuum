//! TCP serve path: accept loop, RPC dispatch, cluster node serve (AGPL).
//!
//! Extracted from the former monolithic `residiuum-sdk` remote module.

use crate::admission::{
    is_replayable_mutation, AdmissionController, AdmissionLimits, ADMISSION_PROFILE,
};
use crate::authz::{check_rpc, AuditLog, AuthzPolicy, FORCE_RECONFIG_CONFIRM, PURGE_CONFIRM};
use crate::bind_policy;
use crate::heap_session::{
    run_qualified_handshake_buffered, serve_qualified_requests_with_host, QualifiedHandshakeParams,
    QualifiedHandshakeResult,
};
use crate::metrics::{
    evaluate_health, is_public_probe_op, HealthEvalInput, MetricsRegistry, HEALTH_PROFILE,
    METRICS_PROFILE,
};
use crate::raft_server;
use crate::runtime::{
    is_mutating_op, ConnectionGuard, MutationGuard, ServerLimits, ServerRuntime, SERVER_PROFILE,
};
use crate::slog::{events as log_events, log_rpc_complete, Logger, LOG_PROFILE};
use residiuum_cluster::{DEFAULT_VIRTUAL_PARTITIONS, HASH_PROFILE_BLAKE3_MOD};
use residiuum_sdk::{
    collection_prefix, create_index_on_store, decode_bytes, decode_json, decode_subject,
    encode_bytes, encode_json, encode_subject, find_on_store, mark_indexes_stale,
    read_frame_or_detect_legacy, validate_collection_name, validate_key, write_json_frame,
    write_reject_frame, AssignmentWire, DirectorySnapshot, Error, ExtentRow, Filter,
    HistoryVersionRow, IndexInfo, IndexInfoRow, IoStream, KeyHistory, NegotiatedSession,
    PresentChunkRow, QueryBudget, QueryOptions, RpcRequest, RpcResponse, ScanRow, SortOrder,
    TlsServerOptions, TlsServerState, PROTOCOL_PROFILE, TLS_PROFILE,
};
use residiuum_store::{DurabilityMode, PayloadResult, Store, WriteReceipt as StoreWriteReceipt};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::str;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Server options for `residiuum serve` / [`serve_store_with`] / cluster node serve.
#[derive(Debug, Clone)]
pub struct ServeOptions {
    /// When set, every RPC must carry a matching `token` field.
    pub auth_token: Option<String>,
    /// When set, `directory` RPC returns this snapshot (network multi-node serve).
    ///
    /// Single-node `residiuum serve` leaves this unset and synthesizes an all-local
    /// directory. Cluster nodes (`residiuum serve-cluster`) pass real placement +
    /// `endpoints.json` so clients can cache routes (CLUSTER_SPEC §13).
    ///
    /// When [`Self::cluster_root`] is also set, each `directory` RPC reloads
    /// `endpoints.json` so late-joining nodes appear without restarting peers.
    pub directory: Option<DirectorySnapshot>,
    /// Dense node index this process represents (informational; for logs/tests).
    pub node_index: Option<u32>,
    /// Cluster root directory for live `endpoints.json` reload on `directory` RPC.
    pub cluster_root: Option<std::path::PathBuf>,
    /// Allow non-loopback plaintext binds (development only; DEF-002).
    ///
    /// Defaults to `false`. Public binds without TLS require this opt-in
    /// (CLI: `--allow-insecure-bind`). Prefer [`Self::tls`] for non-loopback.
    pub allow_insecure_bind: bool,
    /// Acknowledge that network `serve-cluster` is experimental (DEF-002).
    ///
    /// Required by [`serve_cluster_node`]. When Raft state is attached (default),
    /// data-plane put/delete use quorum commit (DEF-037); without Raft the node
    /// remains a routing/advertise prototype only.
    pub experimental_network_cluster: bool,
    /// Skip the structured stderr startup report (when the CLI already printed it).
    pub suppress_startup_report: bool,
    /// Connection admission, idle timeout, and drain bounds (DEF-030).
    pub server_limits: ServerLimits,
    /// Optional external shutdown flag. When set true, the accept loop stops
    /// admitting work and drains in-flight connections (DEF-030).
    pub shutdown: Option<Arc<AtomicBool>>,
    /// Accept legacy newline-delimited JSON without handshake (DEF-031 diagnostic).
    ///
    /// Defaults to `false`. Production servers require the framed
    /// `residiuum-rpc-v1` hello/welcome exchange. Enable only for local debugging.
    pub diagnostic_line_protocol: bool,
    /// Optional TLS server configuration (DEF-032).
    ///
    /// When set, every accepted connection is wrapped in TLS 1.3 before the
    /// framed RPC handshake. Non-loopback binds without TLS still require
    /// [`Self::allow_insecure_bind`].
    pub tls: Option<TlsServerOptions>,
    /// Optional out-parameter filled with the live [`TlsServerState`] so tests
    /// and operators can call [`TlsServerState::reload`] without restarting.
    ///
    /// Not set by CLI defaults; library callers may provide
    /// `Arc::new(Mutex::new(None))` and read it after serve starts.
    pub tls_state_slot: Option<Arc<Mutex<Option<TlsServerState>>>>,
    /// Authorization policy (DEF-033). When unset and [`Self::auth_token`] is
    /// set, a single superuser principal is synthesized from that token. When
    /// both are unset, the server runs in open mode (anonymous superuser).
    pub authz: Option<AuthzPolicy>,
    /// Optional audit log for security- and recovery-sensitive actions (DEF-033).
    pub audit: Option<Arc<AuditLog>>,
    /// Protocol admission limits (DEF-034): rate, auth lockout, connect churn,
    /// expensive-op concurrency, operation-id replay window.
    pub admission_limits: AdmissionLimits,
    /// Optional shared admission controller. When unset, [`serve_store_with`]
    /// builds one from [`Self::admission_limits`]. Tests may inject a prebuilt
    /// controller to observe counters.
    pub admission: Option<Arc<AdmissionController>>,
    /// Network Raft state for this process (DEF-036 control plane + DEF-037 data plane).
    ///
    /// When set:
    /// - Control plane: `raft_request_vote` / `raft_append_entries` /
    ///   `raft_install_snapshot` / `raft_read_index`
    /// - Data plane: collection put/delete/put_bytes propose through Raft and
    ///   return `committed=true` only after quorum + local apply
    ///
    /// Endpoint maps remain routing hints only — never write authority.
    pub raft: Option<raft_server::SharedRaftState>,
    /// Structured process logger (DEF-060). When unset, serve installs a default
    /// stderr NDJSON logger with store/mode context.
    pub logger: Option<Arc<Logger>>,
    /// Process metrics registry (DEF-061). When unset, serve installs a default
    /// in-process registry shared across connections.
    pub metrics: Option<Arc<MetricsRegistry>>,
    /// Shared server runtime for health/metrics RPCs (filled by serve loops).
    ///
    /// Tests may inject a prebuilt runtime; production `serve_store_with` sets
    /// this after constructing the accept-loop runtime.
    pub runtime: Option<Arc<ServerRuntime>>,
    /// When true, connections use the qualified heap-key-v1 sequence (§33.2).
    ///
    /// **HAR-4 T2 product default is `true`.** Requires TLS, a resident heap
    /// registry, and `deployment_id`. Forbids shared `auth_token` and diagnostic
    /// line protocol. Use [`Self::legacy_token_server`] for the non-product
    /// open/token Stage-7 path.
    pub qualified_heap_key: bool,
    /// Explicit non-product open/token path (HAR-4 `--legacy-token-server`).
    ///
    /// Mutually exclusive with [`Self::qualified_heap_key`]. When true, startup
    /// reports label the listener as non-qualified.
    pub legacy_token_server: bool,
    /// Resident heap slots for qualified auth (HP-008).
    pub heap_registry: Option<Arc<crate::ResidentHeapRegistry>>,
    /// Canonical deployment UUID for heap challenges.
    pub deployment_id: Option<String>,
    /// Optional heap-auth audit sink (never on the wire).
    pub heap_auth_audit: Option<Arc<crate::HeapAuthAuditLog>>,
    /// Override trusted unix seconds for HeapKey windows (tests). When unset,
    /// the accept path uses wall-clock unix time.
    pub security_now_unix_s: Option<u64>,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            auth_token: None,
            directory: None,
            node_index: None,
            cluster_root: None,
            allow_insecure_bind: false,
            experimental_network_cluster: false,
            suppress_startup_report: false,
            server_limits: ServerLimits::draft_defaults(),
            shutdown: None,
            diagnostic_line_protocol: false,
            tls: None,
            tls_state_slot: None,
            authz: None,
            audit: None,
            admission_limits: AdmissionLimits::draft_defaults(),
            admission: None,
            raft: None,
            logger: None,
            metrics: None,
            runtime: None,
            // HAR-4 T2: HeapKey is the product default serve profile.
            qualified_heap_key: true,
            legacy_token_server: false,
            heap_registry: None,
            deployment_id: None,
            heap_auth_audit: None,
            security_now_unix_s: None,
        }
    }
}

impl ServeOptions {
    /// Product defaults: qualified HeapKey listener (HAR-4).
    ///
    /// Callers that need open/token Stage-7 posture must chain
    /// [`Self::legacy_token_server`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Non-product open/token (or shared-token) listener path.
    ///
    /// Explicit opt-in for Stage-7 development and legacy tutorials. Clears
    /// qualified HeapKey mode. Co-host with qualified is forbidden at serve
    /// start.
    pub fn legacy_token_server(mut self) -> Self {
        self.qualified_heap_key = false;
        self.legacy_token_server = true;
        self
    }

    /// Require this shared token on every request.
    ///
    /// Incompatible with qualified HeapKey (fail-closed at serve start). Prefer
    /// chaining after [`Self::legacy_token_server`].
    pub fn auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Set an explicit multi-principal authorization policy (DEF-033).
    ///
    /// Overrides the single-token superuser synthesis when present. Prefer this
    /// when writers must not hold admin / salvage / purge rights.
    pub fn authz(mut self, policy: AuthzPolicy) -> Self {
        self.authz = Some(policy);
        self
    }

    /// Attach a tamper-evident audit log (DEF-033).
    pub fn audit(mut self, log: Arc<AuditLog>) -> Self {
        self.audit = Some(log);
        self
    }

    /// Set protocol admission limits (DEF-034).
    pub fn admission_limits(mut self, limits: AdmissionLimits) -> Self {
        self.admission_limits = limits;
        self
    }

    /// Attach network Raft control-plane state (DEF-036).
    pub fn raft(mut self, state: raft_server::SharedRaftState) -> Self {
        self.raft = Some(state);
        self
    }

    /// Inject a shared admission controller (DEF-034 tests / multi-listener).
    pub fn admission(mut self, controller: Arc<AdmissionController>) -> Self {
        self.admission = Some(controller);
        self
    }

    /// Attach a structured logger (DEF-060). Prefer [`Logger::with_sink`] in tests.
    pub fn logger(mut self, logger: Arc<Logger>) -> Self {
        self.logger = Some(logger);
        self
    }

    /// Attach a metrics registry (DEF-061). Prefer a shared [`MetricsRegistry`] in tests.
    pub fn metrics(mut self, metrics: Arc<MetricsRegistry>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Attach a server runtime for health/metrics observation (DEF-061).
    pub fn runtime(mut self, runtime: Arc<ServerRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Enable or disable the qualified heap-key-v1 listener (`HEAP_SPEC` §33).
    ///
    /// When `enabled` is true, clears [`Self::legacy_token_server`]. When false,
    /// does **not** automatically set legacy mode — call
    /// [`Self::legacy_token_server`] for an explicit non-product path.
    pub fn qualified_heap_key(mut self, enabled: bool) -> Self {
        self.qualified_heap_key = enabled;
        if enabled {
            self.legacy_token_server = false;
        }
        self
    }

    /// Install the resident heap registry used by qualified auth.
    pub fn heap_registry(mut self, registry: Arc<crate::ResidentHeapRegistry>) -> Self {
        self.heap_registry = Some(registry);
        self
    }

    /// Set the deployment id echoed in heap challenges.
    pub fn deployment_id(mut self, id: impl Into<String>) -> Self {
        self.deployment_id = Some(id.into());
        self
    }

    /// Attach a heap-auth audit sink (HP-008 diagnostics).
    pub fn heap_auth_audit(mut self, audit: Arc<crate::HeapAuthAuditLog>) -> Self {
        self.heap_auth_audit = Some(audit);
        self
    }

    /// Override security time for HeapKey certificate windows (tests).
    pub fn security_now_unix_s(mut self, unix_s: u64) -> Self {
        self.security_now_unix_s = Some(unix_s);
        self
    }

    /// Resolve the effective authorization policy for this serve configuration.
    pub fn effective_authz(&self) -> AuthzPolicy {
        if let Some(p) = self.authz.clone() {
            return p;
        }
        if let Some(token) = self.auth_token.as_ref() {
            return AuthzPolicy::shared_superuser(token.clone());
        }
        AuthzPolicy::new()
    }

    /// Resolve the process-wide admission controller (build from limits if needed).
    pub fn effective_admission(&self) -> Arc<AdmissionController> {
        if let Some(c) = self.admission.as_ref() {
            return Arc::clone(c);
        }
        AdmissionController::new(self.admission_limits.clone())
    }

    /// Resolve the process logger (default stderr NDJSON when unset).
    pub fn effective_logger(&self) -> Arc<Logger> {
        if let Some(l) = self.logger.as_ref() {
            return Arc::clone(l);
        }
        Logger::stderr().shared()
    }

    /// Resolve the process metrics registry (default empty registry when unset).
    pub fn effective_metrics(&self) -> Arc<MetricsRegistry> {
        if let Some(m) = self.metrics.as_ref() {
            return Arc::clone(m);
        }
        MetricsRegistry::new().shared()
    }

    /// Advertise a cluster placement directory on `directory` RPC.
    pub fn directory(mut self, snapshot: DirectorySnapshot) -> Self {
        self.directory = Some(snapshot);
        self
    }

    /// Record which cluster node index this process is serving.
    pub fn node_index(mut self, index: u32) -> Self {
        self.node_index = Some(index);
        self
    }

    /// Reload `endpoints.json` from this cluster root on every `directory` RPC.
    pub fn cluster_root(mut self, root: impl Into<std::path::PathBuf>) -> Self {
        self.cluster_root = Some(root.into());
        self
    }

    /// Allow binding to a non-loopback address without TLS (development only).
    pub fn allow_insecure_bind(mut self, allow: bool) -> Self {
        self.allow_insecure_bind = allow;
        self
    }

    /// Opt into experimental network cluster serve (Raft control + data plane
    /// when attach succeeds; directory-only routing if attach fails).
    pub fn experimental_network_cluster(mut self, enabled: bool) -> Self {
        self.experimental_network_cluster = enabled;
        self
    }

    /// Do not print the structured serve startup report (caller already did).
    pub fn suppress_startup_report(mut self, suppress: bool) -> Self {
        self.suppress_startup_report = suppress;
        self
    }

    /// Set connection / drain limits for the bounded server (DEF-030).
    pub fn server_limits(mut self, limits: ServerLimits) -> Self {
        self.server_limits = limits;
        self
    }

    /// Maximum simultaneous client connections (shorthand for server limits).
    pub fn max_connections(mut self, n: usize) -> Self {
        self.server_limits.max_connections = n.max(1);
        self
    }

    /// Idle socket timeout for established connections.
    pub fn idle_timeout(mut self, d: Duration) -> Self {
        self.server_limits.idle_timeout = d;
        self
    }

    /// How long graceful shutdown waits for in-flight connections.
    pub fn drain_timeout(mut self, d: Duration) -> Self {
        self.server_limits.drain_timeout = d;
        self
    }

    /// External shutdown signal shared with the accept loop (DEF-030).
    pub fn shutdown_flag(mut self, flag: Arc<AtomicBool>) -> Self {
        self.shutdown = Some(flag);
        self
    }

    /// Enable legacy line-delimited JSON (diagnostic / `nc` debugging only).
    pub fn diagnostic_line_protocol(mut self, enabled: bool) -> Self {
        self.diagnostic_line_protocol = enabled;
        self
    }

    /// Enable TLS 1.3 with the given server options (DEF-032).
    pub fn tls(mut self, options: TlsServerOptions) -> Self {
        self.tls = Some(options);
        self
    }

    /// Whether TLS is configured for this serve process.
    pub fn tls_enabled(&self) -> bool {
        self.tls.is_some()
    }

    /// Publish the live TLS server state into this slot after material loads.
    pub fn tls_state_slot(mut self, slot: Arc<Mutex<Option<TlsServerState>>>) -> Self {
        self.tls_state_slot = Some(slot);
        self
    }
}

/// Outcome of client operation_id lookup before a mutation (DEF-010).
enum DedupOutcome {
    /// Exact retry — return the original receipt without writing again.
    Replay(StoreWriteReceipt),
    /// New operation (optional op_id when client omitted it).
    Fresh {
        op_id: Option<[u8; 16]>,
        content_hash: [u8; 32],
    },
}

fn payload_to_resp(id: u64, result: &PayloadResult) -> Result<RpcResponse, Error> {
    match result {
        PayloadResult::Complete { body } => Ok(RpcResponse {
            id,
            ok: true,
            found: Some(true),
            payload_status: Some("complete".into()),
            bytes_b64: Some(b64_encode(body)),
            ..empty_resp(id)
        }),
        PayloadResult::Partial {
            extents,
            missing,
            content_hash,
            present_bodies,
        } => {
            let present_chunks = present_bodies
                .iter()
                .map(|(idx, body)| PresentChunkRow {
                    index: *idx,
                    bytes_b64: b64_encode(body),
                })
                .collect();
            let extent_rows = extents
                .iter()
                .map(|e| ExtentRow {
                    index: e.index,
                    start: e.range.start,
                    end: e.range.end,
                    present: e.present,
                })
                .collect();
            Ok(RpcResponse {
                id,
                ok: true,
                found: Some(true),
                payload_status: Some("partial".into()),
                content_hash_hex: Some(hex32(content_hash)),
                missing: Some(missing.clone()),
                present_chunks: Some(present_chunks),
                extents: Some(extent_rows),
                ..empty_resp(id)
            })
        }
        PayloadResult::Unavailable {
            content_hash,
            total_chunks,
        } => Ok(RpcResponse {
            id,
            ok: true,
            found: Some(true),
            payload_status: Some("unavailable".into()),
            content_hash_hex: Some(hex32(content_hash)),
            total_chunks: Some(*total_chunks),
            ..empty_resp(id)
        }),
        PayloadResult::Conflicting { index } => Ok(RpcResponse {
            id,
            ok: true,
            found: Some(true),
            payload_status: Some("conflicting".into()),
            conflict_index: Some(*index),
            ..empty_resp(id)
        }),
    }
}

fn hex32(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn parse_hex16_strict(s: &str, field: &str) -> Result<[u8; 16], Error> {
    if s.len() != 32 {
        return Err(Error::ProtocolViolation(format!(
            "expected 32 hex chars for `{field}`, got {}",
            s.len()
        )));
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        let byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|e| Error::ProtocolViolation(format!("invalid hex id for `{field}`: {e}")))?;
        out[i] = byte;
    }
    Ok(out)
}

fn fill_receipt_fields(resp: &mut RpcResponse, receipt: &StoreWriteReceipt) {
    resp.event_id = Some(hex16(&receipt.event_id));
    // Public OCC version = establishing event id (APB-2 / DX §6.4).
    resp.version = Some(hex16(&receipt.event_id));
    resp.store_id = Some(hex16(&receipt.store_id));
    resp.segment_id = Some(hex16(&receipt.segment_id));
    resp.acknowledgement = Some(durability_name(receipt.durability).into());
    resp.committed = Some(true);
}

/// Look up a client operation id; reject content-mismatched reuse.
fn prepare_mutation_dedup(
    store: &Store,
    operation_id_hex: Option<&str>,
    op: &str,
    collection: &str,
    key: &str,
    payload: &[u8],
) -> Result<DedupOutcome, Error> {
    let content_hash = residiuum_store::content_identity(op, collection, key, payload);
    let Some(hex) = operation_id_hex else {
        return Ok(DedupOutcome::Fresh {
            op_id: None,
            content_hash,
        });
    };
    let op_id = parse_hex16_strict(hex, "operation_id")?;
    match store.resolve_write_dedup(&op_id, &content_hash)? {
        Some(prior) => Ok(DedupOutcome::Replay(prior)),
        None => Ok(DedupOutcome::Fresh {
            op_id: Some(op_id),
            content_hash,
        }),
    }
}

/// Serve one store over TCP until shutdown or the listener fails (Stage 7 + DEF-030).
///
/// Opens the store **once** (exclusive writer ownership), then admits a bounded
/// number of concurrent connection workers.
///
/// **HAR-4 T2:** uses product defaults ([`ServeOptions::default`] = qualified
/// HeapKey). That requires TLS + registry + deployment_id. For open/token
/// development, call [`serve_store_with`] with
/// [`ServeOptions::legacy_token_server`].
pub fn serve_store(store_path: impl AsRef<Path>, bind: &str) -> Result<(), Error> {
    serve_store_with(store_path, bind, ServeOptions::default())
}

/// Serve with explicit auth / server options.
///
/// Enforces the bind policy (DEF-002 / DEF-032): non-loopback **plaintext**
/// requires [`ServeOptions::allow_insecure_bind`]; non-loopback with TLS is
/// allowed. Emits a structured startup report to stderr that never claims
/// network quorum durability for single-node serve.
///
/// **Bounded server (DEF-030):** one store owner, worker threads per connection,
/// connection admission limits, idle timeouts, overload responses, and optional
/// graceful drain via [`ServeOptions::shutdown_flag`].
///
/// **TLS (DEF-032):** when [`ServeOptions::tls`] is set, every accepted
/// connection is wrapped in TLS 1.3 before the framed RPC handshake. Call
/// [`TlsServerState::reload`] via the shared state to rotate certs without
/// downtime (new handshakes pick up reloaded material).
///
/// **HAR-4 path honesty:** qualified HeapKey and legacy token/open cannot
/// co-host. Product default is qualified; use
/// [`ServeOptions::legacy_token_server`] for the non-product path.
pub fn serve_store_with(
    store_path: impl AsRef<Path>,
    bind: &str,
    options: ServeOptions,
) -> Result<(), Error> {
    let tls_enabled = options.tls.is_some();
    // Co-host prohibition: cannot claim both product HeapKey and legacy path.
    if options.qualified_heap_key && options.legacy_token_server {
        return Err(Error::ValidationMsg(
            "co-host forbidden: qualified HeapKey and --legacy-token-server cannot share one process (HAR-4)"
                .into(),
        ));
    }
    if options.qualified_heap_key {
        crate::validate_qualified_listener(
            tls_enabled,
            options.auth_token.as_deref(),
            options.diagnostic_line_protocol,
            options.heap_registry.as_ref(),
            options.deployment_id.as_deref(),
        )?;
    } else if !options.legacy_token_server {
        // Ambiguous non-qualified path without explicit legacy opt-in.
        return Err(Error::ValidationMsg(
            "serve path ambiguous: set qualified_heap_key (product) or call legacy_token_server() \
             / --legacy-token-server (non-product open/token) (HAR-4 T2)"
                .into(),
        ));
    }
    bind_policy::validate_bind(bind, options.allow_insecure_bind, tls_enabled)?;
    let path = store_path.as_ref().to_path_buf();
    let tls_state = match options.tls.clone() {
        Some(tls_opts) => Some(TlsServerState::from_options(tls_opts)?),
        None => None,
    };
    if let Some(slot) = options.tls_state_slot.as_ref() {
        if let Ok(mut g) = slot.lock() {
            *g = tls_state.clone();
        }
    }
    let mtls = tls_state
        .as_ref()
        .map(|s| s.options().require_client_cert)
        .unwrap_or(false);
    // Install default structured logger with process context when caller omitted one.
    let mut options = options;
    if options.logger.is_none() {
        options.logger = Some(
            Logger::stderr()
                .store(path.display().to_string())
                .mode("serve")
                .shared(),
        );
    }
    if options.metrics.is_none() {
        options.metrics = Some(MetricsRegistry::new().shared());
    }
    let logger = options.effective_logger();

    // Cluster serve prints its own report; single-node prints unless suppressed.
    if !options.suppress_startup_report
        && options.directory.is_none()
        && options.cluster_root.is_none()
    {
        bind_policy::ServeStartupReport::single_node_tls(
            path.display().to_string(),
            bind,
            options.auth_token.is_some(),
            options.allow_insecure_bind,
            tls_enabled,
            mtls,
        )
        .with_auth_path(options.qualified_heap_key, options.legacy_token_server)
        .emit_stderr();
        eprintln!(
            "residiuum serve: profile={SERVER_PROFILE} protocol={PROTOCOL_PROFILE} \
             tls={TLS_PROFILE}/{} admission={ADMISSION_PROFILE} log={LOG_PROFILE} \
             metrics={METRICS_PROFILE} health={HEALTH_PROFILE} \
             max_connections={} idle_timeout_ms={} global_rps={} diagnostic_line={} \
             auth_path={}",
            if tls_enabled { "on" } else { "off" },
            options.server_limits.max_connections,
            options.server_limits.idle_timeout.as_millis(),
            options.admission_limits.global_max_rps,
            options.diagnostic_line_protocol,
            if options.qualified_heap_key {
                "qualified-heap-key (product)"
            } else {
                "legacy-token-server (non-qualified)"
            }
        );
        logger
            .info(log_events::SERVER_START)
            .reason(&format!(
                "bind={bind} tls={} max_connections={} admission={ADMISSION_PROFILE}",
                if tls_enabled { "on" } else { "off" },
                options.server_limits.max_connections
            ))
            .emit();
    }

    // One coordinated store owner for the whole process (DEF-020 + DEF-030).
    let store = Arc::new(Mutex::new(Store::open(&path)?));
    let runtime = ServerRuntime::new(options.server_limits.clone(), options.shutdown.clone());
    // One process-wide admission controller (DEF-034).
    let admission = options.effective_admission();
    options.admission = Some(Arc::clone(&admission));
    // Expose runtime + metrics to health/metrics RPCs (DEF-061).
    options.runtime = Some(Arc::clone(&runtime));
    if options.metrics.is_none() {
        options.metrics = Some(MetricsRegistry::new().shared());
    }
    let listener = TcpListener::bind(bind).map_err(Error::from_io)?;
    // Non-blocking accept so the loop can observe shutdown without a stuck client.
    listener.set_nonblocking(true).map_err(Error::from_io)?;

    serve_accept_loop(listener, store, runtime, options, tls_state)
}

/// Bounded accept loop: admit connections as worker threads until shutdown.
fn serve_accept_loop(
    listener: TcpListener,
    store: Arc<Mutex<Store>>,
    runtime: Arc<ServerRuntime>,
    options: ServeOptions,
    tls_state: Option<TlsServerState>,
) -> Result<(), Error> {
    let logger = options.effective_logger();
    loop {
        if runtime.is_shutdown_requested() {
            break;
        }
        match listener.accept() {
            Ok((stream, _addr)) => {
                // Accepted fds inherit non-blocking from the listener on some
                // platforms; workers use blocking reads with idle timeouts.
                if let Err(e) = stream.set_nonblocking(false) {
                    logger
                        .warn(log_events::CONNECTION_ERROR)
                        .reason(&format!("set_nonblocking(false) failed: {e}"))
                        .emit();
                    continue;
                }
                // DEF-034: bound connection churn before simultaneous-slot admission.
                let admission = options.effective_admission();
                let metrics = options.effective_metrics();
                if !admission.try_admit_connect() {
                    let max = admission.limits().max_connects_per_window;
                    let reason = format!("connection churn limit exceeded (max {max} per window)");
                    metrics.observe_connection(false);
                    logger
                        .warn(log_events::CONNECTION_REJECTED)
                        .error_code(Some("resource_limit"))
                        .reason(&reason)
                        .emit();
                    let _ = reject_connection(stream, tls_state.as_ref(), &reason);
                    continue;
                }
                if !runtime.try_admit_connection() {
                    // Overload / drain: reply once and drop (backpressure).
                    let max = runtime.limits().max_connections;
                    let reason = if runtime.is_draining() {
                        "server draining; connection refused".to_string()
                    } else {
                        format!("connection limit exceeded (max {max})")
                    };
                    metrics.observe_connection(false);
                    logger
                        .warn(log_events::CONNECTION_REJECTED)
                        .error_code(Some("resource_limit"))
                        .reason(&reason)
                        .emit();
                    let _ = reject_connection(stream, tls_state.as_ref(), &reason);
                    continue;
                }
                metrics.observe_connection(true);
                let guard = ConnectionGuard::new(Arc::clone(&runtime));
                let store_c = Arc::clone(&store);
                let opts_c = options.clone();
                let runtime_c = Arc::clone(&runtime);
                let tls_c = tls_state.clone();
                let logger_c = Arc::clone(&logger);
                // Detach worker: accept loop must not wait on client I/O.
                // On spawn failure the ConnectionGuard drops here and releases the slot.
                thread::Builder::new()
                    .name("residiuum-serve-conn".into())
                    .spawn(move || {
                        let _guard = guard;
                        if let Err(e) =
                            handle_connection_shared(store_c, stream, opts_c, runtime_c, tls_c)
                        {
                            // Avoid logging secrets (tokens never appear in Error Display
                            // for auth failures; still keep messages short).
                            logger_c
                                .error(log_events::CONNECTION_ERROR)
                                .reason(&e.to_string())
                                .emit();
                        }
                    })
                    .map_err(Error::from_io)?;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(Error::from_io(e)),
        }
    }

    // Graceful drain: stop new admissions, wait for workers, report mutation outcome.
    runtime.begin_drain();
    let idle = runtime.wait_for_idle();
    let stats = runtime.stats();
    let drain_status = if idle { "complete" } else { "timeout" };
    eprintln!(
        "residiuum serve: drain {drain_status} active={} peak={} rejected={} accepted={} \
         mutations_started={} mutations_finished={}",
        stats.active_connections,
        stats.peak_connections,
        stats.rejected_connections,
        stats.accepted_connections,
        stats.mutations_started,
        stats.mutations_finished
    );
    logger
        .info(log_events::SERVER_DRAIN)
        .reason(&format!(
            "status={drain_status} active={} peak={} rejected={} accepted={} \
             mutations_started={} mutations_finished={}",
            stats.active_connections,
            stats.peak_connections,
            stats.rejected_connections,
            stats.accepted_connections,
            stats.mutations_started,
            stats.mutations_finished
        ))
        .ok(idle)
        .emit();
    if !idle {
        return Err(Error::ResourceLimit(format!(
            "graceful drain timed out with {} connection(s) still active; \
             mutations_started={} mutations_finished={}",
            stats.active_connections, stats.mutations_started, stats.mutations_finished
        )));
    }
    if stats.mutations_started != stats.mutations_finished {
        return Err(Error::Internal(format!(
            "drain accounting mismatch: mutations_started={} mutations_finished={}",
            stats.mutations_started, stats.mutations_finished
        )));
    }
    Ok(())
}

/// Write an unsolicited overload/drain framed reject and close the socket.
///
/// Emitted before the worker admits the connection (and before application
/// RPCs). Framed clients parse this as a handshake reject with
/// `code=resource_limit`. When TLS is configured the reject is sent after a
/// short TLS handshake so framed clients still observe a clean reject.
fn reject_connection(
    stream: TcpStream,
    tls: Option<&TlsServerState>,
    reason: &str,
) -> Result<(), Error> {
    let mut stream = wrap_server_stream(stream, tls)?;
    let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
    let _ = write_reject_frame(&mut stream, "resource_limit", reason);
    Ok(())
}

/// Wrap an accepted TCP stream with TLS when server TLS is configured.
fn wrap_server_stream(tcp: TcpStream, tls: Option<&TlsServerState>) -> Result<IoStream, Error> {
    match tls {
        Some(state) => {
            let (stream, _peer) = state.accept(tcp)?;
            Ok(stream)
        }
        None => Ok(IoStream::plain(tcp)),
    }
}

/// Qualified heap-key path: TLS exporter → challenge/auth → HeapCap request loop.
///
/// Token/RBAC is not consulted (HP-008). The shared store owner is reused via
/// [`residiuum_store::StoreHost`] so §32.4 data ops stay capability-gated.
fn qualified_connection_shared(
    stream: IoStream,
    options: &ServeOptions,
    store: &std::sync::Arc<std::sync::Mutex<residiuum_store::Store>>,
    store_path: &std::path::Path,
) -> Result<(), Error> {
    let tls_exporter = stream.export_channel_binding()?;
    let mut reader = BufReader::new(stream);
    let registry = options
        .heap_registry
        .as_ref()
        .ok_or_else(|| Error::ValidationMsg("qualified listener missing heap_registry".into()))?;
    let deployment_id = options
        .deployment_id
        .as_deref()
        .ok_or_else(|| Error::ValidationMsg("qualified listener missing deployment_id".into()))?;
    let now_unix_s = options.security_now_unix_s.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    });
    let params = QualifiedHandshakeParams {
        registry,
        deployment_id,
        tls_exporter: &tls_exporter,
        now_unix_s,
        audit: options.heap_auth_audit.as_deref(),
        server_nonce: None,
    };
    let host = residiuum_store::StoreHost::from_shared(std::sync::Arc::clone(store), store_path);
    match run_qualified_handshake_buffered(&mut reader, params)? {
        QualifiedHandshakeResult::Established(session) => {
            serve_qualified_requests_with_host(&mut reader, &session, Some(&host))
        }
        QualifiedHandshakeResult::Rejected { .. } => Ok(()),
    }
}

/// Server handshake over a buffered duplex stream (single ownership).
fn server_handshake_buffered(reader: &mut BufReader<IoStream>) -> Result<NegotiatedSession, Error> {
    // server_handshake needs separate Read/Write; use BufReader + get_mut.
    // Temporarily implement inline using the same logic as residiuum_sdk::server_handshake.
    let payload =
        match read_frame_or_detect_legacy(reader, residiuum_sdk::HANDSHAKE_MAX_FRAME_BYTES)? {
            Some(p) => p,
            None => {
                return Err(Error::ProtocolViolation(
                    "connection closed before protocol hello".into(),
                ));
            }
        };
    let hello = match residiuum_sdk::parse_handshake(&payload) {
        Ok(h) => h,
        Err(e) => {
            let _ = write_json_frame(
                reader.get_mut(),
                &residiuum_sdk::Handshake::reject("protocol_violation", e.to_string()),
            );
            return Err(e);
        }
    };
    if hello.msg != residiuum_sdk::HandshakeMsg::Hello {
        let msg = format!("expected hello, got {:?}", hello.msg);
        let _ = write_json_frame(
            reader.get_mut(),
            &residiuum_sdk::Handshake::reject("protocol_violation", &msg),
        );
        return Err(Error::ProtocolViolation(msg));
    }
    if hello.v != residiuum_sdk::PROTOCOL_MAJOR
        && hello.protocol_major.unwrap_or(hello.v) != residiuum_sdk::PROTOCOL_MAJOR
    {
        let msg = format!(
            "unsupported protocol major {} (server speaks {})",
            hello.v,
            residiuum_sdk::PROTOCOL_MAJOR
        );
        let _ = write_json_frame(
            reader.get_mut(),
            &residiuum_sdk::Handshake::reject("protocol_version_unsupported", &msg),
        );
        return Err(Error::ProtocolViolation(msg));
    }
    let features = match residiuum_sdk::negotiate_features(hello.features.as_deref().unwrap_or(&[]))
    {
        Ok(f) => f,
        Err(e) => {
            let _ = write_json_frame(
                reader.get_mut(),
                &residiuum_sdk::Handshake::reject("protocol_violation", e.to_string()),
            );
            return Err(e);
        }
    };
    let max_frame = residiuum_sdk::negotiate_max_frame(hello.max_frame);
    write_json_frame(
        reader.get_mut(),
        &residiuum_sdk::Handshake::welcome(max_frame, features.clone()),
    )?;
    Ok(NegotiatedSession {
        max_frame,
        features,
        protocol_major: residiuum_sdk::PROTOCOL_MAJOR,
        protocol_minor: residiuum_sdk::PROTOCOL_MINOR,
    })
}

/// Serve one node of a multi-node cluster root over TCP (network follow-on).
///
/// Opens `cluster_root/nodes/node-{node_index}`, upserts `bind` into
/// `endpoints.json`, and advertises the live [`PartitionDirectory`] plus
/// endpoints on every `directory` RPC so multi-seed clients can cache routes.
///
/// When Raft state is attached (default for experimental serve-cluster),
/// control-plane `raft_*` RPCs (DEF-036) and data-plane collection put/delete
/// via Raft propose (DEF-037) are served from durable peer stores under
/// `{cluster_root}/raft/`. Acks report `committed=true` only after quorum.
/// In-process quorum remains available via `Residiuum::open_cluster`.
///
/// **Experimental (DEF-002):** requires
/// [`ServeOptions::experimental_network_cluster`].
pub fn serve_cluster_node(
    cluster_root: impl AsRef<Path>,
    node_index: u32,
    bind: &str,
    options: ServeOptions,
) -> Result<(), Error> {
    if !options.experimental_network_cluster {
        return Err(Error::ValidationMsg(
            "serve-cluster is experimental: pass ServeOptions::experimental_network_cluster(true) \
             or CLI --experimental-network-cluster. When Raft attaches, data-plane writes use \
             quorum commit (DEF-037); control-plane raft_* RPCs are DEF-036."
                .into(),
        ));
    }
    let tls_enabled = options.tls.is_some();
    bind_policy::validate_bind(bind, options.allow_insecure_bind, tls_enabled)?;

    let root = cluster_root.as_ref();
    let meta = residiuum_cluster::ClusterMeta::load(root)
        .map_err(|e| Error::Internal(format!("cluster root {}: {e}", root.display())))?;
    if node_index >= meta.node_count {
        return Err(Error::ValidationMsg(format!(
            "node index {node_index} out of range (cluster has {} nodes)",
            meta.node_count
        )));
    }

    let endpoints = residiuum_cluster::upsert_endpoint(root, node_index, bind)
        .map_err(|e| Error::Internal(format!("endpoints.json: {e}")))?;

    let directory = residiuum_cluster::PartitionDirectory::load(root)
        .map_err(|e| Error::Internal(format!("placement.json: {e}")))?
        .ok_or_else(|| {
            Error::Internal(format!("missing placement.json under {}", root.display()))
        })?;

    let snapshot = DirectorySnapshot::from_directory(&directory, endpoints);
    let mut opts = options
        .directory(snapshot)
        .node_index(node_index)
        .cluster_root(root);

    let store_path = residiuum_cluster::node_store_path(root, node_index);
    if !store_path.join("store-info").is_dir() {
        return Err(Error::Internal(format!(
            "node store missing at {} (expected store-info/)",
            store_path.display()
        )));
    }

    // Install structured logger before Raft attach so attach-failed events carry
    // cluster/store/node context (DEF-060 correlation).
    if opts.logger.is_none() {
        opts.logger = Some(
            Logger::stderr()
                .store(store_path.display().to_string())
                .cluster(root.display().to_string())
                .node_index(node_index)
                .mode("serve-cluster")
                .shared(),
        );
    }
    if opts.metrics.is_none() {
        opts.metrics = Some(MetricsRegistry::new().shared());
    }

    // Attach durable network Raft control plane when the caller did not inject one.
    if opts.raft.is_none() {
        match raft_server::shared_raft_state(root, node_index, opts.auth_token.clone()) {
            Ok(state) => {
                // Seed this node's advertise address into the runtime map.
                if let Ok(mut g) = state.lock() {
                    if let Ok(eps) = residiuum_cluster::load_endpoints(root) {
                        g.set_endpoints(eps);
                    }
                }
                opts = opts.raft(state);
            }
            Err(e) => {
                // Raft attach is best-effort: routing-only serve still works.
                eprintln!(
                    "residiuum serve-cluster: raft control plane not attached: {e} (directory-only mode)"
                );
                opts.effective_logger()
                    .warn(log_events::RAFT_ATTACH_FAILED)
                    .reason(&format!("{e}"))
                    .node_index(node_index)
                    .emit();
            }
        }
    }

    if !opts.suppress_startup_report {
        let mtls = opts
            .tls
            .as_ref()
            .map(|t| t.require_client_cert)
            .unwrap_or(false);
        bind_policy::ServeStartupReport::cluster_node_tls(
            root.display().to_string(),
            bind,
            opts.auth_token.is_some(),
            opts.allow_insecure_bind,
            node_index,
            tls_enabled,
            mtls,
        )
        .emit_stderr();
        eprintln!(
            "residiuum serve-cluster: root={} node={node_index} store={} bind={bind} nodes={} tls={} raft={} log={LOG_PROFILE}",
            root.display(),
            store_path.display(),
            meta.node_count,
            if tls_enabled { "on" } else { "off" },
            if opts.raft.is_some() {
                raft_server::FEATURE_RAFT_RPC_V1
            } else {
                "off"
            }
        );
    }
    // Avoid a second single-node-style report inside serve_store_with.
    let opts = opts.suppress_startup_report(true);
    serve_store_with(store_path, bind, opts)
}

/// Serve a single already-open TCP client (used by tests and `serve_store`).
///
/// Uses the **legacy** open path (HAR-4): exclusive-store helpers cannot host
/// the qualified HeapKey shared-owner sequence. Production multi-client serve
/// goes through [`serve_store_with`] + product defaults.
pub fn handle_connection(store: &mut Store, stream: TcpStream) -> Result<(), Error> {
    handle_connection_with(store, stream, ServeOptions::new().legacy_token_server())
}

/// Handle one client with server options (auth token).
///
/// Single-threaded helper for tests that own a exclusive [`Store`]. Production
/// `serve_store_with` uses [`handle_connection_shared`] so many clients share
/// one store owner under a mutex.
pub fn handle_connection_with(
    store: &mut Store,
    stream: TcpStream,
    options: ServeOptions,
) -> Result<(), Error> {
    // Local single-connection path: no shared runtime accounting.
    let mut options = options;
    let runtime = ServerRuntime::new(options.server_limits.clone(), None);
    // Pretend admitted so drain checks are no-ops.
    let _ = runtime.try_admit_connection();
    let _guard = ConnectionGuard::new(Arc::clone(&runtime));
    options.runtime = Some(Arc::clone(&runtime));
    if options.metrics.is_none() {
        options.metrics = Some(MetricsRegistry::new().shared());
    }
    let tls = match options.tls.clone() {
        Some(o) => Some(TlsServerState::from_options(o)?),
        None => None,
    };
    connection_loop(store, stream, &options, &runtime, tls.as_ref())
}

/// Handle one client against a shared store owner (DEF-030 worker path).
///
/// Holds the store mutex only while dispatching an RPC — never across socket
/// reads/writes — so one slow peer cannot block unrelated clients' store access
/// during network I/O.
pub fn handle_connection_shared(
    store: Arc<Mutex<Store>>,
    stream: TcpStream,
    options: ServeOptions,
    runtime: Arc<ServerRuntime>,
    tls: Option<TlsServerState>,
) -> Result<(), Error> {
    let idle = options.server_limits.idle_timeout;
    let stream = wrap_server_stream(stream, tls.as_ref())?;
    stream
        .set_read_timeout(Some(idle))
        .map_err(Error::from_io)?;
    stream
        .set_write_timeout(Some(idle))
        .map_err(Error::from_io)?;

    if options.qualified_heap_key {
        let store_path = store
            .lock()
            .map_err(|_| Error::Internal("store lock poisoned".into()))?
            .path()
            .to_path_buf();
        return qualified_connection_shared(stream, &options, &store, &store_path);
    }

    let mut reader = BufReader::new(stream);

    let max_frame = if options.diagnostic_line_protocol {
        residiuum_sdk::host_limits().max_rpc_line_bytes
    } else {
        match server_handshake_buffered(&mut reader) {
            Ok(session) => session.max_frame,
            Err(e) => {
                // Handshake already wrote a reject when possible.
                return Err(e);
            }
        }
    };

    loop {
        if runtime.is_draining() {
            // Finish after the current request; refuse further work cleanly.
        }
        let req = match read_rpc_request(&mut reader, max_frame, &options) {
            Ok(Some(r)) => r,
            Ok(None) => break,
            Err(ReadRpc::Idle) => break,
            Err(ReadRpc::Fatal(e)) => return Err(e),
            Err(ReadRpc::Continue) => continue,
            Err(ReadRpc::Close) => break,
        };
        // During drain, refuse application work but still answer public probes so
        // readiness can report not-ready (DEF-061).
        if runtime.is_draining() && !is_public_probe_op(&req.op) {
            write_rpc_response(
                &mut reader,
                &options,
                RpcResponse {
                    id: req.id,
                    ok: false,
                    error: Some("server draining; request refused".into()),
                    code: Some("resource_limit".into()),
                    ..empty_resp(req.id)
                },
            )?;
            break;
        }
        let admission = options.effective_admission();
        let logger = options.effective_logger();
        let metrics = options.effective_metrics();
        let started = Instant::now();
        let gate = admit_and_authorize(&options, &admission, &req);
        let principal_id = match gate {
            Ok(id) => id,
            Err(e) => {
                let resp = RpcResponse {
                    id: req.id,
                    ok: false,
                    error: Some(e.to_string()),
                    code: Some(e.code().as_str().into()),
                    ..empty_resp(req.id)
                };
                log_rpc_from_wire(&logger, &metrics, &req, None, &resp, started.elapsed());
                write_rpc_response(&mut reader, &options, resp)?;
                continue;
            }
        };
        let _expensive = match admission.try_begin_expensive(&req.op) {
            Ok(g) => g,
            Err(e) => {
                let resp = RpcResponse {
                    id: req.id,
                    ok: false,
                    error: Some(e.to_string()),
                    code: Some(e.code().as_str().into()),
                    ..empty_resp(req.id)
                };
                log_rpc_from_wire(
                    &logger,
                    &metrics,
                    &req,
                    Some(principal_id.as_str()),
                    &resp,
                    started.elapsed(),
                );
                write_rpc_response(&mut reader, &options, resp)?;
                continue;
            }
        };
        if is_replayable_mutation(&req.op) {
            if let Some(oid) = req.operation_id.as_deref() {
                if let Err(e) = admission.register_operation_id(&principal_id, oid) {
                    let resp = RpcResponse {
                        id: req.id,
                        ok: false,
                        error: Some(e.to_string()),
                        code: Some(e.code().as_str().into()),
                        ..empty_resp(req.id)
                    };
                    log_rpc_from_wire(
                        &logger,
                        &metrics,
                        &req,
                        Some(principal_id.as_str()),
                        &resp,
                        started.elapsed(),
                    );
                    write_rpc_response(&mut reader, &options, resp)?;
                    continue;
                }
            }
        }
        let resp = {
            let _mutation = if is_mutating_op(&req.op) {
                Some(MutationGuard::new(Arc::clone(&runtime)))
            } else {
                None
            };
            // Serialize store access; release before writing the response.
            let mut guard = store
                .lock()
                .map_err(|_| Error::Internal("store mutex poisoned".into()))?;
            match dispatch(&mut guard, &req, &options) {
                Ok(r) => r,
                Err(e) => RpcResponse {
                    id: req.id,
                    ok: false,
                    error: Some(e.to_string()),
                    code: Some(e.code().as_str().into()),
                    ..empty_resp(req.id)
                },
            }
        };
        log_rpc_from_wire(
            &logger,
            &metrics,
            &req,
            Some(principal_id.as_str()),
            &resp,
            started.elapsed(),
        );
        write_rpc_response(&mut reader, &options, resp)?;
    }
    Ok(())
}

/// Exclusive-store connection loop used by [`handle_connection_with`].
fn connection_loop(
    store: &mut Store,
    stream: TcpStream,
    options: &ServeOptions,
    _runtime: &ServerRuntime,
    tls: Option<&TlsServerState>,
) -> Result<(), Error> {
    let idle = options.server_limits.idle_timeout;
    let stream = wrap_server_stream(stream, tls)?;
    stream
        .set_read_timeout(Some(idle))
        .map_err(Error::from_io)?;
    stream
        .set_write_timeout(Some(idle))
        .map_err(Error::from_io)?;

    if options.qualified_heap_key {
        // Qualified data ops need a shared Arc store owner (StoreHost). The
        // exclusive single-connection test path does not share; use
        // `handle_connection_shared` / `serve_store_with` instead.
        return Err(Error::ValidationMsg(
            "qualified heap-key requires shared store owner (handle_connection_shared)".into(),
        ));
    }

    let mut reader = BufReader::new(stream);
    let admission = options.effective_admission();
    let logger = options.effective_logger();
    let metrics = options.effective_metrics();

    let max_frame = if options.diagnostic_line_protocol {
        residiuum_sdk::host_limits().max_rpc_line_bytes
    } else {
        server_handshake_buffered(&mut reader)?.max_frame
    };

    loop {
        let req = match read_rpc_request(&mut reader, max_frame, options) {
            Ok(Some(r)) => r,
            Ok(None) => break,
            Err(ReadRpc::Idle) => break,
            Err(ReadRpc::Fatal(e)) => return Err(e),
            Err(ReadRpc::Continue) => continue,
            Err(ReadRpc::Close) => break,
        };
        let started = Instant::now();
        let gate = admit_and_authorize(options, &admission, &req);
        let principal_id = match gate {
            Ok(id) => id,
            Err(e) => {
                let resp = RpcResponse {
                    id: req.id,
                    ok: false,
                    error: Some(e.to_string()),
                    code: Some(e.code().as_str().into()),
                    ..empty_resp(req.id)
                };
                log_rpc_from_wire(&logger, &metrics, &req, None, &resp, started.elapsed());
                write_rpc_response(&mut reader, options, resp)?;
                continue;
            }
        };
        let _expensive = match admission.try_begin_expensive(&req.op) {
            Ok(g) => g,
            Err(e) => {
                let resp = RpcResponse {
                    id: req.id,
                    ok: false,
                    error: Some(e.to_string()),
                    code: Some(e.code().as_str().into()),
                    ..empty_resp(req.id)
                };
                log_rpc_from_wire(
                    &logger,
                    &metrics,
                    &req,
                    Some(principal_id.as_str()),
                    &resp,
                    started.elapsed(),
                );
                write_rpc_response(&mut reader, options, resp)?;
                continue;
            }
        };
        if is_replayable_mutation(&req.op) {
            if let Some(oid) = req.operation_id.as_deref() {
                if let Err(e) = admission.register_operation_id(&principal_id, oid) {
                    let resp = RpcResponse {
                        id: req.id,
                        ok: false,
                        error: Some(e.to_string()),
                        code: Some(e.code().as_str().into()),
                        ..empty_resp(req.id)
                    };
                    log_rpc_from_wire(
                        &logger,
                        &metrics,
                        &req,
                        Some(principal_id.as_str()),
                        &resp,
                        started.elapsed(),
                    );
                    write_rpc_response(&mut reader, options, resp)?;
                    continue;
                }
            }
        }
        // Exclusive test path: no process-wide mutation accounting (runtime is
        // local to this connection). Shared workers use MutationGuard.
        let resp = match dispatch(store, &req, options) {
            Ok(r) => r,
            Err(e) => RpcResponse {
                id: req.id,
                ok: false,
                error: Some(e.to_string()),
                code: Some(e.code().as_str().into()),
                ..empty_resp(req.id)
            },
        };
        log_rpc_from_wire(
            &logger,
            &metrics,
            &req,
            Some(principal_id.as_str()),
            &resp,
            started.elapsed(),
        );
        write_rpc_response(&mut reader, options, resp)?;
    }
    Ok(())
}

/// Emit DEF-060 `rpc.complete` (+ optional `guarantee.failed`) and DEF-061 metrics.
fn log_rpc_from_wire(
    logger: &Logger,
    metrics: &MetricsRegistry,
    req: &RpcRequest,
    principal_id: Option<&str>,
    resp: &RpcResponse,
    latency: Duration,
) {
    log_rpc_complete(
        logger,
        &req.op,
        req.id,
        req.operation_id.as_deref(),
        principal_id,
        req.collection.as_deref(),
        resp.ok,
        resp.code.as_deref(),
        latency,
        req.durability.as_deref(),
        resp.acknowledgement.as_deref(),
        resp.committed,
        resp.event_id.as_deref(),
        resp.error.as_deref(),
    );
    metrics.observe_rpc(
        &req.op,
        resp.ok,
        latency,
        req.durability.as_deref(),
        resp.acknowledgement.as_deref(),
        resp.committed,
        resp.code.as_deref(),
    );
}

/// DEF-033 authz + DEF-034 rate/auth-lockout gates. Returns principal id on allow.
///
/// Public probes (`health_live`, `health_ready`) are admitted without a token so
/// orchestrators can probe authenticated servers (DEF-061). Detailed `health`
/// and `metrics` still require normal authentication.
fn admit_and_authorize(
    options: &ServeOptions,
    admission: &AdmissionController,
    req: &RpcRequest,
) -> Result<String, Error> {
    // Public liveness/readiness probes: skip token auth (still rate-limited).
    if is_public_probe_op(&req.op) {
        let probe_principal = "probe";
        admission.admit_rpc(probe_principal)?;
        return Ok(probe_principal.to_string());
    }

    // Bound auth-failure CPU before constant-time token scan.
    admission.check_auth_lockout(req.token.as_deref())?;
    let policy = options.effective_authz();
    let principal = match check_rpc(
        &policy,
        options.audit.as_deref(),
        req.token.as_deref(),
        &req.op,
        req.collection.as_deref(),
        req.confirm.as_deref(),
    ) {
        Ok(p) => {
            admission.clear_auth_failures(req.token.as_deref());
            p
        }
        Err(e) => {
            if matches!(e, Error::AuthenticationFailed(_)) {
                admission.record_auth_failure(req.token.as_deref());
            }
            return Err(e);
        }
    };
    admission.admit_rpc(&principal.id)?;
    Ok(principal.id)
}

/// Outcome of reading one application RPC from the wire.
enum ReadRpc {
    /// Idle timeout on the socket.
    Idle,
    /// Soft error already answered; keep the connection.
    Continue,
    /// Drop the connection (adversarial / oversized).
    Close,
    /// Hard transport failure.
    Fatal(Error),
}

fn read_rpc_request(
    reader: &mut BufReader<IoStream>,
    max_frame: usize,
    options: &ServeOptions,
) -> Result<Option<RpcRequest>, ReadRpc> {
    if options.diagnostic_line_protocol {
        let mut line = String::new();
        let n = match reader.read_line(&mut line) {
            Ok(n) => n,
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                return Err(ReadRpc::Idle);
            }
            Err(e) => return Err(ReadRpc::Fatal(Error::from_io(e))),
        };
        if n == 0 {
            return Ok(None);
        }
        if let Err(e) = residiuum_sdk::check_rpc_line_len(line.len(), &residiuum_sdk::host_limits())
        {
            let _ = write_rpc_response(
                reader,
                options,
                RpcResponse {
                    id: 0,
                    ok: false,
                    error: Some(e.to_string()),
                    code: Some(e.code().as_str().into()),
                    ..empty_resp(0)
                },
            );
            return Err(ReadRpc::Close);
        }
        let line = line.trim();
        if line.is_empty() {
            return Err(ReadRpc::Continue);
        }
        match serde_json::from_str(line) {
            Ok(r) => Ok(Some(r)),
            Err(e) => {
                let _ = write_rpc_response(
                    reader,
                    options,
                    RpcResponse {
                        id: 0,
                        ok: false,
                        error: Some(format!("bad request: {e}")),
                        code: Some("validation_failed".into()),
                        ..empty_resp(0)
                    },
                );
                Err(ReadRpc::Continue)
            }
        }
    } else {
        let bytes = match residiuum_sdk::read_frame(reader, max_frame) {
            Ok(Some(b)) => b,
            Ok(None) => return Ok(None),
            Err(Error::DeadlineExceeded(_)) => {
                return Err(ReadRpc::Idle);
            }
            Err(e) if e.code() == residiuum_sdk::ErrorCode::ResourceLimit => {
                let _ = write_rpc_response(
                    reader,
                    options,
                    RpcResponse {
                        id: 0,
                        ok: false,
                        error: Some(e.to_string()),
                        code: Some("resource_limit".into()),
                        ..empty_resp(0)
                    },
                );
                return Err(ReadRpc::Close);
            }
            Err(e) => return Err(ReadRpc::Fatal(e)),
        };
        match serde_json::from_slice(&bytes) {
            Ok(r) => Ok(Some(r)),
            Err(e) => {
                let _ = write_rpc_response(
                    reader,
                    options,
                    RpcResponse {
                        id: 0,
                        ok: false,
                        error: Some(format!("bad request: {e}")),
                        code: Some("validation_failed".into()),
                        ..empty_resp(0)
                    },
                );
                Err(ReadRpc::Continue)
            }
        }
    }
}

fn write_rpc_response(
    reader: &mut BufReader<IoStream>,
    options: &ServeOptions,
    resp: RpcResponse,
) -> Result<(), Error> {
    if options.diagnostic_line_protocol {
        write_resp_line(reader.get_mut(), resp)
    } else {
        write_json_frame(reader.get_mut(), &resp)
    }
}

fn empty_resp(id: u64) -> RpcResponse {
    RpcResponse {
        id,
        ok: true,
        error: None,
        code: None,
        value: None,
        found: None,
        removed: None,
        key: None,
        committed: None,
        acknowledgement: None,
        keys: None,
        rows: None,
        bytes_b64: None,
        store_id: None,
        path: None,
        live_count: None,
        event_id: None,
        version: None,
        segment_id: None,
        versions: None,
        has_known_holes: None,
        indexes: None,
        payload_status: None,
        content_hash_hex: None,
        missing: None,
        total_chunks: None,
        conflict_index: None,
        present_chunks: None,
        extents: None,
        directory: None,
    }
}

/// Virtual partition for a subject using the server's advertised map.
fn partition_for_subject(options: &ServeOptions, subject: &str) -> u32 {
    use residiuum_cluster::PartitionMap;
    if let Some(dir) = options.directory.as_ref() {
        let map = PartitionMap {
            virtual_partitions: dir.virtual_partitions.max(1),
            hash_profile: if dir.hash_profile.is_empty() {
                HASH_PROFILE_BLAKE3_MOD.to_string()
            } else {
                dir.hash_profile.clone()
            },
        };
        return map.partition_of(subject.as_bytes()).get();
    }
    PartitionMap::new(DEFAULT_VIRTUAL_PARTITIONS)
        .partition_of(subject.as_bytes())
        .get()
}

/// When Raft is attached, propose + apply a mutation; otherwise write the store directly.
fn mutation_via_raft_or_store(
    store: &mut Store,
    options: &ServeOptions,
    subject: &str,
    body: &[u8],
    mode: DurabilityMode,
    is_delete: bool,
    operation_id_hex: Option<&str>,
) -> Result<residiuum_store::WriteReceipt, Error> {
    let Some(raft) = options.raft.as_ref() else {
        return if is_delete {
            Ok(store.delete(subject, mode)?)
        } else {
            Ok(store.put(subject, body, mode)?)
        };
    };
    let partition = partition_for_subject(options, subject);
    let command = if is_delete {
        residiuum_cluster::LogCommand::Delete {
            subject: subject.to_string(),
        }
    } else {
        residiuum_cluster::LogCommand::Put {
            subject: subject.to_string(),
            value: body.to_vec(),
        }
    };
    let mut g = raft
        .lock()
        .map_err(|_| Error::Internal("raft state lock poisoned".into()))?;
    // Refresh peer routing hints before campaign/propose (endpoints are not
    // write authority, but stale maps cause false NoQuorum during startup).
    if let Some(root) = options.cluster_root.as_ref() {
        g.reload_endpoints(root);
    }
    // Verify cluster identity when we have a directory (cluster node).
    if options.directory.is_some() && !g.has_partition(partition) {
        return Err(Error::StaleRoute {
            partition,
            message: format!(
                "this node has no raft replica for partition {partition} (code=not_leader)"
            ),
        });
    }
    if !g.has_partition(partition) {
        // Raft attached for tests with a single partition — fall back to local
        // store when the subject maps elsewhere (single-node serve with raft).
        return if is_delete {
            Ok(store.delete(subject, mode)?)
        } else {
            Ok(store.put(subject, body, mode)?)
        };
    }
    let (_propose, receipt) =
        g.propose_and_apply(partition, command, store, mode, operation_id_hex)?;
    Ok(receipt)
}

/// Catch up state-machine apply after control-plane replication (followers).
fn apply_raft_committed_all(store: &mut Store, options: &ServeOptions) {
    let Some(raft) = options.raft.as_ref() else {
        return;
    };
    if let Ok(mut g) = raft.lock() {
        let _ = g.apply_all_committed(store, DurabilityMode::Durable);
    }
}

/// Linearizable read barrier when Raft is attached (DEF-037).
fn linearizable_read_barrier(
    store: &mut Store,
    options: &ServeOptions,
    subject: &str,
) -> Result<(), Error> {
    let Some(raft) = options.raft.as_ref() else {
        return Ok(());
    };
    let partition = partition_for_subject(options, subject);
    let mut g = raft
        .lock()
        .map_err(|_| Error::Internal("raft state lock poisoned".into()))?;
    if let Some(root) = options.cluster_root.as_ref() {
        g.reload_endpoints(root);
    }
    if !g.has_partition(partition) {
        return Ok(());
    }
    g.read_index_barrier(partition, store)
}

fn write_resp_line(w: &mut IoStream, resp: RpcResponse) -> Result<(), Error> {
    let mut line = serde_json::to_string(&resp).map_err(|e| Error::Internal(e.to_string()))?;
    line.push('\n');
    w.write_all(line.as_bytes()).map_err(Error::from_io)?;
    w.flush().map_err(Error::from_io)?;
    Ok(())
}

fn dispatch(
    store: &mut Store,
    req: &RpcRequest,
    options: &ServeOptions,
) -> Result<RpcResponse, Error> {
    let id = req.id;
    match req.op.as_str() {
        "ping" => Ok(RpcResponse {
            id,
            ok: true,
            ..empty_resp(id)
        }),
        // DEF-061: public liveness — process is handling RPCs.
        "health_live" => {
            let report = health_report_for(store, options, /*ready_gate*/ false);
            Ok(RpcResponse {
                id,
                ok: report.live,
                value: Some(serde_json::to_value(&report).unwrap_or_default()),
                ..empty_resp(id)
            })
        }
        // DEF-061: public readiness — fails when advertised guarantees unavailable.
        "health_ready" => {
            let report = health_report_for(store, options, /*ready_gate*/ true);
            Ok(RpcResponse {
                id,
                ok: report.ready,
                error: if report.ready {
                    None
                } else {
                    Some(
                        report
                            .reasons
                            .first()
                            .cloned()
                            .unwrap_or_else(|| "not ready".into()),
                    )
                },
                code: if report.ready {
                    None
                } else {
                    Some("resource_limit".into())
                },
                value: Some(serde_json::to_value(&report).unwrap_or_default()),
                ..empty_resp(id)
            })
        }
        // DEF-061: authenticated detailed health (Read privilege).
        "health" => {
            let report = health_report_for(store, options, /*ready_gate*/ true);
            Ok(RpcResponse {
                id,
                ok: true,
                value: Some(serde_json::to_value(&report).unwrap_or_default()),
                ..empty_resp(id)
            })
        }
        // DEF-061: authenticated metrics scrape (Admin privilege).
        "metrics" => {
            let metrics = options.effective_metrics();
            let server = options.runtime.as_ref().map(|r| r.stats());
            let admission = Some(options.effective_admission().stats());
            let snap = metrics.snapshot(server, admission);
            Ok(RpcResponse {
                id,
                ok: true,
                value: Some(serde_json::to_value(&snap).unwrap_or_default()),
                ..empty_resp(id)
            })
        }
        "store_info" => Ok(RpcResponse {
            id,
            ok: true,
            path: Some(store.path().display().to_string()),
            store_id: Some(hex16(&store.store_id())),
            live_count: Some(store.live_count()),
            ..empty_resp(id)
        }),
        "raft_request_vote"
        | "raft_append_entries"
        | "raft_install_snapshot"
        | "raft_read_index" => {
            let Some(raft) = options.raft.as_ref() else {
                return Err(Error::ValidationMsg(
                    "raft RPC not available on this server (no RaftServerState)".into(),
                ));
            };
            let body = req
                .json
                .as_ref()
                .ok_or_else(|| Error::QueryInvalid("raft RPC requires json body".into()))?;
            // Reload endpoints so late-joining peers appear (routing only).
            if let Some(root) = options.cluster_root.as_ref() {
                if let Ok(mut g) = raft.lock() {
                    g.reload_endpoints(root);
                }
            }
            let value = {
                let mut g = raft
                    .lock()
                    .map_err(|_| Error::Internal("raft state lock poisoned".into()))?;
                g.dispatch_json(&req.op, body)?
            };
            // DEF-037: after AppendEntries / InstallSnapshot, apply committed
            // entries to the local data store so followers hold replicated data.
            if req.op == "raft_append_entries" || req.op == "raft_install_snapshot" {
                apply_raft_committed_all(store, options);
            }
            Ok(RpcResponse {
                id,
                ok: true,
                value: Some(value),
                ..empty_resp(id)
            })
        }
        "directory" => {
            // Prefer operator-supplied cluster snapshot (serve-cluster).
            if let Some(mut dir) = options.directory.clone() {
                // Live reload endpoints so nodes that join after this process
                // started are visible without restarting every peer.
                if let Some(root) = options.cluster_root.as_ref() {
                    if let Ok(eps) = residiuum_cluster::load_endpoints(root) {
                        dir.endpoints = eps;
                    }
                }
                // Overlay live leaders from Raft (routing hints; epoch still authoritative).
                if let Some(raft) = options.raft.as_ref() {
                    if let Ok(g) = raft.lock() {
                        g.overlay_live_leaders(&mut dir);
                    }
                }
                return Ok(RpcResponse {
                    id,
                    ok: true,
                    directory: Some(dir),
                    ..empty_resp(id)
                });
            }
            // Single-node: all virtual partitions led by node 0 (this server).
            // Clients cache this and may refresh on stale_epoch (CLUSTER_SPEC §13).
            let n = DEFAULT_VIRTUAL_PARTITIONS;
            let mut assignments = Vec::with_capacity(n as usize);
            for p in 0..n {
                assignments.push(AssignmentWire {
                    partition: p,
                    replicas: vec![0],
                    leader: 0,
                    term: 1,
                    placement_epoch: 1,
                });
            }
            let mut endpoints = HashMap::new();
            // Endpoint is informational for single-node; client already connected.
            endpoints.insert(0, String::new());
            Ok(RpcResponse {
                id,
                ok: true,
                directory: Some(DirectorySnapshot {
                    virtual_partitions: n,
                    hash_profile: HASH_PROFILE_BLAKE3_MOD.to_string(),
                    placement_epoch: 1,
                    assignments,
                    endpoints,
                }),
                ..empty_resp(id)
            })
        }
        "list_collections" => Ok(RpcResponse {
            id,
            ok: true,
            keys: Some(store.list_collections()),
            ..empty_resp(id)
        }),
        "put" => {
            let coll = require_coll(req)?;
            let key = require_key(req)?;
            let json = req
                .json
                .as_ref()
                .ok_or_else(|| Error::QueryInvalid("put requires json".into()))?;
            // DEF-029: host limits on depth and payload size (server-side).
            let limits = residiuum_sdk::host_limits();
            residiuum_sdk::check_json_depth(json, &limits)?;
            let mode =
                parse_durability(req.durability.as_deref()).unwrap_or(DurabilityMode::Durable);
            let subject = encode_subject(coll, key)?;
            let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
            let body = encode_json(json)?;
            residiuum_sdk::check_payload_len(body.len(), &limits)?;
            let dedup = prepare_mutation_dedup(
                store,
                req.operation_id.as_deref(),
                "put",
                coll,
                key,
                &body,
            )?;
            let receipt = match dedup {
                DedupOutcome::Replay(r) => r,
                DedupOutcome::Fresh {
                    op_id,
                    content_hash,
                } => {
                    let oid_hex = op_id.as_ref().map(hex16);
                    let receipt = mutation_via_raft_or_store(
                        store,
                        options,
                        subject_str,
                        &body,
                        mode,
                        false,
                        oid_hex.as_deref(),
                    )?;
                    if let Some(op_id) = op_id {
                        store.record_write_dedup(op_id, content_hash, &receipt)?;
                    }
                    receipt
                }
            };
            let _ = mark_indexes_stale(store, coll);
            let mut resp = RpcResponse {
                id,
                ok: true,
                key: Some(key.to_string()),
                ..empty_resp(id)
            };
            fill_receipt_fields(&mut resp, &receipt);
            Ok(resp)
        }
        "get" => {
            let coll = require_coll(req)?;
            let key = require_key(req)?;
            let subject = encode_subject(coll, key)?;
            let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
            // DEF-037: linearizable barrier (leader + apply catch-up) when Raft is on.
            linearizable_read_barrier(store, options, subject_str)?;
            match store.get(subject_str)? {
                None => Ok(RpcResponse {
                    id,
                    ok: true,
                    found: Some(false),
                    ..empty_resp(id)
                }),
                Some(body) => Ok(RpcResponse {
                    id,
                    ok: true,
                    found: Some(true),
                    value: Some(decode_json(&body)?),
                    ..empty_resp(id)
                }),
            }
        }
        "delete" => {
            let coll = require_coll(req)?;
            let key = require_key(req)?;
            let mode =
                parse_durability(req.durability.as_deref()).unwrap_or(DurabilityMode::Durable);
            let subject = encode_subject(coll, key)?;
            let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
            let dedup = prepare_mutation_dedup(
                store,
                req.operation_id.as_deref(),
                "delete",
                coll,
                key,
                &[],
            )?;
            let (receipt, removed) = match dedup {
                DedupOutcome::Replay(r) => {
                    // Exact retry: subject is already gone after the first delete.
                    (r, false)
                }
                DedupOutcome::Fresh {
                    op_id,
                    content_hash,
                } => {
                    let removed = store.get(subject_str)?.is_some();
                    let oid_hex = op_id.as_ref().map(hex16);
                    let receipt = mutation_via_raft_or_store(
                        store,
                        options,
                        subject_str,
                        &[],
                        mode,
                        true,
                        oid_hex.as_deref(),
                    )?;
                    if let Some(op_id) = op_id {
                        store.record_write_dedup(op_id, content_hash, &receipt)?;
                    }
                    (receipt, removed)
                }
            };
            let _ = mark_indexes_stale(store, coll);
            let mut resp = RpcResponse {
                id,
                ok: true,
                key: Some(key.to_string()),
                removed: Some(removed),
                ..empty_resp(id)
            };
            fill_receipt_fields(&mut resp, &receipt);
            Ok(resp)
        }
        "put_bytes" => {
            let coll = require_coll(req)?;
            let key = require_key(req)?;
            let b64 = req
                .bytes_b64
                .as_deref()
                .ok_or_else(|| Error::QueryInvalid("put_bytes requires bytes_b64".into()))?;
            let bytes = b64_decode(b64)?;
            let limits = residiuum_sdk::host_limits();
            residiuum_sdk::check_payload_len(bytes.len().saturating_add(1), &limits)?;
            let mode =
                parse_durability(req.durability.as_deref()).unwrap_or(DurabilityMode::Durable);
            let subject = encode_subject(coll, key)?;
            let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
            let body = encode_bytes(&bytes);
            let dedup = prepare_mutation_dedup(
                store,
                req.operation_id.as_deref(),
                "put_bytes",
                coll,
                key,
                &body,
            )?;
            let receipt = match dedup {
                DedupOutcome::Replay(r) => r,
                DedupOutcome::Fresh {
                    op_id,
                    content_hash,
                } => {
                    let oid_hex = op_id.as_ref().map(hex16);
                    let receipt = mutation_via_raft_or_store(
                        store,
                        options,
                        subject_str,
                        &body,
                        mode,
                        false,
                        oid_hex.as_deref(),
                    )?;
                    if let Some(op_id) = op_id {
                        store.record_write_dedup(op_id, content_hash, &receipt)?;
                    }
                    receipt
                }
            };
            let _ = mark_indexes_stale(store, coll);
            let mut resp = RpcResponse {
                id,
                ok: true,
                key: Some(key.to_string()),
                ..empty_resp(id)
            };
            fill_receipt_fields(&mut resp, &receipt);
            Ok(resp)
        }
        "get_bytes" => {
            let coll = require_coll(req)?;
            let key = require_key(req)?;
            let subject = encode_subject(coll, key)?;
            let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
            linearizable_read_barrier(store, options, subject_str)?;
            match store.get(subject_str)? {
                None => Ok(RpcResponse {
                    id,
                    ok: true,
                    found: Some(false),
                    ..empty_resp(id)
                }),
                Some(body) => Ok(RpcResponse {
                    id,
                    ok: true,
                    found: Some(true),
                    bytes_b64: Some(b64_encode(&decode_bytes(&body)?)),
                    ..empty_resp(id)
                }),
            }
        }
        "list_keys" => {
            let coll = require_coll(req)?;
            let prefix = collection_prefix(coll)?;
            let mut keys = Vec::new();
            for (subject, _body) in store.live_entries() {
                if !subject.starts_with(&prefix) {
                    continue;
                }
                if let Some((c, key)) = decode_subject(subject) {
                    if c == coll {
                        keys.push(key.to_string());
                    }
                }
            }
            keys.sort();
            Ok(RpcResponse {
                id,
                ok: true,
                keys: Some(keys),
                ..empty_resp(id)
            })
        }
        "scan_json" => {
            let coll = require_coll(req)?;
            let prefix = collection_prefix(coll)?;
            let logical = store.live_logical_entries()?;
            let mut rows = Vec::new();
            for (subject, body) in logical {
                if !subject.starts_with(&prefix) {
                    continue;
                }
                let Some((c, key)) = decode_subject(&subject) else {
                    continue;
                };
                if c != coll {
                    continue;
                }
                // DEF-012: never convert decode failure into a shorter success.
                let value = decode_json(&body).map_err(|e| {
                    Error::CoverageIncomplete(format!(
                        "scan_json: key {key:?} failed JSON decode: {e}"
                    ))
                })?;
                rows.push(ScanRow {
                    key: key.to_string(),
                    value,
                });
                if let Some(limit) = req.limit {
                    if rows.len() >= limit {
                        break;
                    }
                }
            }
            rows.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(RpcResponse {
                id,
                ok: true,
                rows: Some(rows),
                ..empty_resp(id)
            })
        }
        "history" => {
            let coll = require_coll(req)?;
            let key = require_key(req)?;
            let subject = encode_subject(coll, key)?;
            let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
            let hist = store.history(subject_str)?;
            let projected = KeyHistory::from_store(key.to_string(), hist)?;
            let versions = projected
                .versions
                .into_iter()
                .map(|v| HistoryVersionRow {
                    kind: v.kind.into(),
                    event_id: v.event_id,
                    item_id: v.item_id,
                    segment_id: v.segment_id,
                    json: v.json,
                    body_b64: v.body.as_deref().map(b64_encode),
                    known_gap_before: v.known_gap_before,
                })
                .collect();
            Ok(RpcResponse {
                id,
                ok: true,
                key: Some(projected.key),
                versions: Some(versions),
                has_known_holes: Some(projected.has_known_holes),
                ..empty_resp(id)
            })
        }
        "index_list" => {
            let coll = require_coll(req)?;
            let indexes = store
                .list_secondary_indexes(coll)?
                .iter()
                .map(|i| IndexInfoRow::from_info(&IndexInfo::from_store(i)))
                .collect();
            Ok(RpcResponse {
                id,
                ok: true,
                indexes: Some(indexes),
                ..empty_resp(id)
            })
        }
        "index_create" => {
            let coll = require_coll(req)?;
            let name = require_key(req)?;
            let fields = req
                .fields
                .as_ref()
                .ok_or_else(|| Error::QueryInvalid("index_create requires fields".into()))?;
            if fields.is_empty() {
                return Err(Error::QueryInvalid(
                    "index requires at least one field".into(),
                ));
            }
            let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
            let info = create_index_on_store(store, coll, name, &field_refs)?;
            Ok(RpcResponse {
                id,
                ok: true,
                indexes: Some(vec![IndexInfoRow::from_info(&info)]),
                ..empty_resp(id)
            })
        }
        "index_drop" => {
            let coll = require_coll(req)?;
            let name = require_key(req)?;
            store.delete_secondary_index(coll, name)?;
            Ok(RpcResponse {
                id,
                ok: true,
                ..empty_resp(id)
            })
        }
        "index_rebuild" => {
            let coll = require_coll(req)?;
            let name = require_key(req)?;
            let existing = store
                .load_secondary_index(coll, name)?
                .ok_or_else(|| Error::QueryInvalid(format!("index not found: {name}")))?;
            let fields: Vec<&str> = existing.meta.fields.iter().map(|s| s.as_str()).collect();
            let info = create_index_on_store(store, coll, name, &fields)?;
            Ok(RpcResponse {
                id,
                ok: true,
                indexes: Some(vec![IndexInfoRow::from_info(&info)]),
                ..empty_resp(id)
            })
        }
        "get_payload" => {
            let coll = require_coll(req)?;
            let key = require_key(req)?;
            let subject = encode_subject(coll, key)?;
            let subject_str = str::from_utf8(&subject).expect("stage 4 subject is UTF-8");
            match store.get_payload(subject_str)? {
                None => Ok(RpcResponse {
                    id,
                    ok: true,
                    found: Some(false),
                    ..empty_resp(id)
                }),
                Some(result) => payload_to_resp(id, &result),
            }
        }
        "find" => {
            let coll = require_coll(req)?;
            let filter_json = req
                .json
                .as_ref()
                .ok_or_else(|| Error::QueryInvalid("find requires json filter object".into()))?;
            let filter = Filter::from_json(filter_json)?;
            let order_by = match (
                req.order_field.as_deref(),
                req.order_dir.as_deref().unwrap_or("asc"),
            ) {
                (Some(f), "desc") => Some((f.to_string(), SortOrder::Desc)),
                (Some(f), _) => Some((f.to_string(), SortOrder::Asc)),
                (None, _) => None,
            };
            let budget = if req.max_docs_scanned.is_some()
                || req.max_bytes_scanned.is_some()
                || req.max_result_bytes.is_some()
            {
                Some(QueryBudget {
                    max_docs_scanned: req.max_docs_scanned,
                    max_bytes_scanned: req.max_bytes_scanned,
                    max_result_bytes: req.max_result_bytes,
                })
            } else {
                None
            };
            let options = QueryOptions {
                limit: req.limit,
                order_by,
                budget,
                force_scan: req.force_scan.unwrap_or(false),
                allow_partial_coverage: false,
                cancel: None,
            };
            let rows = find_on_store(store, coll, &filter, &options)?;
            Ok(RpcResponse {
                id,
                ok: true,
                rows: Some(
                    rows.into_iter()
                        .map(|(key, value)| ScanRow { key, value })
                        .collect(),
                ),
                ..empty_resp(id)
            })
        }
        // DEF-033 privileged / recovery ops: authorization already enforced in
        // the connection loop. These handlers are scaffolds so permission gates
        // and high-friction confirm can be tested end-to-end before full
        // salvage/tier/purge engines land.
        "admin_stats" => Ok(RpcResponse {
            id,
            ok: true,
            live_count: Some(store.live_count()),
            path: Some(store.path().display().to_string()),
            store_id: Some(hex16(&store.store_id())),
            ..empty_resp(id)
        }),
        // Scaffold: privilege + audit already enforced; full engines land later.
        "salvage_export" | "tier_move" | "force_reconfig" => Ok(RpcResponse {
            id,
            ok: true,
            ..empty_resp(id)
        }),
        "purge" => {
            let _ = (PURGE_CONFIRM, FORCE_RECONFIG_CONFIRM);
            Ok(RpcResponse {
                id,
                ok: true,
                removed: Some(false),
                ..empty_resp(id)
            })
        }
        other => Err(Error::QueryInvalid(format!("unknown op: {other}"))),
    }
}

/// Build a DEF-061 health report from the live process + store.
///
/// `ready_gate` is reserved for callers that only care about readiness shape;
/// evaluation always computes both live and ready.
fn health_report_for(
    store: &Store,
    options: &ServeOptions,
    _ready_gate: bool,
) -> crate::metrics::HealthReport {
    let draining = options
        .runtime
        .as_ref()
        .map(|r| r.is_draining())
        .unwrap_or(false);
    let active = options.runtime.as_ref().map(|r| r.active_connections());
    let mode = if options.cluster_root.is_some() || options.experimental_network_cluster {
        "serve-cluster"
    } else {
        "serve"
    };
    let store_path = store.path().display().to_string();
    evaluate_health(HealthEvalInput {
        process_up: true,
        draining,
        store_open: true,
        store_path: Some(store_path.as_str()),
        live_count: Some(store.live_count()),
        mode: Some(mode),
        node_index: options.node_index,
        raft_attached: options.raft.is_some(),
        claims_replication: options.experimental_network_cluster,
        active_connections: active,
        log_profile: LOG_PROFILE,
    })
}

fn require_coll(req: &RpcRequest) -> Result<&str, Error> {
    let c = req
        .collection
        .as_deref()
        .ok_or(Error::InvalidCollectionName("missing collection"))?;
    validate_collection_name(c)?;
    Ok(c)
}

fn require_key(req: &RpcRequest) -> Result<&str, Error> {
    let k = req.key.as_deref().ok_or(Error::InvalidKey("missing key"))?;
    validate_key(k)?;
    Ok(k)
}

fn durability_name(mode: DurabilityMode) -> &'static str {
    mode.as_str()
}

fn parse_durability(s: Option<&str>) -> Option<DurabilityMode> {
    match s? {
        "memory" => Some(DurabilityMode::Memory),
        "buffered" => Some(DurabilityMode::Buffered),
        "durable" => Some(DurabilityMode::Durable),
        _ => None,
    }
}

fn hex16(id: &[u8; 16]) -> String {
    let mut s = String::with_capacity(32);
    for b in id {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn b64_encode(bytes: &[u8]) -> String {
    // Minimal base64 (std-only) for Stage 7 wire encoding.
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    let mut i = 0;
    while i < bytes.len() {
        let b0 = bytes[i] as u32;
        let b1 = if i + 1 < bytes.len() {
            bytes[i + 1] as u32
        } else {
            0
        };
        let b2 = if i + 2 < bytes.len() {
            bytes[i + 2] as u32
        } else {
            0
        };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(T[((triple >> 18) & 63) as usize] as char);
        out.push(T[((triple >> 12) & 63) as usize] as char);
        if i + 1 < bytes.len() {
            out.push(T[((triple >> 6) & 63) as usize] as char);
        } else {
            out.push('=');
        }
        if i + 2 < bytes.len() {
            out.push(T[(triple & 63) as usize] as char);
        } else {
            out.push('=');
        }
        i += 3;
    }
    out
}

fn b64_decode(s: &str) -> Result<Vec<u8>, Error> {
    fn val(c: u8) -> Result<u8, Error> {
        match c {
            b'A'..=b'Z' => Ok(c - b'A'),
            b'a'..=b'z' => Ok(c - b'a' + 26),
            b'0'..=b'9' => Ok(c - b'0' + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(Error::QueryInvalid("invalid base64".into())),
        }
    }
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(Error::QueryInvalid("invalid base64 length".into()));
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let mut i = 0;
    while i < bytes.len() {
        let a = val(bytes[i])?;
        let b = val(bytes[i + 1])?;
        let c = if bytes[i + 2] == b'=' {
            0
        } else {
            val(bytes[i + 2])?
        };
        let d = if bytes[i + 3] == b'=' {
            0
        } else {
            val(bytes[i + 3])?
        };
        let triple = ((a as u32) << 18) | ((b as u32) << 12) | ((c as u32) << 6) | (d as u32);
        out.push(((triple >> 16) & 0xff) as u8);
        if bytes[i + 2] != b'=' {
            out.push(((triple >> 8) & 0xff) as u8);
        }
        if bytes[i + 3] != b'=' {
            out.push((triple & 0xff) as u8);
        }
        i += 4;
    }
    Ok(out)
}
