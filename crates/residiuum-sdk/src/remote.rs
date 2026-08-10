//! Framed, versioned JSON RPC for `residiuum://` remote access (Stages 7 + 8d, DEF-031).
//!
//! Server and client share the same request/response shapes. Transport is TCP.
//! **Production profile** (`residiuum-rpc-v1`):
//! 1. length-prefixed frames (`u32` BE + UTF-8 JSON payload);
//! 2. explicit hello/welcome handshake with feature negotiation;
//! 3. application [`RpcRequest`] / [`RpcResponse`] only after session setup.
//!
//! **Diagnostic profile:** optional newline-delimited JSON when both sides set
//! `diagnostic_line_protocol` (human debugging with `nc`; not for production).
//!
//! Supported ops include put/get/delete/scan, **history**, secondary index
//! list/create/drop/rebuild, **get_payload** (chunk completeness), **find**
//! (server-side filter with index acceleration), and **directory** (Stage 8d
//! partition route snapshot for client caches). Connection-only concerns (auth
//! token, deadlines, connect retries, wire profile) live on [`ConnectOptions`] /
//! `residiuum_server::ServeOptions` — not on the collection API (DX_SPEC §4.2).

use crate::directory_cache::{ClientDirectoryCache, DirectorySnapshot};
use crate::error::Error;
use crate::filter::{Filter, QueryOptions, SortOrder};
use crate::history::{KeyHistory, Version};
use crate::indexes::IndexInfo;
use crate::receipt::{DeleteReceipt, PutOptions, WriteReceipt};
use crate::subject::{encode_subject, validate_collection_name, validate_key};
use crate::tls::{client_connect, IoStream, TlsClientOptions};
use crate::{
    negotiate_features, negotiate_max_frame, parse_handshake, read_frame, write_json_frame,
    Handshake, HandshakeMsg, NegotiatedSession, HANDSHAKE_MAX_FRAME_BYTES,
};
use residiuum_store::{
    random_id, ByteRange, DurabilityMode, IndexState, LogicalExtent, PayloadResult,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

/// Default TCP port from DX_SPEC server examples (`residiuum://localhost:7434/...`).
pub const DEFAULT_PORT: u16 = 7434;

/// Client connection options (DX_SPEC §4.2: authn, deadlines, retry).
///
/// These do **not** change the collection put/get API — only how `connect` and
/// subsequent RPCs behave on the wire.
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// Optional shared secret sent on every RPC as `token`.
    pub auth_token: Option<String>,
    /// Timeout for establishing the TCP connection (per attempt).
    pub connect_timeout: Duration,
    /// Read/write timeout applied to each request/response exchange.
    pub request_timeout: Duration,
    /// How many times to retry a failed connect (including the first try).
    pub max_connect_attempts: u32,
    /// Sleep between connect attempts.
    pub retry_backoff: Duration,
    /// Use legacy newline-delimited JSON (diagnostic only; DEF-031).
    ///
    /// Requires the server to also enable
    /// `residiuum_server::ServeOptions::diagnostic_line_protocol`. Production clients leave
    /// this `false` and perform the framed `residiuum-rpc-v1` handshake.
    pub diagnostic_line_protocol: bool,
    /// Optional TLS client configuration (DEF-032).
    ///
    /// When set, the client performs a TLS 1.3 handshake before the framed
    /// RPC handshake. The server must be configured with matching TLS material.
    pub tls: Option<TlsClientOptions>,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            auth_token: None,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_connect_attempts: 3,
            retry_backoff: Duration::from_millis(50),
            diagnostic_line_protocol: false,
            tls: None,
        }
    }
}

impl ConnectOptions {
    /// Default options (no auth, 5s connect / 30s request, 3 attempts).
    pub fn new() -> Self {
        Self::default()
    }

    /// Require this shared token on the server (`ServeOptions::auth_token`).
    pub fn auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Per-attempt TCP connect timeout.
    pub fn connect_timeout(mut self, d: Duration) -> Self {
        self.connect_timeout = d;
        self
    }

    /// Per-RPC read/write deadline.
    pub fn request_timeout(mut self, d: Duration) -> Self {
        self.request_timeout = d;
        self
    }

    /// Connect attempts including the first try (minimum 1).
    pub fn max_connect_attempts(mut self, n: u32) -> Self {
        self.max_connect_attempts = n.max(1);
        self
    }

    /// Delay between failed connect attempts.
    pub fn retry_backoff(mut self, d: Duration) -> Self {
        self.retry_backoff = d;
        self
    }

    /// Enable legacy line-delimited JSON (must match server diagnostic mode).
    pub fn diagnostic_line_protocol(mut self, enabled: bool) -> Self {
        self.diagnostic_line_protocol = enabled;
        self
    }

    /// Enable TLS 1.3 with the given client options (DEF-032).
    pub fn tls(mut self, options: TlsClientOptions) -> Self {
        self.tls = Some(options);
        self
    }
}

/// Wire request (one JSON line).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    /// Correlation id (echoed in the response).
    pub id: u64,
    /// Operation name.
    pub op: String,
    /// Collection name when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
    /// Application key when applicable (also used as index name for index_* ops).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// JSON document body for put.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json: Option<JsonValue>,
    /// Base64-encoded bytes for put_bytes/get_bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_b64: Option<String>,
    /// Optional result limit for scans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Durability mode name: `memory` | `buffered` | `durable` (default durable).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durability: Option<String>,
    /// Optional shared auth token (connection option; DX_SPEC §4.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// Field paths for index_create.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    /// When true, force a full collection scan for `find` (skip indexes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_scan: Option<bool>,
    /// Order-by field path for `find`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_field: Option<String>,
    /// Order direction for `find`: `asc` | `desc`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_dir: Option<String>,
    /// Max documents the server may examine for `find` (query budget).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_docs_scanned: Option<usize>,
    /// Max payload bytes the server may examine for `find` (DEF-029).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes_scanned: Option<u64>,
    /// Max approximate result materialisation bytes for `find` (DEF-029).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_result_bytes: Option<u64>,
    /// Client-generated operation id (32 hex chars) for idempotent mutations (DEF-010).
    ///
    /// Required on put/delete/put_bytes from modern clients. Retries reuse the
    /// same id; reuse with different content yields `consistency_violation`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    /// High-friction confirmation for privileged ops (DEF-033).
    ///
    /// Required values: `PURGE` for `purge`, `FORCE_RECONFIG` for
    /// `force_reconfig`. Never log this field as a secret — it is a deliberate
    /// operator acknowledgement, not a credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm: Option<String>,
}

/// Wire response (one JSON line).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    /// Correlation id from the request.
    pub id: u64,
    /// Whether the operation succeeded.
    pub ok: bool,
    /// Error message when `ok` is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Stable error code when `ok` is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// JSON value for get.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<JsonValue>,
    /// Whether a get found a value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub found: Option<bool>,
    /// Whether delete removed a visible value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub removed: Option<bool>,
    /// Application key from write/delete receipts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Committed flag from write receipt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed: Option<bool>,
    /// Achieved durability mode name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledgement: Option<String>,
    /// List of keys or collection names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<String>>,
    /// List of (key, json) pairs from scan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<ScanRow>>,
    /// Base64 bytes for get_bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_b64: Option<String>,
    /// Hex store id for store_info.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_id: Option<String>,
    /// Store path string for store_info.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Live subject count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_count: Option<usize>,
    /// Hex event id from write/delete receipts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    /// Hex item/version id from write/delete receipts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Hex segment id from write/delete receipts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segment_id: Option<String>,
    /// Per-key history versions (history op).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub versions: Option<Vec<HistoryVersionRow>>,
    /// Whether history stream observed salvage holes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_known_holes: Option<bool>,
    /// Secondary index metadata rows (index_list / index_create / index_rebuild).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexes: Option<Vec<IndexInfoRow>>,
    /// Payload completeness: `complete` | `partial` | `unavailable` | `conflicting`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_status: Option<String>,
    /// BLAKE3-256 content hash as 64 hex chars (partial/unavailable payloads).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash_hex: Option<String>,
    /// Missing chunk indices for partial payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missing: Option<Vec<u32>>,
    /// Declared total chunk count (unavailable payloads).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_chunks: Option<u32>,
    /// Conflicting chunk index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_index: Option<u32>,
    /// Surviving chunk bodies for partial payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub present_chunks: Option<Vec<PresentChunkRow>>,
    /// Extent map for partial payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extents: Option<Vec<ExtentRow>>,
    /// Partition directory snapshot (`directory` op; Stage 8d).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub directory: Option<DirectorySnapshot>,
}

/// One scan row on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanRow {
    /// Application key.
    pub key: String,
    /// JSON document.
    pub value: JsonValue,
}

/// One history version on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryVersionRow {
    /// `put` or `delete`.
    pub kind: String,
    /// Hex event id.
    pub event_id: String,
    /// Hex item lineage id.
    pub item_id: String,
    /// Hex segment id.
    pub segment_id: String,
    /// JSON document when the put stored JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json: Option<JsonValue>,
    /// Optional typed store body (base64).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_b64: Option<String>,
    /// Known salvage hole before this event.
    #[serde(default)]
    pub known_gap_before: bool,
}

/// Secondary index metadata on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfoRow {
    /// Index name.
    pub name: String,
    /// Collection name.
    pub collection: String,
    /// Indexed field paths.
    pub fields: Vec<String>,
    /// Lifecycle state snake_case name.
    pub state: String,
    /// Number of postings.
    pub entry_count: u64,
    /// Whether the index claims complete coverage.
    pub complete_coverage: bool,
    /// Failure / partial detail (DEF-027); empty when healthy.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub failure_reason: String,
    /// Hex build id (DEF-027).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub build_id_hex: String,
}

/// One present chunk body on the wire (`get_payload` partial).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresentChunkRow {
    /// Chunk index.
    pub index: u32,
    /// Base64 chunk body.
    pub bytes_b64: String,
}

/// One logical extent on the wire (`get_payload` partial).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtentRow {
    /// Chunk index.
    pub index: u32,
    /// Inclusive logical start offset.
    pub start: u64,
    /// Exclusive logical end offset.
    pub end: u64,
    /// Whether the chunk body is present.
    pub present: bool,
}

impl IndexInfoRow {
    /// Build a wire row from SDK index info.
    pub fn from_info(info: &IndexInfo) -> Self {
        Self {
            name: info.name.clone(),
            collection: info.collection.clone(),
            fields: info.fields.clone(),
            state: info.state.as_str().into(),
            entry_count: info.entry_count,
            complete_coverage: info.complete_coverage,
            failure_reason: info.failure_reason.clone(),
            build_id_hex: info.build_id_hex.clone(),
        }
    }

    /// Convert wire row into SDK index info.
    pub fn into_info(self) -> Result<IndexInfo, Error> {
        let state = IndexState::parse(&self.state).ok_or_else(|| {
            Error::Internal(format!("unknown index state from remote: {}", self.state))
        })?;
        Ok(IndexInfo {
            name: self.name,
            collection: self.collection,
            fields: self.fields,
            state,
            entry_count: self.entry_count,
            complete_coverage: self.complete_coverage,
            failure_reason: self.failure_reason,
            build_id_hex: self.build_id_hex,
        })
    }
}

/// Maximum transport retries after a directory refresh (multi-hop polish).
const MAX_ROUTE_TRANSPORT_RETRIES: u32 = 3;

/// Client connection to a `residiuum serve` / `residiuum serve-cluster` process.
///
/// After connect the client fetches a partition [`DirectorySnapshot`] and, for
/// keyed ops, routes to the advertised leader `host:port` (CLUSTER_SPEC §13).
/// On transport failure the directory is refreshed and the op is retried.
pub struct RemoteClient {
    /// Buffered transport (plaintext or TLS). Writes use `reader.get_mut()`.
    reader: BufReader<IoStream>,
    next_id: AtomicU64,
    /// Logical URL (`residiuum://…`) for display / errors.
    endpoint: String,
    /// TCP address of the current connection (`host:port`).
    addr: String,
    options: ConnectOptions,
    /// Cached store id from the first `store_info` after connect.
    store_id: [u8; 16],
    /// Client route cache (populated after connect via `directory` RPC).
    directory: Option<ClientDirectoryCache>,
    /// How many times we reconnected to a different leader for routing.
    route_hops: u64,
    /// How many times we refreshed the directory after transport failure.
    directory_refreshes: u64,
    /// Negotiated max frame size (framed profile) or host line limit (diagnostic).
    max_frame: usize,
    /// Snapshot of negotiated features (empty in diagnostic line mode).
    session: Option<NegotiatedSession>,
}

impl RemoteClient {
    /// Connect to `host:port` with default [`ConnectOptions`].
    pub fn connect(addr: &str, endpoint: String) -> Result<Self, Error> {
        Self::connect_with(addr, endpoint, ConnectOptions::default())
    }

    /// Connect with explicit auth / deadline / retry options.
    ///
    /// On success, loads a partition directory snapshot for multi-hop routing
    /// when the server advertises real endpoints (`residiuum serve-cluster`).
    pub fn connect_with(
        addr: &str,
        endpoint: String,
        options: ConnectOptions,
    ) -> Result<Self, Error> {
        let mut client = Self::connect_raw(addr, endpoint, options)?;
        // Best-effort directory load: single-node servers return a synthetic
        // map; cluster nodes return placement + endpoints.json.
        let _ = client.refresh_directory();
        Ok(client)
    }

    fn connect_raw(addr: &str, endpoint: String, options: ConnectOptions) -> Result<Self, Error> {
        let stream = open_client_stream(addr, &options)?;
        stream
            .set_read_timeout(Some(options.request_timeout))
            .map_err(Error::from_io)?;
        stream
            .set_write_timeout(Some(options.request_timeout))
            .map_err(Error::from_io)?;
        let mut reader = BufReader::new(stream);
        let (max_frame, session) = if options.diagnostic_line_protocol {
            (crate::resource::host_limits().max_rpc_line_bytes, None)
        } else {
            let session = client_handshake_buffered(&mut reader)?;
            (session.max_frame, Some(session))
        };
        let mut client = Self {
            reader,
            next_id: AtomicU64::new(1),
            endpoint,
            addr: addr.to_string(),
            options,
            store_id: [0u8; 16],
            directory: None,
            route_hops: 0,
            directory_refreshes: 0,
            max_frame,
            session,
        };
        // Immediate store_info validates auth token, proves protocol, caches store id.
        let (_path, sid_hex, _n) = client.store_info()?;
        client.store_id = require_hex16(Some(sid_hex.as_str()), "store_info.store_id")?;
        if client.store_id == [0u8; 16] {
            return Err(Error::ProtocolViolation(
                "store_info.store_id must not be the zero id".into(),
            ));
        }
        Ok(client)
    }

    /// Endpoint string used for errors and display.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Current TCP address (`host:port`).
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Connection options used for this client.
    pub fn options(&self) -> &ConnectOptions {
        &self.options
    }

    /// Store identifier reported by the server at connect time.
    pub fn store_id(&self) -> [u8; 16] {
        self.store_id
    }

    /// Borrow the client directory cache when loaded.
    pub fn directory_cache(&self) -> Option<&ClientDirectoryCache> {
        self.directory.as_ref()
    }

    /// Number of multi-hop reconnects performed for routing.
    pub fn route_hops(&self) -> u64 {
        self.route_hops
    }

    /// Number of directory refreshes after transport failure.
    pub fn directory_refreshes(&self) -> u64 {
        self.directory_refreshes
    }

    /// Fetch and install a fresh partition directory snapshot.
    pub fn refresh_directory(&mut self) -> Result<&ClientDirectoryCache, Error> {
        let snap = self.fetch_directory()?;
        self.directory = Some(ClientDirectoryCache::from_snapshot(&snap));
        self.directory_refreshes = self.directory_refreshes.saturating_add(1);
        Ok(self.directory.as_ref().expect("just set"))
    }

    /// Negotiated session after framed handshake (None in diagnostic line mode).
    pub fn session(&self) -> Option<&NegotiatedSession> {
        self.session.as_ref()
    }

    /// Reconnect this client to a different `host:port`, keeping directory cache.
    pub fn reconnect(&mut self, addr: &str) -> Result<(), Error> {
        if addr == self.addr {
            return Ok(());
        }
        let stream = open_client_stream(addr, &self.options)?;
        stream
            .set_read_timeout(Some(self.options.request_timeout))
            .map_err(Error::from_io)?;
        stream
            .set_write_timeout(Some(self.options.request_timeout))
            .map_err(Error::from_io)?;
        let mut reader = BufReader::new(stream);
        if self.options.diagnostic_line_protocol {
            self.max_frame = crate::resource::host_limits().max_rpc_line_bytes;
            self.session = None;
        } else {
            let session = client_handshake_buffered(&mut reader)?;
            self.max_frame = session.max_frame;
            self.session = Some(session);
        }
        self.reader = reader;
        self.addr = addr.to_string();
        self.route_hops = self.route_hops.saturating_add(1);
        // Refresh store id on the new node (each node has its own store).
        // Fail closed on missing/malformed identity (DEF-014); multi-hop may
        // legitimately change store_id when routing to a different node.
        let resp = self.call_on_current(base_req("store_info"))?;
        let sid_hex = resp.store_id.as_deref().ok_or_else(|| {
            Error::ProtocolViolation("store_info response missing store_id".into())
        })?;
        self.store_id = require_hex16(Some(sid_hex), "store_info.store_id")?;
        if self.store_id == [0u8; 16] {
            return Err(Error::ProtocolViolation(
                "store_info.store_id must not be the zero id".into(),
            ));
        }
        Ok(())
    }

    /// Route a keyed op to the cached partition leader when endpoints are known.
    fn ensure_route_for_key(&mut self, collection: &str, key: &str) -> Result<(), Error> {
        let subject = encode_subject(collection, key)?;
        let target = {
            let Some(cache) = self.directory.as_ref() else {
                return Ok(());
            };
            let Some(route) = cache.route(&subject) else {
                return Ok(());
            };
            let Some(ep) = cache.endpoint(route.leader) else {
                return Ok(());
            };
            if ep.is_empty() || ep == self.addr {
                return Ok(());
            }
            ep.to_string()
        };
        self.reconnect(&target)
    }

    /// Try a keyed RPC with multi-hop routing and transport / fence refresh.
    ///
    /// Retries on transport failures **and** typed epoch/term/not-leader errors
    /// (DEF-037). The same `operation_id` is preserved across redirects.
    fn call_keyed(
        &mut self,
        collection: &str,
        key: &str,
        req: RpcRequest,
    ) -> Result<RpcResponse, Error> {
        let mut last_err: Option<Error> = None;
        for attempt in 0..MAX_ROUTE_TRANSPORT_RETRIES {
            // Best-effort hop to the cached leader; fall through on failure.
            let _ = self.ensure_route_for_key(collection, key);
            match self.call_on_current(req.clone()) {
                Ok(r) => return Ok(r),
                Err(e)
                    if is_route_refresh_error(&e) && attempt + 1 < MAX_ROUTE_TRANSPORT_RETRIES =>
                {
                    // Mark partition stale, refresh directory (live leaders), try again.
                    if let Ok(subject) = encode_subject(collection, key) {
                        if let Some(cache) = self.directory.as_mut() {
                            let p = cache.partition_of(&subject);
                            cache.mark_stale(p);
                        }
                    }
                    let _ = self.refresh_directory();
                    // After refresh, hop again to the updated leader endpoint.
                    let _ = self.ensure_route_for_key(collection, key);
                    last_err = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err.unwrap_or_else(|| Error::Internal("route retries exhausted".into())))
    }

    fn call(&mut self, req: RpcRequest) -> Result<RpcResponse, Error> {
        self.call_on_current(req)
    }

    /// Public one-shot RPC on the current connection (control-plane / Raft peers).
    ///
    /// Used by [`residiuum_server::TcpRaftTransport`] so peer RPCs do not need
    /// collection routing. Not a data-plane write authority path.
    pub fn call_rpc(&mut self, req: RpcRequest) -> Result<RpcResponse, Error> {
        self.call_on_current(req)
    }

    fn call_on_current(&mut self, mut req: RpcRequest) -> Result<RpcResponse, Error> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        req.id = id;
        if req.token.is_none() {
            req.token = self.options.auth_token.clone();
        }
        let payload = serde_json::to_vec(&req).map_err(|e| Error::Internal(e.to_string()))?;
        // DEF-029 / DEF-031: fail closed on oversized outbound messages before write.
        if payload.len() > self.max_frame {
            return Err(Error::ResourceLimit(format!(
                "rpc request {} bytes exceeds negotiated max_frame {}",
                payload.len(),
                self.max_frame
            )));
        }
        if self.options.diagnostic_line_protocol {
            {
                let w = self.reader.get_mut();
                w.write_all(&payload).map_err(Error::from_io)?;
                w.write_all(b"\n").map_err(Error::from_io)?;
                w.flush().map_err(Error::from_io)?;
            }
            let mut resp_line = String::new();
            let n = self
                .reader
                .read_line(&mut resp_line)
                .map_err(Error::from_io)?;
            if n == 0 {
                return Err(Error::Internal(format!(
                    "remote closed connection: {} ({})",
                    self.endpoint, self.addr
                )));
            }
            return decode_rpc_response(resp_line.trim().as_bytes(), &self.endpoint, &self.addr);
        }

        write_json_frame(self.reader.get_mut(), &req)?;
        let resp_bytes = read_frame(&mut self.reader, self.max_frame)?.ok_or_else(|| {
            Error::Internal(format!(
                "remote closed connection: {} ({})",
                self.endpoint, self.addr
            ))
        })?;
        decode_rpc_response(&resp_bytes, &self.endpoint, &self.addr)
    }

    /// Ping the server.
    pub fn ping(&mut self) -> Result<(), Error> {
        let _ = self.call(base_req("ping"))?;
        Ok(())
    }

    /// Store path / id summary.
    pub fn store_info(&mut self) -> Result<(String, String, usize), Error> {
        let resp = self.call(base_req("store_info"))?;
        Ok((
            resp.path.unwrap_or_default(),
            resp.store_id.unwrap_or_default(),
            resp.live_count.unwrap_or(0),
        ))
    }

    /// List collection names.
    pub fn list_collections(&mut self) -> Result<Vec<String>, Error> {
        let resp = self.call(base_req("list_collections"))?;
        Ok(resp.keys.unwrap_or_default())
    }

    /// Put JSON under collection/key (multi-hop: routes to partition leader).
    pub fn put_json(
        &mut self,
        collection: &str,
        key: &str,
        value: &JsonValue,
        options: PutOptions,
    ) -> Result<WriteReceipt, Error> {
        validate_collection_name(collection)?;
        validate_key(key)?;
        // Mint once so call_keyed transport retries stay idempotent (DEF-010).
        let operation_id = Some(hex16(&client_operation_id()?));
        let resp = self.call_keyed(
            collection,
            key,
            RpcRequest {
                op: "put".into(),
                collection: Some(collection.into()),
                key: Some(key.into()),
                json: Some(value.clone()),
                durability: Some(durability_name(options.durability).into()),
                operation_id,
                ..base_req("put")
            },
        )?;
        write_receipt_from_resp(key, &resp, options.durability)
    }

    /// Get JSON for collection/key (multi-hop: routes to partition leader).
    pub fn get_json(&mut self, collection: &str, key: &str) -> Result<Option<JsonValue>, Error> {
        validate_collection_name(collection)?;
        validate_key(key)?;
        let resp = self.call_keyed(
            collection,
            key,
            RpcRequest {
                op: "get".into(),
                collection: Some(collection.into()),
                key: Some(key.into()),
                ..base_req("get")
            },
        )?;
        if resp.found == Some(false) {
            return Ok(None);
        }
        Ok(resp.value)
    }

    /// Delete collection/key (multi-hop: routes to partition leader).
    pub fn delete(
        &mut self,
        collection: &str,
        key: &str,
        options: PutOptions,
    ) -> Result<DeleteReceipt, Error> {
        validate_collection_name(collection)?;
        validate_key(key)?;
        let operation_id = Some(hex16(&client_operation_id()?));
        let resp = self.call_keyed(
            collection,
            key,
            RpcRequest {
                op: "delete".into(),
                collection: Some(collection.into()),
                key: Some(key.into()),
                durability: Some(durability_name(options.durability).into()),
                operation_id,
                ..base_req("delete")
            },
        )?;
        delete_receipt_from_resp(key, &resp, options.durability)
    }

    /// Put raw bytes (base64 on the wire; multi-hop routed).
    pub fn put_bytes(
        &mut self,
        collection: &str,
        key: &str,
        bytes: &[u8],
        options: PutOptions,
    ) -> Result<WriteReceipt, Error> {
        validate_collection_name(collection)?;
        validate_key(key)?;
        let operation_id = Some(hex16(&client_operation_id()?));
        let resp = self.call_keyed(
            collection,
            key,
            RpcRequest {
                op: "put_bytes".into(),
                collection: Some(collection.into()),
                key: Some(key.into()),
                bytes_b64: Some(b64_encode(bytes)),
                durability: Some(durability_name(options.durability).into()),
                operation_id,
                ..base_req("put_bytes")
            },
        )?;
        write_receipt_from_resp(key, &resp, options.durability)
    }

    /// Get raw bytes (multi-hop routed).
    pub fn get_bytes(&mut self, collection: &str, key: &str) -> Result<Option<Vec<u8>>, Error> {
        validate_collection_name(collection)?;
        validate_key(key)?;
        let resp = self.call_keyed(
            collection,
            key,
            RpcRequest {
                op: "get_bytes".into(),
                collection: Some(collection.into()),
                key: Some(key.into()),
                ..base_req("get_bytes")
            },
        )?;
        if resp.found == Some(false) {
            return Ok(None);
        }
        match resp.bytes_b64 {
            None => Ok(None),
            Some(s) => Ok(Some(b64_decode(&s)?)),
        }
    }

    /// List keys in a collection.
    pub fn list_keys(&mut self, collection: &str) -> Result<Vec<String>, Error> {
        validate_collection_name(collection)?;
        let resp = self.call(RpcRequest {
            op: "list_keys".into(),
            collection: Some(collection.into()),
            ..base_req("list_keys")
        })?;
        Ok(resp.keys.unwrap_or_default())
    }

    /// Scan JSON rows (optional limit).
    pub fn scan_json(
        &mut self,
        collection: &str,
        limit: Option<usize>,
    ) -> Result<Vec<(String, JsonValue)>, Error> {
        validate_collection_name(collection)?;
        let resp = self.call(RpcRequest {
            op: "scan_json".into(),
            collection: Some(collection.into()),
            limit,
            ..base_req("scan_json")
        })?;
        Ok(resp
            .rows
            .unwrap_or_default()
            .into_iter()
            .map(|r| (r.key, r.value))
            .collect())
    }

    /// Per-key history (DX_SPEC §10.1; multi-hop routed).
    pub fn history(&mut self, collection: &str, key: &str) -> Result<KeyHistory, Error> {
        validate_collection_name(collection)?;
        validate_key(key)?;
        let resp = self.call_keyed(
            collection,
            key,
            RpcRequest {
                op: "history".into(),
                collection: Some(collection.into()),
                key: Some(key.into()),
                ..base_req("history")
            },
        )?;
        let versions = resp
            .versions
            .unwrap_or_default()
            .into_iter()
            .map(history_version_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(KeyHistory {
            key: resp.key.unwrap_or_else(|| key.to_string()),
            versions,
            has_known_holes: resp.has_known_holes.unwrap_or(false),
        })
    }

    /// List secondary indexes on a collection.
    pub fn index_list(&mut self, collection: &str) -> Result<Vec<IndexInfo>, Error> {
        validate_collection_name(collection)?;
        let resp = self.call(RpcRequest {
            op: "index_list".into(),
            collection: Some(collection.into()),
            ..base_req("index_list")
        })?;
        resp.indexes
            .unwrap_or_default()
            .into_iter()
            .map(IndexInfoRow::into_info)
            .collect()
    }

    /// Create (or rebuild-by-create) a secondary field index.
    pub fn index_create(
        &mut self,
        collection: &str,
        name: &str,
        fields: &[&str],
    ) -> Result<IndexInfo, Error> {
        validate_collection_name(collection)?;
        if name.is_empty() {
            return Err(Error::InvalidKey("index name empty"));
        }
        let resp = self.call(RpcRequest {
            op: "index_create".into(),
            collection: Some(collection.into()),
            key: Some(name.into()),
            fields: Some(fields.iter().map(|s| (*s).to_string()).collect()),
            ..base_req("index_create")
        })?;
        let mut rows = resp.indexes.unwrap_or_default();
        let row = rows.pop().ok_or_else(|| {
            Error::Internal("index_create response missing index metadata".into())
        })?;
        row.into_info()
    }

    /// Drop a secondary index by name.
    pub fn index_drop(&mut self, collection: &str, name: &str) -> Result<(), Error> {
        validate_collection_name(collection)?;
        let _ = self.call(RpcRequest {
            op: "index_drop".into(),
            collection: Some(collection.into()),
            key: Some(name.into()),
            ..base_req("index_drop")
        })?;
        Ok(())
    }

    /// Rebuild an existing secondary index from live data.
    pub fn index_rebuild(&mut self, collection: &str, name: &str) -> Result<IndexInfo, Error> {
        validate_collection_name(collection)?;
        let resp = self.call(RpcRequest {
            op: "index_rebuild".into(),
            collection: Some(collection.into()),
            key: Some(name.into()),
            ..base_req("index_rebuild")
        })?;
        let mut rows = resp.indexes.unwrap_or_default();
        let row = rows.pop().ok_or_else(|| {
            Error::Internal("index_rebuild response missing index metadata".into())
        })?;
        row.into_info()
    }

    /// Completeness-aware payload read (chunked values; FORMAT_SPEC §8; multi-hop).
    pub fn get_payload(
        &mut self,
        collection: &str,
        key: &str,
    ) -> Result<Option<PayloadResult>, Error> {
        validate_collection_name(collection)?;
        validate_key(key)?;
        let resp = self.call_keyed(
            collection,
            key,
            RpcRequest {
                op: "get_payload".into(),
                collection: Some(collection.into()),
                key: Some(key.into()),
                ..base_req("get_payload")
            },
        )?;
        if resp.found == Some(false) {
            return Ok(None);
        }
        Ok(Some(payload_from_resp(&resp)?))
    }

    /// Fetch the server's partition directory snapshot (Stage 8d).
    ///
    /// Single-node servers advertise themselves as leader of every virtual
    /// partition so clients can cache routes uniformly with multi-node clusters.
    pub fn fetch_directory(&mut self) -> Result<DirectorySnapshot, Error> {
        let resp = self.call(base_req("directory"))?;
        resp.directory
            .ok_or_else(|| Error::Internal("directory response missing directory payload".into()))
    }

    /// Server-side find with optional index acceleration.
    pub fn find(
        &mut self,
        collection: &str,
        filter: &Filter,
        options: &QueryOptions,
    ) -> Result<Vec<(String, JsonValue)>, Error> {
        validate_collection_name(collection)?;
        let (order_field, order_dir) = match &options.order_by {
            Some((f, SortOrder::Asc)) => (Some(f.clone()), Some("asc".into())),
            Some((f, SortOrder::Desc)) => (Some(f.clone()), Some("desc".into())),
            None => (None, None),
        };
        let max_docs = options.budget.as_ref().and_then(|b| b.max_docs_scanned);
        let max_bytes = options.budget.as_ref().and_then(|b| b.max_bytes_scanned);
        let max_result = options.budget.as_ref().and_then(|b| b.max_result_bytes);
        let resp = self.call(RpcRequest {
            op: "find".into(),
            collection: Some(collection.into()),
            json: Some(filter.to_json()),
            limit: options.limit,
            force_scan: if options.force_scan { Some(true) } else { None },
            order_field,
            order_dir,
            max_docs_scanned: max_docs,
            max_bytes_scanned: max_bytes,
            max_result_bytes: max_result,
            ..base_req("find")
        })?;
        Ok(resp
            .rows
            .unwrap_or_default()
            .into_iter()
            .map(|r| (r.key, r.value))
            .collect())
    }
}

fn history_version_from_row(row: HistoryVersionRow) -> Result<Version, Error> {
    let kind = match row.kind.as_str() {
        "put" => "put",
        "delete" => "delete",
        other => {
            return Err(Error::Internal(format!(
                "unknown history kind from remote: {other}"
            )))
        }
    };
    let body = match row.body_b64 {
        Some(s) => Some(b64_decode(&s)?),
        None => None,
    };
    Ok(Version {
        kind,
        event_id: row.event_id,
        item_id: row.item_id,
        segment_id: row.segment_id,
        json: row.json,
        body,
        known_gap_before: row.known_gap_before,
    })
}

fn base_req(op: &str) -> RpcRequest {
    RpcRequest {
        id: 0,
        op: op.into(),
        collection: None,
        key: None,
        json: None,
        bytes_b64: None,
        limit: None,
        durability: None,
        token: None,
        fields: None,
        force_scan: None,
        order_field: None,
        order_dir: None,
        max_docs_scanned: None,
        max_bytes_scanned: None,
        max_result_bytes: None,
        operation_id: None,
        confirm: None,
    }
}

/// Mint a fresh client operation id via OS CSPRNG (DEF-025; fail closed).
fn client_operation_id() -> Result<[u8; 16], Error> {
    random_id().map_err(Error::from)
}

fn payload_from_resp(resp: &RpcResponse) -> Result<PayloadResult, Error> {
    let status = resp
        .payload_status
        .as_deref()
        .ok_or_else(|| Error::Internal("get_payload response missing payload_status".into()))?;
    match status {
        "complete" => {
            let b64 = resp
                .bytes_b64
                .as_deref()
                .ok_or_else(|| Error::Internal("complete payload missing bytes_b64".into()))?;
            Ok(PayloadResult::Complete {
                body: b64_decode(b64)?,
            })
        }
        "partial" => {
            let content_hash = parse_hex32(resp.content_hash_hex.as_deref())?;
            let missing = resp.missing.clone().unwrap_or_default();
            let extents = resp
                .extents
                .as_ref()
                .map(|rows| {
                    rows.iter()
                        .map(|e| LogicalExtent {
                            index: e.index,
                            range: ByteRange::new(e.start, e.end),
                            present: e.present,
                        })
                        .collect()
                })
                .unwrap_or_default();
            let mut present_bodies = Vec::new();
            for row in resp.present_chunks.as_ref().into_iter().flatten() {
                present_bodies.push((row.index, b64_decode(&row.bytes_b64)?));
            }
            Ok(PayloadResult::Partial {
                extents,
                missing,
                content_hash,
                present_bodies,
            })
        }
        "unavailable" => {
            let content_hash = parse_hex32(resp.content_hash_hex.as_deref())?;
            Ok(PayloadResult::Unavailable {
                content_hash,
                total_chunks: resp.total_chunks.unwrap_or(0),
            })
        }
        "conflicting" => Ok(PayloadResult::Conflicting {
            index: resp.conflict_index.unwrap_or(0),
        }),
        other => Err(Error::Internal(format!(
            "unknown payload_status from remote: {other}"
        ))),
    }
}

fn parse_hex32(s: Option<&str>) -> Result<[u8; 32], Error> {
    let Some(s) = s else {
        return Ok([0u8; 32]);
    };
    if s.len() != 64 {
        return Err(Error::Internal(format!(
            "expected 64 hex chars for content hash, got {}",
            s.len()
        )));
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        let byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|e| Error::Internal(format!("invalid content hash hex: {e}")))?;
        out[i] = byte;
    }
    Ok(out)
}

fn write_receipt_from_resp(
    key: &str,
    resp: &RpcResponse,
    _fallback: DurabilityMode,
) -> Result<WriteReceipt, Error> {
    // DEF-014: never invent stronger guarantees than the server proved.
    let _ = _fallback;
    let acknowledgement = require_durability(resp.acknowledgement.as_deref(), "acknowledgement")?;
    let committed = resp.committed.ok_or_else(|| {
        Error::ProtocolViolation("write receipt missing required field `committed`".into())
    })?;
    let event_id = require_hex16(resp.event_id.as_deref(), "event_id")?;
    // Require version field for protocol completeness; OCC public version is
    // always the establishing event id (aligned with embedded + server put).
    let _wire_version = require_hex16(resp.version.as_deref(), "version")?;
    let store_id = require_hex16(resp.store_id.as_deref(), "store_id")?;
    let segment_id = require_hex16(resp.segment_id.as_deref(), "segment_id")?;
    if event_id == [0u8; 16] {
        return Err(Error::ProtocolViolation(
            "write receipt event_id must not be the zero id".into(),
        ));
    }
    Ok(WriteReceipt {
        // Request key is authoritative when the server omits the echo.
        key: resp.key.clone().unwrap_or_else(|| key.to_string()),
        event_id,
        version: event_id,
        acknowledgement,
        committed,
        store_id,
        segment_id,
    })
}

fn delete_receipt_from_resp(
    key: &str,
    resp: &RpcResponse,
    _fallback: DurabilityMode,
) -> Result<DeleteReceipt, Error> {
    let _ = _fallback;
    let acknowledgement = require_durability(resp.acknowledgement.as_deref(), "acknowledgement")?;
    let committed = resp.committed.ok_or_else(|| {
        Error::ProtocolViolation("delete receipt missing required field `committed`".into())
    })?;
    let removed = resp.removed.ok_or_else(|| {
        Error::ProtocolViolation("delete receipt missing required field `removed`".into())
    })?;
    let event_id = require_hex16(resp.event_id.as_deref(), "event_id")?;
    let _wire_version = require_hex16(resp.version.as_deref(), "version")?;
    let store_id = require_hex16(resp.store_id.as_deref(), "store_id")?;
    let segment_id = require_hex16(resp.segment_id.as_deref(), "segment_id")?;
    if event_id == [0u8; 16] {
        return Err(Error::ProtocolViolation(
            "delete receipt event_id must not be the zero id".into(),
        ));
    }
    Ok(DeleteReceipt {
        key: resp.key.clone().unwrap_or_else(|| key.to_string()),
        removed,
        event_id,
        version: event_id,
        acknowledgement,
        committed,
        store_id,
        segment_id,
    })
}

/// Require a present, well-formed 16-byte hex id (DEF-014: no zero defaults).
fn require_hex16(s: Option<&str>, field: &str) -> Result<[u8; 16], Error> {
    let Some(s) = s else {
        return Err(Error::ProtocolViolation(format!(
            "missing required id field `{field}`"
        )));
    };
    parse_hex16_strict(s, field)
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

/// Parse optional hex id for non-receipt diagnostics (legacy tests only).
#[cfg(test)]
fn parse_hex16(s: Option<&str>) -> Result<[u8; 16], Error> {
    match s {
        None => Err(Error::ProtocolViolation("missing hex id".into())),
        Some(s) => parse_hex16_strict(s, "id"),
    }
}

fn require_durability(s: Option<&str>, field: &str) -> Result<DurabilityMode, Error> {
    match parse_durability(s) {
        Some(m) => Ok(m),
        None => Err(Error::ProtocolViolation(format!(
            "missing or unknown durability field `{field}` (got {s:?})"
        ))),
    }
}

/// Open a client transport (TCP + optional TLS).
fn open_client_stream(addr: &str, options: &ConnectOptions) -> Result<IoStream, Error> {
    let tcp = tcp_connect_with_retry(addr, options)?;
    match &options.tls {
        Some(tls) => {
            let (stream, _peer) = client_connect(tcp, tls)?;
            Ok(stream)
        }
        None => Ok(IoStream::plain(tcp)),
    }
}

/// Client handshake over a buffered duplex stream (single ownership).
fn client_handshake_buffered(reader: &mut BufReader<IoStream>) -> Result<NegotiatedSession, Error> {
    write_json_frame(reader.get_mut(), &Handshake::hello())?;
    let payload = match read_frame(reader, HANDSHAKE_MAX_FRAME_BYTES)? {
        Some(p) => p,
        None => {
            return Err(Error::ProtocolViolation(
                "server closed connection during handshake".into(),
            ));
        }
    };
    let hs = parse_handshake(&payload)?;
    match hs.msg {
        HandshakeMsg::Welcome => {
            let max_frame = negotiate_max_frame(hs.max_frame);
            let features = hs.features.unwrap_or_default();
            negotiate_features(&features)?;
            Ok(NegotiatedSession {
                max_frame,
                features,
                protocol_major: hs.protocol_major.unwrap_or(hs.v),
                protocol_minor: hs.protocol_minor.unwrap_or(0),
            })
        }
        HandshakeMsg::Reject => {
            let code = hs.code.unwrap_or_else(|| "protocol_violation".into());
            let message = hs
                .error
                .unwrap_or_else(|| "server rejected protocol handshake".into());
            Err(match code.as_str() {
                "resource_limit" => Error::ResourceLimit(message),
                "authentication_failed" => Error::AuthenticationFailed(message),
                "protocol_version_unsupported" | "protocol_violation" => {
                    Error::ProtocolViolation(message)
                }
                _ => Error::Remote { code, message },
            })
        }
        other => Err(Error::ProtocolViolation(format!(
            "expected welcome or reject, got {other:?}"
        ))),
    }
}

fn decode_rpc_response(bytes: &[u8], endpoint: &str, addr: &str) -> Result<RpcResponse, Error> {
    let resp: RpcResponse = serde_json::from_slice(bytes).map_err(|e| {
        Error::Internal(format!(
            "invalid rpc response from {endpoint} ({addr}): {e}"
        ))
    })?;
    if !resp.ok {
        let code = resp.code.unwrap_or_else(|| "internal".into());
        let message = resp
            .error
            .unwrap_or_else(|| "remote operation failed".into());
        return Err(match code.as_str() {
            "authentication_failed" => Error::AuthenticationFailed(message),
            "permission_denied" => Error::PermissionDenied(message),
            "deadline_exceeded" => Error::DeadlineExceeded(message),
            "query_budget_required" => Error::QueryBudgetRequired(message),
            "query_invalid" => Error::QueryInvalid(message),
            "consistency_violation" => Error::ConsistencyViolation(message),
            "coverage_incomplete" => Error::CoverageIncomplete(message),
            "resource_limit" => Error::ResourceLimit(message),
            "protocol_violation" => Error::ProtocolViolation(message),
            "durability_unavailable" => Error::DurabilityUnavailable(message),
            // DEF-037: typed fence / leadership errors trigger client directory refresh.
            "stale_route" | "stale_epoch" | "not_leader" => Error::StaleRoute {
                partition: 0,
                message,
            },
            "partition_unavailable" => Error::PartitionUnavailable {
                partition: 0,
                reason: "remote partition unavailable",
            },
            _ => Error::Remote { code, message },
        });
    }
    Ok(resp)
}

/// Whether an error is a transport / connection failure worth re-routing.
fn is_transport_error(err: &Error) -> bool {
    match err {
        Error::DeadlineExceeded(_) | Error::Io(_) => true,
        Error::Internal(msg) => {
            msg.contains("remote closed")
                || msg.contains("Connection refused")
                || msg.contains("Broken pipe")
                || msg.contains("os error")
                || msg.contains("timed out")
                || msg.contains("reset")
        }
        Error::Store(se) if se.is_io() => true,
        _ => {
            let s = err.to_string();
            s.contains("Connection refused")
                || s.contains("Broken pipe")
                || s.contains("Connection reset")
        }
    }
}

/// Transport failure **or** typed leadership/epoch fence (DEF-037).
fn is_route_refresh_error(err: &Error) -> bool {
    if is_transport_error(err) {
        return true;
    }
    match err {
        Error::StaleRoute { .. } | Error::PartitionUnavailable { .. } => true,
        Error::Remote { code, .. } => matches!(
            code.as_str(),
            "stale_route" | "stale_epoch" | "not_leader" | "partition_unavailable"
        ),
        _ => false,
    }
}

fn tcp_connect_with_retry(addr: &str, options: &ConnectOptions) -> Result<TcpStream, Error> {
    let attempts = options.max_connect_attempts.max(1);
    let mut last_err: Option<Error> = None;
    for attempt in 0..attempts {
        match tcp_connect_once(addr, options.connect_timeout) {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                last_err = Some(e);
                if attempt + 1 < attempts {
                    thread::sleep(options.retry_backoff);
                }
            }
        }
    }
    Err(last_err
        .unwrap_or_else(|| Error::Internal(format!("connect failed with no attempts: {addr}"))))
}

fn tcp_connect_once(addr: &str, timeout: Duration) -> Result<TcpStream, Error> {
    let mut addrs = addr.to_socket_addrs().map_err(Error::from_io)?;
    let mut last_io: Option<std::io::Error> = None;
    for sa in addrs.by_ref() {
        match TcpStream::connect_timeout(&sa, timeout) {
            Ok(s) => return Ok(s),
            Err(e) => last_io = Some(e),
        }
    }
    Err(Error::from_io(last_io.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("no addresses resolved for {addr}"),
        )
    })))
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

/// Parsed `residiuum://` URL (Stage 7 single-seed; Stage 8d multi-seed).
///
/// Forms:
/// - `residiuum://host:port[/label]`
/// - `residiuum://h1:p1,h2:p2,h3:p3[/label]` (comma-separated seeds)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedResidiuumUrl {
    /// Seed endpoints as `host:port` (at least one).
    pub seeds: Vec<String>,
    /// Optional path label (informational for Stage 7/8d).
    pub label: Option<String>,
}

/// Parse a `residiuum://host:port[/path]` or multi-seed URL.
pub fn parse_residiuum_url(url: &str) -> Result<ParsedResidiuumUrl, Error> {
    let rest = url
        .strip_prefix("residiuum://")
        .ok_or_else(|| Error::ValidationMsg("URL must start with residiuum://".into()))?;
    if rest.is_empty() {
        return Err(Error::ValidationMsg("empty residiuum:// URL".into()));
    }
    let (hosts_part, label) = match rest.split_once('/') {
        Some((hp, path)) => {
            let label = path.trim_matches('/');
            (
                hp,
                if label.is_empty() {
                    None
                } else {
                    Some(label.to_string())
                },
            )
        }
        None => (rest, None),
    };
    if hosts_part.is_empty() {
        return Err(Error::ValidationMsg("residiuum:// URL missing host".into()));
    }
    let mut seeds = Vec::new();
    for part in hosts_part.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let hostport = if part.contains(':') {
            part.to_string()
        } else {
            format!("{part}:{DEFAULT_PORT}")
        };
        seeds.push(hostport);
    }
    if seeds.is_empty() {
        return Err(Error::ValidationMsg("residiuum:// URL has no seeds".into()));
    }
    Ok(ParsedResidiuumUrl { seeds, label })
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn b64_roundtrip() {
        let samples: &[&[u8]] = &[b"", b"f", b"fo", b"foo", b"foob", b"fooba", b"foobar"];
        for s in samples {
            let enc = b64_encode(s);
            let dec = b64_decode(&enc).unwrap();
            assert_eq!(&dec, s);
        }
    }

    #[test]
    fn parse_url() {
        let p = parse_residiuum_url("residiuum://localhost:7434/app").unwrap();
        assert_eq!(p.seeds, vec!["localhost:7434".to_string()]);
        assert_eq!(p.label.as_deref(), Some("app"));
        let p = parse_residiuum_url("residiuum://127.0.0.1").unwrap();
        assert_eq!(p.seeds, vec!["127.0.0.1:7434".to_string()]);
        assert!(p.label.is_none());
        let p = parse_residiuum_url("residiuum://a:1,b:2,c:3/app").unwrap();
        assert_eq!(
            p.seeds,
            vec!["a:1".to_string(), "b:2".to_string(), "c:3".to_string()]
        );
        assert_eq!(p.label.as_deref(), Some("app"));
    }

    #[test]
    fn hex16_roundtrip() {
        let id = [
            0xde, 0xad, 0xbe, 0xef, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
        ];
        let s = hex16(&id);
        assert_eq!(s.len(), 32);
        assert_eq!(parse_hex16(Some(&s)).unwrap(), id);
        assert!(parse_hex16(None).is_err());
    }

    #[test]
    fn write_receipt_fails_closed_without_committed() {
        let resp = RpcResponse {
            id: 1,
            ok: true,
            event_id: Some(hex16(&[1u8; 16])),
            version: Some(hex16(&[2u8; 16])),
            store_id: Some(hex16(&[3u8; 16])),
            segment_id: Some(hex16(&[4u8; 16])),
            acknowledgement: Some("durable".into()),
            committed: None,
            key: Some("k".into()),
            ..empty_resp(1)
        };
        let err = write_receipt_from_resp("k", &resp, DurabilityMode::Durable).unwrap_err();
        assert_eq!(err.code(), crate::ErrorCode::ProtocolViolation);
    }

    #[test]
    fn write_receipt_rejects_optimistic_durability_fallback() {
        let resp = RpcResponse {
            id: 1,
            ok: true,
            event_id: Some(hex16(&[1u8; 16])),
            version: Some(hex16(&[2u8; 16])),
            store_id: Some(hex16(&[3u8; 16])),
            segment_id: Some(hex16(&[4u8; 16])),
            acknowledgement: None,
            committed: Some(true),
            key: Some("k".into()),
            ..empty_resp(1)
        };
        let err = write_receipt_from_resp("k", &resp, DurabilityMode::Durable).unwrap_err();
        assert_eq!(err.code(), crate::ErrorCode::ProtocolViolation);
    }

    #[test]
    fn write_receipt_requires_event_id() {
        let resp = RpcResponse {
            id: 1,
            ok: true,
            event_id: None,
            version: Some(hex16(&[2u8; 16])),
            store_id: Some(hex16(&[3u8; 16])),
            segment_id: Some(hex16(&[4u8; 16])),
            acknowledgement: Some("durable".into()),
            committed: Some(true),
            key: Some("k".into()),
            ..empty_resp(1)
        };
        let err = write_receipt_from_resp("k", &resp, DurabilityMode::Durable).unwrap_err();
        assert_eq!(err.code(), crate::ErrorCode::ProtocolViolation);
    }

    #[test]
    fn connect_options_builder() {
        let o = ConnectOptions::new()
            .auth_token("secret")
            .connect_timeout(Duration::from_millis(100))
            .request_timeout(Duration::from_secs(2))
            .max_connect_attempts(5)
            .retry_backoff(Duration::from_millis(10));
        assert_eq!(o.auth_token.as_deref(), Some("secret"));
        assert_eq!(o.max_connect_attempts, 5);
        assert_eq!(o.connect_timeout, Duration::from_millis(100));
    }
}
