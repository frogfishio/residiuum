//! Residiuum collection SDK (Stages 4 + 6 + 7 + 8d–8e + heap qualification).
//!
//! ## Surfaces
//!
//! | Surface | Feature | Qualified claim |
//! |---------|---------|-----------------|
//! | Flat `Residiuum::open` / `collection(name)` | `legacy-flat-sdk` (**opt-in**) | **No** (not H6) |
//! | `Residiuum::open_deployment` + `Heap` / SubjectV2 | always (package default) | Path for HP-010 / H1 |
//! | `Residiuum::connect_heap` | always | HeapKey remote process ops |
//!
//! Package default is heap-only (CPR-001). Stages 3–9 demos enable `legacy-flat-sdk`
//! or `cluster` explicitly. Gate H6 / `residiuum-heap-v1` never covers the flat path.
//!
//! **License:** MPL-2.0 for the default embedded + remote surface. The optional
//! `cluster` feature depends on AGPL `residiuum-cluster` (in-process multi-node).
//! Network **serve** lives in AGPL `residiuum-server`. Wire framing lives in MIT
//! `residiuum-client` (re-exported here for compatibility).
//!
//! Normative: DX_SPEC §§1–10, §14; DELIVERY_PLAN Stages 4, 6, 7, and 8d–8e;
//! CLUSTER_SPEC §13 (client routing / directory cache), §17 (query coverage);
//! HEAP_SPEC §7.1 / §30.9 (CPR-001).
//!
//! **Product freeze:** [`SDK_API_VERSION`] labels the **flat** collection API
//! surface after Stage 4 + 7 embedded/server parity (DELIVERY_PLAN §7).

#![deny(missing_docs)]

/// Collection SDK API freeze label (DELIVERY_PLAN §7: Collection SDK 1.0).
///
/// Stages 4 + 7 parity are met for the **legacy flat** surface: embedded,
/// `residiuum serve`, and cluster handles share put/get/delete/scan/find/history/
/// indexes. This label does **not** authorize `residiuum-heap-v1` qualification.
pub const SDK_API_VERSION: &str = "1.0";

/// APP-0 frozen public application façade (`residiuum-rust-app-v1`).
///
/// Prefer these types for new application work. Legacy find/filter budgets still
/// use [`filter::QueryBudget`]; Application Core budgets are
/// [`app_v1::QueryBudget`].
pub mod app_v1;
mod claim;
// APP-4: shared predicates + canonical rql-plan-v1 (public modules).
#[cfg(feature = "cluster")]
mod cluster_backend;
#[cfg(feature = "legacy-flat-sdk")]
mod collection;
/// Authenticated query continuation (`residiuum-cursor-v1`) — APP-6 mint/verify.
pub mod cursor_v1;
mod dialects;
mod directory_cache;
/// Bounded async-first application driver for embedded deployments.
pub mod driver;
mod error;
mod filter;
mod heap;
mod history;
mod indexes;
#[cfg(feature = "legacy-flat-sdk")]
mod multi_query;
/// Canonical Application Core logical plan (`rql-plan-v1`) + plan hash.
pub mod plan_v1;
/// Shared total predicate profile (`residiuum-predicate-v1`).
pub mod predicate;
/// Query bytecode v1 — single product runtime (Decision 0 / RQL-X*).
pub mod query_bytecode_v1;
/// Stable bounded read views — APB-6 T1 scaffold.
pub mod read_view_v1;
mod receipt;
mod remote;
mod remote_heap;
mod residiuum;
mod resource;
/// RQL Application Core source → [`plan_v1::RqlPlanV1`] (`rql-app-core-v1`) — APP-5.
pub mod rql_app_core;
#[cfg(feature = "legacy-flat-sdk")]
mod sda_query;
/// SQL-ish+ → Application Core RQL emit/refuse (`residiuum-sql-plus-to-rql-v1`) — Phase 2 scaffold.
pub mod sql_plus;
mod subject;
mod tls;
mod value;

/// Application Core query budget (APP-0). Distinct from legacy [`filter::QueryBudget`].
pub use app_v1::QueryBudget as AppQueryBudget;
pub use app_v1::{
    AddResult, AdminOperation, CollectionClient, CollectionCreateReceipt, CollectionInfo,
    CollectionQuery, ConsistencyEvidence, ConsistencyMode, Continuation, CoverageEvidence,
    CoveragePolicy, CreateCollectionOptions, CreateCollectionResult, DeleteWithOptions,
    FindJsonOptions, HeapClient, HoleEvidence, IndexManager, KeyProfile, Parameters,
    QueryDiagnostics, QueryExplanation, QueryId, QueryPage, QueryRow, QueryRunOptions,
    ReplaceOptions, ScanJsonOptions, ScanJsonPage, UpsertResult, ViewBoundCollection,
    ViewBoundQuery, CURSOR_PROFILE, KEY_PROFILE_RANDOM_V1, PREDICATE_PROFILE, RQL_APP_CORE_PROFILE,
    RQL_PLAN_PROFILE, RUST_APP_PROFILE,
};
pub use cursor_v1::{
    active_cursor_key_ring, derive_vector_lock_key, install_cursor_key_ring, mint as mint_cursor,
    mint_now as mint_cursor_now, parameter_hash as cursor_parameter_hash, verify as verify_cursor,
    CursorKey, CursorKeyRing, CursorKeyRingGuard, CursorLogical,
    VerifyContext as CursorVerifyContext, CURSOR_KEY_MATERIAL_PROFILE,
    MAC_DOMAIN as CURSOR_MAC_DOMAIN, MAX_ACCEPT_AGE_SECONDS, PARAMETER_HASH_DOMAIN,
    PROFILE as CURSOR_V1_PROFILE, SKEW_SECONDS as CURSOR_SKEW_SECONDS,
    TTL_SECONDS as CURSOR_TTL_SECONDS, VECTOR_LOCK_SEED,
};
pub use plan_v1::{
    where_field_eq_param, AggFn, AggregateSpec, CollectionBindings, GroupAggSpec, NullsOrder,
    OrderDir, OrderTerm, PlanBuilder, PlanSource, RqlPlanV1, DEFAULT_PAGE_SIZE, KEY_TIE_BREAK_PATH,
    MAX_ORDER_TERMS, MAX_PAGE_SIZE, MAX_PROJECT_ITEMS, PLAN_ENCODING_PROFILE, PLAN_HASH_DOMAIN,
    PLAN_PROFILE,
};
pub use rql_app_core::{
    compile_app_core, merge_budgets, CompiledAppCore, TextualAfter, APP_CORE_PROFILE,
    DIAG_RQL_FEATURE_UNAVAILABLE, MAX_RQL_SOURCE_BYTES,
};
pub use sql_plus::{
    compile_sql_to_rql, SqlToRqlEmit, SqlToRqlResult, DIAG_SQL_RQL_CONSTRUCT_UNSUPPORTED,
    DIAG_SQL_RQL_PARSE_ERROR, DIAG_SQL_RQL_STATEMENT_UNSUPPORTED, SQL_PLUS_ALIAS, SQL_PLUS_DIALECT,
    SQL_PLUS_PROFILE,
};
// Product query runtime: QVM1 only (Q0.A10). RQB1 removed from the SDK.
pub use claim::{
    flat_collection_claim_language, heap_only_embedded_profile, legacy_flat_sdk_enabled,
    product_claim_language, product_may_advertise_qualified_heap, FLAT_COLLECTION_SURFACE_LABEL,
    LEGACY_FLAT_SDK_FEATURE,
};
#[cfg(feature = "cluster")]
pub use cluster_backend::{ClusterBackend, ClusterFindResult};
#[cfg(feature = "legacy-flat-sdk")]
pub use collection::{
    find_on_store, Collection, DocumentScanPage, IncompleteDocument, JsonScanIter, JsonScanPage,
    KeyScanPage, UndecodableDocument,
};
pub use dialects::{
    compile_dialect, compile_json_value, compile_sda_source, list_builtin_dialects, BuiltinDialect,
    CompiledDialect, CompiledPortable, CompiledSda, DialectInfo, DialectInfoOwned, DialectRegistry,
    QueryDialect, SdaShape, DIALECT_PROFILE,
};
pub use directory_cache::{
    AssignmentWire, CachedRoute, ClientDirectoryCache, DirectorySnapshot, NodeId, PartitionId,
    PartitionMap, PlacementEpoch, Term, HASH_PROFILE_BLAKE3_MOD,
};
pub use error::{Error, ErrorCode};
#[cfg(feature = "legacy-flat-sdk")]
pub use filter::QueryBuilder;
pub use filter::{
    FieldBuilder, Filter, Pred, QueryBudget, QueryOptions, QueryPlan, SortOrder, QUERY_PLAN_PROFILE,
};
pub use heap::{
    CreatedCollection, DecodedJsonCacheStats, Heap, HeapBatch, HeapCollection, HeapConnection,
    HeapStream, ListedCollection, ResidiuumDeployment, SignedCursor,
};
pub use history::{KeyHistory, Version};
pub use indexes::IndexInfo;
#[cfg(feature = "legacy-flat-sdk")]
pub use indexes::{create_index_on_store, mark_indexes_stale, Indexes};
#[cfg(feature = "legacy-flat-sdk")]
pub use multi_query::{map_joined_sda, JoinBuilder, MultiQuery, MULTI_QUERY_PROFILE};
pub use predicate::{
    field, param, CompareOp, Operand, Path as PredPath, PredField, Predicate, Resolve,
    MAX_PATH_SEGMENTS, MAX_PREDICATE_NODES, PREDICATE_PROFILE_V1,
};
pub use query_bytecode_v1::{
    compile_rql_full, compile_where, execute_bytecode, execute_core_rql,
    execute_full_qvm_on_host_with, execute_full_qvm_with, execute_qvm_bytes, execute_rql_full,
    execute_rql_full_on_host_with, execute_rql_full_with, explain_core_source, explain_rql_full,
    explain_rql_full_on_heap, explain_rql_source, lower_core_source, lower_predicate, qvm_hash,
    refuse_full_language_on_core_wire, source_uses_rql_full_constructs, validate_qvm,
    CompiledKernelWhere, CompiledRqlFull, EnrichAttachMode, EnrichCardinality, EnrichLoadEvidence,
    EnrichStepV1, FullPipelineStepV1, HostCapabilities, HostDocument, HostGroupAggregate,
    HostJsonValue, Instruction, OpCode, ProjectExprV1, ProjectItemV1, QueryBytecodeV1,
    RqlFullExecuteOptions, RqlFullPage, WithinStepV1, ATTACH_IR_PROFILE, BYTECODE_PROFILE,
    DIAG_RQL_ENRICH_CARDINALITY, DIAG_RQL_FULL_RESIDUAL, DIAG_RQL_PROJECTION_CONFLICT,
    DIAG_RQL_PROJECT_TYPE, DIAG_RQL_WITHIN_TYPE, EXEC_PROFILE, FULL_EXPLAIN_HASH_DOMAIN,
    KERNEL_PROFILE, MAX_PROJECT_DEPTH, MAX_WITHIN_DEPTH, ORDER_IR_PROFILE, PAGE_IR_PROFILE,
    PROJECT_IR_PROFILE, QVM_MAGIC, QVM_MAX_BLOB_BYTES, QVM_MAX_OPS, QVM_MAX_TOTAL_BYTES,
    RQL_FULL_PROFILE, VM_PROFILE, VM_VERSION,
};
pub use read_view_v1::{
    AuthoritativeFrontier, FrontierDrift, FrontierKind, PinCapability, ReadView, ReadViewInfo,
    ReadViewOptions, ReadViewRetentionBudget, SemanticVersions, READ_VIEW_PROFILE,
};
pub use receipt::{DeleteReceipt, PutOptions, WriteReceipt};
pub use remote::{
    parse_residiuum_url, ConnectOptions, ExtentRow, HistoryVersionRow, IndexInfoRow,
    ParsedResidiuumUrl, PresentChunkRow, RemoteClient, RpcRequest, RpcResponse, ScanRow,
    DEFAULT_PORT,
};
#[cfg(feature = "dangerous-key-export")]
pub use remote_heap::InMemoryHolderKey;
pub use remote_heap::{
    connect_heap, parse_find_wire, parse_scan_json_wire, CredentialError, FindWirePage,
    HeapCredential, HolderSigner, RemoteCreatedCollection, RemoteHeap, RemoteHeapOptions,
    ScanJsonWirePage,
};
pub use residiuum::Residiuum;
/// Re-export cluster coverage / scan types when the `cluster` feature is on.
#[cfg(feature = "cluster")]
pub use residiuum_cluster::{Coverage, FindResult, ScanOptions};
pub use resource::{
    check_json_depth, check_payload_len, check_rpc_line_len, estimate_json_bytes,
    estimate_row_bytes, host_limits, json_depth, CancelToken, ResourceLimits,
    DEFAULT_MAX_JSON_DEPTH, DEFAULT_MAX_PAYLOAD_BYTES, DEFAULT_MAX_RESULT_BYTES,
    DEFAULT_MAX_RPC_LINE_BYTES, RESOURCE_PROFILE,
};
pub use rql_app_core::DIAG_RQL_OFFSET_DISCARD_UNSUPPORTED;
#[cfg(feature = "legacy-flat-sdk")]
pub use sda_query::{eval_sda_program, SdaTextQuery, SDA_QUERY_PROFILE};
pub use subject::{
    collection_prefix, decode_subject, encode_subject, validate_collection_name, validate_key,
    MAX_COLLECTION_NAME_LEN, MAX_KEY_LEN,
};
pub use tls::{
    build_client_config, client_connect, cluster_urn, constant_time_eq, constant_time_str_eq,
    load_certs, load_private_key, node_urn, redact_secret, IoStream, PeerIdentity,
    TlsClientOptions, TlsServerOptions, TlsServerState, CHANNEL_BINDING_EXPORTER_LABEL,
    CLUSTER_URN_PREFIX, NODE_URN_PREFIX, TLS_PROFILE,
};
pub use value::{decode_bytes, decode_json, encode_bytes, encode_json};

// --- MIT residiuum-client protocol (constants + types re-exported; IO maps errors) ---
pub use residiuum_client::{
    Handshake, HandshakeMsg, NegotiatedSession, DEFAULT_MAX_FRAME_BYTES, FEATURE_IDEMPOTENCY_V1,
    FEATURE_JSON_RPC_V1, FEATURE_RECEIPTS_V1, HANDSHAKE_MAX_FRAME_BYTES, PROTOCOL_MAJOR,
    PROTOCOL_MINOR, PROTOCOL_PROFILE, REQUIRED_DELETE_RECEIPT_FIELDS, REQUIRED_FEATURES,
    REQUIRED_WRITE_RECEIPT_FIELDS, RPC_WIRE_LABEL,
};

/// Encode a length-prefixed frame (see `residiuum-client`).
pub fn encode_frame(payload: &[u8]) -> Result<Vec<u8>, Error> {
    residiuum_client::encode_frame(payload).map_err(Error::from)
}

/// Write one framed payload.
pub fn write_frame<W: std::io::Write>(w: &mut W, payload: &[u8]) -> Result<(), Error> {
    residiuum_client::write_frame(w, payload).map_err(Error::from)
}

/// Write a JSON value as one framed message.
pub fn write_json_frame<W: std::io::Write, T: serde::Serialize>(
    w: &mut W,
    value: &T,
) -> Result<(), Error> {
    residiuum_client::write_json_frame(w, value).map_err(Error::from)
}

/// Read one length-prefixed frame.
pub fn read_frame<R: std::io::Read>(r: &mut R, max_frame: usize) -> Result<Option<Vec<u8>>, Error> {
    residiuum_client::read_frame(r, max_frame).map_err(Error::from)
}

/// Read a frame, detecting legacy line protocol.
pub fn read_frame_or_detect_legacy<R: std::io::Read>(
    r: &mut R,
    max_frame: usize,
) -> Result<Option<Vec<u8>>, Error> {
    residiuum_client::read_frame_or_detect_legacy(r, max_frame).map_err(Error::from)
}

/// Parse handshake JSON.
pub fn parse_handshake(bytes: &[u8]) -> Result<Handshake, Error> {
    residiuum_client::parse_handshake(bytes).map_err(Error::from)
}

/// Intersect client features with required set.
pub fn negotiate_features(client_features: &[String]) -> Result<Vec<String>, Error> {
    residiuum_client::negotiate_features(client_features).map_err(Error::from)
}

/// Negotiate max frame size.
pub fn negotiate_max_frame(client_offer: Option<u32>) -> usize {
    residiuum_client::negotiate_max_frame(client_offer)
}

/// Server-side hello/welcome handshake.
pub fn server_handshake<R: std::io::Read, W: std::io::Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<NegotiatedSession, Error> {
    residiuum_client::server_handshake(reader, writer).map_err(Error::from)
}

/// Client-side hello/welcome handshake.
pub fn client_handshake<R: std::io::Read, W: std::io::Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<NegotiatedSession, Error> {
    residiuum_client::client_handshake(reader, writer).map_err(Error::from)
}

/// Write an unsolicited framed reject.
pub fn write_reject_frame<W: std::io::Write>(
    w: &mut W,
    code: &str,
    error: &str,
) -> Result<(), Error> {
    residiuum_client::write_reject_frame(w, code, error).map_err(Error::from)
}

/// Re-export cluster config for [`Residiuum::create_cluster`] (requires `cluster` feature).
#[cfg(feature = "cluster")]
pub use residiuum_cluster::ClusterConfig;

/// Re-export durability modes used on receipts and put options.
pub use residiuum_store::DurabilityMode;
/// Primary checkpoint acceptance/rejection decision.
pub use residiuum_store::IndexCacheDecision;
/// Primary-index startup disposition.
pub use residiuum_store::IndexOpenDisposition;
/// Re-export index lifecycle states (DX_SPEC §8.2).
pub use residiuum_store::IndexState;
/// Re-export chunked payload completeness (FORMAT_SPEC §8).
pub use residiuum_store::PayloadResult;
/// Physical-store open phase timings and bounded inventory I/O counters.
pub use residiuum_store::StoreOpenMetrics;
/// Structured store startup report.
pub use residiuum_store::StoreOpenReport;

/// Build a `serde_json::Value` from a JSON literal (re-export for examples/tests).
pub use serde_json::json;

/// JSON value type used by [`Collection::get`] and filters.
pub type JsonValue = serde_json::Value;
