//! Residiuum networked server surfaces (AGPL-3.0-or-later).
//!
//! Accept loop, authorization, protocol admission, bind policy, network Raft
//! glue, and TCP serve dispatch. Collection / remote-client APIs remain in
//! `residiuum-sdk`; wire framing is MIT `residiuum-client`.

#![deny(missing_docs)]

mod admission;
mod authz;
mod bind_policy;
mod config;
mod heap_audit;
mod heap_auth;
mod heap_dispatch;
mod heap_registry;
mod heap_session;
mod metrics;
mod raft_server;
mod runtime;
mod serve;
mod slog;

pub use admission::{
    is_expensive_op, is_replayable_mutation, AdmissionController, AdmissionLimits, AdmissionStats,
    ExpensiveGuard, ReplayStatus, ADMISSION_PROFILE, DEFAULT_AUTH_FAILURE_WINDOW,
    DEFAULT_AUTH_LOCKOUT, DEFAULT_CONNECT_WINDOW, DEFAULT_GLOBAL_MAX_RPS,
    DEFAULT_MAX_AUTH_FAILURES, DEFAULT_MAX_CONNECTS_PER_WINDOW, DEFAULT_MAX_EXPENSIVE_CONCURRENT,
    DEFAULT_PER_PRINCIPAL_MAX_RPS, DEFAULT_REPLAY_CAPACITY, DEFAULT_REPLAY_TTL, RATE_WINDOW,
};
pub use authz::{
    authorize, bound_label, check_rpc, redact_token, requirement_for_op, AuditDecision, AuditLog,
    AuditRecord, AuthzPolicy, OpRequirement, Principal, PrincipalSpec, Privilege, PrivilegeSet,
    AUTHZ_PROFILE, FORCE_RECONFIG_CONFIRM, MAX_AUDIT_LABEL_LEN, MAX_PRINCIPAL_ID_LEN,
    PURGE_CONFIRM,
};
pub use bind_policy::{
    bind_host, host_is_loopback, validate_bind, validate_plaintext_bind, ServeStartupReport,
};
pub use config::{
    load_and_validate, redact_json_value, resolve_secret_ref, setting_class, validate_document,
    AdmissionConfigSection, ClusterConfigSection, ConfigError, ConfigLayer, ConfigMode,
    ConfigOverrides, EffectiveConfigReport, EffectiveSetting, ResidiuumConfigFile,
    ServeConfigSection, SettingClass, StoreConfigSection, TlsConfigSection, ValidatedConfig,
    CONFIG_FORMAT_VERSION, CONFIG_PROFILE, DEFAULT_TOKEN_ENV,
};
pub use heap_audit::{HeapAuthAuditEvent, HeapAuthAuditLog, DEFAULT_HEAP_AUDIT_CAPACITY};
pub use heap_auth::{
    authenticate_heap_auth, build_challenge, wire_reject, HeapAuthInternalCause, HeapAuthOutcome,
    PendingChallenge, NONCE_TTL,
};
pub use heap_dispatch::{
    dispatch_heap_request, dispatch_heap_request_with, layout_for_root, request_registry_allows,
    HeapDataCtx, HeapDispatchResult, HeapRpcError, HeapRpcRequest, HeapRpcResponse,
    HEAP_UNAVAILABLE,
};
pub use heap_registry::{ResidentHeap, ResidentHeapRegistry};
pub use heap_session::{
    run_qualified_handshake, run_qualified_handshake_buffered, serve_qualified_requests,
    serve_qualified_requests_with_host, validate_qualified_listener, QualifiedHandshakeParams,
    QualifiedHandshakeResult, QualifiedSession,
};
pub use metrics::{
    evaluate_health, is_public_probe_op, AdmissionStatsWire, EdgeMetrics, GuaranteeMetrics,
    HealthEvalInput, HealthReport, HealthStatus, HistogramBucket, MetricsRegistry, MetricsSnapshot,
    OpMetricSnapshot, ServerStatsWire, HEALTH_PROFILE, LATENCY_BUCKET_MS, MAX_OP_LABELS,
    METRICS_PROFILE,
};
pub use raft_server::{
    shared_raft_state, RaftServerState, SharedRaftState, TcpRaftTransport, CLUSTER_COMMIT_PROFILE,
    FEATURE_CLUSTER_COMMIT_V1, FEATURE_RAFT_RPC_V1, SDK_RAFT_RPC_PROFILE,
};
pub use runtime::{
    is_mutating_op, ConnectionGuard, MutationGuard, ServerLimits, ServerRuntime, ServerStats,
    DEFAULT_DRAIN_TIMEOUT, DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_CONNECTIONS, SERVER_PROFILE,
};
pub use serve::{
    handle_connection, handle_connection_shared, handle_connection_with, serve_cluster_node,
    serve_store, serve_store_with, ServeOptions,
};
pub use slog::{
    bound_field, bound_name, events as log_events, log_rpc_complete, redact_credential,
    sanitize_reason, LogEvent, LogLevel, LogSink, Logger, MemorySink, StderrSink, LOG_PROFILE,
    MAX_FIELD_BYTES, MAX_NAME_BYTES,
};
