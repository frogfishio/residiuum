//! Versioned process configuration (DEF-054).
//!
//! Operators load a JSON document (`residiuum-config-v1`), optionally override via
//! environment and CLI flags, then **validate before** opening writers or
//! binding sockets. Secrets are never stored in the file: only environment
//! variable *names* (or external provider refs) appear in the document.
//! Settings are classified as static, restart-required, or dynamic so reload
//! and diagnostics stay honest.
//!
//! Profile: [`CONFIG_PROFILE`].

use crate::admission::AdmissionLimits;
use crate::bind_policy::{host_is_loopback, validate_bind};
use crate::runtime::ServerLimits;
use crate::serve::ServeOptions;
use residiuum_sdk::TlsServerOptions;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;

/// Configuration schema / profile label (DEF-054).
pub const CONFIG_PROFILE: &str = "residiuum-config-v1";

/// Current document `format_version` accepted by this build.
pub const CONFIG_FORMAT_VERSION: u32 = 1;

/// Environment variable used when `serve.token_env` is omitted.
pub const DEFAULT_TOKEN_ENV: &str = "RESIDIUUM_TOKEN";

/// How a setting may change after process start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettingClass {
    /// Fixed for the life of the deployment (identity, paths, profile).
    Static,
    /// Change requires process restart to take effect.
    RestartRequired,
    /// May be reloaded atomically without restart (admission budgets, …).
    Dynamic,
}

/// Typed configuration errors with operator-actionable text.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Filesystem failure while reading or writing config.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// JSON parse / type error.
    #[error("invalid config JSON: {0}")]
    Parse(String),

    /// Schema or field-level validation failure.
    #[error("config validation failed: {code}: {detail}")]
    Validation {
        /// Stable machine-readable code (`missing_field`, `bad_range`, …).
        code: &'static str,
        /// Human-readable explanation.
        detail: String,
    },

    /// Known-dangerous combination refused until the operator corrects it.
    #[error("unsafe configuration: {code}: {detail}")]
    Unsafe {
        /// Stable machine-readable code.
        code: &'static str,
        /// Human-readable explanation and remediation.
        detail: String,
    },

    /// Document format tag / version not supported by this build.
    #[error(
        "unsupported config format {found:?} (version {version}); expected profile {CONFIG_PROFILE} version {CONFIG_FORMAT_VERSION}"
    )]
    UnsupportedFormat {
        /// Value of the `format` field (or `"<missing>"`).
        found: String,
        /// Value of `format_version` (0 if missing).
        version: u32,
    },

    /// Secret env / provider could not be resolved.
    #[error("secret resolution failed: {0}")]
    Secret(String),
}

impl ConfigError {
    fn validation(code: &'static str, detail: impl Into<String>) -> Self {
        Self::Validation {
            code,
            detail: detail.into(),
        }
    }

    fn unsafe_cfg(code: &'static str, detail: impl Into<String>) -> Self {
        Self::Unsafe {
            code,
            detail: detail.into(),
        }
    }
}

/// Top-level versioned configuration document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResidiuumConfigFile {
    /// Must be [`CONFIG_PROFILE`].
    pub format: String,
    /// Document schema version (currently [`CONFIG_FORMAT_VERSION`]).
    pub format_version: u32,
    /// Optional human comment (ignored by validation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Single-node store settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store: Option<StoreConfigSection>,
    /// TCP serve settings (single-node or one cluster node).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serve: Option<ServeConfigSection>,
    /// Multi-node cluster settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<ClusterConfigSection>,
}

/// Store open / durability defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreConfigSection {
    /// Store directory path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    /// Default durability for CLI mutations when not overridden (`memory` /
    /// `buffered` / `durable`). Informational for serve (server uses store
    /// defaults per RPC).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durability_default: Option<String>,
}

/// TLS file paths (no private key material inline).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TlsConfigSection {
    /// PEM certificate chain path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_path: Option<PathBuf>,
    /// PEM private key path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_path: Option<PathBuf>,
    /// Optional client CA bundle for mTLS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_ca_path: Option<PathBuf>,
    /// Expected peer cluster id SAN (`urn:residiuum:cluster:…`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_cluster_id: Option<String>,
}

/// Protocol admission limits (subset serializable as JSON).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdmissionConfigSection {
    /// Process-wide RPCs per second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_max_rps: Option<u32>,
    /// Per-principal RPCs per second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_principal_max_rps: Option<u32>,
    /// Auth failures before lockout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_auth_failures: Option<u32>,
    /// Concurrent expensive ops.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_expensive_concurrent: Option<usize>,
    /// Operation-id replay window capacity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_capacity: Option<usize>,
}

/// Serve / listener configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ServeConfigSection {
    /// Bind address `host:port`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind: Option<String>,
    /// Allow non-loopback plaintext (development only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_insecure_bind: Option<bool>,
    /// Max simultaneous client connections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<usize>,
    /// Idle socket timeout in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_secs: Option<u64>,
    /// Drain timeout in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain_timeout_secs: Option<u64>,
    /// Opt into experimental network cluster (DEF-002).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub experimental_network_cluster: Option<bool>,
    /// Environment variable **name** holding the shared serve token.
    ///
    /// Never put the token value in the config file. Default: [`DEFAULT_TOKEN_ENV`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_env: Option<String>,
    /// Optional external secret provider ref (opaque string; resolved by host).
    ///
    /// Example: `env:RESIDIUUM_TOKEN`, `file:/run/secrets/residiuum_token`.
    /// When set, takes precedence over `token_env` for resolution order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_secret_ref: Option<String>,
    /// TLS material paths.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfigSection>,
    /// Admission budgets (dynamic-reload candidates).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission: Option<AdmissionConfigSection>,
    /// HAR-4: non-product open/token path (`--legacy-token-server`).
    ///
    /// Mutually exclusive with [`Self::qualified_heap_key`]. When true, serve
    /// is labeled non-qualified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_token_server: Option<bool>,
    /// HAR-4: product HeapKey listener (`--qualified-heap-key`).
    ///
    /// Requires TLS + [`Self::deployment_id`]. Mutually exclusive with
    /// [`Self::legacy_token_server`] and shared token auth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qualified_heap_key: Option<bool>,
    /// Canonical deployment UUID for HeapKey challenges (qualified path).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
}

/// Cluster-root settings for `serve-cluster` and honesty checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ClusterConfigSection {
    /// Cluster root directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<PathBuf>,
    /// Dense node index this process serves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_index: Option<u32>,
    /// How many nodes the deployment claims to run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_node_count: Option<u32>,
    /// When true, the operator claims multi-replica durability/replication.
    ///
    /// Refused when `expected_node_count` is missing or less than 3, or when
    /// `serve.experimental_network_cluster` is not enabled — prevents silent
    /// single-copy “replicated” claims (DEF-054).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_replication: Option<bool>,
}

/// Layers that contributed to the effective configuration (diagnostics).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ConfigLayer {
    /// Built-in draft defaults.
    #[default]
    Default,
    /// JSON document.
    File,
    /// Process environment.
    Env,
    /// Explicit CLI / API override.
    Flag,
}

/// One setting in the effective report (secrets redacted).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EffectiveSetting {
    /// Dotted path (`serve.bind`, `serve.token`).
    pub path: String,
    /// Redacted display value.
    pub value: String,
    /// Change class.
    pub class: SettingClass,
    /// Highest-precedence layer that set this value.
    pub source: ConfigLayer,
}

/// Full redacted effective configuration for diagnostics / startup logs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EffectiveConfigReport {
    /// Profile label.
    pub profile: String,
    /// Document format version.
    pub format_version: u32,
    /// Path of the config file when loaded from disk.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_path: Option<String>,
    /// Mode this config is intended for (`serve`, `serve-cluster`, `validate`).
    pub mode: String,
    /// Redacted settings.
    pub settings: Vec<EffectiveSetting>,
    /// Warnings that did not fail validation (informational).
    pub warnings: Vec<String>,
}

/// Validated, resolved configuration ready to apply to [`ServeOptions`].
#[derive(Debug, Clone)]
pub struct ValidatedConfig {
    /// Original document (paths only; no resolved secrets).
    pub document: ResidiuumConfigFile,
    /// Path the document was loaded from, if any.
    pub config_path: Option<PathBuf>,
    /// Resolved bind address.
    pub bind: String,
    /// Resolved store path (serve) when known.
    pub store_path: Option<PathBuf>,
    /// Resolved cluster root when known.
    pub cluster_root: Option<PathBuf>,
    /// Dense node index for cluster serve.
    pub node_index: u32,
    /// Shared token plaintext when resolved (never written back to disk).
    pub auth_token: Option<String>,
    /// Whether non-loopback plaintext is allowed.
    pub allow_insecure_bind: bool,
    /// Experimental network cluster opt-in.
    pub experimental_network_cluster: bool,
    /// Server connection limits.
    pub server_limits: ServerLimits,
    /// Admission limits.
    pub admission_limits: AdmissionLimits,
    /// TLS options when both cert and key are configured.
    pub tls: Option<TlsServerOptions>,
    /// Operator claim of multi-replica replication (validated).
    pub claim_replication: bool,
    /// Expected deployment node count when set.
    pub expected_node_count: Option<u32>,
    /// HAR-4: explicit non-product open/token path.
    pub legacy_token_server: bool,
    /// HAR-4: product qualified HeapKey path.
    pub qualified_heap_key: bool,
    /// Deployment id for HeapKey challenges when qualified.
    pub deployment_id: Option<String>,
    /// Layer provenance for key fields.
    pub sources: ConfigSources,
    /// Non-fatal warnings.
    pub warnings: Vec<String>,
}

/// Provenance of selected fields (for the effective report).
#[derive(Debug, Clone, Default)]
pub struct ConfigSources {
    /// Source of `bind`.
    pub bind: ConfigLayer,
    /// Source of auth token presence.
    pub auth_token: ConfigLayer,
    /// Source of `allow_insecure_bind`.
    pub allow_insecure_bind: ConfigLayer,
    /// Source of `max_connections`.
    pub max_connections: ConfigLayer,
    /// Source of TLS enablement.
    pub tls: ConfigLayer,
    /// Source of experimental cluster flag.
    pub experimental_network_cluster: ConfigLayer,
    /// Source of auth path (legacy vs qualified).
    pub auth_path: ConfigLayer,
}

/// CLI / API overrides applied after the file and env layers.
#[derive(Debug, Clone, Default)]
pub struct ConfigOverrides {
    /// Override bind address.
    pub bind: Option<String>,
    /// Override store path.
    pub store_path: Option<PathBuf>,
    /// Override cluster root.
    pub cluster_root: Option<PathBuf>,
    /// Override node index.
    pub node_index: Option<u32>,
    /// Explicit token (flag); wins over env resolution.
    pub auth_token: Option<String>,
    /// Override allow_insecure_bind when `Some`.
    pub allow_insecure_bind: Option<bool>,
    /// Override max_connections.
    pub max_connections: Option<usize>,
    /// Override experimental network cluster flag.
    pub experimental_network_cluster: Option<bool>,
    /// Override TLS cert path.
    pub tls_cert: Option<PathBuf>,
    /// Override TLS key path.
    pub tls_key: Option<PathBuf>,
    /// Override TLS client CA.
    pub tls_client_ca: Option<PathBuf>,
    /// Override expected cluster id.
    pub tls_cluster_id: Option<String>,
    /// HAR-4 CLI: force legacy open/token path.
    pub legacy_token_server: Option<bool>,
    /// HAR-4 CLI: force qualified HeapKey path.
    pub qualified_heap_key: Option<bool>,
    /// HAR-4 CLI: deployment id for qualified path.
    pub deployment_id: Option<String>,
}

/// Intended use of the validated configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigMode {
    /// Validate only (no bind / open).
    Validate,
    /// Single-node `residiuum serve`.
    Serve,
    /// Multi-node `residiuum serve-cluster`.
    ServeCluster,
}

impl ResidiuumConfigFile {
    /// Minimal valid empty document (defaults only).
    pub fn empty() -> Self {
        Self {
            format: CONFIG_PROFILE.to_string(),
            format_version: CONFIG_FORMAT_VERSION,
            comment: None,
            store: None,
            serve: None,
            cluster: None,
        }
    }

    /// Parse JSON bytes into a document (no semantic validation).
    pub fn from_json_slice(bytes: &[u8]) -> Result<Self, ConfigError> {
        serde_json::from_slice(bytes).map_err(|e| ConfigError::Parse(e.to_string()))
    }

    /// Parse a JSON string.
    pub fn from_json_str(s: &str) -> Result<Self, ConfigError> {
        Self::from_json_slice(s.as_bytes())
    }

    /// Load a JSON document from a filesystem path.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let bytes = fs::read(path)?;
        Self::from_json_slice(&bytes)
    }

    /// Serialize as pretty JSON.
    pub fn to_json_pretty(&self) -> Result<String, ConfigError> {
        serde_json::to_string_pretty(self).map_err(|e| ConfigError::Parse(e.to_string()))
    }
}

/// Load and validate a configuration for `mode`.
///
/// Order of precedence (low → high): built-in defaults, file, environment
/// secret resolution, then `overrides` (CLI flags).
pub fn load_and_validate(
    path: Option<&Path>,
    mode: ConfigMode,
    overrides: ConfigOverrides,
) -> Result<ValidatedConfig, ConfigError> {
    let (document, config_path) = match path {
        Some(p) => (ResidiuumConfigFile::load(p)?, Some(p.to_path_buf())),
        None => (ResidiuumConfigFile::empty(), None),
    };
    validate_document(document, config_path, mode, overrides)
}

/// Validate an already-parsed document with overrides.
pub fn validate_document(
    document: ResidiuumConfigFile,
    config_path: Option<PathBuf>,
    mode: ConfigMode,
    overrides: ConfigOverrides,
) -> Result<ValidatedConfig, ConfigError> {
    if document.format != CONFIG_PROFILE || document.format_version != CONFIG_FORMAT_VERSION {
        return Err(ConfigError::UnsupportedFormat {
            found: if document.format.is_empty() {
                "<missing>".into()
            } else {
                document.format.clone()
            },
            version: document.format_version,
        });
    }

    let mut sources = ConfigSources::default();
    let mut warnings = Vec::new();
    let serve = document.serve.clone().unwrap_or_default();
    let store = document.store.clone();
    let cluster = document.cluster.clone().unwrap_or_default();

    // --- bind ---
    let (bind, bind_src) = match (overrides.bind.clone(), serve.bind.clone()) {
        (Some(b), _) => (b, ConfigLayer::Flag),
        (None, Some(b)) => (b, ConfigLayer::File),
        (None, None) => (
            format!("127.0.0.1:{}", residiuum_sdk::DEFAULT_PORT),
            ConfigLayer::Default,
        ),
    };
    sources.bind = bind_src;
    if bind.trim().is_empty() {
        return Err(ConfigError::validation(
            "empty_bind",
            "serve.bind must be a non-empty host:port",
        ));
    }

    // --- store / cluster paths ---
    let store_path = overrides
        .store_path
        .clone()
        .or_else(|| store.as_ref().and_then(|s| s.path.clone()));
    let cluster_root = overrides
        .cluster_root
        .clone()
        .or_else(|| cluster.root.clone());
    let node_index = overrides.node_index.or(cluster.node_index).unwrap_or(0);

    match mode {
        ConfigMode::Serve => {
            if store_path.is_none() {
                return Err(ConfigError::validation(
                    "missing_store_path",
                    "serve mode requires store.path in the config file or a store path argument",
                ));
            }
        }
        ConfigMode::ServeCluster => {
            if cluster_root.is_none() {
                return Err(ConfigError::validation(
                    "missing_cluster_root",
                    "serve-cluster mode requires cluster.root in the config file or a cluster path argument",
                ));
            }
        }
        ConfigMode::Validate => {}
    }

    // --- allow_insecure_bind ---
    let (allow_insecure_bind, aib_src) =
        match (overrides.allow_insecure_bind, serve.allow_insecure_bind) {
            (Some(v), _) => (v, ConfigLayer::Flag),
            (None, Some(v)) => (v, ConfigLayer::File),
            (None, None) => (false, ConfigLayer::Default),
        };
    sources.allow_insecure_bind = aib_src;

    // --- experimental cluster ---
    let (experimental_network_cluster, enc_src) = match (
        overrides.experimental_network_cluster,
        serve.experimental_network_cluster,
    ) {
        (Some(v), _) => (v, ConfigLayer::Flag),
        (None, Some(v)) => (v, ConfigLayer::File),
        (None, None) => (false, ConfigLayer::Default),
    };
    sources.experimental_network_cluster = enc_src;

    // --- server limits ---
    let mut server_limits = ServerLimits::draft_defaults();
    let mut max_conn_src = ConfigLayer::Default;
    if let Some(n) = serve.max_connections {
        if n == 0 {
            return Err(ConfigError::validation(
                "bad_range",
                "serve.max_connections must be >= 1",
            ));
        }
        server_limits.max_connections = n;
        max_conn_src = ConfigLayer::File;
    }
    if let Some(n) = overrides.max_connections {
        if n == 0 {
            return Err(ConfigError::validation(
                "bad_range",
                "max_connections override must be >= 1",
            ));
        }
        server_limits.max_connections = n;
        max_conn_src = ConfigLayer::Flag;
    }
    sources.max_connections = max_conn_src;

    if let Some(secs) = serve.idle_timeout_secs {
        if secs == 0 {
            return Err(ConfigError::validation(
                "bad_range",
                "serve.idle_timeout_secs must be >= 1",
            ));
        }
        server_limits.idle_timeout = Duration::from_secs(secs);
    }
    if let Some(secs) = serve.drain_timeout_secs {
        server_limits.drain_timeout = Duration::from_secs(secs);
    }
    server_limits = server_limits.normalized();

    // --- admission ---
    let mut admission_limits = AdmissionLimits::draft_defaults();
    if let Some(a) = &serve.admission {
        if let Some(v) = a.global_max_rps {
            if v == 0 {
                return Err(ConfigError::validation(
                    "bad_range",
                    "serve.admission.global_max_rps must be >= 1",
                ));
            }
            admission_limits.global_max_rps = v;
        }
        if let Some(v) = a.per_principal_max_rps {
            if v == 0 {
                return Err(ConfigError::validation(
                    "bad_range",
                    "serve.admission.per_principal_max_rps must be >= 1",
                ));
            }
            admission_limits.per_principal_max_rps = v;
        }
        if let Some(v) = a.max_auth_failures {
            admission_limits.max_auth_failures = v.max(1);
        }
        if let Some(v) = a.max_expensive_concurrent {
            if v == 0 {
                return Err(ConfigError::validation(
                    "bad_range",
                    "serve.admission.max_expensive_concurrent must be >= 1",
                ));
            }
            admission_limits.max_expensive_concurrent = v;
        }
        if let Some(v) = a.replay_capacity {
            if v == 0 {
                return Err(ConfigError::validation(
                    "bad_range",
                    "serve.admission.replay_capacity must be >= 1",
                ));
            }
            admission_limits.replay_capacity = v;
        }
    }

    // --- durability default (informational) ---
    if let Some(s) = store.as_ref().and_then(|s| s.durability_default.as_ref()) {
        match s.as_str() {
            "memory" | "buffered" | "durable" => {}
            other => {
                return Err(ConfigError::validation(
                    "bad_enum",
                    format!(
                        "store.durability_default must be memory|buffered|durable, got {other:?}"
                    ),
                ));
            }
        }
    }

    // --- TLS ---
    let file_tls = serve.tls.clone().unwrap_or_default();
    let cert = overrides.tls_cert.clone().or(file_tls.cert_path.clone());
    let key = overrides.tls_key.clone().or(file_tls.key_path.clone());
    let client_ca = overrides
        .tls_client_ca
        .clone()
        .or(file_tls.client_ca_path.clone());
    let cluster_id = overrides
        .tls_cluster_id
        .clone()
        .or(file_tls.expected_cluster_id.clone());
    let tls_src = if overrides.tls_cert.is_some() || overrides.tls_key.is_some() {
        ConfigLayer::Flag
    } else if file_tls.cert_path.is_some() || file_tls.key_path.is_some() {
        ConfigLayer::File
    } else {
        ConfigLayer::Default
    };
    sources.tls = tls_src;

    let tls = match (cert, key) {
        (None, None) => {
            if client_ca.is_some() || cluster_id.is_some() {
                return Err(ConfigError::validation(
                    "tls_incomplete",
                    "tls client CA / cluster id require both cert_path and key_path",
                ));
            }
            None
        }
        (Some(c), Some(k)) => {
            if !c.is_file() {
                return Err(ConfigError::validation(
                    "tls_cert_missing",
                    format!(
                        "TLS cert path does not exist or is not a file: {}",
                        c.display()
                    ),
                ));
            }
            if !k.is_file() {
                return Err(ConfigError::validation(
                    "tls_key_missing",
                    format!(
                        "TLS key path does not exist or is not a file: {}",
                        k.display()
                    ),
                ));
            }
            let mut opts = TlsServerOptions::new(c, k);
            if let Some(ca) = client_ca {
                if !ca.is_file() {
                    return Err(ConfigError::validation(
                        "tls_client_ca_missing",
                        format!("TLS client CA path does not exist: {}", ca.display()),
                    ));
                }
                opts = opts.with_client_ca(ca);
            }
            if let Some(id) = cluster_id {
                opts = opts.expected_cluster_id(id);
            }
            Some(opts)
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(ConfigError::validation(
                "tls_incomplete",
                "TLS requires both cert_path and key_path",
            ));
        }
    };
    let tls_enabled = tls.is_some();

    // --- auth token (never from file value) ---
    let (auth_token, token_src) = resolve_auth_token(&serve, &overrides)?;
    sources.auth_token = token_src;

    // --- bind policy (same rules as DEF-002/032) ---
    validate_bind(&bind, allow_insecure_bind, tls_enabled)
        .map_err(|e| ConfigError::unsafe_cfg("insecure_bind", e.to_string()))?;

    // --- replication honesty ---
    let claim_replication = cluster.claim_replication.unwrap_or(false);
    let expected_node_count = cluster.expected_node_count;
    if claim_replication {
        match expected_node_count {
            None => {
                return Err(ConfigError::unsafe_cfg(
                    "replication_claim_without_count",
                    "cluster.claim_replication=true requires cluster.expected_node_count \
                     (refuse silent single-copy replication claims)",
                ));
            }
            Some(n) if n < 3 => {
                return Err(ConfigError::unsafe_cfg(
                    "replication_claim_insufficient_nodes",
                    format!(
                        "cluster.claim_replication=true requires expected_node_count >= 3 \
                         (got {n}); a single local copy is not a replicated deployment"
                    ),
                ));
            }
            Some(_) => {}
        }
        if mode == ConfigMode::ServeCluster && !experimental_network_cluster {
            return Err(ConfigError::unsafe_cfg(
                "replication_claim_without_experimental",
                "cluster.claim_replication=true on serve-cluster requires \
                 serve.experimental_network_cluster=true (DEF-002 honesty gate)",
            ));
        }
        if mode == ConfigMode::Serve {
            return Err(ConfigError::unsafe_cfg(
                "replication_claim_on_single_node",
                "cluster.claim_replication=true is incompatible with single-node serve; \
                 use serve-cluster with multiple voters, or set claim_replication=false",
            ));
        }
    }

    if mode == ConfigMode::ServeCluster && !experimental_network_cluster {
        return Err(ConfigError::unsafe_cfg(
            "serve_cluster_requires_experimental",
            "serve-cluster requires serve.experimental_network_cluster=true \
             (or --experimental-network-cluster)",
        ));
    }

    if allow_insecure_bind
        && !host_is_loopback(
            crate::bind_policy::bind_host(&bind)
                .map_err(|e| ConfigError::validation("bad_bind", e.to_string()))?,
        )
    {
        warnings.push(
            "allow_insecure_bind is set for a non-loopback address: plaintext traffic \
             is development-only and must not be used for production (DEF-002)"
                .into(),
        );
    }

    if auth_token.is_none() && tls.is_none() {
        warnings.push(
            "no shared token and no TLS configured: server runs in open mode \
             (anonymous superuser) — acceptable only for local development"
                .into(),
        );
    }

    // --- HAR-4 T3: auth path (legacy vs qualified) ---
    let (legacy_token_server, qualified_heap_key, deployment_id, auth_path_src) =
        resolve_auth_path(
            &serve,
            &overrides,
            auth_token.is_some(),
            tls.is_some(),
            mode,
            &mut warnings,
        )?;
    sources.auth_path = auth_path_src;

    Ok(ValidatedConfig {
        document,
        config_path,
        bind,
        store_path,
        cluster_root,
        node_index,
        auth_token,
        allow_insecure_bind,
        experimental_network_cluster,
        server_limits,
        admission_limits,
        tls,
        claim_replication,
        expected_node_count,
        legacy_token_server,
        qualified_heap_key,
        deployment_id,
        sources,
        warnings,
    })
}

/// Resolve HAR-4 serve auth path from file + flag layers.
///
/// Co-host of qualified HeapKey and legacy open/token is fail-closed.
fn resolve_auth_path(
    serve: &ServeConfigSection,
    overrides: &ConfigOverrides,
    has_token: bool,
    has_tls: bool,
    mode: ConfigMode,
    warnings: &mut Vec<String>,
) -> Result<(bool, bool, Option<String>, ConfigLayer), ConfigError> {
    // Flag wins over file for each boolean.
    let file_legacy = serve.legacy_token_server.unwrap_or(false);
    let file_qualified = serve.qualified_heap_key.unwrap_or(false);
    let (legacy, legacy_src) = match overrides.legacy_token_server {
        Some(true) => (true, ConfigLayer::Flag),
        Some(false) => (false, ConfigLayer::Flag),
        None if serve.legacy_token_server.is_some() => (file_legacy, ConfigLayer::File),
        None => (false, ConfigLayer::Default),
    };
    let (qualified, qual_src) = match overrides.qualified_heap_key {
        Some(true) => (true, ConfigLayer::Flag),
        Some(false) => (false, ConfigLayer::Flag),
        None if serve.qualified_heap_key.is_some() => (file_qualified, ConfigLayer::File),
        None => (false, ConfigLayer::Default),
    };
    let (deployment_id, dep_src) =
        match (overrides.deployment_id.clone(), serve.deployment_id.clone()) {
            (Some(d), _) => (Some(d), ConfigLayer::Flag),
            (None, Some(d)) => (Some(d), ConfigLayer::File),
            (None, None) => (None, ConfigLayer::Default),
        };
    let mut path_src = match (legacy_src, qual_src) {
        (ConfigLayer::Flag, _) | (_, ConfigLayer::Flag) => ConfigLayer::Flag,
        (ConfigLayer::File, _) | (_, ConfigLayer::File) => ConfigLayer::File,
        _ => dep_src,
    };

    let mut legacy = legacy;
    let qualified = qualified;

    if legacy && qualified {
        return Err(ConfigError::unsafe_cfg(
            "auth_path_cohost",
            "co-host forbidden (HAR-4): serve.legacy_token_server and \
             serve.qualified_heap_key cannot both be true — use only one auth path \
             (or --legacy-token-server / --qualified-heap-key)",
        ));
    }

    if has_token && qualified {
        return Err(ConfigError::unsafe_cfg(
            "token_on_qualified_path",
            "shared token auth is non-qualified (HAR-4): remove serve token / --token \
             when serve.qualified_heap_key is true, or set serve.legacy_token_server=true",
        ));
    }

    // Implicit path resolution for development configs.
    if !legacy && !qualified {
        if has_token {
            legacy = true;
            path_src = ConfigLayer::Default;
            warnings.push(
                "auth path implied legacy-token-server because a shared token is configured \
                 (HAR-4); set serve.legacy_token_server=true explicitly"
                    .into(),
            );
        } else if matches!(mode, ConfigMode::Serve | ConfigMode::ServeCluster) {
            // Preserve Stage-7 open-mode configs with loud honesty; product
            // default remains HeapKey on ServeOptions when apply runs.
            legacy = true;
            path_src = ConfigLayer::Default;
            warnings.push(
                "auth path unset: defaulting config apply to legacy-token-server \
                 (non-qualified open path; HAR-4). Product path requires \
                 serve.qualified_heap_key=true + TLS + serve.deployment_id"
                    .into(),
            );
        }
    }

    if qualified {
        if !has_tls {
            return Err(ConfigError::unsafe_cfg(
                "qualified_requires_tls",
                "serve.qualified_heap_key=true requires TLS (serve.tls.cert_path + key_path \
                 or --tls-cert/--tls-key) (HAR-4)",
            ));
        }
        match &deployment_id {
            Some(d) if !d.trim().is_empty() => {}
            _ => {
                return Err(ConfigError::unsafe_cfg(
                    "qualified_requires_deployment_id",
                    "serve.qualified_heap_key=true requires serve.deployment_id \
                     (canonical deployment UUID) or --deployment-id (HAR-4)",
                ));
            }
        }
    }

    if legacy {
        warnings.push(
            "auth_path=legacy-token-server (non-qualified; not product remote). \
             Product tutorials use connect_heap + qualified HeapKey (HAR-4)."
                .into(),
        );
    }

    Ok((legacy, qualified, deployment_id, path_src))
}

fn resolve_auth_token(
    serve: &ServeConfigSection,
    overrides: &ConfigOverrides,
) -> Result<(Option<String>, ConfigLayer), ConfigError> {
    if let Some(t) = overrides.auth_token.clone() {
        if t.is_empty() {
            return Err(ConfigError::validation(
                "empty_token",
                "auth token override must not be empty when provided",
            ));
        }
        return Ok((Some(t), ConfigLayer::Flag));
    }
    if let Some(ref_name) = serve.token_secret_ref.as_ref() {
        let resolved = resolve_secret_ref(ref_name)?;
        return Ok((Some(resolved), ConfigLayer::Env));
    }
    let env_name = serve.token_env.as_deref().unwrap_or(DEFAULT_TOKEN_ENV);
    match std::env::var(env_name) {
        Ok(v) if !v.is_empty() => Ok((Some(v), ConfigLayer::Env)),
        Ok(_) => Err(ConfigError::Secret(format!(
            "environment variable {env_name} is set but empty"
        ))),
        Err(std::env::VarError::NotPresent) => Ok((None, ConfigLayer::Default)),
        Err(e) => Err(ConfigError::Secret(format!(
            "failed to read environment variable {env_name}: {e}"
        ))),
    }
}

/// Resolve an external secret reference.
///
/// Supported forms:
/// - `env:NAME` — read environment variable `NAME`
/// - `file:PATH` — read and trim a file (for Kubernetes-style secret mounts)
/// - bare `NAME` treated as `env:NAME` when it matches `[A-Za-z_][A-Za-z0-9_]*`
pub fn resolve_secret_ref(spec: &str) -> Result<String, ConfigError> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err(ConfigError::Secret(
            "token_secret_ref must not be empty".into(),
        ));
    }
    if let Some(name) = spec.strip_prefix("env:") {
        let name = name.trim();
        if name.is_empty() {
            return Err(ConfigError::Secret("env: secret ref missing name".into()));
        }
        return std::env::var(name)
            .map_err(|e| {
                ConfigError::Secret(format!("environment variable {name} unavailable: {e}"))
            })
            .and_then(|v| {
                if v.is_empty() {
                    Err(ConfigError::Secret(format!(
                        "environment variable {name} is empty"
                    )))
                } else {
                    Ok(v)
                }
            });
    }
    if let Some(path) = spec.strip_prefix("file:") {
        let path = path.trim();
        if path.is_empty() {
            return Err(ConfigError::Secret("file: secret ref missing path".into()));
        }
        let raw = fs::read_to_string(path)
            .map_err(|e| ConfigError::Secret(format!("failed to read secret file {path}: {e}")))?;
        let v = raw.trim_end_matches(['\r', '\n']).to_string();
        if v.is_empty() {
            return Err(ConfigError::Secret(format!("secret file {path} is empty")));
        }
        return Ok(v);
    }
    // Bare env name.
    if spec
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && spec.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return resolve_secret_ref(&format!("env:{spec}"));
    }
    Err(ConfigError::Secret(format!(
        "unsupported secret ref {spec:?}; use env:NAME or file:PATH"
    )))
}

impl ValidatedConfig {
    /// Apply resolved serve settings onto a [`ServeOptions`] skeleton.
    ///
    /// HAR-4 T3: applies `legacy_token_server` or qualified HeapKey + deployment_id
    /// from the validated config. Callers may still pass a pre-shaped skeleton;
    /// auth path fields from this config win when set.
    pub fn apply_to_serve_options(&self, mut opts: ServeOptions) -> ServeOptions {
        opts = opts
            .allow_insecure_bind(self.allow_insecure_bind)
            .experimental_network_cluster(self.experimental_network_cluster)
            .server_limits(self.server_limits.clone())
            .admission_limits(self.admission_limits.clone());
        if let Some(ref t) = self.auth_token {
            opts = opts.auth_token(t.clone());
        }
        if let Some(ref tls) = self.tls {
            opts = opts.tls(tls.clone());
        }
        if self.cluster_root.is_some() {
            opts = opts.node_index(self.node_index);
        }
        if let Some(ref root) = self.cluster_root {
            opts = opts.cluster_root(root.clone());
        }
        // Auth path (HAR-4 T3).
        if self.legacy_token_server {
            opts = opts.legacy_token_server();
        } else if self.qualified_heap_key {
            opts = opts.qualified_heap_key(true);
            if let Some(ref dep) = self.deployment_id {
                opts = opts.deployment_id(dep.clone());
            }
            // Empty resident registry satisfies listener validation; heaps are
            // admitted by ceremony (HAR-2/3). Same posture as CLI T2.
            if opts.heap_registry.is_none() {
                opts = opts.heap_registry(std::sync::Arc::new(crate::ResidentHeapRegistry::new()));
            }
        }
        opts
    }

    /// Build a redacted effective configuration report for diagnostics.
    pub fn effective_report(&self, mode: ConfigMode) -> EffectiveConfigReport {
        let mode_s = match mode {
            ConfigMode::Validate => "validate",
            ConfigMode::Serve => "serve",
            ConfigMode::ServeCluster => "serve-cluster",
        };
        let mut settings = vec![
            EffectiveSetting {
                path: "serve.bind".into(),
                value: self.bind.clone(),
                class: SettingClass::RestartRequired,
                source: self.sources.bind,
            },
            EffectiveSetting {
                path: "serve.allow_insecure_bind".into(),
                value: self.allow_insecure_bind.to_string(),
                class: SettingClass::RestartRequired,
                source: self.sources.allow_insecure_bind,
            },
            EffectiveSetting {
                path: "serve.max_connections".into(),
                value: self.server_limits.max_connections.to_string(),
                class: SettingClass::RestartRequired,
                source: self.sources.max_connections,
            },
            EffectiveSetting {
                path: "serve.idle_timeout_secs".into(),
                value: self.server_limits.idle_timeout.as_secs().to_string(),
                class: SettingClass::Dynamic,
                source: ConfigLayer::File,
            },
            EffectiveSetting {
                path: "serve.experimental_network_cluster".into(),
                value: self.experimental_network_cluster.to_string(),
                class: SettingClass::RestartRequired,
                source: self.sources.experimental_network_cluster,
            },
            EffectiveSetting {
                path: "serve.tls.enabled".into(),
                value: self.tls.is_some().to_string(),
                class: SettingClass::RestartRequired,
                source: self.sources.tls,
            },
            EffectiveSetting {
                path: "serve.auth_token".into(),
                value: if self.auth_token.is_some() {
                    "[redacted]".into()
                } else {
                    "<unset>".into()
                },
                class: SettingClass::RestartRequired,
                source: self.sources.auth_token,
            },
            EffectiveSetting {
                path: "serve.auth_path".into(),
                value: if self.qualified_heap_key {
                    "qualified-heap-key (product)".into()
                } else if self.legacy_token_server {
                    "legacy-token-server (non-qualified)".into()
                } else {
                    "unset".into()
                },
                class: SettingClass::RestartRequired,
                source: self.sources.auth_path,
            },
            EffectiveSetting {
                path: "serve.legacy_token_server".into(),
                value: self.legacy_token_server.to_string(),
                class: SettingClass::RestartRequired,
                source: self.sources.auth_path,
            },
            EffectiveSetting {
                path: "serve.qualified_heap_key".into(),
                value: self.qualified_heap_key.to_string(),
                class: SettingClass::RestartRequired,
                source: self.sources.auth_path,
            },
            EffectiveSetting {
                path: "serve.deployment_id".into(),
                value: self
                    .deployment_id
                    .clone()
                    .unwrap_or_else(|| "<unset>".into()),
                class: SettingClass::RestartRequired,
                source: self.sources.auth_path,
            },
            EffectiveSetting {
                path: "serve.admission.global_max_rps".into(),
                value: self.admission_limits.global_max_rps.to_string(),
                class: SettingClass::Dynamic,
                source: ConfigLayer::File,
            },
            EffectiveSetting {
                path: "serve.admission.per_principal_max_rps".into(),
                value: self.admission_limits.per_principal_max_rps.to_string(),
                class: SettingClass::Dynamic,
                source: ConfigLayer::File,
            },
            EffectiveSetting {
                path: "cluster.claim_replication".into(),
                value: self.claim_replication.to_string(),
                class: SettingClass::Static,
                source: ConfigLayer::File,
            },
        ];
        if let Some(ref p) = self.store_path {
            settings.push(EffectiveSetting {
                path: "store.path".into(),
                value: p.display().to_string(),
                class: SettingClass::Static,
                source: ConfigLayer::File,
            });
        }
        if let Some(ref p) = self.cluster_root {
            settings.push(EffectiveSetting {
                path: "cluster.root".into(),
                value: p.display().to_string(),
                class: SettingClass::Static,
                source: ConfigLayer::File,
            });
            settings.push(EffectiveSetting {
                path: "cluster.node_index".into(),
                value: self.node_index.to_string(),
                class: SettingClass::Static,
                source: ConfigLayer::File,
            });
        }
        if let Some(n) = self.expected_node_count {
            settings.push(EffectiveSetting {
                path: "cluster.expected_node_count".into(),
                value: n.to_string(),
                class: SettingClass::Static,
                source: ConfigLayer::File,
            });
        }

        EffectiveConfigReport {
            profile: CONFIG_PROFILE.to_string(),
            format_version: CONFIG_FORMAT_VERSION,
            config_path: self.config_path.as_ref().map(|p| p.display().to_string()),
            mode: mode_s.into(),
            settings,
            warnings: self.warnings.clone(),
        }
    }

    /// Dynamic subset that may be reloaded without restart (admission only for now).
    pub fn dynamic_admission_limits(&self) -> AdmissionLimits {
        self.admission_limits.clone()
    }
}

/// Classify a known setting path (for operator docs / tooling).
pub fn setting_class(path: &str) -> Option<SettingClass> {
    match path {
        "store.path"
        | "cluster.root"
        | "cluster.node_index"
        | "cluster.expected_node_count"
        | "cluster.claim_replication" => Some(SettingClass::Static),
        "serve.bind"
        | "serve.allow_insecure_bind"
        | "serve.max_connections"
        | "serve.experimental_network_cluster"
        | "serve.tls"
        | "serve.auth_token"
        | "serve.auth_path"
        | "serve.legacy_token_server"
        | "serve.qualified_heap_key"
        | "serve.deployment_id" => Some(SettingClass::RestartRequired),
        "serve.idle_timeout_secs"
        | "serve.admission.global_max_rps"
        | "serve.admission.per_principal_max_rps"
        | "serve.admission.max_auth_failures"
        | "serve.admission.max_expensive_concurrent"
        | "serve.admission.replay_capacity" => Some(SettingClass::Dynamic),
        _ => None,
    }
}

/// Redact a raw JSON value tree: replace any object key that looks secret.
pub fn redact_json_value(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for k in keys {
                let lower = k.to_ascii_lowercase();
                if lower.contains("token")
                    || lower.contains("secret")
                    || lower.contains("password")
                    || lower.ends_with("_key") && !lower.contains("path") && lower != "key_path"
                {
                    // Keep *paths* and env *names*; redact only obvious values.
                    if lower.ends_with("_env")
                        || lower.ends_with("_path")
                        || lower.ends_with("_ref")
                        || lower == "key_path"
                        || lower == "cert_path"
                    {
                        if let Some(child) = map.get_mut(&k) {
                            redact_json_value(child);
                        }
                        continue;
                    }
                    map.insert(k, serde_json::Value::String("[redacted]".into()));
                } else if let Some(child) = map.get_mut(&k) {
                    redact_json_value(child);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                redact_json_value(item);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    fn write_cfg(dir: &Path, name: &str, body: &str) -> PathBuf {
        let p = dir.join(name);
        let mut f = fs::File::create(&p).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        p
    }

    #[test]
    fn empty_validate_mode_ok() {
        // Point token_env at a name that must not be present so ambient
        // RESIDIUUM_TOKEN in the developer environment cannot fail the test.
        let mut doc = ResidiuumConfigFile::empty();
        doc.serve = Some(ServeConfigSection {
            token_env: Some("RESIDIUUM_TEST_CFG_NO_SUCH_TOKEN".into()),
            ..Default::default()
        });
        let v =
            validate_document(doc, None, ConfigMode::Validate, ConfigOverrides::default()).unwrap();
        assert_eq!(v.bind, format!("127.0.0.1:{}", residiuum_sdk::DEFAULT_PORT));
        assert!(!v.allow_insecure_bind);
        assert!(v.auth_token.is_none());
    }

    #[test]
    fn reject_wrong_format() {
        let mut doc = ResidiuumConfigFile::empty();
        doc.format = "nope".into();
        let err = validate_document(doc, None, ConfigMode::Validate, ConfigOverrides::default())
            .unwrap_err();
        assert!(matches!(err, ConfigError::UnsupportedFormat { .. }));
    }

    #[test]
    fn serve_requires_store_path() {
        let err = validate_document(
            ResidiuumConfigFile::empty(),
            None,
            ConfigMode::Serve,
            ConfigOverrides::default(),
        )
        .unwrap_err();
        match err {
            ConfigError::Validation { code, .. } => assert_eq!(code, "missing_store_path"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn reject_replication_claim_with_one_node() {
        let doc = ResidiuumConfigFile {
            format: CONFIG_PROFILE.into(),
            format_version: 1,
            comment: None,
            store: None,
            serve: Some(ServeConfigSection {
                experimental_network_cluster: Some(true),
                ..Default::default()
            }),
            cluster: Some(ClusterConfigSection {
                root: Some(PathBuf::from("/tmp/c")),
                expected_node_count: Some(1),
                claim_replication: Some(true),
                ..Default::default()
            }),
        };
        let err = validate_document(
            doc,
            None,
            ConfigMode::ServeCluster,
            ConfigOverrides::default(),
        )
        .unwrap_err();
        match err {
            ConfigError::Unsafe { code, .. } => {
                assert_eq!(code, "replication_claim_insufficient_nodes");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn reject_replication_claim_on_single_node_serve() {
        let doc = ResidiuumConfigFile {
            format: CONFIG_PROFILE.into(),
            format_version: 1,
            comment: None,
            store: Some(StoreConfigSection {
                path: Some(PathBuf::from("/tmp/s")),
                durability_default: None,
            }),
            serve: None,
            cluster: Some(ClusterConfigSection {
                expected_node_count: Some(3),
                claim_replication: Some(true),
                ..Default::default()
            }),
        };
        let err = validate_document(doc, None, ConfigMode::Serve, ConfigOverrides::default())
            .unwrap_err();
        match err {
            ConfigError::Unsafe { code, .. } => {
                assert_eq!(code, "replication_claim_on_single_node");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn reject_insecure_public_bind() {
        let doc = ResidiuumConfigFile {
            format: CONFIG_PROFILE.into(),
            format_version: 1,
            comment: None,
            store: Some(StoreConfigSection {
                path: Some(PathBuf::from("/tmp/s")),
                durability_default: None,
            }),
            serve: Some(ServeConfigSection {
                bind: Some("0.0.0.0:7434".into()),
                allow_insecure_bind: Some(false),
                ..Default::default()
            }),
            cluster: None,
        };
        let err = validate_document(doc, None, ConfigMode::Serve, ConfigOverrides::default())
            .unwrap_err();
        assert!(matches!(err, ConfigError::Unsafe { code, .. } if code == "insecure_bind"));
    }

    #[test]
    fn load_file_and_apply_options() {
        let dir = tempdir().unwrap();
        let path = write_cfg(
            dir.path(),
            "residiuum.json",
            r#"{
              "format": "residiuum-config-v1",
              "format_version": 1,
              "store": { "path": "/data/store", "durability_default": "durable" },
              "serve": {
                "bind": "127.0.0.1:9000",
                "max_connections": 16,
                "admission": { "global_max_rps": 100 }
              }
            }"#,
        );
        let v =
            load_and_validate(Some(&path), ConfigMode::Serve, ConfigOverrides::default()).unwrap();
        assert_eq!(v.bind, "127.0.0.1:9000");
        assert_eq!(v.server_limits.max_connections, 16);
        assert_eq!(v.admission_limits.global_max_rps, 100);
        let opts = v.apply_to_serve_options(ServeOptions::new().legacy_token_server());
        assert_eq!(opts.server_limits.max_connections, 16);
        assert_eq!(opts.admission_limits.global_max_rps, 100);

        let report = v.effective_report(ConfigMode::Serve);
        assert_eq!(report.profile, CONFIG_PROFILE);
        assert!(report
            .settings
            .iter()
            .any(|s| s.path == "serve.auth_token" && s.value == "<unset>"));
    }

    #[test]
    fn secret_ref_env_and_file() {
        std::env::set_var("RESIDIUUM_TEST_CFG_TOKEN", "s3cr3t");
        let v = resolve_secret_ref("env:RESIDIUUM_TEST_CFG_TOKEN").unwrap();
        assert_eq!(v, "s3cr3t");
        std::env::remove_var("RESIDIUUM_TEST_CFG_TOKEN");

        let dir = tempdir().unwrap();
        let secret_path = dir.path().join("tok");
        fs::write(&secret_path, "from-file\n").unwrap();
        let v = resolve_secret_ref(&format!("file:{}", secret_path.display())).unwrap();
        assert_eq!(v, "from-file");
    }

    #[test]
    fn flag_token_wins_over_env() {
        // Flag wins regardless of env; no need to mutate process environment.
        let doc = ResidiuumConfigFile {
            format: CONFIG_PROFILE.into(),
            format_version: 1,
            comment: None,
            store: Some(StoreConfigSection {
                path: Some(PathBuf::from("/tmp/s")),
                durability_default: None,
            }),
            serve: Some(ServeConfigSection {
                token_env: Some("RESIDIUUM_TEST_CFG_NO_SUCH_TOKEN".into()),
                ..Default::default()
            }),
            cluster: None,
        };
        let v = validate_document(
            doc,
            None,
            ConfigMode::Serve,
            ConfigOverrides {
                auth_token: Some("from-flag".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(v.auth_token.as_deref(), Some("from-flag"));
        assert_eq!(v.sources.auth_token, ConfigLayer::Flag);
    }

    #[test]
    fn redact_json_hides_token_values_keeps_paths() {
        let mut v = serde_json::json!({
            "token": "hunter2",
            "token_env": "RESIDIUUM_TOKEN",
            "tls": { "key_path": "/secret/key.pem", "cert_path": "/secret/cert.pem" }
        });
        redact_json_value(&mut v);
        assert_eq!(v["token"], "[redacted]");
        assert_eq!(v["token_env"], "RESIDIUUM_TOKEN");
        assert_eq!(v["tls"]["key_path"], "/secret/key.pem");
    }

    #[test]
    fn setting_class_table() {
        assert_eq!(setting_class("store.path"), Some(SettingClass::Static));
        assert_eq!(
            setting_class("serve.bind"),
            Some(SettingClass::RestartRequired)
        );
        assert_eq!(
            setting_class("serve.admission.global_max_rps"),
            Some(SettingClass::Dynamic)
        );
    }

    #[test]
    fn serve_cluster_requires_experimental() {
        let doc = ResidiuumConfigFile {
            format: CONFIG_PROFILE.into(),
            format_version: 1,
            comment: None,
            store: None,
            serve: None,
            cluster: Some(ClusterConfigSection {
                root: Some(PathBuf::from("/tmp/c")),
                ..Default::default()
            }),
        };
        let err = validate_document(
            doc,
            None,
            ConfigMode::ServeCluster,
            ConfigOverrides::default(),
        )
        .unwrap_err();
        match err {
            ConfigError::Unsafe { code, .. } => {
                assert_eq!(code, "serve_cluster_requires_experimental");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn zero_max_connections_rejected() {
        let doc = ResidiuumConfigFile {
            format: CONFIG_PROFILE.into(),
            format_version: 1,
            comment: None,
            store: Some(StoreConfigSection {
                path: Some(PathBuf::from("/tmp/s")),
                durability_default: None,
            }),
            serve: Some(ServeConfigSection {
                max_connections: Some(0),
                ..Default::default()
            }),
            cluster: None,
        };
        let err = validate_document(doc, None, ConfigMode::Serve, ConfigOverrides::default())
            .unwrap_err();
        match err {
            ConfigError::Validation { code, .. } => assert_eq!(code, "bad_range"),
            other => panic!("unexpected {other:?}"),
        }
    }
}
