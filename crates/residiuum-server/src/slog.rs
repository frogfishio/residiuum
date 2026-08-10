//! Structured process logging (DEF-060).
//!
//! Emits versioned NDJSON events with stable event names and bounded field
//! cardinality. Credentials and request/response payloads are never logged.
//!
//! Profile: [`LOG_PROFILE`] = `residiuum-log-v1`.
//!
//! Correlation fields (`operation_id`, `request_id`, `principal_id`,
//! `error_code`, requested/achieved guarantees, latency) let operators join
//! client, server, and replica lines without parsing free-form prose.

use residiuum_sdk::redact_secret;
use serde::Serialize;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Structured logging profile label (capability matrices / startup).
pub const LOG_PROFILE: &str = "residiuum-log-v1";

/// Maximum UTF-8 bytes retained for free-text fields (reason, error message).
pub const MAX_FIELD_BYTES: usize = 256;

/// Maximum length of operation names and error codes after bounding.
pub const MAX_NAME_BYTES: usize = 64;

/// Stable event names for operator tooling and log pipelines.
pub mod events {
    /// Process accepted a bind and is serving.
    pub const SERVER_START: &str = "server.start";
    /// Accept loop drained after shutdown request.
    pub const SERVER_DRAIN: &str = "server.drain";
    /// Inbound connection refused (admission, drain, churn).
    pub const CONNECTION_REJECTED: &str = "connection.rejected";
    /// Connection worker exited with an error (no secrets).
    pub const CONNECTION_ERROR: &str = "connection.error";
    /// One application RPC finished (success or typed failure).
    pub const RPC_COMPLETE: &str = "rpc.complete";
    /// Requested durability/consistency guarantee was not achieved.
    pub const GUARANTEE_FAILED: &str = "guarantee.failed";
    /// Raft control plane attach failed; directory-only mode.
    pub const RAFT_ATTACH_FAILED: &str = "raft.attach_failed";
}

/// Severity for filtering and sinks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    /// Verbose diagnostic (off by default).
    Debug = 0,
    /// Routine operational events.
    Info = 1,
    /// Recoverable problems (rejects, degraded attach).
    Warn = 2,
    /// Failures that broke a request or guarantee.
    Error = 3,
}

impl LogLevel {
    /// Stable lowercase name for NDJSON.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// Where structured log lines go.
pub trait LogSink: Send + Sync {
    /// Emit one complete log line (already serialized NDJSON, no trailing newline required).
    fn emit_line(&self, line: &str);
}

/// Default sink: one NDJSON object per line on stderr.
#[derive(Debug, Default)]
pub struct StderrSink;

impl LogSink for StderrSink {
    fn emit_line(&self, line: &str) {
        let mut err = io::stderr().lock();
        let _ = writeln!(err, "{line}");
    }
}

/// In-memory sink for tests (bounded ring of lines).
#[derive(Debug, Default)]
pub struct MemorySink {
    lines: Mutex<Vec<String>>,
    cap: usize,
}

impl MemorySink {
    /// Create a sink that retains at most `cap` most-recent lines.
    pub fn new(cap: usize) -> Self {
        Self {
            lines: Mutex::new(Vec::new()),
            cap: cap.max(1),
        }
    }

    /// Snapshot of retained lines (oldest first).
    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().map(|g| g.clone()).unwrap_or_default()
    }

    /// Parse retained lines as JSON values (skips corrupt lines).
    pub fn json_lines(&self) -> Vec<serde_json::Value> {
        self.lines()
            .into_iter()
            .filter_map(|l| serde_json::from_str(&l).ok())
            .collect()
    }

    /// Clear retained lines.
    pub fn clear(&self) {
        if let Ok(mut g) = self.lines.lock() {
            g.clear();
        }
    }
}

impl LogSink for MemorySink {
    fn emit_line(&self, line: &str) {
        if let Ok(mut g) = self.lines.lock() {
            if g.len() >= self.cap {
                let drop_n = g.len() + 1 - self.cap;
                g.drain(0..drop_n);
            }
            g.push(line.to_string());
        }
    }
}

/// Process logger: shared sink + default process context.
#[derive(Clone)]
pub struct Logger {
    sink: Arc<dyn LogSink>,
    min_level: LogLevel,
    /// Optional store path (never contains secrets).
    store: Option<String>,
    /// Optional cluster root path.
    cluster: Option<String>,
    /// Dense node index for multi-node serve.
    node_index: Option<u32>,
    /// `serve` or `serve-cluster`.
    mode: Option<&'static str>,
}

impl std::fmt::Debug for Logger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Logger")
            .field("min_level", &self.min_level)
            .field("store", &self.store)
            .field("cluster", &self.cluster)
            .field("node_index", &self.node_index)
            .field("mode", &self.mode)
            .finish()
    }
}

impl Default for Logger {
    fn default() -> Self {
        Self::stderr()
    }
}

impl Logger {
    /// Stderr NDJSON logger at Info+.
    pub fn stderr() -> Self {
        Self {
            sink: Arc::new(StderrSink),
            min_level: LogLevel::Info,
            store: None,
            cluster: None,
            node_index: None,
            mode: None,
        }
    }

    /// Logger writing into a custom sink (tests).
    pub fn with_sink(sink: Arc<dyn LogSink>) -> Self {
        Self {
            sink,
            min_level: LogLevel::Info,
            store: None,
            cluster: None,
            node_index: None,
            mode: None,
        }
    }

    /// Minimum severity that will be emitted.
    pub fn min_level(mut self, level: LogLevel) -> Self {
        self.min_level = level;
        self
    }

    /// Attach default store path context.
    pub fn store(mut self, path: impl Into<String>) -> Self {
        self.store = Some(bound_field(&path.into()));
        self
    }

    /// Attach default cluster root context.
    pub fn cluster(mut self, path: impl Into<String>) -> Self {
        self.cluster = Some(bound_field(&path.into()));
        self
    }

    /// Attach dense node index.
    pub fn node_index(mut self, index: u32) -> Self {
        self.node_index = Some(index);
        self
    }

    /// Attach serve mode label.
    pub fn mode(mut self, mode: &'static str) -> Self {
        self.mode = Some(mode);
        self
    }

    /// Shared reference for `ServeOptions`.
    pub fn shared(self) -> Arc<Logger> {
        Arc::new(self)
    }

    /// Emit a fully built event (applies process defaults + level filter).
    pub fn emit(&self, mut event: LogEvent) {
        let level = parse_level(&event.level);
        if level < self.min_level {
            return;
        }
        // Normalize severity string after filter.
        event.level = level.as_str().to_string();
        if event.store.is_none() {
            event.store = self.store.clone();
        }
        if event.cluster.is_none() {
            event.cluster = self.cluster.clone();
        }
        if event.node_index.is_none() {
            event.node_index = self.node_index;
        }
        if event.mode.is_none() {
            event.mode = self.mode.map(|m| m.to_string());
        }
        let line = event.to_ndjson();
        self.sink.emit_line(&line);
    }

    /// Convenience: info-level event with a stable name.
    pub fn info(&self, event_name: &'static str) -> LogEventBuilder<'_> {
        LogEventBuilder::new(self, LogLevel::Info, event_name)
    }

    /// Convenience: warn-level event.
    pub fn warn(&self, event_name: &'static str) -> LogEventBuilder<'_> {
        LogEventBuilder::new(self, LogLevel::Warn, event_name)
    }

    /// Convenience: error-level event.
    pub fn error(&self, event_name: &'static str) -> LogEventBuilder<'_> {
        LogEventBuilder::new(self, LogLevel::Error, event_name)
    }
}

/// One structured log record (serializes to NDJSON).
#[derive(Debug, Clone, Serialize)]
pub struct LogEvent {
    /// Schema / profile tag.
    pub profile: &'static str,
    /// Unix epoch milliseconds.
    pub ts_ms: u64,
    /// Severity.
    pub level: String,
    /// Stable event name ([`events`]).
    pub event: String,
    /// Serve mode when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// Store path (no secrets).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store: Option<String>,
    /// Cluster root path when multi-node.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    /// Dense node index.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_index: Option<u32>,
    /// Virtual partition id when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<u32>,
    /// Application RPC op name (`put`, `get`, …).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op: Option<String>,
    /// Wire request correlation id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<u64>,
    /// Client mutation idempotency id (hex; not a secret).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    /// Authenticated principal id (never the raw token).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
    /// Stable error code when the request failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    /// Bounded human reason (no tokens / payloads).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Wall latency in milliseconds for the completed unit of work.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    /// Requested durability / guarantee label.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guarantee_requested: Option<String>,
    /// Achieved durability / guarantee label from the receipt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guarantee_achieved: Option<String>,
    /// Receipt `committed` flag when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed: Option<bool>,
    /// Whether the RPC `ok` field was true.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
    /// Hex event id from a write receipt (correlation with store).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    /// Collection name when known (not a payload).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
}

impl LogEvent {
    /// Serialize as a single NDJSON object (no trailing newline).
    pub fn to_ndjson(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| {
            format!(
                "{{\"profile\":\"{LOG_PROFILE}\",\"event\":\"log.serialize_failed\",\"level\":\"error\"}}"
            )
        })
    }
}

/// Fluent builder that emits on drop via [`LogEventBuilder::emit`].
pub struct LogEventBuilder<'a> {
    logger: &'a Logger,
    event: LogEvent,
}

impl<'a> LogEventBuilder<'a> {
    fn new(logger: &'a Logger, level: LogLevel, event_name: &'static str) -> Self {
        Self {
            logger,
            event: LogEvent {
                profile: LOG_PROFILE,
                ts_ms: now_ms(),
                level: level.as_str().to_string(),
                event: event_name.to_string(),
                mode: None,
                store: None,
                cluster: None,
                node_index: None,
                partition: None,
                op: None,
                request_id: None,
                operation_id: None,
                principal_id: None,
                error_code: None,
                reason: None,
                latency_ms: None,
                guarantee_requested: None,
                guarantee_achieved: None,
                committed: None,
                ok: None,
                event_id: None,
                collection: None,
            },
        }
    }

    /// Override severity after construction.
    pub fn level(mut self, level: LogLevel) -> Self {
        self.event.level = level.as_str().to_string();
        self
    }

    /// Application op name.
    pub fn op(mut self, op: &str) -> Self {
        self.event.op = Some(bound_name(op));
        self
    }

    /// Wire request id.
    pub fn request_id(mut self, id: u64) -> Self {
        self.event.request_id = Some(id);
        self
    }

    /// Client operation_id (hex). Tokens must never be passed here.
    pub fn operation_id(mut self, id: Option<&str>) -> Self {
        self.event.operation_id = id.map(bound_name);
        self
    }

    /// Principal id (public label, not the shared secret).
    pub fn principal_id(mut self, id: Option<&str>) -> Self {
        self.event.principal_id = id.map(bound_name);
        self
    }

    /// Stable error code.
    pub fn error_code(mut self, code: Option<&str>) -> Self {
        self.event.error_code = code.map(bound_name);
        self
    }

    /// Bounded free-text reason; secrets are redacted if present as whole field.
    pub fn reason(mut self, reason: &str) -> Self {
        self.event.reason = Some(sanitize_reason(reason));
        self
    }

    /// Latency of the completed unit of work.
    pub fn latency(mut self, d: Duration) -> Self {
        self.event.latency_ms = Some(d.as_millis() as u64);
        self
    }

    /// Latency already measured in milliseconds.
    pub fn latency_ms(mut self, ms: u64) -> Self {
        self.event.latency_ms = Some(ms);
        self
    }

    /// Requested durability label (`durable`, `buffered`, …).
    pub fn guarantee_requested(mut self, g: Option<&str>) -> Self {
        self.event.guarantee_requested = g.map(bound_name);
        self
    }

    /// Achieved durability / acknowledgement label.
    pub fn guarantee_achieved(mut self, g: Option<&str>) -> Self {
        self.event.guarantee_achieved = g.map(bound_name);
        self
    }

    /// Receipt committed flag.
    pub fn committed(mut self, c: Option<bool>) -> Self {
        self.event.committed = c;
        self
    }

    /// RPC ok flag.
    pub fn ok(mut self, ok: bool) -> Self {
        self.event.ok = Some(ok);
        self
    }

    /// Store event id (hex) from a receipt.
    pub fn event_id(mut self, id: Option<&str>) -> Self {
        self.event.event_id = id.map(bound_name);
        self
    }

    /// Collection name (not a document body).
    pub fn collection(mut self, c: Option<&str>) -> Self {
        self.event.collection = c.map(bound_name);
        self
    }

    /// Virtual partition id.
    pub fn partition(mut self, p: Option<u32>) -> Self {
        self.event.partition = p;
        self
    }

    /// Store path override for this event.
    pub fn store(mut self, path: &str) -> Self {
        self.event.store = Some(bound_field(path));
        self
    }

    /// Cluster root override.
    pub fn cluster(mut self, path: &str) -> Self {
        self.event.cluster = Some(bound_field(path));
        self
    }

    /// Node index override.
    pub fn node_index(mut self, n: u32) -> Self {
        self.event.node_index = Some(n);
        self
    }

    /// Emit the event through the parent logger.
    pub fn emit(self) {
        self.logger.emit(self.event);
    }
}

fn parse_level(s: &str) -> LogLevel {
    match s {
        "debug" => LogLevel::Debug,
        "info" => LogLevel::Info,
        "warn" => LogLevel::Warn,
        "error" => LogLevel::Error,
        _ => LogLevel::Info,
    }
}

/// Truncate a free-text field to [`MAX_FIELD_BYTES`] on a char boundary.
pub fn bound_field(s: &str) -> String {
    bound_to(s, MAX_FIELD_BYTES)
}

/// Truncate a short name (op, code, id) to [`MAX_NAME_BYTES`].
pub fn bound_name(s: &str) -> String {
    bound_to(s, MAX_NAME_BYTES)
}

fn bound_to(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = s[..end].to_string();
    out.push('…');
    out
}

/// Sanitize a reason string: never emit obvious secret-shaped values; bound length.
///
/// Whole-field redaction uses [`redact_secret`] when the value looks like a
/// credential (contains `token=` / `Bearer ` / long hex-like secrets). Partial
/// payload bodies are not logged by construction — callers must not pass them.
pub fn sanitize_reason(reason: &str) -> String {
    let lower = reason.to_ascii_lowercase();
    if lower.contains("token=")
        || lower.contains("bearer ")
        || lower.contains("authorization:")
        || lower.contains("password")
        || lower.contains("secret=")
    {
        return redact_secret(reason).to_string();
    }
    bound_field(reason)
}

/// Redact a credential for log fields (always `***`).
pub fn redact_credential(value: &str) -> String {
    redact_secret(value).to_string()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Emit an RPC completion (and optional guarantee-failed) from response metadata.
///
/// Never receives request/response bodies — only scalar correlation fields.
#[allow(clippy::too_many_arguments)] // correlation fields stay explicit (DEF-060)
pub fn log_rpc_complete(
    logger: &Logger,
    op: &str,
    request_id: u64,
    operation_id: Option<&str>,
    principal_id: Option<&str>,
    collection: Option<&str>,
    ok: bool,
    error_code: Option<&str>,
    latency: Duration,
    guarantee_requested: Option<&str>,
    guarantee_achieved: Option<&str>,
    committed: Option<bool>,
    event_id: Option<&str>,
    error_reason: Option<&str>,
) {
    let mut b = if ok {
        logger.info(events::RPC_COMPLETE)
    } else {
        logger.warn(events::RPC_COMPLETE)
    };
    b = b
        .op(op)
        .request_id(request_id)
        .operation_id(operation_id)
        .principal_id(principal_id)
        .collection(collection)
        .ok(ok)
        .error_code(error_code)
        .latency(latency)
        .guarantee_requested(guarantee_requested)
        .guarantee_achieved(guarantee_achieved)
        .committed(committed)
        .event_id(event_id);
    if let Some(r) = error_reason {
        b = b.reason(r);
    }
    b.emit();

    // Explicit guarantee failure event for dashboards / alerts.
    let guarantee_miss = match (ok, committed, guarantee_requested, guarantee_achieved) {
        (true, Some(false), _, _) => true,
        (true, _, Some(req), Some(ach)) if !req.is_empty() && req != ach => true,
        (false, _, Some(_), _) if error_code == Some("durability_unavailable") => true,
        _ => false,
    };
    if guarantee_miss {
        let mut g = logger.error(events::GUARANTEE_FAILED);
        g = g
            .op(op)
            .request_id(request_id)
            .operation_id(operation_id)
            .principal_id(principal_id)
            .collection(collection)
            .ok(ok)
            .error_code(error_code)
            .latency(latency)
            .guarantee_requested(guarantee_requested)
            .guarantee_achieved(guarantee_achieved)
            .committed(committed)
            .event_id(event_id);
        if let Some(r) = error_reason {
            g = g.reason(r);
        } else {
            g = g.reason("requested guarantee not achieved");
        }
        g.emit();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn profile_constant() {
        assert_eq!(LOG_PROFILE, "residiuum-log-v1");
        assert_eq!(events::RPC_COMPLETE, "rpc.complete");
        assert_eq!(events::GUARANTEE_FAILED, "guarantee.failed");
    }

    #[test]
    fn bound_field_truncates_on_char_boundary() {
        let s = "a".repeat(MAX_FIELD_BYTES + 10);
        let b = bound_field(&s);
        assert!(b.ends_with('…'));
        assert!(b.len() <= MAX_FIELD_BYTES + '…'.len_utf8());
    }

    #[test]
    fn sanitize_redacts_token_shaped_reasons() {
        assert_eq!(sanitize_reason("token=super-secret-value"), "***");
        assert_eq!(sanitize_reason("Authorization: Bearer abc"), "***");
        let plain = sanitize_reason("connection limit exceeded (max 32)");
        assert!(plain.contains("connection limit"));
    }

    #[test]
    fn memory_sink_captures_ndjson_with_correlation() {
        let sink = Arc::new(MemorySink::new(32));
        let log = Logger::with_sink(Arc::clone(&sink) as Arc<dyn LogSink>)
            .store("/tmp/store")
            .mode("serve")
            .shared();

        log_rpc_complete(
            &log,
            "put",
            42,
            Some("aabbccddeeff00112233445566778899"),
            Some("alice"),
            Some("docs"),
            true,
            None,
            Duration::from_millis(7),
            Some("durable"),
            Some("durable"),
            Some(true),
            Some("deadbeef"),
            None,
        );

        let lines = sink.json_lines();
        assert_eq!(lines.len(), 1);
        let v = &lines[0];
        assert_eq!(v["profile"], LOG_PROFILE);
        assert_eq!(v["event"], events::RPC_COMPLETE);
        assert_eq!(v["op"], "put");
        assert_eq!(v["request_id"], 42);
        assert_eq!(v["operation_id"], "aabbccddeeff00112233445566778899");
        assert_eq!(v["principal_id"], "alice");
        assert_eq!(v["store"], "/tmp/store");
        assert_eq!(v["mode"], "serve");
        assert_eq!(v["guarantee_requested"], "durable");
        assert_eq!(v["guarantee_achieved"], "durable");
        assert_eq!(v["committed"], true);
        assert_eq!(v["latency_ms"], 7);
        // No payload keys.
        assert!(v.get("json").is_none());
        assert!(v.get("token").is_none());
        assert!(v.get("value").is_none());
        assert!(v.get("bytes_b64").is_none());
    }

    #[test]
    fn guarantee_failed_emitted_when_committed_false() {
        let sink = Arc::new(MemorySink::new(8));
        let log = Logger::with_sink(Arc::clone(&sink) as Arc<dyn LogSink>).shared();
        log_rpc_complete(
            &log,
            "put",
            1,
            Some("op1"),
            None,
            None,
            true,
            None,
            Duration::from_millis(1),
            Some("durable"),
            Some("memory"),
            Some(false),
            None,
            None,
        );
        let events: Vec<String> = sink
            .json_lines()
            .iter()
            .filter_map(|v| v["event"].as_str().map(|s| s.to_string()))
            .collect();
        assert!(events.contains(&events::RPC_COMPLETE.to_string()));
        assert!(events.contains(&events::GUARANTEE_FAILED.to_string()));
    }

    #[test]
    fn min_level_filters_debug() {
        let sink = Arc::new(MemorySink::new(8));
        let log = Logger::with_sink(Arc::clone(&sink) as Arc<dyn LogSink>)
            .min_level(LogLevel::Info)
            .shared();
        log.info(events::SERVER_START).reason("listening").emit();
        // Debug below min — should not appear. Use builder with Debug level.
        log.info(events::RPC_COMPLETE)
            .level(LogLevel::Debug)
            .op("ping")
            .emit();
        let events: Vec<_> = sink
            .json_lines()
            .iter()
            .filter_map(|v| v["event"].as_str().map(|s| s.to_string()))
            .collect();
        assert_eq!(events, vec![events::SERVER_START.to_string()]);
    }

    #[test]
    fn ndjson_roundtrip_has_required_keys() {
        let ev = LogEvent {
            profile: LOG_PROFILE,
            ts_ms: 1,
            level: "info".into(),
            event: events::SERVER_START.into(),
            mode: Some("serve".into()),
            store: Some("/data".into()),
            cluster: None,
            node_index: None,
            partition: None,
            op: None,
            request_id: None,
            operation_id: None,
            principal_id: None,
            error_code: None,
            reason: Some("ready".into()),
            latency_ms: None,
            guarantee_requested: None,
            guarantee_achieved: None,
            committed: None,
            ok: None,
            event_id: None,
            collection: None,
        };
        let line = ev.to_ndjson();
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["profile"], "residiuum-log-v1");
        assert_eq!(v["event"], "server.start");
        assert_eq!(v["level"], "info");
    }
}
