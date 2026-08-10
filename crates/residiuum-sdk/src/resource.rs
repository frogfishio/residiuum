//! Host resource governance (DEF-029 / DX_SPEC §8.1).
//!
//! Separates **explicit query budgets** ([`crate::QueryBudget`]) from hard
//! host limits that bound adversarial input. Exceeding an explicit budget
//! yields [`crate::ErrorCode::QueryBudgetRequired`]; exceeding a hard limit
//! yields [`crate::ErrorCode::ResourceLimit`]. Cancellation is cooperative
//! and checked between page/loop steps.

use crate::error::Error;
use serde_json::Value as JsonValue;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Profile tag for resource-governance defaults and diagnostics (DEF-029).
pub const RESOURCE_PROFILE: &str = "residiuum-resource-v1";

/// Default maximum nesting depth for JSON documents on put/decode paths.
pub const DEFAULT_MAX_JSON_DEPTH: usize = 64;

/// Default maximum typed payload body size (bytes), aligned with format
/// draft `max_body_len` (16 MiB).
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// Default maximum RPC request line size (bytes) for the line-delimited JSON
/// TCP protocol. Oversized lines are refused before full parse.
pub const DEFAULT_MAX_RPC_LINE_BYTES: usize = 16 * 1024 * 1024;

/// Default hard cap on materialised match/sort memory for a single query
/// (64 MiB). Callers may raise this via [`crate::QueryBudget::max_result_bytes`]
/// only up to this host ceiling unless overridden on [`ResourceLimits`].
pub const DEFAULT_MAX_RESULT_BYTES: u64 = 64 * 1024 * 1024;

/// Hard host limits applied regardless of per-query budgets.
///
/// These bound memory/CPU growth from adversarial requests. They are not a
/// substitute for operator-declared [`crate::QueryBudget`] on expensive scans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceLimits {
    /// Maximum JSON nesting depth (objects/arrays). Root is depth 1.
    pub max_json_depth: usize,
    /// Maximum encoded payload body size accepted on put paths.
    pub max_payload_bytes: usize,
    /// Maximum single RPC line (request) size.
    pub max_rpc_line_bytes: usize,
    /// Hard ceiling on materialised result / sort memory for one query.
    pub max_result_bytes: u64,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_json_depth: DEFAULT_MAX_JSON_DEPTH,
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            max_rpc_line_bytes: DEFAULT_MAX_RPC_LINE_BYTES,
            max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
        }
    }
}

impl ResourceLimits {
    /// Draft defaults used by the SDK and single-node server.
    pub fn draft_defaults() -> Self {
        Self::default()
    }
}

/// Process-wide draft defaults (until ServeOptions/Residiuum options thread custom
/// limits). Tests may construct local [`ResourceLimits`] values.
pub fn host_limits() -> ResourceLimits {
    ResourceLimits::draft_defaults()
}

/// Cooperative cancellation token for long-running scans and finds.
///
/// Cloning shares the same flag. Setting cancelled is sticky.
#[derive(Debug, Clone, Default)]
pub struct CancelToken {
    cancelled: Arc<AtomicBool>,
}

impl CancelToken {
    /// New non-cancelled token.
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Request cancellation; subsequent [`check`](Self::check) calls fail.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Whether cancellation was requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Return [`Error::ResourceLimit`] when cancelled.
    pub fn check(&self) -> Result<(), Error> {
        if self.is_cancelled() {
            Err(Error::ResourceLimit("query cancelled".into()))
        } else {
            Ok(())
        }
    }
}

impl PartialEq for CancelToken {
    fn eq(&self, other: &Self) -> bool {
        // Tokens compare equal when they share the same flag identity or both
        // report the same cancelled state (plan/options equality ignores live
        // race detail).
        Arc::ptr_eq(&self.cancelled, &other.cancelled)
            || self.is_cancelled() == other.is_cancelled()
    }
}

impl Eq for CancelToken {}

/// Maximum nesting depth of a JSON value (root depth = 1).
pub fn json_depth(value: &JsonValue) -> usize {
    match value {
        JsonValue::Array(items) => 1 + items.iter().map(json_depth).max().unwrap_or(0),
        JsonValue::Object(map) => 1 + map.values().map(json_depth).max().unwrap_or(0),
        _ => 1,
    }
}

/// Enforce JSON depth against host limits.
pub fn check_json_depth(value: &JsonValue, limits: &ResourceLimits) -> Result<(), Error> {
    let depth = json_depth(value);
    if depth > limits.max_json_depth {
        return Err(Error::ResourceLimit(format!(
            "JSON depth {depth} exceeds limit {}",
            limits.max_json_depth
        )));
    }
    Ok(())
}

/// Enforce encoded payload size against host limits.
pub fn check_payload_len(len: usize, limits: &ResourceLimits) -> Result<(), Error> {
    if len > limits.max_payload_bytes {
        return Err(Error::ResourceLimit(format!(
            "payload {len} bytes exceeds limit {}",
            limits.max_payload_bytes
        )));
    }
    Ok(())
}

/// Enforce RPC line size against host limits.
pub fn check_rpc_line_len(len: usize, limits: &ResourceLimits) -> Result<(), Error> {
    if len > limits.max_rpc_line_bytes {
        return Err(Error::ResourceLimit(format!(
            "RPC line {len} bytes exceeds limit {}",
            limits.max_rpc_line_bytes
        )));
    }
    Ok(())
}

/// Approximate in-memory size of a JSON value for sort/result budgets.
///
/// Conservative overestimate: structural overhead + string/number bytes.
/// Used only for governance, not wire accounting.
pub fn estimate_json_bytes(value: &JsonValue) -> u64 {
    match value {
        JsonValue::Null | JsonValue::Bool(_) => 8,
        JsonValue::Number(n) => 16 + n.to_string().len() as u64,
        JsonValue::String(s) => 16 + s.len() as u64,
        JsonValue::Array(items) => {
            24 + items
                .iter()
                .map(|v| estimate_json_bytes(v).saturating_add(8))
                .sum::<u64>()
        }
        JsonValue::Object(map) => {
            24 + map
                .iter()
                .map(|(k, v)| (16 + k.len() as u64).saturating_add(estimate_json_bytes(v)))
                .sum::<u64>()
        }
    }
}

/// Approximate size of one match row (key + document).
pub fn estimate_row_bytes(key: &str, value: &JsonValue) -> u64 {
    32 + key.len() as u64 + estimate_json_bytes(value)
}

/// Check result-set memory against the tighter of query budget and host ceiling.
pub fn check_result_bytes(
    used: u64,
    budget_cap: Option<u64>,
    limits: &ResourceLimits,
) -> Result<(), Error> {
    if let Some(cap) = budget_cap {
        if used > cap {
            return Err(Error::QueryBudgetRequired(format!(
                "result materialisation {used} bytes exceeds budget max_result_bytes {cap}; \
                 raise budget, add limit, or avoid full sort materialisation"
            )));
        }
    }
    if used > limits.max_result_bytes {
        return Err(Error::ResourceLimit(format!(
            "result materialisation {used} bytes exceeds host max_result_bytes {}; \
             spill-to-disk sort is not enabled in this profile — add limit/index \
             or reduce match set",
            limits.max_result_bytes
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn depth_scalars_and_nesting() {
        assert_eq!(json_depth(&json!(1)), 1);
        assert_eq!(json_depth(&json!({"a": 1})), 2);
        assert_eq!(json_depth(&json!({"a": {"b": [1, {"c": 2}]}})), 5);
    }

    #[test]
    fn depth_limit_boundary() {
        let mut limits = ResourceLimits::draft_defaults();
        limits.max_json_depth = 3;
        let ok = json!({"a": {"b": 1}});
        let bad = json!({"a": {"b": {"c": 1}}});
        assert!(check_json_depth(&ok, &limits).is_ok());
        assert!(matches!(
            check_json_depth(&bad, &limits),
            Err(Error::ResourceLimit(_))
        ));
    }

    #[test]
    fn cancel_token_sticky() {
        let t = CancelToken::new();
        assert!(t.check().is_ok());
        t.cancel();
        assert!(t.is_cancelled());
        assert!(matches!(t.check(), Err(Error::ResourceLimit(_))));
        let clone = t.clone();
        assert!(clone.is_cancelled());
    }

    #[test]
    fn estimate_grows_with_string() {
        let small = estimate_json_bytes(&json!({"x": "ab"}));
        let big = estimate_json_bytes(&json!({"x": "a".repeat(10_000)}));
        assert!(big > small + 9_000);
    }
}
