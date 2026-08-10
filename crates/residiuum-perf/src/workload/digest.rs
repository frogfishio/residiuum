//! Workload digest oracle — hashes logical ops + payload digests, never stores keys/payloads.

use super::generator::{payload_digest, PayloadProfile};
use super::op::LogicalOp;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Independent expected digest for a logical stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDigest {
    pub schema: String,
    /// Hex SHA-256 over the canonical op stream encoding.
    pub stream_digest_hex: String,
    pub op_count: u64,
    pub logical_payload_bytes: u64,
    pub insert_count: u64,
    pub rewrite_count: u64,
    pub history_count: u64,
}

/// Lightweight stats without hashing payloads (for L0 generator measurement).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamStats {
    pub op_count: u64,
    pub logical_payload_bytes: u64,
    pub max_payload_len: u64,
    pub min_payload_len: u64,
}

/// Digest an op stream. Includes per-op payload content digest so contamination
/// of the generator is detectable by L0 without embedding keys/payloads in results.
pub fn digest_stream(seed: u64, profile: PayloadProfile, ops: &[LogicalOp]) -> StreamDigest {
    let mut h = Sha256::new();
    h.update(b"residiuum-pqh2-stream-v1\0");
    h.update(seed.to_le_bytes());
    h.update(profile.as_str().as_bytes());
    h.update(b"\0");

    let mut logical_payload_bytes = 0u64;
    let mut insert_count = 0u64;
    let mut rewrite_count = 0u64;
    let mut history_count = 0u64;

    for op in ops {
        h.update(op.seq.to_le_bytes());
        h.update(op.kind.as_str().as_bytes());
        h.update(b"\0");
        h.update(op.key_index.to_le_bytes());
        h.update(op.generation.to_le_bytes());
        h.update(op.payload_len.to_le_bytes());
        // Content commitment (streamed; O(1) RAM for large values).
        let pd = payload_digest(seed, op.seq, op.generation, profile, op.payload_len);
        h.update(pd);

        logical_payload_bytes = logical_payload_bytes.saturating_add(op.payload_len);
        match op.kind {
            super::op::OpKind::Insert => insert_count += 1,
            super::op::OpKind::Rewrite => rewrite_count += 1,
            super::op::OpKind::HistoryAppend => history_count += 1,
        }
    }

    StreamDigest {
        schema: "residiuum-pqh2-stream-digest-v1".into(),
        stream_digest_hex: hex::encode(h.finalize()),
        op_count: ops.len() as u64,
        logical_payload_bytes,
        insert_count,
        rewrite_count,
        history_count,
    }
}

pub fn stream_stats(ops: &[LogicalOp]) -> StreamStats {
    let mut logical_payload_bytes = 0u64;
    let mut max_payload_len = 0u64;
    let mut min_payload_len = u64::MAX;
    for op in ops {
        logical_payload_bytes = logical_payload_bytes.saturating_add(op.payload_len);
        max_payload_len = max_payload_len.max(op.payload_len);
        min_payload_len = min_payload_len.min(op.payload_len);
    }
    if ops.is_empty() {
        min_payload_len = 0;
    }
    StreamStats {
        op_count: ops.len() as u64,
        logical_payload_bytes,
        max_payload_len,
        min_payload_len,
    }
}

/// Assert a result JSON object does not embed raw keys or payloads (safety).
pub fn result_must_not_contain_payloads(result_json: &str) -> Result<(), String> {
    // Fail closed on obvious leakage fields.
    for bad in [
        "\"payload\":",
        "\"key_bytes\":",
        "\"raw_key\":",
        "\"body\":",
    ] {
        if result_json.contains(bad) {
            return Err(format!("result must not contain {bad}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::op::OpKind;

    fn ops() -> Vec<LogicalOp> {
        vec![
            LogicalOp {
                seq: 0,
                kind: OpKind::Insert,
                key_index: 0,
                generation: 0,
                payload_len: 100,
                producer_id: 0,
            },
            LogicalOp {
                seq: 1,
                kind: OpKind::Rewrite,
                key_index: 0,
                generation: 1,
                payload_len: 100,
                producer_id: 0,
            },
        ]
    }

    #[test]
    fn digest_stable() {
        let a = digest_stream(1, PayloadProfile::Incompressible, &ops());
        let b = digest_stream(1, PayloadProfile::Incompressible, &ops());
        assert_eq!(a, b);
        assert_eq!(a.op_count, 2);
        assert_eq!(a.rewrite_count, 1);
        assert_eq!(a.stream_digest_hex.len(), 64);
    }

    #[test]
    fn digest_changes_with_seed() {
        let a = digest_stream(1, PayloadProfile::Incompressible, &ops());
        let b = digest_stream(2, PayloadProfile::Incompressible, &ops());
        assert_ne!(a.stream_digest_hex, b.stream_digest_hex);
    }

    #[test]
    fn result_payload_guard() {
        assert!(result_must_not_contain_payloads(r#"{"validity":"valid"}"#).is_ok());
        assert!(result_must_not_contain_payloads(r#"{"payload":"secret"}"#).is_err());
    }
}
