//! Large-value policy and rewrite-heavy workload contract (DEF-103).
//!
//! Layout defaults (64 KiB threshold / 16 KiB chunks) are **not** a document-size
//! promise. One versioned [`LargeValuePolicy`] defines admission and layout for
//! every write surface on this store handle.

use crate::chunk_payload::{DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_THRESHOLD};
use crate::error::StoreError;

/// Profile identifier for the stable v1 application large-value policy.
pub const LARGE_VALUE_PROFILE_ID: &str = "residiuum-large-value-v1";

/// Default max logical encoded payload (includes SDK type tags when present).
///
/// Matches the historical `SafetyLimits::max_body_len` / SDK ceiling.
pub const DEFAULT_MAX_LOGICAL_PAYLOAD_BYTES: u64 = 16 * 1024 * 1024;

/// Validated large-value / chunk layout policy (DEF-103).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeValuePolicy {
    /// Profile id (stable label for diagnostics / receipts).
    pub profile_id: String,
    /// Maximum logical payload bytes admitted for a new write.
    pub max_logical_payload_bytes: u64,
    /// Bodies larger than this are written chunked (layout switch only).
    pub chunk_threshold_bytes: usize,
    /// Max payload bytes per chunk frame body (exclusive of chunk header).
    pub chunk_payload_bytes: usize,
    /// Maximum encoded chunk-manifest bytes.
    pub max_manifest_bytes: u64,
    /// Maximum bytes allowed for a complete reassembly allocation.
    pub max_reassembly_bytes: u64,
    /// Estimated peak write-path memory budget for one put (advisory admit).
    pub max_write_peak_memory: u64,
}

impl Default for LargeValuePolicy {
    fn default() -> Self {
        Self::application_v1()
    }
}

impl LargeValuePolicy {
    /// Stable application v1 defaults (16 MiB logical, 64 KiB threshold, 16 KiB chunks).
    pub fn application_v1() -> Self {
        let max = DEFAULT_MAX_LOGICAL_PAYLOAD_BYTES;
        Self {
            profile_id: LARGE_VALUE_PROFILE_ID.to_string(),
            max_logical_payload_bytes: max,
            chunk_threshold_bytes: DEFAULT_CHUNK_THRESHOLD,
            chunk_payload_bytes: DEFAULT_CHUNK_SIZE,
            // Fixed header + 24 bytes per chunk; headroom for ~1k chunks.
            max_manifest_bytes: 8 + 32 + 4 + 8 + 24 * 1024,
            max_reassembly_bytes: max,
            max_write_peak_memory: max.saturating_mul(2).max(1024 * 1024),
        }
    }

    /// Validate internal consistency; reject zero/oversized settings before effect.
    pub fn validate(&self) -> Result<(), StoreError> {
        if self.profile_id.is_empty() {
            return Err(StoreError::CorruptMeta("large-value profile_id empty"));
        }
        if self.max_logical_payload_bytes == 0 {
            return Err(StoreError::CorruptMeta(
                "max_logical_payload_bytes must be non-zero",
            ));
        }
        if self.chunk_payload_bytes == 0 {
            return Err(StoreError::CorruptMeta(
                "chunk_payload_bytes must be non-zero",
            ));
        }
        if self.chunk_threshold_bytes == 0 {
            return Err(StoreError::CorruptMeta(
                "chunk_threshold_bytes must be non-zero",
            ));
        }
        if self.max_manifest_bytes < 52 {
            return Err(StoreError::CorruptMeta(
                "max_manifest_bytes too small for empty manifest header",
            ));
        }
        if self.max_reassembly_bytes < self.max_logical_payload_bytes {
            return Err(StoreError::CorruptMeta(
                "max_reassembly_bytes must be >= max_logical_payload_bytes",
            ));
        }
        // Chunk count must fit u32 for any admitted logical size.
        let max_chunks = self
            .max_logical_payload_bytes
            .div_ceil(self.chunk_payload_bytes as u64);
        if max_chunks > u32::MAX as u64 {
            return Err(StoreError::CorruptMeta(
                "chunk_payload_bytes too small for max_logical_payload_bytes",
            ));
        }
        let manifest_need = 8u64 + 32 + 4 + 8 + max_chunks.saturating_mul(24);
        if manifest_need > self.max_manifest_bytes {
            return Err(StoreError::CorruptMeta(
                "max_manifest_bytes insufficient for worst-case chunk count",
            ));
        }
        Ok(())
    }

    /// Effective write ceiling when combining with another layer's limit.
    pub fn effective_with(&self, other_max: u64) -> u64 {
        self.max_logical_payload_bytes.min(other_max)
    }

    /// Admit a logical payload **before** minting event IDs or appending frames.
    ///
    /// Returns layout choice and chunk count (0 when inline). Does not re-run
    /// full [`Self::validate`] so test overrides of chunk size remain usable;
    /// structural policy validation happens in [`Self::validate`] /
    /// `Store::set_large_value_policy`.
    pub fn admit(&self, logical_len: usize) -> Result<AdmitDecision, StoreError> {
        if self.chunk_payload_bytes == 0 || self.chunk_threshold_bytes == 0 {
            return Err(StoreError::CorruptMeta(
                "chunk size/threshold must be non-zero",
            ));
        }
        let len = logical_len as u64;
        if len > self.max_logical_payload_bytes {
            return Err(StoreError::PayloadTooLarge);
        }
        if len > self.max_reassembly_bytes {
            return Err(StoreError::PayloadTooLarge);
        }
        // Peak: body + chunk/manifest working set (conservative 2×).
        let peak = len.saturating_mul(2).max(len.saturating_add(64 * 1024));
        if peak > self.max_write_peak_memory {
            return Err(StoreError::PayloadTooLarge);
        }

        if logical_len > self.chunk_threshold_bytes {
            let chunk_count = logical_len.div_ceil(self.chunk_payload_bytes);
            if chunk_count == 0 || chunk_count > u32::MAX as usize {
                return Err(StoreError::PayloadTooLarge);
            }
            let manifest_len = 8 + 32 + 4 + 8 + chunk_count * 24;
            if manifest_len as u64 > self.max_manifest_bytes {
                return Err(StoreError::PayloadTooLarge);
            }
            Ok(AdmitDecision {
                layout: PayloadLayout::Chunked,
                logical_len: len,
                chunk_count: chunk_count as u32,
            })
        } else {
            Ok(AdmitDecision {
                layout: PayloadLayout::Inline,
                logical_len: len,
                chunk_count: 0,
            })
        }
    }
}

/// Whether a put is stored inline or as chunk frames + manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadLayout {
    /// Single item-event body holds the logical payload.
    Inline,
    /// Payload-chunk frames + chunk manifest item event.
    Chunked,
}

impl PayloadLayout {
    /// Stable label for receipts / diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inline => "inline",
            Self::Chunked => "chunked",
        }
    }
}

/// Result of policy admission for one write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmitDecision {
    /// Chosen layout.
    pub layout: PayloadLayout,
    /// Logical payload length admitted.
    pub logical_len: u64,
    /// Chunk count when chunked (0 when inline).
    pub chunk_count: u32,
}

/// Recommended key model for rewrite-heavy compound documents (DEF-103).
///
/// Applications should store independently meaningful records rather than one
/// ever-growing document under a single key.
pub mod rewrite_heavy {
    /// Collection-relative key for transcript metadata.
    pub fn transcript_meta(transcript_id: &str) -> String {
        format!("transcript/{transcript_id}/meta")
    }

    /// Collection-relative key for one independently meaningful turn.
    pub fn transcript_turn(transcript_id: &str, turn_id: &str) -> String {
        format!("transcript/{transcript_id}/turn/{turn_id}")
    }

    /// Collection-relative key for a bounded timeline block.
    pub fn transcript_timeline_block(transcript_id: &str, block_id: &str) -> String {
        format!("transcript/{transcript_id}/timeline/{block_id}")
    }

    /// Derived/rebuildable snapshot (optional aggregate).
    pub fn transcript_snapshot(transcript_id: &str, generation: &str) -> String {
        format!("transcript/{transcript_id}/snapshot/{generation}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_admits_threshold_boundary() {
        let p = LargeValuePolicy::default();
        p.validate().unwrap();
        let exact = p.admit(DEFAULT_CHUNK_THRESHOLD).unwrap();
        assert_eq!(exact.layout, PayloadLayout::Inline);
        let over = p.admit(DEFAULT_CHUNK_THRESHOLD + 1).unwrap();
        assert_eq!(over.layout, PayloadLayout::Chunked);
        assert!(over.chunk_count >= 1);
    }

    #[test]
    fn rejects_over_max_logical() {
        let p = LargeValuePolicy::default();
        let too = (p.max_logical_payload_bytes as usize).saturating_add(1);
        assert!(matches!(p.admit(too), Err(StoreError::PayloadTooLarge)));
    }

    #[test]
    fn rejects_zero_chunk_size() {
        let mut p = LargeValuePolicy::default();
        p.chunk_payload_bytes = 0;
        assert!(p.validate().is_err());
    }
}
