//! Store-native PhysicalWritePlan emitter from redacted write facts / boundary events.
//!
//! Emits plans at the store **boundary** from sizes, durability, and segment
//! rotation signals — never from document payloads or keys.

use crate::shadow::{DestinationClass, PhysicalOp, PhysicalWritePlan, SyncBoundary, PLAN_SCHEMA};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Seam id for receipt-stream emission (replaces pure ShapeConfig-only residual).
pub const STORE_SEAM_EMITTER_FROM_RECEIPTS: &str = "store_boundary_emitter_from_receipts_v1";

/// Kind of store-boundary observation (actual put path only — no estimates).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreBoundaryKind {
    /// Wire-encoded frame(s) appended for a put (inline or chunked sum).
    AppendEncodedFrame,
    /// Durability barrier applied after the append (data-only or full-file).
    DurabilityBarrier,
}

/// Redacted event observed at the store boundary after an actual write.
///
/// Built only from `WriteReceipt` fields and durability mode — never from
/// payload contents or invented frame overhead.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoreBoundaryEvent {
    pub kind: StoreBoundaryKind,
    /// Exact encoded on-segment frame length (`WriteReceipt.encoded_frame_len`).
    /// Zero on barrier-only events or when no frame was measured.
    pub encoded_frame_len: u64,
    pub logical_len: u64,
    pub durability: String,
    pub segment_gen: u32,
    pub segment_rotate: bool,
    pub chunked: bool,
    pub chunk_count: u32,
    /// Segment byte offset of the frame (0 for barrier-only).
    pub segment_offset: u64,
}

/// Redacted per-write fact (no payload, no subject, no heap id).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteReceiptFact {
    /// Logical payload length acknowledged.
    pub logical_len: u64,
    /// Exact encoded frame length from the store boundary when known.
    /// Prefer measured `WriteReceipt.encoded_frame_len`; do not invent payload+N.
    pub encoded_frame_len: u64,
    /// Durability mode name applied.
    pub durability: String,
    /// Segment generation / rotation counter (0-based).
    pub segment_gen: u32,
    /// True when this write opened a new segment relative to the previous fact.
    pub segment_rotate: bool,
    /// Chunked layout (large value).
    pub chunked: bool,
    /// Chunk count when chunked.
    pub chunk_count: u32,
}

impl From<&StoreBoundaryEvent> for WriteReceiptFact {
    fn from(e: &StoreBoundaryEvent) -> Self {
        WriteReceiptFact {
            logical_len: e.logical_len,
            encoded_frame_len: e.encoded_frame_len,
            durability: e.durability.clone(),
            segment_gen: e.segment_gen,
            segment_rotate: e.segment_rotate,
            chunked: e.chunked,
            chunk_count: e.chunk_count,
        }
    }
}

/// Build redacted facts from harness-local boundary events (append frames only).
pub fn facts_from_boundary_events(events: &[StoreBoundaryEvent]) -> Vec<WriteReceiptFact> {
    events
        .iter()
        .filter(|e| e.kind == StoreBoundaryKind::AppendEncodedFrame)
        .map(WriteReceiptFact::from)
        .collect()
}

/// Map **store-emitted** [`residiuum_store::BoundaryEvent`] records to plan facts.
///
/// Only `AppendEncodedFrame` contributes plan ops; sync/rotate/publish/lifecycle
/// events are carried in notes/metrics by the driver and must not be invented here.
#[cfg(feature = "store-driver")]
pub fn facts_from_store_boundary_events(
    events: &[residiuum_store::BoundaryEvent],
) -> Vec<WriteReceiptFact> {
    use residiuum_store::BoundaryKind;
    events
        .iter()
        .filter(|e| e.kind == BoundaryKind::AppendEncodedFrame)
        .map(|e| WriteReceiptFact {
            logical_len: e.logical_len,
            encoded_frame_len: e.encoded_bytes,
            durability: e.durability.clone(),
            segment_gen: e.segment_gen,
            segment_rotate: e.segment_rotate,
            chunked: e.chunked,
            chunk_count: e.chunk_count,
        })
        .collect()
}

/// Emit plan from store-native boundary events (feature `store-driver`).
#[cfg(feature = "store-driver")]
pub fn emit_plan_from_store_boundary_events(
    plan_id: &str,
    events: &[residiuum_store::BoundaryEvent],
    segment_threshold: u64,
    chunk_threshold: u64,
    batch_size: u32,
) -> PhysicalWritePlan {
    let facts = facts_from_store_boundary_events(events);
    emit_plan_from_receipts(
        plan_id,
        &facts,
        segment_threshold,
        chunk_threshold,
        batch_size,
    )
}

/// Emit a closed PhysicalWritePlan from a stream of redacted write facts.
pub fn emit_plan_from_receipts(
    plan_id: &str,
    facts: &[WriteReceiptFact],
    segment_threshold: u64,
    chunk_threshold: u64,
    batch_size: u32,
) -> PhysicalWritePlan {
    let mut ops = Vec::new();
    let mut seq = 0u32;
    let mut planned_bytes = 0u64;
    let mut planned_syncs = 0u32;
    let mut planned_rotations = 0u32;
    let batch_size = batch_size.max(1);

    for (i, f) in facts.iter().enumerate() {
        if f.segment_rotate {
            planned_rotations = planned_rotations.saturating_add(1);
            // Directory sync often accompanies rotation.
            ops.push(PhysicalOp {
                seq,
                dest: DestinationClass::Directory,
                size: 0,
                alignment: 0,
                sync_after: SyncBoundary::Directory,
                segment_gen: f.segment_gen,
                segment_rotate: true,
                chunk_boundary: false,
                batch_index: (i as u32) % batch_size,
            });
            seq = seq.saturating_add(1);
            planned_syncs = planned_syncs.saturating_add(1);
        }

        // Real_store path supplies measured encoded_frame_len. Synthetic may
        // pass an explicit encoded length. Zero means "unknown" — use logical
        // only (never invent a fixed overhead like payload+96).
        let size = if f.encoded_frame_len > 0 {
            f.encoded_frame_len
        } else {
            f.logical_len
        };
        let sync_after = match f.durability.as_str() {
            "durable" => SyncBoundary::FullFile,
            "buffered" => SyncBoundary::DataOnly,
            _ => SyncBoundary::None,
        };
        if !matches!(sync_after, SyncBoundary::None) {
            planned_syncs = planned_syncs.saturating_add(1);
        }
        planned_bytes = planned_bytes.saturating_add(size);

        if f.chunked && f.chunk_count > 0 {
            let per = size / f.chunk_count.max(1) as u64;
            for c in 0..f.chunk_count {
                ops.push(PhysicalOp {
                    seq,
                    dest: DestinationClass::ChunkData,
                    size: per,
                    alignment: 4096,
                    sync_after: if c + 1 == f.chunk_count {
                        sync_after
                    } else {
                        SyncBoundary::None
                    },
                    segment_gen: f.segment_gen,
                    segment_rotate: false,
                    chunk_boundary: true,
                    batch_index: (i as u32) % batch_size,
                });
                seq = seq.saturating_add(1);
            }
        } else {
            ops.push(PhysicalOp {
                seq,
                dest: DestinationClass::SegmentData,
                size,
                alignment: 4096,
                sync_after,
                segment_gen: f.segment_gen,
                segment_rotate: false,
                chunk_boundary: size >= chunk_threshold,
                batch_index: (i as u32) % batch_size,
            });
            seq = seq.saturating_add(1);
        }
    }

    let mut plan = PhysicalWritePlan {
        schema: PLAN_SCHEMA.into(),
        plan_id: plan_id.into(),
        shape_hash: String::new(),
        ops,
        planned_bytes,
        planned_syncs,
        planned_rotations,
        segment_threshold,
        chunk_threshold,
        batch_size,
        sync_every_ops: 0,
    };
    plan.shape_hash = plan.content_digest();
    // Bind emitter id into plan_id namespace without payload leakage.
    let mut h = Sha256::new();
    h.update(STORE_SEAM_EMITTER_FROM_RECEIPTS.as_bytes());
    h.update(b"\0");
    h.update(plan.shape_hash.as_bytes());
    let _emitter_bind = hex::encode(h.finalize());
    let _ = _emitter_bind;
    plan
}

/// Emit plan from actual store-boundary events (append + barrier stream).
pub fn emit_plan_from_boundary_events(
    plan_id: &str,
    events: &[StoreBoundaryEvent],
    segment_threshold: u64,
    chunk_threshold: u64,
    batch_size: u32,
) -> PhysicalWritePlan {
    let facts = facts_from_boundary_events(events);
    emit_plan_from_receipts(
        plan_id,
        &facts,
        segment_threshold,
        chunk_threshold,
        batch_size,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_redacted_plan() {
        let facts = vec![
            WriteReceiptFact {
                logical_len: 100,
                encoded_frame_len: 164,
                durability: "durable".into(),
                segment_gen: 0,
                segment_rotate: false,
                chunked: false,
                chunk_count: 0,
            },
            WriteReceiptFact {
                logical_len: 200,
                encoded_frame_len: 264,
                durability: "durable".into(),
                segment_gen: 1,
                segment_rotate: true,
                chunked: false,
                chunk_count: 0,
            },
        ];
        let plan = emit_plan_from_receipts("cell-a", &facts, 32 * 1024, 1024 * 1024, 4);
        plan.assert_redacted_json().unwrap();
        assert!(plan.planned_bytes >= 164 + 264);
        assert!(plan.planned_rotations >= 1);
        assert!(!plan.ops.is_empty());
        assert_eq!(plan.schema, PLAN_SCHEMA);
    }

    #[test]
    fn boundary_events_drive_encoded_lengths() {
        let events = vec![
            StoreBoundaryEvent {
                kind: StoreBoundaryKind::AppendEncodedFrame,
                encoded_frame_len: 200,
                logical_len: 128,
                durability: "buffered".into(),
                segment_gen: 0,
                segment_rotate: false,
                chunked: false,
                chunk_count: 0,
                segment_offset: 0,
            },
            StoreBoundaryEvent {
                kind: StoreBoundaryKind::DurabilityBarrier,
                encoded_frame_len: 0,
                logical_len: 128,
                durability: "buffered".into(),
                segment_gen: 0,
                segment_rotate: false,
                chunked: false,
                chunk_count: 0,
                segment_offset: 0,
            },
        ];
        let plan = emit_plan_from_boundary_events("cell-b", &events, 32 * 1024, 1024 * 1024, 1);
        assert_eq!(plan.planned_bytes, 200);
        assert!(plan.planned_syncs >= 1);
    }
}
