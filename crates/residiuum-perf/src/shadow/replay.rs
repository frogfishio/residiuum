//! Plan replay validator — order, accounting, and integrity.

use super::plan::{PhysicalWritePlan, SyncBoundary, PLAN_SCHEMA};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ReplayError {
    #[error("empty plan")]
    Empty,
    #[error("seq gap at index {index}: expected {expected}, got {got}")]
    SeqGap {
        index: usize,
        expected: u32,
        got: u32,
    },
    #[error("planned_bytes mismatch: header {header}, sum {sum}")]
    BytesMismatch { header: u64, sum: u64 },
    #[error("shape_hash mismatch")]
    ShapeHashMismatch,
    #[error("schema mismatch: {0}")]
    Schema(String),
    #[error("unordered segment_gen regression at seq {seq}")]
    SegmentRegression { seq: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayReport {
    pub op_count: u32,
    pub sum_bytes: u64,
    pub syncs: u32,
    pub rotations_seen: u32,
    pub ok: bool,
}

pub fn validate_plan_replay(plan: &PhysicalWritePlan) -> Result<ReplayReport, ReplayError> {
    if plan.ops.is_empty() {
        return Err(ReplayError::Empty);
    }
    if plan.schema != PLAN_SCHEMA {
        return Err(ReplayError::Schema(plan.schema.clone()));
    }
    if plan.content_digest() != plan.shape_hash {
        return Err(ReplayError::ShapeHashMismatch);
    }

    let mut sum_bytes = 0u64;
    let mut syncs = 0u32;
    let mut rotations = 0u32;
    let mut last_gen = 0u32;

    for (i, op) in plan.ops.iter().enumerate() {
        if op.seq != i as u32 {
            return Err(ReplayError::SeqGap {
                index: i,
                expected: i as u32,
                got: op.seq,
            });
        }
        sum_bytes = sum_bytes.saturating_add(op.size);
        if !matches!(op.sync_after, SyncBoundary::None) {
            syncs = syncs.saturating_add(1);
        }
        if op.segment_rotate {
            rotations = rotations.saturating_add(1);
        }
        if op.segment_gen < last_gen {
            return Err(ReplayError::SegmentRegression { seq: op.seq });
        }
        last_gen = op.segment_gen;
    }

    if sum_bytes != plan.planned_bytes {
        return Err(ReplayError::BytesMismatch {
            header: plan.planned_bytes,
            sum: sum_bytes,
        });
    }

    Ok(ReplayReport {
        op_count: plan.ops.len() as u32,
        sum_bytes,
        syncs,
        rotations_seen: rotations,
        ok: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shadow::plan::{PlanBuilder, ShapeConfig};

    #[test]
    fn valid_plan_replays() {
        let p = PlanBuilder::build(&ShapeConfig {
            write_sizes: vec![100, 200, 300],
            ..ShapeConfig::default()
        });
        let r = validate_plan_replay(&p).unwrap();
        assert!(r.ok);
        assert_eq!(r.sum_bytes, p.planned_bytes);
    }

    #[test]
    fn detects_seq_gap() {
        let mut p = PlanBuilder::build(&ShapeConfig {
            write_sizes: vec![10, 20],
            final_sync: false,
            sync_every_ops: 0,
            batch_size: 100,
            ..ShapeConfig::default()
        });
        p.ops[1].seq = 99;
        // Recompute hash so validation reaches the seq check.
        p.shape_hash = p.content_digest();
        assert!(matches!(
            validate_plan_replay(&p),
            Err(ReplayError::SeqGap { .. })
        ));
    }

    #[test]
    fn detects_bytes_mismatch() {
        let mut p = PlanBuilder::build(&ShapeConfig {
            write_sizes: vec![10],
            final_sync: false,
            sync_every_ops: 0,
            batch_size: 100,
            ..ShapeConfig::default()
        });
        p.planned_bytes = 999;
        // also break hash so we get shape or bytes — fix hash after
        p.shape_hash = p.content_digest();
        assert!(matches!(
            validate_plan_replay(&p),
            Err(ReplayError::BytesMismatch { .. })
        ));
    }
}
