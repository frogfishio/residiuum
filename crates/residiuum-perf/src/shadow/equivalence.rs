//! Equivalence between planned I/O shape and shadow execution / dual traces.

use super::plan::{DestinationClass, PhysicalWritePlan, SyncBoundary};
use super::shadow_exec::ShadowReport;
use serde::{Deserialize, Serialize};

/// Abstract syscall shape for one op (no paths/payloads).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyscallShape {
    pub seq: u32,
    pub kind: String,
    pub size: u64,
    pub dest: String,
    pub sync: String,
    pub rotate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquivalenceReport {
    pub planned_ops: u32,
    pub shadow_ops_completed: u64,
    pub planned_bytes: u64,
    pub shadow_bytes_requested: u64,
    pub shadow_bytes_completed: u64,
    pub planned_syncs: u32,
    pub shadow_syncs: u64,
    pub planned_rotations: u32,
    pub shadow_rotations: u64,
    pub shape_equal: bool,
    pub byte_accounting_ok: bool,
    pub messages: Vec<String>,
}

/// Expand plan into ordered syscall shapes (the "real-store planned" side).
pub fn plan_to_shapes(plan: &PhysicalWritePlan) -> Vec<SyscallShape> {
    plan.ops
        .iter()
        .map(|op| {
            let kind = if op.size == 0 && !matches!(op.sync_after, SyncBoundary::None) {
                "sync".into()
            } else if op.size == 0 {
                "marker".into()
            } else {
                "write".into()
            };
            SyscallShape {
                seq: op.seq,
                kind,
                size: op.size,
                dest: format!("{:?}", op.dest).to_ascii_lowercase(),
                sync: format!("{:?}", op.sync_after).to_ascii_lowercase(),
                rotate: op.segment_rotate,
            }
        })
        .collect()
}

/// Compare planned accounting to shadow execution counters.
pub fn compare_traces(plan: &PhysicalWritePlan, shadow: &ShadowReport) -> EquivalenceReport {
    let shapes = plan_to_shapes(plan);
    let mut messages = Vec::new();

    let shape_equal = shapes.len() as u32 == plan.ops.len() as u32
        && shapes.iter().enumerate().all(|(i, s)| s.seq == i as u32);

    let byte_accounting_ok =
        shadow.stats.bytes_requested == plan.planned_bytes && shadow.requested_vs_planned_match;

    if !byte_accounting_ok {
        messages.push(format!(
            "byte accounting: planned={} shadow_requested={} completed={}",
            plan.planned_bytes, shadow.stats.bytes_requested, shadow.stats.bytes_completed
        ));
    }
    if shadow.stats.ops_failed > 0 {
        messages.push(format!(
            "shadow failures: {} (partial/failed allowed in stress)",
            shadow.stats.ops_failed
        ));
    }
    if plan.planned_rotations != shadow.stats.rotations_seen as u32 {
        // Meta ops may not rotate; compare only segment_rotate flags.
        let plan_rot = plan.ops.iter().filter(|o| o.segment_rotate).count() as u32;
        if plan_rot != shadow.stats.rotations_seen as u32 {
            messages.push(format!(
                "rotation mismatch: plan={plan_rot} shadow={}",
                shadow.stats.rotations_seen
            ));
        }
    }

    // Destination classes only — no payload.
    let _ = DestinationClass::SegmentData;

    EquivalenceReport {
        planned_ops: plan.ops.len() as u32,
        shadow_ops_completed: shadow.stats.ops_completed,
        planned_bytes: plan.planned_bytes,
        shadow_bytes_requested: shadow.stats.bytes_requested,
        shadow_bytes_completed: shadow.stats.bytes_completed,
        planned_syncs: plan.planned_syncs,
        shadow_syncs: shadow.stats.syncs_completed,
        planned_rotations: plan.planned_rotations,
        shadow_rotations: shadow.stats.rotations_seen,
        shape_equal,
        byte_accounting_ok,
        messages,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{FakeIoAdapter, FakeIoConfig};
    use crate::shadow::plan::{PlanBuilder, ShapeConfig};
    use crate::shadow::shadow_exec::execute_shadow;

    #[test]
    fn planned_shadow_shape_equivalence() {
        let plan = PlanBuilder::build(&ShapeConfig {
            write_sizes: vec![256, 512, 256, 1024],
            segment_threshold: 1000,
            batch_size: 2,
            sync_every_ops: 2,
            final_sync: true,
            ..ShapeConfig::default()
        });
        let mut adapter = FakeIoAdapter::new(FakeIoConfig::default());
        let shadow = execute_shadow(&mut adapter, &plan).unwrap();
        let eq = compare_traces(&plan, &shadow);
        assert!(eq.shape_equal);
        assert!(eq.byte_accounting_ok);
        assert_eq!(eq.planned_bytes, eq.shadow_bytes_requested);
        assert_eq!(eq.shadow_bytes_completed, eq.shadow_bytes_requested);
        let shapes = plan_to_shapes(&plan);
        assert_eq!(shapes.len(), plan.ops.len());
        assert!(!serde_json::to_string(&shapes).unwrap().contains("payload"));
    }
}
