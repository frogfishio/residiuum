//! Synthetic L4/L5/L6 driver — harness proxy, non-product.

use super::{DriverCellReport, DriverError, DriverRunConfig};
use crate::matrix::{run_cell, RunCellConfig};
use crate::store_driver::emitter::{emit_plan_from_receipts, WriteReceiptFact};
use crate::store_driver::kinds::{DriverKind, MeasurementSurface};

pub fn run_synthetic(cfg: &DriverRunConfig) -> Result<DriverCellReport, DriverError> {
    let mut cell = cfg.cell.clone();
    // Synthetic smoke may bound ops for unit speed; qualification keeps planned count.
    if cfg.run_class_parsed().allows_smoke_op_cap() {
        cell.op_count = cell
            .op_count
            .min(crate::campaign::RunClass::SMOKE_MAX_OPS)
            .max(1);
    }
    let report = run_cell(&RunCellConfig {
        cell: cell.clone(),
        seed: cfg.seed,
        durability_mutant: cfg.durability_mutant,
        digest_mutant: cfg.digest_mutant,
    })?;

    // Derive a receipt-style plan from the logical sizes used by the synthetic cell
    // so the emitter path is exercised even without the store feature.
    let ops = report.acknowledged.min(16);
    let mut facts = Vec::new();
    let size = cfg.cell.payload_size.min(8192);
    for i in 0..ops {
        // Synthetic proxy only: explicit encoded length (not payload+N invent).
        // real_store uses WriteReceipt.encoded_frame_len from the store boundary.
        facts.push(WriteReceiptFact {
            logical_len: size,
            encoded_frame_len: size.saturating_add(48), // synthetic frame proxy only
            durability: report.durability.clone(),
            segment_gen: (i / 8) as u32,
            segment_rotate: i > 0 && i % 8 == 0,
            chunked: false,
            chunk_count: 0,
        });
    }
    let plan = emit_plan_from_receipts(
        &cfg.cell.cell_id,
        &facts,
        32 * 1024,
        1024 * 1024,
        cfg.cell.batch_size.max(1),
    );

    let mut notes = report.messages.clone();
    notes.push("driver=synthetic NON-PRODUCT measurement surface".into());
    notes.push(format!(
        "plan_source={} (synthetic receipt stream)",
        super::emitter::STORE_SEAM_EMITTER_FROM_RECEIPTS
    ));

    Ok(DriverCellReport {
        cell: report,
        driver_kind: DriverKind::Synthetic,
        measurement_surface: MeasurementSurface::NonProductSynthetic,
        product_claim_eligible: false,
        // Synthetic plan is a harness proxy — not a lossless store-boundary plan.
        plan: Some(plan),
        plan_source: super::emitter::STORE_SEAM_EMITTER_FROM_RECEIPTS.into(),
        lossless_plan_eligible: false,
        boundary_aggregates: None,
        notes,
    })
}
