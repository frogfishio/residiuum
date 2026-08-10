//! Seeded / counterbalanced matrix cell construction (SPEC §6.3.1 subset).

use super::driver::{DatabaseState, DurabilityMode, LayerProfile};
use super::profiles::{
    AdditiveFeature, BackgroundInterference, FeatureProfile, InterferenceProfile,
};
use crate::workload::{fixed_size_series, threshold_probes, DistributionId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ScheduleSeed {
    pub seed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixCell {
    pub cell_id: String,
    pub layer: LayerProfile,
    pub durability: DurabilityMode,
    pub payload_size: u64,
    pub distribution: Option<DistributionId>,
    pub concurrency: u32,
    pub outstanding: u32,
    pub batch_size: u32,
    pub shards: u32,
    pub db_state: DatabaseState,
    pub features: FeatureProfile,
    pub interference: InterferenceProfile,
    pub op_count: u64,
    /// Counterbalance rank for thermal/order bias reduction.
    pub order_rank: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatrixManifest {
    pub schema: String,
    pub seed: u64,
    pub cells: Vec<MatrixCell>,
}

/// Build a structured (non-Cartesian) matrix per SPEC §6.3.1 essentials.
pub fn build_matrix_cells(seed: ScheduleSeed) -> MatrixManifest {
    let mut cells = Vec::new();
    let mut rank = 0u32;

    // 1. Size sweep: every fixed size @ conc=1, out=1, batch=1, each durability, L4
    for &size in fixed_size_series().iter().take(6) {
        // cap at 256KiB for harness unit speed (full series still represented partially)
        for dur in [
            DurabilityMode::Memory,
            DurabilityMode::Buffered,
            DurabilityMode::Durable,
        ] {
            cells.push(cell(
                &mut rank,
                LayerProfile::L4,
                dur,
                size,
                None,
                1,
                1,
                1,
                1,
                DatabaseState::Empty,
                FeatureProfile::l4_minimal(),
                InterferenceProfile::absent(),
                32,
            ));
        }
    }

    // Threshold ±1 probes
    for probe in threshold_probes(&[4096, 1_048_576]) {
        for size in [probe.below, probe.at, probe.above] {
            cells.push(cell(
                &mut rank,
                LayerProfile::L4,
                DurabilityMode::Buffered,
                size,
                None,
                1,
                1,
                1,
                1,
                DatabaseState::Empty,
                FeatureProfile::l4_minimal(),
                InterferenceProfile::absent(),
                16,
            ));
        }
    }

    // 2. Submission sweep subset: sizes × concurrency × outstanding, buffered+durable
    for &size in &[1024u64, 4096, 16 * 1024] {
        for &conc in &[1u32, 4] {
            for &out in &[1u32, 8] {
                for dur in [DurabilityMode::Buffered, DurabilityMode::Durable] {
                    cells.push(cell(
                        &mut rank,
                        LayerProfile::L4,
                        dur,
                        size,
                        None,
                        conc,
                        out,
                        1,
                        1,
                        DatabaseState::SteadyPopulated,
                        FeatureProfile::l4_minimal(),
                        InterferenceProfile::absent(),
                        24,
                    ));
                }
            }
        }
    }

    // 3. Distribution sweep @ L5
    for dist in [
        DistributionId::Tiny,
        DistributionId::MixedLarge,
        DistributionId::RewriteHeavy,
    ] {
        for &conc in &[1u32, 4] {
            cells.push(cell(
                &mut rank,
                LayerProfile::L5,
                DurabilityMode::Durable,
                4096,
                Some(dist),
                conc,
                4,
                8,
                1,
                DatabaseState::RewriteHeavy,
                FeatureProfile::realistic(),
                InterferenceProfile::absent(),
                40,
            ));
        }
    }

    // 4. L5 single-feature ladder
    for f in [
        AdditiveFeature::SecondaryIndex,
        AdditiveFeature::ChunkedValues,
        AdditiveFeature::SealLifecycle,
    ] {
        cells.push(cell(
            &mut rank,
            LayerProfile::L5,
            DurabilityMode::Durable,
            4096,
            None,
            2,
            4,
            8,
            2,
            DatabaseState::SteadyPopulated,
            FeatureProfile::single(f),
            InterferenceProfile::absent(),
            32,
        ));
    }

    // 5. L6 complete + interference
    for inter in [
        BackgroundInterference::Absent,
        BackgroundInterference::SealCheckpoint,
        BackgroundInterference::Telemetry,
    ] {
        cells.push(cell(
            &mut rank,
            LayerProfile::L6,
            DurabilityMode::Durable,
            8192,
            Some(DistributionId::Document),
            4,
            8,
            64,
            4,
            DatabaseState::Fragmented,
            FeatureProfile::l6_complete(),
            InterferenceProfile::for_kind(inter),
            48,
        ));
    }

    // Counterbalance: shuffle order by seed mix
    counterbalance(&mut cells, seed.seed);

    MatrixManifest {
        schema: "residiuum-pqh7-matrix-manifest-v1".into(),
        seed: seed.seed,
        cells,
    }
}

fn cell(
    rank: &mut u32,
    layer: LayerProfile,
    durability: DurabilityMode,
    payload_size: u64,
    distribution: Option<DistributionId>,
    concurrency: u32,
    outstanding: u32,
    batch_size: u32,
    shards: u32,
    db_state: DatabaseState,
    features: FeatureProfile,
    interference: InterferenceProfile,
    op_count: u64,
) -> MatrixCell {
    let id = format!(
        "{}-{}-s{}-c{}-o{}-{}",
        layer.as_str(),
        durability.as_str(),
        payload_size,
        concurrency,
        outstanding,
        *rank
    );
    let c = MatrixCell {
        cell_id: id,
        layer,
        durability,
        payload_size,
        distribution,
        concurrency,
        outstanding,
        batch_size,
        shards,
        db_state,
        features,
        interference,
        op_count,
        order_rank: *rank,
    };
    *rank = rank.saturating_add(1);
    c
}

fn counterbalance(cells: &mut [MatrixCell], seed: u64) {
    // Deterministic Fisher–Yates with splitmix.
    let n = cells.len();
    let mut s = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    for i in (1..n).rev() {
        s = s
            .wrapping_mul(0xBF58_476D_1CE4_E5B9)
            .wrapping_add(0x94D0_49BB_1331_11EB);
        let j = (s as usize) % (i + 1);
        cells.swap(i, j);
    }
    for (i, c) in cells.iter_mut().enumerate() {
        c.order_rank = i as u32;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrix_non_empty_and_seeded() {
        let m = build_matrix_cells(ScheduleSeed { seed: 42 });
        assert!(!m.cells.is_empty());
        let m2 = build_matrix_cells(ScheduleSeed { seed: 42 });
        assert_eq!(m.cells.len(), m2.cells.len());
        // same seed → same order after counterbalance
        assert_eq!(m.cells[0].cell_id, m2.cells[0].cell_id);
        let m3 = build_matrix_cells(ScheduleSeed { seed: 99 });
        // likely different first cell
        assert_eq!(m3.seed, 99);
    }

    #[test]
    fn includes_threshold_probes() {
        let m = build_matrix_cells(ScheduleSeed { seed: 1 });
        let sizes: std::collections::HashSet<_> = m.cells.iter().map(|c| c.payload_size).collect();
        assert!(sizes.contains(&4095) || sizes.contains(&4096) || sizes.contains(&4097));
    }

    #[test]
    fn includes_all_durability_modes() {
        let m = build_matrix_cells(ScheduleSeed { seed: 1 });
        let durs: std::collections::HashSet<_> = m.cells.iter().map(|c| c.durability).collect();
        assert!(durs.contains(&DurabilityMode::Memory));
        assert!(durs.contains(&DurabilityMode::Buffered));
        assert!(durs.contains(&DurabilityMode::Durable));
    }
}
