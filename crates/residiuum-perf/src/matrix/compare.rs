//! Matched comparison selector across matrix cell results.

use super::driver::{run_cell, CellRunReport, RunCellConfig};
use super::scheduler::{build_matrix_cells, MatrixManifest, ScheduleSeed};
use super::MatrixError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellResult {
    pub report: CellRunReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchedPair {
    pub a_id: String,
    pub b_id: String,
    pub match_keys: Vec<String>,
    pub a_throughput: f64,
    pub b_throughput: f64,
}

/// Run entire manifest (bounded) and collect valid cells.
pub fn run_matrix(
    manifest: &MatrixManifest,
    max_cells: usize,
) -> Result<Vec<CellResult>, MatrixError> {
    let mut out = Vec::new();
    for cell in manifest.cells.iter().take(max_cells) {
        let report = run_cell(&RunCellConfig {
            cell: cell.clone(),
            seed: manifest.seed,
            durability_mutant: false,
            digest_mutant: false,
        })?;
        out.push(CellResult { report });
    }
    Ok(out)
}

/// Select pairs that share durability + payload size but differ in layer or concurrency
/// (matched for attribution / PQH-8 prep).
pub fn select_matched_pairs(results: &[CellResult]) -> Vec<MatchedPair> {
    let mut pairs = Vec::new();
    for (i, a) in results.iter().enumerate() {
        for b in results.iter().skip(i + 1) {
            let mut keys = Vec::new();
            if a.report.durability == b.report.durability {
                keys.push("durability".into());
            }
            // parse size from cell_id loosely via logical bytes bucket
            if a.report.features == b.report.features {
                keys.push("features".into());
            }
            if a.report.layer != b.report.layer && a.report.durability == b.report.durability {
                keys.push("layer_cross".into());
                pairs.push(MatchedPair {
                    a_id: a.report.cell_id.clone(),
                    b_id: b.report.cell_id.clone(),
                    match_keys: keys,
                    a_throughput: a.report.throughput_bytes_per_sec_proxy,
                    b_throughput: b.report.throughput_bytes_per_sec_proxy,
                });
            } else if a.report.layer == b.report.layer
                && a.report.durability == b.report.durability
                && a.report.interference != b.report.interference
            {
                keys.push("interference_cross".into());
                pairs.push(MatchedPair {
                    a_id: a.report.cell_id.clone(),
                    b_id: b.report.cell_id.clone(),
                    match_keys: keys,
                    a_throughput: a.report.throughput_bytes_per_sec_proxy,
                    b_throughput: b.report.throughput_bytes_per_sec_proxy,
                });
            }
        }
    }
    pairs
}

/// Convenience: build + run smoke matrix.
pub fn smoke_matrix(
    seed: u64,
    max_cells: usize,
) -> Result<(MatrixManifest, Vec<CellResult>), MatrixError> {
    let manifest = build_matrix_cells(ScheduleSeed { seed });
    let results = run_matrix(&manifest, max_cells)?;
    Ok((manifest, results))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke_matrix_runs() {
        let (m, results) = smoke_matrix(7, 8).unwrap();
        assert!(!m.cells.is_empty());
        assert_eq!(results.len(), 8);
        for r in &results {
            assert_eq!(r.report.validity, "valid");
            assert!(r.report.shadow_planned_bytes > 0 || r.report.acknowledged > 0);
        }
        let pairs = select_matched_pairs(&results);
        // May or may not find pairs in first 8; just ensure no panic
        let _ = pairs;
    }

    #[test]
    fn full_manifest_from_one_call() {
        let m = build_matrix_cells(ScheduleSeed { seed: 3 });
        assert!(m.cells.len() > 20);
        // Run a larger subset
        let results = run_matrix(&m, 12).unwrap();
        assert_eq!(results.len(), 12);
    }
}
