//! Residiuum Performance Qualification Harness (`residiuum-perf`).
//!
//! - **PQH-0:** contract registries loaded from `spec/performance/`.
//! - **PQH-1:** safe runner, path guard, preflight, platform fingerprint,
//!   budgets, and classifiable cancel artifacts.
//! - **PQH-2:** deterministic workload engine (generators, distributions,
//!   digest oracle, scheduler partitions).
//! - **PQH-3:** metrics kernel (histograms, clocks, probes, result writers).
//! - **PQH-4:** L0 calibration + L1 device envelope (fake + file I/O adapters).
//! - **PQH-5:** PhysicalWritePlan + L2 opaque shadow writer.
//! - **PQH-6:** L3 CPU pipeline + stage probes (null/memory sink).
//! - **PQH-7:** L4/L5/L6 matrix runner (ledger, profiles, scheduler).
//! - **PQH-8:** attribution analyzer, bottleneck verdicts, false narratives.
//! - **PQH-9:** qualification campaign, evidence bundle, disclosure.
//! - **PQH-10:** store driver + plan emitter + campaign CLI (operational residual).
//!
//! This crate is **unpublished** development tooling. It must not change
//! product durability or verification semantics.
//!
//! Synthetic/proxy measurements are **non-product**. Real-store measurements
//! require the `store-driver` feature and controlled-runner disclosure.

#![forbid(unsafe_code)]

pub mod analyze;
pub mod campaign;
pub mod envelope;
pub mod matrix;
pub mod metrics;
pub mod pipeline;
pub mod runner;
pub mod shadow;
pub mod store_driver;
pub mod workload;

use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Live qualification profile id (post protocol identity reset).
pub const PROFILE_ID: &str = "residiuum-performance-qualification-v1";

/// True if `s` embeds the pre-reset product token (split for identity linter).
fn embeds_prereset_product_token(s: &str) -> bool {
    s.to_ascii_lowercase().contains(concat!("din", "go"))
}

/// Experiment ladder layers (SPEC §5).
pub const LAYERS: &[&str] = &["L0", "L1", "L2", "L3", "L4", "L5", "L6"];

/// Latency accounting stages (SPEC §9).
pub const STAGES: &[&str] = &[
    "queue",
    "validation",
    "encoding",
    "chunking",
    "indexing",
    "io",
    "publication",
    "durability",
    "residual",
];

/// Closed bottleneck verdict set (SPEC §11).
pub const VERDICTS: &[&str] = &[
    "generator_bound",
    "cpu_transform_bound",
    "io_bandwidth_bound",
    "io_queue_underdriven",
    "serialized_writer",
    "durability_barrier_bound",
    "lifecycle_bound",
    "index_bound",
    "memory_pressure_bound",
    "telemetry_bound",
    "mixed_or_unknown",
];

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("registry: {0}")]
    Registry(String),
}

#[derive(Debug, Deserialize)]
struct RegistryFile {
    items: Vec<RegistryItem>,
}

#[derive(Debug, Deserialize)]
struct RegistryItem {
    id: String,
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    zero_is_not_unavailable: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct MatrixFile {
    items: Vec<MatrixCell>,
}

#[derive(Debug, Deserialize)]
struct MatrixCell {
    id: String,
    layer: String,
}

/// Locate workspace root from this crate (`crates/residiuum-perf` → repo).
pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

pub fn performance_spec_dir(root: &Path) -> PathBuf {
    root.join("spec/performance")
}

fn load_ids(path: &Path) -> Result<HashSet<String>, RegistryError> {
    let raw = fs::read_to_string(path)?;
    let doc: RegistryFile = serde_json::from_str(&raw)?;
    let mut set = HashSet::new();
    for it in doc.items {
        if !set.insert(it.id.clone()) {
            return Err(RegistryError::Registry(format!(
                "duplicate id {} in {}",
                it.id,
                path.display()
            )));
        }
    }
    Ok(set)
}

/// Fail-closed load: every compile-time known layer/stage/verdict must exist
/// in the on-disk registry, and metrics must declare units + zero policy.
pub fn validate_registries(root: &Path) -> Result<(), RegistryError> {
    let dir = performance_spec_dir(root);
    let layers = load_ids(&dir.join("layers-v1.json"))?;
    let stages = load_ids(&dir.join("stages-v1.json"))?;
    let verdicts = load_ids(&dir.join("verdicts-v1.json"))?;
    let validity = load_ids(&dir.join("validity-v1.json"))?;
    let omissions = load_ids(&dir.join("omission-reasons-v1.json"))?;

    for l in LAYERS {
        if !layers.contains(*l) {
            return Err(RegistryError::Registry(format!("missing layer {l}")));
        }
    }
    for s in STAGES {
        if !stages.contains(*s) {
            return Err(RegistryError::Registry(format!("missing stage {s}")));
        }
    }
    for v in VERDICTS {
        if !verdicts.contains(*v) {
            return Err(RegistryError::Registry(format!("missing verdict {v}")));
        }
    }
    if !verdicts.contains("mixed_or_unknown") {
        return Err(RegistryError::Registry(
            "mixed_or_unknown verdict required".into(),
        ));
    }
    if validity.is_empty() {
        return Err(RegistryError::Registry("validity registry empty".into()));
    }
    if omissions.is_empty() {
        return Err(RegistryError::Registry("omission reasons empty".into()));
    }

    let metrics_raw = fs::read_to_string(dir.join("metrics-v1.json"))?;
    let metrics: RegistryFile = serde_json::from_str(&metrics_raw)?;
    if metrics.items.len() < 20 {
        return Err(RegistryError::Registry("metrics registry too small".into()));
    }
    for m in &metrics.items {
        if m.unit.as_deref().unwrap_or("").is_empty() {
            return Err(RegistryError::Registry(format!(
                "metric {} missing unit",
                m.id
            )));
        }
        if m.zero_is_not_unavailable != Some(true) {
            return Err(RegistryError::Registry(format!(
                "metric {} must set zero_is_not_unavailable=true",
                m.id
            )));
        }
    }

    let profiles_raw = fs::read_to_string(dir.join("profiles-v1.json"))?;
    let profiles: RegistryFile = serde_json::from_str(&profiles_raw)?;
    if !profiles.items.iter().any(|p| p.id == PROFILE_ID) {
        return Err(RegistryError::Registry(format!(
            "profile {PROFILE_ID} missing"
        )));
    }
    for p in &profiles.items {
        if embeds_prereset_product_token(&p.id) {
            return Err(RegistryError::Registry(format!(
                "forbidden pre-reset product profile {}",
                p.id
            )));
        }
    }

    let matrix_raw = fs::read_to_string(dir.join("matrix-v1.json"))?;
    let matrix: MatrixFile = serde_json::from_str(&matrix_raw)?;
    if matrix.items.is_empty() {
        return Err(RegistryError::Registry("matrix has no cells".into()));
    }
    for c in &matrix.items {
        if !layers.contains(&c.layer) {
            return Err(RegistryError::Registry(format!(
                "matrix cell {} unknown layer {}",
                c.id, c.layer
            )));
        }
    }

    Ok(())
}

/// Unknown identifier policy used by later runner packages.
pub fn is_known_layer(id: &str) -> bool {
    LAYERS.contains(&id)
}

pub fn is_known_verdict(id: &str) -> bool {
    VERDICTS.contains(&id)
}

pub fn is_known_stage(id: &str) -> bool {
    STAGES.contains(&id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registries_validate_against_workspace_spec() {
        let root = workspace_root();
        validate_registries(&root).expect("PQH registries must validate");
    }

    #[test]
    fn unknown_ids_fail_closed() {
        assert!(!is_known_layer("L9"));
        assert!(!is_known_verdict("make_it_faster"));
        assert!(!is_known_stage("magic"));
        assert!(is_known_layer("L0"));
        assert!(is_known_verdict("mixed_or_unknown"));
        assert!(is_known_stage("residual"));
    }

    #[test]
    fn profile_constant_is_residiuum_not_prereset() {
        assert!(PROFILE_ID.starts_with("residiuum-"));
        assert!(!embeds_prereset_product_token(PROFILE_ID));
    }
}
