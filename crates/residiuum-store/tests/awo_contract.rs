//! AWO-0 integration: pure model agrees with frozen golden vectors.
//!
//! No store write-path exercise. Requires no product mutation.

use residiuum_store::adaptive_write::{decide, GoldenDecisionInput, AWO_PROFILE};
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
struct GoldenFile {
    profile: String,
    items: Vec<GoldenItem>,
}

#[derive(Debug, Deserialize)]
struct GoldenItem {
    id: String,
    queue_count: u64,
    natural_service_lower_ns: u64,
    batch_completion_upper_ns: u64,
    decision_margin_ppm: u32,
    evidence_warm: bool,
    evidence_stale: bool,
    compatible: bool,
    memory_ok: bool,
    earliest_deadline_ns: u64,
    expected_plan: String,
    expected_reason: String,
}

fn load() -> GoldenFile {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../spec/performance/awo/golden-decisions-v1.json");
    let text = std::fs::read_to_string(&path).expect("golden-decisions-v1.json");
    serde_json::from_str(&text).expect("parse goldens")
}

#[test]
fn golden_decisions_twelve_match_pure_model() {
    let g = load();
    assert_eq!(g.profile, AWO_PROFILE);
    assert_eq!(g.items.len(), 12, "closed golden set size");
    for item in &g.items {
        let d = decide(&GoldenDecisionInput {
            queue_count: item.queue_count,
            natural_service_lower_ns: item.natural_service_lower_ns,
            batch_completion_upper_ns: item.batch_completion_upper_ns,
            decision_margin_ppm: item.decision_margin_ppm,
            evidence_warm: item.evidence_warm,
            evidence_stale: item.evidence_stale,
            compatible: item.compatible,
            memory_ok: item.memory_ok,
            earliest_deadline_ns: item.earliest_deadline_ns,
        });
        assert_eq!(
            d.plan.as_str(),
            item.expected_plan.as_str(),
            "plan {}",
            item.id
        );
        assert_eq!(
            d.reason,
            item.expected_reason.as_str(),
            "reason {}",
            item.id
        );
    }
}
