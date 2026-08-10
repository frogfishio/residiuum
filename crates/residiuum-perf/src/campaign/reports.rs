//! Campaign reports: size/distribution/concurrency/durability, multiproc finding, bottlenecks.

use super::run::CampaignResult;
use crate::analyze::{
    classify_bottleneck, falsification_experiments, mean_mad, AttributionInput, RunEvidence,
    SampleStats,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SliceRow {
    pub key: String,
    pub n_runs: usize,
    pub median_throughput: f64,
    pub mean_throughput: f64,
    pub mad: f64,
    pub run_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiprocFinding {
    pub title: String,
    pub payload_sizes: Vec<u64>,
    pub process_count: u32,
    pub rows: Vec<SliceRow>,
    /// Honest statement: harness proxy, not product claim unless disclosure allows.
    pub statement: String,
    pub overstates_product: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedBottleneck {
    pub rank: u32,
    pub verdict: String,
    pub confidence: String,
    pub run_ids: Vec<String>,
    pub supporting_evidence: Vec<String>,
    pub falsification_ids: Vec<String>,
    pub allows_tuning_advice: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FollowUpCard {
    pub card_id: String,
    pub title: String,
    pub related_run_ids: Vec<String>,
    pub suggested_parameter: String,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignReports {
    pub fixed_size: Vec<SliceRow>,
    pub distribution: Vec<SliceRow>,
    pub concurrency: Vec<SliceRow>,
    pub durability: Vec<SliceRow>,
    pub multiproc_finding: MultiprocFinding,
    pub ranked_bottlenecks: Vec<RankedBottleneck>,
    pub follow_up_cards: Vec<FollowUpCard>,
    pub variability_cv_ok: bool,
    pub notes: Vec<String>,
}

/// Build all campaign report slices from completed repetitions.
pub fn build_campaign_reports(result: &CampaignResult) -> CampaignReports {
    let mut fixed_size: BTreeMap<u64, Vec<(&str, f64)>> = BTreeMap::new();
    let mut durability: BTreeMap<String, Vec<(&str, f64)>> = BTreeMap::new();
    let mut concurrency: BTreeMap<u32, Vec<(&str, f64)>> = BTreeMap::new();
    let mut distribution: BTreeMap<String, Vec<(&str, f64)>> = BTreeMap::new();
    let mut multiproc: BTreeMap<u64, Vec<(&str, f64)>> = BTreeMap::new();

    for rep in &result.repetitions {
        if rep.report.validity != "valid" {
            continue;
        }
        let t = rep.report.throughput_bytes_per_sec_proxy;
        let id = rep.run_id.as_str();

        // Parse payload size from cell_id if present: cells embed size in id loosely;
        // use throughput grouping via report fields we have.
        // CellRunReport does not carry payload_size; recover from cell_id patterns or skip.
        // Prefer matching known multiproc sizes via cell_id containing size tokens.
        let cell = &rep.cell_id;
        if let Some(sz) = extract_size_token(cell) {
            fixed_size.entry(sz).or_default().push((id, t));
            if (sz == 4096 || sz == 8192) && rep.process_id > 0 {
                multiproc.entry(sz).or_default().push((id, t));
            }
        }
        durability
            .entry(rep.report.durability.clone())
            .or_default()
            .push((id, t));
        // concurrency proxy: interference string or features — use process_id aggregate later
        concurrency.entry(rep.process_id).or_default().push((id, t));
        distribution
            .entry(rep.report.layer.clone())
            .or_default()
            .push((id, t));
    }

    // Also aggregate multiproc from all 4k/8k cells (including process 0)
    for rep in &result.repetitions {
        if rep.report.validity != "valid" {
            continue;
        }
        if let Some(sz) = extract_size_token(&rep.cell_id) {
            if sz == 4096 || sz == 8192 {
                multiproc.entry(sz).or_default().push((
                    rep.run_id.as_str(),
                    rep.report.throughput_bytes_per_sec_proxy,
                ));
            }
        }
    }

    let fixed_size = map_to_rows_u64(&fixed_size);
    let durability = map_to_rows_str(&durability);
    let concurrency = map_to_rows_u32(&concurrency);
    let distribution = map_to_rows_str(&distribution);

    let mp_rows = map_to_rows_u64(&multiproc);
    let process_count = result.plan.processes;
    let overstates = result.plan.platform.allows_product_baseline();
    let multiproc_finding = MultiprocFinding {
        title: "4 KiB / 8 KiB multi-process finding".into(),
        payload_sizes: vec![4096, 8192],
        process_count,
        rows: mp_rows.clone(),
        statement: if mp_rows.is_empty() {
            "No 4 KiB/8 KiB multiproc cells present in this campaign slice; finding deferred."
                .into()
        } else if overstates {
            format!(
                "Harness re-measured 4 KiB/8 KiB multiproc cells across {process_count} process slots; see rows for proxy throughput. Product claim requires full disclosure on controlled runner."
            )
        } else {
            format!(
                "Synthetic harness multiproc slice ({process_count} process slots) for 4 KiB/8 KiB — NOT a product performance claim."
            )
        },
        overstates_product: false, // never auto-claim product surface
    };

    let ranked_bottlenecks = rank_bottlenecks(result);
    let follow_up_cards = follow_ups(&ranked_bottlenecks);

    // Variability: CV within 5% on any fixed-size row with n>=5 → ok flag for campaign
    let variability_cv_ok = fixed_size
        .iter()
        .any(|r| r.n_runs >= 5 && r.mean_throughput > 0.0 && (r.mad / r.mean_throughput) <= 0.05)
        || fixed_size.is_empty();

    let mut notes = vec![
        "No optimization applied in PQH-9; follow-up cards are stubs only".into(),
        format!(
            "platform={} allows_product_baseline={}",
            result.plan.platform.as_str(),
            result.plan.platform.allows_product_baseline()
        ),
        "attribution: no synthesized L1/L2/L3, stage, residual, queue, CPU, or OS inputs; missing evidence → mixed_or_unknown".into(),
    ];
    if !result.plan.platform.allows_product_baseline() {
        notes.push(
            "Synthetic/harness platform: do not publish absolute MB/s as product qualification"
                .into(),
        );
    }
    if ranked_bottlenecks
        .iter()
        .all(|b| b.verdict == "mixed_or_unknown")
    {
        notes.push(
            "all ranked bottlenecks are mixed_or_unknown (expected without full ladder/stage/OS evidence)"
                .into(),
        );
    }

    CampaignReports {
        fixed_size,
        distribution,
        concurrency,
        durability,
        multiproc_finding,
        ranked_bottlenecks,
        follow_up_cards,
        variability_cv_ok,
        notes,
    }
}

fn rank_bottlenecks(result: &CampaignResult) -> Vec<RankedBottleneck> {
    // Group by cell_id; classify each accepted cell with aggregated samples.
    //
    // **Honesty:** never invent L0/L1/L2/L3 envelope throughputs, stage sums,
    // residual fractions, queue depth, device util, or CPU/OS probes. Missing
    // evidence forces `mixed_or_unknown` (no product bottleneck claim).
    let mut by_cell: BTreeMap<String, Vec<&super::run::CellRepetition>> = BTreeMap::new();
    for r in &result.repetitions {
        if r.report.validity == "valid" {
            by_cell.entry(r.cell_id.clone()).or_default().push(r);
        }
    }

    let mut ranked = Vec::new();
    for (cell_id, reps) in by_cell {
        if reps.len() < 2 {
            continue;
        }
        let samples: Vec<f64> = reps
            .iter()
            .map(|r| r.report.throughput_bytes_per_sec_proxy)
            .collect();
        // Per-cell measured overhead when present (never invent).
        let measured_oh = result
            .observer_overhead_reports
            .iter()
            .find(|r| r.cell_id == cell_id)
            .or_else(|| result.observer_overhead_report.as_ref())
            .map(|r| r.overhead_fraction);
        let subject = run_evidence_from(reps[0], &samples, measured_oh);
        let attr = classify_bottleneck(&AttributionInput {
            subject,
            matched: None,
            throughput_samples: samples,
            // Envelope ladder: only pass when measured; never synthesize.
            t_l0: None,
            t_l2: None,
            t_l3: None,
            // Stage residual: incomplete without real stage accounting.
            e2e_latency_ns: None,
            stage_sum_ns: None,
            // OS/CPU/queue/lifecycle flags: false means "not observed true",
            // not "probed false". Classifier requires positive evidence to fire.
            single_core_saturated: false,
            multi_store: false,
            page_cache_warm_only: false,
            lifecycle_spikes_present: false,
            telemetry_delta_exceeds_budget: false,
            memory_faults_align: false,
            lock_residence_dominates: false,
            index_stage_explains_loss: false,
        });
        let fals = falsification_experiments(&attr);
        ranked.push(RankedBottleneck {
            rank: 0,
            verdict: attr.verdict,
            confidence: attr.confidence,
            run_ids: reps.iter().map(|r| r.run_id.clone()).collect(),
            supporting_evidence: {
                let mut s = attr.supporting_evidence;
                s.push(format!("cell_id={cell_id}"));
                s.push("no synthesized L1/L2/L3/stage/queue/CPU/OS attribution inputs".into());
                s
            },
            falsification_ids: fals.into_iter().map(|f| f.id).collect(),
            allows_tuning_advice: attr.allows_tuning_advice,
        });
    }

    // Prefer non-mixed first; stable by verdict name
    ranked.sort_by(|a, b| {
        let am = a.verdict == "mixed_or_unknown";
        let bm = b.verdict == "mixed_or_unknown";
        am.cmp(&bm).then_with(|| a.verdict.cmp(&b.verdict))
    });
    for (i, r) in ranked.iter_mut().enumerate() {
        r.rank = (i + 1) as u32;
    }
    ranked.truncate(16);
    ranked
}

fn follow_ups(ranked: &[RankedBottleneck]) -> Vec<FollowUpCard> {
    ranked
        .iter()
        .filter(|r| r.verdict != "mixed_or_unknown")
        .take(5)
        .enumerate()
        .map(|(i, r)| FollowUpCard {
            card_id: format!("pqh9-opt-stub-{}", i + 1),
            title: format!("Investigate {}", r.verdict),
            related_run_ids: r.run_ids.clone(),
            suggested_parameter: r
                .falsification_ids
                .first()
                .cloned()
                .unwrap_or_else(|| "one_registered_parameter".into()),
            note: "Stub only — PQH-9 does not implement optimizations; rerun affected cells after a single registered parameter change.".into(),
        })
        .collect()
}

/// Build RunEvidence from campaign cell output only — no invented probes.
///
/// Latency percentiles, residual, device util, queue depth, and CPU/OS fields
/// stay `None`/zero unless a real probe provided them (campaign path does not).
/// `observer_overhead_fraction` is filled only from a campaign-measured
/// `ObserverOverheadReport` (order-balanced probe-off/on), never invented.
fn run_evidence_from(
    rep: &super::run::CellRepetition,
    samples: &[f64],
    observer_overhead_fraction: Option<f64>,
) -> RunEvidence {
    let stats = mean_mad(samples);
    let mean_t = stats
        .as_ref()
        .map(|s| s.mean)
        .unwrap_or(rep.report.throughput_bytes_per_sec_proxy);
    RunEvidence {
        run_id: rep.run_id.clone(),
        layer: rep.report.layer.clone(),
        durability: rep.report.durability.clone(),
        payload_size: extract_size_token(&rep.cell_id).unwrap_or(4096),
        concurrency: 1,
        outstanding: 1,
        config_hash: format!("cell:{}", rep.cell_id),
        throughput_bytes_per_sec: mean_t,
        // Not histogram-measured in campaign path.
        p50_latency_ns: None,
        p99_latency_ns: None,
        // Stage residual not closed without L3 stage probes.
        residual_fraction: None,
        validity: rep.report.validity.clone(),
        failed_ops: rep.report.failed,
        attempted_ops: rep.report.attempted,
        acknowledged_ops: rep.report.acknowledged,
        // OS/device/queue probes absent → None/0 (not synthesized defaults).
        device_util: None,
        outstanding_depth: 0,
        sync_per_op: rep.report.durability == "durable",
        observer_overhead_fraction,
        cpu_cores_busy: None,
        aggregate_cpu_idle: None,
        window_class: rep.report.window.clone(),
        features: rep.report.features.clone(),
    }
}

fn extract_size_token(cell_id: &str) -> Option<u64> {
    // Matrix cell_ids look like "L4-durable-4096-c1-o1-..." style — scan numeric tokens.
    for part in cell_id.split(|c: char| !c.is_ascii_digit()) {
        if part.is_empty() {
            continue;
        }
        if let Ok(n) = part.parse::<u64>() {
            // plausible payload sizes
            if (64..64 * 1024 * 1024).contains(&n) {
                return Some(n);
            }
        }
    }
    None
}

fn map_to_rows_u64(m: &BTreeMap<u64, Vec<(&str, f64)>>) -> Vec<SliceRow> {
    m.iter().map(|(k, v)| slice_row(k.to_string(), v)).collect()
}

fn map_to_rows_u32(m: &BTreeMap<u32, Vec<(&str, f64)>>) -> Vec<SliceRow> {
    m.iter().map(|(k, v)| slice_row(k.to_string(), v)).collect()
}

fn map_to_rows_str(m: &BTreeMap<String, Vec<(&str, f64)>>) -> Vec<SliceRow> {
    m.iter().map(|(k, v)| slice_row(k.clone(), v)).collect()
}

fn slice_row(key: String, pairs: &[(&str, f64)]) -> SliceRow {
    let samples: Vec<f64> = pairs.iter().map(|(_, t)| *t).collect();
    let stats: Option<SampleStats> = mean_mad(&samples);
    let run_ids: Vec<String> = pairs.iter().map(|(id, _)| (*id).to_string()).collect();
    SliceRow {
        key,
        n_runs: pairs.len(),
        median_throughput: stats.as_ref().map(|s| s.median).unwrap_or(0.0),
        mean_throughput: stats.as_ref().map(|s| s.mean).unwrap_or(0.0),
        mad: stats.as_ref().map(|s| s.mad).unwrap_or(0.0),
        run_ids,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::campaign::plan::campaign_plan_synthetic;
    use crate::campaign::run::{run_campaign, CampaignConfig};

    #[test]
    fn reports_have_slices_and_no_opt() {
        let plan = campaign_plan_synthetic(99, 4);
        let result = run_campaign(&CampaignConfig::synthetic(plan)).unwrap();
        let reports = build_campaign_reports(&result);
        assert!(!reports.durability.is_empty() || result.valid_runs == 0);
        assert!(reports.notes.iter().any(|n| n.contains("No optimization")));
        for c in &reports.follow_up_cards {
            assert!(c.note.contains("Stub only"));
            assert!(!c.related_run_ids.is_empty());
        }
        assert!(!reports.multiproc_finding.overstates_product);
    }

    #[test]
    fn campaign_reports_refuse_synthesized_bottlenecks() {
        // Without L1/L2/L3 ladder, stage residual, or OS probes, campaign path
        // must not invent io_queue_underdriven (or any non-mixed primary).
        let plan = campaign_plan_synthetic(77, 3);
        let result = run_campaign(&CampaignConfig::synthetic(plan)).unwrap();
        let reports = build_campaign_reports(&result);
        assert!(
            reports
                .notes
                .iter()
                .any(|n| n.contains("no synthesized") || n.contains("mixed_or_unknown")),
            "expected honesty note about synthesized attribution"
        );
        for b in &reports.ranked_bottlenecks {
            assert_eq!(
                b.verdict, "mixed_or_unknown",
                "unexpected synthesized verdict {} run_ids={:?}",
                b.verdict, b.run_ids
            );
            assert!(!b.allows_tuning_advice);
        }
        // No follow-up opt stubs from non-mixed verdicts.
        assert!(
            reports.follow_up_cards.is_empty()
                || reports
                    .follow_up_cards
                    .iter()
                    .all(|c| c.note.contains("Stub only"))
        );
    }
}
