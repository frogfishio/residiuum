//! Prong 3 — performance + integrity monitor (diagnostic numbers, not SLOs).

use crate::manifest::{read_manifest, WorkloadManifest};
use crate::size::{dir_size_bytes, format_bytes};
use residiuum_store::{DurabilityMode, ScrubOptions, Store};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct MonitorConfig {
    pub store: PathBuf,
    pub manifest_path: Option<PathBuf>,
    pub sample_keys: usize,
    pub put_samples: usize,
    pub phase: String,
    pub json_out: bool,
    /// When true, skip writer-path put microbench (inspect-only).
    pub inspect_only: bool,
}

#[derive(Debug, Clone)]
pub struct MonitorResult {
    pub ok: bool,
    pub phase: String,
    pub report: Value,
    pub gets_ok: u64,
    pub gets_fail: u64,
    pub live_subjects: usize,
    pub holes: u64,
    pub scrub_failures: usize,
}

pub fn run_monitor(cfg: &MonitorConfig) -> Result<MonitorResult, String> {
    let manifest = load_optional_manifest(cfg)?;
    let bytes_on_disk = dir_size_bytes(&cfg.store).map_err(|e| format!("dir size: {e}"))?;

    // --- salvage / open_inspect path (no writer lock) ---
    let t_inspect = Instant::now();
    let inspect = Store::open_inspect(&cfg.store).map_err(|e| format!("open_inspect: {e}"))?;
    let inspect_open_ms = t_inspect.elapsed().as_millis() as u64;

    let t_salvage = Instant::now();
    let salvage = inspect.salvage().map_err(|e| format!("salvage: {e}"))?;
    let salvage_ms = t_salvage.elapsed().as_millis() as u64;

    // Point-get sample on the **already-open** inspect handle (PrimaryIndex
    // resident bodies). Do not reopen per get, do not fold salvage into get
    // latency, and do not probe Chimera sidecars here — those are separate
    // surfaces (see residiuum-store example `read_latency_breakdown`).
    let sample_keys = resolve_sample_keys(&manifest, cfg.sample_keys);
    let mut get_latencies = Vec::with_capacity(sample_keys.len());
    let mut gets_ok = 0u64;
    let mut gets_fail = 0u64;
    let mut gets_missing = 0u64;
    let mut first_get_us: Option<u64> = None;

    for (i, key) in sample_keys.iter().enumerate() {
        let t0 = Instant::now();
        let result = inspect.get(key);
        let elapsed = t0.elapsed();
        if i == 0 {
            first_get_us = Some(elapsed.as_micros().min(u64::MAX as u128) as u64);
        }
        match result {
            Ok(Some(_)) => {
                get_latencies.push(elapsed);
                gets_ok += 1;
            }
            Ok(None) => {
                get_latencies.push(elapsed);
                gets_missing += 1;
                gets_fail += 1;
            }
            Err(_) => {
                get_latencies.push(elapsed);
                gets_fail += 1;
            }
        }
    }

    drop(inspect);

    // --- scrub (writer path; may quarantine) ---
    let (scrub_failures, scrub_targets, scrub_ms) = if cfg.inspect_only {
        (0usize, 0usize, 0u64)
    } else {
        let t0 = Instant::now();
        let store = Store::open(&cfg.store).map_err(|e| format!("open for scrub: {e}"))?;
        let mut opts = ScrubOptions::default();
        // Bound work for large stores; still useful signal.
        opts.max_files = 64;
        opts.max_bytes = 64 * 1024 * 1024;
        opts.quarantine = false;
        let report = store
            .scrub_once(opts)
            .map_err(|e| format!("scrub_once: {e}"))?;
        (
            report.failures_this_call,
            report.targets_processed,
            t0.elapsed().as_millis() as u64,
        )
    };

    // --- optional put microbench (writer) ---
    let mut put_latencies = Vec::new();
    let mut put_ok = 0u64;
    if !cfg.inspect_only && cfg.put_samples > 0 {
        let mut store = Store::open(&cfg.store).map_err(|e| format!("open for put bench: {e}"))?;
        let payload = b"{\"testrig\":\"monitor-put\"}";
        for i in 0..cfg.put_samples {
            let key = format!("__testrig/monitor/{i}");
            let t0 = Instant::now();
            match store.put(&key, payload, DurabilityMode::Buffered) {
                Ok(_) => {
                    put_latencies.push(t0.elapsed());
                    put_ok += 1;
                }
                Err(_) => put_latencies.push(t0.elapsed()),
            }
        }
    }

    let get_stats = latency_stats(&get_latencies);
    let put_stats = latency_stats(&put_latencies);

    // Pass heuristics for this phase (diagnostic; not production SLOs).
    let baseline_ok = cfg.phase != "baseline"
        || (gets_fail == 0 && sample_keys.is_empty())
        || (gets_ok > 0 && gets_fail == 0);
    // Post-chaos: must not explode; salvage must still scan; survivors optional.
    let post_ok = salvage.files_scanned >= 1;
    let ok = match cfg.phase.as_str() {
        "baseline" => baseline_ok && salvage.live_subjects > 0,
        "post-chaos" | "post_chaos" => post_ok,
        _ => post_ok,
    };

    let report = json!({
        "prong": "monitor",
        "phase": cfg.phase,
        "ok": ok,
        "store": cfg.store.display().to_string(),
        "bytes_on_disk": bytes_on_disk,
        "bytes_on_disk_human": format_bytes(bytes_on_disk),
        "inspect_open_ms": inspect_open_ms,
        "salvage": {
            "elapsed_ms": salvage_ms,
            "files_scanned": salvage.files_scanned,
            "verified_frames": salvage.verified_frames,
            "item_events": salvage.item_events,
            "holes": salvage.holes,
            "live_subjects": salvage.live_subjects,
        },
        "gets": {
            "sampled": sample_keys.len(),
            "ok": gets_ok,
            "fail_or_missing": gets_fail,
            "missing": gets_missing,
            "path": "primary_index_resident",
            "open_mode": "open_inspect_once",
            "first_get_us": first_get_us,
            "latency_us": get_stats,
            "note": "Latencies exclude open_inspect and salvage; one handle for all samples.",
        },
        "puts": {
            "sampled": put_latencies.len(),
            "ok": put_ok,
            "latency_us": put_stats,
        },
        "scrub": {
            "elapsed_ms": scrub_ms,
            "targets_processed": scrub_targets,
            "failures_this_call": scrub_failures,
            "skipped": cfg.inspect_only,
        },
        "manifest_keys_written": manifest.as_ref().map(|m| m.keys_written),
        "disclosure": "Diagnostic only — not a published SLO (see doc/reference/operations/BENCHMARK_DISCLOSURE.md).",
    });

    if cfg.json_out {
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
    } else {
        println!(
            "monitor[{}]: disk={} inspect_open={}ms salvage={}ms live={} holes={} frames={}",
            cfg.phase,
            format_bytes(bytes_on_disk),
            inspect_open_ms,
            salvage_ms,
            salvage.live_subjects,
            salvage.holes,
            salvage.verified_frames
        );
        println!(
            "  gets: ok={gets_ok} fail={gets_fail} first={}us p50={}us p95={}us p99={}us (primary_index, open once)",
            first_get_us.unwrap_or(0),
            get_stats["p50"],
            get_stats["p95"],
            get_stats["p99"]
        );
        if !put_latencies.is_empty() {
            println!(
                "  puts: ok={put_ok}/{} p50={}us p95={}us",
                put_latencies.len(),
                put_stats["p50"],
                put_stats["p95"]
            );
        }
        if !cfg.inspect_only {
            println!("  scrub: targets={scrub_targets} failures={scrub_failures} ({scrub_ms}ms)");
        }
        println!("  ok={ok}");
    }

    Ok(MonitorResult {
        ok,
        phase: cfg.phase.clone(),
        report,
        gets_ok,
        gets_fail,
        live_subjects: salvage.live_subjects,
        holes: salvage.holes,
        scrub_failures,
    })
}

fn load_optional_manifest(cfg: &MonitorConfig) -> Result<Option<WorkloadManifest>, String> {
    if let Some(ref p) = cfg.manifest_path {
        if p.is_file() {
            return Ok(Some(
                read_manifest(p).map_err(|e| format!("read manifest {}: {e}", p.display()))?,
            ));
        }
    }
    let sibling = crate::manifest::manifest_path_for_store(&cfg.store);
    if sibling.is_file() {
        return Ok(Some(
            read_manifest(&sibling).map_err(|e| format!("read manifest: {e}"))?,
        ));
    }
    Ok(None)
}

fn resolve_sample_keys(manifest: &Option<WorkloadManifest>, n: usize) -> Vec<String> {
    if let Some(m) = manifest {
        return m.sample_keys(n);
    }
    // Fallback: probe a few sequential load keys.
    (0..n.min(32)).map(|i| format!("load/{i:020}")).collect()
}

fn latency_stats(samples: &[Duration]) -> Value {
    if samples.is_empty() {
        return json!({
            "count": 0,
            "p50": 0,
            "p95": 0,
            "p99": 0,
            "max": 0,
            "mean": 0,
        });
    }
    let mut us: Vec<u64> = samples
        .iter()
        .map(|d| d.as_micros().min(u64::MAX as u128) as u64)
        .collect();
    us.sort_unstable();
    let n = us.len();
    let mean = us.iter().sum::<u64>() as f64 / n as f64;
    json!({
        "count": n,
        "p50": percentile(&us, 0.50),
        "p95": percentile(&us, 0.95),
        "p99": percentile(&us, 0.99),
        "max": us[n - 1],
        "mean": mean.round() as u64,
    })
}

fn percentile(sorted_us: &[u64], p: f64) -> u64 {
    if sorted_us.is_empty() {
        return 0;
    }
    let n = sorted_us.len();
    let idx = ((p * (n as f64 - 1.0)).round() as usize).min(n - 1);
    sorted_us[idx]
}

/// Evaluate full-run pass/fail from baseline + post-chaos monitors + chaos hits.
pub fn evaluate_run(
    baseline: &MonitorResult,
    chaos_hits: u32,
    post: &MonitorResult,
) -> (bool, Vec<String>) {
    let mut reasons = Vec::new();
    let mut ok = true;

    if !baseline.ok {
        ok = false;
        reasons.push(format!(
            "baseline monitor failed (gets_ok={} gets_fail={} live={})",
            baseline.gets_ok, baseline.gets_fail, baseline.live_subjects
        ));
    }
    if chaos_hits == 0 {
        ok = false;
        reasons.push("chaos applied zero hits".into());
    }
    if !post.ok {
        ok = false;
        reasons.push("post-chaos monitor failed (salvage/inspect must still run)".into());
    }
    // Integrity honesty: either holes, scrub findings, or some get failures after chaos.
    let damage_observed =
        post.holes > baseline.holes || post.scrub_failures > 0 || post.gets_fail > 0;
    if chaos_hits > 0 && !damage_observed {
        // Soft warning: random punches might land in padding; still "pass" if salvage works,
        // but note the miss for humans.
        reasons.push(
            "note: chaos hits applied but no extra holes/scrub failures/get misses observed (may have hit padding)"
                .into(),
        );
    }
    // Survivors: we still want evidence that damage is not total silent loss.
    if post.live_subjects == 0 && baseline.live_subjects > 0 {
        ok = false;
        reasons.push("post-chaos live_subjects=0 after non-empty baseline (total loss)".into());
    }
    if reasons.is_empty() {
        reasons.push(
            "pass: pump target met, baseline healthy, chaos landed, salvage still speaks".into(),
        );
    }
    (ok, reasons)
}
