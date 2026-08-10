//! Acknowledgement / finalisation split (NEXT_MEASUREMENT.md).
//!
//! Separates hot-path acknowledged-write throughput from drain, seal, close,
//! reopen, and verification. Measurement only — no storage or AWO changes.

use crate::size::{dir_size_bytes, ensure_free_space, format_bytes};
use crate::write_mimic::{PEER_DATA_BYTES_PER_OP, PEER_OPS};
use residiuum_format::body_hash;
use residiuum_store::{
    ContentHashState, DiagnosticIoSink, DurabilityMode, EnrichmentStageTotals, RotationStageTotals,
    SealStageBreakdown, Store,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DISCLOSURE: &str = "Diagnostic only — ack/finalisation split \
    (doc/todo/performance-qualification/NEXT_MEASUREMENT.md; \
    doc/reference/operations/BENCHMARK_DISCLOSURE.md). Not a published SLO.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeasureCell {
    RealFull,
    RealSkipIndex,
    Discard,
    RawMimic,
}

impl MeasureCell {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "real" | "real-full" | "full" => Ok(Self::RealFull),
            "skip-index" | "real-skip-index" | "indexing-disabled" => Ok(Self::RealSkipIndex),
            "discard" => Ok(Self::Discard),
            "raw-mimic" | "mimic" | "write-mimic" => Ok(Self::RawMimic),
            other => Err(format!(
                "unknown cell `{other}` (real-full|skip-index|discard|raw-mimic)"
            )),
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::RealFull => "Real full",
            Self::RealSkipIndex => "Real, indexing disabled",
            Self::Discard => "Discard",
            Self::RawMimic => "Raw mimic",
        }
    }

    pub fn slug(self) -> &'static str {
        match self {
            Self::RealFull => "real-full",
            Self::RealSkipIndex => "skip-index",
            Self::Discard => "discard",
            Self::RawMimic => "raw-mimic",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AckFinalizeConfig {
    pub work: PathBuf,
    pub cell: MeasureCell,
    pub target_bytes: u64,
    pub payload_size: usize,
    pub concurrency: usize,
    pub seed: u64,
    pub seal_threshold: u64,
    pub min_free_bytes: u64,
    pub json_out: bool,
    /// When false, skip Hydra/Chimera enqueue during the ack window (control).
    pub enrichment_enabled: bool,
}

/// Derived-enrichment telemetry for product (enrichment-on) campaigns.
#[derive(Debug, Clone, Default)]
pub struct EnrichmentTelemetry {
    pub enabled: bool,
    pub peak_backlog: usize,
    pub final_backlog_at_ack: usize,
    pub backlog_after_seal: usize,
    pub backlog_after_enrich_drain: usize,
    /// Ordinary least-squares slope of backlog vs ack elapsed (jobs/sec).
    pub backlog_slope_per_sec: f64,
    pub backlog_samples: usize,
    pub enrichment_drain_elapsed: Duration,
    pub completed_enrichment_jobs: u64,
    /// Jobs completed during post-seal enrich drain / drain wall.
    pub completed_enrichment_ops_per_sec: f64,
    /// Keys / (ack + auth seal breakdown + enrich drain). No reopen/verify.
    pub complete_lifecycle_ops_per_sec: f64,
    pub segments_hash_known: usize,
    pub segments_hash_pending: usize,
    pub index_query_verify_ok: bool,
    pub stage_totals: EnrichmentStageTotals,
}

#[derive(Debug, Clone)]
pub struct AckFinalizeResult {
    pub cell: MeasureCell,
    pub keys: u64,
    pub payload_size: usize,
    pub concurrency: usize,
    pub seal_threshold: u64,
    pub ack_elapsed: Duration,
    pub drain_elapsed: Duration,
    pub seal_elapsed: Duration,
    pub seal_breakdown: SealStageBreakdown,
    pub close_elapsed: Duration,
    pub reopen_elapsed: Duration,
    pub verify_elapsed: Duration,
    /// Ack + post-ack campaign wall (includes reopen/verify). Not DB lifecycle TPS.
    pub campaign_elapsed: Duration,
    pub acknowledged_write_ops_per_sec: f64,
    pub campaign_ops_per_sec: f64,
    pub reopen_exact: bool,
    /// How reopen was verified (`coverage_scan` | `point_get_legacy` | `length_endpoints` | `n/a`).
    pub reopen_verify_mode: &'static str,
    pub pending_seal_inflight_at_last_ack: usize,
    pub pending_seal_paths_at_last_ack: usize,
    pub sealed_segments_at_last_ack: usize,
    pub enrichment_backlog_at_last_ack: usize,
    pub enrichment: EnrichmentTelemetry,
    /// Mid-run auto-rotation stage totals through last ack.
    pub rotation_stages: RotationStageTotals,
    pub logical_bytes: u64,
    pub on_disk_bytes: u64,
    pub timestamps: Timestamps,
}

#[derive(Debug, Clone)]
pub struct Timestamps {
    pub workload_start_unix_ns: u128,
    pub last_successful_ack_unix_ns: u128,
    pub drain_complete_unix_ns: u128,
    pub seal_complete_unix_ns: u128,
    pub close_complete_unix_ns: u128,
    pub reopen_complete_unix_ns: u128,
    pub verification_complete_unix_ns: u128,
}

fn unix_ns_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn fill_payload(size: usize, seed: u64) -> Vec<u8> {
    let mut out = vec![0u8; size];
    let mut state = seed ^ 0x9E37_79B9_7F4A_7C15;
    for b in &mut out {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        *b = (state >> 33) as u8;
    }
    out
}

/// Run one matrix cell. Fail-closed: seal errors and reopen mismatches abort.
pub fn run_ack_finalize(cfg: &AckFinalizeConfig) -> Result<AckFinalizeResult, String> {
    if cfg.payload_size == 0 {
        return Err("--payload-size must be > 0".into());
    }
    if cfg.target_bytes == 0 {
        return Err("--target-bytes must be > 0".into());
    }
    fs::create_dir_all(&cfg.work).map_err(|e| format!("create work: {e}"))?;
    let _ = ensure_free_space(&cfg.work, cfg.min_free_bytes)?;

    let result = match cfg.cell {
        MeasureCell::RawMimic => run_raw_mimic(cfg)?,
        _ => run_store_cell(cfg)?,
    };

    if cfg.json_out {
        println!(
            "{}",
            serde_json::to_string_pretty(&result_json(&result))
                .map_err(|e| format!("serialize cell json: {e}"))?
        );
    } else {
        eprintln!(
            "ack-finalize {}: keys={} ack_tps={:.0} seal_ms={:.1} campaign_tps={:.0} pending_seals={} reopen_exact={}",
            result.cell.slug(),
            result.keys,
            result.acknowledged_write_ops_per_sec,
            result.seal_elapsed.as_secs_f64() * 1000.0,
            result.campaign_ops_per_sec,
            result.pending_seal_inflight_at_last_ack,
            result.reopen_exact
        );
    }
    Ok(result)
}

/// Run the four-cell matrix; write evidence JSON + markdown table under `evidence_dir`.
pub fn run_ack_finalize_matrix(
    work_root: &Path,
    evidence_dir: &Path,
    target_bytes: u64,
    payload_size: usize,
    concurrency: usize,
    seed: u64,
    seal_threshold: u64,
    min_free_bytes: u64,
) -> Result<(), String> {
    fs::create_dir_all(evidence_dir).map_err(|e| format!("create evidence dir: {e}"))?;
    fs::create_dir_all(work_root).map_err(|e| format!("create work root: {e}"))?;

    let cells = [
        MeasureCell::RealFull,
        MeasureCell::RealSkipIndex,
        MeasureCell::Discard,
        MeasureCell::RawMimic,
    ];
    let mut rows: Vec<AckFinalizeResult> = Vec::with_capacity(4);
    let mut errors: Vec<String> = Vec::new();

    for cell in cells {
        let work = work_root.join(cell.slug());
        if work.exists() {
            fs::remove_dir_all(&work).map_err(|e| format!("cleanup {}: {e}", work.display()))?;
        }
        let cfg = AckFinalizeConfig {
            work: work.clone(),
            cell,
            target_bytes,
            payload_size,
            concurrency,
            seed,
            seal_threshold,
            min_free_bytes,
            json_out: false,
            enrichment_enabled: true,
        };
        match run_ack_finalize(&cfg) {
            Ok(r) => {
                let cell_path = evidence_dir.join(format!("{}.json", cell.slug()));
                fs::write(
                    &cell_path,
                    serde_json::to_string_pretty(&result_json(&r))
                        .map_err(|e| format!("serialize {}: {e}", cell.slug()))?,
                )
                .map_err(|e| format!("write {}: {e}", cell_path.display()))?;
                rows.push(r);
            }
            Err(e) => {
                errors.push(format!("{}: {e}", cell.label()));
                eprintln!("ack-finalize cell {} FAILED: {e}", cell.slug());
            }
        }
        // Disk hygiene — always remove work dir after evidence is captured.
        if work.exists() {
            let _ = fs::remove_dir_all(&work);
        }
    }

    let table = render_markdown_table(&rows);
    let summary = json!({
        "kind": "ack_finalize_matrix",
        "disclosure": DISCLOSURE,
        "recipe": {
            "fs": "APFS",
            "payload_size": payload_size,
            "logical_data_bytes": target_bytes,
            "concurrency": concurrency,
            "durability": "Buffered",
            "awo": "Disabled",
            "seed": seed,
            "seal_threshold": seal_threshold,
        },
        "cells": rows.iter().map(result_json).collect::<Vec<_>>(),
        "errors": errors,
        "markdown_table": table,
    });
    let summary_path = evidence_dir.join("matrix.json");
    fs::write(
        &summary_path,
        serde_json::to_string_pretty(&summary).map_err(|e| format!("serialize matrix: {e}"))?,
    )
    .map_err(|e| format!("write {}: {e}", summary_path.display()))?;
    let table_path = evidence_dir.join("EVIDENCE_TABLE.md");
    fs::write(&table_path, format!("{table}\n"))
        .map_err(|e| format!("write {}: {e}", table_path.display()))?;

    println!("{table}");
    eprintln!(
        "ack-finalize matrix: wrote {} and {}",
        summary_path.display(),
        table_path.display()
    );

    if !errors.is_empty() {
        return Err(format!(
            "matrix incomplete ({} cell failure(s)): {}",
            errors.len(),
            errors.join("; ")
        ));
    }
    if rows.len() != 4 {
        return Err(format!("expected 4 cells, got {}", rows.len()));
    }
    Ok(())
}

fn run_store_cell(cfg: &AckFinalizeConfig) -> Result<AckFinalizeResult, String> {
    let store_path = cfg.work.join("store");
    if store_path.exists() {
        fs::remove_dir_all(&store_path).map_err(|e| format!("remove prior store: {e}"))?;
    }
    let mut store =
        Store::create_with_shards(&store_path, 1).map_err(|e| format!("create store: {e}"))?;
    let seal_thr = if cfg.seal_threshold == 0 {
        64 * 1024 * 1024
    } else {
        cfg.seal_threshold
    };
    store.set_seal_threshold(seal_thr);
    store.set_enrichment_enabled(cfg.enrichment_enabled);

    let sink = match cfg.cell {
        MeasureCell::Discard => DiagnosticIoSink::Discard,
        MeasureCell::RealFull | MeasureCell::RealSkipIndex => DiagnosticIoSink::Real,
        MeasureCell::RawMimic => unreachable!("raw mimic handled separately"),
    };
    store
        .set_diagnostic_io_sink(sink)
        .map_err(|e| format!("set_diagnostic_io_sink: {e}"))?;
    if cfg.cell == MeasureCell::RealSkipIndex {
        store.set_diagnostic_skip_index(true);
    }

    let target_keys = cfg.target_bytes.div_ceil(cfg.payload_size as u64).max(1);
    let payload = Arc::new(fill_payload(cfg.payload_size, cfg.seed));
    let expected_hash = body_hash(payload.as_slice());
    let store_arc = Arc::new(Mutex::new(store));
    let next = Arc::new(AtomicU64::new(0));
    let err: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let workers = cfg.concurrency.max(1);

    let workload_start_unix_ns = unix_ns_now();
    let t_workload = Instant::now();
    let sample_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let backlog_samples: Arc<Mutex<Vec<(f64, usize)>>> = Arc::new(Mutex::new(Vec::new()));

    thread::scope(|scope| {
        if cfg.enrichment_enabled {
            let store_arc = Arc::clone(&store_arc);
            let sample_stop = Arc::clone(&sample_stop);
            let backlog_samples = Arc::clone(&backlog_samples);
            let t0 = t_workload;
            scope.spawn(move || {
                while !sample_stop.load(Ordering::Relaxed) {
                    if let Ok(g) = store_arc.try_lock() {
                        let bl = g.enrichment_backlog();
                        let t = t0.elapsed().as_secs_f64();
                        if let Ok(mut samples) = backlog_samples.lock() {
                            samples.push((t, bl));
                        }
                    }
                    thread::sleep(Duration::from_millis(25));
                }
            });
        }
        let mut joins = Vec::with_capacity(workers);
        for _ in 0..workers {
            let store_arc = Arc::clone(&store_arc);
            let next = Arc::clone(&next);
            let err = Arc::clone(&err);
            let payload = Arc::clone(&payload);
            joins.push(scope.spawn(move || loop {
                if err.lock().ok().and_then(|g| g.clone()).is_some() {
                    break;
                }
                let seq = next.fetch_add(1, Ordering::Relaxed);
                if seq >= target_keys {
                    break;
                }
                let key = format!("peer/{:020}", seq);
                let put_err = {
                    let mut g = match store_arc.lock() {
                        Ok(g) => g,
                        Err(_) => {
                            let _ = err.lock().map(|mut e| {
                                *e = Some("store lock poisoned during put".into());
                            });
                            break;
                        }
                    };
                    g.put_many(&[(&key[..], payload.as_slice())], DurabilityMode::Buffered)
                        .map(|_| ())
                        .map_err(|e| format!("put_many: {e}"))
                };
                if let Err(e) = put_err {
                    let _ = err.lock().map(|mut slot| {
                        if slot.is_none() {
                            *slot = Some(e);
                        }
                    });
                    break;
                }
            }));
        }
        for j in joins {
            let _ = j.join();
        }
        // Stop sampler before scope joins it (avoids deadlock).
        sample_stop.store(true, Ordering::Relaxed);
    });

    if let Some(e) = err.lock().ok().and_then(|g| g.clone()) {
        return Err(e);
    }
    let keys_written = target_keys.min(next.load(Ordering::Relaxed));
    if keys_written != target_keys {
        return Err(format!(
            "ack incomplete: wrote {keys_written} of {target_keys} keys"
        ));
    }
    let last_successful_ack_unix_ns = unix_ns_now();
    let ack_elapsed = t_workload.elapsed();

    let mut store = Arc::try_unwrap(store_arc)
        .map_err(|_| "store Arc still shared after ack phase".to_string())?
        .into_inner()
        .map_err(|_| "store mutex poisoned extract".to_string())?;

    let pending_seal_inflight_at_last_ack = store.pending_seal_inflight();
    let enrichment_backlog_at_last_ack = store.enrichment_backlog();
    let rotation_stages = store.rotation_stage_totals();
    let (pending_seal_paths_at_last_ack, sealed_segments_at_last_ack) = match store.lifecycle_diag()
    {
        Ok(d) => (d.pending_seals, d.sealed_segments),
        Err(e) => {
            return Err(format!(
                "lifecycle_diag at last ack failed (fail-closed): {e}"
            ));
        }
    };

    let samples = backlog_samples
        .lock()
        .map(|g| g.clone())
        .unwrap_or_default();
    let peak_backlog = samples
        .iter()
        .map(|(_, b)| *b)
        .chain(std::iter::once(enrichment_backlog_at_last_ack))
        .max()
        .unwrap_or(0);
    let backlog_slope_per_sec = backlog_slope_ols(&samples);

    // Explicit end-of-run seal: drain + final active (breakdown times both).
    let t_seal = Instant::now();
    let seal_breakdown = store
        .seal_active_with_breakdown()
        .map_err(|e| format!("seal_active failed (fail-closed): {e}"))?;
    let seal_complete_unix_ns = unix_ns_now();
    let seal_elapsed = t_seal.elapsed();
    let drain_elapsed = Duration::from_nanos(seal_breakdown.drain_lifecycle_ns);
    let drain_complete_unix_ns =
        last_successful_ack_unix_ns.saturating_add(u128::from(seal_breakdown.drain_lifecycle_ns));

    let backlog_after_seal = store.enrichment_backlog();
    let t_enrich_drain = Instant::now();
    if cfg.enrichment_enabled {
        // Full-product closeout: wait for derived enrichment to idle + digests Known.
        store
            .drain_enrichment(Duration::from_secs(30 * 60))
            .map_err(|e| format!("drain_enrichment failed (fail-closed): {e}"))?;
        let digest_deadline = Instant::now() + Duration::from_secs(30 * 60);
        loop {
            let (known, pending) = count_content_hash_states(&store);
            if pending == 0 && store.enrichment_backlog() == 0 {
                let _ = known;
                break;
            }
            if Instant::now() >= digest_deadline {
                return Err(format!(
                    "enrichment digests incomplete (fail-closed): known={known} pending={pending} backlog={}",
                    store.enrichment_backlog()
                ));
            }
            let _ = store.drain_enrichment(Duration::from_millis(100));
            thread::sleep(Duration::from_millis(10));
        }
    }
    let enrichment_drain_elapsed = t_enrich_drain.elapsed();
    let backlog_after_enrich_drain = store.enrichment_backlog();
    if cfg.enrichment_enabled && backlog_after_enrich_drain > 0 {
        return Err(format!(
            "enrichment drain incomplete (fail-closed): backlog={} after {:.1}s",
            backlog_after_enrich_drain,
            enrichment_drain_elapsed.as_secs_f64()
        ));
    }

    let (segments_hash_known, segments_hash_pending) = count_content_hash_states(&store);
    let stage_totals = store.enrichment_stage_totals();
    // Jobs that finished during post-seal drain (backlog delta) plus those already
    // known before drain are both "completed"; report Known count as total done.
    let completed_enrichment_jobs = segments_hash_known as u64;
    let enrich_wall = ack_elapsed + enrichment_drain_elapsed;
    let completed_enrichment_ops_per_sec = if cfg.enrichment_enabled {
        completed_enrichment_jobs as f64 / enrich_wall.as_secs_f64().max(1e-12)
    } else {
        0.0
    };
    let lifecycle_wall =
        ack_elapsed + Duration::from_nanos(seal_breakdown.total_ns()) + enrichment_drain_elapsed;
    let complete_lifecycle_ops_per_sec =
        keys_written as f64 / lifecycle_wall.as_secs_f64().max(1e-12);

    let t_close = Instant::now();
    drop(store);
    let close_complete_unix_ns = unix_ns_now();
    let close_elapsed = t_close.elapsed();

    let t_reopen = Instant::now();
    let store =
        Store::open(&store_path).map_err(|e| format!("reopen failed (fail-closed): {e}"))?;
    let reopen_complete_unix_ns = unix_ns_now();
    let reopen_elapsed = t_reopen.elapsed();

    let t_verify = Instant::now();
    let (reopen_exact, reopen_verify_mode) = if cfg.cell == MeasureCell::Discard {
        // Discard puts never hit media — coverage scan cannot reconstruct ledger.
        match verify_store_coverage_scan(&store, keys_written, &expected_hash) {
            Ok((exact, mode)) => (exact, mode),
            Err(_) => (false, "coverage_scan"),
        }
    } else {
        verify_store_coverage_scan(&store, keys_written, &expected_hash)?
    };
    let index_query_verify_ok = if cfg.cell == MeasureCell::Discard {
        false
    } else {
        verify_index_query_sample(&store, keys_written, &expected_hash)?
    };
    let verification_complete_unix_ns = unix_ns_now();
    let verify_elapsed = t_verify.elapsed();
    drop(store);

    // Discard never writes put bytes to media; seal may succeed from RAM but
    // ordinary reopen cannot reconstruct the ledger. Report reopen_exact=false
    // honestly — do not invent durability. Real cells remain fail-closed.
    if !reopen_exact && cfg.cell != MeasureCell::Discard {
        return Err(format!(
            "reopen ledger mismatch (fail-closed): coverage_scan failed for {} keys",
            keys_written
        ));
    }
    if cfg.cell != MeasureCell::Discard && !index_query_verify_ok {
        return Err("index/query sample verification failed (fail-closed)".into());
    }

    // seal_breakdown.total_ns already includes drain + final seal stages.
    let campaign_elapsed = ack_elapsed
        + Duration::from_nanos(seal_breakdown.total_ns())
        + enrichment_drain_elapsed
        + close_elapsed
        + reopen_elapsed
        + verify_elapsed;
    let ack_secs = ack_elapsed.as_secs_f64().max(1e-12);
    let campaign_secs = campaign_elapsed.as_secs_f64().max(1e-12);
    let on_disk = dir_size_bytes(&store_path).unwrap_or(0);

    Ok(AckFinalizeResult {
        cell: cfg.cell,
        keys: keys_written,
        payload_size: cfg.payload_size,
        concurrency: workers,
        seal_threshold: seal_thr,
        ack_elapsed,
        drain_elapsed,
        seal_elapsed,
        seal_breakdown,
        close_elapsed,
        reopen_elapsed,
        verify_elapsed,
        campaign_elapsed,
        acknowledged_write_ops_per_sec: keys_written as f64 / ack_secs,
        campaign_ops_per_sec: keys_written as f64 / campaign_secs,
        reopen_exact,
        reopen_verify_mode,
        pending_seal_inflight_at_last_ack,
        pending_seal_paths_at_last_ack,
        sealed_segments_at_last_ack,
        enrichment_backlog_at_last_ack,
        enrichment: EnrichmentTelemetry {
            enabled: cfg.enrichment_enabled,
            peak_backlog,
            final_backlog_at_ack: enrichment_backlog_at_last_ack,
            backlog_after_seal,
            backlog_after_enrich_drain,
            backlog_slope_per_sec,
            backlog_samples: samples.len(),
            enrichment_drain_elapsed,
            completed_enrichment_jobs,
            completed_enrichment_ops_per_sec,
            complete_lifecycle_ops_per_sec,
            segments_hash_known,
            segments_hash_pending,
            index_query_verify_ok,
            stage_totals,
        },
        rotation_stages,
        logical_bytes: keys_written.saturating_mul(cfg.payload_size as u64),
        on_disk_bytes: on_disk,
        timestamps: Timestamps {
            workload_start_unix_ns,
            last_successful_ack_unix_ns,
            drain_complete_unix_ns,
            seal_complete_unix_ns,
            close_complete_unix_ns,
            reopen_complete_unix_ns,
            verification_complete_unix_ns,
        },
    })
}

fn backlog_slope_ols(samples: &[(f64, usize)]) -> f64 {
    if samples.len() < 2 {
        return 0.0;
    }
    let n = samples.len() as f64;
    let mut sum_t = 0.0;
    let mut sum_b = 0.0;
    let mut sum_tt = 0.0;
    let mut sum_tb = 0.0;
    for &(t, b) in samples {
        let bf = b as f64;
        sum_t += t;
        sum_b += bf;
        sum_tt += t * t;
        sum_tb += t * bf;
    }
    let denom = n * sum_tt - sum_t * sum_t;
    if denom.abs() < 1e-12 {
        return 0.0;
    }
    (n * sum_tb - sum_t * sum_b) / denom
}

fn count_content_hash_states(store: &Store) -> (usize, usize) {
    let mut known = 0usize;
    let mut pending = 0usize;
    for id in store.list_segment_ids() {
        match store.sealed_content_hash_state(&id) {
            Some(ContentHashState::Known(_)) => known = known.saturating_add(1),
            Some(ContentHashState::Pending) | None => pending = pending.saturating_add(1),
        }
    }
    (known, pending)
}

fn verify_index_query_sample(
    store: &Store,
    keys_written: u64,
    expected_hash: &[u8; 32],
) -> Result<bool, String> {
    if keys_written == 0 {
        return Ok(true);
    }
    // Point-get endpoints + mid-key: dual-index / primary path must serve bodies.
    let probes = [0u64, keys_written / 2, keys_written.saturating_sub(1)];
    for seq in probes {
        let key = format!("peer/{:020}", seq);
        let got = store
            .get(&key)
            .map_err(|e| format!("index/query get({key}): {e}"))?;
        let Some(body) = got else {
            return Ok(false);
        };
        if body_hash(&body) != *expected_hash {
            return Ok(false);
        }
    }
    Ok(true)
}

fn verify_store_coverage_scan(
    store: &Store,
    keys_written: u64,
    expected_hash: &[u8; 32],
) -> Result<(bool, &'static str), String> {
    let scan = store
        .scan_live_logical()
        .map_err(|e| format!("scan_live_logical failed (fail-closed): {e}"))?;
    if !scan.complete {
        return Err(format!(
            "coverage-aware scan incomplete (fail-closed): incomplete={} tier_coverage_incomplete={}",
            scan.incomplete.len(),
            scan.tier_coverage_incomplete
        ));
    }
    let mut by_key: HashMap<Vec<u8>, Vec<u8>> = HashMap::with_capacity(scan.entries.len());
    for (k, v) in scan.entries {
        by_key.insert(k, v);
    }
    if by_key.len() as u64 != keys_written {
        return Ok((false, "coverage_scan"));
    }
    let mut mismatches = 0u64;
    let mut missing = 0u64;
    for seq in 0..keys_written {
        let key = format!("peer/{:020}", seq);
        match by_key.get(key.as_bytes()) {
            Some(body) => {
                if body_hash(body) != *expected_hash {
                    mismatches = mismatches.saturating_add(1);
                }
            }
            None => missing = missing.saturating_add(1),
        }
    }
    Ok((mismatches == 0 && missing == 0, "coverage_scan"))
}

fn run_raw_mimic(cfg: &AckFinalizeConfig) -> Result<AckFinalizeResult, String> {
    let ops = cfg
        .target_bytes
        .div_ceil(cfg.payload_size as u64)
        .max(1)
        .min(PEER_OPS.saturating_mul(4));
    // Mimic uses peer-calibrated encoded size, not logical payload size.
    let data_bytes_per_op = PEER_DATA_BYTES_PER_OP;
    let data_path = cfg.work.join("data.seg");
    let _ = fs::remove_file(&data_path);
    fs::create_dir_all(&cfg.work).map_err(|e| format!("create mimic work: {e}"))?;

    let data_buf = fill_payload(data_bytes_per_op, cfg.seed);
    let expected_hash = body_hash(&data_buf);

    let mut data = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&data_path)
        .map_err(|e| format!("open data: {e}"))?;

    let workload_start_unix_ns = unix_ns_now();
    let t_workload = Instant::now();
    let mut off = 0u64;
    for _ in 0..ops {
        data.seek(SeekFrom::Start(off))
            .map_err(|e| format!("seek: {e}"))?;
        data.write_all(&data_buf)
            .map_err(|e| format!("write_all: {e}"))?;
        off = off.saturating_add(data_bytes_per_op as u64);
    }
    let last_successful_ack_unix_ns = unix_ns_now();
    let ack_elapsed = t_workload.elapsed();

    let t_drain = Instant::now();
    data.flush().map_err(|e| format!("flush (drain): {e}"))?;
    let drain_complete_unix_ns = unix_ns_now();
    let drain_elapsed = t_drain.elapsed();

    // No store seal — fsync stands in as the durability boundary for mimic.
    let t_seal = Instant::now();
    data.sync_all()
        .map_err(|e| format!("sync_all (seal substitute) failed (fail-closed): {e}"))?;
    let seal_complete_unix_ns = unix_ns_now();
    let seal_elapsed = t_seal.elapsed();

    let t_close = Instant::now();
    drop(data);
    let close_complete_unix_ns = unix_ns_now();
    let close_elapsed = t_close.elapsed();

    let t_reopen = Instant::now();
    let mut f = File::open(&data_path).map_err(|e| format!("reopen mimic failed: {e}"))?;
    let reopen_complete_unix_ns = unix_ns_now();
    let reopen_elapsed = t_reopen.elapsed();

    let t_verify = Instant::now();
    let meta_len = f
        .metadata()
        .map_err(|e| format!("mimic metadata: {e}"))?
        .len();
    let expect_len = ops.saturating_mul(data_bytes_per_op as u64);
    if meta_len != expect_len {
        return Err(format!(
            "mimic reopen length mismatch (fail-closed): got {meta_len} want {expect_len}"
        ));
    }
    // Spot-check first and last op bodies (full scan of 256 MiB encoded is
    // unnecessary for length-exact grow mimic; hash endpoints for ledger shape).
    let mut buf = vec![0u8; data_bytes_per_op];
    f.seek(SeekFrom::Start(0))
        .map_err(|e| format!("seek verify start: {e}"))?;
    use std::io::Read;
    f.read_exact(&mut buf)
        .map_err(|e| format!("read first op: {e}"))?;
    if body_hash(&buf) != expected_hash {
        return Err("mimic first-op hash mismatch (fail-closed)".into());
    }
    if ops > 1 {
        f.seek(SeekFrom::Start(
            (ops - 1).saturating_mul(data_bytes_per_op as u64),
        ))
        .map_err(|e| format!("seek verify last: {e}"))?;
        f.read_exact(&mut buf)
            .map_err(|e| format!("read last op: {e}"))?;
        if body_hash(&buf) != expected_hash {
            return Err("mimic last-op hash mismatch (fail-closed)".into());
        }
    }
    let verification_complete_unix_ns = unix_ns_now();
    let verify_elapsed = t_verify.elapsed();
    drop(f);

    let lifecycle_elapsed =
        drain_elapsed + seal_elapsed + close_elapsed + reopen_elapsed + verify_elapsed;
    let campaign_elapsed = ack_elapsed + lifecycle_elapsed;
    let ack_secs = ack_elapsed.as_secs_f64().max(1e-12);
    let campaign_secs = campaign_elapsed.as_secs_f64().max(1e-12);
    let seal_thr = if cfg.seal_threshold == 0 {
        64 * 1024 * 1024
    } else {
        cfg.seal_threshold
    };

    Ok(AckFinalizeResult {
        cell: cfg.cell,
        keys: ops,
        payload_size: cfg.payload_size,
        concurrency: 1,
        seal_threshold: seal_thr,
        ack_elapsed,
        drain_elapsed,
        seal_elapsed,
        seal_breakdown: SealStageBreakdown::default(),
        close_elapsed,
        reopen_elapsed,
        verify_elapsed,
        campaign_elapsed,
        acknowledged_write_ops_per_sec: ops as f64 / ack_secs,
        campaign_ops_per_sec: ops as f64 / campaign_secs,
        // Honesty: length + first/last only — not a full exact reopen ledger.
        reopen_exact: false,
        reopen_verify_mode: "length_endpoints",
        pending_seal_inflight_at_last_ack: 0,
        pending_seal_paths_at_last_ack: 0,
        sealed_segments_at_last_ack: 0,
        enrichment_backlog_at_last_ack: 0,
        enrichment: EnrichmentTelemetry {
            complete_lifecycle_ops_per_sec: ops as f64
                / (ack_elapsed + drain_elapsed + seal_elapsed)
                    .as_secs_f64()
                    .max(1e-12),
            ..EnrichmentTelemetry::default()
        },
        rotation_stages: RotationStageTotals::default(),
        logical_bytes: ops.saturating_mul(cfg.payload_size as u64),
        on_disk_bytes: expect_len,
        timestamps: Timestamps {
            workload_start_unix_ns,
            last_successful_ack_unix_ns,
            drain_complete_unix_ns,
            seal_complete_unix_ns,
            close_complete_unix_ns,
            reopen_complete_unix_ns,
            verification_complete_unix_ns,
        },
    })
}

fn pct(part_ns: u64, whole: Duration) -> f64 {
    let whole_ns = whole.as_nanos().max(1) as f64;
    (part_ns as f64) * 100.0 / whole_ns
}

fn result_json(r: &AckFinalizeResult) -> Value {
    let rot = &r.rotation_stages;
    let rotation_stages = json!({
        "rotations": rot.rotations,
        "flush_ns": rot.flush_ns,
        "rename_pending_ns": rot.rename_pending_ns,
        "start_active_ns": rot.start_active_ns,
        "backpressure_wait_ns": rot.backpressure_wait_ns,
        "auth_publish_ns": rot.auth_publish_ns,
        "catalog_apply_ns": rot.catalog_apply_ns,
        "total_ns": rot.total_ns(),
        "pct_of_ack_wall": {
            "flush": pct(rot.flush_ns, r.ack_elapsed),
            "rename_pending": pct(rot.rename_pending_ns, r.ack_elapsed),
            "start_active": pct(rot.start_active_ns, r.ack_elapsed),
            "backpressure_wait": pct(rot.backpressure_wait_ns, r.ack_elapsed),
            "auth_publish": pct(rot.auth_publish_ns, r.ack_elapsed),
            "catalog_apply": pct(rot.catalog_apply_ns, r.ack_elapsed),
            "all_rotation_stages": pct(rot.total_ns(), r.ack_elapsed),
        },
    });
    let st = &r.enrichment.stage_totals;
    let n = st.samples.max(1) as f64;
    let mean = |sum: u64| sum as f64 / n;
    let cap = |mean_ns: f64| {
        if mean_ns <= 0.0 {
            0.0
        } else {
            1_000_000_000.0 / mean_ns
        }
    };
    let mean_wall = mean(st.wall_ns);
    let mean_service = mean(st.wall_ns.saturating_sub(st.gap_wait_ns));
    let mean_read_decode = mean(st.read_ns.saturating_add(st.decode_ns));
    let mean_blake3 = mean(st.blake3_ns);
    let mean_hydra = mean(st.hydra_construct_ns.saturating_add(st.hydra_persist_ns));
    let mean_chimera = mean(
        st.chimera_construct_ns
            .saturating_add(st.chimera_persist_ns),
    );
    let mean_catalog = mean(st.catalog_ns);
    let stage_breakdown = json!({
        "samples": st.samples,
        "per_segment_mean_ns": {
            "gap_wait": mean(st.gap_wait_ns),
            "read": mean(st.read_ns),
            "decode": mean(st.decode_ns),
            "read_and_decode": mean_read_decode,
            "blake3": mean_blake3,
            "hydra_construct": mean(st.hydra_construct_ns),
            "hydra_persist": mean(st.hydra_persist_ns),
            "hydra_total": mean_hydra,
            "chimera_construct": mean(st.chimera_construct_ns),
            "chimera_persist": mean(st.chimera_persist_ns),
            "chimera_total": mean_chimera,
            "catalog_apply": mean_catalog,
            "wall_total": mean_wall,
            "service_excluding_gap": mean_service,
            "cpu": mean(st.cpu_ns),
        },
        "per_segment_mean_bytes": {
            "read": mean(st.bytes_read),
            "written": mean(st.bytes_written),
        },
        "sums": {
            "gap_wait_ns": st.gap_wait_ns,
            "read_ns": st.read_ns,
            "decode_ns": st.decode_ns,
            "blake3_ns": st.blake3_ns,
            "hydra_construct_ns": st.hydra_construct_ns,
            "hydra_persist_ns": st.hydra_persist_ns,
            "chimera_construct_ns": st.chimera_construct_ns,
            "chimera_persist_ns": st.chimera_persist_ns,
            "catalog_ns": st.catalog_ns,
            "wall_ns": st.wall_ns,
            "cpu_ns": st.cpu_ns,
            "bytes_read": st.bytes_read,
            "bytes_written": st.bytes_written,
        },
        "service_capacity_seg_per_sec": {
            "wall_total": cap(mean_wall),
            "service_excluding_gap": cap(mean_service),
            "read_and_decode": cap(mean_read_decode),
            "blake3": cap(mean_blake3),
            "hydra": cap(mean_hydra),
            "chimera": cap(mean_chimera),
            "catalog_apply": cap(mean_catalog),
        },
        "floors": {
            "keep_pace_ack_seg_per_sec": 5.8,
            "match_auth_engine_seg_per_sec": 7.0,
            "budget_ns_at_5_8": 1_000_000_000.0 / 5.8,
            "budget_ns_at_7_0": 1_000_000_000.0 / 7.0,
        },
        "cpu_vs_wall_ratio": if mean_wall > 0.0 {
            mean(st.cpu_ns) / mean_wall
        } else {
            0.0
        },
    });
    let enrichment = json!({
        "enabled": r.enrichment.enabled,
        "peak_backlog": r.enrichment.peak_backlog,
        "final_backlog_at_ack": r.enrichment.final_backlog_at_ack,
        "backlog_after_seal": r.enrichment.backlog_after_seal,
        "backlog_after_enrich_drain": r.enrichment.backlog_after_enrich_drain,
        "backlog_slope_per_sec": r.enrichment.backlog_slope_per_sec,
        "backlog_samples": r.enrichment.backlog_samples,
        "enrichment_drain_elapsed_ns": r.enrichment.enrichment_drain_elapsed.as_nanos() as u64,
        "completed_enrichment_jobs": r.enrichment.completed_enrichment_jobs,
        "completed_enrichment_ops_per_sec": r.enrichment.completed_enrichment_ops_per_sec,
        "complete_lifecycle_ops_per_sec": r.enrichment.complete_lifecycle_ops_per_sec,
        "segments_hash_known": r.enrichment.segments_hash_known,
        "segments_hash_pending": r.enrichment.segments_hash_pending,
        "index_query_verify_ok": r.enrichment.index_query_verify_ok,
        "stage_breakdown": stage_breakdown,
    });
    let seal_breakdown = json!({
        "drain_lifecycle_ns": r.seal_breakdown.drain_lifecycle_ns,
        "final_active_seal_ns": r.seal_breakdown.final_active_seal_ns,
        "catalog_publication_ns": r.seal_breakdown.catalog_publication_ns,
        "hydra_ns": r.seal_breakdown.hydra_ns,
        "chimera_ns": r.seal_breakdown.chimera_ns,
        "reopen_active_ns": r.seal_breakdown.reopen_active_ns,
        "total_ns": r.seal_breakdown.total_ns(),
    });
    let timestamps = json!({
        "workload_start": r.timestamps.workload_start_unix_ns as u64,
        "last_successful_ack": r.timestamps.last_successful_ack_unix_ns as u64,
        "drain_complete": r.timestamps.drain_complete_unix_ns as u64,
        "seal_complete": r.timestamps.seal_complete_unix_ns as u64,
        "close_complete": r.timestamps.close_complete_unix_ns as u64,
        "reopen_complete": r.timestamps.reopen_complete_unix_ns as u64,
        "verification_complete": r.timestamps.verification_complete_unix_ns as u64,
    });
    json!({
        "kind": "ack_finalize_cell",
        "disclosure": DISCLOSURE,
        "cell": r.cell.slug(),
        "cell_label": r.cell.label(),
        "keys": r.keys,
        "payload_size": r.payload_size,
        "concurrency": r.concurrency,
        "seal_threshold": r.seal_threshold,
        "logical_bytes": r.logical_bytes,
        "on_disk_bytes": r.on_disk_bytes,
        "on_disk_human": format_bytes(r.on_disk_bytes),
        "acknowledged_write_ops_per_sec": r.acknowledged_write_ops_per_sec,
        "ack_elapsed_ns": r.ack_elapsed.as_nanos() as u64,
        "ack_elapsed_ms": r.ack_elapsed.as_secs_f64() * 1000.0,
        "drain_elapsed_ns": r.drain_elapsed.as_nanos() as u64,
        "seal_elapsed_ns": r.seal_elapsed.as_nanos() as u64,
        "seal_elapsed_ms": r.seal_elapsed.as_secs_f64() * 1000.0,
        "seal_breakdown": seal_breakdown,
        "close_elapsed_ns": r.close_elapsed.as_nanos() as u64,
        "reopen_elapsed_ns": r.reopen_elapsed.as_nanos() as u64,
        "verify_elapsed_ns": r.verify_elapsed.as_nanos() as u64,
        "campaign_elapsed_ns": r.campaign_elapsed.as_nanos() as u64,
        "campaign_ops_per_sec": r.campaign_ops_per_sec,
        "pending_seal_inflight_at_last_ack": r.pending_seal_inflight_at_last_ack,
        "pending_seal_paths_at_last_ack": r.pending_seal_paths_at_last_ack,
        "sealed_segments_at_last_ack": r.sealed_segments_at_last_ack,
        "enrichment_backlog_at_last_ack": r.enrichment_backlog_at_last_ack,
        "enrichment": enrichment,
        "rotation_stages": rotation_stages,
        "reopen_exact": r.reopen_exact,
        "reopen_verify_mode": r.reopen_verify_mode,
        "timestamps_unix_ns": timestamps,
        "note": "Use acknowledged_write_ops_per_sec vs campaign_ops_per_sec (includes reopen+verify); no ambiguous ops_per_sec. skip-index disables live dual-index publish only — Hydra/Chimera still run at seal.",
    })
}

fn render_markdown_table(rows: &[AckFinalizeResult]) -> String {
    let mut out = String::from(
        "| Cell | Ack TPS | Ack time | Seal time | Campaign TPS | Reopen exact |\n\
         |---|---:|---:|---:|---:|---|\n",
    );
    let order = [
        MeasureCell::RealFull,
        MeasureCell::RealSkipIndex,
        MeasureCell::Discard,
        MeasureCell::RawMimic,
    ];
    for cell in order {
        if let Some(r) = rows.iter().find(|r| r.cell == cell) {
            out.push_str(&format!(
                "| {} | {:.0} | {:.2} s | {:.2} s | {:.0} | {} ({}) |\n",
                r.cell.label(),
                r.acknowledged_write_ops_per_sec,
                r.ack_elapsed.as_secs_f64(),
                r.seal_elapsed.as_secs_f64(),
                r.campaign_ops_per_sec,
                if r.reopen_exact { "yes" } else { "no" },
                r.reopen_verify_mode,
            ));
        } else {
            out.push_str(&format!("| {} | — | — | — | — | — |\n", cell.label()));
        }
    }
    out
}

/// Seal Interference Control: Real Full at seal thresholds above the workload.
///
/// Compares baseline (64 MiB) vs 512 MiB vs 1 GiB. Archives evidence under
/// `evidence_dir` (must be outside the active todo tree when principal so requires).
pub fn run_seal_interference_control(
    work_root: &Path,
    evidence_dir: &Path,
    target_bytes: u64,
    payload_size: usize,
    concurrency: usize,
    seed: u64,
    min_free_bytes: u64,
) -> Result<(), String> {
    fs::create_dir_all(evidence_dir).map_err(|e| format!("create evidence dir: {e}"))?;
    fs::create_dir_all(work_root).map_err(|e| format!("create work root: {e}"))?;

    let thresholds: &[(u64, &str)] = &[
        (64 * 1024 * 1024, "seal-64m"),
        (512 * 1024 * 1024, "seal-512m"),
        (1024 * 1024 * 1024, "seal-1g"),
    ];
    let mut rows: Vec<AckFinalizeResult> = Vec::with_capacity(3);
    let mut errors: Vec<String> = Vec::new();

    for &(seal_threshold, slug) in thresholds {
        let work = work_root.join(slug);
        if work.exists() {
            fs::remove_dir_all(&work).map_err(|e| format!("cleanup {}: {e}", work.display()))?;
        }
        let cfg = AckFinalizeConfig {
            work: work.clone(),
            cell: MeasureCell::RealFull,
            target_bytes,
            payload_size,
            concurrency,
            seed,
            seal_threshold,
            min_free_bytes,
            json_out: false,
            enrichment_enabled: true,
        };
        match run_ack_finalize(&cfg) {
            Ok(r) => {
                let path = evidence_dir.join(format!("{slug}.json"));
                fs::write(
                    &path,
                    serde_json::to_string_pretty(&result_json(&r))
                        .map_err(|e| format!("serialize {slug}: {e}"))?,
                )
                .map_err(|e| format!("write {}: {e}", path.display()))?;
                rows.push(r);
            }
            Err(e) => {
                errors.push(format!("{slug}: {e}"));
                eprintln!("seal-interference {slug} FAILED: {e}");
            }
        }
        if work.exists() {
            let _ = fs::remove_dir_all(&work);
        }
    }

    let table = render_seal_interference_table(&rows);
    let summary = json!({
        "kind": "seal_interference_control",
        "disclosure": DISCLOSURE,
        "recipe": {
            "fs": "APFS",
            "cell": "real-full",
            "payload_size": payload_size,
            "logical_data_bytes": target_bytes,
            "concurrency": concurrency,
            "durability": "Buffered",
            "awo": "Disabled",
            "seed": seed,
            "thresholds": ["64M", "512M", "1G"],
        },
        "interpretation": {
            "baseline_ack_tps_reference": 22600.0,
            "if_ack_jumps_significantly": "seal-pipeline interference suppressing live writes → attack sealing",
            "if_ack_stays_22_to_27k": "sealing not the main acknowledged-path bottleneck → return to real append path",
        },
        "honesty": [
            "skip-index disables live dual-index publish only; Hydra/Chimera still run during seal",
            "campaign_ops_per_sec includes reopen+verification — not DB lifecycle throughput",
            "store reopen_exact uses coverage-aware scan_live_logical ledger compare",
        ],
        "runs": rows.iter().map(result_json).collect::<Vec<_>>(),
        "errors": errors,
        "markdown_table": table,
    });
    let summary_path = evidence_dir.join("control.json");
    fs::write(
        &summary_path,
        serde_json::to_string_pretty(&summary).map_err(|e| format!("serialize control: {e}"))?,
    )
    .map_err(|e| format!("write {}: {e}", summary_path.display()))?;
    let table_path = evidence_dir.join("EVIDENCE_TABLE.md");
    fs::write(&table_path, format!("{table}\n"))
        .map_err(|e| format!("write {}: {e}", table_path.display()))?;

    println!("{table}");
    eprintln!(
        "seal-interference control: wrote {} and {}",
        summary_path.display(),
        table_path.display()
    );
    if !errors.is_empty() {
        return Err(format!(
            "seal interference control incomplete: {}",
            errors.join("; ")
        ));
    }
    if rows.len() != 3 {
        return Err(format!("expected 3 threshold runs, got {}", rows.len()));
    }
    Ok(())
}

/// Seal Fast Lane measurement: Real Full @ 64 MiB with many mid-run rotations.
///
/// Success: ack TPS within 10% of the ~83K high-threshold control, coverage
/// reopen exact, sealed_segments_at_last_ack > 1, enrichment backlog reported.
pub fn run_seal_fast_lane_measure(
    work: &Path,
    evidence_dir: &Path,
    target_bytes: u64,
    payload_size: usize,
    concurrency: usize,
    seed: u64,
    min_free_bytes: u64,
) -> Result<(), String> {
    fs::create_dir_all(evidence_dir).map_err(|e| format!("create evidence: {e}"))?;
    if work.exists() {
        fs::remove_dir_all(work).map_err(|e| format!("cleanup work: {e}"))?;
    }
    const CONTROL_ACK_TPS: f64 = 83_000.0;
    const SEAL_64M: u64 = 64 * 1024 * 1024;
    let cfg = AckFinalizeConfig {
        work: work.to_path_buf(),
        cell: MeasureCell::RealFull,
        target_bytes,
        payload_size,
        concurrency,
        seed,
        seal_threshold: SEAL_64M,
        min_free_bytes,
        json_out: false,
        enrichment_enabled: true,
    };
    let r = run_ack_finalize(&cfg)?;
    let floor = CONTROL_ACK_TPS * 0.90;
    let pass_ack = r.acknowledged_write_ops_per_sec >= floor;
    let pass_rotate = r.sealed_segments_at_last_ack >= 2;
    let pass_exact = r.reopen_exact;
    let summary = json!({
        "kind": "seal_fast_lane_measure",
        "disclosure": DISCLOSURE,
        "control_ack_tps": CONTROL_ACK_TPS,
        "success_floor_ack_tps": floor,
        "recipe": {
            "cell": "real-full",
            "seal_threshold": SEAL_64M,
            "logical_data_bytes": target_bytes,
            "payload_size": payload_size,
            "concurrency": concurrency,
            "seed": seed,
            "awo": "Disabled",
        },
        "result": result_json(&r),
        "gates": {
            "ack_within_10pct_of_83k": pass_ack,
            "numerous_rotations": pass_rotate,
            "reopen_exact_coverage_scan": pass_exact,
            "sealed_segments_at_last_ack": r.sealed_segments_at_last_ack,
            "pending_seal_inflight_at_last_ack": r.pending_seal_inflight_at_last_ack,
        },
        "pass": pass_ack && pass_rotate && pass_exact,
    });
    let path = evidence_dir.join("seal-fast-lane.json");
    fs::write(
        &path,
        serde_json::to_string_pretty(&summary).map_err(|e| format!("serialize: {e}"))?,
    )
    .map_err(|e| format!("write {}: {e}", path.display()))?;
    let table = format!(
        "| Metric | Value | Gate |\n|---|---:|---|\n\
         | Ack TPS | {:.0} | {} (≥ {:.0}) |\n\
         | Sealed @ last ack | {} | {} (≥ 2) |\n\
         | Pending seals @ ack | {} | (info) |\n\
         | Campaign TPS | {:.0} | (info) |\n\
         | Reopen exact | {} | {} |\n\
         | Drain ns | {} | (info) |\n\
         | Final seal ns | {} | (info) |\n\
         | Catalog ns | {} | (info) |\n",
        r.acknowledged_write_ops_per_sec,
        if pass_ack { "PASS" } else { "FAIL" },
        floor,
        r.sealed_segments_at_last_ack,
        if pass_rotate { "PASS" } else { "FAIL" },
        r.pending_seal_inflight_at_last_ack,
        r.campaign_ops_per_sec,
        if r.reopen_exact { "yes" } else { "no" },
        if pass_exact { "PASS" } else { "FAIL" },
        r.seal_breakdown.drain_lifecycle_ns,
        r.seal_breakdown.final_active_seal_ns,
        r.seal_breakdown.catalog_publication_ns,
    );
    fs::write(evidence_dir.join("EVIDENCE_TABLE.md"), format!("{table}\n"))
        .map_err(|e| format!("write table: {e}"))?;
    println!("{table}");
    if work.exists() {
        let _ = fs::remove_dir_all(work);
    }
    if !(pass_ack && pass_rotate && pass_exact) {
        return Err(format!(
            "seal fast lane gates failed: ack_pass={pass_ack} rotate_pass={pass_rotate} exact_pass={pass_exact} (ack={:.0} sealed={})",
            r.acknowledged_write_ops_per_sec, r.sealed_segments_at_last_ack
        ));
    }
    Ok(())
}

/// Enrichment-off control @ 64 MiB: isolate authoritative finalisation vs enrichment contention.
///
/// Interpretation: ack ≈ 83K ⇒ enrichment was the residual; ≈ 50K ⇒ auth finalize dominates.
pub fn run_enrichment_off_control(
    work: &Path,
    evidence_dir: &Path,
    target_bytes: u64,
    payload_size: usize,
    concurrency: usize,
    seed: u64,
    min_free_bytes: u64,
) -> Result<(), String> {
    fs::create_dir_all(evidence_dir).map_err(|e| format!("create evidence: {e}"))?;
    if work.exists() {
        fs::remove_dir_all(work).map_err(|e| format!("cleanup work: {e}"))?;
    }
    const CONTROL_ACK_TPS: f64 = 83_000.0;
    const PRIOR_FAST_LANE_ACK_TPS: f64 = 50_000.0;
    const SEAL_64M: u64 = 64 * 1024 * 1024;
    let cfg = AckFinalizeConfig {
        work: work.to_path_buf(),
        cell: MeasureCell::RealFull,
        target_bytes,
        payload_size,
        concurrency,
        seed,
        seal_threshold: SEAL_64M,
        min_free_bytes,
        json_out: false,
        enrichment_enabled: false,
    };
    let r = run_ack_finalize(&cfg)?;
    let near_83k = r.acknowledged_write_ops_per_sec >= CONTROL_ACK_TPS * 0.90;
    let near_50k = (r.acknowledged_write_ops_per_sec - PRIOR_FAST_LANE_ACK_TPS).abs()
        <= PRIOR_FAST_LANE_ACK_TPS * 0.15;
    let interpretation = if near_83k {
        "enrichment_contention_significant"
    } else if near_50k {
        "authoritative_finalisation_dominant"
    } else {
        "mixed_or_other"
    };
    let summary = json!({
        "kind": "enrichment_off_control",
        "disclosure": DISCLOSURE,
        "control_ack_tps": CONTROL_ACK_TPS,
        "prior_fast_lane_ack_tps": PRIOR_FAST_LANE_ACK_TPS,
        "enrichment_enabled": false,
        "recipe": {
            "cell": "real-full",
            "seal_threshold": SEAL_64M,
            "logical_data_bytes": target_bytes,
            "payload_size": payload_size,
            "concurrency": concurrency,
            "seed": seed,
            "awo": "Disabled",
        },
        "result": result_json(&r),
        "interpretation": interpretation,
        "near_83k": near_83k,
        "near_50k": near_50k,
    });
    let path = evidence_dir.join("enrichment-off-control.json");
    fs::write(
        &path,
        serde_json::to_string_pretty(&summary).map_err(|e| format!("serialize: {e}"))?,
    )
    .map_err(|e| format!("write {}: {e}", path.display()))?;
    let table = format!(
        "| Metric | Value |\n|---|---:|\n\
         | Ack TPS (enrichment off) | {:.0} |\n\
         | Sealed @ last ack | {} |\n\
         | Pending seals @ ack | {} |\n\
         | Enrichment backlog @ ack | {} |\n\
         | Reopen exact | {} |\n\
         | Interpretation | {} |\n",
        r.acknowledged_write_ops_per_sec,
        r.sealed_segments_at_last_ack,
        r.pending_seal_inflight_at_last_ack,
        r.enrichment_backlog_at_last_ack,
        if r.reopen_exact { "yes" } else { "no" },
        interpretation,
    );
    fs::write(evidence_dir.join("EVIDENCE_TABLE.md"), format!("{table}\n"))
        .map_err(|e| format!("write table: {e}"))?;
    println!("{table}");
    if work.exists() {
        let _ = fs::remove_dir_all(work);
    }
    Ok(())
}

/// Zero-Scan Authoritative Seal measure: enrichment on (isolated), ≥74.7K ack @ 64 MiB.
pub fn run_zero_scan_auth_seal_measure(
    work: &Path,
    evidence_dir: &Path,
    target_bytes: u64,
    payload_size: usize,
    concurrency: usize,
    seed: u64,
    min_free_bytes: u64,
) -> Result<(), String> {
    fs::create_dir_all(evidence_dir).map_err(|e| format!("create evidence: {e}"))?;
    if work.exists() {
        fs::remove_dir_all(work).map_err(|e| format!("cleanup work: {e}"))?;
    }
    const CONTROL_ACK_TPS: f64 = 83_000.0;
    const SEAL_64M: u64 = 64 * 1024 * 1024;
    let cfg = AckFinalizeConfig {
        work: work.to_path_buf(),
        cell: MeasureCell::RealFull,
        target_bytes,
        payload_size,
        concurrency,
        seed,
        seal_threshold: SEAL_64M,
        min_free_bytes,
        json_out: false,
        enrichment_enabled: true,
    };
    let r = run_ack_finalize(&cfg)?;
    let floor = CONTROL_ACK_TPS * 0.90;
    let pass_ack = r.acknowledged_write_ops_per_sec >= floor;
    let pass_rotate = r.sealed_segments_at_last_ack >= 2;
    let pass_exact = r.reopen_exact;
    let summary = json!({
        "kind": "zero_scan_auth_seal_measure",
        "disclosure": DISCLOSURE,
        "control_ack_tps": CONTROL_ACK_TPS,
        "success_floor_ack_tps": floor,
        "recipe": {
            "cell": "real-full",
            "seal_threshold": SEAL_64M,
            "logical_data_bytes": target_bytes,
            "payload_size": payload_size,
            "concurrency": concurrency,
            "seed": seed,
            "awo": "Disabled",
            "enrichment": "enabled_isolated",
        },
        "result": result_json(&r),
        "gates": {
            "ack_within_10pct_of_83k": pass_ack,
            "numerous_rotations": pass_rotate,
            "reopen_exact_coverage_scan": pass_exact,
            "sealed_segments_at_last_ack": r.sealed_segments_at_last_ack,
            "pending_seal_inflight_at_last_ack": r.pending_seal_inflight_at_last_ack,
            "enrichment_backlog_at_last_ack": r.enrichment_backlog_at_last_ack,
        },
        "pass": pass_ack && pass_rotate && pass_exact,
    });
    let path = evidence_dir.join("zero-scan-auth-seal.json");
    fs::write(
        &path,
        serde_json::to_string_pretty(&summary).map_err(|e| format!("serialize: {e}"))?,
    )
    .map_err(|e| format!("write {}: {e}", path.display()))?;
    let table = format!(
        "| Metric | Value | Gate |\n|---|---:|---|\n\
         | Ack TPS | {:.0} | {} (≥ {:.0}) |\n\
         | Sealed @ last ack | {} | {} (≥ 2) |\n\
         | Pending seals @ ack | {} | (info) |\n\
         | Enrichment backlog @ ack | {} | (info) |\n\
         | Campaign TPS | {:.0} | (info) |\n\
         | Reopen exact | {} | {} |\n",
        r.acknowledged_write_ops_per_sec,
        if pass_ack { "PASS" } else { "FAIL" },
        floor,
        r.sealed_segments_at_last_ack,
        if pass_rotate { "PASS" } else { "FAIL" },
        r.pending_seal_inflight_at_last_ack,
        r.enrichment_backlog_at_last_ack,
        r.campaign_ops_per_sec,
        if r.reopen_exact { "yes" } else { "no" },
        if pass_exact { "PASS" } else { "FAIL" },
    );
    fs::write(evidence_dir.join("EVIDENCE_TABLE.md"), format!("{table}\n"))
        .map_err(|e| format!("write table: {e}"))?;
    println!("{table}");
    if work.exists() {
        let _ = fs::remove_dir_all(work);
    }
    if !(pass_ack && pass_rotate && pass_exact) {
        return Err(format!(
            "zero-scan auth seal gates failed: ack_pass={pass_ack} rotate_pass={pass_rotate} exact_pass={pass_exact} (ack={:.0} sealed={})",
            r.acknowledged_write_ops_per_sec, r.sealed_segments_at_last_ack
        ));
    }
    Ok(())
}

fn render_seal_interference_table(rows: &[AckFinalizeResult]) -> String {
    let mut out = String::from(
        "| Seal threshold | Ack TPS | Ack time | Pending seals @ ack | Sealed @ ack | Drain ns | Final seal ns | Catalog ns | Hydra ns | Chimera ns | Campaign TPS | Reopen exact |\n\
         |---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n",
    );
    for r in rows {
        out.push_str(&format!(
            "| {} | {:.0} | {:.2} s | {} | {} | {} | {} | {} | {} | {} | {:.0} | {} |\n",
            format_bytes(r.seal_threshold),
            r.acknowledged_write_ops_per_sec,
            r.ack_elapsed.as_secs_f64(),
            r.pending_seal_inflight_at_last_ack,
            r.sealed_segments_at_last_ack,
            r.seal_breakdown.drain_lifecycle_ns,
            r.seal_breakdown.final_active_seal_ns,
            r.seal_breakdown.catalog_publication_ns,
            r.seal_breakdown.hydra_ns,
            r.seal_breakdown.chimera_ns,
            r.campaign_ops_per_sec,
            if r.reopen_exact { "yes" } else { "no" },
        ));
    }
    out
}
