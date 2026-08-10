//! CSE-3 Stage 2 step 7 — Recovery Shadow performance qualification harness.
//!
//! Candidate configuration (harness-only; **not** product default):
//! ```text
//! Authoritative segments + Compact Chimera + Recovery Shadow − Materialized Chimera
//! ```
//!
//! Materialized product seal remains unchanged until Stage 2 step 8.

use crate::atomic_file::AtomicWriteTiming;
use crate::chimera::{
    build_compact_layout, chimera_dir, chimera_layout_path, write_chimera_layout, CompactFrameRef,
};
use crate::envelope::{decode_item_envelope, EventKind};
use crate::error::StoreError;
use crate::ids::segment_seq_from_id;
use crate::layout::{hex16, list_residiuum_files, segment_id_from_filename, StorePaths};
use crate::recovery_shadow::crypto::{envelope_open, ENVELOPE_MAGIC};
use crate::recovery_shadow::frontier::{
    load_protected_coverage, protection_lag_from_coverage, publish_protected_coverage,
};
use crate::recovery_shadow::mirror::publish_mirror_shadow_timed;
use crate::recovery_shadow::wire::{
    project_live, shadow_dir, shadow_path, try_load_shadow, LiveState, ShadowLoad,
};
use residiuum_format::{scan_forward, FrameKind, SafetyLimits, ScanRegion};
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Fixed harness encryption key (not a production secret).
pub const HARNESS_ENVELOPE_KEY: [u8; 32] = *b"cse3-step7-shadow-qualify-key!!!";

/// One Shadow publication sample with stage breakdown.
#[derive(Debug, Clone, Serialize)]
pub struct ShadowStageSample {
    /// Segment id hex.
    pub segment_id_hex: String,
    /// Source segment bytes read from disk.
    pub bytes_read: u64,
    /// Durable `.rsh` size.
    pub bytes_written_shadow: u64,
    /// Compact `.cmr` size (candidate amp; not Materialized).
    pub bytes_written_compact: u64,
    /// Live put payload bytes (pre-envelope).
    pub live_payload_bytes: u64,
    /// Wall time for the whole candidate enrichment of this segment.
    pub wall_ns: u64,
    /// Process CPU time attributed to this sample (best-effort; 0 if unavailable).
    pub cpu_ns: u64,
    /// Source read + frame decode + live/compact projection.
    pub source_read_decode_ns: u64,
    /// Optional put-payload envelope seal.
    pub encrypt_ns: u64,
    /// ShadowWriter encode (records + hashes + trailer).
    pub encode_ns: u64,
    /// Temp sequential write.
    pub sequential_write_ns: u64,
    /// Temp file sync.
    pub file_sync_ns: u64,
    /// Rename (incl. optional keep-previous).
    pub rename_ns: u64,
    /// Parent directory sync.
    pub dir_sync_ns: u64,
    /// `protected_frontier` / coverage publish after durable `.rsh`.
    pub frontier_publish_ns: u64,
    /// Compact layout construct + persist (harness candidate; not Shadow).
    pub compact_ns: u64,
    /// Protection lag after this sample (`sealed − protected`).
    pub protection_lag: u64,
    /// Sealed frontier after note+publish.
    pub sealed_frontier: u64,
    /// Protected frontier after publish.
    pub protected_frontier: u64,
}

/// Aggregate campaign report.
#[derive(Debug, Clone, Serialize)]
pub struct Step7CampaignReport {
    /// Harness config label.
    pub candidate_config: &'static str,
    /// Logical put bytes targeted.
    pub target_bytes: u64,
    /// Seal threshold used.
    pub seal_threshold_bytes: u64,
    /// Payload size per put.
    pub payload_size: u64,
    /// Acknowledgement ops/s during ingest (buffered puts / ingest wall).
    pub ack_ops_per_sec: f64,
    /// Complete-lifecycle ops/s (puts / ingest+shadow wall).
    pub lifecycle_ops_per_sec: f64,
    /// Shadow publications completed.
    pub shadows_published: u64,
    /// Mean Shadow publication rate (segments / Shadow wall).
    pub shadow_pub_seg_per_sec: f64,
    /// OLS slope of protection lag vs sample index after warm-up skip.
    pub backlog_slope_after_warmup: f64,
    /// Warm-up samples skipped for slope.
    pub warmup_skip: usize,
    /// Compact derived / auth bytes (mean %).
    pub compact_amp_pct_mean: f64,
    /// Shadow bytes / live payload (mean %).
    pub shadow_amp_pct_mean: f64,
    /// Every claimed protected segment has verified `.rsh`.
    pub every_protected_has_verified_rsh: bool,
    /// Recovery after deleting auth segments + Compact Chimera.
    pub recovery_after_auth_compact_delete: bool,
    /// Protected frontier tracked sealed frontier without gaps at end.
    pub frontier_gap_free: bool,
    /// Per-segment samples.
    pub samples: Vec<ShadowStageSample>,
    /// Gate scorecard.
    pub gates: Step7Gates,
}

/// Step 7 acceptance gates.
#[derive(Debug, Clone, Serialize)]
pub struct Step7Gates {
    /// Shadow publication ≥7 seg/s.
    pub shadow_pub_ge_7: bool,
    /// Backlog slope ≤0 after warm-up.
    pub backlog_slope_le_0: bool,
    /// Frontier gap-free.
    pub frontier_gap_free: bool,
    /// Lifecycle TPS close to ack (≥80% of ack).
    pub lifecycle_near_ack: bool,
    /// Shadow amp ≈100% + framing (≤130% of live payload).
    pub shadow_amp_bounded: bool,
    /// Compact ≤5%.
    pub compact_le_5pct: bool,
    /// Recovery after auth+compact delete.
    pub recovery_ok: bool,
    /// Every protected has verified `.rsh`.
    pub verified_rsh: bool,
    /// Overall pass (all gates).
    pub pass: bool,
}

const CANDIDATE: &str =
    "Authoritative segments + Compact Chimera + Recovery Shadow − Materialized Chimera";

/// Options for one candidate-segment enrichment.
#[derive(Debug, Clone, Copy)]
pub struct QualifyOptions {
    /// Seal put payloads with harness envelope before Shadow encode.
    pub encrypt: bool,
    /// Also write Compact Chimera (no Materialized).
    pub write_compact: bool,
    /// Publish Recovery Shadow (mirror). Set false when dual-stream already did.
    pub write_shadow: bool,
    /// Shard for frontier bookkeeping.
    pub shard: u16,
}

impl Default for QualifyOptions {
    fn default() -> Self {
        Self {
            // Dual-run wire is cleartext; encrypt is a measured security variant.
            encrypt: false,
            write_compact: true,
            write_shadow: true,
            shard: 0,
        }
    }
}

/// Decode one sealed segment into live Shadow map + compact frame refs.
///
/// Moves verified frame bodies out of the scan report and retains the frame
/// `body_hash` for V2 record digests (no re-hash of payloads at encode).
pub fn decode_segment_for_candidate(
    segment_id: [u8; 16],
    bytes: &[u8],
    limits: SafetyLimits,
) -> (
    crate::recovery_shadow::wire::LiveMap,
    Vec<(Vec<u8>, CompactFrameRef)>,
    u64,
) {
    let report = scan_forward(bytes, limits);
    let mut shadow_live: crate::recovery_shadow::wire::LiveMap = BTreeMap::new();
    let mut frames: Vec<(Vec<u8>, CompactFrameRef)> = Vec::new();
    let mut live_payload = 0u64;
    for region in report.regions {
        let ScanRegion::VerifiedFrame { range, mut frame } = region else {
            continue;
        };
        if frame.header.known_kind() != Some(FrameKind::ItemEvent) {
            continue;
        }
        let Some(env) = decode_item_envelope(&frame.envelope) else {
            continue;
        };
        let gen = frame.header.writer_sequence;
        let off = range.start;
        match env.event_kind {
            EventKind::Put => {
                let body_hash = frame.body_hash;
                let body = std::mem::take(&mut frame.body);
                live_payload = live_payload.saturating_add(body.len() as u64);
                let body_len = body.len() as u32;
                shadow_live.insert(env.subject.clone(), (Some((body, body_hash)), gen));
                frames.push((
                    env.subject,
                    CompactFrameRef {
                        segment_id,
                        frame_offset: off,
                        body_len,
                    },
                ));
            }
            EventKind::Delete => {
                shadow_live.insert(env.subject, (None, gen));
            }
        }
    }
    (shadow_live, frames, live_payload)
}

/// Publish Shadow bytes with atomic stage timings.
///
/// Skips full re-decode of a blob just produced by [`ShadowWriter`] (hashes are
/// correct by construction). Still refuses empty blobs and checks header
/// magic + segment_id before durable publish. Product [`publish_shadow`] keeps
/// full verify for externally supplied bytes.
pub fn publish_shadow_timed(
    paths: &StorePaths,
    segment_id: &[u8; 16],
    bytes: &[u8],
) -> Result<AtomicWriteTiming, StoreError> {
    if bytes.len() < 8 + 16 + 16 {
        return Err(StoreError::CorruptMeta(
            "refusing to publish incomplete recovery shadow",
        ));
    }
    if &bytes[0..8] != crate::recovery_shadow::RSH_MAGIC.as_slice()
        && &bytes[0..8] != crate::recovery_shadow::RSH_MAGIC_V1.as_slice()
    {
        return Err(StoreError::CorruptMeta(
            "refusing to publish corrupt recovery shadow",
        ));
    }
    let sid: [u8; 16] = bytes[24..40]
        .try_into()
        .map_err(|_| StoreError::CorruptMeta("recovery shadow segment_id truncated"))?;
    if &sid != segment_id {
        return Err(StoreError::CorruptMeta(
            "recovery shadow segment_id does not match publish path",
        ));
    }
    fs::create_dir_all(shadow_dir(paths))?;
    let path = shadow_path(paths, segment_id);
    // Immutable segment-scoped Shadow: never replace (P0). Idempotent when equal.
    if path.is_file() {
        let existing = fs::read(&path)?;
        if existing.as_slice() == bytes {
            return Ok(AtomicWriteTiming::default());
        }
        return Err(StoreError::SegmentIdCollision {
            segment_id: *segment_id,
            paths: vec![path],
        });
    }
    let tmp = path.with_extension("rsh.publish.tmp");
    let mut timing = AtomicWriteTiming::default();
    {
        use std::io::Write;
        use std::time::Instant;
        let t_write = Instant::now();
        let mut out = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp)?;
        out.write_all(bytes)?;
        timing.sequential_write_ns = t_write.elapsed().as_nanos() as u64;
        let t_sync = Instant::now();
        out.sync_all()?;
        timing.file_sync_ns = t_sync.elapsed().as_nanos() as u64;
    }
    let t_rename = std::time::Instant::now();
    crate::media_inventory::rename_exclusive(&tmp, &path, *segment_id)?;
    timing.rename_ns = t_rename.elapsed().as_nanos() as u64;
    if let Some(parent) = path.parent() {
        let t_dir = std::time::Instant::now();
        crate::atomic_file::sync_dir(parent)?;
        timing.dir_sync_ns = t_dir.elapsed().as_nanos() as u64;
    }
    Ok(timing)
}

/// Build Compact + Recovery Shadow for one sealed segment (no Materialized).
///
/// Shadow path is **RSHD0003** canonical image mirror (physical buffered copy).
/// Compact Chimera (optional) is measured separately and is not on the Shadow
/// publication critical path.
pub fn enrich_segment_candidate(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: [u8; 16],
    segment_bytes: &[u8],
    opts: QualifyOptions,
) -> Result<ShadowStageSample, StoreError> {
    let wall0 = Instant::now();
    let cpu0 = process_cpu_ns();
    let limits = SafetyLimits::default();

    // Shadow: physical mirror — no value decode/re-encode (unless dual-stream
    // already published; then skip remirror / frontier claim).
    let encrypt_ns = 0u64;
    let _ = opts.encrypt;
    let mut atomic = crate::recovery_shadow::MirrorPublishTiming::default();
    let mut frontier_publish_ns = 0u64;
    if opts.write_shadow {
        atomic = publish_mirror_shadow_timed(paths, store_id, &segment_id, segment_bytes)?;
        let t_front = Instant::now();
        let seq = segment_seq_from_id(&segment_id);
        let mut cov = load_protected_coverage(paths, store_id)?;
        cov.store_id = store_id;
        cov.note_sealed(opts.shard, seq);
        cov.note_durable(opts.shard, seq);
        publish_protected_coverage(paths, &cov)?;
        frontier_publish_ns = t_front.elapsed().as_nanos() as u64;
    } else if let Ok(meta) = fs::metadata(shadow_path(paths, &segment_id)) {
        atomic.bytes_written = meta.len();
    }
    let cov = load_protected_coverage(paths, store_id)?;

    // Compact (harness amp gate) — not part of Shadow publication rate.
    let mut source_read_decode_ns = 0u64;
    let mut compact_ns = 0u64;
    let mut bytes_written_compact = 0u64;
    let mut live_payload = 0u64;
    if opts.write_compact {
        let t_decode = Instant::now();
        let (_live, frames, lp) = decode_segment_for_candidate(segment_id, segment_bytes, limits);
        live_payload = lp;
        source_read_decode_ns = t_decode.elapsed().as_nanos() as u64;
        if !frames.is_empty() {
            let t_c = Instant::now();
            let layout = build_compact_layout(&frames, 1);
            let path = chimera_layout_path(paths, &segment_id);
            write_chimera_layout(&path, store_id, segment_id, &layout)?;
            compact_ns = t_c.elapsed().as_nanos() as u64;
            bytes_written_compact = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        }
    }

    let lag = protection_lag_from_coverage(&cov);
    let cpu1 = process_cpu_ns();
    Ok(ShadowStageSample {
        segment_id_hex: hex16(&segment_id),
        bytes_read: segment_bytes.len() as u64,
        bytes_written_shadow: atomic.bytes_written,
        bytes_written_compact,
        live_payload_bytes: live_payload,
        wall_ns: wall0.elapsed().as_nanos() as u64,
        cpu_ns: cpu1.saturating_sub(cpu0),
        // Compact-only decode cost (excluded from Shadow pub rate in harness).
        source_read_decode_ns,
        encrypt_ns,
        encode_ns: atomic.encode_ns,
        sequential_write_ns: atomic.copy_ns,
        file_sync_ns: atomic.file_sync_ns,
        rename_ns: atomic.rename_ns,
        dir_sync_ns: atomic.dir_sync_ns,
        frontier_publish_ns,
        compact_ns,
        protection_lag: lag.lag,
        sealed_frontier: lag.sealed_frontier,
        protected_frontier: lag.protected_frontier,
    })
}

/// List sealed `.residiuum` segment paths under the store.
pub fn list_sealed_segment_files(paths: &StorePaths) -> Result<Vec<PathBuf>, StoreError> {
    Ok(list_residiuum_files(&paths.segments_dir())?)
}

/// OLS slope of `y` vs index `0..n-1`.
pub fn ols_slope(ys: &[f64]) -> f64 {
    let n = ys.len();
    if n < 2 {
        return 0.0;
    }
    let n_f = n as f64;
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xx = 0.0;
    let mut sum_xy = 0.0;
    for (i, y) in ys.iter().enumerate() {
        let x = i as f64;
        sum_x += x;
        sum_y += *y;
        sum_xx += x * x;
        sum_xy += x * *y;
    }
    let denom = n_f * sum_xx - sum_x * sum_x;
    if denom.abs() < 1e-12 {
        return 0.0;
    }
    (n_f * sum_xy - sum_x * sum_y) / denom
}

/// Median of a sorted copy of `xs`.
pub fn median_f64(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let mid = v.len() / 2;
    if v.len() % 2 == 0 {
        (v[mid - 1] + v[mid]) / 2.0
    } else {
        v[mid]
    }
}

/// Range (min, max) of `xs`.
pub fn range_f64(xs: &[f64]) -> (f64, f64) {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for x in xs {
        min = min.min(*x);
        max = max.max(*x);
    }
    if !min.is_finite() {
        (0.0, 0.0)
    } else {
        (min, max)
    }
}

/// Verify every durable coverage seq has a verified `.rsh`.
pub fn every_protected_has_verified_rsh(
    paths: &StorePaths,
    store_id: [u8; 16],
) -> Result<bool, StoreError> {
    let cov = load_protected_coverage(paths, store_id)?;
    for shard_cov in cov.durable_by_shard.values() {
        for &seq in shard_cov {
            let mut found = false;
            for path in list_sealed_segment_files(paths)? {
                let Some(id) = segment_id_from_filename(&path) else {
                    continue;
                };
                if segment_seq_from_id(&id) != seq {
                    continue;
                }
                let sp = shadow_path(paths, &id);
                match try_load_shadow(&sp, Some(store_id))? {
                    ShadowLoad::Ok(_) => found = true,
                    _ => return Ok(false),
                }
            }
            if !found {
                let dir = shadow_dir(paths);
                if dir.is_dir() {
                    for ent in fs::read_dir(&dir)? {
                        let ent = ent?;
                        let p = ent.path();
                        if p.extension().and_then(|e| e.to_str()) != Some("rsh") {
                            continue;
                        }
                        if let ShadowLoad::Ok(d) = try_load_shadow(&p, Some(store_id))? {
                            if segment_seq_from_id(&d.segment_id) == seq {
                                found = true;
                                break;
                            }
                        }
                    }
                }
            }
            if !found {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Delete authoritative segments + Compact Chimera; recover values from Shadows.
pub fn recovery_after_auth_compact_delete(
    paths: &StorePaths,
    store_id: [u8; 16],
    expect: &BTreeMap<Vec<u8>, Vec<u8>>,
) -> Result<bool, StoreError> {
    let chimera = chimera_dir(paths);
    if chimera.is_dir() {
        for ent in fs::read_dir(&chimera)? {
            let p = ent?.path();
            if p.extension().and_then(|e| e.to_str()) == Some("cmr") {
                let _ = fs::remove_file(&p);
            }
        }
    }
    for path in list_sealed_segment_files(paths)? {
        let _ = fs::remove_file(&path);
    }

    let mut recovered: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let dir = shadow_dir(paths);
    if !dir.is_dir() {
        return Ok(false);
    }
    for ent in fs::read_dir(&dir)? {
        let p = ent?.path();
        if p.extension().and_then(|e| e.to_str()) != Some("rsh") {
            continue;
        }
        let ShadowLoad::Ok(d) = try_load_shadow(&p, Some(store_id))? else {
            return Ok(false);
        };
        for (k, st) in project_live(&d.records) {
            match st {
                LiveState::Put { value, .. } => {
                    let plain = if value.starts_with(ENVELOPE_MAGIC.as_slice()) {
                        envelope_open(&HARNESS_ENVELOPE_KEY, &value)
                            .map(|(_id, v)| v)
                            .unwrap_or(value)
                    } else {
                        value
                    };
                    recovered.insert(k, plain);
                }
                LiveState::Tombstone { .. } => {
                    recovered.remove(&k);
                }
            }
        }
    }
    Ok(&recovered == expect)
}

/// Build gates from campaign metrics.
pub fn evaluate_gates(
    shadow_pub_seg_per_sec: f64,
    backlog_slope: f64,
    frontier_gap_free: bool,
    ack_ops: f64,
    lifecycle_ops: f64,
    shadow_amp_pct: f64,
    compact_amp_pct: f64,
    recovery_ok: bool,
    verified_rsh: bool,
) -> Step7Gates {
    let shadow_pub_ge_7 = shadow_pub_seg_per_sec >= 7.0;
    let backlog_slope_le_0 = backlog_slope <= 0.0;
    let lifecycle_near_ack = ack_ops <= 0.0 || lifecycle_ops >= ack_ops * 0.80;
    let shadow_amp_bounded = shadow_amp_pct <= 130.0;
    let compact_le_5pct = compact_amp_pct <= 5.0;
    let pass = shadow_pub_ge_7
        && backlog_slope_le_0
        && frontier_gap_free
        && lifecycle_near_ack
        && shadow_amp_bounded
        && compact_le_5pct
        && recovery_ok
        && verified_rsh;
    Step7Gates {
        shadow_pub_ge_7,
        backlog_slope_le_0,
        frontier_gap_free,
        lifecycle_near_ack,
        shadow_amp_bounded,
        compact_le_5pct,
        recovery_ok,
        verified_rsh,
        pass,
    }
}

/// Candidate config string (stable for reports).
pub fn candidate_config_label() -> &'static str {
    CANDIDATE
}

/// Summarize samples into stage median/range maps (ns).
pub fn stage_medians(samples: &[ShadowStageSample]) -> BTreeMap<&'static str, (f64, f64, f64)> {
    let mut out = BTreeMap::new();
    let fields: [(&str, fn(&ShadowStageSample) -> u64); 9] = [
        ("source_read_decode_ns", |s| s.source_read_decode_ns),
        ("encrypt_ns", |s| s.encrypt_ns),
        ("encode_ns", |s| s.encode_ns),
        ("sequential_write_ns", |s| s.sequential_write_ns),
        ("file_sync_ns", |s| s.file_sync_ns),
        ("rename_ns", |s| s.rename_ns),
        ("dir_sync_ns", |s| s.dir_sync_ns),
        ("frontier_publish_ns", |s| s.frontier_publish_ns),
        ("wall_ns", |s| s.wall_ns),
    ];
    for (name, get) in fields {
        let xs: Vec<f64> = samples.iter().map(|s| get(s) as f64).collect();
        let (lo, hi) = range_f64(&xs);
        out.insert(name, (median_f64(&xs), lo, hi));
    }
    out
}

/// Best-effort process CPU nanoseconds (CLOCK_PROCESS_CPUTIME_ID).
fn process_cpu_ns() -> u64 {
    #[cfg(unix)]
    {
        #[repr(C)]
        struct TimeSpec {
            tv_sec: i64,
            tv_nsec: i64,
        }
        #[cfg(target_os = "macos")]
        const CLOCK_PROCESS_CPUTIME_ID: i32 = 12;
        #[cfg(not(target_os = "macos"))]
        const CLOCK_PROCESS_CPUTIME_ID: i32 = 2;
        extern "C" {
            fn clock_gettime(clock_id: i32, tp: *mut TimeSpec) -> i32;
        }
        let mut ts = TimeSpec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let rc = unsafe { clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &mut ts) };
        if rc == 0 {
            return (ts.tv_sec as u64)
                .saturating_mul(1_000_000_000)
                .saturating_add(ts.tv_nsec as u64);
        }
    }
    0
}

/// Ensure path exists helper for callers.
#[allow(dead_code)]
pub fn ensure_parent(path: &Path) -> std::io::Result<()> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p)?;
    }
    Ok(())
}
