//! PEER-SQL — same-bed Residiuum vs SQLite peer pump (diagnostic only).
//!
//! Contract: `doc/wip/status/surveys/README-PEER-SQL.md`
//!
//! Target is **logical payload** bytes (`keys * payload_size`), not Residiuum
//! on-disk footprint, so engines are comparable for ops/s and logical MiB/s.

use crate::size::{dir_size_bytes, ensure_free_space, format_bytes};
use residiuum_store::adaptive_write::{
    AdaptiveWriteHandle, AdaptiveWriteMode, AdaptiveWritePolicy, AdmissionResult,
};
use residiuum_store::{
    DiagnosticIoSink, DurabilityMode, SegmentGrowthPolicy, Store, WriteCondition,
};
use rusqlite::Connection;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const DISCLOSURE: &str = "Diagnostic only — not a published SLO. PEER-SQL same-bed peer \
    (doc/wip/status/surveys/README-PEER-SQL.md; \
    doc/reference/operations/BENCHMARK_DISCLOSURE.md). Compare A vs A and B vs B only.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerEngine {
    Residiuum,
    Sqlite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerMode {
    /// Autocommit / Residiuum put_batch_size=1.
    A,
    /// SQLite txn-128 / Residiuum put_batch_size=128.
    B,
}

impl PeerMode {
    pub fn batch_size(self) -> usize {
        match self {
            PeerMode::A => 1,
            PeerMode::B => 128,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            PeerMode::A => "A_autocommit",
            PeerMode::B => "B_txn_128",
        }
    }
}

impl PeerEngine {
    pub fn label(self) -> &'static str {
        match self {
            PeerEngine::Residiuum => "residiuum",
            PeerEngine::Sqlite => "sqlite",
        }
    }
}

/// Residiuum AWO lease for peer-pump (SQLite ignores).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PeerAwoMode {
    #[default]
    Disabled,
    Static,
    Adaptive,
}

impl PeerAwoMode {
    pub fn label(self) -> &'static str {
        match self {
            PeerAwoMode::Disabled => "disabled",
            PeerAwoMode::Static => "static",
            PeerAwoMode::Adaptive => "adaptive",
        }
    }

    pub fn lease_active(self) -> bool {
        !matches!(self, PeerAwoMode::Disabled)
    }
}

pub fn parse_awo_mode(s: &str) -> Result<PeerAwoMode, String> {
    match s.to_ascii_lowercase().as_str() {
        "disabled" | "off" | "none" => Ok(PeerAwoMode::Disabled),
        "static" => Ok(PeerAwoMode::Static),
        "adaptive" | "smart" => Ok(PeerAwoMode::Adaptive),
        other => Err(format!(
            "unknown awo-mode `{other}` (disabled|static|adaptive)"
        )),
    }
}

/// Residiuum diagnostic segment-tail sink (SQLite ignores).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PeerDiagIo {
    #[default]
    Real,
    Discard,
    DevNull,
    /// 100 KiB / 250 ms coalesce spike — hammerblast only (QD=1 hangs).
    Coalesce100k,
    SeekOnly,
    RealNoSeek,
    RealOverwrite,
    RealPrealloc,
    RealPreallocFill,
    RealPreallocFcntl,
    RealPreallocZero,
    RealPreallocWatermark,
}

impl PeerDiagIo {
    pub fn label(self) -> &'static str {
        match self {
            PeerDiagIo::Real => "real",
            PeerDiagIo::Discard => "discard",
            PeerDiagIo::DevNull => "devnull",
            PeerDiagIo::Coalesce100k => "coalesce100k",
            PeerDiagIo::SeekOnly => "seekonly",
            PeerDiagIo::RealNoSeek => "realnoseek",
            PeerDiagIo::RealOverwrite => "realoverwrite",
            PeerDiagIo::RealPrealloc => "realprealloc",
            PeerDiagIo::RealPreallocFill => "realpreallocfill",
            PeerDiagIo::RealPreallocFcntl => "realpreallocfcntl",
            PeerDiagIo::RealPreallocZero => "realprealloczero",
            PeerDiagIo::RealPreallocWatermark => "realpreallocwm",
        }
    }

    pub fn to_sink(self) -> DiagnosticIoSink {
        match self {
            PeerDiagIo::Real => DiagnosticIoSink::Real,
            PeerDiagIo::Discard => DiagnosticIoSink::Discard,
            PeerDiagIo::DevNull => DiagnosticIoSink::DevNull,
            PeerDiagIo::Coalesce100k => DiagnosticIoSink::Coalesce100k,
            PeerDiagIo::SeekOnly => DiagnosticIoSink::SeekOnly,
            PeerDiagIo::RealNoSeek => DiagnosticIoSink::RealNoSeek,
            PeerDiagIo::RealOverwrite => DiagnosticIoSink::RealOverwrite,
            PeerDiagIo::RealPrealloc => DiagnosticIoSink::RealPrealloc,
            PeerDiagIo::RealPreallocFill => DiagnosticIoSink::RealPreallocFill,
            PeerDiagIo::RealPreallocFcntl => DiagnosticIoSink::RealPreallocFcntl,
            PeerDiagIo::RealPreallocZero => DiagnosticIoSink::RealPreallocZero,
            PeerDiagIo::RealPreallocWatermark => DiagnosticIoSink::RealPreallocWatermark,
        }
    }
}

pub fn parse_diag_io(s: &str) -> Result<PeerDiagIo, String> {
    match s.to_ascii_lowercase().as_str() {
        "real" | "disk" => Ok(PeerDiagIo::Real),
        "discard" | "none" => Ok(PeerDiagIo::Discard),
        "devnull" | "null" => Ok(PeerDiagIo::DevNull),
        "coalesce" | "coalesce100k" | "100k" | "coalesce64k" | "64k" => Ok(PeerDiagIo::Coalesce100k),
        "seekonly" | "seek" => Ok(PeerDiagIo::SeekOnly),
        "realnoseek" | "noseek" => Ok(PeerDiagIo::RealNoSeek),
        "realoverwrite" | "overwrite" => Ok(PeerDiagIo::RealOverwrite),
        "realprealloc" | "prealloc" => Ok(PeerDiagIo::RealPrealloc),
        "realpreallocfill" | "preallocfill" => Ok(PeerDiagIo::RealPreallocFill),
        "realpreallocfcntl" | "fpreallocate" | "fcntl" => Ok(PeerDiagIo::RealPreallocFcntl),
        "realprealloczero" | "prealloczero" | "zero" => Ok(PeerDiagIo::RealPreallocZero),
        "realpreallocwm" | "watermark" | "ahead" => Ok(PeerDiagIo::RealPreallocWatermark),
        other => Err(format!(
            "unknown diag-io `{other}` (real|discard|devnull|coalesce100k|seekonly|realnoseek|realoverwrite|realprealloc|realpreallocfill|realpreallocfcntl|realprealloczero|realpreallocwm)"
        )),
    }
}

#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// Work directory (created if missing). Residiuum store: `work/store`;
    /// SQLite file: `work/peer.sqlite`.
    pub work: PathBuf,
    pub engine: PeerEngine,
    pub mode: PeerMode,
    /// Logical payload budget: stop when `keys * payload_size >= target_bytes`.
    pub target_bytes: u64,
    pub payload_size: usize,
    pub seed: u64,
    pub min_free_bytes: u64,
    pub json_out: bool,
    /// Soft seal threshold (bytes). Default 64 MiB matches surveys; raise to
    /// measure Mode A without mid-run seal cost (seal is separate from put prep).
    pub seal_threshold: u64,
    /// Residiuum only: attach Static/Adaptive AWO lease (default off).
    pub awo_mode: PeerAwoMode,
    /// Concurrent client threads (server-async feed). Default 1 = embedded sync QD=1.
    /// Each thread is still per-thread QD=1; overlap comes from N threads in flight.
    pub concurrency: usize,
    /// Residiuum only: diagnostic segment-tail I/O sink (default real).
    pub diag_io: PeerDiagIo,
    /// Residiuum only: product segment growth (`grow` default | `watermark`).
    pub segment_growth: PeerSegmentGrowth,
    /// When `segment_growth` is watermark: reserved capacity in MiB.
    /// `None` → [`SegmentGrowthPolicy::watermark_default`] capacity (64 MiB).
    pub wm_capacity_mib: Option<u64>,
    /// When `segment_growth` is watermark: zero-runway chunk in MiB.
    /// `None` → default chunk (64 MiB). Hosts may set multi‑GiB values.
    pub wm_chunk_mib: Option<u64>,
    /// Residiuum diagnostic: skip dual-index publish / derived (data path only).
    pub diag_skip_index: bool,
    /// Residiuum diagnostic: accumulate boundary probe; report write vs index ms.
    pub boundary_probe: bool,
}

/// Product segment growth for Residiuum peer-pump (not a diagnostic sink).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PeerSegmentGrowth {
    #[default]
    GrowOnAppend,
    Watermark,
}

impl PeerSegmentGrowth {
    pub fn label(self) -> &'static str {
        match self {
            PeerSegmentGrowth::GrowOnAppend => "grow",
            PeerSegmentGrowth::Watermark => "watermark",
        }
    }
}

/// Resolve product growth policy from peer flags (capacity/chunk are host knobs).
pub fn segment_growth_policy(
    growth: PeerSegmentGrowth,
    capacity_mib: Option<u64>,
    chunk_mib: Option<u64>,
) -> Result<SegmentGrowthPolicy, String> {
    match growth {
        PeerSegmentGrowth::GrowOnAppend => {
            if capacity_mib.is_some() || chunk_mib.is_some() {
                return Err(
                    "--wm-capacity-mib / --wm-chunk-mib require --segment-growth watermark".into(),
                );
            }
            Ok(SegmentGrowthPolicy::GrowOnAppend)
        }
        PeerSegmentGrowth::Watermark => {
            let capacity_bytes = match capacity_mib {
                Some(0) => return Err("--wm-capacity-mib must be > 0".into()),
                Some(mib) => mib.saturating_mul(1024 * 1024),
                None => residiuum_store::WATERMARK_DEFAULT_CAPACITY_BYTES,
            };
            let chunk_bytes = match chunk_mib {
                Some(0) => return Err("--wm-chunk-mib must be > 0".into()),
                Some(mib) => mib.saturating_mul(1024 * 1024),
                None => residiuum_store::WATERMARK_DEFAULT_CHUNK_BYTES,
            };
            Ok(SegmentGrowthPolicy::watermark(capacity_bytes, chunk_bytes))
        }
    }
}

pub fn parse_segment_growth(s: &str) -> Result<PeerSegmentGrowth, String> {
    match s.to_ascii_lowercase().as_str() {
        "grow" | "grow-on-append" | "default" | "off" => Ok(PeerSegmentGrowth::GrowOnAppend),
        "watermark" | "wm" | "ahead" => Ok(PeerSegmentGrowth::Watermark),
        other => Err(format!("unknown segment-growth `{other}` (grow|watermark)")),
    }
}

#[derive(Debug, Clone, Default)]
struct ProcessSamples {
    peak_rss_bytes: Option<u64>,
    peak_cpu_pct: Option<f64>,
    last_cpu_pct: Option<f64>,
    sample_count: u64,
}

pub fn parse_engine(s: &str) -> Result<PeerEngine, String> {
    match s.to_ascii_lowercase().as_str() {
        "residiuum" | "res" | "rr" => Ok(PeerEngine::Residiuum),
        "sqlite" | "sql" => Ok(PeerEngine::Sqlite),
        other => Err(format!("unknown engine `{other}` (residiuum|sqlite)")),
    }
}

pub fn parse_mode(s: &str) -> Result<PeerMode, String> {
    match s.to_ascii_lowercase().as_str() {
        "a" | "a_autocommit" | "autocommit" => Ok(PeerMode::A),
        "b" | "b_txn_128" | "txn" | "txn128" => Ok(PeerMode::B),
        other => Err(format!("unknown mode `{other}` (A|B)")),
    }
}

pub fn run_peer_pump(cfg: &PeerConfig) -> Result<(), String> {
    if cfg.target_bytes == 0 {
        return Err("target-bytes must be > 0".into());
    }
    if cfg.payload_size == 0 {
        return Err("payload-size must be > 0".into());
    }
    if cfg.awo_mode.lease_active() && cfg.engine != PeerEngine::Residiuum {
        return Err("--awo-mode static|adaptive requires --engine residiuum".into());
    }
    if cfg.concurrency == 0 {
        return Err("--concurrency must be >= 1".into());
    }
    if cfg.concurrency > 1 && cfg.mode != PeerMode::A {
        return Err("--concurrency > 1 currently supports Mode A only (server-async feed)".into());
    }
    if cfg.diag_io != PeerDiagIo::Real && cfg.engine != PeerEngine::Residiuum {
        return Err("--diag-io requires --engine residiuum".into());
    }
    if cfg.diag_io == PeerDiagIo::Coalesce100k && cfg.concurrency <= 1 {
        return Err(
            "--diag-io coalesce100k requires --concurrency > 1 (250ms floor hangs QD=1)".into(),
        );
    }
    if cfg.segment_growth != PeerSegmentGrowth::GrowOnAppend && cfg.engine != PeerEngine::Residiuum
    {
        return Err("--segment-growth requires --engine residiuum".into());
    }
    if cfg.segment_growth == PeerSegmentGrowth::Watermark && cfg.diag_io != PeerDiagIo::Real {
        return Err(
            "--segment-growth watermark requires --diag-io real (product path; use diag wm for bisection)"
                .into(),
        );
    }
    if (cfg.diag_skip_index || cfg.boundary_probe) && cfg.engine != PeerEngine::Residiuum {
        return Err("--diag-skip-index / --boundary-probe require --engine residiuum".into());
    }
    fs::create_dir_all(&cfg.work).map_err(|e| format!("create work dir: {e}"))?;
    let free = ensure_free_space(&cfg.work, cfg.min_free_bytes)?;
    if !cfg.json_out && cfg.min_free_bytes > 0 {
        eprintln!(
            "peer-pump free-space ok: free={} min-free={} path={}",
            format_bytes(free),
            format_bytes(cfg.min_free_bytes),
            cfg.work.display()
        );
    }

    let mut payload = vec![0u8; cfg.payload_size];
    fill_payload(&mut payload, cfg.seed);

    let result = match cfg.engine {
        PeerEngine::Residiuum => pump_residiuum(cfg, &payload)?,
        PeerEngine::Sqlite => pump_sqlite(cfg, &payload)?,
    };

    let report = json!({
        "prong": "peer-pump",
        "ok": result.ok,
        "engine": cfg.engine.label(),
        "mode": cfg.mode.label(),
        "awo_mode": if cfg.engine == PeerEngine::Residiuum {
            cfg.awo_mode.label()
        } else {
            ""
        },
        "awo_path": result.awo_path,
        "cook_parallelism": result.cook_parallelism,
        "concurrency": cfg.concurrency,
        "diag_io": if cfg.engine == PeerEngine::Residiuum {
            cfg.diag_io.label()
        } else {
            ""
        },
        "segment_growth": if cfg.engine == PeerEngine::Residiuum {
            cfg.segment_growth.label()
        } else {
            ""
        },
        "wm_capacity_mib": if cfg.engine == PeerEngine::Residiuum
            && cfg.segment_growth == PeerSegmentGrowth::Watermark
        {
            cfg.wm_capacity_mib.unwrap_or(
                residiuum_store::WATERMARK_DEFAULT_CAPACITY_BYTES / (1024 * 1024),
            )
        } else {
            0
        },
        "wm_chunk_mib": if cfg.engine == PeerEngine::Residiuum
            && cfg.segment_growth == PeerSegmentGrowth::Watermark
        {
            cfg.wm_chunk_mib
                .unwrap_or(residiuum_store::WATERMARK_DEFAULT_CHUNK_BYTES / (1024 * 1024))
        } else {
            0
        },
        "diag_skip_index": if cfg.engine == PeerEngine::Residiuum {
            cfg.diag_skip_index
        } else {
            false
        },
        "boundary": {
            "enabled": cfg.boundary_probe && cfg.engine == PeerEngine::Residiuum,
            "data_write_sum_ms": result.data_write_sum_ms,
            "data_write_samples": result.data_write_samples,
            "index_publish_sum_ms": result.index_publish_sum_ms,
            "index_publish_samples": result.index_publish_samples,
            "index_post_sum_ms": result.index_post_sum_ms,
            "index_post_samples": result.index_post_samples,
            "encode_sum_ms": result.encode_sum_ms,
            "append_sum_ms": result.append_sum_ms,
            "prep_sum_ms": result.prep_sum_ms,
        },
        "feed_shape": if cfg.concurrency <= 1 {
            "embedded_sync_qd1"
        } else {
            "server_async_concurrent"
        },
        "payload_size": cfg.payload_size,
        "target_bytes": cfg.target_bytes,
        "target_kind": "logical_payload",
        "keys_written": result.keys_written,
        "logical_bytes": result.logical_bytes,
        "bytes_on_disk": result.bytes_on_disk,
        "elapsed_ms": result.elapsed_ms,
        "ops_per_sec": result.ops_per_sec,
        "mb_per_sec": result.mb_per_sec_logical,
        "mb_per_sec_disk": result.mb_per_sec_disk,
        "put_batch_size": cfg.mode.batch_size(),
        "peak_rss_bytes": result.peak_rss_bytes,
        "peak_cpu_pct": result.peak_cpu_pct,
        "process_sample_count": result.sample_count,
        "work": cfg.work.display().to_string(),
        "store_or_db": result.path,
        "sqlite_journal": if cfg.engine == PeerEngine::Sqlite { "WAL" } else { "" },
        "sqlite_synchronous": if cfg.engine == PeerEngine::Sqlite { "NORMAL" } else { "" },
        "residiuum_durability": if cfg.engine == PeerEngine::Residiuum { "buffered" } else { "" },
        "disclosure": DISCLOSURE,
        "contract": "doc/wip/status/surveys/README-PEER-SQL.md",
    });

    if cfg.json_out {
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
    } else {
        println!(
            "peer-pump done: engine={} mode={} keys={} logical={} disk={} in {:.2}s — {:.1} ops/s, {:.2} logical MiB/s",
            cfg.engine.label(),
            cfg.mode.label(),
            result.keys_written,
            format_bytes(result.logical_bytes),
            format_bytes(result.bytes_on_disk),
            result.elapsed_ms as f64 / 1000.0,
            result.ops_per_sec,
            result.mb_per_sec_logical
        );
        if let Some(rss) = result.peak_rss_bytes {
            println!(
                "  peak_rss={}  peak_cpu%={}",
                format_bytes(rss),
                result
                    .peak_cpu_pct
                    .map(|c| format!("{c:.0}"))
                    .unwrap_or_else(|| "n/a".into())
            );
        }
        println!("  path={}", result.path);
        println!("  {DISCLOSURE}");
    }

    if !result.ok {
        return Err(format!(
            "peer-pump under logical target: {} < {}",
            format_bytes(result.logical_bytes),
            format_bytes(cfg.target_bytes)
        ));
    }
    Ok(())
}

struct PeerResult {
    ok: bool,
    keys_written: u64,
    logical_bytes: u64,
    bytes_on_disk: u64,
    elapsed_ms: u64,
    ops_per_sec: f64,
    mb_per_sec_logical: f64,
    mb_per_sec_disk: f64,
    peak_rss_bytes: Option<u64>,
    peak_cpu_pct: Option<f64>,
    sample_count: u64,
    path: String,
    awo_path: String,
    cook_parallelism: usize,
    /// Boundary probe: sum of FileWrite durations (data path), ms.
    data_write_sum_ms: f64,
    data_write_samples: u64,
    /// Boundary probe: sum of PublishVisibility (dual-index), ms.
    index_publish_sum_ms: f64,
    index_publish_samples: u64,
    /// Boundary probe: sum of PutPost (collection/derived), ms.
    index_post_sum_ms: f64,
    index_post_samples: u64,
    encode_sum_ms: f64,
    append_sum_ms: f64,
    prep_sum_ms: f64,
}

fn apply_diag_io(store: &mut Store, diag: PeerDiagIo) -> Result<(), String> {
    if diag == PeerDiagIo::Real {
        return Ok(());
    }
    store
        .set_diagnostic_io_sink(diag.to_sink())
        .map_err(|e| format!("set_diagnostic_io_sink: {e}"))
}

fn apply_diag_timing(store: &mut Store, cfg: &PeerConfig) {
    if cfg.diag_skip_index {
        store.set_diagnostic_skip_index(true);
    }
    if cfg.boundary_probe {
        store.enable_boundary_probe();
    }
}

type BoundaryTiming = (f64, u64, f64, u64, f64, u64, f64, f64, f64);

fn empty_boundary_timing() -> BoundaryTiming {
    (0.0, 0, 0.0, 0, 0.0, 0, 0.0, 0.0, 0.0)
}

fn boundary_timing_from_store(store: &Store, cfg: &PeerConfig) -> BoundaryTiming {
    if !cfg.boundary_probe {
        return empty_boundary_timing();
    }
    let snap = store.boundary_snapshot();
    let ns_ms = |sum_ns: u128| sum_ns as f64 / 1e6;
    (
        ns_ms(snap.write_latency.sum_ns),
        snap.write_latency.samples,
        ns_ms(snap.publish_latency.sum_ns),
        snap.publish_latency.samples,
        ns_ms(snap.post_latency.sum_ns),
        snap.post_latency.samples,
        ns_ms(snap.encode_latency.sum_ns),
        ns_ms(snap.append_latency.sum_ns),
        ns_ms(snap.prep_latency.sum_ns),
    )
}

fn apply_timing_tuple(result: &mut PeerResult, timing: BoundaryTiming) {
    let (
        data_write_sum_ms,
        data_write_samples,
        index_publish_sum_ms,
        index_publish_samples,
        index_post_sum_ms,
        index_post_samples,
        encode_sum_ms,
        append_sum_ms,
        prep_sum_ms,
    ) = timing;
    result.data_write_sum_ms = data_write_sum_ms;
    result.data_write_samples = data_write_samples;
    result.index_publish_sum_ms = index_publish_sum_ms;
    result.index_publish_samples = index_publish_samples;
    result.index_post_sum_ms = index_post_sum_ms;
    result.index_post_samples = index_post_samples;
    result.encode_sum_ms = encode_sum_ms;
    result.append_sum_ms = append_sum_ms;
    result.prep_sum_ms = prep_sum_ms;
}

fn apply_segment_growth(store: &mut Store, cfg: &PeerConfig) -> Result<(), String> {
    if cfg.segment_growth == PeerSegmentGrowth::GrowOnAppend {
        if cfg.wm_capacity_mib.is_some() || cfg.wm_chunk_mib.is_some() {
            return Err(
                "--wm-capacity-mib / --wm-chunk-mib require --segment-growth watermark".into(),
            );
        }
        return Ok(());
    }
    let policy = segment_growth_policy(cfg.segment_growth, cfg.wm_capacity_mib, cfg.wm_chunk_mib)?;
    store
        .set_segment_growth_policy(policy)
        .map_err(|e| format!("set_segment_growth_policy: {e}"))?;
    // Pay first-touch off the put timer: same-fd warm of reserved capacity
    // before odometer start (product shape; not put-path zeroing).
    store
        .warm_segment_runway()
        .map_err(|e| format!("warm_segment_runway: {e}"))?;
    Ok(())
}

fn attach_awo(store: &mut Store, mode: PeerAwoMode) -> Result<Option<AdaptiveWriteHandle>, String> {
    if !mode.lease_active() {
        return Ok(None);
    }
    let mut policy = AdaptiveWritePolicy::machine_defaults();
    policy.mode = match mode {
        PeerAwoMode::Static => AdaptiveWriteMode::Static,
        PeerAwoMode::Adaptive => AdaptiveWriteMode::Adaptive,
        PeerAwoMode::Disabled => unreachable!("lease_active false"),
    };
    policy.maximum_cookers = policy.maximum_cookers.min(4).max(1);
    policy.minimum_active_cookers = policy.minimum_active_cookers.min(policy.maximum_cookers);
    AdaptiveWriteHandle::start_static(policy, store)
        .map(Some)
        .map_err(|e| format!("awo attach: {}", e.as_str()))
}

fn pump_residiuum(cfg: &PeerConfig, payload: &[u8]) -> Result<PeerResult, String> {
    if cfg.concurrency > 1 {
        return pump_residiuum_concurrent_mode_a(cfg, payload);
    }
    let store_path = cfg.work.join("store");
    if store_path.exists() {
        fs::remove_dir_all(&store_path)
            .map_err(|e| format!("remove prior residiuum store: {e}"))?;
    }
    let mut store =
        Store::create_with_shards(&store_path, 1).map_err(|e| format!("create store: {e}"))?;
    let seal = if cfg.seal_threshold == 0 {
        64 * 1024 * 1024
    } else {
        cfg.seal_threshold
    };
    store.set_seal_threshold(seal);
    apply_diag_io(&mut store, cfg.diag_io)?;
    apply_segment_growth(&mut store, cfg)?;
    apply_diag_timing(&mut store, cfg);
    // Optional: RESIDIUUM_COOK_PARALLELISM=N for put_many multi-core cook (peer).
    // Engages only when a presented batch has ≥2 items (Mode B); Mode A batch=1 stays serial.
    let mut cook_parallelism = 1usize;
    if let Ok(raw) = std::env::var("RESIDIUUM_COOK_PARALLELISM") {
        if let Ok(n) = raw.parse::<usize>() {
            if n >= 1 {
                store.set_cook_parallelism(n);
                cook_parallelism = store.cook_parallelism();
            }
        }
    }

    let awo = attach_awo(&mut store, cfg.awo_mode)?;
    let batch = cfg.mode.batch_size();
    // Mode A (batch=1) + lease: independent admit_put + collector (QD=1 wait).
    // Mode B (batch>1) + lease: admit_put_batch of presented size.
    let use_independent = awo.is_some() && batch == 1;

    let mut timing = empty_boundary_timing();
    let (keys_written, elapsed, samples, awo_path) = if use_independent {
        let handle = awo.as_ref().expect("awo");
        let store_arc = Arc::new(Mutex::new(store));
        handle.bind_physical(Arc::clone(&store_arc));
        let mut samples = ProcessSamples::default();
        let t0 = Instant::now();
        let mut keys_written = 0u64;
        let mut last_report = Instant::now();
        while keys_written.saturating_mul(cfg.payload_size as u64) < cfg.target_bytes {
            let key = format!("peer/{:020}", keys_written);
            {
                let mut g = store_arc
                    .lock()
                    .map_err(|_| "store lock poisoned during admit".to_string())?;
                match handle.admit_put(
                    &mut g,
                    key.as_bytes(),
                    payload,
                    DurabilityMode::Buffered,
                    WriteCondition::Unconditional,
                ) {
                    AdmissionResult::Admitted(c) => {
                        drop(g);
                        c.wait()
                            .map_err(|e| format!("admit_put wait: {}", e.as_str()))?;
                    }
                    AdmissionResult::Rejected(e) => {
                        return Err(format!("admit_put rejected: {}", e.as_str()));
                    }
                }
            }
            keys_written += 1;
            if last_report.elapsed().as_secs() >= 2 || keys_written == 1 {
                sample_process(&mut samples);
                if !cfg.json_out {
                    let logical = keys_written.saturating_mul(cfg.payload_size as u64);
                    let elapsed = t0.elapsed().as_secs_f64().max(1e-9);
                    eprintln!(
                        "peer-pump residiuum: keys={keys_written} logical≈{} / {} {:.1} ops/s awo={}",
                        format_bytes(logical),
                        format_bytes(cfg.target_bytes),
                        keys_written as f64 / elapsed,
                        cfg.awo_mode.label()
                    );
                }
                last_report = Instant::now();
            }
            if keys_written >= 50_000_000 {
                return Err("peer-pump abort: 50M keys without reaching logical target".into());
            }
        }
        sample_process(&mut samples);
        {
            let mut g = store_arc
                .lock()
                .map_err(|_| "store lock poisoned at detach".to_string())?;
            let _ = handle.drain_writes(Instant::now() + Duration::from_secs(2));
            handle.detach(&mut g);
            timing = boundary_timing_from_store(&g, cfg);
            let _ = g.seal_active();
        }
        handle.join_after_detach();
        let store = Arc::try_unwrap(store_arc)
            .map_err(|_| "store Arc still shared after peer-pump".to_string())?
            .into_inner()
            .map_err(|_| "store mutex poisoned extract".to_string())?;
        drop(store);
        (
            keys_written,
            t0.elapsed(),
            samples,
            "independent_admit_put+collection".to_string(),
        )
    } else {
        let mut samples = ProcessSamples::default();
        let t0 = Instant::now();
        let mut keys_written = 0u64;
        let mut pending: Vec<String> = Vec::with_capacity(batch);
        let mut last_report = Instant::now();
        while keys_written.saturating_mul(cfg.payload_size as u64) < cfg.target_bytes {
            let key = format!("peer/{:020}", keys_written);
            pending.push(key);
            keys_written += 1;
            if pending.len() >= batch {
                flush_residiuum(&mut store, &pending, payload, awo.as_ref())?;
                pending.clear();
            }
            if last_report.elapsed().as_secs() >= 2 || keys_written == 1 {
                sample_process(&mut samples);
                if !cfg.json_out {
                    let logical = keys_written.saturating_mul(cfg.payload_size as u64);
                    let elapsed = t0.elapsed().as_secs_f64().max(1e-9);
                    eprintln!(
                        "peer-pump residiuum: keys={keys_written} logical≈{} / {} {:.1} ops/s awo={}",
                        format_bytes(logical),
                        format_bytes(cfg.target_bytes),
                        keys_written as f64 / elapsed,
                        cfg.awo_mode.label()
                    );
                }
                last_report = Instant::now();
            }
            if keys_written >= 50_000_000 {
                return Err("peer-pump abort: 50M keys without reaching logical target".into());
            }
        }
        if !pending.is_empty() {
            flush_residiuum(&mut store, &pending, payload, awo.as_ref())?;
            pending.clear();
        }
        sample_process(&mut samples);
        let awo_path = if let Some(h) = awo.as_ref() {
            let _ = h.drain_writes(Instant::now() + Duration::from_secs(2));
            h.detach(&mut store);
            h.join_after_detach();
            "admit_put_batch".to_string()
        } else {
            "put_many".to_string()
        };
        timing = boundary_timing_from_store(&store, cfg);
        let _ = store.seal_active();
        drop(store);
        (keys_written, t0.elapsed(), samples, awo_path)
    };

    let mut result = finish_result(cfg, keys_written, &store_path, elapsed, &samples)?;
    result.awo_path = awo_path;
    result.cook_parallelism = cook_parallelism;
    apply_timing_tuple(&mut result, timing);
    Ok(result)
}

fn flush_residiuum(
    store: &mut Store,
    keys: &[String],
    payload: &[u8],
    awo: Option<&AdaptiveWriteHandle>,
) -> Result<(), String> {
    if keys.is_empty() {
        return Ok(());
    }
    let items: Vec<(&str, &[u8])> = keys.iter().map(|k| (k.as_str(), payload)).collect();
    match awo {
        None => store
            .put_many(&items, DurabilityMode::Buffered)
            .map_err(|e| format!("put_many ({} keys): {e}", keys.len()))
            .map(|_| ()),
        Some(h) => h
            .admit_put_batch(store, &items, DurabilityMode::Buffered)
            .map_err(|e| format!("admit_put_batch ({} keys): {}", keys.len(), e.as_str()))
            .map(|_| ()),
    }
}

/// Server-async Mode A: N client threads, each per-thread QD=1.
/// Overlap = concurrency in flight — AWO collector can microbatch.
fn pump_residiuum_concurrent_mode_a(
    cfg: &PeerConfig,
    payload: &[u8],
) -> Result<PeerResult, String> {
    let store_path = cfg.work.join("store");
    if store_path.exists() {
        fs::remove_dir_all(&store_path)
            .map_err(|e| format!("remove prior residiuum store: {e}"))?;
    }
    let mut store =
        Store::create_with_shards(&store_path, 1).map_err(|e| format!("create store: {e}"))?;
    let seal = if cfg.seal_threshold == 0 {
        64 * 1024 * 1024
    } else {
        cfg.seal_threshold
    };
    store.set_seal_threshold(seal);
    apply_diag_io(&mut store, cfg.diag_io)?;
    apply_segment_growth(&mut store, cfg)?;
    apply_diag_timing(&mut store, cfg);
    let mut cook_parallelism = 1usize;
    if let Ok(raw) = std::env::var("RESIDIUUM_COOK_PARALLELISM") {
        if let Ok(n) = raw.parse::<usize>() {
            if n >= 1 {
                store.set_cook_parallelism(n);
                cook_parallelism = store.cook_parallelism();
            }
        }
    }

    let target_keys = cfg.target_bytes.div_ceil(cfg.payload_size as u64).max(1);
    let awo = attach_awo(&mut store, cfg.awo_mode)?;
    let store_arc = Arc::new(Mutex::new(store));
    if let Some(ref h) = awo {
        h.bind_physical(Arc::clone(&store_arc));
    }
    let next = Arc::new(AtomicU64::new(0));
    let err: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let workers = cfg.concurrency.max(1);
    let payload = Arc::new(payload.to_vec());
    let mut samples = ProcessSamples::default();
    let t0 = Instant::now();

    thread::scope(|scope| {
        for _ in 0..workers {
            let store_arc = Arc::clone(&store_arc);
            let next = Arc::clone(&next);
            let err = Arc::clone(&err);
            let payload = Arc::clone(&payload);
            let awo = awo.clone();
            scope.spawn(move || loop {
                if err.lock().ok().and_then(|g| g.clone()).is_some() {
                    break;
                }
                let seq = next.fetch_add(1, Ordering::Relaxed);
                if seq >= target_keys {
                    break;
                }
                let key = format!("peer/{:020}", seq);
                let put_err = if let Some(ref handle) = awo {
                    let admitted = {
                        let mut g = match store_arc.lock() {
                            Ok(g) => g,
                            Err(_) => {
                                let _ = err.lock().map(|mut e| {
                                    *e = Some("store lock poisoned during admit".into());
                                });
                                break;
                            }
                        };
                        handle.admit_put(
                            &mut g,
                            key.as_bytes(),
                            payload.as_slice(),
                            DurabilityMode::Buffered,
                            WriteCondition::Unconditional,
                        )
                    };
                    match admitted {
                        AdmissionResult::Admitted(c) => c
                            .wait()
                            .map(|_| ())
                            .map_err(|e| format!("admit_put wait: {}", e.as_str())),
                        AdmissionResult::Rejected(e) => {
                            Err(format!("admit_put rejected: {}", e.as_str()))
                        }
                    }
                } else {
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
            });
        }
    });

    if let Some(e) = err.lock().ok().and_then(|g| g.clone()) {
        return Err(e);
    }
    let keys_written = target_keys.min(next.load(Ordering::Relaxed));
    sample_process(&mut samples);
    let mut timing = empty_boundary_timing();
    let awo_path = if let Some(ref h) = awo {
        {
            let mut g = store_arc
                .lock()
                .map_err(|_| "store lock poisoned at detach".to_string())?;
            let _ = h.drain_writes(Instant::now() + Duration::from_secs(2));
            h.detach(&mut g);
            timing = boundary_timing_from_store(&g, cfg);
            let _ = g.seal_active();
        }
        h.join_after_detach();
        format!("concurrent_admit_put+collection workers={}", workers)
    } else {
        {
            let mut g = store_arc
                .lock()
                .map_err(|_| "store lock poisoned at seal".to_string())?;
            timing = boundary_timing_from_store(&g, cfg);
            let _ = g.seal_active();
        }
        format!("concurrent_put_many workers={}", workers)
    };
    drop(awo);
    let store = Arc::try_unwrap(store_arc)
        .map_err(|_| "store Arc still shared after concurrent peer-pump".to_string())?
        .into_inner()
        .map_err(|_| "store mutex poisoned extract".to_string())?;
    drop(store);

    let mut result = finish_result(cfg, keys_written, &store_path, t0.elapsed(), &samples)?;
    result.awo_path = awo_path;
    result.cook_parallelism = cook_parallelism;
    apply_timing_tuple(&mut result, timing);
    Ok(result)
}

fn pump_sqlite(cfg: &PeerConfig, payload: &[u8]) -> Result<PeerResult, String> {
    if cfg.concurrency > 1 {
        return pump_sqlite_concurrent_mode_a(cfg, payload);
    }
    let db_path = cfg.work.join("peer.sqlite");
    if db_path.exists() {
        fs::remove_file(&db_path).map_err(|e| format!("remove prior sqlite: {e}"))?;
    }
    // Remove WAL/SHM leftovers if any.
    let _ = fs::remove_file(cfg.work.join("peer.sqlite-wal"));
    let _ = fs::remove_file(cfg.work.join("peer.sqlite-shm"));

    let conn = Connection::open(&db_path).map_err(|e| format!("sqlite open: {e}"))?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         CREATE TABLE kv (
           k TEXT PRIMARY KEY NOT NULL,
           v BLOB NOT NULL
         );",
    )
    .map_err(|e| format!("sqlite setup: {e}"))?;

    let batch = cfg.mode.batch_size();
    let mut samples = ProcessSamples::default();
    let t0 = Instant::now();
    let mut keys_written = 0u64;
    let mut last_report = Instant::now();

    let insert_sql = "INSERT INTO kv(k, v) VALUES (?1, ?2)";

    while keys_written.saturating_mul(cfg.payload_size as u64) < cfg.target_bytes {
        match cfg.mode {
            PeerMode::A => {
                let key = format!("peer/{:020}", keys_written);
                conn.execute(insert_sql, rusqlite::params![key, payload])
                    .map_err(|e| format!("sqlite insert: {e}"))?;
                keys_written += 1;
            }
            PeerMode::B => {
                let tx = conn
                    .unchecked_transaction()
                    .map_err(|e| format!("sqlite begin: {e}"))?;
                let mut stmt = tx
                    .prepare_cached(insert_sql)
                    .map_err(|e| format!("sqlite prepare: {e}"))?;
                let mut n = 0usize;
                while n < batch
                    && keys_written.saturating_mul(cfg.payload_size as u64) < cfg.target_bytes
                {
                    let key = format!("peer/{:020}", keys_written);
                    stmt.execute(rusqlite::params![key, payload])
                        .map_err(|e| format!("sqlite insert: {e}"))?;
                    keys_written += 1;
                    n += 1;
                }
                drop(stmt);
                tx.commit().map_err(|e| format!("sqlite commit: {e}"))?;
            }
        }

        if last_report.elapsed().as_secs() >= 2 || keys_written == 1 {
            sample_process(&mut samples);
            if !cfg.json_out {
                let logical = keys_written.saturating_mul(cfg.payload_size as u64);
                let elapsed = t0.elapsed().as_secs_f64().max(1e-9);
                eprintln!(
                    "peer-pump sqlite: keys={keys_written} logical≈{} / {} {:.1} ops/s",
                    format_bytes(logical),
                    format_bytes(cfg.target_bytes),
                    keys_written as f64 / elapsed
                );
            }
            last_report = Instant::now();
        }
        if keys_written >= 50_000_000 {
            return Err("peer-pump abort: 50M keys without reaching logical target".into());
        }
    }

    // Checkpoint WAL so on-disk size is meaningful for diagnostics.
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .map_err(|e| format!("sqlite checkpoint: {e}"))?;
    // Confirm row count roughly matches.
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM kv", [], |r| r.get(0))
        .map_err(|e| format!("sqlite count: {e}"))?;
    if count as u64 != keys_written {
        return Err(format!(
            "sqlite row count mismatch: table={count} keys_written={keys_written}"
        ));
    }
    drop(conn);

    let elapsed = t0.elapsed();
    sample_process(&mut samples);
    finish_result(cfg, keys_written, &db_path, elapsed, &samples)
}

/// N SQLite connections (WAL), Mode A autocommit — concurrent client feed peer.
fn pump_sqlite_concurrent_mode_a(cfg: &PeerConfig, payload: &[u8]) -> Result<PeerResult, String> {
    let db_path = cfg.work.join("peer.sqlite");
    if db_path.exists() {
        fs::remove_file(&db_path).map_err(|e| format!("remove prior sqlite: {e}"))?;
    }
    let _ = fs::remove_file(cfg.work.join("peer.sqlite-wal"));
    let _ = fs::remove_file(cfg.work.join("peer.sqlite-shm"));

    {
        let conn = Connection::open(&db_path).map_err(|e| format!("sqlite open: {e}"))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE kv (
               k TEXT PRIMARY KEY NOT NULL,
               v BLOB NOT NULL
             );",
        )
        .map_err(|e| format!("sqlite setup: {e}"))?;
    }

    let target_keys = cfg.target_bytes.div_ceil(cfg.payload_size as u64).max(1);
    let next = Arc::new(AtomicU64::new(0));
    let err: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let workers = cfg.concurrency.max(1);
    let payload = Arc::new(payload.to_vec());
    let db_path_arc = Arc::new(db_path.clone());
    let mut samples = ProcessSamples::default();
    let t0 = Instant::now();

    thread::scope(|scope| {
        for _ in 0..workers {
            let next = Arc::clone(&next);
            let err = Arc::clone(&err);
            let payload = Arc::clone(&payload);
            let db_path = Arc::clone(&db_path_arc);
            scope.spawn(move || {
                let conn = match Connection::open(db_path.as_path()) {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = err.lock().map(|mut slot| {
                            *slot = Some(format!("sqlite open worker: {e}"));
                        });
                        return;
                    }
                };
                let _ = conn.execute_batch("PRAGMA synchronous=NORMAL;");
                let insert_sql = "INSERT INTO kv(k, v) VALUES (?1, ?2)";
                loop {
                    if err.lock().ok().and_then(|g| g.clone()).is_some() {
                        break;
                    }
                    let seq = next.fetch_add(1, Ordering::Relaxed);
                    if seq >= target_keys {
                        break;
                    }
                    let key = format!("peer/{:020}", seq);
                    if let Err(e) =
                        conn.execute(insert_sql, rusqlite::params![key, payload.as_slice()])
                    {
                        let _ = err.lock().map(|mut slot| {
                            if slot.is_none() {
                                *slot = Some(format!("sqlite insert: {e}"));
                            }
                        });
                        break;
                    }
                }
            });
        }
    });

    if let Some(e) = err.lock().ok().and_then(|g| g.clone()) {
        return Err(e);
    }
    let keys_written = target_keys.min(next.load(Ordering::Relaxed));
    {
        let conn = Connection::open(&db_path).map_err(|e| format!("sqlite reopen: {e}"))?;
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(|e| format!("sqlite checkpoint: {e}"))?;
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM kv", [], |r| r.get(0))
            .map_err(|e| format!("sqlite count: {e}"))?;
        if count as u64 != keys_written {
            return Err(format!(
                "sqlite row count mismatch: table={count} keys_written={keys_written}"
            ));
        }
    }
    sample_process(&mut samples);
    finish_result(cfg, keys_written, &db_path, t0.elapsed(), &samples)
}

fn finish_result(
    cfg: &PeerConfig,
    keys_written: u64,
    path: &Path,
    elapsed: std::time::Duration,
    samples: &ProcessSamples,
) -> Result<PeerResult, String> {
    let logical_bytes = keys_written.saturating_mul(cfg.payload_size as u64);
    let bytes_on_disk = if path.is_dir() {
        dir_size_bytes(path).map_err(|e| format!("dir size: {e}"))?
    } else {
        // Include WAL/SHM if present next to the db.
        let mut total = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        for suffix in ["-wal", "-shm"] {
            let p = PathBuf::from(format!("{}{suffix}", path.display()));
            if let Ok(m) = fs::metadata(&p) {
                total = total.saturating_add(m.len());
            }
        }
        total
    };
    let secs = elapsed.as_secs_f64().max(1e-9);
    let ops_per_sec = keys_written as f64 / secs;
    let mb_per_sec_logical = (logical_bytes as f64 / (1024.0 * 1024.0)) / secs;
    let mb_per_sec_disk = (bytes_on_disk as f64 / (1024.0 * 1024.0)) / secs;
    Ok(PeerResult {
        ok: logical_bytes >= cfg.target_bytes,
        keys_written,
        logical_bytes,
        bytes_on_disk,
        elapsed_ms: elapsed.as_millis() as u64,
        ops_per_sec,
        mb_per_sec_logical,
        mb_per_sec_disk,
        peak_rss_bytes: samples.peak_rss_bytes,
        peak_cpu_pct: samples.peak_cpu_pct,
        sample_count: samples.sample_count,
        path: path.display().to_string(),
        awo_path: String::new(),
        cook_parallelism: 0,
        data_write_sum_ms: 0.0,
        data_write_samples: 0,
        index_publish_sum_ms: 0.0,
        index_publish_samples: 0,
        index_post_sum_ms: 0.0,
        index_post_samples: 0,
        encode_sum_ms: 0.0,
        append_sum_ms: 0.0,
        prep_sum_ms: 0.0,
    })
}

fn fill_payload(buf: &mut [u8], seed: u64) {
    let mut state = seed ^ 0xD1_160_B17_u64;
    for (i, b) in buf.iter_mut().enumerate() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        *b = ((state >> 33) as u8).wrapping_add((i & 0xff) as u8);
    }
    let magic = b"RESIDIUUM-PEER-SQL-PAYLOAD-v1\n";
    let n = magic.len().min(buf.len());
    buf[..n].copy_from_slice(&magic[..n]);
}

fn sample_process(samples: &mut ProcessSamples) {
    let Some((cpu, rss_kib)) = read_self_ps() else {
        return;
    };
    samples.sample_count += 1;
    samples.last_cpu_pct = Some(cpu);
    samples.peak_cpu_pct = Some(samples.peak_cpu_pct.map(|p| p.max(cpu)).unwrap_or(cpu));
    let rss_bytes = rss_kib.saturating_mul(1024);
    samples.peak_rss_bytes = Some(
        samples
            .peak_rss_bytes
            .map(|p| p.max(rss_bytes))
            .unwrap_or(rss_bytes),
    );
}

fn read_self_ps() -> Option<(f64, u64)> {
    let pid = std::process::id().to_string();
    let output = Command::new("ps")
        .args(["-o", "%cpu=,rss=", "-p", &pid])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let parts: Vec<&str> = text.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }
    let cpu: f64 = parts[0].parse().ok()?;
    let rss_kib: u64 = parts[1].parse().ok()?;
    Some((cpu, rss_kib))
}
