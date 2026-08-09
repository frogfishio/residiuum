//! Background seal / checkpoint pipeline (DEF-096 Axis A + Zero-Scan Auth Seal).
//!
//! Foreground put path prefers **zero-scan rotate**: incremental BLAKE3 + summary
//! already computed while appending; rename active → pending, open next active,
//! append precomputed summary, rename pending → sealed, apply compact catalog
//! metadata (no 64 MiB `Vec` on the writer path).
//!
//! Fallback / recovery still uses the worker finalize path:
//!
//! 1. **Authoritative seal** — summary append, publish into `segments/`, BLAKE3.
//!    Posts [`LifecycleResult::SealDone`] with compact summary metadata.
//! 2. **Derived enrichment** — Hydra / Chimera (rebuildable). Runs with an I/O
//!    gap so it never competes without limit against foreground ingestion.
//!
//! Derived index checkpoints can also run on the worker so `persist_index_cache`
//! fsyncs leave the put acknowledgement path.
//!
//! Correctness:
//! - Pending files are authoritative until sealed (included in open recovery).
//! - Frame offsets are preserved (`ActiveSegment::resume_unsealed` + seal).
//! - Bounded inflight seals apply **only** to authoritative finalize lag.
//! - Explicit [`crate::Store::seal_active`] still runs the synchronous path and
//!   drains the pipeline first (tests / failpoints).

use crate::error::StoreError;
use crate::hydra::{
    hydra_index_path, records_from_segment_bytes, write_hydra_index, HydraBuildOptions,
};
use crate::incremental_seal::{
    meta_publish_plan, ContentHashState, IncrementalSealState, SealPublishPlan,
};
use crate::index::PrimaryIndex;
use crate::index_cache::{write_primary_index_frontier, ChunkLocatorMap, IndexFrontier};
use crate::layout::{list_residiuum_files, segment_id_from_filename, StorePaths};
use residiuum_format::{
    decode_descriptor_body, scan_forward, ActiveSegment, FrameKind, SafetyLimits, SegmentId,
};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

fn elapsed_ns(t0: Instant) -> u64 {
    t0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Per-segment derived enrichment stage timings (ETQ-0 measurement).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EnrichmentStageTiming {
    /// Foreground-priority wait before this job (resource isolation).
    pub gap_wait_ns: u64,
    /// `fs::read` of the sealed segment.
    pub read_ns: u64,
    /// Frame/record decode (Hydra `records_from_segment_bytes` + Chimera scan).
    pub decode_ns: u64,
    /// Whole-segment BLAKE3 over sealed bytes.
    pub blake3_ns: u64,
    /// Hydra index build (in memory).
    pub hydra_construct_ns: u64,
    /// Hydra index durable write.
    pub hydra_persist_ns: u64,
    /// Chimera layout build (in memory).
    pub chimera_construct_ns: u64,
    /// Chimera layout durable write.
    pub chimera_persist_ns: u64,
    /// Writer-side catalog digest refresh on EnrichDone apply.
    pub catalog_ns: u64,
    /// Enrich-worker wall for the whole job (includes gap).
    pub wall_ns: u64,
    /// Thread CPU time for the enrich worker job (0 if unavailable).
    pub cpu_ns: u64,
    /// Sealed segment bytes read.
    pub bytes_read: u64,
    /// Hydra + Chimera bytes written.
    pub bytes_written: u64,
}

impl EnrichmentStageTiming {
    /// Hydra construct + persist.
    pub fn hydra_ns(self) -> u64 {
        self.hydra_construct_ns
            .saturating_add(self.hydra_persist_ns)
    }

    /// Chimera construct + persist.
    pub fn chimera_ns(self) -> u64 {
        self.chimera_construct_ns
            .saturating_add(self.chimera_persist_ns)
    }

    /// Read + decode (requested ETQ-0 aggregate).
    pub fn read_decode_ns(self) -> u64 {
        self.read_ns.saturating_add(self.decode_ns)
    }

    /// Worker service excluding isolation gap.
    pub fn service_ns_excluding_gap(self) -> u64 {
        self.wall_ns.saturating_sub(self.gap_wait_ns)
    }
}

/// Cumulative enrichment stage timings (sums + sample count).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EnrichmentStageTotals {
    /// Number of EnrichDone samples accumulated.
    pub samples: u64,
    /// Sum of isolation-gap waits.
    pub gap_wait_ns: u64,
    /// Sum of sealed-segment read times.
    pub read_ns: u64,
    /// Sum of Hydra+Chimera decode times.
    pub decode_ns: u64,
    /// Sum of BLAKE3 times.
    pub blake3_ns: u64,
    /// Sum of Hydra build times.
    pub hydra_construct_ns: u64,
    /// Sum of Hydra persist times.
    pub hydra_persist_ns: u64,
    /// Sum of Chimera build times.
    pub chimera_construct_ns: u64,
    /// Sum of Chimera persist times.
    pub chimera_persist_ns: u64,
    /// Sum of writer catalog-refresh times.
    pub catalog_ns: u64,
    /// Sum of enrich-worker wall times (includes gap).
    pub wall_ns: u64,
    /// Sum of enrich-worker thread CPU times.
    pub cpu_ns: u64,
    /// Sum of sealed bytes read.
    pub bytes_read: u64,
    /// Sum of Hydra+Chimera bytes written.
    pub bytes_written: u64,
}

impl EnrichmentStageTotals {
    /// Add one per-segment sample into the cumulative totals.
    pub fn accumulate(&mut self, s: EnrichmentStageTiming) {
        self.samples = self.samples.saturating_add(1);
        self.gap_wait_ns = self.gap_wait_ns.saturating_add(s.gap_wait_ns);
        self.read_ns = self.read_ns.saturating_add(s.read_ns);
        self.decode_ns = self.decode_ns.saturating_add(s.decode_ns);
        self.blake3_ns = self.blake3_ns.saturating_add(s.blake3_ns);
        self.hydra_construct_ns = self.hydra_construct_ns.saturating_add(s.hydra_construct_ns);
        self.hydra_persist_ns = self.hydra_persist_ns.saturating_add(s.hydra_persist_ns);
        self.chimera_construct_ns = self
            .chimera_construct_ns
            .saturating_add(s.chimera_construct_ns);
        self.chimera_persist_ns = self.chimera_persist_ns.saturating_add(s.chimera_persist_ns);
        self.catalog_ns = self.catalog_ns.saturating_add(s.catalog_ns);
        self.wall_ns = self.wall_ns.saturating_add(s.wall_ns);
        self.cpu_ns = self.cpu_ns.saturating_add(s.cpu_ns);
        self.bytes_read = self.bytes_read.saturating_add(s.bytes_read);
        self.bytes_written = self.bytes_written.saturating_add(s.bytes_written);
    }

    /// Mean of a summed field across [`Self::samples`] (0 if empty).
    pub fn mean_ns(&self, sum: u64) -> f64 {
        if self.samples == 0 {
            return 0.0;
        }
        sum as f64 / self.samples as f64
    }
}

/// Thread CPU nanoseconds via `CLOCK_THREAD_CPUTIME_ID` (Unix). Measurement only.
fn thread_cpu_ns() -> u64 {
    #[cfg(unix)]
    {
        #[repr(C)]
        struct Timespec {
            tv_sec: i64,
            tv_nsec: i64,
        }
        extern "C" {
            fn clock_gettime(clk_id: i32, tp: *mut Timespec) -> i32;
        }
        // Linux + Darwin: CLOCK_THREAD_CPUTIME_ID == 16.
        const CLOCK_THREAD_CPUTIME_ID: i32 = 16;
        let mut ts = Timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        if unsafe { clock_gettime(CLOCK_THREAD_CPUTIME_ID, &mut ts) } == 0 {
            return (ts.tv_sec as u64)
                .saturating_mul(1_000_000_000)
                .saturating_add(ts.tv_nsec.max(0) as u64);
        }
    }
    0
}

/// Default bound on **authoritative** seals in flight.
///
/// Historically 2 when finalize included Chimera (derived work held the lane).
/// Seal Fast Lane keeps derived on a separate worker, so this bound only limits
/// rename/publish lag. Keep it high enough that writers are not stalled by
/// normal 64 MiB rotations; derived enrichment never counts toward it.
pub const DEFAULT_MAX_PENDING_SEALS: usize = 16;

/// Required foreground-write quiet period before derived enrichment starts.
pub const ENRICHMENT_QUIET_PERIOD: Duration = Duration::from_millis(250);

/// Maximum time one derived job may defer under uninterrupted ingestion.
///
/// This permits bounded index progress for indefinitely busy deployments while
/// ensuring enrichment cannot continuously compete with authoritative writes.
pub const ENRICHMENT_MAX_DEFERRAL: Duration = Duration::from_secs(2);

const ENRICHMENT_ACTIVITY_POLL: Duration = Duration::from_millis(10);

/// Job submitted to the lifecycle worker.
#[allow(private_interfaces)]
pub enum LifecycleJob {
    /// Finalize a rotated active segment sitting under `active/pending/`.
    FinalizeSeal {
        /// Store identity.
        store_id: [u8; 16],
        /// Segment id (filename stem).
        segment_id: [u8; 16],
        /// Path of the unsealed pending file.
        pending_path: PathBuf,
        /// Destination sealed path under `segments/`.
        sealed_path: PathBuf,
        /// Safety limits used while writing.
        limits: SafetyLimits,
        /// Store root paths (Hydra/Chimera layout).
        paths: StorePaths,
        /// When true, `sync_all` sealed image + parent dir (Durable ack path).
        /// When false, write+rename only (Buffered-only segments; CSQ-ACK-004).
        require_fsync: bool,
    },
    /// Zero-scan publish: append precomputed summary only (no pending re-read).
    FinalizeSealPlan {
        /// Segment id.
        segment_id: [u8; 16],
        /// Path of the unsealed pending file (durable prefix only).
        pending_path: PathBuf,
        /// Destination sealed path under `segments/`.
        sealed_path: PathBuf,
        /// Durable prefix length before summary.
        prefix_len: u64,
        /// Precomputed summary + content hash + catalog fields.
        plan: SealPublishPlan,
        /// When true, `sync_all` after summary append.
        require_fsync: bool,
    },
    /// Meta seal: append precomputed summary (no pending read / no BLAKE3).
    ///
    /// Whole-segment hash is derived and filled by [`LifecycleJob::EnrichDerived`].
    FinalizeSealMeta {
        /// Segment identity.
        ids: SegmentId,
        /// Segment id (filename stem).
        segment_id: [u8; 16],
        /// Durable prefix length on the pending file.
        prefix_len: u64,
        /// Frame count before summary.
        frame_count: u64,
        /// Writer sequence for the summary frame.
        writer_sequence: u64,
        /// Item-event frames in the prefix.
        item_events: u64,
        /// Path of the unsealed pending file.
        pending_path: PathBuf,
        /// Destination sealed path under `segments/`.
        sealed_path: PathBuf,
        /// When true, `sync_all` after summary append.
        require_fsync: bool,
    },
    /// Zero-read seal: hash a moved resident prefix (no pending re-read), append summary.
    FinalizeSealResident {
        /// Segment identity.
        ids: SegmentId,
        /// Segment id (filename stem).
        segment_id: [u8; 16],
        /// Full durable prefix bytes moved from the active segment (base_offset == 0).
        prefix: Vec<u8>,
        /// Frame count before summary.
        frame_count: u64,
        /// Writer sequence for the summary frame.
        writer_sequence: u64,
        /// Item-event frames in the prefix.
        item_events: u64,
        /// Path of the unsealed pending file.
        pending_path: PathBuf,
        /// Destination sealed path under `segments/`.
        sealed_path: PathBuf,
        /// When true, `sync_all` after summary append.
        require_fsync: bool,
    },
    /// Write a primary-index frontier checkpoint (derived only).
    Checkpoint {
        /// Destination cache path.
        cache_path: PathBuf,
        /// Store identity.
        store_id: [u8; 16],
        /// Frontier metadata.
        frontier: IndexFrontier,
        /// Durable index snapshot (locator-first).
        index: PrimaryIndex,
        /// Verified payload-chunk locators covered by the frontier.
        chunk_locators: ChunkLocatorMap,
    },
    /// Persist derived tier placement + segment catalog (best-effort; coalesce).
    ///
    /// Not authoritative — loss is recovered by `discover_placements` /
    /// `rebuild_segment_catalog` from sealed segment bytes.
    DerivedCatalogCheckpoint {
        /// Store identity.
        store_id: [u8; 16],
        /// Store root paths (catalogs + tier roots).
        paths: StorePaths,
        /// Snapshot of in-memory placement at submit time.
        placement: crate::tier::TierPlacement,
        /// Snapshot of in-memory segment catalog at submit time.
        segment_catalog: crate::segment_catalog::SegmentCatalog,
    },
    /// Build Hydra/Chimera for an already-published sealed segment (derived only).
    ///
    /// Does **not** count against `inflight_seals` / write-path backpressure.
    EnrichDerived {
        /// Store identity.
        store_id: [u8; 16],
        /// Sealed segment id.
        segment_id: [u8; 16],
        /// Store root paths.
        paths: StorePaths,
        /// Safety limits while scanning sealed bytes.
        limits: SafetyLimits,
    },
    /// Protected seal-pair: auth pending + prepared Shadow → durable publish + frontier.
    ///
    /// Counts against `inflight_seals`. `protected_frontier` advances only after
    /// both authoritative sealed file and `.rsh` are durable.
    FinalizeProtectedPair {
        /// Store identity.
        store_id: [u8; 16],
        /// Segment id.
        segment_id: [u8; 16],
        /// Writer shard (frontier coverage).
        shard: u16,
        /// Pending authoritative image (summary already appended).
        pending_path: PathBuf,
        /// Destination sealed path.
        sealed_path: PathBuf,
        /// Complete off-thread or write-time staging; needs sync + rename.
        prepared_shadow: crate::recovery_shadow::PreparedShadowPublish,
        /// Store paths for frontier / shadow dir.
        paths: StorePaths,
        /// When true, fsync auth sealed image + parent dirs.
        require_fsync: bool,
        /// Sealed byte length (known after summary append).
        size: u64,
        /// Compact catalog summary computed without rereading the segment.
        summary: crate::segment_catalog::SegmentSummary,
    },
    /// Stop the worker after draining queued jobs.
    Shutdown,
}

/// Result posted by the worker after a job completes.
#[derive(Debug)]
pub enum LifecycleResult {
    /// Seal finalized; sealed file is durable.
    ///
    /// Authoritative publication is `{segment_id, size}` (+ summary metadata).
    /// `content_hash` is often [`ContentHashState::Pending`] until enrichment.
    SealDone {
        /// Segment id.
        segment_id: [u8; 16],
        /// Derived whole-segment digest state.
        content_hash: ContentHashState,
        /// Sealed byte length.
        size: u64,
        /// Compact catalog summary (no sealed image buffer).
        summary: crate::segment_catalog::SegmentSummary,
        /// Nanoseconds spent appending summary + renaming into `segments/`.
        auth_publish_ns: u64,
    },
    /// Checkpoint written (or best-effort failed — see `ok`).
    CheckpointDone {
        /// Whether the write succeeded.
        ok: bool,
    },
    /// Seal finalize failed (pending file may remain for recovery).
    SealFailed {
        /// Segment id.
        segment_id: [u8; 16],
        /// Error text.
        error: String,
    },
    /// Derived enrichment finished (informational; never gates writes).
    EnrichDone {
        /// Segment id.
        segment_id: [u8; 16],
        /// Whether Hydra/Chimera writes succeeded.
        ok: bool,
        /// Derived whole-segment digest (Known after successful read/hash).
        content_hash: ContentHashState,
        /// Sealed byte length.
        size: u64,
        /// Per-job stage timings (ETQ-0). Absent on misrouted/failed early exits.
        stages: Option<EnrichmentStageTiming>,
    },
    /// Protected pair durable: auth + Shadow published; frontier advanced.
    ProtectedPairDone {
        /// Segment id.
        segment_id: [u8; 16],
        /// Sealed byte length.
        size: u64,
        /// Compact catalog summary computed before background publication.
        summary: crate::segment_catalog::SegmentSummary,
        /// Auth publish nanoseconds (rename/sync).
        auth_publish_ns: u64,
        /// Shadow durable publish nanoseconds.
        shadow_publish_ns: u64,
        /// Buffered Shadow staging writes completed for this segment.
        shadow_staging_write_operations: u64,
        /// Shadow staging bytes written for this segment.
        shadow_staging_write_bytes: u64,
        /// Wall time in Shadow staging writes.
        shadow_staging_write_ns: u64,
        /// Shadow file durability-barrier time.
        shadow_sync_ns: u64,
    },
}

/// Background lifecycle worker handle owned by a writer `Store`.
pub struct SealPipeline {
    /// Authoritative finalize + checkpoint jobs.
    job_tx: Sender<LifecycleJob>,
    /// Derived enrichment jobs (separate worker — never blocks seal lane).
    enrich_tx: Sender<LifecycleJob>,
    result_rx: Receiver<LifecycleResult>,
    join_seal: Option<JoinHandle<()>>,
    join_enrich: Option<JoinHandle<()>>,
    /// Authoritative seal jobs submitted and not yet applied via result_rx.
    pub inflight_seals: usize,
    /// Max authoritative seals allowed in flight before put backpressure.
    /// Derived enrichment never counts toward this bound.
    pub max_pending_seals: usize,
    /// EnrichDerived jobs queued/running that have not posted EnrichDone yet.
    pub enrichment_backlog: usize,
    /// Monotonic foreground durable-publication activity generation.
    foreground_activity: Arc<AtomicU64>,
    /// Lets shutdown stop queued rebuildable enrichment without delaying authority.
    shutting_down: Arc<AtomicBool>,
}

impl SealPipeline {
    /// Spawn the authoritative seal worker and the derived enrichment worker.
    pub fn start() -> Self {
        let (job_tx, job_rx) = mpsc::channel::<LifecycleJob>();
        let (enrich_tx, enrich_rx) = mpsc::channel::<LifecycleJob>();
        let (result_tx, result_rx) = mpsc::channel::<LifecycleResult>();
        let result_tx_enrich = result_tx.clone();
        let foreground_activity = Arc::new(AtomicU64::new(0));
        let shutting_down = Arc::new(AtomicBool::new(false));
        let enrich_activity = Arc::clone(&foreground_activity);
        let enrich_shutdown = Arc::clone(&shutting_down);
        let join_seal = thread::Builder::new()
            .name("residiuum-seal-auth".into())
            .spawn(move || seal_worker_loop(job_rx, result_tx))
            .expect("spawn seal authoritative worker");
        let join_enrich = thread::Builder::new()
            .name("residiuum-seal-enrich".into())
            .spawn(move || {
                enrich_worker_loop(
                    enrich_rx,
                    result_tx_enrich,
                    enrich_activity,
                    enrich_shutdown,
                )
            })
            .expect("spawn seal enrichment worker");
        Self {
            job_tx,
            enrich_tx,
            result_rx,
            join_seal: Some(join_seal),
            join_enrich: Some(join_enrich),
            inflight_seals: 0,
            max_pending_seals: DEFAULT_MAX_PENDING_SEALS,
            enrichment_backlog: 0,
            foreground_activity,
            shutting_down,
        }
    }

    /// Note foreground durable publication so enrichment yields to ingestion.
    pub fn note_foreground_activity(&self) {
        self.foreground_activity.fetch_add(1, Ordering::Relaxed);
    }

    /// Submit a seal finalize job. Caller tracks `inflight_seals`.
    pub fn submit_seal(&self, job: LifecycleJob) -> Result<(), StoreError> {
        self.job_tx.send(job).map_err(|_| {
            StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "seal pipeline worker gone",
            ))
        })
    }

    /// Submit a checkpoint job (does not count against seal inflight).
    pub fn submit_checkpoint(&self, job: LifecycleJob) -> Result<(), StoreError> {
        self.submit_seal(job)
    }

    /// Submit derived enrichment (Hydra/Chimera). Never increments `inflight_seals`.
    pub fn submit_enrichment(&mut self, job: LifecycleJob) -> Result<(), StoreError> {
        self.enrichment_backlog = self.enrichment_backlog.saturating_add(1);
        self.enrich_tx.send(job).map_err(|_| {
            StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "enrichment worker gone",
            ))
        })
    }

    /// Non-blocking poll for one completed result.
    pub fn try_recv(&self) -> Option<LifecycleResult> {
        match self.result_rx.try_recv() {
            Ok(r) => Some(r),
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => None,
        }
    }

    /// Block until one result arrives (or worker disconnects).
    pub fn recv(&self) -> Option<LifecycleResult> {
        self.result_rx.recv().ok()
    }

    /// Block with timeout for one result.
    pub fn recv_timeout(&self, timeout: Duration) -> Option<LifecycleResult> {
        match self.result_rx.recv_timeout(timeout) {
            Ok(r) => Some(r),
            Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => None,
        }
    }

    /// Shut down workers and join. Authoritative jobs drain; queued derived jobs
    /// are rebuildable and are abandoned after any currently running job exits.
    pub fn shutdown(mut self) {
        self.shutting_down.store(true, Ordering::Release);
        let _ = self.job_tx.send(LifecycleJob::Shutdown);
        let _ = self.enrich_tx.send(LifecycleJob::Shutdown);
        if let Some(h) = self.join_seal.take() {
            let _ = h.join();
        }
        if let Some(h) = self.join_enrich.take() {
            let _ = h.join();
        }
        while self.result_rx.try_recv().is_ok() {}
    }
}

impl Drop for SealPipeline {
    fn drop(&mut self) {
        self.shutting_down.store(true, Ordering::Release);
        let _ = self.job_tx.send(LifecycleJob::Shutdown);
        let _ = self.enrich_tx.send(LifecycleJob::Shutdown);
        if let Some(h) = self.join_seal.take() {
            let _ = h.join();
        }
        if let Some(h) = self.join_enrich.take() {
            let _ = h.join();
        }
    }
}

fn seal_worker_loop(job_rx: Receiver<LifecycleJob>, result_tx: Sender<LifecycleResult>) {
    while let Ok(job) = job_rx.recv() {
        match job {
            LifecycleJob::Shutdown => break,
            LifecycleJob::FinalizeSeal {
                store_id,
                segment_id,
                pending_path,
                sealed_path,
                limits,
                require_fsync,
                ..
            } => {
                // Writer enqueues EnrichDerived separately (isolation + enable flag).
                let t0 = Instant::now();
                match finalize_seal_authoritative(
                    store_id,
                    segment_id,
                    &pending_path,
                    &sealed_path,
                    limits,
                    require_fsync,
                ) {
                    Ok((content_hash, size, sealed_bytes)) => {
                        let auth_publish_ns = elapsed_ns(t0);
                        let hash = ContentHashState::Known(content_hash);
                        let summary = crate::segment_catalog::summarize_segment_bytes(
                            segment_id,
                            crate::tier::TierClass::Hot,
                            &sealed_bytes,
                            hash,
                            size,
                            limits,
                        );
                        drop(sealed_bytes);
                        let _ = result_tx.send(LifecycleResult::SealDone {
                            segment_id,
                            content_hash: hash,
                            size,
                            summary,
                            auth_publish_ns,
                        });
                        let _ = crate::failpoint::hit("store.seal.after_authoritative_publish");
                    }
                    Err(e) => {
                        let _ = result_tx.send(LifecycleResult::SealFailed {
                            segment_id,
                            error: e.to_string(),
                        });
                    }
                }
            }
            LifecycleJob::FinalizeSealPlan {
                segment_id,
                pending_path,
                sealed_path,
                prefix_len,
                plan,
                require_fsync,
            } => {
                let t0 = Instant::now();
                match publish_sealed_from_summary_frame(
                    &pending_path,
                    &sealed_path,
                    prefix_len,
                    &plan.summary_frame,
                    require_fsync,
                ) {
                    Ok(()) => {
                        let _ = result_tx.send(LifecycleResult::SealDone {
                            segment_id,
                            content_hash: plan.content_hash,
                            size: plan.sealed_len,
                            summary: plan.to_segment_summary(),
                            auth_publish_ns: elapsed_ns(t0),
                        });
                    }
                    Err(e) => {
                        let _ = result_tx.send(LifecycleResult::SealFailed {
                            segment_id,
                            error: e.to_string(),
                        });
                    }
                }
            }
            LifecycleJob::FinalizeSealMeta {
                ids,
                segment_id,
                prefix_len,
                frame_count,
                writer_sequence,
                item_events,
                pending_path,
                sealed_path,
                require_fsync,
            } => {
                // Authoritative: summary footer + rename only. Whole-segment
                // BLAKE3 is derived — deferred to EnrichDerived (no pending read).
                let t0 = Instant::now();
                let result = (|| {
                    let plan = meta_publish_plan(
                        ids,
                        prefix_len,
                        frame_count,
                        writer_sequence,
                        item_events,
                    )?;
                    publish_sealed_from_summary_frame(
                        &pending_path,
                        &sealed_path,
                        prefix_len,
                        &plan.summary_frame,
                        require_fsync,
                    )?;
                    Ok::<_, StoreError>(plan)
                })();
                match result {
                    Ok(plan) => {
                        let _ = result_tx.send(LifecycleResult::SealDone {
                            segment_id,
                            content_hash: plan.content_hash,
                            size: plan.sealed_len,
                            summary: plan.to_segment_summary(),
                            auth_publish_ns: elapsed_ns(t0),
                        });
                    }
                    Err(e) => {
                        let _ = result_tx.send(LifecycleResult::SealFailed {
                            segment_id,
                            error: e.to_string(),
                        });
                    }
                }
            }
            LifecycleJob::FinalizeSealResident {
                ids,
                segment_id,
                prefix,
                frame_count,
                writer_sequence,
                item_events,
                pending_path,
                sealed_path,
                require_fsync,
            } => {
                let prefix_len = prefix.len() as u64;
                let t0 = Instant::now();
                let result = (|| {
                    let mut state = IncrementalSealState::new();
                    state.observe_durable_bytes(0, &prefix)?;
                    let plan = state.finish_publish_plan(
                        ids,
                        prefix_len,
                        frame_count,
                        writer_sequence,
                        item_events,
                    )?;
                    drop(prefix);
                    publish_sealed_from_summary_frame(
                        &pending_path,
                        &sealed_path,
                        prefix_len,
                        &plan.summary_frame,
                        require_fsync,
                    )?;
                    Ok::<_, StoreError>(plan)
                })();
                match result {
                    Ok(plan) => {
                        let _ = result_tx.send(LifecycleResult::SealDone {
                            segment_id,
                            content_hash: plan.content_hash,
                            size: plan.sealed_len,
                            summary: plan.to_segment_summary(),
                            auth_publish_ns: elapsed_ns(t0),
                        });
                    }
                    Err(e) => {
                        let _ = result_tx.send(LifecycleResult::SealFailed {
                            segment_id,
                            error: e.to_string(),
                        });
                    }
                }
            }
            LifecycleJob::Checkpoint {
                cache_path,
                store_id,
                frontier,
                index,
                chunk_locators,
            } => {
                let ok = write_primary_index_frontier(
                    &cache_path,
                    store_id,
                    &frontier,
                    &index,
                    &chunk_locators,
                )
                .is_ok();
                let _ = result_tx.send(LifecycleResult::CheckpointDone { ok });
            }
            LifecycleJob::DerivedCatalogCheckpoint {
                store_id,
                paths,
                placement,
                segment_catalog,
            } => {
                let place_path = crate::tier::tier_placement_path(&paths.catalogs_dir());
                let cat_path = crate::segment_catalog::segment_catalog_path(&paths.catalogs_dir());
                let ok = crate::tier::write_placement(&place_path, store_id, &placement)
                    .and_then(|_| crate::tier::write_tier_roots_file(&paths, &placement))
                    .and_then(|_| {
                        crate::segment_catalog::write_segment_catalog(
                            &cat_path,
                            store_id,
                            &segment_catalog,
                        )
                    })
                    .is_ok();
                let _ = result_tx.send(LifecycleResult::CheckpointDone { ok });
            }
            LifecycleJob::FinalizeProtectedPair {
                store_id,
                segment_id,
                shard,
                pending_path,
                sealed_path,
                prepared_shadow,
                paths,
                require_fsync,
                size,
                summary,
            } => {
                let t_auth = Instant::now();
                let auth_ok = (|| -> Result<(), StoreError> {
                    if !pending_path.is_file() {
                        if sealed_path.is_file() {
                            return Ok(());
                        }
                        return Err(StoreError::Io(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "protected-pair pending missing",
                        )));
                    }
                    if sealed_path.exists() {
                        return Err(StoreError::SegmentIdCollision {
                            segment_id,
                            paths: vec![pending_path.clone(), sealed_path.clone()],
                        });
                    }
                    if let Some(parent) = sealed_path.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    // Same Fast Lane failpoints as Materialized finalize (DEF-022).
                    crate::failpoint::hit("store.seal.before_authoritative_rename")?;
                    fs::rename(&pending_path, &sealed_path)?;
                    if require_fsync {
                        let f = OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(&sealed_path)?;
                        f.sync_all()?;
                        if let Some(parent) = sealed_path.parent() {
                            let _ = crate::atomic_file::sync_dir(parent);
                        }
                    }
                    crate::failpoint::hit("store.seal.after_authoritative_publish")?;
                    Ok(())
                })();
                let auth_publish_ns = elapsed_ns(t_auth);
                if let Err(e) = auth_ok {
                    let _ = result_tx.send(LifecycleResult::SealFailed {
                        segment_id,
                        error: e.to_string(),
                    });
                    continue;
                }
                // Authority is visible, but P★ is not. Publish that gap before
                // starting the potentially long bounded Shadow copy so lag is
                // observable and crash recovery has an honest sealed frontier.
                if let Err(e) = crate::recovery_shadow::note_segment_sealed(
                    &paths,
                    store_id,
                    &segment_id,
                    shard,
                ) {
                    let _ = result_tx.send(LifecycleResult::SealFailed {
                        segment_id,
                        error: format!("sealed coverage: {e}"),
                    });
                    continue;
                }
                let t_shadow = Instant::now();
                let shadow_res =
                    crate::recovery_shadow::publish_prepared_shadow(prepared_shadow, &paths).map(
                        |timing| {
                            (
                                timing.staging_write_operations,
                                timing.staging_write_bytes,
                                timing.staging_write_ns,
                                timing.file_sync_ns,
                            )
                        },
                    );
                let shadow_publish_ns = elapsed_ns(t_shadow);
                let (
                    shadow_staging_write_operations,
                    shadow_staging_write_bytes,
                    shadow_staging_write_ns,
                    shadow_sync_ns,
                ) = match shadow_res {
                    Ok(timing) => timing,
                    Err(e) => {
                        let _ = result_tx.send(LifecycleResult::SealFailed {
                            segment_id,
                            error: format!("shadow publish: {e}"),
                        });
                        continue;
                    }
                };
                // P★ only after both sides durable.
                let frontier_ok = (|| -> Result<(), StoreError> {
                    crate::failpoint::hit("rshd4.frontier.publish")?;
                    let seq = crate::ids::segment_seq_from_id(&segment_id);
                    let mut cov =
                        crate::recovery_shadow::load_protected_coverage(&paths, store_id)?;
                    cov.store_id = store_id;
                    cov.note_durable(shard, seq);
                    crate::recovery_shadow::publish_protected_coverage(&paths, &cov)?;
                    Ok(())
                })();
                if let Err(e) = frontier_ok {
                    let _ = result_tx.send(LifecycleResult::SealFailed {
                        segment_id,
                        error: format!("frontier: {e}"),
                    });
                    continue;
                }
                crate::recovery_shadow::PreparedShadowPublish::clear_shard_meta(
                    &paths,
                    &segment_id,
                );
                let _ = result_tx.send(LifecycleResult::ProtectedPairDone {
                    segment_id,
                    size,
                    summary,
                    auth_publish_ns,
                    shadow_publish_ns,
                    shadow_staging_write_operations,
                    shadow_staging_write_bytes,
                    shadow_staging_write_ns,
                    shadow_sync_ns,
                });
            }
            LifecycleJob::EnrichDerived { segment_id, .. } => {
                // Misrouted — should not happen on the authoritative lane.
                let _ = result_tx.send(LifecycleResult::EnrichDone {
                    segment_id,
                    ok: false,
                    content_hash: ContentHashState::Pending,
                    size: 0,
                    stages: None,
                });
            }
        }
    }
}

fn wait_for_enrichment_window(foreground_activity: &AtomicU64, shutting_down: &AtomicBool) -> u64 {
    wait_for_enrichment_window_with(
        foreground_activity,
        shutting_down,
        ENRICHMENT_QUIET_PERIOD,
        ENRICHMENT_MAX_DEFERRAL,
        ENRICHMENT_ACTIVITY_POLL,
    )
}

fn wait_for_enrichment_window_with(
    foreground_activity: &AtomicU64,
    shutting_down: &AtomicBool,
    quiet_period: Duration,
    max_deferral: Duration,
    activity_poll: Duration,
) -> u64 {
    let started = Instant::now();
    let mut quiet_since = Instant::now();
    let mut observed = foreground_activity.load(Ordering::Acquire);
    loop {
        if shutting_down.load(Ordering::Acquire)
            || quiet_since.elapsed() >= quiet_period
            || started.elapsed() >= max_deferral
        {
            return elapsed_ns(started);
        }
        thread::sleep(activity_poll);
        let current = foreground_activity.load(Ordering::Acquire);
        if current != observed {
            observed = current;
            quiet_since = Instant::now();
        }
    }
}

fn enrich_worker_loop(
    job_rx: Receiver<LifecycleJob>,
    result_tx: Sender<LifecycleResult>,
    foreground_activity: Arc<AtomicU64>,
    shutting_down: Arc<AtomicBool>,
) {
    while let Ok(job) = job_rx.recv() {
        // Derived sidecars are rebuildable. Once shutdown begins, do not turn a
        // durable close into a synchronous drain of an arbitrarily large
        // enrichment backlog. A job already executing is allowed to finish;
        // this check abandons the next queued job and closes the worker.
        if shutting_down.load(Ordering::Acquire) {
            break;
        }
        match job {
            LifecycleJob::Shutdown => break,
            LifecycleJob::EnrichDerived {
                store_id,
                segment_id,
                paths,
                limits,
            } => {
                let job_wall = Instant::now();
                let cpu0 = thread_cpu_ns();
                // Foreground ingestion owns CPU and media. Under a permanently
                // busy workload, allow only bounded sparse derived progress.
                // Once writes go quiet, drain normally. Shutdown abandons the
                // queue after any job already past the worker-loop check.
                let gap_wait_ns = wait_for_enrichment_window(&foreground_activity, &shutting_down);
                let sealed_path = paths.sealed_segment(&segment_id);
                let mut stages = EnrichmentStageTiming {
                    gap_wait_ns,
                    ..EnrichmentStageTiming::default()
                };
                let (ok, content_hash, size) = {
                    let t_read = Instant::now();
                    match fs::read(&sealed_path) {
                        Ok(bytes) => {
                            stages.read_ns = elapsed_ns(t_read);
                            stages.bytes_read = bytes.len() as u64;
                            let t_b3 = Instant::now();
                            let hash = ContentHashState::Known(*blake3::hash(&bytes).as_bytes());
                            stages.blake3_ns = elapsed_ns(t_b3);
                            let size = bytes.len() as u64;
                            let enrich_ok = match enrich_sealed_derived_timed(
                                &paths, store_id, segment_id, &bytes, limits,
                            ) {
                                Ok(enrich_stages) => {
                                    stages.decode_ns = enrich_stages.decode_ns;
                                    stages.hydra_construct_ns = enrich_stages.hydra_construct_ns;
                                    stages.hydra_persist_ns = enrich_stages.hydra_persist_ns;
                                    stages.chimera_construct_ns =
                                        enrich_stages.chimera_construct_ns;
                                    stages.chimera_persist_ns = enrich_stages.chimera_persist_ns;
                                    stages.bytes_written = enrich_stages.bytes_written;
                                    true
                                }
                                Err(_) => false,
                            };
                            // Catalog durability is coalesced on the writer via
                            // DerivedCatalogCheckpoint — enrichment must not rewrite
                            // full catalogs (O(segments) per seal → O(n²) lifetime).
                            (enrich_ok, hash, size)
                        }
                        Err(_) => {
                            stages.read_ns = elapsed_ns(t_read);
                            (false, ContentHashState::Pending, 0)
                        }
                    }
                };
                stages.wall_ns = elapsed_ns(job_wall);
                let cpu1 = thread_cpu_ns();
                stages.cpu_ns = cpu1.saturating_sub(cpu0);
                let _ = crate::failpoint::hit("store.seal.after_derived_enrichment");
                let _ = result_tx.send(LifecycleResult::EnrichDone {
                    segment_id,
                    ok,
                    content_hash,
                    size,
                    stages: Some(stages),
                });
            }
            // Ignore non-enrichment jobs on this lane.
            _ => {}
        }
    }
}

/// Stream-hash a pending prefix and build a [`SealPublishPlan`] (no frame scan).
///
/// Retained for diagnostics / recovery experiments. Hot path uses
/// [`meta_publish_plan`] (hash deferred to enrichment).
#[allow(dead_code)]
pub fn plan_from_pending_prefix(
    pending_path: &std::path::Path,
    ids: SegmentId,
    prefix_len: u64,
    frame_count: u64,
    writer_sequence: u64,
    item_events: u64,
) -> Result<SealPublishPlan, StoreError> {
    let meta = fs::metadata(pending_path)?;
    if meta.len() < prefix_len {
        return Err(StoreError::CorruptMeta(
            "pending prefix shorter than durable_len",
        ));
    }
    let mut file = OpenOptions::new().read(true).open(pending_path)?;
    let mut state = IncrementalSealState::new();
    let mut remaining = prefix_len;
    let mut buf = vec![0u8; 1024 * 1024];
    while remaining > 0 {
        let chunk = remaining.min(buf.len() as u64) as usize;
        file.read_exact(&mut buf[..chunk])?;
        let off = prefix_len - remaining;
        state.observe_durable_bytes(off, &buf[..chunk])?;
        remaining -= chunk as u64;
    }
    state.finish_publish_plan(ids, prefix_len, frame_count, writer_sequence, item_events)
}

/// Append a precomputed summary frame to pending and rename into `segments/`.
///
/// Zero-scan path: no frame scan of the 64 MiB prefix.
pub fn publish_sealed_from_summary_frame(
    pending_path: &std::path::Path,
    sealed_path: &std::path::Path,
    prefix_len: u64,
    summary_frame: &[u8],
    require_fsync: bool,
) -> Result<(), StoreError> {
    if !pending_path.is_file() {
        if sealed_path.is_file() {
            return Ok(());
        }
        return Err(StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("pending seal missing: {}", pending_path.display()),
        )));
    }
    crate::failpoint::hit("store.seal.before_authoritative_rename")?;
    {
        {
            let f = OpenOptions::new().write(true).open(pending_path)?;
            f.set_len(prefix_len)?;
        }
        let mut f = OpenOptions::new().append(true).open(pending_path)?;
        f.write_all(summary_frame)?;
        if require_fsync {
            f.sync_all()?;
        }
    }
    if let Some(parent) = sealed_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let segment_id = crate::layout::segment_id_from_filename(sealed_path).unwrap_or([0u8; 16]);
    crate::media_inventory::rename_exclusive(pending_path, sealed_path, segment_id)?;
    if require_fsync {
        if let Some(parent) = sealed_path.parent() {
            let _ = crate::atomic_file::sync_dir(parent);
        }
    }
    crate::failpoint::hit("store.seal.after_authoritative_publish")?;
    Ok(())
}

// NOTE: cross-device fallback removed from the happy path — exclusive rename
// either succeeds or returns SegmentIdCollision / Io without replacing dest.
#[allow(dead_code)]
fn publish_sealed_from_summary_frame_cross_device_fallback(
    pending_path: &std::path::Path,
    sealed_path: &std::path::Path,
    prefix_len: u64,
    summary_frame: &[u8],
    require_fsync: bool,
) -> Result<(), StoreError> {
    let mut bytes = fs::read(pending_path)?;
    if bytes.len() as u64 != prefix_len.saturating_add(summary_frame.len() as u64) {
        bytes.truncate(prefix_len as usize);
        bytes.extend_from_slice(summary_frame);
    }
    let tmp = sealed_path.with_extension("residiuum.tmp");
    {
        let mut out = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        out.write_all(&bytes)?;
        if require_fsync {
            out.sync_all()?;
        }
    }
    let segment_id = crate::layout::segment_id_from_filename(sealed_path).unwrap_or([0u8; 16]);
    crate::media_inventory::rename_exclusive(&tmp, sealed_path, segment_id)?;
    if require_fsync {
        if let Some(parent) = sealed_path.parent() {
            let _ = crate::atomic_file::sync_dir(parent);
        }
    }
    let _ = fs::remove_file(pending_path);
    let _ = crate::failpoint::hit("store.seal.after_authoritative_publish");
    Ok(())
}

/// Authoritative + derived finalize (open recovery / sync fallback).
///
/// Prefer the worker path which posts [`LifecycleResult::SealDone`] before
/// enrichment. This helper still runs both phases for recovery completeness.
pub fn finalize_seal(
    store_id: [u8; 16],
    segment_id: [u8; 16],
    pending_path: &std::path::Path,
    sealed_path: &std::path::Path,
    limits: SafetyLimits,
    paths: &StorePaths,
    require_fsync: bool,
) -> Result<([u8; 32], u64, Vec<u8>), StoreError> {
    let (content_hash, size, sealed_bytes) = finalize_seal_authoritative(
        store_id,
        segment_id,
        pending_path,
        sealed_path,
        limits,
        require_fsync,
    )?;
    let _ = enrich_sealed_derived(paths, store_id, segment_id, &sealed_bytes, limits);
    Ok((content_hash, size, sealed_bytes))
}

/// Authoritative seal only: seal (preserve offsets), publish sealed image,
/// BLAKE3. Does **not** build Hydra/Chimera.
///
/// **Hot path:** append only the segment-summary suffix to the pending file and
/// `rename` into `segments/` (no full ~seal-threshold rewrite). Falls back to
/// write-temp+rename if rename-across-volume fails.
pub fn finalize_seal_authoritative(
    store_id: [u8; 16],
    segment_id: [u8; 16],
    pending_path: &std::path::Path,
    sealed_path: &std::path::Path,
    limits: SafetyLimits,
    require_fsync: bool,
) -> Result<([u8; 32], u64, Vec<u8>), StoreError> {
    if !pending_path.is_file() {
        // Already finalized (retry / recover race).
        if sealed_path.is_file() {
            let bytes = fs::read(sealed_path)?;
            let hash = *blake3::hash(&bytes).as_bytes();
            return Ok((hash, bytes.len() as u64, bytes));
        }
        return Err(StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("pending seal missing: {}", pending_path.display()),
        )));
    }

    crate::failpoint::hit("store.seal.before_authoritative_rename")?;

    let raw = fs::read(pending_path)?;
    let (sealed_bytes, prefix_len) = seal_pending_bytes(raw, store_id, segment_id, limits)?;
    let content_hash = *blake3::hash(&sealed_bytes).as_bytes();
    let size = sealed_bytes.len() as u64;
    debug_assert!(prefix_len as usize <= sealed_bytes.len());
    debug_assert!(
        sealed_bytes.len() >= prefix_len as usize,
        "summary must extend the verified prefix"
    );

    if let Some(parent) = sealed_path.parent() {
        fs::create_dir_all(parent)?;
    }

    // Prefer: truncate pending to verified prefix, append summary only, rename
    // into segments/. Avoids rewriting tens of MiB already on disk.
    let published = publish_sealed_from_pending(
        pending_path,
        sealed_path,
        &sealed_bytes,
        prefix_len,
        require_fsync,
    )?;
    if !published {
        // Cross-device or exotic FS: full write to temp + exclusive rename.
        let tmp = sealed_path.with_extension("residiuum.tmp");
        {
            let mut out = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            out.write_all(&sealed_bytes)?;
            if require_fsync {
                out.sync_all()?;
            }
        }
        crate::media_inventory::rename_exclusive(&tmp, sealed_path, segment_id)?;
        if require_fsync {
            if let Some(parent) = sealed_path.parent() {
                let _ = crate::atomic_file::sync_dir(parent);
            }
        }
        let _ = fs::remove_file(pending_path);
        if require_fsync {
            if let Some(parent) = pending_path.parent() {
                let _ = crate::atomic_file::sync_dir(parent);
            }
        }
    }

    crate::failpoint::hit("store.seal.after_authoritative_publish")?;
    Ok((content_hash, size, sealed_bytes))
}

/// Derived enrichment for a sealed segment (Hydra + Chimera). Never authoritative.
pub fn enrich_sealed_derived(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: [u8; 16],
    sealed_bytes: &[u8],
    limits: SafetyLimits,
) -> Result<(), StoreError> {
    enrich_sealed_derived_timed(paths, store_id, segment_id, sealed_bytes, limits).map(|_| ())
}

/// Timed Hydra+Chimera enrichment (ETQ-0).
fn enrich_sealed_derived_timed(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: [u8; 16],
    sealed_bytes: &[u8],
    limits: SafetyLimits,
) -> Result<EnrichmentStageTiming, StoreError> {
    let mut stages = EnrichmentStageTiming::default();
    let h = write_hydra_for_bytes_timed(paths, store_id, segment_id, sealed_bytes, limits)?;
    let c =
        write_chimera_from_segment_puts_timed(paths, store_id, segment_id, sealed_bytes, limits)?;
    stages.decode_ns = h.decode_ns.saturating_add(c.decode_ns);
    stages.hydra_construct_ns = h.construct_ns;
    stages.hydra_persist_ns = h.persist_ns;
    stages.chimera_construct_ns = c.construct_ns;
    stages.chimera_persist_ns = c.persist_ns;
    stages.bytes_written = h.bytes_written.saturating_add(c.bytes_written);
    Ok(stages)
}

struct TimedWrite {
    decode_ns: u64,
    construct_ns: u64,
    persist_ns: u64,
    bytes_written: u64,
}

/// Append summary to pending and rename to sealed. Returns false if rename failed
/// (caller should fall back to full write).
fn publish_sealed_from_pending(
    pending_path: &std::path::Path,
    sealed_path: &std::path::Path,
    sealed_bytes: &[u8],
    prefix_len: u64,
    require_fsync: bool,
) -> Result<bool, StoreError> {
    let prefix = prefix_len as usize;
    if prefix > sealed_bytes.len() {
        return Ok(false);
    }
    let summary = &sealed_bytes[prefix..];
    // In-place: keep verified prefix, append summary frame(s) only.
    {
        // Truncate first (separate handle), then append summary — avoids
        // platform-dependent seek-after-set_len positioning bugs.
        {
            let f = OpenOptions::new().write(true).open(pending_path)?;
            f.set_len(prefix_len)?;
        }
        let mut f = OpenOptions::new().append(true).open(pending_path)?;
        f.write_all(summary)?;
        if require_fsync {
            f.sync_all()?;
        }
    }
    // Destination must not be replaced (P0 immutable publish).
    let segment_id = crate::layout::segment_id_from_filename(sealed_path).unwrap_or([0u8; 16]);
    match crate::media_inventory::rename_exclusive(pending_path, sealed_path, segment_id) {
        Ok(()) => {
            if require_fsync {
                if let Some(parent) = sealed_path.parent() {
                    let _ = crate::atomic_file::sync_dir(parent);
                }
            }
            Ok(true)
        }
        Err(StoreError::SegmentIdCollision { .. }) => Err(StoreError::SegmentIdCollision {
            segment_id,
            paths: vec![pending_path.to_path_buf(), sealed_path.to_path_buf()],
        }),
        Err(_) => {
            // Leave pending with summary appended; fallback will write sealed_bytes
            // without replacing an existing sealed destination.
            Ok(false)
        }
    }
}

/// Synchronously finalize every pending file under `active/pending/` (open recovery).
pub fn recover_all_pending(
    paths: &StorePaths,
    store_id: [u8; 16],
    limits: SafetyLimits,
) -> Result<usize, StoreError> {
    let dir = paths.pending_seal_dir();
    if !dir.is_dir() {
        return Ok(0);
    }
    let files = list_residiuum_files(&dir)?;
    let mut n = 0;
    for pending_path in files {
        let Some(segment_id) = segment_id_from_filename(&pending_path) else {
            continue;
        };
        let sealed_path = paths.sealed_segment(&segment_id);
        finalize_seal(
            store_id,
            segment_id,
            &pending_path,
            &sealed_path,
            limits,
            paths,
            true, // open recovery: prefer stable sealed publish
        )?;
        n += 1;
    }
    Ok(n)
}

/// List pending seal segment paths (for pread / all_segment_paths).
pub fn list_pending_paths(paths: &StorePaths) -> Result<Vec<PathBuf>, StoreError> {
    list_residiuum_files(&paths.pending_seal_dir()).map_err(StoreError::from)
}

/// Public entry for sealing an unsealed segment image (active or pending file
/// contents) into a sealed image + verified-prefix length.
///
/// Used by async finalize and by sync seal after write-through discard.
pub fn seal_pending_image(
    raw: Vec<u8>,
    store_id: [u8; 16],
    segment_id: [u8; 16],
    limits: SafetyLimits,
) -> Result<(Vec<u8>, u64), StoreError> {
    seal_pending_bytes(raw, store_id, segment_id, limits)
}

/// Returns `(sealed_image, verified_prefix_len)`.
///
/// `verified_prefix_len` is the byte length of the contiguous verified prefix
/// **before** the summary frame is appended — used to append-only publish.
///
/// Hot path avoids a second full-prefix clone: scan, truncate `raw` in place,
/// resume, seal (append summary into the same `Vec`), move out.
fn seal_pending_bytes(
    mut raw: Vec<u8>,
    store_id: [u8; 16],
    segment_id: [u8; 16],
    limits: SafetyLimits,
) -> Result<(Vec<u8>, u64), StoreError> {
    // Contiguous verified prefix only (same discipline as active recovery).
    let report = scan_forward(&raw, limits);
    let mut end = 0u64;
    let mut frame_count = 0u64;
    let mut writer_sequence = 0u64;
    let mut created_ns = 0u64;
    let mut found_id = None;
    for region in &report.regions {
        match region {
            residiuum_format::ScanRegion::VerifiedFrame { range, frame } => {
                if range.start != end {
                    break;
                }
                end = range.end;
                frame_count = frame_count.saturating_add(1);
                writer_sequence = frame.header.writer_sequence.saturating_add(1);
                if frame.header.known_kind() == Some(FrameKind::SegmentDescriptor) {
                    if let Some((ids, ns, _)) = decode_descriptor_body(&frame.body) {
                        if ids.store_id == store_id {
                            found_id = Some(ids.segment_id);
                            created_ns = ns;
                        }
                    }
                }
                // Already sealed? Accept as-is (prefix == full sealed image).
                if frame.header.known_kind() == Some(FrameKind::SegmentSummary) {
                    raw.truncate(end as usize);
                    return Ok((raw, end));
                }
            }
            residiuum_format::ScanRegion::Hole { .. } => break,
        }
    }
    let sid = found_id.unwrap_or(segment_id);
    let prefix_len = end;
    if end == 0 || frame_count == 0 {
        return Err(StoreError::CorruptMeta(
            "pending segment empty or unreadable",
        ));
    }
    // Keep capacity; drop any torn tail past the verified prefix.
    raw.truncate(end as usize);
    // Prefix bytes for integrity check after seal (summary must not rewrite them).
    // We only need to verify length + that seal only appends — compare via
    // prefix_len and that sealed starts with the same length of content by
    // checking sealed_len == prefix + summary and resume used the same Vec.
    let ids = SegmentId::new(store_id, sid);
    let active =
        ActiveSegment::resume_unsealed(ids, limits, raw, frame_count, writer_sequence, created_ns)
            .map_err(|e| {
                StoreError::CorruptMeta(match e {
                    residiuum_format::SegmentError::MissingDescriptor => {
                        "pending missing descriptor"
                    }
                    residiuum_format::SegmentError::AlreadySealed => "pending already sealed",
                    _ => "pending resume failed",
                })
            })?;
    let sealed = active
        .seal()
        .map_err(|_| StoreError::CorruptMeta("pending seal summary failed"))?;
    let sealed_bytes = sealed.into_bytes();
    // Integrity: seal must only append (summary) past the verified prefix.
    if sealed_bytes.len() < prefix_len as usize {
        return Err(StoreError::CorruptMeta(
            "seal summary path failed to preserve verified prefix",
        ));
    }
    Ok((sealed_bytes, prefix_len))
}

fn write_hydra_for_bytes_timed(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: [u8; 16],
    bytes: &[u8],
    limits: SafetyLimits,
) -> Result<TimedWrite, StoreError> {
    let t_decode = Instant::now();
    let records = records_from_segment_bytes(bytes, limits);
    let decode_ns = elapsed_ns(t_decode);
    if records.is_empty() {
        return Ok(TimedWrite {
            decode_ns,
            construct_ns: 0,
            persist_ns: 0,
            bytes_written: 0,
        });
    }
    let t_build = Instant::now();
    let index = crate::hydra::build(&records, &HydraBuildOptions::default());
    let construct_ns = elapsed_ns(t_build);
    let path = hydra_index_path(paths, &segment_id);
    let t_persist = Instant::now();
    write_hydra_index(&path, store_id, segment_id, &index)?;
    let persist_ns = elapsed_ns(t_persist);
    let bytes_written = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    Ok(TimedWrite {
        decode_ns,
        construct_ns,
        persist_ns,
        bytes_written,
    })
}

/// Chimera layout from put events in the sealed segment (derived; may include
/// superseded keys that were later deleted on a newer segment — same class of
/// derived approximation as a full live projection when index is unavailable).
///
/// Also dual-writes a Recovery Shadow (`.rsh`) with puts **and** tombstones
/// during Materialized dual-run (CSE-3 Stage 2 step 5). After CompactShadow
/// flip, Chimera is locator-only Compact and Shadow is already published on
/// the dual-stream seal path — enrichment must not rewrite Materialized.
fn write_chimera_from_segment_puts_timed(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: [u8; 16],
    bytes: &[u8],
    limits: SafetyLimits,
) -> Result<TimedWrite, StoreError> {
    use crate::envelope::decode_item_envelope;
    use residiuum_format::scan_forward;

    let mode = crate::recovery_shadow::load_recovery_mode(paths)
        .unwrap_or(crate::recovery_shadow::RecoveryMode::Materialized);
    if mode.omits_new_materialized() {
        // CompactShadow: Compact Chimera only; dual-stream already published `.rsh`.
        let t_decode = Instant::now();
        let (_live, frames, _lp) =
            crate::recovery_shadow::decode_segment_for_candidate(segment_id, bytes, limits);
        let decode_ns = elapsed_ns(t_decode);
        if frames.is_empty() {
            return Ok(TimedWrite {
                decode_ns,
                construct_ns: 0,
                persist_ns: 0,
                bytes_written: 0,
            });
        }
        let t_build = Instant::now();
        let layout = crate::chimera::build_compact_layout(&frames, 1);
        let construct_ns = elapsed_ns(t_build);
        let path = crate::chimera::chimera_layout_path(paths, &segment_id);
        let t_persist = Instant::now();
        crate::chimera::write_chimera_layout(&path, store_id, segment_id, &layout)?;
        let persist_ns = elapsed_ns(t_persist);
        let bytes_written = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        return Ok(TimedWrite {
            decode_ns,
            construct_ns,
            persist_ns,
            bytes_written,
        });
    }

    let t_decode = Instant::now();
    let report = scan_forward(bytes, limits);
    // CSE-2R: latest put body per subject (Materialized product restore).
    let mut last: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = std::collections::BTreeMap::new();
    for (_off, frame) in report.verified_frames() {
        if frame.header.known_kind() != Some(FrameKind::ItemEvent) {
            continue;
        }
        let Some(env) = decode_item_envelope(&frame.envelope) else {
            continue;
        };
        match env.event_kind {
            crate::envelope::EventKind::Put => {
                last.insert(env.subject, frame.body.to_vec());
            }
            crate::envelope::EventKind::Delete => {
                last.remove(&env.subject);
            }
        }
    }
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = last.into_iter().collect();
    let decode_ns = elapsed_ns(t_decode);

    // Seal observation for gap-aware lag (P★ only after Shadow publish below).
    let _ = crate::recovery_shadow::note_segment_sealed(paths, store_id, &segment_id, 0);

    if pairs.is_empty() && bytes.is_empty() {
        return Ok(TimedWrite {
            decode_ns,
            construct_ns: 0,
            persist_ns: 0,
            bytes_written: 0,
        });
    }
    let t_build = Instant::now();
    let layout = crate::chimera::build_materialized_layout(
        &pairs,
        1,
        &crate::chimera::ClassifyOptions::default(),
    );
    let construct_ns = elapsed_ns(t_build);
    let path = crate::chimera::chimera_layout_path(paths, &segment_id);
    let t_persist = Instant::now();
    if !pairs.is_empty() {
        crate::chimera::write_chimera_layout(&path, store_id, segment_id, &layout)?;
    }
    // Dual-run Recovery Shadow (RSHD0003 canonical image mirror; additive).
    // Materialized remains product recovery until Stage 2 step 8.
    // Skip when dual-stream already published (or staged) `.rsh` — enrichment
    // must not overwrite RSHD0004 or claim P★ after a failed protected pair.
    let rsh = crate::recovery_shadow::shadow_path(paths, &segment_id);
    let staging = crate::recovery_shadow::shadow_dir(paths).join(format!(
        "{}.rsh.dual.tmp",
        crate::layout::hex16(&segment_id)
    ));
    let dual_owns = rsh.is_file() || staging.is_file();
    if !bytes.is_empty() && !dual_owns {
        let _ = crate::recovery_shadow::build_and_publish_mirror_shadow(
            paths, store_id, segment_id, 0, bytes,
        );
    }
    let persist_ns = elapsed_ns(t_persist);
    let bytes_written = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    Ok(TimedWrite {
        decode_ns,
        construct_ns,
        persist_ns,
        bytes_written,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use residiuum_format::{ActiveSegment, FrameKind, SegmentId};
    use tempfile::tempdir;

    #[test]
    fn enrichment_waits_for_a_quiet_window() {
        let activity = AtomicU64::new(0);
        let shutdown = AtomicBool::new(false);
        let waited = wait_for_enrichment_window_with(
            &activity,
            &shutdown,
            Duration::from_millis(20),
            Duration::from_secs(1),
            Duration::from_millis(1),
        );
        assert!(waited >= 20_000_000, "waited_ns={waited}");
    }

    #[test]
    fn enrichment_shutdown_bypasses_policy_wait() {
        let activity = AtomicU64::new(0);
        let shutdown = AtomicBool::new(true);
        let waited = wait_for_enrichment_window_with(
            &activity,
            &shutdown,
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_millis(10),
        );
        assert!(waited < 10_000_000, "waited_ns={waited}");
    }

    #[test]
    fn seal_pending_preserves_prefix_bytes() {
        let ids = SegmentId::new([1u8; 16], [2u8; 16]);
        let mut active = ActiveSegment::create(ids, SafetyLimits::default(), 42).unwrap();
        let off = active
            .append(FrameKind::ItemEvent, &[0xa0], b"hello", [9u8; 16])
            .unwrap();
        assert!(off > 0);
        let raw = active.as_bytes().to_vec();
        let prefix_len = raw.len();
        let (sealed, kept) =
            seal_pending_bytes(raw.clone(), [1u8; 16], [2u8; 16], SafetyLimits::default()).unwrap();
        assert_eq!(kept as usize, prefix_len);
        assert!(sealed.len() > prefix_len);
        assert_eq!(&sealed[..prefix_len], &raw[..]);
        // Original item body still at same offset.
        let (_h, _e, body, _, _) =
            residiuum_format::verify_frame_at(&sealed[off as usize..], SafetyLimits::default())
                .unwrap();
        assert_eq!(body, b"hello");
    }

    #[test]
    fn finalize_append_only_preserves_pending_prefix() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store_id = [1u8; 16];
        let segment_id = [2u8; 16];
        let ids = SegmentId::new(store_id, segment_id);
        let mut active = ActiveSegment::create(ids, SafetyLimits::default(), 1).unwrap();
        let off = active
            .append(FrameKind::ItemEvent, &[0xa0], b"hello", [9u8; 16])
            .unwrap();
        let pending = paths.pending_segment(&segment_id);
        let raw = active.as_bytes().to_vec();
        let prefix_len = raw.len();
        fs::write(&pending, &raw).unwrap();
        let sealed = paths.sealed_segment(&segment_id);
        let (_hash, size, bytes) = finalize_seal(
            store_id,
            segment_id,
            &pending,
            &sealed,
            SafetyLimits::default(),
            &paths,
            false,
        )
        .unwrap();
        assert!(sealed.is_file());
        assert!(!pending.is_file());
        assert_eq!(size, bytes.len() as u64);
        assert_eq!(&bytes[..prefix_len], &raw[..]);
        let on_disk = fs::read(&sealed).unwrap();
        assert_eq!(on_disk, bytes);
        let (_h, _e, body, _, _) =
            residiuum_format::verify_frame_at(&on_disk[off as usize..], SafetyLimits::default())
                .unwrap();
        assert_eq!(body, b"hello");
    }

    #[test]
    fn authoritative_publish_before_derived_enrichment() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store_id = [3u8; 16];
        let segment_id = [4u8; 16];
        let ids = SegmentId::new(store_id, segment_id);
        let mut active = ActiveSegment::create(ids, SafetyLimits::default(), 1).unwrap();
        active
            .append(FrameKind::ItemEvent, &[0xa0], b"body", [3u8; 16])
            .unwrap();
        let pending = paths.pending_segment(&segment_id);
        fs::write(&pending, active.as_bytes()).unwrap();
        let sealed = paths.sealed_segment(&segment_id);
        let (_hash, _size, bytes) = finalize_seal_authoritative(
            store_id,
            segment_id,
            &pending,
            &sealed,
            SafetyLimits::default(),
            false,
        )
        .unwrap();
        assert!(sealed.is_file());
        assert!(!pending.is_file());
        let hydra = hydra_index_path(&paths, &segment_id);
        let chimera = crate::chimera::chimera_layout_path(&paths, &segment_id);
        assert!(!hydra.is_file(), "authoritative seal must not write Hydra");
        assert!(
            !chimera.is_file(),
            "authoritative seal must not write Chimera"
        );
        // Enrichment is best-effort; empty/unparseable records are Ok(()).
        enrich_sealed_derived(
            &paths,
            store_id,
            segment_id,
            &bytes,
            SafetyLimits::default(),
        )
        .unwrap();
    }

    #[test]
    fn finalize_roundtrip_files() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store_id = [7u8; 16];
        let segment_id = [8u8; 16];
        let ids = SegmentId::new(store_id, segment_id);
        let mut active = ActiveSegment::create(ids, SafetyLimits::default(), 1).unwrap();
        active
            .append(FrameKind::ItemEvent, &[0xa0], b"body", [3u8; 16])
            .unwrap();
        let pending = paths.pending_segment(&segment_id);
        fs::write(&pending, active.as_bytes()).unwrap();
        let sealed = paths.sealed_segment(&segment_id);
        let (hash, size, bytes) = finalize_seal(
            store_id,
            segment_id,
            &pending,
            &sealed,
            SafetyLimits::default(),
            &paths,
            true,
        )
        .unwrap();
        assert!(sealed.is_file());
        assert!(!pending.is_file());
        assert_eq!(size, bytes.len() as u64);
        assert_eq!(hash, *blake3::hash(&bytes).as_bytes());
    }
}
