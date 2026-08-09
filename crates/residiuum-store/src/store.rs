//! Filesystem-backed append store (OVERVIEW §§6–7, §9; Stages 3, 6, 9).

use crate::catalog::{
    collections_catalog_path, try_load_collection_catalog, write_collection_catalog,
    CollectionCatalog,
};
use crate::chunk_payload::{
    decode_chunk_manifest, decode_piece_body, encode_chunk_manifest, encode_piece_body,
    is_chunk_manifest, manifest_from_pieces, reassemble_with_manifest, resolve_piece,
    split_into_pieces, PayloadResult, ResolvedChunk, DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_THRESHOLD,
};
use crate::compact::{
    estimate_compact_bytes, new_planned_job, pread_item_body_if_segment, reclaim_source_segments,
    reclaimable_source_ids, report_from_job, try_load_checkpoint, try_load_compact_job,
    verify_live_segment, write_checkpoint, write_compact_job, write_live_segment, CheckpointMeta,
    CompactJob, CompactOptions, CompactPhase, CompactReport,
};
use crate::large_value::{AdmitDecision, LargeValuePolicy, PayloadLayout, LARGE_VALUE_PROFILE_ID};

use crate::durability::DurabilityMode;
use crate::envelope::{
    decode_item_envelope, encode_item_envelope, EventKind, ItemEnvelope, MAX_SUBJECT_LEN,
};
use crate::error::StoreError;
use crate::history::{
    subject_history_tiered, BeforeEvent, HistoricalSearchResult, HistoryEvent, ReadBudget,
    RecoveryReadOptions, SubjectHistory, VersionedPayloadResult,
};
use crate::ids::{random_id, segment_seq_from_id, subject_item_id};
use crate::index::{slim_put_body_for_index, IndexEntry, PrimaryIndex};
use crate::index_cache::{
    diagnose_primary_cache, primary_cache_path, segment_fingerprint, try_load_primary_index,
    try_load_primary_index_frontier, write_primary_index_frontier, ChunkFrameLocator,
    ChunkLocatorMap, IndexFrontier, LifecycleDiag, PrimaryCacheDiag,
};
use crate::layout::{list_residiuum_files, StorePaths};
use crate::seal_pipeline::{
    list_pending_paths, publish_sealed_from_summary_frame, recover_all_pending,
    EnrichmentStageTotals, LifecycleJob, LifecycleResult, SealPipeline, DEFAULT_MAX_PENDING_SEALS,
};
use crate::secondary::{
    delete_secondary_index, list_secondary_index_paths, secondary_index_path,
    try_load_secondary_index, write_secondary_index, SecondaryIndex,
};
use crate::segment_catalog::{
    rebuild_segment_catalog, segment_catalog_path, summarize_segment_bytes,
    try_load_segment_catalog, upsert_sealed_summary, write_segment_catalog, SegmentCatalog,
    SegmentSummary,
};
use crate::tier::{
    classify_segment_bytes, discover_placements, load_tier_roots_file, register_hot_segment,
    register_hot_segment_known, tier_placement_path, transfer_segment, try_load_placement,
    write_placement, write_tier_roots_file, FormatClassification, MigrationEvidence, TierAwareGet,
    TierClass, TierCoverage, TierMoveMode, TierPlacement,
};
use crate::token_keys::ContinuationKeyring;
use crate::write_dedup::{
    append_write_dedup, append_write_dedup_batch_buffered, load_write_dedup_checked,
    mark_write_dedup_session_clean, mark_write_dedup_session_dirty, rewrite_write_dedup_journal,
    save_write_dedup, sync_write_dedup_journal, write_dedup_journal_path, write_dedup_path,
    write_dedup_session_clean, DedupRecord, WriteDedupTable,
};
use crate::writer_lock::{StoreOpenOptions, WriterLock, WriterLockObservation};
use residiuum_format::{
    decode_store_descriptor_body, encode_frame_into, encode_store_descriptor_frame, scan_forward,
    ActiveSegment, FrameFlags, FrameHeader, FrameKind, FrameParts, SafetyLimits, SegmentId,
    WIRE_MAJOR, WIRE_MINOR,
};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

type MutationIdentity = ([u8; 16], [u8; 32]);

/// Draft meta format version written under `store-info/meta`.
const META_VERSION: &str = "residiuum-store-9\n";

/// Soft max size of the active segment before auto-seal (bytes).
const DEFAULT_SEAL_THRESHOLD: u64 = 4 * 1024 * 1024;

/// Default writer shard count (legacy single-active segment).
const DEFAULT_WRITER_SHARDS: usize = 1;

/// Upper bound on writer shards (DEF-096 Axis B) — keeps pending seal backpressure sane.
pub const MAX_WRITER_SHARDS: usize = 64;

/// How many buffered/durable writes may land before a derived-state checkpoint
/// (index cache + collection catalog) is forced (DEF-023 rate limit).
///
/// Full index-cache rewrites are **O(live subjects × body size)**. Doing them
/// every few hundred puts (or on every seal) produced an 87% write-throughput
/// drop over a single gigabyte (classic O(N) scale curve). Recovery does not
/// depend on a fresh checkpoint: open rebuilds from segments or applies the
/// active tail past a stale frontier.
///
/// Checkpoints still run occasionally so long-running writers accelerate open
/// without dominating the hot path. Seal no longer forces a full rewrite;
/// explicit [`Store::persist_index_cache`] always does.
const DERIVED_CHECKPOINT_EVERY_OPS: u64 = 65_536;

/// Coalesce derived tier/segment-catalog disk writes (not authoritative).
///
/// Full catalog rewrite is O(sealed segments). Persisting on every `SealDone`
/// made rotation cost grow with retention (O(n) per seal → O(n²) lifetime).
/// Memory apply stays O(1); durable checkpoints lag and may disappear.
const CATALOG_CHECKPOINT_EVERY_SEALS: u64 = 32;
const CATALOG_CHECKPOINT_MIN_INTERVAL: Duration = Duration::from_secs(2);

/// Why a live subject could not contribute a complete logical body (DEF-012).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncompleteReason {
    /// Chunked payload is only partially available.
    PayloadPartial,
    /// No surviving chunk bodies for a declared manifest.
    PayloadUnavailable,
    /// Conflicting chunk content at a manifest position.
    PayloadConflict,
    /// Subject bytes are not valid UTF-8 (cannot be addressed by string APIs).
    NonUtf8Subject,
    /// Locator offset past end of segment media (DEF-SCAN-001).
    LocatorOffsetInvalid,
    /// Frame verify/checksum failed at locator (DEF-SCAN-001).
    LocatorFrameVerifyFailed,
    /// Envelope segment id does not match index locator (DEF-SCAN-001).
    LocatorSegmentIdMismatch,
    /// Named segment media is absent (DEF-SCAN-001).
    SegmentNotFound,
}

/// One live subject that could not be fully read during a logical scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveIncomplete {
    /// Subject key bytes.
    pub subject: Vec<u8>,
    /// Why reassembly failed.
    pub reason: IncompleteReason,
}

/// Result of scanning live logical payloads with coverage honesty (DEF-012).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveLogicalScan {
    /// Fully reassembled live entries.
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Live subjects that are not fully readable.
    pub incomplete: Vec<LiveIncomplete>,
    /// True only when `incomplete` is empty **and** tier coverage is complete.
    pub complete: bool,
    /// Offline / unmounted tiers or unavailable segments prevent proven completeness.
    pub tier_coverage_incomplete: bool,
}

/// Diagnostic timings for [`Store::seal_active_with_breakdown`] (measurement only).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SealStageBreakdown {
    /// Wait + apply in-flight async auto-seals.
    pub drain_lifecycle_ns: u64,
    /// Flush active, build sealed image, publish to `segments/`.
    pub final_active_seal_ns: u64,
    /// Dual-stream Shadow finalize + frontier publish (CompactShadow).
    pub shadow_dual_ns: u64,
    /// Tier placement + segment catalog note for the sealed segment.
    pub catalog_publication_ns: u64,
    /// Whole-segment BLAKE3 for catalog `ContentHashState::Known`.
    pub content_hash_ns: u64,
    /// Per-segment Hydra index build/write.
    pub hydra_ns: u64,
    /// Per-segment Chimera layout build/write.
    pub chimera_ns: u64,
    /// Start next active writer + persist active meta.
    pub reopen_active_ns: u64,
}

impl SealStageBreakdown {
    /// Sum of all seal stages (excludes caller ack/close/verify).
    pub fn total_ns(self) -> u64 {
        self.drain_lifecycle_ns
            .saturating_add(self.final_active_seal_ns)
            .saturating_add(self.shadow_dual_ns)
            .saturating_add(self.catalog_publication_ns)
            .saturating_add(self.content_hash_ns)
            .saturating_add(self.hydra_ns)
            .saturating_add(self.chimera_ns)
            .saturating_add(self.reopen_active_ns)
    }
}

/// Cumulative mid-run auto-rotation stage times (writer + auth publish).
///
/// Used for sustained-rotation qualification — not the end-of-run
/// [`SealStageBreakdown`] from explicit `seal_active`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RotationStageTotals {
    /// Rotations that completed stage timing (SealDone applied).
    pub rotations: u64,
    /// Writer: durable flush of the retiring active.
    pub flush_ns: u64,
    /// Writer: rename active → pending.
    pub rename_pending_ns: u64,
    /// Writer: open replacement active + persist meta.
    pub start_active_ns: u64,
    /// Writer: wait for authoritative backpressure (`inflight_seals`).
    pub backpressure_wait_ns: u64,
    /// Auth worker: summary append + rename into `segments/` (from SealDone).
    pub auth_publish_ns: u64,
    /// Writer: apply SealDone catalog/tier publication.
    pub catalog_apply_ns: u64,
}

impl RotationStageTotals {
    /// Sum of timed stages (excludes put-path work between rotations).
    pub fn total_ns(self) -> u64 {
        self.flush_ns
            .saturating_add(self.rename_pending_ns)
            .saturating_add(self.start_active_ns)
            .saturating_add(self.backpressure_wait_ns)
            .saturating_add(self.auth_publish_ns)
            .saturating_add(self.catalog_apply_ns)
    }
}

fn elapsed_ns(t0: std::time::Instant) -> u64 {
    t0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

/// One unfenced page of live bodies for secondary index construction (DEF-027).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexBuildPage {
    /// Complete (subject, body) pairs on this page.
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Subjects skipped because payload reassembly was incomplete.
    pub incomplete: Vec<Vec<u8>>,
    /// More live subjects remain after `after`.
    pub has_more: bool,
    /// Exclusive resume point (last examined subject), when any work ran.
    pub after: Option<Vec<u8>>,
    /// Subjects examined (complete + incomplete).
    pub examined: usize,
}

/// Optimistic concurrency precondition for a single-key put/delete (APB-2).
///
/// Checked against the primary index under the exclusive writer path
/// **before** any event is minted or appended — version test + mutation is
/// one Key Atomic when the caller holds this store handle alone (DEF-020).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteCondition {
    /// Always write (existing unconditional put/delete).
    Unconditional,
    /// Key must be absent (no live entry; tombstone counts as absent).
    Absent,
    /// Live establishing event id must equal this token.
    LiveEventId([u8; 16]),
    /// Key must be live (any establishing event id).
    Present,
}

/// Receipt returned after an acknowledged write (OVERVIEW §7.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteReceipt {
    /// Store identifier.
    pub store_id: [u8; 16],
    /// Segment that received the frame.
    pub segment_id: [u8; 16],
    /// Item lineage identifier.
    pub item_id: [u8; 16],
    /// Unique event identifier for this write.
    pub event_id: [u8; 16],
    /// Event kind that was recorded.
    pub event_kind: EventKind,
    /// Durability mode that was actually applied.
    pub durability: DurabilityMode,
    /// Byte offset of the frame within the segment file.
    pub offset: u64,
    /// Exact **encoded** on-segment frame byte length for this write (0 when no
    /// durable/buffered frame was appended, e.g. memory-only paths that skip
    /// frame accounting).
    ///
    /// Meaning: post-append length of the wire-encoded frame(s) for this
    /// operation — `segment.as_bytes().len() - offset` after `append` (summed
    /// across chunk frames + manifest for chunked puts). This is **not**:
    /// host FS allocation size, logical payload alone, or a payload+N estimate.
    pub encoded_frame_len: u64,
    /// Storage layout chosen for this put (DEF-103); default inline for deletes.
    pub layout: PayloadLayout,
    /// Logical payload bytes for puts (0 for deletes).
    pub logical_len: u64,
    /// Chunk count when layout is chunked (0 when inline/delete).
    pub chunk_count: u32,
    /// Effective large-value profile id at write time (DEF-103).
    pub profile_id: String,
}

/// One operation-bearing put admitted to a shared durable commit cohort.
#[derive(Debug, Clone, Copy)]
pub struct OperationPut<'a> {
    /// Fully encoded storage subject.
    pub subject: &'a [u8],
    /// Encoded application body.
    pub body: &'a [u8],
    /// Key-atomic condition evaluated in cohort order.
    pub condition: WriteCondition,
    /// Stable client mutation identity.
    pub operation_id: [u8; 16],
    /// Canonical identity of the complete logical mutation.
    pub content_hash: [u8; 32],
}

/// Physical operation kind retained as an independent framed mutation inside
/// a shared durability cohort.
#[derive(Debug, Clone, Copy)]
pub enum OperationMutationKind<'a> {
    /// Establish a new live value.
    Put(&'a [u8]),
    /// Establish a tombstone.
    Delete,
}

/// One independently identified mutation admitted to a shared physical cohort.
#[derive(Debug, Clone, Copy)]
pub struct OperationMutation<'a> {
    /// Fully encoded storage subject.
    pub subject: &'a [u8],
    /// Put body or tombstone operation.
    pub kind: OperationMutationKind<'a>,
    /// Key-atomic condition evaluated in cohort order.
    pub condition: WriteCondition,
    /// Stable client mutation identity.
    pub operation_id: [u8; 16],
    /// Canonical identity of the complete logical mutation.
    pub content_hash: [u8; 32],
}

/// Individual result from a durable operation cohort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationPutOutcome {
    /// Durable storage receipt.
    pub receipt: WriteReceipt,
    /// True when an earlier authoritative acceptance supplied the receipt.
    pub deduplicated: bool,
}

impl WriteReceipt {
    fn base(
        store_id: [u8; 16],
        segment_id: [u8; 16],
        item_id: [u8; 16],
        event_id: [u8; 16],
        event_kind: EventKind,
        durability: DurabilityMode,
        offset: u64,
    ) -> Self {
        Self {
            store_id,
            segment_id,
            item_id,
            event_id,
            event_kind,
            durability,
            offset,
            encoded_frame_len: 0,
            layout: PayloadLayout::Inline,
            logical_len: 0,
            chunk_count: 0,
            profile_id: LARGE_VALUE_PROFILE_ID.to_string(),
        }
    }

    fn with_layout(mut self, admit: AdmitDecision, profile_id: &str) -> Self {
        self.layout = admit.layout;
        self.logical_len = admit.logical_len;
        self.chunk_count = admit.chunk_count;
        self.profile_id = profile_id.to_string();
        self
    }
}

/// Summary of a catalog-free salvage pass over all segment files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SalvageReport {
    /// Files scanned.
    pub files_scanned: usize,
    /// Structurally verified frames (all kinds).
    pub verified_frames: u64,
    /// Verified item events with decodable draft envelopes.
    pub item_events: u64,
    /// Explicit holes found across files.
    pub holes: u64,
    /// Live subjects after applying events in file order.
    pub live_subjects: usize,
}

/// Result of non-destructive salvage / export into a new store path (Stage 7 + DEF-011).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SalvageCopyReport {
    /// Scan summary of the **source** store (source is never mutated).
    pub source: SalvageReport,
    /// Destination store root that received recovered evidence or live state.
    pub destination: PathBuf,
    /// Recovery mode used for this copy.
    pub mode: crate::recovery::SalvageMode,
    /// Live subjects present in the destination after recovery.
    pub subjects_copied: usize,
    /// Verified frames byte-copied (evidence mode); zero for live-state export.
    pub frames_copied: u64,
    /// Holes recorded in the recovery manifest (evidence mode).
    pub holes_recorded: u64,
    /// Path of the recovery manifest when written (evidence mode).
    pub manifest_path: Option<PathBuf>,
}

/// Timings and bounded-I/O counters captured during the most recent store open.
///
/// Values are diagnostic evidence, not durability semantics. Nanoseconds use a
/// monotonic process clock and saturate at `u64::MAX`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreOpenMetrics {
    /// Entire successful `open_with_options` call.
    pub total_ns: u64,
    /// Writer-lock acquisition plus store identity/profile loading.
    pub identity_and_lock_ns: u64,
    /// Tier-state load.
    pub tier_state_ns: u64,
    /// Authoritative media inventory and collision checks.
    pub inventory_ns: u64,
    /// Bytes read from bounded frame-0 descriptor probes in the returned pass.
    pub inventory_descriptor_probe_bytes: u64,
    /// Full-media fallback bytes; expected to be zero for normal fail-closed open.
    pub inventory_fallback_scan_bytes: u64,
    /// Pending-seal recovery.
    pub pending_recovery_ns: u64,
    /// Pending seals actually finalized during open.
    pub pending_seals_recovered: u64,
    /// Protected-pair recovery.
    pub protected_pair_recovery_ns: u64,
    /// Protected auth/shadow pairs actually repaired during open.
    pub protected_pairs_recovered: u64,
    /// Primary-index load or rebuild, including chunk-locator reconstruction.
    pub index_ns: u64,
    /// Segment bytes fully decoded while opening the primary index.
    ///
    /// Expected to be zero on a clean v4-checkpoint open.
    pub index_full_scan_bytes: u64,
    /// Active-segment bytes decoded after the checkpoint frontier.
    pub index_active_replay_bytes: u64,
    /// True when chunk locators came from a validated v4 checkpoint.
    pub chunk_locators_from_checkpoint: bool,
    /// Observable primary-index startup path.
    pub index_disposition: IndexOpenDisposition,
    /// Cache decision that selected the startup path.
    pub index_cache_decision: IndexCacheDecision,
    /// On-disk primary checkpoint bytes examined.
    pub index_cache_bytes: u64,
    /// Time spent reading and decoding an accepted primary checkpoint.
    pub index_cache_decode_ns: u64,
    /// Time spent cloning the durable projection into the live projection.
    pub index_install_clone_ns: u64,
    /// Time spent deriving collection catalogues from an accepted checkpoint.
    pub index_catalog_derive_ns: u64,
    /// Primary entries installed after load/rebuild.
    pub index_entries: u64,
    /// Chunk locator records installed after load/rebuild.
    pub chunk_locator_entries: u64,
    /// Authoritative segment files included in index validation/recovery.
    pub index_segments_examined: u64,
    /// Collection/segment catalog load or rebuild.
    pub catalog_ns: u64,
    /// Durable segment allocator reconstruction.
    pub allocator_ns: u64,
    /// Dedup-table load.
    pub dedup_ns: u64,
    /// Active-segment resume/start.
    pub active_resume_ns: u64,
    /// Compaction recovery.
    pub compaction_recovery_ns: u64,
    /// Durable compaction jobs examined during open.
    pub compaction_jobs_examined: u64,
    /// Recovery-mode reload.
    pub recovery_mode_ns: u64,
}

/// Structured startup report returned to applications after a successful open.
pub type StoreOpenReport = StoreOpenMetrics;

/// Primary-index work performed during store open.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IndexOpenDisposition {
    /// No index work was recorded.
    #[default]
    NotRun,
    /// A v4 checkpoint covered the full durable frontier.
    Loaded,
    /// A v4 checkpoint loaded and only its active tail was replayed.
    TailReplayed,
    /// Authoritative segments reconstructed the derived state.
    Rebuilt,
    /// A legacy checkpoint loaded, locators were reconstructed, and v4 was written.
    LegacyUpgraded,
}

/// Why the primary checkpoint was accepted or bypassed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IndexCacheDecision {
    /// No decision was recorded.
    #[default]
    NotChecked,
    /// A valid v4 checkpoint was accepted.
    AcceptedV4,
    /// A valid v2/v3 checkpoint required locator reconstruction.
    AcceptedLegacy,
    /// A legacy v1 full-fingerprint checkpoint was accepted.
    AcceptedV1,
    /// No primary checkpoint existed.
    Absent,
    /// The checkpoint could not be decoded or authenticated.
    Rejected,
    /// The sealed-set fingerprint no longer matched.
    SealedFingerprintMismatch,
    /// The checkpoint frontier was ahead of active media.
    ActiveFrontierAhead,
}

#[derive(Debug, Clone, Copy, Default)]
struct IndexOpenStats {
    full_scan_bytes: u64,
    active_replay_bytes: u64,
    chunk_locators_from_checkpoint: bool,
    disposition: IndexOpenDisposition,
    cache_decision: IndexCacheDecision,
    cache_bytes: u64,
    cache_decode_ns: u64,
    install_clone_ns: u64,
    catalog_derive_ns: u64,
    index_entries: u64,
    chunk_locator_entries: u64,
    segments_examined: u64,
}

enum IndexLoadAttempt {
    Loaded(IndexOpenStats),
    Miss(IndexCacheDecision),
}

/// Open single-node store handle.
pub struct Store {
    paths: StorePaths,
    store_id: [u8; 16],
    limits: SafetyLimits,
    /// Successful-open diagnostic breakdown (zeroed for newly created stores).
    open_metrics: StoreOpenMetrics,
    /// Visibility index (includes memory-mode publishes).
    index: PrimaryIndex,
    /// Segment-derived durable projection only (DEF-013 / DEF-023).
    ///
    /// Updated only after buffered/durable append succeeds. Never includes
    /// memory-mode visibility. Used for index-cache and on-disk catalog writes
    /// so the write path never rescans sealed segment bytes.
    durable_index: PrimaryIndex,
    /// Buffered/durable ops since the last derived-state disk checkpoint (DEF-023).
    derived_ops_since_checkpoint: u64,
    /// Active segment per writer shard (DEF-096 Axis B). Length == `writer_shards`.
    ///
    /// Shard 0 with `writer_shards == 1` uses the legacy `active/active.residiuum` path.
    actives: Vec<Option<ActiveWriter>>,
    /// Number of concurrent append shards (subject-hash routing). Always ≥ 1.
    writer_shards: usize,
    /// Counter used to mint sortable segment ids (recovered from on-disk max).
    segment_seq: u64,
    /// Seal active segment when it reaches this many bytes.
    seal_threshold: u64,
    /// Bodies larger than this are written as chunked payloads (Stage 6).
    ///
    /// Mirrored from [`Self::large_value_policy`]; keep in sync via policy setters.
    chunk_threshold: usize,
    /// Max logical bytes per payload-chunk frame.
    chunk_size: usize,
    /// Validated large-value admission / layout policy (DEF-103).
    large_value_policy: LargeValuePolicy,
    /// Derived collection catalog (rebuildable). Includes memory-mode names.
    collection_catalog: CollectionCatalog,
    /// Durable-only collection names (segment-backed); used for on-disk catalog.
    durable_collections: CollectionCatalog,
    /// Segment placement across storage tiers (Stage 9, derived).
    tier_placement: TierPlacement,
    /// Hierarchical segment summary catalog (Stage 9, derived).
    segment_catalog: SegmentCatalog,
    /// Seals applied to in-memory catalogs since last derived-catalog checkpoint.
    catalog_seals_since_checkpoint: u64,
    /// In-memory tier/segment catalog differs from last durable checkpoint.
    catalog_dirty: bool,
    /// Wall clock of last derived-catalog checkpoint submit/flush.
    last_catalog_checkpoint_at: Instant,
    /// Exclusive writer ownership (DEF-020). `None` for inspect/read-only opens.
    writer_lock: Option<WriterLock>,
    /// Client operation dedup table (DEF-010); empty when unused.
    write_dedup: WriteDedupTable,
    /// Previous process ended before certifying its outcome journal complete.
    write_dedup_recovery_required: bool,
    /// The operation coordinator is cooking a cohort under the exclusive store
    /// lock. Frames stay in the active-segment buffer until one gathered tail
    /// write crosses the cohort boundary.
    operation_cohort_gathering: bool,
    /// Background seal/checkpoint worker (DEF-096 Axis A). Writer opens only.
    seal_pipeline: Option<SealPipeline>,
    /// When true, auto-seal on threshold uses O(1) rotate + background finalize.
    /// Explicit [`Self::seal_active`] always drains and runs the synchronous path.
    async_lifecycle: bool,
    /// When false, skip Hydra/Chimera enqueue (measurement control / operator).
    ///
    /// Authoritative seal still runs; enrichment backlog may stay empty.
    enrichment_enabled: bool,
    /// Experimental: write-time dual-stream Recovery Shadow (RSHD0004).
    ///
    /// When enabled, each cooked frame append is mirrored into an independently
    /// allocated Shadow staging file; seal finalizes Shadow before
    /// `protected_frontier` advances. **Not** a product flip (Materialized
    /// remains recovery authority until Stage 2 step 8).
    shadow_dual_stream: bool,
    /// Cumulative dual-stream Shadow finalize nanoseconds (measurement).
    shadow_dual_finalize_ns: u64,
    /// Dual-stream Shadows successfully published this process (measurement).
    shadow_dual_published: u64,
    /// Durable recovery-mode marker (Step 8 flip). Default Materialized dual-run.
    recovery_mode: crate::recovery_shadow::RecoveryMode,
    /// When true, resume/inventory accept foreign-store segment descriptors
    /// (salvage dest / identity-reassign reopen only).
    accept_foreign_store_id: bool,
    /// Cumulative auto-rotation stage timings (sustained-rotation qualification).
    rotation_stage_totals: RotationStageTotals,
    /// Cumulative derived enrichment stage timings (ETQ-0).
    enrichment_stage_totals: EnrichmentStageTotals,
    /// Derived chunk_event_id → physical frame locators (DEF-098).
    ///
    /// Non-authoritative: rebuilt from segment scans and updated on chunk append.
    /// Ordinary chunked get uses these for bounded preads; absence falls back to
    /// a generation-filtered segment scan (never mixes generations by item_id).
    chunk_locators: ChunkLocatorMap,
    /// Secret continuation-token keyring (DEF-097). Never logged or exported.
    token_keyring: ContinuationKeyring,
    /// Optional PQH boundary instrumentation (write/sync/rotate/publish/lifecycle).
    /// Default disabled — zero cost on ordinary product paths.
    boundary_probe: crate::boundary_probe::BoundaryProbe,
    /// Diagnostic I/O detach for phase-bench bisection (default: real file).
    /// **Not a product durability mode** — seals / recovery are undefined under
    /// non-Real sinks; use only with a huge seal threshold for short microbenches.
    diagnostic_io: DiagnosticIoSink,
    /// Cached `/dev/null` handle when [`DiagnosticIoSink::DevNull`] is set.
    null_io_file: Option<File>,
    /// Diagnostic: skip dual-index publish / collection / derived after durable append.
    ///
    /// Isolates **data cooking** (encode + append + tail write) from **indexing**.
    /// Visibility will not match frames; never enable in product paths.
    diagnostic_skip_index: bool,
    /// Diagnostic: skip `segment.append` / `encode_frame_into` (Blake + buffer cook).
    ///
    /// Isolates whether **append_frame** is the data-cooking killer. No new bytes
    /// are written to the active file; never enable in product paths.
    diagnostic_skip_append_frame: bool,
    /// Diagnostic: still encode/copy/write frames but skip BLAKE3 body hash.
    ///
    /// Isolates Blake vs memcpy/write within append_frame. Frames fail verify.
    diagnostic_skip_blake: bool,
    /// Parallel record-cooker workers for single-shard [`Self::put_many`].
    /// `1` = serial (default); `N>1` cooks full frames (env+Blake+encode) in parallel.
    cook_parallelism: usize,
    /// Product opt-in active-segment growth (default grow-on-append).
    ///
    /// Watermark mode is **not** default-on. Only applies when
    /// [`Self::diagnostic_io`] is [`DiagnosticIoSink::Real`] (diag sinks win for
    /// bisection). Does not change CSQ durability labels on receipts.
    segment_growth: crate::segment_growth::SegmentGrowthPolicy,
    /// AWO-1: writer poisoned after uncertain physical tail (short write / barrier fail).
    ///
    /// Mutations must refuse until ordinary close/reopen recovery. Distinct from a
    /// full AdaptiveWriteLease (AWO-3); this is the store-local poison bit.
    awo_writer_poisoned: bool,
    /// AWO-3: adaptive write lease owns mutation; direct put/delete refuse with
    /// [`StoreError::AdaptiveWriterActive`] until the lease is released.
    awo_lease_active: bool,
}

/// Where `write_segment_tail` sends bytes (diagnostic bisection only).
///
/// Used to answer: is wall time on the **store CPU path** or the **OS/media
/// write path**? Product code must leave this at [`DiagnosticIoSink::Real`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DiagnosticIoSink {
    /// Normal path: seek + `write_all` on the active segment file.
    #[default]
    Real,
    /// Full Buffered put path, but **no** `write_all` (detach all OS I/O).
    /// Advances `durable_len` as if the transfer succeeded.
    Discard,
    /// Full path with `write_all` to `/dev/null` (syscall/VFS, no durable media).
    DevNull,
    /// Spike: coalesce real-file `write_all` into ≥100 KiB chunks (or flush after
    /// 250 ms). Diagnostic only — proves whether larger disk writes change thr.
    Coalesce100k,
    /// Seek to `durable_len` only; no `write_all`. Bisects seek tax vs Discard.
    SeekOnly,
    /// Real-file `write_all` **without** seek (cursor assumed at end). Bisects
    /// seek vs page-cache write on the active segment.
    RealNoSeek,
    /// Seek(0) + `write_all` every time (overwrite; file stays tiny). Bisects
    /// **file extension / growth** vs copying bytes into a regular file.
    RealOverwrite,
    /// Like [`Real`], but active segment is `set_len`'d to 512 MiB at create
    /// (often sparse on APFS). Bisects logical pre-size vs grow-on-append.
    RealPrealloc,
    /// Like [`RealPrealloc`], then touch every 1 MiB to force physical pages.
    /// Bisects sparse hole vs allocated extent.
    RealPreallocFill,
    /// Like [`Real`], but macOS `fcntl(F_PREALLOCATE)` (or Linux `posix_fallocate`)
    /// then `set_len`. Tests Gemini-style OS block reserve without page-touch.
    RealPreallocFcntl,
    /// `F_PREALLOCATE` + `set_len` + **bulk zero** (1 MiB writes). Tests whether
    /// first-touch zeroing (not mere extent reserve) is what page-touch bought.
    RealPreallocZero,
    /// `F_PREALLOCATE` + `set_len` + **seal-sized ahead-of-write zero** (64 MiB
    /// chunks during the put path). Amortizes zeroing into the odometer.
    RealPreallocWatermark,
}

/// Staged put awaiting persist-before-publish (AWO-1 batch path).
struct StagedPut {
    subject: Vec<u8>,
    item_id: [u8; 16],
    event_id: [u8; 16],
    segment_id: [u8; 16],
    offset: u64,
    encoded_frame_len: u64,
    admit: crate::large_value::AdmitDecision,
    profile_id: String,
}

struct ActiveWriter {
    segment_id: [u8; 16],
    segment: ActiveSegment,
    file: File,
    /// Bytes known durable on disk for this file (complete frames only).
    durable_len: u64,
    /// Strongest durability **ack** applied to frames in this active segment.
    ///
    /// Seal/rotate flushes at least this strong so we never force `sync_all` on a
    /// segment that only ever received `Buffered` puts (CSQ-ACK-004: Buffered does
    /// not require fsync). Any `Durable` put upgrades the segment for the rest of
    /// its life until sealed.
    max_ack_durability: DurabilityMode,
    /// Coalesce100k spike: pending bytes not yet `write_all`'d (diagnostic only).
    coalesce_buf: Vec<u8>,
    coalesce_off: u64,
    coalesce_since: Option<std::time::Instant>,
    /// Diagnostic / product watermark: file offset through which bytes were bulk-zeroed.
    ///
    /// When [`Self::runway`] is set, the atomic inside the preparer is authoritative
    /// for put-path readiness; this field tracks the last known value for diag sinks.
    zeroed_thru: u64,
    /// Background first-touch for product watermark (None = grow / diag put-path zero).
    runway: Option<crate::runway_preparer::RunwayPreparer>,
    /// Item-event frames observed in this active segment (catalog summary).
    item_events: u64,
    /// Experimental dual-stream Shadow staging for this active segment.
    shadow_dual: Option<crate::recovery_shadow::ShadowDualStream>,
}

/// Measured segment-tail file I/O at the actual boundary (write/sync).
#[derive(Debug, Clone, Default)]
struct TailIoStats {
    write_requested: u64,
    write_completed: u64,
    write_duration_ns: u64,
    write_outcome: crate::boundary_probe::BoundaryOutcome,
    synced: bool,
    sync_duration_ns: u64,
    sync_outcome: crate::boundary_probe::BoundaryOutcome,
    /// When true, caller must surface short-write error after probe record.
    fail_as_short_write: bool,
}

impl Store {
    /// Create a new store at `path` (directory). Fails if a store already exists.
    ///
    /// Uses a single active segment (legacy layout). Prefer
    /// [`Self::create_with_shards`] for multi-core append (DEF-096 Axis B).
    pub fn create(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::create_with_shards(path, DEFAULT_WRITER_SHARDS)
    }

    /// Create a store with `writer_shards` independent active segments.
    ///
    /// Subjects are routed by BLAKE3 hash (`subject_item_id` prefix). Each shard
    /// has its own file handle, append offset, and seal lifecycle. `writer_shards`
    /// must be in `1..=MAX_WRITER_SHARDS`. Count is persisted under
    /// `store-info/writer_shards` and recovered on open (DEF-096 Axis B).
    ///
    /// Fresh stores default to [`RecoveryMode::CompactShadow`] (CSE-3 Stage 2k).
    /// Existing trees without a mode marker remain Materialized on open — there
    /// is no silent migration.
    pub fn create_with_shards(
        path: impl AsRef<Path>,
        writer_shards: usize,
    ) -> Result<Self, StoreError> {
        Self::create_with_shards_mode(
            path,
            writer_shards,
            crate::recovery_shadow::RecoveryMode::CompactShadow,
        )
    }

    /// Create a store with an explicit recovery mode (migration / qual fixtures).
    ///
    /// Prefer [`Self::create_with_shards`] for product defaults. Use
    /// [`RecoveryMode::Materialized`] only for dual-run migration baselines and
    /// Step 8 ceremony fixtures — not for new product stores.
    pub fn create_with_shards_mode(
        path: impl AsRef<Path>,
        writer_shards: usize,
        recovery_mode: crate::recovery_shadow::RecoveryMode,
    ) -> Result<Self, StoreError> {
        let writer_shards = writer_shards.clamp(1, MAX_WRITER_SHARDS);
        let paths = StorePaths::new(path.as_ref());
        if paths.looks_like_store() {
            return Err(StoreError::AlreadyExists(paths.root.clone()));
        }
        if paths.root.exists() {
            if !paths.root.is_dir() {
                return Err(StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "path exists and is not a directory",
                )));
            }
            // Allow empty directory only.
            if fs::read_dir(&paths.root)?.next().is_some() {
                return Err(StoreError::AlreadyExists(paths.root.clone()));
            }
        }
        paths.create_dirs()?;
        // Ensure multi-shard active directories exist.
        if writer_shards > 1 {
            for shard in 0..writer_shards {
                fs::create_dir_all(paths.active_shard_dir(shard, writer_shards))?;
            }
        }
        // Exclusive ownership before any authoritative write (DEF-020).
        let writer_lock = WriterLock::acquire(&paths)?;
        let store_id = random_id()?;
        let created_ns = now_ns();
        crate::atomic_file::write_atomic(&paths.store_id_file(), &store_id)?;
        crate::atomic_file::write_atomic(&paths.meta_file(), META_VERSION.as_bytes())?;
        write_writer_shards_file(&paths, writer_shards)?;
        crate::failpoint::hit("store.create.after_meta")?;
        write_store_descriptor_file(&paths, store_id, created_ns)?;
        // DEF-097: mint store-local continuation secrets (≥256-bit entropy).
        let token_keyring = ContinuationKeyring::mint_new()?;
        token_keyring.save_store(&paths)?;
        // Ensure parent dir entry is durable for create.
        sync_dir(&paths.root)?;

        let mut store = Self {
            paths,
            store_id,
            limits: SafetyLimits::default(),
            open_metrics: StoreOpenMetrics::default(),
            index: PrimaryIndex::new(),
            durable_index: PrimaryIndex::new(),
            derived_ops_since_checkpoint: 0,
            actives: (0..writer_shards).map(|_| None).collect(),
            writer_shards,
            segment_seq: 0,
            seal_threshold: DEFAULT_SEAL_THRESHOLD,
            chunk_threshold: DEFAULT_CHUNK_THRESHOLD,
            chunk_size: DEFAULT_CHUNK_SIZE,
            large_value_policy: LargeValuePolicy::application_v1(),
            collection_catalog: CollectionCatalog::new(),
            durable_collections: CollectionCatalog::new(),
            tier_placement: TierPlacement::new(),
            segment_catalog: SegmentCatalog::new(),
            catalog_seals_since_checkpoint: 0,
            catalog_dirty: false,
            last_catalog_checkpoint_at: Instant::now(),
            writer_lock: Some(writer_lock),
            write_dedup: WriteDedupTable::new(),
            write_dedup_recovery_required: false,
            operation_cohort_gathering: false,
            seal_pipeline: Some(SealPipeline::start()),
            async_lifecycle: true,
            enrichment_enabled: true,
            shadow_dual_stream: false,
            shadow_dual_finalize_ns: 0,
            shadow_dual_published: 0,
            recovery_mode: crate::recovery_shadow::RecoveryMode::Materialized,
            accept_foreign_store_id: false,
            rotation_stage_totals: RotationStageTotals::default(),
            enrichment_stage_totals: EnrichmentStageTotals::default(),
            chunk_locators: HashMap::new(),
            token_keyring,
            boundary_probe: crate::boundary_probe::BoundaryProbe::disabled(),
            diagnostic_io: DiagnosticIoSink::Real,
            null_io_file: None,
            diagnostic_skip_index: false,
            diagnostic_skip_append_frame: false,
            diagnostic_skip_blake: false,
            cook_parallelism: 1,
            segment_growth: crate::segment_growth::SegmentGrowthPolicy::GrowOnAppend,
            awo_writer_poisoned: false,
            awo_lease_active: false,
        };
        // Scale pending-seal backpressure with shard count (each shard may rotate).
        if let Some(pipe) = store.seal_pipeline.as_mut() {
            pipe.max_pending_seals = DEFAULT_MAX_PENDING_SEALS.saturating_mul(writer_shards.max(1));
        }
        store.start_all_active_segments()?;
        store.persist_all_actives(DurabilityMode::Durable)?;
        crate::failpoint::hit("store.create.after_active_header")?;
        store.persist_index_cache()?;
        store.refresh_collection_catalog()?;
        store.refresh_tier_state()?;
        // Durable product mode marker — missing on legacy trees ⇒ Materialized.
        crate::recovery_shadow::persist_recovery_mode(&store.paths, store.store_id, recovery_mode)?;
        store.apply_recovery_mode(recovery_mode);
        // A missing/dirty marker means the next writer must reconcile any
        // authoritative operation frame whose outcome journal append was
        // interrupted. A brand-new store has no such history, but is marked
        // dirty before it can accept its first mutation.
        mark_write_dedup_session_dirty(&store.paths)?;
        Ok(store)
    }

    /// Open an existing store, or create if the path does not exist yet.
    ///
    /// Non-blocking writer lock (DEF-020). Prefer [`Self::open_with_options`]
    /// for bounded wait / cancel (DEF-101).
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_with_options(path, StoreOpenOptions::default())
    }

    /// Open (or create) with writer-lock wait options (DEF-101).
    ///
    /// Writer-lock failure is never database absence. Use [`Self::open_inspect`]
    /// for read-only access while a writer is live. Never delete `writer.lock`
    /// to force unlock — the OS exclusive lock is authoritative.
    pub fn open_with_options(
        path: impl AsRef<Path>,
        options: StoreOpenOptions,
    ) -> Result<Self, StoreError> {
        let open_started = Instant::now();
        let identity_started = Instant::now();
        let root = path.as_ref();
        let paths = StorePaths::new(root);
        if !paths.looks_like_store() {
            if root.exists() {
                // Empty directory → create; non-empty without store-info → error.
                if root.is_dir() {
                    let empty = fs::read_dir(root)?.next().is_none();
                    if empty {
                        return Self::create(root);
                    }
                }
                return Err(StoreError::NotAStore(root.to_path_buf()));
            }
            return Self::create(root);
        }

        // Exclusive ownership before opening the active segment (DEF-020 / DEF-101).
        let writer_lock = WriterLock::acquire_with_options(&paths, &options)?;
        let store_id = read_store_id(&paths)?;
        let meta = fs::read_to_string(paths.meta_file()).unwrap_or_default();
        if !meta.starts_with("residiuum-store-") {
            return Err(StoreError::CorruptMeta("unexpected meta version"));
        }
        // Store descriptor is framed evidence, not the sole identity map.
        // Mismatch with store_id is corrupt; absence is tolerated for older trees.
        verify_store_descriptor_if_present(&paths, store_id)?;
        let writer_shards = read_writer_shards(&paths)?;
        // DEF-097: load or mint keyring (upgrade older trees).
        let token_keyring = ContinuationKeyring::load_or_mint_store(&paths)?;
        let mut open_metrics = StoreOpenMetrics {
            identity_and_lock_ns: elapsed_ns(identity_started),
            ..StoreOpenMetrics::default()
        };

        let preceding_writer_was_clean = write_dedup_session_clean(&paths);
        let mut store = Self {
            paths,
            store_id,
            limits: SafetyLimits::default(),
            open_metrics: StoreOpenMetrics::default(),
            index: PrimaryIndex::new(),
            durable_index: PrimaryIndex::new(),
            derived_ops_since_checkpoint: 0,
            actives: (0..writer_shards).map(|_| None).collect(),
            writer_shards,
            segment_seq: 0,
            seal_threshold: DEFAULT_SEAL_THRESHOLD,
            chunk_threshold: DEFAULT_CHUNK_THRESHOLD,
            chunk_size: DEFAULT_CHUNK_SIZE,
            large_value_policy: LargeValuePolicy::application_v1(),
            collection_catalog: CollectionCatalog::new(),
            durable_collections: CollectionCatalog::new(),
            tier_placement: TierPlacement::new(),
            segment_catalog: SegmentCatalog::new(),
            catalog_seals_since_checkpoint: 0,
            catalog_dirty: false,
            last_catalog_checkpoint_at: Instant::now(),
            writer_lock: Some(writer_lock),
            write_dedup: WriteDedupTable::new(),
            write_dedup_recovery_required: false,
            operation_cohort_gathering: false,
            seal_pipeline: Some(SealPipeline::start()),
            async_lifecycle: true,
            enrichment_enabled: true,
            shadow_dual_stream: false,
            shadow_dual_finalize_ns: 0,
            shadow_dual_published: 0,
            recovery_mode: crate::recovery_shadow::RecoveryMode::Materialized,
            accept_foreign_store_id: false,
            rotation_stage_totals: RotationStageTotals::default(),
            enrichment_stage_totals: EnrichmentStageTotals::default(),
            chunk_locators: HashMap::new(),
            token_keyring,
            boundary_probe: crate::boundary_probe::BoundaryProbe::disabled(),
            diagnostic_io: DiagnosticIoSink::Real,
            null_io_file: None,
            diagnostic_skip_index: false,
            diagnostic_skip_append_frame: false,
            diagnostic_skip_blake: false,
            cook_parallelism: 1,
            segment_growth: crate::segment_growth::SegmentGrowthPolicy::GrowOnAppend,
            awo_writer_poisoned: false,
            awo_lease_active: false,
        };
        store.accept_foreign_store_id = matches!(
            options.inventory_policy,
            crate::media_inventory::InventoryPolicy::TolerateUnidentified
        );
        if let Some(pipe) = store.seal_pipeline.as_mut() {
            pipe.max_pending_seals = DEFAULT_MAX_PENDING_SEALS.saturating_mul(writer_shards.max(1));
        }
        let phase = Instant::now();
        store.load_tier_state()?;
        open_metrics.tier_state_ns = elapsed_ns(phase);
        // P0: inventory authoritative media and refuse collisions **before**
        // pending recovery, index rebuild, or any overwrite-capable mutation.
        let phase = Instant::now();
        let inventory = crate::media_inventory::inventory_authoritative_media(
            &store.paths,
            store.store_id,
            store.writer_shards,
            store.limits,
            options.inventory_policy,
        )?;
        open_metrics.inventory_ns = elapsed_ns(phase);
        open_metrics.inventory_descriptor_probe_bytes = inventory.descriptor_probe_bytes;
        open_metrics.inventory_fallback_scan_bytes = inventory.fallback_scan_bytes;
        // Finish any pending seals left by a prior crash before index rebuild.
        let phase = Instant::now();
        let pending_recovered = recover_all_pending(&store.paths, store.store_id, store.limits)?;
        open_metrics.pending_recovery_ns = elapsed_ns(phase);
        open_metrics.pending_seals_recovered = pending_recovered as u64;
        // Protected seal-pair: finish auth+Shadow+frontier for crash mid-pair.
        let phase = Instant::now();
        let protected_recovered =
            crate::protected_pair::recover_protected_pairs(&store.paths, store.store_id)?;
        open_metrics.protected_pair_recovery_ns = elapsed_ns(phase);
        open_metrics.protected_pairs_recovered = protected_recovered as u64;
        let phase = Instant::now();
        let index_stats = store.load_or_rebuild_index()?;
        open_metrics.index_ns = elapsed_ns(phase);
        open_metrics.index_full_scan_bytes = index_stats.full_scan_bytes;
        open_metrics.index_active_replay_bytes = index_stats.active_replay_bytes;
        open_metrics.chunk_locators_from_checkpoint = index_stats.chunk_locators_from_checkpoint;
        open_metrics.index_disposition = index_stats.disposition;
        open_metrics.index_cache_decision = index_stats.cache_decision;
        open_metrics.index_cache_bytes = index_stats.cache_bytes;
        open_metrics.index_cache_decode_ns = index_stats.cache_decode_ns;
        open_metrics.index_install_clone_ns = index_stats.install_clone_ns;
        open_metrics.index_catalog_derive_ns = index_stats.catalog_derive_ns;
        open_metrics.index_entries = index_stats.index_entries;
        open_metrics.chunk_locator_entries = index_stats.chunk_locator_entries;
        open_metrics.index_segments_examined = index_stats.segments_examined;
        let phase = Instant::now();
        store.load_or_rebuild_catalog()?;
        open_metrics.catalog_ns = elapsed_ns(phase);
        // Durable segment-id high water (never-reuse). Reconstructs above every
        // active/pending/sealed/shadow/chimera id; refuses if ambiguous.
        let accept_foreign = matches!(
            options.inventory_policy,
            crate::media_inventory::InventoryPolicy::TolerateUnidentified
        );
        let phase = Instant::now();
        store.segment_seq = if accept_foreign {
            crate::segment_allocator::reconstruct_reserved_thru_with_policy(
                &store.paths,
                store.store_id,
                store.writer_shards,
                store.limits,
                true,
            )?
        } else {
            crate::segment_allocator::reconstruct_reserved_thru(
                &store.paths,
                store.store_id,
                store.writer_shards,
                store.limits,
            )?
        };
        open_metrics.allocator_ns = elapsed_ns(phase);
        let phase = Instant::now();
        let (write_dedup, write_dedup_journal_complete) =
            load_write_dedup_checked(&write_dedup_path(&store.paths))?;
        store.write_dedup = write_dedup;
        open_metrics.dedup_ns = elapsed_ns(phase);
        let phase = Instant::now();
        store.resume_or_start_all_actives()?;
        open_metrics.active_resume_ns = elapsed_ns(phase);
        // Finish or cancel incomplete compaction jobs (DEF-024).
        let phase = Instant::now();
        let compaction_jobs = store.recover_compact_jobs()?;
        open_metrics.compaction_recovery_ns = elapsed_ns(phase);
        open_metrics.compaction_jobs_examined = compaction_jobs.len() as u64;
        // Step 8: re-arm dual-stream / reclaim policy from durable marker.
        let phase = Instant::now();
        store.reload_recovery_mode()?;
        open_metrics.recovery_mode_ns = elapsed_ns(phase);
        open_metrics.total_ns = elapsed_ns(open_started);
        store.open_metrics = open_metrics;
        store.write_dedup_recovery_required =
            !preceding_writer_was_clean || !write_dedup_journal_complete;
        // Establish the crash sentinel before this writer is returned to an
        // application. It is changed back to clean only by an orderly drop
        // after any required reconciliation has completed.
        mark_write_dedup_session_dirty(&store.paths)?;
        Ok(store)
    }

    /// Non-blocking open of an **existing** store (never creates).
    ///
    /// Returns [`StoreError::NotAStore`] when the path is not a store, and
    /// structured [`StoreError::WriterLockHeld`] on contention (DEF-101).
    pub fn try_open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let root = path.as_ref();
        let paths = StorePaths::new(root);
        if !paths.looks_like_store() {
            return Err(StoreError::NotAStore(root.to_path_buf()));
        }
        Self::open_with_options(root, StoreOpenOptions::non_blocking())
    }

    /// Observe writer-lock status without opening the store (DEF-101).
    ///
    /// Diagnostic only. Free OS lock may still show stale PID text — that text
    /// is advisory and does not hold the lock.
    pub fn writer_lock_status(path: impl AsRef<Path>) -> Result<WriterLockObservation, StoreError> {
        let root = path.as_ref();
        let paths = StorePaths::new(root);
        if !paths.looks_like_store() {
            return Err(StoreError::NotAStore(root.to_path_buf()));
        }
        Ok(WriterLock::observe(&paths))
    }

    /// Active continuation-token key generation id (DEF-097). No secret material.
    pub fn continuation_key_generation(&self) -> u32 {
        self.token_keyring.active_generation_id()
    }

    /// Rotate the continuation-token secret (DEF-097).
    ///
    /// Previous generation remains accepted for verify until
    /// [`Self::retire_previous_continuation_key`] or a second rotate. Requires
    /// writer ownership so the keyring file can be persisted.
    pub fn rotate_continuation_keys(&mut self) -> Result<u32, StoreError> {
        if self.writer_lock.is_none() {
            return Err(StoreError::CorruptMeta(
                "continuation key rotate requires writer open",
            ));
        }
        let id = self.token_keyring.rotate()?;
        self.token_keyring.save_store(&self.paths)?;
        Ok(id)
    }

    /// Drop previous continuation key generation (end grace). Writer only.
    pub fn retire_previous_continuation_key(&mut self) -> Result<(), StoreError> {
        if self.writer_lock.is_none() {
            return Err(StoreError::CorruptMeta(
                "continuation key retire requires writer open",
            ));
        }
        self.token_keyring.retire_previous();
        self.token_keyring.save_store(&self.paths)?;
        Ok(())
    }

    /// Open an **existing** store for read-only inspection (Stage 7 doctor).
    ///
    /// Never creates a store, never opens the active segment for append, and
    /// never persists derived catalogs/indexes. Primary index and collection
    /// catalog are rebuilt in memory from authoritative segment bytes when
    /// needed. Suitable for `residiuum doctor` (DX_SPEC §13.3). Does **not** take
    /// the exclusive writer lock, so it can run while a writer holds the store.
    pub fn open_inspect(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let root = path.as_ref();
        let paths = StorePaths::new(root);
        if !paths.looks_like_store() {
            return Err(StoreError::NotAStore(root.to_path_buf()));
        }

        let store_id = read_store_id(&paths)?;
        let meta = fs::read_to_string(paths.meta_file()).unwrap_or_default();
        if !meta.starts_with("residiuum-store-") {
            return Err(StoreError::CorruptMeta("unexpected meta version"));
        }
        verify_store_descriptor_if_present(&paths, store_id)?;

        let writer_shards = read_writer_shards(&paths)?;
        let token_keyring = ContinuationKeyring::load_or_mint_store(&paths)?;
        let mut store = Self {
            paths,
            store_id,
            limits: SafetyLimits::default(),
            open_metrics: StoreOpenMetrics::default(),
            index: PrimaryIndex::new(),
            durable_index: PrimaryIndex::new(),
            derived_ops_since_checkpoint: 0,
            actives: (0..writer_shards).map(|_| None).collect(),
            writer_shards,
            segment_seq: 0,
            seal_threshold: DEFAULT_SEAL_THRESHOLD,
            chunk_threshold: DEFAULT_CHUNK_THRESHOLD,
            chunk_size: DEFAULT_CHUNK_SIZE,
            large_value_policy: LargeValuePolicy::application_v1(),
            collection_catalog: CollectionCatalog::new(),
            durable_collections: CollectionCatalog::new(),
            tier_placement: TierPlacement::new(),
            segment_catalog: SegmentCatalog::new(),
            catalog_seals_since_checkpoint: 0,
            catalog_dirty: false,
            last_catalog_checkpoint_at: Instant::now(),
            writer_lock: None,
            write_dedup: WriteDedupTable::new(),
            write_dedup_recovery_required: false,
            operation_cohort_gathering: false,
            seal_pipeline: None,
            async_lifecycle: false,
            enrichment_enabled: false,
            shadow_dual_stream: false,
            shadow_dual_finalize_ns: 0,
            shadow_dual_published: 0,
            recovery_mode: crate::recovery_shadow::RecoveryMode::Materialized,
            accept_foreign_store_id: false,
            rotation_stage_totals: RotationStageTotals::default(),
            enrichment_stage_totals: EnrichmentStageTotals::default(),
            chunk_locators: HashMap::new(),
            token_keyring,
            boundary_probe: crate::boundary_probe::BoundaryProbe::disabled(),
            diagnostic_io: DiagnosticIoSink::Real,
            null_io_file: None,
            diagnostic_skip_index: false,
            diagnostic_skip_append_frame: false,
            diagnostic_skip_blake: false,
            cook_parallelism: 1,
            segment_growth: crate::segment_growth::SegmentGrowthPolicy::GrowOnAppend,
            awo_writer_poisoned: false,
            awo_lease_active: false,
        };
        store.load_tier_state_readonly()?;
        // Memory-only index: prefer frontier/v1 cache, else rebuild without writing.
        store.load_or_rebuild_index_readonly()?;
        let seg_paths = all_segment_paths(
            &store.paths,
            Some(&store.tier_placement),
            store.writer_shards,
        )?;
        let fp = segment_fingerprint(&seg_paths)?;
        // Catalog: load if valid, else rebuild in memory only (no write).
        let cat_path = crate::catalog::collections_catalog_path(&store.paths.catalogs_dir());
        if let Some(cat) = try_load_collection_catalog(&cat_path, store.store_id, fp)? {
            store.durable_collections = cat.clone();
            store.collection_catalog = cat;
        } else {
            store.recompute_collection_catalogs_from_index();
        }
        // Intentionally no resume_or_start_active — no writer handle.
        Ok(store)
    }

    /// Enable store-boundary I/O instrumentation (PQH harness). Default off.
    pub fn enable_boundary_probe(&mut self) {
        self.boundary_probe.enable();
    }

    /// Opt-in product segment growth policy (default grow-on-append).
    ///
    /// Watermark mode: OS preallocate + **same-fd** full-capacity zero before puts.
    /// Capacity/chunk are host knobs (default 64 MiB). **Not default-on** (space amp).
    /// Puts only consume ready runway (fail closed if empty) — first-touch is not on
    /// the put path. Ignored while a non-Real [`DiagnosticIoSink`] is active.
    /// Applies immediately to already-open active writers when set.
    pub fn set_segment_growth_policy(
        &mut self,
        policy: crate::segment_growth::SegmentGrowthPolicy,
    ) -> Result<(), StoreError> {
        if !policy.is_watermark() {
            self.stop_all_runway_preparers();
        }
        self.segment_growth = policy;
        if policy.is_watermark() && self.diagnostic_io == DiagnosticIoSink::Real {
            self.apply_product_growth_to_existing_actives()?;
            self.attach_runway_preparers()?;
        }
        Ok(())
    }

    /// Current product segment growth policy.
    pub fn segment_growth_policy(&self) -> crate::segment_growth::SegmentGrowthPolicy {
        self.segment_growth
    }

    /// Block until background preparers have zeroed through each active's capacity
    /// (or `thru_bytes` if smaller). Intended for thr setup **before** the put timer.
    ///
    /// No-op when watermark growth is off. Fail closed on timeout (120s default).
    pub fn warm_segment_runway(&mut self) -> Result<(), StoreError> {
        self.warm_segment_runway_thru(u64::MAX)
    }

    /// Like [`Self::warm_segment_runway`], but stop once each active is zeroed through
    /// at least `thru_bytes` (clamped to capacity).
    ///
    /// Uses the **writer file handle** (same-fd) so first-touch lands in the page
    /// cache the put path will append through. Updates any attached preparer's
    /// shared watermarks afterward.
    pub fn warm_segment_runway_thru(&mut self, thru_bytes: u64) -> Result<(), StoreError> {
        let crate::segment_growth::SegmentGrowthPolicy::Watermark {
            capacity_bytes,
            chunk_bytes,
        } = self.segment_growth
        else {
            return Ok(());
        };
        let want = thru_bytes.min(capacity_bytes);
        let n = self.writer_shards();
        for shard in 0..n {
            let Some(writer) = self.active_mut(shard) else {
                continue;
            };
            // Same-fd warm: do not rely on the preparer's separate open for the
            // bytes the writer will overwrite (APFS / page-cache honesty).
            crate::segment_growth::ensure_zero_watermark(
                &mut writer.file,
                &mut writer.zeroed_thru,
                want.max(writer.durable_len),
                capacity_bytes,
                chunk_bytes,
            )?;
            writer
                .file
                .seek(std::io::SeekFrom::Start(writer.durable_len))?;
            if let Some(runway) = writer.runway.as_ref() {
                runway
                    .shared()
                    .write_head
                    .store(writer.durable_len, std::sync::atomic::Ordering::Release);
                runway
                    .shared()
                    .zeroed_thru
                    .store(writer.zeroed_thru, std::sync::atomic::Ordering::Release);
            }
        }
        Ok(())
    }

    /// Diagnostic only: detach or redirect segment tail I/O for bisection.
    ///
    /// See [`DiagnosticIoSink`]. Does **not** change CSQ durability labels on
    /// receipts — only where bytes go. Never enable in product paths.
    pub fn set_diagnostic_io_sink(&mut self, sink: DiagnosticIoSink) -> Result<(), StoreError> {
        self.diagnostic_io = sink;
        match sink {
            DiagnosticIoSink::DevNull => {
                if self.null_io_file.is_none() {
                    self.null_io_file = Some(
                        OpenOptions::new()
                            .write(true)
                            .open("/dev/null")
                            .map_err(StoreError::from)?,
                    );
                }
            }
            DiagnosticIoSink::Real
            | DiagnosticIoSink::Discard
            | DiagnosticIoSink::Coalesce100k
            | DiagnosticIoSink::SeekOnly
            | DiagnosticIoSink::RealNoSeek
            | DiagnosticIoSink::RealOverwrite
            | DiagnosticIoSink::RealPrealloc
            | DiagnosticIoSink::RealPreallocFill
            | DiagnosticIoSink::RealPreallocFcntl
            | DiagnosticIoSink::RealPreallocZero
            | DiagnosticIoSink::RealPreallocWatermark => {
                self.null_io_file = None;
            }
        }
        // peer-pump sets sink after create — pre-size any already-open actives.
        if matches!(
            sink,
            DiagnosticIoSink::RealPrealloc
                | DiagnosticIoSink::RealPreallocFill
                | DiagnosticIoSink::RealPreallocFcntl
                | DiagnosticIoSink::RealPreallocZero
                | DiagnosticIoSink::RealPreallocWatermark
        ) {
            self.prealloc_existing_actives()?;
        }
        Ok(())
    }

    /// Diagnostic: apply prealloc to already-open active writers (post-create sink set).
    fn prealloc_existing_actives(&mut self) -> Result<(), StoreError> {
        const BYTES: u64 = 512 * 1024 * 1024;
        const CHUNK: u64 = 64 * 1024 * 1024;
        let mode = self.diagnostic_io;
        let n = self.writer_shards();
        for shard in 0..n {
            let Some(writer) = self.active_mut(shard) else {
                continue;
            };
            match mode {
                DiagnosticIoSink::RealPrealloc => {
                    let cur = writer.file.metadata()?.len();
                    if cur < BYTES {
                        writer.file.set_len(BYTES)?;
                    }
                }
                DiagnosticIoSink::RealPreallocFill => {
                    let cur = writer.file.metadata()?.len();
                    if cur < BYTES {
                        writer.file.set_len(BYTES)?;
                    }
                    let mut off = 0u64;
                    let one = [0u8; 1];
                    while off < BYTES {
                        writer.file.seek(SeekFrom::Start(off))?;
                        writer.file.write_all(&one)?;
                        off = off.saturating_add(1024 * 1024);
                    }
                    writer.zeroed_thru = BYTES;
                }
                DiagnosticIoSink::RealPreallocFcntl => {
                    Self::diag_os_preallocate(&writer.file, BYTES)?;
                    writer.file.set_len(BYTES)?;
                }
                DiagnosticIoSink::RealPreallocZero => {
                    Self::diag_os_preallocate(&writer.file, BYTES)?;
                    writer.file.set_len(BYTES)?;
                    Self::diag_bulk_zero_range(&mut writer.file, 0, BYTES)?;
                    writer.zeroed_thru = BYTES;
                }
                DiagnosticIoSink::RealPreallocWatermark => {
                    Self::diag_os_preallocate(&writer.file, BYTES)?;
                    writer.file.set_len(BYTES)?;
                    // Never clobber the live durable prefix (descriptor + frames).
                    // Zeroing from 0 destroyed the on-disk descriptor; peer-pump
                    // ignores seal errors, so end-of-run seal failed closed and
                    // inflated diagnostic watermark ops/s (~32k cheat).
                    if writer.zeroed_thru < writer.durable_len {
                        writer.zeroed_thru = writer.durable_len;
                    }
                    let need = writer.durable_len.saturating_add(CHUNK).min(BYTES);
                    Self::diag_ensure_zero_watermark(writer, need, BYTES)?;
                }
                _ => {}
            }
            writer.file.seek(SeekFrom::Start(writer.durable_len))?;
        }
        Ok(())
    }

    /// Current diagnostic I/O sink (default [`DiagnosticIoSink::Real`]).
    pub fn diagnostic_io_sink(&self) -> DiagnosticIoSink {
        self.diagnostic_io
    }

    /// Diagnostic only: when true, durable puts append+write but **skip** dual-index
    /// publish, collection catalog, and rate-limited derived checkpoints.
    ///
    /// Use with real disk to bisect **indexing** vs **data cooking** (encode/append).
    pub fn set_diagnostic_skip_index(&mut self, skip: bool) {
        self.diagnostic_skip_index = skip;
    }

    /// Whether index publish is skipped (diagnostic).
    pub fn diagnostic_skip_index(&self) -> bool {
        self.diagnostic_skip_index
    }

    /// Diagnostic only: skip frame encode/append (Blake + segment buffer).
    ///
    /// Put still runs prep + envelope encode (+ optional index). No new segment
    /// bytes; used to short-circuit `append_frame` for phase-bench ceilings.
    pub fn set_diagnostic_skip_append_frame(&mut self, skip: bool) {
        self.diagnostic_skip_append_frame = skip;
    }

    /// Whether frame append is skipped (diagnostic).
    pub fn diagnostic_skip_append_frame(&self) -> bool {
        self.diagnostic_skip_append_frame
    }

    /// Diagnostic only: skip BLAKE3 body hash while still copying body into the
    /// segment and writing the tail. Isolates Blake vs rest of append_frame.
    pub fn set_diagnostic_skip_blake(&mut self, skip: bool) {
        self.diagnostic_skip_blake = skip;
        residiuum_format::set_diagnostic_skip_body_hash(skip);
    }

    /// Whether Blake body hash is short-circuited (diagnostic).
    pub fn diagnostic_skip_blake(&self) -> bool {
        self.diagnostic_skip_blake
    }

    /// Parallel cooker worker count for single-shard [`Self::put_many`].
    ///
    /// Values `< 1` clamp to `1`. Cooks full records (envelope + frame + Blake)
    /// on a pool; ordered install + one tail write. Default `1` (serial).
    pub fn set_cook_parallelism(&mut self, workers: usize) {
        self.cook_parallelism = workers.max(1);
    }

    /// Whether batch/adaptive writer is poisoned (AWO-1 uncertain I/O).
    ///
    /// When true, mutations return [`StoreError::AdaptiveWriterPoisoned`] until
    /// the store is closed and reopened.
    pub fn is_awo_writer_poisoned(&self) -> bool {
        self.awo_writer_poisoned
    }

    /// Whether an adaptive-write lease fences direct mutation (AWO-3).
    pub fn is_awo_lease_active(&self) -> bool {
        self.awo_lease_active
    }

    /// Set or clear the adaptive-write lease fence (AWO runtime only).
    pub fn set_awo_lease_active(&mut self, active: bool) {
        self.awo_lease_active = active;
    }

    /// Current cook parallelism (`1` = serial).
    pub fn cook_parallelism(&self) -> usize {
        self.cook_parallelism.max(1)
    }

    /// Enable boundary probe with an explicit sample-vector capacity.
    pub fn enable_boundary_probe_with_capacity(&mut self, max_samples: usize) {
        self.boundary_probe.enable_with_capacity(max_samples);
    }

    /// Whether boundary instrumentation is recording.
    pub fn boundary_probe_enabled(&self) -> bool {
        self.boundary_probe.is_enabled()
    }

    /// Borrow retained sample events (may be incomplete when capped).
    pub fn boundary_events(&self) -> &[crate::boundary_probe::BoundaryEvent] {
        self.boundary_probe.events()
    }

    /// Exact counters / histograms / chain digest / coverage (authoritative).
    pub fn boundary_snapshot(&self) -> crate::boundary_probe::BoundarySnapshot {
        self.boundary_probe.snapshot()
    }

    /// Drain sample events only (counters/histograms/digest remain until clear).
    pub fn take_boundary_events(&mut self) -> Vec<crate::boundary_probe::BoundaryEvent> {
        self.boundary_probe.take_events()
    }

    /// Take a full snapshot and clear probe aggregates (probe stays enabled).
    pub fn take_boundary_snapshot(&mut self) -> crate::boundary_probe::BoundarySnapshot {
        self.boundary_probe.take_snapshot()
    }

    /// Number of writer shards (DEF-096 Axis B). Always ≥ 1.
    pub fn writer_shards(&self) -> usize {
        self.writer_shards.max(1)
    }

    /// Home shard for `subject` bytes (stable BLAKE3-derived).
    pub fn subject_shard(&self, subject: &[u8]) -> usize {
        subject_writer_shard(subject, self.writer_shards())
    }

    /// Writer model label for benchmark disclosure.
    pub fn writer_model(&self) -> &'static str {
        if self.writer_shards() <= 1 {
            "single_active_segment"
        } else {
            "sharded_active_segments"
        }
    }

    /// Store root path.
    pub fn path(&self) -> &Path {
        &self.paths.root
    }

    /// Store identifier.
    pub fn store_id(&self) -> [u8; 16] {
        self.store_id
    }

    /// Diagnostic breakdown of the successful open that created this handle.
    /// Newly created and read-only inspection handles currently report zeros.
    pub fn open_metrics(&self) -> StoreOpenMetrics {
        self.open_metrics
    }

    /// Structured startup disposition, recovery actions, counts, and timings.
    pub fn open_report(&self) -> StoreOpenReport {
        self.open_metrics
    }

    /// Number of live (non-deleted) subjects in the primary index.
    pub fn live_count(&self) -> usize {
        self.index.live_len()
    }

    /// Number of subjects with any recorded state (including delete tombstones).
    pub fn tracked_count(&self) -> usize {
        self.index.len()
    }

    /// Iterate live subjects and **resident** bodies (derived primary index).
    ///
    /// After DEF-095, ordinary durable values may have an empty resident body
    /// (locator-only). Chunk manifests remain resident. Prefer
    /// [`Self::live_logical_entries`] / [`Self::get`] for application payloads.
    pub fn live_entries(&self) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
        self.index
            .live_entries()
            .map(|(k, v)| (k.as_slice(), v.body.as_slice()))
    }

    /// Approximate resident primary-index body bytes (excludes on-disk payloads).
    pub fn resident_index_body_bytes(&self) -> u64 {
        self.index.resident_body_bytes()
    }

    /// Live subjects with logical payloads fully reassembled when chunked.
    ///
    /// **Fail-closed (DEF-012):** if any live subject has a partial, conflicting,
    /// or unavailable payload, returns [`StoreError::CoverageIncomplete`] rather
    /// than silently omitting those subjects. Use [`Self::scan_live_logical`] for
    /// an explicit partial-aware envelope, or [`Self::get_payload`] for one key.
    pub fn live_logical_entries(&self) -> Result<crate::compact::CheckpointPairs, StoreError> {
        let scan = self.scan_live_logical()?;
        if !scan.complete {
            let mut reasons = Vec::new();
            if !scan.incomplete.is_empty() {
                reasons.push(format!(
                    "{} live subject(s) have incomplete payloads",
                    scan.incomplete.len()
                ));
            }
            if scan.tier_coverage_incomplete {
                reasons.push("offline or unavailable storage tier(s)".into());
            }
            return Err(StoreError::CoverageIncomplete(format!(
                "{}; use scan_live_logical or get_payload for partial maps",
                reasons.join("; ")
            )));
        }
        Ok(scan.entries)
    }

    /// Scan live logical payloads with explicit incompleteness (DEF-012).
    ///
    /// Always returns every complete reassembly and lists incomplete subjects.
    /// `complete` is true only when every live subject produced a full body
    /// **and** tier coverage has no offline/unavailable segments.
    ///
    /// **Memory:** materializes the full live set. Prefer
    /// [`Self::scan_live_page`] for bounded-memory scans (DEF-026).
    pub fn scan_live_logical(&self) -> Result<LiveLogicalScan, StoreError> {
        let mut opts = crate::cursor::LiveScanPageOptions::new(crate::cursor::MAX_PAGE_SIZE);
        // Drain all pages without holding a giant intermediate only for subjects —
        // still assembles the full result (legacy API contract).
        let mut entries = Vec::new();
        let mut incomplete = Vec::new();
        let mut tier_coverage_incomplete = false;
        let mut cont: Option<Vec<u8>> = None;
        loop {
            opts.continuation = cont.take();
            let page = self.scan_live_page(&opts)?;
            entries.extend(page.entries);
            incomplete.extend(page.incomplete);
            tier_coverage_incomplete |= page.tier_coverage_incomplete;
            if !page.has_more {
                break;
            }
            cont = page.continuation;
            // Keep prefix if set (none for full scan).
            opts.page_size = crate::cursor::MAX_PAGE_SIZE;
        }
        let complete = incomplete.is_empty() && !tier_coverage_incomplete;
        Ok(LiveLogicalScan {
            entries,
            incomplete,
            complete,
            tier_coverage_incomplete,
        })
    }

    /// One bounded page of live logical payloads (DEF-026).
    ///
    /// Reads at most `options.page_size` complete bodies. Subject order is
    /// ascending. Pass the returned `continuation` token to resume; tokens are
    /// MAC-authenticated to this store and fenced by scan generation.
    ///
    /// Incomplete subjects encountered on the page are reported in
    /// `incomplete` and still advance the cursor so scans make forward progress.
    pub fn scan_live_page(
        &self,
        options: &crate::cursor::LiveScanPageOptions,
    ) -> Result<crate::cursor::LiveScanPage, StoreError> {
        use crate::cursor::{
            decode_token, encode_token, incomplete, scan_generation, CursorState, LiveScanPage,
            DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE,
        };

        let page_size = if options.page_size == 0 {
            DEFAULT_PAGE_SIZE
        } else {
            options.page_size.min(MAX_PAGE_SIZE)
        };

        let seg_fp = self.segment_fingerprint()?;
        let live_count = self.index.live_len() as u64;
        let generation = scan_generation(&self.store_id, &seg_fp, live_count);

        let (prefix, after, token_page_size) = if let Some(ref tok) = options.continuation {
            let state = decode_token(&self.store_id, &self.token_keyring, tok)?;
            if state.generation != generation {
                return Err(StoreError::CursorStale(
                    "scan generation changed (live set or segment fingerprint); restart scan"
                        .into(),
                ));
            }
            // Prefix is fixed for the lifetime of the cursor.
            if let (Some(ref want), Some(ref got)) = (&options.prefix, &state.prefix) {
                if want != got {
                    return Err(StoreError::CursorInvalid(
                        "continuation prefix does not match request".into(),
                    ));
                }
            }
            let prefix = state.prefix.clone().or_else(|| options.prefix.clone());
            (prefix, state.after, state.page_size.clamp(1, MAX_PAGE_SIZE))
        } else {
            (options.prefix.clone(), None, page_size)
        };

        // Prefer token page size on resume so clients cannot silently widen.
        let page_size = if options.continuation.is_some() {
            token_page_size
        } else {
            page_size
        };

        let mut entries = Vec::new();
        let mut incomplete_list = Vec::new();
        let mut examined = 0usize;
        let mut last_subject: Option<Vec<u8>> = after.clone();
        let mut saw_more = false;

        // Bound work per page: page_size complete bodies, or a cap of examined
        // subjects when many are incomplete (forward progress without O(n) bodies).
        let max_examine = page_size.saturating_mul(8).max(page_size);
        let mut iter = self
            .index
            .live_entries_after(after.as_deref(), prefix.as_deref());

        loop {
            if entries.len() >= page_size || examined >= max_examine {
                saw_more = iter.next().is_some();
                break;
            }
            let Some((subject_ref, _)) = iter.next() else {
                break;
            };
            let subject = subject_ref.clone();
            examined += 1;
            last_subject = Some(subject.clone());
            let subject_str = match std::str::from_utf8(&subject) {
                Ok(s) => s,
                Err(_) => {
                    incomplete_list.push(incomplete(subject, IncompleteReason::NonUtf8Subject));
                    continue;
                }
            };
            match self.get_payload(subject_str) {
                Ok(None) => {}
                Ok(Some(PayloadResult::Complete { body })) => entries.push((subject, body)),
                Ok(Some(PayloadResult::Partial { .. })) => {
                    incomplete_list.push(incomplete(subject, IncompleteReason::PayloadPartial));
                }
                Ok(Some(PayloadResult::Unavailable { .. })) => {
                    incomplete_list.push(incomplete(subject, IncompleteReason::PayloadUnavailable));
                }
                Ok(Some(PayloadResult::Conflicting { .. })) => {
                    incomplete_list.push(incomplete(subject, IncompleteReason::PayloadConflict));
                }
                // Locator / media failures: fail-closed as incomplete, not a hard scan abort.
                Err(StoreError::SegmentNotFound) | Err(StoreError::TierOffline(_)) => {
                    incomplete_list.push(incomplete(subject, IncompleteReason::SegmentNotFound));
                }
                Err(StoreError::LocatorFault(f)) => {
                    let reason = match f.kind {
                        crate::error::LocatorFaultKind::OffsetInvalid => {
                            IncompleteReason::LocatorOffsetInvalid
                        }
                        crate::error::LocatorFaultKind::FrameVerifyFailed => {
                            IncompleteReason::LocatorFrameVerifyFailed
                        }
                        crate::error::LocatorFaultKind::SegmentIdMismatch => {
                            IncompleteReason::LocatorSegmentIdMismatch
                        }
                        crate::error::LocatorFaultKind::SegmentNotFound => {
                            IncompleteReason::SegmentNotFound
                        }
                    };
                    incomplete_list.push(incomplete(subject, reason));
                }
                Err(e) => return Err(e),
            }
        }

        let tier_coverage_incomplete = self.tier_coverage().is_incomplete();
        let continuation = if saw_more {
            let state = CursorState {
                generation,
                prefix: prefix.clone(),
                after: last_subject,
                page_size,
            };
            Some(encode_token(&self.store_id, &self.token_keyring, &state)?)
        } else {
            None
        };

        let complete = !saw_more && incomplete_list.is_empty() && !tier_coverage_incomplete;
        Ok(LiveScanPage {
            entries,
            incomplete: incomplete_list,
            complete,
            tier_coverage_incomplete,
            has_more: saw_more,
            continuation,
            examined,
        })
    }

    /// One bounded page of live **subject keys** without body reassembly (DEF-100).
    ///
    /// Uses the same generation-fenced continuation tokens as
    /// [`Self::scan_live_page`]. Never resolves payloads — a missing chunk cannot
    /// suppress a verified key. `coverage_complete` is false when offline tiers
    /// (or other key-bearing authority gaps) prevent proving the key set is full.
    pub fn scan_live_keys_page(
        &self,
        options: &crate::cursor::LiveScanPageOptions,
    ) -> Result<crate::cursor::KeyScanPage, StoreError> {
        use crate::cursor::{
            decode_token, encode_token, scan_generation, CoverageGap, CoverageGapKind, CursorState,
            KeyScanPage, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE,
        };

        let page_size = if options.page_size == 0 {
            DEFAULT_PAGE_SIZE
        } else {
            options.page_size.min(MAX_PAGE_SIZE)
        };

        let seg_fp = self.segment_fingerprint()?;
        let live_count = self.index.live_len() as u64;
        let generation = scan_generation(&self.store_id, &seg_fp, live_count);

        let (prefix, after, token_page_size) = if let Some(ref tok) = options.continuation {
            let state = decode_token(&self.store_id, &self.token_keyring, tok)?;
            if state.generation != generation {
                return Err(StoreError::CursorStale(
                    "scan generation changed (live set or segment fingerprint); restart scan"
                        .into(),
                ));
            }
            if let (Some(ref want), Some(ref got)) = (&options.prefix, &state.prefix) {
                if want != got {
                    return Err(StoreError::CursorInvalid(
                        "continuation prefix does not match request".into(),
                    ));
                }
            }
            let prefix = state.prefix.clone().or_else(|| options.prefix.clone());
            (prefix, state.after, state.page_size.clamp(1, MAX_PAGE_SIZE))
        } else {
            (options.prefix.clone(), None, page_size)
        };

        let page_size = if options.continuation.is_some() {
            token_page_size
        } else {
            page_size
        };

        let mut keys = Vec::new();
        let mut examined = 0usize;
        let mut last_subject: Option<Vec<u8>> = after.clone();
        let mut saw_more = false;

        let mut iter = self
            .index
            .live_entries_after(after.as_deref(), prefix.as_deref());

        loop {
            if keys.len() >= page_size {
                saw_more = iter.next().is_some();
                break;
            }
            let Some((subject_ref, _)) = iter.next() else {
                break;
            };
            let subject = subject_ref.clone();
            examined += 1;
            last_subject = Some(subject.clone());
            keys.push(subject);
        }

        let tier_coverage_incomplete = self.tier_coverage().is_incomplete();
        let mut coverage_gaps = Vec::new();
        if tier_coverage_incomplete {
            coverage_gaps.push(CoverageGap {
                kind: CoverageGapKind::TierUnavailable,
                detail: "one or more storage tiers offline or unavailable".into(),
            });
        }

        let continuation = if saw_more {
            let state = CursorState {
                generation,
                prefix: prefix.clone(),
                after: last_subject,
                page_size,
            };
            Some(encode_token(&self.store_id, &self.token_keyring, &state)?)
        } else {
            None
        };

        let coverage_complete = !saw_more && coverage_gaps.is_empty();
        Ok(KeyScanPage {
            keys,
            continuation,
            has_more: saw_more,
            coverage_complete,
            coverage_gaps,
            examined,
            tier_coverage_incomplete,
        })
    }

    /// One bounded page of live documents with per-key body outcomes (DEF-100).
    ///
    /// Complete bodies appear in `rows`; verified keys with damaged bodies appear
    /// in `incomplete`. Key coverage is independent of body completeness.
    pub fn scan_live_documents_page(
        &self,
        options: &crate::cursor::LiveScanPageOptions,
    ) -> Result<crate::cursor::DocumentScanPage, StoreError> {
        use crate::cursor::{CoverageGap, CoverageGapKind, DocumentScanPage};

        let page = self.scan_live_page(options)?;
        let mut coverage_gaps = Vec::new();
        if page.tier_coverage_incomplete {
            coverage_gaps.push(CoverageGap {
                kind: CoverageGapKind::TierUnavailable,
                detail: "one or more storage tiers offline or unavailable".into(),
            });
        }
        let bytes_examined = page
            .entries
            .iter()
            .map(|(_, b)| b.len() as u64)
            .fold(0u64, u64::saturating_add);
        // Key coverage ignores body incompletes; only authority gaps matter.
        let key_coverage_complete = !page.has_more && coverage_gaps.is_empty();
        Ok(DocumentScanPage {
            rows: page.entries,
            incomplete: page.incomplete,
            key_coverage_complete,
            coverage_gaps,
            tier_coverage_incomplete: page.tier_coverage_incomplete,
            has_more: page.has_more,
            continuation: page.continuation,
            examined: page.examined,
            bytes_examined,
        })
    }

    /// Whether this handle holds exclusive writer ownership (DEF-020).
    pub fn holds_writer_lock(&self) -> bool {
        self.writer_lock.is_some()
    }

    /// Put opaque bytes under `subject` (OVERVIEW put event).
    ///
    /// Bodies larger than the chunk threshold are stored as chunked payloads
    /// (FORMAT_SPEC §8). The primary index retains the chunk manifest; get
    /// reassembles surviving chunks.
    ///
    /// With [`Self::create_with_shards`] / multi-shard open, the subject is
    /// routed to its home writer shard (DEF-096 Axis B).
    pub fn put(
        &mut self,
        subject: &str,
        value: &[u8],
        mode: DurabilityMode,
    ) -> Result<WriteReceipt, StoreError> {
        self.put_subject_bytes(subject.as_bytes(), value, mode)
    }

    /// Put opaque bytes under a **binary** subject (SubjectV2 or legacy v1 bytes).
    ///
    /// Prefer this on the qualified heap path: SubjectV2 keys contain raw UUIDs
    /// and are not valid UTF-8, so the string [`Self::put`] API cannot represent them.
    pub fn put_subject_bytes(
        &mut self,
        subject: &[u8],
        value: &[u8],
        mode: DurabilityMode,
    ) -> Result<WriteReceipt, StoreError> {
        self.refuse_direct_mutation_if_awo()?;
        self.put_subject_bytes_if(subject, value, mode, WriteCondition::Unconditional)
    }

    /// Conditional put: check [`WriteCondition`] then mint/append under the same
    /// exclusive writer path (APB-2 Key Atomic).
    pub fn put_subject_bytes_if(
        &mut self,
        subject: &[u8],
        value: &[u8],
        mode: DurabilityMode,
        condition: WriteCondition,
    ) -> Result<WriteReceipt, StoreError> {
        self.refuse_direct_mutation_if_awo()?;
        self.put_subject_bytes_if_awo_owned(subject, value, mode, condition)
    }

    /// Put under adaptive lease (skips lease fence; still honors poison).
    pub fn put_subject_bytes_if_awo_owned(
        &mut self,
        subject: &[u8],
        value: &[u8],
        mode: DurabilityMode,
        condition: WriteCondition,
    ) -> Result<WriteReceipt, StoreError> {
        self.put_subject_bytes_if_awo_owned_with_identity(subject, value, mode, condition, None)
    }

    fn put_subject_bytes_if_awo_owned_with_identity(
        &mut self,
        subject: &[u8],
        value: &[u8],
        mode: DurabilityMode,
        condition: WriteCondition,
        identity: Option<MutationIdentity>,
    ) -> Result<WriteReceipt, StoreError> {
        if self.awo_writer_poisoned {
            return Err(StoreError::AdaptiveWriterPoisoned);
        }
        self.check_write_condition(subject, condition)?;
        // DEF-103: admit against the effective profile before any event mint /
        // append / derived effect. Also respect scanner body ceiling.
        let effective_max = self
            .large_value_policy
            .effective_with(self.limits.max_body_len);
        if value.len() as u64 > effective_max {
            return Err(StoreError::PayloadTooLarge);
        }
        let admit = self.large_value_policy.admit(value.len())?;
        // Memory mode is visibility-only: keep the full body in the index and
        // never append frames (avoids later durable flushes contaminating disk).
        if mode == DurabilityMode::Memory {
            return Ok(self
                .write_event(subject, EventKind::Put, value, mode, identity)?
                .with_layout(admit, &self.large_value_policy.profile_id));
        }
        let receipt = if admit.layout == PayloadLayout::Chunked {
            self.write_chunked_put(subject, value, mode, identity)?
        } else {
            self.write_event(subject, EventKind::Put, value, mode, identity)?
        };
        Ok(receipt.with_layout(admit, &self.large_value_policy.profile_id))
    }

    /// Live establishing event id for `subject`, or `None` when absent/tombstoned.
    pub fn live_event_id(&self, subject: &[u8]) -> Option<[u8; 16]> {
        match self.index.get(subject) {
            Some(IndexEntry::Live(lv)) => Some(lv.event_id),
            _ => None,
        }
    }

    fn check_write_condition(
        &self,
        subject: &[u8],
        condition: WriteCondition,
    ) -> Result<(), StoreError> {
        let observed = self.live_event_id(subject);
        match condition {
            WriteCondition::Unconditional => Ok(()),
            WriteCondition::Absent => {
                if observed.is_some() {
                    Err(StoreError::KeyExists)
                } else {
                    Ok(())
                }
            }
            WriteCondition::Present => {
                if observed.is_some() {
                    Ok(())
                } else {
                    Err(StoreError::VersionConflict {
                        expected: [0u8; 16],
                        observed: None,
                    })
                }
            }
            WriteCondition::LiveEventId(expected) => match observed {
                Some(live) if live == expected => Ok(()),
                other => Err(StoreError::VersionConflict {
                    expected,
                    observed: other,
                }),
            },
        }
    }

    /// Put many items, partitioning by subject hash across writer shards.
    ///
    /// When `writer_shards > 1` and items are non-chunked buffered/durable puts,
    /// shard appends run in parallel (`std::thread::scope`) then the primary
    /// index is published serially. Single-shard non-chunked buffered/durable
    /// batches append many frames then issue **one** segment tail write (syscall
    /// amortization). Memory mode or any chunked body fall back to sequential
    /// [`Self::put`] (DEF-096 Axis B).
    pub fn put_many(
        &mut self,
        items: &[(&str, &[u8])],
        mode: DurabilityMode,
    ) -> Result<Vec<WriteReceipt>, StoreError> {
        self.refuse_direct_mutation_if_awo()?;
        self.put_many_awo_owned(items, mode)
    }

    /// Batch put under adaptive lease (skips lease fence; still honors poison).
    ///
    /// Uses the same persist-before-publish / parallel-cook paths as [`Self::put_many`].
    pub fn put_many_awo_owned(
        &mut self,
        items: &[(&str, &[u8])],
        mode: DurabilityMode,
    ) -> Result<Vec<WriteReceipt>, StoreError> {
        if self.awo_writer_poisoned {
            return Err(StoreError::AdaptiveWriterPoisoned);
        }
        if items.is_empty() {
            return Ok(Vec::new());
        }
        // DEF-103: fail closed before any parallel work if any item is over limit.
        let effective_max = self
            .large_value_policy
            .effective_with(self.limits.max_body_len);
        for (_, value) in items {
            if value.len() as u64 > effective_max {
                return Err(StoreError::PayloadTooLarge);
            }
            let _ = self.large_value_policy.admit(value.len())?;
        }
        let non_chunked = items.iter().all(|(_, b)| b.len() <= self.chunk_threshold);
        // Memory / chunked: per-item lease-owned put (not public put — lease fence).
        if mode == DurabilityMode::Memory || !non_chunked {
            let mut out = Vec::with_capacity(items.len());
            for (subject, value) in items {
                out.push(self.put_subject_bytes_if_awo_owned(
                    subject.as_bytes(),
                    value,
                    mode,
                    WriteCondition::Unconditional,
                )?);
            }
            return Ok(out);
        }
        if self.writer_shards() > 1 {
            return self.put_many_parallel(items, mode);
        }
        // Single active segment: batch appends + one write_segment_tail.
        self.put_many_single_shard_batched(items, mode)
    }

    /// Batch put under lease with raw subject bytes (independent-write collection).
    ///
    /// Same persist-before-publish semantics as [`Self::put_many_awo_owned`].
    pub fn put_many_subject_bytes_awo_owned(
        &mut self,
        items: &[(&[u8], &[u8])],
        mode: DurabilityMode,
    ) -> Result<Vec<WriteReceipt>, StoreError> {
        if self.awo_writer_poisoned {
            return Err(StoreError::AdaptiveWriterPoisoned);
        }
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let effective_max = self
            .large_value_policy
            .effective_with(self.limits.max_body_len);
        for (_, value) in items {
            if value.len() as u64 > effective_max {
                return Err(StoreError::PayloadTooLarge);
            }
            let _ = self.large_value_policy.admit(value.len())?;
        }
        let non_chunked = items.iter().all(|(_, b)| b.len() <= self.chunk_threshold);
        if mode == DurabilityMode::Memory || !non_chunked {
            let mut out = Vec::with_capacity(items.len());
            for (subject, value) in items {
                out.push(self.put_subject_bytes_if_awo_owned(
                    subject,
                    value,
                    mode,
                    WriteCondition::Unconditional,
                )?);
            }
            return Ok(out);
        }
        if self.writer_shards() > 1 {
            // Multi-shard: sequential lease puts (still correct; residual parallel).
            let mut out = Vec::with_capacity(items.len());
            for (subject, value) in items {
                out.push(self.put_subject_bytes_if_awo_owned(
                    subject,
                    value,
                    mode,
                    WriteCondition::Unconditional,
                )?);
            }
            return Ok(out);
        }
        self.put_many_single_shard_batched_bytes(items, mode)
    }

    /// Single-shard batched put: N in-memory appends, one file tail write.
    ///
    /// This is the dominant throughput path for testrig / bulk loaders. Per-put
    /// `write_all`+`seek` was leaving SSD and CPU idle (~1/3 of sequential ceiling).
    ///
    /// With [`Self::set_cook_parallelism`] `> 1`, full record cooking (env + Blake
    /// + frame encode) runs on a worker pool; frames install in order (Option C).
    ///
    /// **AWO-1:** index publication runs only after the segment tail write succeeds
    /// (persist-before-publish). On pre-I/O failure the segment is restored from
    /// checkpoint and nothing is published.
    fn put_many_single_shard_batched_bytes(
        &mut self,
        items: &[(&[u8], &[u8])],
        mode: DurabilityMode,
    ) -> Result<Vec<WriteReceipt>, StoreError> {
        debug_assert_eq!(self.writer_shards(), 1);
        debug_assert_ne!(mode, DurabilityMode::Memory);

        let workers = self.cook_parallelism();
        // When cook_parallelism > 1 and the batch has ≥2 UTF-8 subjects, reuse the
        // str parallel-cook path (AWO collector flushes are bytes-keyed).
        if workers > 1 && items.len() >= 2 {
            let mut owned: Vec<(String, &[u8])> = Vec::with_capacity(items.len());
            let mut utf8_ok = true;
            for (subject, value) in items {
                match std::str::from_utf8(subject) {
                    Ok(s) => owned.push((s.to_string(), *value)),
                    Err(_) => {
                        utf8_ok = false;
                        break;
                    }
                }
            }
            if utf8_ok {
                let refs: Vec<(&str, &[u8])> =
                    owned.iter().map(|(s, v)| (s.as_str(), *v)).collect();
                return self.put_many_single_shard_parallel_cook(&refs, mode, workers);
            }
        }

        let mut pending: Vec<StagedPut> = Vec::with_capacity(items.len());
        let mut batch_checkpoint: Option<residiuum_format::ActiveSegmentCheckpoint> = None;
        // Accumulate across auto-seal boundaries. Mid-batch
        // `finish_staged_batch_persist_publish` must not drop receipts — AWO
        // collectors zip one receipt per enqueued put (Q1.3 concurrent rotate).
        let mut receipts: Vec<WriteReceipt> = Vec::with_capacity(items.len());

        for (subject, value) in items {
            let admit = self.large_value_policy.admit(value.len())?;
            let subject_bytes = *subject;
            if subject_bytes.len() > MAX_SUBJECT_LEN {
                return Err(StoreError::SubjectTooLong {
                    max: MAX_SUBJECT_LEN,
                });
            }
            if value.len() as u64 > self.limits.max_body_len {
                return Err(StoreError::PayloadTooLarge);
            }

            self.ensure_active(0)?;
            let need_seal = self
                .active_ref(0)
                .map(|w| w.segment.len() >= self.seal_threshold)
                .unwrap_or(false);
            if need_seal {
                let sealed = self.finish_staged_batch_persist_publish(
                    &mut pending,
                    &mut batch_checkpoint,
                    mode,
                )?;
                receipts.extend(sealed);
                self.maybe_auto_seal(0)?;
            }

            let segment_id = self
                .active_ref(0)
                .map(|w| w.segment_id)
                .expect("active segment");
            if batch_checkpoint.is_none() {
                batch_checkpoint = self.active_ref(0).map(|w| w.segment.checkpoint());
            }
            let item_id = match self.index.get(subject_bytes) {
                Some(entry) => entry.item_id(),
                None => subject_item_id(subject_bytes),
            };
            let event_id = self.next_event_id()?;
            let env = ItemEnvelope {
                store_id: self.store_id,
                segment_id,
                item_id,
                event_kind: EventKind::Put,
                created_ns: now_ns(),
                subject: subject_bytes.to_vec(),
                operation_id: None,
                operation_content_hash: None,
            };
            let t_enc = std::time::Instant::now();
            let envelope = encode_item_envelope(&env).map_err(StoreError::BadEnvelope)?;
            let encode_ns = t_enc.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            if self.boundary_probe_enabled() {
                self.boundary_probe.record_encode_envelope(
                    envelope.len() as u64,
                    encode_ns,
                    mode,
                    0,
                );
            }
            if !self
                .limits
                .accepts_lengths(envelope.len() as u32, value.len() as u64)
            {
                return Err(StoreError::PayloadTooLarge);
            }

            crate::failpoint::hit("awo.install.frame.before")?;
            let (offset, encoded_frame_len, append_ns) = {
                let writer = self.active_mut(0).expect("active segment");
                let t_append = std::time::Instant::now();
                let offset =
                    writer
                        .segment
                        .append(FrameKind::ItemEvent, &envelope, value, event_id)?;
                writer.item_events = writer.item_events.saturating_add(1);
                let append_ns = t_append.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                let encoded_frame_len = writer.segment.len().saturating_sub(offset);
                (offset, encoded_frame_len, append_ns)
            };
            crate::failpoint::hit("awo.install.frame.after")?;

            self.boundary_probe.record_append(
                encoded_frame_len,
                value.len() as u64,
                offset,
                mode,
                false,
                false,
                0,
                append_ns,
                0,
            );

            pending.push(StagedPut {
                subject: subject_bytes.to_vec(),
                item_id,
                event_id,
                segment_id,
                offset,
                encoded_frame_len,
                admit,
                profile_id: self.large_value_policy.profile_id.clone(),
            });
        }

        let final_receipts =
            self.finish_staged_batch_persist_publish(&mut pending, &mut batch_checkpoint, mode)?;
        receipts.extend(final_receipts);
        debug_assert_eq!(receipts.len(), items.len());
        Ok(receipts)
    }

    /// Persist staged frames then publish indexes (AWO-1 persist-before-publish).

    fn put_many_single_shard_batched(
        &mut self,
        items: &[(&str, &[u8])],
        mode: DurabilityMode,
    ) -> Result<Vec<WriteReceipt>, StoreError> {
        debug_assert_eq!(self.writer_shards(), 1);
        debug_assert_ne!(mode, DurabilityMode::Memory);

        let workers = self.cook_parallelism();
        if workers > 1 && items.len() >= 2 {
            return self.put_many_single_shard_parallel_cook(items, mode, workers);
        }

        let mut pending: Vec<StagedPut> = Vec::with_capacity(items.len());
        let mut batch_checkpoint: Option<residiuum_format::ActiveSegmentCheckpoint> = None;
        let mut receipts: Vec<WriteReceipt> = Vec::with_capacity(items.len());

        for (subject, value) in items {
            let admit = self.large_value_policy.admit(value.len())?;
            let subject_bytes = subject.as_bytes();
            if subject_bytes.len() > MAX_SUBJECT_LEN {
                return Err(StoreError::SubjectTooLong {
                    max: MAX_SUBJECT_LEN,
                });
            }
            if value.len() as u64 > self.limits.max_body_len {
                return Err(StoreError::PayloadTooLarge);
            }

            self.ensure_active(0)?;
            let need_seal = self
                .active_ref(0)
                .map(|w| w.segment.len() >= self.seal_threshold)
                .unwrap_or(false);
            if need_seal {
                let sealed = self.finish_staged_batch_persist_publish(
                    &mut pending,
                    &mut batch_checkpoint,
                    mode,
                )?;
                receipts.extend(sealed);
                self.maybe_auto_seal(0)?;
            }

            let segment_id = self
                .active_ref(0)
                .map(|w| w.segment_id)
                .expect("active segment");
            if batch_checkpoint.is_none() {
                batch_checkpoint = self.active_ref(0).map(|w| w.segment.checkpoint());
            }
            let item_id = match self.index.get(subject_bytes) {
                Some(entry) => entry.item_id(),
                None => subject_item_id(subject_bytes),
            };
            let event_id = self.next_event_id()?;
            let env = ItemEnvelope {
                store_id: self.store_id,
                segment_id,
                item_id,
                event_kind: EventKind::Put,
                created_ns: now_ns(),
                subject: subject_bytes.to_vec(),
                operation_id: None,
                operation_content_hash: None,
            };
            let t_enc = std::time::Instant::now();
            let envelope = encode_item_envelope(&env).map_err(StoreError::BadEnvelope)?;
            let encode_ns = t_enc.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            if self.boundary_probe_enabled() {
                self.boundary_probe.record_encode_envelope(
                    envelope.len() as u64,
                    encode_ns,
                    mode,
                    0,
                );
            }
            if !self
                .limits
                .accepts_lengths(envelope.len() as u32, value.len() as u64)
            {
                return Err(StoreError::PayloadTooLarge);
            }

            crate::failpoint::hit("awo.install.frame.before")?;
            let (offset, encoded_frame_len, append_ns) = {
                let writer = self.active_mut(0).expect("active segment");
                let t_append = std::time::Instant::now();
                let offset =
                    writer
                        .segment
                        .append(FrameKind::ItemEvent, &envelope, value, event_id)?;
                writer.item_events = writer.item_events.saturating_add(1);
                let append_ns = t_append.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                let encoded_frame_len = writer.segment.len().saturating_sub(offset);
                (offset, encoded_frame_len, append_ns)
            };
            crate::failpoint::hit("awo.install.frame.after")?;

            self.boundary_probe.record_append(
                encoded_frame_len,
                value.len() as u64,
                offset,
                mode,
                false,
                false,
                0,
                append_ns,
                0,
            );

            pending.push(StagedPut {
                subject: subject_bytes.to_vec(),
                item_id,
                event_id,
                segment_id,
                offset,
                encoded_frame_len,
                admit,
                profile_id: self.large_value_policy.profile_id.clone(),
            });
        }

        let final_receipts =
            self.finish_staged_batch_persist_publish(&mut pending, &mut batch_checkpoint, mode)?;
        receipts.extend(final_receipts);
        debug_assert_eq!(receipts.len(), items.len());
        Ok(receipts)
    }

    /// Persist staged frames then publish indexes (AWO-1 persist-before-publish).
    fn finish_staged_batch_persist_publish(
        &mut self,
        pending: &mut Vec<StagedPut>,
        batch_checkpoint: &mut Option<residiuum_format::ActiveSegmentCheckpoint>,
        mode: DurabilityMode,
    ) -> Result<Vec<WriteReceipt>, StoreError> {
        if pending.is_empty() {
            *batch_checkpoint = None;
            return Ok(Vec::new());
        }

        crate::failpoint::hit("awo.persist.before")?;
        let sink = self.diagnostic_io;
        let growth = self.segment_growth;
        let mut null = self.null_io_file.take();
        let tail_result = {
            let writer = self
                .active_mut(0)
                .expect("active segment after staged batch");
            if mode != DurabilityMode::Memory {
                writer.max_ack_durability = stronger_durability(writer.max_ack_durability, mode);
            }
            Self::write_segment_tail(writer, mode, sink, null.as_mut(), growth)
        };
        let tail = match tail_result {
            Ok(stats) => stats,
            Err(e) => {
                if let Some(cp) = batch_checkpoint.take() {
                    if let Some(writer) = self.active_mut(0) {
                        let _ = writer.segment.restore_checkpoint(&cp);
                    }
                }
                self.null_io_file = null;
                pending.clear();
                return Err(e);
            }
        };
        self.null_io_file = null;
        if let Err(e) = self.record_tail_probe(&tail, mode, 0) {
            // Short write / uncertain I/O: do not restore; poison writer.
            *batch_checkpoint = None;
            pending.clear();
            self.awo_writer_poisoned = true;
            return Err(e);
        }
        crate::failpoint::hit("awo.persist.after_write")?;
        if mode == DurabilityMode::Durable {
            crate::failpoint::hit("awo.persist.after_sync")?;
        }

        crate::failpoint::hit("awo.publish.before")?;
        let mut out = Vec::with_capacity(pending.len());
        for p in pending.drain(..) {
            let t_pub = std::time::Instant::now();
            if !self.diagnostic_skip_index {
                self.apply_durable_event(
                    p.subject.clone(),
                    EventKind::Put,
                    Vec::new(),
                    p.item_id,
                    p.event_id,
                    p.segment_id,
                    0,
                    p.offset,
                );
                self.note_collection_for_subject(&p.subject);
                let _ = self.note_durable_derived();
            }
            let publish_ns = t_pub.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            self.boundary_probe
                .record_publish(p.offset, mode, 0, publish_ns);
            let mut receipt = WriteReceipt::base(
                self.store_id,
                p.segment_id,
                p.item_id,
                p.event_id,
                EventKind::Put,
                mode,
                p.offset,
            )
            .with_layout(p.admit, &p.profile_id);
            receipt.encoded_frame_len = p.encoded_frame_len;
            out.push(receipt);
        }
        crate::failpoint::hit("awo.publish.after")?;
        *batch_checkpoint = None;
        crate::failpoint::hit("awo.complete.before")?;
        Ok(out)
    }

    /// Parallel record cooker: full frames (env + Blake + encode) on `workers` threads.
    fn put_many_single_shard_parallel_cook(
        &mut self,
        items: &[(&str, &[u8])],
        mode: DurabilityMode,
        workers: usize,
    ) -> Result<Vec<WriteReceipt>, StoreError> {
        use std::sync::Mutex;
        use std::thread;

        self.ensure_active(0)?;
        self.maybe_auto_seal(0)?;

        let segment_id = self
            .active_ref(0)
            .map(|w| w.segment_id)
            .expect("active segment");
        let mut next_seq = self
            .active_ref(0)
            .map(|w| w.segment.writer_sequence())
            .expect("active segment");

        struct Prep {
            admit: AdmitDecision,
            subject: Vec<u8>,
            body: Vec<u8>,
            item_id: [u8; 16],
            event_id: [u8; 16],
            writer_sequence: u64,
            created_ns: u64,
            profile_id: String,
        }
        let profile = self.large_value_policy.profile_id.clone();
        let mut preps: Vec<Prep> = Vec::with_capacity(items.len());
        for (subject, value) in items {
            let admit = self.large_value_policy.admit(value.len())?;
            let subject_bytes = subject.as_bytes();
            if subject_bytes.len() > MAX_SUBJECT_LEN {
                return Err(StoreError::SubjectTooLong {
                    max: MAX_SUBJECT_LEN,
                });
            }
            if value.len() as u64 > self.limits.max_body_len {
                return Err(StoreError::PayloadTooLarge);
            }
            let item_id = match self.index.get(subject_bytes) {
                Some(entry) => entry.item_id(),
                None => subject_item_id(subject_bytes),
            };
            let event_id = self.next_event_id()?;
            let writer_sequence = next_seq;
            next_seq = next_seq.saturating_add(1);
            preps.push(Prep {
                admit,
                subject: subject_bytes.to_vec(),
                body: value.to_vec(),
                item_id,
                event_id,
                writer_sequence,
                created_ns: now_ns(),
                profile_id: profile.clone(),
            });
        }

        let store_id = self.store_id;
        let limits = self.limits;
        let n = preps.len();
        let workers = workers.min(n).max(1);

        struct Cooked {
            prep_idx: usize,
            frame: Vec<u8>,
        }
        let cooked: Mutex<Vec<Option<Result<Cooked, String>>>> =
            Mutex::new((0..n).map(|_| None).collect());

        thread::scope(|scope| {
            let chunk = (n + workers - 1) / workers;
            for w in 0..workers {
                let start = w * chunk;
                if start >= n {
                    break;
                }
                let end = (start + chunk).min(n);
                let preps_slice = &preps[start..end];
                let cooked = &cooked;
                scope.spawn(move || {
                    for (local_i, p) in preps_slice.iter().enumerate() {
                        let prep_idx = start + local_i;
                        let env = ItemEnvelope {
                            store_id,
                            segment_id,
                            item_id: p.item_id,
                            event_kind: EventKind::Put,
                            created_ns: p.created_ns,
                            subject: p.subject.clone(),
                            operation_id: None,
                            operation_content_hash: None,
                        };
                        let r = (|| {
                            let envelope = encode_item_envelope(&env).map_err(|e| e.to_string())?;
                            if !limits.accepts_lengths(envelope.len() as u32, p.body.len() as u64) {
                                return Err("payload too large".into());
                            }
                            let header = FrameHeader {
                                wire_major: WIRE_MAJOR,
                                wire_minor: WIRE_MINOR,
                                frame_kind: FrameKind::ItemEvent.as_u8(),
                                flags: Default::default(),
                                envelope_len: envelope.len() as u32,
                                body_len: p.body.len() as u64,
                                logical_len: p.body.len() as u64,
                                writer_sequence: p.writer_sequence,
                                event_id: p.event_id,
                            };
                            let mut frame =
                                Vec::with_capacity((p.body.len() + envelope.len() + 128).max(256));
                            encode_frame_into(&mut frame, &header, &envelope, &p.body)
                                .map_err(|e| e.to_string())?;
                            Ok(Cooked { prep_idx, frame })
                        })();
                        if let Ok(mut slot) = cooked.lock() {
                            slot[prep_idx] = Some(r);
                        }
                    }
                });
            }
        });

        let cooked_list = cooked
            .into_inner()
            .map_err(|_| StoreError::CorruptMeta("cook pool lock poisoned"))?;

        let batch_cp = self
            .active_ref(0)
            .map(|w| w.segment.checkpoint())
            .expect("active segment");
        let mut staged: Vec<StagedPut> = Vec::with_capacity(n);
        for (i, cooked_opt) in cooked_list.into_iter().enumerate() {
            let cooked_r = cooked_opt.ok_or(StoreError::CorruptMeta("cook slot empty"))?;
            let cooked = cooked_r.map_err(|e| {
                StoreError::Io(std::io::Error::other(format!("parallel cook: {e}")))
            })?;
            let p = &preps[i];
            debug_assert_eq!(cooked.prep_idx, i);

            let cur_seg = self.active_ref(0).map(|w| w.segment_id).expect("active");
            if cur_seg != segment_id {
                return Err(StoreError::CorruptMeta(
                    "segment rotated mid parallel cook install; retry batch",
                ));
            }

            crate::failpoint::hit("awo.install.frame.before")?;
            let (offset, encoded_frame_len) = {
                let writer = self.active_mut(0).expect("active");
                let offset = writer
                    .segment
                    .append_preencoded_frame(&cooked.frame)
                    .map_err(StoreError::from)?;
                writer.item_events = writer.item_events.saturating_add(1);
                (offset, cooked.frame.len() as u64)
            };
            crate::failpoint::hit("awo.install.frame.after")?;
            self.boundary_probe.record_append(
                encoded_frame_len,
                p.body.len() as u64,
                offset,
                mode,
                false,
                false,
                0,
                0,
                0,
            );

            staged.push(StagedPut {
                subject: p.subject.clone(),
                item_id: p.item_id,
                event_id: p.event_id,
                segment_id,
                offset,
                encoded_frame_len,
                admit: p.admit,
                profile_id: p.profile_id.clone(),
            });
        }

        let mut batch_checkpoint = Some(batch_cp);
        self.finish_staged_batch_persist_publish(&mut staged, &mut batch_checkpoint, mode)
    }

    /// Parallel multi-shard append path (non-chunked durable/buffered only).
    fn put_many_parallel(
        &mut self,
        items: &[(&str, &[u8])],
        mode: DurabilityMode,
    ) -> Result<Vec<WriteReceipt>, StoreError> {
        use std::thread;

        let n = self.writer_shards();
        // Ensure actives and honor auto-seal before taking writers out.
        for shard in 0..n {
            self.ensure_active(shard)?;
            self.maybe_auto_seal(shard)?;
        }

        // Partition item indices by home shard.
        let mut by_shard: Vec<Vec<usize>> = (0..n).map(|_| Vec::new()).collect();
        for (i, (subject, _)) in items.iter().enumerate() {
            let shard = self.subject_shard(subject.as_bytes());
            by_shard[shard].push(i);
        }

        // Pre-mint identities and encode envelopes while we still have &mut self.
        struct Prepared {
            item_idx: usize,
            subject: Vec<u8>,
            body: Vec<u8>,
            segment_id: [u8; 16],
            item_id: [u8; 16],
            event_id: [u8; 16],
            envelope: Vec<u8>,
        }
        let mut prepared: Vec<Vec<Prepared>> = (0..n).map(|_| Vec::new()).collect();
        for shard in 0..n {
            let segment_id = self
                .active_ref(shard)
                .map(|w| w.segment_id)
                .expect("active segment");
            for &item_idx in &by_shard[shard] {
                let (subject, body) = items[item_idx];
                let subject_bytes = subject.as_bytes();
                if subject_bytes.len() > MAX_SUBJECT_LEN {
                    return Err(StoreError::SubjectTooLong {
                        max: MAX_SUBJECT_LEN,
                    });
                }
                if body.len() as u64 > self.limits.max_body_len {
                    return Err(StoreError::PayloadTooLarge);
                }
                let item_id = match self.index.get(subject_bytes) {
                    Some(entry) => entry.item_id(),
                    None => subject_item_id(subject_bytes),
                };
                let event_id = self.next_event_id()?;
                let env = ItemEnvelope {
                    store_id: self.store_id,
                    segment_id,
                    item_id,
                    event_kind: EventKind::Put,
                    created_ns: now_ns(),
                    subject: subject_bytes.to_vec(),
                    operation_id: None,
                    operation_content_hash: None,
                };
                let envelope = encode_item_envelope(&env).map_err(StoreError::BadEnvelope)?;
                if !self
                    .limits
                    .accepts_lengths(envelope.len() as u32, body.len() as u64)
                {
                    return Err(StoreError::PayloadTooLarge);
                }
                prepared[shard].push(Prepared {
                    item_idx,
                    subject: subject_bytes.to_vec(),
                    body: body.to_vec(),
                    segment_id,
                    item_id,
                    event_id,
                    envelope,
                });
            }
        }

        // Pull writers out so each shard thread owns one ActiveWriter exclusively.
        // Mutex lets each scoped thread hold a unique shard without split_at_mut.
        let writers: Vec<std::sync::Mutex<Option<ActiveWriter>>> = (0..n)
            .map(|s| std::sync::Mutex::new(self.take_active(s)))
            .collect();
        let store_id = self.store_id;

        // Results per shard row: identity + append/tail timing for boundary probe.
        // Probe recording happens on the main thread after writers are restored so
        // probe-on and probe-off share this identical product path (DEF PQH-11).
        struct ShardRow {
            item_idx: usize,
            offset: u64,
            segment_id: [u8; 16],
            item_id: [u8; 16],
            event_id: [u8; 16],
            subject: Vec<u8>,
            body: Vec<u8>,
            encoded_frame_len: u64,
            append_ns: u64,
            tail: TailIoStats,
        }
        type ShardOut = Result<Vec<ShardRow>, StoreError>;
        let shard_outputs: Vec<std::sync::Mutex<ShardOut>> = (0..n)
            .map(|_| std::sync::Mutex::new(Ok(Vec::new())))
            .collect();
        let sink = self.diagnostic_io;
        let growth = self.segment_growth;

        thread::scope(|scope| {
            for shard in 0..n {
                let prep = std::mem::take(&mut prepared[shard]);
                if prep.is_empty() {
                    continue;
                }
                let writer_mu = &writers[shard];
                let out_mu = &shard_outputs[shard];
                scope.spawn(move || {
                    let mut writer_guard = writer_mu.lock().unwrap_or_else(|e| e.into_inner());
                    let Some(writer) = writer_guard.as_mut() else {
                        *out_mu.lock().unwrap_or_else(|e| e.into_inner()) =
                            Err(StoreError::CorruptMeta("missing active writer for shard"));
                        return;
                    };
                    // AWO-1: snapshot before any append so pre-I/O failure can roll back RAM.
                    let checkpoint = writer.segment.checkpoint();
                    let mut outs = Vec::with_capacity(prep.len());
                    for p in prep {
                        let t_append = std::time::Instant::now();
                        match writer.segment.append(
                            FrameKind::ItemEvent,
                            &p.envelope,
                            &p.body,
                            p.event_id,
                        ) {
                            Ok(offset) => {
                                writer.item_events = writer.item_events.saturating_add(1);
                                let append_ns = t_append
                                    .elapsed()
                                    .as_nanos()
                                    .min(u128::from(u64::MAX))
                                    as u64;
                                let encoded_frame_len =
                                    writer.segment.len().saturating_sub(offset);
                                outs.push(ShardRow {
                                    item_idx: p.item_idx,
                                    offset,
                                    segment_id: p.segment_id,
                                    item_id: p.item_id,
                                    event_id: p.event_id,
                                    subject: p.subject,
                                    body: p.body,
                                    encoded_frame_len,
                                    append_ns,
                                    tail: TailIoStats::default(),
                                });
                            }
                            Err(e) => {
                                let _ = writer.segment.restore_checkpoint(&checkpoint);
                                *out_mu.lock().unwrap_or_else(|err| err.into_inner()) =
                                    Err(StoreError::Segment(e));
                                return;
                            }
                        }
                    }
                    // One file tail write for the whole shard batch.
                    if !outs.is_empty() {
                        let mut local_null = if sink == DiagnosticIoSink::DevNull {
                            OpenOptions::new().write(true).open("/dev/null").ok()
                        } else {
                            None
                        };
                        match Self::write_segment_tail(writer, mode, sink, local_null.as_mut(), growth) {
                            Ok(tail) => {
                                if tail.fail_as_short_write {
                                    // Partial physical I/O: do not restore; leave uncertain.
                                    *out_mu.lock().unwrap_or_else(|e| e.into_inner()) =
                                        Err(StoreError::Io(std::io::Error::new(
                                            std::io::ErrorKind::WriteZero,
                                            "failpoint short write: store.active.write_tail.short_write",
                                        )));
                                    return;
                                }
                                if let Some(last) = outs.last_mut() {
                                    last.tail = tail;
                                }
                            }
                            Err(e) => {
                                // Clean pre/during-write failure: restore RAM.
                                let _ = writer.segment.restore_checkpoint(&checkpoint);
                                *out_mu.lock().unwrap_or_else(|err| err.into_inner()) = Err(e);
                                return;
                            }
                        }
                    }
                    *out_mu.lock().unwrap_or_else(|e| e.into_inner()) = Ok(outs);
                });
            }
        });

        // Restore writers before index publish / probe record / error return.
        for (shard, mu) in writers.into_iter().enumerate() {
            let w = mu.into_inner().unwrap_or_else(|e| e.into_inner());
            self.set_active(shard, w);
        }

        // AWO-1: all-or-nothing across shards — collect first, publish only if every
        // shard succeeded. Prevents partial index visibility when one shard fails.
        let mut collected: Vec<(u32, Vec<ShardRow>)> = Vec::with_capacity(n);
        let mut first_err: Option<StoreError> = None;
        let mut short_write_poison = false;
        for (shard, out_mu) in shard_outputs.into_iter().enumerate() {
            match out_mu.into_inner().unwrap_or_else(|e| e.into_inner()) {
                Ok(batch) => collected.push((shard as u32, batch)),
                Err(e) => {
                    if matches!(
                        e,
                        StoreError::Io(ref io)
                            if io.kind() == std::io::ErrorKind::WriteZero
                    ) {
                        short_write_poison = true;
                    }
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        if let Some(e) = first_err {
            if short_write_poison {
                self.awo_writer_poisoned = true;
            }
            return Err(e);
        }

        // Seal policy: any on-disk put_many upgrades max ack for shards that worked.
        if mode != DurabilityMode::Memory {
            for shard in 0..n {
                if by_shard[shard].is_empty() {
                    continue;
                }
                if let Some(w) = self.active_mut(shard) {
                    w.max_ack_durability = stronger_durability(w.max_ack_durability, mode);
                }
            }
        }

        let mut receipts: Vec<Option<WriteReceipt>> = (0..items.len()).map(|_| None).collect();
        for (shard_u32, batch) in collected {
            for row in batch {
                self.boundary_probe.record_append(
                    row.encoded_frame_len,
                    row.body.len() as u64,
                    row.offset,
                    mode,
                    false,
                    false,
                    0,
                    row.append_ns,
                    shard_u32,
                );
                // Tail was already recorded as Ok in the worker; re-emit probe only.
                if row.tail.write_requested > 0 || row.tail.write_completed > 0 {
                    self.boundary_probe.record_file_write(
                        row.tail.write_requested,
                        row.tail.write_completed,
                        row.tail.write_duration_ns,
                        row.tail.write_outcome,
                        mode,
                        shard_u32,
                    );
                }
                if row.tail.synced {
                    self.boundary_probe.record_file_sync(
                        row.tail.sync_duration_ns,
                        row.tail.sync_outcome,
                        mode,
                        shard_u32,
                    );
                }

                crate::failpoint::hit("awo.publish.before")?;
                let t_pub = std::time::Instant::now();
                if !self.diagnostic_skip_index {
                    self.apply_durable_event(
                        row.subject.clone(),
                        EventKind::Put,
                        Vec::new(),
                        row.item_id,
                        row.event_id,
                        row.segment_id,
                        0,
                        row.offset,
                    );
                    self.note_collection_for_subject(&row.subject);
                }
                let publish_ns = t_pub.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                self.boundary_probe
                    .record_publish(row.offset, mode, shard_u32, publish_ns);
                let admit = LargeValuePolicy::application_v1()
                    .admit(items[row.item_idx].1.len())
                    .unwrap_or(AdmitDecision {
                        layout: PayloadLayout::Inline,
                        logical_len: items[row.item_idx].1.len() as u64,
                        chunk_count: 0,
                    });
                let mut receipt = WriteReceipt::base(
                    store_id,
                    row.segment_id,
                    row.item_id,
                    row.event_id,
                    EventKind::Put,
                    mode,
                    row.offset,
                )
                .with_layout(admit, LARGE_VALUE_PROFILE_ID);
                receipt.encoded_frame_len = row.encoded_frame_len;
                receipts[row.item_idx] = Some(receipt);
            }
        }
        let _ = self.note_durable_derived();
        crate::failpoint::hit("awo.publish.after")?;
        crate::failpoint::hit("awo.complete.before")?;

        let mut out = Vec::with_capacity(items.len());
        for (i, r) in receipts.into_iter().enumerate() {
            out.push(r.ok_or_else(|| {
                StoreError::CorruptMeta(match items.get(i) {
                    Some(_) => "put_many missing receipt",
                    None => "put_many index OOB",
                })
            })?);
        }
        Ok(out)
    }

    /// Resolve a client operation id for idempotent remote writes (DEF-010).
    ///
    /// - `Ok(Some(receipt))` — exact retry; return the original receipt
    /// - `Ok(None)` — new operation; caller should perform the write then
    ///   [`Self::record_write_dedup`]
    /// - `Err(OperationIdentityConflict)` — id reused with different content
    pub fn resolve_write_dedup(
        &self,
        operation_id: &[u8; 16],
        content_hash: &[u8; 32],
    ) -> Result<Option<WriteReceipt>, StoreError> {
        match self.write_dedup.get(operation_id) {
            None => Ok(None),
            Some(rec) if &rec.content_hash == content_hash => Ok(Some(WriteReceipt::base(
                rec.store_id,
                rec.segment_id,
                rec.item_id,
                rec.event_id,
                rec.event_kind,
                rec.durability,
                rec.offset,
            ))),
            Some(_) => Err(StoreError::OperationIdentityConflict),
        }
    }

    /// Materialize every on-media operation decision into the durable ledger.
    ///
    /// Live-projection compaction may intentionally reclaim historical item
    /// events. Reconciliation before reclaim ensures those operation decisions
    /// remain available for exact retry after their source frames are removed.
    fn reconcile_write_dedup_from_media(&mut self) -> Result<(), StoreError> {
        for path in all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )? {
            let bytes = fs::read(path)?;
            let report = scan_forward(&bytes, self.limits);
            for (offset, frame) in report.verified_frames() {
                if frame.header.known_kind() != Some(FrameKind::ItemEvent) {
                    continue;
                }
                let Some(envelope) = decode_item_envelope(&frame.envelope) else {
                    continue;
                };
                if envelope.store_id != self.store_id {
                    continue;
                }
                let (Some(operation_id), Some(content_hash)) =
                    (envelope.operation_id, envelope.operation_content_hash)
                else {
                    continue;
                };
                match self.write_dedup.get(&operation_id) {
                    Some(record) if record.content_hash == content_hash => continue,
                    Some(_) => return Err(StoreError::OperationIdentityConflict),
                    None => {}
                }
                self.write_dedup.insert(
                    operation_id,
                    DedupRecord {
                        content_hash,
                        store_id: self.store_id,
                        segment_id: envelope.segment_id,
                        item_id: envelope.item_id,
                        event_id: frame.header.event_id,
                        event_kind: envelope.event_kind,
                        // Reclaim only follows durable compact output. The
                        // recovered decision is therefore durable now even if
                        // its initial acceptance requested a weaker mode.
                        durability: DurabilityMode::Durable,
                        offset,
                    },
                );
            }
        }
        // Persist even when every frame was already represented in memory: a
        // prior atomic-file failure can leave the in-memory table ahead of disk.
        save_write_dedup(&write_dedup_path(&self.paths), &self.write_dedup)?;
        rewrite_write_dedup_journal(&write_dedup_journal_path(&self.paths), &self.write_dedup)
    }

    /// Pay the authoritative-media reconciliation cost at most once after an
    /// unclean writer session, never once per new operation.
    fn ensure_write_dedup_reconciled(&mut self) -> Result<(), StoreError> {
        if self.write_dedup_recovery_required {
            self.reconcile_write_dedup_from_media()?;
            self.write_dedup_recovery_required = false;
        }
        Ok(())
    }

    /// Persist a successful mutation under `operation_id` (DEF-010).
    ///
    /// Called after the authoritative append so restart recovers the receipt.
    pub fn record_write_dedup(
        &mut self,
        operation_id: [u8; 16],
        content_hash: [u8; 32],
        receipt: &WriteReceipt,
    ) -> Result<(), StoreError> {
        let record = DedupRecord {
            content_hash,
            store_id: receipt.store_id,
            segment_id: receipt.segment_id,
            item_id: receipt.item_id,
            event_id: receipt.event_id,
            event_kind: receipt.event_kind,
            durability: receipt.durability,
            offset: receipt.offset,
        };
        if let Err(error) = append_write_dedup(
            &write_dedup_journal_path(&self.paths),
            operation_id,
            &record,
        ) {
            // The authoritative frame may already be durable. Do not publish a
            // clean session marker until its outcome has been reconstructed.
            self.write_dedup_recovery_required = true;
            return Err(error);
        }
        self.write_dedup.insert(operation_id, record);
        Ok(())
    }

    /// Atomically resolve or execute an idempotent conditional put.
    ///
    /// The caller must provide a canonical content identity covering the full
    /// logical request. Exact retries return the original receipt; conflicting
    /// reuse fails before another authoritative append.
    pub fn put_subject_bytes_with_operation(
        &mut self,
        subject: &[u8],
        body: &[u8],
        mode: DurabilityMode,
        condition: WriteCondition,
        operation_id: [u8; 16],
        content_hash: [u8; 32],
    ) -> Result<(WriteReceipt, bool), StoreError> {
        self.ensure_write_dedup_reconciled()?;
        if let Some(receipt) = self.resolve_write_dedup(&operation_id, &content_hash)? {
            return Ok((receipt, true));
        }
        self.refuse_direct_mutation_if_awo()?;
        let receipt = self.put_subject_bytes_if_awo_owned_with_identity(
            subject,
            body,
            mode,
            condition,
            Some((operation_id, content_hash)),
        )?;
        self.record_write_dedup(operation_id, content_hash, &receipt)?;
        Ok((receipt, false))
    }

    /// Commit independent operation-bearing puts behind shared stable boundaries.
    ///
    /// Requests retain individual conditions, identities, receipts and errors.
    /// Successful new writes append in buffered mode while this exclusive store
    /// handle prevents outside visibility, then one active-media sync upgrades
    /// every included receipt to Durable. Operation outcomes are then appended
    /// to a derived lookup journal without a second stable boundary; the
    /// authoritative item-event frames reconstruct that journal after a crash.
    pub fn put_operation_cohort_awo_owned(
        &mut self,
        items: &[OperationPut<'_>],
    ) -> Result<Vec<Result<OperationPutOutcome, StoreError>>, StoreError> {
        let mutations: Vec<_> = items
            .iter()
            .map(|item| OperationMutation {
                subject: item.subject,
                kind: OperationMutationKind::Put(item.body),
                condition: item.condition,
                operation_id: item.operation_id,
                content_hash: item.content_hash,
            })
            .collect();
        self.operation_cohort_awo_owned(&mutations)
    }

    /// Commit independent puts and deletes behind shared stable boundaries.
    pub fn operation_cohort_awo_owned(
        &mut self,
        items: &[OperationMutation<'_>],
    ) -> Result<Vec<Result<OperationPutOutcome, StoreError>>, StoreError> {
        if self.awo_writer_poisoned {
            return Err(StoreError::AdaptiveWriterPoisoned);
        }
        if items.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_write_dedup_reconciled()?;

        let mut outcomes: Vec<Option<Result<OperationPutOutcome, StoreError>>> =
            (0..items.len()).map(|_| None).collect();
        let mut owners = HashMap::<[u8; 16], usize>::new();
        let mut expected = HashMap::<[u8; 16], [u8; 32]>::new();
        let mut duplicates = Vec::<(usize, usize)>::new();

        for (index, item) in items.iter().enumerate() {
            match self.resolve_write_dedup(&item.operation_id, &item.content_hash) {
                Ok(Some(receipt)) => {
                    outcomes[index] = Some(Ok(OperationPutOutcome {
                        receipt,
                        deduplicated: true,
                    }));
                }
                Err(error) => outcomes[index] = Some(Err(error)),
                Ok(None) => {
                    if let Some(owner) = owners.get(&item.operation_id).copied() {
                        if expected[&item.operation_id] == item.content_hash {
                            duplicates.push((index, owner));
                        } else {
                            outcomes[index] = Some(Err(StoreError::OperationIdentityConflict));
                        }
                    } else {
                        owners.insert(item.operation_id, index);
                        expected.insert(item.operation_id, item.content_hash);
                    }
                }
            }
        }

        let mut records = Vec::<([u8; 16], DedupRecord)>::new();
        let mut new_indexes = Vec::<usize>::new();

        for owner in owners.values() {
            if outcomes[*owner].is_some() {
                continue;
            }
            new_indexes.push(*owner);
        }
        new_indexes.sort_unstable();

        self.operation_cohort_gathering = true;
        for index in new_indexes {
            let item = &items[index];
            let result = match item.kind {
                OperationMutationKind::Put(body) => self
                    .put_subject_bytes_if_awo_owned_with_identity(
                        item.subject,
                        body,
                        DurabilityMode::Buffered,
                        item.condition,
                        Some((item.operation_id, item.content_hash)),
                    ),
                OperationMutationKind::Delete => self
                    .delete_subject_bytes_if_awo_owned_with_identity(
                        item.subject,
                        DurabilityMode::Buffered,
                        item.condition,
                        Some((item.operation_id, item.content_hash)),
                    ),
            };
            match result {
                Ok(receipt) => {
                    outcomes[index] = Some(Ok(OperationPutOutcome {
                        receipt,
                        deduplicated: false,
                    }));
                }
                Err(error) if operation_request_error(&error) => {
                    outcomes[index] = Some(Err(error));
                }
                Err(error) => {
                    self.operation_cohort_gathering = false;
                    self.awo_writer_poisoned = true;
                    return Err(error);
                }
            }
        }
        self.operation_cohort_gathering = false;

        let has_new = outcomes.iter().any(|outcome| {
            matches!(
                outcome,
                Some(Ok(OperationPutOutcome {
                    deduplicated: false,
                    ..
                }))
            )
        });
        if has_new {
            if let Err(error) = self.persist_all_actives(DurabilityMode::Durable) {
                self.awo_writer_poisoned = true;
                return Err(error);
            }
            for writer in self.actives.iter_mut().flatten() {
                writer.max_ack_durability =
                    stronger_durability(writer.max_ack_durability, DurabilityMode::Durable);
            }
        }

        for (index, outcome) in outcomes.iter_mut().enumerate() {
            if let Some(Ok(value)) = outcome {
                if !value.deduplicated {
                    value.receipt.durability = DurabilityMode::Durable;
                    records.push((
                        items[index].operation_id,
                        dedup_record(items[index].content_hash, &value.receipt),
                    ));
                }
            }
        }

        let refs: Vec<_> = records.iter().map(|(id, record)| (*id, record)).collect();
        if append_write_dedup_batch_buffered(&write_dedup_journal_path(&self.paths), &refs).is_err()
        {
            // The authoritative frames already crossed their stable boundary.
            // Keep exact-retry state in memory and deliberately withhold a
            // clean-session certificate so restart reconciles from media.
            self.write_dedup_recovery_required = true;
        }
        for (operation_id, record) in records {
            self.write_dedup.insert(operation_id, record);
        }

        for (index, owner) in duplicates {
            outcomes[index] = Some(match outcomes[owner].as_ref() {
                Some(Ok(value)) => Ok(OperationPutOutcome {
                    receipt: value.receipt.clone(),
                    deduplicated: true,
                }),
                Some(Err(StoreError::KeyExists)) => Err(StoreError::KeyExists),
                Some(Err(StoreError::VersionConflict { expected, observed })) => {
                    Err(StoreError::VersionConflict {
                        expected: *expected,
                        observed: *observed,
                    })
                }
                Some(Err(StoreError::OperationIdentityConflict)) => {
                    Err(StoreError::OperationIdentityConflict)
                }
                _ => Err(StoreError::ConsistencyViolation(
                    "operation cohort duplicate owner failed without replayable outcome".into(),
                )),
            });
        }

        outcomes
            .into_iter()
            .map(|outcome| {
                outcome.ok_or_else(|| {
                    StoreError::ConsistencyViolation(
                        "operation cohort omitted an individual outcome".into(),
                    )
                })
            })
            .collect()
    }

    /// Atomically resolve or execute an idempotent conditional delete.
    pub fn delete_subject_bytes_with_operation(
        &mut self,
        subject: &[u8],
        mode: DurabilityMode,
        condition: WriteCondition,
        operation_id: [u8; 16],
        content_hash: [u8; 32],
    ) -> Result<(WriteReceipt, bool), StoreError> {
        self.ensure_write_dedup_reconciled()?;
        if let Some(receipt) = self.resolve_write_dedup(&operation_id, &content_hash)? {
            return Ok((receipt, true));
        }
        self.refuse_direct_mutation_if_awo()?;
        let receipt = self.delete_subject_bytes_if_awo_owned_with_identity(
            subject,
            mode,
            condition,
            Some((operation_id, content_hash)),
        )?;
        self.record_write_dedup(operation_id, content_hash, &receipt)?;
        Ok((receipt, false))
    }

    /// Live subjects after `after` with optional byte prefix (heap SubjectV2 scans).
    ///
    /// Returns owned subject keys only (bodies fetched separately via
    /// [`Self::get_subject_bytes`]). Used by capability-gated heap façades that
    /// cannot use the UTF-8 [`Self::scan_live_page`] path.
    pub fn index_live_after(&self, after: Option<&[u8]>, prefix: Option<&[u8]>) -> Vec<Vec<u8>> {
        self.index
            .live_entries_after(after, prefix)
            .map(|(s, _)| s.clone())
            .collect()
    }

    /// Get current live value for `subject`, if any.
    ///
    /// For chunked values this reassembles chunks and returns the complete body
    /// only when every required chunk is present. Use [`Self::get_payload`] for
    /// partial maps.
    pub fn get(&self, subject: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.get_subject_bytes(subject.as_bytes())
    }

    /// Get by binary subject (SubjectV2-capable).
    pub fn get_subject_bytes(&self, subject: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        match self.get_payload_bytes(subject)? {
            None => Ok(None),
            Some(PayloadResult::Complete { body }) => Ok(Some(body)),
            Some(PayloadResult::Partial { .. }) | Some(PayloadResult::Unavailable { .. }) => {
                // Do not silently return incomplete data as a full get.
                Err(StoreError::PayloadPartial)
            }
            // DEF-098: contradictory verified evidence is damage, not mere partial.
            Some(PayloadResult::Conflicting { .. }) => Err(StoreError::PayloadConflict),
        }
    }

    /// Get the current payload with explicit completeness (Stage 6 chunks).
    ///
    /// Returns `Ok(None)` when the subject has no live value. Inline (non-chunked)
    /// bodies always yield [`PayloadResult::Complete`].
    ///
    /// **Hot path (DEF-095):** one map lookup, then either a resident body clone
    /// (memory-mode / chunk manifest) or a **bounded frame pread** at
    /// `frame_offset`. Chimera seal layouts are **derived only** and must never
    /// sit in front of a resolvable locator — loading `indexes/chimera/*.cmr`
    /// on every get re-decodes a full segment placement and was measured as
    /// ~250 ms class latency on 1 GiB testrig samples.
    ///
    /// Chimera is a last-resort fallback when the index has neither a resident
    /// body nor a usable frame offset. See [`Self::get_via_chimera`].
    pub fn get_payload(&self, subject: &str) -> Result<Option<PayloadResult>, StoreError> {
        self.get_payload_bytes(subject.as_bytes())
    }

    /// Binary-subject payload lookup (SubjectV2-capable).
    pub fn get_payload_bytes(&self, subject: &[u8]) -> Result<Option<PayloadResult>, StoreError> {
        let key = subject;
        let Some(entry) = self.index.get(key) else {
            return Ok(None);
        };
        let crate::index::IndexEntry::Live(lv) = entry else {
            return Ok(None);
        };

        let body = self.resolve_live_value_body(key, lv)?;
        if body.is_empty() {
            if let Some(via) = self.try_get_via_chimera(key, &lv.segment_id)? {
                return Ok(Some(PayloadResult::Complete { body: via }));
            }
            return Ok(Some(PayloadResult::Complete { body }));
        }
        if !is_chunk_manifest(&body) {
            return Ok(Some(PayloadResult::Complete { body }));
        }
        let Some(manifest) = decode_chunk_manifest(&body) else {
            return Err(StoreError::CorruptMeta("invalid chunk manifest"));
        };
        // DEF-098: reassemble only the current generation's chunk_event_ids.
        let resolved = self.resolve_manifest_chunks(lv.item_id, &manifest)?;
        Ok(Some(reassemble_with_manifest(
            lv.item_id, &manifest, &resolved,
        )))
    }

    /// Resolve a subject exclusively through the Chimera layout for its live
    /// segment (diagnostic / future body-less path). Does **not** use the
    /// resident PrimaryIndex body. Returns `Ok(None)` when no live entry or
    /// no usable layout exists.
    pub fn get_via_chimera(&self, subject: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let key = subject.as_bytes();
        let Some(entry) = self.index.get(key) else {
            return Ok(None);
        };
        let crate::index::IndexEntry::Live(lv) = entry else {
            return Ok(None);
        };
        self.try_get_via_chimera(key, &lv.segment_id)
    }

    /// Record a logical delete for `subject`.
    pub fn delete(
        &mut self,
        subject: &str,
        mode: DurabilityMode,
    ) -> Result<WriteReceipt, StoreError> {
        self.delete_subject_bytes(subject.as_bytes(), mode)
    }

    /// Delete by binary subject (SubjectV2-capable).
    pub fn delete_subject_bytes(
        &mut self,
        subject: &[u8],
        mode: DurabilityMode,
    ) -> Result<WriteReceipt, StoreError> {
        self.refuse_direct_mutation_if_awo()?;
        self.delete_subject_bytes_if(subject, mode, WriteCondition::Unconditional)
    }

    /// Conditional delete under the exclusive writer path (APB-2 Key Atomic).
    pub fn delete_subject_bytes_if(
        &mut self,
        subject: &[u8],
        mode: DurabilityMode,
        condition: WriteCondition,
    ) -> Result<WriteReceipt, StoreError> {
        self.refuse_direct_mutation_if_awo()?;
        self.delete_subject_bytes_if_awo_owned(subject, mode, condition)
    }

    /// Delete under adaptive lease (skips lease fence; still honors poison).
    pub fn delete_subject_bytes_if_awo_owned(
        &mut self,
        subject: &[u8],
        mode: DurabilityMode,
        condition: WriteCondition,
    ) -> Result<WriteReceipt, StoreError> {
        self.delete_subject_bytes_if_awo_owned_with_identity(subject, mode, condition, None)
    }

    fn delete_subject_bytes_if_awo_owned_with_identity(
        &mut self,
        subject: &[u8],
        mode: DurabilityMode,
        condition: WriteCondition,
        identity: Option<MutationIdentity>,
    ) -> Result<WriteReceipt, StoreError> {
        if self.awo_writer_poisoned {
            return Err(StoreError::AdaptiveWriterPoisoned);
        }
        self.check_write_condition(subject, condition)?;
        self.write_event(subject, EventKind::Delete, &[], mode, identity)
    }

    #[inline]
    fn refuse_direct_mutation_if_awo(&self) -> Result<(), StoreError> {
        if self.awo_writer_poisoned {
            return Err(StoreError::AdaptiveWriterPoisoned);
        }
        if self.awo_lease_active {
            return Err(StoreError::AdaptiveWriterActive);
        }
        Ok(())
    }

    /// Event history for a subject key (oldest first; DX_SPEC §10.1).
    pub fn history(&self, subject: &str) -> Result<SubjectHistory, StoreError> {
        self.history_subject_bytes(subject.as_bytes())
    }

    /// Event history for a **binary** subject (SubjectV2-capable).
    pub fn history_subject_bytes(&self, subject: &[u8]) -> Result<SubjectHistory, StoreError> {
        subject_history_tiered(
            &self.paths,
            self.limits,
            subject,
            Some(&self.tier_placement),
        )
    }

    /// Reconstruct the logical payload of one exact historical event (DEF-099).
    ///
    /// Selection is by authoritative `event_id` only. Chunked puts use the
    /// DEF-098 generation-exact manifest path. Ordinary [`Self::get`] is
    /// unchanged and never falls back here. Read-only: no index or segment writes.
    pub fn get_payload_version(
        &self,
        subject: &str,
        event_id: &[u8; 16],
        budget: ReadBudget,
    ) -> Result<VersionedPayloadResult, StoreError> {
        self.get_payload_version_bytes(subject.as_bytes(), event_id, budget)
    }

    /// Binary-subject form of [`Self::get_payload_version`].
    pub fn get_payload_version_bytes(
        &self,
        subject: &[u8],
        event_id: &[u8; 16],
        budget: ReadBudget,
    ) -> Result<VersionedPayloadResult, StoreError> {
        let hist = self.history_subject_bytes(subject)?;
        let ev = hist
            .events
            .iter()
            .find(|e| e.event_id == *event_id)
            .ok_or(StoreError::HistoryEventNotFound)?;
        self.project_history_event(subject, &hist, ev, false, budget, 1)
    }

    /// Find the newest complete put strictly before `before` (DEF-099).
    ///
    /// Default options stop at the first delete tombstone so deleted values
    /// are not resurrected. Set `cross_tombstone` for labelled forensic search.
    pub fn find_last_complete_version(
        &self,
        subject: &str,
        before: BeforeEvent,
        options: RecoveryReadOptions,
    ) -> Result<HistoricalSearchResult, StoreError> {
        self.find_last_complete_version_bytes(subject.as_bytes(), before, options)
    }

    /// Binary-subject form of [`Self::find_last_complete_version`].
    pub fn find_last_complete_version_bytes(
        &self,
        subject: &[u8],
        before: BeforeEvent,
        options: RecoveryReadOptions,
    ) -> Result<HistoricalSearchResult, StoreError> {
        let hist = self.history_subject_bytes(subject)?;
        let history_coverage_complete = !hist.has_known_holes;

        let bound_id = match before {
            BeforeEvent::Current => self.index.get(subject).map(|e| match e {
                crate::index::IndexEntry::Live(lv) => lv.event_id,
                crate::index::IndexEntry::Deleted { event_id, .. } => *event_id,
            }),
            BeforeEvent::EventId(id) => Some(id),
        };

        // Recovery order is oldest-first; walk newest-first among permitted events.
        let mut end = hist.events.len();
        if let Some(bid) = bound_id {
            if let Some(pos) = hist.events.iter().position(|e| e.event_id == bid) {
                end = pos; // exclusive
            }
            // If bound not found and BeforeEvent::EventId, still search all older
            // by recovery order only among events that appear before first match;
            // when missing, treat as "search entire history" for Current-like use.
        }

        let mut events_examined = 0usize;
        let mut bytes_examined = 0u64;
        let mut incomplete_candidates = 0usize;
        let mut tombstone_stopped = false;
        let mut budget_exhausted = false;
        let mut tombstone_crossed = false;

        for ev in hist.events[..end].iter().rev() {
            events_examined = events_examined.saturating_add(1);
            if options.budget.max_events_examined > 0
                && events_examined > options.budget.max_events_examined
            {
                budget_exhausted = true;
                break;
            }

            match ev.kind {
                EventKind::Delete => {
                    if options.cross_tombstone {
                        tombstone_crossed = true;
                        continue;
                    }
                    tombstone_stopped = true;
                    break;
                }
                EventKind::Put => {
                    let projected = self.project_history_event(
                        subject,
                        &hist,
                        ev,
                        tombstone_crossed,
                        options.budget,
                        events_examined,
                    )?;
                    bytes_examined = bytes_examined.saturating_add(projected.bytes_examined);
                    if options.budget.max_bytes_examined > 0
                        && bytes_examined > options.budget.max_bytes_examined
                    {
                        budget_exhausted = true;
                        break;
                    }
                    match &projected.selected {
                        Some(PayloadResult::Complete { .. }) => {
                            return Ok(HistoricalSearchResult {
                                found: Some(projected),
                                incomplete_candidates,
                                tombstone_stopped: false,
                                budget_exhausted: false,
                                history_coverage_complete,
                                events_examined,
                                bytes_examined,
                            });
                        }
                        Some(_) | None => {
                            incomplete_candidates = incomplete_candidates.saturating_add(1);
                        }
                    }
                }
            }
        }

        Ok(HistoricalSearchResult {
            found: None,
            incomplete_candidates,
            tombstone_stopped,
            budget_exhausted,
            history_coverage_complete,
            events_examined,
            bytes_examined,
        })
    }

    /// Project one history event into a versioned payload (DEF-099 helper).
    fn project_history_event(
        &self,
        subject: &[u8],
        hist: &SubjectHistory,
        ev: &HistoryEvent,
        tombstone_crossed: bool,
        _budget: ReadBudget,
        events_examined: usize,
    ) -> Result<VersionedPayloadResult, StoreError> {
        let (current_event_id, current_completeness) = match self.index.get(subject) {
            Some(crate::index::IndexEntry::Live(lv)) => {
                let completeness = match self.get_payload_bytes(subject)? {
                    Some(p) => Some(p.completeness()),
                    None => None,
                };
                (Some(lv.event_id), completeness)
            }
            Some(crate::index::IndexEntry::Deleted { event_id, .. }) => {
                (Some(*event_id), Some("tombstone"))
            }
            None => (None, None),
        };

        let history_coverage_complete = !hist.has_known_holes;

        match ev.kind {
            EventKind::Delete => Ok(VersionedPayloadResult {
                subject: subject.to_vec(),
                selected_event_id: ev.event_id,
                selected_item_id: ev.item_id,
                selected_segment_id: ev.segment_id,
                selected_kind: EventKind::Delete,
                current_event_id,
                current_completeness,
                selected: None,
                is_tombstone: true,
                known_gap_before: ev.known_gap_before,
                history_coverage_complete,
                tombstone_crossed,
                events_examined,
                bytes_examined: 0,
            }),
            EventKind::Put => {
                let (payload, bytes_examined) = if is_chunk_manifest(&ev.body) {
                    let Some(manifest) = decode_chunk_manifest(&ev.body) else {
                        return Err(StoreError::CorruptMeta("invalid historical chunk manifest"));
                    };
                    let resolved = self.resolve_manifest_chunks(ev.item_id, &manifest)?;
                    let payload = reassemble_with_manifest(ev.item_id, &manifest, &resolved);
                    let bytes = match &payload {
                        PayloadResult::Complete { body } => body.len() as u64,
                        PayloadResult::Partial { present_bodies, .. } => {
                            present_bodies.iter().map(|(_, b)| b.len() as u64).sum()
                        }
                        _ => 0,
                    };
                    (payload, bytes)
                } else {
                    let bytes = ev.body.len() as u64;
                    (
                        PayloadResult::Complete {
                            body: ev.body.clone(),
                        },
                        bytes,
                    )
                };
                Ok(VersionedPayloadResult {
                    subject: subject.to_vec(),
                    selected_event_id: ev.event_id,
                    selected_item_id: ev.item_id,
                    selected_segment_id: ev.segment_id,
                    selected_kind: EventKind::Put,
                    current_event_id,
                    current_completeness,
                    selected: Some(payload),
                    is_tombstone: false,
                    known_gap_before: ev.known_gap_before,
                    history_coverage_complete,
                    tombstone_crossed,
                    events_examined,
                    bytes_examined,
                })
            }
        }
    }

    /// Rebuild the primary index by scanning all segment files (no catalog trust).
    ///
    /// Also refreshes the optional on-disk index cache and collection catalog.
    pub fn rebuild_index(&mut self) -> Result<(), StoreError> {
        self.rebuild_index_from_segments()?;
        // Best-effort cache refresh; failure to write cache must not fail rebuild.
        let _ = self.persist_index_cache();
        let _ = self.refresh_collection_catalog();
        Ok(())
    }

    /// Rebuild derived catalogs from the primary index / segments.
    pub fn rebuild_catalogs(&mut self) -> Result<(), StoreError> {
        self.refresh_collection_catalog()
    }

    /// Collection names known from the derived catalog (sorted).
    pub fn list_collections(&self) -> Vec<String> {
        self.collection_catalog
            .names()
            .map(|s| s.to_string())
            .collect()
    }

    /// Effective large-value policy for this store handle (DEF-103).
    pub fn large_value_policy(&self) -> &LargeValuePolicy {
        &self.large_value_policy
    }

    /// Diagnose the derived primary index cache (DEF-102).
    ///
    /// `primary.idx` is never authority. A tiny cache with a large `active/` log
    /// is a normal healthy shape. Diagnostics never change logical results.
    pub fn primary_cache_diag(&self) -> Result<PrimaryCacheDiag, StoreError> {
        let sealed = sealed_segment_paths(&self.paths, Some(&self.tier_placement))?;
        let sealed_fp = segment_fingerprint(&sealed)?;
        let active_actual = if self.writer_shards() <= 1 {
            let p = self.paths.active_segment_for_shard(0, 1);
            if p.is_file() {
                Some(fs::metadata(&p).map(|m| m.len()).unwrap_or(0))
            } else {
                Some(0)
            }
        } else {
            // Multi-shard: sum active shard lengths for a coarse replay estimate.
            let n = self.writer_shards();
            let mut sum = 0u64;
            for shard in 0..n {
                let p = self.paths.active_segment_for_shard(shard, n);
                if p.is_file() {
                    sum = sum.saturating_add(fs::metadata(&p).map(|m| m.len()).unwrap_or(0));
                }
            }
            Some(sum)
        };
        Ok(diagnose_primary_cache(
            &primary_cache_path(&self.paths.indexes_dir()),
            self.store_id,
            Some(sealed_fp),
            active_actual,
        ))
    }

    /// Lifecycle snapshot for active log + derived checkpoints (DEF-102).
    pub fn lifecycle_diag(&self) -> Result<LifecycleDiag, StoreError> {
        let pending = list_pending_paths(&self.paths)?.len();
        let sealed = sealed_segment_paths(&self.paths, Some(&self.tier_placement))?.len();
        Ok(LifecycleDiag {
            active_shards: self.writer_shards(),
            pending_seals: pending,
            sealed_segments: sealed,
            checkpoint_reason: if self.derived_ops_since_checkpoint == 0 {
                "checkpoint_current_or_none".into()
            } else {
                format!("ops_since_checkpoint={}", self.derived_ops_since_checkpoint)
            },
            derived_ops_since_checkpoint: self.derived_ops_since_checkpoint,
            primary_cache_authoritative: false,
            detail: format!(
                "active_shards={} pending_seals={} sealed_segments={}; primary.idx is derived only \
                 (authoritative data lives in active/ + segments/)",
                self.writer_shards(),
                pending,
                sealed
            ),
        })
    }

    /// Replace the large-value policy after validation (DEF-103).
    ///
    /// Tightening limits does **not** make existing above-policy values
    /// unreadable; it only governs new/replacement writes.
    pub fn set_large_value_policy(&mut self, policy: LargeValuePolicy) -> Result<(), StoreError> {
        policy.validate()?;
        self.chunk_threshold = policy.chunk_threshold_bytes;
        self.chunk_size = policy.chunk_payload_bytes;
        self.large_value_policy = policy;
        Ok(())
    }

    /// Override the chunk size threshold (primarily for tests).
    ///
    /// Updates the active policy; invalidates only layout choice, not max size.
    pub fn set_chunk_threshold(&mut self, threshold: usize) {
        if threshold == 0 {
            return;
        }
        self.chunk_threshold = threshold;
        self.large_value_policy.chunk_threshold_bytes = threshold;
        self.relax_manifest_budget_for_tests();
    }

    /// Override per-chunk payload size (primarily for tests).
    pub fn set_chunk_size(&mut self, size: usize) {
        if size > 0 {
            self.chunk_size = size;
            self.large_value_policy.chunk_payload_bytes = size;
            self.relax_manifest_budget_for_tests();
        }
    }

    /// Ensure worst-case manifest for current max logical / chunk size fits budget.
    fn relax_manifest_budget_for_tests(&mut self) {
        let max_chunks = self
            .large_value_policy
            .max_logical_payload_bytes
            .div_ceil(self.large_value_policy.chunk_payload_bytes as u64)
            .max(1);
        let need = 8 + 32 + 4 + 8 + max_chunks.saturating_mul(24);
        if self.large_value_policy.max_manifest_bytes < need {
            self.large_value_policy.max_manifest_bytes = need;
        }
    }

    /// Compact live state into a new sealed segment (sources retained).
    ///
    /// Runs the DEF-024 phase pipeline through **activate** and leaves sources
    /// on disk. Use [`Self::compact_live_with`] to reclaim after activate.
    pub fn compact_live(&mut self) -> Result<CompactReport, StoreError> {
        self.compact_live_with(CompactOptions::default())
    }

    /// Compact live state with explicit reclaim / horizon options (DEF-024).
    ///
    /// Phases: plan → create → verify → activate → optional reclaim.
    /// Reclaim of live-projection sources requires `allow_history_loss`.
    pub fn compact_live_with(
        &mut self,
        options: CompactOptions,
    ) -> Result<CompactReport, StoreError> {
        if options.reclaim_sources && !options.allow_history_loss {
            return Err(StoreError::ConsistencyViolation(
                "compact reclaim requires allow_history_loss for live-projection coverage".into(),
            ));
        }

        self.seal_active()?;
        let source_paths = all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )?;
        let sources: Vec<String> = source_paths
            .iter()
            .map(|p| examination_source_name(&self.paths.root, p))
            .collect();
        let source_ids = reclaimable_source_ids(&self.paths, &sources);
        let live_planned = self.index.live_entries().count();
        let (est_read, est_write) = estimate_compact_bytes(&self.paths, &sources, &self.index);
        let segment_id = self.next_segment_id()?;
        let job_id = random_id()?;
        let created_ns = now_ns();
        let recovery_generation = next_compact_recovery_generation(&self.paths)?;

        let mut job = new_planned_job(
            self.store_id,
            job_id,
            segment_id,
            sources.clone(),
            source_ids,
            live_planned,
            est_read,
            est_write,
            recovery_generation,
            &options,
            created_ns,
        );
        write_compact_job(&self.paths, &job)?;
        crate::failpoint::hit("store.compact.after_plan")?;

        if job.cancel_requested {
            job.phase = CompactPhase::Cancelled;
            job.detail = Some("cancelled before create".into());
            job.updated_ns = now_ns();
            write_compact_job(&self.paths, &job)?;
            return report_from_job(&job);
        }

        // --- create ---
        // Event ids are pure CSPRNG identities (DEF-025); ordering is writer_seq.
        let store_id = self.store_id;
        let mut mint = || random_id();
        let create_result = write_live_segment(
            &self.paths,
            store_id,
            self.limits,
            &self.index,
            segment_id,
            &mut mint,
            created_ns,
        );
        let (written, bytes_written) = match create_result {
            Ok(v) => v,
            Err(e) => {
                job.phase = CompactPhase::Failed;
                job.detail = Some(format!("create failed: {e}"));
                job.updated_ns = now_ns();
                let _ = write_compact_job(&self.paths, &job);
                return Err(e);
            }
        };
        job.phase = CompactPhase::Created;
        job.live_subjects_written = written;
        job.bytes_written = bytes_written;
        job.bytes_read = est_read;
        job.updated_ns = now_ns();
        write_compact_job(&self.paths, &job)?;
        crate::failpoint::hit("store.compact.after_create")?;

        // --- verify ---
        if let Err(e) =
            verify_live_segment(&self.paths, self.limits, &self.index, &segment_id, written)
        {
            job.phase = CompactPhase::Failed;
            job.detail = Some(format!("verify failed: {e}"));
            job.updated_ns = now_ns();
            let _ = write_compact_job(&self.paths, &job);
            return Err(e);
        }
        job.phase = CompactPhase::Verified;
        job.updated_ns = now_ns();
        write_compact_job(&self.paths, &job)?;

        // --- activate ---
        let _ = register_hot_segment(&self.paths, &mut self.tier_placement, segment_id);
        let _ = self.persist_tier_state();
        let _ = self.refresh_segment_catalog();
        let _ = self.persist_index_cache();
        // Chimera: compile live-projection placement for the compact output
        // (derived only). PrimaryIndex segment_ids still point at sources until
        // reclaim/rebuild; per-source chimera sidecars remain the get path.
        let _ = self.write_chimera_for_live_projection(segment_id);
        // CompactShadow post-flip reclaim requires a durable replacement Shadow
        // for the compact output before source retirement.
        if self.recovery_mode.omits_new_materialized()
            || matches!(
                crate::recovery_shadow::shadow_reclaim_policy(),
                crate::recovery_shadow::ShadowReclaimPolicy::RequireReplacementShadow
            )
        {
            let sealed = self.paths.sealed_segment(&segment_id);
            if sealed.is_file() {
                crate::recovery_shadow::publish_mirror_shadow_from_path(
                    &self.paths,
                    self.store_id,
                    &segment_id,
                    &sealed,
                )?;
            }
        }
        crate::failpoint::hit("store.compact.after_activate")?;
        job.phase = CompactPhase::Activated;
        job.updated_ns = now_ns();
        write_compact_job(&self.paths, &job)?;

        // --- optional reclaim ---
        if options.reclaim_sources {
            job.phase = CompactPhase::RetentionHold;
            job.updated_ns = now_ns();
            write_compact_job(&self.paths, &job)?;
            self.reclaim_compact_job_inner(&mut job)?;
        }

        report_from_job(&job)
    }

    /// Explicitly reclaim sources for an activated compact job (DEF-024).
    ///
    /// Requires the job to have `allow_history_loss` (set at plan time or via
    /// this call's force flag when the job already recorded it).
    pub fn reclaim_compact_job(&mut self, job_id: &[u8; 16]) -> Result<CompactReport, StoreError> {
        let mut job = try_load_compact_job(&self.paths, job_id)?
            .ok_or(StoreError::CorruptMeta("compact job not found"))?;
        if !job.allow_history_loss {
            return Err(StoreError::ConsistencyViolation(
                "compact reclaim refused: job does not allow history loss".into(),
            ));
        }
        if matches!(job.phase, CompactPhase::Activated) {
            job.phase = CompactPhase::RetentionHold;
            job.updated_ns = now_ns();
            write_compact_job(&self.paths, &job)?;
        }
        self.reclaim_compact_job_inner(&mut job)?;
        report_from_job(&job)
    }

    /// Cancel an in-flight compact job that has not yet activated.
    ///
    /// Activated/reclaimed jobs cannot be cancelled (output is already live).
    pub fn cancel_compact_job(&mut self, job_id: &[u8; 16]) -> Result<CompactJob, StoreError> {
        let mut job = try_load_compact_job(&self.paths, job_id)?
            .ok_or(StoreError::CorruptMeta("compact job not found"))?;
        if matches!(
            job.phase,
            CompactPhase::Activated
                | CompactPhase::RetentionHold
                | CompactPhase::Reclaimed
                | CompactPhase::Cancelled
                | CompactPhase::Failed
        ) {
            return Err(StoreError::ConsistencyViolation(format!(
                "cannot cancel compact job in phase {}",
                job.phase.as_str()
            )));
        }
        job.cancel_requested = true;
        job.phase = CompactPhase::Cancelled;
        job.detail = Some("operator cancel".into());
        job.updated_ns = now_ns();
        // Best-effort: remove unactivated output segment so it does not linger.
        if let Some(out_id) = job.output_segment_bytes() {
            let p = self.paths.sealed_segment(&out_id);
            if p.is_file() && matches!(job.phase, CompactPhase::Cancelled) {
                // Only delete if we never activated (still true here).
                let _ = fs::remove_file(&p);
            }
        }
        write_compact_job(&self.paths, &job)?;
        Ok(job)
    }

    /// Load a compaction job record if present.
    pub fn load_compact_job(&self, job_id: &[u8; 16]) -> Result<Option<CompactJob>, StoreError> {
        try_load_compact_job(&self.paths, job_id)
    }

    /// List durable compaction job records.
    pub fn list_compact_jobs(&self) -> Result<Vec<CompactJob>, StoreError> {
        crate::compact::list_compact_jobs(&self.paths)
    }

    /// Resume incomplete compact jobs after open (DEF-024 recovery).
    ///
    /// - `planned`: cancel (no durable output yet, or incomplete create)
    /// - `created` / `verified`: finish verify+activate (sources retained)
    /// - `activated` / `retention_hold` / terminal: leave for operator
    pub fn recover_compact_jobs(&mut self) -> Result<Vec<CompactJob>, StoreError> {
        let jobs = crate::compact::list_compact_jobs(&self.paths)?;
        let mut out = Vec::new();
        for mut job in jobs {
            match job.phase {
                CompactPhase::Planned => {
                    job.phase = CompactPhase::Cancelled;
                    job.detail = Some("cancelled on recover: incomplete plan".into());
                    job.updated_ns = now_ns();
                    if let Some(id) = job.output_segment_bytes() {
                        let p = self.paths.sealed_segment(&id);
                        if p.is_file() {
                            // Incomplete create may have left a partial file;
                            // only remove if not registered as activated output.
                            let _ = fs::remove_file(&p);
                        }
                    }
                    write_compact_job(&self.paths, &job)?;
                }
                CompactPhase::Created | CompactPhase::Verified => {
                    if let Err(e) = self.finish_compact_job_after_create(&mut job) {
                        job.phase = CompactPhase::Failed;
                        job.detail = Some(format!("recover failed: {e}"));
                        job.updated_ns = now_ns();
                        let _ = write_compact_job(&self.paths, &job);
                    }
                }
                CompactPhase::Activated
                | CompactPhase::RetentionHold
                | CompactPhase::Reclaimed
                | CompactPhase::Cancelled
                | CompactPhase::Failed => {}
            }
            out.push(job);
        }
        Ok(out)
    }

    fn finish_compact_job_after_create(&mut self, job: &mut CompactJob) -> Result<(), StoreError> {
        let segment_id = job
            .output_segment_bytes()
            .ok_or(StoreError::CorruptMeta("compact output segment id"))?;
        let expected = job.live_subjects_written.max(job.live_subjects_planned);
        if job.phase == CompactPhase::Created {
            verify_live_segment(&self.paths, self.limits, &self.index, &segment_id, expected)?;
            job.phase = CompactPhase::Verified;
            job.updated_ns = now_ns();
            write_compact_job(&self.paths, job)?;
        }
        let _ = register_hot_segment(&self.paths, &mut self.tier_placement, segment_id);
        let _ = self.persist_tier_state();
        let _ = self.refresh_segment_catalog();
        let _ = self.persist_index_cache();
        let _ = self.write_chimera_for_live_projection(segment_id);
        job.phase = CompactPhase::Activated;
        job.updated_ns = now_ns();
        write_compact_job(&self.paths, job)?;
        Ok(())
    }

    fn reclaim_compact_job_inner(&mut self, job: &mut CompactJob) -> Result<(), StoreError> {
        // Source frames may be the only evidence left when the append succeeded
        // but the post-append ledger write was interrupted. Persist that
        // evidence before history-loss compaction is allowed to remove it.
        self.reconcile_write_dedup_from_media()?;
        self.write_dedup_recovery_required = false;
        let (reclaimed, retained, deleted_ids) = reclaim_source_segments(&self.paths, job)?;
        for id in &deleted_ids {
            self.tier_placement.remove(id);
        }
        let _ = self.persist_tier_state();
        let _ = self.refresh_segment_catalog();
        // Live index still valid; rebuild so segment pointers prefer survivors.
        let _ = self.rebuild_index_from_segments();
        let _ = self.persist_index_cache();
        job.bytes_reclaimed = job.bytes_reclaimed.saturating_add(reclaimed);
        job.bytes_retained = retained;
        job.sources_retained = retained > 0
            || job
                .source_segment_ids
                .iter()
                .filter_map(|h| crate::layout::unhex16(h))
                .any(|id| self.paths.sealed_segment(&id).is_file());
        // After reclaim of all listed sources, sources_retained is false.
        if deleted_ids.len() == job.source_segment_ids.len()
            || job
                .source_segment_ids
                .iter()
                .filter_map(|h| crate::layout::unhex16(h))
                .all(|id| !self.paths.sealed_segment(&id).is_file())
        {
            job.sources_retained = false;
        }
        job.phase = CompactPhase::Reclaimed;
        job.updated_ns = now_ns();
        write_compact_job(&self.paths, job)?;
        Ok(())
    }

    /// Write a derived checkpoint under `snapshots/` with declared coverage.
    pub fn checkpoint(&self, coverage: &str) -> Result<(CheckpointMeta, PathBuf), StoreError> {
        let paths_list = all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )?;
        let fp = segment_fingerprint(&paths_list)?;
        // Resolve locator-only entries so the checkpoint still carries payloads.
        let mut live: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for (k, lv) in self.index.live_entries() {
            let body = self.resolve_live_value_body(k.as_slice(), lv)?;
            live.push((k.clone(), body));
        }
        let pairs: Vec<(&[u8], &[u8])> = live
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let meta = CheckpointMeta {
            checkpoint_id: random_id()?,
            live_subjects: live.len(),
            segment_fingerprint: fp,
            coverage: coverage.to_string(),
            projection: "primary-live-v1".into(),
            created_ns: now_ns(),
        };
        let path = write_checkpoint(&self.paths, self.store_id, &meta, &pairs)?;
        Ok((meta, path))
    }

    /// Load a checkpoint file if it belongs to this store.
    pub fn load_checkpoint(
        &self,
        path: &Path,
    ) -> Result<Option<(CheckpointMeta, crate::compact::CheckpointPairs)>, StoreError> {
        try_load_checkpoint(path, self.store_id)
    }

    /// One page of live complete bodies for secondary index builds (DEF-027).
    ///
    /// Unlike [`Self::scan_live_page`], this walk is **not** generation-fenced:
    /// concurrent writes may extend the live set while the build runs; callers
    /// reconcile via snapshot fingerprint + catch-up before marking Ready.
    /// Incomplete payloads are skipped (listed separately) so builds make
    /// forward progress without blocking writes.
    pub fn scan_live_bodies_for_build(
        &self,
        prefix: Option<&[u8]>,
        after: Option<&[u8]>,
        page_size: usize,
    ) -> Result<IndexBuildPage, StoreError> {
        let page_size = page_size.clamp(1, crate::cursor::MAX_PAGE_SIZE);
        let max_examine = page_size.saturating_mul(8).max(page_size);
        let mut entries = Vec::new();
        let mut incomplete = Vec::new();
        let mut examined = 0usize;
        let mut last_subject: Option<Vec<u8>> = after.map(|a| a.to_vec());
        let mut has_more = false;
        let mut iter = self.index.live_entries_after(after, prefix);
        loop {
            if entries.len() >= page_size || examined >= max_examine {
                has_more = iter.next().is_some();
                break;
            }
            let Some((subject_ref, _)) = iter.next() else {
                break;
            };
            let subject = subject_ref.clone();
            examined += 1;
            last_subject = Some(subject.clone());
            let subject_str = match std::str::from_utf8(&subject) {
                Ok(s) => s,
                Err(_) => {
                    incomplete.push(subject);
                    continue;
                }
            };
            match self.get_payload(subject_str)? {
                None => {}
                Some(PayloadResult::Complete { body }) => entries.push((subject, body)),
                Some(PayloadResult::Partial { .. })
                | Some(PayloadResult::Unavailable { .. })
                | Some(PayloadResult::Conflicting { .. }) => incomplete.push(subject),
            }
        }
        Ok(IndexBuildPage {
            entries,
            incomplete,
            has_more,
            after: last_subject,
            examined,
        })
    }

    /// Persist a secondary index file (derived only).
    pub fn write_secondary_index(&self, index: &SecondaryIndex) -> Result<PathBuf, StoreError> {
        let path = secondary_index_path(&self.paths, &index.meta.collection, &index.meta.name);
        write_secondary_index(&path, self.store_id, index)?;
        Ok(path)
    }

    /// Load a secondary index by collection + name.
    pub fn load_secondary_index(
        &self,
        collection: &str,
        name: &str,
    ) -> Result<Option<SecondaryIndex>, StoreError> {
        let path = secondary_index_path(&self.paths, collection, name);
        try_load_secondary_index(&path, self.store_id)
    }

    /// List secondary indexes for a collection.
    pub fn list_secondary_indexes(
        &self,
        collection: &str,
    ) -> Result<Vec<SecondaryIndex>, StoreError> {
        let mut out = Vec::new();
        for path in list_secondary_index_paths(&self.paths, collection)? {
            if let Some(idx) = try_load_secondary_index(&path, self.store_id)? {
                out.push(idx);
            }
        }
        Ok(out)
    }

    /// Delete a secondary index file (never touches segments).
    pub fn delete_secondary_index(&self, collection: &str, name: &str) -> Result<(), StoreError> {
        let path = secondary_index_path(&self.paths, collection, name);
        delete_secondary_index(&path)
    }

    /// Current segment fingerprint (for index build coverage).
    pub fn segment_fingerprint(&self) -> Result<[u8; 32], StoreError> {
        let paths = all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )?;
        segment_fingerprint(&paths)
    }

    /// Store layout paths (derived dirs safe to wipe for salvage tests).
    pub fn paths(&self) -> &StorePaths {
        &self.paths
    }

    /// Load optional index cache via frontier (DEF-023) or v1 fingerprint; else rebuild.
    fn load_or_rebuild_index(&mut self) -> Result<IndexOpenStats, StoreError> {
        let miss = match self.try_load_index_from_cache()? {
            IndexLoadAttempt::Loaded(stats) => {
                if stats.full_scan_bytes > 0 && !stats.chunk_locators_from_checkpoint {
                // Writable compatibility migration: once a legacy/incomplete
                // derived checkpoint has been repaired from authority, replace
                // it immediately so the next clean open is metadata + tail only.
                // Failure remains non-fatal because the cache is never authority.
                    let _ = self.persist_index_cache();
                }
                return Ok(stats);
            }
            IndexLoadAttempt::Miss(reason) => reason,
        };
        let segments_examined = all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )?
        .len() as u64;
        let full_scan_bytes = total_segment_bytes(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )?;
        self.rebuild_index()?;
        Ok(IndexOpenStats {
            full_scan_bytes,
            disposition: IndexOpenDisposition::Rebuilt,
            cache_decision: miss,
            cache_bytes: primary_cache_bytes(&self.paths),
            index_entries: self.index.len() as u64,
            chunk_locator_entries: chunk_locator_count(&self.chunk_locators),
            segments_examined,
            ..Default::default()
        })
    }

    /// Read-only open path: load cache or rebuild without writing derived files.
    fn load_or_rebuild_index_readonly(&mut self) -> Result<IndexOpenStats, StoreError> {
        let miss = match self.try_load_index_from_cache()? {
            IndexLoadAttempt::Loaded(stats) => return Ok(stats),
            IndexLoadAttempt::Miss(reason) => reason,
        };
        let segments_examined = all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )?
        .len() as u64;
        let full_scan_bytes = total_segment_bytes(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )?;
        self.rebuild_index_from_segments()?;
        Ok(IndexOpenStats {
            full_scan_bytes,
            disposition: IndexOpenDisposition::Rebuilt,
            cache_decision: miss,
            cache_bytes: primary_cache_bytes(&self.paths),
            index_entries: self.index.len() as u64,
            chunk_locator_entries: chunk_locator_count(&self.chunk_locators),
            segments_examined,
            ..Default::default()
        })
    }

    /// Attempt frontier v2 or legacy v1 cache load. Returns true when applied.
    fn try_load_index_from_cache(&mut self) -> Result<IndexLoadAttempt, StoreError> {
        let sealed_paths = sealed_segment_paths(&self.paths, Some(&self.tier_placement))?;
        let sealed_fp = segment_fingerprint(&sealed_paths)?;
        let all_paths = all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )?;
        let cache_path = primary_cache_path(&self.paths.indexes_dir());
        let cache_bytes = fs::metadata(&cache_path).map(|m| m.len()).unwrap_or(0);
        let segments_examined = all_paths.len() as u64;
        let mut miss = if cache_path.is_file() {
            IndexCacheDecision::Rejected
        } else {
            IndexCacheDecision::Absent
        };

        // DEF-023: v2/v3 checkpoint + active-tail delta (O(changed bytes), not full rescan).
        let cache_decode_started = Instant::now();
        if let Some(loaded) = try_load_primary_index_frontier(&cache_path, self.store_id)? {
            let cache_decode_ns = elapsed_ns(cache_decode_started);
            let format_version = loaded.format_version;
            let mut index = loaded.index;
            let frontier = loaded.frontier;
            let mut chunk_locators = loaded.chunk_locators;
            let mut active_replay_bytes = 0u64;
            if frontier.sealed_fingerprint == sealed_fp {
                let active_ok = if self.writer_shards() > 1 {
                    // Axis B: frontier is sealed-only; re-apply every active shard
                    // fully (idempotent latest-wins overwrites same event ids).
                    for path in self.paths.list_active_segment_paths(self.writer_shards()) {
                        active_replay_bytes =
                            active_replay_bytes.saturating_add(apply_active_tail(
                                &mut index,
                                chunk_locators.as_mut(),
                                &path,
                                0,
                                self.limits,
                            )?);
                    }
                    true
                } else {
                    let active_path = self.paths.active_segment_for_shard(0, 1);
                    match (
                        active_path.is_file(),
                        frontier.active_segment_id != [0u8; 16],
                    ) {
                        (false, false) => true,
                        (false, true) => {
                            // Cache expected an active segment that is gone — treat as miss
                            // only when covered_len was non-zero (empty active is fine).
                            frontier.active_covered_len == 0
                        }
                        (true, _) => {
                            let meta_len = fs::metadata(&active_path).map(|m| m.len()).unwrap_or(0);
                            if meta_len < frontier.active_covered_len {
                                miss = IndexCacheDecision::ActiveFrontierAhead;
                                false
                            } else {
                                active_replay_bytes = apply_active_tail(
                                    &mut index,
                                    chunk_locators.as_mut(),
                                    &active_path,
                                    frontier.active_covered_len,
                                    self.limits,
                                )?;
                                true
                            }
                        }
                    }
                };
                if active_ok {
                    let (install_clone_ns, catalog_derive_ns) =
                        self.install_loaded_index(index, &all_paths)?;
                    if let Some(locators) = chunk_locators {
                        if chunk_locator_coverage_complete(&self.durable_index, &locators) {
                            self.chunk_locators = locators;
                            return Ok(IndexLoadAttempt::Loaded(IndexOpenStats {
                                active_replay_bytes,
                                chunk_locators_from_checkpoint: true,
                                disposition: if active_replay_bytes == 0 {
                                    IndexOpenDisposition::Loaded
                                } else {
                                    IndexOpenDisposition::TailReplayed
                                },
                                cache_decision: IndexCacheDecision::AcceptedV4,
                                cache_bytes,
                                cache_decode_ns,
                                install_clone_ns,
                                catalog_derive_ns,
                                index_entries: self.index.len() as u64,
                                chunk_locator_entries: chunk_locator_count(&self.chunk_locators),
                                segments_examined,
                                ..IndexOpenStats::default()
                            }));
                        }
                    }
                    // One-time compatibility path for v2/v3 checkpoints, or
                    // fail-safe repair of an incomplete derived v4 locator set.
                    self.rebuild_chunk_locators_from_segments()?;
                    let full_scan_bytes = total_segment_bytes(
                        &self.paths,
                        Some(&self.tier_placement),
                        self.writer_shards(),
                    )?;
                    return Ok(IndexLoadAttempt::Loaded(IndexOpenStats {
                        full_scan_bytes,
                        active_replay_bytes,
                        chunk_locators_from_checkpoint: false,
                        disposition: IndexOpenDisposition::LegacyUpgraded,
                        cache_decision: if format_version >= 4 {
                            IndexCacheDecision::Rejected
                        } else {
                            IndexCacheDecision::AcceptedLegacy
                        },
                        cache_bytes,
                        cache_decode_ns,
                        install_clone_ns,
                        catalog_derive_ns,
                        index_entries: self.index.len() as u64,
                        chunk_locator_entries: chunk_locator_count(&self.chunk_locators),
                        segments_examined,
                    }));
                }
            } else {
                miss = IndexCacheDecision::SealedFingerprintMismatch;
            }
        }

        // Legacy v1: exact full fingerprint match (sealed + active lengths).
        let fp = segment_fingerprint(&all_paths)?;
        let v1_decode_started = Instant::now();
        if let Some(index) = try_load_primary_index(&cache_path, self.store_id, fp)? {
            let cache_decode_ns = elapsed_ns(v1_decode_started);
            let (install_clone_ns, catalog_derive_ns) =
                self.install_loaded_index(index, &all_paths)?;
            self.rebuild_chunk_locators_from_segments()?;
            let full_scan_bytes = total_segment_bytes(
                &self.paths,
                Some(&self.tier_placement),
                self.writer_shards(),
            )?;
            return Ok(IndexLoadAttempt::Loaded(IndexOpenStats {
                full_scan_bytes,
                disposition: IndexOpenDisposition::LegacyUpgraded,
                cache_decision: IndexCacheDecision::AcceptedV1,
                cache_bytes,
                cache_decode_ns,
                install_clone_ns,
                catalog_derive_ns,
                index_entries: self.index.len() as u64,
                chunk_locator_entries: chunk_locator_count(&self.chunk_locators),
                segments_examined,
                ..IndexOpenStats::default()
            }));
        }
        Ok(IndexLoadAttempt::Miss(miss))
    }

    fn install_loaded_index(
        &mut self,
        index: PrimaryIndex,
        _all_paths: &[PathBuf],
    ) -> Result<(u64, u64), StoreError> {
        let clone_started = Instant::now();
        self.index = index.clone();
        self.durable_index = index;
        let clone_ns = elapsed_ns(clone_started);
        let catalog_started = Instant::now();
        self.recompute_collection_catalogs_from_index();
        let catalog_ns = elapsed_ns(catalog_started);
        // Allocator is sole authority for `segment_seq` — index must not touch it.
        self.derived_ops_since_checkpoint = 0;
        Ok((clone_ns, catalog_ns))
    }

    fn rebuild_index_from_segments(&mut self) -> Result<(), StoreError> {
        let (index, chunk_locators) = index_and_chunk_locators_from_segments(
            &self.paths,
            self.limits,
            Some(&self.tier_placement),
            self.writer_shards(),
        )?;
        self.index = index;
        self.chunk_locators = chunk_locators;
        self.durable_index = self.index.clone();
        self.recompute_collection_catalogs_from_index();
        // Allocator is sole authority for `segment_seq` — index must not touch it.
        self.derived_ops_since_checkpoint = 0;
        Ok(())
    }

    /// Write the optional primary index cache under `indexes/` (Stage 3c / DEF-023).
    ///
    /// Checkpoint is built from the in-memory **durable** projection (no full
    /// segment rescan). Memory-mode publishes are never persisted. Safe to
    /// delete: open/rebuild recovers from segments (full scan) or from a prior
    /// frontier checkpoint plus the active tail.
    pub fn persist_index_cache(&mut self) -> Result<(), StoreError> {
        let frontier = self.current_index_frontier()?;
        let chunk_locators = self.checkpoint_chunk_locators(&frontier);
        write_primary_index_frontier(
            &primary_cache_path(&self.paths.indexes_dir()),
            self.store_id,
            &frontier,
            &self.durable_index,
            &chunk_locators,
        )?;
        self.derived_ops_since_checkpoint = 0;
        Ok(())
    }

    /// Sealed-set fingerprint + active covered length for the durable index.
    ///
    /// Multi-shard (Axis B): frontier records sealed-only coverage
    /// (`active_segment_id = 0`); open re-applies all active shard files.
    fn current_index_frontier(&self) -> Result<IndexFrontier, StoreError> {
        let sealed = sealed_segment_paths(&self.paths, Some(&self.tier_placement))?;
        let sealed_fingerprint = segment_fingerprint(&sealed)?;
        if self.writer_shards() > 1 {
            return Ok(IndexFrontier {
                sealed_fingerprint,
                active_segment_id: [0u8; 16],
                active_covered_len: 0,
            });
        }
        let (active_segment_id, active_covered_len) = match self.active_ref(0) {
            Some(w) => (w.segment_id, w.durable_len),
            None => {
                // No writer handle (inspect) or inactive: use on-disk active metadata.
                let path = self.paths.active_segment_for_shard(0, 1);
                if path.is_file() {
                    let len = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                    // Segment id unknown without scanning; zeros means "any/unknown".
                    // Callers that only persist with an active writer always set id.
                    ([0u8; 16], len)
                } else {
                    ([0u8; 16], 0)
                }
            }
        };
        Ok(IndexFrontier {
            sealed_fingerprint,
            active_segment_id,
            active_covered_len,
        })
    }

    /// Snapshot locators referenced by the durable live index and covered by
    /// `frontier`. Current active frames beyond the durable frontier (including
    /// memory-only writes) are deliberately excluded.
    fn checkpoint_chunk_locators(&self, frontier: &IndexFrontier) -> ChunkLocatorMap {
        let mut expected = HashSet::new();
        for (_subject, entry) in self.durable_index.iter_all() {
            let IndexEntry::Live(value) = entry else {
                continue;
            };
            let Some(manifest) = decode_chunk_manifest(&value.body) else {
                continue;
            };
            expected.extend(manifest.chunks.into_iter().map(|slot| slot.chunk_event_id));
        }

        let active_ids: HashSet<[u8; 16]> = self
            .actives
            .iter()
            .filter_map(|active| active.as_ref().map(|writer| writer.segment_id))
            .collect();
        let mut out = ChunkLocatorMap::new();
        for event_id in expected {
            let Some(locators) = self.chunk_locators.get(&event_id) else {
                continue;
            };
            for locator in locators {
                let covered = if active_ids.contains(&locator.segment_id) {
                    locator.segment_id == frontier.active_segment_id
                        && locator.frame_offset < frontier.active_covered_len
                } else {
                    true
                };
                if covered {
                    out.entry(event_id).or_default().push(locator.clone());
                }
            }
        }
        out
    }

    /// After a buffered/durable append: touch derived state without segment rescan.
    ///
    /// In-memory collection membership is updated by the caller via
    /// [`Self::note_collection_for_subject`]. Disk checkpoints (index cache +
    /// collection catalog) are **rate-limited**: both use atomic fsync writes and
    /// must not sit on every put acknowledgement path (DEF-023).
    ///
    /// With async lifecycle (DEF-096 Axis A), the rate-limited index checkpoint is
    /// submitted to the seal pipeline worker so fsync cost is off the put path.
    fn note_durable_derived(&mut self) -> Result<(), StoreError> {
        let _ = self.poll_lifecycle();
        self.derived_ops_since_checkpoint = self.derived_ops_since_checkpoint.saturating_add(1);
        if self.derived_ops_since_checkpoint >= DERIVED_CHECKPOINT_EVERY_OPS {
            // Best-effort: a failed derived write must not fail the already-acked
            // authoritative append. Recovery rebuilds from segments.
            if self.async_lifecycle_enabled() {
                self.derived_ops_since_checkpoint = 0;
                if let Ok(frontier) = self.current_index_frontier() {
                    let chunk_locators = self.checkpoint_chunk_locators(&frontier);
                    let job = LifecycleJob::Checkpoint {
                        cache_path: primary_cache_path(&self.paths.indexes_dir()),
                        store_id: self.store_id,
                        frontier,
                        index: self.durable_index.clone(),
                        chunk_locators,
                    };
                    if let Some(pipe) = self.seal_pipeline.as_ref() {
                        let _ = pipe.submit_checkpoint(job);
                    }
                }
                let _ = self.refresh_collection_catalog();
            } else {
                let _ = self.persist_index_cache();
                let _ = self.refresh_collection_catalog();
            }
        }
        Ok(())
    }

    /// Record a collection name for a durable subject (visibility + durable set).
    ///
    /// When a **new** durable collection name appears, persist the durable-only
    /// catalog immediately (DEF-013 frontier honesty). Index-cache checkpoints
    /// remain rate-limited via [`Self::note_durable_derived`] (DEF-023).
    fn note_collection_for_subject(&mut self, subject: &[u8]) {
        if let Some(name) = crate::catalog::collection_name_from_subject(subject) {
            self.collection_catalog.insert(name.clone());
            if self.durable_collections.insert(name) {
                let _ = self.refresh_collection_catalog();
            }
        }
    }

    #[allow(clippy::too_many_arguments)] // mirrors index::apply_event event fields
    fn apply_durable_event(
        &mut self,
        subject: Vec<u8>,
        kind: EventKind,
        body: Vec<u8>,
        item_id: [u8; 16],
        event_id: [u8; 16],
        segment_id: [u8; 16],
        writer_sequence: u64,
        frame_offset: u64,
    ) {
        // DEF-095: durable projection is locator-first; do not pin full payloads
        // in both visibility and durable maps (was O(dataset) RSS dual-copy).
        let body = slim_put_body_for_index(body, false);
        self.index.apply_event(
            subject.clone(),
            kind,
            body.clone(),
            item_id,
            event_id,
            segment_id,
            writer_sequence,
            frame_offset,
        );
        self.durable_index.apply_event(
            subject,
            kind,
            body,
            item_id,
            event_id,
            segment_id,
            writer_sequence,
            frame_offset,
        );
    }

    /// Resolve logical stored body for a live entry (resident or frame pread).
    fn resolve_live_value_body(
        &self,
        subject: &[u8],
        lv: &crate::index::LiveValue,
    ) -> Result<Vec<u8>, StoreError> {
        if !lv.body.is_empty() {
            return Ok(lv.body.clone());
        }
        let expect = crate::compact::LocatorExpect {
            segment_id: lv.segment_id,
            event_id: lv.event_id,
            item_id: lv.item_id,
            subject: subject.to_vec(),
            writer_sequence: lv.writer_sequence,
        };
        // Prefer in-memory active tail (write-through may have dropped older frames).
        if let Some(w) = self.find_active_by_segment(&lv.segment_id) {
            let base = w.segment.base_offset();
            if lv.frame_offset >= base {
                let bytes = w.segment.as_bytes();
                let off = (lv.frame_offset - base) as usize;
                if off < bytes.len() {
                    if let Ok((header, envelope, body, _hash, _len)) =
                        residiuum_format::verify_frame_at(&bytes[off..], self.limits)
                    {
                        if header.event_id != expect.event_id {
                            return Err(StoreError::ConsistencyViolation(
                                "locator event_id mismatch at frame offset".into(),
                            ));
                        }
                        let _ = expect.writer_sequence;
                        let _ = header.writer_sequence;
                        if let Some(env) = decode_item_envelope(envelope) {
                            if env.segment_id != expect.segment_id {
                                return Err(StoreError::ConsistencyViolation(
                                    "locator segment_id mismatch in envelope".into(),
                                ));
                            }
                            if env.item_id != expect.item_id {
                                return Err(StoreError::ConsistencyViolation(
                                    "locator item_id mismatch in envelope".into(),
                                ));
                            }
                            if env.subject != expect.subject {
                                return Err(StoreError::ConsistencyViolation(
                                    "locator subject mismatch in envelope".into(),
                                ));
                            }
                        }
                        return Ok(body.to_vec());
                    }
                }
            }
        }
        if lv.frame_offset == 0 {
            return Ok(Vec::new());
        }
        self.pread_body_for_locator_matching(&expect, lv.frame_offset)
    }

    /// Pread an item body by (segment_id, frame_offset).
    ///
    /// Canonical sealed path first; then placement; then any segment file that
    /// holds a verified item frame at that offset whose envelope segment_id
    /// matches. The last path covers salvage/evidence copies that rename active
    /// → hash-named sealed files while preserving original envelope ids.
    fn pread_body_for_locator(
        &self,
        segment_id: &[u8; 16],
        frame_offset: u64,
    ) -> Result<Vec<u8>, StoreError> {
        // Legacy callers: segment-id only.
        let expect = crate::compact::LocatorExpect {
            segment_id: *segment_id,
            event_id: [0u8; 16],
            item_id: [0u8; 16],
            subject: Vec::new(),
            writer_sequence: 0,
        };
        // Use segment-only helper path via tried list + pread_item_body_if_segment.
        self.pread_body_for_locator_segment_only(&expect.segment_id, frame_offset)
    }

    fn pread_body_for_locator_matching(
        &self,
        expect: &crate::compact::LocatorExpect,
        frame_offset: u64,
    ) -> Result<Vec<u8>, StoreError> {
        let mut tried = Vec::new();
        if self.find_active_by_segment(&expect.segment_id).is_some() {
            let n = self.writer_shards();
            for shard in 0..n {
                if self
                    .active_ref(shard)
                    .map(|a| a.segment_id == expect.segment_id)
                    .unwrap_or(false)
                {
                    let p = self.paths.active_segment_for_shard(shard, n);
                    if p.is_file() {
                        tried.push(p);
                    }
                    break;
                }
            }
        }
        if let Some(p) = self.tier_placement.get(&expect.segment_id) {
            if let Ok(path) = crate::tier::resolve_placement_path(&self.paths, p) {
                if path.is_file() {
                    tried.push(path);
                }
            }
        }
        let sealed = self.paths.sealed_segment(&expect.segment_id);
        if sealed.is_file() {
            tried.push(sealed);
        }
        let pending = self.paths.pending_segment(&expect.segment_id);
        if pending.is_file() {
            tried.push(pending);
        }
        let mut last_named_err: Option<StoreError> = None;
        let mut named_media = false;
        for path in &tried {
            named_media = true;
            match crate::compact::pread_item_body_matching(path, frame_offset, expect, self.limits)
            {
                Ok(body) => return Ok(body),
                Err(e)
                    if is_locator_resolve_error(&e)
                        || matches!(e, StoreError::ConsistencyViolation(_)) =>
                {
                    last_named_err = Some(e)
                }
                Err(_) => {}
            }
        }
        if named_media {
            return Err(last_named_err.unwrap_or_else(|| {
                StoreError::LocatorFault(Box::new(crate::error::LocatorFault::at_path(
                    crate::error::LocatorFaultKind::FrameVerifyFailed,
                    expect.segment_id,
                    frame_offset,
                    tried
                        .first()
                        .map(|p| p.as_path())
                        .unwrap_or_else(|| Path::new("")),
                    None,
                    Some("named media unreadable".into()),
                )))
            }));
        }
        for path in all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )? {
            if tried.iter().any(|t| t == &path) {
                continue;
            }
            if !path.is_file() {
                continue;
            }
            if let Ok(body) =
                crate::compact::pread_item_body_matching(&path, frame_offset, expect, self.limits)
            {
                return Ok(body);
            }
        }
        Err(StoreError::LocatorFault(Box::new(
            crate::error::LocatorFault::segment_not_found(expect.segment_id, frame_offset),
        )))
    }

    fn pread_body_for_locator_segment_only(
        &self,
        segment_id: &[u8; 16],
        frame_offset: u64,
    ) -> Result<Vec<u8>, StoreError> {
        let mut tried = Vec::new();
        if self.find_active_by_segment(segment_id).is_some() {
            // Locate the on-disk path for this shard's active file.
            let n = self.writer_shards();
            for shard in 0..n {
                if self
                    .active_ref(shard)
                    .map(|a| a.segment_id == *segment_id)
                    .unwrap_or(false)
                {
                    let p = self.paths.active_segment_for_shard(shard, n);
                    if p.is_file() {
                        tried.push(p);
                    }
                    break;
                }
            }
        }
        if let Some(p) = self.tier_placement.get(segment_id) {
            if let Ok(path) = crate::tier::resolve_placement_path(&self.paths, p) {
                if path.is_file() {
                    tried.push(path);
                }
            }
        }
        let sealed = self.paths.sealed_segment(segment_id);
        if sealed.is_file() {
            tried.push(sealed);
        }
        // DEF-096: rotated-but-not-finalized segment may still hold locators.
        let pending = self.paths.pending_segment(segment_id);
        if pending.is_file() {
            tried.push(pending);
        }
        // Always require envelope segment_id match. After §16.10 reorder/swap,
        // the canonical sealed filename may hold another segment's bytes; bare
        // pread at the same post-descriptor offset would return the wrong body.
        //
        // DEF-SCAN-001: preserve distinct locator failures. Do not collapse bad
        // offset / verify / segment-id mismatch into SegmentNotFound or PayloadPartial.
        // 1) Named media for this segment_id (active / placement / sealed / pending).
        //    If present but unreadable, return that distinct locator error.
        // 2) Only when **no** named media exists, salvage other paths that may hold
        //    the same envelope segment_id (hash renames). Salvage failures must not
        //    re-label "segment file deleted" as OffsetInvalid on a different file.
        let mut last_named_err: Option<StoreError> = None;
        let mut named_media = false;
        for path in &tried {
            named_media = true;
            match pread_item_body_if_segment(path, frame_offset, segment_id, self.limits) {
                Ok(body) => return Ok(body),
                Err(e) if is_locator_resolve_error(&e) => last_named_err = Some(e),
                Err(_) => {}
            }
        }
        if named_media {
            return Err(last_named_err.unwrap_or_else(|| {
                StoreError::LocatorFault(Box::new(crate::error::LocatorFault::at_path(
                    crate::error::LocatorFaultKind::FrameVerifyFailed,
                    *segment_id,
                    frame_offset,
                    tried
                        .first()
                        .map(|p| p.as_path())
                        .unwrap_or_else(|| Path::new("")),
                    None,
                    Some("named media unreadable".into()),
                )))
            }));
        }
        // Salvage/hash-renamed or swapped sealed files (success only).
        for path in all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )? {
            if tried.iter().any(|t| t == &path) {
                continue;
            }
            if !path.is_file() {
                continue;
            }
            if let Ok(body) =
                pread_item_body_if_segment(&path, frame_offset, segment_id, self.limits)
            {
                return Ok(body);
            }
        }
        Err(StoreError::LocatorFault(Box::new(
            crate::error::LocatorFault::segment_not_found(*segment_id, frame_offset),
        )))
    }

    /// Path of the optional primary index cache file.
    pub fn index_cache_path(&self) -> PathBuf {
        primary_cache_path(&self.paths.indexes_dir())
    }

    /// Path of the framed store descriptor under `store-info/`.
    pub fn store_descriptor_path(&self) -> PathBuf {
        self.paths.store_descriptor_file()
    }

    /// Catalog-free salvage: scan every segment file and report counts.
    ///
    /// Does not mutate on-disk authoritative bytes. Live-subject projection
    /// uses the same recovery order and `event_id` dedup as index rebuild.
    pub fn salvage(&self) -> Result<SalvageReport, StoreError> {
        let mut files_scanned = 0usize;
        let mut verified_frames = 0u64;
        let mut item_events = 0u64;
        let mut holes = 0u64;

        for path in all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )? {
            let bytes = fs::read(&path)?;
            files_scanned += 1;
            let report = scan_forward(&bytes, self.limits);
            verified_frames += report.verified_count() as u64;
            holes += report.holes().count() as u64;
            for (_offset, frame) in report.verified_frames() {
                if frame.header.known_kind() != Some(FrameKind::ItemEvent) {
                    continue;
                }
                if decode_item_envelope(&frame.envelope).is_some() {
                    item_events += 1;
                }
            }
        }

        let temp_index = index_from_segments(
            &self.paths,
            self.limits,
            Some(&self.tier_placement),
            self.writer_shards(),
        )?;
        Ok(SalvageReport {
            files_scanned,
            verified_frames,
            item_events,
            holes,
            live_subjects: temp_index.live_entries().count(),
        })
    }

    /// Create a full, content-hashed backup package (DEF-050).
    ///
    /// Distinct from [`Self::salvage_to`] (damage recovery) and
    /// [`Self::export_live_state`] (new lineage). Authoritative trees
    /// (`store-info`, `active`, `segments`, `chunks`, `recovery`, `tiers`) are
    /// copied into `package/store/` with blake3 per file; derived catalogs are
    /// omitted and rebuilt on restore.
    ///
    /// When this handle holds the exclusive writer lock, the active segment is
    /// flushed durable first ([`BackupConsistency::FlushedExclusive`]).
    /// Inspect-only opens copy on-disk files without a flush
    /// ([`BackupConsistency::OnDiskInspect`]).
    ///
    /// `package` must not already exist (or must be empty).
    pub fn backup_to(
        &mut self,
        package: impl AsRef<Path>,
    ) -> Result<crate::backup::BackupReport, StoreError> {
        let consistency = if self.writer_lock.is_some() {
            self.persist_all_actives(DurabilityMode::Durable)?;
            crate::backup::BackupConsistency::FlushedExclusive
        } else {
            crate::backup::BackupConsistency::OnDiskInspect
        };
        crate::backup::write_full_backup(
            &self.paths.root,
            self.store_id,
            package.as_ref(),
            consistency,
        )
    }

    /// Run a bounded integrity scrub step (DEF-051).
    ///
    /// Verifies sealed segments (and optionally active/chunks) with full-file
    /// BLAKE3 and forward frame scan. Compares against placement `content_hash`
    /// when known. Findings are persisted under `recovery/scrub/`; corrupt
    /// evidence may be copied to quarantine without removing the original.
    ///
    /// Work is bounded by [`crate::ScrubOptions::max_files`] /
    /// [`crate::ScrubOptions::max_bytes`] so scrub never starves foreground
    /// callers that schedule multiple steps.
    pub fn scrub_once(&self, opts: crate::ScrubOptions) -> Result<crate::ScrubReport, StoreError> {
        crate::scrub::scrub_once(&self.paths, self.store_id, &self.tier_placement, &opts)
    }

    /// Scrub status: age, coverage, bytes verified, failures, pause flag.
    pub fn scrub_status(&self) -> Result<crate::ScrubStatus, StoreError> {
        crate::scrub::scrub_status(&self.paths, self.store_id)
    }

    /// Pause scrub so subsequent [`Self::scrub_once`] calls no-op until resume.
    pub fn pause_scrub(&self) -> Result<crate::ScrubStatus, StoreError> {
        crate::scrub::pause_scrub(&self.paths, self.store_id)
    }

    /// Resume a paused scrub.
    pub fn resume_scrub(&self) -> Result<crate::ScrubStatus, StoreError> {
        crate::scrub::resume_scrub(&self.paths, self.store_id)
    }

    /// Open scrub findings (hash mismatch, holes, missing media).
    pub fn list_scrub_findings(&self) -> Result<Vec<crate::ScrubFinding>, StoreError> {
        crate::scrub::list_scrub_findings(&self.paths, self.store_id)
    }

    /// Run scrub to completion under the given per-step bounds (loop).
    ///
    /// Stops early if paused or if a single step makes no progress while
    /// targets remain (safety).
    pub fn scrub_to_completion(
        &self,
        opts: crate::ScrubOptions,
    ) -> Result<crate::ScrubReport, StoreError> {
        let mut last = self.scrub_once(opts.clone())?;
        let mut guard = 0u32;
        while !last.cycle_completed && !last.paused && guard < 10_000 {
            guard += 1;
            let next = self.scrub_once(opts.clone())?;
            if next.targets_processed == 0 && !next.cycle_completed {
                break;
            }
            last = next;
        }
        Ok(last)
    }

    /// Format migration preflight (DEF-052): version matrix + segment classification.
    ///
    /// Does not write a durable job. Destination must be empty / absent.
    pub fn migrate_preflight(
        &self,
        dest: impl AsRef<Path>,
    ) -> Result<crate::MigratePreflight, StoreError> {
        crate::migrate::migrate_preflight(&self.paths.root, dest.as_ref(), self.store_id)
    }

    /// Phased format migration into a new store directory (DEF-052).
    ///
    /// Never rewrites the source in place. Copies authoritative trees with
    /// per-file blake3, preserves unsupported / unreadable segment bytes as
    /// opaque evidence, and only marks success after open+verify of the
    /// destination. Durable job under `recovery/migration/job.v1.json`.
    ///
    /// When this handle holds the exclusive writer lock, the active segment is
    /// flushed durable first so the migration boundary is crash-consistent.
    pub fn migrate_to(
        &mut self,
        dest: impl AsRef<Path>,
        opts: crate::MigrateOptions,
    ) -> Result<crate::MigrateReport, StoreError> {
        if self.writer_lock.is_some() {
            self.persist_all_actives(DurabilityMode::Durable)?;
        }
        crate::migrate::migrate_store(&self.paths.root, dest.as_ref(), self.store_id, opts)
    }

    /// Load the durable migration job from this store's recovery directory.
    pub fn load_migration_job(&self) -> Result<Option<crate::MigrationJob>, StoreError> {
        crate::migrate::load_migration_job(&self.paths.root)
    }

    /// Evidence-preserving salvage into a **new** store directory (DX_SPEC §13.4, DEF-011).
    ///
    /// The source store is never mutated. Destination must not already be a
    /// store (same rules as [`Store::create`]). Verified frames are copied
    /// **byte-identical** into destination sealed segments; holes and scan
    /// parameters are recorded under `recovery/salvage-manifest.v1.json`.
    /// Event, item, and frame identities inside those frames are preserved.
    ///
    /// For a clean current-state database (re-put live values, new lineage),
    /// use [`Self::export_live_state`] instead.
    pub fn salvage_to(&self, dest: impl AsRef<Path>) -> Result<SalvageCopyReport, StoreError> {
        let dest = dest.as_ref();
        let source = self.salvage()?;

        // Skeleton destination: empty active segment + store identity. Recovered
        // frames go only into `segments/` so open does not re-encode them.
        let dest_store = Store::create(dest)?;
        let dest_store_id = dest_store.store_id;
        let dest_paths = dest_store.paths.clone();
        drop(dest_store);

        let mut source_files = Vec::new();
        for path in all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )? {
            let rel = examination_source_name(&self.paths.root, &path);
            source_files.push((rel, path));
        }

        let (mut manifest, frames_copied, holes_recorded) = crate::recovery::copy_verified_frames(
            &self.paths.root,
            self.store_id,
            &dest_paths,
            dest_store_id,
            &source_files,
            self.limits,
        )?;

        // Rebuild derived state from the copied frames (does not rewrite them).
        // Dest mint has a new store_id; evidence frames retain source store_id —
        // open with TolerateUnidentified so survivors enumerate without FailClosed.
        let mut dest_open = Store::open_with_options(
            dest,
            StoreOpenOptions::default().tolerate_unidentified_inventory(),
        )?;
        let live_subjects = dest_open.index.live_entries().count();
        manifest.live_subjects = live_subjects;
        // Re-hash after filling live_subjects.
        manifest.content_hash_hex = {
            let mut for_hash = manifest.clone();
            for_hash.content_hash_hex.clear();
            let body = serde_json::to_vec(&for_hash).map_err(|e| {
                StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("serialize recovery manifest for hash: {e}"),
                ))
            })?;
            let h = blake3::hash(&body);
            let mut s = String::with_capacity(64);
            for b in h.as_bytes() {
                s.push_str(&format!("{b:02x}"));
            }
            s
        };
        let manifest_path = crate::recovery::write_recovery_manifest(&dest_paths, &manifest)?;
        let _ = dest_open.persist_index_cache();
        let _ = dest_open.rebuild_catalogs();
        drop(dest_open);

        Ok(SalvageCopyReport {
            source,
            destination: dest.to_path_buf(),
            mode: crate::recovery::SalvageMode::Evidence,
            subjects_copied: live_subjects,
            frames_copied,
            holes_recorded,
            manifest_path: Some(manifest_path),
        })
    }

    /// Materialize **live logical state** into a new store (DEF-011 export path).
    ///
    /// Unlike [`Self::salvage_to`], this re-appends complete live payloads as
    /// durable puts with **new** store/event lineage. History, tombstones,
    /// partials, and holes are **not** preserved. Prefer `salvage_to` when
    /// examination evidence must survive.
    pub fn export_live_state(
        &self,
        dest: impl AsRef<Path>,
    ) -> Result<SalvageCopyReport, StoreError> {
        let dest = dest.as_ref();
        let source = self.salvage()?;
        let live = self.live_logical_entries()?;
        let mut dest_store = Store::create(dest)?;
        let mut subjects_copied = 0usize;
        for (subject, body) in live {
            let subject_str = std::str::from_utf8(&subject).map_err(|_| {
                StoreError::BadEnvelope("non-utf8 subject cannot be materialised via put")
            })?;
            dest_store.put(subject_str, &body, DurabilityMode::Durable)?;
            subjects_copied += 1;
        }
        // Best-effort seal so destination is fully self-describing on disk.
        let _ = dest_store.seal_active();
        let _ = dest_store.persist_index_cache();
        let _ = dest_store.rebuild_catalogs();
        Ok(SalvageCopyReport {
            source,
            destination: dest.to_path_buf(),
            mode: crate::recovery::SalvageMode::LiveStateExport,
            subjects_copied,
            frames_copied: 0,
            holes_recorded: 0,
            manifest_path: None,
        })
    }

    /// Stable scan-report names and raw bytes for every authoritative segment
    /// object (sealed + active), ordered for deterministic examination.
    ///
    /// Source strings are relative to the store root (`segments/….residiuum`,
    /// `active/active.residiuum`). Does not mutate disk. Used by Stage 5
    /// (`residiuum-examine`) to project [`residiuum_format`] salvage regions into
    /// examination units without depending on catalogs or indexes.
    pub fn examination_sources(&self) -> Result<Vec<(String, Vec<u8>)>, StoreError> {
        let mut out = Vec::new();
        for path in all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )? {
            let source = examination_source_name(&self.paths.root, &path);
            let bytes = fs::read(&path)?;
            out.push((source, bytes));
        }
        Ok(out)
    }

    /// Safety limits applied to frame verification and salvage scans.
    pub fn safety_limits(&self) -> SafetyLimits {
        self.limits
    }

    /// Seal every active writer shard, moving each to `segments/`.
    ///
    /// Seal work is **O(active segment size)** for placement + catalog update.
    /// It does **not** rewrite the full primary index cache (that was O(N) in
    /// live subjects and caused write-throughput collapse at GB scale). Use
    /// [`Self::persist_index_cache`] when a durable frontier checkpoint is wanted.
    ///
    /// Always **synchronous** for authoritative publish + catalog visibility
    /// (including CompactShadow protected-pair finalize). Derived Hydra/Chimera
    /// may still enqueue asynchronously under CompactShadow — call
    /// [`Self::drain_lifecycle`] when sidecars must be present (DEF-096).
    /// Drains any in-flight async seals first, then waits for seals submitted
    /// by this call.
    pub fn seal_active(&mut self) -> Result<(), StoreError> {
        self.seal_active_with_breakdown().map(|_| ())
    }

    /// Diagnostic: same as [`Self::seal_active`] but returns stage timings.
    ///
    /// Measurement only — does not change seal semantics. Splits
    /// `drain_lifecycle`, final active seal/publish, catalog publication,
    /// Hydra, and Chimera so campaigns can isolate interference.
    pub fn seal_active_with_breakdown(&mut self) -> Result<SealStageBreakdown, StoreError> {
        let mut out = SealStageBreakdown::default();
        let t_drain = std::time::Instant::now();
        // Wait for in-flight authoritative seals only — do not apply EnrichDone
        // here (that re-serialized derived work onto the seal critical path).
        self.wait_seals_applied()?;
        out.drain_lifecycle_ns = elapsed_ns(t_drain);
        let n = self.writer_shards();
        for shard in 0..n {
            self.seal_active_shard_timed(shard, &mut out)?;
        }
        // Explicit seal_active is synchronous for authoritative publish (DEF-096):
        // CompactShadow may enqueue protected-pair finalize; wait until applied
        // so pending_seal_inflight==0 and sealed media is visible.
        let t_wait = std::time::Instant::now();
        self.wait_seals_applied()?;
        out.drain_lifecycle_ns = out.drain_lifecycle_ns.saturating_add(elapsed_ns(t_wait));
        Ok(out)
    }

    /// Synchronously seal one writer shard (DEF-096 Axis B).
    fn seal_active_shard(&mut self, shard: usize) -> Result<(), StoreError> {
        let mut unused = SealStageBreakdown::default();
        self.seal_active_shard_timed(shard, &mut unused)
    }

    fn seal_active_shard_timed(
        &mut self,
        shard: usize,
        breakdown: &mut SealStageBreakdown,
    ) -> Result<(), StoreError> {
        let t_final = std::time::Instant::now();
        let Some(mut writer) = self.take_active(shard) else {
            return Ok(());
        };
        // Flush strength matches strongest put ack on this segment (not always Durable).
        let flush_mode = seal_flush_mode(writer.max_ack_durability);
        self.flush_active_file(&mut writer, flush_mode, shard as u32)?;
        Self::flush_writer_coalesce(&mut writer)?;
        // After flush, on-disk active length is the verified prefix (no summary yet).
        let prefix_len = writer.durable_len;
        let sealed_id = writer.segment_id;
        let mut shadow_dual = writer.shadow_dual.take();
        let active_path = self
            .paths
            .active_segment_for_shard(shard, self.writer_shards());
        let dest = self.paths.sealed_segment(&sealed_id);

        // Write-through may have discarded the RAM prefix — seal from disk when
        // base_offset > 0 (same bytes as the flushed active file).
        let sealed_owned: Vec<u8> = if writer.segment.base_offset() == 0 {
            let sealed = writer.segment.seal()?;
            drop(writer.file); // release handle before rename / rewrite
            sealed.into_bytes()
        } else {
            drop(writer); // close file; durable prefix is on active_path
            let raw = fs::read(&active_path)?;
            let (bytes, _) = crate::seal_pipeline::seal_pending_image(
                raw,
                self.store_id,
                sealed_id,
                self.limits,
            )?;
            bytes
        };
        let bytes = sealed_owned.as_slice();
        let size = bytes.len() as u64;
        let summary = if (prefix_len as usize) < bytes.len() {
            &bytes[prefix_len as usize..]
        } else {
            &[]
        };
        let pair_async = shadow_dual.is_some() && self.seal_pipeline.is_some();
        let pending_dir = self.paths.pending_seal_dir();
        let pending_path = self.paths.pending_segment(&sealed_id);
        if pair_async {
            fs::create_dir_all(&pending_dir)?;
        }
        let publish_dest = if pair_async {
            pending_path.clone()
        } else {
            dest.clone()
        };

        crate::failpoint::hit("store.seal.before_dest_write")?;

        // Prefer append-summary + rename (no full segment rewrite).
        // Pre-sized actives (watermark / diag prealloc) have EOF past the durable
        // prefix — `OpenOptions::append` would write the summary after the reserved
        // tail and rename a multi-hundred-MiB file. Truncate to the verified prefix,
        // then write the summary at that offset.
        let mut published = false;
        if (prefix_len as usize) < bytes.len() {
            {
                let mut f = OpenOptions::new().write(true).open(&active_path)?;
                f.set_len(prefix_len)?;
                f.seek(SeekFrom::Start(prefix_len))?;
                f.write_all(summary)?;
                // Protected-pair: skip Durable sync on the foreground — worker
                // fsyncs after detach. Sync path still pays sync when not async.
                if flush_mode == DurabilityMode::Durable && !pair_async {
                    f.sync_all()?;
                }
            }
            match crate::media_inventory::rename_exclusive(&active_path, &publish_dest, sealed_id) {
                Ok(()) => published = true,
                Err(StoreError::SegmentIdCollision { .. }) => {
                    return Err(StoreError::SegmentIdCollision {
                        segment_id: sealed_id,
                        paths: vec![active_path.clone(), publish_dest.clone()],
                    });
                }
                Err(_) => {
                    // Cross-device / exotic FS: fall through to staging copy.
                }
            }
        }
        if !published {
            let tmp = crate::atomic_file::temp_path_for(&publish_dest);
            {
                let mut out = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .truncate(true)
                    .open(&tmp)?;
                out.write_all(bytes)?;
                if flush_mode == DurabilityMode::Durable && !pair_async {
                    out.sync_all()?;
                }
            }
            crate::media_inventory::rename_exclusive(&tmp, &publish_dest, sealed_id)?;
            if active_path.exists() {
                fs::remove_file(&active_path)?;
            }
        }
        crate::failpoint::hit("store.seal.after_dest_sync")?;
        if flush_mode == DurabilityMode::Durable && !pair_async {
            sync_dir(&self.paths.segments_dir())?;
            sync_dir(&self.paths.active_shard_dir(shard, self.writer_shards()))?;
        }
        crate::failpoint::hit("store.seal.after_active_remove")?;
        breakdown.final_active_seal_ns = breakdown
            .final_active_seal_ns
            .saturating_add(elapsed_ns(t_final));

        // Protected seal-pair pipeline: prepare Shadow without sync, start next
        // active, finalize auth+Shadow+frontier asynchronously.
        if let Some(dual) = shadow_dual.take() {
            let t_shadow = Instant::now();
            if dual.is_poisoned() || dual.image_len() != prefix_len {
                return Err(StoreError::CorruptMeta(
                    "dual-stream Shadow staging diverged from authoritative prefix; refuse P★",
                ));
            }
            if pair_async {
                let prepared = dual.prepare_async_publish(summary, shard as u16)?;
                prepared.persist_shard_meta(&self.paths)?;
                breakdown.shadow_dual_ns = breakdown
                    .shadow_dual_ns
                    .saturating_add(elapsed_ns(t_shadow));

                crate::segment_allocator::note_in_memory_high_water(
                    &mut self.segment_seq,
                    segment_seq_from_id(&sealed_id),
                );
                let t_reopen = std::time::Instant::now();
                self.start_active_segment_with_mode(shard, flush_mode)?;
                self.persist_active_shard(shard, flush_mode)?;
                breakdown.reopen_active_ns = breakdown
                    .reopen_active_ns
                    .saturating_add(elapsed_ns(t_reopen));

                // Backpressure if protection worker is behind.
                while self
                    .seal_pipeline
                    .as_ref()
                    .map(|p| p.inflight_seals >= p.max_pending_seals)
                    .unwrap_or(false)
                {
                    if !self.wait_one_lifecycle()? {
                        break;
                    }
                }
                if let Some(pipe) = self.seal_pipeline.as_mut() {
                    pipe.inflight_seals = pipe.inflight_seals.saturating_add(1);
                    pipe.submit_seal(LifecycleJob::FinalizeProtectedPair {
                        store_id: self.store_id,
                        segment_id: sealed_id,
                        shard: shard as u16,
                        pending_path,
                        sealed_path: dest,
                        prepared_shadow: prepared,
                        paths: self.paths.clone(),
                        require_fsync: flush_mode == DurabilityMode::Durable,
                        size,
                    })?;
                }
                // Catalog + enrichment applied on ProtectedPairDone (writer).
                return Ok(());
            }

            // Sync fallback (no pipeline): publish Shadow + frontier here.
            let timing = dual.finalize_publish(&self.paths, summary)?;
            self.shadow_dual_finalize_ns = self.shadow_dual_finalize_ns.saturating_add(
                timing
                    .append_summary_ns
                    .saturating_add(timing.encode_ns)
                    .saturating_add(timing.file_sync_ns)
                    .saturating_add(timing.rename_ns)
                    .saturating_add(timing.dir_sync_ns),
            );
            self.shadow_dual_published = self.shadow_dual_published.saturating_add(1);
            let _ = crate::recovery_shadow::note_segment_sealed(
                &self.paths,
                self.store_id,
                &sealed_id,
                shard as u16,
            );
            crate::failpoint::hit("rshd4.frontier.publish")?;
            let seq = crate::ids::segment_seq_from_id(&sealed_id);
            let mut cov =
                crate::recovery_shadow::load_protected_coverage(&self.paths, self.store_id)?;
            cov.store_id = self.store_id;
            cov.note_durable(shard as u16, seq);
            crate::recovery_shadow::publish_protected_coverage(&self.paths, &cov)?;
            breakdown.shadow_dual_ns = breakdown
                .shadow_dual_ns
                .saturating_add(elapsed_ns(t_shadow));
        }

        // Non-dual (or sync dual fallback) continue: start next active + catalog.
        crate::segment_allocator::note_in_memory_high_water(
            &mut self.segment_seq,
            segment_seq_from_id(&sealed_id),
        );
        let t_reopen = std::time::Instant::now();
        self.start_active_segment_with_mode(shard, flush_mode)?;
        self.persist_active_shard(shard, flush_mode)?;
        breakdown.reopen_active_ns = breakdown
            .reopen_active_ns
            .saturating_add(elapsed_ns(t_reopen));

        // Explicit `seal_active`: authoritative publish + Shadow dual finalize
        // stay synchronous (P★). Whole-segment BLAKE3 / Hydra / Chimera are
        // derived — enqueue EnrichDerived like auto-rotate (DEF defer BLAKE3).
        let t_cat = Instant::now();
        let content_hash = crate::incremental_seal::ContentHashState::Pending;
        breakdown.content_hash_ns = breakdown.content_hash_ns.saturating_add(0);
        let _ = register_hot_segment_known(
            &self.paths,
            &mut self.tier_placement,
            sealed_id,
            content_hash,
            size,
        );
        let _ = self.note_sealed_segment(sealed_id, TierClass::Hot, bytes, content_hash, size);
        self.note_derived_catalog_dirty();
        breakdown.catalog_publication_ns = breakdown
            .catalog_publication_ns
            .saturating_add(elapsed_ns(t_cat));
        self.maybe_schedule_derived_catalog_checkpoint(false);
        if self.enrichment_enabled {
            if self.recovery_mode.omits_new_materialized() {
                // CompactShadow: P★ Shadow already published; Compact Chimera is
                // derived — enqueue like auto-rotate so seal wall ≈ auth+Shadow.
                if let Some(pipe) = self.seal_pipeline.as_mut() {
                    let t_enq = Instant::now();
                    let _ = pipe.submit_enrichment(LifecycleJob::EnrichDerived {
                        store_id: self.store_id,
                        segment_id: sealed_id,
                        paths: self.paths.clone(),
                        limits: self.limits,
                    });
                    breakdown.hydra_ns = breakdown.hydra_ns.saturating_add(elapsed_ns(t_enq));
                } else {
                    let t_hydra = std::time::Instant::now();
                    let _ = self.write_hydra_for_sealed(sealed_id, bytes);
                    breakdown.hydra_ns = breakdown.hydra_ns.saturating_add(elapsed_ns(t_hydra));
                    let t_chimera = std::time::Instant::now();
                    let _ = self.write_chimera_for_sealed_bytes(sealed_id, bytes);
                    breakdown.chimera_ns =
                        breakdown.chimera_ns.saturating_add(elapsed_ns(t_chimera));
                }
            } else {
                // Materialized dual-run: keep sync Chimera on explicit seal so
                // operators/tests see `.cmr` before the next statement.
                let t_hydra = std::time::Instant::now();
                let _ = self.write_hydra_for_sealed(sealed_id, bytes);
                breakdown.hydra_ns = breakdown.hydra_ns.saturating_add(elapsed_ns(t_hydra));
                let t_chimera = std::time::Instant::now();
                let _ = self.write_chimera_for_sealed_bytes(sealed_id, bytes);
                breakdown.chimera_ns = breakdown.chimera_ns.saturating_add(elapsed_ns(t_chimera));
            }
        }
        // Deliberately no full index-cache rewrite here (DEF-023 scale).
        Ok(())
    }

    /// Derived enrichment backlog (Hydra/Chimera jobs not yet reported done).
    pub fn enrichment_backlog(&self) -> usize {
        self.seal_pipeline
            .as_ref()
            .map(|p| p.enrichment_backlog)
            .unwrap_or(0)
    }

    /// Cumulative mid-run auto-rotation stage timings.
    pub fn rotation_stage_totals(&self) -> RotationStageTotals {
        self.rotation_stage_totals
    }

    /// Cumulative derived enrichment stage timings (ETQ-0 measurement).
    pub fn enrichment_stage_totals(&self) -> EnrichmentStageTotals {
        self.enrichment_stage_totals
    }

    /// Content-hash state for a sealed segment in the derived catalog (tests).
    pub fn sealed_content_hash_state(
        &self,
        segment_id: &[u8; 16],
    ) -> Option<crate::incremental_seal::ContentHashState> {
        self.segment_catalog
            .get(segment_id)
            .map(|s| s.content_hash)
            .or_else(|| self.tier_placement.get(segment_id).map(|p| p.content_hash))
    }

    /// Best-effort wait for derived enrichment to drain (measurement / tests).
    ///
    /// Never used on the put acknowledgement path.
    pub fn drain_enrichment(&mut self, timeout: std::time::Duration) -> Result<(), StoreError> {
        let deadline = std::time::Instant::now() + timeout;
        while self.enrichment_backlog() > 0 {
            if std::time::Instant::now() >= deadline {
                return Ok(());
            }
            if !self.wait_one_lifecycle()? {
                while self.poll_lifecycle()? {}
                if self.enrichment_backlog() == 0 {
                    return Ok(());
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
        Ok(())
    }

    /// Whether auto-seal uses the async lifecycle pipeline (DEF-096 Axis A).
    pub fn async_lifecycle_enabled(&self) -> bool {
        self.async_lifecycle && self.seal_pipeline.is_some()
    }

    /// Enable or disable async auto-seal (tests / operators). Default: enabled
    /// for writer opens.
    pub fn set_async_lifecycle(&mut self, enabled: bool) {
        self.async_lifecycle = enabled;
    }

    /// Enable or disable derived enrichment (Hydra/Chimera) after seal.
    ///
    /// Measurement control: disable during ack TPS to isolate authoritative
    /// finalisation cost from concurrent enrichment contention.
    pub fn set_enrichment_enabled(&mut self, enabled: bool) {
        self.enrichment_enabled = enabled;
    }

    /// Experimental write-time dual-stream Recovery Shadow (RSHD0004).
    ///
    /// Affects newly started active segments. Call
    /// [`Self::attach_shadow_dual_to_actives`] to also arm the current actives.
    /// Not a product flip.
    pub fn set_shadow_dual_stream(&mut self, enabled: bool) {
        self.shadow_dual_stream = enabled;
        // Dual-stream uses the Protected Seal-Pair Pipeline (async finalize).
        // Do **not** disable async_lifecycle — that serialized ~167ms/seal onto
        // the writer. Keep async so detach → next active overlaps protection.
    }

    /// Attach dual-stream staging to current actives (seeded from durable prefix).
    ///
    /// Used by qualification harnesses that enable dual-stream after `create`.
    pub fn attach_shadow_dual_to_actives(&mut self) -> Result<(), StoreError> {
        self.set_shadow_dual_stream(true);
        let n = self.writer_shards();
        let store_id = self.store_id;
        let paths = self.paths.clone();
        for shard in 0..n {
            let Some(writer) = self.active_mut(shard) else {
                continue;
            };
            if writer.shadow_dual.is_some() {
                continue;
            }
            let durable = writer.durable_len as usize;
            let mut prefix = vec![0u8; durable];
            if durable > 0 {
                use std::io::Read;
                writer.file.seek(SeekFrom::Start(0))?;
                writer.file.read_exact(&mut prefix)?;
                writer.file.seek(SeekFrom::Start(writer.durable_len))?;
            }
            let segment_id = writer.segment_id;
            let mut dual =
                crate::recovery_shadow::ShadowDualStream::begin(&paths, store_id, segment_id)?;
            if !prefix.is_empty() {
                dual.append_image_chunk(&prefix)?;
            }
            writer.shadow_dual = Some(dual);
        }
        Ok(())
    }

    /// Whether experimental dual-stream Shadow staging is enabled.
    pub fn shadow_dual_stream(&self) -> bool {
        self.shadow_dual_stream
    }

    /// Cumulative dual-stream finalize nanoseconds (summary+sync+rename+dir).
    pub fn shadow_dual_finalize_ns(&self) -> u64 {
        self.shadow_dual_finalize_ns
    }

    /// Dual-stream Shadows published since open.
    pub fn shadow_dual_published(&self) -> u64 {
        self.shadow_dual_published
    }

    /// Current durable recovery mode (Step 8).
    pub fn recovery_mode(&self) -> crate::recovery_shadow::RecoveryMode {
        self.recovery_mode
    }

    /// Reload recovery mode from disk and apply process policy.
    pub fn reload_recovery_mode(&mut self) -> Result<(), StoreError> {
        let mode = crate::recovery_shadow::load_recovery_mode(&self.paths)?;
        self.apply_recovery_mode(mode);
        Ok(())
    }

    fn apply_recovery_mode(&mut self, mode: crate::recovery_shadow::RecoveryMode) {
        self.recovery_mode = mode;
        match mode {
            crate::recovery_shadow::RecoveryMode::Materialized => {
                crate::recovery_shadow::set_shadow_reclaim_policy(
                    crate::recovery_shadow::ShadowReclaimPolicy::DualRunMaterializedAuthority,
                );
            }
            crate::recovery_shadow::RecoveryMode::Transitioning => {
                let _ = self.attach_shadow_dual_to_actives();
            }
            crate::recovery_shadow::RecoveryMode::CompactShadow => {
                let _ = self.attach_shadow_dual_to_actives();
                crate::recovery_shadow::set_shadow_reclaim_policy(
                    crate::recovery_shadow::ShadowReclaimPolicy::RequireReplacementShadow,
                );
            }
        }
    }

    /// Step 8 prepare: Transitioning marker + backfill Shadows + gap-free check.
    pub fn prepare_flip_to_compact_shadow(&mut self) -> Result<u64, StoreError> {
        let built =
            crate::recovery_shadow::prepare_flip_to_compact_shadow(&self.paths, self.store_id, 0)?;
        self.apply_recovery_mode(crate::recovery_shadow::RecoveryMode::Transitioning);
        Ok(built)
    }

    /// Step 8 activate: durable CompactShadow marker, then stop new Materialized.
    pub fn activate_compact_shadow_mode(&mut self) -> Result<(), StoreError> {
        crate::recovery_shadow::activate_compact_shadow_mode(&self.paths, self.store_id)?;
        self.apply_recovery_mode(crate::recovery_shadow::RecoveryMode::CompactShadow);
        Ok(())
    }

    /// Step 8 rollback: Materialized dual-run; keep Shadows and Materialized files.
    pub fn rollback_to_materialized_mode(&mut self) -> Result<(), StoreError> {
        crate::recovery_shadow::rollback_to_materialized_mode(&self.paths, self.store_id)?;
        self.apply_recovery_mode(crate::recovery_shadow::RecoveryMode::Materialized);
        // Dual-stream may stay attached for experimental use; product default off.
        self.shadow_dual_stream = false;
        Ok(())
    }

    /// Whether derived enrichment is enqueued after authoritative seal.
    pub fn enrichment_enabled(&self) -> bool {
        self.enrichment_enabled
    }

    /// In-flight background seals not yet applied to in-memory catalogs.
    pub fn pending_seal_inflight(&self) -> usize {
        self.seal_pipeline
            .as_ref()
            .map(|p| p.inflight_seals)
            .unwrap_or(0)
    }

    /// Wait for in-flight authoritative seals and apply them to **in-memory**
    /// catalogs. Does **not** persist derived catalogs (checkpoints may lag).
    pub fn wait_seals_applied(&mut self) -> Result<(), StoreError> {
        loop {
            let inflight = self
                .seal_pipeline
                .as_ref()
                .map(|p| p.inflight_seals)
                .unwrap_or(0);
            if inflight == 0 {
                // Do not drain EnrichDone here — seal critical path must not
                // serialize derived Hydra/Chimera apply between seals.
                return Ok(());
            }
            if !self.wait_one_lifecycle()? {
                let _ = recover_all_pending(&self.paths, self.store_id, self.limits)?;
                if let Some(p) = self.seal_pipeline.as_mut() {
                    p.inflight_seals = 0;
                }
                return Ok(());
            }
        }
    }

    /// Drain the seal pipeline: wait for finalizes, apply pending results
    /// (including enrichment), then best-effort flush derived catalogs.
    pub fn drain_lifecycle(&mut self) -> Result<(), StoreError> {
        self.wait_seals_applied()?;
        while self.poll_lifecycle()? {}
        self.flush_derived_catalogs_best_effort();
        Ok(())
    }

    /// Non-blocking: apply completed lifecycle results. Returns true if any applied.
    fn poll_lifecycle(&mut self) -> Result<bool, StoreError> {
        let mut any = false;
        loop {
            let Some(result) = self.seal_pipeline.as_ref().and_then(|p| p.try_recv()) else {
                break;
            };
            self.apply_lifecycle_result(result)?;
            any = true;
        }
        Ok(any)
    }

    /// Block for one lifecycle result and apply it. Returns false if none/disconnected.
    fn wait_one_lifecycle(&mut self) -> Result<bool, StoreError> {
        let Some(result) = self.seal_pipeline.as_ref().and_then(|p| p.recv()) else {
            return Ok(false);
        };
        self.apply_lifecycle_result(result)?;
        Ok(true)
    }

    fn apply_lifecycle_result(&mut self, result: LifecycleResult) -> Result<(), StoreError> {
        match result {
            LifecycleResult::SealDone {
                segment_id,
                content_hash,
                size,
                summary,
                auth_publish_ns,
            } => {
                if let Some(p) = self.seal_pipeline.as_mut() {
                    p.inflight_seals = p.inflight_seals.saturating_sub(1);
                }
                // In-memory only on the writer path (O(1)). Disk checkpoints
                // coalesce asynchronously — never rewrite full catalogs here.
                let t0 = Instant::now();
                let _ = register_hot_segment_known(
                    &self.paths,
                    &mut self.tier_placement,
                    segment_id,
                    content_hash,
                    size,
                );
                let _ = self.note_sealed_summary(summary);
                self.catalog_dirty = true;
                self.catalog_seals_since_checkpoint =
                    self.catalog_seals_since_checkpoint.saturating_add(1);
                let catalog_ns = elapsed_ns(t0);
                self.rotation_stage_totals.rotations =
                    self.rotation_stage_totals.rotations.saturating_add(1);
                self.rotation_stage_totals.auth_publish_ns = self
                    .rotation_stage_totals
                    .auth_publish_ns
                    .saturating_add(auth_publish_ns);
                self.rotation_stage_totals.catalog_apply_ns = self
                    .rotation_stage_totals
                    .catalog_apply_ns
                    .saturating_add(catalog_ns);
                self.maybe_schedule_derived_catalog_checkpoint(false);
                // Enrich only after authoritative publish — sealed bytes must exist.
                if self.enrichment_enabled {
                    if let Some(pipe) = self.seal_pipeline.as_mut() {
                        let _ = pipe.submit_enrichment(LifecycleJob::EnrichDerived {
                            store_id: self.store_id,
                            segment_id,
                            paths: self.paths.clone(),
                            limits: self.limits,
                        });
                    }
                }
            }
            LifecycleResult::SealFailed { segment_id, error } => {
                if let Some(p) = self.seal_pipeline.as_mut() {
                    p.inflight_seals = p.inflight_seals.saturating_sub(1);
                }
                // Best-effort **auth** recovery only. Never run Chimera/enrichment
                // here — Materialized enrich dual-writes a mirror `.rsh` and would
                // falsely advance P★ after a protected-pair Shadow failure.
                let pending = self.paths.pending_segment(&segment_id);
                let sealed = self.paths.sealed_segment(&segment_id);
                let _ = crate::seal_pipeline::finalize_seal_authoritative(
                    self.store_id,
                    segment_id,
                    &pending,
                    &sealed,
                    self.limits,
                    true,
                );
                return Err(StoreError::Io(std::io::Error::other(format!(
                    "seal/pair failed for {}: {error}",
                    crate::layout::hex16(&segment_id)
                ))));
            }
            LifecycleResult::CheckpointDone { .. } => {}
            LifecycleResult::EnrichDone {
                segment_id,
                content_hash,
                size,
                stages,
                ok: _,
            } => {
                if let Some(p) = self.seal_pipeline.as_mut() {
                    p.enrichment_backlog = p.enrichment_backlog.saturating_sub(1);
                }
                let t_catalog = Instant::now();
                if size > 0 && !content_hash.is_pending() {
                    // Memory digest refresh only; durable catalogs lag.
                    let _ = register_hot_segment_known(
                        &self.paths,
                        &mut self.tier_placement,
                        segment_id,
                        content_hash,
                        size,
                    );
                    if let Some(prior) = self.segment_catalog.get(&segment_id).cloned() {
                        let mut updated = prior;
                        updated.content_hash = content_hash;
                        updated.size = size;
                        let _ = self.note_sealed_summary(updated);
                    }
                    self.note_derived_catalog_dirty();
                    self.maybe_schedule_derived_catalog_checkpoint(false);
                }
                let catalog_ns = elapsed_ns(t_catalog);
                if let Some(mut s) = stages {
                    s.catalog_ns = catalog_ns;
                    self.enrichment_stage_totals.accumulate(s);
                }
            }
            LifecycleResult::ProtectedPairDone {
                segment_id,
                size,
                auth_publish_ns,
                shadow_publish_ns,
            } => {
                if let Some(p) = self.seal_pipeline.as_mut() {
                    p.inflight_seals = p.inflight_seals.saturating_sub(1);
                }
                self.shadow_dual_published = self.shadow_dual_published.saturating_add(1);
                self.shadow_dual_finalize_ns = self
                    .shadow_dual_finalize_ns
                    .saturating_add(shadow_publish_ns);
                self.rotation_stage_totals.rotations =
                    self.rotation_stage_totals.rotations.saturating_add(1);
                self.rotation_stage_totals.auth_publish_ns = self
                    .rotation_stage_totals
                    .auth_publish_ns
                    .saturating_add(auth_publish_ns);
                let content_hash = crate::incremental_seal::ContentHashState::Pending;
                let _ = register_hot_segment_known(
                    &self.paths,
                    &mut self.tier_placement,
                    segment_id,
                    content_hash,
                    size,
                );
                // Hierarchical catalog must see the sealed segment immediately
                // (tiering / seal-cost / list_segment_summaries). Sync seal path
                // calls note_sealed_segment; protected-pair finalize must match.
                let sealed_path = self.paths.sealed_segment(&segment_id);
                if sealed_path.is_file() {
                    if let Ok(bytes) = fs::read(&sealed_path) {
                        let _ = self.note_sealed_segment(
                            segment_id,
                            TierClass::Hot,
                            &bytes,
                            content_hash,
                            size,
                        );
                    }
                }
                self.note_derived_catalog_dirty();
                self.maybe_schedule_derived_catalog_checkpoint(false);
                if self.enrichment_enabled {
                    if let Some(pipe) = self.seal_pipeline.as_mut() {
                        let _ = pipe.submit_enrichment(LifecycleJob::EnrichDerived {
                            store_id: self.store_id,
                            segment_id,
                            paths: self.paths.clone(),
                            limits: self.limits,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Zero-scan foreground rotate: flush → rename active→pending → open next
    /// active → append precomputed summary → sync/rename pending→sealed → apply
    /// compact catalog metadata (no 64 MiB `Vec` on the writer path).
    ///
    /// Derived Hydra/Chimera run on a **separate** enrichment worker with an I/O
    /// gap and never count toward `max_pending_seals` backpressure.
    fn rotate_active_async(&mut self, shard: usize) -> Result<(), StoreError> {
        let _ = self.poll_lifecycle();
        // Backpressure only for authoritative finalize lag (not enrichment).
        let t_bp = std::time::Instant::now();
        while self
            .seal_pipeline
            .as_ref()
            .map(|p| p.inflight_seals >= p.max_pending_seals)
            .unwrap_or(false)
        {
            if !self.wait_one_lifecycle()? {
                break;
            }
        }
        self.rotation_stage_totals.backpressure_wait_ns = self
            .rotation_stage_totals
            .backpressure_wait_ns
            .saturating_add(elapsed_ns(t_bp));

        let Some(mut writer) = self.take_active(shard) else {
            return Ok(());
        };
        let flush_mode = seal_flush_mode(writer.max_ack_durability);
        let t_flush = std::time::Instant::now();
        self.flush_active_file(&mut writer, flush_mode, shard as u32)?;
        Self::flush_writer_coalesce(&mut writer)?;
        self.rotation_stage_totals.flush_ns = self
            .rotation_stage_totals
            .flush_ns
            .saturating_add(elapsed_ns(t_flush));
        let segment_id = writer.segment_id;
        let require_fsync = flush_mode == DurabilityMode::Durable;
        let prefix_len = writer.durable_len;
        let ids = writer.segment.ids();
        let frame_count = writer.segment.frame_count();
        let writer_sequence = writer.segment.writer_sequence();

        let item_events = writer.item_events;
        // Meta publish (summary footer only; no pending read / no auth BLAKE3).
        // Whole-segment hash is derived — EnrichDerived fills it. Put-path and
        // write-tail hashing stay off (measured regressions). See
        // doc/archive/performance-qualification/2026-08-04-defer-segment-blake3/.
        let zero_scan_meta = prefix_len > 0 && frame_count > 0;
        drop(writer);

        let n = self.writer_shards();
        let active_path = self.paths.active_segment_for_shard(shard, n);
        let pending_dir = self.paths.pending_seal_dir();
        fs::create_dir_all(&pending_dir)?;
        let pending_path = self.paths.pending_segment(&segment_id);
        if pending_path.exists() {
            let sealed = self.paths.sealed_segment(&segment_id);
            let _ = crate::seal_pipeline::finalize_seal(
                self.store_id,
                segment_id,
                &pending_path,
                &sealed,
                self.limits,
                &self.paths,
                true,
            );
        }
        let t_rename = std::time::Instant::now();
        fs::rename(&active_path, &pending_path)?;
        if require_fsync {
            sync_dir(&self.paths.active_shard_dir(shard, n))?;
            let _ = sync_dir(&pending_dir);
        }
        self.rotation_stage_totals.rename_pending_ns = self
            .rotation_stage_totals
            .rename_pending_ns
            .saturating_add(elapsed_ns(t_rename));

        // Open next active immediately (put path unblocked for new frames).
        // `start_active_segment_with_mode` owns the `segment_seq` increment.
        // Same active-filename under-count as sync seal — bump before mint.
        crate::segment_allocator::note_in_memory_high_water(
            &mut self.segment_seq,
            segment_seq_from_id(&segment_id),
        );
        let t_start = std::time::Instant::now();
        self.start_active_segment_with_mode(shard, flush_mode)?;
        self.persist_active_shard(shard, flush_mode)?;
        self.rotation_stage_totals.start_active_ns = self
            .rotation_stage_totals
            .start_active_ns
            .saturating_add(elapsed_ns(t_start));

        if let Some(pipe) = self.seal_pipeline.as_mut() {
            pipe.inflight_seals = pipe.inflight_seals.saturating_add(1);
            let max = pipe
                .max_pending_seals
                .max(DEFAULT_MAX_PENDING_SEALS.saturating_mul(n.max(1)));
            pipe.max_pending_seals = max;
            if zero_scan_meta {
                pipe.submit_seal(LifecycleJob::FinalizeSealMeta {
                    ids,
                    segment_id,
                    prefix_len,
                    frame_count,
                    writer_sequence,
                    item_events,
                    pending_path: pending_path.clone(),
                    sealed_path: self.paths.sealed_segment(&segment_id),
                    require_fsync,
                })?;
            } else {
                pipe.submit_seal(LifecycleJob::FinalizeSeal {
                    store_id: self.store_id,
                    segment_id,
                    pending_path: pending_path.clone(),
                    sealed_path: self.paths.sealed_segment(&segment_id),
                    limits: self.limits,
                    paths: self.paths.clone(),
                    require_fsync,
                })?;
            }
            // EnrichDerived is submitted from SealDone apply (sealed file ready).
        } else if zero_scan_meta {
            let plan = crate::incremental_seal::meta_publish_plan(
                ids,
                prefix_len,
                frame_count,
                writer_sequence,
                item_events,
            )?;
            let sealed_path = self.paths.sealed_segment(&segment_id);
            publish_sealed_from_summary_frame(
                &pending_path,
                &sealed_path,
                prefix_len,
                &plan.summary_frame,
                require_fsync,
            )?;
            let _ = register_hot_segment_known(
                &self.paths,
                &mut self.tier_placement,
                segment_id,
                plan.content_hash,
                plan.sealed_len,
            );
            self.note_sealed_summary(plan.to_segment_summary())?;
            self.note_derived_catalog_dirty();
            self.maybe_schedule_derived_catalog_checkpoint(false);
        } else {
            let sealed = self.paths.sealed_segment(&segment_id);
            let (content_hash, size, sealed_bytes) = crate::seal_pipeline::finalize_seal(
                self.store_id,
                segment_id,
                &pending_path,
                &sealed,
                self.limits,
                &self.paths,
                require_fsync,
            )?;
            let hash = crate::incremental_seal::ContentHashState::Known(content_hash);
            let _ = register_hot_segment_known(
                &self.paths,
                &mut self.tier_placement,
                segment_id,
                hash,
                size,
            );
            let _ = self.note_sealed_segment(segment_id, TierClass::Hot, &sealed_bytes, hash, size);
            self.note_derived_catalog_dirty();
            self.maybe_schedule_derived_catalog_checkpoint(false);
        }
        Ok(())
    }

    /// Seal or rotate when the given shard's active segment is at/over threshold.
    ///
    /// Fast path (under threshold) is a single length compare. Seal work is timed
    /// on the probe as `segment_rotate` / `lifecycle_seal` so it does **not** get
    /// mixed into Mode A `put_prep` (which is the per-put hot path only).
    fn maybe_auto_seal(&mut self, shard: usize) -> Result<(), StoreError> {
        let need = self
            .active_ref(shard)
            .map(|w| w.segment.len() >= self.seal_threshold)
            .unwrap_or(false);
        if !need {
            return Ok(());
        }
        let t0 = std::time::Instant::now();
        let r = if self.async_lifecycle_enabled() && !self.shadow_dual_stream {
            self.rotate_active_async(shard)
        } else if self.shadow_dual_stream && self.seal_pipeline.is_some() {
            // Dual-stream: protected pair via explicit seal detach (async P★).
            self.seal_active_shard(shard)
        } else {
            self.seal_active_shard(shard)
        };
        let seal_ns = t0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        if self.boundary_probe_enabled() {
            self.boundary_probe
                .record_segment_rotate_timed(shard as u32, seal_ns);
            if r.is_ok() {
                self.boundary_probe.record_lifecycle_seal(shard as u32);
            }
        }
        r
    }

    /// Compile and persist a Hydra index for one sealed segment (derived only).
    ///
    /// Selection is adaptive: tiny → Eytzinger, ordered numeric → PGM/RadixSpline,
    /// strings → compressed radix, with optional point-only MPHF via rebuild APIs.
    fn write_hydra_for_sealed(&self, segment_id: [u8; 16], bytes: &[u8]) -> Result<(), StoreError> {
        let records = crate::hydra::records_from_segment_bytes(bytes, self.limits);
        if records.is_empty() {
            return Ok(());
        }
        let index = crate::hydra::build(&records, &crate::hydra::HydraBuildOptions::default());
        let path = crate::hydra::hydra_index_path(&self.paths, &segment_id);
        crate::hydra::write_hydra_index(&path, self.store_id, segment_id, &index)
    }

    /// Persist Chimera for a sealed segment.
    ///
    /// - **Materialized / Transitioning:** Materialized layout (product dual-run).
    /// - **CompactShadow (Step 8+):** Compact layout only — no new Materialized.
    ///   Existing `.cmr` Materialized files are retained until operator cleanup.
    fn write_chimera_for_sealed(&self, segment_id: [u8; 16]) -> Result<(), StoreError> {
        if self.recovery_mode.omits_new_materialized() {
            let path = self.paths.sealed_segment(&segment_id);
            if !path.is_file() {
                return Ok(());
            }
            let bytes = fs::read(&path)?;
            return self.write_compact_chimera_from_bytes(segment_id, &bytes);
        }
        let pairs = self.live_put_pairs_for_segment(&segment_id)?;
        if pairs.is_empty() {
            return Ok(());
        }
        let layout = crate::chimera::build_materialized_layout(
            &pairs,
            1,
            &crate::chimera::ClassifyOptions::default(),
        );
        let path = crate::chimera::chimera_layout_path(&self.paths, &segment_id);
        crate::chimera::write_chimera_layout(&path, self.store_id, segment_id, &layout)
    }

    /// Seal-path Chimera using the already-built sealed image (no re-read).
    fn write_chimera_for_sealed_bytes(
        &self,
        segment_id: [u8; 16],
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        if self.recovery_mode.omits_new_materialized() {
            return self.write_compact_chimera_from_bytes(segment_id, bytes);
        }
        self.write_chimera_for_sealed(segment_id)
    }

    fn write_compact_chimera_from_bytes(
        &self,
        segment_id: [u8; 16],
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        let (_live, frames, _lp) =
            crate::recovery_shadow::decode_segment_for_candidate(segment_id, bytes, self.limits);
        if frames.is_empty() {
            return Ok(());
        }
        let layout = crate::chimera::build_compact_layout(&frames, 1);
        let out = crate::chimera::chimera_layout_path(&self.paths, &segment_id);
        crate::chimera::write_chimera_layout(&out, self.store_id, segment_id, &layout)
    }

    /// Materialized Chimera layout for a live-projection compact output (CSE-2R).
    fn write_chimera_for_live_projection(&self, segment_id: [u8; 16]) -> Result<(), StoreError> {
        let pairs = self.live_put_pairs_all()?;
        if pairs.is_empty() {
            return Ok(());
        }
        let layout = crate::chimera::build_materialized_layout(
            &pairs,
            1,
            &crate::chimera::ClassifyOptions::default(),
        );
        let path = crate::chimera::chimera_layout_path(&self.paths, &segment_id);
        crate::chimera::write_chimera_layout(&path, self.store_id, segment_id, &layout)
    }

    /// Live (key, body) pairs established on `segment_id` (for Materialized Chimera).
    fn live_put_pairs_for_segment(
        &self,
        segment_id: &[u8; 16],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let mut pairs = Vec::new();
        for (subject, lv) in self.index.live_entries() {
            if lv.segment_id != *segment_id || lv.frame_offset == 0 {
                continue;
            }
            let body = if !lv.body.is_empty() {
                lv.body.clone()
            } else {
                self.pread_body_for_locator(&lv.segment_id, lv.frame_offset)?
            };
            pairs.push((subject.clone(), body));
        }
        Ok(pairs)
    }

    /// Live (key, body) pairs for all durable live values (live-projection).
    fn live_put_pairs_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let mut pairs = Vec::new();
        for (subject, lv) in self.index.live_entries() {
            if lv.frame_offset == 0 {
                continue;
            }
            let body = if !lv.body.is_empty() {
                lv.body.clone()
            } else {
                self.pread_body_for_locator(&lv.segment_id, lv.frame_offset)?
            };
            pairs.push((subject.clone(), body));
        }
        Ok(pairs)
    }

    /// Resolve a subject via the Chimera layout for `segment_id` when present.
    ///
    /// Loads the full per-segment sidecar from disk (no process-wide cache).
    /// Callers must not put this on the hot get path when a resident body exists.
    fn try_get_via_chimera(
        &self,
        key: &[u8],
        segment_id: &[u8; 16],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let path = crate::chimera::chimera_layout_path(&self.paths, segment_id);
        let Some(layout) =
            crate::chimera::try_load_chimera_layout(&path, self.store_id, *segment_id)?
        else {
            return Ok(None);
        };
        let Some(loc) = layout.locator(key) else {
            return Ok(None);
        };
        match loc {
            crate::chimera::ValueLocator::SegmentFrame {
                segment_id: frame_seg,
                frame_offset,
                body_len,
                ..
            } => {
                let body = self.pread_body_for_locator(frame_seg, *frame_offset)?;
                if *body_len != 0 && body.len() as u32 != *body_len {
                    return Err(StoreError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "chimera segment frame body_len mismatch",
                    )));
                }
                Ok(Some(body))
            }
            other => Ok(Some(layout.resolve_locator(other)?)),
        }
    }

    /// Rebuild Hydra indexes for all available sealed segments (multithread).
    ///
    /// Derived only — safe after catalog wipe. Returns how many indexes were written.
    pub fn rebuild_hydra_indexes(
        &self,
        opts: &crate::hydra::HydraBuildOptions,
    ) -> Result<usize, StoreError> {
        let paths = crate::tier::available_sealed_paths(&self.paths, &self.tier_placement)?;
        if paths.is_empty() {
            return Ok(0);
        }
        let mut batches = Vec::with_capacity(paths.len());
        let mut seg_ids = Vec::with_capacity(paths.len());
        for path in &paths {
            let Some(seg_id) = crate::layout::segment_id_from_filename(path) else {
                continue;
            };
            let bytes = match fs::read(path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let records = crate::hydra::records_from_segment_bytes(&bytes, self.limits);
            if records.is_empty() {
                continue;
            }
            batches.push(records);
            seg_ids.push(seg_id);
        }
        if batches.is_empty() {
            return Ok(0);
        }
        let indexes = crate::hydra::build_many(&batches, opts);
        let mut written = 0usize;
        for (seg_id, index) in seg_ids.into_iter().zip(indexes.into_iter()) {
            let path = crate::hydra::hydra_index_path(&self.paths, &seg_id);
            crate::hydra::write_hydra_index(&path, self.store_id, seg_id, &index)?;
            written += 1;
        }
        Ok(written)
    }

    /// Load the Hydra sidecar for a sealed segment when present and valid.
    pub fn load_hydra_index(
        &self,
        segment_id: [u8; 16],
    ) -> Result<Option<crate::hydra::HydraIndex>, StoreError> {
        let path = crate::hydra::hydra_index_path(&self.paths, &segment_id);
        crate::hydra::try_load_hydra_index(&path, self.store_id, segment_id)
    }

    /// Load the Chimera layout sidecar for a sealed segment when present and valid.
    pub fn load_chimera_layout(
        &self,
        segment_id: [u8; 16],
    ) -> Result<Option<crate::chimera::ChimeraLayout>, StoreError> {
        let path = crate::chimera::chimera_layout_path(&self.paths, &segment_id);
        crate::chimera::try_load_chimera_layout(&path, self.store_id, segment_id)
    }

    /// Rebuild Chimera layouts for all sealed segments that still hold live values.
    ///
    /// Derived only — safe after wiping `indexes/chimera/`. Returns how many
    /// layouts were written.
    pub fn rebuild_chimera_layouts(&self) -> Result<usize, StoreError> {
        let mut written = 0usize;
        let mut seen = HashSet::new();
        for (_subject, lv) in self.index.live_entries() {
            if !seen.insert(lv.segment_id) {
                continue;
            }
            // Only write when the establishing segment file still exists.
            let path = self.paths.sealed_segment(&lv.segment_id);
            if !path.is_file() {
                continue;
            }
            match self.write_chimera_for_sealed(lv.segment_id) {
                Ok(()) => written += 1,
                Err(_) => continue,
            }
        }
        Ok(written)
    }

    /// Paths used for derived state (safe to delete for salvage tests).
    ///
    /// Tier **media** under `tiers/warm|cold|archive` is authoritative when
    /// segments live there; only `catalogs/` placement/summary files are derived.
    pub fn derived_dirs(&self) -> Vec<PathBuf> {
        vec![
            self.paths.catalogs_dir(),
            self.paths.indexes_dir(),
            self.paths.snapshots_dir(),
        ]
    }

    // --- Stage 9: tiers / archive ---

    /// Current tier placement map (segment id → media).
    pub fn tier_placement(&self) -> &TierPlacement {
        &self.tier_placement
    }

    /// Hierarchical segment summary catalog (cold-search accelerator).
    pub fn segment_catalog(&self) -> &SegmentCatalog {
        &self.segment_catalog
    }

    /// Tier coverage for the current open state (offline media → incomplete).
    pub fn tier_coverage(&self) -> TierCoverage {
        self.tier_placement.coverage()
    }

    /// Mark a storage tier online or offline without deleting media.
    ///
    /// Offline tiers create coverage holes; they must not be reported as empty
    /// successful absence (OVERVIEW §9.2).
    pub fn set_tier_available(
        &mut self,
        tier: TierClass,
        available: bool,
    ) -> Result<(), StoreError> {
        self.tier_placement.set_tier_available(tier, available);
        self.persist_tier_state()?;
        // Rebuild index from remaining available segments only.
        self.rebuild_index_from_segments()?;
        let _ = self.persist_index_cache();
        let _ = self.refresh_collection_catalog();
        self.refresh_segment_catalog()?;
        Ok(())
    }

    /// Copy or move a sealed segment to another tier (stable segment identity).
    pub fn transfer_segment_to_tier(
        &mut self,
        segment_id: [u8; 16],
        to_tier: TierClass,
        mode: TierMoveMode,
    ) -> Result<MigrationEvidence, StoreError> {
        // Ensure placement knows about hot sealed segments.
        discover_placements(&self.paths, &mut self.tier_placement)?;
        let evidence = transfer_segment(
            &self.paths,
            &mut self.tier_placement,
            segment_id,
            to_tier,
            mode,
        )?;
        self.persist_tier_state()?;
        self.refresh_segment_catalog()?;
        // Fingerprint changed; refresh derived caches.
        let _ = self.persist_index_cache();
        Ok(evidence)
    }

    /// List sealed segment ids currently registered (any tier).
    pub fn list_segment_ids(&self) -> Vec<[u8; 16]> {
        self.tier_placement
            .entries()
            .map(|p| p.segment_id)
            .collect()
    }

    /// Segment summaries for cold search (hierarchical catalog).
    pub fn list_segment_summaries(&self) -> Vec<SegmentSummary> {
        self.segment_catalog.summaries().cloned().collect()
    }

    /// Rebuild hierarchical segment catalog from available media.
    ///
    /// After catalog loss, offline segments retained in placement keep last-known
    /// metadata when possible; available segments are re-scanned.
    pub fn rebuild_segment_catalog(&mut self) -> Result<(), StoreError> {
        discover_placements(&self.paths, &mut self.tier_placement)?;
        self.refresh_segment_catalog()?;
        self.persist_tier_state()?;
        Ok(())
    }

    /// Get with explicit tier coverage (absence only proven when coverage complete).
    pub fn get_with_tier_coverage(&self, subject: &str) -> Result<TierAwareGet, StoreError> {
        let coverage = self.tier_coverage();
        let value = self.get(subject)?;
        let absence_proven = value.is_none() && coverage.is_complete();
        Ok(TierAwareGet {
            value,
            coverage,
            absence_proven,
        })
    }

    /// Classify a sealed segment file without rewriting bytes (multi-gen readers).
    pub fn classify_segment(
        &self,
        segment_id: &[u8; 16],
    ) -> Result<FormatClassification, StoreError> {
        let path = if let Some(p) = self.tier_placement.get(segment_id) {
            crate::tier::resolve_placement_path(&self.paths, p)?
        } else {
            self.paths.sealed_segment(segment_id)
        };
        if !path.is_file() {
            return Err(StoreError::SegmentNotFound);
        }
        let bytes = fs::read(&path)?;
        Ok(classify_segment_bytes(&bytes))
    }

    /// Soft seal threshold override (tests / operators).
    pub fn set_seal_threshold(&mut self, bytes: u64) {
        if bytes > 0 {
            self.seal_threshold = bytes;
        }
    }

    // --- internals ---

    fn load_tier_state(&mut self) -> Result<(), StoreError> {
        load_tier_roots_file(&self.paths, &mut self.tier_placement);
        let path = tier_placement_path(&self.paths.catalogs_dir());
        if let Some(p) = try_load_placement(&path, self.store_id)? {
            // Preserve offline flags from roots after load.
            let roots_avail: Vec<_> = [
                TierClass::Hot,
                TierClass::Warm,
                TierClass::Cold,
                TierClass::Archive,
            ]
            .into_iter()
            .map(|t| (t, self.tier_placement.is_tier_available(t)))
            .collect();
            self.tier_placement = p;
            for (t, a) in roots_avail {
                // roots.txt is operator source of truth for online/offline.
                if !a {
                    self.tier_placement.set_tier_available(t, false);
                }
            }
            load_tier_roots_file(&self.paths, &mut self.tier_placement);
        }
        discover_placements(&self.paths, &mut self.tier_placement)?;
        let prior = try_load_segment_catalog(
            &segment_catalog_path(&self.paths.catalogs_dir()),
            self.store_id,
        )?;
        self.segment_catalog = rebuild_segment_catalog(
            &self.paths,
            &self.tier_placement,
            prior.as_ref(),
            self.limits,
        )?;
        let _ = write_segment_catalog(
            &segment_catalog_path(&self.paths.catalogs_dir()),
            self.store_id,
            &self.segment_catalog,
        );
        let _ = write_placement(&path, self.store_id, &self.tier_placement);
        let _ = write_tier_roots_file(&self.paths, &self.tier_placement);
        Ok(())
    }

    fn load_tier_state_readonly(&mut self) -> Result<(), StoreError> {
        load_tier_roots_file(&self.paths, &mut self.tier_placement);
        let path = tier_placement_path(&self.paths.catalogs_dir());
        if let Some(p) = try_load_placement(&path, self.store_id)? {
            self.tier_placement = p;
            load_tier_roots_file(&self.paths, &mut self.tier_placement);
        }
        discover_placements(&self.paths, &mut self.tier_placement)?;
        let prior = try_load_segment_catalog(
            &segment_catalog_path(&self.paths.catalogs_dir()),
            self.store_id,
        )?;
        self.segment_catalog = rebuild_segment_catalog(
            &self.paths,
            &self.tier_placement,
            prior.as_ref(),
            self.limits,
        )?;
        Ok(())
    }

    fn refresh_tier_state(&mut self) -> Result<(), StoreError> {
        discover_placements(&self.paths, &mut self.tier_placement)?;
        self.refresh_segment_catalog()?;
        self.persist_tier_state()?;
        Ok(())
    }

    fn persist_tier_state(&self) -> Result<(), StoreError> {
        let path = tier_placement_path(&self.paths.catalogs_dir());
        write_placement(&path, self.store_id, &self.tier_placement)?;
        write_tier_roots_file(&self.paths, &self.tier_placement)?;
        Ok(())
    }

    fn refresh_segment_catalog(&mut self) -> Result<(), StoreError> {
        let prior = self.segment_catalog.clone();
        self.segment_catalog =
            rebuild_segment_catalog(&self.paths, &self.tier_placement, Some(&prior), self.limits)?;
        let _ = write_segment_catalog(
            &segment_catalog_path(&self.paths.catalogs_dir()),
            self.store_id,
            &self.segment_catalog,
        );
        Ok(())
    }

    /// Incrementally record one newly sealed segment in the hierarchical catalog.
    ///
    /// Scans only `bytes` (already in memory on the seal path). Does **not**
    /// rescan prior sealed segments — that O(total retained data) path is
    /// reserved for [`Self::rebuild_segment_catalog`] / recovery.
    fn note_sealed_segment(
        &mut self,
        segment_id: [u8; 16],
        tier: TierClass,
        bytes: &[u8],
        content_hash: crate::incremental_seal::ContentHashState,
        size: u64,
    ) -> Result<(), StoreError> {
        let summary =
            summarize_segment_bytes(segment_id, tier, bytes, content_hash, size, self.limits);
        self.note_sealed_summary(summary)
    }

    /// Apply a precomputed sealed-segment summary in constant time (zero-scan).
    ///
    /// Memory upsert is immediate; durable catalog write is coalesced via
    /// [`Self::maybe_schedule_derived_catalog_checkpoint`] (or best-effort
    /// [`Self::flush_derived_catalogs_best_effort`] on orderly drain).
    fn note_sealed_summary(&mut self, summary: SegmentSummary) -> Result<(), StoreError> {
        upsert_sealed_summary(&mut self.segment_catalog, summary);
        Ok(())
    }

    fn note_derived_catalog_dirty(&mut self) {
        self.catalog_dirty = true;
        self.catalog_seals_since_checkpoint = self.catalog_seals_since_checkpoint.saturating_add(1);
    }

    /// Coalesce async (or sync) persist of derived tier + segment catalogs.
    ///
    /// Checkpoints may lag or be skipped entirely; open rebuilds from segments.
    fn maybe_schedule_derived_catalog_checkpoint(&mut self, force: bool) {
        if !self.catalog_dirty {
            return;
        }
        let due = force
            || self.catalog_seals_since_checkpoint >= CATALOG_CHECKPOINT_EVERY_SEALS
            || self.last_catalog_checkpoint_at.elapsed() >= CATALOG_CHECKPOINT_MIN_INTERVAL;
        if !due {
            return;
        }
        if self.async_lifecycle_enabled() {
            let job = LifecycleJob::DerivedCatalogCheckpoint {
                store_id: self.store_id,
                paths: self.paths.clone(),
                placement: self.tier_placement.clone(),
                segment_catalog: self.segment_catalog.clone(),
            };
            if let Some(pipe) = self.seal_pipeline.as_ref() {
                if pipe.submit_checkpoint(job).is_ok() {
                    self.catalog_dirty = false;
                    self.catalog_seals_since_checkpoint = 0;
                    self.last_catalog_checkpoint_at = Instant::now();
                }
            }
        } else {
            self.flush_derived_catalogs_best_effort();
        }
    }

    /// Best-effort synchronous persist of derived catalogs (orderly drain/shutdown).
    ///
    /// Never an authority condition — failure is ignored; segments remain SoT.
    fn flush_derived_catalogs_best_effort(&mut self) {
        let _ = self.persist_tier_state();
        let _ = self.persist_segment_catalog();
        self.catalog_dirty = false;
        self.catalog_seals_since_checkpoint = 0;
        self.last_catalog_checkpoint_at = Instant::now();
    }

    /// Persist the in-memory segment catalog (best-effort).
    fn persist_segment_catalog(&self) -> Result<(), StoreError> {
        write_segment_catalog(
            &segment_catalog_path(&self.paths.catalogs_dir()),
            self.store_id,
            &self.segment_catalog,
        )
    }

    fn load_or_rebuild_catalog(&mut self) -> Result<(), StoreError> {
        let paths = all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )?;
        let fp = segment_fingerprint(&paths)?;
        let cat_path = crate::catalog::collections_catalog_path(&self.paths.catalogs_dir());
        if let Some(cat) = try_load_collection_catalog(&cat_path, self.store_id, fp)? {
            self.durable_collections = cat.clone();
            // Visibility starts from durable; memory-mode names attach later.
            self.collection_catalog = cat;
            return Ok(());
        }
        self.recompute_collection_catalogs_from_index();
        self.refresh_collection_catalog()
    }

    fn refresh_collection_catalog(&mut self) -> Result<(), StoreError> {
        let paths = all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )?;
        let fp = segment_fingerprint(&paths)?;
        // DEF-013 / DEF-023: persist only the durable name set (no segment rescan,
        // no O(N) subject walk — names are maintained incrementally on put/delete).
        let cat_path = collections_catalog_path(&self.paths.catalogs_dir());
        write_collection_catalog(&cat_path, self.store_id, fp, &self.durable_collections)?;
        Ok(())
    }

    /// Rebuild in-memory collection catalogs from the primary index (open/rebuild).
    fn recompute_collection_catalogs_from_index(&mut self) {
        self.durable_collections = CollectionCatalog::from_index(&self.durable_index);
        self.collection_catalog = CollectionCatalog::from_index(&self.index);
    }

    fn write_chunked_put(
        &mut self,
        subject: &[u8],
        value: &[u8],
        mode: DurabilityMode,
        identity: Option<MutationIdentity>,
    ) -> Result<WriteReceipt, StoreError> {
        let subject_bytes = subject;
        if subject_bytes.len() > MAX_SUBJECT_LEN {
            return Err(StoreError::SubjectTooLong {
                max: MAX_SUBJECT_LEN,
            });
        }
        let shard = self.subject_shard(subject_bytes);
        self.ensure_active(shard)?;
        if !self.operation_cohort_gathering {
            self.maybe_auto_seal(shard)?;
        }

        let segment_id = self
            .active_ref(shard)
            .map(|w| w.segment_id)
            .expect("active segment");
        let item_id = match self.index.get(subject_bytes) {
            Some(entry) => entry.item_id(),
            None => subject_item_id(subject_bytes),
        };

        let pieces = split_into_pieces(item_id, value, self.chunk_size)?;
        // Pre-mint event ids so we do not hold &mut active across next_event_id.
        let mut chunk_event_ids: Vec<[u8; 16]> = Vec::with_capacity(pieces.len());
        for _ in 0..pieces.len() {
            chunk_event_ids.push(self.next_event_id()?);
        }
        let event_id = self.next_event_id()?;
        let created_ns = now_ns();

        let chunk_envelopes: Result<Vec<_>, _> = pieces
            .iter()
            .map(|_| {
                encode_item_envelope(&ItemEnvelope {
                    store_id: self.store_id,
                    segment_id,
                    item_id,
                    event_kind: EventKind::Put,
                    created_ns,
                    subject: subject_bytes.to_vec(),
                    operation_id: None,
                    operation_content_hash: None,
                })
                .map_err(StoreError::BadEnvelope)
            })
            .collect();
        let chunk_envelopes = chunk_envelopes?;

        // Collect (event_id, offset, piece meta) then record locators after the
        // writer borrow ends (DEF-098 generation-exact preads).
        let mut new_chunk_locs: Vec<(
            [u8; 16],
            u64,
            &residiuum_format::ChunkPiece,
            [u8; 32],
        )> = Vec::with_capacity(pieces.len());
        let mut encoded_frame_len: u64 = 0;
        {
            let writer = self.active_mut(shard).expect("active segment");
            for (piece, (chunk_event_id, envelope)) in pieces
                .iter()
                .zip(chunk_event_ids.iter().zip(chunk_envelopes.iter()))
            {
                let body = encode_piece_body(piece);
                let verified_body_hash = *blake3::hash(&body).as_bytes();
                let header = FrameHeader {
                    wire_major: residiuum_format::WIRE_MAJOR,
                    wire_minor: residiuum_format::WIRE_MINOR,
                    frame_kind: FrameKind::PayloadChunk.as_u8(),
                    flags: FrameFlags::new(FrameFlags::CHUNKED),
                    envelope_len: envelope.len() as u32,
                    body_len: body.len() as u64,
                    logical_len: piece.logical_len,
                    writer_sequence: 0,
                    event_id: *chunk_event_id,
                };
                let offset = writer.segment.append_parts(&FrameParts {
                    header,
                    envelope: envelope.clone(),
                    body,
                })?;
                encoded_frame_len =
                    encoded_frame_len.saturating_add(writer.segment.len().saturating_sub(offset));
                new_chunk_locs.push((*chunk_event_id, offset, piece, verified_body_hash));
            }
        }
        for (chunk_event_id, offset, piece, verified_body_hash) in new_chunk_locs {
            self.chunk_locators
                .entry(chunk_event_id)
                .or_default()
                .push(ChunkFrameLocator {
                    segment_id,
                    frame_offset: offset,
                    item_id,
                    chunk_index: piece.index,
                    chunk_total: piece.total,
                    logical_len: piece.logical_len,
                    verified_body_hash,
                });
        }

        let manifest = manifest_from_pieces(&pieces, &chunk_event_ids, value)?;
        let manifest_body = encode_chunk_manifest(&manifest);
        let item_envelope = encode_item_envelope(&ItemEnvelope {
            store_id: self.store_id,
            segment_id,
            item_id,
            event_kind: EventKind::Put,
            created_ns,
            subject: subject_bytes.to_vec(),
            operation_id: identity.map(|value| value.0),
            operation_content_hash: identity.map(|value| value.1),
        })
        .map_err(StoreError::BadEnvelope)?;

        let sink = self.diagnostic_io;
        let growth = self.segment_growth;
        let gather_cohort = self.operation_cohort_gathering;
        let mut null = self.null_io_file.take();
        let (offset, append_ns, tail) = {
            let writer = self.active_mut(shard).expect("active segment");
            let header = FrameHeader {
                wire_major: residiuum_format::WIRE_MAJOR,
                wire_minor: residiuum_format::WIRE_MINOR,
                frame_kind: FrameKind::ItemEvent.as_u8(),
                flags: FrameFlags::new(FrameFlags::CHUNKED),
                envelope_len: item_envelope.len() as u32,
                body_len: manifest_body.len() as u64,
                logical_len: value.len() as u64,
                writer_sequence: 0,
                event_id,
            };
            let t_append = std::time::Instant::now();
            let offset = writer.segment.append_parts(&FrameParts {
                header,
                envelope: item_envelope,
                body: manifest_body.clone(),
            })?;
            writer.item_events = writer.item_events.saturating_add(1);
            let append_ns = t_append.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            encoded_frame_len =
                encoded_frame_len.saturating_add(writer.segment.len().saturating_sub(offset));
            let tail = match mode {
                DurabilityMode::Memory => TailIoStats::default(),
                DurabilityMode::Buffered | DurabilityMode::Durable if gather_cohort => {
                    TailIoStats::default()
                }
                DurabilityMode::Buffered | DurabilityMode::Durable => {
                    Self::write_segment_tail(writer, mode, sink, null.as_mut(), growth)?
                }
            };
            (offset, append_ns, tail)
        };
        self.null_io_file = null;

        // Boundary probe: actual append + file write/sync (not reconstructed by harness).
        self.boundary_probe.record_append(
            encoded_frame_len,
            value.len() as u64,
            offset,
            mode,
            false,
            true,
            0, // pieces already summed into encoded_frame_len
            append_ns,
            shard as u32,
        );
        self.record_tail_probe(&tail, mode, shard as u32)?;

        // Publish visibility only after authoritative append succeeded (DEF-023).
        // Chunk manifest is small and kept resident by slim_put_body_for_index.
        let t_pub = std::time::Instant::now();
        self.apply_durable_event(
            subject_bytes.to_vec(),
            EventKind::Put,
            manifest_body,
            item_id,
            event_id,
            segment_id,
            0,
            offset,
        );
        let publish_ns = t_pub.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        self.boundary_probe
            .record_publish(offset, mode, shard as u32, publish_ns);
        self.note_collection_for_subject(subject_bytes);

        if mode != DurabilityMode::Memory {
            let _ = self.note_durable_derived();
        }

        let mut receipt = WriteReceipt::base(
            self.store_id,
            segment_id,
            item_id,
            event_id,
            EventKind::Put,
            mode,
            offset,
        );
        receipt.encoded_frame_len = encoded_frame_len;
        Ok(receipt)
    }

    /// Resolve only the chunk frames listed in `manifest` (DEF-098).
    ///
    /// Prefer derived locators + bounded preads; when any expected event is
    /// missing from the locator map, fall back to a generation-filtered segment
    /// scan that never selects chunks solely by shared `item_id`.
    fn resolve_manifest_chunks(
        &self,
        expected_item_id: [u8; 16],
        manifest: &crate::chunk_payload::ChunkManifest,
    ) -> Result<Vec<ResolvedChunk>, StoreError> {
        let expected: HashSet<[u8; 16]> =
            manifest.chunks.iter().map(|s| s.chunk_event_id).collect();
        if expected.is_empty() {
            return Ok(Vec::new());
        }

        let all_located = expected
            .iter()
            .all(|id| self.chunk_locators.contains_key(id));
        if all_located {
            let mut out = Vec::new();
            for eid in &expected {
                let Some(locs) = self.chunk_locators.get(eid) else {
                    continue;
                };
                for loc in locs {
                    if let Ok(resolved) = self.pread_resolved_chunk(eid, loc) {
                        // Retain all verified candidates; reassembler enforces slot meta.
                        out.push(resolved);
                    }
                }
            }
            // If every expected event produced at least one verified frame, done.
            let found: HashSet<_> = out.iter().map(|r| r.frame_event_id).collect();
            if expected.iter().all(|e| found.contains(e)) {
                return Ok(out);
            }
        }

        // Fallback / conflict discovery: scan only frames whose event_id is expected.
        let _ = expected_item_id;
        self.scan_resolved_chunks_for_events(&expected)
    }

    fn pread_resolved_chunk(
        &self,
        expected_event_id: &[u8; 16],
        loc: &ChunkFrameLocator,
    ) -> Result<ResolvedChunk, StoreError> {
        // Active in-memory tail first (write-through may drop older frames).
        if let Some(w) = self.find_active_by_segment(&loc.segment_id) {
            let base = w.segment.base_offset();
            if loc.frame_offset >= base {
                let bytes = w.segment.as_bytes();
                let off = (loc.frame_offset - base) as usize;
                if off < bytes.len() {
                    if let Ok((header, _env, body, _hash, _len)) =
                        residiuum_format::verify_frame_at(&bytes[off..], self.limits)
                    {
                        if header.event_id == *expected_event_id
                            && header.known_kind() == Some(FrameKind::PayloadChunk)
                        {
                            if let Some(piece) = decode_piece_body(body) {
                                if chunk_piece_matches_locator(body, &piece, loc) {
                                    return Ok(resolve_piece(
                                        header.event_id,
                                        piece,
                                        loc.segment_id,
                                        loc.frame_offset,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        let body = self.pread_frame_body_for_locator(&loc.segment_id, loc.frame_offset)?;
        let Some(piece) = decode_piece_body(&body) else {
            return Err(StoreError::CorruptMeta(
                "chunk body decode failed at locator",
            ));
        };
        if !chunk_piece_matches_locator(&body, &piece, loc) {
            return Err(StoreError::ConsistencyViolation(
                "chunk locator metadata mismatch at disk frame offset".into(),
            ));
        }
        Ok(resolve_piece(
            *expected_event_id,
            piece,
            loc.segment_id,
            loc.frame_offset,
        ))
    }

    /// Pread raw verified frame body (any kind) at a locator.
    fn pread_frame_body_for_locator(
        &self,
        segment_id: &[u8; 16],
        frame_offset: u64,
    ) -> Result<Vec<u8>, StoreError> {
        // Reuse item pread helper: it verifies the full frame and returns body bytes.
        // Envelope segment_id match still applies for payload chunks (same envelope shape).
        self.pread_body_for_locator(segment_id, frame_offset)
    }

    fn scan_resolved_chunks_for_events(
        &self,
        expected_event_ids: &HashSet<[u8; 16]>,
    ) -> Result<Vec<ResolvedChunk>, StoreError> {
        let mut out = Vec::new();
        let mut seen: HashSet<([u8; 16], [u8; 32])> = HashSet::new();
        for path in all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )? {
            let bytes = fs::read(&path)?;
            let report = scan_forward(&bytes, self.limits);
            for (offset, frame) in report.verified_frames() {
                if frame.header.known_kind() != Some(FrameKind::PayloadChunk) {
                    continue;
                }
                if !expected_event_ids.contains(&frame.header.event_id) {
                    continue;
                }
                let Some(piece) = decode_piece_body(&frame.body) else {
                    continue;
                };
                let h = *blake3::hash(&piece.body).as_bytes();
                if !seen.insert((frame.header.event_id, h)) {
                    continue;
                }
                let segment_id = decode_item_envelope(&frame.envelope)
                    .map(|e| e.segment_id)
                    .unwrap_or([0u8; 16]);
                out.push(resolve_piece(
                    frame.header.event_id,
                    piece,
                    segment_id,
                    offset,
                ));
            }
        }
        Ok(out)
    }

    /// Rebuild the derived chunk_event_id locator map from all segments (DEF-098).
    fn rebuild_chunk_locators_from_segments(&mut self) -> Result<(), StoreError> {
        let mut map: HashMap<[u8; 16], Vec<ChunkFrameLocator>> = HashMap::new();
        for path in all_segment_paths(
            &self.paths,
            Some(&self.tier_placement),
            self.writer_shards(),
        )? {
            let bytes = fs::read(&path)?;
            let report = scan_forward(&bytes, self.limits);
            for (offset, frame) in report.verified_frames() {
                if frame.header.known_kind() != Some(FrameKind::PayloadChunk) {
                    continue;
                }
                let Some(piece) = decode_piece_body(&frame.body) else {
                    continue;
                };
                let segment_id = decode_item_envelope(&frame.envelope)
                    .map(|e| e.segment_id)
                    .unwrap_or([0u8; 16]);
                let h = *blake3::hash(&piece.body).as_bytes();
                map.entry(frame.header.event_id)
                    .or_default()
                    .push(ChunkFrameLocator {
                        segment_id,
                        frame_offset: offset,
                        item_id: piece.item_id,
                        chunk_index: piece.index,
                        chunk_total: piece.total,
                        logical_len: piece.logical_len,
                        verified_body_hash: h,
                    });
            }
        }
        self.chunk_locators = map;
        Ok(())
    }

    fn write_event(
        &mut self,
        subject: &[u8],
        kind: EventKind,
        body: &[u8],
        mode: DurabilityMode,
        identity: Option<MutationIdentity>,
    ) -> Result<WriteReceipt, StoreError> {
        let subject_bytes = subject;
        if subject_bytes.len() > MAX_SUBJECT_LEN {
            return Err(StoreError::SubjectTooLong {
                max: MAX_SUBJECT_LEN,
            });
        }
        if !self.limits.accepts_lengths(0, body.len() as u64) {
            // envelope is non-zero; re-check after encode
        }
        if body.len() as u64 > self.limits.max_body_len {
            return Err(StoreError::PayloadTooLarge);
        }

        let shard = self.subject_shard(subject_bytes);

        // DEF-013: memory mode is visibility-only — never append frames that a
        // later durable write would flush via write_segment_tail.
        if mode == DurabilityMode::Memory {
            self.ensure_active(shard)?;
            let segment_id = self
                .active_ref(shard)
                .map(|w| w.segment_id)
                .expect("active segment");
            let item_id = match self.index.get(subject_bytes) {
                Some(entry) => entry.item_id(),
                None => subject_item_id(subject_bytes),
            };
            let event_id = self.next_event_id()?;
            // Memory mode: body must stay resident (no durable frame to pread).
            self.index.apply_event(
                subject_bytes.to_vec(),
                kind,
                body.to_vec(),
                item_id,
                event_id,
                segment_id,
                0,
                0,
            );
            // Visibility catalog only (not persisted).
            if let Some(name) = crate::catalog::collection_name_from_subject(subject_bytes) {
                self.collection_catalog.insert(name);
            }
            return Ok(WriteReceipt::base(
                self.store_id,
                segment_id,
                item_id,
                event_id,
                kind,
                DurabilityMode::Memory,
                0,
            ));
        }

        let probe = self.boundary_probe_enabled();
        self.ensure_active(shard)?;
        // Seal/rotate is rare and expensive — timed on segment_rotate, not put_prep.
        if !self.operation_cohort_gathering {
            self.maybe_auto_seal(shard)?;
        }

        // put_prep: per-put hot path only (ids + env subject + wall clock).
        let t_prep = std::time::Instant::now();
        let segment_id = self
            .active_ref(shard)
            .map(|w| w.segment_id)
            .expect("active segment");

        // Diagnostic skip_index: avoid index lookup (item_id = subject hash only).
        let item_id = if self.diagnostic_skip_index {
            subject_item_id(subject_bytes)
        } else {
            match self.index.get(subject_bytes) {
                Some(entry) => entry.item_id(),
                None => subject_item_id(subject_bytes),
            }
        };
        let event_id = self.next_event_id()?;
        let created_ns = now_ns();

        let env = ItemEnvelope {
            store_id: self.store_id,
            segment_id,
            item_id,
            event_kind: kind,
            created_ns,
            subject: subject_bytes.to_vec(),
            operation_id: identity.map(|value| value.0),
            operation_content_hash: identity.map(|value| value.1),
        };
        if probe {
            let prep_ns = t_prep.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            self.boundary_probe
                .record_put_prep(prep_ns, mode, shard as u32);
        }
        let t_enc = std::time::Instant::now();
        let envelope = encode_item_envelope(&env).map_err(StoreError::BadEnvelope)?;
        let encode_ns = t_enc.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        if probe {
            self.boundary_probe.record_encode_envelope(
                envelope.len() as u64,
                encode_ns,
                mode,
                shard as u32,
            );
        }
        if !self
            .limits
            .accepts_lengths(envelope.len() as u32, body.len() as u64)
        {
            return Err(StoreError::PayloadTooLarge);
        }

        let sink = self.diagnostic_io;
        let growth = self.segment_growth;
        let skip_append = self.diagnostic_skip_append_frame;
        let gather_cohort = self.operation_cohort_gathering;
        let mut null = self.null_io_file.take();
        let (offset, encoded_frame_len, append_ns, tail) = {
            let writer = self.active_mut(shard).expect("active segment");
            if skip_append {
                // Short-circuit data cook: no Blake, no segment growth, no tail write.
                let offset = writer.segment.len();
                (offset, 0u64, 0u64, TailIoStats::default())
            } else {
                let t_append = std::time::Instant::now();
                let offset =
                    writer
                        .segment
                        .append(FrameKind::ItemEvent, &envelope, body, event_id)?;
                writer.item_events = writer.item_events.saturating_add(1);
                let append_ns = t_append.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                // Exact encoded frame length at store boundary (not logical+estimate).
                let encoded_frame_len = writer.segment.len().saturating_sub(offset);
                let tail = if gather_cohort {
                    TailIoStats::default()
                } else {
                    Self::write_segment_tail(writer, mode, sink, null.as_mut(), growth)?
                };
                (offset, encoded_frame_len, append_ns, tail)
            }
        };
        self.null_io_file = null;

        self.boundary_probe.record_append(
            encoded_frame_len,
            body.len() as u64,
            offset,
            mode,
            false,
            false,
            0,
            append_ns,
            shard as u32,
        );
        if !skip_append {
            self.record_tail_probe(&tail, mode, shard as u32)?;
        }

        // Publish visibility only after authoritative append succeeded (DEF-023).
        // Durable projection is locator-first (DEF-095): frame_offset + slim body.
        // Ordinary put bodies are not kept resident — pass empty rather than
        // allocating `body.to_vec()` only for `slim_put_body_for_index` to drop.
        // Diagnostic: skip_index isolates data cooking (encode/append/write) from dual-index.
        let t_pub = std::time::Instant::now();
        if !self.diagnostic_skip_index {
            self.apply_durable_event(
                subject_bytes.to_vec(),
                kind,
                Vec::new(),
                item_id,
                event_id,
                segment_id,
                0, // writer_sequence already inside frame; not required for index
                offset,
            );
        }
        let publish_ns = t_pub.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        self.boundary_probe
            .record_publish(offset, mode, shard as u32, publish_ns);
        let t_post = std::time::Instant::now();
        if !self.diagnostic_skip_index {
            self.note_collection_for_subject(subject_bytes);
            let _ = self.note_durable_derived();
        }
        if probe {
            let post_ns = t_post.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            self.boundary_probe
                .record_put_post(post_ns, mode, shard as u32);
        }

        let mut receipt = WriteReceipt::base(
            self.store_id,
            segment_id,
            item_id,
            event_id,
            kind,
            mode,
            offset,
        );
        receipt.encoded_frame_len = encoded_frame_len;
        // Track strongest ack for seal policy (Buffered-only ⇒ no seal fsync).
        if mode != DurabilityMode::Memory {
            if let Some(w) = self.active_mut(shard) {
                w.max_ack_durability = stronger_durability(w.max_ack_durability, mode);
            }
        }
        Ok(receipt)
    }

    /// Flush pending segment bytes to the file; returns measured I/O stats.
    ///
    /// After a successful Buffered/Durable transfer, **write-through** drops the
    /// durable prefix from the in-RAM segment (`discard_through`) so large seal
    /// thresholds do not pin a full segment image in process RSS. Locator offsets
    /// stay absolute; reads of older frames use file pread.
    ///
    /// `sink` may redirect or drop I/O for diagnostic bisection only.
    fn write_segment_tail(
        writer: &mut ActiveWriter,
        mode: DurabilityMode,
        sink: DiagnosticIoSink,
        null_file: Option<&mut File>,
        growth: crate::segment_growth::SegmentGrowthPolicy,
    ) -> Result<TailIoStats, StoreError> {
        crate::failpoint::hit("store.active.write_tail.before")?;
        let base = writer.segment.base_offset();
        if writer.durable_len < base {
            return Err(StoreError::CorruptMeta("durable_len behind base_offset"));
        }
        let retained_len = writer.segment.as_bytes().len();
        let start = (writer.durable_len - base) as usize;
        if start > retained_len {
            return Err(StoreError::CorruptMeta("durable_len past segment"));
        }
        let mut stats = TailIoStats::default();
        if start < retained_len {
            // Copy pending slice length before taking file mut borrow; write from
            // a temporary view of retained bytes (segment is not mutated until after).
            let pending_len = retained_len - start;
            stats.write_requested = pending_len as u64;

            match sink {
                DiagnosticIoSink::Discard => {
                    // Detach OS/media entirely: advance durable_len as if write succeeded.
                    let t0 = std::time::Instant::now();
                    writer.durable_len = base.saturating_add(retained_len as u64);
                    stats.write_duration_ns =
                        t0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                    stats.write_completed = pending_len as u64;
                    stats.write_outcome = crate::boundary_probe::BoundaryOutcome::Ok;
                }
                DiagnosticIoSink::DevNull => {
                    // Syscall/VFS path without durable media (bisection vs Real).
                    // Caller must pass a reused `/dev/null` handle (open once).
                    let null = null_file.ok_or_else(|| {
                        StoreError::CorruptMeta("DevNull sink without null_io_file")
                    })?;
                    let t0 = std::time::Instant::now();
                    {
                        let pending = &writer.segment.as_bytes()[start..];
                        null.write_all(pending)?;
                    }
                    stats.write_duration_ns =
                        t0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                    stats.write_completed = pending_len as u64;
                    stats.write_outcome = crate::boundary_probe::BoundaryOutcome::Ok;
                    writer.durable_len = base.saturating_add(retained_len as u64);
                }
                DiagnosticIoSink::Real
                | DiagnosticIoSink::RealPrealloc
                | DiagnosticIoSink::RealPreallocFill
                | DiagnosticIoSink::RealPreallocFcntl
                | DiagnosticIoSink::RealPreallocZero
                | DiagnosticIoSink::RealPreallocWatermark => {
                    if sink == DiagnosticIoSink::RealPreallocWatermark {
                        // Keep ≥64 MiB of zeroed runway ahead of the write head.
                        const CAP: u64 = 512 * 1024 * 1024;
                        const CHUNK: u64 = 64 * 1024 * 1024;
                        let need = writer
                            .durable_len
                            .saturating_add(pending_len as u64)
                            .saturating_add(CHUNK)
                            .min(CAP);
                        Self::diag_ensure_zero_watermark(writer, need, CAP)?;
                    } else if sink == DiagnosticIoSink::Real {
                        if let crate::segment_growth::SegmentGrowthPolicy::Watermark {
                            capacity_bytes,
                            ..
                        } = growth
                        {
                            // Product watermark: first-touch is background-only.
                            // Puts consume ready runway; fail closed if empty.
                            let need = writer.durable_len.saturating_add(pending_len as u64);
                            if need > capacity_bytes {
                                return Err(StoreError::CorruptMeta(
                                    "segment watermark capacity exhausted (active past reserved len)",
                                ));
                            }
                            let ready = writer
                                .runway
                                .as_ref()
                                .map(|r| {
                                    r.shared().write_head.store(
                                        writer.durable_len,
                                        std::sync::atomic::Ordering::Release,
                                    );
                                    r.shared()
                                        .zeroed_thru
                                        .load(std::sync::atomic::Ordering::Acquire)
                                })
                                .unwrap_or(writer.zeroed_thru);
                            if ready < need {
                                return Err(StoreError::CorruptMeta(
                                    "segment watermark runway exhausted (background preparer behind)",
                                ));
                            }
                            writer.zeroed_thru = ready;
                        }
                    }
                    // DEF-022: optional short-write injection mid-append.
                    if crate::failpoint::consume_short_write("store.active.write_tail.short_write")
                    {
                        let n = crate::failpoint::short_write_len(pending_len);
                        let t0 = std::time::Instant::now();
                        if n > 0 {
                            let pending = &writer.segment.as_bytes()[start..start + n];
                            let write_offset = writer.durable_len;
                            crate::positioned_io::write_all_at(
                                &mut writer.file,
                                write_offset,
                                pending,
                            )?;
                            // Do not advance durable_len past the short write so a
                            // later retry could rewrite; crash/drop leaves torn bytes.
                            writer.durable_len += n as u64;
                        }
                        stats.write_completed = n as u64;
                        stats.write_duration_ns =
                            t0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                        stats.write_outcome = crate::boundary_probe::BoundaryOutcome::ShortWrite;
                        // Return Ok so callers can record probe stats, then fail closed.
                        // No write-through discard on short write (prefix may be torn).
                        stats.fail_as_short_write = true;
                        return Ok(stats);
                    }
                    let t0 = std::time::Instant::now();
                    let dual_err = {
                        let pending = &writer.segment.as_bytes()[start..];
                        let write_offset = writer.durable_len;
                        crate::positioned_io::write_all_at(
                            &mut writer.file,
                            write_offset,
                            pending,
                        )?;
                        // Paired Shadow staging write (no sync; independent alloc).
                        if let Some(dual) = writer.shadow_dual.as_mut() {
                            if let Err(e) = dual.append_image_chunk(pending) {
                                // Auth bytes landed — poison staging so seal
                                // cannot claim P★ with a divergent Shadow image.
                                dual.poison();
                                Some(e)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    };
                    stats.write_duration_ns =
                        t0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                    stats.write_completed = pending_len as u64;
                    stats.write_outcome = crate::boundary_probe::BoundaryOutcome::Ok;
                    writer.durable_len = base.saturating_add(retained_len as u64);
                    debug_assert_eq!(writer.durable_len, writer.segment.len());
                    if let Some(runway) = writer.runway.as_ref() {
                        runway
                            .shared()
                            .write_head
                            .store(writer.durable_len, std::sync::atomic::Ordering::Release);
                    }
                    if let Some(e) = dual_err {
                        return Err(e);
                    }
                }
                DiagnosticIoSink::SeekOnly => {
                    // Seek tax only — no bytes transferred.
                    let t0 = std::time::Instant::now();
                    writer.file.seek(SeekFrom::Start(writer.durable_len))?;
                    writer.durable_len = base.saturating_add(retained_len as u64);
                    stats.write_duration_ns =
                        t0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                    stats.write_completed = pending_len as u64;
                    stats.write_outcome = crate::boundary_probe::BoundaryOutcome::Ok;
                }
                DiagnosticIoSink::RealNoSeek => {
                    // Same as Real but skip seek (file cursor should already be at end).
                    let t0 = std::time::Instant::now();
                    {
                        let pending = &writer.segment.as_bytes()[start..];
                        writer.file.write_all(pending)?;
                        if let Some(dual) = writer.shadow_dual.as_mut() {
                            dual.append_image_chunk(pending)?;
                        }
                    }
                    stats.write_duration_ns =
                        t0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                    stats.write_completed = pending_len as u64;
                    stats.write_outcome = crate::boundary_probe::BoundaryOutcome::Ok;
                    writer.durable_len = base.saturating_add(retained_len as u64);
                }
                DiagnosticIoSink::RealOverwrite => {
                    // Thr bisect only: smash bytes at offset 0 so the file does not grow.
                    let t0 = std::time::Instant::now();
                    writer.file.seek(SeekFrom::Start(0))?;
                    {
                        let pending = &writer.segment.as_bytes()[start..];
                        writer.file.write_all(pending)?;
                    }
                    stats.write_duration_ns =
                        t0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                    stats.write_completed = pending_len as u64;
                    stats.write_outcome = crate::boundary_probe::BoundaryOutcome::Ok;
                    writer.durable_len = base.saturating_add(retained_len as u64);
                }
                DiagnosticIoSink::Coalesce100k => {
                    // Spike: coalesce real-file write_all into ≥100 KiB or 250 ms.
                    const CAP: usize = 100 * 1024;
                    const MAX_DELAY: std::time::Duration = std::time::Duration::from_millis(250);
                    let pending = writer.segment.as_bytes()[start..].to_vec();
                    if writer.coalesce_buf.is_empty() {
                        writer.coalesce_off = writer.durable_len;
                        writer.coalesce_since = Some(std::time::Instant::now());
                    }
                    writer.coalesce_buf.extend_from_slice(&pending);
                    writer.durable_len = base.saturating_add(retained_len as u64);
                    stats.write_completed = pending_len as u64;
                    stats.write_outcome = crate::boundary_probe::BoundaryOutcome::Ok;
                    let age = writer
                        .coalesce_since
                        .map(|t| t.elapsed())
                        .unwrap_or_default();
                    if writer.coalesce_buf.len() >= CAP || age >= MAX_DELAY {
                        let t0 = std::time::Instant::now();
                        Self::flush_writer_coalesce(writer)?;
                        stats.write_duration_ns =
                            t0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                    }
                }
            }
        }
        crate::failpoint::hit("store.active.write_tail.after_write")?;
        // Durable sync only applies to the real active file (not Discard/DevNull).
        if mode == DurabilityMode::Durable
            && matches!(
                sink,
                DiagnosticIoSink::Real
                    | DiagnosticIoSink::Coalesce100k
                    | DiagnosticIoSink::RealNoSeek
                    | DiagnosticIoSink::RealOverwrite
                    | DiagnosticIoSink::RealPrealloc
                    | DiagnosticIoSink::RealPreallocFill
                    | DiagnosticIoSink::RealPreallocFcntl
                    | DiagnosticIoSink::RealPreallocZero
                    | DiagnosticIoSink::RealPreallocWatermark
            )
        {
            if sink == DiagnosticIoSink::Coalesce100k {
                Self::flush_writer_coalesce(writer)?;
            }
            let t0 = std::time::Instant::now();
            writer.file.sync_all()?;
            stats.sync_duration_ns = t0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            stats.synced = true;
            stats.sync_outcome = crate::boundary_probe::BoundaryOutcome::Ok;
            crate::failpoint::hit("store.active.write_tail.after_sync")?;
        }
        // Write-through discard: free RAM for durable bytes (locator offsets stay
        // absolute; older frames use file pread).
        if stats.write_outcome == crate::boundary_probe::BoundaryOutcome::Ok
            || stats.write_requested == 0
        {
            writer.segment.discard_through(writer.durable_len);
        }
        Ok(stats)
    }

    /// Force any Coalesce100k pending bytes to the real file (seal / rotate / Durable).
    fn flush_writer_coalesce(writer: &mut ActiveWriter) -> Result<(), StoreError> {
        if writer.coalesce_buf.is_empty() {
            return Ok(());
        }
        crate::positioned_io::write_all_at(
            &mut writer.file,
            writer.coalesce_off,
            &writer.coalesce_buf,
        )?;
        if let Some(dual) = writer.shadow_dual.as_mut() {
            dual.append_image_chunk(&writer.coalesce_buf)?;
        }
        writer.coalesce_buf.clear();
        writer.coalesce_since = None;
        Ok(())
    }

    fn flush_active_file(
        &mut self,
        writer: &mut ActiveWriter,
        mode: DurabilityMode,
        shard: u32,
    ) -> Result<(), StoreError> {
        let sink = self.diagnostic_io;
        let growth = self.segment_growth;
        let mut null = self.null_io_file.take();
        let stats = Self::write_segment_tail(writer, mode, sink, null.as_mut(), growth)?;
        self.null_io_file = null;
        self.record_tail_probe(&stats, mode, shard)?;
        Ok(())
    }

    fn record_tail_probe(
        &mut self,
        stats: &TailIoStats,
        mode: DurabilityMode,
        shard: u32,
    ) -> Result<(), StoreError> {
        if stats.write_requested > 0 || stats.write_completed > 0 {
            self.boundary_probe.record_file_write(
                stats.write_requested,
                stats.write_completed,
                stats.write_duration_ns,
                stats.write_outcome,
                mode,
                shard,
            );
        }
        if stats.synced {
            self.boundary_probe.record_file_sync(
                stats.sync_duration_ns,
                stats.sync_outcome,
                mode,
                shard,
            );
        }
        if stats.fail_as_short_write {
            return Err(StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failpoint short write: store.active.write_tail.short_write",
            )));
        }
        Ok(())
    }

    fn persist_all_actives(&mut self, mode: DurabilityMode) -> Result<(), StoreError> {
        let n = self.writer_shards();
        for shard in 0..n {
            self.persist_active_shard(shard, mode)?;
        }
        Ok(())
    }

    fn persist_active_shard(
        &mut self,
        shard: usize,
        mode: DurabilityMode,
    ) -> Result<(), StoreError> {
        if self.active_mut(shard).is_some() {
            // Split borrows: take stats from writer, then probe on self.
            let sink = self.diagnostic_io;
            let growth = self.segment_growth;
            let mut null = self.null_io_file.take();
            let stats = {
                let writer = self.active_mut(shard).expect("active");
                Self::write_segment_tail(writer, mode, sink, null.as_mut(), growth)?
            };
            self.null_io_file = null;
            self.record_tail_probe(&stats, mode, shard as u32)?;
            if mode == DurabilityMode::Durable {
                crate::failpoint::hit("store.active.dir_sync")?;
                let t0 = std::time::Instant::now();
                sync_dir(&self.paths.active_shard_dir(shard, self.writer_shards()))?;
                let dir_ns = t0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                self.boundary_probe
                    .record_directory_sync(dir_ns, shard as u32);
            }
        }
        Ok(())
    }

    fn start_all_active_segments(&mut self) -> Result<(), StoreError> {
        let n = self.writer_shards();
        for shard in 0..n {
            self.start_active_segment(shard)?;
        }
        Ok(())
    }

    fn start_active_segment(&mut self, shard: usize) -> Result<(), StoreError> {
        let n = self.writer_shards();
        let segment_id = self.next_segment_id()?;
        let ids = SegmentId::new(self.store_id, segment_id);
        let segment = ActiveSegment::create(ids, self.limits, now_ns())?;
        let dir = self.paths.active_shard_dir(shard, n);
        fs::create_dir_all(&dir)?;
        let path = self.paths.active_segment_for_shard(shard, n);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        self.maybe_diagnostic_prealloc(&mut file)?;
        self.maybe_product_growth_prealloc(&mut file)?;
        let initial = segment.as_bytes().to_vec();
        file.write_all(&initial)?;
        let durable_len = segment.len();
        // New active creation: Durable open paths still fsync the descriptor;
        // Buffered-only workloads skip fsync (CSQ-ACK-004).
        // Caller uses Durable when opening/resuming for crash-safe catalog; seal
        // path that only ever saw Buffered acks may start the next active without
        // fsync via `start_active_segment_with_mode`.
        file.sync_all()?;
        let shadow_dual = self.open_shadow_dual(segment_id, &initial)?;
        self.set_active(
            shard,
            Some(ActiveWriter {
                segment_id,
                segment,
                file,
                durable_len,
                max_ack_durability: DurabilityMode::Memory,
                coalesce_buf: Vec::new(),
                coalesce_off: 0,
                coalesce_since: None,
                zeroed_thru: self.product_initial_zeroed_thru(),
                runway: None,
                item_events: 0,
                shadow_dual,
            }),
        );
        crate::failpoint::hit("segalloc.after_active_media")?;
        self.maybe_attach_runway_preparer(shard)?;
        Ok(())
    }

    /// Start a new active segment, fsyncing the descriptor only when `mode` is Durable.
    fn start_active_segment_with_mode(
        &mut self,
        shard: usize,
        mode: DurabilityMode,
    ) -> Result<(), StoreError> {
        let n = self.writer_shards();
        let segment_id = self.next_segment_id()?;
        let ids = SegmentId::new(self.store_id, segment_id);
        let segment = ActiveSegment::create(ids, self.limits, now_ns())?;
        let dir = self.paths.active_shard_dir(shard, n);
        fs::create_dir_all(&dir)?;
        let path = self.paths.active_segment_for_shard(shard, n);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        self.maybe_diagnostic_prealloc(&mut file)?;
        self.maybe_product_growth_prealloc(&mut file)?;
        let initial = segment.as_bytes().to_vec();
        file.write_all(&initial)?;
        let durable_len = segment.len();
        if mode == DurabilityMode::Durable {
            file.sync_all()?;
        }
        let shadow_dual = self.open_shadow_dual(segment_id, &initial)?;
        crate::failpoint::hit("segalloc.after_active_media")?;
        self.set_active(
            shard,
            Some(ActiveWriter {
                segment_id,
                segment,
                file,
                durable_len,
                max_ack_durability: DurabilityMode::Memory,
                coalesce_buf: Vec::new(),
                coalesce_off: 0,
                coalesce_since: None,
                zeroed_thru: self.product_initial_zeroed_thru(),
                runway: None,
                item_events: 0,
                shadow_dual,
            }),
        );
        self.maybe_attach_runway_preparer(shard)?;
        Ok(())
    }

    /// Open experimental dual-stream Shadow staging seeded with `initial_image`.
    fn open_shadow_dual(
        &self,
        segment_id: [u8; 16],
        initial_image: &[u8],
    ) -> Result<Option<crate::recovery_shadow::ShadowDualStream>, StoreError> {
        if !self.shadow_dual_stream {
            return Ok(None);
        }
        let mut dual = crate::recovery_shadow::ShadowDualStream::begin(
            &self.paths,
            self.store_id,
            segment_id,
        )?;
        dual.append_image_chunk(initial_image)?;
        Ok(Some(dual))
    }

    /// Diagnostic: pre-size (and optionally touch) the active file before first write.
    fn maybe_diagnostic_prealloc(&self, file: &mut File) -> Result<(), StoreError> {
        const BYTES: u64 = 512 * 1024 * 1024;
        match self.diagnostic_io {
            DiagnosticIoSink::RealPrealloc => {
                file.set_len(BYTES)?;
                file.seek(SeekFrom::Start(0))?;
            }
            DiagnosticIoSink::RealPreallocFill => {
                file.set_len(BYTES)?;
                // Force physical pages (APFS set_len is often sparse).
                let mut off = 0u64;
                let one = [0u8; 1];
                while off < BYTES {
                    file.seek(SeekFrom::Start(off))?;
                    file.write_all(&one)?;
                    off = off.saturating_add(1024 * 1024);
                }
                file.seek(SeekFrom::Start(0))?;
            }
            DiagnosticIoSink::RealPreallocFcntl => {
                Self::diag_os_preallocate(file, BYTES)?;
                file.set_len(BYTES)?;
                file.seek(SeekFrom::Start(0))?;
            }
            DiagnosticIoSink::RealPreallocZero => {
                Self::diag_os_preallocate(file, BYTES)?;
                file.set_len(BYTES)?;
                Self::diag_bulk_zero_range(file, 0, BYTES)?;
                file.seek(SeekFrom::Start(0))?;
            }
            DiagnosticIoSink::RealPreallocWatermark => {
                const CHUNK: u64 = 64 * 1024 * 1024;
                Self::diag_os_preallocate(file, BYTES)?;
                file.set_len(BYTES)?;
                Self::diag_bulk_zero_range(file, 0, CHUNK)?;
                file.seek(SeekFrom::Start(0))?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Product watermark create-time setup (only when diag sink is Real).
    fn maybe_product_growth_prealloc(&self, file: &mut File) -> Result<(), StoreError> {
        if self.diagnostic_io != DiagnosticIoSink::Real {
            return Ok(());
        }
        crate::segment_growth::prepare_active_file(file, self.segment_growth)
    }

    fn product_initial_zeroed_thru(&self) -> u64 {
        match self.diagnostic_io {
            DiagnosticIoSink::RealPreallocWatermark => 64 * 1024 * 1024,
            DiagnosticIoSink::Real => self.segment_growth.initial_zeroed_thru(),
            _ => 0,
        }
    }

    /// Apply product watermark to already-open actives (post-create policy set).
    fn apply_product_growth_to_existing_actives(&mut self) -> Result<(), StoreError> {
        let policy = self.segment_growth;
        let crate::segment_growth::SegmentGrowthPolicy::Watermark {
            capacity_bytes,
            chunk_bytes,
        } = policy
        else {
            return Ok(());
        };
        let n = self.writer_shards();
        for shard in 0..n {
            let Some(writer) = self.active_mut(shard) else {
                continue;
            };
            // Stop any prior preparer before resizing / re-zero bootstrap.
            writer.runway = None;
            crate::segment_growth::os_preallocate(&writer.file, capacity_bytes)?;
            let cur = writer.file.metadata()?.len();
            if cur < capacity_bytes {
                writer.file.set_len(capacity_bytes)?;
            }
            // Never overwrite live durable frames — only extend zero runway ahead.
            if writer.zeroed_thru < writer.durable_len {
                writer.zeroed_thru = writer.durable_len;
            }
            // Same-fd full-capacity zero before puts (principal prealloc model).
            crate::segment_growth::ensure_zero_watermark(
                &mut writer.file,
                &mut writer.zeroed_thru,
                capacity_bytes,
                capacity_bytes,
                chunk_bytes,
            )?;
            writer.file.seek(SeekFrom::Start(writer.durable_len))?;
        }
        Ok(())
    }

    fn stop_all_runway_preparers(&mut self) {
        let n = self.writer_shards();
        for shard in 0..n {
            if let Some(writer) = self.active_mut(shard) {
                writer.runway = None;
            }
        }
    }

    fn attach_runway_preparers(&mut self) -> Result<(), StoreError> {
        let n = self.writer_shards();
        for shard in 0..n {
            self.maybe_attach_runway_preparer(shard)?;
        }
        Ok(())
    }

    fn maybe_attach_runway_preparer(&mut self, shard: usize) -> Result<(), StoreError> {
        if self.diagnostic_io != DiagnosticIoSink::Real {
            return Ok(());
        }
        let crate::segment_growth::SegmentGrowthPolicy::Watermark {
            capacity_bytes,
            chunk_bytes,
        } = self.segment_growth
        else {
            return Ok(());
        };
        let n = self.writer_shards();
        let path = self.paths.active_segment_for_shard(shard, n);
        let Some(writer) = self.active_mut(shard) else {
            return Ok(());
        };
        if writer.runway.is_some() {
            return Ok(());
        }
        let shared =
            crate::runway_preparer::RunwayShared::new(writer.zeroed_thru, writer.durable_len);
        writer.runway = Some(crate::runway_preparer::RunwayPreparer::start(
            path,
            capacity_bytes,
            chunk_bytes,
            shared,
        )?);
        Ok(())
    }

    /// Extend bulk-zero through `need_thru` in 64 MiB steps (watermark spike).
    fn diag_ensure_zero_watermark(
        writer: &mut ActiveWriter,
        need_thru: u64,
        file_cap: u64,
    ) -> Result<(), StoreError> {
        const CHUNK: u64 = 64 * 1024 * 1024;
        while writer.zeroed_thru < need_thru && writer.zeroed_thru < file_cap {
            let end = writer.zeroed_thru.saturating_add(CHUNK).min(file_cap);
            Self::diag_bulk_zero_range(&mut writer.file, writer.zeroed_thru, end)?;
            writer.zeroed_thru = end;
        }
        Ok(())
    }

    /// Write zeros across `[start, end)` in 1 MiB chunks (diagnostic first-touch).
    fn diag_bulk_zero_range(file: &mut File, start: u64, end: u64) -> Result<(), StoreError> {
        if end <= start {
            return Ok(());
        }
        let chunk = vec![0u8; 1024 * 1024];
        let mut off = start;
        while off < end {
            let n = ((end - off) as usize).min(chunk.len());
            file.seek(SeekFrom::Start(off))?;
            file.write_all(&chunk[..n])?;
            off = off.saturating_add(n as u64);
        }
        Ok(())
    }

    /// Platform physical block reserve (diagnostic). macOS: `F_PREALLOCATE`;
    /// Linux: `posix_fallocate`. Other targets: error.
    fn diag_os_preallocate(file: &File, bytes: u64) -> Result<(), StoreError> {
        #[cfg(target_os = "macos")]
        {
            use std::os::unix::io::AsRawFd;
            #[repr(C)]
            struct FStore {
                fst_flags: u32,
                fst_posmode: i32,
                fst_offset: i64,
                fst_length: i64,
                fst_bytesalloc: i64,
            }
            // From sys/fcntl.h — allocate from EOF / file start region.
            const F_PREALLOCATE: i32 = 42;
            const F_ALLOCATECONTIG: u32 = 0x0000_0002;
            const F_ALLOCATEALL: u32 = 0x0000_0004;
            const F_PEOFPOSMODE: i32 = 3;
            extern "C" {
                fn fcntl(fd: i32, cmd: i32, ...) -> i32;
            }
            let fd = file.as_raw_fd();
            let mut store = FStore {
                fst_flags: F_ALLOCATECONTIG,
                fst_posmode: F_PEOFPOSMODE,
                fst_offset: 0,
                fst_length: bytes as i64,
                fst_bytesalloc: 0,
            };
            let rc = unsafe { fcntl(fd, F_PREALLOCATE, &mut store as *mut FStore) };
            if rc != 0 {
                // Contiguous failed — fall back to any physical alloc.
                store.fst_flags = F_ALLOCATEALL;
                store.fst_bytesalloc = 0;
                let rc2 = unsafe { fcntl(fd, F_PREALLOCATE, &mut store as *mut FStore) };
                if rc2 != 0 {
                    return Err(StoreError::Io(std::io::Error::last_os_error()));
                }
            }
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            extern "C" {
                fn posix_fallocate(fd: i32, offset: i64, len: i64) -> i32;
            }
            let rc = unsafe { posix_fallocate(file.as_raw_fd(), 0, bytes as i64) };
            if rc != 0 {
                return Err(StoreError::Io(std::io::Error::from_raw_os_error(rc)));
            }
            return Ok(());
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            let _ = (file, bytes);
            return Err(StoreError::CorruptMeta(
                "RealPreallocFcntl unsupported on this OS",
            ));
        }
    }

    fn resume_or_start_all_actives(&mut self) -> Result<(), StoreError> {
        let n = self.writer_shards();
        for shard in 0..n {
            self.resume_or_start_active_shard(shard)?;
        }
        Ok(())
    }

    fn resume_or_start_active_shard(&mut self, shard: usize) -> Result<(), StoreError> {
        let n = self.writer_shards();
        let path = self.paths.active_segment_for_shard(shard, n);
        if !path.exists() {
            self.start_active_segment(shard)?;
            self.persist_active_shard(shard, DurabilityMode::Durable)?;
            return Ok(());
        }

        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        // Truncate incomplete tail: keep only verified contiguous prefix from offset 0.
        let (kept, segment_id) = match recover_active_bytes(
            &bytes,
            self.store_id,
            self.limits,
            self.accept_foreign_store_id,
        ) {
            Ok(v) => v,
            Err(StoreError::CorruptMeta(_)) if bytes.is_empty() => {
                drop(file);
                let _ = fs::remove_file(&path);
                self.start_active_segment(shard)?;
                self.persist_active_shard(shard, DurabilityMode::Durable)?;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        // If this id is already sealed/pending elsewhere, refuse open — do not
        // delete the active or mint around the collision (P0).
        if self.paths.sealed_segment(&segment_id).is_file()
            || self.paths.pending_segment(&segment_id).is_file()
        {
            let mut paths = vec![path.clone()];
            let sealed = self.paths.sealed_segment(&segment_id);
            if sealed.is_file() {
                paths.push(sealed);
            }
            let pending = self.paths.pending_segment(&segment_id);
            if pending.is_file() {
                paths.push(pending);
            }
            return Err(StoreError::SegmentIdCollision { segment_id, paths });
        }
        if kept.len() != bytes.len() {
            file.set_len(kept.len() as u64)?;
            file.seek(SeekFrom::Start(kept.len() as u64))?;
            file.sync_all()?;
        }

        // Active path is `active.residiuum` (no seq in the name). Index open
        // therefore cannot see this id via `max_segment_seq_from_paths`; bump
        // so the next mint cannot collide with the resumed segment.
        crate::segment_allocator::note_in_memory_high_water(
            &mut self.segment_seq,
            segment_seq_from_id(&segment_id),
        );

        // Rebuild ActiveSegment by re-appending recovered item events.
        let rebuilt = rebuild_active_from_bytes(&kept, self.store_id, segment_id, self.limits)?;
        let durable_len = rebuilt.len();
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(rebuilt.as_bytes())?;
        file.sync_all()?;

        let mut item_events = 0u64;
        {
            let report = scan_forward(rebuilt.as_bytes(), self.limits);
            for (_off, frame) in report.verified_frames() {
                if frame.header.known_kind() == Some(FrameKind::ItemEvent) {
                    item_events = item_events.saturating_add(1);
                }
            }
        }
        self.set_active(
            shard,
            Some(ActiveWriter {
                segment_id,
                segment: rebuilt,
                file,
                durable_len,
                // Resumed bytes may include prior Durable frames; fail closed.
                max_ack_durability: DurabilityMode::Durable,
                coalesce_buf: Vec::new(),
                coalesce_off: 0,
                coalesce_since: None,
                zeroed_thru: 0,
                runway: None,
                item_events,
                // Dual-stream is re-attached by `reload_recovery_mode` /
                // `attach_shadow_dual_to_actives` when CompactShadow is armed.
                shadow_dual: None,
            }),
        );
        Ok(())
    }

    fn take_active(&mut self, shard: usize) -> Option<ActiveWriter> {
        self.actives.get_mut(shard).and_then(|s| s.take())
    }

    fn set_active(&mut self, shard: usize, writer: Option<ActiveWriter>) {
        if shard < self.actives.len() {
            self.actives[shard] = writer;
        }
    }

    fn active_ref(&self, shard: usize) -> Option<&ActiveWriter> {
        self.actives.get(shard).and_then(|s| s.as_ref())
    }

    fn active_mut(&mut self, shard: usize) -> Option<&mut ActiveWriter> {
        self.actives.get_mut(shard).and_then(|s| s.as_mut())
    }

    /// Find the in-memory active writer that owns `segment_id`, if any.
    fn find_active_by_segment(&self, segment_id: &[u8; 16]) -> Option<&ActiveWriter> {
        self.actives
            .iter()
            .filter_map(|s| s.as_ref())
            .find(|w| w.segment_id == *segment_id)
    }

    fn ensure_active(&mut self, shard: usize) -> Result<(), StoreError> {
        if self.active_ref(shard).is_none() {
            self.start_active_segment(shard)?;
        }
        Ok(())
    }

    fn next_segment_id(&mut self) -> Result<[u8; 16], StoreError> {
        // Durable reservation before media: never remint a published/reserved seq.
        crate::segment_allocator::reserve_next_segment_id(
            &self.paths,
            self.store_id,
            &mut self.segment_seq,
        )
    }

    /// Pure CSPRNG event identity (not sortable; order uses writer_sequence).
    fn next_event_id(&mut self) -> Result<[u8; 16], StoreError> {
        random_id()
    }
}

/// Stable home shard for a subject under `writer_shards` (DEF-096 Axis B).
///
/// Uses the first 8 LE bytes of [`subject_item_id`] (BLAKE3-derived) so routing
/// matches item lineage identity and is independent of UTF-8 string form.
pub fn subject_writer_shard(subject: &[u8], writer_shards: usize) -> usize {
    let n = writer_shards.max(1);
    if n == 1 {
        return 0;
    }
    let id = subject_item_id(subject);
    let h = u64::from_le_bytes(id[0..8].try_into().expect("8 bytes"));
    (h % n as u64) as usize
}

fn write_writer_shards_file(paths: &StorePaths, writer_shards: usize) -> Result<(), StoreError> {
    let n = writer_shards.clamp(1, MAX_WRITER_SHARDS);
    let body = format!("{n}\n");
    crate::atomic_file::write_atomic(&paths.writer_shards_file(), body.as_bytes())?;
    Ok(())
}

fn read_writer_shards(paths: &StorePaths) -> Result<usize, StoreError> {
    let path = paths.writer_shards_file();
    if !path.is_file() {
        return Ok(DEFAULT_WRITER_SHARDS);
    }
    let text = fs::read_to_string(&path).unwrap_or_default();
    let n = text
        .trim()
        .parse::<usize>()
        .unwrap_or(DEFAULT_WRITER_SHARDS)
        .clamp(1, MAX_WRITER_SHARDS);
    Ok(n)
}

/// One verified item event discovered on disk (shared with history module).
#[derive(Debug, Clone)]
pub(crate) struct DiskEventPub {
    pub(crate) file: PathBuf,
    pub(crate) offset: u64,
    pub(crate) writer_sequence: u64,
    pub(crate) subject: Vec<u8>,
    pub(crate) kind: EventKind,
    pub(crate) body: Vec<u8>,
    pub(crate) item_id: [u8; 16],
    pub(crate) event_id: [u8; 16],
    pub(crate) segment_id: [u8; 16],
}

/// Compare recovery order for item events (segment mint order, then sequence).
pub(crate) fn cmp_disk_events_pub(a: &DiskEventPub, b: &DiskEventPub) -> Ordering {
    segment_seq_key(&a.segment_id)
        .cmp(&segment_seq_key(&b.segment_id))
        .then(a.writer_sequence.cmp(&b.writer_sequence))
        .then(a.offset.cmp(&b.offset))
        .then(a.file.cmp(&b.file))
        .then(a.event_id.cmp(&b.event_id))
}

/// Collect verified item events from all segment files; also reports holes.
pub(crate) fn collect_item_events_for_history(
    paths: &StorePaths,
    limits: SafetyLimits,
    placement: Option<&TierPlacement>,
) -> Result<(Vec<DiskEventPub>, bool), StoreError> {
    // History scans discover actives from on-disk layout (legacy + shard dirs).
    let writer_shards = read_writer_shards(paths).unwrap_or(1);
    collect_item_events_tiered(paths, limits, placement, writer_shards)
}

fn collect_item_events_tiered(
    paths: &StorePaths,
    limits: SafetyLimits,
    placement: Option<&TierPlacement>,
    writer_shards: usize,
) -> Result<(Vec<DiskEventPub>, bool), StoreError> {
    let mut events = Vec::new();
    let mut has_holes = false;
    for path in all_segment_paths(paths, placement, writer_shards)? {
        let bytes = fs::read(&path)?;
        let report = scan_forward(&bytes, limits);
        if report.holes().next().is_some() {
            has_holes = true;
        }
        for (offset, frame) in report.verified_frames() {
            if frame.header.known_kind() != Some(FrameKind::ItemEvent) {
                continue;
            }
            let Some(env) = decode_item_envelope(&frame.envelope) else {
                continue;
            };
            events.push(DiskEventPub {
                file: path.clone(),
                offset,
                writer_sequence: frame.header.writer_sequence,
                subject: env.subject,
                kind: env.event_kind,
                body: frame.body.clone(),
                item_id: env.item_id,
                event_id: frame.header.event_id,
                segment_id: env.segment_id,
            });
        }
    }
    Ok((events, has_holes))
}

type DiskEvent = DiskEventPub;

/// Compare recovery order for item events (segment mint order, then sequence).
fn cmp_disk_events(a: &DiskEvent, b: &DiskEvent) -> Ordering {
    cmp_disk_events_pub(a, b)
}

/// Next recovery generation for a compact job (max existing + 1).
fn next_compact_recovery_generation(paths: &StorePaths) -> Result<u64, StoreError> {
    let jobs = crate::compact::list_compact_jobs(paths)?;
    let max = jobs
        .iter()
        .map(|j| j.recovery_generation)
        .max()
        .unwrap_or(0);
    Ok(max.saturating_add(1))
}

/// First 8 LE bytes of segment_id are the mint counter (see `next_segment_id`).
fn segment_seq_key(segment_id: &[u8; 16]) -> u64 {
    segment_seq_from_id(segment_id)
}

#[allow(dead_code)]
fn max_segment_seq_from_paths(paths: &[PathBuf]) -> u64 {
    let mut max = 0u64;
    for path in paths {
        if let Some(id) = crate::layout::segment_id_from_filename(path) {
            max = max.max(segment_seq_key(&id));
        }
    }
    max
}

fn write_store_descriptor_file(
    paths: &StorePaths,
    store_id: [u8; 16],
    created_ns: u64,
) -> Result<(), StoreError> {
    let frame = encode_store_descriptor_frame(store_id, created_ns)?;
    let path = paths.store_descriptor_file();
    crate::atomic_file::write_atomic(&path, &frame)?;
    Ok(())
}

fn verify_store_descriptor_if_present(
    paths: &StorePaths,
    store_id: [u8; 16],
) -> Result<(), StoreError> {
    let path = paths.store_descriptor_file();
    if !path.is_file() {
        return Ok(());
    }
    let bytes = fs::read(&path)?;
    let report = scan_forward(&bytes, SafetyLimits::default());
    let mut found = false;
    for (_off, frame) in report.verified_frames() {
        if frame.header.known_kind() != Some(FrameKind::StoreDescriptor) {
            continue;
        }
        let Some((id, _ns, _tag)) = decode_store_descriptor_body(&frame.body) else {
            return Err(StoreError::CorruptMeta("store descriptor body invalid"));
        };
        if id != store_id {
            return Err(StoreError::CorruptMeta(
                "store descriptor store_id mismatch",
            ));
        }
        found = true;
    }
    if !found {
        // File present but no verified store descriptor — tolerate for salvage;
        // identity still comes from store_id file.
        return Ok(());
    }
    Ok(())
}

fn sealed_segment_paths(
    paths: &StorePaths,
    placement: Option<&TierPlacement>,
) -> Result<Vec<PathBuf>, StoreError> {
    if let Some(p) = placement {
        crate::tier::available_sealed_paths(paths, p)
    } else {
        // Hot sealed only (legacy callers without placement).
        Ok(list_residiuum_files(&paths.segments_dir())?)
    }
}

fn all_segment_paths(
    paths: &StorePaths,
    placement: Option<&TierPlacement>,
    writer_shards: usize,
) -> Result<Vec<PathBuf>, StoreError> {
    let mut out = sealed_segment_paths(paths, placement)?;
    // DEF-096: pending seals are still authoritative for locators / rebuild.
    for p in list_pending_paths(paths)? {
        out.push(p);
    }
    // Axis B: every active shard file is authoritative.
    for p in paths.list_active_segment_paths(writer_shards.max(1)) {
        out.push(p);
    }
    // Sealed + pending first, then actives last.
    Ok(out)
}

fn total_segment_bytes(
    paths: &StorePaths,
    placement: Option<&TierPlacement>,
    writer_shards: usize,
) -> Result<u64, StoreError> {
    let mut total = 0u64;
    for path in all_segment_paths(paths, placement, writer_shards)? {
        total = total.saturating_add(fs::metadata(path)?.len());
    }
    Ok(total)
}

fn primary_cache_bytes(paths: &StorePaths) -> u64 {
    fs::metadata(primary_cache_path(&paths.indexes_dir()))
        .map(|m| m.len())
        .unwrap_or(0)
}

fn chunk_locator_count(locators: &ChunkLocatorMap) -> u64 {
    locators
        .values()
        .fold(0u64, |total, entries| total.saturating_add(entries.len() as u64))
}

fn chunk_locator_coverage_complete(index: &PrimaryIndex, locators: &ChunkLocatorMap) -> bool {
    index.iter_all().all(|(_subject, entry)| {
        let IndexEntry::Live(value) = entry else {
            return true;
        };
        let Some(manifest) = decode_chunk_manifest(&value.body) else {
            return true;
        };
        manifest
            .chunks
            .iter()
            .all(|slot| locators.contains_key(&slot.chunk_event_id))
    })
}

/// Apply item events from the active segment starting at byte offset `from_offset`.
///
/// Used with a frontier checkpoint so open cost is O(active tail), not O(all data).
fn apply_active_tail(
    index: &mut PrimaryIndex,
    mut chunk_locators: Option<&mut ChunkLocatorMap>,
    active_path: &Path,
    from_offset: u64,
    limits: SafetyLimits,
) -> Result<u64, StoreError> {
    let bytes = fs::read(active_path)?;
    if from_offset as usize > bytes.len() {
        return Err(StoreError::CorruptMeta("active frontier past file end"));
    }
    if from_offset as usize == bytes.len() {
        return Ok(0);
    }
    let tail = &bytes[from_offset as usize..];
    let report = scan_forward(tail, limits);
    for (relative_offset, frame) in report.verified_frames() {
        let offset = from_offset.saturating_add(relative_offset);
        if frame.header.known_kind() == Some(FrameKind::PayloadChunk) {
            if let (Some(locators), Some(piece)) = (
                chunk_locators.as_deref_mut(),
                decode_piece_body(&frame.body),
            ) {
                let segment_id = decode_item_envelope(&frame.envelope)
                    .map(|e| e.segment_id)
                    .unwrap_or([0u8; 16]);
                locators
                    .entry(frame.header.event_id)
                    .or_default()
                    .push(ChunkFrameLocator {
                        segment_id,
                        frame_offset: offset,
                        item_id: piece.item_id,
                        chunk_index: piece.index,
                        chunk_total: piece.total,
                        logical_len: piece.logical_len,
                        verified_body_hash: frame.body_hash,
                    });
            }
        } else if frame.header.known_kind() == Some(FrameKind::ItemEvent) {
            let Some(env) = decode_item_envelope(&frame.envelope) else {
                continue;
            };
            let body = slim_put_body_for_index(frame.body.clone(), false);
            index.apply_event(
                env.subject,
                env.event_kind,
                body,
                env.item_id,
                frame.header.event_id,
                env.segment_id,
                frame.header.writer_sequence,
                offset,
            );
        }
    }
    Ok(tail.len() as u64)
}

/// Relative scan-report name for a segment path under the store root.
fn examination_source_name(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .map(|rel| rel.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|_| {
            path.file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "unknown.residiuum".into())
        })
}

/// Collect item events for **index rebuild only** (DEF-095).
///
/// Ordinary put bodies are not retained in the event vector (only chunk
/// manifests), so rebuild peak RSS is O(keys × metadata) rather than O(dataset).
fn collect_item_events_and_chunk_locators_slim_for_index(
    paths: &StorePaths,
    limits: SafetyLimits,
    placement: Option<&TierPlacement>,
    writer_shards: usize,
) -> Result<(Vec<DiskEvent>, ChunkLocatorMap), StoreError> {
    let mut events = Vec::new();
    let mut chunk_locators = ChunkLocatorMap::new();
    for path in all_segment_paths(paths, placement, writer_shards)? {
        let bytes = fs::read(&path)?;
        let report = scan_forward(&bytes, limits);
        for (offset, frame) in report.verified_frames() {
            match frame.header.known_kind() {
                Some(FrameKind::ItemEvent) => {
                    let Some(env) = decode_item_envelope(&frame.envelope) else {
                        continue;
                    };
                    let body = slim_put_body_for_index(frame.body.clone(), false);
                    events.push(DiskEventPub {
                        file: path.clone(),
                        offset,
                        writer_sequence: frame.header.writer_sequence,
                        subject: env.subject,
                        kind: env.event_kind,
                        body,
                        item_id: env.item_id,
                        event_id: frame.header.event_id,
                        segment_id: env.segment_id,
                    });
                }
                Some(FrameKind::PayloadChunk) => {
                    let Some(piece) = decode_piece_body(&frame.body) else {
                        continue;
                    };
                    let segment_id = decode_item_envelope(&frame.envelope)
                        .map(|env| env.segment_id)
                        .unwrap_or([0u8; 16]);
                    chunk_locators
                        .entry(frame.header.event_id)
                        .or_default()
                        .push(ChunkFrameLocator {
                            segment_id,
                            frame_offset: offset,
                            item_id: piece.item_id,
                            chunk_index: piece.index,
                            chunk_total: piece.total,
                            logical_len: piece.logical_len,
                            verified_body_hash: frame.body_hash,
                        });
                }
                _ => {}
            }
        }
    }
    Ok((events, chunk_locators))
}

/// Rebuild a primary index solely from segment bytes (ignores in-memory state).
///
/// Recovery order is content-based so renames / reordering of segment files
/// (OVERVIEW §16.10) do not scramble put/delete application: segment mint
/// order (LE u64 in `segment_id`) → `writer_sequence` → offset. Duplicate
/// segment copies are ignored via `event_id` dedup (first occurrence wins).
///
/// When `placement` is set, only **available** tier media are scanned; offline
/// segments are omitted and must be reported via [`TierCoverage`].
///
/// DEF-095: rebuild is locator-first — does not materialize ordinary payload
/// bodies into the primary projection (chunk manifests only).
fn index_from_segments(
    paths: &StorePaths,
    limits: SafetyLimits,
    placement: Option<&TierPlacement>,
    writer_shards: usize,
) -> Result<PrimaryIndex, StoreError> {
    index_and_chunk_locators_from_segments(paths, limits, placement, writer_shards)
        .map(|(index, _)| index)
}

fn index_and_chunk_locators_from_segments(
    paths: &StorePaths,
    limits: SafetyLimits,
    placement: Option<&TierPlacement>,
    writer_shards: usize,
) -> Result<(PrimaryIndex, ChunkLocatorMap), StoreError> {
    let (mut events, chunk_locators) = collect_item_events_and_chunk_locators_slim_for_index(
        paths,
        limits,
        placement,
        writer_shards,
    )?;
    events.sort_by(cmp_disk_events);
    let mut index = PrimaryIndex::new();
    let mut seen_events: HashSet<[u8; 16]> = HashSet::new();
    for ev in events {
        if !seen_events.insert(ev.event_id) {
            continue;
        }
        index.apply_event(
            ev.subject,
            ev.kind,
            ev.body,
            ev.item_id,
            ev.event_id,
            ev.segment_id,
            ev.writer_sequence,
            ev.offset,
        );
    }
    Ok((index, chunk_locators))
}

/// Keep longest prefix of complete verified frames from the start of the buffer.
/// Incomplete tail is dropped (OVERVIEW §6.2 / §7.3).
fn recover_active_bytes(
    bytes: &[u8],
    store_id: [u8; 16],
    limits: SafetyLimits,
    accept_foreign_store: bool,
) -> Result<(Vec<u8>, [u8; 16]), StoreError> {
    if bytes.is_empty() {
        return Ok((Vec::new(), random_id()?));
    }
    let report = scan_forward(bytes, limits);
    let mut end = 0u64;
    let mut segment_id = None;
    let mut foreign_segment_id = None;
    for region in &report.regions {
        match region {
            residiuum_format::ScanRegion::VerifiedFrame { range, frame } => {
                // Only accept frames that form a contiguous prefix.
                if range.start != end {
                    break;
                }
                end = range.end;
                if frame.header.known_kind() == Some(FrameKind::SegmentDescriptor) {
                    if let Some((ids, _, _)) = residiuum_format::decode_descriptor_body(&frame.body)
                    {
                        if ids.store_id == store_id {
                            segment_id = Some(ids.segment_id);
                        } else if accept_foreign_store && foreign_segment_id.is_none() {
                            foreign_segment_id = Some(ids.segment_id);
                        }
                    }
                }
            }
            residiuum_format::ScanRegion::Hole { .. } => {
                // Stop at first hole after contiguous verified prefix.
                break;
            }
        }
    }
    let kept = bytes[..end as usize].to_vec();
    let sid = match segment_id.or(foreign_segment_id) {
        Some(id) => id,
        None if kept.is_empty() => {
            // Empty / no descriptor — caller must discard and mint via allocator.
            return Err(StoreError::CorruptMeta(
                "active segment has no recoverable segment_id",
            ));
        }
        None => {
            return Err(StoreError::CorruptMeta(
                "active segment missing SegmentDescriptor; refuse resume",
            ));
        }
    };
    Ok((kept, sid))
}

/// Rebuild an ActiveSegment that matches recovered complete frames.
///
/// Strategy: create a new segment with the same ids and re-append item events
/// found in `kept` (descriptor is recreated; summary is not present in active).
fn rebuild_active_from_bytes(
    kept: &[u8],
    store_id: [u8; 16],
    segment_id: [u8; 16],
    limits: SafetyLimits,
) -> Result<ActiveSegment, StoreError> {
    let ids = SegmentId::new(store_id, segment_id);
    let mut created_ns = now_ns();
    if !kept.is_empty() {
        let report = scan_forward(kept, limits);
        for (_r, frame) in report.verified_frames() {
            if frame.header.known_kind() == Some(FrameKind::SegmentDescriptor) {
                if let Some((_, ns, _)) = residiuum_format::decode_descriptor_body(&frame.body) {
                    created_ns = ns;
                }
            }
        }
    }

    let mut seg = ActiveSegment::create(ids, limits, created_ns)?;
    if kept.is_empty() {
        return Ok(seg);
    }

    let report = scan_forward(kept, limits);
    for (_offset, frame) in report.verified_frames() {
        // Re-append application content frames (items + payload chunks).
        // Preserve flags/kind via append_parts so chunked puts survive reopen.
        match frame.header.known_kind() {
            Some(FrameKind::ItemEvent) | Some(FrameKind::PayloadChunk) => {
                let mut header = frame.header.clone();
                // writer_sequence is reassigned by append_parts.
                header.writer_sequence = 0;
                seg.append_parts(&FrameParts {
                    header,
                    envelope: frame.envelope.clone(),
                    body: frame.body.clone(),
                })?;
            }
            _ => {}
        }
    }
    Ok(seg)
}

fn read_store_id(paths: &StorePaths) -> Result<[u8; 16], StoreError> {
    let raw = fs::read(paths.store_id_file())?;
    if raw.len() != 16 {
        return Err(StoreError::CorruptMeta("store_id must be 16 bytes"));
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(&raw);
    Ok(id)
}

/// Order durability modes by strength of the failure boundary (CSQ ack table).
fn durability_rank(mode: DurabilityMode) -> u8 {
    match mode {
        DurabilityMode::Memory => 0,
        DurabilityMode::Buffered => 1,
        DurabilityMode::Durable => 2,
    }
}

/// Stronger of two ack modes (for per-segment seal policy).
fn stronger_durability(a: DurabilityMode, b: DurabilityMode) -> DurabilityMode {
    if durability_rank(a) >= durability_rank(b) {
        a
    } else {
        b
    }
}

fn dedup_record(content_hash: [u8; 32], receipt: &WriteReceipt) -> DedupRecord {
    DedupRecord {
        content_hash,
        store_id: receipt.store_id,
        segment_id: receipt.segment_id,
        item_id: receipt.item_id,
        event_id: receipt.event_id,
        event_kind: receipt.event_kind,
        durability: receipt.durability,
        offset: receipt.offset,
    }
}

fn operation_request_error(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::KeyExists
            | StoreError::VersionConflict { .. }
            | StoreError::OperationIdentityConflict
            | StoreError::SubjectTooLong { .. }
            | StoreError::PayloadTooLarge
    )
}

/// Seal/rotate flush strength for an active segment given its max put ack.
///
/// On-disk frames always need at least `Buffered` (bytes handed to the OS).
/// `Durable` is required only if a Durable put was acked into this segment.
fn is_locator_resolve_error(e: &StoreError) -> bool {
    matches!(e, StoreError::LocatorFault(_))
}

fn seal_flush_mode(max_ack: DurabilityMode) -> DurabilityMode {
    match max_ack {
        DurabilityMode::Durable => DurabilityMode::Durable,
        // Memory-only never appends frames; descriptor-only still goes through
        // Buffered (write, no fsync) so we do not pay Durable for empty rotate.
        DurabilityMode::Memory | DurabilityMode::Buffered => DurabilityMode::Buffered,
    }
}

fn chunk_piece_matches_locator(
    body: &[u8],
    piece: &residiuum_format::ChunkPiece,
    locator: &ChunkFrameLocator,
) -> bool {
    *blake3::hash(body).as_bytes() == locator.verified_body_hash
        && piece.item_id == locator.item_id
        && piece.index == locator.chunk_index
        && piece.total == locator.chunk_total
        && piece.logical_len == locator.logical_len
}

/// Wall-clock ns for envelope `created_ns` (FORMAT_SPEC optional).
///
/// Hot path: do **not** call `SystemTime::now()` every put (Mode A prep was ~65%
/// of wall; OS wall-clock reads dominate). Cache wall time and interpolate with
/// [`Instant`] between refreshes (~1 ms). Still real-time based; not a synthetic
/// counter. Thread-local so concurrent shards stay independent.
fn now_ns() -> u64 {
    use std::cell::RefCell;
    use std::time::Instant;

    struct Cache {
        wall_ns: u64,
        tick: Instant,
    }

    thread_local! {
        static CLOCK: RefCell<Option<Cache>> = const { RefCell::new(None) };
    }

    CLOCK.with(|cell| {
        let mut slot = cell.borrow_mut();
        let cache = slot.get_or_insert_with(|| Cache {
            wall_ns: system_time_ns(),
            tick: Instant::now(),
        });
        let elapsed = cache.tick.elapsed();
        // Refresh OS wall clock about once per millisecond of put traffic.
        if elapsed.as_micros() >= 1000 {
            cache.wall_ns = system_time_ns();
            cache.tick = Instant::now();
            cache.wall_ns
        } else {
            cache
                .wall_ns
                .saturating_add(elapsed.as_nanos().min(u128::from(u64::MAX)) as u64)
        }
    })
}

fn system_time_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn sync_dir(path: &Path) -> Result<(), StoreError> {
    // Directory fsync is best-effort on platforms that support it.
    #[cfg(unix)]
    {
        let dir = File::open(path)?;
        dir.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

impl Drop for Store {
    fn drop(&mut self) {
        // A clean marker is a performance assertion, not authority. Publish it
        // only when this writer completed normally and no unresolved recovery
        // or failed stable boundary can exist. Any doubt deliberately leaves
        // the session dirty, making the next operation reconcile from media.
        if self.writer_lock.is_some()
            && !self.awo_writer_poisoned
            && !self.write_dedup_recovery_required
            && !std::thread::panicking()
        {
            let journal = write_dedup_journal_path(&self.paths);
            if sync_write_dedup_journal(&journal).is_ok() {
                let _ = mark_write_dedup_session_clean(&self.paths);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn create_put_get_delete() {
        let dir = tempdir().unwrap();
        let mut store = Store::create(dir.path()).unwrap();
        let receipt = store
            .put("user-42", b"alice", DurabilityMode::Durable)
            .unwrap();
        assert_eq!(receipt.durability, DurabilityMode::Durable);
        assert_eq!(receipt.event_kind, EventKind::Put);
        assert_eq!(
            store.get("user-42").unwrap().as_deref(),
            Some(b"alice".as_slice())
        );
        store.delete("user-42", DurabilityMode::Durable).unwrap();
        assert!(store.get("user-42").unwrap().is_none());
    }

    #[test]
    fn reopen_recovers_state() {
        let dir = tempdir().unwrap();
        {
            let mut store = Store::create(dir.path()).unwrap();
            store.put("a", b"1", DurabilityMode::Durable).unwrap();
            store.put("b", b"2", DurabilityMode::Buffered).unwrap();
        }
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get("a").unwrap().as_deref(), Some(b"1".as_slice()));
        assert_eq!(store.get("b").unwrap().as_deref(), Some(b"2".as_slice()));
    }

    #[test]
    fn reopen_reports_phase_metrics_and_no_normal_inventory_fallback() {
        let dir = tempdir().unwrap();
        {
            let mut store = Store::create(dir.path()).unwrap();
            store
                .put("measured", b"value", DurabilityMode::Durable)
                .unwrap();
        }
        let store = Store::open(dir.path()).unwrap();
        let metrics = store.open_metrics();
        assert!(metrics.total_ns > 0);
        assert!(metrics.identity_and_lock_ns > 0);
        assert!(metrics.inventory_ns > 0);
        assert!(metrics.inventory_descriptor_probe_bytes > 0);
        assert_eq!(metrics.inventory_fallback_scan_bytes, 0);
        assert_eq!(metrics.pending_seals_recovered, 0);
        assert_eq!(metrics.protected_pairs_recovered, 0);
        assert!(matches!(
            metrics.index_disposition,
            IndexOpenDisposition::Loaded | IndexOpenDisposition::TailReplayed
        ));
        assert!(metrics.total_ns >= metrics.inventory_ns);
    }

    #[test]
    fn clean_v4_checkpoint_reopens_chunked_store_without_full_index_scan() {
        let dir = tempdir().unwrap();
        let body = vec![0x5au8; 96 * 1024];
        {
            let mut store = Store::create(dir.path()).unwrap();
            store.set_chunk_threshold(1024);
            store.set_chunk_size(4096);
            store
                .put("chunked", &body, DurabilityMode::Durable)
                .unwrap();
            store.seal_active().unwrap();
            store.drain_lifecycle().unwrap();
            store.persist_index_cache().unwrap();
        }

        let store = Store::open(dir.path()).unwrap();
        let metrics = store.open_metrics();
        assert_eq!(metrics.index_full_scan_bytes, 0);
        assert_eq!(metrics.index_active_replay_bytes, 0);
        assert!(metrics.chunk_locators_from_checkpoint);
        assert_eq!(metrics.index_disposition, IndexOpenDisposition::Loaded);
        assert_eq!(metrics.index_cache_decision, IndexCacheDecision::AcceptedV4);
        assert!(metrics.index_cache_bytes > 0);
        assert!(metrics.index_entries > 0);
        assert!(metrics.chunk_locator_entries > 0);
        assert_eq!(
            store.get("chunked").unwrap().as_deref(),
            Some(body.as_slice())
        );
    }

    #[test]
    fn v4_checkpoint_replays_only_new_active_chunk_locators() {
        let dir = tempdir().unwrap();
        let before = vec![0x31u8; 24 * 1024];
        let after = vec![0x32u8; 28 * 1024];
        {
            let mut store = Store::create(dir.path()).unwrap();
            store.set_chunk_threshold(1024);
            store.set_chunk_size(4096);
            store
                .put("before", &before, DurabilityMode::Durable)
                .unwrap();
            store.persist_index_cache().unwrap();
            store.put("after", &after, DurabilityMode::Durable).unwrap();
        }

        let store = Store::open(dir.path()).unwrap();
        let metrics = store.open_metrics();
        assert_eq!(metrics.index_full_scan_bytes, 0);
        assert!(metrics.index_active_replay_bytes > 0);
        assert!(metrics.chunk_locators_from_checkpoint);
        assert_eq!(metrics.index_disposition, IndexOpenDisposition::TailReplayed);
        assert_eq!(metrics.index_cache_decision, IndexCacheDecision::AcceptedV4);
        assert_eq!(
            store.get("before").unwrap().as_deref(),
            Some(before.as_slice())
        );
        assert_eq!(
            store.get("after").unwrap().as_deref(),
            Some(after.as_slice())
        );
    }

    #[test]
    fn writable_open_upgrades_v3_checkpoint_after_one_locator_scan() {
        let dir = tempdir().unwrap();
        let body = vec![0x73u8; 20 * 1024];
        {
            let mut store = Store::create(dir.path()).unwrap();
            store.set_chunk_threshold(1024);
            store.set_chunk_size(4096);
            store
                .put("legacy", &body, DurabilityMode::Durable)
                .unwrap();
            let frontier = store.current_index_frontier().unwrap();
            crate::index_cache::write_primary_index_frontier_v3_for_test(
                &primary_cache_path(&store.paths.indexes_dir()),
                store.store_id,
                &frontier,
                &store.durable_index,
            )
            .unwrap();
        }

        let store = Store::open(dir.path()).unwrap();
        let metrics = store.open_metrics();
        assert!(metrics.index_full_scan_bytes > 0);
        assert!(!metrics.chunk_locators_from_checkpoint);
        assert_eq!(metrics.index_disposition, IndexOpenDisposition::LegacyUpgraded);
        assert_eq!(metrics.index_cache_decision, IndexCacheDecision::AcceptedLegacy);
        let cache = fs::read(primary_cache_path(&store.paths.indexes_dir())).unwrap();
        assert_eq!(&cache[..8], b"RIDX0004");
        assert_eq!(
            store.get("legacy").unwrap().as_deref(),
            Some(body.as_slice())
        );
    }

    #[test]
    fn chimera_seal_layout_and_get_resolve() {
        let dir = tempdir().unwrap();
        // CSE-2R Materialized embed expectations — not CompactShadow product default.
        let mut store = Store::create_with_shards_mode(
            dir.path(),
            1,
            crate::recovery_shadow::RecoveryMode::Materialized,
        )
        .unwrap();
        let tiny = b"hi";
        let medium = vec![3u8; 200];
        let large = vec![5u8; 32 * 1024];
        store.put("t", tiny, DurabilityMode::Durable).unwrap();
        store.put("m", &medium, DurabilityMode::Durable).unwrap();
        store.put("l", &large, DurabilityMode::Durable).unwrap();

        // Capture establishing segment from index before seal rotates writer.
        let seg = match store.index.get(b"t") {
            Some(crate::index::IndexEntry::Live(lv)) => lv.segment_id,
            _ => panic!("expected live t"),
        };

        store.seal_active().unwrap();
        store.drain_lifecycle().unwrap();

        let layout = store
            .load_chimera_layout(seg)
            .unwrap()
            .expect("chimera layout after seal");
        let counts = layout.count_by_kind();
        // CSE-2R: product seal embeds Materialized payloads (safety rollback).
        assert_eq!(counts.segment_frame, 0);
        assert!(counts.inline >= 1);
        assert!(layout.get(b"t").unwrap().as_deref() == Some(tiny.as_slice()));

        // Hot Store::get uses PrimaryIndex (not a full .cmr reload).
        assert_eq!(store.get("t").unwrap().as_deref(), Some(tiny.as_slice()));
        assert_eq!(store.get("m").unwrap().as_deref(), Some(medium.as_slice()));
        assert_eq!(store.get("l").unwrap().as_deref(), Some(large.as_slice()));
        // Explicit Chimera probe resolves embedded Materialized bodies.
        assert_eq!(
            store.get_via_chimera("t").unwrap().as_deref(),
            Some(tiny.as_slice())
        );
        assert_eq!(
            store.get_via_chimera("m").unwrap().as_deref(),
            Some(medium.as_slice())
        );
        assert_eq!(
            store.get_via_chimera("l").unwrap().as_deref(),
            Some(large.as_slice())
        );
    }

    #[test]
    fn get_uses_primary_index_without_chimera_sidecars() {
        let dir = tempdir().unwrap();
        let mut store = Store::create_with_shards_mode(
            dir.path(),
            1,
            crate::recovery_shadow::RecoveryMode::Materialized,
        )
        .unwrap();
        store.put("k", b"value", DurabilityMode::Durable).unwrap();
        let seg = match store.index.get(b"k") {
            Some(crate::index::IndexEntry::Live(lv)) => lv.segment_id,
            _ => panic!("expected live k"),
        };
        store.seal_active().unwrap();
        store.drain_lifecycle().unwrap();
        assert!(store.load_chimera_layout(seg).unwrap().is_some());
        // Wipe derived chimera; hot get must still resolve via index locator/pread.
        let chimera_root = crate::chimera::chimera_dir(&store.paths);
        if chimera_root.is_dir() {
            fs::remove_dir_all(&chimera_root).unwrap();
        }
        assert!(store.load_chimera_layout(seg).unwrap().is_none());
        assert_eq!(
            store.get("k").unwrap().as_deref(),
            Some(b"value".as_slice())
        );
        assert!(store.get_via_chimera("k").unwrap().is_none());
    }

    #[test]
    fn durable_puts_are_locator_first_not_body_resident() {
        // DEF-095: multi-KiB durable payloads must not pin in PrimaryIndex RSS.
        let dir = tempdir().unwrap();
        let mut store = Store::create(dir.path()).unwrap();
        let payload = vec![0xABu8; 8192];
        for i in 0..64 {
            store
                .put(&format!("k{i}"), &payload, DurabilityMode::Buffered)
                .unwrap();
        }
        // Resident bodies ≪ 64 * 8 KiB (locator-only; only metadata).
        let resident = store.resident_index_body_bytes();
        assert!(
            resident < 8 * 1024,
            "expected slim index, resident_body_bytes={resident}"
        );
        // Gets still return full payloads via frame pread.
        assert_eq!(
            store.get("k0").unwrap().as_deref(),
            Some(payload.as_slice())
        );
        assert_eq!(
            store.get("k63").unwrap().as_deref(),
            Some(payload.as_slice())
        );

        // Memory-mode still keeps the body resident (no durable frame).
        store
            .put("mem", b"only-in-ram", DurabilityMode::Memory)
            .unwrap();
        assert!(store.resident_index_body_bytes() >= b"only-in-ram".len() as u64);
        assert_eq!(
            store.get("mem").unwrap().as_deref(),
            Some(b"only-in-ram".as_slice())
        );

        // Reopen rebuilds slim index and still serves durable keys.
        drop(store);
        let store = Store::open(dir.path()).unwrap();
        assert!(store.resident_index_body_bytes() < 8 * 1024);
        assert_eq!(
            store.get("k0").unwrap().as_deref(),
            Some(payload.as_slice())
        );
        // Memory-mode publish did not survive reopen.
        assert!(store.get("mem").unwrap().is_none());
    }

    #[test]
    fn chimera_rebuild_after_wipe() {
        let dir = tempdir().unwrap();
        let mut store = Store::create_with_shards_mode(
            dir.path(),
            1,
            crate::recovery_shadow::RecoveryMode::Materialized,
        )
        .unwrap();
        store.put("x", b"tiny-x", DurabilityMode::Durable).unwrap();
        store
            .put("y", &vec![1u8; 128], DurabilityMode::Durable)
            .unwrap();
        let seg = match store.index.get(b"x") {
            Some(crate::index::IndexEntry::Live(lv)) => lv.segment_id,
            _ => panic!("expected live x"),
        };
        store.seal_active().unwrap();
        store.drain_lifecycle().unwrap();
        assert!(store.load_chimera_layout(seg).unwrap().is_some());

        // Wipe derived chimera tree and rebuild.
        let chimera_root = crate::chimera::chimera_dir(&store.paths);
        if chimera_root.is_dir() {
            fs::remove_dir_all(&chimera_root).unwrap();
        }
        assert!(store.load_chimera_layout(seg).unwrap().is_none());
        // Fallback get still works via PrimaryIndex.
        assert_eq!(
            store.get("x").unwrap().as_deref(),
            Some(b"tiny-x".as_slice())
        );
        let n = store.rebuild_chimera_layouts().unwrap();
        assert!(n >= 1);
        let layout = store.load_chimera_layout(seg).unwrap().expect("rebuilt");
        assert_eq!(layout.count_by_kind().segment_frame, 0);
        assert!(layout.count_by_kind().inline >= 1);
        assert_eq!(
            store.get_via_chimera("x").unwrap().as_deref(),
            Some(b"tiny-x".as_slice())
        );
    }

    #[test]
    fn parallel_cook_put_many_roundtrip() {
        let dir = tempdir().unwrap();
        let mut store = Store::create(dir.path()).unwrap();
        store.set_seal_threshold(64 * 1024 * 1024);
        store.set_cook_parallelism(4);
        let payload = vec![7u8; 4096];
        let keys: Vec<String> = (0..64).map(|i| format!("pk{i}")).collect();
        let items: Vec<(&str, &[u8])> = keys
            .iter()
            .map(|k| (k.as_str(), payload.as_slice()))
            .collect();
        let receipts = store.put_many(&items, DurabilityMode::Buffered).unwrap();
        assert_eq!(receipts.len(), 64);
        for k in &keys {
            assert_eq!(store.get(k).unwrap().as_deref(), Some(payload.as_slice()));
        }
    }

    #[test]
    fn chimera_compact_writes_output_layout() {
        let dir = tempdir().unwrap();
        let mut store = Store::create(dir.path()).unwrap();
        store.put("a", b"one", DurabilityMode::Durable).unwrap();
        store
            .put("b", &vec![2u8; 256], DurabilityMode::Durable)
            .unwrap();
        store.seal_active().unwrap();
        let report = store.compact_live().unwrap();
        let out_seg = report.segment_id;
        let layout = store
            .load_chimera_layout(out_seg)
            .unwrap()
            .expect("compact output chimera");
        assert!(layout.len() >= 2);
        assert_eq!(layout.count_by_kind().segment_frame, 0);
        assert!(
            layout.get(b"a").ok().flatten().as_deref() == Some(b"one".as_slice()),
            "CSE-2R live-projection Chimera embeds payloads"
        );
        assert_eq!(
            store.get_via_chimera("a").unwrap().as_deref(),
            Some(b"one".as_slice())
        );
        assert_eq!(
            store.get_via_chimera("b").unwrap().as_deref(),
            Some(vec![2u8; 256].as_slice())
        );
    }

    #[test]
    fn key_atomic_cas_put_and_delete() {
        let dir = tempdir().unwrap();
        let mut store = Store::create(dir.path()).unwrap();
        // create-if-absent
        let r1 = store
            .put_subject_bytes_if(b"k", b"v1", DurabilityMode::Durable, WriteCondition::Absent)
            .unwrap();
        assert!(matches!(
            store.put_subject_bytes_if(
                b"k",
                b"v2",
                DurabilityMode::Durable,
                WriteCondition::Absent,
            ),
            Err(StoreError::KeyExists)
        ));
        // stale version
        let stale = [9u8; 16];
        assert!(matches!(
            store.put_subject_bytes_if(
                b"k",
                b"v3",
                DurabilityMode::Durable,
                WriteCondition::LiveEventId(stale),
            ),
            Err(StoreError::VersionConflict { expected, observed: Some(_) }) if expected == stale
        ));
        // matching version
        let r2 = store
            .put_subject_bytes_if(
                b"k",
                b"v4",
                DurabilityMode::Durable,
                WriteCondition::LiveEventId(r1.event_id),
            )
            .unwrap();
        assert_ne!(r1.event_id, r2.event_id);
        assert_eq!(store.get("k").unwrap().as_deref(), Some(b"v4".as_slice()));
        // delete with version
        store
            .delete_subject_bytes_if(
                b"k",
                DurabilityMode::Durable,
                WriteCondition::LiveEventId(r2.event_id),
            )
            .unwrap();
        assert!(store.get("k").unwrap().is_none());
        // present fails when absent
        assert!(matches!(
            store.delete_subject_bytes_if(b"k", DurabilityMode::Durable, WriteCondition::Present,),
            Err(StoreError::VersionConflict { observed: None, .. })
        ));
    }

    /// APB-2 T8: under exclusive mutex, concurrent put_if with the same LiveEventId
    /// admits exactly one winner (lost-update rejected for others).
    #[test]
    fn concurrent_put_if_one_wins() {
        use std::sync::{Arc, Barrier, Mutex};
        use std::thread;

        let dir = tempdir().unwrap();
        let store = Arc::new(Mutex::new(Store::create(dir.path()).unwrap()));
        let r0 = {
            let mut g = store.lock().unwrap();
            g.put_subject_bytes_if(b"k", b"v0", DurabilityMode::Durable, WriteCondition::Absent)
                .unwrap()
        };
        let n = 8usize;
        let barrier = Arc::new(Barrier::new(n));
        let wins = Arc::new(Mutex::new(0u32));
        let conflicts = Arc::new(Mutex::new(0u32));
        let mut handles = Vec::new();
        for i in 0..n {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let wins = Arc::clone(&wins);
            let conflicts = Arc::clone(&conflicts);
            let expected = r0.event_id;
            handles.push(thread::spawn(move || {
                barrier.wait();
                let body = format!("w{i}").into_bytes();
                let mut g = store.lock().unwrap();
                match g.put_subject_bytes_if(
                    b"k",
                    &body,
                    DurabilityMode::Durable,
                    WriteCondition::LiveEventId(expected),
                ) {
                    Ok(_) => *wins.lock().unwrap() += 1,
                    Err(StoreError::VersionConflict { .. }) => *conflicts.lock().unwrap() += 1,
                    Err(e) => panic!("unexpected: {e:?}"),
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(*wins.lock().unwrap(), 1, "exactly one CAS winner");
        assert_eq!(*conflicts.lock().unwrap(), (n as u32) - 1);
        let live = store.lock().unwrap().get("k").unwrap().unwrap();
        assert!(live.starts_with(b"w"), "winner body present");
    }

    /// Concurrent create-if-absent: exactly one insert wins.
    #[test]
    fn concurrent_create_absent_one_wins() {
        use std::sync::{Arc, Barrier, Mutex};
        use std::thread;

        let dir = tempdir().unwrap();
        let store = Arc::new(Mutex::new(Store::create(dir.path()).unwrap()));
        let n = 8usize;
        let barrier = Arc::new(Barrier::new(n));
        let wins = Arc::new(Mutex::new(0u32));
        let exists = Arc::new(Mutex::new(0u32));
        let mut handles = Vec::new();
        for i in 0..n {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let wins = Arc::clone(&wins);
            let exists = Arc::clone(&exists);
            handles.push(thread::spawn(move || {
                barrier.wait();
                let body = format!("c{i}").into_bytes();
                let mut g = store.lock().unwrap();
                match g.put_subject_bytes_if(
                    b"new",
                    &body,
                    DurabilityMode::Durable,
                    WriteCondition::Absent,
                ) {
                    Ok(_) => *wins.lock().unwrap() += 1,
                    Err(StoreError::KeyExists) => *exists.lock().unwrap() += 1,
                    Err(e) => panic!("unexpected: {e:?}"),
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(*wins.lock().unwrap(), 1);
        assert_eq!(*exists.lock().unwrap(), (n as u32) - 1);
        assert!(store.lock().unwrap().get("new").unwrap().is_some());
    }

    /// Product watermark growth: opt-in API, default off, put+get+reopen round-trip.
    #[test]
    fn segment_growth_watermark_opt_in_put_reopen() {
        let dir = tempdir().unwrap();
        let mut store = Store::create(dir.path()).unwrap();
        assert_eq!(
            store.segment_growth_policy(),
            crate::segment_growth::SegmentGrowthPolicy::GrowOnAppend
        );
        store
            .set_segment_growth_policy(
                crate::segment_growth::SegmentGrowthPolicy::watermark_default(),
            )
            .unwrap();
        assert!(store.segment_growth_policy().is_watermark());
        store.warm_segment_runway().unwrap();
        store
            .put("wm", b"hello-watermark", DurabilityMode::Buffered)
            .unwrap();
        assert_eq!(
            store.get("wm").unwrap().as_deref(),
            Some(b"hello-watermark".as_slice())
        );
        drop(store);
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.get("wm").unwrap().as_deref(),
            Some(b"hello-watermark".as_slice())
        );
        // Policy is process-local (not persisted); reopen returns to grow-on-append.
        assert_eq!(
            store.segment_growth_policy(),
            crate::segment_growth::SegmentGrowthPolicy::GrowOnAppend
        );
    }

    /// Background preparer: warm capacity, then puts must not pay put-path zero.
    #[test]
    fn segment_growth_watermark_background_runway_put() {
        let dir = tempdir().unwrap();
        let mut store = Store::create(dir.path()).unwrap();
        store
            .set_segment_growth_policy(crate::segment_growth::SegmentGrowthPolicy::watermark(
                4 * 1024 * 1024,
                1024 * 1024,
            ))
            .unwrap();
        store.warm_segment_runway().unwrap();
        for i in 0..200u32 {
            let key = format!("k{i}");
            store
                .put(&key, b"xxxxxxxx", DurabilityMode::Buffered)
                .unwrap();
        }
        assert_eq!(
            store.get("k0").unwrap().as_deref(),
            Some(b"xxxxxxxx".as_slice())
        );
    }
}
