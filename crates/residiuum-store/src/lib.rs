//! Residiuum single-node authoritative store (Stages 3 + 6 + 9).
//!
//! Append-only segments on the filesystem, subject-keyed put/get/delete, and
//! catalog-independent recovery via [`residiuum_format`] salvage scanning.
//!
//! Stage 6 adds rebuildable catalogs, secondary index files, subject history,
//! chunked payloads with partial maps, live-state compaction, and checkpoints.
//!
//! Hydra adds adaptive **per-segment** read indexes at seal time (Eytzinger,
//! PGM/RadixSpline, compressed radix, MPHF+fingerprint) with multithreaded
//! rebuild — derived only under `indexes/seg/`.
//!
//! Chimera adds workload-compiled **value placement** (inline / point micro-pages
//! / scan extents / large-value log), adaptive I/O path selection, a background
//! compiler planner, and **seal/compaction layout sidecars** under
//! `indexes/chimera/` (seal/compact derived placement). Hot `Store::get` uses a
//! **locator-first PrimaryIndex** (DEF-095): map lookup then resident body or
//! bounded frame pread — not full-dataset body residency. Use
//! `Store::get_via_chimera` to probe layouts.
//!
//! Stage 9 adds storage tiers (hot/warm/cold/archive), segment move/copy with
//! stable identities, hierarchical segment catalogs, offline-tier coverage
//! honesty, and multi-generation format classification (byte preservation).
//!
//! DEF-052 adds phased format migration (preflight/plan/apply/verify/rollback)
//! with a declared wire and protocol compatibility matrix.
//!
//! Normative: OVERVIEW §§5–7, §9, §13; FORMAT_SPEC frames/segments/chunks.

#![deny(missing_docs)]

/// Adaptive Write Optimiser pure model (AWO-0) and future runtime modules.
pub mod adaptive_write;

mod atomic_file;
mod backup;
mod boundary_probe;
mod catalog;
mod chimera;
mod chunk_payload;
mod compact;
mod composed_failure;
mod crash_matrix;
mod csq5_campaign;
mod csq_harness;
mod cursor;
mod durability;
mod envelope;
mod erasure;
mod error;
mod failpoint;
mod heap;
mod history;
mod hydra;
mod ids;
mod index;
mod index_cache;
mod incremental_seal;
mod kernel;
mod large_value;
mod layout;
mod lifecycle;
mod media;
mod media_inventory;
mod migrate;
mod positioned_io;
mod protected_pair;

pub use protected_pair::recover_protected_pairs;
mod recovery;
mod recovery_shadow;
mod scrub;
mod seal_pipeline;
mod secondary;
mod segment_allocator;
mod segment_catalog;
mod runway_preparer;
mod segment_growth;
mod store;
mod tier;
mod token_keys;
mod write_dedup;
mod writer_lock;

pub use atomic_file::{
    previous_path, read_with_previous, recover_previous_or_corrupt, sync_dir as sync_parent_dir,
    write_atomic, write_atomic_keep_previous, write_atomic_with, AtomicWriteOptions, PREV_SUFFIX,
};
pub use backup::{
    backup_manifest_path, backup_store_path, load_and_verify_manifest, restore_full_backup,
    verify_package_files, write_full_backup, BackupConsistency, BackupFileEntry, BackupManifest,
    BackupReport, RestoreOptions, RestoreReport, BACKUP_MANIFEST_FILE, BACKUP_PROFILE,
    BACKUP_STORE_DIR,
};
pub use boundary_probe::{
    BoundaryCounters, BoundaryCoverage, BoundaryEvent, BoundaryKind, BoundaryOutcome,
    BoundaryProbe, BoundarySnapshot, FileRole, LatencyHistogram,
};
pub use catalog::{
    collection_name_from_subject, collections_catalog_path, try_load_collection_catalog,
    CollectionCatalog, COLLECTIONS_CATALOG_FILE,
};
pub use chimera::{
    build_compact_layout, build_layout, build_materialized_layout, chimera_dir, chimera_layout_path,
    classify_value, decode_record, delete_chimera_layout, initial_locator_kind,
    pack_point_containers, place_value, plan_compile, plan_recluster_range, read_slot, resolve,
    select_io_path, try_load_chimera_layout, write_chimera_layout, ChimeraKindCounts, ChimeraLayout,
    ClassifyOptions, CompactFrameRef, CompilerOp, CompilerOptions, CompilerPlan, ContainerBuilder,
    ContainerSlot, IoHints, IoPath, IoSelectOptions, LifetimeClass, LocatorKind, PlacementHints,
    PointContainer, RecordStats, ResolveContext, ResolvedValue, TemperatureClass, ValueClass,
    ValueLocator, ValueLog, ValueLogRecord, CHIMERA_LAYOUT_VERSION, CHIMERA_LAYOUT_VERSION_LEGACY,
    CODEC_RAW, CONTAINER_MAGIC, CONTAINER_VERSION, DEFAULT_CONTAINER_TARGET, DEFAULT_MEDIUM_MAX,
    DEFAULT_TINY_MAX, VALUE_LOG_HEADER_LEN, VALUE_LOG_MAGIC,
};
pub use chunk_payload::{
    decode_chunk_manifest, encode_chunk_manifest, is_chunk_manifest, reassemble_with_manifest,
    resolve_piece, ChunkManifest, ChunkSlot, PayloadResult, ResolvedChunk, CHUNK_MANIFEST_MAGIC,
    DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_THRESHOLD,
};
pub use compact::{
    compaction_job_path, compaction_jobs_dir, list_compact_jobs, pread_item_body_if_segment,
    pread_item_body_matching, try_load_compact_job, CheckpointMeta, CompactJob, CompactOptions,
    CompactPhase, CompactReport, LocatorExpect, COMPACTION_JOB_DIR, COMPACTION_JOB_SUFFIX,
};
pub use composed_failure::{
    failure_class_action, schedule as schedule_failure_combinations,
    validate_combinations as validate_failure_combinations, FailureCombination,
    FailureCombinationDoc, ScheduleDecision,
};
pub use crash_matrix::{
    all_cells, ci_subset_cells, load_embedded as load_crash_matrix,
    validate as validate_crash_matrix, CrashMatrix, ExpectedReopen, MatrixFailpoint,
    MatrixOperation, CRASH_MATRIX_JSON,
};
pub use csq5_campaign::{
    action_for_failure_class, classify_outcome, composed_schedule, load_failure_combinations,
    matrix_totals, probe_linux_loopback_lane, validate_reopen_expectation, CampaignReport,
    CrashOutcomeClass, FilesystemEvidence, LaneStatus, PortableFsImage,
};
pub use csq_harness::{
    harness_is_approved, parse_harness, BarrierKind, BarrierPhase, CrashController,
    FilesystemImageHarness, HarnessCapability, HARNESS_CAPABILITIES,
};
pub use cursor::{
    scan_generation, verify_continuation_token, CoverageGap, CoverageGapKind, DocumentScanPage,
    KeyScanPage, LiveScanPage, LiveScanPageOptions, CURSOR_PROFILE, DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE, MAX_TOKEN_BYTES,
};
pub use token_keys::{
    ContinuationKeyring, TokenKeyGeneration, CURSOR_TOKEN_KEYS_FILE, TOKEN_KEY_PROFILE,
    TOKEN_SECRET_LEN,
};
/// Extent map types used by [`PayloadResult::Partial`] (FORMAT_SPEC §8).
pub use residiuum_format::{ByteRange, LogicalExtent};
pub use durability::DurabilityMode;
pub use store::{DiagnosticIoSink, StoreWritePathStats, WriteCondition};
pub use envelope::{
    decode_item_envelope, encode_item_envelope, EventKind, ItemEnvelope, MAX_SUBJECT_LEN,
};
pub use erasure::{
    decode_shards, encode_shards, is_shard_key, shard_layout_note, ErasureManifest, ErasureProfile,
    DEFAULT_DATA_SHARDS, DEFAULT_PARITY_SHARDS,
};
pub use error::{LocatorFault, LocatorFaultKind, StoreError};
pub use media_inventory::{
    build_authoritative_inventory, build_authoritative_inventory_with_policy,
    heal_identical_publish_aliases, inventory_authoritative_media, refuse_authoritative_collisions,
    rename_exclusive, InventoryPolicy, MediaInventory,
};
pub use failpoint::{
    any_armed as failpoints_armed, arm as arm_failpoint, arm_n as arm_failpoint_n,
    arm_once as arm_failpoint_once, clear as clear_failpoints, clear_all as clear_failpoints_all,
    clear_visits as clear_failpoint_visits, consume_short_write as consume_failpoint_short_write,
    disable_hit_proof as disable_failpoint_hit_proof, disarm as disarm_failpoint,
    enable_hit_proof as enable_failpoint_hit_proof, hit as hit_failpoint,
    hit_proof_enabled as failpoint_hit_proof_enabled, is_armed as failpoint_is_armed,
    require_visited as require_failpoint_visited, short_write_len as failpoint_short_write_len,
    visit_count as failpoint_visit_count, Action as FailpointAction,
};
/// Capability-gated heap façades (HP-003). Prefer these over the legacy raw store
/// for qualified heap isolation; the unscoped store remains available behind the
/// default `legacy-raw-store` feature.
pub use heap::{
    active_snapshot, admin_op_binding, admin_op_dedup_path, build_backup_manifest,
    collection_create_binding, create_collection_idempotent, create_object, decode_purge_receipt,
    delete_rebuildable_catalogs, destroy_data_key, disaster_recovery_restore_retaining_id,
    encode_purge_receipt, heap_binding_envelope, heap_label_envelope, heap_object_media_dir,
    labelled_unit_readable, load_admin_op_dedup, load_identity_tombstone, load_staged_genesis,
    old_deployment_credential_invalid, publish_staged_genesis, rebuild_and_persist_all_catalogs,
    rebuild_heap_entry_from_chain, rebuild_object_entry_from_chain, record_admin_op_dedup,
    refuse_access_from_payload_restore, refuse_clear_tombstone_via_payload_restore,
    classify_mixed_heap_frame, refuse_retain_id_without_ceremony, rename_heap, rename_object,
    require_admit, resolve_admin_op_dedup, restore_payload_to_new_heap, retire_heap, retire_object,
    save_admin_op_dedup, stage_heap_genesis, staging_is_non_discoverable,
    try_load_collections_catalog, try_load_heap_catalog, try_load_streams_catalog,
    verify_purge_receipt, wipe_heap_object_media, write_identity_tombstone, AdminOpDedupRecord,
    AdminOpDedupTable, AdminReceipt, CreatedCollectionAdmin, CutoverGate,
    DataKeyDestructionReceipt, DataKeyHandle, DataKeyProvider, DisasterRecoveryCeremony,
    CollectionScanHole, CollectionScanHoleReason, CollectionScanPage, DisasterRecoveryPackage,
    DisasterRecoveryTakeoverResult, HeapBackupManifest, HeapCatalogEntry, HeapLifecycle,
    HeapMetaLayout, HeapMigrationJob, HeapRetentionPolicy, HeapStore,
    HsmBackendKind, HsmCapabilities, HsmDataKeyConfig, HsmDataKeyProvider, IdentityTombstone,
    IncompletePurgeResult, InProcessDataKeyProvider, InventoryFrame, InventorySegment,
    MaintenanceStore, MediaDomain, MigrationPhase, MigrationStateV1, MixedHeapSalvageClass,
    ObjectCatalogEntry, ObjectKind, OperationCommitStats, PayloadOnlyRestore, PurgeCoverageUnit,
    PurgePlan, PurgeReceipt,
    RecoveryStore, ReplicaStore, RetentionScheduler, SourceInventory, StagedGenesis, StoreHost,
    VersionedCollectionScanPage, VersionedCollectionValue,
    TombstoneKind, ADMIN_OP_DEDUP_FILE, COLLECTION_CREATE_OP,
    ADMITTED_FILE as HEAP_MIGRATE_ADMITTED_FILE,
    ASSIGNMENTS_FILE as HEAP_MIGRATE_ASSIGNMENTS_FILE,
    ASSIGNMENTS_HASH_DOMAIN as HEAP_MIGRATE_ASSIGNMENTS_HASH_DOMAIN,
    BACKUP_MANIFEST_DOMAIN as HEAP_BACKUP_MANIFEST_DOMAIN,
    COLLECTIONS_CATALOG_FILE as HEAP_COLLECTIONS_CATALOG_FILE,
    DATA_KEY_DESTROY_DOMAIN as HEAP_DATA_KEY_DESTROY_DOMAIN, HEAP_CATALOG_FILE,
    HEAP_LIFECYCLE_PROFILE, HEAP_MIGRATE_DIR, HEAP_MIGRATE_PROFILE,
    INCOMPLETE_PURGE_DOMAIN as HEAP_INCOMPLETE_PURGE_DOMAIN,
    INVENTORY_HASH_DOMAIN as HEAP_MIGRATE_INVENTORY_HASH_DOMAIN,
    LIFECYCLE_DIR as HEAP_LIFECYCLE_DIR, PURGE_COVERAGE_DOMAIN as HEAP_PURGE_COVERAGE_DOMAIN,
    RETENTION_POLICY_DOMAIN as HEAP_RETENTION_POLICY_DOMAIN,
    STATE_FILE as HEAP_MIGRATE_STATE_FILE, STREAMS_CATALOG_FILE as HEAP_STREAMS_CATALOG_FILE,
    TOMBSTONE_DOMAIN as HEAP_TOMBSTONE_DOMAIN,
};
#[cfg(feature = "aws-kms")]
pub use heap::{AwsKmsDataKeyProvider, SharedAwsKmsDataKeyProvider};
pub use history::{
    BeforeEvent, HistoricalSearchResult, HistoryEvent, ReadBudget, RecoveryReadOptions,
    SubjectHistory, VersionedPayloadResult,
};
pub use hydra::{
    build as build_hydra_index, build_many as build_hydra_indexes, classify_keys,
    delete_hydra_index, hydra_dir, hydra_index_path, records_from_segment_bytes, select_index_kind,
    try_load_hydra_index, write_hydra_index, HydraBuildOptions, HydraIndex, IndexKind, KeyShape,
    SegmentRecord, DEFAULT_TINY_THRESHOLD,
};
pub use ids::{
    fill_random, hex16 as id_hex16, mint_sortable_segment_id, random_id, segment_seq_from_id,
    subject_item_id, ID_LEN, ID_PROFILE,
};
pub use large_value::{
    rewrite_heavy, AdmitDecision, LargeValuePolicy, PayloadLayout, DEFAULT_MAX_LOGICAL_PAYLOAD_BYTES,
    LARGE_VALUE_PROFILE_ID,
};
pub use index::{IndexEntry, LiveValue};
pub use index_cache::{
    diagnose_primary_cache, IndexFrontier, LifecycleDiag, PrimaryCacheDiag, PrimaryCacheValidation,
    PRIMARY_CACHE_FILE,
};
pub use layout::{hex16, list_residiuum_files, segment_id_from_filename, unhex16, StorePaths};
pub use lifecycle::{policy_path, LifecyclePolicy, LifecycleRule, LIFECYCLE_POLICY_FILE};
pub use media::{
    media_root_directory, media_root_directory_with, open_media, open_media_with,
    CloudMirrorConfig, FilesystemMedia, LocalObjectMedia, MediaBackend, MediaLocator,
    MirroredCloudMedia, ObjectMediaUri, ObjectScheme, UnsupportedCloudMedia,
};
pub use migrate::{
    load_migration_job, migrate_apply, migrate_dir, migrate_job_path, migrate_plan,
    migrate_preflight, migrate_rollback, migrate_store, migrate_verify, snapshot_protocol_compat,
    snapshot_wire_matrix, MigrateFileAction, MigrateFilePlan, MigrateOptions, MigratePhase,
    MigratePreflight, MigrateReport, MigrationJob, ProtocolCompatSnapshot, WireMatrixRow,
    MIGRATE_DIR, MIGRATE_JOB_FILE, MIGRATE_PROFILE, PROTOCOL_MAJOR_DECLARED,
    PROTOCOL_MINOR_DECLARED, PROTOCOL_PROFILE_DECLARED, RPC_WIRE_LABEL_DECLARED,
};
pub use recovery::{
    salvage_manifest_path, try_load_recovery_manifest, FrameEvidence, HoleEvidence, LimitsSnapshot,
    RecoveryManifest, SalvageMode, SourceFileEvidence, SALVAGE_MANIFEST_FILE,
};
pub use recovery_shadow::{
    build_and_publish_mirror_shadow, build_and_publish_shadow, candidate_config_label,
    contains_plaintext, current_protection_lag, decode_mirror_to_struct, decode_segment_for_candidate,
    decode_shadow, delete_shadow, encode_mirror_shadow, encode_shadow, encode_shadow_from_live_map,
    enrich_segment_candidate, ensure_shadow_dirs, envelope_open, envelope_seal, evaluate_gates,
    every_protected_has_verified_rsh, is_mirror_magic, is_recovery_shadow_path,
    list_sealed_segment_files, load_protected_coverage, load_protected_frontier, median_f64,
    mirror_to_decoded_shadow, note_segment_sealed, ols_slope, project_live, protected_frontier_path,
    protection_lag, protection_lag_from_coverage, publish_mirror_shadow,
    publish_mirror_shadow_from_path, publish_mirror_shadow_timed, publish_protected_coverage,
    publish_protected_frontier, publish_shadow, publish_shadow_claiming_protection,
    publish_shadow_timed, range_f64, rebuild_coverage_from_shadows,
    recovery_after_auth_compact_delete, reset_shadow_reclaim_policy_for_tests,
    retire_shadows_after_replacement, retire_shadows_after_replacement_with_policy,
    secure_erase_shadow, set_shadow_reclaim_policy, shadow_dir, shadow_path, shadow_reclaim_policy,
    snapshot_telemetry, stage_medians, try_load_mirror, try_load_shadow, DecodedShadow, LiveMap,
    LiveState, MirrorPublishTiming, MirroredShadow, ProtectedCoverage, ProtectedFrontier,
    ProtectionLag, QualifyOptions, ShadowLoad, ShadowReclaimPolicy, ShadowRecord, ShadowStageSample,
    ShadowTelemetry, ShadowWriter, Step7CampaignReport, Step7Gates, ENVELOPE_MAGIC, FRONTIER_FILE,
    activate_compact_shadow_mode, backfill_shadows_for_sealed, decode_dual_mirror, is_dual_magic,
    load_recovery_mode, persist_recovery_mode, prepare_flip_to_compact_shadow,
    protected_frontier_gap_free, recovery_mode_path, rollback_to_materialized_mode,
    DualStreamFinalizeTiming, PreparedShadowPublish, RecoveryMode, ShadowDualStream,
    HARNESS_ENVELOPE_KEY, MIRROR_ENVELOPE_LEN, RECOVERY_MODE_FILE, RECOVERY_MODE_MAGIC, RSH_MAGIC,
    RSH_MAGIC_V1, RSH_MAGIC_V3, RSH_MAGIC_V4, TAG_PUT, TAG_TOMBSTONE, publish_prepared_shadow,
};
pub use scrub::{
    list_scrub_findings, load_or_init_scrub_state, load_scrub_findings, pause_scrub,
    plan_scrub_targets, resume_scrub, scrub_dir, scrub_findings_path, scrub_once, scrub_state_path,
    scrub_status, status_from_state, verify_scrub_target, write_scrub_findings, write_scrub_state,
    ScrubFinding, ScrubFindingKind, ScrubFindingsDoc, ScrubOptions, ScrubReport, ScrubState,
    ScrubStatus, ScrubTarget, ScrubTargetKind, ScrubTargetResult, DEFAULT_SCRUB_MAX_BYTES,
    DEFAULT_SCRUB_MAX_FILES, SCRUB_DIR, SCRUB_FINDINGS_FILE, SCRUB_PROFILE, SCRUB_QUARANTINE_DIR,
    SCRUB_STATE_FILE,
};
pub use seal_pipeline::{
    enrich_sealed_derived, finalize_seal, finalize_seal_authoritative, list_pending_paths,
    publish_sealed_from_summary_frame, recover_all_pending, EnrichmentStageTiming,
    EnrichmentStageTotals, SealPipeline, DEFAULT_MAX_PENDING_SEALS,
};
pub use secondary::{
    delete_secondary_index, list_secondary_index_paths, secondary_index_path,
    try_load_secondary_index, write_secondary_index, IndexState, SecondaryIndex,
    SecondaryIndexMeta, INDEX_LIFECYCLE_PROFILE,
};
pub use segment_catalog::{
    segment_catalog_path, SegmentCatalog, SegmentSummary, SEGMENT_CATALOG_FILE,
};
pub use segment_growth::{
    SegmentGrowthPolicy, WATERMARK_DEFAULT_CAPACITY_BYTES, WATERMARK_DEFAULT_CHUNK_BYTES,
};
pub use incremental_seal::ContentHashState;
pub use store::{
    subject_writer_shard, IncompleteReason, IndexBuildPage, IndexCacheDecision,
    IndexOpenDisposition, LiveIncomplete, LiveLogicalScan, OperationMutation,
    OperationMutationKind, OperationPut, OperationPutOutcome,
    RotationStageTotals, SalvageCopyReport, SalvageReport, SealStageBreakdown, StoreOpenMetrics,
    StoreOpenReport, WriteReceipt, MAX_WRITER_SHARDS,
};
/// Legacy unscoped store API. Prefer [`StoreHost`] / [`HeapStore`] on the
/// qualified heap path (`--no-default-features` hides this export).
#[cfg(feature = "legacy-raw-store")]
pub use store::Store;
pub use tier::{
    classify_segment_bytes, tier_placement_path, FormatClassification, MigrationEvidence,
    SegmentPlacement, TierAwareGet, TierClass, TierCoverage, TierMoveMode, TierPlacement,
    TIER_PLACEMENT_FILE,
};
pub use write_dedup::{
    append_write_dedup_batch, content_identity, write_dedup_journal_path, write_dedup_path,
    DedupRecord, WriteDedupTable, WRITE_DEDUP_FILE, WRITE_DEDUP_JOURNAL_FILE,
};
pub use writer_lock::{
    PidLiveness, StoreOpenOptions, WriterLock, WriterLockClass, WriterLockObservation,
    WRITER_LOCK_FILE,
};

/// Local-only staged genesis / publish used by `residiuum-authority` (HP-005).
/// MUST NOT be enabled in the qualified data-service target.
#[cfg(feature = "authority-provisioning")]
pub mod authority_provisioning {
    //! Re-exports catalog ceremony helpers for the authority tool.
    pub use crate::heap::{
        load_staged_genesis, publish_staged_genesis, stage_heap_genesis, HeapMetaLayout,
        StagedGenesis,
    };
}
