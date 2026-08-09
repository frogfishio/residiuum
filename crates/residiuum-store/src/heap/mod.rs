//! Capability-gated store façades (`HEAP_SPEC` §30.4–§30.5 / HP-003–HP-004).

mod admin_op_dedup;
mod catalog;
mod commit_coordinator;
mod heap_store;
mod host;
#[cfg(feature = "aws-kms")]
mod kms_aws;
mod lifecycle;
mod maintenance_store;
mod migration;
mod one_heap;
mod recovery_store;
mod replica_store;

pub use admin_op_dedup::{
    admin_op_binding, admin_op_dedup_path, collection_create_binding, create_collection_idempotent,
    load_admin_op_dedup, record_admin_op_dedup, resolve_admin_op_dedup, save_admin_op_dedup,
    AdminOpDedupRecord, AdminOpDedupTable, CreatedCollectionAdmin, ADMIN_OP_DEDUP_FILE,
    COLLECTION_CREATE_OP,
};
pub use catalog::{
    create_object, delete_rebuildable_catalogs, load_staged_genesis, publish_staged_genesis,
    rebuild_and_persist_all_catalogs, rebuild_heap_entry_from_chain, rebuild_object_entry_from_chain,
    rename_heap, rename_object, retire_heap, retire_object, stage_heap_genesis,
    staging_is_non_discoverable, try_load_collections_catalog, try_load_heap_catalog,
    try_load_streams_catalog, AdminReceipt, HeapCatalogEntry, HeapMetaLayout, ObjectCatalogEntry,
    ObjectKind, StagedGenesis, COLLECTIONS_CATALOG_FILE, HEAP_CATALOG_FILE, STREAMS_CATALOG_FILE,
};
pub use commit_coordinator::OperationCommitStats;
pub use heap_store::{
    CollectionScanHole, CollectionScanHoleReason, CollectionScanPage, HeapStore,
    VersionedCollectionScanPage, VersionedCollectionValue,
};
pub use host::StoreHost;
pub use maintenance_store::MaintenanceStore;
pub use lifecycle::{
    active_snapshot, build_backup_manifest, decode_purge_receipt,
    destroy_data_key, disaster_recovery_restore_retaining_id, encode_purge_receipt,
    heap_label_envelope, heap_object_media_dir, labelled_unit_readable, load_identity_tombstone,
    old_deployment_credential_invalid, refuse_access_from_payload_restore,
    refuse_clear_tombstone_via_payload_restore, refuse_retain_id_without_ceremony,
    restore_payload_to_new_heap, verify_purge_receipt, wipe_heap_object_media,
    write_identity_tombstone, DataKeyDestructionReceipt, DataKeyHandle, DataKeyProvider,
    DisasterRecoveryCeremony, DisasterRecoveryPackage, DisasterRecoveryTakeoverResult,
    HeapBackupManifest, HeapLifecycle, HeapRetentionPolicy, HsmBackendKind, HsmCapabilities,
    HsmDataKeyConfig, HsmDataKeyProvider, IdentityTombstone, InProcessDataKeyProvider,
    IncompletePurgeResult, MediaDomain, PayloadOnlyRestore, PurgeCoverageUnit, PurgePlan,
    PurgeReceipt, RetentionScheduler, TombstoneKind,
    BACKUP_MANIFEST_DOMAIN, DATA_KEY_DESTROY_DOMAIN, HEAP_LIFECYCLE_PROFILE,
    INCOMPLETE_PURGE_DOMAIN, LIFECYCLE_DIR, PURGE_COVERAGE_DOMAIN, RETENTION_POLICY_DOMAIN,
    TOMBSTONE_DOMAIN,
};
#[cfg(feature = "aws-kms")]
pub use kms_aws::{AwsKmsDataKeyProvider, SharedAwsKmsDataKeyProvider};
pub use migration::{
    CutoverGate, HeapMigrationJob, InventoryFrame, InventorySegment, MigrationPhase,
    MigrationStateV1, SourceInventory, ADMITTED_FILE, ASSIGNMENTS_FILE, ASSIGNMENTS_HASH_DOMAIN,
    HEAP_MIGRATE_DIR, HEAP_MIGRATE_PROFILE, INVENTORY_HASH_DOMAIN, STATE_FILE,
};
pub use one_heap::{
    classify_mixed_heap_frame, heap_binding_envelope, require_admit, MixedHeapSalvageClass,
};
pub use recovery_store::RecoveryStore;
pub use replica_store::ReplicaCapStore as ReplicaStore;
