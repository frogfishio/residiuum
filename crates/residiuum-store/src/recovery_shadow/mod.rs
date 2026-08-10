//! Recovery Shadow (`.rsh`) — P★ salvage artifact (CSE-3 Hybrid Stage 2).
//!
//! **Not** derived / disposable Chimera. Loss of Shadow for segment \(S\)
//! withdraws P★ for that coverage until rebuilt. Product sealing remains on
//! Materialized Chimera until Stage 2 delivery step 8.
//!
//! ## Stage 2a invariants (foundation accept)
//!
//! 1. **Atomic publication:** tmp write → file `sync_all` → rename → parent
//!    directory sync ([`crate::atomic_file`]) before protection is claimed.
//! 2. **Self-verifying:** each `.rsh` binds store/segment identity, magic
//!    version, record boundaries/count, per-record hashes, whole-artifact hash.
//! 3. **Gap-aware frontier:** protected prefix is downward closed over sealed
//!    order — completing seq 12 cannot conceal missing seq 11.
//! 4. **Multi-shard:** per-shard coverage; aggregate claim is **min** prefix,
//!    never a single max scalar that overstates protection.
//!
//! Normative: `CSE3_STAGE1_HYBRID_RECOVERY_SHADOW.md`,
//! `CSE3_STAGE2_RECOVERY_SHADOW_IMPLEMENT.md`.

mod crypto;
mod dual_stream;
mod frontier;
mod integrate;
mod mirror;
mod policy;
pub mod qualify;
mod recovery_mode;
mod stager;
mod wire;

pub use crypto::{contains_plaintext, envelope_open, envelope_seal, ENVELOPE_MAGIC};
pub use dual_stream::{
    decode_dual_mirror, is_dual_magic, publish_prepared_shadow, DualStreamFinalizeTiming,
    PreparedShadowPublish, ShadowDualStream, RSH_MAGIC_V4,
};
pub use frontier::{
    load_protected_coverage, load_protected_frontier, protection_lag, protection_lag_from_coverage,
    publish_protected_coverage, publish_protected_frontier, ProtectedCoverage, ProtectedFrontier,
    ProtectionLag, FRONTIER_FILE,
};
pub use integrate::{
    build_and_publish_mirror_shadow, build_and_publish_shadow, current_protection_lag,
    delete_shadow, is_recovery_shadow_path, note_segment_sealed,
    publish_shadow_claiming_protection, rebuild_coverage_from_shadows,
    retire_shadows_after_replacement, retire_shadows_after_replacement_with_policy,
    secure_erase_shadow, snapshot_telemetry, ShadowTelemetry,
};
pub use mirror::{
    decode_mirror_to_struct, encode_mirror_shadow, is_mirror_magic, mirror_to_decoded_shadow,
    publish_mirror_shadow, publish_mirror_shadow_from_path, publish_mirror_shadow_timed,
    try_load_mirror, MirrorPublishTiming, MirroredShadow, MIRROR_ENVELOPE_LEN, RSH_MAGIC_V3,
};
pub use policy::{
    reset_shadow_reclaim_policy_for_tests, set_shadow_reclaim_policy, shadow_reclaim_policy,
    ShadowReclaimPolicy,
};
pub use qualify::{
    candidate_config_label, decode_segment_for_candidate, enrich_segment_candidate, evaluate_gates,
    every_protected_has_verified_rsh, list_sealed_segment_files, median_f64, ols_slope,
    publish_shadow_timed, range_f64, recovery_after_auth_compact_delete, stage_medians,
    QualifyOptions, ShadowStageSample, Step7CampaignReport, Step7Gates, HARNESS_ENVELOPE_KEY,
};
pub use recovery_mode::{
    activate_compact_shadow_mode, backfill_shadows_for_sealed, load_recovery_mode,
    persist_recovery_mode, prepare_flip_to_compact_shadow, protected_frontier_gap_free,
    recovery_mode_path, rollback_to_materialized_mode, RecoveryMode, RECOVERY_MODE_FILE,
    RECOVERY_MODE_MAGIC,
};
pub use stager::{ShadowStageHandle, ShadowStagePipeline};
pub use wire::{
    decode_shadow, encode_shadow, encode_shadow_from_live_map, project_live, publish_shadow,
    shadow_dir, shadow_path, try_load_shadow, DecodedShadow, LiveMap, LiveState, ShadowLoad,
    ShadowRecord, ShadowWriter, RSH_MAGIC, RSH_MAGIC_V1, TAG_PUT, TAG_TOMBSTONE,
};

use crate::layout::StorePaths;
use std::path::PathBuf;

/// Ensure `recovery/shadow/` exists under the store root.
pub fn ensure_shadow_dirs(paths: &StorePaths) -> std::io::Result<()> {
    std::fs::create_dir_all(shadow_dir(paths))
}

/// Absolute path of the protected-frontier control document.
pub fn protected_frontier_path(paths: &StorePaths) -> PathBuf {
    frontier::frontier_path(paths)
}
