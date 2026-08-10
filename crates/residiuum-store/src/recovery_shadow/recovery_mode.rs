//! Stage 2 step 8 — durable recovery-mode marker (controlled product flip).
//!
//! ```text
//! Fresh Store::create → CompactShadow (CSE-3 Stage 2k product default)
//! Legacy tree (no marker) → Materialized on open (no silent flip)
//! Migration: Materialized → Transitioning → CompactShadow
//! Rollback → Materialized (keeps both recovery sources)
//! ```
//!
//! The marker is an atomic control document. Existing stores are never flipped
//! by open alone — operators must run prepare/coverage then activate.

use crate::atomic_file;
use crate::error::StoreError;
use crate::layout::{hex16, segment_id_from_filename, StorePaths};
use crate::recovery_shadow::frontier::{load_protected_coverage, publish_protected_coverage};
use crate::recovery_shadow::mirror::publish_mirror_shadow;
use crate::recovery_shadow::policy::{set_shadow_reclaim_policy, ShadowReclaimPolicy};
use crate::recovery_shadow::qualify::{
    every_protected_has_verified_rsh, list_sealed_segment_files,
};
use crate::recovery_shadow::wire::{shadow_path, try_load_shadow, ShadowLoad};
use std::fs;
use std::path::PathBuf;

/// On-disk magic for the recovery-mode control document.
pub const RECOVERY_MODE_MAGIC: &str = "RMODE001";

/// Relative path under the store root.
pub const RECOVERY_MODE_FILE: &str = "recovery/mode.v1";

/// Product recovery authority mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMode {
    /// Dual-run: Materialized is product recovery; Shadow is additive.
    Materialized,
    /// Flip in progress: backfill Shadows; still create Materialized.
    Transitioning,
    /// Post-flip: Compact + Recovery Shadow are product recovery.
    CompactShadow,
}

impl RecoveryMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Materialized => "materialized",
            Self::Transitioning => "transitioning",
            Self::CompactShadow => "compact_shadow",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "materialized" => Some(Self::Materialized),
            "transitioning" => Some(Self::Transitioning),
            "compact_shadow" => Some(Self::CompactShadow),
            _ => None,
        }
    }

    /// True when new seals should omit Materialized Chimera creation.
    pub fn omits_new_materialized(self) -> bool {
        matches!(self, Self::CompactShadow)
    }

    /// True when dual-stream Shadow should be product-armed.
    pub fn requires_dual_stream(self) -> bool {
        matches!(self, Self::Transitioning | Self::CompactShadow)
    }
}

/// Absolute path of the recovery-mode marker.
pub fn recovery_mode_path(paths: &StorePaths) -> PathBuf {
    paths.root.join(RECOVERY_MODE_FILE)
}

/// Load recovery mode (missing file ⇒ **legacy Materialized** — never silent-flip).
///
/// Fresh [`crate::Store::create`] writes an explicit CompactShadow marker.
/// Trees created before Stage 2k have no marker and stay Materialized until
/// the operator runs prepare → activate.
pub fn load_recovery_mode(paths: &StorePaths) -> Result<RecoveryMode, StoreError> {
    let path = recovery_mode_path(paths);
    if !path.is_file() {
        return Ok(RecoveryMode::Materialized);
    }
    let text = fs::read_to_string(&path)?;
    let mut magic = None;
    let mut mode = None;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("magic=") {
            magic = Some(rest.trim());
        } else if let Some(rest) = line.strip_prefix("mode=") {
            mode = RecoveryMode::parse(rest);
        }
    }
    if magic != Some(RECOVERY_MODE_MAGIC) {
        return Err(StoreError::CorruptMeta(
            "recovery mode marker magic mismatch",
        ));
    }
    mode.ok_or(StoreError::CorruptMeta(
        "recovery mode marker missing mode=",
    ))
}

/// Atomically persist recovery mode (sync_all → rename → dir sync).
pub fn persist_recovery_mode(
    paths: &StorePaths,
    store_id: [u8; 16],
    mode: RecoveryMode,
) -> Result<(), StoreError> {
    crate::failpoint::hit("rshd4.mode.before_persist")?;
    let path = recovery_mode_path(paths);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let body = format!(
        "magic={}\nstore_id={}\nmode={}\n",
        RECOVERY_MODE_MAGIC,
        hex16(&store_id),
        mode.as_str()
    );
    atomic_file::write_atomic(&path, body.as_bytes())?;
    crate::failpoint::hit("rshd4.mode.after_persist")?;
    Ok(())
}

/// Resolve writer-shard for a segment seq from existing coverage (fallback `0`).
fn shard_for_seq(cov: &crate::recovery_shadow::ProtectedCoverage, seq: u64) -> u16 {
    for (&sh, set) in &cov.sealed_by_shard {
        if set.contains(&seq) {
            return sh;
        }
    }
    for (&sh, set) in &cov.durable_by_shard {
        if set.contains(&seq) {
            return sh;
        }
    }
    0
}

/// Backfill RSHD0003 mirrors for every sealed segment lacking a verified Shadow.
///
/// Used during flip prepare. Independent physical copy (no reflink).
/// Shard attribution prefers prior coverage; unknown seqs land on shard 0.
pub fn backfill_shadows_for_sealed(
    paths: &StorePaths,
    store_id: [u8; 16],
    _shard_hint: u16,
) -> Result<u64, StoreError> {
    let mut built = 0u64;
    for path in list_sealed_segment_files(paths)? {
        let Some(segment_id) = segment_id_from_filename(&path) else {
            continue;
        };
        let seq = crate::ids::segment_seq_from_id(&segment_id);
        let cov = load_protected_coverage(paths, store_id)?;
        let shard = shard_for_seq(&cov, seq);
        let rsh = shadow_path(paths, &segment_id);
        let need = match try_load_shadow(&rsh, Some(store_id))? {
            ShadowLoad::Ok(_) => false,
            _ => true,
        };
        if !need {
            let mut cov = cov;
            cov.store_id = store_id;
            cov.note_durable(shard, seq);
            publish_protected_coverage(paths, &cov)?;
            continue;
        }
        let bytes = fs::read(&path)?;
        if bytes.is_empty() {
            continue;
        }
        publish_mirror_shadow(paths, store_id, &segment_id, &bytes)?;
        let mut cov = load_protected_coverage(paths, store_id)?;
        cov.store_id = store_id;
        cov.note_durable(shard, seq);
        publish_protected_coverage(paths, &cov)?;
        built = built.saturating_add(1);
    }
    Ok(built)
}

/// True when every shard's sealed set is fully Shadow-durable (no holes).
///
/// Aggregate `max(sealed) - min(prefix)` lag is **not** used: interleaved
/// global segment sequences across writer shards make that scalar non-zero
/// even when each shard is fully protected.
pub fn protected_frontier_gap_free(
    paths: &StorePaths,
    store_id: [u8; 16],
) -> Result<bool, StoreError> {
    let cov = load_protected_coverage(paths, store_id)?;
    let shards: std::collections::BTreeSet<u16> = cov
        .sealed_by_shard
        .keys()
        .chain(cov.durable_by_shard.keys())
        .copied()
        .collect();
    for sh in shards {
        if cov.protected_prefix(sh) != cov.sealed_high(sh) {
            return Ok(false);
        }
        if let Some(sealed) = cov.sealed_by_shard.get(&sh) {
            let durable = cov.durable_by_shard.get(&sh);
            for &seq in sealed {
                if !durable.map(|d| d.contains(&seq)).unwrap_or(false) {
                    return Ok(false);
                }
            }
        }
    }
    every_protected_has_verified_rsh(paths, store_id)
}

/// Flip prepare: Transitioning marker + backfill + gap-free check.
///
/// Materialized creation continues until [`activate_compact_shadow_mode`].
pub fn prepare_flip_to_compact_shadow(
    paths: &StorePaths,
    store_id: [u8; 16],
    shard: u16,
) -> Result<u64, StoreError> {
    persist_recovery_mode(paths, store_id, RecoveryMode::Transitioning)?;
    let built = backfill_shadows_for_sealed(paths, store_id, shard)?;
    if !protected_frontier_gap_free(paths, store_id)? {
        return Err(StoreError::CorruptMeta(
            "flip prepare: protected frontier not gap-free after backfill",
        ));
    }
    Ok(built)
}

/// Activate Compact+Shadow product mode (marker durable first).
///
/// Sets reclaim policy to require replacement Shadows. Does **not** delete
/// existing Materialized `.cmr` files.
pub fn activate_compact_shadow_mode(
    paths: &StorePaths,
    store_id: [u8; 16],
) -> Result<(), StoreError> {
    if !protected_frontier_gap_free(paths, store_id)? {
        return Err(StoreError::CorruptMeta(
            "flip activate: protected frontier not gap-free",
        ));
    }
    // Marker first — only then may callers stop Materialized creation.
    persist_recovery_mode(paths, store_id, RecoveryMode::CompactShadow)?;
    set_shadow_reclaim_policy(ShadowReclaimPolicy::RequireReplacementShadow);
    Ok(())
}

/// Rollback to Materialized dual-run without deleting Shadows or Materialized.
pub fn rollback_to_materialized_mode(
    paths: &StorePaths,
    store_id: [u8; 16],
) -> Result<(), StoreError> {
    persist_recovery_mode(paths, store_id, RecoveryMode::Materialized)?;
    set_shadow_reclaim_policy(ShadowReclaimPolicy::DualRunMaterializedAuthority);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn mode_roundtrip_atomic() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        assert_eq!(
            load_recovery_mode(&paths).unwrap(),
            RecoveryMode::Materialized
        );
        persist_recovery_mode(&paths, [1u8; 16], RecoveryMode::Transitioning).unwrap();
        assert_eq!(
            load_recovery_mode(&paths).unwrap(),
            RecoveryMode::Transitioning
        );
        persist_recovery_mode(&paths, [1u8; 16], RecoveryMode::CompactShadow).unwrap();
        assert!(load_recovery_mode(&paths).unwrap().omits_new_materialized());
        rollback_to_materialized_mode(&paths, [1u8; 16]).unwrap();
        assert_eq!(
            load_recovery_mode(&paths).unwrap(),
            RecoveryMode::Materialized
        );
    }
}
