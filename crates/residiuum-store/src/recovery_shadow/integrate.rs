//! Stage 2 step 5 — lifecycle integration hooks for Recovery Shadow.
//!
//! Dual-run beside Materialized Chimera. **No product flip** (step 8 only).
//!
//! Atomic publication invariant: protection is claimed only after
//! [`crate::atomic_file::write_atomic`] completes (temp write → file sync →
//! rename → parent-directory sync).

use super::frontier::{
    load_protected_coverage, protection_lag_from_coverage, publish_protected_coverage,
    ProtectedCoverage, ProtectionLag,
};
use super::policy::{shadow_reclaim_policy, ShadowReclaimPolicy};
use super::wire::{
    publish_shadow, shadow_path, try_load_shadow, ShadowLoad, ShadowRecord, ShadowWriter,
};
use crate::error::StoreError;
use crate::ids::segment_seq_from_id;
use crate::layout::StorePaths;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::Instant;

/// Telemetry for Shadow build rate, backlog, and honest protection lag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ShadowTelemetry {
    /// Shadows successfully published this window.
    pub shadows_built: u64,
    /// Bytes written for Shadows this window.
    pub bytes_written: u64,
    /// Nanoseconds spent constructing Shadows.
    pub construct_ns: u64,
    /// Nanoseconds spent publishing (atomic durable).
    pub persist_ns: u64,
    /// Sealed segments observed without durable Shadow (backlog count).
    pub backlog_segments: u64,
    /// Honest protection lag (aggregate sealed high − min protected prefix).
    pub protection_lag: u64,
    /// Aggregate protected prefix (min across shards).
    pub protected_frontier: u64,
    /// Aggregate sealed high.
    pub sealed_frontier: u64,
}

/// Build Shadow records from segment put/delete events (tombstones included).
#[allow(dead_code)] // public helper for callers / future seal paths
pub fn records_from_put_delete_pairs(
    puts_and_deletes: &[(Vec<u8>, Option<Vec<u8>>, u64)],
) -> Vec<ShadowRecord> {
    // (key, Some(value)=put / None=tombstone, gen)
    let mut out = Vec::with_capacity(puts_and_deletes.len());
    for (key, value, gen) in puts_and_deletes {
        match value {
            Some(v) => out.push(ShadowRecord::Put {
                key: key.clone(),
                gen: *gen,
                value: v.clone(),
            }),
            None => out.push(ShadowRecord::Tombstone {
                key: key.clone(),
                gen: *gen,
            }),
        }
    }
    out
}

/// Latest put/delete projection from a map of subject → (optional body, gen).
///
/// `None` body means tombstone at that generation.
fn records_from_live_map(
    live: &std::collections::BTreeMap<Vec<u8>, (Option<Vec<u8>>, u64)>,
) -> Vec<ShadowRecord> {
    live.iter()
        .map(|(key, (body, gen))| match body {
            Some(v) => ShadowRecord::Put {
                key: key.clone(),
                gen: *gen,
                value: v.clone(),
            },
            None => ShadowRecord::Tombstone {
                key: key.clone(),
                gen: *gen,
            },
        })
        .collect()
}

/// Publish a complete Shadow and **then** advance gap-aware coverage.
///
/// Order is normative for Stage 2a:
/// 1. Encode / verify self-integrity
/// 2. Atomic publish (sync → rename → dir sync)
/// 3. Only then `note_durable` + persist coverage
pub fn publish_shadow_claiming_protection(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: &[u8; 16],
    shard: u16,
    bytes: &[u8],
) -> Result<(), StoreError> {
    publish_shadow(paths, segment_id, bytes)?;
    let seq = segment_seq_from_id(segment_id);
    let mut cov = load_protected_coverage(paths, store_id)?;
    if cov.store_id != store_id && cov.store_id != [0u8; 16] {
        // empty() sets store_id; mismatch only if corrupt prior
        if cov.aggregate_sealed_high() > 0 || cov.aggregate_protected_prefix() > 0 {
            return Err(StoreError::CorruptMeta(
                "protected coverage store_id mismatch",
            ));
        }
        cov.store_id = store_id;
    } else {
        cov.store_id = store_id;
    }
    cov.note_durable(shard, seq);
    publish_protected_coverage(paths, &cov)?;
    Ok(())
}

/// Note seal without claiming P★ (protection lag / backlog honesty).
pub fn note_segment_sealed(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: &[u8; 16],
    shard: u16,
) -> Result<(), StoreError> {
    let seq = segment_seq_from_id(segment_id);
    let mut cov = load_protected_coverage(paths, store_id)?;
    cov.store_id = store_id;
    cov.note_sealed(shard, seq);
    publish_protected_coverage(paths, &cov)?;
    Ok(())
}

/// Build + publish Shadow from live put/tombstone map; claim protection.
///
/// Prefer [`build_and_publish_mirror_shadow`] (RSHD0003) for sealed segment
/// dual-run — record re-encode is retained for unit/CSE fixtures only.
pub fn build_and_publish_shadow(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: [u8; 16],
    generation: u64,
    shard: u16,
    live: &std::collections::BTreeMap<Vec<u8>, (Option<Vec<u8>>, u64)>,
) -> Result<ShadowTelemetry, StoreError> {
    let t0 = Instant::now();
    let mut writer = ShadowWriter::new(store_id, segment_id, generation);
    for rec in records_from_live_map(live) {
        match rec {
            ShadowRecord::Put { key, gen, value } => writer.push_put(key, gen, value),
            ShadowRecord::Tombstone { key, gen } => writer.push_tombstone(key, gen),
        }
    }
    let bytes = writer.finish();
    let construct_ns = t0.elapsed().as_nanos() as u64;
    let t1 = Instant::now();
    publish_shadow_claiming_protection(paths, store_id, &segment_id, shard, &bytes)?;
    let persist_ns = t1.elapsed().as_nanos() as u64;
    let lag = current_protection_lag(paths, store_id)?;
    Ok(ShadowTelemetry {
        shadows_built: 1,
        bytes_written: bytes.len() as u64,
        construct_ns,
        persist_ns,
        backlog_segments: lag.lag,
        protection_lag: lag.lag,
        protected_frontier: lag.protected_frontier,
        sealed_frontier: lag.sealed_frontier,
    })
}

/// Publish RSHD0003 canonical segment mirror and claim protection.
pub fn build_and_publish_mirror_shadow(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: [u8; 16],
    shard: u16,
    segment_image: &[u8],
) -> Result<ShadowTelemetry, StoreError> {
    let t0 = Instant::now();
    let timing =
        super::mirror::publish_mirror_shadow_timed(paths, store_id, &segment_id, segment_image)?;
    let construct_ns = t0.elapsed().as_nanos() as u64;
    // Protection only after atomic mirror publish returns.
    let seq = segment_seq_from_id(&segment_id);
    let mut cov = load_protected_coverage(paths, store_id)?;
    cov.store_id = store_id;
    cov.note_durable(shard, seq);
    publish_protected_coverage(paths, &cov)?;
    let lag = protection_lag_from_coverage(&cov);
    Ok(ShadowTelemetry {
        shadows_built: 1,
        bytes_written: timing.bytes_written,
        construct_ns: construct_ns
            .saturating_sub(timing.file_sync_ns + timing.rename_ns + timing.dir_sync_ns),
        persist_ns: timing.file_sync_ns + timing.rename_ns + timing.dir_sync_ns,
        backlog_segments: lag.lag,
        protection_lag: lag.lag,
        protected_frontier: lag.protected_frontier,
        sealed_frontier: lag.sealed_frontier,
    })
}

/// Current honest protection lag from durable coverage.
pub fn current_protection_lag(
    paths: &StorePaths,
    store_id: [u8; 16],
) -> Result<ProtectionLag, StoreError> {
    let cov = load_protected_coverage(paths, store_id)?;
    Ok(protection_lag_from_coverage(&cov))
}

/// Delete Shadow file and withdraw P★ coverage (authoritative ops stay intact).
pub fn delete_shadow(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: &[u8; 16],
    shard: u16,
) -> Result<(), StoreError> {
    let path = shadow_path(paths, segment_id);
    if path.is_file() {
        fs::remove_file(&path)?;
    }
    let seq = segment_seq_from_id(segment_id);
    let mut cov = load_protected_coverage(paths, store_id)?;
    cov.store_id = store_id;
    cov.withdraw_durable(shard, seq);
    publish_protected_coverage(paths, &cov)?;
    Ok(())
}

/// Cryptographically erase Shadow payload (overwrite then unlink).
///
/// Retention / secure-delete must not leave plaintext only in `.rsh`.
pub fn secure_erase_shadow(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: &[u8; 16],
    shard: u16,
) -> Result<(), StoreError> {
    let path = shadow_path(paths, segment_id);
    if path.is_file() {
        overwrite_zeros(&path)?;
        fs::remove_file(&path)?;
        if let Some(parent) = path.parent() {
            let _ = crate::atomic_file::sync_dir(parent);
        }
    }
    let seq = segment_seq_from_id(segment_id);
    let mut cov = load_protected_coverage(paths, store_id)?;
    cov.store_id = store_id;
    cov.withdraw_durable(shard, seq);
    publish_protected_coverage(paths, &cov)?;
    Ok(())
}

/// Compaction retirement under the current [`ShadowReclaimPolicy`].
///
/// - **Dual-run:** Materialized may satisfy recovery; missing replacement
///   Shadow does not block erase of old Shadows.
/// - **Post-flip:** replacement Shadow **must** be durable; never retire the
///   last valid recovery source without it (“when present” is insufficient).
pub fn retire_shadows_after_replacement(
    paths: &StorePaths,
    store_id: [u8; 16],
    replacement_segment_id: &[u8; 16],
    old_segment_ids: &[[u8; 16]],
    shard: u16,
) -> Result<(), StoreError> {
    retire_shadows_after_replacement_with_policy(
        paths,
        store_id,
        replacement_segment_id,
        old_segment_ids,
        shard,
        shadow_reclaim_policy(),
    )
}

/// Compaction retirement with an explicit policy (CSE / flip tests).
pub fn retire_shadows_after_replacement_with_policy(
    paths: &StorePaths,
    store_id: [u8; 16],
    replacement_segment_id: &[u8; 16],
    old_segment_ids: &[[u8; 16]],
    shard: u16,
    policy: ShadowReclaimPolicy,
) -> Result<(), StoreError> {
    let repl = shadow_path(paths, replacement_segment_id);
    let replacement_ok = matches!(try_load_shadow(&repl, Some(store_id))?, ShadowLoad::Ok(_));
    match policy {
        ShadowReclaimPolicy::RequireReplacementShadow => {
            if !replacement_ok {
                return Err(StoreError::ConsistencyViolation(
                    "post-flip reclaim requires durable replacement Shadow; cannot retire last valid recovery source"
                        .into(),
                ));
            }
        }
        ShadowReclaimPolicy::DualRunMaterializedAuthority => {
            // Materialized may satisfy recovery authority during dual-run.
            // Still erase old Shadows for retention honesty when reclaiming.
        }
    }
    for id in old_segment_ids {
        if id == replacement_segment_id {
            continue;
        }
        secure_erase_shadow(paths, store_id, id, shard)?;
    }
    Ok(())
}

/// Whether path is recovery-authoritative Shadow media (backup/scrub/encryption).
pub fn is_recovery_shadow_path(path: &Path) -> bool {
    path.extension().and_then(|e| e.to_str()) == Some("rsh")
        || path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.ends_with(".rsh"))
}

fn overwrite_zeros(path: &Path) -> Result<(), StoreError> {
    let len = fs::metadata(path)?.len() as usize;
    let mut f = OpenOptions::new().write(true).open(path)?;
    let chunk = vec![0u8; 65536.min(len.max(1))];
    let mut remaining = len;
    while remaining > 0 {
        let n = remaining.min(chunk.len());
        f.write_all(&chunk[..n])?;
        remaining -= n;
    }
    f.sync_all()?;
    Ok(())
}

/// Snapshot telemetry from current coverage (backlog ≈ lag in seq units).
pub fn snapshot_telemetry(
    paths: &StorePaths,
    store_id: [u8; 16],
) -> Result<ShadowTelemetry, StoreError> {
    let lag = current_protection_lag(paths, store_id)?;
    Ok(ShadowTelemetry {
        backlog_segments: lag.lag,
        protection_lag: lag.lag,
        protected_frontier: lag.protected_frontier,
        sealed_frontier: lag.sealed_frontier,
        ..ShadowTelemetry::default()
    })
}

/// Rebuild coverage durable set by scanning verified `.rsh` files (operator repair).
pub fn rebuild_coverage_from_shadows(
    paths: &StorePaths,
    store_id: [u8; 16],
    shard: u16,
) -> Result<ProtectedCoverage, StoreError> {
    let mut cov = ProtectedCoverage::empty(store_id);
    let dir = super::wire::shadow_dir(paths);
    if !dir.is_dir() {
        return Ok(cov);
    }
    for ent in fs::read_dir(&dir)? {
        let ent = ent?;
        let path = ent.path();
        if !is_recovery_shadow_path(&path) {
            continue;
        }
        let name = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        let Some(seg) = crate::layout::unhex16(name) else {
            continue;
        };
        match try_load_shadow(&path, Some(store_id))? {
            ShadowLoad::Ok(d) => {
                let seq = segment_seq_from_id(&d.segment_id);
                if d.segment_id != seg {
                    continue;
                }
                cov.note_durable(shard, seq);
            }
            _ => continue,
        }
    }
    publish_protected_coverage(paths, &cov)?;
    Ok(cov)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn seg(seq: u64) -> [u8; 16] {
        crate::ids::mint_sortable_segment_id(seq, &[9u8; 16])
    }

    #[test]
    fn publish_claims_only_after_atomic() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store = [9u8; 16];
        let segment = seg(1);
        let mut live = std::collections::BTreeMap::new();
        live.insert(b"k".to_vec(), (Some(b"v".to_vec()), 1));
        build_and_publish_shadow(&paths, store, segment, 1, 0, &live).unwrap();
        let lag = current_protection_lag(&paths, store).unwrap();
        assert_eq!(lag.protected_frontier, 1);
        assert!(shadow_path(&paths, &segment).is_file());
    }

    #[test]
    fn retire_refuses_without_replacement() {
        use super::super::policy::{
            reset_shadow_reclaim_policy_for_tests, set_shadow_reclaim_policy, ShadowReclaimPolicy,
        };
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store = [9u8; 16];
        let old = seg(1);
        let repl = seg(2);
        set_shadow_reclaim_policy(ShadowReclaimPolicy::RequireReplacementShadow);
        let err = retire_shadows_after_replacement(&paths, store, &repl, &[old], 0).unwrap_err();
        assert!(matches!(err, StoreError::ConsistencyViolation(_)));
        reset_shadow_reclaim_policy_for_tests();
    }

    #[test]
    fn dual_run_allows_retire_without_replacement_shadow() {
        use super::super::policy::{
            reset_shadow_reclaim_policy_for_tests, set_shadow_reclaim_policy, ShadowReclaimPolicy,
        };
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store = [9u8; 16];
        let old = seg(1);
        let repl = seg(2);
        // Seed an old Shadow then reclaim without replacement under dual-run.
        let mut live = std::collections::BTreeMap::new();
        live.insert(b"k".to_vec(), (Some(b"v".to_vec()), 1));
        build_and_publish_shadow(&paths, store, old, 1, 0, &live).unwrap();
        set_shadow_reclaim_policy(ShadowReclaimPolicy::DualRunMaterializedAuthority);
        retire_shadows_after_replacement(&paths, store, &repl, &[old], 0).unwrap();
        assert!(!shadow_path(&paths, &old).is_file());
        reset_shadow_reclaim_policy_for_tests();
    }
}
