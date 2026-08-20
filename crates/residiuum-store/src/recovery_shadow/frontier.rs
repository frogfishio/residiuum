//! Gap-aware, per-shard `protected_frontier` (Stage 2a invariants).
//!
//! Formal: protected prefix is **downward closed** over sealed order:
//!
//! ```text
//! s ∈ ProtectedFrontier  ⇒  ∀ p ≼ s, p ∈ DurableShadow
//! ```
//!
//! Completing segment/seq 12 must **not** claim protection while 11 is missing.
//! Multi-shard: one scalar must **never** overstate — aggregate claim is the
//! **minimum** per-shard prefix (or an explicit coverage set), never the max.

use super::wire::shadow_dir;
use crate::atomic_file;
use crate::error::StoreError;
use crate::layout::StorePaths;
use blake3::Hasher;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

/// Frontier control-document filename under `recovery/shadow/`.
pub const FRONTIER_FILE: &str = "protected_frontier";

/// Wire magic for gap-aware coverage (supersedes scalar `RSHPF001`).
const MAGIC_V2: &[u8; 8] = b"RSHPF002";
/// Legacy scalar frontier (migrated as contiguous 1..=n durable on shard 0).
const MAGIC_V1: &[u8; 8] = b"RSHPF001";

/// Durable Recovery Shadow coverage — gap-aware, per writer-shard.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProtectedCoverage {
    /// Owning store.
    pub store_id: [u8; 16],
    /// Per-shard durable seal sequences (sortable segment seq).
    pub durable_by_shard: BTreeMap<u16, BTreeSet<u64>>,
    /// Per-shard sealed sequences observed (universe for downward-closure).
    pub sealed_by_shard: BTreeMap<u16, BTreeSet<u64>>,
}

impl ProtectedCoverage {
    /// Empty coverage (no P★ claimed).
    pub fn empty(store_id: [u8; 16]) -> Self {
        Self {
            store_id,
            durable_by_shard: BTreeMap::new(),
            sealed_by_shard: BTreeMap::new(),
        }
    }

    /// Note a sealed segment sequence on `shard` (does not claim P★).
    pub fn note_sealed(&mut self, shard: u16, seq: u64) {
        self.sealed_by_shard.entry(shard).or_default().insert(seq);
    }

    /// Note a **durable verified** Shadow for `(shard, seq)`.
    ///
    /// Callers must only invoke after atomic publish completes (file sync →
    /// rename → parent dir sync).
    pub fn note_durable(&mut self, shard: u16, seq: u64) {
        self.sealed_by_shard.entry(shard).or_default().insert(seq);
        self.durable_by_shard.entry(shard).or_default().insert(seq);
    }

    /// Withdraw durable coverage (Shadow deleted / erased).
    pub fn withdraw_durable(&mut self, shard: u16, seq: u64) {
        if let Some(set) = self.durable_by_shard.get_mut(&shard) {
            set.remove(&seq);
        }
    }

    /// Retire a segment that no longer belongs to the authoritative sealed
    /// set after replacement compaction. Both sets change together only after
    /// the replacement Shadow is durable.
    pub fn retire_sealed(&mut self, shard: u16, seq: u64) {
        if let Some(set) = self.sealed_by_shard.get_mut(&shard) {
            set.remove(&seq);
        }
        if let Some(set) = self.durable_by_shard.get_mut(&shard) {
            set.remove(&seq);
        }
    }

    /// Downward-closed protected prefix for one shard.
    ///
    /// Walks sealed sequences in order; stops at the first sealed seq lacking
    /// durable Shadow. Completing a later seq cannot conceal an earlier hole.
    pub fn protected_prefix(&self, shard: u16) -> u64 {
        let sealed = self.sealed_by_shard.get(&shard);
        let durable = self.durable_by_shard.get(&shard);
        let (Some(sealed), Some(durable)) = (sealed, durable) else {
            return 0;
        };
        let mut best = 0u64;
        for &s in sealed {
            if durable.contains(&s) {
                best = s;
            } else {
                break;
            }
        }
        best
    }

    /// Highest sealed sequence observed on `shard` (may lead protected).
    pub fn sealed_high(&self, shard: u16) -> u64 {
        self.sealed_by_shard
            .get(&shard)
            .and_then(|s| s.iter().next_back().copied())
            .unwrap_or(0)
    }

    /// Aggregate protected claim across shards: **minimum** prefix.
    ///
    /// A single max scalar must never overstate multi-shard protection.
    pub fn aggregate_protected_prefix(&self) -> u64 {
        let shards: BTreeSet<u16> = self
            .sealed_by_shard
            .keys()
            .chain(self.durable_by_shard.keys())
            .copied()
            .collect();
        if shards.is_empty() {
            return 0;
        }
        shards
            .iter()
            .map(|&sh| self.protected_prefix(sh))
            .min()
            .unwrap_or(0)
    }

    /// Aggregate sealed high: **maximum** observed (honest lag numerator).
    pub fn aggregate_sealed_high(&self) -> u64 {
        self.sealed_by_shard
            .keys()
            .map(|&sh| self.sealed_high(sh))
            .max()
            .unwrap_or(0)
    }
}

/// Legacy scalar view (derived; never used alone for multi-shard claims).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtectedFrontier {
    /// Owning store.
    pub store_id: [u8; 16],
    /// Aggregate downward-closed prefix ([`ProtectedCoverage::aggregate_protected_prefix`]).
    pub protected_frontier: u64,
    /// Aggregate sealed high ([`ProtectedCoverage::aggregate_sealed_high`]).
    pub sealed_frontier: u64,
}

/// Observable lag between sealed and shadow-protected frontiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtectionLag {
    /// Sealed frontier (aggregate high).
    pub sealed_frontier: u64,
    /// Protected frontier (aggregate **min** prefix — never overstates).
    pub protected_frontier: u64,
    /// `sealed_frontier.saturating_sub(protected_frontier)`.
    pub lag: u64,
}

/// Path: `recovery/shadow/protected_frontier`.
pub fn frontier_path(paths: &StorePaths) -> PathBuf {
    shadow_dir(paths).join(FRONTIER_FILE)
}

/// Compute protection lag telemetry from coverage.
pub fn protection_lag_from_coverage(cov: &ProtectedCoverage) -> ProtectionLag {
    let sealed_frontier = cov.aggregate_sealed_high();
    let protected_frontier = cov.aggregate_protected_prefix();
    ProtectionLag {
        sealed_frontier,
        protected_frontier,
        lag: sealed_frontier.saturating_sub(protected_frontier),
    }
}

/// Compute protection lag from a derived scalar view.
pub fn protection_lag(frontier: &ProtectedFrontier) -> ProtectionLag {
    ProtectionLag {
        sealed_frontier: frontier.sealed_frontier,
        protected_frontier: frontier.protected_frontier,
        lag: frontier
            .sealed_frontier
            .saturating_sub(frontier.protected_frontier),
    }
}

/// Load coverage; missing → empty (no P★ claimed). Migrates legacy v1 scalars.
pub fn load_protected_coverage(
    paths: &StorePaths,
    expect_store: [u8; 16],
) -> Result<ProtectedCoverage, StoreError> {
    let path = frontier_path(paths);
    if !path.is_file() {
        return Ok(ProtectedCoverage::empty(expect_store));
    }
    let bytes = fs::read(&path)?;
    decode_coverage(&bytes, expect_store).ok_or(StoreError::CorruptControl {
        path: path.display().to_string(),
        detail: "protected_frontier decode failed".into(),
        recovery: "rebuild frontier from verified .rsh files; do not invent coverage".into(),
    })
}

/// Load derived scalar view (aggregate min prefix / max sealed).
pub fn load_protected_frontier(
    paths: &StorePaths,
    expect_store: [u8; 16],
) -> Result<ProtectedFrontier, StoreError> {
    let cov = load_protected_coverage(paths, expect_store)?;
    Ok(ProtectedFrontier {
        store_id: cov.store_id,
        protected_frontier: cov.aggregate_protected_prefix(),
        sealed_frontier: cov.aggregate_sealed_high(),
    })
}

/// Atomically publish gap-aware coverage.
pub fn publish_protected_coverage(
    paths: &StorePaths,
    cov: &ProtectedCoverage,
) -> Result<(), StoreError> {
    // Invariant: durable ⊆ sealed (note_durable maintains this; check anyway).
    for (shard, durable) in &cov.durable_by_shard {
        let sealed = cov.sealed_by_shard.get(shard);
        for seq in durable {
            if sealed.map(|s| s.contains(seq)) != Some(true) {
                return Err(StoreError::CorruptMeta(
                    "durable shadow seq missing from sealed universe",
                ));
            }
        }
    }
    fs::create_dir_all(shadow_dir(paths))?;
    let bytes = encode_coverage(cov);
    // Atomic: tmp sync → rename → parent dir sync before any caller may claim.
    atomic_file::write_atomic(&frontier_path(paths), &bytes)?;
    Ok(())
}

/// Publish derived scalar (builds contiguous coverage on shard 0 for tests only).
///
/// Prefer [`publish_protected_coverage`] for production paths.
pub fn publish_protected_frontier(
    paths: &StorePaths,
    frontier: &ProtectedFrontier,
) -> Result<(), StoreError> {
    if frontier.protected_frontier > frontier.sealed_frontier {
        return Err(StoreError::CorruptMeta(
            "protected_frontier must not exceed sealed_frontier",
        ));
    }
    let mut cov = ProtectedCoverage::empty(frontier.store_id);
    for seq in 1..=frontier.sealed_frontier {
        cov.note_sealed(0, seq);
        if seq <= frontier.protected_frontier {
            cov.note_durable(0, seq);
        }
    }
    publish_protected_coverage(paths, &cov)
}

fn encode_coverage(cov: &ProtectedCoverage) -> Vec<u8> {
    let shards: BTreeSet<u16> = cov
        .sealed_by_shard
        .keys()
        .chain(cov.durable_by_shard.keys())
        .copied()
        .collect();
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC_V2);
    out.extend_from_slice(&cov.store_id);
    out.extend_from_slice(&(shards.len() as u16).to_le_bytes());
    for shard in shards {
        out.extend_from_slice(&shard.to_le_bytes());
        let sealed = cov.sealed_by_shard.get(&shard).cloned().unwrap_or_default();
        let durable = cov
            .durable_by_shard
            .get(&shard)
            .cloned()
            .unwrap_or_default();
        out.extend_from_slice(&(sealed.len() as u32).to_le_bytes());
        for seq in &sealed {
            out.extend_from_slice(&seq.to_le_bytes());
        }
        out.extend_from_slice(&(durable.len() as u32).to_le_bytes());
        for seq in &durable {
            out.extend_from_slice(&seq.to_le_bytes());
        }
    }
    let mut h = Hasher::new();
    h.update(&out);
    out.extend_from_slice(h.finalize().as_bytes());
    out
}

fn decode_coverage(bytes: &[u8], expect_store: [u8; 16]) -> Option<ProtectedCoverage> {
    if bytes.len() < 8 + 16 + 32 {
        return None;
    }
    let magic = &bytes[0..8];
    if magic == MAGIC_V1 {
        return decode_legacy_v1(bytes, expect_store);
    }
    if magic != MAGIC_V2 {
        return None;
    }
    let store_id: [u8; 16] = bytes[8..24].try_into().ok()?;
    if store_id != expect_store {
        return None;
    }
    let n_shards = u16::from_le_bytes(bytes[24..26].try_into().ok()?) as usize;
    let body_end = bytes.len().checked_sub(32)?;
    let mut cursor = 26usize;
    let mut cov = ProtectedCoverage::empty(store_id);
    for _ in 0..n_shards {
        if cursor + 2 > body_end {
            return None;
        }
        let shard = u16::from_le_bytes(bytes[cursor..cursor + 2].try_into().ok()?);
        cursor += 2;
        if cursor + 4 > body_end {
            return None;
        }
        let n_sealed = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().ok()?) as usize;
        cursor += 4;
        for _ in 0..n_sealed {
            if cursor + 8 > body_end {
                return None;
            }
            let seq = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().ok()?);
            cursor += 8;
            cov.note_sealed(shard, seq);
        }
        if cursor + 4 > body_end {
            return None;
        }
        let n_durable = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().ok()?) as usize;
        cursor += 4;
        for _ in 0..n_durable {
            if cursor + 8 > body_end {
                return None;
            }
            let seq = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().ok()?);
            cursor += 8;
            cov.note_durable(shard, seq);
        }
    }
    if cursor != body_end {
        return None;
    }
    let mut h = Hasher::new();
    h.update(&bytes[..body_end]);
    let want = *h.finalize().as_bytes();
    let got: [u8; 32] = bytes[body_end..].try_into().ok()?;
    if got != want {
        return None;
    }
    Some(cov)
}

fn decode_legacy_v1(bytes: &[u8], expect_store: [u8; 16]) -> Option<ProtectedCoverage> {
    if bytes.len() != 8 + 16 + 8 + 8 + 32 {
        return None;
    }
    let store_id: [u8; 16] = bytes[8..24].try_into().ok()?;
    if store_id != expect_store {
        return None;
    }
    let protected_frontier = u64::from_le_bytes(bytes[24..32].try_into().ok()?);
    let sealed_frontier = u64::from_le_bytes(bytes[32..40].try_into().ok()?);
    if protected_frontier > sealed_frontier {
        return None;
    }
    let mut h = Hasher::new();
    h.update(&bytes[..40]);
    let want = *h.finalize().as_bytes();
    let got: [u8; 32] = bytes[40..72].try_into().ok()?;
    if got != want {
        return None;
    }
    // Interpret legacy scalar as contiguous sealed+durable on shard 0.
    let mut cov = ProtectedCoverage::empty(store_id);
    for seq in 1..=sealed_frontier {
        cov.note_sealed(0, seq);
        if seq <= protected_frontier {
            cov.note_durable(0, seq);
        }
    }
    Some(cov)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sid() -> [u8; 16] {
        [7u8; 16]
    }

    #[test]
    fn gap_aware_twelve_does_not_conceal_eleven() {
        let mut cov = ProtectedCoverage::empty(sid());
        for seq in [1u64, 2, 3, 11, 12] {
            cov.note_sealed(0, seq);
        }
        for seq in [1u64, 2, 3, 12] {
            cov.note_durable(0, seq);
        }
        // 11 sealed but not durable → prefix stops at 3, even though 12 is durable.
        assert_eq!(cov.protected_prefix(0), 3);
        assert_eq!(cov.aggregate_protected_prefix(), 3);
    }

    #[test]
    fn multi_shard_aggregate_is_min_not_max() {
        let mut cov = ProtectedCoverage::empty(sid());
        for seq in 1..=5u64 {
            cov.note_sealed(0, seq);
            cov.note_durable(0, seq);
        }
        for seq in 1..=5u64 {
            cov.note_sealed(1, seq);
        }
        for seq in 1..=2u64 {
            cov.note_durable(1, seq);
        }
        assert_eq!(cov.protected_prefix(0), 5);
        assert_eq!(cov.protected_prefix(1), 2);
        assert_eq!(cov.aggregate_protected_prefix(), 2);
        let lag = protection_lag_from_coverage(&cov);
        assert_eq!(lag.protected_frontier, 2);
        assert_eq!(lag.sealed_frontier, 5);
        assert_eq!(lag.lag, 3);
    }

    #[test]
    fn coverage_roundtrip() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let mut cov = ProtectedCoverage::empty(sid());
        cov.note_sealed(0, 1);
        cov.note_durable(0, 1);
        cov.note_sealed(0, 2);
        publish_protected_coverage(&paths, &cov).unwrap();
        let loaded = load_protected_coverage(&paths, sid()).unwrap();
        assert_eq!(loaded.protected_prefix(0), 1);
        assert_eq!(loaded.sealed_high(0), 2);
    }

    #[test]
    fn refuses_protected_ahead_of_sealed_scalar_api() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let f = ProtectedFrontier {
            store_id: sid(),
            protected_frontier: 9,
            sealed_frontier: 3,
        };
        assert!(publish_protected_frontier(&paths, &f).is_err());
    }
}
