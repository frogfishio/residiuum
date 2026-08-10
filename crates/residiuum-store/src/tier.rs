//! Storage tiers and segment placement (OVERVIEW §9, Stage 9).
//!
//! Segments keep stable identities when moved or copied between hot, warm,
//! cold, and archive media. Placement catalogs are derived: loss of the
//! catalog must not erase segment bytes; rebuild rescans known media roots.
//!
//! Offline or unmounted tiers create **coverage holes**, never silent absence
//! (OVERVIEW §9.2, CLUSTER_SPEC §18).

use crate::error::StoreError;
use crate::incremental_seal::ContentHashState;
use crate::layout::{hex16, list_residiuum_files, segment_id_from_filename, StorePaths};
use blake3::Hasher;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Filename of the derived tier placement catalog under `catalogs/`.
pub const TIER_PLACEMENT_FILE: &str = "tier-placement.cat";

/// Filename of tier availability / external roots under `tiers/`.
pub const TIER_ROOTS_FILE: &str = "roots.txt";

const PLACEMENT_MAGIC: &[u8; 8] = b"RTIER001";
/// v2: `content_hash` is tagged [`ContentHashState`] (Pending | Known).
const PLACEMENT_VERSION: u32 = 2;

/// Storage performance / retention class (OVERVIEW §9.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum TierClass {
    /// Memory-resident indexes and active working data / local sealed hot.
    Hot = 0,
    /// Locally or remotely available sealed segments.
    Warm = 1,
    /// Low-cost object-style storage (high latency, still online).
    Cold = 2,
    /// High-latency or offline multi-decade retention.
    Archive = 3,
}

impl TierClass {
    /// Stable ASCII name for catalogs and coverage reports.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hot => "hot",
            Self::Warm => "warm",
            Self::Cold => "cold",
            Self::Archive => "archive",
        }
    }

    /// Parse a tier name (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "hot" => Some(Self::Hot),
            "warm" => Some(Self::Warm),
            "cold" => Some(Self::Cold),
            "archive" => Some(Self::Archive),
            _ => None,
        }
    }

    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Hot),
            1 => Some(Self::Warm),
            2 => Some(Self::Cold),
            3 => Some(Self::Archive),
            _ => None,
        }
    }

    /// Whether this class is treated as archive-path (not hot-path SLOs).
    pub fn is_archive_path(self) -> bool {
        matches!(self, Self::Cold | Self::Archive)
    }
}

impl std::fmt::Display for TierClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether a tier move leaves a copy on the source media.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TierMoveMode {
    /// Copy bytes to destination; leave source in place (dual residency).
    Copy,
    /// Copy then remove the source file after verified write (single residency).
    Move,
}

/// One segment's known physical placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentPlacement {
    /// Stable segment identity (unchanged by tier migration).
    pub segment_id: [u8; 16],
    /// Current primary tier class for this segment.
    pub tier: TierClass,
    /// Path relative to the store root when under the store tree, or absolute
    /// when using an external media root.
    pub relative_path: String,
    /// Derived whole-segment BLAKE3 at last placement update.
    pub content_hash: ContentHashState,
    /// File size in bytes at last placement update.
    pub size: u64,
    /// Whether the segment's media is presently readable.
    pub available: bool,
}

/// Evidence recorded when a segment is copied or moved between tiers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationEvidence {
    /// Segment identity (stable).
    pub segment_id: [u8; 16],
    /// Source tier.
    pub from_tier: TierClass,
    /// Destination tier.
    pub to_tier: TierClass,
    /// Move vs copy.
    pub mode: TierMoveMode,
    /// Source content hash before transfer.
    pub source_hash: [u8; 32],
    /// Destination content hash after transfer (must equal source).
    pub dest_hash: [u8; 32],
    /// Bytes transferred.
    pub size: u64,
    /// Tool / format tag for multi-year readability (OVERVIEW §9.5).
    pub tool_version: String,
    /// Wall-clock nanoseconds since UNIX epoch (diagnostic).
    pub migrated_ns: u64,
}

/// Coverage of storage tiers for a query or open (never silent empty success).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TierCoverage {
    /// Tiers that were searched and available.
    pub searched: Vec<TierClass>,
    /// Tiers deliberately excluded by the query profile.
    pub excluded: Vec<TierClass>,
    /// Tiers known but offline / unmounted.
    pub offline: Vec<TierClass>,
    /// Segment ids known to exist but not readable (offline media).
    pub unavailable_segments: Vec<[u8; 16]>,
    /// Free-form notes (e.g. "archive path: no hot-path latency claim").
    pub notes: Vec<String>,
}

impl TierCoverage {
    /// True when no offline tiers and no unavailable segments.
    pub fn is_complete(&self) -> bool {
        self.offline.is_empty() && self.unavailable_segments.is_empty()
    }

    /// True when cold/archive material may be missing from the result set.
    pub fn is_incomplete(&self) -> bool {
        !self.is_complete()
    }

    /// Attach a note.
    pub fn note(&mut self, msg: impl Into<String>) {
        self.notes.push(msg.into());
    }
}

/// Result of a tier-aware read that may be incomplete due to offline media.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierAwareGet {
    /// Live body when present in available tiers.
    pub value: Option<Vec<u8>>,
    /// Tier coverage for this lookup / open state.
    pub coverage: TierCoverage,
    /// When `value` is `None` and coverage is incomplete, absence is **not** proven.
    pub absence_proven: bool,
}

/// In-memory placement map (derived).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TierPlacement {
    /// segment_id → placement
    entries: BTreeMap<[u8; 16], SegmentPlacement>,
    /// Per-tier availability (default true for configured roots).
    tier_available: BTreeMap<TierClass, bool>,
    /// Optional external absolute roots overriding default under-store paths.
    external_roots: BTreeMap<TierClass, PathBuf>,
}

impl TierPlacement {
    /// Empty placement (hot only until segments are discovered).
    pub fn new() -> Self {
        let mut tier_available = BTreeMap::new();
        for t in [
            TierClass::Hot,
            TierClass::Warm,
            TierClass::Cold,
            TierClass::Archive,
        ] {
            tier_available.insert(t, true);
        }
        Self {
            entries: BTreeMap::new(),
            tier_available,
            external_roots: BTreeMap::new(),
        }
    }

    /// All placements in segment-id order.
    pub fn entries(&self) -> impl Iterator<Item = &SegmentPlacement> {
        self.entries.values()
    }

    /// Lookup one segment.
    pub fn get(&self, segment_id: &[u8; 16]) -> Option<&SegmentPlacement> {
        self.entries.get(segment_id)
    }

    /// Number of tracked segments.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether a tier class is considered mounted/available.
    pub fn is_tier_available(&self, tier: TierClass) -> bool {
        self.tier_available.get(&tier).copied().unwrap_or(true)
    }

    /// Mark a tier online or offline (does not delete media).
    pub fn set_tier_available(&mut self, tier: TierClass, available: bool) {
        self.tier_available.insert(tier, available);
        // Reflect availability onto placements of that tier.
        for p in self.entries.values_mut() {
            if p.tier == tier {
                p.available = available;
            }
        }
    }

    /// Configure an external media root for a non-hot tier.
    pub fn set_external_root(&mut self, tier: TierClass, root: PathBuf) {
        if tier != TierClass::Hot {
            self.external_roots.insert(tier, root);
        }
    }

    /// External root if configured.
    pub fn external_root(&self, tier: TierClass) -> Option<&Path> {
        self.external_roots.get(&tier).map(|p| p.as_path())
    }

    /// Insert or replace a placement entry.
    pub fn upsert(&mut self, placement: SegmentPlacement) {
        self.entries.insert(placement.segment_id, placement);
    }

    /// Remove placement (segment file may still exist until deleted separately).
    pub fn remove(&mut self, segment_id: &[u8; 16]) {
        self.entries.remove(segment_id);
    }

    /// Build coverage from current placement + tier availability.
    pub fn coverage(&self) -> TierCoverage {
        let mut searched = Vec::new();
        let mut offline = Vec::new();
        let mut unavailable_segments = Vec::new();

        for tier in [
            TierClass::Hot,
            TierClass::Warm,
            TierClass::Cold,
            TierClass::Archive,
        ] {
            if self.is_tier_available(tier) {
                searched.push(tier);
            } else {
                offline.push(tier);
            }
        }

        for p in self.entries.values() {
            if !p.available || !self.is_tier_available(p.tier) {
                unavailable_segments.push(p.segment_id);
            }
        }
        unavailable_segments.sort();
        unavailable_segments.dedup();

        let mut cov = TierCoverage {
            searched,
            excluded: Vec::new(),
            offline,
            unavailable_segments,
            notes: Vec::new(),
        };
        if cov.searched.iter().any(|t| t.is_archive_path()) {
            cov.note("archive/cold path in scope: not subject to hot-path latency SLOs");
        }
        if !cov.offline.is_empty() {
            cov.note("offline tier(s) present: incomplete coverage, not empty success");
        }
        cov
    }
}

/// Absolute path of the tier placement catalog.
pub fn tier_placement_path(catalogs_dir: &Path) -> PathBuf {
    catalogs_dir.join(TIER_PLACEMENT_FILE)
}

/// Default directory for a non-hot tier under the store root.
pub fn default_tier_dir(paths: &StorePaths, tier: TierClass) -> PathBuf {
    match tier {
        TierClass::Hot => paths.segments_dir(),
        TierClass::Warm => paths.tiers_dir().join("warm"),
        TierClass::Cold => paths.tiers_dir().join("cold"),
        TierClass::Archive => paths.tiers_dir().join("archive"),
    }
}

/// Resolve the media directory for a tier (external root or default).
pub fn tier_media_dir(paths: &StorePaths, placement: &TierPlacement, tier: TierClass) -> PathBuf {
    if let Some(ext) = placement.external_root(tier) {
        return ext.to_path_buf();
    }
    default_tier_dir(paths, tier)
}

/// Absolute path where a sealed segment should live on a tier.
pub fn segment_path_on_tier(
    paths: &StorePaths,
    placement: &TierPlacement,
    tier: TierClass,
    segment_id: &[u8; 16],
) -> PathBuf {
    tier_media_dir(paths, placement, tier).join(format!("{}.residiuum", hex16(segment_id)))
}

/// Relative path string stored in the placement catalog.
pub fn relative_segment_path(paths: &StorePaths, absolute: &Path) -> String {
    absolute
        .strip_prefix(&paths.root)
        .map(|rel| rel.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|_| absolute.to_string_lossy().replace('\\', "/"))
}

/// Hash segment file bytes (BLAKE3-256).
pub fn hash_file(path: &Path) -> Result<([u8; 32], u64), StoreError> {
    let mut file = File::open(path)?;
    let mut hasher = Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    let mut size = 0u64;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        size += n as u64;
    }
    Ok((*hasher.finalize().as_bytes(), size))
}

/// Copy file with fsync of destination (migration safety).
fn copy_verified(
    src: &Path,
    dest: &Path,
    segment_id: [u8; 16],
) -> Result<([u8; 32], u64), StoreError> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = fs::read(src)?;
    let mut hasher = Hasher::new();
    hasher.update(&bytes);
    let hash = *hasher.finalize().as_bytes();
    let size = bytes.len() as u64;

    let tmp = dest.with_extension("residiuum.tmp");
    {
        let mut out = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        out.write_all(&bytes)?;
        out.sync_all()?;
    }
    crate::media_inventory::rename_exclusive(&tmp, dest, segment_id)?;
    if let Some(parent) = dest.parent() {
        let _ = sync_dir(parent);
    }

    // Verify destination matches.
    let (dest_hash, dest_size) = hash_file(dest)?;
    if dest_hash != hash || dest_size != size {
        // Do not delete dest on mismatch after exclusive create — leave for operator;
        // collision-safe publish already forbade replace. Remove only our tmp residue.
        let _ = fs::remove_file(&tmp);
        return Err(StoreError::CorruptMeta("tier migration hash mismatch"));
    }
    Ok((hash, size))
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    let f = File::open(dir)?;
    f.sync_all()
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Persist migration evidence under `recovery/migrations/`.
pub fn write_migration_evidence(
    paths: &StorePaths,
    evidence: &MigrationEvidence,
) -> Result<PathBuf, StoreError> {
    let dir = paths.recovery_dir().join("migrations");
    fs::create_dir_all(&dir)?;
    let name = format!(
        "{}-{}-to-{}.txt",
        hex16(&evidence.segment_id),
        evidence.from_tier.as_str(),
        evidence.to_tier.as_str()
    );
    let path = dir.join(name);
    let mode = match evidence.mode {
        TierMoveMode::Copy => "copy",
        TierMoveMode::Move => "move",
    };
    let body = format!(
        "residiuum-migration-v1\n\
         segment_id={}\n\
         from_tier={}\n\
         to_tier={}\n\
         mode={}\n\
         source_hash={}\n\
         dest_hash={}\n\
         size={}\n\
         tool_version={}\n\
         migrated_ns={}\n",
        hex16(&evidence.segment_id),
        evidence.from_tier,
        evidence.to_tier,
        mode,
        hex_bytes(&evidence.source_hash),
        hex_bytes(&evidence.dest_hash),
        evidence.size,
        evidence.tool_version,
        evidence.migrated_ns,
    );
    crate::atomic_file::write_atomic(&path, body.as_bytes())?;
    Ok(path)
}

fn hex_bytes(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Copy or move a sealed segment to another tier; preserves segment id.
pub fn transfer_segment(
    paths: &StorePaths,
    placement: &mut TierPlacement,
    segment_id: [u8; 16],
    to_tier: TierClass,
    mode: TierMoveMode,
) -> Result<MigrationEvidence, StoreError> {
    if to_tier == TierClass::Hot {
        // Moving back to hot is allowed; destination is segments/.
    }
    if !placement.is_tier_available(to_tier) {
        return Err(StoreError::TierOffline(to_tier.as_str()));
    }

    let current = placement
        .get(&segment_id)
        .cloned()
        .ok_or(StoreError::SegmentNotFound)?;

    if !current.available {
        return Err(StoreError::TierOffline(current.tier.as_str()));
    }

    let src = resolve_placement_path(paths, &current)?;
    if !src.is_file() {
        return Err(StoreError::SegmentNotFound);
    }

    let dest = segment_path_on_tier(paths, placement, to_tier, &segment_id);
    if src == dest {
        // Already there.
        let (hash, size) = hash_file(&src)?;
        return Ok(MigrationEvidence {
            segment_id,
            from_tier: current.tier,
            to_tier,
            mode,
            source_hash: hash,
            dest_hash: hash,
            size,
            tool_version: "residiuum-store-9".into(),
            migrated_ns: now_ns(),
        });
    }

    let (hash, size) = copy_verified(&src, &dest, segment_id)?;
    let evidence = MigrationEvidence {
        segment_id,
        from_tier: current.tier,
        to_tier,
        mode,
        source_hash: hash,
        dest_hash: hash,
        size,
        tool_version: "residiuum-store-9".into(),
        migrated_ns: now_ns(),
    };
    write_migration_evidence(paths, &evidence)?;

    crate::failpoint::hit("store.tier.before_placement_write")?;

    let rel = relative_segment_path(paths, &dest);
    placement.upsert(SegmentPlacement {
        segment_id,
        tier: to_tier,
        relative_path: rel,
        content_hash: ContentHashState::Known(hash),
        size,
        available: true,
    });

    if mode == TierMoveMode::Move && src != dest {
        fs::remove_file(&src)?;
        if let Some(parent) = src.parent() {
            let _ = sync_dir(parent);
        }
    } else if mode == TierMoveMode::Copy && current.tier != to_tier {
        // Dual residency: keep source placement? Spec allows copy. We track
        // primary tier as destination; source file remains for durability.
        // Discover will still find the source if dest is offline later.
    }

    Ok(evidence)
}

/// Resolve an absolute path from a placement entry.
pub fn resolve_placement_path(
    paths: &StorePaths,
    placement: &SegmentPlacement,
) -> Result<PathBuf, StoreError> {
    let p = PathBuf::from(&placement.relative_path);
    if p.is_absolute() {
        return Ok(p);
    }
    Ok(paths.root.join(p))
}

/// Discover sealed segments on all available media and merge into placement.
///
/// Hot `segments/` is always scanned when available. Non-hot default dirs and
/// external roots are scanned when their tier is available. Active segment is
/// not registered as tier-movable (still open for append).
pub fn discover_placements(
    paths: &StorePaths,
    placement: &mut TierPlacement,
) -> Result<(), StoreError> {
    // Ensure default tier dirs exist (empty is fine).
    for tier in [TierClass::Warm, TierClass::Cold, TierClass::Archive] {
        let dir = default_tier_dir(paths, tier);
        let _ = fs::create_dir_all(&dir);
    }

    let scan_dir =
        |placement: &mut TierPlacement, tier: TierClass, dir: &Path| -> Result<(), StoreError> {
            if !placement.is_tier_available(tier) {
                return Ok(());
            }
            for path in list_residiuum_files(dir)? {
                let Some(id) = segment_id_from_filename(&path) else {
                    continue;
                };
                // Honour operator-chosen primary placement when its file still
                // exists (copy leaves dual residency; do not snap back to hot).
                // Check this before hashing: sealed media is immutable and a
                // retained placement already carries its verified hash/size.
                if let Some(existing) = placement.get(&id) {
                    let existing_path = resolve_placement_path(paths, existing)?;
                    if existing_path.is_file() {
                        continue;
                    }
                    // Primary missing: adopt this available copy.
                }
                let (hash, size) = hash_file(&path)?;
                let rel = relative_segment_path(paths, &path);
                placement.upsert(SegmentPlacement {
                    segment_id: id,
                    tier,
                    relative_path: rel,
                    content_hash: ContentHashState::Known(hash),
                    size,
                    available: true,
                });
            }
            Ok(())
        };

    if placement.is_tier_available(TierClass::Hot) {
        scan_dir(placement, TierClass::Hot, &paths.segments_dir())?;
    }
    for tier in [TierClass::Warm, TierClass::Cold, TierClass::Archive] {
        let dir = tier_media_dir(paths, placement, tier);
        scan_dir(placement, tier, &dir)?;
    }

    // Mark placements unavailable when their tier is offline.
    for p in placement.entries.values_mut() {
        if !placement
            .tier_available
            .get(&p.tier)
            .copied()
            .unwrap_or(true)
        {
            p.available = false;
        }
    }
    Ok(())
}

/// Paths of every **available** sealed segment (for index rebuild / salvage).
///
/// Active segment is appended last when present (caller may add it). Offline
/// tier segments are omitted unless a dual-resident copy exists on an online
/// tier — callers must still consult [`TierPlacement::coverage`] for honesty.
pub fn available_sealed_paths(
    paths: &StorePaths,
    placement: &TierPlacement,
) -> Result<Vec<PathBuf>, StoreError> {
    let mut out = Vec::new();
    let mut seen_ids = std::collections::BTreeSet::new();
    let mut seen_paths = std::collections::BTreeSet::new();

    for p in placement.entries() {
        if p.available && placement.is_tier_available(p.tier) {
            let path = resolve_placement_path(paths, p)?;
            if path.is_file() {
                let key = path.to_string_lossy().into_owned();
                if seen_paths.insert(key) {
                    out.push(path);
                    seen_ids.insert(p.segment_id);
                }
                continue;
            }
        }
        // Primary offline or missing: look for dual-resident copies on online tiers.
        if seen_ids.contains(&p.segment_id) {
            continue;
        }
        for tier in [
            TierClass::Hot,
            TierClass::Warm,
            TierClass::Cold,
            TierClass::Archive,
        ] {
            if !placement.is_tier_available(tier) {
                continue;
            }
            let alt = segment_path_on_tier(paths, placement, tier, &p.segment_id);
            if alt.is_file() {
                let key = alt.to_string_lossy().into_owned();
                if seen_paths.insert(key) {
                    out.push(alt);
                    seen_ids.insert(p.segment_id);
                }
                break;
            }
        }
    }

    // Also pick up any hot sealed files not yet in placement (fresh seal).
    if placement.is_tier_available(TierClass::Hot) {
        for path in list_residiuum_files(&paths.segments_dir())? {
            let key = path.to_string_lossy().into_owned();
            if seen_paths.insert(key) {
                out.push(path);
            }
        }
    }

    out.sort();
    Ok(out)
}

/// Encode placement catalog bytes.
pub fn encode_placement(store_id: [u8; 16], placement: &TierPlacement) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(PLACEMENT_MAGIC);
    out.extend_from_slice(&PLACEMENT_VERSION.to_le_bytes());
    out.extend_from_slice(&store_id);

    // Tier availability: 4 entries.
    for tier in [
        TierClass::Hot,
        TierClass::Warm,
        TierClass::Cold,
        TierClass::Archive,
    ] {
        out.push(tier as u8);
        out.push(u8::from(placement.is_tier_available(tier)));
    }

    // External roots.
    let roots: Vec<_> = placement
        .external_roots
        .iter()
        .map(|(t, p)| (*t, p.to_string_lossy().into_owned()))
        .collect();
    out.extend_from_slice(&(roots.len() as u32).to_le_bytes());
    for (tier, path) in &roots {
        out.push(*tier as u8);
        let b = path.as_bytes();
        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
        out.extend_from_slice(b);
    }

    let entries: Vec<_> = placement.entries().cloned().collect();
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in &entries {
        out.extend_from_slice(&e.segment_id);
        out.push(e.tier as u8);
        out.push(u8::from(e.available));
        out.extend_from_slice(&e.size.to_le_bytes());
        e.content_hash.encode_wire(&mut out);
        let pb = e.relative_path.as_bytes();
        out.extend_from_slice(&(pb.len() as u32).to_le_bytes());
        out.extend_from_slice(pb);
    }

    let mut hasher = Hasher::new();
    hasher.update(&out);
    out.extend_from_slice(hasher.finalize().as_bytes());
    out
}

/// Decode placement catalog; `None` if corrupt or store_id mismatch.
pub fn decode_placement(bytes: &[u8], store_id: [u8; 16]) -> Option<TierPlacement> {
    if bytes.len() < 8 + 4 + 16 + 8 + 4 + 4 + 32 {
        return None;
    }
    if &bytes[0..8] != PLACEMENT_MAGIC.as_slice() {
        return None;
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
    if version != PLACEMENT_VERSION {
        return None;
    }
    let sid: [u8; 16] = bytes[12..28].try_into().ok()?;
    if sid != store_id {
        return None;
    }
    let mut cursor = 28usize;
    let mut placement = TierPlacement::new();

    for _ in 0..4 {
        if cursor + 2 > bytes.len().saturating_sub(32) {
            return None;
        }
        let tier = TierClass::from_u8(bytes[cursor])?;
        let avail = bytes[cursor + 1] != 0;
        placement.tier_available.insert(tier, avail);
        cursor += 2;
    }

    if cursor + 4 > bytes.len().saturating_sub(32) {
        return None;
    }
    let n_roots = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().ok()?) as usize;
    cursor += 4;
    for _ in 0..n_roots {
        if cursor + 1 + 4 > bytes.len().saturating_sub(32) {
            return None;
        }
        let tier = TierClass::from_u8(bytes[cursor])?;
        cursor += 1;
        let len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().ok()?) as usize;
        cursor += 4;
        if cursor + len > bytes.len().saturating_sub(32) {
            return None;
        }
        let path = std::str::from_utf8(&bytes[cursor..cursor + len]).ok()?;
        placement.external_roots.insert(tier, PathBuf::from(path));
        cursor += len;
    }

    if cursor + 4 > bytes.len().saturating_sub(32) {
        return None;
    }
    let n = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().ok()?) as usize;
    cursor += 4;
    for _ in 0..n {
        if cursor + 16 + 1 + 1 + 8 + 33 + 4 > bytes.len().saturating_sub(32) {
            return None;
        }
        let segment_id: [u8; 16] = bytes[cursor..cursor + 16].try_into().ok()?;
        cursor += 16;
        let tier = TierClass::from_u8(bytes[cursor])?;
        cursor += 1;
        let available = bytes[cursor] != 0;
        cursor += 1;
        let size = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().ok()?);
        cursor += 8;
        let (content_hash, hn) = ContentHashState::decode_wire(&bytes[cursor..])?;
        cursor += hn;
        let plen = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().ok()?) as usize;
        cursor += 4;
        if cursor + plen > bytes.len().saturating_sub(32) {
            return None;
        }
        let relative_path = std::str::from_utf8(&bytes[cursor..cursor + plen])
            .ok()?
            .to_string();
        cursor += plen;
        placement.upsert(SegmentPlacement {
            segment_id,
            tier,
            relative_path,
            content_hash,
            size,
            available,
        });
    }

    if cursor + 32 != bytes.len() {
        return None;
    }
    let mut hasher = Hasher::new();
    hasher.update(&bytes[..cursor]);
    if hasher.finalize().as_bytes() != &bytes[cursor..cursor + 32] {
        return None;
    }
    Some(placement)
}

/// Persist placement catalog (atomic durable replace, DEF-021).
pub fn write_placement(
    path: &Path,
    store_id: [u8; 16],
    placement: &TierPlacement,
) -> Result<(), StoreError> {
    let bytes = encode_placement(store_id, placement);
    crate::atomic_file::write_atomic_if_changed(path, &bytes)?;
    Ok(())
}

/// Load placement catalog when valid.
pub fn try_load_placement(
    path: &Path,
    store_id: [u8; 16],
) -> Result<Option<TierPlacement>, StoreError> {
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(_) => return Ok(None),
    };
    Ok(decode_placement(&bytes, store_id))
}

/// Persist simple tier roots / availability text (operator-editable).
pub fn write_tier_roots_file(
    paths: &StorePaths,
    placement: &TierPlacement,
) -> Result<(), StoreError> {
    let path = paths.tiers_dir().join(TIER_ROOTS_FILE);
    fs::create_dir_all(paths.tiers_dir())?;
    let mut body = String::from("# residiuum tier roots v1 — derived; edit with care\n");
    for tier in [
        TierClass::Hot,
        TierClass::Warm,
        TierClass::Cold,
        TierClass::Archive,
    ] {
        let avail = if placement.is_tier_available(tier) {
            "online"
        } else {
            "offline"
        };
        let root = placement
            .external_root(tier)
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| default_tier_dir(paths, tier).display().to_string());
        body.push_str(&format!("{} {} {}\n", tier.as_str(), avail, root));
    }
    crate::atomic_file::write_atomic_if_changed(&path, body.as_bytes())?;
    Ok(())
}

/// Best-effort load of roots.txt (availability + external roots).
///
/// The third column may be a filesystem path or a media URI
/// (`object:local:…`, `file://…`, `s3://…`, `gs://…`). Cloud schemes resolve
/// via `RESIDIUUM_S3_ROOT` / `RESIDIUUM_GS_ROOT` mirrors; without a mirror they are
/// recorded as offline so coverage stays honest.
pub fn load_tier_roots_file(paths: &StorePaths, placement: &mut TierPlacement) {
    let path = paths.tiers_dir().join(TIER_ROOTS_FILE);
    let Ok(text) = fs::read_to_string(path) else {
        return;
    };
    let mirror = crate::media::CloudMirrorConfig::from_env();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let Some(name) = parts.next() else { continue };
        let Some(tier) = TierClass::parse(name) else {
            continue;
        };
        let Some(state) = parts.next() else { continue };
        let online = !matches!(state, "offline" | "unmounted" | "unavailable");
        placement.set_tier_available(tier, online);
        if let Some(root) = parts.next() {
            if tier != TierClass::Hot {
                // Prefer resolving media URIs to a concrete directory when the
                // build can open them (filesystem / object:local / mirrored cloud).
                match crate::media::media_root_directory_with(root, &mirror) {
                    Ok(dir) => placement.set_external_root(tier, dir),
                    Err(_) => {
                        // Unresolvable cloud (or bad) root: do not pretend empty success.
                        if root.contains("://") || root.starts_with("object:") {
                            placement.set_tier_available(tier, false);
                        } else {
                            placement.set_external_root(tier, PathBuf::from(root));
                        }
                    }
                }
            }
        }
    }
}

/// Classify whether segment bytes are readable by this wire major.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatClassification {
    /// All discovered frames use a supported wire major (or file empty).
    Supported {
        /// Wire major observed (if any frame found).
        wire_major: Option<u8>,
        /// Wire minor observed.
        wire_minor: Option<u8>,
    },
    /// At least one frame has an unsupported wire major; bytes preserved as-is.
    FormatUnsupported {
        /// Wire major that is not supported.
        wire_major: u8,
        /// File size preserved.
        byte_len: u64,
        /// Content hash of preserved bytes.
        content_hash: [u8; 32],
    },
    /// No verified frames; garbage or empty — still preserve bytes.
    Unreadable {
        /// File size.
        byte_len: u64,
        /// Content hash.
        content_hash: [u8; 32],
    },
}

/// Inspect segment bytes without rewriting them (multi-generation readers).
pub fn classify_segment_bytes(bytes: &[u8]) -> FormatClassification {
    use residiuum_format::{scan_forward, FrameVerifyError, SafetyLimits, WIRE_MAJOR};

    if bytes.is_empty() {
        return FormatClassification::Supported {
            wire_major: None,
            wire_minor: None,
        };
    }

    let mut hasher = Hasher::new();
    hasher.update(bytes);
    let content_hash = *hasher.finalize().as_bytes();
    let byte_len = bytes.len() as u64;

    let report = scan_forward(bytes, SafetyLimits::default());
    let mut supported_major = None;
    let mut supported_minor = None;
    let mut saw_unsupported = None;

    for region in &report.regions {
        match region {
            residiuum_format::ScanRegion::VerifiedFrame { frame, .. } => {
                if frame.header.wire_major != WIRE_MAJOR {
                    saw_unsupported = Some(frame.header.wire_major);
                } else {
                    supported_major = Some(frame.header.wire_major);
                    supported_minor = Some(frame.header.wire_minor);
                }
            }
            residiuum_format::ScanRegion::Hole { reason, .. } => {
                if let residiuum_format::HoleReason::CorruptCandidate {
                    error: FrameVerifyError::UnsupportedWireMajor(m),
                    ..
                } = reason
                {
                    saw_unsupported = Some(*m);
                }
            }
        }
    }

    if let Some(m) = saw_unsupported {
        return FormatClassification::FormatUnsupported {
            wire_major: m,
            byte_len,
            content_hash,
        };
    }

    if report.verified_count() == 0 {
        return FormatClassification::Unreadable {
            byte_len,
            content_hash,
        };
    }

    FormatClassification::Supported {
        wire_major: supported_major,
        wire_minor: supported_minor,
    }
}

/// Register a newly sealed hot segment in placement.
pub fn register_hot_segment(
    paths: &StorePaths,
    placement: &mut TierPlacement,
    segment_id: [u8; 16],
) -> Result<(), StoreError> {
    let path = paths.sealed_segment(&segment_id);
    if !path.is_file() {
        return Ok(());
    }
    let (hash, size) = hash_file(&path)?;
    register_hot_segment_known(
        paths,
        placement,
        segment_id,
        ContentHashState::Known(hash),
        size,
    )
}

/// Register a hot sealed segment when content hash/size are already known.
///
/// Avoids a second full-file hash after seal has just written the bytes
/// (write-path scale: seal work must stay O(segment), not O(retained data)).
/// `content_hash` may be [`ContentHashState::Pending`] until enrichment.
pub fn register_hot_segment_known(
    paths: &StorePaths,
    placement: &mut TierPlacement,
    segment_id: [u8; 16],
    content_hash: ContentHashState,
    size: u64,
) -> Result<(), StoreError> {
    let path = paths.sealed_segment(&segment_id);
    if !path.is_file() {
        return Ok(());
    }
    placement.upsert(SegmentPlacement {
        segment_id,
        tier: TierClass::Hot,
        relative_path: relative_segment_path(paths, &path),
        content_hash,
        size,
        available: placement.is_tier_available(TierClass::Hot),
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn placement_roundtrip() {
        let mut p = TierPlacement::new();
        p.upsert(SegmentPlacement {
            segment_id: [1u8; 16],
            tier: TierClass::Cold,
            relative_path: "tiers/cold/010101...".into(),
            content_hash: ContentHashState::Known([9u8; 32]),
            size: 42,
            available: true,
        });
        p.set_tier_available(TierClass::Archive, false);
        let bytes = encode_placement([7u8; 16], &p);
        let decoded = decode_placement(&bytes, [7u8; 16]).unwrap();
        assert_eq!(decoded.len(), 1);
        assert!(!decoded.is_tier_available(TierClass::Archive));
        assert_eq!(decoded.get(&[1u8; 16]).unwrap().tier, TierClass::Cold);
    }

    #[test]
    fn classify_empty_supported() {
        assert!(matches!(
            classify_segment_bytes(&[]),
            FormatClassification::Supported {
                wire_major: None,
                ..
            }
        ));
    }

    #[test]
    fn coverage_offline_incomplete() {
        let mut p = TierPlacement::new();
        p.set_tier_available(TierClass::Archive, false);
        p.upsert(SegmentPlacement {
            segment_id: [2u8; 16],
            tier: TierClass::Archive,
            relative_path: "tiers/archive/x.residiuum".into(),
            content_hash: ContentHashState::Pending,
            size: 1,
            available: false,
        });
        let c = p.coverage();
        assert!(c.is_incomplete());
        assert!(c.offline.contains(&TierClass::Archive));
        assert_eq!(c.unavailable_segments.len(), 1);
    }

    #[test]
    fn transfer_copy_preserves_identity() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path().join("s"));
        paths.create_dirs().unwrap();
        let seg_id = [3u8; 16];
        let hot = paths.sealed_segment(&seg_id);
        fs::write(&hot, b"RESIDIUUM-fake-segment-bytes-for-hash").unwrap();

        let mut placement = TierPlacement::new();
        register_hot_segment(&paths, &mut placement, seg_id).unwrap();

        let ev = transfer_segment(
            &paths,
            &mut placement,
            seg_id,
            TierClass::Cold,
            TierMoveMode::Copy,
        )
        .unwrap();
        assert_eq!(ev.segment_id, seg_id);
        assert_eq!(ev.from_tier, TierClass::Hot);
        assert_eq!(ev.to_tier, TierClass::Cold);
        assert_eq!(ev.source_hash, ev.dest_hash);
        assert!(hot.is_file()); // copy
        let cold = segment_path_on_tier(&paths, &placement, TierClass::Cold, &seg_id);
        assert!(cold.is_file());
        assert_eq!(placement.get(&seg_id).unwrap().tier, TierClass::Cold);
    }
}
