//! Per-segment Chimera layout sidecars (seal / compaction wire-up).
//!
//! **Product seal default (CSE-2R safety rollback):** seal/enrichment persists a
//! Materialized layout via [`build_materialized_layout`] (inline / point
//! containers / value log). This restores product salvage safety; Compact
//! SegmentFrame salvage equivalence remains **unproven** (CSE-1 FAIL). Compact
//! layouts stay available via [`build_compact_layout`] for ETQ measurement.
//!
//! Layouts live under `indexes/chimera/{hex16}.cmr` and are **derived only** —
//! loss must never block segment salvage or PrimaryIndex rebuild.
//!
//! Product `Store::get` resolves via PrimaryIndex locators. Layouts are loaded
//! by `Store::get_via_chimera` / seal tooling; do not full-read a `.cmr` on
//! every hot get.

use super::{
    pack_point_containers, resolve, ClassifyOptions, IoSelectOptions, LocatorKind, PointContainer,
    ResolveContext, ValueClass, ValueLocator, ValueLog, ValueLogRecord, DEFAULT_CONTAINER_TARGET,
};
use crate::error::StoreError;
use crate::layout::{hex16, StorePaths};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Directory for per-segment chimera layouts: `indexes/chimera/`.
pub fn chimera_dir(paths: &StorePaths) -> PathBuf {
    paths.indexes_dir().join("chimera")
}

/// Path of one segment chimera layout: `indexes/chimera/{hex16}.cmr`.
pub fn chimera_layout_path(paths: &StorePaths, segment_id: &[u8; 16]) -> PathBuf {
    chimera_dir(paths).join(format!("{}.cmr", hex16(segment_id)))
}

const MAGIC: &[u8; 8] = b"RCHIMR01";
/// Current on-disk layout version (compact SegmentFrame default).
pub const CHIMERA_LAYOUT_VERSION: u32 = 2;
/// Legacy full-payload embedding layout (still decodable).
pub const CHIMERA_LAYOUT_VERSION_LEGACY: u32 = 1;

const TAG_INLINE: u8 = 1;
const TAG_POINT: u8 = 2;
const TAG_SCAN: u8 = 3;
const TAG_LARGE: u8 = 4;
const TAG_RESIDENT: u8 = 5;
const TAG_SEGMENT_FRAME: u8 = 6;

/// One compact frame reference used to build a default Chimera layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactFrameRef {
    /// Establishing segment id.
    pub segment_id: [u8; 16],
    /// Item-frame byte offset within that segment.
    pub frame_offset: u64,
    /// Expected body length, or 0 when unknown.
    pub body_len: u32,
}

/// Compiled Chimera placement for one sealed segment (or live-projection output).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChimeraLayout {
    /// Relocation generation baked into locators / containers.
    pub generation: u32,
    /// Subject → physical locator (BTree for deterministic encode).
    pub entries: BTreeMap<Vec<u8>, ValueLocator>,
    /// Point micro-page containers (indexed by sequential container_id from 0).
    pub containers: Vec<PointContainer>,
    /// Large-value log bytes for this layout.
    pub value_log: ValueLog,
}

impl ChimeraLayout {
    /// Empty layout.
    pub fn empty(generation: u32) -> Self {
        Self {
            generation,
            entries: BTreeMap::new(),
            containers: Vec::new(),
            value_log: ValueLog::new(),
        }
    }

    /// Number of subject locators.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no subjects were placed.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Locator for a subject key, if present.
    pub fn locator(&self, key: &[u8]) -> Option<&ValueLocator> {
        self.entries.get(key)
    }

    /// Resolve a subject key to logical bytes using this layout only.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let Some(loc) = self.entries.get(key) else {
            return Ok(None);
        };
        let bytes = self.resolve_locator(loc)?;
        Ok(Some(bytes))
    }

    /// Resolve a locator against containers / value log held in this layout.
    pub fn resolve_locator(&self, loc: &ValueLocator) -> Result<Vec<u8>, StoreError> {
        let mut ctx = ResolveContext::default();
        let container;
        match loc {
            ValueLocator::PointContainer { container_id, .. } => {
                container = self.containers.get(*container_id as usize).ok_or_else(|| {
                    StoreError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "chimera container id out of range",
                    ))
                })?;
                ctx.point_container = Some(container);
            }
            ValueLocator::LargeValueLog { .. } => {
                ctx.value_log = Some(&self.value_log);
            }
            ValueLocator::Inline { .. } | ValueLocator::Resident { .. } => {}
            ValueLocator::ScanExtent { .. } => {
                // Scan extents are not materialised in the foundation seal cut
                // (medium values use point containers). Reject if present.
                return Err(StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "chimera scan extent not supported in segment layout resolve",
                )));
            }
            ValueLocator::SegmentFrame { .. } => {
                // Needs store pread of the authoritative segment frame.
                return Err(StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "chimera segment frame requires store resolve",
                )));
            }
        }
        let resolved = resolve(loc, &ctx, &IoSelectOptions::default())?;
        Ok(resolved.bytes)
    }

    /// Count locators by kind (telemetry / tests).
    pub fn count_by_kind(&self) -> ChimeraKindCounts {
        let mut c = ChimeraKindCounts::default();
        for loc in self.entries.values() {
            match loc.kind() {
                LocatorKind::Resident => c.resident += 1,
                LocatorKind::Inline => c.inline += 1,
                LocatorKind::PointContainer => c.point_container += 1,
                LocatorKind::ScanExtent => c.scan_extent += 1,
                LocatorKind::LargeValueLog => c.large_value_log += 1,
                LocatorKind::SegmentFrame => c.segment_frame += 1,
            }
        }
        c
    }
}

/// Locator-kind histogram for a layout.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChimeraKindCounts {
    /// Resident locators.
    pub resident: usize,
    /// Inline locators.
    pub inline: usize,
    /// Point-container locators.
    pub point_container: usize,
    /// Scan-extent locators.
    pub scan_extent: usize,
    /// Large-value-log locators.
    pub large_value_log: usize,
    /// Segment-frame locators (compact default).
    pub segment_frame: usize,
}

/// Build the **ETQ / measurement** compact layout: key → segment-frame locators only.
///
/// Duplicate keys keep the **last** frame ref. No containers / value-log bytes.
/// Not the product seal default after CSE-2R safety rollback (see
/// [`build_materialized_layout`]). Compact salvage equivalence is still open (CSE-3).
pub fn build_compact_layout(
    frames: &[(Vec<u8>, CompactFrameRef)],
    generation: u32,
) -> ChimeraLayout {
    let mut map: BTreeMap<Vec<u8>, CompactFrameRef> = BTreeMap::new();
    for (k, r) in frames {
        map.insert(k.clone(), *r);
    }
    let mut layout = ChimeraLayout::empty(generation);
    for (k, r) in map {
        layout.entries.insert(
            k,
            ValueLocator::SegmentFrame {
                segment_id: r.segment_id,
                frame_offset: r.frame_offset,
                body_len: r.body_len,
                generation,
            },
        );
    }
    layout
}

/// Product seal layout (CSE-2R safety rollback): embeds tiny/medium/large bodies.
///
/// Tiny → inline, medium → point containers, large → value log.
/// Compact SegmentFrame remains available via [`build_compact_layout`] for ETQ.
/// Restoring this path is **not** Compact minimum parity.
pub fn build_materialized_layout(
    pairs: &[(Vec<u8>, Vec<u8>)],
    generation: u32,
    classify_opts: &ClassifyOptions,
) -> ChimeraLayout {
    build_layout(pairs, generation, classify_opts)
}

/// Build a layout from logical (key, value) pairs using classification.
///
/// Tiny → inline, medium → point containers (ids 0..), large → value log id 0.
/// Duplicate keys keep the **last** value (caller order).
pub fn build_layout(
    pairs: &[(Vec<u8>, Vec<u8>)],
    generation: u32,
    classify_opts: &ClassifyOptions,
) -> ChimeraLayout {
    // Last-wins dedup while preserving insertion for packing order of survivors.
    let mut map: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for (k, v) in pairs {
        map.insert(k.clone(), v.clone());
    }
    let unique: Vec<(Vec<u8>, Vec<u8>)> = map.into_iter().collect();

    let mut layout = ChimeraLayout::empty(generation);
    let mut medium_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut large_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    for (k, v) in &unique {
        match super::classify_value(v.len(), classify_opts) {
            ValueClass::Tiny => {
                layout
                    .entries
                    .insert(k.clone(), ValueLocator::Inline { bytes: v.clone() });
            }
            ValueClass::Medium => medium_pairs.push((k.clone(), v.clone())),
            ValueClass::Large => large_pairs.push((k.clone(), v.clone())),
        }
    }

    if !medium_pairs.is_empty() {
        let (containers, locs) = pack_point_containers(
            &medium_pairs,
            0,
            generation,
            DEFAULT_CONTAINER_TARGET,
            classify_opts,
        );
        layout.containers = containers;
        for ((k, _), loc) in medium_pairs.iter().zip(locs.into_iter()) {
            if let Some(l) = loc {
                layout.entries.insert(k.clone(), l);
            }
        }
    }

    for (k, v) in large_pairs {
        let (offset, len) = layout.value_log.append(&ValueLogRecord::new(generation, v));
        layout.entries.insert(
            k,
            ValueLocator::LargeValueLog {
                log_id: 0,
                offset,
                len,
                generation,
            },
        );
    }

    layout
}

/// Persist a chimera layout (atomic durable replace). Derived only.
pub fn write_chimera_layout(
    path: &Path,
    store_id: [u8; 16],
    segment_id: [u8; 16],
    layout: &ChimeraLayout,
) -> Result<(), StoreError> {
    let bytes = encode(store_id, segment_id, layout);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    crate::atomic_file::write_atomic(path, &bytes)?;
    Ok(())
}

/// Load a chimera layout when magic/version/store_id/segment_id match.
///
/// Returns `Ok(None)` for absent or unusable files (never blocks recovery).
pub fn try_load_chimera_layout(
    path: &Path,
    store_id: [u8; 16],
    segment_id: [u8; 16],
) -> Result<Option<ChimeraLayout>, StoreError> {
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(_) => return Ok(None),
    };
    Ok(decode(&bytes, store_id, segment_id))
}

/// Delete one chimera sidecar (never touches authoritative segments).
pub fn delete_chimera_layout(path: &Path) -> Result<(), StoreError> {
    if path.is_file() {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn encode(store_id: [u8; 16], segment_id: [u8; 16], layout: &ChimeraLayout) -> Vec<u8> {
    let mut out = Vec::with_capacity(128 + layout.len() * 48);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&CHIMERA_LAYOUT_VERSION.to_le_bytes());
    out.extend_from_slice(&store_id);
    out.extend_from_slice(&segment_id);
    out.extend_from_slice(&layout.generation.to_le_bytes());
    out.extend_from_slice(&(layout.entries.len() as u32).to_le_bytes());
    out.extend_from_slice(&(layout.containers.len() as u32).to_le_bytes());
    let vlog = layout.value_log.as_bytes();
    out.extend_from_slice(&(vlog.len() as u64).to_le_bytes());

    for (key, loc) in &layout.entries {
        out.extend_from_slice(&(key.len() as u32).to_le_bytes());
        out.extend_from_slice(key);
        encode_locator(loc, &mut out);
    }

    for c in &layout.containers {
        let enc = c.encode();
        out.extend_from_slice(&(enc.len() as u32).to_le_bytes());
        out.extend_from_slice(&enc);
    }

    out.extend_from_slice(vlog);
    out
}

fn encode_locator(loc: &ValueLocator, out: &mut Vec<u8>) {
    match loc {
        ValueLocator::Inline { bytes } => {
            out.push(TAG_INLINE);
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }
        ValueLocator::PointContainer {
            container_id,
            slot,
            generation,
        } => {
            out.push(TAG_POINT);
            out.extend_from_slice(&container_id.to_le_bytes());
            out.extend_from_slice(&slot.to_le_bytes());
            out.extend_from_slice(&generation.to_le_bytes());
        }
        ValueLocator::ScanExtent {
            extent_id,
            offset,
            len,
            generation,
        } => {
            out.push(TAG_SCAN);
            out.extend_from_slice(&extent_id.to_le_bytes());
            out.extend_from_slice(&offset.to_le_bytes());
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&generation.to_le_bytes());
        }
        ValueLocator::LargeValueLog {
            log_id,
            offset,
            len,
            generation,
        } => {
            out.push(TAG_LARGE);
            out.extend_from_slice(&log_id.to_le_bytes());
            out.extend_from_slice(&offset.to_le_bytes());
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&generation.to_le_bytes());
        }
        ValueLocator::Resident { generation } => {
            out.push(TAG_RESIDENT);
            out.extend_from_slice(&generation.to_le_bytes());
        }
        ValueLocator::SegmentFrame {
            segment_id,
            frame_offset,
            body_len,
            generation,
        } => {
            out.push(TAG_SEGMENT_FRAME);
            out.extend_from_slice(segment_id);
            out.extend_from_slice(&frame_offset.to_le_bytes());
            out.extend_from_slice(&body_len.to_le_bytes());
            out.extend_from_slice(&generation.to_le_bytes());
        }
    }
}

fn decode(bytes: &[u8], store_id: [u8; 16], segment_id: [u8; 16]) -> Option<ChimeraLayout> {
    let mut i = 0usize;
    if bytes.len() < 8 + 4 + 16 + 16 + 4 + 4 + 4 + 8 {
        return None;
    }
    if &bytes[i..i + 8] != MAGIC.as_slice() {
        return None;
    }
    i += 8;
    let version = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?);
    i += 4;
    if version != CHIMERA_LAYOUT_VERSION && version != CHIMERA_LAYOUT_VERSION_LEGACY {
        return None;
    }
    if bytes[i..i + 16] != store_id {
        return None;
    }
    i += 16;
    if bytes[i..i + 16] != segment_id {
        return None;
    }
    i += 16;
    let generation = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?);
    i += 4;
    let n_entries = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?) as usize;
    i += 4;
    let n_containers = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?) as usize;
    i += 4;
    let vlog_len = u64::from_le_bytes(bytes[i..i + 8].try_into().ok()?) as usize;
    i += 8;

    let mut entries = BTreeMap::new();
    for _ in 0..n_entries {
        if i + 4 > bytes.len() {
            return None;
        }
        let key_len = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?) as usize;
        i += 4;
        if i + key_len > bytes.len() {
            return None;
        }
        let key = bytes[i..i + key_len].to_vec();
        i += key_len;
        let (loc, ni) = decode_locator(bytes, i)?;
        i = ni;
        entries.insert(key, loc);
    }

    let mut containers = Vec::with_capacity(n_containers);
    for _ in 0..n_containers {
        if i + 4 > bytes.len() {
            return None;
        }
        let len = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?) as usize;
        i += 4;
        if i + len > bytes.len() {
            return None;
        }
        let c = PointContainer::decode(&bytes[i..i + len]).ok()?;
        i += len;
        containers.push(c);
    }

    if i + vlog_len > bytes.len() {
        return None;
    }
    let value_log = ValueLog::from_bytes(bytes[i..i + vlog_len].to_vec());

    Some(ChimeraLayout {
        generation,
        entries,
        containers,
        value_log,
    })
}

fn decode_locator(bytes: &[u8], mut i: usize) -> Option<(ValueLocator, usize)> {
    if i >= bytes.len() {
        return None;
    }
    let tag = bytes[i];
    i += 1;
    match tag {
        TAG_INLINE => {
            if i + 4 > bytes.len() {
                return None;
            }
            let len = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?) as usize;
            i += 4;
            if i + len > bytes.len() {
                return None;
            }
            let v = bytes[i..i + len].to_vec();
            i += len;
            Some((ValueLocator::Inline { bytes: v }, i))
        }
        TAG_POINT => {
            if i + 8 + 4 + 4 > bytes.len() {
                return None;
            }
            let container_id = u64::from_le_bytes(bytes[i..i + 8].try_into().ok()?);
            i += 8;
            let slot = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?);
            i += 4;
            let generation = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?);
            i += 4;
            Some((
                ValueLocator::PointContainer {
                    container_id,
                    slot,
                    generation,
                },
                i,
            ))
        }
        TAG_SCAN => {
            if i + 8 + 4 + 4 + 4 > bytes.len() {
                return None;
            }
            let extent_id = u64::from_le_bytes(bytes[i..i + 8].try_into().ok()?);
            i += 8;
            let offset = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?);
            i += 4;
            let len = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?);
            i += 4;
            let generation = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?);
            i += 4;
            Some((
                ValueLocator::ScanExtent {
                    extent_id,
                    offset,
                    len,
                    generation,
                },
                i,
            ))
        }
        TAG_LARGE => {
            if i + 8 + 8 + 8 + 4 > bytes.len() {
                return None;
            }
            let log_id = u64::from_le_bytes(bytes[i..i + 8].try_into().ok()?);
            i += 8;
            let offset = u64::from_le_bytes(bytes[i..i + 8].try_into().ok()?);
            i += 8;
            let len = u64::from_le_bytes(bytes[i..i + 8].try_into().ok()?);
            i += 8;
            let generation = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?);
            i += 4;
            Some((
                ValueLocator::LargeValueLog {
                    log_id,
                    offset,
                    len,
                    generation,
                },
                i,
            ))
        }
        TAG_RESIDENT => {
            if i + 4 > bytes.len() {
                return None;
            }
            let generation = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?);
            i += 4;
            Some((ValueLocator::Resident { generation }, i))
        }
        TAG_SEGMENT_FRAME => {
            if i + 16 + 8 + 4 + 4 > bytes.len() {
                return None;
            }
            let mut segment_id = [0u8; 16];
            segment_id.copy_from_slice(&bytes[i..i + 16]);
            i += 16;
            let frame_offset = u64::from_le_bytes(bytes[i..i + 8].try_into().ok()?);
            i += 8;
            let body_len = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?);
            i += 4;
            let generation = u32::from_le_bytes(bytes[i..i + 4].try_into().ok()?);
            i += 4;
            Some((
                ValueLocator::SegmentFrame {
                    segment_id,
                    frame_offset,
                    body_len,
                    generation,
                },
                i,
            ))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn build_materialized_places_all_classes() {
        let pairs = vec![
            (b"tiny".to_vec(), b"hi".to_vec()),
            (b"med".to_vec(), vec![9u8; 200]),
            (b"big".to_vec(), vec![7u8; 32 * 1024]),
        ];
        let layout = build_materialized_layout(&pairs, 3, &ClassifyOptions::default());
        assert_eq!(layout.len(), 3);
        let counts = layout.count_by_kind();
        assert_eq!(counts.inline, 1);
        assert_eq!(counts.point_container, 1);
        assert_eq!(counts.large_value_log, 1);
        assert_eq!(layout.get(b"tiny").unwrap().unwrap(), b"hi");
        assert_eq!(layout.get(b"med").unwrap().unwrap(), vec![9u8; 200]);
        assert_eq!(layout.get(b"big").unwrap().unwrap(), vec![7u8; 32 * 1024]);
    }

    #[test]
    fn compact_layout_roundtrip_and_amp() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.indexes_dir()).unwrap();
        let store_id = [1u8; 16];
        let seg = [2u8; 16];
        let auth_bytes: u64 = 64 * 1024 * 1024;
        let n = 8192usize;
        let body_len = (auth_bytes / n as u64) as u32;
        let mut frames = Vec::with_capacity(n);
        for i in 0..n {
            let key = format!("k{i:05}").into_bytes();
            frames.push((
                key,
                CompactFrameRef {
                    segment_id: seg,
                    frame_offset: (i as u64) * 8192,
                    body_len,
                },
            ));
        }
        let layout = build_compact_layout(&frames, 1);
        assert_eq!(layout.count_by_kind().segment_frame, n);
        assert!(layout.containers.is_empty());
        assert!(layout.value_log.as_bytes().is_empty());
        // In-layout get cannot resolve segment frames without store pread.
        assert!(layout
            .get(b"k00000")
            .unwrap_err()
            .to_string()
            .contains("store resolve"));

        let path = chimera_layout_path(&paths, &seg);
        write_chimera_layout(&path, store_id, seg, &layout).unwrap();
        let loaded = try_load_chimera_layout(&path, store_id, seg)
            .unwrap()
            .expect("layout present");
        assert_eq!(loaded.len(), n);
        let derived = fs::metadata(&path).unwrap().len();
        let ratio = derived as f64 / auth_bytes as f64;
        assert!(
            ratio <= 0.05,
            "compact chimera amp {ratio:.4} (derived={derived}) exceeds 5%"
        );
        match loaded.locator(b"k00000") {
            Some(ValueLocator::SegmentFrame {
                frame_offset,
                body_len: bl,
                ..
            }) => {
                assert_eq!(*frame_offset, 0);
                assert_eq!(*bl, body_len);
            }
            other => panic!("expected SegmentFrame, got {other:?}"),
        }
        assert!(try_load_chimera_layout(&path, [9u8; 16], seg)
            .unwrap()
            .is_none());
    }

    #[test]
    fn materialized_roundtrip_disk_still_supported() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.indexes_dir()).unwrap();
        let store_id = [1u8; 16];
        let seg = [2u8; 16];
        let pairs = vec![
            (b"a".to_vec(), b"tiny-a".to_vec()),
            (b"b".to_vec(), vec![1u8; 128]),
            (b"c".to_vec(), vec![2u8; 20_000]),
        ];
        let layout = build_materialized_layout(&pairs, 1, &ClassifyOptions::default());
        let path = chimera_layout_path(&paths, &seg);
        write_chimera_layout(&path, store_id, seg, &layout).unwrap();
        let loaded = try_load_chimera_layout(&path, store_id, seg)
            .unwrap()
            .expect("layout present");
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.get(b"a").unwrap().unwrap(), b"tiny-a");
        assert_eq!(loaded.get(b"b").unwrap().unwrap(), vec![1u8; 128]);
        assert_eq!(loaded.get(b"c").unwrap().unwrap(), vec![2u8; 20_000]);
    }
}
