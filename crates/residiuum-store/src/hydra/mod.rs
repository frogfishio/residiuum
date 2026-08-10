//! Hydra: adaptive per-segment indexes (INDEXING_STRATEGY_PROPOSAL).
//!
//! At seal time each immutable segment independently compiles the best physical
//! index for its key distribution:
//!
//! | Shape | Structure |
//! |-------|-----------|
//! | Tiny segment | Sorted Eytzinger array |
//! | Ordered numeric keys | PGM++ or RadixSpline |
//! | Ordered strings / irregular | Compressed ART / radix |
//! | Point-only immutable set | MPHF + fingerprint |
//!
//! Construction is safe to parallelize across segments (`build_many`). All
//! hydra indexes are **derived only** — loss must never prevent segment salvage.

mod eytzinger;
mod mphf;
mod pgm;
mod radix;
mod select;

use crate::error::StoreError;
use crate::layout::{hex16, StorePaths};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::thread;

pub use select::{
    classify_keys, select_index_kind, HydraBuildOptions, IndexKind, KeyShape,
    DEFAULT_TINY_THRESHOLD,
};

/// One subject key mapped to a byte offset inside its segment file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRecord {
    /// Subject / key bytes.
    pub key: Vec<u8>,
    /// Absolute byte offset of the establishing frame in the segment file.
    pub offset: u64,
}

/// Compiled hydra index for one sealed segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HydraIndex {
    /// Tiny: Eytzinger-ordered key/offset array.
    Eytzinger(eytzinger::EytzingerIndex),
    /// Ordered numeric: piecewise-linear PGM with bounded last-mile search.
    Pgm(pgm::PgmIndex),
    /// Dense ordered numeric: radix table + spline knots.
    RadixSpline(pgm::RadixSplineIndex),
    /// Ordered strings / irregular byte keys: path-compressed radix.
    CompressedRadix(radix::CompressedRadixIndex),
    /// Point-only: minimal perfect hash + fingerprint verification.
    Mphf(mphf::MphfIndex),
}

impl HydraIndex {
    /// Selected physical structure.
    pub fn kind(&self) -> IndexKind {
        match self {
            Self::Eytzinger(_) => IndexKind::Eytzinger,
            Self::Pgm(_) => IndexKind::Pgm,
            Self::RadixSpline(_) => IndexKind::RadixSpline,
            Self::CompressedRadix(_) => IndexKind::CompressedRadix,
            Self::Mphf(_) => IndexKind::Mphf,
        }
    }

    /// Number of keys.
    pub fn len(&self) -> usize {
        match self {
            Self::Eytzinger(i) => i.len(),
            Self::Pgm(i) => i.len(),
            Self::RadixSpline(i) => i.len(),
            Self::CompressedRadix(i) => i.len(),
            Self::Mphf(i) => i.len(),
        }
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Point lookup: returns the stored offset when the key is present.
    pub fn get(&self, key: &[u8]) -> Option<u64> {
        match self {
            Self::Eytzinger(i) => i.get(key),
            Self::Pgm(i) => i.get(key),
            Self::RadixSpline(i) => i.get(key),
            Self::CompressedRadix(i) => i.get(key),
            Self::Mphf(i) => i.get(key),
        }
    }

    /// Keys strictly after `after` (exclusive), up to `limit` results, in key order.
    ///
    /// MPHF indexes do not support ordered scan and return an empty vec.
    pub fn scan_after(&self, after: Option<&[u8]>, limit: usize) -> Vec<(Vec<u8>, u64)> {
        match self {
            Self::Eytzinger(i) => i.scan_after(after, limit),
            Self::Pgm(i) => i.scan_after(after, limit),
            Self::RadixSpline(i) => i.scan_after(after, limit),
            Self::CompressedRadix(i) => i.scan_after(after, limit),
            Self::Mphf(_) => Vec::new(),
        }
    }
}

/// Build a hydra index for one segment from unsorted records.
///
/// Records with duplicate keys keep the **last** occurrence (seal-time latest).
pub fn build(records: &[SegmentRecord], opts: &HydraBuildOptions) -> HydraIndex {
    let sorted = sort_unique(records);
    build_sorted(&sorted, opts)
}

/// Build when keys are already unique and sorted ascending by key.
pub fn build_sorted(sorted: &[(Vec<u8>, u64)], opts: &HydraBuildOptions) -> HydraIndex {
    let kind = select_index_kind(sorted, opts);
    match kind {
        IndexKind::Eytzinger => HydraIndex::Eytzinger(eytzinger::EytzingerIndex::build(sorted)),
        IndexKind::Pgm => HydraIndex::Pgm(pgm::PgmIndex::build(sorted, opts.pgm_epsilon)),
        IndexKind::RadixSpline => {
            HydraIndex::RadixSpline(pgm::RadixSplineIndex::build(sorted, opts.spline_radix_bits))
        }
        IndexKind::CompressedRadix => {
            HydraIndex::CompressedRadix(radix::CompressedRadixIndex::build(sorted))
        }
        IndexKind::Mphf => HydraIndex::Mphf(mphf::MphfIndex::build(sorted)),
    }
}

/// Multithreaded construction of many independent segment indexes.
///
/// Each batch is built on a worker; order of results matches input order.
/// Uses at most `opts.threads` workers (clamped to `1..=num_cpus_estimate`).
pub fn build_many(batches: &[Vec<SegmentRecord>], opts: &HydraBuildOptions) -> Vec<HydraIndex> {
    if batches.is_empty() {
        return Vec::new();
    }
    let threads = opts.effective_threads().min(batches.len()).max(1);
    if threads == 1 || batches.len() == 1 {
        return batches.iter().map(|b| build(b, opts)).collect();
    }

    let n = batches.len();
    let next = AtomicUsize::new(0);
    let out = std::sync::Mutex::new(vec![None; n]);
    let opts = opts.clone();

    thread::scope(|scope| {
        for _ in 0..threads {
            let next = &next;
            let out = &out;
            let opts = &opts;
            scope.spawn(move || loop {
                let i = next.fetch_add(1, AtomicOrdering::Relaxed);
                if i >= n {
                    break;
                }
                let idx = build(&batches[i], opts);
                out.lock().expect("hydra build_many lock")[i] = Some(idx);
            });
        }
    });

    out.into_inner()
        .expect("hydra build_many lock")
        .into_iter()
        .map(|o| o.expect("hydra build_many: every slot filled"))
        .collect()
}

/// Directory for per-segment hydra sidecars: `indexes/seg/`.
pub fn hydra_dir(paths: &StorePaths) -> PathBuf {
    paths.indexes_dir().join("seg")
}

/// Path of one segment hydra index: `indexes/seg/{hex16}.hdx`.
pub fn hydra_index_path(paths: &StorePaths, segment_id: &[u8; 16]) -> PathBuf {
    hydra_dir(paths).join(format!("{}.hdx", hex16(segment_id)))
}

const MAGIC: &[u8; 8] = b"RHYDRA01";
const VERSION: u32 = 1;

/// Persist a hydra index (atomic durable replace). Derived only.
pub fn write_hydra_index(
    path: &Path,
    store_id: [u8; 16],
    segment_id: [u8; 16],
    index: &HydraIndex,
) -> Result<(), StoreError> {
    let bytes = encode(store_id, segment_id, index);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    crate::atomic_file::write_atomic(path, &bytes)?;
    Ok(())
}

/// Load a hydra index when magic/version/store_id/segment_id match.
///
/// Returns `Ok(None)` for absent or unusable files (never blocks recovery).
pub fn try_load_hydra_index(
    path: &Path,
    store_id: [u8; 16],
    segment_id: [u8; 16],
) -> Result<Option<HydraIndex>, StoreError> {
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(_) => return Ok(None),
    };
    Ok(decode(&bytes, store_id, segment_id))
}

/// Delete one hydra sidecar (never touches authoritative segments).
pub fn delete_hydra_index(path: &Path) -> Result<(), StoreError> {
    if path.is_file() {
        fs::remove_file(path)?;
    }
    Ok(())
}

/// Extract unique (key → last offset) pairs from sealed segment bytes.
pub fn records_from_segment_bytes(
    bytes: &[u8],
    limits: residiuum_format::SafetyLimits,
) -> Vec<SegmentRecord> {
    use crate::envelope::decode_item_envelope;
    use residiuum_format::{scan_forward, FrameKind};

    let report = scan_forward(bytes, limits);
    let mut map: std::collections::BTreeMap<Vec<u8>, u64> = std::collections::BTreeMap::new();
    for (offset, frame) in report.verified_frames() {
        if frame.header.known_kind() != Some(FrameKind::ItemEvent) {
            continue;
        }
        let Some(env) = decode_item_envelope(&frame.envelope) else {
            continue;
        };
        // Latest frame for a subject wins (higher offset within the segment).
        map.insert(env.subject, offset);
    }
    map.into_iter()
        .map(|(key, offset)| SegmentRecord { key, offset })
        .collect()
}

fn sort_unique(records: &[SegmentRecord]) -> Vec<(Vec<u8>, u64)> {
    let mut map: std::collections::BTreeMap<Vec<u8>, u64> = std::collections::BTreeMap::new();
    for r in records {
        map.insert(r.key.clone(), r.offset);
    }
    map.into_iter().collect()
}

fn encode(store_id: [u8; 16], segment_id: [u8; 16], index: &HydraIndex) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + index.len() * 24);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&store_id);
    out.extend_from_slice(&segment_id);
    out.push(index.kind().as_u8());
    match index {
        HydraIndex::Eytzinger(i) => eytzinger::encode(i, &mut out),
        HydraIndex::Pgm(i) => pgm::encode_pgm(i, &mut out),
        HydraIndex::RadixSpline(i) => pgm::encode_rs(i, &mut out),
        HydraIndex::CompressedRadix(i) => radix::encode(i, &mut out),
        HydraIndex::Mphf(i) => mphf::encode(i, &mut out),
    }
    out
}

fn decode(bytes: &[u8], store_id: [u8; 16], segment_id: [u8; 16]) -> Option<HydraIndex> {
    if bytes.len() < 8 + 4 + 16 + 16 + 1 {
        return None;
    }
    if &bytes[0..8] != MAGIC.as_slice() {
        return None;
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
    if version != VERSION {
        return None;
    }
    if bytes[12..28] != store_id {
        return None;
    }
    if bytes[28..44] != segment_id {
        return None;
    }
    let kind = IndexKind::from_u8(bytes[44])?;
    let body = &bytes[45..];
    match kind {
        IndexKind::Eytzinger => Some(HydraIndex::Eytzinger(eytzinger::decode(body)?)),
        IndexKind::Pgm => Some(HydraIndex::Pgm(pgm::decode_pgm(body)?)),
        IndexKind::RadixSpline => Some(HydraIndex::RadixSpline(pgm::decode_rs(body)?)),
        IndexKind::CompressedRadix => Some(HydraIndex::CompressedRadix(radix::decode(body)?)),
        IndexKind::Mphf => Some(HydraIndex::Mphf(mphf::decode(body)?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn rec(k: &[u8], off: u64) -> SegmentRecord {
        SegmentRecord {
            key: k.to_vec(),
            offset: off,
        }
    }

    #[test]
    fn tiny_selects_eytzinger() {
        let records: Vec<_> = (0..8u64)
            .map(|i| rec(format!("k{i:02}").as_bytes(), i * 100))
            .collect();
        let idx = build(&records, &HydraBuildOptions::default());
        assert_eq!(idx.kind(), IndexKind::Eytzinger);
        for r in &records {
            assert_eq!(idx.get(&r.key), Some(r.offset));
        }
        assert!(idx.get(b"missing").is_none());
    }

    #[test]
    fn numeric_selects_pgm_or_spline() {
        let records: Vec<_> = (0..500u64)
            .map(|i| {
                // fixed-width u64 keys
                rec(&i.to_be_bytes(), i * 32)
            })
            .collect();
        let idx = build(&records, &HydraBuildOptions::default());
        assert!(
            matches!(idx.kind(), IndexKind::Pgm | IndexKind::RadixSpline),
            "got {:?}",
            idx.kind()
        );
        for r in &records {
            assert_eq!(idx.get(&r.key), Some(r.offset));
        }
        let miss = 9999u64.to_be_bytes();
        assert!(idx.get(&miss).is_none());
    }

    #[test]
    fn strings_select_compressed_radix() {
        let records: Vec<_> = (0..200u64)
            .map(|i| {
                // Irregular-length string keys.
                let k = format!("user/{i}/profile");
                rec(k.as_bytes(), i * 10)
            })
            .collect();
        let idx = build(&records, &HydraBuildOptions::default());
        assert_eq!(idx.kind(), IndexKind::CompressedRadix);
        for r in &records {
            assert_eq!(idx.get(&r.key), Some(r.offset));
        }
        assert!(idx.get(b"user/x/profile").is_none());
    }

    #[test]
    fn point_only_selects_mphf() {
        let records: Vec<_> = (0..300u64)
            .map(|i| rec(format!("id-{i}").as_bytes(), i))
            .collect();
        let opts = HydraBuildOptions {
            point_only: true,
            ..Default::default()
        };
        let idx = build(&records, &opts);
        assert_eq!(idx.kind(), IndexKind::Mphf);
        for r in &records {
            assert_eq!(idx.get(&r.key), Some(r.offset));
        }
        assert!(idx.get(b"id-missing").is_none());
        // Ordered scan unsupported.
        assert!(idx.scan_after(None, 10).is_empty());
    }

    #[test]
    fn eytzinger_scan_order() {
        let records: Vec<_> = (0..16u64)
            .map(|i| rec(format!("{i:02}").as_bytes(), i))
            .collect();
        let idx = build(&records, &HydraBuildOptions::default());
        let page = idx.scan_after(Some(b"05"), 4);
        assert_eq!(
            page.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
            vec![
                b"06".to_vec(),
                b"07".to_vec(),
                b"08".to_vec(),
                b"09".to_vec()
            ]
        );
    }

    #[test]
    fn build_many_parallel_preserves_order() {
        let mut batches = Vec::new();
        for b in 0..8u64 {
            let mut batch = Vec::new();
            for i in 0..40u64 {
                batch.push(rec(format!("b{b}-k{i:03}").as_bytes(), b * 1000 + i));
            }
            batches.push(batch);
        }
        let opts = HydraBuildOptions {
            threads: 4,
            ..Default::default()
        };
        let built = build_many(&batches, &opts);
        assert_eq!(built.len(), 8);
        for (b, idx) in built.iter().enumerate() {
            for i in 0..40u64 {
                let k = format!("b{b}-k{i:03}");
                assert_eq!(idx.get(k.as_bytes()), Some(b as u64 * 1000 + i));
            }
        }
    }

    #[test]
    fn codec_roundtrip_all_kinds() {
        let cases: Vec<(HydraBuildOptions, Vec<SegmentRecord>)> = vec![
            (
                HydraBuildOptions::default(),
                (0..10u64)
                    .map(|i| rec(format!("t{i}").as_bytes(), i))
                    .collect(),
            ),
            (
                HydraBuildOptions::default(),
                (0..400u64).map(|i| rec(&i.to_be_bytes(), i * 8)).collect(),
            ),
            (
                HydraBuildOptions::default(),
                (0..150u64)
                    .map(|i| rec(format!("str-{i}-x").as_bytes(), i))
                    .collect(),
            ),
            (
                HydraBuildOptions {
                    point_only: true,
                    ..Default::default()
                },
                (0..250u64)
                    .map(|i| rec(format!("p{i}").as_bytes(), i))
                    .collect(),
            ),
        ];
        let store_id = [1u8; 16];
        let seg_id = [2u8; 16];
        for (opts, records) in cases {
            let idx = build(&records, &opts);
            let bytes = encode(store_id, seg_id, &idx);
            let dec = decode(&bytes, store_id, seg_id).expect("decode");
            assert_eq!(dec.kind(), idx.kind());
            assert_eq!(dec.len(), idx.len());
            for r in &records {
                assert_eq!(dec.get(&r.key), Some(r.offset));
            }
        }
    }

    #[test]
    fn write_load_sidecar() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.indexes_dir()).unwrap();
        let store_id = [9u8; 16];
        let seg_id = [7u8; 16];
        let records: Vec<_> = (0..20u64)
            .map(|i| rec(format!("k{i:02}").as_bytes(), i * 50))
            .collect();
        let idx = build(&records, &HydraBuildOptions::default());
        let path = hydra_index_path(&paths, &seg_id);
        write_hydra_index(&path, store_id, seg_id, &idx).unwrap();
        let loaded = try_load_hydra_index(&path, store_id, seg_id)
            .unwrap()
            .expect("present");
        assert_eq!(loaded.get(b"k05"), Some(250));
        assert!(try_load_hydra_index(&path, [0u8; 16], seg_id)
            .unwrap()
            .is_none());
    }

    #[test]
    fn last_duplicate_wins() {
        let records = vec![rec(b"a", 1), rec(b"b", 2), rec(b"a", 99), rec(b"b", 3)];
        let idx = build(&records, &HydraBuildOptions::default());
        assert_eq!(idx.get(b"a"), Some(99));
        assert_eq!(idx.get(b"b"), Some(3));
        assert_eq!(idx.len(), 2);
    }
}
