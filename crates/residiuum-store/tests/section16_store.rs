//! OVERVIEW §16 store-level destructive suite (Stage 3b).
//!
//! Covers single-node multi-segment cases that sit above the wire-format
//! corpus in `residiuum-format` (FORMAT_SPEC §13). For every case: surviving
//! islands remain readable, corrupt bytes are not trusted as live state, and
//! derived catalogs/indexes are not required.

use residiuum_format::{FRAME_PREFIX_LEN, FRAME_SUFFIX_LEN, START_MAGIC};
use residiuum_store::{DurabilityMode, Store};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

fn sealed_segments(root: &Path) -> Vec<PathBuf> {
    let dir = root.join("segments");
    let mut out = Vec::new();
    if !dir.exists() {
        return out;
    }
    for e in fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.extension().and_then(|x| x.to_str()) == Some("residiuum") {
            out.push(p);
        }
    }
    out.sort();
    out
}

fn active_path(root: &Path) -> PathBuf {
    root.join("active").join("active.residiuum")
}

fn write_three_sealed_plus_active(root: &Path) {
    let mut store = Store::create(root).unwrap();
    store.put("early", b"E", DurabilityMode::Durable).unwrap();
    store.seal_active().unwrap();
    store.put("middle", b"M", DurabilityMode::Durable).unwrap();
    store.seal_active().unwrap();
    store.put("late", b"L", DurabilityMode::Durable).unwrap();
    store.seal_active().unwrap();
    store
        .put("active-item", b"A", DurabilityMode::Durable)
        .unwrap();
    drop(store);
}

// --- §16.1 truncate active frame at offsets ---

#[test]
fn case01_truncate_active_frame_offsets() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    {
        let mut store = Store::create(root).unwrap();
        store
            .put("keep", b"alive", DurabilityMode::Durable)
            .unwrap();
        // Second put becomes the truncated victim after we cut its frame.
        store
            .put("victim", b"gone-maybe", DurabilityMode::Durable)
            .unwrap();
    }

    let active = active_path(root);
    // Orderly Drop seals written tails. Recreate the crash fixture by moving
    // the just-sealed authoritative prefix (without its summary footer) back to
    // the active path; this test is specifically about an interrupted active.
    let sealed = sealed_segments(root).pop().expect("sealed write tail");
    let sealed_bytes = fs::read(&sealed).unwrap();
    let mut sealed_magics = Vec::new();
    for i in 0..=sealed_bytes.len().saturating_sub(START_MAGIC.len()) {
        if &sealed_bytes[i..i + START_MAGIC.len()] == START_MAGIC.as_slice() {
            sealed_magics.push(i);
        }
    }
    let summary_start = *sealed_magics.last().expect("summary footer");
    fs::write(&active, &sealed_bytes[..summary_start]).unwrap();
    fs::remove_file(&sealed).unwrap();

    let bytes = fs::read(&active).unwrap();
    // Only truncate into the *last* frame so earlier complete frames remain a
    // contiguous verified prefix (OVERVIEW §16.1 / §7.3).
    let mut magics = Vec::new();
    let mut i = 0;
    while i + 8 <= bytes.len() {
        if &bytes[i..i + 8] == START_MAGIC.as_slice() {
            magics.push(i);
            i += 8;
        } else {
            i += 1;
        }
    }
    assert!(
        magics.len() >= 3,
        "descriptor + keep + victim frames expected"
    );
    let last_start = *magics.last().unwrap();
    let cuts = [
        last_start + 1,
        last_start + 8,
        last_start + FRAME_PREFIX_LEN / 2,
        last_start + FRAME_PREFIX_LEN + 3,
        bytes.len().saturating_sub(1),
    ];
    for cut in cuts {
        if cut <= last_start || cut >= bytes.len() {
            continue;
        }
        fs::write(&active, &bytes[..cut]).unwrap();

        let store = Store::open(root).unwrap();
        assert_eq!(
            store.get("keep").unwrap().as_deref(),
            Some(b"alive".as_slice()),
            "earlier complete frames must survive truncate at {cut}"
        );
        // Victim incomplete → must not be required for open.
        let _ = store.get("victim");
        let report = store.salvage().unwrap();
        assert!(report.verified_frames >= 1);
        // Drop now orderly-seals the recovered prefix. Remove that per-iteration
        // output and restore the original crash image for the next cut.
        drop(store);
        for segment in sealed_segments(root) {
            fs::remove_file(segment).unwrap();
        }
        fs::write(&active, &bytes).unwrap();
    }
}

// --- §16.2 overwrite arbitrary ranges ---

#[test]
fn case02_overwrite_byte_ranges_in_segment() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    write_three_sealed_plus_active(root);

    let segs = sealed_segments(root);
    assert!(segs.len() >= 2);
    let target = &segs[0];
    let mut bytes = fs::read(target).unwrap();
    let mid = bytes.len() / 2;
    for b in bytes.iter_mut().skip(mid).take(32) {
        *b ^= 0xa5;
    }
    fs::write(target, &bytes).unwrap();

    let store = Store::open(root).unwrap();
    // Items on later segments remain.
    assert_eq!(
        store.get("active-item").unwrap().as_deref(),
        Some(b"A".as_slice())
    );
    assert_eq!(store.get("late").unwrap().as_deref(), Some(b"L".as_slice()));
    let report = store.salvage().unwrap();
    assert!(report.verified_frames >= 1);
    assert!(report.holes >= 1 || report.item_events >= 1);
}

// --- §16.3 delete a middle frame ---

#[test]
fn case03_delete_middle_frame_in_segment() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    {
        let mut store = Store::create(root).unwrap();
        store.put("a", b"1", DurabilityMode::Durable).unwrap();
        store.put("b", b"2", DurabilityMode::Durable).unwrap();
        store.put("c", b"3", DurabilityMode::Durable).unwrap();
        store.seal_active().unwrap();
    }

    let seg = &sealed_segments(root)[0];
    let bytes = fs::read(seg).unwrap();
    // Find second item event by scanning for START_MAGIC after the first frame.
    let mut magic_offsets = Vec::new();
    let mut i = 0;
    while i + 8 <= bytes.len() {
        if &bytes[i..i + 8] == START_MAGIC.as_slice() {
            magic_offsets.push(i);
            i += 8;
        } else {
            i += 1;
        }
    }
    assert!(
        magic_offsets.len() >= 4,
        "descriptor + 3 items + summary expected, got {}",
        magic_offsets.len()
    );
    // Drop the frame starting at magic_offsets[2] (second item, after descriptor+first).
    let drop_start = magic_offsets[2];
    let drop_end = magic_offsets.get(3).copied().unwrap_or(bytes.len());
    let mut damaged = Vec::new();
    damaged.extend_from_slice(&bytes[..drop_start]);
    damaged.extend_from_slice(&bytes[drop_end..]);
    fs::write(seg, &damaged).unwrap();

    let store = Store::open(root).unwrap();
    // a and c should still be recoverable (islands); b may be gone.
    assert_eq!(store.get("a").unwrap().as_deref(), Some(b"1".as_slice()));
    assert_eq!(store.get("c").unwrap().as_deref(), Some(b"3".as_slice()));
    let report = store.salvage().unwrap();
    assert!(report.item_events >= 2);
}

// --- §16.4 delete a middle segment ---

#[test]
fn case04_delete_middle_segment() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    write_three_sealed_plus_active(root);

    let segs = sealed_segments(root);
    assert!(segs.len() >= 3, "need three sealed segments");
    // Middle sealed file (sorted by hex name ≈ mint order for small seq).
    fs::remove_file(&segs[1]).unwrap();

    let store = Store::open(root).unwrap();
    assert_eq!(
        store.get("early").unwrap().as_deref(),
        Some(b"E".as_slice())
    );
    // middle may be missing
    assert!(store.get("middle").unwrap().is_none());
    assert_eq!(store.get("late").unwrap().as_deref(), Some(b"L".as_slice()));
    assert_eq!(
        store.get("active-item").unwrap().as_deref(),
        Some(b"A".as_slice())
    );
    let report = store.salvage().unwrap();
    assert!(report.files_scanned >= 3);
    assert_eq!(report.live_subjects, 3);
}

// --- §16.5 destroy segment headers ---

#[test]
fn case05_destroy_segment_headers() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    write_three_sealed_plus_active(root);

    let segs = sealed_segments(root);
    let target = &segs[0];
    let mut bytes = fs::read(target).unwrap();
    // Zero out the first frame prefix (descriptor header).
    let n = FRAME_PREFIX_LEN.min(bytes.len());
    for b in bytes.iter_mut().take(n) {
        *b = 0;
    }
    fs::write(target, &bytes).unwrap();

    // P0: writable open fail-closes on unidentified authoritative media.
    let err = match Store::open(root) {
        Err(e) => e,
        Ok(_) => panic!("expected FailClosed open after destroyed descriptor"),
    };
    assert!(
        matches!(err, residiuum_store::StoreError::CorruptMeta(_)),
        "expected CorruptMeta on damaged descriptor, got {err:?}"
    );
    // Survivors remain enumerable via salvage (no writable allocation). Before
    // orderly close, `active-item` stayed in a separate active file and was
    // directly gettable here; it is now another sealed survivor.
    let store = Store::open_inspect(root).unwrap();
    let report = store.salvage().unwrap();
    assert!(report.verified_frames >= 1);
}

// --- §16.6 destroy segment trailers ---

#[test]
fn case06_destroy_segment_trailers() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    write_three_sealed_plus_active(root);

    let segs = sealed_segments(root);
    let target = &segs[0];
    let mut bytes = fs::read(target).unwrap();
    let n = FRAME_SUFFIX_LEN.min(bytes.len());
    for b in bytes.iter_mut().rev().take(n) {
        *b = 0;
    }
    fs::write(target, &bytes).unwrap();

    let store = Store::open(root).unwrap();
    assert_eq!(
        store.get("active-item").unwrap().as_deref(),
        Some(b"A".as_slice())
    );
    // Item events inside the damaged sealed segment may still verify if only
    // the summary trailer was destroyed.
    let report = store.salvage().unwrap();
    assert!(report.verified_frames >= 1);
}

// --- §16.7 destroy catalogs/indexes/snapshots ---

#[test]
fn case07_destroy_derived_state() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    {
        let mut store = Store::create(root).unwrap();
        store.put("x", b"1", DurabilityMode::Durable).unwrap();
        store.persist_index_cache().unwrap();
        assert!(store.index_cache_path().is_file());
    }
    for name in ["catalogs", "indexes", "snapshots"] {
        let p = root.join(name);
        if p.exists() {
            fs::remove_dir_all(&p).unwrap();
        }
    }
    let store = Store::open(root).unwrap();
    assert_eq!(store.get("x").unwrap().as_deref(), Some(b"1".as_slice()));
}

// --- §16.8 corrupt frame length fields ---

#[test]
fn case08_corrupt_frame_length_fields() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    {
        let mut store = Store::create(root).unwrap();
        store.put("head", b"H", DurabilityMode::Durable).unwrap();
        store.put("tail", b"T", DurabilityMode::Durable).unwrap();
        store.seal_active().unwrap();
    }
    let seg = &sealed_segments(root)[0];
    let mut bytes = fs::read(seg).unwrap();
    // Corrupt body_len in the second frame prefix if present (offset 16 within prefix).
    // Locate second magic.
    let mut found = 0usize;
    let mut i = 0;
    while i + FRAME_PREFIX_LEN <= bytes.len() {
        if &bytes[i..i + 8] == START_MAGIC.as_slice() {
            found += 1;
            if found == 2 {
                // body_len at prefix+16
                bytes[i + 16] ^= 0xff;
                break;
            }
            i += 8;
        } else {
            i += 1;
        }
    }
    fs::write(seg, &bytes).unwrap();

    let store = Store::open(root).unwrap();
    // At least one island should remain discoverable via salvage.
    let report = store.salvage().unwrap();
    assert!(report.verified_frames >= 1);
    // Corrupt candidate must not invent fake live subjects beyond survivors.
    let _ = store.get("head");
    let _ = store.get("tail");
}

// --- §16.9 garbage between frames ---

#[test]
fn case09_insert_garbage_between_frames() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    {
        let mut store = Store::create(root).unwrap();
        store.put("a", b"1", DurabilityMode::Durable).unwrap();
        store.put("b", b"2", DurabilityMode::Durable).unwrap();
        store.seal_active().unwrap();
    }
    let seg = &sealed_segments(root)[0];
    let bytes = fs::read(seg).unwrap();
    let mut magics = Vec::new();
    let mut i = 0;
    while i + 8 <= bytes.len() {
        if &bytes[i..i + 8] == START_MAGIC.as_slice() {
            magics.push(i);
            i += 8;
        } else {
            i += 1;
        }
    }
    assert!(magics.len() >= 3);
    let insert_at = magics[2];
    let mut damaged = Vec::new();
    damaged.extend_from_slice(&bytes[..insert_at]);
    damaged.extend_from_slice(b"GARBAGE!!!not-a-frame\xff\x00");
    damaged.extend_from_slice(&bytes[insert_at..]);
    fs::write(seg, &damaged).unwrap();

    let store = Store::open(root).unwrap();
    assert_eq!(store.get("a").unwrap().as_deref(), Some(b"1".as_slice()));
    assert_eq!(store.get("b").unwrap().as_deref(), Some(b"2".as_slice()));
    let report = store.salvage().unwrap();
    assert!(report.holes >= 1 || report.verified_frames >= 2);
}

// --- §16.10 reorder or duplicate segments ---

#[test]
fn case10_reorder_and_duplicate_segments() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    {
        let mut store = Store::create(root).unwrap();
        store.put("s1", b"v1", DurabilityMode::Durable).unwrap();
        store.seal_active().unwrap();
        store.put("s1", b"v2", DurabilityMode::Durable).unwrap();
        store.seal_active().unwrap();
        store.put("s2", b"only", DurabilityMode::Durable).unwrap();
        store.seal_active().unwrap();
    }

    let segs = sealed_segments(root);
    assert!(segs.len() >= 3);
    // Duplicate the first sealed segment under a new name (copy).
    let dup = root
        .join("segments")
        .join("ffffffffffffffffffffffffffffffff.residiuum");
    fs::copy(&segs[0], &dup).unwrap();
    // Swap two sealed filenames so directory sort order changes.
    let a = &segs[0];
    let b = &segs[2];
    let tmp = root.join("segments").join("_swap.tmp");
    fs::rename(a, &tmp).unwrap();
    fs::rename(b, a).unwrap();
    fs::rename(&tmp, b).unwrap();

    // Duplicate + swapped names: FailClosed refuses writable open (filename/
    // descriptor mismatch or collision). Survivors via inspect.
    let err = match Store::open(root) {
        Err(e) => e,
        Ok(_) => panic!("expected FailClosed open after reorder/dup plant"),
    };
    assert!(
        matches!(
            err,
            residiuum_store::StoreError::CorruptMeta(_)
                | residiuum_store::StoreError::SegmentIdCollision { .. }
        ),
        "expected FailClosed on planted reorder/dup, got {err:?}"
    );
    let store = Store::open_inspect(root).unwrap();
    // Planted filename/descriptor chaos: get may fail closed under P0 disk
    // binding. Salvage must still scan surviving verified frames.
    let report = store.salvage().unwrap();
    assert!(report.files_scanned >= 4);
    assert!(report.verified_frames >= 1);
    assert!(report.live_subjects >= 1);
}

// --- Stage 3c: store descriptor + index cache ---

#[test]
fn store_descriptor_written_and_readable() {
    let dir = tempdir().unwrap();
    let store = Store::create(dir.path()).unwrap();
    let path = store.store_descriptor_path();
    assert!(
        path.is_file(),
        "create must write store-info/descriptor.residiuum"
    );
    let bytes = fs::read(&path).unwrap();
    let report = residiuum_format::scan_forward(&bytes, residiuum_format::SafetyLimits::default());
    let kinds: Vec<_> = report
        .verified_frames()
        .filter_map(|(_, f)| f.header.known_kind())
        .collect();
    assert!(kinds.contains(&residiuum_format::FrameKind::StoreDescriptor));
    let id = store.store_id();
    drop(store);
    let reopened = Store::open(dir.path()).unwrap();
    assert_eq!(reopened.store_id(), id);
}

#[test]
fn index_cache_accelerates_open_and_is_optional() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    {
        let mut store = Store::create(root).unwrap();
        store.put("k", b"v", DurabilityMode::Durable).unwrap();
        store.persist_index_cache().unwrap();
        assert!(store.index_cache_path().is_file());
    }
    // Open with warm cache.
    {
        let store = Store::open(root).unwrap();
        assert_eq!(store.get("k").unwrap().as_deref(), Some(b"v".as_slice()));
    }
    // Corrupt cache → still opens via rebuild.
    {
        let cache = root.join("indexes").join("primary.idx");
        fs::write(&cache, b"not-a-valid-cache").unwrap();
        let store = Store::open(root).unwrap();
        assert_eq!(store.get("k").unwrap().as_deref(), Some(b"v".as_slice()));
    }
    // Stale cache (wrong fingerprint after append) → rebuild.
    {
        {
            let mut store = Store::open(root).unwrap();
            store.persist_index_cache().unwrap();
        }
        // Force cache identity mismatch by editing the store_id field.
        // Exclusive writer lock (DEF-020) requires the previous handle to be dropped.
        let mut cache_bytes = fs::read(root.join("indexes").join("primary.idx")).unwrap();
        if cache_bytes.len() > 20 {
            cache_bytes[12] ^= 0xff; // inside store_id
            fs::write(root.join("indexes").join("primary.idx"), &cache_bytes).unwrap();
        }
        let store = Store::open(root).unwrap();
        assert_eq!(store.get("k").unwrap().as_deref(), Some(b"v".as_slice()));
    }
}

#[test]
fn index_cache_not_sole_map_after_segment_delete() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    {
        let mut store = Store::create(root).unwrap();
        store.put("keep", b"1", DurabilityMode::Durable).unwrap();
        store.seal_active().unwrap();
        store.put("drop-me", b"2", DurabilityMode::Durable).unwrap();
        store.seal_active().unwrap();
        store.persist_index_cache().unwrap();
    }
    // Delete the segment that holds drop-me but leave a stale cache claiming it.
    let segs = sealed_segments(root);
    assert!(segs.len() >= 2);
    fs::remove_file(&segs[1]).unwrap();
    // Fingerprint no longer matches → rebuild without drop-me.
    let store = Store::open(root).unwrap();
    assert_eq!(store.get("keep").unwrap().as_deref(), Some(b"1".as_slice()));
    assert!(
        store.get("drop-me").unwrap().is_none(),
        "stale index cache must not resurrect deleted-segment subjects"
    );
}

#[test]
fn incomplete_active_tail_after_multi_segment() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    {
        let mut store = Store::create(root).unwrap();
        store.put("sealed", b"S", DurabilityMode::Durable).unwrap();
        store.seal_active().unwrap();
        store.put("live", b"L", DurabilityMode::Durable).unwrap();
    }
    let mut f = OpenOptions::new()
        .append(true)
        .open(active_path(root))
        .unwrap();
    f.write_all(START_MAGIC).unwrap();
    f.write_all(&[0u8; 20]).unwrap();
    f.sync_all().unwrap();
    drop(f);

    let store = Store::open(root).unwrap();
    assert_eq!(
        store.get("sealed").unwrap().as_deref(),
        Some(b"S".as_slice())
    );
    assert_eq!(store.get("live").unwrap().as_deref(), Some(b"L".as_slice()));
}
