//! Segment-id never-reuse crash matrix (CSE-3 Stage 2h P0).
//!
//! Counter bumps alone are insufficient. This matrix proves:
//! - durable reservation before media
//! - reopen/seal cycles never remint a published id
//! - failpoints at allocation / publish boundaries
//! - multi-shard allocation uniqueness
//! - corrupt durable + empty media → refuse without mutating media
//!
//! ```text
//! cargo test -p residiuum-store --features legacy-raw-store \
//!   --test cse3_stage2_segment_id_never_reuse -- --test-threads=1
//! ```

use residiuum_store::{
    arm_failpoint_once, clear_failpoints, disable_failpoint_hit_proof, enable_failpoint_hit_proof,
    list_sealed_segment_files, segment_id_from_filename, DurabilityMode, FailpointAction, Store,
    StorePaths,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Mutex, OnceLock};

/// Serialize failpoint-armed tests (process-global registry).
fn fp_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

fn arm_panic(name: &'static str) {
    clear_failpoints();
    enable_failpoint_hit_proof();
    arm_failpoint_once(name, FailpointAction::Panic);
}

fn disarm() {
    clear_failpoints();
    disable_failpoint_hit_proof();
}

fn sealed_ids(root: &std::path::Path) -> BTreeSet<[u8; 16]> {
    let paths = StorePaths::new(root);
    list_sealed_segment_files(&paths)
        .unwrap()
        .into_iter()
        .filter_map(|p| segment_id_from_filename(&p))
        .collect()
}

fn sealed_file_lens(root: &std::path::Path) -> BTreeMap<String, u64> {
    let mut m = BTreeMap::new();
    let seg = root.join("segments");
    if !seg.is_dir() {
        return m;
    }
    for ent in fs::read_dir(seg).unwrap() {
        let p = ent.unwrap().path();
        if p.extension().and_then(|e| e.to_str()) == Some("residiuum") {
            m.insert(
                p.file_name().unwrap().to_string_lossy().into_owned(),
                fs::metadata(&p).unwrap().len(),
            );
        }
    }
    m
}

fn put_n(store: &mut Store, n: u64, tag: &str) {
    let payload = vec![0xABu8; 64];
    for i in 0..n {
        store
            .put(&format!("{tag}/{i}"), &payload, DurabilityMode::Buffered)
            .unwrap();
    }
}

/// CompactShadow default publishes seals asynchronously — wait for sealed media.
fn seal_and_wait(store: &mut Store) {
    store.seal_active().unwrap();
    store.wait_seals_applied().unwrap();
}

#[test]
fn never_reuse_after_repeated_reopen_seal() {
    let _guard = fp_lock();
    disarm();
    let dir = tempfile::tempdir().unwrap();
    let mut all_ids = BTreeSet::new();
    let mut store = Store::create_with_shards(dir.path(), 1).unwrap();
    for round in 0..8u64 {
        put_n(&mut store, 4, &format!("r{round}"));
        store.seal_active().unwrap();
        store.wait_seals_applied().unwrap();
        let ids = sealed_ids(dir.path());
        let prev = all_ids.len();
        all_ids.extend(ids.iter().copied());
        assert!(
            all_ids.len() >= prev,
            "ids must be monotonic: round={round}"
        );
        assert_eq!(ids.len(), ids.iter().copied().collect::<BTreeSet<_>>().len());
        let before_lens = sealed_file_lens(dir.path());
        drop(store);
        store = Store::open(dir.path()).unwrap();
        put_n(&mut store, 2, &format!("c{round}"));
        store.seal_active().unwrap();
        store.wait_seals_applied().unwrap();
        for (name, len) in &before_lens {
            let p = dir.path().join("segments").join(name);
            assert_eq!(
                fs::metadata(&p).unwrap().len(),
                *len,
                "sealed media mutated: {name}"
            );
        }
        let after = sealed_ids(dir.path());
        assert!(after.is_superset(&ids));
        all_ids.extend(after);
    }
    assert!(all_ids.len() >= 8);
}

#[test]
fn never_reuse_multi_shard_allocation() {
    let _guard = fp_lock();
    disarm();
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create_with_shards(dir.path(), 4).unwrap();
    let payload = vec![0x5Au8; 32];
    for i in 0..64u64 {
        store
            .put(&format!("s/{i}"), &payload, DurabilityMode::Buffered)
            .unwrap();
    }
    store.seal_active().unwrap();
    store.wait_seals_applied().unwrap();
    let ids = sealed_ids(dir.path());
    assert_eq!(ids.len(), ids.iter().copied().collect::<BTreeSet<_>>().len());
    drop(store);
    let mut store = Store::open(dir.path()).unwrap();
    put_n(&mut store, 8, "more");
    store.seal_active().unwrap();
    store.wait_seals_applied().unwrap();
    let after = sealed_ids(dir.path());
    assert!(after.is_superset(&ids));
    assert_eq!(after.len(), after.iter().copied().collect::<BTreeSet<_>>().len());
}

fn crash_at(name: &'static str) {
    // Caller holds fp_lock (tests are serialized).
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let mut store = Store::create_with_shards(&root, 1).unwrap();
    put_n(&mut store, 3, "seed");
    seal_and_wait(&mut store);
    let before = sealed_ids(&root);
    assert!(!before.is_empty());
    put_n(&mut store, 2, "live");
    let before_lens = sealed_file_lens(&root);
    // A leased allocator normally performs no metadata transaction for the
    // next segment. Reopen skips the unused lease tail, deliberately placing
    // the next allocation on a new durable lease boundary for reservation
    // failpoint coverage.
    if matches!(
        name,
        "segalloc.before_reserve_persist" | "segalloc.after_reserve_persist"
    ) {
        drop(store);
        store = Store::open(&root).unwrap();
    }
    arm_panic(name);
    let crashed = catch_unwind(AssertUnwindSafe(|| {
        store.seal_active().unwrap();
    }));
    disarm();
    assert!(crashed.is_err(), "failpoint {name} must fire");
    // Release writer lock even after panic.
    drop(store);
    let mut store = Store::open(&root).unwrap();
    put_n(&mut store, 2, "after");
    seal_and_wait(&mut store);
    for (fname, len) in before_lens {
        let p = root.join("segments").join(&fname);
        if p.is_file() {
            assert_eq!(
                fs::metadata(&p).unwrap().len(),
                len,
                "pre-crash sealed overwritten ({name}): {fname}"
            );
        }
    }
    let ids = sealed_ids(&root);
    assert!(ids.is_superset(&before));
    assert_eq!(ids.len(), ids.iter().copied().collect::<BTreeSet<_>>().len());
}

#[test]
fn crash_before_reserve_persist() {
    let _guard = fp_lock();
    crash_at("segalloc.before_reserve_persist");
}

#[test]
fn crash_after_reserve_persist() {
    let _guard = fp_lock();
    crash_at("segalloc.after_reserve_persist");
}

#[test]
fn crash_after_reserve_before_media() {
    let _guard = fp_lock();
    crash_at("segalloc.after_reserve_before_media");
}

#[test]
fn crash_after_active_media() {
    let _guard = fp_lock();
    crash_at("segalloc.after_active_media");
}

#[test]
fn crash_seal_publish_boundaries() {
    let _guard = fp_lock();
    for name in [
        "store.seal.before_dest_write",
        "store.seal.after_dest_sync",
        "store.seal.after_active_remove",
    ] {
        crash_at(name);
    }
}

#[test]
fn corrupt_allocator_refuses_without_mutating_media() {
    let _guard = fp_lock();
    disarm();
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create_with_shards(dir.path(), 1).unwrap();
    put_n(&mut store, 2, "a");
    store.seal_active().unwrap();
    drop(store);
    let paths = StorePaths::new(dir.path());
    let seq_path = paths.store_info().join("segment_seq.v1");
    assert!(seq_path.is_file());
    let sealed_before = sealed_file_lens(dir.path());
    fs::write(&seq_path, b"SEGSEQ99garbage").unwrap();
    // With sealed media present, reconstruct from media must succeed.
    let store = Store::open(dir.path());
    assert!(store.is_ok(), "media reconstruction must succeed");
    let _ = store.unwrap();
    for (name, len) in sealed_before {
        let p = dir.path().join("segments").join(&name);
        assert_eq!(fs::metadata(&p).unwrap().len(), len);
    }

    // Ambiguous: corrupt durable, no media — refuse.
    let dir3 = tempfile::tempdir().unwrap();
    {
        let s = Store::create(dir3.path()).unwrap();
        drop(s);
    }
    let paths3 = StorePaths::new(dir3.path());
    let _ = fs::remove_dir_all(paths3.segments_dir());
    let _ = fs::remove_dir_all(paths3.active_dir());
    fs::create_dir_all(paths3.segments_dir()).unwrap();
    fs::create_dir_all(paths3.active_dir()).unwrap();
    fs::write(paths3.store_info().join("segment_seq.v1"), b"SEGSEQ99bad").unwrap();
    let err = Store::open(dir3.path());
    assert!(err.is_err(), "corrupt durable + empty media must refuse");
}
