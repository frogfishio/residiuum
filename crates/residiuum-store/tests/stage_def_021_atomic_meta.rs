//! DEF-021 — atomic durable metadata writes + previous-generation recovery.

use residiuum_store::{
    arm_failpoint, clear_failpoints, previous_path, write_atomic, write_atomic_keep_previous,
    DurabilityMode, FailpointAction, LifecyclePolicy, LifecycleRule, Store, StoreError,
    PRIMARY_CACHE_FILE,
};
use std::fs;
use std::sync::{Mutex, MutexGuard, OnceLock};
use tempfile::tempdir;

/// Serialize this suite: failpoints are process-global (shared with DEF-022).
fn suite_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let g = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    clear_failpoints();
    g
}

#[test]
fn atomic_write_replaces_without_torn_primary() {
    let _g = suite_lock();
    let dir = tempdir().unwrap();
    let path = dir.path().join("control.bin");
    write_atomic(&path, b"generation-a").unwrap();
    write_atomic(&path, b"generation-b").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"generation-b");
}

#[test]
fn keep_previous_and_recover_from_prev() {
    let _g = suite_lock();
    let dir = tempdir().unwrap();
    let path = dir.path().join("write_dedup.v1");
    write_atomic_keep_previous(&path, b"old-good").unwrap();
    write_atomic_keep_previous(&path, b"new-good").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"new-good");
    assert_eq!(fs::read(previous_path(&path)).unwrap(), b"old-good");

    // Damage primary; consumers of keep_previous loads should still see old-good.
    fs::write(&path, b"\xff\xff torn").unwrap();
    assert_eq!(fs::read(previous_path(&path)).unwrap(), b"old-good");
}

#[test]
fn store_meta_writes_survive_reopen() {
    let _g = suite_lock();
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    {
        let mut store = Store::create(&root).unwrap();
        store.put("k", b"v", DurabilityMode::Durable).unwrap();
    }
    // Identity files present.
    assert!(root.join("store-info").join("store_id").is_file());
    assert!(root.join("store-info").join("meta").is_file());
    assert!(root
        .join("store-info")
        .join("descriptor.residiuum")
        .is_file());

    let store = Store::open(&root).unwrap();
    assert_eq!(store.get("k").unwrap().as_deref(), Some(b"v".as_slice()));
}

#[test]
fn index_cache_and_catalog_use_atomic_helper() {
    let _g = suite_lock();
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    store.put("a", b"1", DurabilityMode::Durable).unwrap();
    // Derived caches written after durable put.
    let idx = root.join("indexes").join(PRIMARY_CACHE_FILE);
    assert!(idx.is_file(), "primary index cache should exist");
    // Replacing with garbage must not prevent open (rebuild path).
    fs::write(&idx, b"not-a-cache").unwrap();
    drop(store);
    let store = Store::open(&root).unwrap();
    assert_eq!(store.get("a").unwrap().as_deref(), Some(b"1".as_slice()));
}

#[test]
fn lifecycle_policy_keeps_previous_and_loads_fallback() {
    let _g = suite_lock();
    let dir = tempdir().unwrap();
    let mut pol = LifecyclePolicy::new();
    pol.rules.push(LifecycleRule {
        name: "r1".into(),
        from_tier: "hot".into(),
        to_tier: "warm".into(),
        min_age_secs: 60,
        delete_source: false,
    });
    pol.save(dir.path()).unwrap();

    pol.rules[0].name = "r2".into();
    pol.save(dir.path()).unwrap();

    let path = dir.path().join("tiers").join("lifecycle.json");
    assert!(previous_path(&path).is_file());

    // Corrupt primary → load previous.
    fs::write(&path, b"{bad").unwrap();
    let loaded = LifecyclePolicy::load(dir.path()).unwrap();
    assert_eq!(loaded.rules[0].name, "r1");
}

#[test]
fn write_dedup_journal_replays_complete_prefix_after_torn_tail() {
    let _g = suite_lock();
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    {
        let mut store = Store::create(&root).unwrap();
        let receipt = store.put("k", b"v", DurabilityMode::Durable).unwrap();
        store
            .record_write_dedup([7u8; 16], [1u8; 32], &receipt)
            .unwrap();
    }
    let dedup = root
        .join("store-info")
        .join(residiuum_store::WRITE_DEDUP_JOURNAL_FILE);
    assert!(dedup.is_file());
    // A second accepted operation appends rather than replacing the table.
    {
        let mut store = Store::open(&root).unwrap();
        let receipt = store.put("k2", b"v2", DurabilityMode::Durable).unwrap();
        store
            .record_write_dedup([8u8; 16], [2u8; 32], &receipt)
            .unwrap();
    }
    let stable_len = fs::metadata(&dedup).unwrap().len();
    // Simulate a torn final append. The complete prefix must still replay.
    use std::io::Write as _;
    let mut file = fs::OpenOptions::new().append(true).open(&dedup).unwrap();
    file.write_all(b"TORN").unwrap();
    assert!(fs::metadata(&dedup).unwrap().len() > stable_len);
    let store = Store::open(&root).unwrap();
    assert_eq!(store.get("k").unwrap().as_deref(), Some(b"v".as_slice()));
    // The complete journal prefix still contains operation 7.
    let retry = store.resolve_write_dedup(&[7u8; 16], &[1u8; 32]).unwrap();
    assert!(
        retry.is_some(),
        "complete journal prefix must supply the retry receipt"
    );
}

#[test]
fn failpoint_before_rename_leaves_old_generation() {
    let _g = suite_lock();
    let dir = tempdir().unwrap();
    let path = dir.path().join("meta.dat");
    write_atomic(&path, b"stable").unwrap();

    arm_failpoint("atomic.before_rename", FailpointAction::Error);
    let err = write_atomic(&path, b"new-value").unwrap_err();
    assert!(
        matches!(err, StoreError::Failpoint(_)),
        "expected failpoint error, got {err:?}"
    );
    clear_failpoints();

    // Published path must still be the old generation (rename never happened).
    assert_eq!(fs::read(&path).unwrap(), b"stable");
}

#[test]
fn failpoint_after_tmp_sync_leaves_old_or_absent() {
    let _g = suite_lock();
    let dir = tempdir().unwrap();
    let path = dir.path().join("catalog.dat");
    write_atomic_keep_previous(&path, b"gen1").unwrap();

    arm_failpoint("atomic.after_tmp_sync", FailpointAction::Error);
    let err = write_atomic_keep_previous(&path, b"gen2").unwrap_err();
    assert!(matches!(err, StoreError::Failpoint(_)));
    clear_failpoints();

    // Primary still gen1 (we fail before renaming primary to .prev and installing new).
    assert_eq!(fs::read(&path).unwrap(), b"gen1");
}
