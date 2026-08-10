//! DEF-050 — productized single-node full backup and verified restore.
//!
//! Covers:
//! - crash-consistent full backup under exclusive writer (flushed active)
//! - inspect-path backup (on-disk files)
//! - restore preserving identity
//! - restore with identity reassignment (clone)
//! - verified manifest + file blake3; tamper detection
//! - salvage remains distinct from backup
//! - partial / interrupted package refusal

use residiuum_store::{
    backup_manifest_path, load_and_verify_manifest, restore_full_backup, verify_package_files,
    BackupConsistency, DurabilityMode, RestoreOptions, Store, BACKUP_PROFILE,
};
use std::fs;
use tempfile::tempdir;

#[test]
fn exclusive_backup_is_flushed_and_restorable() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");
    let dst = dir.path().join("dst");

    let mut store = Store::create(&src).unwrap();
    store
        .put("col/a", br#"{"v":1}"#, DurabilityMode::Durable)
        .unwrap();
    store
        .put("col/b", br#"{"v":2}"#, DurabilityMode::Durable)
        .unwrap();
    store.delete("col/a", DurabilityMode::Durable).unwrap();
    let sid = store.store_id();

    let report = store.backup_to(&bak).unwrap();
    assert_eq!(report.consistency, BackupConsistency::FlushedExclusive);
    assert_eq!(report.store_id, sid);
    assert!(report.manifest_path.is_file());
    assert!(report.files_copied >= 2);
    assert!(report.total_bytes > 0);
    drop(store);

    let manifest = load_and_verify_manifest(&bak).unwrap();
    assert_eq!(manifest.profile, BACKUP_PROFILE);
    assert_eq!(manifest.consistency, BackupConsistency::FlushedExclusive);
    verify_package_files(&bak, &manifest).unwrap();

    let restored = restore_full_backup(&bak, &dst, RestoreOptions::default()).unwrap();
    assert_eq!(restored.restored_store_id, sid);
    assert!(!restored.identity_reassigned);
    // live: only col/b (col/a deleted)
    assert_eq!(restored.live_subjects, 1);

    let opened = Store::open(&dst).unwrap();
    assert_eq!(opened.store_id(), sid);
    assert!(opened.get("col/a").unwrap().is_none());
    assert_eq!(
        opened.get("col/b").unwrap().as_deref(),
        Some(br#"{"v":2}"#.as_slice())
    );
}

#[test]
fn inspect_backup_uses_on_disk_consistency() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");

    let mut w = Store::create(&src).unwrap();
    w.put("k", b"v", DurabilityMode::Durable).unwrap();
    drop(w);

    let mut inspect = Store::open_inspect(&src).unwrap();
    let report = inspect.backup_to(&bak).unwrap();
    assert_eq!(report.consistency, BackupConsistency::OnDiskInspect);

    let dst = dir.path().join("dst");
    let restored = restore_full_backup(&bak, &dst, RestoreOptions::default()).unwrap();
    assert_eq!(restored.live_subjects, 1);
    let opened = Store::open(&dst).unwrap();
    assert_eq!(opened.get("k").unwrap().as_deref(), Some(b"v".as_slice()));
}

#[test]
fn reassign_identity_produces_distinct_store() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");
    let dst = dir.path().join("clone");

    let mut store = Store::create(&src).unwrap();
    store
        .put("users/x", br#"{"ok":true}"#, DurabilityMode::Durable)
        .unwrap();
    let sid = store.store_id();
    store.backup_to(&bak).unwrap();
    drop(store);

    let restored = restore_full_backup(
        &bak,
        &dst,
        RestoreOptions {
            reassign_identity: true,
        },
    )
    .unwrap();
    assert_eq!(restored.source_store_id, sid);
    assert_ne!(restored.restored_store_id, sid);
    assert!(restored.identity_reassigned);

    let opened = Store::open_with_options(
        &dst,
        residiuum_store::StoreOpenOptions::default().tolerate_unidentified_inventory(),
    )
    .unwrap();
    assert_eq!(opened.store_id(), restored.restored_store_id);
    assert_eq!(
        opened.get("users/x").unwrap().as_deref(),
        Some(br#"{"ok":true}"#.as_slice())
    );
}

#[test]
fn backup_refuses_non_empty_destination() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");
    fs::create_dir_all(&bak).unwrap();
    fs::write(bak.join("stale"), b"x").unwrap();

    let mut store = Store::create(&src).unwrap();
    store.put("k", b"v", DurabilityMode::Durable).unwrap();
    let err = store.backup_to(&bak).unwrap_err();
    assert!(
        err.to_string().contains("already exists") || err.to_string().contains("exists"),
        "err={err}"
    );
}

#[test]
fn restore_refuses_existing_store() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");
    let existing = dir.path().join("existing");

    let mut store = Store::create(&src).unwrap();
    store.put("k", b"v", DurabilityMode::Durable).unwrap();
    store.backup_to(&bak).unwrap();
    drop(store);

    let _ = Store::create(&existing).unwrap();
    let err = restore_full_backup(&bak, &existing, RestoreOptions::default()).unwrap_err();
    assert!(err.to_string().contains("already exists"), "err={err}");
}

#[test]
fn tampered_manifest_hash_rejected() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");

    let mut store = Store::create(&src).unwrap();
    store.put("k", b"v", DurabilityMode::Durable).unwrap();
    store.backup_to(&bak).unwrap();
    drop(store);

    let path = backup_manifest_path(&bak);
    let mut bytes = fs::read(&path).unwrap();
    // Flip one hex nibble inside the content_hash_hex string value.
    let key = b"\"content_hash_hex\"";
    let pos = bytes
        .windows(key.len())
        .position(|w| w == key)
        .expect("content_hash_hex key");
    let mut flipped = false;
    for b in bytes.iter_mut().skip(pos + key.len()) {
        if b.is_ascii_hexdigit() {
            *b = if *b == b'0' { b'1' } else { b'0' };
            flipped = true;
            break;
        }
    }
    assert!(flipped);
    fs::write(&path, &bytes).unwrap();

    let err = load_and_verify_manifest(&bak).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("content_hash") || msg.contains("mismatch") || msg.contains("corrupt"),
        "err={msg}"
    );
}

#[test]
fn salvage_is_not_a_backup_package() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let salvage_dst = dir.path().join("salvaged");
    let as_bak = dir.path().join("not-a-package");

    let mut store = Store::create(&src).unwrap();
    store.put("k", b"v", DurabilityMode::Durable).unwrap();
    drop(store);

    let inspect = Store::open_inspect(&src).unwrap();
    inspect.salvage_to(&salvage_dst).unwrap();

    // A live store directory is not a backup package.
    let err = load_and_verify_manifest(&salvage_dst).unwrap_err();
    assert!(
        err.to_string().contains("not a Residiuum store")
            || err.to_string().contains("missing")
            || err.to_string().contains("not a"),
        "err={err}"
    );

    // Empty dir also fails.
    fs::create_dir_all(&as_bak).unwrap();
    assert!(load_and_verify_manifest(&as_bak).is_err());
}

#[test]
fn source_unchanged_after_backup() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");

    let mut store = Store::create(&src).unwrap();
    store.put("k", b"before", DurabilityMode::Durable).unwrap();
    let before = store.get("k").unwrap();
    store.backup_to(&bak).unwrap();
    assert_eq!(store.get("k").unwrap(), before);
    // Source still accepts writes after backup.
    store.put("k", b"after", DurabilityMode::Durable).unwrap();
    assert_eq!(
        store.get("k").unwrap().as_deref(),
        Some(b"after".as_slice())
    );
}
