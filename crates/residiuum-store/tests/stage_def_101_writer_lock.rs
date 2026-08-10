//! DEF-101 — truthful writer-lock acquisition and diagnostics.

use residiuum_store::{
    DurabilityMode, Store, StoreError, StoreOpenOptions, WriterLock, WriterLockClass,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn two_handles_same_process_in_process_class() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let _a = Store::create(&path).unwrap();
    let err = match Store::open(&path) {
        Ok(_) => panic!("second open should fail"),
        Err(e) => e,
    };
    match err {
        StoreError::WriterLockHeld(obs) => {
            assert_eq!(obs.class, WriterLockClass::InProcess);
            assert!(!obs.retryable);
            assert!(obs.os_lock_authoritative);
            assert!(!obs.detail.to_lowercase().contains("empty"));
            assert!(
                obs.detail.contains("do not delete writer.lock") || obs.detail.contains("drop")
            );
        }
        StoreError::NotAStore(_) => panic!("WriterLockHeld must not become NotAStore / empty"),
        other => panic!("expected WriterLockHeld, got {other:?}"),
    }
}

#[test]
fn open_inspect_while_writer_live() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let mut writer = Store::create(&path).unwrap();
    writer.put("k", b"v", DurabilityMode::Durable).unwrap();
    let inspect = Store::open_inspect(&path).unwrap();
    assert!(!inspect.holds_writer_lock());
    assert_eq!(inspect.get("k").unwrap().as_deref(), Some(b"v".as_slice()));
}

#[test]
fn stale_pid_text_does_not_block_free_lock() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    {
        let _s = Store::create(&path).unwrap();
    }
    // After drop, OS lock is free. Overwrite diagnostic with dead/stale PID.
    let lock = path.join("store-info").join("writer.lock");
    std::fs::write(
        &lock,
        "residiuum-writer-lock\npid=1\nacquired_ns=0\nnote=stale-dead\n",
    )
    .unwrap();
    let reopened = Store::open(&path).unwrap();
    assert!(reopened.holds_writer_lock());
}

#[test]
fn try_open_never_creates() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("missing");
    let err = match Store::try_open(&path) {
        Ok(_) => panic!("try_open must not create"),
        Err(e) => e,
    };
    assert!(matches!(err, StoreError::NotAStore(_)));
}

#[test]
fn writer_lock_status_free_after_close() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    {
        let _s = Store::create(&path).unwrap();
    }
    let obs = Store::writer_lock_status(&path).unwrap();
    assert_eq!(obs.class, WriterLockClass::Free);
    assert!(obs.os_lock_authoritative);
}

#[test]
fn writer_lock_status_in_process_while_held() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let _s = Store::create(&path).unwrap();
    let obs = Store::writer_lock_status(&path).unwrap();
    assert_eq!(obs.class, WriterLockClass::InProcess);
}

#[test]
fn bounded_wait_cancel_creates_no_store_effect() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let _holder = Store::create(&path).unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    let cancel2 = cancel.clone();
    let path2 = path.clone();
    let handle = std::thread::spawn(move || {
        Store::open_with_options(
            &path2,
            StoreOpenOptions::wait_for(Duration::from_secs(5)).with_cancel(cancel2),
        )
    });
    std::thread::sleep(Duration::from_millis(80));
    cancel.store(true, Ordering::Relaxed);
    let err = match handle.join().unwrap() {
        Ok(_) => panic!("cancelled wait should fail"),
        Err(e) => e,
    };
    match err {
        StoreError::WriterLockHeld(obs) => {
            // Same-process second open is InProcess (registry) before OS wait.
            assert_eq!(obs.class, WriterLockClass::InProcess);
            assert!(!obs.retryable);
            assert!(obs.os_lock_authoritative);
        }
        other => panic!("expected WriterLockHeld on cancel, got {other:?}"),
    }
    // Holder still owns; no second writer.
    assert!(_holder.holds_writer_lock());
}

#[test]
fn wait_timeout_structured() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let _holder = Store::create(&path).unwrap();
    let err = match Store::open_with_options(
        &path,
        StoreOpenOptions::wait_for(Duration::from_millis(100)),
    ) {
        Ok(_) => panic!("timeout should fail"),
        Err(e) => e,
    };
    match err {
        StoreError::WriterLockHeld(obs) => {
            // Same process: in-process registry fails immediately (no multi-second wait).
            assert_eq!(obs.class, WriterLockClass::InProcess);
            assert!(!obs.retryable);
            assert!(obs.os_lock_authoritative);
            // Not database absence.
            assert!(!obs.detail.to_lowercase().contains("not a store"));
        }
        other => panic!("expected WriterLockHeld timeout/contention, got {other:?}"),
    }
}

#[test]
fn observe_via_writer_lock_api() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    {
        let _s = Store::create(&path).unwrap();
    }
    let paths = residiuum_store::StorePaths::new(&path);
    let obs = WriterLock::observe(&paths);
    assert_eq!(obs.class, WriterLockClass::Free);
}
