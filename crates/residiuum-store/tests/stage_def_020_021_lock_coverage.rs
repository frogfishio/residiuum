//! DEF-020 exclusive writer lock + DEF-012 coverage-aware live scans.

use residiuum_store::{DurabilityMode, IncompleteReason, Store, StoreError};
use std::fs;
use std::process::Command;
use tempfile::tempdir;

#[test]
fn second_writer_handle_fails_before_touching_segments() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let _writer = Store::create(&path).unwrap();
    match Store::open(&path) {
        Ok(_) => panic!("second writer open must fail while first holds the lock"),
        Err(err) => assert!(
            matches!(err, StoreError::WriterLockHeld(_)),
            "expected WriterLockHeld, got {err:?} (not empty store / NotAStore)"
        ),
    }
}

#[test]
fn inspect_can_run_while_writer_holds_lock() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let mut writer = Store::create(&path).unwrap();
    writer.put("k", b"v", DurabilityMode::Durable).unwrap();
    assert!(writer.holds_writer_lock());

    let inspect = Store::open_inspect(&path).unwrap();
    assert!(!inspect.holds_writer_lock());
    assert_eq!(inspect.get("k").unwrap().as_deref(), Some(b"v".as_slice()));
}

#[test]
fn drop_writer_releases_lock_for_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    {
        let mut s = Store::create(&path).unwrap();
        s.put("a", b"1", DurabilityMode::Durable).unwrap();
    }
    let s = Store::open(&path).unwrap();
    assert_eq!(s.get("a").unwrap().as_deref(), Some(b"1".as_slice()));
}

#[test]
fn cross_process_second_writer_fails() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let _holder = Store::create(&path).unwrap();

    let lock_path = path.join("store-info").join("writer.lock");
    assert!(lock_path.is_file());

    let status = Command::new("python3")
        .args([
            "-c",
            &format!(
                r#"
import fcntl, sys
p = {lock_path:?}
f = open(p, "a+")
try:
    fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    sys.exit(0)  # expected: lock held
sys.exit(2)  # unexpected: acquired lock while holder alive
"#
            ),
        ])
        .status()
        .expect("python3 available for flock probe");
    assert!(
        status.success(),
        "cross-process flock probe failed: {status}"
    );
}

#[test]
fn live_logical_entries_fails_closed_when_chunks_missing() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(20);
    store.set_chunk_size(8);

    let payload: Vec<u8> = (0u8..60).collect();
    store.put("big", &payload, DurabilityMode::Durable).unwrap();
    store.put("small", b"ok", DurabilityMode::Durable).unwrap();
    // Move frames to a sealed segment, then delete that file while the in-memory
    // index still lists the chunked subject as live.
    store.seal_active().unwrap();
    let segments_dir = store.path().join("segments");
    for entry in fs::read_dir(&segments_dir).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().and_then(|e| e.to_str()) == Some("residiuum") {
            fs::remove_file(&p).unwrap();
        }
    }

    // Ordinary complete scan must not succeed with silent omission (DEF-012).
    let err = store.live_logical_entries().unwrap_err();
    assert!(
        matches!(
            err,
            StoreError::CoverageIncomplete(_) | StoreError::PayloadPartial
        ),
        "expected coverage/partial error, got {err:?}"
    );

    let scan = store.scan_live_logical().unwrap();
    assert!(!scan.complete, "scan must report incomplete coverage");
    assert!(
        !scan.incomplete.is_empty(),
        "at least one incomplete subject expected"
    );
    assert!(
        scan.incomplete
            .iter()
            .any(|i| i.reason == IncompleteReason::PayloadPartial
                || i.reason == IncompleteReason::PayloadUnavailable
                || i.reason == IncompleteReason::PayloadConflict),
        "incomplete reasons: {:?}",
        scan.incomplete
    );
    // Complete small inline value may still appear if its frames were only in
    // the deleted segment — either way, completeness is false.
    assert!(
        store.get("big").is_err() || store.get("big").unwrap().is_none(),
        "chunked subject must not return a silent complete body"
    );
}

#[test]
fn complete_scan_succeeds_when_all_payloads_readable() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.put("a", b"1", DurabilityMode::Durable).unwrap();
    store.put("b", b"2", DurabilityMode::Durable).unwrap();
    let scan = store.scan_live_logical().unwrap();
    assert!(scan.complete);
    assert!(scan.incomplete.is_empty());
    assert_eq!(store.live_logical_entries().unwrap().len(), 2);
}
