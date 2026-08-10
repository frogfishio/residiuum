//! DEF-096 Axis A — async lifecycle: O(1) rotate + background seal finalize.

use residiuum_store::{DurabilityMode, Store};
use std::fs;
use std::time::{Duration, Instant};

fn wait_drained(store: &mut Store) {
    store.drain_lifecycle().expect("drain lifecycle");
    assert_eq!(store.pending_seal_inflight(), 0);
}

#[test]
fn async_auto_seal_preserves_gets_and_writes_hydra() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    assert!(store.async_lifecycle_enabled());
    // Small threshold so a handful of puts trigger rotate.
    store.set_seal_threshold(8 * 1024);

    let payload = vec![b'x'; 512];
    for i in 0..80 {
        store
            .put(&format!("k/{i}"), &payload, DurabilityMode::Buffered)
            .unwrap();
    }
    wait_drained(&mut store);

    for i in 0..80 {
        assert_eq!(
            store.get(&format!("k/{i}")).unwrap().as_deref(),
            Some(payload.as_slice()),
            "missing key after async seal k/{i}"
        );
    }

    let sealed: Vec<_> = fs::read_dir(root.join("segments"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("residiuum"))
        .collect();
    assert!(
        !sealed.is_empty(),
        "expected at least one sealed segment after auto-seal"
    );
    let hydra_dir = root.join("indexes").join("seg");
    assert!(hydra_dir.is_dir(), "hydra index dir missing");
    let hydra_n = fs::read_dir(&hydra_dir).unwrap().count();
    assert!(hydra_n > 0, "expected hydra sidecars after seal finalize");
}

#[test]
fn explicit_seal_active_still_sync_and_drains() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();
    store.set_seal_threshold(64 * 1024 * 1024); // avoid auto-seal
    store.put("a", b"1", DurabilityMode::Durable).unwrap();
    store.seal_active().unwrap();
    assert_eq!(store.pending_seal_inflight(), 0);
    assert_eq!(store.get("a").unwrap().as_deref(), Some(b"1".as_slice()));

    let sealed: Vec<_> = fs::read_dir(root.join("segments"))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("residiuum"))
        .collect();
    assert_eq!(sealed.len(), 1);
}

#[test]
fn reopen_recovers_pending_seal_files() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("s");
    {
        let mut store = Store::create(&root).unwrap();
        store.set_async_lifecycle(true);
        store.set_seal_threshold(256);
        for i in 0..40 {
            store
                .put(
                    &format!("bulk/{i}"),
                    &vec![b'z'; 64],
                    DurabilityMode::Buffered,
                )
                .unwrap();
        }
        // Drop without drain: pending seals (if any) must recover on open.
    }

    let store = Store::open(&root).unwrap();
    for i in 0..40 {
        assert_eq!(
            store.get(&format!("bulk/{i}")).unwrap().as_deref(),
            Some(vec![b'z'; 64].as_slice()),
            "bulk/{i}"
        );
    }
    let pending_dir = root.join("active").join("pending");
    if pending_dir.is_dir() {
        let left = fs::read_dir(&pending_dir).unwrap().count();
        assert_eq!(left, 0, "pending seals should be finalized on open");
    }
}

#[test]
fn async_put_loop_completes_with_backpressure() {
    let dir = tempfile::tempdir().unwrap();
    let payload = vec![b'y'; 1024];
    let mut store = Store::create(dir.path().join("async")).unwrap();
    store.set_async_lifecycle(true);
    store.set_seal_threshold(4 * 1024);
    let t0 = Instant::now();
    for i in 0..200 {
        store
            .put(&format!("a/{i}"), &payload, DurabilityMode::Buffered)
            .unwrap();
    }
    let put_elapsed = t0.elapsed();
    wait_drained(&mut store);
    for i in 0..200 {
        assert_eq!(
            store.get(&format!("a/{i}")).unwrap().as_deref(),
            Some(payload.as_slice())
        );
    }
    assert!(
        put_elapsed < Duration::from_secs(60),
        "async put loop hung: {put_elapsed:?}"
    );
}
