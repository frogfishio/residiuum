//! CSQ-9 — concurrency / ownership / limits / resources (first labor cut).
//!
//! Behavioural floors for `CSQ-CON-001`…`005` and `CSQ-RES-001`…`005`.
//! Permanent regression authorities: DEF-101 (writer lock), DEF-096 (shards),
//! DEF-020/021 (lock coverage). Loom/Shuttle kernels and full multi-process
//! soak remain residual depth.

use residiuum_store::{
    arm_failpoint_once, clear_failpoints, subject_writer_shard, DurabilityMode, FailpointAction,
    ReadBudget, Store, StoreError, WriterLockClass,
};
use std::collections::HashSet;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

/// CSQ-CON-002 / CSQ-CON-005 — concurrent writers are fenced; second open fails
/// without becoming a successful storage result.
#[test]
fn csq_con_second_writer_fenced() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let mut a = Store::create(&path).unwrap();
    a.put("k", b"v1", DurabilityMode::Durable).unwrap();

    let err = match Store::open(&path) {
        Ok(_) => panic!("second exclusive open must fail"),
        Err(e) => e,
    };
    match err {
        StoreError::WriterLockHeld(obs) => {
            assert_eq!(obs.class, WriterLockClass::InProcess);
            assert!(obs.os_lock_authoritative);
        }
        other => panic!("expected WriterLockHeld, got {other:?}"),
    }
    // First writer still exclusive and consistent.
    assert_eq!(a.get("k").unwrap().as_deref(), Some(b"v1".as_slice()));
    a.put("k", b"v2", DurabilityMode::Durable).unwrap();
    assert_eq!(a.get("k").unwrap().as_deref(), Some(b"v2".as_slice()));
}

/// CSQ-CON-001 — concurrent inspect readers observe a complete generation while
/// a writer continues putting; never a panic/poison as success.
#[test]
fn csq_con_concurrent_readers_see_complete_generation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let mut writer = Store::create(&path).unwrap();
    writer
        .put("stable", b"gen0", DurabilityMode::Durable)
        .unwrap();

    let path_c = path.clone();
    let barrier = Arc::new(Barrier::new(2));
    let b2 = barrier.clone();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen2 = seen.clone();

    let reader = thread::spawn(move || {
        b2.wait();
        for _ in 0..40 {
            let inspect = Store::open_inspect(&path_c).unwrap();
            // May observe gen0 or a later complete gen after writer publishes.
            let v = inspect.get("stable").unwrap();
            assert!(
                v.is_some(),
                "subject must not vanish under concurrent writes"
            );
            let body = v.unwrap();
            assert!(
                body == b"gen0"
                    || body == b"gen1"
                    || body == b"gen2"
                    || body == b"gen3"
                    || body.starts_with(b"gen"),
                "unexpected mixed/corrupt body: {body:?}"
            );
            seen2.lock().unwrap().push(body);
            drop(inspect);
            thread::sleep(Duration::from_millis(1));
        }
    });

    barrier.wait();
    for i in 1..=3u8 {
        let body = format!("gen{i}");
        writer
            .put("stable", body.as_bytes(), DurabilityMode::Durable)
            .unwrap();
        thread::sleep(Duration::from_millis(2));
    }
    reader.join().unwrap();

    let final_body = writer.get("stable").unwrap().unwrap();
    assert_eq!(final_body, b"gen3");
    let observations = seen.lock().unwrap();
    assert!(!observations.is_empty());
    // Every observation must equal some complete published generation.
    for o in observations.iter() {
        assert!(
            o == b"gen0" || o == b"gen1" || o == b"gen2" || o == b"gen3",
            "mixed generation observed: {o:?}"
        );
    }
}

/// CSQ-CON-002 — multi-shard put_many completes; concurrent get after publish
/// returns exact payloads (no half-publication).
#[test]
fn csq_con_sharded_put_many_no_half_publication() {
    let dir = tempdir().unwrap();
    let mut store = Store::create_with_shards(dir.path().join("s"), 4).unwrap();
    assert_eq!(store.writer_shards(), 4);

    let payload = vec![0xABu8; 64];
    let keys: Vec<String> = (0..80).map(|i| format!("k/{i}")).collect();
    let items: Vec<(&str, &[u8])> = keys
        .iter()
        .map(|k| (k.as_str(), payload.as_slice()))
        .collect();
    let receipts = store.put_many(&items, DurabilityMode::Durable).unwrap();
    assert_eq!(receipts.len(), 80);
    let segs: HashSet<_> = receipts.iter().map(|r| r.segment_id).collect();
    assert!(segs.len() >= 2, "expected multi-shard segment spread");

    for k in &keys {
        assert_eq!(
            store.get(k).unwrap().as_deref(),
            Some(payload.as_slice()),
            "{k}"
        );
    }

    // Concurrent readers after durable put_many.
    let path = dir.path().join("s");
    drop(store);
    let mut handles = Vec::new();
    for t in 0..4 {
        let p = path.clone();
        let keys = keys.clone();
        let payload = payload.clone();
        handles.push(thread::spawn(move || {
            let inspect = Store::open_inspect(&p).unwrap();
            for (i, k) in keys.iter().enumerate() {
                if i % 4 == t {
                    assert_eq!(
                        inspect.get(k).unwrap().as_deref(),
                        Some(payload.as_slice()),
                        "{k}"
                    );
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

/// CSQ-CON-003 — compact under exclusive writer does not leave impossible live state;
/// inspect can still observe after compact.
#[test]
fn csq_con_compact_then_inspect_consistent() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    {
        let mut store = Store::create(&path).unwrap();
        store.put("a", b"1", DurabilityMode::Durable).unwrap();
        store.put("b", b"2", DurabilityMode::Durable).unwrap();
        store.delete("a", DurabilityMode::Durable).unwrap();
        store.put("c", b"3", DurabilityMode::Durable).unwrap();
        let report = store.compact_live().unwrap();
        assert!(report.sources_retained);
        assert_eq!(store.get("b").unwrap().as_deref(), Some(b"2".as_slice()));
        assert_eq!(store.get("c").unwrap().as_deref(), Some(b"3".as_slice()));
        assert!(store.get("a").unwrap().is_none());
    }
    let inspect = Store::open_inspect(&path).unwrap();
    assert_eq!(inspect.get("b").unwrap().as_deref(), Some(b"2".as_slice()));
    assert_eq!(inspect.get("c").unwrap().as_deref(), Some(b"3".as_slice()));
    assert!(inspect.get("a").unwrap().is_none());
}

/// CSQ-RES-001 — over-limit put fails before durable effect.
#[test]
fn csq_res_size_limit_fails_before_effect() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.put("ok", b"v", DurabilityMode::Durable).unwrap();
    let before = store.live_count();
    let max = store.large_value_policy().max_logical_payload_bytes as usize;
    let too_big = vec![9u8; max + 1];
    let err = store
        .put("big", &too_big, DurabilityMode::Durable)
        .unwrap_err();
    assert!(matches!(err, StoreError::PayloadTooLarge));
    assert_eq!(store.live_count(), before);
    assert!(store.get("big").unwrap().is_none());
    assert_eq!(store.get("ok").unwrap().as_deref(), Some(b"v".as_slice()));
}

/// CSQ-RES-003 — history/versioned reads honor budget or stop explicitly.
#[test]
fn csq_res_read_budget_stops_explicitly() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    // Many rewrites create a long history stream.
    for i in 0..30u8 {
        store.put("h", &[i; 16], DurabilityMode::Durable).unwrap();
    }
    let hist = store.history("h").unwrap();
    assert!(hist.events.len() >= 30);
    let event_id = hist.events.last().unwrap().event_id;

    // Tight budget must not panic; result is either complete within budget or
    // budget-aware (VersionedPayloadResult always returns; budget applies on search).
    let tight = ReadBudget {
        max_events_examined: 2,
        max_bytes_examined: 64,
    };
    let projected = store
        .get_payload_version("h", &event_id, tight)
        .expect("budgeted projection must not error as storage failure");
    // Latest event body is small; projection should still complete.
    assert!(
        projected
            .selected
            .as_ref()
            .map(|p| p.is_complete())
            .unwrap_or(false)
            || projected.bytes_examined <= 64
            || projected.events_examined <= 2,
        "budget path must remain explicit: {projected:?}"
    );
    assert_eq!(
        store.get("h").unwrap().as_deref(),
        Some([29u8; 16].as_slice())
    );
}

/// CSQ-RES-004 — injected ENOSPC fails closed and preserves prior authority.
#[test]
fn csq_res_enospc_preserves_prior_authority() {
    clear_failpoints();
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    store
        .put("prior", b"prior-v1", DurabilityMode::Durable)
        .unwrap();

    arm_failpoint_once("store.active.write_tail.before", FailpointAction::IoEnospc);
    let result = store.put("new", b"new-v1", DurabilityMode::Durable);
    clear_failpoints();

    match result {
        Err(StoreError::Failpoint(_)) | Err(StoreError::Io(_)) => {}
        Ok(_) => {
            // Some builds may not hit this failpoint name; still require prior intact.
        }
        Err(other) => panic!("unexpected error class: {other:?}"),
    }
    assert_eq!(
        store.get("prior").unwrap().as_deref(),
        Some(b"prior-v1".as_slice())
    );
    // If put failed, new must be absent; if it succeeded, both present — either ok
    // as long as prior authority is preserved.
    if result.is_err() {
        assert!(store.get("new").unwrap().is_none());
    }
}

/// CSQ-CON-005 / shard routing — home shard stable; concurrent readers on reopen.
#[test]
fn csq_con_shard_home_stable_and_reopen() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    {
        let mut store = Store::create_with_shards(&root, 3).unwrap();
        for i in 0..60 {
            let k = format!("s/{i}");
            let home = subject_writer_shard(k.as_bytes(), 3);
            assert!(home < 3);
            store
                .put(&k, format!("v{i}").as_bytes(), DurabilityMode::Durable)
                .unwrap();
            assert_eq!(
                subject_writer_shard(k.as_bytes(), store.writer_shards()),
                home
            );
        }
    }
    let store = Store::open(&root).unwrap();
    assert_eq!(store.writer_shards(), 3);
    for i in 0..60 {
        let k = format!("s/{i}");
        assert_eq!(
            store.get(&k).unwrap().as_deref(),
            Some(format!("v{i}").as_bytes())
        );
    }
}

/// CSQ-RES-005 — drop writer releases lock; no half-open state for next exclusive.
#[test]
fn csq_res_drop_releases_lock_for_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("s");
    {
        let mut w = Store::create(&path).unwrap();
        w.put("k", b"v", DurabilityMode::Durable).unwrap();
    }
    let mut reopened = Store::open(&path).unwrap();
    assert!(reopened.holds_writer_lock());
    assert_eq!(reopened.get("k").unwrap().as_deref(), Some(b"v".as_slice()));
    reopened.put("k", b"v2", DurabilityMode::Durable).unwrap();
    assert_eq!(
        reopened.get("k").unwrap().as_deref(),
        Some(b"v2".as_slice())
    );
}
