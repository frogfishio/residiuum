//! AWO-1: put_many batch path must not publish index before segment tail write.
//!
//! Uses failpoints to inject persist failure after in-memory appends; the store
//! must restore the active segment checkpoint and leave keys absent.

use residiuum_store::adaptive_write::AWO_FAILPOINTS;
use residiuum_store::{
    arm_failpoint_once, clear_failpoints, enable_failpoint_hit_proof, require_failpoint_visited,
    DurabilityMode, FailpointAction, Store,
};

#[test]
fn awo_failpoints_closed_set_named() {
    assert!(AWO_FAILPOINTS.contains(&"awo.persist.before"));
    assert!(AWO_FAILPOINTS.contains(&"awo.publish.before"));
    assert_eq!(AWO_FAILPOINTS.len(), 11);
}

#[test]
fn put_many_persist_fail_publishes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path()).unwrap();
    clear_failpoints();
    enable_failpoint_hit_proof();
    arm_failpoint_once("awo.persist.before", FailpointAction::Error);

    let items: Vec<(&str, &[u8])> = vec![("awo/k1", b"v1"), ("awo/k2", b"v2"), ("awo/k3", b"v3")];
    let err = store
        .put_many(&items, DurabilityMode::Buffered)
        .expect_err("persist failpoint must fail batch");
    let _ = err;
    require_failpoint_visited("awo.persist.before");

    // Index must not contain staged keys.
    assert!(store.get("awo/k1").unwrap().is_none());
    assert!(store.get("awo/k2").unwrap().is_none());
    assert!(store.get("awo/k3").unwrap().is_none());

    // After failure, a clean put_many must still succeed.
    clear_failpoints();
    let ok = store
        .put_many(&[("awo/k1", b"v1")], DurabilityMode::Buffered)
        .unwrap();
    assert_eq!(ok.len(), 1);
    assert!(store.get("awo/k1").unwrap().is_some());
}

#[test]
fn put_many_success_publish_after_persist() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path()).unwrap();
    clear_failpoints();
    let items: Vec<(&str, &[u8])> = vec![("awo/ok1", b"a"), ("awo/ok2", b"b")];
    let receipts = store
        .put_many(&items, DurabilityMode::Buffered)
        .expect("batch ok");
    assert_eq!(receipts.len(), 2);
    assert!(store.get("awo/ok1").unwrap().is_some());
    assert!(store.get("awo/ok2").unwrap().is_some());

    drop(store);
    let store = Store::open(dir.path()).unwrap();
    assert!(store.get("awo/ok1").unwrap().is_some());
    assert!(store.get("awo/ok2").unwrap().is_some());
}

#[test]
fn multi_shard_put_many_persist_fail_publishes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create_with_shards(dir.path(), 4).unwrap();
    clear_failpoints();
    enable_failpoint_hit_proof();
    // Multi-shard uses store.active.write_tail failpoint inside write_segment_tail.
    arm_failpoint_once("store.active.write_tail.before", FailpointAction::Error);

    let mut items: Vec<(String, Vec<u8>)> = Vec::new();
    for i in 0..32 {
        items.push((format!("awo/ms/{i}"), format!("v{i}").into_bytes()));
    }
    let refs: Vec<(&str, &[u8])> = items
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_slice()))
        .collect();
    let err = store
        .put_many(&refs, DurabilityMode::Buffered)
        .expect_err("multi-shard persist fail");
    let _ = err;
    require_failpoint_visited("store.active.write_tail.before");

    for (k, _) in &items {
        assert!(
            store.get(k).unwrap().is_none(),
            "key {k} must not be published after multi-shard fail"
        );
    }

    clear_failpoints();
    // After clean fail (not short-write poison), mutations still allowed.
    store
        .put_many(&[("awo/ms/retry", b"ok")], DurabilityMode::Buffered)
        .expect("retry after clean fail");
    assert!(store.get("awo/ms/retry").unwrap().is_some());
}
