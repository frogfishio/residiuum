//! AWO-1: publication failpoint after successful persist must not leave a half-published
//! index for the *current* batch (all-or-nothing publish stage).

use residiuum_store::{
    arm_failpoint_once, clear_failpoints, enable_failpoint_hit_proof, require_failpoint_visited,
    DurabilityMode, FailpointAction, Store,
};

#[test]
fn publish_failpoint_aborts_before_index_update() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path()).unwrap();
    clear_failpoints();
    enable_failpoint_hit_proof();
    arm_failpoint_once("awo.publish.before", FailpointAction::Error);

    let err = store
        .put_many(
            &[("awo/pub/a", b"1"), ("awo/pub/b", b"2")],
            DurabilityMode::Buffered,
        )
        .expect_err("publish failpoint");
    let _ = err;
    require_failpoint_visited("awo.publish.before");

    // Index visibility not applied for this batch.
    assert!(store.get("awo/pub/a").unwrap().is_none());
    assert!(store.get("awo/pub/b").unwrap().is_none());

    // Frames may already be durable on the active file; reopen recovery can rebuild.
    // After a clean (non-short-write) publish fail, writer is not poisoned.
    clear_failpoints();
    assert!(!store.is_awo_writer_poisoned());
    store
        .put_many(&[("awo/pub/c", b"3")], DurabilityMode::Buffered)
        .expect("mutation after clean publish fail");
    assert!(store.get("awo/pub/c").unwrap().is_some());
}
