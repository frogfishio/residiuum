//! CR-ATM2-001: directory-image crash prefixes and first stable boundary.

#[path = "harness.rs"]
mod harness;

use harness::*;
use residiuum_atomic_lane::{DurableLane, LaneError};
use residiuum_atomics::{MemberPhase, OrdinaryCell, PreparePhase, StagingFailpoint, StagingHeap};

fn assert_no_ordinary_leak(heap: &StagingHeap, k: &str) {
    assert!(heap.get(cid(1), &key(k)).is_none());
    assert!(!heap.scan().any(|(_, kk, _)| kk == &key(k)));
}

#[test]
fn before_prepare_reopen_has_no_prepare_and_no_ordinary_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    lane.arm(StagingFailpoint::BeforePrepare);
    let m = create_member(aid(1), 0, "k", b"x");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"x"]);
    assert!(matches!(
        lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m)),
        Err(LaneError::Injected(StagingFailpoint::BeforePrepare))
    ));
    let lane = lane.reopen().unwrap();
    assert!(!lane.heap().can_resolve(aid(1)));
    assert!(lane.heap().inspect_staged(aid(1)).is_none());
    assert_no_ordinary_leak(lane.heap(), "k");
    assert!(!dir
        .path()
        .join("intent")
        .join(format!("{}", aid(1)))
        .exists());
    assert!(!dir.path().join("plan").join(format!("{}", aid(1))).exists());
}

#[test]
fn after_prepare_reopen_keeps_prepare_and_does_not_publish() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    lane.arm(StagingFailpoint::AfterPrepare);
    let m = create_member(aid(2), 0, "k", b"x");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"x"]);
    assert!(matches!(
        lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m)),
        Err(LaneError::Injected(StagingFailpoint::AfterPrepare))
    ));
    let lane = lane.reopen().unwrap();
    assert!(lane.heap().can_resolve(aid(2)));
    assert_eq!(lane.heap().inspect_staged(aid(2)).unwrap().len(), 0);
    assert_eq!(
        lane.heap().lifecycle(aid(2)).unwrap().prepare,
        PreparePhase::Prepared
    );
    assert_no_ordinary_leak(lane.heap(), "k");
    assert!(dir.path().join("coordinator.log").is_file());
}

#[test]
fn after_member_n_reopen_examines_surviving_member_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 2).unwrap();
    let a = create_member(aid(3), 0, "a", b"A");
    let b = create_member(aid(3), 1, "b", b"B");
    let plan = plan_for(hid(1), &[a.clone(), b.clone()], &[b"A", b"B"]);
    lane.begin_prepare(&plan, FRONTIER, &[a.clone(), b.clone()])
        .unwrap();
    lane.arm(StagingFailpoint::AfterMember(0));
    assert!(matches!(
        lane.append_staged(a, b"A".to_vec()),
        Err(LaneError::Injected(StagingFailpoint::AfterMember(0)))
    ));
    let lane = lane.reopen().unwrap();
    let staged = lane.heap().inspect_staged(aid(3)).unwrap();
    assert_eq!(staged.len(), 1);
    assert_eq!(staged[0].payload, b"A");
    assert_no_ordinary_leak(lane.heap(), "a");
    assert_no_ordinary_leak(lane.heap(), "b");
}

#[test]
fn seal_reopen_is_durable_invisible_and_lists_boundary_files() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 2).unwrap();
    let a = create_member(aid(4), 0, "a", b"A");
    let b = create_member(aid(4), 1, "b", b"B");
    let plan = plan_for(hid(1), &[a.clone(), b.clone()], &[b"A", b"B"]);
    lane.begin_prepare(&plan, FRONTIER, &[a.clone(), b.clone()])
        .unwrap();
    lane.append_staged(a, b"A".to_vec()).unwrap();
    lane.append_staged(b, b"B".to_vec()).unwrap();
    lane.seal_member_boundary(aid(4)).unwrap();

    let sealed = dir.path().join("sealed").join(format!("{}", aid(4)));
    assert!(sealed.is_file(), "first stable boundary marker");
    assert!(dir.path().join("coordinator.log").is_file());
    assert!(dir.path().join("shard-00000000.log").is_file());
    assert!(dir.path().join("shard-00000001.log").is_file());

    let lane = lane.reopen().unwrap();
    assert_eq!(
        lane.heap().lifecycle(aid(4)).unwrap().members,
        MemberPhase::DurableInvisible
    );
    assert_eq!(lane.heap().inspect_staged(aid(4)).unwrap().len(), 2);
    assert_no_ordinary_leak(lane.heap(), "a");
    assert_no_ordinary_leak(lane.heap(), "b");
}

#[test]
fn second_heap_cannot_resolve_first_atomic() {
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let mut a = DurableLane::create(a_dir.path(), hid(1), 1).unwrap();
    let b = DurableLane::create(b_dir.path(), hid(2), 1).unwrap();
    let m = create_member(aid(5), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"v"]);
    a.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    a.append_staged(m, b"v".to_vec()).unwrap();
    assert!(a.heap().can_resolve(aid(5)));
    assert!(!b.heap().can_resolve(aid(5)));
    assert!(b.heap().inspect_staged(aid(5)).is_none());
    assert_no_ordinary_leak(b.heap(), "k");
}

#[test]
fn negative_control_detects_a_leaked_staged_member() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let m = create_member(aid(6), 0, "leak", b"secret");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"secret"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    lane.append_staged(m, b"secret".to_vec()).unwrap();
    assert_no_ordinary_leak(lane.heap(), "leak");

    // Intentional leak: if this assertion is inverted, the negative control is dead.
    lane.publish_ordinary(
        cid(1),
        key("leak"),
        OrdinaryCell {
            version: vid(9),
            value: b"secret".to_vec(),
        },
    );
    assert!(
        lane.heap().get(cid(1), &key("leak")).is_some(),
        "negative control must observe a published staged member"
    );
}
