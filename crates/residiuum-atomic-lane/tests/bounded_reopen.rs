//! CR-ATMR3-004: reopen is bounded and can use a checkpoint prefix.

#[path = "harness.rs"]
mod harness;

use harness::*;
use residiuum_atomic_lane::{DurableLane, LaneError, RecoveryLimits};
use std::fs::{self, OpenOptions};

fn prepare_one(lane: &mut DurableLane, n: u8, key: &str, payload: &[u8]) {
    let id = aid(n);
    let m = create_member(id, 0, key, payload);
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[payload]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    lane.append_staged(m, payload.to_vec()).unwrap();
}

#[test]
fn second_open_uses_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_one(&mut lane, 1, "k", b"v");
    drop(lane);
    let first = DurableLane::open(dir.path()).unwrap();
    assert!(first.heap().can_resolve(aid(1)));
    drop(first);
    let second = DurableLane::open(dir.path()).unwrap();
    assert!(second.recovery_stats().used_checkpoint);
    assert_eq!(second.recovery_stats().atomics, 1);
    assert!(second.heap().can_resolve(aid(1)));
}

#[test]
fn many_atomics_reopen_is_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    for i in 1..=48u8 {
        prepare_one(&mut lane, i, "k", &[i]);
    }
    drop(lane);
    let lane = DurableLane::open(dir.path()).unwrap();
    let stats = lane.recovery_stats();
    assert_eq!(stats.atomics, 48);
    assert!(stats.bytes_scanned <= RecoveryLimits::prototype().max_log_bytes);
    for i in 1..=48u8 {
        assert!(lane.heap().can_resolve(aid(i)));
    }
}

#[test]
fn huge_sparse_log_is_refused_without_reading_it() {
    let dir = tempfile::tempdir().unwrap();
    let lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    drop(lane);
    let log = dir.path().join("coordinator.log");
    let file = OpenOptions::new().write(true).open(&log).unwrap();
    file.set_len(1 << 30).unwrap();
    drop(file);
    match DurableLane::open(dir.path()) {
        Err(LaneError::Incomplete {
            what: "log bytes",
            observed,
            limit,
        }) => {
            assert!(observed > limit);
            assert_eq!(limit, RecoveryLimits::prototype().max_log_bytes);
        }
        _other => panic!("expected Incomplete, got ok_or_other"),
    }
}

#[test]
fn corrupt_checkpoint_falls_back_to_bounded_scan() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_one(&mut lane, 3, "k", b"v");
    drop(lane);
    fs::write(dir.path().join("checkpoint"), b"NOTCKP-garbage-header").unwrap();
    let lane = DurableLane::open(dir.path()).unwrap();
    assert!(!lane.recovery_stats().used_checkpoint);
    assert!(lane.heap().can_resolve(aid(3)));
}

#[test]
fn recovery_stats_are_reported() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_one(&mut lane, 4, "k", b"abcd");
    drop(lane);
    fs::remove_file(dir.path().join("checkpoint")).unwrap();
    let lane = DurableLane::open(dir.path()).unwrap();
    let stats = lane.recovery_stats();
    assert!(stats.bytes_scanned > 0);
    assert!(stats.bytes_scanned <= RecoveryLimits::prototype().max_log_bytes);
    assert!(stats.frames_verified >= 2);
    assert_eq!(stats.atomics, 1);
    assert_eq!(stats.members, 1);
    assert!(!stats.used_checkpoint);
}

#[test]
fn hostile_checkpoint_atomic_count_is_incomplete() {
    let dir = tempfile::tempdir().unwrap();
    let lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    drop(lane);
    let mut buf = b"R2CKP1".to_vec();
    buf.push(1);
    buf.extend_from_slice(&0u64.to_be_bytes());
    buf.extend_from_slice(&1u32.to_be_bytes());
    buf.extend_from_slice(&0u64.to_be_bytes());
    buf.extend_from_slice(&u32::MAX.to_be_bytes());
    fs::write(dir.path().join("checkpoint"), buf).unwrap();
    match DurableLane::open(dir.path()) {
        Err(LaneError::Incomplete {
            what: "checkpoint atomics",
            ..
        }) => {}
        _other => panic!("expected Incomplete checkpoint atomics"),
    }
}
