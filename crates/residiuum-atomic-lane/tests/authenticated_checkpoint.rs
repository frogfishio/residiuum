//! CR-ATMR4-002: checkpoint is checksummed; sealed is not invented.

#[path = "harness.rs"]
mod harness;

use harness::*;
use residiuum_atomic_lane::{DurableLane, LaneError};
use residiuum_atomics::MemberPhase;
use std::fs;

fn prepare_and_seal(lane: &mut DurableLane) {
    let m = create_member(aid(1), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"v"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    lane.append_staged(m, b"v".to_vec()).unwrap();
    lane.seal_member_boundary(aid(1)).unwrap();
}

#[test]
fn flipped_checkpoint_byte_is_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_and_seal(&mut lane);
    drop(lane);
    let path = dir.path().join("checkpoint");
    let mut bytes = fs::read(&path).unwrap();
    let i = bytes.len() - 1;
    bytes[i] ^= 1;
    fs::write(&path, bytes).unwrap();
    match DurableLane::open(dir.path()) {
        Err(LaneError::Corrupt("checkpoint checksum")) => {}
        Ok(_) => panic!("forged checkpoint must not open"),
        Err(e) => panic!("expected checksum corrupt, got {e:?}"),
    }
}

#[test]
fn missing_seal_is_not_invented_from_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_and_seal(&mut lane);
    drop(lane);
    fs::remove_file(dir.path().join("sealed").join(format!("{}", aid(1)))).unwrap();
    let lane = DurableLane::open(dir.path()).unwrap();
    let life = lane.heap().lifecycle(aid(1)).unwrap();
    assert_ne!(life.members, MemberPhase::DurableInvisible);
}

#[test]
fn flipped_coordinator_head_is_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_and_seal(&mut lane);
    drop(lane);
    let path = dir.path().join("coordinator.log");
    let mut bytes = fs::read(&path).unwrap();
    assert!(!bytes.is_empty());
    bytes[0] ^= 0x5a;
    fs::write(&path, bytes).unwrap();
    match DurableLane::open(dir.path()) {
        Err(LaneError::Corrupt(msg)) => {
            assert!(
                msg.contains("prefix") || msg.contains("head") || msg.contains("hash"),
                "unexpected corrupt reason {msg}"
            );
        }
        Ok(_) => panic!("mutated covered prefix must not open"),
        Err(e) => panic!("expected prefix corrupt, got {e:?}"),
    }
}

#[test]
fn valid_checkpoint_reopen_matches_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    prepare_and_seal(&mut lane);
    drop(lane);
    let lane = DurableLane::open(dir.path()).unwrap();
    assert!(lane.recovery_stats().used_checkpoint);
    assert!(!lane.recovery_stats().rebuilt_checkpoint);
    assert_eq!(
        lane.heap().lifecycle(aid(1)).unwrap().members,
        MemberPhase::DurableInvisible
    );
}
