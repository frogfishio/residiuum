//! CR-R2-002: reopen binds intent, members, and seal to the persisted prepare.

#[path = "harness.rs"]
mod harness;

use harness::*;
use residiuum_atomic_lane::{DurableLane, LaneError};
use residiuum_atomics::{decode_prepare, prepare_from_closed_plan, MemberPhase};
use residiuum_format::{examine_atomic_frame, scan_forward, AtomicEvidenceClass, AtomicFrameRole};
use std::fs;

fn sealed_lane() -> (tempfile::TempDir, DurableLane) {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let m = create_member(aid(1), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"v"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    lane.append_staged(m, b"v".to_vec()).unwrap();
    lane.seal_member_boundary(aid(1)).unwrap();
    (dir, lane)
}

fn first_prepare(dir: &std::path::Path) -> residiuum_atomics::AtomicPrepare {
    let bytes = fs::read(dir.join("coordinator.log")).unwrap();
    let report = scan_forward(&bytes, residiuum_format::SafetyLimits::draft_defaults());
    for (_, frame) in report.verified_frames() {
        if let Some(AtomicEvidenceClass::Valid(link)) = examine_atomic_frame(frame) {
            if link.role == AtomicFrameRole::Prepare {
                return decode_prepare(&frame.body).unwrap();
            }
        }
    }
    panic!("no prepare frame");
}

fn flip(path: &std::path::Path) {
    let mut bytes = fs::read(path).unwrap();
    let i = bytes.len() / 2;
    bytes[i] ^= 0xff;
    fs::write(path, bytes).unwrap();
}

#[test]
fn sealed_reopen_recovers_derived_prepare_not_placeholders() {
    let (dir, lane) = sealed_lane();
    let prepare = first_prepare(dir.path());
    let member = create_member(aid(1), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&member), &[b"v"]);
    let expected =
        prepare_from_closed_plan(&plan, FRONTIER, std::slice::from_ref(&member)).unwrap();
    assert_eq!(prepare, expected);
    assert_ne!(prepare.frontier, [1u8; 32]);
    assert_ne!(prepare.read_set_root, [2u8; 32]);
    assert_ne!(prepare.predicate_set_root, [3u8; 32]);
    assert_ne!(prepare.active_rule_revision_root, [4u8; 32]);
    let lane = lane.reopen().unwrap();
    assert_eq!(
        lane.heap().lifecycle(aid(1)).unwrap().members,
        MemberPhase::DurableInvisible
    );
    let again = first_prepare(dir.path());
    assert_eq!(prepare, again);
    assert_eq!(
        lane.heap()
            .placement(aid(1))
            .unwrap()
            .member_manifest_root(),
        prepare.ordered_member_manifest_root
    );
}

#[test]
fn mutated_intent_cannot_reconstruct_a_sealed_prepare() {
    let (dir, lane) = sealed_lane();
    drop(lane);
    flip(&dir.path().join("intent").join(format!("{}", aid(1))));
    assert!(
        matches!(
            DurableLane::open(dir.path()),
            Err(LaneError::Corrupt(_)) | Err(LaneError::Kernel(_))
        ),
        "mutated intent must not reopen"
    );
}

#[test]
fn mutated_seal_cannot_reconstruct_a_sealed_prepare() {
    let (dir, lane) = sealed_lane();
    drop(lane);
    flip(&dir.path().join("sealed").join(format!("{}", aid(1))));
    assert!(matches!(
        DurableLane::open(dir.path()),
        Err(LaneError::Corrupt(_))
    ));
}

#[test]
fn legacy_plaintext_seal_is_refused() {
    let (dir, lane) = sealed_lane();
    drop(lane);
    fs::write(
        dir.path().join("sealed").join(format!("{}", aid(1))),
        b"boundary\n",
    )
    .unwrap();
    assert!(matches!(
        DurableLane::open(dir.path()),
        Err(LaneError::Corrupt(_))
    ));
}

#[test]
fn mutated_payload_cannot_reconstruct_a_sealed_prepare() {
    let (dir, lane) = sealed_lane();
    drop(lane);
    fs::write(
        dir.path().join("payload").join(format!("{}-0", aid(1))),
        b"xx",
    )
    .unwrap();
    assert!(matches!(
        DurableLane::open(dir.path()),
        Err(LaneError::Kernel(_) | LaneError::Corrupt(_))
    ));
}

#[test]
fn trailing_torn_coordinator_frame_does_not_invent_prepare() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let m = create_member(aid(2), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[b"v"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    drop(lane);
    let log = dir.path().join("coordinator.log");
    let bytes = fs::read(&log).unwrap();
    fs::write(&log, &bytes[..bytes.len().saturating_sub(1)]).unwrap();
    assert!(
        matches!(
            DurableLane::open(dir.path()),
            Err(LaneError::Coverage { .. } | LaneError::Corrupt(_))
        ),
        "truncating an acknowledged prepare is coverage damage, not absence"
    );
}
