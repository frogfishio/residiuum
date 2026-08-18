//! CR-ATMR3-001: durable prepare is derived from the closed plan.

#[path = "harness.rs"]
mod harness;

use harness::*;
use residiuum_atomic_lane::{DurableLane, LaneError};
use residiuum_atomics::{
    decode_prepare, plan_content_root, prepare_from_closed_plan, AtomicRefuseReason, AtomicsError,
    CanonicalKey, CollectionId, PlanMutation, PlanPredicate, PredicateKind, ReadWitness,
};
use residiuum_format::{examine_atomic_frame, scan_forward, AtomicEvidenceClass, AtomicFrameRole};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

fn first_prepare(dir: &Path) -> residiuum_atomics::AtomicPrepare {
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

fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if entry.file_name().to_string_lossy().ends_with(".tmp") {
            continue;
        }
        if path.is_dir() {
            walk(root, &path, out);
        } else {
            out.insert(
                path.strip_prefix(root).unwrap().to_path_buf(),
                fs::read(&path).unwrap(),
            );
        }
    }
}

fn refused(err: LaneError) -> AtomicRefuseReason {
    match err {
        LaneError::Kernel(AtomicsError::Refused(r)) => r,
        other => panic!("expected kernel refusal, got {other:?}"),
    }
}

#[test]
fn reopen_prepare_matches_independent_recomputation() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let member = create_member(aid(1), 0, "k", b"v");
    let plan = close(
        hid(1),
        aid(1),
        vec![PlanMutation {
            kind: member.member_kind,
            collection_id: member.object_identity.collection_id,
            key: member.object_identity.key.clone(),
            encoded_value: Some(b"v".to_vec()),
            if_version: None,
        }],
        Some([3u8; 32]),
        vec![ReadWitness {
            collection_id: CollectionId::from_bytes({
                let mut b = [0u8; 16];
                b[0] = 1;
                b
            })
            .unwrap(),
            key: CanonicalKey::String("seen".into()),
            observed_version: Some(vid(9)),
            projection_hash: [9u8; 32],
        }],
        vec![PlanPredicate {
            kind: PredicateKind::AssertAbsent,
            collection_id: Some(member.object_identity.collection_id),
            key: Some(CanonicalKey::String("gone".into())),
            version: None,
            encoded: None,
        }],
        vec![[7u8; 32]],
    );
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&member))
        .unwrap();
    let expected =
        prepare_from_closed_plan(&plan, FRONTIER, std::slice::from_ref(&member)).unwrap();
    let persisted = first_prepare(dir.path());
    assert_eq!(persisted, expected);
    assert_eq!(persisted.content_root, plan_content_root(&plan).unwrap());
    let lane = lane.reopen().unwrap();
    assert!(lane.heap().can_resolve(aid(1)));
    assert_eq!(first_prepare(dir.path()), expected);
}

#[test]
fn assertion_only_plan_reopens_with_exact_roots() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let plan = plan_empty(hid(1), aid(8));
    lane.begin_prepare(&plan, FRONTIER, &[]).unwrap();
    let expected = prepare_from_closed_plan(&plan, FRONTIER, &[]).unwrap();
    assert_eq!(first_prepare(dir.path()), expected);
    let lane = lane.reopen().unwrap();
    assert!(lane.heap().can_resolve(aid(8)));
    assert_eq!(first_prepare(dir.path()), expected);
    assert_eq!(
        lane.heap()
            .placement(aid(8))
            .unwrap()
            .member_manifest_root(),
        expected.ordered_member_manifest_root
    );
}

#[test]
fn different_frontier_or_plan_field_is_conflict_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let member = create_member(aid(2), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&member), &[b"v"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&member))
        .unwrap();
    let before = snapshot(dir.path());
    assert_eq!(
        refused(
            lane.begin_prepare(&plan, [0xB2; 32], std::slice::from_ref(&member))
                .unwrap_err()
        ),
        AtomicRefuseReason::AtomicIdConflict
    );
    assert_eq!(snapshot(dir.path()), before);
    let other = create_member(aid(2), 0, "k", b"other");
    let other_plan = plan_for(hid(1), std::slice::from_ref(&other), &[b"other"]);
    assert_eq!(
        refused(
            lane.begin_prepare(&other_plan, FRONTIER, std::slice::from_ref(&other))
                .unwrap_err()
        ),
        AtomicRefuseReason::AtomicIdConflict
    );
    assert_eq!(snapshot(dir.path()), before);
}

#[test]
fn member_that_does_not_match_plan_refuses_before_persist() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let member = create_member(aid(3), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&member), &[b"v"]);
    let before = snapshot(dir.path());
    let mut bad = member.clone();
    bad.before_version = Some(vid(9));
    assert_eq!(
        refused(
            lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&bad))
                .unwrap_err()
        ),
        AtomicRefuseReason::InvalidValue
    );
    assert_eq!(snapshot(dir.path()), before);
    assert!(!dir.path().join("plan").join(format!("{}", aid(3))).exists());
}

#[test]
fn mutated_plan_sidecar_cannot_reconstruct_prepare() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let member = create_member(aid(4), 0, "k", b"v");
    let plan = plan_for(hid(1), std::slice::from_ref(&member), &[b"v"]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&member))
        .unwrap();
    drop(lane);
    let path = dir.path().join("plan").join(format!("{}", aid(4)));
    let mut bytes = fs::read(&path).unwrap();
    let i = bytes.len() / 2;
    bytes[i] ^= 0xff;
    fs::write(&path, bytes).unwrap();
    assert!(matches!(
        DurableLane::open(dir.path()),
        Err(LaneError::Corrupt(_) | LaneError::Kernel(_))
    ));
}
