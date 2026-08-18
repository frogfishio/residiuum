//! CR-ATMR3-005: durable chunk-manifest / chunk append, reopen, failpoint, seal.

#[path = "harness.rs"]
mod harness;

use harness::*;
use residiuum_atomic_lane::{DurableLane, LaneError};
use residiuum_atomics::{
    AtomicRefuseReason, AtomicsError, ChunkPlan, MemberPhase, StagingFailpoint, StagingHeap,
};
use std::fs;

fn h(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

fn assert_no_ordinary_leak(heap: &StagingHeap, k: &str) {
    assert!(heap.get(cid(1), &key(k)).is_none());
    assert!(!heap.scan().any(|(_, kk, _)| kk == &key(k)));
}

fn two_chunks() -> (Vec<u8>, Vec<u8>, Vec<u8>, ChunkPlan) {
    let p0 = b"aaaa".to_vec();
    let p1 = b"bbbb".to_vec();
    let mut whole = Vec::new();
    whole.extend_from_slice(&p0);
    whole.extend_from_slice(&p1);
    let plan = ChunkPlan {
        total: 2,
        chunk_hashes: vec![h(&p0), h(&p1)],
    };
    (p0, p1, whole, plan)
}

#[test]
fn complete_chunks_survive_reopen_with_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let (p0, p1, whole, chunks) = two_chunks();
    let m = create_member(aid(1), 0, "k", &whole);
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[whole.as_slice()]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    lane.commit_chunk_manifest(aid(1), 0, chunks.clone())
        .unwrap();
    lane.append_chunk(m.clone(), 0, p0.clone()).unwrap();
    lane.append_chunk(m, 1, p1.clone()).unwrap();

    let manifest = dir
        .path()
        .join("chunk-manifest")
        .join(format!("{}-0", aid(1)));
    assert!(manifest.is_file());
    assert!(dir
        .path()
        .join("chunk")
        .join(format!("{}-0-0", aid(1)))
        .is_file());
    assert!(dir
        .path()
        .join("chunk")
        .join(format!("{}-0-1", aid(1)))
        .is_file());

    let lane = lane.reopen().unwrap();
    assert_eq!(lane.heap().chunk_plan(aid(1), 0), Some(&chunks));
    let staged = lane.heap().inspect_staged(aid(1)).unwrap();
    assert_eq!(staged.len(), 1);
    assert!(staged[0].payload_complete);
    assert_eq!(staged[0].payload, whole);
    let bodies = staged[0].chunks.as_ref().expect("chunk slots");
    assert_eq!(bodies[0].as_deref(), Some(p0.as_slice()));
    assert_eq!(bodies[1].as_deref(), Some(p1.as_slice()));
    assert_no_ordinary_leak(lane.heap(), "k");
}

#[test]
fn after_first_chunk_reopen_keeps_prefix_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let (p0, _p1, whole, chunks) = two_chunks();
    let m = create_member(aid(2), 0, "k", &whole);
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[whole.as_slice()]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    lane.commit_chunk_manifest(aid(2), 0, chunks.clone())
        .unwrap();
    lane.arm(StagingFailpoint::AfterChunk {
        ordinal: 0,
        index: 0,
    });
    assert!(matches!(
        lane.append_chunk(m, 0, p0.clone()),
        Err(LaneError::Injected(StagingFailpoint::AfterChunk {
            ordinal: 0,
            index: 0
        }))
    ));

    assert!(!dir
        .path()
        .join("payload")
        .join(format!("{}-0", aid(2)))
        .exists());

    let mut lane = lane.reopen().unwrap();
    assert_eq!(lane.heap().chunk_plan(aid(2), 0), Some(&chunks));
    let staged = lane.heap().inspect_staged(aid(2)).unwrap();
    assert_eq!(staged.len(), 1);
    assert!(!staged[0].payload_complete);
    assert_eq!(staged[0].chunks.as_ref().unwrap()[0].as_deref(), Some(p0.as_slice()));
    assert!(staged[0].chunks.as_ref().unwrap()[1].is_none());
    assert!(lane.seal_member_boundary(aid(2)).is_err());
    assert_no_ordinary_leak(lane.heap(), "k");
}

#[test]
fn bad_chunk_hash_does_not_write_or_replace() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let (p0, _p1, whole, chunks) = two_chunks();
    let m = create_member(aid(3), 0, "k", &whole);
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[whole.as_slice()]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    lane.commit_chunk_manifest(aid(3), 0, chunks).unwrap();
    lane.append_chunk(m.clone(), 0, p0).unwrap();
    let chunk_path = dir.path().join("chunk").join(format!("{}-0-1", aid(3)));
    assert!(!chunk_path.exists());
    match lane.append_chunk(m, 1, b"xxxx".to_vec()) {
        Err(LaneError::Kernel(AtomicsError::Refused(AtomicRefuseReason::InvalidValue))) => {}
        other => panic!("expected InvalidValue, got {other:?}"),
    }
    assert!(!chunk_path.exists());
    assert!(!dir
        .path()
        .join("payload")
        .join(format!("{}-0", aid(3)))
        .exists());
}

#[test]
fn seal_after_chunks_is_durable_invisible() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let (p0, p1, whole, chunks) = two_chunks();
    let m = create_member(aid(4), 0, "k", &whole);
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[whole.as_slice()]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    lane.commit_chunk_manifest(aid(4), 0, chunks).unwrap();
    lane.append_chunk(m.clone(), 0, p0).unwrap();
    lane.append_chunk(m, 1, p1).unwrap();
    lane.seal_member_boundary(aid(4)).unwrap();
    assert!(dir
        .path()
        .join("sealed")
        .join(format!("{}", aid(4)))
        .is_file());

    let lane = lane.reopen().unwrap();
    assert_eq!(
        lane.heap().lifecycle(aid(4)).unwrap().members,
        MemberPhase::DurableInvisible
    );
    let staged = lane.heap().inspect_staged(aid(4)).unwrap();
    assert!(staged[0].payload_complete);
    assert_eq!(staged[0].payload, whole);
    assert_no_ordinary_leak(lane.heap(), "k");
}

#[test]
fn identical_chunk_append_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let (p0, p1, whole, chunks) = two_chunks();
    let m = create_member(aid(5), 0, "k", &whole);
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[whole.as_slice()]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    lane.commit_chunk_manifest(aid(5), 0, chunks.clone())
        .unwrap();
    lane.append_chunk(m.clone(), 0, p0.clone()).unwrap();
    lane.append_chunk(m.clone(), 0, p0).unwrap();
    lane.append_chunk(m.clone(), 1, p1).unwrap();
    lane.commit_chunk_manifest(aid(5), 0, chunks).unwrap();
    let staged = lane.heap().inspect_staged(aid(5)).unwrap();
    assert!(staged[0].payload_complete);
    assert_eq!(staged[0].payload, whole);
}

#[test]
fn payload_file_is_not_used_when_chunk_manifest_exists() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = DurableLane::create(dir.path(), hid(1), 1).unwrap();
    let (p0, p1, whole, chunks) = two_chunks();
    let m = create_member(aid(6), 0, "k", &whole);
    let plan = plan_for(hid(1), std::slice::from_ref(&m), &[whole.as_slice()]);
    lane.begin_prepare(&plan, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    lane.commit_chunk_manifest(aid(6), 0, chunks.clone())
        .unwrap();
    lane.append_chunk(m.clone(), 0, p0).unwrap();
    lane.append_chunk(m, 1, p1).unwrap();
    // Completeness wrote payload; recovery must still rebuild via chunks.
    assert!(dir
        .path()
        .join("payload")
        .join(format!("{}-0", aid(6)))
        .is_file());
    let _ = fs::remove_file(dir.path().join("checkpoint"));
    let lane = lane.reopen().unwrap();
    assert_eq!(lane.heap().chunk_plan(aid(6), 0), Some(&chunks));
    assert!(lane.heap().inspect_staged(aid(6)).unwrap()[0].payload_complete);
    assert_no_ordinary_leak(lane.heap(), "k");
}
