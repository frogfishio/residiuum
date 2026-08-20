//! ATM-4B: logical decision truth and surviving material truth are independent.

use residiuum_atomics::{
    decode_member, encode_decision, AtomicAbortReason, AtomicDecision, AtomicId, AtomicMember,
    AtomicOutcome, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, ChunkPlan,
    CollectionId, CoordinationScope, HeapId, LogicalStatus, MaterialStatus, MutationKind,
    ObjectIdentity, PlanMutation, ResourceLimits, VersionId,
};
use residiuum_format::{
    encode_atomic_commit_envelope, encode_atomic_frame, encode_frame, examine_atomic_frame,
    scan_forward, AtomicEvidenceClass, AtomicFrameRole, FrameKind, FrameParts, SafetyLimits,
    EMPTY_ENVELOPE,
};
use residiuum_store::{atomic_stage_checkpoint_path, StageEvidenceClass, StageEvidenceKind, Store};
use std::fs;
use std::path::Path;

const FRONTIER: [u8; 32] = [0xA1; 32];

fn aid() -> AtomicId {
    AtomicId::from_bytes([0x41; 32]).unwrap()
}

fn other_aid() -> AtomicId {
    AtomicId::from_bytes([0x71; 32]).unwrap()
}

fn cid() -> CollectionId {
    CollectionId::from_bytes([0x42; 16]).unwrap()
}

fn member(ordinal: u32, value: &[u8]) -> AtomicMember {
    let mut event = [0x43; 16];
    event[0] = ordinal as u8 + 1;
    AtomicMember {
        atomic_id: aid(),
        ordinal,
        object_identity: ObjectIdentity::new(
            cid(),
            CanonicalKey::String(format!("damage-{ordinal}")),
        ),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(*blake3::hash(value).as_bytes()),
        event_id: VersionId::from_bytes(event).unwrap(),
    }
}

fn plan(heap_id: HeapId, members: &[AtomicMember], values: &[&[u8]]) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: aid(),
        heap_id,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![],
        mutations: members
            .iter()
            .zip(values)
            .map(|(member, value)| PlanMutation {
                kind: member.member_kind,
                collection_id: member.object_identity.collection_id,
                key: member.object_identity.key.clone(),
                encoded_value: Some(value.to_vec()),
                if_version: member.before_version,
            })
            .collect(),
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}

fn media_files(store: &Store) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    for dir in [store.paths().active_dir(), store.paths().segments_dir()] {
        if let Ok(entries) = fs::read_dir(dir) {
            files.extend(
                entries
                    .flatten()
                    .map(|entry| entry.path())
                    .filter(|path| path.is_file()),
            );
        }
    }
    files.sort();
    files
}

/// Preserve the outer frame and Heap-qualified envelope while making one
/// member body undecodable. This is attributable material damage, not a global
/// unknown hole.
fn corrupt_atomic_body(store: &Store, role: AtomicFrameRole, ordinal: Option<u32>) {
    for path in media_files(store) {
        let mut bytes = fs::read(&path).unwrap();
        let scan = scan_forward(&bytes, SafetyLimits::draft_defaults());
        for region in scan.regions {
            let residiuum_format::ScanRegion::VerifiedFrame { frame, range } = region else {
                continue;
            };
            let Some(AtomicEvidenceClass::Valid(link)) = examine_atomic_frame(&frame) else {
                continue;
            };
            if link.role != role {
                continue;
            }
            if role == AtomicFrameRole::Member
                && decode_member(&frame.body).ok().map(|m| m.ordinal) != ordinal
            {
                continue;
            }
            let replacement = encode_frame(&FrameParts {
                header: frame.header,
                envelope: frame.envelope,
                body: vec![0xFF; frame.body.len()],
            })
            .unwrap();
            assert_eq!(replacement.len(), range.end as usize - range.start as usize);
            bytes[range.start as usize..range.end as usize].copy_from_slice(&replacement);
            fs::write(path, bytes).unwrap();
            return;
        }
    }
    panic!("Atomic role {role:?} ordinal {ordinal:?} not found");
}

#[test]
fn committed_member_damage_preserves_decision_and_healthy_sibling_across_checkpoint_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let members = [member(0, b"healthy"), member(1, b"damage-me")];
    let closed = plan(heap_id, &members, &[b"healthy", b"damage-me"]);
    store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_outcome(&closed)
        .unwrap();

    corrupt_atomic_body(&store, AtomicFrameRole::Member, Some(1));
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));

    for reopen in 0..2 {
        let stage = store.atomic_stage_for_heap(heap_id).unwrap();
        let status = stage.atomic_status(aid()).unwrap();
        assert_eq!(
            status.logical,
            LogicalStatus::Committed,
            "reopen {reopen}; findings={:?}",
            stage.findings().records
        );
        assert_eq!(
            status.material,
            MaterialStatus::Partial,
            "reopen {reopen}; report={:?}; findings={:?}",
            stage.open_report(),
            stage.findings().records
        );
        assert!(
            status.receipt.is_none(),
            "partial material cannot invent receipt"
        );
        let examined = stage.examine(aid());
        assert_eq!(examined.present_members, 1, "healthy sibling must survive");
        assert_eq!(examined.intended_members, 2);
        assert!(stage.findings().records.iter().any(|finding| {
            finding.heap_id == Some(heap_id)
                && finding.atomic_id == Some(aid())
                && finding.kind == StageEvidenceKind::Member
                && finding.class == StageEvidenceClass::Partial
        }));
    }
    drop(store);
    let mut reopened = Store::open(&path).expect("material damage must not kneel the whole store");
    assert_eq!(reopened.open_report().atomic_stage_publication_degraded, 1);
    let stage = reopened.atomic_stage_for_heap(heap_id).unwrap();
    let status = stage.atomic_status(aid()).unwrap();
    assert_eq!(status.logical, LogicalStatus::Committed);
    assert_eq!(status.material, MaterialStatus::Partial);
    assert_eq!(stage.examine(aid()).present_members, 1);
}

#[test]
fn damaged_decision_body_is_recovered_exactly_from_tombstone_and_detail() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let members = [member(0, b"one"), member(1, b"two")];
    let closed = plan(heap_id, &members, &[b"one", b"two"]);
    let original = store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_outcome(&closed)
        .unwrap();

    corrupt_atomic_body(&store, AtomicFrameRole::Commit, None);
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));

    for reopen in 0..2 {
        let mut stage = store.atomic_stage_for_heap(heap_id).unwrap();
        let status = stage.atomic_status(aid()).unwrap();
        assert_eq!(status.logical, LogicalStatus::Committed, "reopen {reopen}");
        assert_eq!(status.material, MaterialStatus::Complete);
        let receipt = status
            .receipt
            .expect("complete detail reconstructs receipt");
        assert_eq!(receipt.commit_position, 1);
        assert!(receipt.replayed);
        let replay = stage.decide_plan_outcome(&closed).unwrap();
        let (AtomicOutcome::Committed(original), AtomicOutcome::Committed(replay)) =
            (&original, replay)
        else {
            panic!("expected committed outcomes")
        };
        assert_eq!(replay.decision_hash, original.decision_hash);
        assert_eq!(replay.members, original.members);
        assert!(replay.replayed);
    }
}

fn write_payload_sidecar(
    path: &Path,
    heap_id: HeapId,
    atomic_id: AtomicId,
    ordinal: u32,
    payload: &[u8],
) {
    let mut body = b"ATPAY1".to_vec();
    body.extend_from_slice(heap_id.as_bytes());
    body.extend_from_slice(atomic_id.as_bytes());
    body.extend_from_slice(&ordinal.to_be_bytes());
    body.extend_from_slice(payload);
    let bytes =
        encode_atomic_frame(FrameKind::PayloadChunk, EMPTY_ENVELOPE, &body, [0x61; 16]).unwrap();
    fs::write(path, bytes).unwrap();
}

#[test]
fn conflicting_payload_does_not_rewrite_committed_logical_truth() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let members = [member(0, b"one")];
    let closed = plan(heap_id, &members, &[b"one"]);
    store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_outcome(&closed)
        .unwrap();

    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    write_payload_sidecar(
        &segs.join("conflicting-payload.residiuum"),
        heap_id,
        aid(),
        0,
        b"different",
    );
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));

    for reopen in 0..2 {
        let stage = store.atomic_stage_for_heap(heap_id).unwrap();
        let status = stage.atomic_status(aid()).unwrap();
        assert_eq!(status.logical, LogicalStatus::Committed, "reopen {reopen}");
        assert_eq!(status.material, MaterialStatus::Conflicting);
        assert!(status.receipt.is_none());
        assert!(stage.findings().records.iter().any(|finding| {
            finding.heap_id == Some(heap_id)
                && finding.atomic_id == Some(aid())
                && finding.kind == StageEvidenceKind::Payload
                && finding.class == StageEvidenceClass::Conflict
        }));
    }
}

#[test]
fn damaged_prepare_preserves_exact_commit_but_degrades_material() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let members = [member(0, b"one"), member(1, b"two")];
    let closed = plan(heap_id, &members, &[b"one", b"two"]);
    store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_outcome(&closed)
        .unwrap();

    corrupt_atomic_body(&store, AtomicFrameRole::Prepare, None);
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    for reopen in 0..2 {
        let stage = store.atomic_stage_for_heap(heap_id).unwrap();
        let status = stage.atomic_status(aid()).unwrap();
        assert_eq!(status.logical, LogicalStatus::Committed, "reopen {reopen}");
        assert_eq!(status.material, MaterialStatus::Partial);
        assert_eq!(stage.examine(aid()).present_members, 2);
        assert!(status.receipt.is_none());
    }
}

fn write_seal_sidecar(path: &Path, heap_id: HeapId, root: [u8; 32]) {
    let mut body = b"ATSEAL1".to_vec();
    body.extend_from_slice(heap_id.as_bytes());
    body.extend_from_slice(aid().as_bytes());
    body.extend_from_slice(&root);
    let bytes =
        encode_atomic_frame(FrameKind::PayloadChunk, EMPTY_ENVELOPE, &body, [0x62; 16]).unwrap();
    fs::write(path, bytes).unwrap();
}

#[test]
fn conflicting_seal_is_material_conflict_not_decision_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let members = [member(0, b"one")];
    let closed = plan(heap_id, &members, &[b"one"]);
    store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_outcome(&closed)
        .unwrap();
    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    write_seal_sidecar(
        &segs.join("conflicting-seal.residiuum"),
        heap_id,
        [0x73; 32],
    );
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));

    for reopen in 0..2 {
        let stage = store.atomic_stage_for_heap(heap_id).unwrap();
        let status = stage.atomic_status(aid()).unwrap();
        assert_eq!(status.logical, LogicalStatus::Committed, "reopen {reopen}");
        assert_eq!(status.material, MaterialStatus::Conflicting);
    }
}

fn write_order_frontier_sidecar(path: &Path, heap_id: HeapId) {
    let mut body = b"ATORD1".to_vec();
    body.extend_from_slice(heap_id.as_bytes());
    body.extend_from_slice(aid().as_bytes());
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&[0x7A; 16]);
    body.extend_from_slice(&999u64.to_be_bytes());
    let bytes =
        encode_atomic_frame(FrameKind::PayloadChunk, EMPTY_ENVELOPE, &body, [0x63; 16]).unwrap();
    fs::write(path, bytes).unwrap();
}

#[test]
fn conflicting_order_witness_is_material_conflict_not_decision_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let members = [member(0, b"one")];
    let closed = plan(heap_id, &members, &[b"one"]);
    store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_outcome(&closed)
        .unwrap();
    let extra = store
        .paths()
        .segments_dir()
        .join("conflicting-order.residiuum");
    fs::create_dir_all(store.paths().segments_dir()).unwrap();
    write_order_frontier_sidecar(&extra, heap_id);
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));

    for _ in 0..2 {
        let stage = store.atomic_stage_for_heap(heap_id).unwrap();
        let status = stage.atomic_status(aid()).unwrap();
        assert_eq!(status.logical, LogicalStatus::Committed);
        assert_eq!(status.material, MaterialStatus::Conflicting);
        assert!(stage.findings().records.iter().any(|finding| {
            finding.heap_id == Some(heap_id)
                && finding.atomic_id == Some(aid())
                && finding.kind == StageEvidenceKind::OrderFrontier
                && finding.class == StageEvidenceClass::Conflict
        }));
    }
}

fn corrupt_sidecar_body(store: &Store, magic: &[u8]) {
    for path in media_files(store) {
        let mut bytes = fs::read(&path).unwrap();
        let scan = scan_forward(&bytes, SafetyLimits::draft_defaults());
        for region in scan.regions {
            let residiuum_format::ScanRegion::VerifiedFrame { frame, range } = region else {
                continue;
            };
            if !frame.body.starts_with(magic) {
                continue;
            }
            let mut body = frame.body;
            let mutate_at = if magic == b"ATTOMB2" {
                b"ATTOMB2".len() + 16 + 32 + 8 + 4
            } else {
                body.len() - 1
            };
            body[mutate_at] ^= 0xFF;
            let replacement = encode_frame(&FrameParts {
                header: frame.header,
                envelope: frame.envelope,
                body,
            })
            .unwrap();
            assert_eq!(replacement.len(), range.end as usize - range.start as usize);
            bytes[range.start as usize..range.end as usize].copy_from_slice(&replacement);
            fs::write(path, bytes).unwrap();
            return;
        }
    }
    panic!("sidecar {:?} not found", String::from_utf8_lossy(magic));
}

#[test]
fn damaged_tombstone_does_not_erase_healthy_detailed_decision() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let members = [member(0, b"one")];
    let closed = plan(heap_id, &members, &[b"one"]);
    store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_outcome(&closed)
        .unwrap();
    corrupt_sidecar_body(&store, b"ATTOMB2");
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));

    for reopen in 0..2 {
        let stage = store.atomic_stage_for_heap(heap_id).unwrap();
        let status = stage.atomic_status(aid()).unwrap();
        assert_eq!(
            status.logical,
            LogicalStatus::Committed,
            "reopen {reopen}; findings={:?}",
            stage.findings().records
        );
        assert_eq!(status.material, MaterialStatus::Complete);
        assert!(status.receipt.is_some());
    }
}

#[test]
fn damaged_lifetime_index_never_proves_absence_or_hides_exact_decision() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let members = [member(0, b"one")];
    let closed = plan(heap_id, &members, &[b"one"]);
    store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_outcome(&closed)
        .unwrap();
    let index_path = store.paths().store_info().join("atomic-tombstones.idx");
    let stage = store.atomic_stage_for_heap(heap_id).unwrap();
    let mut index = fs::read(&index_path).unwrap();
    let midpoint = index.len() / 2;
    index[midpoint] ^= 0x80;
    fs::write(&index_path, index).unwrap();

    let known = stage.atomic_status(aid()).unwrap();
    assert_eq!(known.logical, LogicalStatus::Committed);
    assert_eq!(known.material, MaterialStatus::CoverageIncomplete);
    assert!(known.receipt.is_none());
    let absent = stage.atomic_status(other_aid()).unwrap();
    assert_eq!(absent.logical, LogicalStatus::UnknownCommit);
    assert_eq!(absent.material, MaterialStatus::CoverageIncomplete);
}

#[test]
fn missing_covered_atomic_file_preserves_known_decision_and_refuses_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let members = [member(0, b"one")];
    let closed = plan(heap_id, &members, &[b"one"]);
    store
        .atomic_stage_for_heap(heap_id)
        .unwrap()
        .decide_plan_outcome(&closed)
        .unwrap();
    let extra = store.paths().segments_dir().join("covered-extra.residiuum");
    fs::create_dir_all(store.paths().segments_dir()).unwrap();
    write_payload_sidecar(&extra, heap_id, aid(), 0, b"one");
    {
        let stage = store.atomic_stage_for_heap(heap_id).unwrap();
        assert!(!stage.open_report().coverage_degraded);
    }
    fs::remove_file(extra).unwrap();

    for reopen in 0..2 {
        let stage = store.atomic_stage_for_heap(heap_id).unwrap();
        let known = stage.atomic_status(aid()).unwrap();
        assert_eq!(known.logical, LogicalStatus::Committed, "reopen {reopen}");
        assert_eq!(known.material, MaterialStatus::CoverageIncomplete);
        let absent = stage.atomic_status(other_aid()).unwrap();
        assert_eq!(absent.logical, LogicalStatus::UnknownCommit);
        assert_eq!(absent.material, MaterialStatus::CoverageIncomplete);
    }
    drop(store);
    let mut reopened = Store::open(dir.path().join("s"))
        .expect("coverage damage must preserve access to healthy store material");
    assert_eq!(reopened.open_report().atomic_stage_publication_degraded, 1);
    let stage = reopened.atomic_stage_for_heap(heap_id).unwrap();
    assert_eq!(
        stage.atomic_status(aid()).unwrap().logical,
        LogicalStatus::Committed
    );
    assert_eq!(
        stage.atomic_status(other_aid()).unwrap().logical,
        LogicalStatus::UnknownCommit
    );
}

fn flip_member_frame_byte(store: &Store, ordinal: u32, selector: usize) {
    for path in media_files(store) {
        let mut bytes = fs::read(&path).unwrap();
        let scan = scan_forward(&bytes, SafetyLimits::draft_defaults());
        for region in scan.regions {
            let residiuum_format::ScanRegion::VerifiedFrame { frame, range } = region else {
                continue;
            };
            if !matches!(
                examine_atomic_frame(&frame),
                Some(AtomicEvidenceClass::Valid(ref link))
                    if link.role == AtomicFrameRole::Member
            ) || decode_member(&frame.body).ok().map(|m| m.ordinal) != Some(ordinal)
            {
                continue;
            }
            let len = range.end as usize - range.start as usize;
            let relative = match selector {
                0 => 0,
                1 => len / 2,
                _ => len - 1,
            };
            bytes[range.start as usize + relative] ^= 1;
            fs::write(path, bytes).unwrap();
            return;
        }
    }
    panic!("member frame not found");
}

#[test]
fn negative_control_physical_member_head_middle_tail_flips_change_material_truth() {
    for selector in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::create(dir.path().join(format!("flip-{selector}"))).unwrap();
        let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
        let members = [member(0, b"one"), member(1, b"two")];
        let closed = plan(heap_id, &members, &[b"one", b"two"]);
        store
            .atomic_stage_for_heap(heap_id)
            .unwrap()
            .decide_plan_outcome(&closed)
            .unwrap();
        let before = store
            .atomic_stage_for_heap(heap_id)
            .unwrap()
            .atomic_status(aid())
            .unwrap();
        assert_eq!(before.logical, LogicalStatus::Committed);
        assert_eq!(before.material, MaterialStatus::Complete);
        assert!(before.receipt.is_some());
        flip_member_frame_byte(&store, 1, selector);
        let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));

        let stage = store.atomic_stage_for_heap(heap_id).unwrap();
        let status = stage.atomic_status(aid()).unwrap();
        assert_eq!(
            status.logical,
            LogicalStatus::Committed,
            "selector {selector}"
        );
        assert_eq!(status.material, MaterialStatus::CoverageIncomplete);
        assert!(status.receipt.is_none());
        assert!(stage.open_report().holes >= 1);
        assert!(stage.findings().records.iter().any(|finding| {
            finding.kind == StageEvidenceKind::Hole && finding.class == StageEvidenceClass::Corrupt
        }));
    }
}

fn truncate_tombstone_frame(store: &Store, selector: usize) {
    for path in media_files(store) {
        let bytes = fs::read(&path).unwrap();
        let scan = scan_forward(&bytes, SafetyLimits::draft_defaults());
        for region in scan.regions {
            let residiuum_format::ScanRegion::VerifiedFrame { frame, range } = region else {
                continue;
            };
            if !frame.body.starts_with(b"ATTOMB2") {
                continue;
            }
            let len = range.end - range.start;
            let retained = match selector {
                0 => 1,
                1 => len / 2,
                _ => len - 1,
            };
            let file = fs::OpenOptions::new().write(true).open(path).unwrap();
            file.set_len(range.start + retained).unwrap();
            return;
        }
    }
    panic!("tombstone frame not found");
}

#[test]
fn tombstone_truncation_boundaries_preserve_detailed_commit_and_degrade_coverage() {
    for selector in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::create(dir.path().join(format!("trunc-{selector}"))).unwrap();
        let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
        let members = [member(0, b"one")];
        let closed = plan(heap_id, &members, &[b"one"]);
        store
            .atomic_stage_for_heap(heap_id)
            .unwrap()
            .decide_plan_outcome(&closed)
            .unwrap();
        truncate_tombstone_frame(&store, selector);
        let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));

        let stage = store.atomic_stage_for_heap(heap_id).unwrap();
        let status = stage.atomic_status(aid()).unwrap();
        assert_eq!(
            status.logical,
            LogicalStatus::Committed,
            "selector {selector}"
        );
        assert_eq!(status.material, MaterialStatus::CoverageIncomplete);
        assert!(status.receipt.is_none());
    }
}

#[test]
fn chunk_plan_and_body_damage_are_material_conflicts_not_logical_conflicts() {
    for magic in [b"ATMAP1".as_slice(), b"ATCHK1".as_slice()] {
        let dir = tempfile::tempdir().unwrap();
        let mut store =
            Store::create(dir.path().join(String::from_utf8_lossy(magic).as_ref())).unwrap();
        let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
        let m = member(0, b"secret");
        let closed = plan(heap_id, std::slice::from_ref(&m), &[b"secret"]);
        let first = b"se";
        let second = b"cret";
        let chunks = ChunkPlan {
            total: 2,
            chunk_hashes: vec![
                *blake3::hash(first).as_bytes(),
                *blake3::hash(second).as_bytes(),
            ],
        };
        {
            let mut stage = store.atomic_stage_for_heap(heap_id).unwrap();
            stage
                .begin_prepare(&closed, FRONTIER, std::slice::from_ref(&m))
                .unwrap();
            stage.commit_chunk_manifest(aid(), 0, chunks).unwrap();
            stage.append_chunk(m.clone(), 0, first.to_vec()).unwrap();
            stage.append_chunk(m, 1, second.to_vec()).unwrap();
            stage.seal_member_boundary(aid()).unwrap();
            stage.persist_committed_decision(aid()).unwrap();
        }
        corrupt_sidecar_body(&store, magic);
        let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));

        for _ in 0..2 {
            let stage = store.atomic_stage_for_heap(heap_id).unwrap();
            let status = stage.atomic_status(aid()).unwrap();
            assert_eq!(status.logical, LogicalStatus::Committed, "{magic:?}");
            assert_eq!(status.material, MaterialStatus::Conflicting, "{magic:?}");
            assert!(stage.findings().records.iter().any(|finding| {
                finding.heap_id == Some(heap_id)
                    && finding.atomic_id == Some(aid())
                    && finding.class == StageEvidenceClass::Conflict
                    && matches!(
                        finding.kind,
                        StageEvidenceKind::ChunkPlan | StageEvidenceKind::ChunkBody
                    )
            }));
        }
    }
}

fn write_decision(path: &Path, heap_id: HeapId, decision: &AtomicDecision, event: u8) {
    let content_root = [0x51; 32];
    let envelope = encode_atomic_commit_envelope(
        heap_id.as_bytes(),
        decision.atomic_id.as_bytes(),
        &content_root,
        decision.commit_position,
    )
    .unwrap();
    let body = encode_decision(decision).unwrap();
    let mut event_id = [0x52; 16];
    event_id[0] = event;
    let bytes = encode_atomic_frame(FrameKind::BatchCommit, &envelope, &body, event_id).unwrap();
    fs::write(path, bytes).unwrap();
}

#[test]
fn contradictory_valid_decisions_are_logical_conflict_in_both_file_orders() {
    for reverse in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(if reverse { "ba" } else { "ab" });
        let mut store = Store::create(&path).unwrap();
        let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
        let committed = AtomicDecision::committed(aid(), [1; 32], [2; 32], 1, 7).unwrap();
        let aborted = AtomicDecision::not_committed(
            aid(),
            [1; 32],
            [2; 32],
            1,
            AtomicAbortReason::RecoveryAbort,
        );
        let (first, second) = if reverse {
            (&aborted, &committed)
        } else {
            (&committed, &aborted)
        };
        let segs = store.paths().segments_dir();
        fs::create_dir_all(&segs).unwrap();
        write_decision(&segs.join("aaa.residiuum"), heap_id, first, 1);
        write_decision(&segs.join("bbb.residiuum"), heap_id, second, 2);
        let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));

        for reopen in 0..2 {
            let stage = store.atomic_stage_for_heap(heap_id).unwrap();
            let status = stage.atomic_status(aid()).unwrap();
            assert_eq!(
                status.logical,
                LogicalStatus::ConflictingDecisionEvidence,
                "reverse={reverse} reopen={reopen}"
            );
            assert_eq!(status.material, MaterialStatus::Conflicting);
        }
    }
}
