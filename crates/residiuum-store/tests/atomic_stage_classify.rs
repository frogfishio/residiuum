//! CR-ATMR5-002: store catalogue does not guess through damage or conflicts.

use residiuum_atomics::{
    encode_member, encode_prepare, prepare_from_closed_plan, AtomicId, AtomicMember, AtomicPlan,
    AtomicPlanParts, AtomicProfile, CanonicalKey, ChunkPlan, CollectionId, ContentRoot,
    CoordinationScope, HeapId, MutationKind, ObjectIdentity, PlanMutation, ResourceLimits,
    VersionId,
};
use residiuum_format::{
    encode_atomic_frame, encode_atomic_member_envelope, encode_atomic_prepare_envelope,
    encode_frame, FrameHeader, FrameKind, FrameParts, EMPTY_ENVELOPE,
};
use residiuum_store::{
    atomic_stage_checkpoint_path, AtomicStageClass, StageEvidenceClass, StageEvidenceKind, Store,
    StoreError,
};
use std::fs;
use std::path::Path;

const FRONTIER: [u8; 32] = [0xA1; 32];

fn aid() -> AtomicId {
    let mut b = [0u8; 32];
    b[0] = 9;
    AtomicId::from_bytes(b).unwrap()
}

fn cid() -> CollectionId {
    let mut b = [0u8; 16];
    b[0] = 2;
    CollectionId::from_bytes(b).unwrap()
}

fn vid() -> VersionId {
    let mut b = [0u8; 16];
    b[0] = 3;
    VersionId::from_bytes(b).unwrap()
}

fn member() -> AtomicMember {
    AtomicMember {
        atomic_id: aid(),
        ordinal: 0,
        object_identity: ObjectIdentity::new(cid(), CanonicalKey::String("k".into())),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(*blake3::hash(b"secret").as_bytes()),
        event_id: vid(),
    }
}

fn plan(heap: HeapId, members: &[AtomicMember], value: &[u8]) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: members[0].atomic_id,
        heap_id: heap,
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: vec![],
        predicates: vec![],
        mutations: members
            .iter()
            .map(|m| PlanMutation {
                kind: m.member_kind,
                collection_id: m.object_identity.collection_id,
                key: m.object_identity.key.clone(),
                encoded_value: Some(value.to_vec()),
                if_version: m.before_version,
            })
            .collect(),
        active_rule_revisions: vec![],
        limits: ResourceLimits::hard_local_heap(),
    })
    .unwrap()
}

fn write_prepare_file(path: &Path, prepare: &residiuum_atomics::AtomicPrepare, event: u8) {
    let env = encode_atomic_prepare_envelope(
        prepare.heap_id.as_bytes(),
        prepare.atomic_id.as_bytes(),
        prepare.content_root.as_bytes(),
    )
    .unwrap();
    let body = encode_prepare(prepare).unwrap();
    let mut event_id = [0u8; 16];
    event_id[0] = event;
    let bytes = encode_atomic_frame(FrameKind::BatchPrepare, &env, &body, event_id).unwrap();
    fs::write(path, bytes).unwrap();
}

fn write_payload_chunk(path: &Path, body: Vec<u8>, event: u8) {
    let mut event_id = [0u8; 16];
    event_id[0] = event;
    let parts = FrameParts {
        header: FrameHeader::new_draft(
            FrameKind::PayloadChunk,
            EMPTY_ENVELOPE.len() as u32,
            body.len() as u64,
            event_id,
        ),
        envelope: EMPTY_ENVELOPE.to_vec(),
        body,
    };
    fs::write(path, encode_frame(&parts).unwrap()).unwrap();
}

fn two_prepares(
    store_id: [u8; 16],
) -> (
    residiuum_atomics::AtomicPrepare,
    residiuum_atomics::AtomicPrepare,
) {
    let heap = HeapId::from_bytes(store_id).unwrap();
    let m = member();
    let a = prepare_from_closed_plan(
        &plan(heap, std::slice::from_ref(&m), b"secret"),
        FRONTIER,
        std::slice::from_ref(&m),
    )
    .unwrap();
    let other_frontier = [0xB2; 32];
    let b = prepare_from_closed_plan(
        &plan(heap, std::slice::from_ref(&m), b"secret"),
        other_frontier,
        &[m],
    )
    .unwrap();
    assert_ne!(a, b);
    (a, b)
}

#[test]
fn conflicting_prepares_same_result_regardless_of_file_order() {
    let dir = tempfile::tempdir().unwrap();
    for tag in ["ab", "ba"] {
        let root = dir.path().join(tag);
        let mut store = Store::create(&root).unwrap();
        let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
        let (a, b) = two_prepares(store.store_id());
        let (left, right) = if tag == "ab" { (&a, &b) } else { (&b, &a) };
        let segs = store.paths().segments_dir();
        fs::create_dir_all(&segs).unwrap();
        write_prepare_file(&segs.join("aaa.residiuum"), left, 1);
        write_prepare_file(&segs.join("bbb.residiuum"), right, 2);
        let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
        let stage = store.atomic_stage().unwrap();
        assert!(
            stage.kernel().placement(aid()).is_none(),
            "{tag}: conflicting identity must not be installed"
        );
        assert!(
            stage
                .findings()
                .records
                .iter()
                .any(|f| f.class == StageEvidenceClass::Conflict && f.atomic_id == Some(aid())),
            "{tag}: expected Conflict, got {:?}",
            stage.findings().records
        );
        drop(stage);
        let p = plan(heap_id, std::slice::from_ref(&member()), b"secret");
        let mut stage = store.atomic_stage().unwrap();
        match stage.begin_prepare(&p, FRONTIER, &[member()]) {
            Err(StoreError::AtomicStage(msg)) => {
                assert!(
                    msg.contains("blocked"),
                    "{tag}: identity must not be reusable, got {msg}"
                );
            }
            Ok(_) => panic!("{tag}: blocked identity must not prepare"),
            Err(other) => panic!("{tag}: unexpected {other}"),
        }
    }
}

#[test]
fn mutated_prepare_sidecar_is_corrupt_not_absent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    let mut body = b"ATPREP1".to_vec();
    body.extend_from_slice(&[0xFF; 24]);
    write_payload_chunk(&segs.join("bad-prep.residiuum"), body, 7);
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    let stage = store.atomic_stage().unwrap();
    assert!(stage
        .findings()
        .records
        .iter()
        .any(|f| f.kind == StageEvidenceKind::Prepare
            && matches!(
                f.class,
                StageEvidenceClass::Corrupt | StageEvidenceClass::Partial
            )));
    assert!(stage.kernel().placement(aid()).is_none());
}

#[test]
fn other_heap_prepare_is_catalogued_but_capability_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let mut foreign_id = store.store_id();
    foreign_id[0] ^= 0xFF;
    let foreign_heap = HeapId::from_bytes(foreign_id).unwrap();
    let m = member();
    let foreign_plan = plan(foreign_heap, std::slice::from_ref(&m), b"secret");
    let prepare = prepare_from_closed_plan(&foreign_plan, FRONTIER, &[m]).unwrap();
    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    write_prepare_file(&segs.join("foreign.residiuum"), &prepare, 3);
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    {
        let stage = store.atomic_stage().unwrap();
        assert!(stage.kernel().placement(aid()).is_none());
        assert_eq!(stage.examine(aid()).class, AtomicStageClass::Absent);
        assert!(!stage
            .findings()
            .records
            .iter()
            .any(|f| f.class == StageEvidenceClass::ForeignHeap && f.atomic_id == Some(aid())));
    }
    let stage = store.atomic_stage_for_heap(foreign_heap).unwrap();
    assert_eq!(stage.examine(aid()).class, AtomicStageClass::Prepared);
}

#[test]
fn mismatched_seal_root_cannot_seal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m), b"secret");
    {
        let mut stage = store.atomic_stage().unwrap();
        stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        stage.append_staged(m, b"secret".to_vec()).unwrap();
    }
    let mut wrong = [0x11u8; 32];
    wrong[0] = 0x5E;
    let root = ContentRoot::from_bytes(wrong).unwrap();
    let mut body = b"ATSEAL1".to_vec();
    body.extend_from_slice(aid().as_bytes());
    body.extend_from_slice(root.as_bytes());
    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    write_payload_chunk(&segs.join("bad-seal.residiuum"), body, 8);
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    let stage = store.atomic_stage().unwrap();
    let life = stage.kernel().lifecycle(aid());
    assert!(
        life.is_none()
            || !matches!(
                life.unwrap().members,
                residiuum_atomics::MemberPhase::DurableInvisible
            ),
        "mismatched seal must not invent the first stable boundary"
    );
    assert!(stage
        .findings()
        .records
        .iter()
        .any(|f| f.kind == StageEvidenceKind::Seal && f.class == StageEvidenceClass::Conflict));
}

#[test]
fn holes_are_reported_not_swallowed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    fs::write(segs.join("garbage.residiuum"), b"ATPAY1-damaged").unwrap();
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    let stage = store.atomic_stage().unwrap();
    assert!(
        stage.open_report().holes >= 1,
        "physical holes must be counted, report={:?}",
        stage.open_report()
    );
    assert!(stage
        .findings()
        .records
        .iter()
        .any(|f| f.kind == StageEvidenceKind::Hole));
}

fn refuse_reuse(store: &mut Store) {
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let p = plan(heap_id, std::slice::from_ref(&member()), b"secret");
    let mut stage = store.atomic_stage().unwrap();
    match stage.begin_prepare(&p, FRONTIER, &[member()]) {
        Err(StoreError::AtomicStage(msg)) => {
            assert!(
                msg.contains("blocked"),
                "identity must stay blocked, got {msg}"
            );
        }
        Ok(_) => panic!("blocked identity must not prepare"),
        Err(other) => panic!("expected blocked AtomicStage, got {other}"),
    }
}

#[test]
fn mismatched_seal_stays_blocked_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m), b"secret");
    {
        let mut stage = store.atomic_stage().unwrap();
        stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        stage.append_staged(m, b"secret".to_vec()).unwrap();
    }
    let mut wrong = [0x11u8; 32];
    wrong[0] = 0x5E;
    let root = ContentRoot::from_bytes(wrong).unwrap();
    let mut body = b"ATSEAL1".to_vec();
    body.extend_from_slice(aid().as_bytes());
    body.extend_from_slice(root.as_bytes());
    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    write_payload_chunk(&segs.join("bad-seal.residiuum"), body, 8);
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    {
        let stage = store.atomic_stage().unwrap();
        assert!(stage
            .findings()
            .records
            .iter()
            .any(|f| f.kind == StageEvidenceKind::Seal && f.class == StageEvidenceClass::Conflict));
    }
    {
        let stage = store.atomic_stage().unwrap();
        assert!(
            stage
                .findings()
                .records
                .iter()
                .any(|f| f.kind == StageEvidenceKind::Seal
                    && f.class == StageEvidenceClass::Conflict),
            "seal conflict must survive checkpoint reopen, got {:?}",
            stage.findings().records
        );
    }
    refuse_reuse(&mut store);
}

#[test]
fn conflicting_seals_stay_blocked_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap, std::slice::from_ref(&m), b"secret");
    let root_a = {
        let mut stage = store.atomic_stage().unwrap();
        let (_, placement) = stage
            .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
            .unwrap();
        stage.append_staged(m, b"secret".to_vec()).unwrap();
        placement.content_root()
    };
    let mut other = *root_a.as_bytes();
    other[0] ^= 0xFF;
    let root_b = ContentRoot::from_bytes(other).unwrap();
    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    for (name, root, ev) in [
        ("seal-a.residiuum", root_a, 21u8),
        ("seal-b.residiuum", root_b, 22),
    ] {
        let mut body = b"ATSEAL1".to_vec();
        body.extend_from_slice(aid().as_bytes());
        body.extend_from_slice(root.as_bytes());
        write_payload_chunk(&segs.join(name), body, ev);
    }
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    {
        let stage = store.atomic_stage().unwrap();
        assert!(stage
            .findings()
            .records
            .iter()
            .any(|f| f.kind == StageEvidenceKind::Seal && f.class == StageEvidenceClass::Conflict));
    }
    {
        let stage = store.atomic_stage().unwrap();
        assert!(stage
            .findings()
            .records
            .iter()
            .any(|f| f.kind == StageEvidenceKind::Seal && f.class == StageEvidenceClass::Conflict));
    }
    refuse_reuse(&mut store);
}

fn write_member_file(path: &Path, heap: HeapId, member: &AtomicMember, root: ContentRoot) {
    let env = encode_atomic_member_envelope(
        heap.as_bytes(),
        member.atomic_id.as_bytes(),
        u64::from(member.ordinal),
        root.as_bytes(),
        None,
    )
    .unwrap();
    let body = encode_member(member).unwrap();
    let bytes = encode_atomic_frame(
        FrameKind::ItemEvent,
        &env,
        &body,
        member.event_id.to_bytes(),
    )
    .unwrap();
    fs::write(path, bytes).unwrap();
}

fn write_orphan_role(segs: &Path, heap: HeapId, role: &str) {
    match role {
        "member" => write_member_file(
            &segs.join("orphan-member.residiuum"),
            heap,
            &member(),
            ContentRoot::from_bytes([3u8; 32]).unwrap(),
        ),
        "payload" => {
            let mut body = b"ATPAY1".to_vec();
            body.extend_from_slice(aid().as_bytes());
            body.extend_from_slice(&0u32.to_be_bytes());
            body.extend_from_slice(b"secret");
            write_payload_chunk(&segs.join("orphan-pay.residiuum"), body, 31);
        }
        "seal" => {
            let mut body = b"ATSEAL1".to_vec();
            body.extend_from_slice(aid().as_bytes());
            body.extend_from_slice(&[0x44u8; 32]);
            write_payload_chunk(&segs.join("orphan-seal.residiuum"), body, 32);
        }
        "chunk-plan" => {
            let plan = ChunkPlan {
                total: 2,
                chunk_hashes: vec![[0x55u8; 32], [0x56u8; 32]],
            };
            let mut body = b"ATMAP1".to_vec();
            body.extend_from_slice(aid().as_bytes());
            body.extend_from_slice(&0u32.to_be_bytes());
            body.extend_from_slice(&plan.total.to_be_bytes());
            for hash in &plan.chunk_hashes {
                body.extend_from_slice(hash);
            }
            write_payload_chunk(&segs.join("orphan-map.residiuum"), body, 33);
        }
        "chunk-body" => {
            let mut body = b"ATCHK1".to_vec();
            body.extend_from_slice(aid().as_bytes());
            body.extend_from_slice(&0u32.to_be_bytes());
            body.extend_from_slice(&0u32.to_be_bytes());
            body.extend_from_slice(b"chunk");
            write_payload_chunk(&segs.join("orphan-chk.residiuum"), body, 34);
        }
        other => panic!("unknown orphan role {other}"),
    }
}

#[test]
fn orphan_roles_before_prepare_block_reuse() {
    for role in ["member", "payload", "seal", "chunk-plan", "chunk-body"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(role);
        let mut store = Store::create(&path).unwrap();
        let heap = HeapId::from_bytes(store.store_id()).unwrap();
        let segs = store.paths().segments_dir();
        fs::create_dir_all(&segs).unwrap();
        write_orphan_role(&segs, heap, role);
        let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
        {
            let stage = store.atomic_stage().unwrap();
            assert!(
                stage
                    .findings()
                    .records
                    .iter()
                    .any(|f| f.atomic_id == Some(aid()) && f.class == StageEvidenceClass::Corrupt),
                "{role}: expected orphan damage, got {:?}",
                stage.findings().records
            );
            assert!(stage.kernel().placement(aid()).is_none());
        }
        {
            let stage = store.atomic_stage().unwrap();
            assert!(
                stage
                    .findings()
                    .records
                    .iter()
                    .any(|f| f.atomic_id == Some(aid()) && f.class == StageEvidenceClass::Corrupt),
                "{role}: orphan must survive reopen, got {:?}",
                stage.findings().records
            );
        }
        refuse_reuse(&mut store);
    }
}

#[test]
fn holes_remain_after_two_checkpoint_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    fs::write(segs.join("garbage.residiuum"), b"ATPAY1-damaged").unwrap();
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    {
        let stage = store.atomic_stage().unwrap();
        assert!(stage.open_report().holes >= 1);
        assert!(stage
            .findings()
            .records
            .iter()
            .any(|f| f.kind == StageEvidenceKind::Hole));
    }
    {
        let stage = store.atomic_stage().unwrap();
        assert!(
            stage.open_report().holes >= 1,
            "holes must persist across checkpoint skip, report={:?}",
            stage.open_report()
        );
        assert!(stage
            .findings()
            .records
            .iter()
            .any(|f| f.kind == StageEvidenceKind::Hole));
    }
}

#[test]
fn missing_covered_file_degrades_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let segs = store.paths().segments_dir();
    fs::create_dir_all(&segs).unwrap();
    let bomb = segs.join("atomic.residiuum");
    let heap = HeapId::from_bytes(store.store_id()).unwrap();
    let p = prepare_from_closed_plan(&plan(heap, &[member()], b"secret"), FRONTIER, &[member()])
        .unwrap();
    write_prepare_file(&bomb, &p, 31);
    let exact = fs::read(&bomb).unwrap();
    let _ = fs::remove_file(atomic_stage_checkpoint_path(store.paths()));
    {
        let stage = store.atomic_stage().unwrap();
        assert!(!stage.open_report().coverage_degraded);
    }
    fs::remove_file(&bomb).unwrap();
    {
        let mut stage = store.atomic_stage().unwrap();
        assert!(
            stage.open_report().coverage_degraded,
            "missing covered media must degrade coverage"
        );
        assert!(stage
            .findings()
            .records
            .iter()
            .any(|f| f.kind == StageEvidenceKind::Coverage));
        let mut other_member = member();
        let mut other_id = [0u8; 32];
        other_id[0] = 44;
        other_member.atomic_id = AtomicId::from_bytes(other_id).unwrap();
        let other = plan(heap, std::slice::from_ref(&other_member), b"secret");
        match stage.begin_prepare(&other, FRONTIER, std::slice::from_ref(&other_member)) {
            Err(StoreError::AtomicStage(msg)) => assert!(msg.contains("coverage")),
            Ok(_) => panic!("new identity must be refused while coverage is incomplete"),
            Err(other) => panic!("expected coverage refusal, got {other}"),
        }
        match stage.scrub_coverage() {
            Err(StoreError::AtomicStage(msg)) => {
                assert!(msg.contains("scrub"), "expected scrub refusal, got {msg}");
            }
            Ok(()) => panic!("scrub must refuse while covered media is missing"),
            Err(other) => panic!("expected scrub refusal, got {other}"),
        }
        fs::write(&bomb, vec![0xA5; exact.len()]).unwrap();
        assert!(
            stage.scrub_coverage().is_err(),
            "arbitrary replacement must not authenticate"
        );
        fs::write(&bomb, &exact).unwrap();
        stage.scrub_coverage().unwrap();
        assert!(!stage.open_report().coverage_degraded);
    }
    {
        let stage = store.atomic_stage().unwrap();
        assert!(!stage.open_report().coverage_degraded);
    }
}
