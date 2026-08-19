//! CR-ATMR5-001: store Atomic recovery is bounded and not a per-op full scan.

use residiuum_atomics::{
    AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, CollectionId,
    CoordinationScope, HeapId, MutationKind, ObjectIdentity, PlanMutation, ResourceLimits,
    VersionId,
};
use residiuum_store::{
    atomic_stage_checkpoint_path, AtomicStageDisposition, DurabilityMode, Store, StoreError,
};

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

fn stage_one(store: &mut Store) {
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap_id, std::slice::from_ref(&m), b"secret");
    let mut stage = store.atomic_stage().unwrap();
    stage
        .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    stage.append_staged(m, b"secret".to_vec()).unwrap();
    stage.seal_member_boundary(aid()).unwrap();
}

#[test]
fn checkpoint_reopen_skips_covered_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    store
        .put("hist", &vec![0xAB; 64 * 1024], DurabilityMode::Durable)
        .unwrap();
    stage_one(&mut store);
    assert!(atomic_stage_checkpoint_path(store.paths()).is_file());

    let first = store.atomic_stage().unwrap();
    let report = first.open_report();
    assert_eq!(report.disposition, AtomicStageDisposition::Checkpoint);
    assert_eq!(report.catalog_loads, 1);
    assert!(report.files_skipped >= 1, "settled media must be skipped");
    assert!(
        report.bytes_verified >= 64 * 1024,
        "covered prefix verification must charge actual bytes, verified {}",
        report.bytes_verified
    );
    assert!(first.kernel().placement(aid()).is_some());
    drop(first);
    let metrics = store.open_report();
    assert_eq!(
        metrics.atomic_stage_disposition,
        AtomicStageDisposition::Checkpoint
    );
    assert_eq!(metrics.atomic_stage_atomics, 1);
}

#[test]
fn ordinary_growth_is_tailed_not_fully_rescanned() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    store
        .put("hist", &vec![0xCD; 128 * 1024], DurabilityMode::Durable)
        .unwrap();
    stage_one(&mut store);
    store
        .put("more", &vec![0xEF; 48 * 1024], DurabilityMode::Durable)
        .unwrap();

    let stage = store.atomic_stage().unwrap();
    let report = stage.open_report();
    assert_eq!(report.disposition, AtomicStageDisposition::Checkpoint);
    assert!(
        report.files_tailed >= 1,
        "dirty active tail must be streamed"
    );
    assert!(
        report.bytes_verified >= 128 * 1024,
        "covered prefix verification must charge actual bytes, verified {}",
        report.bytes_verified
    );
    assert!(
        report.bytes_scanned > 16 * 1024,
        "the new tail must be charged, scanned {}",
        report.bytes_scanned
    );
    assert!(stage.kernel().placement(aid()).is_some());
}

#[test]
fn per_operation_does_not_reload_catalogue() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    let heap_id = HeapId::from_bytes(store.store_id()).unwrap();
    let m = member();
    let p = plan(heap_id, std::slice::from_ref(&m), b"secret");
    let mut stage = store.atomic_stage().unwrap();
    let scanned = stage.open_report().bytes_scanned;
    let loads = stage.open_report().catalog_loads;
    stage
        .begin_prepare(&p, FRONTIER, std::slice::from_ref(&m))
        .unwrap();
    stage.append_staged(m, b"secret".to_vec()).unwrap();
    stage.seal_member_boundary(aid()).unwrap();
    let after = stage.open_report();
    assert_eq!(after.catalog_loads, loads);
    assert_eq!(after.bytes_scanned, scanned);
}

#[test]
fn oversized_new_segment_is_refused_before_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    stage_one(&mut store);
    let bomb = store.paths().segments_dir().join("hostile.residiuum");
    std::fs::create_dir_all(store.paths().segments_dir()).unwrap();
    let f = std::fs::File::create(&bomb).unwrap();
    f.set_len(128 * 1024 * 1024).unwrap();
    drop(f);
    match store.atomic_stage() {
        Err(StoreError::AtomicStage(msg)) => {
            assert!(
                msg.contains("segment bytes"),
                "expected size refusal, got {msg}"
            );
        }
        Ok(_) => panic!("expected AtomicStage size refusal"),
        Err(other) => panic!("expected AtomicStage size refusal, got {other}"),
    }
}

#[test]
fn directory_depth_ceiling_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    stage_one(&mut store);
    let mut nested = store.paths().segments_dir();
    std::fs::create_dir_all(&nested).unwrap();
    for i in 0..10 {
        nested = nested.join(format!("d{i}"));
        std::fs::create_dir_all(&nested).unwrap();
    }
    std::fs::write(nested.join("leaf.residiuum"), b"x").unwrap();
    match store.atomic_stage() {
        Err(StoreError::AtomicStage(msg)) => {
            assert!(msg.contains("depth"), "expected depth refusal, got {msg}");
        }
        Ok(_) => panic!("expected AtomicStage depth refusal"),
        Err(other) => panic!("expected AtomicStage depth refusal, got {other}"),
    }
}

#[test]
fn interior_covered_prefix_flip_is_not_healthy_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s");
    let mut store = Store::create(&path).unwrap();
    store
        .put("hist", &vec![0xAB; 64 * 1024], DurabilityMode::Durable)
        .unwrap();
    stage_one(&mut store);
    let active = store.paths().active_segment();
    let mut bytes = std::fs::read(&active).unwrap();
    assert!(
        bytes.len() > 64,
        "need interior bytes that are neither head nor tail, got {}",
        bytes.len()
    );
    let flip_at = bytes.len() / 2;
    bytes[flip_at] ^= 0xff;
    std::fs::write(&active, &bytes).unwrap();

    match store.atomic_stage() {
        Err(StoreError::AtomicStage(msg)) => {
            assert!(
                msg.contains("covered prefix"),
                "expected covered-prefix refusal, got {msg}"
            );
        }
        Ok(stage) => panic!(
            "interior flip must not be a healthy checkpoint, disposition {:?}",
            stage.open_report().disposition
        ),
        Err(other) => panic!("expected AtomicStage covered-prefix refusal, got {other}"),
    }
}
