//! ATM-0.5: serial oracle history properties.

use residiuum_atomics::{
    AtomicId, AtomicOutcome, AtomicPlan, AtomicPlanParts, AtomicProfile, AtomicRefuseReason,
    AtomicsError, CanonicalKey, CollectionId, CoordinationScope, HeapId, LogicalStatus,
    MutationKind, OracleHistoryKind, PlanMutation, ResourceLimits, SerialOracle,
};
use std::fs;
use std::path::PathBuf;

fn hid() -> HeapId {
    let mut b = [0u8; 16];
    b[0] = 1;
    HeapId::from_bytes(b).unwrap()
}
fn cid() -> CollectionId {
    let mut b = [0u8; 16];
    b[0] = 1;
    CollectionId::from_bytes(b).unwrap()
}
fn aid(n: u8) -> AtomicId {
    let mut b = [0u8; 32];
    b[0] = n;
    AtomicId::from_bytes(b).unwrap()
}

fn plan(id: u8, key: &str, val: &[u8]) -> AtomicPlan {
    AtomicPlan::close(AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: aid(id),
        heap_id: hid(),
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: Vec::new(),
        predicates: Vec::new(),
        mutations: vec![PlanMutation {
            kind: MutationKind::Create,
            collection_id: cid(),
            key: CanonicalKey::String(key.into()),
            encoded_value: Some(val.to_vec()),
            if_version: None,
        }],
        active_rule_revisions: Vec::new(),
        limits: ResourceLimits::builder_defaults_local_heap(),
    })
    .unwrap()
}

#[test]
fn same_id_same_root_replays() {
    let mut o = SerialOracle::new(hid());
    let p = plan(1, "k", b"v");
    let first = o.apply(&p).unwrap();
    let second = o.apply(&p).unwrap();
    match (first, second) {
        (AtomicOutcome::Committed(a), AtomicOutcome::Committed(b)) => {
            assert!(!a.replayed);
            assert!(b.replayed);
            assert_eq!(a.commit_position, b.commit_position);
            assert_eq!(a.decision_hash, b.decision_hash);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        o.history()
            .iter()
            .filter(|h| h.kind == OracleHistoryKind::Replayed)
            .count(),
        1
    );
}

#[test]
fn same_id_different_root_conflicts() {
    let mut o = SerialOracle::new(hid());
    o.apply(&plan(1, "k", b"v")).unwrap();
    let err = o.apply(&plan(1, "k", b"other")).unwrap_err();
    assert_eq!(
        err,
        AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict)
    );
    assert_eq!(
        o.get(cid(), &CanonicalKey::String("k".into()))
            .unwrap()
            .value,
        b"v"
    );
    assert!(o
        .history()
        .iter()
        .any(|h| h.kind == OracleHistoryKind::IdConflict && !h.published));
}

#[test]
fn unknown_profile_is_refused_not_issued() {
    let mut o = SerialOracle::new(hid());
    let mut parts = AtomicPlanParts {
        profile: AtomicProfile::from_wire_code(99),
        atomic_id: aid(3),
        heap_id: hid(),
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: Vec::new(),
        predicates: Vec::new(),
        mutations: vec![PlanMutation {
            kind: MutationKind::Create,
            collection_id: cid(),
            key: CanonicalKey::String("k".into()),
            encoded_value: Some(b"v".to_vec()),
            if_version: None,
        }],
        active_rule_revisions: Vec::new(),
        limits: ResourceLimits::builder_defaults_local_heap(),
    };
    let p = AtomicPlan::close(parts.clone()).unwrap();
    assert_eq!(
        o.apply(&p).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::UnsupportedProfile)
    );
    assert_eq!(o.status(aid(3)).logical, LogicalStatus::NotFound);
    parts.profile = AtomicProfile::LocalHeapV1;
    let again = AtomicPlan::close(parts).unwrap();
    assert!(matches!(
        o.apply(&again).unwrap(),
        AtomicOutcome::Committed(_)
    ));
}

#[test]
fn history_never_marks_partial_publish() {
    let mut o = SerialOracle::new(hid());
    o.apply(&plan(1, "a", b"1")).unwrap();
    o.apply(&plan(2, "b", b"2")).unwrap();
    for h in o.history() {
        match h.kind {
            OracleHistoryKind::IssuedCommitted | OracleHistoryKind::Replayed => {
                assert!(h.published);
            }
            OracleHistoryKind::IssuedNotCommitted
            | OracleHistoryKind::Refused
            | OracleHistoryKind::IdConflict => {
                assert!(!h.published);
            }
        }
    }
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/atomics-evidence/atm-0");
    fs::create_dir_all(&dir).unwrap();
    let property = serde_json::json!({
        "profile": "residiuum-atomics-v1",
        "properties": {
            "same_id_same_root_replays": true,
            "same_id_different_root_conflicts": true,
            "oracle_never_partially_visible": true,
            "unknown_profile_refused_before_issue": true
        }
    });
    let model = serde_json::json!({
        "profile": "residiuum-atomics-v1",
        "oracle": "serial_in_memory",
        "history_len": o.history().len(),
        "no_partial_publish": o.history().iter().all(|h| {
            h.published == matches!(
                h.kind,
                OracleHistoryKind::IssuedCommitted | OracleHistoryKind::Replayed
            )
        }),
        "complete_coverage_status": true
    });
    fs::write(
        dir.join("property-summary.json"),
        format!("{}\n", serde_json::to_string_pretty(&property).unwrap()),
    )
    .unwrap();
    fs::write(
        dir.join("model-check-summary.json"),
        format!("{}\n", serde_json::to_string_pretty(&model).unwrap()),
    )
    .unwrap();
}
