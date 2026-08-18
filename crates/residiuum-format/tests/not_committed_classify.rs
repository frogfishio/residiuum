//! CR-ATMR3-002: valid not-committed evidence is a terminal outcome, not corrupt.

use residiuum_atomics::{
    encode_decision, encode_member, encode_prepare, ordered_member_manifest_root, prepare_hash,
    AtomicAbortReason, AtomicDecision, AtomicId, AtomicMember, AtomicPrepare, CanonicalKey,
    CollectionId, ContentRoot, CoordinationScope, DecisionCode, HeapId, MutationKind,
    ObjectIdentity, ResourceLimits, VersionId,
};
use residiuum_format::{
    decode_deterministic_uint_map, encode_atomic_commit_envelope, encode_atomic_frame,
    encode_atomic_member_envelope, encode_atomic_prepare_envelope, encode_deterministic_uint_map,
    read_atomic_evidence, AtomicExamReason, AtomicGroupClass, CborValue, FrameKind, SafetyLimits,
};

fn hid() -> HeapId {
    let mut b = [0u8; 16];
    b[0] = 1;
    HeapId::from_bytes(b).unwrap()
}

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

fn root() -> ContentRoot {
    ContentRoot::from_bytes([7u8; 32]).unwrap()
}

fn member() -> AtomicMember {
    AtomicMember {
        atomic_id: aid(),
        ordinal: 0,
        object_identity: ObjectIdentity::new(cid(), CanonicalKey::String("k".into())),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(*blake3::hash(b"v").as_bytes()),
        event_id: vid(),
    }
}

fn prepare() -> AtomicPrepare {
    let m = member();
    let manifest = ordered_member_manifest_root(hid(), std::slice::from_ref(&m)).unwrap();
    AtomicPrepare {
        atomic_id: aid(),
        heap_id: hid(),
        scope: CoordinationScope::LocalHeap,
        content_root: root(),
        frontier: [1u8; 32],
        ordered_member_manifest_root: manifest,
        read_set_root: [3u8; 32],
        predicate_set_root: [4u8; 32],
        active_rule_revision_root: [5u8; 32],
        limits: ResourceLimits::hard_local_heap(),
    }
}

fn eid(n: u8) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = n;
    id
}

fn prepare_frame(prep: &AtomicPrepare) -> Vec<u8> {
    encode_atomic_frame(
        FrameKind::BatchPrepare,
        &encode_atomic_prepare_envelope(hid().as_bytes(), aid().as_bytes(), root().as_bytes())
            .unwrap(),
        &encode_prepare(prep).unwrap(),
        eid(1),
    )
    .unwrap()
}

fn decision_frame(decision: &AtomicDecision) -> Vec<u8> {
    encode_atomic_frame(
        FrameKind::BatchCommit,
        &encode_atomic_commit_envelope(hid().as_bytes(), aid().as_bytes(), root().as_bytes(), None)
            .unwrap(),
        &encode_decision(decision).unwrap(),
        eid(2),
    )
    .unwrap()
}

fn member_frame() -> Vec<u8> {
    let m = member();
    encode_atomic_frame(
        FrameKind::ItemEvent,
        &encode_atomic_member_envelope(
            hid().as_bytes(),
            aid().as_bytes(),
            0,
            root().as_bytes(),
            None,
        )
        .unwrap(),
        &encode_member(&m).unwrap(),
        eid(3),
    )
    .unwrap()
}

fn aborted(reason: AtomicAbortReason) -> AtomicDecision {
    let prep = prepare();
    AtomicDecision::not_committed(
        aid(),
        prepare_hash(&prep).unwrap(),
        prep.ordered_member_manifest_root,
        0,
        reason,
    )
}

fn report(parts: &[&[u8]]) -> residiuum_format::AtomicRecoveryReport {
    let mut bytes = Vec::new();
    for p in parts {
        bytes.extend_from_slice(p);
    }
    read_atomic_evidence(&bytes, SafetyLimits::default())
}

const ALL_ABORTS: [AtomicAbortReason; 4] = [
    AtomicAbortReason::PreconditionConflict,
    AtomicAbortReason::RuleRejected,
    AtomicAbortReason::RecoveryAbort,
    AtomicAbortReason::CoverageIncomplete,
];

#[test]
fn every_abort_reason_with_prepare_and_no_members_is_not_committed() {
    let prep = prepare();
    let pf = prepare_frame(&prep);
    for reason in ALL_ABORTS {
        let rec = report(&[&pf, &decision_frame(&aborted(reason))]);
        assert_eq!(rec.valid().count(), 2, "{reason:?}");
        assert_eq!(rec.groups.len(), 1, "{reason:?}");
        assert_eq!(
            rec.groups[0].class,
            AtomicGroupClass::NotCommitted {
                reclaimable_members: false
            },
            "{reason:?}"
        );
    }
}

#[test]
fn leftover_staged_members_are_reclaimable_not_committed() {
    let prep = prepare();
    let rec = report(&[
        &prepare_frame(&prep),
        &member_frame(),
        &decision_frame(&aborted(AtomicAbortReason::RuleRejected)),
    ]);
    assert_eq!(rec.valid().count(), 3);
    assert_eq!(
        rec.groups[0].class,
        AtomicGroupClass::NotCommitted {
            reclaimable_members: true
        }
    );
}

#[test]
fn wrong_prepare_hash_is_body_mismatch() {
    let prep = prepare();
    let mut decision = aborted(AtomicAbortReason::PreconditionConflict);
    decision.prepare_hash = [0x11; 32];
    let rec = report(&[&prepare_frame(&prep), &decision_frame(&decision)]);
    assert_eq!(
        rec.groups[0].class,
        AtomicGroupClass::Corrupt {
            reason: AtomicExamReason::BodyMismatch
        }
    );
}

#[test]
fn wrong_intended_root_is_body_mismatch() {
    let prep = prepare();
    let mut decision = aborted(AtomicAbortReason::PreconditionConflict);
    decision.member_root = [0x22; 32];
    let rec = report(&[&prepare_frame(&prep), &decision_frame(&decision)]);
    assert_eq!(
        rec.groups[0].class,
        AtomicGroupClass::Corrupt {
            reason: AtomicExamReason::BodyMismatch
        }
    );
}

#[test]
fn not_committed_member_count_nonzero_is_illegally_published() {
    let prep = prepare();
    let mut decision = aborted(AtomicAbortReason::PreconditionConflict);
    decision.member_count = 1;
    let rec = report(&[&prepare_frame(&prep), &decision_frame(&decision)]);
    assert_eq!(
        rec.groups[0].class,
        AtomicGroupClass::Corrupt {
            reason: AtomicExamReason::BodyMismatch
        }
    );
}

fn mutate_decision(
    decision: &AtomicDecision,
    edit: impl FnOnce(&mut Vec<(u64, CborValue)>),
) -> Vec<u8> {
    let mut map = decode_deterministic_uint_map(&encode_decision(decision).unwrap()).unwrap();
    edit(&mut map);
    let body = encode_deterministic_uint_map(&map).unwrap();
    encode_atomic_frame(
        FrameKind::BatchCommit,
        &encode_atomic_commit_envelope(hid().as_bytes(), aid().as_bytes(), root().as_bytes(), None)
            .unwrap(),
        &body,
        eid(9),
    )
    .unwrap()
}

#[test]
fn missing_abort_reason_is_refused() {
    let prep = prepare();
    let frame = mutate_decision(&aborted(AtomicAbortReason::PreconditionConflict), |map| {
        map.retain(|(k, _)| *k != 8);
    });
    let rec = report(&[&prepare_frame(&prep), &frame]);
    assert!(
        rec.examined.iter().any(|e| matches!(
            e.class,
            residiuum_format::AtomicEvidenceClass::Corrupt {
                reason: AtomicExamReason::BodyCorrupt
            }
        )),
        "{:?}",
        rec.examined
    );
}

#[test]
fn commit_position_on_not_committed_is_refused() {
    let prep = prepare();
    let frame = mutate_decision(&aborted(AtomicAbortReason::PreconditionConflict), |map| {
        map.push((6, CborValue::Uint(1)));
    });
    let rec = report(&[&prepare_frame(&prep), &frame]);
    assert!(
        rec.examined.iter().any(|e| matches!(
            e.class,
            residiuum_format::AtomicEvidenceClass::Corrupt {
                reason: AtomicExamReason::BodyCorrupt
            }
        )),
        "{:?}",
        rec.examined
    );
}

#[test]
fn committed_path_still_requires_complete_members() {
    let prep = prepare();
    let committed = AtomicDecision::committed(
        aid(),
        prepare_hash(&prep).unwrap(),
        prep.ordered_member_manifest_root,
        1,
        1,
    )
    .unwrap();
    assert_eq!(committed.decision, DecisionCode::Committed);
    let commit = encode_atomic_frame(
        FrameKind::BatchCommit,
        &encode_atomic_commit_envelope(
            hid().as_bytes(),
            aid().as_bytes(),
            root().as_bytes(),
            Some(1),
        )
        .unwrap(),
        &encode_decision(&committed).unwrap(),
        eid(2),
    )
    .unwrap();
    let rec = report(&[&prepare_frame(&prep), &commit]);
    assert_eq!(
        rec.groups[0].class,
        AtomicGroupClass::Corrupt {
            reason: AtomicExamReason::BodyMismatch
        }
    );
}
