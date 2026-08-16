//! ATM-2.4: failpoints and prepared-visibility negatives.

use residiuum_atomics::{
    AtomicId, AtomicMember, CanonicalKey, CollectionId, ContentRoot, FaultError, FaultSession,
    HeapId, MutationKind, ObjectIdentity, OrdinaryCell, StagingFailpoint, StagingHeap, VersionId,
};

fn hid(n: u8) -> HeapId {
    let mut b = [0u8; 16];
    b[0] = n;
    HeapId::from_bytes(b).unwrap()
}

fn cid(n: u8) -> CollectionId {
    let mut b = [0u8; 16];
    b[0] = n;
    CollectionId::from_bytes(b).unwrap()
}

fn aid(n: u8) -> AtomicId {
    let mut b = [0u8; 32];
    b[0] = n;
    AtomicId::from_bytes(b).unwrap()
}

fn root(n: u8) -> ContentRoot {
    let mut b = [0u8; 32];
    b[0] = n;
    ContentRoot::from_bytes(b).unwrap()
}

fn vid(n: u8) -> VersionId {
    let mut b = [0u8; 16];
    b[0] = n;
    VersionId::from_bytes(b).unwrap()
}

fn key(s: &str) -> CanonicalKey {
    CanonicalKey::String(s.to_owned())
}

fn create_member(id: AtomicId, ordinal: u32, k: &str, payload: &[u8]) -> AtomicMember {
    AtomicMember {
        atomic_id: id,
        ordinal,
        object_identity: ObjectIdentity::new(cid(1), key(k)),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(*blake3::hash(payload).as_bytes()),
        event_id: vid(1),
    }
}

fn assert_no_ordinary_leak(heap: &StagingHeap, k: &str) {
    assert!(heap.get(cid(1), &key(k)).is_none());
    assert!(!heap.scan().any(|(_, kk, _)| kk == &key(k)));
}

#[test]
fn before_prepare_reopen_has_no_prepare_and_no_ordinary_mutation() {
    let mut session = FaultSession::new(StagingHeap::new(hid(1), 1).unwrap());
    session.arm(StagingFailpoint::BeforePrepare);
    let m = create_member(aid(1), 0, "k", b"x");
    assert_eq!(
        session.begin_prepare(aid(1), root(1), std::slice::from_ref(&m)),
        Err(FaultError::Injected(StagingFailpoint::BeforePrepare))
    );
    let heap = session.reopen();
    assert!(!heap.can_resolve(aid(1)));
    assert!(heap.inspect_staged(aid(1)).is_none());
    assert_no_ordinary_leak(&heap, "k");
}

#[test]
fn after_prepare_reopen_keeps_prepare_and_does_not_publish() {
    let mut session = FaultSession::new(StagingHeap::new(hid(1), 1).unwrap());
    session.arm(StagingFailpoint::AfterPrepare);
    let m = create_member(aid(2), 0, "k", b"x");
    assert_eq!(
        session.begin_prepare(aid(2), root(2), std::slice::from_ref(&m)),
        Err(FaultError::Injected(StagingFailpoint::AfterPrepare))
    );
    let heap = session.reopen();
    assert!(heap.can_resolve(aid(2)));
    assert_eq!(heap.inspect_staged(aid(2)).unwrap().len(), 0);
    assert_eq!(
        heap.lifecycle(aid(2)).unwrap().prepare,
        residiuum_atomics::PreparePhase::Prepared
    );
    assert_no_ordinary_leak(&heap, "k");
}

#[test]
fn after_member_n_reopen_examines_surviving_member_only() {
    let mut session = FaultSession::new(StagingHeap::new(hid(1), 2).unwrap());
    let a = create_member(aid(3), 0, "a", b"A");
    let b = create_member(aid(3), 1, "b", b"B");
    session
        .begin_prepare(aid(3), root(3), &[a.clone(), b.clone()])
        .unwrap();
    session.arm(StagingFailpoint::AfterMember(0));
    assert_eq!(
        session.append_staged(a, b"A".to_vec()),
        Err(FaultError::Injected(StagingFailpoint::AfterMember(0)))
    );
    let heap = session.reopen();
    let staged = heap.inspect_staged(aid(3)).unwrap();
    assert_eq!(staged.len(), 1);
    assert_eq!(staged[0].payload, b"A");
    assert_no_ordinary_leak(&heap, "a");
    assert_no_ordinary_leak(&heap, "b");
}

#[test]
fn second_heap_cannot_resolve_first_atomic() {
    let mut a = StagingHeap::new(hid(1), 1).unwrap();
    let b = StagingHeap::new(hid(2), 1).unwrap();
    let m = create_member(aid(4), 0, "k", b"v");
    a.begin_prepare(aid(4), root(4), std::slice::from_ref(&m))
        .unwrap();
    a.append_staged(m, b"v".to_vec()).unwrap();
    assert!(a.can_resolve(aid(4)));
    assert!(!b.can_resolve(aid(4)));
    assert!(b.inspect_staged(aid(4)).is_none());
    assert_no_ordinary_leak(&b, "k");
}

#[test]
fn negative_control_detects_a_leaked_staged_member() {
    let mut heap = StagingHeap::new(hid(1), 1).unwrap();
    let m = create_member(aid(5), 0, "leak", b"secret");
    heap.begin_prepare(aid(5), root(5), std::slice::from_ref(&m))
        .unwrap();
    heap.append_staged(m, b"secret".to_vec()).unwrap();
    assert_no_ordinary_leak(&heap, "leak");

    // Intentional leak: if this assertion is inverted, the negative control is dead.
    heap.publish_ordinary(
        cid(1),
        key("leak"),
        OrdinaryCell {
            version: vid(9),
            value: b"secret".to_vec(),
        },
    );
    assert!(
        heap.get(cid(1), &key("leak")).is_some(),
        "negative control must observe a published staged member"
    );
}
