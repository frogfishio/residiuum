//! ATM-2.2: coordinator, placement, staged members invisible to ordinary get/scan.

use residiuum_atomics::{
    AtomicId, AtomicMember, CanonicalKey, CollectionId, ContentRoot, HeapId, MemberPhase,
    MutationKind, ObjectIdentity, OrdinaryCell, PreparePhase, PublicationPhase, StagingHeap,
    VersionId,
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

fn create_member(id: AtomicId, ordinal: u32, k: &str, hash_n: u8) -> AtomicMember {
    let mut hash = [0u8; 32];
    hash[0] = hash_n;
    AtomicMember {
        atomic_id: id,
        ordinal,
        object_identity: ObjectIdentity::new(cid(1), key(k)),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(hash),
        event_id: vid(10 + ordinal as u8),
    }
}

#[test]
fn staged_create_is_invisible_to_ordinary_get_and_scan() {
    let mut heap = StagingHeap::new(hid(1), 2).unwrap();
    let id = aid(1);
    let m = create_member(id, 0, "k", 9);
    heap.begin_prepare(id, root(1), std::slice::from_ref(&m))
        .unwrap();
    heap.append_staged(m, b"secret".to_vec()).unwrap();

    assert!(heap.get(cid(1), &key("k")).is_none());
    assert_eq!(heap.scan().count(), 0);
    let staged = heap.inspect_staged(id).unwrap();
    assert_eq!(staged.len(), 1);
    assert_eq!(staged[0].payload, b"secret");
    let life = heap.lifecycle(id).unwrap();
    assert_eq!(life.prepare, PreparePhase::Prepared);
    assert_eq!(life.members, MemberPhase::Staged);
    assert_eq!(life.publication, PublicationPhase::Unpublished);
    assert!(!life.ordinary_visible());
}

#[test]
fn staged_overwrite_does_not_replace_ordinary_cell() {
    let mut heap = StagingHeap::new(hid(1), 1).unwrap();
    heap.publish_ordinary(
        cid(1),
        key("k"),
        OrdinaryCell {
            version: vid(1),
            value: b"live".to_vec(),
        },
    );
    let id = aid(2);
    let m = create_member(id, 0, "k", 3);
    heap.begin_prepare(id, root(2), std::slice::from_ref(&m))
        .unwrap();
    heap.append_staged(m, b"staged".to_vec()).unwrap();
    assert_eq!(heap.get(cid(1), &key("k")).unwrap().value, b"live");
    assert_eq!(heap.scan().count(), 1);
}

#[test]
fn coordinator_allocates_monotonic_nonzero_sequences() {
    let mut heap = StagingHeap::new(hid(1), 2).unwrap();
    let a = create_member(aid(3), 0, "a", 1);
    let b = create_member(aid(4), 0, "b", 2);
    let (s1, _) = heap
        .begin_prepare(aid(3), root(3), std::slice::from_ref(&a))
        .unwrap();
    let (s2, _) = heap
        .begin_prepare(aid(4), root(4), std::slice::from_ref(&b))
        .unwrap();
    assert_eq!(s1.as_u64(), 1);
    assert_eq!(s2.as_u64(), 2);
    assert!(s2 > s1);
    assert_eq!(heap.coordinator().records().len(), 2);
    assert_eq!(heap.prepare_seq(aid(3)).unwrap(), s1);
}

#[test]
fn placement_spreads_members_across_shards() {
    let mut heap = StagingHeap::new(hid(1), 2).unwrap();
    let id = aid(5);
    let members = vec![
        create_member(id, 0, "a", 1),
        create_member(id, 1, "b", 2),
        create_member(id, 2, "c", 3),
    ];
    let (_, manifest) = heap.begin_prepare(id, root(5), &members).unwrap();
    let shards: Vec<u32> = manifest
        .entries()
        .iter()
        .map(|e| e.shard.as_u32())
        .collect();
    assert_eq!(shards, vec![0, 1, 0]);
    heap.append_staged(members[1].clone(), b"b".to_vec())
        .unwrap();
    assert_eq!(heap.inspect_staged(id).unwrap()[0].shard.as_u32(), 1);
}

#[test]
fn two_heaps_cannot_see_each_others_staged_members() {
    let mut a = StagingHeap::new(hid(1), 1).unwrap();
    let b = StagingHeap::new(hid(2), 1).unwrap();
    let id = aid(6);
    let m = create_member(id, 0, "same", 1);
    a.begin_prepare(id, root(6), std::slice::from_ref(&m))
        .unwrap();
    a.append_staged(m, b"A".to_vec()).unwrap();
    assert!(b.inspect_staged(id).is_none());
    assert!(b.get(cid(1), &key("same")).is_none());
    assert_eq!(a.inspect_staged(id).unwrap()[0].payload, b"A");
}

#[test]
fn duplicate_atomic_id_on_coordinator_is_refused() {
    let mut heap = StagingHeap::new(hid(1), 1).unwrap();
    let m = create_member(aid(7), 0, "k", 1);
    heap.begin_prepare(aid(7), root(7), std::slice::from_ref(&m))
        .unwrap();
    assert!(heap
        .begin_prepare(aid(7), root(8), std::slice::from_ref(&m))
        .is_err());
}

#[test]
fn staged_ordinal_must_match_manifest() {
    let mut heap = StagingHeap::new(hid(1), 1).unwrap();
    let planned = create_member(aid(8), 0, "k", 1);
    heap.begin_prepare(aid(8), root(8), std::slice::from_ref(&planned))
        .unwrap();
    let other = create_member(aid(8), 3, "k", 1);
    assert!(heap.append_staged(other, b"x".to_vec()).is_err());
}
