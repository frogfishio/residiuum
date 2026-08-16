//! ATM-2.3: chunked members, first stable boundary, shard rotation, cohort identity.

use residiuum_atomics::{
    AtomicId, AtomicMember, CanonicalKey, ChunkPlan, CollectionId, ContentRoot, HeapId,
    MemberPhase, MutationKind, ObjectIdentity, StagingHeap, VersionId,
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

fn create_member(id: AtomicId, ordinal: u32, k: &str, payload_hash: [u8; 32]) -> AtomicMember {
    AtomicMember {
        atomic_id: id,
        ordinal,
        object_identity: ObjectIdentity::new(cid(1), key(k)),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some(payload_hash),
        event_id: vid(20),
    }
}

fn h(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

#[test]
fn incomplete_chunks_cannot_seal_and_stay_invisible() {
    let mut heap = StagingHeap::new(hid(1), 2).unwrap();
    let id = aid(1);
    let p0 = b"aaaa";
    let p1 = b"bbbb";
    let mut whole = Vec::new();
    whole.extend_from_slice(p0);
    whole.extend_from_slice(p1);
    let member = create_member(id, 0, "k", h(&whole));
    heap.begin_prepare(id, root(1), std::slice::from_ref(&member))
        .unwrap();
    heap.commit_chunk_manifest(
        id,
        0,
        ChunkPlan {
            total: 2,
            chunk_hashes: vec![h(p0), h(p1)],
        },
    )
    .unwrap();
    heap.append_chunk(member.clone(), 0, p0.to_vec()).unwrap();
    assert!(heap.seal_member_boundary(id).is_err());
    assert!(heap.get(cid(1), &key("k")).is_none());
    assert_eq!(heap.lifecycle(id).unwrap().members, MemberPhase::Staged);

    heap.append_chunk(member, 1, p1.to_vec()).unwrap();
    heap.seal_member_boundary(id).unwrap();
    assert_eq!(
        heap.lifecycle(id).unwrap().members,
        MemberPhase::DurableInvisible
    );
    assert!(heap.get(cid(1), &key("k")).is_none());
    assert_eq!(heap.inspect_staged(id).unwrap()[0].payload, whole);
    assert!(!heap.lifecycle(id).unwrap().ordinary_visible());
}

#[test]
fn bad_chunk_hash_is_refused() {
    let mut heap = StagingHeap::new(hid(1), 1).unwrap();
    let id = aid(2);
    let member = create_member(id, 0, "k", h(b"abcd"));
    heap.begin_prepare(id, root(2), std::slice::from_ref(&member))
        .unwrap();
    heap.commit_chunk_manifest(
        id,
        0,
        ChunkPlan {
            total: 2,
            chunk_hashes: vec![h(b"ab"), h(b"cd")],
        },
    )
    .unwrap();
    assert!(heap.append_chunk(member, 0, b"xx".to_vec()).is_err());
}

#[test]
fn shard_rotation_does_not_publish_or_reassign_staged() {
    let mut heap = StagingHeap::new(hid(1), 2).unwrap();
    let id = aid(3);
    let member = create_member(id, 0, "k", h(b"v"));
    heap.begin_prepare(id, root(3), std::slice::from_ref(&member))
        .unwrap();
    heap.append_staged(member, b"v".to_vec()).unwrap();
    let shard_before = heap.inspect_staged(id).unwrap()[0].shard;
    heap.rotate_writer_shards(8).unwrap();
    heap.seal_member_boundary(id).unwrap();
    assert_eq!(heap.inspect_staged(id).unwrap()[0].shard, shard_before);
    assert!(heap.get(cid(1), &key("k")).is_none());
    assert_eq!(heap.placement(id).unwrap().entries()[0].shard, shard_before);
}

#[test]
fn cohort_neighbour_cannot_install_foreign_identity() {
    let mut heap = StagingHeap::new(hid(1), 1).unwrap();
    let a = create_member(aid(4), 0, "a", h(b"A"));
    let b = create_member(aid(5), 0, "b", h(b"B"));
    heap.begin_prepare(aid(4), root(4), std::slice::from_ref(&a))
        .unwrap();
    heap.begin_prepare(aid(5), root(5), std::slice::from_ref(&b))
        .unwrap();
    let mut stolen = a.clone();
    stolen.atomic_id = aid(5);
    assert!(heap.append_staged(stolen, b"A".to_vec()).is_err());
    heap.append_staged(a, b"A".to_vec()).unwrap();
    assert!(heap.inspect_staged(aid(5)).unwrap().is_empty());
    assert_eq!(heap.inspect_staged(aid(4)).unwrap()[0].payload, b"A");
}

#[test]
fn seal_refuses_further_staged_appends() {
    let mut heap = StagingHeap::new(hid(1), 1).unwrap();
    let first = create_member(aid(6), 0, "a", h(b"A"));
    heap.begin_prepare(aid(6), root(6), std::slice::from_ref(&first))
        .unwrap();
    heap.append_staged(first, b"A".to_vec()).unwrap();
    heap.seal_member_boundary(aid(6)).unwrap();
    let extra = create_member(aid(6), 0, "a", h(b"Z"));
    assert!(heap.append_staged(extra, b"Z".to_vec()).is_err());
}
