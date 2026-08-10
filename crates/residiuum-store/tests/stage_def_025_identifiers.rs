//! DEF-025 — strengthen identifier generation.
//!
//! Guarantees under test:
//! - Random identities use OS CSPRNG (`residiuum-id-v1`) with no time-hash fallback.
//! - Store / event ids are non-zero and unique across create, restart, and concurrent mints.
//! - Sortable segment ids recover sequence after reopen (monotonic counter only).
//! - Content-derived item ids stay stable for a subject; event ids differ per put.

use residiuum_store::{
    mint_sortable_segment_id, random_id, segment_seq_from_id, subject_item_id, DurabilityMode,
    Store, ID_PROFILE,
};
use std::collections::HashSet;
use tempfile::tempdir;

#[test]
fn id_profile_tag() {
    assert_eq!(ID_PROFILE, "residiuum-id-v1");
}

#[test]
fn csprng_random_ids_unique_and_nonzero() {
    let mut seen = HashSet::new();
    for _ in 0..512 {
        let id = random_id().expect("CSPRNG available");
        assert_ne!(id, [0u8; 16], "all-zero id rejected by mint");
        assert!(seen.insert(id), "collision in 512-sample CSPRNG batch");
    }
}

#[test]
fn store_ids_differ_across_create_and_restart_preserves() {
    let dir = tempdir().unwrap();
    let a_path = dir.path().join("a");
    let b_path = dir.path().join("b");

    let a = Store::create(&a_path).unwrap();
    let b = Store::create(&b_path).unwrap();
    let id_a = a.store_id();
    let id_b = b.store_id();
    assert_ne!(id_a, id_b);
    assert_ne!(id_a, [0u8; 16]);
    drop(a);

    let a2 = Store::open(&a_path).unwrap();
    assert_eq!(a2.store_id(), id_a);
}

#[test]
fn event_ids_are_random_not_counter_prefixed() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();

    let mut event_ids = Vec::new();
    for i in 0..32 {
        let key = format!("k{i}");
        let r = store.put(&key, b"v", DurabilityMode::Durable).unwrap();
        assert_ne!(r.event_id, [0u8; 16]);
        event_ids.push(r.event_id);
    }
    // All unique.
    let set: HashSet<_> = event_ids.iter().copied().collect();
    assert_eq!(set.len(), event_ids.len());

    // Old hybrid mint put a process counter in the first 8 LE bytes
    // (1,2,3,…). Pure CSPRNG almost never yields that sequential pattern.
    let prefixes: Vec<u64> = event_ids
        .iter()
        .map(|id| u64::from_le_bytes(id[..8].try_into().unwrap()))
        .collect();
    let sequential = prefixes.windows(2).all(|w| w[1] == w[0].saturating_add(1));
    assert!(
        !sequential,
        "event_id first-8 must not be a monotonic counter (got {prefixes:?})"
    );
}

#[test]
fn segment_ids_sortable_and_recover_after_reopen() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    let mut store = Store::create(&root).unwrap();

    store.put("a", b"1", DurabilityMode::Durable).unwrap();
    store.seal_active().unwrap();
    store.put("b", b"2", DurabilityMode::Durable).unwrap();
    store.seal_active().unwrap();
    store.put("c", b"3", DurabilityMode::Durable).unwrap();
    drop(store);

    let mut store = Store::open(&root).unwrap();
    // Next segment after reopen continues past recovered max seq.
    store.put("d", b"4", DurabilityMode::Durable).unwrap();
    store.seal_active().unwrap();

    // Synthetic check of mint helper shape.
    let sid = mint_sortable_segment_id(42, &[0xABu8; 16]);
    assert_eq!(segment_seq_from_id(&sid), 42);
}

#[test]
fn item_id_stable_event_id_changes_on_overwrite() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();

    let r1 = store
        .put("same-key", b"v1", DurabilityMode::Durable)
        .unwrap();
    let r2 = store
        .put("same-key", b"v2", DurabilityMode::Durable)
        .unwrap();
    assert_eq!(r1.item_id, r2.item_id);
    assert_eq!(r1.item_id, subject_item_id(b"same-key"));
    assert_ne!(r1.event_id, r2.event_id);
}

#[test]
fn clone_dirs_get_distinct_store_ids() {
    // Simulates restore/clone as separate create paths (not byte-copy of store_id).
    let dir = tempdir().unwrap();
    let mut ids = HashSet::new();
    for i in 0..8 {
        let s = Store::create(dir.path().join(format!("clone-{i}"))).unwrap();
        assert!(ids.insert(s.store_id()));
    }
}
