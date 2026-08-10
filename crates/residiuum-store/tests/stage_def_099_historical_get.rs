//! DEF-099 — exact historical value reads and last-complete recovery.

use residiuum_store::{
    BeforeEvent, DurabilityMode, PayloadResult, ReadBudget, RecoveryReadOptions, Store,
};
use tempfile::tempdir;

fn large(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i % 251) as u8))
        .collect()
}

#[test]
fn exact_inline_and_chunked_event_reads() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(20);
    store.set_chunk_size(8);

    let a = b"inline-a".to_vec();
    let b = large(1, 60);
    let c = large(2, 48);

    let r1 = store.put("k", &a, DurabilityMode::Durable).unwrap();
    let r2 = store.put("k", &b, DurabilityMode::Durable).unwrap();
    let r3 = store.put("k", &c, DurabilityMode::Durable).unwrap();

    assert_eq!(store.get("k").unwrap().as_deref(), Some(c.as_slice()));

    let v1 = store
        .get_payload_version("k", &r1.event_id, ReadBudget::default())
        .unwrap();
    assert!(!v1.is_tombstone);
    assert_eq!(
        v1.selected.as_ref().and_then(|p| p.complete_body()),
        Some(a.as_slice())
    );
    assert_eq!(v1.selected_event_id, r1.event_id);
    assert_eq!(v1.current_event_id, Some(r3.event_id));

    let v2 = store
        .get_payload_version("k", &r2.event_id, ReadBudget::default())
        .unwrap();
    assert_eq!(
        v2.selected.as_ref().and_then(|p| p.complete_body()),
        Some(b.as_slice())
    );

    let v3 = store
        .get_payload_version("k", &r3.event_id, ReadBudget::default())
        .unwrap();
    assert_eq!(
        v3.selected.as_ref().and_then(|p| p.complete_body()),
        Some(c.as_slice())
    );
}

#[test]
fn find_last_complete_after_replacements() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(16);
    store.set_chunk_size(8);

    let a = large(1, 40);
    let b = large(2, 40);
    let _r1 = store.put("k", &a, DurabilityMode::Durable).unwrap();
    let _r2 = store.put("k", &b, DurabilityMode::Durable).unwrap();

    // Current is complete B; last complete before current is A.
    let found = store
        .find_last_complete_version("k", BeforeEvent::Current, RecoveryReadOptions::default())
        .unwrap();
    assert!(found.found.is_some());
    let v = found.found.unwrap();
    assert_eq!(
        v.selected.as_ref().and_then(|p| p.complete_body()),
        Some(a.as_slice())
    );
    assert!(!v.tombstone_crossed);
}

#[test]
fn delete_boundary_stops_default_search() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();

    let old = b"before-delete".to_vec();
    store.put("k", &old, DurabilityMode::Durable).unwrap();
    store.delete("k", DurabilityMode::Durable).unwrap();
    let newer = b"after-delete".to_vec();
    store.put("k", &newer, DurabilityMode::Durable).unwrap();

    // Last complete before current should be after-delete's predecessor path:
    // newest before current is the delete → stop, no resurrection of old.
    let found = store
        .find_last_complete_version("k", BeforeEvent::Current, RecoveryReadOptions::default())
        .unwrap();
    assert!(found.found.is_none());
    assert!(found.tombstone_stopped);

    // Forensic: cross tombstone and recover old.
    let forensic = store
        .find_last_complete_version(
            "k",
            BeforeEvent::Current,
            RecoveryReadOptions {
                cross_tombstone: true,
                budget: ReadBudget::default(),
            },
        )
        .unwrap();
    let v = forensic.found.expect("forensic should find pre-delete put");
    assert_eq!(
        v.selected.as_ref().and_then(|p| p.complete_body()),
        Some(old.as_slice())
    );
    assert!(v.tombstone_crossed);
}

#[test]
fn ordinary_get_never_auto_falls_back() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.set_chunk_threshold(20);
    store.set_chunk_size(8);

    let a = large(3, 50);
    let b = large(4, 50);
    let r1 = store.put("k", &a, DurabilityMode::Durable).unwrap();
    store.put("k", &b, DurabilityMode::Durable).unwrap();

    // Current B is complete; get returns B.
    assert_eq!(store.get("k").unwrap().as_deref(), Some(b.as_slice()));

    // Historical A still available via version API.
    let v = store
        .get_payload_version("k", &r1.event_id, ReadBudget::default())
        .unwrap();
    assert_eq!(
        v.selected.as_ref().and_then(|p| p.complete_body()),
        Some(a.as_slice())
    );
}

#[test]
fn missing_event_id_errors() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.put("k", b"v", DurabilityMode::Durable).unwrap();
    let err = store
        .get_payload_version("k", &[9u8; 16], ReadBudget::default())
        .unwrap_err();
    assert!(matches!(
        err,
        residiuum_store::StoreError::HistoryEventNotFound
    ));
}

#[test]
fn recovery_is_read_only() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    let a = b"v1".to_vec();
    let b = b"v2".to_vec();
    let r1 = store.put("k", &a, DurabilityMode::Durable).unwrap();
    store.put("k", &b, DurabilityMode::Durable).unwrap();
    let live_before = store.live_count();
    let _ = store
        .get_payload_version("k", &r1.event_id, ReadBudget::default())
        .unwrap();
    let _ = store
        .find_last_complete_version("k", BeforeEvent::Current, RecoveryReadOptions::default())
        .unwrap();
    assert_eq!(store.live_count(), live_before);
    assert_eq!(store.get("k").unwrap().as_deref(), Some(b.as_slice()));
    // current completeness still complete
    assert!(matches!(
        store.get_payload("k").unwrap(),
        Some(PayloadResult::Complete { .. })
    ));
}

#[test]
fn delete_event_projects_as_tombstone() {
    let dir = tempdir().unwrap();
    let mut store = Store::create(dir.path().join("s")).unwrap();
    store.put("k", b"v", DurabilityMode::Durable).unwrap();
    let del = store.delete("k", DurabilityMode::Durable).unwrap();
    let v = store
        .get_payload_version("k", &del.event_id, ReadBudget::default())
        .unwrap();
    assert!(v.is_tombstone);
    assert!(v.selected.is_none());
}
