use residiuum_store::{
    BoundaryKind, DurabilityMode, OperationMutation, OperationMutationKind, OperationPut, Store,
    StoreError, WriteCondition,
};

#[test]
fn operation_cohort_shares_segment_sync_and_replays_individual_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    let mut store = Store::create(&root).unwrap();
    store.enable_boundary_probe();

    let large = vec![0x5au8; 80 * 1024];
    let operations = [
        OperationPut {
            subject: b"cohort/a",
            body: b"alpha",
            condition: WriteCondition::Absent,
            operation_id: [1u8; 16],
            content_hash: [11u8; 32],
        },
        OperationPut {
            subject: b"cohort/b",
            body: large.as_slice(),
            condition: WriteCondition::Unconditional,
            operation_id: [2u8; 16],
            content_hash: [22u8; 32],
        },
        OperationPut {
            subject: b"cohort/a",
            body: b"must-not-win",
            condition: WriteCondition::Absent,
            operation_id: [3u8; 16],
            content_hash: [33u8; 32],
        },
    ];

    let outcomes = store.put_operation_cohort_awo_owned(&operations).unwrap();
    assert_eq!(outcomes.len(), 3);
    let first = outcomes[0].as_ref().unwrap();
    let second = outcomes[1].as_ref().unwrap();
    assert_eq!(first.receipt.durability, DurabilityMode::Durable);
    assert_eq!(second.receipt.durability, DurabilityMode::Durable);
    assert!(!first.deduplicated);
    assert!(!second.deduplicated);
    assert!(matches!(outcomes[2], Err(StoreError::KeyExists)));

    let probe = store.take_boundary_snapshot();
    assert_eq!(
        probe.counters.count(BoundaryKind::FileSync),
        1,
        "two successful logical mutations, including a chunked value, must share one segment sync"
    );
    assert_eq!(
        probe.counters.count(BoundaryKind::FileWrite),
        1,
        "cohort frames must cross the active tail in one gathered write"
    );
    assert_eq!(
        store.get("cohort/a").unwrap().as_deref(),
        Some(b"alpha".as_slice())
    );
    assert_eq!(store.get("cohort/b").unwrap().unwrap(), large);

    let first_event = first.receipt.event_id;
    drop(store);

    let mut reopened = Store::open(&root).unwrap();
    let (replayed, deduplicated) = reopened
        .put_subject_bytes_with_operation(
            b"cohort/a",
            b"alpha",
            DurabilityMode::Durable,
            WriteCondition::Absent,
            [1u8; 16],
            [11u8; 32],
        )
        .unwrap();
    assert!(deduplicated);
    assert_eq!(replayed.event_id, first_event);
}

#[test]
fn mixed_put_delete_cohort_keeps_individual_receipts_and_one_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    let mut store = Store::create(&root).unwrap();
    store
        .put("mixed/delete", b"old", DurabilityMode::Durable)
        .unwrap();
    store.enable_boundary_probe();

    let operations = [
        OperationMutation {
            subject: b"mixed/delete",
            kind: OperationMutationKind::Delete,
            condition: WriteCondition::Present,
            operation_id: [41; 16],
            content_hash: [42; 32],
        },
        OperationMutation {
            subject: b"mixed/put",
            kind: OperationMutationKind::Put(b"new"),
            condition: WriteCondition::Absent,
            operation_id: [43; 16],
            content_hash: [44; 32],
        },
    ];
    let outcomes = store.operation_cohort_awo_owned(&operations).unwrap();
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(|outcome| outcome.is_ok()));
    assert_eq!(outcomes[0].as_ref().unwrap().receipt.logical_len, 0);
    assert_eq!(
        outcomes[1].as_ref().unwrap().receipt.durability,
        DurabilityMode::Durable
    );
    assert_eq!(store.get("mixed/delete").unwrap(), None);
    assert_eq!(
        store.get("mixed/put").unwrap().as_deref(),
        Some(b"new".as_slice())
    );
    let probe = store.take_boundary_snapshot();
    assert_eq!(probe.counters.count(BoundaryKind::FileWrite), 1);
    assert_eq!(probe.counters.count(BoundaryKind::FileSync), 1);

    drop(store);
    let mut reopened = Store::open(&root).unwrap();
    assert_eq!(reopened.get("mixed/delete").unwrap(), None);
    assert_eq!(
        reopened.get("mixed/put").unwrap().as_deref(),
        Some(b"new".as_slice())
    );
    let (receipt, deduplicated) = reopened
        .delete_subject_bytes_with_operation(
            b"mixed/delete",
            DurabilityMode::Durable,
            WriteCondition::Present,
            [41; 16],
            [42; 32],
        )
        .unwrap();
    assert!(deduplicated);
    assert_eq!(
        receipt.event_id,
        outcomes[0].as_ref().unwrap().receipt.event_id
    );
}

#[test]
fn duplicate_operation_inside_cohort_gets_the_owner_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create(dir.path()).unwrap();
    let operations = [
        OperationPut {
            subject: b"same/a",
            body: b"value",
            condition: WriteCondition::Unconditional,
            operation_id: [9u8; 16],
            content_hash: [7u8; 32],
        },
        OperationPut {
            subject: b"same/a",
            body: b"value",
            condition: WriteCondition::Unconditional,
            operation_id: [9u8; 16],
            content_hash: [7u8; 32],
        },
    ];
    let outcomes = store.put_operation_cohort_awo_owned(&operations).unwrap();
    let owner = outcomes[0].as_ref().unwrap();
    let duplicate = outcomes[1].as_ref().unwrap();
    assert!(!owner.deduplicated);
    assert!(duplicate.deduplicated);
    assert_eq!(owner.receipt.event_id, duplicate.receipt.event_id);
}
