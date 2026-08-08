//! Smart-driver mutation identity remains recoverable when the authoritative
//! append succeeds but the derived dedup table update is interrupted.

use residiuum_store::{
    arm_failpoint_once, clear_failpoints, CompactOptions, DurabilityMode, FailpointAction, Store,
    StoreError, WriteCondition,
};
use std::sync::{Mutex, MutexGuard, OnceLock};

fn suite_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let guard = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    clear_failpoints();
    guard
}

#[test]
fn retry_recovers_original_receipt_after_append_to_dedup_crash_window() {
    let _guard = suite_lock();
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("store");
    let operation_id = [0x41; 16];
    let content_hash = [0x52; 32];

    {
        let mut store = Store::create(&root).unwrap();
        arm_failpoint_once("store.dedup.before_write", FailpointAction::Error);
        let error = store
            .put_subject_bytes_with_operation(
                b"conversation/7",
                b"document-v1",
                DurabilityMode::Durable,
                WriteCondition::Unconditional,
                operation_id,
                content_hash,
            )
            .expect_err("dedup persistence must be interrupted after append");
        assert!(matches!(error, StoreError::Failpoint(_)));
        assert_eq!(
            store.get("conversation/7").unwrap().as_deref(),
            Some(b"document-v1".as_slice())
        );
    }

    clear_failpoints();
    let mut store = Store::open(&root).unwrap();
    let original_event_id = store.history("conversation/7").unwrap().events[0].event_id;
    let (receipt, replayed) = store
        .put_subject_bytes_with_operation(
            b"conversation/7",
            b"document-v1",
            DurabilityMode::Durable,
            WriteCondition::Unconditional,
            operation_id,
            content_hash,
        )
        .unwrap();

    assert!(replayed);
    assert_eq!(receipt.event_id, original_event_id);
    assert_eq!(store.history("conversation/7").unwrap().events.len(), 1);
}

#[test]
fn recovered_operation_identity_still_rejects_different_content() {
    let _guard = suite_lock();
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("store");
    let operation_id = [0x61; 16];

    {
        let mut store = Store::create(&root).unwrap();
        arm_failpoint_once("store.dedup.before_write", FailpointAction::Error);
        let _ = store.put_subject_bytes_with_operation(
            b"conversation/9",
            b"document-v1",
            DurabilityMode::Durable,
            WriteCondition::Unconditional,
            operation_id,
            [0x71; 32],
        );
    }

    clear_failpoints();
    let mut store = Store::open(&root).unwrap();
    let error = store
        .put_subject_bytes_with_operation(
            b"conversation/9",
            b"document-v2",
            DurabilityMode::Durable,
            WriteCondition::Unconditional,
            operation_id,
            [0x72; 32],
        )
        .expect_err("same operation id with another request must conflict");
    assert!(matches!(error, StoreError::OperationIdentityConflict));
    assert_eq!(
        store.get("conversation/9").unwrap().as_deref(),
        Some(b"document-v1".as_slice())
    );
}

#[test]
fn delete_retry_recovers_tombstone_without_writing_another_delete() {
    let _guard = suite_lock();
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("store");
    let operation_id = [0x21; 16];
    let content_hash = [0x31; 32];

    {
        let mut store = Store::create(&root).unwrap();
        store
            .put("conversation/11", b"document-v1", DurabilityMode::Durable)
            .unwrap();
        arm_failpoint_once("store.dedup.before_write", FailpointAction::Error);
        let error = store
            .delete_subject_bytes_with_operation(
                b"conversation/11",
                DurabilityMode::Durable,
                WriteCondition::Present,
                operation_id,
                content_hash,
            )
            .expect_err("dedup persistence must be interrupted after tombstone append");
        assert!(matches!(error, StoreError::Failpoint(_)));
        assert!(store.get("conversation/11").unwrap().is_none());
    }

    clear_failpoints();
    let mut store = Store::open(&root).unwrap();
    let history_before = store.history("conversation/11").unwrap();
    let original_delete_id = history_before.events[1].event_id;
    let (receipt, replayed) = store
        .delete_subject_bytes_with_operation(
            b"conversation/11",
            DurabilityMode::Durable,
            WriteCondition::Present,
            operation_id,
            content_hash,
        )
        .unwrap();

    assert!(replayed);
    assert_eq!(receipt.event_id, original_delete_id);
    assert_eq!(store.history("conversation/11").unwrap().events.len(), 2);
}

#[test]
fn compaction_reconciles_operation_ledger_before_reclaiming_source_evidence() {
    let _guard = suite_lock();
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("store");
    let operation_id = [0x81; 16];
    let content_hash = [0x91; 32];
    let original_event_id;

    {
        let mut store = Store::create(&root).unwrap();
        arm_failpoint_once("store.dedup.before_write", FailpointAction::Error);
        let error = store
            .put_subject_bytes_with_operation(
                b"conversation/13",
                b"document-v1",
                DurabilityMode::Durable,
                WriteCondition::Unconditional,
                operation_id,
                content_hash,
            )
            .expect_err("ledger persistence must be interrupted after append");
        assert!(matches!(error, StoreError::Failpoint(_)));
        original_event_id = store.history("conversation/13").unwrap().events[0].event_id;

        clear_failpoints();
        store
            .compact_live_with(CompactOptions {
                reclaim_sources: true,
                allow_history_loss: true,
                ..CompactOptions::default()
            })
            .unwrap();
    }

    let mut store = Store::open(&root).unwrap();
    let history_len = store.history("conversation/13").unwrap().events.len();
    let (receipt, replayed) = store
        .put_subject_bytes_with_operation(
            b"conversation/13",
            b"document-v1",
            DurabilityMode::Durable,
            WriteCondition::Unconditional,
            operation_id,
            content_hash,
        )
        .unwrap();

    assert!(replayed);
    assert_eq!(receipt.event_id, original_event_id);
    assert_eq!(
        store.history("conversation/13").unwrap().events.len(),
        history_len
    );
}
