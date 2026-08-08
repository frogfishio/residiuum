//! DRV-1/DRV-5 embedded smart-client contract and concurrency evidence.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapCap, HeapId, HeapSecuritySnapshot, HeapSlot, Rights,
    SecurityRevision, TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::driver::{
    Client, Collection, CreateCollectionOptions, DeleteOptions, EmbeddedOptions, ErrorCode,
    OperationContext, OperationId, PutOptions, ReplaceOptions, ScanOptions, MAX_SCAN_PAGE_SIZE,
};
use residiuum_sdk::ResidiuumDeployment;
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use serde_json::{json, Value};
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use tempfile::tempdir;

struct ThreadWake(std::thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}

fn mint_cap_for(heap: HeapId, deployment: DeploymentId) -> HeapCap {
    let snapshot = HeapSecuritySnapshot {
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        previous_generation: None,
        grace_deadline_unix_s: None,
        master_public_key: [7; 32],
        previous_master_public_key: None,
        security_revision: SecurityRevision::new(1).unwrap(),
        authority_chain_head_hash: [9; 32],
        administrative_state: HeapAdministrativeState::Active,
        blacklist: vec![],
        policy_rights_ceiling: None,
    };
    let slot = Arc::new(HeapSlot::new(snapshot));
    let certificate = VerifiedCertificate {
        cose_bytes: vec![1],
        fingerprint: [3; 32],
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        certificate_id: CertificateId::new_random().unwrap(),
        holder_public_key: [4; 32],
        rights: Rights::from_bits_certificate(0x0d).unwrap(),
        constraints: Constraints::empty(),
        not_before: 1,
        expires_at: 4_000_000_000,
        issuer_master_key_id: [5; 32],
    };
    mint_capability(
        slot,
        &certificate,
        TrustedInstant {
            unix_s: 1_700_000_000,
        },
    )
    .unwrap()
}

fn prepared_deployment() -> (tempfile::TempDir, HeapCap) {
    let directory = tempdir().unwrap();
    let root = directory.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let deployment_id = DeploymentId::new_random().unwrap();
    let heap_id = HeapId::new_random().unwrap();
    let collection_seed = *residiuum_heap::CollectionId::new_random()
        .unwrap()
        .as_bytes();
    let staged = stage_heap_genesis(
        &layout,
        *deployment_id.as_bytes(),
        *heap_id.as_bytes(),
        collection_seed,
        "driver-test",
    )
    .unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let capability = mint_cap_for(heap_id, deployment_id);
    drop(deployment);
    (directory, capability)
}

fn operation(byte: u8) -> OperationId {
    OperationId([byte; 16])
}

fn assert_handle_contract<T: Clone + Send + Sync>() {}

#[test]
fn compile_contract_handles_are_clone_send_sync() {
    assert_handle_contract::<Client>();
    assert_handle_contract::<Collection<Value>>();
}

#[test]
fn embedded_driver_is_bounded_concurrent_idempotent_and_shared_close() {
    let (directory, capability) = prepared_deployment();
    let client = block_on(Client::open_embedded(
        EmbeddedOptions::new(directory.path(), capability)
            .heap_name("driver-test")
            .workers(2)
            .queue_capacity(8),
    ))
    .unwrap();
    assert!(client.capabilities().embedded);
    assert!(client.capabilities().mutation_identity);
    assert!(!client.capabilities().remote_pooling);
    assert!(client.open_report().total_ns > 0);

    let collection: Collection<Value> = block_on(client.create_collection(
        "orders",
        CreateCollectionOptions {
            context: OperationContext {
                operation_id: Some(operation(1)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap();
    assert_eq!(block_on(client.list_collections()).unwrap().len(), 1);

    let mut threads = Vec::new();
    for index in 0..8u8 {
        let collection = collection.clone();
        threads.push(std::thread::spawn(move || {
            block_on(collection.put(format!("k-{index}"), &json!({ "n": index }))).unwrap()
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(client.inspect().workers, 2);
    assert_eq!(client.inspect().queue_capacity, 8);

    let options = PutOptions {
        context: OperationContext {
            operation_id: Some(operation(2)),
            ..OperationContext::default()
        },
    };
    let first =
        block_on(collection.put_with("stable", &json!({ "v": 1 }), options.clone())).unwrap();
    let replay =
        block_on(collection.put_with("stable", &json!({ "v": 1 }), options.clone())).unwrap();
    assert!(!first.deduplicated);
    assert!(replay.deduplicated);
    assert_eq!(first.storage.event_id, replay.storage.event_id);

    let conflict =
        block_on(collection.put_with("stable", &json!({ "v": 2 }), options)).unwrap_err();
    assert_eq!(conflict.code, ErrorCode::OperationIdentityConflict);

    let replaced = block_on(collection.replace(
        "stable",
        &json!({ "v": 3 }),
        ReplaceOptions {
            if_version: first.storage.event_id,
            context: OperationContext {
                operation_id: Some(operation(3)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap();
    assert_eq!(
        block_on(collection.get("stable")).unwrap(),
        Some(json!({ "v": 3 }))
    );

    let delete_options = DeleteOptions {
        if_version: Some(replaced.storage.event_id),
        context: OperationContext {
            operation_id: Some(operation(4)),
            ..OperationContext::default()
        },
        ..DeleteOptions::default()
    };
    let deleted = block_on(collection.delete("stable", delete_options.clone())).unwrap();
    let delete_replay = block_on(collection.delete("stable", delete_options)).unwrap();
    assert!(deleted.storage.removed);
    assert!(delete_replay.storage.removed);
    assert!(delete_replay.deduplicated);
    assert_eq!(deleted.storage.event_id, delete_replay.storage.event_id);

    let original = block_on(collection.put("cas-delete", &json!({ "v": 1 }))).unwrap();
    let current = block_on(collection.put("cas-delete", &json!({ "v": 2 }))).unwrap();
    let stale_delete = block_on(collection.delete(
        "cas-delete",
        DeleteOptions {
            if_version: Some(original.storage.event_id),
            context: OperationContext {
                operation_id: Some(operation(5)),
                ..OperationContext::default()
            },
            ..DeleteOptions::default()
        },
    ))
    .unwrap_err();
    assert_eq!(stale_delete.code, ErrorCode::Conflict);
    assert_eq!(
        block_on(collection.get("cas-delete")).unwrap(),
        Some(json!({ "v": 2 }))
    );
    let current_delete = block_on(collection.delete(
        "cas-delete",
        DeleteOptions {
            if_version: Some(current.storage.event_id),
            context: OperationContext {
                operation_id: Some(operation(6)),
                ..OperationContext::default()
            },
            ..DeleteOptions::default()
        },
    ))
    .unwrap();
    assert!(current_delete.storage.removed);

    let clone = client.clone();
    block_on(client.close()).unwrap();
    assert!(clone.inspect().closed);
    let closed = block_on(collection.get("stable")).unwrap_err();
    assert_eq!(closed.code, ErrorCode::Closed);
}

#[test]
fn embedded_named_heap_and_bounded_scan_pages_are_honest() {
    let (directory, capability) = prepared_deployment();
    let client = block_on(Client::open_embedded(
        EmbeddedOptions::new(directory.path(), capability)
            .heap_name("driver-test")
            .workers(2)
            .queue_capacity(16),
    ))
    .unwrap();
    assert!(client.capabilities().bounded_scan_pages);
    assert!(!client.capabilities().atomics);

    let records: Collection<Value> = block_on(client.create_collection(
        "records",
        CreateCollectionOptions {
            context: OperationContext {
                operation_id: Some(operation(30)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap();
    let other: Collection<Value> = block_on(client.create_collection(
        "other",
        CreateCollectionOptions {
            context: OperationContext {
                operation_id: Some(operation(31)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap();
    for index in 0..5u8 {
        block_on(records.put(format!("k-{index}"), &json!({ "n": index }))).unwrap();
    }

    let first = block_on(records.scan_page(ScanOptions {
        page_size: 2,
        ..ScanOptions::default()
    }))
    .unwrap();
    assert!(first.complete);
    assert!(first.incomplete.is_empty());
    assert_eq!(first.rows.len(), 2);
    assert_eq!(first.rows[0].key, "k-0");
    let first_cursor = first.continuation.clone().expect("more records");

    let mismatch = block_on(other.scan_page(ScanOptions {
        page_size: 2,
        continuation: Some(first_cursor.clone()),
        ..ScanOptions::default()
    }))
    .unwrap_err();
    assert_eq!(mismatch.code, ErrorCode::PermissionDenied);

    let second = block_on(records.scan_page(ScanOptions {
        page_size: 2,
        continuation: Some(first_cursor),
        ..ScanOptions::default()
    }))
    .unwrap();
    assert_eq!(second.rows[0].key, "k-2");
    let third = block_on(records.scan_page(ScanOptions {
        page_size: 2,
        continuation: second.continuation,
        ..ScanOptions::default()
    }))
    .unwrap();
    assert_eq!(third.rows.len(), 1);
    assert_eq!(third.rows[0].key, "k-4");
    assert!(third.continuation.is_none());

    let oversized = block_on(records.scan_page(ScanOptions {
        page_size: MAX_SCAN_PAGE_SIZE + 1,
        ..ScanOptions::default()
    }))
    .unwrap_err();
    assert_eq!(oversized.code, ErrorCode::Validation);
}

#[test]
fn embedded_named_heap_refuses_a_capability_name_mismatch() {
    let (directory, capability) = prepared_deployment();
    let error = block_on(Client::open_embedded(
        EmbeddedOptions::new(directory.path(), capability).heap_name("not-driver-test"),
    ))
    .err()
    .expect("heap name mismatch must be refused");
    assert_eq!(error.code, ErrorCode::Validation);
}

#[test]
fn gremlin_profile_replaces_one_large_conversation_document_per_command() {
    let (directory, capability) = prepared_deployment();
    let client = block_on(Client::open_embedded(
        EmbeddedOptions::new(directory.path(), capability)
            .workers(2)
            .queue_capacity(16),
    ))
    .unwrap();
    let conversations: Collection<Value> = block_on(client.create_collection(
        "conversations",
        CreateCollectionOptions {
            context: OperationContext {
                operation_id: Some(operation(20)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap();

    let initial = json!({
        "conversation_id": "gremlin-7",
        "version": 0,
        "turns": [],
    });
    let established = block_on(conversations.put_with(
        "gremlin-7",
        &initial,
        PutOptions {
            context: OperationContext {
                operation_id: Some(operation(21)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap();

    // Deliberately larger than ordinary chat turns: the supported profile is
    // still one logical document mutation, not an application-owned tree.
    let rambling = "agent-stream ".repeat(40_000);
    let completed_turn = json!({
        "conversation_id": "gremlin-7",
        "version": 1,
        "turns": [{
            "ordinal": 1,
            "turn_id": "turn-1",
            "role": "agent",
            "content": rambling,
        }],
    });
    let replace_options = ReplaceOptions {
        if_version: established.storage.event_id,
        context: OperationContext {
            operation_id: Some(operation(22)),
            ..OperationContext::default()
        },
    };
    let committed = block_on(conversations.replace(
        "gremlin-7",
        &completed_turn,
        replace_options.clone(),
    ))
    .unwrap();
    let replay = block_on(conversations.replace(
        "gremlin-7",
        &completed_turn,
        replace_options,
    ))
    .unwrap();
    assert!(replay.deduplicated);
    assert_eq!(committed.storage.event_id, replay.storage.event_id);

    let stale = block_on(conversations.replace(
        "gremlin-7",
        &json!({ "conversation_id": "gremlin-7", "version": 2, "turns": [] }),
        ReplaceOptions {
            if_version: established.storage.event_id,
            context: OperationContext {
                operation_id: Some(operation(23)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap_err();
    assert_eq!(stale.code, ErrorCode::Conflict);
    assert_eq!(
        block_on(conversations.get("gremlin-7")).unwrap(),
        Some(completed_turn)
    );
}
