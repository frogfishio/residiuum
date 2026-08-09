//! DRV-1/DRV-5 embedded smart-client contract and concurrency evidence.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapCap, HeapId, HeapSecuritySnapshot, HeapSlot, Rights,
    SecurityRevision, TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::driver::{
    Client, Collection, CreateCollectionOptions, DeleteOptions, EmbeddedOptions, ErrorCode,
    HeapClient, OperationContext, OperationId, PutOptions, ReplaceOptions, ScanOptions,
    MAX_SCAN_PAGE_SIZE,
};
use residiuum_sdk::{Parameters, QueryRunOptions, ResidiuumDeployment, RqlFullExecuteOptions};
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

fn prepared_two_heap_deployment() -> (tempfile::TempDir, HeapCap, HeapCap) {
    let directory = tempdir().unwrap();
    let root = directory.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let deployment_id = DeploymentId::new_random().unwrap();

    let tinker_id = HeapId::new_random().unwrap();
    let tinker = stage_heap_genesis(
        &layout,
        *deployment_id.as_bytes(),
        *tinker_id.as_bytes(),
        *residiuum_heap::CollectionId::new_random()
            .unwrap()
            .as_bytes(),
        "tinker",
    )
    .unwrap();
    publish_staged_genesis(&layout, &tinker.staging_id, &tinker.descriptor_hash).unwrap();

    let gremlin_id = HeapId::new_random().unwrap();
    let gremlin = stage_heap_genesis(
        &layout,
        *deployment_id.as_bytes(),
        *gremlin_id.as_bytes(),
        *residiuum_heap::CollectionId::new_random()
            .unwrap()
            .as_bytes(),
        "gremlin",
    )
    .unwrap();
    publish_staged_genesis(&layout, &gremlin.staging_id, &gremlin.descriptor_hash).unwrap();

    let tinker_capability = mint_cap_for(tinker_id, deployment_id);
    let gremlin_capability = mint_cap_for(gremlin_id, deployment_id);
    drop(deployment);
    (directory, tinker_capability, gremlin_capability)
}

fn operation(byte: u8) -> OperationId {
    OperationId([byte; 16])
}

fn assert_handle_contract<T: Clone + Send + Sync>() {}

#[test]
fn compile_contract_handles_are_clone_send_sync() {
    assert_handle_contract::<Client>();
    assert_handle_contract::<HeapClient>();
    assert_handle_contract::<Collection<Value>>();
}

#[test]
fn bounded_driver_runs_core_and_full_queries_on_one_shared_connection() {
    let (directory, capability) = prepared_deployment();
    let connection = block_on(Client::open_embedded(
        EmbeddedOptions::new(directory.path())
            .workers(4)
            .queue_capacity(32),
    ))
    .unwrap();
    let heap = block_on(connection.open_heap(capability)).unwrap();
    let docs: Collection<Value> =
        block_on(heap.create_collection("docs", CreateCollectionOptions::default())).unwrap();
    let customers: Collection<Value> =
        block_on(heap.create_collection("customers", CreateCollectionOptions::default())).unwrap();
    block_on(customers.put("c-1", &json!({"name":"Customer 1"}))).unwrap();
    for index in 0..256u32 {
        block_on(docs.put(
            format!("d-{index:04}"),
            &json!({
                "status": if index % 2 == 0 { "paid" } else { "open" },
                "customer_id": "c-1",
                "ordinal": index,
            }),
        ))
        .unwrap();
    }

    let barrier = Arc::new(std::sync::Barrier::new(4));
    let mut workers = Vec::new();
    for _ in 0..4 {
        let docs = docs.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            block_on(docs.rql(
                r#"from docs where status = "paid""#,
                &Parameters::default(),
                QueryRunOptions::default(),
            ))
            .unwrap()
        }));
    }
    for worker in workers {
        let page = worker.join().unwrap();
        assert!(!page.rows.is_empty());
    }
    assert!(
        connection.inspect().peak_running >= 2,
        "queries must overlap inside the shared bounded scheduler: {:?}",
        connection.inspect()
    );

    let full = block_on(heap.rql_full(
        "from docs enrich customer using customers matching customer_id = _key expect exactly_one",
        &Parameters::default(),
        RqlFullExecuteOptions::default(),
    ))
    .unwrap();
    assert!(!full.rows.is_empty());
    assert!(full
        .rows
        .iter()
        .all(|(_, row)| row.get("customer").is_some()));
    block_on(connection.close()).unwrap();
}

#[test]
fn one_physical_connection_serves_multiple_authorized_heaps() {
    let (directory, tinker_capability, gremlin_capability) = prepared_two_heap_deployment();
    let connection = block_on(Client::open_embedded(
        EmbeddedOptions::new(directory.path())
            .workers(2)
            .queue_capacity(16),
    ))
    .unwrap();
    assert!(connection.capabilities().multi_heap_bindings);
    let tinker = block_on(connection.open_named_heap("tinker", tinker_capability.clone())).unwrap();
    let gremlin =
        block_on(connection.open_named_heap("gremlin", gremlin_capability.clone())).unwrap();
    assert_ne!(tinker.heap_id(), gremlin.heap_id());

    let wrong_name = block_on(connection.open_named_heap("gremlin", tinker_capability.clone()))
        .err()
        .expect("capability cannot be rebound by another Heap name");
    assert_eq!(wrong_name.code, ErrorCode::Validation);

    let tinker_sessions: Collection<Value> = block_on(tinker.create_collection(
        "sessions",
        CreateCollectionOptions {
            context: OperationContext {
                operation_id: Some(operation(50)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap();
    let gremlin_sessions: Collection<Value> = block_on(gremlin.create_collection(
        "sessions",
        CreateCollectionOptions {
            context: OperationContext {
                operation_id: Some(operation(51)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap();

    let tinker_writer = {
        let sessions = tinker_sessions.clone();
        std::thread::spawn(move || {
            block_on(sessions.put("shared-key", &json!({ "owner": "tinker" }))).unwrap();
            block_on(sessions.put("tinker-next", &json!({ "owner": "tinker" }))).unwrap();
        })
    };
    let gremlin_writer = {
        let sessions = gremlin_sessions.clone();
        std::thread::spawn(move || {
            block_on(sessions.put("shared-key", &json!({ "owner": "gremlin" }))).unwrap();
        })
    };
    tinker_writer.join().unwrap();
    gremlin_writer.join().unwrap();

    assert_eq!(
        block_on(tinker_sessions.get("shared-key")).unwrap(),
        Some(json!({ "owner": "tinker" }))
    );
    assert_eq!(
        block_on(gremlin_sessions.get("shared-key")).unwrap(),
        Some(json!({ "owner": "gremlin" }))
    );

    let tinker_page = block_on(tinker_sessions.scan_page(ScanOptions {
        page_size: 1,
        ..ScanOptions::default()
    }))
    .unwrap();
    let cross_heap_cursor = tinker_page.continuation.expect("second Tinker row");
    let cursor_error = block_on(gremlin_sessions.scan_page(ScanOptions {
        page_size: 1,
        continuation: Some(cross_heap_cursor),
        ..ScanOptions::default()
    }))
    .unwrap_err();
    assert_eq!(cursor_error.code, ErrorCode::PermissionDenied);

    assert_eq!(tinker.inspect(), gremlin.inspect());
    block_on(connection.close()).unwrap();
    assert_eq!(
        block_on(tinker_sessions.get("shared-key"))
            .unwrap_err()
            .code,
        ErrorCode::Closed
    );
    assert_eq!(
        block_on(gremlin_sessions.get("shared-key"))
            .unwrap_err()
            .code,
        ErrorCode::Closed
    );
    drop(tinker_sessions);
    drop(gremlin_sessions);
    drop(tinker);
    drop(gremlin);
    drop(connection);

    let reopened = block_on(Client::open_embedded(EmbeddedOptions::new(
        directory.path(),
    )))
    .unwrap();
    let reopened_tinker = block_on(reopened.open_named_heap("tinker", tinker_capability)).unwrap();
    let reopened_gremlin =
        block_on(reopened.open_named_heap("gremlin", gremlin_capability)).unwrap();
    let reopened_tinker_sessions: Collection<Value> =
        block_on(reopened_tinker.open_collection("sessions")).unwrap();
    let reopened_gremlin_sessions: Collection<Value> =
        block_on(reopened_gremlin.open_collection("sessions")).unwrap();
    assert_eq!(
        block_on(reopened_tinker_sessions.get("shared-key")).unwrap(),
        Some(json!({ "owner": "tinker" }))
    );
    assert_eq!(
        block_on(reopened_gremlin_sessions.get("shared-key")).unwrap(),
        Some(json!({ "owner": "gremlin" }))
    );
}

#[test]
fn embedded_driver_is_bounded_concurrent_idempotent_and_shared_close() {
    let (directory, capability) = prepared_deployment();
    let connection = block_on(Client::open_embedded(
        EmbeddedOptions::new(directory.path())
            .workers(2)
            .queue_capacity(8),
    ))
    .unwrap();
    let client = block_on(connection.open_named_heap("driver-test", capability)).unwrap();
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

    let clone = connection.clone();
    block_on(connection.close()).unwrap();
    assert!(clone.inspect().closed);
    let closed = block_on(collection.get("stable")).unwrap_err();
    assert_eq!(closed.code, ErrorCode::Closed);
}

#[test]
fn embedded_named_heap_and_bounded_scan_pages_are_honest() {
    let (directory, capability) = prepared_deployment();
    let connection = block_on(Client::open_embedded(
        EmbeddedOptions::new(directory.path())
            .workers(2)
            .queue_capacity(16),
    ))
    .unwrap();
    let client = block_on(connection.open_named_heap("driver-test", capability)).unwrap();
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
fn versioned_reads_survive_restart_and_supply_real_cas_tokens() {
    let (directory, capability) = prepared_deployment();
    let first_connection = block_on(Client::open_embedded(EmbeddedOptions::new(
        directory.path(),
    )))
    .unwrap();
    let first_client =
        block_on(first_connection.open_named_heap("driver-test", capability.clone())).unwrap();
    let first_records: Collection<Value> = block_on(first_client.create_collection(
        "restart-records",
        CreateCollectionOptions {
            context: OperationContext {
                operation_id: Some(operation(40)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap();
    let established = block_on(first_records.put_with(
        "conversation-1",
        &json!({ "revision": 1 }),
        PutOptions {
            context: OperationContext {
                operation_id: Some(operation(41)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap();
    block_on(first_connection.close()).unwrap();
    drop(first_records);
    drop(first_client);
    drop(first_connection);

    let second_connection = block_on(Client::open_embedded(EmbeddedOptions::new(
        directory.path(),
    )))
    .unwrap();
    let second_client =
        block_on(second_connection.open_named_heap("driver-test", capability.clone())).unwrap();
    let second_records: Collection<Value> =
        block_on(second_client.open_collection("restart-records")).unwrap();
    let after_restart = block_on(second_records.get_versioned("conversation-1"))
        .unwrap()
        .expect("record after restart");
    assert_eq!(after_restart.value, json!({ "revision": 1 }));
    assert_eq!(after_restart.version, established.storage.event_id);

    let replaced = block_on(second_records.replace(
        "conversation-1",
        &json!({ "revision": 2 }),
        ReplaceOptions {
            if_version: after_restart.version,
            context: OperationContext {
                operation_id: Some(operation(42)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap();
    let stale = block_on(second_records.replace(
        "conversation-1",
        &json!({ "revision": "stale" }),
        ReplaceOptions {
            if_version: after_restart.version,
            context: OperationContext {
                operation_id: Some(operation(43)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap_err();
    assert_eq!(stale.code, ErrorCode::Conflict);
    block_on(second_connection.close()).unwrap();
    drop(second_records);
    drop(second_client);
    drop(second_connection);

    let third_connection = block_on(Client::open_embedded(EmbeddedOptions::new(
        directory.path(),
    )))
    .unwrap();
    let third_client =
        block_on(third_connection.open_named_heap("driver-test", capability)).unwrap();
    let third_records: Collection<Value> =
        block_on(third_client.open_collection("restart-records")).unwrap();
    let page = block_on(third_records.scan_page(ScanOptions {
        page_size: 8,
        ..ScanOptions::default()
    }))
    .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].value, json!({ "revision": 2 }));
    assert_eq!(page.rows[0].version, replaced.storage.event_id);
    block_on(third_records.replace(
        "conversation-1",
        &json!({ "revision": 3 }),
        ReplaceOptions {
            if_version: page.rows[0].version,
            context: OperationContext {
                operation_id: Some(operation(44)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap();
}

#[test]
fn embedded_named_heap_refuses_a_capability_name_mismatch() {
    let (directory, capability) = prepared_deployment();
    let connection = block_on(Client::open_embedded(EmbeddedOptions::new(
        directory.path(),
    )))
    .unwrap();
    let error = block_on(connection.open_named_heap("not-driver-test", capability))
        .err()
        .expect("heap name mismatch must be refused");
    assert_eq!(error.code, ErrorCode::Validation);
}

#[test]
fn gremlin_profile_replaces_one_large_conversation_document_per_command() {
    let (directory, capability) = prepared_deployment();
    let connection = block_on(Client::open_embedded(
        EmbeddedOptions::new(directory.path())
            .workers(2)
            .queue_capacity(16),
    ))
    .unwrap();
    let client = block_on(connection.open_heap(capability)).unwrap();
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
    let committed =
        block_on(conversations.replace("gremlin-7", &completed_turn, replace_options.clone()))
            .unwrap();
    let replay =
        block_on(conversations.replace("gremlin-7", &completed_turn, replace_options)).unwrap();
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
