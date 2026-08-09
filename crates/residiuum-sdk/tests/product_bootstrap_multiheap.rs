//! Product bootstrap → one physical Client → two capability-bound Heaps.

use residiuum_authority::{
    bootstrap_development_file_product, DevelopmentFileProductBootstrap, ProductHeapDisposition,
    ProductHeapRequest,
};
use residiuum_heap::Rights;
use residiuum_sdk::driver::{
    Client, Collection, CreateCollectionOptions, EmbeddedOptions, OperationContext, OperationId,
};
use residiuum_sdk::ResidiuumDeployment;
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

fn operation(byte: u8) -> OperationId {
    OperationId([byte; 16])
}

fn application_rights() -> Rights {
    Rights::from_bits_certificate(
        Rights::READ.bits()
            | Rights::WRITE.bits()
            | Rights::INDEX_ADMIN.bits()
            | Rights::HEAP_ADMIN.bits(),
    )
    .unwrap()
}

#[test]
fn bootstrap_caps_open_both_heaps_on_one_client_and_survive_restart() {
    let directory = tempdir().unwrap();
    let store = directory.path().join("store");
    drop(ResidiuumDeployment::create(&store).unwrap());
    let config = DevelopmentFileProductBootstrap::new(
        &store,
        directory.path().join("authority"),
        directory.path().join("credentials/gremlin.v1.cbor"),
        "io.koderra.gremlin",
        vec![
            ProductHeapRequest::new("tinker", application_rights()),
            ProductHeapRequest::new("gremlin", application_rights()),
        ],
    );

    let bootstrapped = bootstrap_development_file_product(&config).unwrap();
    assert!(bootstrapped
        .heaps
        .iter()
        .all(|heap| heap.disposition == ProductHeapDisposition::Created));
    let tinker_cap = bootstrapped.heaps[0].capability.clone();
    let gremlin_cap = bootstrapped.heaps[1].capability.clone();
    let deployment_id = bootstrapped.deployment_id;
    drop(bootstrapped);

    let client = block_on(Client::open_embedded(
        EmbeddedOptions::new(&store).workers(2).queue_capacity(16),
    ))
    .unwrap();
    let tinker = block_on(client.open_named_heap("tinker", tinker_cap)).unwrap();
    let gremlin = block_on(client.open_named_heap("gremlin", gremlin_cap)).unwrap();
    assert_ne!(tinker.heap_id(), gremlin.heap_id());
    assert_eq!(tinker.connection().inspect().workers, 2);
    assert_eq!(gremlin.connection().inspect().workers, 2);

    let tinker_records: Collection<Value> = block_on(tinker.create_collection(
        "records",
        CreateCollectionOptions {
            context: OperationContext {
                operation_id: Some(operation(1)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap();
    let gremlin_records: Collection<Value> = block_on(gremlin.create_collection(
        "records",
        CreateCollectionOptions {
            context: OperationContext {
                operation_id: Some(operation(2)),
                ..OperationContext::default()
            },
        },
    ))
    .unwrap();
    block_on(tinker_records.put("same-key", &json!({"owner": "tinker"}))).unwrap();
    block_on(gremlin_records.put("same-key", &json!({"owner": "gremlin"}))).unwrap();
    assert_eq!(
        block_on(tinker_records.get("same-key")).unwrap(),
        Some(json!({"owner": "tinker"}))
    );
    assert_eq!(
        block_on(gremlin_records.get("same-key")).unwrap(),
        Some(json!({"owner": "gremlin"}))
    );
    block_on(client.close()).unwrap();
    drop(tinker_records);
    drop(gremlin_records);
    drop(tinker);
    drop(gremlin);
    drop(client);

    let restarted = bootstrap_development_file_product(&config).unwrap();
    assert_eq!(restarted.deployment_id, deployment_id);
    assert!(restarted
        .heaps
        .iter()
        .all(|heap| heap.disposition == ProductHeapDisposition::Loaded));
    let reopened = block_on(Client::open_embedded(EmbeddedOptions::new(&store))).unwrap();
    let tinker =
        block_on(reopened.open_named_heap("tinker", restarted.heaps[0].capability.clone()))
            .unwrap();
    let gremlin =
        block_on(reopened.open_named_heap("gremlin", restarted.heaps[1].capability.clone()))
            .unwrap();
    let tinker_records: Collection<Value> = block_on(tinker.open_collection("records")).unwrap();
    let gremlin_records: Collection<Value> = block_on(gremlin.open_collection("records")).unwrap();
    assert_eq!(
        block_on(tinker_records.get("same-key")).unwrap(),
        Some(json!({"owner": "tinker"}))
    );
    assert_eq!(
        block_on(gremlin_records.get("same-key")).unwrap(),
        Some(json!({"owner": "gremlin"}))
    );
    block_on(reopened.close()).unwrap();
}
