//! DRV-1/DRV-5 embedded smart-client contract and concurrency evidence.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapCap, HeapId, HeapSecuritySnapshot, HeapSlot, Rights,
    SecurityRevision, TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::driver::{
    atomics::{AtomicId, AtomicOptions, AtomicOutcome, AtomicRead},
    Client, Collection, CreateCollectionOptions, DeleteOptions, EmbeddedOptions, ErrorCode,
    HeapClient, OperationContext, OperationId, PutManyEntry, PutOptions, ReplaceOptions,
    ScanOptions, MAX_SCAN_PAGE_SIZE,
};
use residiuum_sdk::{Parameters, QueryRunOptions, ResidiuumDeployment, RqlFullExecuteOptions};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use serde_json::{json, Value};
use std::future::Future;
use std::path::Path;
use std::pin::pin;
use std::process::Command;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};
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

fn crash_deployment_ids() -> (DeploymentId, HeapId) {
    let mut deployment = [0x51; 16];
    deployment[6] = (deployment[6] & 0x0f) | 0x40;
    deployment[8] = (deployment[8] & 0x3f) | 0x80;
    let mut heap = [0x52; 16];
    heap[6] = (heap[6] & 0x0f) | 0x40;
    heap[8] = (heap[8] & 0x3f) | 0x80;
    (
        DeploymentId::from_bytes(deployment).unwrap(),
        HeapId::from_bytes(heap).unwrap(),
    )
}

fn prepare_crash_deployment(root: &Path) -> HeapCap {
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let (deployment_id, heap_id) = crash_deployment_ids();
    let staged = stage_heap_genesis(
        &layout,
        *deployment_id.as_bytes(),
        *heap_id.as_bytes(),
        [0x53; 16],
        "gremlin-crash",
    )
    .unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    drop(deployment);
    mint_cap_for(heap_id, deployment_id)
}

fn crash_atomic_id() -> AtomicId {
    AtomicId::from_bytes([0x54; 32]).unwrap()
}

fn build_crash_plan(
    heap: &HeapClient,
    states: &Collection<Value>,
    turns: &Collection<Value>,
    locators: &Collection<Value>,
) -> residiuum_sdk::driver::atomics::AtomicPlan {
    let mut atomic = heap.atomic(AtomicOptions::new(crash_atomic_id())).unwrap();
    atomic
        .create(states, "conversation-kill", &json!({"turn": 1}))
        .unwrap()
        .create(turns, "turn-kill", &json!({"text": "survives sigkill"}))
        .unwrap()
        .create(locators, "turn-id-kill", &json!({"turn_key": "turn-kill"}))
        .unwrap();
    atomic.build().unwrap()
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
    assert!(EmbeddedOptions::new("unused").enrichment_enabled);
    assert!(
        !EmbeddedOptions::new("unused")
            .enrichment_enabled(false)
            .enrichment_enabled
    );
}

#[test]
fn atomic_driver_runs_gremlin_three_record_journey_and_restart_resolution() {
    let (directory, capability) = prepared_deployment();
    let retained_capability = capability.clone();
    let connection = block_on(Client::open_embedded(
        EmbeddedOptions::new(directory.path())
            .workers(2)
            .queue_capacity(32),
    ))
    .unwrap();
    assert!(
        !connection.capabilities().atomics,
        "release gate remains closed"
    );
    let heap = block_on(connection.open_heap(capability)).unwrap();
    let states: Collection<Value> =
        block_on(heap.create_collection("states", CreateCollectionOptions::default())).unwrap();
    let turns: Collection<Value> =
        block_on(heap.create_collection("turns", CreateCollectionOptions::default())).unwrap();
    let locators: Collection<Value> =
        block_on(heap.create_collection("locators", CreateCollectionOptions::default())).unwrap();
    let initial = block_on(states.put("conversation-1", &json!({"turn": 0}))).unwrap();

    let atomic_id = AtomicId::random().unwrap();
    let mut atomic = heap.atomic(AtomicOptions::new(atomic_id)).unwrap();
    assert!(matches!(
        block_on(atomic.read(&states, "conversation-1")).unwrap(),
        AtomicRead::Present {
            version: Some(_),
            ..
        }
    ));
    atomic
        .replace(
            &states,
            "conversation-1",
            initial.storage.event_id,
            &json!({"turn": 1}),
        )
        .unwrap()
        .create(&turns, "turn-1", &json!({"text": "hello"}))
        .unwrap()
        .create(&locators, "turn-id-1", &json!({"turn_key": "turn-1"}))
        .unwrap();
    assert!(matches!(
        block_on(atomic.read(&turns, "turn-1")).unwrap(),
        AtomicRead::Present { version: None, .. }
    ));
    let plan = atomic.build().unwrap();
    let committed = block_on(heap.commit_atomic(plan.clone())).unwrap();
    let AtomicOutcome::Committed(receipt) = committed else {
        panic!("three-record journey did not commit")
    };
    assert_eq!(receipt.members.len(), 3);
    assert!(!receipt.replayed);
    assert_eq!(
        block_on(states.get("conversation-1")).unwrap().unwrap()["turn"],
        1
    );
    assert_eq!(
        block_on(turns.get("turn-1")).unwrap().unwrap()["text"],
        "hello"
    );
    assert_eq!(
        block_on(locators.get("turn-id-1")).unwrap().unwrap()["turn_key"],
        "turn-1"
    );
    let first_store_stats = connection.inspect().atomics.store.unwrap();
    assert_eq!(first_store_stats.executions, 1);
    assert_eq!(first_store_stats.committed, 1);
    assert_eq!(first_store_stats.members, 3);
    assert_eq!(first_store_stats.durability_cohorts, 1);
    assert_eq!(first_store_stats.max_cohort_members, 3);
    assert_eq!(first_store_stats.authoritative_sync_operations, 2);
    assert!(first_store_stats.authoritative_write_operations > 0);
    assert!(first_store_stats.authoritative_write_bytes > 0);
    assert!(first_store_stats.catalog_open_ns > 0);
    assert!(first_store_stats.decision_publish_ns > 0);
    assert!(first_store_stats.validation_ns > 0);
    assert!(first_store_stats.member_boundary_ns > 0);
    assert!(first_store_stats.decision_boundary_ns > 0);
    assert!(first_store_stats.publication_ns > 0);

    let AtomicOutcome::Committed(replayed) = block_on(heap.commit_atomic(plan.clone())).unwrap()
    else {
        panic!("same id/root did not replay")
    };
    assert!(replayed.replayed);
    assert_eq!(replayed.commit_position, receipt.commit_position);
    let replay_store_stats = connection.inspect().atomics.store.unwrap();
    assert_eq!(replay_store_stats.executions, 2);
    assert_eq!(replay_store_stats.committed, 2);
    assert_eq!(replay_store_stats.replayed, 1);
    assert_eq!(replay_store_stats.members, 6);
    assert_eq!(replay_store_stats.durability_cohorts, 1);
    assert_eq!(
        replay_store_stats.authoritative_sync_operations,
        first_store_stats.authoritative_sync_operations,
        "durable replay must not create another physical sync"
    );
    assert_eq!(
        block_on(heap.atomic_status(atomic_id)).unwrap().logical,
        residiuum_atomics::LogicalStatus::Committed
    );

    let mut changed = heap.atomic(AtomicOptions::new(atomic_id)).unwrap();
    changed
        .create(
            &turns,
            "same-id-different-root",
            &json!({"text": "must refuse"}),
        )
        .unwrap();
    let conflict = block_on(heap.commit_atomic(changed.build().unwrap())).unwrap_err();
    assert_eq!(conflict.code, ErrorCode::AtomicIdConflict);
    assert_eq!(
        conflict.atomic_code,
        Some(residiuum_sdk::driver::atomics::AtomicCode::AtomicIdConflict)
    );
    assert!(block_on(turns.get("same-id-different-root"))
        .unwrap()
        .is_none());

    let established = block_on(states.get_versioned("conversation-1"))
        .unwrap()
        .unwrap();
    let mut left = heap
        .atomic(AtomicOptions::new(AtomicId::random().unwrap()))
        .unwrap();
    left.replace(
        &states,
        "conversation-1",
        established.version,
        &json!({"turn": 2, "winner": "left"}),
    )
    .unwrap()
    .create(&turns, "race-left", &json!({"winner": "left"}))
    .unwrap();
    let mut right = heap
        .atomic(AtomicOptions::new(AtomicId::random().unwrap()))
        .unwrap();
    right
        .replace(
            &states,
            "conversation-1",
            established.version,
            &json!({"turn": 2, "winner": "right"}),
        )
        .unwrap()
        .create(&turns, "race-right", &json!({"winner": "right"}))
        .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let left_heap = heap.clone();
    let left_barrier = Arc::clone(&barrier);
    let left_thread = std::thread::spawn(move || {
        left_barrier.wait();
        block_on(left_heap.commit_atomic(left.build().unwrap())).unwrap()
    });
    let right_heap = heap.clone();
    let right_barrier = Arc::clone(&barrier);
    let right_thread = std::thread::spawn(move || {
        right_barrier.wait();
        block_on(right_heap.commit_atomic(right.build().unwrap())).unwrap()
    });
    let race = [left_thread.join().unwrap(), right_thread.join().unwrap()];
    assert_eq!(
        race.iter()
            .filter(|outcome| matches!(outcome, AtomicOutcome::Committed(_)))
            .count(),
        1
    );
    assert_eq!(
        race.iter()
            .filter(|outcome| matches!(outcome, AtomicOutcome::NotCommitted { .. }))
            .count(),
        1
    );
    let race_left = block_on(turns.get("race-left")).unwrap();
    let race_right = block_on(turns.get("race-right")).unwrap();
    assert_ne!(race_left.is_some(), race_right.is_some());

    let stale_id = AtomicId::random().unwrap();
    let mut stale = heap.atomic(AtomicOptions::new(stale_id)).unwrap();
    stale
        .replace(
            &states,
            "conversation-1",
            initial.storage.event_id,
            &json!({"turn": 2}),
        )
        .unwrap()
        .create(&turns, "must-not-exist", &json!({"text": "partial"}))
        .unwrap();
    assert!(matches!(
        block_on(heap.commit_atomic(stale.build().unwrap())).unwrap(),
        AtomicOutcome::NotCommitted { .. }
    ));
    assert!(block_on(turns.get("must-not-exist")).unwrap().is_none());

    let lost_reply_id = AtomicId::random().unwrap();
    let mut lost_reply = heap.atomic(AtomicOptions::new(lost_reply_id)).unwrap();
    lost_reply
        .create(&turns, "lost-reply", &json!({"durable": true}))
        .unwrap();
    let lost_reply_plan = lost_reply.build().unwrap();
    residiuum_store::arm_failpoint_once(
        "store.atomic.before_ack",
        residiuum_store::FailpointAction::Error,
    );
    assert!(matches!(
        block_on(heap.commit_atomic(lost_reply_plan.clone())).unwrap(),
        AtomicOutcome::Unknown { atomic_id, .. } if atomic_id == lost_reply_id
    ));
    residiuum_store::disarm_failpoint("store.atomic.before_ack");
    assert_eq!(
        block_on(heap.atomic_status(lost_reply_id)).unwrap().logical,
        residiuum_atomics::LogicalStatus::Committed
    );
    let AtomicOutcome::Committed(lost_reply_replay) =
        block_on(heap.commit_atomic(lost_reply_plan)).unwrap()
    else {
        panic!("lost reply retry did not resolve committed")
    };
    assert!(lost_reply_replay.replayed);
    let atomic_inspection = connection.inspect().atomics;
    assert_eq!(atomic_inspection.inflight, 0);
    assert!(atomic_inspection.submitted >= 8);
    assert!(atomic_inspection.committed >= 4);
    assert!(atomic_inspection.not_committed >= 2);
    assert!(atomic_inspection.unknown >= 1);
    assert!(atomic_inspection.refused >= 1);
    assert!(atomic_inspection.identity_conflicts >= 1);
    assert!(atomic_inspection.status_lookups >= 2);
    assert!(atomic_inspection.submitted_members >= 14);
    assert!(atomic_inspection.max_members >= 3);
    assert!(atomic_inspection.submitted_plan_bytes > 0);
    assert!(atomic_inspection.max_plan_bytes > 0);
    assert!(atomic_inspection.total_engine_latency_ns > 0);
    assert!(atomic_inspection.max_engine_latency_ns > 0);

    block_on(connection.close()).unwrap();
    drop(states);
    drop(turns);
    drop(locators);
    drop(heap);
    drop(connection);
    let mut compacted = residiuum_store::Store::open(directory.path()).unwrap();
    compacted.compact_live().unwrap();
    drop(compacted);
    let reopened = block_on(Client::open_embedded(EmbeddedOptions::new(
        directory.path(),
    )))
    .unwrap();
    let reopened_heap = block_on(reopened.open_heap(retained_capability)).unwrap();
    assert_eq!(
        block_on(reopened_heap.atomic_status(atomic_id))
            .unwrap()
            .logical,
        residiuum_atomics::LogicalStatus::Committed
    );
    let AtomicOutcome::Committed(replayed_after_restart) =
        block_on(reopened_heap.commit_atomic(plan)).unwrap()
    else {
        panic!("restart retry did not replay")
    };
    assert!(replayed_after_restart.replayed);
}

#[test]
fn atomic_builder_refuses_collection_from_another_heap_before_build() {
    let (directory, left_cap, right_cap) = prepared_two_heap_deployment();
    let connection = block_on(Client::open_embedded(EmbeddedOptions::new(
        directory.path(),
    )))
    .unwrap();
    let left = block_on(connection.open_heap(left_cap)).unwrap();
    let right = block_on(connection.open_heap(right_cap)).unwrap();
    let left_collection: Collection<Value> =
        block_on(left.create_collection("left-docs", CreateCollectionOptions::default())).unwrap();
    let right_collection: Collection<Value> =
        block_on(right.create_collection("right-docs", CreateCollectionOptions::default()))
            .unwrap();
    let mut atomic = left
        .atomic(AtomicOptions::new(AtomicId::random().unwrap()))
        .unwrap();
    atomic
        .create(&left_collection, "ok", &json!({"v": 1}))
        .unwrap();
    let error = match atomic.create(&right_collection, "no", &json!({"v": 2})) {
        Ok(_) => panic!("foreign collection was admitted"),
        Err(error) => error,
    };
    assert_eq!(error.code, ErrorCode::PermissionDenied);
    assert_eq!(
        error.atomic_code,
        Some(residiuum_sdk::driver::atomics::AtomicCode::AtomicScopeEscape)
    );
}

#[test]
fn atomic_submission_deadline_before_dispatch_issues_nothing_and_plan_can_be_renewed() {
    let (directory, capability) = prepared_deployment();
    let connection = block_on(Client::open_embedded(EmbeddedOptions::new(
        directory.path(),
    )))
    .unwrap();
    let heap = block_on(connection.open_heap(capability)).unwrap();
    let records: Collection<Value> =
        block_on(heap.create_collection("deadline-records", CreateCollectionOptions::default()))
            .unwrap();
    let atomic_id = AtomicId::random().unwrap();
    let mut atomic = heap
        .atomic(
            AtomicOptions::new(atomic_id)
                .with_deadline(Instant::now() + Duration::from_millis(100)),
        )
        .unwrap();
    atomic
        .create(&records, "deadline-key", &json!({"v": 1}))
        .unwrap();
    let plan = atomic.build().unwrap();
    std::thread::sleep(Duration::from_millis(120));

    let error = block_on(heap.commit_atomic(plan.clone())).unwrap_err();
    assert_eq!(error.code, ErrorCode::AtomicDeadlineExceeded);
    assert_eq!(
        error.atomic_code,
        Some(residiuum_sdk::driver::atomics::AtomicCode::AtomicDeadlineExceeded)
    );
    assert_eq!(
        block_on(heap.atomic_status(atomic_id)).unwrap().logical,
        residiuum_atomics::LogicalStatus::NotFound
    );
    assert!(block_on(records.get("deadline-key")).unwrap().is_none());

    let renewed = plan.with_deadline(Instant::now() + Duration::from_secs(1));
    assert!(matches!(
        block_on(heap.commit_atomic(renewed)).unwrap(),
        AtomicOutcome::Committed(_)
    ));
    let inspection = connection.inspect().atomics;
    assert_eq!(inspection.submitted, 2);
    assert_eq!(inspection.inflight, 0);
    assert_eq!(inspection.committed, 1);
    assert_eq!(inspection.refused, 1);
    assert_eq!(inspection.status_lookups, 1);
    assert_eq!(inspection.submitted_members, 2);
    assert_eq!(inspection.max_members, 1);
    assert!(inspection.submitted_plan_bytes >= inspection.max_plan_bytes as u64);
}

#[test]
fn terminal_atomics_do_not_consume_the_eight_outstanding_slots() {
    let (directory, capability) = prepared_deployment();
    let reopen_capability = capability.clone();
    let connection = block_on(Client::open_embedded(EmbeddedOptions::new(
        directory.path(),
    )))
    .unwrap();
    let heap = block_on(connection.open_heap(capability)).unwrap();
    let records: Collection<Value> = block_on(
        heap.create_collection("terminal-slot-records", CreateCollectionOptions::default()),
    )
    .unwrap();
    let mut ids = Vec::new();
    for index in 0..12 {
        let atomic_id = AtomicId::random().unwrap();
        let mut atomic = heap.atomic(AtomicOptions::new(atomic_id)).unwrap();
        atomic
            .create(
                &records,
                format!("record-{index}"),
                &json!({"index": index}),
            )
            .unwrap();
        assert!(matches!(
            block_on(heap.commit_atomic(atomic.build().unwrap())).unwrap(),
            AtomicOutcome::Committed(_)
        ));
        ids.push(atomic_id);
    }
    let stats = connection.inspect().atomics.store.unwrap();
    assert_eq!(stats.executions, 12);
    assert_eq!(stats.durability_cohorts, 12);
    assert_eq!(stats.authoritative_sync_operations, 24);
    block_on(connection.close()).unwrap();
    drop(records);
    drop(heap);
    drop(connection);

    let reopened = block_on(Client::open_embedded(EmbeddedOptions::new(
        directory.path(),
    )))
    .unwrap();
    let heap = block_on(reopened.open_heap(reopen_capability)).unwrap();
    for atomic_id in ids {
        let status = block_on(heap.atomic_status(atomic_id)).unwrap();
        assert_eq!(status.logical, residiuum_atomics::LogicalStatus::Committed);
        assert!(status.receipt.is_some());
    }
}

#[cfg(unix)]
#[test]
fn atomic_external_sigkill_after_decision_before_ack_resolves_and_replays() {
    const CHILD_ROOT: &str = "RESIDIUUM_ATM5_SIGKILL_ROOT";
    const CHILD_MARKER: &str = "RESIDIUUM_ATM5_SIGKILL_MARKER";
    const FAILPOINT: &str = "store.atomic.before_ack";

    if let (Ok(root), Ok(marker)) = (std::env::var(CHILD_ROOT), std::env::var(CHILD_MARKER)) {
        let capability = prepare_crash_deployment(Path::new(&root));
        let connection = block_on(Client::open_embedded(EmbeddedOptions::new(&root))).unwrap();
        let heap = block_on(connection.open_heap(capability)).unwrap();
        let states: Collection<Value> =
            block_on(heap.create_collection("kill-states", CreateCollectionOptions::default()))
                .unwrap();
        let turns: Collection<Value> =
            block_on(heap.create_collection("kill-turns", CreateCollectionOptions::default()))
                .unwrap();
        let locators: Collection<Value> =
            block_on(heap.create_collection("kill-locators", CreateCollectionOptions::default()))
                .unwrap();
        let plan = build_crash_plan(&heap, &states, &turns, &locators);

        residiuum_store::clear_failpoint_visits();
        residiuum_store::enable_failpoint_hit_proof();
        let marker_path = std::path::PathBuf::from(marker);
        std::thread::spawn(move || loop {
            if residiuum_store::failpoint_visit_count(FAILPOINT) != 0 {
                std::fs::write(&marker_path, FAILPOINT).unwrap();
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        });
        residiuum_store::arm_failpoint_once(FAILPOINT, residiuum_store::FailpointAction::Pause);
        let _never_returns = block_on(heap.commit_atomic(plan));
        panic!("external crash controller did not kill paused child");
    }

    let directory = tempdir().unwrap();
    let marker = directory.path().join("before-ack.reached");
    let executable = std::env::current_exe().unwrap();
    let mut child = Command::new(executable)
        .args([
            "--exact",
            "atomic_external_sigkill_after_decision_before_ack_resolves_and_replays",
            "--nocapture",
        ])
        .env(CHILD_ROOT, directory.path())
        .env(CHILD_MARKER, &marker)
        .spawn()
        .unwrap();

    let wait_deadline = Instant::now() + Duration::from_secs(15);
    while !marker.is_file() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("crash child exited before the boundary: {status}");
        }
        assert!(
            Instant::now() < wait_deadline,
            "crash child did not reach {FAILPOINT}"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    child.kill().unwrap();
    let status = child.wait().unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(
        status.signal(),
        Some(9),
        "child must die by external SIGKILL"
    );

    let (deployment_id, heap_id) = crash_deployment_ids();
    let capability = mint_cap_for(heap_id, deployment_id);
    let connection = block_on(Client::open_embedded(EmbeddedOptions::new(
        directory.path(),
    )))
    .unwrap();
    let heap = block_on(connection.open_heap(capability)).unwrap();
    let recovery_stats = connection.inspect().atomics.store.unwrap();
    assert!(recovery_stats.recovered_atomics >= 1);
    assert!(recovery_stats.recovery_frames > 0);
    assert!(recovery_stats.recovery_ns > 0);
    let status = block_on(heap.atomic_status(crash_atomic_id())).unwrap();
    assert_eq!(status.logical, residiuum_atomics::LogicalStatus::Committed);
    assert!(status.receipt.is_some());

    let states: Collection<Value> = block_on(heap.open_collection("kill-states")).unwrap();
    let turns: Collection<Value> = block_on(heap.open_collection("kill-turns")).unwrap();
    let locators: Collection<Value> = block_on(heap.open_collection("kill-locators")).unwrap();
    assert_eq!(
        block_on(states.get("conversation-kill")).unwrap().unwrap()["turn"],
        1
    );
    assert_eq!(
        block_on(turns.get("turn-kill")).unwrap().unwrap()["text"],
        "survives sigkill"
    );
    assert_eq!(
        block_on(locators.get("turn-id-kill")).unwrap().unwrap()["turn_key"],
        "turn-kill"
    );
    let plan = build_crash_plan(&heap, &states, &turns, &locators);
    let AtomicOutcome::Committed(replayed) = block_on(heap.commit_atomic(plan)).unwrap() else {
        panic!("same plan did not replay committed after SIGKILL")
    };
    assert!(replayed.replayed);
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

    let barrier = Arc::new(std::sync::Barrier::new(8));
    let mut threads = Vec::new();
    for index in 0..8u8 {
        let collection = collection.clone();
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            block_on(collection.put(format!("k-{index}"), &json!({ "n": index }))).unwrap()
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    let inspection = client.inspect();
    assert_eq!(inspection.workers, 2);
    assert_eq!(inspection.queue_capacity, 8);
    assert!(inspection.queue_byte_capacity > 0);
    assert_eq!(inspection.admitted_bytes, 0);
    assert!(inspection.peak_admitted_bytes > 0);
    assert_eq!(inspection.byte_refused, 0);
    assert_eq!(inspection.inflight_mutations, 0);
    assert!(
        inspection.peak_inflight_mutations > inspection.workers,
        "durable writes must not be limited by read/query worker occupancy: {inspection:?}"
    );
    let commits = inspection.operation_commits;
    assert_eq!(commits.submitted, 8);
    assert_eq!(commits.committed, 8);
    assert_eq!(commits.failed, 0);
    assert!(
        commits.cohorts < commits.submitted,
        "concurrent durable acknowledgements must share physical cohorts: {commits:?}"
    );
    assert!(commits.max_cohort_entries >= 2);
    assert_eq!(commits.admitted_bytes, 0);
    assert!(commits.admitted_byte_capacity > 0);
    assert!(commits.peak_admitted_bytes > 0);
    assert!(commits.peak_admitted_bytes <= commits.admitted_byte_capacity);
    let write_path = inspection
        .write_path
        .expect("healthy embedded store must expose write-path counters");
    assert!(write_path.async_lifecycle_enabled);
    assert!(write_path.shadow_dual_stream);
    assert_eq!(commits.successful_media_sync_cohorts, commits.cohorts);
    assert_eq!(commits.successful_journal_sync_cohorts, 0);
    assert_eq!(commits.successful_journal_append_cohorts, commits.cohorts);

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
fn async_bulk_put_pipelines_independent_receipts_without_atomicity_claims() {
    let (directory, capability) = prepared_deployment();
    let connection = block_on(Client::open_embedded(
        EmbeddedOptions::new(directory.path()).queue_capacity(64),
    ))
    .unwrap();
    let heap = block_on(connection.open_named_heap("driver-test", capability)).unwrap();
    let collection: Collection<Value> =
        block_on(heap.create_collection("bulk-records", CreateCollectionOptions::default()))
            .unwrap();

    let entries = (0..32u8)
        .map(|index| {
            PutManyEntry::new(format!("bulk-{index:03}"), json!({ "n": index })).options(
                PutOptions {
                    context: OperationContext {
                        operation_id: Some(operation(index.saturating_add(100))),
                        ..OperationContext::default()
                    },
                },
            )
        })
        .collect();
    let outcomes = block_on(collection.put_many(entries)).unwrap();
    assert_eq!(outcomes.len(), 32);
    assert!(outcomes.iter().all(|outcome| outcome.result.is_ok()));
    assert_eq!(outcomes[0].key, "bulk-000");
    assert_eq!(outcomes[31].key, "bulk-031");

    let inspection = connection.inspect();
    assert!(inspection.operation_commits.max_cohort_entries >= 2);
    assert!(inspection.operation_commits.parallel_cooked_operations >= 2);
    assert!(inspection.operation_commits.reserved_cooked_cohorts >= 1);
    assert_eq!(inspection.operation_commits.reserved_cooked_operations, 32);

    let retries = (0..32u8)
        .map(|index| {
            PutManyEntry::new(format!("bulk-{index:03}"), json!({ "n": index })).options(
                PutOptions {
                    context: OperationContext {
                        operation_id: Some(operation(index.saturating_add(100))),
                        ..OperationContext::default()
                    },
                },
            )
        })
        .collect();
    let retried = block_on(collection.put_many(retries)).unwrap();
    assert!(retried.iter().all(|outcome| {
        outcome
            .result
            .as_ref()
            .map(|receipt| receipt.deduplicated)
            .unwrap_or(false)
    }));
    for index in 0..32u8 {
        assert_eq!(
            block_on(collection.get(format!("bulk-{index:03}"))).unwrap(),
            Some(json!({ "n": index }))
        );
    }

    block_on(connection.close()).unwrap();
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
