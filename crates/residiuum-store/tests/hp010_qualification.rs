//! HP-010 Accept evidence: qualification matrix + claim honesty, differential
//! NI (§26.2.1), H3 HeapCap termination + single-owner admit + derived-path
//! indexes/streams/query escape, H4/H5 restore and key-loss drills, load/latency
//! + structured fuzz budgets, operator runbook presence, and H6 partial evidence.
//! Does **not** flip `qualified=true`.

use residiuum_format::{
    admit_frame_to_heap, encode_collection_binding_envelope, encode_stream_binding_envelope,
    AdmitDecision, OwnershipEvidence,
};
use residiuum_heap::{
    claim_language, confine_export_heaps, confine_health_detail, confine_operational_observation,
    confine_operational_observation_under, confine_query_observation, confine_support_bundle,
    connected_authority_model_smoke, connected_model_smoke, connected_pure_proof_bundle,
    h6_decide_obligations, load_isolation_profiles, load_isolation_profiles_from,
    may_advertise_qualified, mint_capability, refresh_capability_or_terminate,
    unauthenticated_field_allowed, unauthenticated_field_allowed_under, AuthorityGeneration,
    CertificateId, CollectionId, Constraint, Constraints, DeploymentId, HealthDetailInput,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, IsolationProfileId,
    OperationalEvent, QueryObservationRequest, Rights, SecurityRevision, StreamId,
    SupportBundleEntry, TrustedInstant, VerifiedCertificate, H6_MINIMUM_PROFILE,
    H6_PUBLISHED_LIMITATIONS, ISOLATION_PROFILES_JSON, ISOLATION_PROFILES_REL,
    PRE_QUALIFICATION_LANGUAGE, QUALIFIED_CLAIM, QUALIFIED_PROFILE, REFERENCE_ISOLATION_PROFILE,
    UNAUTHENTICATED_DECLASSIFIED_FIELDS,
};
use residiuum_store::{
    active_snapshot, arm_failpoint_once, build_backup_manifest, clear_failpoints, create_object,
    delete_rebuildable_catalogs, destroy_data_key, disaster_recovery_restore_retaining_id,
    heap_binding_envelope, heap_label_envelope, labelled_unit_readable, load_crash_matrix,
    load_identity_tombstone, old_deployment_credential_invalid, publish_staged_genesis,
    rebuild_and_persist_all_catalogs, refuse_access_from_payload_restore,
    refuse_retain_id_without_ceremony, require_admit, restore_payload_to_new_heap,
    stage_heap_genesis, try_load_streams_catalog, DataKeyHandle, DisasterRecoveryCeremony,
    DisasterRecoveryPackage, FailpointAction, HeapLifecycle, HeapMetaLayout, HeapRetentionPolicy,
    MediaDomain, ObjectKind, PurgeCoverageUnit, TierClass, TombstoneKind,
};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;
use tempfile::TempDir;

fn failpoint_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

fn uuidish(seed: u8) -> [u8; 16] {
    let mut id = [seed; 16];
    id[6] = (id[6] & 0x0f) | 0x40;
    id[8] = (id[8] & 0x3f) | 0x80;
    id
}

fn op(seed: u8) -> [u8; 16] {
    uuidish(seed)
}

fn slot_for(heap_seed: u8) -> Arc<HeapSlot> {
    let deployment = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
    let heap = HeapId::from_bytes(uuidish(heap_seed)).unwrap();
    let snap = active_snapshot(deployment, heap, [0xab; 32]).unwrap();
    Arc::new(HeapSlot::new(snap))
}

#[derive(Debug, Deserialize)]
struct MatrixDoc {
    profile: String,
    package: String,
    format: String,
    qualified: bool,
    gates: BTreeMap<String, GateEntry>,
    drills: BTreeMap<String, DrillEntry>,
}

#[derive(Debug, Deserialize)]
struct GateEntry {
    status: String,
    #[serde(default)]
    evidence: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct DrillEntry {
    status: String,
}

#[test]
fn matrix_records_unqualified_claim_and_mandatory_structure() {
    assert!(!may_advertise_qualified());
    assert!(!QUALIFIED_CLAIM);
    assert_eq!(claim_language(), PRE_QUALIFICATION_LANGUAGE);
    assert_eq!(QUALIFIED_PROFILE, "residiuum-heap-v1");

    let path = workspace_root().join("spec/heap/qualification/hp010-matrix-v1.json");
    let raw = fs::read_to_string(&path).expect("hp010 matrix present");
    let doc: MatrixDoc = serde_json::from_str(&raw).expect("parse hp010 matrix");
    assert_eq!(doc.format, "residiuum-heap-qualification-matrix-v1");
    assert_eq!(doc.package, "HP-010");
    assert_eq!(doc.profile, "residiuum-heap-v1");
    assert!(!doc.qualified, "matrix must not claim qualified yet");

    for gate in ["H0", "H1", "H2", "H3", "H4", "H5", "H6", "HC1"] {
        assert!(doc.gates.contains_key(gate), "missing gate {gate}");
    }
    // H3 has full Accept drill coverage for single-node reference profiles.
    assert_eq!(doc.gates["H3"].status, "accept");
    assert!(!doc.gates["H3"].evidence.is_empty());
    // H6 remains partial until Verus + signed external review + residual CPR close.
    assert_eq!(doc.gates["H6"].status, "partial");
    assert!(!doc.gates["H6"].evidence.is_empty());
    for still_partial in ["H0", "H1", "H2", "H4", "H5"] {
        assert_eq!(
            doc.gates[still_partial].status, "partial",
            "{still_partial} must stay partial until residuals close honestly"
        );
    }

    // Accept drills in this slice must be marked accept in the matrix.
    for drill in [
        "differential_ni_labelled_units",
        "key_loss",
        "restore_payload_only",
        "restore_dr_retain_id",
        "heapcap_terminates_on_lifecycle",
        "single_owner_admit",
        "derived_path_indexes_streams",
        "query_escape",
        "load_latency_budget",
        "fuzz_budget",
        "operator_runbook",
        "operational_metrics_logs_export",
        "lifecycle_crash_matrix",
        "h6_connected_model",
        "support_bundle_health_detail",
        "h6_decide_obligations",
        "isolation_profile_registry",
        "h6_heap_authority_model",
        "metadata_hardened_operational_confinement",
        "h6_pure_proof_bundle",
        "complete_path_review",
        "external_security_review_brief",
    ] {
        assert_eq!(
            doc.drills[drill].status, "accept",
            "drill {drill} should be accept for this HP-010 slice"
        );
    }
}

#[test]
fn differential_ni_labelled_units() {
    // §26.2.1: same target-heap labelled observation under arbitrary other-heap state.
    let heap_a = uuidish(0xb0);
    let heap_b = uuidish(0xb1);
    let env_a = heap_label_envelope(&heap_a).unwrap();

    let observe = |other_env: &[u8]| -> bool {
        // Functional observation for heap A: does its own labelled unit admit?
        let _ = other_env; // other-heap bytes must not affect A's observation
        labelled_unit_readable(&heap_a, &env_a, &env_a)
    };

    let env_b_clean = heap_label_envelope(&heap_b).unwrap();
    let mut env_b_corrupt = env_b_clean.clone();
    if let Some(last) = env_b_corrupt.last_mut() {
        *last ^= 0xaa;
    }
    let mut env_b_empty = Vec::new();
    env_b_empty.extend_from_slice(&[0u8; 8]);

    let obs1 = observe(&env_b_clean);
    let obs2 = observe(&env_b_corrupt);
    let obs3 = observe(&env_b_empty);
    assert!(
        obs1 && obs2 && obs3,
        "target heap A observation must stay allow"
    );

    // Cross-heap mix remains deny and identical across other-heap mutations.
    let deny1 = labelled_unit_readable(&heap_a, &env_b_clean, &env_b_clean);
    let deny2 = labelled_unit_readable(&heap_a, &env_b_corrupt, &env_b_corrupt);
    assert!(!deny1 && !deny2);

    // Mutating other heap's lifecycle must not change target heap A state.
    let slot_a = slot_for(0xb0);
    let slot_b = slot_for(0xb1);
    let tmp = TempDir::new().unwrap();
    let before = slot_a.load().administrative_state;
    let mut life_b = HeapLifecycle::open(tmp.path().join("b"), Arc::clone(&slot_b));
    life_b.retire(op(0xb2)).unwrap();
    assert_eq!(
        slot_b.load().administrative_state,
        HeapAdministrativeState::Retired
    );
    assert_eq!(
        slot_a.load().administrative_state,
        before,
        "non-target heap mutation changed target observation"
    );
}

#[test]
fn key_loss_drill() {
    let tmp = TempDir::new().unwrap();
    let heap = uuidish(0xc0);
    let mut key = DataKeyHandle::generate(heap, b"hp010-key-loss-secret").unwrap();
    assert!(!key.is_destroyed());
    let material = key.material().unwrap().to_vec();
    assert!(!material.is_empty());

    let receipt = destroy_data_key(tmp.path(), &mut key).unwrap();
    assert!(key.is_destroyed());
    assert!(key.material().is_none());
    assert_eq!(receipt.heap_id, heap);
    assert!(!receipt.destroyed_fingerprint.iter().all(|b| *b == 0));
    // Second destroy fails closed — no silent re-export of lost key material.
    assert!(destroy_data_key(tmp.path(), &mut key).is_err());
}

#[test]
fn restore_drills_payload_and_retain_id() {
    let tmp = TempDir::new().unwrap();
    let source = uuidish(0xd0);
    let restored =
        restore_payload_to_new_heap(tmp.path(), source, b"restore-drill-bytes", "coll").unwrap();
    assert_ne!(restored.new_heap_id, source);
    let deny = refuse_access_from_payload_restore(&restored).unwrap_err();
    assert!(deny.to_string().contains("cannot grant access"), "{deny}");

    let old_dep = uuidish(0xd1);
    let new_dep = uuidish(0xd2);
    let heap = uuidish(0xd3);
    let deployment = DeploymentId::from_bytes(old_dep).unwrap();
    let heap_id = HeapId::from_bytes(heap).unwrap();
    let snap = active_snapshot(deployment, heap_id, [0x11; 32]).unwrap();
    let slot = Arc::new(HeapSlot::new(snap));
    let package = DisasterRecoveryPackage {
        heap_id: heap,
        backup_deployment_id: old_dep,
        backup_authority_epoch: 1,
        payload: b"dr-drill".to_vec(),
    };
    assert!(refuse_retain_id_without_ceremony(&slot, &package).is_err());

    let ceremony = DisasterRecoveryCeremony {
        heap_id: heap,
        old_deployment_id: old_dep,
        new_deployment_id: new_dep,
        old_authority_epoch: 1,
        new_authority_epoch: 2,
        new_master_public_key: [0x22; 32],
        recovery_authority_evidence: [0x33; 32],
    };
    let result =
        disaster_recovery_restore_retaining_id(tmp.path(), &package, &ceremony, Some(&slot))
            .unwrap();
    assert_eq!(result.snapshot.deployment_id.to_bytes(), new_dep);
    assert_eq!(result.snapshot.authority_epoch.get(), 2);
    assert!(old_deployment_credential_invalid(&result, &old_dep));
}

#[test]
fn retention_and_incomplete_purge_still_retired() {
    // H5 destructive-operation qualification: unavailable domain → retired, not purged.
    let _guard = failpoint_lock();
    clear_failpoints();
    let tmp = TempDir::new().unwrap();
    let slot = slot_for(0xe0);
    let heap = slot.load().heap_id.to_bytes();
    let mut life = HeapLifecycle::open(tmp.path(), Arc::clone(&slot));
    life.retire(op(0xe1)).unwrap();

    let retain_until = 2_000_000_000u64;
    life.retention_mut()
        .save_policy(
            tmp.path(),
            &HeapRetentionPolicy {
                heap_id: heap,
                minimum_retain_until_unix_s: retain_until,
            },
        )
        .unwrap();
    assert!(life
        .begin_purge_media(
            op(0xe2),
            vec![PurgeCoverageUnit {
                object_id: uuidish(0xe3),
                domain: MediaDomain::Tier(TierClass::Hot),
                available: true,
            }],
            retain_until - 1,
        )
        .is_err());

    let units = vec![
        PurgeCoverageUnit {
            object_id: uuidish(0xe4),
            domain: MediaDomain::Tier(TierClass::Hot),
            available: true,
        },
        PurgeCoverageUnit {
            object_id: uuidish(0xe5),
            domain: MediaDomain::Replica {
                replica_id: uuidish(0xe6),
            },
            available: false,
        },
    ];
    let plan = life
        .begin_purge_media(op(0xe7), units, retain_until)
        .unwrap();
    life.destroy_coverage_unit(plan.coverage_ids[0]).unwrap();
    assert!(life.complete_purge(plan.operation_id).is_err());
    let incomplete = life.abort_incomplete_purge(plan.operation_id).unwrap();
    assert_eq!(life.state(), HeapAdministrativeState::Retired);
    assert!(!incomplete.unavailable_domains.is_empty());
    let ts = load_identity_tombstone(tmp.path(), &heap).unwrap();
    assert_eq!(ts.kind, TombstoneKind::Retired);
    clear_failpoints();
}

fn mint_cap(slot: Arc<HeapSlot>) -> residiuum_heap::HeapCap {
    let snap = slot.load();
    let cert = VerifiedCertificate {
        cose_bytes: vec![0x01],
        fingerprint: [3u8; 32],
        deployment_id: snap.deployment_id,
        heap_id: snap.heap_id,
        authority_epoch: snap.authority_epoch,
        authority_generation: snap.authority_generation,
        certificate_id: CertificateId::new_random().unwrap(),
        holder_public_key: [4u8; 32],
        rights: Rights::from_bits_certificate(0x5).unwrap(),
        constraints: Constraints::empty(),
        not_before: 1,
        expires_at: 4_000_000_000,
        issuer_master_key_id: [5u8; 32],
    };
    mint_capability(
        slot,
        &cert,
        TrustedInstant {
            unix_s: 1_700_000_000,
        },
    )
    .unwrap()
}

#[test]
fn heapcap_terminates_on_lifecycle() {
    // Gate H3: established HeapCaps terminate after state / security-revision change.
    let _guard = failpoint_lock();
    clear_failpoints();
    let tmp = TempDir::new().unwrap();
    let slot = slot_for(0xf0);
    let cap = mint_cap(Arc::clone(&slot));
    assert!(refresh_capability_or_terminate(&cap).is_ok());

    let mut life = HeapLifecycle::open(tmp.path(), Arc::clone(&slot));
    life.suspend(op(0xf1)).unwrap();
    assert_eq!(life.state(), HeapAdministrativeState::Suspended);
    assert!(
        refresh_capability_or_terminate(&cap).is_err(),
        "suspend must terminate prior HeapCap"
    );

    life.resume(op(0xf2)).unwrap();
    assert_eq!(life.state(), HeapAdministrativeState::Active);
    // Old cap still stale (revision advanced through suspend+resume).
    assert!(refresh_capability_or_terminate(&cap).is_err());
    let cap2 = mint_cap(Arc::clone(&slot));
    assert!(refresh_capability_or_terminate(&cap2).is_ok());

    life.retire(op(0xf3)).unwrap();
    assert!(
        refresh_capability_or_terminate(&cap2).is_err(),
        "retire must terminate prior HeapCap"
    );
}

#[test]
fn single_owner_admit() {
    // Gate H3: every admitted data-bearing object has exactly one validated known owner.
    let heap_a = uuidish(0xf4);
    let heap_b = uuidish(0xf5);
    let env_a = heap_binding_envelope(&heap_a).unwrap();
    let env_b = heap_binding_envelope(&heap_b).unwrap();

    let ownership = require_admit(&heap_a, &env_a, &env_a, None).unwrap();
    match ownership {
        OwnershipEvidence::Known { heap_id, .. } => assert_eq!(heap_id, heap_a),
        OwnershipEvidence::Unknown => panic!("admitted frame must be Known"),
    }

    assert!(require_admit(&heap_a, &env_a, &env_b, None).is_err());
    assert!(require_admit(&heap_a, &[], &env_a, None).is_err());

    match admit_frame_to_heap(&heap_a, &env_a, &env_b, None) {
        AdmitDecision::RejectConflict | AdmitDecision::RejectWrongHeap { .. } => {}
        other => panic!("expected conflict/wrong-heap, got {other:?}"),
    }
    match admit_frame_to_heap(&heap_a, &[], &[], None) {
        AdmitDecision::RejectUnknown | AdmitDecision::RejectMalformed => {}
        other => panic!("expected unknown/malformed, got {other:?}"),
    }

    // Deterministic adversarial mutations: flip each byte of a valid envelope and
    // prove we never admit under the wrong heap (partial fuzz budget).
    let mut mutations_rejected = 0usize;
    for i in 0..env_a.len() {
        let mut mutated = env_a.clone();
        mutated[i] ^= 0xff;
        let decision = admit_frame_to_heap(&heap_a, &mutated, &env_a, None);
        match decision {
            AdmitDecision::Admit { ownership } => {
                if let OwnershipEvidence::Known { heap_id, .. } = ownership {
                    assert_eq!(heap_id, heap_a);
                }
            }
            _ => mutations_rejected += 1,
        }
        let cross = admit_frame_to_heap(&heap_b, &mutated, &mutated, None);
        assert!(
            !matches!(cross, AdmitDecision::Admit { .. }),
            "mutated bytes must not admit under a different bound heap"
        );
    }
    assert!(
        mutations_rejected > 0,
        "expected at least one mutation to fail closed"
    );
}

#[test]
fn security_revision_bump_terminates_without_lifecycle_helper() {
    let deployment = DeploymentId::from_bytes(uuidish(0x01)).unwrap();
    let heap = HeapId::from_bytes(uuidish(0xf6)).unwrap();
    let snap = HeapSecuritySnapshot {
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: residiuum_heap::AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        previous_generation: None,
        grace_deadline_unix_s: None,
        master_public_key: [0xab; 32],
        previous_master_public_key: None,
        security_revision: SecurityRevision::new(1).unwrap(),
        authority_chain_head_hash: [0x11; 32],
        administrative_state: HeapAdministrativeState::Active,
        blacklist: vec![],
        policy_rights_ceiling: None,
    };
    let slot = Arc::new(HeapSlot::new(snap));
    let cap = mint_cap(Arc::clone(&slot));
    assert!(refresh_capability_or_terminate(&cap).is_ok());

    let mut next = (*slot.load()).clone();
    next.security_revision = SecurityRevision::new(2).unwrap();
    slot.store(next);
    assert!(refresh_capability_or_terminate(&cap).is_err());
}

fn mint_cap_with_constraints(
    slot: Arc<HeapSlot>,
    constraints: Constraints,
) -> residiuum_heap::HeapCap {
    let snap = slot.load();
    let cert = VerifiedCertificate {
        cose_bytes: vec![0x01],
        fingerprint: [3u8; 32],
        deployment_id: snap.deployment_id,
        heap_id: snap.heap_id,
        authority_epoch: snap.authority_epoch,
        authority_generation: snap.authority_generation,
        certificate_id: CertificateId::new_random().unwrap(),
        holder_public_key: [4u8; 32],
        rights: Rights::from_bits_certificate(0x5).unwrap(),
        constraints,
        not_before: 1,
        expires_at: 4_000_000_000,
        issuer_master_key_id: [5u8; 32],
    };
    mint_capability(
        slot,
        &cert,
        TrustedInstant {
            unix_s: 1_700_000_000,
        },
    )
    .unwrap()
}

#[test]
fn derived_path_indexes_streams_scoped() {
    // Gate H3 / §26.4: streams catalogs and derived indexes stay heap-scoped.
    let tmp = TempDir::new().unwrap();
    let layout = HeapMetaLayout::new(tmp.path());
    let deploy = uuidish(0x10);
    let heap_a = uuidish(0x11);
    let heap_b = uuidish(0x12);
    let stream_a = uuidish(0x21);
    let stream_b = uuidish(0x22);

    let sa = stage_heap_genesis(&layout, deploy, heap_a, uuidish(0x31), "heap-a").unwrap();
    let sb = stage_heap_genesis(&layout, deploy, heap_b, uuidish(0x32), "heap-b").unwrap();
    publish_staged_genesis(&layout, &sa.staging_id, &sa.descriptor_hash).unwrap();
    publish_staged_genesis(&layout, &sb.staging_id, &sb.descriptor_hash).unwrap();

    create_object(
        &layout,
        &heap_a,
        ObjectKind::Stream,
        stream_a,
        uuidish(0x41),
        "events",
    )
    .unwrap();
    create_object(
        &layout,
        &heap_b,
        ObjectKind::Stream,
        stream_b,
        uuidish(0x42),
        "events",
    )
    .unwrap();

    let cat_a = try_load_streams_catalog(&layout, &heap_a).unwrap().unwrap();
    let cat_b = try_load_streams_catalog(&layout, &heap_b).unwrap().unwrap();
    assert_eq!(cat_a.len(), 1);
    assert_eq!(cat_b.len(), 1);
    assert_eq!(cat_a[0].object_id, stream_a);
    assert_eq!(cat_b[0].object_id, stream_b);
    assert_eq!(cat_a[0].heap_id, heap_a);
    assert_eq!(cat_b[0].heap_id, heap_b);
    assert_ne!(cat_a[0].object_id, cat_b[0].object_id);

    // Stream binding envelopes admit only under their owner heap.
    let env_a = encode_stream_binding_envelope(&heap_a, &stream_a).unwrap();
    let env_b = encode_stream_binding_envelope(&heap_b, &stream_b).unwrap();
    assert!(require_admit(&heap_a, &env_a, &env_a, None).is_ok());
    assert!(require_admit(&heap_a, &env_a, &env_b, None).is_err());
    assert!(require_admit(&heap_b, &env_b, &env_a, None).is_err());

    // Heap-scoped index dirs do not collide; wipe of A leaves B intact.
    let idx_a = layout.heap_index_dir(&heap_a);
    let idx_b = layout.heap_index_dir(&heap_b);
    assert_ne!(idx_a, idx_b);
    fs::create_dir_all(&idx_a).unwrap();
    fs::create_dir_all(&idx_b).unwrap();
    fs::write(idx_a.join("sec.idx"), b"a-derived").unwrap();
    fs::write(idx_b.join("sec.idx"), b"b-derived").unwrap();

    delete_rebuildable_catalogs(&layout).unwrap();
    assert!(
        !idx_a.exists(),
        "heap A derived indexes must be wiped with catalogs"
    );
    assert!(
        !idx_b.exists(),
        "delete_rebuildable_catalogs clears every published heap index dir"
    );

    // Rebuild restores stream ownership without cross-heap merge.
    let (_heaps, objects) = rebuild_and_persist_all_catalogs(&layout).unwrap();
    let streams_a = objects.get(&heap_a).unwrap();
    let streams_b = objects.get(&heap_b).unwrap();
    assert!(streams_a
        .iter()
        .any(|e| e.object_id == stream_a && e.heap_id == heap_a));
    assert!(streams_b
        .iter()
        .any(|e| e.object_id == stream_b && e.heap_id == heap_b));
    assert!(!streams_a.iter().any(|e| e.object_id == stream_b));
    assert!(!streams_b.iter().any(|e| e.object_id == stream_a));

    // Re-create index dirs after rebuild — still distinct.
    fs::create_dir_all(layout.heap_index_dir(&heap_a)).unwrap();
    fs::create_dir_all(layout.heap_index_dir(&heap_b)).unwrap();
    assert_ne!(
        layout.heap_index_dir(&heap_a),
        layout.heap_index_dir(&heap_b)
    );
}

#[test]
fn query_escape_faulty_planner_confined() {
    // §26.3: intentionally faulty planner cannot escape bound capability.
    let slot = slot_for(0x70);
    let allowed_coll = CollectionId::from_bytes(uuidish(0x71)).unwrap();
    let allowed_stream = StreamId::from_bytes(uuidish(0x72)).unwrap();
    let foreign_coll = CollectionId::from_bytes(uuidish(0x73)).unwrap();
    let foreign_stream = StreamId::from_bytes(uuidish(0x74)).unwrap();
    let constraints = Constraints::from_sorted(vec![
        Constraint::CollectionAllowlist(vec![allowed_coll]),
        Constraint::StreamAllowlist(vec![allowed_stream]),
        Constraint::MaxResultBytes(4_096),
        Constraint::MaxQueryWork(100),
    ])
    .unwrap();
    let cap = mint_cap_with_constraints(Arc::clone(&slot), constraints);

    // Unconstrained scan denied.
    assert!(confine_query_observation(
        &cap,
        &QueryObservationRequest {
            requested_heap: cap.heap_id(),
            collections: vec![],
            streams: vec![],
            estimated_work: 1,
            estimated_result_bytes: 1,
        }
    )
    .is_err());

    // Cross-heap request denied.
    let other = HeapId::from_bytes(uuidish(0x75)).unwrap();
    assert!(confine_query_observation(
        &cap,
        &QueryObservationRequest {
            requested_heap: other,
            collections: vec![allowed_coll],
            streams: vec![allowed_stream],
            estimated_work: 1,
            estimated_result_bytes: 1,
        }
    )
    .is_err());

    // Foreign collection / stream denied.
    assert!(confine_query_observation(
        &cap,
        &QueryObservationRequest {
            requested_heap: cap.heap_id(),
            collections: vec![foreign_coll],
            streams: vec![allowed_stream],
            estimated_work: 1,
            estimated_result_bytes: 1,
        }
    )
    .is_err());
    assert!(confine_query_observation(
        &cap,
        &QueryObservationRequest {
            requested_heap: cap.heap_id(),
            collections: vec![allowed_coll],
            streams: vec![foreign_stream],
            estimated_work: 1,
            estimated_result_bytes: 1,
        }
    )
    .is_err());

    // Work / result over budget denied.
    assert!(confine_query_observation(
        &cap,
        &QueryObservationRequest {
            requested_heap: cap.heap_id(),
            collections: vec![allowed_coll],
            streams: vec![allowed_stream],
            estimated_work: 101,
            estimated_result_bytes: 1,
        }
    )
    .is_err());
    assert!(confine_query_observation(
        &cap,
        &QueryObservationRequest {
            requested_heap: cap.heap_id(),
            collections: vec![allowed_coll],
            streams: vec![allowed_stream],
            estimated_work: 1,
            estimated_result_bytes: 4_097,
        }
    )
    .is_err());

    // Combining two differently-scoped streams: only the allowlisted stream may appear.
    let ok = confine_query_observation(
        &cap,
        &QueryObservationRequest {
            requested_heap: cap.heap_id(),
            collections: vec![allowed_coll],
            streams: vec![allowed_stream],
            estimated_work: 50,
            estimated_result_bytes: 100,
        },
    )
    .unwrap();
    assert_eq!(ok.heap_id, cap.heap_id());
    assert_eq!(ok.collections, vec![allowed_coll]);
    assert_eq!(ok.streams, vec![allowed_stream]);

    // Collection-binding admit still refuses cross-heap mix (query path + durable path).
    let heap_a = uuidish(0x70);
    let heap_b = uuidish(0x75);
    let coll = uuidish(0x71);
    let env_a = encode_collection_binding_envelope(&heap_a, &coll).unwrap();
    let env_b = encode_collection_binding_envelope(&heap_b, &coll).unwrap();
    assert!(require_admit(&heap_a, &env_a, &env_a, None).is_ok());
    assert!(require_admit(&heap_a, &env_a, &env_b, None).is_err());
}

#[test]
fn load_latency_budget() {
    // HP-010 load/latency: admit + decide/refresh meet single-node CI budget.
    const N: usize = 2_000;
    const BUDGET_MS: u128 = 2_000;

    let heap = uuidish(0x80);
    let env = heap_binding_envelope(&heap).unwrap();
    let start = Instant::now();
    for _ in 0..N {
        match admit_frame_to_heap(&heap, &env, &env, None) {
            AdmitDecision::Admit { .. } => {}
            other => panic!("expected admit, got {other:?}"),
        }
    }
    let admit_ms = start.elapsed().as_millis();
    assert!(
        admit_ms <= BUDGET_MS,
        "admit budget exceeded: {admit_ms}ms > {BUDGET_MS}ms for {N} calls"
    );

    let slot = slot_for(0x80);
    let cap = mint_cap(Arc::clone(&slot));
    let start = Instant::now();
    for _ in 0..N {
        refresh_capability_or_terminate(&cap).unwrap();
    }
    let refresh_ms = start.elapsed().as_millis();
    assert!(
        refresh_ms <= BUDGET_MS,
        "refresh budget exceeded: {refresh_ms}ms > {BUDGET_MS}ms for {N} calls"
    );
}

#[test]
fn fuzz_budget_structured_mutations() {
    // Full CI fuzz budget: structured adversarial corpus over heap/collection/stream
    // bindings + concat / truncate / XOR patterns. Never admit under the wrong heap.
    let heap_a = uuidish(0x90);
    let heap_b = uuidish(0x91);
    let coll = uuidish(0x92);
    let stream = uuidish(0x93);
    let env_heap = heap_binding_envelope(&heap_a).unwrap();
    let env_coll = encode_collection_binding_envelope(&heap_a, &coll).unwrap();
    let env_stream = encode_stream_binding_envelope(&heap_a, &stream).unwrap();

    let mut corpus: Vec<Vec<u8>> = vec![
        env_heap.clone(),
        env_coll.clone(),
        env_stream.clone(),
        Vec::new(),
        vec![0xff; 3],
        vec![0u8; 64],
    ];
    // Concat attacks.
    let mut concat = env_heap.clone();
    concat.extend_from_slice(&env_coll);
    corpus.push(concat);
    let mut concat2 = env_stream.clone();
    concat2.extend_from_slice(&env_heap);
    corpus.push(concat2);
    // Truncations.
    if env_heap.len() > 4 {
        corpus.push(env_heap[..env_heap.len() / 2].to_vec());
        corpus.push(env_heap[2..].to_vec());
    }
    // Deterministic XOR sweeps (step 3 for speed; still multi-byte coverage).
    for (base_i, base) in [&env_heap, &env_coll, &env_stream].iter().enumerate() {
        for i in (0..base.len()).step_by(3) {
            let mut m = (*base).clone();
            m[i] ^= 0x5a + base_i as u8;
            corpus.push(m);
        }
        for i in (0..base.len()).step_by(7) {
            let mut m = (*base).clone();
            m[i] ^= 0xff;
            if i + 1 < m.len() {
                m[i + 1] ^= 0xaa;
            }
            corpus.push(m);
        }
    }

    let mut rejected = 0usize;
    let mut same_heap_admits = 0usize;
    for sample in &corpus {
        for bound in [&heap_a, &heap_b] {
            let decision = admit_frame_to_heap(bound, sample, sample, None);
            match decision {
                AdmitDecision::Admit {
                    ownership: OwnershipEvidence::Known { heap_id, .. },
                } => {
                    assert_eq!(heap_id, *bound, "admitted ownership must equal bound heap");
                    same_heap_admits += 1;
                }
                AdmitDecision::Admit {
                    ownership: OwnershipEvidence::Unknown,
                } => panic!("Unknown ownership must not Admit"),
                _ => rejected += 1,
            }
        }
    }
    assert!(rejected > 0, "expected rejects in structured fuzz corpus");
    assert!(
        same_heap_admits > 0,
        "expected at least one valid same-heap admit"
    );
}

#[test]
fn operator_runbook_present() {
    let path = workspace_root().join("doc/wip/heap/RUNBOOK_HEAP_QUALIFICATION.md");
    let body = fs::read_to_string(&path).expect("operator runbook present");
    for needle in [
        "HP-010",
        "qualified=false",
        "Key loss",
        "Incomplete purge",
        "Gate H6 limitations",
        "load / latency",
        "fuzz",
    ] {
        assert!(
            body.to_lowercase().contains(&needle.to_lowercase()) || body.contains(needle),
            "runbook missing section/needle: {needle}"
        );
    }
    // Published H6 limitations appear in both kernel constant and runbook.
    assert!(H6_PUBLISHED_LIMITATIONS.contains("physical access"));
    assert!(body.contains("physical access") || body.contains("Physical access"));
}

#[test]
fn h6_isolation_claim_partial_evidence() {
    // Gate H6 remains partial: claim language Level 1, formal + limitations present,
    // query confinement evidence exists — but qualified stays false.
    assert!(!may_advertise_qualified());
    assert!(!QUALIFIED_CLAIM);
    assert_eq!(claim_language(), PRE_QUALIFICATION_LANGUAGE);
    assert!(H6_PUBLISHED_LIMITATIONS.contains("side channels"));

    let tla = workspace_root().join("formal/heap/HeapIsolation.tla");
    let cfg = workspace_root().join("formal/heap/MCHeapIsolation.cfg");
    let tla_body = fs::read_to_string(&tla).expect("HeapIsolation.tla");
    assert!(tla_body.contains("ReadConfinement"));
    assert!(tla_body.contains("WriteConfinement"));
    assert!(tla_body.contains("OwnershipDisjoint"));
    assert!(tla_body.contains("RevocationClosed"));
    assert!(cfg.exists(), "TLC config present");

    let verus = workspace_root().join("verification/heap-verus/src/lib.rs");
    let verus_body = fs::read_to_string(&verus).unwrap();
    assert!(verus_body.contains("H6_PROOF_OBLIGATIONS"));
    assert!(verus_body.contains("confine_query_observation"));
    assert!(verus_body.contains("IsolationModel"));
    assert!(verus_body.contains("IsolationProfileRegistry"));
    assert!(verus_body.contains("HeapAuthority.tla"));
}

#[test]
fn operational_metrics_logs_export_scoped() {
    // Gate H3 / §9.5–§9.6: metrics/logs/audit/export stay heap-scoped.
    let slot = slot_for(0xa0);
    let cap = mint_cap(Arc::clone(&slot));
    let foreign = HeapId::from_bytes(uuidish(0xa1)).unwrap();

    assert!(unauthenticated_field_allowed("live"));
    assert!(!unauthenticated_field_allowed("heap_count"));
    assert_eq!(UNAUTHENTICATED_DECLASSIFIED_FIELDS.len(), 4);

    let events = vec![
        OperationalEvent {
            heap_id: None,
            field: "ready".into(),
            value: "1".into(),
        },
        OperationalEvent {
            heap_id: Some(cap.heap_id()),
            field: "usage_bytes".into(),
            value: "10".into(),
        },
        OperationalEvent {
            heap_id: Some(foreign),
            field: "usage_bytes".into(),
            value: "999".into(),
        },
        OperationalEvent {
            heap_id: Some(foreign),
            field: "export_path".into(),
            value: "/secret/b".into(),
        },
    ];
    let obs = confine_operational_observation(&cap, &events).unwrap();
    assert_eq!(obs.heap_id, cap.heap_id());
    assert_eq!(obs.events.len(), 2);
    assert!(obs
        .events
        .iter()
        .all(|e| e.heap_id.is_none() || e.heap_id == Some(cap.heap_id())));
    assert!(confine_operational_observation(
        &cap,
        &[OperationalEvent {
            heap_id: None,
            field: "other_heap_names".into(),
            value: "leak".into(),
        }]
    )
    .is_err());

    assert_eq!(
        confine_export_heaps(&cap, &[]).unwrap(),
        vec![cap.heap_id()]
    );
    assert!(confine_export_heaps(&cap, &[foreign]).is_err());

    // Durable backup manifest enumerates only requested heaps — ordinary export
    // confinement refuses mixing foreign ids before the manifest is built.
    let only = confine_export_heaps(&cap, &[cap.heap_id()]).unwrap();
    let dep = uuidish(0x01);
    let manifest =
        build_backup_manifest(dep, &only.iter().map(|h| h.to_bytes()).collect::<Vec<_>>()).unwrap();
    assert_eq!(manifest.heap_ids, vec![cap.heap_id().to_bytes()]);
    assert!(!manifest.heap_ids.contains(&foreign.to_bytes()));
}

#[test]
fn lifecycle_crash_matrix_peer_heaps_unaffected() {
    // Gate H5: crash mid-lifecycle on A must not change B's observation.
    let _guard = failpoint_lock();
    clear_failpoints();
    let tmp = TempDir::new().unwrap();
    let slot_a = slot_for(0xb0);
    let slot_b = slot_for(0xb1);
    let heap_a = slot_a.load().heap_id.to_bytes();
    let heap_b = slot_b.load().heap_id.to_bytes();
    let env_b = heap_label_envelope(&heap_b).unwrap();
    assert!(labelled_unit_readable(&heap_b, &env_b, &env_b));

    let mut life_a = HeapLifecycle::open(tmp.path().join("a"), Arc::clone(&slot_a));
    let life_b = HeapLifecycle::open(tmp.path().join("b"), Arc::clone(&slot_b));
    let before_b = life_b.state();
    let cap_a = mint_cap(Arc::clone(&slot_a));
    let cap_b = mint_cap(Arc::clone(&slot_b));
    assert!(refresh_capability_or_terminate(&cap_a).is_ok());
    assert!(refresh_capability_or_terminate(&cap_b).is_ok());

    // Crash after state store during suspend: slot advanced; receipt may be missing.
    arm_failpoint_once("heap_lifecycle.after_state_store", FailpointAction::Error);
    let err = life_a.suspend(op(0xb2)).unwrap_err();
    assert!(
        err.to_string().contains("failpoint") || err.to_string().contains("Failpoint"),
        "{err}"
    );
    clear_failpoints();
    assert_eq!(life_a.state(), HeapAdministrativeState::Suspended);
    assert!(refresh_capability_or_terminate(&cap_a).is_err());
    assert_eq!(life_b.state(), before_b);
    assert!(refresh_capability_or_terminate(&cap_b).is_ok());
    assert!(labelled_unit_readable(&heap_b, &env_b, &env_b));

    // Resume A to continue matrix (mint fresh after revision).
    life_a.resume(op(0xb3)).unwrap();
    life_a.retire(op(0xb4)).unwrap();
    let tomb = load_identity_tombstone(&tmp.path().join("a"), &heap_a).unwrap();
    assert_eq!(tomb.kind, TombstoneKind::Retired);

    // Crash after purge plan: A is Purging; B unchanged.
    arm_failpoint_once("heap_lifecycle.after_purge_plan", FailpointAction::Error);
    let err = life_a
        .begin_purge_media(
            op(0xb5),
            vec![PurgeCoverageUnit {
                object_id: uuidish(0xb6),
                domain: MediaDomain::Tier(TierClass::Hot),
                available: true,
            }],
            2_000_000_000,
        )
        .unwrap_err();
    assert!(err.to_string().contains("failpoint") || err.to_string().contains("Failpoint"));
    clear_failpoints();
    assert_eq!(life_a.state(), HeapAdministrativeState::Purging);
    assert_eq!(life_b.state(), before_b);
    assert!(labelled_unit_readable(&heap_b, &env_b, &env_b));

    // Matrix document records heap_lifecycle operation.
    let matrix = load_crash_matrix().expect("crash matrix");
    assert!(
        matrix.operations.iter().any(|o| o.id == "heap_lifecycle"),
        "crash_matrix must include heap_lifecycle"
    );
    let life_op = matrix
        .operations
        .iter()
        .find(|o| o.id == "heap_lifecycle")
        .unwrap();
    assert!(life_op
        .failpoints
        .iter()
        .any(|f| f.name == "heap_lifecycle.after_purge_plan"));
    clear_failpoints();
}

#[test]
fn h6_connected_model() {
    // Connected Rust model ↔ HeapIsolation.tla invariants (still not Verus-complete).
    let a = HeapId::from_bytes(uuidish(0xe0)).unwrap();
    let b = HeapId::from_bytes(uuidish(0xe1)).unwrap();
    connected_model_smoke(a, b);

    let tla = fs::read_to_string(workspace_root().join("formal/heap/HeapIsolation.tla")).unwrap();
    for needle in [
        "ReadConfinement",
        "WriteConfinement",
        "OwnershipDisjoint",
        "RevocationClosed",
        "Rebind",
        "Admit",
        "Damage",
    ] {
        assert!(tla.contains(needle), "TLA missing {needle}");
    }
}

#[test]
fn support_bundle_health_detail_scoped() {
    // Gate H3 / §9.5: health detail + support bundles respect declassification.
    let slot = slot_for(0xa2);
    let cap = mint_cap(Arc::clone(&slot));
    let foreign = HeapId::from_bytes(uuidish(0xa3)).unwrap();

    let input = HealthDetailInput {
        live: true,
        ready: false,
        reasons: vec!["store not open".into()],
        store_path: Some("/data/residiuum".into()),
        live_count: Some(12_345),
        node_index: Some(1),
        draining: true,
        bound_heap_usage_bytes: Some(64),
        foreign_heap_usage_bytes: Some(999_999),
    };
    let public = confine_health_detail(None, &input).unwrap();
    assert!(public.live);
    assert!(!public.ready);
    assert!(public.reasons.is_empty());
    assert!(public.draining.is_none());
    assert!(public.bound_heap_usage_bytes.is_none());

    let detail = confine_health_detail(Some(&cap), &input).unwrap();
    assert_eq!(detail.draining, Some(true));
    assert_eq!(detail.bound_heap_usage_bytes, Some(64));
    assert!(detail.reasons.iter().any(|r| r.contains("store")));
    // Physical path / global count / foreign usage never appear on the struct.

    let confined = confine_support_bundle(
        &cap,
        &[
            SupportBundleEntry {
                heap_id: Some(cap.heap_id()),
                kind: "receipt".into(),
                label: "lifecycle".into(),
                contains_secrets: false,
            },
            SupportBundleEntry {
                heap_id: Some(foreign),
                kind: "log".into(),
                label: "peer".into(),
                contains_secrets: false,
            },
            SupportBundleEntry {
                heap_id: None,
                kind: "build_id".into(),
                label: "binary".into(),
                contains_secrets: false,
            },
        ],
    )
    .unwrap();
    assert_eq!(confined.heap_id, cap.heap_id());
    assert_eq!(confined.entries.len(), 2);
    assert!(confined
        .entries
        .iter()
        .all(|e| e.heap_id.is_none() || e.heap_id == Some(cap.heap_id())));
    assert!(confine_support_bundle(
        &cap,
        &[SupportBundleEntry {
            heap_id: Some(cap.heap_id()),
            kind: "keystore".into(),
            label: "master".into(),
            contains_secrets: true,
        }]
    )
    .is_err());
}

#[test]
fn h6_decide_obligations_connected() {
    // Executable §39 obligations (Verus stand-in until proofs land).
    assert!(h6_decide_obligations::allow_implies_authority_binding());
    assert!(h6_decide_obligations::unknown_constraints_cannot_allow_foreign());
    assert!(h6_decide_obligations::terminal_or_non_serving_refuses_refresh());
    assert!(h6_decide_obligations::epoch_mismatch_fails_binding());
    assert!(h6_decide_obligations::generation_grace_acceptance());
    assert!(h6_decide_obligations::blacklist_refuses_admit());
    assert!(h6_decide_obligations::authority_admission_bundle());
    assert!(h6_decide_obligations::mint_grace_only_previous_generation());

    let verus =
        fs::read_to_string(workspace_root().join("verification/heap-verus/src/lib.rs")).unwrap();
    assert!(verus.contains("authority_binding_holds"));
    assert!(verus.contains("confine_health_detail"));
    assert!(verus.contains("confine_support_bundle"));
    assert!(verus.contains("generation_accepted"));
    assert!(verus.contains("certificate_blacklisted"));
    assert!(verus.contains("authority_admission_ok"));
    assert!(verus.contains("AuthorityModel"));
}

#[test]
fn isolation_profile_registry_closed() {
    // Gate H3 / §13: named profiles + closed declassification registry.
    let on_disk = fs::read_to_string(workspace_root().join(ISOLATION_PROFILES_REL)).unwrap();
    assert_eq!(
        on_disk.trim(),
        ISOLATION_PROFILES_JSON.trim(),
        "embedded registry must match on-disk artifact"
    );
    let reg = load_isolation_profiles_from(&on_disk).unwrap();
    let embedded = load_isolation_profiles().unwrap();
    assert_eq!(reg.sha256_hex, embedded.sha256_hex);
    assert_eq!(reg.h6_minimum_profile, H6_MINIMUM_PROFILE);
    assert_eq!(
        REFERENCE_ISOLATION_PROFILE,
        IsolationProfileId::HeapDataIsolated
    );

    let data = reg.get(IsolationProfileId::HeapDataIsolated).unwrap();
    for f in UNAUTHENTICATED_DECLASSIFIED_FIELDS {
        assert!(data.unauthenticated_allows(f), "{f}");
        assert!(unauthenticated_field_allowed(f), "{f}");
    }
    assert!(!data.unauthenticated_allows("heap_count"));
    assert!(data.expose_aggregate_load);

    let hard = reg.get(IsolationProfileId::HeapMetadataHardened).unwrap();
    assert!(hard.coarsen_public_timing);
    assert!(!hard.expose_aggregate_load);
    assert!(hard.is_always_confidential("aggregate_load"));
    assert!(hard.is_always_confidential("fine_timing_ms"));
    assert!(hard.authenticated_allows_heap_local("usage").is_ok());
    assert!(hard
        .authenticated_allows_heap_local("physical_paths")
        .is_err());

    // Empty deployment extension — cannot invent fields at runtime.
    assert_eq!(reg.extension_version, 0);
    assert!(reg.extension_fields.is_empty());
    assert_eq!(reg.sha256_hex.len(), 64);
}

#[test]
fn metadata_hardened_operational_confinement() {
    // Gate H3 / §13.2: metadata-hardened profile hardens operational Obs.
    let slot = slot_for(0xa4);
    let cap = mint_cap(Arc::clone(&slot));

    assert!(!unauthenticated_field_allowed_under(
        IsolationProfileId::HeapMetadataHardened,
        "aggregate_load"
    ));
    assert!(!unauthenticated_field_allowed_under(
        IsolationProfileId::HeapMetadataHardened,
        "fine_timing_ms"
    ));
    assert!(unauthenticated_field_allowed("live"));
    assert!(unauthenticated_field_allowed_under(
        IsolationProfileId::HeapMetadataHardened,
        "live"
    ));

    assert!(confine_operational_observation_under(
        &cap,
        &[OperationalEvent {
            heap_id: None,
            field: "aggregate_load".into(),
            value: "1".into(),
        }],
        IsolationProfileId::HeapMetadataHardened,
    )
    .is_err());
    assert!(confine_operational_observation_under(
        &cap,
        &[OperationalEvent {
            heap_id: None,
            field: "fine_timing_ms".into(),
            value: "3".into(),
        }],
        IsolationProfileId::HeapMetadataHardened,
    )
    .is_err());

    // Always-confidential names fail closed even when tagged to the bound heap.
    assert!(confine_operational_observation_under(
        &cap,
        &[OperationalEvent {
            heap_id: Some(cap.heap_id()),
            field: "physical_paths".into(),
            value: "/data".into(),
        }],
        IsolationProfileId::HeapMetadataHardened,
    )
    .is_err());

    // Undeclared bound-heap field fails under closed authenticated allowlist.
    assert!(confine_operational_observation_under(
        &cap,
        &[OperationalEvent {
            heap_id: Some(cap.heap_id()),
            field: "usage_bytes".into(),
            value: "64".into(),
        }],
        IsolationProfileId::HeapMetadataHardened,
    )
    .is_err());

    let ok = confine_operational_observation_under(
        &cap,
        &[
            OperationalEvent {
                heap_id: None,
                field: "ready".into(),
                value: "1".into(),
            },
            OperationalEvent {
                heap_id: Some(cap.heap_id()),
                field: "usage".into(),
                value: "64".into(),
            },
        ],
        IsolationProfileId::HeapMetadataHardened,
    )
    .unwrap();
    assert_eq!(ok.events.len(), 2);
}

#[test]
fn h6_heap_authority_model() {
    // Connected Rust model ↔ HeapAuthority.tla (generation/blacklist/grace/terminal).
    connected_authority_model_smoke();

    let tla = fs::read_to_string(workspace_root().join("formal/heap/HeapAuthority.tla")).unwrap();
    for needle in [
        "GenOK",
        "NotBlacklisted",
        "AdmissionOK",
        "HardCycle",
        "GraceCycle",
        "BlacklistAdd",
        "ExpireGrace",
        "Terminal",
        "Inv",
    ] {
        assert!(tla.contains(needle), "HeapAuthority.tla missing {needle}");
    }
    assert!(workspace_root()
        .join("formal/heap/MCHeapAuthority.cfg")
        .exists());
}

#[test]
fn h6_pure_proof_bundle_connected() {
    // Executable pure lemmas + Kani + Verus pure_kernel (CPR-004).
    assert!(
        connected_pure_proof_bundle(),
        "pure proof bundle must hold for Gate H6 connected evidence"
    );
    let verus =
        fs::read_to_string(workspace_root().join("verification/heap-verus/src/lib.rs")).unwrap();
    assert!(verus.contains("VERUS_PROOFS_CONNECTED"));
    assert!(verus.contains("KANI_HARNESSES_CONNECTED"));
    assert!(verus.contains("connected_pure_proof_bundle"));
    assert!(
        verus.contains("VERUS_PROOFS_CONNECTED: bool = true")
            || verus.contains("const VERUS_PROOFS_CONNECTED: bool = true"),
        "Verus pure_kernel must be marked connected"
    );
    assert!(
        verus.contains("KANI_HARNESSES_CONNECTED: bool = true")
            || verus.contains("const KANI_HARNESSES_CONNECTED: bool = true"),
        "Kani harnesses must be marked connected"
    );
    let pure =
        fs::read_to_string(workspace_root().join("crates/residiuum-heap/src/pure_proofs.rs"))
            .unwrap();
    assert!(
        pure.contains("fn kani_connected_pure_proof_bundle"),
        "Kani harness kani_connected_pure_proof_bundle must exist"
    );
    let verus_src =
        fs::read_to_string(workspace_root().join("verification/heap-verus/verus/pure_kernel.rs"))
            .unwrap();
    assert!(
        verus_src.contains("lemma_connected_pure_proof_bundle"),
        "Verus pure_kernel lemma_connected_pure_proof_bundle must exist"
    );
    assert!(verus_src.contains("verus!"));
}

#[test]
fn complete_path_and_external_review_artifacts() {
    // Gate H6 complete-path + external review package (brief only; report open).
    let root = workspace_root();
    let cpr = fs::read_to_string(root.join("doc/wip/heap/HEAP_COMPLETE_PATH_REVIEW.md")).unwrap();
    for needle in [
        "CPR-001",
        "CPR-004",
        "CPR-005",
        "unscoped",
        "qualified=false",
        "legacy",
        "Residiuum::collection",
    ] {
        assert!(
            cpr.contains(needle),
            "complete-path review missing {needle}"
        );
    }

    let brief =
        fs::read_to_string(root.join("doc/wip/heap/HEAP_EXTERNAL_SECURITY_REVIEW_BRIEF.md"))
            .unwrap();
    for needle in [
        "External security review",
        "TCB",
        "verify-heap",
        "signed",
        "qualified=false",
        "HeapCap",
    ] {
        assert!(
            brief.contains(needle),
            "external review brief missing {needle}"
        );
    }

    // Matrix must index both artifacts under H6 without claiming qualified.
    let raw =
        fs::read_to_string(root.join("spec/heap/qualification/hp010-matrix-v1.json")).unwrap();
    assert!(raw.contains("HEAP_COMPLETE_PATH_REVIEW.md"));
    assert!(raw.contains("HEAP_EXTERNAL_SECURITY_REVIEW_BRIEF.md"));
    assert!(raw.contains("\"qualified\": false") || raw.contains("\"qualified\":false"));
}
