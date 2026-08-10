//! APB-6 T3: retention budget + remote pin residual + multipage under pin.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    ErrorCode, FrontierDrift, HeapClient, Parameters, PinCapability, QueryRunOptions,
    ReadViewOptions, ReadViewRetentionBudget, ResidiuumDeployment, READ_VIEW_PROFILE,
};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, TempDir};

fn mint_cap_for(heap: HeapId, deployment: DeploymentId) -> residiuum_heap::HeapCap {
    let snap = HeapSecuritySnapshot {
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        previous_generation: None,
        grace_deadline_unix_s: None,
        master_public_key: [7u8; 32],
        previous_master_public_key: None,
        security_revision: SecurityRevision::new(1).unwrap(),
        authority_chain_head_hash: [9u8; 32],
        administrative_state: HeapAdministrativeState::Active,
        blacklist: vec![],
        policy_rights_ceiling: None,
    };
    let slot = Arc::new(HeapSlot::new(snap));
    let cert = VerifiedCertificate {
        cose_bytes: vec![0x01],
        fingerprint: [3u8; 32],
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        certificate_id: CertificateId::new_random().unwrap(),
        holder_public_key: [4u8; 32],
        rights: Rights::from_bits_certificate(0x0d).unwrap(),
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

fn uuid() -> [u8; 16] {
    *residiuum_heap::CollectionId::new_random()
        .unwrap()
        .as_bytes()
}

fn open_bound_client() -> (TempDir, HeapClient) {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-apb6-t3").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    (dir, HeapClient::from(deployment.open_heap(cap)))
}

#[test]
fn embedded_pin_capability_and_profile() {
    let (_dir, mut client) = open_bound_client();
    let _ = client.create_collection("orders").unwrap();
    let view = client
        .read_view(ReadViewOptions {
            max_age: Some(Duration::from_secs(300)),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(view.pin_capability(), PinCapability::SegmentFingerprint);
    assert!(view.is_observation_pinned());
    assert_eq!(view.info().semantic_versions.read_view, READ_VIEW_PROFILE);
    assert_eq!(view.check_drift().unwrap(), FrontierDrift::Stable);
}

#[test]
fn retention_max_hold_tightens_expiry() {
    let (_dir, mut client) = open_bound_client();
    // max_age long, max_hold zero → already expired after open.
    let view = client
        .read_view(ReadViewOptions {
            max_age: Some(Duration::from_secs(3600)),
            retention_budget: Some(ReadViewRetentionBudget {
                max_pinned_documents: None,
                max_hold: Some(Duration::from_secs(0)),
            }),
            ..Default::default()
        })
        .unwrap();
    // expires_at == opened_at; is_expired_at uses now > exp, so same-second may still be usable.
    // Force hold check via note after sleeping isn't reliable; instead verify expires equals open.
    let info = view.info();
    assert_eq!(
        info.expires_at_unix,
        Some(info.opened_at_unix),
        "max_hold=0 must win over long max_age"
    );
}

#[test]
fn retention_max_pinned_documents_fail_closed() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    for i in 0..5 {
        col.put(&format!("k{i}"), &serde_json::json!({"i": i}))
            .unwrap();
    }
    drop(col);

    let mut view = client
        .read_view(ReadViewOptions {
            max_age: Some(Duration::from_secs(600)),
            retention_budget: Some(ReadViewRetentionBudget {
                // Cap below a single multipage-sized examination.
                max_pinned_documents: Some(1),
                max_hold: None,
            }),
            ..Default::default()
        })
        .unwrap();
    let mut bound = view
        .open_collection(&mut client, "orders")
        .expect("open under pin");

    // page_size 2 examines ≥2 documents → exceeds max_pinned_documents=1 after page.
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(2);
    let err = bound
        .rql("from orders", &Parameters::default(), opts)
        .unwrap_err();
    assert_eq!(
        err.code(),
        ErrorCode::ResourceLimit,
        "expected retention budget fail, got {err}"
    );
    assert!(
        err.to_string().contains("max_pinned_documents") || err.to_string().contains("retention"),
        "{err}"
    );
}

#[test]
fn multipage_under_pin_rechecks_and_accounts() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    for i in 0..6 {
        col.put(&format!("k{i}"), &serde_json::json!({"i": i}))
            .unwrap();
    }
    drop(col);

    let mut view = client
        .read_view(ReadViewOptions {
            max_age: Some(Duration::from_secs(600)),
            retention_budget: Some(ReadViewRetentionBudget {
                max_pinned_documents: Some(100),
                max_hold: Some(Duration::from_secs(600)),
            }),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(view.retention_documents_examined(), 0);

    let mut bound = view.open_collection(&mut client, "orders").unwrap();
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(2);
    let mut keys = Vec::new();
    loop {
        let page = bound
            .rql("from orders", &Parameters::default(), opts.clone())
            .expect("view multipage");
        assert!(page.coverage.complete);
        for r in &page.rows {
            keys.push(r.key.clone());
        }
        if page.exhausted {
            break;
        }
        opts.after = page.next;
    }
    assert_eq!(keys.len(), 6);
    // Drop bound to reborrow view for examined count.
    drop(bound);
    assert!(
        view.retention_documents_examined() >= 6,
        "examined={}",
        view.retention_documents_examined()
    );
    assert_eq!(view.check_drift().unwrap(), FrontierDrift::Stable);
}
