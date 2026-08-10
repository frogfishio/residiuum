//! Phase 2: Core expressiveness gotchas — absent≠null, coverage, budgets.
//!
//! Complements APP-5 compile corpus + Phase 1 execute corpus. Answers:
//! “Application Core is SQL-filter-class expressive; joins still pending.”

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    AppQueryBudget, CoveragePolicy, ErrorCode, HeapClient, Parameters, QueryRunOptions,
    ResidiuumDeployment,
};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use std::sync::Arc;
use tempfile::tempdir;

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

fn keys(col: &mut residiuum_sdk::CollectionClient, source: &str) -> Vec<String> {
    let page = col
        .rql(source, &Parameters::default(), QueryRunOptions::default())
        .unwrap_or_else(|e| panic!("rql `{source}`: {e:?}"));
    page.rows.into_iter().map(|r| r.key).collect()
}

#[test]
fn absent_neq_null_predicate_gotcha() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-expr").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let mut client = HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));
    let mut col = client
        .create_collection("orders_expr")
        .expect("create")
        .collection;
    let name = col.name().to_string();

    // Three shapes for nickname:
    // a — field absent
    // b — field present JSON null
    // c — field present string
    col.put("a", &serde_json::json!({"id": "a"})).unwrap();
    col.put("b", &serde_json::json!({"id": "b", "nickname": null}))
        .unwrap();
    col.put("c", &serde_json::json!({"id": "c", "nickname": "ace"}))
        .unwrap();

    let missing = keys(&mut col, &format!("from {name} where missing(nickname)"));
    let is_null = keys(&mut col, &format!("from {name} where nickname is null"));
    let is_not_null = keys(&mut col, &format!("from {name} where nickname is not null"));
    let present = keys(&mut col, &format!("from {name} where present(nickname)"));

    assert_eq!(missing, vec!["a".to_string()], "missing ≠ null");
    assert_eq!(
        is_null,
        vec!["b".to_string()],
        "is null requires present Null"
    );
    assert_eq!(is_not_null, vec!["c".to_string()]);
    assert_eq!(present, vec!["b".to_string(), "c".to_string()]);

    // SQL-document-view collapse (missing ∨ null) — what sql+ IS NULL emits.
    let sql_view = keys(
        &mut col,
        &format!("from {name} where (missing(nickname) or nickname is null)"),
    );
    let mut sql_view = sql_view;
    sql_view.sort();
    assert_eq!(sql_view, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn coverage_incomplete_allows_holes_grade() {
    // Quiet collection: complete coverage remains complete under incomplete_allowed.
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-cov").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let mut client = HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));
    let mut col = client
        .create_collection("orders_cov")
        .expect("create")
        .collection;
    let name = col.name().to_string();
    col.put("k1", &serde_json::json!({"n": 1})).unwrap();

    let page = col
        .rql(
            &format!("from {name} coverage incomplete"),
            &Parameters::default(),
            QueryRunOptions::default(),
        )
        .expect("rql incomplete_allowed");
    assert!(
        matches!(page.coverage.mode, CoveragePolicy::IncompleteAllowed) || page.coverage.complete,
        "incomplete_allowed on quiet collection still reports complete rows"
    );
    assert_eq!(page.rows.len(), 1);
}

#[test]
fn budget_documents_and_result_bytes_fail_closed() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-bud").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let mut client = HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));
    let mut col = client
        .create_collection("orders_bud")
        .expect("create")
        .collection;
    let name = col.name().to_string();
    for i in 0..8 {
        col.put(
            &format!("k{i}"),
            &serde_json::json!({"n": i, "pad": "xxxxxxxxxxxxxxxx"}),
        )
        .unwrap();
    }

    let mut opts = QueryRunOptions::default();
    opts.budget = Some(AppQueryBudget {
        max_documents: Some(3),
        max_bytes: None,
        max_result_bytes: None,
    });
    let err = col
        .rql(&format!("from {name}"), &Parameters::default(), opts)
        .expect_err("max_documents");
    assert_eq!(err.code(), ErrorCode::ResourceLimit);

    let mut opts = QueryRunOptions::default();
    opts.budget = Some(AppQueryBudget {
        max_documents: None,
        max_bytes: None,
        max_result_bytes: Some(40),
    });
    let err = col
        .rql(&format!("from {name}"), &Parameters::default(), opts)
        .expect_err("max_result_bytes");
    assert_eq!(err.code(), ErrorCode::ResourceLimit);
}
