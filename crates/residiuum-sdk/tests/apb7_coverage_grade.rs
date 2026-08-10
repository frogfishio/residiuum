//! APB-7 T9: QueryPage complete-by-default coverage grade.
//!
//! Holey-host cases go through **QVM1 product path** (`QueryBytecodeV1::from_core_plan` →
//! `execute_bytecode`) — sole public Core execute path (Q0.A5; RQB1 not public).

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    execute_bytecode, CollectionBindings, CoveragePolicy, ErrorCode, HeapClient, HostCapabilities,
    OrderDir, Parameters, PlanBuilder, QueryBytecodeV1, QueryRunOptions, ResidiuumDeployment,
};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::Arc;
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
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-apb7-t9").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    (dir, HeapClient::from(deployment.open_heap(cap)))
}

/// Host that lists keys but omits some bodies → known holes.
struct HoleyHost {
    keys: Vec<String>,
    absent: BTreeMap<String, JsonValue>,
    present: BTreeMap<String, JsonValue>,
}

impl HostCapabilities for HoleyHost {
    fn list_keys(
        &mut self,
        _collection_id: residiuum_heap::CollectionId,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<Vec<String>, residiuum_sdk::Error> {
        let mut out: Vec<String> = self
            .keys
            .iter()
            .filter(|k| after_key.map(|a| k.as_str() > a).unwrap_or(true))
            .cloned()
            .collect();
        if let Some(n) = limit {
            out.truncate(n);
        }
        Ok(out)
    }

    fn get_json(
        &mut self,
        _collection_id: residiuum_heap::CollectionId,
        key: &str,
    ) -> Result<Option<JsonValue>, residiuum_sdk::Error> {
        if self.absent.contains_key(key) {
            return Ok(None);
        }
        Ok(self.present.get(key).cloned())
    }
}

#[test]
fn healthy_query_reports_complete_coverage_grade() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    col.put("a", &serde_json::json!({"n": 1})).unwrap();
    col.put("b", &serde_json::json!({"n": 2})).unwrap();

    let page = col
        .rql(
            "from orders",
            &Parameters::default(),
            QueryRunOptions::default(),
        )
        .expect("rql");
    assert!(page.coverage.complete);
    assert_eq!(page.coverage.mode, CoveragePolicy::Complete);
    assert_eq!(page.coverage.hole_count, 0);
    assert!(
        page.coverage.examined_documents >= 2,
        "examined_documents={}",
        page.coverage.examined_documents
    );
    assert!(page.known_holes.is_empty());
    assert_eq!(page.rows.len(), 2);
}

fn uuidish(seed: u8) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0] = seed;
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    b
}

#[test]
fn complete_policy_fails_closed_on_holes() {
    let mut host = HoleyHost {
        keys: vec!["a".into(), "b".into(), "c".into()],
        absent: BTreeMap::from([("b".into(), JsonValue::Null)]),
        present: BTreeMap::from([
            ("a".into(), serde_json::json!({"n": 1})),
            ("c".into(), serde_json::json!({"n": 3})),
        ]),
    };
    let heap = HeapId::from_bytes(uuidish(1)).expect("heap");
    let cid = residiuum_heap::CollectionId::from_bytes(uuidish(2)).expect("cid");
    let mut bindings = CollectionBindings::default();
    bindings.bind("orders", cid);
    let plan = PlanBuilder::from_source("orders")
        .compile(&bindings)
        .unwrap();
    assert_eq!(plan.coverage, CoveragePolicy::Complete);
    let bc = QueryBytecodeV1::from_core_plan(plan, None).expect("encode qvm");

    let err = execute_bytecode(
        &mut host,
        &bc,
        &BTreeMap::new(),
        &QueryRunOptions::default(),
        heap,
        cid,
    )
    .unwrap_err();
    assert_eq!(err.code(), ErrorCode::CoverageIncomplete);
    assert!(
        err.to_string().contains("hole") || err.to_string().contains("complete"),
        "{err}"
    );
}

#[test]
fn incomplete_allowed_returns_page_with_hole_evidence() {
    let mut host = HoleyHost {
        keys: vec!["a".into(), "b".into(), "c".into()],
        absent: BTreeMap::from([("b".into(), JsonValue::Null)]),
        present: BTreeMap::from([
            ("a".into(), serde_json::json!({"n": 1})),
            ("c".into(), serde_json::json!({"n": 3})),
        ]),
    };
    let heap = HeapId::from_bytes(uuidish(3)).expect("heap");
    let cid = residiuum_heap::CollectionId::from_bytes(uuidish(4)).expect("cid");
    let mut bindings = CollectionBindings::default();
    bindings.bind("orders", cid);
    let plan = PlanBuilder::from_source("orders")
        .coverage(CoveragePolicy::IncompleteAllowed)
        .compile(&bindings)
        .unwrap();
    let bc = QueryBytecodeV1::from_core_plan(plan, None).expect("encode qvm");

    let page = execute_bytecode(
        &mut host,
        &bc,
        &BTreeMap::new(),
        &QueryRunOptions {
            coverage: CoveragePolicy::IncompleteAllowed,
            ..Default::default()
        },
        heap,
        cid,
    )
    .expect("incomplete allowed");
    assert!(!page.coverage.complete);
    assert_eq!(page.coverage.mode, CoveragePolicy::IncompleteAllowed);
    assert_eq!(page.coverage.hole_count, 1);
    assert_eq!(page.known_holes.len(), 1);
    assert_eq!(page.known_holes[0].code, "key_listed_absent");
    assert_eq!(page.known_holes[0].key.as_deref(), Some("b"));
    let keys: Vec<_> = page.rows.iter().map(|r| r.key.clone()).collect();
    assert_eq!(keys, vec!["a".to_string(), "c".to_string()]);
}

#[test]
fn builder_incomplete_allowed_path() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    col.put("k", &serde_json::json!({"v": 1})).unwrap();
    let page = col
        .query()
        .coverage(CoveragePolicy::IncompleteAllowed)
        .run(
            &Parameters::default(),
            QueryRunOptions {
                coverage: CoveragePolicy::IncompleteAllowed,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(page.coverage.complete);
    assert_eq!(page.coverage.hole_count, 0);
    let _ = OrderDir::Asc;
}
