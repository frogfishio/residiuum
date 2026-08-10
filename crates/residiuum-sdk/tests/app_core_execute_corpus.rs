//! Phase 1 Core compile→execute corpus (RQL PATH T1).
//!
//! Seeds a quiet collection, runs Application Core RQL pages, and reconciles
//! keys against an independent `list_keys`+`get` oracle. Complements APP-5
//! compile-only corpus (`rql_app_core_corpus_v1.json`) with execution oracles.
//!
//! Not package accept — APB-7/APP-6/APP-7 remain principal-gated.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    compile_app_core, CollectionBindings, ErrorCode, HeapClient, Parameters, QueryRunOptions,
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

fn drain_rql(
    col: &mut residiuum_sdk::CollectionClient,
    source: &str,
    params: &Parameters,
    page_size: u32,
) -> Vec<String> {
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(page_size);
    let mut all = Vec::new();
    let mut pages = 0u32;
    loop {
        pages += 1;
        assert!(pages < 100, "execute corpus: runaway pagination");
        let page = col
            .rql(source, params, opts.clone())
            .unwrap_or_else(|e| panic!("rql page: {e:?}"));
        assert!(
            page.coverage.complete,
            "execute corpus expects complete coverage"
        );
        for r in &page.rows {
            all.push(r.key.clone());
        }
        if page.exhausted {
            assert!(page.next.is_none());
            break;
        }
        opts.after = page.next;
    }
    all
}

fn oracle_keys(
    col: &mut residiuum_sdk::CollectionClient,
    pred: impl Fn(&serde_json::Value) -> bool,
) -> Vec<String> {
    let mut out = Vec::new();
    for k in col.list_keys(Some(1000), None).expect("list_keys") {
        if let Some(v) = col.get(&k).expect("get") {
            if pred(&v) {
                out.push(k);
            }
        }
    }
    out
}

fn seed_orders(col: &mut residiuum_sdk::CollectionClient) {
    let rows = [
        (
            "o1",
            serde_json::json!({
                "id": "o1",
                "status": "open",
                "amount": 25,
                "region": "us",
                "sku": "AB-1",
                "notes": "rush order",
                "tags": ["a"],
                "customer": {"id": "c1"},
                "score": 10
            }),
        ),
        (
            "o2",
            serde_json::json!({
                "id": "o2",
                "status": "cancelled",
                "amount": 5,
                "region": "eu",
                "sku": "ZZ-9",
                "notes": "hold",
                "customer": {"id": "c2"},
                "score": 30
            }),
        ),
        (
            "o3",
            serde_json::json!({
                "id": "o3",
                "status": "open",
                "amount": 80,
                "region": "us",
                "sku": "AB-2",
                "notes": "rush + gift",
                "tags": ["b"],
                "customer": {"id": "c3"},
                "score": 20
            }),
        ),
        (
            "o4",
            serde_json::json!({
                "id": "o4",
                "status": "closed",
                "amount": 150,
                "region": "apac",
                "sku": "CD-1",
                "notes": "standard",
                "deleted_at": "2020-01-01",
                "score": 5
            }),
        ),
        (
            "o5",
            serde_json::json!({
                "id": "o5",
                "status": "open",
                "amount": 40,
                "region": "eu",
                "sku": "AB-3",
                "notes": "standard delivery",
                "tags": ["c"],
                "customer": {"id": "c5"},
                "score": 40
            }),
        ),
    ];
    for (k, v) in rows {
        col.put(k, &v).unwrap_or_else(|e| panic!("put({k}): {e:?}"));
    }
}

#[test]
fn core_execute_corpus_compile_execute_oracle() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-core-exec").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    let mut client = HeapClient::from(deployment.open_heap(cap));
    let mut col = client
        .create_collection("orders_core_exec")
        .expect("create_collection")
        .collection;
    seed_orders(&mut col);

    let name = col.name().to_string();
    let mut bindings = CollectionBindings::default();
    bindings.bind(&name, col.id());

    // 1) Compare-range multipage vs oracle.
    {
        let src = format!("from {name} where amount >= 10 and amount < 100 page size 2");
        let compiled = compile_app_core(&src, &bindings).expect("compile range");
        assert_eq!(compiled.plan.page_size, 2);
        let got = drain_rql(&mut col, &src, &Parameters::default(), 2);
        let mut oracle = oracle_keys(&mut col, |v| {
            let a = v["amount"].as_i64().unwrap_or(0);
            (10..100).contains(&a)
        });
        oracle.sort();
        let mut got_sorted = got.clone();
        got_sorted.sort();
        assert_eq!(got_sorted, oracle, "range filter keys");
        assert_eq!(got.len(), 3, "o1,o3,o5 in range");
    }

    // 2) missing + present.
    {
        let src =
            format!("from {name} where missing(deleted_at) and present(customer.id) page size 8");
        let got = drain_rql(&mut col, &src, &Parameters::default(), 8);
        let mut oracle = oracle_keys(&mut col, |v| {
            v.get("deleted_at").is_none() && v.pointer("/customer/id").is_some()
        });
        oracle.sort();
        let mut got_sorted = got.clone();
        got_sorted.sort();
        assert_eq!(got_sorted, oracle);
        assert!(!got.iter().any(|k| k == "o4"));
    }

    // 3) contains + starts_with.
    {
        let src = format!(
            "from {name} where contains(notes, \"rush\") and starts_with(sku, \"AB\") page size 4"
        );
        let got = drain_rql(&mut col, &src, &Parameters::default(), 4);
        let mut oracle = oracle_keys(&mut col, |v| {
            v["notes"].as_str().is_some_and(|n| n.contains("rush"))
                && v["sku"].as_str().is_some_and(|s| s.starts_with("AB"))
        });
        oracle.sort();
        let mut got_sorted = got.clone();
        got_sorted.sort();
        assert_eq!(got_sorted, oracle);
        assert_eq!(got.len(), 2, "o1 and o3");
    }

    // 4) neq / or.
    {
        let src =
            format!("from {name} where status != \"cancelled\" or region = \"us\" page size 3");
        let got = drain_rql(&mut col, &src, &Parameters::default(), 3);
        let mut oracle = oracle_keys(&mut col, |v| {
            v["status"] != "cancelled" || v["region"] == "us"
        });
        oracle.sort();
        let mut got_sorted = got.clone();
        got_sorted.sort();
        assert_eq!(got_sorted, oracle);
    }

    // 5) Field order multipage vs independent score oracle.
    {
        let src = format!("from {name} order by score desc page size 2");
        let got = drain_rql(&mut col, &src, &Parameters::default(), 2);
        let mut rows: Vec<(String, i64)> = Vec::new();
        for k in col.list_keys(Some(1000), None).unwrap() {
            if let Some(v) = col.get(&k).unwrap() {
                rows.push((k, v["score"].as_i64().unwrap_or(0)));
            }
        }
        rows.sort_by(|(ka, sa), (kb, sb)| sb.cmp(sa).then_with(|| ka.cmp(kb)));
        let oracle: Vec<String> = rows.into_iter().map(|(k, _)| k).collect();
        assert_eq!(got, oracle, "score desc multipage");
    }

    // 6) explain surface: plan hash only (no row claim).
    {
        let src = format!("from {name} where status = \"open\" page size 10");
        let compiled = compile_app_core(&src, &bindings).unwrap();
        let expl = col
            .explain_rql(&src, &Parameters::default(), QueryRunOptions::default())
            .expect("explain");
        assert_eq!(expl.plan_hash, compiled.plan.plan_hash());
    }

    // 7) Budget fail-closed (max_documents).
    {
        let src = format!("from {name}");
        let mut opts = QueryRunOptions::default();
        opts.budget = Some(residiuum_sdk::AppQueryBudget {
            max_documents: Some(2),
            max_bytes: None,
            max_result_bytes: None,
        });
        let err = col
            .rql(&src, &Parameters::default(), opts)
            .expect_err("budget must fail closed");
        assert_eq!(err.code(), ErrorCode::ResourceLimit);
    }
}
