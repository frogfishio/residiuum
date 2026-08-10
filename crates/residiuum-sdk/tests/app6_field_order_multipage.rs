//! APP-6 T3: multipage field-order continuation via last_sort_tuple.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{HeapClient, OrderDir, Parameters, QueryRunOptions, ResidiuumDeployment};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
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
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-app6-t3").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let cap = mint_cap_for(heap_id, dep_id);
    (dir, HeapClient::from(deployment.open_heap(cap)))
}

#[test]
fn multipage_field_order_desc_matches_oracle() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    // Keys intentionally reverse of score order.
    col.put("z", &serde_json::json!({"score": 1, "name": "a"}))
        .unwrap();
    col.put("y", &serde_json::json!({"score": 2, "name": "b"}))
        .unwrap();
    col.put("x", &serde_json::json!({"score": 3, "name": "c"}))
        .unwrap();
    col.put("w", &serde_json::json!({"score": 4, "name": "d"}))
        .unwrap();
    col.put("v", &serde_json::json!({"score": 5, "name": "e"}))
        .unwrap();

    // Oracle: score desc → keys v,w,x,y,z
    let mut oracle: Vec<String> = Vec::new();
    let mut rows: Vec<(String, serde_json::Value)> = Vec::new();
    for k in col.list_keys(Some(100), None).unwrap() {
        if let Some(v) = col.get(&k).unwrap() {
            rows.push((k, v));
        }
    }
    rows.sort_by(|(ka, va), (kb, vb)| {
        let sa = va["score"].as_i64().unwrap_or(0);
        let sb = vb["score"].as_i64().unwrap_or(0);
        sb.cmp(&sa).then_with(|| ka.cmp(kb))
    });
    oracle.extend(rows.into_iter().map(|(k, _)| k));
    assert_eq!(
        oracle,
        vec![
            "v".to_string(),
            "w".to_string(),
            "x".to_string(),
            "y".to_string(),
            "z".to_string()
        ]
    );

    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(2);

    let p1 = col
        .query()
        .order_by("score", OrderDir::Desc)
        .unwrap()
        .run(&Parameters::default(), opts.clone())
        .expect("page1");
    let k1: Vec<_> = p1.rows.iter().map(|r| r.key.clone()).collect();
    assert_eq!(k1, oracle[0..2]);
    assert!(!p1.exhausted);
    let cont = p1.next.expect("continuation after page1");

    opts.after = Some(cont);
    let p2 = col
        .query()
        .order_by("score", OrderDir::Desc)
        .unwrap()
        .run(&Parameters::default(), opts.clone())
        .expect("page2");
    let k2: Vec<_> = p2.rows.iter().map(|r| r.key.clone()).collect();
    assert_eq!(
        k2,
        oracle[2..4],
        "page2 must continue field order, not key order"
    );
    assert!(!p2.exhausted);
    let cont2 = p2.next.expect("continuation after page2");

    opts.after = Some(cont2);
    let p3 = col
        .query()
        .order_by("score", OrderDir::Desc)
        .unwrap()
        .run(&Parameters::default(), opts)
        .expect("page3");
    let k3: Vec<_> = p3.rows.iter().map(|r| r.key.clone()).collect();
    assert_eq!(k3, oracle[4..5]);
    assert!(p3.exhausted);
    assert!(p3.next.is_none());

    // Concatenated pages == full oracle.
    let mut all = k1;
    all.extend(k2);
    all.extend(k3);
    assert_eq!(all, oracle);
}

#[test]
fn multipage_field_order_rql_and_builder_agree() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("items").unwrap().collection;
    for (k, n) in [("a", 10), ("b", 30), ("c", 20), ("d", 40)] {
        col.put(k, &serde_json::json!({"n": n})).unwrap();
    }

    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(2);
    let b1 = col
        .query()
        .order_by("n", OrderDir::Asc)
        .unwrap()
        .run(&Parameters::default(), opts.clone())
        .unwrap();
    let r1 = col
        .rql(
            "from items order by n asc",
            &Parameters::default(),
            opts.clone(),
        )
        .unwrap();
    assert_eq!(
        b1.rows.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
        r1.rows.iter().map(|r| r.key.clone()).collect::<Vec<_>>()
    );
    assert_eq!(b1.plan_hash, r1.plan_hash);

    opts.after = b1.next.clone();
    let b2 = col
        .query()
        .order_by("n", OrderDir::Asc)
        .unwrap()
        .run(&Parameters::default(), opts.clone())
        .unwrap();
    opts.after = r1.next;
    let r2 = col
        .rql("from items order by n asc", &Parameters::default(), opts)
        .unwrap();
    assert_eq!(
        b2.rows.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
        r2.rows.iter().map(|r| r.key.clone()).collect::<Vec<_>>()
    );
    // Asc by n: a(10), c(20), b(30), d(40)
    assert_eq!(
        b1.rows.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
        vec!["a".to_string(), "c".to_string()]
    );
    assert_eq!(
        b2.rows.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
        vec!["b".to_string(), "d".to_string()]
    );
}

#[test]
fn multipage_key_order_still_streams() {
    let (_dir, mut client) = open_bound_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..5 {
        col.put(&format!("k{i}"), &serde_json::json!({"i": i}))
            .unwrap();
    }
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(2);
    let p1 = col
        .rql("from docs", &Parameters::default(), opts.clone())
        .unwrap();
    assert_eq!(p1.rows.len(), 2);
    assert!(!p1.exhausted);
    opts.after = p1.next;
    let p2 = col.rql("from docs", &Parameters::default(), opts).unwrap();
    assert_eq!(p2.rows.len(), 2);
    // Keys remain lexicographic stream.
    assert!(p1.rows[0].key < p1.rows[1].key);
    assert!(p1.rows[1].key < p2.rows[0].key);
}
