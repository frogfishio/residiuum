//! RQL-Q3.4 — page-concat metamorphic law (APP-6 continuation).
//!
//! Authority: `RQL_QUERY_QUALIFICATION_PROGRAM.md` §6.3
//!   page_1 ++ page_2 ++ … = unpaged(Q)
//!
//! Residual closed by this labor (product run-option `after` continuation):
//! multipage `CollectionClient::rql` with authenticated continuation equals a
//! single large page (keys, values, order when declared, coverage complete).
//!
//! Source `after $cursor` is also drained with a host-issued opaque token and
//! must reconstruct the same stream without changing canonical plan identity.
//!
//! Not Gate-1; not RQL-Q3 package accept.

#[path = "common/rql_evidence_write.rs"]
mod rql_evidence_write;

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{Parameters, QueryPage, QueryRunOptions, ResidiuumDeployment};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
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

fn open_client() -> (tempfile::TempDir, residiuum_sdk::HeapClient) {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-q3-4").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    (
        dir,
        residiuum_sdk::HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id))),
    )
}

#[derive(Clone, Debug, PartialEq)]
struct Row {
    key: String,
    value: Value,
}

fn page_rows(page: &QueryPage) -> Vec<Row> {
    page.rows
        .iter()
        .map(|r| Row {
            key: r.key.clone(),
            value: r.value.clone(),
        })
        .collect()
}

fn concat_pages(pages: &[QueryPage]) -> Vec<Row> {
    pages.iter().flat_map(page_rows).collect()
}

/// Drain multipage rql until exhausted (or safety cap).
fn multipage_rql(
    col: &mut residiuum_sdk::CollectionClient,
    source: &str,
    params: &Parameters,
    page_size: u32,
) -> Vec<QueryPage> {
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(page_size);
    let mut pages = Vec::new();
    let mut guard = 0u32;
    loop {
        guard += 1;
        assert!(guard < 64, "page drain runaway");
        let page = col.rql(source, params, opts.clone()).expect("rql page");
        let done = page.exhausted || page.next.is_none();
        let next = page.next.clone();
        pages.push(page);
        if done {
            break;
        }
        opts.after = next;
    }
    pages
}

fn unpaged_rql(
    col: &mut residiuum_sdk::CollectionClient,
    source: &str,
    params: &Parameters,
) -> QueryPage {
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(4096);
    col.rql(source, params, opts).expect("unpaged rql")
}

fn assert_concat_equals_unpaged(pages: &[QueryPage], unpaged: &QueryPage, ordered: bool) {
    let cat = concat_pages(pages);
    let full = page_rows(unpaged);
    assert_eq!(
        cat.len(),
        full.len(),
        "row_count diverge concat={} unpaged={}",
        cat.len(),
        full.len()
    );
    // Coverage: every intermediate complete-by-default page should be complete
    // for its sources; final exhausted page complete.
    for (i, p) in pages.iter().enumerate() {
        assert!(
            p.coverage.complete,
            "page {i} must not claim incomplete under healthy media"
        );
        assert_eq!(p.coverage.hole_count, 0, "page {i} holes");
    }
    assert!(unpaged.coverage.complete);
    assert!(unpaged.exhausted || unpaged.next.is_none());

    if ordered {
        for (i, (a, b)) in cat.iter().zip(full.iter()).enumerate() {
            assert_eq!(a.key, b.key, "order key diverge at {i}");
            assert_eq!(a.value, b.value, "order value diverge at {i} key={}", a.key);
        }
    } else {
        let mut ca: BTreeMap<(String, String), u32> = BTreeMap::new();
        let mut fa: BTreeMap<(String, String), u32> = BTreeMap::new();
        for r in &cat {
            let v = serde_json::to_string(&r.value).unwrap_or_default();
            *ca.entry((r.key.clone(), v)).or_default() += 1;
        }
        for r in &full {
            let v = serde_json::to_string(&r.value).unwrap_or_default();
            *fa.entry((r.key.clone(), v)).or_default() += 1;
        }
        assert_eq!(ca, fa, "multiset diverge");
    }

    // No duplicate keys across page boundaries under exclusive scan (key stream).
    let mut seen = std::collections::BTreeSet::new();
    for r in &cat {
        assert!(
            seen.insert(r.key.clone()),
            "duplicate key across pages: {}",
            r.key
        );
    }
}

#[test]
fn q34_law_page_concat_equals_unpaged_key_order() {
    let (_dir, mut client) = open_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..11 {
        col.put(&format!("k{i:02}"), &json!({"i": i, "tag": "t"}))
            .unwrap();
    }
    let params = Parameters::default();
    let source = "from docs";
    let pages = multipage_rql(&mut col, source, &params, 3);
    assert!(
        pages.len() >= 4,
        "expected multiple pages, got {}",
        pages.len()
    );
    let unpaged = unpaged_rql(&mut col, source, &params);
    assert_concat_equals_unpaged(&pages, &unpaged, true);
    assert_eq!(page_rows(&unpaged).len(), 11);
}

#[test]
fn q34_law_page_concat_equals_unpaged_field_order() {
    let (_dir, mut client) = open_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    // Keys reverse of score order.
    for (k, score) in [
        ("z", 1),
        ("y", 2),
        ("x", 3),
        ("w", 4),
        ("v", 5),
        ("u", 6),
        ("t", 7),
    ] {
        col.put(k, &json!({"score": score, "name": k})).unwrap();
    }
    let params = Parameters::default();
    let source = "from orders order by score desc, _key asc";
    let pages = multipage_rql(&mut col, source, &params, 2);
    assert!(pages.len() >= 3);
    let unpaged = unpaged_rql(&mut col, source, &params);
    assert_concat_equals_unpaged(&pages, &unpaged, true);
    let keys: Vec<_> = page_rows(&unpaged).into_iter().map(|r| r.key).collect();
    assert_eq!(
        keys,
        vec![
            "t".to_string(),
            "u".to_string(),
            "v".to_string(),
            "w".to_string(),
            "x".to_string(),
            "y".to_string(),
            "z".to_string()
        ]
    );
}

#[test]
fn q34_law_page_concat_filtered_equals_unpaged() {
    let (_dir, mut client) = open_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    for i in 0..20 {
        let st = if i % 3 == 0 { "paid" } else { "open" };
        col.put(&format!("o{i:02}"), &json!({"status": st, "n": i}))
            .unwrap();
    }
    let params = Parameters::default();
    let source = r#"from orders where status = "paid" order by _key asc"#;
    let pages = multipage_rql(&mut col, source, &params, 2);
    let unpaged = unpaged_rql(&mut col, source, &params);
    assert_concat_equals_unpaged(&pages, &unpaged, true);
    assert!(!page_rows(&unpaged).is_empty());
}

#[test]
fn q34_law_single_page_is_unpaged() {
    // When page_size ≥ cardinality, multipage path is one page == unpaged.
    let (_dir, mut client) = open_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..5 {
        col.put(&format!("k{i}"), &json!({"i": i})).unwrap();
    }
    let params = Parameters::default();
    let pages = multipage_rql(&mut col, "from docs", &params, 64);
    assert_eq!(pages.len(), 1);
    let unpaged = unpaged_rql(&mut col, "from docs", &params);
    assert_concat_equals_unpaged(&pages, &unpaged, true);
}

#[test]
fn q34_law_textual_after_concat_equals_unpaged() {
    let (_dir, mut client) = open_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..11 {
        col.put(&format!("k{i:02}"), &json!({"i": i})).unwrap();
    }
    let first_source = "from docs page size 3";
    let resumed_source = "from docs page size 3 after $cursor";
    let mut params = Parameters::default();
    let mut pages = Vec::new();
    let first = col
        .rql(first_source, &params, QueryRunOptions::default())
        .unwrap();
    let mut next = first.next.clone();
    pages.push(first);
    while let Some(cursor) = next {
        params.values.insert(
            "cursor".into(),
            Value::String(String::from_utf8(cursor.token).unwrap()),
        );
        let page = col
            .rql(resumed_source, &params, QueryRunOptions::default())
            .unwrap();
        next = page.next.clone();
        pages.push(page);
        assert!(pages.len() < 16, "textual cursor drain runaway");
    }
    let unpaged = unpaged_rql(&mut col, "from docs", &Parameters::default());
    assert_concat_equals_unpaged(&pages, &unpaged, true);
}

#[test]
fn q34_write_report() {
    // Evidence stamp for verify script (unit law suite presence).
    // F8: default → target/ only; RESIDIUUM_WRITE_SPEC_EVIDENCE=1 publishes spec/.
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let report = json!({
        "format": "residiuum-rql-q3-4-page-concat-report-v1",
        "package": "RQL-Q3",
        "task": "Q3.4",
        "authority": "doc/todo/rql/RQL_QUERY_QUALIFICATION_PROGRAM.md §6.3",
        "summary": {
            "laws": [
                "page_concat_key_order",
                "page_concat_field_order",
                "page_concat_filtered",
                "single_page_equals_unpaged",
                "textual_after_page_concat"
            ],
            "law_count": 5,
            "product_path": "CollectionClient::rql + QueryRunOptions.after + textual after $cursor",
            "source_after_cursor_residual": false,
            "false_absence_defects": 0,
        },
        "non_claims": [
            "not_gate1",
            "not_package_accept"
        ],
        "human": "doc/todo/rql/RQL_Q3_4_PAGE_CONCAT.md",
    });
    let path = rql_evidence_write::write_q3_report(
        &root,
        "q3_4_page_concat_report.json",
        &serde_json::to_string_pretty(&report).unwrap(),
    );
    assert!(path.is_file());
}
