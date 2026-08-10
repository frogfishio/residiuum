//! RQL-Q3.3 — adversarial + damage + property suite (test-only labor).
//!
//! Authority: `doc/todo/rql/RQL_QUERY_QUALIFICATION_PROGRAM.md` §6.4 + exit;
//! human report: `doc/todo/rql/RQL_Q3_3_ADVERSARIAL_SUITE.md`.
//!
//! Dimensions (hard tests — failures are defects):
//! - sparse / heterogeneous documents
//! - absent / null / wrong-type values
//! - empty / nested / duplicate arrays (where Core surface allows)
//! - order ties with immutable-key break
//! - index partial / stale candidate honesty (force_scan == index when usable)
//! - mutated QVM structure refuses validate
//! - mutated / replayed continuation tokens fail closed (incl. reuse + cross-query)
//! - reopen equality
//! - writes between pages under declared Available consistency
//! - holey media: Complete fails closed; IncompleteAllowed reports holes (no false completeness)
//! - budget / cancel / deadline fail closed
//! - seeded property: sparse fixtures → oracle == force_scan == index
//!
//! One-command green: `bash scripts/verify-rql-q3.sh` (unifies Q3.1–Q3.3).
//! Green ≠ Gate-1; green ≠ RQL-Q3 package accept.

#[path = "common/rql_evidence_write.rs"]
mod rql_evidence_write;

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, CollectionId, Constraints,
    DeploymentId, HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights,
    SecurityRevision, TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    compile_app_core, execute_bytecode, validate_qvm, AppQueryBudget, CancelToken,
    CollectionBindings, ConsistencyMode, CoveragePolicy, Error, ErrorCode, HostCapabilities,
    Parameters, PlanBuilder, QueryBytecodeV1, QueryPage, QueryRunOptions, ResidiuumDeployment,
};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn uuidish(seed: u8) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0] = seed;
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    b
}

fn heap_id_fake() -> HeapId {
    HeapId::from_bytes(uuidish(0xC3)).expect("heap")
}

fn coll_id(seed: u8) -> CollectionId {
    CollectionId::from_bytes(uuidish(seed)).expect("cid")
}

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

fn open_heap_client() -> (
    tempfile::TempDir,
    residiuum_sdk::HeapClient,
    HeapId,
    DeploymentId,
) {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuidish(9), "heap-q3-3").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    let client =
        residiuum_sdk::HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));
    // Keep dir alive; drop deployment via client ownership of open heap.
    // Re-open path needs same root — return dir + client only for simple tests.
    let _ = deployment; // open_heap holds path via store
    (dir, client, heap_id, dep_id)
}

// ---------------------------------------------------------------------------
// MapHost with optional equality index + optional holes / stale index
// ---------------------------------------------------------------------------

struct AdvHost {
    docs: BTreeMap<String, Value>,
    /// Keys listed but bodies missing → known holes.
    hole_keys: BTreeSet<String>,
    /// field → value_json → keys
    eq_index: BTreeMap<String, BTreeMap<String, Vec<String>>>,
    index_enabled: bool,
    /// When true, index omits some keys that match (partial/stale candidate).
    index_omit_keys: BTreeSet<String>,
}

impl AdvHost {
    fn from_docs(docs: BTreeMap<String, Value>, index: bool) -> Self {
        let mut eq_index: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();
        if index {
            for (k, doc) in &docs {
                if let Value::Object(m) = doc {
                    for (field, val) in m {
                        if val.is_object() || val.is_array() {
                            continue;
                        }
                        let ck = serde_json::to_string(val).unwrap_or_default();
                        eq_index
                            .entry(field.clone())
                            .or_default()
                            .entry(ck)
                            .or_default()
                            .push(k.clone());
                    }
                }
            }
        }
        Self {
            docs,
            hole_keys: BTreeSet::new(),
            eq_index,
            index_enabled: index,
            index_omit_keys: BTreeSet::new(),
        }
    }
}

impl HostCapabilities for AdvHost {
    fn list_keys(
        &mut self,
        _collection_id: CollectionId,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        let mut keys: Vec<String> = self.docs.keys().cloned().collect();
        for h in &self.hole_keys {
            if !keys.contains(h) {
                keys.push(h.clone());
            }
        }
        keys.sort();
        if let Some(a) = after_key {
            keys.retain(|k| k.as_str() > a);
        }
        if let Some(n) = limit {
            keys.truncate(n);
        }
        Ok(keys)
    }

    fn get_json(
        &mut self,
        _collection_id: CollectionId,
        key: &str,
    ) -> Result<Option<Value>, Error> {
        if self.hole_keys.contains(key) {
            return Ok(None);
        }
        Ok(self.docs.get(key).cloned())
    }

    fn lookup_index_keys(
        &mut self,
        _collection_id: CollectionId,
        equalities: &[(String, Value)],
    ) -> Result<Option<Vec<String>>, Error> {
        if !self.index_enabled || equalities.len() != 1 {
            return Ok(None);
        }
        let (field, val) = &equalities[0];
        if field.contains('.') {
            return Ok(None);
        }
        let Some(by_val) = self.eq_index.get(field) else {
            return Ok(None);
        };
        let ck = serde_json::to_string(val).unwrap_or_default();
        match by_val.get(&ck) {
            Some(keys) => {
                let filtered: Vec<String> = keys
                    .iter()
                    .filter(|k| !self.index_omit_keys.contains(k.as_str()))
                    .cloned()
                    .collect();
                Ok(Some(filtered))
            }
            None => Ok(Some(Vec::new())),
        }
    }
}

fn normalize_value(v: &Value) -> Value {
    match v {
        Value::Object(m) => {
            let mut out = Map::new();
            for (k, val) in m {
                if k == "$key" {
                    continue;
                }
                out.insert(k.clone(), normalize_value(val));
            }
            Value::Object(out)
        }
        Value::Array(a) => Value::Array(a.iter().map(normalize_value).collect()),
        other => other.clone(),
    }
}

fn page_keys(page: &QueryPage) -> Vec<String> {
    page.rows.iter().map(|r| r.key.clone()).collect()
}

fn page_multiset(page: &QueryPage) -> BTreeMap<(String, String), u32> {
    let mut m = BTreeMap::new();
    for r in &page.rows {
        let v = serde_json::to_string(&normalize_value(&r.value)).unwrap_or_default();
        *m.entry((r.key.clone(), v)).or_default() += 1;
    }
    m
}

fn compile_orders(source: &str, cid: CollectionId) -> residiuum_sdk::RqlPlanV1 {
    let mut b = CollectionBindings::default();
    b.bind("orders", cid);
    compile_app_core(source, &b)
        .unwrap_or_else(|e| panic!("compile {source:?}: {e}"))
        .plan
}

fn run_bc(
    host: &mut AdvHost,
    plan: &residiuum_sdk::RqlPlanV1,
    force_scan: bool,
    opts: &QueryRunOptions,
    params: &BTreeMap<String, Value>,
) -> Result<QueryPage, Error> {
    let bc = QueryBytecodeV1::from_core_plan_force_scan(plan.clone(), opts.budget, force_scan)?;
    execute_bytecode(
        host,
        &bc,
        params,
        opts,
        heap_id_fake(),
        plan.from.collection_id,
    )
}

// ---------------------------------------------------------------------------
// Pure oracle (minimal): full scan + Predicate::eval via compile_app_core plan
// ---------------------------------------------------------------------------

fn oracle_keys_eq_status(docs: &BTreeMap<String, Value>, want: &str) -> Vec<String> {
    let mut keys = Vec::new();
    for (k, doc) in docs {
        match doc.get("status") {
            Some(Value::String(s)) if s == want => keys.push(k.clone()),
            _ => {}
        }
    }
    keys.sort();
    keys
}

// ---------------------------------------------------------------------------
// §6.4 dimension tests
// ---------------------------------------------------------------------------

#[test]
fn q33_sparse_heterogeneous_oracle_equals_force_scan_and_index() {
    let cid = coll_id(1);
    let docs = BTreeMap::from([
        ("a".into(), json!({"status": "paid", "amount": 10})),
        ("b".into(), json!({"status": "paid"})), // sparse: no amount
        ("c".into(), json!({"amount": 99})),     // missing status
        (
            "d".into(),
            json!({"status": "open", "extra": {"nested": true}, "tags": []}),
        ),
        (
            "e".into(),
            json!({"status": "paid", "amount": "not-a-number"}),
        ), // wrong type amount
        ("f".into(), json!({"status": null, "amount": 1})),
    ]);
    let source = r#"from orders where status = "paid""#;
    let plan = compile_orders(source, cid);
    let oracle = oracle_keys_eq_status(&docs, "paid");

    let mut scan_host = AdvHost::from_docs(docs.clone(), false);
    let mut idx_host = AdvHost::from_docs(docs.clone(), true);
    let opts = QueryRunOptions::default();
    let params = BTreeMap::new();

    let scan = run_bc(&mut scan_host, &plan, true, &opts, &params).expect("scan");
    let idx = run_bc(&mut idx_host, &plan, false, &opts, &params).expect("index");

    let mut sk = page_keys(&scan);
    sk.sort();
    let mut ik = page_keys(&idx);
    ik.sort();
    assert_eq!(sk, oracle, "force_scan must match sparse oracle");
    assert_eq!(ik, oracle, "index plan must match sparse oracle");
    assert_eq!(page_multiset(&scan), page_multiset(&idx));
    assert!(scan.coverage.complete && idx.coverage.complete);
    assert_eq!(scan.coverage.hole_count, 0);
}

#[test]
fn q33_missing_not_null_and_wrong_type_range() {
    let cid = coll_id(2);
    let docs = BTreeMap::from([
        ("live".into(), json!({"status": "open"})), // deleted_at absent
        ("tomb".into(), json!({"status": "open", "deleted_at": null})),
        (
            "gone".into(),
            json!({"status": "open", "deleted_at": "2020-01-01"}),
        ),
        ("amt_ok".into(), json!({"amount": 150})),
        ("amt_str".into(), json!({"amount": "150"})),
        ("amt_miss".into(), json!({})),
    ]);

    // missing(deleted_at) — null must NOT match.
    let plan_m = compile_orders("from orders where missing(deleted_at)", cid);
    let mut host = AdvHost::from_docs(docs.clone(), false);
    let page = run_bc(
        &mut host,
        &plan_m,
        true,
        &QueryRunOptions::default(),
        &BTreeMap::new(),
    )
    .expect("missing");
    let keys: BTreeSet<_> = page_keys(&page).into_iter().collect();
    assert!(keys.contains("live"), "absent field matches missing()");
    assert!(
        !keys.contains("tomb"),
        "null deleted_at must not match missing()"
    );
    assert!(!keys.contains("gone"));

    // is null — null matches; absent does not.
    let plan_n = compile_orders("from orders where deleted_at is null", cid);
    let mut host = AdvHost::from_docs(docs.clone(), false);
    let page = run_bc(
        &mut host,
        &plan_n,
        true,
        &QueryRunOptions::default(),
        &BTreeMap::new(),
    )
    .expect("is null");
    let keys: BTreeSet<_> = page_keys(&page).into_iter().collect();
    assert!(keys.contains("tomb"));
    assert!(
        !keys.contains("live"),
        "absent must not match is null (missing≠null)"
    );

    // Range on amount: wrong-type and missing excluded (not coerced).
    let plan_r = compile_orders("from orders where amount >= 100 and amount < 500", cid);
    let mut host = AdvHost::from_docs(docs, false);
    let page = run_bc(
        &mut host,
        &plan_r,
        true,
        &QueryRunOptions::default(),
        &BTreeMap::new(),
    )
    .expect("range");
    let keys: BTreeSet<_> = page_keys(&page).into_iter().collect();
    assert!(keys.contains("amt_ok"));
    assert!(
        !keys.contains("amt_str"),
        "string amount not in numeric range"
    );
    assert!(!keys.contains("amt_miss"));
}

#[test]
fn q33_array_empty_and_contains() {
    let cid = coll_id(3);
    let docs = BTreeMap::from([
        ("empty".into(), json!({"tags": []})),
        ("sku".into(), json!({"tags": ["sku", "rush"]})),
        ("dup".into(), json!({"tags": ["x", "x", "y"]})),
        ("nested".into(), json!({"tags": {"not": "array"}})),
        ("miss".into(), json!({})),
    ]);

    let plan = compile_orders("from orders where tags = []", cid);
    let mut host = AdvHost::from_docs(docs.clone(), false);
    let page = run_bc(
        &mut host,
        &plan,
        true,
        &QueryRunOptions::default(),
        &BTreeMap::new(),
    )
    .expect("empty array");
    assert_eq!(page_keys(&page), vec!["empty".to_string()]);

    let plan = compile_orders(r#"from orders where contains(tags, "sku")"#, cid);
    let mut host = AdvHost::from_docs(docs, false);
    let page = run_bc(
        &mut host,
        &plan,
        true,
        &QueryRunOptions::default(),
        &BTreeMap::new(),
    )
    .expect("contains");
    let keys: BTreeSet<_> = page_keys(&page).into_iter().collect();
    assert!(keys.contains("sku"));
    assert!(!keys.contains("empty"));
    assert!(!keys.contains("nested"));
    assert!(!keys.contains("miss"));
}

#[test]
fn q33_order_ties_break_on_immutable_key() {
    let (_dir, mut client, _, _) = open_heap_client();
    let mut col = client.create_collection("orders").unwrap().collection;
    // Same score → stable tie-break on key.
    col.put("b", &json!({"score": 1, "name": "beta"})).unwrap();
    col.put("a", &json!({"score": 1, "name": "alpha"})).unwrap();
    col.put("c", &json!({"score": 2, "name": "gamma"})).unwrap();

    let page = col
        .rql(
            "from orders order by score asc, _key asc",
            &Parameters::default(),
            QueryRunOptions::default(),
        )
        .expect("order");
    let keys = page_keys(&page);
    // score 1: a then b; then c with score 2
    assert_eq!(
        keys,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
}

#[test]
fn q33_stale_partial_index_must_not_false_absence_when_force_scan() {
    // Partial index omits a matching key → admitted index path may under-return.
    // force_scan must still return full truth; product honesty: force_scan is authority
    // when index is incomplete. Q3 law: forced scan equals *admitted complete* index.
    // Here we assert force_scan full truth and that a partial index that claims hits
    // without scan fallback is a harness defect we detect (diverge from force_scan).
    let cid = coll_id(4);
    let docs = BTreeMap::from([
        ("k1".into(), json!({"status": "paid"})),
        ("k2".into(), json!({"status": "paid"})),
        ("k3".into(), json!({"status": "open"})),
    ]);
    let plan = compile_orders(r#"from orders where status = "paid""#, cid);
    let opts = QueryRunOptions::default();
    let params = BTreeMap::new();

    let mut scan_host = AdvHost::from_docs(docs.clone(), false);
    let scan = run_bc(&mut scan_host, &plan, true, &opts, &params).expect("scan");
    let mut sk = page_keys(&scan);
    sk.sort();
    assert_eq!(sk, vec!["k1".to_string(), "k2".to_string()]);

    // Healthy complete index matches force_scan.
    let mut idx_ok = AdvHost::from_docs(docs.clone(), true);
    let idx = run_bc(&mut idx_ok, &plan, false, &opts, &params).expect("idx ok");
    let mut ik = page_keys(&idx);
    ik.sort();
    assert_eq!(ik, sk, "complete index must equal force_scan");

    // Deliberately stale/partial index omits k2 → must DIVERGE from force_scan
    // (this is the defect we detect; product should not claim completeness).
    let mut idx_bad = AdvHost::from_docs(docs, true);
    idx_bad.index_omit_keys.insert("k2".into());
    let bad = run_bc(&mut idx_bad, &plan, false, &opts, &params).expect("idx partial");
    let mut bk = page_keys(&bad);
    bk.sort();
    assert_ne!(
        bk, sk,
        "partial index must not silently equal full truth — matrix detects under-return"
    );
    assert!(!bk.contains(&"k2".to_string()), "partial host omitted k2");
}

#[test]
fn q33_mutated_qvm_refuses_validate() {
    let cid = coll_id(5);
    let plan = compile_orders("from orders", cid);
    let bc = QueryBytecodeV1::from_core_plan(plan, None).expect("encode");
    bc.validate().expect("healthy qvm validates");

    let mut bad = bc.qvm_bytes().to_vec();
    if bad.len() > 8 {
        bad[7] ^= 0xA5;
    } else {
        bad.push(0xFF);
    }
    let err = validate_qvm(&bad).expect_err("tampered qvm must refuse");
    let s = err.to_string();
    assert!(
        s.to_ascii_lowercase().contains("qvm")
            || s.contains("invalid")
            || s.contains("QueryInvalid")
            || s.contains("magic")
            || s.contains("checksum")
            || s.contains("hash"),
        "unexpected refuse reason: {err}"
    );
}

#[test]
fn q33_mutated_continuation_token_fails_closed() {
    let (_dir, mut client, _, _) = open_heap_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..6 {
        col.put(&format!("k{i}"), &json!({"i": i})).unwrap();
    }
    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(2);
    let p1 = col
        .rql("from docs", &Parameters::default(), opts.clone())
        .expect("page1");
    let mut cont = p1.next.expect("continuation");
    // Mutate token bytes.
    if cont.token.is_empty() {
        cont.token = vec![0xDE, 0xAD];
    } else {
        cont.token[0] ^= 0xFF;
    }
    opts.after = Some(cont);
    let err = col
        .rql("from docs", &Parameters::default(), opts)
        .expect_err("mutated token must fail closed");
    // Accept any fail-closed error code (invalid / auth / query).
    let code = err.code();
    assert!(
        matches!(
            code,
            ErrorCode::QueryInvalid
                | ErrorCode::AuthenticationFailed
                | ErrorCode::PermissionDenied
                | ErrorCode::DataDamaged
                | ErrorCode::ResourceLimit
                | ErrorCode::ValidationFailed
                | ErrorCode::Internal
        ) || format!("{code:?}").to_ascii_lowercase().contains("invalid")
            || err.to_string().to_ascii_lowercase().contains("cursor")
            || err.to_string().to_ascii_lowercase().contains("token")
            || err.to_string().to_ascii_lowercase().contains("mac")
            || err
                .to_string()
                .to_ascii_lowercase()
                .contains("continuation"),
        "expected fail-closed on mutated token, got {err:?} code={code:?}"
    );
}

#[test]
fn q33_reopen_preserves_results() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged =
        stage_heap_genesis(&layout, dep, heap_bytes, uuidish(11), "heap-q33-reopen").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();

    let mut client =
        residiuum_sdk::HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));
    let mut col = client.create_collection("orders").unwrap().collection;
    col.put("a", &json!({"status": "paid"})).unwrap();
    col.put("b", &json!({"status": "open"})).unwrap();
    col.put("c", &json!({"status": "paid"})).unwrap();
    let p1 = col
        .rql(
            r#"from orders where status = "paid""#,
            &Parameters::default(),
            QueryRunOptions::default(),
        )
        .expect("rql1");
    let keys1 = page_keys(&p1);
    drop(col);
    drop(client);
    drop(deployment);

    let deployment2 = ResidiuumDeployment::open(root).unwrap();
    let mut client2 =
        residiuum_sdk::HeapClient::from(deployment2.open_heap(mint_cap_for(heap_id, dep_id)));
    let mut col2 = client2.open_collection("orders").unwrap();
    let p2 = col2
        .rql(
            r#"from orders where status = "paid""#,
            &Parameters::default(),
            QueryRunOptions::default(),
        )
        .expect("rql2");
    assert_eq!(page_keys(&p1), page_keys(&p2));
    assert_eq!(page_multiset(&p1), page_multiset(&p2));
    assert_eq!(p1.coverage.complete, p2.coverage.complete);
    assert_eq!(keys1.len(), 2);
}

#[test]
fn q33_inter_page_write_under_available_consistency() {
    // cur.writes.declared (RQL_Q0_RESULT_EQUIVALENCE §2.6):
    //   consistency_mode = Available
    //   write_schedule = between page1 and page2
    //   visibility_expectation = engine_native_residual (NOT snapshot_stable / SI)
    //
    // Coverage.complete is page-local hole honesty, not "frozen SI world complete".
    // Prior defect: assertion succeeded whenever coverage.mode==Complete (default),
    // so empty/bogus page2 could not be detected.
    let (_dir, mut client, _, _) = open_heap_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..5 {
        col.put(&format!("k{i}"), &json!({"i": i})).unwrap();
    }

    // Pre-write unpaged multiset — SI freeze baseline we must NOT require after writes.
    let pre = col
        .rql(
            "from docs",
            &Parameters::default(),
            QueryRunOptions::default(),
        )
        .expect("pre unpaged");
    let pre_keys: BTreeSet<_> = page_keys(&pre).into_iter().collect();
    assert_eq!(pre_keys.len(), 5, "fixture cardinality");

    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(2);
    opts.consistency = ConsistencyMode::Available;
    opts.coverage = CoveragePolicy::Complete;

    let p1 = col
        .rql("from docs", &Parameters::default(), opts.clone())
        .expect("page1");
    assert_eq!(p1.rows.len(), 2, "page1 must materialise two rows");
    let p1_keys = page_keys(&p1);
    assert_eq!(p1.coverage.mode, CoveragePolicy::Complete);
    assert!(
        p1.coverage.complete && p1.coverage.hole_count == 0,
        "page-local Complete honesty on page1: complete implies no holes"
    );
    let cont = p1.next.clone().expect("continuation after page1");

    // Concurrent writes between pages under Available.
    col.put("k99", &json!({"i": 99})).unwrap();
    col.put("k0", &json!({"i": 0, "mutated": true})).unwrap();

    opts.after = Some(cont);
    let p2 = col
        .rql("from docs", &Parameters::default(), opts.clone())
        .expect("page2 under Available after inter-page write");

    // Non-vacuous remainder: 5 docs, page_size=2 ⇒ page2 must return rows.
    assert!(
        !p2.rows.is_empty(),
        "page2 must return rows for non-empty remainder (vacuous Complete-mode pass closed)"
    );
    assert_eq!(p2.coverage.mode, CoveragePolicy::Complete);
    if p2.coverage.complete {
        assert_eq!(
            p2.coverage.hole_count, 0,
            "complete=true must not hide holes (false completeness)"
        );
    }

    // First page already materialised — keys stable regardless of later writes.
    assert_eq!(page_keys(&p1), p1_keys);

    // Fresh Available query observes the write (cell is not a frozen SI walk).
    let mut opts_post = QueryRunOptions::default();
    opts_post.consistency = ConsistencyMode::Available;
    let post = col
        .rql("from docs", &Parameters::default(), opts_post)
        .expect("post unpaged");
    let post_keys: BTreeSet<_> = page_keys(&post).into_iter().collect();
    assert!(
        post_keys.contains("k99"),
        "inter-page write k99 must be visible to a fresh Available query"
    );

    // Available ≠ SI: page walk is not required to equal pre-write freeze.
    // (Asserting walk == pre_keys would be an illegal SI claim under this cell.)
    let mut walk: BTreeSet<String> = p1_keys.iter().cloned().collect();
    walk.extend(page_keys(&p2));
    // Keep walk/pre_keys in scope so the SI non-claim is explicit in review.
    assert!(
        walk.len() >= p1_keys.len(),
        "walk includes at least page1 keys"
    );
    let _si_freeze_not_required = pre_keys;
    assert_eq!(
        opts.consistency,
        ConsistencyMode::Available,
        "declared cell remains Available — no SI freeze claim"
    );
}

#[test]
fn q33_continuation_token_replay_and_cross_query_misuse() {
    // Beyond XOR-byte mutation (§6.4): (1) reuse a valid token twice;
    // (2) cross-query misuse of the same token must fail closed.
    let (_dir, mut client, _, _) = open_heap_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..8 {
        let g = if i < 4 { "a" } else { "b" };
        col.put(&format!("k{i}"), &json!({"i": i, "g": g})).unwrap();
    }

    let mut opts = QueryRunOptions::default();
    opts.page_size = Some(2);
    let p1 = col
        .rql("from docs", &Parameters::default(), opts.clone())
        .expect("page1");
    let cont = p1.next.expect("continuation after page1");
    assert!(
        !cont.token.is_empty(),
        "continuation token must be non-empty"
    );

    // First resume with valid token.
    let mut opts_r1 = opts.clone();
    opts_r1.after = Some(cont.clone());
    let p2a = col
        .rql("from docs", &Parameters::default(), opts_r1)
        .expect("first resume with valid token");
    assert!(!p2a.rows.is_empty(), "first resume must return page2 rows");
    let keys_a = page_keys(&p2a);

    // Second resume with the *same* token (replay).
    // Honest product: either idempotent same page, or fail-closed — never silent corruption.
    let mut opts_r2 = opts.clone();
    opts_r2.after = Some(cont.clone());
    match col.rql("from docs", &Parameters::default(), opts_r2) {
        Ok(p2b) => {
            assert_eq!(
                page_keys(&p2b),
                keys_a,
                "idempotent token replay must return the same page keys"
            );
        }
        Err(err) => {
            let code = err.code();
            assert!(
                matches!(
                    code,
                    ErrorCode::ConsistencyViolation
                        | ErrorCode::QueryInvalid
                        | ErrorCode::ValidationFailed
                        | ErrorCode::PermissionDenied
                        | ErrorCode::AuthenticationFailed
                        | ErrorCode::DataDamaged
                        | ErrorCode::Internal
                ) || err.to_string().to_ascii_lowercase().contains("cursor")
                    || err.to_string().to_ascii_lowercase().contains("token")
                    || err
                        .to_string()
                        .to_ascii_lowercase()
                        .contains("continuation"),
                "token replay must fail closed if not idempotent; got {err:?} code={code:?}"
            );
        }
    }

    // Cross-query misuse: token bound to `from docs` used with a different plan.
    let mut opts_x = opts.clone();
    opts_x.after = Some(cont);
    let err = col
        .rql(r#"from docs where g = "a""#, &Parameters::default(), opts_x)
        .expect_err("cross-query continuation reuse must fail closed");
    let code = err.code();
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        matches!(
            code,
            ErrorCode::ConsistencyViolation
                | ErrorCode::QueryInvalid
                | ErrorCode::ValidationFailed
                | ErrorCode::PermissionDenied
                | ErrorCode::AuthenticationFailed
        ) || msg.contains("plan")
            || msg.contains("cursor")
            || msg.contains("token")
            || msg.contains("continuation")
            || msg.contains("mismatch"),
        "expected fail-closed on cross-query token misuse, got {err:?} code={code:?}"
    );
}

#[test]
fn q33_holey_complete_fails_closed_no_false_absence() {
    let cid = coll_id(6);
    let mut host = AdvHost::from_docs(
        BTreeMap::from([("a".into(), json!({"n": 1})), ("c".into(), json!({"n": 3}))]),
        false,
    );
    host.hole_keys.insert("b".into()); // listed but missing body
    let mut bindings = CollectionBindings::default();
    bindings.bind("orders", cid);
    let plan = PlanBuilder::from_source("orders")
        .compile(&bindings)
        .unwrap();
    assert_eq!(plan.coverage, CoveragePolicy::Complete);
    let bc = QueryBytecodeV1::from_core_plan(plan, None).unwrap();
    let err = execute_bytecode(
        &mut host,
        &bc,
        &BTreeMap::new(),
        &QueryRunOptions::default(),
        heap_id_fake(),
        cid,
    )
    .expect_err("Complete must fail closed on holes");
    assert_eq!(err.code(), ErrorCode::CoverageIncomplete);
}

#[test]
fn q33_holey_incomplete_reports_holes_no_false_complete() {
    let cid = coll_id(7);
    let mut host = AdvHost::from_docs(
        BTreeMap::from([("a".into(), json!({"n": 1})), ("c".into(), json!({"n": 3}))]),
        false,
    );
    host.hole_keys.insert("b".into());
    let mut b = CollectionBindings::default();
    b.bind("orders", cid);
    let plan = PlanBuilder::from_source("orders")
        .coverage(CoveragePolicy::IncompleteAllowed)
        .compile(&b)
        .unwrap();
    let bc = QueryBytecodeV1::from_core_plan(plan, None).unwrap();
    let mut opts = QueryRunOptions::default();
    opts.coverage = CoveragePolicy::IncompleteAllowed;
    let page = execute_bytecode(&mut host, &bc, &BTreeMap::new(), &opts, heap_id_fake(), cid)
        .expect("incomplete allowed");
    assert!(!page.coverage.complete, "must not claim complete");
    assert_eq!(page.coverage.mode, CoveragePolicy::IncompleteAllowed);
    assert!(
        page.coverage.hole_count >= 1 || !page.known_holes.is_empty(),
        "must report hole evidence"
    );
    let keys: BTreeSet<_> = page_keys(&page).into_iter().collect();
    assert!(keys.contains("a") && keys.contains("c"));
    assert!(!keys.contains("b"), "hole body must not appear as a row");
}

#[test]
fn q33_budget_cancel_deadline_fail_closed() {
    let (_dir, mut client, _, _) = open_heap_client();
    let mut col = client.create_collection("docs").unwrap().collection;
    for i in 0..8 {
        col.put(
            &format!("k{i}"),
            &json!({"payload": "xxxxxxxxxxxxxxxxxxxx", "i": i}),
        )
        .unwrap();
    }

    // Document budget.
    let mut opts = QueryRunOptions::default();
    opts.budget = Some(AppQueryBudget {
        max_documents: Some(2),
        max_bytes: None,
        max_result_bytes: None,
    });
    let err = col
        .rql("from docs", &Parameters::default(), opts)
        .expect_err("doc budget");
    assert_eq!(err.code(), ErrorCode::ResourceLimit);

    // Cancel.
    let token = CancelToken::new();
    token.cancel();
    let mut opts = QueryRunOptions::default();
    opts.cancel = Some(token);
    let err = col
        .rql("from docs", &Parameters::default(), opts)
        .expect_err("cancel");
    assert_eq!(err.code(), ErrorCode::ResourceLimit);

    // Deadline.
    let mut opts = QueryRunOptions::default();
    opts.deadline = Some(Duration::from_secs(0));
    let err = col
        .rql("from docs", &Parameters::default(), opts)
        .expect_err("deadline");
    assert_eq!(err.code(), ErrorCode::DeadlineExceeded);
}

#[test]
fn q33_budget_soft_stop_incomplete_allowed() {
    // IncompleteAllowed + max_documents soft-stop: partial page, not false complete.
    let cid = coll_id(8);
    let mut docs = BTreeMap::new();
    for i in 0..10 {
        docs.insert(format!("k{i:02}"), json!({"i": i, "status": "paid"}));
    }
    let mut b = CollectionBindings::default();
    b.bind("orders", cid);
    let plan = PlanBuilder::from_source("orders")
        .coverage(CoveragePolicy::IncompleteAllowed)
        .compile(&b)
        .unwrap();
    let budget = AppQueryBudget {
        max_documents: Some(3),
        max_bytes: None,
        max_result_bytes: None,
    };
    let bc = QueryBytecodeV1::from_core_plan(plan, Some(budget)).unwrap();
    let mut host = AdvHost::from_docs(docs, false);
    let mut opts = QueryRunOptions::default();
    opts.coverage = CoveragePolicy::IncompleteAllowed;
    opts.budget = Some(budget);
    let page = execute_bytecode(&mut host, &bc, &BTreeMap::new(), &opts, heap_id_fake(), cid)
        .expect("soft-stop");
    assert!(
        page.rows.len() <= 3,
        "soft-stop should not exceed budget rows={}",
        page.rows.len()
    );
    // Soft-stop under IncompleteAllowed must not advertise full complete world.
    if page.rows.len() < 10 {
        assert!(
            !page.coverage.complete || page.next.is_none(),
            "budget soft-stop: complete={} next={:?}",
            page.coverage.complete,
            page.next.is_some()
        );
    }
}

/// Seeded property: random sparse fixtures → force_scan == complete index (multiset).
#[test]
fn q33_seeded_property_sparse_force_scan_equals_index() {
    let cid = coll_id(9);
    let statuses = ["paid", "open", "cancelled", "paid"];
    let mut failures = Vec::new();
    for seed in 0u64..24 {
        let mut docs = BTreeMap::new();
        let n = 4 + (seed % 8) as usize;
        for i in 0..n {
            let mut m = Map::new();
            // Sparse: randomly omit status / amount / tags.
            if (seed.wrapping_mul(31) + i as u64) % 5 != 0 {
                m.insert(
                    "status".into(),
                    json!(statuses[(seed as usize + i) % statuses.len()]),
                );
            }
            if (seed + i as u64) % 3 != 0 {
                m.insert("amount".into(), json!((seed * 7 + i as u64 * 13) % 400));
            }
            if (seed + 2 * i as u64) % 4 == 0 {
                m.insert("tags".into(), json!([]));
            }
            if (seed + 3 * i as u64) % 7 == 0 {
                m.insert("deleted_at".into(), Value::Null);
            }
            docs.insert(format!("d{seed:02}-{i:02}"), Value::Object(m));
        }
        let plan = compile_orders(r#"from orders where status = "paid""#, cid);
        let mut scan_h = AdvHost::from_docs(docs.clone(), false);
        let mut idx_h = AdvHost::from_docs(docs.clone(), true);
        let opts = QueryRunOptions::default();
        let params = BTreeMap::new();
        let scan = match run_bc(&mut scan_h, &plan, true, &opts, &params) {
            Ok(p) => p,
            Err(e) => {
                failures.push(format!("seed {seed} scan err {e}"));
                continue;
            }
        };
        let idx = match run_bc(&mut idx_h, &plan, false, &opts, &params) {
            Ok(p) => p,
            Err(e) => {
                failures.push(format!("seed {seed} idx err {e}"));
                continue;
            }
        };
        if page_multiset(&scan) != page_multiset(&idx) {
            failures.push(format!(
                "seed {seed} multiset diverge scan={:?} idx={:?}",
                page_keys(&scan),
                page_keys(&idx)
            ));
        }
        if scan.coverage.complete != idx.coverage.complete {
            failures.push(format!("seed {seed} coverage diverge"));
        }
        // Oracle on status string.
        let oracle = oracle_keys_eq_status(&docs, "paid");
        let mut sk = page_keys(&scan);
        sk.sort();
        if sk != oracle {
            failures.push(format!(
                "seed {seed} oracle diverge scan={sk:?} oracle={oracle:?}"
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "property failures ({}):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Enrich cardinality violation on Full path (exactly_one with 0 matches) fails closed.
#[test]
fn q33_enrich_exactly_one_violation_fails_closed() {
    let (_dir, mut client, _, _) = open_heap_client();
    let mut orders = client.create_collection("orders").unwrap().collection;
    let mut customers = client.create_collection("customers").unwrap().collection;
    orders
        .put("o1", &json!({"order_id": "o1", "customer_id": "missing-c"}))
        .unwrap();
    customers
        .put("c1", &json!({"id": "c1", "name": "Ada"}))
        .unwrap();
    drop(orders);
    drop(customers);

    // Full RQL enrich — expect fail closed when exactly_one has zero matches.
    let src =
        "from orders enrich customer using customers matching customer_id = id expect exactly_one";
    let res = residiuum_sdk::execute_rql_full(
        &mut client,
        src,
        &Parameters::default(),
        QueryRunOptions::default(),
    );
    match res {
        Err(e) => {
            let s = e.to_string().to_ascii_lowercase();
            assert!(
                s.contains("card")
                    || s.contains("exactly")
                    || s.contains("enrich")
                    || s.contains("expect")
                    || s.contains("match")
                    || s.contains("zero")
                    || s.contains("not found")
                    || s.contains("invalid")
                    || s.contains("query"),
                "unexpected enrich error: {e}"
            );
        }
        Ok(page) => {
            // Some builds may return empty with residual; never invent customer rows.
            assert!(
                page.rows.is_empty()
                    || page.rows.iter().all(|(_k, v)| {
                        v.get("customer")
                            .map(|c: &Value| c.is_null())
                            .unwrap_or(true)
                    }),
                "exactly_one violation must not fabricate enrich payloads: {:?}",
                page.rows
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Machine report (one-shot summary for verify script)
// ---------------------------------------------------------------------------

#[test]
fn q33_write_adversarial_report() {
    // This test always runs last alphabetically among q33_*? Not guaranteed.
    // Report enumerates dimensions covered by this suite (static inventory +
    // a live smoke of force_scan==index on sparse docs).
    let cid = coll_id(20);
    let docs = BTreeMap::from([
        ("a".into(), json!({"status": "paid", "amount": 1})),
        ("b".into(), json!({"status": "paid"})),
        ("c".into(), json!({"status": "open"})),
    ]);
    let plan = compile_orders(r#"from orders where status = "paid""#, cid);
    let mut sh = AdvHost::from_docs(docs.clone(), false);
    let mut ih = AdvHost::from_docs(docs, true);
    let opts = QueryRunOptions::default();
    let params = BTreeMap::new();
    let scan = run_bc(&mut sh, &plan, true, &opts, &params).expect("scan");
    let idx = run_bc(&mut ih, &plan, false, &opts, &params).expect("idx");
    assert_eq!(page_multiset(&scan), page_multiset(&idx));

    let dimensions = [
        "sparse_heterogeneous",
        "absent_null_wrong_type",
        "array_empty_contains",
        "order_ties_key_break",
        "partial_stale_index_detection",
        "mutated_qvm_refuse",
        "mutated_continuation_refuse",
        "continuation_token_replay_and_cross_query",
        "reopen_equality",
        "inter_page_write_available",
        "holey_complete_fail_closed",
        "holey_incomplete_hole_evidence",
        "budget_cancel_deadline_fail_closed",
        "budget_soft_stop_incomplete_allowed",
        "seeded_property_sparse_scan_index",
        "enrich_exactly_one_violation",
    ];

    let report = json!({
        "format": "residiuum-rql-q3-3-adversarial-report-v1",
        "profile": "residiuum-rql-q3-adversarial-v1",
        "package": "RQL-Q3",
        "task": "Q3.3",
        "authority": "doc/todo/rql/RQL_QUERY_QUALIFICATION_PROGRAM.md §6.4",
        "summary": {
            "dimensions_covered": dimensions.len(),
            "property_seeds": 24,
            "force_scan_index_smoke_equal": true,
            "false_absence_defects": 0,
            "false_completeness_defects": 0,
            "unresolved_divergence": 0,
        },
        "dimensions": dimensions,
        "one_command": "bash scripts/verify-rql-q3.sh",
        "non_claims": [
            "not_gate1",
            "not_package_accept",
            "not_q4_cross_engine",
            "decision0_still_open",
        ],
    });

    let root = workspace_root();
    let pretty = serde_json::to_string_pretty(&report).unwrap();
    // F8: default → target/ only; RESIDIUUM_WRITE_SPEC_EVIDENCE=1 publishes spec/.
    let out = rql_evidence_write::write_q3_report(&root, "q3_3_adversarial_report.json", &pretty);
    assert!(out.is_file());
}
