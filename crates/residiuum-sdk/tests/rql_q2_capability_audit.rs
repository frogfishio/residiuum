//! RQL-Q2.1 — Tier-A corpus compile + product execute capability audit.
//!
//! Authority: `doc/todo/rql/RQL_QUERY_QUALIFICATION_PROGRAM.md` §5;
//! machine corpus: `spec/rql/qualification/corpus-v1/corpus-v1.json`.
//!
//! This is a **capability audit**, not Q3 semantic qualification:
//! every Tier-A case is attempted on the product path
//! (`compile_rql_full` + `execute_rql_full` / QVM). Failures are classified;
//! no silent skips. Default evidence is written under `target/rql-q2/`;
//! `RESIDIUUM_WRITE_SPEC_EVIDENCE=1` explicitly publishes the checked snapshot.
//!
//! Exit of this test = audit completed (report written). Green test ≠ Gate-1
//! pass and ≠ 100% Tier-A expressible.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, CollectionId, Constraints,
    DeploymentId, HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights,
    SecurityRevision, TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    compile_rql_full, execute_rql_full, CollectionBindings, CoveragePolicy, HeapClient, Parameters,
    QueryRunOptions, ResidiuumDeployment, DIAG_RQL_FEATURE_UNAVAILABLE, DIAG_RQL_FULL_RESIDUAL,
};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::tempdir;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
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

fn uuid_bytes() -> [u8; 16] {
    *CollectionId::new_random().unwrap().as_bytes()
}

/// Stable fake collection ids for pure compile (no store).
fn compile_bindings_for(names: &BTreeSet<String>) -> CollectionBindings {
    let mut b = CollectionBindings::default();
    // Deterministic-ish ids from name hash so re-runs compare cleanly.
    for (i, name) in names.iter().enumerate() {
        let mut bytes = [0u8; 16];
        let n = name.as_bytes();
        for (j, &c) in n.iter().enumerate().take(12) {
            bytes[j] = c;
        }
        bytes[12] = (i as u8).wrapping_add(1);
        bytes[13] = 0xA1;
        bytes[14] = 0xB2;
        bytes[15] = 0xC3.max(1);
        // Ensure nonzero UUID-shaped id via CollectionId helper when possible.
        let id = CollectionId::from_bytes_unchecked_nonzero(bytes)
            .or_else(|_| CollectionId::new_random())
            .expect("collection id");
        b.bind(name, id);
    }
    b
}

fn extract_collection_names(source: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let tokens: Vec<&str> = source.split_whitespace().collect();
    let mut i = 0;
    while i < tokens.len() {
        let t = tokens[i].to_ascii_lowercase();
        if (t == "from" || t == "using") && i + 1 < tokens.len() {
            let n = tokens[i + 1]
                .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .to_string();
            if !n.is_empty() && n != "where" && n != "project" {
                names.insert(n);
            }
        }
        // Corpus dialect: `enrich X from Y`
        if t == "enrich" && i + 3 < tokens.len() && tokens[i + 2].eq_ignore_ascii_case("from") {
            let n = tokens[i + 3]
                .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .to_string();
            if !n.is_empty() {
                names.insert(n);
            }
        }
        i += 1;
    }
    names
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum FailureClass {
    Semantic,
    Syntax,
    Compiler,
    Qvm,
    Host,
    Index,
    Wire,
    /// Expected stable refusal path behaved correctly (not a gap).
    ExpectedRefusal,
    /// Case has no runnable RQL source.
    CorpusGap,
}

impl FailureClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Semantic => "semantic",
            Self::Syntax => "syntax",
            Self::Compiler => "compiler",
            Self::Qvm => "qvm",
            Self::Host => "host",
            Self::Index => "index",
            Self::Wire => "wire",
            Self::ExpectedRefusal => "expected_refusal",
            Self::CorpusGap => "corpus_gap",
        }
    }
}

fn classify_error(msg: &str, source: &str) -> FailureClass {
    let m = msg.to_ascii_lowercase();
    let s = source.to_ascii_lowercase();

    if m.contains("op 118") || m.contains("core wire") || m.contains("not on application core") {
        return FailureClass::Wire;
    }
    if m.contains("index")
        && (m.contains("pushdown") || m.contains("missing") || m.contains("stale"))
    {
        return FailureClass::Index;
    }
    if m.contains("qvm") || m.contains("opcode") || m.contains("bytecode") || m.contains("run_vm") {
        return FailureClass::Qvm;
    }
    // Budget cases must return partial coverage, not hard resource-limit abort.
    if (m.contains("resource limit")
        || m.contains("budget")
        || m.contains("max_documents")
        || m.contains("max_result_bytes")
        || m.contains("result_bytes"))
        && s.contains("budget")
    {
        return FailureClass::Semantic;
    }
    if m.contains("unbound")
        || m.contains("list_collections")
        || m.contains("open_collection")
        || m.contains("not found")
        || m.contains("no such collection")
        || m.contains("fixture")
    {
        return FailureClass::Host;
    }

    // Semantic residuals (feature not in slice) before generic parse.
    if m.contains(DIAG_RQL_FEATURE_UNAVAILABLE)
        || m.contains(DIAG_RQL_FULL_RESIDUAL)
        || m.contains("outside rql-")
        || m.contains("unsupported")
        || m.contains("after / continuation")
        || s.contains(" group ")
        || s.contains("group by")
        || s.contains(" count(")
        || s.contains(" sum(")
        || s.contains(" avg(")
        || s.contains(" min(")
        || s.contains(" max(")
        || s.contains(" if ")
        || s.contains(" then ")
        || s.contains(" offset ")
        || s.contains(" after ")
        || s.contains("fulltext")
        || s.contains("knn(")
        || s.contains("geo_near")
        || s.starts_with("watch ")
    {
        return FailureClass::Semantic;
    }

    // Corpus enrich dialect vs product `using … matching … expect`.
    if (s.contains(" enrich ") || s.contains("\nenrich "))
        && (s.contains(" from ") && s.contains(" on ") && !s.contains(" using "))
    {
        return FailureClass::Syntax;
    }

    if m.contains("parse")
        || m.contains("unexpected")
        || m.contains("expected")
        || m.contains("unterminated")
        || m.contains("invalid token")
        || m.contains("query invalid")
    {
        // Prefer syntax when message looks like parse; else compiler.
        if m.contains("expected") || m.contains("unexpected") || m.contains("parse") {
            return FailureClass::Syntax;
        }
        return FailureClass::Compiler;
    }

    FailureClass::Compiler
}

/// Coarse gap package id for Q2.2 ordering (impact-ranked later).
fn gap_package_id(class: FailureClass, source: &str, family_tags: &[String]) -> String {
    let s = source.to_ascii_lowercase();
    if class == FailureClass::ExpectedRefusal {
        return "none_expected_refusal".into();
    }
    if class == FailureClass::CorpusGap {
        return "corpus_source_gap".into();
    }
    if s.contains("group by")
        || s.contains(" count(")
        || s.contains(" sum(")
        || s.contains(" avg(")
        || s.contains(" min(")
        || s.contains(" max(")
        || family_tags.iter().any(|t| t == "group_aggregate")
    {
        return "pkg_group_aggregate".into();
    }
    if s.contains(" if ")
        || s.contains(" then ")
        || family_tags
            .iter()
            .any(|t| t == "projection_computed_conditional")
    {
        return "pkg_computed_conditional_project".into();
    }
    if s.contains("enrich") || family_tags.iter().any(|t| t == "enrichment_cardinality") {
        // Dialect mismatch is its own package before full enrich semantics.
        if s.contains(" from ") && s.contains(" on ") && !s.contains(" using ") {
            return "pkg_enrich_corpus_dialect".into();
        }
        return "pkg_enrich_semantics".into();
    }
    if s.contains("within") {
        return "pkg_within_array".into();
    }
    if s.contains(" offset ") {
        return "pkg_offset_refusal_harden".into();
    }
    if s.contains(" after ") || s.contains("after $") {
        return "pkg_cursor_after_clause".into();
    }
    if s.contains("budget")
        || family_tags
            .iter()
            .any(|t| t == "budget_coverage_damage_refusal")
    {
        return "pkg_budget_partial_coverage".into();
    }
    if s.contains("contains") || s.contains("= []") || s.contains("[]") {
        return "pkg_array_predicate_surface".into();
    }
    if s.contains("explain") {
        return "pkg_executable_explain".into();
    }
    if class == FailureClass::Wire {
        return "pkg_full_wire".into();
    }
    if class == FailureClass::Index {
        return "pkg_index_planning".into();
    }
    if class == FailureClass::Qvm {
        return "pkg_qvm_runtime".into();
    }
    if class == FailureClass::Host {
        return "pkg_host_params_bindings".into();
    }
    if class == FailureClass::Syntax {
        return "pkg_syntax_surface".into();
    }
    if class == FailureClass::Semantic {
        return "pkg_semantic_residual".into();
    }
    "pkg_compiler_misc".into()
}

/// Bind `$param` names found in source so parameterised cases are not host-false-negatives.
fn parameters_for_source(source: &str) -> Parameters {
    let mut values = BTreeMap::new();
    let bytes = source.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                end += 1;
            }
            if end > start {
                let name = &source[start..end];
                // Cursor tokens need authenticated continuation, not JSON params.
                if name != "cursor" && !values.contains_key(name) {
                    let val = match name {
                        "sender" => json!("u-0004"),
                        "st" | "status" => json!("paid"),
                        _ if name.contains("id") => json!(format!("{name}-0004")),
                        _ => json!(name),
                    };
                    values.insert(name.to_string(), val);
                }
            }
            i = end;
            continue;
        }
        i += 1;
    }
    Parameters { values }
}

fn materialise_fixture(
    root: &Path,
    generator_id: &str,
    seed: u64,
    params: &Value,
) -> Result<Value, String> {
    let script = root.join("tools/rql_q1/materialise_fixture.py");
    let params_s = serde_json::to_string(params).map_err(|e| e.to_string())?;
    let out = Command::new("python3")
        .arg(&script)
        .arg("--generator")
        .arg(generator_id)
        .arg("--seed")
        .arg(seed.to_string())
        .arg("--params")
        .arg(params_s)
        .output()
        .map_err(|e| format!("spawn materialise_fixture: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "materialise_fixture failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    serde_json::from_slice(&out.stdout).map_err(|e| format!("fixture json: {e}"))
}

fn open_heap_client(root: &Path) -> HeapClient {
    let deployment = ResidiuumDeployment::create(root).unwrap();
    let layout = HeapMetaLayout::new(root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged =
        stage_heap_genesis(&layout, dep, heap_bytes, uuid_bytes(), "heap-q2-audit").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)))
}

fn seed_collections(client: &mut HeapClient, collections: &Map<String, Value>) {
    for (name, docs) in collections {
        let mut col = client
            .create_collection(name)
            .unwrap_or_else(|e| panic!("create_collection {name}: {e}"))
            .collection;
        let arr = docs
            .as_array()
            .unwrap_or_else(|| panic!("{name} not array"));
        for doc in arr {
            let key = doc
                .get("_key")
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| panic!("{name} doc missing _key string"));
            // Strip _key from body (store key is separate); keep other fields.
            let mut body = doc.clone();
            if let Some(obj) = body.as_object_mut() {
                obj.remove("_key");
            }
            col.put(key, &body)
                .unwrap_or_else(|e| panic!("put {name}/{key}: {e}"));
        }
    }
}

/// Map collection name → default generator when the primary fixture omits it.
fn companion_generator_for(collection: &str) -> Option<&'static str> {
    match collection {
        "orders" => Some("commerce.orders_v1"),
        "customers" => Some("commerce.customers_v1"),
        "line_items" => Some("commerce.line_items_v1"),
        "products" => Some("commerce.products_v1"),
        "inventory" => Some("commerce.inventory_v1"),
        "conversations" => Some("messaging.conversations_v1"),
        "messages" => Some("messaging.messages_v1"),
        "participants" => Some("messaging.participants_v1"),
        "entries" => Some("directory.entries_v1"),
        "categories" => Some("directory.categories_v1"),
        "locations" => Some("directory.locations_v1"),
        "devices" => Some("telemetry.devices_v1"),
        "events" => Some("telemetry.events_v1"),
        "metrics" => Some("telemetry.metrics_v1"),
        "projects" => Some("project_management.projects_v1"),
        "tasks" => Some("project_management.tasks_v1"),
        "revisions" => Some("project_management.revisions_v1"),
        "memberships" => Some("project_management.memberships_v1"),
        _ => None,
    }
}

fn merge_companion_collections(
    root: &Path,
    cols: &mut Map<String, Value>,
    names: &BTreeSet<String>,
    seed: u64,
    params: &Value,
) -> Result<(), String> {
    for name in names {
        if cols.contains_key(name) {
            continue;
        }
        let Some(gen) = companion_generator_for(name) else {
            continue;
        };
        let mat = materialise_fixture(root, gen, seed, params)?;
        if let Some(obj) = mat.get("collections").and_then(|c| c.as_object()) {
            for (k, v) in obj {
                cols.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
    }
    Ok(())
}

fn ensure_named_collections(client: &mut HeapClient, names: &BTreeSet<String>) {
    let existing: BTreeSet<String> = client
        .list_collections()
        .unwrap()
        .into_iter()
        .map(|i| i.name)
        .collect();
    for n in names {
        if !existing.contains(n) {
            let _ = client
                .create_collection(n)
                .unwrap_or_else(|e| panic!("create empty {n}: {e}"));
        }
    }
}

#[test]
fn rql_q2_1_tier_a_capability_audit() {
    let root = workspace_root();
    let corpus_path = root.join("spec/rql/qualification/corpus-v1/corpus-v1.json");
    let text = fs::read_to_string(&corpus_path).expect("read corpus-v1.json");
    let doc: Value = serde_json::from_str(&text).expect("parse corpus");
    let corpus_version = doc["corpus_version"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();
    let cases = doc["cases"].as_array().expect("cases array");

    let mut case_results: Vec<Value> = Vec::new();
    let mut class_counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut package_hits: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut tier_a = 0u64;
    let mut compile_ok = 0u64;
    let mut execute_ok = 0u64;
    let mut expressible = 0u64; // compile+execute product path without error
    let mut expected_refusal_ok = 0u64;
    let mut silent_skip = 0u64;

    for case in cases {
        let tier = case["tier"].as_str().unwrap_or("");
        if tier != "A" {
            continue;
        }
        tier_a += 1;
        let case_id = case["case_id"].as_str().unwrap_or("?").to_string();
        let domain = case["domain"].as_str().unwrap_or("").to_string();
        let family_tags: Vec<String> = case["family_tags"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let capability_ids: Vec<String> = case["capability_ids"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let expected_kind = case["expected"]["kind"].as_str().unwrap_or("").to_string();
        let rql = &case["implementations"]["rql"];
        let rql_status = rql["status"].as_str().unwrap_or("").to_string();
        let source = rql["source"].as_str().unwrap_or("").to_string();
        let refusal_code = rql["refusal_code"].as_str().map(|s| s.to_string());

        let mut outcome = Map::new();
        outcome.insert("case_id".into(), json!(case_id));
        outcome.insert("tier".into(), json!("A"));
        outcome.insert("domain".into(), json!(domain));
        outcome.insert("family_tags".into(), json!(family_tags));
        outcome.insert("capability_ids".into(), json!(capability_ids));
        outcome.insert("expected_kind".into(), json!(expected_kind));
        outcome.insert("rql_status".into(), json!(rql_status));
        outcome.insert("source".into(), json!(source));

        if source.is_empty() {
            silent_skip += 1;
            outcome.insert(
                "compile".into(),
                json!({"status": "skipped", "reason": "no_source"}),
            );
            outcome.insert(
                "execute".into(),
                json!({"status": "skipped", "reason": "no_source"}),
            );
            outcome.insert(
                "failure_class".into(),
                json!(FailureClass::CorpusGap.as_str()),
            );
            outcome.insert(
                "gap_package".into(),
                json!(gap_package_id(FailureClass::CorpusGap, "", &family_tags)),
            );
            outcome.insert(
                "notes".into(),
                json!("Tier-A case has empty RQL source — corpus gap, not empty success"),
            );
            *class_counts
                .entry(FailureClass::CorpusGap.as_str().into())
                .or_default() += 1;
            package_hits
                .entry("corpus_source_gap".into())
                .or_default()
                .push(case_id);
            case_results.push(Value::Object(outcome));
            continue;
        }

        let names = extract_collection_names(&source);
        let bindings = compile_bindings_for(&names);

        // --- compile (product full path) ---
        let compile_res = compile_rql_full(&source, &bindings);
        let want_refusal = expected_kind == "stable_refusal"
            || rql_status == "stable_refusal"
            || rql_status == "deliberate_exclusion";

        match &compile_res {
            Ok(compiled) => {
                compile_ok += 1;
                outcome.insert(
                    "compile".into(),
                    json!({
                        "status": "ok",
                        "profile": compiled.profile,
                        "base_source": compiled.base_source,
                        "pipeline_len": compiled.pipeline.len(),
                        "has_project": compiled.project.is_some(),
                    }),
                );
            }
            Err(e) => {
                let msg = e.to_string();
                let class = if want_refusal {
                    FailureClass::ExpectedRefusal
                } else {
                    classify_error(&msg, &source)
                };
                outcome.insert(
                    "compile".into(),
                    json!({
                        "status": "error",
                        "message": msg,
                        "error_code": format!("{:?}", e.code()),
                    }),
                );
                if want_refusal {
                    expected_refusal_ok += 1;
                    outcome.insert(
                        "execute".into(),
                        json!({
                            "status": "skipped",
                            "reason": "expected_refusal_at_compile"
                        }),
                    );
                    outcome.insert("failure_class".into(), json!(class.as_str()));
                    outcome.insert(
                        "gap_package".into(),
                        json!(gap_package_id(class, &source, &family_tags)),
                    );
                    outcome.insert("product_expressible".into(), json!(false));
                    outcome.insert(
                        "notes".into(),
                        json!(format!(
                            "stable refusal at compile (corpus refusal_code={:?})",
                            refusal_code
                        )),
                    );
                    *class_counts.entry(class.as_str().into()).or_default() += 1;
                    package_hits
                        .entry(gap_package_id(class, &source, &family_tags))
                        .or_default()
                        .push(case_id.clone());
                    case_results.push(Value::Object(outcome));
                    continue;
                }
                // Compile failed: still record execute as skipped.
                outcome.insert(
                    "execute".into(),
                    json!({"status": "skipped", "reason": "compile_failed"}),
                );
                outcome.insert("failure_class".into(), json!(class.as_str()));
                let pkg = gap_package_id(class, &source, &family_tags);
                outcome.insert("gap_package".into(), json!(pkg));
                outcome.insert("product_expressible".into(), json!(false));
                *class_counts.entry(class.as_str().into()).or_default() += 1;
                package_hits.entry(pkg).or_default().push(case_id.clone());
                case_results.push(Value::Object(outcome));
                continue;
            }
        }

        // Compile succeeded but we wanted a refusal → capability leak / gap.
        if want_refusal {
            outcome.insert(
                "execute".into(),
                json!({
                    "status": "skipped",
                    "reason": "refusal_expected_but_compiled"
                }),
            );
            outcome.insert(
                "failure_class".into(),
                json!(FailureClass::Semantic.as_str()),
            );
            outcome.insert("gap_package".into(), json!("pkg_offset_refusal_harden"));
            outcome.insert("product_expressible".into(), json!(false));
            outcome.insert(
                "notes".into(),
                json!("expected stable refusal but compile_rql_full accepted source"),
            );
            *class_counts
                .entry(FailureClass::Semantic.as_str().into())
                .or_default() += 1;
            package_hits
                .entry("pkg_offset_refusal_harden".into())
                .or_default()
                .push(case_id);
            case_results.push(Value::Object(outcome));
            continue;
        }

        // --- execute (product path) ---
        let fixture = &case["fixture"];
        let generator_id = fixture["generator_id"].as_str().unwrap_or("");
        let seed = fixture["seed"].as_u64().unwrap_or(0);
        let mut params = fixture.get("params").cloned().unwrap_or_else(|| json!({}));
        // A continuation capability case must actually have a second page.
        // Some corpus fixtures are intentionally small/selective, so enlarge
        // only their generator cardinality for this capability audit.
        if source.contains(" after $cursor") {
            if let Some(obj) = params.as_object_mut() {
                for (key, minimum) in [("n_messages", 512_u64), ("n_events", 512_u64)] {
                    if obj
                        .get(key)
                        .and_then(Value::as_u64)
                        .is_some_and(|n| n < minimum)
                    {
                        obj.insert(key.into(), json!(minimum));
                    }
                }
            }
        }

        let exec_outcome = (|| -> Result<(u64, bool), (FailureClass, String)> {
            let mat = materialise_fixture(&root, generator_id, seed, &params)
                .map_err(|e| (FailureClass::Host, e))?;
            let mut cols = mat
                .get("collections")
                .and_then(|c| c.as_object())
                .cloned()
                .ok_or_else(|| {
                    (
                        FailureClass::Host,
                        "fixture missing collections object".into(),
                    )
                })?;
            // Enrich / multi-collection cases often name a second collection that
            // is not in the primary generator. Seed companions with the same seed.
            merge_companion_collections(&root, &mut cols, &names, seed, &params)
                .map_err(|e| (FailureClass::Host, e))?;

            let dir = tempdir().map_err(|e| (FailureClass::Host, e.to_string()))?;
            let mut client = open_heap_client(dir.path());
            seed_collections(&mut client, &cols);
            ensure_named_collections(&mut client, &names);

            let mut opts = QueryRunOptions::default();
            opts.page_size = Some(64);
            if source.contains(" after $cursor") {
                // Preserve the source page size so the first-page cursor binds
                // to exactly the same effective plan as textual resume.
                opts.page_size = None;
            }
            // Budget cases require incomplete coverage when budgets exhaust (not hard fail).
            if source.to_ascii_lowercase().contains("budget") {
                opts.coverage = CoveragePolicy::IncompleteAllowed;
            }
            let mut run_params = parameters_for_source(&source);
            if source.contains(" after $cursor") {
                let first_source = source.strip_suffix(" after $cursor").ok_or_else(|| {
                    (
                        FailureClass::Compiler,
                        "cursor clause is not terminal".into(),
                    )
                })?;
                let first = execute_rql_full(&mut client, first_source, &run_params, opts.clone())
                    .map_err(|e| (classify_error(&e.to_string(), first_source), e.to_string()))?;
                let cursor = first.base.next.ok_or_else(|| {
                    (
                        FailureClass::Semantic,
                        "cursor capability fixture did not produce a second page".into(),
                    )
                })?;
                let token = String::from_utf8(cursor.token).map_err(|_| {
                    (
                        FailureClass::Compiler,
                        "issued continuation is not representable as textual bytes".into(),
                    )
                })?;
                run_params.values.insert("cursor".into(), json!(token));
            }
            match execute_rql_full(&mut client, &source, &run_params, opts) {
                Ok(page) => {
                    let n = page.rows.len() as u64;
                    let complete = page.base.coverage.complete;
                    Ok((n, complete))
                }
                Err(e) => {
                    let msg = e.to_string();
                    Err((classify_error(&msg, &source), msg))
                }
            }
        })();

        match exec_outcome {
            Ok((row_count, coverage_complete)) => {
                execute_ok += 1;
                expressible += 1;
                outcome.insert(
                    "execute".into(),
                    json!({
                        "status": "ok",
                        "row_count": row_count,
                        "coverage_complete": coverage_complete,
                        "path": "execute_rql_full",
                    }),
                );
                outcome.insert("failure_class".into(), json!(null));
                outcome.insert("gap_package".into(), json!(null));
                outcome.insert("product_expressible".into(), json!(true));
                // Note: Q2.1 does not verify oracle equivalence (Q3).
                outcome.insert(
                    "notes".into(),
                    json!("compile+execute ok; result correctness deferred to Q3"),
                );
            }
            Err((class, msg)) => {
                outcome.insert(
                    "execute".into(),
                    json!({
                        "status": "error",
                        "message": msg,
                        "path": "execute_rql_full",
                    }),
                );
                outcome.insert("failure_class".into(), json!(class.as_str()));
                let pkg = gap_package_id(class, &source, &family_tags);
                outcome.insert("gap_package".into(), json!(pkg));
                outcome.insert("product_expressible".into(), json!(false));
                *class_counts.entry(class.as_str().into()).or_default() += 1;
                package_hits.entry(pkg).or_default().push(case_id.clone());
            }
        }

        case_results.push(Value::Object(outcome));
    }

    assert_eq!(
        silent_skip, 0,
        "Q2.1 forbids silent skips of Tier-A cases with empty handling; silent_skip={silent_skip}"
    );
    assert_eq!(
        case_results.len() as u64,
        tier_a,
        "every Tier-A case must appear in the report"
    );

    // Impact order: packages that block the most non-expressible cases first.
    let mut packages: Vec<Value> = package_hits
        .iter()
        .filter(|(k, _)| *k != "none_expected_refusal")
        .map(|(id, cases)| {
            json!({
                "package_id": id,
                "blocked_case_count": cases.len(),
                "case_ids": cases,
            })
        })
        .collect();
    packages.sort_by(|a, b| {
        let ca = a["blocked_case_count"].as_u64().unwrap_or(0);
        let cb = b["blocked_case_count"].as_u64().unwrap_or(0);
        cb.cmp(&ca).then_with(|| {
            a["package_id"]
                .as_str()
                .unwrap_or("")
                .cmp(b["package_id"].as_str().unwrap_or(""))
        })
    });

    let recommended_order: Vec<String> = packages
        .iter()
        .filter_map(|p| p["package_id"].as_str().map(|s| s.to_string()))
        .collect();

    let git_sha = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&root)
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".into());

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let report = json!({
        "format": "residiuum-rql-q2-1-capability-audit-v1",
        "profile": "rql-gate1-practical-corpus-v1",
        "corpus_version": corpus_version,
        "git_sha": git_sha,
        "generated_unix_s": now,
        "authority": {
            "programme": "doc/todo/rql/RQL_QUERY_QUALIFICATION_PROGRAM.md §5",
            "task": "Q2.1 Corpus compile/execute capability audit",
            "product_paths": [
                "compile_rql_full",
                "execute_rql_full (QVM1)"
            ],
            "non_claims": [
                "Not Q3 oracle equivalence",
                "Not Gate-1 pass",
                "Not RQL-C1 / Decision-0 close",
                "Execute success means product path returned a page; not result-correctness accept"
            ]
        },
        "summary": {
            "tier_a_cases": tier_a,
            "compile_ok": compile_ok,
            "execute_ok": execute_ok,
            "product_expressible": expressible,
            "expected_refusal_ok": expected_refusal_ok,
            "failure_class_counts": class_counts,
            "gap_case_count": tier_a.saturating_sub(expressible + expected_refusal_ok),
        },
        "recommended_implementation_package_order": recommended_order,
        "packages": packages,
        "cases": case_results,
    });

    let out_path = if std::env::var_os("RESIDIUUM_WRITE_SPEC_EVIDENCE").is_some() {
        root.join("spec/rql/qualification/corpus-v1/q2_1_capability_audit.json")
    } else {
        root.join("target/rql-q2/q2_1_capability_audit.json")
    };
    fs::create_dir_all(out_path.parent().expect("report parent"))
        .expect("create Q2 report directory");
    let pretty = serde_json::to_string_pretty(&report).expect("serialize report");
    fs::write(&out_path, pretty + "\n").expect("write gap report");

    eprintln!(
        "rql_q2_1_capability_audit: tier_a={tier_a} compile_ok={compile_ok} execute_ok={execute_ok} \
         expressible={expressible} expected_refusal_ok={expected_refusal_ok} report={}",
        out_path.display()
    );
}
