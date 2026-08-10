//! APP-5: `rql-app-core-v1` conformance corpus (accept + reject).
//!
//! Normative: CORE plan §9 / §14 APP-5; `spec/app/v1/rql_app_core_corpus_v1.json`.
//! Also re-checks `plan_vectors_v1.json` `source_rql` → plan hash (APP-4 lock).

use residiuum_heap::CollectionId;
use residiuum_sdk::app_v1::{ConsistencyMode, CoveragePolicy};
use residiuum_sdk::plan_v1::{CollectionBindings, NullsOrder, OrderDir, PLAN_HASH_DOMAIN};
use residiuum_sdk::rql_app_core::{
    compile_app_core, APP_CORE_PROFILE, DIAG_RQL_FEATURE_UNAVAILABLE,
};
use residiuum_sdk::{Error, PLAN_ENCODING_PROFILE, PLAN_PROFILE, PREDICATE_PROFILE};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn read_json(rel: &str) -> Value {
    let path = workspace_root().join(rel);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("json {}: {e}", path.display()))
}

fn bindings_from(map: &Value) -> CollectionBindings {
    let mut b = CollectionBindings::default();
    let obj = map.as_object().expect("default_bindings object");
    for (name, id) in obj {
        let s = id.as_str().unwrap_or_else(|| panic!("binding {name} id"));
        b.bind(name, CollectionId::from_str(s).expect("collection id"));
    }
    b
}

#[test]
fn app5_profiles() {
    assert_eq!(APP_CORE_PROFILE, "rql-app-core-v1");
    assert_eq!(PLAN_PROFILE, "rql-plan-v1");
    assert_eq!(PLAN_ENCODING_PROFILE, "rql-plan-encoding-v1");
    assert_eq!(PLAN_HASH_DOMAIN, "residiuum:rql-plan-v1:canonical-v1");
    assert_eq!(PREDICATE_PROFILE, "residiuum-predicate-v1");
    assert_eq!(DIAG_RQL_FEATURE_UNAVAILABLE, "rql_feature_unavailable");
}

#[test]
fn app5_corpus_accept_and_reject() {
    let doc = read_json("spec/app/v1/rql_app_core_corpus_v1.json");
    assert_eq!(doc["profile"].as_str(), Some(APP_CORE_PROFILE));
    assert_eq!(doc["hash_domain"].as_str(), Some(PLAN_HASH_DOMAIN));

    let bindings = bindings_from(&doc["default_bindings"]);

    let accept = doc["accept"].as_array().expect("accept array");
    assert!(
        accept.len() >= 5,
        "corpus must retain a useful accept set (got {})",
        accept.len()
    );
    for v in accept {
        let id = v["id"].as_str().unwrap_or("?");
        let src = v["source_rql"]
            .as_str()
            .unwrap_or_else(|| panic!("{id}: source_rql"));
        let compiled = compile_app_core(src, &bindings)
            .unwrap_or_else(|e| panic!("{id}: expected accept, got error: {e}\n source: {src}"));
        assert_eq!(compiled.profile, APP_CORE_PROFILE, "{id}");

        if let Some(hex) = v.get("plan_hash_hex").and_then(|h| h.as_str()) {
            assert_eq!(
                compiled.plan.plan_hash_hex(),
                hex,
                "{id}: plan_hash_hex locked"
            );
        }

        if let Some(exp) = v.get("expect") {
            if let Some(ps) = exp.get("page_size").and_then(|x| x.as_u64()) {
                assert_eq!(u64::from(compiled.plan.page_size), ps, "{id}: page_size");
            }
            if let Some(lim) = exp.get("limit") {
                if lim.is_null() {
                    assert!(compiled.plan.limit.is_none(), "{id}: limit null");
                } else if let Some(n) = lim.as_u64() {
                    assert_eq!(compiled.plan.limit, Some(n), "{id}: limit");
                }
            }
            if let Some(ex) = exp.get("explain").and_then(|x| x.as_bool()) {
                assert_eq!(compiled.explain, ex, "{id}: explain");
            }
            if let Some(sn) = exp.get("source_name").and_then(|x| x.as_str()) {
                assert_eq!(compiled.plan.from.source_name, sn, "{id}: source_name");
            }
            if let Some(c) = exp.get("coverage").and_then(|x| x.as_str()) {
                match c {
                    "complete" => assert!(
                        matches!(compiled.plan.coverage, CoveragePolicy::Complete),
                        "{id}: coverage complete"
                    ),
                    "incomplete_allowed" | "incomplete" => assert!(
                        matches!(compiled.plan.coverage, CoveragePolicy::IncompleteAllowed),
                        "{id}: coverage incomplete_allowed"
                    ),
                    other => panic!("{id}: unknown coverage expect `{other}`"),
                }
            }
            if let Some(c) = exp.get("consistency").and_then(|x| x.as_str()) {
                match c {
                    "available" => assert!(
                        matches!(compiled.plan.consistency, ConsistencyMode::Available),
                        "{id}: consistency available"
                    ),
                    "current" => assert!(
                        matches!(compiled.plan.consistency, ConsistencyMode::Current),
                        "{id}: consistency current"
                    ),
                    other => panic!("{id}: unknown consistency expect `{other}`"),
                }
            }
            if let Some(n) = exp.get("first_order_nulls").and_then(|x| x.as_str()) {
                let term = compiled
                    .plan
                    .order
                    .first()
                    .unwrap_or_else(|| panic!("{id}: missing order"));
                match n {
                    "first" => assert_eq!(term.nulls, NullsOrder::First, "{id}"),
                    "last" => assert_eq!(term.nulls, NullsOrder::Last, "{id}"),
                    other => panic!("{id}: bad first_order_nulls `{other}`"),
                }
            }
            if let Some(d) = exp.get("first_order_dir").and_then(|x| x.as_str()) {
                let term = compiled.plan.order.first().expect("order");
                match d {
                    "asc" => assert_eq!(term.dir, OrderDir::Asc, "{id}"),
                    "desc" => assert_eq!(term.dir, OrderDir::Desc, "{id}"),
                    other => panic!("{id}: bad first_order_dir `{other}`"),
                }
            }
            if let Some(n) = exp.get("budget_documents").and_then(|x| x.as_u64()) {
                let b = compiled
                    .budget
                    .as_ref()
                    .unwrap_or_else(|| panic!("{id}: expected budget"));
                assert_eq!(b.max_documents, Some(n), "{id}: budget_documents");
            }
            if let Some(n) = exp.get("budget_bytes").and_then(|x| x.as_u64()) {
                let b = compiled.budget.as_ref().expect("budget");
                assert_eq!(b.max_bytes, Some(n), "{id}: budget_bytes");
            }
            if let Some(n) = exp.get("budget_result_bytes").and_then(|x| x.as_u64()) {
                let b = compiled.budget.as_ref().expect("budget");
                assert_eq!(b.max_result_bytes, Some(n), "{id}: budget_result_bytes");
            }
        }
    }

    let reject = doc["reject"].as_array().expect("reject array");
    assert!(
        reject.len() >= 8,
        "corpus must retain a useful reject matrix (got {})",
        reject.len()
    );
    for v in reject {
        let id = v["id"].as_str().unwrap_or("?");
        let src = v["source_rql"]
            .as_str()
            .unwrap_or_else(|| panic!("{id}: source_rql"));
        let err = compile_app_core(src, &bindings).err().unwrap_or_else(|| {
            panic!("{id}: expected reject, but compile succeeded\n source: {src}")
        });
        assert!(
            matches!(err, Error::QueryInvalid(_)),
            "{id}: expected QueryInvalid, got {err:?}"
        );
        let msg = err.to_string();
        let needles = v["diagnostic_contains"]
            .as_array()
            .expect("diagnostic_contains array");
        for n in needles {
            let s = n.as_str().expect("needle string");
            assert!(
                msg.to_ascii_lowercase().contains(&s.to_ascii_lowercase()),
                "{id}: error `{msg}` missing diagnostic `{s}`"
            );
        }
    }
}

#[test]
fn app5_plan_vectors_source_rql_hashes() {
    // APP-4 locked vectors that include source_rql must compile identically under APP-5.
    let doc = read_json("spec/app/v1/plan_vectors_v1.json");
    let mut bindings = CollectionBindings::default();
    // Collection id used by plan_vectors fixtures.
    bindings.bind(
        "orders",
        CollectionId::from_str("00000000-0000-4000-8000-0000000000a1").unwrap(),
    );

    let vectors = doc["vectors"].as_array().expect("vectors");
    let mut checked = 0usize;
    for v in vectors {
        let id = v["id"].as_str().unwrap_or("?");
        if v.get("expect").is_some() {
            let src = match v.get("source_rql").and_then(|s| s.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let err = compile_app_core(src, &bindings).unwrap_err();
            let msg = err.to_string();
            let diag = v["expect"]
                .get("diagnostic")
                .and_then(|d| d.as_str())
                .unwrap_or(DIAG_RQL_FEATURE_UNAVAILABLE);
            assert!(
                msg.contains(diag),
                "{id}: reject vector missing diagnostic `{diag}` in `{msg}`"
            );
            checked += 1;
            continue;
        }
        let src = match v.get("source_rql").and_then(|s| s.as_str()) {
            Some(s) => s,
            None => continue,
        };
        let hex = v["plan_hash_hex"]
            .as_str()
            .unwrap_or_else(|| panic!("{id}: plan_hash_hex"));
        let compiled = compile_app_core(src, &bindings)
            .unwrap_or_else(|e| panic!("{id}: compile source_rql: {e}"));
        assert_eq!(
            compiled.plan.plan_hash_hex(),
            hex,
            "{id}: APP-5 source_rql must match locked APP-4 plan hash"
        );
        checked += 1;
    }
    assert!(
        checked >= 2,
        "expected to check plan_vectors source_rql cases, checked {checked}"
    );
}
