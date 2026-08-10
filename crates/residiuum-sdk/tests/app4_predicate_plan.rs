//! APP-4: `residiuum-predicate-v1` + `rql-plan-v1` canonical encoding/hash lock.
//!
//! Normative: CORE plan §14 APP-4; `spec/app/v1/plan_vectors_v1.json`.

use residiuum_heap::CollectionId;
use residiuum_sdk::plan_v1::{
    CollectionBindings, OrderDir, PlanBuilder, RqlPlanV1, PLAN_ENCODING_PROFILE, PLAN_HASH_DOMAIN,
    PLAN_PROFILE,
};
use residiuum_sdk::predicate::{field, param, Predicate};
use residiuum_sdk::{CoveragePolicy, PREDICATE_PROFILE};
use serde_json::Value;
use std::collections::BTreeMap;
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

#[test]
fn app4_profiles_and_encoding_ids() {
    assert_eq!(PLAN_PROFILE, "rql-plan-v1");
    assert_eq!(PLAN_ENCODING_PROFILE, "rql-plan-encoding-v1");
    assert_eq!(PLAN_HASH_DOMAIN, "residiuum:rql-plan-v1:canonical-v1");
    assert_eq!(PREDICATE_PROFILE, "residiuum-predicate-v1");
}

#[test]
fn app4_plan_vectors_hashes_match_builder() {
    let doc = read_json("spec/app/v1/plan_vectors_v1.json");
    assert_eq!(
        doc["hash_domain"].as_str(),
        Some(PLAN_HASH_DOMAIN),
        "fixture hash_domain must match PLAN_HASH_DOMAIN"
    );
    assert_eq!(
        doc["encoding_profile"].as_str(),
        Some(PLAN_ENCODING_PROFILE)
    );
    assert_eq!(doc["hash_algorithm"].as_str(), Some("blake3-256"));

    let vectors = doc["vectors"].as_array().expect("vectors");
    for v in vectors {
        let id = v["id"].as_str().unwrap_or("?");
        if v.get("expect").is_some() {
            // reject vectors are APP-5 compiler work
            continue;
        }
        let logical = v
            .get("logical_plan")
            .expect("logical_plan on non-reject vector");
        let from_fixture = RqlPlanV1::from_plan_vector_json(logical)
            .unwrap_or_else(|e| panic!("{id}: parse fixture: {e}"));
        let expected_hex = v["plan_hash_hex"]
            .as_str()
            .unwrap_or_else(|| panic!("{id}: plan_hash_hex"));
        assert_eq!(
            v["plan_hash_status"].as_str(),
            Some("canonical_v1"),
            "{id}: hash status"
        );
        assert_eq!(
            from_fixture.plan_hash_hex(),
            expected_hex,
            "{id}: fixture parse hash"
        );

        // Builder path for the two known compile vectors.
        if id == "plan-orders-status-open-page" {
            let cid = CollectionId::from_str("00000000-0000-4000-8000-0000000000a1").unwrap();
            let mut bindings = CollectionBindings::default();
            bindings.bind("orders", cid);
            let built = PlanBuilder::from_source("orders")
                .where_(field("status").unwrap().eq(param("status")))
                .project(["id", "status"])
                .unwrap()
                .order_by("created_at", OrderDir::Desc)
                .unwrap()
                .limit(1000)
                .page_size(100)
                .unwrap()
                .coverage(CoveragePolicy::Complete)
                .compile(&bindings)
                .unwrap();
            assert_eq!(built.plan_hash_hex(), expected_hex, "{id}: builder hash");
            assert_eq!(
                built.plan_hash(),
                from_fixture.plan_hash(),
                "{id}: builder vs fixture"
            );
        }
        if id == "plan-defaults-only" {
            let cid = CollectionId::from_str("00000000-0000-4000-8000-0000000000a1").unwrap();
            let mut bindings = CollectionBindings::default();
            bindings.bind("orders", cid);
            let built = PlanBuilder::from_source("orders")
                .compile(&bindings)
                .unwrap();
            assert_eq!(built.plan_hash_hex(), expected_hex, "{id}: builder hash");
        }
    }
}

#[test]
fn app4_predicate_totality_model() {
    // Absent != value is false (Residiuum §6.2) — differs from legacy Filter::Ne.
    let p = field("gone").unwrap().ne(param("x"));
    let doc = serde_json::json!({"status": "ok"});
    let mut params = BTreeMap::new();
    params.insert("x".into(), serde_json::json!("x"));
    assert!(!p.eval(&doc, &params).unwrap());

    let p_eq = Predicate::field_eq_param(
        residiuum_sdk::PredPath::parse_dotted("status").unwrap(),
        "status",
    );
    params.insert("status".into(), serde_json::json!("ok"));
    assert!(p_eq.eval(&doc, &params).unwrap());
}

#[test]
fn app4_name_binding_required() {
    let err = PlanBuilder::from_source("missing")
        .compile(&CollectionBindings::default())
        .unwrap_err();
    assert!(
        err.to_string().contains("unknown collection")
            || matches!(err, residiuum_sdk::Error::QueryInvalid(_))
    );
}
