//! APP-0 contract lock: error map, plan/cursor vectors, wire fixtures, compile surface.
//!
//! Normative: `doc/todo/application-baseline/CORE_APPLICATION_API_IMPLEMENTATION_PLAN.md` §12–§14 APP-0.

use residiuum_heap::{CollectionId, HeapId};
use residiuum_sdk::{
    AppQueryBudget, CollectionClient, ConsistencyMode, CoveragePolicy, ErrorCode, HeapClient,
    Parameters, QueryRunOptions, CURSOR_PROFILE, PREDICATE_PROFILE, RQL_APP_CORE_PROFILE,
    RQL_PLAN_PROFILE, RUST_APP_PROFILE,
};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

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

fn must_exist(rel: &str) {
    let path = workspace_root().join(rel);
    assert!(
        path.is_file(),
        "missing required APP-0 artifact {}",
        path.display()
    );
}

#[test]
fn app0_wire_artifacts_present() {
    for rel in [
        "spec/heap/rpc-v1/collection_create.request.json",
        "spec/heap/rpc-v1/collection_create.response.json",
        "spec/heap/rpc-v1/rql_query.request.json",
        "spec/heap/rpc-v1/rql_query.response.json",
        "spec/heap/fixtures/collection_create.accepted.json",
        "spec/heap/fixtures/collection_create.rejected.json",
        "spec/heap/fixtures/rql_query.accepted.json",
        "spec/heap/fixtures/rql_query.rejected.json",
        "spec/app/v1/error_mapping_v1.json",
        "spec/app/v1/plan_vectors_v1.json",
        "spec/app/v1/cursor_vectors_v1.json",
        "spec/app/v1/README.md",
    ] {
        must_exist(rel);
        if rel.ends_with(".json") {
            let _ = read_json(rel);
        }
    }
}

#[test]
fn app0_ops_106_and_118_active() {
    let ops = read_json("spec/heap/operations-v1.json");
    let list = ops["operations"].as_array().expect("operations array");
    let op106 = list
        .iter()
        .find(|o| o["id"].as_u64() == Some(106))
        .expect("missing op 106");
    assert_eq!(
        op106["status"].as_str(),
        Some("active"),
        "APP-1 activates 106"
    );
    assert!(
        op106["request_schema"].is_string() && op106["response_schema"].is_string(),
        "active op 106 must have schema pointers"
    );
    let op118 = list
        .iter()
        .find(|o| o["id"].as_u64() == Some(118))
        .expect("missing op 118");
    assert_eq!(
        op118["status"].as_str(),
        Some("active"),
        "APP-7 activates op 118"
    );
    assert!(
        op118["request_schema"].is_string() && op118["response_schema"].is_string(),
        "active op 118 must have schema pointers"
    );
}

#[test]
fn app0_error_mapping_codes_are_sdk_codes() {
    let map = read_json("spec/app/v1/error_mapping_v1.json");
    let required = map["required_error_codes"]
        .as_array()
        .expect("required_error_codes");
    let known: Vec<&str> = [
        ErrorCode::AlreadyExists,
        ErrorCode::NotFound,
        ErrorCode::VersionConflict,
        ErrorCode::ValidationFailed,
        ErrorCode::QueryInvalid,
        ErrorCode::QueryBudgetRequired,
        ErrorCode::ResourceLimit,
        ErrorCode::CoverageIncomplete,
        ErrorCode::PartitionUnavailable,
        ErrorCode::StaleRoute,
        ErrorCode::ConsistencyViolation,
        ErrorCode::DurabilityUnavailable,
        ErrorCode::DataDamaged,
        ErrorCode::PayloadPartial,
        ErrorCode::FormatUnsupported,
        ErrorCode::TypeMismatch,
        ErrorCode::AuthenticationFailed,
        ErrorCode::PermissionDenied,
        ErrorCode::DeadlineExceeded,
        ErrorCode::Io,
        ErrorCode::ProtocolViolation,
        ErrorCode::WriterLockHeld,
        ErrorCode::Internal,
    ]
    .into_iter()
    .map(|c| c.as_str())
    .collect();

    for code in required {
        let s = code.as_str().expect("code string");
        assert!(
            known.contains(&s),
            "error_mapping required code {s} missing from ErrorCode::as_str"
        );
    }

    // Spot-check CORE plan §12 mappings.
    let mappings = map["mappings"].as_array().expect("mappings");
    let mut found_feature = false;
    for m in mappings {
        if m["diagnostic"].as_str() == Some("rql_feature_unavailable") {
            found_feature = true;
            assert_eq!(m["code"].as_str(), Some("query_invalid"));
        }
    }
    assert!(
        found_feature,
        "must map unsupported RQL feature → query_invalid + rql_feature_unavailable"
    );
}

#[test]
fn app0_plan_and_cursor_vectors_shaped() {
    let plans = read_json("spec/app/v1/plan_vectors_v1.json");
    assert_eq!(plans["profile"].as_str(), Some("rql-plan-v1"));
    let vectors = plans["vectors"].as_array().expect("plan vectors");
    assert!(vectors.len() >= 3, "need at least three plan vectors");

    let cursors = read_json("spec/app/v1/cursor_vectors_v1.json");
    assert_eq!(cursors["profile"].as_str(), Some("residiuum-cursor-v1"));
    let fields = cursors["fields_required"].as_array().expect("fields");
    assert!(fields.iter().any(|f| f.as_str() == Some("plan_hash")));
    assert!(fields.iter().any(|f| f.as_str() == Some("mac")));
    let cv = cursors["vectors"].as_array().expect("cursor vectors");
    assert!(cv.len() >= 2);
}

#[test]
fn app0_fixtures_match_op_ids() {
    let create = read_json("spec/heap/fixtures/collection_create.accepted.json");
    assert_eq!(create["op"].as_u64(), Some(106));
    assert_eq!(create["ok"].as_bool(), Some(true));
    assert!(create["result"]["receipt"]["operation"].as_str() == Some("create_collection"));

    let create_rej = read_json("spec/heap/fixtures/collection_create.rejected.json");
    assert_eq!(create_rej["op"].as_u64(), Some(106));
    assert_eq!(create_rej["ok"].as_bool(), Some(false));

    let rql = read_json("spec/heap/fixtures/rql_query.accepted.json");
    assert_eq!(rql["op"].as_u64(), Some(118));
    assert_eq!(rql["ok"].as_bool(), Some(true));
    assert!(rql["result"]["exhausted"].as_bool().is_some());

    let rql_rej = read_json("spec/heap/fixtures/rql_query.rejected.json");
    assert_eq!(
        rql_rej["error"]["diagnostic"].as_str(),
        Some("rql_feature_unavailable")
    );
}

fn v4(seed: u8) -> [u8; 16] {
    let mut b = [seed; 16];
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    b
}

#[test]
fn app0_rust_compile_surface() {
    assert_eq!(RUST_APP_PROFILE, "residiuum-rust-app-v1");
    assert_eq!(RQL_APP_CORE_PROFILE, "rql-app-core-v1");
    assert_eq!(RQL_PLAN_PROFILE, "rql-plan-v1");
    assert_eq!(CURSOR_PROFILE, "residiuum-cursor-v1");
    assert_eq!(PREDICATE_PROFILE, "residiuum-predicate-v1");

    let hid = HeapId::from_bytes(v4(0x11)).unwrap();
    let cid = CollectionId::from_bytes(v4(0x22)).unwrap();
    let heap = HeapClient::from_id_for_contract(hid);
    assert_eq!(heap.id(), hid);
    let mut col = CollectionClient::from_parts_for_contract(hid, cid, "orders");
    assert_eq!(col.name(), "orders");

    let params = Parameters::default();
    let opts = QueryRunOptions {
        page_size: Some(100),
        coverage: CoveragePolicy::Complete,
        consistency: ConsistencyMode::Available,
        budget: Some(AppQueryBudget::documents(50_000)),
        explain: false,
        after: None,
        deadline: None,
        cancel: None,
        diagnostics: false,
    };
    let err = col.rql("from orders", &params, opts).unwrap_err();
    assert_eq!(err.code(), ErrorCode::Internal);

    // Touch workspace Path so unused import stays honest if layout moves.
    assert!(Path::new(&workspace_root())
        .join("doc/todo/application-baseline/CORE_APPLICATION_API_IMPLEMENTATION_PLAN.md")
        .is_file());
}
