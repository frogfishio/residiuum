//! HAR-4 dep (query product): gate locks for qualified remote + op 118.
//!
//! HAR-4 T2: product default is qualified HeapKey; legacy requires explicit
//! opt-in. APP-7 T6 activated op 118 in the registry (package accept still open).

use residiuum_server::{
    request_registry_allows, validate_qualified_listener, ResidentHeapRegistry, ServeOptions,
};
use std::sync::Arc;

#[test]
fn op_118_rql_query_is_active() {
    // APP-7 T6: wire registry active. Package accept remains principal-gated.
    assert!(
        request_registry_allows(118),
        "op 118 rql_query must be active after APP-7 T6"
    );
}

#[test]
fn serve_options_product_default_is_qualified_heap_key() {
    // HAR-4 T2: HeapKey is the product ServeOptions default.
    let opts = ServeOptions::default();
    assert!(
        opts.qualified_heap_key,
        "HAR-4 T2: default serve must be qualified_heap_key=true"
    );
    assert!(
        !opts.legacy_token_server,
        "product default is not legacy_token_server"
    );
}

#[test]
fn legacy_token_server_clears_qualified_and_labels_path() {
    let opts = ServeOptions::new().legacy_token_server();
    assert!(!opts.qualified_heap_key);
    assert!(opts.legacy_token_server);
}

#[test]
fn qualified_and_legacy_flags_are_mutually_exclusive_at_api() {
    // Builder: enabling qualified clears legacy.
    let opts = ServeOptions::new()
        .legacy_token_server()
        .qualified_heap_key(true);
    assert!(opts.qualified_heap_key);
    assert!(!opts.legacy_token_server);

    // Setting both true is possible only by field mutation; serve_store_with
    // must refuse. Simulate via explicit field set after builders.
    let mut bad = ServeOptions::new().legacy_token_server();
    bad.qualified_heap_key = true;
    assert!(bad.qualified_heap_key && bad.legacy_token_server);
}

#[test]
fn qualified_listener_requires_tls_and_forbids_token() {
    let reg = Arc::new(ResidentHeapRegistry::default());
    let deployment = "00000000-0000-4000-8000-000000000001";

    assert!(
        validate_qualified_listener(false, None, false, Some(&reg), Some(deployment)).is_err(),
        "TLS required"
    );
    assert!(
        validate_qualified_listener(
            true,
            Some("shared-token"),
            false,
            Some(&reg),
            Some(deployment)
        )
        .is_err(),
        "shared token forbidden on qualified path"
    );
    assert!(
        validate_qualified_listener(true, None, true, Some(&reg), Some(deployment)).is_err(),
        "diagnostic line protocol forbidden"
    );
    assert!(
        validate_qualified_listener(true, None, false, None, Some(deployment)).is_err(),
        "registry required"
    );
    assert!(
        validate_qualified_listener(true, None, false, Some(&reg), None).is_err(),
        "deployment_id required"
    );
    assert!(
        validate_qualified_listener(true, None, false, Some(&reg), Some(deployment)).is_ok(),
        "minimal qualified posture must validate"
    );
}

#[test]
fn serve_store_with_refuses_co_host_qualified_and_legacy() {
    use residiuum_server::serve_store_with;
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let path = dir.path().join("app.residiuum");
    residiuum_store::Store::open(&path).unwrap();

    let mut opts = ServeOptions::new().legacy_token_server();
    // Force both flags (builder normally clears legacy when enabling qualified).
    opts.qualified_heap_key = true;
    let err = serve_store_with(&path, "127.0.0.1:0", opts).expect_err("co-host must fail");
    let msg = format!("{err}");
    assert!(
        msg.contains("co-host") || msg.contains("legacy"),
        "err={msg}"
    );
}

#[test]
fn baseline_ops_json_marks_118_active() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../spec/app/baseline-v1/operations-v1.json");
    let raw = std::fs::read_to_string(&path).expect("operations-v1.json");
    let v: serde_json::Value = serde_json::from_str(&raw).expect("json");
    // apb.collection.rql wire 118 must be active with schemas.
    let ops = v["operations"].as_array().expect("operations array");
    let rql = ops
        .iter()
        .find(|o| o["app_id"] == "apb.collection.rql")
        .expect("apb.collection.rql");
    let wire = rql["wire"]
        .as_array()
        .and_then(|a| a.iter().find(|w| w["id"] == 118))
        .expect("wire 118");
    assert_eq!(wire["wire_status"], "active");
    assert!(
        wire["request_schema"]
            .as_str()
            .unwrap_or("")
            .contains("rql_query"),
        "schema link required"
    );
}
