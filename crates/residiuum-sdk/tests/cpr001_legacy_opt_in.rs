//! CPR-001: legacy flat SDK is **opt-in** (`legacy-flat-sdk`); default is heap-only.
//!
//! This integration test requires `legacy-flat-sdk` so the flat surface is
//! present for claim labelling and open paths. Default profile honesty is
//! covered by unit tests in `claim.rs` and `check_heap_architecture.sh`.

use residiuum_sdk::{
    flat_collection_claim_language, heap_only_embedded_profile, legacy_flat_sdk_enabled,
    product_may_advertise_qualified_heap, Residiuum, ResidiuumDeployment,
    FLAT_COLLECTION_SURFACE_LABEL, LEGACY_FLAT_SDK_FEATURE, SDK_API_VERSION,
};
use tempfile::tempdir;

#[test]
fn legacy_flat_feature_is_opt_in_and_labelled() {
    // This test binary is built with `legacy-flat-sdk` (required-features).
    assert!(
        legacy_flat_sdk_enabled(),
        "cpr001 suite requires {LEGACY_FLAT_SDK_FEATURE}"
    );
    assert!(!heap_only_embedded_profile());
    assert!(FLAT_COLLECTION_SURFACE_LABEL.contains("CPR-001"));
    assert!(FLAT_COLLECTION_SURFACE_LABEL.contains("not residiuum-heap-v1"));
    assert!(!flat_collection_claim_language().is_empty());
    assert_eq!(SDK_API_VERSION, "1.0");
    // HP-010 claim must stay Level 1 until matrix flips.
    assert!(!product_may_advertise_qualified_heap());
}

#[test]
fn flat_open_and_deployment_host_both_available() {
    let dir = tempdir().unwrap();
    let flat_path = dir.path().join("flat.residiuum");
    let dep_path = dir.path().join("dep.residiuum");

    {
        let mut db = Residiuum::open(&flat_path).expect("legacy open");
        let mut users = db.collection("users").expect("flat collection");
        users
            .put("k", &serde_json::json!({"v": 1}))
            .expect("flat put");
    }
    let _ = Residiuum::open_compatibility(&flat_path).expect("open_compatibility");

    let deployment = Residiuum::open_deployment(&dep_path).expect("open_deployment");
    assert_eq!(deployment.root(), dep_path.as_path());
    let dep2_path = dir.path().join("dep2.residiuum");
    let _ = Residiuum::create_deployment(&dep2_path).expect("create_deployment");
    let _ = ResidiuumDeployment::open(&dep2_path).expect("reopen deployment");
}

#[test]
fn raw_store_access_documented_as_non_qualified() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("raw.residiuum");
    let db = Residiuum::open(&path).unwrap();
    let store = db.store().expect("legacy store()");
    let _ = store.store_id();
}
