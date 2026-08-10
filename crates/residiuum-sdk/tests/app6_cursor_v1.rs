//! APP-6 T1: `residiuum-cursor-v1` mint/verify + cursor_vectors lock.
//!
//! Normative: CORE plan §11; `spec/app/v1/cursor_vectors_v1.json`.
//! No product query execution claim.

use residiuum_sdk::cursor_v1::{
    mint, verify, CursorKeyRing, CursorLogical, VerifyContext, CURSOR_KEY_MATERIAL_PROFILE,
    MAC_DOMAIN, MAX_ACCEPT_AGE_SECONDS, PROFILE, SKEW_SECONDS, TTL_SECONDS,
};
use residiuum_sdk::{ErrorCode, CURSOR_PROFILE};
use serde_json::Value as JsonValue;
use std::fs;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn load_vectors() -> JsonValue {
    let path = repo_root().join("spec/app/v1/cursor_vectors_v1.json");
    let raw = fs::read_to_string(&path).expect("cursor_vectors_v1.json");
    serde_json::from_str(&raw).expect("cursor vectors json")
}

#[test]
fn profile_and_policy_constants_match_vectors() {
    let doc = load_vectors();
    assert_eq!(doc["profile"].as_str().unwrap(), PROFILE);
    assert_eq!(PROFILE, CURSOR_PROFILE);
    assert_eq!(doc["ttl_seconds"].as_u64().unwrap(), TTL_SECONDS);
    assert_eq!(doc["skew_seconds"].as_u64().unwrap(), SKEW_SECONDS);
    assert_eq!(
        doc["max_accept_age_seconds"].as_u64().unwrap(),
        MAX_ACCEPT_AGE_SECONDS
    );
    assert_eq!(MAC_DOMAIN, "residiuum:cursor-v1:mac-v1");
    assert_eq!(
        CURSOR_KEY_MATERIAL_PROFILE,
        "residiuum-cursor-key-material-v1"
    );
}

#[test]
fn vector_valid_shape_mac_locked_and_verifies() {
    let doc = load_vectors();
    let vectors = doc["vectors"].as_array().expect("vectors");
    let v0 = vectors
        .iter()
        .find(|v| v["id"] == "cursor-page1-valid-shape")
        .expect("cursor-page1-valid-shape");
    assert_eq!(
        v0["mac_status"].as_str().unwrap(),
        "locked_vector_key_material_v1"
    );
    let logical_json = &v0["token_logical"];
    let logical = CursorLogical::from_json(logical_json).expect("parse logical");
    let ring = CursorKeyRing::vector_lock();
    let expected_mac = logical.mac_hex(&ring.current.secret);
    let file_mac = logical_json["mac"].as_str().expect("mac field");
    assert_eq!(
        file_mac, expected_mac,
        "cursor vector mac must match vector-lock key material"
    );

    let cont = mint(&logical, &ring).expect("mint");
    let ctx = VerifyContext {
        heap_id: logical.heap_id.clone(),
        collection_id: logical.collection_id.clone(),
        plan_hash: Some(logical.plan_hash.clone()),
        parameter_hash: Some(logical.parameter_hash.clone()),
    };
    let got = verify(&cont.token, &ctx, &ring, logical.issued_at + 60).expect("verify");
    assert_eq!(got.heap_id, logical.heap_id);
    assert_eq!(got.collection_id, logical.collection_id);
    assert_eq!(got.remaining_limit, 900);
}

#[test]
fn vector_heap_mismatch_fail_closed() {
    let doc = load_vectors();
    let vectors = doc["vectors"].as_array().unwrap();
    let base = vectors
        .iter()
        .find(|v| v["id"] == "cursor-page1-valid-shape")
        .unwrap();
    let mut logical = CursorLogical::from_json(&base["token_logical"]).unwrap();
    let ring = CursorKeyRing::vector_lock();
    let cont = mint(&logical, &ring).unwrap();

    let heap_case = vectors
        .iter()
        .find(|v| v["id"] == "cursor-heap-mismatch")
        .unwrap();
    let bad_heap = heap_case["mutation"]["heap_id"].as_str().unwrap();
    let ctx = VerifyContext {
        heap_id: bad_heap.into(),
        collection_id: logical.collection_id.clone(),
        plan_hash: None,
        parameter_hash: None,
    };
    // Token still has original heap; context claims another → mismatch.
    let err = verify(&cont.token, &ctx, &ring, logical.issued_at + 10).unwrap_err();
    assert_eq!(err.code(), ErrorCode::ConsistencyViolation);
    assert_eq!(
        heap_case["expect"]["code"].as_str().unwrap(),
        "consistency_violation"
    );

    // Mutated token heap_id must also fail against original context after remint.
    logical.heap_id = bad_heap.into();
    let cont2 = mint(&logical, &ring).unwrap();
    let ctx_ok = VerifyContext {
        heap_id: base["token_logical"]["heap_id"].as_str().unwrap().into(),
        collection_id: base["token_logical"]["collection_id"]
            .as_str()
            .unwrap()
            .into(),
        plan_hash: None,
        parameter_hash: None,
    };
    let err2 = verify(&cont2.token, &ctx_ok, &ring, logical.issued_at + 10).unwrap_err();
    assert_eq!(err2.code(), ErrorCode::ConsistencyViolation);
}

#[test]
fn vector_expired_fail_closed() {
    let doc = load_vectors();
    let vectors = doc["vectors"].as_array().unwrap();
    let base = vectors
        .iter()
        .find(|v| v["id"] == "cursor-page1-valid-shape")
        .unwrap();
    let mut logical = CursorLogical::from_json(&base["token_logical"]).unwrap();
    let exp = vectors
        .iter()
        .find(|v| v["id"] == "cursor-expired")
        .unwrap();
    logical.issued_at = exp["mutation"]["issued_at"].as_u64().unwrap();
    logical.expires_at = exp["mutation"]["expires_at"].as_u64().unwrap();
    let ring = CursorKeyRing::vector_lock();
    let cont = mint(&logical, &ring).unwrap();
    let ctx = VerifyContext {
        heap_id: logical.heap_id.clone(),
        collection_id: logical.collection_id.clone(),
        plan_hash: None,
        parameter_hash: None,
    };
    // now far beyond max accept age
    let err = verify(&cont.token, &ctx, &ring, 10_000).unwrap_err();
    assert_eq!(err.code(), ErrorCode::ConsistencyViolation);
}
