//! ATM-0.3: accepted/rejected protocol vectors and stable content-root hashes.

use residiuum_atomics::{
    decode_canonical_plan, encode_canonical_plan, plan_content_root, AtomicPlan, AtomicPlanParts,
    AtomicProfile, AtomicRefuseReason, AtomicsError, CanonicalKey, CollectionId, CoordinationScope,
    HeapId, MutationKind, PlanMutation, ResourceLimits,
};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

fn parse_hex(s: &str) -> Vec<u8> {
    assert!(s.len() % 2 == 0, "hex length must be even, got {}", s.len());
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn spec_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../spec/atomics")
        .join(name)
}

#[test]
fn accepted_vectors_roundtrip_and_match_roots() {
    let raw = fs::read_to_string(spec_path("protocol-vectors.json")).unwrap();
    let doc: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(doc["profile"], "residiuum-atomics-v1");
    let vectors = doc["vectors"].as_array().expect("vectors");
    assert!(vectors.len() >= 8, "expected the ATM-0.3 accepted set");
    for v in vectors {
        let name = v["name"].as_str().unwrap();
        let bytes = parse_hex(v["bytes_hex"].as_str().unwrap());
        let want_root = v["content_root_hex"].as_str().unwrap();
        let plan =
            decode_canonical_plan(&bytes).unwrap_or_else(|e| panic!("{name}: decode failed: {e}"));
        let again = encode_canonical_plan(&plan).unwrap();
        assert_eq!(again, bytes, "{name}: encode(decode(bytes)) != bytes");
        let root = hex(plan_content_root(&plan).unwrap().as_bytes());
        assert_eq!(root, want_root, "{name}: content root drifted");
        if name == "unknown_profile_preserved" {
            assert!(!plan.profile().execution_supported());
        } else {
            assert!(plan.profile().execution_supported(), "{name}");
        }
    }
}

#[test]
fn rejected_vectors_fail_as_documented() {
    let raw = fs::read_to_string(spec_path("rejected-vectors.json")).unwrap();
    let doc: Value = serde_json::from_str(&raw).unwrap();
    let vectors = doc["vectors"].as_array().expect("vectors");
    assert!(vectors.len() >= 5);
    for v in vectors {
        if v["stage"].as_str() == Some("close") {
            continue;
        }
        let name = v["name"].as_str().unwrap();
        let reason = v["reason"].as_str().unwrap();
        let bytes = parse_hex(v["bytes_hex"].as_str().unwrap_or(""));
        let err = decode_canonical_plan(&bytes).expect_err(name);
        match reason {
            "cbor" => assert!(matches!(err, AtomicsError::Cbor(_)), "{name}: {err:?}"),
            other => assert_eq!(err.as_str(), other, "{name}: {err:?}"),
        }
    }
}

#[test]
fn rejected_close_cases_are_documented() {
    let raw = fs::read_to_string(spec_path("rejected-vectors.json")).unwrap();
    let doc: Value = serde_json::from_str(&raw).unwrap();
    let closes: Vec<_> = doc["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| v["stage"].as_str() == Some("close"))
        .collect();
    assert!(
        closes.iter().any(|v| v["reason"] == "duplicate_target"),
        "duplicate_target close refusal must be documented"
    );
    assert!(
        closes.iter().any(|v| v["reason"] == "invalid_value"),
        "invalid_value close refusal must be documented"
    );
    assert!(
        closes
            .iter()
            .any(|v| v["name"] == "duplicate_read_identity"),
        "duplicate read identity close refusal must be documented"
    );
    assert!(
        closes.iter().any(|v| v["name"] == "reads_without_frontier"),
        "reads_without_frontier close refusal must be documented"
    );

    fn hid() -> HeapId {
        let mut b = [0u8; 16];
        b[0] = 1;
        HeapId::from_bytes(b).unwrap()
    }
    fn cid() -> CollectionId {
        let mut b = [0u8; 16];
        b[0] = 1;
        CollectionId::from_bytes(b).unwrap()
    }
    fn aid() -> residiuum_atomics::AtomicId {
        let mut b = [0u8; 32];
        b[0] = 9;
        residiuum_atomics::AtomicId::from_bytes(b).unwrap()
    }
    let mk = |val: Option<Vec<u8>>| PlanMutation {
        kind: MutationKind::Create,
        collection_id: cid(),
        key: CanonicalKey::String("same".into()),
        encoded_value: val,
        if_version: None,
    };
    let base = || AtomicPlanParts {
        profile: AtomicProfile::LocalHeapV1,
        atomic_id: aid(),
        heap_id: hid(),
        scope: CoordinationScope::LocalHeap,
        read_frontier: None,
        reads: Vec::new(),
        predicates: Vec::new(),
        mutations: Vec::new(),
        active_rule_revisions: Vec::new(),
        limits: ResourceLimits::builder_defaults_local_heap(),
    };
    let mut dup = base();
    dup.mutations = vec![mk(Some(b"1".to_vec())), mk(Some(b"2".to_vec()))];
    assert_eq!(
        AtomicPlan::close(dup).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::DuplicateTarget)
    );
    let mut bad = base();
    bad.mutations = vec![mk(None)];
    assert_eq!(
        AtomicPlan::close(bad).unwrap_err(),
        AtomicsError::Refused(AtomicRefuseReason::InvalidValue)
    );
}
