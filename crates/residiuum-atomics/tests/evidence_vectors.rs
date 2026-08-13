//! ATM-0.7: durable evidence codecs, hashes, and hostile nested maps.

use residiuum_atomics::{
    decision_hash, decode_decision, decode_member, decode_prepare, decode_tombstone,
    encode_decision, encode_member, encode_prepare, encode_tombstone, member_hash,
    ordered_member_manifest_root, prepare_hash, tombstone_hash, AtomicAbortReason, AtomicId,
    AtomicMember, AtomicOutcome, AtomicPrepare, AtomicsError, CanonicalKey, CollectionId,
    ContentRoot, CoordinationScope, HeapId, MutationKind, ObjectIdentity, ResourceLimits,
    VersionId,
};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

fn parse_hex(s: &str) -> Vec<u8> {
    assert!(s.len() % 2 == 0, "hex length must be even");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn spec_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spec/atomics/evidence-vectors.json")
}

fn aid() -> AtomicId {
    let mut b = [0u8; 32];
    b[0] = 9;
    AtomicId::from_bytes(b).unwrap()
}

#[test]
fn evidence_vectors_roundtrip_and_match_hashes() {
    let raw = fs::read_to_string(spec_path()).unwrap();
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/atomics-evidence/atm-0");
    let _ = fs::create_dir_all(&dir);
    let _ = fs::write(dir.join("evidence-vectors.json"), &raw);
    let doc: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(doc["profile"], "residiuum-atomics-v1");
    let accepted = doc["accepted"].as_array().expect("accepted");
    assert!(
        accepted.len() >= 6,
        "expected prepare/member/decision/tombstone set"
    );
    for v in accepted {
        let name = v["name"].as_str().unwrap();
        let record = v["record"].as_str().unwrap();
        let bytes = parse_hex(v["bytes_hex"].as_str().unwrap());
        let want = v["hash_hex"].as_str().unwrap();
        let again;
        let got;
        match record {
            "prepare" => {
                let rec = decode_prepare(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
                again = encode_prepare(&rec).unwrap();
                got = hex(&prepare_hash(&rec).unwrap());
            }
            "member" => {
                let rec = decode_member(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
                assert_eq!(rec.object_identity.key, CanonicalKey::String("k".into()));
                again = encode_member(&rec).unwrap();
                got = hex(&member_hash(&rec).unwrap());
            }
            "decision" => {
                let rec = decode_decision(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
                if name.contains("not_committed") {
                    match rec.not_committed_outcome().unwrap() {
                        AtomicOutcome::NotCommitted { reason, .. } => {
                            assert_eq!(reason, AtomicAbortReason::PreconditionConflict);
                        }
                        other => panic!("{name}: {other:?}"),
                    }
                } else {
                    assert!(rec.abort_reason.is_none());
                    assert!(rec.commit_position.is_some());
                }
                again = encode_decision(&rec).unwrap();
                got = hex(&decision_hash(&rec).unwrap());
            }
            "tombstone" => {
                let rec = decode_tombstone(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
                if name.contains("not_committed") {
                    match rec.not_committed_outcome().unwrap() {
                        AtomicOutcome::NotCommitted { reason, .. } => {
                            assert_eq!(reason, AtomicAbortReason::PreconditionConflict);
                        }
                        other => panic!("{name}: {other:?}"),
                    }
                }
                again = encode_tombstone(&rec).unwrap();
                got = hex(&tombstone_hash(&rec).unwrap());
            }
            other => panic!("unknown record {other}"),
        }
        assert_eq!(again, bytes, "{name}: encode(decode(bytes)) != bytes");
        assert_eq!(got, want, "{name}: evidence hash drifted");
    }
}

#[test]
fn evidence_rejected_vectors_fail_as_documented() {
    let raw = fs::read_to_string(spec_path()).unwrap();
    let doc: Value = serde_json::from_str(&raw).unwrap();
    for v in doc["rejected"].as_array().expect("rejected") {
        let name = v["name"].as_str().unwrap();
        let record = v["record"].as_str().unwrap();
        let reason = v["reason"].as_str().unwrap();
        let bytes = parse_hex(v["bytes_hex"].as_str().unwrap_or(""));
        let err = match record {
            "prepare" => decode_prepare(&bytes).expect_err(name),
            "member" => decode_member(&bytes).expect_err(name),
            "decision" => decode_decision(&bytes).expect_err(name),
            "tombstone" => decode_tombstone(&bytes).expect_err(name),
            other => panic!("unknown record {other}"),
        };
        match reason {
            "cbor" => assert!(matches!(err, AtomicsError::Cbor(_)), "{name}: {err:?}"),
            other => assert_eq!(err.as_str(), other, "{name}: {err:?}"),
        }
    }
}

#[test]
fn member_identity_changes_manifest_root() {
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
    fn vid() -> VersionId {
        let mut b = [0u8; 16];
        b[0] = 3;
        VersionId::from_bytes(b).unwrap()
    }
    let mk = |key: &str| AtomicMember {
        atomic_id: aid(),
        ordinal: 0,
        object_identity: ObjectIdentity::new(cid(), CanonicalKey::String(key.into())),
        member_kind: MutationKind::Create,
        before_version: None,
        after_content_hash: Some([8u8; 32]),
        event_id: vid(),
    };
    let a = ordered_member_manifest_root(hid(), &[mk("alpha")]).unwrap();
    let b = ordered_member_manifest_root(hid(), &[mk("beta")]).unwrap();
    assert_ne!(a, b);
    let _ = AtomicPrepare {
        atomic_id: aid(),
        heap_id: hid(),
        scope: CoordinationScope::LocalHeap,
        content_root: ContentRoot::from_bytes([7u8; 32]).unwrap(),
        frontier: [1u8; 32],
        ordered_member_manifest_root: a,
        read_set_root: [3u8; 32],
        predicate_set_root: [4u8; 32],
        active_rule_revision_root: [5u8; 32],
        limits: ResourceLimits::builder_defaults_local_heap(),
    };
}
