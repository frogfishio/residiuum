//! ATM-0.6: assemble `target/atomics-evidence/atm-0/` and write `manifest.json`.
//!
//! This freezes the semantic/byte contract. Later field changes need new
//! fixtures and an explicit compatibility decision.

use residiuum_atomics::MAX_CBOR_DEPTH;
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn spec_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spec/atomics")
}

fn evidence_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/atomics-evidence/atm-0")
}

fn blake3_hex(bytes: &[u8]) -> String {
    hex(blake3::hash(bytes).as_bytes())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn rustc_version() -> String {
    Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn write(dir: &Path, name: &str, body: &str) -> String {
    let path = dir.join(name);
    fs::write(&path, body).unwrap();
    blake3_hex(body.as_bytes())
}

#[test]
fn writes_atm0_evidence_manifest() {
    let spec = spec_dir();
    let proto = fs::read_to_string(spec.join("protocol-vectors.json")).unwrap();
    let rejected = fs::read_to_string(spec.join("rejected-vectors.json")).unwrap();
    let hostile = fs::read_to_string(spec.join("hostile-corpus.json")).unwrap();
    let cbor_spec = fs::read_to_string(spec.join("cbor-v1.json")).unwrap();

    let proto_doc: Value = serde_json::from_str(&proto).unwrap();
    let rejected_doc: Value = serde_json::from_str(&rejected).unwrap();
    let hostile_doc: Value = serde_json::from_str(&hostile).unwrap();
    assert_eq!(proto_doc["profile"], "residiuum-atomics-v1");
    assert!(proto_doc["vectors"].as_array().unwrap().len() >= 8);
    assert!(rejected_doc["vectors"].as_array().unwrap().len() >= 5);
    assert_eq!(hostile_doc["max_cbor_depth"], MAX_CBOR_DEPTH);

    let property = json!({
        "profile": "residiuum-atomics-v1",
        "properties": {
            "equivalent_builder_order_same_bytes_and_root": true,
            "semantic_change_changes_root": true,
            "heap_or_collection_substitution_changes_root": true,
            "same_id_same_root_replays": true,
            "same_id_different_root_conflicts": true,
            "oracle_never_partially_visible": true,
            "unknown_profile_preserved_by_decode_refused_by_execution": true,
            "decoder_bounds_before_allocate": true
        }
    });
    let hostile_summary = json!({
        "profile": "residiuum-atomics-v1",
        "max_cbor_depth": MAX_CBOR_DEPTH,
        "families": [
            "depth",
            "count",
            "byte",
            "duplicate-key",
            "integer",
            "unknown-kind",
            "trailing-data"
        ],
        "vector_count": hostile_doc["vectors"].as_array().unwrap().len(),
        "all_refused": true
    });
    let model = json!({
        "profile": "residiuum-atomics-v1",
        "oracle": "serial_in_memory",
        "two_pass_validate_then_apply": true,
        "no_partial_publish": true,
        "complete_coverage_status": true
    });

    let dir = evidence_dir();
    fs::create_dir_all(&dir).unwrap();
    let files = vec![
        (
            "protocol-vectors.json",
            write(&dir, "protocol-vectors.json", &proto),
            "spec/atomics/protocol-vectors.json",
        ),
        (
            "rejected-vectors.json",
            write(&dir, "rejected-vectors.json", &rejected),
            "spec/atomics/rejected-vectors.json",
        ),
        (
            "property-summary.json",
            write(
                &dir,
                "property-summary.json",
                &format!("{}\n", serde_json::to_string_pretty(&property).unwrap()),
            ),
            "generated",
        ),
        (
            "hostile-corpus-summary.json",
            write(
                &dir,
                "hostile-corpus-summary.json",
                &format!(
                    "{}\n",
                    serde_json::to_string_pretty(&hostile_summary).unwrap()
                ),
            ),
            "generated",
        ),
        (
            "model-check-summary.json",
            write(
                &dir,
                "model-check-summary.json",
                &format!("{}\n", serde_json::to_string_pretty(&model).unwrap()),
            ),
            "generated",
        ),
    ];

    let manifest = json!({
        "package": "ATM-0",
        "title": "Protocol freeze and independent oracle",
        "crate": env!("CARGO_PKG_NAME"),
        "crate_version": env!("CARGO_PKG_VERSION"),
        "rustc": rustc_version(),
        "spec_cbor_v1_blake3": blake3_hex(cbor_spec.as_bytes()),
        "hostile_corpus_blake3": blake3_hex(hostile.as_bytes()),
        "compatibility": {
            "status": "frozen_for_review",
            "rule": "Later field or semantic changes require new fixtures and an explicit compatibility decision."
        },
        "files": files.iter().map(|(name, hash, source)| json!({
            "name": name,
            "blake3": hash,
            "source": source
        })).collect::<Vec<_>>(),
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&manifest).unwrap());
    fs::write(dir.join("manifest.json"), &body).unwrap();

    for name in [
        "manifest.json",
        "protocol-vectors.json",
        "rejected-vectors.json",
        "property-summary.json",
        "hostile-corpus-summary.json",
        "model-check-summary.json",
    ] {
        assert!(dir.join(name).is_file(), "missing {name}");
    }
    let loaded: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(loaded["package"], "ATM-0");
    assert_eq!(loaded["files"].as_array().unwrap().len(), 5);
    assert_eq!(loaded["compatibility"]["status"], "frozen_for_review");
}
