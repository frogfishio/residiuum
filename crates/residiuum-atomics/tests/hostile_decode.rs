//! ATM-0.4: hostile decoder corpus.
//!
//! Families: depth, count, byte, duplicate-key, integer, unknown-kind, trailing-data.
//! The decoder must refuse each case without panicking or allocating from a
//! declared hostile length.

use residiuum_atomics::{decode_canonical_plan, AtomicsError, CborError, MAX_CBOR_DEPTH};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::time::Instant;

include!("support/spec_dir.rs");

fn parse_hex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "hex length must be even");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

#[test]
fn hostile_corpus_covers_required_families_and_refuses() {
    assert_eq!(MAX_CBOR_DEPTH, 8);
    let raw = fs::read_to_string(spec_path("hostile-corpus.json")).unwrap();
    let doc: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(doc["max_cbor_depth"], MAX_CBOR_DEPTH);

    let required = [
        "depth",
        "count",
        "byte",
        "duplicate-key",
        "integer",
        "unknown-kind",
        "trailing-data",
    ];
    let vectors = doc["vectors"].as_array().expect("vectors");
    let families: BTreeSet<_> = vectors
        .iter()
        .map(|v| v["family"].as_str().unwrap())
        .collect();
    for fam in required {
        assert!(families.contains(fam), "missing hostile family {fam}");
    }

    for v in vectors {
        let name = v["name"].as_str().unwrap();
        let family = v["family"].as_str().unwrap();
        let want = v["reason"].as_str().unwrap();
        let bytes = parse_hex(v["bytes_hex"].as_str().unwrap());
        let start = Instant::now();
        let err = decode_canonical_plan(&bytes).expect_err(name);
        let elapsed_ms = start.elapsed().as_millis();
        assert!(
            elapsed_ms < 250,
            "{name}: decoder hung ({elapsed_ms} ms) on hostile length"
        );
        match err {
            AtomicsError::Cbor(cbor) => {
                assert_eq!(cbor.as_str(), want, "{name}: {cbor:?}");
                // Depth/count/byte must not be a silent success or a panic.
                if family == "count" || family == "byte" {
                    assert_eq!(cbor, CborError::Truncated, "{name}");
                }
            }
            other => panic!("{name}: expected Cbor, got {other:?}"),
        }
    }
}
