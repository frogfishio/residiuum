//! CSQ-0: Rust agreement with core-storage JSON registries.
//!
//! Validates the same structural rules as `scripts/verify-core-storage-registry.sh`
//! so Rust and shell validators agree.

use serde_json::Value;
use std::collections::HashSet;
use std::path::PathBuf;

fn workspace_root() -> PathBuf {
    // crates/residiuum-store/tests -> workspace root
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn load_items(name: &str) -> Vec<Value> {
    let path = workspace_root()
        .join("spec/verification/core-storage")
        .join(name);
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let v: Value =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));
    v.get("items")
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default()
}

fn ids(items: &[Value]) -> HashSet<String> {
    items
        .iter()
        .filter_map(|i| i.get("id").and_then(|x| x.as_str()).map(str::to_string))
        .collect()
}

#[test]
fn csq0_registry_profile_and_identity() {
    let profiles = load_items("profiles-v1.json");
    assert!(
        profiles
            .iter()
            .any(|p| p.get("id").and_then(|x| x.as_str()) == Some("residiuum-core-storage-v1")),
        "residiuum-core-storage-v1 must be registered"
    );
    for p in &profiles {
        let id = p.get("id").and_then(|x| x.as_str()).unwrap_or("");
        assert!(
            !id.to_ascii_lowercase().contains(concat!("din", "go")),
            "forbidden pre-reset product profile id: {id}"
        );
    }

    let schema_path = workspace_root().join("spec/verification/core-storage/report-v1.schema.json");
    let schema: Value =
        serde_json::from_str(&std::fs::read_to_string(schema_path).unwrap()).unwrap();
    assert_eq!(
        schema["properties"]["profile"]["const"],
        "residiuum-core-storage-v1"
    );
}

#[test]
fn csq0_registry_graph_closed() {
    let invariants = load_items("invariants-v1.json");
    let operations = load_items("operations-v1.json");
    let boundaries = load_items("boundaries-v1.json");
    let oracles = load_items("oracles-v1.json");
    let suites = load_items("suites-v1.json");
    let failures = load_items("failures-v1.json");
    let combos = load_items("failure-combinations-v1.json");
    let claims = load_items("claims-v1.json");
    let errors = load_items("errors-v1.json");
    let proofs = load_items("proofs-v1.json");
    let mutations = load_items("mutations-v1.json");

    assert!(
        invariants.len() >= 80,
        "expected full CSQ invariant registry"
    );
    assert!(operations.len() >= 10);
    assert!(boundaries.len() >= 20);
    assert!(errors.len() >= 10);
    assert_eq!(oracles.len(), 3);

    let inv_ids = ids(&invariants);
    let op_ids = ids(&operations);
    let bnd_ids = ids(&boundaries);
    let oracle_ids = ids(&oracles);
    let suite_ids = ids(&suites);
    let fail_ids = ids(&failures);
    let proof_ids = ids(&proofs);
    let mut_ids = ids(&mutations);

    // unique boundary ids
    assert_eq!(bnd_ids.len(), boundaries.len());

    for o in &oracles {
        let id = o.get("id").and_then(|x| x.as_str()).unwrap();
        if id == "CSQ-ORACLE-MODEL" || id == "CSQ-ORACLE-READER" {
            assert_eq!(
                o.get("imports_production_store").and_then(|x| x.as_bool()),
                Some(false)
            );
        }
    }

    for inv in &invariants {
        let id = inv.get("id").and_then(|x| x.as_str()).unwrap();
        let olist = inv["oracles"].as_array().expect("oracles");
        let slist = inv["suites"].as_array().expect("suites");
        assert!(!olist.is_empty(), "{id} missing oracles");
        assert!(!slist.is_empty(), "{id} missing suites");
        for o in olist {
            let os = o.as_str().unwrap();
            assert!(oracle_ids.contains(os), "{id} unknown oracle {os}");
        }
        for s in slist {
            let ss = s.as_str().unwrap();
            assert!(suite_ids.contains(ss), "{id} unknown suite {ss}");
        }
        for p in inv["proof_obligations"].as_array().unwrap_or(&vec![]) {
            let ps = p.as_str().unwrap();
            assert!(proof_ids.contains(ps), "{id} unknown proof {ps}");
        }
    }

    for op in &operations {
        let id = op.get("id").and_then(|x| x.as_str()).unwrap();
        for b in op["boundaries"].as_array().unwrap() {
            let bs = b.as_str().unwrap();
            assert!(bnd_ids.contains(bs), "op {id} unknown boundary {bs}");
        }
        for inv in op["invariants"].as_array().unwrap_or(&vec![]) {
            let s = inv.as_str().unwrap();
            if s.starts_with("CSQ-") {
                assert!(inv_ids.contains(s), "op {id} unknown invariant {s}");
            }
        }
        for o in op["oracles"].as_array().unwrap_or(&vec![]) {
            assert!(oracle_ids.contains(o.as_str().unwrap()));
        }
        for s in op["suites"].as_array().unwrap_or(&vec![]) {
            assert!(suite_ids.contains(s.as_str().unwrap()));
        }
        for f in op["failure_classes"].as_array().unwrap_or(&vec![]) {
            assert!(
                fail_ids.contains(f.as_str().unwrap()),
                "op {id} unknown failure"
            );
        }
    }

    for b in &boundaries {
        let op = b.get("operation_id").and_then(|x| x.as_str()).unwrap();
        assert!(op_ids.contains(op));
        assert!(b.get("harness").and_then(|x| x.as_str()).is_some());
    }

    for c in &combos {
        let owner = c.get("executable_owner").and_then(|x| x.as_str()).unwrap();
        assert!(suite_ids.contains(owner));
        for f in c["failures"].as_array().unwrap() {
            assert!(fail_ids.contains(f.as_str().unwrap()));
        }
    }

    for cl in &claims {
        assert_eq!(
            cl.get("profile").and_then(|x| x.as_str()),
            Some("residiuum-core-storage-v1")
        );
        assert!(!cl["invariants"].as_array().unwrap().is_empty());
        assert!(!cl["oracles"].as_array().unwrap().is_empty());
        assert!(!cl["suites"].as_array().unwrap().is_empty());
        assert!(!cl["assumptions"].as_array().unwrap().is_empty());
    }

    for m in &mutations {
        assert!(
            !m["must_be_killed_by"].as_array().unwrap().is_empty(),
            "mutation must have killer"
        );
        let _ = mut_ids.contains(m.get("id").and_then(|x| x.as_str()).unwrap());
    }
}

#[test]
fn csq0_crash_matrix_import_preserves_historical_ids() {
    let root = workspace_root();
    let cm: Value = serde_json::from_str(
        &std::fs::read_to_string(root.join("crates/residiuum-store/crash_matrix.v1.json")).unwrap(),
    )
    .unwrap();
    let imp: Value = serde_json::from_str(
        &std::fs::read_to_string(
            root.join("spec/verification/core-storage/crash-matrix-import-v1.json"),
        )
        .unwrap(),
    )
    .unwrap();

    let mut src = HashSet::new();
    for op in cm["operations"].as_array().unwrap() {
        for fp in op["failpoints"].as_array().unwrap() {
            src.insert(fp["name"].as_str().unwrap().to_string());
        }
    }
    let mut imported = HashSet::new();
    for op in imp["operations"].as_array().unwrap() {
        for fp in op["failpoints"].as_array().unwrap() {
            imported.insert(fp["historical_cell_id"].as_str().unwrap().to_string());
        }
    }
    assert_eq!(src, imported, "historical failpoint IDs must be preserved");

    let bnd_ids = ids(&load_items("boundaries-v1.json"));
    for op in imp["operations"].as_array().unwrap() {
        for fp in op["failpoints"].as_array().unwrap() {
            let bid = fp["boundary_id"].as_str().unwrap();
            assert!(bnd_ids.contains(bid), "missing boundary for {bid}");
        }
    }
}

#[test]
fn csq0_store_errors_registered() {
    // Spot-check critical variants exist in registry.
    let errors = load_items("errors-v1.json");
    let rust_names: HashSet<String> = errors
        .iter()
        .filter_map(|e| {
            e.get("rust_variant")
                .and_then(|x| x.as_str())
                .map(str::to_string)
        })
        .collect();
    for required in [
        "StoreError::Io",
        "StoreError::CorruptMeta",
        "StoreError::PayloadPartial",
        "StoreError::HistoryEventNotFound",
        "StoreError::WriterLockHeld",
        "StoreError::CoverageIncomplete",
    ] {
        assert!(
            rust_names.contains(required),
            "missing error mapping for {required}"
        );
    }
}
