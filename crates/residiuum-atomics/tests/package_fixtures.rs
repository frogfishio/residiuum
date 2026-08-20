//! ATM-0.17: packaged crate ships the conformance fixtures tests read.

use std::fs;

include!("support/spec_dir.rs");

const FIXTURES: &[&str] = &[
    "cbor-v1.json",
    "predicates-v1.json",
    "protocol-vectors.json",
    "rejected-vectors.json",
    "hostile-corpus.json",
    "evidence-vectors.json",
];

#[test]
fn crate_spec_bundle_is_complete() {
    for name in FIXTURES {
        let path = spec_path(name);
        assert!(
            path.is_file(),
            "missing packaged fixture {}",
            path.display()
        );
        assert!(
            fs::metadata(&path).unwrap().len() > 0,
            "empty fixture {name}"
        );
    }
}

#[test]
fn crate_spec_matches_workspace_when_present() {
    let workspace = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spec/atomics");
    if !workspace.is_dir() {
        return;
    }
    for name in FIXTURES {
        let packaged = fs::read(spec_path(name)).unwrap();
        let tree = fs::read(workspace.join(name)).unwrap();
        assert_eq!(packaged, tree, "{name} drifted from workspace spec/atomics");
    }
}
