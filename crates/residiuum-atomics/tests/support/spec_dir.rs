// Shared fixture locator for ATM-0 tests.
// Packaged crates cannot see the monorepo `spec/atomics` tree. Prefer the
// crate-local `spec/` bundle; fall back to the workspace path in-tree.

#[allow(dead_code)]
fn spec_dir() -> std::path::PathBuf {
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let local = manifest.join("spec");
    if local.join("cbor-v1.json").is_file() {
        local
    } else {
        manifest.join("../../spec/atomics")
    }
}

#[allow(dead_code)]
fn spec_path(name: &str) -> std::path::PathBuf {
    spec_dir().join(name)
}