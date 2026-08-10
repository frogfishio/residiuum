//! Integration test for `residiuum console`.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use tempfile::tempdir;

fn residiuum_bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_residiuum"))
}

#[test]
fn console_executes_put_get_via_stdin() {
    let dir = tempdir().expect("tempdir");
    let store: PathBuf = dir.path().join("store");

    // Script: create a key, then read it back.
    // Console protocol is expected to accept newline-separated RQL.
    let script = format!(
        "PUT {} users/user-1 {{\"name\":\"hello\"}}\nGET {} users/user-1\nQUIT\n",
        store.display(),
        store.display()
    );

    let mut child = residiuum_bin()
        .args(["console", store.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn residiuum console");

    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(script.as_bytes())
        .expect("write script");

    let out = child.wait_with_output().expect("wait_with_output");

    assert!(
        out.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    // Expect JSON-ish output containing the inserted value.
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("\"name\"") && stdout.contains("hello"),
        "stdout did not contain expected payload: {stdout}"
    );
}
