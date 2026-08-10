use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

fn sda_bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_residiuum-sda"))
}

fn unique_temp_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "residiuum-sda-{name}-{}-{nanos}",
        std::process::id()
    ))
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace crates dir")
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn expected_version_string() -> String {
    let root = repo_root();
    let version = fs::read_to_string(root.join("VERSION")).expect("read VERSION");
    let build = fs::read_to_string(root.join("BUILD")).expect("read BUILD");
    format!("{}-build {}\n", version.trim(), build.trim())
}

#[test]
fn version_reports_root_version_and_build() {
    let output = sda_bin()
        .arg("--version")
        .output()
        .expect("run residiuum-sda --version");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        expected_version_string()
    );
}

#[test]
fn license_prints_notice() {
    let output = sda_bin()
        .arg("--license")
        .output()
        .expect("run residiuum-sda --license");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Alexander R. Croft"));
    assert!(stdout.contains("MIT License"));
}

#[test]
fn help_mentions_core_workflows() {
    let output = sda_bin()
        .arg("--help")
        .output()
        .expect("run residiuum-sda --help");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Residiuum SDA+ENR1 hybrid command-line interface"));
    assert!(stdout.contains("residiuum-sda eval -e 'values(input)' < event.json"));
    assert!(stdout.contains("--license"));
    assert!(stdout.contains("--version"));
    assert!(stdout.contains("residiuum-sda fmt --stdin-filepath extract.sda < extract.sda"));
}

#[test]
fn eval_reads_stdin_json() {
    let mut child = sda_bin()
        .args(["eval", "-e", "values(input)"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn sda");

    use std::io::Write;
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(br#"{"b":2,"a":1}"#)
        .expect("write stdin");
    drop(child.stdin.take());

    let output = child.wait_with_output().expect("wait output");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout), "[\n  1,\n  2\n]\n");
}

#[test]
fn eval_reads_source_and_input_files() {
    let source_path = unique_temp_path("program.sda");
    let input_path = unique_temp_path("input.json");
    fs::write(&source_path, "values(input)").expect("write source file");
    fs::write(&input_path, r#"{"z":3,"a":1,"m":2}"#).expect("write input file");

    let output = sda_bin()
        .args([
            "eval",
            "-f",
            source_path.to_str().expect("source path str"),
            "-i",
            input_path.to_str().expect("input path str"),
            "--compact",
        ])
        .output()
        .expect("run sda eval");

    let _ = fs::remove_file(&source_path);
    let _ = fs::remove_file(&input_path);

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout), "[1,2,3]\n");
}

#[test]
fn check_reports_ok_for_valid_source() {
    let output = sda_bin()
        .args(["check", "-e", "values(input)"])
        .output()
        .expect("run sda check");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout), "ok\n");
}

#[test]
fn fmt_emits_canonical_source() {
    let output = sda_bin()
        .args(["fmt", "-e", " let x=1+2; input<name>! "])
        .output()
        .expect("run sda fmt");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "let x = 1 + 2;\ninput<\"name\">!;\n"
    );
}

#[test]
fn fmt_reads_source_from_stdin() {
    let mut child = sda_bin()
        .args(["fmt", "--stdin-filepath", "editor-buffer.sda"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn sda fmt");

    use std::io::Write;
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(br#" let x=1+2; input<name>! "#)
        .expect("write stdin");
    drop(child.stdin.take());

    let output = child.wait_with_output().expect("wait output");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "let x = 1 + 2;\ninput<\"name\">!;\n"
    );
}

#[test]
fn fmt_check_succeeds_for_canonical_source() {
    let output = sda_bin()
        .args(["fmt", "-e", "let x = 1 + 2;", "--check"])
        .output()
        .expect("run sda fmt --check");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
}

#[test]
fn fmt_check_exits_nonzero_for_noncanonical_source() {
    let output = sda_bin()
        .args(["fmt", "-e", "let x=1+2;", "--check"])
        .output()
        .expect("run noncanonical sda fmt --check");

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("not canonically formatted"));
}

#[test]
fn fmt_check_succeeds_for_canonical_stdin_source() {
    let mut child = sda_bin()
        .args(["fmt", "--check", "--stdin-filepath", "editor-buffer.sda"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn sda fmt --check");

    use std::io::Write;
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"let x = 1 + 2;\n")
        .expect("write stdin");
    drop(child.stdin.take());

    let output = child.wait_with_output().expect("wait output");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
}

#[test]
fn fmt_write_rewrites_source_file() {
    let source_path = unique_temp_path("format-write.sda");
    fs::write(&source_path, " let x=1+2; input<name>! ").expect("write source file");

    let output = sda_bin()
        .args([
            "fmt",
            "-f",
            source_path.to_str().expect("source path str"),
            "--write",
        ])
        .output()
        .expect("run sda fmt --write");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(&source_path).expect("read source file"),
        "let x = 1 + 2;\ninput<\"name\">!;\n"
    );

    let _ = fs::remove_file(&source_path);
}

#[test]
fn check_exits_nonzero_for_invalid_source() {
    let output = sda_bin()
        .args(["check", "-e", "let x = ;"])
        .output()
        .expect("run invalid sda check");

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Error:"));
}

#[test]
fn fmt_exits_nonzero_for_invalid_source() {
    let output = sda_bin()
        .args(["fmt", "-e", "let x = ;"])
        .output()
        .expect("run invalid sda fmt");

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Error:"));
}

#[test]
fn eval_exits_nonzero_for_invalid_input_json() {
    let mut child = sda_bin()
        .args(["eval", "-e", "input"])
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn sda");

    use std::io::Write;
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(br#"{"#)
        .expect("write invalid stdin");
    drop(child.stdin.take());

    let output = child.wait_with_output().expect("wait output");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid input JSON"));
}
