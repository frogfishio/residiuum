//! DEF-041-N — multi-process OS chaos + short soak (not in-process SimWorld).
//!
//! Acceptance for this labor cut:
//! - OS process spawn / clean exit / force-abort around durable writers
//! - Rolling restart: acks survive reopen in a new process
//! - Abort after recorded acks: those acks survive parent reopen
//! - Cross-process exclusive writer lock
//! - Failures retain seed + history dump ([`MULTIPROC_PROFILE`])
//! - Short CI soak; long soak gated by `RESIDIUUM_MULTIPROC_LONG_SOAK=1`
//!
//! Residual: full Jepsen PORC against live `serve-cluster` TCP partitions,
//! multi-hour soak, CLUSTER_SPEC §22.9–.20 network surface.

use residiuum_cluster::{MultiprocHistory, MULTIPROC_PROFILE};
use residiuum_store::{DurabilityMode, Store};
use std::env;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;
use tempfile::tempdir;

fn child_bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_residiuum-cluster-multiproc-child"))
}

fn run_child(
    store: &Path,
    history: &Path,
    seed: u64,
    ops: u64,
    abort_after: Option<u64>,
    mode: &str,
) -> std::process::Output {
    let mut cmd = child_bin();
    cmd.env("RESIDIUUM_MP_STORE", store)
        .env("RESIDIUUM_MP_HISTORY", history)
        .env("RESIDIUUM_MP_SEED", seed.to_string())
        .env("RESIDIUUM_MP_OPS", ops.to_string())
        .env("RESIDIUUM_MP_MODE", mode)
        .env("RESIDIUUM_MP_PREFIX", "mp/")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(n) = abort_after {
        cmd.env("RESIDIUUM_MP_ABORT_AFTER", n.to_string());
    }
    cmd.output().expect("spawn multiproc child")
}

#[test]
fn multiproc_profile_stable() {
    assert_eq!(MULTIPROC_PROFILE, "residiuum-cluster-multiproc-v1");
}

#[test]
fn rolling_restart_preserves_acked_puts() {
    let dir = tempdir().unwrap();
    let store = dir.path().join("s");
    let history = dir.path().join("h.json");
    let seed = 0x4100_0001u64;

    let out = run_child(&store, &history, seed, 5, None, "put_series");
    assert!(
        out.status.success(),
        "child failed: status={:?} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    let hist = MultiprocHistory::load(&history).expect("history");
    assert_eq!(hist.seed, seed, "{}", hist.dump());
    assert_eq!(hist.ops.len(), 5, "{}", hist.dump());
    assert!(hist.ops.iter().all(|o| o.acked));

    // New OS process (parent reopens) — all acks must be readable.
    let reopened = Store::open(&store).expect("reopen");
    for op in &hist.ops {
        let got = reopened.get(&op.subject).unwrap();
        assert_eq!(
            got.as_deref(),
            Some(op.value.as_bytes()),
            "lost ack {}\n{}",
            op.subject,
            hist.dump()
        );
    }
}

#[test]
fn abort_after_acks_still_durable() {
    let dir = tempdir().unwrap();
    let store = dir.path().join("s");
    let history = dir.path().join("h.json");
    let seed = 0x4100_0002u64;
    let ack_n = 3u64;

    let out = run_child(&store, &history, seed, 10, Some(ack_n), "put_series");
    // Abort is not a success exit.
    assert!(
        !out.status.success(),
        "expected abort/non-zero, got success"
    );
    let hist = MultiprocHistory::load(&history).expect("history after abort");
    assert!(
        hist.ops.len() >= ack_n as usize,
        "need recorded acks before abort: {}",
        hist.dump()
    );
    assert!(
        hist.dump().contains(&format!("seed={seed}")),
        "dump must retain seed: {}",
        hist.dump()
    );

    let reopened = Store::open(&store).expect("reopen after abort");
    for op in hist.ops.iter().filter(|o| o.acked).take(ack_n as usize) {
        let got = reopened.get(&op.subject).unwrap();
        assert_eq!(
            got.as_deref(),
            Some(op.value.as_bytes()),
            "acked write lost after OS abort\n{}",
            hist.dump()
        );
    }
}

#[test]
fn cross_process_writer_lock_exclusion() {
    let dir = tempdir().unwrap();
    let store = dir.path().join("s");
    let history = dir.path().join("h.json");
    // Parent holds exclusive writer.
    let mut parent = Store::create(&store).unwrap();
    parent
        .put("held", b"by-parent", DurabilityMode::Durable)
        .unwrap();

    let out = run_child(&store, &history, 7, 0, None, "try_open_only");
    assert_eq!(
        out.status.code(),
        Some(6),
        "child must exit 6 on WriterLockHeld, got {:?} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    let hist = MultiprocHistory::load(&history).unwrap();
    assert!(
        hist.notes.iter().any(|n| n.contains("writer_lock_held")),
        "{}",
        hist.dump()
    );
    // Parent still owns data.
    assert_eq!(
        parent.get("held").unwrap().as_deref(),
        Some(b"by-parent".as_slice())
    );
}

#[test]
fn short_soak_sequential_process_restarts() {
    let dir = tempdir().unwrap();
    let store = dir.path().join("s");
    let seed = 0x50a1_0041u64;
    let rounds = if env::var("RESIDIUUM_MULTIPROC_LONG_SOAK").ok().as_deref() == Some("1") {
        32
    } else {
        4
    };
    let ops_per = if env::var("RESIDIUUM_MULTIPROC_LONG_SOAK").ok().as_deref() == Some("1") {
        16
    } else {
        4
    };

    let mut all_subjects = Vec::new();
    for r in 0..rounds {
        let history = dir.path().join(format!("h-{r}.json"));
        // Each round is a separate process; first creates, later open.
        let mut cmd = child_bin();
        cmd.env("RESIDIUUM_MP_STORE", &store)
            .env("RESIDIUUM_MP_HISTORY", &history)
            .env("RESIDIUUM_MP_SEED", (seed + r as u64).to_string())
            .env("RESIDIUUM_MP_OPS", ops_per.to_string())
            .env("RESIDIUUM_MP_MODE", "put_series")
            .env("RESIDIUUM_MP_PREFIX", format!("r{r}/"));
        let out = cmd.output().unwrap();
        assert!(
            out.status.success(),
            "round {r} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let hist = MultiprocHistory::load(&history).unwrap();
        for op in hist.ops {
            all_subjects.push((op.subject, op.value));
        }
        // Process ended — next round is a true rolling restart.
        std::thread::sleep(Duration::from_millis(5));
    }

    let final_store = Store::open(&store).unwrap();
    for (subject, value) in &all_subjects {
        assert_eq!(
            final_store.get(subject).unwrap().as_deref(),
            Some(value.as_bytes()),
            "soak lost {subject}"
        );
    }
    assert!(!all_subjects.is_empty());
}

#[test]
fn history_dump_contract_on_failure_path() {
    // Parent holds the writer; child try_open fails and still dumps seed.
    let dir = tempdir().unwrap();
    let store = dir.path().join("s");
    let history = dir.path().join("h.json");
    let seed = 99u64;
    let _held = Store::create(&store).unwrap();
    let out = run_child(&store, &history, seed, 0, None, "try_open_only");
    assert!(
        !out.status.success(),
        "child should fail while parent holds lock"
    );
    let hist = MultiprocHistory::load(&history).expect("history dump required");
    assert_eq!(hist.seed, seed);
    assert!(
        hist.dump().contains("seed=99"),
        "dump must retain seed: {}",
        hist.dump()
    );
}
