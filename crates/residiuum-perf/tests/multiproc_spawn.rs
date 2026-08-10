//! Integration: genuine OS worker spawn + barrier sync (not sequential slots).
//!
//! Uses the built `residiuum-perf` binary so children understand the `worker`
//! subcommand. Unit tests keep sequential process slots for speed.

use residiuum_perf::campaign::{campaign_plan_synthetic, run_campaign, CampaignConfig, RunClass};
use residiuum_perf::store_driver::DriverKind;
use std::path::PathBuf;

fn worker_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_residiuum-perf"))
}

#[test]
fn spawned_workers_have_distinct_pids() {
    let dir = tempfile::tempdir().unwrap();
    let mut plan = campaign_plan_synthetic(0xA11CE, 1);
    plan.max_cells = 1;
    plan.include_multiproc_finding = false;
    plan.repetitions = 5;
    plan.processes = 2;

    let result = run_campaign(&CampaignConfig {
        plan,
        driver: DriverKind::Synthetic,
        work_root: Some(dir.path().to_path_buf()),
        declare_controlled_runner: false,
        run_class: RunClass::Smoke,
        spawn_workers: true,
        worker_bin: Some(worker_bin()),
        require_qualification_preflight: false,
        awo_mode: residiuum_perf::store_driver::AwoMode::Disabled,
        presentation: Default::default(),
    })
    .expect("spawned multiproc campaign");

    assert!(result.workers_spawned, "expected genuine OS workers");
    assert_eq!(result.worker_pids.len(), 2);
    assert_ne!(
        result.worker_pids[0], result.worker_pids[1],
        "process slots must be distinct OS PIDs, not sequential fakes"
    );
    assert!(result.valid_runs >= 2);
    assert!(result
        .withdrawals
        .iter()
        .any(|w| w.contains("genuine OS workers")));
}
