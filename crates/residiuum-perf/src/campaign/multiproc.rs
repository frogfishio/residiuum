//! Genuinely spawned, synchronized worker processes for campaign process slots.
//!
//! Replaces sequential in-process "process_id" loops with OS children of the
//! same `residiuum-perf` binary (`worker` subcommand), barrier-synchronized
//! before work starts.

use super::run::{CellRepetition, ProcessSlot};
use super::run_class::RunClass;
use super::CampaignError;
use crate::matrix::MatrixCell;
use crate::store_driver::{
    run_driver_cell, store_driver_compiled, AwoMode, DriverKind, DriverRunConfig,
    MeasurementSurface,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerJob {
    pub campaign_id: String,
    pub process_id: u32,
    pub process_seed: u64,
    pub reps: u32,
    pub seed: u64,
    pub driver: String,
    pub run_class: String,
    pub work_root: PathBuf,
    pub cells: Vec<MatrixCell>,
    pub include_multiproc_finding: bool,
    pub multiproc_cells: Vec<MatrixCell>,
    /// `disabled` | `static` | `adaptive` (default disabled).
    #[serde(default = "default_awo_mode_str")]
    pub awo_mode: String,
}

fn default_awo_mode_str() -> String {
    AwoMode::Disabled.as_str().into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerResult {
    pub process_id: u32,
    pub worker_pid: u32,
    pub spawned: bool,
    pub repetitions: Vec<CellRepetition>,
    pub error: Option<String>,
}

/// Run process slots as real OS children of `worker_bin`, barrier-synced.
pub fn run_spawned_workers(
    worker_bin: &Path,
    jobs: &[WorkerJob],
    sync_root: &Path,
) -> Result<Vec<WorkerResult>, CampaignError> {
    fs::create_dir_all(sync_root).map_err(|e| CampaignError::Io(e.to_string()))?;
    let go_path = sync_root.join("go");
    let _ = fs::remove_file(&go_path);

    let mut children = Vec::new();
    let mut ready_paths = Vec::new();

    for job in jobs {
        let job_dir = sync_root.join(format!("p{}", job.process_id));
        fs::create_dir_all(&job_dir).map_err(|e| CampaignError::Io(e.to_string()))?;
        let job_path = job_dir.join("job.json");
        let out_path = job_dir.join("result.json");
        let ready_path = job_dir.join("ready");
        let _ = fs::remove_file(&ready_path);
        let _ = fs::remove_file(&out_path);
        let job_json =
            serde_json::to_string_pretty(job).map_err(|e| CampaignError::Msg(e.to_string()))?;
        fs::write(&job_path, job_json).map_err(|e| CampaignError::Io(e.to_string()))?;

        let child = Command::new(worker_bin)
            .arg("worker")
            .arg("--job")
            .arg(&job_path)
            .arg("--out")
            .arg(&out_path)
            .arg("--ready")
            .arg(&ready_path)
            .arg("--go")
            .arg(&go_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| CampaignError::Msg(format!("spawn worker: {e}")))?;

        ready_paths.push(ready_path);
        children.push((job.process_id, child, out_path));
    }

    // Wait until every worker has written ready (or timed out).
    let deadline = Instant::now() + Duration::from_secs(30);
    for rp in &ready_paths {
        while !rp.exists() {
            if Instant::now() > deadline {
                return Err(CampaignError::Msg(format!(
                    "worker ready timeout: {}",
                    rp.display()
                )));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    // Release barrier: all workers start measurement together.
    fs::write(&go_path, b"go").map_err(|e| CampaignError::Io(e.to_string()))?;

    let mut results = Vec::new();
    for (process_id, mut child, out_path) in children {
        let status = child
            .wait()
            .map_err(|e| CampaignError::Msg(format!("wait worker: {e}")))?;
        let pid = child.id();
        if !status.success() {
            let err = format!("worker p{process_id} exit={status:?}");
            results.push(WorkerResult {
                process_id,
                worker_pid: pid,
                spawned: true,
                repetitions: vec![],
                error: Some(err),
            });
            continue;
        }
        let raw = fs::read_to_string(&out_path).map_err(|e| CampaignError::Io(e.to_string()))?;
        let mut wr: WorkerResult =
            serde_json::from_str(&raw).map_err(|e| CampaignError::Msg(e.to_string()))?;
        wr.spawned = true;
        wr.worker_pid = pid;
        results.push(wr);
    }
    Ok(results)
}

/// In-process worker entry (same binary). Waits for barrier then runs jobs.
pub fn run_worker_job(
    job: &WorkerJob,
    ready_path: &Path,
    go_path: &Path,
) -> Result<WorkerResult, CampaignError> {
    // Signal ready, then wait for go.
    fs::write(ready_path, b"ready").map_err(|e| CampaignError::Io(e.to_string()))?;
    let deadline = Instant::now() + Duration::from_secs(60);
    while !go_path.exists() {
        if Instant::now() > deadline {
            return Err(CampaignError::Msg("go barrier timeout".into()));
        }
        thread::sleep(Duration::from_millis(5));
    }

    let driver = DriverKind::parse(&job.driver)
        .ok_or_else(|| CampaignError::Msg(format!("bad driver {}", job.driver)))?;
    if driver == DriverKind::RealStore && !store_driver_compiled() {
        return Err(CampaignError::Msg(
            "real_store requires --features store-driver".into(),
        ));
    }
    let run_class = RunClass::parse(&job.run_class).unwrap_or(RunClass::Smoke);
    let awo_mode = AwoMode::parse(&job.awo_mode).unwrap_or(AwoMode::Disabled);
    let surface = match driver {
        DriverKind::Synthetic => MeasurementSurface::NonProductSynthetic,
        DriverKind::RealStore => MeasurementSurface::RealStoreUncontrolled,
    };

    let mut repetitions = Vec::new();
    for cell in &job.cells {
        for rep in 0..job.reps {
            let seed = job
                .process_seed
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(rep as u64)
                .wrapping_add(cell.order_rank as u64);
            let mut cell_run = cell.clone();
            if run_class.allows_smoke_op_cap() {
                cell_run.op_count = cell_run.op_count.min(RunClass::SMOKE_MAX_OPS).max(1);
            }
            let drep = run_driver_cell(&DriverRunConfig {
                cell: cell_run,
                seed,
                kind: driver,
                work_root: Some(job.work_root.clone()),
                durability_mutant: false,
                digest_mutant: false,
                run_class: run_class.as_str().into(),
                awo_mode,
            })
            .map_err(|e| CampaignError::Msg(e.to_string()))?;
            let run_id = format!(
                "{}-p{}-r{}-{}",
                job.campaign_id, job.process_id, rep, cell.cell_id
            );
            repetitions.push(CellRepetition {
                run_id,
                cell_id: cell.cell_id.clone(),
                process_id: job.process_id,
                rep,
                report: drep.cell,
                driver_kind: driver.as_str().into(),
                measurement_surface: surface.as_str().into(),
                plan_source: Some(drep.plan_source),
            });
        }
    }

    if job.include_multiproc_finding {
        for cell in &job.multiproc_cells {
            for rep in 0..job.reps {
                let seed = job
                    .process_seed
                    .wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
                    .wrapping_add(rep as u64);
                let mut cell_run = cell.clone();
                if run_class.allows_smoke_op_cap() {
                    cell_run.op_count = cell_run.op_count.min(RunClass::SMOKE_MAX_OPS).max(1);
                }
                let drep = run_driver_cell(&DriverRunConfig {
                    cell: cell_run,
                    seed,
                    kind: driver,
                    work_root: Some(job.work_root.clone()),
                    durability_mutant: false,
                    digest_mutant: false,
                    run_class: run_class.as_str().into(),
                    awo_mode,
                })
                .map_err(|e| CampaignError::Msg(e.to_string()))?;
                let run_id = format!(
                    "{}-mp-p{}-r{}-{}",
                    job.campaign_id, job.process_id, rep, cell.cell_id
                );
                repetitions.push(CellRepetition {
                    run_id,
                    cell_id: cell.cell_id.clone(),
                    process_id: job.process_id,
                    rep,
                    report: drep.cell,
                    driver_kind: driver.as_str().into(),
                    measurement_surface: surface.as_str().into(),
                    plan_source: Some(drep.plan_source),
                });
            }
        }
    }

    Ok(WorkerResult {
        process_id: job.process_id,
        worker_pid: std::process::id(),
        spawned: true,
        repetitions,
        error: None,
    })
}

/// Build worker jobs from campaign slots (one OS process per process slot).
pub fn build_worker_jobs(
    campaign_id: &str,
    seed: u64,
    process_slots: &[ProcessSlot],
    reps_per_proc: u32,
    cells: &[MatrixCell],
    multiproc_cells: &[MatrixCell],
    include_multiproc_finding: bool,
    driver: DriverKind,
    run_class: RunClass,
    work_root: &Path,
    awo_mode: AwoMode,
) -> Vec<WorkerJob> {
    process_slots
        .iter()
        .map(|slot| WorkerJob {
            campaign_id: campaign_id.into(),
            process_id: slot.process_id,
            process_seed: slot.process_seed,
            reps: reps_per_proc,
            seed,
            driver: driver.as_str().into(),
            run_class: run_class.as_str().into(),
            work_root: work_root.to_path_buf(),
            cells: cells.to_vec(),
            include_multiproc_finding,
            multiproc_cells: multiproc_cells.to_vec(),
            awo_mode: awo_mode.as_str().into(),
        })
        .collect()
}

/// Resolve the current executable for self-spawn (tests/CI).
pub fn current_worker_bin() -> Result<PathBuf, CampaignError> {
    std::env::current_exe().map_err(|e| CampaignError::Io(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::campaign::plan::campaign_plan_synthetic;
    use crate::matrix::{build_matrix_cells, ScheduleSeed};

    #[test]
    fn build_jobs_matches_slots() {
        let plan = campaign_plan_synthetic(1, 1);
        let slots = vec![
            ProcessSlot {
                process_id: 0,
                process_seed: 1,
            },
            ProcessSlot {
                process_id: 1,
                process_seed: 2,
            },
        ];
        let m = build_matrix_cells(ScheduleSeed { seed: plan.seed });
        let cells: Vec<_> = m.cells.into_iter().take(1).collect();
        let jobs = build_worker_jobs(
            &plan.campaign_id,
            plan.seed,
            &slots,
            3,
            &cells,
            &[],
            false,
            DriverKind::Synthetic,
            RunClass::Smoke,
            Path::new("/tmp"),
            AwoMode::Disabled,
        );
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].reps, 3);
        assert_eq!(jobs[1].process_id, 1);
    }
}
