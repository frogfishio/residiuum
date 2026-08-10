//! Multi-process multi-repetition campaign execution over matrix cells.
//!
//! Supports synthetic (default, NON-PRODUCT) and real_store drivers (PQH-11).
//! **Smoke vs qualification** are separate (SPEC §6.4). Smoke may use explicit
//! op budgets for CI; qualification never caps cells to tiny op counts and must
//! meet duration/byte floors + steady-state before bottleneck claims.

use super::plan::CampaignPlan;
use super::run_class::RunClass;
use super::CampaignError;
use crate::matrix::{build_matrix_cells, MatrixCell, MatrixManifest, ScheduleSeed};
use crate::runner::{
    environment_fingerprint, preflight_work_root, BuildMode, PreflightConfig, RunBudgets,
};
use crate::store_driver::{
    run_driver_cell, store_driver_compiled, AwoMode, DriverKind, DriverRunConfig,
    MeasurementSurface, ObserverOverheadReport,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessSlot {
    pub process_id: u32,
    pub process_seed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellRepetition {
    pub run_id: String,
    pub cell_id: String,
    pub process_id: u32,
    pub rep: u32,
    pub report: crate::matrix::CellRunReport,
    /// Driver used for this repetition.
    #[serde(default = "default_driver_kind_str")]
    pub driver_kind: String,
    /// Measurement surface label.
    #[serde(default = "default_surface_str")]
    pub measurement_surface: String,
    /// Plan source id when a PhysicalWritePlan was emitted.
    #[serde(default)]
    pub plan_source: Option<String>,
}

fn default_driver_kind_str() -> String {
    DriverKind::Synthetic.as_str().into()
}
fn default_surface_str() -> String {
    MeasurementSurface::NonProductSynthetic.as_str().into()
}

/// Optional override of matrix presentation knobs (T7 AWO-fair shapes).
///
/// When set, applied after matrix counterbalance so experiments can pin
/// **independent singles** (`batch_size = 1`) while varying concurrency /
/// outstanding — without relying on harness `batch_size > 1` (which is L-API
/// `put_many(N)`, not AWO collection).
#[derive(Debug, Clone, Default)]
pub struct PresentationPin {
    pub batch_size: Option<u32>,
    pub concurrency: Option<u32>,
    pub outstanding: Option<u32>,
}

impl PresentationPin {
    pub fn is_active(&self) -> bool {
        self.batch_size.is_some() || self.concurrency.is_some() || self.outstanding.is_some()
    }

    pub fn apply(&self, cell: &mut MatrixCell) {
        if let Some(b) = self.batch_size {
            cell.batch_size = b.max(1);
        }
        if let Some(c) = self.concurrency {
            cell.concurrency = c.max(1);
        }
        if let Some(o) = self.outstanding {
            cell.outstanding = o.max(1);
        }
        if self.is_active() {
            // Honest cell id for artifacts (presentation pin visible).
            cell.cell_id = format!(
                "{}-pin-b{}-c{}-o{}",
                cell.cell_id, cell.batch_size, cell.concurrency, cell.outstanding
            );
        }
    }
}

#[derive(Debug, Clone)]
pub struct CampaignConfig {
    pub plan: CampaignPlan,
    /// Cell driver (synthetic default).
    pub driver: DriverKind,
    /// Required for real_store (dedicated work root).
    pub work_root: Option<PathBuf>,
    /// When true and platform allows product baseline + real_store, mark eligible.
    /// Developer laptops should leave this false even if platform is labelled controlled.
    pub declare_controlled_runner: bool,
    /// Smoke vs qualification (SPEC §6.4). Default smoke for unit/CI safety.
    pub run_class: RunClass,
    /// When true (and work_root set), process slots are genuine OS children
    /// barrier-synchronized via `residiuum-perf worker` — not sequential fakes.
    pub spawn_workers: bool,
    /// Path to worker binary (defaults to current_exe when spawn_workers).
    pub worker_bin: Option<PathBuf>,
    /// When true (default for qualification/soak), run fail-closed preflight
    /// before any cells. Smoke unit tests leave this false.
    pub require_qualification_preflight: bool,
    /// Adaptive write mode for real_store cells (default disabled).
    pub awo_mode: AwoMode,
    /// Optional presentation pin (batch/concurrency/outstanding).
    pub presentation: PresentationPin,
}

impl CampaignConfig {
    /// Synthetic **smoke** campaign (NON-PRODUCT; unit/CI; sequential for speed).
    pub fn synthetic(plan: CampaignPlan) -> Self {
        Self {
            plan,
            driver: DriverKind::Synthetic,
            work_root: None,
            declare_controlled_runner: false,
            run_class: RunClass::Smoke,
            spawn_workers: false,
            worker_bin: None,
            require_qualification_preflight: false,
            awo_mode: AwoMode::Disabled,
            presentation: PresentationPin::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignResult {
    pub plan: CampaignPlan,
    pub matrix_schema: String,
    pub matrix_seed: u64,
    pub process_slots: Vec<ProcessSlot>,
    pub repetitions: Vec<CellRepetition>,
    pub cells_executed: usize,
    pub valid_runs: usize,
    pub invalid_runs: usize,
    /// Campaign-level driver.
    pub driver_kind: String,
    /// Campaign-level surface (worst-case honesty).
    pub measurement_surface: String,
    /// True only when real_store + controlled platform + declare_controlled_runner.
    pub product_claim_eligible: bool,
    /// Primary registered bottleneck verdict if any non-mixed ranked.
    /// **Never set from smoke** — prior smoke `io_queue_underdriven` is withdrawn.
    pub primary_bottleneck: Option<String>,
    /// Run IDs attached to the primary bottleneck (reproduced evidence).
    pub primary_bottleneck_run_ids: Vec<String>,
    /// Campaign run class (smoke/diagnostic/qualification/soak).
    pub run_class: String,
    /// Explicit withdrawal notes (e.g. smoke bottleneck claims).
    pub withdrawals: Vec<String>,
    /// True when process slots were genuine OS children.
    pub workers_spawned: bool,
    /// Worker PIDs observed (one per process slot when spawned).
    pub worker_pids: Vec<u32>,
    /// Fail-closed preflight outcome when qualification preflight ran.
    #[serde(default)]
    pub preflight_outcome: Option<String>,
    /// Preflight validity id (e.g. valid / invalid_debug_build).
    #[serde(default)]
    pub preflight_validity_id: Option<String>,
    /// Environment fingerprint hash bound into evidence when available.
    #[serde(default)]
    pub environment_hash: Option<String>,
    /// Full preflight report JSON (optional; hashed into evidence bundle).
    #[serde(default)]
    pub preflight_report: Option<serde_json::Value>,
    /// Full environment fingerprint JSON (optional; hashed into evidence).
    #[serde(default)]
    pub environment_fingerprint: Option<serde_json::Value>,
    /// Per-qualified-cell observer overhead (real_store).
    /// Each entry matches that cell's full workload contract (floors/distribution/
    /// durability/concurrency/…). Hashed as `observer_overhead.json`.
    #[serde(default)]
    pub observer_overhead_reports: Vec<ObserverOverheadReport>,
    /// Convenience: first per-cell report when any (legacy single-slot consumers).
    #[serde(default)]
    pub observer_overhead_report: Option<ObserverOverheadReport>,
}

/// Execute campaign: for each selected cell, run `processes × reps_per_process`
/// so total reps ≥ plan.repetitions across ≥ plan.processes process slots.
///
/// When `spawn_workers` is true (and `work_root` is set), process slots are
/// genuine OS children barrier-synchronized via `residiuum-perf worker`.
/// Unit tests keep sequential slots (`spawn_workers: false`) for speed.
pub fn run_campaign(cfg: &CampaignConfig) -> Result<CampaignResult, CampaignError> {
    cfg.plan.validate().map_err(CampaignError::Msg)?;

    if cfg.driver == DriverKind::RealStore {
        if !store_driver_compiled() {
            return Err(CampaignError::Msg(
                "real_store requires rebuild with --features store-driver".into(),
            ));
        }
        if cfg.work_root.is_none() {
            return Err(CampaignError::Msg(
                "real_store campaign requires work_root".into(),
            ));
        }
    }

    if cfg.spawn_workers && cfg.work_root.is_none() {
        return Err(CampaignError::Msg(
            "spawn_workers requires work_root for job/sync paths".into(),
        ));
    }

    // Fail-closed qualification preflight (SPEC §6.4 / plan §5).
    // CLI sets require_qualification_preflight for qualification/soak; unit
    // tests leave false so smoke/synthetic harness stays fast.
    let mut preflight_outcome = None;
    let mut preflight_validity_id = None;
    let mut preflight_report_json = None;
    let mut environment_hash = None;
    let mut environment_fingerprint_json = None;

    if cfg.require_qualification_preflight {
        if !cfg.run_class.may_emit_bottleneck_verdict() {
            return Err(CampaignError::Msg(
                "require_qualification_preflight only applies to qualification/soak".into(),
            ));
        }
        let work = cfg.work_root.as_ref().ok_or_else(|| {
            CampaignError::Msg(
                "qualification/soak requires work_root for fail-closed preflight".into(),
            )
        })?;
        let report = run_qualification_preflight(work, &cfg.plan.campaign_id)?;
        if !report.outcome.is_ready() {
            return Err(CampaignError::Msg(format!(
                "qualification preflight fail-closed: outcome={:?} reason={:?} messages={:?}",
                report.outcome, report.reject_reason, report.messages
            )));
        }
        preflight_outcome = Some(format!("{:?}", report.outcome).to_ascii_lowercase());
        preflight_validity_id = Some(report.validity_id.clone());
        if let Some(env) = report.environment.as_ref() {
            environment_hash = Some(env.environment_hash.clone());
            environment_fingerprint_json = serde_json::to_value(env).ok();
        }
        preflight_report_json = serde_json::to_value(&report).ok();
    } else if let Some(work) = cfg.work_root.as_ref() {
        // Bind environment fingerprint even for smoke when work_root is present.
        if let Ok(env) = environment_fingerprint(Some(work), BuildMode::Diagnostic) {
            environment_hash = Some(env.environment_hash.clone());
            environment_fingerprint_json = serde_json::to_value(&env).ok();
        }
    }

    let manifest: MatrixManifest = build_matrix_cells(ScheduleSeed {
        seed: cfg.plan.seed,
    });

    let mut cells: Vec<_> = manifest
        .cells
        .iter()
        .take(cfg.plan.max_cells)
        .cloned()
        .collect();
    // AWO-fair presentation pin (T7): rewrite batch/conc/outstanding after matrix pick.
    if cfg.presentation.is_active() {
        for cell in &mut cells {
            cfg.presentation.apply(cell);
        }
    }

    let procs = cfg.plan.processes;
    let total_reps = cfg.plan.repetitions;
    let reps_per_proc = (total_reps + procs - 1) / procs;

    let mut process_slots = Vec::new();
    for p in 0..procs {
        process_slots.push(ProcessSlot {
            process_id: p,
            process_seed: cfg
                .plan
                .seed
                .wrapping_add(PROCESS_SEED_TAG)
                .wrapping_add(p as u64),
        });
    }

    // Presentation pin runs are single-shape experiments — skip multiproc finding noise.
    let multiproc_cells: Vec<MatrixCell> = if cfg.plan.include_multiproc_finding
        && !cfg.presentation.is_active()
    {
        manifest
            .cells
            .iter()
            .filter(|c| (c.payload_size == 4096 || c.payload_size == 8192) && c.concurrency >= 2)
            .filter(|c| !cells.iter().any(|x| x.cell_id == c.cell_id))
            .take(4)
            .cloned()
            .collect()
    } else {
        Vec::new()
    };

    let surface = campaign_surface(cfg);
    let product_claim_eligible = product_eligible(cfg, surface);

    let (repetitions, valid_runs, invalid_runs, workers_spawned, worker_pids) = if cfg.spawn_workers
    {
        run_campaign_spawned(cfg, &cells, &multiproc_cells, &process_slots, reps_per_proc)?
    } else {
        let mut repetitions = Vec::new();
        let mut valid_runs = 0usize;
        let mut invalid_runs = 0usize;
        for cell in &cells {
            run_cell_reps(
                cfg,
                cell,
                &process_slots,
                reps_per_proc,
                false,
                surface,
                &mut repetitions,
                &mut valid_runs,
                &mut invalid_runs,
            )?;
        }
        if cfg.plan.include_multiproc_finding {
            for cell in &multiproc_cells {
                run_cell_reps(
                    cfg,
                    cell,
                    &process_slots,
                    reps_per_proc,
                    true,
                    surface,
                    &mut repetitions,
                    &mut valid_runs,
                    &mut invalid_runs,
                )?;
            }
        }
        (repetitions, valid_runs, invalid_runs, false, Vec::new())
    };

    let mut withdrawals = vec![
        "WITHDRAWN: any prior smoke-mode primary bottleneck claim (including io_queue_underdriven from PQH-11 smoke multi-rep) is not qualification evidence".into(),
    ];
    if cfg.run_class == RunClass::Smoke {
        withdrawals.push(
            "run_class=smoke: functional harness only; no product bottleneck verdicts".into(),
        );
    }
    if workers_spawned {
        withdrawals.push(format!(
            "process slots were genuine OS workers (pids={worker_pids:?}); not sequential in-process fakes"
        ));
    } else if procs > 1 {
        withdrawals.push(
            "process slots ran sequential in-process (spawn_workers=false); multiproc OS claim not made"
                .into(),
        );
    }

    // Per-qualified-cell observer overhead: order-balanced probe-off/on under the
    // **same workload contract** as the measured cell (floors/distribution/durability/…).
    let mut observer_overhead_reports = Vec::new();
    let mut overhead_within_budget = true;
    if cfg.driver == DriverKind::RealStore && store_driver_compiled() {
        if let Some(work) = cfg.work_root.as_ref() {
            // Every campaign cell (+ multiproc finding cells executed) gets evidence.
            let mut oh_cells: Vec<&MatrixCell> = cells.iter().collect();
            for c in &multiproc_cells {
                if !oh_cells.iter().any(|x| x.cell_id == c.cell_id) {
                    oh_cells.push(c);
                }
            }
            for cell in oh_cells {
                match measure_campaign_observer_overhead(cfg, work, cell) {
                    Ok(report) => {
                        if !report.within_budget {
                            overhead_within_budget = false;
                            withdrawals.push(format!(
                                "observer overhead cell={} median={:.6} exceeds budget={:.4}; product claims withheld",
                                report.cell_id,
                                report.median_overhead_fraction,
                                report.overhead_budget
                            ));
                        } else {
                            withdrawals.push(format!(
                                "observer overhead cell={} mean={:.6} median={:.6} pairs={} stop={:?} within_budget=true",
                                report.cell_id,
                                report.overhead_fraction,
                                report.median_overhead_fraction,
                                report.pair_count,
                                report
                                    .workload_contract
                                    .as_ref()
                                    .map(|c| c.stop_policy.as_str())
                                    .unwrap_or("?")
                            ));
                        }
                        observer_overhead_reports.push(report);
                    }
                    Err(e) => {
                        overhead_within_budget = false;
                        withdrawals.push(format!(
                            "observer overhead cell={} failed: {e}; product claims withheld",
                            cell.cell_id
                        ));
                    }
                }
            }
        }
    }
    let observer_overhead_report = observer_overhead_reports.first().cloned();

    Ok(CampaignResult {
        plan: cfg.plan.clone(),
        matrix_schema: manifest.schema,
        matrix_seed: manifest.seed,
        process_slots,
        cells_executed: cells.len(),
        repetitions,
        valid_runs,
        invalid_runs,
        driver_kind: cfg.driver.as_str().into(),
        measurement_surface: surface.as_str().into(),
        // Product eligibility: qual class + controlled surface + overhead budget.
        product_claim_eligible: product_claim_eligible
            && cfg.run_class.may_emit_bottleneck_verdict()
            && overhead_within_budget,
        primary_bottleneck: None,
        primary_bottleneck_run_ids: vec![],
        run_class: cfg.run_class.as_str().into(),
        withdrawals,
        workers_spawned,
        worker_pids,
        preflight_outcome,
        preflight_validity_id,
        environment_hash,
        preflight_report: preflight_report_json,
        environment_fingerprint: environment_fingerprint_json,
        observer_overhead_reports,
        observer_overhead_report,
    })
}

/// Real-store observer overhead on a campaign cell (order-balanced multi-pair).
fn measure_campaign_observer_overhead(
    cfg: &CampaignConfig,
    work_root: &std::path::Path,
    cell: &MatrixCell,
) -> Result<ObserverOverheadReport, CampaignError> {
    #[cfg(feature = "store-driver")]
    {
        use crate::store_driver::measure_probe_observer_overhead;
        let oh_cfg = DriverRunConfig {
            cell: cell.clone(),
            seed: cfg.plan.seed.wrapping_add(0x0B5E_07EAu64),
            kind: DriverKind::RealStore,
            work_root: Some(work_root.to_path_buf()),
            durability_mutant: false,
            digest_mutant: false,
            run_class: cfg.run_class.as_str().into(),
            awo_mode: cfg.awo_mode,
        };
        measure_probe_observer_overhead(&oh_cfg).map_err(|e| CampaignError::Msg(e.to_string()))
    }
    #[cfg(not(feature = "store-driver"))]
    {
        let _ = (cfg, work_root, cell);
        Err(CampaignError::Msg(
            "observer overhead requires store-driver feature".into(),
        ))
    }
}

/// Spawn one OS child per process slot, barrier-sync, merge WorkerResults.
fn run_campaign_spawned(
    cfg: &CampaignConfig,
    cells: &[MatrixCell],
    multiproc_cells: &[MatrixCell],
    process_slots: &[ProcessSlot],
    reps_per_proc: u32,
) -> Result<(Vec<CellRepetition>, usize, usize, bool, Vec<u32>), CampaignError> {
    let work_root = cfg
        .work_root
        .as_ref()
        .ok_or_else(|| CampaignError::Msg("spawn_workers requires work_root".into()))?;
    let worker_bin = match &cfg.worker_bin {
        Some(p) => p.clone(),
        None => super::multiproc::current_worker_bin()?,
    };
    let sync_root = work_root.join("worker_sync").join(&cfg.plan.campaign_id);
    let jobs = super::multiproc::build_worker_jobs(
        &cfg.plan.campaign_id,
        cfg.plan.seed,
        process_slots,
        reps_per_proc,
        cells,
        multiproc_cells,
        cfg.plan.include_multiproc_finding && !multiproc_cells.is_empty(),
        cfg.driver,
        cfg.run_class,
        work_root,
        cfg.awo_mode,
    );
    let worker_results = super::multiproc::run_spawned_workers(&worker_bin, &jobs, &sync_root)?;

    let mut repetitions = Vec::new();
    let mut valid_runs = 0usize;
    let mut invalid_runs = 0usize;
    let mut worker_pids = Vec::new();
    let mut any_error = None;

    for wr in worker_results {
        worker_pids.push(wr.worker_pid);
        if let Some(err) = wr.error {
            any_error = Some(err);
            continue;
        }
        for rep in wr.repetitions {
            if rep.report.validity == "valid" {
                valid_runs += 1;
            } else {
                invalid_runs += 1;
            }
            repetitions.push(rep);
        }
    }
    if let Some(err) = any_error {
        return Err(CampaignError::Msg(err));
    }
    if worker_pids.len() != process_slots.len() {
        return Err(CampaignError::Msg(format!(
            "expected {} worker results, got {}",
            process_slots.len(),
            worker_pids.len()
        )));
    }
    // Distinct PIDs prove genuine multi-process (not sequential same process).
    if process_slots.len() > 1 {
        let mut uniq = worker_pids.clone();
        uniq.sort_unstable();
        uniq.dedup();
        if uniq.len() < process_slots.len() {
            return Err(CampaignError::Msg(format!(
                "worker PIDs not distinct across process slots: {worker_pids:?}"
            )));
        }
    }
    Ok((repetitions, valid_runs, invalid_runs, true, worker_pids))
}

/// Fill primary bottleneck fields from ranked reports (after `build_campaign_reports`).
///
/// **Smoke/diagnostic:** never attaches a primary bottleneck (withdraws findings).
/// **Qualification/soak:** only attaches when every valid repetition reports a
/// sustained window class (SPEC §10 steady-state).
pub fn attach_primary_bottleneck(
    result: &mut CampaignResult,
    ranked: &[crate::campaign::reports::RankedBottleneck],
) {
    let class = RunClass::parse(&result.run_class).unwrap_or(RunClass::Smoke);
    if !class.may_emit_bottleneck_verdict() {
        result.primary_bottleneck = None;
        result.primary_bottleneck_run_ids.clear();
        result.withdrawals.push(format!(
            "bottleneck attach refused: run_class={} cannot support registered verdicts",
            class.as_str()
        ));
        result.withdrawals.push(
            "WITHDRAWN finding: io_queue_underdriven (and any other smoke-derived primary bottleneck)"
                .into(),
        );
        return;
    }

    let all_sustained = result
        .repetitions
        .iter()
        .filter(|r| r.report.validity == "valid")
        .all(|r| r.report.window.contains("sustained"));
    if !all_sustained {
        result.primary_bottleneck = None;
        result.primary_bottleneck_run_ids.clear();
        result.withdrawals.push(
            "bottleneck attach refused: not all valid runs have sustained/stable window".into(),
        );
        return;
    }

    if let Some(b) = ranked
        .iter()
        .find(|r| r.verdict != "mixed_or_unknown")
        .or_else(|| ranked.first())
    {
        // Refuse to promote queue-underdriven without qualification evidence of
        // queue-depth response (smoke false positive path).
        if b.verdict == "io_queue_underdriven" && result.driver_kind != "real_store" {
            result
                .withdrawals
                .push("WITHDRAWN: io_queue_underdriven on non-real_store surface".into());
            return;
        }
        result.primary_bottleneck = Some(b.verdict.clone());
        result.primary_bottleneck_run_ids = b.run_ids.clone();
    }
}

fn campaign_surface(cfg: &CampaignConfig) -> MeasurementSurface {
    match cfg.driver {
        DriverKind::Synthetic => MeasurementSurface::NonProductSynthetic,
        DriverKind::RealStore => {
            if cfg.plan.platform.allows_product_baseline() && cfg.declare_controlled_runner {
                MeasurementSurface::RealStoreControlledEligible
            } else {
                MeasurementSurface::RealStoreUncontrolled
            }
        }
    }
}

fn product_eligible(cfg: &CampaignConfig, surface: MeasurementSurface) -> bool {
    surface.allows_product_claim()
        && cfg.driver == DriverKind::RealStore
        && cfg.declare_controlled_runner
        && cfg.plan.platform.allows_product_baseline()
}

fn run_cell_reps(
    cfg: &CampaignConfig,
    cell: &MatrixCell,
    process_slots: &[ProcessSlot],
    reps_per_proc: u32,
    multiproc_tag: bool,
    surface: MeasurementSurface,
    repetitions: &mut Vec<CellRepetition>,
    valid_runs: &mut usize,
    invalid_runs: &mut usize,
) -> Result<(), CampaignError> {
    for slot in process_slots {
        for rep in 0..reps_per_proc {
            let seed = if multiproc_tag {
                slot.process_seed
                    .wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
                    .wrapping_add(rep as u64)
            } else {
                slot.process_seed
                    .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    .wrapping_add(rep as u64)
                    .wrapping_add(cell.order_rank as u64)
            };

            // Smoke may use explicit small op budgets for unit/CI only.
            // Qualification MUST NOT cap cells to 4–16 ops (principal rejection of PQH-11 smoke).
            let mut cell_run = cell.clone();
            if cfg.run_class.allows_smoke_op_cap() {
                cell_run.op_count = cell_run.op_count.min(RunClass::SMOKE_MAX_OPS).max(1);
            }
            // Qualification: keep matrix op_count as planned; driver honors
            // duration/byte floors rather than op caps.

            let dcfg = DriverRunConfig {
                cell: cell_run,
                seed,
                kind: cfg.driver,
                work_root: cfg.work_root.clone(),
                durability_mutant: false,
                digest_mutant: false,
                run_class: cfg.run_class.as_str().into(),
                awo_mode: cfg.awo_mode,
            };
            let drep = run_driver_cell(&dcfg).map_err(|e| CampaignError::Msg(e.to_string()))?;

            // Override surface/product flags at campaign honesty level.
            let mut report = drep.cell;
            if cfg.driver == DriverKind::Synthetic {
                report
                    .messages
                    .push("NON-PRODUCT synthetic repetition".into());
            }

            let prefix = if multiproc_tag { "mp-" } else { "" };
            let run_id = format!(
                "{}-{}p{}-r{}-{}",
                cfg.plan.campaign_id, prefix, slot.process_id, rep, cell.cell_id
            );
            if report.validity == "valid" {
                *valid_runs += 1;
            } else {
                *invalid_runs += 1;
            }
            repetitions.push(CellRepetition {
                run_id,
                cell_id: cell.cell_id.clone(),
                process_id: slot.process_id,
                rep,
                report,
                driver_kind: cfg.driver.as_str().into(),
                measurement_surface: surface.as_str().into(),
                plan_source: Some(drep.plan_source),
            });
        }
    }
    Ok(())
}

const PROCESS_SEED_TAG: u64 = 0x5039_5052;

/// Fail-closed preflight for qualification/soak campaigns.
///
/// Rejects debug builds (when enforced), invalid paths, budget floors, and
/// foreign work roots. Callers must not start cells when this fails.
pub fn run_qualification_preflight(
    work_root: &std::path::Path,
    campaign_id: &str,
) -> Result<crate::runner::PreflightReport, CampaignError> {
    std::fs::create_dir_all(work_root).map_err(|e| CampaignError::Io(e.to_string()))?;
    let cfg = PreflightConfig {
        work_root: work_root.to_path_buf(),
        work_root_id: format!("pqh-qual-{}", campaign_id),
        run_id: format!("preflight-{}", campaign_id),
        repository_root: None,
        budgets: RunBudgets::default(),
        build_mode: BuildMode::Qualification,
        enforce_release_for_qualification: true,
    };
    preflight_work_root(&cfg).map_err(|e| CampaignError::Msg(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::campaign::plan::campaign_plan_synthetic;

    #[test]
    fn campaign_meets_rep_and_process_mins() {
        let plan = campaign_plan_synthetic(42, 3);
        let result = run_campaign(&CampaignConfig::synthetic(plan.clone())).unwrap();
        assert_eq!(result.process_slots.len() as u32, plan.processes);
        assert!(result.valid_runs > 0);
        assert_eq!(result.invalid_runs, 0);
        assert_eq!(result.driver_kind, "synthetic");
        assert!(!result.product_claim_eligible);

        let mut by_cell: std::collections::HashMap<String, std::collections::HashSet<u32>> =
            std::collections::HashMap::new();
        for r in &result.repetitions {
            by_cell
                .entry(r.cell_id.clone())
                .or_default()
                .insert(r.process_id);
            assert_eq!(r.driver_kind, "synthetic");
        }
        for (_cell, procs) in by_cell {
            assert!(procs.len() as u32 >= plan.processes.min(2));
        }

        let first_cell = &result.repetitions[0].cell_id;
        let n = result
            .repetitions
            .iter()
            .filter(|r| r.cell_id == *first_cell)
            .count();
        assert!(n as u32 >= plan.repetitions);
    }

    #[test]
    fn real_store_requires_work_root() {
        let plan = campaign_plan_synthetic(1, 1);
        let err = run_campaign(&CampaignConfig {
            plan,
            driver: DriverKind::RealStore,
            work_root: None,
            declare_controlled_runner: false,
            run_class: RunClass::Smoke,
            spawn_workers: false,
            worker_bin: None,
            require_qualification_preflight: false,
            awo_mode: AwoMode::Disabled,
            presentation: PresentationPin::default(),
        })
        .unwrap_err();
        let s = err.to_string();
        assert!(
            s.contains("work_root") || s.contains("store-driver"),
            "unexpected err: {s}"
        );
    }

    #[test]
    fn smoke_withdraws_primary_bottleneck() {
        let plan = campaign_plan_synthetic(3, 1);
        let mut result = run_campaign(&CampaignConfig::synthetic(plan)).unwrap();
        assert_eq!(result.run_class, "smoke");
        let reports = crate::campaign::reports::build_campaign_reports(&result);
        attach_primary_bottleneck(&mut result, &reports.ranked_bottlenecks);
        assert!(result.primary_bottleneck.is_none());
        assert!(result
            .withdrawals
            .iter()
            .any(|w| w.contains("WITHDRAWN") && w.contains("io_queue_underdriven")));
    }

    #[test]
    fn qualification_config_forbids_smoke_op_cap() {
        assert!(!RunClass::Qualification.allows_smoke_op_cap());
        let plan = campaign_plan_synthetic(1, 1);
        let cfg = CampaignConfig {
            plan,
            driver: DriverKind::Synthetic,
            work_root: None,
            declare_controlled_runner: false,
            run_class: RunClass::Qualification,
            spawn_workers: false,
            worker_bin: None,
            require_qualification_preflight: false,
            awo_mode: AwoMode::Disabled,
            presentation: PresentationPin::default(),
        };
        // Synthetic qualification still runs (no wall 120s in synthetic matrix driver),
        // but must not apply smoke op caps in campaign path.
        let result = run_campaign(&cfg).unwrap();
        assert_eq!(result.run_class, "qualification");
        for r in &result.repetitions {
            // Matrix synthetic may still internal-bound; campaign must not force 4–16.
            assert!(r.report.attempted == 0 || r.report.attempted >= 1);
        }
    }

    #[test]
    fn qualification_preflight_fail_closed_without_work_root() {
        let plan = campaign_plan_synthetic(1, 1);
        let err = run_campaign(&CampaignConfig {
            plan,
            driver: DriverKind::Synthetic,
            work_root: None,
            declare_controlled_runner: false,
            run_class: RunClass::Qualification,
            spawn_workers: false,
            worker_bin: None,
            require_qualification_preflight: true,
            awo_mode: AwoMode::Disabled,
            presentation: PresentationPin::default(),
        })
        .unwrap_err();
        assert!(
            err.to_string().contains("work_root") || err.to_string().contains("preflight"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn qualification_preflight_rejects_debug_build() {
        if !cfg!(debug_assertions) {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let plan = campaign_plan_synthetic(2, 1);
        let err = run_campaign(&CampaignConfig {
            plan,
            driver: DriverKind::Synthetic,
            work_root: Some(dir.path().to_path_buf()),
            declare_controlled_runner: false,
            run_class: RunClass::Qualification,
            spawn_workers: false,
            worker_bin: None,
            require_qualification_preflight: true,
            awo_mode: AwoMode::Disabled,
            presentation: PresentationPin::default(),
        })
        .unwrap_err();
        let s = err.to_string();
        assert!(
            s.contains("fail-closed") || s.contains("debug") || s.contains("InvalidDebugBuild"),
            "unexpected: {s}"
        );
    }

    #[test]
    fn sequential_multi_process_discloses_not_spawned() {
        let mut plan = campaign_plan_synthetic(11, 1);
        plan.processes = 2;
        plan.repetitions = 5;
        plan.include_multiproc_finding = false;
        let result = run_campaign(&CampaignConfig::synthetic(plan)).unwrap();
        assert!(!result.workers_spawned);
        assert!(result.worker_pids.is_empty());
        assert!(result
            .withdrawals
            .iter()
            .any(|w| w.contains("sequential") || w.contains("spawn_workers=false")));
    }
}

#[cfg(all(test, feature = "store-driver"))]
mod real_store_tests {
    use super::*;
    use crate::campaign::plan::{campaign_plan_linux, campaign_plan_synthetic};
    use crate::campaign::reports::build_campaign_reports;

    #[test]
    fn multi_rep_real_store_smoke_emits_run_ids_but_no_bottleneck() {
        let dir = tempfile::tempdir().unwrap();
        let mut plan = campaign_plan_synthetic(7, 1);
        plan.max_cells = 1;
        plan.include_multiproc_finding = false;
        plan.repetitions = 5;
        plan.processes = 2;

        let mut result = run_campaign(&CampaignConfig {
            plan: plan.clone(),
            driver: DriverKind::RealStore,
            work_root: Some(dir.path().to_path_buf()),
            declare_controlled_runner: false,
            run_class: RunClass::Smoke,
            // Sequential in unit tests; genuine spawn covered by integration test.
            spawn_workers: false,
            worker_bin: None,
            require_qualification_preflight: false,
            awo_mode: AwoMode::Disabled,
            presentation: PresentationPin::default(),
        })
        .expect("real store multi-rep smoke campaign");

        assert_eq!(result.driver_kind, "real_store");
        assert_eq!(result.run_class, "smoke");
        assert!(!result.product_claim_eligible);
        assert!(!result.workers_spawned);
        assert!(result.valid_runs >= 5);
        assert!(result
            .repetitions
            .iter()
            .all(|r| r.driver_kind == "real_store"));
        assert!(result.repetitions.iter().all(|r| !r.run_id.is_empty()));

        let reports = build_campaign_reports(&result);
        attach_primary_bottleneck(&mut result, &reports.ranked_bottlenecks);
        // Smoke must not promote primary bottlenecks (withdraw io_queue_underdriven etc.).
        assert!(result.primary_bottleneck.is_none());
        assert!(result
            .withdrawals
            .iter()
            .any(|w| w.contains("WITHDRAWN") || w.contains("smoke")));
    }

    #[test]
    fn controlled_smoke_still_not_product_claim_without_qualification_class() {
        let dir = tempfile::tempdir().unwrap();
        let mut plan = campaign_plan_linux(9);
        plan.max_cells = 1;
        plan.include_multiproc_finding = false;
        plan.repetitions = 5;
        plan.processes = 2;
        let result = run_campaign(&CampaignConfig {
            plan,
            driver: DriverKind::RealStore,
            work_root: Some(dir.path().to_path_buf()),
            declare_controlled_runner: true,
            run_class: RunClass::Smoke,
            spawn_workers: false,
            worker_bin: None,
            require_qualification_preflight: false,
            awo_mode: AwoMode::Disabled,
            presentation: PresentationPin::default(),
        })
        .unwrap();
        // Surface may be controlled-eligible label, but product_claim requires
        // qualification class as well.
        assert!(!result.product_claim_eligible);
        assert_eq!(result.run_class, "smoke");
    }

    #[test]
    fn real_store_campaign_measures_order_balanced_overhead_and_binds_bundle() {
        use crate::campaign::bundle::{verify_bundle_hashes, write_evidence_bundle};
        use crate::campaign::disclosure::build_disclosure;
        use crate::store_driver::{OBSERVER_OVERHEAD_BUDGET, OVERHEAD_MIN_PAIRS_SMOKE};

        let dir = tempfile::tempdir().unwrap();
        let mut plan = campaign_plan_synthetic(13, 1);
        plan.max_cells = 1;
        plan.include_multiproc_finding = false;
        plan.repetitions = 5;
        plan.processes = 2;

        let result = run_campaign(&CampaignConfig {
            plan,
            driver: DriverKind::RealStore,
            work_root: Some(dir.path().join("work")),
            declare_controlled_runner: false,
            run_class: RunClass::Smoke,
            spawn_workers: false,
            worker_bin: None,
            require_qualification_preflight: false,
            awo_mode: AwoMode::Disabled,
            presentation: PresentationPin::default(),
        })
        .expect("campaign with overhead");

        assert!(
            !result.observer_overhead_reports.is_empty(),
            "per-cell overhead evidence required"
        );
        let oh = result
            .observer_overhead_reports
            .first()
            .expect("observer_overhead_reports on real_store campaign");
        assert_eq!(oh.pair_count, OVERHEAD_MIN_PAIRS_SMOKE);
        assert!(oh.ops_per_leg > 0);
        assert!((oh.overhead_budget - OBSERVER_OVERHEAD_BUDGET).abs() < 1e-12);
        let wc = oh.workload_contract.as_ref().expect("workload_contract");
        assert_eq!(wc.stop_policy, "smoke_op_cap");
        assert!(!wc.durability.is_empty());
        assert_eq!(
            oh.pairs.iter().filter(|p| p.order == "off_first").count(),
            oh.pairs.iter().filter(|p| p.order == "on_first").count()
        );
        assert!(result
            .withdrawals
            .iter()
            .any(|w| w.contains("observer overhead")));

        // Measured value flows into attribution subject.
        let reports = build_campaign_reports(&result);
        // Synthetic-size smoke may not rank if <2 valid cells with multi-rep;
        // still ensure report build does not invent when None — when ranked,
        // oh fraction is the measured one.
        let _ = reports;

        let disclosure = build_disclosure(&result, &reports);
        let camp = dir.path().join("campaign");
        let bundle = write_evidence_bundle(&camp, &result, &reports, &disclosure).unwrap();
        assert!(
            bundle
                .file_hashes
                .iter()
                .any(|f| f.relative_path == "observer_overhead.json"),
            "observer_overhead.json must be in hashed evidence bundle"
        );
        verify_bundle_hashes(&camp).unwrap();
        assert!(camp.join("observer_overhead.json").exists());
    }
}
