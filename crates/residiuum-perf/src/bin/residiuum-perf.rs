//! Runnable PQH campaign CLI (plan §14 surface subset).
//!
//! ```text
//! residiuum-perf preflight --work <dir>
//! residiuum-perf run --work <dir> [--driver synthetic|real_store] [--max-cells N]
//! residiuum-perf analyze --campaign <dir>
//! residiuum-perf verify --campaign <dir>
//! residiuum-perf driver-smoke --work <dir> [--driver synthetic|real_store]
//!     [--awo-mode disabled|static|adaptive]
//! residiuum-perf worker --job <path> --out <path> --ready <path> --go <path>
//! ```
//!
//! Synthetic/proxy results are always labelled non-product. Real-store driver
//! requires `--features store-driver` at build time. No optimisations applied.
//! Multi-process campaigns spawn genuine OS workers (barrier-synced).

use residiuum_perf::campaign::{
    attach_primary_bottleneck, build_campaign_reports, build_disclosure, campaign_plan_linux,
    campaign_plan_macos_apple_silicon, campaign_plan_synthetic, run_campaign, run_worker_job,
    verify_bundle_hashes, write_evidence_bundle, CampaignConfig, PlatformClass, PresentationPin,
    RunClass, WorkerJob,
};
use residiuum_perf::matrix::{build_matrix_cells, ScheduleSeed};
use residiuum_perf::runner::{
    environment_fingerprint, preflight_work_root, write_run_artifacts, BuildMode, PreflightConfig,
    RunBudgets,
};
use residiuum_perf::store_driver::{
    run_driver_cell, store_driver_compiled, AwoMode, DriverKind, DriverRunConfig,
};
use residiuum_perf::PROFILE_ID;
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() || args.iter().any(|a| a == "-h" || a == "--help") {
        print_help();
        return ExitCode::SUCCESS;
    }
    let cmd = args.remove(0);
    let result = match cmd.as_str() {
        "preflight" => cmd_preflight(&args),
        "run" => cmd_run(&args),
        "analyze" => cmd_analyze(&args),
        "verify" => cmd_verify(&args),
        "driver-smoke" => cmd_driver_smoke(&args),
        "worker" => cmd_worker(&args),
        other => Err(format!("unknown command {other}; see --help")),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn print_help() {
    println!(
        "residiuum-perf — Performance Qualification Harness CLI
profile: {PROFILE_ID}
store-driver compiled: {}

Commands:
  preflight --work <dir> [--qualification]
  run --work <dir> [--driver synthetic|real_store] [--max-cells N] [--seed N]
      [--platform synthetic|macos_as|linux] [--class smoke|diagnostic|qualification|soak]
      [--controlled] [--spawn-workers|--no-spawn-workers]
      [--awo-mode disabled|static|adaptive]
      [--present-batch N] [--present-concurrency N] [--present-outstanding N]
        (pin presentation after matrix pick; AWO-fair: keep batch=1, vary conc/out)
  analyze --campaign <dir>
  verify --campaign <dir>
  driver-smoke --work <dir> [--driver synthetic|real_store]
      [--awo-mode disabled|static|adaptive]
  worker --job <path> --out <path> --ready <path> --go <path>
      (internal: barrier-synced process-slot child; do not invoke by hand)

Honesty:
  Default --class is smoke (functional only; NO bottleneck verdicts).
  Qualification requires --class qualification (120s + 512MiB floors; no op caps).
  Multi-process runs spawn genuine OS workers by default (barrier-synced).
  synthetic/proxy results are NON-PRODUCT.
  real_store needs --features store-driver.
  --awo-mode (default disabled): real_store only; static/adaptive attach AWO lease.
  --present-batch/concurrency/outstanding: pin presentation (do not use batch>1 for AWO ranking).
  --controlled only on a declared controlled runner + qualification class.
  No optimisations. Smoke-derived io_queue_underdriven is WITHDRAWN.
  AWO three-way numbers are diagnostic unless a controlled qualification campaign says otherwise.
",
        store_driver_compiled()
    );
}

fn flag_value(args: &[String], name: &str) -> Option<String> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == name {
            return args.get(i + 1).cloned();
        }
        if let Some(rest) = args[i].strip_prefix(&format!("{name}=")) {
            return Some(rest.to_string());
        }
        i += 1;
    }
    None
}

fn has_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

fn parse_awo_mode(args: &[String]) -> Result<AwoMode, String> {
    let raw = flag_value(args, "--awo-mode").unwrap_or_else(|| "disabled".into());
    AwoMode::parse(&raw)
        .ok_or_else(|| format!("bad --awo-mode {raw}; expected disabled|static|adaptive"))
}

fn parse_presentation_pin(args: &[String]) -> Result<PresentationPin, String> {
    let parse_u32 = |name: &str| -> Result<Option<u32>, String> {
        match flag_value(args, name) {
            None => Ok(None),
            Some(s) => s
                .parse::<u32>()
                .map(Some)
                .map_err(|_| format!("bad {name} {s}; expected u32")),
        }
    };
    Ok(PresentationPin {
        batch_size: parse_u32("--present-batch")?,
        concurrency: parse_u32("--present-concurrency")?,
        outstanding: parse_u32("--present-outstanding")?,
    })
}

fn cmd_preflight(args: &[String]) -> Result<(), String> {
    let work = PathBuf::from(flag_value(args, "--work").ok_or("--work required")?);
    std::fs::create_dir_all(&work).map_err(|e| e.to_string())?;
    let qual = has_flag(args, "--qualification");
    let work_root_id = format!("pqh-work-{:x}", std::process::id());
    let cfg = PreflightConfig {
        work_root: work.clone(),
        work_root_id: work_root_id.clone(),
        run_id: "preflight".into(),
        repository_root: None,
        budgets: RunBudgets::default(),
        build_mode: if qual {
            BuildMode::Qualification
        } else {
            BuildMode::Diagnostic
        },
        enforce_release_for_qualification: qual,
    };
    let report = preflight_work_root(&cfg).map_err(|e| e.to_string())?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?
    );
    if report.outcome.is_ready() {
        let env = environment_fingerprint(Some(&work), cfg.build_mode).ok();
        let _ = write_run_artifacts(
            &work,
            &work_root_id,
            "preflight-scaffold",
            &report,
            env.as_ref(),
        );
    }
    Ok(())
}

fn cmd_run(args: &[String]) -> Result<(), String> {
    let work = PathBuf::from(flag_value(args, "--work").ok_or("--work required")?);
    std::fs::create_dir_all(&work).map_err(|e| e.to_string())?;
    let seed: u64 = flag_value(args, "--seed")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let max_cells: usize = flag_value(args, "--max-cells")
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let driver = flag_value(args, "--driver").unwrap_or_else(|| "synthetic".into());
    let kind = DriverKind::parse(&driver).ok_or_else(|| format!("bad --driver {driver}"))?;
    if kind == DriverKind::RealStore && !store_driver_compiled() {
        return Err("real_store driver not compiled; rebuild with --features store-driver".into());
    }

    let platform = flag_value(args, "--platform").unwrap_or_else(|| "synthetic".into());
    let mut plan = match platform.as_str() {
        "macos_as" | "macos" => campaign_plan_macos_apple_silicon(seed),
        "linux" => campaign_plan_linux(seed),
        _ => campaign_plan_synthetic(seed, max_cells),
    };
    plan.max_cells = max_cells;
    if kind == DriverKind::Synthetic {
        plan.platform = PlatformClass::SyntheticHarness;
        plan.notes
            .push("CLI run with synthetic driver — NON-PRODUCT".into());
    }
    let controlled = has_flag(args, "--controlled");
    if controlled && kind != DriverKind::RealStore {
        return Err("--controlled requires --driver real_store".into());
    }
    let class_s = flag_value(args, "--class").unwrap_or_else(|| "smoke".into());
    let run_class = RunClass::parse(&class_s).ok_or_else(|| format!("bad --class {class_s}"))?;
    if controlled && !run_class.may_emit_bottleneck_verdict() {
        return Err("--controlled requires --class qualification (or soak)".into());
    }
    let awo_mode = parse_awo_mode(args)?;
    if awo_mode.lease_active() && kind != DriverKind::RealStore {
        return Err("--awo-mode static|adaptive requires --driver real_store".into());
    }
    let presentation = parse_presentation_pin(args)?;
    if presentation.is_active() {
        plan.include_multiproc_finding = false;
        plan.notes.push(format!(
            "presentation_pin batch={:?} concurrency={:?} outstanding={:?} (AWO-fair shapes; multiproc finding off)",
            presentation.batch_size, presentation.concurrency, presentation.outstanding
        ));
    }

    // Genuine multiproc: default spawn when processes > 1. Override with flags.
    let spawn_workers = if has_flag(args, "--no-spawn-workers") {
        false
    } else if has_flag(args, "--spawn-workers") {
        true
    } else {
        plan.processes > 1
    };
    let worker_bin = std::env::current_exe().ok();

    let mut result = run_campaign(&CampaignConfig {
        plan: plan.clone(),
        driver: kind,
        work_root: Some(work.clone()),
        declare_controlled_runner: controlled,
        run_class,
        spawn_workers,
        worker_bin,
        require_qualification_preflight: run_class.may_emit_bottleneck_verdict(),
        awo_mode,
        presentation,
    })
    .map_err(|e| e.to_string())?;

    let mut reports = build_campaign_reports(&result);
    attach_primary_bottleneck(&mut result, &reports.ranked_bottlenecks);
    reports.notes.extend(result.withdrawals.iter().cloned());

    if kind == DriverKind::Synthetic || run_class == RunClass::Smoke {
        reports.notes.push(
            "NON-PRODUCT / smoke: not qualification evidence; no absolute MB/s claims".into(),
        );
        reports.multiproc_finding.overstates_product = false;
    }
    if kind == DriverKind::RealStore {
        reports.notes.push(format!(
            "real_store multi-rep run_class={} valid_runs={} product_claim_eligible={} surface={} workers_spawned={} worker_pids={:?}",
            result.run_class,
            result.valid_runs,
            result.product_claim_eligible,
            result.measurement_surface,
            result.workers_spawned,
            result.worker_pids,
        ));
        if run_class == RunClass::Smoke {
            reports.notes.push(
                "WITHDRAWN: smoke-mode primary bottlenecks (incl. io_queue_underdriven)".into(),
            );
        }
        if let Some(v) = &result.primary_bottleneck {
            reports.notes.push(format!(
                "primary_bottleneck={} run_ids={}",
                v,
                result.primary_bottleneck_run_ids.join(",")
            ));
        } else {
            reports.notes.push(
                "no primary bottleneck attached (smoke or missing sustained window/floors)".into(),
            );
        }
        reports
            .notes
            .push("NO optimisations applied; follow-up cards are stubs only".into());
        reports.multiproc_finding.overstates_product = false;
    }

    let disclosure = build_disclosure(&result, &reports);
    let campaign_dir = work.join("campaign").join(&plan.campaign_id);
    let bundle = write_evidence_bundle(&campaign_dir, &result, &reports, &disclosure)
        .map_err(|e| e.to_string())?;
    println!(
        "{}",
        serde_json::json!({
            "ok": true,
            "campaign_dir": campaign_dir,
            "campaign_id": plan.campaign_id,
            "driver": kind.as_str(),
            "awo_mode": awo_mode.as_str(),
            "run_class": result.run_class,
            "platform": plan.platform.as_str(),
            "measurement_surface": result.measurement_surface,
            "product_claim_eligible": result.product_claim_eligible,
            "valid_runs": result.valid_runs,
            "workers_spawned": result.workers_spawned,
            "worker_pids": result.worker_pids,
            "primary_bottleneck": result.primary_bottleneck,
            "primary_bottleneck_run_ids": result.primary_bottleneck_run_ids,
            "withdrawals": result.withdrawals,
            "content_hash": bundle.content_hash,
            "non_product": kind == DriverKind::Synthetic
                || run_class == RunClass::Smoke
                || !result.product_claim_eligible,
            "notes": reports.notes,
        })
    );
    Ok(())
}

/// Child entry for barrier-synced process slots (invoked by `run_spawned_workers`).
fn cmd_worker(args: &[String]) -> Result<(), String> {
    let job_path = PathBuf::from(flag_value(args, "--job").ok_or("--job required")?);
    let out_path = PathBuf::from(flag_value(args, "--out").ok_or("--out required")?);
    let ready_path = PathBuf::from(flag_value(args, "--ready").ok_or("--ready required")?);
    let go_path = PathBuf::from(flag_value(args, "--go").ok_or("--go required")?);

    let raw = std::fs::read_to_string(&job_path).map_err(|e| e.to_string())?;
    let job: WorkerJob = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
    match run_worker_job(&job, &ready_path, &go_path) {
        Ok(result) => {
            let json = serde_json::to_string_pretty(&result).map_err(|e| e.to_string())?;
            std::fs::write(&out_path, json).map_err(|e| e.to_string())?;
            Ok(())
        }
        Err(e) => {
            // Still write a result with error so the parent can collect PIDs.
            let err_result = residiuum_perf::campaign::WorkerResult {
                process_id: job.process_id,
                worker_pid: std::process::id(),
                spawned: true,
                repetitions: vec![],
                error: Some(e.to_string()),
            };
            let _ = std::fs::write(
                &out_path,
                serde_json::to_string_pretty(&err_result).unwrap_or_default(),
            );
            Err(e.to_string())
        }
    }
}

fn cmd_analyze(args: &[String]) -> Result<(), String> {
    let campaign = PathBuf::from(flag_value(args, "--campaign").ok_or("--campaign required")?);
    let disclosure_md = campaign.join("DISCLOSURE.md");
    if disclosure_md.exists() {
        let s = std::fs::read_to_string(&disclosure_md).map_err(|e| e.to_string())?;
        println!("{s}");
    }
    let reports_path = campaign.join("reports.json");
    if reports_path.exists() {
        let raw = std::fs::read_to_string(&reports_path).map_err(|e| e.to_string())?;
        let reports: residiuum_perf::campaign::CampaignReports =
            serde_json::from_str(&raw).map_err(|e| e.to_string())?;
        println!("\n## Ranked bottlenecks (from reports.json)\n");
        for b in &reports.ranked_bottlenecks {
            println!(
                "{}. {} ({}) runs={}",
                b.rank,
                b.verdict,
                b.confidence,
                b.run_ids.len()
            );
        }
        if reports.notes.iter().any(|n| n.contains("NON-PRODUCT")) {
            println!("\nNOTE: this campaign is labelled NON-PRODUCT.\n");
        }
        println!("No optimisations applied (PQH policy).\n");
    } else if !disclosure_md.exists() {
        return Err("campaign dir missing DISCLOSURE.md and reports.json".into());
    }
    Ok(())
}

fn cmd_verify(args: &[String]) -> Result<(), String> {
    let campaign = PathBuf::from(flag_value(args, "--campaign").ok_or("--campaign required")?);
    verify_bundle_hashes(&campaign).map_err(|e| e.to_string())?;
    println!(
        "{}",
        serde_json::json!({"ok": true, "campaign": campaign, "hashes": "verified"})
    );
    Ok(())
}

fn cmd_driver_smoke(args: &[String]) -> Result<(), String> {
    let work = PathBuf::from(flag_value(args, "--work").ok_or("--work required")?);
    std::fs::create_dir_all(&work).map_err(|e| e.to_string())?;
    let driver = flag_value(args, "--driver").unwrap_or_else(|| "synthetic".into());
    let kind = DriverKind::parse(&driver).ok_or_else(|| format!("bad --driver {driver}"))?;
    if kind == DriverKind::RealStore && !store_driver_compiled() {
        return Err("rebuild with --features store-driver".into());
    }
    let awo_mode = parse_awo_mode(args)?;
    if awo_mode.lease_active() && kind != DriverKind::RealStore {
        return Err("--awo-mode static|adaptive requires --driver real_store".into());
    }
    let manifest = build_matrix_cells(ScheduleSeed { seed: 1 });
    let cell = manifest.cells.first().cloned().ok_or("empty matrix")?;
    let report = run_driver_cell(&DriverRunConfig {
        cell,
        seed: 1,
        kind,
        work_root: Some(work),
        durability_mutant: false,
        digest_mutant: false,
        run_class: "smoke".into(),
        awo_mode,
    })
    .map_err(|e| e.to_string())?;
    println!(
        "{}",
        serde_json::json!({
            "driver": report.driver_kind.as_str(),
            "awo_mode": awo_mode.as_str(),
            "surface": report.measurement_surface.as_str(),
            "product_claim_eligible": report.product_claim_eligible,
            "validity": report.cell.validity,
            "acknowledged": report.cell.acknowledged,
            "plan_source": report.plan_source,
            "planned_bytes": report.plan.as_ref().map(|p| p.planned_bytes),
            "notes": report.notes,
        })
    );
    Ok(())
}
