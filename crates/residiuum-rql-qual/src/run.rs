//! Smoke runner + evidence publication (Q4.3).
//!
//! Runs mandatory cell plans against:
//! - logical harness (Ready digests + metrics)
//! - Mongo/CBL adapters (shared work loaded; execute NotConfigured)
//!
//! Publishes hashed evidence bundles. **Not competitive** / not Gate-1.

use crate::cell_plan::{section_7_2_expanded_portfolio, smoke_portfolio, MeasuredCellPlan};
use crate::concurrent::{run_logical_concurrent, ConcurrentError};
use crate::engine::{
    AdapterStatus, CblEmbeddedAdapter, EngineAdapter, EngineRunOutcome, MongoLocalAdapter,
};
#[cfg(feature = "residiuum-embedded")]
use crate::engine::{ExecutionKind, ResidiuumEmbeddedAdapter};
#[cfg(feature = "residiuum-embedded")]
use crate::evidence::RawRepetition;
use crate::evidence::{
    compare_ready_outcomes, scaffold_cell_with_concurrency, EnvFingerprint, EvidenceBundle,
    EvidenceError,
};
use crate::generator::generate_dataset;
use crate::lane::LanePairing;
use crate::shared_work::SharedLogicalWork;
#[cfg(feature = "residiuum-embedded")]
use crate::{
    cells::MandatoryCell,
    dataset::{DatasetSpec, MemoryRatio, PayloadClass},
    generator::LogicalDataset,
    lifecycle::{ColdMethod, LifecycleSpec},
    metrics::LifecycleClass,
};
use serde_json::{json, Value};
#[cfg(feature = "residiuum-embedded")]
use sha2::{Digest, Sha256};
#[cfg(feature = "residiuum-embedded")]
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("evidence: {0}")]
    Evidence(#[from] EvidenceError),
    #[error("adapter: {0}")]
    Adapter(String),
    #[error("concurrency: {0}")]
    Concurrency(#[from] ConcurrentError),
    #[error("io: {0}")]
    Io(String),
}

/// Result of one smoke cell on lane E pairing (logical vs CBL stub).
#[derive(Debug, Clone)]
pub struct SmokeCellResult {
    pub plan_id: String,
    pub side_a: EngineRunOutcome,
    pub side_b: EngineRunOutcome,
    pub shared_hash: String,
    /// Plan-requested concurrency (§7.2).
    pub requested_concurrency: u32,
    /// Peak simultaneous logical workers observed (F10).
    pub achieved_concurrency: u32,
}

/// One product-backed concurrency proof for a mandatory Q4 cell.
#[cfg(feature = "residiuum-embedded")]
#[derive(Debug, Clone)]
pub struct ProductConcurrencyCellResult {
    pub cell_id: String,
    pub plan_id: String,
    pub requested_concurrency: u32,
    pub achieved_concurrency: u32,
    pub oversubscribed: bool,
    pub workload_wall_ns: u64,
    pub workload_operations: u64,
    pub aggregate_ops_per_s: f64,
    pub outcome: EngineRunOutcome,
}

#[cfg(feature = "residiuum-embedded")]
fn achieved_concurrency(detail: &str) -> Option<u32> {
    detail
        .split_whitespace()
        .find_map(|part| part.strip_prefix("concurrency_achieved="))
        .and_then(|value| value.parse().ok())
}

#[cfg(feature = "residiuum-embedded")]
fn detail_value<'a>(detail: &'a str, key: &str) -> Option<&'a str> {
    detail
        .split_whitespace()
        .find_map(|part| part.strip_prefix(key))
}

#[cfg(feature = "residiuum-embedded")]
fn scaling_observation(detail: &str) -> Option<(u64, u64, f64)> {
    let wall = detail_value(detail, "workload_wall_ns=")?.parse().ok()?;
    let operations = detail_value(detail, "workload_operations=")?.parse().ok()?;
    let throughput = detail_value(detail, "aggregate_ops_per_s=")?.parse().ok()?;
    Some((wall, operations, throughput))
}

/// Run every mandatory Q4 cell through the real embedded product using one
/// bounded smart-client connection shared by all workers in that cell.
#[cfg(feature = "residiuum-embedded")]
pub fn run_product_concurrency_smoke(
    seed: u64,
    concurrency: u32,
) -> Result<Vec<ProductConcurrencyCellResult>, RunError> {
    if concurrency < 2 {
        return Err(RunError::Adapter(
            "product concurrency proof requires at least two workers".into(),
        ));
    }
    let mut results = Vec::with_capacity(crate::cells::MandatoryCell::ALL.len());
    for cell in crate::cells::MandatoryCell::ALL {
        let plan = MeasuredCellPlan::smoke_for(*cell, seed).with_concurrency(concurrency, false);
        let work = SharedLogicalWork::from_dataset(generate_dataset(&plan.dataset));
        let mut adapter = ResidiuumEmbeddedAdapter::default();
        adapter
            .load_shared_work(&work)
            .map_err(|error| RunError::Adapter(error.to_string()))?;
        let outcome = adapter
            .execute_plan(&plan)
            .map_err(|error| RunError::Adapter(error.to_string()))?;
        let achieved = outcome
            .detail
            .as_deref()
            .and_then(achieved_concurrency)
            .ok_or_else(|| {
                RunError::Adapter(format!(
                    "product outcome omitted achieved concurrency: {}",
                    plan.plan_id
                ))
            })?;
        let (workload_wall_ns, workload_operations, aggregate_ops_per_s) = outcome
            .detail
            .as_deref()
            .and_then(scaling_observation)
            .ok_or_else(|| {
                RunError::Adapter(format!(
                    "product outcome omitted scaling observation: {}",
                    plan.plan_id
                ))
            })?;
        if outcome.execution_kind != ExecutionKind::Product
            || !outcome.is_product_ready()
            || outcome.metrics.is_none()
            || outcome.shared_work_hash.as_deref() != Some(work.content_hash.as_str())
            || achieved != concurrency
            || workload_wall_ns == 0
            || workload_operations == 0
            || !aggregate_ops_per_s.is_finite()
            || aggregate_ops_per_s <= 0.0
        {
            return Err(RunError::Adapter(format!(
                "invalid product concurrency proof plan={} requested={} achieved={} status={:?}",
                plan.plan_id, concurrency, achieved, outcome.status
            )));
        }
        results.push(ProductConcurrencyCellResult {
            cell_id: cell.id().into(),
            plan_id: plan.plan_id,
            requested_concurrency: concurrency,
            achieved_concurrency: achieved,
            oversubscribed: false,
            workload_wall_ns,
            workload_operations,
            aggregate_ops_per_s,
            outcome,
        });
    }
    Ok(results)
}

/// Host declaration used for the single oversubscribed §7.2 slot. Twice the
/// available logical parallelism is explicit, reproducible in the report, and
/// guaranteed to exceed the host capacity reported to this process.
#[cfg(feature = "residiuum-embedded")]
pub fn product_oversubscribed_concurrency() -> u32 {
    std::thread::available_parallelism()
        .map(|value| value.get() as u32)
        .unwrap_or(1)
        .saturating_mul(2)
        .max(16)
}

/// Execute the complete §7.2 product concurrency matrix through the smart
/// client: all twelve mandatory cells at 1/2/4/8 plus one host-derived
/// oversubscribed level.
#[cfg(feature = "residiuum-embedded")]
pub fn run_product_scaling_matrix(
    seed: u64,
) -> Result<(u32, Vec<ProductConcurrencyCellResult>), RunError> {
    let host_parallelism = std::thread::available_parallelism()
        .map(|value| value.get() as u32)
        .unwrap_or(1);
    let oversubscribed = product_oversubscribed_concurrency();
    let levels = [1, 2, 4, 8, oversubscribed];
    let mut results = Vec::with_capacity(crate::cells::MandatoryCell::ALL.len() * levels.len());
    for cell in crate::cells::MandatoryCell::ALL {
        for concurrency in levels {
            let is_oversubscribed = concurrency == oversubscribed;
            let plan = MeasuredCellPlan::smoke_for(*cell, seed)
                .with_concurrency(concurrency, is_oversubscribed);
            let work = SharedLogicalWork::from_dataset(generate_dataset(&plan.dataset));
            let outcome =
                crate::residiuum_embedded::execute_plan_embedded_smart_client(&work, &plan)
                    .map_err(|error| RunError::Adapter(error.to_string()))?;
            let achieved = outcome
                .detail
                .as_deref()
                .and_then(achieved_concurrency)
                .ok_or_else(|| {
                    RunError::Adapter(format!(
                        "scaling outcome omitted achieved concurrency: {}",
                        plan.plan_id
                    ))
                })?;
            let (workload_wall_ns, workload_operations, aggregate_ops_per_s) = outcome
                .detail
                .as_deref()
                .and_then(scaling_observation)
                .ok_or_else(|| {
                    RunError::Adapter(format!(
                        "scaling outcome omitted workload observation: {}",
                        plan.plan_id
                    ))
                })?;
            if !outcome.is_product_ready()
                || outcome.metrics.is_none()
                || outcome.shared_work_hash.as_deref() != Some(work.content_hash.as_str())
                || achieved != concurrency
                || workload_wall_ns == 0
                || workload_operations == 0
                || !aggregate_ops_per_s.is_finite()
                || aggregate_ops_per_s <= 0.0
            {
                return Err(RunError::Adapter(format!(
                    "invalid product scaling proof plan={} requested={} achieved={} status={:?}",
                    plan.plan_id, concurrency, achieved, outcome.status
                )));
            }
            results.push(ProductConcurrencyCellResult {
                cell_id: cell.id().into(),
                plan_id: plan.plan_id,
                requested_concurrency: concurrency,
                achieved_concurrency: achieved,
                oversubscribed: is_oversubscribed,
                workload_wall_ns,
                workload_operations,
                aggregate_ops_per_s,
                outcome,
            });
        }
    }
    Ok((host_parallelism, results))
}

/// Publish the explicit product scaling campaign. It is intentionally a
/// single-repetition engineering campaign, not a Q5 competitive baseline.
#[cfg(feature = "residiuum-embedded")]
pub fn publish_product_scaling_evidence(
    workspace_root: &Path,
    seed: u64,
) -> Result<PathBuf, RunError> {
    let (host_parallelism, results) = run_product_scaling_matrix(seed)?;
    let oversubscribed = product_oversubscribed_concurrency();
    let cells: Vec<Value> = results
        .iter()
        .map(|result| {
            json!({
                "cell_id": result.cell_id,
                "plan_id": result.plan_id,
                "requested_concurrency": result.requested_concurrency,
                "achieved_concurrency": result.achieved_concurrency,
                "oversubscribed": result.oversubscribed,
                "workload_wall_ns": result.workload_wall_ns,
                "workload_operations": result.workload_operations,
                "aggregate_ops_per_s": result.aggregate_ops_per_s,
                "one_physical_connection": true,
                "product_ready": result.outcome.is_product_ready(),
                "outcome": result.outcome,
            })
        })
        .collect();
    let report = json!({
        "format": "residiuum-rql-q4-product-scaling-report-v1",
        "harness_profile": crate::HARNESS_PROFILE,
        "seed": seed,
        "host": {
            "available_parallelism": host_parallelism,
            "oversubscribed_concurrency": oversubscribed,
            "oversubscription_rule": "2_x_available_parallelism",
        },
        "summary": {
            "mandatory_cells": crate::cells::MandatoryCell::ALL.len(),
            "levels": [1, 2, 4, 8, oversubscribed],
            "matrix_rows": cells.len(),
            "expected_matrix_rows": crate::cells::MandatoryCell::ALL.len() * 5,
            "product_ready": results.iter().filter(|result| result.outcome.is_product_ready()).count(),
            "exact_concurrency_matches": results.iter().filter(|result| result.requested_concurrency == result.achieved_concurrency).count(),
            "oversubscribed_rows": results.iter().filter(|result| result.oversubscribed).count(),
            "connection_model": "one_physical_client_shared_by_all_workers_per_cell",
        },
        "cells": cells,
        "non_claims": [
            "not_gate1",
            "not_competitive",
            "not_q5_baseline",
            "single_repetition_engineering_campaign",
            "no_cross_engine_comparison"
        ],
        "authority": "doc/todo/rql/RQL_QUERY_QUALIFICATION_PROGRAM.md#72-mandatory-measured-cells",
    });
    let body =
        serde_json::to_string_pretty(&report).map_err(|error| RunError::Io(error.to_string()))?;
    let target = workspace_root.join("target/rql-q4/q4_product_scaling_report.json");
    crate::evidence::write_evidence_artifact(
        &target,
        workspace_root.join("spec/rql/qualification/harness-v1/q4_product_scaling_report.json"),
        &body,
    )?;
    Ok(target)
}

#[cfg(feature = "residiuum-embedded")]
fn sha256_json<T: serde::Serialize>(value: &T) -> Result<String, RunError> {
    let bytes = serde_json::to_vec(value).map_err(|error| RunError::Io(error.to_string()))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

#[cfg(feature = "residiuum-embedded")]
fn raw_product_repetition(
    repetition: u32,
    plan: &MeasuredCellPlan,
    outcome: &EngineRunOutcome,
    cache_state: &str,
) -> Result<RawRepetition, RunError> {
    let detail = outcome
        .detail
        .as_deref()
        .ok_or_else(|| RunError::Adapter("raw repetition missing product detail".into()))?;
    let (duration_ns, operations, _) = scaling_observation(detail)
        .ok_or_else(|| RunError::Adapter("raw repetition missing workload observation".into()))?;
    let result_digest = outcome
        .result
        .as_ref()
        .map(|result| result.values_digest.clone())
        .ok_or_else(|| RunError::Adapter("raw repetition missing result digest".into()))?;
    let qvm_hash = outcome
        .metrics
        .as_ref()
        .and_then(|metrics| metrics.path.explain_plan_digest.as_deref())
        .and_then(|value| value.strip_prefix("product_plan_hash:"))
        .map(str::to_owned);
    Ok(RawRepetition {
        repetition,
        engine: crate::lane::EngineId::ResidiuumEmbedded,
        operations,
        duration_ns,
        valid: outcome.is_product_ready(),
        result_digest,
        query_hash: hex::encode(Sha256::digest(plan.rql_source.as_bytes())),
        qvm_hash,
        index_config_hash: sha256_json(&plan.indexes)?,
        cache_state: cache_state.into(),
    })
}

/// Publish a product-only rehearsal for raw repetition and lifecycle mechanics.
/// Seven repetitions reuse one prepared deployment but reopen the physical
/// client each time, then warm the measured path on that same client. A second
/// 4× fixture proves larger-dataset plumbing without claiming memory saturation.
#[cfg(feature = "residiuum-embedded")]
pub fn publish_product_repetition_lifecycle_evidence(
    workspace_root: &Path,
    seed: u64,
) -> Result<PathBuf, RunError> {
    const REPETITIONS: u32 = 7;
    const CONCURRENCY: u32 = 2;
    const LARGER_DOCS: u64 = 256;
    let mut repeated_cells = Vec::with_capacity(crate::cells::MandatoryCell::ALL.len());
    let mut larger_cells = Vec::with_capacity(crate::cells::MandatoryCell::ALL.len());
    let mut rss_snapshots_present = 0usize;
    let mut cpu_time_samples_present = 0usize;
    let mut peak_rss_samples_present = 0usize;
    let mut physical_io_deltas_present = 0usize;
    let mut logical_byte_counters_present = 0usize;
    let mut read_amplification_present = 0usize;

    for cell in crate::cells::MandatoryCell::ALL {
        let plan = MeasuredCellPlan::smoke_for(*cell, seed).with_concurrency(CONCURRENCY, false);
        let work = SharedLogicalWork::from_dataset(generate_dataset(&plan.dataset));
        let prepared = crate::residiuum_embedded::prepare_concurrent_product(&work, &plan)
            .map_err(|error| RunError::Adapter(error.to_string()))?;
        let mut repetitions = Vec::with_capacity(REPETITIONS as usize);
        let mut last_outcome = None;
        for repetition in 0..REPETITIONS {
            let outcome =
                crate::residiuum_embedded::execute_prepared_smart_client(&work, &plan, &prepared)
                    .map_err(|error| RunError::Adapter(error.to_string()))?;
            repetitions.push(raw_product_repetition(
                repetition,
                &plan,
                &outcome,
                "same_deployment_reopen_then_same_connection_warmup",
            )?);
            last_outcome = Some(outcome);
        }
        let first = repetitions
            .first()
            .ok_or_else(|| RunError::Adapter("no raw repetitions".into()))?;
        if repetitions.iter().any(|raw| {
            !raw.valid
                || raw.result_digest != first.result_digest
                || raw.query_hash != first.query_hash
                || raw.qvm_hash != first.qvm_hash
                || raw.index_config_hash != first.index_config_hash
        }) {
            return Err(RunError::Adapter(format!(
                "raw repetition identity drift for {}",
                plan.plan_id
            )));
        }
        if last_outcome
            .as_ref()
            .and_then(|outcome| outcome.metrics.as_ref())
            .and_then(|metrics| metrics.resource.rss_bytes)
            .is_some()
        {
            rss_snapshots_present += 1;
        }
        if last_outcome
            .as_ref()
            .and_then(|outcome| outcome.metrics.as_ref())
            .and_then(|metrics| metrics.resource.cpu_time_ns)
            .is_some_and(|cpu_time_ns| cpu_time_ns > 0)
        {
            cpu_time_samples_present += 1;
        }
        if last_outcome
            .as_ref()
            .and_then(|outcome| outcome.metrics.as_ref())
            .and_then(|metrics| metrics.resource.peak_rss_bytes)
            .is_some()
        {
            peak_rss_samples_present += 1;
        }
        if last_outcome
            .as_ref()
            .and_then(|outcome| outcome.metrics.as_ref())
            .is_some_and(|metrics| {
                metrics.resource.physical_bytes_read.is_some()
                    && metrics.resource.physical_bytes_written.is_some()
            })
        {
            physical_io_deltas_present += 1;
        }
        if last_outcome
            .as_ref()
            .and_then(|outcome| outcome.metrics.as_ref())
            .is_some_and(|metrics| metrics.path.logical_bytes_examined.unwrap_or(0) > 0)
        {
            logical_byte_counters_present += 1;
        }
        if last_outcome
            .as_ref()
            .and_then(|outcome| outcome.metrics.as_ref())
            .is_some_and(|metrics| metrics.resource.read_amplification.is_some())
        {
            read_amplification_present += 1;
        }
        repeated_cells.push(json!({
            "cell_id": cell.id(),
            "plan_id": plan.plan_id,
            "shared_work_hash": work.content_hash,
            "lifecycle": "fresh_reopen_then_warm_same_connection",
            "claims_device_cold": false,
            "repetitions": repetitions,
            "last_outcome": last_outcome,
        }));

        let mut larger_plan = MeasuredCellPlan::smoke_for(*cell, seed.wrapping_add(0x4000))
            .with_concurrency(CONCURRENCY, false);
        larger_plan.dataset.doc_count = LARGER_DOCS;
        larger_plan.plan_id = format!("{}_larger_smoke_c2", cell.id());
        let larger_work = SharedLogicalWork::from_dataset(generate_dataset(&larger_plan.dataset));
        let larger_outcome = crate::residiuum_embedded::execute_plan_embedded_smart_client(
            &larger_work,
            &larger_plan,
        )
        .map_err(|error| RunError::Adapter(error.to_string()))?;
        if !larger_outcome.is_product_ready() {
            return Err(RunError::Adapter(format!(
                "larger fixture not product ready: {}",
                larger_plan.plan_id
            )));
        }
        if larger_outcome
            .metrics
            .as_ref()
            .and_then(|metrics| metrics.resource.rss_bytes)
            .is_some()
        {
            rss_snapshots_present += 1;
        }
        if larger_outcome
            .metrics
            .as_ref()
            .and_then(|metrics| metrics.resource.cpu_time_ns)
            .is_some_and(|cpu_time_ns| cpu_time_ns > 0)
        {
            cpu_time_samples_present += 1;
        }
        if larger_outcome
            .metrics
            .as_ref()
            .and_then(|metrics| metrics.resource.peak_rss_bytes)
            .is_some()
        {
            peak_rss_samples_present += 1;
        }
        if larger_outcome.metrics.as_ref().is_some_and(|metrics| {
            metrics.resource.physical_bytes_read.is_some()
                && metrics.resource.physical_bytes_written.is_some()
        }) {
            physical_io_deltas_present += 1;
        }
        if larger_outcome
            .metrics
            .as_ref()
            .is_some_and(|metrics| metrics.path.logical_bytes_examined.unwrap_or(0) > 0)
        {
            logical_byte_counters_present += 1;
        }
        if larger_outcome
            .metrics
            .as_ref()
            .is_some_and(|metrics| metrics.resource.read_amplification.is_some())
        {
            read_amplification_present += 1;
        }
        larger_cells.push(json!({
            "cell_id": cell.id(),
            "plan_id": larger_plan.plan_id,
            "document_count": LARGER_DOCS,
            "smoke_document_count": 64,
            "scale_factor": 4,
            "claims_larger_than_memory": false,
            "shared_work_hash": larger_work.content_hash,
            "outcome": larger_outcome,
        }));
    }

    let report = json!({
        "format": "residiuum-rql-q4-product-repetition-lifecycle-report-v1",
        "harness_profile": crate::HARNESS_PROFILE,
        "seed": seed,
        "summary": {
            "mandatory_cells": crate::cells::MandatoryCell::ALL.len(),
            "repetitions_per_cell": REPETITIONS,
            "raw_repetitions": crate::cells::MandatoryCell::ALL.len() * REPETITIONS as usize,
            "stable_identity_cells": repeated_cells.len(),
            "same_deployment_reopen_cells": repeated_cells.len(),
            "same_connection_warmup_cells": repeated_cells.len(),
            "larger_dataset_cells": larger_cells.len(),
            "larger_dataset_document_count": LARGER_DOCS,
            "larger_dataset_scale_factor": 4,
            "claims_device_cold": false,
            "claims_larger_than_memory": false,
            "resource_probe_rows": crate::cells::MandatoryCell::ALL.len() * 2,
            "rss_snapshots_present": rss_snapshots_present,
            "cpu_time_samples_present": cpu_time_samples_present,
            "peak_rss_samples_present": peak_rss_samples_present,
            "physical_io_deltas_present": physical_io_deltas_present,
            "logical_byte_counters_present": logical_byte_counters_present,
            "read_amplification_present": read_amplification_present,
            "rss_probe_status": if rss_snapshots_present > 0 { "in_process_interval_end" } else { "unavailable_in_campaign_environment" },
            "cpu_time_probe_status": if cpu_time_samples_present > 0 { "process_cpu_clock_interval_including_sampler" } else { "unavailable_in_campaign_environment" },
            "peak_rss_probe_status": if peak_rss_samples_present > 0 { "sampled_1ms_in_process" } else { "unavailable_in_campaign_environment" },
            "physical_io_probe_status": if physical_io_deltas_present > 0 { "in_process_interval_delta" } else { "unavailable_in_campaign_environment" },
            "read_amplification_probe_status": if read_amplification_present > 0 { "physical_over_vm_logical_bytes" } else { "unavailable_in_campaign_environment" },
        },
        "repeated_cells": repeated_cells,
        "larger_dataset_cells": larger_cells,
        "non_claims": [
            "not_gate1",
            "not_competitive",
            "not_q5_baseline",
            "no_comparator_repetitions",
            "reopen_is_not_device_cold",
            "four_x_smoke_is_not_larger_than_memory"
        ],
        "authority": "doc/todo/rql/RQL_QUERY_QUALIFICATION_PROGRAM.md#73-cache-and-lifecycle-classes",
    });
    let body =
        serde_json::to_string_pretty(&report).map_err(|error| RunError::Io(error.to_string()))?;
    let target = workspace_root.join("target/rql-q4/q4_product_repetition_lifecycle_report.json");
    crate::evidence::write_evidence_artifact(
        &target,
        workspace_root
            .join("spec/rql/qualification/harness-v1/q4_product_repetition_lifecycle_report.json"),
        &body,
    )?;
    Ok(target)
}

#[cfg(feature = "residiuum-embedded")]
fn host_memory_bytes() -> Option<u64> {
    if let Ok(value) = std::env::var("RESIDIUUM_Q4_HOST_MEMORY_BYTES") {
        if let Ok(bytes) = value.parse::<u64>() {
            return Some(bytes);
        }
    }
    use sysinfo::{MemoryRefreshKind, System};
    let mut system = System::new();
    system.refresh_memory_specifics(MemoryRefreshKind::new().with_ram());
    let total = system.total_memory();
    if total > 0 {
        return Some(total);
    }

    // The sandboxed macOS host can deny the sysctl used by sysinfo while the
    // public `memory_pressure` utility remains available. Use its fixed physical
    // capacity, never its transient free percentage: R400 must be larger than
    // the controlled host capacity, not merely larger than momentary headroom.
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("memory_pressure")
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let text = String::from_utf8_lossy(&output.stdout);
        return text
            .lines()
            .find_map(|line| line.strip_prefix("The system has "))?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok();
    }
    #[allow(unreachable_code)]
    None
}

#[cfg(feature = "residiuum-embedded")]
fn filesystem_available_bytes(path: &Path) -> Option<u64> {
    let output = std::process::Command::new("df")
        .args(["-Pk"])
        .arg(path)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let line = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .next_back()?
        .to_string();
    let available_kib = line.split_whitespace().nth(3)?.parse::<u64>().ok()?;
    Some(available_kib.saturating_mul(1024))
}

#[cfg(feature = "residiuum-embedded")]
fn metadata_only_work(spec: &DatasetSpec, content_hash: String) -> SharedLogicalWork {
    SharedLogicalWork {
        content_hash: content_hash.clone(),
        doc_count: spec.doc_count,
        seed: spec.seed,
        dataset: LogicalDataset {
            spec: spec.clone(),
            collections: BTreeMap::new(),
            content_hash,
        },
    }
}

/// Execute, or honestly refuse, the two controlled lifecycle claims which
/// cannot be represented by smoke fixtures: R400 and device-cache cold.
#[cfg(feature = "residiuum-embedded")]
pub fn publish_product_r400_cold_evidence(
    workspace_root: &Path,
    seed: u64,
) -> Result<PathBuf, RunError> {
    let host_memory_capacity = host_memory_bytes();
    let filesystem_available = filesystem_available_bytes(workspace_root);
    let filesystem_floor = std::env::var("RESIDIUUM_Q4_FILESYSTEM_FLOOR_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(2 * 1024 * 1024 * 1024);
    let campaign_cap = std::env::var("RESIDIUUM_Q4_MEMORY_CAMPAIGN_MAX_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(256 * 1024 * 1024);
    let required_r400 = host_memory_capacity.map(|bytes| bytes.saturating_mul(4));
    // Product deployments maintain dual authoritative media.  The first real
    // R400 campaign measured 135.3 GiB on disk for a 64 GiB logical target,
    // so the former 1.5x estimate was unsafe.  Admit at 2.25x to cover the
    // observed ~2.11x footprint plus framing/headroom.
    let planned_filesystem_bytes = required_r400.map(|bytes| bytes.saturating_mul(9) / 4);
    let admitted = matches!(
        (required_r400, planned_filesystem_bytes, filesystem_available),
        (Some(required), Some(planned), Some(free))
            if required <= campaign_cap
                && planned <= free.saturating_sub(filesystem_floor)
    );

    let r400 = if admitted {
        let required = required_r400.expect("admission established memory");
        let mut plan = MeasuredCellPlan::smoke_for(MandatoryCell::FirstAndDeepCursor, seed);
        let mut spec = DatasetSpec::smoke_default(seed);
        spec.memory_ratio = MemoryRatio::R400;
        spec.payload = PayloadClass::Approx64KiB;
        spec.doc_count = required
            .saturating_add(spec.payload.target_bytes() as u64 - 1)
            .saturating_div(spec.payload.target_bytes() as u64)
            .saturating_mul(102)
            .saturating_add(99)
            .saturating_div(100)
            .max(1);
        plan.plan_id = "cell_r400_streaming_full_scan_c1".into();
        plan.dataset = spec.clone();
        plan.lifecycle = LifecycleSpec::for_class(LifecycleClass::LargerThanMemory);
        plan.rql_source = "from docs order by _key asc project i".into();
        plan.page_size = Some(4096);
        plan.require_deep_cursor = true;
        plan.concurrency = 1;
        let campaign_root = workspace_root.join("target/rql-q4/r400-work");
        let (prepared, content_hash, loaded_logical_bytes) =
            crate::residiuum_embedded::prepare_streaming_product(
                &campaign_root,
                &spec,
                filesystem_floor,
            )
            .map_err(|error| RunError::Adapter(error.to_string()))?;
        if loaded_logical_bytes < required {
            return Err(RunError::Adapter(format!(
                "R400 loader underfilled: loaded={loaded_logical_bytes} required={required}"
            )));
        }
        let work = metadata_only_work(&spec, content_hash);
        let outcome = crate::residiuum_embedded::execute_prepared_smart_client_without_warmup(
            &work, &plan, &prepared,
        )
        .map_err(|error| RunError::Adapter(error.to_string()))?;
        let scanned = outcome
            .metrics
            .as_ref()
            .and_then(|metrics| metrics.path.logical_bytes_examined)
            .unwrap_or(0);
        let complete = outcome.is_product_ready() && scanned >= required;
        json!({
            "status": if complete { "executed_complete" } else { "executed_incomplete" },
            "executed": true,
            "claims_larger_than_memory": complete,
            "host_memory_capacity_bytes": host_memory_capacity,
            "required_r400_bytes": required,
            "planned_filesystem_bytes": planned_filesystem_bytes,
            "filesystem_available_bytes_at_admission": filesystem_available,
            "filesystem_floor_bytes": filesystem_floor,
            "campaign_cap_bytes": campaign_cap,
            "document_count": spec.doc_count,
            "payload_class": spec.payload.as_str(),
            "loaded_logical_bytes": loaded_logical_bytes,
            "scanned_logical_bytes": scanned,
            "streaming_loader": true,
            "outcome": outcome,
        })
    } else {
        json!({
            "status": "refused_resource_admission",
            "executed": false,
            "claims_larger_than_memory": false,
            "host_memory_capacity_bytes": host_memory_capacity,
            "required_r400_bytes": required_r400,
            "planned_filesystem_bytes": planned_filesystem_bytes,
            "filesystem_available_bytes_at_admission": filesystem_available,
            "filesystem_floor_bytes": filesystem_floor,
            "campaign_cap_bytes": campaign_cap,
            "memory_cap_sufficient": required_r400.is_some_and(|required| required <= campaign_cap),
            "filesystem_sufficient": match (planned_filesystem_bytes, filesystem_available) {
                (Some(planned), Some(free)) => planned <= free.saturating_sub(filesystem_floor),
                _ => false,
            },
            "streaming_loader": true,
        })
    };

    let cache_drop_allowed = std::env::var("RESIDIUUM_Q4_ALLOW_PAGE_CACHE_DROP")
        .ok()
        .as_deref()
        == Some("1");
    let device_cold = if cache_drop_allowed {
        let mut plan =
            MeasuredCellPlan::smoke_for(MandatoryCell::FirstAndDeepCursor, seed ^ 0xc01d);
        let mut spec = plan.dataset.clone();
        spec.payload = PayloadClass::Approx64KiB;
        spec.doc_count = 2048;
        plan.dataset = spec.clone();
        plan.plan_id = "cell_device_cold_128m_scan_c1".into();
        plan.rql_source = "from docs order by _key asc project i".into();
        plan.page_size = Some(4096);
        plan.require_deep_cursor = true;
        plan.lifecycle = LifecycleSpec {
            class: LifecycleClass::FreshReopen,
            cold_method: ColdMethod::AttemptedPageCacheDrop,
            notes: "Successful host page-cache purge immediately before unwarmed query.".into(),
            claims_device_cold: true,
        };
        let campaign_root = workspace_root.join("target/rql-q4/cold-work");
        let (prepared, content_hash, loaded_logical_bytes) =
            crate::residiuum_embedded::prepare_streaming_product(
                &campaign_root,
                &spec,
                filesystem_floor,
            )
            .map_err(|error| RunError::Adapter(error.to_string()))?;
        let purge = std::process::Command::new("/usr/sbin/purge").output();
        match purge {
            Ok(output) if output.status.success() => {
                let work = metadata_only_work(&spec, content_hash);
                let outcome =
                    crate::residiuum_embedded::execute_prepared_smart_client_without_warmup(
                        &work, &plan, &prepared,
                    )
                    .map_err(|error| RunError::Adapter(error.to_string()))?;
                json!({
                    "status": "executed_after_successful_page_cache_drop",
                    "executed": true,
                    "claims_device_cold": outcome.is_product_ready(),
                    "cold_method": "attempted_page_cache_drop",
                    "warmup_operations": 0,
                    "loaded_logical_bytes": loaded_logical_bytes,
                    "outcome": outcome,
                })
            }
            Ok(output) => json!({
                "status": "refused_page_cache_drop_failed",
                "executed": false,
                "claims_device_cold": false,
                "cold_method": "attempted_page_cache_drop",
                "exit_status": output.status.code(),
                "stderr": String::from_utf8_lossy(&output.stderr).trim(),
            }),
            Err(error) => json!({
                "status": "refused_page_cache_drop_unavailable",
                "executed": false,
                "claims_device_cold": false,
                "cold_method": "attempted_page_cache_drop",
                "error": error.to_string(),
            }),
        }
    } else {
        json!({
            "status": "refused_page_cache_drop_not_authorized",
            "executed": false,
            "claims_device_cold": false,
            "cold_method": null,
        })
    };

    let report = json!({
        "format": "residiuum-rql-q4-product-r400-cold-report-v1",
        "harness_profile": crate::HARNESS_PROFILE,
        "seed": seed,
        "r400": r400,
        "device_cold": device_cold,
        "authority": "doc/todo/rql/RQL_QUERY_QUALIFICATION_PROGRAM.md#73-cache-and-lifecycle-classes",
    });
    let body =
        serde_json::to_string_pretty(&report).map_err(|error| RunError::Io(error.to_string()))?;
    let target = workspace_root.join("target/rql-q4/q4_product_r400_cold_report.json");
    crate::evidence::write_evidence_artifact(
        &target,
        workspace_root.join("spec/rql/qualification/harness-v1/q4_product_r400_cold_report.json"),
        &body,
    )?;
    Ok(target)
}

#[cfg(feature = "residiuum-embedded")]
fn corrupt_verified_item_frames(root: &Path) -> Result<(String, u64, u64), RunError> {
    let segments = root.join("segments");
    let mut paths: Vec<_> = fs::read_dir(&segments)
        .map_err(|error| RunError::Io(error.to_string()))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("residiuum"))
        .collect();
    paths.sort();
    let path = paths
        .first()
        .ok_or_else(|| RunError::Adapter("damage setup found no sealed segment".into()))?;
    let mut bytes = fs::read(path).map_err(|error| RunError::Io(error.to_string()))?;
    let scan = residiuum_format::scan_forward(&bytes, residiuum_format::SafetyLimits::default());
    let targets: Vec<usize> = scan
        .verified_frames()
        .filter(|(_, frame)| {
            frame.header.known_kind() == Some(residiuum_format::FrameKind::ItemEvent)
        })
        .enumerate()
        .filter_map(|(index, (offset, frame))| {
            (index % 5 == 0).then(|| {
                (offset as usize)
                    .saturating_add((frame.frame_len as usize / 2).max(1))
                    .min(bytes.len().saturating_sub(1))
            })
        })
        .collect();
    if targets.is_empty() {
        return Err(RunError::Adapter(
            "damage setup found no verified ItemEvent targets".into(),
        ));
    }
    for &offset in &targets {
        bytes[offset] ^= 0x5a;
    }
    fs::write(path, &bytes).map_err(|error| RunError::Io(error.to_string()))?;
    Ok((
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("sealed.residiuum")
            .into(),
        targets.len() as u64,
        targets.len() as u64,
    ))
}

/// Product lifecycle rehearsal for explicit seal/compaction and declared
/// physical damage. Maintenance setup uses the store's public operator API
/// while all before/after query observations use the smart-client product path.
#[cfg(feature = "residiuum-embedded")]
pub fn publish_product_maintenance_damage_evidence(
    workspace_root: &Path,
    seed: u64,
) -> Result<PathBuf, RunError> {
    let mut maintenance_cells = Vec::with_capacity(crate::cells::MandatoryCell::ALL.len());
    for cell in crate::cells::MandatoryCell::ALL {
        let plan = MeasuredCellPlan::smoke_for(*cell, seed).with_concurrency(2, false);
        let work = SharedLogicalWork::from_dataset(generate_dataset(&plan.dataset));
        let prepared = crate::residiuum_embedded::prepare_concurrent_product(&work, &plan)
            .map_err(|error| RunError::Adapter(error.to_string()))?;
        let before =
            crate::residiuum_embedded::execute_prepared_smart_client(&work, &plan, &prepared)
                .map_err(|error| RunError::Adapter(error.to_string()))?;
        let before_digest = before
            .result
            .as_ref()
            .map(|result| result.values_digest.clone())
            .ok_or_else(|| RunError::Adapter("maintenance baseline missing digest".into()))?;
        let (seal, compact) = {
            let mut store = residiuum_store::Store::open(prepared.root())
                .map_err(|error| RunError::Adapter(format!("operator store open: {error}")))?;
            let seal = store
                .seal_active_with_breakdown()
                .map_err(|error| RunError::Adapter(format!("seal active: {error}")))?;
            let compact = store
                .compact_live()
                .map_err(|error| RunError::Adapter(format!("compact live: {error}")))?;
            (seal, compact)
        };
        let after =
            crate::residiuum_embedded::execute_prepared_smart_client(&work, &plan, &prepared)
                .map_err(|error| RunError::Adapter(error.to_string()))?;
        let after_digest = after
            .result
            .as_ref()
            .map(|result| result.values_digest.clone())
            .ok_or_else(|| RunError::Adapter("post-compaction query missing digest".into()))?;
        if !after.is_product_ready() || before_digest != after_digest {
            return Err(RunError::Adapter(format!(
                "maintenance changed product result for {}",
                plan.plan_id
            )));
        }
        maintenance_cells.push(json!({
            "cell_id": cell.id(),
            "plan_id": plan.plan_id,
            "operator_boundary": "public_residiuum_store_maintenance_setup",
            "before_result_digest": before_digest,
            "after_result_digest": after_digest,
            "seal": {
                "total_ns": seal.total_ns(),
                "drain_lifecycle_ns": seal.drain_lifecycle_ns,
                "final_active_seal_ns": seal.final_active_seal_ns,
                "catalog_publication_ns": seal.catalog_publication_ns,
                "hydra_ns": seal.hydra_ns,
                "chimera_ns": seal.chimera_ns,
            },
            "compaction": {
                "phase": compact.phase.as_str(),
                "live_subjects_written": compact.live_subjects_written,
                "source_segments": compact.source_segments,
                "sources_retained": compact.sources_retained,
                "coverage": compact.coverage,
                "bytes_estimated_read": compact.bytes_estimated_read,
                "bytes_estimated_write": compact.bytes_estimated_write,
                "bytes_read": compact.bytes_read,
                "bytes_written": compact.bytes_written,
                "bytes_retained": compact.bytes_retained,
                "bytes_reclaimed": compact.bytes_reclaimed,
            },
            "after_outcome": after,
        }));
    }

    // Damage uses a broad incomplete-allowed query so verified survivors may
    // return while the coverage flag remains authoritative.
    let mut damage_plan =
        MeasuredCellPlan::smoke_for(crate::cells::MandatoryCell::KeyGet, seed ^ 0xdada)
            .with_concurrency(2, false);
    damage_plan.plan_id = "cell_declared_damage_survivors_c2".into();
    damage_plan.rql_source = "from docs coverage allow_incomplete".into();
    damage_plan.lifecycle =
        crate::lifecycle::LifecycleSpec::for_class(crate::metrics::LifecycleClass::DeclaredDamage);
    let damage_work = SharedLogicalWork::from_dataset(generate_dataset(&damage_plan.dataset));
    let damage_prepared =
        crate::residiuum_embedded::prepare_concurrent_product(&damage_work, &damage_plan)
            .map_err(|error| RunError::Adapter(error.to_string()))?;
    {
        let mut store = residiuum_store::Store::open(damage_prepared.root())
            .map_err(|error| RunError::Adapter(format!("damage store open: {error}")))?;
        store
            .seal_active()
            .map_err(|error| RunError::Adapter(format!("damage seal: {error}")))?;
    }
    let damage_baseline = crate::residiuum_embedded::execute_prepared_smart_client(
        &damage_work,
        &damage_plan,
        &damage_prepared,
    )
    .map_err(|error| RunError::Adapter(format!("damage baseline: {error}")))?;
    let baseline_result = damage_baseline
        .result
        .as_ref()
        .ok_or_else(|| RunError::Adapter("damage baseline missing result".into()))?;
    let baseline_digest = baseline_result.values_digest.clone();
    let baseline_rows = baseline_result.row_count;
    let (damaged_file, damaged_frames, damaged_bytes) =
        corrupt_verified_item_frames(damage_prepared.root())?;
    let damage_execution = match crate::residiuum_embedded::execute_prepared_smart_client(
        &damage_work,
        &damage_plan,
        &damage_prepared,
    ) {
        Ok(outcome) => {
            let coverage = outcome
                .metrics
                .as_ref()
                .and_then(|metrics| metrics.coverage_complete);
            let result = outcome.result.as_ref();
            let exact_recovery = result
                .map(|result| {
                    result.row_count == baseline_rows && result.values_digest == baseline_digest
                })
                .unwrap_or(false);
            if outcome.status == AdapterStatus::Ready && coverage == Some(true) && !exact_recovery {
                return Err(RunError::Adapter(format!(
                    "declared damage returned false complete coverage baseline_rows={baseline_rows} observed_rows={}",
                    result.map(|result| result.row_count).unwrap_or(0)
                )));
            }
            let classification = if outcome.status == AdapterStatus::Ready
                && coverage == Some(true)
                && exact_recovery
            {
                "recovered_exact_complete"
            } else if outcome.status == AdapterStatus::Ready
                && coverage == Some(false)
                && result
                    .is_some_and(|result| result.row_count > 0 && result.row_count < baseline_rows)
            {
                "partial_survivors_incomplete_coverage"
            } else {
                "refused_outcome"
            };
            json!({"classification": classification, "exact_recovery": exact_recovery, "outcome": outcome})
        }
        Err(error) => json!({
            "classification": "fail_closed_error",
            "error": error.to_string(),
        }),
    };
    let mut strict_damage_plan = damage_plan.clone();
    strict_damage_plan.plan_id = "cell_declared_damage_strict_c2".into();
    strict_damage_plan.rql_source = "from docs".into();
    let strict_damage_execution = match crate::residiuum_embedded::execute_prepared_smart_client(
        &damage_work,
        &strict_damage_plan,
        &damage_prepared,
    ) {
        Ok(outcome) if outcome.status != AdapterStatus::Ready => json!({
            "classification": "fail_closed_outcome",
            "outcome": outcome,
        }),
        Err(error) => json!({
            "classification": "fail_closed_error",
            "error": error.to_string(),
        }),
        Ok(outcome) => {
            return Err(RunError::Adapter(format!(
                "strict coverage returned Ready on declared damage: {:?}",
                outcome.result
            )))
        }
    };

    let host_memory = host_memory_bytes();
    let filesystem_available = filesystem_available_bytes(workspace_root);
    let filesystem_floor = std::env::var("RESIDIUUM_Q4_FILESYSTEM_FLOOR_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(2 * 1024 * 1024 * 1024);
    let safety_cap = std::env::var("RESIDIUUM_Q4_MEMORY_CAMPAIGN_MAX_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(256 * 1024 * 1024);
    let required_bytes = host_memory.map(|bytes| bytes.saturating_mul(4));
    let memory_admission = match (host_memory, required_bytes, filesystem_available) {
        (None, _, _) => json!({
            "status": "refused_host_memory_unavailable",
            "host_memory_bytes": null,
            "required_r400_bytes": null,
            "safety_cap_bytes": safety_cap,
            "executed": false,
            "claims_larger_than_memory": false,
        }),
        (Some(host), Some(required), Some(free))
            if required > safety_cap || required > free.saturating_sub(filesystem_floor) =>
        {
            json!({
                "status": "refused_resource_admission",
                "host_memory_bytes": host,
                "required_r400_bytes": required,
                "safety_cap_bytes": safety_cap,
                "filesystem_available_bytes": free,
                "filesystem_floor_bytes": filesystem_floor,
                "safety_cap_sufficient": required <= safety_cap,
                "filesystem_sufficient": required <= free.saturating_sub(filesystem_floor),
                "executed": false,
                "claims_larger_than_memory": false,
            })
        }
        (Some(host), Some(required), Some(free)) => json!({
            "status": "admitted_external_campaign_required",
            "host_memory_bytes": host,
            "required_r400_bytes": required,
            "safety_cap_bytes": safety_cap,
            "filesystem_available_bytes": free,
            "filesystem_floor_bytes": filesystem_floor,
            "safety_cap_sufficient": true,
            "filesystem_sufficient": true,
            "executed": false,
            "claims_larger_than_memory": false,
        }),
        (Some(host), Some(required), None) => json!({
            "status": "refused_filesystem_probe_unavailable",
            "host_memory_bytes": host,
            "required_r400_bytes": required,
            "safety_cap_bytes": safety_cap,
            "filesystem_available_bytes": null,
            "filesystem_floor_bytes": filesystem_floor,
            "executed": false,
            "claims_larger_than_memory": false,
        }),
        _ => unreachable!(),
    };

    let report = json!({
        "format": "residiuum-rql-q4-product-maintenance-damage-report-v1",
        "harness_profile": crate::HARNESS_PROFILE,
        "seed": seed,
        "summary": {
            "maintenance_cells": maintenance_cells.len(),
            "maintenance_digest_stable": maintenance_cells.len(),
            "declared_damage_cells": 1,
            "false_complete_damage_outcomes": 0,
            "memory_saturation_executed": false,
        },
        "maintenance_cells": maintenance_cells,
        "declared_damage": {
            "damaged_file": damaged_file,
            "damaged_item_frames": damaged_frames,
            "damaged_bytes": damaged_bytes,
            "baseline_rows": baseline_rows,
            "baseline_result_digest": baseline_digest,
            "injection": "xor_one_middle_byte_in_every_fifth_verified_item_event",
            "execution": damage_execution,
            "strict_coverage_execution": strict_damage_execution,
        },
        "memory_admission": memory_admission,
        "non_claims": [
            "not_gate1",
            "not_competitive",
            "maintenance_setup_uses_store_operator_boundary",
            "no_device_cold_claim",
            "memory_saturation_not_executed"
        ],
        "authority": "doc/todo/rql/RQL_QUERY_QUALIFICATION_PROGRAM.md#73-cache-and-lifecycle-classes",
    });
    let body =
        serde_json::to_string_pretty(&report).map_err(|error| RunError::Io(error.to_string()))?;
    let target = workspace_root.join("target/rql-q4/q4_product_maintenance_damage_report.json");
    crate::evidence::write_evidence_artifact(
        &target,
        workspace_root
            .join("spec/rql/qualification/harness-v1/q4_product_maintenance_damage_report.json"),
        &body,
    )?;
    Ok(target)
}

/// Publish a non-competitive product-concurrency report. This is separate from
/// the logical-vs-comparator scaffold bundle so product execution cannot be
/// mistaken for a configured comparator campaign.
#[cfg(feature = "residiuum-embedded")]
pub fn publish_product_concurrency_evidence(
    workspace_root: &Path,
    seed: u64,
    concurrency: u32,
) -> Result<PathBuf, RunError> {
    let results = run_product_concurrency_smoke(seed, concurrency)?;
    let cells: Vec<Value> = results
        .iter()
        .map(|result| {
            json!({
                "cell_id": result.cell_id,
                "plan_id": result.plan_id,
                "requested_concurrency": result.requested_concurrency,
                "achieved_concurrency": result.achieved_concurrency,
                "workload_wall_ns": result.workload_wall_ns,
                "workload_operations": result.workload_operations,
                "aggregate_ops_per_s": result.aggregate_ops_per_s,
                "one_physical_connection": true,
                "product_ready": result.outcome.is_product_ready(),
                "outcome": result.outcome,
            })
        })
        .collect();
    let report = json!({
        "format": "residiuum-rql-q4-product-concurrency-report-v1",
        "harness_profile": crate::HARNESS_PROFILE,
        "seed": seed,
        "summary": {
            "mandatory_cells": cells.len(),
            "product_ready": results.iter().filter(|result| result.outcome.is_product_ready()).count(),
            "requested_concurrency": concurrency,
            "exact_concurrency_matches": results.iter().filter(|result| result.achieved_concurrency == concurrency).count(),
            "connection_model": "one_physical_client_shared_by_all_workers_per_cell",
        },
        "cells": cells,
        "non_claims": [
            "not_gate1",
            "not_competitive",
            "not_comparator_campaign",
            "two_worker_smoke_does_not_replace_full_scaling_matrix"
        ],
        "authority": "doc/todo/rql/RQL_Q4_3_METRICS_ADAPTERS.md",
    });
    let body = serde_json::to_string_pretty(&report).map_err(|e| RunError::Io(e.to_string()))?;
    let target = workspace_root.join("target/rql-q4/q4_product_concurrency_report.json");
    crate::evidence::write_evidence_artifact(
        &target,
        workspace_root.join("spec/rql/qualification/harness-v1/q4_product_concurrency_report.json"),
        &body,
    )?;
    Ok(target)
}

/// Run smoke portfolio: logical harness (A) + CBL adapter (B) with shared work.
pub fn run_smoke_lane_embedded(seed: u64) -> Result<Vec<SmokeCellResult>, RunError> {
    let plans = smoke_portfolio(seed);
    let mut out = Vec::with_capacity(plans.len());
    for plan in plans {
        out.push(run_one_embedded_pair(&plan)?);
    }
    Ok(out)
}

/// F2: expanded §7.2 portfolio (enrich/rw/concurrency variants) executed.
pub fn run_section_7_2_expanded(seed: u64) -> Result<Vec<SmokeCellResult>, RunError> {
    let plans = section_7_2_expanded_portfolio(seed);
    let mut out = Vec::with_capacity(plans.len());
    for plan in plans {
        out.push(run_one_embedded_pair(&plan)?);
    }
    Ok(out)
}

fn run_one_embedded_pair(plan: &MeasuredCellPlan) -> Result<SmokeCellResult, RunError> {
    let ds = generate_dataset(&plan.dataset);
    let work = SharedLogicalWork::from_dataset(ds);
    let hash = work.content_hash.clone();

    // F10: real concurrent workers when plan.concurrency > 1 (not metadata-only).
    let concurrent = run_logical_concurrent(&work, plan)?;
    let side_a = concurrent.primary;

    let mut cbl = CblEmbeddedAdapter::default();
    cbl.load_shared_work(&work)
        .map_err(|e| RunError::Adapter(e.to_string()))?;
    let side_b = cbl
        .execute_plan(plan)
        .map_err(|e| RunError::Adapter(e.to_string()))?;

    // Cross-engine fixture identity: both sides must report same shared hash.
    if side_a.shared_work_hash.as_deref() != Some(hash.as_str()) {
        return Err(RunError::Adapter(format!(
            "side_a shared hash mismatch plan={}",
            plan.plan_id
        )));
    }
    if side_b.shared_work_hash.as_deref() != Some(hash.as_str()) {
        return Err(RunError::Adapter(format!(
            "side_b shared hash mismatch plan={}",
            plan.plan_id
        )));
    }

    Ok(SmokeCellResult {
        plan_id: plan.plan_id.clone(),
        side_a,
        side_b,
        shared_hash: hash,
        requested_concurrency: concurrent.requested_concurrency,
        achieved_concurrency: concurrent.achieved_concurrency,
    })
}

/// Lane S smoke: logical (as Residiuum stand-in is not server) + Mongo stub.
/// Uses Mongo + ResidiuumServer adapters both NotConfigured after shared load —
/// proves fixture identity, not comparative digests.
pub fn run_smoke_lane_server_fixture_identity(seed: u64) -> Result<(String, bool), RunError> {
    let plan = MeasuredCellPlan::smoke_for(crate::cells::MandatoryCell::KeyGet, seed);
    let ds = generate_dataset(&plan.dataset);
    let work = SharedLogicalWork::from_dataset(ds);
    let mut mongo = MongoLocalAdapter::default();
    mongo
        .load_shared_work(&work)
        .map_err(|e| RunError::Adapter(e.to_string()))?;
    let mut server = crate::engine::ResidiuumServerAdapter::default();
    server
        .load_shared_work(&work)
        .map_err(|e| RunError::Adapter(e.to_string()))?;
    let a = server
        .execute_plan(&plan)
        .map_err(|e| RunError::Adapter(e.to_string()))?;
    let b = mongo
        .execute_plan(&plan)
        .map_err(|e| RunError::Adapter(e.to_string()))?;
    let ok = a.shared_work_hash == b.shared_work_hash
        && a.shared_work_hash.as_deref() == Some(work.content_hash.as_str());
    Ok((work.content_hash, ok))
}

/// Publish evidence bundle for smoke portfolio under workspace paths.
pub fn publish_smoke_evidence(
    workspace_root: &Path,
    seed: u64,
    campaign_id: &str,
) -> Result<PathBuf, RunError> {
    let env = EnvFingerprint::capture(workspace_root)?;
    // Scaffold campaigns may run on dirty trees; competitive Q5 uses assert_clean.
    let mut bundle = EvidenceBundle::new(env, campaign_id);
    bundle.protocol = crate::evidence::CampaignProtocol::scaffold(seed);
    bundle
        .notes
        .push("Q4.3 smoke — not competitive / not Gate-1".into());
    bundle
        .notes
        .push("side_a=logical_harness digests; side_b=CBL NotConfigured with shared_work".into());

    let results = run_smoke_lane_embedded(seed)?;
    let mut ready_ok = 0u64;
    let mut configured_b = 0u64;
    for r in results {
        if r.side_a.status == AdapterStatus::Ready && r.side_a.result.is_some() {
            ready_ok += 1;
        }
        if r.side_b.shared_work_hash.is_some() {
            configured_b += 1;
        }
        let mut cell = scaffold_cell_with_concurrency(
            &r.plan_id,
            LanePairing::SCAFFOLD_LOGICAL_VS_CBL,
            r.side_a.clone(),
            r.side_b.clone(),
            r.requested_concurrency,
            r.achieved_concurrency,
        );
        cell.case_id = Some(r.plan_id.clone());
        cell.metrics_a = r.side_a.metrics.clone();
        cell.metrics_b = r.side_b.metrics.clone();
        // Equivalence: only when both Ready with results (CBL not ready → None).
        let (eq, detail) = compare_ready_outcomes(&r.side_a, &r.side_b);
        cell.equivalent = eq;
        cell.equivalence_detail = detail.or_else(|| {
            Some(format!(
                "shared_work_hash={} side_b={}",
                r.shared_hash,
                r.side_b.refuse_code.clone().unwrap_or_else(|| "ok".into())
            ))
        });
        bundle.push_cell(cell);
    }

    let (lane_s_hash, lane_s_ok) = run_smoke_lane_server_fixture_identity(seed)?;
    bundle.notes.push(format!(
        "lane_s_fixture_identity ok={lane_s_ok} hash={lane_s_hash}"
    ));

    // F8: default → target/rql-q4/; RESIDIUUM_WRITE_SPEC_EVIDENCE=1 also writes spec/.
    let target_bundle = workspace_root.join("target/rql-q4/q4_3_smoke_evidence_bundle.json");
    let spec_bundle =
        workspace_root.join("spec/rql/qualification/harness-v1/q4_3_smoke_evidence_bundle.json");
    bundle.write_json(&target_bundle)?;
    if crate::evidence::write_spec_evidence_enabled() {
        bundle.write_json(&spec_bundle)?;
    }
    let out = target_bundle;

    // Architecture/labor report
    let report = json!({
        "format": "residiuum-rql-q4-3-metrics-adapters-report-v1",
        "harness_profile": crate::HARNESS_PROFILE,
        "summary": {
            "smoke_cells": bundle.cells.len(),
            "logical_ready_with_result": ready_ok,
            "cbl_shared_work_loaded": configured_b,
            "lane_s_fixture_identity": lane_s_ok,
            "content_hash": bundle.content_hash,
            "metric_envelope": "§7.4 collectors + path digests",
            "mongo_cbl_execute": "not_configured_honest",
        },
        "bundle_path": out.display().to_string(),
        "non_claims": [
            "not_gate1",
            "not_competitive",
            "not_q4_package_accept",
            "mongo_cbl_driver_residual",
            "residiuum_server_residual"
        ],
        "authority": "doc/todo/rql/RQL_Q4_3_METRICS_ADAPTERS.md",
    });
    let body = serde_json::to_string_pretty(&report).map_err(|e| RunError::Io(e.to_string()))?;
    crate::evidence::write_evidence_artifact(
        workspace_root.join("target/rql-q4/q4_3_metrics_adapters_report.json"),
        workspace_root.join("spec/rql/qualification/harness-v1/q4_3_metrics_adapters_report.json"),
        &body,
    )?;

    let _ = ready_ok;
    Ok(out)
}

/// Machine JSON for verify (also used as lightweight call without full publish).
pub fn q4_3_report_value(workspace_root: &Path) -> Result<Value, RunError> {
    let _ = publish_smoke_evidence(workspace_root, 0x04_43, "q4-3-smoke")?;
    let target = workspace_root.join("target/rql-q4/q4_3_metrics_adapters_report.json");
    let spec =
        workspace_root.join("spec/rql/qualification/harness-v1/q4_3_metrics_adapters_report.json");
    let p = if target.is_file() { target } else { spec };
    let raw = fs::read_to_string(&p).map_err(|e| RunError::Io(e.to_string()))?;
    serde_json::from_str(&raw).map_err(|e| RunError::Io(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workspace_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .unwrap()
    }

    #[test]
    fn smoke_portfolio_logical_ready() {
        let results = run_smoke_lane_embedded(3).expect("smoke");
        assert_eq!(results.len(), 12);
        for r in &results {
            assert_eq!(r.side_a.status, AdapterStatus::Ready);
            assert!(r.side_a.result.is_some(), "plan {}", r.plan_id);
            assert!(r.side_a.metrics.is_some());
            assert_eq!(r.side_b.status, AdapterStatus::NotConfigured);
            assert_eq!(
                r.side_b.shared_work_hash.as_deref(),
                Some(r.shared_hash.as_str())
            );
        }
    }

    #[test]
    fn section_7_2_expanded_executes_variants() {
        let results = run_section_7_2_expanded(7).expect("expanded");
        // F11: full inventory (smoke + variants + all-cell concurrency).
        assert!(
            results.len() >= 80,
            "expected ≥80 plans after F11, got {}",
            results.len()
        );
        let mut saw_deep = false;
        let mut saw_writes = false;
        let mut saw_nested_only = false;
        let mut saw_array_only = false;
        let mut saw_covered = false;
        let mut saw_non_covered = false;
        let mut saw_group_high = false;
        for r in &results {
            assert!(r.side_a.result.is_some(), "{}", r.plan_id);
            // F10: achieved concurrency must match requested (real workers, not metadata).
            assert_eq!(
                r.achieved_concurrency, r.requested_concurrency,
                "plan {} requested={} achieved={}",
                r.plan_id, r.requested_concurrency, r.achieved_concurrency
            );
            if r.requested_concurrency > 1 {
                assert!(
                    r.achieved_concurrency >= 2,
                    "concurrency>1 must not run serial-only: {}",
                    r.plan_id
                );
            }
            let d = r.side_a.detail.as_deref().unwrap_or("");
            if d.contains("cursor pages=") && d.contains("deep_start=") {
                saw_deep = true;
            }
            if d.contains("writes=") {
                if d.contains("writes=0") {
                    panic!("mixed R/W produced zero writes: {d}");
                }
                saw_writes = true;
            }
            if d.contains("nested_array_focus=nested_only") {
                saw_nested_only = true;
            }
            if d.contains("nested_array_focus=array_only") {
                saw_array_only = true;
            }
            if d.contains("projection_cover=covered") {
                saw_covered = true;
            }
            if d.contains("projection_cover=non_covered") {
                saw_non_covered = true;
            }
            if d.contains("group_card=card_high") {
                saw_group_high = true;
            }
        }
        assert!(saw_deep, "expected deep cursor detail");
        assert!(saw_writes, "expected mixed R/W writes");
        assert!(saw_nested_only, "expected nested-only predicate execution");
        assert!(saw_array_only, "expected array-only predicate execution");
        assert!(saw_covered, "expected covered projection execution");
        assert!(saw_non_covered, "expected non-covered projection execution");
        assert!(saw_group_high, "expected high-cardinality group execution");
        assert!(
            results.iter().any(|r| r.plan_id.contains("agg_stats")),
            "expected agg plan"
        );
        assert!(
            results.iter().any(|r| r.plan_id.contains("many")),
            "expected enrich many variant"
        );
        assert!(
            results.iter().any(|r| r.plan_id.contains("_c8")),
            "expected concurrency 8 plan executed"
        );
        // F10/F11: multi-worker plans across cells, not key-get only.
        let multi: Vec<_> = results
            .iter()
            .filter(|r| r.requested_concurrency > 1)
            .collect();
        assert!(
            multi.len() >= 40,
            "expected broad concurrency matrix (>1), got {}",
            multi.len()
        );
        for r in &multi {
            assert_eq!(
                r.achieved_concurrency, r.requested_concurrency,
                "F10 real concurrency failed for {}",
                r.plan_id
            );
            let d = r.side_a.detail.as_deref().unwrap_or("");
            assert!(
                d.contains("concurrency_achieved="),
                "missing achieved stamp on {}",
                r.plan_id
            );
        }
        assert!(
            results.iter().any(|r| r.plan_id.contains("high_band")
                || r.side_a
                    .detail
                    .as_deref()
                    .unwrap_or("")
                    .contains("conditional high_band")),
            "expected conditional computed"
        );
    }

    #[test]
    fn f11_variant_plans_execute_and_differ() {
        use crate::cell_plan::{
            group_cardinality_variants, nested_array_predicate_variants, projection_cover_variants,
        };
        use crate::engine::{EngineAdapter, LogicalHarnessEngine};

        // Nested vs array: both Ready, different shapes/focus in detail.
        for plan in nested_array_predicate_variants(9) {
            let r = run_one_embedded_pair(&plan).expect("nested/array pair");
            assert!(r.side_a.result.is_some(), "{}", plan.plan_id);
            let d = r.side_a.detail.as_deref().unwrap_or("");
            if plan.plan_id.contains("nested_only") {
                assert!(d.contains("nested_only"), "{d}");
            } else {
                assert!(d.contains("array_only"), "{d}");
            }
        }

        // Covered vs non-covered: cover label + non-empty filter results.
        for plan in projection_cover_variants(9) {
            let r = run_one_embedded_pair(&plan).expect("project pair");
            let d = r.side_a.detail.as_deref().unwrap_or("");
            assert!(
                d.contains(&format!(
                    "projection_cover={}",
                    plan.project_cover.unwrap().as_str()
                )),
                "plan={} detail={d}",
                plan.plan_id
            );
            assert!(
                r.side_a.result.as_ref().unwrap().row_count > 0,
                "status filter should hit st-0000"
            );
        }

        // Low vs high group: high must have more distinct groups than low.
        let groups = group_cardinality_variants(9);
        let mut eng = LogicalHarnessEngine::new();
        let mut counts = Vec::new();
        for plan in &groups {
            let ds = generate_dataset(&plan.dataset);
            let work = SharedLogicalWork::from_dataset(ds);
            eng.load_shared_work(&work).unwrap();
            let out = eng.execute_plan(plan).unwrap();
            counts.push((
                plan.dataset.cardinality,
                out.result.as_ref().unwrap().row_count,
            ));
        }
        let low = counts
            .iter()
            .find(|(c, _)| *c == crate::dataset::CardinalityClass::Low)
            .unwrap()
            .1;
        let high = counts
            .iter()
            .find(|(c, _)| *c == crate::dataset::CardinalityClass::High)
            .unwrap()
            .1;
        assert!(
            high > low,
            "high card groups ({high}) must exceed low ({low})"
        );
    }

    #[test]
    fn real_concurrency_not_metadata_only() {
        use crate::cell_plan::concurrency_matrix;
        use crate::cells::MandatoryCell;
        use crate::evidence::EvidenceBundle;

        let base = MeasuredCellPlan::smoke_for(MandatoryCell::KeyGet, 11);
        let plans = concurrency_matrix(&base, 2);
        let mut saw_c8 = false;
        for plan in plans {
            let r = run_one_embedded_pair(&plan).expect("pair");
            assert_eq!(r.requested_concurrency, plan.concurrency);
            assert_eq!(
                r.achieved_concurrency, plan.concurrency,
                "achieved must equal plan concurrency for {}",
                plan.plan_id
            );
            if plan.concurrency == 8 {
                saw_c8 = true;
            }
            // Evidence row must carry both fields (not hard-coded 1).
            let cell = scaffold_cell_with_concurrency(
                &r.plan_id,
                LanePairing::SCAFFOLD_LOGICAL_VS_CBL,
                r.side_a.clone(),
                r.side_b.clone(),
                r.requested_concurrency,
                r.achieved_concurrency,
            );
            assert_eq!(cell.concurrency, plan.concurrency);
            assert_eq!(cell.achieved_concurrency, plan.concurrency);
            if plan.concurrency > 1 {
                assert_ne!(
                    cell.concurrency, 1,
                    "evidence must not collapse multi-worker plan to concurrency=1"
                );
            }
        }
        assert!(saw_c8);

        // Bundle round-trip preserves achieved_concurrency.
        let root = workspace_root();
        let env = EnvFingerprint::capture(&root).unwrap();
        let mut bundle = EvidenceBundle::new(env, "f10-concurrency");
        let plan = MeasuredCellPlan::smoke_for(MandatoryCell::KeyGet, 3).with_concurrency(4, false);
        let r = run_one_embedded_pair(&plan).unwrap();
        bundle.push_cell(scaffold_cell_with_concurrency(
            &r.plan_id,
            LanePairing::SCAFFOLD_LOGICAL_VS_CBL,
            r.side_a,
            r.side_b,
            r.requested_concurrency,
            r.achieved_concurrency,
        ));
        let path = root.join("target/rql-q4/f10_concurrency_bundle.json");
        bundle.write_json(&path).unwrap();
        let raw = fs::read_to_string(&path).unwrap();
        let v: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["cells"][0]["concurrency"], 4);
        assert_eq!(v["cells"][0]["achieved_concurrency"], 4);
    }

    #[test]
    fn publish_evidence_bundle() {
        let root = workspace_root();
        let path = publish_smoke_evidence(&root, 0x04_43, "q4-3-unit").expect("publish");
        assert!(path.is_file());
        // F8: default publishes under target/rql-q4/
        assert!(
            path.starts_with(root.join("target/rql-q4")),
            "default publish must write under target/: {}",
            path.display()
        );
        let report = root.join("target/rql-q4/q4_3_metrics_adapters_report.json");
        assert!(report.is_file());
        let v: Value = serde_json::from_str(&fs::read_to_string(report).unwrap()).unwrap();
        assert_eq!(v["format"], "residiuum-rql-q4-3-metrics-adapters-report-v1");
        assert_eq!(v["summary"]["smoke_cells"], 12);
        assert_eq!(v["summary"]["logical_ready_with_result"], 12);
        assert_eq!(v["summary"]["lane_s_fixture_identity"], true);
    }

    #[test]
    fn lane_s_fixture_identity() {
        let (hash, ok) = run_smoke_lane_server_fixture_identity(1).unwrap();
        assert!(ok);
        assert_eq!(hash.len(), 64);
    }

    #[cfg(feature = "residiuum-embedded")]
    #[test]
    fn product_concurrency_all_mandatory_cells_and_report() {
        let root = workspace_root();
        let path = publish_product_concurrency_evidence(&root, 0x04_44, 2)
            .expect("product concurrency evidence");
        let report: Value =
            serde_json::from_str(&fs::read_to_string(path).expect("read report")).unwrap();
        assert_eq!(
            report["format"],
            "residiuum-rql-q4-product-concurrency-report-v1"
        );
        assert_eq!(report["summary"]["mandatory_cells"], 12);
        assert_eq!(report["summary"]["product_ready"], 12);
        assert_eq!(report["summary"]["requested_concurrency"], 2);
        assert_eq!(report["summary"]["exact_concurrency_matches"], 12);
        for cell in report["cells"].as_array().unwrap() {
            assert_eq!(cell["requested_concurrency"], 2);
            assert_eq!(cell["achieved_concurrency"], 2);
            assert_eq!(cell["one_physical_connection"], true);
            assert_eq!(cell["product_ready"], true);
            assert!(cell["outcome"]["result"].is_object());
            assert!(cell["outcome"]["metrics"].is_object());
        }
    }

    #[cfg(feature = "residiuum-embedded")]
    #[test]
    fn product_scaling_matrix_shape_is_complete() {
        let host = std::thread::available_parallelism()
            .map(|value| value.get() as u32)
            .unwrap_or(1);
        let oversubscribed = product_oversubscribed_concurrency();
        assert!(oversubscribed > host);
        assert_eq!([1, 2, 4, 8, oversubscribed].len(), 5);
        assert_eq!(crate::cells::MandatoryCell::ALL.len() * 5, 60);
    }

    #[cfg(feature = "residiuum-embedded")]
    #[test]
    fn host_resource_admission_probe_is_available() {
        let root = workspace_root();
        let memory = host_memory_bytes().expect("safe available-memory probe");
        let disk = filesystem_available_bytes(&root).expect("filesystem free-space probe");
        println!("host_memory_capacity_bytes={memory} filesystem_available_bytes={disk}");
        assert!(memory > 0);
        assert!(disk > 0);
    }

    /// Explicit engineering campaign: deliberately excluded from ordinary unit
    /// runs because it creates and measures sixty product deployments.
    #[cfg(feature = "residiuum-embedded")]
    #[test]
    #[ignore = "explicit 60-cell product scaling campaign"]
    fn publish_product_scaling_campaign() {
        let root = workspace_root();
        let path = publish_product_scaling_evidence(&root, 0x04_45)
            .expect("publish product scaling evidence");
        let report: Value =
            serde_json::from_str(&fs::read_to_string(path).expect("read scaling report")).unwrap();
        assert_eq!(report["summary"]["matrix_rows"], 60);
        assert_eq!(report["summary"]["product_ready"], 60);
        assert_eq!(report["summary"]["exact_concurrency_matches"], 60);
        assert_eq!(report["summary"]["oversubscribed_rows"], 12);
    }

    #[cfg(feature = "residiuum-embedded")]
    #[test]
    #[ignore = "explicit 84-repetition plus 12 larger-fixture product campaign"]
    fn publish_product_repetition_lifecycle_campaign() {
        let root = workspace_root();
        let path = publish_product_repetition_lifecycle_evidence(&root, 0x04_46)
            .expect("publish product repetition/lifecycle evidence");
        let report: Value =
            serde_json::from_str(&fs::read_to_string(path).expect("read lifecycle report"))
                .unwrap();
        assert_eq!(report["summary"]["raw_repetitions"], 84);
        assert_eq!(report["summary"]["stable_identity_cells"], 12);
        assert_eq!(report["summary"]["same_deployment_reopen_cells"], 12);
        assert_eq!(report["summary"]["larger_dataset_cells"], 12);
        assert_eq!(report["summary"]["claims_device_cold"], false);
        assert_eq!(report["summary"]["claims_larger_than_memory"], false);
    }

    #[cfg(feature = "residiuum-embedded")]
    #[test]
    #[ignore = "explicit maintenance and physical-damage product campaign"]
    fn publish_product_maintenance_damage_campaign() {
        let root = workspace_root();
        let path = publish_product_maintenance_damage_evidence(&root, 0x04_47)
            .expect("publish maintenance/damage evidence");
        let report: Value =
            serde_json::from_str(&fs::read_to_string(path).expect("read maintenance report"))
                .unwrap();
        assert_eq!(report["summary"]["maintenance_cells"], 12);
        assert_eq!(report["summary"]["maintenance_digest_stable"], 12);
        assert_eq!(report["summary"]["declared_damage_cells"], 1);
        assert_eq!(report["summary"]["false_complete_damage_outcomes"], 0);
        assert_eq!(report["summary"]["memory_saturation_executed"], false);
        assert_eq!(
            report["memory_admission"]["claims_larger_than_memory"],
            false
        );
    }

    #[cfg(feature = "residiuum-embedded")]
    #[test]
    #[ignore = "explicit resource-admitted R400 and device-cold campaign"]
    fn publish_product_r400_cold_campaign() {
        let root = workspace_root();
        let path =
            publish_product_r400_cold_evidence(&root, 0x04_48).expect("publish R400/cold evidence");
        let report: Value =
            serde_json::from_str(&fs::read_to_string(path).expect("read R400/cold report"))
                .unwrap();
        assert!(report["r400"]["executed"].is_boolean());
        assert!(report["device_cold"]["executed"].is_boolean());
    }
}
