//! Real multi-worker concurrency for measured cell plans (F10 / programme §7.2).
//!
//! Plans carry `concurrency` metadata; this module **executes** them with
//! simultaneous OS threads (barrier rendezvous), not serial loops that only
//! stamp `concurrency` onto evidence.
//!
//! Honesty: when `requested > 1`, `achieved` must be ≥ 2 (and equals requested
//! when all workers reach the overlap barrier). Serial-only execution of a
//! multi-worker plan is a hard error.

use crate::cell_plan::MeasuredCellPlan;
use crate::engine::{
    AdapterError, AdapterStatus, EngineAdapter, EngineRunOutcome, LogicalHarnessEngine,
};
use crate::metrics::{assemble_metrics, LatencyCollector, QueryPathMetrics};
use crate::shared_work::SharedLogicalWork;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

/// Outcome of one concurrent plan execution.
#[derive(Debug, Clone)]
pub struct ConcurrentPlanResult {
    /// Plan-declared concurrency (requested).
    pub requested_concurrency: u32,
    /// Peak simultaneous workers observed at the overlap barrier.
    pub achieved_concurrency: u32,
    /// Threads that were spawned and joined with a result (ok or err).
    pub worker_count: u32,
    /// Representative outcome (worker 0), metrics merged across workers when Ready.
    pub primary: EngineRunOutcome,
    /// Per-worker values digests (when Ready).
    pub worker_digests: Vec<Option<String>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConcurrentError {
    #[error("adapter: {0}")]
    Adapter(#[from] AdapterError),
    #[error("concurrency honesty: {0}")]
    Honesty(String),
    #[error("worker join: {0}")]
    Join(String),
    #[error("worker panic or disconnect")]
    WorkerPanic,
}

/// Fail closed when a multi-worker plan did not actually run concurrent workers.
pub fn assert_concurrency_honest(requested: u32, achieved: u32) -> Result<(), ConcurrentError> {
    if requested == 0 {
        return Err(ConcurrentError::Honesty(
            "requested concurrency must be ≥ 1".into(),
        ));
    }
    if requested > 1 && achieved < 2 {
        return Err(ConcurrentError::Honesty(format!(
            "plan requested concurrency={requested} but achieved_concurrency={achieved} \
             (serial/metadata-only execution forbidden for concurrency>1)"
        )));
    }
    if requested > 1 && achieved < requested {
        return Err(ConcurrentError::Honesty(format!(
            "plan requested concurrency={requested} but only achieved={achieved} \
             simultaneous workers at overlap barrier"
        )));
    }
    Ok(())
}

/// Execute `plan` against the logical harness with genuinely simultaneous workers.
///
/// - `concurrency == 1`: single-threaded (no thread pool).
/// - `concurrency > 1`: N OS threads, dual-barrier rendezvous so all workers are
///   in-flight together before any runs `execute_plan`, then each evaluates.
pub fn run_logical_concurrent(
    work: &SharedLogicalWork,
    plan: &MeasuredCellPlan,
) -> Result<ConcurrentPlanResult, ConcurrentError> {
    let requested = plan.concurrency.max(1);
    if requested == 1 {
        let mut eng = LogicalHarnessEngine::new();
        eng.load_shared_work(work)?;
        let mut primary = eng.execute_plan(plan)?;
        annotate_detail(&mut primary, 1, 1, 1);
        return Ok(ConcurrentPlanResult {
            requested_concurrency: 1,
            achieved_concurrency: 1,
            worker_count: 1,
            worker_digests: vec![primary.result.as_ref().map(|r| r.values_digest.clone())],
            primary,
        });
    }

    let n = requested as usize;
    let start = Arc::new(Barrier::new(n));
    let overlap = Arc::new(Barrier::new(n));
    let active = Arc::new(AtomicU32::new(0));
    let peak = Arc::new(AtomicU32::new(0));

    let plan = Arc::new(plan.clone());
    let work = Arc::new(work.clone());

    let mut handles = Vec::with_capacity(n);
    for worker_id in 0..n {
        let start = Arc::clone(&start);
        let overlap = Arc::clone(&overlap);
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);
        let plan = Arc::clone(&plan);
        let work = Arc::clone(&work);
        handles.push(thread::spawn(
            move || -> Result<EngineRunOutcome, AdapterError> {
                let mut eng = LogicalHarnessEngine::new();
                eng.load_shared_work(&work)?;

                // Rendezvous so all workers are ready, then enter critical section together.
                start.wait();
                let cur = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(cur, Ordering::SeqCst);
                // All N must be active before any proceeds — proves simultaneous workers.
                overlap.wait();

                let t0 = Instant::now();
                let mut outcome = eng.execute_plan(&plan)?;
                let elapsed = t0.elapsed();
                active.fetch_sub(1, Ordering::SeqCst);

                // Stamp per-worker wall into detail for diagnostics.
                if let Some(d) = outcome.detail.as_mut() {
                    d.push_str(&format!(
                        " worker_id={worker_id} worker_wall_ns={}",
                        elapsed.as_nanos()
                    ));
                }
                Ok(outcome)
            },
        ));
    }

    let mut outcomes: Vec<EngineRunOutcome> = Vec::with_capacity(n);
    for h in handles {
        let o = h
            .join()
            .map_err(|_| ConcurrentError::WorkerPanic)?
            .map_err(ConcurrentError::from)?;
        outcomes.push(o);
    }

    let achieved = peak.load(Ordering::SeqCst);
    assert_concurrency_honest(requested, achieved)?;

    let worker_digests: Vec<Option<String>> = outcomes
        .iter()
        .map(|o| o.result.as_ref().map(|r| r.values_digest.clone()))
        .collect();

    // All Ready workers must agree on values digest (deterministic logical eval).
    let ready_digests: Vec<&str> = worker_digests.iter().filter_map(|d| d.as_deref()).collect();
    if let Some(first) = ready_digests.first() {
        for d in &ready_digests[1..] {
            if d != first {
                return Err(ConcurrentError::Honesty(format!(
                    "worker digest divergence under concurrency={requested}: {first} vs {d}"
                )));
            }
        }
    }

    let primary = merge_worker_outcomes(&outcomes, requested, achieved)?;

    Ok(ConcurrentPlanResult {
        requested_concurrency: requested,
        achieved_concurrency: achieved,
        worker_count: n as u32,
        primary,
        worker_digests,
    })
}

fn annotate_detail(outcome: &mut EngineRunOutcome, requested: u32, achieved: u32, workers: u32) {
    let stamp = format!(
        " concurrency_requested={requested} concurrency_achieved={achieved} workers={workers}"
    );
    match outcome.detail.as_mut() {
        Some(d) => d.push_str(&stamp),
        None => outcome.detail = Some(stamp.trim().into()),
    }
}

/// Prefer worker 0 as primary; merge latency samples across Ready workers.
fn merge_worker_outcomes(
    outcomes: &[EngineRunOutcome],
    requested: u32,
    achieved: u32,
) -> Result<EngineRunOutcome, ConcurrentError> {
    let mut primary = outcomes
        .first()
        .cloned()
        .ok_or_else(|| ConcurrentError::Honesty("no worker outcomes".into()))?;

    let mut lat = LatencyCollector::new();
    let mut examined = 0u64;
    let mut digest = primary
        .result
        .as_ref()
        .map(|r| r.values_digest.clone())
        .unwrap_or_default();
    let mut any_ready = false;

    for o in outcomes {
        if o.status == AdapterStatus::Ready {
            any_ready = true;
            if let Some(m) = &o.metrics {
                if let Some(p50) = m.latency.p50 {
                    // Single-sample worker: use p50 as the sample (one op per worker).
                    lat.record_ns(p50);
                }
                if let Some(ex) = m.path.documents_examined {
                    examined = examined.max(ex);
                }
                if let Some(d) = &m.result_digest_echo {
                    digest = d.clone();
                }
            }
        }
    }

    if any_ready && lat.quantiles().samples > 0 {
        let life = primary
            .metrics
            .as_ref()
            .and_then(|m| m.lifecycle)
            .unwrap_or(crate::metrics::LifecycleClass::WarmSteady);
        let cold = primary.metrics.as_ref().and_then(|m| m.cold_method.clone());
        let explain_plan_digest = primary
            .metrics
            .as_ref()
            .and_then(|m| m.path.explain_plan_digest.clone());
        primary.metrics = Some(assemble_metrics(
            &lat,
            QueryPathMetrics {
                documents_examined: Some(examined),
                logical_bytes_examined: None,
                index_entries_examined: None,
                index_size_bytes: None,
                index_build_ns: None,
                indexed_write_penalty_ns: None,
                explain_plan_digest,
            },
            Some(life),
            cold,
            Some(digest),
            Some(true),
            Some(true),
        ));
    }

    annotate_detail(&mut primary, requested, achieved, outcomes.len() as u32);
    Ok(primary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell_plan::MeasuredCellPlan;
    use crate::cells::MandatoryCell;
    use crate::generator::generate_dataset;
    use crate::shared_work::SharedLogicalWork;

    fn key_plan(c: u32) -> (SharedLogicalWork, MeasuredCellPlan) {
        let plan =
            MeasuredCellPlan::smoke_for(MandatoryCell::KeyGet, 42).with_concurrency(c, false);
        let ds = generate_dataset(&plan.dataset);
        let work = SharedLogicalWork::from_dataset(ds);
        (work, plan)
    }

    #[test]
    fn honesty_rejects_serial_when_requested_gt1() {
        assert!(assert_concurrency_honest(4, 1).is_err());
        assert!(assert_concurrency_honest(2, 1).is_err());
        assert!(assert_concurrency_honest(8, 4).is_err()); // partial < requested
        assert!(assert_concurrency_honest(1, 1).is_ok());
        assert!(assert_concurrency_honest(4, 4).is_ok());
    }

    #[test]
    fn concurrent_c4_achieves_four_workers() {
        let (work, plan) = key_plan(4);
        let r = run_logical_concurrent(&work, &plan).expect("c4");
        assert_eq!(r.requested_concurrency, 4);
        assert_eq!(r.achieved_concurrency, 4);
        assert_eq!(r.worker_count, 4);
        assert_eq!(r.primary.status, AdapterStatus::Ready);
        assert!(r.primary.result.is_some());
        assert_eq!(r.worker_digests.len(), 4);
        // All digests equal
        let d0 = r.worker_digests[0].as_ref().unwrap();
        for d in &r.worker_digests {
            assert_eq!(d.as_ref().unwrap(), d0);
        }
        let detail = r.primary.detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("concurrency_requested=4") && detail.contains("concurrency_achieved=4"),
            "detail={detail}"
        );
    }

    #[test]
    fn concurrent_c1_is_single_threaded() {
        let (work, plan) = key_plan(1);
        let r = run_logical_concurrent(&work, &plan).expect("c1");
        assert_eq!(r.requested_concurrency, 1);
        assert_eq!(r.achieved_concurrency, 1);
        assert_eq!(r.worker_count, 1);
    }

    #[test]
    fn concurrent_c8_and_oversub() {
        let (work, plan) = key_plan(8);
        let r = run_logical_concurrent(&work, &plan).expect("c8");
        assert_eq!(r.achieved_concurrency, 8);

        let mut over =
            MeasuredCellPlan::smoke_for(MandatoryCell::KeyGet, 7).with_concurrency(16, true);
        over.notes = "oversub".into();
        let ds = generate_dataset(&over.dataset);
        let work = SharedLogicalWork::from_dataset(ds);
        let r = run_logical_concurrent(&work, &over).expect("oversub");
        assert_eq!(r.requested_concurrency, 16);
        assert_eq!(r.achieved_concurrency, 16);
    }
}
