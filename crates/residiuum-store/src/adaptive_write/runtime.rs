//! AWO-3 Static Intake Arbiter surface (product attach, mode default disabled).
//!
//! - [`AdaptiveWriteMode::Disabled`]: no lease, no cooker; natural store paths.
//! - [`AdaptiveWriteMode::Static`]: lease fences direct `Store` mutation;
//!   independent unconditional puts **collect** then install via multi-item
//!   `put_many_subject_bytes_awo_owned` when a physical store is bound;
//!   [`AdaptiveWriteHandle::admit_put_batch`] uses caller-presented batches.
//! - [`AdaptiveWriteMode::Adaptive`]: lease + pipeline + live [`select_plan`] sizing
//!   (AWO-5 wired); estimator warms from observed batch service times.
//!
//! Callers **must not** hold the physical mutex across [`WriteCompletion::wait`]
//! when collection is active (heap `put_if` waits outside the lock).

use super::collection::IndependentCollector;
use super::controller::{
    AwoClock, ControllerPolicy, ControllerSignals, InstantClock, ManualClock, ScaleController,
};
use super::cooker::PersistentCookerPool;
use super::coordinator::{PipelineCoordinator, PipelineError, PipelineStatus};
use super::credits::{mutation_credit, CreditLedger};
use super::estimator::ServiceEstimator;
use super::policy::{AdaptiveWriteMode, AdaptiveWritePolicy, PolicyError};
use super::selector::{select_plan, CandidateBounds, Selection};
use super::types::AwoPlan;
use crate::durability::DurabilityMode;
use crate::error::StoreError;
use crate::store::{Store, WriteCondition, WriteReceipt};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Closed AWO admission / runtime errors (plan §5).
#[derive(Debug, Clone)]
pub enum AdaptiveWriteError {
    /// Queue entry or byte credit exhausted.
    QueueFull {
        /// Caller may retry after this delay (policy default collection cap).
        retry_after: Duration,
    },
    /// Admission or completion deadline exceeded.
    AdmissionDeadlineExceeded,
    /// Runtime is draining; no new admits.
    Draining,
    /// Mode is disabled — use natural store paths / ordinary create-open.
    ModeDisabled,
    /// Policy failed validation.
    InvalidPolicy(PolicyError),
    /// Writer poisoned after uncertain I/O.
    WriterPoisoned {
        /// Ordinary close/reopen recovery required.
        recovery_required: bool,
    },
    /// Adaptive lease owns mutation; direct path refused.
    WriterActive,
    /// Underlying store error.
    Store(String),
    /// Pipeline depth / seal / shutdown refusal (AWO-4).
    Pipeline(PipelineError),
}

impl AdaptiveWriteError {
    /// Stable wire / metric id.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::QueueFull { .. } => "write_overloaded",
            Self::AdmissionDeadlineExceeded => "write_deadline",
            Self::Draining => "server_draining",
            Self::ModeDisabled => "mode_disabled",
            Self::InvalidPolicy(_) => "invalid_policy",
            Self::WriterPoisoned { .. } => "write_outcome_uncertain",
            Self::WriterActive => "writer_active",
            Self::Store(_) => "store_error",
            Self::Pipeline(PipelineError::DepthExceeded { .. }) => "pipeline_depth_exceeded",
            Self::Pipeline(PipelineError::Sealing) => "pipeline_sealing",
            Self::Pipeline(PipelineError::ShuttingDown) => "pipeline_shutdown",
            Self::Pipeline(_) => "pipeline_error",
        }
    }
}

impl From<StoreError> for AdaptiveWriteError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::AdaptiveWriterPoisoned => Self::WriterPoisoned {
                recovery_required: true,
            },
            StoreError::AdaptiveWriterActive => Self::WriterActive,
            other => Self::Store(other.to_string()),
        }
    }
}

/// Snapshot of adaptive-write runtime (telemetry / drain).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveWriteStatus {
    /// Configured mode.
    pub mode: AdaptiveWriteMode,
    /// Whether the lease fences direct store mutation.
    pub lease_active: bool,
    /// Whether the runtime is draining.
    pub draining: bool,
    /// Active cooker permits (0 when disabled).
    pub active_cookers: usize,
    /// Threads created at warm (0 when disabled).
    pub cooker_threads: usize,
    /// Reserved queue entries.
    pub entries_used: usize,
    /// Reserved queue bytes.
    pub bytes_used: usize,
    /// Pending cook tasks.
    pub pending_cook_tasks: usize,
    /// Pipeline depth snapshot (AWO-4); zeros when disabled.
    pub pipeline: PipelineStatus,
}

/// One-shot completion for an admitted write (plan §5).
#[derive(Debug)]
pub struct WriteCompletion {
    inner: WriteCompletionInner,
}

#[derive(Debug)]
enum WriteCompletionInner {
    /// Already resolved (natural path).
    Ready(Result<WriteReceipt, AdaptiveWriteError>),
    /// Waiting on independent-write collection install.
    Pending(Receiver<Result<WriteReceipt, AdaptiveWriteError>>),
}

impl WriteCompletion {
    fn ready(receipt: Result<WriteReceipt, AdaptiveWriteError>) -> Self {
        Self {
            inner: WriteCompletionInner::Ready(receipt),
        }
    }

    fn pending(rx: Receiver<Result<WriteReceipt, AdaptiveWriteError>>) -> Self {
        Self {
            inner: WriteCompletionInner::Pending(rx),
        }
    }

    /// Block until the write is installed (natural or collected batch).
    ///
    /// **Do not hold the physical store mutex across this call** when collection
    /// is active — the collector needs the same lock to install.
    pub fn wait(self) -> Result<WriteReceipt, AdaptiveWriteError> {
        match self.inner {
            WriteCompletionInner::Ready(r) => r,
            WriteCompletionInner::Pending(rx) => rx.recv().unwrap_or_else(|_| {
                // Sender dropped without reply — not the same as runtime drain.
                Err(AdaptiveWriteError::Store(
                    "collection completion dropped before install reply".into(),
                ))
            }),
        }
    }

    /// Whether the completion already holds a result.
    pub fn is_ready(&self) -> bool {
        matches!(self.inner, WriteCompletionInner::Ready(_))
    }
}

/// Admission outcome.
#[derive(Debug)]
pub enum AdmissionResult {
    /// Accepted; completion may be waited.
    Admitted(WriteCompletion),
    /// Rejected before ownership transfer.
    Rejected(AdaptiveWriteError),
}

/// V1 eligibility class for a mutation (profile-v1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EligibilityClass {
    /// Unconditional inline put — batch-eligible.
    UnconditionalInlinePut,
    /// Unconditional delete — batch-eligible.
    UnconditionalDelete,
    /// Conditional / natural-only path.
    Natural,
}

/// Classify a put/delete for Static routing.
pub fn classify_put(condition: WriteCondition, durability: DurabilityMode) -> EligibilityClass {
    if durability == DurabilityMode::Memory {
        return EligibilityClass::Natural;
    }
    match condition {
        WriteCondition::Unconditional => EligibilityClass::UnconditionalInlinePut,
        _ => EligibilityClass::Natural,
    }
}

/// Classify a delete for Static routing.
pub fn classify_delete(condition: WriteCondition, durability: DurabilityMode) -> EligibilityClass {
    if durability == DurabilityMode::Memory {
        return EligibilityClass::Natural;
    }
    match condition {
        WriteCondition::Unconditional => EligibilityClass::UnconditionalDelete,
        _ => EligibilityClass::Natural,
    }
}

struct RuntimeInner {
    policy: AdaptiveWritePolicy,
    lease_active: bool,
    draining: bool,
    credits: Option<CreditLedger>,
    cooker: Option<PersistentCookerPool>,
    pipeline: Option<Arc<PipelineCoordinator>>,
    /// Independent-write collector (Static/Adaptive when lease active).
    collector: Option<Arc<IndependentCollector>>,
    /// Collector signaled under store lock; join only after mutex release.
    collector_join: Option<Arc<IndependentCollector>>,
    /// Adaptive-mode service estimator (AWO-5).
    estimator: Option<ServiceEstimator>,
    /// Adaptive-mode scale controller (AWO-5).
    scale: Option<ScaleController>,
    /// Process monotonic clock for estimator/controller.
    clock: InstantClock,
    /// Last adaptive selection (tests / telemetry).
    last_selection: Option<Selection>,
}

/// Process-local adaptive write runtime (threads + credit ledger).
pub struct AdaptiveWriteRuntime {
    inner: Mutex<RuntimeInner>,
}

/// Cloneable control/enqueue handle (plan §5).
#[derive(Clone)]
pub struct AdaptiveWriteHandle {
    runtime: Arc<AdaptiveWriteRuntime>,
}

impl AdaptiveWriteHandle {
    /// Build a disabled handle (no lease, no cooker).
    pub fn disabled(policy: AdaptiveWritePolicy) -> Result<Self, AdaptiveWriteError> {
        policy
            .validate()
            .map_err(AdaptiveWriteError::InvalidPolicy)?;
        let mut policy = policy;
        policy.mode = AdaptiveWriteMode::Disabled;
        Ok(Self {
            runtime: Arc::new(AdaptiveWriteRuntime {
                inner: Mutex::new(RuntimeInner {
                    policy,
                    lease_active: false,
                    draining: false,
                    credits: None,
                    cooker: None,
                    pipeline: None,
                    collector: None,
                    collector_join: None,
                    estimator: None,
                    scale: None,
                    clock: InstantClock::new(),
                    last_selection: None,
                }),
            }),
        })
    }

    /// Start a Static/Adaptive runtime and mark the store lease active.
    pub fn start_static(
        policy: AdaptiveWritePolicy,
        store: &mut Store,
    ) -> Result<Self, AdaptiveWriteError> {
        policy
            .validate()
            .map_err(AdaptiveWriteError::InvalidPolicy)?;
        if policy.mode == AdaptiveWriteMode::Disabled {
            return Self::disabled(policy);
        }
        if store.is_awo_writer_poisoned() {
            return Err(AdaptiveWriteError::WriterPoisoned {
                recovery_required: true,
            });
        }
        let credits = CreditLedger::new(policy.queue_entry_limit, policy.queue_byte_limit);
        let cooker = PersistentCookerPool::start(
            policy.maximum_cookers,
            policy.minimum_active_cookers,
            policy.queue_entry_limit.min(4096).max(16),
            policy.queue_byte_limit,
            0,
        );
        let pipeline = Arc::new(PipelineCoordinator::new(policy.pipeline_depth_limit));
        let collector = IndependentCollector::start(
            policy.maximum_batch_entries,
            policy.maximum_collection_delay,
        );
        let adaptive = policy.mode == AdaptiveWriteMode::Adaptive;
        let mut estimator = ServiceEstimator::with_policy_defaults();
        estimator.min_samples = policy.estimator_min_samples;
        estimator.stale_after_ns = policy
            .estimator_stale_after
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        let scale = ScaleController::new(
            ControllerPolicy::machine_defaults(
                policy.minimum_active_cookers,
                policy.maximum_cookers,
            ),
            policy.minimum_active_cookers,
        );
        store.set_awo_lease_active(true);
        Ok(Self {
            runtime: Arc::new(AdaptiveWriteRuntime {
                inner: Mutex::new(RuntimeInner {
                    policy,
                    lease_active: true,
                    draining: false,
                    credits: Some(credits),
                    cooker: Some(cooker),
                    pipeline: Some(pipeline),
                    collector: Some(collector),
                    collector_join: None,
                    estimator: if adaptive { Some(estimator) } else { None },
                    scale: if adaptive { Some(scale) } else { None },
                    clock: InstantClock::new(),
                    last_selection: None,
                }),
            }),
        })
    }

    /// Bind the shared physical store so the collector can install without the
    /// caller holding the mutex across [`WriteCompletion::wait`].
    pub fn bind_physical(&self, physical: Arc<Mutex<Store>>) {
        let g = self.runtime.inner.lock().expect("awo runtime");
        if let Some(c) = g.collector.as_ref() {
            c.bind_physical(physical);
        }
    }

    /// Last adaptive selection (None until Adaptive admit runs select_plan).
    pub fn last_selection(&self) -> Option<Selection> {
        self.runtime
            .inner
            .lock()
            .expect("awo runtime")
            .last_selection
            .clone()
    }

    /// Current status snapshot.
    pub fn status(&self) -> AdaptiveWriteStatus {
        let g = self.runtime.inner.lock().expect("awo runtime");
        let (entries_used, bytes_used) = g
            .credits
            .as_ref()
            .map(|c| (c.entries_used(), c.bytes_used()))
            .unwrap_or((0, 0));
        let (active_cookers, cooker_threads, pending) = g
            .cooker
            .as_ref()
            .map(|c| (c.active_cookers(), c.threads_created(), c.pending_tasks()))
            .unwrap_or((0, 0, 0));
        let pipeline = g
            .pipeline
            .as_ref()
            .map(|p| p.status())
            .unwrap_or(PipelineStatus {
                depth_limit: g.policy.pipeline_depth_limit,
                in_flight: 0,
                cooking: 0,
                installing: 0,
                sealing: false,
                shutting_down: false,
            });
        AdaptiveWriteStatus {
            mode: g.policy.mode,
            lease_active: g.lease_active,
            draining: g.draining,
            active_cookers,
            cooker_threads,
            entries_used,
            bytes_used,
            pending_cook_tasks: pending,
            pipeline,
        }
    }

    /// Pipeline coordinator when Static/Adaptive (AWO-4).
    pub fn pipeline(&self) -> Option<Arc<PipelineCoordinator>> {
        self.runtime
            .inner
            .lock()
            .expect("awo runtime")
            .pipeline
            .clone()
    }

    /// Configured policy mode.
    pub fn mode(&self) -> AdaptiveWriteMode {
        self.runtime.inner.lock().expect("awo runtime").policy.mode
    }

    /// Whether direct store mutation is fenced.
    pub fn lease_active(&self) -> bool {
        self.runtime.inner.lock().expect("awo runtime").lease_active
    }

    /// Admit a put under the adaptive lease (natural execution on AWO-3 floor).
    pub fn admit_put(
        &self,
        store: &mut Store,
        subject: &[u8],
        value: &[u8],
        mode: DurabilityMode,
        condition: WriteCondition,
    ) -> AdmissionResult {
        let class = classify_put(condition, mode);
        // Independent-write collection: enqueue and return pending when bound.
        if class == EligibilityClass::UnconditionalInlinePut {
            if let Some(rx) = self.try_collect_put(store, subject, value, mode) {
                return rx;
            }
        }
        self.admit_natural(store, |s| {
            s.put_subject_bytes_if_awo_owned(subject, value, mode, condition)
        })
    }

    /// Try collection path; `None` means fall back to natural.
    fn try_collect_put(
        &self,
        store: &mut Store,
        subject: &[u8],
        value: &[u8],
        mode: DurabilityMode,
    ) -> Option<AdmissionResult> {
        let (collector, credit) = {
            let g = self.runtime.inner.lock().expect("awo runtime");
            if g.policy.mode == AdaptiveWriteMode::Disabled || !g.lease_active || g.draining {
                return None;
            }
            if store.is_awo_writer_poisoned() {
                return Some(AdmissionResult::Rejected(
                    AdaptiveWriteError::WriterPoisoned {
                        recovery_required: true,
                    },
                ));
            }
            let collector = g.collector.as_ref()?.clone();
            if !collector.is_bound() {
                return None;
            }
            let credit = match mutation_credit(subject.len(), value.len()) {
                Ok(c) => c,
                Err(_) => {
                    return Some(AdmissionResult::Rejected(AdaptiveWriteError::QueueFull {
                        retry_after: g.policy.maximum_collection_delay,
                    }));
                }
            };
            if let Some(credits) = g.credits.as_ref() {
                if credits.try_reserve(1, credit).is_err() {
                    return Some(AdmissionResult::Rejected(AdaptiveWriteError::QueueFull {
                        retry_after: g.policy.maximum_collection_delay,
                    }));
                }
            }
            (collector, credit)
        };

        match collector.enqueue(subject.to_vec(), value.to_vec(), mode) {
            Ok(rx) => {
                // Help-flush when a pile-up is already visible under this lock.
                if collector.pending_len() >= 2 {
                    collector.flush_with_store(store);
                }
                // Credits released at install time inside collector? Currently reserved
                // until natural path release — release after wait in natural only.
                // For collection: release credit when enqueued settles — approximate
                // release now so ledger does not stick (install still holds durability).
                {
                    let g = self.runtime.inner.lock().expect("awo runtime");
                    if let Some(credits) = g.credits.as_ref() {
                        let _ = credits.release(1, credit);
                    }
                }
                Some(AdmissionResult::Admitted(WriteCompletion::pending(rx)))
            }
            Err(e) => {
                let g = self.runtime.inner.lock().expect("awo runtime");
                if let Some(credits) = g.credits.as_ref() {
                    let _ = credits.release(1, credit);
                }
                Some(AdmissionResult::Rejected(e))
            }
        }
    }

    /// Admit a delete under the adaptive lease (natural execution on AWO-3 floor).
    pub fn admit_delete(
        &self,
        store: &mut Store,
        subject: &[u8],
        mode: DurabilityMode,
        condition: WriteCondition,
    ) -> AdmissionResult {
        let _class = classify_delete(condition, mode);
        self.admit_natural(store, |s| {
            s.delete_subject_bytes_if_awo_owned(subject, mode, condition)
        })
    }

    /// Admit a batch of unconditional puts under the lease (AWO-3 deepen).
    ///
    /// Reserves credit for each item, then installs via
    /// [`Store::put_many_awo_owned`] (persist-before-publish; uses store
    /// cook_parallelism when configured). Independent receipts returned in
    /// input order.
    pub fn admit_put_batch(
        &self,
        store: &mut Store,
        items: &[(&str, &[u8])],
        mode: DurabilityMode,
    ) -> Result<Vec<WriteReceipt>, AdaptiveWriteError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        // Memory / conditional work is natural one-by-one (profile natural class).
        if mode == DurabilityMode::Memory {
            let mut out = Vec::with_capacity(items.len());
            for (subject, value) in items {
                match self.admit_put(
                    store,
                    subject.as_bytes(),
                    value,
                    mode,
                    WriteCondition::Unconditional,
                ) {
                    AdmissionResult::Admitted(c) => out.push(c.wait()?),
                    AdmissionResult::Rejected(e) => return Err(e),
                }
            }
            return Ok(out);
        }

        // Adaptive: size the batch via select_plan; Static takes the full slice.
        let take_n = self.plan_batch_take(items, store)?;
        let items = &items[..take_n];

        let mut total_credit: usize = 0;
        for (subject, value) in items {
            let c = mutation_credit(subject.len(), value.len()).map_err(|_| {
                AdaptiveWriteError::QueueFull {
                    retry_after: Duration::from_millis(1),
                }
            })?;
            total_credit = total_credit
                .checked_add(c)
                .ok_or(AdaptiveWriteError::QueueFull {
                    retry_after: Duration::from_millis(1),
                })?;
        }

        let retry_after;
        let pipeline = {
            let g = self.runtime.inner.lock().expect("awo runtime");
            retry_after = g.policy.maximum_collection_delay;
            if g.policy.mode == AdaptiveWriteMode::Disabled {
                return Err(AdaptiveWriteError::ModeDisabled);
            }
            if g.draining {
                return Err(AdaptiveWriteError::Draining);
            }
            if !g.lease_active {
                return Err(AdaptiveWriteError::ModeDisabled);
            }
            if store.is_awo_writer_poisoned() {
                return Err(AdaptiveWriteError::WriterPoisoned {
                    recovery_required: true,
                });
            }
            if let Some(credits) = g.credits.as_ref() {
                if credits.try_reserve(items.len(), total_credit).is_err() {
                    return Err(AdaptiveWriteError::QueueFull { retry_after });
                }
            }
            g.pipeline.clone()
        };

        // AWO-4: one pipeline reservation for this batch (cook → install).
        let reservation = if let Some(ref pipe) = pipeline {
            match pipe.try_begin_reservation() {
                Ok(id) => Some(id),
                Err(e) => {
                    let g = self.runtime.inner.lock().expect("awo runtime");
                    if let Some(credits) = g.credits.as_ref() {
                        let _ = credits.release(items.len(), total_credit);
                    }
                    return Err(AdaptiveWriteError::Pipeline(e));
                }
            }
        } else {
            None
        };

        // Prefer parallel store cook when the runtime has a cooker pool.
        let cooker_n = {
            let mut g = self.runtime.inner.lock().expect("awo runtime");
            let pending = g.cooker.as_ref().map(|c| c.pending_tasks()).unwrap_or(0);
            let active = g.cooker.as_ref().map(|c| c.active_cookers()).unwrap_or(1);
            let now_ns = g.clock.now_ns();
            // Adaptive scale: evaluate once per batch with simple signals.
            if let Some(ref mut scale) = g.scale {
                let util = if active > 0 {
                    if pending > 0 {
                        900_000
                    } else {
                        100_000
                    }
                } else {
                    0
                };
                let sig = ControllerSignals {
                    cook_queue_bytes_increased: pending > 0,
                    cooker_utilization_ppm: util,
                    writer_ready_increasing: false,
                    ready_queue_fill_ppm: 0,
                    positive_marginal_throughput: true,
                    writer_saturated: false,
                    deadline_miss_from_cook: false,
                };
                // Temporary clock view for evaluate (InstantClock is Copy-free; use Manual snapshot).
                let snap = ManualClock::new(now_ns);
                let _ = scale.evaluate(&sig, &snap);
                let target = scale.active_cookers;
                if let Some(ref cooker) = g.cooker {
                    cooker.set_active_cookers(target);
                }
            }
            g.cooker
                .as_ref()
                .map(|c| c.active_cookers().max(1))
                .unwrap_or(1)
        };
        if cooker_n > 1 {
            store.set_cook_parallelism(cooker_n);
        }

        if let (Some(ref pipe), Some(id)) = (pipeline.as_ref(), reservation) {
            let _ = pipe.note_cook_complete(id);
        }

        let t0 = Instant::now();
        let result = store.put_many_awo_owned(items, mode);
        let elapsed_ns = t0.elapsed().as_nanos().min(u64::MAX as u128) as u64;

        if let (Some(ref pipe), Some(id)) = (pipeline.as_ref(), reservation) {
            match &result {
                Ok(_) => {
                    let _ = pipe.note_install_complete(id);
                }
                Err(_) => {
                    let _ = pipe.abort_reservation(id);
                }
            }
        }
        {
            let mut g = self.runtime.inner.lock().expect("awo runtime");
            if let Some(credits) = g.credits.as_ref() {
                let _ = credits.release(items.len(), total_credit);
            }
            if result.is_ok() {
                let now = g.clock.now_ns();
                if let Some(ref mut est) = g.estimator {
                    // Per-item average service sample for natural lower bound.
                    let per = elapsed_ns / (items.len().max(1) as u64);
                    est.ewma.observe(per.max(1), now);
                }
            }
        }
        result.map_err(AdaptiveWriteError::from)
    }

    /// Choose how many items to take from a presented batch (Static = all;
    /// Adaptive = select_plan).
    fn plan_batch_take(
        &self,
        items: &[(&str, &[u8])],
        store: &Store,
    ) -> Result<usize, AdaptiveWriteError> {
        let mut g = self.runtime.inner.lock().expect("awo runtime");
        if g.policy.mode != AdaptiveWriteMode::Adaptive {
            return Ok(items.len());
        }
        let entries_available = g
            .credits
            .as_ref()
            .map(|c| c.entries_available() as u64)
            .unwrap_or(items.len() as u64);
        let bytes_available = g
            .credits
            .as_ref()
            .map(|c| c.bytes_available() as u64)
            .unwrap_or(u64::MAX);
        let avg_bytes = if items.is_empty() {
            0u64
        } else {
            let sum: u64 = items.iter().map(|(s, v)| (s.len() + v.len()) as u64).sum();
            sum / items.len() as u64
        };
        let (warm, stale) = g
            .estimator
            .as_ref()
            .map(|e| e.evidence_flags(&g.clock))
            .unwrap_or((false, true));
        let natural_lower = g
            .estimator
            .as_ref()
            .map(|e| e.ewma.lower_ns().max(1))
            .unwrap_or(1);
        let bounds = CandidateBounds {
            queued_count: items.len() as u64,
            entries_available,
            maximum_batch_entries: g.policy.maximum_batch_entries as u64,
            bytes_per_entry: avg_bytes.max(1),
            bytes_available,
            maximum_batch_bytes: g.policy.maximum_batch_bytes as u64,
            segment_room_entries: items.len() as u64,
        };
        let margin = g.policy.decision_margin_ppm;
        let deadline_ns = g.policy.default_completion_deadline.as_nanos() as u64;
        // Conservative batch upper: natural_lower * n (no free lunch until warm curves).
        let selection = select_plan(
            &bounds,
            natural_lower,
            margin,
            warm,
            stale,
            true,
            deadline_ns,
            |n| natural_lower.saturating_mul(n.saturating_add(1)),
        );
        g.last_selection = Some(selection.clone());
        let _ = store; // future: segment room probe
        let take = match selection.plan {
            AwoPlan::Natural => 1usize,
            AwoPlan::Batch => (selection.entries as usize).clamp(1, items.len()),
        };
        Ok(take)
    }

    fn admit_natural<F>(&self, store: &mut Store, op: F) -> AdmissionResult
    where
        F: FnOnce(&mut Store) -> Result<WriteReceipt, StoreError>,
    {
        let credit = match mutation_credit(0, 0) {
            Ok(c) => c,
            Err(_) => {
                return AdmissionResult::Rejected(AdaptiveWriteError::QueueFull {
                    retry_after: Duration::from_millis(1),
                });
            }
        };

        {
            let g = self.runtime.inner.lock().expect("awo runtime");
            if g.policy.mode == AdaptiveWriteMode::Disabled {
                return AdmissionResult::Rejected(AdaptiveWriteError::ModeDisabled);
            }
            if g.draining {
                return AdmissionResult::Rejected(AdaptiveWriteError::Draining);
            }
            if !g.lease_active {
                return AdmissionResult::Rejected(AdaptiveWriteError::ModeDisabled);
            }
            if store.is_awo_writer_poisoned() {
                return AdmissionResult::Rejected(AdaptiveWriteError::WriterPoisoned {
                    recovery_required: true,
                });
            }
            if let Some(credits) = g.credits.as_ref() {
                if let Err(e) = credits.try_reserve(1, credit) {
                    let _ = e;
                    return AdmissionResult::Rejected(AdaptiveWriteError::QueueFull {
                        retry_after: g.policy.maximum_collection_delay,
                    });
                }
            }
        }

        let receipt = op(store);
        {
            let g = self.runtime.inner.lock().expect("awo runtime");
            if let Some(credits) = g.credits.as_ref() {
                let _ = credits.release(1, credit);
            }
        }

        match receipt {
            Ok(r) => AdmissionResult::Admitted(WriteCompletion::ready(Ok(r))),
            Err(e) => {
                AdmissionResult::Admitted(WriteCompletion::ready(Err(AdaptiveWriteError::from(e))))
            }
        }
    }

    /// Map AWO errors onto store errors for façade boundaries.
    pub fn to_store_error(err: AdaptiveWriteError) -> StoreError {
        match err {
            AdaptiveWriteError::WriterPoisoned { .. } => StoreError::AdaptiveWriterPoisoned,
            AdaptiveWriteError::WriterActive => StoreError::AdaptiveWriterActive,
            AdaptiveWriteError::QueueFull { .. } => StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "awo queue full",
            )),
            AdaptiveWriteError::AdmissionDeadlineExceeded => StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "awo admission deadline",
            )),
            AdaptiveWriteError::Draining => StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "awo draining",
            )),
            AdaptiveWriteError::ModeDisabled => StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "awo mode disabled",
            )),
            AdaptiveWriteError::InvalidPolicy(p) => StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("awo policy: {p:?}"),
            )),
            AdaptiveWriteError::Store(s) => StoreError::Io(std::io::Error::other(s)),
            AdaptiveWriteError::Pipeline(PipelineError::DepthExceeded { .. }) => {
                StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "awo pipeline depth exceeded",
                ))
            }
            AdaptiveWriteError::Pipeline(e) => {
                StoreError::Io(std::io::Error::other(format!("awo pipeline: {e:?}")))
            }
        }
    }

    /// Drain admits until idle or deadline (cooker pending + credits + pipeline).
    pub fn drain_writes(&self, deadline: Instant) -> Result<(), AdaptiveWriteError> {
        let collector = {
            let mut g = self.runtime.inner.lock().expect("awo runtime");
            g.draining = true;
            if let Some(ref p) = g.pipeline {
                p.begin_shutdown();
            }
            g.collector.clone()
        };
        // Force-flush collected independent puts via caller-held path when possible
        // is not available here without store; collector thread will install on delay.
        while Instant::now() < deadline {
            let pending = {
                let g = self.runtime.inner.lock().expect("awo runtime");
                let cook = g.cooker.as_ref().map(|c| c.pending_tasks()).unwrap_or(0);
                let cred = g.credits.as_ref().map(|c| c.entries_used()).unwrap_or(0);
                let pipe = g
                    .pipeline
                    .as_ref()
                    .map(|p| p.status().in_flight)
                    .unwrap_or(0);
                let coll = collector.as_ref().map(|c| c.pending_len()).unwrap_or(0);
                cook + cred + pipe + coll
            };
            if pending == 0 {
                return Ok(());
            }
            std::thread::yield_now();
        }
        Err(AdaptiveWriteError::AdmissionDeadlineExceeded)
    }

    /// Release lease on the store and shut down cookers (drop / reset path).
    ///
    /// Flushes the independent collector under the caller-held store lock, then
    /// **signals** collector shutdown without joining. Call
    /// [`Self::join_after_detach`] **after** releasing the physical mutex —
    /// joining under the lock deadlocks the collector on `physical.lock()`.
    pub fn detach(&self, store: &mut Store) {
        let collector = {
            let mut g = self.runtime.inner.lock().expect("awo runtime");
            g.draining = true;
            g.lease_active = false;
            if let Some(ref p) = g.pipeline {
                p.begin_shutdown();
            }
            if let Some(cooker) = g.cooker.take() {
                cooker.shutdown();
            }
            g.credits = None;
            g.pipeline = None;
            g.collector.take()
        };
        if let Some(c) = collector {
            c.flush_all_with_store(store);
            c.request_shutdown();
            let mut g = self.runtime.inner.lock().expect("awo runtime");
            debug_assert!(
                g.collector_join.is_none(),
                "join_after_detach must run before a second detach"
            );
            g.collector_join = Some(c);
        }
        store.set_awo_lease_active(false);
    }

    /// Join the collector thread signaled by [`Self::detach`].
    ///
    /// Must not be called while the joining thread holds the physical store mutex.
    pub fn join_after_detach(&self) {
        let collector = {
            let mut g = self.runtime.inner.lock().expect("awo runtime");
            g.collector_join.take()
        };
        if let Some(c) = collector {
            c.join_worker();
        }
    }
}
