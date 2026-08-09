//! Product commit coordinator for operation-bearing collection mutations.

use crate::error::StoreError;
use crate::kernel::PhysicalStore;
use crate::store::{OperationPut, OperationPutOutcome, WriteCondition, WriteReceipt};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const MAX_COHORT_ENTRIES: usize = 1_024;
const MAX_COHORT_BYTES: usize = 16 * 1024 * 1024;
const MAX_ADMITTED_BYTES: usize = 2 * MAX_COHORT_BYTES;
const MAX_COLLECTION_DELAY: Duration = Duration::from_micros(250);

/// Redacted deployment-wide durable cohort counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OperationCommitStats {
    /// Logical operations admitted to the coordinator.
    pub submitted: u64,
    /// Encoded subject/body bytes admitted to the coordinator.
    pub submitted_bytes: u64,
    /// Byte-credit capacity shared by queued and currently installing work.
    pub admitted_byte_capacity: usize,
    /// Byte credits held by queued and currently installing work.
    pub admitted_bytes: usize,
    /// Highest observed admitted-byte credit usage.
    pub peak_admitted_bytes: usize,
    /// Submissions that waited for byte credits.
    pub byte_admission_waits: u64,
    /// Operations larger than the byte-credit window, admitted exclusively.
    pub oversized_admissions: u64,
    /// Physical stable-boundary cohorts attempted.
    pub cohorts: u64,
    /// Individual operations that returned a successful new commit.
    pub committed: u64,
    /// Individual operations served from an earlier acceptance.
    pub deduplicated: u64,
    /// Individual operations that returned an error.
    pub failed: u64,
    /// Cohorts whose new writes crossed the authoritative-media boundary.
    pub successful_media_sync_cohorts: u64,
    /// Legacy compatibility counter for cohorts that forced the derived
    /// outcome journal. The gathered-WAL path leaves this at zero.
    pub successful_journal_sync_cohorts: u64,
    /// Cohorts appended to the derived outcome lookup without a stable barrier.
    pub successful_journal_append_cohorts: u64,
    /// Largest cohort observed since deployment open.
    pub max_cohort_entries: usize,
    /// Largest encoded subject/body payload admitted in one cohort.
    pub max_cohort_bytes: usize,
}

#[derive(Default)]
struct CoordinatorCounters {
    submitted: AtomicU64,
    submitted_bytes: AtomicU64,
    admitted_bytes: AtomicUsize,
    peak_admitted_bytes: AtomicUsize,
    byte_admission_waits: AtomicU64,
    oversized_admissions: AtomicU64,
    cohorts: AtomicU64,
    committed: AtomicU64,
    deduplicated: AtomicU64,
    failed: AtomicU64,
    successful_media_sync_cohorts: AtomicU64,
    successful_journal_sync_cohorts: AtomicU64,
    successful_journal_append_cohorts: AtomicU64,
    max_cohort_entries: AtomicUsize,
    max_cohort_bytes: AtomicUsize,
}

impl CoordinatorCounters {
    fn snapshot(&self) -> OperationCommitStats {
        OperationCommitStats {
            submitted: self.submitted.load(Ordering::Relaxed),
            submitted_bytes: self.submitted_bytes.load(Ordering::Relaxed),
            admitted_byte_capacity: MAX_ADMITTED_BYTES,
            admitted_bytes: self.admitted_bytes.load(Ordering::Acquire),
            peak_admitted_bytes: self.peak_admitted_bytes.load(Ordering::Relaxed),
            byte_admission_waits: self.byte_admission_waits.load(Ordering::Relaxed),
            oversized_admissions: self.oversized_admissions.load(Ordering::Relaxed),
            cohorts: self.cohorts.load(Ordering::Relaxed),
            committed: self.committed.load(Ordering::Relaxed),
            deduplicated: self.deduplicated.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            successful_media_sync_cohorts: self
                .successful_media_sync_cohorts
                .load(Ordering::Relaxed),
            successful_journal_sync_cohorts: self
                .successful_journal_sync_cohorts
                .load(Ordering::Relaxed),
            successful_journal_append_cohorts: self
                .successful_journal_append_cohorts
                .load(Ordering::Relaxed),
            max_cohort_entries: self.max_cohort_entries.load(Ordering::Relaxed),
            max_cohort_bytes: self.max_cohort_bytes.load(Ordering::Relaxed),
        }
    }
}

struct PendingOperation {
    subject: Vec<u8>,
    body: Vec<u8>,
    condition: WriteCondition,
    operation_id: [u8; 16],
    content_hash: [u8; 32],
    credit_bytes: usize,
    reply: SyncSender<Result<(WriteReceipt, bool), StoreError>>,
}

struct CoordinatorState {
    queue: VecDeque<PendingOperation>,
    queued_bytes: usize,
    admitted_credit_bytes: usize,
    shutdown: bool,
}

struct CoordinatorInner {
    physical: Arc<Mutex<PhysicalStore>>,
    counters: CoordinatorCounters,
    state: Mutex<CoordinatorState>,
    wake: Condvar,
}

/// One deployment-wide cohorting domain shared by every authorized Heap.
pub(super) struct OperationCommitCoordinator {
    inner: Arc<CoordinatorInner>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl OperationCommitCoordinator {
    pub(super) fn start(physical: Arc<Mutex<PhysicalStore>>) -> Arc<Self> {
        let inner = Arc::new(CoordinatorInner {
            physical,
            counters: CoordinatorCounters::default(),
            state: Mutex::new(CoordinatorState {
                queue: VecDeque::new(),
                queued_bytes: 0,
                admitted_credit_bytes: 0,
                shutdown: false,
            }),
            wake: Condvar::new(),
        });
        let worker_inner = Arc::clone(&inner);
        let worker = thread::Builder::new()
            .name("residiuum-commit".into())
            .spawn(move || coordinator_loop(worker_inner))
            .expect("commit coordinator thread");
        Arc::new(Self {
            inner,
            worker: Mutex::new(Some(worker)),
        })
    }

    pub(super) fn submit(
        &self,
        subject: Vec<u8>,
        body: Vec<u8>,
        condition: WriteCondition,
        operation_id: [u8; 16],
        content_hash: [u8; 32],
    ) -> Result<(WriteReceipt, bool), StoreError> {
        let (reply, result) = mpsc::sync_channel(1);
        let bytes = subject.len().saturating_add(body.len());
        let credit_bytes = bytes.min(MAX_ADMITTED_BYTES);
        {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| StoreError::CorruptMeta("commit coordinator lock poisoned"))?;
            let mut waited = false;
            while !state.shutdown
                && state.admitted_credit_bytes > MAX_ADMITTED_BYTES.saturating_sub(credit_bytes)
            {
                if !waited {
                    waited = true;
                    self.inner
                        .counters
                        .byte_admission_waits
                        .fetch_add(1, Ordering::Relaxed);
                }
                state = self
                    .inner
                    .wake
                    .wait(state)
                    .map_err(|_| StoreError::CorruptMeta("commit coordinator lock poisoned"))?;
            }
            if state.shutdown {
                return Err(StoreError::CorruptMeta("commit coordinator stopped"));
            }
            if bytes > MAX_ADMITTED_BYTES {
                self.inner
                    .counters
                    .oversized_admissions
                    .fetch_add(1, Ordering::Relaxed);
            }
            state.admitted_credit_bytes = state
                .admitted_credit_bytes
                .saturating_add(credit_bytes);
            self.inner
                .counters
                .admitted_bytes
                .store(state.admitted_credit_bytes, Ordering::Release);
            self.inner
                .counters
                .peak_admitted_bytes
                .fetch_max(state.admitted_credit_bytes, Ordering::Relaxed);
            state.queued_bytes = state.queued_bytes.saturating_add(bytes);
            state.queue.push_back(PendingOperation {
                subject,
                body,
                condition,
                operation_id,
                content_hash,
                credit_bytes,
                reply,
            });
            self.inner
                .counters
                .submitted
                .fetch_add(1, Ordering::Relaxed);
            self.inner
                .counters
                .submitted_bytes
                .fetch_add(bytes.min(u64::MAX as usize) as u64, Ordering::Relaxed);
        }
        self.inner.wake.notify_one();
        result.recv().unwrap_or_else(|_| {
            Err(StoreError::CorruptMeta(
                "commit coordinator dropped mutation outcome",
            ))
        })
    }

    pub(super) fn stats(&self) -> OperationCommitStats {
        self.inner.counters.snapshot()
    }
}

impl Drop for OperationCommitCoordinator {
    fn drop(&mut self) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.shutdown = true;
            self.inner.wake.notify_all();
        }
        if let Ok(mut worker) = self.worker.lock() {
            if let Some(worker) = worker.take() {
                let _ = worker.join();
            }
        }
    }
}

fn coordinator_loop(inner: Arc<CoordinatorInner>) {
    loop {
        let batch = {
            let mut state = match inner.state.lock() {
                Ok(state) => state,
                Err(_) => return,
            };
            while state.queue.is_empty() && !state.shutdown {
                state = match inner.wake.wait(state) {
                    Ok(state) => state,
                    Err(_) => return,
                };
            }
            if state.shutdown && state.queue.is_empty() {
                return;
            }
            let deadline = Instant::now() + MAX_COLLECTION_DELAY;
            while state.queue.len() < MAX_COHORT_ENTRIES
                && state.queued_bytes < MAX_COHORT_BYTES
                && !state.shutdown
            {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                let (next_state, timed_out) = match inner.wake.wait_timeout(state, remaining) {
                    Ok((state, timeout)) => (state, timeout.timed_out()),
                    Err(_) => return,
                };
                state = next_state;
                if timed_out {
                    break;
                }
            }

            let mut batch = Vec::new();
            let mut bytes = 0usize;
            while batch.len() < MAX_COHORT_ENTRIES {
                let Some(next) = state.queue.front() else {
                    break;
                };
                let next_bytes = next.subject.len().saturating_add(next.body.len());
                if !batch.is_empty() && bytes.saturating_add(next_bytes) > MAX_COHORT_BYTES {
                    break;
                }
                let next = state.queue.pop_front().expect("front existed");
                state.queued_bytes = state.queued_bytes.saturating_sub(next_bytes);
                bytes = bytes.saturating_add(next_bytes);
                batch.push(next);
            }
            batch
        };

        install_batch(&inner, batch);
    }
}

fn install_batch(inner: &CoordinatorInner, batch: Vec<PendingOperation>) {
    if batch.is_empty() {
        return;
    }
    let batch_bytes = batch.iter().fold(0usize, |total, pending| {
        total
            .saturating_add(pending.subject.len())
            .saturating_add(pending.body.len())
    });
    let batch_credits = batch.iter().fold(0usize, |total, pending| {
        total.saturating_add(pending.credit_bytes)
    });
    inner.counters.cohorts.fetch_add(1, Ordering::Relaxed);
    inner
        .counters
        .max_cohort_entries
        .fetch_max(batch.len(), Ordering::Relaxed);
    inner
        .counters
        .max_cohort_bytes
        .fetch_max(batch_bytes, Ordering::Relaxed);
    let requests: Vec<_> = batch
        .iter()
        .map(|pending| OperationPut {
            subject: pending.subject.as_slice(),
            body: pending.body.as_slice(),
            condition: pending.condition,
            operation_id: pending.operation_id,
            content_hash: pending.content_hash,
        })
        .collect();
    let outcomes = inner
        .physical
        .lock()
        .map_err(|_| StoreError::CorruptMeta("store lock poisoned"))
        .and_then(|mut store| store.put_operation_cohort_awo_owned(&requests));
    // The byte window covers queued data plus physical installation. Outcome
    // delivery retains no subject/body ownership requirement for admission.
    release_byte_credits(inner, batch_credits);

    match outcomes {
        Ok(outcomes) if outcomes.len() == batch.len() => {
            let committed = outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Ok(value) if !value.deduplicated))
                .count() as u64;
            if committed > 0 {
                inner
                    .counters
                    .successful_media_sync_cohorts
                    .fetch_add(1, Ordering::Relaxed);
                inner
                    .counters
                    .successful_journal_append_cohorts
                    .fetch_add(1, Ordering::Relaxed);
            }
            for (pending, outcome) in batch.into_iter().zip(outcomes) {
                match &outcome {
                    Ok(value) if value.deduplicated => {
                        inner.counters.deduplicated.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(_) => {
                        inner.counters.committed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        inner.counters.failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                let result = outcome.map(
                    |OperationPutOutcome {
                         receipt,
                         deduplicated,
                     }| (receipt, deduplicated),
                );
                let _ = pending.reply.send(result);
            }
        }
        Ok(_) => {
            inner
                .counters
                .failed
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
            fail_batch(batch, "commit cohort returned the wrong outcome count");
        }
        Err(error) => {
            inner
                .counters
                .failed
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
            fail_batch(batch, &format!("commit cohort failed: {error}"));
        }
    }
}

fn release_byte_credits(inner: &CoordinatorInner, credits: usize) {
    if let Ok(mut state) = inner.state.lock() {
        state.admitted_credit_bytes = state.admitted_credit_bytes.saturating_sub(credits);
        inner
            .counters
            .admitted_bytes
            .store(state.admitted_credit_bytes, Ordering::Release);
        inner.wake.notify_all();
    }
}

fn fail_batch(batch: Vec<PendingOperation>, detail: &str) {
    for pending in batch {
        let _ = pending.reply.send(Err(StoreError::Io(std::io::Error::other(
            detail.to_string(),
        ))));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    #[test]
    fn byte_credits_bound_queue_plus_install_and_release_after_completion() {
        let directory = tempfile::tempdir().unwrap();
        let physical = Arc::new(Mutex::new(Store::create(directory.path()).unwrap()));
        let coordinator = OperationCommitCoordinator::start(Arc::clone(&physical));
        let physical_guard = physical.lock().unwrap();
        let body_bytes = 16 * 1024 * 1024;

        let first = {
            let coordinator = Arc::clone(&coordinator);
            thread::spawn(move || {
                coordinator.submit(
                    b"credit/first".to_vec(),
                    vec![0x41; body_bytes],
                    WriteCondition::Unconditional,
                    [0x11; 16],
                    [0x21; 32],
                )
            })
        };

        let deadline = Instant::now() + Duration::from_secs(5);
        while coordinator.stats().admitted_bytes < body_bytes {
            assert!(Instant::now() < deadline, "first operation was not admitted");
            thread::yield_now();
        }

        let second = {
            let coordinator = Arc::clone(&coordinator);
            thread::spawn(move || {
                coordinator.submit(
                    b"credit/second".to_vec(),
                    vec![0x42; body_bytes],
                    WriteCondition::Unconditional,
                    [0x12; 16],
                    [0x22; 32],
                )
            })
        };

        while coordinator.stats().byte_admission_waits == 0 {
            assert!(Instant::now() < deadline, "second operation did not wait for credits");
            thread::yield_now();
        }
        let blocked = coordinator.stats();
        assert!(blocked.admitted_bytes <= blocked.admitted_byte_capacity);
        assert!(blocked.peak_admitted_bytes <= blocked.admitted_byte_capacity);

        drop(physical_guard);
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
        let complete = coordinator.stats();
        assert_eq!(complete.admitted_bytes, 0);
        assert!(complete.byte_admission_waits >= 1);
        assert!(complete.peak_admitted_bytes <= complete.admitted_byte_capacity);
    }
}
