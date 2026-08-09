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
const MAX_COLLECTION_DELAY: Duration = Duration::from_micros(250);

/// Redacted deployment-wide durable cohort counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OperationCommitStats {
    /// Logical operations admitted to the coordinator.
    pub submitted: u64,
    /// Physical stable-boundary cohorts attempted.
    pub cohorts: u64,
    /// Individual operations that returned a successful new commit.
    pub committed: u64,
    /// Individual operations served from an earlier acceptance.
    pub deduplicated: u64,
    /// Individual operations that returned an error.
    pub failed: u64,
    /// Largest cohort observed since deployment open.
    pub max_cohort_entries: usize,
    /// Largest encoded subject/body payload admitted in one cohort.
    pub max_cohort_bytes: usize,
}

#[derive(Default)]
struct CoordinatorCounters {
    submitted: AtomicU64,
    cohorts: AtomicU64,
    committed: AtomicU64,
    deduplicated: AtomicU64,
    failed: AtomicU64,
    max_cohort_entries: AtomicUsize,
    max_cohort_bytes: AtomicUsize,
}

impl CoordinatorCounters {
    fn snapshot(&self) -> OperationCommitStats {
        OperationCommitStats {
            submitted: self.submitted.load(Ordering::Relaxed),
            cohorts: self.cohorts.load(Ordering::Relaxed),
            committed: self.committed.load(Ordering::Relaxed),
            deduplicated: self.deduplicated.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
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
    reply: SyncSender<Result<(WriteReceipt, bool), StoreError>>,
}

struct CoordinatorState {
    queue: VecDeque<PendingOperation>,
    queued_bytes: usize,
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
        {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| StoreError::CorruptMeta("commit coordinator lock poisoned"))?;
            if state.shutdown {
                return Err(StoreError::CorruptMeta("commit coordinator stopped"));
            }
            state.queued_bytes = state.queued_bytes.saturating_add(bytes);
            state.queue.push_back(PendingOperation {
                subject,
                body,
                condition,
                operation_id,
                content_hash,
                reply,
            });
            self.inner
                .counters
                .submitted
                .fetch_add(1, Ordering::Relaxed);
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

    match outcomes {
        Ok(outcomes) if outcomes.len() == batch.len() => {
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

fn fail_batch(batch: Vec<PendingOperation>, detail: &str) {
    for pending in batch {
        let _ = pending.reply.send(Err(StoreError::Io(std::io::Error::other(
            detail.to_string(),
        ))));
    }
}
