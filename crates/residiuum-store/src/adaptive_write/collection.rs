//! Independent-write collection (T9 residual connect).
//!
//! Unconditional Buffered/Durable singles enqueue here; a collector installs
//! multi-item batches via [`Store::put_many_awo_owned`] so concurrent admits
//! share one Durable barrier. Natural (immediate) path remains for Memory,
//! conditionals, and when no physical store is bound.

use crate::durability::DurabilityMode;
use crate::error::StoreError;
use crate::store::{Store, WriteReceipt};
use std::collections::VecDeque;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use super::runtime::AdaptiveWriteError;

/// One independent put waiting for install.
pub(crate) struct PendingPut {
    pub subject: Vec<u8>,
    pub value: Vec<u8>,
    pub mode: DurabilityMode,
    pub tx: SyncSender<Result<WriteReceipt, AdaptiveWriteError>>,
    pub enqueued_at: Instant,
}

struct CollectInner {
    queue: VecDeque<PendingPut>,
    /// Bound physical store for background flush (optional until host binds).
    physical: Option<Arc<Mutex<Store>>>,
    shutdown: bool,
    /// Maximum entries per install.
    max_batch_entries: usize,
    /// Spec maximum collection delay.
    collection_delay: Duration,
}

/// Shared collection state + collector thread control.
pub(crate) struct IndependentCollector {
    inner: Mutex<CollectInner>,
    wake: Condvar,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl IndependentCollector {
    pub fn start(max_batch_entries: usize, collection_delay: Duration) -> Arc<Self> {
        let this = Arc::new(Self {
            inner: Mutex::new(CollectInner {
                queue: VecDeque::new(),
                physical: None,
                shutdown: false,
                max_batch_entries: max_batch_entries.max(1),
                collection_delay,
            }),
            wake: Condvar::new(),
            join: Mutex::new(None),
        });
        let worker = Arc::clone(&this);
        let handle = thread::Builder::new()
            .name("awo-collect".into())
            .spawn(move || worker.collector_loop())
            .expect("awo collector thread");
        *this.join.lock().expect("join lock") = Some(handle);
        this
    }

    pub fn bind_physical(&self, physical: Arc<Mutex<Store>>) {
        let mut g = self.inner.lock().expect("collect lock");
        g.physical = Some(physical);
        self.wake.notify_all();
    }

    pub fn is_bound(&self) -> bool {
        self.inner.lock().expect("collect lock").physical.is_some()
    }

    pub fn pending_len(&self) -> usize {
        self.inner.lock().expect("collect lock").queue.len()
    }

    /// Enqueue one put; returns a receiver for the install receipt.
    pub fn enqueue(
        &self,
        subject: Vec<u8>,
        value: Vec<u8>,
        mode: DurabilityMode,
    ) -> Result<Receiver<Result<WriteReceipt, AdaptiveWriteError>>, AdaptiveWriteError> {
        let (tx, rx) = mpsc::sync_channel(1);
        {
            let mut g = self.inner.lock().expect("collect lock");
            if g.shutdown {
                return Err(AdaptiveWriteError::Draining);
            }
            if g.physical.is_none() {
                return Err(AdaptiveWriteError::Store(
                    "collection requires bound physical store".into(),
                ));
            }
            g.queue.push_back(PendingPut {
                subject,
                value,
                mode,
                tx,
                enqueued_at: Instant::now(),
            });
        }
        self.wake.notify_one();
        Ok(rx)
    }

    /// Flush now using a store already locked by the caller (help path).
    pub fn flush_with_store(&self, store: &mut Store) {
        let batch = self.take_batch(true);
        if !batch.is_empty() {
            Self::install_batch(store, batch);
        }
    }

    /// Drain everything (shutdown / drain_writes).
    pub fn flush_all_with_store(&self, store: &mut Store) {
        loop {
            let batch = self.take_batch(true);
            if batch.is_empty() {
                break;
            }
            Self::install_batch(store, batch);
        }
    }

    /// Request collector exit (flag + wake). Does **not** join the worker.
    ///
    /// Callers that hold the physical store mutex (detach/Drop) must signal
    /// under the lock after `flush_all_with_store`, then [`join_worker`] only
    /// **after** releasing the mutex — otherwise the worker blocks on
    /// `physical.lock()` while detach waits on join (process hang).
    pub fn request_shutdown(&self) {
        {
            let mut g = self.inner.lock().expect("collect lock");
            g.shutdown = true;
        }
        self.wake.notify_all();
    }

    /// Join the collector thread. Safe only when the physical mutex is **not**
    /// held by the joining thread (see [`request_shutdown`]).
    pub fn join_worker(&self) {
        if let Some(h) = self.join.lock().expect("join lock").take() {
            let _ = h.join();
        }
        // Fail any stragglers if physical gone / install abandoned.
        let mut leftover = self.take_batch(true);
        for p in leftover.drain(..) {
            let _ = p.tx.send(Err(AdaptiveWriteError::Draining));
        }
    }

    /// Signal + join (collector Drop / tests that do not hold the store lock).
    pub fn shutdown(&self) {
        self.request_shutdown();
        self.join_worker();
    }

    fn is_shutdown(&self) -> bool {
        self.inner.lock().expect("collect lock").shutdown
    }

    fn take_batch(&self, force: bool) -> Vec<PendingPut> {
        let mut g = self.inner.lock().expect("collect lock");
        if g.queue.is_empty() {
            return Vec::new();
        }
        if !force {
            let n = g.queue.len();
            let oldest = g
                .queue
                .front()
                .map(|p| p.enqueued_at)
                .unwrap_or_else(Instant::now);
            let aged = oldest.elapsed() >= g.collection_delay;
            // Coalesce when concurrent pile-up or collection window elapsed.
            if n < 2 && !aged {
                return Vec::new();
            }
        }
        let take = g.queue.len().min(g.max_batch_entries);
        g.queue.drain(..take).collect()
    }

    fn install_batch(store: &mut Store, batch: Vec<PendingPut>) {
        if batch.is_empty() {
            return;
        }
        // Group by durability mode (strongest-first not required for v1: one mode per batch).
        // Split if mixed modes appear.
        let mut i = 0;
        while i < batch.len() {
            let mode = batch[i].mode;
            let mut j = i + 1;
            while j < batch.len() && batch[j].mode == mode {
                j += 1;
            }
            let slice = &batch[i..j];
            let items: Vec<(&[u8], &[u8])> = slice
                .iter()
                .map(|p| (p.subject.as_slice(), p.value.as_slice()))
                .collect();
            match store.put_many_subject_bytes_awo_owned(&items, mode) {
                Ok(receipts) => {
                    if receipts.len() != slice.len() {
                        // Fail closed: short receipt vectors used to drop PendingPut
                        // senders (waiters saw disconnect → false "awo draining").
                        let err = AdaptiveWriteError::Store(format!(
                            "collection install receipt count {} != batch {}",
                            receipts.len(),
                            slice.len()
                        ));
                        for p in slice {
                            let _ = p.tx.send(Err(err.clone()));
                        }
                    } else {
                        for (p, r) in slice.iter().zip(receipts.into_iter()) {
                            let _ = p.tx.send(Ok(r));
                        }
                    }
                }
                Err(e) => {
                    let err = AdaptiveWriteError::from(e);
                    for p in slice {
                        let _ = p.tx.send(Err(err.clone()));
                    }
                }
            }
            i = j;
        }
    }

    fn collector_loop(&self) {
        loop {
            let (physical, delay, should_exit) = {
                let mut g = self.inner.lock().expect("collect lock");
                if g.shutdown && g.queue.is_empty() {
                    break;
                }
                if g.queue.is_empty() {
                    g = self.wake.wait(g).expect("collect wait");
                    if g.shutdown && g.queue.is_empty() {
                        break;
                    }
                    continue;
                }
                let delay = g.collection_delay;
                let physical = g.physical.clone();
                let should_exit = g.shutdown;
                (physical, delay, should_exit)
            };

            // Collection window: interruptible wait so detach shutdown is not
            // stuck behind a full `thread::sleep(delay)`.
            if !delay.is_zero() && !should_exit {
                let g = self.inner.lock().expect("collect lock");
                if !g.shutdown {
                    let (_g, _) = self
                        .wake
                        .wait_timeout(g, delay)
                        .expect("collect delay wait");
                }
                if self.is_shutdown() && self.pending_len() == 0 {
                    break;
                }
            }

            let Some(physical) = physical else {
                continue;
            };
            // try_lock: detach may hold the mutex while requesting shutdown after
            // an under-lock flush — blocking here deadlocks join_worker.
            let mut store = match physical.try_lock() {
                Ok(g) => g,
                Err(std::sync::TryLockError::WouldBlock) => {
                    if self.is_shutdown() {
                        break;
                    }
                    thread::yield_now();
                    continue;
                }
                Err(std::sync::TryLockError::Poisoned(_)) => continue,
            };
            // Force flush: after delay even a single item installs.
            self.flush_all_with_store(&mut store);
            if should_exit || self.is_shutdown() {
                self.flush_all_with_store(&mut store);
                break;
            }
        }
    }
}

impl Drop for IndependentCollector {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Test helper: error conversion already on AdaptiveWriteError.
#[allow(dead_code)]
fn _store_err(e: StoreError) -> AdaptiveWriteError {
    AdaptiveWriteError::from(e)
}
