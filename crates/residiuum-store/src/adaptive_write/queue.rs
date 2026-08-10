//! Bounded `Mutex<VecDeque<T>>` + `Condvar` queues (plan §4.1, AWO-2).
//!
//! No external queue crate. Capacity is entry-count only; byte credits live in
//! [`super::credits::CreditLedger`].

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// Push failed because the queue is at capacity (or shut down).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueError {
    /// Entry capacity exhausted.
    Full,
    /// Queue has been shut down.
    Shutdown,
}

#[derive(Debug)]
struct QueueInner<T> {
    items: VecDeque<T>,
    capacity: usize,
    shutdown: bool,
}

/// Bounded multi-producer multi-consumer queue.
#[derive(Debug)]
pub struct BoundedQueue<T> {
    inner: Mutex<QueueInner<T>>,
    not_empty: Condvar,
    not_full: Condvar,
}

impl<T> BoundedQueue<T> {
    /// Create a queue with hard entry capacity (`capacity == 0` is allowed but
    /// always full).
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(QueueInner {
                items: VecDeque::with_capacity(capacity.min(1024)),
                capacity,
                shutdown: false,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
        }
    }

    /// Shared handle convenience.
    pub fn shared(capacity: usize) -> Arc<Self> {
        Arc::new(Self::new(capacity))
    }

    /// Non-blocking push.
    pub fn try_push(&self, item: T) -> Result<(), QueueError> {
        let mut g = self.inner.lock().expect("queue lock");
        if g.shutdown {
            return Err(QueueError::Shutdown);
        }
        if g.items.len() >= g.capacity {
            return Err(QueueError::Full);
        }
        g.items.push_back(item);
        self.not_empty.notify_one();
        Ok(())
    }

    /// Blocking push until space, shutdown, or timeout.
    pub fn push_timeout(&self, item: T, timeout: Duration) -> Result<(), QueueError> {
        let mut g = self.inner.lock().expect("queue lock");
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if g.shutdown {
                return Err(QueueError::Shutdown);
            }
            if g.items.len() < g.capacity {
                g.items.push_back(item);
                self.not_empty.notify_one();
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(QueueError::Full);
            }
            let (guard, wait) = self
                .not_full
                .wait_timeout(g, remaining)
                .expect("queue wait");
            g = guard;
            if wait.timed_out() && g.items.len() >= g.capacity && !g.shutdown {
                return Err(QueueError::Full);
            }
        }
    }

    /// Non-blocking pop.
    pub fn try_pop(&self) -> Option<T> {
        let mut g = self.inner.lock().expect("queue lock");
        let item = g.items.pop_front();
        if item.is_some() {
            self.not_full.notify_one();
        }
        item
    }

    /// Blocking pop until an item is available or the queue is shut down and empty.
    pub fn pop_wait(&self) -> Option<T> {
        let mut g = self.inner.lock().expect("queue lock");
        loop {
            if let Some(item) = g.items.pop_front() {
                self.not_full.notify_one();
                return Some(item);
            }
            if g.shutdown {
                return None;
            }
            g = self.not_empty.wait(g).expect("queue wait");
        }
    }

    /// Pop with a timeout. Returns `None` on timeout, or on shutdown with empty queue.
    pub fn pop_timeout(&self, timeout: Duration) -> Option<T> {
        let mut g = self.inner.lock().expect("queue lock");
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(item) = g.items.pop_front() {
                self.not_full.notify_one();
                return Some(item);
            }
            if g.shutdown {
                return None;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let (guard, wait) = self
                .not_empty
                .wait_timeout(g, remaining)
                .expect("queue wait");
            g = guard;
            if wait.timed_out() && g.items.is_empty() {
                return None;
            }
        }
    }

    /// Sleep on the queue condvar up to `timeout` (permit / shutdown wakeups).
    pub fn wait_timeout(&self, timeout: Duration) {
        let g = self.inner.lock().expect("queue lock");
        let _ = self.not_empty.wait_timeout(g, timeout);
    }

    /// Current length.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("queue lock").items.len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Mark shutdown and wake all waiters.
    pub fn shutdown(&self) {
        let mut g = self.inner.lock().expect("queue lock");
        g.shutdown = true;
        self.not_empty.notify_all();
        self.not_full.notify_all();
    }

    /// Whether shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.inner.lock().expect("queue lock").shutdown
    }

    /// Wake waiters (e.g. after permit scale change) without pushing.
    pub fn notify_all_waiters(&self) {
        self.not_empty.notify_all();
        self.not_full.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_and_pop() {
        let q = BoundedQueue::new(2);
        q.try_push(1).unwrap();
        q.try_push(2).unwrap();
        assert_eq!(q.try_push(3), Err(QueueError::Full));
        assert_eq!(q.try_pop(), Some(1));
        q.try_push(3).unwrap();
        assert_eq!(q.try_pop(), Some(2));
        assert_eq!(q.try_pop(), Some(3));
        assert!(q.try_pop().is_none());
    }

    #[test]
    fn shutdown_unblocks() {
        let q = Arc::new(BoundedQueue::<u32>::new(4));
        let q2 = Arc::clone(&q);
        let h = std::thread::spawn(move || q2.pop_wait());
        q.shutdown();
        assert!(h.join().unwrap().is_none());
    }
}
