//! Ticket-indexed ordered ready ring (plan §10, AWO-2).
//!
//! AWO-2 uses `BTreeMap<ticket, outcome>` plus optional byte credits held in
//! the ring. The coordinator (AWO-3/4) removes only the next expected ticket.

use super::persist::LaneTicket;
use std::collections::BTreeMap;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

/// Ready-ring error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadyError {
    /// Duplicate ticket insert.
    DuplicateTicket,
    /// Byte capacity would be exceeded.
    BytesExhausted,
    /// Checked arithmetic overflow.
    Overflow,
}

#[derive(Debug)]
struct ReadyState<T> {
    next_expected: u64,
    ready: BTreeMap<u64, (T, usize)>,
    bytes_held: usize,
    byte_limit: usize,
}

/// Ordered ready ring keyed by lane ticket.
#[derive(Debug)]
pub struct OrderedReadyRing<T> {
    inner: Mutex<ReadyState<T>>,
    progress: Condvar,
}

impl<T> OrderedReadyRing<T> {
    /// Create a ring starting at `first_ticket` with a byte hold limit.
    pub fn new(first_ticket: u64, byte_limit: usize) -> Self {
        Self {
            inner: Mutex::new(ReadyState {
                next_expected: first_ticket,
                ready: BTreeMap::new(),
                bytes_held: 0,
                byte_limit,
            }),
            progress: Condvar::new(),
        }
    }

    /// Insert a completed outcome for `ticket` holding `bytes` of credit.
    ///
    /// On failure, returns the outcome so the caller can retry without clone.
    pub fn push(
        &self,
        ticket: LaneTicket,
        outcome: T,
        bytes: usize,
    ) -> Result<(), (ReadyError, T)> {
        let mut g = self.inner.lock().expect("ready lock");
        if g.ready.contains_key(&ticket.ticket) {
            return Err((ReadyError::DuplicateTicket, outcome));
        }
        let Some(next_bytes) = g.bytes_held.checked_add(bytes) else {
            return Err((ReadyError::Overflow, outcome));
        };
        if next_bytes > g.byte_limit {
            return Err((ReadyError::BytesExhausted, outcome));
        }
        g.ready.insert(ticket.ticket, (outcome, bytes));
        g.bytes_held = next_bytes;
        self.progress.notify_all();
        Ok(())
    }

    /// Pop the next expected ticket if present.
    pub fn try_pop_next(&self) -> Option<(LaneTicket, T)> {
        let mut g = self.inner.lock().expect("ready lock");
        let key = g.next_expected;
        let (outcome, bytes) = g.ready.remove(&key)?;
        g.bytes_held = g.bytes_held.saturating_sub(bytes);
        g.next_expected = g.next_expected.saturating_add(1);
        self.progress.notify_all();
        Some((LaneTicket { ticket: key }, outcome))
    }

    /// Wait until the next expected ticket is ready (or `predicate` says stop).
    pub fn pop_next_wait_while<F>(&self, mut should_wait: F) -> Option<(LaneTicket, T)>
    where
        F: FnMut() -> bool,
    {
        let mut g = self.inner.lock().expect("ready lock");
        loop {
            let key = g.next_expected;
            if let Some((outcome, bytes)) = g.ready.remove(&key) {
                g.bytes_held = g.bytes_held.saturating_sub(bytes);
                g.next_expected = g.next_expected.saturating_add(1);
                self.progress.notify_all();
                return Some((LaneTicket { ticket: key }, outcome));
            }
            if !should_wait() {
                return None;
            }
            g = self.progress.wait(g).expect("ready wait");
        }
    }

    /// Wait until the next expected ticket is ready or `deadline` expires.
    pub fn pop_next_until(&self, deadline: Instant) -> Option<(LaneTicket, T)> {
        let mut g = self.inner.lock().expect("ready lock");
        loop {
            let key = g.next_expected;
            if let Some((outcome, bytes)) = g.ready.remove(&key) {
                g.bytes_held = g.bytes_held.saturating_sub(bytes);
                g.next_expected = g.next_expected.saturating_add(1);
                self.progress.notify_all();
                return Some((LaneTicket { ticket: key }, outcome));
            }
            let remaining = deadline.checked_duration_since(Instant::now())?;
            let (next, timeout) = self
                .progress
                .wait_timeout(g, remaining)
                .expect("ready wait");
            g = next;
            if timeout.timed_out() {
                return None;
            }
        }
    }

    /// Next ticket the coordinator will install.
    pub fn next_expected(&self) -> u64 {
        self.inner.lock().expect("ready lock").next_expected
    }

    /// How many out-of-order outcomes are buffered.
    pub fn buffered_count(&self) -> usize {
        self.inner.lock().expect("ready lock").ready.len()
    }

    /// Bytes currently held by buffered outcomes.
    pub fn bytes_held(&self) -> usize {
        self.inner.lock().expect("ready lock").bytes_held
    }

    /// Whether the next expected ticket is already present.
    pub fn next_is_ready(&self) -> bool {
        let g = self.inner.lock().expect("ready lock");
        g.ready.contains_key(&g.next_expected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn ordered_pop_despite_out_of_order_push() {
        let ring = OrderedReadyRing::new(0, 10_000);
        ring.push(LaneTicket { ticket: 2 }, "c", 1).unwrap();
        ring.push(LaneTicket { ticket: 0 }, "a", 1).unwrap();
        assert_eq!(ring.buffered_count(), 2);
        assert_eq!(ring.try_pop_next().unwrap().1, "a");
        assert!(ring.try_pop_next().is_none());
        ring.push(LaneTicket { ticket: 1 }, "b", 1).unwrap();
        assert_eq!(ring.try_pop_next().unwrap().1, "b");
        assert_eq!(ring.try_pop_next().unwrap().1, "c");
        assert_eq!(ring.next_expected(), 3);
    }

    #[test]
    fn duplicate_rejected() {
        let ring = OrderedReadyRing::new(0, 100);
        ring.push(LaneTicket { ticket: 0 }, 1u8, 1).unwrap();
        let err = ring.push(LaneTicket { ticket: 0 }, 2u8, 1).unwrap_err();
        assert_eq!(err.0, ReadyError::DuplicateTicket);
        assert_eq!(err.1, 2u8);
    }

    #[test]
    fn deadline_returns_without_advancing_expected_ticket() {
        let ring = OrderedReadyRing::<u8>::new(7, 100);
        assert!(ring
            .pop_next_until(Instant::now() + Duration::from_millis(1))
            .is_none());
        assert_eq!(ring.next_expected(), 7);
        ring.push(LaneTicket { ticket: 7 }, 9, 1).unwrap();
        assert_eq!(ring.try_pop_next().unwrap().1, 9);
    }
}
