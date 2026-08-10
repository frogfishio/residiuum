//! AWO-4 ordered pipeline coordinator (depth ≤ 2, seal-safe, bounded shutdown).
//!
//! Normative overlap (plan §9 / AWO-4):
//! - **Batch A** may be installing (persist-before-publish) while
//! - **Batch B** is already reserved and cooking;
//! - a **third** reservation must not pass while both are unresolved.
//!
//! This module is the pure reservation/phase ledger. Product cook/install still
//! run under the AWO-3 lease paths; the coordinator refuses illegal depth and
//! seal/shutdown races. No Tokio; `std` only.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Phase of an unresolved pipeline reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservationPhase {
    /// Identities reserved; frames cooking (or ready for install).
    Cooking,
    /// Persist/install in progress (store lock owned by writer path).
    Installing,
}

/// Process-local reservation identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReservationId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
struct InFlight {
    id: ReservationId,
    phase: ReservationPhase,
}

/// Why a reservation or phase transition was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineError {
    /// Unresolved count already at `pipeline_depth_limit` (no third).
    DepthExceeded {
        /// Configured limit (V1 default 2).
        limit: usize,
        /// Current unresolved count.
        in_flight: usize,
    },
    /// Segment rotation / seal in progress; no new reservation.
    Sealing,
    /// Coordinator is shutting down; no new reservation.
    ShuttingDown,
    /// Unknown reservation id.
    UnknownReservation,
    /// Illegal phase transition (e.g. install before cook complete).
    BadPhase {
        /// Observed phase.
        found: ReservationPhase,
        /// Expected phase for this op.
        expected: ReservationPhase,
    },
    /// Shutdown drain exceeded deadline with work still in flight.
    DrainTimeout {
        /// Reservations still unresolved at deadline.
        remaining: usize,
    },
}

/// Snapshot for tests / status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineStatus {
    /// Max unresolved reservations (1..=4; V1 = 2).
    pub depth_limit: usize,
    /// Current unresolved count.
    pub in_flight: usize,
    /// Count in [`ReservationPhase::Cooking`].
    pub cooking: usize,
    /// Count in [`ReservationPhase::Installing`].
    pub installing: usize,
    /// Seal fence active.
    pub sealing: bool,
    /// Shutdown (reject new admits).
    pub shutting_down: bool,
}

/// Ordered pipeline depth ledger (one per writer shard in product integration).
pub struct PipelineCoordinator {
    inner: Mutex<PipelineInner>,
    next_id: AtomicU64,
}

struct PipelineInner {
    depth_limit: usize,
    in_flight: VecDeque<InFlight>,
    sealing: bool,
    shutting_down: bool,
}

impl PipelineCoordinator {
    /// Create with closed depth limit (clamped to 1..=4).
    pub fn new(depth_limit: usize) -> Self {
        let depth_limit = depth_limit.clamp(1, 4);
        Self {
            inner: Mutex::new(PipelineInner {
                depth_limit,
                in_flight: VecDeque::new(),
                sealing: false,
                shutting_down: false,
            }),
            next_id: AtomicU64::new(1),
        }
    }

    /// V1 default: depth 2.
    pub fn depth_two() -> Self {
        Self::new(2)
    }

    /// Begin a new reservation (Cooking). Fails if depth full, sealing, or shutdown.
    pub fn try_begin_reservation(&self) -> Result<ReservationId, PipelineError> {
        let mut g = self.inner.lock().expect("pipeline lock");
        if g.shutting_down {
            return Err(PipelineError::ShuttingDown);
        }
        if g.sealing {
            return Err(PipelineError::Sealing);
        }
        if g.in_flight.len() >= g.depth_limit {
            return Err(PipelineError::DepthExceeded {
                limit: g.depth_limit,
                in_flight: g.in_flight.len(),
            });
        }
        let id = ReservationId(self.next_id.fetch_add(1, Ordering::Relaxed));
        g.in_flight.push_back(InFlight {
            id,
            phase: ReservationPhase::Cooking,
        });
        Ok(id)
    }

    /// Mark cook complete → Installing (batch ready for persist-before-publish).
    pub fn note_cook_complete(&self, id: ReservationId) -> Result<(), PipelineError> {
        let mut g = self.inner.lock().expect("pipeline lock");
        let slot = g
            .in_flight
            .iter_mut()
            .find(|r| r.id == id)
            .ok_or(PipelineError::UnknownReservation)?;
        if slot.phase != ReservationPhase::Cooking {
            return Err(PipelineError::BadPhase {
                found: slot.phase,
                expected: ReservationPhase::Cooking,
            });
        }
        slot.phase = ReservationPhase::Installing;
        Ok(())
    }

    /// Mark install/publish complete; drop reservation from the pipeline.
    pub fn note_install_complete(&self, id: ReservationId) -> Result<(), PipelineError> {
        let mut g = self.inner.lock().expect("pipeline lock");
        let pos = g
            .in_flight
            .iter()
            .position(|r| r.id == id)
            .ok_or(PipelineError::UnknownReservation)?;
        let found = g.in_flight[pos].phase;
        if found != ReservationPhase::Installing {
            return Err(PipelineError::BadPhase {
                found,
                expected: ReservationPhase::Installing,
            });
        }
        g.in_flight.remove(pos);
        Ok(())
    }

    /// Abort a reservation in any phase (failure / poison path).
    pub fn abort_reservation(&self, id: ReservationId) -> Result<(), PipelineError> {
        let mut g = self.inner.lock().expect("pipeline lock");
        let pos = g
            .in_flight
            .iter()
            .position(|r| r.id == id)
            .ok_or(PipelineError::UnknownReservation)?;
        g.in_flight.remove(pos);
        Ok(())
    }

    /// Enter seal fence: reject new reservations until [`Self::end_seal`].
    pub fn begin_seal(&self) {
        self.inner.lock().expect("pipeline lock").sealing = true;
    }

    /// Leave seal fence after rotation completes.
    pub fn end_seal(&self) {
        self.inner.lock().expect("pipeline lock").sealing = false;
    }

    /// Begin shutdown: no new reservations; existing may complete.
    pub fn begin_shutdown(&self) {
        self.inner.lock().expect("pipeline lock").shutting_down = true;
    }

    /// Whether shutdown has been requested.
    pub fn is_shutting_down(&self) -> bool {
        self.inner.lock().expect("pipeline lock").shutting_down
    }

    /// Wait until `in_flight == 0` or deadline (bounded shutdown).
    ///
    /// Does not complete reservations itself — the product writer must call
    /// [`Self::note_install_complete`] / [`Self::abort_reservation`]. This
    /// poll is for tests and host `drain_writes` integration.
    pub fn wait_empty(&self, deadline: Instant) -> Result<(), PipelineError> {
        loop {
            let remaining = {
                let g = self.inner.lock().expect("pipeline lock");
                g.in_flight.len()
            };
            if remaining == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(PipelineError::DrainTimeout { remaining });
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Status snapshot.
    pub fn status(&self) -> PipelineStatus {
        let g = self.inner.lock().expect("pipeline lock");
        let cooking = g
            .in_flight
            .iter()
            .filter(|r| r.phase == ReservationPhase::Cooking)
            .count();
        let installing = g
            .in_flight
            .iter()
            .filter(|r| r.phase == ReservationPhase::Installing)
            .count();
        PipelineStatus {
            depth_limit: g.depth_limit,
            in_flight: g.in_flight.len(),
            cooking,
            installing,
            sealing: g.sealing,
            shutting_down: g.shutting_down,
        }
    }

    /// In-flight reservation ids (oldest first).
    pub fn in_flight_ids(&self) -> Vec<ReservationId> {
        self.inner
            .lock()
            .expect("pipeline lock")
            .in_flight
            .iter()
            .map(|r| r.id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_two_blocks_third() {
        let p = PipelineCoordinator::depth_two();
        let a = p.try_begin_reservation().unwrap();
        let b = p.try_begin_reservation().unwrap();
        assert_ne!(a, b);
        assert!(matches!(
            p.try_begin_reservation(),
            Err(PipelineError::DepthExceeded {
                limit: 2,
                in_flight: 2
            })
        ));
        p.note_cook_complete(a).unwrap();
        p.note_install_complete(a).unwrap();
        let c = p.try_begin_reservation().unwrap();
        assert_eq!(p.status().in_flight, 2); // b cooking + c cooking
        let _ = c;
    }

    #[test]
    fn cook_then_install_overlap_shape() {
        let p = PipelineCoordinator::depth_two();
        let a = p.try_begin_reservation().unwrap();
        let b = p.try_begin_reservation().unwrap();
        // Write A while cook B: A → Installing, B stays Cooking.
        p.note_cook_complete(a).unwrap();
        let st = p.status();
        assert_eq!(st.installing, 1);
        assert_eq!(st.cooking, 1);
        assert!(p.try_begin_reservation().is_err());
        p.note_install_complete(a).unwrap();
        // B still cooking; can start C? depth = 1 (B only) → yes
        let c = p.try_begin_reservation().unwrap();
        p.note_cook_complete(b).unwrap();
        p.note_install_complete(b).unwrap();
        p.note_cook_complete(c).unwrap();
        p.note_install_complete(c).unwrap();
        assert_eq!(p.status().in_flight, 0);
    }

    #[test]
    fn seal_and_shutdown_refuse_new() {
        let p = PipelineCoordinator::depth_two();
        p.begin_seal();
        assert_eq!(p.try_begin_reservation(), Err(PipelineError::Sealing));
        p.end_seal();
        let id = p.try_begin_reservation().unwrap();
        p.begin_shutdown();
        assert_eq!(p.try_begin_reservation(), Err(PipelineError::ShuttingDown));
        p.note_cook_complete(id).unwrap();
        p.note_install_complete(id).unwrap();
        p.wait_empty(Instant::now() + Duration::from_secs(1))
            .unwrap();
    }
}
