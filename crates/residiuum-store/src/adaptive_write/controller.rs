//! Autoscaling controller with hysteresis (plan §11.6, AWO-5).
//!
//! Evaluates on an injected [`AwoClock`] — correctness tests never sleep on
//! wall time. Scale-up / scale-down consecutive intervals and dwell come from
//! `policy-v1.json` defaults.

use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic nanosecond clock for AWO control loops.
pub trait AwoClock {
    /// Monotonic nanoseconds (not wall clock).
    fn now_ns(&self) -> u64;
}

/// Real monotonic clock (`std::time::Instant` based, process-local).
#[derive(Debug, Clone)]
pub struct InstantClock {
    origin: std::time::Instant,
}

impl InstantClock {
    /// Start at process Instant::now() as zero.
    pub fn new() -> Self {
        Self {
            origin: std::time::Instant::now(),
        }
    }
}

impl Default for InstantClock {
    fn default() -> Self {
        Self::new()
    }
}

impl AwoClock for InstantClock {
    fn now_ns(&self) -> u64 {
        self.origin.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }
}

/// Manual clock for deterministic tests.
#[derive(Debug)]
pub struct ManualClock {
    now: AtomicU64,
}

impl ManualClock {
    /// Create at `start_ns`.
    pub fn new(start_ns: u64) -> Self {
        Self {
            now: AtomicU64::new(start_ns),
        }
    }

    /// Advance by `delta_ns`.
    pub fn advance(&self, delta_ns: u64) {
        self.now.fetch_add(delta_ns, Ordering::SeqCst);
    }

    /// Set absolute time.
    pub fn set(&self, ns: u64) {
        self.now.store(ns, Ordering::SeqCst);
    }
}

impl AwoClock for ManualClock {
    fn now_ns(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }
}

/// Closed controller knobs (policy-v1 defaults).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControllerPolicy {
    /// Evaluation period (ns). Default 100 ms.
    pub interval_ns: u64,
    /// Consecutive intervals for scale-up. Default 5.
    pub scale_up_consecutive: u32,
    /// Consecutive intervals for scale-down. Default 20.
    pub scale_down_consecutive: u32,
    /// Cooker utilisation ppm for scale-up. Default 800_000 (80%).
    pub scale_up_utilization_ppm: u32,
    /// Cooker utilisation ppm for scale-down. Default 300_000 (30%).
    pub scale_down_utilization_ppm: u32,
    /// Ready-queue fill ppm that forces scale-down path. Default 750_000.
    pub ready_queue_scale_down_ppm: u32,
    /// Min dwell after scale-up (ns). Default 500 ms.
    pub scale_up_dwell_ns: u64,
    /// Min dwell after scale-down (ns). Default 2 s.
    pub scale_down_dwell_ns: u64,
    /// Minimum active cookers.
    pub minimum_active_cookers: usize,
    /// Maximum cookers.
    pub maximum_cookers: usize,
}

impl ControllerPolicy {
    /// Defaults from `policy-v1.json`.
    pub fn machine_defaults(min_cookers: usize, max_cookers: usize) -> Self {
        Self {
            interval_ns: 100_000_000,
            scale_up_consecutive: 5,
            scale_down_consecutive: 20,
            scale_up_utilization_ppm: 800_000,
            scale_down_utilization_ppm: 300_000,
            ready_queue_scale_down_ppm: 750_000,
            scale_up_dwell_ns: 500_000_000,
            scale_down_dwell_ns: 2_000_000_000,
            minimum_active_cookers: min_cookers.max(1),
            maximum_cookers: max_cookers.max(1),
        }
    }
}

/// Observed signals for one controller interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControllerSignals {
    /// Cook queue bytes increased vs prior interval.
    pub cook_queue_bytes_increased: bool,
    /// Cooker utilisation in ppm (0..=1_000_000).
    pub cooker_utilization_ppm: u32,
    /// Writer-ready queue bytes increased.
    pub writer_ready_increasing: bool,
    /// Ready queue fill ppm of limit.
    pub ready_queue_fill_ppm: u32,
    /// Last scale-up showed positive marginal throughput.
    pub positive_marginal_throughput: bool,
    /// Host/writer saturation blocks scale-up.
    pub writer_saturated: bool,
    /// Deadline miss attributed to cooking.
    pub deadline_miss_from_cook: bool,
}

/// Scale decision for one evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScaleDecision {
    /// No change.
    Hold,
    /// Increase active cookers by one.
    ScaleUp,
    /// Decrease active cookers by one.
    ScaleDown,
}

/// Stateful hysteresis controller.
#[derive(Debug, Clone)]
pub struct ScaleController {
    /// Policy knobs.
    pub policy: ControllerPolicy,
    /// Current active cookers.
    pub active_cookers: usize,
    up_streak: u32,
    down_streak: u32,
    last_scale_up_at_ns: Option<u64>,
    last_scale_down_at_ns: Option<u64>,
    last_eval_at_ns: Option<u64>,
}

impl ScaleController {
    /// Create with initial active count.
    pub fn new(policy: ControllerPolicy, initial_active: usize) -> Self {
        let active = initial_active.clamp(policy.minimum_active_cookers, policy.maximum_cookers);
        Self {
            policy,
            active_cookers: active,
            up_streak: 0,
            down_streak: 0,
            last_scale_up_at_ns: None,
            last_scale_down_at_ns: None,
            last_eval_at_ns: None,
        }
    }

    /// Evaluate once if `interval_ns` has elapsed; otherwise Hold without side effects.
    pub fn evaluate<C: AwoClock + ?Sized>(
        &mut self,
        signals: &ControllerSignals,
        clock: &C,
    ) -> ScaleDecision {
        let now = clock.now_ns();
        if let Some(last) = self.last_eval_at_ns {
            if now.saturating_sub(last) < self.policy.interval_ns {
                return ScaleDecision::Hold;
            }
        }
        self.last_eval_at_ns = Some(now);

        let want_up = signals.cook_queue_bytes_increased
            && signals.cooker_utilization_ppm >= self.policy.scale_up_utilization_ppm
            && !signals.writer_ready_increasing
            && signals.positive_marginal_throughput
            && !signals.writer_saturated
            && self.active_cookers < self.policy.maximum_cookers;

        let want_down = (signals.cooker_utilization_ppm <= self.policy.scale_down_utilization_ppm
            || signals.ready_queue_fill_ppm >= self.policy.ready_queue_scale_down_ppm)
            && !signals.deadline_miss_from_cook
            && self.active_cookers > self.policy.minimum_active_cookers;

        if want_up {
            self.up_streak = self.up_streak.saturating_add(1);
            self.down_streak = 0;
        } else if want_down {
            self.down_streak = self.down_streak.saturating_add(1);
            self.up_streak = 0;
        } else {
            self.up_streak = 0;
            self.down_streak = 0;
        }

        if self.up_streak >= self.policy.scale_up_consecutive {
            if let Some(t) = self.last_scale_up_at_ns {
                if now.saturating_sub(t) < self.policy.scale_up_dwell_ns {
                    return ScaleDecision::Hold;
                }
            }
            // Also respect scale-down dwell before flipping up? plan: min dwell after each.
            if let Some(t) = self.last_scale_down_at_ns {
                if now.saturating_sub(t) < self.policy.scale_down_dwell_ns {
                    return ScaleDecision::Hold;
                }
            }
            self.active_cookers += 1;
            self.up_streak = 0;
            self.last_scale_up_at_ns = Some(now);
            return ScaleDecision::ScaleUp;
        }

        if self.down_streak >= self.policy.scale_down_consecutive {
            if let Some(t) = self.last_scale_down_at_ns {
                if now.saturating_sub(t) < self.policy.scale_down_dwell_ns {
                    return ScaleDecision::Hold;
                }
            }
            if let Some(t) = self.last_scale_up_at_ns {
                if now.saturating_sub(t) < self.policy.scale_up_dwell_ns {
                    return ScaleDecision::Hold;
                }
            }
            self.active_cookers -= 1;
            self.down_streak = 0;
            self.last_scale_down_at_ns = Some(now);
            return ScaleDecision::ScaleDown;
        }

        ScaleDecision::Hold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> ControllerPolicy {
        ControllerPolicy {
            interval_ns: 100,
            scale_up_consecutive: 3,
            scale_down_consecutive: 3,
            scale_up_utilization_ppm: 800_000,
            scale_down_utilization_ppm: 300_000,
            ready_queue_scale_down_ppm: 750_000,
            scale_up_dwell_ns: 500,
            scale_down_dwell_ns: 2000,
            minimum_active_cookers: 1,
            maximum_cookers: 4,
        }
    }

    fn up_signals() -> ControllerSignals {
        ControllerSignals {
            cook_queue_bytes_increased: true,
            cooker_utilization_ppm: 900_000,
            writer_ready_increasing: false,
            ready_queue_fill_ppm: 100_000,
            positive_marginal_throughput: true,
            writer_saturated: false,
            deadline_miss_from_cook: false,
        }
    }

    #[test]
    fn scale_up_after_consecutive_intervals() {
        let clock = ManualClock::new(0);
        let mut c = ScaleController::new(policy(), 1);
        let sig = up_signals();
        assert_eq!(c.evaluate(&sig, &clock), ScaleDecision::Hold); // streak 1
        clock.advance(100);
        assert_eq!(c.evaluate(&sig, &clock), ScaleDecision::Hold); // 2
        clock.advance(100);
        assert_eq!(c.evaluate(&sig, &clock), ScaleDecision::ScaleUp); // 3
        assert_eq!(c.active_cookers, 2);
    }

    #[test]
    fn dwell_blocks_immediate_re_scale() {
        let clock = ManualClock::new(0);
        let mut c = ScaleController::new(policy(), 1);
        let sig = up_signals();
        for _ in 0..3 {
            let _ = c.evaluate(&sig, &clock);
            clock.advance(100);
        }
        // last eval advanced after ScaleUp; reset streaks by evaluating again soon
        // After ScaleUp, up_streak=0. Need 3 more — but dwell 500 after scale-up at ~200.
        // At time after third eval: started 0, after first last=0, advance 100 →1, advance 100→2, ScaleUp at 200.
        assert_eq!(c.active_cookers, 2);
        clock.advance(100); // 300 — need 500 dwell from 200 → still blocked at 300+ for consecutive
                            // Build streak again
        assert_eq!(c.evaluate(&sig, &clock), ScaleDecision::Hold);
        clock.advance(100);
        assert_eq!(c.evaluate(&sig, &clock), ScaleDecision::Hold);
        clock.advance(100);
        // streak 3 but dwell: now=500, last_scale_up=200, delta=300 < 500 → Hold
        assert_eq!(c.evaluate(&sig, &clock), ScaleDecision::Hold);
        clock.advance(200); // now 700, dwell ok, but streak was reset? Hold reset streaks when want_up still true increments
                            // After Hold with want_up, streak was already 3 then Hold due to dwell — looking at code:
                            // want_up true → up_streak += 1 each time. When up_streak >= 3 and dwell fails, return Hold WITHOUT resetting streak.
                            // So next evaluate with dwell ok should ScaleUp.
        assert_eq!(c.evaluate(&sig, &clock), ScaleDecision::ScaleUp);
        assert_eq!(c.active_cookers, 3);
    }

    #[test]
    fn writer_saturation_blocks_up() {
        let clock = ManualClock::new(0);
        let mut c = ScaleController::new(policy(), 1);
        let mut sig = up_signals();
        sig.writer_saturated = true;
        for _ in 0..5 {
            assert_eq!(c.evaluate(&sig, &clock), ScaleDecision::Hold);
            clock.advance(100);
        }
        assert_eq!(c.active_cookers, 1);
    }
}
