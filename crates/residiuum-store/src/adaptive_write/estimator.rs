//! Workload EWMA estimator (plan §11.2, AWO-5).
//!
//! ```text
//! m' = m + (x - m) / 8
//! d' = d + (abs(x - m') - d) / 8
//! lower = max(0, m - 3d)
//! upper = m + 3d
//! ```
//!
//! First sample sets `m = x`, `d = 0`. Usable only after `min_samples` and before
//! monotonic staleness. No wall-clock sleeps — callers supply [`AwoClock`] time.

use super::controller::AwoClock;

/// Default α denominator from `policy-v1.json` (`estimator_alpha_denominator`).
pub const ALPHA_DENOMINATOR: u64 = 8;

/// Default deviation multiplier (`estimator_deviation_multiplier`).
pub const DEVIATION_MULTIPLIER: u64 = 3;

/// Default warm sample floor (`estimator_min_samples`).
pub const MIN_SAMPLES_DEFAULT: u32 = 32;

/// Default staleness (`estimator_stale_after_ns` = 30s).
pub const STALE_AFTER_NS_DEFAULT: u64 = 30_000_000_000;

/// Integer EWMA mean + absolute deviation envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EwmaEstimate {
    /// EWMA mean (nanoseconds).
    pub mean_ns: u64,
    /// EWMA absolute deviation (nanoseconds).
    pub deviation_ns: u64,
    /// Samples observed.
    pub samples: u32,
    /// Monotonic ns of last sample.
    pub last_sample_at_ns: u64,
}

impl EwmaEstimate {
    /// Cold empty estimator.
    pub fn cold() -> Self {
        Self {
            mean_ns: 0,
            deviation_ns: 0,
            samples: 0,
            last_sample_at_ns: 0,
        }
    }

    /// Observe sample `x` at monotonic time `now_ns`.
    pub fn observe(&mut self, x: u64, now_ns: u64) {
        if self.samples == 0 {
            self.mean_ns = x;
            self.deviation_ns = 0;
            self.samples = 1;
            self.last_sample_at_ns = now_ns;
            return;
        }
        // m' = m + (x - m) / 8  using signed intermediate for (x - m)
        let m = self.mean_ns as i128;
        let xv = x as i128;
        let m_prime = m + (xv - m) / (ALPHA_DENOMINATOR as i128);
        let m_prime_u = if m_prime < 0 {
            0u64
        } else {
            m_prime.min(u64::MAX as i128) as u64
        };
        let abs_dev = (xv - m_prime).unsigned_abs();
        let d = self.deviation_ns as i128;
        let d_prime = d + (abs_dev as i128 - d) / (ALPHA_DENOMINATOR as i128);
        let d_prime_u = if d_prime < 0 {
            0u64
        } else {
            d_prime.min(u64::MAX as i128) as u64
        };
        self.mean_ns = m_prime_u;
        self.deviation_ns = d_prime_u;
        self.samples = self.samples.saturating_add(1);
        self.last_sample_at_ns = now_ns;
    }

    /// Lower uncertainty bound `max(0, m - 3d)`.
    pub fn lower_ns(&self) -> u64 {
        let three_d = self.deviation_ns.saturating_mul(DEVIATION_MULTIPLIER);
        self.mean_ns.saturating_sub(three_d)
    }

    /// Upper uncertainty bound `m + 3d` (saturating).
    pub fn upper_ns(&self) -> u64 {
        let three_d = self.deviation_ns.saturating_mul(DEVIATION_MULTIPLIER);
        self.mean_ns.saturating_add(three_d)
    }

    /// Warm after `min_samples`.
    pub fn is_warm(&self, min_samples: u32) -> bool {
        self.samples >= min_samples
    }

    /// Stale when `now - last > stale_after`.
    pub fn is_stale(&self, now_ns: u64, stale_after_ns: u64) -> bool {
        if self.samples == 0 {
            return true;
        }
        now_ns.saturating_sub(self.last_sample_at_ns) > stale_after_ns
    }

    /// Usable for batch decisions: warm and not stale.
    pub fn is_usable(&self, now_ns: u64, min_samples: u32, stale_after_ns: u64) -> bool {
        self.is_warm(min_samples) && !self.is_stale(now_ns, stale_after_ns)
    }
}

/// Per-bucket natural service estimator (single bucket for AWO-5 floor).
#[derive(Debug, Clone)]
pub struct ServiceEstimator {
    /// Inner EWMA.
    pub ewma: EwmaEstimate,
    /// Warm sample floor.
    pub min_samples: u32,
    /// Staleness window (ns).
    pub stale_after_ns: u64,
}

impl ServiceEstimator {
    /// Defaults from `policy-v1.json`.
    pub fn with_policy_defaults() -> Self {
        Self {
            ewma: EwmaEstimate::cold(),
            min_samples: MIN_SAMPLES_DEFAULT,
            stale_after_ns: STALE_AFTER_NS_DEFAULT,
        }
    }

    /// Observe a service-time sample using `clock.now_ns()`.
    pub fn observe<C: AwoClock + ?Sized>(&mut self, x_ns: u64, clock: &C) {
        self.ewma.observe(x_ns, clock.now_ns());
    }

    /// Evidence flags for [`crate::adaptive_write::model::decide`].
    pub fn evidence_flags<C: AwoClock + ?Sized>(&self, clock: &C) -> (bool, bool) {
        let now = clock.now_ns();
        let warm = self.ewma.is_warm(self.min_samples);
        let stale = self.ewma.is_stale(now, self.stale_after_ns);
        (warm, stale)
    }
}

/// Payload size power-of-two bucket (plan §11.1).
///
/// `0` for empty/delete; otherwise `2^floor(log2(payload))` capped at `cap`.
pub fn payload_bucket(payload_bytes: u64, cap: u64) -> u64 {
    if payload_bytes == 0 {
        return 0;
    }
    let pot = 1u64 << (63 - payload_bytes.leading_zeros());
    // floor power of two of payload_bytes
    let floor = if pot > payload_bytes { pot >> 1 } else { pot };
    let floor = floor.max(1);
    if cap == 0 {
        floor
    } else {
        floor.min(cap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adaptive_write::controller::ManualClock;

    #[test]
    fn first_sample_sets_mean() {
        let mut e = EwmaEstimate::cold();
        e.observe(1000, 10);
        assert_eq!(e.mean_ns, 1000);
        assert_eq!(e.deviation_ns, 0);
        assert_eq!(e.samples, 1);
        assert_eq!(e.lower_ns(), 1000);
        assert_eq!(e.upper_ns(), 1000);
    }

    #[test]
    fn ewma_moves_toward_sample() {
        let mut e = EwmaEstimate::cold();
        e.observe(800, 0);
        e.observe(1600, 1);
        // m' = 800 + (1600-800)/8 = 800 + 100 = 900
        assert_eq!(e.mean_ns, 900);
        assert!(e.deviation_ns > 0);
    }

    #[test]
    fn warm_and_stale() {
        let mut e = EwmaEstimate::cold();
        let clock = ManualClock::new(0);
        for i in 0..32 {
            e.observe(100, clock.now_ns() + i);
        }
        assert!(e.is_warm(32));
        assert!(!e.is_stale(31, 30));
        assert!(e.is_stale(31 + 31, 30));
    }

    #[test]
    fn payload_bucket_pow2() {
        assert_eq!(payload_bucket(0, 1024), 0);
        assert_eq!(payload_bucket(1, 1024), 1);
        assert_eq!(payload_bucket(3, 1024), 2);
        assert_eq!(payload_bucket(8, 1024), 8);
        assert_eq!(payload_bucket(9, 1024), 8);
        assert_eq!(payload_bucket(10_000, 1024), 1024);
    }
}
