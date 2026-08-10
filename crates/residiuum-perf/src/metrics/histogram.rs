//! Fixed-bucket HDR-compatible latency histogram (nanoseconds).

use serde::{Deserialize, Serialize};

/// Number of power-of-two buckets: covers 2^0 .. 2^63 ns (≈584 years).
pub const HISTOGRAM_BUCKETS: usize = 64;

/// Latency histogram: bucket `i` counts samples in `[2^i, 2^{i+1})` ns
/// (bucket 0 is `[0, 2)` so 0–1 ns land together).
///
/// HDR-compatible in the sense of fixed log-spaced buckets, O(1) record,
/// mergeable, no per-sample heap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyHistogram {
    pub schema: String,
    pub unit: String,
    /// Length always [`HISTOGRAM_BUCKETS`] (Vec for serde; fixed capacity).
    pub buckets: Vec<u64>,
    pub total_count: u64,
    pub min_ns: u64,
    pub max_ns: u64,
    /// Saturating sum for mean (may saturate at u128::MAX conceptually via u64 cap flag).
    #[serde(
        serialize_with = "ser_u128_as_str",
        deserialize_with = "de_u128_from_str"
    )]
    pub sum_ns: u128,
    /// True when a counter bucket hit u64::MAX (overflow).
    pub saturated: bool,
}

fn ser_u128_as_str<S: serde::Serializer>(v: &u128, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&v.to_string())
}

fn de_u128_from_str<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u128, D::Error> {
    let s = String::deserialize(d)?;
    s.parse().map_err(serde::de::Error::custom)
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    pub fn new() -> Self {
        Self {
            schema: "residiuum-pqh3-latency-histogram-v1".into(),
            unit: "ns".into(),
            buckets: vec![0; HISTOGRAM_BUCKETS],
            total_count: 0,
            min_ns: u64::MAX,
            max_ns: 0,
            sum_ns: 0,
            saturated: false,
        }
    }

    /// Bucket index for a latency in nanoseconds.
    pub fn bucket_index(ns: u64) -> usize {
        if ns <= 1 {
            return 0;
        }
        // floor(log2(ns)) — bucket i covers [2^i, 2^{i+1})
        let z = 63 - ns.leading_zeros();
        (z as usize).min(HISTOGRAM_BUCKETS - 1)
    }

    /// Lower bound (inclusive) of bucket `i` in ns.
    pub fn bucket_lo(i: usize) -> u64 {
        if i == 0 {
            0
        } else {
            1u64 << i
        }
    }

    pub fn record(&mut self, ns: u64) {
        let idx = Self::bucket_index(ns);
        let b = &mut self.buckets[idx];
        if *b == u64::MAX {
            self.saturated = true;
        } else {
            *b = b.saturating_add(1);
        }
        if self.total_count == u64::MAX {
            self.saturated = true;
        } else {
            self.total_count = self.total_count.saturating_add(1);
        }
        self.min_ns = self.min_ns.min(ns);
        self.max_ns = self.max_ns.max(ns);
        self.sum_ns = self.sum_ns.saturating_add(u128::from(ns));
    }

    pub fn merge_from(&mut self, other: &Self) {
        for i in 0..HISTOGRAM_BUCKETS {
            let (sum, overflow) = self.buckets[i].overflowing_add(other.buckets[i]);
            self.buckets[i] = sum;
            if overflow {
                self.buckets[i] = u64::MAX;
                self.saturated = true;
            }
        }
        let (tc, ov) = self.total_count.overflowing_add(other.total_count);
        self.total_count = if ov {
            self.saturated = true;
            u64::MAX
        } else {
            tc
        };
        if other.total_count > 0 {
            self.min_ns = self.min_ns.min(other.min_ns);
            self.max_ns = self.max_ns.max(other.max_ns);
        }
        self.sum_ns = self.sum_ns.saturating_add(other.sum_ns);
        self.saturated |= other.saturated;
    }

    /// Approximate percentile in ns using bucket upper bound as representative.
    ///
    /// Golden-vector mode: for values that land in the same bucket the estimate
    /// is the bucket's upper bound (HDR-style). Exact when all samples share a
    /// value that is a power of two (bucket lower edge for single-value tests
    /// we use the recorded min when total_count==1 or all in one bucket with
    /// min==max).
    pub fn percentile_ns(&self, p: f64) -> Option<u64> {
        if self.total_count == 0 || !(0.0..=100.0).contains(&p) {
            return None;
        }
        if self.min_ns == self.max_ns {
            return Some(self.min_ns);
        }
        let target = percentile_rank(self.total_count, p);
        let mut cum = 0u64;
        for i in 0..HISTOGRAM_BUCKETS {
            cum = cum.saturating_add(self.buckets[i]);
            if cum >= target {
                // Representative: geometric mean-ish → use high edge for conservative p99.
                let hi = if i + 1 >= HISTOGRAM_BUCKETS {
                    u64::MAX
                } else {
                    (1u64 << (i + 1)).saturating_sub(1)
                };
                // Clamp into observed range.
                return Some(hi.clamp(self.min_ns, self.max_ns));
            }
        }
        Some(self.max_ns)
    }

    pub fn percentiles(&self) -> Percentiles {
        Percentiles {
            p50_ns: self.percentile_ns(50.0),
            p90_ns: self.percentile_ns(90.0),
            p95_ns: self.percentile_ns(95.0),
            p99_ns: self.percentile_ns(99.0),
            p999_ns: self.percentile_ns(99.9),
            max_ns: if self.total_count == 0 {
                None
            } else {
                Some(self.max_ns)
            },
            min_ns: if self.total_count == 0 {
                None
            } else {
                Some(self.min_ns)
            },
            count: self.total_count,
            saturated: self.saturated,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Percentiles {
    pub p50_ns: Option<u64>,
    pub p90_ns: Option<u64>,
    pub p95_ns: Option<u64>,
    pub p99_ns: Option<u64>,
    pub p999_ns: Option<u64>,
    pub max_ns: Option<u64>,
    pub min_ns: Option<u64>,
    pub count: u64,
    pub saturated: bool,
}

/// Nearest-rank: ceil(p/100 * N), at least 1 when N>0.
fn percentile_rank(n: u64, p: f64) -> u64 {
    if n == 0 {
        return 0;
    }
    let r = (p / 100.0 * n as f64).ceil() as u64;
    r.clamp(1, n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_index_log_spaced() {
        assert_eq!(LatencyHistogram::bucket_index(0), 0);
        assert_eq!(LatencyHistogram::bucket_index(1), 0);
        assert_eq!(LatencyHistogram::bucket_index(2), 1);
        assert_eq!(LatencyHistogram::bucket_index(3), 1);
        assert_eq!(LatencyHistogram::bucket_index(4), 2);
        assert_eq!(LatencyHistogram::bucket_index(1024), 10);
    }

    #[test]
    fn constant_series_exact_percentiles() {
        let mut h = LatencyHistogram::new();
        for _ in 0..1000 {
            h.record(1000);
        }
        let p = h.percentiles();
        assert_eq!(p.p50_ns, Some(1000));
        assert_eq!(p.p99_ns, Some(1000));
        assert_eq!(p.max_ns, Some(1000));
        assert_eq!(p.count, 1000);
    }

    #[test]
    fn golden_two_point_distribution() {
        // 90% @ 100ns, 10% @ 10_000ns
        let mut h = LatencyHistogram::new();
        for _ in 0..900 {
            h.record(100);
        }
        for _ in 0..100 {
            h.record(10_000);
        }
        let p50 = h.percentile_ns(50.0).unwrap();
        let p99 = h.percentile_ns(99.0).unwrap();
        // p50 in the low cluster
        assert!(p50 <= 200, "p50={p50}");
        // p99 in the high cluster
        assert!(p99 >= 10_000, "p99={p99}");
    }

    #[test]
    fn merge_preserves_counts() {
        let mut a = LatencyHistogram::new();
        let mut b = LatencyHistogram::new();
        for i in 0..50u64 {
            a.record(i + 1);
            b.record(i + 100);
        }
        a.merge_from(&b);
        assert_eq!(a.total_count, 100);
    }

    #[test]
    fn saturation_flag() {
        let mut h = LatencyHistogram::new();
        h.buckets[0] = u64::MAX;
        h.record(0);
        assert!(h.saturated);
    }
}
