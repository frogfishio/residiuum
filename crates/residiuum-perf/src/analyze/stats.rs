//! Repetition statistics and bootstrap confidence intervals.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SampleStats {
    pub n: usize,
    pub mean: f64,
    pub median: f64,
    pub min: f64,
    pub max: f64,
    pub mad: f64,
    /// Coefficient of variation (std/mean); None if mean≈0.
    pub cv: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BootstrapCi {
    pub low: f64,
    pub high: f64,
    pub level: f64,
    pub replicates: usize,
}

/// Mean, median, MAD for a sample (requires non-empty finite values).
pub fn mean_mad(samples: &[f64]) -> Option<SampleStats> {
    let mut v: Vec<f64> = samples.iter().copied().filter(|x| x.is_finite()).collect();
    if v.is_empty() {
        return None;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    let mean = v.iter().sum::<f64>() / n as f64;
    let median = if n % 2 == 1 {
        v[n / 2]
    } else {
        0.5 * (v[n / 2 - 1] + v[n / 2])
    };
    let mut devs: Vec<f64> = v.iter().map(|x| (x - median).abs()).collect();
    devs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mad = if n % 2 == 1 {
        devs[n / 2]
    } else {
        0.5 * (devs[n / 2 - 1] + devs[n / 2])
    };
    let var = v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n as f64;
    let std = var.sqrt();
    let cv = if mean.abs() > 1e-12 {
        Some(std / mean.abs())
    } else {
        None
    };
    Some(SampleStats {
        n,
        mean,
        median,
        min: v[0],
        max: v[n - 1],
        mad,
        cv,
    })
}

/// Deterministic bootstrap CI for the mean using a seed (no OS RNG).
pub fn bootstrap_ci_mean(
    samples: &[f64],
    replicates: usize,
    seed: u64,
    level: f64,
) -> Option<BootstrapCi> {
    let v: Vec<f64> = samples.iter().copied().filter(|x| x.is_finite()).collect();
    if v.is_empty() || replicates == 0 {
        return None;
    }
    let n = v.len();
    let mut means = Vec::with_capacity(replicates);
    let mut s = seed.wrapping_add(1);
    for _ in 0..replicates {
        let mut sum = 0.0;
        for _ in 0..n {
            s = s
                .wrapping_mul(0xBF58_476D_1CE4_E5B9)
                .wrapping_add(0x94D0_49BB_1331_11EB);
            let idx = (s as usize) % n;
            sum += v[idx];
        }
        means.push(sum / n as f64);
    }
    means.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let alpha = (1.0 - level).clamp(0.0, 1.0);
    let lo_i = ((alpha / 2.0) * (replicates as f64 - 1.0)).floor() as usize;
    let hi_i = ((1.0 - alpha / 2.0) * (replicates as f64 - 1.0)).ceil() as usize;
    let hi_i = hi_i.min(replicates - 1);
    Some(BootstrapCi {
        low: means[lo_i],
        high: means[hi_i],
        level,
        replicates,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mean_mad_constant() {
        let s = mean_mad(&[10.0, 10.0, 10.0, 10.0]).unwrap();
        assert_eq!(s.mean, 10.0);
        assert_eq!(s.mad, 0.0);
        assert_eq!(s.cv, Some(0.0));
    }

    #[test]
    fn bootstrap_stable() {
        let samples = [1.0, 2.0, 3.0, 4.0, 5.0];
        let a = bootstrap_ci_mean(&samples, 200, 42, 0.95).unwrap();
        let b = bootstrap_ci_mean(&samples, 200, 42, 0.95).unwrap();
        assert_eq!(a.low, b.low);
        assert_eq!(a.high, b.high);
        assert!(a.low <= a.high);
        assert!(a.low <= 3.0 && a.high >= 3.0);
    }
}
