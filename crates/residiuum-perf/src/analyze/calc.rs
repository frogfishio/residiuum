//! Throughput retention, efficiency, amplification, scaling (SPEC §9).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetentionReport {
    pub layer_i: String,
    pub layer_im1: String,
    pub t_i: f64,
    pub t_im1: f64,
    /// R_i = T_i / T_{i-1}
    pub retention: Option<f64>,
    /// L_i = 1 - R_i
    pub loss: Option<f64>,
}

/// Throughput retention between matched layers.
pub fn throughput_retention(
    t_i: f64,
    t_im1: f64,
    layer_i: &str,
    layer_im1: &str,
) -> RetentionReport {
    let (retention, loss) = if t_im1 > 0.0 && t_i.is_finite() && t_im1.is_finite() {
        let r = t_i / t_im1;
        (Some(r), Some(1.0 - r))
    } else {
        (None, None)
    };
    RetentionReport {
        layer_i: layer_i.into(),
        layer_im1: layer_im1.into(),
        t_i,
        t_im1,
        retention,
        loss,
    }
}

/// End-to-end efficiency vs Residiuum-shaped I/O ceiling: E = T_L6 / T_L2.
pub fn efficiency(t_l6: f64, t_l2: f64) -> Option<f64> {
    if t_l2 > 0.0 && t_l6.is_finite() {
        Some(t_l6 / t_l2)
    } else {
        None
    }
}

/// Write amplification = filesystem_bytes / logical_ack_bytes.
pub fn amplification(fs_bytes: f64, logical_ack_bytes: f64) -> Option<f64> {
    if logical_ack_bytes > 0.0 && fs_bytes.is_finite() {
        Some(fs_bytes / logical_ack_bytes)
    } else {
        None
    }
}

/// Concurrency scaling S(n)=T(n)/T(1), efficiency P(n)=S(n)/n.
pub fn scaling_efficiency(t_n: f64, t_1: f64, n: f64) -> (Option<f64>, Option<f64>) {
    if t_1 > 0.0 && n > 0.0 {
        let s = t_n / t_1;
        (Some(s), Some(s / n))
    } else {
        (None, None)
    }
}

/// Residual fraction |e2e - stage_sum| / e2e.
pub fn latency_residual_fraction(e2e_ns: f64, stage_sum_ns: f64) -> Option<f64> {
    if e2e_ns > 0.0 {
        Some((e2e_ns - stage_sum_ns).abs() / e2e_ns)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_and_loss() {
        let r = throughput_retention(80.0, 100.0, "L4", "L3");
        assert!((r.retention.unwrap() - 0.8).abs() < 1e-9);
        assert!((r.loss.unwrap() - 0.2).abs() < 1e-9);
    }

    #[test]
    fn efficiency_and_amp() {
        assert!((efficiency(50.0, 100.0).unwrap() - 0.5).abs() < 1e-9);
        assert!((amplification(200.0, 100.0).unwrap() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn scaling() {
        let (s, p) = scaling_efficiency(300.0, 100.0, 4.0);
        assert!((s.unwrap() - 3.0).abs() < 1e-9);
        assert!((p.unwrap() - 0.75).abs() < 1e-9);
    }
}
