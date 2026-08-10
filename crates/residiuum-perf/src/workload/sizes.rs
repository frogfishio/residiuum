//! Fixed payload size series and threshold ±1 probes (SPEC §6.1).

/// Mandatory fixed-size series (bytes). Crosses inline/chunk/RPC boundaries.
pub const FIXED_PAYLOAD_SIZES: &[u64] = &[
    256,
    1024,             // 1 KiB
    4 * 1024,         // 4 KiB
    8 * 1024,         // 8 KiB
    16 * 1024,        // 16 KiB
    64 * 1024,        // 64 KiB
    256 * 1024,       // 256 KiB
    1024 * 1024,      // 1 MiB
    4 * 1024 * 1024,  // 4 MiB
    16 * 1024 * 1024, // 16 MiB
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThresholdProbe {
    pub threshold: u64,
    pub below: u64,
    pub at: u64,
    pub above: u64,
}

/// Return the fixed-size series as an owned vec (for manifests / tests).
pub fn fixed_size_series() -> Vec<u64> {
    FIXED_PAYLOAD_SIZES.to_vec()
}

/// Boundary probes for every active threshold: `t-1`, `t`, `t+1`.
/// `threshold == 0` is rejected (no below).
pub fn threshold_probes(thresholds: &[u64]) -> Vec<ThresholdProbe> {
    let mut out = Vec::with_capacity(thresholds.len());
    for &t in thresholds {
        if t == 0 {
            continue;
        }
        out.push(ThresholdProbe {
            threshold: t,
            below: t - 1,
            at: t,
            above: t.saturating_add(1),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_series_matches_spec() {
        assert_eq!(FIXED_PAYLOAD_SIZES.len(), 10);
        assert_eq!(FIXED_PAYLOAD_SIZES[0], 256);
        assert_eq!(FIXED_PAYLOAD_SIZES[9], 16 * 1024 * 1024);
        // Crosses common 4 KiB / 8 KiB boundaries.
        assert!(FIXED_PAYLOAD_SIZES.contains(&4096));
        assert!(FIXED_PAYLOAD_SIZES.contains(&8192));
    }

    #[test]
    fn threshold_probes_cover_minus_at_plus() {
        let p = threshold_probes(&[4096, 1_048_576]);
        assert_eq!(p.len(), 2);
        assert_eq!(
            p[0],
            ThresholdProbe {
                threshold: 4096,
                below: 4095,
                at: 4096,
                above: 4097,
            }
        );
        assert_eq!(p[1].below, 1_048_575);
        assert_eq!(p[1].above, 1_048_577);
    }
}
