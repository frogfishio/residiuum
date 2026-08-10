//! Canonical size distributions (SPEC §6.2).

use serde::{Deserialize, Serialize};

/// Registered distribution identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DistributionId {
    Tiny,
    Document,
    MixedLarge,
    RewriteHeavy,
    AppendHistory,
    ChunkBoundary,
    /// Explicit size list from a user-supplied manifest (sizes only).
    UserSizes,
    /// Pure fixed-size series (one size repeated).
    Fixed,
}

impl DistributionId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tiny => "tiny",
            Self::Document => "document",
            Self::MixedLarge => "mixed-large",
            Self::RewriteHeavy => "rewrite-heavy",
            Self::AppendHistory => "append-history",
            Self::ChunkBoundary => "chunk-boundary",
            Self::UserSizes => "user-sizes",
            Self::Fixed => "fixed",
        }
    }

    pub fn all_canonical() -> &'static [DistributionId] {
        &[
            Self::Tiny,
            Self::Document,
            Self::MixedLarge,
            Self::RewriteHeavy,
            Self::AppendHistory,
            Self::ChunkBoundary,
        ]
    }
}

/// Deterministic size sampler driven by a counter (not OS RNG).
#[derive(Debug, Clone)]
pub struct SizeSampler {
    dist: DistributionId,
    fixed_size: u64,
    /// Chunk threshold for `ChunkBoundary` (default 1 MiB).
    chunk_threshold: u64,
    /// Optional user size table.
    user_sizes: Vec<u64>,
}

impl SizeSampler {
    pub fn fixed(size: u64) -> Self {
        Self {
            dist: DistributionId::Fixed,
            fixed_size: size,
            chunk_threshold: 1024 * 1024,
            user_sizes: Vec::new(),
        }
    }

    pub fn distribution(dist: DistributionId) -> Self {
        Self {
            dist,
            fixed_size: 4096,
            chunk_threshold: 1024 * 1024,
            user_sizes: Vec::new(),
        }
    }

    pub fn with_chunk_threshold(mut self, t: u64) -> Self {
        self.chunk_threshold = t.max(2);
        self
    }

    pub fn with_user_sizes(mut self, sizes: Vec<u64>) -> Self {
        self.user_sizes = sizes;
        self.dist = DistributionId::UserSizes;
        self
    }

    pub fn distribution_id(&self) -> DistributionId {
        self.dist
    }

    /// Size for logical operation counter `i` (0-based). Pure function of `i`.
    pub fn size_at(&self, i: u64) -> u64 {
        match self.dist {
            DistributionId::Fixed => self.fixed_size,
            DistributionId::UserSizes => {
                if self.user_sizes.is_empty() {
                    return self.fixed_size;
                }
                self.user_sizes[(i as usize) % self.user_sizes.len()]
            }
            DistributionId::Tiny => {
                // 90% 256 B, 9% 4 KiB, 1% 64 KiB — via counter mod 100.
                match i % 100 {
                    0..=89 => 256,
                    90..=98 => 4 * 1024,
                    _ => 64 * 1024,
                }
            }
            DistributionId::MixedLarge => {
                // 70% 4 KiB, 20% 64 KiB, 9% 1 MiB, 1% 16 MiB.
                match i % 100 {
                    0..=69 => 4 * 1024,
                    70..=89 => 64 * 1024,
                    90..=98 => 1024 * 1024,
                    _ => 16 * 1024 * 1024,
                }
            }
            DistributionId::Document => {
                // Log-normal-ish discrete ladder: median 4 KiB, p95 ~64 KiB, cap 1 MiB.
                // Use a fixed 32-bucket histogram approximating the shape.
                const BUCKETS: [u64; 32] = [
                    512,
                    1024,
                    1024,
                    2048,
                    2048,
                    4096,
                    4096,
                    4096,
                    4096,
                    4096,
                    4096,
                    4096,
                    4096,
                    4096,
                    8192,
                    8192,
                    8192,
                    16384,
                    16384,
                    16384,
                    32768,
                    32768,
                    65536,
                    65536,
                    65536,
                    131072,
                    131072,
                    262144,
                    262144,
                    524288,
                    524288,
                    1024 * 1024,
                ];
                BUCKETS[(i as usize) % BUCKETS.len()]
            }
            DistributionId::RewriteHeavy | DistributionId::AppendHistory => {
                // Stable sizes around 4 KiB for key-set / history focus.
                4 * 1024
            }
            DistributionId::ChunkBoundary => {
                // Concentrate around chunk threshold: t-1, t, t+1 cycling.
                let t = self.chunk_threshold;
                match i % 3 {
                    0 => t - 1,
                    1 => t,
                    _ => t + 1,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn tiny_hits_all_three_sizes() {
        let s = SizeSampler::distribution(DistributionId::Tiny);
        let mut seen = HashSet::new();
        for i in 0..200 {
            seen.insert(s.size_at(i));
        }
        assert!(seen.contains(&256));
        assert!(seen.contains(&(4 * 1024)));
        assert!(seen.contains(&(64 * 1024)));
    }

    #[test]
    fn mixed_large_hits_boundaries() {
        let s = SizeSampler::distribution(DistributionId::MixedLarge);
        let mut seen = HashSet::new();
        for i in 0..200 {
            seen.insert(s.size_at(i));
        }
        assert!(seen.contains(&(16 * 1024 * 1024)));
        assert!(seen.contains(&(1024 * 1024)));
    }

    #[test]
    fn chunk_boundary_hits_threshold_minus_at_plus() {
        let s = SizeSampler::distribution(DistributionId::ChunkBoundary)
            .with_chunk_threshold(1_048_576);
        let mut seen = HashSet::new();
        for i in 0..9 {
            seen.insert(s.size_at(i));
        }
        assert_eq!(seen.len(), 3);
        assert!(seen.contains(&1_048_575));
        assert!(seen.contains(&1_048_576));
        assert!(seen.contains(&1_048_577));
    }

    #[test]
    fn size_at_is_pure() {
        let s = SizeSampler::distribution(DistributionId::Document);
        assert_eq!(s.size_at(17), s.size_at(17));
    }
}
