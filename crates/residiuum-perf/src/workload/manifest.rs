//! Workload manifest: closed config → op stream + digest oracle.

use super::digest::{digest_stream, stream_stats, StreamDigest, StreamStats};
use super::distribution::{DistributionId, SizeSampler};
use super::generator::PayloadProfile;
use super::op::{LogicalOp, OpKind};
use super::scheduler::ScheduleConfig;
use super::WorkloadError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// How insert / rewrite / history ops are mixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadMode {
    /// All inserts, unique keys.
    InsertOnly,
    /// Insert then geometric rewrites over a stable key set.
    RewriteHeavy,
    /// Unique keys with repeated history generations.
    AppendHistory,
    /// Mixed: insert stream with occasional rewrites (every Nth op).
    Mixed,
}

impl WorkloadMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InsertOnly => "insert_only",
            Self::RewriteHeavy => "rewrite_heavy",
            Self::AppendHistory => "append_history",
            Self::Mixed => "mixed",
        }
    }
}

/// Complete workload configuration (no payloads).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadManifest {
    pub schema: String,
    pub workload_id: String,
    pub seed: u64,
    pub op_count: u64,
    pub mode: WorkloadMode,
    pub distribution: DistributionId,
    /// Used when `distribution == Fixed`.
    #[serde(default = "default_fixed_size")]
    pub fixed_size: u64,
    #[serde(default = "default_chunk_threshold")]
    pub chunk_threshold: u64,
    /// Key-space size for rewrite-heavy (distinct keys).
    #[serde(default = "default_key_space")]
    pub key_space: u64,
    pub payload_profile: PayloadProfile,
    pub schedule: ScheduleConfig,
    /// Optional explicit sizes for `UserSizes` distribution.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub user_sizes: Vec<u64>,
}

fn default_fixed_size() -> u64 {
    4096
}
fn default_chunk_threshold() -> u64 {
    1024 * 1024
}
fn default_key_space() -> u64 {
    1024
}

impl WorkloadManifest {
    pub fn fixed_size_insert(
        workload_id: impl Into<String>,
        seed: u64,
        size: u64,
        op_count: u64,
    ) -> Self {
        Self {
            schema: "residiuum-pqh2-workload-manifest-v1".into(),
            workload_id: workload_id.into(),
            seed,
            op_count,
            mode: WorkloadMode::InsertOnly,
            distribution: DistributionId::Fixed,
            fixed_size: size,
            chunk_threshold: default_chunk_threshold(),
            key_space: op_count.max(1),
            payload_profile: PayloadProfile::Incompressible,
            schedule: ScheduleConfig::default(),
            user_sizes: Vec::new(),
        }
    }

    pub fn config_hash(&self) -> String {
        let raw = serde_json::to_vec(self).unwrap_or_default();
        let mut h = Sha256::new();
        h.update(b"pqh2-manifest\0");
        h.update(&raw);
        hex::encode(h.finalize())
    }

    fn sampler(&self) -> SizeSampler {
        match self.distribution {
            DistributionId::Fixed => SizeSampler::fixed(self.fixed_size),
            DistributionId::UserSizes => SizeSampler::distribution(DistributionId::UserSizes)
                .with_user_sizes(self.user_sizes.clone()),
            other => SizeSampler::distribution(other).with_chunk_threshold(self.chunk_threshold),
        }
    }

    /// Build the full logical op plan (descriptors only — no payload bodies).
    pub fn build_ops(&self) -> Result<Vec<LogicalOp>, WorkloadError> {
        self.schedule.validate().map_err(WorkloadError::Msg)?;
        if self.op_count == 0 {
            return Ok(Vec::new());
        }
        let sampler = self.sampler();
        let key_space = self.key_space.max(1);
        let mut ops = Vec::with_capacity(self.op_count as usize);
        // Track generation per key for rewrite/history modes.
        let mut gens: Vec<u32> = vec![0; key_space.min(self.op_count).max(1) as usize];
        // For large key_space, use modular counters instead of a huge vec.
        let use_mod = key_space > 1_000_000 || key_space as usize != gens.len();

        for seq in 0..self.op_count {
            let payload_len = sampler.size_at(seq);
            let (kind, key_index, generation) = match self.mode {
                WorkloadMode::InsertOnly => (OpKind::Insert, seq, 0u32),
                WorkloadMode::RewriteHeavy => {
                    // First key_space ops insert; then rewrites with geometric-ish reuse.
                    if seq < key_space {
                        (OpKind::Insert, seq, 0)
                    } else {
                        // Prefer lower key indices more often (geometric-ish via bit mix).
                        let ki = mix_key(seq, key_space);
                        let gen = if use_mod {
                            // approximate generation from how many times we've seen this key
                            ((seq - key_space) / key_space + 1) as u32
                        } else {
                            let slot = ki as usize;
                            gens[slot] = gens[slot].saturating_add(1);
                            gens[slot]
                        };
                        (OpKind::Rewrite, ki, gen)
                    }
                }
                WorkloadMode::AppendHistory => {
                    // Unique keys for first pass; then history_append generations.
                    if seq < key_space {
                        (OpKind::Insert, seq, 0)
                    } else {
                        let ki = (seq - key_space) % key_space;
                        let gen = ((seq - key_space) / key_space + 1) as u32;
                        (OpKind::HistoryAppend, ki, gen)
                    }
                }
                WorkloadMode::Mixed => {
                    if seq % 5 == 4 && seq > 0 {
                        let ki = (seq / 5) % key_space;
                        (OpKind::Rewrite, ki, 1)
                    } else {
                        (OpKind::Insert, seq, 0)
                    }
                }
            };
            ops.push(LogicalOp {
                seq,
                kind,
                key_index,
                generation,
                payload_len,
                producer_id: 0,
            });
        }
        Ok(ops)
    }

    /// Stream ops one at a time without retaining the full plan (for large N).
    pub fn for_each_op<F>(&self, mut f: F) -> Result<(), WorkloadError>
    where
        F: FnMut(&LogicalOp) -> Result<(), WorkloadError>,
    {
        // Reuse build for modest counts; for large counts generate on the fly.
        if self.op_count <= 100_000 {
            for op in self.build_ops()? {
                f(&op)?;
            }
            return Ok(());
        }
        // Streaming path: same logic as build_ops without Vec retention of all.
        self.schedule.validate().map_err(WorkloadError::Msg)?;
        let sampler = self.sampler();
        let key_space = self.key_space.max(1);
        for seq in 0..self.op_count {
            let payload_len = sampler.size_at(seq);
            let (kind, key_index, generation) = match self.mode {
                WorkloadMode::InsertOnly => (OpKind::Insert, seq, 0u32),
                WorkloadMode::RewriteHeavy => {
                    if seq < key_space {
                        (OpKind::Insert, seq, 0)
                    } else {
                        let ki = mix_key(seq, key_space);
                        let gen = ((seq - key_space) / key_space + 1) as u32;
                        (OpKind::Rewrite, ki, gen)
                    }
                }
                WorkloadMode::AppendHistory => {
                    if seq < key_space {
                        (OpKind::Insert, seq, 0)
                    } else {
                        let ki = (seq - key_space) % key_space;
                        let gen = ((seq - key_space) / key_space + 1) as u32;
                        (OpKind::HistoryAppend, ki, gen)
                    }
                }
                WorkloadMode::Mixed => {
                    if seq % 5 == 4 && seq > 0 {
                        let ki = (seq / 5) % key_space;
                        (OpKind::Rewrite, ki, 1)
                    } else {
                        (OpKind::Insert, seq, 0)
                    }
                }
            };
            let op = LogicalOp {
                seq,
                kind,
                key_index,
                generation,
                payload_len,
                producer_id: 0,
            };
            f(&op)?;
        }
        Ok(())
    }

    pub fn expected_digest(&self) -> Result<StreamDigest, WorkloadError> {
        let ops = self.build_ops()?;
        Ok(digest_stream(self.seed, self.payload_profile, &ops))
    }

    pub fn stats(&self) -> Result<StreamStats, WorkloadError> {
        let ops = self.build_ops()?;
        Ok(stream_stats(&ops))
    }

    /// Oracle package: digest + stats + config hash (safe for result artifacts).
    pub fn oracle(&self) -> Result<WorkloadOracle, WorkloadError> {
        let ops = self.build_ops()?;
        Ok(WorkloadOracle {
            schema: "residiuum-pqh2-oracle-v1".into(),
            workload_id: self.workload_id.clone(),
            config_hash: self.config_hash(),
            digest: digest_stream(self.seed, self.payload_profile, &ops),
            stats: stream_stats(&ops),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadOracle {
    pub schema: String,
    pub workload_id: String,
    pub config_hash: String,
    pub digest: StreamDigest,
    pub stats: StreamStats,
}

/// Prefer lower keys more often (stable geometric-ish).
fn mix_key(seq: u64, key_space: u64) -> u64 {
    // Splitmix64-style avalanche then mod — deterministic, portable.
    let mut z = seq
        .wrapping_add(0x9E37_79B9_7F4A_7C15)
        .wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 30)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    z % key_space
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::scheduler::{merge_partitions, partition_ops};

    #[test]
    fn same_seed_same_digest() {
        let m = WorkloadManifest::fixed_size_insert("w1", 0xC0FFEE, 4096, 50);
        let a = m.expected_digest().unwrap();
        let b = m.expected_digest().unwrap();
        assert_eq!(a.stream_digest_hex, b.stream_digest_hex);
        assert_eq!(a.logical_payload_bytes, 50 * 4096);
    }

    #[test]
    fn partition_does_not_change_digest() {
        let m = WorkloadManifest::fixed_size_insert("w2", 7, 256, 64);
        let ops = m.build_ops().unwrap();
        let d1 = digest_stream(m.seed, m.payload_profile, &ops);
        let parts = partition_ops(&ops, 8);
        let merged = merge_partitions(&parts);
        // Clear producer_id for digest equality of logical content.
        let cleaned: Vec<_> = merged
            .into_iter()
            .map(|mut o| {
                o.producer_id = 0;
                o
            })
            .collect();
        let d2 = digest_stream(m.seed, m.payload_profile, &cleaned);
        assert_eq!(d1.stream_digest_hex, d2.stream_digest_hex);
    }

    #[test]
    fn rewrite_heavy_emits_rewrites() {
        let mut m = WorkloadManifest::fixed_size_insert("rw", 1, 1024, 40);
        m.mode = WorkloadMode::RewriteHeavy;
        m.key_space = 10;
        let d = m.expected_digest().unwrap();
        assert!(d.rewrite_count > 0);
        assert!(d.insert_count >= 10);
    }

    #[test]
    fn oracle_has_no_payload_fields() {
        let m = WorkloadManifest::fixed_size_insert("o", 3, 128, 10);
        let o = m.oracle().unwrap();
        let json = serde_json::to_string(&o).unwrap();
        assert!(!json.contains("\"payload\":"));
        assert!(!json.contains("\"key_bytes\":"));
        assert!(json.contains("stream_digest_hex"));
    }

    #[test]
    fn distribution_tiny_stable() {
        let mut m = WorkloadManifest::fixed_size_insert("t", 11, 0, 100);
        m.distribution = DistributionId::Tiny;
        let a = m.expected_digest().unwrap();
        let b = m.expected_digest().unwrap();
        assert_eq!(a, b);
        // Boundaries hit in sizes.
        let ops = m.build_ops().unwrap();
        let sizes: std::collections::HashSet<_> = ops.iter().map(|o| o.payload_len).collect();
        assert!(sizes.contains(&256));
        assert!(sizes.contains(&(4 * 1024)));
    }

    #[test]
    fn for_each_matches_build() {
        let m = WorkloadManifest::fixed_size_insert("fe", 5, 64, 20);
        let built = m.build_ops().unwrap();
        let mut streamed = Vec::new();
        m.for_each_op(|op| {
            streamed.push(op.clone());
            Ok(())
        })
        .unwrap();
        assert_eq!(built, streamed);
    }
}
