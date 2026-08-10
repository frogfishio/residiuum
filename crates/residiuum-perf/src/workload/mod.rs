//! PQH-2: deterministic workload engine.
//!
//! Counter-based generators, fixed-size / distribution streams, payload
//! profiles, scheduling dimensions, and a digest oracle that never stores
//! full payloads in results.

mod digest;
mod distribution;
mod generator;
mod manifest;
mod op;
mod scheduler;
mod sizes;

pub use digest::{
    digest_stream, result_must_not_contain_payloads, stream_stats, StreamDigest, StreamStats,
};
pub use distribution::{DistributionId, SizeSampler};
pub use generator::{
    fill_payload, fill_payload_range, generate_key, payload_digest, PayloadProfile,
};
pub use manifest::{WorkloadManifest, WorkloadMode, WorkloadOracle};
pub use op::{LogicalOp, OpCursor, OpKind};
pub use scheduler::{
    batch_groups, merge_partitions, partition_ops, ProducerPartition, ScheduleConfig,
};
pub use sizes::{fixed_size_series, threshold_probes, ThresholdProbe, FIXED_PAYLOAD_SIZES};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum WorkloadError {
    #[error("workload: {0}")]
    Msg(String),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}
