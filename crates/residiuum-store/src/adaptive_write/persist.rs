//! AWO-1 persist-before-publish scaffolding + AWO-2 reservation types.
//!
//! Product batch paths in [`crate::store::Store`]:
//! - single-shard `put_many` / parallel cook: stage → tail write → publish
//! - multi-shard `put_many_parallel`: per-shard checkpoint + tail; **all-or-nothing**
//!   index publish only after every shard succeeds; short-write sets
//!   `awo_writer_poisoned`
//!
//! Persistent cookers + credit ledger live in sibling modules (AWO-2). Full
//! coordinator as sole mutation authority is AWO-3.

use residiuum_format::ActiveSegmentCheckpoint;

/// Lane ticket identity for ordered install (filled by coordinator in AWO-2/3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LaneTicket {
    /// Monotonic ticket within a lane.
    pub ticket: u64,
}

/// Reservation snapshot for one ordered batch (plan §9).
#[derive(Debug, Clone)]
pub struct BatchReservation {
    /// Process-local lease identity.
    pub lease_id: u64,
    /// Active segment id at reservation time.
    pub segment_id: [u8; 16],
    /// Writer generation / open generation (store-defined).
    pub segment_generation: u64,
    /// First writer sequence assigned to the batch.
    pub first_writer_sequence: u64,
    /// Logical start offset of the first frame.
    pub start_offset: u64,
    /// Frame count before reservation.
    pub frame_count_before: u64,
    /// Exact active-segment checkpoint for pre-I/O restore.
    pub segment_checkpoint: ActiveSegmentCheckpoint,
}

/// Failpoint names closed for AWO-1 crash matrix (implementation plan §15).
pub const AWO_FAILPOINTS: &[&str] = &[
    "awo.reserve.after",
    "awo.cook.before",
    "awo.cook.after",
    "awo.install.frame.before",
    "awo.install.frame.after",
    "awo.persist.before",
    "awo.persist.after_write",
    "awo.persist.after_sync",
    "awo.publish.before",
    "awo.publish.after",
    "awo.complete.before",
];
