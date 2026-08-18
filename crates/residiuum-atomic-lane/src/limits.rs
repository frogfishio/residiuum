//! Frozen lane ceilings (CR-R2-003 / CR-ATMR4-007). Applied before allocation.

use residiuum_atomics::{ChunkLimits, ResourceLimits};

/// Maximum writer shards a lane may create or reopen.
pub(crate) const MAX_SHARDS: u32 = 64;

/// Hard member-count ceiling (same freeze as LocalHeap generated members).
pub(crate) const fn max_intent_members() -> u32 {
    ResourceLimits::hard_local_heap().total_generated_members
}

/// Hard encoded-member-record ceiling (same freeze as Key-scope plan bytes).
pub(crate) const fn max_encoded_member_bytes() -> u32 {
    ResourceLimits::hard_key().canonical_plan_bytes
}

/// Hard payload-file ceiling (same freeze as chunk reassembly).
pub(crate) const fn max_payload_bytes() -> u32 {
    ChunkLimits::hard().max_assembled_bytes
}

/// Durable sidecar roles that recovery must size-check before `read`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SidecarRole {
    Plan,
    Intent,
    Payload,
    Checkpoint,
    Seal,
    ChunkManifest,
    ChunkBody,
    Ack,
}

impl SidecarRole {
    /// Maximum encoded bytes that may be allocated for this role.
    pub(crate) const fn max_bytes(self) -> u64 {
        match self {
            Self::Plan => 8 + 32 + ResourceLimits::hard_local_heap().canonical_plan_bytes as u64,
            Self::Intent => 4 + (max_intent_members() as u64) * (4 + 2048),
            Self::Payload => max_payload_bytes() as u64,
            Self::Checkpoint => 23 + 64 * 8 + 4096 * 101,
            Self::Seal => 156,
            Self::ChunkManifest => 4 + (ChunkLimits::hard().max_chunks as u64) * 32,
            Self::ChunkBody => ChunkLimits::hard().max_chunk_bytes as u64,
            Self::Ack => 8,
        }
    }
}

/// Ceilings for one reopen (CR-ATMR3-004). Applied before allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryLimits {
    /// Maximum coordinator or shard log bytes that may be read.
    pub max_log_bytes: u64,
    /// Maximum Atomics reconstructed into the live kernel.
    pub max_atomics: u32,
    /// Maximum staged members reconstructed into the live kernel.
    pub max_members: u32,
    /// Maximum directory entries visited (seals, plans).
    pub max_dirents: u32,
    /// Maximum sidecar bytes charged to one reopen (CR-ATMR4-007).
    pub max_sidecar_bytes: u64,
}

impl RecoveryLimits {
    /// Prototype diagnostic ceilings. Independent of total media size.
    pub const fn prototype() -> Self {
        Self {
            max_log_bytes: 16 * 1024 * 1024,
            max_atomics: 4096,
            max_members: 4096,
            max_dirents: 8192,
            max_sidecar_bytes: 16 * 1024 * 1024,
        }
    }
}

impl Default for RecoveryLimits {
    fn default() -> Self {
        Self::prototype()
    }
}