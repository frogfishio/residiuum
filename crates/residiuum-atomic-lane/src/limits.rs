//! Frozen lane ceilings (CR-R2-003). Applied before allocation or path scans.

use residiuum_atomics::{ChunkLimits, ResourceLimits};

/// Maximum writer shards a lane may create or reopen.
pub(crate) const MAX_SHARDS: u32 = 64;

/// Hard member-count ceiling (same freeze as LocalHeap generated members).
pub(crate) fn max_intent_members() -> u32 {
    ResourceLimits::hard_local_heap().total_generated_members
}

/// Hard encoded-member-record ceiling (same freeze as Key-scope plan bytes).
pub(crate) fn max_encoded_member_bytes() -> u32 {
    ResourceLimits::hard_key().canonical_plan_bytes
}

/// Hard payload-file ceiling (same freeze as chunk reassembly).
pub(crate) fn max_payload_bytes() -> u32 {
    ChunkLimits::hard().max_assembled_bytes
}
