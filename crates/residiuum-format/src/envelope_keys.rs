//! Authoritative frame-envelope key registry (CR-ATM2-002).
//!
//! One uint-key map is shared by FORMAT_SPEC §4.4, HEAP ownership (HP-002),
//! and the Atomic extension. Keys 1–30 keep FORMAT_SPEC core meanings.
//! Keys 31–36 are HEAP ownership. Keys 37–40 are the reserved Atomic
//! namespace. Client `operation_id` / `operation_content_hash` live at 41 / 42
//! (CR-R2-006). Atomic and ownership helpers do not emit them.

/// Envelope key: `heap_id` (bstr 16). Every heap-aware frame.
pub const ENV_HEAP_ID: u64 = 31;
/// Envelope key: `collection_id` (bstr 16).
pub const ENV_COLLECTION_ID: u64 = 32;
/// Envelope key: `stream_id` (bstr 16).
pub const ENV_STREAM_ID: u64 = 33;
/// Envelope key: `ownership_profile` (uint).
pub const ENV_OWNERSHIP_PROFILE: u64 = 34;
/// Envelope key: `source_heap_id` (bstr 16). Provenance only.
pub const ENV_SOURCE_HEAP_ID: u64 = 35;
/// Envelope key: `source_object_id` (bstr). Provenance only.
pub const ENV_SOURCE_OBJECT_ID: u64 = 36;

/// Envelope key: `atomic_id` (`bstr` 32). Reserved Atomic namespace.
pub const ENV_ATOMIC_ID: u64 = 37;
/// Envelope key: member `ordinal` (uint; `ItemEvent` only).
pub const ENV_ATOMIC_ORDINAL: u64 = 38;
/// Envelope key: plan `content_root` (`bstr` 32).
pub const ENV_ATOMIC_CONTENT_ROOT: u64 = 39;
/// Envelope key: Heap `commit_position` (nonzero uint).
pub const ENV_ATOMIC_COMMIT_POSITION: u64 = 40;

/// Client `operation_id` (`bstr` 16). Store writers emit this pair, not 31/32.
pub const ENV_OPERATION_ID: u64 = 41;
/// `operation_content_hash` (`bstr` 32). Emitted with [`ENV_OPERATION_ID`].
pub const ENV_OPERATION_CONTENT_HASH: u64 = 42;

/// First key of the reserved Atomic extension namespace.
pub const ATOMIC_EXTENSION_FIRST: u64 = ENV_ATOMIC_ID;
/// Last key of the reserved Atomic extension namespace.
pub const ATOMIC_EXTENSION_LAST: u64 = ENV_ATOMIC_COMMIT_POSITION;

/// True when `key` is in the reserved Atomic extension (37–40).
pub const fn is_atomic_extension_key(key: u64) -> bool {
    key >= ATOMIC_EXTENSION_FIRST && key <= ATOMIC_EXTENSION_LAST
}

/// True when `key` is a HEAP ownership field (31–36).
pub const fn is_ownership_key(key: u64) -> bool {
    key >= ENV_HEAP_ID && key <= ENV_SOURCE_OBJECT_ID
}
