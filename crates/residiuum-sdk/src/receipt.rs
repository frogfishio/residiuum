//! Write and delete receipts (DX_SPEC §6.2 / §6.7).

use residiuum_store::{DurabilityMode, WriteReceipt as StoreWriteReceipt};

/// Options for put/delete (durability default is safe: durable).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutOptions {
    /// Requested durability mode. Receipt reports the mode actually applied.
    pub durability: DurabilityMode,
}

impl Default for PutOptions {
    fn default() -> Self {
        Self {
            durability: DurabilityMode::Durable,
        }
    }
}

impl PutOptions {
    /// Durable acknowledgement (default).
    pub fn durable() -> Self {
        Self {
            durability: DurabilityMode::Durable,
        }
    }

    /// Buffered (page-cache) acknowledgement.
    pub fn buffered() -> Self {
        Self {
            durability: DurabilityMode::Buffered,
        }
    }

    /// Process-memory only (does not survive restart).
    pub fn memory() -> Self {
        Self {
            durability: DurabilityMode::Memory,
        }
    }
}

/// Receipt after a successful put (DX_SPEC §6.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteReceipt {
    /// Application key that was written.
    pub key: String,
    /// Unique event identifier for this write.
    pub event_id: [u8; 16],
    /// Optimistic-concurrency version for the live value.
    ///
    /// Equals the establishing [`Self::event_id`] of this write (APB-2 OCC).
    /// Store item lineage remains available via history `item_id` (DEF-099).
    pub version: [u8; 16],
    /// Durability mode that was actually applied.
    pub acknowledgement: DurabilityMode,
    /// Whether the write is considered committed for the chosen mode.
    pub committed: bool,
    /// Store identifier.
    pub store_id: [u8; 16],
    /// Segment that received the frame.
    pub segment_id: [u8; 16],
}

impl WriteReceipt {
    pub(crate) fn from_store(key: String, r: StoreWriteReceipt) -> Self {
        Self {
            key,
            event_id: r.event_id,
            // OCC token: establishing event id (matches cluster_backend + DX §6.4).
            version: r.event_id,
            acknowledgement: r.durability,
            // Memory/buffered/durable acks from the store are successful commits
            // for their respective failure boundaries.
            committed: true,
            store_id: r.store_id,
            segment_id: r.segment_id,
        }
    }
}

/// Receipt after a delete (DX_SPEC §6.7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteReceipt {
    /// Application key that was deleted.
    pub key: String,
    /// True when a visible value was present and is now absent.
    pub removed: bool,
    /// Unique event identifier for the tombstone write.
    pub event_id: [u8; 16],
    /// OCC version for this delete event (equals [`Self::event_id`]).
    pub version: [u8; 16],
    /// Durability mode that was actually applied.
    pub acknowledgement: DurabilityMode,
    /// Whether the delete is considered committed for the chosen mode.
    pub committed: bool,
    /// Store identifier.
    pub store_id: [u8; 16],
    /// Segment that received the frame.
    pub segment_id: [u8; 16],
}

impl DeleteReceipt {
    pub(crate) fn from_store(key: String, removed: bool, r: StoreWriteReceipt) -> Self {
        Self {
            key,
            removed,
            event_id: r.event_id,
            version: r.event_id,
            acknowledgement: r.durability,
            committed: true,
            store_id: r.store_id,
            segment_id: r.segment_id,
        }
    }
}
