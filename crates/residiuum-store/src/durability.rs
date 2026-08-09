//! Durability modes (OVERVIEW §7.2).

/// Acknowledged failure boundary for a write.
///
/// Every successful put/delete returns the mode that actually applied.
/// Performance claims MUST name the mode measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum DurabilityMode {
    /// Acknowledgement after process-memory publication.
    ///
    /// Process or power failure may lose the write. Bytes may not be on disk.
    Memory,

    /// Acknowledgement after transfer to the OS page cache / device queue.
    ///
    /// Power failure may lose recent writes. Does not require `fsync`.
    #[default]
    Buffered,

    /// Acknowledgement only after authoritative bytes and required allocation
    /// metadata have crossed this implementation's stable-storage boundary
    /// (`write` + `sync_data` on an already-published active segment; segment
    /// creation/publication uses `sync_all` + parent-directory sync).
    Durable,
}

impl DurabilityMode {
    /// Short stable name for receipts and logs.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Buffered => "buffered",
            Self::Durable => "durable",
        }
    }
}
