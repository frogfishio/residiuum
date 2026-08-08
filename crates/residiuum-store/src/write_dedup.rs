//! Client-operation deduplication for idempotent mutations (DEF-010).
//!
//! Derived side-channel under `store-info/`. New idempotent item-event frames
//! carry their operation identity and canonical request hash in authoritative
//! media, so an interrupted or missing table entry can be reconstructed on
//! demand. Records are written atomically after the authoritative append.

use crate::durability::DurabilityMode;
use crate::envelope::EventKind;
use crate::error::StoreError;
use crate::layout::StorePaths;
use blake3::Hasher;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Filename under `store-info/` for the write dedup table.
pub const WRITE_DEDUP_FILE: &str = "write_dedup.v1";

const MAGIC: &[u8; 8] = b"RDED0001";
const VERSION: u32 = 1;

/// One accepted client operation and the receipt it produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupRecord {
    /// Content identity hash covering op, collection, key, kind, and payload.
    pub content_hash: [u8; 32],
    /// Store that accepted the write.
    pub store_id: [u8; 16],
    /// Segment that holds the event.
    pub segment_id: [u8; 16],
    /// Item lineage id.
    pub item_id: [u8; 16],
    /// Event id assigned (or reused) for this operation.
    pub event_id: [u8; 16],
    /// Put or delete.
    pub event_kind: EventKind,
    /// Durability mode that was applied.
    pub durability: DurabilityMode,
    /// Byte offset within the segment (0 for pure memory publishes).
    pub offset: u64,
}

/// In-memory dedup table keyed by client `operation_id`.
#[derive(Debug, Clone, Default)]
pub struct WriteDedupTable {
    map: HashMap<[u8; 16], DedupRecord>,
}

impl WriteDedupTable {
    /// Empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Lookup a prior acceptance for `operation_id`.
    pub fn get(&self, operation_id: &[u8; 16]) -> Option<&DedupRecord> {
        self.map.get(operation_id)
    }

    /// Insert or replace a record.
    pub fn insert(&mut self, operation_id: [u8; 16], record: DedupRecord) {
        self.map.insert(operation_id, record);
    }

    /// Number of retained records.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// Absolute path of the write dedup file.
pub fn write_dedup_path(paths: &StorePaths) -> PathBuf {
    paths.store_info().join(WRITE_DEDUP_FILE)
}

/// Content identity over mutation fields (DEF-010).
///
/// Covers operation name, collection, key, and payload bytes. Durability is
/// intentionally excluded so a retry with the same logical mutation but a
/// stronger durability request still matches when the server already committed.
pub fn content_identity(op: &str, collection: &str, key: &str, payload: &[u8]) -> [u8; 32] {
    let mut h = Hasher::new();
    h.update(op.as_bytes());
    h.update(&[0]);
    h.update(collection.as_bytes());
    h.update(&[0]);
    h.update(key.as_bytes());
    h.update(&[0]);
    h.update(payload);
    *h.finalize().as_bytes()
}

/// Load dedup table when present and valid; otherwise empty.
///
/// Tries the previous known-good generation when the primary fails validation
/// (DEF-021). Loss of the table affects fast lookup only; new-profile segment
/// envelopes retain authoritative retry evidence for on-demand reconstruction.
pub fn load_write_dedup(path: &Path) -> Result<WriteDedupTable, StoreError> {
    if path.is_file() {
        if let Ok(bytes) = fs::read(path) {
            if let Some(table) = decode_table(&bytes) {
                return Ok(table);
            }
        }
    }
    let prev = crate::atomic_file::previous_path(path);
    if prev.is_file() {
        if let Ok(bytes) = fs::read(&prev) {
            if let Some(table) = decode_table(&bytes) {
                return Ok(table);
            }
        }
    }
    Ok(WriteDedupTable::new())
}

/// Persist the full dedup table (atomic durable replace; keeps prior generation).
pub fn save_write_dedup(path: &Path, table: &WriteDedupTable) -> Result<(), StoreError> {
    let bytes = encode_table(table);
    crate::failpoint::hit("store.dedup.before_write")?;
    crate::atomic_file::write_atomic_keep_previous(path, &bytes)?;
    crate::failpoint::hit("store.dedup.after_rename")?;
    Ok(())
}

fn encode_table(table: &WriteDedupTable) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + 4 + 4 + table.len() * 128);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&(table.map.len() as u32).to_le_bytes());
    // Stable order for deterministic files.
    let mut keys: Vec<_> = table.map.keys().copied().collect();
    keys.sort();
    for op_id in keys {
        let rec = &table.map[&op_id];
        out.extend_from_slice(&op_id);
        out.extend_from_slice(&rec.content_hash);
        out.extend_from_slice(&rec.store_id);
        out.extend_from_slice(&rec.segment_id);
        out.extend_from_slice(&rec.item_id);
        out.extend_from_slice(&rec.event_id);
        out.push(event_kind_byte(rec.event_kind));
        out.push(durability_byte(rec.durability));
        out.extend_from_slice(&rec.offset.to_le_bytes());
    }
    let mut hasher = Hasher::new();
    hasher.update(&out);
    out.extend_from_slice(hasher.finalize().as_bytes());
    out
}

fn decode_table(bytes: &[u8]) -> Option<WriteDedupTable> {
    if bytes.len() < 8 + 4 + 4 + 32 {
        return None;
    }
    if &bytes[0..8] != MAGIC.as_slice() {
        return None;
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
    if version != VERSION {
        return None;
    }
    let n = u32::from_le_bytes(bytes[12..16].try_into().ok()?) as usize;
    let mut cursor = 16usize;
    let mut map = HashMap::new();
    for _ in 0..n {
        // op_id(16) + content(32) + store(16) + seg(16) + item(16) + event(16)
        // + kind(1) + dur(1) + offset(8) = 122
        if cursor + 122 > bytes.len().saturating_sub(32) {
            return None;
        }
        let op_id: [u8; 16] = bytes[cursor..cursor + 16].try_into().ok()?;
        cursor += 16;
        let content_hash: [u8; 32] = bytes[cursor..cursor + 32].try_into().ok()?;
        cursor += 32;
        let store_id: [u8; 16] = bytes[cursor..cursor + 16].try_into().ok()?;
        cursor += 16;
        let segment_id: [u8; 16] = bytes[cursor..cursor + 16].try_into().ok()?;
        cursor += 16;
        let item_id: [u8; 16] = bytes[cursor..cursor + 16].try_into().ok()?;
        cursor += 16;
        let event_id: [u8; 16] = bytes[cursor..cursor + 16].try_into().ok()?;
        cursor += 16;
        let event_kind = event_kind_from_byte(bytes[cursor])?;
        cursor += 1;
        let durability = durability_from_byte(bytes[cursor])?;
        cursor += 1;
        let offset = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().ok()?);
        cursor += 8;
        map.insert(
            op_id,
            DedupRecord {
                content_hash,
                store_id,
                segment_id,
                item_id,
                event_id,
                event_kind,
                durability,
                offset,
            },
        );
    }
    if cursor + 32 != bytes.len() {
        return None;
    }
    let mut hasher = Hasher::new();
    hasher.update(&bytes[..cursor]);
    if hasher.finalize().as_bytes() != &bytes[cursor..cursor + 32] {
        return None;
    }
    Some(WriteDedupTable { map })
}

fn event_kind_byte(k: EventKind) -> u8 {
    match k {
        EventKind::Put => 1,
        EventKind::Delete => 2,
    }
}

fn event_kind_from_byte(b: u8) -> Option<EventKind> {
    match b {
        1 => Some(EventKind::Put),
        2 => Some(EventKind::Delete),
        _ => None,
    }
}

fn durability_byte(d: DurabilityMode) -> u8 {
    match d {
        DurabilityMode::Memory => 1,
        DurabilityMode::Buffered => 2,
        DurabilityMode::Durable => 3,
    }
}

fn durability_from_byte(b: u8) -> Option<DurabilityMode> {
    match b {
        1 => Some(DurabilityMode::Memory),
        2 => Some(DurabilityMode::Buffered),
        3 => Some(DurabilityMode::Durable),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::EventKind;

    #[test]
    fn content_identity_stable_and_sensitive() {
        let a = content_identity("put", "users", "alice", b"{\"n\":1}");
        let b = content_identity("put", "users", "alice", b"{\"n\":1}");
        let c = content_identity("put", "users", "alice", b"{\"n\":2}");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn table_roundtrip() {
        let mut table = WriteDedupTable::new();
        let op = [9u8; 16];
        table.insert(
            op,
            DedupRecord {
                content_hash: [1u8; 32],
                store_id: [2u8; 16],
                segment_id: [3u8; 16],
                item_id: [4u8; 16],
                event_id: [5u8; 16],
                event_kind: EventKind::Put,
                durability: DurabilityMode::Durable,
                offset: 42,
            },
        );
        let bytes = encode_table(&table);
        let decoded = decode_table(&bytes).unwrap();
        assert_eq!(decoded.get(&op), table.get(&op));
    }
}
