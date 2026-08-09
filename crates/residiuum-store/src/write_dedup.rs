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
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Filename under `store-info/` for the write dedup table.
pub const WRITE_DEDUP_FILE: &str = "write_dedup.v1";
/// Append-only derived outcome journal. The authoritative item-event envelope
/// remains the recovery source; this file is the bounded fast-lookup path.
pub const WRITE_DEDUP_JOURNAL_FILE: &str = "write_dedup.journal.v1";

const MAGIC: &[u8; 8] = b"RDED0001";
const VERSION: u32 = 1;
const JOURNAL_MAGIC: &[u8; 8] = b"RDEJ0001";
const JOURNAL_VERSION: u32 = 1;
const JOURNAL_HEADER_LEN: usize = 12;
// content(32) + store/segment/item/event(4 * 16) + kind/durability(2) + offset(8)
const JOURNAL_PAYLOAD_LEN: usize = 106;
const JOURNAL_RECORD_LEN: usize = 16 + JOURNAL_PAYLOAD_LEN + 32;

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

/// Absolute path of the append-only outcome journal.
pub fn write_dedup_journal_path(paths: &StorePaths) -> PathBuf {
    paths.store_info().join(WRITE_DEDUP_JOURNAL_FILE)
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
    let mut table = load_write_dedup_checkpoint(path);
    let journal = path.with_file_name(WRITE_DEDUP_JOURNAL_FILE);
    if let Ok(bytes) = fs::read(journal) {
        replay_journal(&bytes, &mut table);
    }
    Ok(table)
}

fn load_write_dedup_checkpoint(path: &Path) -> WriteDedupTable {
    if path.is_file() {
        if let Ok(bytes) = fs::read(path) {
            if let Some(table) = decode_table(&bytes) {
                return table;
            }
        }
    }
    let prev = crate::atomic_file::previous_path(path);
    if prev.is_file() {
        if let Ok(bytes) = fs::read(&prev) {
            if let Some(table) = decode_table(&bytes) {
                return table;
            }
        }
    }
    WriteDedupTable::new()
}

/// Append one accepted operation outcome and cross its stable-storage boundary.
///
/// Unlike the former full-table atomic replacement this performs O(1) work and
/// writes a fixed-size checksummed record. A torn or corrupt suffix is ignored
/// on load because authoritative item-event envelopes remain recoverable.
pub fn append_write_dedup(
    path: &Path,
    operation_id: [u8; 16],
    record: &DedupRecord,
) -> Result<(), StoreError> {
    append_write_dedup_batch(path, &[(operation_id, record)])
}

/// Append a cohort of outcomes and cross one shared journal boundary.
pub fn append_write_dedup_batch(
    path: &Path,
    records: &[([u8; 16], &DedupRecord)],
) -> Result<(), StoreError> {
    if records.is_empty() {
        return Ok(());
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let new_file = !path.is_file() || fs::metadata(path)?.len() == 0;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if new_file {
        file.write_all(JOURNAL_MAGIC)?;
        file.write_all(&JOURNAL_VERSION.to_le_bytes())?;
    }
    crate::failpoint::hit("store.dedup.before_write")?;
    for (operation_id, record) in records {
        file.write_all(&encode_journal_record(*operation_id, record))?;
    }
    file.sync_data()?;
    crate::failpoint::hit("store.dedup.after_append")?;
    Ok(())
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

fn encode_journal_record(operation_id: [u8; 16], record: &DedupRecord) -> Vec<u8> {
    let mut out = Vec::with_capacity(JOURNAL_RECORD_LEN);
    out.extend_from_slice(&operation_id);
    encode_record_payload(&mut out, record);
    let checksum = blake3::hash(&out);
    out.extend_from_slice(checksum.as_bytes());
    out
}

fn encode_record_payload(out: &mut Vec<u8>, rec: &DedupRecord) {
    out.extend_from_slice(&rec.content_hash);
    out.extend_from_slice(&rec.store_id);
    out.extend_from_slice(&rec.segment_id);
    out.extend_from_slice(&rec.item_id);
    out.extend_from_slice(&rec.event_id);
    out.push(event_kind_byte(rec.event_kind));
    out.push(durability_byte(rec.durability));
    out.extend_from_slice(&rec.offset.to_le_bytes());
}

fn replay_journal(bytes: &[u8], table: &mut WriteDedupTable) {
    if bytes.len() < JOURNAL_HEADER_LEN
        || &bytes[..8] != JOURNAL_MAGIC
        || u32::from_le_bytes(bytes[8..12].try_into().unwrap()) != JOURNAL_VERSION
    {
        return;
    }
    let mut cursor = JOURNAL_HEADER_LEN;
    while cursor + JOURNAL_RECORD_LEN <= bytes.len() {
        let record = &bytes[cursor..cursor + JOURNAL_RECORD_LEN];
        let payload_end = 16 + JOURNAL_PAYLOAD_LEN;
        if blake3::hash(&record[..payload_end]).as_bytes()
            != &record[payload_end..JOURNAL_RECORD_LEN]
        {
            break;
        }
        let operation_id: [u8; 16] = record[..16].try_into().unwrap();
        let Some(decoded) = decode_record_payload(&record[16..payload_end]) else {
            break;
        };
        table.insert(operation_id, decoded);
        cursor += JOURNAL_RECORD_LEN;
    }
}

fn decode_record_payload(bytes: &[u8]) -> Option<DedupRecord> {
    if bytes.len() != JOURNAL_PAYLOAD_LEN {
        return None;
    }
    let mut cursor = 0usize;
    let content_hash = bytes[cursor..cursor + 32].try_into().ok()?;
    cursor += 32;
    let store_id = bytes[cursor..cursor + 16].try_into().ok()?;
    cursor += 16;
    let segment_id = bytes[cursor..cursor + 16].try_into().ok()?;
    cursor += 16;
    let item_id = bytes[cursor..cursor + 16].try_into().ok()?;
    cursor += 16;
    let event_id = bytes[cursor..cursor + 16].try_into().ok()?;
    cursor += 16;
    let event_kind = event_kind_from_byte(bytes[cursor])?;
    cursor += 1;
    let durability = durability_from_byte(bytes[cursor])?;
    cursor += 1;
    let offset = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().ok()?);
    Some(DedupRecord {
        content_hash,
        store_id,
        segment_id,
        item_id,
        event_id,
        event_kind,
        durability,
        offset,
    })
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

    #[test]
    fn journal_replays_complete_records_and_ignores_torn_tail() {
        let op = [9u8; 16];
        let record = DedupRecord {
            content_hash: [1u8; 32],
            store_id: [2u8; 16],
            segment_id: [3u8; 16],
            item_id: [4u8; 16],
            event_id: [5u8; 16],
            event_kind: EventKind::Put,
            durability: DurabilityMode::Durable,
            offset: 42,
        };
        let mut bytes = Vec::from(JOURNAL_MAGIC.as_slice());
        bytes.extend_from_slice(&JOURNAL_VERSION.to_le_bytes());
        bytes.extend_from_slice(&encode_journal_record(op, &record));
        bytes.extend_from_slice(&encode_journal_record([8u8; 16], &record)[..37]);
        let mut table = WriteDedupTable::new();
        replay_journal(&bytes, &mut table);
        assert_eq!(table.get(&op), Some(&record));
        assert!(table.get(&[8u8; 16]).is_none());
    }

    #[test]
    fn journal_growth_is_constant_per_operation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(WRITE_DEDUP_JOURNAL_FILE);
        let record = DedupRecord {
            content_hash: [1u8; 32],
            store_id: [2u8; 16],
            segment_id: [3u8; 16],
            item_id: [4u8; 16],
            event_id: [5u8; 16],
            event_kind: EventKind::Put,
            durability: DurabilityMode::Durable,
            offset: 42,
        };
        append_write_dedup(&path, [7u8; 16], &record).unwrap();
        let first = fs::metadata(&path).unwrap().len();
        append_write_dedup(&path, [8u8; 16], &record).unwrap();
        let second = fs::metadata(&path).unwrap().len();
        assert_eq!(first, (JOURNAL_HEADER_LEN + JOURNAL_RECORD_LEN) as u64);
        assert_eq!(second - first, JOURNAL_RECORD_LEN as u64);
    }
}
