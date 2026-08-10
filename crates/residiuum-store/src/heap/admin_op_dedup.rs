//! Heap-scoped admin operation dedup (APP-1 / HEAP_SPEC §14.2).
//!
//! Key: `(HeapId, operation_id)`. Value binds operation code, request fingerprint,
//! object id, and the original admin receipt fields so an identical retry returns
//! the original outcome and a conflicting fingerprint is `ConsistencyViolation`.
//!
//! Durable path: `meta/heaps/<heap-hex>/admin_op_dedup.v1` (rebuildable side
//! channel; loss resets retry memory only — authoritative collection chains remain).

use crate::atomic_file;
use crate::error::StoreError;
use crate::heap::catalog::HeapMetaLayout;
use blake3::Hasher;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Filename under a heap directory for the admin op dedup table.
pub const ADMIN_OP_DEDUP_FILE: &str = "admin_op_dedup.v1";

const MAGIC: &[u8; 8] = b"AOPD0001";
const VERSION: u32 = 1;

/// Domain tag for collection_create request fingerprints.
pub const COLLECTION_CREATE_OP: &str = "create_collection";

/// One accepted admin mutation and the receipt it produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminOpDedupRecord {
    /// Request-binding hash (op + args identity).
    pub request_binding: [u8; 32],
    /// Immutable object id created (or bound) by the first acceptance.
    pub object_id: [u8; 16],
    /// Original admin receipt id.
    pub receipt_id: [u8; 16],
    /// Descriptor hash at acceptance.
    pub descriptor_hash: [u8; 32],
    /// Original create timestamp (unix seconds).
    pub created_at: u64,
    /// Canonical name bound into the fingerprint (for diagnostics).
    pub name: String,
    /// Operation tag (e.g. `create_collection`).
    pub operation: String,
}

/// In-memory table keyed by client `operation_id` (heap-scoped on disk).
#[derive(Debug, Clone, Default)]
pub struct AdminOpDedupTable {
    map: HashMap<[u8; 16], AdminOpDedupRecord>,
}

impl AdminOpDedupTable {
    /// Empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Lookup a prior acceptance for `operation_id`.
    pub fn get(&self, operation_id: &[u8; 16]) -> Option<&AdminOpDedupRecord> {
        self.map.get(operation_id)
    }

    /// Insert or replace a record.
    pub fn insert(&mut self, operation_id: [u8; 16], record: AdminOpDedupRecord) {
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

/// Absolute path of the admin op dedup file for `heap_id`.
pub fn admin_op_dedup_path(layout: &HeapMetaLayout, heap_id: &[u8; 16]) -> PathBuf {
    layout.heap_dir(heap_id).join(ADMIN_OP_DEDUP_FILE)
}

/// Fingerprint for `create_collection` with canonical name `name`.
///
/// Covers operation tag and name only (envelope `operation_id` is the map key).
pub fn collection_create_binding(name: &str) -> [u8; 32] {
    admin_op_binding(COLLECTION_CREATE_OP, name.as_bytes())
}

/// Generic admin op binding: `op || 0x00 || args`.
pub fn admin_op_binding(op: &str, args: &[u8]) -> [u8; 32] {
    let mut h = Hasher::new();
    h.update(b"RESIDIUUM-HEAP-ADMIN-OP-V1");
    h.update(&[0]);
    h.update(op.as_bytes());
    h.update(&[0]);
    h.update(args);
    *h.finalize().as_bytes()
}

/// Load table when present and valid; otherwise empty.
pub fn load_admin_op_dedup(path: &Path) -> Result<AdminOpDedupTable, StoreError> {
    if path.is_file() {
        if let Ok(bytes) = fs::read(path) {
            if let Some(table) = decode_table(&bytes) {
                return Ok(table);
            }
        }
    }
    let prev = atomic_file::previous_path(path);
    if prev.is_file() {
        if let Ok(bytes) = fs::read(&prev) {
            if let Some(table) = decode_table(&bytes) {
                return Ok(table);
            }
        }
    }
    Ok(AdminOpDedupTable::new())
}

/// Persist the full table (atomic durable replace; keeps prior generation).
pub fn save_admin_op_dedup(path: &Path, table: &AdminOpDedupTable) -> Result<(), StoreError> {
    let bytes = encode_table(table);
    atomic_file::write_atomic_keep_previous(path, &bytes)
}

/// Resolve `(heap, operation_id)` against `request_binding`.
///
/// - `Ok(Some(rec))` — exact retry; return original outcome
/// - `Ok(None)` — new operation
/// - `Err(ConsistencyViolation)` — id reused with different binding
pub fn resolve_admin_op_dedup(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
    operation_id: &[u8; 16],
    request_binding: &[u8; 32],
) -> Result<Option<AdminOpDedupRecord>, StoreError> {
    let path = admin_op_dedup_path(layout, heap_id);
    let table = load_admin_op_dedup(&path)?;
    match table.get(operation_id) {
        None => Ok(None),
        Some(rec) if &rec.request_binding == request_binding => Ok(Some(rec.clone())),
        Some(_) => Err(StoreError::ConsistencyViolation(
            "operation_id reused with different admin request fingerprint".into(),
        )),
    }
}

/// Record a successful admin mutation under `(heap, operation_id)`.
pub fn record_admin_op_dedup(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
    operation_id: [u8; 16],
    record: AdminOpDedupRecord,
) -> Result<(), StoreError> {
    let path = admin_op_dedup_path(layout, heap_id);
    let mut table = load_admin_op_dedup(&path)?;
    table.insert(operation_id, record);
    save_admin_op_dedup(&path, &table)
}

/// Outcome of an idempotent collection create (APP-1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedCollectionAdmin {
    /// Immutable collection object id.
    pub object_id: [u8; 16],
    /// Canonical name.
    pub name: String,
    /// Admin receipt id (original on replay).
    pub receipt_id: [u8; 16],
    /// Descriptor hash at first acceptance.
    pub descriptor_hash: [u8; 32],
    /// Create timestamp (unix seconds; original on replay).
    pub created_at: u64,
    /// Client operation id.
    pub operation_id: [u8; 16],
    /// True when this result was served from the durable dedup table.
    pub replayed: bool,
}

/// Create a collection with durable `(HeapId, operation_id)` fingerprint/replay.
///
/// Shared by embedded SDK and qualified wire op 106.
pub fn create_collection_idempotent(
    layout: &HeapMetaLayout,
    heap_id: &[u8; 16],
    operation_id: [u8; 16],
    name: &str,
) -> Result<CreatedCollectionAdmin, StoreError> {
    if name.is_empty() {
        return Err(StoreError::HeapAdmit("empty collection name".into()));
    }
    if name.len() > 256 {
        return Err(StoreError::HeapAdmit("collection name too long".into()));
    }
    if name.as_bytes().contains(&0) {
        return Err(StoreError::HeapAdmit("collection name contains NUL".into()));
    }
    let binding = collection_create_binding(name);
    if let Some(prior) = resolve_admin_op_dedup(layout, heap_id, &operation_id, &binding)? {
        return Ok(CreatedCollectionAdmin {
            object_id: prior.object_id,
            name: prior.name,
            receipt_id: prior.receipt_id,
            descriptor_hash: prior.descriptor_hash,
            created_at: prior.created_at,
            operation_id,
            replayed: true,
        });
    }
    // Collection identities are UUIDv4 on the app façade (`CollectionId::from_str`).
    // Mint version bits here so remote create responses parse without unchecked recovery.
    let object_id = *residiuum_heap::CollectionId::new_random()
        .map_err(|e| StoreError::HeapAdmit(format!("mint collection id: {e}")))?
        .as_bytes();
    let admin = super::catalog::create_object(
        layout,
        heap_id,
        super::catalog::ObjectKind::Collection,
        object_id,
        operation_id,
        name,
    )?;
    let object_id = admin
        .object_id
        .ok_or_else(|| StoreError::HeapAdmit("create_object omitted object_id".into()))?;
    record_admin_op_dedup(
        layout,
        heap_id,
        operation_id,
        AdminOpDedupRecord {
            request_binding: binding,
            object_id,
            receipt_id: admin.receipt_id,
            descriptor_hash: admin.descriptor_hash,
            created_at: admin.created_at,
            name: name.to_string(),
            operation: COLLECTION_CREATE_OP.into(),
        },
    )?;
    Ok(CreatedCollectionAdmin {
        object_id,
        name: name.to_string(),
        receipt_id: admin.receipt_id,
        descriptor_hash: admin.descriptor_hash,
        created_at: admin.created_at,
        operation_id,
        replayed: false,
    })
}

fn encode_table(table: &AdminOpDedupTable) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + 4 + 4 + table.len() * 160);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&(table.map.len() as u32).to_le_bytes());
    let mut keys: Vec<_> = table.map.keys().copied().collect();
    keys.sort();
    for op_id in keys {
        let rec = &table.map[&op_id];
        out.extend_from_slice(&op_id);
        out.extend_from_slice(&rec.request_binding);
        out.extend_from_slice(&rec.object_id);
        out.extend_from_slice(&rec.receipt_id);
        out.extend_from_slice(&rec.descriptor_hash);
        out.extend_from_slice(&rec.created_at.to_le_bytes());
        let name_bytes = rec.name.as_bytes();
        let op_bytes = rec.operation.as_bytes();
        if name_bytes.len() > u16::MAX as usize || op_bytes.len() > u16::MAX as usize {
            // Defensive: names are validated well under 256; truncate encoding impossible path.
            continue;
        }
        out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(name_bytes);
        out.extend_from_slice(&(op_bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(op_bytes);
    }
    let mut hasher = Hasher::new();
    hasher.update(&out);
    out.extend_from_slice(hasher.finalize().as_bytes());
    out
}

fn decode_table(bytes: &[u8]) -> Option<AdminOpDedupTable> {
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
        // fixed: op(16)+bind(32)+obj(16)+rcpt(16)+desc(32)+ts(8) = 120
        if cursor + 120 > bytes.len().saturating_sub(32) {
            return None;
        }
        let op_id: [u8; 16] = bytes[cursor..cursor + 16].try_into().ok()?;
        cursor += 16;
        let request_binding: [u8; 32] = bytes[cursor..cursor + 32].try_into().ok()?;
        cursor += 32;
        let object_id: [u8; 16] = bytes[cursor..cursor + 16].try_into().ok()?;
        cursor += 16;
        let receipt_id: [u8; 16] = bytes[cursor..cursor + 16].try_into().ok()?;
        cursor += 16;
        let descriptor_hash: [u8; 32] = bytes[cursor..cursor + 32].try_into().ok()?;
        cursor += 32;
        let created_at = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().ok()?);
        cursor += 8;
        if cursor + 2 > bytes.len().saturating_sub(32) {
            return None;
        }
        let name_len = u16::from_le_bytes(bytes[cursor..cursor + 2].try_into().ok()?) as usize;
        cursor += 2;
        if cursor + name_len + 2 > bytes.len().saturating_sub(32) {
            return None;
        }
        let name = std::str::from_utf8(&bytes[cursor..cursor + name_len])
            .ok()?
            .to_string();
        cursor += name_len;
        let op_len = u16::from_le_bytes(bytes[cursor..cursor + 2].try_into().ok()?) as usize;
        cursor += 2;
        if cursor + op_len > bytes.len().saturating_sub(32) {
            return None;
        }
        let operation = std::str::from_utf8(&bytes[cursor..cursor + op_len])
            .ok()?
            .to_string();
        cursor += op_len;
        map.insert(
            op_id,
            AdminOpDedupRecord {
                request_binding,
                object_id,
                receipt_id,
                descriptor_hash,
                created_at,
                name,
                operation,
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
    Some(AdminOpDedupTable { map })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_stable_and_name_sensitive() {
        let a = collection_create_binding("orders");
        let b = collection_create_binding("orders");
        let c = collection_create_binding("users");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn table_roundtrip() {
        let mut table = AdminOpDedupTable::new();
        let op = [9u8; 16];
        table.insert(
            op,
            AdminOpDedupRecord {
                request_binding: [1u8; 32],
                object_id: [2u8; 16],
                receipt_id: [3u8; 16],
                descriptor_hash: [4u8; 32],
                created_at: 1_700_000_000,
                name: "orders".into(),
                operation: COLLECTION_CREATE_OP.into(),
            },
        );
        let bytes = encode_table(&table);
        let decoded = decode_table(&bytes).unwrap();
        assert_eq!(decoded.get(&op), table.get(&op));
    }
}
