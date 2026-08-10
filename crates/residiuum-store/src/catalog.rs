//! Rebuildable collection / subject catalog (OVERVIEW §5.5, Stage 6).
//!
//! Catalogs are derived only. Deleting `catalogs/` never loses authoritative
//! data; open/rebuild rescans segments.

use crate::error::StoreError;
use crate::index::PrimaryIndex;
use crate::layout::StorePaths;
use blake3::Hasher;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Filename under `catalogs/` for the collection name set.
pub const COLLECTIONS_CATALOG_FILE: &str = "collections.cat";

const MAGIC: &[u8; 8] = b"RCAT0001";
const VERSION: u32 = 1;

/// Derived catalog of collection names observed in live subjects.
///
/// Collection identity is extracted from Stage 4 subject encoding when present
/// (`0x01 || coll_len:u16 LE || collection UTF-8 || key UTF-8`). Subjects that
/// do not match the draft encoding are listed under the empty collection name
/// only for diagnostics (not exposed as a named collection).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollectionCatalog {
    /// Sorted unique collection names.
    names: BTreeSet<String>,
}

impl CollectionCatalog {
    /// Empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Collection names in sorted order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.names.iter().map(|s| s.as_str())
    }

    /// Number of known collections.
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// Insert a collection name.
    ///
    /// Returns `true` when the name was not already present.
    pub fn insert(&mut self, name: impl Into<String>) -> bool {
        let name = name.into();
        if name.is_empty() {
            return false;
        }
        self.names.insert(name)
    }

    /// Rebuild from a primary index (live + deleted subjects).
    pub fn from_index(index: &PrimaryIndex) -> Self {
        let mut cat = Self::new();
        for (subject, _) in index.iter_all() {
            if let Some(name) = collection_name_from_subject(subject) {
                cat.insert(name);
            }
        }
        cat
    }
}

/// Absolute path of the collections catalog file.
pub fn collections_catalog_path(catalogs_dir: &Path) -> PathBuf {
    catalogs_dir.join(COLLECTIONS_CATALOG_FILE)
}

/// Persist the collection catalog (atomic durable replace, DEF-021).
pub fn write_collection_catalog(
    path: &Path,
    store_id: [u8; 16],
    fingerprint: [u8; 32],
    catalog: &CollectionCatalog,
) -> Result<(), StoreError> {
    let bytes = encode_catalog(store_id, fingerprint, catalog);
    crate::failpoint::hit("store.catalog.before_write")?;
    crate::atomic_file::write_atomic(path, &bytes)?;
    Ok(())
}

/// Load catalog when magic/version/store_id/fingerprint match; otherwise `None`.
pub fn try_load_collection_catalog(
    path: &Path,
    store_id: [u8; 16],
    expected_fp: [u8; 32],
) -> Result<Option<CollectionCatalog>, StoreError> {
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(_) => return Ok(None),
    };
    Ok(decode_catalog(&bytes, store_id, expected_fp))
}

/// Rebuild catalog purely from store paths (segment scan → primary index).
///
/// Prefer building from a **durable** (segment-derived) index so memory-mode
/// publishes never enter the on-disk catalog (DEF-013).
#[allow(dead_code)] // retained for salvage / operator rebuild tools
pub fn rebuild_collection_catalog(
    paths: &StorePaths,
    store_id: [u8; 16],
    index: &PrimaryIndex,
    fingerprint: [u8; 32],
) -> Result<CollectionCatalog, StoreError> {
    let catalog = CollectionCatalog::from_index(index);
    let path = collections_catalog_path(&paths.catalogs_dir());
    // Best-effort persist; rebuild result is still returned if write fails.
    let _ = write_collection_catalog(&path, store_id, fingerprint, &catalog);
    Ok(catalog)
}

fn encode_catalog(
    store_id: [u8; 16],
    fingerprint: [u8; 32],
    catalog: &CollectionCatalog,
) -> Vec<u8> {
    let names: Vec<_> = catalog.names().collect();
    let mut out = Vec::with_capacity(8 + 4 + 16 + 32 + 4 + names.len() * 32);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&store_id);
    out.extend_from_slice(&fingerprint);
    out.extend_from_slice(&(names.len() as u32).to_le_bytes());
    for name in names {
        let b = name.as_bytes();
        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
        out.extend_from_slice(b);
    }
    // Trailing content hash over the header+entries for corruption resistance.
    let mut hasher = Hasher::new();
    hasher.update(&out);
    out.extend_from_slice(hasher.finalize().as_bytes());
    out
}

fn decode_catalog(
    bytes: &[u8],
    store_id: [u8; 16],
    expected_fp: [u8; 32],
) -> Option<CollectionCatalog> {
    if bytes.len() < 8 + 4 + 16 + 32 + 4 + 32 {
        return None;
    }
    if &bytes[0..8] != MAGIC.as_slice() {
        return None;
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
    if version != VERSION {
        return None;
    }
    let sid: [u8; 16] = bytes[12..28].try_into().ok()?;
    if sid != store_id {
        return None;
    }
    let fp: [u8; 32] = bytes[28..60].try_into().ok()?;
    if fp != expected_fp {
        return None;
    }
    let n = u32::from_le_bytes(bytes[60..64].try_into().ok()?) as usize;
    let mut cursor = 64usize;
    let mut cat = CollectionCatalog::new();
    for _ in 0..n {
        if cursor + 4 > bytes.len().saturating_sub(32) {
            return None;
        }
        let len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().ok()?) as usize;
        cursor += 4;
        if cursor + len > bytes.len().saturating_sub(32) {
            return None;
        }
        let name = std::str::from_utf8(&bytes[cursor..cursor + len]).ok()?;
        cat.insert(name);
        cursor += len;
    }
    if cursor + 32 != bytes.len() {
        return None;
    }
    let mut hasher = Hasher::new();
    hasher.update(&bytes[..cursor]);
    let expect = hasher.finalize();
    if expect.as_bytes() != &bytes[cursor..cursor + 32] {
        return None;
    }
    Some(cat)
}

/// Extract Stage 4 draft collection name from a subject, if encoding matches.
pub fn collection_name_from_subject(subject: &[u8]) -> Option<String> {
    // 0x01 || coll_len:u16 LE || collection UTF-8 || key UTF-8
    if subject.first() != Some(&0x01) || subject.len() < 3 {
        return None;
    }
    let coll_len = u16::from_le_bytes(subject[1..3].try_into().ok()?) as usize;
    if coll_len == 0 || 3 + coll_len > subject.len() {
        return None;
    }
    let name = std::str::from_utf8(&subject[3..3 + coll_len]).ok()?;
    Some(name.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_roundtrip() {
        let mut cat = CollectionCatalog::new();
        cat.insert("users");
        cat.insert("orders");
        let store_id = [1u8; 16];
        let fp = [2u8; 32];
        let enc = encode_catalog(store_id, fp, &cat);
        let dec = decode_catalog(&enc, store_id, fp).unwrap();
        assert_eq!(dec, cat);
    }

    #[test]
    fn subject_collection_parse() {
        // Build subject: 0x01, len=5 "users", "u1"
        let mut s = vec![0x01];
        s.extend_from_slice(&5u16.to_le_bytes());
        s.extend_from_slice(b"users");
        s.extend_from_slice(b"u1");
        assert_eq!(collection_name_from_subject(&s).as_deref(), Some("users"));
    }
}
