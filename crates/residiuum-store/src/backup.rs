//! Crash-consistent full backup and verified restore (DEF-050).
//!
//! A **backup** is an intentional, content-hashed package of a store's
//! authoritative bytes. It is distinct from:
//!
//! - **salvage** ([`crate::Store::salvage_to`]) — evidence-preserving recovery
//!   from a possibly damaged store (may omit holes; writes a recovery manifest);
//! - **export-live** ([`crate::Store::export_live_state`]) — re-put of current
//!   logical values with **new** event lineage.
//!
//! Backup packages preserve store identity by default. Restore can optionally
//! reassign identity for clones. Derived catalogs/indexes are not required in
//! the package; open rebuilds them from segments.
//!
//! Profile: [`BACKUP_PROFILE`].

use crate::error::StoreError;
use crate::ids::{hex16, random_id};
use crate::layout::StorePaths;
use residiuum_format::WIRE_PROFILE_LABEL;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Profile tag for the backup package format (DEF-050).
pub const BACKUP_PROFILE: &str = "residiuum-backup-v1";

/// Filename of the hashed backup manifest at the package root.
pub const BACKUP_MANIFEST_FILE: &str = "backup-manifest.v1.json";

/// Directory under the package root that holds the store tree.
pub const BACKUP_STORE_DIR: &str = "store";

/// Relative paths under a store root that hold **authoritative** bytes copied
/// into a full backup. Derived trees (`catalogs/`, `indexes/`, `snapshots/`)
/// are omitted so restore always rebuilds from segments.
///
/// `recovery/` includes Recovery Shadow media (`recovery/shadow/*.rsh`) — these
/// are **recovery-authoritative** for P★ (CSE-3), not disposable Chimera.
const AUTHORITATIVE_TOP_LEVEL: &[&str] = &[
    "store-info",
    "active",
    "segments",
    "chunks",
    "recovery",
    "tiers",
];

/// Filenames under `store-info/` that must **not** be copied (runtime locks).
const SKIP_STORE_INFO_NAMES: &[&str] = &["writer.lock", "writer.lock.pid"];

/// How the backup relates to concurrent writers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupConsistency {
    /// Exclusive writer held the store; active segment was flushed durable
    /// before the file walk. Crash-consistent at that boundary.
    FlushedExclusive,
    /// Read-only inspect open; files copied as found on disk without a flush.
    /// Concurrent unflushed buffers are not included.
    OnDiskInspect,
}

/// One file entry inside the package `store/` tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupFileEntry {
    /// Path relative to the package `store/` directory (POSIX `/` separators).
    pub relative_path: String,
    /// File size in bytes.
    pub size: u64,
    /// BLAKE3-256 hex of the full file contents.
    pub blake3_hex: String,
}

/// Hashed backup package manifest (DEF-050).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    /// Manifest schema version (currently `1`).
    pub format_version: u32,
    /// Profile label ([`BACKUP_PROFILE`]).
    pub profile: String,
    /// Unique backup generation id (hex).
    pub backup_id_hex: String,
    /// Source store root (display form).
    pub source_root: String,
    /// Source `store_id` (hex).
    pub source_store_id_hex: String,
    /// `store-info/meta` text (trimmed) when present.
    pub meta_version: String,
    /// Wire profile of the producing build.
    pub wire_profile: String,
    /// Consistency class of this package.
    pub consistency: BackupConsistency,
    /// Wall-clock creation time (unix nanoseconds, best-effort).
    pub created_unix_ns: u64,
    /// Tool identity that produced the package.
    pub tool: String,
    /// Files under `store/` with content hashes.
    pub files: Vec<BackupFileEntry>,
    /// Sum of file sizes.
    pub total_bytes: u64,
    /// BLAKE3-256 hex of the canonical manifest body without this field.
    pub content_hash_hex: String,
}

/// Result of creating a backup package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupReport {
    /// Source store root.
    pub source: PathBuf,
    /// Backup package root.
    pub destination: PathBuf,
    /// Path of the written manifest.
    pub manifest_path: PathBuf,
    /// Source store id.
    pub store_id: [u8; 16],
    /// Number of files copied.
    pub files_copied: usize,
    /// Total bytes copied.
    pub total_bytes: u64,
    /// Consistency class achieved.
    pub consistency: BackupConsistency,
    /// Backup generation id.
    pub backup_id: [u8; 16],
}

/// Options controlling restore identity and verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RestoreOptions {
    /// When true, mint a new `store_id` and rewrite the store descriptor so the
    /// restored tree is a distinct lineage (clone). Segment payload bytes are
    /// preserved; derived state is rebuilt. Default preserves identity (true
    /// disaster recovery of the same store).
    pub reassign_identity: bool,
}

/// Result of restoring a backup package into a new store directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreReport {
    /// Backup package root that was restored.
    pub backup_root: PathBuf,
    /// Destination store root.
    pub destination: PathBuf,
    /// `store_id` recorded in the backup manifest.
    pub source_store_id: [u8; 16],
    /// `store_id` after restore (new when reassigned).
    pub restored_store_id: [u8; 16],
    /// Files restored from the package.
    pub files_restored: usize,
    /// Whether identity was reassigned.
    pub identity_reassigned: bool,
    /// Live subjects observed after open+rebuild (best-effort).
    pub live_subjects: usize,
    /// Path of the backup manifest that was verified.
    pub manifest_path: PathBuf,
}

/// Path of the backup manifest under a package root.
pub fn backup_manifest_path(package_root: &Path) -> PathBuf {
    package_root.join(BACKUP_MANIFEST_FILE)
}

/// Path of the store tree inside a package.
pub fn backup_store_path(package_root: &Path) -> PathBuf {
    package_root.join(BACKUP_STORE_DIR)
}

/// Load and integrity-check a backup manifest (hash match required).
pub fn load_and_verify_manifest(package_root: &Path) -> Result<BackupManifest, StoreError> {
    let path = backup_manifest_path(package_root);
    if !path.is_file() {
        return Err(StoreError::NotAStore(package_root.to_path_buf()));
    }
    let bytes = fs::read(&path)?;
    let m: BackupManifest = serde_json::from_slice(&bytes).map_err(|e| {
        StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("parse backup manifest: {e}"),
        ))
    })?;
    if m.profile != BACKUP_PROFILE {
        return Err(StoreError::CorruptMeta("unsupported backup profile"));
    }
    if m.format_version != 1 {
        return Err(StoreError::CorruptMeta("unsupported backup format_version"));
    }
    let expected = hash_manifest(&m)?;
    if expected != m.content_hash_hex {
        return Err(StoreError::CorruptControl {
            path: path.display().to_string(),
            detail: "content_hash_hex mismatch".into(),
            recovery: "discard package; re-run backup from source".into(),
        });
    }
    Ok(m)
}

/// Verify every listed file under `store/` matches size + blake3.
pub fn verify_package_files(
    package_root: &Path,
    manifest: &BackupManifest,
) -> Result<(), StoreError> {
    let store_root = backup_store_path(package_root);
    for entry in &manifest.files {
        let path = store_root.join(
            entry
                .relative_path
                .replace('/', std::path::MAIN_SEPARATOR_STR),
        );
        if !path.is_file() {
            return Err(StoreError::CorruptControl {
                path: entry.relative_path.clone(),
                detail: "listed file missing from backup package".into(),
                recovery: "discard package; re-run backup".into(),
            });
        }
        let meta = fs::metadata(&path)?;
        if meta.len() != entry.size {
            return Err(StoreError::CorruptControl {
                path: entry.relative_path.clone(),
                detail: format!("size mismatch: expected {} got {}", entry.size, meta.len()),
                recovery: "discard package; re-run backup".into(),
            });
        }
        let hex = blake3_file_hex(&path)?;
        if hex != entry.blake3_hex {
            return Err(StoreError::CorruptControl {
                path: entry.relative_path.clone(),
                detail: "blake3 mismatch".into(),
                recovery: "discard package; re-run backup".into(),
            });
        }
    }
    Ok(())
}

/// Create a full backup package of `source_root` into `package_root`.
///
/// `package_root` must not already exist (or must be an empty directory).
/// The store tree is copied under `store/`; the hashed manifest is written at
/// the package root.
///
/// When `consistency` is [`BackupConsistency::FlushedExclusive`], the caller
/// must already have flushed durable state under an exclusive writer.
pub fn write_full_backup(
    source_root: &Path,
    source_store_id: [u8; 16],
    package_root: &Path,
    consistency: BackupConsistency,
) -> Result<BackupReport, StoreError> {
    let source_paths = StorePaths::new(source_root);
    if !source_paths.looks_like_store() {
        return Err(StoreError::NotAStore(source_root.to_path_buf()));
    }

    prepare_empty_package_root(package_root)?;
    let dest_store = backup_store_path(package_root);
    fs::create_dir_all(&dest_store)?;

    let mut files = Vec::new();
    let mut total_bytes = 0u64;

    for top in AUTHORITATIVE_TOP_LEVEL {
        let src_top = source_root.join(top);
        if !src_top.exists() {
            continue;
        }
        walk_and_copy(
            source_root,
            &src_top,
            &dest_store,
            *top == "store-info",
            &mut files,
            &mut total_bytes,
        )?;
    }

    // Deterministic manifest order.
    files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));

    let meta_version = fs::read_to_string(source_paths.meta_file())
        .unwrap_or_default()
        .trim()
        .to_string();

    let backup_id = random_id()?;
    let mut manifest = BackupManifest {
        format_version: 1,
        profile: BACKUP_PROFILE.to_string(),
        backup_id_hex: hex16(&backup_id),
        source_root: source_root.display().to_string(),
        source_store_id_hex: hex16(&source_store_id),
        meta_version,
        wire_profile: WIRE_PROFILE_LABEL.to_string(),
        consistency,
        created_unix_ns: now_ns(),
        tool: "residiuum-store".to_string(),
        files: files.clone(),
        total_bytes,
        content_hash_hex: String::new(),
    };
    manifest.content_hash_hex = hash_manifest(&manifest)?;

    let manifest_path = backup_manifest_path(package_root);
    write_manifest_atomic(&manifest_path, &manifest)?;

    // Parent package dir durable.
    crate::atomic_file::sync_dir(package_root)?;

    Ok(BackupReport {
        source: source_root.to_path_buf(),
        destination: package_root.to_path_buf(),
        manifest_path,
        store_id: source_store_id,
        files_copied: files.len(),
        total_bytes,
        consistency,
        backup_id,
    })
}

/// Restore a verified backup package into `dest_root` (must not be a store).
///
/// Verifies manifest hash and every file blake3, copies the store tree, then
/// optionally reassigns identity. Returns after a successful open+rebuild so
/// the destination is known-good.
pub fn restore_full_backup(
    package_root: &Path,
    dest_root: &Path,
    opts: RestoreOptions,
) -> Result<RestoreReport, StoreError> {
    let manifest = load_and_verify_manifest(package_root)?;
    verify_package_files(package_root, &manifest)?;

    let source_store_id = parse_store_id_hex(&manifest.source_store_id_hex)?;

    let dest_paths = StorePaths::new(dest_root);
    if dest_paths.looks_like_store() {
        return Err(StoreError::AlreadyExists(dest_root.to_path_buf()));
    }
    if dest_root.exists() {
        if !dest_root.is_dir() {
            return Err(StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "restore destination exists and is not a directory",
            )));
        }
        if fs::read_dir(dest_root)?.next().is_some() {
            return Err(StoreError::AlreadyExists(dest_root.to_path_buf()));
        }
    } else {
        fs::create_dir_all(dest_root)?;
    }

    let src_store = backup_store_path(package_root);
    // Copy only listed files (manifest is source of truth).
    for entry in &manifest.files {
        let rel = entry
            .relative_path
            .replace('/', std::path::MAIN_SEPARATOR_STR);
        let from = src_store.join(&rel);
        let to = dest_root.join(&rel);
        if let Some(parent) = to.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(&from, &to)?;
        // Re-hash destination for fail-closed integrity.
        let hex = blake3_file_hex(&to)?;
        if hex != entry.blake3_hex {
            return Err(StoreError::CorruptControl {
                path: to.display().to_string(),
                detail: "post-copy blake3 mismatch".into(),
                recovery: "delete destination; re-run restore".into(),
            });
        }
    }

    // Ensure empty derived dirs exist for a normal layout.
    dest_paths.create_dirs()?;

    let mut restored_store_id = source_store_id;
    let identity_reassigned = opts.reassign_identity;
    if opts.reassign_identity {
        restored_store_id = random_id()?;
        crate::atomic_file::write_atomic(&dest_paths.store_id_file(), &restored_store_id)?;
        // Drop descriptor so open does not treat old identity as corrupt; a
        // fresh descriptor is written below after open when possible.
        let desc = dest_paths.store_descriptor_file();
        if desc.is_file() {
            let _ = fs::remove_file(&desc);
        }
        // Derived caches are keyed by store_id — wipe so they cannot silently
        // apply under the wrong identity.
        for dir in [
            dest_paths.catalogs_dir(),
            dest_paths.indexes_dir(),
            dest_paths.snapshots_dir(),
        ] {
            if dir.is_dir() {
                let _ = fs::remove_dir_all(&dir);
                let _ = fs::create_dir_all(&dir);
            }
        }
        // write_dedup embeds store_id; drop to avoid consistency violations.
        let dedup = dest_paths.store_info().join("write_dedup.v1");
        if dedup.is_file() {
            let _ = fs::remove_file(&dedup);
        }
        // Allocator reservation embeds store_id — drop and reconstruct on open.
        let seg_seq = crate::segment_allocator::segment_seq_path(&dest_paths);
        if seg_seq.is_file() {
            let _ = fs::remove_file(&seg_seq);
        }
    }

    // Verify open + live count. Use a nested scope so the writer lock releases.
    let live_subjects = {
        let mut opened = if opts.reassign_identity {
            // Segment descriptors still carry the source store_id (byte-identical
            // backup). Tolerate foreign ownership mapping; collisions still refuse.
            crate::store::Store::open_with_options(
                dest_root,
                crate::writer_lock::StoreOpenOptions::default().tolerate_unidentified_inventory(),
            )?
        } else {
            crate::store::Store::open(dest_root)?
        };
        if opened.store_id() != restored_store_id {
            return Err(StoreError::CorruptMeta(
                "restored store_id mismatch after open",
            ));
        }
        // Best-effort: refresh derived state (identity rewrite is separate).
        let _ = opened.persist_index_cache();
        let _ = opened.rebuild_catalogs();
        opened.live_logical_entries()?.len()
    };

    // After reassignment, write a fresh store descriptor matching the new id.
    if opts.reassign_identity {
        write_restored_descriptor(dest_root, restored_store_id)?;
    }

    Ok(RestoreReport {
        backup_root: package_root.to_path_buf(),
        destination: dest_root.to_path_buf(),
        source_store_id,
        restored_store_id,
        files_restored: manifest.files.len(),
        identity_reassigned,
        live_subjects,
        manifest_path: backup_manifest_path(package_root),
    })
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

fn prepare_empty_package_root(package_root: &Path) -> Result<(), StoreError> {
    if package_root.exists() {
        if !package_root.is_dir() {
            return Err(StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "backup destination exists and is not a directory",
            )));
        }
        if fs::read_dir(package_root)?.next().is_some() {
            return Err(StoreError::AlreadyExists(package_root.to_path_buf()));
        }
    } else {
        fs::create_dir_all(package_root)?;
    }
    Ok(())
}

fn walk_and_copy(
    source_root: &Path,
    current: &Path,
    dest_store: &Path,
    under_store_info: bool,
    files: &mut Vec<BackupFileEntry>,
    total_bytes: &mut u64,
) -> Result<(), StoreError> {
    if current.is_file() {
        copy_one(
            source_root,
            current,
            dest_store,
            under_store_info,
            files,
            total_bytes,
        )?;
        return Ok(());
    }
    if !current.is_dir() {
        return Ok(());
    }
    let mut entries: Vec<_> = fs::read_dir(current)?.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if under_store_info && SKIP_STORE_INFO_NAMES.iter().any(|s| *s == name_str) {
            continue;
        }
        // Skip lock prev files and temp atomic leftovers.
        if name_str.ends_with(".tmp") || name_str.ends_with(".lock") {
            continue;
        }
        let next_under_info = under_store_info
            || path
                .strip_prefix(source_root)
                .ok()
                .and_then(|r| r.components().next())
                .map(|c| c.as_os_str() == "store-info")
                .unwrap_or(false);
        if path.is_dir() {
            walk_and_copy(
                source_root,
                &path,
                dest_store,
                next_under_info,
                files,
                total_bytes,
            )?;
        } else if path.is_file() {
            copy_one(
                source_root,
                &path,
                dest_store,
                next_under_info,
                files,
                total_bytes,
            )?;
        }
    }
    Ok(())
}

fn copy_one(
    source_root: &Path,
    src_file: &Path,
    dest_store: &Path,
    under_store_info: bool,
    files: &mut Vec<BackupFileEntry>,
    total_bytes: &mut u64,
) -> Result<(), StoreError> {
    let rel = src_file
        .strip_prefix(source_root)
        .map_err(|_| {
            StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "backup path not under source root",
            ))
        })?
        .to_path_buf();
    let rel_str = path_to_posix(&rel);
    if under_store_info {
        let base = src_file.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if SKIP_STORE_INFO_NAMES.contains(&base) {
            return Ok(());
        }
    }

    let dest = dest_store.join(&rel);
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(src_file, &dest)?;
    let size = fs::metadata(&dest)?.len();
    let blake3_hex = blake3_file_hex(&dest)?;
    *total_bytes = total_bytes.saturating_add(size);
    files.push(BackupFileEntry {
        relative_path: rel_str,
        size,
        blake3_hex,
    });
    Ok(())
}

fn path_to_posix(p: &Path) -> String {
    p.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn blake3_file_hex(path: &Path) -> Result<String, StoreError> {
    let mut f = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex32(hasher.finalize().as_bytes()))
}

fn hash_manifest(manifest: &BackupManifest) -> Result<String, StoreError> {
    let mut for_hash = manifest.clone();
    for_hash.content_hash_hex.clear();
    let body = serde_json::to_vec(&for_hash).map_err(|e| {
        StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("serialize backup manifest for hash: {e}"),
        ))
    })?;
    Ok(hex32(blake3::hash(&body).as_bytes()))
}

fn write_manifest_atomic(path: &Path, manifest: &BackupManifest) -> Result<(), StoreError> {
    let mut body = serde_json::to_vec_pretty(manifest).map_err(|e| {
        StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("serialize backup manifest: {e}"),
        ))
    })?;
    body.push(b'\n');
    crate::atomic_file::write_atomic_keep_previous(path, &body)
}

fn parse_store_id_hex(s: &str) -> Result<[u8; 16], StoreError> {
    crate::layout::unhex16(s).ok_or(StoreError::CorruptMeta("invalid store_id hex"))
}

fn hex32(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn write_restored_descriptor(dest_root: &Path, store_id: [u8; 16]) -> Result<(), StoreError> {
    use residiuum_format::encode_store_descriptor_frame;
    let paths = StorePaths::new(dest_root);
    let frame = encode_store_descriptor_frame(store_id, now_ns()).map_err(|e| {
        StoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("encode store descriptor: {e}"),
        ))
    })?;
    crate::atomic_file::write_atomic(&paths.store_descriptor_file(), &frame)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::DurabilityMode;
    use crate::store::Store;
    use tempfile::tempdir;

    #[test]
    fn backup_profile_constant() {
        assert_eq!(BACKUP_PROFILE, "residiuum-backup-v1");
    }

    #[test]
    fn full_backup_restore_roundtrip() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("src");
        let bak = dir.path().join("bak");
        let dst = dir.path().join("dst");

        let mut store = Store::create(&src).unwrap();
        store
            .put("users/a", br#"{"n":1}"#, DurabilityMode::Durable)
            .unwrap();
        store
            .put("users/b", br#"{"n":2}"#, DurabilityMode::Durable)
            .unwrap();
        let sid = store.store_id();
        let report = store.backup_to(&bak).unwrap();
        assert_eq!(report.store_id, sid);
        assert!(report.files_copied >= 2);
        assert_eq!(report.consistency, BackupConsistency::FlushedExclusive);
        drop(store);

        let restored = restore_full_backup(&bak, &dst, RestoreOptions::default()).unwrap();
        assert_eq!(restored.source_store_id, sid);
        assert_eq!(restored.restored_store_id, sid);
        assert!(!restored.identity_reassigned);
        assert_eq!(restored.live_subjects, 2);

        let opened = Store::open(&dst).unwrap();
        assert_eq!(opened.store_id(), sid);
        assert_eq!(
            opened.get("users/a").unwrap().as_deref(),
            Some(br#"{"n":1}"#.as_slice())
        );
        assert_eq!(
            opened.get("users/b").unwrap().as_deref(),
            Some(br#"{"n":2}"#.as_slice())
        );
    }

    #[test]
    fn reassign_identity_clone() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("src");
        let bak = dir.path().join("bak");
        let dst = dir.path().join("clone");

        let mut store = Store::create(&src).unwrap();
        store.put("k", b"v", DurabilityMode::Durable).unwrap();
        let sid = store.store_id();
        store.backup_to(&bak).unwrap();
        drop(store);

        let restored = restore_full_backup(
            &bak,
            &dst,
            RestoreOptions {
                reassign_identity: true,
            },
        )
        .unwrap();
        assert_eq!(restored.source_store_id, sid);
        assert_ne!(restored.restored_store_id, sid);
        assert!(restored.identity_reassigned);

        let opened = Store::open_with_options(
            &dst,
            crate::writer_lock::StoreOpenOptions::default().tolerate_unidentified_inventory(),
        )
        .unwrap();
        assert_eq!(opened.store_id(), restored.restored_store_id);
        assert_eq!(opened.get("k").unwrap().as_deref(), Some(b"v".as_slice()));
    }

    #[test]
    fn tampered_file_fails_verify() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("src");
        let bak = dir.path().join("bak");
        let dst = dir.path().join("dst");

        let mut store = Store::create(&src).unwrap();
        store.put("k", b"v", DurabilityMode::Durable).unwrap();
        store.backup_to(&bak).unwrap();
        drop(store);

        // Corrupt a copied segment/active file.
        let store_tree = backup_store_path(&bak);
        let mut corrupted = false;
        for entry in walkdir_files(&store_tree) {
            if entry.extension().and_then(|s| s.to_str()) == Some("residiuum") {
                let mut bytes = fs::read(&entry).unwrap();
                if !bytes.is_empty() {
                    let last = bytes.len() - 1;
                    bytes[last] ^= 0xff;
                    fs::write(&entry, bytes).unwrap();
                    corrupted = true;
                    break;
                }
            }
        }
        assert!(corrupted);

        let err = restore_full_backup(&bak, &dst, RestoreOptions::default()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("blake3") || msg.contains("mismatch") || msg.contains("corrupt"),
            "err={msg}"
        );
    }

    fn walkdir_files(root: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        fn rec(dir: &Path, out: &mut Vec<PathBuf>) {
            if let Ok(rd) = fs::read_dir(dir) {
                for e in rd.flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        rec(&p, out);
                    } else if p.is_file() {
                        out.push(p);
                    }
                }
            }
        }
        rec(root, &mut out);
        out
    }
}
