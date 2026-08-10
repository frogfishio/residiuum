//! Atomic durable control-document writes (DEF-021).
//!
//! All mutable store / cluster control documents MUST use this helper so a
//! crash at any boundary leaves either the previous valid generation or the
//! new valid generation — never a torn half-write at the published path.
//!
//! ## Protocol
//!
//! 1. Create a temp file in the **same directory** as the target
//! 2. Write all bytes
//! 3. `File::sync_all` the temp file
//! 4. Optionally promote the existing target to `path` + [PREV_SUFFIX]
//! 5. `rename` temp → target (atomic replace on POSIX)
//! 6. `sync_all` the parent directory
//!
//! Failpoints (when armed) fire at named steps for crash-matrix testing:
//! `atomic.before_tmp_write`, `atomic.after_tmp_sync`, `atomic.before_rename`,
//! `atomic.after_rename`, `atomic.after_dir_sync`.

use crate::error::StoreError;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Suffix for the previous known-good generation of a control document.
pub const PREV_SUFFIX: &str = ".prev";

/// Options for an atomic control-document write.
#[derive(Debug, Clone, Copy, Default)]
pub struct AtomicWriteOptions {
    /// When true, rename any existing target to [`previous_path`] before
    /// installing the new generation (non-trivial reconstruction documents).
    pub keep_previous: bool,
}

/// Path of the previous known-good generation for `path`.
///
/// Example: `endpoints.json` → `endpoints.json.prev`.
pub fn previous_path(path: &Path) -> PathBuf {
    let mut os = path.as_os_str().to_owned();
    os.push(PREV_SUFFIX);
    PathBuf::from(os)
}

/// Stage timings for one atomic control-document publish (qualification).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AtomicWriteTiming {
    /// Bytes written into the temp file (before sync).
    pub sequential_write_ns: u64,
    /// `File::sync_all` on the temp file.
    pub file_sync_ns: u64,
    /// Rename temp → published path (and optional keep-previous rename).
    pub rename_ns: u64,
    /// Parent directory sync after rename.
    pub dir_sync_ns: u64,
}

/// Atomically replace `path` with `bytes` (no previous generation retained).
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    write_atomic_with(path, bytes, AtomicWriteOptions::default())
}

/// Atomically publish `bytes` only when the currently published bytes differ.
///
/// This is intended for derived control documents reconstructed during clean
/// open. An unchanged document needs no durability event; changed or unreadable
/// state still uses the complete atomic publish and directory-sync protocol.
/// Returns `true` when a publish was performed.
pub fn write_atomic_if_changed(path: &Path, bytes: &[u8]) -> Result<bool, StoreError> {
    if fs::read(path).is_ok_and(|current| current == bytes) {
        return Ok(false);
    }
    write_atomic(path, bytes)?;
    Ok(true)
}

/// Atomically replace `path`, keeping the prior file as [`previous_path`].
pub fn write_atomic_keep_previous(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    write_atomic_with(
        path,
        bytes,
        AtomicWriteOptions {
            keep_previous: true,
        },
    )
}

/// Atomically replace `path` with `bytes` under `opts`.
pub fn write_atomic_with(
    path: &Path,
    bytes: &[u8],
    opts: AtomicWriteOptions,
) -> Result<(), StoreError> {
    write_atomic_with_timed(path, bytes, opts).map(|_| ())
}

/// Atomically replace `path` with `bytes` under `opts`, returning stage timings.
///
/// When `data_sync_only` is true, the temp file uses `sync_data` instead of
/// `sync_all` (metadata still covered by the parent directory sync after rename).
/// Intended for large Recovery Shadow payloads where full `sync_all` metadata
/// flush dominates without adding durability for the published content.
pub fn write_atomic_with_timed_ex(
    path: &Path,
    bytes: &[u8],
    opts: AtomicWriteOptions,
    data_sync_only: bool,
) -> Result<AtomicWriteTiming, StoreError> {
    use std::time::Instant;

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;

    let tmp = temp_path_for(path);
    // Clean a leftover temp from a previous crash (best-effort).
    let _ = fs::remove_file(&tmp);

    crate::failpoint::hit("atomic.before_tmp_write")?;

    let mut timing = AtomicWriteTiming::default();
    {
        let mut f = OpenOptions::new().create_new(true).write(true).open(&tmp)?;
        // DEF-022: optional short-write of control-document temp body.
        if crate::failpoint::consume_short_write("atomic.tmp.short_write") {
            let n = crate::failpoint::short_write_len(bytes.len());
            if n > 0 {
                f.write_all(&bytes[..n])?;
            }
            // Leave the incomplete temp in place; published path untouched.
            return Err(StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failpoint short write: atomic.tmp.short_write",
            )));
        }
        let t_write = Instant::now();
        f.write_all(bytes)?;
        timing.sequential_write_ns = t_write.elapsed().as_nanos() as u64;
        let t_sync = Instant::now();
        if data_sync_only {
            f.sync_data()?;
        } else {
            f.sync_all()?;
        }
        timing.file_sync_ns = t_sync.elapsed().as_nanos() as u64;
    }

    crate::failpoint::hit("atomic.after_tmp_sync")?;

    let t_rename = Instant::now();
    if opts.keep_previous && path.is_file() {
        let prev = previous_path(path);
        // Replace any older .prev so we retain exactly one known-good generation.
        let _ = fs::remove_file(&prev);
        // rename is atomic on POSIX; if we crash after this, load falls back to .prev.
        fs::rename(path, &prev)?;
    }

    crate::failpoint::hit("atomic.before_rename")?;
    fs::rename(&tmp, path)?;
    crate::failpoint::hit("atomic.after_rename")?;
    timing.rename_ns = t_rename.elapsed().as_nanos() as u64;

    let t_dir = Instant::now();
    sync_dir(parent)?;
    timing.dir_sync_ns = t_dir.elapsed().as_nanos() as u64;
    crate::failpoint::hit("atomic.after_dir_sync")?;
    Ok(timing)
}

/// Same protocol as [`write_atomic_with`], returning stage timings.
pub fn write_atomic_with_timed(
    path: &Path,
    bytes: &[u8],
    opts: AtomicWriteOptions,
) -> Result<AtomicWriteTiming, StoreError> {
    write_atomic_with_timed_ex(path, bytes, opts, false)
}

/// Read control-document bytes from `path`, falling back to the previous
/// generation when the primary is missing or unreadable as a file.
///
/// Does **not** parse; callers validate checksums / schema. Returns
/// `(bytes, from_previous)`.
pub fn read_with_previous(path: &Path) -> Result<Option<(Vec<u8>, bool)>, StoreError> {
    if path.is_file() {
        match fs::read(path) {
            Ok(b) => return Ok(Some((b, false))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(StoreError::Io(e)),
        }
    }
    let prev = previous_path(path);
    if prev.is_file() {
        match fs::read(&prev) {
            Ok(b) => return Ok(Some((b, true))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(StoreError::Io(e)),
        }
    }
    Ok(None)
}

/// Build a diagnostic when primary parse fails; try previous generation bytes.
///
/// Returns `Ok(Some(bytes))` from `.prev` when present, `Ok(None)` when no
/// usable generation exists. Always records the primary failure in
/// [`StoreError::CorruptControl`] when neither generation is usable **and**
/// `require_valid` is true — callers that treat missing as empty pass
/// `require_valid = false` and only error when primary exists but is garbage
/// **and** previous is also unusable.
pub fn recover_previous_or_corrupt(
    path: &Path,
    primary_detail: &str,
    recovery: &str,
) -> Result<Option<Vec<u8>>, StoreError> {
    let prev = previous_path(path);
    if prev.is_file() {
        if let Ok(b) = fs::read(&prev) {
            return Ok(Some(b));
        }
    }
    Err(StoreError::CorruptControl {
        path: path.display().to_string(),
        detail: primary_detail.to_string(),
        recovery: recovery.to_string(),
    })
}

/// Temp path sibling used by atomic publishers (including Recovery Shadow mirror).
pub fn temp_path_for(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("control");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id();
    path.with_file_name(format!(".{file_name}.{pid}.{nanos}.tmp"))
}

/// Sync a directory entry (best-effort on non-Unix).
pub fn sync_dir(path: &Path) -> Result<(), StoreError> {
    #[cfg(unix)]
    {
        let dir = File::open(path)?;
        dir.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn write_atomic_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("meta.json");
        write_atomic(&path, b"{\"v\":1}").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"{\"v\":1}");
        write_atomic(&path, b"{\"v\":2}").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"{\"v\":2}");
        assert!(!previous_path(&path).exists());
    }

    #[test]
    fn write_atomic_if_changed_skips_identical_publish() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("derived.bin");
        assert!(write_atomic_if_changed(&path, b"one").unwrap());
        assert!(!write_atomic_if_changed(&path, b"one").unwrap());
        assert!(write_atomic_if_changed(&path, b"two").unwrap());
        assert_eq!(fs::read(path).unwrap(), b"two");
    }

    #[test]
    fn keep_previous_survives_replace() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("endpoints.json");
        write_atomic_keep_previous(&path, b"gen1").unwrap();
        write_atomic_keep_previous(&path, b"gen2").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"gen2");
        assert_eq!(fs::read(previous_path(&path)).unwrap(), b"gen1");
    }

    #[test]
    fn read_with_previous_falls_back() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("placement.json");
        write_atomic_keep_previous(&path, b"old").unwrap();
        write_atomic_keep_previous(&path, b"new").unwrap();
        fs::remove_file(&path).unwrap();
        let (bytes, from_prev) = read_with_previous(&path).unwrap().unwrap();
        assert!(from_prev);
        assert_eq!(bytes, b"old");
    }

    #[test]
    fn torn_temp_does_not_replace_published() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("catalog.bin");
        write_atomic(&path, b"good").unwrap();
        // Simulate crash leftover temp without rename.
        let tmp = temp_path_for(&path);
        fs::write(&tmp, b"torn").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"good");
        // Next write cleans and succeeds.
        write_atomic(&path, b"better").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"better");
    }
}
