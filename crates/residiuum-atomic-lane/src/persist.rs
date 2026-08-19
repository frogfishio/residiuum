//! Instrumented exclusive / atomic / log persist helpers (CR-ATMR3-008).

use crate::error::LaneError;
use crate::io_fail::{self, IoInjected, IoPhase, IoPoint, IoSite};
use crate::limits::SidecarRole;
use residiuum_atomics::{AtomicRefuseReason, AtomicsError};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_SEQ: AtomicU64 = AtomicU64::new(1);

fn map_injected(err: IoInjected) -> LaneError {
    match err {
        IoInjected::Kill(point) => LaneError::InjectedIo {
            site: point.site,
            phase: point.phase,
        },
        IoInjected::Io(e) => LaneError::Io(e),
    }
}

fn hit(site: IoSite, phase: IoPhase) -> Result<(), LaneError> {
    io_fail::hit(IoPoint::new(site, phase)).map_err(map_injected)
}

/// Publish `path` via a unique temp file. Never replace a different identity.
///
/// Partial temps are never final authority. An existing final is same-ID
/// retry only when the bytes match exactly. Prefix/empty/other bytes are
/// preserved as conflict or unauthenticated damage (CR-ATMR5-007).
/// Shared `.tmp` names are not used (CR-ATMR4-006).
pub fn write_exclusive(path: &Path, bytes: &[u8], site: IoSite) -> Result<(), LaneError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if path.exists() {
        return match classify_existing(path, bytes, site)? {
            Existing::Same => Ok(()),
            Existing::Conflict => {
                Err(AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict).into())
            }
            Existing::Damaged => Err(LaneError::Corrupt("unauthenticated exclusive final")),
        };
    }
    hit(site, IoPhase::BeforeWrite)?;
    let tmp = unique_sidecar_temp(path);
    {
        let mut file = File::create(&tmp)?;
        if io_fail::consume_short_write(site) {
            let n = bytes.len() / 2;
            if n > 0 {
                file.write_all(&bytes[..n])?;
            }
            let _ = fs::remove_file(&tmp);
            return Err(map_injected(io_fail::short_write_error(site)));
        }
        file.write_all(bytes)?;
        hit(site, IoPhase::AfterWrite)?;
        if !io_fail::omit_file_sync() {
            file.sync_all()?;
            hit(site, IoPhase::AfterFileSync)?;
        }
    }
    hit(site, IoPhase::BeforeRename)?;
    if !io_fail::omit_rename() {
        publish_no_replace(&tmp, path, bytes, site)?;
        hit(site, IoPhase::AfterRename)?;
        let _ = fs::remove_file(&tmp);
    }
    if let Some(parent) = path.parent() {
        if !io_fail::omit_dir_sync() {
            sync_dir(parent)?;
            hit(site, IoPhase::AfterDirSync)?;
        }
    }
    Ok(())
}

enum Existing {
    Same,
    Conflict,
    Damaged,
}

fn sidecar_limit(site: IoSite) -> u64 {
    match site {
        IoSite::Plan => SidecarRole::Plan.max_bytes(),
        IoSite::Intent => SidecarRole::Intent.max_bytes(),
        IoSite::Payload => SidecarRole::Payload.max_bytes(),
        IoSite::ChunkManifest => SidecarRole::ChunkManifest.max_bytes(),
        IoSite::Chunk => SidecarRole::ChunkBody.max_bytes(),
        IoSite::Seal => SidecarRole::Seal.max_bytes(),
        IoSite::Ack => SidecarRole::Ack.max_bytes(),
        IoSite::Checkpoint => SidecarRole::Checkpoint.max_bytes(),
        IoSite::Identity => SidecarRole::Plan.max_bytes(),
        IoSite::Coordinator | IoSite::Shard => {
            crate::limits::RecoveryLimits::prototype().max_log_bytes
        }
    }
}

fn classify_existing(path: &Path, intended: &[u8], site: IoSite) -> Result<Existing, LaneError> {
    let len = fs::metadata(path)?.len();
    let limit = sidecar_limit(site);
    if len > limit {
        return Err(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded).into());
    }
    if len == 0 {
        return Ok(Existing::Damaged);
    }
    let existing = fs::read(path)?;
    if existing.as_slice() == intended {
        Ok(Existing::Same)
    } else {
        Ok(Existing::Conflict)
    }
}

fn unique_sidecar_temp(path: &Path) -> PathBuf {
    let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "exclusive".into());
    path.with_file_name(format!(".{name}.{}.{seq}.tmp", std::process::id()))
}

fn publish_no_replace(
    tmp: &Path,
    dest: &Path,
    bytes: &[u8],
    site: IoSite,
) -> Result<(), LaneError> {
    match fs::hard_link(tmp, dest) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            match classify_existing(dest, bytes, site)? {
                Existing::Same => Ok(()),
                Existing::Conflict => {
                    Err(AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict).into())
                }
                Existing::Damaged => Err(LaneError::Corrupt("unauthenticated exclusive final")),
            }
        }
        Err(e) => Err(e.into()),
    }
}

/// Temp-file write, sync, rename, directory sync.
pub fn write_atomic(path: &Path, bytes: &[u8], site: IoSite) -> Result<(), LaneError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    {
        hit(site, IoPhase::BeforeWrite)?;
        let mut file = File::create(&tmp)?;
        if io_fail::consume_short_write(site) {
            let n = bytes.len() / 2;
            if n > 0 {
                file.write_all(&bytes[..n])?;
            }
            return Err(map_injected(io_fail::short_write_error(site)));
        }
        file.write_all(bytes)?;
        hit(site, IoPhase::AfterWrite)?;
        if !io_fail::omit_file_sync() {
            file.sync_all()?;
            hit(site, IoPhase::AfterFileSync)?;
        }
    }
    hit(site, IoPhase::BeforeRename)?;
    if !io_fail::omit_rename() {
        fs::rename(&tmp, path)?;
        hit(site, IoPhase::AfterRename)?;
    }
    if let Some(parent) = path.parent() {
        if !io_fail::omit_dir_sync() {
            sync_dir(parent)?;
            hit(site, IoPhase::AfterDirSync)?;
        }
    }
    Ok(())
}

/// Append `bytes` to a log, sync file and directory, then persist the ack.
pub fn append_synced(path: &Path, bytes: &[u8], site: IoSite) -> Result<(), LaneError> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    hit(site, IoPhase::BeforeWrite)?;
    if io_fail::consume_short_write(site) {
        let n = bytes.len() / 2;
        if n > 0 {
            file.write_all(&bytes[..n])?;
        }
        return Err(map_injected(io_fail::short_write_error(site)));
    }
    file.write_all(bytes)?;
    hit(site, IoPhase::AfterWrite)?;
    if !io_fail::omit_file_sync() {
        file.sync_all()?;
        hit(site, IoPhase::AfterFileSync)?;
    }
    if let Some(parent) = path.parent() {
        if !io_fail::omit_dir_sync() {
            sync_dir(parent)?;
            hit(site, IoPhase::AfterDirSync)?;
        }
    }
    persist_log_ack(path)
}

/// Exclusive acknowledged-length sidecar for `log_path`.
pub fn persist_log_ack(log_path: &Path) -> Result<(), LaneError> {
    let len = fs::metadata(log_path)?.len();
    write_atomic(&log_ack_path(log_path), &len.to_be_bytes(), IoSite::Ack)
}

fn log_ack_path(log_path: &Path) -> std::path::PathBuf {
    log_path.with_extension("ack")
}

/// `sync_all` a regular file.
pub fn sync_path(path: &Path) -> Result<(), LaneError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

/// `sync_all` a directory.
pub fn sync_dir(path: &Path) -> Result<(), LaneError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use residiuum_atomics::AtomicsError;

    fn refused(err: LaneError) -> AtomicRefuseReason {
        match err {
            LaneError::Kernel(AtomicsError::Refused(r)) => r,
            other => panic!("expected kernel refusal, got {other:?}"),
        }
    }

    #[test]
    fn exact_retry_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p");
        write_exclusive(&path, b"same", IoSite::Payload).unwrap();
        write_exclusive(&path, b"same", IoSite::Payload).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"same");
    }

    #[test]
    fn shorter_prefix_identity_is_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p");
        fs::write(&path, b"hello").unwrap();
        assert_eq!(
            refused(write_exclusive(&path, b"hello world", IoSite::Payload).unwrap_err()),
            AtomicRefuseReason::AtomicIdConflict
        );
        assert_eq!(fs::read(&path).unwrap(), b"hello");
        assert!(!dir.path().read_dir().unwrap().any(|e| e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("torn")));
    }

    #[test]
    fn empty_legacy_final_is_damaged_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p");
        fs::write(&path, []).unwrap();
        match write_exclusive(&path, b"intended", IoSite::Payload) {
            Err(LaneError::Corrupt("unauthenticated exclusive final")) => {}
            other => panic!("expected damaged exclusive final, got {other:?}"),
        }
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
    }

    #[test]
    fn oversized_existing_refuses_before_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p");
        let file = File::create(&path).unwrap();
        file.set_len(SidecarRole::Payload.max_bytes() + 1).unwrap();
        drop(file);
        assert_eq!(
            refused(write_exclusive(&path, b"x", IoSite::Payload).unwrap_err()),
            AtomicRefuseReason::LimitExceeded
        );
    }
}
