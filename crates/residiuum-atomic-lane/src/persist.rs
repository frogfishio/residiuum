//! Instrumented exclusive / atomic / log persist helpers (CR-ATMR3-008).

use crate::error::LaneError;
use crate::io_fail::{self, IoInjected, IoPhase, IoPoint, IoSite};
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
/// A torn leftover (empty or a strict prefix of `bytes`) is quarantined so
/// exact same-ID retry can complete. Shared `.tmp` names are not used
/// (CR-ATMR4-006).
pub fn write_exclusive(path: &Path, bytes: &[u8], site: IoSite) -> Result<(), LaneError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if path.exists() {
        match classify_existing(path, bytes)? {
            Existing::Same => return Ok(()),
            Existing::Conflict => {
                return Err(AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict).into());
            }
            Existing::Torn => quarantine_torn(path)?,
        }
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
        publish_no_replace(&tmp, path, bytes)?;
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
    Torn,
    Conflict,
}

fn classify_existing(path: &Path, intended: &[u8]) -> Result<Existing, LaneError> {
    let existing = fs::read(path)?;
    if existing == intended {
        Ok(Existing::Same)
    } else if is_torn_identity(&existing, intended) {
        Ok(Existing::Torn)
    } else {
        Ok(Existing::Conflict)
    }
}

fn is_torn_identity(existing: &[u8], intended: &[u8]) -> bool {
    existing.is_empty() || (existing.len() < intended.len() && intended.starts_with(existing))
}

fn unique_sidecar_temp(path: &Path) -> PathBuf {
    let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "exclusive".into());
    path.with_file_name(format!(".{name}.{}.{seq}.tmp", std::process::id()))
}

fn quarantine_torn(path: &Path) -> Result<(), LaneError> {
    let dest = path.with_file_name(format!(
        ".{}.torn.{}",
        path.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "exclusive".into()),
        TEMP_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    fs::rename(path, dest)?;
    Ok(())
}

fn publish_no_replace(tmp: &Path, dest: &Path, bytes: &[u8]) -> Result<(), LaneError> {
    match fs::hard_link(tmp, dest) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            match classify_existing(dest, bytes)? {
                Existing::Same => Ok(()),
                Existing::Torn => {
                    quarantine_torn(dest)?;
                    fs::hard_link(tmp, dest)?;
                    Ok(())
                }
                Existing::Conflict => {
                    Err(AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict).into())
                }
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
