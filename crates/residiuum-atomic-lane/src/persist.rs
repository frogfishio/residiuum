//! Instrumented exclusive / atomic / log persist helpers (CR-ATMR3-008).

use crate::error::LaneError;
use crate::io_fail::{self, IoInjected, IoPhase, IoPoint, IoSite};
use residiuum_atomics::{AtomicRefuseReason, AtomicsError};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;

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

/// Create `path` with `O_EXCL` or accept identical existing bytes. Never replace.
pub fn write_exclusive(path: &Path, bytes: &[u8], site: IoSite) -> Result<(), LaneError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
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
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = fs::read(path)?;
            if existing == bytes {
                Ok(())
            } else {
                Err(AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict).into())
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
