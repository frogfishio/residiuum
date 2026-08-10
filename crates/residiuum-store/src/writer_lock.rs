//! Exclusive store writer ownership (DEF-020 / DEF-101).
//!
//! Uses an OS advisory exclusive lock on `store-info/writer.lock` plus an
//! in-process path registry so two writer handles cannot open the same store
//! from one process either. Lock file text is diagnostic only and is never
//! trusted in place of the OS lock — stale PID text cannot block a free lock,
//! and Residiuum never deletes the lock file or kills a process to acquire ownership.

use crate::error::StoreError;
use crate::layout::StorePaths;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Diagnostic-only lock file name under `store-info/`.
pub const WRITER_LOCK_FILE: &str = "writer.lock";

fn held_paths() -> &'static Mutex<HashSet<PathBuf>> {
    static HELD: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    HELD.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Writer-lock status / failure class (DEF-101).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterLockClass {
    /// OS exclusive lock is free (observe only; not an error).
    Free,
    /// Another writer handle is already open for this store root in-process.
    InProcess,
    /// Another process holds the OS exclusive lock.
    ExternalHolder,
    /// Filesystem does not support exclusive advisory locks.
    Unsupported,
    /// I/O failure while opening or locking.
    IoFailure,
}

impl WriterLockClass {
    /// Stable snake_case label for diagnostics / doctor.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Free => "free",
            Self::InProcess => "in_process",
            Self::ExternalHolder => "external_holder",
            Self::Unsupported => "unsupported",
            Self::IoFailure => "io_failure",
        }
    }
}

/// Advisory liveness of the diagnostic PID (never authoritative).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PidLiveness {
    /// Process appears alive (signal 0 succeeded).
    Alive,
    /// Process appears dead (ESRCH).
    Dead,
    /// Could not determine (permissions / platform).
    Unknown,
}

impl PidLiveness {
    /// Stable label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Alive => "alive",
            Self::Dead => "dead",
            Self::Unknown => "unknown",
        }
    }
}

/// Structured writer-lock observation (DEF-101).
///
/// Diagnostic fields are advisory. Only successful OS/in-process ownership
/// establishes writer authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterLockObservation {
    /// Failure / status class.
    pub class: WriterLockClass,
    /// PID parsed from diagnostic lock file text, if any.
    pub diagnostic_pid: Option<u32>,
    /// Advisory liveness of `diagnostic_pid`.
    pub diagnostic_pid_liveness: PidLiveness,
    /// `acquired_ns` from diagnostic file, if any.
    pub diagnostic_acquired_ns: Option<u128>,
    /// Always true: OS exclusive lock is the authority.
    pub os_lock_authoritative: bool,
    /// Time spent waiting for the lock (zero for non-blocking).
    pub waited: Duration,
    /// Whether a later retry may succeed without operator force.
    pub retryable: bool,
    /// Human-readable detail (never recommends deleting writer.lock).
    pub detail: String,
}

impl std::fmt::Display for WriterLockObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} (retryable={}): {}",
            self.class.as_str(),
            self.retryable,
            self.detail
        )
    }
}

/// Options for writer-lock acquisition on open (DEF-101).
#[derive(Debug, Clone)]
pub struct StoreOpenOptions {
    /// When set, poll for the OS lock up to this duration. `None` = non-blocking.
    pub writer_lock_wait: Option<Duration>,
    /// Poll interval while waiting (default 50ms).
    pub writer_lock_poll_interval: Duration,
    /// Optional cancellation flag checked between polls.
    pub cancelled: Option<Arc<AtomicBool>>,
    /// Pre-mutation media inventory policy (P0). Default fail-closed.
    pub inventory_policy: crate::media_inventory::InventoryPolicy,
}

impl Default for StoreOpenOptions {
    fn default() -> Self {
        Self {
            writer_lock_wait: None,
            writer_lock_poll_interval: Duration::from_millis(50),
            cancelled: None,
            inventory_policy: crate::media_inventory::InventoryPolicy::FailClosed,
        }
    }
}

impl StoreOpenOptions {
    /// Non-blocking open (existing default behavior).
    pub fn non_blocking() -> Self {
        Self::default()
    }

    /// Bound wait for the writer lock.
    pub fn wait_for(d: Duration) -> Self {
        Self {
            writer_lock_wait: Some(d),
            ..Self::default()
        }
    }

    /// Attach a cancellation flag (checked between polls).
    pub fn with_cancel(mut self, flag: Arc<AtomicBool>) -> Self {
        self.cancelled = Some(flag);
        self
    }

    /// Salvage / identity-reassign reopen: tolerate foreign-store descriptors
    /// and unscannable media while still refusing segment-id collisions.
    pub fn tolerate_unidentified_inventory(mut self) -> Self {
        self.inventory_policy = crate::media_inventory::InventoryPolicy::TolerateUnidentified;
        self
    }
}

/// Held exclusive writer ownership for one store root.
#[derive(Debug)]
pub struct WriterLock {
    /// Canonical store root used for the in-process registry.
    root: PathBuf,
    /// Open lock file whose OS exclusive lock is held for the handle lifetime.
    file: File,
    path: PathBuf,
}

impl WriterLock {
    /// Acquire exclusive writer ownership (non-blocking).
    pub fn acquire(paths: &StorePaths) -> Result<Self, StoreError> {
        Self::acquire_with_options(paths, &StoreOpenOptions::default())
    }

    /// Acquire exclusive writer ownership with wait / cancel options (DEF-101).
    pub fn acquire_with_options(
        paths: &StorePaths,
        options: &StoreOpenOptions,
    ) -> Result<Self, StoreError> {
        let root = canonicalize_root(&paths.root)?;
        let start = Instant::now();

        // In-process registry first (same process cannot open two writers).
        {
            let mut held = held_paths().lock().unwrap_or_else(|e| e.into_inner());
            if held.contains(&root) {
                let diag = read_diagnostic(&paths.store_info().join(WRITER_LOCK_FILE));
                return Err(StoreError::WriterLockHeld(Box::new(
                    WriterLockObservation {
                        class: WriterLockClass::InProcess,
                        diagnostic_pid: diag.pid.or(Some(std::process::id())),
                        diagnostic_pid_liveness: PidLiveness::Alive,
                        diagnostic_acquired_ns: diag.acquired_ns,
                        os_lock_authoritative: true,
                        waited: start.elapsed(),
                        retryable: false,
                        detail: format!(
                            "another writer handle is already open for {} in this process \
                         (drop the other handle; do not delete writer.lock)",
                            root.display()
                        ),
                    },
                )));
            }
            held.insert(root.clone());
        }

        let lock_path = paths.store_info().join(WRITER_LOCK_FILE);
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let poll = options
            .writer_lock_poll_interval
            .max(Duration::from_millis(1));
        let deadline = options.writer_lock_wait.map(|w| start + w);

        let acquired = loop {
            if options
                .cancelled
                .as_ref()
                .is_some_and(|c| c.load(Ordering::Relaxed))
            {
                break Err(observation_err(
                    WriterLockClass::ExternalHolder,
                    &lock_path,
                    start.elapsed(),
                    true,
                    "writer lock wait cancelled by caller (no store mutation; do not delete writer.lock)",
                ));
            }

            match try_acquire_os(&lock_path) {
                Ok((file, path)) => break Ok((file, path)),
                Err(AcquireOsError::Busy) => {
                    if deadline.is_none() {
                        break Err(observation_err(
                            WriterLockClass::ExternalHolder,
                            &lock_path,
                            start.elapsed(),
                            true,
                            &format!(
                                "another process holds the exclusive OS lock on {} \
                                 (retry later or use open_inspect; do not delete writer.lock)",
                                lock_path.display()
                            ),
                        ));
                    }
                    if Instant::now() >= deadline.unwrap() {
                        break Err(observation_err(
                            WriterLockClass::ExternalHolder,
                            &lock_path,
                            start.elapsed(),
                            true,
                            &format!(
                                "timed out after {:?} waiting for exclusive OS lock on {} \
                                 (lock still held by peer; do not delete writer.lock)",
                                start.elapsed(),
                                lock_path.display()
                            ),
                        ));
                    }
                    std::thread::sleep(poll);
                }
                Err(AcquireOsError::Unsupported(e)) => {
                    break Err(observation_err(
                        WriterLockClass::Unsupported,
                        &lock_path,
                        start.elapsed(),
                        false,
                        &format!(
                            "exclusive advisory lock unsupported on {}: {e} \
                             (in-process registry still applies)",
                            lock_path.display()
                        ),
                    ));
                }
                Err(AcquireOsError::Io(e)) => {
                    break Err(StoreError::WriterLockHeld(Box::new(
                        WriterLockObservation {
                            class: WriterLockClass::IoFailure,
                            diagnostic_pid: read_diagnostic(&lock_path).pid,
                            diagnostic_pid_liveness: PidLiveness::Unknown,
                            diagnostic_acquired_ns: read_diagnostic(&lock_path).acquired_ns,
                            os_lock_authoritative: true,
                            waited: start.elapsed(),
                            retryable: true,
                            detail: format!(
                                "I/O error acquiring writer lock on {}: {e} \
                             (not database absence; do not delete writer.lock to 'fix')",
                                lock_path.display()
                            ),
                        },
                    )));
                }
            }
        };

        match acquired {
            Ok((file, path)) => Ok(Self { root, file, path }),
            Err(e) => {
                let mut held = held_paths().lock().unwrap_or_else(|err| err.into_inner());
                held.remove(&root);
                Err(e)
            }
        }
    }

    /// Path of the diagnostic lock file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Observe lock status without taking long-term ownership (DEF-101).
    ///
    /// Attempts a non-blocking OS lock: if free, acquires briefly to confirm,
    /// rewrites nothing lasting (releases immediately). Never deletes the file.
    pub fn observe(paths: &StorePaths) -> WriterLockObservation {
        let lock_path = paths.store_info().join(WRITER_LOCK_FILE);
        let root = canonicalize_root(&paths.root).unwrap_or_else(|_| paths.root.clone());
        let diag = read_diagnostic(&lock_path);

        {
            let held = held_paths().lock().unwrap_or_else(|e| e.into_inner());
            if held.contains(&root) {
                return WriterLockObservation {
                    class: WriterLockClass::InProcess,
                    diagnostic_pid: diag.pid.or(Some(std::process::id())),
                    diagnostic_pid_liveness: PidLiveness::Alive,
                    diagnostic_acquired_ns: diag.acquired_ns,
                    os_lock_authoritative: true,
                    waited: Duration::ZERO,
                    retryable: false,
                    detail: format!(
                        "writer handle already open in this process for {}",
                        root.display()
                    ),
                };
            }
        }

        match try_acquire_os(&lock_path) {
            Ok((file, _)) => {
                let _ = unlock_exclusive(&file);
                drop(file);
                WriterLockObservation {
                    class: WriterLockClass::Free,
                    diagnostic_pid: diag.pid,
                    diagnostic_pid_liveness: diag
                        .pid
                        .map(probe_pid_liveness)
                        .unwrap_or(PidLiveness::Unknown),
                    diagnostic_acquired_ns: diag.acquired_ns,
                    os_lock_authoritative: true,
                    waited: Duration::ZERO,
                    retryable: true,
                    detail: format!(
                        "OS exclusive lock is free on {} (diagnostic PID text is advisory only; \
                         stale text does not hold the lock)",
                        lock_path.display()
                    ),
                }
            }
            Err(AcquireOsError::Busy) => WriterLockObservation {
                class: WriterLockClass::ExternalHolder,
                diagnostic_pid: diag.pid,
                diagnostic_pid_liveness: diag
                    .pid
                    .map(probe_pid_liveness)
                    .unwrap_or(PidLiveness::Unknown),
                diagnostic_acquired_ns: diag.acquired_ns,
                os_lock_authoritative: true,
                waited: Duration::ZERO,
                retryable: true,
                detail: format!(
                    "OS exclusive lock is held on {} (peer process; do not delete writer.lock)",
                    lock_path.display()
                ),
            },
            Err(AcquireOsError::Unsupported(e)) => WriterLockObservation {
                class: WriterLockClass::Unsupported,
                diagnostic_pid: diag.pid,
                diagnostic_pid_liveness: PidLiveness::Unknown,
                diagnostic_acquired_ns: diag.acquired_ns,
                os_lock_authoritative: true,
                waited: Duration::ZERO,
                retryable: false,
                detail: format!("lock unsupported: {e}"),
            },
            Err(AcquireOsError::Io(e)) => WriterLockObservation {
                class: WriterLockClass::IoFailure,
                diagnostic_pid: diag.pid,
                diagnostic_pid_liveness: PidLiveness::Unknown,
                diagnostic_acquired_ns: diag.acquired_ns,
                os_lock_authoritative: true,
                waited: Duration::ZERO,
                retryable: true,
                detail: format!("I/O observing lock: {e}"),
            },
        }
    }
}

impl Drop for WriterLock {
    fn drop(&mut self) {
        let _ = unlock_exclusive(&self.file);
        let mut held = held_paths().lock().unwrap_or_else(|e| e.into_inner());
        held.remove(&self.root);
    }
}

enum AcquireOsError {
    Busy,
    Unsupported(io::Error),
    Io(io::Error),
}

fn try_acquire_os(lock_path: &Path) -> Result<(File, PathBuf), AcquireOsError> {
    if let Some(parent) = lock_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            return Err(AcquireOsError::Io(e));
        }
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)
        .map_err(AcquireOsError::Io)?;
    match try_lock_exclusive(&file) {
        Ok(()) => {
            if let Err(e) = write_diagnostic(&mut file) {
                let _ = unlock_exclusive(&file);
                return Err(AcquireOsError::Io(match e {
                    StoreError::Io(ioe) => ioe,
                    other => io::Error::other(other.to_string()),
                }));
            }
            Ok((file, lock_path.to_path_buf()))
        }
        Err(e) => {
            let busy = e.kind() == io::ErrorKind::WouldBlock
                || e.raw_os_error() == Some(11) // Linux EAGAIN
                || e.raw_os_error() == Some(35); // macOS EAGAIN
            if busy {
                Err(AcquireOsError::Busy)
            } else if e.raw_os_error() == Some(45) // ENOTSUP
                || e.raw_os_error() == Some(95)
            // ENOTSUP alternate
            {
                Err(AcquireOsError::Unsupported(e))
            } else {
                Err(AcquireOsError::Io(e))
            }
        }
    }
}

fn observation_err(
    class: WriterLockClass,
    lock_path: &Path,
    waited: Duration,
    retryable: bool,
    detail: &str,
) -> StoreError {
    let diag = read_diagnostic(lock_path);
    StoreError::WriterLockHeld(Box::new(WriterLockObservation {
        class,
        diagnostic_pid: diag.pid,
        diagnostic_pid_liveness: diag
            .pid
            .map(probe_pid_liveness)
            .unwrap_or(PidLiveness::Unknown),
        diagnostic_acquired_ns: diag.acquired_ns,
        os_lock_authoritative: true,
        waited,
        retryable,
        detail: detail.to_string(),
    }))
}

struct DiagnosticFile {
    pid: Option<u32>,
    acquired_ns: Option<u128>,
}

fn read_diagnostic(path: &Path) -> DiagnosticFile {
    let mut out = DiagnosticFile {
        pid: None,
        acquired_ns: None,
    };
    let Ok(mut f) = File::open(path) else {
        return out;
    };
    let mut s = String::new();
    if f.read_to_string(&mut s).is_err() {
        return out;
    }
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("pid=") {
            out.pid = rest.trim().parse().ok();
        }
        if let Some(rest) = line.strip_prefix("acquired_ns=") {
            out.acquired_ns = rest.trim().parse().ok();
        }
    }
    out
}

fn probe_pid_liveness(pid: u32) -> PidLiveness {
    #[cfg(unix)]
    {
        // kill(pid, 0) — existence check; ESRCH => dead; EPERM => alive unknown-ish → Alive
        let rc = unsafe { libc_kill(pid as i32, 0) };
        if rc == 0 {
            return PidLiveness::Alive;
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(3) => PidLiveness::Dead,  // ESRCH
            Some(1) => PidLiveness::Alive, // EPERM — exists but not signalable
            _ => PidLiveness::Unknown,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        PidLiveness::Unknown
    }
}

#[cfg(unix)]
extern "C" {
    #[link_name = "kill"]
    fn libc_kill(pid: i32, sig: i32) -> i32;
}

fn canonicalize_root(root: &Path) -> Result<PathBuf, StoreError> {
    match std::fs::canonicalize(root) {
        Ok(p) => Ok(p),
        Err(_) => {
            let abs = if root.is_absolute() {
                root.to_path_buf()
            } else {
                std::env::current_dir()
                    .map(|cwd| cwd.join(root))
                    .unwrap_or_else(|_| root.to_path_buf())
            };
            Ok(abs)
        }
    }
}

fn write_diagnostic(file: &mut File) -> Result<(), StoreError> {
    let pid = std::process::id();
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    file.set_len(0)?;
    writeln!(
        file,
        "residiuum-writer-lock\npid={pid}\nacquired_ns={ns}\nnote=diagnostic-only; OS exclusive lock is authoritative; never delete this file to force unlock\n"
    )?;
    let _ = file.sync_all();
    Ok(())
}

#[cfg(unix)]
fn try_lock_exclusive(file: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;
    let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn unlock_exclusive(file: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    const LOCK_UN: i32 = 8;
    let rc = unsafe { flock(file.as_raw_fd(), LOCK_UN) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(unix)]
extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
}

#[cfg(not(unix))]
fn try_lock_exclusive(file: &File) -> io::Result<()> {
    let _ = file;
    Ok(())
}

#[cfg(not(unix))]
fn unlock_exclusive(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn second_in_process_writer_fails() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("s");
        std::fs::create_dir_all(root.join("store-info")).unwrap();
        let paths = StorePaths::new(&root);
        let _a = WriterLock::acquire(&paths).unwrap();
        let err = WriterLock::acquire(&paths).unwrap_err();
        match err {
            StoreError::WriterLockHeld(obs) => {
                assert_eq!(obs.class, WriterLockClass::InProcess);
                assert!(!obs.retryable);
            }
            other => panic!("expected WriterLockHeld, got {other:?}"),
        }
    }

    #[test]
    fn drop_releases_for_reopen() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("s");
        std::fs::create_dir_all(root.join("store-info")).unwrap();
        let paths = StorePaths::new(&root);
        {
            let _a = WriterLock::acquire(&paths).unwrap();
        }
        let _b = WriterLock::acquire(&paths).unwrap();
    }

    #[test]
    fn stale_diagnostic_text_does_not_block() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("s");
        let info = root.join("store-info");
        std::fs::create_dir_all(&info).unwrap();
        let lock_path = info.join(WRITER_LOCK_FILE);
        std::fs::write(
            &lock_path,
            "residiuum-writer-lock\npid=1\nacquired_ns=0\nnote=stale\n",
        )
        .unwrap();
        let paths = StorePaths::new(&root);
        // OS lock free despite stale PID=1 text.
        let _lock = WriterLock::acquire(&paths).unwrap();
    }

    #[test]
    fn observe_reports_free_when_unlocked() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("s");
        std::fs::create_dir_all(root.join("store-info")).unwrap();
        let paths = StorePaths::new(&root);
        let obs = WriterLock::observe(&paths);
        assert!(obs.os_lock_authoritative);
        assert!(obs.detail.contains("free") || obs.retryable);
    }
}
