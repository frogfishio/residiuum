//! Named failpoints for crash-consistency testing (DEF-022 / CSQ-2).
//!
//! Failpoints are **always compiled** but are no-ops unless armed. Production
//! paths pay only a mutex lock + map lookup when any failpoint has ever been
//! armed in the process; when the registry is empty, [`hit`] returns immediately
//! after a cheap empty check (unless hit-proof tracking is enabled).
//!
//! ## Actions
//!
//! - [`Action::Panic`] — process-local crash simulation (catchable with
//!   `catch_unwind` in tests; drop the store handle and reopen).
//! - [`Action::Abort`] — true process death via [`std::process::abort`] (for
//!   multi-process kill harnesses; not catchable).
//! - [`Action::Error`] / [`Action::Return`] — inject [`StoreError::Failpoint`].
//! - [`Action::IoEnospc`] / [`Action::IoPermission`] — inject realistic
//!   [`StoreError::Io`] kinds (filesystem-full / permission loss).
//! - [`Action::ShortWrite`] — consumed by instrumented write sites via
//!   [`consume_short_write`]; partial bytes may land, then the site returns
//!   a short-write I/O error.
//! - [`Action::Cancel`] — cooperative cancellation injection.
//! - [`Action::AllocFail`] — allocator/resource exhaustion injection.
//!
//! Arm with [`arm`] / [`arm_once`]. Clear with [`disarm`] / [`clear`].
//!
//! ## Hit proof (CSQ-2)
//!
//! Unreachable injection must **fail**, not silently pass. Tests enable
//! [`enable_hit_proof`] and use [`visit_count`] / [`require_visited`] /
//! [`is_armed`]. Names are stable identifiers listed in
//! `crash_matrix.v1.json` and `boundaries-v1.json`.

use crate::error::StoreError;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

/// What happens when a named failpoint is hit while armed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Unwind the current thread (crash simulation).
    Panic,
    /// Kill the process immediately (multi-process crash harness).
    Abort,
    /// Return [`StoreError::Failpoint`] to the caller.
    Error,
    /// Same as [`Action::Error`] (matrix synonym for injected I/O failure).
    Return,
    /// Inject `ErrorKind::StorageFull` (ENOSPC-class).
    IoEnospc,
    /// Inject `ErrorKind::PermissionDenied`.
    IoPermission,
    /// Arm a short-write at the next instrumented write site for this name.
    ///
    /// Use [`consume_short_write`] from write paths; [`hit`] treats this as a
    /// no-op so boundary markers can share names with write injection when
    /// needed. Prefer dedicated `*.short_write` names in the matrix.
    ShortWrite,
    /// Cooperative cancellation / shutdown barrier (CSQ-2).
    Cancel,
    /// Allocator / FD / resource exhaustion barrier (CSQ-2).
    AllocFail,
}

#[derive(Debug, Clone)]
struct Armed {
    action: Action,
    /// Remaining hits; `None` means fire every time until disarmed.
    remaining: Option<u32>,
    /// Optional thread scope for parallel-safe in-process qualification.
    thread: Option<std::thread::ThreadId>,
}

static REGISTRY: OnceLock<Mutex<HashMap<&'static str, Armed>>> = OnceLock::new();
static VISITS: OnceLock<Mutex<HashMap<&'static str, u64>>> = OnceLock::new();
static HIT_PROOF: AtomicBool = AtomicBool::new(false);

fn registry() -> &'static Mutex<HashMap<&'static str, Armed>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn visits() -> &'static Mutex<HashMap<&'static str, u64>> {
    VISITS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Enable per-name visit counting for CSQ-2 hit proof (tests only).
pub fn enable_hit_proof() {
    HIT_PROOF.store(true, Ordering::SeqCst);
}

/// Disable visit counting (default production path).
pub fn disable_hit_proof() {
    HIT_PROOF.store(false, Ordering::SeqCst);
}

/// Whether hit-proof visit tracking is enabled.
pub fn hit_proof_enabled() -> bool {
    HIT_PROOF.load(Ordering::SeqCst)
}

fn record_visit(name: &'static str) {
    if !HIT_PROOF.load(Ordering::Relaxed) {
        return;
    }
    let mut g = visits().lock().expect("failpoint visits");
    *g.entry(name).or_insert(0) += 1;
}

/// How many times `name` was visited while hit-proof tracking was enabled.
pub fn visit_count(name: &str) -> u64 {
    let g = visits().lock().expect("failpoint visits");
    g.get(name).copied().unwrap_or(0)
}

/// Clear visit counters (does not clear armed failpoints).
pub fn clear_visits() {
    let mut g = visits().lock().expect("failpoint visits");
    g.clear();
}

/// Assert that `name` was visited at least once (CSQ-2: unreachable = fail).
pub fn require_visited(name: &str) {
    let n = visit_count(name);
    assert!(
        n > 0,
        "failpoint {name} was never reached (visit_count=0); unreachable injection is fail, not pass"
    );
}

/// Whether `name` is currently armed.
pub fn is_armed(name: &str) -> bool {
    let g = registry().lock().expect("failpoint registry");
    g.contains_key(name)
}

/// Arm `name` so the next hits perform `action` until disarmed.
pub fn arm(name: &'static str, action: Action) {
    let mut g = registry().lock().expect("failpoint registry");
    g.insert(
        name,
        Armed {
            action,
            remaining: None,
            thread: None,
        },
    );
}

/// Arm `name` to fire `action` at most `times` times, then auto-disarm.
pub fn arm_once(name: &'static str, action: Action) {
    arm_n(name, action, 1);
}

/// Arm one hit only on the calling thread.
///
/// This preserves process-global failpoints for crash controllers while
/// allowing parallel Rust tests to inject a boundary without stealing an
/// unrelated worker's visit to the same production site.
pub fn arm_once_current_thread(name: &'static str, action: Action) {
    let mut g = registry().lock().expect("failpoint registry");
    g.insert(
        name,
        Armed {
            action,
            remaining: Some(1),
            thread: Some(std::thread::current().id()),
        },
    );
}

/// Arm `name` to fire `action` at most `n` times.
pub fn arm_n(name: &'static str, action: Action, n: u32) {
    let mut g = registry().lock().expect("failpoint registry");
    g.insert(
        name,
        Armed {
            action,
            remaining: Some(n),
            thread: None,
        },
    );
}

/// Remove a single failpoint.
pub fn disarm(name: &str) {
    let mut g = registry().lock().expect("failpoint registry");
    g.remove(name);
}

/// Remove all failpoints (call from test teardown). Does not clear visits.
pub fn clear() {
    let mut g = registry().lock().expect("failpoint registry");
    g.clear();
}

/// Clear arms and visit counters; disable hit-proof tracking.
pub fn clear_all() {
    clear();
    clear_visits();
    disable_hit_proof();
}

/// Whether any failpoint is currently armed.
pub fn any_armed() -> bool {
    let g = registry().lock().expect("failpoint registry");
    !g.is_empty()
}

fn take_action(name: &'static str) -> Option<Action> {
    let mut g = registry().lock().expect("failpoint registry");
    let entry = g.get_mut(name)?;
    if entry
        .thread
        .is_some_and(|thread| thread != std::thread::current().id())
    {
        return None;
    }
    let action = entry.action;
    if let Some(ref mut rem) = entry.remaining {
        if *rem == 0 {
            g.remove(name);
            return None;
        }
        *rem = rem.saturating_sub(1);
        if *rem == 0 {
            g.remove(name);
        }
    }
    Some(action)
}

/// Hit a named failpoint. No-op when not armed or armed only for short-write
/// consumption (use [`consume_short_write`] for those).
///
/// Always records a visit when hit-proof tracking is enabled (CSQ-2).
///
/// # Panics
///
/// Panics when the armed action is [`Action::Panic`] (intentional).
///
/// # Aborts
///
/// Calls [`std::process::abort`] when the armed action is [`Action::Abort`].
pub fn hit(name: &'static str) -> Result<(), StoreError> {
    record_visit(name);
    let Some(action) = take_action(name) else {
        return Ok(());
    };
    match action {
        Action::Panic => panic!("residiuum failpoint: {name}"),
        Action::Abort => {
            // True process death — not catchable with catch_unwind.
            std::process::abort();
        }
        Action::Error | Action::Return => Err(StoreError::Failpoint(name)),
        Action::IoEnospc => Err(StoreError::Io(io::Error::new(
            io::ErrorKind::StorageFull,
            format!("failpoint enospc: {name}"),
        ))),
        Action::IoPermission => Err(StoreError::Io(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("failpoint permission: {name}"),
        ))),
        Action::Cancel => Err(StoreError::Failpoint(name)),
        Action::AllocFail => Err(StoreError::Io(io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("failpoint alloc: {name}"),
        ))),
        // ShortWrite is consumed only via consume_short_write at write sites.
        // If hit() is called on a ShortWrite arm by mistake, re-arm once so
        // the write site can still observe it, and continue.
        Action::ShortWrite => {
            arm_once(name, Action::ShortWrite);
            Ok(())
        }
    }
}

/// If `name` is armed with [`Action::ShortWrite`], consume one hit and return
/// `true` so the write site can truncate the next write.
pub fn consume_short_write(name: &'static str) -> bool {
    record_visit(name);
    let mut g = registry().lock().expect("failpoint registry");
    let Some(entry) = g.get_mut(name) else {
        return false;
    };
    if entry
        .thread
        .is_some_and(|thread| thread != std::thread::current().id())
    {
        return false;
    }
    if entry.action != Action::ShortWrite {
        return false;
    }
    if let Some(ref mut rem) = entry.remaining {
        if *rem == 0 {
            g.remove(name);
            return false;
        }
        *rem = rem.saturating_sub(1);
        if *rem == 0 {
            g.remove(name);
        }
    }
    true
}

/// How many bytes of `len` to actually write under a short-write injection.
///
/// Returns a value in `0..len` when `len > 0` so the write is strictly short.
pub fn short_write_len(len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    if len == 1 {
        return 0;
    }
    len / 2
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    /// Failpoints are process-global; unit tests must not interleave clear/arm/hit.
    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|p| p.into_inner())
    }

    #[test]
    fn unarmed_is_noop() {
        let _g = test_lock();
        clear();
        assert!(hit("test.never_armed").is_ok());
        assert!(!consume_short_write("test.never_armed"));
    }

    #[test]
    fn error_action_returns_failpoint() {
        let _g = test_lock();
        clear();
        arm_once("test.error", Action::Error);
        let err = hit("test.error").unwrap_err();
        assert!(matches!(err, StoreError::Failpoint("test.error")));
        // second hit auto-disarmed
        assert!(hit("test.error").is_ok());
        clear();
    }

    #[test]
    fn current_thread_arm_is_not_stolen_by_parallel_worker() {
        let _g = test_lock();
        clear();
        arm_once_current_thread("test.thread_scoped", Action::Error);
        std::thread::spawn(|| assert!(hit("test.thread_scoped").is_ok()))
            .join()
            .unwrap();
        let err = hit("test.thread_scoped").unwrap_err();
        assert!(matches!(err, StoreError::Failpoint("test.thread_scoped")));
        assert!(hit("test.thread_scoped").is_ok());
        clear();
    }

    #[test]
    fn panic_action_unwinds() {
        let _g = test_lock();
        clear();
        arm_once("test.panic", Action::Panic);
        let r = std::panic::catch_unwind(|| {
            let _ = hit("test.panic");
        });
        assert!(r.is_err());
        clear();
    }

    #[test]
    fn io_enospc_is_storage_full() {
        let _g = test_lock();
        clear();
        arm_once("test.enospc", Action::IoEnospc);
        let err = hit("test.enospc").unwrap_err();
        match err {
            StoreError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::StorageFull),
            other => panic!("expected Io, got {other:?}"),
        }
        clear();
    }

    #[test]
    fn short_write_consume() {
        let _g = test_lock();
        clear();
        arm_once("test.short", Action::ShortWrite);
        assert!(consume_short_write("test.short"));
        assert!(!consume_short_write("test.short"));
        assert_eq!(short_write_len(10), 5);
        assert_eq!(short_write_len(1), 0);
        clear();
    }

    #[test]
    fn io_permission_is_permission_denied() {
        let _g = test_lock();
        clear();
        arm_once("test.perm", Action::IoPermission);
        let err = hit("test.perm").unwrap_err();
        match err {
            StoreError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::PermissionDenied),
            other => panic!("expected Io, got {other:?}"),
        }
        clear();
    }

    #[test]
    fn hit_proof_requires_visit() {
        let _g = test_lock();
        clear_all();
        enable_hit_proof();
        assert_eq!(visit_count("test.proof"), 0);
        assert!(hit("test.proof").is_ok());
        assert_eq!(visit_count("test.proof"), 1);
        require_visited("test.proof");
        clear_all();
    }

    #[test]
    fn cancel_and_alloc_actions() {
        let _g = test_lock();
        clear();
        arm_once("test.cancel", Action::Cancel);
        assert!(matches!(
            hit("test.cancel").unwrap_err(),
            StoreError::Failpoint("test.cancel")
        ));
        arm_once("test.alloc", Action::AllocFail);
        match hit("test.alloc").unwrap_err() {
            StoreError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::OutOfMemory),
            other => panic!("expected OOM Io, got {other:?}"),
        }
        clear();
    }

    #[test]
    fn unreached_arm_stays_armed() {
        let _g = test_lock();
        clear();
        arm_once("test.never", Action::Error);
        assert!(is_armed("test.never"));
        // Do not call hit — arm must remain so matrix drivers can detect miss.
        assert!(is_armed("test.never"));
        clear();
    }
}
