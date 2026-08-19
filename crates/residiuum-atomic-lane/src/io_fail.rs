//! Phase-indexed I/O failpoints for the durable lane (CR-ATMR3-008).
//!
//! Always compiled. Production pays a mutex lookup only after a test has
//! armed a point. Mutants omit a required sync/rename edge so the prefix
//! suite can prove it would detect a broken write protocol.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Mutex, OnceLock};

/// Durable file role that is about to perform I/O.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IoSite {
    /// `heap.id` / `meta`.
    Identity,
    /// Closed-plan sidecar.
    Plan,
    /// Frozen member intent.
    Intent,
    /// Assembled member payload.
    Payload,
    /// Frozen chunk map.
    ChunkManifest,
    /// One chunk body.
    Chunk,
    /// Coordinator log append.
    Coordinator,
    /// Shard member log append.
    Shard,
    /// Acknowledged log-length sidecar.
    Ack,
    /// Recovery checkpoint.
    Checkpoint,
    /// First stable boundary marker.
    Seal,
}

/// Ordered step inside one durable write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IoPhase {
    /// Before any bytes are written.
    BeforeWrite,
    /// Write a proper prefix, then fail (no later sync/rename).
    ShortWrite,
    /// After `write_all` and before `sync_all`.
    AfterWrite,
    /// After the file `sync_all`.
    AfterFileSync,
    /// After file sync, before `rename` (atomic replace only).
    BeforeRename,
    /// After `rename` of the temp file.
    AfterRename,
    /// After parent-directory sync.
    AfterDirSync,
    /// After seal-time log `sync_all`.
    AfterLogSync,
}

/// What the armed point does when visited.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoAction {
    /// Return an I/O error. Upcoming work at this phase is skipped when the
    /// caller checks `hit` *before* the syscall.
    Error,
    /// Write a prefix of the buffer, then fail.
    ShortWrite,
    /// Finish the named phase, then return a process-kill analogue.
    Kill,
}

/// One cell in the I/O prefix matrix.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct IoPoint {
    /// File role.
    pub site: IoSite,
    /// Step inside that write.
    pub phase: IoPhase,
}

impl IoPoint {
    /// Construct a matrix cell.
    pub const fn new(site: IoSite, phase: IoPhase) -> Self {
        Self { site, phase }
    }
}

/// Required ordering edge a mutant may drop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoMutant {
    /// Skip `sync_all` on the written file.
    OmitFileSync,
    /// Leave the temp file and do not publish.
    OmitRename,
    /// Skip parent-directory sync after publish.
    OmitDirSync,
}

/// Injected result from [`hit`] / [`short_write_error`].
#[derive(Debug)]
pub enum IoInjected {
    /// Process-kill analogue after the named phase completed.
    Kill(IoPoint),
    /// I/O-class failure (error or short write).
    Io(io::Error),
}

#[derive(Clone, Copy)]
struct Armed {
    action: IoAction,
    remaining: Option<u32>,
}

static REGISTRY: OnceLock<Mutex<HashMap<IoPoint, Armed>>> = OnceLock::new();
static VISITS: OnceLock<Mutex<HashMap<IoPoint, u64>>> = OnceLock::new();
static MUTANT: AtomicU8 = AtomicU8::new(0);

const MUT_NONE: u8 = 0;
const MUT_FILE_SYNC: u8 = 1;
const MUT_RENAME: u8 = 2;
const MUT_DIR_SYNC: u8 = 3;

fn registry() -> &'static Mutex<HashMap<IoPoint, Armed>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn visits() -> &'static Mutex<HashMap<IoPoint, u64>> {
    VISITS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn record_visit(point: IoPoint) {
    let mut g = visits().lock().expect("io_fail visits");
    *g.entry(point).or_insert(0) += 1;
}

/// Arm `point` until [`disarm_all`].
pub fn arm(point: IoPoint, action: IoAction) {
    registry().lock().expect("io_fail registry").insert(
        point,
        Armed {
            action,
            remaining: None,
        },
    );
}

/// Arm `point` for a single visit.
pub fn arm_once(point: IoPoint, action: IoAction) {
    registry().lock().expect("io_fail registry").insert(
        point,
        Armed {
            action,
            remaining: Some(1),
        },
    );
}

/// Drop every armed point. Does not clear visit counters or mutants.
pub fn disarm_all() {
    registry().lock().expect("io_fail registry").clear();
}

/// Reset visit counters.
pub fn clear_visits() {
    visits().lock().expect("io_fail visits").clear();
}

/// How many times `point` was visited.
pub fn visit_count(point: IoPoint) -> u64 {
    visits()
        .lock()
        .expect("io_fail visits")
        .get(&point)
        .copied()
        .unwrap_or(0)
}

/// Fail if `point` was never visited (unreachable injection must not pass).
pub fn require_visited(point: IoPoint) {
    assert!(
        visit_count(point) > 0,
        "required I/O edge was never visited: {point:?}",
    );
}

/// Enable a write-protocol mutant. Replaces any previous mutant.
pub fn set_mutant(mutant: IoMutant) {
    let v = match mutant {
        IoMutant::OmitFileSync => MUT_FILE_SYNC,
        IoMutant::OmitRename => MUT_RENAME,
        IoMutant::OmitDirSync => MUT_DIR_SYNC,
    };
    MUTANT.store(v, Ordering::SeqCst);
}

/// Clear the write-protocol mutant.
pub fn clear_mutants() {
    MUTANT.store(MUT_NONE, Ordering::SeqCst);
}

/// True when the armed mutant drops file `sync_all`.
pub fn omit_file_sync() -> bool {
    MUTANT.load(Ordering::SeqCst) == MUT_FILE_SYNC
}

/// True when the armed mutant skips `rename`.
pub fn omit_rename() -> bool {
    MUTANT.load(Ordering::SeqCst) == MUT_RENAME
}

/// True when the armed mutant skips parent-directory sync.
pub fn omit_dir_sync() -> bool {
    MUTANT.load(Ordering::SeqCst) == MUT_DIR_SYNC
}

/// Visit `point`. Records the visit even when nothing is armed.
pub fn hit(point: IoPoint) -> Result<(), IoInjected> {
    record_visit(point);
    let mut g = registry().lock().expect("io_fail registry");
    let Some(armed) = g.get_mut(&point) else {
        return Ok(());
    };
    if let Some(left) = armed.remaining.as_mut() {
        if *left == 0 {
            g.remove(&point);
            return Ok(());
        }
        *left -= 1;
        if *left == 0 {
            let action = armed.action;
            g.remove(&point);
            return fire(point, action);
        }
    }
    fire(point, armed.action)
}

fn fire(point: IoPoint, action: IoAction) -> Result<(), IoInjected> {
    match action {
        IoAction::Error => Err(IoInjected::Io(io::Error::new(
            io::ErrorKind::Other,
            format!("injected I/O error {point:?}"),
        ))),
        IoAction::ShortWrite => Ok(()),
        IoAction::Kill => Err(IoInjected::Kill(point)),
    }
}

/// True when this write site should emit a truncated body and fail.
pub fn consume_short_write(site: IoSite) -> bool {
    let point = IoPoint::new(site, IoPhase::ShortWrite);
    record_visit(point);
    let mut g = registry().lock().expect("io_fail registry");
    let Some(armed) = g.get(&point) else {
        return false;
    };
    if armed.action != IoAction::ShortWrite {
        return false;
    }
    if let Some(left) = armed.remaining {
        if left == 0 {
            g.remove(&point);
            return false;
        }
    }
    g.remove(&point);
    true
}

/// Reset failpoints and mutants as a new process image would.
pub fn reset_process() {
    disarm_all();
    clear_mutants();
}

/// Serialize tests that mutate the process-global failpoint tables (CR-ATMR4-008).
pub fn serial_guard() -> std::sync::MutexGuard<'static, ()> {
    static HARNESS: OnceLock<Mutex<()>> = OnceLock::new();
    HARNESS
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// I/O error used after a short write.
pub fn short_write_error(site: IoSite) -> IoInjected {
    IoInjected::Io(io::Error::new(
        io::ErrorKind::WriteZero,
        format!("injected short write {site:?}"),
    ))
}
