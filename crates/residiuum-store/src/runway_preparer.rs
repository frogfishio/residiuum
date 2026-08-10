//! Background segment runway preparer (product watermark).
//!
//! Keeps bulk-zeroed bytes ahead of the active write head **off** the put path.
//! Puts only consume already-ready runway and fail closed if exhausted.
//! See `doc/archive/performance-qualification/2026-08-03-lab-notebook/WHAT_DO_WE_DO.md`.

use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::error::StoreError;
use crate::segment_growth;

/// Shared head / zero watermark between the put path and the preparer thread.
#[derive(Debug)]
pub(crate) struct RunwayShared {
    /// Offset through which the file has been bulk-zeroed.
    pub zeroed_thru: AtomicU64,
    /// Latest durable write head published by the put path.
    pub write_head: AtomicU64,
    /// Optional warm floor (thr setup): preparer zeros through at least this offset.
    pub target_floor: AtomicU64,
}

impl RunwayShared {
    pub fn new(zeroed_thru: u64, write_head: u64) -> Arc<Self> {
        Arc::new(Self {
            zeroed_thru: AtomicU64::new(zeroed_thru),
            write_head: AtomicU64::new(write_head),
            target_floor: AtomicU64::new(0),
        })
    }
}

/// Owns the background zeroing thread for one active segment file.
pub(crate) struct RunwayPreparer {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
    shared: Arc<RunwayShared>,
    capacity_bytes: u64,
}

impl RunwayPreparer {
    /// Spawn a preparer that keeps `chunk_bytes` of zeroed runway ahead of `write_head`.
    pub fn start(
        path: PathBuf,
        capacity_bytes: u64,
        chunk_bytes: u64,
        shared: Arc<RunwayShared>,
    ) -> Result<Self, StoreError> {
        if capacity_bytes == 0 || chunk_bytes == 0 {
            return Err(StoreError::CorruptMeta(
                "runway preparer requires capacity_bytes>0 and chunk_bytes>0",
            ));
        }
        let stop = Arc::new(AtomicBool::new(false));
        let stop_w = Arc::clone(&stop);
        let shared_w = Arc::clone(&shared);
        let join = thread::Builder::new()
            .name("wm-runway".into())
            .spawn(move || {
                preparer_loop(path, capacity_bytes, chunk_bytes, shared_w, stop_w);
            })
            .map_err(StoreError::Io)?;
        Ok(Self {
            stop,
            join: Some(join),
            shared,
            capacity_bytes,
        })
    }

    pub fn shared(&self) -> &Arc<RunwayShared> {
        &self.shared
    }

    /// Block until `zeroed_thru >= target` (clamped to capacity) or `timeout`.
    ///
    /// Prefer [`crate::store::Store::warm_segment_runway`] (same-fd) for product
    /// warm; this waits on the preparer's own progress atomics.
    #[allow(dead_code)]
    pub fn wait_until_zeroed(&self, target: u64, timeout: Duration) -> Result<(), StoreError> {
        let want = target.min(self.capacity_bytes);
        self.shared.target_floor.store(want, Ordering::Release);
        let deadline = Instant::now() + timeout;
        loop {
            if self.shared.zeroed_thru.load(Ordering::Acquire) >= want {
                self.shared.target_floor.store(0, Ordering::Release);
                return Ok(());
            }
            if Instant::now() >= deadline {
                self.shared.target_floor.store(0, Ordering::Release);
                return Err(StoreError::CorruptMeta(
                    "runway preparer warm timed out before target zeroed",
                ));
            }
            thread::sleep(Duration::from_millis(2));
        }
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.join.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for RunwayPreparer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn preparer_loop(
    path: PathBuf,
    capacity_bytes: u64,
    chunk_bytes: u64,
    shared: Arc<RunwayShared>,
    stop: Arc<AtomicBool>,
) {
    let mut file: Option<File> = None;
    while !stop.load(Ordering::Acquire) {
        if file.is_none() {
            match OpenOptions::new().read(true).write(true).open(&path) {
                Ok(f) => file = Some(f),
                Err(_) => {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                }
            }
        }
        let Some(ref mut f) = file else {
            continue;
        };
        let head = shared.write_head.load(Ordering::Acquire);
        let floor = shared.target_floor.load(Ordering::Acquire);
        let mut thru = shared.zeroed_thru.load(Ordering::Acquire);
        // Durable bytes are already page-touched by puts — never re-zero behind head.
        if thru < head {
            shared.zeroed_thru.store(head, Ordering::Release);
            thru = head;
        }
        let ahead = head.saturating_add(chunk_bytes);
        let target = ahead.max(floor).min(capacity_bytes);
        if thru >= target {
            thread::sleep(Duration::from_millis(1));
            continue;
        }
        let end = thru.saturating_add(chunk_bytes).min(capacity_bytes);
        if end <= thru {
            thread::sleep(Duration::from_millis(1));
            continue;
        }
        if segment_growth::bulk_zero_range(f, thru, end).is_err() {
            // Transient I/O: drop handle and retry; put path fails closed if starved.
            file = None;
            thread::sleep(Duration::from_millis(5));
            continue;
        }
        shared.zeroed_thru.store(end, Ordering::Release);
    }
}
