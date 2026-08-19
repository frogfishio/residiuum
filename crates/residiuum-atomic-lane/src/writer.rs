//! Exclusive physical writer ownership (CR-ATMR3-007).
//!
//! One process, one `DurableLane` per directory. The OS lock is non-blocking
//! `flock`. An in-process table rejects a second instance that would otherwise
//! succeed on some Unixes when two fds lock the same inode.

use crate::error::LaneError;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

const LOCK_NAME: &str = "writer.lock";

fn held() -> &'static Mutex<HashSet<PathBuf>> {
    static HELD: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    HELD.get_or_init(|| Mutex::new(HashSet::new()))
}

fn lock_key(root: &Path) -> PathBuf {
    fs_canonicalize(root)
}

fn fs_canonicalize(root: &Path) -> PathBuf {
    std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf())
}

/// Held exclusive writer. Not `Clone`. Dropping releases the directory.
#[derive(Debug)]
pub struct LaneWriterGuard {
    key: PathBuf,
    _file: File,
}

impl LaneWriterGuard {
    /// Acquire exclusive ownership of `root` (`writer.lock`).
    pub fn acquire(root: &Path) -> Result<Self, LaneError> {
        std::fs::create_dir_all(root)?;
        let key = lock_key(root);
        {
            let mut table = held().lock().unwrap_or_else(|e| e.into_inner());
            if !table.insert(key.clone()) {
                return Err(LaneError::WriterHeld);
            }
        }
        let path = root.join(LOCK_NAME);
        let file = match OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
        {
            Ok(f) => f,
            Err(e) => {
                release_key(&key);
                return Err(e.into());
            }
        };
        if let Err(e) = flock_exclusive_nb(&file) {
            release_key(&key);
            return Err(e);
        }
        Ok(Self { key, _file: file })
    }
}

impl Drop for LaneWriterGuard {
    fn drop(&mut self) {
        release_key(&self.key);
    }
}

fn release_key(key: &Path) {
    if let Ok(mut table) = held().lock() {
        table.remove(key);
    }
}

#[cfg(unix)]
fn flock_exclusive_nb(file: &File) -> Result<(), LaneError> {
    use std::os::unix::io::AsRawFd;
    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;
    let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    if rc == 0 {
        Ok(())
    } else {
        Err(LaneError::WriterHeld)
    }
}

#[cfg(not(unix))]
fn flock_exclusive_nb(_file: &File) -> Result<(), LaneError> {
    Err(LaneError::Format("writer lock requires unix".into()))
}

#[cfg(unix)]
extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
}
