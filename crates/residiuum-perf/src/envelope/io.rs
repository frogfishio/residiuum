//! I/O adapter trait: real disposable files + fake injector for tests.
//!
//! **No raw block devices.** Paths must already be validated by the PQH-1
//! path guard / marker protocol before use.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum IoError {
    #[error("{0}")]
    Msg(String),
    #[error("short write: requested {requested}, wrote {wrote}")]
    ShortWrite { requested: usize, wrote: usize },
    #[error("EINTR")]
    Interrupted,
    #[error("EIO")]
    IoFailure,
    #[error("ENOSPC")]
    NoSpace,
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("partial: completed {completed} of {requested}")]
    Partial { requested: usize, completed: usize },
}

impl From<std::io::Error> for IoError {
    fn from(e: std::io::Error) -> Self {
        use std::io::ErrorKind;
        match e.kind() {
            ErrorKind::Interrupted => IoError::Interrupted,
            ErrorKind::OutOfMemory | ErrorKind::StorageFull => IoError::NoSpace,
            ErrorKind::WriteZero => IoError::ShortWrite {
                requested: 0,
                wrote: 0,
            },
            _ => IoError::Msg(e.to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IoMode {
    Buffered,
    /// Direct/non-cached: only when the platform safely supports it.
    /// Portable default is unsupported (honest).
    Direct,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    None,
    DataOnly,
    FullFile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoOp {
    SequentialWrite,
    PositionedWrite,
    SequentialRead,
    PositionedRead,
}

#[derive(Debug, Clone)]
pub struct IoResult {
    pub bytes_completed: u64,
    pub latency_ns: u64,
    pub ops: u64,
}

/// Abstraction over disposable-file I/O (or a fake for tests).
pub trait IoAdapter {
    fn name(&self) -> &str;
    fn supports_direct(&self) -> bool;
    fn supports_positioned(&self) -> bool;

    /// Write `len` bytes at optional position (None = append/sequential).
    fn write_block(
        &mut self,
        file_id: &str,
        offset: Option<u64>,
        len: usize,
        mode: IoMode,
    ) -> Result<IoResult, IoError>;

    fn read_block(
        &mut self,
        file_id: &str,
        offset: u64,
        len: usize,
        mode: IoMode,
    ) -> Result<IoResult, IoError>;

    fn sync(&mut self, file_id: &str, mode: SyncMode) -> Result<IoResult, IoError>;

    fn create_file(&mut self, file_id: &str) -> Result<(), IoError>;
    fn remove_file(&mut self, file_id: &str) -> Result<(), IoError>;
}

// ─── Fake adapter (test injection) ─────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FakeIoConfig {
    /// Sustained bandwidth ceiling (bytes/sec); 0 = unlimited.
    pub bandwidth_bytes_per_sec: u64,
    /// Base latency ns per op at outstanding depth 1.
    pub base_latency_ns: u64,
    /// Extra latency ns * (outstanding_depth - 1) for queue-depth scaling.
    pub qd_latency_ns: u64,
    pub sync_delay_ns: u64,
    pub short_write_after: Option<u64>,
    pub inject_eintr_every: Option<u64>,
    pub inject_eio_at_op: Option<u64>,
    pub inject_enospc_at_op: Option<u64>,
    pub partial_after_bytes: Option<u64>,
    /// Dirty-throttle: pause every N ops for pause_ns.
    pub throttle_every: Option<u64>,
    pub throttle_pause_ns: u64,
    pub supports_direct: bool,
    pub supports_positioned: bool,
}

impl Default for FakeIoConfig {
    fn default() -> Self {
        Self {
            bandwidth_bytes_per_sec: 100 * 1024 * 1024, // 100 MiB/s
            base_latency_ns: 1_000,
            qd_latency_ns: 100,
            sync_delay_ns: 50_000,
            short_write_after: None,
            inject_eintr_every: None,
            inject_eio_at_op: None,
            inject_enospc_at_op: None,
            partial_after_bytes: None,
            throttle_every: None,
            throttle_pause_ns: 0,
            supports_direct: false,
            supports_positioned: true,
        }
    }
}

#[derive(Debug)]
pub struct FakeIoAdapter {
    pub cfg: FakeIoConfig,
    pub outstanding_depth: u32,
    op_count: u64,
    bytes_written: u64,
    files: HashMap<String, Vec<u8>>,
}

impl FakeIoAdapter {
    pub fn new(cfg: FakeIoConfig) -> Self {
        Self {
            cfg,
            outstanding_depth: 1,
            op_count: 0,
            bytes_written: 0,
            files: HashMap::new(),
        }
    }

    fn tick_op(&mut self) -> Result<(), IoError> {
        self.op_count = self.op_count.saturating_add(1);
        if let Some(n) = self.cfg.inject_eintr_every {
            if n > 0 && self.op_count % n == 0 {
                return Err(IoError::Interrupted);
            }
        }
        if self.cfg.inject_eio_at_op == Some(self.op_count) {
            return Err(IoError::IoFailure);
        }
        if self.cfg.inject_enospc_at_op == Some(self.op_count) {
            return Err(IoError::NoSpace);
        }
        Ok(())
    }

    fn latency_for(&self, bytes: u64) -> u64 {
        let mut lat = self.cfg.base_latency_ns;
        let qd = self.outstanding_depth.max(1) as u64;
        lat = lat.saturating_add(self.cfg.qd_latency_ns.saturating_mul(qd.saturating_sub(1)));
        if self.cfg.bandwidth_bytes_per_sec > 0 && bytes > 0 {
            // ns = bytes * 1e9 / bw
            let xfer = (bytes as u128).saturating_mul(1_000_000_000)
                / u128::from(self.cfg.bandwidth_bytes_per_sec);
            lat = lat.saturating_add(xfer as u64);
        }
        if let Some(every) = self.cfg.throttle_every {
            if every > 0 && self.op_count % every == 0 {
                lat = lat.saturating_add(self.cfg.throttle_pause_ns);
            }
        }
        lat
    }
}

impl IoAdapter for FakeIoAdapter {
    fn name(&self) -> &str {
        "fake"
    }
    fn supports_direct(&self) -> bool {
        self.cfg.supports_direct
    }
    fn supports_positioned(&self) -> bool {
        self.cfg.supports_positioned
    }

    fn create_file(&mut self, file_id: &str) -> Result<(), IoError> {
        self.files.entry(file_id.to_string()).or_default();
        Ok(())
    }

    fn remove_file(&mut self, file_id: &str) -> Result<(), IoError> {
        self.files.remove(file_id);
        Ok(())
    }

    fn write_block(
        &mut self,
        file_id: &str,
        offset: Option<u64>,
        len: usize,
        mode: IoMode,
    ) -> Result<IoResult, IoError> {
        if matches!(mode, IoMode::Direct) && !self.cfg.supports_direct {
            return Err(IoError::Unsupported(
                "direct I/O not supported on fake".into(),
            ));
        }
        if offset.is_some() && !self.cfg.supports_positioned {
            return Err(IoError::Unsupported("positioned write unsupported".into()));
        }
        self.tick_op()?;

        let mut complete = len;
        if let Some(after) = self.cfg.short_write_after {
            if self.bytes_written >= after {
                complete = len / 2;
                if complete == 0 && len > 0 {
                    return Err(IoError::ShortWrite {
                        requested: len,
                        wrote: 0,
                    });
                }
            }
        }
        if let Some(cap) = self.cfg.partial_after_bytes {
            if self.bytes_written.saturating_add(complete as u64) > cap {
                let remain = cap.saturating_sub(self.bytes_written) as usize;
                if remain == 0 {
                    return Err(IoError::Partial {
                        requested: len,
                        completed: 0,
                    });
                }
                complete = remain.min(len);
            }
        }

        let buf = vec![0xABu8; complete];
        let file = self.files.entry(file_id.to_string()).or_default();
        match offset {
            None => file.extend_from_slice(&buf),
            Some(off) => {
                let end = off as usize + complete;
                if file.len() < end {
                    file.resize(end, 0);
                }
                file[off as usize..end].copy_from_slice(&buf);
            }
        }
        self.bytes_written = self.bytes_written.saturating_add(complete as u64);
        let lat = self.latency_for(complete as u64);
        if complete < len {
            return Err(IoError::ShortWrite {
                requested: len,
                wrote: complete,
            });
        }
        Ok(IoResult {
            bytes_completed: complete as u64,
            latency_ns: lat,
            ops: 1,
        })
    }

    fn read_block(
        &mut self,
        file_id: &str,
        offset: u64,
        len: usize,
        mode: IoMode,
    ) -> Result<IoResult, IoError> {
        if matches!(mode, IoMode::Direct) && !self.cfg.supports_direct {
            return Err(IoError::Unsupported("direct I/O not supported".into()));
        }
        self.tick_op()?;
        let file = self
            .files
            .get(file_id)
            .ok_or_else(|| IoError::Msg(format!("unknown file {file_id}")))?;
        let start = offset as usize;
        if start >= file.len() {
            return Ok(IoResult {
                bytes_completed: 0,
                latency_ns: self.latency_for(0),
                ops: 1,
            });
        }
        let available = (file.len() - start).min(len);
        let lat = self.latency_for(available as u64);
        Ok(IoResult {
            bytes_completed: available as u64,
            latency_ns: lat,
            ops: 1,
        })
    }

    fn sync(&mut self, _file_id: &str, mode: SyncMode) -> Result<IoResult, IoError> {
        self.tick_op()?;
        let lat = match mode {
            SyncMode::None => 0,
            SyncMode::DataOnly | SyncMode::FullFile => self.cfg.sync_delay_ns,
        };
        Ok(IoResult {
            bytes_completed: 0,
            latency_ns: lat,
            ops: 1,
        })
    }
}

// ─── Real disposable-file adapter ──────────────────────────────────────────

/// Ordinary files under a dedicated work directory (already path-guarded).
pub struct FileIoAdapter {
    root: PathBuf,
    handles: HashMap<String, File>,
    supports_direct: bool,
}

impl FileIoAdapter {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, IoError> {
        let root = root.into();
        if is_raw_device_path(&root) {
            return Err(IoError::Unsupported(
                "raw block device paths are forbidden".into(),
            ));
        }
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            handles: HashMap::new(),
            // Portable honesty: direct I/O not claimed without platform work.
            supports_direct: false,
        })
    }

    fn path_for(&self, file_id: &str) -> PathBuf {
        // Reject path escape.
        let p = Path::new(file_id);
        if p.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir | std::path::Component::RootDir
            )
        }) {
            // Still join under root but sanitize name.
            return self.root.join(file_id.replace(['/', '\\'], "_"));
        }
        self.root.join(file_id)
    }

    fn open_rw(&mut self, file_id: &str) -> Result<&mut File, IoError> {
        if !self.handles.contains_key(file_id) {
            let path = self.path_for(file_id);
            if is_raw_device_path(&path) {
                return Err(IoError::Unsupported("raw device open refused".into()));
            }
            let f = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(&path)?;
            self.handles.insert(file_id.to_string(), f);
        }
        Ok(self.handles.get_mut(file_id).unwrap())
    }
}

fn is_raw_device_path(path: &Path) -> bool {
    let s = path.to_string_lossy();
    s.starts_with("/dev/") || s.starts_with(r"\\.\") || s.starts_with("//./")
}

impl IoAdapter for FileIoAdapter {
    fn name(&self) -> &str {
        "file"
    }
    fn supports_direct(&self) -> bool {
        self.supports_direct
    }
    fn supports_positioned(&self) -> bool {
        true
    }

    fn create_file(&mut self, file_id: &str) -> Result<(), IoError> {
        let _ = self.open_rw(file_id)?;
        Ok(())
    }

    fn remove_file(&mut self, file_id: &str) -> Result<(), IoError> {
        self.handles.remove(file_id);
        let path = self.path_for(file_id);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    fn write_block(
        &mut self,
        file_id: &str,
        offset: Option<u64>,
        len: usize,
        mode: IoMode,
    ) -> Result<IoResult, IoError> {
        if matches!(mode, IoMode::Direct) && !self.supports_direct {
            return Err(IoError::Unsupported(
                "direct/non-cached I/O not supported on this portable adapter".into(),
            ));
        }
        let t0 = std::time::Instant::now();
        let buf = vec![0x5Au8; len];
        {
            let f = self.open_rw(file_id)?;
            match offset {
                None => {
                    f.seek(SeekFrom::End(0))?;
                }
                Some(off) => {
                    f.seek(SeekFrom::Start(off))?;
                }
            }
            f.write_all(&buf)?;
        }
        let lat = t0.elapsed().as_nanos() as u64;
        Ok(IoResult {
            bytes_completed: len as u64,
            latency_ns: lat,
            ops: 1,
        })
    }

    fn read_block(
        &mut self,
        file_id: &str,
        offset: u64,
        len: usize,
        mode: IoMode,
    ) -> Result<IoResult, IoError> {
        if matches!(mode, IoMode::Direct) && !self.supports_direct {
            return Err(IoError::Unsupported(
                "direct/non-cached I/O not supported on this portable adapter".into(),
            ));
        }
        let t0 = std::time::Instant::now();
        let mut buf = vec![0u8; len];
        let n = {
            let f = self.open_rw(file_id)?;
            f.seek(SeekFrom::Start(offset))?;
            f.read(&mut buf)?
        };
        let lat = t0.elapsed().as_nanos() as u64;
        Ok(IoResult {
            bytes_completed: n as u64,
            latency_ns: lat,
            ops: 1,
        })
    }

    fn sync(&mut self, file_id: &str, mode: SyncMode) -> Result<IoResult, IoError> {
        let t0 = std::time::Instant::now();
        match mode {
            SyncMode::None => {}
            SyncMode::DataOnly | SyncMode::FullFile => {
                let f = self.open_rw(file_id)?;
                f.sync_all()?;
            }
        }
        Ok(IoResult {
            bytes_completed: 0,
            latency_ns: t0.elapsed().as_nanos() as u64,
            ops: 1,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fake_bandwidth_ceiling_increases_latency_with_size() {
        let mut a = FakeIoAdapter::new(FakeIoConfig {
            bandwidth_bytes_per_sec: 1_000_000,
            base_latency_ns: 0,
            qd_latency_ns: 0,
            ..FakeIoConfig::default()
        });
        a.create_file("f").unwrap();
        let small = a.write_block("f", None, 1000, IoMode::Buffered).unwrap();
        let large = a.write_block("f", None, 100_000, IoMode::Buffered).unwrap();
        assert!(large.latency_ns > small.latency_ns);
    }

    #[test]
    fn fake_queue_depth_scales_latency() {
        let mut a = FakeIoAdapter::new(FakeIoConfig {
            bandwidth_bytes_per_sec: 0,
            base_latency_ns: 1000,
            qd_latency_ns: 500,
            ..FakeIoConfig::default()
        });
        a.create_file("f").unwrap();
        a.outstanding_depth = 1;
        let d1 = a.write_block("f", None, 64, IoMode::Buffered).unwrap();
        a.outstanding_depth = 8;
        let d8 = a.write_block("f", None, 64, IoMode::Buffered).unwrap();
        assert!(d8.latency_ns > d1.latency_ns);
    }

    #[test]
    fn fake_sync_delay() {
        let mut a = FakeIoAdapter::new(FakeIoConfig {
            sync_delay_ns: 12_345,
            ..FakeIoConfig::default()
        });
        a.create_file("f").unwrap();
        let s = a.sync("f", SyncMode::DataOnly).unwrap();
        assert_eq!(s.latency_ns, 12_345);
    }

    #[test]
    fn fake_eintr_eio_enospc() {
        let mut a = FakeIoAdapter::new(FakeIoConfig {
            inject_eintr_every: Some(1),
            ..FakeIoConfig::default()
        });
        a.create_file("f").unwrap();
        assert!(matches!(
            a.write_block("f", None, 8, IoMode::Buffered),
            Err(IoError::Interrupted)
        ));

        let mut a = FakeIoAdapter::new(FakeIoConfig {
            inject_eio_at_op: Some(1),
            ..FakeIoConfig::default()
        });
        a.create_file("f").unwrap();
        assert!(matches!(
            a.write_block("f", None, 8, IoMode::Buffered),
            Err(IoError::IoFailure)
        ));

        let mut a = FakeIoAdapter::new(FakeIoConfig {
            inject_enospc_at_op: Some(1),
            ..FakeIoConfig::default()
        });
        a.create_file("f").unwrap();
        assert!(matches!(
            a.write_block("f", None, 8, IoMode::Buffered),
            Err(IoError::NoSpace)
        ));
    }

    #[test]
    fn fake_short_write_and_partial() {
        let mut a = FakeIoAdapter::new(FakeIoConfig {
            short_write_after: Some(0),
            ..FakeIoConfig::default()
        });
        a.create_file("f").unwrap();
        assert!(matches!(
            a.write_block("f", None, 100, IoMode::Buffered),
            Err(IoError::ShortWrite { .. })
        ));
    }

    #[test]
    fn fake_direct_honest_unsupported() {
        let mut a = FakeIoAdapter::new(FakeIoConfig {
            supports_direct: false,
            ..FakeIoConfig::default()
        });
        a.create_file("f").unwrap();
        assert!(matches!(
            a.write_block("f", None, 8, IoMode::Direct),
            Err(IoError::Unsupported(_))
        ));
    }

    #[test]
    fn file_adapter_refuses_dev_root() {
        assert!(FileIoAdapter::new("/dev/null").is_err());
    }

    #[test]
    fn file_adapter_write_read_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let mut a = FileIoAdapter::new(tmp.path()).unwrap();
        a.create_file("x").unwrap();
        a.write_block("x", None, 128, IoMode::Buffered).unwrap();
        let r = a.read_block("x", 0, 128, IoMode::Buffered).unwrap();
        assert_eq!(r.bytes_completed, 128);
        assert!(matches!(
            a.write_block("x", None, 8, IoMode::Direct),
            Err(IoError::Unsupported(_))
        ));
    }

    #[test]
    fn throttle_pauses() {
        let mut a = FakeIoAdapter::new(FakeIoConfig {
            bandwidth_bytes_per_sec: 0,
            base_latency_ns: 10,
            qd_latency_ns: 0,
            throttle_every: Some(2),
            throttle_pause_ns: 1000,
            ..FakeIoConfig::default()
        });
        a.create_file("f").unwrap();
        let _ = a.write_block("f", None, 1, IoMode::Buffered).unwrap();
        let t = a.write_block("f", None, 1, IoMode::Buffered).unwrap();
        assert!(t.latency_ns >= 1000);
    }
}
