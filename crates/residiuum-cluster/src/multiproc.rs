//! Multi-process OS chaos + short soak harness (DEF-041-N).
//!
//! Complements the in-process [`crate::sim`] program (`residiuum-cluster-verify-v1`).
//! This module defines the **history dump contract** shared by the multiproc
//! child binary and integration tests so failures retain seed + ops for replay.
//!
//! Profile: [`MULTIPROC_PROFILE`].
//!
//! Scope of this labor cut:
//! - OS process isolation (spawn/exit/kill) around durable store writers
//! - Rolling restart: clean exit then reopen — no lost acks
//! - Force-kill after recorded acks — acks survive reopen
//! - Cross-process writer lock exclusion
//! - Short CI soak; long soak via `RESIDIUUM_MULTIPROC_LONG_SOAK=1`
//!
//! Residual: full Jepsen PORC against live `serve-cluster` TCP + multi-hour soak.

use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, Write};
use std::path::Path;

/// Profile tag for multiproc verification (DEF-041-N).
pub const MULTIPROC_PROFILE: &str = "residiuum-cluster-multiproc-v1";

/// One client operation recorded by the multiproc child (after durable ack).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiprocOp {
    /// Monotonic op index within the campaign.
    pub index: u64,
    /// Subject key.
    pub subject: String,
    /// Payload bytes (UTF-8 when printable; otherwise lossy string form).
    pub value: String,
    /// Hex event id from the write receipt when available.
    pub event_id_hex: Option<String>,
    /// True only when the child recorded a successful durable ack before exit.
    pub acked: bool,
}

/// Campaign history retained for deterministic replay (seed + ops + notes).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiprocHistory {
    /// Profile tag.
    pub profile: String,
    /// Campaign seed (parent-chosen).
    pub seed: u64,
    /// Store path (diagnostic only).
    pub store: String,
    /// Operations that crossed the durable ack boundary.
    pub ops: Vec<MultiprocOp>,
    /// Free-form events (spawn, kill, reopen, error).
    pub notes: Vec<String>,
}

impl MultiprocHistory {
    /// Start an empty history for `seed` at `store`.
    pub fn new(seed: u64, store: impl Into<String>) -> Self {
        Self {
            profile: MULTIPROC_PROFILE.into(),
            seed,
            store: store.into(),
            ops: Vec::new(),
            notes: Vec::new(),
        }
    }

    /// Append a diagnostic note.
    pub fn note(&mut self, msg: impl Into<String>) {
        self.notes.push(msg.into());
    }

    /// Pretty dump used in test failures (must include seed).
    pub fn dump(&self) -> String {
        format!(
            "profile={} seed={} store={} ops={} notes={:?} history_json={}",
            self.profile,
            self.seed,
            self.store,
            self.ops.len(),
            self.notes,
            serde_json::to_string(self).unwrap_or_else(|_| "<serialize-failed>".into())
        )
    }

    /// Write JSON to `path` (atomic-ish: write then rename via temp).
    pub fn save(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Load history JSON.
    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let bytes = fs::read(path.as_ref())?;
        serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

/// Hex-encode a 16-byte id (no secrets).
pub fn hex16(id: &[u8; 16]) -> String {
    let mut s = String::with_capacity(32);
    for b in id {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn dump_includes_seed_and_roundtrips() {
        let dir = tempdir().unwrap();
        let seed = 0xdef0_0041u64;
        let mut h = MultiprocHistory::new(seed, dir.path().display().to_string());
        h.ops.push(MultiprocOp {
            index: 0,
            subject: "k0".into(),
            value: "v0".into(),
            event_id_hex: Some("aa".repeat(16)),
            acked: true,
        });
        h.note("spawned child");
        let dump = h.dump();
        assert!(
            dump.contains(&format!("seed={seed}")),
            "dump must retain seed for replay: {dump}"
        );
        let p = dir.path().join("h.json");
        h.save(&p).unwrap();
        let loaded = MultiprocHistory::load(&p).unwrap();
        assert_eq!(loaded, h);
        assert_eq!(loaded.profile, MULTIPROC_PROFILE);
    }
}
