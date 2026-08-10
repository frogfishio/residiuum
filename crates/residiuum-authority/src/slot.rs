//! Two-slot durable selector algorithm (`HEAP_SPEC` §35).

use crate::error::AuthorityStoreError;
use residiuum_format::{decode_deterministic_uint_map, encode_deterministic_uint_map, CborValue};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Slot letter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Slot {
    /// `*.a.cbor`
    A,
    /// `*.b.cbor`
    B,
}

impl Slot {
    /// ASCII selector byte (`a` / `b`).
    pub fn as_ascii(self) -> u8 {
        match self {
            Self::A => b'a',
            Self::B => b'b',
        }
    }

    /// Parse selector hint.
    pub fn from_ascii(b: u8) -> Option<Self> {
        match b {
            b'a' | b'A' => Some(Self::A),
            b'b' | b'B' => Some(Self::B),
            _ => None,
        }
    }

    /// Opposite slot (inactive when `self` is current).
    pub fn other(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    /// Filename stem suffix.
    pub fn file_suffix(self) -> &'static str {
        match self {
            Self::A => "a",
            Self::B => "b",
        }
    }
}

/// Wrap payload as `{1: payload, 2: SHA-256(payload)}`.
pub fn encode_slot_file(payload: &[u8]) -> Result<Vec<u8>, AuthorityStoreError> {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    let digest = hasher.finalize();
    encode_deterministic_uint_map(&[
        (1u64, CborValue::Bytes(payload.to_vec())),
        (2, CborValue::Bytes(digest.to_vec())),
    ])
    .map_err(|_| AuthorityStoreError::Corrupt("slot encode"))
}

/// Decode and verify a slot file; return payload bytes.
pub fn decode_slot_file(bytes: &[u8]) -> Result<Vec<u8>, AuthorityStoreError> {
    let map = decode_deterministic_uint_map(bytes)
        .map_err(|_| AuthorityStoreError::Corrupt("slot cbor"))?;
    let mut payload = None;
    let mut digest = None;
    for (k, v) in map {
        match k {
            1 => match v {
                CborValue::Bytes(b) => payload = Some(b),
                _ => return Err(AuthorityStoreError::Corrupt("slot payload")),
            },
            2 => match v {
                CborValue::Bytes(b) if b.len() == 32 => {
                    let mut a = [0u8; 32];
                    a.copy_from_slice(&b);
                    digest = Some(a);
                }
                _ => return Err(AuthorityStoreError::Corrupt("slot digest")),
            },
            _ => return Err(AuthorityStoreError::Corrupt("slot unknown key")),
        }
    }
    let payload = payload.ok_or(AuthorityStoreError::Corrupt("slot missing payload"))?;
    let digest = digest.ok_or(AuthorityStoreError::Corrupt("slot missing digest"))?;
    let mut hasher = Sha256::new();
    hasher.update(&payload);
    let expect = hasher.finalize();
    if expect.as_slice() != digest.as_slice() {
        return Err(AuthorityStoreError::Corrupt("slot digest mismatch"));
    }
    Ok(payload)
}

/// SHA-256 of arbitrary bytes.
pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let d = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&d);
    out
}

/// Read `current` / `time-current` selector hint (`a\n` or `b\n`).
pub fn read_selector(path: &Path) -> Result<Option<Slot>, AuthorityStoreError> {
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = fs::read(path).map_err(|_| AuthorityStoreError::Corrupt("selector read"))?;
    if bytes.len() != 2 || bytes[1] != b'\n' {
        return Err(AuthorityStoreError::Corrupt("selector shape"));
    }
    Ok(Some(
        Slot::from_ascii(bytes[0]).ok_or(AuthorityStoreError::Corrupt("selector letter"))?,
    ))
}

/// Atomically write selector hint.
pub fn write_selector(path: &Path, slot: Slot) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&[slot.as_ascii(), b'\n'])?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        let _ = sync_dir(parent);
    }
    Ok(())
}

/// Write bytes to `path` via temp + rename + dir sync.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        let _ = sync_dir(parent);
    }
    Ok(())
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    let f = fs::File::open(dir)?;
    f.sync_all()
}

/// Path helpers for a named two-slot pair.
pub fn slot_path(dir: &Path, stem: &str, slot: Slot) -> PathBuf {
    dir.join(format!("{stem}.{}.cbor", slot.file_suffix()))
}
