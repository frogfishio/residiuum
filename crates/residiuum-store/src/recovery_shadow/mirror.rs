//! RSHD0003 — Recovery Shadow as a canonical segment image mirror.
//!
//! ```text
//! [envelope | exact sealed segment bytes | commitment]
//! ```
//!
//! No value decode / re-encode. Physical buffered copy only (never APFS
//! clone/reflink). Atomic publish still uses `sync_all` → rename → dir sync.

use super::wire::{shadow_dir, shadow_path, DecodedShadow, ShadowLoad, ShadowRecord};
use crate::atomic_file;
use crate::envelope::{decode_item_envelope, EventKind};
use crate::error::StoreError;
use crate::layout::StorePaths;
use blake3::Hasher;
use residiuum_format::{scan_forward, FrameKind, SafetyLimits};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::time::Instant;

/// Current Recovery Shadow magic (canonical segment mirror).
pub const RSH_MAGIC_V3: &[u8; 8] = b"RSHD0003";

/// Envelope length before the mirrored segment image.
/// `magic(8) + store_id(16) + segment_id(16) + encoded_len(8)`.
pub const MIRROR_ENVELOPE_LEN: usize = 8 + 16 + 16 + 8;

const HASH_LEN: usize = 32;
/// Physical copy chunk size (buffered read/write; never clone/reflink).
const COPY_BUF: usize = 1024 * 1024;

/// Verified RSHD0003 artifact (image is an exact sealed `.residiuum` byte image).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirroredShadow {
    /// Owning store.
    pub store_id: [u8; 16],
    /// Segment this Shadow covers.
    pub segment_id: [u8; 16],
    /// Exact canonical segment bytes.
    pub image: Vec<u8>,
}

/// Stage timings for one mirror publish (qualification).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MirrorPublishTiming {
    /// Logical copy chunks submitted to the buffered writer.
    pub copy_operations: u64,
    /// Buffered physical copy of the segment image (read+write+hash).
    pub copy_ns: u64,
    /// Envelope + commitment framing write (usually folded into copy).
    pub encode_ns: u64,
    /// `File::sync_all` on the temp Shadow.
    pub file_sync_ns: u64,
    /// Rename temp → published path.
    pub rename_ns: u64,
    /// Parent directory sync.
    pub dir_sync_ns: u64,
    /// Bytes written (envelope + image + commitment).
    pub bytes_written: u64,
}

/// True when `bytes` begin with the V3 mirror magic.
pub fn is_mirror_magic(bytes: &[u8]) -> bool {
    bytes.len() >= 8 && &bytes[0..8] == RSH_MAGIC_V3.as_slice()
}

/// Load a V3 mirror from disk.
pub fn try_load_mirror(
    path: &Path,
    expect_store: Option<[u8; 16]>,
) -> Result<Result<MirroredShadow, ShadowLoad>, StoreError> {
    if !path.is_file() {
        return Ok(Err(ShadowLoad::Missing));
    }
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Err(ShadowLoad::Missing));
        }
        Err(e) => return Err(StoreError::Io(e)),
    };
    Ok(decode_mirror_to_struct(&bytes, expect_store))
}

/// Decode bytes into [`MirroredShadow`] or a fail-closed [`ShadowLoad`].
pub fn decode_mirror_to_struct(
    bytes: &[u8],
    expect_store: Option<[u8; 16]>,
) -> Result<MirroredShadow, ShadowLoad> {
    if bytes.len() < MIRROR_ENVELOPE_LEN + HASH_LEN {
        return Err(if bytes.is_empty() {
            ShadowLoad::Missing
        } else {
            ShadowLoad::Incomplete
        });
    }
    if !is_mirror_magic(bytes) {
        return Err(ShadowLoad::Corrupt {
            detail: "not RSHD0003 mirror magic".into(),
        });
    }
    let store_id: [u8; 16] = bytes[8..24].try_into().map_err(|_| ShadowLoad::Corrupt {
        detail: "store_id truncated".into(),
    })?;
    if let Some(exp) = expect_store {
        if store_id != exp {
            return Err(ShadowLoad::Corrupt {
                detail: "store_id mismatch".into(),
            });
        }
    }
    let segment_id: [u8; 16] = bytes[24..40].try_into().map_err(|_| ShadowLoad::Corrupt {
        detail: "segment_id truncated".into(),
    })?;
    let encoded_len =
        u64::from_le_bytes(bytes[40..48].try_into().map_err(|_| ShadowLoad::Corrupt {
            detail: "encoded_len truncated".into(),
        })?) as usize;
    let need = MIRROR_ENVELOPE_LEN + encoded_len + HASH_LEN;
    if bytes.len() < need {
        return Err(ShadowLoad::Incomplete);
    }
    if bytes.len() > need {
        return Err(ShadowLoad::Corrupt {
            detail: "trailing garbage after mirror commitment".into(),
        });
    }
    let mut h = Hasher::new();
    h.update(&bytes[..need - HASH_LEN]);
    let got = &bytes[need - HASH_LEN..];
    if h.finalize().as_bytes() != got {
        return Err(ShadowLoad::Corrupt {
            detail: "mirror commitment mismatch".into(),
        });
    }
    Ok(MirroredShadow {
        store_id,
        segment_id,
        image: bytes[MIRROR_ENVELOPE_LEN..MIRROR_ENVELOPE_LEN + encoded_len].to_vec(),
    })
}

/// Project a verified mirror image into the legacy record salvage API.
pub fn mirror_to_decoded_shadow(m: &MirroredShadow) -> DecodedShadow {
    let report = scan_forward(&m.image, SafetyLimits::default());
    let mut records = Vec::new();
    for (_off, frame) in report.verified_frames() {
        if frame.header.known_kind() != Some(FrameKind::ItemEvent) {
            continue;
        }
        let Some(env) = decode_item_envelope(&frame.envelope) else {
            continue;
        };
        let gen = frame.header.writer_sequence;
        match env.event_kind {
            EventKind::Put => records.push(ShadowRecord::Put {
                key: env.subject,
                gen,
                value: frame.body.clone(),
            }),
            EventKind::Delete => records.push(ShadowRecord::Tombstone {
                key: env.subject,
                gen,
            }),
        }
    }
    records.sort_by(|a, b| {
        let (ak, ag) = match a {
            ShadowRecord::Put { key, gen, .. } | ShadowRecord::Tombstone { key, gen } => {
                (key.as_slice(), *gen)
            }
        };
        let (bk, bg) = match b {
            ShadowRecord::Put { key, gen, .. } | ShadowRecord::Tombstone { key, gen } => {
                (key.as_slice(), *gen)
            }
        };
        ak.cmp(bk).then(ag.cmp(&bg))
    });
    DecodedShadow {
        store_id: m.store_id,
        segment_id: m.segment_id,
        generation: 1,
        records,
    }
}

/// Encode a complete V3 mirror blob in memory (tests / small segments).
pub fn encode_mirror_shadow(store_id: [u8; 16], segment_id: [u8; 16], image: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(MIRROR_ENVELOPE_LEN + image.len() + HASH_LEN);
    out.extend_from_slice(RSH_MAGIC_V3);
    out.extend_from_slice(&store_id);
    out.extend_from_slice(&segment_id);
    out.extend_from_slice(&(image.len() as u64).to_le_bytes());
    out.extend_from_slice(image);
    let mut h = Hasher::new();
    h.update(&out);
    out.extend_from_slice(h.finalize().as_bytes());
    out
}

/// Physically copy `image` into a durable V3 Shadow (buffered; no reflink).
///
/// Protocol: temp write (envelope + image + commitment) → `sync_all` → rename →
/// parent directory sync. Protection must not be claimed before this returns.
pub fn publish_mirror_shadow(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: &[u8; 16],
    image: &[u8],
) -> Result<MirrorPublishTiming, StoreError> {
    publish_mirror_shadow_timed(paths, store_id, segment_id, image)
}

/// Same as [`publish_mirror_shadow`] with stage timings.
pub fn publish_mirror_shadow_timed(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: &[u8; 16],
    image: &[u8],
) -> Result<MirrorPublishTiming, StoreError> {
    fs::create_dir_all(shadow_dir(paths))?;
    let path = shadow_path(paths, segment_id);
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp = atomic_file::temp_path_for(&path);

    // Immutable: refuse replace of an existing Shadow for this segment id (P0).
    // Same-path retry: idempotent when bytes already match the intended image;
    // different bytes → typed collision (do not modify media).
    if path.is_file() {
        let existing = fs::read(&path)?;
        let mut content = Hasher::new();
        content.update(RSH_MAGIC_V3);
        content.update(&store_id);
        content.update(segment_id);
        content.update(&(image.len() as u64).to_le_bytes());
        content.update(image);
        let commit = *content.finalize().as_bytes();
        let mut intended = Vec::with_capacity(MIRROR_ENVELOPE_LEN + image.len() + HASH_LEN);
        intended.extend_from_slice(RSH_MAGIC_V3);
        intended.extend_from_slice(&store_id);
        intended.extend_from_slice(segment_id);
        intended.extend_from_slice(&(image.len() as u64).to_le_bytes());
        intended.extend_from_slice(image);
        intended.extend_from_slice(&commit);
        if existing == intended {
            return Ok(MirrorPublishTiming::default());
        }
        return Err(StoreError::SegmentIdCollision {
            segment_id: *segment_id,
            paths: vec![path.clone()],
        });
    }

    let _ = fs::remove_file(&tmp);
    crate::failpoint::hit("atomic.before_tmp_write")?;

    let mut timing = MirrorPublishTiming::default();
    let t_copy = Instant::now();
    {
        // Direct file writes: image is already contiguous in memory — avoid an
        // extra BufWriter memcpy on the hot path.
        let mut file = OpenOptions::new().create_new(true).write(true).open(&tmp)?;
        let mut content = Hasher::new();

        let mut write_part = |part: &[u8]| -> Result<(), StoreError> {
            file.write_all(part)?;
            content.update(part);
            Ok(())
        };

        write_part(RSH_MAGIC_V3)?;
        write_part(&store_id)?;
        write_part(segment_id)?;
        write_part(&(image.len() as u64).to_le_bytes())?;

        // Physical copy of the canonical image (chunked; no clone/reflink).
        let mut offset = 0usize;
        while offset < image.len() {
            let end = (offset + COPY_BUF).min(image.len());
            write_part(&image[offset..end])?;
            timing.copy_operations = timing.copy_operations.saturating_add(1);
            offset = end;
        }

        let commit = *content.finalize().as_bytes();
        file.write_all(&commit)?;
        timing.copy_ns = t_copy.elapsed().as_nanos() as u64;
        timing.encode_ns = 0; // framing folded into copy
        timing.bytes_written = (MIRROR_ENVELOPE_LEN + image.len() + HASH_LEN) as u64;

        let t_sync = Instant::now();
        file.sync_all()?;
        timing.file_sync_ns = t_sync.elapsed().as_nanos() as u64;
    }
    crate::failpoint::hit("atomic.after_tmp_sync")?;

    let t_rename = Instant::now();
    crate::failpoint::hit("atomic.before_rename")?;
    crate::media_inventory::rename_exclusive(&tmp, &path, *segment_id)?;
    crate::failpoint::hit("atomic.after_rename")?;
    timing.rename_ns = t_rename.elapsed().as_nanos() as u64;

    let t_dir = Instant::now();
    atomic_file::sync_dir(parent)?;
    timing.dir_sync_ns = t_dir.elapsed().as_nanos() as u64;
    crate::failpoint::hit("atomic.after_dir_sync")?;
    Ok(timing)
}

/// Buffered physical copy from an on-disk sealed segment into a V3 Shadow.
///
/// Reads and writes through 1 MiB buffers; never uses clonefile/reflink.
pub fn publish_mirror_shadow_from_path(
    paths: &StorePaths,
    store_id: [u8; 16],
    segment_id: &[u8; 16],
    segment_path: &Path,
) -> Result<MirrorPublishTiming, StoreError> {
    let meta = fs::metadata(segment_path)?;
    let encoded_len = meta.len();
    fs::create_dir_all(shadow_dir(paths))?;
    let path = shadow_path(paths, segment_id);
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp = atomic_file::temp_path_for(&path);

    if path.is_file() {
        // Retry is idempotent only when the already-published Shadow verifies
        // and contains the exact current authoritative image. Never convert a
        // same-name corrupt or divergent artifact into a protection claim.
        let existing = try_load_mirror(&path, Some(store_id))?;
        if let Ok(mirror) = existing {
            let authority = fs::read(segment_path)?;
            if mirror.segment_id == *segment_id && mirror.image == authority {
                return Ok(MirrorPublishTiming::default());
            }
        }
        return Err(StoreError::SegmentIdCollision {
            segment_id: *segment_id,
            paths: vec![path.clone(), segment_path.to_path_buf()],
        });
    }

    let _ = fs::remove_file(&tmp);

    crate::failpoint::hit("atomic.before_tmp_write")?;
    let mut timing = MirrorPublishTiming::default();
    let t_copy = Instant::now();
    {
        let src = File::open(segment_path)?;
        let mut reader = BufReader::with_capacity(COPY_BUF, src);
        let dst = OpenOptions::new().create_new(true).write(true).open(&tmp)?;
        let mut w = BufWriter::with_capacity(COPY_BUF, dst);
        let mut content = Hasher::new();

        let mut write_part = |part: &[u8]| -> Result<(), StoreError> {
            w.write_all(part)?;
            content.update(part);
            Ok(())
        };
        write_part(RSH_MAGIC_V3)?;
        write_part(&store_id)?;
        write_part(segment_id)?;
        write_part(&encoded_len.to_le_bytes())?;

        let mut buf = vec![0u8; COPY_BUF];
        let mut remaining = encoded_len;
        while remaining > 0 {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                return Err(StoreError::CorruptMeta(
                    "sealed segment truncated during mirror copy",
                ));
            }
            let take = (n as u64).min(remaining) as usize;
            write_part(&buf[..take])?;
            timing.copy_operations = timing.copy_operations.saturating_add(1);
            remaining -= take as u64;
        }

        let commit = *content.finalize().as_bytes();
        w.write_all(&commit)?;
        w.flush()?;
        timing.copy_ns = t_copy.elapsed().as_nanos() as u64;
        timing.bytes_written = MIRROR_ENVELOPE_LEN as u64 + encoded_len + HASH_LEN as u64;

        let t_sync = Instant::now();
        w.get_mut().sync_all()?;
        timing.file_sync_ns = t_sync.elapsed().as_nanos() as u64;
    }
    crate::failpoint::hit("atomic.after_tmp_sync")?;

    let t_rename = Instant::now();
    crate::failpoint::hit("atomic.before_rename")?;
    crate::media_inventory::rename_exclusive(&tmp, &path, *segment_id)?;
    crate::failpoint::hit("atomic.after_rename")?;
    timing.rename_ns = t_rename.elapsed().as_nanos() as u64;

    let t_dir = Instant::now();
    atomic_file::sync_dir(parent)?;
    timing.dir_sync_ns = t_dir.elapsed().as_nanos() as u64;
    crate::failpoint::hit("atomic.after_dir_sync")?;
    Ok(timing)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::StorePaths;
    use tempfile::tempdir;

    #[test]
    fn mirror_roundtrip_and_commitment() {
        let image = b"canonical-segment-bytes-for-mirror-test".to_vec();
        let store = [9u8; 16];
        let seg = [3u8; 16];
        let bytes = encode_mirror_shadow(store, seg, &image);
        assert!(is_mirror_magic(&bytes));
        let m = decode_mirror_to_struct(&bytes, Some(store)).unwrap();
        assert_eq!(m.image, image);
        assert_eq!(m.segment_id, seg);
    }

    #[test]
    fn publish_mirror_is_durable_path() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store = [1u8; 16];
        let seg = [2u8; 16];
        let image = vec![0xABu8; 4096];
        publish_mirror_shadow(&paths, store, &seg, &image).unwrap();
        let loaded = try_load_mirror(&shadow_path(&paths, &seg), Some(store))
            .unwrap()
            .unwrap();
        assert_eq!(loaded.image, image);
    }

    #[test]
    fn publish_mirror_republish_same_bytes_is_idempotent() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store = [9u8; 16];
        let seg = [7u8; 16];
        let image = vec![0xCDu8; 1024];
        publish_mirror_shadow(&paths, store, &seg, &image).unwrap();
        // Compact / qualify retry must not collide when the intended image matches.
        publish_mirror_shadow(&paths, store, &seg, &image).unwrap();
        let loaded = try_load_mirror(&shadow_path(&paths, &seg), Some(store))
            .unwrap()
            .unwrap();
        assert_eq!(loaded.image, image);
    }

    #[test]
    fn publish_mirror_republish_different_bytes_collides() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store = [3u8; 16];
        let seg = [4u8; 16];
        publish_mirror_shadow(&paths, store, &seg, b"aaa").unwrap();
        let err = publish_mirror_shadow(&paths, store, &seg, b"bbb").unwrap_err();
        assert!(matches!(err, StoreError::SegmentIdCollision { .. }));
    }

    #[test]
    fn publish_mirror_from_path_refuses_corrupt_existing_shadow() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store = [5u8; 16];
        let seg = [6u8; 16];
        let segment_path = dir.path().join("sealed.residiuum");
        fs::write(&segment_path, b"authoritative").unwrap();
        fs::create_dir_all(shadow_dir(&paths)).unwrap();
        fs::write(shadow_path(&paths, &seg), b"corrupt").unwrap();

        let err = publish_mirror_shadow_from_path(&paths, store, &seg, &segment_path).unwrap_err();
        assert!(matches!(err, StoreError::SegmentIdCollision { .. }));
    }

    #[test]
    fn torn_mirror_fails_closed() {
        let image = b"abc";
        let mut bytes = encode_mirror_shadow([1; 16], [2; 16], image);
        bytes.pop();
        assert!(matches!(
            decode_mirror_to_struct(&bytes, None),
            Err(ShadowLoad::Incomplete | ShadowLoad::Corrupt { .. })
        ));
    }
}
