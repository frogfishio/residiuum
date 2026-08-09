//! Experimental write-time dual streaming for Recovery Shadow (RSHD0004).
//!
//! ```text
//! Cook encoded frame once
//!  ├── append authoritative active
//!  └── append shadow staging (independent allocation)
//! ```
//!
//! At seal: append the same summary to staging, `sync_all` the Shadow
//! independently, atomic-publish, then advance `protected_frontier`.
//!
//! Commitment binds store/segment identity, encoded length, and the ordered
//! list of existing verified frame body hashes plus offset/length/generation —
//! payloads are not re-hashed at seal.

use super::wire::{shadow_dir, shadow_path, ShadowLoad};
use crate::atomic_file;
use crate::error::StoreError;
use crate::layout::StorePaths;
use blake3::Hasher;
use residiuum_format::{FRAME_PREFIX_LEN, FRAME_SUFFIX_LEN, START_MAGIC};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Dual-stream Recovery Shadow magic (frame-hash commitment).
pub const RSH_MAGIC_V4: &[u8; 8] = b"RSHD0004";

/// Envelope before the mirrored segment image (same layout as RSHD0003).
pub const DUAL_ENVELOPE_LEN: usize = 8 + 16 + 16 + 8;

const HASH_LEN: usize = 32;
/// Bound outstanding Shadow staging bytes before a non-sync flush.
const SHADOW_BUF: usize = 1024 * 1024;

/// One frame's identity for the segment-level commitment (no payload bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameCommitMeta {
    /// Frame body hash from the encoded suffix (already computed at cook).
    pub body_hash: [u8; 32],
    /// Absolute offset in the segment image.
    pub offset: u64,
    /// Encoded frame length.
    pub len: u64,
    /// Writer sequence stamped in the frame header.
    pub gen: u64,
}

/// Stage timings for dual-stream finalize (qualification).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DualStreamFinalizeTiming {
    /// Buffered staging-file write operations performed before publication.
    pub staging_write_operations: u64,
    /// Image bytes submitted to the staging file before publication.
    pub staging_write_bytes: u64,
    /// Wall time spent in staging-file writes.
    pub staging_write_ns: u64,
    /// Summary append + buffered flush into staging.
    pub append_summary_ns: u64,
    /// Envelope patch + commitment write.
    pub encode_ns: u64,
    /// `File::sync_all` on the Shadow temp.
    pub file_sync_ns: u64,
    /// Rename temp → published `.rsh`.
    pub rename_ns: u64,
    /// Parent directory sync.
    pub dir_sync_ns: u64,
    /// Bytes in the published Shadow (envelope + image + commitment).
    pub bytes_written: u64,
}

/// True when `bytes` begin with the V4 dual-stream magic.
pub fn is_dual_magic(bytes: &[u8]) -> bool {
    bytes.len() >= 8 && &bytes[0..8] == RSH_MAGIC_V4.as_slice()
}

/// Commitment over identity + ordered frame metas (not payload bytes).
pub fn dual_commitment(
    store_id: &[u8; 16],
    segment_id: &[u8; 16],
    encoded_len: u64,
    metas: &[FrameCommitMeta],
) -> [u8; 32] {
    let mut h = Hasher::new();
    h.update(store_id);
    h.update(segment_id);
    h.update(&encoded_len.to_le_bytes());
    for m in metas {
        h.update(&m.body_hash);
        h.update(&m.offset.to_le_bytes());
        h.update(&m.len.to_le_bytes());
        h.update(&m.gen.to_le_bytes());
    }
    *h.finalize().as_bytes()
}

/// Extract frame commit metas from a frame-aligned image slice.
pub fn metas_from_image_chunk(
    chunk: &[u8],
    base_offset: u64,
) -> Result<Vec<FrameCommitMeta>, StoreError> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos < chunk.len() {
        if chunk.len() - pos < FRAME_PREFIX_LEN {
            return Err(StoreError::CorruptMeta(
                "dual-stream chunk truncated mid-frame prefix",
            ));
        }
        if &chunk[pos..pos + 8] != START_MAGIC.as_slice() {
            return Err(StoreError::CorruptMeta(
                "dual-stream chunk missing RESIDFRM magic",
            ));
        }
        let envelope_len = u32::from_le_bytes(chunk[pos + 12..pos + 16].try_into().unwrap());
        let body_len = u64::from_le_bytes(chunk[pos + 16..pos + 24].try_into().unwrap());
        let gen = u64::from_le_bytes(chunk[pos + 32..pos + 40].try_into().unwrap());
        let frame_len = (FRAME_PREFIX_LEN as u64)
            .checked_add(envelope_len as u64)
            .and_then(|n| n.checked_add(body_len))
            .and_then(|n| n.checked_add(FRAME_SUFFIX_LEN as u64))
            .ok_or(StoreError::CorruptMeta(
                "dual-stream frame length overflow",
            ))?;
        let fl = frame_len as usize;
        if chunk.len() - pos < fl {
            return Err(StoreError::CorruptMeta(
                "dual-stream chunk truncated mid-frame",
            ));
        }
        let suffix = pos + fl - FRAME_SUFFIX_LEN;
        let mut body_hash = [0u8; 32];
        body_hash.copy_from_slice(&chunk[suffix + 16..suffix + 48]);
        out.push(FrameCommitMeta {
            body_hash,
            offset: base_offset + pos as u64,
            len: frame_len,
            gen,
        });
        pos += fl;
    }
    Ok(out)
}

/// Recompute commitment by walking a sealed image (verify path; no payload hash).
pub fn commitment_from_image(
    store_id: &[u8; 16],
    segment_id: &[u8; 16],
    image: &[u8],
) -> Result<[u8; 32], StoreError> {
    let metas = metas_from_image_chunk(image, 0)?;
    Ok(dual_commitment(
        store_id,
        segment_id,
        image.len() as u64,
        &metas,
    ))
}

/// Decode/verify a V4 dual-stream Shadow.
pub fn decode_dual_mirror(
    bytes: &[u8],
    expect_store: Option<[u8; 16]>,
) -> Result<( [u8; 16], [u8; 16], Vec<u8> ), ShadowLoad> {
    if bytes.len() < DUAL_ENVELOPE_LEN + HASH_LEN {
        return Err(if bytes.is_empty() {
            ShadowLoad::Missing
        } else {
            ShadowLoad::Incomplete
        });
    }
    if !is_dual_magic(bytes) {
        return Err(ShadowLoad::Corrupt {
            detail: "not RSHD0004 dual-stream magic".into(),
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
    let encoded_len = u64::from_le_bytes(
        bytes[40..48]
            .try_into()
            .map_err(|_| ShadowLoad::Corrupt {
                detail: "encoded_len truncated".into(),
            })?,
    ) as usize;
    let need = DUAL_ENVELOPE_LEN + encoded_len + HASH_LEN;
    if bytes.len() < need {
        return Err(ShadowLoad::Incomplete);
    }
    if bytes.len() > need {
        return Err(ShadowLoad::Corrupt {
            detail: "trailing garbage after dual-stream commitment".into(),
        });
    }
    let image = bytes[DUAL_ENVELOPE_LEN..DUAL_ENVELOPE_LEN + encoded_len].to_vec();
    let got = &bytes[need - HASH_LEN..];
    let expect = commitment_from_image(&store_id, &segment_id, &image).map_err(|e| {
        ShadowLoad::Corrupt {
            detail: format!("dual-stream image unreadable: {e}"),
        }
    })?;
    if expect.as_slice() != got {
        return Err(ShadowLoad::Corrupt {
            detail: "dual-stream frame-hash commitment mismatch".into(),
        });
    }
    Ok((store_id, segment_id, image))
}

/// Staging writer that mirrors authoritative image bytes independently.
pub struct ShadowDualStream {
    store_id: [u8; 16],
    segment_id: [u8; 16],
    /// Temp path under `recovery/shadow/` (envelope placeholder + image).
    tmp_path: PathBuf,
    file: File,
    /// Bytes written after the envelope placeholder (= image length so far).
    image_len: u64,
    metas: Vec<FrameCommitMeta>,
    buf: Vec<u8>,
    /// Wall time spent appending image bytes (put path; no sync).
    pub append_ns: u64,
    staging_write_operations: u64,
    staging_write_bytes: u64,
    staging_write_ns: u64,
    /// Set when a staging append failed after authoritative bytes landed.
    poisoned: bool,
}

/// Shadow staging fully encoded on disk, awaiting durable sync + rename (P★).
///
/// Produced by [`ShadowDualStream::prepare_async_publish`] on the writer path;
/// consumed by [`publish_prepared_shadow`] on the protection worker.
#[derive(Debug)]
pub struct PreparedShadowPublish {
    /// Store identity.
    pub store_id: [u8; 16],
    /// Segment identity.
    pub segment_id: [u8; 16],
    /// Writer shard that sealed this pair (crash recover must not assume 0).
    pub shard: u16,
    /// Encoded staging path (`*.rsh.dual.tmp`).
    pub tmp_path: PathBuf,
    /// Image length (excluding envelope + commitment).
    pub encoded_len: u64,
    /// Staging write telemetry transferred from the foreground assembler.
    pub staging_write_operations: u64,
    /// Staging bytes written before durable publication.
    pub staging_write_bytes: u64,
    /// Time spent writing staging bytes.
    pub staging_write_ns: u64,
}

impl PreparedShadowPublish {
    /// Sidecar next to staging: `{hex}.rsh.dual.shard` (u16 LE).
    pub fn shard_meta_path(paths: &StorePaths, segment_id: &[u8; 16]) -> PathBuf {
        shadow_dir(paths).join(format!(
            "{}.rsh.dual.shard",
            crate::layout::hex16(segment_id)
        ))
    }

    /// Persist shard for crash recovery of partial pairs.
    pub fn persist_shard_meta(&self, paths: &StorePaths) -> Result<(), StoreError> {
        let path = Self::shard_meta_path(paths, &self.segment_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, self.shard.to_le_bytes())?;
        Ok(())
    }

    /// Load shard sidecar; missing → `None` (legacy / single-shard recover).
    pub fn load_shard_meta(paths: &StorePaths, segment_id: &[u8; 16]) -> Option<u16> {
        let path = Self::shard_meta_path(paths, segment_id);
        let bytes = fs::read(&path).ok()?;
        if bytes.len() != 2 {
            return None;
        }
        Some(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    /// Remove shard sidecar after durable publish or abandon.
    pub fn clear_shard_meta(paths: &StorePaths, segment_id: &[u8; 16]) {
        let _ = fs::remove_file(Self::shard_meta_path(paths, segment_id));
    }
}

impl Drop for PreparedShadowPublish {
    fn drop(&mut self) {
        if !self.tmp_path.as_os_str().is_empty() && self.tmp_path.exists() {
            let _ = fs::remove_file(&self.tmp_path);
        }
    }
}

/// Durable-publish a prepared Shadow staging file (sync → rename → dir sync).
pub fn publish_prepared_shadow(
    mut prepared: PreparedShadowPublish,
    paths: &StorePaths,
) -> Result<DualStreamFinalizeTiming, StoreError> {
    let mut timing = DualStreamFinalizeTiming::default();
    timing.staging_write_operations = prepared.staging_write_operations;
    timing.staging_write_bytes = prepared.staging_write_bytes;
    timing.staging_write_ns = prepared.staging_write_ns;
    let tmp_path = prepared.tmp_path.clone();
    let segment_id = prepared.segment_id;
    let result = (|| -> Result<DualStreamFinalizeTiming, StoreError> {
        let file = OpenOptions::new().read(true).write(true).open(&tmp_path)?;
        let t_sync = Instant::now();
        crate::failpoint::hit("rshd4.finalize.sync")?;
        file.sync_all()?;
        timing.file_sync_ns = t_sync.elapsed().as_nanos() as u64;
        drop(file);

        let final_path = shadow_path(paths, &segment_id);
        let parent = final_path.parent().unwrap_or_else(|| Path::new("."));
        let t_rename = Instant::now();
        crate::failpoint::hit("rshd4.finalize.rename")?;
        crate::media_inventory::rename_exclusive(&tmp_path, &final_path, segment_id)?;
        timing.rename_ns = t_rename.elapsed().as_nanos() as u64;

        let t_dir = Instant::now();
        crate::failpoint::hit("rshd4.finalize.dir_sync")?;
        atomic_file::sync_dir(parent)?;
        timing.dir_sync_ns = t_dir.elapsed().as_nanos() as u64;
        timing.bytes_written =
            DUAL_ENVELOPE_LEN as u64 + prepared.encoded_len + HASH_LEN as u64;
        // Transferred — do not delete on Drop.
        prepared.tmp_path = PathBuf::new();
        PreparedShadowPublish::clear_shard_meta(paths, &segment_id);
        Ok(timing)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp_path);
        prepared.tmp_path = PathBuf::new();
        // Keep shard meta while staging may still exist for recover; if tmp
        // is gone, drop the sidecar too.
        if !tmp_path.exists() {
            PreparedShadowPublish::clear_shard_meta(paths, &segment_id);
        }
    }
    result
}

impl ShadowDualStream {
    /// Open a new staging file with a zero envelope placeholder (independent alloc).
    pub fn begin(
        paths: &StorePaths,
        store_id: [u8; 16],
        segment_id: [u8; 16],
    ) -> Result<Self, StoreError> {
        fs::create_dir_all(shadow_dir(paths))?;
        let final_path = shadow_path(paths, &segment_id);
        let tmp_path = staging_temp_path(&final_path);
        let _ = fs::remove_file(&tmp_path);
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&tmp_path)?;
        // Placeholder envelope; patched at finalize with real encoded_len.
        let mut env = [0u8; DUAL_ENVELOPE_LEN];
        env[0..8].copy_from_slice(RSH_MAGIC_V4);
        env[8..24].copy_from_slice(&store_id);
        env[24..40].copy_from_slice(&segment_id);
        // encoded_len left 0 until finalize
        crate::positioned_io::write_all_at(&mut file, 0, &env)?;
        Ok(Self {
            store_id,
            segment_id,
            tmp_path,
            file,
            image_len: 0,
            metas: Vec::new(),
            buf: Vec::with_capacity(SHADOW_BUF),
            append_ns: 0,
            staging_write_operations: 0,
            staging_write_bytes: 0,
            staging_write_ns: 0,
            poisoned: false,
        })
    }

    /// Bytes mirrored so far (image only, excluding envelope).
    pub fn image_len(&self) -> u64 {
        self.image_len
    }

    /// True when staging diverged from authority (refuse P★ finalize).
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Mark staging unusable for protection claims.
    pub fn poison(&mut self) {
        self.poisoned = true;
    }

    /// Append a frame-aligned image chunk (no sync; bounded buffer flush only).
    pub fn append_image_chunk(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        if chunk.is_empty() {
            return Ok(());
        }
        if self.poisoned {
            return Err(StoreError::CorruptMeta(
                "dual-stream Shadow staging poisoned; refuse further append",
            ));
        }
        crate::failpoint::hit("rshd4.shadow.append")?;
        let t0 = Instant::now();
        let base = self.image_len;
        let mut metas = metas_from_image_chunk(chunk, base)?;
        self.metas.append(&mut metas);
        self.buf.extend_from_slice(chunk);
        self.image_len = self.image_len.saturating_add(chunk.len() as u64);
        if self.buf.len() >= SHADOW_BUF {
            if let Err(e) = self.flush_buf() {
                self.poisoned = true;
                return Err(e);
            }
        }
        self.append_ns = self
            .append_ns
            .saturating_add(t0.elapsed().as_nanos() as u64);
        Ok(())
    }

    fn flush_buf(&mut self) -> Result<(), StoreError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        crate::failpoint::hit("rshd4.shadow.flush")?;
        // Image follows the envelope placeholder.
        let pos = DUAL_ENVELOPE_LEN as u64 + self.image_len - self.buf.len() as u64;
        let started = Instant::now();
        crate::positioned_io::write_all_at(&mut self.file, pos, &self.buf)?;
        self.staging_write_operations = self.staging_write_operations.saturating_add(1);
        self.staging_write_bytes = self
            .staging_write_bytes
            .saturating_add(self.buf.len() as u64);
        self.staging_write_ns = self
            .staging_write_ns
            .saturating_add(started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
        self.buf.clear();
        Ok(())
    }

    /// Encode envelope + commitment on the staging file **without** `sync_all` /
    /// rename. Transfers ownership of the temp path for async durable publish.
    ///
    /// Foreground seal-pair detach: append summary, flush, encode — leave sync
    /// + rename to the protection worker so the writer can open the next pair.
    pub fn prepare_async_publish(
        mut self,
        summary_chunk: &[u8],
        shard: u16,
    ) -> Result<PreparedShadowPublish, StoreError> {
        if self.poisoned {
            return Err(StoreError::CorruptMeta(
                "dual-stream Shadow staging poisoned; refuse P★ prepare",
            ));
        }
        crate::failpoint::hit("rshd4.finalize.summary")?;
        if !summary_chunk.is_empty() {
            self.append_image_chunk(summary_chunk)?;
        }
        self.flush_buf()?;

        let encoded_len = self.image_len;
        let commit = dual_commitment(
            &self.store_id,
            &self.segment_id,
            encoded_len,
            &self.metas,
        );

        let mut env = [0u8; DUAL_ENVELOPE_LEN];
        env[0..8].copy_from_slice(RSH_MAGIC_V4);
        env[8..24].copy_from_slice(&self.store_id);
        env[24..40].copy_from_slice(&self.segment_id);
        env[40..48].copy_from_slice(&encoded_len.to_le_bytes());
        crate::positioned_io::write_all_at(&mut self.file, 0, &env)?;
        let commit_off = DUAL_ENVELOPE_LEN as u64 + encoded_len;
        crate::positioned_io::write_all_at(&mut self.file, commit_off, &commit)?;
        self.file.set_len(commit_off + HASH_LEN as u64)?;

        let out = PreparedShadowPublish {
            store_id: self.store_id,
            segment_id: self.segment_id,
            shard,
            tmp_path: self.tmp_path.clone(),
            encoded_len,
            staging_write_operations: self.staging_write_operations,
            staging_write_bytes: self.staging_write_bytes,
            staging_write_ns: self.staging_write_ns,
        };
        // Prevent Drop from deleting the staging file we just transferred.
        self.tmp_path = PathBuf::new();
        drop(self);
        Ok(out)
    }

    /// Finalize: append summary chunk (if any), patch envelope, commitment, sync, publish.
    pub fn finalize_publish(
        mut self,
        paths: &StorePaths,
        summary_chunk: &[u8],
    ) -> Result<DualStreamFinalizeTiming, StoreError> {
        let tmp_path = self.tmp_path.clone();
        match self.finalize_publish_inner(paths, summary_chunk) {
            Ok(t) => Ok(t),
            Err(e) => {
                let _ = fs::remove_file(&tmp_path);
                Err(e)
            }
        }
    }

    fn finalize_publish_inner(
        &mut self,
        paths: &StorePaths,
        summary_chunk: &[u8],
    ) -> Result<DualStreamFinalizeTiming, StoreError> {
        if self.poisoned {
            return Err(StoreError::CorruptMeta(
                "dual-stream Shadow staging poisoned; refuse P★ finalize",
            ));
        }
        let mut timing = DualStreamFinalizeTiming::default();
        let t_sum = Instant::now();
        crate::failpoint::hit("rshd4.finalize.summary")?;
        if !summary_chunk.is_empty() {
            self.append_image_chunk(summary_chunk)?;
        }
        self.flush_buf()?;
        timing.append_summary_ns = t_sum.elapsed().as_nanos() as u64;
        timing.staging_write_operations = self.staging_write_operations;
        timing.staging_write_bytes = self.staging_write_bytes;
        timing.staging_write_ns = self.staging_write_ns;

        let encoded_len = self.image_len;
        let commit = dual_commitment(
            &self.store_id,
            &self.segment_id,
            encoded_len,
            &self.metas,
        );

        let t_enc = Instant::now();
        // Patch envelope encoded_len.
        let mut env = [0u8; DUAL_ENVELOPE_LEN];
        env[0..8].copy_from_slice(RSH_MAGIC_V4);
        env[8..24].copy_from_slice(&self.store_id);
        env[24..40].copy_from_slice(&self.segment_id);
        env[40..48].copy_from_slice(&encoded_len.to_le_bytes());
        crate::positioned_io::write_all_at(&mut self.file, 0, &env)?;
        // Commitment after image.
        let commit_off = DUAL_ENVELOPE_LEN as u64 + encoded_len;
        crate::positioned_io::write_all_at(&mut self.file, commit_off, &commit)?;
        self.file.set_len(commit_off + HASH_LEN as u64)?;
        timing.encode_ns = t_enc.elapsed().as_nanos() as u64;
        timing.bytes_written = DUAL_ENVELOPE_LEN as u64 + encoded_len + HASH_LEN as u64;

        let t_sync = Instant::now();
        crate::failpoint::hit("rshd4.finalize.sync")?;
        self.file.sync_all()?;
        timing.file_sync_ns = t_sync.elapsed().as_nanos() as u64;

        let final_path = shadow_path(paths, &self.segment_id);
        let parent = final_path.parent().unwrap_or_else(|| Path::new("."));
        let t_rename = Instant::now();
        crate::failpoint::hit("rshd4.finalize.rename")?;
        // Close before rename on platforms that require it.
        // (File drops when self drops after Ok; rename by path is fine while open on unix.)
        crate::media_inventory::rename_exclusive(&self.tmp_path, &final_path, self.segment_id)?;
        timing.rename_ns = t_rename.elapsed().as_nanos() as u64;

        let t_dir = Instant::now();
        crate::failpoint::hit("rshd4.finalize.dir_sync")?;
        atomic_file::sync_dir(parent)?;
        timing.dir_sync_ns = t_dir.elapsed().as_nanos() as u64;
        // Prevent Drop from treating the renamed path as abandoned staging.
        self.tmp_path = PathBuf::new();
        Ok(timing)
    }

}

impl Drop for ShadowDualStream {
    fn drop(&mut self) {
        if !self.tmp_path.as_os_str().is_empty() && self.tmp_path.exists() {
            let _ = fs::remove_file(&self.tmp_path);
        }
    }
}

fn staging_temp_path(final_path: &Path) -> PathBuf {
    let mut name = final_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("segment.rsh")
        .to_string();
    name.push_str(".dual.tmp");
    final_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::StorePaths;
    use residiuum_format::{
        encode_frame, FrameHeader, FrameKind, FrameParts, SegmentId, ActiveSegment, SafetyLimits,
        WIRE_MAJOR, WIRE_MINOR,
    };
    use tempfile::tempdir;

    #[test]
    fn dual_stream_roundtrip_commitment() {
        let dir = tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        paths.create_dirs().unwrap();
        let store = [1u8; 16];
        let seg = [2u8; 16];
        let ids = SegmentId::new(store, seg);
        let mut active = ActiveSegment::create(ids, SafetyLimits::default(), 1).unwrap();
        let env = residiuum_format::EMPTY_ENVELOPE;
        let body = b"dual-stream-payload";
        let header = FrameHeader {
            wire_major: WIRE_MAJOR,
            wire_minor: WIRE_MINOR,
            frame_kind: FrameKind::ItemEvent.as_u8(),
            flags: Default::default(),
            envelope_len: env.len() as u32,
            body_len: body.len() as u64,
            logical_len: body.len() as u64,
            writer_sequence: active.writer_sequence(),
            event_id: [7u8; 16],
        };
        let frame = encode_frame(&FrameParts {
            header,
            envelope: env.to_vec(),
            body: body.to_vec(),
        })
        .unwrap();
        let off = active.append_preencoded_frame(&frame).unwrap();
        assert_eq!(off, active.len() - frame.len() as u64);

        let mut dual = ShadowDualStream::begin(&paths, store, seg).unwrap();
        dual.append_image_chunk(active.as_bytes()).unwrap();
        // Seal: summary would be appended here; use empty for unit test of items-only.
        let timing = dual.finalize_publish(&paths, &[]).unwrap();
        assert!(timing.bytes_written > DUAL_ENVELOPE_LEN as u64);
        assert_eq!(timing.staging_write_operations, 1);
        assert_eq!(timing.staging_write_bytes, active.as_bytes().len() as u64);
        assert!(timing.staging_write_ns > 0);

        let bytes = fs::read(shadow_path(&paths, &seg)).unwrap();
        assert!(is_dual_magic(&bytes));
        let (sid, seg_id, image) = decode_dual_mirror(&bytes, Some(store)).unwrap();
        assert_eq!(sid, store);
        assert_eq!(seg_id, seg);
        assert_eq!(image, active.as_bytes());
    }
}
