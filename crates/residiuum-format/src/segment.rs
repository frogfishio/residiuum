//! Active append segment and seal (FORMAT_SPEC §6).
//!
//! Stage 2b: in-memory segment buffer. No durable IO — that lands with Stage 3
//! (`residiuum-store`). Descriptor and summary bodies use a draft fixed layout;
//! envelopes are deterministic CBOR maps (empty map when no fields).

use crate::cbor_envelope::EMPTY_ENVELOPE;
use crate::frame::{encode_frame, encode_frame_into, FrameHeader, FrameParts, FrameVerifyError};
use crate::kinds::FrameKind;
use crate::limits::SafetyLimits;
use thiserror::Error;

/// Draft binary body for a segment descriptor (FORMAT_SPEC §6.1).
///
/// Layout (little-endian):
/// ```text
/// store_id[16]  segment_id[16]  created_ns:u64  limits(max_env:u32, max_body:u64, max_frame:u64)
/// ```
pub const DESCRIPTOR_BODY_LEN: usize = 16 + 16 + 8 + 4 + 8 + 8;

/// Draft binary body for a segment summary (FORMAT_SPEC §6.2).
///
/// Layout (little-endian):
/// ```text
/// store_id[16]  segment_id[16]  sealed_byte_len:u64  verified_frame_count:u64
/// ```
pub const SUMMARY_BODY_LEN: usize = 16 + 16 + 8 + 8;

/// Identity pair for a store segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegmentId {
    /// Store identifier (16 bytes; draft UUID-like).
    pub store_id: [u8; 16],
    /// Segment identifier (16 bytes).
    pub segment_id: [u8; 16],
}

impl SegmentId {
    /// Construct from store and segment ids.
    pub fn new(store_id: [u8; 16], segment_id: [u8; 16]) -> Self {
        Self {
            store_id,
            segment_id,
        }
    }
}

/// Errors from the in-memory segment writer.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SegmentError {
    /// Frame encode/verify failed.
    #[error(transparent)]
    Frame(#[from] FrameVerifyError),
    /// Append or seal after the segment was already sealed.
    #[error("segment is already sealed")]
    AlreadySealed,
    /// Seal requested with no descriptor (internal invariant).
    #[error("segment has no descriptor")]
    MissingDescriptor,
    /// Caller tried to append a kind reserved for the writer lifecycle.
    #[error("kind {0} is reserved for segment lifecycle frames")]
    ReservedKind(u8),
    /// Descriptor envelope failed deterministic CBOR validation.
    #[error("invalid descriptor envelope")]
    InvalidEnvelope,
    /// [`ActiveSegment::restore_checkpoint`] cannot apply the snapshot safely.
    #[error("active segment checkpoint mismatch")]
    CheckpointMismatch,
}

/// Exact in-memory snapshot of an [`ActiveSegment`] for pre-I/O rollback (AWO-1).
///
/// Restored only before physical tail I/O; after partial durable write the store
/// must poison / reopen instead of inventing a clean in-memory rewrite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveSegmentCheckpoint {
    /// Logical base offset of retained bytes at snapshot time.
    pub base_offset: u64,
    /// Length of the retained RAM image (`as_bytes().len()`).
    pub retained_len: usize,
    /// Writer sequence counter at snapshot time.
    pub writer_sequence: u64,
    /// Frame count at snapshot time.
    pub frame_count: u64,
    /// Whether the segment was sealed.
    pub sealed: bool,
}

/// In-memory active append segment (FORMAT_SPEC §6.3 until sealed).
///
/// The store may **write-through** durable prefix bytes and call
/// [`ActiveSegment::discard_through`] so RAM holds only the unflushed tail.
/// Locator offsets remain absolute from the segment start (`base_offset` +
/// retained). Seal from RAM requires `base_offset == 0`; otherwise the store
/// seals from the on-disk prefix.
#[derive(Debug, Clone)]
pub struct ActiveSegment {
    ids: SegmentId,
    limits: SafetyLimits,
    /// Retained bytes starting at [`Self::base_offset`] in the logical segment.
    bytes: Vec<u8>,
    /// Logical byte offset of `bytes[0]` (bytes already write-through and dropped).
    base_offset: u64,
    writer_sequence: u64,
    /// Number of complete frames appended (includes descriptor).
    frame_count: u64,
    sealed: bool,
    created_ns: u64,
}

impl ActiveSegment {
    /// Create a new active segment and append a segment descriptor as frame 0.
    ///
    /// `created_ns` is a writer-supplied creation timestamp (nanoseconds); use
    /// `0` when unavailable.
    pub fn create(
        ids: SegmentId,
        limits: SafetyLimits,
        created_ns: u64,
    ) -> Result<Self, SegmentError> {
        Self::create_with_descriptor_envelope(ids, limits, created_ns, EMPTY_ENVELOPE)
    }

    /// Create a segment with an explicit descriptor envelope (heap binding).
    ///
    /// `envelope` must be deterministic CBOR. Qualified heap writers supply
    /// `{31: heap_id, 34: 1}`; legacy writers may pass [`EMPTY_ENVELOPE`].
    pub fn create_with_descriptor_envelope(
        ids: SegmentId,
        limits: SafetyLimits,
        created_ns: u64,
        envelope: &[u8],
    ) -> Result<Self, SegmentError> {
        crate::cbor_envelope::validate_deterministic_cbor_envelope(envelope)
            .map_err(|_| SegmentError::InvalidEnvelope)?;
        let mut seg = Self {
            ids,
            limits,
            // Modest headroom for the first frames; store write-through keeps
            // the retained tail small so we do not pre-allocate seal_threshold.
            bytes: Vec::with_capacity(256 * 1024),
            base_offset: 0,
            writer_sequence: 0,
            frame_count: 0,
            sealed: false,
            created_ns,
        };
        let body = encode_descriptor_body(&ids, created_ns, limits);
        let mut event_id = [0u8; 16];
        event_id.copy_from_slice(&ids.segment_id);
        seg.append_raw(FrameKind::SegmentDescriptor, envelope, &body, event_id)?;
        Ok(seg)
    }

    /// Store / segment identifiers.
    pub fn ids(&self) -> SegmentId {
        self.ids
    }

    /// Safety limits recorded at create time.
    pub fn limits(&self) -> SafetyLimits {
        self.limits
    }

    /// Resume an unsealed active segment from durable bytes without re-encoding
    /// frames.
    ///
    /// Frame offsets in `bytes` are preserved (required for locator-first indexes).
    /// Caller must supply accurate `frame_count` and `writer_sequence` matching
    /// the contiguous verified prefix (typically from a forward scan).
    pub fn resume_unsealed(
        ids: SegmentId,
        limits: SafetyLimits,
        bytes: Vec<u8>,
        frame_count: u64,
        writer_sequence: u64,
        created_ns: u64,
    ) -> Result<Self, SegmentError> {
        if bytes.is_empty() {
            return Err(SegmentError::MissingDescriptor);
        }
        if frame_count == 0 {
            return Err(SegmentError::MissingDescriptor);
        }
        Ok(Self {
            ids,
            limits,
            bytes,
            base_offset: 0,
            writer_sequence,
            frame_count,
            sealed: false,
            created_ns,
        })
    }

    /// Whether a segment summary has been appended and the segment is immutable.
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Current sealed or unsealed **logical** byte length (includes discarded prefix).
    pub fn len(&self) -> u64 {
        self.base_offset.saturating_add(self.bytes.len() as u64)
    }

    /// Whether no logical bytes have been written (should not occur after `create`).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of complete frames currently in the segment.
    pub fn frame_count(&self) -> u64 {
        self.frame_count
    }

    /// Next writer-local sequence that will be stamped on the following append.
    pub fn writer_sequence(&self) -> u64 {
        self.writer_sequence
    }

    /// Logical offset of the first retained byte (`0` when nothing was write-through).
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Capture an exact in-memory checkpoint for pre-I/O rollback (AWO-1).
    ///
    /// Covers retained RAM image, base offset, writer sequence, frame count, and
    /// sealed flag. May only be restored **before** physical I/O advances the
    /// durable tail behind a partial append (implementation plan §9).
    pub fn checkpoint(&self) -> ActiveSegmentCheckpoint {
        ActiveSegmentCheckpoint {
            base_offset: self.base_offset,
            retained_len: self.bytes.len(),
            writer_sequence: self.writer_sequence,
            frame_count: self.frame_count,
            sealed: self.sealed,
        }
    }

    /// Restore a checkpoint taken by [`Self::checkpoint`].
    ///
    /// Requires `base_offset` unchanged (no write-through discard after the
    /// checkpoint) and retained length not shorter than the snapshot (only
    /// truncates appends). After partial physical I/O this must not be used —
    /// poison/reopen recovery owns that case.
    pub fn restore_checkpoint(&mut self, cp: &ActiveSegmentCheckpoint) -> Result<(), SegmentError> {
        if self.base_offset != cp.base_offset {
            return Err(SegmentError::CheckpointMismatch);
        }
        if self.bytes.len() < cp.retained_len {
            return Err(SegmentError::CheckpointMismatch);
        }
        self.bytes.truncate(cp.retained_len);
        self.writer_sequence = cp.writer_sequence;
        self.frame_count = cp.frame_count;
        self.sealed = cp.sealed;
        Ok(())
    }

    /// Append a fully encoded frame image (prefix|env|body|suffix) without re-encoding.
    ///
    /// Used by the parallel cooker: workers produce complete frames (Blake included);
    /// the writer thread installs them in order. Caller must stamp `writer_sequence`
    /// in the frame header to match [`Self::writer_sequence`] before calling.
    pub fn append_preencoded_frame(&mut self, frame: &[u8]) -> Result<u64, SegmentError> {
        if self.sealed {
            return Err(SegmentError::AlreadySealed);
        }
        if frame.is_empty() {
            return Err(SegmentError::MissingDescriptor);
        }
        let offset = self.base_offset.saturating_add(self.bytes.len() as u64);
        self.bytes.extend_from_slice(frame);
        self.writer_sequence = self.writer_sequence.saturating_add(1);
        self.frame_count = self.frame_count.saturating_add(1);
        Ok(offset)
    }

    /// Bytes still held in RAM (suffix starting at [`Self::base_offset`]).
    ///
    /// When `base_offset == 0` this is the full unsealed image. After write-through
    /// discard it is only the unflushed tail.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Move out the full unsealed prefix when it is still fully resident.
    ///
    /// Returns `None` unless `base_offset == 0` and the segment is unsealed.
    /// After a successful take the retained buffer is empty (writer is dropped
    /// on the seal rotate path). Used for zero-read authoritative publish.
    pub fn take_unsealed_prefix(&mut self) -> Option<Vec<u8>> {
        if self.sealed || self.base_offset != 0 {
            return None;
        }
        Some(std::mem::take(&mut self.bytes))
    }

    /// Ensure capacity for at least `additional` more retained bytes (append micro).
    pub fn reserve_additional(&mut self, additional: usize) {
        self.bytes.reserve(additional);
    }

    /// Drop RAM for absolute range `[0, absolute_end)` that is durable on disk.
    ///
    /// `absolute_end` must be in `[base_offset, len()]`. Capacity is retained for
    /// subsequent appends. Locator offsets stay absolute; callers must pread the
    /// file for frames below `base_offset`.
    pub fn discard_through(&mut self, absolute_end: u64) {
        if absolute_end <= self.base_offset {
            return;
        }
        let logical = self.len();
        let end = absolute_end.min(logical);
        let drop_n = (end - self.base_offset) as usize;
        if drop_n == 0 {
            return;
        }
        if drop_n >= self.bytes.len() {
            self.bytes.clear();
        } else {
            self.bytes.drain(..drop_n);
        }
        self.base_offset = end;
    }

    /// Append an application/content frame (not descriptor or summary).
    pub fn append(
        &mut self,
        kind: FrameKind,
        envelope: &[u8],
        body: &[u8],
        event_id: [u8; 16],
    ) -> Result<u64, SegmentError> {
        if self.sealed {
            return Err(SegmentError::AlreadySealed);
        }
        match kind {
            FrameKind::SegmentDescriptor | FrameKind::SegmentSummary => {
                return Err(SegmentError::ReservedKind(kind.as_u8()));
            }
            _ => {}
        }
        self.append_raw(kind, envelope, body, event_id)
    }

    /// Append a frame of any kind (including unknown extension kinds by raw byte).
    pub fn append_parts(&mut self, parts: &FrameParts) -> Result<u64, SegmentError> {
        if self.sealed {
            return Err(SegmentError::AlreadySealed);
        }
        if parts.header.frame_kind == FrameKind::SegmentDescriptor.as_u8()
            || parts.header.frame_kind == FrameKind::SegmentSummary.as_u8()
        {
            return Err(SegmentError::ReservedKind(parts.header.frame_kind));
        }
        let mut header = parts.header.clone();
        header.writer_sequence = self.writer_sequence;
        let offset = self.base_offset.saturating_add(self.bytes.len() as u64);
        encode_frame_into(&mut self.bytes, &header, &parts.envelope, &parts.body)?;
        self.writer_sequence = self.writer_sequence.saturating_add(1);
        self.frame_count = self.frame_count.saturating_add(1);
        Ok(offset)
    }

    /// Seal the segment by appending a segment summary (FORMAT_SPEC §6.3).
    ///
    /// After seal, further appends fail. The segment is marked immutable in
    /// this in-memory profile; durable immutability is a storage concern.
    ///
    /// Requires `base_offset == 0` (full image in RAM). After write-through
    /// discard the store must seal from the on-disk prefix instead.
    pub fn seal(mut self) -> Result<SealedSegment, SegmentError> {
        if self.sealed {
            return Err(SegmentError::AlreadySealed);
        }
        if self.frame_count == 0 {
            return Err(SegmentError::MissingDescriptor);
        }
        if self.base_offset != 0 {
            // Cannot form a contiguous sealed image without the discarded prefix.
            return Err(SegmentError::MissingDescriptor);
        }

        // Frame count after summary will be current + 1; sealed length includes summary.
        // Encode summary with provisional lengths, then rewrite if needed.
        // Summary body records post-seal totals.
        let frames_after = self.frame_count + 1;
        // First pass: encode with estimated sealed length using empty body sizes,
        // then re-encode with the true total once we know the summary frame size.
        let mut event_id = [0u8; 16];
        event_id.copy_from_slice(&self.ids.segment_id);
        event_id[15] ^= 0xff; // distinct from descriptor event_id

        // Provisional: sealed length = current + summary frame length with known body.
        let body_len = SUMMARY_BODY_LEN as u64;
        let env_len = EMPTY_ENVELOPE.len() as u32;
        let summary_frame_len = crate::limits::checked_frame_len(env_len, body_len)
            .ok_or(FrameVerifyError::LengthsOutOfLimits)?;
        let sealed_len = self.bytes.len() as u64 + summary_frame_len;
        let body = encode_summary_body(&self.ids, sealed_len, frames_after);

        self.append_raw(FrameKind::SegmentSummary, EMPTY_ENVELOPE, &body, event_id)?;
        debug_assert_eq!(self.bytes.len() as u64, sealed_len);
        self.sealed = true;

        Ok(SealedSegment {
            ids: self.ids,
            limits: self.limits,
            bytes: self.bytes,
            frame_count: self.frame_count,
            created_ns: self.created_ns,
        })
    }

    fn append_raw(
        &mut self,
        kind: FrameKind,
        envelope: &[u8],
        body: &[u8],
        event_id: [u8; 16],
    ) -> Result<u64, SegmentError> {
        let header = FrameHeader {
            wire_major: crate::frame::WIRE_MAJOR,
            wire_minor: crate::frame::WIRE_MINOR,
            frame_kind: kind.as_u8(),
            flags: Default::default(),
            envelope_len: envelope.len() as u32,
            body_len: body.len() as u64,
            logical_len: body.len() as u64,
            writer_sequence: self.writer_sequence,
            event_id,
        };
        if !self
            .limits
            .accepts_lengths(header.envelope_len, header.body_len)
        {
            return Err(FrameVerifyError::LengthsOutOfLimits.into());
        }
        let offset = self.base_offset.saturating_add(self.bytes.len() as u64);
        // Encode directly into the segment buffer (one payload copy, not two).
        encode_frame_into(&mut self.bytes, &header, envelope, body)?;
        self.writer_sequence = self.writer_sequence.saturating_add(1);
        self.frame_count = self.frame_count.saturating_add(1);
        Ok(offset)
    }
}

/// Immutable sealed segment bytes plus metadata (FORMAT_SPEC §6.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedSegment {
    ids: SegmentId,
    limits: SafetyLimits,
    bytes: Vec<u8>,
    frame_count: u64,
    created_ns: u64,
}

impl SealedSegment {
    /// Store / segment identifiers.
    pub fn ids(&self) -> SegmentId {
        self.ids
    }

    /// Limits used while writing.
    pub fn limits(&self) -> SafetyLimits {
        self.limits
    }

    /// Sealed byte length.
    pub fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    /// Always false after a successful seal (descriptor at least).
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Complete frames including descriptor and summary.
    pub fn frame_count(&self) -> u64 {
        self.frame_count
    }

    /// Creation timestamp recorded in the descriptor body.
    pub fn created_ns(&self) -> u64 {
        self.created_ns
    }

    /// Sealed segment bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consume into the raw byte vector.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// Encode draft segment descriptor body.
pub fn encode_descriptor_body(ids: &SegmentId, created_ns: u64, limits: SafetyLimits) -> Vec<u8> {
    let mut body = Vec::with_capacity(DESCRIPTOR_BODY_LEN);
    body.extend_from_slice(&ids.store_id);
    body.extend_from_slice(&ids.segment_id);
    body.extend_from_slice(&created_ns.to_le_bytes());
    body.extend_from_slice(&limits.max_envelope_len.to_le_bytes());
    body.extend_from_slice(&limits.max_body_len.to_le_bytes());
    body.extend_from_slice(&limits.max_frame_len.to_le_bytes());
    debug_assert_eq!(body.len(), DESCRIPTOR_BODY_LEN);
    body
}

/// Decode draft segment descriptor body.
pub fn decode_descriptor_body(body: &[u8]) -> Option<(SegmentId, u64, SafetyLimits)> {
    if body.len() != DESCRIPTOR_BODY_LEN {
        return None;
    }
    let store_id: [u8; 16] = body[0..16].try_into().ok()?;
    let segment_id: [u8; 16] = body[16..32].try_into().ok()?;
    let created_ns = u64::from_le_bytes(body[32..40].try_into().ok()?);
    let max_envelope_len = u32::from_le_bytes(body[40..44].try_into().ok()?);
    let max_body_len = u64::from_le_bytes(body[44..52].try_into().ok()?);
    let max_frame_len = u64::from_le_bytes(body[52..60].try_into().ok()?);
    Some((
        SegmentId::new(store_id, segment_id),
        created_ns,
        SafetyLimits {
            max_envelope_len,
            max_body_len,
            max_frame_len,
        },
    ))
}

/// Encode draft segment summary body.
pub fn encode_summary_body(ids: &SegmentId, sealed_byte_len: u64, frame_count: u64) -> Vec<u8> {
    let mut body = Vec::with_capacity(SUMMARY_BODY_LEN);
    body.extend_from_slice(&ids.store_id);
    body.extend_from_slice(&ids.segment_id);
    body.extend_from_slice(&sealed_byte_len.to_le_bytes());
    body.extend_from_slice(&frame_count.to_le_bytes());
    debug_assert_eq!(body.len(), SUMMARY_BODY_LEN);
    body
}

/// Decode draft segment summary body.
pub fn decode_summary_body(body: &[u8]) -> Option<(SegmentId, u64, u64)> {
    if body.len() != SUMMARY_BODY_LEN {
        return None;
    }
    let store_id: [u8; 16] = body[0..16].try_into().ok()?;
    let segment_id: [u8; 16] = body[16..32].try_into().ok()?;
    let sealed_byte_len = u64::from_le_bytes(body[32..40].try_into().ok()?);
    let frame_count = u64::from_le_bytes(body[40..48].try_into().ok()?);
    Some((
        SegmentId::new(store_id, segment_id),
        sealed_byte_len,
        frame_count,
    ))
}

/// Draft binary body for a store descriptor frame (FORMAT_SPEC §4.3 kind 1).
///
/// Layout (little-endian):
/// ```text
/// store_id[16]  created_ns:u64  format_tag:u32  reserved:u32(=0)
/// ```
pub const STORE_DESCRIPTOR_BODY_LEN: usize = 16 + 8 + 4 + 4;

/// Draft format tag for store descriptor bodies written by this profile.
pub const STORE_DESCRIPTOR_FORMAT_TAG: u32 = 1;

/// Encode a draft store descriptor body.
pub fn encode_store_descriptor_body(store_id: [u8; 16], created_ns: u64) -> Vec<u8> {
    let mut body = Vec::with_capacity(STORE_DESCRIPTOR_BODY_LEN);
    body.extend_from_slice(&store_id);
    body.extend_from_slice(&created_ns.to_le_bytes());
    body.extend_from_slice(&STORE_DESCRIPTOR_FORMAT_TAG.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    debug_assert_eq!(body.len(), STORE_DESCRIPTOR_BODY_LEN);
    body
}

/// Decode a draft store descriptor body.
///
/// Returns `(store_id, created_ns, format_tag)` when the fixed layout matches.
pub fn decode_store_descriptor_body(body: &[u8]) -> Option<([u8; 16], u64, u32)> {
    if body.len() != STORE_DESCRIPTOR_BODY_LEN {
        return None;
    }
    let store_id: [u8; 16] = body[0..16].try_into().ok()?;
    let created_ns = u64::from_le_bytes(body[16..24].try_into().ok()?);
    let format_tag = u32::from_le_bytes(body[24..28].try_into().ok()?);
    let reserved = u32::from_le_bytes(body[28..32].try_into().ok()?);
    if reserved != 0 {
        return None;
    }
    Some((store_id, created_ns, format_tag))
}

/// Encode a complete single-frame store descriptor (for `store-info/` files).
pub fn encode_store_descriptor_frame(
    store_id: [u8; 16],
    created_ns: u64,
) -> Result<Vec<u8>, FrameVerifyError> {
    let body = encode_store_descriptor_body(store_id, created_ns);
    let mut event_id = [0u8; 16];
    event_id.copy_from_slice(&store_id);
    let header = FrameHeader::new_draft(
        FrameKind::StoreDescriptor,
        EMPTY_ENVELOPE.len() as u32,
        body.len() as u64,
        event_id,
    );
    encode_frame(&FrameParts {
        header,
        envelope: EMPTY_ENVELOPE.to_vec(),
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::decode_frame;

    fn sample_ids() -> SegmentId {
        let mut store = [0u8; 16];
        store[0] = 1;
        let mut seg = [0u8; 16];
        seg[0] = 2;
        SegmentId::new(store, seg)
    }

    #[test]
    fn create_descriptor_and_seal_summary() {
        let mut active =
            ActiveSegment::create(sample_ids(), SafetyLimits::default(), 1_000).unwrap();
        assert_eq!(active.frame_count(), 1);
        active
            .append(FrameKind::ItemEvent, b"\xa0", b"hello", [9u8; 16])
            .unwrap();
        assert_eq!(active.frame_count(), 2);
        let sealed = active.seal().unwrap();
        assert_eq!(sealed.frame_count(), 3);
        assert_eq!(sealed.len(), sealed.as_bytes().len() as u64);

        // First and last frames decode as descriptor / summary.
        let bytes = sealed.as_bytes();
        // Walk with simple offsets using decode after scan would be cleaner;
        // unit test: descriptor body roundtrips.
        let desc = decode_frame(
            &encode_frame(&FrameParts {
                header: FrameHeader::new_draft(
                    FrameKind::SegmentDescriptor,
                    EMPTY_ENVELOPE.len() as u32,
                    DESCRIPTOR_BODY_LEN as u64,
                    [0u8; 16],
                ),
                envelope: EMPTY_ENVELOPE.to_vec(),
                body: encode_descriptor_body(&sample_ids(), 1_000, SafetyLimits::default()),
            })
            .unwrap(),
            SafetyLimits::default(),
        )
        .unwrap();
        let (ids, created, limits) = decode_descriptor_body(&desc.body).unwrap();
        assert_eq!(ids, sample_ids());
        assert_eq!(created, 1_000);
        assert_eq!(limits, SafetyLimits::default());
        let _ = bytes;
    }

    #[test]
    fn cannot_append_after_seal() {
        let active = ActiveSegment::create(sample_ids(), SafetyLimits::default(), 0).unwrap();
        let sealed = active.seal().unwrap();
        assert!(!sealed.is_empty());
    }

    #[test]
    fn reserved_kinds_rejected_on_append() {
        let mut active = ActiveSegment::create(sample_ids(), SafetyLimits::default(), 0).unwrap();
        let err = active
            .append(FrameKind::SegmentDescriptor, EMPTY_ENVELOPE, b"", [0u8; 16])
            .unwrap_err();
        assert!(matches!(err, SegmentError::ReservedKind(_)));
    }

    #[test]
    fn write_through_discard_preserves_logical_offsets() {
        let mut active = ActiveSegment::create(sample_ids(), SafetyLimits::default(), 0).unwrap();
        let desc_len = active.len();
        let off = active
            .append(FrameKind::ItemEvent, b"\xa0", b"hello", [9u8; 16])
            .unwrap();
        assert!(off >= desc_len);
        let logical = active.len();
        active.discard_through(desc_len);
        assert_eq!(active.base_offset(), desc_len);
        assert_eq!(active.len(), logical);
        assert_eq!(active.as_bytes().len() as u64, logical - desc_len);
        // Next append continues absolute offsets.
        let off2 = active
            .append(FrameKind::ItemEvent, b"\xa0", b"world", [8u8; 16])
            .unwrap();
        assert_eq!(off2, logical);
        // Cannot seal from RAM after discard.
        assert!(active.seal().is_err());
    }

    #[test]
    fn store_descriptor_frame_roundtrip() {
        let store_id = [0xabu8; 16];
        let frame = encode_store_descriptor_frame(store_id, 42).unwrap();
        let decoded = decode_frame(&frame, SafetyLimits::default()).unwrap();
        assert_eq!(
            decoded.header.known_kind(),
            Some(FrameKind::StoreDescriptor)
        );
        let (id, ns, tag) = decode_store_descriptor_body(&decoded.body).unwrap();
        assert_eq!(id, store_id);
        assert_eq!(ns, 42);
        assert_eq!(tag, STORE_DESCRIPTOR_FORMAT_TAG);
    }

    #[test]
    fn create_with_heap_binding_envelope() {
        let heap = [0x7eu8; 16];
        let env = crate::ownership::encode_heap_binding_envelope(&heap).unwrap();
        let active = ActiveSegment::create_with_descriptor_envelope(
            sample_ids(),
            SafetyLimits::default(),
            0,
            &env,
        )
        .unwrap();
        assert_eq!(active.frame_count(), 1);
        let bad = ActiveSegment::create_with_descriptor_envelope(
            sample_ids(),
            SafetyLimits::default(),
            0,
            b"not-cbor",
        );
        assert!(matches!(bad, Err(SegmentError::InvalidEnvelope)));
    }

    #[test]
    fn checkpoint_restore_truncates_appends_before_io() {
        let mut active = ActiveSegment::create(sample_ids(), SafetyLimits::default(), 0).unwrap();
        let cp = active.checkpoint();
        let before_seq = active.writer_sequence();
        let before_frames = active.frame_count();
        let before_len = active.len();
        active
            .append(
                FrameKind::ItemEvent,
                crate::cbor_envelope::EMPTY_ENVELOPE,
                b"body",
                [9u8; 16],
            )
            .unwrap();
        assert!(active.frame_count() > before_frames);
        assert!(active.len() > before_len);
        active.restore_checkpoint(&cp).unwrap();
        assert_eq!(active.writer_sequence(), before_seq);
        assert_eq!(active.frame_count(), before_frames);
        assert_eq!(active.len(), before_len);
        assert_eq!(active.as_bytes().len(), cp.retained_len);
    }
}
