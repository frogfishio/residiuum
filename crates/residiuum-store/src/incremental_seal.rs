//! Incremental seal helpers for zero-scan / zero-read publish plans.
//!
//! **Authority:** the sealed image is `{durable prefix ‖ segment-summary frame}`.
//! Whole-segment BLAKE3 is **derived** (tier placement / segment catalog only);
//! frame CRC + body hashes remain the authoritative corruption detectors.
//! Hot path: [`meta_publish_plan`] — no pending read, hash [`ContentHashState::Pending`]
//! until enrichment. Stream-hash / resident-prefix / write-tail paths remain for
//! recovery experiments (see performance-qualification archives).

use crate::error::StoreError;
use crate::segment_catalog::SegmentSummary;
use crate::tier::TierClass;
use residiuum_format::{
    encode_frame, encode_summary_body, FrameFlags, FrameHeader, FrameKind, FrameParts, SegmentId,
    EMPTY_ENVELOPE, SUMMARY_BODY_LEN, WIRE_MAJOR, WIRE_MINOR,
};

/// Derived whole-segment BLAKE3 state (never a magic all-zero digest).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContentHashState {
    /// Sealed image published; digest not computed yet (enrichment pending).
    Pending,
    /// BLAKE3-256 of the sealed file bytes.
    Known([u8; 32]),
}

impl ContentHashState {
    /// Known digest wrapper.
    pub fn known(hash: [u8; 32]) -> Self {
        Self::Known(hash)
    }

    /// Whether enrichment has not yet supplied a digest.
    pub fn is_pending(self) -> bool {
        matches!(self, Self::Pending)
    }

    /// Known digest bytes, if present.
    pub fn known_hash(self) -> Option<[u8; 32]> {
        match self {
            Self::Pending => None,
            Self::Known(h) => Some(h),
        }
    }

    /// Wire: `tag(u8) ‖ digest[32]` — tag `0` = Pending, `1` = Known.
    pub fn encode_wire(self, out: &mut Vec<u8>) {
        match self {
            Self::Pending => {
                out.push(0);
                out.extend_from_slice(&[0u8; 32]);
            }
            Self::Known(h) => {
                out.push(1);
                out.extend_from_slice(&h);
            }
        }
    }

    /// Decode one wire record; returns `(state, bytes_consumed)`.
    pub fn decode_wire(bytes: &[u8]) -> Option<(Self, usize)> {
        if bytes.len() < 33 {
            return None;
        }
        match bytes[0] {
            0 => Some((Self::Pending, 33)),
            1 => {
                let h: [u8; 32] = bytes[1..33].try_into().ok()?;
                Some((Self::Known(h), 33))
            }
            _ => None,
        }
    }
}

/// Compact publish plan produced at rotation (no sealed image `Vec`).
#[derive(Debug, Clone)]
pub struct SealPublishPlan {
    /// Encoded segment-summary frame to append (small; not the segment body).
    pub summary_frame: Vec<u8>,
    /// Derived whole-segment digest state.
    pub content_hash: ContentHashState,
    /// Total sealed byte length including summary.
    pub sealed_len: u64,
    /// Frame count including the summary frame.
    pub frame_count: u64,
    /// Item-event frames observed before summary.
    pub item_events: u64,
    /// Segment id.
    pub segment_id: [u8; 16],
}

impl SealPublishPlan {
    /// Constant-time catalog summary (no frame scan).
    pub fn to_segment_summary(&self) -> SegmentSummary {
        SegmentSummary {
            segment_id: self.segment_id,
            tier: TierClass::Hot,
            size: self.sealed_len,
            content_hash: self.content_hash,
            verified_frames: self.frame_count,
            item_events: self.item_events,
            holes: 0,
            available: true,
        }
    }

    /// Whether whole-segment BLAKE3 is still pending (derived).
    pub fn hash_pending(&self) -> bool {
        self.content_hash.is_pending()
    }
}

/// Authoritative meta publish plan: summary footer only, **no** pending read/hash.
///
/// `content_hash` is [`ContentHashState::Pending`]; enrichment computes BLAKE3 later.
pub fn meta_publish_plan(
    ids: SegmentId,
    prefix_len: u64,
    frame_count: u64,
    writer_sequence: u64,
    item_events: u64,
) -> Result<SealPublishPlan, StoreError> {
    build_publish_plan(
        ids,
        prefix_len,
        frame_count,
        writer_sequence,
        item_events,
        ContentHashState::Pending,
        None,
    )
}

fn build_publish_plan(
    ids: SegmentId,
    prefix_len: u64,
    frame_count: u64,
    writer_sequence: u64,
    item_events: u64,
    content_hash: ContentHashState,
    mut hasher: Option<blake3::Hasher>,
) -> Result<SealPublishPlan, StoreError> {
    if frame_count == 0 {
        return Err(StoreError::CorruptMeta(
            "seal publish: missing descriptor frame",
        ));
    }
    let frames_after = frame_count.saturating_add(1);
    let body_len = SUMMARY_BODY_LEN as u64;
    let env_len = EMPTY_ENVELOPE.len() as u32;
    let summary_frame_len = 120u64
        .saturating_add(u64::from(env_len))
        .saturating_add(body_len);
    let sealed_len = prefix_len.saturating_add(summary_frame_len);
    let body = encode_summary_body(&ids, sealed_len, frames_after);
    let mut event_id = [0u8; 16];
    event_id.copy_from_slice(&ids.segment_id);
    event_id[15] ^= 0xff;
    let header = FrameHeader {
        wire_major: WIRE_MAJOR,
        wire_minor: WIRE_MINOR,
        frame_kind: FrameKind::SegmentSummary.as_u8(),
        flags: FrameFlags::default(),
        envelope_len: env_len,
        body_len,
        logical_len: body_len,
        writer_sequence,
        event_id,
    };
    let summary_frame = encode_frame(&FrameParts {
        header,
        envelope: EMPTY_ENVELOPE.to_vec(),
        body,
    })
    .map_err(|_| StoreError::CorruptMeta("seal publish: encode summary"))?;
    if summary_frame.len() as u64 != summary_frame_len {
        return Err(StoreError::CorruptMeta(
            "seal publish: summary frame length mismatch",
        ));
    }

    let content_hash = if let Some(mut h) = hasher.take() {
        h.update(&summary_frame);
        ContentHashState::Known(*h.finalize().as_bytes())
    } else {
        content_hash
    };

    Ok(SealPublishPlan {
        summary_frame,
        content_hash,
        sealed_len,
        frame_count: frames_after,
        item_events,
        segment_id: ids.segment_id,
    })
}

/// Rolling seal hash state (auth worker / tests).
#[derive(Debug, Clone, Default)]
pub struct IncrementalSealState {
    hasher: blake3::Hasher,
    /// Logical offset through which durable frame bytes were hashed.
    hashed_thru: u64,
}

impl IncrementalSealState {
    /// Empty state for a new prefix.
    pub fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
            hashed_thru: 0,
        }
    }

    /// Observe contiguous durable bytes starting at `logical_off`.
    pub fn observe_durable_bytes(
        &mut self,
        logical_off: u64,
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        if bytes.is_empty() {
            return Ok(());
        }
        if logical_off != self.hashed_thru {
            return Err(StoreError::CorruptMeta(
                "incremental seal hash gap (durable bytes out of order)",
            ));
        }
        self.hasher.update(bytes);
        self.hashed_thru = self.hashed_thru.saturating_add(bytes.len() as u64);
        Ok(())
    }

    /// Build summary frame + content hash from incremental state (no segment scan).
    ///
    /// Used by experimental stream-hash / resident paths. Hot path prefers
    /// [`meta_publish_plan`] (hash deferred to enrichment).
    pub fn finish_publish_plan(
        self,
        ids: SegmentId,
        prefix_len: u64,
        frame_count: u64,
        writer_sequence: u64,
        item_events: u64,
    ) -> Result<SealPublishPlan, StoreError> {
        if prefix_len != self.hashed_thru {
            return Err(StoreError::CorruptMeta(
                "incremental seal: prefix_len != hashed_thru",
            ));
        }
        build_publish_plan(
            ids,
            prefix_len,
            frame_count,
            writer_sequence,
            item_events,
            ContentHashState::Pending,
            Some(self.hasher),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use residiuum_format::{ActiveSegment, SafetyLimits};

    #[test]
    fn rolling_hash_matches_full_blake3_of_sealed_image() {
        let ids = SegmentId::new([1u8; 16], [2u8; 16]);
        let mut active = ActiveSegment::create(ids, SafetyLimits::default(), 1).unwrap();
        active
            .append(FrameKind::ItemEvent, EMPTY_ENVELOPE, b"hello", [9u8; 16])
            .unwrap();
        let prefix = active.as_bytes().to_vec();
        let frame_count = active.frame_count();
        let seq = active.writer_sequence();

        let mut state = IncrementalSealState::new();
        state.observe_durable_bytes(0, &prefix).unwrap();
        let plan = state
            .finish_publish_plan(ids, prefix.len() as u64, frame_count, seq, 1)
            .unwrap();

        let mut sealed = prefix;
        sealed.extend_from_slice(&plan.summary_frame);
        assert_eq!(sealed.len() as u64, plan.sealed_len);
        assert_eq!(
            plan.content_hash,
            ContentHashState::Known(*blake3::hash(&sealed).as_bytes())
        );

        let mut active2 = ActiveSegment::create(ids, SafetyLimits::default(), 1).unwrap();
        active2
            .append(FrameKind::ItemEvent, EMPTY_ENVELOPE, b"hello", [9u8; 16])
            .unwrap();
        let sealed2 = active2.seal().unwrap().into_bytes();
        assert_eq!(sealed2, sealed);
    }

    #[test]
    fn meta_publish_plan_defers_content_hash() {
        let ids = SegmentId::new([1u8; 16], [2u8; 16]);
        let mut active = ActiveSegment::create(ids, SafetyLimits::default(), 1).unwrap();
        active
            .append(FrameKind::ItemEvent, EMPTY_ENVELOPE, b"hello", [9u8; 16])
            .unwrap();
        let prefix_len = active.as_bytes().len() as u64;
        let plan = meta_publish_plan(
            ids,
            prefix_len,
            active.frame_count(),
            active.writer_sequence(),
            1,
        )
        .unwrap();
        assert!(plan.hash_pending());
        assert_eq!(plan.content_hash, ContentHashState::Pending);
        assert_eq!(
            plan.sealed_len,
            prefix_len + plan.summary_frame.len() as u64
        );
    }

    #[test]
    fn content_hash_state_wire_roundtrip() {
        let mut buf = Vec::new();
        ContentHashState::Pending.encode_wire(&mut buf);
        ContentHashState::Known([7u8; 32]).encode_wire(&mut buf);
        let (a, n0) = ContentHashState::decode_wire(&buf).unwrap();
        assert_eq!(a, ContentHashState::Pending);
        let (b, n1) = ContentHashState::decode_wire(&buf[n0..]).unwrap();
        assert_eq!(b, ContentHashState::Known([7u8; 32]));
        assert_eq!(n0 + n1, buf.len());
    }
}
