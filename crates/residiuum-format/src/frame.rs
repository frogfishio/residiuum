//! Frame layout: fixed prefix, envelope, body, fixed suffix (FORMAT_SPEC §4).

use crate::cbor_envelope::{validate_deterministic_cbor_envelope, CborEnvelopeError};

#[cfg(test)]
use crate::cbor_envelope::EMPTY_ENVELOPE;
use crate::integrity::{body_hash, prefix_crc32c, suffix_crc32c, BODY_HASH_LEN};
use crate::kinds::{FrameFlags, FrameKind};
use crate::limits::{checked_frame_len, SafetyLimits};
use std::cell::Cell;
use thiserror::Error;

thread_local! {
    /// Diagnostic only: when true, [`encode_frame_into`] copies the body but
    /// writes a zero body-hash (skips BLAKE3). **Not for product paths** —
    /// frames will fail verify. Used by store phase-bench short-circuit.
    static DIAGNOSTIC_SKIP_BODY_HASH: Cell<bool> = const { Cell::new(false) };
}

/// Diagnostic only: skip BLAKE3 body hash during [`encode_frame_into`].
///
/// Frames get an all-zero body-hash field and will not pass structural verify.
/// Must remain false in product code.
pub fn set_diagnostic_skip_body_hash(skip: bool) {
    DIAGNOSTIC_SKIP_BODY_HASH.with(|c| c.set(skip));
}

/// Whether body-hash is currently short-circuited (diagnostic).
pub fn diagnostic_skip_body_hash() -> bool {
    DIAGNOSTIC_SKIP_BODY_HASH.with(|c| c.get())
}

/// Fixed prefix length in bytes.
pub const FRAME_PREFIX_LEN: usize = 64;
/// Fixed suffix length in bytes.
pub const FRAME_SUFFIX_LEN: usize = 56;
/// Start magic: ASCII `RESIDFRM`.
pub const START_MAGIC: &[u8; 8] = b"RESIDFRM";
/// End magic: ASCII `RESIDEND`.
pub const END_MAGIC: &[u8; 8] = b"RESIDEND";
/// Wire major for this draft profile.
pub const WIRE_MAJOR: u8 = 1;
/// Wire minor for this draft profile.
pub const WIRE_MINOR: u8 = 0;

/// Logical header fields from the fixed prefix (excluding integrity fields).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    /// Wire major version.
    pub wire_major: u8,
    /// Wire minor version.
    pub wire_minor: u8,
    /// Frame kind byte (known or unknown/extension).
    pub frame_kind: u8,
    /// Flag bits.
    pub flags: FrameFlags,
    /// Envelope length in bytes.
    pub envelope_len: u32,
    /// Stored body length in bytes.
    pub body_len: u64,
    /// Logical payload length after reversing declared transforms.
    pub logical_len: u64,
    /// Writer-local diagnostic sequence (not global identity).
    pub writer_sequence: u64,
    /// Event or structural object identifier (16 bytes).
    pub event_id: [u8; 16],
}

impl FrameHeader {
    /// Build a draft header for a known kind with empty logical payload metadata.
    pub fn new_draft(
        kind: FrameKind,
        envelope_len: u32,
        body_len: u64,
        event_id: [u8; 16],
    ) -> Self {
        Self {
            wire_major: WIRE_MAJOR,
            wire_minor: WIRE_MINOR,
            frame_kind: kind.as_u8(),
            flags: FrameFlags::default(),
            envelope_len,
            body_len,
            logical_len: body_len,
            writer_sequence: 0,
            event_id,
        }
    }

    /// Known core kind, if this byte is assigned in §4.3.
    pub fn known_kind(&self) -> Option<FrameKind> {
        FrameKind::from_u8(self.frame_kind).ok()
    }
}

/// In-memory parts used to encode a frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameParts {
    /// Fixed-prefix logical fields (lengths must match envelope/body).
    pub header: FrameHeader,
    /// Deterministic-CBOR envelope bytes (FORMAT_SPEC §4.4).
    pub envelope: Vec<u8>,
    /// Stored body bytes.
    pub body: Vec<u8>,
}

/// A verified decoded frame with owned regions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedFrame {
    /// Parsed header fields.
    pub header: FrameHeader,
    /// Envelope bytes (retained losslessly).
    pub envelope: Vec<u8>,
    /// Stored body bytes.
    pub body: Vec<u8>,
    /// BLAKE3-256 of the stored body.
    pub body_hash: [u8; BODY_HASH_LEN],
    /// Total encoded frame length.
    pub frame_len: u64,
}

/// Reasons a candidate fails structural verification.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FrameVerifyError {
    /// Buffer shorter than required for the candidate.
    #[error("truncated: need at least {need} bytes, have {have}")]
    Truncated {
        /// Bytes required for the current check.
        need: usize,
        /// Bytes actually available.
        have: usize,
    },
    /// Start magic mismatch.
    #[error("start magic mismatch")]
    BadStartMagic,
    /// Unsupported wire major.
    #[error("unsupported wire major {0}")]
    UnsupportedWireMajor(u8),
    /// Declared lengths exceed safety limits or overflow.
    #[error("lengths outside safety limits")]
    LengthsOutOfLimits,
    /// Prefix/envelope CRC32C mismatch.
    #[error("prefix/envelope CRC32C mismatch")]
    BadPrefixCrc,
    /// End magic mismatch at computed boundary.
    #[error("end magic mismatch")]
    BadEndMagic,
    /// Suffix CRC32C mismatch.
    #[error("suffix CRC32C mismatch")]
    BadSuffixCrc,
    /// Suffix `frame_len` does not match computed length.
    #[error("suffix frame_len mismatch: suffix={suffix}, computed={computed}")]
    FrameLenMismatch {
        /// `frame_len` stored in the suffix.
        suffix: u64,
        /// Length computed from prefix fields.
        computed: u64,
    },
    /// BLAKE3-256 of body does not match suffix field.
    #[error("body hash mismatch")]
    BadBodyHash,
    /// Header lengths disagree with provided envelope/body slices.
    #[error("header lengths disagree with payload slices")]
    HeaderPayloadMismatch,
    /// Writer reserved prefix field was non-zero (encode-time only).
    #[error("reserved prefix field must be zero when writing")]
    ReservedNonZero,
    /// Buffer is longer than the verified frame (exact-length decode only).
    #[error("trailing bytes after frame: frame_len={frame_len}, buffer_len={buffer_len}")]
    TrailingBytes {
        /// Length of the verified frame.
        frame_len: u64,
        /// Length of the provided buffer.
        buffer_len: u64,
    },
    /// Envelope fails deterministic CBOR rules (FORMAT_SPEC §5 condition 6).
    #[error("envelope deterministic CBOR: {0}")]
    BadEnvelopeCbor(#[from] CborEnvelopeError),
}

/// Encode a complete frame into a new buffer.
///
/// The envelope must satisfy deterministic CBOR rules (FORMAT_SPEC §4.4).
pub fn encode_frame(parts: &FrameParts) -> Result<Vec<u8>, FrameVerifyError> {
    let frame_len = checked_frame_len(parts.header.envelope_len, parts.header.body_len)
        .ok_or(FrameVerifyError::LengthsOutOfLimits)?;
    let mut out = Vec::with_capacity(frame_len as usize);
    encode_frame_into(&mut out, &parts.header, &parts.envelope, &parts.body)?;
    Ok(out)
}

/// Encode a complete frame by appending into `out` (no intermediate body clone).
///
/// Hot path for segment append: previously `encode_frame` allocated a full
/// frame `Vec` (copying the payload) and the segment then `extend_from_slice`d
/// it (second payload copy). This writes once into the caller's buffer.
pub fn encode_frame_into(
    out: &mut Vec<u8>,
    header: &FrameHeader,
    envelope: &[u8],
    body: &[u8],
) -> Result<(), FrameVerifyError> {
    if envelope.len() as u32 != header.envelope_len || body.len() as u64 != header.body_len {
        return Err(FrameVerifyError::HeaderPayloadMismatch);
    }
    if header.wire_major != WIRE_MAJOR {
        return Err(FrameVerifyError::UnsupportedWireMajor(header.wire_major));
    }
    validate_deterministic_cbor_envelope(envelope)?;
    let frame_len = checked_frame_len(header.envelope_len, header.body_len)
        .ok_or(FrameVerifyError::LengthsOutOfLimits)?;
    out.reserve(frame_len as usize);

    let mut prefix = [0u8; FRAME_PREFIX_LEN];
    write_prefix_fields(&mut prefix, header);
    let pcrc = prefix_crc32c(&prefix, envelope);
    prefix[56..60].copy_from_slice(&pcrc.to_le_bytes());
    // reserved at 60..64 remains zero

    out.extend_from_slice(&prefix);
    out.extend_from_slice(envelope);

    // Single-pass body: BLAKE3 while copying into the segment buffer.
    // Previously: body_hash(body) (full payload read) then extend_from_slice
    // (second full read + write). Same digest, one fewer memory pass.
    let hash = append_body_hashed(out, body);

    let mut suffix = [0u8; FRAME_SUFFIX_LEN];
    suffix[0..8].copy_from_slice(END_MAGIC);
    suffix[8..16].copy_from_slice(&frame_len.to_le_bytes());
    suffix[16..48].copy_from_slice(&hash);
    let scrc = suffix_crc32c(&suffix);
    suffix[48..52].copy_from_slice(&scrc.to_le_bytes());
    // reserved 52..56 zero

    out.extend_from_slice(&suffix);
    Ok(())
}

/// Append `body` to `out` while computing BLAKE3-256 (one sequential payload pass).
fn append_body_hashed(out: &mut Vec<u8>, body: &[u8]) -> [u8; BODY_HASH_LEN] {
    // Diagnostic short-circuit: copy only, zero hash (frames fail verify).
    if diagnostic_skip_body_hash() {
        out.extend_from_slice(body);
        return [0u8; BODY_HASH_LEN];
    }
    if body.is_empty() {
        return body_hash(body);
    }
    // Chunked update keeps the hash working set warm and matches blake3's
    // preferred incremental API without an extra full-buffer read.
    let mut hasher = blake3::Hasher::new();
    const CHUNK: usize = 16 * 1024;
    let mut offset = 0usize;
    while offset < body.len() {
        let end = (offset + CHUNK).min(body.len());
        let chunk = &body[offset..end];
        hasher.update(chunk);
        out.extend_from_slice(chunk);
        offset = end;
    }
    *hasher.finalize().as_bytes()
}

/// Decode and structurally verify a complete frame buffer (exact length).
///
/// Includes deterministic CBOR envelope rules (FORMAT_SPEC §4.4 / §5 condition 6).
pub fn decode_frame(bytes: &[u8], limits: SafetyLimits) -> Result<DecodedFrame, FrameVerifyError> {
    let (header, envelope, body, hash, frame_len) = verify_frame_at(bytes, limits)?;
    if bytes.len() as u64 != frame_len {
        return Err(FrameVerifyError::TrailingBytes {
            frame_len,
            buffer_len: bytes.len() as u64,
        });
    }
    Ok(DecodedFrame {
        header,
        envelope: envelope.to_vec(),
        body: body.to_vec(),
        body_hash: hash,
        frame_len,
    })
}

/// Views into a verified frame: header, envelope, body, body hash, frame length.
pub type VerifiedFrameViews<'a> = (FrameHeader, &'a [u8], &'a [u8], [u8; BODY_HASH_LEN], u64);

/// Verify a frame candidate starting at offset 0 of `bytes`.
///
/// The buffer may be longer than the frame: only the leading `frame_len` bytes
/// are checked. Used by the salvage scanner when candidates sit inside a larger
/// segment. Returns `(header, envelope, body, body_hash, frame_len)`.
pub fn verify_frame_at(
    bytes: &[u8],
    limits: SafetyLimits,
) -> Result<VerifiedFrameViews<'_>, FrameVerifyError> {
    if bytes.len() < FRAME_PREFIX_LEN {
        return Err(FrameVerifyError::Truncated {
            need: FRAME_PREFIX_LEN,
            have: bytes.len(),
        });
    }

    let prefix: &[u8; FRAME_PREFIX_LEN] = bytes[..FRAME_PREFIX_LEN]
        .try_into()
        .expect("prefix length checked");

    if &prefix[0..8] != START_MAGIC.as_slice() {
        return Err(FrameVerifyError::BadStartMagic);
    }

    let wire_major = prefix[8];
    if wire_major != WIRE_MAJOR {
        return Err(FrameVerifyError::UnsupportedWireMajor(wire_major));
    }

    let header = read_prefix_fields(prefix);
    if !limits.accepts_lengths(header.envelope_len, header.body_len) {
        return Err(FrameVerifyError::LengthsOutOfLimits);
    }

    let frame_len = checked_frame_len(header.envelope_len, header.body_len)
        .ok_or(FrameVerifyError::LengthsOutOfLimits)?;
    let frame_len_usize = frame_len as usize;
    if bytes.len() < frame_len_usize {
        return Err(FrameVerifyError::Truncated {
            need: frame_len_usize,
            have: bytes.len(),
        });
    }

    let env_end = FRAME_PREFIX_LEN + header.envelope_len as usize;
    let body_end = env_end + header.body_len as usize;
    let envelope = &bytes[FRAME_PREFIX_LEN..env_end];
    let body = &bytes[env_end..body_end];
    let suffix_bytes: &[u8; FRAME_SUFFIX_LEN] = bytes[body_end..frame_len_usize]
        .try_into()
        .expect("suffix length from frame_len");

    let expected_pcrc = prefix_crc32c(prefix, envelope);
    let actual_pcrc = u32::from_le_bytes(prefix[56..60].try_into().unwrap());
    if expected_pcrc != actual_pcrc {
        return Err(FrameVerifyError::BadPrefixCrc);
    }

    // §5 condition 6: envelope deterministic-CBOR rules verify.
    validate_deterministic_cbor_envelope(envelope)?;

    if &suffix_bytes[0..8] != END_MAGIC.as_slice() {
        return Err(FrameVerifyError::BadEndMagic);
    }

    let expected_scrc = suffix_crc32c(suffix_bytes);
    let actual_scrc = u32::from_le_bytes(suffix_bytes[48..52].try_into().unwrap());
    if expected_scrc != actual_scrc {
        return Err(FrameVerifyError::BadSuffixCrc);
    }

    let suffix_frame_len = u64::from_le_bytes(suffix_bytes[8..16].try_into().unwrap());
    if suffix_frame_len != frame_len {
        return Err(FrameVerifyError::FrameLenMismatch {
            suffix: suffix_frame_len,
            computed: frame_len,
        });
    }

    let expected_hash = body_hash(body);
    let mut actual_hash = [0u8; BODY_HASH_LEN];
    actual_hash.copy_from_slice(&suffix_bytes[16..48]);
    if expected_hash != actual_hash {
        return Err(FrameVerifyError::BadBodyHash);
    }

    Ok((header, envelope, body, expected_hash, frame_len))
}

/// Exact-length verify (alias used by callers that already sliced the window).
///
/// Prefer [`verify_frame_at`] when the buffer may contain trailing bytes.
pub fn verify_frame_bytes(
    bytes: &[u8],
    limits: SafetyLimits,
) -> Result<VerifiedFrameViews<'_>, FrameVerifyError> {
    let (header, envelope, body, hash, frame_len) = verify_frame_at(bytes, limits)?;
    if bytes.len() as u64 != frame_len {
        return Err(FrameVerifyError::TrailingBytes {
            frame_len,
            buffer_len: bytes.len() as u64,
        });
    }
    Ok((header, envelope, body, hash, frame_len))
}

fn write_prefix_fields(prefix: &mut [u8; FRAME_PREFIX_LEN], header: &FrameHeader) {
    prefix[0..8].copy_from_slice(START_MAGIC);
    prefix[8] = header.wire_major;
    prefix[9] = header.wire_minor;
    prefix[10] = header.frame_kind;
    prefix[11] = header.flags.as_u8();
    prefix[12..16].copy_from_slice(&header.envelope_len.to_le_bytes());
    prefix[16..24].copy_from_slice(&header.body_len.to_le_bytes());
    prefix[24..32].copy_from_slice(&header.logical_len.to_le_bytes());
    prefix[32..40].copy_from_slice(&header.writer_sequence.to_le_bytes());
    prefix[40..56].copy_from_slice(&header.event_id);
    // 56..60 CRC filled later; 60..64 reserved zero
}

fn read_prefix_fields(prefix: &[u8; FRAME_PREFIX_LEN]) -> FrameHeader {
    FrameHeader {
        wire_major: prefix[8],
        wire_minor: prefix[9],
        frame_kind: prefix[10],
        flags: FrameFlags::new(prefix[11]),
        envelope_len: u32::from_le_bytes(prefix[12..16].try_into().unwrap()),
        body_len: u64::from_le_bytes(prefix[16..24].try_into().unwrap()),
        logical_len: u64::from_le_bytes(prefix[24..32].try_into().unwrap()),
        writer_sequence: u64::from_le_bytes(prefix[32..40].try_into().unwrap()),
        event_id: prefix[40..56].try_into().unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kinds::FrameKind;

    fn sample_parts(envelope: &[u8], body: &[u8]) -> FrameParts {
        let mut event_id = [0u8; 16];
        event_id[0] = 0x42;
        FrameParts {
            header: FrameHeader::new_draft(
                FrameKind::ItemEvent,
                envelope.len() as u32,
                body.len() as u64,
                event_id,
            ),
            envelope: envelope.to_vec(),
            body: body.to_vec(),
        }
    }

    #[test]
    fn roundtrip_empty_envelope_and_body() {
        let parts = sample_parts(EMPTY_ENVELOPE, b"");
        let bytes = encode_frame(&parts).unwrap();
        assert_eq!(bytes.len(), 121);
        assert_eq!(&bytes[0..8], START_MAGIC);
        assert_eq!(&bytes[bytes.len() - 56..bytes.len() - 48], END_MAGIC);

        let decoded = decode_frame(&bytes, SafetyLimits::default()).unwrap();
        assert_eq!(decoded.header.frame_kind, FrameKind::ItemEvent.as_u8());
        assert_eq!(decoded.envelope, EMPTY_ENVELOPE);
        assert_eq!(decoded.body, b"");
        assert_eq!(decoded.frame_len, 121);
    }

    #[test]
    fn roundtrip_with_payload() {
        let parts = sample_parts(EMPTY_ENVELOPE, b"hello-body");
        let bytes = encode_frame(&parts).unwrap();
        let decoded = decode_frame(&bytes, SafetyLimits::default()).unwrap();
        assert_eq!(decoded.envelope, EMPTY_ENVELOPE);
        assert_eq!(decoded.body, b"hello-body");
        assert_eq!(decoded.body_hash, body_hash(b"hello-body"));
    }

    #[test]
    fn rejects_non_cbor_envelope_on_encode() {
        let parts = sample_parts(b"not-cbor", b"");
        let err = encode_frame(&parts).unwrap_err();
        assert!(matches!(err, FrameVerifyError::BadEnvelopeCbor(_)));
    }

    #[test]
    fn detects_body_corruption() {
        let parts = sample_parts(EMPTY_ENVELOPE, b"payload");
        let mut bytes = encode_frame(&parts).unwrap();
        let body_off = FRAME_PREFIX_LEN + EMPTY_ENVELOPE.len();
        bytes[body_off] ^= 0xff;
        let err = decode_frame(&bytes, SafetyLimits::default()).unwrap_err();
        // Body change may fail body hash; CRC over body is only via body_hash.
        assert!(
            matches!(
                err,
                FrameVerifyError::BadBodyHash | FrameVerifyError::BadPrefixCrc
            ),
            "unexpected err {err:?}"
        );
        // empty envelope: only body hash should fail
        assert_eq!(err, FrameVerifyError::BadBodyHash);
    }

    #[test]
    fn detects_prefix_crc_corruption() {
        let parts = sample_parts(EMPTY_ENVELOPE, b"body");
        let mut bytes = encode_frame(&parts).unwrap();
        bytes[56] ^= 0x01;
        assert_eq!(
            decode_frame(&bytes, SafetyLimits::default()).unwrap_err(),
            FrameVerifyError::BadPrefixCrc
        );
    }

    #[test]
    fn detects_end_magic_corruption() {
        let parts = sample_parts(EMPTY_ENVELOPE, b"x");
        let mut bytes = encode_frame(&parts).unwrap();
        let end_magic_off = bytes.len() - FRAME_SUFFIX_LEN;
        bytes[end_magic_off] ^= 0x01;
        // suffix CRC may also fail depending on order of checks — we check magic first.
        assert_eq!(
            decode_frame(&bytes, SafetyLimits::default()).unwrap_err(),
            FrameVerifyError::BadEndMagic
        );
    }

    #[test]
    fn rejects_bad_start_magic() {
        let parts = sample_parts(EMPTY_ENVELOPE, b"");
        let mut bytes = encode_frame(&parts).unwrap();
        bytes[0] = b'X';
        assert_eq!(
            decode_frame(&bytes, SafetyLimits::default()).unwrap_err(),
            FrameVerifyError::BadStartMagic
        );
    }

    #[test]
    fn rejects_truncation() {
        // map{1: 0}
        let parts = sample_parts(&[0xa1, 0x01, 0x00], b"cd");
        let bytes = encode_frame(&parts).unwrap();
        let err = decode_frame(&bytes[..bytes.len() - 1], SafetyLimits::default()).unwrap_err();
        assert!(matches!(err, FrameVerifyError::Truncated { .. }));
    }

    #[test]
    fn rejects_oversize_via_limits() {
        let parts = sample_parts(EMPTY_ENVELOPE, b"hi");
        let bytes = encode_frame(&parts).unwrap();
        let tight = SafetyLimits {
            max_envelope_len: 0,
            max_body_len: 1,
            max_frame_len: 200,
        };
        assert_eq!(
            decode_frame(&bytes, tight).unwrap_err(),
            FrameVerifyError::LengthsOutOfLimits
        );
    }

    #[test]
    fn encode_requires_matching_lengths() {
        let mut parts = sample_parts(EMPTY_ENVELOPE, b"y");
        parts.header.envelope_len = 99;
        assert_eq!(
            encode_frame(&parts).unwrap_err(),
            FrameVerifyError::HeaderPayloadMismatch
        );
    }
}
