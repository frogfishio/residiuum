//! Store-layer errors.

use residiuum_format::{FrameVerifyError, SegmentError};
use std::fmt;
use std::io;
use thiserror::Error;

/// Kind of a structured locator resolve failure (DEF-SCAN-001).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocatorFaultKind {
    /// Frame offset past EOF or frame spans past end of media.
    OffsetInvalid,
    /// Frame bytes failed integrity verify (checksum / framing).
    FrameVerifyFailed,
    /// Envelope segment id does not match the index locator.
    SegmentIdMismatch,
    /// Named media for the expected segment id is absent (and salvage failed).
    SegmentNotFound,
}

impl LocatorFaultKind {
    /// Stable snake_case label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OffsetInvalid => "locator_offset_invalid",
            Self::FrameVerifyFailed => "locator_frame_verify_failed",
            Self::SegmentIdMismatch => "locator_segment_id_mismatch",
            Self::SegmentNotFound => "segment_not_found",
        }
    }
}

impl fmt::Display for LocatorFaultKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Field diagnostics for a failed index locator resolve (DEF-SCAN-001).
///
/// Unit error categories are insufficient for operators: diagnosis needs
/// segment id, offset, path, file length, and underlying cause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocatorFault {
    /// Distinct failure kind.
    pub kind: LocatorFaultKind,
    /// Segment id from the index locator (expected).
    pub segment_id: [u8; 16],
    /// Frame byte offset from the index locator.
    pub frame_offset: u64,
    /// Candidate media path that was examined (if any).
    pub path: Option<String>,
    /// Length of that media file when examined.
    pub file_len: Option<u64>,
    /// Envelope segment id observed in the frame (segment-id mismatch).
    pub observed_segment_id: Option<[u8; 16]>,
    /// Underlying I/O or verify detail.
    pub cause: Option<String>,
}

impl LocatorFault {
    /// Hex form of [`Self::segment_id`] (32 lowercase chars).
    pub fn segment_hex(&self) -> String {
        Self::hex16(&self.segment_id)
    }

    /// Hex-encode a 16-byte id (32 lowercase chars).
    pub fn hex16(id: &[u8; 16]) -> String {
        let mut s = String::with_capacity(32);
        for b in id {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }

    /// Build a fault for a known path examination.
    pub fn at_path(
        kind: LocatorFaultKind,
        segment_id: [u8; 16],
        frame_offset: u64,
        path: &std::path::Path,
        file_len: Option<u64>,
        cause: Option<String>,
    ) -> Self {
        Self {
            kind,
            segment_id,
            frame_offset,
            path: Some(path.display().to_string()),
            file_len,
            observed_segment_id: None,
            cause,
        }
    }

    /// Segment-id mismatch with both expected and observed ids.
    pub fn segment_mismatch(
        expected: [u8; 16],
        observed: [u8; 16],
        frame_offset: u64,
        path: &std::path::Path,
        file_len: Option<u64>,
    ) -> Self {
        Self {
            kind: LocatorFaultKind::SegmentIdMismatch,
            segment_id: expected,
            frame_offset,
            path: Some(path.display().to_string()),
            file_len,
            observed_segment_id: Some(observed),
            cause: None,
        }
    }

    /// Named media missing for `segment_id` after salvage.
    pub fn segment_not_found(segment_id: [u8; 16], frame_offset: u64) -> Self {
        Self {
            kind: LocatorFaultKind::SegmentNotFound,
            segment_id,
            frame_offset,
            path: None,
            file_len: None,
            observed_segment_id: None,
            cause: None,
        }
    }
}

impl fmt::Display for LocatorFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} segment={} offset={}",
            self.kind,
            Self::hex16(&self.segment_id),
            self.frame_offset
        )?;
        if let Some(ref p) = self.path {
            write!(f, " path={p}")?;
        }
        if let Some(len) = self.file_len {
            write!(f, " file_len={len}")?;
        }
        if let Some(obs) = &self.observed_segment_id {
            write!(f, " observed_segment={}", Self::hex16(obs))?;
        }
        if let Some(ref c) = self.cause {
            write!(f, " cause={c}")?;
        }
        Ok(())
    }
}

/// Errors from store open, write, and recovery paths.
#[derive(Debug, Error)]
pub enum StoreError {
    /// Underlying IO failure.
    #[error("io: {0}")]
    Io(#[from] io::Error),

    /// Frame encode/verify failed.
    #[error(transparent)]
    Frame(#[from] FrameVerifyError),

    /// In-memory segment writer failed.
    #[error(transparent)]
    Segment(#[from] SegmentError),

    /// Path exists but is not a Residiuum store (missing store-info).
    #[error("not a Residiuum store: missing store-info at {0}")]
    NotAStore(std::path::PathBuf),

    /// Store already exists at path when create was exclusive.
    #[error("store already exists at {0}")]
    AlreadyExists(std::path::PathBuf),

    /// Draft item envelope could not be decoded.
    #[error("invalid item envelope: {0}")]
    BadEnvelope(&'static str),

    /// Subject key exceeds draft limits.
    #[error("subject too long (max {max} bytes)")]
    SubjectTooLong {
        /// Maximum allowed subject byte length.
        max: usize,
    },

    /// Body exceeds safety limits.
    #[error("payload too large for configured safety limits")]
    PayloadTooLarge,

    /// Non-blocking durable mutation admission exhausted its bounded window.
    #[error("durable mutation admission window is full")]
    WriteAdmissionFull,

    /// Adaptive write lease owns mutation; direct `Store` mutation is refused (AWO-3).
    #[error("adaptive writer active; direct mutation refused")]
    AdaptiveWriterActive,

    /// Adaptive/batch writer poisoned after uncertain I/O; reopen required (AWO-1).
    #[error("adaptive writer poisoned; close and reopen for recovery")]
    AdaptiveWriterPoisoned,

    /// Corrupt or incomplete store metadata.
    #[error("corrupt store metadata: {0}")]
    CorruptMeta(&'static str),

    /// Authoritative segment id owned by multiple physical paths (P0).
    ///
    /// Open / publish refuse without mutating either side. `paths` lists every
    /// conflicting owner discovered (active, pending, sealed, tier, …).
    #[error("segment id collision ({} conflicting path(s))", .paths.len())]
    SegmentIdCollision {
        /// Colliding segment identity.
        segment_id: [u8; 16],
        /// Every conflicting physical path (must not be empty).
        paths: Vec<std::path::PathBuf>,
    },

    /// Control document failed validation; recovery action is documented.
    ///
    /// Used when a mutable control file (endpoints, dedup table, catalogs, …)
    /// is damaged and the previous generation is also unusable (DEF-021).
    #[error("corrupt control document {path}: {detail} (recovery: {recovery})")]
    CorruptControl {
        /// Absolute or store-relative path of the damaged document.
        path: String,
        /// Why the primary generation was rejected.
        detail: String,
        /// Operator / automatic recovery action (rebuild, use .prev, etc.).
        recovery: String,
    },

    /// Payload is only partially available (missing/corrupt chunks).
    #[error("payload only partially available")]
    PayloadPartial,

    /// Chunk reassembly found conflicting content at a manifest position.
    #[error("conflicting chunk content")]
    PayloadConflict,

    /// Requested historical `event_id` is not present in subject history (DEF-099).
    #[error("history event not found")]
    HistoryEventNotFound,

    /// Requested sealed segment is not registered or not on disk.
    ///
    /// Prefer [`Self::LocatorFault`] with [`LocatorFaultKind::SegmentNotFound`]
    /// when the expected segment id / path context is known (DEF-SCAN-001).
    #[error("segment not found")]
    SegmentNotFound,

    /// Index locator resolve failed with **structured field diagnostics** (DEF-SCAN-001).
    ///
    /// Carries segment id, frame offset, path, file length, and optional cause.
    /// Distinct kinds: offset invalid, frame verify failed, segment-id mismatch,
    /// or named segment media absent. Not a bucket for chunk
    /// [`Self::PayloadPartial`].
    #[error("locator fault: {0}")]
    LocatorFault(Box<LocatorFault>),

    /// Required storage tier is offline or unmounted (Stage 9).
    #[error("storage tier offline: {0}")]
    TierOffline(&'static str),

    /// Segment bytes use a wire major this build cannot interpret.
    ///
    /// Authoritative bytes are preserved; interpretation is refused
    /// (`format-unsupported`, OVERVIEW §9.5).
    #[error("format unsupported: wire major {wire_major}")]
    FormatUnsupported {
        /// Unsupported wire major observed.
        wire_major: u8,
    },

    /// Media locator requires a backend this build does not ship (e.g. live S3/GCS).
    #[error("media backend unsupported: {0}")]
    MediaUnsupported(String),

    /// Another process or handle already holds the exclusive writer lock (DEF-020 / DEF-101).
    ///
    /// Carries a structured [`crate::writer_lock::WriterLockObservation`]. This is
    /// never database absence — do not treat it as an empty store, and do not
    /// delete `writer.lock` to force unlock.
    #[error("store writer lock held: {0}")]
    WriterLockHeld(Box<crate::writer_lock::WriterLockObservation>),

    /// Store-owned Atomic staging refused (CR-ATMR4-005).
    #[error("atomic stage: {0}")]
    AtomicStage(String),

    /// Scan/get coverage is incomplete; ordinary complete results are refused (DEF-012).
    #[error("coverage incomplete: {0}")]
    CoverageIncomplete(String),

    /// Client operation id reused with different content (DEF-010).
    #[error("operation identity reused with different canonical request")]
    OperationIdentityConflict,

    /// General store consistency invariant failed.
    #[error("consistency violation: {0}")]
    ConsistencyViolation(String),

    /// Injected failure from an armed failpoint (DEF-022 testing only).
    #[error("failpoint hit: {0}")]
    Failpoint(&'static str),

    /// OS CSPRNG was required and unavailable (DEF-025).
    ///
    /// Store/event/operation identity must not fall back to wall-clock or
    /// weak mixes when secure randomness is needed.
    #[error("secure randomness unavailable: {0}")]
    RandomUnavailable(String),

    /// Continuation token is malformed, tampered, expired shape, or wrong store (DEF-026).
    #[error("invalid scan cursor: {0}")]
    CursorInvalid(String),

    /// Continuation token generation no longer matches live store state (DEF-026).
    ///
    /// The scan generation fence changed (segment fingerprint and/or live count);
    /// restart the scan from the first page.
    #[error("stale scan cursor: {0}")]
    CursorStale(String),

    /// Heap capability check failed (HP-003 façades).
    #[error("heap capability: {0}")]
    HeapCapability(String),

    /// One-heap admission rejected the frame (HP-002).
    #[error("heap admit: {0}")]
    HeapAdmit(String),

    /// Conditional put/delete version precondition failed (APB-2 Key Atomic).
    ///
    /// `expected` is the caller-supplied live event id token (or zero for
    /// absence-only creates). `observed` is the live establishing event id
    /// when present, or `None` when the key is absent / tombstoned.
    #[error("version conflict")]
    VersionConflict {
        /// Token the caller required.
        expected: [u8; 16],
        /// Live establishing event id, or `None` if absent.
        observed: Option<[u8; 16]>,
    },

    /// Conditional create failed because the key is already live (APB-2).
    #[error("key already exists")]
    KeyExists,
}

impl StoreError {
    /// Whether this error is an ordinary IO failure.
    pub fn is_io(&self) -> bool {
        matches!(self, Self::Io(_))
    }
}
