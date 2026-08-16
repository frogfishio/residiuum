//! Residiuum survival wire format (FORMAT_SPEC draft → major-1 candidate).
//!
//! Stage 2: frame codec, active segment + seal, forward/reverse salvage
//! scanning, event-id conflict analysis, and chunk reassembly helpers.
//! Deterministic CBOR envelope validation (FORMAT_SPEC §5 condition 6).
//! ATM-2.1: Atomic BatchPrepare/BatchCommit envelopes, member linkage, and a
//! byte-buffer recovery reader (no store write path).
//! CR-ATM2-002: one envelope-key registry; Atomic frames carry heap ownership
//! keys 31/34 plus Atomic 37–40 so `admit_frame_to_heap` can bind them.
//! No durable storage IO (Stage 3).
//!
//! **Wire freeze:** [`WIRE_MAJOR`] / [`WIRE_MINOR`] are the draft major-1
//! candidate (DELIVERY_PLAN §7). Production soak may still adjust minor; a
//! breaking on-disk change requires a major bump and dual-read support.

#![deny(missing_docs)]

/// Product freeze label for the draft wire major-1 candidate.
///
/// Same values as [`WIRE_MAJOR`].[`WIRE_MINOR`]; string form for packaging
/// manifests and benchmark disclosure (OVERVIEW §12.2).
pub const WIRE_PROFILE_LABEL: &str = "1.0-draft";

mod admit;
mod atomic;
mod cbor_envelope;
mod chunks;
mod compat;
mod csq_corpus;
mod descriptors;
mod envelope_keys;
mod events;
mod frame;
mod integrity;
mod kinds;
mod limits;
mod ownership;
mod scan;
mod segment;
mod subject_v2;

pub use csq_corpus::{
    apply_mutation, apply_pair, bit_flip_mutations, byte_replace_mutations, canonical_microframe,
    canonical_microsegment, content_hash_hex, delete_mutations, frozen_artifacts, hole_mutations,
    insert_mutations, pairwise_fault_covering, survivor_microframe, truncate_mutations,
    unsupported_kind_microframe, FaultClass, FrozenArtifact, Mutation, PairFault,
    BYTE_REPLACEMENTS, CANONICAL_BODY, CORPUS_GENERATOR, CORPUS_PROFILE, HOLE_MAX_LEN,
    HOLE_REGION_CAP, INSERTION_ALPHABET, SURVIVOR_BODY,
};

pub use atomic::{
    encode_atomic_commit_envelope, encode_atomic_frame, encode_atomic_member_envelope,
    encode_atomic_prepare_envelope, examine_atomic_frame, read_atomic_evidence,
    AtomicEnvelopeError, AtomicEvidenceClass, AtomicExamReason, AtomicFrameRole, AtomicLinkage,
    AtomicRecoveryReport, ExaminedAtomicFrame, ENV_ATOMIC_COMMIT_POSITION, ENV_ATOMIC_CONTENT_ROOT,
    ENV_ATOMIC_ID, ENV_ATOMIC_ORDINAL,
};
pub use cbor_envelope::{
    decode_deterministic_uint_map, encode_deterministic_uint_map,
    validate_deterministic_cbor_envelope, CborEnvelopeError, CborValue, EMPTY_ENVELOPE,
};
pub use chunks::{
    decode_chunk_body, encode_chunk_body, reassemble_chunks, ChunkPiece, LogicalExtent,
    ReassemblyState, CHUNK_BODY_HEADER_LEN,
};
pub use compat::{
    wire_compat_matrix, wire_freeze_criteria, wire_freeze_criteria_all_met, wire_freeze_readiness,
    wire_freeze_summary, wire_is_frozen, wire_reader_supports, wire_support_for,
    wire_support_summary, wire_writer_emits, WireFreezeCriterion, WireFreezeCriterionStatus,
    WireFreezeReadiness, WireSupportEntry, WireSupportStatus, SUPPORTED_READER_MAJORS,
    WIRE_FREEZE_POLICY_ID, WRITER_WIRE_MAJOR, WRITER_WIRE_MINOR,
};
pub use envelope_keys::{
    is_atomic_extension_key, is_ownership_key, ATOMIC_EXTENSION_FIRST, ATOMIC_EXTENSION_LAST,
    ENV_OPERATION_CONTENT_HASH, ENV_OPERATION_ID,
};
pub use events::{group_by_event_id, EventIdOutcome};
pub use frame::{
    decode_frame, diagnostic_skip_body_hash, encode_frame, encode_frame_into,
    set_diagnostic_skip_body_hash, verify_frame_at, verify_frame_bytes, DecodedFrame, FrameHeader,
    FrameParts, FrameVerifyError, VerifiedFrameViews, END_MAGIC, FRAME_PREFIX_LEN,
    FRAME_SUFFIX_LEN, START_MAGIC, WIRE_MAJOR, WIRE_MINOR,
};
// WIRE_PROFILE_LABEL is defined at crate root for packaging / disclosure.
pub use admit::{admit_frame_to_heap, salvage_admit_frame, AdmitDecision, AdmitError};
pub use descriptors::{
    decode_heap_descriptor, decode_object_descriptor, descriptor_hash, encode_heap_descriptor,
    encode_object_descriptor, DescriptorError, HeapDescriptor, HeapDescriptorState,
    ObjectDescriptor, ObjectDescriptorState, HEAP_DESCRIPTOR_PROFILE,
};
pub use integrity::{body_hash, prefix_crc32c, suffix_crc32c, BODY_HASH_LEN};
pub use kinds::{FrameFlags, FrameKind};
pub use limits::SafetyLimits;
pub use ownership::{
    agree_ownership, encode_collection_binding_envelope, encode_heap_binding_envelope,
    encode_stream_binding_envelope, parse_ownership_envelope, OwnershipError, OwnershipEvidence,
    ENV_COLLECTION_ID, ENV_HEAP_ID, ENV_OWNERSHIP_PROFILE, ENV_SOURCE_HEAP_ID,
    ENV_SOURCE_OBJECT_ID, ENV_STREAM_ID, OWNERSHIP_PROFILE_V1,
};
pub use scan::{
    find_end_magic_rightmost, find_start_magic, scan_forward, scan_reverse, ByteRange, HoleReason,
    ScanRegion, ScanReport,
};
pub use segment::{
    decode_descriptor_body, decode_store_descriptor_body, decode_summary_body,
    encode_descriptor_body, encode_store_descriptor_body, encode_store_descriptor_frame,
    encode_summary_body, ActiveSegment, ActiveSegmentCheckpoint, SealedSegment, SegmentError,
    SegmentId, DESCRIPTOR_BODY_LEN, STORE_DESCRIPTOR_BODY_LEN, STORE_DESCRIPTOR_FORMAT_TAG,
    SUMMARY_BODY_LEN,
};
pub use subject_v2::{
    decode_subject_v2, encode_subject_v2, SubjectObjectKind, SubjectV2, SubjectV2Error,
};
