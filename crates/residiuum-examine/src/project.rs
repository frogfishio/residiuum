//! Map salvage scan regions → ExaminationUnit (SDA_PROFILE + FORMAT_SPEC §7).

use crate::unit::{
    EnvelopeEntry, EnvelopeValue, ExaminationUnit, IntegrityEvidence, PayloadInfo,
    PhysicalLocation, ProvenanceEntry,
};
use residiuum_format::{
    scan_forward, DecodedFrame, FrameKind, FrameVerifyError, HoleReason, SafetyLimits, ScanRegion,
};
use residiuum_store::{decode_item_envelope, hex16, EventKind};

/// Tool identity stamped into recovered provenance.
const TOOL: &str = "residiuum-examine";
const TOOL_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Options controlling payload materialization during projection.
#[derive(Debug, Clone)]
pub struct ProjectOptions {
    /// When false, verified payload bodies are omitted (`value = None`) and
    /// uncertainty gains `resource-limited` only if the host also marks
    /// availability unavailable. Default true.
    pub materialize_payloads: bool,
    /// Optional known store id (hex filled from open store).
    pub store_id: Option<[u8; 16]>,
}

impl Default for ProjectOptions {
    fn default() -> Self {
        Self {
            materialize_payloads: true,
            store_id: None,
        }
    }
}

/// Project a single scan region into zero or more examination units.
///
/// Holes become one unit each. Verified frames become one unit each.
pub fn project_region(source: &str, region: &ScanRegion, opts: &ProjectOptions) -> ExaminationUnit {
    match region {
        ScanRegion::VerifiedFrame { range, frame } => {
            project_verified_frame(source, range.start, range.len(), frame, opts)
        }
        ScanRegion::Hole { range, reason } => project_hole(source, *range, reason, opts),
    }
}

/// Forward-scan `bytes` and project every region (unordered until caller sorts).
pub fn project_bytes(
    source: &str,
    bytes: &[u8],
    limits: SafetyLimits,
    opts: &ProjectOptions,
) -> Vec<ExaminationUnit> {
    let report = scan_forward(bytes, limits);
    report
        .regions
        .iter()
        .map(|r| project_region(source, r, opts))
        .collect()
}

fn project_verified_frame(
    source: &str,
    offset: u64,
    encoded_length: u64,
    frame: &DecodedFrame,
    opts: &ProjectOptions,
) -> ExaminationUnit {
    let physical = PhysicalLocation {
        source: source.to_string(),
        offset: Some(offset),
        encoded_length: Some(encoded_length),
        wire_major: Some(frame.header.wire_major),
        wire_minor: Some(frame.header.wire_minor),
    };
    let integrity = IntegrityEvidence::verified_no_auth();
    let event_id = Some(hex16(&frame.header.event_id));
    let provenance = vec![ProvenanceEntry {
        action: "recovered".into(),
        source_id: None,
        tool: TOOL.into(),
        tool_version: TOOL_VERSION.into(),
    }];

    match frame.header.known_kind() {
        Some(FrameKind::ItemEvent) => project_item_event(
            source, physical, integrity, event_id, frame, opts, provenance,
        ),
        Some(kind) => ExaminationUnit {
            unit_kind: "structural-frame".into(),
            status: "verified-complete".into(),
            store_id: opts.store_id.map(|id| hex16(&id)),
            segment_id: None,
            item_id: None,
            event_id,
            event_kind: None,
            physical,
            integrity,
            envelope: vec![EnvelopeEntry {
                key: "frame_kind".into(),
                value: EnvelopeValue::Str(frame_kind_name(kind).into()),
            }],
            payload: project_body_payload(frame, opts, false),
            holes: vec![],
            provenance,
            uncertainty: vec![],
        },
        None => ExaminationUnit {
            unit_kind: "structural-frame".into(),
            status: "format-unsupported".into(),
            store_id: opts.store_id.map(|id| hex16(&id)),
            segment_id: None,
            item_id: None,
            event_id,
            event_kind: None,
            physical,
            integrity,
            envelope: vec![EnvelopeEntry {
                key: "frame_kind".into(),
                value: EnvelopeValue::Num(i64::from(frame.header.frame_kind)),
            }],
            payload: project_body_payload(frame, opts, true),
            holes: vec![],
            provenance,
            uncertainty: vec!["unsupported-decoder".into()],
        },
    }
}

fn project_item_event(
    _source: &str,
    physical: PhysicalLocation,
    integrity: IntegrityEvidence,
    event_id: Option<String>,
    frame: &DecodedFrame,
    opts: &ProjectOptions,
    provenance: Vec<ProvenanceEntry>,
) -> ExaminationUnit {
    match decode_item_envelope(&frame.envelope) {
        Some(env) => {
            let store_id = Some(hex16(&env.store_id));
            let segment_id = Some(hex16(&env.segment_id));
            let item_id = Some(hex16(&env.item_id));
            let event_kind = Some(env.event_kind.as_str().to_string());
            let mut envelope = vec![
                EnvelopeEntry {
                    key: "subject".into(),
                    value: match String::from_utf8(env.subject.clone()) {
                        Ok(s) => EnvelopeValue::Str(s),
                        Err(_) => EnvelopeValue::Bytes(env.subject),
                    },
                },
                EnvelopeEntry {
                    key: "event_kind".into(),
                    value: EnvelopeValue::Str(env.event_kind.as_str().into()),
                },
            ];
            if env.created_ns != 0 {
                envelope.push(EnvelopeEntry {
                    key: "created_ns".into(),
                    value: EnvelopeValue::Num(env.created_ns as i64),
                });
            }
            // Delete events have empty body — still verified complete.
            let payload = match env.event_kind {
                EventKind::Delete if frame.body.is_empty() => PayloadInfo::not_applicable(),
                _ => project_body_payload(frame, opts, false),
            };
            let mut uncertainty = vec![];
            if payload.availability == "unavailable" && !opts.materialize_payloads {
                uncertainty.push("resource-limited".into());
            }
            ExaminationUnit {
                unit_kind: "event".into(),
                status: "verified-complete".into(),
                store_id,
                segment_id,
                item_id,
                event_id,
                event_kind,
                physical,
                integrity,
                envelope,
                payload,
                holes: vec![],
                provenance,
                uncertainty,
            }
        }
        None => {
            // Structurally verified frame but draft envelope cannot be decoded:
            // keep physical + envelope bytes; status is verified-envelope when
            // framing/structure/content verified (SDA_PROFILE core statuses).
            let mut uncertainty = vec![];
            let payload = if frame.body.is_empty() {
                PayloadInfo::not_applicable()
            } else {
                project_body_payload(frame, opts, false)
            };
            if !opts.materialize_payloads && payload.availability == "unavailable" {
                uncertainty.push("resource-limited".into());
            }
            ExaminationUnit {
                unit_kind: "event".into(),
                status: "verified-envelope".into(),
                store_id: opts.store_id.map(|id| hex16(&id)),
                segment_id: None,
                item_id: None,
                event_id,
                event_kind: None,
                physical,
                integrity,
                envelope: vec![EnvelopeEntry {
                    key: "wire:raw".into(),
                    value: EnvelopeValue::Bytes(frame.envelope.clone()),
                }],
                payload,
                holes: vec![],
                provenance,
                uncertainty,
            }
        }
    }
}

fn project_body_payload(
    frame: &DecodedFrame,
    opts: &ProjectOptions,
    unsupported: bool,
) -> PayloadInfo {
    if unsupported {
        return PayloadInfo {
            availability: "unsupported".into(),
            representation: "bytes".into(),
            media_type: Some("application/octet-stream".into()),
            logical_length: Some(frame.header.logical_len),
            value: if opts.materialize_payloads {
                Some(frame.body.clone())
            } else {
                None
            },
            extents: vec![],
        };
    }
    if !opts.materialize_payloads {
        // SDA_PROFILE §7.2: value=None, availability unavailable, host adds
        // uncertainty resource-limited on the unit when applicable.
        return PayloadInfo::resource_limited(Some(frame.header.logical_len));
    }
    let mut info = PayloadInfo::complete_bytes(&frame.body, Some("application/octet-stream"));
    info.logical_length = Some(frame.header.logical_len);
    info
}

fn project_hole(
    source: &str,
    range: residiuum_format::ByteRange,
    reason: &HoleReason,
    opts: &ProjectOptions,
) -> ExaminationUnit {
    let (profile_reason, certainty, status) = map_hole_reason(reason);
    let (offset, encoded_length) = if range.is_empty() {
        (None, None)
    } else {
        (Some(range.start), Some(range.len()))
    };
    let envelope = vec![
        EnvelopeEntry {
            key: "scope".into(),
            value: EnvelopeValue::Str("physical-range".into()),
        },
        EnvelopeEntry {
            key: "reason".into(),
            value: EnvelopeValue::Str(profile_reason.into()),
        },
        EnvelopeEntry {
            key: "certainty".into(),
            value: EnvelopeValue::Str(certainty.into()),
        },
        EnvelopeEntry {
            key: "affects".into(),
            value: EnvelopeValue::StrSet(vec!["payload".into(), "state-completeness".into()]),
        },
    ];
    ExaminationUnit {
        unit_kind: "hole".into(),
        status: status.into(),
        store_id: opts.store_id.map(|id| hex16(&id)),
        segment_id: None,
        item_id: None,
        event_id: None,
        event_kind: None,
        physical: PhysicalLocation {
            source: source.to_string(),
            offset,
            encoded_length,
            wire_major: None,
            wire_minor: None,
        },
        integrity: IntegrityEvidence::failed(),
        envelope,
        payload: PayloadInfo::not_applicable(),
        holes: vec![],
        provenance: vec![ProvenanceEntry {
            action: "recovered".into(),
            source_id: None,
            tool: TOOL.into(),
            tool_version: TOOL_VERSION.into(),
        }],
        uncertainty: vec!["history-gap".into()],
    }
}

/// Map FORMAT_SPEC hole reasons onto SDA_PROFILE core reason/status tags.
fn map_hole_reason(reason: &HoleReason) -> (&'static str, &'static str, &'static str) {
    match reason {
        HoleReason::UnclassifiedGarbage => ("unknown", "known", "corrupt"),
        HoleReason::CorruptCandidate { error, .. } | HoleReason::DamagedCandidate { error, .. } => {
            match error {
                FrameVerifyError::Truncated { .. } => ("truncated", "known", "corrupt"),
                FrameVerifyError::BadPrefixCrc | FrameVerifyError::BadSuffixCrc => {
                    ("checksum-failure", "known", "corrupt")
                }
                FrameVerifyError::BadBodyHash => ("hash-failure", "known", "corrupt"),
                FrameVerifyError::BadStartMagic
                | FrameVerifyError::BadEndMagic
                | FrameVerifyError::FrameLenMismatch { .. }
                | FrameVerifyError::HeaderPayloadMismatch
                | FrameVerifyError::ReservedNonZero
                | FrameVerifyError::TrailingBytes { .. }
                | FrameVerifyError::BadEnvelopeCbor(_) => ("invalid-framing", "known", "corrupt"),
                FrameVerifyError::UnsupportedWireMajor(_) => {
                    ("unsupported-format", "known", "format-unsupported")
                }
                FrameVerifyError::LengthsOutOfLimits => ("resource-limit", "bounded", "corrupt"),
            }
        }
    }
}

fn frame_kind_name(kind: FrameKind) -> &'static str {
    match kind {
        FrameKind::Invalid => "invalid",
        FrameKind::StoreDescriptor => "store-descriptor",
        FrameKind::SegmentDescriptor => "segment-descriptor",
        FrameKind::ItemEvent => "item-event",
        FrameKind::PayloadChunk => "payload-chunk",
        FrameKind::BatchPrepare => "batch-prepare",
        FrameKind::BatchCommit => "batch-commit",
        FrameKind::SegmentSummary => "segment-summary",
        FrameKind::PurgeAttestation => "purge-attestation",
        FrameKind::Padding => "padding",
        FrameKind::HeapDescriptor => "heap-descriptor",
        FrameKind::CollectionDescriptor => "collection-descriptor",
        FrameKind::StreamDescriptor => "stream-descriptor",
        FrameKind::HeapMigrationEvidence => "heap-migration-evidence",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use residiuum_format::{encode_frame, FrameHeader, FrameParts, START_MAGIC};

    #[test]
    fn verified_item_projects_complete_event() {
        use residiuum_store::{encode_item_envelope, ItemEnvelope};
        let env = ItemEnvelope {
            store_id: [1u8; 16],
            segment_id: [2u8; 16],
            item_id: [3u8; 16],
            event_kind: EventKind::Put,
            created_ns: 0,
            subject: b"s".to_vec(),
            operation_id: None,
            operation_content_hash: None,
        };
        let envelope = encode_item_envelope(&env).unwrap();
        let body = b"payload";
        let parts = FrameParts {
            header: FrameHeader::new_draft(
                FrameKind::ItemEvent,
                envelope.len() as u32,
                body.len() as u64,
                [9u8; 16],
            ),
            envelope,
            body: body.to_vec(),
        };
        let bytes = encode_frame(&parts).unwrap();
        let units = project_bytes(
            "seg.residiuum",
            &bytes,
            SafetyLimits::default(),
            &ProjectOptions {
                materialize_payloads: true,
                store_id: Some([1u8; 16]),
            },
        );
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].unit_kind, "event");
        assert_eq!(units[0].status, "verified-complete");
        assert_eq!(units[0].payload.availability, "complete");
        assert_eq!(
            units[0].payload.value.as_deref(),
            Some(b"payload".as_slice())
        );
    }

    #[test]
    fn garbage_projects_as_hole() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(START_MAGIC);
        bytes.extend_from_slice(&[0u8; 20]);
        let units = project_bytes(
            "bad.residiuum",
            &bytes,
            SafetyLimits::default(),
            &ProjectOptions::default(),
        );
        assert!(units.iter().any(|u| u.unit_kind == "hole"));
        let hole = units.iter().find(|u| u.unit_kind == "hole").unwrap();
        assert_eq!(hole.status, "corrupt");
        assert!(hole.envelope.iter().any(|e| e.key == "reason"));
    }
}
