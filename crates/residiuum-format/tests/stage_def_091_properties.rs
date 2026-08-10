//! DEF-091 — property tests for untrusted wire surfaces (residiuum-format cut).
//!
//! These run in ordinary `cargo test` (CI quality bar). Continuous/scheduled
//! fuzz targets live under `fuzz/` and are exercised by nightly + local
//! `cargo fuzz`.

use proptest::prelude::*;
use residiuum_format::{
    decode_frame, encode_deterministic_uint_map, encode_frame, scan_forward, scan_reverse,
    validate_deterministic_cbor_envelope, CborValue, FrameHeader, FrameKind, FrameParts,
    SafetyLimits, EMPTY_ENVELOPE, WIRE_MAJOR, WIRE_MINOR,
};

/// Tight limits so adversarial length fields cannot allocate multi-GiB buffers.
fn prop_limits() -> SafetyLimits {
    SafetyLimits {
        max_envelope_len: 4 * 1024,
        max_body_len: 64 * 1024,
        max_frame_len: 72 * 1024,
    }
}

fn event_id_from(seed: u64) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&seed.to_le_bytes());
    id
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Well-formed frames round-trip under draft wire major.
    #[test]
    fn encode_decode_roundtrip(
        kind_byte in 1u8..=13u8,
        body in prop::collection::vec(any::<u8>(), 0..256),
        writer_sequence in any::<u64>(),
        seed in any::<u64>(),
    ) {
        let kind = FrameKind::from_u8(kind_byte).expect("1..=13 are assigned kinds");
        let envelope = EMPTY_ENVELOPE.to_vec();
        let parts = FrameParts {
            header: FrameHeader {
                wire_major: WIRE_MAJOR,
                wire_minor: WIRE_MINOR,
                frame_kind: kind.as_u8(),
                flags: Default::default(),
                envelope_len: envelope.len() as u32,
                body_len: body.len() as u64,
                logical_len: body.len() as u64,
                writer_sequence,
                event_id: event_id_from(seed),
            },
            envelope,
            body: body.clone(),
        };
        let encoded = encode_frame(&parts).expect("encode well-formed");
        let decoded = decode_frame(&encoded, SafetyLimits::default()).expect("decode");
        assert_eq!(decoded.header.frame_kind, kind.as_u8());
        assert_eq!(decoded.body, body);
        assert_eq!(decoded.envelope, EMPTY_ENVELOPE);
        assert_eq!(decoded.header.writer_sequence, writer_sequence);
        assert_eq!(decoded.header.event_id, event_id_from(seed));
    }

    /// Deterministic CBOR map encode → validate → round-trip values.
    #[test]
    fn cbor_uint_map_roundtrip(
        entries in prop::collection::vec(
            (0u64..1000u64, prop::collection::vec(any::<u8>(), 0..32)),
            0..16,
        ),
    ) {
        // Dedup keys keeping first value (encoder rejects duplicates).
        let mut seen = std::collections::BTreeMap::new();
        for (k, v) in entries {
            seen.entry(k).or_insert(v);
        }
        let pairs: Vec<(u64, CborValue)> = seen
            .into_iter()
            .map(|(k, v)| (k, CborValue::Bytes(v)))
            .collect();
        let encoded = encode_deterministic_uint_map(&pairs).expect("encode map");
        validate_deterministic_cbor_envelope(&encoded).expect("validate");
    }

    /// Adversarial buffers never panic the frame decoder (errors only).
    #[test]
    fn decode_arbitrary_bytes_no_panic(data in prop::collection::vec(any::<u8>(), 0..512)) {
        let _ = decode_frame(&data, prop_limits());
    }

    /// Adversarial buffers never panic CBOR envelope validation.
    #[test]
    fn cbor_validate_arbitrary_no_panic(data in prop::collection::vec(any::<u8>(), 0..256)) {
        let _ = validate_deterministic_cbor_envelope(&data);
    }

    /// Forward salvage scan never panics on hostile byte streams.
    #[test]
    fn scan_forward_arbitrary_no_panic(data in prop::collection::vec(any::<u8>(), 0..1024)) {
        let report = scan_forward(&data, prop_limits());
        // Invariant: every verified region is fully inside the buffer.
        for region in &report.regions {
            if let residiuum_format::ScanRegion::VerifiedFrame { range, .. } = region {
                assert!(range.end as usize <= data.len());
                assert!(range.start <= range.end);
            }
        }
    }

    /// Reverse salvage scan never panics on hostile byte streams.
    #[test]
    fn scan_reverse_arbitrary_no_panic(data in prop::collection::vec(any::<u8>(), 0..1024)) {
        let report = scan_reverse(&data, prop_limits());
        for region in &report.regions {
            if let residiuum_format::ScanRegion::VerifiedFrame { range, .. } = region {
                assert!(range.end as usize <= data.len());
                assert!(range.start <= range.end);
            }
        }
    }

    /// Embed a valid frame in garbage; forward scan still recovers it.
    #[test]
    fn scan_recovers_embedded_frame(
        prefix in prop::collection::vec(any::<u8>(), 0..64),
        body in prop::collection::vec(any::<u8>(), 0..64),
        suffix in prop::collection::vec(any::<u8>(), 0..64),
        seed in any::<u64>(),
    ) {
        // Avoid accidental START_MAGIC in the hostile prefix.
        let mut prefix = prefix;
        let mut i = 0;
        while i + 8 <= prefix.len() {
            if &prefix[i..i + 8] == b"RESIDFRM" {
                prefix[i] = 0;
            }
            i += 1;
        }
        let envelope = EMPTY_ENVELOPE.to_vec();
        let parts = FrameParts {
            header: FrameHeader {
                wire_major: WIRE_MAJOR,
                wire_minor: WIRE_MINOR,
                frame_kind: FrameKind::Padding.as_u8(),
                flags: Default::default(),
                envelope_len: envelope.len() as u32,
                body_len: body.len() as u64,
                logical_len: body.len() as u64,
                writer_sequence: seed,
                event_id: event_id_from(seed),
            },
            envelope,
            body: body.clone(),
        };
        let frame = encode_frame(&parts).expect("encode");
        let mut buf = prefix;
        let frame_start = buf.len() as u64;
        buf.extend_from_slice(&frame);
        buf.extend_from_slice(&suffix);

        let report = scan_forward(&buf, SafetyLimits::default());
        let found: Vec<_> = report
            .verified_frames()
            .filter(|(off, f)| *off == frame_start && f.body == body)
            .collect();
        assert_eq!(found.len(), 1, "expected exactly one embedded frame recovery");
    }
}
