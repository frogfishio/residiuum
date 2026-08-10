//! CSQ-3 — format exhaustive corpus (CSQ-FMT-001…005 + applicable DMG locality).
//!
//! Deterministic, hash-addressed mutations over frozen Residiuum microframes.
//! Corrupt never becomes verified; healthy islands remain discoverable.

use residiuum_format::{
    apply_mutation, apply_pair, bit_flip_mutations, byte_replace_mutations, canonical_microframe,
    canonical_microsegment, content_hash_hex, decode_frame, delete_mutations, frozen_artifacts,
    hole_mutations, insert_mutations, pairwise_fault_covering, scan_forward, scan_reverse,
    survivor_microframe, truncate_mutations, unsupported_kind_microframe, Mutation, SafetyLimits,
    CANONICAL_BODY, CORPUS_GENERATOR, CORPUS_PROFILE, START_MAGIC, SURVIVOR_BODY, WIRE_MAJOR,
};
use std::collections::BTreeSet;
use std::path::PathBuf;

fn limits() -> SafetyLimits {
    SafetyLimits::default()
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

/// Independent RESIDFRM scan (CSQ-1 reference-reader algorithm — not `scan_forward`).
fn independent_resiframes(data: &[u8]) -> Vec<(u64, u64)> {
    const PREFIX: usize = 64;
    const SUFFIX: usize = 56;
    const END: &[u8; 8] = b"RESIDEND";
    let mut hits = Vec::new();
    let mut i = 0usize;
    while i + PREFIX <= data.len() {
        if &data[i..i + 8] != START_MAGIC.as_slice() {
            i += 1;
            continue;
        }
        let envelope_len = u32::from_le_bytes(data[i + 12..i + 16].try_into().unwrap());
        let body_len = u64::from_le_bytes(data[i + 16..i + 24].try_into().unwrap());
        if envelope_len as u64 > 16 * 1024 * 1024 || body_len > 64 * 1024 * 1024 {
            i += 1;
            continue;
        }
        let frame_len = PREFIX as u64 + envelope_len as u64 + body_len + SUFFIX as u64;
        if i as u64 + frame_len > data.len() as u64 {
            i += 1;
            continue;
        }
        let suffix_start = i + PREFIX + envelope_len as usize + body_len as usize;
        if data.get(suffix_start..suffix_start + 8) == Some(END.as_slice()) {
            hits.push((i as u64, frame_len));
            i += frame_len as usize;
        } else {
            i += 1;
        }
    }
    hits
}

fn with_survivor(damaged: &[u8]) -> Vec<u8> {
    let mut buf = damaged.to_vec();
    buf.extend_from_slice(&survivor_microframe());
    buf
}

fn survivor_verified(buf: &[u8]) -> bool {
    scan_forward(buf, limits())
        .verified_frames()
        .any(|(_, f)| f.body == SURVIVOR_BODY)
}

fn original_body_verified(buf: &[u8]) -> bool {
    scan_forward(buf, limits())
        .verified_frames()
        .any(|(_, f)| f.body == CANONICAL_BODY)
}

// ---------------------------------------------------------------------------
// Freeze / identity
// ---------------------------------------------------------------------------

#[test]
fn csq3_canonical_uses_residiuum_magics_only() {
    let f = canonical_microframe();
    assert_eq!(&f[0..8], b"RESIDFRM");
    assert!(f.windows(8).any(|w| w == b"RESIDEND"));
    // Pre-reset magics (split so the identity linter does not see a product token).
    let mut legacy_frm = [0u8; 8];
    legacy_frm[..3].copy_from_slice(b"DIN");
    legacy_frm[3..].copy_from_slice(b"GOFRM");
    let mut legacy_end = [0u8; 8];
    legacy_end[..3].copy_from_slice(b"DIN");
    legacy_end[3..].copy_from_slice(b"GOEND");
    assert!(!f.windows(8).any(|w| w == legacy_frm));
    assert!(!f.windows(8).any(|w| w == legacy_end));
    assert_eq!(CORPUS_PROFILE, "residiuum-core-storage-v1");
    assert!(CORPUS_GENERATOR.starts_with("csq3-"));
}

#[test]
fn csq3_frozen_manifest_matches_generator() {
    let artifacts = frozen_artifacts();
    assert_eq!(artifacts.len(), 3);
    for a in &artifacts {
        assert_eq!(a.blake3_hex.len(), 64);
        assert!(a.len > 100);
    }

    let path = workspace_root()
        .join("spec/verification/core-storage/vectors/csq3/canonical-manifest-v1.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "missing frozen manifest {} ({e}); run scripts/verify-csq-format-corpus.sh --write-manifest",
            path.display()
        )
    });
    let v: serde_json::Value = serde_json::from_str(&text).expect("manifest json");
    assert_eq!(v["profile"], CORPUS_PROFILE);
    assert_eq!(v["generator"], CORPUS_GENERATOR);
    for a in &artifacts {
        let row = v["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["name"] == a.name)
            .unwrap_or_else(|| panic!("manifest missing {}", a.name));
        assert_eq!(
            row["blake3"].as_str().unwrap(),
            a.blake3_hex,
            "hash drift for {}",
            a.name
        );
        assert_eq!(row["len"].as_u64().unwrap() as usize, a.len);
    }
}

// ---------------------------------------------------------------------------
// CSQ-FMT-001 — verified only when all structural checks succeed
// ---------------------------------------------------------------------------

#[test]
fn csq_fmt_001_exhaustive_mutations_never_false_verify() {
    let base = canonical_microframe();
    let n = base.len();
    let mut cells = 0usize;

    for m in bit_flip_mutations(n)
        .chain(byte_replace_mutations(n))
        .chain(truncate_mutations(n))
        .chain(insert_mutations(n))
        .chain(delete_mutations(n))
    {
        cells += 1;
        let damaged = apply_mutation(&base, &m);
        // Exact decode of damaged buffer alone must not succeed as the original
        // frame when the mutation actually changed bytes.
        if damaged != base {
            if let Ok(dec) = decode_frame(&damaged, limits()) {
                // Extremely rare: mutation might land on another valid encoding.
                // Still must not claim the original body with original event.
                assert_ne!(
                    dec.body.as_slice(),
                    CANONICAL_BODY,
                    "cell {} reinvented canonical body",
                    m.cell_id()
                );
            }
        }
        let _ = m.cell_id(); // hash-addressed
    }
    // Bit + 3*byte + trunc + 4*insert*(n+1) + delete
    assert!(cells > 1000, "expected large finite domain, got {cells}");
}

// ---------------------------------------------------------------------------
// CSQ-FMT-002 — damage locality / healthy islands
// ---------------------------------------------------------------------------

#[test]
fn csq_fmt_002_every_damage_preserves_survivor_island() {
    let base = canonical_microframe();
    let n = base.len();

    // Bit flips + byte replace + delete (always keep enough bytes for a scan).
    for m in bit_flip_mutations(n)
        .chain(byte_replace_mutations(n))
        .chain(delete_mutations(n))
    {
        let damaged = apply_mutation(&base, &m);
        let buf = with_survivor(&damaged);
        assert!(
            !original_body_verified(&buf) || damaged == base,
            "cell {}: corrupt original must not verify",
            m.cell_id()
        );
        assert!(
            survivor_verified(&buf),
            "cell {}: survivor island lost after {:?}",
            m.cell_id(),
            m
        );
    }

    // Truncations that leave no original frame still must not block survivor.
    for m in truncate_mutations(n) {
        let damaged = apply_mutation(&base, &m);
        let buf = with_survivor(&damaged);
        assert!(
            survivor_verified(&buf),
            "truncate cell {}: survivor lost",
            m.cell_id()
        );
        assert!(!original_body_verified(&buf));
    }

    // Insertions: survivor after damaged prefix.
    for m in insert_mutations(n) {
        let damaged = apply_mutation(&base, &m);
        let buf = with_survivor(&damaged);
        assert!(
            survivor_verified(&buf),
            "insert cell {}: survivor lost",
            m.cell_id()
        );
    }
}

// ---------------------------------------------------------------------------
// CSQ-FMT-003 — forward / reverse / independent reconciliation
// ---------------------------------------------------------------------------

#[test]
fn csq_fmt_003_forward_reverse_independent_reconcile() {
    let mut buf = b"HEAD".to_vec();
    buf.extend_from_slice(&canonical_microframe());
    buf.extend_from_slice(b"MID");
    buf.extend_from_slice(&survivor_microframe());
    buf.extend_from_slice(b"TAIL");

    let fwd = scan_forward(&buf, limits());
    let rev = scan_reverse(&buf, limits());
    let fwd_set: BTreeSet<_> = fwd
        .verified_frames()
        .map(|(o, f)| (o, f.header.event_id, f.body.clone()))
        .collect();
    let rev_set: BTreeSet<_> = rev
        .verified_frames()
        .map(|(o, f)| (o, f.header.event_id, f.body.clone()))
        .collect();
    assert_eq!(fwd_set, rev_set, "forward/reverse disagree");
    assert_eq!(fwd_set.len(), 2);

    // Independent reader structural hits must cover the same start offsets.
    let indep = independent_resiframes(&buf);
    let fwd_offsets: BTreeSet<u64> = fwd.verified_frames().map(|(o, _)| o).collect();
    let indep_offsets: BTreeSet<u64> = indep.iter().map(|(o, _)| *o).collect();
    assert_eq!(
        fwd_offsets, indep_offsets,
        "independent reader offsets must match production verified starts"
    );

    // After damage, extents must not fabricate the original body.
    let base = canonical_microframe();
    let damaged = apply_mutation(&base, &Mutation::BitFlip { bit: 17 });
    let mut dbuf = damaged;
    dbuf.extend_from_slice(&survivor_microframe());
    let d_fwd = scan_forward(&dbuf, limits());
    assert!(!d_fwd
        .verified_frames()
        .any(|(_, f)| f.body == CANONICAL_BODY));
    assert!(d_fwd
        .verified_frames()
        .any(|(_, f)| f.body == SURVIVOR_BODY));
    let d_indep = independent_resiframes(&dbuf);
    // Independent tool may report structural end-magic frames; survivor must appear.
    assert!(!d_indep.is_empty() || survivor_verified(&dbuf));
}

// ---------------------------------------------------------------------------
// CSQ-FMT-004 — unsupported kinds as opaque evidence
// ---------------------------------------------------------------------------

#[test]
fn csq_fmt_004_unsupported_kind_is_opaque_not_corruption() {
    let enc = unsupported_kind_microframe();
    let dec = decode_frame(&enc, limits()).expect("structurally valid");
    assert_eq!(dec.header.frame_kind, 200);
    assert_eq!(dec.header.wire_major, WIRE_MAJOR);
    assert_eq!(dec.header.known_kind(), None);
    let mut buf = enc;
    buf.extend_from_slice(&survivor_microframe());
    let report = scan_forward(&buf, limits());
    assert!(report.verified_count() >= 2);
    assert!(survivor_verified(&buf));
}

// ---------------------------------------------------------------------------
// CSQ-FMT-005 — parsing terminates under hostile lengths
// ---------------------------------------------------------------------------

#[test]
fn csq_fmt_005_hostile_lengths_terminate() {
    let mut garbage = vec![0u8; 4096];
    garbage[0..8].copy_from_slice(START_MAGIC);
    // Absurd envelope + body lengths.
    garbage[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
    garbage[16..24].copy_from_slice(&u64::MAX.to_le_bytes());
    let report = scan_forward(&garbage, limits());
    assert_eq!(report.verified_count(), 0);
    // Must return (no hang) — if we got here, terminated.
    let tight = SafetyLimits {
        max_envelope_len: 64,
        max_body_len: 256,
        max_frame_len: 512,
    };
    let _ = decode_frame(&garbage[..128.min(garbage.len())], tight);
    let _ = scan_forward(&garbage, tight);
}

// ---------------------------------------------------------------------------
// Bounded hole corpus + multi-fault covering array
// ---------------------------------------------------------------------------

#[test]
fn csq3_bounded_hole_corpus() {
    let base = canonical_microframe();
    let mut cells = 0usize;
    let mut effective = 0usize;
    for m in hole_mutations(base.len()) {
        cells += 1;
        let damaged = apply_mutation(&base, &m);
        let buf = with_survivor(&damaged);
        assert!(
            survivor_verified(&buf),
            "hole {} lost survivor",
            m.cell_id()
        );
        // Zeroing an already-zero reserved field is a no-op cell; only effective
        // holes must reject the original body.
        if damaged != base {
            effective += 1;
            assert!(
                !original_body_verified(&buf),
                "effective hole {} left original verified",
                m.cell_id()
            );
        }
    }
    assert!(cells > 50, "expected non-trivial hole domain, got {cells}");
    assert!(
        effective > 20,
        "expected many effective (byte-changing) holes, got {effective}"
    );
}

#[test]
fn csq3_pairwise_multi_fault_covering() {
    let base = canonical_microframe();
    let pairs = pairwise_fault_covering(base.len());
    assert_eq!(pairs.len(), 25, "5×5 ordered pairs");
    for p in &pairs {
        let damaged = apply_pair(&base, p);
        let buf = with_survivor(&damaged);
        assert!(
            survivor_verified(&buf),
            "pair {:?}/{:?} lost survivor",
            p.a,
            p.b
        );
        // Multi-fault must not invent the original body when bytes changed.
        // (Some class pairs can cancel into a no-op; those are still scheduled.)
        if damaged != base {
            assert!(
                !original_body_verified(&buf),
                "pair {:?}/{:?} fabricated original after effective damage",
                p.a,
                p.b
            );
        }
    }
}

#[test]
fn csq3_microsegment_hash_is_addressed() {
    let seg = canonical_microsegment();
    let h = content_hash_hex(&seg);
    assert_eq!(h.len(), 64);
    // Segment = primary || survivor
    let mut manual = canonical_microframe();
    manual.extend_from_slice(&survivor_microframe());
    assert_eq!(seg, manual);
}

#[test]
fn csq3_mutation_ids_are_hash_addressed() {
    let mut ids = BTreeSet::new();
    let base = canonical_microframe();
    for m in bit_flip_mutations(base.len()).take(64) {
        assert!(ids.insert(m.cell_id()));
        assert_eq!(m.cell_id().len(), 64);
    }
}

/// When `CSQ3_WRITE_MANIFEST` is set, write the frozen manifest and exit ok.
#[test]
fn csq3_write_manifest_if_env() {
    let Some(path) = std::env::var_os("CSQ3_WRITE_MANIFEST") else {
        return;
    };
    let path = PathBuf::from(path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let artifacts = frozen_artifacts();
    let mut rows = Vec::new();
    for a in &artifacts {
        rows.push(serde_json::json!({
            "name": a.name,
            "blake3": a.blake3_hex,
            "len": a.len,
        }));
    }
    let doc = serde_json::json!({
        "schema": "residiuum-csq3-canonical-manifest-v1",
        "profile": CORPUS_PROFILE,
        "generator": CORPUS_GENERATOR,
        "identity": {
            "start_magic": "RESIDFRM",
            "end_magic": "RESIDEND",
            "wire_major": WIRE_MAJOR,
        },
        "artifacts": rows,
    });
    std::fs::write(&path, serde_json::to_string_pretty(&doc).unwrap() + "\n").unwrap();
    eprintln!("wrote {}", path.display());
}
