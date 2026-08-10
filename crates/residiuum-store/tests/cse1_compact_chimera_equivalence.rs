//! CSE-1 — Compact Chimera salvage equivalence campaign (historical FAIL).
//!
//! Runs the frozen CSE-0 failure set `F` against **Compact SegmentFrame**
//! layouts (installed explicitly — product seal is Materialized after CSE-2R)
//! and compares recoverable sets to the Materialized baseline.
//!
//! Required inequality (per channel):
//!   Recoverable_compact(f) ⊇ Recoverable_materialized(f)
//!
//! Charter: `doc/todo/performance-qualification/CHIMERA_SALVAGE_EQUIVALENCE.md`.
//! CSE-0 archive: `doc/archive/performance-qualification/2026-08-04-cse0-materialized-recovery-baseline/`.
//!
//! Honest scope: measure + compare. Documents Compact FAIL → CSE-2R rollback / CSE-3 recovery.

use residiuum_format::{scan_forward, verify_frame_at, FrameKind, SafetyLimits};
use residiuum_store::{
    build_compact_layout, chimera_dir, chimera_layout_path, decode_item_envelope, hex16,
    segment_id_from_filename, write_chimera_layout, CompactFrameRef, DurabilityMode, EventKind,
    Store,
};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

const KEYS: [&str; 3] = ["t", "m", "l"];

fn expected_body(key: &str) -> Vec<u8> {
    match key {
        "t" => b"tiny-cse1".to_vec(),
        "m" => vec![0x3cu8; 200],
        "l" => vec![0x5au8; 32 * 1024],
        _ => panic!("unknown key {key}"),
    }
}

/// Frozen Materialized recoverable sets from CSE-0 `baseline.json` (RHS).
fn materialized_baseline(failure: &str) -> (BTreeSet<String>, BTreeSet<String>, BTreeSet<String>) {
    match failure {
        "F0_control" => (
            bset(&["l", "m", "t"]),
            bset(&["l", "m", "t"]),
            bset(&["l", "m", "t"]),
        ),
        "F1_wipe_chimera" => (bset(&["l", "m", "t"]), bset(&[]), bset(&[])),
        "F2_corrupt_chimera" => (bset(&["l", "m", "t"]), bset(&[]), bset(&[])),
        "F3_corrupt_auth_body_t" => (
            bset(&["l", "m"]),
            bset(&["l", "m", "t"]),
            bset(&["l", "m", "t"]),
        ),
        "F4_delete_sealed_segment" => (bset(&[]), bset(&[]), bset(&["l", "m", "t"])),
        "F5_corrupt_auth_t_wipe_chimera" => (bset(&["l", "m"]), bset(&[]), bset(&[])),
        other => panic!("unknown CSE-0 failure id {other}"),
    }
}

struct Fixture {
    root: PathBuf,
    store_id: [u8; 16],
    segment_id: [u8; 16],
    frame_offsets: Vec<(String, u64)>,
}

fn sealed_segment_path(root: &Path, segment_id: &[u8; 16]) -> PathBuf {
    Store::open(root)
        .unwrap()
        .paths()
        .sealed_segment(segment_id)
}

/// Seed store then **install Compact** over product seal (CSE-1 Compact campaign).
/// After CSE-2R, product seal writes Materialized; this test still measures Compact.
fn seed_compact_fixture() -> Fixture {
    let dir = tempdir().unwrap();
    let root = dir.keep();

    let mut store = Store::create(&root).unwrap();
    for k in KEYS {
        store
            .put(k, &expected_body(k), DurabilityMode::Durable)
            .unwrap();
    }
    let store_id = store.store_id();
    store.seal_active().unwrap();
    drop(store);

    let seg_path = {
        let segs = root.join("segments");
        fs::read_dir(&segs)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().and_then(|x| x.to_str()) == Some("residiuum"))
            .expect("sealed segment")
    };
    let segment_id = segment_id_from_filename(&seg_path).expect("segment id from filename");

    let bytes = fs::read(&seg_path).unwrap();
    let report = scan_forward(&bytes, SafetyLimits::default());
    let mut last: std::collections::BTreeMap<String, (u64, u32)> =
        std::collections::BTreeMap::new();
    for (off, frame) in report.verified_frames() {
        if frame.header.known_kind() != Some(FrameKind::ItemEvent) {
            continue;
        }
        let Some(env) = decode_item_envelope(&frame.envelope) else {
            continue;
        };
        if env.event_kind != EventKind::Put {
            continue;
        }
        let subj = String::from_utf8(env.subject).expect("utf8 subject");
        if KEYS.contains(&subj.as_str()) {
            last.insert(subj, (off, frame.body.len() as u32));
        }
    }
    let frame_offsets: Vec<(String, u64)> = KEYS
        .iter()
        .map(|k| {
            (
                (*k).to_string(),
                last.get(*k)
                    .map(|(o, _)| *o)
                    .unwrap_or_else(|| panic!("missing frame for {k}")),
            )
        })
        .collect();

    // Overwrite product Materialized seal with Compact SegmentFrame (CSE-1 LHS).
    let frames: Vec<(Vec<u8>, CompactFrameRef)> = KEYS
        .iter()
        .map(|k| {
            let (off, body_len) = *last.get(*k).unwrap_or_else(|| panic!("missing {k}"));
            (
                k.as_bytes().to_vec(),
                CompactFrameRef {
                    segment_id,
                    frame_offset: off,
                    body_len,
                },
            )
        })
        .collect();
    let layout = build_compact_layout(&frames, 1);
    assert!(layout.count_by_kind().segment_frame >= 3);
    let store = Store::open(&root).unwrap();
    let path = chimera_layout_path(store.paths(), &segment_id);
    write_chimera_layout(&path, store_id, segment_id, &layout).unwrap();
    let loaded = store
        .load_chimera_layout(segment_id)
        .unwrap()
        .expect("compact layout present");
    let counts = loaded.count_by_kind();
    assert!(
        counts.segment_frame >= 3,
        "CSE-1 requires Compact SegmentFrame layout (got {counts:?})"
    );
    assert_eq!(
        counts.inline + counts.point_container + counts.large_value_log,
        0
    );
    assert!(
        loaded.get(b"t").is_err() || loaded.get(b"t").ok().flatten().is_none(),
        "Compact layout.get must not embed t body"
    );
    drop(store);

    Fixture {
        root,
        store_id,
        segment_id,
        frame_offsets,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Channel {
    AuthGet,
    ChimeraGet,
}

fn recoverable(store: &Store, channel: Channel) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for k in KEYS {
        let want = expected_body(k);
        let got = match channel {
            Channel::AuthGet => store.get(k).ok().flatten(),
            Channel::ChimeraGet => store.get_via_chimera(k).ok().flatten(),
        };
        if got.as_deref() == Some(want.as_slice()) {
            out.insert(k.to_string());
        }
    }
    out
}

fn xor_frame_body(path: &Path, frame_offset: u64, xor: u8) {
    let mut bytes = fs::read(path).unwrap();
    let off = frame_offset as usize;
    assert!(off < bytes.len(), "frame_offset out of range");
    let (_h, _e, body, _hash, _flen) = verify_frame_at(&bytes[off..], SafetyLimits::default())
        .expect("frame must verify before damage");
    let body_rel = body.as_ptr() as usize - bytes[off..].as_ptr() as usize;
    let start = off + body_rel;
    let end = (start + body.len().min(64)).max(start + 1);
    assert!(end <= bytes.len());
    for b in &mut bytes[start..end] {
        *b ^= xor;
    }
    fs::write(path, &bytes).unwrap();
}

fn corrupt_chimera_bytes(root: &Path, segment_id: &[u8; 16]) {
    let path = Store::open(root)
        .unwrap()
        .paths()
        .indexes_dir()
        .join("chimera")
        .join(format!("{}.cmr", hex16(segment_id)));
    let mut bytes = fs::read(&path).unwrap();
    assert!(bytes.len() > 32);
    for b in &mut bytes[24..40] {
        *b ^= 0xa5;
    }
    fs::write(&path, &bytes).unwrap();
}

fn wipe_chimera(root: &Path) {
    let paths = Store::open(root).unwrap().paths().clone();
    let dir = chimera_dir(&paths);
    if dir.is_dir() {
        fs::remove_dir_all(&dir).unwrap();
    }
}

#[derive(Debug, Clone, Copy)]
enum Failure {
    Control,
    WipeChimera,
    CorruptChimera,
    CorruptAuthBodyT,
    DeleteSealedSegment,
    CorruptAuthBodyTAndWipeChimera,
}

impl Failure {
    fn id(self) -> &'static str {
        match self {
            Self::Control => "F0_control",
            Self::WipeChimera => "F1_wipe_chimera",
            Self::CorruptChimera => "F2_corrupt_chimera",
            Self::CorruptAuthBodyT => "F3_corrupt_auth_body_t",
            Self::DeleteSealedSegment => "F4_delete_sealed_segment",
            Self::CorruptAuthBodyTAndWipeChimera => "F5_corrupt_auth_t_wipe_chimera",
        }
    }

    fn all() -> &'static [Failure] {
        &[
            Self::Control,
            Self::WipeChimera,
            Self::CorruptChimera,
            Self::CorruptAuthBodyT,
            Self::DeleteSealedSegment,
            Self::CorruptAuthBodyTAndWipeChimera,
        ]
    }
}

fn apply_failure(fx: &Fixture, f: Failure) {
    let seg_path = sealed_segment_path(&fx.root, &fx.segment_id);
    match f {
        Failure::Control => {}
        Failure::WipeChimera => wipe_chimera(&fx.root),
        Failure::CorruptChimera => corrupt_chimera_bytes(&fx.root, &fx.segment_id),
        Failure::CorruptAuthBodyT => {
            let off = fx
                .frame_offsets
                .iter()
                .find(|(k, _)| k == "t")
                .map(|(_, o)| *o)
                .expect("t offset");
            xor_frame_body(&seg_path, off, 0x5a);
        }
        Failure::DeleteSealedSegment => {
            fs::remove_file(&seg_path).unwrap();
        }
        Failure::CorruptAuthBodyTAndWipeChimera => {
            let off = fx
                .frame_offsets
                .iter()
                .find(|(k, _)| k == "t")
                .map(|(_, o)| *o)
                .expect("t offset");
            xor_frame_body(&seg_path, off, 0x3c);
            wipe_chimera(&fx.root);
        }
    }
}

struct Outcome {
    failure: &'static str,
    auth: BTreeSet<String>,
    chimera: BTreeSet<String>,
    /// Format-only: `load_chimera_layout` + `ChimeraLayout::get` (no store pread).
    /// Compact SegmentFrame locators yield empty here by design.
    layout_direct: BTreeSet<String>,
}

fn recoverable_layout_direct(store: &Store, segment_id: [u8; 16]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let Ok(Some(layout)) = store.load_chimera_layout(segment_id) else {
        return out;
    };
    for k in KEYS {
        let want = expected_body(k);
        if layout.get(k.as_bytes()).ok().flatten().as_deref() == Some(want.as_slice()) {
            out.insert(k.to_string());
        }
    }
    out
}

fn run_one(f: Failure) -> Outcome {
    let fx = seed_compact_fixture();
    apply_failure(&fx, f);
    let store = Store::open(&fx.root).unwrap();
    assert_eq!(store.store_id(), fx.store_id);
    Outcome {
        failure: f.id(),
        auth: recoverable(&store, Channel::AuthGet),
        chimera: recoverable(&store, Channel::ChimeraGet),
        layout_direct: recoverable_layout_direct(&store, fx.segment_id),
    }
}

fn bset(keys: &[&str]) -> BTreeSet<String> {
    keys.iter().map(|k| (*k).to_string()).collect()
}

fn set_json(set: &BTreeSet<String>) -> String {
    set.iter()
        .map(|k| format!("\"{k}\""))
        .collect::<Vec<_>>()
        .join(",")
}

fn missing(lhs: &BTreeSet<String>, rhs: &BTreeSet<String>) -> BTreeSet<String> {
    rhs.difference(lhs).cloned().collect()
}

#[test]
fn cse1_compact_equivalence_campaign() {
    let mut rows = Vec::new();
    for &f in Failure::all() {
        rows.push(run_one(f));
    }

    let mut gaps: Vec<String> = Vec::new();
    for r in &rows {
        let (m_auth, m_chimera, m_layout) = materialized_baseline(r.failure);
        for (ch, compact, mat) in [
            ("auth", &r.auth, &m_auth),
            ("chimera", &r.chimera, &m_chimera),
            ("layout_direct", &r.layout_direct, &m_layout),
        ] {
            let miss = missing(compact, mat);
            if !miss.is_empty() {
                gaps.push(format!(
                    "{}:{} missing {{{}}}",
                    r.failure,
                    ch,
                    miss.iter().cloned().collect::<Vec<_>>().join(",")
                ));
            }
        }
    }

    let equivalence_holds = gaps.is_empty();
    let summary = format!(
        "{{\"package\":\"CSE-1\",\"equivalence_holds\":{},\"requires_cse3\":{},\"gaps\":[{}],\"compact\":[{}],\"materialized_rhs\":\"CSE-0 baseline.json\"}}",
        equivalence_holds,
        !equivalence_holds,
        gaps.iter()
            .map(|g| format!("\"{}\"", g.replace('"', "\\\"")))
            .collect::<Vec<_>>()
            .join(","),
        rows.iter()
            .map(|r| {
                format!(
                    "{{\"id\":\"{}\",\"auth\":[{}],\"chimera\":[{}],\"layout_direct\":[{}]}}",
                    r.failure,
                    set_json(&r.auth),
                    set_json(&r.chimera),
                    set_json(&r.layout_direct)
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    );
    eprintln!("CSE1_EQUIVALENCE_JSON={summary}");

    // --- Sanity: Compact auth tracks Materialized auth (segment authority) ---
    let f0 = rows.iter().find(|r| r.failure == "F0_control").unwrap();
    assert_eq!(f0.auth, bset(&["t", "m", "l"]));
    assert_eq!(f0.chimera, bset(&["t", "m", "l"]));
    // Compact format-only channel cannot resolve SegmentFrame without pread.
    assert!(
        f0.layout_direct.is_empty(),
        "Compact layout_direct must be empty (SegmentFrame needs store pread); got {:?}",
        f0.layout_direct
    );

    let f3 = rows
        .iter()
        .find(|r| r.failure == "F3_corrupt_auth_body_t")
        .unwrap();
    assert!(!f3.auth.contains("t"));
    // Compact Chimera points at the same damaged frame → cannot expand salvage for t.
    assert!(
        !f3.chimera.contains("t"),
        "Compact must NOT recover damaged t via ChimeraGet (no embedded body); got {:?}",
        f3.chimera
    );
    assert!(f3.chimera.contains("m") && f3.chimera.contains("l"));

    let f4 = rows
        .iter()
        .find(|r| r.failure == "F4_delete_sealed_segment")
        .unwrap();
    assert!(f4.auth.is_empty());
    assert!(f4.chimera.is_empty());
    assert!(
        f4.layout_direct.is_empty(),
        "Compact has no embedded payloads → layout_direct empty under F4"
    );

    // Campaign complete: inequality outcome is the deliverable (not package accept).
    // Current Compact is expected to fail ⊇ on F0/F3/F4 layout_direct and F3 chimera.
    assert!(
        !equivalence_holds,
        "unexpected: Compact held Materialized recoverability — re-check CSE-3 need; gaps={gaps:?}"
    );
    assert!(
        gaps.iter()
            .any(|g| g.contains("F3_corrupt_auth_body_t:chimera")),
        "expected F3 chimera gap (Materialized expands salvage); gaps={gaps:?}"
    );
    assert!(
        gaps.iter().any(|g| g.contains("layout_direct")),
        "expected layout_direct gap(s); gaps={gaps:?}"
    );
}
