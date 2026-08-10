//! CSE-2R — Product Chimera **safety rollback** (NOT Compact parity).
//!
//! After CSE-1 proved Compact SegmentFrame fails salvage equivalence, product
//! seal/enrichment was restored to Materialized embeds (`Product_new =
//! Product_old`). That restores product safety; Compact recovery remains
//! unresolved. Compact layouts stay available via `build_compact_layout` for
//! ETQ measurement only.
//!
//! This guard re-runs frozen F0–F5 on the **product seal** path and asserts
//! recoverable sets match the CSE-0 Materialized RHS — expected because the
//! product path *is* Materialized again, not because Compact gained parity.
//!
//! Charter: `doc/todo/performance-qualification/CHIMERA_SALVAGE_EQUIVALENCE.md`.
//! Next: CSE-3 Compact + explicit recovery code.

use residiuum_format::{scan_forward, verify_frame_at, FrameKind, SafetyLimits};
use residiuum_store::{
    chimera_dir, decode_item_envelope, hex16, segment_id_from_filename, DurabilityMode, EventKind,
    Store,
};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

const KEYS: [&str; 3] = ["t", "m", "l"];

fn expected_body(key: &str) -> Vec<u8> {
    match key {
        "t" => b"tiny-cse2".to_vec(),
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

/// Seed via product seal — CSE-2R expects Materialized embed restore (not Compact).
fn seed_product_seal_fixture() -> Fixture {
    let dir = tempdir().unwrap();
    let root = dir.keep();

    let mut store =
        Store::create_with_shards_mode(&root, 1, residiuum_store::RecoveryMode::Materialized)
            .unwrap();
    for k in KEYS {
        store
            .put(k, &expected_body(k), DurabilityMode::Durable)
            .unwrap();
    }
    let store_id = store.store_id();
    store.seal_active().unwrap();
    store.drain_lifecycle().unwrap();
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
    let mut last: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
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
            last.insert(subj, off);
        }
    }
    let frame_offsets: Vec<(String, u64)> = KEYS
        .iter()
        .map(|k| {
            (
                (*k).to_string(),
                *last
                    .get(*k)
                    .unwrap_or_else(|| panic!("missing frame for {k}")),
            )
        })
        .collect();

    let store = Store::open(&root).unwrap();
    let loaded = store
        .load_chimera_layout(segment_id)
        .unwrap()
        .expect("product Chimera after seal");
    let counts = loaded.count_by_kind();
    assert_eq!(
        counts.segment_frame, 0,
        "CSE-2R product seal must be Materialized embed, not Compact SegmentFrame (got {counts:?})"
    );
    assert!(
        counts.inline + counts.point_container + counts.large_value_log >= 3,
        "CSE-2R product seal must embed payloads (got {counts:?})"
    );
    assert_eq!(
        loaded.get(b"t").unwrap().as_deref(),
        Some(expected_body("t").as_slice())
    );
    drop(store);

    Fixture {
        root,
        store_id,
        segment_id,
        frame_offsets,
    }
}

#[derive(Debug, Clone, Copy)]
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
    let fx = seed_product_seal_fixture();
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
fn cse2r_product_seal_safety_rollback_guard() {
    let mut rows = Vec::new();
    for &f in Failure::all() {
        rows.push(run_one(f));
    }

    let mut gaps: Vec<String> = Vec::new();
    for r in &rows {
        let (m_auth, m_chimera, m_layout) = materialized_baseline(r.failure);
        for (ch, product, mat) in [
            ("auth", &r.auth, &m_auth),
            ("chimera", &r.chimera, &m_chimera),
            ("layout_direct", &r.layout_direct, &m_layout),
        ] {
            let miss = missing(product, mat);
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

    // Product path matches Materialized RHS because it *is* Materialized again.
    let product_matches_materialized = gaps.is_empty();
    let summary = format!(
        "{{\"package\":\"CSE-2R\",\"classification\":\"safety_rollback\",\"not\":\"compact_minimum_parity\",\"product_matches_materialized_rhs\":{},\"compact_equivalence_holds\":false,\"gaps\":[{}],\"product\":[{}],\"note\":\"Product_new=Product_old Materialized restore; Compact still unresolved; ETQ-2 paused\"}}",
        product_matches_materialized,
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
    eprintln!("CSE2R_ROLLBACK_JSON={summary}");

    assert!(
        product_matches_materialized,
        "CSE-2R safety rollback: product Materialized seal must match CSE-0 RHS; gaps={gaps:?}"
    );

    // Materialized salvage properties must hold on the product path again.
    let f3 = rows
        .iter()
        .find(|r| r.failure == "F3_corrupt_auth_body_t")
        .unwrap();
    assert!(
        f3.chimera.contains("t"),
        "F3 ChimeraGet must expand salvage for damaged t (Materialized product)"
    );
    assert!(f3.layout_direct.contains("t"));

    let f4 = rows
        .iter()
        .find(|r| r.failure == "F4_delete_sealed_segment")
        .unwrap();
    assert_eq!(f4.layout_direct, bset(&["t", "m", "l"]));
}
