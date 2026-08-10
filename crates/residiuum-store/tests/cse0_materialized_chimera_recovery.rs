//! CSE-0 — Materialized Chimera recovery baseline.
//!
//! Freezes failure set `F` and recovery oracle for Chimera Salvage Equivalence.
//! Charter: `doc/todo/performance-qualification/CHIMERA_SALVAGE_EQUIVALENCE.md`.
//!
//! Honest scope: measure what **Materialized** Chimera recovers under controlled
//! damage vs PrimaryIndex/segment authority. Does **not** claim product default
//! or Compact durability.

use residiuum_format::{scan_forward, verify_frame_at, FrameKind, SafetyLimits};
use residiuum_store::{
    build_materialized_layout, chimera_dir, chimera_layout_path, decode_item_envelope, hex16,
    segment_id_from_filename, write_chimera_layout, ClassifyOptions, DurabilityMode, EventKind,
    Store,
};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

const KEYS: [&str; 3] = ["t", "m", "l"];

fn expected_body(key: &str) -> Vec<u8> {
    match key {
        "t" => b"tiny-cse0".to_vec(),
        "m" => vec![0x3cu8; 200],
        "l" => vec![0x5au8; 32 * 1024],
        _ => panic!("unknown key {key}"),
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

fn seed_materialized_fixture() -> Fixture {
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

    // Install Materialized Chimera (CSE-0 baseline format) over Compact seal output.
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = KEYS
        .iter()
        .map(|k| (k.as_bytes().to_vec(), expected_body(k)))
        .collect();
    let layout = build_materialized_layout(&pairs, 1, &ClassifyOptions::default());
    assert_eq!(layout.count_by_kind().segment_frame, 0);
    assert!(layout.count_by_kind().inline >= 1);
    let store = Store::open(&root).unwrap();
    let path = chimera_layout_path(store.paths(), &segment_id);
    write_chimera_layout(&path, store_id, segment_id, &layout).unwrap();
    let loaded = store
        .load_chimera_layout(segment_id)
        .unwrap()
        .expect("materialized layout present");
    assert_eq!(loaded.count_by_kind().segment_frame, 0);
    assert_eq!(loaded.get(b"t").unwrap().unwrap(), expected_body("t"));
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
    /// Product `Store::get` (PrimaryIndex → segment pread).
    AuthGet,
    /// Explicit `Store::get_via_chimera` (Materialized embedded resolve).
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

/// Frozen failure ids (CSE-0).
#[derive(Debug, Clone, Copy)]
enum Failure {
    /// F0 — control: no damage.
    Control,
    /// F1 — wipe all Chimera sidecars.
    WipeChimera,
    /// F2 — corrupt Materialized `.cmr` bytes (fail-closed load).
    CorruptChimera,
    /// F3 — XOR establishing item body for key `t` in the sealed segment.
    CorruptAuthBodyT,
    /// F4 — delete sealed segment file (Chimera left intact).
    DeleteSealedSegment,
    /// F5 — corrupt auth body `t` **and** wipe Chimera.
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
    /// Format-level Materialized resolve via `load_chimera_layout` + `get`
    /// (no PrimaryIndex / segment pread). Empty when sidecar missing/corrupt.
    layout_direct: BTreeSet<String>,
}

/// Recoverable set from Materialized `.cmr` alone (CSE format oracle).
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
    let fx = seed_materialized_fixture();
    apply_failure(&fx, f);
    // Reopen to force cold paths (no active-writer tail).
    let store = Store::open(&fx.root).unwrap();
    assert_eq!(store.store_id(), fx.store_id);
    Outcome {
        failure: f.id(),
        auth: recoverable(&store, Channel::AuthGet),
        chimera: recoverable(&store, Channel::ChimeraGet),
        layout_direct: recoverable_layout_direct(&store, fx.segment_id),
    }
}

#[test]
fn cse0_materialized_recovery_baseline_matrix() {
    let mut rows = Vec::new();
    for &f in Failure::all() {
        rows.push(run_one(f));
    }

    // --- Oracle freezes (assert + document) ---

    let f0 = rows.iter().find(|r| r.failure == "F0_control").unwrap();
    assert_eq!(f0.auth, bset(&["t", "m", "l"]));
    assert_eq!(f0.chimera, bset(&["t", "m", "l"]));

    let f1 = rows
        .iter()
        .find(|r| r.failure == "F1_wipe_chimera")
        .unwrap();
    assert_eq!(
        f1.auth,
        bset(&["t", "m", "l"]),
        "auth must not depend on Chimera"
    );
    assert!(f1.chimera.is_empty(), "no Chimera → chimera channel empty");

    let f2 = rows
        .iter()
        .find(|r| r.failure == "F2_corrupt_chimera")
        .unwrap();
    assert_eq!(f2.auth, bset(&["t", "m", "l"]));
    assert!(
        f2.chimera.is_empty(),
        "corrupt Materialized sidecar must fail-closed on chimera channel"
    );

    let f3 = rows
        .iter()
        .find(|r| r.failure == "F3_corrupt_auth_body_t")
        .unwrap();
    // Product auth path must not return a wrong body for damaged `t`.
    assert!(
        !f3.auth.contains("t"),
        "auth get must not recover damaged frame body for t (got {:?})",
        f3.auth
    );
    // Materialized Chimera embeds payloads → chimera channel may still recover `t`.
    assert!(
        f3.chimera.contains("t"),
        "Materialized Chimera must still yield exact t from embedded layout under F3"
    );
    // Undamaged keys remain on both channels when their frames are intact.
    assert!(f3.auth.contains("m") && f3.auth.contains("l"));
    assert!(f3.chimera.contains("m") && f3.chimera.contains("l"));

    let f4 = rows
        .iter()
        .find(|r| r.failure == "F4_delete_sealed_segment")
        .unwrap();
    assert!(
        f4.auth.is_empty(),
        "deleted sealed segment → auth cannot recover (got {:?})",
        f4.auth
    );
    // Honest product reopen: `get_via_chimera` requires a PrimaryIndex live
    // entry. Deleting the sealed segment invalidates/rebuilds the index with
    // no live keys → chimera *channel* is empty even though `.cmr` embeds bodies.
    assert!(
        f4.chimera.is_empty(),
        "product ChimeraGet needs index live entry; empty after segment delete (got {:?})",
        f4.chimera
    );
    assert_eq!(
        f4.layout_direct,
        bset(&["t", "m", "l"]),
        "Materialized *format* still resolves all keys from `.cmr` alone under F4"
    );

    // F3 also: layout-direct recovers damaged `t` (format property).
    assert!(
        f3.layout_direct.contains("t"),
        "Materialized layout-direct must recover t under F3"
    );

    let f5 = rows
        .iter()
        .find(|r| r.failure == "F5_corrupt_auth_t_wipe_chimera")
        .unwrap();
    assert!(!f5.auth.contains("t"));
    assert!(!f5.chimera.contains("t"));
    assert!(!f5.layout_direct.contains("t"));
    // No fabricated exact `t` when both auth body and Chimera are gone.

    // Emit machine-readable baseline for archive / CSE-1.
    let summary = serde_json_summary(&rows);
    eprintln!("CSE0_BASELINE_JSON={summary}");
}

fn bset(keys: &[&str]) -> BTreeSet<String> {
    keys.iter().map(|k| (*k).to_string()).collect()
}

fn serde_json_summary(rows: &[Outcome]) -> String {
    // Tiny JSON without pulling serde into test deps beyond what's already there.
    let mut s = String::from("{\"failures\":[");
    for (i, r) in rows.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            "{{\"id\":\"{}\",\"auth\":[{}],\"chimera\":[{}],\"layout_direct\":[{}]}}",
            r.failure,
            set_json(&r.auth),
            set_json(&r.chimera),
            set_json(&r.layout_direct)
        ));
    }
    s.push_str("]}");
    s
}

fn set_json(set: &BTreeSet<String>) -> String {
    set.iter()
        .map(|k| format!("\"{k}\""))
        .collect::<Vec<_>>()
        .join(",")
}
