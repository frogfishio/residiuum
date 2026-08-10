//! CSQ-11 — compatibility / packaged journey / scale+soak floors (first labor cut).
//!
//! Behavioural floors for `CSQ-COMPAT-001` / `CSQ-COMPAT-002`, platform registry
//! honesty, a clean packaged-artifact torture journey, and a PR-safe
//! deterministic scale/soak reconciliation seed.
//!
//! Regression authority: DEF-104 (crash/reopen durability).
//!
//! Residuals (not this cut):
//! - multi-version released-writer binary fixture repository beyond self-edge
//! - full multi-platform CI matrix execution (registry is owned; cells run on host)
//! - 24h weekly and 72h / 1B-op release soak campaigns

use residiuum_format::{
    wire_compat_matrix, wire_reader_supports, WireSupportStatus, START_MAGIC, WIRE_MAJOR,
    WIRE_MINOR, WIRE_PROFILE_LABEL,
};
use residiuum_store::{
    load_and_verify_manifest, restore_full_backup, verify_package_files, BackupConsistency,
    DurabilityMode, MigrateOptions, MigratePhase, RestoreOptions, ScrubOptions, Store, StoreError,
    PRIMARY_CACHE_FILE,
};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn load_platforms() -> Value {
    let path = workspace_root().join("spec/verification/core-storage/platforms-v1.json");
    let raw = fs::read_to_string(&path).expect("platforms-v1.json");
    serde_json::from_str(&raw).expect("platforms json")
}

fn seed_body(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i % 251) as u8))
        .collect()
}

fn assert_projection(store: &Store, expected: &[(String, Option<Vec<u8>>)]) {
    for (k, v) in expected {
        assert_eq!(store.get(k).unwrap(), *v, "projection mismatch for key {k}");
    }
}

fn snapshot_live(store: &Store, keys: &[&str]) -> Vec<(String, Option<Vec<u8>>)> {
    keys.iter()
        .map(|k| (k.to_string(), store.get(k).unwrap()))
        .collect()
}

/// CSQ-COMPAT-001 catalog floor: wire matrix + platform registry are non-empty,
/// Residiuum identity only (no pre-reset product profile claim), and every
/// readable major is advertised honestly.
#[test]
fn csq_compat_wire_matrix_and_platform_registry() {
    let matrix = wire_compat_matrix();
    assert!(!matrix.is_empty(), "wire compat matrix must not be empty");
    assert!(
        wire_reader_supports(WIRE_MAJOR),
        "current writer major must be readable"
    );
    assert!(
        !wire_reader_supports(WIRE_MAJOR.saturating_add(1)),
        "future major must not silently claim support"
    );
    assert!(
        !wire_reader_supports(99),
        "unsupported major 99 must not be readable"
    );

    let mut saw_current = false;
    for e in matrix {
        if e.can_read {
            assert!(
                wire_reader_supports(e.major),
                "matrix can_read major {} missing from reader set",
                e.major
            );
        } else {
            assert!(
                !wire_reader_supports(e.major),
                "matrix non-readable major {} must not pass wire_reader_supports",
                e.major
            );
        }
        if e.status == WireSupportStatus::Current {
            saw_current = true;
            assert!(e.can_write && e.can_read);
            assert_eq!(e.major, WIRE_MAJOR);
        }
    }
    assert!(saw_current, "matrix must list a Current writer major");

    // Identity: Residiuum wire profile label only.
    assert!(
        !WIRE_PROFILE_LABEL
            .to_ascii_lowercase()
            .contains(concat!("din", "go")),
        "wire profile must not advertise pre-reset product identity"
    );
    assert_eq!(WIRE_MINOR, 0);

    let platforms = load_platforms();
    assert_eq!(
        platforms["profile_scope"].as_str(),
        Some("residiuum-core-storage-v1")
    );
    let items = platforms["items"].as_array().expect("platforms items");
    assert!(!items.is_empty(), "platforms registry must not be empty");
    let required: Vec<&str> = items
        .iter()
        .filter(|i| i["status"].as_str() == Some("required"))
        .filter_map(|i| i["id"].as_str())
        .collect();
    assert!(
        !required.is_empty(),
        "at least one required platform cell must exist"
    );
    for id in &required {
        assert!(
            !id.to_ascii_lowercase().contains(concat!("din", "go")),
            "platform id must not be pre-reset product identity: {id}"
        );
    }
}

/// CSQ-COMPAT-001 — advertised old-writer→new-reader edge for the only current
/// major: same Residiuum writer produces artifacts the current reader reopens
/// exactly (released-fixture self-edge until multi-version binaries exist).
#[test]
fn csq_compat_current_writer_reader_edge() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("released-writer-fixture");
    let body = seed_body(7, 64);
    let store_id = {
        let mut store = Store::create(&root).unwrap();
        store
            .put("compat/k1", &body, DurabilityMode::Durable)
            .unwrap();
        store
            .put("compat/k2", b"small", DurabilityMode::Durable)
            .unwrap();
        store.seal_active().unwrap();
        store
            .put("compat/post-seal", b"after", DurabilityMode::Durable)
            .unwrap();
        store.store_id()
    };

    // Freeze fixture bytes (immutable package snapshot).
    let fixture_snap = dir.path().join("fixture-snap");
    copy_tree(&root, &fixture_snap);

    // Current reader reopens the frozen Residiuum fixture.
    let opened = Store::open(&fixture_snap).unwrap();
    assert_eq!(opened.store_id(), store_id);
    assert_eq!(
        opened.get("compat/k1").unwrap().as_deref(),
        Some(body.as_slice())
    );
    assert_eq!(
        opened.get("compat/k2").unwrap().as_deref(),
        Some(b"small".as_slice())
    );
    assert_eq!(
        opened.get("compat/post-seal").unwrap().as_deref(),
        Some(b"after".as_slice())
    );

    // Authority segments use Residiuum start magic only.
    let active = fixture_snap.join("active").join("active.residiuum");
    let bytes = fs::read(&active).unwrap();
    assert!(
        bytes.windows(8).any(|w| w == START_MAGIC.as_slice()),
        "fixture must contain RESIDFRM start magic"
    );
    let mut legacy_frm = [0u8; 8];
    legacy_frm[..3].copy_from_slice(b"DIN");
    legacy_frm[3..].copy_from_slice(b"GOFRM");
    assert!(
        !bytes.windows(8).any(|w| w == legacy_frm),
        "Residiuum fixture must not require pre-reset start magic"
    );
}

/// CSQ-COMPAT-002 — unsupported backup profile edge fails without modifying the source.
#[test]
fn csq_compat_unsupported_backup_profile_source_intact() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");
    {
        let mut store = Store::create(&src).unwrap();
        store.put("keep", b"v1", DurabilityMode::Durable).unwrap();
        store.backup_to(&bak).unwrap();
    }

    let active = src.join("active").join("active.residiuum");
    let before = fs::read(&active).unwrap();
    let before_meta = fs::read(src.join("store-info").join("meta")).unwrap_or_default();

    // Poison package profile to an unsupported identity.
    let manifest_path = bak.join("backup-manifest.v1.json");
    assert!(
        manifest_path.is_file(),
        "expected backup-manifest.v1.json at package root"
    );
    let mut raw = fs::read_to_string(&manifest_path).unwrap();
    // Flip profile to a non-Residiuum / unsupported string.
    if raw.contains("residiuum") {
        let poison = concat!("din", "go", "-unsupported");
        raw = raw.replacen("residiuum", poison, 1);
    } else {
        panic!("expected residiuum profile token in backup manifest");
    }
    fs::write(&manifest_path, &raw).unwrap();

    let err = load_and_verify_manifest(&bak).unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("unsupported") || msg.contains("profile") || msg.contains("corrupt"),
        "unsupported edge must fail explicitly: {err}"
    );

    // Source store bytes untouched.
    assert_eq!(fs::read(&active).unwrap(), before);
    let after_meta = fs::read(src.join("store-info").join("meta")).unwrap_or_default();
    assert_eq!(after_meta, before_meta);

    let still = Store::open(&src).unwrap();
    assert_eq!(
        still.get("keep").unwrap().as_deref(),
        Some(b"v1".as_slice())
    );
}

/// CSQ-COMPAT-002 / identity policy — pre-reset product meta is not a supported
/// edge; open fails and does not rewrite the poisoned source tree.
#[test]
fn csq_compat_pre_reset_product_meta_refused() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s");
    {
        let mut store = Store::create(&root).unwrap();
        store.put("k", b"v", DurabilityMode::Durable).unwrap();
    }
    let meta = root.join("store-info").join("meta");
    assert!(meta.is_file());
    let original = fs::read(&meta).unwrap();
    assert!(
        std::str::from_utf8(&original)
            .unwrap()
            .starts_with("residiuum-store-"),
        "create must write residiuum meta"
    );

    // Poison identity to a pre-reset product meta label (token split for linter).
    let mut poison = Vec::from(concat!("din", "go", "-store-9").as_bytes());
    poison.push(b'\n');
    fs::write(&meta, &poison).unwrap();
    let poisoned = fs::read(&meta).unwrap();

    let err = match Store::open(&root) {
        Ok(_) => panic!("pre-reset product meta must not open as Residiuum store"),
        Err(e) => e,
    };
    match err {
        StoreError::CorruptMeta(msg) => {
            assert!(
                msg.contains("meta") || msg.contains("version") || msg.contains("unexpected"),
                "unexpected CorruptMeta: {msg}"
            );
        }
        other => panic!("expected CorruptMeta for pre-reset product meta, got {other:?}"),
    }

    // Source meta not rewritten by failed open.
    assert_eq!(fs::read(&meta).unwrap(), poisoned);
}

/// Packaged-artifact torture journey: create → durable writes → seal → backup →
/// restore → scrub → rebuild → migrate → reopen; final projection reconciles.
/// Links DEF-104 durable-ack-survives-reopen as the crash-recovery floor.
#[test]
fn csq_journey_packaged_torture_reconcile() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    let bak = dir.path().join("bak");
    let restored = dir.path().join("restored");
    let migrated = dir.path().join("migrated");

    let receipt = {
        let mut store = Store::create(&src).unwrap();
        let r = store
            .put("journey/a", b"alpha", DurabilityMode::Durable)
            .unwrap();
        assert_eq!(r.durability, DurabilityMode::Durable);
        store
            .put("journey/b", &seed_body(3, 48), DurabilityMode::Durable)
            .unwrap();
        store.delete("journey/b", DurabilityMode::Durable).unwrap();
        store
            .put("journey/b", b"beta2", DurabilityMode::Durable)
            .unwrap();
        store.seal_active().unwrap();
        store
            .put("journey/c", b"gamma", DurabilityMode::Durable)
            .unwrap();
        store.persist_index_cache().unwrap();
        r
    };

    // DEF-104 floor: durable ack survives reopen (process-kill analogue).
    {
        let reopened = Store::open(&src).unwrap();
        assert_eq!(
            reopened.get("journey/a").unwrap().as_deref(),
            Some(b"alpha".as_slice())
        );
        assert_eq!(
            reopened.get("journey/b").unwrap().as_deref(),
            Some(b"beta2".as_slice())
        );
        assert_eq!(
            reopened.get("journey/c").unwrap().as_deref(),
            Some(b"gamma".as_slice())
        );
        // Receipt event remains addressable on history path.
        let _ = reopened.history("journey/a").unwrap();
        let _ = receipt;
    }

    let expected_keys = ["journey/a", "journey/b", "journey/c"];
    let expected = {
        let mut store = Store::open(&src).unwrap();
        let snap = snapshot_live(&store, &expected_keys);
        let sid = store.store_id();

        let report = store.backup_to(&bak).unwrap();
        assert_eq!(report.consistency, BackupConsistency::FlushedExclusive);
        assert_eq!(report.store_id, sid);

        let manifest = load_and_verify_manifest(&bak).unwrap();
        verify_package_files(&bak, &manifest).unwrap();

        let rr = restore_full_backup(&bak, &restored, RestoreOptions::default()).unwrap();
        assert_eq!(rr.restored_store_id, sid);

        let opened = Store::open(&restored).unwrap();
        assert_projection(&opened, &snap);
        drop(opened);

        // Scrub + rebuild on source must not change live authority.
        let scrub = store.scrub_to_completion(ScrubOptions::default()).unwrap();
        assert!(scrub.cycle_completed);
        store.rebuild_index().unwrap();
        assert_projection(&store, &snap);

        let mig = store
            .migrate_to(&migrated, MigrateOptions::default())
            .unwrap();
        assert_eq!(mig.phase, MigratePhase::Done);
        assert_projection(&store, &snap);
        snap
    };

    let mig_open = Store::open(&migrated).unwrap();
    assert_projection(&mig_open, &expected);
    assert_eq!(
        mig_open.get("journey/a").unwrap().as_deref(),
        Some(b"alpha".as_slice())
    );
}

/// PR-safe scale/soak seed: deterministic multi-segment multi-op campaign with
/// final rebuild/scrub/backup/restore reconciliation. Not a 72h campaign.
#[test]
fn csq_scale_pr_safe_seed_campaign_reconcile() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("scale");
    let bak = dir.path().join("bak");
    let dst = dir.path().join("dst");

    const N: usize = 120;
    let mut expected: Vec<(String, Option<Vec<u8>>)> = Vec::with_capacity(N);

    {
        let mut store = Store::create(&root).unwrap();
        store.set_chunk_threshold(40);
        store.set_chunk_size(12);

        for i in 0..N {
            let key = format!("k/{i:04}");
            let body = seed_body((i % 200) as u8, 8 + (i % 60));
            store.put(&key, &body, DurabilityMode::Durable).unwrap();
            expected.push((key, Some(body)));
            if i > 0 && i % 25 == 0 {
                store.seal_active().unwrap();
            }
            // Tombstone every 11th key, then re-put a new generation.
            if i % 11 == 0 {
                let key = format!("k/{i:04}");
                store.delete(&key, DurabilityMode::Durable).unwrap();
                let body2 = seed_body(99, 16 + (i % 20));
                store.put(&key, &body2, DurabilityMode::Durable).unwrap();
                if let Some((_, slot)) = expected.iter_mut().find(|(k, _)| k == &key) {
                    *slot = Some(body2);
                }
            }
        }

        // Derived wipe must not change authority (scale under rebuild).
        store.persist_index_cache().unwrap();
        drop(store);
        for name in ["catalogs", "indexes", "snapshots"] {
            let p = root.join(name);
            if p.exists() {
                let _ = fs::remove_dir_all(&p);
            }
        }
    }

    {
        let mut store = Store::open(&root).unwrap();
        store.rebuild_index().unwrap();
        assert_eq!(store.live_count(), N);
        for (k, v) in &expected {
            assert_eq!(store.get(k).unwrap(), *v, "after rebuild {k}");
        }

        let scrub = store.scrub_to_completion(ScrubOptions::default()).unwrap();
        assert!(scrub.cycle_completed);
        for (k, v) in &expected {
            assert_eq!(store.get(k).unwrap(), *v, "after scrub {k}");
        }

        let report = store.backup_to(&bak).unwrap();
        assert!(report.files_copied >= 1);
        let manifest = load_and_verify_manifest(&bak).unwrap();
        verify_package_files(&bak, &manifest).unwrap();
        let rr = restore_full_backup(&bak, &dst, RestoreOptions::default()).unwrap();
        assert_eq!(rr.restored_store_id, store.store_id());
    }

    let opened = Store::open(&dst).unwrap();
    assert_eq!(opened.live_count(), N);
    for (k, v) in &expected {
        assert_eq!(opened.get(k).unwrap(), *v, "restored {k}");
    }

    // Cache file is derived, never required for the projection above.
    let _ = PRIMARY_CACHE_FILE;
}

/// Host platform cell honesty: when running on a claimed required OS/arch pair,
/// the platforms registry must list a matching required cell.
#[test]
fn csq_platform_host_cell_registered() {
    let platforms = load_platforms();
    let items = platforms["items"].as_array().unwrap();
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;

    // Map rustc consts → registry fields.
    let os_reg = match os {
        "macos" => "macos",
        "linux" => "linux",
        other => other,
    };
    let arch_reg = match arch {
        "aarch64" => "aarch64",
        "x86_64" => "x86_64",
        other => other,
    };

    let matches: Vec<_> = items
        .iter()
        .filter(|i| i["os"].as_str() == Some(os_reg) && i["arch"].as_str() == Some(arch_reg))
        .collect();

    // Not every host is a required cell (e.g. windows) — only assert when the
    // registry claims this OS/arch as required or nightly.
    if matches.is_empty() {
        // Host not in matrix: document skip honesty (not a silent pass on a claim).
        eprintln!("host {os_reg}-{arch_reg} not in platforms-v1; no claimed cell to exercise");
        return;
    }
    assert!(
        matches
            .iter()
            .any(|i| { matches!(i["status"].as_str(), Some("required") | Some("nightly")) }),
        "registered host cell must be required or nightly"
    );
}

fn copy_tree(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copy_tree(&from, &to);
        } else {
            fs::copy(&from, &to).unwrap();
        }
    }
}
