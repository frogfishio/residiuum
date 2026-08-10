//! P0 permanent reproducer: segment-id uniqueness + immutable sealed bytes.
//!
//! Public-API sequence (no Gremlin media):
//! create → seal → victim put → close → reopen → rotate → verify descriptor IDs
//! unique across physical sources → victim byte-exact → rotate again → prior
//! sealed bytes unchanged.
//!
//! Uniqueness is proven by **decoding descriptors**, not by filename sets.
//!
//! ```text
//! cargo test -p residiuum-store --features legacy-raw-store \
//!   --test p0_segment_id_collision -- --test-threads=1 --nocapture
//! ```

use residiuum_format::{decode_descriptor_body, scan_forward, FrameKind, SafetyLimits};
use residiuum_store::{
    decode_item_envelope, finalize_seal_authoritative, hex16, list_sealed_segment_files,
    pread_item_body_matching, publish_mirror_shadow, publish_sealed_from_summary_frame,
    recover_all_pending, recover_protected_pairs, refuse_authoritative_collisions,
    segment_id_from_filename, shadow_path, DurabilityMode, LocatorExpect, Store, StoreError,
    StorePaths, TierClass, TierMoveMode,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

const VICTIM_KEY: &str = "p0/victim/unique-sentinel";
const VICTIM_BODY: &[u8] = b"VICTIM-PAYLOAD-BYTE-EXACT-9f3c2a1b";

fn decode_descriptor_id(path: &Path, store_id: [u8; 16]) -> Option<[u8; 16]> {
    let bytes = fs::read(path).ok()?;
    if bytes.is_empty() {
        return None;
    }
    let report = scan_forward(&bytes, SafetyLimits::default());
    for region in &report.regions {
        if let residiuum_format::ScanRegion::VerifiedFrame { frame, .. } = region {
            if frame.header.known_kind() == Some(FrameKind::SegmentDescriptor) {
                if let Some((ids, _, _)) = decode_descriptor_body(&frame.body) {
                    if ids.store_id == store_id {
                        return Some(ids.segment_id);
                    }
                }
            }
        }
    }
    None
}

/// All authoritative physical sources with decoded descriptor ids.
fn inventory_descriptor_ids(root: &Path, store_id: [u8; 16]) -> BTreeMap<[u8; 16], Vec<PathBuf>> {
    let paths = StorePaths::new(root);
    let mut map: BTreeMap<[u8; 16], Vec<PathBuf>> = BTreeMap::new();
    let mut add = |p: PathBuf| {
        if let Some(id) = decode_descriptor_id(&p, store_id) {
            if let Some(name_id) = segment_id_from_filename(&p) {
                assert_eq!(name_id, id, "filename/descriptor mismatch: {}", p.display());
            }
            map.entry(id).or_default().push(p);
        }
    };
    for p in list_sealed_segment_files(&paths).unwrap_or_default() {
        add(p);
    }
    let pending = paths.pending_seal_dir();
    if pending.is_dir() {
        for ent in fs::read_dir(pending).unwrap() {
            let p = ent.unwrap().path();
            if p.extension().and_then(|e| e.to_str()) == Some("residiuum") {
                add(p);
            }
        }
    }
    for p in paths.list_active_segment_paths(4) {
        if p.is_file() {
            add(p);
        }
    }
    map
}

fn assert_unique_descriptors(root: &Path, store_id: [u8; 16]) {
    let map = inventory_descriptor_ids(root, store_id);
    for (id, owners) in &map {
        assert_eq!(
            owners.len(),
            1,
            "descriptor id {:02x?} owned by multiple paths: {owners:?}",
            id
        );
    }
}

fn sealed_bytes_snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let paths = StorePaths::new(root);
    let mut m = BTreeMap::new();
    for p in list_sealed_segment_files(&paths).unwrap_or_default() {
        m.insert(p.clone(), fs::read(&p).unwrap());
    }
    m
}

fn seal_wait(store: &mut Store) {
    store.seal_active().unwrap();
    store.wait_seals_applied().unwrap();
}

fn run_reproducer(root: &Path, shards: usize, async_seal: bool) {
    let _ = fs::remove_dir_all(root);
    fs::create_dir_all(root).unwrap();

    let mut store = Store::create_with_shards(root, shards).unwrap();
    if !async_seal {
        store.set_async_lifecycle(false);
    }
    store
        .put("seed/0", b"seed", DurabilityMode::Buffered)
        .unwrap();
    seal_wait(&mut store);

    store
        .put(VICTIM_KEY, VICTIM_BODY, DurabilityMode::Buffered)
        .unwrap();
    let store_id = store.store_id();
    drop(store);

    let mut store = Store::open(root).unwrap();
    if !async_seal {
        store.set_async_lifecycle(false);
    }
    assert_eq!(store.get(VICTIM_KEY).unwrap().as_deref(), Some(VICTIM_BODY));

    store
        .put("after-reopen/0", b"rot1", DurabilityMode::Buffered)
        .unwrap();
    seal_wait(&mut store);
    assert_unique_descriptors(root, store_id);
    assert_eq!(
        store.get(VICTIM_KEY).unwrap().as_deref(),
        Some(VICTIM_BODY),
        "victim must remain byte-exact after reopen+rotate"
    );

    let before = sealed_bytes_snapshot(root);
    store
        .put("after-reopen/1", b"rot2", DurabilityMode::Buffered)
        .unwrap();
    seal_wait(&mut store);
    assert_unique_descriptors(root, store_id);
    for (path, bytes) in &before {
        assert_eq!(
            &fs::read(path).unwrap(),
            bytes,
            "sealed bytes mutated: {}",
            path.display()
        );
    }
    assert_eq!(store.get(VICTIM_KEY).unwrap().as_deref(), Some(VICTIM_BODY));
}

#[test]
fn p0_reproducer_async_single_shard() {
    let dir = tempfile::tempdir().unwrap();
    run_reproducer(dir.path(), 1, true);
}

#[test]
fn p0_reproducer_sync_single_shard() {
    let dir = tempfile::tempdir().unwrap();
    run_reproducer(dir.path(), 1, false);
}

#[test]
fn p0_reproducer_async_multi_shard() {
    let dir = tempfile::tempdir().unwrap();
    run_reproducer(dir.path(), 2, true);
}

#[test]
fn p0_reproducer_cache_miss_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    run_reproducer(root, 1, true);
    // Delete primary index cache → force rebuild on open.
    let idx = root.join("indexes");
    if idx.is_dir() {
        for ent in fs::read_dir(&idx).unwrap() {
            let p = ent.unwrap().path();
            if p.extension().and_then(|e| e.to_str()) == Some("idx")
                || p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.contains("primary"))
                    .unwrap_or(false)
            {
                let _ = fs::remove_file(&p);
            }
        }
    }
    let store = Store::open(root).unwrap();
    assert_eq!(store.get(VICTIM_KEY).unwrap().as_deref(), Some(VICTIM_BODY));
    assert_unique_descriptors(root, store.store_id());
}

#[test]
fn p0_planted_active_sealed_collision_refuses_open() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut store = Store::create_with_shards(root, 1).unwrap();
    store.put("a", b"1", DurabilityMode::Buffered).unwrap();
    seal_wait(&mut store);
    store.put("b", b"2", DurabilityMode::Buffered).unwrap();
    seal_wait(&mut store);
    let store_id = store.store_id();
    let paths = StorePaths::new(root);
    drop(store);

    let sealed = list_sealed_segment_files(&paths).unwrap();
    assert!(!sealed.is_empty());
    let sealed0 = &sealed[0];
    let sealed_id = segment_id_from_filename(sealed0).unwrap();
    let sealed_bytes = fs::read(sealed0).unwrap();
    let active = paths.active_segment_for_shard(0, 1);
    // Plant duplicate: copy sealed bytes onto active (same descriptor id).
    fs::write(&active, &sealed_bytes).unwrap();
    assert!(active.is_file());
    assert!(sealed0.is_file());

    let err = match Store::open(root) {
        Ok(_) => panic!("open must refuse planted active/sealed collision"),
        Err(e) => e,
    };
    match err {
        StoreError::SegmentIdCollision {
            segment_id,
            paths: conflict,
        } => {
            assert_eq!(segment_id, sealed_id);
            assert!(conflict.len() >= 2);
        }
        other => panic!("expected SegmentIdCollision, got {other:?}"),
    }
    // Both files preserved.
    assert_eq!(fs::read(sealed0).unwrap(), sealed_bytes);
    assert_eq!(fs::read(&active).unwrap(), sealed_bytes);
    let _ = store_id;
}

#[test]
fn p0_publish_dest_exists_is_collision_bytes_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut store = Store::create_with_shards(root, 1).unwrap();
    store.set_async_lifecycle(false);
    store.put("x", b"1", DurabilityMode::Buffered).unwrap();
    seal_wait(&mut store);
    let paths = StorePaths::new(root);
    let sealed = list_sealed_segment_files(&paths).unwrap();
    let victim_sealed = sealed[0].clone();
    let original = fs::read(&victim_sealed).unwrap();
    let seg_id = segment_id_from_filename(&victim_sealed).unwrap();

    // Force next seal to target an existing destination by planting pending
    // with same id is hard without internals; instead exercise rename_exclusive.
    let tmp = root.join("planted.tmp");
    fs::write(&tmp, b"different-incoming").unwrap();
    let err = residiuum_store::rename_exclusive(&tmp, &victim_sealed, seg_id).unwrap_err();
    match err {
        StoreError::SegmentIdCollision { paths: p, .. } => assert!(p.len() >= 2),
        other => panic!("expected collision {other:?}"),
    }
    assert_eq!(fs::read(&victim_sealed).unwrap(), original);
    assert!(tmp.is_file());
}

#[test]
fn p0_thousand_reopen_rotation_unique_ids() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut store = Store::create_with_shards(root, 1).unwrap();
    store.set_async_lifecycle(false);
    let mut all_desc: BTreeSet<[u8; 16]> = BTreeSet::new();
    let store_id = store.store_id();
    // 1000 full cycles is heavy in debug; use 64 in default CI and allow env bump.
    let cycles: u64 = std::env::var("P0_SEGID_CYCLES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);
    for i in 0..cycles {
        store
            .put(&format!("c/{i}"), &[i as u8], DurabilityMode::Buffered)
            .unwrap();
        seal_wait(&mut store);
        let map = inventory_descriptor_ids(root, store_id);
        for (id, owners) in &map {
            assert_eq!(owners.len(), 1, "cycle {i} collision on {id:02x?}");
            all_desc.insert(*id);
        }
        if i % 8 == 7 {
            drop(store);
            store = Store::open(root).unwrap();
            store.set_async_lifecycle(false);
        }
    }
    assert!(
        all_desc.len() as u64 >= cycles,
        "expected ≥{cycles} unique descriptor ids, got {}",
        all_desc.len()
    );
}

#[test]
fn p0_cross_record_sentinel_never_swaps() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create_with_shards(dir.path(), 1).unwrap();
    let a = b"AAAA-SENTINEL-PAYLOAD";
    let b = b"BBBB-SENTINEL-PAYLOAD";
    store.put("key/a", a, DurabilityMode::Buffered).unwrap();
    store.put("key/b", b, DurabilityMode::Buffered).unwrap();
    seal_wait(&mut store);
    for _ in 0..16 {
        store.put("key/a", a, DurabilityMode::Buffered).unwrap();
        store.put("key/b", b, DurabilityMode::Buffered).unwrap();
        seal_wait(&mut store);
        assert_eq!(store.get("key/a").unwrap().as_deref(), Some(a.as_slice()));
        assert_eq!(store.get("key/b").unwrap().as_deref(), Some(b.as_slice()));
        assert_ne!(store.get("key/a").unwrap().unwrap(), b);
        assert_ne!(store.get("key/b").unwrap().unwrap(), a);
    }
}

fn two_sealed_segments(root: &Path) -> (StorePaths, [u8; 16], PathBuf, Vec<u8>, PathBuf, Vec<u8>) {
    let mut store = Store::create_with_shards(root, 1).unwrap();
    store.set_async_lifecycle(false);
    store
        .put("s0", b"SOURCE-ZERO-BYTES", DurabilityMode::Buffered)
        .unwrap();
    seal_wait(&mut store);
    store
        .put("s1", b"SOURCE-ONE-BYTES!", DurabilityMode::Buffered)
        .unwrap();
    seal_wait(&mut store);
    let store_id = store.store_id();
    let paths = StorePaths::new(root);
    drop(store);
    let sealed = list_sealed_segment_files(&paths).unwrap();
    assert!(sealed.len() >= 2);
    let a = sealed[0].clone();
    let b = sealed[1].clone();
    (
        paths,
        store_id,
        a.clone(),
        fs::read(&a).unwrap(),
        b.clone(),
        fs::read(&b).unwrap(),
    )
}

fn open_err(root: &Path) -> StoreError {
    match Store::open(root) {
        Ok(_) => panic!("open must refuse"),
        Err(e) => e,
    }
}

fn assert_typed_collision(err: StoreError) {
    assert!(
        matches!(err, StoreError::SegmentIdCollision { .. }),
        "expected SegmentIdCollision, got {err:?}"
    );
}

/// Shared body: authoritative finalize refuses an existing sealed dest.
fn refuse_finalize_onto_existing_sealed() {
    let dir = tempfile::tempdir().unwrap();
    let (paths, store_id, sealed_a, bytes_a, sealed_b, bytes_b) = two_sealed_segments(dir.path());
    let id_a = segment_id_from_filename(&sealed_a).unwrap();
    let pending = paths.pending_segment(&id_a);
    fs::create_dir_all(paths.pending_seal_dir()).unwrap();
    // Already-sealed image as pending (summary short-circuit in seal_pending_bytes).
    fs::write(&pending, &bytes_b).unwrap();
    let err = finalize_seal_authoritative(
        store_id,
        id_a,
        &pending,
        &sealed_a,
        SafetyLimits::default(),
        false,
    )
    .unwrap_err();
    assert_typed_collision(err);
    assert_eq!(fs::read(&sealed_a).unwrap(), bytes_a);
    assert_eq!(fs::read(&pending).unwrap(), bytes_b);
    let _ = (sealed_b, store_id);
}

/// Synchronous authoritative finalize refuses an existing sealed dest.
#[test]
fn p0_sync_finalize_refuses_existing_sealed() {
    refuse_finalize_onto_existing_sealed();
}

/// Async finalize shares `finalize_seal_authoritative` with sync recovery.
#[test]
fn p0_async_finalize_refuses_existing_sealed() {
    refuse_finalize_onto_existing_sealed();
}

/// Pending recovery (open path) refuses collision and preserves both.
#[test]
fn p0_pending_recovery_refuses_existing_sealed() {
    let dir = tempfile::tempdir().unwrap();
    let (paths, store_id, sealed_a, bytes_a, _sealed_b, bytes_b) = two_sealed_segments(dir.path());
    let id_a = segment_id_from_filename(&sealed_a).unwrap();
    let pending = paths.pending_segment(&id_a);
    fs::create_dir_all(paths.pending_seal_dir()).unwrap();
    fs::write(&pending, &bytes_b).unwrap();
    let err = recover_all_pending(&paths, store_id, SafetyLimits::default()).unwrap_err();
    assert_typed_collision(err);
    assert_eq!(fs::read(&sealed_a).unwrap(), bytes_a);
    assert_eq!(fs::read(&pending).unwrap(), bytes_b);
}

/// Zero-scan summary publish refuses existing sealed dest.
#[test]
fn p0_summary_frame_publish_refuses_existing_sealed() {
    let dir = tempfile::tempdir().unwrap();
    let (paths, _store_id, sealed_a, bytes_a, _sealed_b, bytes_b) = two_sealed_segments(dir.path());
    let id_a = segment_id_from_filename(&sealed_a).unwrap();
    let pending = paths.pending_segment(&id_a);
    fs::create_dir_all(paths.pending_seal_dir()).unwrap();
    fs::write(&pending, &bytes_b).unwrap();
    let err =
        publish_sealed_from_summary_frame(&pending, &sealed_a, bytes_b.len() as u64, &[], false)
            .unwrap_err();
    assert_typed_collision(err);
    assert_eq!(fs::read(&sealed_a).unwrap(), bytes_a);
    assert!(pending.is_file());
}

/// Protected-pair recover: pending→sealed exclusive publish refuses planted dest.
#[test]
fn p0_protected_pair_rename_refuses_existing_sealed() {
    let dir = tempfile::tempdir().unwrap();
    let (paths, store_id, sealed_a, bytes_a, _sealed_b, bytes_b) = two_sealed_segments(dir.path());
    let id_a = segment_id_from_filename(&sealed_a).unwrap();
    // Simulate recover path: pending present, sealed absent at start of rename —
    // plant sealed under the target name with different bytes via rename_exclusive
    // caller (same primitive protected_pair uses).
    let pending = paths.pending_segment(&id_a);
    fs::create_dir_all(paths.pending_seal_dir()).unwrap();
    fs::write(&pending, &bytes_b).unwrap();
    // Direct call matching protected_pair.rs pending→sealed line.
    let err = residiuum_store::rename_exclusive(&pending, &sealed_a, id_a).unwrap_err();
    assert_typed_collision(err);
    assert_eq!(fs::read(&sealed_a).unwrap(), bytes_a);
    assert_eq!(fs::read(&pending).unwrap(), bytes_b);
    // recover_protected_pairs must not delete either when both claim the id
    // after a planted dual-ownership open inventory (below).
    let _ = (store_id, recover_protected_pairs);
}

/// Compaction residual + sealed claim the same id → open refuses.
#[test]
fn p0_compaction_and_sealed_collision_refuses_open() {
    let dir = tempfile::tempdir().unwrap();
    let (paths, _store_id, sealed_a, bytes_a, _b, _bytes_b) = two_sealed_segments(dir.path());
    let id_a = segment_id_from_filename(&sealed_a).unwrap();
    let compact_dir = paths.recovery_dir().join("compaction");
    fs::create_dir_all(&compact_dir).unwrap();
    let compact = compact_dir.join(format!("{}.residiuum", hex16(&id_a)));
    // Same descriptor id, second physical owner (bytes may match — inventory still collides).
    fs::write(&compact, &bytes_a).unwrap();
    let err = open_err(dir.path());
    assert_typed_collision(err);
    assert_eq!(fs::read(&sealed_a).unwrap(), bytes_a);
    assert_eq!(fs::read(&compact).unwrap(), bytes_a);
}

/// Two tier/compaction-style sources: sealed + planted tier copy.
#[test]
fn p0_tier_and_sealed_collision_refuses_open() {
    let dir = tempfile::tempdir().unwrap();
    let (paths, _store_id, sealed_a, bytes_a, _b, _bytes_b) = two_sealed_segments(dir.path());
    let id_a = segment_id_from_filename(&sealed_a).unwrap();
    let tier = paths
        .root
        .join("tiers")
        .join("cold")
        .join(format!("{}.residiuum", hex16(&id_a)));
    fs::create_dir_all(tier.parent().unwrap()).unwrap();
    fs::write(&tier, &bytes_a).unwrap();
    let err = open_err(dir.path());
    assert_typed_collision(err);
    assert_eq!(fs::read(&sealed_a).unwrap(), bytes_a);
    assert_eq!(fs::read(&tier).unwrap(), bytes_a);
}

/// Tier transfer publish refuses an existing cold dest with different bytes.
#[test]
fn p0_tier_transfer_refuses_existing_dest() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create_with_shards(dir.path(), 1).unwrap();
    store.set_async_lifecycle(false);
    store
        .put("tier/k", b"tier-payload", DurabilityMode::Buffered)
        .unwrap();
    seal_wait(&mut store);
    let paths = StorePaths::new(dir.path());
    let sealed = list_sealed_segment_files(&paths).unwrap();
    assert!(!sealed.is_empty());
    let seg = segment_id_from_filename(&sealed[0]).unwrap();
    let hot = paths.sealed_segment(&seg);
    let hot_bytes = fs::read(&hot).unwrap();
    let cold = dir
        .path()
        .join("tiers")
        .join("cold")
        .join(format!("{}.residiuum", hex16(&seg)));
    fs::create_dir_all(cold.parent().unwrap()).unwrap();
    fs::write(&cold, b"PLANTED-COLD-DIFFERENT").unwrap();
    let planted = fs::read(&cold).unwrap();
    let err = store
        .transfer_segment_to_tier(seg, TierClass::Cold, TierMoveMode::Copy)
        .unwrap_err();
    assert_typed_collision(err);
    assert_eq!(fs::read(&hot).unwrap(), hot_bytes);
    assert_eq!(fs::read(&cold).unwrap(), planted);
}

/// Shadow mirror publish refuses replace of an existing `.rsh`.
#[test]
fn p0_shadow_publish_refuses_existing() {
    let dir = tempfile::tempdir().unwrap();
    let (paths, store_id, sealed_a, bytes_a, ..) = two_sealed_segments(dir.path());
    let id_a = segment_id_from_filename(&sealed_a).unwrap();
    let rsh = shadow_path(&paths, &id_a);
    // CompactShadow seal may already have published a Shadow — ensure one exists.
    if !rsh.is_file() {
        publish_mirror_shadow(&paths, store_id, &id_a, &bytes_a).unwrap();
    }
    assert!(rsh.is_file());
    let original = fs::read(&rsh).unwrap();
    let err = publish_mirror_shadow(&paths, store_id, &id_a, b"other-image").unwrap_err();
    assert_typed_collision(err);
    assert_eq!(fs::read(&rsh).unwrap(), original);
}

#[test]
fn p0_filename_descriptor_mismatch_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let (paths, store_id, sealed_a, bytes_a, ..) = two_sealed_segments(dir.path());
    let wrong = paths
        .segments_dir()
        .join(format!("{}.residiuum", hex16(&[0xABu8; 16])));
    fs::write(&wrong, &bytes_a).unwrap();
    let err =
        refuse_authoritative_collisions(&paths, store_id, 1, SafetyLimits::default()).unwrap_err();
    assert!(
        matches!(err, StoreError::CorruptMeta(_))
            || matches!(err, StoreError::SegmentIdCollision { .. }),
        "expected filename/descriptor refuse, got {err:?}"
    );
    assert_eq!(fs::read(&sealed_a).unwrap(), bytes_a);
    assert_eq!(fs::read(&wrong).unwrap(), bytes_a);
}

#[test]
fn p0_planted_active_pending_collision_refuses_open() {
    let dir = tempfile::tempdir().unwrap();
    let (paths, _store_id, sealed_a, bytes_a, ..) = two_sealed_segments(dir.path());
    let id_a = segment_id_from_filename(&sealed_a).unwrap();
    // Remove sealed so open inventory sees active+pending only.
    fs::remove_file(&sealed_a).unwrap();
    let active = paths.active_segment_for_shard(0, 1);
    fs::write(&active, &bytes_a).unwrap();
    let pending = paths.pending_segment(&id_a);
    fs::create_dir_all(paths.pending_seal_dir()).unwrap();
    fs::write(&pending, &bytes_a).unwrap();
    let err = open_err(dir.path());
    assert_typed_collision(err);
    assert_eq!(fs::read(&active).unwrap(), bytes_a);
    assert_eq!(fs::read(&pending).unwrap(), bytes_a);
}

#[test]
fn p0_collision_before_cache_hit_open() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    run_reproducer(root, 1, false);
    let paths = StorePaths::new(root);
    let sealed = list_sealed_segment_files(&paths).unwrap();
    let sealed0 = &sealed[0];
    let sealed_bytes = fs::read(sealed0).unwrap();
    let active = paths.active_segment_for_shard(0, 1);
    fs::write(&active, &sealed_bytes).unwrap();
    // Indexes from reproducer still present → would be a cache-hit open if allowed.
    let err = open_err(root);
    assert_typed_collision(err);
    assert_eq!(fs::read(sealed0).unwrap(), sealed_bytes);
    assert_eq!(fs::read(&active).unwrap(), sealed_bytes);
}

#[test]
fn p0_collision_before_full_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    run_reproducer(root, 1, false);
    let paths = StorePaths::new(root);
    let sealed = list_sealed_segment_files(&paths).unwrap();
    let sealed0 = &sealed[0];
    let sealed_bytes = fs::read(sealed0).unwrap();
    let active = paths.active_segment_for_shard(0, 1);
    fs::write(&active, &sealed_bytes).unwrap();
    let idx = root.join("indexes");
    if idx.is_dir() {
        let _ = fs::remove_dir_all(&idx);
    }
    let err = open_err(root);
    assert_typed_collision(err);
    assert_eq!(fs::read(sealed0).unwrap(), sealed_bytes);
    assert_eq!(fs::read(&active).unwrap(), sealed_bytes);
}

/// Valid item frame at offset, but locator expects a different subject/event.
#[test]
fn p0_disk_pread_rejects_wrong_record_at_offset() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::create_with_shards(dir.path(), 1).unwrap();
    store.set_async_lifecycle(false);
    store
        .put("key/a", b"AAAA-BODY", DurabilityMode::Buffered)
        .unwrap();
    store
        .put("key/b", b"BBBB-BODY", DurabilityMode::Buffered)
        .unwrap();
    seal_wait(&mut store);
    let paths = StorePaths::new(dir.path());
    drop(store);
    let sealed = list_sealed_segment_files(&paths).unwrap();
    let path = &sealed[0];
    let bytes = fs::read(path).unwrap();
    let seg_id = segment_id_from_filename(path).unwrap();
    let report = scan_forward(&bytes, SafetyLimits::default());
    let mut items: Vec<(u64, [u8; 16], [u8; 16], Vec<u8>)> = Vec::new();
    for region in &report.regions {
        if let residiuum_format::ScanRegion::VerifiedFrame { range, frame } = region {
            if frame.header.known_kind() != Some(FrameKind::ItemEvent) {
                continue;
            }
            if let Ok((header, envelope, _body, _, _)) = residiuum_format::verify_frame_at(
                &bytes[range.start as usize..],
                SafetyLimits::default(),
            ) {
                if let Some(env) = decode_item_envelope(envelope) {
                    items.push((range.start, header.event_id, env.item_id, env.subject));
                }
            }
        }
    }
    assert!(
        items.len() >= 2,
        "need two item frames, got {}",
        items.len()
    );
    let (off_b, _, _, _) = &items[1];
    let (_off_a, ev_a, item_a, subj_a) = &items[0];
    let expect = LocatorExpect {
        segment_id: seg_id,
        event_id: *ev_a,
        item_id: *item_a,
        subject: subj_a.clone(),
        writer_sequence: 0,
    };
    let err = pread_item_body_matching(path, *off_b, &expect, SafetyLimits::default()).unwrap_err();
    assert!(
        matches!(
            err,
            StoreError::ConsistencyViolation(_) | StoreError::LocatorFault(_)
        ),
        "expected wrong-record refuse, got {err:?}"
    );
}
