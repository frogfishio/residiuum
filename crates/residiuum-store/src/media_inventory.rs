//! Pre-mutation authoritative media inventory + immutable publish helpers (P0).
//!
//! Before pending recovery, index rebuild, or any filesystem mutation that can
//! overwrite segment media, Residiuum inventories every authoritative physical
//! source, maps `segment_id → paths`, and **fails closed** on collisions.
//!
//! Recovery Shadows / Hydra / Chimera may contribute to allocator high-water
//! discovery elsewhere; they are **not** authoritative segment ownership here.

use crate::error::StoreError;
use crate::layout::{list_residiuum_files, segment_id_from_filename, StorePaths};
use crate::seal_pipeline::list_pending_paths;
use residiuum_format::{
    decode_descriptor_body, scan_forward, verify_frame_at, FrameKind, SafetyLimits,
    FRAME_PREFIX_LEN, FRAME_SUFFIX_LEN,
};
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// How to treat non-empty authoritative `.residiuum` without a store-matching
/// descriptor (P0 open vs salvage/reassign).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum InventoryPolicy {
    /// Writable / pre-mutation: unidentified media → [`StoreError::CorruptMeta`].
    #[default]
    FailClosed,
    /// Inspect / salvage / identity-reassign reopen: map foreign-store
    /// descriptors by `segment_id`; skip truly unscannable files into
    /// [`MediaInventory::unidentified`]. Collisions among identifiable owners
    /// still refuse.
    TolerateUnidentified,
}

/// One authoritative physical owner of a segment id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoritativeOwner {
    /// Absolute or store-relative path.
    pub path: PathBuf,
    /// Role label for diagnostics.
    pub role: &'static str,
}

/// Inventory: `segment_id → one or more authoritative owners`.
#[derive(Debug, Default, Clone)]
pub struct MediaInventory {
    /// Map of segment id to owners (collision when `owners.len() > 1`).
    pub by_id: BTreeMap<[u8; 16], Vec<AuthoritativeOwner>>,
    /// Non-empty media with no recoverable descriptor (Tolerate policy only).
    pub unidentified: Vec<AuthoritativeOwner>,
    /// Bytes read while verifying bounded frame-0 segment descriptors.
    pub descriptor_probe_bytes: u64,
    /// Bytes read by full-media fallback scans (salvage/tolerant policy only).
    pub fallback_scan_bytes: u64,
}

impl MediaInventory {
    /// First collision if any owner list has length > 1.
    pub fn first_collision(&self) -> Option<([u8; 16], Vec<PathBuf>)> {
        for (id, owners) in &self.by_id {
            if owners.len() > 1 {
                let paths = owners.iter().map(|o| o.path.clone()).collect();
                return Some((*id, paths));
            }
        }
        None
    }

    fn record(&mut self, id: [u8; 16], path: PathBuf, role: &'static str) {
        self.by_id
            .entry(id)
            .or_default()
            .push(AuthoritativeOwner { path, role });
    }
}

fn decode_segment_id_from_bytes(
    bytes: &[u8],
    store_id: [u8; 16],
    limits: SafetyLimits,
    accept_foreign_store: bool,
) -> Result<Option<[u8; 16]>, StoreError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let report = scan_forward(bytes, limits);
    let mut foreign: Option<[u8; 16]> = None;
    for region in &report.regions {
        if let residiuum_format::ScanRegion::VerifiedFrame { frame, .. } = region {
            if frame.header.known_kind() == Some(FrameKind::SegmentDescriptor) {
                if let Some((ids, _, _)) = decode_descriptor_body(&frame.body) {
                    if ids.store_id == store_id {
                        return Ok(Some(ids.segment_id));
                    }
                    if accept_foreign_store && foreign.is_none() {
                        foreign = Some(ids.segment_id);
                    }
                }
            }
        }
    }
    Ok(foreign)
}

/// Read and verify frame 0 only. Valid segment writers always place the
/// authenticated segment descriptor first, so ordinary fail-closed inventory
/// need not read the remaining segment body.
fn decode_segment_id_from_frame_zero(
    path: &Path,
    store_id: [u8; 16],
    limits: SafetyLimits,
    accept_foreign_store: bool,
) -> Result<(Option<[u8; 16]>, u64), StoreError> {
    let mut file = fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    if file_len == 0 {
        return Ok((None, 0));
    }

    let prefix_len = (FRAME_PREFIX_LEN as u64).min(file_len) as usize;
    let mut prefix = vec![0u8; prefix_len];
    file.read_exact(&mut prefix)?;
    let mut bytes_read = prefix_len as u64;
    if prefix.len() < FRAME_PREFIX_LEN {
        return Ok((None, bytes_read));
    }

    let envelope_len = u32::from_le_bytes(prefix[12..16].try_into().expect("prefix length"));
    let body_len = u64::from_le_bytes(prefix[16..24].try_into().expect("prefix length"));
    if !limits.accepts_lengths(envelope_len, body_len) {
        return Ok((None, bytes_read));
    }
    let Some(frame_len) = (FRAME_PREFIX_LEN as u64)
        .checked_add(u64::from(envelope_len))
        .and_then(|n| n.checked_add(body_len))
        .and_then(|n| n.checked_add(FRAME_SUFFIX_LEN as u64))
    else {
        return Ok((None, bytes_read));
    };
    if frame_len > file_len || frame_len > limits.max_frame_len {
        return Ok((None, bytes_read));
    }

    let mut frame = vec![0u8; frame_len as usize];
    frame[..FRAME_PREFIX_LEN].copy_from_slice(&prefix);
    file.read_exact(&mut frame[FRAME_PREFIX_LEN..])?;
    bytes_read = frame_len;
    let Ok((header, _envelope, body, _hash, _verified_len)) = verify_frame_at(&frame, limits)
    else {
        return Ok((None, bytes_read));
    };
    if header.known_kind() != Some(FrameKind::SegmentDescriptor) {
        return Ok((None, bytes_read));
    }
    let Some((ids, _, _)) = decode_descriptor_body(body) else {
        return Ok((None, bytes_read));
    };
    if ids.store_id == store_id || accept_foreign_store {
        return Ok((Some(ids.segment_id), bytes_read));
    }
    Ok((None, bytes_read))
}

fn decode_segment_id_from_file(
    path: &Path,
    store_id: [u8; 16],
    limits: SafetyLimits,
    policy: InventoryPolicy,
) -> Result<(Option<[u8; 16]>, u64, u64), StoreError> {
    let accept_foreign = matches!(policy, InventoryPolicy::TolerateUnidentified);
    let (first, probe_bytes) =
        decode_segment_id_from_frame_zero(path, store_id, limits, accept_foreign)?;
    if first.is_some() || matches!(policy, InventoryPolicy::FailClosed) {
        return Ok((first, probe_bytes, 0));
    }

    // Explicit salvage/reassign mode retains the historical ability to find a
    // recoverable descriptor after damaged leading bytes. Normal writable open
    // never pays this full-media cost.
    let bytes = fs::read(path)?;
    let found = decode_segment_id_from_bytes(&bytes, store_id, limits, true)?;
    Ok((found, probe_bytes, bytes.len() as u64))
}

fn validate_filename_id(path: &Path, descriptor_id: [u8; 16]) -> Result<(), StoreError> {
    if let Some(name_id) = segment_id_from_filename(path) {
        if name_id != descriptor_id {
            return Err(StoreError::CorruptMeta(
                "filename segment id does not match descriptor segment id",
            ));
        }
    }
    Ok(())
}

fn inventory_residiuum_file(
    inv: &mut MediaInventory,
    path: PathBuf,
    store_id: [u8; 16],
    limits: SafetyLimits,
    role: &'static str,
    policy: InventoryPolicy,
) -> Result<(), StoreError> {
    let file_len = fs::metadata(&path)?.len();
    let (id, probe_bytes, fallback_bytes) =
        decode_segment_id_from_file(&path, store_id, limits, policy)?;
    inv.descriptor_probe_bytes = inv.descriptor_probe_bytes.saturating_add(probe_bytes);
    inv.fallback_scan_bytes = inv.fallback_scan_bytes.saturating_add(fallback_bytes);
    let Some(id) = id else {
        if file_len == 0 {
            return Ok(());
        }
        match policy {
            InventoryPolicy::FailClosed => {
                return Err(StoreError::CorruptMeta(
                    "authoritative segment media without recoverable store-matching descriptor",
                ));
            }
            InventoryPolicy::TolerateUnidentified => {
                inv.unidentified.push(AuthoritativeOwner { path, role });
                return Ok(());
            }
        }
    };
    // FailClosed: filename must match descriptor. Tolerate (salvage evidence /
    // reassign): allow hash-renamed or foreign-named media; still map by
    // descriptor segment_id for collision detection.
    if matches!(policy, InventoryPolicy::FailClosed) {
        validate_filename_id(&path, id)?;
    }
    inv.record(id, path, role);
    Ok(())
}

/// Build authoritative inventory (active / pending / sealed / tier copies).
///
/// Does **not** classify Recovery Shadow, Hydra, or Chimera as authoritative
/// segment ownership.
pub fn build_authoritative_inventory(
    paths: &StorePaths,
    store_id: [u8; 16],
    writer_shards: usize,
    limits: SafetyLimits,
) -> Result<MediaInventory, StoreError> {
    build_authoritative_inventory_with_policy(
        paths,
        store_id,
        writer_shards,
        limits,
        InventoryPolicy::FailClosed,
    )
}

/// Like [`build_authoritative_inventory`] with an explicit [`InventoryPolicy`].
pub fn build_authoritative_inventory_with_policy(
    paths: &StorePaths,
    store_id: [u8; 16],
    writer_shards: usize,
    limits: SafetyLimits,
    policy: InventoryPolicy,
) -> Result<MediaInventory, StoreError> {
    build_authoritative_inventory_with_placement_policy(
        paths,
        store_id,
        writer_shards,
        limits,
        policy,
        None,
    )
}

fn build_authoritative_inventory_with_placement_policy(
    paths: &StorePaths,
    store_id: [u8; 16],
    writer_shards: usize,
    limits: SafetyLimits,
    policy: InventoryPolicy,
    placement: Option<&crate::tier::TierPlacement>,
) -> Result<MediaInventory, StoreError> {
    let mut inv = MediaInventory::default();

    for path in list_residiuum_files(&paths.segments_dir())? {
        inventory_residiuum_file(&mut inv, path, store_id, limits, "sealed", policy)?;
    }
    for path in list_pending_paths(paths)? {
        inventory_residiuum_file(&mut inv, path, store_id, limits, "pending", policy)?;
    }

    // Tier placement copies (stable segment identity on other mount roots).
    // A placement-aware open scans configured external roots as well as local
    // tiers, but never touches a tier explicitly declared offline.
    if let Some(placement) = placement {
        let mut tier_dirs = [
            crate::tier::TierClass::Warm,
            crate::tier::TierClass::Cold,
            crate::tier::TierClass::Archive,
        ]
        .into_iter()
        .filter(|tier| placement.is_tier_available(*tier))
        .map(|tier| crate::tier::tier_media_dir(paths, placement, tier))
        .collect::<Vec<_>>();
        tier_dirs.sort();
        tier_dirs.dedup();
        for tier_dir in tier_dirs {
            if !tier_dir.is_dir() {
                continue;
            }
            for entry in walkdir_residiuum(&tier_dir)? {
                inventory_residiuum_file(&mut inv, entry, store_id, limits, "tier", policy)?;
            }
        }
    } else {
        let tier_root = paths.root.join("tiers");
        if tier_root.is_dir() {
            for entry in walkdir_residiuum(&tier_root)? {
                inventory_residiuum_file(&mut inv, entry, store_id, limits, "tier", policy)?;
            }
        }
    }

    // Compaction outputs under recovery (if any residual `.residiuum`).
    let compact_dir = paths.recovery_dir().join("compaction");
    if compact_dir.is_dir() {
        for ent in walkdir_residiuum(&compact_dir)? {
            inventory_residiuum_file(&mut inv, ent, store_id, limits, "compaction", policy)?;
        }
    }

    for path in paths.list_active_segment_paths(writer_shards.max(1)) {
        if !path.is_file() {
            continue;
        }
        inventory_residiuum_file(&mut inv, path, store_id, limits, "active", policy)?;
    }

    Ok(inv)
}

fn walkdir_residiuum(dir: &Path) -> Result<Vec<PathBuf>, StoreError> {
    let mut out = Vec::new();
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), StoreError> {
        for ent in fs::read_dir(dir)? {
            let p = ent?.path();
            if p.is_dir() {
                walk(&p, out)?;
            } else if p.extension().and_then(|e| e.to_str()) == Some("residiuum") {
                out.push(p);
            }
        }
        Ok(())
    }
    walk(dir, &mut out)?;
    Ok(out)
}

/// Fail closed if any segment id has multiple authoritative owners.
///
/// Call **before** pending recovery, index rebuild, or media mutation.
/// First heals hard-link / byte-identical publish aliases left by a crash
/// between exclusive link and source unlink (same inode or identical bytes).
pub fn refuse_authoritative_collisions(
    paths: &StorePaths,
    store_id: [u8; 16],
    writer_shards: usize,
    limits: SafetyLimits,
) -> Result<MediaInventory, StoreError> {
    inventory_authoritative_media(
        paths,
        store_id,
        writer_shards,
        limits,
        InventoryPolicy::FailClosed,
    )
}

/// Inventory + collision refuse under [`InventoryPolicy`].
///
/// `FailClosed` heals publish aliases then refuses unidentified media and
/// collisions. `TolerateUnidentified` skips heal (no mutation on salvage
/// reopen), maps foreign-store descriptors, and still refuses collisions.
pub fn inventory_authoritative_media(
    paths: &StorePaths,
    store_id: [u8; 16],
    writer_shards: usize,
    limits: SafetyLimits,
    policy: InventoryPolicy,
) -> Result<MediaInventory, StoreError> {
    inventory_authoritative_media_with_placement(
        paths,
        store_id,
        writer_shards,
        limits,
        policy,
        None,
    )
}

/// Placement-aware writable inventory used by Store open. External online
/// roots participate in collision truth; offline roots remain coverage holes.
pub(crate) fn inventory_authoritative_media_with_placement(
    paths: &StorePaths,
    store_id: [u8; 16],
    writer_shards: usize,
    limits: SafetyLimits,
    policy: InventoryPolicy,
    placement: Option<&crate::tier::TierPlacement>,
) -> Result<MediaInventory, StoreError> {
    let mut inv = build_authoritative_inventory_with_placement_policy(
        paths,
        store_id,
        writer_shards,
        limits,
        policy,
        placement,
    )?;
    if matches!(policy, InventoryPolicy::FailClosed) && heal_publish_aliases_from_inventory(&inv)? {
        // Directory entries changed. Rebuild once so collision evidence and
        // open metrics describe the post-heal authoritative set.
        inv = build_authoritative_inventory_with_placement_policy(
            paths,
            store_id,
            writer_shards,
            limits,
            policy,
            placement,
        )?;
    }
    if let Some((segment_id, collision_paths)) = inv.first_collision() {
        return Err(StoreError::SegmentIdCollision {
            segment_id,
            paths: collision_paths,
        });
    }
    Ok(inv)
}

fn role_rank(role: &str) -> u8 {
    match role {
        "sealed" => 0,
        "tier" => 1,
        "compaction" => 2,
        "pending" => 3,
        "active" => 4,
        _ => 5,
    }
}

#[cfg(unix)]
fn file_inode_key(path: &Path) -> io::Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let m = fs::metadata(path)?;
    Ok((m.dev(), m.ino()))
}

#[cfg(not(unix))]
fn file_inode_key(path: &Path) -> io::Result<(u64, u64)> {
    let _ = path;
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "inode identity unavailable",
    ))
}

/// Collapse hard-link publish aliases left by a crash between link and unlink.
///
/// Only **same-inode** dual names are healed (true hard-link identity). Distinct
/// files with identical bytes remain a typed collision — dual residency /
/// planted corruption, not a mid-publish alias.
pub fn heal_identical_publish_aliases(
    paths: &StorePaths,
    store_id: [u8; 16],
    writer_shards: usize,
    limits: SafetyLimits,
) -> Result<(), StoreError> {
    let inv = build_authoritative_inventory(paths, store_id, writer_shards, limits)?;
    let _ = heal_publish_aliases_from_inventory(&inv)?;
    Ok(())
}

/// Remove only proven same-inode duplicate names. Returns true when directory
/// entries changed and the caller must rebuild its inventory.
fn heal_publish_aliases_from_inventory(inv: &MediaInventory) -> Result<bool, StoreError> {
    let mut changed = false;
    for owners in inv.by_id.values() {
        if owners.len() <= 1 {
            continue;
        }
        let mut by_inode: BTreeMap<(u64, u64), Vec<AuthoritativeOwner>> = BTreeMap::new();
        for o in owners {
            if let Ok(key) = file_inode_key(&o.path) {
                by_inode.entry(key).or_default().push(o.clone());
            }
        }
        for (_key, mut group) in by_inode {
            if group.len() <= 1 {
                continue;
            }
            group.sort_by_key(|o| role_rank(o.role));
            for extra in group.iter().skip(1) {
                match fs::remove_file(&extra.path) {
                    Ok(()) => changed = true,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(StoreError::Io(e)),
                }
            }
        }
    }
    Ok(changed)
}

fn files_byte_identical(a: &Path, b: &Path) -> Result<bool, StoreError> {
    let ma = fs::metadata(a)?;
    let mb = fs::metadata(b)?;
    if ma.len() != mb.len() {
        return Ok(false);
    }
    let mut fa = fs::File::open(a)?;
    let mut fb = fs::File::open(b)?;
    let mut ba = [0u8; 64 * 1024];
    let mut bb = [0u8; 64 * 1024];
    loop {
        let na = fa.read(&mut ba)?;
        let nb = fb.read(&mut bb)?;
        if na != nb || ba[..na] != bb[..nb] {
            return Ok(false);
        }
        if na == 0 {
            return Ok(true);
        }
    }
}

/// Platform exclusive rename: succeed only when `dest` does not exist.
fn rename_noreplace(src: &Path, dest: &Path) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        const AT_FDCWD: i32 = -100;
        const RENAME_NOREPLACE: u32 = 1;
        extern "C" {
            fn renameat2(
                olddirfd: i32,
                oldpath: *const std::os::raw::c_char,
                newdirfd: i32,
                newpath: *const std::os::raw::c_char,
                flags: u32,
            ) -> i32;
        }
        let old = CString::new(src.as_os_str().as_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let new = CString::new(dest.as_os_str().as_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let rc = unsafe {
            renameat2(
                AT_FDCWD,
                old.as_ptr(),
                AT_FDCWD,
                new.as_ptr(),
                RENAME_NOREPLACE,
            )
        };
        if rc == 0 {
            return Ok(());
        }
        Err(io::Error::last_os_error())
    }
    #[cfg(target_os = "macos")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        const RENAME_EXCL: u32 = 0x0000_0004;
        extern "C" {
            fn renamex_np(
                from: *const std::os::raw::c_char,
                to: *const std::os::raw::c_char,
                flags: u32,
            ) -> i32;
        }
        let old = CString::new(src.as_os_str().as_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let new = CString::new(dest.as_os_str().as_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let rc = unsafe { renamex_np(old.as_ptr(), new.as_ptr(), RENAME_EXCL) };
        if rc == 0 {
            return Ok(());
        }
        Err(io::Error::last_os_error())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (src, dest);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "exclusive rename not available on this platform",
        ))
    }
}

fn collision(segment_id: [u8; 16], a: &Path, b: &Path) -> StoreError {
    StoreError::SegmentIdCollision {
        segment_id,
        paths: vec![a.to_path_buf(), b.to_path_buf()],
    }
}

fn force_cross_device_publish() -> bool {
    crate::failpoint::is_armed("media.publish.force_cross_device")
        || std::env::var_os("RESIDIUUM_FORCE_CROSS_DEVICE_PUBLISH").is_some()
}

fn force_hard_link_publish() -> bool {
    crate::failpoint::is_armed("media.publish.force_hard_link")
}

/// Cross-filesystem / forced staging publish (never partially writes `dest`).
///
/// 1. unique temp in dest dir → 2. copy+verify → 3. sync_all →
/// 4. no-replace publish temp→dest → 5. sync dest dir → 6. unlink source.
fn publish_via_staging_copy(
    src: &Path,
    dest: &Path,
    segment_id: [u8; 16],
) -> Result<(), StoreError> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = crate::atomic_file::temp_path_for(dest);
    let _ = fs::remove_file(&tmp);

    {
        let mut out = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp)
            .map_err(|e| {
                if e.kind() == io::ErrorKind::AlreadyExists {
                    StoreError::Io(e)
                } else {
                    StoreError::Io(e)
                }
            })?;
        crate::failpoint::hit("media.publish.after_create")?;

        let mut input = fs::File::open(src)?;
        if crate::failpoint::consume_short_write("media.publish.partial_copy") {
            let mut buf = [0u8; 64 * 1024];
            let n = input.read(&mut buf)?;
            let take = crate::failpoint::short_write_len(n.max(1)).min(n);
            if take > 0 {
                out.write_all(&buf[..take])?;
            }
            let _ = fs::remove_file(&tmp);
            return Err(StoreError::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                "failpoint short write: media.publish.partial_copy",
            )));
        }
        io::copy(&mut input, &mut out)?;
        out.sync_all()?;
    }
    crate::failpoint::hit("media.publish.after_file_sync")?;

    if !files_byte_identical(src, &tmp)? {
        let _ = fs::remove_file(&tmp);
        return Err(StoreError::CorruptMeta(
            "cross-device publish staging copy failed byte verify",
        ));
    }

    match rename_noreplace(&tmp, dest) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            if files_byte_identical(src, dest).unwrap_or(false) {
                let _ = fs::remove_file(&tmp);
            } else {
                let _ = fs::remove_file(&tmp);
                return Err(collision(segment_id, src, dest));
            }
        }
        Err(e)
            if e.raw_os_error() == Some(18) /* EXDEV */
                || e.kind() == io::ErrorKind::Unsupported =>
        {
            // Staging tmp is already on dest's filesystem; exclusive create of
            // dest via hard_link from tmp, then drop tmp.
            match fs::hard_link(&tmp, dest) {
                Ok(()) => {
                    let _ = fs::remove_file(&tmp);
                }
                Err(e2) if e2.kind() == io::ErrorKind::AlreadyExists => {
                    let _ = fs::remove_file(&tmp);
                    if files_byte_identical(src, dest).unwrap_or(false) {
                        // ok
                    } else {
                        return Err(collision(segment_id, src, dest));
                    }
                }
                Err(e2) => {
                    let _ = fs::remove_file(&tmp);
                    return Err(StoreError::Io(e2));
                }
            }
        }
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            return Err(StoreError::Io(e));
        }
    }

    crate::failpoint::hit("media.publish.after_dest_publish")?;
    if let Some(parent) = dest.parent() {
        let _ = crate::atomic_file::sync_dir(parent);
    }
    crate::failpoint::hit("media.publish.before_source_unlink")?;
    let _ = fs::remove_file(src);
    Ok(())
}

fn publish_via_hard_link(src: &Path, dest: &Path, segment_id: [u8; 16]) -> Result<(), StoreError> {
    match fs::hard_link(src, dest) {
        Ok(()) => {
            crate::failpoint::hit("media.publish.after_link")?;
            crate::failpoint::hit("media.publish.after_dest_publish")?;
            if let Some(parent) = dest.parent() {
                let _ = crate::atomic_file::sync_dir(parent);
            }
            crate::failpoint::hit("media.publish.before_source_unlink")?;
            let _ = fs::remove_file(src);
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            if files_byte_identical(src, dest).unwrap_or(false) {
                crate::failpoint::hit("media.publish.before_source_unlink")?;
                let _ = fs::remove_file(src);
                Ok(())
            } else {
                Err(collision(segment_id, src, dest))
            }
        }
        Err(e)
            if e.raw_os_error() == Some(18) /* EXDEV */
                || e.kind() == io::ErrorKind::Unsupported =>
        {
            publish_via_staging_copy(src, dest, segment_id)
        }
        Err(e) => Err(StoreError::Io(e)),
    }
}

/// Publish `src` to `dest` with **crash-atomic exclusive** semantics (P0).
///
/// Protocol:
/// 1. If `dest` exists and bytes match `src` → idempotent: unlink `src` only.
/// 2. If `dest` exists and bytes differ → [`StoreError::SegmentIdCollision`].
/// 3. Same filesystem: platform no-replace rename (`renameat2` /
///    `renamex_np`) when available — atomic move, no dual-name window.
/// 4. Else hard-link + unlink; crash between link and unlink leaves identical
///    aliases that [`heal_identical_publish_aliases`] collapses on open.
/// 5. Cross-device (`EXDEV`): unique temp in dest dir → copy+verify →
///    `sync_all` → no-replace publish → dir sync → unlink source. Never
///    writes a partial final pathname.
pub fn rename_exclusive(src: &Path, dest: &Path, segment_id: [u8; 16]) -> Result<(), StoreError> {
    if dest.exists() {
        if files_byte_identical(src, dest)? {
            crate::failpoint::hit("media.publish.before_source_unlink")?;
            let _ = fs::remove_file(src);
            return Ok(());
        }
        return Err(collision(segment_id, src, dest));
    }
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }

    if force_cross_device_publish() {
        return publish_via_staging_copy(src, dest, segment_id);
    }
    if force_hard_link_publish() {
        return publish_via_hard_link(src, dest, segment_id);
    }

    match rename_noreplace(src, dest) {
        Ok(()) => {
            crate::failpoint::hit("media.publish.after_dest_publish")?;
            if let Some(parent) = dest.parent() {
                let _ = crate::atomic_file::sync_dir(parent);
            }
            // Source already unlinked by rename; checkpoint still fires.
            crate::failpoint::hit("media.publish.before_source_unlink")?;
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            if files_byte_identical(src, dest).unwrap_or(false) {
                crate::failpoint::hit("media.publish.before_source_unlink")?;
                let _ = fs::remove_file(src);
                Ok(())
            } else {
                Err(collision(segment_id, src, dest))
            }
        }
        Err(e)
            if e.raw_os_error() == Some(18) /* EXDEV */
                || e.kind() == io::ErrorKind::Unsupported =>
        {
            publish_via_staging_copy(src, dest, segment_id)
        }
        Err(_e) => {
            // Older kernels / exotic FS: exclusive hard-link then unlink.
            publish_via_hard_link(src, dest, segment_id)
        }
    }
}

/// Create `dest` exclusively (no truncate-overwrite of an existing file).
///
/// Prefer [`rename_exclusive`] for byte publication — this helper opens the
/// final name and must not be used for multi-step copies (partial visibility).
#[allow(dead_code)]
pub fn create_new_exclusive(dest: &Path, segment_id: [u8; 16]) -> Result<fs::File, StoreError> {
    if dest.exists() {
        return Err(StoreError::SegmentIdCollision {
            segment_id,
            paths: vec![dest.to_path_buf()],
        });
    }
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(dest)
        .map_err(|e| {
            if e.kind() == io::ErrorKind::AlreadyExists {
                StoreError::SegmentIdCollision {
                    segment_id,
                    paths: vec![dest.to_path_buf()],
                }
            } else {
                StoreError::Io(e)
            }
        })
}

/// Read descriptor id from an active file (empty → None).
#[allow(dead_code)]
pub fn active_descriptor_id(
    path: &Path,
    store_id: [u8; 16],
    limits: SafetyLimits,
) -> Result<Option<[u8; 16]>, StoreError> {
    decode_segment_id_from_file(path, store_id, limits, InventoryPolicy::FailClosed)
        .map(|(id, _, _)| id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::failpoint::{self, Action};
    use residiuum_format::{ActiveSegment, SegmentId, START_MAGIC};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::{Mutex, OnceLock};
    use tempfile::tempdir;

    fn fp_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|p| p.into_inner())
    }

    #[test]
    fn fail_closed_descriptor_probe_does_not_read_segment_tail() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("large.residiuum");
        let store_id = [0x11; 16];
        let segment_id = [0x22; 16];
        let segment = ActiveSegment::create(
            SegmentId::new(store_id, segment_id),
            SafetyLimits::default(),
            7,
        )
        .unwrap();
        let descriptor_len = segment.as_bytes().len() as u64;
        let mut bytes = segment.as_bytes().to_vec();
        bytes.resize(bytes.len() + 4 * 1024 * 1024, 0x5a);
        fs::write(&path, &bytes).unwrap();

        let (found, probe_bytes, fallback_bytes) = decode_segment_id_from_file(
            &path,
            store_id,
            SafetyLimits::default(),
            InventoryPolicy::FailClosed,
        )
        .unwrap();
        assert_eq!(found, Some(segment_id));
        assert_eq!(probe_bytes, descriptor_len);
        assert_eq!(fallback_bytes, 0);
        assert!(probe_bytes < fs::metadata(path).unwrap().len());
    }

    #[test]
    fn fail_closed_descriptor_probe_rejects_truncated_and_oversized_prefixes() {
        let dir = tempdir().unwrap();
        let truncated = dir.path().join("truncated.residiuum");
        fs::write(&truncated, START_MAGIC).unwrap();
        let (found, probe_bytes, fallback_bytes) = decode_segment_id_from_file(
            &truncated,
            [1; 16],
            SafetyLimits::default(),
            InventoryPolicy::FailClosed,
        )
        .unwrap();
        assert_eq!(found, None);
        assert_eq!(probe_bytes, START_MAGIC.len() as u64);
        assert_eq!(fallback_bytes, 0);

        let oversized = dir.path().join("oversized.residiuum");
        let mut prefix = [0u8; FRAME_PREFIX_LEN];
        prefix[..START_MAGIC.len()].copy_from_slice(START_MAGIC);
        prefix[12..16]
            .copy_from_slice(&(SafetyLimits::default().max_envelope_len + 1).to_le_bytes());
        fs::write(&oversized, prefix).unwrap();
        let (found, probe_bytes, fallback_bytes) = decode_segment_id_from_file(
            &oversized,
            [1; 16],
            SafetyLimits::default(),
            InventoryPolicy::FailClosed,
        )
        .unwrap();
        assert_eq!(found, None);
        assert_eq!(probe_bytes, FRAME_PREFIX_LEN as u64);
        assert_eq!(fallback_bytes, 0);
    }

    #[test]
    fn tolerant_inventory_retains_full_scan_salvage_fallback() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("salvage.residiuum");
        let store_id = [0x33; 16];
        let segment_id = [0x44; 16];
        let segment = ActiveSegment::create(
            SegmentId::new(store_id, segment_id),
            SafetyLimits::default(),
            9,
        )
        .unwrap();
        let mut bytes = b"damaged-leading-bytes".to_vec();
        bytes.extend_from_slice(segment.as_bytes());
        fs::write(&path, &bytes).unwrap();

        let (found, probe_bytes, fallback_bytes) = decode_segment_id_from_file(
            &path,
            store_id,
            SafetyLimits::default(),
            InventoryPolicy::TolerateUnidentified,
        )
        .unwrap();
        assert_eq!(found, Some(segment_id));
        assert_eq!(probe_bytes, FRAME_PREFIX_LEN as u64);
        assert_eq!(fallback_bytes, bytes.len() as u64);
    }

    #[test]
    fn rename_exclusive_refuses_different_dest() {
        let _guard = fp_lock();
        failpoint::clear_all();
        failpoint::disable_hit_proof();
        let dir = tempdir().unwrap();
        let src = dir.path().join("a.residiuum");
        let dest = dir.path().join("b.residiuum");
        fs::write(&src, b"src-bytes").unwrap();
        fs::write(&dest, b"dest-bytes").unwrap();
        let id = [1u8; 16];
        let err = rename_exclusive(&src, &dest, id).unwrap_err();
        match err {
            StoreError::SegmentIdCollision { paths, .. } => {
                assert_eq!(paths.len(), 2);
                assert!(src.is_file());
                assert!(dest.is_file());
                assert_eq!(fs::read(&dest).unwrap(), b"dest-bytes");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn rename_exclusive_idempotent_same_bytes() {
        let _guard = fp_lock();
        failpoint::clear_all();
        failpoint::disable_hit_proof();
        let dir = tempdir().unwrap();
        let src = dir.path().join("a.residiuum");
        let dest = dir.path().join("b.residiuum");
        fs::write(&src, b"same").unwrap();
        fs::write(&dest, b"same").unwrap();
        rename_exclusive(&src, &dest, [2u8; 16]).unwrap();
        assert!(!src.exists());
        assert_eq!(fs::read(&dest).unwrap(), b"same");
    }

    #[test]
    fn rename_exclusive_uses_exclusive_publish_not_replace() {
        let _guard = fp_lock();
        failpoint::clear_all();
        failpoint::disable_hit_proof();
        let dir = tempdir().unwrap();
        let src = dir.path().join("a.residiuum");
        let dest = dir.path().join("b.residiuum");
        fs::write(&src, b"payload-bytes").unwrap();
        rename_exclusive(&src, &dest, [3u8; 16]).unwrap();
        assert!(!src.exists());
        assert_eq!(fs::read(&dest).unwrap(), b"payload-bytes");
        // Dest already present → collision, not replace.
        fs::write(&src, b"other").unwrap();
        let err = rename_exclusive(&src, &dest, [3u8; 16]).unwrap_err();
        assert!(matches!(err, StoreError::SegmentIdCollision { .. }));
        assert_eq!(fs::read(&dest).unwrap(), b"payload-bytes");
        assert_eq!(fs::read(&src).unwrap(), b"other");
    }

    #[test]
    fn rename_exclusive_hard_link_crash_before_unlink_retry_ok() {
        let _guard = fp_lock();
        failpoint::clear_all();
        failpoint::disable_hit_proof();
        // Simulate dual-name window: hard_link then crash before unlink.
        let dir = tempdir().unwrap();
        let src = dir.path().join("src.residiuum");
        let dest = dir.path().join("dest.residiuum");
        fs::write(&src, b"link-payload").unwrap();
        fs::hard_link(&src, &dest).unwrap();
        assert!(src.is_file() && dest.is_file());
        // Idempotent completion (same bytes).
        rename_exclusive(&src, &dest, [4u8; 16]).unwrap();
        assert!(!src.exists());
        assert_eq!(fs::read(&dest).unwrap(), b"link-payload");
    }

    #[test]
    fn rename_exclusive_crash_after_link_before_unlink() {
        let _guard = fp_lock();
        failpoint::clear_all();
        failpoint::disable_hit_proof();
        failpoint::arm("media.publish.force_hard_link", Action::Error);
        failpoint::arm_once("media.publish.after_link", Action::Panic);
        let dir = tempdir().unwrap();
        let src = dir.path().join("src.residiuum");
        let dest = dir.path().join("dest.residiuum");
        fs::write(&src, b"link-crash-payload").unwrap();
        let caught = catch_unwind(AssertUnwindSafe(|| {
            rename_exclusive(&src, &dest, [10u8; 16]).unwrap();
        }));
        assert!(caught.is_err());
        failpoint::clear_all();
        // Both names remain, identical — safe; retry completes.
        assert!(src.is_file() && dest.is_file());
        assert_eq!(fs::read(&src).unwrap(), fs::read(&dest).unwrap());
        rename_exclusive(&src, &dest, [10u8; 16]).unwrap();
        assert!(!src.exists());
        assert_eq!(fs::read(&dest).unwrap(), b"link-crash-payload");
    }

    #[test]
    fn rename_exclusive_partial_copy_leaves_no_final() {
        let _guard = fp_lock();
        failpoint::clear_all();
        failpoint::disable_hit_proof();
        failpoint::enable_hit_proof();
        failpoint::arm("media.publish.force_cross_device", Action::Error);
        failpoint::arm_once("media.publish.partial_copy", Action::ShortWrite);
        let dir = tempdir().unwrap();
        let src = dir.path().join("src.residiuum");
        let dest = dir.path().join("dest.residiuum");
        fs::write(&src, vec![b'x'; 8192]).unwrap();
        let err = rename_exclusive(&src, &dest, [6u8; 16]).unwrap_err();
        assert!(matches!(err, StoreError::Io(_) | StoreError::Failpoint(_)));
        assert!(!dest.exists(), "partial final must not be visible");
        assert!(src.is_file());
        failpoint::require_visited("media.publish.partial_copy");
        failpoint::clear_all();
    }

    #[test]
    fn rename_exclusive_crash_after_create_no_final() {
        let _guard = fp_lock();
        failpoint::clear_all();
        failpoint::disable_hit_proof();
        failpoint::arm("media.publish.force_cross_device", Action::Error);
        failpoint::arm_once("media.publish.after_create", Action::Panic);
        let dir = tempdir().unwrap();
        let src = dir.path().join("src.residiuum");
        let dest = dir.path().join("dest.residiuum");
        fs::write(&src, b"payload").unwrap();
        let caught = catch_unwind(AssertUnwindSafe(|| {
            rename_exclusive(&src, &dest, [7u8; 16]).unwrap();
        }));
        assert!(caught.is_err());
        failpoint::clear_all();
        assert!(!dest.exists());
        assert!(src.is_file());
    }

    #[test]
    fn rename_exclusive_crash_after_file_sync_no_final() {
        let _guard = fp_lock();
        failpoint::clear_all();
        failpoint::arm("media.publish.force_cross_device", Action::Error);
        failpoint::arm_once("media.publish.after_file_sync", Action::Panic);
        let dir = tempdir().unwrap();
        let src = dir.path().join("src.residiuum");
        let dest = dir.path().join("dest.residiuum");
        fs::write(&src, b"payload-sync").unwrap();
        let caught = catch_unwind(AssertUnwindSafe(|| {
            rename_exclusive(&src, &dest, [8u8; 16]).unwrap();
        }));
        assert!(caught.is_err());
        failpoint::clear_all();
        assert!(!dest.exists());
        assert!(src.is_file());
        // Retry completes.
        rename_exclusive(&src, &dest, [8u8; 16]).unwrap();
        assert!(!src.exists());
        assert_eq!(fs::read(&dest).unwrap(), b"payload-sync");
    }

    #[test]
    fn rename_exclusive_crash_after_dest_publish_completes_on_retry() {
        let _guard = fp_lock();
        failpoint::clear_all();
        failpoint::disable_hit_proof();
        failpoint::arm("media.publish.force_cross_device", Action::Error);
        failpoint::arm_once("media.publish.after_dest_publish", Action::Panic);
        let dir = tempdir().unwrap();
        let src = dir.path().join("src.residiuum");
        let dest = dir.path().join("dest.residiuum");
        fs::write(&src, b"published-bytes").unwrap();
        let caught = catch_unwind(AssertUnwindSafe(|| {
            rename_exclusive(&src, &dest, [9u8; 16]).unwrap();
        }));
        assert!(caught.is_err());
        failpoint::clear_all();
        assert!(dest.is_file());
        assert_eq!(fs::read(&dest).unwrap(), b"published-bytes");
        // Source may remain; retry is idempotent.
        rename_exclusive(&src, &dest, [9u8; 16]).unwrap();
        assert!(!src.exists());
    }
}
