//! Bounded store-owned Atomic catalogue (CR-ATMR5-001).
//!
//! Recovery reads an authenticated checkpoint plus dirty segment tails. It
//! never `fs::read`s a file before a size check, never walks unbounded
//! directory trees, and does not rescan settled history on each staging call.

use crate::atomic_file::write_atomic;
use crate::atomic_stage_media::{ingest_frame, StageCatalog};
use crate::error::StoreError;
use crate::layout::StorePaths;
use residiuum_atomics::{decode_member, decode_prepare, encode_member, encode_prepare, AtomicId};
use residiuum_format::{scan_forward, SafetyLimits};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

const CHECKPOINT_MAGIC: &[u8] = b"ATCKP1";
const CHECKPOINT_VERSION: u8 = 1;
const CHECKPOINT_DOMAIN: &[u8] = b"RESIDIUUM-STORE-ATOMIC-STAGE-CKP-V1";
/// On-disk catalogue under `store-info/`.
pub const ATOMIC_STAGE_CHECKPOINT_FILE: &str = "atomic-stage.ckpt";

/// Ceilings applied before allocation (independent of total store size).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AtomicStageLimits {
    /// Maximum bytes read from one segment file (tail or whole).
    pub max_segment_bytes: u64,
    /// Maximum aggregate bytes actually read.
    pub max_scan_bytes: u64,
    /// Maximum reconstructed Atomics.
    pub max_atomics: u32,
    /// Maximum reconstructed members.
    pub max_members: u32,
    /// Maximum retained payload bytes.
    pub max_payload_bytes: u64,
    /// Maximum directory entries visited.
    pub max_dirents: u32,
    /// Maximum walk depth from each media root.
    pub max_depth: u32,
    /// Maximum media files considered.
    pub max_files: u32,
    /// Maximum retained catalogue work memory.
    pub max_work_bytes: u64,
    /// Maximum checkpoint sidecar bytes.
    pub max_checkpoint_bytes: u64,
}

impl AtomicStageLimits {
    /// Prototype diagnostic ceilings. Not a function of database size.
    pub const fn prototype() -> Self {
        Self {
            max_segment_bytes: 16 * 1024 * 1024,
            max_scan_bytes: 16 * 1024 * 1024,
            max_atomics: 4096,
            max_members: 4096,
            max_payload_bytes: 16 * 1024 * 1024,
            max_dirents: 8192,
            max_depth: 8,
            max_files: 4096,
            max_work_bytes: 16 * 1024 * 1024,
            max_checkpoint_bytes: 16 * 1024 * 1024,
        }
    }
}

impl Default for AtomicStageLimits {
    fn default() -> Self {
        Self::prototype()
    }
}

/// How Atomic-stage recovery classified the checkpoint.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AtomicStageDisposition {
    /// `atomic_stage` has not run on this handle.
    #[default]
    NotRun,
    /// No usable checkpoint; bounded scan of discovered media.
    Rebuild,
    /// Authenticated checkpoint supplied the settled prefix; only tails were read.
    Checkpoint,
}

/// Measured Atomic-stage recovery costs (CR-ATMR5-001).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AtomicStageOpenReport {
    /// Checkpoint / rebuild / not-run.
    pub disposition: AtomicStageDisposition,
    /// Bytes actually read from media (not skipped covered prefixes).
    pub bytes_scanned: u64,
    /// Checkpoint sidecar bytes read.
    pub checkpoint_bytes: u64,
    /// Verified frames ingested from media.
    pub frames: u32,
    /// Directory entries visited.
    pub dirents: u32,
    /// Media files skipped because the checkpoint covered them.
    pub files_skipped: u32,
    /// Media files whose dirty tail was streamed.
    pub files_tailed: u32,
    /// Media files scanned from offset 0.
    pub files_rebuilt: u32,
    /// Atomics in the reconstructed catalogue.
    pub atomics: u32,
    /// Members in the reconstructed catalogue.
    pub members: u32,
    /// Retained payload bytes.
    pub payload_bytes: u64,
    /// Approximate retained catalogue memory.
    pub work_bytes: u64,
    /// Catalogue loads performed (must stay 1 for the life of a handle).
    pub catalog_loads: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct CoveredFile {
    pub rel_path: String,
    pub covered_len: u64,
    pub head: [u8; 32],
    pub tail: [u8; 32],
}

struct Budget {
    limits: AtomicStageLimits,
    report: AtomicStageOpenReport,
}

impl Budget {
    fn new(limits: AtomicStageLimits) -> Self {
        Self {
            limits,
            report: AtomicStageOpenReport::default(),
        }
    }

    fn fail(what: &str, observed: u64, limit: u64) -> StoreError {
        StoreError::AtomicStage(format!(
            "atomic stage recovery {what} {observed} exceeds limit {limit}"
        ))
    }

    fn charge_dirent(&mut self) -> Result<(), StoreError> {
        let next = self.report.dirents.saturating_add(1);
        if next > self.limits.max_dirents {
            return Err(Self::fail(
                "dirents",
                u64::from(next),
                u64::from(self.limits.max_dirents),
            ));
        }
        self.report.dirents = next;
        Ok(())
    }

    fn charge_file(&mut self) -> Result<(), StoreError> {
        let next = self
            .report
            .files_skipped
            .saturating_add(self.report.files_tailed)
            .saturating_add(self.report.files_rebuilt)
            .saturating_add(1);
        if next > self.limits.max_files {
            return Err(Self::fail(
                "files",
                u64::from(next),
                u64::from(self.limits.max_files),
            ));
        }
        Ok(())
    }

    fn charge_bytes(&mut self, n: u64) -> Result<(), StoreError> {
        let next = self.report.bytes_scanned.saturating_add(n);
        if next > self.limits.max_scan_bytes {
            return Err(Self::fail("scan bytes", next, self.limits.max_scan_bytes));
        }
        self.report.bytes_scanned = next;
        Ok(())
    }

    fn charge_catalog(&mut self, catalog: &StageCatalog) -> Result<(), StoreError> {
        let atomics = catalog.prepares.len() as u32;
        if atomics > self.limits.max_atomics {
            return Err(Self::fail(
                "atomics",
                u64::from(atomics),
                u64::from(self.limits.max_atomics),
            ));
        }
        let members = catalog
            .members
            .values()
            .map(|ms| ms.len() as u32)
            .sum::<u32>();
        if members > self.limits.max_members {
            return Err(Self::fail(
                "members",
                u64::from(members),
                u64::from(self.limits.max_members),
            ));
        }
        let payload: u64 = catalog.payloads.values().map(|p| p.len() as u64).sum();
        if payload > self.limits.max_payload_bytes {
            return Err(Self::fail(
                "payload bytes",
                payload,
                self.limits.max_payload_bytes,
            ));
        }
        let work = catalog.work_bytes();
        if work > self.limits.max_work_bytes {
            return Err(Self::fail("work bytes", work, self.limits.max_work_bytes));
        }
        self.report.atomics = atomics;
        self.report.members = members;
        self.report.payload_bytes = payload;
        self.report.work_bytes = work;
        Ok(())
    }
}

/// Path of the store-owned Atomic-stage checkpoint.
pub fn atomic_stage_checkpoint_path(paths: &StorePaths) -> PathBuf {
    paths.store_info().join(ATOMIC_STAGE_CHECKPOINT_FILE)
}

pub(crate) struct CatalogOpen {
    pub catalog: StageCatalog,
    pub covered: Vec<CoveredFile>,
    pub report: AtomicStageOpenReport,
}

/// Open or rebuild the store-owned catalogue from checkpoint plus tails.
pub(crate) fn open_catalog(
    paths: &StorePaths,
    limits: AtomicStageLimits,
) -> Result<CatalogOpen, StoreError> {
    let mut budget = Budget::new(limits);
    let (mut catalog, mut covered, disposition) = match load_checkpoint(paths, &mut budget)? {
        Some((catalog, covered)) => (catalog, covered, AtomicStageDisposition::Checkpoint),
        None => (
            StageCatalog::default(),
            Vec::new(),
            AtomicStageDisposition::Rebuild,
        ),
    };
    budget.report.disposition = disposition;
    budget.charge_catalog(&catalog)?;

    let files = list_media_files(paths, &mut budget)?;
    let mut unmatched: Vec<CoveredFile> = covered.clone();
    let mut next_covered = Vec::new();

    for path in files {
        budget.charge_file()?;
        let rel = rel_path(paths, &path);
        let meta = fs::metadata(&path)?;
        let size = meta.len();
        if size > limits.max_segment_bytes {
            return Err(Budget::fail(
                "segment bytes",
                size,
                limits.max_segment_bytes,
            ));
        }
        let path_hit = unmatched
            .iter()
            .position(|c| c.rel_path == rel && c.covered_len == size);
        let rename_hit = if path_hit.is_none() {
            unmatched.iter().position(|c| {
                c.covered_len == size && fingerprint_matches(&path, c).unwrap_or(false)
            })
        } else {
            None
        };
        if let Some(idx) = path_hit.or(rename_hit) {
            let c = unmatched.swap_remove(idx);
            next_covered.push(CoveredFile {
                rel_path: rel,
                covered_len: size,
                head: c.head,
                tail: c.tail,
            });
            budget.report.files_skipped = budget.report.files_skipped.saturating_add(1);
            continue;
        }
        let start = unmatched
            .iter()
            .find(|c| c.rel_path == rel && c.covered_len < size)
            .map(|c| c.covered_len)
            .unwrap_or(0);
        if start > 0 {
            unmatched.retain(|c| c.rel_path != rel);
            budget.report.files_tailed = budget.report.files_tailed.saturating_add(1);
        } else {
            budget.report.files_rebuilt = budget.report.files_rebuilt.saturating_add(1);
        }
        ingest_file_tail(&path, start, size, &mut catalog, &mut budget)?;
        let fp = fingerprint(&path, size)?;
        next_covered.push(CoveredFile {
            rel_path: rel,
            covered_len: size,
            head: fp.0,
            tail: fp.1,
        });
    }

    budget.charge_catalog(&catalog)?;
    budget.report.catalog_loads = 1;
    covered = next_covered;
    persist_checkpoint(paths, &catalog, &covered)?;
    Ok(CatalogOpen {
        catalog,
        covered,
        report: budget.report,
    })
}

/// Rewrite the checkpoint after a durable staging append (no media rescan).
pub(crate) fn persist_live_checkpoint(
    paths: &StorePaths,
    catalog: &StageCatalog,
    covered: &mut Vec<CoveredFile>,
) -> Result<(), StoreError> {
    cover_active(paths, covered)?;
    persist_checkpoint(paths, catalog, covered)
}

fn cover_active(paths: &StorePaths, covered: &mut Vec<CoveredFile>) -> Result<(), StoreError> {
    let active = paths.active_segment();
    if !active.is_file() {
        return Ok(());
    }
    let size = fs::metadata(&active)?.len();
    let rel = rel_path(paths, &active);
    let fp = fingerprint(&active, size)?;
    if let Some(slot) = covered.iter_mut().find(|c| c.rel_path == rel) {
        slot.covered_len = size;
        slot.head = fp.0;
        slot.tail = fp.1;
    } else {
        covered.push(CoveredFile {
            rel_path: rel,
            covered_len: size,
            head: fp.0,
            tail: fp.1,
        });
    }
    Ok(())
}

fn ingest_file_tail(
    path: &Path,
    start: u64,
    size: u64,
    catalog: &mut StageCatalog,
    budget: &mut Budget,
) -> Result<(), StoreError> {
    if start > size {
        return Ok(());
    }
    let want = size - start;
    if want > budget.limits.max_segment_bytes {
        return Err(Budget::fail(
            "segment bytes",
            want,
            budget.limits.max_segment_bytes,
        ));
    }
    budget.charge_bytes(want)?;
    let mut file = File::open(path)?;
    if start > 0 {
        file.seek(SeekFrom::Start(start))?;
    }
    let mut bytes = vec![0u8; want as usize];
    file.read_exact(&mut bytes)?;
    let report = scan_forward(&bytes, SafetyLimits::draft_defaults());
    for (_, frame) in report.verified_frames() {
        let next = budget.report.frames.saturating_add(1);
        budget.report.frames = next;
        ingest_frame(catalog, frame);
        budget.charge_catalog(catalog)?;
    }
    Ok(())
}

fn list_media_files(paths: &StorePaths, budget: &mut Budget) -> Result<Vec<PathBuf>, StoreError> {
    let mut out = Vec::new();
    collect_files(&paths.active_dir(), 0, budget, &mut out)?;
    collect_files(&paths.segments_dir(), 0, budget, &mut out)?;
    collect_files(&paths.pending_seal_dir(), 0, budget, &mut out)?;
    out.sort();
    Ok(out)
}

fn collect_files(
    dir: &Path,
    depth: u32,
    budget: &mut Budget,
    out: &mut Vec<PathBuf>,
) -> Result<(), StoreError> {
    if !dir.exists() {
        return Ok(());
    }
    if depth > budget.limits.max_depth {
        return Err(Budget::fail(
            "depth",
            u64::from(depth),
            u64::from(budget.limits.max_depth),
        ));
    }
    budget.charge_dirent()?;
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };
    for entry in entries {
        let entry = entry?;
        budget.charge_dirent()?;
        let name = entry.file_name();
        if name.to_string_lossy().ends_with(".tmp") {
            continue;
        }
        let path = entry.path();
        let ft = entry.file_type()?;
        if ft.is_dir() {
            collect_files(&path, depth + 1, budget, out)?;
        } else if ft.is_file() {
            out.push(path);
        }
    }
    Ok(())
}

fn rel_path(paths: &StorePaths, path: &Path) -> String {
    path.strip_prefix(&paths.root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn fingerprint(path: &Path, covered_len: u64) -> Result<([u8; 32], [u8; 32]), StoreError> {
    let mut file = File::open(path)?;
    let mut head = [0u8; 32];
    let mut tail = [0u8; 32];
    if covered_len == 0 {
        return Ok((head, tail));
    }
    let head_n = covered_len.min(32) as usize;
    file.read_exact(&mut head[..head_n])?;
    if covered_len > 32 {
        file.seek(SeekFrom::Start(covered_len - 32))?;
        file.read_exact(&mut tail)?;
    } else {
        tail[..head_n].copy_from_slice(&head[..head_n]);
    }
    Ok((head, tail))
}

fn fingerprint_matches(path: &Path, covered: &CoveredFile) -> Result<bool, StoreError> {
    let fp = fingerprint(path, covered.covered_len)?;
    Ok(fp.0 == covered.head && fp.1 == covered.tail)
}

fn load_checkpoint(
    paths: &StorePaths,
    budget: &mut Budget,
) -> Result<Option<(StageCatalog, Vec<CoveredFile>)>, StoreError> {
    let path = atomic_stage_checkpoint_path(paths);
    if !path.is_file() {
        return Ok(None);
    }
    let len = fs::metadata(&path)?.len();
    if len > budget.limits.max_checkpoint_bytes {
        return Err(Budget::fail(
            "checkpoint bytes",
            len,
            budget.limits.max_checkpoint_bytes,
        ));
    }
    budget.report.checkpoint_bytes = len;
    let bytes = fs::read(&path)?;
    Ok(decode_checkpoint(&bytes))
}

fn persist_checkpoint(
    paths: &StorePaths,
    catalog: &StageCatalog,
    covered: &[CoveredFile],
) -> Result<(), StoreError> {
    let bytes = encode_checkpoint(catalog, covered)?;
    if bytes.len() as u64 > AtomicStageLimits::prototype().max_checkpoint_bytes {
        return Err(Budget::fail(
            "checkpoint bytes",
            bytes.len() as u64,
            AtomicStageLimits::prototype().max_checkpoint_bytes,
        ));
    }
    write_atomic(&atomic_stage_checkpoint_path(paths), &bytes)
}

fn encode_checkpoint(
    catalog: &StageCatalog,
    covered: &[CoveredFile],
) -> Result<Vec<u8>, StoreError> {
    let mut body = Vec::new();
    body.extend_from_slice(CHECKPOINT_MAGIC);
    body.push(CHECKPOINT_VERSION);
    body.extend_from_slice(&(covered.len() as u32).to_be_bytes());
    for file in covered {
        let rel = file.rel_path.as_bytes();
        body.extend_from_slice(&(rel.len() as u16).to_be_bytes());
        body.extend_from_slice(rel);
        body.extend_from_slice(&file.covered_len.to_be_bytes());
        body.extend_from_slice(&file.head);
        body.extend_from_slice(&file.tail);
    }
    body.extend_from_slice(&(catalog.prepares.len() as u32).to_be_bytes());
    for prepare in catalog.prepares.values() {
        let encoded =
            encode_prepare(prepare).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        body.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        body.extend_from_slice(&encoded);
    }
    let member_count: u32 = catalog.members.values().map(|ms| ms.len() as u32).sum();
    body.extend_from_slice(&member_count.to_be_bytes());
    for members in catalog.members.values() {
        for member in members {
            let encoded =
                encode_member(member).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
            body.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            body.extend_from_slice(&encoded);
        }
    }
    body.extend_from_slice(&(catalog.payloads.len() as u32).to_be_bytes());
    for ((id, ordinal), payload) in &catalog.payloads {
        body.extend_from_slice(id.as_bytes());
        body.extend_from_slice(&ordinal.to_be_bytes());
        body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        body.extend_from_slice(payload);
    }
    body.extend_from_slice(&(catalog.seals.len() as u32).to_be_bytes());
    for id in &catalog.seals {
        body.extend_from_slice(id.as_bytes());
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(CHECKPOINT_DOMAIN);
    hasher.update(&body);
    let mut out = body;
    out.extend_from_slice(hasher.finalize().as_bytes());
    Ok(out)
}

fn decode_checkpoint(bytes: &[u8]) -> Option<(StageCatalog, Vec<CoveredFile>)> {
    if bytes.len() < CHECKPOINT_MAGIC.len() + 1 + 32 {
        return None;
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if !body.starts_with(CHECKPOINT_MAGIC) || body[CHECKPOINT_MAGIC.len()] != CHECKPOINT_VERSION {
        return None;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(CHECKPOINT_DOMAIN);
    hasher.update(body);
    if hasher.finalize().as_bytes() != digest {
        return None;
    }
    let mut cur = &body[CHECKPOINT_MAGIC.len() + 1..];
    let n_files = read_u32(&mut cur)? as usize;
    let mut covered = Vec::with_capacity(n_files);
    for _ in 0..n_files {
        let n = read_u16(&mut cur)? as usize;
        if cur.len() < n + 8 + 64 {
            return None;
        }
        let rel = String::from_utf8(cur[..n].to_vec()).ok()?;
        cur = &cur[n..];
        let covered_len = read_u64(&mut cur)?;
        let mut head = [0u8; 32];
        let mut tail = [0u8; 32];
        head.copy_from_slice(&cur[..32]);
        tail.copy_from_slice(&cur[32..64]);
        cur = &cur[64..];
        covered.push(CoveredFile {
            rel_path: rel,
            covered_len,
            head,
            tail,
        });
    }
    let mut catalog = StageCatalog::default();
    let n_prepares = read_u32(&mut cur)? as usize;
    for _ in 0..n_prepares {
        let n = read_u32(&mut cur)? as usize;
        if cur.len() < n {
            return None;
        }
        let prepare = decode_prepare(&cur[..n]).ok()?;
        cur = &cur[n..];
        catalog.prepares.insert(prepare.atomic_id, prepare);
    }
    let n_members = read_u32(&mut cur)? as usize;
    for _ in 0..n_members {
        let n = read_u32(&mut cur)? as usize;
        if cur.len() < n {
            return None;
        }
        let member = decode_member(&cur[..n]).ok()?;
        cur = &cur[n..];
        catalog
            .members
            .entry(member.atomic_id)
            .or_default()
            .push(member);
    }
    let n_payloads = read_u32(&mut cur)? as usize;
    for _ in 0..n_payloads {
        if cur.len() < 32 + 4 + 4 {
            return None;
        }
        let id = AtomicId::from_bytes(cur[..32].try_into().ok()?).ok()?;
        cur = &cur[32..];
        let ordinal = read_u32(&mut cur)?;
        let n = read_u32(&mut cur)? as usize;
        if cur.len() < n {
            return None;
        }
        catalog.payloads.insert((id, ordinal), cur[..n].to_vec());
        cur = &cur[n..];
    }
    let n_seals = read_u32(&mut cur)? as usize;
    for _ in 0..n_seals {
        if cur.len() < 32 {
            return None;
        }
        let id = AtomicId::from_bytes(cur[..32].try_into().ok()?).ok()?;
        cur = &cur[32..];
        catalog.seals.insert(id);
    }
    if !cur.is_empty() {
        return None;
    }
    Some((catalog, covered))
}

fn read_u16(cur: &mut &[u8]) -> Option<u16> {
    if cur.len() < 2 {
        return None;
    }
    let v = u16::from_be_bytes(cur[..2].try_into().ok()?);
    *cur = &cur[2..];
    Some(v)
}

fn read_u32(cur: &mut &[u8]) -> Option<u32> {
    if cur.len() < 4 {
        return None;
    }
    let v = u32::from_be_bytes(cur[..4].try_into().ok()?);
    *cur = &cur[4..];
    Some(v)
}

fn read_u64(cur: &mut &[u8]) -> Option<u64> {
    if cur.len() < 8 {
        return None;
    }
    let v = u64::from_be_bytes(cur[..8].try_into().ok()?);
    *cur = &cur[8..];
    Some(v)
}
