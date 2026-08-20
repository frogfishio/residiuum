//! Bounded store-owned Atomic catalogue (CR-ATMR5-001 / CR-ATMR6-001 / CR-ATMR6-002).
//!
//! Recovery reads an authenticated checkpoint plus dirty segment tails. It
//! never `fs::read`s a file before a size check and never walks unbounded
//! directory trees. Covered prefixes are skipped for frame ingest only after
//! every stored frontier block is re-hashed. Head/tail samples are not a
//! coverage transfer. Findings, blocked identities, and coverage degradation
//! are persisted; ordinary reopen does not forget them.

use crate::atomic_file::write_atomic;
use crate::atomic_stage_classify::{
    classify_coverage_loss, classify_hole, decode_finding_class, decode_finding_kind,
    encode_finding_class, encode_finding_kind, finalize_catalog, ingest_classified_frame,
    StageEvidenceClass, StageFinding, StageFindings,
};
use crate::atomic_stage_media::{decode_stage_sidecar, BodyRef, SidecarDecode, StageCatalog};
use crate::error::StoreError;
use crate::layout::StorePaths;
use residiuum_atomics::{
    decode_member, decode_prepare, encode_member, encode_prepare, AtomicId, ChunkPlan, ContentRoot,
    HeapId,
};
use residiuum_format::{
    read_atomic_evidence, scan_forward, AtomicEvidenceClass, SafetyLimits, ScanRegion,
};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

const CHECKPOINT_MAGIC: &[u8] = b"ATCKP1";
const CHECKPOINT_VERSION: u8 = 9;
const CHECKPOINT_DOMAIN: &[u8] = b"RESIDIUUM-STORE-ATOMIC-STAGE-CKP-V9";
const BLOCK_DOMAIN: &[u8] = b"RESIDIUUM-STORE-ATOMIC-STAGE-BLK-V1";
/// Fixed leaf size for the covered-prefix block frontier (CR-ATMR6-001).
const FRONTIER_BLOCK: u64 = 64 * 1024;
const COORD_MAGIC: &[u8] = b"ATCRD1";
const COORD_VERSION: u8 = 1;
const COORD_DOMAIN: &[u8] = b"RESIDIUUM-STORE-ATOMIC-COORD-V1";
/// On-disk catalogue under `store-info/`.
pub const ATOMIC_STAGE_CHECKPOINT_FILE: &str = "atomic-stage.ckpt";
/// Durable coordinator sequence log (CR-ATMR5-004).
pub const ATOMIC_COORD_FILE: &str = "atomic-coord.ckpt";

/// Outstanding LocalHeap Atomics admitted before reclaim (CR-ATMR6-004).
pub const ADMISSION_OUTSTANDING_ATOMICS: u32 = 8;
/// Store default segment growth; covered prefixes may be this large.
const STORE_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Ceilings applied before allocation (independent of total store size).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AtomicStageLimits {
    /// Maximum dirty tail or uncovered file bytes read from one media file.
    pub max_segment_bytes: u64,
    /// Maximum aggregate dirty-tail / rebuild bytes actually ingested.
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
    /// Operable ceilings derived from LocalHeap hard limits and admission.
    ///
    /// Covered immutable segment length is not a recovery ceiling. Dirty tails
    /// are bounded to one store segment. Payload admission is outstanding
    /// Atomics times the frozen 8 MiB per-Atomic value ceiling.
    pub const fn operable() -> Self {
        let hard = residiuum_atomics::ResourceLimits::hard_local_heap();
        let atomics = ADMISSION_OUTSTANDING_ATOMICS;
        let payload = atomics as u64 * hard.total_proposed_value_bytes as u64;
        Self {
            max_segment_bytes: STORE_SEGMENT_BYTES,
            max_scan_bytes: STORE_SEGMENT_BYTES,
            max_atomics: atomics,
            max_members: atomics.saturating_mul(hard.total_generated_members),
            max_payload_bytes: payload,
            max_dirents: 8192,
            max_depth: 8,
            max_files: 4096,
            max_work_bytes: payload.saturating_add(16 * 1024 * 1024),
            max_checkpoint_bytes: 16 * 1024 * 1024,
        }
    }

    /// Historical name. Same as [`Self::operable`].
    pub const fn prototype() -> Self {
        Self::operable()
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
    /// Bytes actually read from media, including covered-prefix verification.
    pub bytes_scanned: u64,
    /// Checkpoint sidecar bytes read.
    pub checkpoint_bytes: u64,
    /// Verified frames ingested from media.
    pub frames: u32,
    /// Directory entries visited.
    pub dirents: u32,
    /// Media files whose frames were not re-ingested after block verification.
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
    /// Physical holes observed during media ingest.
    pub holes: u32,
    /// Corrupt / partial records.
    pub corrupt: u32,
    /// Conflicting records for one identity.
    pub conflicts: u32,
    /// Foreign-Heap records refused locally.
    pub foreign: u32,
    /// Covered media is missing, replaced, or otherwise degraded (CR-ATMR6-002).
    pub coverage_degraded: bool,
    /// Covered-prefix verification bytes (not charged against `max_scan_bytes`).
    pub bytes_verified: u64,
    /// Limits applied for this open (CR-ATMR6-004).
    pub limits: AtomicStageLimits,
}

#[derive(Clone, Debug)]
pub(crate) struct CoveredFile {
    pub rel_path: String,
    pub covered_len: u64,
    pub head: [u8; 32],
    pub tail: [u8; 32],
    pub block_hashes: Vec<[u8; 32]>,
    pub leftover_len: u32,
    pub leftover_hash: [u8; 32],
}

struct Budget {
    limits: AtomicStageLimits,
    report: AtomicStageOpenReport,
}

impl Budget {
    fn new(limits: AtomicStageLimits) -> Self {
        Self {
            limits,
            report: AtomicStageOpenReport {
                limits,
                ..AtomicStageOpenReport::default()
            },
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
        let payload = catalog.retained_payload_bytes();
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

/// Path of the durable coordinator sequence snapshot.
pub fn atomic_coord_path(paths: &StorePaths) -> PathBuf {
    paths.store_info().join(ATOMIC_COORD_FILE)
}

const ATOMIC_BODY_MAGICS: [&[u8]; 5] = [b"ATPAY1", b"ATSEAL1", b"ATPREP1", b"ATMAP1", b"ATCHK1"];

/// True when durable Atomic staging evidence exists that seal, compact,
/// reclaim, or identity-reassign clone must not retire (CR-ATMR6-006).
///
/// Detection is fail-closed and bounded:
/// - an authenticated checkpoint with catalogue or covered media;
/// - a present but unreadable checkpoint;
/// - a coordinator snapshot with issued sequences;
/// - Atomic frames or sidecar magics in the dirty active / pending-seal tails.
pub fn outstanding_atomic_evidence(paths: &StorePaths) -> Result<bool, StoreError> {
    if checkpoint_indicates_outstanding(paths)? {
        return Ok(true);
    }
    if coordinator_indicates_outstanding(paths)? {
        return Ok(true);
    }
    dirty_media_has_atomic_records(paths)
}

/// Refuse seal / compact / reclaim / identity-reassign while staging remains.
pub fn refuse_maintenance_while_outstanding(paths: &StorePaths) -> Result<(), StoreError> {
    if outstanding_atomic_evidence(paths)? {
        return Err(StoreError::AtomicStage(
            "seal/compact/reclaim refused while outstanding Atomic staging evidence exists".into(),
        ));
    }
    Ok(())
}

fn checkpoint_indicates_outstanding(paths: &StorePaths) -> Result<bool, StoreError> {
    let path = atomic_stage_checkpoint_path(paths);
    if !path.is_file() {
        return Ok(false);
    }
    let len = fs::metadata(&path)?.len();
    if len == 0 {
        return Ok(false);
    }
    let mut budget = Budget::new(AtomicStageLimits::operable());
    match load_checkpoint(paths, &mut budget) {
        Ok(Some((catalog, covered))) => {
            Ok(catalog.has_outstanding_evidence() || !covered.is_empty())
        }
        Ok(None) => Ok(true),
        Err(_) => Ok(true),
    }
}

fn coordinator_indicates_outstanding(paths: &StorePaths) -> Result<bool, StoreError> {
    let path = atomic_coord_path(paths);
    if !path.is_file() {
        return Ok(false);
    }
    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err.into()),
    };
    if bytes.len() < COORD_MAGIC.len() + 1 + 8 + 4 + 32 {
        return Ok(false);
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if !body.starts_with(COORD_MAGIC) || body[COORD_MAGIC.len()] != COORD_VERSION {
        return Ok(true);
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(COORD_DOMAIN);
    hasher.update(body);
    if hasher.finalize().as_bytes() != digest {
        return Ok(true);
    }
    let mut cur = &body[COORD_MAGIC.len() + 1..];
    let _next = match read_u64(&mut cur) {
        Some(v) => v,
        None => return Ok(true),
    };
    let n = match read_u32(&mut cur) {
        Some(v) => v,
        None => return Ok(true),
    };
    Ok(n > 0)
}

fn dirty_media_has_atomic_records(paths: &StorePaths) -> Result<bool, StoreError> {
    let mut budget = Budget::new(AtomicStageLimits::operable());
    let mut files = Vec::new();
    collect_files(&paths.active_dir(), 0, &mut budget, &mut files)?;
    collect_files(&paths.pending_seal_dir(), 0, &mut budget, &mut files)?;
    for path in files {
        if file_has_atomic_records(&path, budget.limits.max_segment_bytes)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn file_has_atomic_records(path: &Path, max_bytes: u64) -> Result<bool, StoreError> {
    let meta = match fs::metadata(path) {
        Ok(m) => m,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err.into()),
    };
    if meta.len() == 0 {
        return Ok(false);
    }
    let to_read = meta.len().min(max_bytes);
    let mut file = File::open(path)?;
    let mut buf = vec![0u8; to_read as usize];
    let n = file.read(&mut buf)?;
    buf.truncate(n);
    if contains_atomic_body_magic(&buf) {
        return Ok(true);
    }
    let report = read_atomic_evidence(&buf, SafetyLimits::draft_defaults());
    Ok(report
        .examined
        .iter()
        .any(|e| matches!(e.class, AtomicEvidenceClass::Valid(_))))
}

fn contains_atomic_body_magic(bytes: &[u8]) -> bool {
    ATOMIC_BODY_MAGICS
        .iter()
        .any(|magic| find_subslice(bytes, magic))
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || hay.len() < needle.len() {
        return false;
    }
    hay.windows(needle.len()).any(|w| w == needle)
}

pub(crate) struct CatalogOpen {
    pub catalog: StageCatalog,
    pub covered: Vec<CoveredFile>,
    pub report: AtomicStageOpenReport,
    pub findings: StageFindings,
}

/// Open or rebuild the store-owned catalogue from checkpoint plus tails.
pub(crate) fn open_catalog(
    paths: &StorePaths,
    limits: AtomicStageLimits,
    bound_heap: HeapId,
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
    let mut findings = std::mem::take(&mut catalog.findings);

    let files = list_media_files(paths, &mut budget)?;
    let mut unmatched: Vec<CoveredFile> = covered.clone();
    let mut next_covered = Vec::new();

    for path in files {
        budget.charge_file()?;
        let rel = rel_path(paths, &path);
        let meta = fs::metadata(&path)?;
        let size = meta.len();
        if let Some(idx) = unmatched.iter().position(|c| c.rel_path == rel) {
            let c = unmatched.swap_remove(idx);
            if size < c.covered_len {
                budget.report.files_rebuilt = budget.report.files_rebuilt.saturating_add(1);
                ingest_file_tail(
                    &path,
                    &rel,
                    0,
                    size,
                    bound_heap,
                    &mut catalog,
                    &mut findings,
                    &mut budget,
                )?;
                next_covered.push(cover_file(&path, rel, size)?);
                continue;
            }
            let n = verify_covered_blocks(&path, &c)?;
            budget.report.bytes_verified = budget.report.bytes_verified.saturating_add(n);
            if size == c.covered_len {
                next_covered.push(CoveredFile { rel_path: rel, ..c });
                budget.report.files_skipped = budget.report.files_skipped.saturating_add(1);
                continue;
            }
            budget.report.files_tailed = budget.report.files_tailed.saturating_add(1);
            ingest_file_tail(
                &path,
                &rel,
                c.covered_len,
                size,
                bound_heap,
                &mut catalog,
                &mut findings,
                &mut budget,
            )?;
            next_covered.push(extend_cover(&path, Some(&c), rel, size)?);
            continue;
        }
        budget.report.files_rebuilt = budget.report.files_rebuilt.saturating_add(1);
        ingest_file_tail(
            &path,
            &rel,
            0,
            size,
            bound_heap,
            &mut catalog,
            &mut findings,
            &mut budget,
        )?;
        next_covered.push(cover_file(&path, rel, size)?);
    }

    if !unmatched.is_empty() {
        catalog.coverage_degraded = true;
        for file in &unmatched {
            if !catalog.missing_covered.iter().any(|p| p == &file.rel_path) {
                catalog.missing_covered.push(file.rel_path.clone());
            }
        }
        classify_coverage_loss(&mut findings);
    }

    load_coordinator(paths, &mut catalog)?;
    finalize_catalog(&mut catalog, bound_heap, &mut findings);
    catalog.assign_missing_coord_seqs();
    catalog.findings = findings.clone();
    budget.charge_catalog(&catalog)?;
    budget.report.catalog_loads = 1;
    budget.report.coverage_degraded = catalog.coverage_degraded;
    budget.report.holes = findings
        .records
        .iter()
        .filter(|r| r.kind == crate::atomic_stage_classify::StageEvidenceKind::Hole)
        .count() as u32;
    budget.report.corrupt =
        findings.count(StageEvidenceClass::Corrupt) + findings.count(StageEvidenceClass::Partial);
    budget.report.conflicts = findings.count(StageEvidenceClass::Conflict);
    budget.report.foreign = findings.count(StageEvidenceClass::ForeignHeap);
    covered = next_covered;
    persist_checkpoint(paths, &catalog, &covered)?;
    persist_coordinator(paths, &catalog)?;
    Ok(CatalogOpen {
        catalog,
        covered,
        report: budget.report,
        findings,
    })
}

/// Rewrite the checkpoint after a durable staging append (no media rescan).
pub(crate) fn persist_live_checkpoint(
    paths: &StorePaths,
    catalog: &StageCatalog,
    covered: &mut Vec<CoveredFile>,
) -> Result<(), StoreError> {
    cover_active(paths, covered)?;
    persist_checkpoint(paths, catalog, covered)?;
    persist_coordinator(paths, catalog)
}

fn cover_active(paths: &StorePaths, covered: &mut Vec<CoveredFile>) -> Result<(), StoreError> {
    let active = paths.active_segment();
    if !active.is_file() {
        return Ok(());
    }
    let size = fs::metadata(&active)?.len();
    let rel = rel_path(paths, &active);
    let prev = covered.iter().find(|c| c.rel_path == rel).cloned();
    let next = extend_cover(&active, prev.as_ref(), rel.clone(), size)?;
    if let Some(slot) = covered.iter_mut().find(|c| c.rel_path == rel) {
        *slot = next;
    } else {
        covered.push(next);
    }
    Ok(())
}

fn ingest_file_tail(
    path: &Path,
    rel: &str,
    start: u64,
    size: u64,
    bound_heap: HeapId,
    catalog: &mut StageCatalog,
    findings: &mut StageFindings,
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
    for region in &report.regions {
        match region {
            ScanRegion::Hole { reason, .. } => classify_hole(findings, reason),
            ScanRegion::VerifiedFrame { frame, range } => {
                budget.report.frames = budget.report.frames.saturating_add(1);
                ingest_classified_frame(catalog, bound_heap, frame, findings);
                let lo = range.start as usize;
                let hi = (range.end as usize).min(bytes.len());
                if lo < hi {
                    record_frame_locator(
                        catalog,
                        rel,
                        start.saturating_add(range.start),
                        (hi - lo) as u64,
                        &bytes[lo..hi],
                        &frame.body,
                    );
                }
                budget.charge_catalog(catalog)?;
            }
        }
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

pub(crate) fn rel_path(paths: &StorePaths, path: &Path) -> String {
    path.strip_prefix(&paths.root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn block_hash(index: u64, bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(BLOCK_DOMAIN);
    hasher.update(&index.to_be_bytes());
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn leftover_hash(bytes: &[u8]) -> [u8; 32] {
    if bytes.is_empty() {
        return [0u8; 32];
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(BLOCK_DOMAIN);
    hasher.update(b"tail");
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn empty_coverage(rel_path: String) -> CoveredFile {
    CoveredFile {
        rel_path,
        covered_len: 0,
        head: [0u8; 32],
        tail: [0u8; 32],
        block_hashes: Vec::new(),
        leftover_len: 0,
        leftover_hash: [0u8; 32],
    }
}

fn coverage_from_bytes(rel_path: String, bytes: &[u8]) -> CoveredFile {
    let mut head = [0u8; 32];
    let mut tail = [0u8; 32];
    let head_n = bytes.len().min(32);
    if head_n > 0 {
        head[..head_n].copy_from_slice(&bytes[..head_n]);
    }
    if bytes.len() > 32 {
        tail.copy_from_slice(&bytes[bytes.len() - 32..]);
    } else {
        tail[..head_n].copy_from_slice(&bytes[..head_n]);
    }
    let mut block_hashes = Vec::new();
    let mut off = 0usize;
    while off + FRONTIER_BLOCK as usize <= bytes.len() {
        let block = &bytes[off..off + FRONTIER_BLOCK as usize];
        block_hashes.push(block_hash(block_hashes.len() as u64, block));
        off += FRONTIER_BLOCK as usize;
    }
    let leftover = &bytes[off..];
    CoveredFile {
        rel_path,
        covered_len: bytes.len() as u64,
        head,
        tail,
        block_hashes,
        leftover_len: leftover.len() as u32,
        leftover_hash: leftover_hash(leftover),
    }
}

fn cover_file(path: &Path, rel_path: String, covered_len: u64) -> Result<CoveredFile, StoreError> {
    if covered_len == 0 {
        return Ok(empty_coverage(rel_path));
    }
    let mut file = File::open(path)?;
    let mut bytes = vec![0u8; covered_len as usize];
    file.read_exact(&mut bytes)?;
    Ok(coverage_from_bytes(rel_path, &bytes))
}

fn extend_cover(
    path: &Path,
    prev: Option<&CoveredFile>,
    rel_path: String,
    new_len: u64,
) -> Result<CoveredFile, StoreError> {
    let Some(prev) = prev else {
        return cover_file(path, rel_path, new_len);
    };
    if prev.covered_len == 0 || prev.covered_len > new_len {
        return cover_file(path, rel_path, new_len);
    }
    if prev.covered_len == new_len {
        return Ok(CoveredFile {
            rel_path,
            ..prev.clone()
        });
    }
    let mut file = File::open(path)?;
    let mut marks = prev.clone();
    marks.rel_path = rel_path;
    let leftover_start = marks.block_hashes.len() as u64 * FRONTIER_BLOCK;
    let mut leftover = vec![0u8; (new_len - leftover_start) as usize];
    file.seek(SeekFrom::Start(leftover_start))?;
    file.read_exact(&mut leftover)?;
    marks
        .block_hashes
        .truncate((leftover_start / FRONTIER_BLOCK) as usize);
    let mut rest = leftover.as_slice();
    while rest.len() as u64 >= FRONTIER_BLOCK {
        let (block, next) = rest.split_at(FRONTIER_BLOCK as usize);
        marks
            .block_hashes
            .push(block_hash(marks.block_hashes.len() as u64, block));
        rest = next;
    }
    marks.leftover_len = rest.len() as u32;
    marks.leftover_hash = leftover_hash(rest);
    marks.covered_len = new_len;
    let tail_n = std::cmp::min(32, new_len as usize);
    let mut tail = [0u8; 32];
    file.seek(SeekFrom::Start(new_len - tail_n as u64))?;
    file.read_exact(&mut tail[..tail_n])?;
    marks.tail = tail;
    if new_len <= 32 {
        marks.head = tail;
    }
    Ok(marks)
}

fn record_frame_locator(
    catalog: &mut StageCatalog,
    rel: &str,
    offset: u64,
    len: u64,
    frame_bytes: &[u8],
    sidecar_body: &[u8],
) {
    let Ok(len) = u32::try_from(len) else {
        return;
    };
    let refer = BodyRef {
        rel_path: rel.to_string(),
        offset,
        len,
        hash: *blake3::hash(frame_bytes).as_bytes(),
    };
    match decode_stage_sidecar(sidecar_body) {
        SidecarDecode::Payload {
            atomic_id, ordinal, ..
        } => {
            catalog.payload_refs.insert((atomic_id, ordinal), refer);
        }
        SidecarDecode::ChunkBody {
            atomic_id,
            ordinal,
            index,
            ..
        } => {
            catalog
                .chunk_refs
                .insert((atomic_id, ordinal, index), refer);
        }
        _ => {}
    }
}

pub(crate) fn resolve_payload_body(
    paths: &StorePaths,
    catalog: &StageCatalog,
    atomic_id: AtomicId,
    ordinal: u32,
) -> Result<Option<Vec<u8>>, StoreError> {
    if let Some(payload) = catalog.payloads.get(&(atomic_id, ordinal)) {
        return Ok(Some(payload.clone()));
    }
    let Some(refer) = catalog.payload_refs.get(&(atomic_id, ordinal)) else {
        return Ok(None);
    };
    sidecar_payload_from_frame(&load_body_ref(paths, refer)?)
}

pub(crate) fn resolve_chunk_body(
    paths: &StorePaths,
    catalog: &StageCatalog,
    atomic_id: AtomicId,
    ordinal: u32,
    index: u32,
) -> Result<Option<Vec<u8>>, StoreError> {
    if let Some(body) = catalog.chunks.get(&(atomic_id, ordinal, index)) {
        return Ok(Some(body.clone()));
    }
    let Some(refer) = catalog.chunk_refs.get(&(atomic_id, ordinal, index)) else {
        return Ok(None);
    };
    sidecar_chunk_from_frame(&load_body_ref(paths, refer)?)
}

fn sidecar_payload_from_frame(frame_bytes: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
    match decode_stage_sidecar(frame_bytes) {
        SidecarDecode::Payload { payload, .. } => return Ok(Some(payload)),
        _ => {}
    }
    let report = scan_forward(frame_bytes, SafetyLimits::draft_defaults());
    for region in &report.regions {
        if let ScanRegion::VerifiedFrame { frame, .. } = region {
            if let SidecarDecode::Payload { payload, .. } = decode_stage_sidecar(&frame.body) {
                return Ok(Some(payload));
            }
        }
    }
    Ok(None)
}

fn sidecar_chunk_from_frame(frame_bytes: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
    match decode_stage_sidecar(frame_bytes) {
        SidecarDecode::ChunkBody { body, .. } => return Ok(Some(body)),
        _ => {}
    }
    let report = scan_forward(frame_bytes, SafetyLimits::draft_defaults());
    for region in &report.regions {
        if let ScanRegion::VerifiedFrame { frame, .. } = region {
            if let SidecarDecode::ChunkBody { body, .. } = decode_stage_sidecar(&frame.body) {
                return Ok(Some(body));
            }
        }
    }
    Ok(None)
}

pub(crate) fn load_body_ref(paths: &StorePaths, refer: &BodyRef) -> Result<Vec<u8>, StoreError> {
    let path = paths.root.join(&refer.rel_path);
    let mut file = File::open(&path)?;
    file.seek(SeekFrom::Start(refer.offset))?;
    let mut bytes = vec![0u8; refer.len as usize];
    file.read_exact(&mut bytes)?;
    if *blake3::hash(&bytes).as_bytes() != refer.hash {
        return Err(StoreError::AtomicStage(
            "atomic stage body locator hash mismatch".into(),
        ));
    }
    Ok(bytes)
}

fn verify_covered_blocks(path: &Path, covered: &CoveredFile) -> Result<u64, StoreError> {
    if covered.covered_len == 0 {
        return Ok(0);
    }
    if !path.is_file() {
        return Err(StoreError::AtomicStage(
            "atomic stage covered prefix missing".into(),
        ));
    }
    let file_len = fs::metadata(path)?.len();
    if file_len < covered.covered_len {
        return Err(StoreError::AtomicStage(
            "atomic stage covered prefix truncated".into(),
        ));
    }
    let expected =
        covered.block_hashes.len() as u64 * FRONTIER_BLOCK + u64::from(covered.leftover_len);
    if expected != covered.covered_len {
        return Err(StoreError::AtomicStage(
            "atomic stage covered prefix frontier coverage".into(),
        ));
    }
    let mut file = File::open(path)?;
    let mut read = 0u64;
    let mut buf = vec![0u8; FRONTIER_BLOCK as usize];
    for (i, expect) in covered.block_hashes.iter().enumerate() {
        file.seek(SeekFrom::Start(i as u64 * FRONTIER_BLOCK))?;
        file.read_exact(&mut buf)?;
        read = read.saturating_add(FRONTIER_BLOCK);
        if block_hash(i as u64, &buf) != *expect {
            return Err(StoreError::AtomicStage(
                "atomic stage covered prefix block mismatch".into(),
            ));
        }
    }
    if covered.leftover_len > 0 {
        let mut left = vec![0u8; covered.leftover_len as usize];
        let off = covered.block_hashes.len() as u64 * FRONTIER_BLOCK;
        file.seek(SeekFrom::Start(off))?;
        file.read_exact(&mut left)?;
        read = read.saturating_add(u64::from(covered.leftover_len));
        if leftover_hash(&left) != covered.leftover_hash {
            return Err(StoreError::AtomicStage(
                "atomic stage covered prefix leftover mismatch".into(),
            ));
        }
    }
    Ok(read)
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

fn persist_coordinator(paths: &StorePaths, catalog: &StageCatalog) -> Result<(), StoreError> {
    let mut body = Vec::new();
    body.extend_from_slice(COORD_MAGIC);
    body.push(COORD_VERSION);
    body.extend_from_slice(&catalog.coord_next.to_be_bytes());
    body.extend_from_slice(&(catalog.coord_seq.len() as u32).to_be_bytes());
    let mut rows: Vec<(u64, AtomicId)> = catalog
        .coord_seq
        .iter()
        .map(|(id, seq)| (*seq, *id))
        .collect();
    rows.sort_by_key(|(seq, _)| *seq);
    for (seq, id) in rows {
        body.extend_from_slice(&seq.to_be_bytes());
        body.extend_from_slice(id.as_bytes());
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(COORD_DOMAIN);
    hasher.update(&body);
    body.extend_from_slice(hasher.finalize().as_bytes());
    write_atomic(&atomic_coord_path(paths), &body)
}

fn load_coordinator(paths: &StorePaths, catalog: &mut StageCatalog) -> Result<(), StoreError> {
    let path = atomic_coord_path(paths);
    if !path.is_file() {
        return Ok(());
    }
    let len = fs::metadata(&path)?.len();
    if len > AtomicStageLimits::prototype().max_checkpoint_bytes {
        return Err(StoreError::AtomicStage(format!(
            "atomic coordinator bytes {len} exceed limit"
        )));
    }
    let bytes = fs::read(&path)?;
    if bytes.len() < COORD_MAGIC.len() + 1 + 32 {
        return Ok(());
    }
    let (body, digest) = bytes.split_at(bytes.len() - 32);
    if !body.starts_with(COORD_MAGIC) || body[COORD_MAGIC.len()] != COORD_VERSION {
        return Ok(());
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(COORD_DOMAIN);
    hasher.update(body);
    if hasher.finalize().as_bytes() != digest {
        return Err(StoreError::AtomicStage(
            "atomic coordinator checksum mismatch".into(),
        ));
    }
    let mut cur = &body[COORD_MAGIC.len() + 1..];
    let next = read_u64(&mut cur)
        .ok_or_else(|| StoreError::AtomicStage("atomic coordinator truncated".into()))?;
    let n = read_u32(&mut cur)
        .ok_or_else(|| StoreError::AtomicStage("atomic coordinator truncated".into()))?
        as usize;
    let mut seen = std::collections::BTreeSet::new();
    catalog.coord_seq.clear();
    for _ in 0..n {
        let seq = read_u64(&mut cur)
            .ok_or_else(|| StoreError::AtomicStage("atomic coordinator truncated".into()))?;
        if cur.len() < 32 {
            return Err(StoreError::AtomicStage(
                "atomic coordinator truncated".into(),
            ));
        }
        let id = AtomicId::from_bytes(cur[..32].try_into().unwrap_or([0; 32]))
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        cur = &cur[32..];
        if seq == 0 || !seen.insert(seq) {
            return Err(StoreError::AtomicStage(format!(
                "duplicate or zero coordinator sequence {seq}"
            )));
        }
        catalog.coord_seq.insert(id, seq);
        if !catalog.prepare_seen.contains(&id) {
            catalog.prepare_seen.push(id);
        }
    }
    if !cur.is_empty() {
        return Err(StoreError::AtomicStage(
            "atomic coordinator trailing bytes".into(),
        ));
    }
    catalog.coord_next = next.max(
        catalog
            .coord_seq
            .values()
            .copied()
            .max()
            .unwrap_or(0)
            .saturating_add(1),
    );
    Ok(())
}

fn persist_checkpoint(
    paths: &StorePaths,
    catalog: &StageCatalog,
    covered: &[CoveredFile],
) -> Result<(), StoreError> {
    if crate::failpoint::hit("store.atomic.checkpoint.capacity").is_err() {
        return Ok(());
    }
    let bytes = encode_checkpoint(catalog, covered)?;
    if bytes.len() as u64 > AtomicStageLimits::operable().max_checkpoint_bytes {
        // Do not strand acknowledged media behind a sidecar ceiling.
        return Ok(());
    }
    write_atomic(&atomic_stage_checkpoint_path(paths), &bytes)
}

fn encode_body_ref(body: &mut Vec<u8>, refer: &BodyRef) {
    let rel = refer.rel_path.as_bytes();
    body.extend_from_slice(&(rel.len() as u16).to_be_bytes());
    body.extend_from_slice(rel);
    body.extend_from_slice(&refer.offset.to_be_bytes());
    body.extend_from_slice(&refer.len.to_be_bytes());
    body.extend_from_slice(&refer.hash);
}

fn decode_body_ref(cur: &mut &[u8]) -> Option<BodyRef> {
    let n = read_u16(cur)? as usize;
    if cur.len() < n + 8 + 4 + 32 {
        return None;
    }
    let rel = String::from_utf8(cur[..n].to_vec()).ok()?;
    *cur = &cur[n..];
    let offset = read_u64(cur)?;
    let len = read_u32(cur)?;
    if cur.len() < 32 {
        return None;
    }
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&cur[..32]);
    *cur = &cur[32..];
    Some(BodyRef {
        rel_path: rel,
        offset,
        len,
        hash,
    })
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
        body.extend_from_slice(&(file.block_hashes.len() as u32).to_be_bytes());
        for hash in &file.block_hashes {
            body.extend_from_slice(hash);
        }
        body.extend_from_slice(&file.leftover_len.to_be_bytes());
        body.extend_from_slice(&file.leftover_hash);
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
    body.extend_from_slice(&(catalog.payload_refs.len() as u32).to_be_bytes());
    for ((id, ordinal), refer) in &catalog.payload_refs {
        body.extend_from_slice(id.as_bytes());
        body.extend_from_slice(&ordinal.to_be_bytes());
        encode_body_ref(&mut body, refer);
    }
    body.extend_from_slice(&(catalog.seals.len() as u32).to_be_bytes());
    for (id, root) in &catalog.seals {
        body.extend_from_slice(id.as_bytes());
        body.extend_from_slice(root.as_bytes());
    }
    body.extend_from_slice(&(catalog.blocked.len() as u32).to_be_bytes());
    for id in &catalog.blocked {
        body.extend_from_slice(id.as_bytes());
    }
    body.extend_from_slice(&(catalog.prepare_batch.len() as u32).to_be_bytes());
    for id in &catalog.prepare_batch {
        body.extend_from_slice(id.as_bytes());
    }
    body.extend_from_slice(&catalog.coord_next.to_be_bytes());
    body.extend_from_slice(&(catalog.coord_seq.len() as u32).to_be_bytes());
    for (id, seq) in &catalog.coord_seq {
        body.extend_from_slice(id.as_bytes());
        body.extend_from_slice(&seq.to_be_bytes());
    }
    body.extend_from_slice(&(catalog.chunk_plans.len() as u32).to_be_bytes());
    for ((id, ordinal), plan) in &catalog.chunk_plans {
        body.extend_from_slice(id.as_bytes());
        body.extend_from_slice(&ordinal.to_be_bytes());
        body.extend_from_slice(&plan.total.to_be_bytes());
        body.extend_from_slice(&(plan.chunk_hashes.len() as u32).to_be_bytes());
        for hash in &plan.chunk_hashes {
            body.extend_from_slice(hash);
        }
    }
    body.extend_from_slice(&(catalog.chunk_refs.len() as u32).to_be_bytes());
    for ((id, ordinal, index), refer) in &catalog.chunk_refs {
        body.extend_from_slice(id.as_bytes());
        body.extend_from_slice(&ordinal.to_be_bytes());
        body.extend_from_slice(&index.to_be_bytes());
        encode_body_ref(&mut body, refer);
    }
    body.push(u8::from(catalog.coverage_degraded));
    body.extend_from_slice(&(catalog.findings.records.len() as u32).to_be_bytes());
    for finding in &catalog.findings.records {
        body.push(encode_finding_kind(finding.kind));
        body.push(encode_finding_class(finding.class));
        match finding.atomic_id {
            Some(id) => {
                body.push(1);
                body.extend_from_slice(id.as_bytes());
            }
            None => body.push(0),
        }
    }
    body.extend_from_slice(&(catalog.missing_covered.len() as u32).to_be_bytes());
    for rel in &catalog.missing_covered {
        let bytes = rel.as_bytes();
        body.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        body.extend_from_slice(bytes);
    }
    body.extend_from_slice(&(catalog.intended_members.len() as u32).to_be_bytes());
    for (id, n) in &catalog.intended_members {
        body.extend_from_slice(id.as_bytes());
        body.extend_from_slice(&n.to_be_bytes());
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
        if cur.len() < n + 8 + 64 + 4 {
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
        let n_blocks = read_u32(&mut cur)? as usize;
        if cur.len() < n_blocks.saturating_mul(32) + 4 + 32 {
            return None;
        }
        let mut block_hashes = Vec::with_capacity(n_blocks);
        for _ in 0..n_blocks {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&cur[..32]);
            cur = &cur[32..];
            block_hashes.push(hash);
        }
        let leftover_len = read_u32(&mut cur)?;
        if cur.len() < 32 {
            return None;
        }
        let mut leftover_hash = [0u8; 32];
        leftover_hash.copy_from_slice(&cur[..32]);
        cur = &cur[32..];
        if n_blocks as u64 * FRONTIER_BLOCK + u64::from(leftover_len) != covered_len {
            return None;
        }
        covered.push(CoveredFile {
            rel_path: rel,
            covered_len,
            head,
            tail,
            block_hashes,
            leftover_len,
            leftover_hash,
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
        if cur.len() < 32 + 4 {
            return None;
        }
        let id = AtomicId::from_bytes(cur[..32].try_into().ok()?).ok()?;
        cur = &cur[32..];
        let ordinal = read_u32(&mut cur)?;
        let refer = decode_body_ref(&mut cur)?;
        catalog.payload_refs.insert((id, ordinal), refer);
    }
    let n_seals = read_u32(&mut cur)? as usize;
    for _ in 0..n_seals {
        if cur.len() < 64 {
            return None;
        }
        let id = AtomicId::from_bytes(cur[..32].try_into().ok()?).ok()?;
        let root = ContentRoot::from_bytes(cur[32..64].try_into().ok()?).ok()?;
        cur = &cur[64..];
        catalog.seals.insert(id, root);
    }
    let n_blocked = read_u32(&mut cur)? as usize;
    for _ in 0..n_blocked {
        if cur.len() < 32 {
            return None;
        }
        let id = AtomicId::from_bytes(cur[..32].try_into().ok()?).ok()?;
        cur = &cur[32..];
        catalog.blocked.insert(id);
    }
    let n_batch = read_u32(&mut cur)? as usize;
    for _ in 0..n_batch {
        if cur.len() < 32 {
            return None;
        }
        let id = AtomicId::from_bytes(cur[..32].try_into().ok()?).ok()?;
        cur = &cur[32..];
        catalog.prepare_batch.insert(id);
    }
    catalog.coord_next = read_u64(&mut cur)?;
    let n_coord = read_u32(&mut cur)? as usize;
    for _ in 0..n_coord {
        if cur.len() < 40 {
            return None;
        }
        let id = AtomicId::from_bytes(cur[..32].try_into().ok()?).ok()?;
        cur = &cur[32..];
        let seq = read_u64(&mut cur)?;
        if seq == 0 || catalog.coord_seq.values().any(|&s| s == seq) {
            return None;
        }
        catalog.coord_seq.insert(id, seq);
    }
    let n_plans = read_u32(&mut cur)? as usize;
    for _ in 0..n_plans {
        if cur.len() < 32 + 4 + 4 + 4 {
            return None;
        }
        let id = AtomicId::from_bytes(cur[..32].try_into().ok()?).ok()?;
        cur = &cur[32..];
        let ordinal = read_u32(&mut cur)?;
        let total = read_u32(&mut cur)?;
        let n_hashes = read_u32(&mut cur)? as usize;
        if n_hashes != total as usize || cur.len() < n_hashes.saturating_mul(32) {
            return None;
        }
        let mut chunk_hashes = Vec::with_capacity(n_hashes);
        for _ in 0..n_hashes {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&cur[..32]);
            cur = &cur[32..];
            chunk_hashes.push(hash);
        }
        catalog.chunk_plans.insert(
            (id, ordinal),
            ChunkPlan {
                total,
                chunk_hashes,
            },
        );
    }
    let n_chunks = read_u32(&mut cur)? as usize;
    for _ in 0..n_chunks {
        if cur.len() < 32 + 4 + 4 {
            return None;
        }
        let id = AtomicId::from_bytes(cur[..32].try_into().ok()?).ok()?;
        cur = &cur[32..];
        let ordinal = read_u32(&mut cur)?;
        let index = read_u32(&mut cur)?;
        let refer = decode_body_ref(&mut cur)?;
        catalog.chunk_refs.insert((id, ordinal, index), refer);
    }
    if cur.is_empty() {
        return None;
    }
    catalog.coverage_degraded = cur[0] != 0;
    cur = &cur[1..];
    let n_findings = read_u32(&mut cur)? as usize;
    for _ in 0..n_findings {
        if cur.len() < 3 {
            return None;
        }
        let kind = decode_finding_kind(cur[0])?;
        let class = decode_finding_class(cur[1])?;
        let has_id = cur[2];
        cur = &cur[3..];
        let atomic_id = match has_id {
            0 => None,
            1 => {
                if cur.len() < 32 {
                    return None;
                }
                let id = AtomicId::from_bytes(cur[..32].try_into().ok()?).ok()?;
                cur = &cur[32..];
                Some(id)
            }
            _ => return None,
        };
        let finding = StageFinding {
            kind,
            class,
            atomic_id,
        };
        if !catalog.findings.records.contains(&finding) {
            catalog.findings.records.push(finding);
        }
    }
    let n_missing = read_u32(&mut cur)? as usize;
    for _ in 0..n_missing {
        let n = read_u16(&mut cur)? as usize;
        if cur.len() < n {
            return None;
        }
        let rel = String::from_utf8(cur[..n].to_vec()).ok()?;
        cur = &cur[n..];
        catalog.missing_covered.push(rel);
    }
    let n_intended = read_u32(&mut cur)? as usize;
    for _ in 0..n_intended {
        if cur.len() < 36 {
            return None;
        }
        let id = AtomicId::from_bytes(cur[..32].try_into().ok()?).ok()?;
        cur = &cur[32..];
        let n = read_u32(&mut cur)?;
        catalog.intended_members.insert(id, n);
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

#[cfg(test)]
mod checkpoint_v9_freeze {
    use super::*;
    use crate::atomic_stage_media::{
        encode_stage_chunk_body, encode_stage_chunk_plan, encode_stage_payload, encode_stage_seal,
    };

    fn aid() -> AtomicId {
        let mut b = [0u8; 32];
        b[0] = 9;
        AtomicId::from_bytes(b).unwrap()
    }

    #[test]
    fn v9_empty_checkpoint_roundtrips() {
        let catalog = StageCatalog::default();
        let bytes = encode_checkpoint(&catalog, &[]).unwrap();
        assert!(bytes.starts_with(CHECKPOINT_MAGIC));
        assert_eq!(bytes[CHECKPOINT_MAGIC.len()], CHECKPOINT_VERSION);
        let (decoded, covered) = decode_checkpoint(&bytes).expect("v9 empty");
        assert!(covered.is_empty());
        assert!(!decoded.has_outstanding_evidence());
    }

    #[test]
    fn sidecar_magics_are_frozen() {
        let id = aid();
        let root = ContentRoot::from_bytes([0x11; 32]).unwrap();
        assert!(encode_stage_payload(id, 0, b"x").starts_with(b"ATPAY1"));
        assert!(encode_stage_seal(id, root).starts_with(b"ATSEAL1"));
        assert!(encode_stage_chunk_plan(
            id,
            0,
            &ChunkPlan {
                total: 1,
                chunk_hashes: vec![[0x22; 32]],
            }
        )
        .starts_with(b"ATMAP1"));
        assert!(encode_stage_chunk_body(id, 0, 0, b"c").starts_with(b"ATCHK1"));
    }
}
