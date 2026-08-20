//! Bounded store-owned Atomic catalogue (CR-ATMR5-001 / CR-ATMR6-001 / CR-ATMR6-002).
//!
//! Recovery reads an authenticated checkpoint plus dirty segment tails. It
//! never `fs::read`s a file before a size check and never walks unbounded
//! directory trees. Covered prefixes are skipped for frame ingest only after
//! every stored frontier block is re-hashed. Head/tail samples are not a
//! coverage transfer. Findings, blocked identities, and coverage degradation
//! are persisted; ordinary reopen does not forget them.

use crate::atomic_file::{write_atomic_with_failpoints, AtomicWriteFailpoints};
use crate::atomic_stage_classify::{
    classify_coverage_loss, classify_hole, decode_finding_class, decode_finding_kind,
    encode_finding_class, encode_finding_kind, finalize_catalog, ingest_classified_frame,
    StageEvidenceClass, StageFinding, StageFindings,
};
use crate::atomic_stage_media::{
    decode_stage_sidecar, stage_key, AtomicShardFrontier, BodyRef, RetainedDecisionTombstone,
    SidecarDecode, StageAtomicKey, StageCatalog,
};
use crate::atomic_tombstone_index::{
    lookup as lookup_tombstone, maybe_reclaim_generation, refresh_index,
    retire_obsolete_generations, validate_index, TombstoneIndexMeta,
};
use crate::error::StoreError;
use crate::layout::StorePaths;
use residiuum_atomics::{
    decode_decision, decode_member, decode_prepare, decode_tombstone, encode_decision,
    encode_member, encode_prepare, encode_tombstone, AtomicId, ChunkPlan, ContentRoot, HeapId,
};
use residiuum_format::{
    examine_atomic_frame, read_atomic_evidence, scan_forward, AtomicEvidenceClass, SafetyLimits,
    ScanRegion,
};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

const CHECKPOINT_MAGIC: &[u8] = b"ATCKP1";
const CHECKPOINT_VERSION: u8 = 18;
const CHECKPOINT_DOMAIN: &[u8] = b"RESIDIUUM-STORE-ATOMIC-STAGE-CKP-V18";
const BLOCK_DOMAIN: &[u8] = b"RESIDIUUM-STORE-ATOMIC-STAGE-BLK-V1";
/// Fixed leaf size for the covered-prefix block frontier (CR-ATMR6-001).
const FRONTIER_BLOCK: u64 = 64 * 1024;
const COORD_MAGIC: &[u8] = b"ATCRD1";
const COORD_VERSION: u8 = 2;
const COORD_DOMAIN: &[u8] = b"RESIDIUUM-STORE-ATOMIC-COORD-V2";
const CHECKPOINT_WRITE_POINTS: AtomicWriteFailpoints = AtomicWriteFailpoints {
    after_write: "store.atomic.checkpoint.after_write",
    after_file_sync: "store.atomic.checkpoint.after_file_sync",
    after_rename: "store.atomic.checkpoint.after_rename",
    after_dir_sync: "store.atomic.checkpoint.after_dir_sync",
};
const COORD_WRITE_POINTS: AtomicWriteFailpoints = AtomicWriteFailpoints {
    after_write: "store.atomic.coord.after_write",
    after_file_sync: "store.atomic.coord.after_file_sync",
    after_rename: "store.atomic.coord.after_rename",
    after_dir_sync: "store.atomic.coord.after_dir_sync",
};
const AUTHORITY_WRITE_POINTS: AtomicWriteFailpoints = AtomicWriteFailpoints {
    after_write: "store.atomic.authority.after_write",
    after_file_sync: "store.atomic.authority.after_file_sync",
    after_rename: "store.atomic.authority.after_rename",
    after_dir_sync: "store.atomic.authority.after_dir_sync",
};
/// On-disk catalogue under `store-info/`.
pub const ATOMIC_STAGE_CHECKPOINT_FILE: &str = "atomic-stage.ckpt";
/// Durable coordinator sequence log (CR-ATMR5-004).
pub const ATOMIC_COORD_FILE: &str = "atomic-coord.ckpt";
/// Immutable replacement generations used to make destructive maintenance safe.
pub const ATOMIC_AUTHORITY_DIR: &str = "atomic-authority";

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
    /// Maximum reconstructed non-terminal/outstanding Atomics.
    pub max_atomics: u32,
    /// Maximum reconstructed members belonging to outstanding Atomics.
    pub max_members: u32,
    /// Maximum retained payload bytes belonging to outstanding Atomics.
    pub max_payload_bytes: u64,
    /// Independent ceiling for historical and outstanding Atomic identities.
    pub max_retained_atomics: u32,
    /// Independent ceiling for historical and outstanding members.
    pub max_retained_members: u32,
    /// Independent ceiling for all reconstructed inline/referenced payloads.
    pub max_retained_payload_bytes: u64,
    /// Maximum directory entries visited.
    pub max_dirents: u32,
    /// Maximum walk depth from each media root.
    pub max_depth: u32,
    /// Maximum media files considered.
    pub max_files: u32,
    /// Maximum catalogue work memory belonging to outstanding Atomics.
    pub max_work_bytes: u64,
    /// Independent ceiling for total reconstructed catalogue work memory.
    pub max_retained_work_bytes: u64,
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
            // One unsettled Atomic prefix plus one newly discovered/tail
            // segment. Both are charged; retained ordinary history is not.
            max_scan_bytes: STORE_SEGMENT_BYTES.saturating_mul(2),
            max_atomics: atomics,
            max_members: atomics.saturating_mul(hard.total_generated_members),
            max_payload_bytes: payload,
            // Terminal authority remains bounded independently of the small
            // in-flight window until it is moved into a persistent paged index.
            max_retained_atomics: 65_536,
            max_retained_members: 1_048_576,
            max_retained_payload_bytes: STORE_SEGMENT_BYTES.saturating_mul(2),
            max_dirents: 8192,
            max_depth: 8,
            max_files: 4096,
            max_work_bytes: payload.saturating_add(16 * 1024 * 1024),
            max_retained_work_bytes: STORE_SEGMENT_BYTES.saturating_mul(4),
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
    /// Lifetime decision tombstones in the reconstructed catalogue.
    pub tombstones: u32,
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
    /// Accepted prepares deterministically closed as not committed on this open.
    pub recovery_aborts: u32,
    /// Committed Atomic publications intentionally left untouched because
    /// their authority or material was degraded. The store remains open for
    /// healthy data and status examination; no missing value is invented.
    pub publication_degraded: u32,
    /// Limits applied for this open (CR-ATMR6-004).
    pub limits: AtomicStageLimits,
}

#[derive(Clone, Debug)]
pub(crate) struct CoveredFile {
    pub rel_path: String,
    /// True when this file contains Atomic authority and therefore requires
    /// authenticated full-prefix verification while unsettled.
    pub atomic_evidence: bool,
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
        if atomics > self.limits.max_retained_atomics {
            return Err(Self::fail(
                "retained atomics",
                u64::from(atomics),
                u64::from(self.limits.max_retained_atomics),
            ));
        }
        let outstanding_atomics = catalog.outstanding_atomics();
        if outstanding_atomics > self.limits.max_atomics {
            return Err(Self::fail(
                "outstanding atomics",
                u64::from(outstanding_atomics),
                u64::from(self.limits.max_atomics),
            ));
        }
        let members = catalog
            .members
            .values()
            .map(|ms| ms.len() as u32)
            .sum::<u32>();
        if members > self.limits.max_retained_members {
            return Err(Self::fail(
                "retained members",
                u64::from(members),
                u64::from(self.limits.max_retained_members),
            ));
        }
        let outstanding_members = catalog.outstanding_members();
        if outstanding_members > self.limits.max_members {
            return Err(Self::fail(
                "outstanding members",
                u64::from(outstanding_members),
                u64::from(self.limits.max_members),
            ));
        }
        let outstanding_payload = catalog.outstanding_payload_bytes();
        if outstanding_payload > self.limits.max_payload_bytes {
            return Err(Self::fail(
                "outstanding payload bytes",
                outstanding_payload,
                self.limits.max_payload_bytes,
            ));
        }
        let retained_payload = catalog.retained_payload_bytes();
        if retained_payload > self.limits.max_retained_payload_bytes {
            return Err(Self::fail(
                "retained payload bytes",
                retained_payload,
                self.limits.max_retained_payload_bytes,
            ));
        }
        let outstanding_work = catalog.outstanding_work_bytes();
        if outstanding_work > self.limits.max_work_bytes {
            return Err(Self::fail(
                "outstanding work bytes",
                outstanding_work,
                self.limits.max_work_bytes,
            ));
        }
        let retained_work = catalog.work_bytes();
        if retained_work > self.limits.max_retained_work_bytes {
            return Err(Self::fail(
                "retained work bytes",
                retained_work,
                self.limits.max_retained_work_bytes,
            ));
        }
        self.report.atomics = atomics;
        self.report.members = members;
        self.report.tombstones = catalog
            .tombstone_index
            .map_or(catalog.tombstones.len() as u64, |index| index.record_count)
            .min(u64::from(u32::MAX)) as u32;
        self.report.payload_bytes = retained_payload;
        self.report.work_bytes = retained_work;
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

const ATOMIC_BODY_MAGICS: [&[u8]; 7] = [
    b"ATPAY1", b"ATSEAL1", b"ATPREP1", b"ATMAP1", b"ATCHK1", b"ATORD1", b"ATTOMB2",
];

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
    let quick = outstanding_atomic_evidence(paths)?;
    let catalogued = if !quick
        && (atomic_stage_checkpoint_path(paths).is_file() || atomic_coord_path(paths).is_file())
    {
        open_catalog_readonly(paths, AtomicStageLimits::operable())?
            .catalog
            .has_outstanding_evidence()
    } else {
        false
    };
    if quick || catalogued {
        return Err(StoreError::AtomicStage(
            "seal/compact/reclaim refused while outstanding Atomic staging evidence exists".into(),
        ));
    }
    Ok(())
}

/// Permit byte-preserving relocation only when every issued Atomic has an
/// exact terminal decision and the authenticated catalogue is healthy.
///
/// This is deliberately narrower than a general reclaim permission: source
/// deletion after a rewritten replacement still requires an ATM-4C generation
/// proof. It is sufficient for active-to-sealed and whole-segment tier moves.
pub(crate) fn refuse_atomic_relocation_unless_terminal(
    paths: &StorePaths,
) -> Result<(), StoreError> {
    if !atomic_stage_checkpoint_path(paths).is_file()
        && !atomic_coord_path(paths).is_file()
        && !dirty_media_has_atomic_records(paths)?
    {
        return Ok(());
    }
    let opened = open_catalog_readonly(paths, AtomicStageLimits::operable())?;
    let catalog = &opened.catalog;
    let damaged = catalog.coverage_degraded
        || !catalog.blocked.is_empty()
        || opened.findings.records.iter().any(|finding| {
            matches!(
                finding.class,
                StageEvidenceClass::Partial
                    | StageEvidenceClass::Corrupt
                    | StageEvidenceClass::Conflict
            )
        });
    let undecided = catalog
        .prepares
        .keys()
        .any(|key| !catalog.decisions.contains_key(key));
    let incomplete_commit = catalog.decisions.iter().any(|(key, decision)| {
        decision.decision == residiuum_atomics::DecisionCode::Committed
            && !crate::atomic_stage_status::material_complete(catalog, *key)
    });
    if damaged || undecided || incomplete_commit {
        return Err(StoreError::AtomicStage(format!(
            "maintenance relocation refused: Atomic evidence is undecided, damaged, conflicting, or incomplete (coverage_degraded={}, blocked={}, adverse_findings={}, undecided={}, incomplete_commit={})",
            catalog.coverage_degraded,
            catalog.blocked.len(),
            opened
                .findings
                .records
                .iter()
                .filter(|finding| matches!(
                    finding.class,
                    StageEvidenceClass::Partial
                        | StageEvidenceClass::Corrupt
                        | StageEvidenceClass::Conflict
                ))
                .count(),
            undecided,
            incomplete_commit,
        )));
    }
    Ok(())
}

/// Publish a byte-exact, independently reconstructed Atomic authority
/// generation before destructive maintenance retires any source segment.
///
/// The immutable generation is durable first. The checkpoint swap is the
/// linearization point: it covers the replacement and marks every previous
/// Atomic-bearing path superseded. Therefore a crash before the swap retains
/// the old authority, while a crash after it ignores old files even if source
/// deletion had not completed.
pub(crate) fn install_compaction_authority_generation(
    paths: &StorePaths,
    job_id: &[u8; 16],
) -> Result<bool, StoreError> {
    refuse_atomic_relocation_unless_terminal(paths)?;
    let old = open_catalog_readonly(paths, AtomicStageLimits::operable())?;
    if !old.catalog.has_outstanding_evidence() {
        return Ok(false);
    }

    let mut atomic_files: Vec<_> = old
        .covered
        .iter()
        .filter(|file| {
            file.atomic_evidence
                && !old
                    .catalog
                    .superseded_media
                    .iter()
                    .any(|path| path == &file.rel_path)
        })
        .cloned()
        .collect();
    atomic_files.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    if atomic_files.is_empty() {
        return Err(StoreError::AtomicStage(
            "Atomic authority generation refused: catalogue has authority but no authenticated media"
                .into(),
        ));
    }

    crate::failpoint::hit("store.atomic.authority.before_collect")?;
    let limits = AtomicStageLimits::operable();
    let mut generation = Vec::new();
    let mut omitted_test_frame = false;
    for covered in &atomic_files {
        let source = paths.root.join(&covered.rel_path);
        if fs::metadata(&source)?.len() != covered.covered_len {
            return Err(StoreError::AtomicStage(
                "Atomic authority generation refused: covered source length changed".into(),
            ));
        }
        verify_covered_blocks(&source, covered)?;
        let bytes = fs::read(&source)?;
        let report = scan_forward(&bytes, SafetyLimits::draft_defaults());
        for region in &report.regions {
            let ScanRegion::VerifiedFrame { range, frame } = region else {
                continue;
            };
            let atomic_frame = examine_atomic_frame(frame).is_some()
                || !matches!(decode_stage_sidecar(&frame.body), SidecarDecode::NotSidecar);
            if !atomic_frame {
                continue;
            }
            if !omitted_test_frame
                && crate::failpoint::consume_short_write("store.atomic.authority.omit_frame")
            {
                omitted_test_frame = true;
                continue;
            }
            let lo = range.start as usize;
            let hi = range.end as usize;
            let frame_bytes = bytes.get(lo..hi).ok_or_else(|| {
                StoreError::AtomicStage(
                    "Atomic authority generation refused: verified frame range escaped source"
                        .into(),
                )
            })?;
            let next = generation
                .len()
                .checked_add(frame_bytes.len())
                .ok_or_else(|| StoreError::AtomicStage("Atomic authority size overflow".into()))?;
            if next as u64 > limits.max_scan_bytes {
                return Err(Budget::fail(
                    "authority generation bytes",
                    next as u64,
                    limits.max_scan_bytes,
                ));
            }
            generation.extend_from_slice(frame_bytes);
        }
    }
    if generation.is_empty() {
        return Err(StoreError::AtomicStage(
            "Atomic authority generation refused: authenticated media yielded no Atomic frames"
                .into(),
        ));
    }

    let generation_name = format!("{}.residiuum", crate::layout::hex16(job_id));
    let generation_path = paths
        .store_info()
        .join(ATOMIC_AUTHORITY_DIR)
        .join(generation_name);
    crate::failpoint::hit("store.atomic.authority.before_publish")?;
    write_atomic_with_failpoints(&generation_path, &generation, AUTHORITY_WRITE_POINTS)?;
    crate::failpoint::hit("store.atomic.authority.after_publish")?;
    if fs::read(&generation_path)? != generation {
        return Err(StoreError::AtomicStage(
            "Atomic authority generation verification failed after durable publish".into(),
        ));
    }

    let generation_rel = rel_path(paths, &generation_path);
    let mut fresh = StageCatalog::default();
    let mut findings = StageFindings::default();
    let mut budget = Budget::new(limits);
    let generation_len = generation.len() as u64;
    let atomic = ingest_file_tail(
        &generation_path,
        &generation_rel,
        0,
        generation_len,
        &mut fresh,
        &mut findings,
        &mut budget,
    )?;
    if !atomic {
        return Err(StoreError::AtomicStage(
            "Atomic authority generation did not reconstruct Atomic evidence".into(),
        ));
    }
    load_coordinator(paths, &mut fresh)?;
    finalize_catalog(&mut fresh, &mut findings);
    fresh.assign_missing_coord_seqs();
    fresh.findings = findings;
    verify_replacement_catalog(paths, &old.catalog, &fresh)?;

    let replacement_cover = cover_file(
        &generation_path,
        generation_rel.clone(),
        generation_len,
        true,
    )?;
    fresh.superseded_media = old.catalog.superseded_media.clone();
    for covered in atomic_files {
        if covered.rel_path != generation_rel && !fresh.superseded_media.contains(&covered.rel_path)
        {
            fresh.superseded_media.push(covered.rel_path);
        }
    }
    fresh.superseded_media.sort();
    crate::failpoint::hit("store.atomic.authority.before_checkpoint_swap")?;
    persist_checkpoint(
        paths,
        &mut fresh,
        &[replacement_cover],
        limits.max_checkpoint_bytes,
    )?;
    persist_coordinator(paths, &fresh)?;
    crate::failpoint::hit("store.atomic.authority.after_checkpoint_swap")?;
    Ok(true)
}

/// Retire obsolete authority-generation files and collapse superseded markers
/// after source deletion. A crash before this housekeeping is harmless: the
/// checkpoint continues to ignore every marked path. A successful pass keeps
/// checkpoint size proportional to the current crash window, not compaction
/// history.
pub(crate) fn prune_deleted_superseded_media(paths: &StorePaths) -> Result<(), StoreError> {
    if !atomic_stage_checkpoint_path(paths).is_file() {
        return Ok(());
    }
    let limits = AtomicStageLimits::operable();
    let mut opened = open_catalog(paths, limits)?;
    let authority_prefix = format!("store-info/{ATOMIC_AUTHORITY_DIR}/");
    for rel in &opened.catalog.superseded_media {
        if !rel.starts_with(&authority_prefix) || !safe_store_relative_path(rel) {
            continue;
        }
        let path = paths.root.join(rel);
        if path.is_file() {
            fs::remove_file(path)?;
        }
    }
    opened
        .catalog
        .superseded_media
        .retain(|rel| safe_store_relative_path(rel) && paths.root.join(rel).is_file());
    persist_checkpoint(
        paths,
        &mut opened.catalog,
        &opened.covered,
        limits.max_checkpoint_bytes,
    )?;
    Ok(())
}

/// Current authority-only media that ordinary segment enumeration does not
/// see. Evidence salvage includes these files byte-for-byte.
pub(crate) fn authority_generation_paths(paths: &StorePaths) -> Result<Vec<PathBuf>, StoreError> {
    if !atomic_stage_checkpoint_path(paths).is_file() {
        return Ok(Vec::new());
    }
    let opened = open_catalog_readonly(paths, AtomicStageLimits::operable())?;
    let prefix = format!("store-info/{ATOMIC_AUTHORITY_DIR}/");
    Ok(opened
        .covered
        .into_iter()
        .filter(|covered| {
            covered.atomic_evidence
                && covered.rel_path.starts_with(&prefix)
                && !opened.catalog.superseded_media.contains(&covered.rel_path)
        })
        .map(|covered| paths.root.join(covered.rel_path))
        .filter(|path| path.is_file())
        .collect())
}

fn safe_store_relative_path(rel: &str) -> bool {
    let path = Path::new(rel);
    !path.is_absolute()
        && path.components().all(|component| {
            matches!(
                component,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        })
}

/// Authenticated checkpoint references may name configured external tier
/// media by absolute path. They are comparison/coverage references only: code
/// that removes files must still require [`safe_store_relative_path`].
fn safe_checkpoint_media_reference(reference: &str) -> bool {
    let path = Path::new(reference);
    safe_store_relative_path(reference)
        || (path.is_absolute()
            && path.components().all(|component| {
                matches!(
                    component,
                    std::path::Component::RootDir | std::path::Component::Normal(_)
                )
            }))
}

fn verify_replacement_catalog(
    paths: &StorePaths,
    old: &StageCatalog,
    fresh: &StageCatalog,
) -> Result<(), StoreError> {
    fn same_ref_material<K: Ord>(
        left: &BTreeMap<K, BodyRef>,
        right: &BTreeMap<K, BodyRef>,
    ) -> bool {
        left.len() == right.len()
            && left.iter().all(|(key, value)| {
                right
                    .get(key)
                    .is_some_and(|other| value.len == other.len && value.hash == other.hash)
            })
    }
    fn same_adverse_findings(left: &StageFindings, right: &StageFindings) -> bool {
        let left = left
            .records
            .iter()
            .filter(|finding| finding.class != StageEvidenceClass::Valid)
            .collect::<Vec<_>>();
        let right = right
            .records
            .iter()
            .filter(|finding| finding.class != StageEvidenceClass::Valid)
            .collect::<Vec<_>>();
        left.len() == right.len()
            && left.iter().all(|finding| {
                left.iter()
                    .filter(|candidate| *candidate == finding)
                    .count()
                    == right
                        .iter()
                        .filter(|candidate| *candidate == finding)
                        .count()
            })
    }
    let mut old_tombstones = old.tombstone_index.map_or(0, |index| index.record_count);
    for (key, retained) in &old.tombstones {
        let indexed = match old.tombstone_index {
            Some(index) => lookup_tombstone(paths, index, *key)?,
            None => None,
        };
        match indexed {
            Some(existing) if existing == *retained => {}
            Some(_) => {
                return Err(StoreError::AtomicStage(
                    "Atomic authority generation refused: tail tombstone conflicts with indexed lifetime authority"
                        .into(),
                ));
            }
            None => old_tombstones = old_tombstones.saturating_add(1),
        }
    }
    let fresh_tombstones = fresh.tombstones.len() as u64;
    let tombstone_material_matches = old_tombstones == fresh_tombstones
        && fresh.tombstones.iter().all(|(key, retained)| {
            if old.tombstones.get(key) == Some(retained) {
                return true;
            }
            old.tombstone_index
                .and_then(|index| lookup_tombstone(paths, index, *key).ok().flatten())
                .is_some_and(|indexed| indexed == *retained)
        });
    let mut differences = Vec::new();
    macro_rules! compare {
        ($name:literal, $condition:expr) => {
            if !$condition {
                differences.push($name);
            }
        };
    }
    compare!("prepares", old.prepares == fresh.prepares);
    compare!("members", old.members == fresh.members);
    compare!(
        "payload_refs",
        same_ref_material(&old.payload_refs, &fresh.payload_refs)
    );
    compare!("chunk_plans", old.chunk_plans == fresh.chunk_plans);
    compare!(
        "chunk_refs",
        same_ref_material(&old.chunk_refs, &fresh.chunk_refs)
    );
    compare!("seals", old.seals == fresh.seals);
    compare!("decisions", old.decisions == fresh.decisions);
    compare!("tombstones", tombstone_material_matches);
    compare!("commit_next", old.commit_next == fresh.commit_next);
    compare!(
        "order_frontiers",
        old.order_frontiers == fresh.order_frontiers
    );
    compare!("blocked", old.blocked == fresh.blocked);
    compare!("prepare_batch", old.prepare_batch == fresh.prepare_batch);
    compare!("coord_seq", old.coord_seq == fresh.coord_seq);
    compare!("coord_next", old.coord_next == fresh.coord_next);
    compare!(
        "findings",
        same_adverse_findings(&old.findings, &fresh.findings)
    );
    compare!(
        "coverage_degraded",
        old.coverage_degraded == fresh.coverage_degraded
    );
    compare!(
        "missing_covered",
        old.missing_covered == fresh.missing_covered
    );
    compare!(
        "intended_members",
        old.intended_members == fresh.intended_members
    );
    if !differences.is_empty() {
        return Err(StoreError::AtomicStage(format!(
            "Atomic authority generation refused: replacement catalogue is not materially identical ({}) [tombstones old={} fresh={}; findings old={} fresh={}]",
            differences.join(","),
            old_tombstones,
            fresh_tombstones,
            old.findings.records.len(),
            fresh.findings.records.len(),
        )));
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
        Ok(Some((catalog, _covered))) => Ok(catalog.has_outstanding_evidence()),
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
) -> Result<CatalogOpen, StoreError> {
    open_catalog_inner(paths, limits, true)
}

/// Open or rebuild the catalogue without changing any store media.
///
/// Used by `Store::open_inspect`, whose contract forbids refreshing derived
/// checkpoints or coordinator accelerators while a live writer may exist.
pub(crate) fn open_catalog_readonly(
    paths: &StorePaths,
    limits: AtomicStageLimits,
) -> Result<CatalogOpen, StoreError> {
    open_catalog_inner(paths, limits, false)
}

fn open_catalog_inner(
    paths: &StorePaths,
    limits: AtomicStageLimits,
    persist: bool,
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

    let mut files = list_media_files(paths, &mut budget)?;
    files.retain(|path| {
        let rel = rel_path(paths, path);
        !catalog.superseded_media.iter().any(|old| old == &rel)
    });
    heal_relocated_coverage(paths, &files, &mut catalog, &mut covered, &mut budget)?;
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
                if c.atomic_evidence {
                    catalog.coverage_degraded = true;
                    if !catalog.missing_covered.contains(&rel) {
                        catalog.missing_covered.push(rel.clone());
                    }
                    classify_coverage_loss(&mut findings);
                    next_covered.push(c);
                } else {
                    let atomic = ingest_file_tail(
                        &path,
                        &rel,
                        0,
                        size,
                        &mut catalog,
                        &mut findings,
                        &mut budget,
                    )?;
                    next_covered.push(cover_file(&path, rel, size, atomic)?);
                    budget.report.files_rebuilt = budget.report.files_rebuilt.saturating_add(1);
                }
                continue;
            }
            if c.atomic_evidence {
                let n = verify_covered_blocks(&path, &c)?;
                budget.charge_bytes(n)?;
                budget.report.bytes_verified = budget.report.bytes_verified.saturating_add(n);
            }
            if size == c.covered_len {
                next_covered.push(CoveredFile { rel_path: rel, ..c });
                budget.report.files_skipped = budget.report.files_skipped.saturating_add(1);
                continue;
            }
            budget.report.files_tailed = budget.report.files_tailed.saturating_add(1);
            let tail_atomic = ingest_file_tail(
                &path,
                &rel,
                c.covered_len,
                size,
                &mut catalog,
                &mut findings,
                &mut budget,
            )?;
            let atomic = c.atomic_evidence || tail_atomic;
            next_covered.push(if atomic && !c.atomic_evidence {
                cover_file(&path, rel, size, true)?
            } else {
                extend_cover(&path, Some(&c), rel, size, atomic)?
            });
            continue;
        }
        budget.report.files_rebuilt = budget.report.files_rebuilt.saturating_add(1);
        let atomic = ingest_file_tail(
            &path,
            &rel,
            0,
            size,
            &mut catalog,
            &mut findings,
            &mut budget,
        )?;
        next_covered.push(cover_file(&path, rel, size, atomic)?);
    }

    if !unmatched.is_empty() {
        unmatched.retain(|file| file.atomic_evidence);
        if !unmatched.is_empty() {
            catalog.coverage_degraded = true;
            for file in &unmatched {
                if !catalog.missing_covered.iter().any(|p| p == &file.rel_path) {
                    catalog.missing_covered.push(file.rel_path.clone());
                }
            }
            classify_coverage_loss(&mut findings);
            // Keep the authenticated frontier: scrub must prove that the exact
            // missing bytes returned, not merely that a path now exists.
            next_covered.extend(unmatched);
        }
    }

    load_coordinator(paths, &mut catalog)?;
    finalize_catalog(&mut catalog, &mut findings);
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
    if persist {
        persist_checkpoint(paths, &mut catalog, &covered, limits.max_checkpoint_bytes)?;
        persist_coordinator(paths, &catalog)?;
    }
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
    catalog: &mut StageCatalog,
    covered: &mut Vec<CoveredFile>,
    limits: AtomicStageLimits,
) -> Result<(), StoreError> {
    cover_active(paths, covered)?;
    persist_checkpoint(paths, catalog, covered, limits.max_checkpoint_bytes)?;
    persist_coordinator(paths, catalog)
}

/// Exact encoded size used for pre-append admission.
pub(crate) fn checkpoint_encoded_len(
    catalog: &StageCatalog,
    covered: &[CoveredFile],
) -> Result<u64, StoreError> {
    Ok(encode_checkpoint(catalog, covered)?.len() as u64)
}

/// Authenticate every path retained as missing against its original frontier.
pub(crate) fn verify_missing_coverage(
    paths: &StorePaths,
    catalog: &StageCatalog,
    covered: &[CoveredFile],
    limits: AtomicStageLimits,
) -> Result<(), StoreError> {
    let mut verified = 0u64;
    for rel in &catalog.missing_covered {
        let expected = covered
            .iter()
            .find(|file| &file.rel_path == rel)
            .ok_or_else(|| {
                StoreError::AtomicStage(
                    "atomic stage scrub refused: missing authenticated frontier".into(),
                )
            })?;
        let path = paths.root.join(rel);
        let actual = fs::metadata(&path).map_err(|_| {
            StoreError::AtomicStage(
                "atomic stage scrub refused: covered media still missing".into(),
            )
        })?;
        if !actual.is_file() || actual.len() != expected.covered_len {
            return Err(StoreError::AtomicStage(
                "atomic stage scrub refused: covered media length mismatch".into(),
            ));
        }
        verified = verified.saturating_add(verify_covered_blocks(&path, expected)?);
        if verified > limits.max_scan_bytes {
            return Err(Budget::fail("scrub bytes", verified, limits.max_scan_bytes));
        }
    }
    Ok(())
}

fn cover_active(paths: &StorePaths, covered: &mut Vec<CoveredFile>) -> Result<(), StoreError> {
    // Atomic coordinator evidence is always appended on writer shard zero.
    // Multi-shard stores keep that active under `active/shard-00/`; legacy and
    // single-shard stores retain `active/active.residiuum`.
    let sharded = paths.active_segment_for_shard(0, 2);
    let active = if sharded.is_file() {
        sharded
    } else {
        paths.active_segment()
    };
    if !active.is_file() {
        return Ok(());
    }
    let size = fs::metadata(&active)?.len();
    let rel = rel_path(paths, &active);
    let prev = covered.iter().find(|c| c.rel_path == rel).cloned();
    // This path is called only after an Atomic append/catalogue change.
    let next = extend_cover(&active, prev.as_ref(), rel.clone(), size, true)?;
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
    catalog: &mut StageCatalog,
    findings: &mut StageFindings,
    budget: &mut Budget,
) -> Result<bool, StoreError> {
    if start > size {
        return Ok(false);
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
    let atomic_report = read_atomic_evidence(&bytes, SafetyLimits::draft_defaults());
    let atomic_related = contains_atomic_body_magic(&bytes)
        || atomic_report.examined.iter().any(|examined| {
            !matches!(
                examined.class,
                residiuum_format::AtomicEvidenceClass::Unsupported { .. }
            )
        });
    let report = scan_forward(&bytes, SafetyLimits::draft_defaults());
    if !atomic_related {
        return Ok(false);
    }
    for region in &report.regions {
        match region {
            ScanRegion::Hole { reason, .. } => classify_hole(findings, reason),
            ScanRegion::VerifiedFrame { frame, range } => {
                budget.report.frames = budget.report.frames.saturating_add(1);
                ingest_classified_frame(catalog, frame, findings);
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
    if findings.records.iter().any(|finding| {
        finding.atomic_id.is_none()
            && matches!(
                finding.class,
                StageEvidenceClass::Corrupt | StageEvidenceClass::Partial
            )
    }) {
        catalog.coverage_degraded = true;
    }
    Ok(true)
}

fn list_media_files(paths: &StorePaths, budget: &mut Budget) -> Result<Vec<PathBuf>, StoreError> {
    let mut out = Vec::new();
    collect_files(&paths.active_dir(), 0, budget, &mut out)?;
    collect_files(&paths.segments_dir(), 0, budget, &mut out)?;
    collect_files(&paths.pending_seal_dir(), 0, budget, &mut out)?;
    collect_files(
        &paths.store_info().join(ATOMIC_AUTHORITY_DIR),
        0,
        budget,
        &mut out,
    )?;
    for tier_dir in crate::tier::configured_available_tier_dirs(paths) {
        let mut tier_files = Vec::new();
        collect_files(&tier_dir, 0, budget, &mut tier_files)?;
        out.extend(tier_files.into_iter().filter(|path| {
            path.extension().and_then(|extension| extension.to_str()) == Some("residiuum")
        }));
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// Heal a stale checkpoint pathname only when exactly one currently discovered
/// file authenticates the complete covered prefix. This covers rename-based
/// seal and whole-object tier movement without trusting names or placement
/// catalogues. Ambiguity remains degraded rather than choosing a copy.
fn heal_relocated_coverage(
    paths: &StorePaths,
    files: &[PathBuf],
    catalog: &mut StageCatalog,
    covered: &mut [CoveredFile],
    budget: &mut Budget,
) -> Result<(), StoreError> {
    let existing: std::collections::BTreeSet<String> =
        files.iter().map(|path| rel_path(paths, path)).collect();
    let claimed: std::collections::BTreeSet<String> = covered
        .iter()
        .filter(|frontier| {
            existing.contains(&frontier.rel_path)
                && fs::metadata(paths.root.join(&frontier.rel_path))
                    .is_ok_and(|meta| meta.len() >= frontier.covered_len)
        })
        .map(|frontier| frontier.rel_path.clone())
        .collect();
    let mut used = std::collections::BTreeSet::new();

    for frontier in covered.iter_mut().filter(|frontier| {
        frontier.atomic_evidence
            && (!existing.contains(&frontier.rel_path)
                || match fs::metadata(paths.root.join(&frontier.rel_path)) {
                    Ok(meta) => meta.len() < frontier.covered_len,
                    Err(_) => true,
                })
    }) {
        let mut matches = Vec::new();
        for candidate in files {
            let candidate_rel = rel_path(paths, candidate);
            if claimed.contains(&candidate_rel) || used.contains(&candidate_rel) {
                continue;
            }
            let Ok(meta) = fs::metadata(candidate) else {
                continue;
            };
            if meta.len() < frontier.covered_len {
                continue;
            }
            // Charge before reading so adversarial lookalikes cannot turn path
            // healing into an unbounded reopen scan.
            budget.charge_bytes(frontier.covered_len)?;
            if verify_covered_blocks(candidate, frontier).is_ok() {
                matches.push(candidate_rel);
                if matches.len() > 1 {
                    break;
                }
            }
        }
        if matches.len() == 1 {
            let from = frontier.rel_path.clone();
            let to = matches.pop().expect("single relocation match");
            frontier.rel_path = to.clone();
            catalog.relocate_body_refs(&from, &to);
            used.insert(to);
        }
    }
    Ok(())
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
        atomic_evidence: false,
        covered_len: 0,
        head: [0u8; 32],
        tail: [0u8; 32],
        block_hashes: Vec::new(),
        leftover_len: 0,
        leftover_hash: [0u8; 32],
    }
}

fn coverage_from_bytes(rel_path: String, bytes: &[u8], atomic_evidence: bool) -> CoveredFile {
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
    if !atomic_evidence {
        return CoveredFile {
            rel_path,
            atomic_evidence,
            covered_len: bytes.len() as u64,
            head,
            tail,
            block_hashes,
            leftover_len: 0,
            leftover_hash: [0u8; 32],
        };
    }
    let mut off = 0usize;
    while off + FRONTIER_BLOCK as usize <= bytes.len() {
        let block = &bytes[off..off + FRONTIER_BLOCK as usize];
        block_hashes.push(block_hash(block_hashes.len() as u64, block));
        off += FRONTIER_BLOCK as usize;
    }
    let leftover = &bytes[off..];
    CoveredFile {
        rel_path,
        atomic_evidence,
        covered_len: bytes.len() as u64,
        head,
        tail,
        block_hashes,
        leftover_len: leftover.len() as u32,
        leftover_hash: leftover_hash(leftover),
    }
}

fn cover_file(
    path: &Path,
    rel_path: String,
    covered_len: u64,
    atomic_evidence: bool,
) -> Result<CoveredFile, StoreError> {
    if covered_len == 0 {
        return Ok(empty_coverage(rel_path));
    }
    if !atomic_evidence {
        return Ok(CoveredFile {
            rel_path,
            atomic_evidence: false,
            covered_len,
            head: [0; 32],
            tail: [0; 32],
            block_hashes: Vec::new(),
            leftover_len: 0,
            leftover_hash: [0; 32],
        });
    }
    let mut file = File::open(path)?;
    let mut bytes = vec![0u8; covered_len as usize];
    file.read_exact(&mut bytes)?;
    Ok(coverage_from_bytes(rel_path, &bytes, true))
}

fn extend_cover(
    path: &Path,
    prev: Option<&CoveredFile>,
    rel_path: String,
    new_len: u64,
    atomic_evidence: bool,
) -> Result<CoveredFile, StoreError> {
    let Some(prev) = prev else {
        return cover_file(path, rel_path, new_len, atomic_evidence);
    };
    if !atomic_evidence {
        return cover_file(path, rel_path, new_len, false);
    }
    if prev.covered_len == 0 || prev.covered_len > new_len {
        return cover_file(path, rel_path, new_len, true);
    }
    if !prev.atomic_evidence {
        return cover_file(path, rel_path, new_len, true);
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
            heap_id,
            atomic_id,
            ordinal,
            ..
        } => {
            catalog
                .payload_refs
                .entry((heap_id, atomic_id, ordinal))
                .or_insert(refer);
        }
        SidecarDecode::ChunkBody {
            heap_id,
            atomic_id,
            ordinal,
            index,
            ..
        } => {
            catalog
                .chunk_refs
                .entry((heap_id, atomic_id, ordinal, index))
                .or_insert(refer);
        }
        _ => {}
    }
}

pub(crate) fn resolve_payload_body(
    paths: &StorePaths,
    catalog: &StageCatalog,
    key: StageAtomicKey,
    ordinal: u32,
) -> Result<Option<Vec<u8>>, StoreError> {
    if let Some(payload) = catalog.payloads.get(&(key.0, key.1, ordinal)) {
        return Ok(Some(payload.clone()));
    }
    let Some(refer) = catalog.payload_refs.get(&(key.0, key.1, ordinal)) else {
        return Ok(None);
    };
    sidecar_payload_from_frame(&load_body_ref(paths, refer)?)
}

/// Resolve one exact locator-backed ATPAY1 payload for a published Atomic
/// value. Both identity fields are checked after full frame/hash verification.
pub(crate) fn resolve_published_payload(
    paths: &StorePaths,
    refer: &BodyRef,
    expected_heap_id: HeapId,
    expected_atomic_id: AtomicId,
    expected_ordinal: u32,
) -> Result<Vec<u8>, StoreError> {
    let bytes = load_body_ref(paths, refer)?;
    let decoded = sidecar_payload_from_frame(&bytes)?;
    let Some(payload) = decoded else {
        return Err(StoreError::AtomicStage(
            "published Atomic locator does not name a payload".into(),
        ));
    };

    // sidecar_payload_from_frame deliberately returns only the body for older
    // staging callers. Re-decode the verified bytes here to bind the locator to
    // the exact Atomic identity and ordinal used by the primary projection.
    let matches = match decode_stage_sidecar(&bytes) {
        SidecarDecode::Payload {
            heap_id,
            atomic_id,
            ordinal,
            ..
        } => {
            heap_id == expected_heap_id
                && atomic_id == expected_atomic_id
                && ordinal == expected_ordinal
        }
        _ => {
            let report = scan_forward(&bytes, SafetyLimits::draft_defaults());
            let matched = report.verified_frames().any(|(_, frame)| {
                matches!(
                    decode_stage_sidecar(&frame.body),
                    SidecarDecode::Payload { heap_id, atomic_id, ordinal, .. }
                        if heap_id == expected_heap_id
                            && atomic_id == expected_atomic_id
                            && ordinal == expected_ordinal
                )
            });
            matched
        }
    };
    if !matches {
        return Err(StoreError::AtomicStage(
            "published Atomic locator identity mismatch".into(),
        ));
    }
    Ok(payload)
}

pub(crate) fn resolve_chunk_body(
    paths: &StorePaths,
    catalog: &StageCatalog,
    key: StageAtomicKey,
    ordinal: u32,
    index: u32,
) -> Result<Option<Vec<u8>>, StoreError> {
    if let Some(body) = catalog.chunks.get(&(key.0, key.1, ordinal, index)) {
        return Ok(Some(body.clone()));
    }
    let Some(refer) = catalog.chunk_refs.get(&(key.0, key.1, ordinal, index)) else {
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
    let decoded = decode_checkpoint(&bytes);
    if let Some((catalog, covered)) = &decoded {
        let allowed = |reference: &str| {
            safe_store_relative_path(reference)
                || crate::tier::is_configured_tier_segment_path(paths, Path::new(reference))
        };
        let references_are_allowed = catalog
            .payload_refs
            .values()
            .chain(catalog.chunk_refs.values())
            .all(|reference| allowed(&reference.rel_path))
            && covered.iter().all(|file| allowed(&file.rel_path))
            && catalog.missing_covered.iter().all(|path| allowed(path))
            && catalog.superseded_media.iter().all(|path| allowed(path));
        if !references_are_allowed {
            return Ok(None);
        }
        if let Some(index) = catalog.tombstone_index {
            if validate_index(paths, index).is_err() {
                return Ok(None);
            }
        }
    }
    Ok(decoded)
}

fn persist_coordinator(paths: &StorePaths, catalog: &StageCatalog) -> Result<(), StoreError> {
    crate::failpoint::hit("store.atomic.coord.before_persist")?;
    let mut body = Vec::new();
    body.extend_from_slice(COORD_MAGIC);
    body.push(COORD_VERSION);
    body.extend_from_slice(&catalog.coord_next.to_be_bytes());
    body.extend_from_slice(&(catalog.coord_seq.len() as u32).to_be_bytes());
    let mut rows: Vec<(u64, StageAtomicKey)> = catalog
        .coord_seq
        .iter()
        .map(|(key, seq)| (*seq, *key))
        .collect();
    rows.sort_by_key(|(seq, _)| *seq);
    for (seq, key) in rows {
        body.extend_from_slice(&seq.to_be_bytes());
        body.extend_from_slice(key.0.as_bytes());
        body.extend_from_slice(key.1.as_bytes());
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(COORD_DOMAIN);
    hasher.update(&body);
    body.extend_from_slice(hasher.finalize().as_bytes());
    write_atomic_with_failpoints(&atomic_coord_path(paths), &body, COORD_WRITE_POINTS)?;
    crate::failpoint::hit("store.atomic.coord.after_persist")?;
    Ok(())
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
        if cur.len() < 48 {
            return Err(StoreError::AtomicStage(
                "atomic coordinator truncated".into(),
            ));
        }
        let heap_id = HeapId::from_bytes(cur[..16].try_into().unwrap_or([0; 16]))
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        let id = AtomicId::from_bytes(cur[16..48].try_into().unwrap_or([0; 32]))
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        cur = &cur[48..];
        let key = stage_key(heap_id, id);
        if seq == 0 || !seen.insert(seq) {
            return Err(StoreError::AtomicStage(format!(
                "duplicate or zero coordinator sequence {seq}"
            )));
        }
        catalog.coord_seq.insert(key, seq);
        if !catalog.prepare_seen.contains(&key) {
            catalog.prepare_seen.push(key);
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
    catalog: &mut StageCatalog,
    covered: &[CoveredFile],
    max_checkpoint_bytes: u64,
) -> Result<(), StoreError> {
    crate::failpoint::hit("store.atomic.checkpoint.capacity")?;
    crate::failpoint::hit("store.atomic.checkpoint.before_persist")?;
    if catalog.tombstone_index_dirty || catalog.tombstone_index.is_none() {
        catalog.tombstone_index =
            refresh_index(paths, catalog.tombstone_index, &catalog.tombstones)?;
        if let Some(index) = catalog.tombstone_index {
            catalog.tombstone_index = Some(maybe_reclaim_generation(paths, index)?);
        }
        catalog.tombstone_index_dirty = false;
    }
    // The index is the bounded checkpoint representation. Detailed decisions
    // remain catalogued separately; retaining a second tombstone table here
    // would reintroduce checkpoint growth with decision cardinality.
    catalog.tombstones.clear();
    let bytes = encode_checkpoint(catalog, covered)?;
    if bytes.len() as u64 > max_checkpoint_bytes {
        return Err(Budget::fail(
            "checkpoint bytes",
            bytes.len() as u64,
            max_checkpoint_bytes,
        ));
    }
    write_atomic_with_failpoints(
        &atomic_stage_checkpoint_path(paths),
        &bytes,
        CHECKPOINT_WRITE_POINTS,
    )?;
    crate::failpoint::hit("store.atomic.checkpoint.after_persist")?;
    if let Some(index) = catalog.tombstone_index {
        retire_obsolete_generations(paths, index)?;
    }
    Ok(())
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
        body.push(u8::from(file.atomic_evidence));
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
    for (key, members) in &catalog.members {
        for member in members {
            body.extend_from_slice(key.0.as_bytes());
            let encoded =
                encode_member(member).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
            body.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            body.extend_from_slice(&encoded);
        }
    }
    body.extend_from_slice(&(catalog.payload_refs.len() as u32).to_be_bytes());
    for ((heap_id, id, ordinal), refer) in &catalog.payload_refs {
        body.extend_from_slice(heap_id.as_bytes());
        body.extend_from_slice(id.as_bytes());
        body.extend_from_slice(&ordinal.to_be_bytes());
        encode_body_ref(&mut body, refer);
    }
    body.extend_from_slice(&(catalog.seals.len() as u32).to_be_bytes());
    for ((heap_id, id), root) in &catalog.seals {
        body.extend_from_slice(heap_id.as_bytes());
        body.extend_from_slice(id.as_bytes());
        body.extend_from_slice(root.as_bytes());
    }
    body.extend_from_slice(&(catalog.decisions.len() as u32).to_be_bytes());
    for (key, decision) in &catalog.decisions {
        body.extend_from_slice(key.0.as_bytes());
        let encoded =
            encode_decision(decision).map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        body.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        body.extend_from_slice(&encoded);
    }
    body.extend_from_slice(&(catalog.tombstones.len() as u32).to_be_bytes());
    for ((heap_id, _), retained) in &catalog.tombstones {
        body.extend_from_slice(heap_id.as_bytes());
        body.extend_from_slice(&retained.decided_at_unix_s.to_be_bytes());
        let encoded = encode_tombstone(&retained.tombstone)
            .map_err(|e| StoreError::AtomicStage(e.to_string()))?;
        body.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        body.extend_from_slice(&encoded);
    }
    match catalog.tombstone_index {
        Some(index) => {
            body.push(1);
            body.extend_from_slice(&index.record_count.to_be_bytes());
            body.extend_from_slice(&index.file_len.to_be_bytes());
            body.extend_from_slice(&index.root);
        }
        None => body.push(0),
    }
    body.extend_from_slice(&(catalog.commit_next.len() as u32).to_be_bytes());
    for (heap_id, next) in &catalog.commit_next {
        body.extend_from_slice(heap_id.as_bytes());
        body.extend_from_slice(&next.to_be_bytes());
    }
    body.extend_from_slice(&(catalog.order_frontiers.len() as u32).to_be_bytes());
    for ((heap_id, id), frontiers) in &catalog.order_frontiers {
        body.extend_from_slice(heap_id.as_bytes());
        body.extend_from_slice(id.as_bytes());
        body.extend_from_slice(&(frontiers.len() as u16).to_be_bytes());
        for frontier in frontiers {
            body.extend_from_slice(&frontier.shard.to_be_bytes());
            body.extend_from_slice(&frontier.segment_id);
            body.extend_from_slice(&frontier.next_writer_sequence.to_be_bytes());
        }
    }
    body.extend_from_slice(&(catalog.blocked.len() as u32).to_be_bytes());
    for (heap_id, id) in &catalog.blocked {
        body.extend_from_slice(heap_id.as_bytes());
        body.extend_from_slice(id.as_bytes());
    }
    body.extend_from_slice(&(catalog.prepare_batch.len() as u32).to_be_bytes());
    for (heap_id, id) in &catalog.prepare_batch {
        body.extend_from_slice(heap_id.as_bytes());
        body.extend_from_slice(id.as_bytes());
    }
    body.extend_from_slice(&catalog.coord_next.to_be_bytes());
    body.extend_from_slice(&(catalog.coord_seq.len() as u32).to_be_bytes());
    for ((heap_id, id), seq) in &catalog.coord_seq {
        body.extend_from_slice(heap_id.as_bytes());
        body.extend_from_slice(id.as_bytes());
        body.extend_from_slice(&seq.to_be_bytes());
    }
    body.extend_from_slice(&(catalog.chunk_plans.len() as u32).to_be_bytes());
    for ((heap_id, id, ordinal), plan) in &catalog.chunk_plans {
        body.extend_from_slice(heap_id.as_bytes());
        body.extend_from_slice(id.as_bytes());
        body.extend_from_slice(&ordinal.to_be_bytes());
        body.extend_from_slice(&plan.total.to_be_bytes());
        body.extend_from_slice(&(plan.chunk_hashes.len() as u32).to_be_bytes());
        for hash in &plan.chunk_hashes {
            body.extend_from_slice(hash);
        }
    }
    body.extend_from_slice(&(catalog.chunk_refs.len() as u32).to_be_bytes());
    for ((heap_id, id, ordinal, index), refer) in &catalog.chunk_refs {
        body.extend_from_slice(heap_id.as_bytes());
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
        match finding.heap_id {
            Some(heap_id) => {
                body.push(1);
                body.extend_from_slice(heap_id.as_bytes());
            }
            None => body.push(0),
        }
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
    for ((heap_id, id), n) in &catalog.intended_members {
        body.extend_from_slice(heap_id.as_bytes());
        body.extend_from_slice(id.as_bytes());
        body.extend_from_slice(&n.to_be_bytes());
    }
    body.extend_from_slice(&(catalog.superseded_media.len() as u32).to_be_bytes());
    for rel in &catalog.superseded_media {
        let bytes = rel.as_bytes();
        body.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        body.extend_from_slice(bytes);
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
        if cur.len() < n + 1 + 8 + 64 + 4 {
            return None;
        }
        let rel = String::from_utf8(cur[..n].to_vec()).ok()?;
        cur = &cur[n..];
        let atomic_evidence = match cur[0] {
            0 => false,
            1 => true,
            _ => return None,
        };
        cur = &cur[1..];
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
        if atomic_evidence
            && n_blocks as u64 * FRONTIER_BLOCK + u64::from(leftover_len) != covered_len
        {
            return None;
        }
        if !atomic_evidence && (n_blocks != 0 || leftover_len != 0) {
            return None;
        }
        covered.push(CoveredFile {
            rel_path: rel,
            atomic_evidence,
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
        catalog
            .prepares
            .insert(stage_key(prepare.heap_id, prepare.atomic_id), prepare);
    }
    let n_members = read_u32(&mut cur)? as usize;
    for _ in 0..n_members {
        if cur.len() < 16 {
            return None;
        }
        let heap_id = HeapId::from_bytes(cur[..16].try_into().ok()?).ok()?;
        cur = &cur[16..];
        let n = read_u32(&mut cur)? as usize;
        if cur.len() < n {
            return None;
        }
        let member = decode_member(&cur[..n]).ok()?;
        cur = &cur[n..];
        catalog
            .members
            .entry(stage_key(heap_id, member.atomic_id))
            .or_default()
            .push(member);
    }
    let n_payloads = read_u32(&mut cur)? as usize;
    for _ in 0..n_payloads {
        if cur.len() < 16 + 32 + 4 {
            return None;
        }
        let heap_id = HeapId::from_bytes(cur[..16].try_into().ok()?).ok()?;
        let id = AtomicId::from_bytes(cur[16..48].try_into().ok()?).ok()?;
        cur = &cur[48..];
        let ordinal = read_u32(&mut cur)?;
        let refer = decode_body_ref(&mut cur)?;
        catalog.payload_refs.insert((heap_id, id, ordinal), refer);
    }
    let n_seals = read_u32(&mut cur)? as usize;
    for _ in 0..n_seals {
        if cur.len() < 80 {
            return None;
        }
        let heap_id = HeapId::from_bytes(cur[..16].try_into().ok()?).ok()?;
        let id = AtomicId::from_bytes(cur[16..48].try_into().ok()?).ok()?;
        let root = ContentRoot::from_bytes(cur[48..80].try_into().ok()?).ok()?;
        cur = &cur[80..];
        catalog.seals.insert(stage_key(heap_id, id), root);
    }
    let n_decisions = read_u32(&mut cur)? as usize;
    for _ in 0..n_decisions {
        if cur.len() < 16 {
            return None;
        }
        let heap_id = HeapId::from_bytes(cur[..16].try_into().ok()?).ok()?;
        cur = &cur[16..];
        let n = read_u32(&mut cur)? as usize;
        if cur.len() < n {
            return None;
        }
        let decision = decode_decision(&cur[..n]).ok()?;
        cur = &cur[n..];
        if catalog
            .decisions
            .insert(stage_key(heap_id, decision.atomic_id), decision)
            .is_some()
        {
            return None;
        }
    }
    let n_tombstones = read_u32(&mut cur)? as usize;
    for _ in 0..n_tombstones {
        if cur.len() < 16 + 8 + 4 {
            return None;
        }
        let heap_id = HeapId::from_bytes(cur[..16].try_into().ok()?).ok()?;
        cur = &cur[16..];
        let decided_at_unix_s = read_u64(&mut cur)?;
        let n = read_u32(&mut cur)? as usize;
        if cur.len() < n {
            return None;
        }
        let tombstone = decode_tombstone(&cur[..n]).ok()?;
        cur = &cur[n..];
        let key = stage_key(heap_id, tombstone.atomic_id);
        if catalog
            .tombstones
            .insert(
                key,
                RetainedDecisionTombstone {
                    decided_at_unix_s,
                    tombstone,
                },
            )
            .is_some()
        {
            return None;
        }
    }
    let has_tombstone_index = *cur.first()?;
    cur = &cur[1..];
    catalog.tombstone_index = match has_tombstone_index {
        0 => None,
        1 => {
            let record_count = read_u64(&mut cur)?;
            let file_len = read_u64(&mut cur)?;
            if cur.len() < 32 {
                return None;
            }
            let mut root = [0u8; 32];
            root.copy_from_slice(&cur[..32]);
            cur = &cur[32..];
            Some(TombstoneIndexMeta {
                record_count,
                file_len,
                root,
            })
        }
        _ => return None,
    };
    let n_commit_heaps = read_u32(&mut cur)? as usize;
    for _ in 0..n_commit_heaps {
        if cur.len() < 24 {
            return None;
        }
        let heap_id = HeapId::from_bytes(cur[..16].try_into().ok()?).ok()?;
        cur = &cur[16..];
        let next = read_u64(&mut cur)?;
        if next == 0 || catalog.commit_next.insert(heap_id, next).is_some() {
            return None;
        }
    }
    let n_order_frontiers = read_u32(&mut cur)? as usize;
    for _ in 0..n_order_frontiers {
        if cur.len() < 50 {
            return None;
        }
        let heap_id = HeapId::from_bytes(cur[..16].try_into().ok()?).ok()?;
        let id = AtomicId::from_bytes(cur[16..48].try_into().ok()?).ok()?;
        cur = &cur[48..];
        let count = read_u16(&mut cur)? as usize;
        if count == 0 || cur.len() < count.saturating_mul(26) {
            return None;
        }
        let mut frontiers = Vec::with_capacity(count);
        for expected_shard in 0..count {
            let shard = read_u16(&mut cur)?;
            if usize::from(shard) != expected_shard || cur.len() < 24 {
                return None;
            }
            let segment_id = cur[..16].try_into().ok()?;
            cur = &cur[16..];
            let next_writer_sequence = read_u64(&mut cur)?;
            if segment_id == [0; 16] {
                return None;
            }
            frontiers.push(AtomicShardFrontier {
                shard,
                segment_id,
                next_writer_sequence,
            });
        }
        if catalog
            .order_frontiers
            .insert(stage_key(heap_id, id), frontiers)
            .is_some()
        {
            return None;
        }
    }
    let n_blocked = read_u32(&mut cur)? as usize;
    for _ in 0..n_blocked {
        if cur.len() < 48 {
            return None;
        }
        let heap_id = HeapId::from_bytes(cur[..16].try_into().ok()?).ok()?;
        let id = AtomicId::from_bytes(cur[16..48].try_into().ok()?).ok()?;
        cur = &cur[48..];
        catalog.blocked.insert(stage_key(heap_id, id));
    }
    let n_batch = read_u32(&mut cur)? as usize;
    for _ in 0..n_batch {
        if cur.len() < 48 {
            return None;
        }
        let heap_id = HeapId::from_bytes(cur[..16].try_into().ok()?).ok()?;
        let id = AtomicId::from_bytes(cur[16..48].try_into().ok()?).ok()?;
        cur = &cur[48..];
        catalog.prepare_batch.insert(stage_key(heap_id, id));
    }
    catalog.coord_next = read_u64(&mut cur)?;
    let n_coord = read_u32(&mut cur)? as usize;
    for _ in 0..n_coord {
        if cur.len() < 56 {
            return None;
        }
        let heap_id = HeapId::from_bytes(cur[..16].try_into().ok()?).ok()?;
        let id = AtomicId::from_bytes(cur[16..48].try_into().ok()?).ok()?;
        cur = &cur[48..];
        let seq = read_u64(&mut cur)?;
        if seq == 0 || catalog.coord_seq.values().any(|&s| s == seq) {
            return None;
        }
        catalog.coord_seq.insert(stage_key(heap_id, id), seq);
    }
    let n_plans = read_u32(&mut cur)? as usize;
    for _ in 0..n_plans {
        if cur.len() < 16 + 32 + 4 + 4 + 4 {
            return None;
        }
        let heap_id = HeapId::from_bytes(cur[..16].try_into().ok()?).ok()?;
        let id = AtomicId::from_bytes(cur[16..48].try_into().ok()?).ok()?;
        cur = &cur[48..];
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
            (heap_id, id, ordinal),
            ChunkPlan {
                total,
                chunk_hashes,
            },
        );
    }
    let n_chunks = read_u32(&mut cur)? as usize;
    for _ in 0..n_chunks {
        if cur.len() < 16 + 32 + 4 + 4 {
            return None;
        }
        let heap_id = HeapId::from_bytes(cur[..16].try_into().ok()?).ok()?;
        let id = AtomicId::from_bytes(cur[16..48].try_into().ok()?).ok()?;
        cur = &cur[48..];
        let ordinal = read_u32(&mut cur)?;
        let index = read_u32(&mut cur)?;
        let refer = decode_body_ref(&mut cur)?;
        catalog
            .chunk_refs
            .insert((heap_id, id, ordinal, index), refer);
    }
    if cur.is_empty() {
        return None;
    }
    catalog.coverage_degraded = cur[0] != 0;
    cur = &cur[1..];
    let n_findings = read_u32(&mut cur)? as usize;
    for _ in 0..n_findings {
        if cur.len() < 4 {
            return None;
        }
        let kind = decode_finding_kind(cur[0])?;
        let class = decode_finding_class(cur[1])?;
        let has_heap = cur[2];
        cur = &cur[3..];
        let heap_id = match has_heap {
            0 => None,
            1 => {
                if cur.len() < 16 {
                    return None;
                }
                let heap_id = HeapId::from_bytes(cur[..16].try_into().ok()?).ok()?;
                cur = &cur[16..];
                Some(heap_id)
            }
            _ => return None,
        };
        let has_id = *cur.first()?;
        cur = &cur[1..];
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
            heap_id,
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
        if cur.len() < 52 {
            return None;
        }
        let heap_id = HeapId::from_bytes(cur[..16].try_into().ok()?).ok()?;
        let id = AtomicId::from_bytes(cur[16..48].try_into().ok()?).ok()?;
        cur = &cur[48..];
        let n = read_u32(&mut cur)?;
        catalog.intended_members.insert(stage_key(heap_id, id), n);
    }
    let n_superseded = read_u32(&mut cur)? as usize;
    for _ in 0..n_superseded {
        let n = read_u16(&mut cur)? as usize;
        if cur.len() < n {
            return None;
        }
        let rel = String::from_utf8(cur[..n].to_vec()).ok()?;
        cur = &cur[n..];
        if rel.is_empty()
            || !safe_checkpoint_media_reference(&rel)
            || catalog.superseded_media.contains(&rel)
        {
            return None;
        }
        catalog.superseded_media.push(rel);
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
mod checkpoint_v18_freeze {
    use super::*;
    use crate::atomic_stage_media::{
        encode_stage_chunk_body, encode_stage_chunk_plan, encode_stage_payload, encode_stage_seal,
        encode_stage_tombstone, RetainedDecisionTombstone,
    };
    use residiuum_atomics::{decision_hash, AtomicAbortReason, AtomicDecision, DecisionCode};

    fn aid() -> AtomicId {
        let mut b = [0u8; 32];
        b[0] = 9;
        AtomicId::from_bytes(b).unwrap()
    }

    #[test]
    fn v18_empty_checkpoint_roundtrips() {
        let catalog = StageCatalog::default();
        let bytes = encode_checkpoint(&catalog, &[]).unwrap();
        assert!(bytes.starts_with(CHECKPOINT_MAGIC));
        assert_eq!(bytes[CHECKPOINT_MAGIC.len()], CHECKPOINT_VERSION);
        let (decoded, covered) = decode_checkpoint(&bytes).expect("v18 empty");
        assert!(covered.is_empty());
        assert!(!decoded.has_outstanding_evidence());
    }

    #[test]
    fn v18_finding_roundtrips_heap_qualified_role_and_identity() {
        let heap_id = HeapId::from_bytes([0x44; 16]).unwrap();
        let mut catalog = StageCatalog::default();
        catalog.findings.records.push(StageFinding {
            kind: crate::atomic_stage_classify::StageEvidenceKind::Tombstone,
            class: StageEvidenceClass::Corrupt,
            heap_id: Some(heap_id),
            atomic_id: Some(aid()),
        });
        let bytes = encode_checkpoint(&catalog, &[]).unwrap();
        let (decoded, _) = decode_checkpoint(&bytes).expect("v18 finding");
        assert_eq!(decoded.findings, catalog.findings);
    }

    #[test]
    fn replacement_comparison_normalizes_only_valid_scan_observations() {
        let dir = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        let heap_id = HeapId::from_bytes([0x44; 16]).unwrap();
        let valid = StageFinding {
            kind: crate::atomic_stage_classify::StageEvidenceKind::ChunkBody,
            class: StageEvidenceClass::Valid,
            heap_id: Some(heap_id),
            atomic_id: Some(aid()),
        };
        let corrupt = StageFinding {
            class: StageEvidenceClass::Corrupt,
            ..valid.clone()
        };

        let old = StageCatalog::default();
        let mut fresh = StageCatalog::default();
        fresh.findings.records.push(valid);
        verify_replacement_catalog(&paths, &old, &fresh)
            .expect("valid reconstruction provenance is not material damage");

        fresh.findings.records.push(corrupt);
        let err = verify_replacement_catalog(&paths, &old, &fresh).unwrap_err();
        assert!(err.to_string().contains("findings"));
    }

    #[test]
    fn absolute_body_reference_requires_a_configured_tier_root() {
        let dir = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(dir.path().join("store"));
        paths.create_dirs().unwrap();
        let external = dir.path().join("external");
        fs::create_dir_all(&external).unwrap();
        let segment = crate::ids::mint_sortable_segment_id(1, &[3u8; 16]);
        let absolute = external
            .join(format!("{}.residiuum", crate::layout::hex16(&segment)))
            .to_string_lossy()
            .into_owned();
        let heap_id = HeapId::from_bytes([0x44; 16]).unwrap();
        let mut catalog = StageCatalog::default();
        catalog.payload_refs.insert(
            (heap_id, aid(), 0),
            BodyRef {
                rel_path: absolute,
                offset: 0,
                len: 1,
                hash: [7u8; 32],
            },
        );
        let checkpoint = encode_checkpoint(&catalog, &[]).unwrap();
        fs::write(atomic_stage_checkpoint_path(&paths), &checkpoint).unwrap();
        let mut budget = Budget::new(AtomicStageLimits::operable());
        assert!(load_checkpoint(&paths, &mut budget).unwrap().is_none());

        let mut placement = crate::tier::TierPlacement::new();
        placement.set_external_root(crate::tier::TierClass::Cold, external);
        crate::tier::write_tier_roots_file(&paths, &placement).unwrap();
        let mut budget = Budget::new(AtomicStageLimits::operable());
        assert!(load_checkpoint(&paths, &mut budget).unwrap().is_some());
    }

    #[test]
    fn paged_tombstones_do_not_grow_the_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let paths = StorePaths::new(dir.path());
        fs::create_dir_all(paths.store_info()).unwrap();
        let heap_id = HeapId::from_bytes([0x44; 16]).unwrap();
        let mut catalog = StageCatalog::default();
        for n in 1u32..=1_500 {
            let mut id_bytes = [0u8; 32];
            id_bytes[..4].copy_from_slice(&n.to_be_bytes());
            let atomic_id = AtomicId::from_bytes(id_bytes).unwrap();
            let mut root_bytes = [0u8; 32];
            root_bytes[..4].copy_from_slice(&n.to_be_bytes());
            catalog.tombstones.insert(
                stage_key(heap_id, atomic_id),
                RetainedDecisionTombstone {
                    decided_at_unix_s: u64::from(n),
                    tombstone: residiuum_atomics::DecisionTombstone {
                        atomic_id,
                        content_root: ContentRoot::from_bytes(root_bytes).unwrap(),
                        decision: DecisionCode::NotCommitted,
                        commit_position: None,
                        decision_hash: *blake3::hash(&n.to_be_bytes()).as_bytes(),
                        abort_reason: Some(AtomicAbortReason::RecoveryAbort),
                    },
                },
            );
        }
        catalog.tombstone_index_dirty = true;
        persist_checkpoint(&paths, &mut catalog, &[], u64::MAX).unwrap();

        let checkpoint = fs::read(atomic_stage_checkpoint_path(&paths)).unwrap();
        assert!(
            checkpoint.len() < 256,
            "checkpoint was {} bytes",
            checkpoint.len()
        );
        let (decoded, _) = decode_checkpoint(&checkpoint).unwrap();
        assert!(decoded.tombstones.is_empty());
        assert_eq!(decoded.tombstone_index.unwrap().record_count, 1_500);
    }

    #[test]
    fn sidecar_magics_are_frozen() {
        let id = aid();
        let heap_id = HeapId::from_bytes([0x44; 16]).unwrap();
        let root = ContentRoot::from_bytes([0x11; 32]).unwrap();
        assert!(encode_stage_payload(heap_id, id, 0, b"x").starts_with(b"ATPAY1"));
        assert!(encode_stage_seal(heap_id, id, root).starts_with(b"ATSEAL1"));
        assert!(encode_stage_chunk_plan(
            heap_id,
            id,
            0,
            &ChunkPlan {
                total: 1,
                chunk_hashes: vec![[0x22; 32]],
            }
        )
        .starts_with(b"ATMAP1"));
        assert!(encode_stage_chunk_body(heap_id, id, 0, 0, b"c").starts_with(b"ATCHK1"));
        let decision = AtomicDecision::not_committed(
            id,
            [0x31; 32],
            [0x32; 32],
            1,
            AtomicAbortReason::RecoveryAbort,
        );
        let retained = RetainedDecisionTombstone {
            decided_at_unix_s: 7,
            tombstone: decision.tombstone(root, decision_hash(&decision).unwrap()),
        };
        let encoded = encode_stage_tombstone(heap_id, retained).unwrap();
        assert!(encoded.starts_with(b"ATTOMB2"));
        assert!(matches!(
            decode_stage_sidecar(&encoded),
            SidecarDecode::Tombstone { heap_id: got_heap, retained: got }
                if got_heap == heap_id && got == retained
        ));
    }
}
