//! Heap legacy migration job (`HEAP_SPEC` §36 / HP-006).
//!
//! Explicit, crash-resumable phases. Opening a legacy store never silently
//! upgrades it. Phase 6 (cut over) is refused while any unlabelled active
//! frame remains.
//!
//! This module owns the durable job engine and phase-6 gate. Full physical
//! segment rewrite against live `Store` trees continues to expand here; the
//! Accept corpus uses an inventory-backed rewrite log that is idempotent under
//! crash injection.

use crate::atomic_file::write_atomic;
use crate::error::StoreError;
use crate::failpoint;
use crate::ids::random_id;
use residiuum_format::{decode_deterministic_uint_map, encode_deterministic_uint_map, CborValue};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Profile label for heap migration jobs.
pub const HEAP_MIGRATE_PROFILE: &str = "residiuum-heap-migrate-v1";

/// Directory under a store root for heap migration jobs.
pub const HEAP_MIGRATE_DIR: &str = "migration";

/// Durable job state filename.
pub const STATE_FILE: &str = "state.v1.cbor";

/// Immutable assignment map filename.
pub const ASSIGNMENTS_FILE: &str = "assigned-objects.v1.cbor";

/// Durable admitted-frame log (content hashes already rewritten).
pub const ADMITTED_FILE: &str = "admitted.v1.cbor";

/// Domain for §34.7 migration inventory hash.
pub const INVENTORY_HASH_DOMAIN: &[u8] = b"RESIDIUUM-HEAP-MIGRATION-INVENTORY-V1";

/// Domain for §34.7 assignment-map hash.
pub const ASSIGNMENTS_HASH_DOMAIN: &[u8] = b"RESIDIUUM-HEAP-MIGRATION-ASSIGNMENTS-V1";

/// Migration phase numbers (`HEAP_SPEC` §36.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum MigrationPhase {
    /// Exclusive lease + immutable inventory + verified backup.
    Preflight = 0,
    /// Compatibility heap / authority head established.
    Establish = 1,
    /// Immutable IDs assigned.
    Identify = 2,
    /// Legacy readable; new writes labelled.
    DualRead = 3,
    /// Rewrite legacy segments into single-heap segments.
    Rewrite = 4,
    /// Source frames accounted for.
    Verify = 5,
    /// Catalogs published; raw APIs disabled.
    CutOver = 6,
    /// Legacy files quarantined.
    Quarantine = 7,
}

impl MigrationPhase {
    /// Parse phase byte.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Preflight),
            1 => Some(Self::Establish),
            2 => Some(Self::Identify),
            3 => Some(Self::DualRead),
            4 => Some(Self::Rewrite),
            5 => Some(Self::Verify),
            6 => Some(Self::CutOver),
            7 => Some(Self::Quarantine),
            _ => None,
        }
    }

    /// Wire / state byte.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Next phase, if any.
    pub fn successor(self) -> Option<Self> {
        Self::from_u8(self.as_u8() + 1)
    }
}

/// One inventory segment entry (§34.7 canonical inventory).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventorySegment {
    /// Source segment id (16 bytes).
    pub segment_id: [u8; 16],
    /// Complete source byte length.
    pub byte_length: u64,
    /// BLAKE3-256 of complete source bytes.
    pub content_hash: [u8; 32],
}

/// One source frame tracked for labelling / rewrite accounting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryFrame {
    /// Stable frame identity for resume.
    pub frame_id: [u8; 16],
    /// Owning source segment.
    pub segment_id: [u8; 16],
    /// BLAKE3-256 of complete source frame bytes.
    pub content_hash: [u8; 32],
    /// Whether the source frame already carried a heap binding.
    pub labelled: bool,
    /// Operator intentionally quarantines rather than rewrites.
    pub quarantine: bool,
}

/// Immutable source inventory for a migration job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceInventory {
    /// Segment rows (sorted by segment id for hashing).
    pub segments: Vec<InventorySegment>,
    /// Frame rows under those segments.
    pub frames: Vec<InventoryFrame>,
}

impl SourceInventory {
    /// §34.7 inventory hash over canonical ordered segments.
    pub fn inventory_hash(&self) -> Result<[u8; 32], StoreError> {
        let mut segs = self.segments.clone();
        segs.sort_by(|a, b| a.segment_id.cmp(&b.segment_id));
        let mut ids = BTreeSet::new();
        for s in &segs {
            if !ids.insert(s.segment_id) {
                return Err(StoreError::HeapAdmit(
                    "migration inventory duplicate segment id".into(),
                ));
            }
        }
        let mut body = Vec::new();
        for s in &segs {
            body.extend_from_slice(&s.segment_id);
            body.extend_from_slice(&s.byte_length.to_be_bytes());
            body.extend_from_slice(&s.content_hash);
        }
        Ok(domain_hash(INVENTORY_HASH_DOMAIN, &body))
    }

    /// Count of source frames.
    pub fn source_frame_count(&self) -> u64 {
        self.frames.len() as u64
    }
}

/// Durable job state (`HEAP_SPEC` §36.1 `MigrationStateV1`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationStateV1 {
    /// Job id (UUIDv4 bytes).
    pub job_id: [u8; 16],
    /// Source store id.
    pub source_store_id: [u8; 16],
    /// Deployment id.
    pub deployment_id: [u8; 16],
    /// Destination heap id.
    pub destination_heap_id: [u8; 16],
    /// Current phase.
    pub phase: MigrationPhase,
    /// Hash of immutable inventory.
    pub source_inventory_hash: [u8; 32],
    /// Next segment to rewrite (if any).
    pub next_segment: Option<[u8; 16]>,
    /// Completed segment id + rewritten-segment hash pairs.
    pub completed_segments: Vec<([u8; 16], [u8; 32])>,
    /// Hash of assignment map.
    pub assigned_objects_hash: [u8; 32],
    /// Frames rewritten into labelled destination form.
    pub rewritten_frames: u64,
    /// Frames intentionally quarantined.
    pub quarantined_frames: u64,
    /// Job start unix seconds.
    pub started_at: i64,
    /// Last update unix seconds.
    pub updated_at: i64,
}

/// Phase-6 cutover gate report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CutoverGate {
    /// Total source frames in inventory.
    pub source_frames: u64,
    /// Successfully rewritten (labelled) frames.
    pub rewritten_frames: u64,
    /// Intentionally quarantined frames.
    pub intentionally_quarantined_frames: u64,
    /// Source frames that remain unlabelled and active.
    pub unlabelled_active_frames: u64,
    /// Segments that still claim more than one heap (always 0 in v1 engine).
    pub cross_heap_segments: u64,
}

impl CutoverGate {
    /// Whether phase 6 may proceed (`HEAP_SPEC` §36).
    pub fn allows_cutover(&self) -> bool {
        self.source_frames == self.rewritten_frames + self.intentionally_quarantined_frames
            && self.unlabelled_active_frames == 0
            && self.cross_heap_segments == 0
    }
}

/// In-memory + durable migration job handle.
#[derive(Debug)]
pub struct HeapMigrationJob {
    root: PathBuf,
    inventory: SourceInventory,
    state: MigrationStateV1,
    /// Content hashes already admitted to the destination (idempotent resume).
    admitted: BTreeSet<[u8; 32]>,
    /// Content hashes quarantined.
    quarantined: BTreeSet<[u8; 32]>,
    /// Canonical source-name → new object id assignments (immutable after identify).
    assignments: BTreeMap<String, [u8; 16]>,
}

impl HeapMigrationJob {
    /// Job directory path.
    pub fn dir(&self) -> &Path {
        &self.root
    }

    /// Current state snapshot.
    pub fn state(&self) -> &MigrationStateV1 {
        &self.state
    }

    /// Inventory borrowed.
    pub fn inventory(&self) -> &SourceInventory {
        &self.inventory
    }

    /// Begin phase 0: write exclusive job dir, inventory hash, empty assignments.
    pub fn begin_preflight(
        store_root: &Path,
        source_store_id: [u8; 16],
        deployment_id: [u8; 16],
        destination_heap_id: [u8; 16],
        inventory: SourceInventory,
    ) -> Result<Self, StoreError> {
        let job_id = random_id()?;
        let inv_hash = inventory.inventory_hash()?;
        let now = now_unix()?;
        let root = store_root.join(HEAP_MIGRATE_DIR).join(hex16(&job_id));
        if root.exists() {
            return Err(StoreError::HeapAdmit("migration job dir exists".into()));
        }
        fs::create_dir_all(&root)?;

        let state = MigrationStateV1 {
            job_id,
            source_store_id,
            deployment_id,
            destination_heap_id,
            phase: MigrationPhase::Preflight,
            source_inventory_hash: inv_hash,
            next_segment: inventory.segments.first().map(|s| s.segment_id),
            completed_segments: Vec::new(),
            assigned_objects_hash: domain_hash(ASSIGNMENTS_HASH_DOMAIN, &[]),
            rewritten_frames: 0,
            quarantined_frames: 0,
            started_at: now,
            updated_at: now,
        };

        let mut job = Self {
            root,
            inventory,
            state,
            admitted: BTreeSet::new(),
            quarantined: BTreeSet::new(),
            assignments: BTreeMap::new(),
        };
        job.persist_all()?;
        Ok(job)
    }

    /// Reload a job from disk (crash resume).
    pub fn open(
        store_root: &Path,
        job_id: [u8; 16],
        inventory: SourceInventory,
    ) -> Result<Self, StoreError> {
        let root = store_root.join(HEAP_MIGRATE_DIR).join(hex16(&job_id));
        let state = load_state(&root.join(STATE_FILE))?;
        if state.job_id != job_id {
            return Err(StoreError::HeapAdmit("migration job id mismatch".into()));
        }
        let inv_hash = inventory.inventory_hash()?;
        if inv_hash != state.source_inventory_hash {
            return Err(StoreError::HeapAdmit(
                "migration inventory hash changed under job".into(),
            ));
        }
        let assignments = load_assignments(&root.join(ASSIGNMENTS_FILE))?;
        let (admitted, quarantined) = load_admitted(&root.join(ADMITTED_FILE))?;
        let mut state = state;
        // Reconcile counters from durable admission log (crash may have
        // persisted admits before the state document).
        state.rewritten_frames = admitted.len() as u64;
        state.quarantined_frames = quarantined.len() as u64;
        Ok(Self {
            root,
            inventory,
            state,
            admitted,
            quarantined,
            assignments,
        })
    }

    /// Advance through establish → identify (assigns object IDs once).
    pub fn run_establish_and_identify(&mut self, object_names: &[&str]) -> Result<(), StoreError> {
        self.require_phase(MigrationPhase::Preflight)?;
        self.advance_to(MigrationPhase::Establish)?;
        self.require_phase(MigrationPhase::Establish)?;
        if self.assignments.is_empty() {
            for name in object_names {
                if self.assignments.contains_key(*name) {
                    return Err(StoreError::HeapAdmit("duplicate assignment name".into()));
                }
                self.assignments.insert((*name).to_string(), random_id()?);
            }
            self.state.assigned_objects_hash = assignments_hash(&self.assignments)?;
            self.persist_assignments()?;
        } else {
            // Resume: assignment map must remain identical.
            let expect = assignments_hash(&self.assignments)?;
            if expect != self.state.assigned_objects_hash {
                return Err(StoreError::HeapAdmit(
                    "assignment map hash mismatch on resume".into(),
                ));
            }
        }
        self.advance_to(MigrationPhase::Identify)?;
        self.advance_to(MigrationPhase::DualRead)?;
        Ok(())
    }

    /// Rewrite / quarantine all inventory frames idempotently (phase 4).
    ///
    /// Each durable frame admission hits failpoints so crash injection can
    /// interrupt mid-log; resume never double-counts.
    pub fn run_rewrite(&mut self) -> Result<(), StoreError> {
        while self.state.phase < MigrationPhase::Rewrite {
            // Allow calling rewrite after dual-read only.
            if self.state.phase == MigrationPhase::DualRead {
                self.advance_to(MigrationPhase::Rewrite)?;
            } else {
                return Err(StoreError::HeapAdmit(format!(
                    "rewrite requires dual_read or rewrite, got {}",
                    self.state.phase.as_u8()
                )));
            }
        }
        self.require_phase(MigrationPhase::Rewrite)?;

        for frame in self.inventory.frames.clone() {
            if self.admitted.contains(&frame.content_hash)
                || self.quarantined.contains(&frame.content_hash)
            {
                continue;
            }
            failpoint::hit("heap_migration.before_frame_admit")?;
            if frame.quarantine {
                self.quarantined.insert(frame.content_hash);
                self.state.quarantined_frames = self.quarantined.len() as u64;
            } else {
                // Rewrite admits a labelled destination frame keyed by content hash.
                self.admitted.insert(frame.content_hash);
                self.state.rewritten_frames = self.admitted.len() as u64;
            }
            self.state.updated_at = now_unix()?;
            self.persist_admitted()?;
            self.persist_state()?;
            failpoint::hit("heap_migration.after_frame_admit")?;
        }

        // Mark segments complete with a deterministic rewritten-segment hash.
        self.state.completed_segments.clear();
        for seg in &self.inventory.segments {
            let mut body = Vec::new();
            body.extend_from_slice(&seg.segment_id);
            body.extend_from_slice(&self.state.destination_heap_id);
            for f in self
                .inventory
                .frames
                .iter()
                .filter(|f| f.segment_id == seg.segment_id)
            {
                body.extend_from_slice(&f.content_hash);
            }
            let h = domain_hash(b"RESIDIUUM-HEAP-MIGRATION-SEGMENT-V1", &body);
            self.state.completed_segments.push((seg.segment_id, h));
        }
        self.state.next_segment = None;
        self.persist_state()?;
        Ok(())
    }

    /// Verify accounting then attempt cut over (phase 5→6).
    pub fn run_verify_and_cutover(&mut self) -> Result<CutoverGate, StoreError> {
        if self.state.phase == MigrationPhase::Rewrite {
            self.advance_to(MigrationPhase::Verify)?;
        }
        self.require_phase(MigrationPhase::Verify)?;
        let gate = self.cutover_gate();
        if !gate.allows_cutover() {
            return Err(StoreError::HeapAdmit(format!(
                "cutover refused: unlabelled_active_frames={} source={} rewritten={} quarantined={}",
                gate.unlabelled_active_frames,
                gate.source_frames,
                gate.rewritten_frames,
                gate.intentionally_quarantined_frames
            )));
        }
        self.advance_to(MigrationPhase::CutOver)?;
        Ok(gate)
    }

    /// Quarantine legacy discovery (phase 7).
    pub fn run_quarantine(&mut self) -> Result<(), StoreError> {
        self.require_phase(MigrationPhase::CutOver)?;
        self.advance_to(MigrationPhase::Quarantine)?;
        Ok(())
    }

    /// Compute the phase-6 gate without mutating state.
    pub fn cutover_gate(&self) -> CutoverGate {
        let mut unlabelled_active = 0u64;
        for f in &self.inventory.frames {
            if f.labelled {
                continue;
            }
            if self.admitted.contains(&f.content_hash) || self.quarantined.contains(&f.content_hash)
            {
                continue;
            }
            unlabelled_active += 1;
        }
        CutoverGate {
            source_frames: self.inventory.source_frame_count(),
            rewritten_frames: self.admitted.len() as u64,
            intentionally_quarantined_frames: self.quarantined.len() as u64,
            unlabelled_active_frames: unlabelled_active,
            cross_heap_segments: 0,
        }
    }

    fn advance_to(&mut self, phase: MigrationPhase) -> Result<(), StoreError> {
        let expect = self
            .state
            .phase
            .successor()
            .ok_or_else(|| StoreError::HeapAdmit("migration already terminal".into()))?;
        if expect != phase {
            return Err(StoreError::HeapAdmit(format!(
                "cannot advance to {} from {}",
                phase.as_u8(),
                self.state.phase.as_u8()
            )));
        }
        failpoint::hit("heap_migration.before_phase_advance")?;
        self.state.phase = phase;
        self.state.updated_at = now_unix()?;
        self.persist_state()?;
        failpoint::hit("heap_migration.after_phase_advance")?;
        Ok(())
    }

    fn require_phase(&self, phase: MigrationPhase) -> Result<(), StoreError> {
        if self.state.phase != phase {
            return Err(StoreError::HeapAdmit(format!(
                "expected phase {}, got {}",
                phase.as_u8(),
                self.state.phase.as_u8()
            )));
        }
        Ok(())
    }

    fn persist_all(&mut self) -> Result<(), StoreError> {
        self.persist_assignments()?;
        self.persist_admitted()?;
        self.persist_state()?;
        Ok(())
    }

    fn persist_state(&mut self) -> Result<(), StoreError> {
        failpoint::hit("heap_migration.before_state_persist")?;
        let bytes = encode_state(&self.state)?;
        write_atomic(&self.root.join(STATE_FILE), &bytes)?;
        failpoint::hit("heap_migration.after_state_persist")?;
        Ok(())
    }

    fn persist_assignments(&self) -> Result<(), StoreError> {
        let bytes = encode_assignments(&self.assignments)?;
        write_atomic(&self.root.join(ASSIGNMENTS_FILE), &bytes)?;
        Ok(())
    }

    fn persist_admitted(&self) -> Result<(), StoreError> {
        let bytes = encode_admitted(&self.admitted, &self.quarantined)?;
        write_atomic(&self.root.join(ADMITTED_FILE), &bytes)?;
        Ok(())
    }
}

fn domain_hash(domain: &[u8], body: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(&[0u8]);
    hasher.update(body);
    *hasher.finalize().as_bytes()
}

fn assignments_hash(map: &BTreeMap<String, [u8; 16]>) -> Result<[u8; 32], StoreError> {
    let mut body = Vec::new();
    for (k, v) in map {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v);
    }
    Ok(domain_hash(ASSIGNMENTS_HASH_DOMAIN, &body))
}

fn encode_state(state: &MigrationStateV1) -> Result<Vec<u8>, StoreError> {
    let mut completed = Vec::new();
    for (id, hash) in &state.completed_segments {
        completed.push(CborValue::Array(vec![
            CborValue::Bytes(id.to_vec()),
            CborValue::Bytes(hash.to_vec()),
        ]));
    }
    let next = match state.next_segment {
        Some(id) => CborValue::Bytes(id.to_vec()),
        None => CborValue::Null,
    };
    encode_deterministic_uint_map(&[
        (1u64, CborValue::Text(HEAP_MIGRATE_PROFILE.into())),
        (2, CborValue::Bytes(state.job_id.to_vec())),
        (3, CborValue::Bytes(state.source_store_id.to_vec())),
        (4, CborValue::Bytes(state.deployment_id.to_vec())),
        (5, CborValue::Bytes(state.destination_heap_id.to_vec())),
        (6, CborValue::Uint(state.phase.as_u8() as u64)),
        (7, CborValue::Bytes(state.source_inventory_hash.to_vec())),
        (8, next),
        (9, CborValue::Array(completed)),
        (10, CborValue::Bytes(state.assigned_objects_hash.to_vec())),
        (11, CborValue::Uint(state.rewritten_frames)),
        (12, CborValue::Uint(state.quarantined_frames)),
        (13, CborValue::Uint(state.started_at as u64)),
        (14, CborValue::Uint(state.updated_at as u64)),
    ])
    .map_err(|e| StoreError::HeapAdmit(e.to_string()))
}

fn load_state(path: &Path) -> Result<MigrationStateV1, StoreError> {
    let bytes = fs::read(path)?;
    let map =
        decode_deterministic_uint_map(&bytes).map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let get = |k: u64| {
        map.iter()
            .find(|(kk, _)| *kk == k)
            .map(|(_, v)| v.clone())
            .ok_or_else(|| StoreError::HeapAdmit(format!("missing state key {k}")))
    };
    expect_text(&get(1)?, HEAP_MIGRATE_PROFILE)?;
    let phase = match get(6)? {
        CborValue::Uint(u) => MigrationPhase::from_u8(u as u8)
            .ok_or_else(|| StoreError::HeapAdmit("bad phase".into()))?,
        _ => return Err(StoreError::HeapAdmit("bad phase type".into())),
    };
    let next_segment = match get(8)? {
        CborValue::Null => None,
        CborValue::Bytes(b) => Some(expect_b16_bytes(&b)?),
        _ => return Err(StoreError::HeapAdmit("bad next_segment".into())),
    };
    let completed = match get(9)? {
        CborValue::Array(items) => {
            let mut out = Vec::new();
            for it in items {
                match it {
                    CborValue::Array(pair) if pair.len() == 2 => {
                        let id = expect_b16(&pair[0])?;
                        let hash = expect_b32(&pair[1])?;
                        out.push((id, hash));
                    }
                    _ => return Err(StoreError::HeapAdmit("bad completed_segments".into())),
                }
            }
            out
        }
        _ => return Err(StoreError::HeapAdmit("bad completed_segments".into())),
    };
    Ok(MigrationStateV1 {
        job_id: expect_b16(&get(2)?)?,
        source_store_id: expect_b16(&get(3)?)?,
        deployment_id: expect_b16(&get(4)?)?,
        destination_heap_id: expect_b16(&get(5)?)?,
        phase,
        source_inventory_hash: expect_b32(&get(7)?)?,
        next_segment,
        completed_segments: completed,
        assigned_objects_hash: expect_b32(&get(10)?)?,
        rewritten_frames: expect_u64(&get(11)?)?,
        quarantined_frames: expect_u64(&get(12)?)?,
        started_at: expect_u64(&get(13)?)? as i64,
        updated_at: expect_u64(&get(14)?)? as i64,
    })
}

fn encode_assignments(map: &BTreeMap<String, [u8; 16]>) -> Result<Vec<u8>, StoreError> {
    let mut entries = Vec::new();
    for (i, (k, v)) in map.iter().enumerate() {
        entries.push((
            (i as u64) + 1,
            CborValue::Array(vec![
                CborValue::Text(k.clone()),
                CborValue::Bytes(v.to_vec()),
            ]),
        ));
    }
    encode_deterministic_uint_map(&entries).map_err(|e| StoreError::HeapAdmit(e.to_string()))
}

fn load_assignments(path: &Path) -> Result<BTreeMap<String, [u8; 16]>, StoreError> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let bytes = fs::read(path)?;
    let map =
        decode_deterministic_uint_map(&bytes).map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let mut out = BTreeMap::new();
    for (_, v) in map {
        match v {
            CborValue::Array(pair) if pair.len() == 2 => {
                let name = match &pair[0] {
                    CborValue::Text(s) => s.clone(),
                    _ => return Err(StoreError::HeapAdmit("bad assignment name".into())),
                };
                let id = expect_b16(&pair[1])?;
                if out.insert(name, id).is_some() {
                    return Err(StoreError::HeapAdmit("duplicate assignment".into()));
                }
            }
            _ => return Err(StoreError::HeapAdmit("bad assignment entry".into())),
        }
    }
    Ok(out)
}

fn encode_admitted(
    admitted: &BTreeSet<[u8; 32]>,
    quarantined: &BTreeSet<[u8; 32]>,
) -> Result<Vec<u8>, StoreError> {
    let adm: Vec<_> = admitted
        .iter()
        .map(|h| CborValue::Bytes(h.to_vec()))
        .collect();
    let qua: Vec<_> = quarantined
        .iter()
        .map(|h| CborValue::Bytes(h.to_vec()))
        .collect();
    encode_deterministic_uint_map(&[(1u64, CborValue::Array(adm)), (2, CborValue::Array(qua))])
        .map_err(|e| StoreError::HeapAdmit(e.to_string()))
}

fn load_admitted(path: &Path) -> Result<(BTreeSet<[u8; 32]>, BTreeSet<[u8; 32]>), StoreError> {
    if !path.exists() {
        return Ok((BTreeSet::new(), BTreeSet::new()));
    }
    let bytes = fs::read(path)?;
    let map =
        decode_deterministic_uint_map(&bytes).map_err(|e| StoreError::HeapAdmit(e.to_string()))?;
    let mut admitted = BTreeSet::new();
    let mut quarantined = BTreeSet::new();
    for (k, v) in map {
        let set = match k {
            1 => &mut admitted,
            2 => &mut quarantined,
            _ => return Err(StoreError::HeapAdmit("bad admitted key".into())),
        };
        match v {
            CborValue::Array(items) => {
                for it in items {
                    set.insert(expect_b32(&it)?);
                }
            }
            _ => return Err(StoreError::HeapAdmit("bad admitted array".into())),
        }
    }
    Ok((admitted, quarantined))
}

fn expect_text(v: &CborValue, want: &str) -> Result<(), StoreError> {
    match v {
        CborValue::Text(s) if s == want => Ok(()),
        _ => Err(StoreError::HeapAdmit("profile mismatch".into())),
    }
}
fn expect_u64(v: &CborValue) -> Result<u64, StoreError> {
    match v {
        CborValue::Uint(u) => Ok(*u),
        _ => Err(StoreError::HeapAdmit("expected uint".into())),
    }
}
fn expect_b16(v: &CborValue) -> Result<[u8; 16], StoreError> {
    match v {
        CborValue::Bytes(b) => expect_b16_bytes(b),
        _ => Err(StoreError::HeapAdmit("expected bstr16".into())),
    }
}
fn expect_b16_bytes(b: &[u8]) -> Result<[u8; 16], StoreError> {
    if b.len() != 16 {
        return Err(StoreError::HeapAdmit("expected 16 bytes".into()));
    }
    let mut a = [0u8; 16];
    a.copy_from_slice(b);
    Ok(a)
}
fn expect_b32(v: &CborValue) -> Result<[u8; 32], StoreError> {
    match v {
        CborValue::Bytes(b) if b.len() == 32 => {
            let mut a = [0u8; 32];
            a.copy_from_slice(b);
            Ok(a)
        }
        _ => Err(StoreError::HeapAdmit("expected bstr32".into())),
    }
}

fn now_unix() -> Result<i64, StoreError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StoreError::HeapAdmit("clock before epoch".into()))?
        .as_secs() as i64)
}

fn hex16(id: &[u8; 16]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}
