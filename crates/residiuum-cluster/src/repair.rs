//! Anti-entropy inventory and replica repair (CLUSTER_SPEC §15.3, DEF-039).
//!
//! Replicas exchange verified subject inventories (content hash + consensus
//! frontier). Repair source selection uses integrity and majority/consensus
//! evidence — **never** filesystem mtime or catalog generation alone.
//!
//! Corrupt or divergent live values are overwritten only when a healthier
//! source is selected by policy. Irrecoverable subjects become explicit holes
//! in the repair report (never fabricated from wall-clock order).

use crate::error::ClusterError;
use crate::id::{LogPosition, NodeId, PartitionId};
use blake3::Hasher;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Profile tag for anti-entropy / repair documents (DEF-039).
pub const ANTI_ENTROPY_PROFILE: &str = "residiuum-anti-entropy-v1";

/// Filename under the cluster root for durable repair audit log.
pub const REPAIR_AUDIT_FILE: &str = "repair_audit.json";

/// Classification of one replica's view of a subject (or absence).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReplicaObservation {
    /// Live body present and fully readable; content hash recorded.
    Healthy,
    /// Subject absent from this replica's live index.
    Missing,
    /// Live key present but payload unreadable / partial / conflicting chunks.
    Corrupt,
    /// Live body readable but disagrees with the selected repair target.
    Divergent,
}

impl ReplicaObservation {
    /// Stable name for operators and tests.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Missing => "missing",
            Self::Corrupt => "corrupt",
            Self::Divergent => "divergent",
        }
    }
}

/// Per-node observation for one subject during inventory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSubjectView {
    /// Replica node.
    pub node: NodeId,
    /// Observation class.
    pub observation: ReplicaObservation,
    /// Hex blake3 of live body when readable; empty otherwise.
    #[serde(default)]
    pub content_hash_hex: String,
    /// Body bytes when readable (not serialized to audit by default path).
    #[serde(skip)]
    pub body: Option<Vec<u8>>,
}

/// Hierarchical inventory unit for one subject across a partition's replicas.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubjectInventory {
    /// Subject key.
    pub subject: String,
    /// Owning partition.
    pub partition: PartitionId,
    /// Per-replica views (online replicas only).
    pub views: Vec<NodeSubjectView>,
    /// Selected repair target hash (hex), if majority/consensus evidence exists.
    #[serde(default)]
    pub target_hash_hex: Option<String>,
    /// Node chosen as repair source when a target exists.
    #[serde(default)]
    pub source_node: Option<NodeId>,
    /// Explicit conflict: no unique majority / consensus target.
    #[serde(default)]
    pub conflicting: bool,
    /// No healthy copy remains among online replicas.
    #[serde(default)]
    pub irrecoverable: bool,
}

/// Partition-level inventory: log frontier + subject digests + segment fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionInventory {
    /// Partition id.
    pub partition: PartitionId,
    /// Configured replica set from placement.
    pub replicas: Vec<NodeId>,
    /// Online replicas that contributed inventory.
    pub online_replicas: Vec<NodeId>,
    /// Raft commit frontier (max across online peers) when available.
    pub log_frontier: LogPosition,
    /// Leader at inventory time, if any.
    #[serde(default)]
    pub leader: Option<NodeId>,
    /// Segment content fingerprints per online node (hex blake3 of store segments).
    #[serde(default)]
    pub segment_fingerprints: Vec<(NodeId, String)>,
    /// Subject-level inventory entries.
    pub subjects: Vec<SubjectInventory>,
    /// Count of subjects needing repair (missing/divergent/corrupt vs target).
    pub needs_repair: u64,
    /// Count of irrecoverable subjects (no healthy source).
    pub irrecoverable: u64,
    /// Count of explicit content conflicts (no majority).
    pub conflicts: u64,
}

/// Cluster-wide inventory summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ClusterInventory {
    /// Per-partition inventories.
    pub partitions: Vec<PartitionInventory>,
    /// Sum of subjects needing repair.
    pub needs_repair: u64,
    /// Sum of irrecoverable holes.
    pub irrecoverable: u64,
    /// Sum of conflicts.
    pub conflicts: u64,
}

/// Kind of repair action recorded in the audit trail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepairActionKind {
    /// Copied verified body from source to a lagging / corrupt replica.
    Copied,
    /// Recorded corrupt observation; did not use it as a source.
    QuarantinedCorrupt,
    /// Preserved explicit multi-variant conflict without inventing a winner.
    PreservedConflict,
    /// No healthy source; hole remains explicit.
    IrrecoverableHole,
    /// Destination already matched target; no write.
    AlreadyMatched,
}

impl RepairActionKind {
    /// Stable name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Copied => "copied",
            Self::QuarantinedCorrupt => "quarantined-corrupt",
            Self::PreservedConflict => "preserved-conflict",
            Self::IrrecoverableHole => "irrecoverable-hole",
            Self::AlreadyMatched => "already-matched",
        }
    }
}

/// One audited repair decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairAuditEntry {
    /// Opaque sequence id within an audit file generation.
    pub seq: u64,
    /// Subject repaired (or examined).
    pub subject: String,
    /// Partition.
    pub partition: PartitionId,
    /// Action taken.
    pub action: RepairActionKind,
    /// Source node when a copy was performed.
    #[serde(default)]
    pub source: Option<NodeId>,
    /// Destination node when a copy was performed.
    #[serde(default)]
    pub destination: Option<NodeId>,
    /// Target content hash (hex) when known.
    #[serde(default)]
    pub content_hash_hex: String,
    /// Human-readable reason (integrity / majority / conflict / hole).
    #[serde(default)]
    pub reason: String,
}

/// Outcome of a repair pass.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepairReport {
    /// Subjects successfully copied onto at least one lagging replica.
    pub subjects_repaired: u64,
    /// Individual put operations performed.
    pub copies_written: u64,
    /// Corrupt observations recorded (not used as sources).
    pub corrupt_quarantined: u64,
    /// Conflicts preserved without a silent winner.
    pub conflicts_preserved: u64,
    /// Explicit irrecoverable holes.
    pub irrecoverable_holes: u64,
    /// Subjects skipped because rate budget exhausted.
    pub budget_remaining_subjects: u64,
    /// True when rate limit stopped the pass early.
    pub budget_exhausted: bool,
    /// Audit entries produced this pass.
    pub audit: Vec<RepairAuditEntry>,
    /// Inventory snapshot that drove the pass (optional summary).
    pub needs_repair_before: u64,
}

/// Options for a bounded repair pass (DEF-039 rate limit / foreground isolation).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepairOptions {
    /// Maximum distinct subjects to repair (copy) in this pass.
    pub max_subjects: Option<usize>,
    /// Maximum total body bytes to write in this pass.
    pub max_bytes: Option<u64>,
    /// When true, only inventory / plan — do not write.
    pub dry_run: bool,
}

impl RepairOptions {
    /// Unlimited repair (still isolated from the put path; operator-invoked).
    pub fn unlimited() -> Self {
        Self::default()
    }

    /// Cap subjects repaired this pass.
    pub fn max_subjects(mut self, n: usize) -> Self {
        self.max_subjects = Some(n);
        self
    }

    /// Cap body bytes written this pass.
    pub fn max_bytes(mut self, n: u64) -> Self {
        self.max_bytes = Some(n);
        self
    }

    /// Inventory-only.
    pub fn dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }
}

/// Durable append-only style audit document (rewritten with generation bump).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairAuditFile {
    /// Format / profile tag.
    #[serde(default = "default_repair_format")]
    pub format: String,
    /// Monotonic generation.
    #[serde(default)]
    pub generation: u64,
    /// BLAKE3-256 hex of the canonical entry list.
    #[serde(default)]
    pub content_blake3: String,
    /// Appended audit entries (capped by operators via rotation later).
    #[serde(default)]
    pub entries: Vec<RepairAuditEntry>,
}

fn default_repair_format() -> String {
    ANTI_ENTROPY_PROFILE.into()
}

impl RepairAuditFile {
    /// Empty document.
    pub fn new() -> Self {
        Self {
            format: default_repair_format(),
            generation: 0,
            content_blake3: String::new(),
            entries: Vec::new(),
        }
    }

    /// Recompute checksum over entries.
    pub fn refresh_checksum(&mut self) {
        self.content_blake3 = entries_content_hash(&self.entries);
    }

    /// Validate format + checksum when present.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.format != ANTI_ENTROPY_PROFILE && !self.format.is_empty() {
            return Err(ClusterError::CorruptMeta("unsupported repair_audit format"));
        }
        if !self.content_blake3.is_empty() {
            let expect = entries_content_hash(&self.entries);
            if self.content_blake3 != expect {
                return Err(ClusterError::CorruptMeta(
                    "repair_audit.json content_blake3 mismatch",
                ));
            }
        }
        Ok(())
    }

    /// Load from cluster root, or empty if missing. Falls back to `.prev`.
    pub fn load(root: &Path) -> Result<Self, ClusterError> {
        let path = root.join(REPAIR_AUDIT_FILE);
        if let Some(file) = try_parse_audit(&path)? {
            return Ok(file);
        }
        let prev = residiuum_store::previous_path(&path);
        if let Some(file) = try_parse_audit(&prev)? {
            return Ok(file);
        }
        if path.is_file() || prev.is_file() {
            return Err(ClusterError::CorruptMeta(
                "repair_audit.json unreadable; restore .prev or clear repair audit",
            ));
        }
        Ok(Self::new())
    }

    /// Persist under the cluster root (atomic durable; keeps previous generation).
    pub fn save(&self, root: &Path) -> Result<(), ClusterError> {
        let path = root.join(REPAIR_AUDIT_FILE);
        let mut out = self.clone();
        out.generation = self.generation.saturating_add(1).max(1);
        out.refresh_checksum();
        let json = serde_json::to_string_pretty(&out)
            .map_err(|_| ClusterError::CorruptMeta("serialize repair_audit.json"))?;
        residiuum_store::write_atomic_keep_previous(&path, json.as_bytes())?;
        Ok(())
    }

    /// Append entries and save.
    pub fn append_and_save(
        root: &Path,
        new_entries: &[RepairAuditEntry],
    ) -> Result<Self, ClusterError> {
        let mut file = Self::load(root)?;
        let mut next_seq = file
            .entries
            .last()
            .map(|e| e.seq.saturating_add(1))
            .unwrap_or(1);
        for e in new_entries {
            let mut e = e.clone();
            e.seq = next_seq;
            next_seq = next_seq.saturating_add(1);
            file.entries.push(e);
        }
        // Soft cap to keep the document bounded in long soaks.
        const MAX_ENTRIES: usize = 10_000;
        if file.entries.len() > MAX_ENTRIES {
            let drop = file.entries.len() - MAX_ENTRIES;
            file.entries.drain(0..drop);
        }
        file.save(root)?;
        Ok(file)
    }
}

impl Default for RepairAuditFile {
    fn default() -> Self {
        Self::new()
    }
}

fn entries_content_hash(entries: &[RepairAuditEntry]) -> String {
    let mut h = Hasher::new();
    for e in entries {
        let bytes = serde_json::to_vec(e).unwrap_or_default();
        h.update(&(bytes.len() as u64).to_le_bytes());
        h.update(&bytes);
    }
    h.finalize().to_hex().to_string()
}

fn try_parse_audit(path: &Path) -> Result<Option<RepairAuditFile>, ClusterError> {
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(_) => return Ok(None),
    };
    let file: RepairAuditFile = match serde_json::from_slice(&bytes) {
        Ok(f) => f,
        Err(_) => return Ok(None),
    };
    if file.validate().is_err() {
        return Ok(None);
    }
    Ok(Some(file))
}

/// Hex-encode a 32-byte blake3 digest.
pub fn hash_hex(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Parse hex blake3; returns None if not 64 hex chars.
pub fn parse_hash_hex(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        let byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
        out[i] = byte;
    }
    Some(out)
}

/// Select repair target from per-node views using integrity + majority.
///
/// Policy (CLUSTER_SPEC §15.3):
/// - Only **Healthy** observations may be sources (readable full body).
/// - **Never** consult mtime / wall clock.
/// - Prefer the content hash held by a strict majority of online replicas that
///   have a Healthy or Divergent-class readable body; Corrupt never votes.
/// - On a tie / multi-way split, mark conflicting and do not invent a winner.
/// - Prefer a source that is the current leader when the leader holds the
///   majority hash; otherwise any healthy holder of the target hash.
///
/// Returns `(target_hash, source_node)` when a unique majority target exists.
pub fn select_repair_source(
    views: &[NodeSubjectView],
    leader: Option<NodeId>,
) -> Result<([u8; 32], NodeId), SourceSelectError> {
    // Count votes by content hash among readable (healthy) bodies only.
    // Divergent is assigned after target selection; at inventory time all
    // readable views are Healthy until compared to the target.
    let mut tallies: Vec<([u8; 32], u32, Vec<NodeId>)> = Vec::new();
    let mut corrupt_only = true;
    let mut any_readable = false;

    for v in views {
        match v.observation {
            ReplicaObservation::Corrupt => {}
            ReplicaObservation::Missing => {
                corrupt_only = false;
            }
            ReplicaObservation::Healthy | ReplicaObservation::Divergent => {
                corrupt_only = false;
                any_readable = true;
                let Some(h) = parse_hash_hex(&v.content_hash_hex) else {
                    continue;
                };
                if let Some(slot) = tallies.iter_mut().find(|(hh, _, _)| *hh == h) {
                    slot.1 += 1;
                    slot.2.push(v.node);
                } else {
                    tallies.push((h, 1, vec![v.node]));
                }
            }
        }
    }

    if !any_readable {
        if corrupt_only
            && views
                .iter()
                .any(|v| v.observation == ReplicaObservation::Corrupt)
        {
            return Err(SourceSelectError::Irrecoverable);
        }
        // All missing — nothing to repair for this subject.
        return Err(SourceSelectError::AllMissing);
    }

    if tallies.is_empty() {
        return Err(SourceSelectError::Irrecoverable);
    }

    tallies.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let best_count = tallies[0].1;
    let winners: Vec<_> = tallies.iter().filter(|t| t.1 == best_count).collect();
    if winners.len() > 1 {
        return Err(SourceSelectError::Conflict);
    }

    let (hash, _count, holders) = winners[0];
    // Prefer leader if it holds the winning hash.
    if let Some(l) = leader {
        if holders.contains(&l) {
            return Ok((*hash, l));
        }
    }
    // Stable: lowest node index among holders.
    let mut holders = holders.clone();
    holders.sort();
    Ok((*hash, holders[0]))
}

/// Why source selection did not produce a unique target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceSelectError {
    /// No readable healthy body remains.
    Irrecoverable,
    /// Subject missing everywhere (not a hole from corruption — simply absent).
    AllMissing,
    /// Multiple content hashes with equal top vote — preserve conflict.
    Conflict,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(node: u32, obs: ReplicaObservation, body: Option<&[u8]>) -> NodeSubjectView {
        let (hex, body_opt) = match body {
            Some(b) => {
                let h = *blake3::hash(b).as_bytes();
                (hash_hex(&h), Some(b.to_vec()))
            }
            None => (String::new(), None),
        };
        NodeSubjectView {
            node: NodeId::new(node),
            observation: obs,
            content_hash_hex: hex,
            body: body_opt,
        }
    }

    #[test]
    fn majority_beats_lone_corrupt_newer_body() {
        // Two healthy "good", one readable "evil" (simulates corrupt-but-readable
        // newer mtime replica). Majority wins; evil is never preferred.
        let views = vec![
            view(0, ReplicaObservation::Healthy, Some(b"good")),
            view(1, ReplicaObservation::Healthy, Some(b"good")),
            view(2, ReplicaObservation::Healthy, Some(b"evil-newer")),
        ];
        let (h, src) = select_repair_source(&views, Some(NodeId::new(2))).unwrap();
        assert_eq!(hash_hex(&h), hash_hex(blake3::hash(b"good").as_bytes()));
        // Leader holds evil only — source must still be a majority holder.
        assert!(src == NodeId::new(0) || src == NodeId::new(1));
    }

    #[test]
    fn never_use_corrupt_as_source() {
        let views = vec![
            view(0, ReplicaObservation::Corrupt, None),
            view(1, ReplicaObservation::Healthy, Some(b"ok")),
            view(2, ReplicaObservation::Missing, None),
        ];
        let (h, src) = select_repair_source(&views, None).unwrap();
        assert_eq!(src, NodeId::new(1));
        assert_eq!(hash_hex(&h), hash_hex(blake3::hash(b"ok").as_bytes()));
    }

    #[test]
    fn equal_split_is_conflict() {
        let views = vec![
            view(0, ReplicaObservation::Healthy, Some(b"a")),
            view(1, ReplicaObservation::Healthy, Some(b"b")),
        ];
        assert_eq!(
            select_repair_source(&views, None),
            Err(SourceSelectError::Conflict)
        );
    }

    #[test]
    fn all_corrupt_is_irrecoverable() {
        let views = vec![
            view(0, ReplicaObservation::Corrupt, None),
            view(1, ReplicaObservation::Corrupt, None),
        ];
        assert_eq!(
            select_repair_source(&views, None),
            Err(SourceSelectError::Irrecoverable)
        );
    }

    #[test]
    fn leader_preferred_when_on_majority() {
        let views = vec![
            view(0, ReplicaObservation::Healthy, Some(b"good")),
            view(1, ReplicaObservation::Healthy, Some(b"good")),
            view(2, ReplicaObservation::Missing, None),
        ];
        let (_, src) = select_repair_source(&views, Some(NodeId::new(1))).unwrap();
        assert_eq!(src, NodeId::new(1));
    }

    #[test]
    fn audit_file_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let entry = RepairAuditEntry {
            seq: 0,
            subject: "s".into(),
            partition: PartitionId::new(1),
            action: RepairActionKind::Copied,
            source: Some(NodeId::new(0)),
            destination: Some(NodeId::new(1)),
            content_hash_hex: "ab".into(),
            reason: "majority".into(),
        };
        RepairAuditFile::append_and_save(dir.path(), &[entry]).unwrap();
        let loaded = RepairAuditFile::load(dir.path()).unwrap();
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(loaded.entries[0].seq, 1);
        assert_eq!(loaded.entries[0].action, RepairActionKind::Copied);
        assert!(loaded.generation >= 1);
    }
}
