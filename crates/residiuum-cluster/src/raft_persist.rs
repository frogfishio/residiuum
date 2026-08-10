//! Durable Raft peer storage (DEF-035 / CLUSTER_SPEC §10.1 persistence).
//!
//! # Layout (per voting replica, per partition)
//!
//! ```text
//! {cluster_root}/raft/node-{n}/p{partition}/
//!   hard_state.json   # term, voted_for, commit_index, last_applied (+ checksum)
//!   membership.json   # voters + placement_epoch (+ checksum)
//!   log.ndjson        # append-only length-prefixed checksummed entries
//!   snapshot.meta.json
//!   snapshot.blob     # optional compacted SM payload (checksummed in meta)
//! ```
//!
//! # Persistence boundaries
//!
//! | Mutation | Must be durable before |
//! |----------|------------------------|
//! | `current_term` / `voted_for` | granting a vote or acknowledging a higher term |
//! | log append / truncate | returning AppendEntries success |
//! | `commit_index` / `last_applied` | advertising commit or applying as durable |
//! | membership | using the new voter set for quorum |
//! | snapshot install | truncating the log past `last_included` |
//!
//! Torn tails and corrupt checksums are truncated/discarded; the peer never
//! fabricates a higher commit than what validated on disk. User payload frames
//! remain in ordinary `residiuum-store` segments and are independently salvageable.

use crate::error::ClusterError;
use crate::id::{NodeId, PartitionId, Term};
use crate::raft::{LogCommand, LogEntry, RaftPeer, RaftRole};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Profile tag for on-disk Raft peer documents (DEF-035).
pub const RAFT_PERSIST_PROFILE: &str = "residiuum-raft-persist-v1";

const HARD_STATE_FILE: &str = "hard_state.json";
const MEMBERSHIP_FILE: &str = "membership.json";
const LOG_FILE: &str = "log.ndjson";
const SNAPSHOT_META_FILE: &str = "snapshot.meta.json";
const SNAPSHOT_BLOB_FILE: &str = "snapshot.blob";

/// Evidence class for a consensus frame after recovery (DEF-035 acceptance).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusEvidenceClass {
    /// Index ≤ durable `commit_index` on this peer.
    Committed,
    /// Present in the durable log but not yet past `commit_index`.
    Prepared,
    /// Same index exists with a conflicting term/body (post-truncation conflict).
    Conflicting,
    /// Index is past the durable log end; commitment cannot be asserted.
    UnknownCommit,
}

/// Durable hard state (Raft §3.2 + applied frontier).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct HardState {
    /// Latest term this peer has observed.
    pub current_term: u64,
    /// Candidate voted for in `current_term` (dense node index), if any.
    pub voted_for: Option<u32>,
    /// Highest log index known committed on this peer.
    pub commit_index: u64,
    /// Highest log index applied to the local state machine on this peer.
    pub last_applied: u64,
}

/// Durable membership configuration for the partition group.
///
/// During rebalance joint consensus (DEF-038 / CLUSTER_SPEC §14), `joint` is
/// true, `voters` is the union of old and new sets, and `outgoing` /
/// `incoming` record the pre-activation configuration for recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipState {
    /// Dense node indexes of voting members (joint union when [`Self::joint`]).
    pub voters: Vec<u32>,
    /// Placement epoch fencing writes for this assignment.
    pub placement_epoch: u64,
    /// True while voters are in joint (old ∪ new) configuration.
    #[serde(default)]
    pub joint: bool,
    /// Pre-rebalance replica set (dense indexes); empty when not joint.
    #[serde(default)]
    pub outgoing: Vec<u32>,
    /// Target replica set after activation; empty when not joint.
    #[serde(default)]
    pub incoming: Vec<u32>,
}

/// Snapshot metadata with integrity over the optional blob and last-included position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMeta {
    /// Last log index included in the snapshot (Raft lastIncludedIndex).
    pub last_included_index: u64,
    /// Term of that index (Raft lastIncludedTerm).
    pub last_included_term: u64,
    /// BLAKE3 hex of `snapshot.blob` (empty string when no blob).
    pub blob_checksum: String,
    /// Byte length of the blob.
    pub blob_len: u64,
    /// Optional human note (e.g. "sm-v1").
    pub note: String,
}

/// Fully loaded snapshot (meta + optional blob bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Checksummed snapshot metadata.
    pub meta: SnapshotMeta,
    /// Compacted state-machine payload (may be empty).
    pub blob: Vec<u8>,
}

/// On-disk store for one Raft peer of one partition.
#[derive(Debug, Clone)]
pub struct RaftPeerStore {
    root: PathBuf,
    node: NodeId,
    partition: PartitionId,
}

impl RaftPeerStore {
    /// Directory for this peer under a cluster root.
    pub fn dir(cluster_root: &Path, node: NodeId, partition: PartitionId) -> PathBuf {
        cluster_root
            .join("raft")
            .join(format!("node-{}", node.index()))
            .join(format!("p{}", partition.get()))
    }

    /// Open or create the peer store directory.
    pub fn open(
        cluster_root: &Path,
        node: NodeId,
        partition: PartitionId,
    ) -> Result<Self, ClusterError> {
        let root = Self::dir(cluster_root, node, partition);
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            node,
            partition,
        })
    }

    /// Store root path.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Node this store belongs to.
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// Partition this store belongs to.
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Load durable hard state (defaults when missing).
    pub fn load_hard_state(&self) -> Result<HardState, ClusterError> {
        match read_checksum_doc::<HardState>(&self.root.join(HARD_STATE_FILE))? {
            Some(hs) => Ok(hs),
            None => Ok(HardState::default()),
        }
    }

    /// Persist hard state **before** granting votes or advertising commit.
    pub fn persist_hard_state(&self, hs: &HardState) -> Result<(), ClusterError> {
        write_checksum_doc(&self.root.join(HARD_STATE_FILE), hs)
    }

    /// Load membership if present.
    pub fn load_membership(&self) -> Result<Option<MembershipState>, ClusterError> {
        read_checksum_doc(&self.root.join(MEMBERSHIP_FILE))
    }

    /// Persist membership configuration.
    pub fn persist_membership(&self, m: &MembershipState) -> Result<(), ClusterError> {
        write_checksum_doc(&self.root.join(MEMBERSHIP_FILE), m)
    }

    /// Load the durable log after applying any valid snapshot base.
    ///
    /// Torn or checksum-invalid tail records are truncated and removed from
    /// the file so subsequent appends stay contiguous.
    pub fn load_log(&self) -> Result<Vec<LogEntry>, ClusterError> {
        let mut entries = Vec::new();
        if let Some(snap) = self.load_snapshot()? {
            // Snapshot supplies a base; trailing log entries start after last_included.
            // We do not re-materialize synthetic log slots for the snapshotted range;
            // the peer's log vector holds only post-snapshot entries, with indices
            // starting at last_included+1.
            let _ = snap;
        }
        let path = self.root.join(LOG_FILE);
        if !path.is_file() {
            return Ok(entries);
        }
        let bytes = fs::read(&path)?;
        let (valid, consumed) = decode_log_records(&bytes);
        entries = valid;
        // Truncate torn / corrupt tail on disk.
        if consumed < bytes.len() {
            let f = OpenOptions::new().write(true).open(&path)?;
            f.set_len(consumed as u64)?;
            f.sync_all()?;
            sync_parent_dir(&path)?;
        }
        Ok(entries)
    }

    /// Replace the entire durable log with `entries` (atomic rewrite).
    ///
    /// Used after conflict truncation and after snapshot install.
    pub fn rewrite_log(&self, entries: &[LogEntry]) -> Result<(), ClusterError> {
        let path = self.root.join(LOG_FILE);
        let mut buf = Vec::new();
        for e in entries {
            encode_log_record(e, &mut buf)?;
        }
        residiuum_store::write_atomic(&path, &buf)?;
        Ok(())
    }

    /// Append entries that are contiguous after the current durable log end.
    ///
    /// Each record is length-prefixed and checksummed; the file is `sync_all`'d
    /// before return (persist-before-ack).
    pub fn append_log(&self, entries: &[LogEntry]) -> Result<(), ClusterError> {
        if entries.is_empty() {
            return Ok(());
        }
        let path = self.root.join(LOG_FILE);
        let mut f = OpenOptions::new().create(true).append(true).open(&path)?;
        for e in entries {
            let mut rec = Vec::new();
            encode_log_record(e, &mut rec)?;
            f.write_all(&rec)?;
        }
        f.sync_all()?;
        sync_parent_dir(&path)?;
        Ok(())
    }

    /// Load snapshot if meta+blob validate.
    pub fn load_snapshot(&self) -> Result<Option<Snapshot>, ClusterError> {
        let meta_path = self.root.join(SNAPSHOT_META_FILE);
        let Some(meta) = read_checksum_doc::<SnapshotMeta>(&meta_path)? else {
            return Ok(None);
        };
        let blob_path = self.root.join(SNAPSHOT_BLOB_FILE);
        let blob = if meta.blob_len == 0 {
            Vec::new()
        } else if blob_path.is_file() {
            let b = fs::read(&blob_path)?;
            if b.len() as u64 != meta.blob_len {
                // Corrupt / torn blob — discard snapshot rather than invent state.
                return Ok(None);
            }
            let sum = blake3::hash(&b);
            if hex32(sum.as_bytes()) != meta.blob_checksum {
                return Ok(None);
            }
            b
        } else {
            return Ok(None);
        };
        Ok(Some(Snapshot { meta, blob }))
    }

    /// Atomically install a snapshot, then rewrite the log to retain only
    /// entries with `index > last_included_index`.
    pub fn install_snapshot(
        &self,
        meta: SnapshotMeta,
        blob: &[u8],
        remaining_log: &[LogEntry],
    ) -> Result<(), ClusterError> {
        // Validate caller-supplied checksums.
        if blob.len() as u64 != meta.blob_len {
            return Err(ClusterError::CorruptMeta("snapshot blob_len mismatch"));
        }
        let sum = blake3::hash(blob);
        if hex32(sum.as_bytes()) != meta.blob_checksum {
            return Err(ClusterError::CorruptMeta("snapshot blob checksum mismatch"));
        }

        // Write blob to temp then rename (same dir for atomicity).
        let blob_path = self.root.join(SNAPSHOT_BLOB_FILE);
        residiuum_store::write_atomic(&blob_path, blob)?;
        write_checksum_doc(&self.root.join(SNAPSHOT_META_FILE), &meta)?;
        self.rewrite_log(remaining_log)?;
        Ok(())
    }

    /// Build a [`RaftPeer`] from durable state (always starts as Follower).
    ///
    /// Volatile leader fields (`next_index`, `match_index`) are empty until election.
    pub fn load_peer(&self) -> Result<RaftPeer, ClusterError> {
        let hs = self.load_hard_state()?;
        let mut log = self.load_log()?;
        let snap = self.load_snapshot()?;

        if let Some(ref s) = snap {
            let base = s.meta.last_included_index;
            log.retain(|e| e.index > base);
            // Drop non-contiguous prefix after snapshot base (never invent gaps).
            while !log.is_empty() {
                let first = log[0].index;
                if first == base + 1 {
                    break;
                }
                if first > base + 1 {
                    log.clear();
                    break;
                }
                log.remove(0);
            }
            // Placeholder so last_log_index/term match lastIncluded after compact.
            if log.is_empty() && base > 0 {
                log.push(LogEntry {
                    term: Term(s.meta.last_included_term),
                    index: base,
                    command: LogCommand::Delete {
                        subject: "__residiuum_snapshot_base__".into(),
                    },
                });
            }
        }

        let max_log = log
            .last()
            .map(|e| e.index)
            .or_else(|| snap.as_ref().map(|s| s.meta.last_included_index))
            .unwrap_or(0);
        let commit_index = hs.commit_index.min(max_log);
        let last_applied = hs.last_applied.min(commit_index);

        Ok(RaftPeer {
            node_id: self.node,
            current_term: Term(hs.current_term),
            voted_for: hs.voted_for.map(NodeId::new),
            role: RaftRole::Follower,
            log,
            commit_index,
            last_applied,
            next_index: Default::default(),
            match_index: Default::default(),
        })
    }

    /// Persist a full peer hard state + log rewrite (crash-safe checkpoint).
    pub fn save_peer(&self, peer: &RaftPeer) -> Result<(), ClusterError> {
        let hs = HardState {
            current_term: peer.current_term.0,
            voted_for: peer.voted_for.map(|n| n.index()),
            commit_index: peer.commit_index,
            last_applied: peer.last_applied,
        };
        // Log first would allow orphan entries; hard state commit ≤ log is safer
        // when rewritten together: write log, then hard state.
        // Filter snapshot-base placeholder for disk (real entries only when
        // snapshot meta already records the base).
        let snap = self.load_snapshot()?;
        let entries: Vec<LogEntry> = peer
            .log
            .iter()
            .filter(|e| {
                if let Some(ref s) = snap {
                    e.index > s.meta.last_included_index
                } else {
                    e.command.subject() != "__residiuum_snapshot_base__"
                }
            })
            .cloned()
            .collect();
        self.rewrite_log(&entries)?;
        self.persist_hard_state(&hs)?;
        Ok(())
    }

    /// Classify durable evidence for `index` on this peer.
    pub fn evidence_class(&self, index: u64) -> Result<ConsensusEvidenceClass, ClusterError> {
        let hs = self.load_hard_state()?;
        let log = self.load_log()?;
        let snap = self.load_snapshot()?;
        let max_idx = log
            .last()
            .map(|e| e.index)
            .or_else(|| snap.as_ref().map(|s| s.meta.last_included_index))
            .unwrap_or(0);
        if index == 0 {
            return Ok(ConsensusEvidenceClass::Committed);
        }
        if index > max_idx {
            return Ok(ConsensusEvidenceClass::UnknownCommit);
        }
        if index <= hs.commit_index.min(max_idx) {
            return Ok(ConsensusEvidenceClass::Committed);
        }
        if log.iter().any(|e| e.index == index)
            || snap
                .as_ref()
                .map(|s| index <= s.meta.last_included_index)
                .unwrap_or(false)
        {
            return Ok(ConsensusEvidenceClass::Prepared);
        }
        Ok(ConsensusEvidenceClass::UnknownCommit)
    }
}

/// Build a snapshot meta for `last_included` covering `blob`.
pub fn snapshot_meta_for(
    last_included_index: u64,
    last_included_term: Term,
    blob: &[u8],
    note: impl Into<String>,
) -> SnapshotMeta {
    let sum = blake3::hash(blob);
    SnapshotMeta {
        last_included_index,
        last_included_term: last_included_term.0,
        blob_checksum: hex32(sum.as_bytes()),
        blob_len: blob.len() as u64,
        note: note.into(),
    }
}

// --- encoding helpers -------------------------------------------------------

fn write_checksum_doc<T: Serialize>(path: &Path, body: &T) -> Result<(), ClusterError> {
    // Canonical body: Value form so write/read hashing agrees.
    let body_val = serde_json::to_value(body)
        .map_err(|_| ClusterError::CorruptMeta("serialize raft control doc"))?;
    let body_bytes = serde_json::to_vec(&body_val)
        .map_err(|_| ClusterError::CorruptMeta("canonicalize raft control doc"))?;
    let sum = blake3::hash(&body_bytes);
    let envelope = serde_json::json!({
        "format": RAFT_PERSIST_PROFILE,
        "checksum": hex32(sum.as_bytes()),
        "body": body_val,
    });
    let bytes = serde_json::to_vec_pretty(&envelope)
        .map_err(|_| ClusterError::CorruptMeta("serialize raft control envelope"))?;
    residiuum_store::write_atomic_keep_previous(path, &bytes)?;
    Ok(())
}

fn read_checksum_doc<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Option<T>, ClusterError> {
    // Prefer primary; fall back to previous generation if primary fails checksum.
    if path.is_file() {
        if let Ok(bytes) = fs::read(path) {
            if let Ok(Some(t)) = parse_checksum_doc::<T>(&bytes) {
                return Ok(Some(t));
            }
        }
    }
    let prev = residiuum_store::previous_path(path);
    if prev.is_file() {
        if let Ok(bytes) = fs::read(&prev) {
            if let Ok(Some(t)) = parse_checksum_doc::<T>(&bytes) {
                return Ok(Some(t));
            }
        }
    }
    Ok(None)
}

fn parse_checksum_doc<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
) -> Result<Option<T>, ClusterError> {
    let v: serde_json::Value = match serde_json::from_slice(bytes) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let format = v.get("format").and_then(|x| x.as_str()).unwrap_or("");
    if format != RAFT_PERSIST_PROFILE {
        return Ok(None);
    }
    let checksum = v.get("checksum").and_then(|x| x.as_str()).unwrap_or("");
    let body_val = match v.get("body") {
        Some(b) => b.clone(),
        None => return Ok(None),
    };
    let body_bytes = match serde_json::to_vec(&body_val) {
        Ok(b) => b,
        Err(_) => return Ok(None),
    };
    let sum = blake3::hash(&body_bytes);
    if hex32(sum.as_bytes()) != checksum {
        return Ok(None);
    }
    match serde_json::from_value::<T>(body_val) {
        Ok(t) => Ok(Some(t)),
        Err(_) => Ok(None),
    }
}

/// Record: u32le length | blake3-32 | payload-json
fn encode_log_record(entry: &LogEntry, out: &mut Vec<u8>) -> Result<(), ClusterError> {
    let payload =
        serde_json::to_vec(entry).map_err(|_| ClusterError::CorruptMeta("serialize log entry"))?;
    let sum = blake3::hash(&payload);
    let len = (32 + payload.len()) as u32;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(sum.as_bytes());
    out.extend_from_slice(&payload);
    Ok(())
}

/// Decode contiguous valid records; return (entries, bytes_consumed).
fn decode_log_records(bytes: &[u8]) -> (Vec<LogEntry>, usize) {
    let mut entries: Vec<LogEntry> = Vec::new();
    let mut off = 0usize;
    while off + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
        let rec_start = off + 4;
        let rec_end = rec_start + len;
        if len < 32 || rec_end > bytes.len() {
            // Torn tail.
            break;
        }
        let sum_bytes = &bytes[rec_start..rec_start + 32];
        let payload = &bytes[rec_start + 32..rec_end];
        let got = blake3::hash(payload);
        if got.as_bytes() != sum_bytes {
            // Corrupt record — stop; do not skip (could desync indices).
            break;
        }
        match serde_json::from_slice::<LogEntry>(payload) {
            Ok(e) => {
                // Contiguity: if we already have entries, next must be last+1
                if let Some(last) = entries.last() {
                    if e.index != last.index + 1 {
                        break;
                    }
                }
                entries.push(e);
                off = rec_end;
            }
            Err(_) => break,
        }
    }
    (entries, off)
}

fn hex32(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

fn sync_parent_dir(path: &Path) -> Result<(), ClusterError> {
    if let Some(parent) = path.parent() {
        #[cfg(unix)]
        {
            use std::fs::File;
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }
        #[cfg(not(unix))]
        {
            let _ = parent;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::LogCommand;

    fn tmp() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn hard_state_roundtrip_and_checksum() {
        let dir = tmp();
        let store = RaftPeerStore::open(dir.path(), NodeId::new(0), PartitionId::new(1)).unwrap();
        let hs = HardState {
            current_term: 7,
            voted_for: Some(2),
            commit_index: 3,
            last_applied: 2,
        };
        store.persist_hard_state(&hs).unwrap();
        assert_eq!(store.load_hard_state().unwrap(), hs);

        // Second write creates hard_state.json.prev via keep_previous.
        let hs2 = HardState {
            current_term: 8,
            voted_for: Some(2),
            commit_index: 4,
            last_applied: 3,
        };
        store.persist_hard_state(&hs2).unwrap();
        assert_eq!(store.load_hard_state().unwrap().current_term, 8);

        // Corrupt primary → fall back to previous generation (term 7), never invent.
        let path = store.root().join(HARD_STATE_FILE);
        fs::write(&path, b"{not json").unwrap();
        let loaded = store.load_hard_state().unwrap();
        assert_eq!(loaded.current_term, 7);
        assert_eq!(loaded, hs);
    }

    #[test]
    fn log_append_and_torn_tail_truncation() {
        let dir = tmp();
        let store = RaftPeerStore::open(dir.path(), NodeId::new(0), PartitionId::new(0)).unwrap();
        let e1 = LogEntry {
            term: Term(1),
            index: 1,
            command: LogCommand::Put {
                subject: "a".into(),
                value: b"1".to_vec(),
            },
        };
        let e2 = LogEntry {
            term: Term(1),
            index: 2,
            command: LogCommand::Delete {
                subject: "a".into(),
            },
        };
        store.append_log(&[e1.clone(), e2.clone()]).unwrap();
        assert_eq!(store.load_log().unwrap(), vec![e1.clone(), e2.clone()]);

        // Append garbage as a torn record.
        let path = store.root().join(LOG_FILE);
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0x04, 0, 0, 0, 0xde, 0xad]).unwrap(); // short record
            f.sync_all().unwrap();
        }
        let loaded = store.load_log().unwrap();
        assert_eq!(loaded, vec![e1, e2]);
        // Second load should see clean file.
        assert_eq!(store.load_log().unwrap().len(), 2);
    }

    #[test]
    fn snapshot_install_truncates_log() {
        let dir = tmp();
        let store = RaftPeerStore::open(dir.path(), NodeId::new(1), PartitionId::new(3)).unwrap();
        let entries: Vec<LogEntry> = (1..=5)
            .map(|i| LogEntry {
                term: Term(1),
                index: i,
                command: LogCommand::Put {
                    subject: format!("k{i}"),
                    value: vec![i as u8],
                },
            })
            .collect();
        store.rewrite_log(&entries).unwrap();
        let blob = br#"{"sm":"v1"}"#;
        let meta = snapshot_meta_for(3, Term(1), blob, "test");
        let remaining: Vec<LogEntry> = entries.into_iter().filter(|e| e.index > 3).collect();
        store
            .install_snapshot(meta.clone(), blob, &remaining)
            .unwrap();
        let snap = store.load_snapshot().unwrap().expect("snap");
        assert_eq!(snap.meta, meta);
        assert_eq!(snap.blob, blob);
        let log = store.load_log().unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].index, 4);
    }

    #[test]
    fn evidence_classes() {
        let dir = tmp();
        let store = RaftPeerStore::open(dir.path(), NodeId::new(0), PartitionId::new(0)).unwrap();
        store
            .append_log(&[LogEntry {
                term: Term(1),
                index: 1,
                command: LogCommand::Put {
                    subject: "x".into(),
                    value: b"y".to_vec(),
                },
            }])
            .unwrap();
        store
            .persist_hard_state(&HardState {
                current_term: 1,
                voted_for: Some(0),
                commit_index: 1,
                last_applied: 1,
            })
            .unwrap();
        store
            .append_log(&[LogEntry {
                term: Term(1),
                index: 2,
                command: LogCommand::Put {
                    subject: "x".into(),
                    value: b"z".to_vec(),
                },
            }])
            .unwrap();
        assert_eq!(
            store.evidence_class(1).unwrap(),
            ConsensusEvidenceClass::Committed
        );
        assert_eq!(
            store.evidence_class(2).unwrap(),
            ConsensusEvidenceClass::Prepared
        );
        assert_eq!(
            store.evidence_class(9).unwrap(),
            ConsensusEvidenceClass::UnknownCommit
        );
    }

    #[test]
    fn save_load_peer_roundtrip() {
        let dir = tmp();
        let store = RaftPeerStore::open(dir.path(), NodeId::new(2), PartitionId::new(5)).unwrap();
        let peer = RaftPeer {
            node_id: NodeId::new(2),
            current_term: Term(4),
            voted_for: Some(NodeId::new(2)),
            role: RaftRole::Leader, // should load as Follower
            log: vec![LogEntry {
                term: Term(4),
                index: 1,
                command: LogCommand::Delete {
                    subject: "t".into(),
                },
            }],
            commit_index: 1,
            last_applied: 1,
            next_index: Default::default(),
            match_index: Default::default(),
        };
        store.save_peer(&peer).unwrap();
        let loaded = store.load_peer().unwrap();
        assert_eq!(loaded.current_term, Term(4));
        assert_eq!(loaded.voted_for, Some(NodeId::new(2)));
        assert_eq!(loaded.role, RaftRole::Follower);
        assert_eq!(loaded.log.len(), 1);
        assert_eq!(loaded.commit_index, 1);
        // Role is volatile — leader never survives restart as leader.
        assert_eq!(peer.role, RaftRole::Leader);
    }
}
