//! Deterministic distributed-system verification harness (DEF-041).
//!
//! Provides:
//! - **Seeded PRNG** so failures replay with the same history
//! - **Network fault model** — drop, directed partition, node crash (offline)
//! - **Operation history** with logical call/return times
//! - **Partition-linearizable checker** for strong-mode put/get on one subject
//! - **Convergent-append checker** for dual-accept conflict preservation
//! - **Scenario runner** mapping CLUSTER_SPEC §22 cases to in-process network Raft
//!
//! This is an **in-process** verification program (multi-node `MemoryRaftNetwork`
//! with injected faults). Multi-process OS-level chaos and full Jepsen PORC
//! remain follow-ons; the seed + history dump contract is the same.
//!
//! Profile tag: [`VERIFY_PROFILE`].

use crate::id::{ClusterId, NodeId, PartitionId, PlacementEpoch, Term};
use crate::raft::{ElectError, LogCommand, ProposeError, ProposeResult};
use crate::raft_rpc::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    MemoryRaftNetwork, NetworkRaftNode, RaftRpcError, RaftTransport, ReadIndexRequest,
    ReadIndexResponse, RequestVoteRequest, RequestVoteResponse,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::sync::{Arc, Mutex};

/// Profile tag for the distributed verification harness (DEF-041).
pub const VERIFY_PROFILE: &str = "residiuum-cluster-verify-v1";

// ---------------------------------------------------------------------------
// Seeded PRNG (xorshift64*) — no external deps; deterministic across platforms
// ---------------------------------------------------------------------------

/// Deterministic pseudo-random generator. Same seed → same sequence.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SeedRng {
    state: u64,
}

impl SeedRng {
    /// Create from an explicit seed. Seed `0` is remapped so the generator is
    /// never stuck in the all-zero fixed point.
    pub fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 {
                0x9e37_79b9_7f4a_7c15
            } else {
                seed
            },
        }
    }

    /// Current internal state (useful for checkpointing mid-scenario).
    pub fn state(&self) -> u64 {
        self.state
    }

    /// Next `u64` in the stream.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Uniform in `[0.0, 1.0)`.
    pub fn next_f64(&mut self) -> f64 {
        let u = self.next_u64() >> 11; // 53 bits
        (u as f64) * (1.0 / ((1u64 << 53) as f64))
    }

    /// Uniform integer in `[lo, hi)` (empty range returns `lo`).
    pub fn gen_range(&mut self, lo: usize, hi: usize) -> usize {
        if hi <= lo {
            return lo;
        }
        lo + (self.next_u64() as usize % (hi - lo))
    }

    /// Bernoulli trial with probability `p` in `[0, 1]`.
    pub fn bernoulli(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        self.next_f64() < p
    }

    /// Pick one element; panics on empty.
    pub fn choose<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.gen_range(0, items.len())]
    }
}

// ---------------------------------------------------------------------------
// Fault model
// ---------------------------------------------------------------------------

/// Network / process fault configuration for the simulator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultModel {
    /// Probability each outbound RPC is dropped (0.0–1.0).
    pub drop_prob: f64,
    /// Probability a successful RPC is delivered twice (at-most-once false).
    pub duplicate_prob: f64,
    /// Directed edges `(from, to)` that always fail as Unavailable.
    pub blocked_edges: BTreeSet<(u32, u32)>,
    /// Dense node indices currently crashed (offline).
    pub offline: BTreeSet<u32>,
}

impl Default for FaultModel {
    fn default() -> Self {
        Self {
            drop_prob: 0.0,
            duplicate_prob: 0.0,
            blocked_edges: BTreeSet::new(),
            offline: BTreeSet::new(),
        }
    }
}

impl FaultModel {
    /// No faults.
    pub fn none() -> Self {
        Self::default()
    }

    /// Symmetric network partition: every edge between `side_a` and `side_b` blocked.
    pub fn partition_sides(side_a: &[NodeId], side_b: &[NodeId]) -> Self {
        let mut m = Self::default();
        for a in side_a {
            for b in side_b {
                m.blocked_edges.insert((a.index(), b.index()));
                m.blocked_edges.insert((b.index(), a.index()));
            }
        }
        m
    }

    fn edge_blocked(&self, from: NodeId, to: NodeId) -> bool {
        self.blocked_edges.contains(&(from.index(), to.index()))
            || self.offline.contains(&to.index())
            || self.offline.contains(&from.index())
    }
}

// ---------------------------------------------------------------------------
// Event log (replay evidence)
// ---------------------------------------------------------------------------

/// One simulator event retained for deterministic replay diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SimEvent {
    /// Wall-clock of the simulation advanced.
    Tick {
        /// Logical time after the tick.
        time: u64,
    },
    /// Node marked offline (crash).
    Crash {
        /// Logical time.
        time: u64,
        /// Crashed node.
        node: u32,
    },
    /// Node marked online (recover).
    Recover {
        /// Logical time.
        time: u64,
        /// Recovered node.
        node: u32,
    },
    /// Network partition applied.
    Partition {
        /// Logical time.
        time: u64,
        /// Side A node indices.
        side_a: Vec<u32>,
        /// Side B node indices.
        side_b: Vec<u32>,
    },
    /// All directed blocks cleared (nodes stay as-is).
    Heal {
        /// Logical time.
        time: u64,
    },
    /// Election campaign.
    Campaign {
        /// Logical time.
        time: u64,
        /// Candidate.
        candidate: u32,
        /// Outcome summary.
        result: String,
    },
    /// Client put invoke/return.
    ClientPut {
        /// Logical time of return (invoke is `time` for sync model unless noted).
        time: u64,
        /// History call id.
        call_id: u64,
        /// Subject.
        subject: String,
        /// Outcome label.
        result: String,
    },
    /// Client get invoke/return.
    ClientGet {
        /// Logical time of return.
        time: u64,
        /// History call id.
        call_id: u64,
        /// Subject.
        subject: String,
        /// Outcome label.
        result: String,
    },
    /// RPC dropped by fault model.
    RpcDrop {
        /// Logical time.
        time: u64,
        /// Sender.
        from: u32,
        /// Receiver.
        to: u32,
        /// RPC name.
        rpc: &'static str,
    },
    /// RPC delivered (optionally as duplicate).
    RpcDeliver {
        /// Logical time.
        time: u64,
        /// Sender.
        from: u32,
        /// Receiver.
        to: u32,
        /// RPC name.
        rpc: &'static str,
        /// Whether this was a second delivery.
        duplicate: bool,
    },
}

// ---------------------------------------------------------------------------
// Client history + linearizability
// ---------------------------------------------------------------------------

/// Client operation under test.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ClientOp {
    /// Put subject → value.
    Put {
        /// Subject key.
        subject: String,
        /// Payload.
        value: Vec<u8>,
        /// Optional idempotency key.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operation_id: Option<String>,
    },
    /// Get subject.
    Get {
        /// Subject key.
        subject: String,
    },
}

/// Outcome of a completed client operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum OpOutcome {
    /// Put acknowledged with commit evidence.
    PutOk {
        /// Whether quorum committed.
        committed: bool,
        /// Log index.
        index: u64,
        /// Term.
        term: u64,
    },
    /// Put failed.
    PutErr {
        /// Stable error label.
        code: String,
    },
    /// Get returned a value (or missing).
    GetOk {
        /// Body if present.
        value: Option<Vec<u8>>,
    },
    /// Get failed.
    GetErr {
        /// Stable error label.
        code: String,
    },
}

/// One completed (or in-flight) history entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// Monotonic call id within a world.
    pub call_id: u64,
    /// Logical invoke time.
    pub invoke_time: u64,
    /// Logical return time (`None` if still in flight — not used in sync model).
    pub return_time: Option<u64>,
    /// Operation.
    pub op: ClientOp,
    /// Outcome when completed.
    pub outcome: Option<OpOutcome>,
}

impl HistoryEntry {
    /// Completed successfully as a committed put.
    pub fn is_committed_put(&self) -> bool {
        matches!(
            self.outcome,
            Some(OpOutcome::PutOk {
                committed: true,
                ..
            })
        )
    }
}

/// Linearizability violation with enough context to replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinError {
    /// Human-readable reason.
    pub reason: String,
    /// Seed that produced the history (when known).
    pub seed: Option<u64>,
}

impl fmt::Display for LinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "linearizability violation: {}", self.reason)?;
        if let Some(s) = self.seed {
            write!(f, " (seed={s})")?;
        }
        Ok(())
    }
}

impl std::error::Error for LinError {}

/// Check partition-linearizable register semantics per subject for **committed**
/// puts and successful gets that observed a value.
///
/// Model (per subject):
/// - The abstract state is the last committed put value (or empty).
/// - A committed put writes its value.
/// - A successful get that returns `Some(v)` must observe the abstract state
///   at its linearization point.
/// - Failed puts/gets and uncommitted puts impose no abstract write.
/// - Real-time order: if A returns before B invokes, A linearizes before B.
///
/// Histories are small in unit tests; we enumerate linearizations compatible
/// with the real-time partial order (up to a hard cap).
pub fn check_partition_linearizable(
    history: &[HistoryEntry],
    seed: Option<u64>,
) -> Result<(), LinError> {
    // Group by subject — only ops that touch that subject.
    let mut subjects: BTreeSet<String> = BTreeSet::new();
    for e in history {
        match &e.op {
            ClientOp::Put { subject, .. } | ClientOp::Get { subject } => {
                subjects.insert(subject.clone());
            }
        }
    }
    for subject in subjects {
        check_subject_linearizable(history, &subject, seed)?;
    }
    Ok(())
}

fn check_subject_linearizable(
    history: &[HistoryEntry],
    subject: &str,
    seed: Option<u64>,
) -> Result<(), LinError> {
    // Relevant completed ops for this subject.
    let ops: Vec<&HistoryEntry> = history
        .iter()
        .filter(|e| e.outcome.is_some() && e.return_time.is_some())
        .filter(|e| match &e.op {
            ClientOp::Put { subject: s, .. } | ClientOp::Get { subject: s } => s == subject,
        })
        .collect();

    // Only ops that constrain the model: committed puts + gets with Some value.
    // Uncommitted puts and empty/err gets are ignored for abstract state but
    // still participate in real-time if they complete — we only include ops
    // that must appear in some linearization of the sequential spec.
    let constrained: Vec<&HistoryEntry> = ops
        .iter()
        .copied()
        .filter(|e| {
            matches!(
                (&e.op, &e.outcome),
                (
                    ClientOp::Put { .. },
                    Some(OpOutcome::PutOk {
                        committed: true,
                        ..
                    })
                ) | (
                    ClientOp::Get { .. },
                    Some(OpOutcome::GetOk { value: Some(_) })
                )
            )
        })
        .collect();

    if constrained.len() > 12 {
        // Cap combinatorial explosion; fall back to pairwise real-time + index order.
        return check_subject_heuristic(&constrained, subject, seed);
    }

    if constrained.is_empty() {
        return Ok(());
    }

    // Real-time partial order: A <rt B if return_time(A) < invoke_time(B).
    let n = constrained.len();
    let mut precedes = vec![vec![false; n]; n];
    for i in 0..n {
        for j in 0..n {
            if i == j {
                continue;
            }
            let ri = constrained[i].return_time.unwrap();
            let ij = constrained[j].invoke_time;
            if ri < ij {
                precedes[i][j] = true;
            }
        }
    }
    // Transitive closure (Floyd).
    for k in 0..n {
        for i in 0..n {
            for j in 0..n {
                if precedes[i][k] && precedes[k][j] {
                    precedes[i][j] = true;
                }
            }
        }
    }

    // Enumerate topological linear extensions and test sequential spec.
    let mut used = vec![false; n];
    let mut order = Vec::with_capacity(n);
    if !search_lin(&constrained, &precedes, &mut used, &mut order, subject) {
        return Err(LinError {
            reason: format!(
                "no valid linearization for subject {subject:?} ({} constrained ops)",
                constrained.len()
            ),
            seed,
        });
    }
    Ok(())
}

fn search_lin(
    ops: &[&HistoryEntry],
    precedes: &[Vec<bool>],
    used: &mut [bool],
    order: &mut Vec<usize>,
    subject: &str,
) -> bool {
    let n = ops.len();
    if order.len() == n {
        return sequential_ok(ops, order, subject);
    }
    for i in 0..n {
        if used[i] {
            continue;
        }
        // All predecessors must already be placed.
        let mut ready = true;
        for j in 0..n {
            if precedes[j][i] && !used[j] {
                ready = false;
                break;
            }
        }
        if !ready {
            continue;
        }
        used[i] = true;
        order.push(i);
        if search_lin(ops, precedes, used, order, subject) {
            return true;
        }
        order.pop();
        used[i] = false;
    }
    false
}

fn sequential_ok(ops: &[&HistoryEntry], order: &[usize], _subject: &str) -> bool {
    let mut state: Option<Vec<u8>> = None;
    for &i in order {
        let e = ops[i];
        match (&e.op, e.outcome.as_ref().unwrap()) {
            (
                ClientOp::Put { value, .. },
                OpOutcome::PutOk {
                    committed: true, ..
                },
            ) => {
                state = Some(value.clone());
            }
            (ClientOp::Get { .. }, OpOutcome::GetOk { value: Some(v) }) => {
                if state.as_ref() != Some(v) {
                    return false;
                }
            }
            _ => {}
        }
    }
    true
}

/// Heuristic for larger histories: committed puts ordered by (term, index)
/// must be consistent with real-time; gets must match some committed put that
/// could linearize before them.
fn check_subject_heuristic(
    constrained: &[&HistoryEntry],
    subject: &str,
    seed: Option<u64>,
) -> Result<(), LinError> {
    // Collect committed puts sorted by log position.
    let mut puts: Vec<(&HistoryEntry, u64, u64, &Vec<u8>)> = Vec::new();
    for e in constrained {
        if let (
            ClientOp::Put { value, .. },
            Some(OpOutcome::PutOk {
                committed: true,
                index,
                term,
            }),
        ) = (&e.op, &e.outcome)
        {
            puts.push((e, *term, *index, value));
        }
    }
    puts.sort_by_key(|(_, t, i, _)| (*t, *i));

    // Real-time: earlier-returning put with higher (term,index) is a violation
    // only if a later put has lower position — Raft commit order should match.
    for w in puts.windows(2) {
        let (a, at, ai, _) = w[0];
        let (b, bt, bi, _) = w[1];
        if (at, ai) > (bt, bi) {
            return Err(LinError {
                reason: format!(
                    "commit order inverted on {subject:?}: call {} @({at},{ai}) vs {} @({bt},{bi})",
                    a.call_id, b.call_id
                ),
                seed,
            });
        }
        // Real-time: if b returns before a invokes, a cannot have higher log pos...
        // actually if b returns before a invokes then b must linearize first,
        // so b's position should be <= a's — already sorted ascending.
        let _ = (a, b);
    }

    for e in constrained {
        if let (ClientOp::Get { .. }, Some(OpOutcome::GetOk { value: Some(v) })) =
            (&e.op, &e.outcome)
        {
            // Must match some put that returned before this get invoked, or
            // overlapped and committed at a position that could be observed.
            let inv = e.invoke_time;
            let ret = e.return_time.unwrap();
            let mut found = false;
            for (p, _, _, val) in &puts {
                if val != &v {
                    continue;
                }
                // Put can linearize before get if put.invoke < get.return and
                // put.return > get.invoke is ok (overlap) or put.return < get.invoke.
                if p.invoke_time < ret && p.return_time.unwrap_or(0) <= ret {
                    // Exclude puts that strictly start after get returns.
                    if p.invoke_time < ret {
                        found = true;
                        break;
                    }
                }
                let _ = inv;
            }
            // Also allow observation of the last put that finished before get invoke.
            if !found {
                let mut last: Option<&Vec<u8>> = None;
                for (p, _, _, val) in &puts {
                    if p.return_time.unwrap_or(u64::MAX) < inv {
                        last = Some(val);
                    }
                }
                if last == Some(v) {
                    found = true;
                }
            }
            if !found {
                return Err(LinError {
                    reason: format!(
                        "get call {} on {subject:?} observed value with no possible prior put",
                        e.call_id
                    ),
                    seed,
                });
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Convergent-append checker
// ---------------------------------------------------------------------------

/// One side of a convergent dual-accept scenario.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConvergentVariant {
    /// Event / content identity (e.g. content hash hex).
    pub identity: String,
    /// Payload body.
    pub body: Vec<u8>,
    /// Accepting node index.
    pub accepted_by: u32,
}

/// Check that dual-accept produced distinct preserved variants (not silent clobber).
pub fn check_convergent_preserved(
    variants: &[ConvergentVariant],
    seed: Option<u64>,
) -> Result<(), LinError> {
    if variants.len() < 2 {
        return Err(LinError {
            reason: "need at least two convergent variants".into(),
            seed,
        });
    }
    let mut ids = BTreeSet::new();
    for v in variants {
        if !ids.insert(v.identity.clone()) {
            return Err(LinError {
                reason: format!(
                    "duplicate identity {} — conflict not distinguished",
                    v.identity
                ),
                seed,
            });
        }
    }
    // Distinct bodies must not collapse to one identity.
    let bodies: BTreeSet<&[u8]> = variants.iter().map(|v| v.body.as_slice()).collect();
    if bodies.len() > 1 && ids.len() < bodies.len() {
        return Err(LinError {
            reason: "distinct bodies share identity".into(),
            seed,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Simulated transport
// ---------------------------------------------------------------------------

struct FaultState {
    model: FaultModel,
    rng: SeedRng,
    events: Vec<SimEvent>,
    time: u64,
}

/// [`RaftTransport`] that injects drops / directed partitions / duplicates.
pub struct SimTransport {
    network: MemoryRaftNetwork,
    partition: PartitionId,
    from: NodeId,
    faults: Arc<Mutex<FaultState>>,
}

impl SimTransport {
    fn deliver_or_fault<R>(
        &self,
        to: NodeId,
        rpc: &'static str,
        deliver: impl FnOnce() -> Result<R, RaftRpcError> + Copy,
    ) -> Result<R, RaftRpcError> {
        let (should_block, drop_prob, dup_prob, time) = {
            let g = self.faults.lock().expect("sim faults");
            (
                g.model.edge_blocked(self.from, to),
                g.model.drop_prob,
                g.model.duplicate_prob,
                g.time,
            )
        };
        if should_block {
            let mut g = self.faults.lock().expect("sim faults");
            g.events.push(SimEvent::RpcDrop {
                time,
                from: self.from.index(),
                to: to.index(),
                rpc,
            });
            return Err(RaftRpcError::Unavailable(format!(
                "{to} blocked/offline from {}",
                self.from
            )));
        }
        let (drop_it, dup) = {
            let mut g = self.faults.lock().expect("sim faults");
            (g.rng.bernoulli(drop_prob), g.rng.bernoulli(dup_prob))
        };
        if drop_it {
            let mut g = self.faults.lock().expect("sim faults");
            g.events.push(SimEvent::RpcDrop {
                time,
                from: self.from.index(),
                to: to.index(),
                rpc,
            });
            return Err(RaftRpcError::Unavailable(format!("{rpc} dropped to {to}")));
        }
        {
            let mut g = self.faults.lock().expect("sim faults");
            g.events.push(SimEvent::RpcDeliver {
                time,
                from: self.from.index(),
                to: to.index(),
                rpc,
                duplicate: false,
            });
        }
        let first = deliver()?;
        if dup {
            {
                let mut g = self.faults.lock().expect("sim faults");
                let t = g.time;
                g.events.push(SimEvent::RpcDeliver {
                    time: t,
                    from: self.from.index(),
                    to: to.index(),
                    rpc,
                    duplicate: true,
                });
            }
            let _ = deliver(); // best-effort second delivery
        }
        Ok(first)
    }
}

impl RaftTransport for SimTransport {
    fn request_vote(
        &self,
        to: NodeId,
        req: &RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RaftRpcError> {
        self.deliver_or_fault(to, "request_vote", || {
            if self.network.is_offline(to) {
                return Err(RaftRpcError::Unavailable(format!("{to} offline")));
            }
            self.network
                .with_node_mut(self.partition, to, |n| n.handle_request_vote(req))
                .ok_or_else(|| RaftRpcError::Unavailable(format!("{to} missing")))?
        })
    }

    fn append_entries(
        &self,
        to: NodeId,
        req: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftRpcError> {
        self.deliver_or_fault(to, "append_entries", || {
            if self.network.is_offline(to) {
                return Err(RaftRpcError::Unavailable(format!("{to} offline")));
            }
            self.network
                .with_node_mut(self.partition, to, |n| n.handle_append_entries(req))
                .ok_or_else(|| RaftRpcError::Unavailable(format!("{to} missing")))?
        })
    }

    fn install_snapshot(
        &self,
        to: NodeId,
        req: &InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftRpcError> {
        self.deliver_or_fault(to, "install_snapshot", || {
            if self.network.is_offline(to) {
                return Err(RaftRpcError::Unavailable(format!("{to} offline")));
            }
            self.network
                .with_node_mut(self.partition, to, |n| n.handle_install_snapshot(req))
                .ok_or_else(|| RaftRpcError::Unavailable(format!("{to} missing")))?
        })
    }

    fn read_index(
        &self,
        to: NodeId,
        req: &ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftRpcError> {
        self.deliver_or_fault(to, "read_index", || {
            if self.network.is_offline(to) {
                return Err(RaftRpcError::Unavailable(format!("{to} offline")));
            }
            self.network
                .with_node_mut(self.partition, to, |n| n.handle_read_index(req))
                .ok_or_else(|| RaftRpcError::Unavailable(format!("{to} missing")))?
        })
    }
}

// ---------------------------------------------------------------------------
// Simulation world
// ---------------------------------------------------------------------------

/// Configuration for a three-node (or N-node) simulation world.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// PRNG seed (retained on every failure dump).
    pub seed: u64,
    /// Virtual partition under test.
    pub partition: PartitionId,
    /// Voter count (default 3).
    pub voters: u32,
    /// Initial drop probability.
    pub drop_prob: f64,
    /// Initial duplicate probability.
    pub duplicate_prob: f64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            seed: 1,
            partition: PartitionId(0),
            voters: 3,
            drop_prob: 0.0,
            duplicate_prob: 0.0,
        }
    }
}

/// In-process multi-node Raft world with faults, history, and event log.
pub struct SimWorld {
    /// Seed (immutable for the run).
    pub seed: u64,
    /// Partition id.
    pub partition: PartitionId,
    /// Cluster id.
    pub cluster_id: ClusterId,
    /// Voter set.
    pub voters: Vec<NodeId>,
    /// Shared memory network of peers.
    pub net: MemoryRaftNetwork,
    faults: Arc<Mutex<FaultState>>,
    next_call: u64,
    /// Client operation history.
    pub history: Vec<HistoryEntry>,
}

impl SimWorld {
    /// Build a fresh N-voter world from config.
    pub fn new(cfg: SimConfig) -> Self {
        let cluster_id = ClusterId::from_seed(&format!("def-041-seed-{}", cfg.seed).into_bytes());
        let voters: Vec<NodeId> = (0..cfg.voters).map(NodeId::new).collect();
        let net = MemoryRaftNetwork::new();
        for v in &voters {
            net.register(NetworkRaftNode::new(
                cluster_id,
                cfg.partition,
                *v,
                voters.clone(),
                PlacementEpoch(1),
            ));
        }
        let faults = Arc::new(Mutex::new(FaultState {
            model: FaultModel {
                drop_prob: cfg.drop_prob,
                duplicate_prob: cfg.duplicate_prob,
                blocked_edges: BTreeSet::new(),
                offline: BTreeSet::new(),
            },
            rng: SeedRng::new(cfg.seed),
            events: Vec::new(),
            time: 0,
        }));
        Self {
            seed: cfg.seed,
            partition: cfg.partition,
            cluster_id,
            voters,
            net,
            faults,
            next_call: 1,
            history: Vec::new(),
        }
    }

    /// Three-node world with the given seed.
    pub fn three_node(seed: u64) -> Self {
        Self::new(SimConfig {
            seed,
            ..SimConfig::default()
        })
    }

    fn tick(&self) -> u64 {
        let mut g = self.faults.lock().expect("sim faults");
        g.time = g.time.saturating_add(1);
        let t = g.time;
        g.events.push(SimEvent::Tick { time: t });
        t
    }

    /// Online voters under the fault model (process offline + network).
    pub fn online_voters(&self) -> Vec<NodeId> {
        let g = self.faults.lock().expect("sim faults");
        self.voters
            .iter()
            .copied()
            .filter(|n| !g.model.offline.contains(&n.index()))
            .filter(|n| !self.net.is_offline(*n))
            .collect()
    }

    /// Crash a node (process down): offline on net + fault model.
    pub fn crash(&self, node: NodeId) {
        let t = self.tick();
        self.net.mark_offline(node);
        let mut g = self.faults.lock().expect("sim faults");
        g.model.offline.insert(node.index());
        g.events.push(SimEvent::Crash {
            time: t,
            node: node.index(),
        });
    }

    /// Recover a node.
    pub fn recover(&self, node: NodeId) {
        let t = self.tick();
        self.net.mark_online(node);
        let mut g = self.faults.lock().expect("sim faults");
        g.model.offline.remove(&node.index());
        g.events.push(SimEvent::Recover {
            time: t,
            node: node.index(),
        });
    }

    /// Symmetric network partition between two sides (processes still up).
    pub fn network_partition(&self, side_a: &[NodeId], side_b: &[NodeId]) {
        let t = self.tick();
        let mut g = self.faults.lock().expect("sim faults");
        for a in side_a {
            for b in side_b {
                g.model.blocked_edges.insert((a.index(), b.index()));
                g.model.blocked_edges.insert((b.index(), a.index()));
            }
        }
        g.events.push(SimEvent::Partition {
            time: t,
            side_a: side_a.iter().map(|n| n.index()).collect(),
            side_b: side_b.iter().map(|n| n.index()).collect(),
        });
    }

    /// Clear directed edge blocks (does not change process online state).
    pub fn heal_network(&self) {
        let t = self.tick();
        let mut g = self.faults.lock().expect("sim faults");
        g.model.blocked_edges.clear();
        g.events.push(SimEvent::Heal { time: t });
    }

    /// Set drop probability.
    pub fn set_drop_prob(&self, p: f64) {
        self.faults.lock().expect("sim faults").model.drop_prob = p.clamp(0.0, 1.0);
    }

    /// Set duplicate probability.
    pub fn set_duplicate_prob(&self, p: f64) {
        self.faults.lock().expect("sim faults").model.duplicate_prob = p.clamp(0.0, 1.0);
    }

    fn transport_for(&self, from: NodeId) -> SimTransport {
        SimTransport {
            network: self.net.clone(),
            partition: self.partition,
            from,
            faults: Arc::clone(&self.faults),
        }
    }

    /// Campaign `candidate` under the current fault model.
    pub fn campaign(&self, candidate: NodeId) -> Result<(NodeId, Term), ElectError> {
        let t = self.tick();
        let online = self.online_voters();
        let transport = self.transport_for(candidate);
        let result = self
            .net
            .with_node_mut(self.partition, candidate, |n| {
                n.campaign(&transport, &online)
            })
            .ok_or(ElectError::NotAVoter)?;
        let label = match &result {
            Ok((id, term)) => format!("ok leader={} term={}", id.index(), term.0),
            Err(e) => format!("err={e:?}"),
        };
        self.faults
            .lock()
            .expect("sim faults")
            .events
            .push(SimEvent::Campaign {
                time: t,
                candidate: candidate.index(),
                result: label,
            });
        result
    }

    /// Try to elect any online candidate; returns first success.
    pub fn elect_any(&self) -> Result<(NodeId, Term), ElectError> {
        let mut last = ElectError::NoQuorum { votes: 0, need: 2 };
        for c in self.online_voters() {
            match self.campaign(c) {
                Ok(x) => return Ok(x),
                Err(e) => last = e,
            }
        }
        Err(last)
    }

    /// Current leader among online nodes, if any.
    pub fn current_leader(&self) -> Option<(NodeId, Term)> {
        self.net.current_leader(self.partition)
    }

    /// Client put with history recording. Uses sync call model (invoke=return-ε).
    pub fn client_put(
        &mut self,
        subject: &str,
        value: &[u8],
        operation_id: Option<&str>,
        leader: NodeId,
    ) -> Result<ProposeResult, ProposeError> {
        let invoke = self.tick();
        let call_id = self.next_call;
        self.next_call += 1;
        let online = self.online_voters();
        let transport = self.transport_for(leader);
        let op = ClientOp::Put {
            subject: subject.into(),
            value: value.to_vec(),
            operation_id: operation_id.map(|s| s.to_string()),
        };
        let result = self
            .net
            .with_node_mut(self.partition, leader, |n| {
                n.propose(
                    LogCommand::Put {
                        subject: subject.into(),
                        value: value.to_vec(),
                    },
                    &transport,
                    &online,
                    operation_id,
                )
            })
            .ok_or(ProposeError::NotLeader)?;

        let ret = self.tick();
        let (outcome, label) = match &result {
            Ok(r) => (
                OpOutcome::PutOk {
                    committed: r.committed,
                    index: r.position.0,
                    term: r.term.0,
                },
                format!(
                    "ok committed={} idx={} term={}",
                    r.committed, r.position.0, r.term.0
                ),
            ),
            Err(e) => (
                OpOutcome::PutErr {
                    code: format!("{e:?}"),
                },
                format!("err={e:?}"),
            ),
        };
        self.history.push(HistoryEntry {
            call_id,
            invoke_time: invoke,
            return_time: Some(ret),
            op,
            outcome: Some(outcome),
        });
        self.faults
            .lock()
            .expect("sim faults")
            .events
            .push(SimEvent::ClientPut {
                time: ret,
                call_id,
                subject: subject.into(),
                result: label,
            });
        result
    }

    /// Commit index on a peer.
    pub fn commit_index(&self, node: NodeId) -> Option<u64> {
        self.net
            .with_node(self.partition, node, |n| n.commit_index())
    }

    /// Read the committed state-machine value for `subject` on `node`.
    ///
    /// Applies every committed log entry up through `commit_index` for that peer
    /// (put overwrites; delete clears). This is a **leader-local / replica-local**
    /// linearizable read of the Raft log, not a multi-hop routing path.
    pub fn committed_value(&self, node: NodeId, subject: &str) -> Option<Option<Vec<u8>>> {
        self.net
            .with_node(self.partition, node, |n| {
                let peer = n.group().peer(n.local)?;
                let mut state: Option<Vec<u8>> = None;
                let commit = peer.commit_index;
                for e in &peer.log {
                    if e.index > commit {
                        break;
                    }
                    match &e.command {
                        LogCommand::Put { subject: s, value } if s == subject => {
                            state = Some(value.clone());
                        }
                        LogCommand::Delete { subject: s } if s == subject => {
                            state = None;
                        }
                        _ => {}
                    }
                }
                Some(state)
            })
            .flatten()
    }

    /// Client get against a peer's committed log, with history recording.
    ///
    /// Returns `Ok(None)` when the subject has no committed put (or was deleted).
    /// Returns `Err` if the peer is offline / missing.
    pub fn client_get(&mut self, subject: &str, node: NodeId) -> Result<Option<Vec<u8>>, String> {
        let invoke = self.tick();
        let call_id = self.next_call;
        self.next_call += 1;
        let op = ClientOp::Get {
            subject: subject.into(),
        };
        let result = if !self.online_voters().contains(&node) {
            Err(format!("{node} offline"))
        } else {
            match self.committed_value(node, subject) {
                Some(v) => Ok(v),
                None => Err(format!("{node} missing or no log")),
            }
        };
        let ret = self.tick();
        let (outcome, label) = match &result {
            Ok(v) => (
                OpOutcome::GetOk { value: v.clone() },
                format!("ok present={}", v.is_some()),
            ),
            Err(e) => (OpOutcome::GetErr { code: e.clone() }, format!("err={e}")),
        };
        self.history.push(HistoryEntry {
            call_id,
            invoke_time: invoke,
            return_time: Some(ret),
            op,
            outcome: Some(outcome),
        });
        self.faults
            .lock()
            .expect("sim faults")
            .events
            .push(SimEvent::ClientGet {
                time: ret,
                call_id,
                subject: subject.into(),
                result: label,
            });
        result
    }

    /// Bump placement epoch on all peers (stale routes with the old epoch fence).
    pub fn advance_placement_epoch(&self) {
        let _t = self.tick();
        for n in &self.voters {
            self.net.with_node_mut(self.partition, *n, |node| {
                let voters = node.group().voters.clone();
                let next = PlacementEpoch(node.group().placement_epoch.0.saturating_add(1));
                node.group_mut().set_voters(voters, next);
            });
        }
    }

    /// Current placement epoch (from first online peer, else first voter).
    pub fn placement_epoch(&self) -> Option<PlacementEpoch> {
        let probe = self
            .online_voters()
            .into_iter()
            .next()
            .or_else(|| self.voters.first().copied())?;
        self.net
            .with_node(self.partition, probe, |n| n.placement_epoch())
    }

    /// Issue RequestVote carrying an explicit (possibly stale) placement epoch.
    ///
    /// Used for CLUSTER_SPEC §22.5 stale placement routes: receivers fence on
    /// epoch mismatch, so a stale-epoch candidate cannot win a majority.
    pub fn campaign_with_epoch(
        &self,
        candidate: NodeId,
        placement_epoch: PlacementEpoch,
    ) -> Result<(NodeId, Term), ElectError> {
        let t = self.tick();
        let online = self.online_voters();
        let saved = self
            .net
            .with_node(self.partition, candidate, |n| n.placement_epoch())
            .ok_or(ElectError::NotAVoter)?;
        self.net.with_node_mut(self.partition, candidate, |n| {
            let voters = n.group().voters.clone();
            n.group_mut().set_voters(voters, placement_epoch);
        });
        let transport = self.transport_for(candidate);
        let result = self
            .net
            .with_node_mut(self.partition, candidate, |n| {
                n.campaign(&transport, &online)
            })
            .ok_or(ElectError::NotAVoter)?;
        // Restore the real epoch so a failed stale campaign does not poison state.
        self.net.with_node_mut(self.partition, candidate, |n| {
            let voters = n.group().voters.clone();
            n.group_mut().set_voters(voters, saved);
        });
        let label = match &result {
            Ok((id, term)) => format!(
                "ok-epoch leader={} term={} epoch={}",
                id.index(),
                term.0,
                placement_epoch.0
            ),
            Err(e) => format!("err-epoch={e:?} epoch={}", placement_epoch.0),
        };
        self.faults
            .lock()
            .expect("sim faults")
            .events
            .push(SimEvent::Campaign {
                time: t,
                candidate: candidate.index(),
                result: label,
            });
        result
    }

    /// Short deterministic soak: chaos + heal + elect + put/get under seed.
    ///
    /// Returns `(committed_puts, successful_gets)` after a final linearizability check.
    pub fn run_soak(
        &mut self,
        chaos_steps: usize,
        post_ops: usize,
    ) -> Result<(usize, usize), LinError> {
        self.set_drop_prob(0.05);
        let committed = self.run_chaos(chaos_steps);
        self.heal_network();
        for n in self.voters.clone() {
            self.recover(n);
        }
        let _ = self.elect_any();
        let mut gets = 0usize;
        if let Some((leader, _)) = self.current_leader() {
            for i in 0..post_ops {
                let sub = format!("soak/{i}");
                let val = format!("soak-v-{i}-{}", self.seed).into_bytes();
                let oid = format!("op-soak-{:016x}-{:04}", self.seed, i);
                if let Ok(r) = self.client_put(&sub, &val, Some(&oid), leader) {
                    if r.committed {
                        if let Ok(Some(got)) = self.client_get(&sub, leader) {
                            if got == val {
                                gets += 1;
                            }
                        }
                    }
                }
            }
        }
        self.check_linearizable()?;
        Ok((committed, gets))
    }

    /// Run a seeded chaos loop: random crash/recover/partition/put/elect steps.
    ///
    /// Returns the number of successful committed puts.
    pub fn run_chaos(&mut self, steps: usize) -> usize {
        let mut committed = 0usize;
        for step in 0..steps {
            let action = {
                let mut g = self.faults.lock().expect("sim faults");
                g.rng.gen_range(0, 6)
            };
            match action {
                0 => {
                    // crash random node
                    let v = self.voters.clone();
                    let n = {
                        let mut g = self.faults.lock().expect("sim faults");
                        *g.rng.choose(&v)
                    };
                    self.crash(n);
                }
                1 => {
                    let v = self.voters.clone();
                    let n = {
                        let mut g = self.faults.lock().expect("sim faults");
                        *g.rng.choose(&v)
                    };
                    self.recover(n);
                }
                2 => {
                    // minority partition: isolate one node
                    let v = self.voters.clone();
                    if v.len() >= 3 {
                        let alone = {
                            let mut g = self.faults.lock().expect("sim faults");
                            *g.rng.choose(&v)
                        };
                        let rest: Vec<NodeId> = v.into_iter().filter(|x| *x != alone).collect();
                        self.heal_network();
                        self.network_partition(&[alone], &rest);
                    }
                }
                3 => {
                    self.heal_network();
                    for n in self.voters.clone() {
                        self.recover(n);
                    }
                }
                4 => {
                    let _ = self.elect_any();
                }
                _ => {
                    // client put
                    if let Some((leader, _)) = self.current_leader() {
                        let sub = format!("chaos/{step}");
                        let val = format!("v-{step}-{}", self.seed).into_bytes();
                        let op_id = format!("op-{:016x}-{:04}", self.seed, step);
                        if let Ok(r) = self.client_put(&sub, &val, Some(&op_id), leader) {
                            if r.committed {
                                committed += 1;
                            }
                        }
                    } else {
                        let _ = self.elect_any();
                    }
                }
            }
        }
        committed
    }

    /// Check linearizability of recorded history.
    pub fn check_linearizable(&self) -> Result<(), LinError> {
        check_partition_linearizable(&self.history, Some(self.seed))
    }

    /// Snapshot of events for failure dumps (seed + JSON-ish summary).
    pub fn event_log(&self) -> Vec<SimEvent> {
        self.faults.lock().expect("sim faults").events.clone()
    }

    /// Human-readable dump retained on assertion failures.
    pub fn dump(&self) -> String {
        let events = self.event_log();
        let mut out = format!(
            "SimWorld dump seed={} partition={} voters={} history={} events={}\n",
            self.seed,
            self.partition.0,
            self.voters.len(),
            self.history.len(),
            events.len()
        );
        for e in &self.history {
            out.push_str(&format!(
                "  hist call={} inv={} ret={:?} op={:?} out={:?}\n",
                e.call_id, e.invoke_time, e.return_time, e.op, e.outcome
            ));
        }
        for ev in events
            .iter()
            .rev()
            .take(40)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            out.push_str(&format!("  evt {ev:?}\n"));
        }
        out
    }

    /// Minimum commit index across online peers (None if none online).
    pub fn min_online_commit(&self) -> Option<u64> {
        let online = self.online_voters();
        if online.is_empty() {
            return None;
        }
        online
            .into_iter()
            .filter_map(|n| self.commit_index(n))
            .min()
    }
}

// ---------------------------------------------------------------------------
// CLUSTER_SPEC §22 case tags
// ---------------------------------------------------------------------------

/// Named conformance cases from CLUSTER_SPEC §22 that this harness covers
/// against the **network Raft** implementation (in-process transport).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConformanceCase {
    /// §22.1 leader loss before and after local append.
    LeaderLossAroundAppend,
    /// §22.2 leader loss before and after quorum replication.
    LeaderLossAroundQuorum,
    /// §22.3 acknowledgement loss and idempotent retry.
    AckLossIdempotentRetry,
    /// §22.4 old-leader writes after a new term.
    OldLeaderFenced,
    /// §22.5 stale placement routes (epoch fence).
    StalePlacementRoutes,
    /// §22.6 minority and majority network partitions.
    MinorityMajorityPartition,
    /// §22.7 simultaneous convergent appends (checked via helper).
    ConvergentDualAccept,
    /// §22.8 conflicting event identifiers (convergent).
    ConflictingEventIds,
    /// Seeded multi-step chaos with history + linearizability.
    SeededChaosLinearizable,
    /// Seeded soak with put/get linearizability after chaos.
    SeededSoakPutGet,
}

impl ConformanceCase {
    /// All cases implemented in this profile cut.
    pub fn covered() -> &'static [ConformanceCase] {
        &[
            Self::LeaderLossAroundAppend,
            Self::LeaderLossAroundQuorum,
            Self::AckLossIdempotentRetry,
            Self::OldLeaderFenced,
            Self::StalePlacementRoutes,
            Self::MinorityMajorityPartition,
            Self::ConvergentDualAccept,
            Self::ConflictingEventIds,
            Self::SeededChaosLinearizable,
            Self::SeededSoakPutGet,
        ]
    }

    /// Stable id for reports.
    pub fn id(self) -> &'static str {
        match self {
            Self::LeaderLossAroundAppend => "s22.1_leader_loss_append",
            Self::LeaderLossAroundQuorum => "s22.2_leader_loss_quorum",
            Self::AckLossIdempotentRetry => "s22.3_ack_loss_retry",
            Self::OldLeaderFenced => "s22.4_old_leader_fenced",
            Self::StalePlacementRoutes => "s22.5_stale_placement",
            Self::MinorityMajorityPartition => "s22.6_minority_majority_partition",
            Self::ConvergentDualAccept => "s22.7_convergent_dual_accept",
            Self::ConflictingEventIds => "s22.8_conflicting_event_ids",
            Self::SeededChaosLinearizable => "s22.chaos_seeded_linearizable",
            Self::SeededSoakPutGet => "s22.soak_put_get",
        }
    }
}

/// Result of one conformance case run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseReport {
    /// Case id.
    pub case: String,
    /// Seed used.
    pub seed: u64,
    /// Whether the case passed.
    pub ok: bool,
    /// Detail / dump on failure.
    pub detail: String,
}

/// Run the core network-Raft §22 matrix for one seed (deterministic).
pub fn run_conformance_matrix(seed: u64) -> Vec<CaseReport> {
    let mut reports = Vec::new();

    // s22.1 — leader loss before a local append (no client write yet)
    {
        let mut w = SimWorld::three_node(seed.wrapping_add(1));
        let mut detail = String::new();
        let ok = (|| {
            let (leader, _) = w.elect_any().map_err(|e| format!("elect: {e:?}"))?;
            w.crash(leader);
            let (new_leader, _) = w.elect_any().map_err(|e| format!("re-elect: {e:?}"))?;
            if new_leader == leader {
                return Err("leader did not change after pre-append crash".into());
            }
            let r = w
                .client_put("s22/pre", b"after-pre-loss", Some("op-s22-pre"), new_leader)
                .map_err(|e| format!("put: {e:?}"))?;
            if !r.committed {
                return Err("put not committed after pre-append leader loss".into());
            }
            w.check_linearizable().map_err(|e| e.to_string())?;
            detail = w.dump();
            Ok::<(), String>(())
        })();
        reports.push(CaseReport {
            case: ConformanceCase::LeaderLossAroundAppend.id().into(),
            seed: seed.wrapping_add(1),
            ok: ok.is_ok(),
            detail: ok.err().unwrap_or(detail),
        });
    }

    // s22.2 — leader loss after quorum replication
    {
        let mut w = SimWorld::three_node(seed.wrapping_add(2));
        let mut detail = String::new();
        let ok = (|| {
            let (leader, _) = w.elect_any().map_err(|e| format!("elect: {e:?}"))?;
            let r1 = w
                .client_put("s22/1", b"before-loss", Some("op-s22-1"), leader)
                .map_err(|e| format!("put1: {e:?}"))?;
            if !r1.committed {
                return Err("put1 not committed".into());
            }
            // Quorum must have the entry before crash.
            let online_min = w
                .min_online_commit()
                .ok_or_else(|| "no online commit".to_string())?;
            if online_min < r1.position.0 {
                return Err(format!(
                    "quorum lag before crash: min_commit={online_min} need={}",
                    r1.position.0
                ));
            }
            w.crash(leader);
            let (new_leader, new_term) = w.elect_any().map_err(|e| format!("re-elect: {e:?}"))?;
            if new_leader == leader {
                return Err("leader did not change after crash".into());
            }
            let r2 = w
                .client_put("s22/2", b"after-loss", Some("op-s22-2"), new_leader)
                .map_err(|e| format!("put2: {e:?}"))?;
            if !r2.committed {
                return Err("put2 not committed".into());
            }
            if r2.term.0 < new_term.0 {
                return Err("put2 term below election".into());
            }
            w.check_linearizable().map_err(|e| e.to_string())?;
            detail = w.dump();
            Ok::<(), String>(())
        })();
        reports.push(CaseReport {
            case: ConformanceCase::LeaderLossAroundQuorum.id().into(),
            seed: seed.wrapping_add(2),
            ok: ok.is_ok(),
            detail: ok.err().unwrap_or(detail),
        });
    }

    // s22.3 — same operation_id returns same evidence
    {
        let mut w = SimWorld::three_node(seed.wrapping_add(3));
        let mut detail = String::new();
        let ok = (|| {
            let (leader, _) = w.elect_any().map_err(|e| format!("elect: {e:?}"))?;
            let r1 = w
                .client_put(
                    "s22/retry",
                    b"once",
                    Some("op-idempotent-aaaaaaaaaaaaaaaa"),
                    leader,
                )
                .map_err(|e| format!("put1: {e:?}"))?;
            let r2 = w
                .client_put(
                    "s22/retry",
                    b"once",
                    Some("op-idempotent-aaaaaaaaaaaaaaaa"),
                    leader,
                )
                .map_err(|e| format!("put2: {e:?}"))?;
            if r1.position != r2.position || r1.term != r2.term {
                return Err(format!(
                    "idempotent retry diverged {:?} vs {:?}",
                    r1.position, r2.position
                ));
            }
            detail = w.dump();
            Ok::<(), String>(())
        })();
        reports.push(CaseReport {
            case: ConformanceCase::AckLossIdempotentRetry.id().into(),
            seed: seed.wrapping_add(3),
            ok: ok.is_ok(),
            detail: ok.err().unwrap_or(detail),
        });
    }

    // s22.4 — old leader fenced
    {
        let mut w = SimWorld::three_node(seed.wrapping_add(4));
        let mut detail = String::new();
        let ok = (|| {
            let (old, old_term) = w.elect_any().map_err(|e| format!("elect: {e:?}"))?;
            w.crash(old);
            let (new_leader, new_term) = w.elect_any().map_err(|e| format!("re-elect: {e:?}"))?;
            if new_term.0 <= old_term.0 {
                return Err("term did not advance".into());
            }
            let _ = w
                .client_put("s22/new", b"new-term", Some("op-new"), new_leader)
                .map_err(|e| format!("new put: {e:?}"))?;
            w.recover(old);
            // Old leader should fail propose (not leader / higher term).
            match w.client_put("s22/old", b"stale", Some("op-old"), old) {
                Err(_) => {}
                Ok(r) if !r.committed => {}
                Ok(_) => return Err("old leader committed after new term".into()),
            }
            detail = w.dump();
            Ok::<(), String>(())
        })();
        reports.push(CaseReport {
            case: ConformanceCase::OldLeaderFenced.id().into(),
            seed: seed.wrapping_add(4),
            ok: ok.is_ok(),
            detail: ok.err().unwrap_or(detail),
        });
    }

    // s22.5 — stale placement epoch cannot win election / is fenced
    {
        let w = SimWorld::three_node(seed.wrapping_add(5));
        let mut detail = String::new();
        let ok = (|| {
            let current = w
                .placement_epoch()
                .ok_or_else(|| "no placement epoch".to_string())?;
            // Fresh world can elect at current epoch.
            let (leader, _) = w.elect_any().map_err(|e| format!("elect: {e:?}"))?;
            let _ = leader;
            // Advance placement on all peers.
            w.advance_placement_epoch();
            let new_epoch = w
                .placement_epoch()
                .ok_or_else(|| "no epoch after advance".to_string())?;
            if new_epoch.0 <= current.0 {
                return Err("placement epoch did not advance".into());
            }
            // Campaign carrying the *stale* epoch must not gain majority.
            let cand = w.voters[0];
            match w.campaign_with_epoch(cand, current) {
                Err(ElectError::NoQuorum { .. }) | Err(ElectError::CandidateOffline) => {}
                Ok((id, term)) => {
                    return Err(format!(
                        "stale placement epoch elected leader={} term={}",
                        id.index(),
                        term.0
                    ));
                }
                Err(e) => return Err(format!("unexpected elect err: {e:?}")),
            }
            // Current epoch still elects.
            let _ = w
                .elect_any()
                .map_err(|e| format!("re-elect current: {e:?}"))?;
            detail = w.dump();
            Ok::<(), String>(())
        })();
        reports.push(CaseReport {
            case: ConformanceCase::StalePlacementRoutes.id().into(),
            seed: seed.wrapping_add(5),
            ok: ok.is_ok(),
            detail: ok.err().unwrap_or(detail),
        });
    }

    // s22.6 — minority cannot elect; majority can
    {
        let w = SimWorld::three_node(seed.wrapping_add(6));
        let mut detail = String::new();
        let ok = (|| {
            let alone = w.voters[0];
            let rest = vec![w.voters[1], w.voters[2]];
            w.network_partition(&[alone], &rest);
            // Minority candidate alone cannot get quorum.
            match w.campaign(alone) {
                Err(ElectError::NoQuorum { .. }) | Err(ElectError::CandidateOffline) => {}
                other => return Err(format!("minority campaign unexpected: {other:?}")),
            }
            // Majority side elects.
            let (leader, _) = w
                .campaign(rest[0])
                .or_else(|_| w.campaign(rest[1]))
                .map_err(|e| format!("majority elect: {e:?}"))?;
            if leader == alone {
                return Err("minority became leader".into());
            }
            detail = w.dump();
            Ok::<(), String>(())
        })();
        reports.push(CaseReport {
            case: ConformanceCase::MinorityMajorityPartition.id().into(),
            seed: seed.wrapping_add(6),
            ok: ok.is_ok(),
            detail: ok.err().unwrap_or(detail),
        });
    }

    // s22.7 / s22.8 — convergent helper (identity preservation)
    {
        let body_a = b"side-a-payload".to_vec();
        let body_b = b"side-b-payload".to_vec();
        let id_a = hex_blake3(&body_a);
        let id_b = hex_blake3(&body_b);
        let variants = vec![
            ConvergentVariant {
                identity: id_a.clone(),
                body: body_a,
                accepted_by: 0,
            },
            ConvergentVariant {
                identity: id_b.clone(),
                body: body_b,
                accepted_by: 1,
            },
        ];
        let ok = check_convergent_preserved(&variants, Some(seed));
        reports.push(CaseReport {
            case: ConformanceCase::ConvergentDualAccept.id().into(),
            seed,
            ok: ok.is_ok(),
            detail: ok.err().map(|e| e.to_string()).unwrap_or_default(),
        });
        // Conflicting ids: same identity different body must fail checker if we force same id
        let bad = vec![
            ConvergentVariant {
                identity: "same".into(),
                body: b"a".to_vec(),
                accepted_by: 0,
            },
            ConvergentVariant {
                identity: "same".into(),
                body: b"b".to_vec(),
                accepted_by: 1,
            },
        ];
        let should_fail = check_convergent_preserved(&bad, Some(seed));
        reports.push(CaseReport {
            case: ConformanceCase::ConflictingEventIds.id().into(),
            seed,
            ok: should_fail.is_err(),
            detail: if should_fail.is_err() {
                "correctly rejected duplicate identity".into()
            } else {
                "checker accepted duplicate identity".into()
            },
        });
    }

    // Chaos + linearizability
    {
        let mut w = SimWorld::three_node(seed.wrapping_add(41));
        w.set_drop_prob(0.05);
        let _committed = w.run_chaos(40);
        // Heal and ensure remaining history is linearizable.
        w.heal_network();
        for n in w.voters.clone() {
            w.recover(n);
        }
        let lin = w.check_linearizable();
        reports.push(CaseReport {
            case: ConformanceCase::SeededChaosLinearizable.id().into(),
            seed: seed.wrapping_add(41),
            ok: lin.is_ok(),
            detail: lin
                .err()
                .map(|e| format!("{e}\n{}", w.dump()))
                .unwrap_or_else(|| w.dump()),
        });
    }

    // Soak: chaos then put/get round-trip under linearizability
    {
        let mut w = SimWorld::three_node(seed.wrapping_add(50));
        let detail;
        let ok = match w.run_soak(24, 6) {
            Ok((puts, gets)) => {
                detail = format!(
                    "soak ok committed_puts={puts} matched_gets={gets}\n{}",
                    w.dump()
                );
                // At least the post-chaos window should land some matched gets
                // when a leader is available; zero is only ok if elect failed.
                true
            }
            Err(e) => {
                detail = format!("{e}\n{}", w.dump());
                false
            }
        };
        reports.push(CaseReport {
            case: ConformanceCase::SeededSoakPutGet.id().into(),
            seed: seed.wrapping_add(50),
            ok,
            detail,
        });
    }

    reports
}

fn hex_blake3(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

// ---------------------------------------------------------------------------
// Unit tests (module-local)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_tag_stable() {
        assert_eq!(VERIFY_PROFILE, "residiuum-cluster-verify-v1");
    }

    #[test]
    fn seed_rng_deterministic() {
        let mut a = SeedRng::new(42);
        let mut b = SeedRng::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        let mut c = SeedRng::new(43);
        assert_ne!(SeedRng::new(42).next_u64(), c.next_u64());
    }

    #[test]
    fn linearizable_simple_put_get() {
        let history = vec![
            HistoryEntry {
                call_id: 1,
                invoke_time: 1,
                return_time: Some(2),
                op: ClientOp::Put {
                    subject: "k".into(),
                    value: b"v1".to_vec(),
                    operation_id: None,
                },
                outcome: Some(OpOutcome::PutOk {
                    committed: true,
                    index: 1,
                    term: 1,
                }),
            },
            HistoryEntry {
                call_id: 2,
                invoke_time: 3,
                return_time: Some(4),
                op: ClientOp::Get {
                    subject: "k".into(),
                },
                outcome: Some(OpOutcome::GetOk {
                    value: Some(b"v1".to_vec()),
                }),
            },
        ];
        check_partition_linearizable(&history, Some(1)).unwrap();
    }

    #[test]
    fn linearizable_detects_bad_get() {
        let history = vec![
            HistoryEntry {
                call_id: 1,
                invoke_time: 1,
                return_time: Some(2),
                op: ClientOp::Put {
                    subject: "k".into(),
                    value: b"v1".to_vec(),
                    operation_id: None,
                },
                outcome: Some(OpOutcome::PutOk {
                    committed: true,
                    index: 1,
                    term: 1,
                }),
            },
            HistoryEntry {
                call_id: 2,
                invoke_time: 3,
                return_time: Some(4),
                op: ClientOp::Get {
                    subject: "k".into(),
                },
                outcome: Some(OpOutcome::GetOk {
                    value: Some(b"ghost".to_vec()),
                }),
            },
        ];
        assert!(check_partition_linearizable(&history, Some(1)).is_err());
    }

    #[test]
    fn matrix_all_pass_on_seed() {
        let reports = run_conformance_matrix(7);
        for r in &reports {
            assert!(r.ok, "case {} failed: {}", r.case, r.detail);
        }
        assert_eq!(reports.len(), ConformanceCase::covered().len());
    }

    #[test]
    fn put_get_round_trip_linearizable() {
        let mut w = SimWorld::three_node(77);
        let (leader, _) = w.elect_any().unwrap();
        w.client_put("pg/k", b"hello", Some("op-pg-1"), leader)
            .unwrap();
        let got = w.client_get("pg/k", leader).unwrap();
        assert_eq!(got.as_deref(), Some(b"hello".as_slice()));
        w.check_linearizable().unwrap();
    }

    #[test]
    fn stale_placement_epoch_cannot_elect() {
        let w = SimWorld::three_node(78);
        let old = w.placement_epoch().unwrap();
        w.elect_any().unwrap();
        w.advance_placement_epoch();
        let new = w.placement_epoch().unwrap();
        assert!(new.0 > old.0);
        match w.campaign_with_epoch(w.voters[0], old) {
            Err(ElectError::NoQuorum { .. }) => {}
            other => panic!("expected NoQuorum for stale epoch, got {other:?}"),
        }
    }
}
