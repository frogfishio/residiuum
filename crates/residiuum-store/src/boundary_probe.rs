//! Store-boundary I/O instrumentation (PQH-11).
//!
//! Records **actual** write-path observations at the store seam — append,
//! durability sync, segment rotation, visibility publication, and lifecycle
//! seal — without payloads, subjects, or estimated frame sizes.
//!
//! **Honesty (pre-qual):** when the optional sample vector is full, the probe
//! **never silently truncates**. Exact counters, latency histograms, and an
//! event-chain digest continue for every observation; `dropped_samples` and
//! coverage fields account for unsampled events explicitly.
//!
//! Default off (zero cost). Enable via [`BoundaryProbe::enable`] /
//! [`crate::Store::enable_boundary_probe`] for qualification harnesses.

use crate::durability::DurabilityMode;
use serde::{Deserialize, Serialize};

/// Kind of store-boundary I/O observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryKind {
    /// Wire-encoded frame(s) appended to the active segment buffer.
    AppendEncodedFrame,
    /// Bytes flushed from the active buffer to the segment file (write_all).
    FileWrite,
    /// Data durability barrier (`sync_data`) on the active segment file.
    FileSync,
    /// Directory sync for active-shard durability.
    DirectorySync,
    /// Segment rotated or sealed due to size / explicit seal (lifecycle).
    SegmentRotate,
    /// Visibility published into the durable projection after append (DEF-023).
    PublishVisibility,
    /// Lifecycle finalize / seal pipeline work completed for a rotated segment.
    LifecycleSeal,
    /// Item envelope CBOR encode (pre-append hot path; Mode A resolution).
    EncodeEnvelope,
    /// Pre-encode setup: ensure_active, maybe_auto_seal, id mint, envelope struct.
    PutPrep,
    /// Post-publish derived work: collection note + rate-limited checkpoint touch.
    PutPost,
}

impl BoundaryKind {
    /// Stable short name for digests and reports.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AppendEncodedFrame => "append_encoded_frame",
            Self::FileWrite => "file_write",
            Self::FileSync => "file_sync",
            Self::DirectorySync => "directory_sync",
            Self::SegmentRotate => "segment_rotate",
            Self::PublishVisibility => "publish_visibility",
            Self::LifecycleSeal => "lifecycle_seal",
            Self::EncodeEnvelope => "encode_envelope",
            Self::PutPrep => "put_prep",
            Self::PutPost => "put_post",
        }
    }

    /// Index into fixed-size counter arrays (0..KIND_COUNT).
    pub fn index(self) -> usize {
        match self {
            Self::AppendEncodedFrame => 0,
            Self::FileWrite => 1,
            Self::FileSync => 2,
            Self::DirectorySync => 3,
            Self::SegmentRotate => 4,
            Self::PublishVisibility => 5,
            Self::LifecycleSeal => 6,
            Self::EncodeEnvelope => 7,
            Self::PutPrep => 8,
            Self::PutPost => 9,
        }
    }

    /// Number of distinct kinds.
    pub const COUNT: usize = 10;

    /// All kinds in index order.
    pub const ALL: [BoundaryKind; Self::COUNT] = [
        Self::AppendEncodedFrame,
        Self::FileWrite,
        Self::FileSync,
        Self::DirectorySync,
        Self::SegmentRotate,
        Self::PublishVisibility,
        Self::LifecycleSeal,
        Self::EncodeEnvelope,
        Self::PutPrep,
        Self::PutPost,
    ];
}

/// Outcome of the boundary I/O step (not reconstructed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryOutcome {
    /// Step completed as requested.
    #[default]
    Ok,
    /// Short write (failpoint or OS short write); completed < requested.
    ShortWrite,
    /// I/O error after some or no bytes completed.
    IoError,
}

impl BoundaryOutcome {
    /// Stable short name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::ShortWrite => "short_write",
            Self::IoError => "io_error",
        }
    }
}

/// File / path role for the observed I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileRole {
    /// Active segment data file for a writer shard.
    ActiveSegment,
    /// Active-shard directory (dir sync).
    ActiveDirectory,
    /// In-memory / no file role (append buffer, publish, lifecycle flags).
    None,
}

impl FileRole {
    /// Stable short name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ActiveSegment => "active_segment",
            Self::ActiveDirectory => "active_directory",
            Self::None => "none",
        }
    }
}

/// One redacted boundary observation (no payload, no subject, no heap id).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundaryEvent {
    /// Monotonic probe sequence (process-local, starts at enable).
    pub seq: u64,
    /// Kind of store-boundary I/O observation.
    pub kind: BoundaryKind,
    /// Exact encoded frame length for appends; completed file bytes for writes.
    pub encoded_bytes: u64,
    /// Logical payload length for appends (0 for non-append events).
    pub logical_len: u64,
    /// Bytes requested for this step (write/sync size intent).
    pub requested_bytes: u64,
    /// Bytes completed for this step (may be < requested on short write/error).
    pub completed_bytes: u64,
    /// Wall duration of the actual I/O step in nanoseconds (0 if not timed).
    pub duration_ns: u64,
    /// Outcome of the step at the boundary.
    pub outcome: BoundaryOutcome,
    /// Writer shard index that performed the step.
    pub shard: u32,
    /// File role involved in the step.
    pub file_role: FileRole,
    /// Segment byte offset of the frame (append/publish); 0 otherwise.
    pub offset: u64,
    /// Opaque segment generation counter (not a product identity claim).
    pub segment_gen: u32,
    /// Durability mode applied for this step when relevant.
    pub durability: String,
    /// True when this step opened/rotated a segment relative to the prior append.
    pub segment_rotate: bool,
    /// Chunked layout flag for appends.
    pub chunked: bool,
    /// Chunk count when chunked (0 otherwise).
    pub chunk_count: u32,
}

/// Fixed-bucket latency histogram (nanoseconds), power-of-two style.
///
/// Buckets: [0,1), [1,2), [2,4), … [2^62, ∞). Exact counts; no reservoir.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyHistogram {
    /// Count per bucket (63 buckets: 0..=62). Stored as `Vec` for serde.
    pub buckets: Vec<u64>,
    /// Samples recorded into this histogram.
    pub samples: u64,
    /// Sum of observed durations (ns).
    pub sum_ns: u128,
    /// Max observed duration (ns).
    pub max_ns: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: vec![0; 63],
            samples: 0,
            sum_ns: 0,
            max_ns: 0,
        }
    }
}

impl LatencyHistogram {
    /// Record one duration sample.
    pub fn record(&mut self, duration_ns: u64) {
        if self.buckets.len() < 63 {
            self.buckets.resize(63, 0);
        }
        let idx = if duration_ns == 0 {
            0
        } else {
            (63 - duration_ns.leading_zeros() as usize).min(62)
        };
        self.buckets[idx] = self.buckets[idx].saturating_add(1);
        self.samples = self.samples.saturating_add(1);
        self.sum_ns = self.sum_ns.saturating_add(u128::from(duration_ns));
        if duration_ns > self.max_ns {
            self.max_ns = duration_ns;
        }
    }

    /// Approximate mean duration in nanoseconds (0 if empty).
    pub fn mean_ns(&self) -> f64 {
        if self.samples == 0 {
            0.0
        } else {
            self.sum_ns as f64 / self.samples as f64
        }
    }
}

/// Exact per-kind counters (never truncated).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundaryCounters {
    /// Observations by kind index ([`BoundaryKind::index`]).
    pub by_kind: Vec<u64>,
    /// Ok / ShortWrite / IoError totals.
    pub outcome_ok: u64,
    /// Short-write outcomes.
    pub outcome_short_write: u64,
    /// I/O error outcomes.
    pub outcome_io_error: u64,
    /// Sum of requested bytes across all FileWrite/FileSync/append steps.
    pub total_requested_bytes: u64,
    /// Sum of completed bytes across those steps.
    pub total_completed_bytes: u64,
    /// Sum of durations (ns) for timed I/O.
    pub total_duration_ns: u128,
}

impl Default for BoundaryCounters {
    fn default() -> Self {
        Self {
            by_kind: vec![0; BoundaryKind::COUNT],
            outcome_ok: 0,
            outcome_short_write: 0,
            outcome_io_error: 0,
            total_requested_bytes: 0,
            total_completed_bytes: 0,
            total_duration_ns: 0,
        }
    }
}

impl BoundaryCounters {
    fn record(&mut self, ev: &BoundaryEvent) {
        if self.by_kind.len() < BoundaryKind::COUNT {
            self.by_kind.resize(BoundaryKind::COUNT, 0);
        }
        let i = ev.kind.index();
        self.by_kind[i] = self.by_kind[i].saturating_add(1);
        match ev.outcome {
            BoundaryOutcome::Ok => self.outcome_ok = self.outcome_ok.saturating_add(1),
            BoundaryOutcome::ShortWrite => {
                self.outcome_short_write = self.outcome_short_write.saturating_add(1)
            }
            BoundaryOutcome::IoError => {
                self.outcome_io_error = self.outcome_io_error.saturating_add(1)
            }
        }
        self.total_requested_bytes = self
            .total_requested_bytes
            .saturating_add(ev.requested_bytes);
        self.total_completed_bytes = self
            .total_completed_bytes
            .saturating_add(ev.completed_bytes);
        self.total_duration_ns = self
            .total_duration_ns
            .saturating_add(u128::from(ev.duration_ns));
    }

    /// Count for a kind.
    pub fn count(&self, kind: BoundaryKind) -> u64 {
        self.by_kind.get(kind.index()).copied().unwrap_or(0)
    }
}

/// Explicit sample-vector coverage (no silent truncation).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BoundaryCoverage {
    /// Total observations seen while enabled (exact).
    pub total_observed: u64,
    /// Samples retained in the event vector.
    pub samples_retained: u64,
    /// Observations not retained in the vector (capacity full).
    pub samples_dropped: u64,
    /// Max capacity of the sample vector.
    pub sample_capacity: u64,
    /// True when any observation was dropped from the sample vector.
    pub sample_vector_capped: bool,
    /// Human-readable drop reason when capped.
    pub drop_reason: Option<String>,
}

/// Snapshot of probe state for harness consumption.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundarySnapshot {
    /// Sample events (bounded; may be incomplete when capped).
    pub samples: Vec<BoundaryEvent>,
    /// Exact counters for all observations.
    pub counters: BoundaryCounters,
    /// Latency histogram for FileWrite.
    pub write_latency: LatencyHistogram,
    /// Latency histogram for FileSync.
    pub sync_latency: LatencyHistogram,
    /// Latency histogram for AppendEncodedFrame (encode_frame_into into segment buffer).
    pub append_latency: LatencyHistogram,
    /// Latency histogram for EncodeEnvelope (item CBOR before append).
    pub encode_latency: LatencyHistogram,
    /// Latency histogram for PublishVisibility (dual-index apply after append).
    pub publish_latency: LatencyHistogram,
    /// Latency histogram for PutPrep (seal check, ids, env struct).
    pub prep_latency: LatencyHistogram,
    /// Latency histogram for PutPost (collection + derived checkpoint touch).
    pub post_latency: LatencyHistogram,
    /// Latency histogram for SegmentRotate seal/rotate wall time.
    pub seal_latency: LatencyHistogram,
    /// Blake3 hex digest of the full event chain (all observations, including dropped samples).
    pub event_chain_digest: String,
    /// Explicit coverage / drop accounting.
    pub coverage: BoundaryCoverage,
    /// Next seq that would be assigned.
    pub next_seq: u64,
}

/// Bounded in-memory probe attached to a [`crate::Store`].
#[derive(Debug, Clone)]
pub struct BoundaryProbe {
    enabled: bool,
    next_seq: u64,
    /// Optional sample ring for plan emission / debugging (not authoritative alone).
    samples: Vec<BoundaryEvent>,
    max_samples: usize,
    segment_gen: u32,
    counters: BoundaryCounters,
    write_latency: LatencyHistogram,
    sync_latency: LatencyHistogram,
    append_latency: LatencyHistogram,
    encode_latency: LatencyHistogram,
    publish_latency: LatencyHistogram,
    prep_latency: LatencyHistogram,
    post_latency: LatencyHistogram,
    seal_latency: LatencyHistogram,
    /// Running blake3 hasher for the full event chain.
    chain_hasher: blake3::Hasher,
    samples_dropped: u64,
    sample_vector_capped: bool,
}

impl Default for BoundaryProbe {
    fn default() -> Self {
        Self::disabled()
    }
}

impl BoundaryProbe {
    /// Default sample capacity when enabled (bounded; drops are explicit).
    pub const DEFAULT_SAMPLE_CAPACITY: usize = 4096;

    /// Disabled probe (default): all record methods are no-ops.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            next_seq: 0,
            samples: Vec::new(),
            max_samples: Self::DEFAULT_SAMPLE_CAPACITY,
            segment_gen: 0,
            counters: BoundaryCounters::default(),
            write_latency: LatencyHistogram::default(),
            sync_latency: LatencyHistogram::default(),
            append_latency: LatencyHistogram::default(),
            encode_latency: LatencyHistogram::default(),
            publish_latency: LatencyHistogram::default(),
            prep_latency: LatencyHistogram::default(),
            post_latency: LatencyHistogram::default(),
            seal_latency: LatencyHistogram::default(),
            chain_hasher: blake3::Hasher::new(),
            samples_dropped: 0,
            sample_vector_capped: false,
        }
    }

    /// Enable recording (idempotent).
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Enable with an explicit sample-vector capacity.
    pub fn enable_with_capacity(&mut self, max_samples: usize) {
        self.enabled = true;
        self.max_samples = max_samples.max(1);
    }

    /// Whether recording is active.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Borrow retained sample events (may be incomplete when capped).
    pub fn events(&self) -> &[BoundaryEvent] {
        &self.samples
    }

    /// Exact counters for all observations (including dropped samples).
    pub fn counters(&self) -> &BoundaryCounters {
        &self.counters
    }

    /// Coverage / drop accounting.
    pub fn coverage(&self) -> BoundaryCoverage {
        BoundaryCoverage {
            total_observed: self.next_seq,
            samples_retained: self.samples.len() as u64,
            samples_dropped: self.samples_dropped,
            sample_capacity: self.max_samples as u64,
            sample_vector_capped: self.sample_vector_capped,
            drop_reason: if self.sample_vector_capped {
                Some(format!(
                    "sample vector full at capacity {}; counters+histograms+chain_digest still exact",
                    self.max_samples
                ))
            } else {
                None
            },
        }
    }

    /// Hex blake3 of the full event chain so far.
    pub fn event_chain_digest(&self) -> String {
        // Finalize a copy so we can keep hashing.
        let h = self.chain_hasher.clone();
        hex_encode(h.finalize().as_bytes())
    }

    /// Snapshot for harnesses (samples + exact aggregates).
    pub fn snapshot(&self) -> BoundarySnapshot {
        BoundarySnapshot {
            samples: self.samples.clone(),
            counters: self.counters.clone(),
            write_latency: self.write_latency.clone(),
            sync_latency: self.sync_latency.clone(),
            append_latency: self.append_latency.clone(),
            encode_latency: self.encode_latency.clone(),
            publish_latency: self.publish_latency.clone(),
            prep_latency: self.prep_latency.clone(),
            post_latency: self.post_latency.clone(),
            seal_latency: self.seal_latency.clone(),
            event_chain_digest: self.event_chain_digest(),
            coverage: self.coverage(),
            next_seq: self.next_seq,
        }
    }

    /// Drain sample events only (leaves counters/histograms/digest intact).
    pub fn take_events(&mut self) -> Vec<BoundaryEvent> {
        std::mem::take(&mut self.samples)
    }

    /// Take a full snapshot and clear samples (counters/histograms reset too).
    pub fn take_snapshot(&mut self) -> BoundarySnapshot {
        let snap = self.snapshot();
        self.clear();
        snap
    }

    /// Clear samples and aggregates without disabling.
    pub fn clear(&mut self) {
        self.samples.clear();
        self.next_seq = 0;
        self.segment_gen = 0;
        self.counters = BoundaryCounters::default();
        self.write_latency = LatencyHistogram::default();
        self.sync_latency = LatencyHistogram::default();
        self.append_latency = LatencyHistogram::default();
        self.encode_latency = LatencyHistogram::default();
        self.publish_latency = LatencyHistogram::default();
        self.prep_latency = LatencyHistogram::default();
        self.post_latency = LatencyHistogram::default();
        self.seal_latency = LatencyHistogram::default();
        self.chain_hasher = blake3::Hasher::new();
        self.samples_dropped = 0;
        self.sample_vector_capped = false;
    }

    fn observe(&mut self, mut ev: BoundaryEvent) {
        if !self.enabled {
            return;
        }
        ev.seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);

        // Always update exact aggregates + chain digest.
        self.counters.record(&ev);
        match ev.kind {
            BoundaryKind::FileWrite => self.write_latency.record(ev.duration_ns),
            BoundaryKind::FileSync => self.sync_latency.record(ev.duration_ns),
            BoundaryKind::AppendEncodedFrame => self.append_latency.record(ev.duration_ns),
            BoundaryKind::EncodeEnvelope => self.encode_latency.record(ev.duration_ns),
            BoundaryKind::PublishVisibility => self.publish_latency.record(ev.duration_ns),
            BoundaryKind::PutPrep => self.prep_latency.record(ev.duration_ns),
            BoundaryKind::PutPost => self.post_latency.record(ev.duration_ns),
            BoundaryKind::SegmentRotate => self.seal_latency.record(ev.duration_ns),
            _ => {}
        }
        feed_chain(&mut self.chain_hasher, &ev);

        // Sample vector: retain until capacity; then explicit drop (no silent truncate).
        if self.samples.len() < self.max_samples {
            self.samples.push(ev);
        } else {
            self.sample_vector_capped = true;
            self.samples_dropped = self.samples_dropped.saturating_add(1);
        }
    }

    /// Record an append of wire-encoded frame bytes (post-append length).
    pub fn record_append(
        &mut self,
        encoded_bytes: u64,
        logical_len: u64,
        offset: u64,
        durability: DurabilityMode,
        segment_rotate: bool,
        chunked: bool,
        chunk_count: u32,
        duration_ns: u64,
        shard: u32,
    ) {
        if segment_rotate {
            self.segment_gen = self.segment_gen.saturating_add(1);
        }
        self.observe(BoundaryEvent {
            seq: 0,
            kind: BoundaryKind::AppendEncodedFrame,
            encoded_bytes,
            logical_len,
            requested_bytes: encoded_bytes,
            completed_bytes: encoded_bytes,
            duration_ns,
            outcome: BoundaryOutcome::Ok,
            shard,
            file_role: FileRole::None,
            offset,
            segment_gen: self.segment_gen,
            durability: durability.as_str().into(),
            segment_rotate,
            chunked,
            chunk_count,
        });
    }

    /// Record a file write of pending segment bytes at the actual boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn record_file_write(
        &mut self,
        requested_bytes: u64,
        completed_bytes: u64,
        duration_ns: u64,
        outcome: BoundaryOutcome,
        durability: DurabilityMode,
        shard: u32,
    ) {
        self.observe(BoundaryEvent {
            seq: 0,
            kind: BoundaryKind::FileWrite,
            encoded_bytes: completed_bytes,
            logical_len: 0,
            requested_bytes,
            completed_bytes,
            duration_ns,
            outcome,
            shard,
            file_role: FileRole::ActiveSegment,
            offset: 0,
            segment_gen: self.segment_gen,
            durability: durability.as_str().into(),
            segment_rotate: false,
            chunked: false,
            chunk_count: 0,
        });
    }

    /// Record a full-file sync barrier at the actual boundary.
    pub fn record_file_sync(
        &mut self,
        duration_ns: u64,
        outcome: BoundaryOutcome,
        durability: DurabilityMode,
        shard: u32,
    ) {
        self.observe(BoundaryEvent {
            seq: 0,
            kind: BoundaryKind::FileSync,
            encoded_bytes: 0,
            logical_len: 0,
            requested_bytes: 0,
            completed_bytes: 0,
            duration_ns,
            outcome,
            shard,
            file_role: FileRole::ActiveSegment,
            offset: 0,
            segment_gen: self.segment_gen,
            durability: durability.as_str().into(),
            segment_rotate: false,
            chunked: false,
            chunk_count: 0,
        });
    }

    /// Record a directory sync.
    pub fn record_directory_sync(&mut self, duration_ns: u64, shard: u32) {
        self.observe(BoundaryEvent {
            seq: 0,
            kind: BoundaryKind::DirectorySync,
            encoded_bytes: 0,
            logical_len: 0,
            requested_bytes: 0,
            completed_bytes: 0,
            duration_ns,
            outcome: BoundaryOutcome::Ok,
            shard,
            file_role: FileRole::ActiveDirectory,
            offset: 0,
            segment_gen: self.segment_gen,
            durability: DurabilityMode::Durable.as_str().into(),
            segment_rotate: false,
            chunked: false,
            chunk_count: 0,
        });
    }

    /// Record segment rotation / seal start.
    pub fn record_segment_rotate(&mut self, shard: u32) {
        self.record_segment_rotate_timed(shard, 0);
    }

    /// Record segment rotation with wall duration of the seal/rotate work.
    pub fn record_segment_rotate_timed(&mut self, shard: u32, duration_ns: u64) {
        self.segment_gen = self.segment_gen.saturating_add(1);
        self.observe(BoundaryEvent {
            seq: 0,
            kind: BoundaryKind::SegmentRotate,
            encoded_bytes: 0,
            logical_len: 0,
            requested_bytes: 0,
            completed_bytes: 0,
            duration_ns,
            outcome: BoundaryOutcome::Ok,
            shard,
            file_role: FileRole::None,
            offset: 0,
            segment_gen: self.segment_gen,
            durability: String::new(),
            segment_rotate: true,
            chunked: false,
            chunk_count: 0,
        });
    }

    /// Record put prep (ensure_active / seal check / ids / envelope subject).
    pub fn record_put_prep(&mut self, duration_ns: u64, durability: DurabilityMode, shard: u32) {
        self.observe(BoundaryEvent {
            seq: 0,
            kind: BoundaryKind::PutPrep,
            encoded_bytes: 0,
            logical_len: 0,
            requested_bytes: 0,
            completed_bytes: 0,
            duration_ns,
            outcome: BoundaryOutcome::Ok,
            shard,
            file_role: FileRole::None,
            offset: 0,
            segment_gen: self.segment_gen,
            durability: durability.as_str().into(),
            segment_rotate: false,
            chunked: false,
            chunk_count: 0,
        });
    }

    /// Record post-publish derived work.
    pub fn record_put_post(&mut self, duration_ns: u64, durability: DurabilityMode, shard: u32) {
        self.observe(BoundaryEvent {
            seq: 0,
            kind: BoundaryKind::PutPost,
            encoded_bytes: 0,
            logical_len: 0,
            requested_bytes: 0,
            completed_bytes: 0,
            duration_ns,
            outcome: BoundaryOutcome::Ok,
            shard,
            file_role: FileRole::None,
            offset: 0,
            segment_gen: self.segment_gen,
            durability: durability.as_str().into(),
            segment_rotate: false,
            chunked: false,
            chunk_count: 0,
        });
    }

    /// Record item-envelope CBOR encode (pre-append).
    pub fn record_encode_envelope(
        &mut self,
        envelope_len: u64,
        duration_ns: u64,
        durability: DurabilityMode,
        shard: u32,
    ) {
        self.observe(BoundaryEvent {
            seq: 0,
            kind: BoundaryKind::EncodeEnvelope,
            encoded_bytes: envelope_len,
            logical_len: 0,
            requested_bytes: envelope_len,
            completed_bytes: envelope_len,
            duration_ns,
            outcome: BoundaryOutcome::Ok,
            shard,
            file_role: FileRole::None,
            offset: 0,
            segment_gen: self.segment_gen,
            durability: durability.as_str().into(),
            segment_rotate: false,
            chunked: false,
            chunk_count: 0,
        });
    }

    /// Record durable visibility publication after append (timed).
    pub fn record_publish(
        &mut self,
        offset: u64,
        durability: DurabilityMode,
        shard: u32,
        duration_ns: u64,
    ) {
        self.observe(BoundaryEvent {
            seq: 0,
            kind: BoundaryKind::PublishVisibility,
            encoded_bytes: 0,
            logical_len: 0,
            requested_bytes: 0,
            completed_bytes: 0,
            duration_ns,
            outcome: BoundaryOutcome::Ok,
            shard,
            file_role: FileRole::None,
            offset,
            segment_gen: self.segment_gen,
            durability: durability.as_str().into(),
            segment_rotate: false,
            chunked: false,
            chunk_count: 0,
        });
    }

    /// Record lifecycle seal finalize for a rotated segment.
    pub fn record_lifecycle_seal(&mut self, shard: u32) {
        self.observe(BoundaryEvent {
            seq: 0,
            kind: BoundaryKind::LifecycleSeal,
            encoded_bytes: 0,
            logical_len: 0,
            requested_bytes: 0,
            completed_bytes: 0,
            duration_ns: 0,
            outcome: BoundaryOutcome::Ok,
            shard,
            file_role: FileRole::None,
            offset: 0,
            segment_gen: self.segment_gen,
            durability: String::new(),
            segment_rotate: false,
            chunked: false,
            chunk_count: 0,
        });
    }
}

fn feed_chain(h: &mut blake3::Hasher, ev: &BoundaryEvent) {
    h.update(&ev.seq.to_le_bytes());
    h.update(ev.kind.as_str().as_bytes());
    h.update(&[0]);
    h.update(&ev.requested_bytes.to_le_bytes());
    h.update(&ev.completed_bytes.to_le_bytes());
    h.update(&ev.duration_ns.to_le_bytes());
    h.update(ev.outcome.as_str().as_bytes());
    h.update(&[0]);
    h.update(&ev.shard.to_le_bytes());
    h.update(ev.file_role.as_str().as_bytes());
    h.update(&[0]);
    h.update(&ev.segment_gen.to_le_bytes());
    h.update(ev.durability.as_bytes());
    h.update(&[0]);
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_is_noop() {
        let mut p = BoundaryProbe::disabled();
        p.record_append(100, 100, 0, DurabilityMode::Durable, false, false, 0, 10, 0);
        assert!(p.events().is_empty());
        assert_eq!(p.counters().count(BoundaryKind::AppendEncodedFrame), 0);
    }

    #[test]
    fn enabled_records_kinds_and_digest() {
        let mut p = BoundaryProbe::disabled();
        p.enable();
        p.record_append(
            164,
            100,
            0,
            DurabilityMode::Buffered,
            false,
            false,
            0,
            50,
            0,
        );
        p.record_file_write(
            164,
            164,
            100,
            BoundaryOutcome::Ok,
            DurabilityMode::Buffered,
            0,
        );
        p.record_file_sync(200, BoundaryOutcome::Ok, DurabilityMode::Durable, 0);
        p.record_encode_envelope(40, 12, DurabilityMode::Buffered, 0);
        p.record_publish(0, DurabilityMode::Buffered, 0, 25);
        p.record_segment_rotate(0);
        p.record_lifecycle_seal(0);
        assert_eq!(p.events().len(), 7);
        assert_eq!(p.counters().count(BoundaryKind::AppendEncodedFrame), 1);
        assert_eq!(p.counters().count(BoundaryKind::FileWrite), 1);
        assert_eq!(p.counters().count(BoundaryKind::EncodeEnvelope), 1);
        assert_eq!(p.write_latency.samples, 1);
        assert_eq!(p.sync_latency.samples, 1);
        assert_eq!(p.encode_latency.samples, 1);
        assert_eq!(p.publish_latency.samples, 1);
        assert_eq!(p.coverage().total_observed, 7);
        assert!(!p.coverage().sample_vector_capped);
        let d1 = p.event_chain_digest();
        assert_eq!(d1.len(), 64);
        // Digest is stable for the same sequence.
        let d2 = p.event_chain_digest();
        assert_eq!(d1, d2);
    }

    #[test]
    fn sample_cap_drops_explicitly_counters_exact() {
        let mut p = BoundaryProbe::disabled();
        p.enable_with_capacity(3);
        for i in 0..10u64 {
            p.record_file_write(
                100 + i,
                100 + i,
                i * 10,
                BoundaryOutcome::Ok,
                DurabilityMode::Buffered,
                1,
            );
        }
        let cov = p.coverage();
        assert_eq!(cov.total_observed, 10);
        assert_eq!(cov.samples_retained, 3);
        assert_eq!(cov.samples_dropped, 7);
        assert!(cov.sample_vector_capped);
        assert!(cov.drop_reason.is_some());
        assert_eq!(p.counters().count(BoundaryKind::FileWrite), 10);
        assert_eq!(p.counters().total_completed_bytes, (100..110).sum::<u64>());
        assert_eq!(p.write_latency.samples, 10);
        assert_eq!(p.events().len(), 3);
        // Chain digest reflects all 10, not just samples.
        assert_eq!(p.event_chain_digest().len(), 64);
    }

    #[test]
    fn short_write_outcome_counted() {
        let mut p = BoundaryProbe::disabled();
        p.enable();
        p.record_file_write(
            1000,
            100,
            5,
            BoundaryOutcome::ShortWrite,
            DurabilityMode::Durable,
            0,
        );
        assert_eq!(p.counters().outcome_short_write, 1);
        assert_eq!(p.counters().total_requested_bytes, 1000);
        assert_eq!(p.counters().total_completed_bytes, 100);
    }
}
