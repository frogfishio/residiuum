//! Selectable L3 CPU stages — validation → encoding → integrity → chunking →
//! manifest → index prep. No filesystem I/O.

use super::residual::{residual_from_stage_ns, ResidualReport};
use super::sink::BoundedSink;
use super::timeline::{check_timeline, TimelineEvent};
use super::PipelineError;
use crate::workload::{fill_payload, generate_key, PayloadProfile};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Canonical L3 stage order (must match SPEC / registry residual path).
pub const STAGE_ORDER: &[StageId] = &[
    StageId::Validation,
    StageId::Encoding,
    StageId::Integrity,
    StageId::Chunking,
    StageId::Manifest,
    StageId::IndexPrep,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageId {
    Validation,
    Encoding,
    Integrity,
    Chunking,
    Manifest,
    IndexPrep,
}

impl StageId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Validation => "validation",
            Self::Encoding => "encoding",
            Self::Integrity => "integrity",
            Self::Chunking => "chunking",
            Self::Manifest => "manifest",
            Self::IndexPrep => "index_prep",
        }
    }

    pub fn order_index(self) -> usize {
        STAGE_ORDER
            .iter()
            .position(|s| *s == self)
            .unwrap_or(usize::MAX)
    }
}

/// Which stages are enabled (independently selectable).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageSet {
    pub validation: bool,
    pub encoding: bool,
    pub integrity: bool,
    pub chunking: bool,
    pub manifest: bool,
    pub index_prep: bool,
}

impl Default for StageSet {
    fn default() -> Self {
        Self {
            validation: true,
            encoding: true,
            integrity: true,
            chunking: true,
            manifest: true,
            index_prep: false,
        }
    }
}

impl StageSet {
    pub fn enabled_in_order(&self) -> Vec<StageId> {
        let mut v = Vec::new();
        for s in STAGE_ORDER {
            let on = match s {
                StageId::Validation => self.validation,
                StageId::Encoding => self.encoding,
                StageId::Integrity => self.integrity,
                StageId::Chunking => self.chunking,
                StageId::Manifest => self.manifest,
                StageId::IndexPrep => self.index_prep,
            };
            if on {
                v.push(*s);
            }
        }
        v
    }

    /// Reject configs that list stages out of canonical order when provided as a list.
    pub fn from_ordered_list(list: &[StageId]) -> Result<Self, PipelineError> {
        let mut last = 0usize;
        for s in list {
            let idx = s.order_index();
            if idx < last {
                return Err(PipelineError::StageOrder(format!(
                    "stage {} out of order",
                    s.as_str()
                )));
            }
            last = idx;
        }
        let mut set = StageSet {
            validation: false,
            encoding: false,
            integrity: false,
            chunking: false,
            manifest: false,
            index_prep: false,
        };
        for s in list {
            match s {
                StageId::Validation => set.validation = true,
                StageId::Encoding => set.encoding = true,
                StageId::Integrity => set.integrity = true,
                StageId::Chunking => set.chunking = true,
                StageId::Manifest => set.manifest = true,
                StageId::IndexPrep => set.index_prep = true,
            }
        }
        Ok(set)
    }
}

/// Injected delays (ns proxy work units) for attribution tests.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InjectedDelays {
    pub queue_ns: u64,
    pub lock_ns: u64,
    pub cpu_burn_ns: u64,
    /// Extra burn applied only to a named stage.
    pub stage_extra: Vec<(StageId, u64)>,
}

impl InjectedDelays {
    fn for_stage(&self, stage: StageId) -> u64 {
        self.stage_extra
            .iter()
            .find(|(s, _)| *s == stage)
            .map(|(_, n)| *n)
            .unwrap_or(0)
            .saturating_add(self.cpu_burn_ns)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L3Config {
    pub seed: u64,
    pub ops: u64,
    pub payload_len: usize,
    pub producers: u32,
    pub stages: StageSet,
    pub delays: InjectedDelays,
    /// Memory sink capacity (0 = null).
    pub sink_cap: usize,
}

impl Default for L3Config {
    fn default() -> Self {
        Self {
            seed: 1,
            ops: 100,
            payload_len: 1024,
            producers: 1,
            stages: StageSet::default(),
            delays: InjectedDelays::default(),
            sink_cap: 4096,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L3Report {
    pub schema: String,
    pub layer: String,
    pub ops: u64,
    pub payload_len: usize,
    pub producers: u32,
    pub stages_enabled: Vec<String>,
    pub stage_ns: Vec<(String, u64)>,
    pub queue_ns: u64,
    pub lock_ns: u64,
    pub e2e_ns: u64,
    pub residual: ResidualReport,
    pub output_digest_hex: String,
    pub sink_bytes: u64,
    pub filesystem_touched: bool,
    pub timeline_ok: bool,
    pub validity: String,
    pub messages: Vec<String>,
    /// Throughput proxy: logical bytes / e2e_ns * 1e9
    pub bytes_per_sec_proxy: f64,
}

/// Run L3 pipeline. Uses synthetic ns work units for determinism in tests.
pub fn run_l3_pipeline(cfg: &L3Config) -> Result<L3Report, PipelineError> {
    let enabled = cfg.stages.enabled_in_order();
    if enabled.is_empty() {
        return Err(PipelineError::Msg("no stages enabled".into()));
    }

    let mut sink = if cfg.sink_cap == 0 {
        BoundedSink::null()
    } else {
        BoundedSink::memory(cfg.sink_cap)
    };

    let mut stage_ns: Vec<(StageId, u64)> = enabled.iter().map(|s| (*s, 0u64)).collect();
    let mut queue_ns = 0u64;
    let mut lock_ns = 0u64;
    let mut e2e_ns = 0u64;
    let mut events: Vec<TimelineEvent> = Vec::new();
    let mut t = 0u64;
    let mut messages = Vec::new();

    // Timed interval — no FS.
    for seq in 0..cfg.ops {
        // queue delay (admission)
        if cfg.delays.queue_ns > 0 {
            burn(cfg.delays.queue_ns);
            queue_ns = queue_ns.saturating_add(cfg.delays.queue_ns);
            e2e_ns = e2e_ns.saturating_add(cfg.delays.queue_ns);
            t = t.saturating_add(cfg.delays.queue_ns);
        }

        // lock delay
        if cfg.delays.lock_ns > 0 {
            burn(cfg.delays.lock_ns);
            lock_ns = lock_ns.saturating_add(cfg.delays.lock_ns);
            e2e_ns = e2e_ns.saturating_add(cfg.delays.lock_ns);
            t = t.saturating_add(cfg.delays.lock_ns);
        }

        let key = generate_key(cfg.seed, seq);
        let mut buf = vec![0u8; cfg.payload_len.max(1)];
        fill_payload(cfg.seed, seq, 0, PayloadProfile::Incompressible, &mut buf);

        for (stage, acc) in stage_ns.iter_mut() {
            let start = t;
            events.push(TimelineEvent {
                seq,
                stage: stage.as_str().into(),
                t_ns: start,
                kind: "enter".into(),
            });

            let cost = run_stage(*stage, &key, &buf, &mut sink, cfg.delays.for_stage(*stage));
            *acc = acc.saturating_add(cost);
            e2e_ns = e2e_ns.saturating_add(cost);
            t = t.saturating_add(cost);

            events.push(TimelineEvent {
                seq,
                stage: stage.as_str().into(),
                t_ns: t,
                kind: "exit".into(),
            });
        }

        // Producer count scales CPU work proportionally (ceiling model).
        if cfg.producers > 1 {
            let scale = cfg.producers.saturating_sub(1) as u64;
            let extra = (cfg.payload_len as u64 / 64 + 1).saturating_mul(scale);
            burn(extra);
            e2e_ns = e2e_ns.saturating_add(extra);
            t = t.saturating_add(extra);
        }
    }

    let timeline = check_timeline(&events);
    let stage_map: Vec<(String, u64)> = stage_ns
        .iter()
        .map(|(s, n)| (s.as_str().into(), *n))
        .collect();
    let stage_sum: u64 = stage_ns.iter().map(|(_, n)| *n).sum();
    // Residual relative to e2e after queue+lock+stages.
    let residual = residual_from_stage_ns(
        e2e_ns,
        stage_sum.saturating_add(queue_ns).saturating_add(lock_ns),
    );

    let mut validity = "valid".to_string();
    if sink.filesystem_touched() {
        validity = "invalid_correctness".into();
        messages.push("filesystem touched during L3".into());
    }
    if !timeline.ok {
        validity = "invalid_instrumentation".into();
        messages.push(format!("timeline violations: {:?}", timeline.violations));
    }

    let logical = cfg.ops.saturating_mul(cfg.payload_len as u64);
    let bps = if e2e_ns == 0 {
        0.0
    } else {
        (logical as f64) * 1_000_000_000.0 / (e2e_ns as f64)
    };

    messages.push("L3 CPU ceiling (no storage wait)".into());
    if !residual.attribution_complete {
        messages.push("stage residual exceeds 5%".into());
    }

    Ok(L3Report {
        schema: "residiuum-pqh6-l3-report-v1".into(),
        layer: "L3".into(),
        ops: cfg.ops,
        payload_len: cfg.payload_len,
        producers: cfg.producers,
        stages_enabled: enabled.iter().map(|s| s.as_str().into()).collect(),
        stage_ns: stage_map,
        queue_ns,
        lock_ns,
        e2e_ns,
        residual,
        output_digest_hex: sink.digest_hex(),
        sink_bytes: sink.total_written(),
        filesystem_touched: sink.filesystem_touched(),
        timeline_ok: timeline.ok,
        validity,
        messages,
        bytes_per_sec_proxy: bps,
    })
}

fn run_stage(
    stage: StageId,
    key: &[u8],
    payload: &[u8],
    sink: &mut BoundedSink,
    extra_ns: u64,
) -> u64 {
    let mut cost = 1u64 + extra_ns;
    burn(extra_ns);

    match stage {
        StageId::Validation => {
            // Validate key non-empty and payload length.
            if key.is_empty() || payload.is_empty() {
                cost = cost.saturating_add(1);
            }
            cost = cost.saturating_add(key.len() as u64 / 8 + 1);
            burn(key.len() as u64 / 8 + 1);
        }
        StageId::Encoding => {
            // Opaque frame header + payload to sink (CPU transform model).
            let mut frame = Vec::with_capacity(8 + payload.len());
            frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            frame.extend_from_slice(&(key.len() as u32).to_le_bytes());
            frame.extend_from_slice(payload);
            sink.write(&frame);
            cost = cost.saturating_add(frame.len() as u64 / 16 + 1);
            burn(frame.len() as u64 / 16 + 1);
        }
        StageId::Integrity => {
            let mut h = Sha256::new();
            h.update(key);
            h.update(payload);
            let dig = h.finalize();
            sink.write(&dig);
            cost = cost.saturating_add(32 + payload.len() as u64 / 32);
            burn(32 + payload.len() as u64 / 32);
        }
        StageId::Chunking => {
            const CHUNK: usize = 4096;
            let mut off = 0usize;
            while off < payload.len() {
                let end = (off + CHUNK).min(payload.len());
                sink.write(&payload[off..end]);
                cost = cost.saturating_add(1 + (end - off) as u64 / 64);
                burn(1 + (end - off) as u64 / 64);
                off = end;
            }
        }
        StageId::Manifest => {
            // Manifest entry: size + digest of payload (opaque).
            let mut h = Sha256::new();
            h.update(payload);
            let dig = h.finalize();
            let mut entry = Vec::with_capacity(40);
            entry.extend_from_slice(&(payload.len() as u64).to_le_bytes());
            entry.extend_from_slice(&dig);
            sink.write(&entry);
            cost = cost.saturating_add(40);
            burn(40);
        }
        StageId::IndexPrep => {
            // Index key hash only — no store mutation.
            let mut h = Sha256::new();
            h.update(key);
            sink.write(&h.finalize());
            cost = cost.saturating_add(16 + key.len() as u64);
            burn(16 + key.len() as u64);
        }
    }
    cost
}

/// Deterministic busy-work proportional to `units` (not wall clock).
fn burn(units: u64) {
    let mut x = 0u64;
    let n = units.min(10_000);
    for i in 0..n {
        x = x.wrapping_add(i.wrapping_mul(0x9E37_79B9));
    }
    std::hint::black_box(x);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_stream_same_digest() {
        let cfg = L3Config {
            ops: 20,
            payload_len: 128,
            ..L3Config::default()
        };
        let a = run_l3_pipeline(&cfg).unwrap();
        let b = run_l3_pipeline(&cfg).unwrap();
        assert_eq!(a.output_digest_hex, b.output_digest_hex);
        assert!(!a.filesystem_touched);
        assert_eq!(a.layer, "L3");
    }

    #[test]
    fn no_filesystem_in_l3() {
        let r = run_l3_pipeline(&L3Config::default()).unwrap();
        assert!(!r.filesystem_touched);
        assert_eq!(r.validity, "valid");
    }

    #[test]
    fn injected_cpu_increases_stage() {
        let base = run_l3_pipeline(&L3Config {
            ops: 10,
            delays: InjectedDelays::default(),
            ..L3Config::default()
        })
        .unwrap();
        let mut delays = InjectedDelays::default();
        delays.stage_extra.push((StageId::Encoding, 5000));
        let hot = run_l3_pipeline(&L3Config {
            ops: 10,
            delays,
            ..L3Config::default()
        })
        .unwrap();
        let base_enc = base
            .stage_ns
            .iter()
            .find(|(n, _)| n == "encoding")
            .map(|(_, v)| *v)
            .unwrap_or(0);
        let hot_enc = hot
            .stage_ns
            .iter()
            .find(|(n, _)| n == "encoding")
            .map(|(_, v)| *v)
            .unwrap_or(0);
        assert!(hot_enc > base_enc);
        assert!(hot.e2e_ns > base.e2e_ns);
    }

    #[test]
    fn queue_and_lock_distinguished() {
        let mut d = InjectedDelays::default();
        d.queue_ns = 100;
        d.lock_ns = 200;
        let r = run_l3_pipeline(&L3Config {
            ops: 5,
            delays: d,
            ..L3Config::default()
        })
        .unwrap();
        assert_eq!(r.queue_ns, 500); // 5 * 100
        assert_eq!(r.lock_ns, 1000); // 5 * 200
    }

    #[test]
    fn reordered_stages_rejected() {
        let err = StageSet::from_ordered_list(&[StageId::Encoding, StageId::Validation]);
        assert!(matches!(err, Err(PipelineError::StageOrder(_))));
    }

    #[test]
    fn residual_closes_on_fixture() {
        // When e2e is only stage sum (no queue/lock), residual ~ 0.
        let r = run_l3_pipeline(&L3Config {
            ops: 15,
            delays: InjectedDelays::default(),
            stages: StageSet {
                validation: true,
                encoding: true,
                integrity: true,
                chunking: false,
                manifest: false,
                index_prep: false,
            },
            ..L3Config::default()
        })
        .unwrap();
        // residual uses e2e vs stages+queue+lock; should be small.
        assert!(
            r.residual.residual_fraction.unwrap_or(1.0) <= 0.05 + 1e-9
                || r.residual.attribution_complete,
            "residual={:?}",
            r.residual
        );
    }
}
