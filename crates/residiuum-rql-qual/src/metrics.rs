//! §7.4 metric envelopes + collectors (Q4.3).

use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

/// Latency quantiles in nanoseconds.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LatencyQuantilesNs {
    pub p50: Option<u64>,
    pub p95: Option<u64>,
    pub p99: Option<u64>,
    pub max: Option<u64>,
    pub samples: u64,
}

/// Resource snapshot for one cell / repetition.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceSnapshot {
    pub cpu_time_ns: Option<u64>,
    pub rss_bytes: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
    pub physical_bytes_read: Option<u64>,
    pub physical_bytes_written: Option<u64>,
    pub read_amplification: Option<f64>,
}

/// Monotonic process counters used to derive one measured workload interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessResourceCounters {
    pub cpu_time_ns: u64,
    pub rss_bytes: u64,
    pub physical_bytes_read: u64,
    pub physical_bytes_written: u64,
}

/// In-process sampler for one measured workload interval.
///
/// Sampling is deliberately independent of `ps` and unsafe application code.
/// RSS/I/O come from the safe process view; accumulated CPU comes from the
/// POSIX process CPU clock through Rustix's safe interface.
pub struct ProcessResourceSampler {
    start: ProcessResourceCounters,
    peak_rss: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl ProcessResourceSampler {
    /// Start a 1 ms RSS sampler after workload warm-up.
    pub fn start() -> Option<Self> {
        let start = try_process_resource_counters()?;
        let peak_rss = Arc::new(AtomicU64::new(start.rss_bytes));
        let stop = Arc::new(AtomicBool::new(false));
        let sample_peak = Arc::clone(&peak_rss);
        let sample_stop = Arc::clone(&stop);
        let thread = std::thread::Builder::new()
            .name("rql-q4-resource-sampler".into())
            .spawn(move || {
                while !sample_stop.load(Ordering::Relaxed) {
                    if let Some(sample) = try_process_resource_counters() {
                        sample_peak.fetch_max(sample.rss_bytes, Ordering::Relaxed);
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            })
            .ok()?;
        Some(Self {
            start,
            peak_rss,
            stop,
            thread: Some(thread),
        })
    }

    /// Stop sampling and return interval deltas plus the sampled RSS peak.
    pub fn finish(mut self) -> Option<ResourceSnapshot> {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let end = try_process_resource_counters()?;
        self.peak_rss.fetch_max(end.rss_bytes, Ordering::Relaxed);
        Some(ResourceSnapshot {
            cpu_time_ns: Some(end.cpu_time_ns.saturating_sub(self.start.cpu_time_ns)),
            rss_bytes: Some(end.rss_bytes),
            peak_rss_bytes: Some(self.peak_rss.load(Ordering::Relaxed)),
            physical_bytes_read: Some(
                end.physical_bytes_read
                    .saturating_sub(self.start.physical_bytes_read),
            ),
            physical_bytes_written: Some(
                end.physical_bytes_written
                    .saturating_sub(self.start.physical_bytes_written),
            ),
            read_amplification: None,
        })
    }
}

impl Drop for ProcessResourceSampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Query-path accounting.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueryPathMetrics {
    pub documents_examined: Option<u64>,
    /// Serialized JSON payload bytes loaded by the QVM for this interval.
    pub logical_bytes_examined: Option<u64>,
    pub index_entries_examined: Option<u64>,
    pub index_size_bytes: Option<u64>,
    pub index_build_ns: Option<u64>,
    pub indexed_write_penalty_ns: Option<u64>,
    pub explain_plan_digest: Option<String>,
}

/// Cache / lifecycle class label (programme §7.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleClass {
    WarmSteady,
    FreshReopen,
    LargerThanMemory,
    ReadOnly,
    ConcurrentWrites,
    RotationCompaction,
    DeclaredDamage,
}

impl LifecycleClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WarmSteady => "warm_steady",
            Self::FreshReopen => "fresh_reopen",
            Self::LargerThanMemory => "larger_than_memory",
            Self::ReadOnly => "read_only",
            Self::ConcurrentWrites => "concurrent_writes",
            Self::RotationCompaction => "rotation_compaction",
            Self::DeclaredDamage => "declared_damage",
        }
    }
}

/// Full per-cell metrics envelope (programme §7.4 list).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CellMetrics {
    pub queries_per_s: Option<f64>,
    pub latency: LatencyQuantilesNs,
    pub resource: ResourceSnapshot,
    pub path: QueryPathMetrics,
    pub lifecycle: Option<LifecycleClass>,
    pub cold_method: Option<String>,
    pub deferred_work_units: Option<u64>,
    pub deferred_drained: Option<bool>,
    pub result_digest_echo: Option<String>,
    pub coverage_complete: Option<bool>,
    /// Validity flag: digests present and coverage consistent with policy.
    pub validity_ok: Option<bool>,
}

/// Required metric field names for evidence completeness checks.
pub const REQUIRED_METRIC_KEYS: &[&str] = &[
    "result_digest",
    "coverage",
    "validity",
    "queries_per_s",
    "latency_p50_p95_p99_max",
    "cpu_rss",
    "physical_bytes_rw_amplification",
    "docs_index_examined",
    "index_size_build_write_penalty",
    "explain_plan",
    "cache_lifecycle_state",
    "deferred_work_drain",
];

/// Collect latency samples and compute quantiles.
#[derive(Debug, Clone, Default)]
pub struct LatencyCollector {
    samples_ns: Vec<u64>,
}

impl LatencyCollector {
    pub fn new() -> Self {
        Self {
            samples_ns: Vec::new(),
        }
    }

    pub fn record_ns(&mut self, ns: u64) {
        self.samples_ns.push(ns);
    }

    pub fn record_duration(&mut self, d: Duration) {
        self.record_ns(d.as_nanos().min(u128::from(u64::MAX)) as u64);
    }

    pub fn quantiles(&self) -> LatencyQuantilesNs {
        if self.samples_ns.is_empty() {
            return LatencyQuantilesNs::default();
        }
        let mut s = self.samples_ns.clone();
        s.sort_unstable();
        let n = s.len();
        let pick = |p: f64| -> u64 {
            let idx = ((p * (n as f64 - 1.0)).round() as usize).min(n - 1);
            s[idx]
        };
        LatencyQuantilesNs {
            p50: Some(pick(0.50)),
            p95: Some(pick(0.95)),
            p99: Some(pick(0.99)),
            max: Some(*s.last().unwrap()),
            samples: n as u64,
        }
    }

    pub fn mean_ns(&self) -> Option<f64> {
        if self.samples_ns.is_empty() {
            return None;
        }
        let sum: u128 = self.samples_ns.iter().map(|&x| x as u128).sum();
        Some(sum as f64 / self.samples_ns.len() as f64)
    }
}

/// Wall-clock timer for one query attempt.
pub struct QueryTimer {
    start: Instant,
}

impl QueryTimer {
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    pub fn elapsed_ns(&self) -> u64 {
        self.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
    }
}

/// Best-effort safe in-process resource counters.
pub fn try_process_resource_counters() -> Option<ProcessResourceCounters> {
    use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};

    let pid = sysinfo::get_current_pid().ok()?;
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::new().with_memory().with_disk_usage(),
    );
    let process = system.process(pid)?;
    let disk = process.disk_usage();
    Some(ProcessResourceCounters {
        cpu_time_ns: process_cpu_time_ns()?,
        rss_bytes: process.memory(),
        physical_bytes_read: disk.total_read_bytes,
        physical_bytes_written: disk.total_written_bytes,
    })
}

#[cfg(unix)]
fn process_cpu_time_ns() -> Option<u64> {
    let value = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    let seconds = u64::try_from(value.tv_sec).ok()?;
    let nanos = u64::try_from(value.tv_nsec).ok()?;
    Some(seconds.saturating_mul(1_000_000_000).saturating_add(nanos))
}

#[cfg(not(unix))]
fn process_cpu_time_ns() -> Option<u64> {
    None
}

/// Best-effort current RSS; retained for non-campaign metric envelopes.
pub fn try_rss_bytes() -> Option<u64> {
    try_process_resource_counters().map(|sample| sample.rss_bytes)
}

/// Build cell metrics from latency samples + optional path/resource fields.
pub fn assemble_metrics(
    lat: &LatencyCollector,
    path: QueryPathMetrics,
    lifecycle: Option<LifecycleClass>,
    cold_method: Option<String>,
    result_digest: Option<String>,
    coverage_complete: Option<bool>,
    deferred_drained: Option<bool>,
) -> CellMetrics {
    let latency = lat.quantiles();
    let queries_per_s = lat.mean_ns().map(|mean| {
        if mean <= 0.0 {
            0.0
        } else {
            1_000_000_000.0 / mean
        }
    });
    let rss = try_rss_bytes();
    let validity_ok = result_digest.is_some() && coverage_complete.is_some();
    CellMetrics {
        queries_per_s,
        latency,
        resource: ResourceSnapshot {
            cpu_time_ns: None, // wall latency is primary; CPU residual host-specific
            rss_bytes: rss,
            // A single resident snapshot is not a sampled peak.
            peak_rss_bytes: None,
            physical_bytes_read: None,
            physical_bytes_written: None,
            read_amplification: None,
        },
        path,
        lifecycle,
        cold_method,
        deferred_work_units: Some(0),
        deferred_drained,
        result_digest_echo: result_digest,
        coverage_complete,
        validity_ok: Some(validity_ok),
    }
}

/// Presence classification for a required §7.4 metric key.
///
/// Missing instrumentation must **not** be reported as present. Residual and
/// not-supported keys are honest gaps: scaffold smoke may still publish them;
/// competitive completeness fails until they are `Present` or a principal
/// residual waiver is recorded outside this check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricPresenceState {
    /// Measured value is populated in the envelope.
    Present,
    /// Known instrumentation gap (store/host probes not wired yet).
    Residual,
    /// Platform or engine cannot provide this metric.
    NotSupported,
}

impl MetricPresenceState {
    pub fn is_present(self) -> bool {
        matches!(self, Self::Present)
    }

    /// Competitive cells require a measured value (no silent residual).
    pub fn competitive_ok(self) -> bool {
        self.is_present()
    }

    /// Scaffold publication may include residual / not-supported flags.
    pub fn scaffold_ok(self) -> bool {
        matches!(self, Self::Present | Self::Residual | Self::NotSupported)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Residual => "residual",
            Self::NotSupported => "not_supported",
        }
    }
}

/// Keys that stay residual until store/host probes or explain plumbing land.
/// Documented for Q4/Q5 honesty — do not treat as competitive-present.
pub const RESIDUAL_UNTIL_PROBES_KEYS: &[&str] = &[
    "cpu_rss",
    "physical_bytes_rw_amplification",
    "index_size_build_write_penalty",
    "explain_plan",
];

/// Human-readable residual notes for evidence / principal review.
pub const RESIDUAL_METRIC_NOTES: &[(&str, &str)] = &[
    (
        "cpu_rss",
        "RSS and 1 ms sampled peak RSS are collected in-process; accumulated short-interval CPU time remains residual",
    ),
    (
        "physical_bytes_rw_amplification",
        "Physical read/write interval deltas are collected in-process; logical-byte read amplification remains residual",
    ),
    (
        "index_size_build_write_penalty",
        "Index size/build/write-penalty residual until index accounting probes",
    ),
    (
        "explain_plan",
        "Explain plan digest residual until adapters echo executed plan hash",
    ),
];

fn present_or_residual(has_value: bool) -> MetricPresenceState {
    if has_value {
        MetricPresenceState::Present
    } else {
        MetricPresenceState::Residual
    }
}

fn present_or_absent_core(has_value: bool) -> MetricPresenceState {
    // Core correctness / latency keys have no residual waiver in-tree:
    // missing means Residual for scaffold visibility, but competitive fails.
    present_or_residual(has_value)
}

/// Per-key presence for every required §7.4 metric (never unconditional present).
pub fn metric_key_presence(m: &CellMetrics) -> Vec<(String, MetricPresenceState)> {
    vec![
        (
            "result_digest".into(),
            present_or_absent_core(m.result_digest_echo.is_some()),
        ),
        (
            "coverage".into(),
            present_or_absent_core(m.coverage_complete.is_some()),
        ),
        (
            "validity".into(),
            present_or_absent_core(m.validity_ok.is_some()),
        ),
        (
            "queries_per_s".into(),
            present_or_absent_core(m.queries_per_s.is_some()),
        ),
        (
            "latency_p50_p95_p99_max".into(),
            present_or_absent_core(m.latency.samples > 0 && m.latency.p50.is_some()),
        ),
        (
            "cpu_rss".into(),
            present_or_residual(
                m.resource.cpu_time_ns.is_some()
                    && m.resource.rss_bytes.is_some()
                    && m.resource.peak_rss_bytes.is_some(),
            ),
        ),
        (
            "physical_bytes_rw_amplification".into(),
            present_or_residual(
                m.resource.physical_bytes_read.is_some()
                    && m.resource.physical_bytes_written.is_some()
                    && m.resource.read_amplification.is_some(),
            ),
        ),
        (
            "docs_index_examined".into(),
            present_or_absent_core(
                m.path.documents_examined.is_some() || m.path.index_entries_examined.is_some(),
            ),
        ),
        (
            "index_size_build_write_penalty".into(),
            present_or_residual(
                m.path.index_size_bytes.is_some()
                    || m.path.index_build_ns.is_some()
                    || m.path.indexed_write_penalty_ns.is_some(),
            ),
        ),
        (
            "explain_plan".into(),
            present_or_residual(m.path.explain_plan_digest.is_some()),
        ),
        (
            "cache_lifecycle_state".into(),
            present_or_absent_core(m.lifecycle.is_some()),
        ),
        (
            "deferred_work_drain".into(),
            present_or_absent_core(m.deferred_drained.is_some()),
        ),
    ]
}

/// Competitive completeness: every required §7.4 key must be `Present`.
/// Residual / not_supported without an external principal waiver ⇒ fail.
pub fn metrics_competitive_complete(m: &CellMetrics) -> bool {
    metric_key_presence(m)
        .iter()
        .all(|(_, state)| state.competitive_ok())
}

/// Scaffold smoke may publish when residual-class keys are Residual and core
/// measured keys (digest/latency/path basics) are Present.
pub fn metrics_scaffold_publishable(m: &CellMetrics) -> bool {
    let presence = metric_key_presence(m);
    if !presence.iter().all(|(_, s)| s.scaffold_ok()) {
        return false;
    }
    // Core keys that scaffold still needs filled (not residual-class).
    const CORE: &[&str] = &[
        "result_digest",
        "coverage",
        "validity",
        "queries_per_s",
        "latency_p50_p95_p99_max",
        "docs_index_examined",
        "cache_lifecycle_state",
        "deferred_work_drain",
    ];
    for key in CORE {
        match presence.iter().find(|(k, _)| k == key) {
            Some((_, MetricPresenceState::Present)) => {}
            _ => return false,
        }
    }
    true
}

/// Keys that are residual or not_supported (for evidence notes).
pub fn metric_residual_keys(m: &CellMetrics) -> Vec<String> {
    metric_key_presence(m)
        .into_iter()
        .filter(|(_, s)| !s.is_present())
        .map(|(k, _)| k)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_process_resource_sampler_reports_rss_and_io_counters() {
        let sampler = ProcessResourceSampler::start().expect("self process probe");
        let allocation = vec![0x5au8; 256 * 1024];
        std::hint::black_box(&allocation);
        std::thread::sleep(Duration::from_millis(3));
        let snapshot = sampler.finish().expect("finish self process probe");
        assert!(snapshot.rss_bytes.unwrap_or(0) > 0);
        assert!(snapshot.peak_rss_bytes.unwrap_or(0) >= snapshot.rss_bytes.unwrap_or(0));
        assert!(snapshot.physical_bytes_read.is_some());
        assert!(snapshot.physical_bytes_written.is_some());
        assert!(
            snapshot.cpu_time_ns.is_some(),
            "CPU interval must be sampled"
        );
    }

    #[test]
    fn required_keys_cover_programme_list() {
        assert!(REQUIRED_METRIC_KEYS.len() >= 10);
        assert!(REQUIRED_METRIC_KEYS.contains(&"result_digest"));
        assert!(REQUIRED_METRIC_KEYS.contains(&"latency_p50_p95_p99_max"));
        for key in RESIDUAL_UNTIL_PROBES_KEYS {
            assert!(
                REQUIRED_METRIC_KEYS.contains(key),
                "residual key {key} must stay on required list"
            );
        }
    }

    #[test]
    fn latency_quantiles_sorted() {
        let mut c = LatencyCollector::new();
        for ns in [100u64, 200, 300, 400, 500, 600, 700, 800, 900, 1000] {
            c.record_ns(ns);
        }
        let q = c.quantiles();
        assert_eq!(q.samples, 10);
        assert_eq!(q.p50, Some(600)); // round 0.5*(9)=4.5→5 → 600 (0-index 5)
        assert_eq!(q.max, Some(1000));
        assert!(q.p95.unwrap() >= q.p50.unwrap());
    }

    #[test]
    fn assemble_sets_qps_and_validity() {
        let mut c = LatencyCollector::new();
        c.record_ns(1_000_000); // 1ms
        let m = assemble_metrics(
            &c,
            QueryPathMetrics {
                documents_examined: Some(10),
                ..Default::default()
            },
            Some(LifecycleClass::WarmSteady),
            Some("not_cold_warm_steady".into()),
            Some("abc".into()),
            Some(true),
            Some(true),
        );
        assert!(m.queries_per_s.unwrap() > 0.0);
        assert_eq!(m.validity_ok, Some(true));
        let presence = metric_key_presence(&m);
        assert!(presence
            .iter()
            .any(|(k, s)| { k == "result_digest" && *s == MetricPresenceState::Present }));
    }

    #[test]
    fn no_unconditional_present_on_empty_residual_keys() {
        let m = CellMetrics::default();
        let presence = metric_key_presence(&m);
        for key in RESIDUAL_UNTIL_PROBES_KEYS {
            let state = presence
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, s)| *s)
                .expect(key);
            assert_eq!(
                state,
                MetricPresenceState::Residual,
                "{key} must be residual when empty, not present"
            );
        }
        // Never claim competitive-complete on empty envelope.
        assert!(!metrics_competitive_complete(&m));
        assert!(!metrics_scaffold_publishable(&m));
    }

    #[test]
    fn residual_vs_present_and_competitive_gate() {
        let mut c = LatencyCollector::new();
        c.record_ns(500_000);
        let mut m = assemble_metrics(
            &c,
            QueryPathMetrics {
                documents_examined: Some(3),
                ..Default::default()
            },
            Some(LifecycleClass::WarmSteady),
            Some("not_cold_warm_steady".into()),
            Some("digest".into()),
            Some(true),
            Some(true),
        );

        // Typical scaffold assemble: residual-class keys empty → Residual.
        let presence = metric_key_presence(&m);
        assert_eq!(
            presence.iter().find(|(k, _)| k == "cpu_rss").unwrap().1,
            MetricPresenceState::Residual
        );
        assert_eq!(
            presence
                .iter()
                .find(|(k, _)| k == "physical_bytes_rw_amplification")
                .unwrap()
                .1,
            MetricPresenceState::Residual
        );
        assert_eq!(
            presence
                .iter()
                .find(|(k, _)| k == "index_size_build_write_penalty")
                .unwrap()
                .1,
            MetricPresenceState::Residual
        );
        assert_eq!(
            presence
                .iter()
                .find(|(k, _)| k == "explain_plan")
                .unwrap()
                .1,
            MetricPresenceState::Residual
        );
        assert!(metrics_scaffold_publishable(&m));
        assert!(
            !metrics_competitive_complete(&m),
            "competitive completeness must fail while residual keys are empty"
        );

        // Fill residual-class fields → competitive complete.
        m.resource.rss_bytes = Some(1_048_576);
        m.resource.peak_rss_bytes = Some(1_048_576);
        m.resource.cpu_time_ns = Some(100);
        m.resource.physical_bytes_read = Some(4096);
        m.resource.physical_bytes_written = Some(0);
        m.resource.read_amplification = Some(1.0);
        m.path.index_size_bytes = Some(128);
        m.path.index_build_ns = Some(10);
        m.path.indexed_write_penalty_ns = Some(1);
        m.path.explain_plan_digest = Some("plan_abc".into());

        let presence = metric_key_presence(&m);
        for key in RESIDUAL_UNTIL_PROBES_KEYS {
            assert_eq!(
                presence.iter().find(|(k, _)| k == key).unwrap().1,
                MetricPresenceState::Present,
                "{key} should be present when populated"
            );
        }
        assert!(metrics_competitive_complete(&m));
        assert!(metrics_scaffold_publishable(&m));
        assert!(metric_residual_keys(&m).is_empty());
    }

    #[test]
    fn residual_notes_cover_probe_keys() {
        for key in RESIDUAL_UNTIL_PROBES_KEYS {
            assert!(
                RESIDUAL_METRIC_NOTES.iter().any(|(k, _)| k == key),
                "missing residual note for {key}"
            );
        }
    }
}
