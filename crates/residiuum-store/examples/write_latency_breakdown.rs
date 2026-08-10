//! Phase breakdown: is buffered write latency from **data I/O** or **indexing**?
//!
//! Measures orthogonal surfaces on the same machine/tmpfs-or-disk:
//! - Memory mode: in-memory primary index only (no segment append).
//! - Buffered put: encode + segment append + `write_all` + dual index apply.
//! - Index-only delta: buffered − memory ≈ disk/append overhead (same payload).
//! - Full index-cache checkpoint: O(live × body) serialization + atomic fsync.
//! - Seal: O(segment) rewrite + blake3 + fsync (no full primary.idx rewrite).
//!
//! Run: `cargo run -p residiuum-store --release --example write_latency_breakdown`

use residiuum_store::{DurabilityMode, Store};
use std::time::{Duration, Instant};

fn mean_ns(samples: &[Duration]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.iter().map(|d| d.as_nanos() as f64).sum::<f64>() / samples.len() as f64
}

fn pctl_ns(samples: &mut [Duration], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort();
    let idx = ((samples.len() as f64 - 1.0) * p).round() as usize;
    samples[idx.min(samples.len() - 1)].as_nanos() as f64
}

fn bench_puts(
    store: &mut Store,
    mode: DurabilityMode,
    prefix: &str,
    n: usize,
    payload: &[u8],
) -> Vec<Duration> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let key = format!("{prefix}{i:08}");
        let t0 = Instant::now();
        store.put(&key, payload, mode).expect("put");
        out.push(t0.elapsed());
    }
    out
}

fn summarize(label: &str, mut samples: Vec<Duration>, payload_len: usize) {
    let count = samples.len();
    let mean = mean_ns(&samples);
    let p50 = pctl_ns(&mut samples, 0.50);
    let p95 = pctl_ns(&mut samples, 0.95);
    let p99 = pctl_ns(&mut samples, 0.99);
    let ops = 1e9 / mean.max(1.0);
    let mb_s = (payload_len as f64 * ops) / (1024.0 * 1024.0);
    eprintln!(
        "{label:>28}: mean={mean:8.0}ns p50={p50:8.0}ns p95={p95:8.0}ns p99={p99:8.0}ns \
         ({ops:9.0} ops/s, {mb_s:7.2} MB/s logical)"
    );
    println!(
        "{{\"phase\":\"{label}\",\"n\":{count},\"payload\":{payload_len},\
         \"mean_ns\":{mean:.0},\"p50_ns\":{p50:.0},\"p95_ns\":{p95:.0},\"p99_ns\":{p99:.0},\
         \"ops_s\":{ops:.1},\"mb_s\":{mb_s:.3}}}"
    );
}

/// Attribute buffered put cost.
///
/// Memory mode applies the live index once (visibility only). Buffered mode
/// dual-applies `index` + `durable_index` then encodes/appends/writes. So:
/// - naive: (buf − mem) as "data" **over-counts data** (delta still includes one index apply)
/// - dual: treat 2×memory as index cost, remainder as data I/O (better model)
fn attribute(label: &str, mean_mem: f64, mean_buf: f64) {
    let naive_data = ((mean_buf - mean_mem) / mean_buf.max(1.0) * 100.0).clamp(0.0, 100.0);
    let dual_index_ns = (2.0 * mean_mem).min(mean_buf);
    let dual_index_pct = (dual_index_ns / mean_buf.max(1.0) * 100.0).clamp(0.0, 100.0);
    let dual_data_pct = 100.0 - dual_index_pct;
    eprintln!(
        "{label:>28}: naive data≈{naive_data:.1}% | dual-index model: data≈{dual_data_pct:.1}% index≈{dual_index_pct:.1}%"
    );
    println!(
        "{{\"phase\":\"{label}\",\"naive_data_pct\":{naive_data:.2},\
         \"dual_model_data_pct\":{dual_data_pct:.2},\"dual_model_index_pct\":{dual_index_pct:.2},\
         \"mean_mem_ns\":{mean_mem:.0},\"mean_buf_ns\":{mean_buf:.0}}}"
    );
}

fn main() {
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("breakdown");
    let mut store = Store::create(&path).expect("create");
    // Large threshold so mid-run auto-seal does not contaminate put samples.
    store.set_seal_threshold(256 * 1024 * 1024);

    let warmup = 200usize;
    let n = 2_000usize;
    let small = b"tiny";
    let large = vec![0x5Au8; 8 * 1024];

    // Warmup both paths.
    let _ = bench_puts(&mut store, DurabilityMode::Memory, "w/m/", warmup, small);
    let _ = bench_puts(&mut store, DurabilityMode::Buffered, "w/b/", warmup, small);

    eprintln!("=== small payload ({} B) ===", small.len());
    let mem_small = bench_puts(&mut store, DurabilityMode::Memory, "m/s/", n, small);
    let buf_small = bench_puts(&mut store, DurabilityMode::Buffered, "b/s/", n, small);
    let mean_mem_s = mean_ns(&mem_small);
    let mean_buf_s = mean_ns(&buf_small);
    summarize("memory_index_only_small", mem_small, small.len());
    summarize("buffered_put_small", buf_small, small.len());
    attribute("attribution_small", mean_mem_s, mean_buf_s);

    eprintln!("=== large payload ({} B) ===", large.len());
    let mem_large = bench_puts(&mut store, DurabilityMode::Memory, "m/l/", n, &large);
    let buf_large = bench_puts(&mut store, DurabilityMode::Buffered, "b/l/", n, &large);
    let mean_mem_l = mean_ns(&mem_large);
    let mean_buf_l = mean_ns(&buf_large);
    summarize("memory_index_only_large", mem_large, large.len());
    summarize("buffered_put_large", buf_large, large.len());
    attribute("attribution_large", mean_mem_l, mean_buf_l);

    // Full primary.idx rewrite cost at current live set (includes all subjects above).
    let live = store.live_count();
    let mut ckpt_samples = Vec::with_capacity(5);
    for _ in 0..5 {
        let t0 = Instant::now();
        store.persist_index_cache().expect("ckpt");
        ckpt_samples.push(t0.elapsed());
    }
    eprintln!("=== full index-cache checkpoint (live_subjects={live}) ===");
    summarize("persist_index_cache", ckpt_samples, 0);

    // Seal cost for ~4 MiB active (buffered data only; no full index rewrite on seal).
    store.set_seal_threshold(4 * 1024 * 1024);
    let mut seal_samples = Vec::new();
    // Fill and seal a few times.
    let fill = vec![0xABu8; 4 * 1024];
    for s in 0..6 {
        for k in 0..1100 {
            store
                .put(&format!("seal/{s}/{k:05}"), &fill, DurabilityMode::Buffered)
                .expect("put fill");
        }
        let t0 = Instant::now();
        store.seal_active().expect("seal");
        seal_samples.push(t0.elapsed());
    }
    // Drop first seal as cold.
    let seal_warm: Vec<_> = seal_samples.into_iter().skip(1).collect();
    eprintln!("=== seal_active (~4MiB segment, O(segment) path) ===");
    summarize("seal_active", seal_warm, 4 * 1024 * 1024);

    eprintln!();
    eprintln!("Interpretation:");
    eprintln!(
        "  Steady-state buffered puts: data path (encode/append/write) + dual in-memory index."
    );
    eprintln!("  Dual-index model: memory×2 ≈ index publish cost; remainder ≈ data I/O.");
    eprintln!("  Asymptotic indexing failure (full rewrite on seal / every few puts): FIXED.");
    eprintln!("  Remaining bottleneck: synchronous lifecycle when it fires —");
    eprintln!("    - persist_index_cache: O(live×body) serialize+fsync (tens of ms at scale)");
    eprintln!("    - seal_active: O(segment) rewrite+hash+fsync (not a full primary.idx rewrite)");
    eprintln!("  Ordinary put latency → data + index publish; long-run spikes → lifecycle.");
}
