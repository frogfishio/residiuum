//! One-shot write scale curve: early vs late windows over ~128 MiB buffered puts.
use residiuum_store::{DurabilityMode, Store};
use std::time::Instant;

fn main() {
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("curve");
    let mut store = Store::create(&path).expect("create");
    store.set_seal_threshold(4 * 1024 * 1024);
    let payload = vec![0x5Au8; 8 * 1024];
    let target_bytes: u64 = 128 * 1024 * 1024;
    let window = 2000u64;
    let mut keys = 0u64;
    let t0 = Instant::now();
    let mut last_report_keys = 0u64;
    let mut last_report_t = Instant::now();
    let mut windows: Vec<(u64, f64, f64)> = Vec::new(); // (keys_end, ops/s, MB/s)

    while keys * (payload.len() as u64) < target_bytes {
        let key = format!("k{:020}", keys);
        store
            .put(&key, &payload, DurabilityMode::Buffered)
            .expect("put");
        keys += 1;
        if keys % window == 0 {
            let elapsed = last_report_t.elapsed().as_secs_f64().max(1e-9);
            let dk = (keys - last_report_keys) as f64;
            let ops = dk / elapsed;
            let mb = (dk * payload.len() as f64) / (1024.0 * 1024.0) / elapsed;
            windows.push((keys, ops, mb));
            eprintln!("keys={keys} window_ops/s={ops:.1} window_MB/s={mb:.2}");
            last_report_keys = keys;
            last_report_t = Instant::now();
        }
    }
    let _ = store.seal_active();
    let total = t0.elapsed().as_secs_f64().max(1e-9);
    let early = &windows[0];
    let late = windows.last().unwrap();
    let ratio = late.1 / early.1;
    println!(
        "{{\"keys\":{keys},\"early_ops\":{:.1},\"late_ops\":{:.1},\"early_mb\":{:.2},\"late_mb\":{:.2},\"total_ops\":{:.1},\"ratio_late_over_early\":{:.3}}}",
        early.1, late.1, early.2, late.2, keys as f64 / total, ratio
    );
    eprintln!();
    eprintln!("Interpretation:");
    eprintln!("  ratio_late_over_early={ratio:.3} (historic full-index-on-seal collapse ≈ 0.13).");
    eprintln!("  ratio ≳ 0.7 → asymptotic indexing failure fixed; residual drop is");
    eprintln!(
        "  synchronous lifecycle (auto-seal + rare persist_index_cache), not O(N) index inserts."
    );
}
