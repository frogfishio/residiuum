//! SDA phase breakdown: where does pure-eval latency go?
//!
//! Measures orthogonal surfaces on the same machine:
//! - **lex+parse** once (compile)
//! - **from_json** bridge
//! - **eval** on a pre-parsed program (SDA Value in → SDA Value out)
//! - **to_json** bridge
//! - **full run** (`run` / re-parse every call) vs **compile-once run_json**
//!
//! Program classes cover common host patterns: arithmetic literal, map
//! projection, filter-shaped predicate, and a sequence comprehension.
//!
//! Run: `cargo run -p residiuum-sda --release --example sda_latency_breakdown`
//!
//! Numbers are **diagnostic only**. See `doc/reference/operations/BENCHMARK_DISCLOSURE.md` and
//! `doc/reference/operations/PERFORMANCE_STRATEGIES.md` (SDA section). Do not publish as SLOs.

use residiuum_sda::{from_json, to_json, Program};
use std::time::{Duration, Instant};

/// Default iters for micro programs (override with `SDA_BENCH_ITERS`).
const DEFAULT_ITERS: usize = 2_000;
/// Warmup rounds (override with `SDA_BENCH_WARMUP`).
const DEFAULT_WARMUP: usize = 100;
/// Heavy cases (large input) use fewer timed iters (override with `SDA_BENCH_HEAVY_ITERS`).
const DEFAULT_HEAVY_ITERS: usize = 50;

struct Case {
    name: &'static str,
    source: &'static str,
    input: serde_json::Value,
    /// When true, use heavy-iter budget (large inputs / expensive eval).
    heavy: bool,
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "literal_arith",
            source: "1 + 2 * 3",
            input: serde_json::Value::Null,
            heavy: false,
        },
        Case {
            name: "map_projection",
            source: r#"input<"name">!"#,
            input: serde_json::json!({"name": "Ada", "age": 36, "city": "London"}),
            heavy: false,
        },
        Case {
            name: "filter_eq",
            // Shape used by DEF-028 portable filters (boolean over input).
            source: r#"getPath(input, "status") = Some("active")"#,
            input: serde_json::json!({"status": "active", "age": 21}),
            heavy: false,
        },
        Case {
            name: "filter_and",
            source: r#"(getPath(input, "status") = Some("active")) and (getPath(input, "age") >= Some(18))"#,
            input: serde_json::json!({"status": "active", "age": 21}),
            heavy: false,
        },
        Case {
            name: "seq_comp_small",
            source: r#"{ yield x * 2 | x in input }"#,
            input: serde_json::json!([1, 2, 3, 4, 5, 6, 7, 8]),
            heavy: false,
        },
        Case {
            name: "seq_comp_1k",
            source: r#"{ yield x + 1 | x in input }"#,
            input: serde_json::Value::Array((0..1000).map(|i| serde_json::json!(i)).collect()),
            heavy: true,
        },
    ]
}

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

fn summarize(label: &str, mut samples: Vec<Duration>) {
    let count = samples.len();
    let mean = mean_ns(&samples);
    let p50 = pctl_ns(&mut samples, 0.50);
    let p95 = pctl_ns(&mut samples, 0.95);
    let p99 = pctl_ns(&mut samples, 0.99);
    let ops = 1e9 / mean.max(1.0);
    eprintln!(
        "{label:>28}: mean={mean:8.0}ns p50={p50:8.0}ns p95={p95:8.0}ns p99={p99:8.0}ns \
         ({ops:9.0} ops/s)"
    );
    println!(
        "{{\"phase\":\"{label}\",\"n\":{count},\
         \"mean_ns\":{mean:.0},\"p50_ns\":{p50:.0},\"p95_ns\":{p95:.0},\"p99_ns\":{p99:.0},\
         \"ops_s\":{ops:.1}}}"
    );
}

fn bench_durations(n: usize, mut body: impl FnMut()) -> Vec<Duration> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let t0 = Instant::now();
        body();
        out.push(t0.elapsed());
    }
    out
}

fn main() {
    let warmup = env_usize("SDA_BENCH_WARMUP", DEFAULT_WARMUP);
    let iters = env_usize("SDA_BENCH_ITERS", DEFAULT_ITERS);
    let heavy_iters = env_usize("SDA_BENCH_HEAVY_ITERS", DEFAULT_HEAVY_ITERS);

    eprintln!(
        "sda_latency_breakdown  warmup={warmup} iters={iters} heavy_iters={heavy_iters}  diagnostic only"
    );
    eprintln!("path_class=pure_sda  durability=n/a  storage=n/a");
    eprintln!("env: SDA_BENCH_WARMUP / SDA_BENCH_ITERS / SDA_BENCH_HEAVY_ITERS");
    eprintln!();

    for case in cases() {
        let n = if case.heavy { heavy_iters } else { iters };
        eprintln!("--- case={} (n={n}) ---", case.name);
        eprintln!("source: {}", case.source);

        // Warm compile + one correctness touch.
        let program = Program::parse(case.source)
            .unwrap_or_else(|e| panic!("parse failed for {}: {e}", case.name));
        let _probe = program
            .run_json("input", case.input.clone())
            .unwrap_or_else(|e| panic!("eval failed for {}: {e}", case.name));

        // Warmup full paths.
        for _ in 0..warmup {
            let _ = Program::parse(case.source).expect("parse warmup");
            let _ = program
                .run_json("input", case.input.clone())
                .expect("run_json warmup");
            let _ = residiuum_sda::run(case.source, case.input.clone()).expect("run warmup");
        }

        // --- parse only ---
        let parse_samples = bench_durations(n, || {
            let _ = Program::parse(case.source).expect("parse");
        });
        summarize(&format!("{}|parse", case.name), parse_samples);

        // --- from_json ---
        let from_samples = bench_durations(n, || {
            let _ = from_json(case.input.clone());
        });
        summarize(&format!("{}|from_json", case.name), from_samples);

        // --- eval only (preconverted Value; clone per iter = multi-doc cost) ---
        let sda_input = from_json(case.input.clone());
        let eval_samples = bench_durations(n, || {
            let _ = program.eval("input", sda_input.clone()).expect("eval");
        });
        summarize(&format!("{}|eval", case.name), eval_samples);

        // --- to_json of a stable result ---
        let eval_once = program.eval("input", sda_input.clone()).expect("eval once");
        let to_samples = bench_durations(n, || {
            let _ = to_json(eval_once.clone());
        });
        summarize(&format!("{}|to_json", case.name), to_samples);

        // --- compile-once run_json (JSON bridge + eval, no re-parse) ---
        let once_samples = bench_durations(n, || {
            let _ = program
                .run_json("input", case.input.clone())
                .expect("run_json");
        });
        summarize(&format!("{}|run_json_once", case.name), once_samples);

        // --- full run (re-parse every call) ---
        let full_samples = bench_durations(n, || {
            let _ = residiuum_sda::run(case.source, case.input.clone()).expect("run");
        });
        summarize(&format!("{}|run_reparse", case.name), full_samples);

        // Attribute: how much of re-parse full path is parse vs the rest.
        let attr_n = (n / 4).max(1);
        let mean_parse = mean_ns(&bench_durations(attr_n, || {
            let _ = Program::parse(case.source).expect("parse");
        }));
        let mean_once = mean_ns(&bench_durations(attr_n, || {
            let _ = program
                .run_json("input", case.input.clone())
                .expect("run_json");
        }));
        let mean_full = mean_ns(&bench_durations(attr_n, || {
            let _ = residiuum_sda::run(case.source, case.input.clone()).expect("run");
        }));
        let parse_share = (mean_parse / mean_full.max(1.0) * 100.0).clamp(0.0, 100.0);
        let once_vs_full = (mean_once / mean_full.max(1.0) * 100.0).clamp(0.0, 100.0);
        eprintln!(
            "{:>28}: reparse full≈{mean_full:.0}ns; parse share≈{parse_share:.1}%; \
             compile-once is ≈{once_vs_full:.1}% of reparse full",
            format!("{}|attribute", case.name)
        );
        println!(
            "{{\"phase\":\"{}|attribute\",\"mean_full_ns\":{mean_full:.0},\
             \"mean_parse_ns\":{mean_parse:.0},\"mean_run_json_once_ns\":{mean_once:.0},\
             \"parse_share_pct\":{parse_share:.2},\"once_vs_full_pct\":{once_vs_full:.2}}}",
            case.name
        );
        eprintln!();
    }

    eprintln!(
        "done. diagnostic only — see doc/reference/operations/PERFORMANCE_STRATEGIES.md (SDA)."
    );
}
