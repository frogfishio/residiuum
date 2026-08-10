//! SDA performance harness skeleton (no marketing claims).
//!
//! Times parse-once vs re-parse full run and a few program classes.
//! Assertions only guard against absurd failures (timeouts), not performance
//! targets. See `doc/reference/operations/PERFORMANCE_STRATEGIES.md` and
//! `doc/reference/operations/BENCHMARK_DISCLOSURE.md` before any public comparison.

use residiuum_sda::{from_json, Program};
use std::time::Instant;

#[test]
fn sda_bench_skeleton_parse_eval_classes() {
    let cases: &[(&str, &str, serde_json::Value)] = &[
        ("literal", "1 + 2", serde_json::Value::Null),
        (
            "projection",
            r#"input<"name">!"#,
            serde_json::json!({"name": "Ada"}),
        ),
        (
            "filter_eq",
            r#"getPath(input, "status") = Some("active")"#,
            serde_json::json!({"status": "active"}),
        ),
        (
            "seq_comp",
            r#"{ yield x * 2 | x in input }"#,
            serde_json::json!([1, 2, 3, 4, 5]),
        ),
    ];

    let n = 200usize;
    for (name, source, input) in cases {
        let t0 = Instant::now();
        let program =
            Program::parse(source).unwrap_or_else(|e| panic!("{name}: parse failed: {e}"));
        let parse_once = t0.elapsed();

        // Warmth + correctness (value shape varies by program class).
        let _out = program
            .run_json("input", input.clone())
            .unwrap_or_else(|e| panic!("{name}: run_json failed: {e}"));

        let t0 = Instant::now();
        for _ in 0..n {
            let _ = program
                .eval("input", from_json(input.clone()))
                .unwrap_or_else(|e| panic!("{name}: eval failed: {e}"));
        }
        let eval_loop = t0.elapsed();

        let t0 = Instant::now();
        for _ in 0..n {
            let _ = program
                .run_json("input", input.clone())
                .unwrap_or_else(|e| panic!("{name}: run_json failed: {e}"));
        }
        let run_json_loop = t0.elapsed();

        let t0 = Instant::now();
        for _ in 0..n {
            let _ = residiuum_sda::run(source, input.clone())
                .unwrap_or_else(|e| panic!("{name}: run failed: {e}"));
        }
        let reparse_loop = t0.elapsed();

        eprintln!(
            "sda_bench_skeleton case={name} n={n}\n  parse_once={parse_once:?}\n  \
             eval_loop={eval_loop:?}\n  run_json_loop={run_json_loop:?}\n  \
             reparse_loop={reparse_loop:?}"
        );

        // Sanity: operations finished in under a generous bound (CI safety).
        assert!(parse_once.as_secs() < 5, "{name}: parse absurdly slow");
        assert!(eval_loop.as_secs() < 30, "{name}: eval loop absurdly slow");
        assert!(
            run_json_loop.as_secs() < 30,
            "{name}: run_json loop absurdly slow"
        );
        assert!(
            reparse_loop.as_secs() < 60,
            "{name}: reparse loop absurdly slow"
        );
    }
}

#[test]
fn program_parse_once_agrees_with_run() {
    let source = r#"input<"name">!"#;
    let input = serde_json::json!({"name": "Ada"});
    let via_run = residiuum_sda::run(source, input.clone()).unwrap();
    let via_once = Program::parse(source)
        .unwrap()
        .run_json("input", input)
        .unwrap();
    assert_eq!(via_run, via_once);
}
