//! DEF-028: native filters and SDA share absence / Null / comparison semantics.
//!
//! Corpus runs every portable filter operator through:
//! - native [`Filter::matches`]
//! - compiled SDA [`Filter::matches_sda`]
//! - embedded `find` (scan path)
//!
//! and asserts agreement. Also checks versioned [`QueryPlan`] round-trips.

use residiuum_sdk::{json, Filter, Pred, QueryOptions, QueryPlan, Residiuum, QUERY_PLAN_PROFILE};

use serde_json::Value as JsonValue;
use tempfile::tempdir;

struct Case {
    name: &'static str,
    filter: Filter,
    doc: JsonValue,
    want: bool,
}

fn corpus() -> Vec<Case> {
    vec![
        Case {
            name: "eq_hit",
            filter: Filter::field("status").eq("active"),
            doc: json!({"status": "active", "age": 21}),
            want: true,
        },
        Case {
            name: "eq_miss",
            filter: Filter::field("status").eq("active"),
            doc: json!({"status": "paused"}),
            want: false,
        },
        Case {
            name: "eq_null_present",
            filter: Filter::field("n").eq(JsonValue::Null),
            doc: json!({"n": null}),
            want: true,
        },
        Case {
            name: "eq_null_absent",
            filter: Filter::field("n").eq(JsonValue::Null),
            doc: json!({}),
            want: false,
        },
        Case {
            name: "ne_missing",
            filter: Filter::field("missing").ne(1),
            doc: json!({"status": "x"}),
            want: true,
        },
        Case {
            name: "ne_equal",
            filter: Filter::field("status").ne("active"),
            doc: json!({"status": "active"}),
            want: false,
        },
        Case {
            name: "lt_num",
            filter: Filter::field("age").lt(18),
            doc: json!({"age": 10}),
            want: true,
        },
        Case {
            name: "lte_num",
            filter: Filter::field("age").lte(18),
            doc: json!({"age": 18}),
            want: true,
        },
        Case {
            name: "gt_str",
            filter: Filter::field("name").gt("M"),
            doc: json!({"name": "Zed"}),
            want: true,
        },
        Case {
            name: "gte_missing",
            filter: Filter::field("age").gte(18),
            doc: json!({}),
            want: false,
        },
        Case {
            name: "gte_null",
            filter: Filter::field("age").gte(18),
            doc: json!({"age": null}),
            want: false,
        },
        Case {
            name: "gte_wrong_type",
            filter: Filter::field("age").gte(18),
            doc: json!({"age": "x"}),
            want: false,
        },
        Case {
            name: "in_hit",
            filter: Filter::field("c").is_in(["TH", "SG"]),
            doc: json!({"c": "TH"}),
            want: true,
        },
        Case {
            name: "in_miss",
            filter: Filter::field("c").is_in(["TH", "SG"]),
            doc: json!({"c": "US"}),
            want: false,
        },
        Case {
            name: "exists_true_null",
            filter: Filter::field("a").exists(true),
            doc: json!({"a": null}),
            want: true,
        },
        Case {
            name: "exists_false_absent",
            filter: Filter::field("a").exists(false),
            doc: json!({}),
            want: true,
        },
        Case {
            name: "exists_false_null",
            filter: Filter::field("a").exists(false),
            doc: json!({"a": null}),
            want: false,
        },
        Case {
            name: "prefix",
            filter: Filter::field("name").prefix("Al"),
            doc: json!({"name": "Alice"}),
            want: true,
        },
        Case {
            name: "prefix_miss",
            filter: Filter::field("name").prefix("Bo"),
            doc: json!({"name": "Alice"}),
            want: false,
        },
        Case {
            name: "contains_str",
            filter: Filter::field("name").contains("ice"),
            doc: json!({"name": "Alice"}),
            want: true,
        },
        Case {
            name: "contains_arr",
            filter: Filter::field("tags").contains("y"),
            doc: json!({"tags": ["x", "y"]}),
            want: true,
        },
        Case {
            name: "contains_arr_num",
            filter: Filter::Field {
                path: "nums".into(),
                pred: Pred::Contains(json!(2)),
            },
            doc: json!({"nums": [1, 2, 3]}),
            want: true,
        },
        Case {
            name: "dotted_path",
            filter: Filter::field("address.city").eq("Bangkok"),
            doc: json!({"address": {"city": "Bangkok"}}),
            want: true,
        },
        Case {
            name: "dotted_intermediate_non_object_absent",
            filter: Filter::field("a.city").exists(false),
            doc: json!({"a": "not-an-object"}),
            want: true,
        },
        Case {
            name: "and_combo",
            filter: Filter::and([
                Filter::field("status").eq("active"),
                Filter::field("age").gte(18),
            ]),
            doc: json!({"status": "active", "age": 21}),
            want: true,
        },
        Case {
            name: "or_combo",
            filter: Filter::or([
                Filter::field("status").eq("active"),
                Filter::field("status").eq("trial"),
            ]),
            doc: json!({"status": "trial"}),
            want: true,
        },
        Case {
            name: "not_eq",
            filter: Filter::not(Filter::field("status").eq("active")),
            doc: json!({"status": "paused"}),
            want: true,
        },
        Case {
            name: "always",
            filter: Filter::always(),
            doc: json!({}),
            want: true,
        },
        Case {
            name: "object_filter_json",
            filter: Filter::from_json(&json!({
                "status": "active",
                "age": { "$gte": 18 },
                "country": { "$in": ["TH", "SG"] }
            }))
            .unwrap(),
            doc: json!({"status": "active", "age": 20, "country": "TH"}),
            want: true,
        },
    ]
}

#[test]
fn native_and_sda_agree_on_corpus() {
    for c in corpus() {
        let native = c.filter.matches(&c.doc);
        let sda = c
            .filter
            .matches_sda(&c.doc)
            .unwrap_or_else(|e| panic!("{}: SDA error {e}; prog={}", c.name, c.filter.to_sda()));
        assert_eq!(
            native,
            c.want,
            "{}: native={} want={}; prog={}",
            c.name,
            native,
            c.want,
            c.filter.to_sda()
        );
        assert_eq!(
            sda,
            c.want,
            "{}: sda={} want={}; prog={}",
            c.name,
            sda,
            c.want,
            c.filter.to_sda()
        );
        assert_eq!(
            native,
            sda,
            "{}: native≠sda; prog={}",
            c.name,
            c.filter.to_sda()
        );
    }
}

#[test]
fn embedded_find_agrees_with_filter_matches() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("app.residiuum")).unwrap();
    let mut col = db.collection("docs").unwrap();

    let docs = [
        (
            "d1",
            json!({"status": "active", "age": 21, "country": "TH", "name": "Alice", "tags": ["x", "y"]}),
        ),
        (
            "d2",
            json!({"status": "paused", "age": 17, "country": "US", "name": "Bob", "tags": ["z"]}),
        ),
        (
            "d3",
            json!({"status": "active", "age": 30, "country": "SG", "name": "Ada", "n": null}),
        ),
        ("d4", json!({"address": {"city": "Bangkok"}})),
        ("d5", json!({"a": "not-object"})),
    ];
    for (k, v) in &docs {
        col.put(k, v).unwrap();
    }

    let filters = [
        Filter::field("status").eq("active"),
        Filter::field("age").gte(18),
        Filter::field("country").is_in(["TH", "SG"]),
        Filter::field("name").prefix("A"),
        Filter::field("tags").contains("y"),
        Filter::field("n").eq(JsonValue::Null),
        Filter::field("address.city").eq("Bangkok"),
        Filter::field("a.city").exists(false),
        Filter::and([
            Filter::field("status").eq("active"),
            Filter::field("age").gte(18),
        ]),
        Filter::not(Filter::field("status").eq("active")),
    ];

    for filter in &filters {
        let mut expected: Vec<String> = docs
            .iter()
            .filter(|(_, v)| filter.matches(v))
            .map(|(k, _)| (*k).to_string())
            .collect();
        expected.sort();
        let mut found: Vec<String> = col
            .find(filter)
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        found.sort();
        assert_eq!(found, expected, "find≠matches for prog={}", filter.to_sda());

        // Force-scan path must agree too.
        let mut forced: Vec<String> = col
            .find_with(filter, QueryOptions::new().force_scan())
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        forced.sort();
        assert_eq!(
            forced,
            expected,
            "force_scan≠matches for prog={}",
            filter.to_sda()
        );
    }
}

#[test]
fn query_plan_profile_roundtrip_and_reject_unknown() {
    let plan = QueryPlan::new(
        Filter::field("status").eq("active"),
        QueryOptions::new().limit(5),
    );
    assert_eq!(plan.profile, QUERY_PLAN_PROFILE);
    let j = plan.to_json();
    let back = QueryPlan::from_json(&j).unwrap();
    assert_eq!(back.options.limit, Some(5));
    assert!(back.filter.matches(&json!({"status": "active"})));

    let bad = json!({"profile": "residiuum-query-plan-v0", "filter": {}});
    let err = QueryPlan::from_json(&bad).unwrap_err();
    assert!(err.to_string().contains("unsupported query plan profile"));
}

#[test]
fn sda_program_is_parseable() {
    for c in corpus() {
        sda_core::check(&c.filter.to_sda()).unwrap_or_else(|e| {
            panic!(
                "{}: SDA parse failed: {e}; prog={}",
                c.name,
                c.filter.to_sda()
            )
        });
    }
}
