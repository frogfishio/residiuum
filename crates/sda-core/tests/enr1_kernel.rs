//! ENR1 kernel profile tests (additive to standalone SDA).
//!
//! ENR1 lives in the same `residiuum-sda` compile path as SDA (`Program::parse` once).
//! Spec spine: `crates/enr-core/ENR1.md` minimal subset. ENR2 is out of scope.
//!
//! Profile tag: [`residiuum_sda::ENR1_PROFILE_TAG`].

use residiuum_sda::{from_json, Program, Value, ENR1_PROFILE_TAG};
use serde_json::json;

fn run(src: &str) -> serde_json::Value {
    residiuum_sda::run(src, serde_json::Value::Null).expect("enr1 program should evaluate")
}

fn run_input(src: &str, input: serde_json::Value) -> serde_json::Value {
    residiuum_sda::run(src, input).expect("enr1 program should evaluate")
}

#[test]
fn enr1_profile_tag_is_stable() {
    assert_eq!(ENR1_PROFILE_TAG, "sda-enr1-v0.1");
    assert_eq!(
        residiuum_sda::SdaRuntime::enr1_profile_tag(),
        ENR1_PROFILE_TAG
    );
}

#[test]
fn one_opt_empty_one_and_duplicate() {
    assert_eq!(run(r#"one?(Bag{});"#), json!({"$type": "none"}));
    assert_eq!(
        run(r#"one?(Bag{42});"#),
        json!({"$type": "some", "$value": 42})
    );
    assert_eq!(
        run(r#"one?(Bag{1, 2});"#),
        json!({"$type": "fail", "$code": "t_enr_duplicate", "$msg": "duplicate match"})
    );
    // Sugar + ASCII aliases share semantics.
    assert_eq!(run(r#"oneOpt(Seq[7]);"#), run(r#"one?(Seq[7]);"#));
}

#[test]
fn one_req_missing_unique_duplicate() {
    assert_eq!(
        run(r#"one!(Bag{});"#),
        json!({"$type": "fail", "$code": "t_enr_missing", "$msg": "missing match"})
    );
    assert_eq!(run(r#"one!(Bag{"ok"});"#), json!("ok"));
    assert_eq!(
        run(r#"one!(Seq[1, 1]);"#),
        json!({"$type": "fail", "$code": "t_enr_duplicate", "$msg": "duplicate match"})
    );
    assert_eq!(run(r#"oneReq(Bag{9});"#), run(r#"one!(Bag{9});"#));
}

#[test]
fn only_is_exact_uniqueness() {
    assert_eq!(run(r#"only(Bag{3});"#), json!(3));
    assert_eq!(
        run(r#"only(Bag{});"#),
        json!({"$type": "fail", "$code": "t_enr_missing", "$msg": "missing match"})
    );
    assert_eq!(
        run(r#"only(Bag{1, 2});"#),
        json!({"$type": "fail", "$code": "t_enr_duplicate", "$msg": "duplicate match"})
    );
}

#[test]
fn first_last_require_source_order() {
    assert_eq!(
        run(r#"first(Seq[10, 20, 30]);"#),
        json!({"$type": "some", "$value": 10})
    );
    assert_eq!(
        run(r#"last(Seq[10, 20, 30]);"#),
        json!({"$type": "some", "$value": 30})
    );
    assert_eq!(run(r#"first(Seq[]);"#), json!({"$type": "none"}));
    // Bag/Set have no source order claim → ordered-policy fail.
    assert_eq!(
        run(r#"first(Bag{1, 2});"#),
        json!({"$type": "fail", "$code": "t_enr_unordered_policy", "$msg": "unordered policy"})
    );
    assert_eq!(
        run(r#"last(Set{1});"#),
        json!({"$type": "fail", "$code": "t_enr_unordered_policy", "$msg": "unordered policy"})
    );
}

#[test]
fn merge_and_plus_attach_on_prod_and_map() {
    assert_eq!(
        run(r#"merge(Prod{a: 1}, Prod{b: 2});"#),
        json!({"$type": "prod", "$fields": {"a": 1, "b": 2}})
    );
    assert_eq!(
        run(r#"Prod{a: 1} + Prod{b: 2};"#),
        json!({"$type": "prod", "$fields": {"a": 1, "b": 2}})
    );
    assert_eq!(
        run(r#"Map{"a" -> 1} + Map{"b" -> 2};"#),
        json!({"a": 1, "b": 2})
    );
    assert_eq!(
        run(r#"merge(Prod{a: 1}, Prod{a: 9});"#),
        json!({"$type": "fail", "$code": "t_enr_field_collision", "$msg": "field collision"})
    );
    assert_eq!(
        run(r#"Prod{a: 1} + Prod{a: 2};"#),
        json!({"$type": "fail", "$code": "t_enr_field_collision", "$msg": "field collision"})
    );
    assert_eq!(
        run(r#"merge(1, 2);"#),
        json!({"$type": "fail", "$code": "t_enr_wrong_shape", "$msg": "wrong shape"})
    );
    // Numeric + still SDA arithmetic (unchanged).
    assert_eq!(run(r#"1 + 2;"#), json!(3));
}

#[test]
fn as_bag_and_match_bag_carrier() {
    assert_eq!(
        run(r#"asBag(Seq[1, 2, 2]);"#),
        json!({"$type": "bag", "$items": [1, 2, 2]})
    );
    assert_eq!(
        run(r#"matchBag(Set{1, 2});"#),
        json!({"$type": "bag", "$items": [1, 2]})
    );
    assert_eq!(
        run(r#"asBag(Map{"a" -> 1});"#),
        json!({"$type": "fail", "$code": "t_enr_wrong_shape", "$msg": "wrong shape"})
    );
}

#[test]
fn match_bag_comprehension_plus_cardinality() {
    // Canonical ENR1 form: match bag + one! (SDA comprehension, ENR cardinality).
    // getPath returns Opt — compare against Some(...) (Null ≠ absence).
    let src = r#"
      one!({ c | c in Bag{
        Map{"id" -> "c1", "name" -> "Ada"},
        Map{"id" -> "c2", "name" -> "Bob"}
      } | getPath(c, Seq["id"]) = Some("c1") })
    "#;
    assert_eq!(run(src), json!({"id": "c1", "name": "Ada"}));

    let missing = r#"
      one!({ c | c in Bag{ Map{"id" -> "c1"} } | getPath(c, Seq["id"]) = Some("nope") })
    "#;
    assert_eq!(
        run(missing),
        json!({"$type": "fail", "$code": "t_enr_missing", "$msg": "missing match"})
    );
}

#[test]
fn dataset_attach_required_one() {
    // ENR1 §05.1 style attach via + and one! over left rows.
    let program = r#"
      bindOpt(getPath(input, Seq["orders"]), orders =>
      bindOpt(getPath(input, Seq["customers"]), customers =>
        Some({
          yield o + Map{
            "customer" -> one!({
              c | c in customers
                | getPath(c, Seq["id"]) = getPath(o, Seq["customer_id"])
            })
          }
          | o in orders
        })
      ))
    "#;
    let input = json!({
        "orders": [
            {"id": "o1", "customer_id": "c1", "qty": 2},
            {"id": "o2", "customer_id": "c2", "qty": 1}
        ],
        "customers": [
            {"id": "c1", "name": "Ada"},
            {"id": "c2", "name": "Bob"}
        ]
    });
    let out = run_input(program, input);
    // bindOpt wraps Some(Seq[...]).
    assert_eq!(
        out,
        json!({
            "$type": "some",
            "$value": [
                {
                    "id": "o1",
                    "customer_id": "c1",
                    "qty": 2,
                    "customer": {"id": "c1", "name": "Ada"}
                },
                {
                    "id": "o2",
                    "customer_id": "c2",
                    "qty": 1,
                    "customer": {"id": "c2", "name": "Bob"}
                }
            ]
        })
    );
}

#[test]
fn null_row_is_still_a_match() {
    // ENR law: Null is a value, not absence — empty bag means no match.
    let src = r#"
      one?(Bag{ null })
    "#;
    assert_eq!(run(src), json!({"$type": "some", "$value": null}));
}

#[test]
fn compile_once_enr1_program() {
    let prog = Program::parse(r#"one!(Bag{input})"#).expect("parse");
    let a = prog.run_json("input", json!(1)).expect("eval a");
    let b = prog.run_json("input", json!(2)).expect("eval b");
    assert_eq!(a, json!(1));
    assert_eq!(b, json!(2));
    // Format preserves sugar name for one!.
    let formatted = prog.format();
    assert!(
        formatted.contains("one!"),
        "expected one! in format, got {formatted}"
    );
}

#[test]
fn wrong_shape_on_cardinality_non_carrier() {
    assert_eq!(
        run(r#"one?(42);"#),
        json!({"$type": "fail", "$code": "t_enr_wrong_shape", "$msg": "wrong shape"})
    );
}

#[test]
fn from_json_value_path_still_pure() {
    // Host bridge: ENR ops work on Value without JSON.
    let prog = Program::parse(r#"one?(input)"#).unwrap();
    let bag = from_json(json!({"$type": "bag", "$items": ["x"]}));
    match prog.eval("input", bag).unwrap() {
        Value::Some_(inner) => assert_eq!(*inner, Value::Str("x".into())),
        other => panic!("expected Some, got {other:?}"),
    }
}

#[test]
fn match_special_form_returns_bag() {
    // Match(l, R, kL, kR) = { r ∈ R | kR(r) = kL(l) } as Bag.
    let src = r#"
      one!(Match(
        Map{"customer_id" -> "c1"},
        Bag{
          Map{"id" -> "c1", "name" -> "Ada"},
          Map{"id" -> "c2", "name" -> "Bob"}
        },
        getPath(l, Seq["customer_id"]),
        getPath(r, Seq["id"])
      ))
    "#;
    assert_eq!(run(src), json!({"id": "c1", "name": "Ada"}));

    let missing = r#"
      one!(Match(
        Map{"customer_id" -> "nope"},
        Bag{ Map{"id" -> "c1"} },
        getPath(l, Seq["customer_id"]),
        getPath(r, Seq["id"])
      ))
    "#;
    assert_eq!(
        run(missing),
        json!({"$type": "fail", "$code": "t_enr_missing", "$msg": "missing match"})
    );
}

#[test]
fn enrich_pipe_attach_with_match() {
    let program = r#"
      orders
      |> enrich {
          customer:
            one!(
              Match(
                l,
                customers,
                getPath(l, Seq["customer_id"]),
                getPath(r, Seq["id"])
              )
            )
        }
    "#;
    let input = json!({
        "orders": [
            {"id": "o1", "customer_id": "c1", "qty": 2},
            {"id": "o2", "customer_id": "c2", "qty": 1}
        ],
        "customers": [
            {"id": "c1", "name": "Ada"},
            {"id": "c2", "name": "Bob"}
        ]
    });
    let prog = Program::parse(program).expect("parse enrich");
    let out = prog
        .run_json_bindings([
            ("input".into(), input.clone()),
            ("orders".into(), input["orders"].clone()),
            ("customers".into(), input["customers"].clone()),
        ])
        .expect("eval enrich");
    assert_eq!(
        out,
        json!([
            {
                "id": "o1",
                "customer_id": "c1",
                "qty": 2,
                "customer": {"id": "c1", "name": "Ada"}
            },
            {
                "id": "o2",
                "customer_id": "c2",
                "qty": 1,
                "customer": {"id": "c2", "name": "Bob"}
            }
        ])
    );
}

#[test]
fn merge_left_right_policies() {
    // mergeFail (default) still collides.
    assert_eq!(
        run(r#"mergeFail(Prod{a: 1}, Prod{a: 9});"#),
        json!({"$type": "fail", "$code": "t_enr_field_collision", "$msg": "field collision"})
    );
    // mergeLeft keeps left; mergeRight overwrites with right.
    assert_eq!(
        run(r#"mergeLeft(Prod{a: 1, b: 2}, Prod{a: 9, c: 3});"#),
        json!({"$type": "prod", "$fields": {"a": 1, "b": 2, "c": 3}})
    );
    assert_eq!(
        run(r#"mergeRight(Map{"a" -> 1, "b" -> 2}, Map{"a" -> 9, "c" -> 3});"#),
        json!({"a": 9, "b": 2, "c": 3})
    );
}

#[test]
fn keyed_sugar_desugars_to_match_bag() {
    // ENR1 §07: R[kR = kL] → { r | r in R | kR = kL }
    let sugar = r#"
      one!(customers[
        getPath(r, Seq["id"]) = getPath(l, Seq["customer_id"])
      ])
    "#;
    let expanded = r#"
      one!({
        r | r in customers
          | getPath(r, Seq["id"]) = getPath(l, Seq["customer_id"])
      })
    "#;
    let input = json!({
        "l": {"customer_id": "c1"},
        "customers": [
            {"id": "c1", "name": "Ada"},
            {"id": "c2", "name": "Bob"}
        ]
    });
    let a = Program::parse(sugar)
        .unwrap()
        .run_json_bindings([
            ("l".into(), input["l"].clone()),
            ("customers".into(), input["customers"].clone()),
        ])
        .unwrap();
    let b = Program::parse(expanded)
        .unwrap()
        .run_json_bindings([
            ("l".into(), input["l"].clone()),
            ("customers".into(), input["customers"].clone()),
        ])
        .unwrap();
    assert_eq!(a, b);
    assert_eq!(a, json!({"id": "c1", "name": "Ada"}));
}

#[test]
fn source_declarations_are_annotations() {
    let src = r#"
      source customers : Index[Str, Customer];
      source items : MultiIndex[Str, Item];
      source raw : Dataset;
      one!(Bag{42});
    "#;
    let prog = Program::parse(src).expect("parse sources");
    let formatted = prog.format();
    assert!(formatted.contains("source customers : Index[Str, Customer]"));
    assert!(formatted.contains("source items : MultiIndex[Str, Item]"));
    assert!(formatted.contains("source raw : Dataset"));
    // Declarations do not affect evaluation of subsequent expressions.
    assert_eq!(prog.run_json("input", json!(null)).unwrap(), json!(42));
}

#[test]
fn match_invalid_key_tag() {
    // Lambda is not comparable → t_enr_invalid_key.
    let src = r#"
      Match(
        1,
        Bag{ Map{"id" -> 1} },
        x => x,
        getPath(r, Seq["id"])
      )
    "#;
    assert_eq!(
        run(src),
        json!({"$type": "fail", "$code": "t_enr_invalid_key", "$msg": "invalid key"})
    );
}

#[test]
fn enrich_then_refine_pipe_shapes_result() {
    let program = r#"
      orders
      |> enrich {
          customer:
            one!(
              Match(
                l,
                customers,
                getPath(l, Seq["customer_id"]),
                getPath(r, Seq["id"])
              )
            )
        }
      |> refine {
          yield o + Map{
            "customer_name" -> getPath(o, Seq["customer", "name"])
          }
          | o in _
        }
    "#;
    let input = json!({
        "orders": [
            {"id": "o1", "customer_id": "c1"}
        ],
        "customers": [
            {"id": "c1", "name": "Ada"}
        ]
    });
    let prog = Program::parse(program).expect("parse pipeline");
    let out = prog
        .run_json_bindings([
            ("input".into(), input.clone()),
            ("orders".into(), input["orders"].clone()),
            ("customers".into(), input["customers"].clone()),
        ])
        .expect("eval pipeline");
    assert_eq!(out.as_array().unwrap().len(), 1);
    assert_eq!(out[0]["id"], json!("o1"));
    assert_eq!(out[0]["customer"]["name"], json!("Ada"));
    // getPath returns Opt — surface keeps Some(...) rather than auto-unwrap.
    assert_eq!(
        out[0]["customer_name"],
        json!({"$type": "some", "$value": "Ada"})
    );
}
