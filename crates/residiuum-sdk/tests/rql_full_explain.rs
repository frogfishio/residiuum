//! RQL-F1: structured explain for `rql-full-v1` + RQL-F2 wire refuse honesty.

use residiuum_heap::CollectionId;
use residiuum_sdk::plan_v1::CollectionBindings;
use residiuum_sdk::rql_app_core::DIAG_RQL_FEATURE_UNAVAILABLE;
use residiuum_sdk::{
    explain_rql_full, refuse_full_language_on_core_wire, source_uses_rql_full_constructs,
    RQL_FULL_PROFILE,
};
use std::str::FromStr;

fn bindings() -> CollectionBindings {
    let mut b = CollectionBindings::default();
    b.bind(
        "orders",
        CollectionId::from_str("00000000-0000-4000-8000-0000000000a1").unwrap(),
    );
    b.bind(
        "customers",
        CollectionId::from_str("00000000-0000-4000-8000-0000000000a2").unwrap(),
    );
    b.bind(
        "line_items",
        CollectionId::from_str("00000000-0000-4000-8000-0000000000a3").unwrap(),
    );
    b
}

#[test]
fn explain_rql_full_pipeline_tree() {
    let ex = explain_rql_full(
        r#"from orders
           enrich customer using customers matching customer_id = id expect exactly_one
           enrich items using line_items matching order_id = order_id expect many
           within items as item {
             enrich product using customers as candidate
               matching item.sku = candidate.id
               expect optional
           }
           project { id, customer { name } }
           page size 16"#,
        &bindings(),
    )
    .expect("explain");
    assert_eq!(ex.plan_profile, RQL_FULL_PROFILE);
    assert_ne!(ex.plan_hash, [0u8; 32]);
    assert_eq!(ex.tree["profile"], RQL_FULL_PROFILE);
    assert_eq!(ex.tree["attach_oracle"], "scan_or_equality_index");
    assert_eq!(ex.tree["wire"], "local_facade_only");
    let pipe = ex.tree["pipeline"].as_array().expect("pipeline");
    assert_eq!(pipe.len(), 3);
    assert_eq!(pipe[0]["kind"], "enrich");
    assert_eq!(pipe[0]["expect"], "exactly_one");
    assert_eq!(pipe[1]["kind"], "enrich");
    assert_eq!(pipe[2]["kind"], "within");
    assert_eq!(pipe[2]["steps"][0]["kind"], "enrich");
    assert!(ex.tree["project"].is_array());
    assert_eq!(ex.tree["base_plan_hash"].as_str().unwrap().len(), 64);
}

#[test]
fn explain_hash_stable_for_same_source() {
    let src = r#"from orders
                 enrich customer using customers matching customer_id = id expect optional
                 page size 8"#;
    let a = explain_rql_full(src, &bindings()).unwrap();
    let b = explain_rql_full(src, &bindings()).unwrap();
    assert_eq!(a.plan_hash, b.plan_hash);
    assert_eq!(a.tree, b.tree);
}

#[test]
fn core_wire_refuses_enrich_within_brace_project() {
    assert!(source_uses_rql_full_constructs(
        "from orders enrich x using customers matching a = b expect many"
    ));
    assert!(source_uses_rql_full_constructs(
        "from orders within items as i { }"
    ));
    assert!(source_uses_rql_full_constructs(
        "from orders project { id }"
    ));
    assert!(!source_uses_rql_full_constructs(
        "from orders where status = \"open\" project id, status page size 8"
    ));

    let err = refuse_full_language_on_core_wire(
        "from orders enrich c using customers matching customer_id = id expect exactly_one",
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains(DIAG_RQL_FEATURE_UNAVAILABLE), "{msg}");
    assert!(msg.contains("HeapClient::rql_full"), "{msg}");

    refuse_full_language_on_core_wire("from orders where true page size 4").unwrap();
    let computed = "from orders project band = if amount < 100 then \"low\" else \"high\"";
    assert!(source_uses_rql_full_constructs(computed));
    assert!(refuse_full_language_on_core_wire(computed).is_err());
}
