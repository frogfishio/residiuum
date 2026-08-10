//! Raw SDA / ENR1 text query path (DX_SPEC §7.6).
//!
//! People who know the algebra still write programs as text — not only fluent
//! `Filter` / `MultiQuery` builders. This suite locks:
//!
//! 1. [`Collection::sda`] — single-collection program over scanned docs
//! 2. [`Collection::filter_sda`] — per-doc boolean predicate as text
//! 3. [`Residiuum::sda_query`] / [`Residiuum::sda`] — multi-collection ENR1 attach as text

use residiuum_sdk::{json, Filter, QueryOptions, Residiuum};
use tempfile::tempdir;

#[test]
fn collection_sda_filters_active_users() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("col-sda.residiuum")).unwrap();
    {
        let mut users = db.collection("users").unwrap();
        users
            .put("a", &json!({"name": "Ada", "status": "active"}))
            .unwrap();
        users
            .put("b", &json!({"name": "Bob", "status": "paused"}))
            .unwrap();
        users
            .put("c", &json!({"name": "Cy", "status": "active"}))
            .unwrap();
    }

    let program = r#"{
      yield u | u in input
        | getPath(u, Seq["status"]) = Some("active")
    }"#;

    let mut users = db.collection("users").unwrap();
    let out = users.sda(program).expect("collection.sda");
    let arr = out.as_array().expect("seq of active users");
    assert_eq!(arr.len(), 2);
    let names: Vec<&str> = arr.iter().filter_map(|u| u["name"].as_str()).collect();
    assert!(names.contains(&"Ada"));
    assert!(names.contains(&"Cy"));
    assert!(!names.contains(&"Bob"));
}

#[test]
fn collection_filter_sda_keeps_keys() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("filter-sda.residiuum")).unwrap();
    {
        let mut users = db.collection("users").unwrap();
        users
            .put("a", &json!({"status": "active", "n": 1}))
            .unwrap();
        users
            .put("b", &json!({"status": "paused", "n": 2}))
            .unwrap();
    }

    let mut users = db.collection("users").unwrap();
    let rows = users
        .filter_sda(r#"getPath(input, Seq["status"]) = Some("active")"#)
        .expect("filter_sda");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "a");
    assert_eq!(rows[0].1["n"], json!(1));
}

#[test]
fn collection_sda_with_limit() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("lim.residiuum")).unwrap();
    {
        let mut c = db.collection("items").unwrap();
        for i in 0..10 {
            let key = format!("k{i}");
            c.put(&key, &json!({"i": i})).unwrap();
        }
    }
    let mut c = db.collection("items").unwrap();
    // Limit applies to the host scan before SDA sees `input`.
    let out = c
        .sda_with(
            r#"{ yield getPath(u, Seq["i"]) | u in input }"#,
            QueryOptions::new().limit(3),
        )
        .unwrap();
    assert_eq!(out.as_array().unwrap().len(), 3);
}

#[test]
fn multi_collection_enr1_text_attach() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("enr-multi.residiuum")).unwrap();
    {
        let mut orders = db.collection("orders").unwrap();
        orders
            .put(
                "o1",
                &json!({"id": "o1", "customer_id": "c1", "status": "paid", "qty": 2}),
            )
            .unwrap();
        orders
            .put(
                "o2",
                &json!({"id": "o2", "customer_id": "c2", "status": "open", "qty": 1}),
            )
            .unwrap();
        orders
            .put(
                "o3",
                &json!({"id": "o3", "customer_id": "c1", "status": "paid", "qty": 5}),
            )
            .unwrap();
    }
    {
        let mut customers = db.collection("customers").unwrap();
        customers
            .put("c1", &json!({"id": "c1", "name": "Ada"}))
            .unwrap();
        customers
            .put("c2", &json!({"id": "c2", "name": "Bob"}))
            .unwrap();
    }

    // ENR1 as text: filter paid orders, attach required unique customer via one!.
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
          | getPath(o, Seq["status"]) = Some("paid")
        })
      ))
    "#;

    // Convenience API
    let out = db
        .sda(&["orders", "customers"], program)
        .expect("Residiuum::sda");
    let arr = out.get("$value").and_then(|v| v.as_array()).expect("Some");
    assert_eq!(arr.len(), 2);
    let mut by_id = std::collections::HashMap::new();
    for row in arr {
        by_id.insert(row["id"].as_str().unwrap().to_string(), row.clone());
    }
    assert_eq!(by_id["o1"]["customer"]["name"], json!("Ada"));
    assert_eq!(by_id["o3"]["customer"]["name"], json!("Ada"));
    assert!(!by_id.contains_key("o2"));

    // Builder with host-side prefilter (same result, smaller right/left bags)
    let out2 = db
        .sda_query()
        .bind("orders")
        .filter(Filter::field("status").eq("paid"))
        .bind("customers")
        .run(
            r#"
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
        "#,
        )
        .expect("sda_query.run");
    let arr2 = out2.get("$value").and_then(|v| v.as_array()).unwrap();
    assert_eq!(arr2.len(), 2);
}

#[test]
fn enr1_missing_match_surfaces_as_fail() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("enr-miss.residiuum")).unwrap();
    {
        let mut orders = db.collection("orders").unwrap();
        orders
            .put("o1", &json!({"id": "o1", "customer_id": "missing"}))
            .unwrap();
    }
    {
        let mut customers = db.collection("customers").unwrap();
        customers
            .put("c1", &json!({"id": "c1", "name": "Ada"}))
            .unwrap();
    }

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

    let out = db.sda(&["orders", "customers"], program).expect("eval");
    // one! fails → Fail value is attached under "customer" (merge still yields the row).
    let arr = out.get("$value").and_then(|v| v.as_array()).unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["customer"]["$type"], json!("fail"));
    assert_eq!(arr[0]["customer"]["$code"], json!("t_enr_missing"));
    assert_eq!(arr[0]["id"], json!("o1"));
}

#[test]
fn fluent_join_and_text_enr_are_both_available() {
    // Regression lock: text path does not replace MultiQuery; both ship.
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("both.residiuum")).unwrap();
    {
        let mut a = db.collection("a").unwrap();
        a.put("1", &json!({"id": 1})).unwrap();
    }
    {
        let mut b = db.collection("b").unwrap();
        b.put("10", &json!({"a_id": 1, "y": "yy"})).unwrap();
    }

    let join_rows = db
        .query()
        .from("a")
        .join("b")
        .on("id", "a_id")
        .collect()
        .unwrap();
    assert_eq!(join_rows.len(), 1);

    // Same relationship as ENR1 text: host supplies both bags; match + one!.
    let text = db
        .sda(
            &["a", "b"],
            r#"
          bindOpt(getPath(input, Seq["a"]), lefts =>
          bindOpt(getPath(input, Seq["b"]), rights =>
            Some({
              yield left + Map{
                "b" -> one!({
                  right | right in rights
                    | getPath(right, Seq["a_id"]) = getPath(left, Seq["id"])
                })
              }
              | left in lefts
            })
          ))
        "#,
        )
        .unwrap();
    let arr = text.get("$value").and_then(|v| v.as_array()).unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["b"]["y"], json!("yy"));
    assert_eq!(arr[0]["id"], json!(1));
}

#[test]
fn enr_query_match_enrich_refine_pipeline() {
    // Preferred ENR surface: free collection names + Match + enrich + refine pipe.
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("enr-pipe.residiuum")).unwrap();
    {
        let mut orders = db.collection("orders").unwrap();
        orders
            .put(
                "o1",
                &json!({"id": "o1", "customer_id": "c1", "status": "paid"}),
            )
            .unwrap();
        orders
            .put(
                "o2",
                &json!({"id": "o2", "customer_id": "c2", "status": "open"}),
            )
            .unwrap();
    }
    {
        let mut customers = db.collection("customers").unwrap();
        customers
            .put("c1", &json!({"id": "c1", "name": "Ada"}))
            .unwrap();
        customers
            .put("c2", &json!({"id": "c2", "name": "Bob"}))
            .unwrap();
    }

    let out = db
        .enr_query()
        .bind("orders")
        .bind("customers")
        .run(
            r#"
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
        "#,
        )
        .expect("enr_query pipeline");

    let arr = out.as_array().expect("seq of enriched rows");
    assert_eq!(arr.len(), 2);
    let mut by_id = std::collections::HashMap::new();
    for row in arr {
        by_id.insert(row["id"].as_str().unwrap().to_string(), row.clone());
    }
    assert_eq!(by_id["o1"]["customer"]["name"], json!("Ada"));
    assert_eq!(
        by_id["o1"]["customer_name"],
        json!({"$type": "some", "$value": "Ada"})
    );
    assert_eq!(by_id["o2"]["customer"]["name"], json!("Bob"));
}
