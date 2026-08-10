//! Multi-collection join axis (`Residiuum::query`) + SDA normalisation.
//!
//! Pipeline under test:
//! 1. Seed `customers` / `products` / `orders`.
//! 2. Host equijoin: orders ⨝ customers ON customer_id=id ⨝ products ON product_id=id
//!    with SQL/Mongo-ish filters (the "rough joined pile").
//! 3. Pass that bag through pure SDA for projection / normalisation.
//!
//! Complements `multi_collection_sda_join` (which joins *inside* SDA). Here the
//! join axis is the SDK; SDA only shapes the result.

use residiuum_sdk::{json, Filter, Residiuum, MULTI_QUERY_PROFILE};
use serde_json::Value as JsonValue;
use tempfile::tempdir;

/// Project joined rows into a flat line-item shape (normalisation step).
const NORMALISE_SDA: &str = r#"
{
  yield Map{
    "order_id"    -> getPath(row, Seq["orders", "id"]),
    "customer"    -> getPath(row, Seq["customers", "name"]),
    "product"     -> getPath(row, Seq["products", "sku"]),
    "qty"         -> getPath(row, Seq["orders", "qty"]),
    "unit_price"  -> getPath(row, Seq["products", "price"])
  }
  | row in input
  | bindOpt(getPath(row, Seq["orders", "qty"]), q => Some(q > 1)) = Some(true)
}
"#;

fn seed_shop(db: &mut Residiuum) {
    {
        let mut customers = db.collection("customers").unwrap();
        for (id, name, region, active) in [
            ("c1", "Ada Lovelace", "EU", true),
            ("c2", "Bob Builder", "US", true),
            ("c3", "Inactive Ivy", "APAC", false),
        ] {
            customers
                .put(
                    id,
                    &json!({
                        "id": id,
                        "name": name,
                        "region": region,
                        "active": active,
                    }),
                )
                .unwrap();
        }
    }
    {
        let mut products = db.collection("products").unwrap();
        for (id, sku, price, in_stock) in [
            ("p1", "WIDGET", 10, true),
            ("p2", "GADGET", 25, true),
            ("p3", "DISCO", 5, false),
        ] {
            products
                .put(
                    id,
                    &json!({
                        "id": id,
                        "sku": sku,
                        "price": price,
                        "in_stock": in_stock,
                    }),
                )
                .unwrap();
        }
    }
    {
        let mut orders = db.collection("orders").unwrap();
        // o1: paid qty 2 → keep
        // o2: pending → filtered out at source
        // o3: paid qty 3 → keep
        // o4: paid qty 1 → joined but dropped by SDA qty>1
        // o5: paid inactive customer → filtered out on customers source
        for (id, cid, pid, qty, status) in [
            ("o1", "c1", "p1", 2, "paid"),
            ("o2", "c2", "p2", 5, "pending"),
            ("o3", "c1", "p2", 3, "paid"),
            ("o4", "c2", "p1", 1, "paid"),
            ("o5", "c3", "p1", 4, "paid"),
        ] {
            orders
                .put(
                    id,
                    &json!({
                        "id": id,
                        "customer_id": cid,
                        "product_id": pid,
                        "qty": qty,
                        "status": status,
                    }),
                )
                .unwrap();
        }
    }
}

#[test]
fn multi_query_join_bag_then_sda_normalise() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("shop.residiuum")).unwrap();
    seed_shop(&mut db);

    // --- axis 1: multi-collection equijoin (rough pile) --------------------
    let bag = db
        .query()
        .from("orders")
        .where_eq("status", "paid")
        .join("customers")
        .filter(Filter::and([
            Filter::field("active").eq(true),
            Filter::field("region").is_in(["EU", "US"]),
        ]))
        .on("customer_id", "id")
        .join("products")
        .where_eq("in_stock", true)
        .on("product_id", "id")
        .collect()
        .expect("join collect");

    // Paid orders that match active EU/US customers and in-stock products:
    // o1 (c1×p1), o3 (c1×p2), o4 (c2×p1). o2 pending; o5 inactive customer.
    assert_eq!(bag.len(), 3, "joined bag size: {bag:?}");

    // Namespaced shape.
    for row in &bag {
        assert!(row.get("orders").is_some());
        assert!(row.get("customers").is_some());
        assert!(row.get("products").is_some());
    }

    let plan = db
        .query()
        .from("orders")
        .join("customers")
        .on("customer_id", "id")
        .join("products")
        .on("product_id", "id")
        .describe();
    assert_eq!(plan["profile"], MULTI_QUERY_PROFILE);
    assert_eq!(plan["joins"].as_array().unwrap().len(), 2);

    // --- axis 2: SDA normalisation over the pile ---------------------------
    let out = residiuum_sdk::map_joined_sda(&bag, NORMALISE_SDA).expect("sda map");
    let rows = out
        .as_array()
        .unwrap_or_else(|| panic!("expected Seq array from SDA, got {out}"));

    let mut hits: Vec<(String, String, String, i64, i64)> = rows
        .iter()
        .map(|r| {
            (
                unwrap_opt_str(r.get("order_id").unwrap()),
                unwrap_opt_str(r.get("customer").unwrap()),
                unwrap_opt_str(r.get("product").unwrap()),
                unwrap_opt_i64(r.get("qty").unwrap()),
                unwrap_opt_i64(r.get("unit_price").unwrap()),
            )
        })
        .collect();
    hits.sort_by(|a, b| a.0.cmp(&b.0));

    // qty > 1 drops o4; remaining o1 + o3.
    assert_eq!(
        hits,
        vec![
            ("o1".into(), "Ada Lovelace".into(), "WIDGET".into(), 2, 10),
            ("o3".into(), "Ada Lovelace".into(), "GADGET".into(), 3, 25),
        ]
    );
}

#[test]
fn multi_query_map_sda_one_shot() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("shop2.residiuum")).unwrap();
    seed_shop(&mut db);

    let out = db
        .query()
        .from("orders")
        .where_eq("status", "paid")
        .join("customers")
        .where_eq("active", true)
        .on("customer_id", "id")
        .join("products")
        .where_eq("in_stock", true)
        .on("product_id", "id")
        .map_sda(NORMALISE_SDA)
        .expect("join+sda");

    let rows = out.as_array().expect("sda seq");
    let mut ids: Vec<String> = rows
        .iter()
        .map(|r| unwrap_opt_str(r.get("order_id").unwrap()))
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["o1".to_string(), "o3".to_string()]);
}

#[test]
fn multi_query_include_keys_and_limit() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("k.residiuum")).unwrap();
    seed_shop(&mut db);

    let rows = db
        .query()
        .from("orders")
        .where_eq("status", "paid")
        .join("customers")
        .on("customer_id", "id")
        .include_keys()
        .limit(2)
        .collect()
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows[0]["orders"].get("_key").is_some());
    assert!(rows[0]["customers"].get("_key").is_some());
}

#[test]
fn multi_query_on_from_explicit_left() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("e.residiuum")).unwrap();
    {
        let mut o = db.collection("orders").unwrap();
        o.put(
            "o1",
            &json!({"id": "o1", "customer_id": "c1", "product_id": "p1"}),
        )
        .unwrap();
    }
    {
        let mut c = db.collection("customers").unwrap();
        c.put("c1", &json!({"id": "c1", "name": "Ada"})).unwrap();
    }
    {
        let mut p = db.collection("products").unwrap();
        p.put("p1", &json!({"id": "p1", "sku": "W"})).unwrap();
    }

    // Explicit left alias (same as default FROM for the second join).
    let rows = db
        .query()
        .from("orders")
        .join("customers")
        .on_from("orders", "customer_id", "id")
        .join("products")
        .on_from("orders", "product_id", "id")
        .collect()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["customers"]["name"], json!("Ada"));
    assert_eq!(rows[0]["products"]["sku"], json!("W"));
}

#[test]
fn multi_query_requires_from() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("empty.residiuum")).unwrap();
    let err = db.query().collect().unwrap_err();
    assert_eq!(err.code(), residiuum_sdk::ErrorCode::QueryInvalid);
}

fn unwrap_opt_str(v: &JsonValue) -> String {
    match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Object(m) if m.get("$type") == Some(&json!("some")) => m
            .get("$value")
            .and_then(|x| x.as_str())
            .unwrap_or_else(|| panic!("Some non-string: {v}"))
            .to_string(),
        _ => panic!("expected string or Some(string), got {v}"),
    }
}

fn unwrap_opt_i64(v: &JsonValue) -> i64 {
    match v {
        JsonValue::Number(n) => n.as_i64().expect("i64"),
        JsonValue::Object(m) if m.get("$type") == Some(&json!("some")) => m
            .get("$value")
            .and_then(|x| x.as_i64())
            .unwrap_or_else(|| panic!("Some non-i64: {v}")),
        _ => panic!("expected number or Some(number), got {v}"),
    }
}

// ---------------------------------------------------------------------------
// Performance harness — host hash equijoin + SDA normalise (diagnostic)
// ---------------------------------------------------------------------------
//
// Compare against nested-SDA join in `multi_collection_sda_join_perf` at the
// same (c,p,o) scales. Host join does the X=Y work; SDA only projects.
//
// Run (release, show numbers):
//   cargo test -p residiuum-sdk --release --test multi_query_join_sda \
//     multi_query_join_perf -- --nocapture
//
// Optional larger sample:
//   MULTI_JOIN_BENCH_STRESS=1 cargo test -p residiuum-sdk --release \
//     --test multi_query_join_sda multi_query_join_perf -- --nocapture
//
// Numbers are diagnostic only. See doc/reference/operations/BENCHMARK_DISCLOSURE.md.

use std::io::{self, Write};
use std::time::Instant;

fn progress(label: &str, msg: &str) {
    let _ = writeln!(io::stderr(), "[{label}] {msg}");
    let _ = io::stderr().flush();
}

/// ~1.5 KiB filler so stored bodies match the nested-SDA harness.
fn garbage_blob() -> String {
    "GARBAGE-PAD-xxxxxxxxxxxxxxxx".repeat(48)
}

/// Seed scaled shop data (same layout as multi_collection_sda_join seed).
fn seed_shop_scaled(
    db: &mut Residiuum,
    blob: &str,
    n_customers: usize,
    n_products: usize,
    n_orders: usize,
) {
    {
        let mut customers = db.collection("customers").unwrap();
        for i in 0..n_customers {
            let id = format!("c{i}");
            let region = if i % 3 == 0 {
                "APAC"
            } else if i % 2 == 0 {
                "US"
            } else {
                "EU"
            };
            let active = i % 3 != 2;
            customers
                .put(
                    &id,
                    &json!({
                        "id": id,
                        "name": format!("Customer {i}"),
                        "region": region,
                        "active": active,
                        "garbage": blob,
                    }),
                )
                .unwrap();
        }
    }
    {
        let mut products = db.collection("products").unwrap();
        for i in 0..n_products {
            let id = format!("p{i}");
            products
                .put(
                    &id,
                    &json!({
                        "id": id,
                        "sku": format!("SKU-{i}"),
                        "price": 10 + (i % 50) as i64,
                        "in_stock": i % 4 != 3,
                        "garbage": blob,
                    }),
                )
                .unwrap();
        }
    }
    {
        let mut orders = db.collection("orders").unwrap();
        for i in 0..n_orders {
            let id = format!("o{i}");
            let cid = format!("c{}", i % n_customers.max(1));
            let pid = format!("p{}", i % n_products.max(1));
            let qty = 1 + (i % 5) as i64;
            let status = if i % 5 == 0 { "pending" } else { "paid" };
            orders
                .put(
                    &id,
                    &json!({
                        "id": id,
                        "customer_id": cid,
                        "product_id": pid,
                        "qty": qty,
                        "status": status,
                        "garbage": blob,
                    }),
                )
                .unwrap();
        }
    }
}

fn run_multi_query_scale(
    label: &str,
    n_customers: usize,
    n_products: usize,
    n_orders: usize,
    join_iters: usize,
) {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join(format!("mq_{label}.residiuum"))).unwrap();
    let blob = garbage_blob();
    let body_bytes = {
        let sample = json!({"id": "x", "garbage": blob});
        serde_json::to_vec(&sample).unwrap().len()
    };
    let seed_docs = n_customers + n_products + n_orders;

    progress(
        label,
        &format!("seed {seed_docs} docs (c={n_customers} p={n_products} o={n_orders})…"),
    );
    let t0 = Instant::now();
    seed_shop_scaled(&mut db, &blob, n_customers, n_products, n_orders);
    let seed_ms = t0.elapsed().as_secs_f64() * 1e3;
    let seed_docs_s = seed_docs as f64 / (t0.elapsed().as_secs_f64().max(1e-9));
    progress(label, &format!("seed done {seed_ms:.1} ms"));

    // Warmup one full join+sda so first timed sample is not cold-open.
    progress(label, "warmup host join + SDA normalise…");
    let _ = db
        .query()
        .from("orders")
        .where_eq("status", "paid")
        .join("customers")
        .filter(Filter::and([
            Filter::field("active").eq(true),
            Filter::field("region").is_in(["EU", "US"]),
        ]))
        .on("customer_id", "id")
        .join("products")
        .where_eq("in_stock", true)
        .on("product_id", "id")
        .map_sda(NORMALISE_SDA)
        .expect("warmup map_sda");

    // --- collect only (host hash equijoin) ---------------------------------
    progress(label, "join_collect_once (host hash equijoin)…");
    let t0 = Instant::now();
    let bag = db
        .query()
        .from("orders")
        .where_eq("status", "paid")
        .join("customers")
        .filter(Filter::and([
            Filter::field("active").eq(true),
            Filter::field("region").is_in(["EU", "US"]),
        ]))
        .on("customer_id", "id")
        .join("products")
        .where_eq("in_stock", true)
        .on("product_id", "id")
        .collect()
        .expect("join collect");
    let collect_once_ms = t0.elapsed().as_secs_f64() * 1e3;
    progress(
        label,
        &format!(
            "join_collect_once done {collect_once_ms:.1} ms (bag={})",
            bag.len()
        ),
    );

    let join_iters = join_iters.max(1);
    progress(
        label,
        &format!("join_collect_loop ×{join_iters} (find + hash join only)…"),
    );
    let t0 = Instant::now();
    for _ in 0..join_iters {
        let _ = db
            .query()
            .from("orders")
            .where_eq("status", "paid")
            .join("customers")
            .filter(Filter::and([
                Filter::field("active").eq(true),
                Filter::field("region").is_in(["EU", "US"]),
            ]))
            .on("customer_id", "id")
            .join("products")
            .where_eq("in_stock", true)
            .on("product_id", "id")
            .collect()
            .expect("join loop");
    }
    let collect_loop = t0.elapsed();
    let collect_loop_ms = collect_loop.as_secs_f64() * 1e3;
    let collect_mean_ms = collect_loop_ms / join_iters as f64;
    let collects_s = join_iters as f64 / collect_loop.as_secs_f64().max(1e-9);

    // --- SDA normalise over the already-joined bag -------------------------
    progress(label, "sda_normalise_once over joined bag…");
    let t0 = Instant::now();
    let out = residiuum_sdk::map_joined_sda(&bag, NORMALISE_SDA).expect("sda map");
    let sda_once_ms = t0.elapsed().as_secs_f64() * 1e3;
    let sda_hits = out.as_array().map(|a| a.len()).unwrap_or(0);
    progress(
        label,
        &format!("sda_normalise_once done {sda_once_ms:.1} ms (hits={sda_hits})"),
    );

    progress(
        label,
        &format!("sda_normalise_loop ×{join_iters} over fixed bag…"),
    );
    let t0 = Instant::now();
    for _ in 0..join_iters {
        let _ = residiuum_sdk::map_joined_sda(&bag, NORMALISE_SDA).expect("sda loop");
    }
    let sda_loop = t0.elapsed();
    let sda_loop_ms = sda_loop.as_secs_f64() * 1e3;
    let sda_mean_ms = sda_loop_ms / join_iters as f64;
    let sda_s = join_iters as f64 / sda_loop.as_secs_f64().max(1e-9);

    // --- one-shot map_sda (collect + normalise, product API) ---------------
    progress(label, "map_sda_once (collect + normalise one-shot)…");
    let t0 = Instant::now();
    let out2 = db
        .query()
        .from("orders")
        .where_eq("status", "paid")
        .join("customers")
        .filter(Filter::and([
            Filter::field("active").eq(true),
            Filter::field("region").is_in(["EU", "US"]),
        ]))
        .on("customer_id", "id")
        .join("products")
        .where_eq("in_stock", true)
        .on("product_id", "id")
        .map_sda(NORMALISE_SDA)
        .expect("map_sda once");
    let map_sda_once_ms = t0.elapsed().as_secs_f64() * 1e3;
    let map_hits = out2.as_array().map(|a| a.len()).unwrap_or(0);
    progress(
        label,
        &format!("map_sda_once done {map_sda_once_ms:.1} ms (hits={map_hits})"),
    );

    progress(label, &format!("map_sda_loop ×{join_iters} product path…"));
    let t0 = Instant::now();
    for _ in 0..join_iters {
        let _ = db
            .query()
            .from("orders")
            .where_eq("status", "paid")
            .join("customers")
            .filter(Filter::and([
                Filter::field("active").eq(true),
                Filter::field("region").is_in(["EU", "US"]),
            ]))
            .on("customer_id", "id")
            .join("products")
            .where_eq("in_stock", true)
            .on("product_id", "id")
            .map_sda(NORMALISE_SDA)
            .expect("map_sda loop");
    }
    let map_loop = t0.elapsed();
    let map_loop_ms = map_loop.as_secs_f64() * 1e3;
    let map_mean_ms = map_loop_ms / join_iters as f64;
    let maps_s = join_iters as f64 / map_loop.as_secs_f64().max(1e-9);

    eprintln!(
        "\n=== multi_query_join_perf scale={label} ===\n\
         n_customers={n_customers} n_products={n_products} n_orders={n_orders}\n\
         body≈{body_bytes} B/doc  bag_rows={}  sda_hits={sda_hits}  map_hits={map_hits}\n\
         seed:            {seed_ms:10.2} ms  ({seed_docs_s:9.0} docs/s)\n\
         collect_once:    {collect_once_ms:10.2} ms  (host hash equijoin + finds)\n\
         collect_loop:    {collect_loop_ms:10.2} ms / {join_iters} = {collect_mean_ms:8.3} ms mean  ({collects_s:8.1} collects/s)\n\
         sda_once:        {sda_once_ms:10.2} ms  (normalise fixed bag)\n\
         sda_loop:        {sda_loop_ms:10.2} ms / {join_iters} = {sda_mean_ms:8.3} ms mean  ({sda_s:8.1} sda/s)\n\
         map_sda_once:    {map_sda_once_ms:10.2} ms  (product one-shot)\n\
         map_sda_loop:    {map_loop_ms:10.2} ms / {join_iters} = {map_mean_ms:8.3} ms mean  ({maps_s:8.1} maps/s)",
        bag.len()
    );
    println!(
        "{{\"phase\":\"multi_query_join\",\"scale\":\"{label}\",\
         \"n_customers\":{n_customers},\"n_products\":{n_products},\"n_orders\":{n_orders},\
         \"body_bytes\":{body_bytes},\"bag_rows\":{},\"sda_hits\":{sda_hits},\"map_hits\":{map_hits},\
         \"seed_ms\":{seed_ms:.3},\"seed_docs_s\":{seed_docs_s:.1},\
         \"collect_once_ms\":{collect_once_ms:.3},\"collect_mean_ms\":{collect_mean_ms:.3},\
         \"collects_s\":{collects_s:.2},\"sda_once_ms\":{sda_once_ms:.3},\
         \"sda_mean_ms\":{sda_mean_ms:.3},\"sda_s\":{sda_s:.2},\
         \"map_sda_once_ms\":{map_sda_once_ms:.3},\"map_mean_ms\":{map_mean_ms:.3},\
         \"maps_s\":{maps_s:.2},\"join_iters\":{join_iters}}}",
        bag.len()
    );

    // CI absurdity bounds only (not performance gates).
    assert!(seed_ms < 120_000.0, "{label}: seed absurdly slow");
    assert!(
        collect_once_ms < 60_000.0,
        "{label}: host join absurdly slow"
    );
    assert!(map_sda_once_ms < 60_000.0, "{label}: map_sda absurdly slow");
}

#[test]
fn multi_query_join_perf() {
    // Same scales as multi_collection_sda_join_perf for head-to-head read.
    run_multi_query_scale("demo", 3, 3, 5, 50);
    run_multi_query_scale("small", 30, 20, 100, 20);

    if std::env::var("MULTI_JOIN_BENCH_STRESS").ok().as_deref() == Some("1") {
        run_multi_query_scale("mod", 100, 50, 500, 5);
    }
}
