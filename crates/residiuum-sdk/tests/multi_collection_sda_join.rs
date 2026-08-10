//! Multi-collection SDA pipeline (viability check).
//!
//! Application-level pattern:
//! 1. Seed three collections (`customers`, `products`, `orders`) with fat
//!    records (payload bodies > 1 KiB of intentional garbage).
//! 2. Query each collection through an SDA-compiled filter program.
//! 3. Combine the three result sequences into one host value.
//! 4. Run a nested SDA join + filter over that combined value.
//! 5. Assert the joined projection.
//!
//! ## Is this sensible / viable?
//!
//! **Yes, as a client-side pattern.** Residiuum collections are isolated;
//! `find` is per-collection. Standalone SDA is pure algebra over values the
//! host supplies — it does not open collections or issue IO. The natural
//! composition is:
//!
//! - per-collection `find` / scan with portable filters (SDA-aligned, DEF-028)
//! - host assembles `{ customers, products, orders }`
//! - one SDA program joins and projects
//!
//! This is **not** a built-in multi-collection join engine or a cross-collection
//! transaction. Nested comprehensions are O(|orders| × |customers| × |products|)
//! for the naive form used here; keep joined sets small or reshape to maps for
//! real workloads. Fat record fields that are not projected still ride along
//! only if the host includes them in the combined value — drop garbage before
//! the join when bandwidth matters.

use residiuum_sdk::{json, Filter, Residiuum};
use sda_core::Program;
use serde_json::Value as JsonValue;
use tempfile::tempdir;

/// ~1.5 KiB of filler so each stored document body exceeds 1 KiB.
fn garbage_blob() -> String {
    // 48 × 32 = 1536 printable ASCII bytes.
    "GARBAGE-PAD-xxxxxxxxxxxxxxxx".repeat(48)
}

fn assert_body_over_1kb(doc: &JsonValue) {
    let encoded = serde_json::to_vec(doc).expect("serialize");
    assert!(
        encoded.len() > 1024,
        "expected >1KiB JSON body, got {} bytes",
        encoded.len()
    );
}

/// Scan a collection and keep rows that match a filter evaluated as SDA.
///
/// Portable [`Filter`] compiles to standalone SDA (`getPath` / comparisons).
/// This path deliberately uses the SDA evaluator rather than native
/// [`Filter::matches`], so the "query via SDA" step is real.
fn find_via_sda(
    col: &mut residiuum_sdk::Collection<'_>,
    filter: &Filter,
) -> Vec<(String, JsonValue)> {
    let program = filter.compile_sda().unwrap_or_else(|e| {
        panic!(
            "SDA compile failed for {}: {e}; src={}",
            col.name(),
            filter.to_sda()
        )
    });
    let candidates = col
        .find(&Filter::always())
        .unwrap_or_else(|e| panic!("scan {}: {e}", col.name()));
    candidates
        .into_iter()
        .filter(|(_, doc)| {
            Filter::matches_compiled_sda(&program, doc).unwrap_or_else(|e| {
                panic!(
                    "SDA match failed on {}: {e}; src={}",
                    col.name(),
                    filter.to_sda()
                )
            })
        })
        .collect()
}

fn docs_only(rows: Vec<(String, JsonValue)>) -> Vec<JsonValue> {
    rows.into_iter().map(|(_, v)| v).collect()
}

/// Nested join: paid orders with qty > 1, attach matching customer + product.
///
/// SDA surface notes used here:
/// - JSON objects are `Map`; path walk is `getPath(doc, Seq["field", ...]) → Opt`
/// - Arrays are `Seq`; comprehensions iterate `Seq` / `Set` / `Bag` only
/// - `bindOpt` unwraps `Some` so nested generators see bare sequences
/// - equality on `Opt` (`Some("paid")`) is lawful; numeric compare needs bare ints
const JOIN_SDA: &str = r#"
bindOpt(getPath(input, Seq["orders"]), orders =>
bindOpt(getPath(input, Seq["customers"]), customers =>
bindOpt(getPath(input, Seq["products"]), products =>
  Some({
    yield Map{
      "order_id" -> getPath(o, Seq["id"]),
      "customer" -> { yield getPath(c, Seq["name"])
                      | c in customers
                      | getPath(c, Seq["id"]) = getPath(o, Seq["customer_id"]) },
      "product"  -> { yield getPath(p, Seq["sku"])
                      | p in products
                      | getPath(p, Seq["id"]) = getPath(o, Seq["product_id"]) },
      "qty"      -> getPath(o, Seq["qty"]),
      "unit_price" -> { yield getPath(p, Seq["price"])
                        | p in products
                        | getPath(p, Seq["id"]) = getPath(o, Seq["product_id"]) }
    }
    | o in orders
    | bindOpt(getPath(o, Seq["status"]), s =>
        bindOpt(getPath(o, Seq["qty"]), q =>
          Some(s = "paid" and q > 1))) = Some(true)
  })
)))
"#;

#[test]
fn three_collections_sda_query_combine_join_filter() {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join("shop.residiuum")).unwrap();
    let blob = garbage_blob();

    // --- seed ---------------------------------------------------------------
    {
        let mut customers = db.collection("customers").unwrap();
        for (id, name, region, active) in [
            ("c1", "Ada Lovelace", "EU", true),
            ("c2", "Bob Builder", "US", true),
            ("c3", "Inactive Ivy", "APAC", false),
        ] {
            let doc = json!({
                "id": id,
                "name": name,
                "region": region,
                "active": active,
                "garbage": blob,
            });
            assert_body_over_1kb(&doc);
            customers.put(id, &doc).unwrap();
        }
    }
    {
        let mut products = db.collection("products").unwrap();
        for (id, sku, price, in_stock) in [
            ("p1", "WIDGET", 10, true),
            ("p2", "GADGET", 25, true),
            ("p3", "DISCO", 5, false),
        ] {
            let doc = json!({
                "id": id,
                "sku": sku,
                "price": price,
                "in_stock": in_stock,
                "garbage": blob,
            });
            assert_body_over_1kb(&doc);
            products.put(id, &doc).unwrap();
        }
    }
    {
        let mut orders = db.collection("orders").unwrap();
        // o1: paid, qty 2 → should appear (Ada × WIDGET)
        // o2: pending → filtered out
        // o3: paid, qty 3 → should appear (Ada × GADGET)
        // o4: paid, qty 1 → qty filter drops
        // o5: paid but inactive customer still joins if customer query includes them;
        //     we filter customers to active only, so o5's customer seq is empty
        //     (still a row — demonstrates left-ish join shape of nested yield)
        for (id, cid, pid, qty, status) in [
            ("o1", "c1", "p1", 2, "paid"),
            ("o2", "c2", "p2", 5, "pending"),
            ("o3", "c1", "p2", 3, "paid"),
            ("o4", "c2", "p1", 1, "paid"),
            ("o5", "c3", "p1", 4, "paid"),
        ] {
            let doc = json!({
                "id": id,
                "customer_id": cid,
                "product_id": pid,
                "qty": qty,
                "status": status,
                "garbage": blob,
            });
            assert_body_over_1kb(&doc);
            orders.put(id, &doc).unwrap();
        }
    }

    // --- per-collection SDA queries -----------------------------------------
    let customer_filter = Filter::and([
        Filter::field("active").eq(true),
        Filter::field("region").is_in(["EU", "US"]),
    ]);
    let product_filter = Filter::field("in_stock").eq(true);
    // Pull candidate orders with status paid; qty > 1 is applied in the join
    // program so the SDA join step has real filter work.
    let order_filter = Filter::field("status").eq("paid");

    // Prove DEF-028 agreement before the SDA scan path.
    for (name, filter, sample) in [
        (
            "customer",
            &customer_filter,
            json!({"id":"c1","active":true,"region":"EU","name":"x","garbage":"y"}),
        ),
        (
            "product",
            &product_filter,
            json!({"id":"p1","in_stock":true,"sku":"W","price":1,"garbage":"y"}),
        ),
        (
            "order",
            &order_filter,
            json!({"id":"o1","status":"paid","qty":2,"customer_id":"c1","product_id":"p1","garbage":"y"}),
        ),
    ] {
        let native = filter.matches(&sample);
        let sda = filter.matches_sda(&sample).unwrap();
        assert_eq!(native, sda, "{name}: native≠sda; {}", filter.to_sda());
        assert!(native, "{name}: sample should match");
    }

    let customers = docs_only(find_via_sda(
        &mut db.collection("customers").unwrap(),
        &customer_filter,
    ));
    let products = docs_only(find_via_sda(
        &mut db.collection("products").unwrap(),
        &product_filter,
    ));
    let orders = docs_only(find_via_sda(
        &mut db.collection("orders").unwrap(),
        &order_filter,
    ));

    assert_eq!(customers.len(), 2, "active EU/US customers");
    assert_eq!(products.len(), 2, "in-stock products");
    assert_eq!(orders.len(), 4, "paid orders (qty filter later)");

    // Garbage still present on each queried row (host did not strip it).
    for doc in customers.iter().chain(products.iter()).chain(orders.iter()) {
        assert_body_over_1kb(doc);
    }

    // --- combine ------------------------------------------------------------
    let combined = json!({
        "customers": customers,
        "products": products,
        "orders": orders,
    });

    // --- join + filter via SDA ----------------------------------------------
    let program = Program::parse(JOIN_SDA).expect("join program parses");
    let out = program
        .run_json("input", combined)
        .expect("join program evaluates");

    // bindOpt chain → Some(Seq[...])
    let rows = out
        .get("$value")
        .cloned()
        .unwrap_or_else(|| panic!("expected Opt Some wrapper, got {out}"));
    let rows = rows
        .as_array()
        .unwrap_or_else(|| panic!("expected array, got {rows}"));

    // Expected after join filter (paid ∧ qty > 1) against *active* customers
    // and *in-stock* products:
    // - o1: Ada × WIDGET, qty 2, line 20
    // - o3: Ada × GADGET, qty 3, line 75
    // - o4: qty 1 dropped
    // - o5: customer c3 inactive → customer/product may still match product,
    //       but customer yield is empty; we still count rows with non-empty
    //       customer side only for the assertion below.
    let mut hits: Vec<(String, String, String, i64)> = Vec::new();
    for row in rows {
        let order_id = unwrap_some_str(row.get("order_id").expect("order_id"));
        let customer = first_some_str(row.get("customer").expect("customer"));
        let product = first_some_str(row.get("product").expect("product"));
        let qty = unwrap_some_i64(row.get("qty").expect("qty"));
        if customer.is_some() && product.is_some() {
            hits.push((order_id, customer.unwrap(), product.unwrap(), qty));
        }
    }

    hits.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        hits,
        vec![
            ("o1".into(), "Ada Lovelace".into(), "WIDGET".into(), 2),
            ("o3".into(), "Ada Lovelace".into(), "GADGET".into(), 3),
        ],
        "joined paid qty>1 lines for active customers"
    );

    // unit_price is a Seq from the product-side nested yield; o1 → WIDGET @ 10.
    let o1 = rows
        .iter()
        .find(|r| unwrap_some_str(r.get("order_id").unwrap()) == "o1")
        .expect("o1 row");
    let prices = o1
        .get("unit_price")
        .and_then(|v| v.as_array())
        .expect("unit_price seq");
    assert_eq!(prices.len(), 1);
    assert_eq!(unwrap_some_i64(&prices[0]), 10);
    // Host can finish arithmetic after SDA projection if desired:
    assert_eq!(unwrap_some_i64(o1.get("qty").unwrap()) * 10, 20);
}

fn unwrap_some_str(v: &JsonValue) -> String {
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

fn unwrap_some_i64(v: &JsonValue) -> i64 {
    match v {
        JsonValue::Number(n) => n.as_i64().expect("i64"),
        JsonValue::Object(m) if m.get("$type") == Some(&json!("some")) => m
            .get("$value")
            .and_then(|x| x.as_i64())
            .unwrap_or_else(|| panic!("Some non-i64: {v}")),
        _ => panic!("expected number or Some(number), got {v}"),
    }
}

fn first_some_str(v: &JsonValue) -> Option<String> {
    let arr = v.as_array()?;
    let first = arr.first()?;
    Some(unwrap_some_str(first))
}

// ---------------------------------------------------------------------------
// Performance harness (diagnostic; not an SLO gate)
// ---------------------------------------------------------------------------
//
// Run (release, show numbers):
//   cargo test -p residiuum-sdk --release --test multi_collection_sda_join \
//     multi_collection_sda_join_perf -- --nocapture
//
// Optional cliff sample (~30–40s wall, one core ~100% CPU — not a hang):
//   MULTI_JOIN_BENCH_STRESS=1 cargo test -p residiuum-sdk --release \
//     --test multi_collection_sda_join multi_collection_sda_join_perf -- --nocapture
//
// Phases (each prints a progress line *before* the work so 99% CPU is not silent):
//   seed              — buffered puts of >1 KiB docs
//   find_sda          — per-collection scan + compiled SDA filter
//   strip_combine     — drop garbage field, assemble host value
//   join_parse        — Program::parse once
//   join_json_bridge  — from_json once + to_json once (bridge attribution)
//   join_eval         — compile-once Program::eval on pre-converted Value
//   join_eval_repeat  — same program × N (pure eval; no re-bridge)
//
// Expected walls (release, M4-class; nested JOIN_SDA):
//   demo  (3/3/5)     — sub-second
//   small (30/20/100) — ~2s total; join ~150 ms
//   mod   (100/50/500)— ~10–15s if stress on; join ~8 s (O(orders×lookups))
//
// 99% of one core with no new lines mid-join is **CPU-bound nested eval**, not a
// deadlock. Kill only if a phase prints and then stalls far past the bounds above.
// Numbers are diagnostic only. See doc/reference/operations/BENCHMARK_DISCLOSURE.md.

use std::io::{self, Write};
use std::time::Instant;

fn progress(label: &str, msg: &str) {
    let _ = writeln!(io::stderr(), "[{label}] {msg}");
    let _ = io::stderr().flush();
}

/// Seed `n_customers` / `n_products` / `n_orders` with >1 KiB bodies.
/// Returns (active_customers, in_stock_products, paid_orders) expected counts
/// under the same filters as the correctness test (scaled).
fn seed_shop(
    db: &mut Residiuum,
    blob: &str,
    n_customers: usize,
    n_products: usize,
    n_orders: usize,
) -> (usize, usize, usize) {
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
            // ~2/3 active; region filter keeps EU/US only among active.
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

    // Expected under filters used below (approximate closed form for asserts).
    let mut expect_c = 0usize;
    for i in 0..n_customers {
        let region = if i % 3 == 0 {
            "APAC"
        } else if i % 2 == 0 {
            "US"
        } else {
            "EU"
        };
        let active = i % 3 != 2;
        if active && (region == "EU" || region == "US") {
            expect_c += 1;
        }
    }
    let expect_p = (0..n_products).filter(|i| i % 4 != 3).count();
    let expect_o = (0..n_orders).filter(|i| i % 5 != 0).count(); // paid
    (expect_c, expect_p, expect_o)
}

fn strip_garbage(docs: Vec<JsonValue>) -> Vec<JsonValue> {
    docs.into_iter()
        .map(|mut d| {
            if let Some(obj) = d.as_object_mut() {
                obj.remove("garbage");
            }
            d
        })
        .collect()
}

fn run_scale(
    label: &str,
    n_customers: usize,
    n_products: usize,
    n_orders: usize,
    join_iters: usize,
) {
    let dir = tempdir().unwrap();
    let mut db = Residiuum::open(dir.path().join(format!("shop_{label}.residiuum"))).unwrap();
    let blob = garbage_blob();
    let body_bytes = {
        let sample = json!({"id":"x","garbage": blob});
        serde_json::to_vec(&sample).unwrap().len()
    };

    let seed_docs = n_customers + n_products + n_orders;
    progress(
        label,
        &format!("seed {seed_docs} docs (c={n_customers} p={n_products} o={n_orders})…"),
    );
    let t0 = Instant::now();
    let (expect_c, expect_p, expect_o) =
        seed_shop(&mut db, &blob, n_customers, n_products, n_orders);
    let seed_ms = t0.elapsed().as_secs_f64() * 1e3;
    let seed_docs_s = seed_docs as f64 / (t0.elapsed().as_secs_f64().max(1e-9));
    progress(label, &format!("seed done {seed_ms:.1} ms"));

    let customer_filter = Filter::and([
        Filter::field("active").eq(true),
        Filter::field("region").is_in(["EU", "US"]),
    ]);
    let product_filter = Filter::field("in_stock").eq(true);
    let order_filter = Filter::field("status").eq("paid");

    // Compile once (A2); time only the scan+match path.
    progress(label, "find_sda (scan + compiled filter)…");
    let t0 = Instant::now();
    let customers = docs_only(find_via_sda(
        &mut db.collection("customers").unwrap(),
        &customer_filter,
    ));
    let products = docs_only(find_via_sda(
        &mut db.collection("products").unwrap(),
        &product_filter,
    ));
    let orders = docs_only(find_via_sda(
        &mut db.collection("orders").unwrap(),
        &order_filter,
    ));
    let find_ms = t0.elapsed().as_secs_f64() * 1e3;
    let find_docs_s = seed_docs as f64 / (t0.elapsed().as_secs_f64().max(1e-9));
    progress(label, &format!("find_sda done {find_ms:.1} ms"));

    assert_eq!(customers.len(), expect_c, "{label}: customers");
    assert_eq!(products.len(), expect_p, "{label}: products");
    assert_eq!(orders.len(), expect_o, "{label}: orders");

    // Strip fat fields before join (bandwidth hygiene called out in the module docs).
    progress(label, "strip garbage + combine…");
    let t0 = Instant::now();
    let combined = json!({
        "customers": strip_garbage(customers),
        "products": strip_garbage(products),
        "orders": strip_garbage(orders),
    });
    let combined_bytes = serde_json::to_vec(&combined).unwrap().len();
    let strip_ms = t0.elapsed().as_secs_f64() * 1e3;

    progress(label, "join Program::parse…");
    let t0 = Instant::now();
    let program = Program::parse(JOIN_SDA).expect("join parse");
    let parse_ms = t0.elapsed().as_secs_f64() * 1e3;

    // Bridge attribution: one from_json + one to_json, separate from pure eval.
    // Nested join cost is dominated by eval; re-bridging every loop iteration
    // made earlier runs look "stuck" at 99% CPU with no phase lines.
    progress(
        label,
        &format!(
            "join JSON bridge once (from_json+to_json); filter sets c={expect_c} p={expect_p} o={expect_o}…"
        ),
    );
    let t0 = Instant::now();
    let input_val = sda_core::from_json(combined);
    let from_json_ms = t0.elapsed().as_secs_f64() * 1e3;

    // Warmup pure eval (clones input each time — env takes ownership).
    progress(
        label,
        &format!("join warmup eval (nested O(orders×lookups); multi-second is normal at mod)…"),
    );
    let _ = program
        .eval("input", input_val.clone())
        .expect("join warmup");

    progress(label, "join_once (pure eval)…");
    let t0 = Instant::now();
    let out_val = program.eval("input", input_val.clone()).expect("join eval");
    let join_once_ms = t0.elapsed().as_secs_f64() * 1e3;
    progress(label, &format!("join_once done {join_once_ms:.1} ms"));

    let t0 = Instant::now();
    let out_json = sda_core::to_json(out_val.clone());
    let to_json_ms = t0.elapsed().as_secs_f64() * 1e3;
    let bridge_ms = from_json_ms + to_json_ms;

    let join_iters = join_iters.max(1);
    progress(
        label,
        &format!("join_loop ×{join_iters} pure eval (progress every iter if slow)…"),
    );
    let t0 = Instant::now();
    let mut last_beat = Instant::now();
    for i in 0..join_iters {
        let _ = program.eval("input", input_val.clone()).expect("join loop");
        // Heartbeat when an iter is long or every second so kills aren't "premature".
        if last_beat.elapsed().as_millis() >= 1000 || join_once_ms > 500.0 {
            progress(
                label,
                &format!(
                    "join_loop {i}/{join_iters} elapsed {:.1}s",
                    t0.elapsed().as_secs_f64()
                ),
            );
            last_beat = Instant::now();
        }
    }
    let join_loop = t0.elapsed();
    let join_loop_ms = join_loop.as_secs_f64() * 1e3;
    let join_mean_ms = join_loop_ms / join_iters as f64;
    let join_s = join_iters as f64 / join_loop.as_secs_f64().max(1e-9);

    // Count projected hits (customer+product both non-empty).
    let rows = out_json
        .get("$value")
        .and_then(|v| v.as_array())
        .expect("join result array");
    let mut hits = 0usize;
    for row in rows {
        let customer = first_some_str(row.get("customer").expect("customer"));
        let product = first_some_str(row.get("product").expect("product"));
        if customer.is_some() && product.is_some() {
            hits += 1;
        }
    }

    // Rough work units for disclosure (not a formal complexity proof).
    let work_units = expect_o as u64 * (expect_c as u64 + 2 * expect_p as u64);

    eprintln!(
        "\n=== multi_collection_sda_join_perf scale={label} ===\n\
         n_customers={n_customers} n_products={n_products} n_orders={n_orders}\n\
         body≈{body_bytes} B/doc  combined_stripped={combined_bytes} B\n\
         expect filter: c={expect_c} p={expect_p} o={expect_o}  join_hits={hits}  work≈{work_units}\n\
         seed:        {seed_ms:10.2} ms  ({seed_docs_s:9.0} docs/s)\n\
         find_sda:    {find_ms:10.2} ms  ({find_docs_s:9.0} docs/s over all collections)\n\
         strip+comb:  {strip_ms:10.2} ms\n\
         join_parse:  {parse_ms:10.2} ms\n\
         from_json:   {from_json_ms:10.2} ms\n\
         to_json:     {to_json_ms:10.2} ms  (bridge total {bridge_ms:.2} ms)\n\
         join_once:   {join_once_ms:10.2} ms  (pure eval)\n\
         join_loop:   {join_loop_ms:10.2} ms / {join_iters} = {join_mean_ms:8.3} ms mean  ({join_s:8.1} joins/s)"
    );
    println!(
        "{{\"phase\":\"multi_collection_sda_join\",\"scale\":\"{label}\",\
         \"n_customers\":{n_customers},\"n_products\":{n_products},\"n_orders\":{n_orders},\
         \"body_bytes\":{body_bytes},\"combined_bytes\":{combined_bytes},\
         \"expect_c\":{expect_c},\"expect_p\":{expect_p},\"expect_o\":{expect_o},\"join_hits\":{hits},\
         \"work_units\":{work_units},\
         \"seed_ms\":{seed_ms:.3},\"seed_docs_s\":{seed_docs_s:.1},\
         \"find_sda_ms\":{find_ms:.3},\"find_docs_s\":{find_docs_s:.1},\
         \"strip_combine_ms\":{strip_ms:.3},\"join_parse_ms\":{parse_ms:.3},\
         \"from_json_ms\":{from_json_ms:.3},\"to_json_ms\":{to_json_ms:.3},\
         \"join_once_ms\":{join_once_ms:.3},\"join_mean_ms\":{join_mean_ms:.3},\
         \"join_iters\":{join_iters},\"joins_s\":{join_s:.2}}}"
    );

    // CI absurdity bounds only (not performance gates).
    assert!(seed_ms < 120_000.0, "{label}: seed absurdly slow");
    assert!(find_ms < 120_000.0, "{label}: find absurdly slow");
    assert!(join_once_ms < 60_000.0, "{label}: join absurdly slow");
}

#[test]
fn multi_collection_sda_join_perf() {
    // Demo scale (same order of magnitude as the correctness test).
    run_scale("demo", 3, 3, 5, 50);

    // Small product-ish set: exposes nested-join cost without multi-minute walls.
    // Complexity is roughly O(|orders| × (|customers| + |products|)) per join
    // (nested yields over full sequences for each order row).
    run_scale("small", 30, 20, 100, 5);

    // Optional larger cliff sample: `MULTI_JOIN_BENCH_STRESS=1`.
    // One timed join is enough for the cliff number; warmup already exercised once.
    // Expect ~8s pure eval + seed ~2.5s; one core near 100% is normal, not stuck.
    if std::env::var("MULTI_JOIN_BENCH_STRESS").ok().as_deref() == Some("1") {
        run_scale("mod", 100, 50, 500, 1);
    }
}
