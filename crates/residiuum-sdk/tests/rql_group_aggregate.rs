//! Q2 `pkg_group_aggregate` — group by + count/sum/min/max/avg on product path.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, Constraints, DeploymentId,
    HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights, SecurityRevision,
    TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::{
    compile_rql_full, execute_rql_full, CollectionBindings, HeapClient, Parameters,
    QueryRunOptions, ResidiuumDeployment,
};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::tempdir;

fn mint_cap_for(heap: HeapId, deployment: DeploymentId) -> residiuum_heap::HeapCap {
    let snap = HeapSecuritySnapshot {
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        previous_generation: None,
        grace_deadline_unix_s: None,
        master_public_key: [7u8; 32],
        previous_master_public_key: None,
        security_revision: SecurityRevision::new(1).unwrap(),
        authority_chain_head_hash: [9u8; 32],
        administrative_state: HeapAdministrativeState::Active,
        blacklist: vec![],
        policy_rights_ceiling: None,
    };
    let slot = Arc::new(HeapSlot::new(snap));
    let cert = VerifiedCertificate {
        cose_bytes: vec![0x01],
        fingerprint: [3u8; 32],
        deployment_id: deployment,
        heap_id: heap,
        authority_epoch: AuthorityEpoch::new(1).unwrap(),
        authority_generation: AuthorityGeneration::new(1).unwrap(),
        certificate_id: CertificateId::new_random().unwrap(),
        holder_public_key: [4u8; 32],
        rights: Rights::from_bits_certificate(0x0d).unwrap(),
        constraints: Constraints::empty(),
        not_before: 1,
        expires_at: 4_000_000_000,
        issuer_master_key_id: [5u8; 32],
    };
    mint_capability(
        slot,
        &cert,
        TrustedInstant {
            unix_s: 1_700_000_000,
        },
    )
    .unwrap()
}

fn uuid() -> [u8; 16] {
    *residiuum_heap::CollectionId::new_random()
        .unwrap()
        .as_bytes()
}

fn open_client() -> HeapClient {
    let dir = tempdir().unwrap();
    let root = dir.path().to_path_buf();
    // Keep tempdir alive for the process duration of the test via leak (tests are short).
    std::mem::forget(dir);
    let deployment = ResidiuumDeployment::create(&root).unwrap();
    let layout = HeapMetaLayout::new(&root);
    let dep = *DeploymentId::new_random().unwrap().as_bytes();
    let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, uuid(), "heap-agg").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
    let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
    HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)))
}

#[test]
fn group_by_region_count_and_sum() {
    let mut client = open_client();
    let mut orders = client.create_collection("orders").unwrap().collection;
    for (k, region, amount, status) in [
        ("o1", "us", 10, "paid"),
        ("o2", "us", 5, "paid"),
        ("o3", "eu", 7, "open"),
        ("o4", "eu", 3, "paid"),
        ("o5", "us", 2, "open"),
    ] {
        orders
            .put(
                k,
                &serde_json::json!({"region": region, "amount": amount, "status": status}),
            )
            .unwrap();
    }

    let page = execute_rql_full(
        &mut client,
        "from orders group by region project region, count() as order_count, sum(amount) as total_amount",
        &Parameters::default(),
        QueryRunOptions::default(),
    )
    .expect("execute group by");

    assert_eq!(page.rows.len(), 2);
    let mut by = BTreeMap::new();
    for (_k, v) in &page.rows {
        let r = v
            .get("region")
            .and_then(|x| x.as_str())
            .unwrap()
            .to_string();
        let c = v.get("order_count").and_then(|x| x.as_u64()).unwrap();
        let s = v.get("total_amount").and_then(|x| x.as_i64()).unwrap();
        by.insert(r, (c, s));
    }
    assert_eq!(by.get("us"), Some(&(3, 17)));
    assert_eq!(by.get("eu"), Some(&(2, 10)));
}

#[test]
fn group_pages_concatenate_without_truncating_high_cardinality_output() {
    let mut client = open_client();
    let mut docs = client.create_collection("docs").unwrap().collection;
    for index in 0..7 {
        docs.put(
            &format!("d{index}"),
            &serde_json::json!({"bucket": format!("b{index}")}),
        )
        .unwrap();
    }

    let source = "from docs group by _key project _key, count() as n";
    let mut options = QueryRunOptions::default();
    options.page_size = Some(3);
    let mut rows = Vec::new();
    let mut examined = 0;
    loop {
        let page =
            execute_rql_full(&mut client, source, &Parameters::default(), options.clone()).unwrap();
        examined += page.base.coverage.examined_documents;
        rows.extend(page.rows);
        if page.base.exhausted {
            break;
        }
        options.after = Some(page.base.next.expect("non-exhausted group page cursor"));
    }

    assert_eq!(rows.len(), 7);
    let mut keys = rows
        .into_iter()
        .map(|(_, value)| value["_key"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    keys.sort();
    assert_eq!(
        keys,
        (0..7).map(|index| format!("d{index}")).collect::<Vec<_>>()
    );
    assert_eq!(examined, 7, "the completed group bag must be scanned once");
}

#[test]
fn group_spool_replay_or_frontier_drift_recomputes_safely() {
    let mut client = open_client();
    let mut docs = client.create_collection("docs").unwrap().collection;
    for index in 0..7 {
        docs.put(
            &format!("d{index}"),
            &serde_json::json!({"bucket": format!("b{index}")}),
        )
        .unwrap();
    }

    let source = "from docs group by _key project _key, count() as n";
    let options = QueryRunOptions {
        page_size: Some(3),
        diagnostics: true,
        ..Default::default()
    };
    let first = execute_rql_full(&mut client, source, &Parameters::default(), options.clone())
        .expect("first group page");
    let cursor = first.base.next.clone().expect("group continuation");

    let mut resumed = options.clone();
    resumed.after = Some(cursor.clone());
    let cached = execute_rql_full(&mut client, source, &Parameters::default(), resumed.clone())
        .expect("cached group continuation");
    assert_eq!(cached.base.coverage.examined_documents, 0);

    let replayed = execute_rql_full(&mut client, source, &Parameters::default(), resumed)
        .expect("consumed spool cursor recomputes");
    assert_eq!(replayed.rows, cached.rows);
    assert_eq!(replayed.base.coverage.examined_documents, 7);

    let first = execute_rql_full(&mut client, source, &Parameters::default(), options.clone())
        .expect("fresh first group page");
    docs.put("z", &serde_json::json!({"bucket": "bz"})).unwrap();
    let mut drifted = options;
    drifted.after = first.base.next;
    let drifted = execute_rql_full(&mut client, source, &Parameters::default(), drifted)
        .expect("frontier drift recomputes");
    assert_eq!(drifted.base.coverage.examined_documents, 8);
}

#[test]
fn logical_key_grouping_cannot_be_spoofed_by_a_body_field() {
    let mut client = open_client();
    let mut docs = client.create_collection("docs").unwrap().collection;
    docs.put("a", &serde_json::json!({"_key": "spoof"}))
        .unwrap();
    docs.put("b", &serde_json::json!({"_key": "spoof"}))
        .unwrap();

    let page = execute_rql_full(
        &mut client,
        "from docs group by _key project _key, count() as n",
        &Parameters::default(),
        QueryRunOptions::default(),
    )
    .unwrap();
    let groups = page
        .rows
        .iter()
        .map(|(_, value)| {
            (
                value["_key"].as_str().unwrap().to_string(),
                value["n"].as_u64().unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(groups, BTreeMap::from([("a".into(), 1), ("b".into(), 1)]));
}

#[test]
fn logical_key_grouping_preserves_numeric_aggregates() {
    let mut client = open_client();
    let mut docs = client.create_collection("docs").unwrap().collection;
    docs.put("a", &serde_json::json!({"amount": 7})).unwrap();
    docs.put("b", &serde_json::json!({"amount": 11})).unwrap();

    let page = execute_rql_full(
        &mut client,
        "from docs group by _key project _key, count() as n, sum(amount) as total, min(amount) as lo, max(amount) as hi, avg(amount) as mean",
        &Parameters::default(),
        QueryRunOptions::default(),
    )
    .unwrap();
    let groups = page
        .rows
        .into_iter()
        .map(|(_, value)| (value["_key"].as_str().unwrap().to_string(), value))
        .collect::<BTreeMap<_, _>>();

    for (key, amount) in [("a", 7), ("b", 11)] {
        let value = &groups[key];
        assert_eq!(value["n"], 1);
        assert_eq!(value["total"], amount);
        assert_eq!(value["lo"], amount);
        assert_eq!(value["hi"], amount);
        assert_eq!(value["mean"], amount as f64);
    }
}

#[test]
fn global_min_max_avg_with_where() {
    let mut client = open_client();
    let mut orders = client.create_collection("orders").unwrap().collection;
    for (k, amount, status) in [
        ("o1", 10, "paid"),
        ("o2", 30, "paid"),
        ("o3", 5, "open"),
        ("o4", 20, "paid"),
    ] {
        orders
            .put(k, &serde_json::json!({"amount": amount, "status": status}))
            .unwrap();
    }

    let page = execute_rql_full(
        &mut client,
        r#"from orders where status = "paid" project min(amount) as min_amount, max(amount) as max_amount, avg(amount) as avg_amount"#,
        &Parameters::default(),
        QueryRunOptions::default(),
    )
    .expect("execute global agg");

    assert_eq!(page.rows.len(), 1);
    let v = &page.rows[0].1;
    assert_eq!(v.get("min_amount").and_then(|x| x.as_i64()), Some(10));
    assert_eq!(v.get("max_amount").and_then(|x| x.as_i64()), Some(30));
    // avg of 10,30,20 = 20
    assert_eq!(v.get("avg_amount").and_then(|x| x.as_i64()), Some(20));
}

#[test]
fn warmed_filtered_group_uses_validated_index_candidates() {
    let mut client = open_client();
    let mut docs = client.create_collection("docs").unwrap().collection;
    for (key, region, status) in [
        ("a", "us", "open"),
        ("b", "us", "open"),
        ("c", "us", "closed"),
        ("d", "eu", "open"),
    ] {
        docs.put(
            key,
            &serde_json::json!({"region": region, "status": status}),
        )
        .unwrap();
    }
    docs.indexes().create("by_region", &["region"]).unwrap();

    let run = |client: &mut HeapClient| {
        let page = execute_rql_full(
            client,
            r#"from docs where region = "us" group by status project status, count() as n"#,
            &Parameters::default(),
            QueryRunOptions {
                diagnostics: true,
                ..Default::default()
            },
        )
        .unwrap();
        let counts = page
            .rows
            .iter()
            .map(|(_, value)| {
                (
                    value["status"].as_str().unwrap().to_string(),
                    value["n"].as_u64().unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        (
            counts,
            page.base.coverage.examined_documents,
            page.base.diagnostics.unwrap().host_read_calls,
        )
    };

    let expected = BTreeMap::from([("closed".to_string(), 1), ("open".to_string(), 2)]);
    let cold = run(&mut client);
    assert_eq!(cold.0, expected);
    assert_eq!(cold.1, 3);
    let warm = run(&mut client);
    assert_eq!(warm.0, expected);
    assert_eq!(warm.1, 3);
    assert!(cold.2 <= 2, "cold pushdown resolves one bounded body batch");
    assert!(warm.2 <= cold.2, "warm pushdown adds no host round trip");
}

#[test]
fn projected_filter_preserves_missing_null_and_value_distinction() {
    let mut client = open_client();
    let mut docs = client.create_collection("docs").unwrap().collection;
    for (key, value) in [
        ("a", serde_json::json!({"status": "missing"})),
        ("b", serde_json::json!({"marker": null, "status": "null"})),
        ("c", serde_json::json!({"marker": "y", "status": "value"})),
        (
            "d",
            serde_json::json!({"marker": "x", "status": "excluded"}),
        ),
    ] {
        docs.put(key, &value).unwrap();
    }

    let mut options = QueryRunOptions::default();
    options.diagnostics = true;
    let page = execute_rql_full(
        &mut client,
        r#"from docs where marker != "x" group by status project status, count() as n"#,
        &Parameters::default(),
        options,
    )
    .unwrap();
    let counts = page
        .rows
        .iter()
        .map(|(_, row)| {
            (
                row["status"].as_str().unwrap().to_string(),
                row["n"].as_u64().unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        counts,
        BTreeMap::from([("null".to_string(), 1), ("value".to_string(), 1)])
    );
    assert_eq!(page.base.coverage.examined_documents, 4);
    assert!(page.base.diagnostics.unwrap().host_read_calls <= 2);
}

#[test]
fn warmed_decoded_scan_cannot_survive_a_replacement_event() {
    let mut client = open_client();
    let mut orders = client.create_collection("orders").unwrap().collection;
    orders
        .put("o1", &serde_json::json!({"region": "us", "amount": 1}))
        .unwrap();

    let run_sum = |client: &mut HeapClient| {
        let mut options = QueryRunOptions::default();
        options.diagnostics = true;
        let page = execute_rql_full(
            client,
            "from orders group by region project region, sum(amount) as total",
            &Parameters::default(),
            options,
        )
        .unwrap();
        (
            page.rows[0].1["total"].as_i64().unwrap(),
            page.base.diagnostics.unwrap().host_read_calls,
            page.base.coverage.examined_documents,
        )
    };

    assert_eq!(run_sum(&mut client), (1, 2, 1));
    assert_eq!(
        run_sum(&mut client),
        (1, 2, 1),
        "second scan uses aggregate pushdown over the warmed decoded cache"
    );
    orders
        .put("o1", &serde_json::json!({"region": "us", "amount": 9}))
        .unwrap();
    assert_eq!(
        run_sum(&mut client),
        (9, 2, 1),
        "new event must bypass old decode"
    );
}

#[test]
fn projected_group_scan_preserves_nested_missing_and_null_semantics() {
    let mut client = open_client();
    let mut docs = client.create_collection("docs").unwrap().collection;
    docs.put("a", &serde_json::json!({"profile": {"region": "us"}}))
        .unwrap();
    docs.put("b", &serde_json::json!({"profile": {"region": null}}))
        .unwrap();
    docs.put("c", &serde_json::json!({"unrelated": true}))
        .unwrap();

    let mut options = QueryRunOptions::default();
    options.diagnostics = true;
    let page = execute_rql_full(
        &mut client,
        "from docs group by profile.region project profile.region, count() as n",
        &Parameters::default(),
        options,
    )
    .unwrap();
    let counts = page
        .rows
        .iter()
        .map(|(_, row)| (row["region"].clone(), row["n"].as_u64().unwrap()))
        .collect::<Vec<_>>();
    assert!(counts.contains(&(serde_json::Value::String("us".into()), 1)));
    assert!(counts.contains(&(serde_json::Value::Null, 2)));
    assert_eq!(page.base.coverage.examined_documents, 3);
    assert_eq!(page.base.diagnostics.unwrap().host_read_calls, 2);
}

#[test]
fn complete_compound_index_covers_group_and_numeric_aggregates() {
    let mut client = open_client();
    let mut orders = client.create_collection("orders").unwrap().collection;
    for (key, region, amount) in [("a", "us", 2), ("b", "eu", 7), ("c", "us", 5)] {
        orders
            .put(
                key,
                &serde_json::json!({"region": region, "amount": amount, "ignored": "large"}),
            )
            .unwrap();
    }
    let index = orders
        .indexes()
        .create("by_region_amount", &["region", "amount"])
        .unwrap();
    assert!(index.complete_coverage);

    let mut options = QueryRunOptions::default();
    options.diagnostics = true;
    let page = execute_rql_full(
        &mut client,
        "from orders group by region project region, count() as n, sum(amount) as total, min(amount) as lo, max(amount) as hi, avg(amount) as mean",
        &Parameters::default(),
        options.clone(),
    )
    .unwrap();
    let by_region = page
        .rows
        .iter()
        .map(|(_, value)| (value["region"].as_str().unwrap(), value))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(by_region["us"]["n"], 2);
    assert_eq!(by_region["us"]["total"], 7);
    assert_eq!(by_region["us"]["lo"], 2);
    assert_eq!(by_region["us"]["hi"], 5);
    assert_eq!(by_region["us"]["mean"], 3.5);
    assert_eq!(by_region["eu"]["total"], 7);
    assert_eq!(page.base.coverage.examined_documents, 3);
    assert_eq!(page.base.diagnostics.unwrap().host_read_calls, 1);

    // A query may consume a subset of a compound covering index. The omitted
    // amount column is still decoded and validated, but is not materialized
    // into the accumulator input row.
    let counts = execute_rql_full(
        &mut client,
        "from orders group by region project region, count() as n",
        &Parameters::default(),
        options,
    )
    .unwrap();
    let counts_by_region = counts
        .rows
        .iter()
        .map(|(_, value)| {
            (
                value["region"].as_str().unwrap(),
                value["n"].as_u64().unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(counts_by_region, BTreeMap::from([("eu", 1), ("us", 2)]));
    assert_eq!(counts.base.coverage.examined_documents, 3);
    assert_eq!(counts.base.diagnostics.unwrap().host_read_calls, 1);

    // Any later write marks the covering index stale; the same query must
    // automatically fall back to authoritative documents and see the change.
    orders
        .put(
            "a",
            &serde_json::json!({"region": "us", "amount": 20, "ignored": "changed"}),
        )
        .unwrap();
    let fallback = execute_rql_full(
        &mut client,
        "from orders group by region project region, sum(amount) as total",
        &Parameters::default(),
        QueryRunOptions::default(),
    )
    .unwrap();
    let us = fallback
        .rows
        .iter()
        .find(|(_, value)| value["region"] == "us")
        .unwrap();
    assert_eq!(us.1["total"], 25);
}

#[test]
fn compile_group_plan_has_group_agg() {
    let mut client = open_client();
    let _ = client.create_collection("orders").unwrap();
    let infos = client.list_collections().unwrap();
    let mut bindings = CollectionBindings::default();
    for info in &infos {
        bindings.bind(&info.name, info.collection_id);
    }
    let compiled = compile_rql_full(
        "from orders group by status project status, count() as n",
        &bindings,
    )
    .expect("compile");
    assert!(compiled.base.plan.group_agg.is_active());
    assert_eq!(compiled.base.plan.group_agg.group_by.len(), 1);
    assert_eq!(compiled.base.plan.group_agg.aggregates.len(), 1);
}
