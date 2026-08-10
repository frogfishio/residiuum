//! Phase 3: `rql-full-v1` surface corpus (compile accept/refuse + execute oracles).
//!
//! Normative host: `spec/app/v1/rql_full_v1_corpus_v1.json`.
//! Residual inventory: `doc/todo/rql/PHASE3_SURFACE_RESIDUAL.md`.

use residiuum_heap::{
    mint_capability, AuthorityEpoch, AuthorityGeneration, CertificateId, CollectionId, Constraints,
    DeploymentId, HeapAdministrativeState, HeapId, HeapSecuritySnapshot, HeapSlot, Rights,
    SecurityRevision, TrustedInstant, VerifiedCertificate,
};
use residiuum_sdk::plan_v1::CollectionBindings;
use residiuum_sdk::rql_app_core::{compile_app_core, DIAG_RQL_FEATURE_UNAVAILABLE};
use residiuum_sdk::{
    compile_rql_full, execute_rql_full, FullPipelineStepV1, HeapClient, Parameters, ProjectItemV1,
    QueryRunOptions, ResidiuumDeployment, RQL_FULL_PROFILE,
};
use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tempfile::tempdir;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn read_json(rel: &str) -> Value {
    let path = workspace_root().join(rel);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("json {}: {e}", path.display()))
}

fn bindings_from(map: &Value) -> CollectionBindings {
    let mut b = CollectionBindings::default();
    for (name, id) in map.as_object().expect("default_bindings") {
        b.bind(
            name,
            CollectionId::from_str(id.as_str().expect("id")).expect("collection id"),
        );
    }
    b
}

fn pipeline_kinds(compiled: &residiuum_sdk::CompiledRqlFull) -> Vec<&'static str> {
    compiled
        .pipeline
        .iter()
        .map(|s| match s {
            FullPipelineStepV1::Enrich(_) => "enrich",
            FullPipelineStepV1::Within(_) => "within",
            FullPipelineStepV1::Filter(_) => "filter",
        })
        .collect()
}

fn within_step_kinds(w: &residiuum_sdk::WithinStepV1) -> Vec<&'static str> {
    w.steps
        .iter()
        .map(|s| match s {
            FullPipelineStepV1::Enrich(_) => "enrich",
            FullPipelineStepV1::Within(_) => "within",
            FullPipelineStepV1::Filter(_) => "filter",
        })
        .collect()
}

fn project_outputs(items: &[ProjectItemV1]) -> Vec<String> {
    items
        .iter()
        .map(|i| match i {
            ProjectItemV1::Leaf { output, .. }
            | ProjectItemV1::Nested { output, .. }
            | ProjectItemV1::Computed { output, .. } => output.clone(),
        })
        .collect()
}

#[test]
fn rql_full_corpus_compile_accept_and_refuse() {
    let doc = read_json("spec/app/v1/rql_full_v1_corpus_v1.json");
    assert_eq!(doc["profile"].as_str(), Some(RQL_FULL_PROFILE));
    let bindings = bindings_from(&doc["default_bindings"]);

    let accept = doc["accept"].as_array().expect("accept");
    assert!(
        accept.len() >= 6,
        "accept corpus too small: {}",
        accept.len()
    );
    for v in accept {
        let id = v["id"].as_str().unwrap_or("?");
        let src = v["source_rql"].as_str().expect("source_rql");
        let compiled = compile_rql_full(src, &bindings)
            .unwrap_or_else(|e| panic!("{id}: accept compile failed: {e}\n{src}"));
        assert_eq!(compiled.profile, RQL_FULL_PROFILE, "{id}");

        if let Some(exp) = v.get("expect") {
            if let Some(pipe) = exp.get("pipeline").and_then(|x| x.as_array()) {
                let got = pipeline_kinds(&compiled);
                let want: Vec<&str> = pipe.iter().filter_map(|x| x.as_str()).collect();
                assert_eq!(got, want, "{id}: pipeline");
            }
            if let Some(outs) = exp.get("enrich_outputs").and_then(|x| x.as_array()) {
                let got: Vec<&str> = compiled
                    .root_enrich()
                    .iter()
                    .map(|e| e.output.as_str())
                    .collect();
                let want: Vec<&str> = outs.iter().filter_map(|x| x.as_str()).collect();
                assert_eq!(got, want, "{id}: enrich_outputs");
            }
            if let Some(flag) = exp.get("project").and_then(|x| x.as_bool()) {
                assert_eq!(compiled.project.is_some(), flag, "{id}: project");
            }
            if let Some(pouts) = exp.get("project_outputs").and_then(|x| x.as_array()) {
                let items = compiled.project.as_ref().expect("project");
                let got = project_outputs(items);
                let want: Vec<&str> = pouts.iter().filter_map(|x| x.as_str()).collect();
                assert_eq!(got, want, "{id}: project_outputs");
            }
            if let Some(ps) = exp.get("page_size").and_then(|x| x.as_u64()) {
                assert_eq!(
                    u64::from(compiled.base.plan.page_size),
                    ps,
                    "{id}: page_size"
                );
            }
            if let Some(s) = exp.get("base_contains").and_then(|x| x.as_str()) {
                assert!(
                    compiled.base_source.contains(s),
                    "{id}: base_contains `{s}` in `{}`",
                    compiled.base_source
                );
            }
            if let Some(s) = exp.get("base_excludes").and_then(|x| x.as_str()) {
                assert!(
                    !compiled.base_source.contains(s),
                    "{id}: base_excludes `{s}` but found in `{}`",
                    compiled.base_source
                );
            }
            if let Some(true) = exp.get("candidate_where").and_then(|x| x.as_bool()) {
                assert!(
                    compiled.root_enrich()[0].candidate_where.is_some(),
                    "{id}: candidate_where"
                );
            }
            if let Some(wouts) = exp.get("within_enrich_outputs").and_then(|x| x.as_array()) {
                let w = compiled.first_within().expect("within");
                let got: Vec<&str> = w.enrich_steps().iter().map(|e| e.output.as_str()).collect();
                let want: Vec<&str> = wouts.iter().filter_map(|x| x.as_str()).collect();
                assert_eq!(got, want, "{id}: within_enrich_outputs");
            }
            if let Some(ws) = exp.get("within_steps").and_then(|x| x.as_array()) {
                let w = compiled.first_within().expect("within");
                let got = within_step_kinds(w);
                let want: Vec<&str> = ws.iter().filter_map(|x| x.as_str()).collect();
                assert_eq!(got, want, "{id}: within_steps");
            }
        }
    }

    let refuse = doc["refuse"].as_array().expect("refuse");
    assert!(
        refuse.len() >= 4,
        "refuse corpus too small: {}",
        refuse.len()
    );
    for v in refuse {
        let id = v["id"].as_str().unwrap_or("?");
        let src = v["source_rql"].as_str().expect("source_rql");
        let needle = v["diagnostic_contains"].as_str().expect("diagnostic");
        let err = match v.get("surface").and_then(|x| x.as_str()) {
            Some("rql-app-core-v1") => compile_app_core(src, &bindings).unwrap_err(),
            _ => compile_rql_full(src, &bindings).unwrap_err(),
        };
        let msg = err.to_string();
        assert!(msg.contains(needle), "{id}: expected `{needle}` in `{msg}`");
        if needle == DIAG_RQL_FEATURE_UNAVAILABLE {
            assert!(msg.contains(DIAG_RQL_FEATURE_UNAVAILABLE), "{id}");
        }
    }
}

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
    *CollectionId::new_random().unwrap().as_bytes()
}

fn json_path<'a>(v: &'a Value, path: &str) -> &'a Value {
    let mut cur = v;
    for seg in path.split('.') {
        if let Ok(i) = seg.parse::<usize>() {
            cur = cur
                .as_array()
                .unwrap_or_else(|| panic!("path {path}: not array at {seg}"))
                .get(i)
                .unwrap_or_else(|| panic!("path {path}: missing index {i}"));
        } else {
            cur = cur
                .get(seg)
                .unwrap_or_else(|| panic!("path {path}: missing `{seg}`"));
        }
    }
    cur
}

#[test]
fn rql_full_corpus_execute_oracles() {
    let doc = read_json("spec/app/v1/rql_full_v1_corpus_v1.json");
    let cases = doc["execute"].as_array().expect("execute");
    assert!(cases.len() >= 3, "execute corpus too small");

    for case in cases {
        let id = case["id"].as_str().unwrap_or("?");
        let dir = tempdir().unwrap();
        let root = dir.path();
        let deployment = ResidiuumDeployment::create(root).unwrap();
        let layout = HeapMetaLayout::new(root);
        let dep = *DeploymentId::new_random().unwrap().as_bytes();
        let heap_bytes = *HeapId::new_random().unwrap().as_bytes();
        let staged =
            stage_heap_genesis(&layout, dep, heap_bytes, uuid(), &format!("heap-{id}")).unwrap();
        publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
        let heap_id = HeapId::from_bytes_unchecked_nonzero(heap_bytes).unwrap();
        let dep_id = DeploymentId::from_bytes_unchecked_nonzero(dep).unwrap();
        let mut client = HeapClient::from(deployment.open_heap(mint_cap_for(heap_id, dep_id)));

        let cols = case["collections"].as_object().expect("collections");
        for (name, docs) in cols {
            let mut col = client.create_collection(name).unwrap().collection;
            for d in docs.as_array().expect("docs") {
                let key = d["key"].as_str().expect("key");
                col.put(key, &d["value"]).unwrap();
            }
        }

        let src = case["source_rql"].as_str().expect("source_rql");
        let page = execute_rql_full(
            &mut client,
            src,
            &Parameters::default(),
            QueryRunOptions::default(),
        )
        .unwrap_or_else(|e| panic!("{id}: execute failed: {e}"));

        assert_eq!(page.profile, RQL_FULL_PROFILE, "{id}");

        if let Some(n) = case.get("expect_row_count").and_then(|x| x.as_u64()) {
            assert_eq!(page.rows.len() as u64, n, "{id}: row_count");
        }
        if let Some(keys) = case.get("expect_keys").and_then(|x| x.as_array()) {
            let got: Vec<&str> = page.rows.iter().map(|(k, _)| k.as_str()).collect();
            let want: Vec<&str> = keys.iter().filter_map(|x| x.as_str()).collect();
            assert_eq!(got, want, "{id}: keys");
        }
        if let Some(rows) = case.get("expect_rows").and_then(|x| x.as_array()) {
            assert_eq!(page.rows.len(), rows.len(), "{id}: expect_rows len");
            for (i, er) in rows.iter().enumerate() {
                assert_eq!(page.rows[i].0, er["key"].as_str().unwrap(), "{id}: key {i}");
                assert_eq!(page.rows[i].1, er["value"], "{id}: value {i}");
            }
        }
        if let Some(paths) = case.get("expect_path_equals").and_then(|x| x.as_array()) {
            let row = &page.rows[0].1;
            for pe in paths {
                let path = pe["path"].as_str().unwrap();
                assert_eq!(json_path(row, path), &pe["value"], "{id}: {path}");
            }
        }
    }
}
