//! HP-000 / HP-001 contract and vector tests.

use residiuum_heap::{
    decide, sig_structure_for, verify_certificate, verify_holder_proof, AuthorizationDecision,
    HeapAdministrativeState, HeapSecuritySnapshot, Operation, OperationDescriptor,
    SecurityRevision, TrustedInstant, EXTERNAL_AAD_CERTIFICATE, GENERATED_OPS,
};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn hex(s: &str) -> Vec<u8> {
    hex::decode(s).unwrap()
}

#[test]
fn registry_unique_and_hp000_active_set() {
    let mut ids = std::collections::BTreeSet::new();
    let mut names = std::collections::BTreeSet::new();
    for op in GENERATED_OPS {
        assert!(ids.insert(op.id), "dup id {}", op.id);
        assert!(names.insert(op.wire_name), "dup name {}", op.wire_name);
    }
    let active: Vec<_> = GENERATED_OPS
        .iter()
        .filter(|o| o.status == "active")
        .map(|o| o.id)
        .collect();
    assert!(active.contains(&1) && active.contains(&2) && active.contains(&3));
    for id in [
        105u16, 110, 111, 112, 114, 115, 116, 117, 120, 121, 122, 130, 131, 132, 133,
    ] {
        assert!(active.contains(&id), "§32.4 data op {id} must be active");
        assert!(Operation::is_callable(id), "data op {id} must be callable");
    }
    // Still-reserved example (export).
    assert!(!Operation::is_callable(140));
}

#[test]
fn artifacts_present_and_schemas_for_active_ops() {
    let root = workspace_root().join("spec/heap");
    assert!(root.join("operations-v1.json").is_file());
    assert!(root.join("cbor-v1.json").is_file());
    assert!(root.join("vectors-v1.json").is_file());
    assert!(root.join("rpc-v1.schema.json").is_file());
    for name in [
        "ping",
        "health_live",
        "health_ready",
        "collection_open",
        "list_collections",
        "get",
        "get_bytes",
        "list_keys",
        "scan_json",
        "find",
        "history",
        "put",
        "put_bytes",
        "delete",
        "index_list",
        "index_create",
        "index_drop",
        "index_rebuild",
    ] {
        assert!(root.join(format!("rpc-v1/{name}.request.json")).is_file());
        assert!(root.join(format!("rpc-v1/{name}.response.json")).is_file());
        assert!(root
            .join(format!("fixtures/{name}.accepted.json"))
            .is_file());
        assert!(root
            .join(format!("fixtures/{name}.rejected.json"))
            .is_file());
    }
    // Drift: build recorded lengths must match current files.
    let ops_len = fs::metadata(root.join("operations-v1.json")).unwrap().len() as usize;
    assert_eq!(ops_len, residiuum_heap::artifacts::OPS_LEN);
}

#[test]
fn bootstrap_vectors_verify_byte_for_byte() {
    let root = workspace_root().join("spec/heap/vectors-v1.json");
    let doc: serde_json::Value = serde_json::from_str(&fs::read_to_string(root).unwrap()).unwrap();
    let inputs = &doc["inputs"];
    let master_pk: [u8; 32] = hex(inputs["master_public_key"].as_str().unwrap())
        .try_into()
        .unwrap();
    let issuer = hex(inputs["issuer_key_hash"].as_str().unwrap());
    assert_eq!(Sha256::digest(master_pk).as_slice(), issuer.as_slice());

    let cert = &doc["accepted"][0];
    let cose = hex(cert["cose_sign1"].as_str().unwrap());
    let protected = hex(cert["protected"].as_str().unwrap());
    let payload = hex(cert["payload"].as_str().unwrap());
    let sig_structure = hex(cert["sig_structure"].as_str().unwrap());
    let recomputed = sig_structure_for(&protected, EXTERNAL_AAD_CERTIFICATE, &payload);
    assert_eq!(recomputed, sig_structure, "sig_structure mismatch");

    let verified = verify_certificate(&cose, &master_pk).expect("cert");
    assert_eq!(
        hex::encode(verified.fingerprint),
        cert["sha256"].as_str().unwrap()
    );

    // Second decoder: ciborium must parse the same COSE array length.
    let cbor_val: ciborium::value::Value =
        ciborium::de::from_reader(cose.as_slice()).expect("ciborium");
    match cbor_val {
        ciborium::value::Value::Array(items) => assert_eq!(items.len(), 4),
        _ => panic!("expected array"),
    }

    let proof = &doc["accepted"][1];
    let proof_cose = hex(proof["cose_sign1"].as_str().unwrap());
    let nonce: [u8; 32] = hex(inputs["server_nonce"].as_str().unwrap())
        .try_into()
        .unwrap();
    let exporter: [u8; 32] = hex(inputs["tls_exporter"].as_str().unwrap())
        .try_into()
        .unwrap();
    verify_holder_proof(&proof_cose, &verified, &nonce, &exporter).expect("proof");

    for rej in doc["rejected"].as_array().unwrap() {
        let bad = hex(rej["cose_sign1"].as_str().unwrap());
        let kind = rej["kind"].as_str().unwrap();
        let err = if kind == "heap_key_certificate" {
            verify_certificate(&bad, &master_pk).err()
        } else {
            verify_holder_proof(&bad, &verified, &nonce, &exporter).err()
        };
        assert!(err.is_some(), "expected rejection for {}", rej["id"]);
    }
}

#[test]
fn decide_allows_ping_and_denies_reserved() {
    let doc: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(workspace_root().join("spec/heap/vectors-v1.json")).unwrap(),
    )
    .unwrap();
    let inputs = &doc["inputs"];
    let master_pk: [u8; 32] = hex(inputs["master_public_key"].as_str().unwrap())
        .try_into()
        .unwrap();
    let cose = hex(doc["accepted"][0]["cose_sign1"].as_str().unwrap());
    let cert = verify_certificate(&cose, &master_pk).unwrap();

    let snap = HeapSecuritySnapshot {
        deployment_id: cert.deployment_id,
        heap_id: cert.heap_id,
        authority_epoch: cert.authority_epoch,
        authority_generation: cert.authority_generation,
        previous_generation: None,
        grace_deadline_unix_s: None,
        master_public_key: master_pk,
        previous_master_public_key: None,
        security_revision: SecurityRevision::new(1).unwrap(),
        authority_chain_head_hash: [1u8; 32],
        administrative_state: HeapAdministrativeState::Active,
        blacklist: vec![],
        policy_rights_ceiling: None,
    };
    let now = TrustedInstant {
        unix_s: cert.not_before + 1,
    };
    assert_eq!(
        decide(
            &snap,
            &cert,
            &OperationDescriptor {
                operation_id: 1,
                request_bytes: 0
            },
            now
        ),
        AuthorizationDecision::Allow
    );
    // §32.4 data get / find / index_list are active and allowed under Read rights.
    assert_eq!(
        decide(
            &snap,
            &cert,
            &OperationDescriptor {
                operation_id: 111,
                request_bytes: 0
            },
            now
        ),
        AuthorizationDecision::Allow
    );
    assert_eq!(
        decide(
            &snap,
            &cert,
            &OperationDescriptor {
                operation_id: 116,
                request_bytes: 0
            },
            now
        ),
        AuthorizationDecision::Allow
    );
    assert_eq!(
        decide(
            &snap,
            &cert,
            &OperationDescriptor {
                operation_id: 117,
                request_bytes: 0
            },
            now
        ),
        AuthorizationDecision::Allow
    );
    assert_eq!(
        decide(
            &snap,
            &cert,
            &OperationDescriptor {
                operation_id: 130,
                request_bytes: 0
            },
            now
        ),
        AuthorizationDecision::Allow
    );
    // index_create requires IndexAdmin; bootstrap cert rights_mask includes bit 3 (13).
    assert_eq!(
        decide(
            &snap,
            &cert,
            &OperationDescriptor {
                operation_id: 131,
                request_bytes: 0
            },
            now
        ),
        AuthorizationDecision::Allow
    );
    assert!(cert.rights.contains(residiuum_heap::Rights::INDEX_ADMIN));
    // Reserved ops still deny (e.g. export = 140).
    assert!(matches!(
        decide(
            &snap,
            &cert,
            &OperationDescriptor {
                operation_id: 140,
                request_bytes: 0
            },
            now
        ),
        AuthorizationDecision::Deny(_)
    ));
}
