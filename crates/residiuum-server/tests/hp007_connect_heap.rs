//! HP-007 residual: `Residiuum::connect_heap` qualified remote session + process ops.

#[path = "../../residiuum-sdk/tests/common/apb1_facade_parity.rs"]
mod apb1_facade_parity;

#[path = "../../residiuum-sdk/tests/common/apb7_query_parity.rs"]
mod apb7_query_parity;

use ed25519_dalek::{Signer, SigningKey};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose, SanType};
use residiuum_format::{encode_deterministic_uint_map, CborValue};
use residiuum_heap::{
    sig_structure_for, verify_certificate, HeapAdministrativeState, HeapSecuritySnapshot, HeapSlot,
    SecurityRevision, AUDIENCE_DATA_V1, CONTENT_TYPE_CERTIFICATE, EXTERNAL_AAD_CERTIFICATE,
    HEAP_PROFILE, PROFILE_VERSION,
};
use residiuum_sdk::{
    explain_rql_full_on_heap, AppQueryBudget, ErrorCode, HeapClient, HeapCredential,
    InMemoryHolderKey, Parameters, QueryRunOptions, RemoteHeapOptions, Residiuum, TlsClientOptions,
    TlsServerOptions, RQL_FULL_PROFILE,
};
use residiuum_server::{
    serve_store_with, HeapAuthAuditLog, ResidentHeap, ResidentHeapRegistry, ServeOptions,
};
use residiuum_store::Store;
use sha2::{Digest, Sha256};
use std::fs;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

/// READ | WRITE | INDEX_ADMIN | HEAP_ADMIN — full G6 remote pack including create.
const RIGHTS_FULL_G6: u64 = 0x1 | 0x4 | 0x8 | 0x1000;

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

fn vectors() -> serde_json::Value {
    let root = workspace_root().join("spec/heap/vectors-v1.json");
    serde_json::from_str(&fs::read_to_string(root).unwrap()).unwrap()
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn wait_for_server(bind: &str) {
    for _ in 0..100 {
        if TcpStream::connect(bind).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("server did not accept on {bind}");
}

fn write_text(out: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    if b.len() < 24 {
        out.push(0x60 | (b.len() as u8));
    } else {
        out.push(0x78);
        out.push(b.len() as u8);
    }
    out.extend_from_slice(b);
}

fn write_bstr(out: &mut Vec<u8>, b: &[u8]) {
    if b.len() < 24 {
        out.push(0x40 | (b.len() as u8));
    } else if b.len() < 256 {
        out.push(0x58);
        out.push(b.len() as u8);
    } else {
        out.push(0x59);
        out.extend_from_slice(&(b.len() as u16).to_be_bytes());
    }
    out.extend_from_slice(b);
}

fn hex16(s: &str) -> [u8; 16] {
    let b = hex::decode(s).expect("hex16");
    b.try_into().expect("16")
}

fn hex32(s: &str) -> [u8; 32] {
    let b = hex::decode(s).expect("hex32");
    b.try_into().expect("32")
}

/// Mint a bootstrap COSE cert from vector seeds with an expanded rights mask.
///
/// Does **not** rewrite `vectors-v1.json` — test-local only so product vectors
/// stay at Read|Write|IndexAdmin (13) unless deliberately regenerated.
fn mint_vector_cose_with_rights(
    rights_mask: u64,
) -> (Vec<u8>, residiuum_heap::VerifiedCertificate) {
    let doc = vectors();
    let inputs = &doc["inputs"];
    let master_seed = hex32(inputs["master_seed"].as_str().unwrap());
    let holder_seed = hex32(inputs["holder_seed"].as_str().unwrap());
    let master = SigningKey::from_bytes(&master_seed);
    let holder = SigningKey::from_bytes(&holder_seed);
    let master_pk = master.verifying_key().to_bytes();
    let holder_pk = holder.verifying_key().to_bytes();
    assert_eq!(
        hex::encode(master_pk),
        inputs["master_public_key"].as_str().unwrap()
    );
    assert_eq!(
        hex::encode(holder_pk),
        inputs["holder_public_key"].as_str().unwrap()
    );

    let deployment = hex16(inputs["deployment_id"].as_str().unwrap());
    let heap = hex16(inputs["heap_id"].as_str().unwrap());
    let cert_id = hex16(inputs["certificate_id"].as_str().unwrap());
    let issuer = Sha256::digest(master_pk);
    let not_before = inputs["not_before"].as_u64().unwrap();
    let expires_at = inputs["expires_at"].as_u64().unwrap();

    let mut protected = Vec::new();
    protected.push(0xa2);
    protected.push(0x01);
    protected.push(0x27);
    protected.push(0x03);
    write_text(&mut protected, CONTENT_TYPE_CERTIFICATE);

    let payload = encode_deterministic_uint_map(&[
        (1u64, CborValue::Uint(PROFILE_VERSION)),
        (2, CborValue::Bytes(deployment.to_vec())),
        (3, CborValue::Bytes(heap.to_vec())),
        (4, CborValue::Uint(1)),
        (5, CborValue::Uint(1)),
        (6, CborValue::Bytes(cert_id.to_vec())),
        (7, CborValue::Bytes(holder_pk.to_vec())),
        (8, CborValue::Uint(rights_mask)),
        (9, CborValue::Array(vec![])),
        (10, CborValue::Uint(not_before)),
        (11, CborValue::Uint(expires_at)),
        (12, CborValue::Text(AUDIENCE_DATA_V1.into())),
        (13, CborValue::Bytes(issuer.to_vec())),
    ])
    .expect("payload");

    let sig_structure = sig_structure_for(&protected, EXTERNAL_AAD_CERTIFICATE, &payload);
    let signature = master.sign(&sig_structure).to_bytes();

    let mut cose = Vec::new();
    cose.push(0x84);
    write_bstr(&mut cose, &protected);
    cose.push(0xa0);
    write_bstr(&mut cose, &payload);
    write_bstr(&mut cose, &signature);

    let verified = verify_certificate(&cose, &master_pk).expect("verify mint");
    assert_eq!(verified.rights.bits(), rights_mask);
    (cose, verified)
}

fn issue_localhost_tls(dir: &std::path::Path) -> (PathBuf, PathBuf, PathBuf) {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "residiuum-heap-test-ca");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_path = dir.join("ca.pem");
    fs::write(&ca_path, ca_cert.pem()).unwrap();

    let srv_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    params.subject_alt_names = vec![SanType::DnsName("localhost".try_into().unwrap())];
    let srv_cert = params.signed_by(&srv_key, &ca_cert, &ca_key).unwrap();
    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");
    fs::write(&cert_path, srv_cert.pem()).unwrap();
    fs::write(&key_path, srv_key.serialize_pem()).unwrap();
    (ca_path, cert_path, key_path)
}

#[test]
fn connect_heap_welcome_and_process_ops() {
    let doc = vectors();
    let inputs = &doc["inputs"];
    let master_pk: [u8; 32] = hex(inputs["master_public_key"].as_str().unwrap())
        .try_into()
        .unwrap();
    let holder_seed: [u8; 32] = hex(inputs["holder_seed"].as_str().unwrap())
        .try_into()
        .unwrap();
    let cose = hex(doc["accepted"][0]["cose_sign1"].as_str().unwrap());
    let verified = verify_certificate(&cose, &master_pk).unwrap();
    let now_unix = verified.not_before + 10;

    let snap = HeapSecuritySnapshot {
        deployment_id: verified.deployment_id,
        heap_id: verified.heap_id,
        authority_epoch: verified.authority_epoch,
        authority_generation: verified.authority_generation,
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
    let registry = Arc::new(ResidentHeapRegistry::new());
    registry.insert(
        verified.heap_id,
        ResidentHeap {
            slot: Arc::new(HeapSlot::new(snap)),
            display_name: Some("accounts".into()),
        },
    );
    let audit = Arc::new(HeapAuthAuditLog::default());

    let tmp = TempDir::new().unwrap();
    let store_path = tmp.path().join("app.residiuum");
    Store::open(&store_path).unwrap();
    let (ca_path, cert_path, key_path) = issue_localhost_tls(tmp.path());

    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&shutdown);
    let opts = ServeOptions::new()
        .legacy_token_server()
        .tls(TlsServerOptions::new(&cert_path, &key_path))
        .qualified_heap_key(true)
        .heap_registry(Arc::clone(&registry))
        .deployment_id(verified.deployment_id.to_string())
        .heap_auth_audit(Arc::clone(&audit))
        .security_now_unix_s(now_unix)
        .shutdown_flag(Arc::clone(&shutdown));
    let store_c = store_path.clone();
    let bind_c = bind.clone();
    thread::spawn(move || {
        let _ = serve_store_with(store_c, &bind_c, opts);
    });
    wait_for_server(&bind);

    let holder = Arc::new(InMemoryHolderKey::from_seed(holder_seed));
    let credential = HeapCredential::new(&cose, holder).expect("credential");
    assert_eq!(credential.heap_id(), verified.heap_id);

    let options = RemoteHeapOptions::new(
        TlsClientOptions::new("localhost").ca_path(ca_path),
        credential,
    )
    .expected_heap_name("accounts")
    .now_unix_s(now_unix);

    let url = format!("residiuum://127.0.0.1:{port}/accounts");
    let mut heap = Residiuum::connect_heap(&url, options).expect("connect_heap");
    assert_eq!(heap.id(), verified.heap_id);
    assert_eq!(heap.welcome().msg, "welcome");
    assert_eq!(heap.heap_profile(), HEAP_PROFILE);

    heap.ping().expect("ping");
    assert!(heap.live().expect("live"));
    assert!(heap.ready().expect("ready"));

    drop(heap);
    flag.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(50));

    assert!(
        audit
            .snapshot()
            .iter()
            .any(|e| matches!(e, residiuum_server::HeapAuthAuditEvent::Welcome { .. })),
        "audit must record welcome"
    );
}

#[test]
fn connect_heap_wrong_name_rejects() {
    let doc = vectors();
    let inputs = &doc["inputs"];
    let master_pk: [u8; 32] = hex(inputs["master_public_key"].as_str().unwrap())
        .try_into()
        .unwrap();
    let holder_seed: [u8; 32] = hex(inputs["holder_seed"].as_str().unwrap())
        .try_into()
        .unwrap();
    let cose = hex(doc["accepted"][0]["cose_sign1"].as_str().unwrap());
    let verified = verify_certificate(&cose, &master_pk).unwrap();
    let now_unix = verified.not_before + 10;

    let snap = HeapSecuritySnapshot {
        deployment_id: verified.deployment_id,
        heap_id: verified.heap_id,
        authority_epoch: verified.authority_epoch,
        authority_generation: verified.authority_generation,
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
    let registry = Arc::new(ResidentHeapRegistry::new());
    registry.insert(
        verified.heap_id,
        ResidentHeap {
            slot: Arc::new(HeapSlot::new(snap)),
            display_name: Some("accounts".into()),
        },
    );

    let tmp = TempDir::new().unwrap();
    let store_path = tmp.path().join("app.residiuum");
    Store::open(&store_path).unwrap();
    let (ca_path, cert_path, key_path) = issue_localhost_tls(tmp.path());

    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&shutdown);
    let opts = ServeOptions::new()
        .legacy_token_server()
        .tls(TlsServerOptions::new(&cert_path, &key_path))
        .qualified_heap_key(true)
        .heap_registry(Arc::clone(&registry))
        .deployment_id(verified.deployment_id.to_string())
        .security_now_unix_s(now_unix)
        .shutdown_flag(Arc::clone(&shutdown));
    let store_c = store_path.clone();
    let bind_c = bind.clone();
    thread::spawn(move || {
        let _ = serve_store_with(store_c, &bind_c, opts);
    });
    wait_for_server(&bind);

    let holder = Arc::new(InMemoryHolderKey::from_seed(holder_seed));
    let credential = HeapCredential::new(&cose, holder).unwrap();
    let options = RemoteHeapOptions::new(
        TlsClientOptions::new("localhost").ca_path(ca_path),
        credential,
    )
    .expected_heap_name("wrong-name")
    .now_unix_s(now_unix)
    .max_connect_attempts(std::num::NonZeroU32::new(1).unwrap());

    let url = format!("residiuum://127.0.0.1:{port}/wrong-name");
    let err = match Residiuum::connect_heap(&url, options) {
        Ok(_) => panic!("name mismatch should reject"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("heap_unavailable") || msg.contains("unavailable"),
        "got {msg}"
    );

    flag.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(50));
}

#[test]
fn connect_heap_put_get_delete_subject_v2() {
    use residiuum_store::{
        create_object, publish_staged_genesis, stage_heap_genesis, HeapMetaLayout, ObjectKind,
    };

    let doc = vectors();
    let inputs = &doc["inputs"];
    let master_pk: [u8; 32] = hex(inputs["master_public_key"].as_str().unwrap())
        .try_into()
        .unwrap();
    let holder_seed: [u8; 32] = hex(inputs["holder_seed"].as_str().unwrap())
        .try_into()
        .unwrap();
    let cose = hex(doc["accepted"][0]["cose_sign1"].as_str().unwrap());
    let verified = verify_certificate(&cose, &master_pk).unwrap();
    let now_unix = verified.not_before + 10;

    let snap = HeapSecuritySnapshot {
        deployment_id: verified.deployment_id,
        heap_id: verified.heap_id,
        authority_epoch: verified.authority_epoch,
        authority_generation: verified.authority_generation,
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
    let registry = Arc::new(ResidentHeapRegistry::new());
    registry.insert(
        verified.heap_id,
        ResidentHeap {
            slot: Arc::new(HeapSlot::new(snap)),
            display_name: Some("accounts".into()),
        },
    );

    let tmp = TempDir::new().unwrap();
    let store_path = tmp.path().join("app.residiuum");
    Store::open(&store_path).unwrap();
    // Heap meta + collection on the same store root the server opens.
    let layout = HeapMetaLayout::new(&store_path);
    let dep = *verified.deployment_id.as_bytes();
    let heap = *verified.heap_id.as_bytes();
    let coll = *residiuum_heap::CollectionId::new_random()
        .unwrap()
        .as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap, [9u8; 16], "accounts").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    create_object(
        &layout,
        &heap,
        ObjectKind::Collection,
        coll,
        [8u8; 16],
        "users",
    )
    .unwrap();

    let (ca_path, cert_path, key_path) = issue_localhost_tls(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&shutdown);
    let opts = ServeOptions::new()
        .legacy_token_server()
        .tls(TlsServerOptions::new(&cert_path, &key_path))
        .qualified_heap_key(true)
        .heap_registry(Arc::clone(&registry))
        .deployment_id(verified.deployment_id.to_string())
        .security_now_unix_s(now_unix)
        .shutdown_flag(Arc::clone(&shutdown));
    let store_c = store_path.clone();
    let bind_c = bind.clone();
    thread::spawn(move || {
        let _ = serve_store_with(store_c, &bind_c, opts);
    });
    wait_for_server(&bind);

    let holder = Arc::new(InMemoryHolderKey::from_seed(holder_seed));
    let credential = HeapCredential::new(&cose, holder).unwrap();
    let options = RemoteHeapOptions::new(
        TlsClientOptions::new("localhost").ca_path(ca_path),
        credential,
    )
    .expected_heap_name("accounts")
    .now_unix_s(now_unix);

    let mut heap =
        Residiuum::connect_heap(format!("residiuum://127.0.0.1:{port}/accounts"), options)
            .expect("connect_heap");
    let cid = heap.collection_open("users").expect("collection_open");
    heap.put_json(&cid, "user-1", &serde_json::json!({"name": "Alice"}))
        .expect("put");
    let got = heap.get_json(&cid, "user-1").expect("get").expect("found");
    assert_eq!(got["name"], "Alice");
    heap.put_bytes(&cid, "blob-1", b"\x00\xff")
        .expect("put_bytes");
    assert_eq!(
        heap.get_bytes(&cid, "blob-1").expect("get_bytes").unwrap(),
        b"\x00\xff"
    );
    assert!(heap.delete(&cid, "user-1").expect("delete"));
    assert!(heap
        .get_json(&cid, "user-1")
        .expect("get after delete")
        .is_none());

    drop(heap);
    flag.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(50));
}

/// APB-1 G1b/G6: remote `HeapClient` façade runs the shared collection-plane parity pack.
///
/// Vector cert is Read|Write|IndexAdmin (no HeapAdmin) — create is exercised only
/// on the embedded suite; remote uses pre-provisioned `users` + list/open.
#[test]
fn apb1_heap_client_from_remote_open_put_get_delete() {
    use residiuum_store::{
        create_object, publish_staged_genesis, stage_heap_genesis, HeapMetaLayout, ObjectKind,
    };

    let doc = vectors();
    let inputs = &doc["inputs"];
    let master_pk: [u8; 32] = hex(inputs["master_public_key"].as_str().unwrap())
        .try_into()
        .unwrap();
    let holder_seed: [u8; 32] = hex(inputs["holder_seed"].as_str().unwrap())
        .try_into()
        .unwrap();
    let cose = hex(doc["accepted"][0]["cose_sign1"].as_str().unwrap());
    let verified = verify_certificate(&cose, &master_pk).unwrap();
    let now_unix = verified.not_before + 10;

    let snap = HeapSecuritySnapshot {
        deployment_id: verified.deployment_id,
        heap_id: verified.heap_id,
        authority_epoch: verified.authority_epoch,
        authority_generation: verified.authority_generation,
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
    let registry = Arc::new(ResidentHeapRegistry::new());
    registry.insert(
        verified.heap_id,
        ResidentHeap {
            slot: Arc::new(HeapSlot::new(snap)),
            display_name: Some("accounts".into()),
        },
    );

    let tmp = TempDir::new().unwrap();
    let store_path = tmp.path().join("app.residiuum");
    Store::open(&store_path).unwrap();
    let layout = HeapMetaLayout::new(&store_path);
    let dep = *verified.deployment_id.as_bytes();
    let heap_bytes = *verified.heap_id.as_bytes();
    let coll = *residiuum_heap::CollectionId::new_random()
        .unwrap()
        .as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, [9u8; 16], "accounts").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    create_object(
        &layout,
        &heap_bytes,
        ObjectKind::Collection,
        coll,
        [8u8; 16],
        "users",
    )
    .unwrap();

    let (ca_path, cert_path, key_path) = issue_localhost_tls(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&shutdown);
    let opts = ServeOptions::new()
        .legacy_token_server()
        .tls(TlsServerOptions::new(&cert_path, &key_path))
        .qualified_heap_key(true)
        .heap_registry(Arc::clone(&registry))
        .deployment_id(verified.deployment_id.to_string())
        .security_now_unix_s(now_unix)
        .shutdown_flag(Arc::clone(&shutdown));
    let store_c = store_path.clone();
    let bind_c = bind.clone();
    thread::spawn(move || {
        let _ = serve_store_with(store_c, &bind_c, opts);
    });
    wait_for_server(&bind);

    let holder = Arc::new(InMemoryHolderKey::from_seed(holder_seed));
    let credential = HeapCredential::new(&cose, holder).unwrap();
    let options = RemoteHeapOptions::new(
        TlsClientOptions::new("localhost").ca_path(ca_path),
        credential,
    )
    .expected_heap_name("accounts")
    .now_unix_s(now_unix);

    let remote = Residiuum::connect_heap(format!("residiuum://127.0.0.1:{port}/accounts"), options)
        .expect("connect_heap");

    let mut client = HeapClient::from(remote);
    assert!(client.is_bound());
    assert_eq!(client.id(), verified.heap_id);

    // G6: same collection-plane scenarios as embedded `facade_parity_pack_embedded`.
    assert_eq!(
        apb1_facade_parity::SCENARIO_IDS,
        &[
            "create_open_list",
            "put_get_delete",
            "history_versions",
            "index_lifecycle",
            "apb2_mutations",
        ]
    );
    let mut col = apb1_facade_parity::scenario_list_and_open(&mut client, "users");
    assert_eq!(col.heap_id(), verified.heap_id);
    apb1_facade_parity::run_collection_plane_parity(&mut col);

    drop(client);
    flag.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(50));
}

/// APB-1 G6: full remote façade parity including create (test-local HeapAdmin COSE).
///
/// Vector product cert stays at rights 13; this test mints Read|Write|IndexAdmin|HeapAdmin
/// from the same seeds without rewriting `vectors-v1.json`.
#[test]
fn apb1_heap_client_from_remote_full_parity_heap_admin() {
    use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};

    let doc = vectors();
    let inputs = &doc["inputs"];
    let holder_seed: [u8; 32] = hex(inputs["holder_seed"].as_str().unwrap())
        .try_into()
        .unwrap();
    let master_pk: [u8; 32] = hex(inputs["master_public_key"].as_str().unwrap())
        .try_into()
        .unwrap();

    let (cose, verified) = mint_vector_cose_with_rights(RIGHTS_FULL_G6);
    assert!(
        verified.rights.bits() & 0x1000 != 0,
        "HeapAdmin bit required for create"
    );
    let now_unix = verified.not_before + 10;

    let snap = HeapSecuritySnapshot {
        deployment_id: verified.deployment_id,
        heap_id: verified.heap_id,
        authority_epoch: verified.authority_epoch,
        authority_generation: verified.authority_generation,
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
    let registry = Arc::new(ResidentHeapRegistry::new());
    registry.insert(
        verified.heap_id,
        ResidentHeap {
            slot: Arc::new(HeapSlot::new(snap)),
            display_name: Some("accounts".into()),
        },
    );

    let tmp = TempDir::new().unwrap();
    let store_path = tmp.path().join("app.residiuum");
    Store::open(&store_path).unwrap();
    // Genesis only — no pre-created collections so create_open_list is real.
    let layout = HeapMetaLayout::new(&store_path);
    let dep = *verified.deployment_id.as_bytes();
    let heap_bytes = *verified.heap_id.as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, [9u8; 16], "accounts").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();

    let (ca_path, cert_path, key_path) = issue_localhost_tls(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&shutdown);
    let opts = ServeOptions::new()
        .legacy_token_server()
        .tls(TlsServerOptions::new(&cert_path, &key_path))
        .qualified_heap_key(true)
        .heap_registry(Arc::clone(&registry))
        .deployment_id(verified.deployment_id.to_string())
        .security_now_unix_s(now_unix)
        .shutdown_flag(Arc::clone(&shutdown));
    let store_c = store_path.clone();
    let bind_c = bind.clone();
    thread::spawn(move || {
        let _ = serve_store_with(store_c, &bind_c, opts);
    });
    wait_for_server(&bind);

    let holder = Arc::new(InMemoryHolderKey::from_seed(holder_seed));
    let credential = HeapCredential::new(&cose, holder).expect("admin credential");
    let options = RemoteHeapOptions::new(
        TlsClientOptions::new("localhost").ca_path(ca_path),
        credential,
    )
    .expected_heap_name("accounts")
    .now_unix_s(now_unix);

    let remote = Residiuum::connect_heap(format!("residiuum://127.0.0.1:{port}/accounts"), options)
        .expect("connect_heap admin");

    let mut client = HeapClient::from(remote);
    assert!(client.is_bound());
    assert_eq!(client.id(), verified.heap_id);

    // Full G6 pack — same as embedded `run_full_facade_parity`.
    apb1_facade_parity::run_full_facade_parity(&mut client, "orders");

    // Duplicate create fail-closed (semantic parity with embedded).
    match client.create_collection("orders") {
        Ok(_) => panic!("duplicate name must fail on remote façade"),
        Err(_) => {}
    }

    drop(client);
    flag.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(50));
}

/// APB-7 T7: remote collection-plane Application Core query dual pack.
///
/// Same shared scenarios as embedded `apb7_query_dual_pack`. Remote façade now
/// uses product wire op **118** `rql_query` for Application Core (APP-7 T6).
/// No package accept.
#[test]
fn apb7_query_from_remote_collection_plane() {
    use residiuum_store::{publish_staged_genesis, stage_heap_genesis, HeapMetaLayout};

    let doc = vectors();
    let inputs = &doc["inputs"];
    let holder_seed: [u8; 32] = hex(inputs["holder_seed"].as_str().unwrap())
        .try_into()
        .unwrap();
    let master_pk: [u8; 32] = hex(inputs["master_public_key"].as_str().unwrap())
        .try_into()
        .unwrap();

    let (cose, verified) = mint_vector_cose_with_rights(RIGHTS_FULL_G6);
    assert!(
        verified.rights.bits() & 0x1000 != 0,
        "HeapAdmin bit required for create"
    );
    let now_unix = verified.not_before + 10;

    let snap = HeapSecuritySnapshot {
        deployment_id: verified.deployment_id,
        heap_id: verified.heap_id,
        authority_epoch: verified.authority_epoch,
        authority_generation: verified.authority_generation,
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
    let registry = Arc::new(ResidentHeapRegistry::new());
    registry.insert(
        verified.heap_id,
        ResidentHeap {
            slot: Arc::new(HeapSlot::new(snap)),
            display_name: Some("accounts".into()),
        },
    );

    let tmp = TempDir::new().unwrap();
    let store_path = tmp.path().join("app.residiuum");
    Store::open(&store_path).unwrap();
    let layout = HeapMetaLayout::new(&store_path);
    let dep = *verified.deployment_id.as_bytes();
    let heap_bytes = *verified.heap_id.as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_bytes, [9u8; 16], "accounts").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();

    let (ca_path, cert_path, key_path) = issue_localhost_tls(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&shutdown);
    let opts = ServeOptions::new()
        .legacy_token_server()
        .tls(TlsServerOptions::new(&cert_path, &key_path))
        .qualified_heap_key(true)
        .heap_registry(Arc::clone(&registry))
        .deployment_id(verified.deployment_id.to_string())
        .security_now_unix_s(now_unix)
        .shutdown_flag(Arc::clone(&shutdown));
    let store_c = store_path.clone();
    let bind_c = bind.clone();
    thread::spawn(move || {
        let _ = serve_store_with(store_c, &bind_c, opts);
    });
    wait_for_server(&bind);

    let holder = Arc::new(InMemoryHolderKey::from_seed(holder_seed));
    let credential = HeapCredential::new(&cose, holder).expect("admin credential");
    let options = RemoteHeapOptions::new(
        TlsClientOptions::new("localhost").ca_path(ca_path),
        credential,
    )
    .expected_heap_name("accounts")
    .now_unix_s(now_unix);

    let remote = Residiuum::connect_heap(format!("residiuum://127.0.0.1:{port}/accounts"), options)
        .expect("connect_heap admin");

    let mut client = HeapClient::from(remote);
    assert!(client.is_bound());
    assert_eq!(
        apb7_query_parity::SCENARIO_IDS,
        &[
            "builder_rql_plan_hash",
            "equality_filter_keys",
            "multipage_key_order_oracle",
            "multipage_field_order_oracle",
            "explain_rql_surface",
            "scan_json_page",
            "budget_fail_closed",
            "index_equality_pushdown",
        ]
    );
    // APP-7 T6: remote rql uses op 118 wire (not collection-plane scan only).
    apb7_query_parity::run_full_query_parity(&mut client, "orders_t7");

    // Full profile uses one Heap-bound op 118 request: the server, not the
    // client, performs the foreign collection scan and QVM attachment.
    let mut orders = client
        .create_collection("full_wire_orders")
        .expect("create Full wire orders")
        .collection;
    let mut customers = client
        .create_collection("full_wire_customers")
        .expect("create Full wire customers")
        .collection;
    customers
        .put("c1", &serde_json::json!({"id": "c1", "name": "Ada"}))
        .unwrap();
    customers
        .put("c2", &serde_json::json!({"id": "c2", "name": "Bob"}))
        .unwrap();
    orders
        .put(
            "o1",
            &serde_json::json!({"customer_id": "c1", "amount": 50}),
        )
        .unwrap();
    orders
        .put(
            "o2",
            &serde_json::json!({"customer_id": "c2", "amount": 150}),
        )
        .unwrap();

    let source = r#"from full_wire_orders
        enrich customer using full_wire_customers
          matching customer_id = id expect exactly_one
        project { customer { name }, amount }
        page size 1"#;
    let first = client
        .rql_full(source, &Parameters::default(), QueryRunOptions::default())
        .expect("remote Full page one");
    assert_eq!(first.profile, RQL_FULL_PROFILE);
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.base.rows.len(), 1);
    assert_eq!(first.enrich_loads.len(), 1);
    assert!(first.rows[0].1["customer"]["name"].is_string());
    assert!(first.rows[0].1.get("customer_id").is_none());
    assert!(!first.base.exhausted);
    let explained = explain_rql_full_on_heap(&mut client, source).expect("Full explain");
    assert_eq!(
        explained.plan_hash, first.base.plan_hash,
        "Full explain must identify the QVM programme actually executed"
    );

    let mut next_options = QueryRunOptions::default();
    next_options.after = first.base.next.clone();
    let second = client
        .rql_full(source, &Parameters::default(), next_options)
        .expect("remote Full page two");
    assert_eq!(second.rows.len(), 1);
    assert_ne!(first.rows[0].0, second.rows[0].0);
    assert!(second.base.exhausted);

    let computed = client
        .rql_full(
            "from full_wire_orders project _key, amount_band = if amount < 100 then \"low\" else \"high\"",
            &Parameters::default(),
            QueryRunOptions::default(),
        )
        .expect("remote Full computed project");
    let computed_by_key: std::collections::BTreeMap<_, _> = computed.rows.into_iter().collect();
    assert_eq!(computed_by_key["o1"]["amount_band"], "low");
    assert_eq!(computed_by_key["o2"]["amount_band"], "high");

    let mut lines = client
        .create_collection("full_wire_lines")
        .expect("create Full wire lines")
        .collection;
    lines
        .put("l1", &serde_json::json!({"order_id": "o1", "qty": 2}))
        .unwrap();
    lines
        .put("l2", &serde_json::json!({"order_id": "o1", "qty": 0}))
        .unwrap();
    lines
        .put("l3", &serde_json::json!({"order_id": "o2", "qty": 3}))
        .unwrap();
    let within = client
        .rql_full(
            r#"from full_wire_orders
                enrich items using full_wire_lines matching _key = order_id expect many
                within items { where qty > 0 }
                project { items { qty } }"#,
            &Parameters::default(),
            QueryRunOptions::default(),
        )
        .expect("remote Full within");
    let within_by_key: std::collections::BTreeMap<_, _> = within.rows.into_iter().collect();
    assert_eq!(within_by_key["o1"]["items"].as_array().unwrap().len(), 1);
    assert_eq!(within_by_key["o1"]["items"][0]["qty"], 2);
    assert_eq!(within_by_key["o2"]["items"][0]["qty"], 3);

    let isolation = client.rql_full(
        "from collection_not_in_authorised_heap project _key",
        &Parameters::default(),
        QueryRunOptions::default(),
    );
    assert!(
        isolation.is_err(),
        "Full binding must stay inside the Heap catalogue"
    );

    customers
        .put(
            "c1",
            &serde_json::json!({"id": "c1", "name": "x".repeat(16 * 1024)}),
        )
        .unwrap();
    let mut bounded = QueryRunOptions::default();
    bounded.budget = Some(AppQueryBudget {
        max_documents: None,
        max_bytes: None,
        max_result_bytes: Some(2 * 1024),
    });
    let bounded_err = client
        .rql_full(source, &Parameters::default(), bounded)
        .expect_err("remote Full attachment must enforce result budget");
    assert_eq!(bounded_err.code(), ErrorCode::QueryBudgetRequired);

    let mut bad_orders = client
        .create_collection("full_wire_bad_orders")
        .expect("create Full wire refusal collection")
        .collection;
    bad_orders
        .put("bad", &serde_json::json!({"customer_id": "missing"}))
        .unwrap();
    let refusal = client.rql_full(
        "from full_wire_bad_orders enrich customer using full_wire_customers matching customer_id = id expect exactly_one",
        &Parameters::default(),
        QueryRunOptions::default(),
    );
    assert!(refusal.is_err(), "remote Full cardinality must fail closed");

    drop(client);
    flag.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(50));
}

#[test]
fn connect_heap_list_and_scan_json() {
    use residiuum_store::{
        create_object, publish_staged_genesis, stage_heap_genesis, HeapMetaLayout, ObjectKind,
    };

    let doc = vectors();
    let inputs = &doc["inputs"];
    let master_pk: [u8; 32] = hex(inputs["master_public_key"].as_str().unwrap())
        .try_into()
        .unwrap();
    let holder_seed: [u8; 32] = hex(inputs["holder_seed"].as_str().unwrap())
        .try_into()
        .unwrap();
    let cose = hex(doc["accepted"][0]["cose_sign1"].as_str().unwrap());
    let verified = verify_certificate(&cose, &master_pk).unwrap();
    let now_unix = verified.not_before + 10;

    let snap = HeapSecuritySnapshot {
        deployment_id: verified.deployment_id,
        heap_id: verified.heap_id,
        authority_epoch: verified.authority_epoch,
        authority_generation: verified.authority_generation,
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
    let registry = Arc::new(ResidentHeapRegistry::new());
    registry.insert(
        verified.heap_id,
        ResidentHeap {
            slot: Arc::new(HeapSlot::new(snap)),
            display_name: Some("accounts".into()),
        },
    );

    let tmp = TempDir::new().unwrap();
    let store_path = tmp.path().join("app.residiuum");
    Store::open(&store_path).unwrap();
    let layout = HeapMetaLayout::new(&store_path);
    let dep = *verified.deployment_id.as_bytes();
    let heap_b = *verified.heap_id.as_bytes();
    let coll = *residiuum_heap::CollectionId::new_random()
        .unwrap()
        .as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_b, [9u8; 16], "accounts").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    create_object(
        &layout,
        &heap_b,
        ObjectKind::Collection,
        coll,
        [8u8; 16],
        "users",
    )
    .unwrap();

    let (ca_path, cert_path, key_path) = issue_localhost_tls(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&shutdown);
    let opts = ServeOptions::new()
        .legacy_token_server()
        .tls(TlsServerOptions::new(&cert_path, &key_path))
        .qualified_heap_key(true)
        .heap_registry(Arc::clone(&registry))
        .deployment_id(verified.deployment_id.to_string())
        .security_now_unix_s(now_unix)
        .shutdown_flag(Arc::clone(&shutdown));
    let store_c = store_path.clone();
    let bind_c = bind.clone();
    thread::spawn(move || {
        let _ = serve_store_with(store_c, &bind_c, opts);
    });
    wait_for_server(&bind);

    let holder = Arc::new(InMemoryHolderKey::from_seed(holder_seed));
    let credential = HeapCredential::new(&cose, holder).unwrap();
    let options = RemoteHeapOptions::new(
        TlsClientOptions::new("localhost").ca_path(ca_path),
        credential,
    )
    .expected_heap_name("accounts")
    .now_unix_s(now_unix);

    let mut remote =
        Residiuum::connect_heap(format!("residiuum://127.0.0.1:{port}/accounts"), options)
            .expect("connect_heap");
    let listed = remote.list_collections().expect("list_collections");
    assert!(
        listed.iter().any(|(_, n)| n == "users"),
        "users missing from {listed:?}"
    );
    let cid = remote.collection_open("users").expect("open");
    remote
        .put_json(&cid, "a", &serde_json::json!({"n": 1}))
        .unwrap();
    remote
        .put_json(&cid, "b", &serde_json::json!({"n": 2}))
        .unwrap();
    remote
        .put_json(&cid, "c", &serde_json::json!({"n": 3}))
        .unwrap();

    let keys = remote.list_keys(&cid, Some(10), None).expect("list_keys");
    assert_eq!(keys, vec!["a", "b", "c"]);
    let page = remote.list_keys(&cid, Some(1), Some("a")).expect("page");
    assert_eq!(page, vec!["b"]);

    let page = remote.scan_json(&cid, Some(10), None).expect("scan");
    assert_eq!(page.rows.len(), 3);
    assert_eq!(page.rows[0].0, "a");
    assert_eq!(page.rows[0].1["n"], 1);
    assert_eq!(page.rows[2].1["n"], 3);
    assert!(!page.has_more);
    assert!(page.exhausted);
    assert!(page.next_after_key.is_none());
    assert!(page.coverage_complete);
    assert!(page.incomplete.is_empty());

    drop(remote);
    flag.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(50));
}

#[test]
fn connect_heap_history() {
    use residiuum_store::{
        create_object, publish_staged_genesis, stage_heap_genesis, HeapMetaLayout, ObjectKind,
    };

    let doc = vectors();
    let inputs = &doc["inputs"];
    let master_pk: [u8; 32] = hex(inputs["master_public_key"].as_str().unwrap())
        .try_into()
        .unwrap();
    let holder_seed: [u8; 32] = hex(inputs["holder_seed"].as_str().unwrap())
        .try_into()
        .unwrap();
    let cose = hex(doc["accepted"][0]["cose_sign1"].as_str().unwrap());
    let verified = verify_certificate(&cose, &master_pk).unwrap();
    let now_unix = verified.not_before + 10;

    let snap = HeapSecuritySnapshot {
        deployment_id: verified.deployment_id,
        heap_id: verified.heap_id,
        authority_epoch: verified.authority_epoch,
        authority_generation: verified.authority_generation,
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
    let registry = Arc::new(ResidentHeapRegistry::new());
    registry.insert(
        verified.heap_id,
        ResidentHeap {
            slot: Arc::new(HeapSlot::new(snap)),
            display_name: Some("accounts".into()),
        },
    );

    let tmp = TempDir::new().unwrap();
    let store_path = tmp.path().join("app.residiuum");
    Store::open(&store_path).unwrap();
    let layout = HeapMetaLayout::new(&store_path);
    let dep = *verified.deployment_id.as_bytes();
    let heap_b = *verified.heap_id.as_bytes();
    let coll = *residiuum_heap::CollectionId::new_random()
        .unwrap()
        .as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_b, [9u8; 16], "accounts").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    create_object(
        &layout,
        &heap_b,
        ObjectKind::Collection,
        coll,
        [8u8; 16],
        "users",
    )
    .unwrap();

    let (ca_path, cert_path, key_path) = issue_localhost_tls(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&shutdown);
    let opts = ServeOptions::new()
        .legacy_token_server()
        .tls(TlsServerOptions::new(&cert_path, &key_path))
        .qualified_heap_key(true)
        .heap_registry(Arc::clone(&registry))
        .deployment_id(verified.deployment_id.to_string())
        .security_now_unix_s(now_unix)
        .shutdown_flag(Arc::clone(&shutdown));
    let store_c = store_path.clone();
    let bind_c = bind.clone();
    thread::spawn(move || {
        let _ = serve_store_with(store_c, &bind_c, opts);
    });
    wait_for_server(&bind);

    let holder = Arc::new(InMemoryHolderKey::from_seed(holder_seed));
    let credential = HeapCredential::new(&cose, holder).unwrap();
    let options = RemoteHeapOptions::new(
        TlsClientOptions::new("localhost").ca_path(ca_path),
        credential,
    )
    .expected_heap_name("accounts")
    .now_unix_s(now_unix);

    let mut remote =
        Residiuum::connect_heap(format!("residiuum://127.0.0.1:{port}/accounts"), options)
            .expect("connect_heap");
    let cid = remote.collection_open("users").unwrap();
    remote
        .put_json(&cid, "k1", &serde_json::json!({"v": 1}))
        .unwrap();
    remote
        .put_json(&cid, "k1", &serde_json::json!({"v": 2}))
        .unwrap();
    assert!(remote.delete(&cid, "k1").unwrap());
    remote
        .put_json(&cid, "k1", &serde_json::json!({"v": 3}))
        .unwrap();

    let (versions, holes) = remote.history(&cid, "k1").expect("history");
    assert!(!holes);
    assert!(
        versions.len() >= 3,
        "expected put/put/delete/put style stream, got {}",
        versions.len()
    );
    let kinds: Vec<&str> = versions
        .iter()
        .filter_map(|v| v.get("kind").and_then(|k| k.as_str()))
        .collect();
    assert!(kinds.iter().any(|k| *k == "put"));
    assert!(kinds.iter().any(|k| *k == "delete"));
    let last_put = versions
        .iter()
        .rev()
        .find(|v| v.get("kind").and_then(|k| k.as_str()) == Some("put"))
        .expect("last put");
    assert_eq!(last_put.get("json").and_then(|j| j.get("v")).unwrap(), 3);

    drop(remote);
    flag.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(50));
}

#[test]
fn connect_heap_find_filter() {
    use residiuum_store::{
        create_object, publish_staged_genesis, stage_heap_genesis, HeapMetaLayout, ObjectKind,
    };

    let doc = vectors();
    let inputs = &doc["inputs"];
    let master_pk: [u8; 32] = hex(inputs["master_public_key"].as_str().unwrap())
        .try_into()
        .unwrap();
    let holder_seed: [u8; 32] = hex(inputs["holder_seed"].as_str().unwrap())
        .try_into()
        .unwrap();
    let cose = hex(doc["accepted"][0]["cose_sign1"].as_str().unwrap());
    let verified = verify_certificate(&cose, &master_pk).unwrap();
    let now_unix = verified.not_before + 10;

    let snap = HeapSecuritySnapshot {
        deployment_id: verified.deployment_id,
        heap_id: verified.heap_id,
        authority_epoch: verified.authority_epoch,
        authority_generation: verified.authority_generation,
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
    let registry = Arc::new(ResidentHeapRegistry::new());
    registry.insert(
        verified.heap_id,
        ResidentHeap {
            slot: Arc::new(HeapSlot::new(snap)),
            display_name: Some("accounts".into()),
        },
    );

    let tmp = TempDir::new().unwrap();
    let store_path = tmp.path().join("app.residiuum");
    Store::open(&store_path).unwrap();
    let layout = HeapMetaLayout::new(&store_path);
    let dep = *verified.deployment_id.as_bytes();
    let heap_b = *verified.heap_id.as_bytes();
    let coll = *residiuum_heap::CollectionId::new_random()
        .unwrap()
        .as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_b, [9u8; 16], "accounts").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    create_object(
        &layout,
        &heap_b,
        ObjectKind::Collection,
        coll,
        [8u8; 16],
        "users",
    )
    .unwrap();

    let (ca_path, cert_path, key_path) = issue_localhost_tls(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&shutdown);
    let opts = ServeOptions::new()
        .legacy_token_server()
        .tls(TlsServerOptions::new(&cert_path, &key_path))
        .qualified_heap_key(true)
        .heap_registry(Arc::clone(&registry))
        .deployment_id(verified.deployment_id.to_string())
        .security_now_unix_s(now_unix)
        .shutdown_flag(Arc::clone(&shutdown));
    let store_c = store_path.clone();
    let bind_c = bind.clone();
    thread::spawn(move || {
        let _ = serve_store_with(store_c, &bind_c, opts);
    });
    wait_for_server(&bind);

    let holder = Arc::new(InMemoryHolderKey::from_seed(holder_seed));
    let credential = HeapCredential::new(&cose, holder).unwrap();
    let options = RemoteHeapOptions::new(
        TlsClientOptions::new("localhost").ca_path(ca_path),
        credential,
    )
    .expected_heap_name("accounts")
    .now_unix_s(now_unix);

    let mut remote =
        Residiuum::connect_heap(format!("residiuum://127.0.0.1:{port}/accounts"), options)
            .expect("connect_heap");
    let cid = remote.collection_open("users").unwrap();
    remote
        .put_json(&cid, "a", &serde_json::json!({"status": "active", "n": 1}))
        .unwrap();
    remote
        .put_json(&cid, "b", &serde_json::json!({"status": "paused", "n": 2}))
        .unwrap();
    remote
        .put_json(&cid, "c", &serde_json::json!({"status": "active", "n": 3}))
        .unwrap();

    let hits = remote
        .find(&cid, &serde_json::json!({"status": "active"}), Some(10))
        .expect("find");
    assert!(hits.coverage_complete);
    assert_eq!(hits.rows.len(), 2);
    assert!(hits.rows.iter().any(|(k, _)| k == "a"));
    assert!(hits.rows.iter().any(|(k, _)| k == "c"));
    assert!(!hits.rows.iter().any(|(k, _)| k == "b"));

    let gte = remote
        .find(&cid, &serde_json::json!({"n": {"$gte": 2}}), Some(10))
        .expect("find gte");
    assert!(gte.coverage_complete);
    assert_eq!(gte.rows.len(), 2);

    drop(remote);
    flag.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(50));
}

#[test]
fn connect_heap_indexes() {
    use residiuum_store::{
        create_object, publish_staged_genesis, stage_heap_genesis, HeapMetaLayout, ObjectKind,
    };

    let doc = vectors();
    let inputs = &doc["inputs"];
    let master_pk: [u8; 32] = hex(inputs["master_public_key"].as_str().unwrap())
        .try_into()
        .unwrap();
    let holder_seed: [u8; 32] = hex(inputs["holder_seed"].as_str().unwrap())
        .try_into()
        .unwrap();
    let cose = hex(doc["accepted"][0]["cose_sign1"].as_str().unwrap());
    let verified = verify_certificate(&cose, &master_pk).unwrap();
    let now_unix = verified.not_before + 10;

    let snap = HeapSecuritySnapshot {
        deployment_id: verified.deployment_id,
        heap_id: verified.heap_id,
        authority_epoch: verified.authority_epoch,
        authority_generation: verified.authority_generation,
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
    let registry = Arc::new(ResidentHeapRegistry::new());
    registry.insert(
        verified.heap_id,
        ResidentHeap {
            slot: Arc::new(HeapSlot::new(snap)),
            display_name: Some("accounts".into()),
        },
    );

    let tmp = TempDir::new().unwrap();
    let store_path = tmp.path().join("app.residiuum");
    Store::open(&store_path).unwrap();
    let layout = HeapMetaLayout::new(&store_path);
    let dep = *verified.deployment_id.as_bytes();
    let heap_b = *verified.heap_id.as_bytes();
    let coll = *residiuum_heap::CollectionId::new_random()
        .unwrap()
        .as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_b, [9u8; 16], "accounts").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    create_object(
        &layout,
        &heap_b,
        ObjectKind::Collection,
        coll,
        [8u8; 16],
        "users",
    )
    .unwrap();

    let (ca_path, cert_path, key_path) = issue_localhost_tls(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&shutdown);
    let opts = ServeOptions::new()
        .legacy_token_server()
        .tls(TlsServerOptions::new(&cert_path, &key_path))
        .qualified_heap_key(true)
        .heap_registry(Arc::clone(&registry))
        .deployment_id(verified.deployment_id.to_string())
        .security_now_unix_s(now_unix)
        .shutdown_flag(Arc::clone(&shutdown));
    let store_c = store_path.clone();
    let bind_c = bind.clone();
    thread::spawn(move || {
        let _ = serve_store_with(store_c, &bind_c, opts);
    });
    wait_for_server(&bind);

    let holder = Arc::new(InMemoryHolderKey::from_seed(holder_seed));
    let credential = HeapCredential::new(&cose, holder).unwrap();
    let options = RemoteHeapOptions::new(
        TlsClientOptions::new("localhost").ca_path(ca_path),
        credential,
    )
    .expected_heap_name("accounts")
    .now_unix_s(now_unix);

    let mut remote =
        Residiuum::connect_heap(format!("residiuum://127.0.0.1:{port}/accounts"), options)
            .expect("connect_heap");
    let cid = remote.collection_open("users").unwrap();
    remote
        .put_json(&cid, "a", &serde_json::json!({"status": "active", "n": 1}))
        .unwrap();
    remote
        .put_json(&cid, "b", &serde_json::json!({"status": "paused", "n": 2}))
        .unwrap();

    let empty = remote.index_list(&cid).expect("index_list empty");
    assert!(empty.is_empty());

    let created = remote
        .index_create(&cid, "by-status", &["status"])
        .expect("index_create");
    assert_eq!(
        created.get("name").and_then(|v| v.as_str()),
        Some("by-status")
    );
    assert_eq!(created.get("state").and_then(|v| v.as_str()), Some("ready"));
    assert_eq!(created.get("entry_count").and_then(|v| v.as_u64()), Some(2));
    assert_eq!(
        created.get("complete_coverage").and_then(|v| v.as_bool()),
        Some(true)
    );

    let listed = remote.index_list(&cid).expect("index_list");
    assert_eq!(listed.len(), 1);
    assert_eq!(
        listed[0].get("name").and_then(|v| v.as_str()),
        Some("by-status")
    );

    let rebuilt = remote
        .index_rebuild(&cid, "by-status")
        .expect("index_rebuild");
    assert_eq!(
        rebuilt.get("name").and_then(|v| v.as_str()),
        Some("by-status")
    );
    assert_eq!(rebuilt.get("state").and_then(|v| v.as_str()), Some("ready"));
    assert_eq!(rebuilt.get("entry_count").and_then(|v| v.as_u64()), Some(2));

    assert!(remote.index_drop(&cid, "by-status").expect("index_drop"));
    let after = remote.index_list(&cid).expect("index_list after drop");
    assert!(after.is_empty());

    drop(remote);
    flag.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(50));
}

/// Equality find uses a ready secondary index when present (op 116 + 131).
#[test]
fn connect_heap_find_via_index() {
    use residiuum_store::{
        create_object, publish_staged_genesis, stage_heap_genesis, HeapMetaLayout, ObjectKind,
    };

    let doc = vectors();
    let inputs = &doc["inputs"];
    let master_pk: [u8; 32] = hex(inputs["master_public_key"].as_str().unwrap())
        .try_into()
        .unwrap();
    let holder_seed: [u8; 32] = hex(inputs["holder_seed"].as_str().unwrap())
        .try_into()
        .unwrap();
    let cose = hex(doc["accepted"][0]["cose_sign1"].as_str().unwrap());
    let verified = verify_certificate(&cose, &master_pk).unwrap();
    let now_unix = verified.not_before + 10;
    assert_eq!(
        inputs["rights_mask"].as_u64().unwrap(),
        13,
        "bootstrap cert must include IndexAdmin"
    );

    let snap = HeapSecuritySnapshot {
        deployment_id: verified.deployment_id,
        heap_id: verified.heap_id,
        authority_epoch: verified.authority_epoch,
        authority_generation: verified.authority_generation,
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
    let registry = Arc::new(ResidentHeapRegistry::new());
    registry.insert(
        verified.heap_id,
        ResidentHeap {
            slot: Arc::new(HeapSlot::new(snap)),
            display_name: Some("accounts".into()),
        },
    );

    let tmp = TempDir::new().unwrap();
    let store_path = tmp.path().join("app.residiuum");
    Store::open(&store_path).unwrap();
    let layout = HeapMetaLayout::new(&store_path);
    let dep = *verified.deployment_id.as_bytes();
    let heap_b = *verified.heap_id.as_bytes();
    let coll = *residiuum_heap::CollectionId::new_random()
        .unwrap()
        .as_bytes();
    let staged = stage_heap_genesis(&layout, dep, heap_b, [9u8; 16], "accounts").unwrap();
    publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash).unwrap();
    create_object(
        &layout,
        &heap_b,
        ObjectKind::Collection,
        coll,
        [8u8; 16],
        "users",
    )
    .unwrap();

    let (ca_path, cert_path, key_path) = issue_localhost_tls(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&shutdown);
    let opts = ServeOptions::new()
        .legacy_token_server()
        .tls(TlsServerOptions::new(&cert_path, &key_path))
        .qualified_heap_key(true)
        .heap_registry(Arc::clone(&registry))
        .deployment_id(verified.deployment_id.to_string())
        .security_now_unix_s(now_unix)
        .shutdown_flag(Arc::clone(&shutdown));
    let store_c = store_path.clone();
    let bind_c = bind.clone();
    thread::spawn(move || {
        let _ = serve_store_with(store_c, &bind_c, opts);
    });
    wait_for_server(&bind);

    let holder = Arc::new(InMemoryHolderKey::from_seed(holder_seed));
    let credential = HeapCredential::new(&cose, holder).unwrap();
    let options = RemoteHeapOptions::new(
        TlsClientOptions::new("localhost").ca_path(ca_path),
        credential,
    )
    .expected_heap_name("accounts")
    .now_unix_s(now_unix);

    let mut remote =
        Residiuum::connect_heap(format!("residiuum://127.0.0.1:{port}/accounts"), options)
            .expect("connect_heap");
    let cid = remote.collection_open("users").unwrap();
    remote
        .put_json(&cid, "a", &serde_json::json!({"status": "active", "n": 1}))
        .unwrap();
    remote
        .put_json(&cid, "b", &serde_json::json!({"status": "paused", "n": 2}))
        .unwrap();
    remote
        .put_json(&cid, "c", &serde_json::json!({"status": "active", "n": 3}))
        .unwrap();

    remote
        .index_create(&cid, "by-status", &["status"])
        .expect("index_create for find acceleration");

    let hits = remote
        .find(&cid, &serde_json::json!({"status": "active"}), Some(10))
        .expect("find via index");
    assert!(hits.coverage_complete);
    assert_eq!(hits.rows.len(), 2);
    assert!(hits.rows.iter().any(|(k, _)| k == "a"));
    assert!(hits.rows.iter().any(|(k, _)| k == "c"));

    // Absence on a ready complete index should not surface paused rows.
    let none = remote
        .find(&cid, &serde_json::json!({"status": "gone"}), Some(10))
        .expect("find miss");
    assert!(none.coverage_complete);
    assert!(none.rows.is_empty());

    // Non-equality still works via scan fallback.
    let gte = remote
        .find(&cid, &serde_json::json!({"n": {"$gte": 2}}), Some(10))
        .expect("find gte scan");
    assert!(gte.coverage_complete);
    assert_eq!(gte.rows.len(), 2);

    // Write after index → stale; equality miss cannot prove absence via index.
    remote
        .put_json(&cid, "d", &serde_json::json!({"status": "active", "n": 4}))
        .unwrap();
    let listed = remote.index_list(&cid).expect("index_list after write");
    assert_eq!(listed.len(), 1);
    assert_eq!(
        listed[0].get("state").and_then(|v| v.as_str()),
        Some("stale"),
        "post-write index must be stale: {:?}",
        listed[0]
    );
    // Rebuild restores ready + complete coverage.
    let rebuilt = remote
        .index_rebuild(&cid, "by-status")
        .expect("rebuild after stale");
    assert_eq!(rebuilt.get("state").and_then(|v| v.as_str()), Some("ready"));
    let hits2 = remote
        .find(&cid, &serde_json::json!({"status": "active"}), Some(10))
        .expect("find after rebuild");
    assert!(hits2.coverage_complete);
    assert_eq!(hits2.rows.len(), 3);

    drop(remote);
    flag.store(true, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(50));
}

#[test]
fn credential_rejects_holder_mismatch() {
    let doc = vectors();
    let cose = hex(doc["accepted"][0]["cose_sign1"].as_str().unwrap());
    let wrong = Arc::new(InMemoryHolderKey::from_seed([9u8; 32]));
    let err = match HeapCredential::new(&cose, wrong) {
        Ok(_) => panic!("holder mismatch should reject"),
        Err(e) => e,
    };
    assert_eq!(err, residiuum_sdk::CredentialError::HolderKeyMismatch);
}
