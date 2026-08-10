//! HP-008 Accept: live TLS accept-loop runs qualified heap session (no token/RBAC).

use ed25519_dalek::{Signer, SigningKey};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose, SanType};
use residiuum_client::{
    b64u_decode, b64u_encode, read_frame, write_json_frame, Handshake, HeapAuth, HeapChallenge,
    HeapWelcome, FEATURE_HEAP_KEY_V1,
};
use residiuum_format::{encode_deterministic_uint_map, CborValue};
use residiuum_heap::{
    sig_structure_for, verify_certificate, HeapAdministrativeState, HeapSecuritySnapshot, HeapSlot,
    SecurityRevision, AUDIENCE_DATA_V1, CONTENT_TYPE_HOLDER_PROOF, EXTERNAL_AAD_HOLDER_PROOF,
    PROFILE_VERSION,
};
use residiuum_sdk::{client_connect, TlsClientOptions, TlsServerOptions};
use residiuum_server::{
    serve_store_with, HeapAuthAuditLog, ResidentHeap, ResidentHeapRegistry, ServeOptions,
};
use residiuum_store::Store;
use std::fs;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

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

fn write_bstr(out: &mut Vec<u8>, bytes: &[u8]) {
    if bytes.len() <= 23 {
        out.push(0x40 | (bytes.len() as u8));
    } else if bytes.len() <= 255 {
        out.push(0x58);
        out.push(bytes.len() as u8);
    } else {
        out.push(0x59);
        out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

fn write_text(out: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    if b.len() <= 23 {
        out.push(0x60 | (b.len() as u8));
    } else {
        out.push(0x78);
        out.push(b.len() as u8);
    }
    out.extend_from_slice(b);
}

fn sign_holder_proof(
    holder: &SigningKey,
    cert_fingerprint: [u8; 32],
    deployment: [u8; 16],
    heap: [u8; 16],
    epoch: u64,
    server_nonce: &[u8; 32],
    tls_exporter: &[u8; 32],
    proof_id: [u8; 16],
    created_at: u64,
) -> Vec<u8> {
    let payload = encode_deterministic_uint_map(&[
        (1u64, CborValue::Uint(PROFILE_VERSION)),
        (2, CborValue::Bytes(proof_id.to_vec())),
        (3, CborValue::Uint(created_at)),
        (4, CborValue::Bytes(cert_fingerprint.to_vec())),
        (5, CborValue::Bytes(deployment.to_vec())),
        (6, CborValue::Bytes(heap.to_vec())),
        (7, CborValue::Uint(epoch)),
        (8, CborValue::Text(AUDIENCE_DATA_V1.into())),
        (9, CborValue::Bytes(server_nonce.to_vec())),
        (10, CborValue::Bytes(tls_exporter.to_vec())),
        (
            11,
            CborValue::Array(vec![
                CborValue::Uint(1),
                CborValue::Uint(1),
                CborValue::Uint(0),
            ]),
        ),
    ])
    .unwrap();

    let mut protected = Vec::new();
    protected.push(0xa2);
    protected.push(0x01);
    protected.push(0x27);
    protected.push(0x03);
    write_text(&mut protected, CONTENT_TYPE_HOLDER_PROOF);

    let sig_structure = sig_structure_for(&protected, EXTERNAL_AAD_HOLDER_PROOF, &payload);
    let signature = holder.sign(&sig_structure).to_bytes();

    let mut cose = Vec::new();
    cose.push(0x84);
    write_bstr(&mut cose, &protected);
    cose.push(0xa0);
    write_bstr(&mut cose, &payload);
    write_bstr(&mut cose, &signature);
    cose
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
fn live_tls_accept_loop_heap_key_ping_without_token() {
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
    let holder = SigningKey::from_bytes(&holder_seed);
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
            display_name: Some("alpha".into()),
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

    let tcp = TcpStream::connect(&bind).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    tcp.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    let (mut stream, _) = client_connect(tcp, &TlsClientOptions::new("localhost").ca_path(ca_path))
        .expect("tls client");
    assert!(stream.is_tls());
    let exporter = stream.export_channel_binding().expect("client exporter");

    let mut hello = Handshake::hello();
    let mut feats = hello.features.take().unwrap_or_default();
    feats.push(FEATURE_HEAP_KEY_V1.into());
    hello.features = Some(feats);
    write_json_frame(&mut stream, &hello).unwrap();

    let challenge_bytes = read_frame(&mut stream, 65_536).unwrap().unwrap();
    let challenge: HeapChallenge = serde_json::from_slice(&challenge_bytes).unwrap();
    assert_eq!(challenge.msg, "heap_challenge");
    let nonce_vec = b64u_decode(&challenge.server_nonce_b64u).unwrap();
    let mut nonce = [0u8; 32];
    nonce.copy_from_slice(&nonce_vec);

    let proof = sign_holder_proof(
        &holder,
        verified.fingerprint,
        *verified.deployment_id.as_bytes(),
        *verified.heap_id.as_bytes(),
        verified.authority_epoch.get(),
        &nonce,
        &exporter,
        [0x55; 16],
        now_unix,
    );
    let auth = HeapAuth {
        v: 1,
        msg: "heap_auth".into(),
        heap_id: verified.heap_id.to_string(),
        certificate_b64u: b64u_encode(&cose),
        holder_proof_b64u: b64u_encode(&proof),
        expected_heap_name: Some("alpha".into()),
    };
    write_json_frame(&mut stream, &auth).unwrap();

    let welcome_bytes = read_frame(&mut stream, 65_536).unwrap().unwrap();
    let welcome: HeapWelcome = serde_json::from_slice(&welcome_bytes).unwrap();
    assert_eq!(welcome.msg, "welcome");
    assert_eq!(welcome.heap_id, verified.heap_id.to_string());

    // Qualified ping — no token field.
    let ping = serde_json::json!({"v":1,"id":1,"op_id":1,"args":{}});
    write_json_frame(&mut stream, &ping).unwrap();
    let resp_bytes = read_frame(&mut stream, 65_536).unwrap().unwrap();
    let resp: serde_json::Value = serde_json::from_slice(&resp_bytes).unwrap();
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["result"]["pong"], true);

    // Token on qualified path must fail closed as heap_unavailable.
    let with_token = serde_json::json!({"v":1,"id":2,"op_id":1,"args":{},"token":"nope"});
    write_json_frame(&mut stream, &with_token).unwrap();
    let bad_bytes = read_frame(&mut stream, 65_536).unwrap().unwrap();
    let bad: serde_json::Value = serde_json::from_slice(&bad_bytes).unwrap();
    assert_eq!(bad["ok"], false);
    assert_eq!(bad["error"]["code"], "heap_unavailable");
    assert_eq!(bad["error"]["retryable"], false);

    // Drop client; stop server.
    drop(stream);
    flag.store(true, Ordering::SeqCst);
    // Give the accept loop a moment to notice shutdown.
    thread::sleep(Duration::from_millis(50));

    assert!(
        audit
            .snapshot()
            .iter()
            .any(|e| matches!(e, residiuum_server::HeapAuthAuditEvent::Welcome { .. })),
        "audit must record welcome"
    );
}
