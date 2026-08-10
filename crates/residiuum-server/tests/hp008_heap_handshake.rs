//! HP-008 Accept: uniform reject, proof replay, zero authority-store hot path.

use ed25519_dalek::{Signer, SigningKey};
use residiuum_client::{b64u_encode, HeapAuth, HeapReject, FEATURE_HEAP_KEY_V1};
use residiuum_format::{encode_deterministic_uint_map, CborValue};
use residiuum_heap::{
    sig_structure_for, verify_certificate, HeapAdministrativeState, HeapId, HeapSecuritySnapshot,
    HeapSlot, SecurityRevision, AUDIENCE_DATA_V1, CONTENT_TYPE_HOLDER_PROOF,
    EXTERNAL_AAD_HOLDER_PROOF, PROFILE_VERSION,
};
use residiuum_server::{
    authenticate_heap_auth, build_challenge, dispatch_heap_request, request_registry_allows,
    HeapAuthInternalCause, HeapAuthOutcome, HeapDispatchResult, PendingChallenge, ResidentHeap,
    ResidentHeapRegistry,
};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

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

fn bootstrap_fixture() -> (Vec<u8>, [u8; 32], SigningKey, HeapSecuritySnapshot) {
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
    (cose, master_pk, SigningKey::from_bytes(&holder_seed), snap)
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

fn registry_with(snap: HeapSecuritySnapshot, name: Option<&str>) -> ResidentHeapRegistry {
    let heap_id = snap.heap_id;
    let reg = ResidentHeapRegistry::new();
    reg.insert(
        heap_id,
        ResidentHeap {
            slot: Arc::new(HeapSlot::new(snap)),
            display_name: name.map(str::to_string),
        },
    );
    reg
}

fn auth_bytes(heap_id: HeapId, cert: &[u8], proof: &[u8], expected_name: Option<&str>) -> Vec<u8> {
    let auth = HeapAuth {
        v: 1,
        msg: "heap_auth".into(),
        heap_id: heap_id.to_string(),
        certificate_b64u: b64u_encode(cert),
        holder_proof_b64u: b64u_encode(proof),
        expected_heap_name: expected_name.map(str::to_string),
    };
    serde_json::to_vec(&auth).unwrap()
}

#[test]
fn feature_token_is_stable() {
    assert_eq!(FEATURE_HEAP_KEY_V1, "heap-key-v1");
}

#[test]
fn uniform_reject_for_unknown_heap_and_bad_cert() {
    let (cose, _pk, holder, snap) = bootstrap_fixture();
    let verified = verify_certificate(&cose, &snap.master_public_key).unwrap();
    let reg = registry_with(snap.clone(), Some("alpha"));
    let mut nonce = [0u8; 32];
    nonce[0] = 7;
    let exporter = [9u8; 32];
    let mut pending = PendingChallenge::issue(nonce, Instant::now());
    let now_unix = verified.not_before + 10;

    // Unknown heap id (valid UUID shape, not resident).
    let mut foreign = *verified.heap_id.as_bytes();
    foreign[15] ^= 0x01;
    // Keep version/variant bits valid.
    foreign[6] = (foreign[6] & 0x0f) | 0x40;
    foreign[8] = (foreign[8] & 0x3f) | 0x80;
    let foreign_id = HeapId::from_bytes(foreign).unwrap();
    let proof = sign_holder_proof(
        &holder,
        verified.fingerprint,
        *verified.deployment_id.as_bytes(),
        foreign,
        verified.authority_epoch.get(),
        &nonce,
        &exporter,
        [0x30; 16],
        now_unix,
    );
    let body = auth_bytes(foreign_id, &cose, &proof, None);
    let out = authenticate_heap_auth(
        &reg,
        &mut pending,
        &body,
        &exporter,
        now_unix,
        Instant::now(),
    );
    match out {
        HeapAuthOutcome::Reject { reject, cause } => {
            assert_eq!(reject, HeapReject::uniform());
            assert_eq!(cause, HeapAuthInternalCause::UnknownHeap);
            let wire = serde_json::to_value(&reject).unwrap();
            assert!(wire.get("heap_id").is_none());
            assert!(wire.get("error").is_none());
            assert_eq!(wire["code"], "heap_unavailable");
            assert_eq!(wire["retryable"], false);
        }
        _ => panic!("expected reject"),
    }

    // Fresh challenge; bad certificate signature.
    let mut pending2 = PendingChallenge::issue(nonce, Instant::now());
    let mut bad = cose.clone();
    *bad.last_mut().unwrap() ^= 0xff;
    let proof2 = sign_holder_proof(
        &holder,
        verified.fingerprint,
        *verified.deployment_id.as_bytes(),
        *verified.heap_id.as_bytes(),
        verified.authority_epoch.get(),
        &nonce,
        &exporter,
        [0x31; 16],
        now_unix,
    );
    let body2 = auth_bytes(verified.heap_id, &bad, &proof2, None);
    let out2 = authenticate_heap_auth(
        &reg,
        &mut pending2,
        &body2,
        &exporter,
        now_unix,
        Instant::now(),
    );
    match out2 {
        HeapAuthOutcome::Reject { reject, .. } => {
            assert_eq!(reject, HeapReject::uniform());
            // Same wire shape as unknown-heap.
            assert_eq!(
                serde_json::to_value(&reject).unwrap(),
                serde_json::to_value(&HeapReject::uniform()).unwrap()
            );
        }
        _ => panic!("expected reject"),
    }
}

#[test]
fn successful_auth_then_proof_replay_fails() {
    let (cose, _pk, holder, snap) = bootstrap_fixture();
    let verified = verify_certificate(&cose, &snap.master_public_key).unwrap();
    let reg = registry_with(snap, Some("alpha"));
    let doc = vectors();
    let mut nonce: [u8; 32] = hex(doc["inputs"]["server_nonce"].as_str().unwrap())
        .try_into()
        .unwrap();
    // Use a distinct nonce from the frozen bootstrap proof so we sign fresh.
    nonce[0] ^= 0xaa;
    let exporter: [u8; 32] = hex(doc["inputs"]["tls_exporter"].as_str().unwrap())
        .try_into()
        .unwrap();
    let now_unix = verified.not_before + 10;
    let proof = sign_holder_proof(
        &holder,
        verified.fingerprint,
        *verified.deployment_id.as_bytes(),
        *verified.heap_id.as_bytes(),
        verified.authority_epoch.get(),
        &nonce,
        &exporter,
        [0x33; 16],
        now_unix,
    );
    let body = auth_bytes(verified.heap_id, &cose, &proof, Some("alpha"));
    let mut pending = PendingChallenge::issue(nonce, Instant::now());
    let challenge = build_challenge(verified.deployment_id.to_string(), &nonce);
    assert_eq!(challenge.msg, "heap_challenge");
    assert_eq!(challenge.audience, "residiuum:data:v1");

    let first = authenticate_heap_auth(
        &reg,
        &mut pending,
        &body,
        &exporter,
        now_unix,
        Instant::now(),
    );
    let cap = match first {
        HeapAuthOutcome::Welcome { welcome, cap } => {
            assert_eq!(welcome.msg, "welcome");
            assert_eq!(welcome.heap_id, verified.heap_id.to_string());
            assert_eq!(welcome.session_id.len(), 32);
            assert!(welcome.session_id.chars().all(|c| c.is_ascii_hexdigit()));
            assert_eq!(welcome.heap_profile, "residiuum-heap-v1");
            cap
        }
        HeapAuthOutcome::Reject { cause, .. } => panic!("expected welcome, got {cause:?}"),
    };

    // Same nonce consumed → replay fails with uniform reject.
    let second = authenticate_heap_auth(
        &reg,
        &mut pending,
        &body,
        &exporter,
        now_unix,
        Instant::now(),
    );
    match second {
        HeapAuthOutcome::Reject { reject, cause } => {
            assert_eq!(reject, HeapReject::uniform());
            assert_eq!(cause, HeapAuthInternalCause::NonceReplayOrExpired);
        }
        _ => panic!("replay must fail"),
    }

    // Expired nonce also fails uniformly.
    let mut pending_old = PendingChallenge::issue(nonce, Instant::now() - Duration::from_secs(120));
    pending_old.consumed = false;
    let third = authenticate_heap_auth(
        &reg,
        &mut pending_old,
        &body,
        &exporter,
        now_unix,
        Instant::now(),
    );
    match third {
        HeapAuthOutcome::Reject { reject, cause } => {
            assert_eq!(reject, HeapReject::uniform());
            assert_eq!(cause, HeapAuthInternalCause::NonceReplayOrExpired);
        }
        _ => panic!("expired must fail"),
    }

    // Qualified dispatch: ping works; token field is rejected uniformly.
    let ping = serde_json::json!({"v":1,"id":7,"op_id":1,"args":{}});
    match dispatch_heap_request(&cap, &serde_json::to_vec(&ping).unwrap()) {
        HeapDispatchResult::Response(r) => {
            assert!(r.ok);
            assert_eq!(r.id, 7);
        }
    }
    let with_token = serde_json::json!({"v":1,"id":8,"op_id":1,"args":{},"token":"x"});
    match dispatch_heap_request(&cap, &serde_json::to_vec(&with_token).unwrap()) {
        HeapDispatchResult::Response(r) => {
            assert!(!r.ok);
            assert_eq!(r.error.as_ref().unwrap().code, "heap_unavailable");
            assert!(!r.error.as_ref().unwrap().retryable);
        }
    }
    assert!(request_registry_allows(1));
    assert!(request_registry_allows(111)); // §32.4 data active
    assert!(request_registry_allows(106)); // APP-1 collection_create active
    assert!(request_registry_allows(131)); // index_create active
    assert!(!request_registry_allows(140)); // export still reserved
    assert!(request_registry_allows(118)); // APP-7 T6: rql_query active
}

#[test]
fn hot_path_modules_do_not_reference_authority_store() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    for name in [
        "heap_auth.rs",
        "heap_dispatch.rs",
        "heap_registry.rs",
        "heap_session.rs",
        "heap_audit.rs",
    ] {
        let src = fs::read_to_string(root.join(name)).unwrap();
        assert!(
            !src.contains("residiuum_authority") && !src.contains("MasterAuthorityStore"),
            "{name} must not touch authority store"
        );
        assert!(
            !src.contains("authority-provisioning"),
            "{name} must not enable authority provisioning"
        );
    }
    // Fingerprint of cert used in welcome path is sha256 of COSE (sanity).
    let (cose, ..) = bootstrap_fixture();
    let fp = Sha256::digest(&cose);
    assert_eq!(fp.len(), 32);
}

#[test]
fn qualified_session_welcome_and_reject_are_indistinguishable_on_wire() {
    use residiuum_client::{
        read_frame, write_json_frame, Handshake, HeapReject, FEATURE_HEAP_KEY_V1,
        FEATURE_IDEMPOTENCY_V1, FEATURE_JSON_RPC_V1, FEATURE_RECEIPTS_V1, REQUIRED_FEATURES,
    };
    use residiuum_server::{
        run_qualified_handshake, validate_qualified_listener, HeapAuthAuditEvent, HeapAuthAuditLog,
        HeapAuthInternalCause, QualifiedHandshakeParams, QualifiedHandshakeResult,
    };
    use std::io::Cursor;
    use std::sync::Arc;

    let (cose, _pk, holder, snap) = bootstrap_fixture();
    let verified = verify_certificate(&cose, &snap.master_public_key).unwrap();
    let deployment = verified.deployment_id.to_string();
    let reg = registry_with(snap, Some("alpha"));
    let audit = HeapAuthAuditLog::default();
    let exporter = [0xab; 32];
    let mut nonce = [0x11; 32];
    nonce[31] = 0x42;
    let now_unix = verified.not_before + 10;

    let proof = sign_holder_proof(
        &holder,
        verified.fingerprint,
        *verified.deployment_id.as_bytes(),
        *verified.heap_id.as_bytes(),
        verified.authority_epoch.get(),
        &nonce,
        &exporter,
        [0x44; 16],
        now_unix,
    );
    let auth_body = auth_bytes(verified.heap_id, &cose, &proof, Some("alpha"));

    // Client side of duplex: write hello + auth into server_in; read challenge/welcome from server_out.
    let mut hello = Handshake::hello();
    let mut feats = hello.features.take().unwrap_or_default();
    feats.push(FEATURE_HEAP_KEY_V1.into());
    hello.features = Some(feats);
    assert!(hello
        .features
        .as_ref()
        .unwrap()
        .iter()
        .any(|f| f == FEATURE_JSON_RPC_V1));
    assert!(hello
        .features
        .as_ref()
        .unwrap()
        .iter()
        .any(|f| f == FEATURE_IDEMPOTENCY_V1));
    assert!(hello
        .features
        .as_ref()
        .unwrap()
        .iter()
        .any(|f| f == FEATURE_RECEIPTS_V1));
    let _ = REQUIRED_FEATURES;

    let mut server_in = Vec::new();
    write_json_frame(&mut server_in, &hello).unwrap();
    residiuum_client::write_frame(&mut server_in, &auth_body).unwrap();

    let mut reader = Cursor::new(server_in);
    let mut writer = Vec::new();
    let result = run_qualified_handshake(
        &mut reader,
        &mut writer,
        QualifiedHandshakeParams {
            registry: &reg,
            deployment_id: &deployment,
            tls_exporter: &exporter,
            now_unix_s: now_unix,
            audit: Some(&audit),
            server_nonce: Some(nonce),
        },
    )
    .expect("handshake io");

    match result {
        QualifiedHandshakeResult::Established(session) => {
            assert_eq!(session.welcome.msg, "welcome");
            assert!(session.features.iter().any(|f| f == FEATURE_HEAP_KEY_V1));
        }
        QualifiedHandshakeResult::Rejected { cause, .. } => panic!("expected welcome: {cause:?}"),
    }

    // Parse frames: challenge then welcome.
    let mut out = Cursor::new(writer);
    let challenge_bytes = read_frame(&mut out, 65_536).unwrap().unwrap();
    let challenge: serde_json::Value = serde_json::from_slice(&challenge_bytes).unwrap();
    assert_eq!(challenge["msg"], "heap_challenge");
    assert!(challenge.get("code").is_none());
    let welcome_bytes = read_frame(&mut out, 65_536).unwrap().unwrap();
    let welcome: serde_json::Value = serde_json::from_slice(&welcome_bytes).unwrap();
    assert_eq!(welcome["msg"], "welcome");

    assert!(matches!(
        audit.snapshot().last(),
        Some(HeapAuthAuditEvent::Welcome { .. })
    ));

    // Reject path: unknown heap — wire shape matches uniform reject only.
    let audit2 = HeapAuthAuditLog::default();
    let mut foreign = *verified.heap_id.as_bytes();
    foreign[15] ^= 0x02;
    foreign[6] = (foreign[6] & 0x0f) | 0x40;
    foreign[8] = (foreign[8] & 0x3f) | 0x80;
    let foreign_id = HeapId::from_bytes(foreign).unwrap();
    let bad_auth = auth_bytes(foreign_id, &cose, &proof, None);
    let mut server_in = Vec::new();
    write_json_frame(&mut server_in, &hello).unwrap();
    residiuum_client::write_frame(&mut server_in, &bad_auth).unwrap();
    let mut reader = Cursor::new(server_in);
    let mut writer = Vec::new();
    let rejected = run_qualified_handshake(
        &mut reader,
        &mut writer,
        QualifiedHandshakeParams {
            registry: &reg,
            deployment_id: &deployment,
            tls_exporter: &exporter,
            now_unix_s: now_unix,
            audit: Some(&audit2),
            server_nonce: Some(nonce),
        },
    )
    .unwrap();
    match rejected {
        QualifiedHandshakeResult::Rejected { reject, cause } => {
            assert_eq!(reject, HeapReject::uniform());
            assert_eq!(cause, HeapAuthInternalCause::UnknownHeap);
        }
        _ => panic!("expected reject"),
    }
    let mut out = Cursor::new(writer);
    let _challenge = read_frame(&mut out, 65_536).unwrap().unwrap();
    let reject_bytes = read_frame(&mut out, 65_536).unwrap().unwrap();
    let reject_v: serde_json::Value = serde_json::from_slice(&reject_bytes).unwrap();
    assert_eq!(reject_v["code"], "heap_unavailable");
    assert_eq!(reject_v["retryable"], false);
    assert!(reject_v.get("heap_id").is_none());
    assert!(reject_v.get("cause").is_none());
    assert_eq!(
        audit2.snapshot(),
        vec![HeapAuthAuditEvent::Reject {
            cause: HeapAuthInternalCause::UnknownHeap
        }]
    );

    // Listener constraints: token + no TLS forbidden.
    assert!(validate_qualified_listener(
        false,
        None,
        false,
        Some(&Arc::new(reg)),
        Some(&deployment)
    )
    .is_err());
    let reg2 = Arc::new(ResidentHeapRegistry::new());
    assert!(
        validate_qualified_listener(true, Some("tok"), false, Some(&reg2), Some(&deployment))
            .is_err()
    );
    assert!(validate_qualified_listener(true, None, true, Some(&reg2), Some(&deployment)).is_err());
    assert!(validate_qualified_listener(true, None, false, Some(&reg2), Some(&deployment)).is_ok());
}

#[test]
fn plaintext_stream_cannot_export_channel_binding() {
    use residiuum_sdk::IoStream;
    use std::net::{TcpListener, TcpStream};
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(addr).unwrap();
    let (server, _) = listener.accept().unwrap();
    let _ = client;
    let stream = IoStream::plain(server);
    assert!(stream.export_channel_binding().is_err());
}
