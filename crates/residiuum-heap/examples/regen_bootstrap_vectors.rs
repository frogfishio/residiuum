//! One-shot: regenerate HP-000 bootstrap cert+proof with an expanded rights mask.
//!
//! Usage (from workspace root):
//!   cargo run -p residiuum-heap --example regen_bootstrap_vectors -- 13
//!
//! Default rights_mask is 13 = Read | Write | IndexAdmin.

use ed25519_dalek::{Signer, SigningKey};
use residiuum_format::{encode_deterministic_uint_map, CborValue};
use residiuum_heap::{
    build_holder_proof, inspect_certificate, sig_structure_for, verify_certificate,
    verify_holder_proof, AUDIENCE_DATA_V1, CONTENT_TYPE_CERTIFICATE, EXTERNAL_AAD_CERTIFICATE,
    PROFILE_VERSION,
};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let rights_mask: u64 = env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(13); // Read(1)|Write(4)|IndexAdmin(8)

    let root = workspace_root();
    let path = root.join("spec/heap/vectors-v1.json");
    let mut doc: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&path).expect("read vectors")).expect("json");

    let inputs = doc["inputs"].as_object_mut().expect("inputs");
    inputs.insert("rights_mask".into(), serde_json::json!(rights_mask));

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
    let proof_id = hex16(inputs["proof_id"].as_str().unwrap());
    let issuer = Sha256::digest(master_pk);
    let not_before = inputs["not_before"].as_u64().unwrap();
    let expires_at = inputs["expires_at"].as_u64().unwrap();
    let nonce = hex32(inputs["server_nonce"].as_str().unwrap());
    let exporter = hex32(inputs["tls_exporter"].as_str().unwrap());

    // protected: {1: -8, 3: content-type}
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

    let verified = verify_certificate(&cose, &master_pk).expect("verify cert");
    assert_eq!(verified.rights.bits(), rights_mask);

    let cert_entry = serde_json::json!({
        "id": "cert-bootstrap-v1",
        "kind": "heap_key_certificate",
        "protected": hex::encode(&protected),
        "payload": hex::encode(&payload),
        "sig_structure": hex::encode(&sig_structure),
        "cose_sign1": hex::encode(&cose),
        "sha256": hex::encode(verified.fingerprint),
    });

    let inspected = inspect_certificate(&cose).expect("inspect");
    let proof_cose = build_holder_proof(
        &inspected,
        &nonce,
        &exporter,
        proof_id,
        not_before + 10,
        |msg| Ok(holder.sign(msg).to_bytes()),
    )
    .expect("proof");
    verify_holder_proof(&proof_cose, &verified, &nonce, &exporter).expect("verify proof");

    // Re-extract proof fields for the vector document.
    let proof_parts = decode_cose4(&proof_cose);
    let proof_sig_structure = sig_structure_for(
        &proof_parts.0,
        residiuum_heap::EXTERNAL_AAD_HOLDER_PROOF,
        &proof_parts.2,
    );

    let proof_entry = serde_json::json!({
        "id": "proof-bootstrap-v1",
        "kind": "holder_proof",
        "protected": hex::encode(&proof_parts.0),
        "payload": hex::encode(&proof_parts.2),
        "sig_structure": hex::encode(&proof_sig_structure),
        "cose_sign1": hex::encode(&proof_cose),
    });

    doc["accepted"] = serde_json::json!([cert_entry, proof_entry]);

    // Rebuild rejected corpus from accepted materials (same mutation catalog).
    let mut rejected = Vec::new();
    rejected.push(flip_last(
        &cose,
        "cert_flip_last_sig_byte",
        "heap_key_certificate",
    ));
    rejected.push(flip_at(
        &cose,
        40,
        "cert_flip_byte_40",
        "heap_key_certificate",
    ));
    rejected.push(truncate_bytes(
        &cose,
        2,
        "cert_truncated",
        "heap_key_certificate",
    ));
    rejected.push(flip_last(
        &proof_cose,
        "proof_flip_last_sig_byte",
        "holder_proof",
    ));
    rejected.push(flip_at(
        &proof_cose,
        80,
        "proof_flip_byte_80",
        "holder_proof",
    ));
    rejected.push(truncate_bytes(
        &proof_cose,
        2,
        "proof_truncated",
        "holder_proof",
    ));
    rejected.push(serde_json::json!({
        "id": "empty",
        "kind": "heap_key_certificate",
        "cose_sign1": "",
        "expect_public": "heap_unavailable",
        "expect_internal": "malformed_or_bad_signature"
    }));
    rejected.push(serde_json::json!({
        "id": "not_cbor_array",
        "kind": "heap_key_certificate",
        "cose_sign1": "a0",
        "expect_public": "heap_unavailable",
        "expect_internal": "malformed_or_bad_signature"
    }));
    // Payload region mutations (keep same relative offsets as historical corpus).
    rejected.push(flip_at(
        &cose,
        55,
        "cert_flip_payload_rights_byte",
        "heap_key_certificate",
    ));
    rejected.push(flip_at(
        &cose,
        30,
        "cert_flip_heap_id_nibble",
        "heap_key_certificate",
    ));
    rejected.push(flip_at(
        &cose,
        100,
        "cert_flip_not_before",
        "heap_key_certificate",
    ));
    rejected.push(flip_at(
        &proof_cose,
        70,
        "proof_flip_nonce_region",
        "holder_proof",
    ));
    rejected.push(flip_at(
        &proof_cose,
        50,
        "proof_flip_exporter_region",
        "holder_proof",
    ));
    // Flip protected alg byte (-8 / 0x27 → 0x26).
    let mut proof_alg = proof_cose.clone();
    if let Some(pos) = proof_alg.iter().position(|&b| b == 0x27) {
        proof_alg[pos] = 0x26;
    }
    rejected.push(serde_json::json!({
        "id": "proof_flip_protected_alg",
        "kind": "holder_proof",
        "cose_sign1": hex::encode(proof_alg),
        "expect_public": "heap_unavailable",
        "expect_internal": "malformed_or_bad_signature"
    }));

    // Preserve note on rights flip when present.
    if let Some(obj) = rejected
        .iter_mut()
        .find(|v| v["id"] == "cert_flip_payload_rights_byte")
    {
        obj.as_object_mut().unwrap().insert(
            "note".into(),
            serde_json::json!("mutates near rights/constraints region of payload"),
        );
    }

    doc["rejected"] = serde_json::Value::Array(rejected);

    let out = serde_json::to_string_pretty(&doc).expect("serialize") + "\n";
    fs::write(&path, out).expect("write vectors");
    println!(
        "updated {} with rights_mask=0x{:x} ({})",
        path.display(),
        rights_mask,
        rights_mask
    );
}

fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // crates
    p.pop(); // workspace
    p
}

fn hex16(s: &str) -> [u8; 16] {
    let b = hex::decode(s).expect("hex16");
    b.try_into().expect("16")
}

fn hex32(s: &str) -> [u8; 32] {
    let b = hex::decode(s).expect("hex32");
    b.try_into().expect("32")
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

fn decode_cose4(bytes: &[u8]) -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) {
    // Minimal: use ciborium-less hand parse for definite array of 4 bstr/map.
    // Rely on known structure from build_holder_proof.
    assert_eq!(bytes[0], 0x84);
    let mut i = 1;
    let protected = read_bstr(bytes, &mut i);
    assert_eq!(bytes[i], 0xa0);
    i += 1;
    let payload = read_bstr(bytes, &mut i);
    let signature = read_bstr(bytes, &mut i);
    (protected, vec![], payload, signature)
}

fn read_bstr(bytes: &[u8], i: &mut usize) -> Vec<u8> {
    let b0 = bytes[*i];
    *i += 1;
    let len = if b0 & 0xe0 == 0x40 {
        let ai = b0 & 0x1f;
        if ai < 24 {
            ai as usize
        } else if ai == 24 {
            let l = bytes[*i] as usize;
            *i += 1;
            l
        } else if ai == 25 {
            let l = u16::from_be_bytes([bytes[*i], bytes[*i + 1]]) as usize;
            *i += 2;
            l
        } else {
            panic!("bstr len")
        }
    } else {
        panic!("not bstr {:02x}", b0);
    };
    let out = bytes[*i..*i + len].to_vec();
    *i += len;
    out
}

fn flip_last(cose: &[u8], id: &str, kind: &str) -> serde_json::Value {
    let mut b = cose.to_vec();
    if let Some(last) = b.last_mut() {
        *last ^= 1;
    }
    reject(id, kind, &b)
}

fn flip_at(cose: &[u8], idx: usize, id: &str, kind: &str) -> serde_json::Value {
    let mut b = cose.to_vec();
    if idx < b.len() {
        b[idx] ^= 1;
    }
    reject(id, kind, &b)
}

fn truncate_bytes(cose: &[u8], n: usize, id: &str, kind: &str) -> serde_json::Value {
    let end = cose.len().saturating_sub(n);
    reject(id, kind, &cose[..end])
}

fn reject(id: &str, kind: &str, cose: &[u8]) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "kind": kind,
        "cose_sign1": hex::encode(cose),
        "expect_public": "heap_unavailable",
        "expect_internal": "malformed_or_bad_signature"
    })
}
