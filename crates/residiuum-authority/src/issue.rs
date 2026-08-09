//! Local HeapKey issuance (`HEAP_SPEC` §31.2 / HP-005).

use crate::error::AuthorityError;
use crate::provider::MasterKeyProvider;
use crate::store::MasterAuthorityStore;
use ed25519_dalek::VerifyingKey;
use residiuum_format::{encode_deterministic_uint_map, CborValue};
use residiuum_heap::{
    verify_certificate, AuthorityEpoch, AuthorityGeneration, CertificateId, DeploymentId, HeapId,
    Rights, AUDIENCE_DATA_V1, CERT_MAX_LIFETIME_S, CONTENT_TYPE_CERTIFICATE,
    EXTERNAL_AAD_CERTIFICATE, PROFILE_VERSION,
};
use sha2::{Digest, Sha256};

/// Issue request (holder generates its own keypair).
#[derive(Debug, Clone)]
pub struct IssueRequest {
    /// Holder Ed25519 public key.
    pub holder_public_key: [u8; 32],
    /// Rights mask.
    pub rights: Rights,
    /// not_before unix seconds.
    pub not_before: u64,
    /// expires_at unix seconds.
    pub expires_at: u64,
}

/// Issued certificate bytes + fingerprint.
#[derive(Debug, Clone)]
pub struct IssuedHeapKey {
    /// Untagged COSE_Sign1 bytes.
    pub cose_sign1: Vec<u8>,
    /// SHA-256 of COSE bytes.
    pub fingerprint: [u8; 32],
}

/// Issue a HeapKey against the current anchored authority head.
pub fn issue_heap_key(
    store: &MasterAuthorityStore,
    provider: &dyn MasterKeyProvider,
    req: IssueRequest,
) -> Result<IssuedHeapKey, AuthorityError> {
    let head = store
        .load_head()?
        .ok_or_else(|| AuthorityError::Refused("no authority head".into()))?;
    if provider.public_key() != head.master_public_key {
        return Err(AuthorityError::Refused(
            "provider does not match head".into(),
        ));
    }
    VerifyingKey::from_bytes(&req.holder_public_key)
        .map_err(|e| AuthorityError::InvalidArgument(format!("holder key: {e}")))?;
    if req.expires_at <= req.not_before || req.expires_at - req.not_before > CERT_MAX_LIFETIME_S {
        return Err(AuthorityError::InvalidArgument("validity window".into()));
    }
    if req.rights.bits() & !head.access_policy.allowed_rights_mask != 0 {
        return Err(AuthorityError::Refused("rights exceed policy".into()));
    }

    let cert_id = CertificateId::new_random().map_err(|e| AuthorityError::Heap(e.to_string()))?;
    let deployment = DeploymentId::from_bytes_unchecked_nonzero(head.deployment_id)
        .map_err(|e| AuthorityError::Heap(e.to_string()))?;
    let heap = HeapId::from_bytes_unchecked_nonzero(head.heap_id)
        .map_err(|e| AuthorityError::Heap(e.to_string()))?;
    let epoch = AuthorityEpoch::new(head.authority_epoch)
        .map_err(|e| AuthorityError::Heap(e.to_string()))?;
    let generation = AuthorityGeneration::new(head.master_generation)
        .map_err(|e| AuthorityError::Heap(e.to_string()))?;

    let mut issuer = [0u8; 32];
    issuer.copy_from_slice(&Sha256::digest(head.master_public_key));

    let payload = encode_deterministic_uint_map(&[
        (1u64, CborValue::Uint(PROFILE_VERSION)),
        (2, CborValue::Bytes(deployment.as_bytes().to_vec())),
        (3, CborValue::Bytes(heap.as_bytes().to_vec())),
        (4, CborValue::Uint(epoch.get())),
        (5, CborValue::Uint(generation.get())),
        (6, CborValue::Bytes(cert_id.as_bytes().to_vec())),
        (7, CborValue::Bytes(req.holder_public_key.to_vec())),
        (8, CborValue::Uint(req.rights.bits())),
        (9, CborValue::Array(vec![])),
        (10, CborValue::Uint(req.not_before)),
        (11, CborValue::Uint(req.expires_at)),
        (12, CborValue::Text(AUDIENCE_DATA_V1.into())),
        (13, CborValue::Bytes(issuer.to_vec())),
    ])
    .map_err(|e| AuthorityError::Crypto(e.to_string()))?;

    let protected = encode_protected_header(CONTENT_TYPE_CERTIFICATE)?;
    let sig_structure = encode_sig_structure(&protected, EXTERNAL_AAD_CERTIFICATE, &payload);
    let signature = provider.sign(&sig_structure)?;

    let mut cose = Vec::new();
    cose.push(0x84);
    write_bstr(&mut cose, &protected);
    cose.push(0xa0);
    write_bstr(&mut cose, &payload);
    write_bstr(&mut cose, &signature);

    let verified = verify_certificate(&cose, &head.master_public_key)
        .map_err(|e| AuthorityError::Crypto(format!("self-verify: {e}")))?;

    Ok(IssuedHeapKey {
        cose_sign1: cose,
        fingerprint: verified.fingerprint,
    })
}

fn encode_protected_header(content_type: &str) -> Result<Vec<u8>, AuthorityError> {
    let mut out = vec![0xa2, 0x01, 0x27, 0x03]; // alg = -8 (EdDSA)
    write_text(&mut out, content_type);
    Ok(out)
}

fn encode_sig_structure(protected: &[u8], aad: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(0x84);
    write_text(&mut out, "Signature1");
    write_bstr(&mut out, protected);
    write_bstr(&mut out, aad);
    write_bstr(&mut out, payload);
    out
}

fn write_text(out: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    write_len_header(out, 0x60, b.len());
    out.extend_from_slice(b);
}

fn write_bstr(out: &mut Vec<u8>, b: &[u8]) {
    write_len_header(out, 0x40, b.len());
    out.extend_from_slice(b);
}

fn write_len_header(out: &mut Vec<u8>, major: u8, len: usize) {
    if len < 24 {
        out.push(major | (len as u8));
    } else if len < 256 {
        out.push(major | 24);
        out.push(len as u8);
    } else {
        out.push(major | 25);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    }
}
