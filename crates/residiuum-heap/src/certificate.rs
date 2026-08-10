//! Untagged COSE Sign1 + HeapKey certificate verification (`HEAP_SPEC` §31.2).

use crate::constraints::Constraints;
use crate::error::{HeapError, HeapUnavailableCause};
use crate::ids::{AuthorityEpoch, AuthorityGeneration, CertificateId, DeploymentId, HeapId};
use crate::rights::Rights;
use crate::wire::{
    AUDIENCE_DATA_V1, CERT_MAX_BYTES, CERT_MAX_LIFETIME_S, CONTENT_TYPE_CERTIFICATE,
    COSE_ALG_EDDSA, EXTERNAL_AAD_CERTIFICATE, PROFILE_VERSION,
};
use ed25519_dalek::{Signature, VerifyingKey};
use residiuum_format::{decode_deterministic_uint_map, CborValue};
use sha2::{Digest, Sha256};

/// Verified certificate claims (no private key material).
#[derive(Debug, Clone)]
pub struct VerifiedCertificate {
    /// Complete COSE bytes that were verified.
    pub cose_bytes: Vec<u8>,
    /// SHA-256 of `cose_bytes`.
    pub fingerprint: [u8; 32],
    /// Deployment.
    pub deployment_id: DeploymentId,
    /// Heap.
    pub heap_id: HeapId,
    /// Authority epoch.
    pub authority_epoch: AuthorityEpoch,
    /// Authority generation.
    pub authority_generation: AuthorityGeneration,
    /// Certificate id.
    pub certificate_id: CertificateId,
    /// Holder verifying key bytes.
    pub holder_public_key: [u8; 32],
    /// Rights.
    pub rights: Rights,
    /// Constraints.
    pub constraints: Constraints,
    /// not_before unix seconds.
    pub not_before: u64,
    /// expires_at unix seconds.
    pub expires_at: u64,
    /// Issuer master key id (SHA-256 of master public key).
    pub issuer_master_key_id: [u8; 32],
}

/// Structurally inspect a certificate COSE without verifying the master signature.
///
/// Used by local [`HeapCredential`] construction to bind a holder signer to the
/// certificate's claimed public key. The **server** still performs full
/// [`verify_certificate`] against the resident master key; this function alone
/// is not authority.
pub fn inspect_certificate(cose: &[u8]) -> Result<VerifiedCertificate, HeapError> {
    if cose.len() > CERT_MAX_BYTES || cose.is_empty() {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let parts = decode_cose_sign1(cose)?;
    if !parts.unprotected_empty {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let (alg, content_type) = decode_protected_header(&parts.protected)?;
    if alg != COSE_ALG_EDDSA || content_type != CONTENT_TYPE_CERTIFICATE {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    if parts.signature.len() != 64 {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let claims = decode_certificate_payload(&parts.payload)?;
    let mut fingerprint = [0u8; 32];
    fingerprint.copy_from_slice(&Sha256::digest(cose));
    Ok(VerifiedCertificate {
        cose_bytes: cose.to_vec(),
        fingerprint,
        deployment_id: claims.deployment_id,
        heap_id: claims.heap_id,
        authority_epoch: claims.authority_epoch,
        authority_generation: claims.authority_generation,
        certificate_id: claims.certificate_id,
        holder_public_key: claims.holder_public_key,
        rights: claims.rights,
        constraints: claims.constraints,
        not_before: claims.not_before,
        expires_at: claims.expires_at,
        issuer_master_key_id: claims.issuer_master_key_id,
    })
}

/// Verify an untagged COSE Sign1 HeapKey certificate against a master public key.
pub fn verify_certificate(
    cose: &[u8],
    master_public_key: &[u8; 32],
) -> Result<VerifiedCertificate, HeapError> {
    if cose.len() > CERT_MAX_BYTES || cose.is_empty() {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let parts = decode_cose_sign1(cose)?;
    if !parts.unprotected_empty {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let (alg, content_type) = decode_protected_header(&parts.protected)?;
    if alg != COSE_ALG_EDDSA || content_type != CONTENT_TYPE_CERTIFICATE {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let sig_structure =
        encode_sig_structure(&parts.protected, EXTERNAL_AAD_CERTIFICATE, &parts.payload);
    let vk = VerifyingKey::from_bytes(master_public_key)
        .map_err(|_| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;
    let signature = Signature::from_slice(&parts.signature)
        .map_err(|_| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;
    vk.verify_strict(&sig_structure, &signature)
        .map_err(|_| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;

    let claims = decode_certificate_payload(&parts.payload)?;
    let expected_issuer = Sha256::digest(master_public_key);
    if claims.issuer_master_key_id != expected_issuer.as_slice() {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let mut fingerprint = [0u8; 32];
    fingerprint.copy_from_slice(&Sha256::digest(cose));
    Ok(VerifiedCertificate {
        cose_bytes: cose.to_vec(),
        fingerprint,
        deployment_id: claims.deployment_id,
        heap_id: claims.heap_id,
        authority_epoch: claims.authority_epoch,
        authority_generation: claims.authority_generation,
        certificate_id: claims.certificate_id,
        holder_public_key: claims.holder_public_key,
        rights: claims.rights,
        constraints: claims.constraints,
        not_before: claims.not_before,
        expires_at: claims.expires_at,
        issuer_master_key_id: claims.issuer_master_key_id,
    })
}

struct CoseParts {
    protected: Vec<u8>,
    unprotected_empty: bool,
    payload: Vec<u8>,
    signature: Vec<u8>,
}

fn decode_cose_sign1(bytes: &[u8]) -> Result<CoseParts, HeapError> {
    let mut cur = Cursor::new(bytes);
    let n = cur.read_array_len()?;
    if n != 4 {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let protected = cur.read_bstr()?;
    // unprotected map must be empty definite map 0xa0
    let unprot = cur.read_raw_map_is_empty()?;
    let payload = cur.read_bstr()?;
    let signature = cur.read_bstr()?;
    if !cur.is_empty() {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    if signature.len() != 64 {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    Ok(CoseParts {
        protected,
        unprotected_empty: unprot,
        payload,
        signature,
    })
}

fn decode_protected_header(bytes: &[u8]) -> Result<(i64, String), HeapError> {
    let map = decode_deterministic_uint_map(bytes)
        .map_err(|_| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;
    if map.len() != 2 {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let mut alg = None;
    let mut ct = None;
    for (k, v) in map {
        match k {
            1 => match v {
                CborValue::Nint(n) if n + 1 == 8 => alg = Some(-8i64), // -1 - n = -8 => n = 7
                CborValue::Nint(n) => {
                    // CBOR neg int: value = -1 - n
                    let v = -1i64 - (n as i64);
                    alg = Some(v);
                }
                _ => {
                    return Err(HeapError::unavailable(
                        HeapUnavailableCause::MalformedOrBadSignature,
                    ))
                }
            },
            3 => match v {
                CborValue::Text(s) => ct = Some(s),
                _ => {
                    return Err(HeapError::unavailable(
                        HeapUnavailableCause::MalformedOrBadSignature,
                    ))
                }
            },
            _ => {
                return Err(HeapError::unavailable(
                    HeapUnavailableCause::MalformedOrBadSignature,
                ))
            }
        }
    }
    match (alg, ct) {
        (Some(a), Some(c)) => Ok((a, c)),
        _ => Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        )),
    }
}

fn encode_sig_structure(protected: &[u8], aad: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    // array of 4
    out.push(0x84);
    // text "Signature1"
    write_text(&mut out, "Signature1");
    write_bstr(&mut out, protected);
    write_bstr(&mut out, aad);
    write_bstr(&mut out, payload);
    out
}

struct Claims {
    deployment_id: DeploymentId,
    heap_id: HeapId,
    authority_epoch: AuthorityEpoch,
    authority_generation: AuthorityGeneration,
    certificate_id: CertificateId,
    holder_public_key: [u8; 32],
    rights: Rights,
    constraints: Constraints,
    not_before: u64,
    expires_at: u64,
    issuer_master_key_id: [u8; 32],
}

fn decode_certificate_payload(bytes: &[u8]) -> Result<Claims, HeapError> {
    let map = decode_deterministic_uint_map(bytes)
        .map_err(|_| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;
    if map.len() != 13 {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let mut profile = None;
    let mut deployment = None;
    let mut heap = None;
    let mut epoch = None;
    let mut generation = None;
    let mut cert_id = None;
    let mut holder = None;
    let mut rights = None;
    let mut constraints = None;
    let mut not_before = None;
    let mut expires = None;
    let mut audience = None;
    let mut issuer = None;
    for (k, v) in map {
        match k {
            1 => profile = Some(expect_uint(&v)?),
            2 => deployment = Some(expect_bstr16(&v).and_then(DeploymentId::from_bytes)?),
            3 => heap = Some(expect_bstr16(&v).and_then(HeapId::from_bytes)?),
            4 => epoch = Some(AuthorityEpoch::new(expect_uint(&v)?)?),
            5 => generation = Some(AuthorityGeneration::new(expect_uint(&v)?)?),
            6 => cert_id = Some(expect_bstr16(&v).and_then(CertificateId::from_bytes)?),
            7 => holder = Some(expect_bstr32(&v)?),
            8 => rights = Some(Rights::from_bits_certificate(expect_uint(&v)?)?),
            9 => constraints = Some(decode_constraints_value(&v)?),
            10 => not_before = Some(expect_uint(&v)?),
            11 => expires = Some(expect_uint(&v)?),
            12 => audience = Some(expect_text(&v)?),
            13 => issuer = Some(expect_bstr32(&v)?),
            _ => {
                return Err(HeapError::unavailable(
                    HeapUnavailableCause::MalformedOrBadSignature,
                ))
            }
        }
    }
    if profile != Some(PROFILE_VERSION) {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let not_before = not_before
        .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;
    let expires = expires
        .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;
    if expires <= not_before || expires - not_before > CERT_MAX_LIFETIME_S {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    if audience.as_deref() != Some(AUDIENCE_DATA_V1) {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    // Reject non-canonical / weak holder keys by attempting VerifyingKey parse.
    let holder = holder
        .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;
    VerifyingKey::from_bytes(&holder)
        .map_err(|_| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;

    Ok(Claims {
        deployment_id: deployment
            .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?,
        heap_id: heap
            .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?,
        authority_epoch: epoch
            .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?,
        authority_generation: generation
            .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?,
        certificate_id: cert_id
            .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?,
        holder_public_key: holder,
        rights: rights
            .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?,
        constraints: constraints
            .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?,
        not_before,
        expires_at: expires,
        issuer_master_key_id: issuer
            .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?,
    })
}

fn decode_constraints_value(v: &CborValue) -> Result<Constraints, HeapError> {
    match v {
        CborValue::Array(items) if items.is_empty() => Ok(Constraints::empty()),
        CborValue::Array(_) => {
            // Full constraint decode lands with richer CBOR helpers; empty is the bootstrap case.
            Err(HeapError::unavailable(
                HeapUnavailableCause::MalformedOrBadSignature,
            ))
        }
        _ => Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        )),
    }
}

fn expect_uint(v: &CborValue) -> Result<u64, HeapError> {
    match v {
        CborValue::Uint(u) => Ok(*u),
        _ => Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        )),
    }
}

fn expect_text(v: &CborValue) -> Result<String, HeapError> {
    match v {
        CborValue::Text(s) => Ok(s.clone()),
        _ => Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        )),
    }
}

fn expect_bstr16(v: &CborValue) -> Result<[u8; 16], HeapError> {
    match v {
        CborValue::Bytes(b) if b.len() == 16 => {
            let mut a = [0u8; 16];
            a.copy_from_slice(b);
            Ok(a)
        }
        _ => Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        )),
    }
}

fn expect_bstr32(v: &CborValue) -> Result<[u8; 32], HeapError> {
    match v {
        CborValue::Bytes(b) if b.len() == 32 => {
            let mut a = [0u8; 32];
            a.copy_from_slice(b);
            Ok(a)
        }
        _ => Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        )),
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    fn is_empty(&self) -> bool {
        self.pos >= self.bytes.len()
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], HeapError> {
        if self.pos + n > self.bytes.len() {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::MalformedOrBadSignature,
            ));
        }
        let s = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn read_array_len(&mut self) -> Result<usize, HeapError> {
        let b = self.take(1)?[0];
        if b & 0xe0 != 0x80 {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::MalformedOrBadSignature,
            ));
        }
        decode_len(b & 0x1f, self)
    }
    fn read_bstr(&mut self) -> Result<Vec<u8>, HeapError> {
        let b = self.take(1)?[0];
        if b & 0xe0 != 0x40 {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::MalformedOrBadSignature,
            ));
        }
        let n = decode_len(b & 0x1f, self)?;
        Ok(self.take(n)?.to_vec())
    }
    fn read_raw_map_is_empty(&mut self) -> Result<bool, HeapError> {
        let b = self.take(1)?[0];
        Ok(b == 0xa0)
    }
}

fn decode_len(ai: u8, cur: &mut Cursor<'_>) -> Result<usize, HeapError> {
    match ai {
        n @ 0..=23 => Ok(n as usize),
        24 => Ok(cur.take(1)?[0] as usize),
        25 => {
            let b = cur.take(2)?;
            Ok(u16::from_be_bytes([b[0], b[1]]) as usize)
        }
        _ => Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        )),
    }
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

/// Recompute Sig_structure for tests / second decoder comparison.
pub fn sig_structure_for(protected: &[u8], aad: &[u8], payload: &[u8]) -> Vec<u8> {
    encode_sig_structure(protected, aad, payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn bootstrap_certificate_verifies() {
        let vectors: serde_json::Value =
            serde_json::from_str(include_str!("../spec/vectors-v1.json")).unwrap();
        let master_pk: [u8; 32] = hex(vectors["inputs"]["master_public_key"].as_str().unwrap())
            .try_into()
            .unwrap();
        let cert = vectors["accepted"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["id"] == "cert-bootstrap-v1")
            .unwrap();
        let cose = hex(cert["cose_sign1"].as_str().unwrap());
        let v = verify_certificate(&cose, &master_pk).expect("verify");
        assert_eq!(hex::encode(v.fingerprint), cert["sha256"].as_str().unwrap());
        assert_eq!(v.rights.bits(), 13);
    }
}
