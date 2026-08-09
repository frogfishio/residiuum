//! Channel-bound holder proof verification and client-side construction
//! (`HEAP_SPEC` §31.3).

use crate::certificate::sig_structure_for;
use crate::certificate::VerifiedCertificate;
use crate::error::{HeapError, HeapUnavailableCause};
use crate::ids::{AuthorityEpoch, DeploymentId, HeapId};
use crate::wire::{
    AUDIENCE_DATA_V1, CONTENT_TYPE_HOLDER_PROOF, EXTERNAL_AAD_HOLDER_PROOF, PROFILE_VERSION,
    PROOF_MAX_BYTES,
};
use residiuum_format::{decode_deterministic_uint_map, encode_deterministic_uint_map, CborValue};
use ed25519_dalek::{Signature, VerifyingKey};

/// Verified holder proof claims.
#[derive(Debug, Clone)]
pub struct VerifiedHolderProof {
    /// Proof id bytes.
    pub proof_id: [u8; 16],
    /// created_at.
    pub created_at: u64,
    /// Certificate hash bound into the proof.
    pub certificate_hash: [u8; 32],
    /// Server nonce.
    pub server_nonce: [u8; 32],
    /// TLS exporter.
    pub tls_exporter: [u8; 32],
}

/// Build an untagged COSE Sign1 holder proof for the HeapKey handshake.
///
/// `sign` receives the COSE `Sig_structure` bytes and must return a 64-byte
/// Ed25519 signature. The SDK's `HolderSigner` supplies this without exporting
/// private key material.
pub fn build_holder_proof(
    certificate: &VerifiedCertificate,
    server_nonce: &[u8; 32],
    tls_exporter: &[u8; 32],
    proof_id: [u8; 16],
    created_at: u64,
    sign: impl FnOnce(&[u8]) -> Result<[u8; 64], HeapError>,
) -> Result<Vec<u8>, HeapError> {
    let payload = encode_deterministic_uint_map(&[
        (1u64, CborValue::Uint(PROFILE_VERSION)),
        (2, CborValue::Bytes(proof_id.to_vec())),
        (3, CborValue::Uint(created_at)),
        (4, CborValue::Bytes(certificate.fingerprint.to_vec())),
        (5, CborValue::Bytes(certificate.deployment_id.as_bytes().to_vec())),
        (6, CborValue::Bytes(certificate.heap_id.as_bytes().to_vec())),
        (7, CborValue::Uint(certificate.authority_epoch.get())),
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
    .map_err(|_| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;

    // protected: {1: -8, 3: "application/residiuum-heap-holder-proof-v1"}
    let mut protected = Vec::new();
    protected.push(0xa2);
    protected.push(0x01);
    protected.push(0x27); // -8 as CBOR nint
    protected.push(0x03);
    write_text(&mut protected, CONTENT_TYPE_HOLDER_PROOF);

    let sig_structure = sig_structure_for(&protected, EXTERNAL_AAD_HOLDER_PROOF, &payload);
    let signature = sign(&sig_structure)?;
    if signature.len() != 64 {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }

    let mut cose = Vec::new();
    cose.push(0x84);
    write_bstr(&mut cose, &protected);
    cose.push(0xa0);
    write_bstr(&mut cose, &payload);
    write_bstr(&mut cose, &signature);
    if cose.len() > PROOF_MAX_BYTES {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    Ok(cose)
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

fn write_bstr(out: &mut Vec<u8>, b: &[u8]) {
    if b.len() <= 23 {
        out.push(0x40 | (b.len() as u8));
    } else if b.len() <= 255 {
        out.push(0x58);
        out.push(b.len() as u8);
    } else {
        out.push(0x59);
        out.extend_from_slice(&(b.len() as u16).to_be_bytes());
    }
    out.extend_from_slice(b);
}

/// Verify a holder proof against a previously verified certificate and channel binding.
pub fn verify_holder_proof(
    cose: &[u8],
    certificate: &VerifiedCertificate,
    server_nonce: &[u8; 32],
    tls_exporter: &[u8; 32],
) -> Result<VerifiedHolderProof, HeapError> {
    if cose.len() > PROOF_MAX_BYTES || cose.is_empty() {
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
    let (alg, content_type) = decode_protected(&parts.protected)?;
    if alg != -8 || content_type != CONTENT_TYPE_HOLDER_PROOF {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let sig_structure =
        sig_structure_for(&parts.protected, EXTERNAL_AAD_HOLDER_PROOF, &parts.payload);
    let vk = VerifyingKey::from_bytes(&certificate.holder_public_key)
        .map_err(|_| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;
    let signature = Signature::from_slice(&parts.signature)
        .map_err(|_| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;
    vk.verify_strict(&sig_structure, &signature)
        .map_err(|_| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;

    let map = decode_deterministic_uint_map(&parts.payload)
        .map_err(|_| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;
    if map.len() != 11 {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }

    let mut profile = None;
    let mut proof_id = None;
    let mut created_at = None;
    let mut cert_hash = None;
    let mut deployment = None;
    let mut heap = None;
    let mut epoch = None;
    let mut audience = None;
    let mut nonce = None;
    let mut exporter = None;
    let mut protocol = None;

    for (k, v) in map {
        match k {
            1 => profile = Some(expect_uint(&v)?),
            2 => proof_id = Some(expect_bstr16(&v)?),
            3 => created_at = Some(expect_uint(&v)?),
            4 => cert_hash = Some(expect_bstr32(&v)?),
            5 => deployment = Some(expect_bstr16(&v).and_then(DeploymentId::from_bytes)?),
            6 => heap = Some(expect_bstr16(&v).and_then(HeapId::from_bytes)?),
            7 => epoch = Some(AuthorityEpoch::new(expect_uint(&v)?)?),
            8 => audience = Some(expect_text(&v)?),
            9 => nonce = Some(expect_bstr32(&v)?),
            10 => exporter = Some(expect_bstr32(&v)?),
            11 => protocol = Some(expect_protocol(&v)?),
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
    if audience.as_deref() != Some(AUDIENCE_DATA_V1) {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let cert_hash = cert_hash
        .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;
    if cert_hash != certificate.fingerprint {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    if deployment != Some(certificate.deployment_id)
        || heap != Some(certificate.heap_id)
        || epoch != Some(certificate.authority_epoch)
    {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let nonce = nonce
        .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;
    let exporter = exporter
        .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;
    if &nonce != server_nonce || &exporter != tls_exporter {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    if protocol != Some([1, 1, 0]) {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }

    Ok(VerifiedHolderProof {
        proof_id: proof_id
            .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?,
        created_at: created_at
            .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?,
        certificate_hash: cert_hash,
        server_nonce: nonce,
        tls_exporter: exporter,
    })
}

struct CoseParts {
    protected: Vec<u8>,
    unprotected_empty: bool,
    payload: Vec<u8>,
    signature: Vec<u8>,
}

fn decode_cose_sign1(bytes: &[u8]) -> Result<CoseParts, HeapError> {
    // Reuse certificate decoder shape inline to avoid pub(crate) churn.
    let mut pos = 0;
    let take = |p: &mut usize, n: usize| -> Result<&[u8], HeapError> {
        if *p + n > bytes.len() {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::MalformedOrBadSignature,
            ));
        }
        let s = &bytes[*p..*p + n];
        *p += n;
        Ok(s)
    };
    let hdr = take(&mut pos, 1)?[0];
    if hdr != 0x84 {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let protected = read_bstr(bytes, &mut pos)?;
    if take(&mut pos, 1)?[0] != 0xa0 {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let payload = read_bstr(bytes, &mut pos)?;
    let signature = read_bstr(bytes, &mut pos)?;
    if pos != bytes.len() || signature.len() != 64 {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    Ok(CoseParts {
        protected,
        unprotected_empty: true,
        payload,
        signature,
    })
}

fn read_bstr(bytes: &[u8], pos: &mut usize) -> Result<Vec<u8>, HeapError> {
    if *pos >= bytes.len() {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let b = bytes[*pos];
    *pos += 1;
    if b & 0xe0 != 0x40 {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let ai = b & 0x1f;
    let n = match ai {
        n @ 0..=23 => n as usize,
        24 => {
            let v = bytes.get(*pos).copied().ok_or_else(|| {
                HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature)
            })?;
            *pos += 1;
            v as usize
        }
        25 => {
            if *pos + 2 > bytes.len() {
                return Err(HeapError::unavailable(
                    HeapUnavailableCause::MalformedOrBadSignature,
                ));
            }
            let v = u16::from_be_bytes([bytes[*pos], bytes[*pos + 1]]) as usize;
            *pos += 2;
            v
        }
        _ => {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::MalformedOrBadSignature,
            ))
        }
    };
    if *pos + n > bytes.len() {
        return Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        ));
    }
    let out = bytes[*pos..*pos + n].to_vec();
    *pos += n;
    Ok(out)
}

fn decode_protected(bytes: &[u8]) -> Result<(i64, String), HeapError> {
    let map = decode_deterministic_uint_map(bytes)
        .map_err(|_| HeapError::unavailable(HeapUnavailableCause::MalformedOrBadSignature))?;
    let mut alg = None;
    let mut ct = None;
    for (k, v) in map {
        match k {
            1 => match v {
                CborValue::Nint(n) => alg = Some(-1i64 - (n as i64)),
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
fn expect_protocol(v: &CborValue) -> Result<[u64; 3], HeapError> {
    match v {
        CborValue::Array(items) if items.len() == 3 => Ok([
            expect_uint(&items[0])?,
            expect_uint(&items[1])?,
            expect_uint(&items[2])?,
        ]),
        _ => Err(HeapError::unavailable(
            HeapUnavailableCause::MalformedOrBadSignature,
        )),
    }
}
