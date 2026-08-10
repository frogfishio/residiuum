//! Heap-key handshake wire types (`HEAP_SPEC` §33 / HP-008).
//!
//! Control messages after TLS + optional feature `heap-key-v1`.

use serde::{Deserialize, Serialize};

/// Feature token for HeapKey channel binding (`HEAP_SPEC` §33.2).
pub const FEATURE_HEAP_KEY_V1: &str = "heap-key-v1";

/// Audience constant echoed in challenges.
pub const HEAP_AUDIENCE_DATA_V1: &str = "residiuum:data:v1";

/// Maximum complete framed `heap_auth` JSON bytes (§33.8).
pub const HEAP_AUTH_MAX_BYTES: usize = 32_768;

/// Server → client challenge after hello when `heap-key-v1` is granted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeapChallenge {
    /// Protocol major.
    pub v: u16,
    /// Always `heap_challenge`.
    pub msg: String,
    /// Canonical lowercase hyphenated deployment UUID.
    pub deployment_id: String,
    /// Exactly `residiuum:data:v1`.
    pub audience: String,
    /// Unpadded base64url of 32-byte nonce.
    pub server_nonce_b64u: String,
    /// Heap profile major.
    pub heap_profile: u16,
    /// RPC protocol major.
    pub protocol_major: u16,
    /// RPC protocol minor.
    pub protocol_minor: u16,
}

impl HeapChallenge {
    /// Build a challenge message.
    pub fn new(deployment_id: String, server_nonce: &[u8; 32]) -> Self {
        Self {
            v: 1,
            msg: "heap_challenge".into(),
            deployment_id,
            audience: HEAP_AUDIENCE_DATA_V1.into(),
            server_nonce_b64u: b64u_encode(server_nonce),
            heap_profile: 1,
            protocol_major: 1,
            protocol_minor: 0,
        }
    }
}

/// Client → server HeapKey authentication.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeapAuth {
    /// Protocol major.
    pub v: u16,
    /// Always `heap_auth`.
    pub msg: String,
    /// Canonical heap UUID (informational; authority comes from certificate).
    pub heap_id: String,
    /// COSE certificate bytes, unpadded base64url.
    pub certificate_b64u: String,
    /// COSE holder proof bytes, unpadded base64url.
    pub holder_proof_b64u: String,
    /// Optional human name check after authority succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_heap_name: Option<String>,
}

/// Uniform pre-welcome reject (`HEAP_SPEC` §33.4).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeapReject {
    /// Protocol major.
    pub v: u16,
    /// Always `reject`.
    pub msg: String,
    /// Always `heap_unavailable`.
    pub code: String,
    /// Always false for handshake failures.
    pub retryable: bool,
}

impl HeapReject {
    /// The only allowed handshake reject shape.
    pub fn uniform() -> Self {
        Self {
            v: 1,
            msg: "reject".into(),
            code: "heap_unavailable".into(),
            retryable: false,
        }
    }
}

/// Heap-bound welcome after successful HeapKey validation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeapWelcome {
    /// Protocol major.
    pub v: u16,
    /// Always `welcome`.
    pub msg: String,
    /// Random capability/session id (32 lowercase hex).
    pub session_id: String,
    /// Canonical heap UUID.
    pub heap_id: String,
    /// Authority epoch.
    pub authority_epoch: u64,
    /// Authority generation.
    pub authority_generation: u64,
    /// Security revision.
    pub security_revision: u64,
    /// Capability expiry unix seconds.
    pub capability_expires_at: u64,
    /// Profile string.
    pub heap_profile: String,
}

/// Unpadded base64url encode.
pub fn b64u_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity((bytes.len() + 2) / 3 * 4);
    let mut i = 0;
    while i + 3 <= bytes.len() {
        let n = ((bytes[i] as u32) << 16) | ((bytes[i + 1] as u32) << 8) | (bytes[i + 2] as u32);
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push(TABLE[((n >> 6) & 63) as usize] as char);
        out.push(TABLE[(n & 63) as usize] as char);
        i += 3;
    }
    let rem = bytes.len() - i;
    if rem == 1 {
        let n = (bytes[i] as u32) << 16;
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
    } else if rem == 2 {
        let n = ((bytes[i] as u32) << 16) | ((bytes[i + 1] as u32) << 8);
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push(TABLE[((n >> 6) & 63) as usize] as char);
    }
    out
}

/// Unpadded base64url decode.
pub fn b64u_decode(s: &str) -> Result<Vec<u8>, &'static str> {
    fn val(c: u8) -> Result<u8, &'static str> {
        match c {
            b'A'..=b'Z' => Ok(c - b'A'),
            b'a'..=b'z' => Ok(c - b'a' + 26),
            b'0'..=b'9' => Ok(c - b'0' + 52),
            b'-' => Ok(62),
            b'_' => Ok(63),
            _ => Err("invalid base64url"),
        }
    }
    let bytes = s.as_bytes();
    if bytes.iter().any(|&b| b == b'=') {
        return Err("padded base64url rejected");
    }
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4 + 1);
    let mut i = 0;
    while i + 4 <= bytes.len() {
        let n = ((val(bytes[i])? as u32) << 18)
            | ((val(bytes[i + 1])? as u32) << 12)
            | ((val(bytes[i + 2])? as u32) << 6)
            | (val(bytes[i + 3])? as u32);
        out.push(((n >> 16) & 0xff) as u8);
        out.push(((n >> 8) & 0xff) as u8);
        out.push((n & 0xff) as u8);
        i += 4;
    }
    let rem = bytes.len() - i;
    if rem == 0 {
        return Ok(out);
    }
    if rem == 2 {
        let n = ((val(bytes[i])? as u32) << 18) | ((val(bytes[i + 1])? as u32) << 12);
        out.push(((n >> 16) & 0xff) as u8);
        return Ok(out);
    }
    if rem == 3 {
        let n = ((val(bytes[i])? as u32) << 18)
            | ((val(bytes[i + 1])? as u32) << 12)
            | ((val(bytes[i + 2])? as u32) << 6);
        out.push(((n >> 16) & 0xff) as u8);
        out.push(((n >> 8) & 0xff) as u8);
        return Ok(out);
    }
    Err("invalid base64url length")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b64u_roundtrip() {
        let raw = [0u8, 1, 2, 250, 255, 10];
        let enc = b64u_encode(&raw);
        assert!(!enc.contains('='));
        assert_eq!(b64u_decode(&enc).unwrap(), raw);
    }

    #[test]
    fn uniform_reject_shape() {
        let j = serde_json::to_value(HeapReject::uniform()).unwrap();
        assert_eq!(j["code"], "heap_unavailable");
        assert_eq!(j["retryable"], false);
        assert!(j.get("error").is_none());
        assert!(j.get("heap_id").is_none());
    }
}
