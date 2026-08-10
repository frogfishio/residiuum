//! Authenticated query continuation (`residiuum-cursor-v1`) — APP-6 slice.
//!
//! Normative: CORE plan §11; `spec/app/v1/cursor_vectors_v1.json`.
//!
//! This module freezes **mint / verify** for opaque [`Continuation`] tokens with
//! domain-separated BLAKE3 keyed MACs.
//!
//! **APB-7 T10:** product cursors use an installable [`CursorKeyRing`] (not
//! vector-lock-only) and bind a canonical [`parameter_hash`] into mint/verify.
//! Heap-confined durable secret storage remains a residual (CORE §11).
//!
//! Test / vector key material uses
//! [`CURSOR_KEY_MATERIAL_PROFILE`] when no product ring is installed.

use crate::app_v1::{Continuation, CURSOR_PROFILE};
use crate::error::Error;
use serde_json::{json, Map, Value as JsonValue};
use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Domain separation for parameter binding hashes (APB-7 T10).
pub const PARAMETER_HASH_DOMAIN: &str = "residiuum:cursor-v1:parameter-hash-v1";

/// Cursor profile label (Class C; matches [`CURSOR_PROFILE`]).
pub const PROFILE: &str = CURSOR_PROFILE;

/// Domain separation for MAC (BLAKE3 keyed over domain || 0x00 || canonical body).
pub const MAC_DOMAIN: &str = "residiuum:cursor-v1:mac-v1";

/// Key material profile id for vector lock and tests (not product secret storage).
pub const CURSOR_KEY_MATERIAL_PROFILE: &str = "residiuum-cursor-key-material-v1";

/// Token TTL seconds after issue (CORE §11).
pub const TTL_SECONDS: u64 = 900;

/// Accepted clock skew seconds.
pub const SKEW_SECONDS: u64 = 120;

/// Maximum age from issue that may still be accepted (TTL + skew).
pub const MAX_ACCEPT_AGE_SECONDS: u64 = 1_020;

/// Seed label used to derive the vector-lock key for `ck-0001`.
pub const VECTOR_LOCK_SEED: &str = "vector-lock-seed-v1";

/// Logical fields inside a `residiuum-cursor-v1` token (before authentication).
#[derive(Debug, Clone, PartialEq)]
pub struct CursorLogical {
    /// Profile id.
    pub cursor_profile: String,
    /// Signing key id.
    pub key_id: String,
    /// Bound Heap id (UUID text).
    pub heap_id: String,
    /// Bound collection id (UUID text).
    pub collection_id: String,
    /// Authority epoch fence.
    pub authority_epoch: u64,
    /// Canonical plan hash (64 hex chars).
    pub plan_hash: String,
    /// Canonical parameter hash (64 hex chars).
    pub parameter_hash: String,
    /// Normalized order terms (opaque JSON array).
    pub order_normalized: JsonValue,
    /// Last sort tuple from prior page (opaque JSON array).
    pub last_sort_tuple: JsonValue,
    /// Source frontier evidence (opaque JSON object).
    pub source_frontier: JsonValue,
    /// Remaining total limit across pages.
    pub remaining_limit: u64,
    /// Effective page size.
    pub page_size: u32,
    /// Coverage mode string.
    pub coverage_mode: String,
    /// Consistency mode string.
    pub consistency_mode: String,
    /// Issue time (unix seconds).
    pub issued_at: u64,
    /// Expiry time (unix seconds).
    pub expires_at: u64,
}

/// Binding context required before a token may execute a next page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyContext {
    /// Expected Heap id (UUID text).
    pub heap_id: String,
    /// Expected collection id (UUID text).
    pub collection_id: String,
    /// Expected plan hash when set (64 hex).
    pub plan_hash: Option<String>,
    /// Expected parameter hash when set (64 hex).
    pub parameter_hash: Option<String>,
}

/// One MAC key in the verification window.
#[derive(Debug, Clone)]
pub struct CursorKey {
    /// Public key id carried in the token.
    pub key_id: String,
    /// 32-byte secret (never log / export).
    pub secret: [u8; 32],
}

/// Current + previous keys (CORE §11 rotation window).
#[derive(Debug, Clone)]
pub struct CursorKeyRing {
    /// Active signing key.
    pub current: CursorKey,
    /// Previous key still accepted (optional).
    pub previous: Option<CursorKey>,
}

impl CursorKeyRing {
    /// Vector-lock ring: single key id `ck-0001` derived from fixed seed labels.
    ///
    /// Used only for fixtures/tests — not production secret storage.
    pub fn vector_lock() -> Self {
        Self {
            current: CursorKey {
                key_id: "ck-0001".into(),
                secret: derive_vector_lock_key("ck-0001"),
            },
            previous: None,
        }
    }

    /// Product / host-provided secret ring (APB-7 T10).
    ///
    /// `secret` MUST NOT be derived only from public identifiers. This type
    /// never logs the secret. Durable Heap-confined storage is residual.
    pub fn product(key_id: impl Into<String>, secret: [u8; 32]) -> Self {
        Self {
            current: CursorKey {
                key_id: key_id.into(),
                secret,
            },
            previous: None,
        }
    }

    /// Product ring with previous key still accepted (rotation window).
    pub fn product_with_previous(
        current_key_id: impl Into<String>,
        current_secret: [u8; 32],
        previous_key_id: impl Into<String>,
        previous_secret: [u8; 32],
    ) -> Self {
        Self {
            current: CursorKey {
                key_id: current_key_id.into(),
                secret: current_secret,
            },
            previous: Some(CursorKey {
                key_id: previous_key_id.into(),
                secret: previous_secret,
            }),
        }
    }

    fn lookup(&self, key_id: &str) -> Option<&CursorKey> {
        if self.current.key_id == key_id {
            return Some(&self.current);
        }
        self.previous.as_ref().filter(|k| k.key_id == key_id)
    }
}

/// Derive the fixed vector-lock secret for `key_id`.
pub fn derive_vector_lock_key(key_id: &str) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(CURSOR_KEY_MATERIAL_PROFILE.as_bytes());
    h.update(&[0u8]);
    h.update(key_id.as_bytes());
    h.update(&[0u8]);
    h.update(VECTOR_LOCK_SEED.as_bytes());
    *h.finalize().as_bytes()
}

/// Canonical parameter hash bound into cursor mint/verify (64 hex chars).
///
/// Empty map hashes to a stable digest (not the all-zero placeholder).
pub fn parameter_hash(params: &BTreeMap<String, JsonValue>) -> String {
    let body = serde_json::to_vec(params).unwrap_or_else(|_| b"{}".to_vec());
    let mut h = blake3::Hasher::new();
    h.update(PARAMETER_HASH_DOMAIN.as_bytes());
    h.update(&[0u8]);
    h.update(&body);
    hex32(h.finalize().as_bytes())
}

fn active_ring_slot() -> &'static Mutex<CursorKeyRing> {
    static SLOT: OnceLock<Mutex<CursorKeyRing>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(CursorKeyRing::vector_lock()))
}

/// Install the process-local cursor key ring used by page mint/verify.
///
/// Returns the previous ring (for restore in tests).
pub fn install_cursor_key_ring(ring: CursorKeyRing) -> CursorKeyRing {
    let mut guard = active_ring_slot().lock().unwrap_or_else(|e| e.into_inner());
    std::mem::replace(&mut *guard, ring)
}

/// Clone of the active process-local cursor key ring.
pub fn active_cursor_key_ring() -> CursorKeyRing {
    active_ring_slot()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

/// RAII restore of the previous cursor key ring on drop.
pub struct CursorKeyRingGuard {
    previous: Option<CursorKeyRing>,
}

impl CursorKeyRingGuard {
    /// Install `ring` until this guard is dropped.
    pub fn install(ring: CursorKeyRing) -> Self {
        Self {
            previous: Some(install_cursor_key_ring(ring)),
        }
    }
}

impl Drop for CursorKeyRingGuard {
    fn drop(&mut self) {
        if let Some(prev) = self.previous.take() {
            let _ = install_cursor_key_ring(prev);
        }
    }
}

impl CursorLogical {
    /// Parse logical token JSON (with or without `mac`).
    pub fn from_json(v: &JsonValue) -> Result<Self, Error> {
        let obj = v
            .as_object()
            .ok_or_else(|| Error::QueryInvalid("cursor token must be object".into()))?;
        let get_str = |k: &str| -> Result<String, Error> {
            obj.get(k)
                .and_then(|x| x.as_str())
                .map(|s| s.to_string())
                .ok_or_else(|| Error::QueryInvalid(format!("cursor missing string field `{k}`")))
        };
        let get_u64 = |k: &str| -> Result<u64, Error> {
            obj.get(k)
                .and_then(|x| x.as_u64())
                .ok_or_else(|| Error::QueryInvalid(format!("cursor missing u64 field `{k}`")))
        };
        let profile = get_str("cursor_profile")?;
        if profile != PROFILE {
            return Err(Error::ConsistencyViolation(format!(
                "cursor profile mismatch: {profile}"
            )));
        }
        Ok(Self {
            cursor_profile: profile,
            key_id: get_str("key_id")?,
            heap_id: get_str("heap_id")?,
            collection_id: get_str("collection_id")?,
            authority_epoch: get_u64("authority_epoch")?,
            plan_hash: get_str("plan_hash")?,
            parameter_hash: get_str("parameter_hash")?,
            order_normalized: obj
                .get("order_normalized")
                .cloned()
                .ok_or_else(|| Error::QueryInvalid("cursor missing order_normalized".into()))?,
            last_sort_tuple: obj
                .get("last_sort_tuple")
                .cloned()
                .ok_or_else(|| Error::QueryInvalid("cursor missing last_sort_tuple".into()))?,
            source_frontier: obj
                .get("source_frontier")
                .cloned()
                .ok_or_else(|| Error::QueryInvalid("cursor missing source_frontier".into()))?,
            remaining_limit: get_u64("remaining_limit")?,
            page_size: get_u64("page_size")? as u32,
            coverage_mode: get_str("coverage_mode")?,
            consistency_mode: get_str("consistency_mode")?,
            issued_at: get_u64("issued_at")?,
            expires_at: get_u64("expires_at")?,
        })
    }

    /// Canonical JSON object used for MAC (no `mac` field; stable key order).
    pub fn to_canonical_json(&self) -> JsonValue {
        let mut m = BTreeMap::new();
        m.insert("authority_epoch".into(), json!(self.authority_epoch));
        m.insert("collection_id".into(), json!(self.collection_id));
        m.insert("consistency_mode".into(), json!(self.consistency_mode));
        m.insert("coverage_mode".into(), json!(self.coverage_mode));
        m.insert("cursor_profile".into(), json!(self.cursor_profile));
        m.insert("expires_at".into(), json!(self.expires_at));
        m.insert("heap_id".into(), json!(self.heap_id));
        m.insert("issued_at".into(), json!(self.issued_at));
        m.insert("key_id".into(), json!(self.key_id));
        m.insert("last_sort_tuple".into(), self.last_sort_tuple.clone());
        m.insert("order_normalized".into(), self.order_normalized.clone());
        m.insert("page_size".into(), json!(self.page_size));
        m.insert("parameter_hash".into(), json!(self.parameter_hash));
        m.insert("plan_hash".into(), json!(self.plan_hash));
        m.insert("remaining_limit".into(), json!(self.remaining_limit));
        m.insert("source_frontier".into(), self.source_frontier.clone());
        btree_to_obj(m)
    }

    /// Canonical body bytes for MAC.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&self.to_canonical_json()).expect("cursor canonical json")
    }

    /// Compute MAC hex (64 chars) for this logical body under `secret`.
    pub fn mac_hex(&self, secret: &[u8; 32]) -> String {
        hex32(&mac_bytes(secret, &self.canonical_bytes()))
    }

    /// Full token JSON including `mac`.
    pub fn to_token_json(&self, mac_hex: &str) -> JsonValue {
        let mut obj = match self.to_canonical_json() {
            JsonValue::Object(m) => m,
            _ => Map::new(),
        };
        obj.insert("mac".into(), json!(mac_hex));
        JsonValue::Object(obj)
    }
}

/// Mint a [`Continuation`] for `logical` using the ring's current key.
///
/// Sets `key_id` from the current key. Caller supplies issue/expiry timestamps.
pub fn mint(logical: &CursorLogical, ring: &CursorKeyRing) -> Result<Continuation, Error> {
    let mut body = logical.clone();
    body.key_id = ring.current.key_id.clone();
    if body.cursor_profile != PROFILE {
        body.cursor_profile = PROFILE.into();
    }
    let mac = body.mac_hex(&ring.current.secret);
    let token = serde_json::to_vec(&body.to_token_json(&mac))
        .map_err(|e| Error::Internal(format!("cursor encode: {e}")))?;
    Ok(Continuation { token })
}

/// Mint with `issued_at = now` and `expires_at = now + TTL_SECONDS`.
pub fn mint_now(logical: &CursorLogical, ring: &CursorKeyRing) -> Result<Continuation, Error> {
    let now = unix_now()?;
    let mut body = logical.clone();
    body.issued_at = now;
    body.expires_at = now.saturating_add(TTL_SECONDS);
    mint(&body, ring)
}

/// Verify a continuation token against binding context and key ring.
///
/// Fail-closed: profile, MAC, Heap/collection binding, optional plan/parameter
/// hashes, and expiry / max-accept-age.
pub fn verify(
    token: &[u8],
    ctx: &VerifyContext,
    ring: &CursorKeyRing,
    now_unix: u64,
) -> Result<CursorLogical, Error> {
    let v: JsonValue = serde_json::from_slice(token)
        .map_err(|e| Error::ConsistencyViolation(format!("cursor parse: {e}")))?;
    let mac_hex = v
        .get("mac")
        .and_then(|m| m.as_str())
        .ok_or_else(|| Error::ConsistencyViolation("cursor missing mac".into()))?
        .to_string();
    let logical = CursorLogical::from_json(&v)?;

    // Expiry fence (CORE §11).
    if now_unix.saturating_add(SKEW_SECONDS) < logical.issued_at {
        return Err(Error::ConsistencyViolation(
            "cursor issued_at is in the future beyond skew".into(),
        ));
    }
    if now_unix > logical.issued_at.saturating_add(MAX_ACCEPT_AGE_SECONDS) {
        return Err(Error::ConsistencyViolation(
            "cursor exceeded max accept age".into(),
        ));
    }
    if now_unix > logical.expires_at.saturating_add(SKEW_SECONDS) {
        return Err(Error::ConsistencyViolation("cursor expired".into()));
    }

    // Binding fences.
    if logical.heap_id != ctx.heap_id {
        return Err(Error::ConsistencyViolation(
            "cursor heap_id mismatch".into(),
        ));
    }
    if logical.collection_id != ctx.collection_id {
        return Err(Error::ConsistencyViolation(
            "cursor collection_id mismatch".into(),
        ));
    }
    if let Some(ref ph) = ctx.plan_hash {
        if &logical.plan_hash != ph {
            return Err(Error::ConsistencyViolation(
                "cursor plan_hash mismatch".into(),
            ));
        }
    }
    if let Some(ref ph) = ctx.parameter_hash {
        if &logical.parameter_hash != ph {
            return Err(Error::ConsistencyViolation(
                "cursor parameter_hash mismatch".into(),
            ));
        }
    }

    let key = ring.lookup(&logical.key_id).ok_or_else(|| {
        Error::ConsistencyViolation(format!("cursor unknown key_id {}", logical.key_id))
    })?;
    let expect = logical.mac_hex(&key.secret);
    if !constant_time_eq_hex(&expect, &mac_hex) {
        return Err(Error::ConsistencyViolation("cursor mac invalid".into()));
    }
    Ok(logical)
}

fn mac_bytes(secret: &[u8; 32], body: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new_keyed(secret);
    h.update(MAC_DOMAIN.as_bytes());
    h.update(&[0u8]);
    h.update(body);
    *h.finalize().as_bytes()
}

fn hex32(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn constant_time_eq_hex(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let (aa, bb) = (a.as_bytes(), b.as_bytes());
    let mut diff = 0u8;
    for i in 0..aa.len() {
        diff |= aa[i] ^ bb[i];
    }
    diff == 0
}

fn btree_to_obj(m: BTreeMap<String, JsonValue>) -> JsonValue {
    JsonValue::Object(m.into_iter().collect())
}

fn unix_now() -> Result<u64, Error> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|e| Error::Internal(format!("clock: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_logical() -> CursorLogical {
        CursorLogical {
            cursor_profile: PROFILE.into(),
            key_id: "ck-0001".into(),
            heap_id: "00000000-0000-4000-8000-0000000000b1".into(),
            collection_id: "00000000-0000-4000-8000-0000000000a1".into(),
            authority_epoch: 1,
            plan_hash: "bb".repeat(32),
            parameter_hash: "cc".repeat(32),
            order_normalized: json!([
                {"path": ["created_at"], "dir": "desc"},
                {"path": ["$key"], "dir": "asc", "tie_break": true}
            ]),
            last_sort_tuple: json!([1785360000, "ord-1"]),
            source_frontier: json!({"generation": 7, "segment_id": "dd".repeat(16)}),
            remaining_limit: 900,
            page_size: 100,
            coverage_mode: "complete".into(),
            consistency_mode: "available".into(),
            issued_at: 1_785_360_100,
            expires_at: 1_785_361_000,
        }
    }

    #[test]
    fn mint_verify_roundtrip() {
        let ring = CursorKeyRing::vector_lock();
        let logical = sample_logical();
        let cont = mint(&logical, &ring).unwrap();
        let ctx = VerifyContext {
            heap_id: logical.heap_id.clone(),
            collection_id: logical.collection_id.clone(),
            plan_hash: Some(logical.plan_hash.clone()),
            parameter_hash: Some(logical.parameter_hash.clone()),
        };
        let got = verify(&cont.token, &ctx, &ring, logical.issued_at + 10).unwrap();
        assert_eq!(got.heap_id, logical.heap_id);
        assert_eq!(got.remaining_limit, 900);
    }

    #[test]
    fn heap_mismatch_fails() {
        let ring = CursorKeyRing::vector_lock();
        let logical = sample_logical();
        let cont = mint(&logical, &ring).unwrap();
        let ctx = VerifyContext {
            heap_id: "00000000-0000-4000-8000-0000000000ff".into(),
            collection_id: logical.collection_id.clone(),
            plan_hash: None,
            parameter_hash: None,
        };
        let err = verify(&cont.token, &ctx, &ring, logical.issued_at + 10).unwrap_err();
        assert_eq!(err.code(), crate::error::ErrorCode::ConsistencyViolation);
    }

    #[test]
    fn expired_fails() {
        let ring = CursorKeyRing::vector_lock();
        let mut logical = sample_logical();
        logical.issued_at = 0;
        logical.expires_at = 1;
        let cont = mint(&logical, &ring).unwrap();
        let ctx = VerifyContext {
            heap_id: logical.heap_id.clone(),
            collection_id: logical.collection_id.clone(),
            plan_hash: None,
            parameter_hash: None,
        };
        let err = verify(&cont.token, &ctx, &ring, 10_000).unwrap_err();
        assert_eq!(err.code(), crate::error::ErrorCode::ConsistencyViolation);
    }

    #[test]
    fn tampered_mac_fails() {
        let ring = CursorKeyRing::vector_lock();
        let logical = sample_logical();
        let cont = mint(&logical, &ring).unwrap();
        let mut v: JsonValue = serde_json::from_slice(&cont.token).unwrap();
        v.as_object_mut()
            .unwrap()
            .insert("mac".into(), json!("00".repeat(32)));
        let bad = serde_json::to_vec(&v).unwrap();
        let ctx = VerifyContext {
            heap_id: logical.heap_id.clone(),
            collection_id: logical.collection_id.clone(),
            plan_hash: None,
            parameter_hash: None,
        };
        let err = verify(&bad, &ctx, &ring, logical.issued_at + 10).unwrap_err();
        assert_eq!(err.code(), crate::error::ErrorCode::ConsistencyViolation);
    }
}
