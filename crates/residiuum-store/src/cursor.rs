//! Bounded-memory live scan cursors (DEF-026).
//!
//! # Consistency model (`residiuum-cursor-v1`)
//!
//! Pages are ordered by **subject bytes ascending**. A cursor is bound to a
//! **scan generation** computed at first page open:
//!
//! ```text
//! generation = BLAKE3(store_id || segment_fingerprint || live_count_le)
//! ```
//!
//! Subsequent pages must present a token whose generation still matches.
//! Concurrent durable/memory mutations that change the live set size or
//! segment fingerprint invalidate the token ([`StoreError::CursorStale`]).
//!
//! This is **read-committed paging with generation fencing**, not MVCC
//! snapshot isolation. Under the declared model, a successful multi-page scan
//! that never sees `CursorStale` yields each live subject at most once and
//! omits no subject that remained live for the whole scan.
//!
//! # Tokens (DEF-097)
//!
//! Continuation tokens are opaque bytes. Integrity is a BLAKE3 keyed MAC whose
//! key is derived from **store-local secret entropy** ([`ContinuationKeyring`]),
//! not from the public `store_id` alone. Knowledge of store id + token bytes is
//! insufficient to forge or modify a valid token.
//!
//! Wire profile: `residiuum-cursor-v2` (magic `RCSR0002`) embeds `key_generation_id`
//! so rotation can accept the previous generation during grace.

use crate::error::StoreError;
use crate::store::{IncompleteReason, LiveIncomplete, LiveLogicalScan};
use crate::token_keys::ContinuationKeyring;
use blake3::Hasher;

/// Profile tag for cursor / continuation token encoding (DEF-097 secret keys).
pub const CURSOR_PROFILE: &str = "residiuum-cursor-v2";

/// Domain separation for store cursor MAC keys.
const MAC_DOMAIN: &[u8] = b"residiuum-cursor-v2-mac\0";

/// Default page size when the caller does not specify one.
pub const DEFAULT_PAGE_SIZE: usize = 64;

/// Hard cap on page size (and on continuation token subject/prefix lengths).
pub const MAX_PAGE_SIZE: usize = 4096;

/// Hard cap on token size (prefix + after subject + headers + MAC).
pub const MAX_TOKEN_BYTES: usize = 8192;

const TOKEN_MAGIC: &[u8; 8] = b"RCSR0002";
const MAC_LEN: usize = 16;
const GEN_LEN: usize = 32;

/// Options for one page of a live logical scan.
#[derive(Debug, Clone)]
pub struct LiveScanPageOptions {
    /// Maximum **complete** entries to return in this page (default 64).
    pub page_size: usize,
    /// Optional subject-byte prefix (collection scope).
    pub prefix: Option<Vec<u8>>,
    /// Opaque continuation from a previous page (`None` = start).
    pub continuation: Option<Vec<u8>>,
}

impl Default for LiveScanPageOptions {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            prefix: None,
            continuation: None,
        }
    }
}

impl LiveScanPageOptions {
    /// Start a new scan with the given page size.
    pub fn new(page_size: usize) -> Self {
        Self {
            page_size: page_size.clamp(1, MAX_PAGE_SIZE),
            prefix: None,
            continuation: None,
        }
    }

    /// Restrict to subjects with this byte prefix.
    pub fn prefix(mut self, prefix: impl Into<Vec<u8>>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Resume from a prior continuation token.
    pub fn continuation(mut self, token: impl Into<Vec<u8>>) -> Self {
        self.continuation = Some(token.into());
        self
    }
}

/// One page of live logical payloads (DEF-026).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveScanPage {
    /// Fully reassembled live entries in subject order (at most `page_size`).
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Live subjects observed on this page that could not be fully read.
    pub incomplete: Vec<LiveIncomplete>,
    /// True when no further pages remain **and** no incomplete subjects were
    /// seen on this page **and** tier coverage is complete.
    ///
    /// For multi-page scans, overall completeness requires every page's
    /// `incomplete` empty, final page `has_more == false`, and no tier holes.
    pub complete: bool,
    /// Offline / unmounted tiers or unavailable segments.
    pub tier_coverage_incomplete: bool,
    /// True when a further page may be fetched with `continuation`.
    pub has_more: bool,
    /// Opaque token for the next page (`None` when `has_more` is false).
    pub continuation: Option<Vec<u8>>,
    /// Subjects examined on this page (complete + incomplete), for budgets.
    pub examined: usize,
}

/// Why key-bearing coverage cannot be proven complete (DEF-100).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoverageGapKind {
    /// Offline / unmounted tier or unavailable segment media.
    TierUnavailable,
}

/// One typed coverage gap (no Heap secrets or unauthorized subjects).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageGap {
    /// Stable gap class.
    pub kind: CoverageGapKind,
    /// Operator-safe diagnostic detail.
    pub detail: String,
}

/// One page of live **keys only** — no body reassembly (DEF-100).
///
/// Body damage never suppresses a verified key. `coverage_complete` is true
/// only when key-bearing authority is fully available for the scan range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyScanPage {
    /// Live subject keys in ascending order (at most `page_size`).
    pub keys: Vec<Vec<u8>>,
    /// Opaque token for the next page (`None` when `has_more` is false).
    pub continuation: Option<Vec<u8>>,
    /// True when a further page may be fetched with `continuation`.
    pub has_more: bool,
    /// True only when no further pages remain **and** key coverage has no gaps.
    pub coverage_complete: bool,
    /// Typed reasons key coverage is incomplete (empty when complete).
    pub coverage_gaps: Vec<CoverageGap>,
    /// Subjects examined on this page (equals `keys.len()` for key-only path).
    pub examined: usize,
    /// Offline / unmounted tiers or unavailable segments.
    pub tier_coverage_incomplete: bool,
}

/// One page of live documents with per-key body outcomes (DEF-100).
///
/// Healthy rows stream alongside incomplete/undecodable keys; a single bad body
/// does not abort the page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentScanPage {
    /// Fully reassembled (subject, body) pairs.
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    /// Verified keys whose body is partial/unavailable/conflicting.
    pub incomplete: Vec<LiveIncomplete>,
    /// True when no further pages remain **and** key coverage has no gaps
    /// (body incompletes do not alone falsify key coverage).
    pub key_coverage_complete: bool,
    /// Typed key-coverage gaps.
    pub coverage_gaps: Vec<CoverageGap>,
    /// Offline / unmounted tiers.
    pub tier_coverage_incomplete: bool,
    /// True when a further page may be fetched with `continuation`.
    pub has_more: bool,
    /// Opaque token for the next page.
    pub continuation: Option<Vec<u8>>,
    /// Subjects examined (rows + incomplete).
    pub examined: usize,
    /// Body bytes examined for complete rows on this page.
    pub bytes_examined: u64,
}

impl LiveScanPage {
    /// Convert a single full-set page into the legacy full-scan envelope shape.
    pub fn into_full_scan(self) -> LiveLogicalScan {
        LiveLogicalScan {
            entries: self.entries,
            incomplete: self.incomplete,
            complete: self.complete && !self.has_more,
            tier_coverage_incomplete: self.tier_coverage_incomplete,
        }
    }
}

/// Internal decoded continuation (not public API).
#[derive(Debug, Clone)]
pub(crate) struct CursorState {
    pub generation: [u8; GEN_LEN],
    pub prefix: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
    pub page_size: usize,
}

/// Compute the scan generation fence for the current store state.
pub fn scan_generation(
    store_id: &[u8; 16],
    segment_fingerprint: &[u8; 32],
    live_count: u64,
) -> [u8; GEN_LEN] {
    let mut h = Hasher::new();
    h.update(b"residiuum-cursor-v1-gen\0");
    h.update(store_id);
    h.update(segment_fingerprint);
    h.update(&live_count.to_le_bytes());
    *h.finalize().as_bytes()
}

/// Encode a continuation token for `store_id` using the active key generation.
pub(crate) fn encode_token(
    store_id: &[u8; 16],
    keyring: &ContinuationKeyring,
    state: &CursorState,
) -> Result<Vec<u8>, StoreError> {
    let prefix = state.prefix.as_deref().unwrap_or(&[]);
    let after = state.after.as_deref().unwrap_or(&[]);
    if prefix.len() > MAX_TOKEN_BYTES || after.len() > MAX_TOKEN_BYTES {
        return Err(StoreError::CursorInvalid(
            "prefix or after subject exceeds token size budget".into(),
        ));
    }
    let key_gen = keyring.active_generation_id();
    let mut body =
        Vec::with_capacity(8 + 16 + 4 + GEN_LEN + 4 + 4 + 4 + prefix.len() + after.len());
    body.extend_from_slice(TOKEN_MAGIC);
    body.extend_from_slice(store_id);
    body.extend_from_slice(&key_gen.to_le_bytes());
    body.extend_from_slice(&state.generation);
    body.extend_from_slice(&(state.page_size as u32).to_le_bytes());
    body.extend_from_slice(&(prefix.len() as u32).to_le_bytes());
    body.extend_from_slice(prefix);
    body.extend_from_slice(&(after.len() as u32).to_le_bytes());
    body.extend_from_slice(after);
    if body.len() + MAC_LEN > MAX_TOKEN_BYTES {
        return Err(StoreError::CursorInvalid(
            "continuation token would exceed size budget".into(),
        ));
    }
    let key = keyring.active_mac_key(MAC_DOMAIN, store_id);
    let tag = blake3::keyed_hash(&key, &body);
    body.extend_from_slice(&tag.as_bytes()[..MAC_LEN]);
    Ok(body)
}

/// Authenticate a continuation token and discard internal state (DEF-091-F fuzz).
///
/// Returns `Ok(())` when the MAC and shape verify for this store keyring.
pub fn verify_continuation_token(
    store_id: &[u8; 16],
    keyring: &ContinuationKeyring,
    token: &[u8],
) -> Result<(), StoreError> {
    decode_token(store_id, keyring, token).map(|_| ())
}

/// Decode and authenticate a continuation token with the store keyring.
pub(crate) fn decode_token(
    store_id: &[u8; 16],
    keyring: &ContinuationKeyring,
    token: &[u8],
) -> Result<CursorState, StoreError> {
    if token.len() < 8 + 16 + 4 + GEN_LEN + 4 + 4 + 4 + MAC_LEN || token.len() > MAX_TOKEN_BYTES {
        return Err(StoreError::CursorInvalid(
            "continuation token length out of range".into(),
        ));
    }
    let (payload, mac) = token.split_at(token.len() - MAC_LEN);
    if &payload[..8] != TOKEN_MAGIC.as_slice() {
        return Err(StoreError::CursorInvalid(
            "continuation token magic/version mismatch (need residiuum-cursor-v2)".into(),
        ));
    }
    let tok_store = &payload[8..24];
    if tok_store != store_id.as_slice() {
        return Err(StoreError::CursorInvalid(
            "continuation token store_id mismatch".into(),
        ));
    }
    let key_gen = u32::from_le_bytes(payload[24..28].try_into().unwrap());
    let Some(key) = keyring.mac_key_for(key_gen, MAC_DOMAIN, store_id) else {
        return Err(StoreError::CursorInvalid(
            "continuation token key generation retired or unknown".into(),
        ));
    };
    let expected = blake3::keyed_hash(&key, payload);
    if !constant_time_eq(mac, &expected.as_bytes()[..MAC_LEN]) {
        return Err(StoreError::CursorInvalid(
            "continuation token MAC mismatch (tampered, wrong store, or forged)".into(),
        ));
    }
    let mut generation = [0u8; GEN_LEN];
    generation.copy_from_slice(&payload[28..28 + GEN_LEN]);
    let mut o = 28 + GEN_LEN;
    let page_size = u32::from_le_bytes(payload[o..o + 4].try_into().unwrap()) as usize;
    o += 4;
    if page_size == 0 || page_size > MAX_PAGE_SIZE {
        return Err(StoreError::CursorInvalid(
            "continuation token page_size invalid".into(),
        ));
    }
    let plen = u32::from_le_bytes(payload[o..o + 4].try_into().unwrap()) as usize;
    o += 4;
    if o + plen + 4 > payload.len() {
        return Err(StoreError::CursorInvalid(
            "continuation token prefix truncated".into(),
        ));
    }
    let prefix = if plen == 0 {
        None
    } else {
        Some(payload[o..o + plen].to_vec())
    };
    o += plen;
    let alen = u32::from_le_bytes(payload[o..o + 4].try_into().unwrap()) as usize;
    o += 4;
    if o + alen != payload.len() {
        return Err(StoreError::CursorInvalid(
            "continuation token after-subject length mismatch".into(),
        ));
    }
    let after = if alen == 0 {
        None
    } else {
        Some(payload[o..o + alen].to_vec())
    };
    Ok(CursorState {
        generation,
        prefix,
        after,
        page_size,
    })
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut v = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        v |= x ^ y;
    }
    v == 0
}

/// Build incomplete reason helpers used by the store page path.
pub(crate) fn incomplete(subject: Vec<u8>, reason: IncompleteReason) -> LiveIncomplete {
    LiveIncomplete { subject, reason }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token_keys::ContinuationKeyring;

    #[test]
    fn token_roundtrip_and_mac() {
        let store_id = [7u8; 16];
        let keyring = ContinuationKeyring::mint_new().unwrap();
        let state = CursorState {
            generation: [9u8; 32],
            prefix: Some(b"coll/".to_vec()),
            after: Some(b"coll/key-10".to_vec()),
            page_size: 32,
        };
        let tok = encode_token(&store_id, &keyring, &state).unwrap();
        let dec = decode_token(&store_id, &keyring, &tok).unwrap();
        assert_eq!(dec.generation, state.generation);
        assert_eq!(dec.prefix, state.prefix);
        assert_eq!(dec.after, state.after);
        assert_eq!(dec.page_size, 32);

        // Tamper
        let mut bad = tok.clone();
        let last = bad.len() - 1;
        bad[last] ^= 0xff;
        assert!(matches!(
            decode_token(&store_id, &keyring, &bad),
            Err(StoreError::CursorInvalid(_))
        ));

        // Wrong store id (same keyring material would still fail store_id check
        // after MAC if we swapped — use different keyring for forge attempt).
        let other = [8u8; 16];
        assert!(matches!(
            decode_token(&other, &keyring, &tok),
            Err(StoreError::CursorInvalid(_))
        ));
    }

    #[test]
    fn public_store_id_cannot_forge_mac() {
        // Attacker knows store_id and can re-derive the OLD public-only key
        // scheme; with secret keyring, that material must not verify.
        let store_id = [3u8; 16];
        let keyring = ContinuationKeyring::mint_new().unwrap();
        let state = CursorState {
            generation: [1u8; 32],
            prefix: None,
            after: Some(b"k".to_vec()),
            page_size: 8,
        };
        let tok = encode_token(&store_id, &keyring, &state).unwrap();
        // Second independent keyring (attacker cannot mint store secret).
        let attacker = ContinuationKeyring::mint_new().unwrap();
        assert!(matches!(
            decode_token(&store_id, &attacker, &tok),
            Err(StoreError::CursorInvalid(_))
        ));
    }

    #[test]
    fn rotation_grace_then_retire() {
        let store_id = [1u8; 16];
        let mut keyring = ContinuationKeyring::mint_new().unwrap();
        let state = CursorState {
            generation: [2u8; 32],
            prefix: None,
            after: Some(b"a".to_vec()),
            page_size: 4,
        };
        let tok_old = encode_token(&store_id, &keyring, &state).unwrap();
        keyring.rotate().unwrap();
        // Previous generation still verifies during grace.
        decode_token(&store_id, &keyring, &tok_old).unwrap();
        let tok_new = encode_token(&store_id, &keyring, &state).unwrap();
        decode_token(&store_id, &keyring, &tok_new).unwrap();
        keyring.retire_previous();
        assert!(matches!(
            decode_token(&store_id, &keyring, &tok_old),
            Err(StoreError::CursorInvalid(_))
        ));
        decode_token(&store_id, &keyring, &tok_new).unwrap();
    }

    #[test]
    fn generation_changes_with_inputs() {
        let a = scan_generation(&[1u8; 16], &[2u8; 32], 10);
        let b = scan_generation(&[1u8; 16], &[2u8; 32], 11);
        let c = scan_generation(&[1u8; 16], &[3u8; 32], 10);
        assert_ne!(a, b);
        assert_ne!(a, c);
    }
}
