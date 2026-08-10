//! HeapKey challenge / auth / welcome session (`HEAP_SPEC` §33 / HP-008).
//!
//! Hot path uses only [`crate::heap_registry::ResidentHeapRegistry`] and
//! `residiuum-heap` verify/decide/mint. It never opens a master authority store.

use crate::heap_registry::ResidentHeapRegistry;
use residiuum_client::{
    b64u_decode, HeapAuth, HeapChallenge, HeapReject, HeapWelcome, HEAP_AUTH_MAX_BYTES,
};
use residiuum_heap::{
    decide, mint_capability, verify_certificate, verify_holder_proof, AuthorizationDecision,
    HeapCap, OperationDescriptor, TrustedInstant, HEAP_PROFILE,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long a challenge nonce remains valid (`HEAP_SPEC` §33.3).
pub const NONCE_TTL: Duration = Duration::from_secs(60);

/// Internal audit cause — never returned on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeapAuthInternalCause {
    /// Encoded size or JSON shape.
    Malformed,
    /// Certificate / proof crypto failure.
    BadMaterial,
    /// Heap not in resident registry.
    UnknownHeap,
    /// Deployment / epoch / generation / issuer mismatch.
    StaleAuthority,
    /// Blacklist / state / rights / constraints.
    Denied,
    /// Nonce already consumed or expired.
    NonceReplayOrExpired,
    /// TLS exporter mismatch.
    ChannelBinding,
    /// `expected_heap_name` mismatch after authority.
    NameMismatch,
}

/// Outcome of processing `heap_auth`.
#[derive(Debug)]
pub enum HeapAuthOutcome {
    /// Welcome + minted capability.
    Welcome {
        /// Wire welcome object.
        welcome: HeapWelcome,
        /// Session capability (not serializable).
        cap: HeapCap,
    },
    /// Uniform reject (wire) plus internal cause (audit only).
    Reject {
        /// Always the uniform reject shape.
        reject: HeapReject,
        /// Audit-only cause.
        cause: HeapAuthInternalCause,
    },
}

/// Pending challenge on one connection.
#[derive(Debug, Clone)]
pub struct PendingChallenge {
    /// Server nonce (32 bytes).
    pub server_nonce: [u8; 32],
    /// When the nonce expires.
    pub deadline: Instant,
    /// Whether the nonce has already been consumed.
    pub consumed: bool,
}

impl PendingChallenge {
    /// Issue a fresh nonce with TTL from `now`.
    pub fn issue(server_nonce: [u8; 32], now: Instant) -> Self {
        Self {
            server_nonce,
            deadline: now + NONCE_TTL,
            consumed: false,
        }
    }

    /// Whether the challenge is still usable.
    pub fn is_live(&self, now: Instant) -> bool {
        !self.consumed && now <= self.deadline
    }
}

/// Build a wire challenge for `deployment_id` (canonical UUID string).
pub fn build_challenge(deployment_id: String, server_nonce: &[u8; 32]) -> HeapChallenge {
    HeapChallenge::new(deployment_id, server_nonce)
}

/// Authenticate a `heap_auth` message against resident slots only.
///
/// `tls_exporter` is the RFC 9266 exporter (or a test stand-in). `now_unix_s`
/// is trusted security time for certificate windows.
pub fn authenticate_heap_auth(
    registry: &ResidentHeapRegistry,
    pending: &mut PendingChallenge,
    auth_json_bytes: &[u8],
    tls_exporter: &[u8; 32],
    now_unix_s: u64,
    wall_now: Instant,
) -> HeapAuthOutcome {
    let reject = |cause: HeapAuthInternalCause| HeapAuthOutcome::Reject {
        reject: HeapReject::uniform(),
        cause,
    };

    if auth_json_bytes.len() > HEAP_AUTH_MAX_BYTES {
        return reject(HeapAuthInternalCause::Malformed);
    }
    if !pending.is_live(wall_now) {
        return reject(HeapAuthInternalCause::NonceReplayOrExpired);
    }
    // Consume before any further work so replay of the same framed body fails.
    pending.consumed = true;

    let auth: HeapAuth = match serde_json::from_slice(auth_json_bytes) {
        Ok(a) => a,
        Err(_) => return reject(HeapAuthInternalCause::Malformed),
    };
    if auth.v != 1 || auth.msg != "heap_auth" {
        return reject(HeapAuthInternalCause::Malformed);
    }

    let cert_bytes = match b64u_decode(&auth.certificate_b64u) {
        Ok(b) => b,
        Err(_) => return reject(HeapAuthInternalCause::Malformed),
    };
    let proof_bytes = match b64u_decode(&auth.holder_proof_b64u) {
        Ok(b) => b,
        Err(_) => return reject(HeapAuthInternalCause::Malformed),
    };

    // Peek certificate heap id via verify with a provisional master key from registry.
    // We need the heap id from the cert payload; verify_certificate needs master PK.
    // Decode path: try verify against each resident master's key is wrong.
    // Correct order per §33.4: parse → resolve slot by cert HeapId → verify with slot master.
    // So we use a lightweight parse: verify_certificate requires master key, so first
    // extract heap_id by attempting verification only after we know which slot — chicken/egg.
    // Solution: call verify once we have a candidate. Spec says resolve by certificate's HeapId.
    // Parse COSE payload without full verify is internal; use verify_certificate after looking
    // up by decoding unverified heap_id from CBOR — but that could leak timing. For HP-008 we
    // extract heap_id from the client-supplied `heap_id` field only as a hint, then verify
    // against that slot; on miss/mismatch still uniform reject. Authority is the cert+slot.

    let hint_heap = match auth.heap_id.parse::<residiuum_heap::HeapId>() {
        Ok(h) => h,
        Err(_) => return reject(HeapAuthInternalCause::Malformed),
    };
    let resident = match registry.get(&hint_heap) {
        Some(r) => r,
        None => return reject(HeapAuthInternalCause::UnknownHeap),
    };
    let snap = resident.slot.load();
    let verified = match verify_certificate(&cert_bytes, &snap.master_public_key) {
        Ok(v) => v,
        Err(_) => return reject(HeapAuthInternalCause::BadMaterial),
    };
    if verified.heap_id != hint_heap || verified.heap_id != snap.heap_id {
        return reject(HeapAuthInternalCause::UnknownHeap);
    }
    if verified.deployment_id != snap.deployment_id {
        return reject(HeapAuthInternalCause::StaleAuthority);
    }

    let now = TrustedInstant { unix_s: now_unix_s };
    // Handshake itself is gated like admitting a connection: use public ping (op 1)
    // as the decide probe for certificate/slot consistency before minting.
    let probe = OperationDescriptor {
        operation_id: 1,
        request_bytes: auth_json_bytes.len() as u64,
    };
    match decide(&snap, &verified, &probe, now) {
        AuthorizationDecision::Allow => {}
        AuthorizationDecision::Deny(_) => return reject(HeapAuthInternalCause::Denied),
    }

    match verify_holder_proof(&proof_bytes, &verified, &pending.server_nonce, tls_exporter) {
        Ok(_proof) => {}
        Err(_) => {
            // Distinguishing exporter vs signature would leak; collapse publicly.
            return reject(HeapAuthInternalCause::BadMaterial);
        }
    }

    if let Some(expected) = auth.expected_heap_name.as_deref() {
        match resident.display_name.as_deref() {
            Some(name) if name == expected => {}
            _ => return reject(HeapAuthInternalCause::NameMismatch),
        }
    }

    let cap = match mint_capability(Arc::clone(&resident.slot), &verified, now) {
        Ok(c) => c,
        Err(_) => return reject(HeapAuthInternalCause::Denied),
    };

    let session_id = hex32(cap.capability_id().as_bytes());
    let welcome = HeapWelcome {
        v: 1,
        msg: "welcome".into(),
        session_id,
        heap_id: verified.heap_id.to_string(),
        authority_epoch: verified.authority_epoch.get(),
        authority_generation: verified.authority_generation.get(),
        security_revision: snap.security_revision.get(),
        capability_expires_at: verified.expires_at,
        heap_profile: HEAP_PROFILE.into(),
    };

    HeapAuthOutcome::Welcome { welcome, cap }
}

fn hex32(bytes: &[u8; 16]) -> String {
    let mut s = String::with_capacity(32);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Collapse any auth failure to the wire reject (for logging helpers).
pub fn wire_reject() -> HeapReject {
    HeapReject::uniform()
}
