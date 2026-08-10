//! Qualified heap-key session handshake (`HEAP_SPEC` §33.2 / HP-008).
//!
//! Sequence after TLS 1.3:
//! `hello` → `heap_challenge` → `heap_auth` → `welcome` | uniform `reject`.
//!
//! Protocol `welcome` is not sent before HeapKey validation. Token/RBAC is not
//! consulted on this path.

use crate::heap_audit::HeapAuthAuditLog;
use crate::heap_auth::{
    authenticate_heap_auth, build_challenge, HeapAuthInternalCause, HeapAuthOutcome,
    PendingChallenge,
};
use crate::heap_registry::ResidentHeapRegistry;
use residiuum_client::{
    negotiate_qualified_features, parse_handshake, read_frame, write_json_frame, HandshakeMsg,
    HeapReject, HeapWelcome, FEATURE_HEAP_KEY_V1, HANDSHAKE_MAX_FRAME_BYTES, HEAP_AUTH_MAX_BYTES,
    PROTOCOL_MAJOR,
};
use residiuum_heap::HeapCap;
use residiuum_sdk::Error;
use std::io::{BufReader, Read, Write};
use std::sync::Arc;
use std::time::Instant;

/// Established qualified session after HeapKey welcome.
#[derive(Debug)]
pub struct QualifiedSession {
    /// Negotiated max frame (server default when challenge omits it).
    pub max_frame: usize,
    /// Granted features (includes `heap-key-v1`).
    pub features: Vec<String>,
    /// Wire welcome object already written to the client.
    pub welcome: HeapWelcome,
    /// Session capability (not serializable).
    pub cap: HeapCap,
}

/// Outcome of the qualified handshake sequence.
#[derive(Debug)]
pub enum QualifiedHandshakeResult {
    /// Heap-bound session ready for request dispatch.
    Established(QualifiedSession),
    /// Uniform reject already written; connection should close.
    Rejected {
        /// Wire reject (always uniform).
        reject: HeapReject,
        /// Audit-only cause.
        cause: HeapAuthInternalCause,
    },
}

/// Inputs for a qualified handshake (no authority store).
pub struct QualifiedHandshakeParams<'a> {
    /// Resident heap slots.
    pub registry: &'a ResidentHeapRegistry,
    /// Canonical deployment UUID string echoed in challenges.
    pub deployment_id: &'a str,
    /// RFC 9266 exporter (32 bytes) from the TLS connection.
    pub tls_exporter: &'a [u8; 32],
    /// Trusted unix seconds for certificate windows.
    pub now_unix_s: u64,
    /// Optional audit sink.
    pub audit: Option<&'a HeapAuthAuditLog>,
    /// Server nonce (tests may inject; production fills randomly).
    pub server_nonce: Option<[u8; 32]>,
}

/// Run the qualified `hello` → challenge → auth → welcome|reject sequence.
///
/// On reject, writes exactly one uniform reject frame and returns
/// [`QualifiedHandshakeResult::Rejected`]. On success, writes welcome and
/// returns an established session. Never consults token/RBAC or an authority
/// store.
pub fn run_qualified_handshake<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    params: QualifiedHandshakeParams<'_>,
) -> Result<QualifiedHandshakeResult, Error> {
    let hello_payload = match read_frame(reader, HANDSHAKE_MAX_FRAME_BYTES).map_err(Error::from)? {
        Some(p) => p,
        None => {
            return Err(Error::ProtocolViolation(
                "connection closed before protocol hello".into(),
            ));
        }
    };
    let hello = match parse_handshake(&hello_payload) {
        Ok(h) => h,
        Err(e) => {
            let _ = write_json_frame(
                writer,
                &residiuum_client::Handshake::reject("protocol_violation", e.to_string()),
            );
            return Err(Error::from(e));
        }
    };
    if hello.msg != HandshakeMsg::Hello {
        let msg = format!("expected hello, got {:?}", hello.msg);
        let _ = write_json_frame(
            writer,
            &residiuum_client::Handshake::reject("protocol_violation", &msg),
        );
        return Err(Error::ProtocolViolation(msg));
    }
    if hello.v != PROTOCOL_MAJOR && hello.protocol_major.unwrap_or(hello.v) != PROTOCOL_MAJOR {
        let msg = format!(
            "unsupported protocol major {} (server speaks {PROTOCOL_MAJOR})",
            hello.v
        );
        let _ = write_json_frame(
            writer,
            &residiuum_client::Handshake::reject("protocol_version_unsupported", &msg),
        );
        return Err(Error::ProtocolViolation(msg));
    }

    let features = match negotiate_qualified_features(hello.features.as_deref().unwrap_or(&[])) {
        Ok(f) => f,
        Err(e) => {
            let _ = write_json_frame(
                writer,
                &residiuum_client::Handshake::reject("protocol_violation", e.to_string()),
            );
            return Err(Error::from(e));
        }
    };
    debug_assert!(features.iter().any(|f| f == FEATURE_HEAP_KEY_V1));

    let max_frame = residiuum_client::negotiate_max_frame(hello.max_frame);
    let mut nonce = params.server_nonce.unwrap_or([0u8; 32]);
    if params.server_nonce.is_none() {
        getrandom::fill(&mut nonce)
            .map_err(|_| Error::Internal("getrandom failed for heap challenge nonce".into()))?;
    }
    let challenge = build_challenge(params.deployment_id.to_string(), &nonce);
    write_json_frame(writer, &challenge).map_err(Error::from)?;

    let auth_payload = match read_frame(reader, HEAP_AUTH_MAX_BYTES).map_err(Error::from)? {
        Some(p) => p,
        None => {
            return Err(Error::ProtocolViolation(
                "connection closed before heap_auth".into(),
            ));
        }
    };

    let mut pending = PendingChallenge::issue(nonce, Instant::now());
    let outcome = authenticate_heap_auth(
        params.registry,
        &mut pending,
        &auth_payload,
        params.tls_exporter,
        params.now_unix_s,
        Instant::now(),
    );

    match outcome {
        HeapAuthOutcome::Welcome { welcome, cap } => {
            if let Some(audit) = params.audit {
                audit.record_welcome(&welcome.heap_id);
            }
            write_json_frame(writer, &welcome).map_err(Error::from)?;
            Ok(QualifiedHandshakeResult::Established(QualifiedSession {
                max_frame,
                features,
                welcome,
                cap,
            }))
        }
        HeapAuthOutcome::Reject { reject, cause } => {
            if let Some(audit) = params.audit {
                audit.record_reject(cause);
            }
            write_json_frame(writer, &reject).map_err(Error::from)?;
            Ok(QualifiedHandshakeResult::Rejected { reject, cause })
        }
    }
}

/// Same as [`run_qualified_handshake`], but reads/writes through one buffered duplex
/// (the production accept-loop shape).
pub fn run_qualified_handshake_buffered<S: Read + Write>(
    reader: &mut BufReader<S>,
    params: QualifiedHandshakeParams<'_>,
) -> Result<QualifiedHandshakeResult, Error> {
    // Split borrows: read via BufReader, write via inner stream.
    // We carefully only hold one mutable borrow at a time.
    let hello_payload = match read_frame(reader, HANDSHAKE_MAX_FRAME_BYTES).map_err(Error::from)? {
        Some(p) => p,
        None => {
            return Err(Error::ProtocolViolation(
                "connection closed before protocol hello".into(),
            ));
        }
    };
    let hello = match parse_handshake(&hello_payload) {
        Ok(h) => h,
        Err(e) => {
            let _ = write_json_frame(
                reader.get_mut(),
                &residiuum_client::Handshake::reject("protocol_violation", e.to_string()),
            );
            return Err(Error::from(e));
        }
    };
    if hello.msg != HandshakeMsg::Hello {
        let msg = format!("expected hello, got {:?}", hello.msg);
        let _ = write_json_frame(
            reader.get_mut(),
            &residiuum_client::Handshake::reject("protocol_violation", &msg),
        );
        return Err(Error::ProtocolViolation(msg));
    }
    if hello.v != PROTOCOL_MAJOR && hello.protocol_major.unwrap_or(hello.v) != PROTOCOL_MAJOR {
        let msg = format!(
            "unsupported protocol major {} (server speaks {PROTOCOL_MAJOR})",
            hello.v
        );
        let _ = write_json_frame(
            reader.get_mut(),
            &residiuum_client::Handshake::reject("protocol_version_unsupported", &msg),
        );
        return Err(Error::ProtocolViolation(msg));
    }

    let features = match negotiate_qualified_features(hello.features.as_deref().unwrap_or(&[])) {
        Ok(f) => f,
        Err(e) => {
            let _ = write_json_frame(
                reader.get_mut(),
                &residiuum_client::Handshake::reject("protocol_violation", e.to_string()),
            );
            return Err(Error::from(e));
        }
    };
    debug_assert!(features.iter().any(|f| f == FEATURE_HEAP_KEY_V1));

    let max_frame = residiuum_client::negotiate_max_frame(hello.max_frame);
    let mut nonce = params.server_nonce.unwrap_or([0u8; 32]);
    if params.server_nonce.is_none() {
        getrandom::fill(&mut nonce)
            .map_err(|_| Error::Internal("getrandom failed for heap challenge nonce".into()))?;
    }
    let challenge = build_challenge(params.deployment_id.to_string(), &nonce);
    write_json_frame(reader.get_mut(), &challenge).map_err(Error::from)?;

    let auth_payload = match read_frame(reader, HEAP_AUTH_MAX_BYTES).map_err(Error::from)? {
        Some(p) => p,
        None => {
            return Err(Error::ProtocolViolation(
                "connection closed before heap_auth".into(),
            ));
        }
    };

    let mut pending = PendingChallenge::issue(nonce, Instant::now());
    let outcome = authenticate_heap_auth(
        params.registry,
        &mut pending,
        &auth_payload,
        params.tls_exporter,
        params.now_unix_s,
        Instant::now(),
    );

    match outcome {
        HeapAuthOutcome::Welcome { welcome, cap } => {
            if let Some(audit) = params.audit {
                audit.record_welcome(&welcome.heap_id);
            }
            write_json_frame(reader.get_mut(), &welcome).map_err(Error::from)?;
            Ok(QualifiedHandshakeResult::Established(QualifiedSession {
                max_frame,
                features,
                welcome,
                cap,
            }))
        }
        HeapAuthOutcome::Reject { reject, cause } => {
            if let Some(audit) = params.audit {
                audit.record_reject(cause);
            }
            write_json_frame(reader.get_mut(), &reject).map_err(Error::from)?;
            Ok(QualifiedHandshakeResult::Rejected { reject, cause })
        }
    }
}

/// Serve qualified application frames using the session capability.
///
/// Does **not** consult token/RBAC or an authority store. Returns when the
/// peer closes or an idle/fatal transport error occurs.
///
/// When `host` is provided, §32.4 data ops (collection open / get / put / delete)
/// are dispatched through a capability-gated [`residiuum_store::HeapStore`].
pub fn serve_qualified_requests<S: Read + Write>(
    reader: &mut BufReader<S>,
    session: &QualifiedSession,
) -> Result<(), Error> {
    serve_qualified_requests_with_host(reader, session, None)
}

/// Like [`serve_qualified_requests`], with optional store host for data ops.
pub fn serve_qualified_requests_with_host<S: Read + Write>(
    reader: &mut BufReader<S>,
    session: &QualifiedSession,
    host: Option<&residiuum_store::StoreHost>,
) -> Result<(), Error> {
    use crate::heap_dispatch::{
        dispatch_heap_request, dispatch_heap_request_with, layout_for_root, HeapDataCtx,
        HeapDispatchResult,
    };
    use residiuum_client::write_json_frame;

    let heap_store = host.map(|h| h.open_heap(session.cap.clone()));
    let layout = host.map(|h| layout_for_root(h.root()));

    loop {
        let raw = match read_frame(reader, session.max_frame).map_err(Error::from)? {
            Some(p) => p,
            None => break,
        };
        let outcome = match (heap_store.as_ref(), layout.as_ref()) {
            (Some(store), Some(layout)) => {
                dispatch_heap_request_with(&session.cap, &raw, Some(HeapDataCtx { store, layout }))
            }
            _ => dispatch_heap_request(&session.cap, &raw),
        };
        match outcome {
            HeapDispatchResult::Response(resp) => {
                write_json_frame(reader.get_mut(), &resp).map_err(Error::from)?;
            }
        }
    }
    Ok(())
}

/// Validate ServeOptions constraints for the qualified listener.
pub fn validate_qualified_listener(
    tls_enabled: bool,
    auth_token: Option<&str>,
    diagnostic_line: bool,
    registry: Option<&Arc<ResidentHeapRegistry>>,
    deployment_id: Option<&str>,
) -> Result<(), Error> {
    if !tls_enabled {
        return Err(Error::ValidationMsg(
            "qualified heap-key listener requires TLS 1.3".into(),
        ));
    }
    if auth_token.is_some() {
        return Err(Error::ValidationMsg(
            "qualified heap-key listener forbids shared token auth".into(),
        ));
    }
    if diagnostic_line {
        return Err(Error::ValidationMsg(
            "qualified heap-key listener forbids diagnostic line protocol".into(),
        ));
    }
    if registry.is_none() {
        return Err(Error::ValidationMsg(
            "qualified heap-key listener requires a resident heap registry".into(),
        ));
    }
    match deployment_id {
        Some(id) if !id.is_empty() => Ok(()),
        _ => Err(Error::ValidationMsg(
            "qualified heap-key listener requires deployment_id".into(),
        )),
    }
}
