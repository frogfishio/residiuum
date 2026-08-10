//! Versioned network RPC protocol (DEF-031).
//!
//! Separates **transport framing** from **application encoding**:
//!
//! - **Frame:** big-endian `u32` length prefix + UTF-8 JSON payload.
//! - **Handshake:** first framed messages negotiate protocol major, max frame
//!   size, and feature tokens before any application RPC.
//! - **Application:** existing [`crate::remote::RpcRequest`] /
//!   [`crate::remote::RpcResponse`] JSON objects ride inside frames.
//!
//! The legacy newline-delimited JSON path is retained only as an explicit
//! **diagnostic** profile (`diagnostic_line_protocol` on connect/serve options)
//! for human debugging with tools like `nc`. Production clients and servers
//! require the framed handshake.
//!
//! Profile tag: [`PROTOCOL_PROFILE`]. Draft interoperability label:
//! [`RPC_WIRE_LABEL`] (`1.0-draft`).

use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

/// Default maximum application frame size (matches historical host RPC ceiling).
pub const DEFAULT_MAX_RPC_LINE_BYTES: usize = 16 * 1024 * 1024;

/// Profile tag for the framed RPC protocol (DEF-031).
pub const PROTOCOL_PROFILE: &str = "residiuum-rpc-v1";

/// Server runtime profile advertised on welcome (wire-stable string).
pub const DEFAULT_SERVER_PROFILE: &str = "residiuum-server-v1";

/// Draft network RPC interoperability label (not a freeze).
pub const RPC_WIRE_LABEL: &str = "1.0-draft";

/// Negotiated protocol major version for this build.
pub const PROTOCOL_MAJOR: u16 = 1;

/// Negotiated protocol minor version for this build.
pub const PROTOCOL_MINOR: u16 = 0;

/// Maximum frame size accepted during handshake before negotiation completes.
///
/// Prevents unbounded allocation from a malicious first length prefix.
pub const HANDSHAKE_MAX_FRAME_BYTES: usize = 64 * 1024;

/// Default negotiated maximum application frame size (matches host RPC ceiling).
pub const DEFAULT_MAX_FRAME_BYTES: usize = DEFAULT_MAX_RPC_LINE_BYTES;

/// Feature: length-prefixed JSON application RPCs (this document).
pub const FEATURE_JSON_RPC_V1: &str = "json-rpc-v1";

/// Feature: write/delete receipts must include the required field set.
pub const FEATURE_RECEIPTS_V1: &str = "receipts-v1";

/// Feature: client-supplied `operation_id` for mutation idempotency (DEF-010).
pub const FEATURE_IDEMPOTENCY_V1: &str = "idempotency-v1";

/// Features this build always offers and requires for a successful handshake.
pub const REQUIRED_FEATURES: &[&str] = &[
    FEATURE_JSON_RPC_V1,
    FEATURE_RECEIPTS_V1,
    FEATURE_IDEMPOTENCY_V1,
];

/// Receipt fields the client must treat as mandatory under `receipts-v1`
/// (aligns with DEF-014 fail-closed parsing).
pub const REQUIRED_WRITE_RECEIPT_FIELDS: &[&str] = &[
    "committed",
    "acknowledgement",
    "event_id",
    "version",
    "store_id",
    "segment_id",
];

/// Delete receipts additionally require `removed`.
pub const REQUIRED_DELETE_RECEIPT_FIELDS: &[&str] = &[
    "committed",
    "acknowledgement",
    "event_id",
    "version",
    "store_id",
    "segment_id",
    "removed",
];

/// Handshake / control message kinds on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandshakeMsg {
    /// Client → server first message.
    Hello,
    /// Server → client success.
    Welcome,
    /// Server → client (or unsolicited overload) rejection.
    Reject,
}

/// Framed handshake envelope (control plane; not an application RPC).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Handshake {
    /// Protocol major the sender speaks / offers.
    pub v: u16,
    /// Message kind.
    pub msg: HandshakeMsg,
    /// Sender's maximum acceptable frame size (bytes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_frame: Option<u32>,
    /// Feature tokens offered (hello) or granted (welcome).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub features: Option<Vec<String>>,
    /// Protocol minor (welcome).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_minor: Option<u16>,
    /// Protocol major echoed on welcome/reject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_major: Option<u16>,
    /// Required receipt field names under `receipts-v1` (welcome).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_receipt_fields: Option<Vec<String>>,
    /// Server runtime profile tag (welcome).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_profile: Option<String>,
    /// Protocol profile tag (welcome).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_profile: Option<String>,
    /// Draft wire label (welcome).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire_label: Option<String>,
    /// Error message on reject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Stable error code on reject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

impl Handshake {
    /// Client hello with this build's defaults.
    pub fn hello() -> Self {
        Self {
            v: PROTOCOL_MAJOR,
            msg: HandshakeMsg::Hello,
            max_frame: Some(DEFAULT_MAX_FRAME_BYTES as u32),
            features: Some(REQUIRED_FEATURES.iter().map(|s| (*s).to_string()).collect()),
            protocol_minor: Some(PROTOCOL_MINOR),
            protocol_major: Some(PROTOCOL_MAJOR),
            required_receipt_fields: None,
            server_profile: None,
            protocol_profile: Some(PROTOCOL_PROFILE.into()),
            wire_label: Some(RPC_WIRE_LABEL.into()),
            error: None,
            code: None,
        }
    }

    /// Server welcome after successful negotiation.
    pub fn welcome(negotiated_max_frame: usize, features: Vec<String>) -> Self {
        Self {
            v: PROTOCOL_MAJOR,
            msg: HandshakeMsg::Welcome,
            max_frame: Some(negotiated_max_frame as u32),
            features: Some(features),
            protocol_minor: Some(PROTOCOL_MINOR),
            protocol_major: Some(PROTOCOL_MAJOR),
            required_receipt_fields: Some(
                REQUIRED_WRITE_RECEIPT_FIELDS
                    .iter()
                    .map(|s| (*s).to_string())
                    .collect(),
            ),
            server_profile: Some(DEFAULT_SERVER_PROFILE.into()),
            protocol_profile: Some(PROTOCOL_PROFILE.into()),
            wire_label: Some(RPC_WIRE_LABEL.into()),
            error: None,
            code: None,
        }
    }

    /// Rejection (version mismatch, missing features, overload, etc.).
    pub fn reject(code: &str, error: impl Into<String>) -> Self {
        Self {
            v: PROTOCOL_MAJOR,
            msg: HandshakeMsg::Reject,
            max_frame: None,
            features: None,
            protocol_minor: Some(PROTOCOL_MINOR),
            protocol_major: Some(PROTOCOL_MAJOR),
            required_receipt_fields: None,
            server_profile: None,
            protocol_profile: Some(PROTOCOL_PROFILE.into()),
            wire_label: Some(RPC_WIRE_LABEL.into()),
            error: Some(error.into()),
            code: Some(code.into()),
        }
    }
}

/// Result of a successful handshake (client or server view).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedSession {
    /// Agreed maximum frame payload size in bytes.
    pub max_frame: usize,
    /// Granted feature tokens.
    pub features: Vec<String>,
    /// Protocol major in use.
    pub protocol_major: u16,
    /// Protocol minor in use.
    pub protocol_minor: u16,
}

impl NegotiatedSession {
    /// Whether a feature token was granted.
    pub fn has_feature(&self, name: &str) -> bool {
        self.features.iter().any(|f| f == name)
    }
}

/// Encode a handshake or application payload as a length-prefixed frame.
pub fn encode_frame(payload: &[u8]) -> Result<Vec<u8>, Error> {
    if payload.len() > u32::MAX as usize {
        return Err(Error::ProtocolViolation(format!(
            "frame payload {} exceeds u32 length",
            payload.len()
        )));
    }
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

/// Write one length-prefixed frame to `w`.
pub fn write_frame<W: Write>(w: &mut W, payload: &[u8]) -> Result<(), Error> {
    let frame = encode_frame(payload)?;
    w.write_all(&frame).map_err(Error::from_io)?;
    w.flush().map_err(Error::from_io)?;
    Ok(())
}

/// Write a JSON-serializable value as one framed message.
pub fn write_json_frame<W: Write, T: Serialize>(w: &mut W, value: &T) -> Result<(), Error> {
    let bytes = serde_json::to_vec(value).map_err(|e| Error::Internal(e.to_string()))?;
    write_frame(w, &bytes)
}

/// Read exactly `n` bytes into `buf` (must already be sized).
fn read_exact_n<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<(), Error> {
    r.read_exact(buf).map_err(Error::from_io)
}

/// Read one length-prefixed frame, refusing lengths above `max_frame` **before**
/// allocating the payload buffer (DEF-031).
///
/// Returns `Ok(None)` on clean EOF before any length bytes. A truncated length
/// prefix or payload is a protocol violation.
pub fn read_frame<R: Read>(r: &mut R, max_frame: usize) -> Result<Option<Vec<u8>>, Error> {
    let mut len_buf = [0u8; 4];
    let mut filled = 0usize;
    while filled < 4 {
        match r.read(&mut len_buf[filled..]) {
            Ok(0) if filled == 0 => return Ok(None),
            Ok(0) => {
                return Err(Error::ProtocolViolation(
                    "truncated frame length prefix".into(),
                ));
            }
            Ok(n) => filled += n,
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                return Err(Error::from_io(e));
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(Error::from_io(e)),
        }
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_frame {
        return Err(Error::ResourceLimit(format!(
            "frame length {len} exceeds max_frame {max_frame}; refused before allocation"
        )));
    }
    let mut payload = vec![0u8; len];
    if len > 0 {
        read_exact_n(r, &mut payload)?;
    }
    Ok(Some(payload))
}

/// Like [`read_frame`], but treats a leading `{` as a legacy line-protocol probe
/// and returns a clear protocol-violation error without unbounded allocation.
///
/// Used on the server before handshake so old clients fail clearly (DEF-031).
pub fn read_frame_or_detect_legacy<R: Read>(
    r: &mut R,
    max_frame: usize,
) -> Result<Option<Vec<u8>>, Error> {
    let mut first = [0u8; 1];
    match r.read_exact(&mut first) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(Error::from_io(e)),
    }
    if first[0] == b'{' {
        // Drain a little of the line so we do not leave the peer hanging forever,
        // but cap the discard.
        let mut discard = [0u8; 256];
        let _ = r.read(&mut discard);
        return Err(Error::ProtocolViolation(
            "legacy line-delimited JSON is not accepted on the production profile; \
             send a framed hello (residiuum-rpc-v1) or enable diagnostic_line_protocol \
             on both client and server"
                .into(),
        ));
    }
    // Reconstruct the 4-byte length prefix.
    let mut len_buf = [0u8; 4];
    len_buf[0] = first[0];
    read_exact_n(r, &mut len_buf[1..])?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_frame {
        return Err(Error::ResourceLimit(format!(
            "frame length {len} exceeds max_frame {max_frame}; refused before allocation"
        )));
    }
    let mut payload = vec![0u8; len];
    if len > 0 {
        read_exact_n(r, &mut payload)?;
    }
    Ok(Some(payload))
}

/// Parse a handshake JSON payload.
pub fn parse_handshake(bytes: &[u8]) -> Result<Handshake, Error> {
    serde_json::from_slice(bytes)
        .map_err(|e| Error::ProtocolViolation(format!("invalid handshake JSON: {e}")))
}

/// Intersect client-offered features with server-required features.
///
/// Returns `Ok(granted)` when every required feature is present on the client;
/// otherwise `Err` with a protocol violation detailing the gap.
pub fn negotiate_features(client_features: &[String]) -> Result<Vec<String>, Error> {
    let mut granted = Vec::new();
    for req in REQUIRED_FEATURES {
        if client_features.iter().any(|f| f == *req) {
            granted.push((*req).to_string());
        } else {
            return Err(Error::ProtocolViolation(format!(
                "client hello missing required feature `{req}`; supported: {:?}",
                REQUIRED_FEATURES
            )));
        }
    }
    Ok(granted)
}

/// Negotiate required features, then grant optional features the client offers.
pub fn negotiate_features_with_optional(
    client_features: &[String],
    optional: &[&str],
) -> Result<Vec<String>, Error> {
    let mut granted = negotiate_features(client_features)?;
    for opt in optional {
        if client_features.iter().any(|f| f == *opt) && !granted.iter().any(|g| g == *opt) {
            granted.push((*opt).to_string());
        }
    }
    Ok(granted)
}

/// Feature set for the qualified remote profile (`HEAP_SPEC` §33.2).
///
/// Requires the base RPC features plus `heap-key-v1`.
pub fn negotiate_qualified_features(client_features: &[String]) -> Result<Vec<String>, Error> {
    negotiate_features_with_optional(client_features, &[crate::FEATURE_HEAP_KEY_V1]).and_then(
        |granted| {
            if granted.iter().any(|f| f == crate::FEATURE_HEAP_KEY_V1) {
                Ok(granted)
            } else {
                Err(Error::ProtocolViolation(format!(
                    "client hello missing required feature `{}`",
                    crate::FEATURE_HEAP_KEY_V1
                )))
            }
        },
    )
}

/// Negotiate max frame: min(client offer, DEFAULT).
pub fn negotiate_max_frame(client_offer: Option<u32>) -> usize {
    let client = client_offer
        .map(|n| n as usize)
        .unwrap_or(DEFAULT_MAX_FRAME_BYTES)
        .max(1024); // floor so tiny offers cannot break control messages
    client.min(DEFAULT_MAX_FRAME_BYTES)
}

/// Server-side: read hello, validate, write welcome (or reject). Returns session.
pub fn server_handshake<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<NegotiatedSession, Error> {
    let payload = match read_frame_or_detect_legacy(reader, HANDSHAKE_MAX_FRAME_BYTES)? {
        Some(p) => p,
        None => {
            return Err(Error::ProtocolViolation(
                "connection closed before protocol hello".into(),
            ));
        }
    };
    let hello = match parse_handshake(&payload) {
        Ok(h) => h,
        Err(e) => {
            let _ = write_json_frame(
                writer,
                &Handshake::reject("protocol_violation", e.to_string()),
            );
            return Err(e);
        }
    };
    if hello.msg != HandshakeMsg::Hello {
        let msg = format!("expected hello, got {:?}", hello.msg);
        let _ = write_json_frame(writer, &Handshake::reject("protocol_violation", &msg));
        return Err(Error::ProtocolViolation(msg));
    }
    if hello.v != PROTOCOL_MAJOR && hello.protocol_major.unwrap_or(hello.v) != PROTOCOL_MAJOR {
        let msg = format!(
            "unsupported protocol major {} (server speaks {PROTOCOL_MAJOR})",
            hello.v
        );
        let _ = write_json_frame(
            writer,
            &Handshake::reject("protocol_version_unsupported", &msg),
        );
        return Err(Error::ProtocolViolation(msg));
    }
    let features = match negotiate_features(hello.features.as_deref().unwrap_or(&[])) {
        Ok(f) => f,
        Err(e) => {
            let _ = write_json_frame(
                writer,
                &Handshake::reject("protocol_version_unsupported", e.to_string()),
            );
            return Err(e);
        }
    };
    let max_frame = negotiate_max_frame(hello.max_frame);
    let welcome = Handshake::welcome(max_frame, features.clone());
    write_json_frame(writer, &welcome)?;
    Ok(NegotiatedSession {
        max_frame,
        features,
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
    })
}

/// Client-side: write hello, read welcome (or reject). Returns session.
pub fn client_handshake<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<NegotiatedSession, Error> {
    write_json_frame(writer, &Handshake::hello())?;
    let payload = match read_frame(reader, HANDSHAKE_MAX_FRAME_BYTES)? {
        Some(p) => p,
        None => {
            return Err(Error::ProtocolViolation(
                "server closed connection during handshake".into(),
            ));
        }
    };
    let hs = parse_handshake(&payload)?;
    match hs.msg {
        HandshakeMsg::Welcome => {
            let max_frame = negotiate_max_frame(hs.max_frame);
            let features = hs.features.unwrap_or_default();
            // Client also requires the documented feature set.
            negotiate_features(&features)?;
            Ok(NegotiatedSession {
                max_frame,
                features,
                protocol_major: hs.protocol_major.unwrap_or(hs.v),
                protocol_minor: hs.protocol_minor.unwrap_or(0),
            })
        }
        HandshakeMsg::Reject => {
            let code = hs.code.unwrap_or_else(|| "protocol_violation".into());
            let message = hs
                .error
                .unwrap_or_else(|| "server rejected protocol handshake".into());
            Err(match code.as_str() {
                "resource_limit" => Error::ResourceLimit(message),
                "authentication_failed" => Error::AuthenticationFailed(message),
                "protocol_version_unsupported" | "protocol_violation" => {
                    Error::ProtocolViolation(message)
                }
                _ => Error::Remote { code, message },
            })
        }
        HandshakeMsg::Hello => Err(Error::ProtocolViolation(
            "server sent hello; expected welcome or reject".into(),
        )),
    }
}

/// Write an unsolicited framed reject (overload / drain before worker admit).
pub fn write_reject_frame<W: Write>(w: &mut W, code: &str, error: &str) -> Result<(), Error> {
    write_json_frame(w, &Handshake::reject(code, error))
}

/// Decode a golden fixture path relative to the crate (tests).
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn protocol_constants() {
        assert_eq!(PROTOCOL_PROFILE, "residiuum-rpc-v1");
        assert_eq!(RPC_WIRE_LABEL, "1.0-draft");
        assert_eq!(PROTOCOL_MAJOR, 1);
        assert!(REQUIRED_FEATURES.contains(&FEATURE_JSON_RPC_V1));
        assert!(REQUIRED_WRITE_RECEIPT_FIELDS.contains(&"event_id"));
    }

    #[test]
    fn frame_roundtrip() {
        let mut buf = Vec::new();
        write_frame(&mut buf, b"{\"ok\":true}").unwrap();
        let mut cur = Cursor::new(buf);
        let got = read_frame(&mut cur, 1024).unwrap().unwrap();
        assert_eq!(got, b"{\"ok\":true}");
    }

    #[test]
    fn oversized_frame_refused_before_alloc() {
        let mut bad = Vec::new();
        // Claim 100 MiB payload.
        bad.extend_from_slice(&(100u32 * 1024 * 1024).to_be_bytes());
        // No payload bytes follow — must fail on length check, not hang.
        let mut cur = Cursor::new(bad);
        let err = read_frame(&mut cur, 1024).unwrap_err();
        assert_eq!(err.code(), crate::error::ErrorCode::ResourceLimit);
        assert!(err.to_string().contains("refused before allocation"));
    }

    #[test]
    fn legacy_brace_detected() {
        let mut cur = Cursor::new(b"{\"id\":1,\"op\":\"ping\"}\n".as_slice());
        let err = read_frame_or_detect_legacy(&mut cur, 1024).unwrap_err();
        assert_eq!(err.code(), crate::error::ErrorCode::ProtocolViolation);
        assert!(err.to_string().contains("legacy"));
    }

    #[test]
    fn handshake_client_server() {
        // Server and client over shared buffers is awkward; simulate sequentially.
        let mut client_out = Vec::new();
        write_json_frame(&mut client_out, &Handshake::hello()).unwrap();

        let mut server_in = Cursor::new(client_out);
        let mut server_out = Vec::new();
        let session = server_handshake(&mut server_in, &mut server_out).unwrap();
        assert_eq!(session.protocol_major, 1);
        assert!(session.has_feature(FEATURE_RECEIPTS_V1));

        let mut client_in = Cursor::new(server_out);
        // Client already wrote hello; only read welcome side:
        let payload = read_frame(&mut client_in, HANDSHAKE_MAX_FRAME_BYTES)
            .unwrap()
            .unwrap();
        let hs = parse_handshake(&payload).unwrap();
        assert_eq!(hs.msg, HandshakeMsg::Welcome);
        assert_eq!(hs.protocol_profile.as_deref(), Some(PROTOCOL_PROFILE));
    }

    #[test]
    fn missing_feature_rejected() {
        let hello = Handshake {
            features: Some(vec![FEATURE_JSON_RPC_V1.into()]), // incomplete
            ..Handshake::hello()
        };
        let mut client_out = Vec::new();
        write_json_frame(&mut client_out, &hello).unwrap();
        let mut server_in = Cursor::new(client_out);
        let mut server_out = Vec::new();
        let err = server_handshake(&mut server_in, &mut server_out).unwrap_err();
        assert_eq!(err.code(), crate::error::ErrorCode::ProtocolViolation);
        let reject = read_frame(&mut Cursor::new(server_out), HANDSHAKE_MAX_FRAME_BYTES)
            .unwrap()
            .unwrap();
        let hs = parse_handshake(&reject).unwrap();
        assert_eq!(hs.msg, HandshakeMsg::Reject);
    }

    #[test]
    fn encode_frame_prefix_is_be_u32() {
        let f = encode_frame(b"ab").unwrap();
        assert_eq!(&f[..4], &2u32.to_be_bytes());
        assert_eq!(&f[4..], b"ab");
    }
}
