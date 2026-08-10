//! Qualified remote heap connection (`HEAP_SPEC` §7.1 / §30.9 / HP-007 residual).
//!
//! `Residiuum::connect_heap` performs TLS 1.3 + HeapKey handshake
//! (`hello` → `heap_challenge` → `heap_auth` → `welcome`) and returns a
//! [`RemoteHeap`] session bound to one `HeapId`. Active process ops 1–3 plus
//! §32.4 data (105–106/110–112/114–118/120–122) and secondary indexes (130–133).
//! Op **118** `rql_query` is active (APP-7 T6).

use crate::error::Error;
use crate::remote::{parse_residiuum_url, DEFAULT_PORT};
use crate::tls::{client_connect, IoStream, TlsClientOptions};
use residiuum_client::{
    b64u_decode, b64u_encode, read_frame, write_json_frame, Handshake, HeapAuth, HeapChallenge,
    HeapReject, HeapWelcome, FEATURE_HEAP_KEY_V1, HANDSHAKE_MAX_FRAME_BYTES, HEAP_AUTH_MAX_BYTES,
};
use residiuum_heap::{
    build_holder_proof, inspect_certificate, HeapId, VerifiedCertificate, HEAP_PROFILE,
};
use serde_json::{Map, Value};
use std::net::{TcpStream, ToSocketAddrs};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// One page from remote op **115** `scan_json` (DEF-SCAN-001).
///
/// Required wire fields (T8 / blocker #4): `incomplete`, `coverage_complete`,
/// `has_more`, `exhausted`, `next_after_key` (plus `rows`).
///
/// Invariants enforced on parse:
/// - `exhausted == !has_more`
/// - `has_more` ⇒ `next_after_key` is `Some`
/// - `!has_more` ⇒ `next_after_key` is `None`
///
/// `next_after_key` is the last **examined** collection key (complete or hole).
/// Do not resume from the last successful row alone.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanJsonWirePage {
    /// Fully resolved JSON rows on this page.
    pub rows: Vec<(String, Value)>,
    /// Per-key holes with reason (+ optional locator diagnostics). Required field.
    pub incomplete: Vec<Value>,
    /// True only when `incomplete` is empty for examined subjects.
    pub coverage_complete: bool,
    /// More subjects may exist after this page.
    pub has_more: bool,
    /// No further subjects after this page (`== !has_more`).
    pub exhausted: bool,
    /// Resume cursor when [`Self::has_more`] — last examined key, not last row.
    pub next_after_key: Option<String>,
}

impl ScanJsonWirePage {
    /// Alias for [`Self::coverage_complete`] (historical name).
    pub fn scan_complete(&self) -> bool {
        self.coverage_complete
    }
}

/// One page from remote op **116** `find` (DEF-SCAN-001 blocker #5).
///
/// Required: `rows`, `incomplete`, `coverage_complete`. Index-path and scan-path
/// materialization must both surface holes — never rows-only when incomplete.
#[derive(Debug, Clone, PartialEq)]
pub struct FindWirePage {
    /// Matching complete JSON rows.
    pub rows: Vec<(String, Value)>,
    /// Holes from index candidates and/or scan subjects.
    pub incomplete: Vec<Value>,
    /// True only when `incomplete` is empty.
    pub coverage_complete: bool,
}

/// Parse and validate op 116 `find` result (DEF-SCAN-001 T9).
pub fn parse_find_wire(result: &Value) -> Result<FindWirePage, Error> {
    let obj = result
        .as_object()
        .ok_or_else(|| Error::ProtocolViolation("find result must be object".into()))?;
    let arr = obj
        .get("rows")
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::ProtocolViolation("find missing required field rows".into()))?;
    let mut rows = Vec::new();
    for row in arr {
        let key = row
            .get("key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ProtocolViolation("find row key missing".into()))?
            .to_string();
        let json = row
            .get("json")
            .cloned()
            .ok_or_else(|| Error::ProtocolViolation("find row json missing".into()))?;
        rows.push((key, json));
    }
    let incomplete = obj
        .get("incomplete")
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::ProtocolViolation("find missing required field incomplete".into()))?
        .clone();
    let coverage_complete = obj
        .get("coverage_complete")
        .and_then(|v| v.as_bool())
        .ok_or_else(|| {
            Error::ProtocolViolation("find missing required field coverage_complete".into())
        })?;
    if coverage_complete && !incomplete.is_empty() {
        return Err(Error::ProtocolViolation(
            "find coverage_complete true with non-empty incomplete".into(),
        ));
    }
    if !coverage_complete && incomplete.is_empty() {
        return Err(Error::ProtocolViolation(
            "find coverage_complete false with empty incomplete".into(),
        ));
    }
    Ok(FindWirePage {
        rows,
        incomplete,
        coverage_complete,
    })
}

/// Parse and validate op 115 `scan_json` result object (DEF-SCAN-001 T8).
///
/// Rejects missing required fields and contradictory pagination metadata.
pub fn parse_scan_json_wire(result: &Value) -> Result<ScanJsonWirePage, Error> {
    let obj = result
        .as_object()
        .ok_or_else(|| Error::ProtocolViolation("scan_json result must be object".into()))?;
    let arr = obj
        .get("rows")
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::ProtocolViolation("scan_json missing required field rows".into()))?;
    let mut rows = Vec::new();
    for row in arr {
        let key = row
            .get("key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ProtocolViolation("scan row key missing".into()))?
            .to_string();
        let json = row
            .get("json")
            .cloned()
            .ok_or_else(|| Error::ProtocolViolation("scan row json missing".into()))?;
        rows.push((key, json));
    }
    let incomplete = obj
        .get("incomplete")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            Error::ProtocolViolation("scan_json missing required field incomplete".into())
        })?
        .clone();
    let coverage_complete = obj
        .get("coverage_complete")
        .and_then(|v| v.as_bool())
        .ok_or_else(|| {
            Error::ProtocolViolation("scan_json missing required field coverage_complete".into())
        })?;
    let has_more = obj
        .get("has_more")
        .and_then(|v| v.as_bool())
        .ok_or_else(|| {
            Error::ProtocolViolation("scan_json missing required field has_more".into())
        })?;
    let exhausted = obj
        .get("exhausted")
        .and_then(|v| v.as_bool())
        .ok_or_else(|| {
            Error::ProtocolViolation("scan_json missing required field exhausted".into())
        })?;
    if !obj.contains_key("next_after_key") {
        return Err(Error::ProtocolViolation(
            "scan_json missing required field next_after_key".into(),
        ));
    }
    let next_after_key = match obj.get("next_after_key") {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Null) => None,
        _ => {
            return Err(Error::ProtocolViolation(
                "scan_json next_after_key must be string or null".into(),
            ))
        }
    };
    // Pagination invariants (blocker #4).
    if exhausted != !has_more {
        return Err(Error::ProtocolViolation(format!(
            "scan_json exhausted ({exhausted}) must equal !has_more ({})",
            !has_more
        )));
    }
    if has_more && next_after_key.is_none() {
        return Err(Error::ProtocolViolation(
            "scan_json has_more without next_after_key".into(),
        ));
    }
    if !has_more && next_after_key.is_some() {
        return Err(Error::ProtocolViolation(
            "scan_json next_after_key must be null when has_more is false".into(),
        ));
    }
    // Coverage honesty: coverage_complete implies no holes on this page.
    if coverage_complete && !incomplete.is_empty() {
        return Err(Error::ProtocolViolation(
            "scan_json coverage_complete true with non-empty incomplete".into(),
        ));
    }
    if !coverage_complete && incomplete.is_empty() {
        return Err(Error::ProtocolViolation(
            "scan_json coverage_complete false with empty incomplete".into(),
        ));
    }
    Ok(ScanJsonWirePage {
        rows,
        incomplete,
        coverage_complete,
        has_more,
        exhausted,
        next_after_key,
    })
}

/// Errors from credential construction (local only; never contact a server).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialError {
    /// Certificate COSE failed structural inspection.
    MalformedCertificate(String),
    /// Holder public key does not match the certificate claim.
    HolderKeyMismatch,
    /// Signer failed to produce a proof signature.
    SignFailed(String),
}

impl std::fmt::Display for CredentialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MalformedCertificate(m) => write!(f, "malformed certificate: {m}"),
            Self::HolderKeyMismatch => write!(f, "holder public key mismatch"),
            Self::SignFailed(m) => write!(f, "holder proof sign failed: {m}"),
        }
    }
}

impl std::error::Error for CredentialError {}

impl From<CredentialError> for Error {
    fn from(e: CredentialError) -> Self {
        Error::AuthenticationFailed(e.to_string())
    }
}

/// Signs the COSE `Sig_structure` for a holder proof without exporting secrets.
pub trait HolderSigner: Send + Sync {
    /// Ed25519 verifying key bytes claimed by the certificate.
    fn public_key(&self) -> [u8; 32];
    /// Sign the holder-proof `Sig_structure` message.
    fn sign_holder_proof(&self, message: &[u8]) -> Result<[u8; 64], CredentialError>;
}

/// Local HeapKey credential: certificate bytes + holder signer.
///
/// Not `Debug` / `Serialize` / `Deserialize`. Clone only by cloning the signer
/// handle. Master signature is verified by the **server**, not here.
pub struct HeapCredential {
    certificate_cose: Vec<u8>,
    inspected: VerifiedCertificate,
    signer: Arc<dyn HolderSigner>,
}

impl HeapCredential {
    /// Bind `certificate_cose` to `signer` after structural inspect.
    ///
    /// Rejects a signer whose public key differs from the certificate claim.
    /// Does not contact a server and does not verify the master signature.
    pub fn new(
        certificate_cose: &[u8],
        signer: Arc<dyn HolderSigner>,
    ) -> Result<Self, CredentialError> {
        let inspected = inspect_certificate(certificate_cose)
            .map_err(|e| CredentialError::MalformedCertificate(e.to_string()))?;
        if inspected.holder_public_key != signer.public_key() {
            return Err(CredentialError::HolderKeyMismatch);
        }
        Ok(Self {
            certificate_cose: certificate_cose.to_vec(),
            inspected,
            signer,
        })
    }

    /// Heap id claimed by the certificate (informational until server welcomes).
    pub fn heap_id(&self) -> HeapId {
        self.inspected.heap_id
    }

    /// Certificate fingerprint (SHA-256 of COSE bytes).
    pub fn certificate_fingerprint(&self) -> [u8; 32] {
        self.inspected.fingerprint
    }

    /// Explicit clone via shared signer handle.
    pub fn clone_credential(&self) -> Self {
        Self {
            certificate_cose: self.certificate_cose.clone(),
            inspected: self.inspected.clone(),
            signer: Arc::clone(&self.signer),
        }
    }
}

/// In-memory Ed25519 holder key (tests / local dev only).
///
/// Gated behind feature `dangerous-key-export`. Does not expose a raw secret
/// export method; construction from a 32-byte seed is the gated surface.
#[cfg(feature = "dangerous-key-export")]
pub struct InMemoryHolderKey {
    sk: ed25519_dalek::SigningKey,
}

#[cfg(feature = "dangerous-key-export")]
impl InMemoryHolderKey {
    /// Construct from a 32-byte seed (reference / test only).
    pub fn from_seed(seed: [u8; 32]) -> Self {
        Self {
            sk: ed25519_dalek::SigningKey::from_bytes(&seed),
        }
    }
}

#[cfg(feature = "dangerous-key-export")]
impl HolderSigner for InMemoryHolderKey {
    fn public_key(&self) -> [u8; 32] {
        self.sk.verifying_key().to_bytes()
    }

    fn sign_holder_proof(&self, message: &[u8]) -> Result<[u8; 64], CredentialError> {
        use ed25519_dalek::Signer;
        Ok(self.sk.sign(message).to_bytes())
    }
}

/// Options for [`crate::Residiuum::connect_heap`].
///
/// TLS is mandatory. There is no token, role, username, diagnostic-line,
/// plaintext, or caller-supplied heap-id field.
pub struct RemoteHeapOptions {
    /// TLS client identity / trust.
    pub tls: TlsClientOptions,
    /// HeapKey credential.
    pub credential: HeapCredential,
    /// Optional human name checked after authority succeeds.
    pub expected_heap_name: Option<String>,
    /// Per-attempt TCP connect timeout.
    pub connect_timeout: Duration,
    /// Read/write timeout on the established stream.
    pub request_timeout: Duration,
    /// Total connect attempts (at least 1).
    pub max_connect_attempts: NonZeroU32,
    /// Backoff between connect attempts.
    pub retry_backoff: Duration,
    /// Override security-time for proof `created_at` (tests).
    pub now_unix_s: Option<u64>,
}

impl RemoteHeapOptions {
    /// Defaults: no expected name, 5s connect, 30s request, 3 attempts, 50ms backoff.
    pub fn new(tls: TlsClientOptions, credential: HeapCredential) -> Self {
        Self {
            tls,
            credential,
            expected_heap_name: None,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_connect_attempts: NonZeroU32::new(3).expect("3"),
            retry_backoff: Duration::from_millis(50),
            now_unix_s: None,
        }
    }

    /// Expected human heap label checked after authority.
    pub fn expected_heap_name(mut self, name: impl Into<String>) -> Self {
        self.expected_heap_name = Some(name.into());
        self
    }

    /// Per-attempt TCP connect timeout.
    pub fn connect_timeout(mut self, value: Duration) -> Self {
        self.connect_timeout = value;
        self
    }

    /// Stream I/O timeout after connect.
    pub fn request_timeout(mut self, value: Duration) -> Self {
        self.request_timeout = value;
        self
    }

    /// Total connect attempts (including the first).
    pub fn max_connect_attempts(mut self, value: NonZeroU32) -> Self {
        self.max_connect_attempts = value;
        self
    }

    /// Sleep between failed connect attempts.
    pub fn retry_backoff(mut self, value: Duration) -> Self {
        self.retry_backoff = value;
        self
    }

    /// Pin proof `created_at` (and local time basis) for tests.
    pub fn now_unix_s(mut self, value: u64) -> Self {
        self.now_unix_s = Some(value);
        self
    }
}

/// Established qualified remote heap session (HP-007 `connect_heap`).
///
/// Bound to a single `HeapId` from the server welcome. Exposes process ops
/// 1–3 until collection data ops activate under §32.4.
pub struct RemoteHeap {
    stream: IoStream,
    welcome: HeapWelcome,
    heap_id: HeapId,
    max_frame: usize,
    next_id: AtomicU64,
}

impl RemoteHeap {
    /// Bound heap id from the server welcome.
    pub fn id(&self) -> HeapId {
        self.heap_id
    }

    /// Wire welcome object.
    pub fn welcome(&self) -> &HeapWelcome {
        &self.welcome
    }

    /// Session id from welcome (capability/session hex).
    pub fn session_id(&self) -> &str {
        &self.welcome.session_id
    }

    /// Heap profile string from welcome.
    pub fn heap_profile(&self) -> &str {
        &self.welcome.heap_profile
    }

    /// Qualified process ping (op_id = 1).
    pub fn ping(&mut self) -> Result<(), Error> {
        let result = self.call_process(1)?;
        if result.get("pong") == Some(&Value::Bool(true)) {
            Ok(())
        } else {
            Err(Error::ProtocolViolation(
                "qualified ping missing pong:true".into(),
            ))
        }
    }

    /// Qualified live probe (op_id = 2).
    pub fn live(&mut self) -> Result<bool, Error> {
        let result = self.call_process(2)?;
        Ok(result.get("live") == Some(&Value::Bool(true)))
    }

    /// Qualified ready probe (op_id = 3).
    pub fn ready(&mut self) -> Result<bool, Error> {
        let result = self.call_process(3)?;
        Ok(result.get("ready") == Some(&Value::Bool(true)))
    }

    /// Open a collection by canonical name (op_id = 105). Returns collection UUID string.
    pub fn collection_open(&mut self, name: &str) -> Result<String, Error> {
        let result = self.call_args(105, None, serde_json::json!({ "name": name }))?;
        result
            .get("collection_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| Error::ProtocolViolation("collection_open missing collection_id".into()))
    }

    /// Create a collection by canonical name (op_id = 106). Requires `HeapAdmin`.
    ///
    /// When `operation_id` is `None`, the client mints a random 16-byte id.
    /// Returns `(collection_id_uuid, descriptor_hash_hex, replayed)`.
    pub fn create_collection(
        &mut self,
        name: &str,
        operation_id: Option<[u8; 16]>,
    ) -> Result<RemoteCreatedCollection, Error> {
        let op = operation_id.unwrap_or_else(|| {
            // Prefer UUIDv4 so admin/op envelopes match CollectionId-shaped mints.
            if let Ok(id) = residiuum_heap::CollectionId::new_random() {
                return *id.as_bytes();
            }
            let mut b = [0u8; 16];
            if getrandom::fill(&mut b).is_err() {
                let n = self.next_id.load(Ordering::Relaxed);
                b[..8].copy_from_slice(&n.to_le_bytes());
            }
            b[6] = (b[6] & 0x0f) | 0x40;
            b[8] = (b[8] & 0x3f) | 0x80;
            b
        });
        let op_hex: String = op.iter().map(|x| format!("{x:02x}")).collect();
        let result = self.call_mutation(
            106,
            &op_hex,
            None,
            serde_json::json!({ "canonical_name": name }),
        )?;
        let collection_id = result
            .get("collection_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::ProtocolViolation("collection_create missing collection_id".into())
            })?
            .to_string();
        let descriptor_hash = result
            .get("descriptor_hash")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::ProtocolViolation("collection_create missing descriptor_hash".into())
            })?
            .to_string();
        let replayed = result
            .get("replayed")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let receipt_id = result
            .pointer("/receipt/receipt_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Ok(RemoteCreatedCollection {
            collection_id,
            canonical_name: name.to_string(),
            descriptor_hash,
            receipt_id,
            operation_id: op,
            replayed,
        })
    }

    /// Put JSON under `key` in `collection_id` (op_id = 120).
    pub fn put_json(
        &mut self,
        collection_id: &str,
        key: &str,
        json: &Value,
    ) -> Result<(String, String), Error> {
        self.put_json_if(collection_id, key, json, None, false)
    }

    /// Conditional put JSON (APB-2 T7 wire Key Atomic).
    ///
    /// - `if_version: Some` → live establishing event id must match
    /// - `if_absent: true` → key must be absent (create)
    /// - both off → unconditional put
    pub fn put_json_if(
        &mut self,
        collection_id: &str,
        key: &str,
        json: &Value,
        if_version: Option<[u8; 16]>,
        if_absent: bool,
    ) -> Result<(String, String), Error> {
        let mut args = serde_json::json!({ "key": key, "json": json });
        if let Some(v) = if_version {
            args.as_object_mut().unwrap().insert(
                "if_version".into(),
                Value::String(residiuum_store::hex16(&v)),
            );
        }
        if if_absent {
            args.as_object_mut()
                .unwrap()
                .insert("if_absent".into(), Value::Bool(true));
        }
        let result = self.call_args(120, Some(collection_id), args)?;
        let event_id = result
            .get("event_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ProtocolViolation("put missing event_id".into()))?
            .to_string();
        let version = result
            .get("version")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ProtocolViolation("put missing version".into()))?
            .to_string();
        Ok((event_id, version))
    }

    /// Put opaque bytes under `key` (op_id = 121).
    pub fn put_bytes(
        &mut self,
        collection_id: &str,
        key: &str,
        bytes: &[u8],
    ) -> Result<(String, String), Error> {
        let result = self.call_args(
            121,
            Some(collection_id),
            serde_json::json!({
                "key": key,
                "bytes_b64": residiuum_client::b64u_encode(bytes),
            }),
        )?;
        let event_id = result
            .get("event_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ProtocolViolation("put_bytes missing event_id".into()))?
            .to_string();
        let version = result
            .get("version")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::ProtocolViolation("put_bytes missing version".into()))?
            .to_string();
        Ok((event_id, version))
    }

    /// Get JSON for `key` (op_id = 111). `None` when not found.
    pub fn get_json(&mut self, collection_id: &str, key: &str) -> Result<Option<Value>, Error> {
        let result = self.call_args(111, Some(collection_id), serde_json::json!({ "key": key }))?;
        match result.get("found") {
            Some(Value::Bool(false)) => Ok(None),
            Some(Value::Bool(true)) => Ok(result.get("json").cloned()),
            _ => Err(Error::ProtocolViolation("get missing found".into())),
        }
    }

    /// Get opaque bytes for `key` (op_id = 112).
    pub fn get_bytes(&mut self, collection_id: &str, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let result = self.call_args(112, Some(collection_id), serde_json::json!({ "key": key }))?;
        match result.get("found") {
            Some(Value::Bool(false)) => Ok(None),
            Some(Value::Bool(true)) => {
                let b64 = result
                    .get("bytes_b64")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        Error::ProtocolViolation("get_bytes missing bytes_b64".into())
                    })?;
                residiuum_client::b64u_decode(b64)
                    .map(Some)
                    .map_err(|e| Error::ProtocolViolation(format!("bytes_b64: {e}")))
            }
            _ => Err(Error::ProtocolViolation("get_bytes missing found".into())),
        }
    }

    /// Delete `key` (op_id = 122). Returns whether a value was present.
    pub fn delete(&mut self, collection_id: &str, key: &str) -> Result<bool, Error> {
        Ok(self.delete_if(collection_id, key, None, false)?.0)
    }

    /// Conditional delete (APB-2 T7 wire Key Atomic).
    ///
    /// Returns `(removed, event_id_hex, version_hex)`.
    ///
    /// - `if_version: Some` → live establishing event id must match
    /// - `if_present: true` → absence is `not_found` (unless version also set)
    pub fn delete_if(
        &mut self,
        collection_id: &str,
        key: &str,
        if_version: Option<[u8; 16]>,
        if_present: bool,
    ) -> Result<(bool, String, String), Error> {
        let mut args = serde_json::json!({ "key": key });
        if let Some(v) = if_version {
            args.as_object_mut().unwrap().insert(
                "if_version".into(),
                Value::String(residiuum_store::hex16(&v)),
            );
        }
        if if_present {
            args.as_object_mut()
                .unwrap()
                .insert("if_present".into(), Value::Bool(true));
        }
        let result = self.call_args(122, Some(collection_id), args)?;
        let removed = result.get("removed") == Some(&Value::Bool(true));
        let event_id = result
            .get("event_id")
            .and_then(|v| v.as_str())
            .unwrap_or("00000000000000000000000000000000")
            .to_string();
        let version = result
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or(event_id.as_str())
            .to_string();
        Ok((removed, event_id, version))
    }

    /// List collections in the bound heap (op_id = 110).
    ///
    /// Each item is `(collection_id, name)`.
    pub fn list_collections(&mut self) -> Result<Vec<(String, String)>, Error> {
        let result = self.call_args(110, None, serde_json::json!({}))?;
        let arr = result
            .get("collections")
            .and_then(|v| v.as_array())
            .ok_or_else(|| Error::ProtocolViolation("list_collections missing array".into()))?;
        let mut out = Vec::new();
        for item in arr {
            let id = item
                .get("collection_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::ProtocolViolation("collection_id missing".into()))?
                .to_string();
            let name = item
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::ProtocolViolation("name missing".into()))?
                .to_string();
            out.push((id, name));
        }
        Ok(out)
    }

    /// List keys in a collection (op_id = 114).
    pub fn list_keys(
        &mut self,
        collection_id: &str,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        let mut args = serde_json::Map::new();
        if let Some(l) = limit {
            args.insert("limit".into(), Value::from(l as u64));
        }
        if let Some(a) = after_key {
            args.insert("after_key".into(), Value::String(a.into()));
        }
        let result = self.call_args(114, Some(collection_id), Value::Object(args))?;
        let arr = result
            .get("keys")
            .and_then(|v| v.as_array())
            .ok_or_else(|| Error::ProtocolViolation("list_keys missing keys".into()))?;
        Ok(arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect())
    }

    /// Per-key event history (op_id = 117), oldest first.
    ///
    /// Each version is a JSON object with `kind`, `event_id`, `item_id`,
    /// `segment_id`, `known_gap_before`, and optional `json` for put events.
    pub fn history(&mut self, collection_id: &str, key: &str) -> Result<(Vec<Value>, bool), Error> {
        let result = self.call_args(117, Some(collection_id), serde_json::json!({ "key": key }))?;
        let holes = result
            .get("has_known_holes")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let versions = result
            .get("versions")
            .and_then(|v| v.as_array())
            .cloned()
            .ok_or_else(|| Error::ProtocolViolation("history missing versions".into()))?;
        Ok((versions, holes))
    }

    /// Find JSON rows matching a Mongo-style filter object (op_id = 116).
    ///
    /// Filter vocabulary matches [`crate::Filter::from_json`]. Server may use
    /// secondary-index acceleration; incompleteness is required on the wire
    /// for **every** materialization source (DEF-SCAN-001 blocker #5).
    pub fn find(
        &mut self,
        collection_id: &str,
        filter: &Value,
        limit: Option<usize>,
    ) -> Result<FindWirePage, Error> {
        let mut args = serde_json::Map::new();
        args.insert("filter".into(), filter.clone());
        if let Some(l) = limit {
            args.insert("limit".into(), Value::from(l as u64));
        }
        let result = self.call_args(116, Some(collection_id), Value::Object(args))?;
        parse_find_wire(&result)
    }

    /// List secondary indexes on a collection (op_id = 130).
    pub fn index_list(&mut self, collection_id: &str) -> Result<Vec<Value>, Error> {
        let result = self.call_args(130, Some(collection_id), serde_json::json!({}))?;
        let arr = result
            .get("indexes")
            .and_then(|v| v.as_array())
            .cloned()
            .ok_or_else(|| Error::ProtocolViolation("index_list missing indexes".into()))?;
        Ok(arr)
    }

    /// Create a secondary field index (op_id = 131). Full rebuild, first cut.
    ///
    /// Requires IndexAdmin on the session capability.
    pub fn index_create(
        &mut self,
        collection_id: &str,
        name: &str,
        fields: &[&str],
    ) -> Result<Value, Error> {
        let fields: Vec<Value> = fields.iter().map(|f| Value::String((*f).into())).collect();
        let result = self.call_args(
            131,
            Some(collection_id),
            serde_json::json!({ "name": name, "fields": fields }),
        )?;
        if result.get("name").and_then(|v| v.as_str()) != Some(name) {
            return Err(Error::ProtocolViolation(
                "index_create response missing metadata".into(),
            ));
        }
        Ok(result)
    }

    /// Drop a secondary index by name (op_id = 132).
    pub fn index_drop(&mut self, collection_id: &str, name: &str) -> Result<bool, Error> {
        let result = self.call_args(
            132,
            Some(collection_id),
            serde_json::json!({ "name": name }),
        )?;
        Ok(result
            .get("dropped")
            .and_then(|v| v.as_bool())
            .unwrap_or(false))
    }

    /// Rebuild an existing secondary index from a full collection scan (op_id = 133).
    pub fn index_rebuild(&mut self, collection_id: &str, name: &str) -> Result<Value, Error> {
        let result = self.call_args(
            133,
            Some(collection_id),
            serde_json::json!({ "name": name }),
        )?;
        if result.get("name").and_then(|v| v.as_str()) != Some(name) {
            return Err(Error::ProtocolViolation(
                "index_rebuild response missing metadata".into(),
            ));
        }
        Ok(result)
    }

    /// Application Core RQL page / explain (op_id = **118** `rql_query`).
    ///
    /// APP-7 T6: product wire path. Server recompiles `source` and returns a
    /// page object matching `rql_query.response.json`. Not a package-accept claim.
    pub fn rql_query(&mut self, collection_id: &str, args: Value) -> Result<Value, Error> {
        self.call_args(118, Some(collection_id), args)
    }

    /// Scan JSON rows in a collection (op_id = 115).
    ///
    /// Returns complete rows plus **required** wire pagination / hole metadata.
    /// Continuation is [`ScanJsonWirePage::next_after_key`] (last examined key),
    /// never the last successful row alone (DEF-SCAN-001). Rejects missing or
    /// contradictory fields (T8 / blocker #4).
    pub fn scan_json(
        &mut self,
        collection_id: &str,
        limit: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<ScanJsonWirePage, Error> {
        let mut args = serde_json::Map::new();
        if let Some(l) = limit {
            args.insert("limit".into(), Value::from(l as u64));
        }
        if let Some(a) = after_key {
            args.insert("after_key".into(), Value::String(a.into()));
        }
        let result = self.call_args(115, Some(collection_id), Value::Object(args))?;
        parse_scan_json_wire(&result)
    }

    fn call_process(&mut self, op_id: u16) -> Result<Value, Error> {
        self.call_args(op_id, None, serde_json::json!({}))
    }

    fn call_args(
        &mut self,
        op_id: u16,
        collection_id: Option<&str>,
        args: Value,
    ) -> Result<Value, Error> {
        self.call_envelope(op_id, None, collection_id, args)
    }

    fn call_mutation(
        &mut self,
        op_id: u16,
        operation_id_hex: &str,
        collection_id: Option<&str>,
        args: Value,
    ) -> Result<Value, Error> {
        self.call_envelope(op_id, Some(operation_id_hex), collection_id, args)
    }

    fn call_envelope(
        &mut self,
        op_id: u16,
        operation_id_hex: Option<&str>,
        collection_id: Option<&str>,
        args: Value,
    ) -> Result<Value, Error> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut req = serde_json::json!({
            "v": 1,
            "id": id,
            "op_id": op_id,
            "args": args,
        });
        if let Some(oid) = operation_id_hex {
            req.as_object_mut()
                .unwrap()
                .insert("operation_id".into(), Value::String(oid.into()));
        }
        if let Some(cid) = collection_id {
            req.as_object_mut()
                .unwrap()
                .insert("collection_id".into(), Value::String(cid.into()));
        }
        write_json_frame(&mut self.stream, &req).map_err(Error::from)?;
        let resp_bytes = read_frame(&mut self.stream, self.max_frame)
            .map_err(Error::from)?
            .ok_or_else(|| Error::ProtocolViolation("connection closed during RPC".into()))?;
        let resp: Value = serde_json::from_slice(&resp_bytes)
            .map_err(|e| Error::ProtocolViolation(format!("rpc response: {e}")))?;
        if resp.get("ok") != Some(&Value::Bool(true)) {
            let code = resp
                .pointer("/error/code")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            // APB-2 T7: surface structured version_conflict when server provides tokens.
            if code == "version_conflict" {
                let expected = resp
                    .pointer("/result/expected")
                    .and_then(|v| v.as_str())
                    .and_then(unhex16_local)
                    .unwrap_or([0u8; 16]);
                let observed = resp
                    .pointer("/result/observed")
                    .and_then(|v| {
                        if v.is_null() {
                            Some(None)
                        } else {
                            v.as_str().and_then(unhex16_local).map(Some)
                        }
                    })
                    .unwrap_or(None);
                return Err(Error::VersionConflict { expected, observed });
            }
            return Err(Error::Remote {
                code: code.into(),
                message: format!("qualified op {op_id} failed"),
            });
        }
        Ok(resp
            .get("result")
            .cloned()
            .unwrap_or(Value::Object(Map::new())))
    }
}

fn unhex16_local(s: &str) -> Option<[u8; 16]> {
    residiuum_store::unhex16(s)
}

/// Result of [`RemoteHeap::create_collection`] (op 106).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteCreatedCollection {
    /// Immutable collection id as canonical UUID string.
    pub collection_id: String,
    /// Canonical name.
    pub canonical_name: String,
    /// Descriptor hash as 64 lowercase hex characters.
    pub descriptor_hash: String,
    /// Admin receipt id as 32 lowercase hex characters (empty if omitted).
    pub receipt_id: String,
    /// Client operation id used for this attempt.
    pub operation_id: [u8; 16],
    /// True when the server returned a dedup replay.
    pub replayed: bool,
}

/// Connect to a qualified heap listener (`residiuum://host:port[/label]`).
///
/// The URL path label is an expected human name only when
/// [`RemoteHeapOptions::expected_heap_name`] is not set; when both are set the
/// option wins. Authority comes solely from the credential certificate.
pub fn connect_heap(url: impl AsRef<str>, options: RemoteHeapOptions) -> Result<RemoteHeap, Error> {
    let url = url.as_ref();
    let parsed = parse_residiuum_url(url)?;
    if parsed.seeds.is_empty() {
        return Err(Error::ValidationMsg("empty residiuum:// URL".into()));
    }
    let expected_name = options
        .expected_heap_name
        .clone()
        .or_else(|| parsed.label.clone().filter(|s| !s.is_empty()));

    let mut last_err: Option<Error> = None;
    for attempt in 0..options.max_connect_attempts.get() {
        if attempt > 0 {
            thread::sleep(options.retry_backoff);
        }
        for hostport in &parsed.seeds {
            match connect_heap_once(hostport, &options, expected_name.as_deref()) {
                Ok(remote) => return Ok(remote),
                Err(e) => last_err = Some(e),
            }
        }
    }
    Err(last_err.unwrap_or_else(|| Error::Internal("connect_heap failed".into())))
}

fn connect_heap_once(
    hostport: &str,
    options: &RemoteHeapOptions,
    expected_heap_name: Option<&str>,
) -> Result<RemoteHeap, Error> {
    let addr = resolve_hostport(hostport)?;
    let tcp = TcpStream::connect_timeout(&addr, options.connect_timeout).map_err(|e| {
        Error::from_io(std::io::Error::new(
            e.kind(),
            format!("connect {hostport}: {e}"),
        ))
    })?;
    tcp.set_read_timeout(Some(options.request_timeout))
        .map_err(Error::from_io)?;
    tcp.set_write_timeout(Some(options.request_timeout))
        .map_err(Error::from_io)?;

    let (mut stream, _) = client_connect(tcp, &options.tls)?;
    if !stream.is_tls() {
        return Err(Error::ProtocolViolation(
            "connect_heap requires TLS 1.3".into(),
        ));
    }
    let exporter = stream.export_channel_binding()?;

    // hello with heap-key-v1
    let mut hello = Handshake::hello();
    let mut feats = hello.features.take().unwrap_or_default();
    if !feats.iter().any(|f| f == FEATURE_HEAP_KEY_V1) {
        feats.push(FEATURE_HEAP_KEY_V1.into());
    }
    hello.features = Some(feats);
    write_json_frame(&mut stream, &hello).map_err(Error::from)?;

    let challenge_bytes = read_frame(&mut stream, HANDSHAKE_MAX_FRAME_BYTES)
        .map_err(Error::from)?
        .ok_or_else(|| {
            Error::ProtocolViolation("connection closed before heap_challenge".into())
        })?;
    // Uniform reject before challenge is a protocol error shape; parse either.
    if let Ok(reject) = serde_json::from_slice::<HeapReject>(&challenge_bytes) {
        if reject.msg == "reject" {
            return Err(Error::Remote {
                code: reject.code,
                message: "heap unavailable during handshake".into(),
            });
        }
    }
    let challenge: HeapChallenge = serde_json::from_slice(&challenge_bytes)
        .map_err(|e| Error::ProtocolViolation(format!("heap_challenge decode: {e}")))?;
    if challenge.msg != "heap_challenge" {
        return Err(Error::ProtocolViolation(format!(
            "expected heap_challenge, got {}",
            challenge.msg
        )));
    }
    let nonce_vec = b64u_decode(&challenge.server_nonce_b64u)
        .map_err(|e| Error::ProtocolViolation(format!("nonce b64u: {e}")))?;
    if nonce_vec.len() != 32 {
        return Err(Error::ProtocolViolation("nonce must be 32 bytes".into()));
    }
    let mut nonce = [0u8; 32];
    nonce.copy_from_slice(&nonce_vec);

    let now_unix = options.now_unix_s.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    });
    let mut proof_id = [0u8; 16];
    getrandom::fill(&mut proof_id)
        .map_err(|_| Error::Internal("getrandom failed for proof_id".into()))?;

    let cert = &options.credential.inspected;
    let signer = Arc::clone(&options.credential.signer);
    let proof = build_holder_proof(cert, &nonce, &exporter, proof_id, now_unix, |msg| {
        signer.sign_holder_proof(msg).map_err(|_| {
            residiuum_heap::HeapError::unavailable(
                residiuum_heap::HeapUnavailableCause::MalformedOrBadSignature,
            )
        })
    })
    .map_err(|e| Error::AuthenticationFailed(format!("holder proof: {e}")))?;

    let auth = HeapAuth {
        v: 1,
        msg: "heap_auth".into(),
        heap_id: cert.heap_id.to_string(),
        certificate_b64u: b64u_encode(&options.credential.certificate_cose),
        holder_proof_b64u: b64u_encode(&proof),
        expected_heap_name: expected_heap_name.map(|s| s.to_string()),
    };
    write_json_frame(&mut stream, &auth).map_err(Error::from)?;

    let welcome_bytes = read_frame(&mut stream, HEAP_AUTH_MAX_BYTES)
        .map_err(Error::from)?
        .ok_or_else(|| Error::ProtocolViolation("connection closed before welcome".into()))?;
    if let Ok(reject) = serde_json::from_slice::<HeapReject>(&welcome_bytes) {
        if reject.msg == "reject" {
            return Err(Error::Remote {
                code: reject.code,
                message: "heap unavailable after heap_auth".into(),
            });
        }
    }
    let welcome: HeapWelcome = serde_json::from_slice(&welcome_bytes)
        .map_err(|e| Error::ProtocolViolation(format!("welcome decode: {e}")))?;
    if welcome.msg != "welcome" {
        return Err(Error::ProtocolViolation(format!(
            "expected welcome, got {}",
            welcome.msg
        )));
    }
    if welcome.heap_profile != HEAP_PROFILE {
        return Err(Error::ProtocolViolation(format!(
            "unexpected heap_profile {}",
            welcome.heap_profile
        )));
    }
    let heap_id: HeapId = welcome
        .heap_id
        .parse()
        .map_err(|e| Error::ProtocolViolation(format!("welcome heap_id: {e}")))?;
    if heap_id != cert.heap_id {
        return Err(Error::ProtocolViolation(
            "welcome heap_id mismatches credential".into(),
        ));
    }

    Ok(RemoteHeap {
        stream,
        welcome,
        heap_id,
        max_frame: residiuum_client::DEFAULT_MAX_FRAME_BYTES,
        next_id: AtomicU64::new(1),
    })
}

fn resolve_hostport(hostport: &str) -> Result<std::net::SocketAddr, Error> {
    let with_port = if hostport.contains(':') {
        hostport.to_string()
    } else {
        format!("{hostport}:{DEFAULT_PORT}")
    };
    with_port
        .to_socket_addrs()
        .map_err(|e| Error::ValidationMsg(format!("resolve {with_port}: {e}")))?
        .next()
        .ok_or_else(|| Error::ValidationMsg(format!("no addresses for {with_port}")))
}

#[cfg(test)]
mod scan_json_wire_tests {
    use super::parse_scan_json_wire;
    use serde_json::json;

    fn good_page() -> serde_json::Value {
        json!({
            "rows": [{"key": "a", "json": {"n": 1}}],
            "incomplete": [],
            "coverage_complete": true,
            "has_more": false,
            "exhausted": true,
            "next_after_key": null
        })
    }

    #[test]
    fn accepts_valid_exhausted_page() {
        let p = parse_scan_json_wire(&good_page()).unwrap();
        assert!(!p.has_more);
        assert!(p.exhausted);
        assert!(p.next_after_key.is_none());
        assert!(p.coverage_complete);
        assert!(p.incomplete.is_empty());
    }

    #[test]
    fn accepts_valid_has_more_page() {
        let v = json!({
            "rows": [{"key": "a", "json": {}}],
            "incomplete": [{"key": "b", "reason": "segment_not_found"}],
            "coverage_complete": false,
            "has_more": true,
            "exhausted": false,
            "next_after_key": "b"
        });
        let p = parse_scan_json_wire(&v).unwrap();
        assert!(p.has_more);
        assert!(!p.exhausted);
        assert_eq!(p.next_after_key.as_deref(), Some("b"));
        assert!(!p.coverage_complete);
    }

    #[test]
    fn rejects_missing_required_fields() {
        for field in [
            "incomplete",
            "coverage_complete",
            "has_more",
            "exhausted",
            "next_after_key",
            "rows",
        ] {
            let mut m = good_page();
            m.as_object_mut().unwrap().remove(field);
            assert!(
                parse_scan_json_wire(&m).is_err(),
                "expected error missing {field}"
            );
        }
    }

    #[test]
    fn rejects_exhausted_neq_not_has_more() {
        let mut m = good_page();
        m["exhausted"] = json!(false); // has_more false, exhausted must be true
        assert!(parse_scan_json_wire(&m).is_err());
    }

    #[test]
    fn rejects_has_more_without_cursor() {
        let v = json!({
            "rows": [],
            "incomplete": [],
            "coverage_complete": true,
            "has_more": true,
            "exhausted": false,
            "next_after_key": null
        });
        assert!(parse_scan_json_wire(&v).is_err());
    }

    #[test]
    fn rejects_cursor_when_not_has_more() {
        let mut m = good_page();
        m["next_after_key"] = json!("z");
        assert!(parse_scan_json_wire(&m).is_err());
    }

    #[test]
    fn rejects_coverage_complete_vs_incomplete_mismatch() {
        let mut m = good_page();
        m["coverage_complete"] = json!(false);
        assert!(parse_scan_json_wire(&m).is_err());
        let mut m2 = good_page();
        m2["incomplete"] = json!([{"key": "x", "reason": "segment_not_found"}]);
        // coverage_complete still true
        assert!(parse_scan_json_wire(&m2).is_err());
    }
}

#[cfg(test)]
mod find_wire_tests {
    use super::parse_find_wire;
    use serde_json::json;

    #[test]
    fn accepts_complete_find() {
        let v = json!({
            "rows": [{"key": "a", "json": {"n": 1}}],
            "incomplete": [],
            "coverage_complete": true
        });
        let p = parse_find_wire(&v).unwrap();
        assert!(p.coverage_complete);
        assert_eq!(p.rows.len(), 1);
    }

    #[test]
    fn accepts_index_holes() {
        let v = json!({
            "rows": [],
            "incomplete": [{"key": "x", "reason": "segment_not_found"}],
            "coverage_complete": false
        });
        let p = parse_find_wire(&v).unwrap();
        assert!(!p.coverage_complete);
        assert_eq!(p.incomplete.len(), 1);
    }

    #[test]
    fn rejects_missing_incomplete() {
        let v = json!({
            "rows": [],
            "coverage_complete": true
        });
        assert!(parse_find_wire(&v).is_err());
    }

    #[test]
    fn rejects_rows_only_legacy() {
        // Pre-T9 dishonest shape: rows without incomplete/coverage_complete.
        let v = json!({ "rows": [{"key": "a", "json": {}}] });
        assert!(parse_find_wire(&v).is_err());
    }
}
