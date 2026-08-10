//! TLS 1.3 transport and authenticated peer identity (DEF-032).
//!
//! Production non-loopback binds require TLS. Mutual TLS authenticates
//! node-to-node (and optionally client) peers. Certificate SANs carry
//! cluster/node identity as URIs:
//!
//! - `urn:residiuum:cluster:{cluster_id}`
//! - `urn:residiuum:node:{node_id}`
//!
//! Plaintext remains loopback-only (or explicit `--allow-insecure-bind`).
//! Shared application tokens use constant-time comparison and must not be
//! logged.

use crate::error::Error;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{
    ClientConfig, ClientConnection, DigitallySignedStruct, Error as RustlsError, RootCertStore,
    ServerConfig, ServerConnection, SignatureScheme, StreamOwned,
};
use std::fs;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// Profile tag for the TLS transport surface (DEF-032).
pub const TLS_PROFILE: &str = "residiuum-tls-v1";

/// URI scheme prefix for Residiuum cluster identity in certificate SANs.
pub const CLUSTER_URN_PREFIX: &str = "urn:residiuum:cluster:";

/// URI scheme prefix for Residiuum node identity in certificate SANs.
pub const NODE_URN_PREFIX: &str = "urn:residiuum:node:";

/// Build a cluster identity URN for embedding in a certificate SAN.
pub fn cluster_urn(cluster_id: &str) -> String {
    format!("{CLUSTER_URN_PREFIX}{cluster_id}")
}

/// Build a node identity URN for embedding in a certificate SAN.
pub fn node_urn(node_id: &str) -> String {
    format!("{NODE_URN_PREFIX}{node_id}")
}

/// Parse cluster id from a certificate SAN URI, if present.
pub fn parse_cluster_id_from_uri(uri: &str) -> Option<&str> {
    uri.strip_prefix(CLUSTER_URN_PREFIX)
        .filter(|s| !s.is_empty())
}

/// Parse node id from a certificate SAN URI, if present.
pub fn parse_node_id_from_uri(uri: &str) -> Option<&str> {
    uri.strip_prefix(NODE_URN_PREFIX).filter(|s| !s.is_empty())
}

/// Ensure the default rustls crypto provider is installed once.
fn ensure_crypto_provider() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Constant-time equality for secrets (DEF-032).
///
/// Length is not secret for our shared-token mode (tokens are fixed operator
/// strings), but we still avoid early-exit byte compares on equal-length inputs.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Constant-time string equality for auth tokens.
pub fn constant_time_str_eq(a: &str, b: &str) -> bool {
    constant_time_eq(a.as_bytes(), b.as_bytes())
}

/// Redact a secret for logs (never emit the real value).
pub fn redact_secret(_value: &str) -> &'static str {
    "***"
}

/// TLS material and policy for a serving process.
#[derive(Debug, Clone)]
pub struct TlsServerOptions {
    /// PEM certificate chain path (leaf first).
    pub cert_path: PathBuf,
    /// PEM private key path (PKCS#8).
    pub key_path: PathBuf,
    /// Optional PEM CA bundle used to verify client certificates (mTLS).
    pub client_ca_path: Option<PathBuf>,
    /// When true (default if `client_ca_path` is set), require a client cert.
    pub require_client_cert: bool,
    /// When set, peer (client) certificate must carry this cluster URN.
    pub expected_cluster_id: Option<String>,
    /// Optional denylist of certificate serial numbers (hex, lowercase) for
    /// operator-driven revocation without full CRL infrastructure.
    pub revoked_serials_hex: Vec<String>,
}

impl TlsServerOptions {
    /// Server TLS from certificate + key paths (TLS-only client auth optional).
    pub fn new(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
            client_ca_path: None,
            require_client_cert: false,
            expected_cluster_id: None,
            revoked_serials_hex: Vec::new(),
        }
    }

    /// Require mTLS with the given client CA bundle.
    pub fn with_client_ca(mut self, path: impl Into<PathBuf>) -> Self {
        self.client_ca_path = Some(path.into());
        self.require_client_cert = true;
        self
    }

    /// Require peer certificates to present this cluster id.
    pub fn expected_cluster_id(mut self, id: impl Into<String>) -> Self {
        self.expected_cluster_id = Some(id.into());
        self
    }

    /// Revoke specific certificate serials (hex).
    pub fn revoke_serials_hex(
        mut self,
        serials: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.revoked_serials_hex = serials
            .into_iter()
            .map(|s| s.into().to_ascii_lowercase())
            .collect();
        self
    }
}

/// TLS material and policy for a client connection.
#[derive(Debug, Clone)]
pub struct TlsClientOptions {
    /// DNS/IP name verified against the server certificate (SNI + hostname).
    pub server_name: String,
    /// Optional PEM CA bundle (when unset, webpki roots are used).
    pub ca_path: Option<PathBuf>,
    /// Optional client certificate path (mTLS).
    pub client_cert_path: Option<PathBuf>,
    /// Optional client private key path (mTLS).
    pub client_key_path: Option<PathBuf>,
    /// When set, server certificate must carry this cluster URN.
    pub expected_cluster_id: Option<String>,
    /// When set, server certificate must carry this node URN.
    pub expected_node_id: Option<String>,
    /// Optional denylist of certificate serial numbers (hex, lowercase).
    pub revoked_serials_hex: Vec<String>,
}

impl TlsClientOptions {
    /// Client TLS verifying `server_name` against the certificate.
    pub fn new(server_name: impl Into<String>) -> Self {
        Self {
            server_name: server_name.into(),
            ca_path: None,
            client_cert_path: None,
            client_key_path: None,
            expected_cluster_id: None,
            expected_node_id: None,
            revoked_serials_hex: Vec::new(),
        }
    }

    /// Trust this PEM CA bundle (private PKI).
    pub fn ca_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca_path = Some(path.into());
        self
    }

    /// Present a client certificate (mTLS).
    pub fn client_identity(
        mut self,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.client_cert_path = Some(cert_path.into());
        self.client_key_path = Some(key_path.into());
        self
    }

    /// Require the server cert to carry this cluster id.
    pub fn expected_cluster_id(mut self, id: impl Into<String>) -> Self {
        self.expected_cluster_id = Some(id.into());
        self
    }

    /// Require the server cert to carry this node id.
    pub fn expected_node_id(mut self, id: impl Into<String>) -> Self {
        self.expected_node_id = Some(id.into());
        self
    }

    /// Revoke specific certificate serials (hex).
    pub fn revoke_serials_hex(
        mut self,
        serials: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.revoked_serials_hex = serials
            .into_iter()
            .map(|s| s.into().to_ascii_lowercase())
            .collect();
        self
    }
}

/// Peer identity extracted from a verified certificate chain.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PeerIdentity {
    /// DNS names from SAN.
    pub dns_names: Vec<String>,
    /// URI SANs.
    pub uris: Vec<String>,
    /// Cluster id from `urn:residiuum:cluster:…` if present.
    pub cluster_id: Option<String>,
    /// Node id from `urn:residiuum:node:…` if present.
    pub node_id: Option<String>,
    /// Leaf serial number as lowercase hex.
    pub serial_hex: Option<String>,
}

impl PeerIdentity {
    /// Extract identity fields from a leaf certificate DER.
    pub fn from_cert_der(der: &[u8]) -> Result<Self, Error> {
        let (_, cert) = x509_parser::parse_x509_certificate(der).map_err(|e| {
            Error::AuthenticationFailed(format!("failed to parse peer certificate: {e}"))
        })?;
        let mut identity = PeerIdentity {
            serial_hex: Some(hex_encode(cert.raw_serial())),
            ..Default::default()
        };
        if let Ok(Some(san)) = cert.subject_alternative_name() {
            for name in &san.value.general_names {
                match name {
                    x509_parser::extensions::GeneralName::DNSName(d) => {
                        identity.dns_names.push((*d).to_string());
                    }
                    x509_parser::extensions::GeneralName::URI(u) => {
                        let u = (*u).to_string();
                        if let Some(c) = parse_cluster_id_from_uri(&u) {
                            identity.cluster_id = Some(c.to_string());
                        }
                        if let Some(n) = parse_node_id_from_uri(&u) {
                            identity.node_id = Some(n.to_string());
                        }
                        identity.uris.push(u);
                    }
                    _ => {}
                }
            }
        }
        Ok(identity)
    }

    /// Enforce expected cluster / node ids when configured.
    pub fn check_expectations(
        &self,
        expected_cluster_id: Option<&str>,
        expected_node_id: Option<&str>,
    ) -> Result<(), Error> {
        if let Some(want) = expected_cluster_id {
            match self.cluster_id.as_deref() {
                Some(got) if got == want => {}
                Some(got) => {
                    return Err(Error::AuthenticationFailed(format!(
                        "peer cluster id mismatch: expected {want:?}, got {got:?}"
                    )));
                }
                None => {
                    return Err(Error::AuthenticationFailed(format!(
                        "peer certificate missing cluster id (expected {want:?})"
                    )));
                }
            }
        }
        if let Some(want) = expected_node_id {
            match self.node_id.as_deref() {
                Some(got) if got == want => {}
                Some(got) => {
                    return Err(Error::AuthenticationFailed(format!(
                        "peer node id mismatch: expected {want:?}, got {got:?}"
                    )));
                }
                None => {
                    return Err(Error::AuthenticationFailed(format!(
                        "peer certificate missing node id (expected {want:?})"
                    )));
                }
            }
        }
        Ok(())
    }

    fn check_not_revoked(&self, revoked: &[String]) -> Result<(), Error> {
        if let Some(ref serial) = self.serial_hex {
            if revoked.iter().any(|r| r == serial) {
                return Err(Error::AuthenticationFailed(format!(
                    "peer certificate serial {serial} is revoked"
                )));
            }
        }
        Ok(())
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Load PEM certificates from a file (one or more CERTIFICATE blocks).
pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, Error> {
    let data = fs::read(path)
        .map_err(|e| Error::ValidationMsg(format!("read cert {}: {e}", path.display())))?;
    let mut reader = io::Cursor::new(data);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Error::ValidationMsg(format!("parse cert {}: {e}", path.display())))?;
    if certs.is_empty() {
        return Err(Error::ValidationMsg(format!(
            "no certificates in {}",
            path.display()
        )));
    }
    Ok(certs)
}

/// Load a PEM private key (PKCS#8 or PKCS#1).
pub fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, Error> {
    let data = fs::read(path)
        .map_err(|e| Error::ValidationMsg(format!("read key {}: {e}", path.display())))?;
    let mut reader = io::Cursor::new(data);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| Error::ValidationMsg(format!("parse key {}: {e}", path.display())))?
        .ok_or_else(|| Error::ValidationMsg(format!("no private key found in {}", path.display())))
}

fn load_roots(path: &Path) -> Result<RootCertStore, Error> {
    let certs = load_certs(path)?;
    let mut roots = RootCertStore::empty();
    for c in certs {
        roots
            .add(c)
            .map_err(|e| Error::ValidationMsg(format!("add CA from {}: {e}", path.display())))?;
    }
    Ok(roots)
}

/// Hot-reloadable server certificate resolver (rotation without downtime).
struct ReloadableCertResolver {
    inner: RwLock<Arc<CertifiedKey>>,
    cert_path: PathBuf,
    key_path: PathBuf,
}

impl std::fmt::Debug for ReloadableCertResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadableCertResolver")
            .field("cert_path", &self.cert_path)
            .field("key_path", &self.key_path)
            .finish_non_exhaustive()
    }
}

impl ReloadableCertResolver {
    fn load(cert_path: &Path, key_path: &Path) -> Result<Arc<CertifiedKey>, Error> {
        ensure_crypto_provider();
        let certs = load_certs(cert_path)?;
        let key = load_private_key(key_path)?;
        let signing_key = rustls::crypto::ring::sign::any_supported_type(&key).map_err(|e| {
            Error::ValidationMsg(format!(
                "unsupported private key {}: {e}",
                key_path.display()
            ))
        })?;
        Ok(Arc::new(CertifiedKey::new(certs, signing_key)))
    }

    fn new(cert_path: PathBuf, key_path: PathBuf) -> Result<Self, Error> {
        let certified = Self::load(&cert_path, &key_path)?;
        Ok(Self {
            inner: RwLock::new(certified),
            cert_path,
            key_path,
        })
    }

    fn reload(&self) -> Result<(), Error> {
        let certified = Self::load(&self.cert_path, &self.key_path)?;
        *self
            .inner
            .write()
            .map_err(|_| Error::Internal("tls cert resolver lock poisoned".into()))? = certified;
        Ok(())
    }
}

impl ResolvesServerCert for ReloadableCertResolver {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.inner.read().ok().map(|g| Arc::clone(&*g))
    }
}

/// Shared TLS server state (config + reload handle).
#[derive(Clone)]
pub struct TlsServerState {
    config: Arc<ServerConfig>,
    resolver: Arc<ReloadableCertResolver>,
    options: TlsServerOptions,
}

impl std::fmt::Debug for TlsServerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsServerState")
            .field("options", &self.options)
            .field("mtls", &self.options.require_client_cert)
            .finish_non_exhaustive()
    }
}

impl TlsServerState {
    /// Build server TLS state from options (TLS 1.3 only).
    pub fn from_options(options: TlsServerOptions) -> Result<Self, Error> {
        ensure_crypto_provider();
        let resolver = Arc::new(ReloadableCertResolver::new(
            options.cert_path.clone(),
            options.key_path.clone(),
        )?);

        let builder = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13]);

        let builder = if let Some(ref ca_path) = options.client_ca_path {
            let roots = Arc::new(load_roots(ca_path)?);
            let verifier = if options.require_client_cert {
                WebPkiClientVerifier::builder(roots)
                    .build()
                    .map_err(|e| Error::ValidationMsg(format!("client cert verifier: {e}")))?
            } else {
                WebPkiClientVerifier::builder(roots)
                    .allow_unauthenticated()
                    .build()
                    .map_err(|e| Error::ValidationMsg(format!("client cert verifier: {e}")))?
            };
            builder.with_client_cert_verifier(verifier)
        } else {
            builder.with_no_client_auth()
        };

        let mut config =
            builder.with_cert_resolver(Arc::clone(&resolver) as Arc<dyn ResolvesServerCert>);
        config.alpn_protocols = vec![b"residiuum-rpc-v1".to_vec()];

        Ok(Self {
            config: Arc::new(config),
            resolver,
            options,
        })
    }

    /// Reload certificate and key from disk (new handshakes use the new material).
    pub fn reload(&self) -> Result<(), Error> {
        self.resolver.reload()
    }

    /// Whether TLS is configured (always true for this type).
    pub fn enabled(&self) -> bool {
        true
    }

    /// Borrow the configured options.
    pub fn options(&self) -> &TlsServerOptions {
        &self.options
    }

    /// Perform a TLS server handshake over an accepted TCP stream.
    pub fn accept(&self, tcp: TcpStream) -> Result<(IoStream, Option<PeerIdentity>), Error> {
        let conn = ServerConnection::new(Arc::clone(&self.config)).map_err(tls_err)?;
        let mut stream = StreamOwned::new(conn, tcp);
        // Complete handshake before application framing so identity is available.
        while stream.conn.is_handshaking() {
            stream
                .conn
                .complete_io(&mut stream.sock)
                .map_err(map_tls_io)?;
        }
        let peer = stream
            .conn
            .peer_certificates()
            .and_then(|c| c.first())
            .map(|c| PeerIdentity::from_cert_der(c.as_ref()))
            .transpose()?;
        if let Some(ref id) = peer {
            id.check_expectations(self.options.expected_cluster_id.as_deref(), None)?;
            id.check_not_revoked(&self.options.revoked_serials_hex)?;
        } else if self.options.require_client_cert {
            return Err(Error::AuthenticationFailed(
                "client certificate required (mTLS)".into(),
            ));
        }
        Ok((IoStream::TlsServer(Box::new(stream)), peer))
    }
}

/// Client-side identity-aware certificate verifier.
#[derive(Debug)]
struct IdentityServerVerifier {
    inner: Arc<WebPkiServerVerifier>,
    expected_cluster_id: Option<String>,
    expected_node_id: Option<String>,
    revoked_serials_hex: Vec<String>,
}

impl ServerCertVerifier for IdentityServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;
        let identity = PeerIdentity::from_cert_der(end_entity.as_ref())
            .map_err(|e| RustlsError::General(e.to_string()))?;
        identity
            .check_not_revoked(&self.revoked_serials_hex)
            .map_err(|e| RustlsError::General(e.to_string()))?;
        identity
            .check_expectations(
                self.expected_cluster_id.as_deref(),
                self.expected_node_id.as_deref(),
            )
            .map_err(|e| RustlsError::General(e.to_string()))?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Build a rustls client config from options.
pub fn build_client_config(options: &TlsClientOptions) -> Result<Arc<ClientConfig>, Error> {
    ensure_crypto_provider();
    let builder = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13]);

    let roots = if let Some(ref ca) = options.ca_path {
        load_roots(ca)?
    } else {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        roots
    };
    let webpki = WebPkiServerVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|e| Error::ValidationMsg(format!("server cert verifier: {e}")))?;
    let verifier = Arc::new(IdentityServerVerifier {
        inner: webpki,
        expected_cluster_id: options.expected_cluster_id.clone(),
        expected_node_id: options.expected_node_id.clone(),
        revoked_serials_hex: options.revoked_serials_hex.clone(),
    });
    let builder = builder
        .dangerous()
        .with_custom_certificate_verifier(verifier);

    let mut config =
        if let (Some(cert), Some(key)) = (&options.client_cert_path, &options.client_key_path) {
            let certs = load_certs(cert)?;
            let key = load_private_key(key)?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| Error::ValidationMsg(format!("client identity: {e}")))?
        } else {
            builder.with_no_client_auth()
        };
    config.alpn_protocols = vec![b"residiuum-rpc-v1".to_vec()];
    Ok(Arc::new(config))
}

/// Perform a TLS client handshake over a connected TCP stream.
pub fn client_connect(
    tcp: TcpStream,
    options: &TlsClientOptions,
) -> Result<(IoStream, Option<PeerIdentity>), Error> {
    let config = build_client_config(options)?;
    let server_name = ServerName::try_from(options.server_name.as_str())
        .map_err(|_| {
            Error::ValidationMsg(format!("invalid TLS server_name {:?}", options.server_name))
        })?
        .to_owned();
    let conn = ClientConnection::new(config, server_name).map_err(tls_err)?;
    let mut stream = StreamOwned::new(conn, tcp);
    while stream.conn.is_handshaking() {
        stream
            .conn
            .complete_io(&mut stream.sock)
            .map_err(map_tls_io)?;
    }
    let peer = stream
        .conn
        .peer_certificates()
        .and_then(|c| c.first())
        .map(|c| PeerIdentity::from_cert_der(c.as_ref()))
        .transpose()?;
    Ok((IoStream::TlsClient(Box::new(stream)), peer))
}

fn tls_err(e: RustlsError) -> Error {
    Error::AuthenticationFailed(format!("tls: {e}"))
}

fn map_tls_io(e: io::Error) -> Error {
    // rustls complete_io surfaces handshake failures as io errors sometimes;
    // keep a clear authentication class for cert failures when message hints.
    let msg = e.to_string();
    if msg.contains("certificate")
        || msg.contains("Certificate")
        || msg.contains("tls")
        || msg.contains("TLS")
        || msg.contains("handshake")
        || msg.contains("revoked")
        || msg.contains("cluster id")
        || msg.contains("node id")
    {
        Error::AuthenticationFailed(format!("tls handshake: {msg}"))
    } else {
        Error::from_io(e)
    }
}

/// RFC 9266 exporter label for HeapKey channel binding (`HEAP_SPEC` §33.3).
pub const CHANNEL_BINDING_EXPORTER_LABEL: &[u8] = b"EXPORTER-Channel-Binding";

/// Byte-stream transport: plaintext TCP or TLS-wrapped TCP.
pub enum IoStream {
    /// Unencrypted TCP (loopback / insecure-bind only).
    Plain(TcpStream),
    /// TLS client connection.
    TlsClient(Box<StreamOwned<ClientConnection, TcpStream>>),
    /// TLS server connection.
    TlsServer(Box<StreamOwned<ServerConnection, TcpStream>>),
}

impl IoStream {
    /// Wrap a plain TCP stream.
    pub fn plain(tcp: TcpStream) -> Self {
        Self::Plain(tcp)
    }

    /// Set read timeout on the underlying TCP socket.
    pub fn set_read_timeout(&self, timeout: Option<std::time::Duration>) -> io::Result<()> {
        match self {
            Self::Plain(s) => s.set_read_timeout(timeout),
            Self::TlsClient(s) => s.sock.set_read_timeout(timeout),
            Self::TlsServer(s) => s.sock.set_read_timeout(timeout),
        }
    }

    /// Set write timeout on the underlying TCP socket.
    pub fn set_write_timeout(&self, timeout: Option<std::time::Duration>) -> io::Result<()> {
        match self {
            Self::Plain(s) => s.set_write_timeout(timeout),
            Self::TlsClient(s) => s.sock.set_write_timeout(timeout),
            Self::TlsServer(s) => s.sock.set_write_timeout(timeout),
        }
    }

    /// Whether this stream is TLS-protected.
    pub fn is_tls(&self) -> bool {
        !matches!(self, Self::Plain(_))
    }

    /// Derive the 32-byte RFC 9266 channel-binding exporter for HeapKey proofs.
    ///
    /// Label: `EXPORTER-Channel-Binding`; empty context. Plaintext streams fail
    /// closed — the qualified profile requires TLS 1.3.
    pub fn export_channel_binding(&self) -> Result<[u8; 32], Error> {
        let mut out = [0u8; 32];
        match self {
            Self::Plain(_) => {
                return Err(Error::ProtocolViolation(
                    "channel binding requires TLS 1.3".into(),
                ));
            }
            Self::TlsClient(s) => {
                s.conn
                    .export_keying_material(&mut out, CHANNEL_BINDING_EXPORTER_LABEL, None)
                    .map_err(|e| Error::Internal(format!("tls exporter: {e}")))?;
            }
            Self::TlsServer(s) => {
                s.conn
                    .export_keying_material(&mut out, CHANNEL_BINDING_EXPORTER_LABEL, None)
                    .map_err(|e| Error::Internal(format!("tls exporter: {e}")))?;
            }
        }
        Ok(out)
    }
}

impl Read for IoStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf),
            Self::TlsClient(s) => s.read(buf),
            Self::TlsServer(s) => s.read(buf),
        }
    }
}

impl Write for IoStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            Self::TlsClient(s) => s.write(buf),
            Self::TlsServer(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            Self::TlsClient(s) => s.flush(),
            Self::TlsServer(s) => s.flush(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_basic() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_str_eq("token", "token"));
        assert!(!constant_time_str_eq("token", "tokem"));
    }

    #[test]
    fn urn_helpers() {
        assert_eq!(cluster_urn("c1"), "urn:residiuum:cluster:c1");
        assert_eq!(
            parse_cluster_id_from_uri("urn:residiuum:cluster:c1"),
            Some("c1")
        );
        assert_eq!(parse_node_id_from_uri("urn:residiuum:node:n0"), Some("n0"));
        assert!(parse_cluster_id_from_uri("https://example").is_none());
    }

    #[test]
    fn redact_never_leaks() {
        assert_eq!(redact_secret("super-secret-token"), "***");
    }
}
