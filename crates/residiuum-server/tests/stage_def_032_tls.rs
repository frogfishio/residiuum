//! DEF-032: TLS 1.3, mTLS peer identity, hostname/cluster checks, rotation.
//!
//! Uses an ephemeral private PKI (rcgen) — not system roots.

use residiuum_sdk::{
    cluster_urn, node_urn, ConnectOptions, Error, PeerIdentity, RemoteClient, Residiuum,
    TlsClientOptions, TlsServerOptions, TlsServerState,
};

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, SanType,
};
use residiuum_server::{serve_store_with, validate_bind, ServeOptions};
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn wait_for_server(bind: &str) {
    for _ in 0..100 {
        if std::net::TcpStream::connect(bind).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("server did not accept on {bind}");
}

struct Pki {
    dir: TempDir,
    ca_cert: Certificate,
    ca_key: KeyPair,
}

impl Pki {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let ca_key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, "residiuum-test-ca");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca_cert = params.self_signed(&ca_key).unwrap();
        fs::write(dir.path().join("ca.pem"), ca_cert.pem()).unwrap();
        Self {
            dir,
            ca_cert,
            ca_key,
        }
    }

    fn ca_path(&self) -> PathBuf {
        self.dir.path().join("ca.pem")
    }

    fn issue(
        &self,
        name: &str,
        dns_names: &[&str],
        cluster_id: Option<&str>,
        node_id: Option<&str>,
        not_before: Option<SystemTime>,
        not_after: Option<SystemTime>,
    ) -> (PathBuf, PathBuf, String) {
        let key = KeyPair::generate().unwrap();
        let names: Vec<String> = dns_names.iter().map(|s| (*s).to_string()).collect();
        let mut params = CertificateParams::new(names).unwrap();
        params.distinguished_name.push(DnType::CommonName, name);
        params.use_authority_key_identifier_extension = true;
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        // CertificateParams::new already adds DNS SANs from the name list; add URIs.
        if let Some(c) = cluster_id {
            params
                .subject_alt_names
                .push(SanType::URI(cluster_urn(c).try_into().unwrap()));
        }
        if let Some(n) = node_id {
            params
                .subject_alt_names
                .push(SanType::URI(node_urn(n).try_into().unwrap()));
        }
        if let Some(t) = not_before {
            params.not_before = t.into();
        }
        if let Some(t) = not_after {
            params.not_after = t.into();
        }
        let cert = params.signed_by(&key, &self.ca_cert, &self.ca_key).unwrap();
        let cert_pem = cert.pem();
        let key_pem = key.serialize_pem();
        let cert_path = self.dir.path().join(format!("{name}.crt"));
        let key_path = self.dir.path().join(format!("{name}.key"));
        fs::write(&cert_path, &cert_pem).unwrap();
        fs::write(&key_path, &key_pem).unwrap();
        let serial = {
            let der = cert.der();
            let id = PeerIdentity::from_cert_der(der).unwrap();
            id.serial_hex.unwrap()
        };
        (cert_path, key_path, serial)
    }
}

fn open_store(dir: &Path) -> PathBuf {
    let path = dir.join("app.residiuum");
    {
        let _ = Residiuum::open(&path).unwrap();
    }
    path
}

fn spawn_server(path: PathBuf, bind: &str, options: ServeOptions) -> Arc<AtomicBool> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&shutdown);
    let bind_c = bind.to_string();
    let opts = options.shutdown_flag(Arc::clone(&shutdown));
    thread::spawn(move || {
        let _ = serve_store_with(path, &bind_c, opts);
    });
    wait_for_server(bind);
    flag
}

fn assert_auth_err(result: Result<RemoteClient, Error>, ctx: &str) {
    match result {
        Ok(_) => panic!("{ctx}: expected authentication/tls failure"),
        Err(e) => {
            let s = e.to_string().to_lowercase();
            assert!(
                matches!(e, Error::AuthenticationFailed(_))
                    || s.contains("tls")
                    || s.contains("cert")
                    || s.contains("cluster")
                    || s.contains("revoked")
                    || s.contains("expired")
                    || s.contains("handshake"),
                "{ctx}: unexpected error {e}"
            );
        }
    }
}

#[test]
fn tls_happy_path_ping() {
    let pki = Pki::new();
    let (cert, key, _) = pki.issue("server", &["localhost"], Some("c1"), Some("n0"), None, None);
    let tmp = TempDir::new().unwrap();
    let store = open_store(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = spawn_server(
        store,
        &bind,
        ServeOptions::new()
            .legacy_token_server()
            .tls(TlsServerOptions::new(&cert, &key)),
    );

    let mut client = RemoteClient::connect_with(
        &bind,
        format!("residiuum://localhost:{port}/"),
        ConnectOptions::new().tls(
            TlsClientOptions::new("localhost")
                .ca_path(pki.ca_path())
                .expected_cluster_id("c1")
                .expected_node_id("n0"),
        ),
    )
    .expect("tls connect");
    client.ping().unwrap();
    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn wrong_hostname_fails() {
    let pki = Pki::new();
    let (cert, key, _) = pki.issue("server", &["localhost"], Some("c1"), None, None, None);
    let tmp = TempDir::new().unwrap();
    let store = open_store(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = spawn_server(
        store,
        &bind,
        ServeOptions::new()
            .legacy_token_server()
            .tls(TlsServerOptions::new(&cert, &key)),
    );

    assert_auth_err(
        RemoteClient::connect_with(
            &bind,
            format!("residiuum://localhost:{port}/"),
            ConnectOptions::new()
                .tls(TlsClientOptions::new("not-localhost").ca_path(pki.ca_path())),
        ),
        "wrong host",
    );
    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn wrong_cluster_id_fails() {
    let pki = Pki::new();
    let (cert, key, _) = pki.issue(
        "server",
        &["localhost"],
        Some("cluster-a"),
        None,
        None,
        None,
    );
    let tmp = TempDir::new().unwrap();
    let store = open_store(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = spawn_server(
        store,
        &bind,
        ServeOptions::new()
            .legacy_token_server()
            .tls(TlsServerOptions::new(&cert, &key)),
    );

    assert_auth_err(
        RemoteClient::connect_with(
            &bind,
            format!("residiuum://localhost:{port}/"),
            ConnectOptions::new().tls(
                TlsClientOptions::new("localhost")
                    .ca_path(pki.ca_path())
                    .expected_cluster_id("cluster-b"),
            ),
        ),
        "wrong cluster",
    );
    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn expired_certificate_fails() {
    let pki = Pki::new();
    let past = SystemTime::now() - Duration::from_secs(3600 * 24 * 30);
    let also_past = SystemTime::now() - Duration::from_secs(3600 * 24);
    let (cert, key, _) = pki.issue(
        "expired",
        &["localhost"],
        Some("c1"),
        None,
        Some(past),
        Some(also_past),
    );
    let tmp = TempDir::new().unwrap();
    let store = open_store(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = spawn_server(
        store,
        &bind,
        ServeOptions::new()
            .legacy_token_server()
            .tls(TlsServerOptions::new(&cert, &key)),
    );

    assert_auth_err(
        RemoteClient::connect_with(
            &bind,
            format!("residiuum://localhost:{port}/"),
            ConnectOptions::new().tls(TlsClientOptions::new("localhost").ca_path(pki.ca_path())),
        ),
        "expired cert",
    );
    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn mitm_wrong_ca_fails() {
    let pki_good = Pki::new();
    let pki_evil = Pki::new();
    let (cert, key, _) = pki_evil.issue("evil", &["localhost"], Some("c1"), None, None, None);
    let tmp = TempDir::new().unwrap();
    let store = open_store(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = spawn_server(
        store,
        &bind,
        ServeOptions::new()
            .legacy_token_server()
            .tls(TlsServerOptions::new(&cert, &key)),
    );

    assert_auth_err(
        RemoteClient::connect_with(
            &bind,
            format!("residiuum://localhost:{port}/"),
            ConnectOptions::new()
                .tls(TlsClientOptions::new("localhost").ca_path(pki_good.ca_path())),
        ),
        "MITM / wrong CA",
    );
    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn revoked_serial_fails() {
    let pki = Pki::new();
    let (cert, key, serial) = pki.issue("server", &["localhost"], Some("c1"), None, None, None);
    let tmp = TempDir::new().unwrap();
    let store = open_store(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = spawn_server(
        store,
        &bind,
        ServeOptions::new()
            .legacy_token_server()
            .tls(TlsServerOptions::new(&cert, &key)),
    );

    assert_auth_err(
        RemoteClient::connect_with(
            &bind,
            format!("residiuum://localhost:{port}/"),
            ConnectOptions::new().tls(
                TlsClientOptions::new("localhost")
                    .ca_path(pki.ca_path())
                    .revoke_serials_hex([serial]),
            ),
        ),
        "revoked serial",
    );
    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn mtls_requires_client_cert() {
    let pki = Pki::new();
    let (srv_cert, srv_key, _) =
        pki.issue("server", &["localhost"], Some("c1"), Some("n0"), None, None);
    let (cli_cert, cli_key, _) = pki.issue(
        "client",
        &["client.local"],
        Some("c1"),
        Some("client"),
        None,
        None,
    );
    let tmp = TempDir::new().unwrap();
    let store = open_store(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = spawn_server(
        store,
        &bind,
        ServeOptions::new().legacy_token_server().tls(
            TlsServerOptions::new(&srv_cert, &srv_key)
                .with_client_ca(pki.ca_path())
                .expected_cluster_id("c1"),
        ),
    );

    assert_auth_err(
        RemoteClient::connect_with(
            &bind,
            format!("residiuum://localhost:{port}/"),
            ConnectOptions::new().tls(TlsClientOptions::new("localhost").ca_path(pki.ca_path())),
        ),
        "mTLS without client cert",
    );

    let mut client = RemoteClient::connect_with(
        &bind,
        format!("residiuum://localhost:{port}/"),
        ConnectOptions::new().tls(
            TlsClientOptions::new("localhost")
                .ca_path(pki.ca_path())
                .client_identity(&cli_cert, &cli_key)
                .expected_cluster_id("c1"),
        ),
    )
    .expect("mtls connect");
    client.ping().unwrap();
    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn cert_rotation_keeps_new_handshakes_healthy() {
    let pki = Pki::new();
    let (cert_a, key_a, _) = pki.issue("a", &["localhost"], Some("c1"), Some("n0"), None, None);
    let (cert_b, key_b, _) = pki.issue("b", &["localhost"], Some("c1"), Some("n0"), None, None);

    let live_cert = pki.dir.path().join("live.crt");
    let live_key = pki.dir.path().join("live.key");
    fs::copy(&cert_a, &live_cert).unwrap();
    fs::copy(&key_a, &live_key).unwrap();

    let tmp = TempDir::new().unwrap();
    let store = open_store(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let slot = Arc::new(Mutex::new(None));
    let shutdown = spawn_server(
        store,
        &bind,
        ServeOptions::new()
            .legacy_token_server()
            .tls(TlsServerOptions::new(&live_cert, &live_key))
            .tls_state_slot(Arc::clone(&slot)),
    );

    let state = {
        let mut s = None;
        for _ in 0..100 {
            if let Ok(g) = slot.lock() {
                if g.is_some() {
                    s = g.clone();
                    break;
                }
            }
            thread::sleep(Duration::from_millis(20));
        }
        s.expect("tls state published")
    };

    let connect = || {
        RemoteClient::connect_with(
            &bind,
            format!("residiuum://localhost:{port}/"),
            ConnectOptions::new().tls(
                TlsClientOptions::new("localhost")
                    .ca_path(pki.ca_path())
                    .expected_cluster_id("c1"),
            ),
        )
    };

    connect().expect("pre-rotation").ping().unwrap();

    fs::copy(&cert_b, &live_cert).unwrap();
    fs::copy(&key_b, &live_key).unwrap();
    state.reload().expect("reload");

    connect().expect("post-rotation").ping().unwrap();
    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn plaintext_loopback_still_works() {
    let tmp = TempDir::new().unwrap();
    let store = open_store(tmp.path());
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let shutdown = spawn_server(store, &bind, ServeOptions::new().legacy_token_server());
    let mut client = RemoteClient::connect_with(
        &bind,
        format!("residiuum://127.0.0.1:{port}/"),
        ConnectOptions::new(),
    )
    .unwrap();
    client.ping().unwrap();
    shutdown.store(true, Ordering::SeqCst);
}

#[test]
fn public_bind_allowed_with_tls() {
    validate_bind("0.0.0.0:7434", false, true).unwrap();
    assert!(validate_bind("0.0.0.0:7434", false, false).is_err());
}

#[test]
fn constant_time_and_redact() {
    assert!(residiuum_sdk::constant_time_str_eq("abc", "abc"));
    assert!(!residiuum_sdk::constant_time_str_eq("abc", "abd"));
    assert_eq!(residiuum_sdk::redact_secret("super-secret"), "***");
    let pki = Pki::new();
    let (cert, key, _) = pki.issue("s", &["localhost"], None, None, None, None);
    let _ = TlsServerState::from_options(TlsServerOptions::new(cert, key)).unwrap();
}
