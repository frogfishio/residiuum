//! DEF-054 — safe configuration management.
//!
//! Covers:
//! - versioned `residiuum-config-v1` schema load
//! - validation before serve (missing paths, bad ranges)
//! - unsafe combination: replication claim with insufficient nodes
//! - unsafe combination: public plaintext bind without opt-in
//! - secret resolution via env / file refs (no inline secrets)
//! - redacted effective configuration report
//! - apply to ServeOptions (admission + limits)

use residiuum_server::{
    load_and_validate, redact_json_value, resolve_secret_ref, setting_class, validate_document,
    ClusterConfigSection, ConfigError, ConfigLayer, ConfigMode, ConfigOverrides,
    ResidiuumConfigFile, ServeConfigSection, SettingClass, StoreConfigSection, CONFIG_PROFILE,
};
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

#[test]
fn profile_and_setting_classes() {
    assert_eq!(CONFIG_PROFILE, "residiuum-config-v1");
    assert_eq!(setting_class("store.path"), Some(SettingClass::Static));
    assert_eq!(
        setting_class("serve.max_connections"),
        Some(SettingClass::RestartRequired)
    );
    assert_eq!(
        setting_class("serve.admission.global_max_rps"),
        Some(SettingClass::Dynamic)
    );
}

#[test]
fn load_validate_apply_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("node.json");
    fs::write(
        &path,
        r#"{
          "format": "residiuum-config-v1",
          "format_version": 1,
          "comment": "stage_def_054 fixture",
          "store": { "path": "/data/store", "durability_default": "durable" },
          "serve": {
            "bind": "127.0.0.1:9123",
            "max_connections": 12,
            "idle_timeout_secs": 30,
            "token_env": "RESIDIUUM_TEST_STAGE054_ABSENT",
            "admission": {
              "global_max_rps": 77,
              "per_principal_max_rps": 11,
              "max_expensive_concurrent": 2
            }
          }
        }"#,
    )
    .unwrap();

    let v = load_and_validate(Some(&path), ConfigMode::Serve, ConfigOverrides::default())
        .expect("valid config");
    assert_eq!(v.bind, "127.0.0.1:9123");
    assert_eq!(v.server_limits.max_connections, 12);
    assert_eq!(v.server_limits.idle_timeout.as_secs(), 30);
    assert_eq!(v.admission_limits.global_max_rps, 77);
    assert_eq!(v.admission_limits.per_principal_max_rps, 11);
    assert_eq!(v.admission_limits.max_expensive_concurrent, 2);
    assert_eq!(v.sources.bind, ConfigLayer::File);
    assert!(v.auth_token.is_none());

    let opts =
        v.apply_to_serve_options(residiuum_server::ServeOptions::new().legacy_token_server());
    assert_eq!(opts.server_limits.max_connections, 12);
    assert_eq!(opts.admission_limits.global_max_rps, 77);

    let report = v.effective_report(ConfigMode::Serve);
    assert_eq!(report.profile, CONFIG_PROFILE);
    assert!(report
        .settings
        .iter()
        .any(|s| s.path == "serve.auth_token" && s.value == "<unset>"));
    assert!(report
        .settings
        .iter()
        .any(|s| s.path == "serve.bind" && s.value == "127.0.0.1:9123"));
}

#[test]
fn flag_overrides_file_bind() {
    let doc = ResidiuumConfigFile {
        format: CONFIG_PROFILE.into(),
        format_version: 1,
        comment: None,
        store: Some(StoreConfigSection {
            path: Some(PathBuf::from("/data")),
            durability_default: None,
        }),
        serve: Some(ServeConfigSection {
            bind: Some("127.0.0.1:1".into()),
            token_env: Some("RESIDIUUM_TEST_STAGE054_ABSENT".into()),
            ..Default::default()
        }),
        cluster: None,
    };
    let v = validate_document(
        doc,
        None,
        ConfigMode::Serve,
        ConfigOverrides {
            bind: Some("127.0.0.1:9999".into()),
            max_connections: Some(3),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(v.bind, "127.0.0.1:9999");
    assert_eq!(v.sources.bind, ConfigLayer::Flag);
    assert_eq!(v.server_limits.max_connections, 3);
    assert_eq!(v.sources.max_connections, ConfigLayer::Flag);
}

#[test]
fn refuse_replication_claim_single_copy() {
    let doc = ResidiuumConfigFile {
        format: CONFIG_PROFILE.into(),
        format_version: 1,
        comment: None,
        store: None,
        serve: Some(ServeConfigSection {
            experimental_network_cluster: Some(true),
            token_env: Some("RESIDIUUM_TEST_STAGE054_ABSENT".into()),
            ..Default::default()
        }),
        cluster: Some(ClusterConfigSection {
            root: Some(PathBuf::from("/cluster")),
            expected_node_count: Some(1),
            claim_replication: Some(true),
            ..Default::default()
        }),
    };
    let err = validate_document(
        doc,
        None,
        ConfigMode::ServeCluster,
        ConfigOverrides::default(),
    )
    .unwrap_err();
    match err {
        ConfigError::Unsafe { code, detail } => {
            assert_eq!(code, "replication_claim_insufficient_nodes");
            assert!(detail.contains("expected_node_count"), "{detail}");
        }
        other => panic!("expected Unsafe, got {other:?}"),
    }
}

#[test]
fn har4_t3_cohost_legacy_and_qualified_refused() {
    let doc = ResidiuumConfigFile {
        format: CONFIG_PROFILE.into(),
        format_version: 1,
        comment: None,
        store: Some(StoreConfigSection {
            path: Some(PathBuf::from("/data")),
            durability_default: None,
        }),
        serve: Some(ServeConfigSection {
            bind: Some("127.0.0.1:9".into()),
            legacy_token_server: Some(true),
            qualified_heap_key: Some(true),
            deployment_id: Some("00000000-0000-4000-8000-000000000001".into()),
            token_env: Some("RESIDIUUM_TEST_STAGE054_ABSENT".into()),
            ..Default::default()
        }),
        cluster: None,
    };
    let err =
        validate_document(doc, None, ConfigMode::Serve, ConfigOverrides::default()).unwrap_err();
    match err {
        ConfigError::Unsafe { code, detail } => {
            assert_eq!(code, "auth_path_cohost");
            assert!(
                detail.contains("HAR-4") || detail.contains("co-host"),
                "{detail}"
            );
        }
        other => panic!("expected co-host Unsafe, got {other:?}"),
    }
}

#[test]
fn har4_t3_qualified_requires_tls_and_deployment_id() {
    let doc = ResidiuumConfigFile {
        format: CONFIG_PROFILE.into(),
        format_version: 1,
        comment: None,
        store: Some(StoreConfigSection {
            path: Some(PathBuf::from("/data")),
            durability_default: None,
        }),
        serve: Some(ServeConfigSection {
            bind: Some("127.0.0.1:9".into()),
            qualified_heap_key: Some(true),
            token_env: Some("RESIDIUUM_TEST_STAGE054_ABSENT".into()),
            ..Default::default()
        }),
        cluster: None,
    };
    let err =
        validate_document(doc, None, ConfigMode::Serve, ConfigOverrides::default()).unwrap_err();
    match err {
        ConfigError::Unsafe { code, .. } => {
            assert!(
                code == "qualified_requires_tls" || code == "qualified_requires_deployment_id",
                "code={code}"
            );
        }
        other => panic!("expected Unsafe, got {other:?}"),
    }
}

#[test]
fn har4_t3_legacy_apply_and_report_labels() {
    let doc = ResidiuumConfigFile {
        format: CONFIG_PROFILE.into(),
        format_version: 1,
        comment: None,
        store: Some(StoreConfigSection {
            path: Some(PathBuf::from("/data")),
            durability_default: None,
        }),
        serve: Some(ServeConfigSection {
            bind: Some("127.0.0.1:9123".into()),
            legacy_token_server: Some(true),
            token_env: Some("RESIDIUUM_TEST_STAGE054_ABSENT".into()),
            ..Default::default()
        }),
        cluster: None,
    };
    let v = validate_document(doc, None, ConfigMode::Serve, ConfigOverrides::default()).unwrap();
    assert!(v.legacy_token_server);
    assert!(!v.qualified_heap_key);
    let opts = v.apply_to_serve_options(residiuum_server::ServeOptions::new());
    assert!(opts.legacy_token_server);
    assert!(!opts.qualified_heap_key);
    let report = v.effective_report(ConfigMode::Serve);
    assert!(report
        .settings
        .iter()
        .any(|s| s.path == "serve.auth_path" && s.value.contains("legacy")));
    assert!(report
        .settings
        .iter()
        .any(|s| s.path == "serve.legacy_token_server" && s.value == "true"));
}

#[test]
fn refuse_public_plaintext_without_opt_in() {
    let doc = ResidiuumConfigFile {
        format: CONFIG_PROFILE.into(),
        format_version: 1,
        comment: None,
        store: Some(StoreConfigSection {
            path: Some(PathBuf::from("/data")),
            durability_default: None,
        }),
        serve: Some(ServeConfigSection {
            bind: Some("0.0.0.0:7434".into()),
            allow_insecure_bind: Some(false),
            token_env: Some("RESIDIUUM_TEST_STAGE054_ABSENT".into()),
            ..Default::default()
        }),
        cluster: None,
    };
    let err =
        validate_document(doc, None, ConfigMode::Serve, ConfigOverrides::default()).unwrap_err();
    assert!(matches!(
        err,
        ConfigError::Unsafe {
            code: "insecure_bind",
            ..
        }
    ));
}

#[test]
fn secret_file_ref_and_redaction() {
    let dir = tempdir().unwrap();
    let secret = dir.path().join("token");
    fs::write(&secret, "super-secret\n").unwrap();
    let got = resolve_secret_ref(&format!("file:{}", secret.display())).unwrap();
    assert_eq!(got, "super-secret");

    let mut raw = serde_json::json!({
        "auth_token": "should-hide",
        "token_env": "RESIDIUUM_TOKEN",
        "nested": { "password": "x", "bind": "127.0.0.1:1" }
    });
    redact_json_value(&mut raw);
    assert_eq!(raw["auth_token"], "[redacted]");
    assert_eq!(raw["token_env"], "RESIDIUUM_TOKEN");
    assert_eq!(raw["nested"]["password"], "[redacted]");
    assert_eq!(raw["nested"]["bind"], "127.0.0.1:1");
}

#[test]
fn effective_report_never_leaks_token() {
    let v = validate_document(
        ResidiuumConfigFile {
            format: CONFIG_PROFILE.into(),
            format_version: 1,
            comment: None,
            store: Some(StoreConfigSection {
                path: Some(PathBuf::from("/data")),
                durability_default: Some("durable".into()),
            }),
            serve: Some(ServeConfigSection {
                token_env: Some("RESIDIUUM_TEST_STAGE054_ABSENT".into()),
                ..Default::default()
            }),
            cluster: None,
        },
        None,
        ConfigMode::Serve,
        ConfigOverrides {
            auth_token: Some("literally-a-secret".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(v.auth_token.as_deref(), Some("literally-a-secret"));
    let report = v.effective_report(ConfigMode::Serve);
    let json = serde_json::to_string(&report).unwrap();
    assert!(!json.contains("literally-a-secret"), "leaked: {json}");
    assert!(json.contains("[redacted]"), "json={json}");
}
