//! Residiuum CLI (Stage 7): put/get/list, doctor, salvage, serve.

use clap::{ArgAction, Parser, Subcommand};
use residiuum_examine::{examine_store, ExaminationUnit, ExamineLimits};
use residiuum_sdk::Residiuum;
use residiuum_server::{
    load_and_validate, serve_cluster_node, serve_store_with, ConfigMode, ConfigOverrides,
    ServeOptions, CONFIG_PROFILE,
};
use residiuum_store::{
    load_migration_job, migrate_rollback, restore_full_backup, MigrateOptions, RestoreOptions,
    ScrubOptions, Store, MIGRATE_PROFILE,
};
use serde_json::{json as sjson, Value as JsonValue};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
mod console;
use std::process::ExitCode;

const APP_VERSION: &str = concat!(
    env!("RESIDIUUM_VERSION"),
    "-build ",
    env!("RESIDIUUM_BUILD")
);
const CLI_ABOUT: &str = "Residiuum command-line interface";
const CLI_LONG_ABOUT: &str = "Residiuum command-line interface\n\nEveryday put/get/list, read-only doctor diagnostics, evidence-preserving salvage (and explicit export-live materialization), full backup/restore with verified manifests (DEF-050), integrity scrub (DEF-051), format migration preflight/plan/apply/verify/rollback (DEF-052), versioned config validate/show (DEF-054), single-node TCP serve (development), and experimental multi-node serve-cluster (Raft control + data-plane commit when attached; not production-ready).";
const LICENSE_TEXT: &str = "Copyright (c) 2026 Alexander R. Croft\nGNU Affero General Public License v3.0 or later\n\nThis program (`residiuum`) is offered under the AGPL-3.0-or-later.\nSee LICENSE-AGPL-3.0 and doc/reference/operations/LICENSING.md in the repository for full terms.\n\nResidiuum is multi-licensed by crate: MIT (SDA/format), MPL-2.0 (store/examine),\nAGPL-3.0-or-later (cluster, server, this CLI; SDK remains AGPL until embedded-only).";

#[derive(Parser)]
#[command(
    name = "residiuum",
    version = APP_VERSION,
    about = CLI_ABOUT,
    long_about = CLI_LONG_ABOUT,
    disable_help_subcommand = true,
    disable_version_flag = true,
    next_line_help = true,
)]
struct Cli {
    /// Print the shipped semantic version and build number.
    #[arg(long = "version", global = true, action = ArgAction::SetTrue)]
    version_flag: bool,

    /// Print copyright and license information.
    #[arg(long, global = true, action = ArgAction::SetTrue)]
    license: bool,

    /// Emit JSON on stdout (stable machine-readable). Distinct from put `--json` body.
    #[arg(long = "json-out", global = true, action = ArgAction::SetTrue)]
    json_out: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Put a JSON document: `residiuum put PATH COLL/KEY --json '...'`
    Put {
        /// Store directory path.
        store: PathBuf,
        /// Collection/key path (`users/user-42`).
        target: String,
        /// JSON document body.
        #[arg(long = "json")]
        json_body: String,
    },
    /// Get a JSON document: `residiuum get PATH COLL/KEY`
    Get { store: PathBuf, target: String },
    /// Delete a key: `residiuum delete PATH COLL/KEY`
    Delete { store: PathBuf, target: String },
    /// List collections, or keys in a collection.
    List {
        store: PathBuf,
        /// Optional collection name.
        collection: Option<String>,
    },
    /// Put raw bytes from a file: `residiuum put-bytes PATH COLL/KEY FILE`
    PutBytes {
        store: PathBuf,
        target: String,
        file: PathBuf,
    },
    /// Show event history for a key (embedded).
    History { store: PathBuf, target: String },
    /// Minimal interactive console.
    ///
    /// Invocation: `residiuum console ./store`
    Console {
        /// Store directory.
        store: PathBuf,
    },
    /// Read-only store health report (DX_SPEC §13.3).
    Doctor { store: PathBuf },
    /// Evidence-preserving salvage to a new path (DX_SPEC §13.4, DEF-011).
    ///
    /// Copies verified frames byte-identically and writes a recovery manifest.
    /// Does not re-encode live values. For a clean current-state database with
    /// new lineage, use `export-live` instead.
    Salvage {
        /// Source store (never mutated).
        store: PathBuf,
        /// Destination store path (must not already be a store).
        #[arg(long = "output", short = 'o')]
        output: PathBuf,
    },
    /// Export live logical state to a new store (DEF-011 materialization).
    ///
    /// Re-appends complete live payloads with **new** event lineage. History,
    /// tombstones, partials, and holes are not preserved. Prefer `salvage` when
    /// examination evidence must survive.
    ExportLive {
        /// Source store (never mutated).
        store: PathBuf,
        /// Destination store path (must not already be a store).
        #[arg(long = "output", short = 'o')]
        output: PathBuf,
    },
    /// Full backup package with verified content hashes (DEF-050).
    ///
    /// Copies authoritative store trees into a package directory with a hashed
    /// `backup-manifest.v1.json`. Distinct from `salvage` (damage recovery) and
    /// `export-live` (new lineage). Opens the source exclusively and flushes
    /// durable state first when no other writer holds the lock.
    Backup {
        /// Source store directory.
        store: PathBuf,
        /// Backup package directory (must not already exist / must be empty).
        #[arg(long = "output", short = 'o')]
        output: PathBuf,
    },
    /// Restore a verified backup package into a new store path (DEF-050).
    ///
    /// Verifies the package manifest and every file blake3 before materializing.
    /// Default preserves store identity; `--reassign-identity` mints a new
    /// `store_id` for clones.
    Restore {
        /// Backup package directory (contains `backup-manifest.v1.json`).
        backup: PathBuf,
        /// Destination store path (must not already be a store).
        #[arg(long = "output", short = 'o')]
        output: PathBuf,
        /// Mint a new store identity (clone) instead of preserving the original.
        #[arg(long = "reassign-identity", action = ArgAction::SetTrue)]
        reassign_identity: bool,
    },
    /// Bounded integrity scrub of segments and chunks (DEF-051).
    ///
    /// Hashes files, compares placement content hashes when known, and frame-
    /// scans segments. Persists frontier and findings under `recovery/scrub/`.
    /// Corrupt evidence may be quarantined without removing the original.
    /// Default runs to completion under per-step bounds; use `--once` for a
    /// single bounded step, `--status` for a read-only snapshot.
    Scrub {
        /// Store directory.
        store: PathBuf,
        /// Print durable scrub status only (no progress).
        #[arg(long = "status", action = ArgAction::SetTrue)]
        status_only: bool,
        /// Run a single bounded step instead of scrubbing to completion.
        #[arg(long = "once", action = ArgAction::SetTrue)]
        once: bool,
        /// Pause scrub so further steps no-op until `--resume`.
        #[arg(long = "pause", action = ArgAction::SetTrue)]
        pause: bool,
        /// Resume a paused scrub.
        #[arg(long = "resume", action = ArgAction::SetTrue)]
        resume: bool,
        /// Max files verified per step (default 32).
        #[arg(long = "max-files", default_value_t = 32)]
        max_files: usize,
        /// Max bytes hashed/scanned per step (default 67108864).
        #[arg(long = "max-bytes", default_value_t = 67_108_864)]
        max_bytes: u64,
        /// Skip quarantining corrupt evidence copies.
        #[arg(long = "no-quarantine", action = ArgAction::SetTrue)]
        no_quarantine: bool,
    },
    /// Format migration with explicit phases (DEF-052).
    ///
    /// Never rewrites the source in place. Copies authoritative trees into a
    /// new destination store, preserves unsupported/unreadable segment bytes,
    /// and records a durable job under `recovery/migration/`. Default runs
    /// preflight → plan → apply → verify. Use `--plan-only` to stop after the
    /// plan, `--preflight` for a read-only matrix/classification report, or
    /// `--rollback` to abandon an incomplete destination.
    Migrate {
        /// Source store directory (never rewritten in place).
        store: PathBuf,
        /// Destination store path (must not already be a store).
        #[arg(long = "output", short = 'o')]
        output: Option<PathBuf>,
        /// Print preflight only (version matrix + classification; no job write
        /// unless combined with a full run). Requires `--output`.
        #[arg(long = "preflight", action = ArgAction::SetTrue)]
        preflight: bool,
        /// Stop after writing the durable plan (no destination bytes).
        #[arg(long = "plan-only", action = ArgAction::SetTrue)]
        plan_only: bool,
        /// Apply without verify (operator may verify later).
        #[arg(long = "skip-verify", action = ArgAction::SetTrue)]
        skip_verify: bool,
        /// Print durable migration job status from the source store.
        #[arg(long = "status", action = ArgAction::SetTrue)]
        status_only: bool,
        /// Rollback an incomplete migration (refuses completed destinations).
        #[arg(long = "rollback", action = ArgAction::SetTrue)]
        rollback: bool,
    },
    /// Serve the store over TCP for remote clients.
    ///
    /// **HAR-4 T2 auth path:** product default is qualified HeapKey
    /// (`--qualified-heap-key` with TLS + `--deployment-id`). Non-product
    /// open/token Stage-7 posture requires explicit `--legacy-token-server`.
    /// Co-host of both paths is refused. Defaults to loopback. Non-loopback
    /// plaintext binds require `--allow-insecure-bind`. Optional `--config`
    /// loads a `residiuum-config-v1` document; CLI flags override the file
    /// (DEF-054).
    Serve {
        /// Store directory path (overrides `store.path` from `--config`).
        store: PathBuf,
        /// Optional versioned config file (`residiuum-config-v1`, DEF-054).
        #[arg(long = "config", short = 'c')]
        config: Option<PathBuf>,
        /// Bind address (default `127.0.0.1:7434`, or `serve.bind` from config).
        #[arg(long = "bind")]
        bind: Option<String>,
        /// Optional shared auth token (clients must pass the same via ConnectOptions).
        /// Also accepted from the `RESIDIUUM_TOKEN` environment variable when the flag is omitted.
        /// Requires `--legacy-token-server` (incompatible with qualified HeapKey).
        #[arg(long = "token")]
        token: Option<String>,
        /// Allow non-loopback plaintext bind (development only).
        #[arg(long = "allow-insecure-bind", action = ArgAction::SetTrue)]
        allow_insecure_bind: bool,
        /// Max simultaneous client connections (DEF-030; default 64).
        #[arg(long = "max-connections")]
        max_connections: Option<usize>,
        /// PEM certificate chain for TLS 1.3 (DEF-032). Requires `--tls-key`.
        #[arg(long = "tls-cert")]
        tls_cert: Option<PathBuf>,
        /// PEM private key for TLS 1.3 (DEF-032). Requires `--tls-cert`.
        #[arg(long = "tls-key")]
        tls_key: Option<PathBuf>,
        /// PEM CA bundle to verify client certificates (mTLS).
        #[arg(long = "tls-client-ca")]
        tls_client_ca: Option<PathBuf>,
        /// Expected peer cluster id (`urn:residiuum:cluster:…` SAN).
        #[arg(long = "tls-cluster-id")]
        tls_cluster_id: Option<String>,
        /// Product path: qualified HeapKey TLS listener (HAR-4). Requires TLS +
        /// `--deployment-id`. Incompatible with `--legacy-token-server` / `--token`.
        #[arg(long = "qualified-heap-key", action = ArgAction::SetTrue)]
        qualified_heap_key: bool,
        /// Non-product open/token Stage-7 path (HAR-4). Explicit opt-in; not a
        /// product remote posture claim. Required when not using qualified HeapKey.
        #[arg(long = "legacy-token-server", action = ArgAction::SetTrue)]
        legacy_token_server: bool,
        /// Canonical deployment UUID string for HeapKey challenges (qualified path).
        #[arg(long = "deployment-id")]
        deployment_id: Option<String>,
    },
    /// Serve one node of a multi-node cluster root (**experimental**).
    ///
    /// Requires `--experimental-network-cluster`. When Raft attaches (default),
    /// collection put/delete use partition propose and acks report `committed`
    /// only after quorum (DEF-036/037). If attach fails, directory-only routing
    /// applies writes to this node alone. Not production-ready. Prefer
    /// in-process `Residiuum::open_cluster` for deterministic multi-replica tests.
    ///
    /// Optional `--config` loads a `residiuum-config-v1` document (DEF-054).
    ///
    /// Example:
    /// `residiuum serve-cluster ./cluster --node 0 --bind 127.0.0.1:7434 --experimental-network-cluster`
    ServeCluster {
        /// Cluster root (contains cluster.json, placement.json, nodes/).
        cluster: PathBuf,
        /// Optional versioned config file (`residiuum-config-v1`, DEF-054).
        #[arg(long = "config", short = 'c')]
        config: Option<PathBuf>,
        /// Dense node index to serve (`nodes/node-N`).
        #[arg(long = "node")]
        node: Option<u32>,
        /// Bind address (default `127.0.0.1:7434`, or `serve.bind` from config).
        #[arg(long = "bind")]
        bind: Option<String>,
        /// Optional shared auth token (also `RESIDIUUM_TOKEN`).
        #[arg(long = "token")]
        token: Option<String>,
        /// Allow non-loopback plaintext bind (development only).
        #[arg(long = "allow-insecure-bind", action = ArgAction::SetTrue)]
        allow_insecure_bind: bool,
        /// Required opt-in: network serve-cluster is experimental (DEF-002).
        #[arg(long = "experimental-network-cluster", action = ArgAction::SetTrue)]
        experimental_network_cluster: bool,
        /// PEM certificate chain for TLS 1.3 (DEF-032). Requires `--tls-key`.
        #[arg(long = "tls-cert")]
        tls_cert: Option<PathBuf>,
        /// PEM private key for TLS 1.3 (DEF-032). Requires `--tls-cert`.
        #[arg(long = "tls-key")]
        tls_key: Option<PathBuf>,
        /// PEM CA bundle to verify client certificates (mTLS).
        #[arg(long = "tls-client-ca")]
        tls_client_ca: Option<PathBuf>,
        /// Expected peer cluster id (`urn:residiuum:cluster:…` SAN).
        #[arg(long = "tls-cluster-id")]
        tls_cluster_id: Option<String>,
        /// Product path: qualified HeapKey TLS listener (HAR-4). Requires TLS +
        /// `--deployment-id`.
        #[arg(long = "qualified-heap-key", action = ArgAction::SetTrue)]
        qualified_heap_key: bool,
        /// Non-product open/token path (HAR-4). Explicit opt-in.
        #[arg(long = "legacy-token-server", action = ArgAction::SetTrue)]
        legacy_token_server: bool,
        /// Canonical deployment UUID for HeapKey challenges (qualified path).
        #[arg(long = "deployment-id")]
        deployment_id: Option<String>,
    },
    /// Validate or show a versioned configuration document (DEF-054).
    ///
    /// Config files use profile `residiuum-config-v1`. Secrets must never appear
    /// inline: use `serve.token_env` / `serve.token_secret_ref` (env: or file:).
    /// `validate` fails on unsafe combinations (e.g. replication claim with one
    /// local copy). `show` prints a redacted effective report.
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// List collection names (alias of `list` without collection).
    Collections { store: PathBuf },
}

/// Subcommands under `residiuum config` (DEF-054).
#[derive(Subcommand)]
enum ConfigAction {
    /// Validate a config file (schema, ranges, unsafe combinations).
    Validate {
        /// Path to a `residiuum-config-v1` JSON document.
        file: PathBuf,
        /// Validation mode: `serve`, `serve-cluster`, or `validate` (default).
        #[arg(long = "mode", default_value = "validate")]
        mode: String,
    },
    /// Print the redacted effective configuration report.
    Show {
        /// Path to a `residiuum-config-v1` JSON document.
        file: PathBuf,
        /// Mode used when resolving required paths (`serve` / `serve-cluster` / `validate`).
        #[arg(long = "mode", default_value = "validate")]
        mode: String,
    },
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let cli = Cli::parse();
    if cli.version_flag {
        println!("{APP_VERSION}");
        return Ok(());
    }
    if cli.license {
        println!("{LICENSE_TEXT}");
        return Ok(());
    }
    let Some(cmd) = cli.command else {
        return Err("missing command; try `residiuum --help`".into());
    };
    let json_out = cli.json_out;
    match cmd {
        Command::Put {
            store,
            target,
            json_body,
        } => cmd_put(&store, &target, &json_body, json_out),
        Command::Get { store, target } => cmd_get(&store, &target, json_out),
        Command::Delete { store, target } => cmd_delete(&store, &target, json_out),
        Command::List { store, collection } => cmd_list(&store, collection.as_deref(), json_out),
        Command::Console { store } => console::run_console(&store),
        Command::PutBytes {
            store,
            target,
            file,
        } => cmd_put_bytes(&store, &target, &file, json_out),
        Command::History { store, target } => cmd_history(&store, &target, json_out),
        Command::Doctor { store } => cmd_doctor(&store, json_out),
        Command::Salvage { store, output } => cmd_salvage(&store, &output, json_out),
        Command::ExportLive { store, output } => cmd_export_live(&store, &output, json_out),
        Command::Backup { store, output } => cmd_backup(&store, &output, json_out),
        Command::Restore {
            backup,
            output,
            reassign_identity,
        } => cmd_restore(&backup, &output, reassign_identity, json_out),
        Command::Migrate {
            store,
            output,
            preflight,
            plan_only,
            skip_verify,
            status_only,
            rollback,
        } => cmd_migrate(
            &store,
            output.as_deref(),
            preflight,
            plan_only,
            skip_verify,
            status_only,
            rollback,
            json_out,
        ),
        Command::Scrub {
            store,
            status_only,
            once,
            pause,
            resume,
            max_files,
            max_bytes,
            no_quarantine,
        } => cmd_scrub(
            &store,
            status_only,
            once,
            pause,
            resume,
            max_files,
            max_bytes,
            no_quarantine,
            json_out,
        ),
        Command::Serve {
            store,
            config,
            bind,
            token,
            allow_insecure_bind,
            max_connections,
            tls_cert,
            tls_key,
            tls_client_ca,
            tls_cluster_id,
            qualified_heap_key,
            legacy_token_server,
            deployment_id,
        } => {
            let overrides = ConfigOverrides {
                bind,
                store_path: Some(store.clone()),
                auth_token: token,
                allow_insecure_bind: if allow_insecure_bind {
                    Some(true)
                } else {
                    None
                },
                max_connections,
                tls_cert,
                tls_key,
                tls_client_ca,
                tls_cluster_id,
                legacy_token_server: if legacy_token_server {
                    Some(true)
                } else {
                    None
                },
                qualified_heap_key: if qualified_heap_key { Some(true) } else { None },
                deployment_id,
                ..Default::default()
            };
            let validated = load_and_validate(config.as_deref(), ConfigMode::Serve, overrides)
                .map_err(|e| e.to_string())?;
            let bind = validated.bind.clone();
            let store_path = validated.store_path.clone().unwrap_or(store);
            let _ = json_out; // serve is long-running; use `residiuum config show` for reports
            for w in &validated.warnings {
                eprintln!("config warning: {w}");
            }
            // HAR-4 T3: config validation already resolved auth path; apply it.
            let opts = validated.apply_to_serve_options(ServeOptions::new());
            serve_store_with(&store_path, &bind, opts).map_err(|e| e.to_string())
        }
        Command::ServeCluster {
            cluster,
            config,
            node,
            bind,
            token,
            allow_insecure_bind,
            experimental_network_cluster,
            tls_cert,
            tls_key,
            tls_client_ca,
            tls_cluster_id,
            qualified_heap_key,
            legacy_token_server,
            deployment_id,
        } => {
            let overrides = ConfigOverrides {
                bind,
                cluster_root: Some(cluster.clone()),
                node_index: node,
                auth_token: token,
                allow_insecure_bind: if allow_insecure_bind {
                    Some(true)
                } else {
                    None
                },
                experimental_network_cluster: if experimental_network_cluster {
                    Some(true)
                } else {
                    None
                },
                tls_cert,
                tls_key,
                tls_client_ca,
                tls_cluster_id,
                legacy_token_server: if legacy_token_server {
                    Some(true)
                } else {
                    None
                },
                qualified_heap_key: if qualified_heap_key { Some(true) } else { None },
                deployment_id,
                ..Default::default()
            };
            let validated =
                load_and_validate(config.as_deref(), ConfigMode::ServeCluster, overrides)
                    .map_err(|e| e.to_string())?;
            let bind = validated.bind.clone();
            let cluster_root = validated.cluster_root.clone().unwrap_or(cluster);
            let node_index = validated.node_index;
            let _ = json_out;
            for w in &validated.warnings {
                eprintln!("config warning: {w}");
            }
            let opts = validated.apply_to_serve_options(ServeOptions::new());
            serve_cluster_node(&cluster_root, node_index, &bind, opts).map_err(|e| e.to_string())
        }
        Command::Config { action } => match action {
            ConfigAction::Validate { file, mode } => cmd_config_validate(&file, &mode, json_out),
            ConfigAction::Show { file, mode } => cmd_config_show(&file, &mode, json_out),
        },
        Command::Collections { store } => cmd_list(&store, None, json_out),
    }
}

fn parse_config_mode(mode: &str) -> Result<ConfigMode, String> {
    match mode {
        "validate" => Ok(ConfigMode::Validate),
        "serve" => Ok(ConfigMode::Serve),
        "serve-cluster" | "cluster" => Ok(ConfigMode::ServeCluster),
        other => Err(format!(
            "unknown config mode {other:?}; expected validate|serve|serve-cluster"
        )),
    }
}

fn cmd_config_validate(file: &Path, mode: &str, json_out: bool) -> Result<(), String> {
    let mode = parse_config_mode(mode)?;
    let validated = load_and_validate(Some(file), mode, ConfigOverrides::default())
        .map_err(|e| e.to_string())?;
    let report = validated.effective_report(mode);
    if json_out {
        emit_json(sjson!({
            "ok": true,
            "profile": CONFIG_PROFILE,
            "mode": report.mode,
            "config_path": report.config_path,
            "warnings": report.warnings,
            "settings": report.settings,
        }))?;
    } else {
        println!(
            "config ok profile={} mode={} path={}",
            CONFIG_PROFILE,
            report.mode,
            file.display()
        );
        for w in &report.warnings {
            println!("warning: {w}");
        }
        println!("settings={} bind={}", report.settings.len(), validated.bind);
    }
    Ok(())
}

fn cmd_config_show(file: &Path, mode: &str, json_out: bool) -> Result<(), String> {
    let mode = parse_config_mode(mode)?;
    // Show is best-effort: for serve modes without paths, fall back to validate
    // so operators can still inspect bind/admission without a full path set.
    let validated = match load_and_validate(Some(file), mode, ConfigOverrides::default()) {
        Ok(v) => v,
        Err(e) if mode != ConfigMode::Validate => {
            // Retry in pure validate mode for partial documents.
            eprintln!("note: full mode validation failed ({e}); showing validate-mode report");
            load_and_validate(Some(file), ConfigMode::Validate, ConfigOverrides::default())
                .map_err(|e| e.to_string())?
        }
        Err(e) => return Err(e.to_string()),
    };
    let report = validated.effective_report(mode);
    if json_out {
        emit_json(serde_json::to_value(&report).map_err(|e| e.to_string())?)?;
    } else {
        println!(
            "profile={} format_version={}",
            report.profile, report.format_version
        );
        if let Some(p) = &report.config_path {
            println!("config_path={p}");
        }
        println!("mode={}", report.mode);
        for s in &report.settings {
            println!("  {} = {} ({:?}, {:?})", s.path, s.value, s.class, s.source);
        }
        for w in &report.warnings {
            println!("warning: {w}");
        }
    }
    Ok(())
}

fn parse_target(target: &str) -> Result<(String, String), String> {
    let (coll, key) = target
        .split_once('/')
        .ok_or_else(|| format!("target must be COLL/KEY, got {target:?}"))?;
    if coll.is_empty() || key.is_empty() {
        return Err("collection and key must be non-empty".into());
    }
    if key.contains('/') {
        // Allow multi-segment keys: users/a/b → coll=users, key=a/b
        let rest = &target[coll.len() + 1..];
        return Ok((coll.to_string(), rest.to_string()));
    }
    Ok((coll.to_string(), key.to_string()))
}

fn cmd_put(store: &Path, target: &str, json_body: &str, json_out: bool) -> Result<(), String> {
    let (coll, key) = parse_target(target)?;
    let value: JsonValue =
        serde_json::from_str(json_body).map_err(|e| format!("invalid --json: {e}"))?;
    let mut db = Residiuum::open(store).map_err(|e| e.to_string())?;
    let receipt = db
        .collection(&coll)
        .map_err(|e| e.to_string())?
        .put(&key, &value)
        .map_err(|e| e.to_string())?;
    if json_out {
        emit_json(sjson!({
            "ok": true,
            "store": store.display().to_string(),
            "collection": coll,
            "key": key,
            "committed": receipt.committed,
            "acknowledgement": receipt.acknowledgement.as_str(),
        }))?;
    } else {
        println!(
            "put {}/{} ok (ack={})",
            coll,
            key,
            receipt.acknowledgement.as_str()
        );
    }
    Ok(())
}

fn cmd_get(store: &Path, target: &str, json_out: bool) -> Result<(), String> {
    let (coll, key) = parse_target(target)?;
    let mut db = Residiuum::open(store).map_err(|e| e.to_string())?;
    let found = db
        .collection(&coll)
        .map_err(|e| e.to_string())?
        .get(&key)
        .map_err(|e| e.to_string())?;
    match found {
        None => {
            if json_out {
                emit_json(sjson!({
                    "ok": true,
                    "store": store.display().to_string(),
                    "collection": coll,
                    "key": key,
                    "found": false,
                }))?;
            } else {
                println!("not found: {coll}/{key}");
            }
            Err(format!("not found: {coll}/{key}"))
        }
        Some(v) => {
            if json_out {
                emit_json(sjson!({
                    "ok": true,
                    "store": store.display().to_string(),
                    "collection": coll,
                    "key": key,
                    "found": true,
                    "value": v,
                }))?;
            } else {
                println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
            }
            Ok(())
        }
    }
}

fn cmd_delete(store: &Path, target: &str, json_out: bool) -> Result<(), String> {
    let (coll, key) = parse_target(target)?;
    let mut db = Residiuum::open(store).map_err(|e| e.to_string())?;
    let receipt = db
        .collection(&coll)
        .map_err(|e| e.to_string())?
        .delete(&key)
        .map_err(|e| e.to_string())?;
    if json_out {
        emit_json(sjson!({
            "ok": true,
            "store": store.display().to_string(),
            "collection": coll,
            "key": key,
            "removed": receipt.removed,
            "acknowledgement": receipt.acknowledgement.as_str(),
        }))?;
    } else {
        println!(
            "delete {}/{} removed={} (ack={})",
            coll,
            key,
            receipt.removed,
            receipt.acknowledgement.as_str()
        );
    }
    Ok(())
}

fn cmd_list(store: &Path, collection: Option<&str>, json_out: bool) -> Result<(), String> {
    let mut db = Residiuum::open(store).map_err(|e| e.to_string())?;
    match collection {
        None => {
            let cols = db.list_collections().map_err(|e| e.to_string())?;
            if json_out {
                emit_json(sjson!({
                    "ok": true,
                    "store": store.display().to_string(),
                    "collections": cols,
                }))?;
            } else {
                for c in cols {
                    println!("{c}");
                }
            }
        }
        Some(coll) => {
            let keys = db
                .collection(coll)
                .map_err(|e| e.to_string())?
                .scan_keys()
                .map_err(|e| e.to_string())?;
            if json_out {
                emit_json(sjson!({
                    "ok": true,
                    "store": store.display().to_string(),
                    "collection": coll,
                    "keys": keys,
                }))?;
            } else {
                for k in keys {
                    println!("{k}");
                }
            }
        }
    }
    Ok(())
}

fn cmd_put_bytes(store: &Path, target: &str, file: &Path, json_out: bool) -> Result<(), String> {
    let (coll, key) = parse_target(target)?;
    let bytes = fs::read(file).map_err(|e| format!("read {}: {e}", file.display()))?;
    let mut db = Residiuum::open(store).map_err(|e| e.to_string())?;
    let receipt = db
        .collection(&coll)
        .map_err(|e| e.to_string())?
        .put_bytes(&key, &bytes)
        .map_err(|e| e.to_string())?;
    if json_out {
        emit_json(sjson!({
            "ok": true,
            "store": store.display().to_string(),
            "collection": coll,
            "key": key,
            "bytes": bytes.len(),
            "acknowledgement": receipt.acknowledgement.as_str(),
        }))?;
    } else {
        println!(
            "put-bytes {}/{} {} bytes (ack={})",
            coll,
            key,
            bytes.len(),
            receipt.acknowledgement.as_str()
        );
    }
    Ok(())
}

fn cmd_history(store: &Path, target: &str, json_out: bool) -> Result<(), String> {
    let (coll, key) = parse_target(target)?;
    let mut db = Residiuum::open(store).map_err(|e| e.to_string())?;
    let hist = db
        .collection(&coll)
        .map_err(|e| e.to_string())?
        .history(&key)
        .map_err(|e| e.to_string())?;
    if json_out {
        let versions: Vec<JsonValue> = hist
            .versions
            .iter()
            .map(|v| {
                sjson!({
                    "kind": v.kind,
                    "event_id": v.event_id,
                    "item_id": v.item_id,
                    "segment_id": v.segment_id,
                    "json": v.json,
                    "known_gap_before": v.known_gap_before,
                })
            })
            .collect();
        emit_json(sjson!({
            "ok": true,
            "store": store.display().to_string(),
            "collection": coll,
            "key": key,
            "has_known_holes": hist.has_known_holes,
            "versions": versions,
        }))?;
    } else {
        println!(
            "history {}/{} versions={} holes={}",
            coll,
            key,
            hist.versions.len(),
            hist.has_known_holes
        );
        for (i, v) in hist.versions.iter().enumerate() {
            println!("  [{i}] {} event={}", v.kind, v.event_id);
        }
    }
    Ok(())
}

fn cmd_doctor(store: &Path, json_out: bool) -> Result<(), String> {
    // Read-only open: no active writer, no derived persistence.
    let inspect = Store::open_inspect(store).map_err(|e| e.to_string())?;
    let salvage = inspect.salvage().map_err(|e| e.to_string())?;
    let page = examine_store(
        &inspect,
        ExamineLimits::default()
            .without_payloads()
            .max_units(10_000),
    )
    .map_err(|e| e.to_string())?;
    let summary = summarize_units(&page.units);
    let collections = inspect.list_collections();
    let indexes_dir = store.join("indexes");
    let catalogs_dir = store.join("catalogs");
    let index_cache_present = indexes_dir.join("primary.idx").is_file();
    let catalog_present = catalogs_dir.join("collections.cat").is_file();
    // DEF-101: lock-status is diagnostic only; OS lock is authoritative.
    let lock_status = Store::writer_lock_status(store).ok();
    // DEF-102: primary.idx is derived only — never authority / never size-as-health.
    let primary_cache = inspect.primary_cache_diag().ok();
    let lifecycle = inspect.lifecycle_diag().ok();

    let healthy = salvage.holes == 0 && summary.damaged == 0 && summary.holes == 0;
    let recommendations = doctor_recommendations(
        &salvage,
        &summary,
        index_cache_present,
        primary_cache.as_ref(),
    );

    if json_out {
        let lock_json = lock_status.as_ref().map(|obs| {
            sjson!({
                "class": obs.class.as_str(),
                "diagnostic_pid": obs.diagnostic_pid,
                "diagnostic_pid_liveness": obs.diagnostic_pid_liveness.as_str(),
                "diagnostic_acquired_ns": obs.diagnostic_acquired_ns.map(|n| n.to_string()),
                "os_lock_authoritative": obs.os_lock_authoritative,
                "retryable": obs.retryable,
                "detail": obs.detail,
                "guidance": "never delete writer.lock to force unlock; OS exclusive lock is authoritative",
            })
        });
        let primary_cache_json = primary_cache.as_ref().map(|d| {
            sjson!({
                "present": d.present,
                "format_version": d.format_version,
                "byte_len": d.byte_len,
                "validation": d.validation.as_str(),
                "sealed_fingerprint": d.sealed_fingerprint.as_ref().map(|fp| hex_bytes(fp)),
                "active_segment_id": d.active_segment_id.as_ref().map(|id| hex16(id)),
                "active_covered_len": d.active_covered_len,
                "active_actual_len": d.active_actual_len,
                "replay_bytes": d.replay_bytes,
                "resident_entries": d.resident_entries,
                "resident_body_bytes": d.resident_body_bytes,
                "authoritative": d.authoritative,
                "detail": d.detail,
            })
        });
        let lifecycle_json = lifecycle.as_ref().map(|d| {
            sjson!({
                "active_shards": d.active_shards,
                "pending_seals": d.pending_seals,
                "sealed_segments": d.sealed_segments,
                "checkpoint_reason": d.checkpoint_reason,
                "derived_ops_since_checkpoint": d.derived_ops_since_checkpoint,
                "primary_cache_authoritative": d.primary_cache_authoritative,
                "detail": d.detail,
            })
        });
        emit_json(sjson!({
            "ok": true,
            "store": store.display().to_string(),
            "store_id": hex16(&inspect.store_id()),
            "read_only": true,
            "healthy": healthy,
            "live_subjects": salvage.live_subjects,
            "files_scanned": salvage.files_scanned,
            "verified_frames": salvage.verified_frames,
            "item_events": salvage.item_events,
            "holes": salvage.holes,
            "examination": {
                "units": page.units.len(),
                "complete": page.complete,
                "verified_complete": summary.verified_complete,
                "partial": summary.partial,
                "damaged": summary.damaged,
                "holes": summary.holes,
            },
            "collections": collections,
            "derived": {
                "index_cache_present": index_cache_present,
                "catalog_present": catalog_present,
            },
            "primary_cache": primary_cache_json,
            "lifecycle": lifecycle_json,
            "writer_lock": lock_json,
            "recommendations": recommendations,
        }))?;
    } else {
        println!("residiuum doctor {}", store.display());
        println!("  read_only: true");
        println!("  healthy: {healthy}");
        println!("  store_id: {}", hex16(&inspect.store_id()));
        if let Some(obs) = &lock_status {
            println!(
                "  writer_lock: class={} pid={:?} liveness={} retryable={} (OS lock authoritative; do not delete writer.lock)",
                obs.class.as_str(),
                obs.diagnostic_pid,
                obs.diagnostic_pid_liveness.as_str(),
                obs.retryable
            );
            println!("    detail: {}", obs.detail);
        }
        println!("  live_subjects: {}", salvage.live_subjects);
        println!(
            "  segments: files={} verified_frames={} item_events={} holes={}",
            salvage.files_scanned, salvage.verified_frames, salvage.item_events, salvage.holes
        );
        println!(
            "  examination: units={} complete={} verified={} partial={} damaged={} holes={}",
            page.units.len(),
            page.complete,
            summary.verified_complete,
            summary.partial,
            summary.damaged,
            summary.holes
        );
        println!(
            "  collections: {}",
            if collections.is_empty() {
                "(none)".into()
            } else {
                collections.join(", ")
            }
        );
        println!(
            "  derived: index_cache={} catalog={}",
            index_cache_present, catalog_present
        );
        if let Some(d) = &primary_cache {
            println!(
                "  primary_cache: present={} validation={} byte_len={} format_version={:?} \
                 authoritative={} (size is not stored-data size)",
                d.present,
                d.validation.as_str(),
                d.byte_len,
                d.format_version,
                d.authoritative
            );
            println!(
                "    covered_len={:?} active_actual_len={:?} replay_bytes={:?} resident_entries={:?}",
                d.active_covered_len, d.active_actual_len, d.replay_bytes, d.resident_entries
            );
            println!("    detail: {}", d.detail);
        }
        if let Some(d) = &lifecycle {
            println!(
                "  lifecycle: active_shards={} pending_seals={} sealed_segments={} checkpoint={}",
                d.active_shards, d.pending_seals, d.sealed_segments, d.checkpoint_reason
            );
            println!("    detail: {}", d.detail);
        }
        if !recommendations.is_empty() {
            println!("  recommendations:");
            for r in &recommendations {
                println!("    - {r}");
            }
        }
    }
    // Nonzero exit when damaged (failed health guarantee) — still printed report.
    if !healthy {
        return Err("store health check found holes or damaged units".into());
    }
    Ok(())
}

fn cmd_salvage(source: &Path, dest: &Path, json_out: bool) -> Result<(), String> {
    if source == dest {
        return Err("salvage source and --output must differ".into());
    }
    // Inspect source without mutating; evidence-preserving salvage_to creates dest.
    let inspect = Store::open_inspect(source).map_err(|e| e.to_string())?;
    let report = inspect.salvage_to(dest).map_err(|e| e.to_string())?;
    emit_copy_report("evidence", source, &report, json_out)
}

fn cmd_export_live(source: &Path, dest: &Path, json_out: bool) -> Result<(), String> {
    if source == dest {
        return Err("export-live source and --output must differ".into());
    }
    let inspect = Store::open_inspect(source).map_err(|e| e.to_string())?;
    let report = inspect.export_live_state(dest).map_err(|e| e.to_string())?;
    emit_copy_report("live_state_export", source, &report, json_out)
}

fn cmd_backup(source: &Path, package: &Path, json_out: bool) -> Result<(), String> {
    if source == package {
        return Err("backup source and --output must differ".into());
    }
    // Prefer exclusive open so we can flush durable state (crash-consistent).
    // Fall back to inspect if another writer holds the lock.
    let report = match Store::open(source) {
        Ok(mut store) => store.backup_to(package).map_err(|e| e.to_string())?,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("writer lock") || msg.contains("lock held") {
                let mut inspect = Store::open_inspect(source).map_err(|e| e.to_string())?;
                inspect.backup_to(package).map_err(|e| e.to_string())?
            } else {
                return Err(msg);
            }
        }
    };
    if json_out {
        emit_json(sjson!({
            "ok": true,
            "mode": "full_backup",
            "profile": residiuum_store::BACKUP_PROFILE,
            "source": report.source.display().to_string(),
            "destination": report.destination.display().to_string(),
            "manifest": report.manifest_path.display().to_string(),
            "store_id_hex": hex16(&report.store_id),
            "backup_id_hex": hex16(&report.backup_id),
            "files_copied": report.files_copied,
            "total_bytes": report.total_bytes,
            "consistency": match report.consistency {
                residiuum_store::BackupConsistency::FlushedExclusive => "flushed_exclusive",
                residiuum_store::BackupConsistency::OnDiskInspect => "on_disk_inspect",
            },
        }))?;
    } else {
        println!("full_backup");
        println!("  profile: {}", residiuum_store::BACKUP_PROFILE);
        println!("  source: {}", report.source.display());
        println!("  package: {}", report.destination.display());
        println!("  manifest: {}", report.manifest_path.display());
        println!("  store_id: {}", hex16(&report.store_id));
        println!("  backup_id: {}", hex16(&report.backup_id));
        println!("  files_copied: {}", report.files_copied);
        println!("  total_bytes: {}", report.total_bytes);
        println!(
            "  consistency: {}",
            match report.consistency {
                residiuum_store::BackupConsistency::FlushedExclusive => "flushed_exclusive",
                residiuum_store::BackupConsistency::OnDiskInspect => "on_disk_inspect",
            }
        );
    }
    Ok(())
}

fn cmd_restore(
    package: &Path,
    dest: &Path,
    reassign_identity: bool,
    json_out: bool,
) -> Result<(), String> {
    if package == dest {
        return Err("restore backup and --output must differ".into());
    }
    let report = restore_full_backup(package, dest, RestoreOptions { reassign_identity })
        .map_err(|e| e.to_string())?;
    if json_out {
        emit_json(sjson!({
            "ok": true,
            "mode": "restore",
            "backup": report.backup_root.display().to_string(),
            "destination": report.destination.display().to_string(),
            "manifest": report.manifest_path.display().to_string(),
            "source_store_id_hex": hex16(&report.source_store_id),
            "restored_store_id_hex": hex16(&report.restored_store_id),
            "identity_reassigned": report.identity_reassigned,
            "files_restored": report.files_restored,
            "live_subjects": report.live_subjects,
        }))?;
    } else {
        println!("restore");
        println!("  backup: {}", report.backup_root.display());
        println!("  destination: {}", report.destination.display());
        println!("  manifest: {}", report.manifest_path.display());
        println!("  source_store_id: {}", hex16(&report.source_store_id));
        println!("  restored_store_id: {}", hex16(&report.restored_store_id));
        println!("  identity_reassigned: {}", report.identity_reassigned);
        println!("  files_restored: {}", report.files_restored);
        println!("  live_subjects: {}", report.live_subjects);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // CLI flag surface maps 1:1 to params
fn cmd_migrate(
    store: &Path,
    output: Option<&Path>,
    preflight: bool,
    plan_only: bool,
    skip_verify: bool,
    status_only: bool,
    rollback: bool,
    json_out: bool,
) -> Result<(), String> {
    if status_only {
        let job = load_migration_job(store).map_err(|e| e.to_string())?;
        return match job {
            None => {
                if json_out {
                    emit_json(sjson!({
                        "ok": true,
                        "mode": "migrate_status",
                        "store": store.display().to_string(),
                        "job": null,
                    }))
                } else {
                    println!("migrate_status");
                    println!("  store: {}", store.display());
                    println!("  job: none");
                    Ok(())
                }
            }
            Some(j) => emit_migrate_job("migrate_status", store, &j, json_out),
        };
    }

    if rollback {
        let job = load_migration_job(store)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "no migration job on source; nothing to rollback".to_string())?;
        let rolled = migrate_rollback(&job).map_err(|e| e.to_string())?;
        return emit_migrate_job("migrate_rollback", store, &rolled, json_out);
    }

    let dest = output.ok_or_else(|| {
        "migrate requires --output DEST (or use --status / --rollback)".to_string()
    })?;
    if store == dest {
        return Err("migrate source and --output must differ".into());
    }

    if preflight && plan_only {
        return Err("use either --preflight or --plan-only, not both".into());
    }

    // Prefer exclusive open for crash-consistent boundary; fall back to inspect.
    let mut exclusive = Store::open(store).ok();
    if preflight {
        let pre = if let Some(ref s) = exclusive {
            s.migrate_preflight(dest).map_err(|e| e.to_string())?
        } else {
            let inspect = Store::open_inspect(store).map_err(|e| e.to_string())?;
            inspect.migrate_preflight(dest).map_err(|e| e.to_string())?
        };
        if json_out {
            return emit_json(sjson!({
                "ok": pre.blockers.is_empty(),
                "mode": "migrate_preflight",
                "profile": MIGRATE_PROFILE,
                "source": pre.source_root,
                "destination": pre.dest_root,
                "source_store_id_hex": pre.source_store_id_hex,
                "writer_wire": pre.writer_wire,
                "writer_wire_profile": pre.writer_wire_profile,
                "wire_support_summary": pre.wire_support_summary,
                "wire_matrix": pre.wire_matrix,
                "protocol": pre.protocol,
                "files_classified": pre.files_classified,
                "supported_segments": pre.supported_segments,
                "unsupported_segments": pre.unsupported_segments,
                "unreadable_segments": pre.unreadable_segments,
                "dest_ok": pre.dest_ok,
                "blockers": pre.blockers,
                "warnings": pre.warnings,
            }));
        }
        println!("migrate_preflight");
        println!("  source: {}", pre.source_root);
        println!("  destination: {}", pre.dest_root);
        println!(
            "  writer_wire: {} ({})",
            pre.writer_wire, pre.writer_wire_profile
        );
        println!("  files_classified: {}", pre.files_classified);
        println!("  supported_segments: {}", pre.supported_segments);
        println!("  unsupported_segments: {}", pre.unsupported_segments);
        println!("  unreadable_segments: {}", pre.unreadable_segments);
        println!("  dest_ok: {}", pre.dest_ok);
        for b in &pre.blockers {
            println!("  blocker: {b}");
        }
        for w in &pre.warnings {
            println!("  warning: {w}");
        }
        if !pre.blockers.is_empty() {
            return Err("migration preflight blocked".into());
        }
        return Ok(());
    }

    let opts = MigrateOptions {
        plan_only,
        skip_verify,
    };
    let report = if let Some(ref mut s) = exclusive {
        s.migrate_to(dest, opts).map_err(|e| e.to_string())?
    } else {
        let mut inspect = Store::open_inspect(store).map_err(|e| e.to_string())?;
        inspect.migrate_to(dest, opts).map_err(|e| e.to_string())?
    };

    if json_out {
        emit_json(sjson!({
            "ok": true,
            "mode": "migrate",
            "profile": MIGRATE_PROFILE,
            "phase": report.phase.as_str(),
            "source": report.source.display().to_string(),
            "destination": report.destination.display().to_string(),
            "job_id_hex": hex16(&report.job_id),
            "job_path": report.job_path.display().to_string(),
            "files_planned": report.files_planned,
            "files_applied": report.files_applied,
            "bytes_applied": report.bytes_applied,
            "verified_live_subjects": report.verified_live_subjects,
            "unsupported_preserved": report.unsupported_preserved,
            "unreadable_preserved": report.unreadable_preserved,
        }))?;
    } else {
        println!("migrate");
        println!("  phase: {}", report.phase.as_str());
        println!("  source: {}", report.source.display());
        println!("  destination: {}", report.destination.display());
        println!("  job_id: {}", hex16(&report.job_id));
        println!("  files_planned: {}", report.files_planned);
        println!("  files_applied: {}", report.files_applied);
        println!("  bytes_applied: {}", report.bytes_applied);
        if let Some(n) = report.verified_live_subjects {
            println!("  verified_live_subjects: {n}");
        }
        println!("  unsupported_preserved: {}", report.unsupported_preserved);
        println!("  unreadable_preserved: {}", report.unreadable_preserved);
    }
    Ok(())
}

fn emit_migrate_job(
    mode: &str,
    store: &Path,
    job: &residiuum_store::MigrationJob,
    json_out: bool,
) -> Result<(), String> {
    if json_out {
        emit_json(sjson!({
            "ok": true,
            "mode": mode,
            "store": store.display().to_string(),
            "profile": job.profile,
            "job_id_hex": job.job_id_hex,
            "phase": job.phase.as_str(),
            "source_root": job.source_root,
            "dest_root": job.dest_root,
            "files": job.files.len(),
            "files_applied": job.files_applied,
            "bytes_applied": job.bytes_applied,
            "verified_live_subjects": job.verified_live_subjects,
            "error": job.error,
            "target_wire_profile": job.target_wire_profile,
        }))
    } else {
        println!("{mode}");
        println!("  store: {}", store.display());
        println!("  job_id: {}", job.job_id_hex);
        println!("  phase: {}", job.phase.as_str());
        println!("  destination: {}", job.dest_root);
        println!("  files: {}", job.files.len());
        println!("  files_applied: {}", job.files_applied);
        if let Some(n) = job.verified_live_subjects {
            println!("  verified_live_subjects: {n}");
        }
        if let Some(ref e) = job.error {
            println!("  error: {e}");
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)] // CLI flag surface maps 1:1 to params
fn cmd_scrub(
    store: &Path,
    status_only: bool,
    once: bool,
    pause: bool,
    resume: bool,
    max_files: usize,
    max_bytes: u64,
    no_quarantine: bool,
    json_out: bool,
) -> Result<(), String> {
    if pause && resume {
        return Err("use either --pause or --resume, not both".into());
    }
    // Inspect is enough: scrub only writes under recovery/scrub/.
    let inspect = Store::open_inspect(store).map_err(|e| e.to_string())?;

    if pause {
        let st = inspect.pause_scrub().map_err(|e| e.to_string())?;
        return emit_scrub_status("scrub_paused", store, &st, &inspect, json_out);
    }
    if resume {
        let st = inspect.resume_scrub().map_err(|e| e.to_string())?;
        return emit_scrub_status("scrub_resumed", store, &st, &inspect, json_out);
    }
    if status_only {
        let st = inspect.scrub_status().map_err(|e| e.to_string())?;
        return emit_scrub_status("scrub_status", store, &st, &inspect, json_out);
    }

    let opts = ScrubOptions {
        max_files,
        max_bytes,
        quarantine: !no_quarantine,
        ..ScrubOptions::default()
    };
    let report = if once {
        inspect.scrub_once(opts).map_err(|e| e.to_string())?
    } else {
        inspect
            .scrub_to_completion(opts)
            .map_err(|e| e.to_string())?
    };

    let findings = inspect.list_scrub_findings().map_err(|e| e.to_string())?;
    let finding_summaries: Vec<JsonValue> = findings
        .iter()
        .map(|f| {
            sjson!({
                "finding_id": f.finding_id,
                "kind": f.finding.as_str(),
                "target_kind": f.kind.as_str(),
                "path": f.relative_path,
                "detail": f.detail,
                "quarantine_path": f.quarantine_path,
            })
        })
        .collect();

    if json_out {
        emit_json(sjson!({
            "ok": true,
            "mode": if once { "scrub_once" } else { "scrub" },
            "store": store.display().to_string(),
            "store_id_hex": hex16(&inspect.store_id()),
            "targets_processed": report.targets_processed,
            "bytes_processed": report.bytes_processed,
            "failures_this_call": report.failures_this_call,
            "cycle_completed": report.cycle_completed,
            "paused": report.paused,
            "coverage_ratio": report.status.coverage_ratio,
            "bytes_verified_total": report.status.bytes_verified_total,
            "failures_total": report.status.failures_total,
            "open_findings": report.status.open_findings,
            "last_complete_cycle_ns": report.status.last_complete_cycle_ns,
            "last_complete_age_ns": report.status.last_complete_age_ns,
            "findings": finding_summaries,
        }))?;
    } else {
        println!("{}", if once { "scrub_once" } else { "scrub" });
        println!("  store: {}", store.display());
        println!("  store_id: {}", hex16(&inspect.store_id()));
        println!("  targets_processed: {}", report.targets_processed);
        println!("  bytes_processed: {}", report.bytes_processed);
        println!("  failures_this_call: {}", report.failures_this_call);
        println!("  cycle_completed: {}", report.cycle_completed);
        println!("  paused: {}", report.paused);
        println!("  coverage_ratio: {:.4}", report.status.coverage_ratio);
        println!(
            "  bytes_verified_total: {}",
            report.status.bytes_verified_total
        );
        println!("  failures_total: {}", report.status.failures_total);
        println!("  open_findings: {}", report.status.open_findings);
        if let Some(age) = report.status.last_complete_age_ns {
            println!("  last_complete_age_ns: {age}");
        }
        for f in &findings {
            println!(
                "  finding: {} {} {}",
                f.finding.as_str(),
                f.relative_path,
                f.detail
            );
        }
    }
    Ok(())
}

fn emit_scrub_status(
    mode: &str,
    store: &Path,
    st: &residiuum_store::ScrubStatus,
    inspect: &Store,
    json_out: bool,
) -> Result<(), String> {
    let findings = inspect.list_scrub_findings().map_err(|e| e.to_string())?;
    if json_out {
        emit_json(sjson!({
            "ok": true,
            "mode": mode,
            "store": store.display().to_string(),
            "store_id_hex": hex16(&inspect.store_id()),
            "paused": st.paused,
            "cycle_id": st.cycle_id,
            "coverage_ratio": st.coverage_ratio,
            "bytes_verified_total": st.bytes_verified_total,
            "failures_total": st.failures_total,
            "open_findings": st.open_findings,
            "targets_remaining": st.targets_remaining,
            "targets_in_cycle": st.targets_in_cycle,
            "last_complete_cycle_ns": st.last_complete_cycle_ns,
            "last_complete_age_ns": st.last_complete_age_ns,
            "finding_count": findings.len(),
        }))?;
    } else {
        println!("{mode}");
        println!("  store: {}", store.display());
        println!("  store_id: {}", hex16(&inspect.store_id()));
        println!("  paused: {}", st.paused);
        println!("  cycle_id: {}", st.cycle_id);
        println!("  coverage_ratio: {:.4}", st.coverage_ratio);
        println!("  bytes_verified_total: {}", st.bytes_verified_total);
        println!("  failures_total: {}", st.failures_total);
        println!("  open_findings: {}", st.open_findings);
        println!("  targets_remaining: {}", st.targets_remaining);
        if let Some(age) = st.last_complete_age_ns {
            println!("  last_complete_age_ns: {age}");
        }
    }
    Ok(())
}

fn emit_copy_report(
    mode: &str,
    source: &Path,
    report: &residiuum_store::SalvageCopyReport,
    json_out: bool,
) -> Result<(), String> {
    if json_out {
        emit_json(sjson!({
            "ok": true,
            "mode": mode,
            "source": source.display().to_string(),
            "destination": report.destination.display().to_string(),
            "source_immutable": true,
            "files_scanned": report.source.files_scanned,
            "verified_frames": report.source.verified_frames,
            "item_events": report.source.item_events,
            "holes": report.source.holes,
            "live_subjects": report.source.live_subjects,
            "subjects_copied": report.subjects_copied,
            "frames_copied": report.frames_copied,
            "holes_recorded": report.holes_recorded,
            "manifest_path": report.manifest_path.as_ref().map(|p| p.display().to_string()),
        }))?;
    } else {
        println!(
            "{mode} {} → {}",
            source.display(),
            report.destination.display()
        );
        println!("  source immutable: true");
        println!(
            "  source: files={} frames={} items={} holes={} live={}",
            report.source.files_scanned,
            report.source.verified_frames,
            report.source.item_events,
            report.source.holes,
            report.source.live_subjects
        );
        println!("  subjects_copied: {}", report.subjects_copied);
        println!("  frames_copied: {}", report.frames_copied);
        println!("  holes_recorded: {}", report.holes_recorded);
        if let Some(m) = &report.manifest_path {
            println!("  manifest: {}", m.display());
        }
    }
    Ok(())
}

struct UnitSummary {
    verified_complete: usize,
    partial: usize,
    damaged: usize,
    holes: usize,
}

fn summarize_units(units: &[ExaminationUnit]) -> UnitSummary {
    let mut s = UnitSummary {
        verified_complete: 0,
        partial: 0,
        damaged: 0,
        holes: 0,
    };
    for u in units {
        let kind = u.unit_kind.to_lowercase();
        let status = u.status.to_lowercase();
        if kind.contains("hole") || status.contains("hole") {
            s.holes += 1;
        } else if status.contains("partial") || u.payload.availability == "partial" {
            s.partial += 1;
        } else if status.contains("damaged")
            || status.contains("corrupt")
            || status.contains("failed")
        {
            s.damaged += 1;
        } else if status.contains("verified") || status == "complete" {
            s.verified_complete += 1;
        } else {
            // Neutral structural units count as verified for health rollup.
            s.verified_complete += 1;
        }
    }
    s
}

fn doctor_recommendations(
    salvage: &residiuum_store::SalvageReport,
    summary: &UnitSummary,
    index_cache: bool,
    primary_cache: Option<&residiuum_store::PrimaryCacheDiag>,
) -> Vec<String> {
    let mut out = Vec::new();
    if salvage.holes > 0 || summary.holes > 0 {
        out.push(
            "holes detected: run `residiuum salvage SRC --output DST` for evidence-preserving recovery (source stays immutable); use `residiuum export-live` only for clean live-state materialization".into(),
        );
    }
    if summary.damaged > 0 {
        out.push("damaged units present: examine with residiuum-examine / SDA filters".into());
    }
    if !index_cache {
        out.push(
            "primary index cache missing (derived only; open/rebuild will rescan segments; \
             byte size of primary.idx is never stored-data size)"
                .into(),
        );
    } else if let Some(d) = primary_cache {
        use residiuum_store::PrimaryCacheValidation;
        match d.validation {
            PrimaryCacheValidation::Accepted => {
                // Healthy minimal cache with a large active/ is normal.
            }
            PrimaryCacheValidation::Absent => {
                out.push(
                    "primary.idx absent (derived only; open rebuilds from active/ + segments/)"
                        .into(),
                );
            }
            PrimaryCacheValidation::Stale => {
                out.push(format!(
                    "primary.idx stale — open will replay/rebuild; cache is not authority ({})",
                    d.detail
                ));
            }
            PrimaryCacheValidation::Corrupt => {
                out.push(format!(
                    "primary.idx corrupt/ahead/truncated — rebuild from segments ({})",
                    d.detail
                ));
            }
            PrimaryCacheValidation::Foreign => {
                out.push(format!(
                    "primary.idx foreign store_id — ignore and rebuild ({})",
                    d.detail
                ));
            }
            PrimaryCacheValidation::Unsupported => {
                out.push(format!(
                    "primary.idx unsupported format — rebuild preferred ({})",
                    d.detail
                ));
            }
        }
    }
    if salvage.live_subjects == 0 && salvage.item_events > 0 {
        out.push("item events found but no live subjects (all deleted or incomplete)".into());
    }
    if out.is_empty() {
        out.push("no action required".into());
    }
    out
}

fn emit_json(v: JsonValue) -> Result<(), String> {
    let mut out = io::stdout();
    serde_json::to_writer(&mut out, &v).map_err(|e| e.to_string())?;
    out.write_all(b"\n").map_err(|e| e.to_string())?;
    Ok(())
}

fn hex16(id: &[u8; 16]) -> String {
    hex_bytes(id)
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}
