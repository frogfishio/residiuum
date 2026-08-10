//! `residiuum-authority` — local-only heap authority ceremony CLI (AGPL).

use clap::{Parser, Subcommand};
use residiuum_authority::{
    apply_reload_request, commit_genesis, issue_heap_key, AuthorityPaths,
    EphemeralMasterKeyProvider, GenesisRequest, IssueRequest, MasterAuthorityStore,
    MasterKeyProvider,
};
use residiuum_heap::{DeploymentId, HeapId, Rights};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

const LICENSE_TEXT: &str = "Copyright (c) 2026 Alexander R. Croft\nGNU Affero General Public License v3.0 or later\n\nThis program (`residiuum-authority`) is offered under the AGPL-3.0-or-later.\nSee LICENSE-AGPL-3.0 and doc/reference/operations/LICENSING.md.\n\nThe qualified data server must never link this crate or a MasterKeyProvider.";

#[derive(Parser)]
#[command(
    name = "residiuum-authority",
    about = "Local-only Residiuum heap authority ceremony tool",
    disable_version_flag = true
)]
struct Cli {
    /// Print copyright and license information.
    #[arg(long = "license")]
    license: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Stage storage genesis, commit authority root, publish staged bytes.
    Genesis {
        /// Authority root directory (separate from data root when qualified).
        #[arg(long)]
        authority_root: PathBuf,
        /// Data root for staged/published descriptors.
        #[arg(long)]
        data_root: PathBuf,
        /// Canonical heap name.
        #[arg(long)]
        name: String,
        /// Optional fixed 64-hex master seed (tests). Random when omitted.
        #[arg(long)]
        master_seed_hex: Option<String>,
    },
    /// Issue a HeapKey for a holder public key (64-hex).
    Issue {
        #[arg(long)]
        authority_root: PathBuf,
        #[arg(long)]
        deployment_id: String,
        #[arg(long)]
        heap_id: String,
        #[arg(long)]
        holder_public_key_hex: String,
        #[arg(long, default_value_t = 5)]
        rights: u64,
        #[arg(long)]
        master_seed_hex: String,
    },
    /// Apply a pending data-root reload request (read-only authority load).
    Reload {
        #[arg(long)]
        data_root: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if cli.license {
        println!("{LICENSE_TEXT}");
        return ExitCode::SUCCESS;
    }
    match cli.command {
        None => {
            eprintln!("usage: residiuum-authority <genesis|issue|reload|--license>");
            ExitCode::FAILURE
        }
        Some(Commands::Genesis {
            authority_root,
            data_root,
            name,
            master_seed_hex,
        }) => match run_genesis(authority_root, data_root, name, master_seed_hex) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("genesis failed: {e}");
                ExitCode::FAILURE
            }
        },
        Some(Commands::Issue {
            authority_root,
            deployment_id,
            heap_id,
            holder_public_key_hex,
            rights,
            master_seed_hex,
        }) => match run_issue(
            authority_root,
            deployment_id,
            heap_id,
            holder_public_key_hex,
            rights,
            master_seed_hex,
        ) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("issue failed: {e}");
                ExitCode::FAILURE
            }
        },
        Some(Commands::Reload { data_root }) => match apply_reload_request(&data_root) {
            Ok(Some(snap)) => {
                println!(
                    "reloaded heap={} generation={}",
                    snap.heap_id,
                    snap.authority_generation.get()
                );
                ExitCode::SUCCESS
            }
            Ok(None) => {
                println!("no pending reload");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("reload failed: {e}");
                ExitCode::FAILURE
            }
        },
    }
}

fn run_genesis(
    authority_root: PathBuf,
    data_root: PathBuf,
    name: String,
    master_seed_hex: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let provider: Box<dyn MasterKeyProvider> = match master_seed_hex {
        Some(h) => Box::new(EphemeralMasterKeyProvider::from_seed(parse_hex32(&h)?)),
        None => Box::new(EphemeralMasterKeyProvider::generate()?),
    };
    let deployment = DeploymentId::new_random()?;
    let heap = HeapId::new_random()?;
    let creation = {
        let mut id = [0u8; 16];
        getrandom::fill(&mut id).map_err(|e| format!("getrandom: {e}"))?;
        id[6] = (id[6] & 0x0f) | 0x40;
        id[8] = (id[8] & 0x3f) | 0x80;
        id
    };
    let effective_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .max(1);
    let result = commit_genesis(
        provider.as_ref(),
        GenesisRequest {
            authority_root,
            data_root,
            deployment_id: *deployment.as_bytes(),
            heap_id: *heap.as_bytes(),
            name,
            creation_event_id: creation,
            effective_at,
        },
    )?;
    println!("deployment_id={deployment}");
    println!("heap_id={heap}");
    println!("descriptor_hash={}", hex::encode(result.descriptor_hash));
    println!(
        "authority_chain_head={}",
        hex::encode(result.authority_chain_head_hash)
    );
    println!(
        "master_public_key={}",
        hex::encode(result.master_public_key)
    );
    Ok(())
}

fn run_issue(
    authority_root: PathBuf,
    deployment_id: String,
    heap_id: String,
    holder_public_key_hex: String,
    rights: u64,
    master_seed_hex: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let deployment: DeploymentId = deployment_id.parse()?;
    let heap: HeapId = heap_id.parse()?;
    let provider = EphemeralMasterKeyProvider::from_seed(parse_hex32(&master_seed_hex)?);
    let store = MasterAuthorityStore::open(AuthorityPaths::new(
        &authority_root,
        deployment.as_bytes(),
        heap.as_bytes(),
    ))?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let issued = issue_heap_key(
        &store,
        &provider,
        IssueRequest {
            holder_public_key: parse_hex32(&holder_public_key_hex)?,
            rights: Rights::from_bits_certificate(rights)?,
            not_before: now,
            expires_at: now + 3600,
        },
    )?;
    println!("fingerprint={}", hex::encode(issued.fingerprint));
    println!("cose_sign1={}", hex::encode(issued.cose_sign1));
    Ok(())
}

fn parse_hex32(s: &str) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let bytes = hex::decode(s)?;
    if bytes.len() != 32 {
        return Err("expected 32 bytes".into());
    }
    let mut a = [0u8; 32];
    a.copy_from_slice(&bytes);
    Ok(a)
}
