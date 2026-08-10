//! Multi-process crash child for DEF-022.
//!
//! Invoked by integration tests: opens (or creates) a store path, arms an
//! `Abort` failpoint, drives one operation, and either aborts mid-op or exits
//! with a status code describing the outcome.
//!
//! Environment:
//! - `RESIDIUUM_CRASH_STORE` — store directory (required)
//! - `RESIDIUUM_CRASH_OP` — `put_durable` | `put_many_durable` | `delete_durable` | `seed_prior`
//! - `RESIDIUUM_CRASH_FP` — failpoint name to arm with Abort (optional; omit to finish cleanly)
//! - `RESIDIUUM_CRASH_KEY` — subject key (default `k`)
//! - `RESIDIUUM_CRASH_VAL` — put payload (default `v-new`)

use residiuum_store::{
    arm_failpoint_once, clear_failpoints, DurabilityMode, FailpointAction, Store,
};
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let store_path = match env::var_os("RESIDIUUM_CRASH_STORE") {
        Some(p) => PathBuf::from(p),
        None => {
            eprintln!("RESIDIUUM_CRASH_STORE required");
            return ExitCode::from(2);
        }
    };
    let op = env::var("RESIDIUUM_CRASH_OP").unwrap_or_else(|_| "put_durable".into());
    let key = env::var("RESIDIUUM_CRASH_KEY").unwrap_or_else(|_| "k".into());
    let val = env::var("RESIDIUUM_CRASH_VAL").unwrap_or_else(|_| "v-new".into());
    let fp = env::var("RESIDIUUM_CRASH_FP").ok();

    clear_failpoints();

    match op.as_str() {
        "seed_prior" => {
            let mut s = match Store::create(&store_path) {
                Ok(s) => s,
                Err(_) => match Store::open(&store_path) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("open/create: {e}");
                        return ExitCode::from(3);
                    }
                },
            };
            if let Err(e) = s.put("prior", b"prior-v1", DurabilityMode::Durable) {
                eprintln!("seed prior: {e}");
                return ExitCode::from(4);
            }
            ExitCode::SUCCESS
        }
        "put_durable" => {
            let mut s = match Store::open(&store_path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("open: {e}");
                    return ExitCode::from(3);
                }
            };
            if let Some(ref name) = fp {
                let leaked: &'static str = Box::leak(name.clone().into_boxed_str());
                arm_failpoint_once(leaked, FailpointAction::Abort);
            }
            // Abort failpoint kills the process; success path returns 0.
            match s.put(&key, val.as_bytes(), DurabilityMode::Durable) {
                Ok(_) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("put: {e}");
                    ExitCode::from(5)
                }
            }
        }
        // AWO multi-process cells: put_many hits awo.persist.* / awo.publish.* failpoints.
        "put_many_durable" => {
            let mut s = match Store::open(&store_path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("open: {e}");
                    return ExitCode::from(3);
                }
            };
            if let Some(ref name) = fp {
                let leaked: &'static str = Box::leak(name.clone().into_boxed_str());
                arm_failpoint_once(leaked, FailpointAction::Abort);
            }
            let k2 = format!("{key}-b");
            match s.put_many(
                &[(key.as_str(), val.as_bytes()), (k2.as_str(), b"v-b")],
                DurabilityMode::Durable,
            ) {
                Ok(_) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("put_many: {e}");
                    ExitCode::from(5)
                }
            }
        }
        "delete_durable" => {
            let mut s = match Store::open(&store_path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("open: {e}");
                    return ExitCode::from(3);
                }
            };
            if let Some(ref name) = fp {
                let leaked: &'static str = Box::leak(name.clone().into_boxed_str());
                arm_failpoint_once(leaked, FailpointAction::Abort);
            }
            match s.delete(&key, DurabilityMode::Durable) {
                Ok(_) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("delete: {e}");
                    ExitCode::from(5)
                }
            }
        }
        other => {
            eprintln!("unknown RESIDIUUM_CRASH_OP={other}");
            ExitCode::from(2)
        }
    }
}
