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

use residiuum_atomics::{
    AtomicId, AtomicMember, AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, CollectionId,
    CoordinationScope, HeapId, MutationKind, ObjectIdentity, PlanMutation, ResourceLimits,
    VersionId,
};
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
        "atomic_prepare" => {
            let mut s = match Store::open(&store_path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("open: {e}");
                    return ExitCode::from(3);
                }
            };
            let heap = HeapId::from_bytes(s.store_id()).expect("store id is heap id");
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = 9;
            let id = AtomicId::from_bytes(id_bytes).expect("atomic id");
            let collection = CollectionId::from_bytes([2u8; 16]).expect("collection id");
            let event = VersionId::from_bytes([3u8; 16]).expect("event id");
            let member = AtomicMember {
                atomic_id: id,
                ordinal: 0,
                object_identity: ObjectIdentity::new(collection, CanonicalKey::String("k".into())),
                member_kind: MutationKind::Create,
                before_version: None,
                after_content_hash: Some(*blake3::hash(b"secret").as_bytes()),
                event_id: event,
            };
            let plan = AtomicPlan::close(AtomicPlanParts {
                profile: AtomicProfile::LocalHeapV1,
                atomic_id: id,
                heap_id: heap,
                scope: CoordinationScope::LocalHeap,
                read_frontier: None,
                reads: vec![],
                predicates: vec![],
                mutations: vec![PlanMutation {
                    kind: MutationKind::Create,
                    collection_id: collection,
                    key: CanonicalKey::String("k".into()),
                    encoded_value: Some(b"secret".to_vec()),
                    if_version: None,
                }],
                active_rule_revisions: vec![],
                limits: ResourceLimits::hard_local_heap(),
            })
            .expect("closed plan");
            if let Some(ref name) = fp {
                let leaked: &'static str = Box::leak(name.clone().into_boxed_str());
                arm_failpoint_once(leaked, FailpointAction::Abort);
            }
            match s.atomic_stage().and_then(|mut stage| {
                stage
                    .begin_prepare(&plan, [0xA1; 32], std::slice::from_ref(&member))
                    .map(|_| ())
            }) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("atomic prepare: {e}");
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
