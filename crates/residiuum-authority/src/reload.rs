//! Local security-barrier / reload notify (`HEAP_SPEC` §8.9 / HP-005).
//!
//! The data service may observe a reload request and re-load the anchored
//! authority head. It MUST NOT mutate authority state through this path.

use crate::error::AuthorityError;
use crate::store::{AuthorityPaths, MasterAuthorityStore};
use residiuum_format::{decode_deterministic_uint_map, encode_deterministic_uint_map, CborValue};
use residiuum_heap::{
    AuthorityEpoch, AuthorityGeneration, DeploymentId, HeapAdministrativeState, HeapId,
    HeapSecuritySnapshot, SecurityRevision,
};
use std::fs;
use std::path::{Path, PathBuf};

const RELOAD_DIR: &str = "authority-reload";
const RELOAD_FILE: &str = "pending.cbor";

/// Reload notification written by `residiuum-authority`.
#[derive(Debug, Clone)]
pub struct ReloadNotify {
    /// Deployment.
    pub deployment_id: [u8; 16],
    /// Heap.
    pub heap_id: [u8; 16],
    /// Authority root path (string in file).
    pub authority_root: PathBuf,
    /// Expected chain head hash.
    pub chain_head_hash: [u8; 32],
}

/// Decoded reload request (read-only for data plane).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadRequest {
    /// Deployment.
    pub deployment_id: [u8; 16],
    /// Heap.
    pub heap_id: [u8; 16],
    /// Authority root as stored.
    pub authority_root: String,
    /// Expected chain head.
    pub chain_head_hash: [u8; 32],
}

fn reload_path(data_root: &Path) -> PathBuf {
    data_root.join(RELOAD_DIR).join(RELOAD_FILE)
}

/// Drop a reload request for the data service to observe.
pub fn notify_reload(data_root: &Path, notify: &ReloadNotify) -> Result<(), AuthorityError> {
    let dir = data_root.join(RELOAD_DIR);
    fs::create_dir_all(&dir)?;
    let bytes = encode_deterministic_uint_map(&[
        (1u64, CborValue::Uint(1)),
        (2, CborValue::Bytes(notify.deployment_id.to_vec())),
        (3, CborValue::Bytes(notify.heap_id.to_vec())),
        (
            4,
            CborValue::Text(notify.authority_root.to_string_lossy().into_owned()),
        ),
        (5, CborValue::Bytes(notify.chain_head_hash.to_vec())),
    ])
    .map_err(|e| AuthorityError::Crypto(e.to_string()))?;
    crate::slot::write_atomic(&reload_path(data_root), &bytes)?;
    Ok(())
}

/// Peek pending reload request without applying.
pub fn peek_reload_request(data_root: &Path) -> Result<Option<ReloadRequest>, AuthorityError> {
    let path = reload_path(data_root);
    if !path.is_file() {
        return Ok(None);
    }
    let map = decode_deterministic_uint_map(&fs::read(path)?)
        .map_err(|e| AuthorityError::Crypto(e.to_string()))?;
    let mut deployment = None;
    let mut heap = None;
    let mut root = None;
    let mut hash = None;
    for (k, v) in map {
        match k {
            2 => deployment = Some(expect_b16(&v)?),
            3 => heap = Some(expect_b16(&v)?),
            4 => match v {
                CborValue::Text(s) => root = Some(s),
                _ => return Err(AuthorityError::Crypto("reload root".into())),
            },
            5 => hash = Some(expect_b32(&v)?),
            _ => {}
        }
    }
    Ok(Some(ReloadRequest {
        deployment_id: deployment
            .ok_or_else(|| AuthorityError::Crypto("reload deployment".into()))?,
        heap_id: heap.ok_or_else(|| AuthorityError::Crypto("reload heap".into()))?,
        authority_root: root.ok_or_else(|| AuthorityError::Crypto("reload root".into()))?,
        chain_head_hash: hash.ok_or_else(|| AuthorityError::Crypto("reload hash".into()))?,
    }))
}

/// Data-plane reload: load anchored head into a resident snapshot.
///
/// This function has **no** write path into the authority store except clearing
/// the pending reload file after a successful read.
pub fn apply_reload_request(
    data_root: &Path,
) -> Result<Option<HeapSecuritySnapshot>, AuthorityError> {
    let Some(req) = peek_reload_request(data_root)? else {
        return Ok(None);
    };
    let paths = AuthorityPaths::new(
        Path::new(&req.authority_root),
        &req.deployment_id,
        &req.heap_id,
    );
    let store = MasterAuthorityStore::open(paths)?;
    let head = store
        .load_head()?
        .ok_or_else(|| AuthorityError::Refused("reload: no head".into()))?;
    if head.authority_chain_head_hash != req.chain_head_hash {
        return Err(AuthorityError::Refused(
            "reload: chain head mismatch".into(),
        ));
    }
    let snap = HeapSecuritySnapshot {
        deployment_id: DeploymentId::from_bytes_unchecked_nonzero(head.deployment_id)
            .map_err(|e| AuthorityError::Heap(e.to_string()))?,
        heap_id: HeapId::from_bytes_unchecked_nonzero(head.heap_id)
            .map_err(|e| AuthorityError::Heap(e.to_string()))?,
        authority_epoch: AuthorityEpoch::new(head.authority_epoch)
            .map_err(|e| AuthorityError::Heap(e.to_string()))?,
        authority_generation: AuthorityGeneration::new(head.master_generation)
            .map_err(|e| AuthorityError::Heap(e.to_string()))?,
        previous_generation: match head.previous_generation {
            None => None,
            Some(g) => {
                Some(AuthorityGeneration::new(g).map_err(|e| AuthorityError::Heap(e.to_string()))?)
            }
        },
        grace_deadline_unix_s: head.grace_deadline,
        master_public_key: head.master_public_key,
        previous_master_public_key: head.previous_public_key,
        security_revision: SecurityRevision::new(head.security_revision)
            .map_err(|e| AuthorityError::Heap(e.to_string()))?,
        authority_chain_head_hash: head.authority_chain_head_hash,
        administrative_state: head.heap_state,
        blacklist: head.blacklist.clone(),
        policy_rights_ceiling: None,
    };
    // Clear pending request only; never write authority slots here.
    let _ = fs::remove_file(reload_path(data_root));
    let _ = snap.administrative_state == HeapAdministrativeState::Active;
    Ok(Some(snap))
}

fn expect_b16(v: &CborValue) -> Result<[u8; 16], AuthorityError> {
    match v {
        CborValue::Bytes(b) if b.len() == 16 => {
            let mut a = [0u8; 16];
            a.copy_from_slice(b);
            Ok(a)
        }
        _ => Err(AuthorityError::Crypto("bstr16".into())),
    }
}

fn expect_b32(v: &CborValue) -> Result<[u8; 32], AuthorityError> {
    match v {
        CborValue::Bytes(b) if b.len() == 32 => {
            let mut a = [0u8; 32];
            a.copy_from_slice(b);
            Ok(a)
        }
        _ => Err(AuthorityError::Crypto("bstr32".into())),
    }
}
