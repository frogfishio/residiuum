//! Staged-genesis authority commit (`HEAP_SPEC` §8.9.1 / HP-005).

use crate::error::{AuthorityError, AuthorityStoreError};
use crate::head::{AccessPolicy, AuthorityHead, RecoveryProfile};
use crate::provider::MasterKeyProvider;
use crate::reload::{notify_reload, ReloadNotify};
use crate::slot::sha256;
use crate::store::{AuthorityPaths, MasterAuthorityStore};
use residiuum_format::{encode_deterministic_uint_map, CborValue};
use residiuum_heap::HeapAdministrativeState;
use residiuum_store::{
    load_staged_genesis, publish_staged_genesis, stage_heap_genesis, HeapMetaLayout, StagedGenesis,
};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Genesis ceremony inputs.
#[derive(Debug, Clone)]
pub struct GenesisRequest {
    /// Authority root (separate from data root in qualified profiles).
    pub authority_root: PathBuf,
    /// Data root for staged / published descriptors.
    pub data_root: PathBuf,
    /// Deployment id.
    pub deployment_id: [u8; 16],
    /// Heap id (UUIDv4).
    pub heap_id: [u8; 16],
    /// Canonical heap name.
    pub name: String,
    /// Creation event id.
    pub creation_event_id: [u8; 16],
    /// Unix seconds floor / effective-at.
    pub effective_at: u64,
}

/// Successful genesis outcomes.
#[derive(Debug, Clone)]
pub struct GenesisResult {
    /// Staging id used before publication.
    pub staging_id: [u8; 16],
    /// Storage genesis descriptor hash (head labels 25/26).
    pub descriptor_hash: [u8; 32],
    /// Authority-chain head event hash.
    pub authority_chain_head_hash: [u8; 32],
    /// Master public key committed.
    pub master_public_key: [u8; 32],
}

/// Durable inputs required to resume a staged Heap genesis after interruption.
///
/// Callers that need restart-safe orchestration must persist this value (or the
/// equivalent fields) before calling [`commit_prepared_genesis`].
#[derive(Debug, Clone)]
pub struct PreparedGenesis {
    /// Original, authority-bound request.
    pub request: GenesisRequest,
    /// Non-discoverable storage genesis staged under the data root.
    pub staged: StagedGenesis,
    /// Stable authority-root event id used by retries and receipts.
    pub root_event_id: [u8; 16],
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Encode §31.4.1 authority-root event body (creation).
fn encode_root_event(
    req: &GenesisRequest,
    master_pk: &[u8; 32],
    genesis_hash: &[u8; 32],
    root_event_id: [u8; 16],
    possession_sig: [u8; 64],
    master_sig: Option<[u8; 64]>,
) -> Result<Vec<u8>, AuthorityError> {
    let master_sig_v = match master_sig {
        None => CborValue::Null,
        Some(s) => CborValue::Bytes(s.to_vec()),
    };
    encode_deterministic_uint_map(&[
        (1u64, CborValue::Uint(1)),
        (2, CborValue::Bytes(req.deployment_id.to_vec())),
        (3, CborValue::Bytes(req.heap_id.to_vec())),
        (4, CborValue::Uint(0)), // from epoch
        (5, CborValue::Uint(1)), // to epoch
        (6, CborValue::Uint(1)), // reason: creation
        (7, CborValue::Uint(1)), // new generation
        (8, CborValue::Bytes(master_pk.to_vec())),
        (9, CborValue::Uint(1)), // no-master-recovery
        (10, CborValue::Array(vec![])),
        (11, CborValue::Uint(0)),
        (12, CborValue::Bytes(root_event_id.to_vec())),
        (13, CborValue::Uint(req.effective_at)),
        (14, CborValue::Bytes([0u8; 32].to_vec())),
        (15, master_sig_v),
        (16, CborValue::Array(vec![])),
        (17, CborValue::Bytes(possession_sig.to_vec())),
        (18, CborValue::Bytes(genesis_hash.to_vec())),
        (19, CborValue::Bytes(genesis_hash.to_vec())),
    ])
    .map_err(|e| AuthorityError::Crypto(e.to_string()))
}

/// Stage storage genesis, commit authority root, publish byte-identical staged bytes.
///
/// Crash before the authority selector/anchor commit leaves staged bytes
/// invisible to published catalogs. Crash after requires publishing the exact
/// staged hash or fail closed.
pub fn commit_genesis(
    provider: &dyn MasterKeyProvider,
    req: GenesisRequest,
) -> Result<GenesisResult, AuthorityError> {
    let prepared = prepare_genesis(req)?;
    commit_prepared_genesis(provider, &prepared)
}

/// Stage a non-discoverable storage genesis and allocate the stable authority
/// event identity needed for an interruption-safe commit.
pub fn prepare_genesis(req: GenesisRequest) -> Result<PreparedGenesis, AuthorityError> {
    let layout = HeapMetaLayout::new(&req.data_root);
    let staged: StagedGenesis = stage_heap_genesis(
        &layout,
        req.deployment_id,
        req.heap_id,
        req.creation_event_id,
        &req.name,
    )
    .map_err(|e| AuthorityError::Provisioning(e.to_string()))?;
    Ok(PreparedGenesis {
        request: req,
        staged,
        root_event_id: random_id16()?,
    })
}

/// Commit and publish a previously prepared genesis.
///
/// This operation is idempotent for the exact prepared request. If authority
/// committed before an interruption, retry validates the anchored head and
/// completes publication of the byte-identical staged descriptor. Conflicting
/// authority or published storage fails closed.
pub fn commit_prepared_genesis(
    provider: &dyn MasterKeyProvider,
    prepared: &PreparedGenesis,
) -> Result<GenesisResult, AuthorityError> {
    let req = &prepared.request;
    let staged = &prepared.staged;
    if staged.heap_id != req.heap_id {
        return Err(AuthorityError::InvalidArgument(
            "prepared heap id does not match request".into(),
        ));
    }
    if staged.name != req.name {
        return Err(AuthorityError::InvalidArgument(
            "prepared heap name does not match request".into(),
        ));
    }
    let layout = HeapMetaLayout::new(&req.data_root);

    let master_pk = provider.public_key();
    let root_event_id = prepared.root_event_id;
    // New-master possession: sign domain || 0x00 || SHA-256(labels 1..14 map without sigs).
    // Simplified HP-005: sign genesis hash under possession domain.
    let mut possession_msg = Vec::new();
    possession_msg.extend_from_slice(b"RESIDIUUM-HEAP-NEW-MASTER-POSSESSION-V1");
    possession_msg.push(0);
    possession_msg.extend_from_slice(&staged.descriptor_hash);
    let possession_sig = provider.sign(&possession_msg)?;

    let body = encode_root_event(
        req,
        &master_pk,
        &staged.descriptor_hash,
        root_event_id,
        possession_sig,
        None,
    )?;
    // Event file wraps body; chain hash is SHA-256 of the complete event file.
    // We compute head chain hash after wrapping inside commit_head — build head
    // with a placeholder then fix by constructing event hash the same way.
    let prev_event = [0u8; 32];
    let event_file = encode_deterministic_uint_map(&[
        (1u64, CborValue::Uint(1)),
        (2, CborValue::Uint(1)), // root
        (3, CborValue::Bytes(body.clone())),
        (4, CborValue::Bytes(prev_event.to_vec())),
    ])
    .map_err(|e| AuthorityError::Crypto(e.to_string()))?;
    let event_hash = sha256(&event_file);

    let floor = req.effective_at.max(1);
    let head = AuthorityHead {
        deployment_id: req.deployment_id,
        heap_id: req.heap_id,
        authority_epoch: 1,
        security_revision: 1,
        authority_revision: 1,
        state_revision: 1,
        policy_revision: 1,
        heap_state: HeapAdministrativeState::Active,
        master_generation: 1,
        master_public_key: master_pk,
        previous_generation: None,
        previous_public_key: None,
        grace_deadline: None,
        blacklist: vec![],
        trusted_time_floor: floor,
        authority_chain_head_hash: event_hash,
        recovery_profile: RecoveryProfile::NoMasterRecovery,
        file_sequence: 1,
        access_policy: AccessPolicy::default_open(),
        storage_genesis_hash: staged.descriptor_hash,
        current_descriptor_hash: staged.descriptor_hash,
    };

    let paths = AuthorityPaths::new(&req.authority_root, &req.deployment_id, &req.heap_id);
    let store = MasterAuthorityStore::open(paths)?;
    let existing = store.load_head()?;
    if let Some(existing) = existing.as_ref() {
        validate_retry_head(existing, req, staged, &master_pk, &event_hash)?;
    } else {
        store.commit_head(&head, 1, &body, prev_event)?;
    }

    // After authority commit: must publish byte-identical staged genesis.
    match residiuum_store::rebuild_heap_entry_from_chain(&layout, &req.heap_id)
        .map_err(|e| AuthorityError::Provisioning(e.to_string()))?
    {
        Some(published) => {
            if published.heap_id != req.heap_id
                || published.name != req.name
                || published.origin_deployment_id != req.deployment_id
                || published.descriptor_hash != staged.descriptor_hash
            {
                return Err(AuthorityStoreError::StagedGenesisConflict.into());
            }
        }
        None => {
            let still = load_staged_genesis(&layout, &staged.staging_id)
                .map_err(|e| AuthorityError::Provisioning(e.to_string()))?
                .ok_or(AuthorityStoreError::StagedGenesisConflict)?;
            if still != *staged {
                return Err(AuthorityStoreError::StagedGenesisConflict.into());
            }
            publish_staged_genesis(&layout, &staged.staging_id, &staged.descriptor_hash)
                .map_err(|e| AuthorityError::Provisioning(e.to_string()))?;
        }
    }

    let receipt = encode_deterministic_uint_map(&[
        (1u64, CborValue::Uint(1)),
        (2, CborValue::Text("genesis".into())),
        (3, CborValue::Bytes(req.heap_id.to_vec())),
        (4, CborValue::Bytes(staged.descriptor_hash.to_vec())),
        (5, CborValue::Uint(now_secs())),
    ])
    .map_err(|e| AuthorityError::Crypto(e.to_string()))?;
    store.write_receipt(1, &root_event_id, &receipt)?;

    // Notify data-plane reload (file drop). Reload path cannot mutate authority.
    let _ = notify_reload(
        Path::new(&req.data_root),
        &ReloadNotify {
            deployment_id: req.deployment_id,
            heap_id: req.heap_id,
            authority_root: req.authority_root.clone(),
            chain_head_hash: event_hash,
        },
    );

    Ok(GenesisResult {
        staging_id: staged.staging_id,
        descriptor_hash: staged.descriptor_hash,
        authority_chain_head_hash: event_hash,
        master_public_key: master_pk,
    })
}

fn validate_retry_head(
    head: &AuthorityHead,
    req: &GenesisRequest,
    staged: &StagedGenesis,
    master_pk: &[u8; 32],
    event_hash: &[u8; 32],
) -> Result<(), AuthorityError> {
    let exact = head.deployment_id == req.deployment_id
        && head.heap_id == req.heap_id
        && head.authority_epoch == 1
        && head.security_revision == 1
        && head.authority_revision == 1
        && head.master_generation == 1
        && &head.master_public_key == master_pk
        && &head.authority_chain_head_hash == event_hash
        && head.storage_genesis_hash == staged.descriptor_hash
        && head.current_descriptor_hash == staged.descriptor_hash;
    if exact {
        Ok(())
    } else {
        Err(AuthorityError::Refused(
            "existing heap authority conflicts with prepared genesis".into(),
        ))
    }
}

fn random_id16() -> Result<[u8; 16], AuthorityError> {
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).map_err(|e| AuthorityError::Crypto(e.to_string()))?;
    id[6] = (id[6] & 0x0f) | 0x40;
    id[8] = (id[8] & 0x3f) | 0x80;
    if id == [0u8; 16] {
        return Err(AuthorityError::Crypto("zero id".into()));
    }
    Ok(id)
}
