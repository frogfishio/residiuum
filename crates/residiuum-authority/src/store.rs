//! Filesystem master authority store (`HEAP_SPEC` §35).

use crate::error::{AuthorityError, AuthorityStoreError};
use crate::head::AuthorityHead;
use crate::slot::{
    decode_slot_file, encode_slot_file, read_selector, sha256, slot_path, write_atomic,
    write_selector, Slot,
};
use residiuum_format::{decode_deterministic_uint_map, encode_deterministic_uint_map, CborValue};
use std::fs;
use std::path::{Path, PathBuf};

/// Paths under `authority_root/<deployment>/<heap>/`.
#[derive(Debug, Clone)]
pub struct AuthorityPaths {
    root: PathBuf,
}

impl AuthorityPaths {
    /// Construct for one heap under `authority_root`.
    pub fn new(
        authority_root: impl AsRef<Path>,
        deployment_id: &[u8; 16],
        heap_id: &[u8; 16],
    ) -> Self {
        Self {
            root: authority_root
                .as_ref()
                .join(hex16(deployment_id))
                .join(hex16(heap_id)),
        }
    }

    /// Heap authority directory.
    pub fn heap_dir(&self) -> &Path {
        &self.root
    }

    fn head_path(&self, slot: Slot) -> PathBuf {
        slot_path(&self.root, "head", slot)
    }

    fn time_path(&self, slot: Slot) -> PathBuf {
        slot_path(&self.root, "time-floor", slot)
    }

    fn current(&self) -> PathBuf {
        self.root.join("current")
    }

    fn time_current(&self) -> PathBuf {
        self.root.join("time-current")
    }

    fn anchor(&self) -> PathBuf {
        self.root.join("anchor.cbor")
    }

    fn events_dir(&self, epoch: u64) -> PathBuf {
        self.root.join("events").join(format!("{epoch:020}"))
    }

    fn event_path(&self, epoch: u64, revision: u64) -> PathBuf {
        self.events_dir(epoch).join(format!("{revision:020}.cbor"))
    }

    fn receipts_dir(&self, epoch: u64) -> PathBuf {
        self.root.join("receipts").join(format!("{epoch:020}"))
    }
}

/// Anchor value: selected head hash + monotonic counter + time floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchorValue {
    /// Monotonic publication counter.
    pub monotonic_counter: u64,
    /// Selected head payload hash (SHA-256 of head payload bytes).
    pub head_hash: [u8; 32],
    /// Security time floor seconds.
    pub security_time_floor: u64,
}

impl AnchorValue {
    fn encode(&self) -> Result<Vec<u8>, AuthorityStoreError> {
        encode_deterministic_uint_map(&[
            (1u64, CborValue::Uint(1)),
            (2, CborValue::Uint(self.monotonic_counter)),
            (3, CborValue::Bytes(self.head_hash.to_vec())),
            (4, CborValue::Uint(self.security_time_floor)),
        ])
        .map_err(|_| AuthorityStoreError::Corrupt("anchor encode"))
    }

    fn decode(bytes: &[u8]) -> Result<Self, AuthorityStoreError> {
        let map = decode_deterministic_uint_map(bytes)
            .map_err(|_| AuthorityStoreError::Corrupt("anchor cbor"))?;
        let mut counter = None;
        let mut hash = None;
        let mut floor = None;
        let mut profile = None;
        for (k, v) in map {
            match k {
                1 => profile = Some(expect_uint(&v)?),
                2 => counter = Some(expect_uint(&v)?),
                3 => hash = Some(expect_b32(&v)?),
                4 => floor = Some(expect_uint(&v)?),
                _ => return Err(AuthorityStoreError::Corrupt("anchor key")),
            }
        }
        if profile != Some(1) {
            return Err(AuthorityStoreError::Corrupt("anchor profile"));
        }
        Ok(Self {
            monotonic_counter: counter.ok_or(AuthorityStoreError::Corrupt("anchor counter"))?,
            head_hash: hash.ok_or(AuthorityStoreError::Corrupt("anchor hash"))?,
            security_time_floor: floor.ok_or(AuthorityStoreError::Corrupt("anchor floor"))?,
        })
    }
}

/// Two-slot authority store for one heap.
pub struct MasterAuthorityStore {
    paths: AuthorityPaths,
}

impl MasterAuthorityStore {
    /// Open or create the heap authority directory.
    pub fn open(paths: AuthorityPaths) -> Result<Self, AuthorityError> {
        fs::create_dir_all(paths.heap_dir())?;
        Ok(Self { paths })
    }

    /// Paths.
    pub fn paths(&self) -> &AuthorityPaths {
        &self.paths
    }

    /// Load the anchored head, validating both slots and fail-closed on fork.
    pub fn load_head(&self) -> Result<Option<AuthorityHead>, AuthorityError> {
        let anchor_path = self.paths.anchor();
        if !anchor_path.is_file() {
            return Ok(None);
        }
        let anchor = AnchorValue::decode(&fs::read(&anchor_path)?)?;
        let slot_a = self.try_load_slot(Slot::A)?;
        let slot_b = self.try_load_slot(Slot::B)?;
        let mut matches = Vec::new();
        if let Some((h, hash)) = slot_a {
            if hash == anchor.head_hash {
                if h.trusted_time_floor != anchor.security_time_floor {
                    return Err(AuthorityStoreError::Corrupt("floor disagree").into());
                }
                matches.push(h);
            }
        }
        if let Some((h, hash)) = slot_b {
            if hash == anchor.head_hash {
                if h.trusted_time_floor != anchor.security_time_floor {
                    return Err(AuthorityStoreError::Corrupt("floor disagree").into());
                }
                matches.push(h);
            }
        }
        match matches.len() {
            0 => Err(AuthorityStoreError::AnchorMismatch.into()),
            1 => Ok(Some(matches.pop().unwrap())),
            _ => {
                // Identical payloads OK; unequal equal-hash impossible.
                if matches[0] == matches[1] {
                    Ok(Some(matches.pop().unwrap()))
                } else {
                    Err(AuthorityStoreError::Fork.into())
                }
            }
        }
    }

    fn try_load_slot(
        &self,
        slot: Slot,
    ) -> Result<Option<(AuthorityHead, [u8; 32])>, AuthorityError> {
        let path = self.paths.head_path(slot);
        if !path.is_file() {
            return Ok(None);
        }
        let file = fs::read(&path)?;
        let payload = decode_slot_file(&file)?;
        let hash = sha256(&payload);
        let head = AuthorityHead::decode_payload(&payload)?;
        Ok(Some((head, hash)))
    }

    /// Commit a new head into the inactive slot, advance anchor, flip selector.
    ///
    /// Also appends an event file and advances the time floor.
    pub fn commit_head(
        &self,
        head: &AuthorityHead,
        event_kind: u64,
        event_body: &[u8],
        previous_event_hash: [u8; 32],
    ) -> Result<[u8; 32], AuthorityError> {
        let payload = head.encode_payload()?;
        let head_hash = sha256(&payload);
        let event = encode_deterministic_uint_map(&[
            (1u64, CborValue::Uint(1)),
            (2, CborValue::Uint(event_kind)),
            (3, CborValue::Bytes(event_body.to_vec())),
            (4, CborValue::Bytes(previous_event_hash.to_vec())),
        ])
        .map_err(|e| AuthorityError::Crypto(e.to_string()))?;
        let event_hash = sha256(&event);
        if head.authority_chain_head_hash != event_hash {
            return Err(AuthorityError::InvalidArgument(
                "head chain hash must equal event hash".into(),
            ));
        }

        // 1. Write event
        let event_path = self
            .paths
            .event_path(head.authority_epoch, head.authority_revision);
        write_atomic(&event_path, &event)?;

        // 2. Choose inactive slot
        let hint = read_selector(&self.paths.current())?.unwrap_or(Slot::A);
        let inactive = if self.paths.head_path(hint).is_file() {
            hint.other()
        } else {
            hint
        };

        // 3. Write inactive head slot
        let slot_bytes = encode_slot_file(&payload)?;
        write_atomic(&self.paths.head_path(inactive), &slot_bytes)?;

        // 4. Write time floor inactive slot (same floor as head label 16)
        let tf_payload = encode_deterministic_uint_map(&[
            (1u64, CborValue::Uint(1)),
            (2, CborValue::Bytes(head.deployment_id.to_vec())),
            (3, CborValue::Bytes(head.heap_id.to_vec())),
            (4, CborValue::Uint(head.trusted_time_floor)),
            (5, CborValue::Uint(head.file_sequence)),
            (6, CborValue::Uint(0)),
            (
                7,
                CborValue::Uint(head.trusted_time_floor.saturating_mul(1_000_000_000)),
            ),
            (8, CborValue::Uint(0)),
            (
                9,
                CborValue::Array(vec![CborValue::Uint(0), CborValue::Uint(0)]),
            ),
        ])
        .map_err(|e| AuthorityError::Crypto(e.to_string()))?;
        let tf_slot = encode_slot_file(&tf_payload)?;
        let tf_hint = read_selector(&self.paths.time_current())?.unwrap_or(Slot::A);
        let tf_inactive = if self.paths.time_path(tf_hint).is_file() {
            tf_hint.other()
        } else {
            tf_hint
        };
        write_atomic(&self.paths.time_path(tf_inactive), &tf_slot)?;

        // 5. Advance anchor (logical commit precedes selector; crash after this
        //    still recovers via anchor hash match).
        let prev_counter = if self.paths.anchor().is_file() {
            AnchorValue::decode(&fs::read(self.paths.anchor())?)?.monotonic_counter
        } else {
            0
        };
        let anchor = AnchorValue {
            monotonic_counter: prev_counter + 1,
            head_hash,
            security_time_floor: head.trusted_time_floor,
        };
        write_atomic(&self.paths.anchor(), &anchor.encode()?)?;

        // 6. Flip selectors (logical publication)
        write_selector(&self.paths.current(), inactive)?;
        write_selector(&self.paths.time_current(), tf_inactive)?;

        Ok(event_hash)
    }

    /// Write a local receipt after successful publication.
    pub fn write_receipt(
        &self,
        epoch: u64,
        operation_id: &[u8; 16],
        body: &[u8],
    ) -> Result<(), AuthorityError> {
        let dir = self.paths.receipts_dir(epoch);
        fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.cbor", hex16(operation_id)));
        write_atomic(&path, body)?;
        Ok(())
    }

    /// Detect equal-sequence unequal payloads across slots (fork).
    pub fn detect_slot_fork(&self) -> Result<(), AuthorityError> {
        let a = self.try_load_slot(Slot::A)?;
        let b = self.try_load_slot(Slot::B)?;
        if let (Some((ha, ha_hash)), Some((hb, hb_hash))) = (a, b) {
            if ha.file_sequence == hb.file_sequence && ha_hash != hb_hash {
                return Err(AuthorityStoreError::Fork.into());
            }
            if ha.authority_revision == hb.authority_revision
                && ha.authority_epoch == hb.authority_epoch
                && ha_hash != hb_hash
                && ha.file_sequence == hb.file_sequence
            {
                return Err(AuthorityStoreError::Fork.into());
            }
        }
        Ok(())
    }
}

fn hex16(id: &[u8; 16]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

fn expect_uint(v: &CborValue) -> Result<u64, AuthorityStoreError> {
    match v {
        CborValue::Uint(u) => Ok(*u),
        _ => Err(AuthorityStoreError::Corrupt("uint")),
    }
}

fn expect_b32(v: &CborValue) -> Result<[u8; 32], AuthorityStoreError> {
    match v {
        CborValue::Bytes(b) if b.len() == 32 => {
            let mut a = [0u8; 32];
            a.copy_from_slice(b);
            Ok(a)
        }
        _ => Err(AuthorityStoreError::Corrupt("bstr32")),
    }
}
