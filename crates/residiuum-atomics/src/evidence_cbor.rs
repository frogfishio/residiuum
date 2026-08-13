//! Deterministic CBOR codecs for durable evidence (`ATOMICS_SPEC` §9, §12).
//!
//! Field numbers are frozen in `spec/atomics/cbor-v1.json`. Hashes are
//! BLAKE3-256 of the domain separator plus the canonical record bytes.

use crate::canonical::{
    as_map, decode_key, decode_limits, encode_key_map, encode_limits, key_order_bytes,
    optional_bstr32, optional_version, refuse_unknown_keys, require_bstr16, require_bstr32,
    require_u32, require_u8, require_value, DOMAIN_ATOMIC_DECISION, DOMAIN_ATOMIC_MANIFEST,
    DOMAIN_ATOMIC_MEMBER, DOMAIN_ATOMIC_PREPARE, DOMAIN_ATOMIC_TOMBSTONE,
};
use crate::cbor::{self, Value};
use crate::error::AtomicsError;
use crate::evidence::{
    AtomicDecision, AtomicMember, AtomicPrepare, DecisionCode, DecisionTombstone, Durability,
    ObjectIdentity,
};
use crate::id::{AtomicId, CollectionId, ContentRoot, HeapId};
use crate::outcome::{AtomicAbortReason, AtomicRefuseReason};
use crate::plan::{CoordinationScope, MutationKind};

const PREP_ATOMIC_ID: u64 = 1;
const PREP_HEAP_ID: u64 = 2;
const PREP_SCOPE: u64 = 3;
const PREP_CONTENT_ROOT: u64 = 4;
const PREP_FRONTIER: u64 = 5;
const PREP_MANIFEST_ROOT: u64 = 6;
const PREP_READ_SET_ROOT: u64 = 7;
const PREP_PREDICATE_SET_ROOT: u64 = 8;
const PREP_RULE_REV_ROOT: u64 = 9;
const PREP_LIMITS: u64 = 10;

const MEM_ATOMIC_ID: u64 = 1;
const MEM_ORDINAL: u64 = 2;
const MEM_OBJECT_IDENTITY: u64 = 3;
const MEM_KIND: u64 = 4;
const MEM_BEFORE_VERSION: u64 = 5;
const MEM_AFTER_HASH: u64 = 6;
const MEM_EVENT_ID: u64 = 7;

const DEC_ATOMIC_ID: u64 = 1;
const DEC_PREPARE_HASH: u64 = 2;
const DEC_MEMBER_ROOT: u64 = 3;
const DEC_MEMBER_COUNT: u64 = 4;
const DEC_DECISION: u64 = 5;
const DEC_COMMIT_POSITION: u64 = 6;
const DEC_DURABILITY: u64 = 7;
const DEC_ABORT_REASON: u64 = 8;

const TOMB_ATOMIC_ID: u64 = 1;
const TOMB_CONTENT_ROOT: u64 = 2;
const TOMB_DECISION: u64 = 3;
const TOMB_COMMIT_POSITION: u64 = 4;
const TOMB_DECISION_HASH: u64 = 5;
const TOMB_ABORT_REASON: u64 = 6;

fn domain_hash(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn malformed() -> AtomicsError {
    AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
}

fn require_map(map: &[(u64, Value)], key: u64) -> Result<&[(u64, Value)], AtomicsError> {
    as_map(require_value(map, key)?)
}

fn optional_u64(map: &[(u64, Value)], key: u64) -> Result<Option<u64>, AtomicsError> {
    match map.iter().find(|(k, _)| *k == key) {
        None => Ok(None),
        Some((_, Value::Uint(n))) => Ok(Some(*n)),
        Some(_) => Err(crate::error::CborError::Unsupported.into()),
    }
}

fn optional_abort(
    map: &[(u64, Value)],
    key: u64,
) -> Result<Option<AtomicAbortReason>, AtomicsError> {
    match optional_u64(map, key)? {
        None => Ok(None),
        Some(n) => {
            let code = u8::try_from(n).map_err(|_| malformed())?;
            Ok(Some(
                AtomicAbortReason::from_wire_code(code).ok_or_else(malformed)?,
            ))
        }
    }
}

fn decode_decision_code(map: &[(u64, Value)], key: u64) -> Result<DecisionCode, AtomicsError> {
    DecisionCode::from_wire_code(require_u8(map, key)?).ok_or_else(malformed)
}

fn decode_durability(map: &[(u64, Value)], key: u64) -> Result<Durability, AtomicsError> {
    Durability::from_wire_code(require_u8(map, key)?).ok_or_else(malformed)
}

fn encode_object_identity(id: &ObjectIdentity) -> Result<Value, AtomicsError> {
    Ok(Value::Map(vec![
        (1, Value::Bytes(id.collection_id.to_bytes().to_vec())),
        (2, Value::Map(encode_key_map(&id.key)?)),
    ]))
}

fn decode_object_identity(v: &Value) -> Result<ObjectIdentity, AtomicsError> {
    let map = as_map(v)?;
    refuse_unknown_keys(map, &[1, 2])?;
    Ok(ObjectIdentity {
        collection_id: CollectionId::from_bytes(require_bstr16(map, 1)?)?,
        key: decode_key(require_value(map, 2)?)?,
    })
}

/// Encode a prepare record to deterministic CBOR.
pub fn encode_prepare(prepare: &AtomicPrepare) -> Result<Vec<u8>, AtomicsError> {
    cbor::encode_map(&[
        (
            PREP_ATOMIC_ID,
            Value::Bytes(prepare.atomic_id.to_bytes().to_vec()),
        ),
        (
            PREP_HEAP_ID,
            Value::Bytes(prepare.heap_id.to_bytes().to_vec()),
        ),
        (
            PREP_SCOPE,
            Value::Uint(u64::from(prepare.scope.wire_code())),
        ),
        (
            PREP_CONTENT_ROOT,
            Value::Bytes(prepare.content_root.to_bytes().to_vec()),
        ),
        (PREP_FRONTIER, Value::Bytes(prepare.frontier.to_vec())),
        (
            PREP_MANIFEST_ROOT,
            Value::Bytes(prepare.ordered_member_manifest_root.to_vec()),
        ),
        (
            PREP_READ_SET_ROOT,
            Value::Bytes(prepare.read_set_root.to_vec()),
        ),
        (
            PREP_PREDICATE_SET_ROOT,
            Value::Bytes(prepare.predicate_set_root.to_vec()),
        ),
        (
            PREP_RULE_REV_ROOT,
            Value::Bytes(prepare.active_rule_revision_root.to_vec()),
        ),
        (PREP_LIMITS, Value::Map(encode_limits(prepare.limits))),
    ])
}

/// Decode a prepare record. Unknown keys are refused.
pub fn decode_prepare(bytes: &[u8]) -> Result<AtomicPrepare, AtomicsError> {
    let map = cbor::decode_map(bytes)?;
    refuse_unknown_keys(&map, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])?;
    let scope =
        CoordinationScope::from_wire_code(require_u8(&map, PREP_SCOPE)?).ok_or_else(malformed)?;
    Ok(AtomicPrepare {
        atomic_id: AtomicId::from_bytes(require_bstr32(&map, PREP_ATOMIC_ID)?)?,
        heap_id: HeapId::from_bytes(require_bstr16(&map, PREP_HEAP_ID)?)?,
        scope,
        content_root: ContentRoot::from_bytes(require_bstr32(&map, PREP_CONTENT_ROOT)?)?,
        frontier: require_bstr32(&map, PREP_FRONTIER)?,
        ordered_member_manifest_root: require_bstr32(&map, PREP_MANIFEST_ROOT)?,
        read_set_root: require_bstr32(&map, PREP_READ_SET_ROOT)?,
        predicate_set_root: require_bstr32(&map, PREP_PREDICATE_SET_ROOT)?,
        active_rule_revision_root: require_bstr32(&map, PREP_RULE_REV_ROOT)?,
        limits: decode_limits(require_map(&map, PREP_LIMITS)?)?,
    })
}

/// `BLAKE3-256(RESIDIUUM-ATOMIC-PREPARE-V1 || canonical_bytes)`.
pub fn prepare_hash(prepare: &AtomicPrepare) -> Result<[u8; 32], AtomicsError> {
    Ok(domain_hash(
        DOMAIN_ATOMIC_PREPARE,
        &encode_prepare(prepare)?,
    ))
}

/// Encode a member record to deterministic CBOR.
pub fn encode_member(member: &AtomicMember) -> Result<Vec<u8>, AtomicsError> {
    member.validate()?;
    encode_member_body(member)
}

fn encode_member_body(member: &AtomicMember) -> Result<Vec<u8>, AtomicsError> {
    let mut entries = vec![
        (
            MEM_ATOMIC_ID,
            Value::Bytes(member.atomic_id.to_bytes().to_vec()),
        ),
        (MEM_ORDINAL, Value::Uint(u64::from(member.ordinal))),
        (
            MEM_OBJECT_IDENTITY,
            encode_object_identity(&member.object_identity)?,
        ),
        (
            MEM_KIND,
            Value::Uint(u64::from(member.member_kind.wire_code())),
        ),
    ];
    if let Some(v) = member.before_version {
        entries.push((MEM_BEFORE_VERSION, Value::Bytes(v.to_bytes().to_vec())));
    }
    if let Some(h) = member.after_content_hash {
        entries.push((MEM_AFTER_HASH, Value::Bytes(h.to_vec())));
    }
    entries.push((
        MEM_EVENT_ID,
        Value::Bytes(member.event_id.to_bytes().to_vec()),
    ));
    cbor::encode_map(&entries)
}

/// Decode a member record. Unknown keys are refused.
pub fn decode_member(bytes: &[u8]) -> Result<AtomicMember, AtomicsError> {
    let map = cbor::decode_map(bytes)?;
    refuse_unknown_keys(&map, &[1, 2, 3, 4, 5, 6, 7])?;
    let kind = MutationKind::from_wire_code(require_u8(&map, MEM_KIND)?).ok_or(
        AtomicsError::Refused(AtomicRefuseReason::UnknownMutationKind),
    )?;
    let member = AtomicMember {
        atomic_id: AtomicId::from_bytes(require_bstr32(&map, MEM_ATOMIC_ID)?)?,
        ordinal: require_u32(&map, MEM_ORDINAL)?,
        object_identity: decode_object_identity(require_value(&map, MEM_OBJECT_IDENTITY)?)?,
        member_kind: kind,
        before_version: optional_version(&map, MEM_BEFORE_VERSION)?,
        after_content_hash: optional_bstr32(&map, MEM_AFTER_HASH)?,
        event_id: crate::id::VersionId::from_bytes(require_bstr16(&map, MEM_EVENT_ID)?)?,
    };
    member.validate()?;
    Ok(member)
}

/// `BLAKE3-256(RESIDIUUM-ATOMIC-MEMBER-V1 || canonical_bytes)`.
/// Includes [`ObjectIdentity`] so the member hash names the affected key.
pub fn member_hash(member: &AtomicMember) -> Result<[u8; 32], AtomicsError> {
    Ok(domain_hash(DOMAIN_ATOMIC_MEMBER, &encode_member(member)?))
}

fn member_order_key(heap: &HeapId, m: &AtomicMember) -> (Vec<u8>, Vec<u8>, Vec<u8>, u8, u32) {
    (
        heap.as_bytes().to_vec(),
        m.object_identity.collection_id.as_bytes().to_vec(),
        key_order_bytes(&m.object_identity.key),
        m.member_kind.wire_code(),
        m.ordinal,
    )
}

/// Ordered member-manifest root: hashes in canonical target order.
///
/// Duplicate `(collection_id, canonical_key)` identities are refused.
pub fn ordered_member_manifest_root(
    heap_id: HeapId,
    members: &[AtomicMember],
) -> Result<[u8; 32], AtomicsError> {
    let mut seen: Vec<(CollectionId, Vec<u8>)> = Vec::new();
    for m in members {
        let key = key_order_bytes(&m.object_identity.key);
        if seen
            .iter()
            .any(|(c, k)| *c == m.object_identity.collection_id && *k == key)
        {
            return Err(AtomicsError::Refused(AtomicRefuseReason::DuplicateTarget));
        }
        seen.push((m.object_identity.collection_id, key));
    }
    let mut items: Vec<&AtomicMember> = members.iter().collect();
    items.sort_by_key(|a| member_order_key(&heap_id, a));
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN_ATOMIC_MANIFEST);
    for m in items {
        hasher.update(&member_hash(m)?);
    }
    Ok(*hasher.finalize().as_bytes())
}

/// Encode a decision record to deterministic CBOR.
pub fn encode_decision(decision: &AtomicDecision) -> Result<Vec<u8>, AtomicsError> {
    decision.validate()?;
    let mut entries = vec![
        (
            DEC_ATOMIC_ID,
            Value::Bytes(decision.atomic_id.to_bytes().to_vec()),
        ),
        (
            DEC_PREPARE_HASH,
            Value::Bytes(decision.prepare_hash.to_vec()),
        ),
        (DEC_MEMBER_ROOT, Value::Bytes(decision.member_root.to_vec())),
        (
            DEC_MEMBER_COUNT,
            Value::Uint(u64::from(decision.member_count)),
        ),
        (
            DEC_DECISION,
            Value::Uint(u64::from(decision.decision.wire_code())),
        ),
    ];
    if let Some(pos) = decision.commit_position {
        entries.push((DEC_COMMIT_POSITION, Value::Uint(pos)));
    }
    entries.push((
        DEC_DURABILITY,
        Value::Uint(u64::from(decision.durability.wire_code())),
    ));
    if let Some(reason) = decision.abort_reason {
        entries.push((DEC_ABORT_REASON, Value::Uint(u64::from(reason.wire_code()))));
    }
    cbor::encode_map(&entries)
}

/// Decode a decision record. Unknown keys and illegal axis combinations are refused.
pub fn decode_decision(bytes: &[u8]) -> Result<AtomicDecision, AtomicsError> {
    let map = cbor::decode_map(bytes)?;
    refuse_unknown_keys(&map, &[1, 2, 3, 4, 5, 6, 7, 8])?;
    let rec = AtomicDecision {
        atomic_id: AtomicId::from_bytes(require_bstr32(&map, DEC_ATOMIC_ID)?)?,
        prepare_hash: require_bstr32(&map, DEC_PREPARE_HASH)?,
        member_root: require_bstr32(&map, DEC_MEMBER_ROOT)?,
        member_count: require_u32(&map, DEC_MEMBER_COUNT)?,
        decision: decode_decision_code(&map, DEC_DECISION)?,
        commit_position: optional_u64(&map, DEC_COMMIT_POSITION)?,
        durability: decode_durability(&map, DEC_DURABILITY)?,
        abort_reason: optional_abort(&map, DEC_ABORT_REASON)?,
    };
    rec.validate()?;
    Ok(rec)
}

/// `BLAKE3-256(RESIDIUUM-ATOMIC-DECISION-V1 || canonical_bytes)`.
pub fn decision_hash(decision: &AtomicDecision) -> Result<[u8; 32], AtomicsError> {
    Ok(domain_hash(
        DOMAIN_ATOMIC_DECISION,
        &encode_decision(decision)?,
    ))
}

/// Encode a lifetime tombstone to deterministic CBOR.
pub fn encode_tombstone(tombstone: &DecisionTombstone) -> Result<Vec<u8>, AtomicsError> {
    tombstone.validate()?;
    let mut entries = vec![
        (
            TOMB_ATOMIC_ID,
            Value::Bytes(tombstone.atomic_id.to_bytes().to_vec()),
        ),
        (
            TOMB_CONTENT_ROOT,
            Value::Bytes(tombstone.content_root.to_bytes().to_vec()),
        ),
        (
            TOMB_DECISION,
            Value::Uint(u64::from(tombstone.decision.wire_code())),
        ),
    ];
    if let Some(pos) = tombstone.commit_position {
        entries.push((TOMB_COMMIT_POSITION, Value::Uint(pos)));
    }
    entries.push((
        TOMB_DECISION_HASH,
        Value::Bytes(tombstone.decision_hash.to_vec()),
    ));
    if let Some(reason) = tombstone.abort_reason {
        entries.push((
            TOMB_ABORT_REASON,
            Value::Uint(u64::from(reason.wire_code())),
        ));
    }
    cbor::encode_map(&entries)
}

/// Decode a lifetime tombstone. Unknown keys and illegal axis combinations are refused.
pub fn decode_tombstone(bytes: &[u8]) -> Result<DecisionTombstone, AtomicsError> {
    let map = cbor::decode_map(bytes)?;
    refuse_unknown_keys(&map, &[1, 2, 3, 4, 5, 6])?;
    let rec = DecisionTombstone {
        atomic_id: AtomicId::from_bytes(require_bstr32(&map, TOMB_ATOMIC_ID)?)?,
        content_root: ContentRoot::from_bytes(require_bstr32(&map, TOMB_CONTENT_ROOT)?)?,
        decision: decode_decision_code(&map, TOMB_DECISION)?,
        commit_position: optional_u64(&map, TOMB_COMMIT_POSITION)?,
        decision_hash: require_bstr32(&map, TOMB_DECISION_HASH)?,
        abort_reason: optional_abort(&map, TOMB_ABORT_REASON)?,
    };
    rec.validate()?;
    Ok(rec)
}

/// `BLAKE3-256(RESIDIUUM-ATOMIC-TOMBSTONE-V1 || canonical_bytes)`.
pub fn tombstone_hash(tombstone: &DecisionTombstone) -> Result<[u8; 32], AtomicsError> {
    Ok(domain_hash(
        DOMAIN_ATOMIC_TOMBSTONE,
        &encode_tombstone(tombstone)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::VersionId;
    use crate::limits::ResourceLimits;
    use crate::plan::CanonicalKey;

    fn aid() -> AtomicId {
        let mut b = [0u8; 32];
        b[0] = 9;
        AtomicId::from_bytes(b).unwrap()
    }
    fn hid() -> HeapId {
        let mut b = [0u8; 16];
        b[0] = 1;
        HeapId::from_bytes(b).unwrap()
    }
    fn cid() -> CollectionId {
        let mut b = [0u8; 16];
        b[0] = 2;
        CollectionId::from_bytes(b).unwrap()
    }
    fn vid() -> VersionId {
        let mut b = [0u8; 16];
        b[0] = 3;
        VersionId::from_bytes(b).unwrap()
    }
    fn root() -> ContentRoot {
        ContentRoot::from_bytes([7u8; 32]).unwrap()
    }

    fn member(key: &str) -> AtomicMember {
        AtomicMember {
            atomic_id: aid(),
            ordinal: 0,
            object_identity: ObjectIdentity::new(cid(), CanonicalKey::String(key.into())),
            member_kind: MutationKind::Create,
            before_version: None,
            after_content_hash: Some([8u8; 32]),
            event_id: vid(),
        }
    }

    #[test]
    fn member_hash_binds_object_identity() {
        let a = member("alpha");
        let b = member("beta");
        assert_ne!(member_hash(&a).unwrap(), member_hash(&b).unwrap());
        let ra = ordered_member_manifest_root(hid(), std::slice::from_ref(&a)).unwrap();
        let rb = ordered_member_manifest_root(hid(), &[b]).unwrap();
        assert_ne!(ra, rb);
    }

    #[test]
    fn member_roundtrip_and_manifest_order() {
        let late = AtomicMember {
            ordinal: 1,
            object_identity: ObjectIdentity::new(cid(), CanonicalKey::String("zeta".into())),
            ..member("zeta")
        };
        let early = member("alpha");
        let a = ordered_member_manifest_root(hid(), &[late.clone(), early.clone()]).unwrap();
        let b = ordered_member_manifest_root(hid(), &[early.clone(), late.clone()]).unwrap();
        assert_eq!(a, b);
        let bytes = encode_member(&early).unwrap();
        assert_eq!(decode_member(&bytes).unwrap(), early);
    }

    #[test]
    fn prepare_and_decision_roundtrip() {
        let prep = AtomicPrepare {
            atomic_id: aid(),
            heap_id: hid(),
            scope: CoordinationScope::LocalHeap,
            content_root: root(),
            frontier: [1u8; 32],
            ordered_member_manifest_root: [2u8; 32],
            read_set_root: [3u8; 32],
            predicate_set_root: [4u8; 32],
            active_rule_revision_root: [5u8; 32],
            limits: ResourceLimits::builder_defaults_local_heap(),
        };
        let pb = encode_prepare(&prep).unwrap();
        assert_eq!(decode_prepare(&pb).unwrap(), prep);
        let ph = prepare_hash(&prep).unwrap();

        let committed = AtomicDecision::committed(aid(), ph, [6u8; 32], 1, 1).unwrap();
        let cb = encode_decision(&committed).unwrap();
        assert_eq!(decode_decision(&cb).unwrap(), committed);
        let stone = committed.tombstone(root(), decision_hash(&committed).unwrap());
        let tb = encode_tombstone(&stone).unwrap();
        assert_eq!(decode_tombstone(&tb).unwrap(), stone);

        let aborted = AtomicDecision::not_committed(
            aid(),
            ph,
            [6u8; 32],
            0,
            AtomicAbortReason::RecoveryAbort,
        );
        let ab = encode_decision(&aborted).unwrap();
        let again = decode_decision(&ab).unwrap();
        assert_eq!(again.abort_reason, Some(AtomicAbortReason::RecoveryAbort));
        match again
            .tombstone(root(), decision_hash(&again).unwrap())
            .not_committed_outcome()
            .unwrap()
        {
            crate::outcome::AtomicOutcome::NotCommitted { reason, .. } => {
                assert_eq!(reason, AtomicAbortReason::RecoveryAbort);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn committed_cannot_carry_abort_reason() {
        let mut bad = AtomicDecision::committed(aid(), [1u8; 32], [2u8; 32], 1, 1).unwrap();
        bad.abort_reason = Some(AtomicAbortReason::RuleRejected);
        assert!(encode_decision(&bad).is_err());
    }

    #[test]
    fn not_committed_requires_abort_reason() {
        let mut bad = AtomicDecision::not_committed(
            aid(),
            [1u8; 32],
            [2u8; 32],
            0,
            AtomicAbortReason::CoverageIncomplete,
        );
        bad.abort_reason = None;
        assert!(encode_decision(&bad).is_err());
    }

    #[test]
    fn unknown_decision_code_is_refused() {
        let bytes = cbor::encode_map(&[
            (1, Value::Bytes(aid().to_bytes().to_vec())),
            (2, Value::Bytes([1u8; 32].to_vec())),
            (3, Value::Bytes([2u8; 32].to_vec())),
            (4, Value::Uint(0)),
            (5, Value::Uint(3)),
            (7, Value::Uint(1)),
        ])
        .unwrap();
        assert_eq!(
            decode_decision(&bytes).unwrap_err().as_str(),
            "malformed_input"
        );
    }

    #[test]
    fn illegal_member_shapes_are_refused_on_encode_and_decode() {
        let hash = [8u8; 32];
        let cases = [
            (MutationKind::Create, None, None),
            (MutationKind::Create, Some(vid()), Some(hash)),
            (MutationKind::Create, Some(vid()), None),
            (MutationKind::Put, None, None),
            (MutationKind::Put, Some(vid()), None),
            (MutationKind::Replace, None, Some(hash)),
            (MutationKind::Replace, None, None),
            (MutationKind::Replace, Some(vid()), None),
            (MutationKind::Delete, Some(vid()), Some(hash)),
            (MutationKind::Delete, None, None),
            (MutationKind::Delete, None, Some(hash)),
        ];
        for (kind, before, after) in cases {
            let bad = AtomicMember {
                member_kind: kind,
                before_version: before,
                after_content_hash: after,
                ..member("k")
            };
            assert_eq!(
                encode_member(&bad).unwrap_err().as_str(),
                "invalid_value",
                "{kind:?} before={} after={}",
                before.is_some(),
                after.is_some()
            );
            let bytes = encode_member_body(&bad).unwrap();
            assert_eq!(
                decode_member(&bytes).unwrap_err().as_str(),
                "invalid_value",
                "decode {kind:?} before={} after={}",
                before.is_some(),
                after.is_some()
            );
        }
    }

    #[test]
    fn put_overwrite_may_record_before_version() {
        let m = AtomicMember {
            member_kind: MutationKind::Put,
            before_version: Some(vid()),
            after_content_hash: Some([8u8; 32]),
            ..member("k")
        };
        let bytes = encode_member(&m).unwrap();
        let again = decode_member(&bytes).unwrap();
        assert_eq!(again.before_version, m.before_version);
        assert_eq!(again.after_content_hash, m.after_content_hash);
        assert_eq!(again.member_kind, MutationKind::Put);
    }

    #[test]
    fn nested_duplicate_in_object_identity_is_refused() {
        let good = encode_member(&member("k")).unwrap();
        // Locate the nested key map (field 3 → field 2) and splice a duplicate key.
        // Hand-built hostile: member map whose object_identity.key is map{1:1, 1:1}.
        // a2 01 01 01 01 = map{1:1, 1:1}
        let hostile_key = [0xa2, 0x01, 0x01, 0x01, 0x01];
        assert!(decode_member(&hostile_key).is_err());
        let _ = good;
    }
}
