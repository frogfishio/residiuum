//! Domain separators and canonical-order vocabulary (`ATOMICS_SPEC` §§5–6, 9).
//!
//! ATM-0.2: deterministic CBOR encode/decode, content-root hash, canonical
//! target ordering, and duplicate-target refusal.

use crate::cbor::{self, Value};
use crate::error::{AtomicsError, CborError};
use crate::id::{AtomicId, CollectionId, ContentRoot, HeapId, VersionId};
use crate::limits::ResourceLimits;
use crate::outcome::AtomicRefuseReason;
use crate::plan::{
    AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, CoordinationScope, MutationKind,
    PlanMutation, PlanPredicate, PredicateKind, ReadWitness,
};
use std::time::Duration;

/// Engine `AtomicId` derivation (`ATOMICS_SPEC` §5).
pub const DOMAIN_ATOMIC_ID: &[u8] = b"RESIDIUUM-ATOMIC-ID-V1";

/// Plan content root (`ATOMICS_SPEC` §5).
pub const DOMAIN_ATOMIC_CONTENT: &[u8] = b"RESIDIUUM-ATOMIC-CONTENT-V1";

/// Prepare evidence (`ATOMICS_SPEC` §9).
pub const DOMAIN_ATOMIC_PREPARE: &[u8] = b"RESIDIUUM-ATOMIC-PREPARE-V1";

/// Member evidence (`ATOMICS_SPEC` §9).
pub const DOMAIN_ATOMIC_MEMBER: &[u8] = b"RESIDIUUM-ATOMIC-MEMBER-V1";

/// Decision evidence (`ATOMICS_SPEC` §9).
pub const DOMAIN_ATOMIC_DECISION: &[u8] = b"RESIDIUUM-ATOMIC-DECISION-V1";

/// Lifetime decision tombstone (`ATOMICS_SPEC` §12).
pub const DOMAIN_ATOMIC_TOMBSTONE: &[u8] = b"RESIDIUUM-ATOMIC-TOMBSTONE-V1";

/// Ordered member manifest root (`ATOMICS_SPEC` §9).
pub const DOMAIN_ATOMIC_MANIFEST: &[u8] = b"RESIDIUUM-ATOMIC-MANIFEST-V1";

/// Read-set root (`ATOMICS_SPEC` §9).
pub const DOMAIN_ATOMIC_READSET: &[u8] = b"RESIDIUUM-ATOMIC-READSET-V1";

/// Predicate-set root (`ATOMICS_SPEC` §9).
pub const DOMAIN_ATOMIC_PREDICATES: &[u8] = b"RESIDIUUM-ATOMIC-PREDICATES-V1";

/// Active rule-revision root (`ATOMICS_SPEC` §9).
pub const DOMAIN_ATOMIC_RULES: &[u8] = b"RESIDIUUM-ATOMIC-RULES-V1";

/// Canonical member order components. Encoding of these bytes is ATM-0.2.
///
/// Order is `(heap_id, collection_id, canonical_key_bytes, member_kind, ordinal)`.
pub const CANONICAL_TARGET_ORDER: &str =
    "heap_id, collection_id, canonical_key_bytes, member_kind, ordinal";

/// Relative path of the ATM-0.2 field-number freeze.
pub const CBOR_V1_REL: &str = "spec/atomics/cbor-v1.json";

const PLAN_PROFILE: u64 = 1;
const PLAN_ATOMIC_ID: u64 = 2;
const PLAN_HEAP_ID: u64 = 3;
const PLAN_SCOPE: u64 = 4;
const PLAN_READ_FRONTIER: u64 = 5;
const PLAN_READS: u64 = 6;
const PLAN_PREDICATES: u64 = 7;
const PLAN_MUTATIONS: u64 = 8;
const PLAN_RULE_REVISIONS: u64 = 9;
const PLAN_LIMITS: u64 = 10;

/// Total order key for a read witness (heap, collection, key, version, projection).
type ReadOrderKey = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, [u8; 32]);

/// Total order key for a predicate (heap, collection, key, kind, version, payload).
type PredicateOrderKey = (Vec<u8>, Vec<u8>, Vec<u8>, u8, Vec<u8>, Vec<u8>);

/// Close unordered parts: refuse duplicate identities, require a read frontier
/// when witnesses are present, then sort with a total order.
pub(crate) fn close_plan(mut parts: AtomicPlanParts) -> Result<AtomicPlan, AtomicsError> {
    refuse_duplicate_targets(&parts.mutations)?;
    refuse_duplicate_reads(&parts.reads)?;
    refuse_duplicate_predicates(&parts.predicates)?;
    require_frontier_with_reads(&parts)?;
    let heap_id = parts.heap_id;
    parts.mutations.sort_by_key(|a| target_order(&heap_id, a));
    parts.reads.sort_by_key(|a| read_order(&heap_id, a));
    parts.predicates.sort_by_key(|a| pred_order(&heap_id, a));
    parts.active_rule_revisions.sort_unstable();
    validate_mutation_shapes(&parts.mutations)?;
    validate_predicate_shapes(&parts.predicates)?;
    validate_plan_keys(&parts)?;
    Ok(AtomicPlan::from_closed(parts))
}

/// Encode a closed plan to deterministic CBOR.
pub fn encode_canonical_plan(plan: &AtomicPlan) -> Result<Vec<u8>, AtomicsError> {
    cbor::encode_map(&plan_entries(plan)?)
}

/// Decode a closed plan. Unknown map keys are refused.
pub fn decode_canonical_plan(bytes: &[u8]) -> Result<AtomicPlan, AtomicsError> {
    let map = cbor::decode_map(bytes)?;
    let profile = AtomicProfile::from_wire_code(require_u16(&map, PLAN_PROFILE)?);
    let atomic_id = AtomicId::from_bytes(require_bstr32(&map, PLAN_ATOMIC_ID)?)?;
    let heap_id = HeapId::from_bytes(require_bstr16(&map, PLAN_HEAP_ID)?)?;
    let scope = CoordinationScope::from_wire_code(require_u8(&map, PLAN_SCOPE)?)
        .ok_or(AtomicsError::Refused(AtomicRefuseReason::MalformedInput))?;
    let read_frontier = optional_bstr32(&map, PLAN_READ_FRONTIER)?;
    let reads = require_array(&map, PLAN_READS)?
        .iter()
        .map(decode_read)
        .collect::<Result<Vec<_>, _>>()?;
    let predicates = require_array(&map, PLAN_PREDICATES)?
        .iter()
        .map(decode_predicate)
        .collect::<Result<Vec<_>, _>>()?;
    let mutations = require_array(&map, PLAN_MUTATIONS)?
        .iter()
        .map(decode_mutation)
        .collect::<Result<Vec<_>, _>>()?;
    let revisions = require_array(&map, PLAN_RULE_REVISIONS)?
        .iter()
        .map(decode_rev)
        .collect::<Result<Vec<_>, _>>()?;
    let limits = decode_limits(require_map(&map, PLAN_LIMITS)?)?;
    refuse_unknown_keys(&map, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])?;
    AtomicPlan::close(AtomicPlanParts {
        profile,
        atomic_id,
        heap_id,
        scope,
        read_frontier,
        reads,
        predicates,
        mutations,
        active_rule_revisions: revisions,
        limits,
    })
}

/// Content root of a closed plan (`ATOMICS_SPEC` §5).
pub fn plan_content_root(plan: &AtomicPlan) -> Result<ContentRoot, AtomicsError> {
    Ok(ContentRoot::from_canonical_plan_bytes(
        &encode_canonical_plan(plan)?,
    ))
}

fn refuse_duplicate_targets(mutations: &[PlanMutation]) -> Result<(), AtomicsError> {
    let mut seen: Vec<(CollectionId, Vec<u8>)> = Vec::new();
    for m in mutations {
        let key = key_order_bytes(&m.key);
        if seen.iter().any(|(c, k)| *c == m.collection_id && *k == key) {
            return Err(AtomicsError::Refused(AtomicRefuseReason::DuplicateTarget));
        }
        seen.push((m.collection_id, key));
    }
    Ok(())
}

fn refuse_duplicate_reads(reads: &[ReadWitness]) -> Result<(), AtomicsError> {
    let mut seen: Vec<(CollectionId, Vec<u8>)> = Vec::new();
    for r in reads {
        let key = key_order_bytes(&r.key);
        if seen.iter().any(|(c, k)| *c == r.collection_id && *k == key) {
            return Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput));
        }
        seen.push((r.collection_id, key));
    }
    Ok(())
}

fn refuse_duplicate_predicates(predicates: &[PlanPredicate]) -> Result<(), AtomicsError> {
    let mut seen: Vec<(u8, Option<CollectionId>, Vec<u8>)> = Vec::new();
    for p in predicates {
        let key = p.key.as_ref().map(key_order_bytes).unwrap_or_default();
        let ident = (p.kind.wire_code(), p.collection_id, key);
        if seen.contains(&ident) {
            return Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput));
        }
        seen.push(ident);
    }
    Ok(())
}

fn require_frontier_with_reads(parts: &AtomicPlanParts) -> Result<(), AtomicsError> {
    if !parts.reads.is_empty() && parts.read_frontier.is_none() {
        return Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput));
    }
    Ok(())
}

fn validate_mutation_shapes(mutations: &[PlanMutation]) -> Result<(), AtomicsError> {
    for m in mutations {
        match m.kind {
            MutationKind::Create | MutationKind::Put => {
                if m.encoded_value.is_none() || m.if_version.is_some() {
                    return Err(AtomicsError::Refused(AtomicRefuseReason::InvalidValue));
                }
            }
            MutationKind::Replace => {
                if m.encoded_value.is_none() || m.if_version.is_none() {
                    return Err(AtomicsError::Refused(AtomicRefuseReason::InvalidValue));
                }
            }
            MutationKind::Delete => {
                if m.encoded_value.is_some() || m.if_version.is_none() {
                    return Err(AtomicsError::Refused(AtomicRefuseReason::InvalidValue));
                }
            }
        }
    }
    Ok(())
}

fn validate_predicate_shapes(predicates: &[PlanPredicate]) -> Result<(), AtomicsError> {
    for p in predicates {
        let ok = match p.kind {
            PredicateKind::AssertAbsent | PredicateKind::AssertPresent => {
                p.collection_id.is_some()
                    && p.key.is_some()
                    && p.version.is_none()
                    && p.encoded.is_none()
            }
            PredicateKind::AssertVersion => {
                p.collection_id.is_some()
                    && p.key.is_some()
                    && p.version.is_some()
                    && p.encoded.is_none()
            }
            PredicateKind::HeapAuthorityRevision => {
                p.collection_id.is_none()
                    && p.key.is_none()
                    && p.version.is_none()
                    && p.encoded.as_ref().is_some_and(|e| e.len() == 32)
            }
            PredicateKind::ExactScalarEquality => {
                p.collection_id.is_some()
                    && p.key.is_some()
                    && p.version.is_none()
                    && p.encoded.as_deref().is_some_and(|bytes| {
                        crate::predicate::decode_exact_scalar_payload(bytes).is_ok()
                    })
            }
            _ => true,
        };
        if !ok {
            // Public asserts must be well-formed before prepare; a missing
            // version is not a runtime precondition conflict.
            return Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput));
        }
    }
    Ok(())
}

fn validate_plan_keys(parts: &AtomicPlanParts) -> Result<(), AtomicsError> {
    for m in &parts.mutations {
        crate::encoding::validate_canonical_key(&m.key)?;
    }
    for r in &parts.reads {
        crate::encoding::validate_canonical_key(&r.key)?;
    }
    for p in &parts.predicates {
        if let Some(key) = &p.key {
            crate::encoding::validate_canonical_key(key)?;
        }
    }
    Ok(())
}

fn target_order(heap: &HeapId, m: &PlanMutation) -> (Vec<u8>, Vec<u8>, Vec<u8>, u8) {
    let key = key_order_bytes(&m.key);
    (
        heap.as_bytes().to_vec(),
        m.collection_id.as_bytes().to_vec(),
        key,
        m.kind.wire_code(),
    )
}

fn optional_version_order(v: Option<VersionId>) -> Vec<u8> {
    match v {
        None => vec![0],
        Some(id) => {
            let mut out = vec![1];
            out.extend_from_slice(id.as_bytes());
            out
        }
    }
}

fn optional_bytes_order(bytes: Option<&[u8]>) -> Vec<u8> {
    match bytes {
        None => vec![0],
        Some(b) => {
            let mut out = vec![1];
            out.extend_from_slice(b);
            out
        }
    }
}

fn read_order(heap: &HeapId, r: &ReadWitness) -> ReadOrderKey {
    (
        heap.as_bytes().to_vec(),
        r.collection_id.as_bytes().to_vec(),
        key_order_bytes(&r.key),
        optional_version_order(r.observed_version),
        r.projection_hash,
    )
}

fn pred_order(heap: &HeapId, p: &PlanPredicate) -> PredicateOrderKey {
    let coll = p
        .collection_id
        .map(|c| c.as_bytes().to_vec())
        .unwrap_or_default();
    let key = p.key.as_ref().map(key_order_bytes).unwrap_or_default();
    (
        heap.as_bytes().to_vec(),
        coll,
        key,
        p.kind.wire_code(),
        optional_version_order(p.version),
        optional_bytes_order(p.encoded.as_deref()),
    )
}

/// Order/identity bytes for a key: kind tag plus profile payload, not CBOR length prefixes.
pub(crate) fn key_order_bytes(key: &CanonicalKey) -> Vec<u8> {
    let mut out = vec![key.kind().wire_code()];
    match key {
        CanonicalKey::String(s) => out.extend_from_slice(s.as_bytes()),
        CanonicalKey::Opaque(b) | CanonicalKey::Integer(b) => out.extend_from_slice(b),
        CanonicalKey::Decimal { coefficient, scale } => {
            out.extend_from_slice(&scale.to_be_bytes());
            out.extend_from_slice(coefficient);
        }
    }
    out
}

fn plan_entries(plan: &AtomicPlan) -> Result<Vec<(u64, Value)>, AtomicsError> {
    let mut entries = vec![
        (
            PLAN_PROFILE,
            Value::Uint(u64::from(plan.profile().wire_code())),
        ),
        (
            PLAN_ATOMIC_ID,
            Value::Bytes(plan.atomic_id().to_bytes().to_vec()),
        ),
        (
            PLAN_HEAP_ID,
            Value::Bytes(plan.heap_id().to_bytes().to_vec()),
        ),
        (PLAN_SCOPE, Value::Uint(u64::from(plan.scope().wire_code()))),
    ];
    if let Some(front) = plan.read_frontier() {
        entries.push((PLAN_READ_FRONTIER, Value::Bytes(front.to_vec())));
    }
    entries.push((
        PLAN_READS,
        Value::Array(
            plan.reads()
                .iter()
                .map(encode_read)
                .collect::<Result<Vec<_>, _>>()?,
        ),
    ));
    entries.push((
        PLAN_PREDICATES,
        Value::Array(
            plan.predicates()
                .iter()
                .map(encode_predicate)
                .collect::<Result<Vec<_>, _>>()?,
        ),
    ));
    entries.push((
        PLAN_MUTATIONS,
        Value::Array(
            plan.mutations()
                .iter()
                .map(encode_mutation)
                .collect::<Result<Vec<_>, _>>()?,
        ),
    ));
    entries.push((
        PLAN_RULE_REVISIONS,
        Value::Array(
            plan.active_rule_revisions()
                .iter()
                .map(|r| Value::Bytes(r.to_vec()))
                .collect(),
        ),
    ));
    entries.push((PLAN_LIMITS, Value::Map(encode_limits(plan.limits()))));
    Ok(entries)
}

pub(crate) fn encode_key_map(key: &CanonicalKey) -> Result<Vec<(u64, Value)>, AtomicsError> {
    crate::encoding::validate_canonical_key(key)?;
    let mut entries = vec![
        (1, Value::Uint(u64::from(key.kind().wire_code()))),
        (
            2,
            Value::Bytes(match key {
                CanonicalKey::String(s) => s.as_bytes().to_vec(),
                CanonicalKey::Opaque(b) | CanonicalKey::Integer(b) => b.clone(),
                CanonicalKey::Decimal { coefficient, .. } => coefficient.clone(),
            }),
        ),
    ];
    if let CanonicalKey::Decimal { scale, .. } = key {
        entries.push((3, encode_int(*scale)));
    }
    Ok(entries)
}

fn encode_int(n: i64) -> Value {
    if n >= 0 {
        Value::Uint(n as u64)
    } else {
        Value::Nint((!n) as u64)
    }
}

pub(crate) fn encode_read(r: &ReadWitness) -> Result<Value, AtomicsError> {
    let mut e = vec![
        (1, Value::Bytes(r.collection_id.to_bytes().to_vec())),
        (2, Value::Map(encode_key_map(&r.key)?)),
        (4, Value::Bytes(r.projection_hash.to_vec())),
    ];
    if let Some(v) = r.observed_version {
        e.push((3, Value::Bytes(v.to_bytes().to_vec())));
    }
    Ok(Value::Map(e))
}

fn encode_mutation(m: &PlanMutation) -> Result<Value, AtomicsError> {
    let mut e = vec![
        (1, Value::Uint(u64::from(m.kind.wire_code()))),
        (2, Value::Bytes(m.collection_id.to_bytes().to_vec())),
        (3, Value::Map(encode_key_map(&m.key)?)),
    ];
    if let Some(val) = &m.encoded_value {
        e.push((4, Value::Bytes(val.clone())));
    }
    if let Some(v) = m.if_version {
        e.push((5, Value::Bytes(v.to_bytes().to_vec())));
    }
    Ok(Value::Map(e))
}

pub(crate) fn encode_predicate(p: &PlanPredicate) -> Result<Value, AtomicsError> {
    let mut e = vec![(1, Value::Uint(u64::from(p.kind.wire_code())))];
    if let Some(c) = p.collection_id {
        e.push((2, Value::Bytes(c.to_bytes().to_vec())));
    }
    if let Some(k) = &p.key {
        e.push((3, Value::Map(encode_key_map(k)?)));
    }
    if let Some(v) = p.version {
        e.push((4, Value::Bytes(v.to_bytes().to_vec())));
    }
    if let Some(enc) = &p.encoded {
        e.push((5, Value::Bytes(enc.clone())));
    }
    Ok(Value::Map(e))
}

pub(crate) fn encode_limits(l: ResourceLimits) -> Vec<(u64, Value)> {
    vec![
        (1, Value::Uint(u64::from(l.caller_mutations))),
        (2, Value::Uint(u64::from(l.total_generated_members))),
        (3, Value::Uint(u64::from(l.canonical_plan_bytes))),
        (4, Value::Uint(u64::from(l.total_proposed_value_bytes))),
        (5, Value::Uint(u64::from(l.read_witnesses))),
        (6, Value::Uint(u64::from(l.predicates))),
        (7, Value::Uint(u64::from(l.affected_collections))),
        (8, Value::Uint(u64::from(l.active_rule_revisions))),
        (
            9,
            Value::Uint(u64::try_from(l.construction_deadline.as_millis()).unwrap_or(u64::MAX)),
        ),
        (10, Value::Uint(u64::from(l.emitted_violations))),
    ]
}

pub(crate) fn decode_key(v: &Value) -> Result<CanonicalKey, AtomicsError> {
    let map = as_map(v)?;
    refuse_unknown_keys(map, &[1, 2, 3])?;
    let kind = require_u8(map, 1)?;
    let payload = require_bytes(map, 2)?;
    let has_scale = map.iter().any(|(k, _)| *k == 3);
    match kind {
        1 => {
            refuse_scale_on_non_decimal(has_scale)?;
            let s = String::from_utf8(payload)
                .map_err(|_| AtomicsError::Refused(AtomicRefuseReason::InvalidValue))?;
            Ok(CanonicalKey::String(s))
        }
        2 => {
            refuse_scale_on_non_decimal(has_scale)?;
            Ok(CanonicalKey::Opaque(payload))
        }
        3 => {
            refuse_scale_on_non_decimal(has_scale)?;
            CanonicalKey::integer_bytes(&payload)
        }
        4 => {
            let key = CanonicalKey::decimal_bytes(&payload, decode_int(require_value(map, 3)?)?)?;
            Ok(key)
        }
        _ => Err(AtomicsError::Refused(AtomicRefuseReason::InvalidValue)),
    }
}

fn refuse_scale_on_non_decimal(has_scale: bool) -> Result<(), AtomicsError> {
    if has_scale {
        // Field 3 is decimal-only. Ignoring it would accept two byte encodings
        // for one logical string/opaque/integer key.
        Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput))
    } else {
        Ok(())
    }
}

fn decode_read(v: &Value) -> Result<ReadWitness, AtomicsError> {
    let map = as_map(v)?;
    refuse_unknown_keys(map, &[1, 2, 3, 4])?;
    Ok(ReadWitness {
        collection_id: CollectionId::from_bytes(require_bstr16(map, 1)?)?,
        key: decode_key(require_value(map, 2)?)?,
        observed_version: optional_version(map, 3)?,
        projection_hash: require_bstr32(map, 4)?,
    })
}

fn decode_mutation(v: &Value) -> Result<PlanMutation, AtomicsError> {
    let map = as_map(v)?;
    refuse_unknown_keys(map, &[1, 2, 3, 4, 5])?;
    let kind = MutationKind::from_wire_code(require_u8(map, 1)?).ok_or(AtomicsError::Refused(
        AtomicRefuseReason::UnknownMutationKind,
    ))?;
    Ok(PlanMutation {
        kind,
        collection_id: CollectionId::from_bytes(require_bstr16(map, 2)?)?,
        key: decode_key(require_value(map, 3)?)?,
        encoded_value: optional_bytes(map, 4)?,
        if_version: optional_version(map, 5)?,
    })
}

fn decode_predicate(v: &Value) -> Result<PlanPredicate, AtomicsError> {
    let map = as_map(v)?;
    refuse_unknown_keys(map, &[1, 2, 3, 4, 5])?;
    let kind = PredicateKind::from_wire_code(require_u8(map, 1)?).ok_or(AtomicsError::Refused(
        AtomicRefuseReason::UnsupportedPredicate,
    ))?;
    let key = match map.iter().find(|(k, _)| *k == 3) {
        Some((_, v)) => Some(decode_key(v)?),
        None => None,
    };
    Ok(PlanPredicate {
        kind,
        collection_id: optional_collection(map, 2)?,
        key,
        version: optional_version(map, 4)?,
        encoded: optional_bytes(map, 5)?,
    })
}

pub(crate) fn decode_limits(map: &[(u64, Value)]) -> Result<ResourceLimits, AtomicsError> {
    refuse_unknown_keys(map, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])?;
    Ok(ResourceLimits {
        caller_mutations: require_u32(map, 1)?,
        total_generated_members: require_u32(map, 2)?,
        canonical_plan_bytes: require_u32(map, 3)?,
        total_proposed_value_bytes: require_u32(map, 4)?,
        read_witnesses: require_u32(map, 5)?,
        predicates: require_u32(map, 6)?,
        affected_collections: require_u32(map, 7)?,
        active_rule_revisions: require_u32(map, 8)?,
        construction_deadline: Duration::from_millis(require_u64(map, 9)?),
        emitted_violations: require_u32(map, 10)?,
    })
}

fn decode_rev(v: &Value) -> Result<[u8; 32], AtomicsError> {
    match v {
        Value::Bytes(b) if b.len() == 32 => {
            let mut out = [0u8; 32];
            out.copy_from_slice(b);
            Ok(out)
        }
        _ => Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput)),
    }
}

fn decode_int(v: &Value) -> Result<i64, AtomicsError> {
    match v {
        Value::Uint(n) => i64::try_from(*n).map_err(|_| CborError::Unsupported.into()),
        Value::Nint(n) => {
            let mag = i64::try_from(*n).map_err(|_| CborError::Unsupported)?;
            mag.checked_neg()
                .and_then(|v| v.checked_sub(1))
                .ok_or(CborError::Unsupported.into())
        }
        _ => Err(CborError::Unsupported.into()),
    }
}

pub(crate) fn as_map(v: &Value) -> Result<&[(u64, Value)], AtomicsError> {
    match v {
        Value::Map(m) => Ok(m),
        _ => Err(CborError::NotMap.into()),
    }
}

pub(crate) fn require_value(map: &[(u64, Value)], key: u64) -> Result<&Value, AtomicsError> {
    map.iter()
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v)
        .ok_or(AtomicsError::Refused(AtomicRefuseReason::MalformedInput))
}

pub(crate) fn require_u64(map: &[(u64, Value)], key: u64) -> Result<u64, AtomicsError> {
    match require_value(map, key)? {
        Value::Uint(n) => Ok(*n),
        _ => Err(CborError::Unsupported.into()),
    }
}

pub(crate) fn require_u32(map: &[(u64, Value)], key: u64) -> Result<u32, AtomicsError> {
    u32::try_from(require_u64(map, key)?).map_err(|_| CborError::Unsupported.into())
}

fn require_u16(map: &[(u64, Value)], key: u64) -> Result<u16, AtomicsError> {
    u16::try_from(require_u64(map, key)?).map_err(|_| CborError::Unsupported.into())
}

pub(crate) fn require_u8(map: &[(u64, Value)], key: u64) -> Result<u8, AtomicsError> {
    u8::try_from(require_u64(map, key)?).map_err(|_| CborError::Unsupported.into())
}

pub(crate) fn require_bytes(map: &[(u64, Value)], key: u64) -> Result<Vec<u8>, AtomicsError> {
    match require_value(map, key)? {
        Value::Bytes(b) => Ok(b.clone()),
        _ => Err(CborError::Unsupported.into()),
    }
}

fn require_array(map: &[(u64, Value)], key: u64) -> Result<&[Value], AtomicsError> {
    match require_value(map, key)? {
        Value::Array(a) => Ok(a),
        _ => Err(CborError::Unsupported.into()),
    }
}

fn require_map(map: &[(u64, Value)], key: u64) -> Result<&[(u64, Value)], AtomicsError> {
    as_map(require_value(map, key)?)
}

pub(crate) fn require_bstr16(map: &[(u64, Value)], key: u64) -> Result<[u8; 16], AtomicsError> {
    let b = require_bytes(map, key)?;
    <[u8; 16]>::try_from(b).map_err(|_| AtomicsError::Refused(AtomicRefuseReason::MalformedInput))
}

pub(crate) fn require_bstr32(map: &[(u64, Value)], key: u64) -> Result<[u8; 32], AtomicsError> {
    let b = require_bytes(map, key)?;
    <[u8; 32]>::try_from(b).map_err(|_| AtomicsError::Refused(AtomicRefuseReason::MalformedInput))
}

pub(crate) fn optional_bytes(
    map: &[(u64, Value)],
    key: u64,
) -> Result<Option<Vec<u8>>, AtomicsError> {
    match map.iter().find(|(k, _)| *k == key) {
        None => Ok(None),
        Some((_, Value::Bytes(b))) => Ok(Some(b.clone())),
        Some(_) => Err(CborError::Unsupported.into()),
    }
}

pub(crate) fn optional_bstr32(
    map: &[(u64, Value)],
    key: u64,
) -> Result<Option<[u8; 32]>, AtomicsError> {
    match optional_bytes(map, key)? {
        None => Ok(None),
        Some(b) => Ok(Some(<[u8; 32]>::try_from(b).map_err(|_| {
            AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
        })?)),
    }
}

pub(crate) fn optional_version(
    map: &[(u64, Value)],
    key: u64,
) -> Result<Option<VersionId>, AtomicsError> {
    match optional_bytes(map, key)? {
        None => Ok(None),
        Some(b) => {
            let arr = <[u8; 16]>::try_from(b)
                .map_err(|_| AtomicsError::Refused(AtomicRefuseReason::MalformedInput))?;
            Ok(Some(VersionId::from_bytes(arr)?))
        }
    }
}

fn optional_collection(
    map: &[(u64, Value)],
    key: u64,
) -> Result<Option<CollectionId>, AtomicsError> {
    match optional_bytes(map, key)? {
        None => Ok(None),
        Some(b) => {
            let arr = <[u8; 16]>::try_from(b)
                .map_err(|_| AtomicsError::Refused(AtomicRefuseReason::MalformedInput))?;
            Ok(Some(CollectionId::from_bytes(arr)?))
        }
    }
}

pub(crate) fn refuse_unknown_keys(
    map: &[(u64, Value)],
    allowed: &[u64],
) -> Result<(), AtomicsError> {
    for (k, _) in map {
        if !allowed.contains(k) {
            return Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_separators_are_v1_and_distinct() {
        let all = [
            DOMAIN_ATOMIC_ID,
            DOMAIN_ATOMIC_CONTENT,
            DOMAIN_ATOMIC_PREPARE,
            DOMAIN_ATOMIC_MEMBER,
            DOMAIN_ATOMIC_DECISION,
            DOMAIN_ATOMIC_TOMBSTONE,
            DOMAIN_ATOMIC_MANIFEST,
            DOMAIN_ATOMIC_READSET,
            DOMAIN_ATOMIC_PREDICATES,
            DOMAIN_ATOMIC_RULES,
        ];
        for (i, a) in all.iter().enumerate() {
            assert!(a.starts_with(b"RESIDIUUM-ATOMIC-"));
            assert!(a.ends_with(b"-V1"));
            for (j, b) in all.iter().enumerate() {
                if i != j {
                    assert_ne!(a, b);
                }
            }
        }
    }

    fn key_map(kind: u64, payload: &[u8], scale: Option<i64>) -> Value {
        let mut e = vec![(1, Value::Uint(kind)), (2, Value::Bytes(payload.to_vec()))];
        if let Some(s) = scale {
            e.push((3, encode_int(s)));
        }
        Value::Map(e)
    }

    #[test]
    fn scale_is_decimal_only() {
        assert_eq!(
            decode_key(&key_map(1, b"k", Some(0))).unwrap_err(),
            AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
        );
        assert_eq!(
            decode_key(&key_map(2, b"k", Some(0))).unwrap_err(),
            AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
        );
        assert_eq!(
            decode_key(&key_map(3, b"k", Some(0))).unwrap_err(),
            AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
        );
        assert_eq!(
            decode_key(&key_map(4, b"\x01", None)).unwrap_err(),
            AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
        );
        assert_eq!(
            decode_key(&key_map(4, b"\x01", Some(2))).unwrap(),
            CanonicalKey::Decimal {
                coefficient: vec![1],
                scale: 2,
            }
        );
        assert_eq!(
            decode_key(&key_map(1, b"k", None)).unwrap(),
            CanonicalKey::String("k".into())
        );
    }
}
