//! ATM-1.1: typed plan encodings and byte accounting (`ATOMICS_SPEC` §§6, 13).
//!
//! Values are serialized before close. The closed-plan validator and oracle
//! differential are ATM-1.2. The SDK-internal builder is ATM-1.3.

use crate::canonical::encode_canonical_plan;
use crate::id::{CollectionId, VersionId};
use crate::outcome::AtomicRefuseReason;
use crate::plan::{
    AtomicPlan, CanonicalKey, MutationKind, PlanMutation, PlanPredicate, PredicateKind,
};
use crate::AtomicsError;

/// Collection-encoded value frozen before plan admission.
///
/// Construction is fallible under a [`ValueEncoding`]. A closed plan cannot
/// hold a host object.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CanonicalValue {
    bytes: Vec<u8>,
}

impl CanonicalValue {
    /// Verify already-encoded payload bytes under `encoding`.
    pub fn serialize(
        encoding: crate::encoding::ValueEncoding,
        payload: &[u8],
    ) -> Result<Self, AtomicsError> {
        crate::encoding::EncodingProfile::new(crate::plan::CanonicalKeyKind::String, encoding)
            .admit_value_bytes(payload)?;
        Ok(Self {
            bytes: payload.to_vec(),
        })
    }

    /// Opaque bytes. Valid only under [`crate::encoding::ValueEncoding::Bytes`].
    pub fn from_bytes(payload: &[u8]) -> Self {
        Self {
            bytes: payload.to_vec(),
        }
    }

    /// Canonical signed integer value.
    pub fn from_integer(n: i128) -> Self {
        Self {
            bytes: crate::encoding::encode_signed_integer(n),
        }
    }

    /// Canonical exact-decimal value (coefficient + scale).
    pub fn from_decimal(coefficient: i128, scale: i64) -> Result<Self, AtomicsError> {
        Ok(Self {
            bytes: crate::encoding::encode_decimal_value(coefficient, scale)?,
        })
    }

    /// Admitted bytes that will become `encoded_value`.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Admitted length in bytes.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// True when the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// Verify a collection-encoded payload before admission.
pub fn serialize_canonical_value(
    encoding: crate::encoding::ValueEncoding,
    payload: &[u8],
) -> Result<CanonicalValue, AtomicsError> {
    CanonicalValue::serialize(encoding, payload)
}

/// Encode create-if-absent (`ATOMICS_SPEC` §6).
pub fn encode_create(
    collection_id: CollectionId,
    key: CanonicalKey,
    value: CanonicalValue,
) -> PlanMutation {
    PlanMutation {
        kind: MutationKind::Create,
        collection_id,
        key,
        encoded_value: Some(value.bytes),
        if_version: None,
    }
}

/// Encode explicit blind upsert (`put_unconditional`).
pub fn encode_put(
    collection_id: CollectionId,
    key: CanonicalKey,
    value: CanonicalValue,
) -> PlanMutation {
    PlanMutation {
        kind: MutationKind::Put,
        collection_id,
        key,
        encoded_value: Some(value.bytes),
        if_version: None,
    }
}

/// Encode version-replace.
pub fn encode_replace(
    collection_id: CollectionId,
    key: CanonicalKey,
    if_version: VersionId,
    value: CanonicalValue,
) -> PlanMutation {
    PlanMutation {
        kind: MutationKind::Replace,
        collection_id,
        key,
        encoded_value: Some(value.bytes),
        if_version: Some(if_version),
    }
}

/// Encode version-delete.
pub fn encode_delete(
    collection_id: CollectionId,
    key: CanonicalKey,
    if_version: VersionId,
) -> PlanMutation {
    PlanMutation {
        kind: MutationKind::Delete,
        collection_id,
        key,
        encoded_value: None,
        if_version: Some(if_version),
    }
}

/// Encode `assert_absent`.
pub fn encode_assert_absent(collection_id: CollectionId, key: CanonicalKey) -> PlanPredicate {
    PlanPredicate {
        kind: PredicateKind::AssertAbsent,
        collection_id: Some(collection_id),
        key: Some(key),
        version: None,
        encoded: None,
    }
}

/// Encode `assert_present`.
pub fn encode_assert_present(collection_id: CollectionId, key: CanonicalKey) -> PlanPredicate {
    PlanPredicate {
        kind: PredicateKind::AssertPresent,
        collection_id: Some(collection_id),
        key: Some(key),
        version: None,
        encoded: None,
    }
}

/// Encode `assert_version`.
pub fn encode_assert_version(
    collection_id: CollectionId,
    key: CanonicalKey,
    version: VersionId,
) -> PlanPredicate {
    PlanPredicate {
        kind: PredicateKind::AssertVersion,
        collection_id: Some(collection_id),
        key: Some(key),
        version: Some(version),
        encoded: None,
    }
}

/// Encode heap authority/security revision (`ATOMICS_SPEC` §7).
///
/// This is not an active RRE rule revision. The payload is the 32-byte
/// authority revision hash.
pub fn encode_heap_authority_revision(revision: [u8; 32]) -> PlanPredicate {
    PlanPredicate {
        kind: PredicateKind::HeapAuthorityRevision,
        collection_id: None,
        key: None,
        version: None,
        encoded: Some(revision.to_vec()),
    }
}

/// Requested vs worst-case generated-member accounting for a closed plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlanAccounting {
    /// Caller mutation count. Assertions that share a target fold into that
    /// member and do not add to this count.
    pub requested_members: u32,
    /// Sum of admitted `encoded_value` lengths.
    pub requested_value_bytes: u32,
    /// Canonical closed-plan CBOR length.
    pub requested_plan_bytes: u32,
    /// Worst-case generated members for this plan. Without RRE compilation
    /// this equals [`Self::requested_members`].
    pub worst_case_generated_members: u32,
    /// Worst-case generated-member payload bytes. Without RRE compilation
    /// this equals [`Self::requested_value_bytes`].
    pub worst_case_generated_member_bytes: u32,
}

/// Compute requested and worst-case generated-member accounting.
pub fn account_closed_plan(plan: &AtomicPlan) -> Result<PlanAccounting, AtomicsError> {
    let requested_members = u32_len(plan.mutations().len())?;
    let mut requested_value_bytes = 0u32;
    for mutation in plan.mutations() {
        if let Some(value) = &mutation.encoded_value {
            requested_value_bytes = requested_value_bytes
                .checked_add(u32_len(value.len())?)
                .ok_or(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded))?;
        }
    }
    let requested_plan_bytes = u32_len(encode_canonical_plan(plan)?.len())?;
    // ATM-1.1 has no RRE compiler: each caller mutation is at most one member.
    Ok(PlanAccounting {
        requested_members,
        requested_value_bytes,
        requested_plan_bytes,
        worst_case_generated_members: requested_members,
        worst_case_generated_member_bytes: requested_value_bytes,
    })
}

impl AtomicPlan {
    /// Requested and worst-case generated-member accounting.
    pub fn accounting(&self) -> Result<PlanAccounting, AtomicsError> {
        account_closed_plan(self)
    }
}

fn u32_len(n: usize) -> Result<u32, AtomicsError> {
    u32::try_from(n).map_err(|_| AtomicsError::Refused(AtomicRefuseReason::LimitExceeded))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{AtomicId, CollectionId, HeapId, VersionId};
    use crate::limits::ResourceLimits;
    use crate::plan::{AtomicPlanParts, AtomicProfile, CoordinationScope};

    fn hid(n: u8) -> HeapId {
        let mut b = [0u8; 16];
        b[0] = n;
        HeapId::from_bytes(b).expect("nonzero")
    }

    fn cid(n: u8) -> CollectionId {
        let mut b = [0u8; 16];
        b[0] = n;
        CollectionId::from_bytes(b).expect("nonzero")
    }

    fn aid(n: u8) -> AtomicId {
        let mut b = [0u8; 32];
        b[0] = n;
        AtomicId::from_bytes(b).expect("nonzero")
    }

    fn vid(n: u8) -> VersionId {
        let mut b = [0u8; 16];
        b[0] = n;
        VersionId::from_bytes(b).expect("nonzero")
    }

    fn key(s: &str) -> CanonicalKey {
        CanonicalKey::String(s.to_owned())
    }

    fn close(mutations: Vec<PlanMutation>, predicates: Vec<PlanPredicate>) -> AtomicPlan {
        AtomicPlan::close(AtomicPlanParts {
            profile: AtomicProfile::LocalHeapV1,
            atomic_id: aid(1),
            heap_id: hid(1),
            scope: CoordinationScope::LocalHeap,
            read_frontier: None,
            reads: Vec::new(),
            predicates,
            mutations,
            active_rule_revisions: Vec::new(),
            limits: ResourceLimits::builder_defaults_local_heap(),
        })
        .expect("close")
    }

    #[test]
    fn serialize_canonical_value_is_admitted_bytes() {
        let v = serialize_canonical_value(crate::encoding::ValueEncoding::Bytes, b"hello").unwrap();
        assert_eq!(v.as_bytes(), b"hello");
        assert_eq!(v.len(), 5);
        assert!(!v.is_empty());
        assert!(
            serialize_canonical_value(crate::encoding::ValueEncoding::Bytes, b"")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn serialize_rejects_noncanonical_integer() {
        assert_eq!(
            serialize_canonical_value(crate::encoding::ValueEncoding::Integer, &[0x00, 0x01])
                .unwrap_err(),
            AtomicsError::Refused(AtomicRefuseReason::InvalidValue)
        );
        let v =
            serialize_canonical_value(crate::encoding::ValueEncoding::Integer, &[0x01]).unwrap();
        assert_eq!(v.as_bytes(), &[0x01]);
        assert_eq!(CanonicalValue::from_integer(1).as_bytes(), &[0x01]);
    }

    #[test]
    fn encode_create_and_put_are_value_bearing() {
        let c = encode_create(cid(1), key("a"), CanonicalValue::from_bytes(b"one"));
        assert_eq!(c.kind, MutationKind::Create);
        assert_eq!(c.encoded_value.as_deref(), Some(b"one".as_slice()));
        assert_eq!(c.if_version, None);

        let p = encode_put(cid(1), key("b"), CanonicalValue::from_bytes(b"two"));
        assert_eq!(p.kind, MutationKind::Put);
        assert_eq!(p.if_version, None);
        assert_eq!(p.encoded_value.as_deref(), Some(b"two".as_slice()));
    }

    #[test]
    fn encode_replace_and_delete_carry_version() {
        let r = encode_replace(
            cid(2),
            key("c"),
            vid(7),
            CanonicalValue::from_bytes(b"three"),
        );
        assert_eq!(r.kind, MutationKind::Replace);
        assert_eq!(r.if_version, Some(vid(7)));
        assert_eq!(r.encoded_value.as_deref(), Some(b"three".as_slice()));

        let d = encode_delete(cid(2), key("d"), vid(8));
        assert_eq!(d.kind, MutationKind::Delete);
        assert_eq!(d.if_version, Some(vid(8)));
        assert_eq!(d.encoded_value, None);
    }

    #[test]
    fn encode_public_builder_asserts() {
        let a = encode_assert_absent(cid(1), key("k"));
        assert_eq!(a.kind, PredicateKind::AssertAbsent);
        assert_eq!(a.collection_id, Some(cid(1)));
        assert_eq!(a.version, None);

        let p = encode_assert_present(cid(1), key("k"));
        assert_eq!(p.kind, PredicateKind::AssertPresent);

        let v = encode_assert_version(cid(1), key("k"), vid(3));
        assert_eq!(v.kind, PredicateKind::AssertVersion);
        assert_eq!(v.version, Some(vid(3)));
    }

    #[test]
    fn accounting_counts_mutations_not_assertions() {
        let plan = close(
            vec![
                encode_create(cid(1), key("a"), CanonicalValue::from_bytes(b"aa")),
                encode_delete(cid(1), key("b"), vid(1)),
            ],
            vec![encode_assert_absent(cid(1), key("c"))],
        );
        let acc = plan.accounting().expect("account");
        assert_eq!(acc.requested_members, 2);
        assert_eq!(acc.requested_value_bytes, 2);
        assert!(acc.requested_plan_bytes > 0);
        assert_eq!(acc.worst_case_generated_members, acc.requested_members);
        assert_eq!(
            acc.worst_case_generated_member_bytes,
            acc.requested_value_bytes
        );
    }

    #[test]
    fn assertion_only_plan_has_zero_members() {
        let plan = close(vec![], vec![encode_assert_present(cid(1), key("only"))]);
        let acc = plan.accounting().expect("account");
        assert_eq!(acc.requested_members, 0);
        assert_eq!(acc.requested_value_bytes, 0);
        assert!(acc.requested_plan_bytes > 0);
    }
}
