//! Convert an admitted/closed plan into the exact prepare evidence (CR-ATMR3-001).
//!
//! The durable lane must persist this record, not a synthetic frontier or empty
//! semantic roots. The member slice must be exactly the closed plan mutations.
//! Leftover members are refused until generated consequences have a typed
//! closed-plan representation (CR-ATMR4-001).

use crate::canonical::{
    encode_predicate, encode_read, key_order_bytes, plan_content_root, DOMAIN_ATOMIC_PREDICATES,
    DOMAIN_ATOMIC_READSET, DOMAIN_ATOMIC_RULES,
};
use crate::cbor::{self, Value};
use crate::error::AtomicsError;
use crate::evidence::{AtomicMember, AtomicPrepare};
use crate::id::{AtomicId, CollectionId};
use crate::outcome::AtomicRefuseReason;
use crate::plan::AtomicPlan;
use crate::validate::validate_closed_plan;
use crate::{member_hash, ordered_member_manifest_root};

/// Derive [`AtomicPrepare`] from a closed plan, the bound serialization
/// frontier, and the exact closed members.
///
/// Recomputes `plan_content_root`. Read, predicate, and rule-revision roots
/// come from the plan's canonical data. `limits` and `scope` are the plan's.
/// Members must match plan mutations one-to-one by identity, kind, version,
/// and value hash. Extra, missing, or duplicate members are refused. Reordering
/// the exact set is accepted; the manifest root is canonical.
pub fn prepare_from_closed_plan(
    plan: &AtomicPlan,
    frontier: [u8; 32],
    members: &[AtomicMember],
) -> Result<AtomicPrepare, AtomicsError> {
    validate_closed_plan(plan, plan.heap_id())?;
    bind_members_to_plan(plan, members)?;
    let content_root = plan_content_root(plan)?;
    let ordered_member_manifest_root = ordered_member_manifest_root(plan.heap_id(), members)?;
    Ok(AtomicPrepare {
        atomic_id: plan.atomic_id(),
        heap_id: plan.heap_id(),
        scope: plan.scope(),
        content_root,
        frontier,
        ordered_member_manifest_root,
        read_set_root: plan_read_set_root(plan)?,
        predicate_set_root: plan_predicate_set_root(plan)?,
        active_rule_revision_root: plan_rule_revision_root(plan),
        limits: plan.limits(),
    })
}

/// Canonical read-set root for a closed plan.
pub fn plan_read_set_root(plan: &AtomicPlan) -> Result<[u8; 32], AtomicsError> {
    hash_encoded(DOMAIN_ATOMIC_READSET, plan.reads().iter().map(encode_read))
}

/// Canonical predicate-set root for a closed plan.
pub fn plan_predicate_set_root(plan: &AtomicPlan) -> Result<[u8; 32], AtomicsError> {
    hash_encoded(
        DOMAIN_ATOMIC_PREDICATES,
        plan.predicates().iter().map(encode_predicate),
    )
}

/// Canonical active-rule-revision root for a closed plan.
pub fn plan_rule_revision_root(plan: &AtomicPlan) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN_ATOMIC_RULES);
    for rev in plan.active_rule_revisions() {
        hasher.update(rev);
    }
    *hasher.finalize().as_bytes()
}

fn hash_encoded(
    domain: &[u8],
    items: impl Iterator<Item = Result<Value, AtomicsError>>,
) -> Result<[u8; 32], AtomicsError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    for item in items {
        hasher.update(&cbor::encode_value(&item?)?);
    }
    Ok(*hasher.finalize().as_bytes())
}

fn bind_members_to_plan(plan: &AtomicPlan, members: &[AtomicMember]) -> Result<(), AtomicsError> {
    let mut seen: Vec<(CollectionId, Vec<u8>)> = Vec::new();
    for member in members {
        if member.atomic_id != plan.atomic_id() {
            return Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput));
        }
        member.validate()?;
        let _ = member_hash(member)?;
        let key = key_order_bytes(&member.object_identity.key);
        if seen.iter().any(|(collection, seen_key)| {
            *collection == member.object_identity.collection_id && *seen_key == key
        }) {
            return Err(AtomicsError::Refused(AtomicRefuseReason::DuplicateTarget));
        }
        seen.push((member.object_identity.collection_id, key));
    }
    let mut unused: Vec<&AtomicMember> = members.iter().collect();
    for mutation in plan.mutations() {
        let key = key_order_bytes(&mutation.key);
        let pos = unused.iter().position(|member| {
            member.object_identity.collection_id == mutation.collection_id
                && key_order_bytes(&member.object_identity.key) == key
        });
        let Some(pos) = pos else {
            return Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput));
        };
        let member = unused.swap_remove(pos);
        if member.member_kind != mutation.kind || member.before_version != mutation.if_version {
            return Err(AtomicsError::Refused(AtomicRefuseReason::InvalidValue));
        }
        match (&mutation.encoded_value, member.after_content_hash) {
            (None, None) => {}
            (Some(value), Some(hash)) if hash == *blake3::hash(value).as_bytes() => {}
            _ => return Err(AtomicsError::Refused(AtomicRefuseReason::InvalidValue)),
        }
    }
    if !unused.is_empty() {
        return Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput));
    }
    Ok(())
}

/// True when `members` is the exact closed set named by `prepare`.
pub fn members_match_prepare(prepare: &AtomicPrepare, members: &[AtomicMember]) -> bool {
    members
        .iter()
        .all(|m| m.atomic_id == prepare.atomic_id && m.validate().is_ok())
        && ordered_member_manifest_root(prepare.heap_id, members)
            .ok()
            .is_some_and(|root| root == prepare.ordered_member_manifest_root)
}

/// Identity used by exclusive lane files.
pub fn prepare_identity(prepare: &AtomicPrepare) -> AtomicId {
    prepare.atomic_id
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{CollectionId, ContentRoot, HeapId, VersionId};
    use crate::limits::ResourceLimits;
    use crate::plan::{
        AtomicPlanParts, AtomicProfile, CanonicalKey, CoordinationScope, MutationKind,
        PlanMutation, PlanPredicate, PredicateKind, ReadWitness,
    };
    use crate::ObjectIdentity;

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

    fn aid(n: u8) -> AtomicId {
        let mut b = [0u8; 32];
        b[0] = n;
        AtomicId::from_bytes(b).unwrap()
    }

    fn vid(n: u8) -> VersionId {
        let mut b = [0u8; 16];
        b[0] = n;
        VersionId::from_bytes(b).unwrap()
    }

    fn close(
        atomic_id: AtomicId,
        mutations: Vec<PlanMutation>,
        reads: Vec<ReadWitness>,
        predicates: Vec<PlanPredicate>,
        rules: Vec<[u8; 32]>,
        read_frontier: Option<[u8; 32]>,
    ) -> AtomicPlan {
        AtomicPlan::close(AtomicPlanParts {
            profile: AtomicProfile::LocalHeapV1,
            atomic_id,
            heap_id: hid(),
            scope: CoordinationScope::LocalHeap,
            read_frontier,
            reads,
            predicates,
            mutations,
            active_rule_revisions: rules,
            limits: ResourceLimits::hard_local_heap(),
        })
        .unwrap()
    }

    fn create_member(id: AtomicId, value: &[u8]) -> AtomicMember {
        member_on(id, 0, "k", value)
    }

    fn member_on(id: AtomicId, ordinal: u32, key: &str, value: &[u8]) -> AtomicMember {
        AtomicMember {
            atomic_id: id,
            ordinal,
            object_identity: ObjectIdentity::new(cid(), CanonicalKey::String(key.into())),
            member_kind: MutationKind::Create,
            before_version: None,
            after_content_hash: Some(*blake3::hash(value).as_bytes()),
            event_id: vid(ordinal as u8 + 1),
        }
    }

    fn mutation(key: &str, value: &[u8]) -> PlanMutation {
        PlanMutation {
            kind: MutationKind::Create,
            collection_id: cid(),
            key: CanonicalKey::String(key.into()),
            encoded_value: Some(value.to_vec()),
            if_version: None,
        }
    }

    #[test]
    fn prepare_roots_come_from_the_plan_not_placeholders() {
        let id = aid(1);
        let value = b"payload";
        let plan = close(
            id,
            vec![PlanMutation {
                kind: MutationKind::Create,
                collection_id: cid(),
                key: CanonicalKey::String("k".into()),
                encoded_value: Some(value.to_vec()),
                if_version: None,
            }],
            vec![ReadWitness {
                collection_id: cid(),
                key: CanonicalKey::String("seen".into()),
                observed_version: Some(vid(9)),
                projection_hash: [9u8; 32],
            }],
            vec![PlanPredicate {
                kind: PredicateKind::AssertAbsent,
                collection_id: Some(cid()),
                key: Some(CanonicalKey::String("gone".into())),
                version: None,
                encoded: None,
            }],
            vec![[7u8; 32]],
            Some([3u8; 32]),
        );
        let member = create_member(id, value);
        let frontier = [0x11u8; 32];
        let prepare = prepare_from_closed_plan(&plan, frontier, std::slice::from_ref(&member))
            .expect("derive");
        assert_eq!(prepare.content_root, plan_content_root(&plan).unwrap());
        assert_eq!(prepare.frontier, frontier);
        assert_eq!(prepare.limits, plan.limits());
        assert_eq!(prepare.scope, plan.scope());
        assert_eq!(prepare.read_set_root, plan_read_set_root(&plan).unwrap());
        assert_eq!(
            prepare.predicate_set_root,
            plan_predicate_set_root(&plan).unwrap()
        );
        assert_eq!(
            prepare.active_rule_revision_root,
            plan_rule_revision_root(&plan)
        );
        assert_ne!(
            prepare.read_set_root,
            *blake3::hash(DOMAIN_ATOMIC_READSET).as_bytes()
        );
        assert_ne!(
            prepare.predicate_set_root,
            *blake3::hash(DOMAIN_ATOMIC_PREDICATES).as_bytes()
        );
        assert_ne!(
            prepare.active_rule_revision_root,
            *blake3::hash(DOMAIN_ATOMIC_RULES).as_bytes()
        );
        assert_ne!(
            prepare.content_root,
            ContentRoot::from_bytes([1u8; 32]).unwrap()
        );
        let again =
            prepare_from_closed_plan(&plan, frontier, std::slice::from_ref(&member)).unwrap();
        assert_eq!(prepare, again);
    }

    #[test]
    fn assertion_only_plan_keeps_empty_manifest_and_semantic_roots() {
        let id = aid(2);
        let plan = close(id, vec![], vec![], vec![], vec![], None);
        let prepare = prepare_from_closed_plan(&plan, [0x22; 32], &[]).unwrap();
        assert_eq!(prepare.content_root, plan_content_root(&plan).unwrap());
        assert_eq!(
            prepare.ordered_member_manifest_root,
            ordered_member_manifest_root(hid(), &[]).unwrap()
        );
        assert_eq!(
            prepare.read_set_root,
            *blake3::hash(DOMAIN_ATOMIC_READSET).as_bytes()
        );
        assert_eq!(
            prepare.predicate_set_root,
            *blake3::hash(DOMAIN_ATOMIC_PREDICATES).as_bytes()
        );
        assert_eq!(
            prepare.active_rule_revision_root,
            *blake3::hash(DOMAIN_ATOMIC_RULES).as_bytes()
        );
    }

    #[test]
    fn mutating_value_or_missing_member_refuses() {
        let id = aid(3);
        let plan = close(
            id,
            vec![PlanMutation {
                kind: MutationKind::Create,
                collection_id: cid(),
                key: CanonicalKey::String("k".into()),
                encoded_value: Some(b"good".to_vec()),
                if_version: None,
            }],
            vec![],
            vec![],
            vec![],
            None,
        );
        let mut bad = create_member(id, b"good");
        bad.after_content_hash = Some(*blake3::hash(b"other").as_bytes());
        assert_eq!(
            prepare_from_closed_plan(&plan, [0; 32], std::slice::from_ref(&bad)).unwrap_err(),
            AtomicsError::Refused(AtomicRefuseReason::InvalidValue)
        );
        assert_eq!(
            prepare_from_closed_plan(&plan, [0; 32], &[]).unwrap_err(),
            AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
        );
    }

    #[test]
    fn leftover_member_is_refused() {
        let id = aid(4);
        let plan = close(id, vec![mutation("k", b"v")], vec![], vec![], vec![], None);
        let primary = create_member(id, b"v");
        let extra = member_on(id, 1, "hist", b"hist");
        assert_eq!(
            prepare_from_closed_plan(&plan, [1; 32], &[primary, extra]).unwrap_err(),
            AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
        );
    }

    #[test]
    fn closed_member_set_has_specified_results() {
        let id = aid(5);
        let a = member_on(id, 0, "a", b"va");
        let b = member_on(id, 1, "b", b"vb");
        let plan = close(
            id,
            vec![mutation("a", b"va"), mutation("b", b"vb")],
            vec![],
            vec![],
            vec![],
            None,
        );
        let frontier = [2u8; 32];
        let accepted = prepare_from_closed_plan(&plan, frontier, &[a.clone(), b.clone()]).unwrap();
        let reordered = prepare_from_closed_plan(&plan, frontier, &[b.clone(), a.clone()]).unwrap();
        assert_eq!(accepted, reordered);
        assert_eq!(
            accepted.ordered_member_manifest_root,
            ordered_member_manifest_root(hid(), &[a.clone(), b.clone()]).unwrap()
        );
        assert_eq!(accepted.content_root, plan_content_root(&plan).unwrap());
        assert_eq!(
            prepare_from_closed_plan(&plan, frontier, std::slice::from_ref(&a)).unwrap_err(),
            AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
        );
        assert_eq!(
            prepare_from_closed_plan(
                &plan,
                frontier,
                &[a.clone(), b.clone(), member_on(id, 2, "c", b"vc")]
            )
            .unwrap_err(),
            AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
        );
        let mut duplicate = a.clone();
        duplicate.ordinal = 2;
        duplicate.event_id = vid(3);
        assert_eq!(
            prepare_from_closed_plan(&plan, frontier, &[a.clone(), duplicate]).unwrap_err(),
            AtomicsError::Refused(AtomicRefuseReason::DuplicateTarget)
        );
        let substituted = member_on(id, 0, "other", b"va");
        assert_eq!(
            prepare_from_closed_plan(&plan, frontier, &[substituted, b]).unwrap_err(),
            AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
        );
        let empty = close(id, vec![], vec![], vec![], vec![], None);
        assert_eq!(
            prepare_from_closed_plan(&empty, frontier, std::slice::from_ref(&a)).unwrap_err(),
            AtomicsError::Refused(AtomicRefuseReason::MalformedInput)
        );
    }
}
