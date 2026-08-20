//! Deliberately slow serial in-memory oracle (`ATOMICS_SPEC` §8, plan §6).
//!
//! One plan at a time. Validate every member, then apply all or none. No store,
//! thread, or network. The history format is the shared record later store
//! tests replay against this same kernel.

use crate::canonical::{key_order_bytes, plan_content_root, DOMAIN_ATOMIC_DECISION};
use crate::error::AtomicsError;
use crate::evidence::Durability;
use crate::id::{AtomicId, CollectionId, ContentRoot, HeapId, VersionId};
use crate::outcome::{
    AtomicAbortReason, AtomicMemberReceipt, AtomicOutcome, AtomicReceipt, AtomicRefuseReason,
    AtomicStatus, LogicalStatus, MaterialStatus,
};
use crate::plan::{
    AtomicPlan, CanonicalKey, MutationKind, PlanMutation, PlanPredicate, PredicateKind,
};
use crate::predicate::{decode_collection_lifecycle_payload, CollectionLifecycleState};
use std::collections::BTreeMap;

/// Shared history record consumed by this oracle and later store tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleHistoryRecord {
    /// What the oracle did.
    pub kind: OracleHistoryKind,
    /// Plan identity, when one was named.
    pub atomic_id: AtomicId,
    /// Plan content root, when computed.
    pub content_root: Option<ContentRoot>,
    /// Commit position when a committed decision was issued.
    pub commit_position: Option<u64>,
    /// True only after a whole-plan publication. Never true for a partial apply.
    pub published: bool,
    /// Refuse reason when no Atomic was issued.
    pub refuse: Option<AtomicRefuseReason>,
    /// Abort reason when a not-committed decision was issued.
    pub abort: Option<AtomicAbortReason>,
}

/// History kinds. There is no "partially published" kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OracleHistoryKind {
    /// Structural refusal. No tombstone, no mutation.
    Refused,
    /// Same ID and root returned the original issued outcome.
    Replayed,
    /// Same ID, different root. No new execution.
    IdConflict,
    /// Issued a committed decision and published every member.
    IssuedCommitted,
    /// Issued a not-committed decision. Visible state unchanged.
    IssuedNotCommitted,
}

/// Visible cell after a committed publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleCell {
    /// Current version.
    pub version: VersionId,
    /// Canonical value bytes.
    pub value: Vec<u8>,
}

#[derive(Clone, Debug)]
enum Issued {
    Committed(AtomicReceipt),
    NotCommitted(AtomicAbortReason),
}

/// Serial in-memory Heap oracle.
///
/// Intentionally two-pass and single-threaded so later packages cannot treat
/// this as a fast path or a concurrent writer.
#[derive(Clone, Debug)]
pub struct SerialOracle {
    heap_id: HeapId,
    next_position: u64,
    cells: BTreeMap<(CollectionId, Vec<u8>), OracleCell>,
    collection_lifecycles: BTreeMap<CollectionId, CollectionLifecycleState>,
    issued: BTreeMap<AtomicId, (ContentRoot, Issued)>,
    history: Vec<OracleHistoryRecord>,
}

impl SerialOracle {
    /// Bind one Heap. Cross-Heap plans are refused.
    pub fn new(heap_id: HeapId) -> Self {
        Self {
            heap_id,
            next_position: 1,
            cells: BTreeMap::new(),
            collection_lifecycles: BTreeMap::new(),
            issued: BTreeMap::new(),
            history: Vec::new(),
        }
    }

    /// Heap this oracle models.
    pub const fn heap_id(&self) -> HeapId {
        self.heap_id
    }

    /// Append-only history. Store tests may replay this sequence.
    pub fn history(&self) -> &[OracleHistoryRecord] {
        &self.history
    }

    /// Point read of published state. Staged-but-unpublished never exists here.
    pub fn get(&self, collection: CollectionId, key: &CanonicalKey) -> Option<&OracleCell> {
        self.cells.get(&(collection, key_order_bytes(key)))
    }

    /// Establish authoritative lifecycle state for a collection. Collections
    /// not explicitly established model ordinary active handles.
    pub fn set_collection_lifecycle(
        &mut self,
        collection: CollectionId,
        state: CollectionLifecycleState,
    ) {
        self.collection_lifecycles.insert(collection, state);
    }

    /// Status under complete coverage.
    pub fn status(&self, atomic_id: AtomicId) -> AtomicStatus {
        match self.issued.get(&atomic_id) {
            None => AtomicStatus::not_found(),
            Some((root, Issued::Committed(receipt))) => AtomicStatus {
                logical: LogicalStatus::Committed,
                material: MaterialStatus::Complete,
                content_root: Some(*root),
                receipt: Some(receipt.clone()),
            },
            Some((root, Issued::NotCommitted(_))) => AtomicStatus {
                logical: LogicalStatus::NotCommitted,
                material: MaterialStatus::Complete,
                content_root: Some(*root),
                receipt: None,
            },
        }
    }

    /// Apply one closed plan. Serial: no overlapping apply, no partial publish.
    pub fn apply(&mut self, plan: &AtomicPlan) -> Result<AtomicOutcome, AtomicsError> {
        if let Err(err) = crate::validate::validate_closed_plan(plan, self.heap_id) {
            return match err {
                AtomicsError::Refused(reason) => self.refuse(plan, reason),
                other => Err(other),
            };
        }

        let root = plan_content_root(plan)?;
        if let Some((prev_root, issued)) = self.issued.get(&plan.atomic_id()) {
            if *prev_root == root {
                let outcome = match issued {
                    Issued::Committed(r) => {
                        let mut replayed = r.clone();
                        replayed.replayed = true;
                        AtomicOutcome::Committed(replayed)
                    }
                    Issued::NotCommitted(reason) => AtomicOutcome::NotCommitted {
                        atomic_id: plan.atomic_id(),
                        reason: *reason,
                    },
                };
                self.history.push(OracleHistoryRecord {
                    kind: OracleHistoryKind::Replayed,
                    atomic_id: plan.atomic_id(),
                    content_root: Some(root),
                    commit_position: match &outcome {
                        AtomicOutcome::Committed(r) => Some(r.commit_position),
                        _ => None,
                    },
                    published: matches!(outcome, AtomicOutcome::Committed(_)),
                    refuse: None,
                    abort: match &outcome {
                        AtomicOutcome::NotCommitted { reason, .. } => Some(*reason),
                        _ => None,
                    },
                });
                return Ok(outcome);
            }
            self.history.push(OracleHistoryRecord {
                kind: OracleHistoryKind::IdConflict,
                atomic_id: plan.atomic_id(),
                content_root: Some(root),
                commit_position: None,
                published: false,
                refuse: Some(AtomicRefuseReason::AtomicIdConflict),
                abort: None,
            });
            return Err(AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict));
        }

        if let Some(reason) = self.validate_all(plan) {
            self.issued
                .insert(plan.atomic_id(), (root, Issued::NotCommitted(reason)));
            self.history.push(OracleHistoryRecord {
                kind: OracleHistoryKind::IssuedNotCommitted,
                atomic_id: plan.atomic_id(),
                content_root: Some(root),
                commit_position: None,
                published: false,
                refuse: None,
                abort: Some(reason),
            });
            return Ok(AtomicOutcome::NotCommitted {
                atomic_id: plan.atomic_id(),
                reason,
            });
        }

        // Second pass only after every member validated. No member is visible
        // until this loop finishes and the committed record is stored.
        let position = self.next_position;
        self.next_position += 1;
        let mut members = Vec::with_capacity(plan.mutations().len());
        for (i, m) in plan.mutations().iter().enumerate() {
            let key_bytes = key_order_bytes(&m.key);
            let slot = (m.collection_id, key_bytes.clone());
            let before = self.cells.get(&slot).map(|c| c.version);
            let after = match m.kind {
                MutationKind::Delete => {
                    self.cells.remove(&slot);
                    None
                }
                MutationKind::Create | MutationKind::Put | MutationKind::Replace => {
                    let value = m.encoded_value.clone().unwrap_or_default();
                    let version = mint_version(position, i as u32);
                    self.cells.insert(slot, OracleCell { version, value });
                    Some(version)
                }
            };
            members.push(AtomicMemberReceipt {
                collection_id: m.collection_id,
                key: key_bytes,
                before_version: before,
                after_version: after,
                event_id: mint_version(position, 0x8000_0000 | i as u32),
            });
        }

        let receipt = AtomicReceipt {
            atomic_id: plan.atomic_id(),
            heap_id: self.heap_id,
            content_root: root,
            commit_position: position,
            durability: Durability::Durable,
            members,
            decision_hash: decision_hash(plan.atomic_id(), root, position),
            replayed: false,
        };
        self.issued
            .insert(plan.atomic_id(), (root, Issued::Committed(receipt.clone())));
        self.history.push(OracleHistoryRecord {
            kind: OracleHistoryKind::IssuedCommitted,
            atomic_id: plan.atomic_id(),
            content_root: Some(root),
            commit_position: Some(position),
            published: true,
            refuse: None,
            abort: None,
        });
        Ok(AtomicOutcome::Committed(receipt))
    }

    fn refuse(
        &mut self,
        plan: &AtomicPlan,
        reason: AtomicRefuseReason,
    ) -> Result<AtomicOutcome, AtomicsError> {
        self.history.push(OracleHistoryRecord {
            kind: OracleHistoryKind::Refused,
            atomic_id: plan.atomic_id(),
            content_root: None,
            commit_position: None,
            published: false,
            refuse: Some(reason),
            abort: None,
        });
        Err(AtomicsError::Refused(reason))
    }

    fn validate_all(&self, plan: &AtomicPlan) -> Option<AtomicAbortReason> {
        for p in plan.predicates() {
            if let Some(reason) = self.check_predicate(p) {
                return Some(reason);
            }
        }
        for w in plan.reads() {
            let cell = self.get(w.collection_id, &w.key);
            match (w.observed_version, cell) {
                (None, None) => {}
                (Some(v), Some(c)) if c.version == v => {}
                _ => return Some(AtomicAbortReason::PreconditionConflict),
            }
        }
        for m in plan.mutations() {
            if let Some(reason) = self.check_mutation(m) {
                return Some(reason);
            }
        }
        None
    }

    fn check_predicate(&self, p: &PlanPredicate) -> Option<AtomicAbortReason> {
        if p.kind == PredicateKind::HeapAuthorityRevision {
            // Bound at admission. Not a data precondition.
            return None;
        }
        if p.kind == PredicateKind::CollectionLifecycleState {
            let collection = match p.collection_id {
                Some(collection) => collection,
                None => return Some(AtomicAbortReason::PreconditionConflict),
            };
            let expected = match p
                .encoded
                .as_deref()
                .and_then(|bytes| decode_collection_lifecycle_payload(bytes).ok())
            {
                Some(expected) => expected,
                None => return Some(AtomicAbortReason::PreconditionConflict),
            };
            let actual = self
                .collection_lifecycles
                .get(&collection)
                .copied()
                .unwrap_or(CollectionLifecycleState::Active);
            return (actual != expected).then_some(AtomicAbortReason::PreconditionConflict);
        }
        let (coll, key) = match (p.collection_id, p.key.as_ref()) {
            (Some(c), Some(k)) => (c, k),
            _ => return Some(AtomicAbortReason::PreconditionConflict),
        };
        let cell = self.get(coll, key);
        match p.kind {
            PredicateKind::AssertAbsent => {
                if cell.is_some() {
                    Some(AtomicAbortReason::PreconditionConflict)
                } else {
                    None
                }
            }
            PredicateKind::AssertPresent => {
                if cell.is_none() {
                    Some(AtomicAbortReason::PreconditionConflict)
                } else {
                    None
                }
            }
            PredicateKind::AssertVersion => match (p.version, cell) {
                (Some(v), Some(c)) if c.version == v => None,
                _ => Some(AtomicAbortReason::PreconditionConflict),
            },
            _ => Some(AtomicAbortReason::RuleRejected),
        }
    }

    fn check_mutation(&self, m: &PlanMutation) -> Option<AtomicAbortReason> {
        let cell = self.get(m.collection_id, &m.key);
        match m.kind {
            MutationKind::Create => {
                if cell.is_some() {
                    Some(AtomicAbortReason::PreconditionConflict)
                } else {
                    None
                }
            }
            MutationKind::Put => None,
            MutationKind::Replace | MutationKind::Delete => match (m.if_version, cell) {
                (Some(v), Some(c)) if c.version == v => None,
                _ => Some(AtomicAbortReason::PreconditionConflict),
            },
        }
    }
}

fn mint_version(position: u64, ordinal: u32) -> VersionId {
    let mut bytes = [0u8; 16];
    bytes[0..8].copy_from_slice(&position.to_be_bytes());
    bytes[8..12].copy_from_slice(&ordinal.to_be_bytes());
    bytes[15] = 1;
    VersionId::from_bytes(bytes).expect("nonzero version")
}

fn decision_hash(id: AtomicId, root: ContentRoot, position: u64) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN_ATOMIC_DECISION);
    hasher.update(id.as_bytes());
    hasher.update(root.as_bytes());
    hasher.update(&position.to_be_bytes());
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{AtomicPlanParts, AtomicProfile, CoordinationScope};
    use crate::ResourceLimits;

    fn hid() -> HeapId {
        let mut b = [0u8; 16];
        b[0] = 1;
        HeapId::from_bytes(b).unwrap()
    }

    fn cid(n: u8) -> CollectionId {
        let mut b = [0u8; 16];
        b[0] = n;
        CollectionId::from_bytes(b).unwrap()
    }

    fn aid(n: u8) -> AtomicId {
        let mut b = [0u8; 32];
        b[0] = n;
        AtomicId::from_bytes(b).unwrap()
    }

    fn create(id: u8, k: &str, val: &[u8]) -> AtomicPlan {
        AtomicPlan::close(AtomicPlanParts {
            profile: AtomicProfile::LocalHeapV1,
            atomic_id: aid(id),
            heap_id: hid(),
            scope: CoordinationScope::LocalHeap,
            read_frontier: None,
            reads: Vec::new(),
            predicates: Vec::new(),
            mutations: vec![PlanMutation {
                kind: MutationKind::Create,
                collection_id: cid(1),
                key: CanonicalKey::String(k.into()),
                encoded_value: Some(val.to_vec()),
                if_version: None,
            }],
            active_rule_revisions: Vec::new(),
            limits: ResourceLimits::builder_defaults_local_heap(),
        })
        .unwrap()
    }

    #[test]
    fn commit_then_point_get() {
        let mut o = SerialOracle::new(hid());
        let plan = create(1, "k", b"v");
        match o.apply(&plan).unwrap() {
            AtomicOutcome::Committed(r) => {
                assert!(!r.replayed);
                assert_eq!(r.commit_position, 1);
                assert_eq!(r.durability, Durability::Durable);
            }
            other => panic!("{other:?}"),
        }
        let status = o.status(plan.atomic_id());
        assert_eq!(status.logical, LogicalStatus::Committed);
        assert_eq!(
            status.receipt.as_ref().unwrap().durability,
            Durability::Durable
        );
        assert_eq!(o.status(aid(9)).logical, LogicalStatus::NotFound);
        assert!(o.status(aid(9)).receipt.is_none());
        assert_eq!(
            o.get(cid(1), &CanonicalKey::String("k".into()))
                .unwrap()
                .value,
            b"v"
        );
        for h in o.history() {
            if h.kind == OracleHistoryKind::IssuedNotCommitted {
                assert!(!h.published);
            }
        }
    }

    #[test]
    fn lifecycle_predicate_observes_configured_authoritative_state() {
        use crate::encode::encode_collection_lifecycle_state;

        let plan = AtomicPlan::close(AtomicPlanParts {
            profile: AtomicProfile::LocalHeapV1,
            atomic_id: aid(8),
            heap_id: hid(),
            scope: CoordinationScope::LocalHeap,
            read_frontier: None,
            reads: Vec::new(),
            predicates: vec![encode_collection_lifecycle_state(
                cid(1),
                CollectionLifecycleState::Active,
            )
            .unwrap()],
            mutations: vec![PlanMutation {
                kind: MutationKind::Create,
                collection_id: cid(1),
                key: CanonicalKey::String("k".into()),
                encoded_value: Some(b"v".to_vec()),
                if_version: None,
            }],
            active_rule_revisions: Vec::new(),
            limits: ResourceLimits::builder_defaults_local_heap(),
        })
        .unwrap();

        let mut active = SerialOracle::new(hid());
        assert!(matches!(
            active.apply(&plan).unwrap(),
            AtomicOutcome::Committed(_)
        ));

        let mut retired = SerialOracle::new(hid());
        retired.set_collection_lifecycle(cid(1), CollectionLifecycleState::Retired);
        assert!(matches!(
            retired.apply(&plan).unwrap(),
            AtomicOutcome::NotCommitted {
                reason: AtomicAbortReason::PreconditionConflict,
                ..
            }
        ));
        assert!(retired
            .get(cid(1), &CanonicalKey::String("k".into()))
            .is_none());
    }

    #[test]
    fn failed_second_member_does_not_publish_first() {
        let mut o = SerialOracle::new(hid());
        let first = create(1, "keep", b"1");
        o.apply(&first).unwrap();
        let two = AtomicPlan::close(AtomicPlanParts {
            profile: AtomicProfile::LocalHeapV1,
            atomic_id: aid(2),
            heap_id: hid(),
            scope: CoordinationScope::LocalHeap,
            read_frontier: None,
            reads: Vec::new(),
            predicates: Vec::new(),
            mutations: vec![
                PlanMutation {
                    kind: MutationKind::Create,
                    collection_id: cid(1),
                    key: CanonicalKey::String("new".into()),
                    encoded_value: Some(b"x".to_vec()),
                    if_version: None,
                },
                PlanMutation {
                    kind: MutationKind::Create,
                    collection_id: cid(1),
                    key: CanonicalKey::String("keep".into()),
                    encoded_value: Some(b"clash".to_vec()),
                    if_version: None,
                },
            ],
            active_rule_revisions: Vec::new(),
            limits: ResourceLimits::builder_defaults_local_heap(),
        })
        .unwrap();
        match o.apply(&two).unwrap() {
            AtomicOutcome::NotCommitted { reason, .. } => {
                assert_eq!(reason, AtomicAbortReason::PreconditionConflict);
            }
            other => panic!("{other:?}"),
        }
        assert!(o.get(cid(1), &CanonicalKey::String("new".into())).is_none());
        assert_eq!(
            o.get(cid(1), &CanonicalKey::String("keep".into()))
                .unwrap()
                .value,
            b"1"
        );
        assert!(!o.history().last().unwrap().published);
    }

    #[test]
    fn partition_scope_is_refused_without_status() {
        let mut o = SerialOracle::new(hid());
        let plan = AtomicPlan::close(AtomicPlanParts {
            profile: AtomicProfile::LocalHeapV1,
            atomic_id: aid(7),
            heap_id: hid(),
            scope: CoordinationScope::Partition,
            read_frontier: None,
            reads: Vec::new(),
            predicates: Vec::new(),
            mutations: vec![PlanMutation {
                kind: MutationKind::Create,
                collection_id: cid(1),
                key: CanonicalKey::String("k".into()),
                encoded_value: Some(b"v".to_vec()),
                if_version: None,
            }],
            active_rule_revisions: Vec::new(),
            limits: ResourceLimits::hard_partition(),
        })
        .unwrap();
        assert_eq!(
            o.apply(&plan).unwrap_err(),
            AtomicsError::Refused(AtomicRefuseReason::ScopeUnavailable)
        );
        assert_eq!(o.status(plan.atomic_id()).logical, LogicalStatus::NotFound);
        assert!(o.get(cid(1), &CanonicalKey::String("k".into())).is_none());
        let last = o.history().last().unwrap();
        assert_eq!(last.kind, OracleHistoryKind::Refused);
        assert!(!last.published);
        assert_eq!(last.refuse, Some(AtomicRefuseReason::ScopeUnavailable));
    }
}
