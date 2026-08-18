//! SDK-internal typed builder (`ATOMICS_SPEC` §§6, 14–15).
//!
//! Lives in this pure crate so `residiuum-sdk` can wrap it later without
//! growing store or inventing a second compiler. Collection handles are Heap-
//! bound IDs plus ordinary rights — not `HeapClient` types. ATM-5 owns the
//! public SDK surface and must not advertise `Capabilities::atomics` until
//! that package is accepted.

use crate::canonical::key_order_bytes;
use crate::encode::{
    encode_assert_absent, encode_assert_present, encode_assert_version, encode_create,
    encode_delete, encode_heap_authority_revision, encode_put, encode_replace, CanonicalValue,
};
use crate::encoding::EncodingProfile;
use crate::error::AtomicsError;
use crate::id::{AtomicId, CollectionId, HeapId, VersionId};
use crate::limits::ResourceLimits;
use crate::outcome::AtomicRefuseReason;
use crate::plan::{
    AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, CoordinationScope, MutationKind,
    PlanMutation, PlanPredicate, PredicateKind, ReadWitness,
};
use crate::validate::validate_closed_plan;
use std::collections::BTreeMap;
use std::time::Instant;

/// Ordinary collection rights (`ATOMICS_SPEC` §14).
///
/// Administrative bits (`RuleAdmin`, `AtomicAdmin`) are not granted here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct CollectionRights(u8);

impl CollectionRights {
    /// Observe a key, including public-builder asserts and read witnesses.
    pub const READ: Self = Self(1 << 0);
    /// Create-if-absent.
    pub const CREATE: Self = Self(1 << 1);
    /// Explicit blind upsert.
    pub const PUT: Self = Self(1 << 2);
    /// Version-replace.
    pub const REPLACE: Self = Self(1 << 3);
    /// Version-delete.
    pub const DELETE: Self = Self(1 << 4);

    /// No ordinary rights.
    pub const fn empty() -> Self {
        Self(0)
    }

    /// Union of ordinary data rights.
    pub const fn ordinary() -> Self {
        Self(Self::READ.0 | Self::CREATE.0 | Self::PUT.0 | Self::REPLACE.0 | Self::DELETE.0)
    }

    /// Bitwise union.
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// True when every bit in `other` is present.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CollectionGrant {
    rights: CollectionRights,
    encoding: EncodingProfile,
}

/// Trusted current Heap authority and per-collection grants.
///
/// This type is a trust boundary, not a bag of fields. Downstream crates
/// cannot construct or elevate a view (CR-R2-004). ATM-5 / the capability
/// subsystem will mint one from an unforgeable token. Tests use
/// `cfg(test)` or the crate-local `test-authority` feature only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustedAuthorityView {
    heap_id: HeapId,
    revision: [u8; 32],
    grants: BTreeMap<CollectionId, CollectionGrant>,
}

impl TrustedAuthorityView {
    /// Empty grant set at `revision` on one Heap.
    ///
    /// Crate-internal / test mint only. Downstream crates cannot call this
    /// (CR-R2-004). ATM-5 will mint from an unforgeable capability token.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(heap_id: HeapId, revision: [u8; 32]) -> Self {
        Self {
            heap_id,
            revision,
            grants: BTreeMap::new(),
        }
    }

    /// Record granted ordinary rights. Encoding defaults to string keys / byte values.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn grant(
        &mut self,
        collection_id: CollectionId,
        rights: CollectionRights,
    ) -> &mut Self {
        let encoding = self
            .grants
            .get(&collection_id)
            .map(|g| g.encoding)
            .unwrap_or(EncodingProfile::STRING_BYTES);
        self.grant_with_encoding(collection_id, rights, encoding)
    }

    /// Record granted rights together with the collection's frozen encoding profile.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn grant_with_encoding(
        &mut self,
        collection_id: CollectionId,
        rights: CollectionRights,
        encoding: EncodingProfile,
    ) -> &mut Self {
        let entry = self.grants.entry(collection_id).or_insert(CollectionGrant {
            rights: CollectionRights::empty(),
            encoding,
        });
        entry.rights = entry.rights.union(rights);
        entry.encoding = encoding;
        self
    }

    /// Heap this view describes.
    pub const fn heap_id(&self) -> HeapId {
        self.heap_id
    }

    /// Current authority/security revision.
    pub const fn revision(&self) -> [u8; 32] {
        self.revision
    }

    /// Granted rights for a collection, if any.
    pub fn granted(&self, collection_id: CollectionId) -> CollectionRights {
        self.grants
            .get(&collection_id)
            .map(|g| g.rights)
            .unwrap_or_else(CollectionRights::empty)
    }

    /// Frozen encoding profile for a granted collection.
    pub fn encoding(&self, collection_id: CollectionId) -> Option<EncodingProfile> {
        self.grants.get(&collection_id).map(|g| g.encoding)
    }
}

/// Heap-bound collection identity presented to the builder.
///
/// Construct only via [`BoundCollection::from_trusted`]. A handle from another
/// Heap is refused as [`AtomicRefuseReason::CrossHeapCollection`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundCollection {
    heap_id: HeapId,
    collection_id: CollectionId,
    rights: CollectionRights,
    authority_revision: [u8; 32],
    encoding: EncodingProfile,
}

impl BoundCollection {
    /// Project a granted collection out of a trusted authority view.
    pub fn from_trusted(
        view: &TrustedAuthorityView,
        collection_id: CollectionId,
    ) -> Result<Self, AtomicsError> {
        let grant = view
            .grants
            .get(&collection_id)
            .ok_or(AtomicsError::Refused(
                AtomicRefuseReason::AuthorizationFailure,
            ))?;
        if grant.rights == CollectionRights::empty() {
            return Err(AtomicsError::Refused(
                AtomicRefuseReason::AuthorizationFailure,
            ));
        }
        Ok(Self {
            heap_id: view.heap_id,
            collection_id,
            rights: grant.rights,
            authority_revision: view.revision,
            encoding: grant.encoding,
        })
    }

    /// Heap this handle belongs to.
    pub const fn heap_id(&self) -> HeapId {
        self.heap_id
    }

    /// Collection identity.
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    /// Granted ordinary rights.
    pub const fn rights(&self) -> CollectionRights {
        self.rights
    }

    /// Authority revision frozen into the handle.
    pub const fn authority_revision(&self) -> [u8; 32] {
        self.authority_revision
    }

    /// Frozen key/value encoding profile.
    pub const fn encoding(&self) -> EncodingProfile {
        self.encoding
    }
}

/// Construction options. The Atomic identity is required (`ATOMICS_SPEC` §15).
#[derive(Clone, Debug)]
pub struct AtomicOptions {
    atomic_id: AtomicId,
    deadline: Option<Instant>,
    limits: ResourceLimits,
    scope: CoordinationScope,
    read_frontier: Option<[u8; 32]>,
}

impl AtomicOptions {
    /// Require a caller-retained Atomic identity.
    pub fn new(atomic_id: AtomicId) -> Self {
        Self {
            atomic_id,
            deadline: None,
            limits: ResourceLimits::builder_defaults_local_heap(),
            scope: CoordinationScope::LocalHeap,
            read_frontier: None,
        }
    }

    /// Absolute construction deadline. After this instant every method refuses.
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Applied semantic limits. Raising above the scope hard ceiling is refused.
    pub fn with_limits(mut self, limits: ResourceLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Coordination scope. Partition is refused until a later profile.
    pub fn with_scope(mut self, scope: CoordinationScope) -> Self {
        self.scope = scope;
        self
    }

    /// Read frontier required when the plan records prior-read witnesses.
    pub fn with_read_frontier(mut self, frontier: [u8; 32]) -> Self {
        self.read_frontier = Some(frontier);
        self
    }

    /// Issued identity.
    pub const fn atomic_id(&self) -> AtomicId {
        self.atomic_id
    }

    /// Applied limits.
    pub const fn limits(&self) -> ResourceLimits {
        self.limits
    }

    /// Declared scope.
    pub const fn scope(&self) -> CoordinationScope {
        self.scope
    }
}

/// Typed compiler from one Heap's collection IDs to an immutable closed plan.
#[derive(Clone, Debug)]
pub struct AtomicBuilder {
    heap_id: HeapId,
    options: AtomicOptions,
    started: Instant,
    mutations: Vec<crate::plan::PlanMutation>,
    predicates: Vec<crate::plan::PlanPredicate>,
    reads: Vec<ReadWitness>,
    required_rights: CollectionRights,
    bound_authority: Option<[u8; 32]>,
    rule_revisions: Vec<[u8; 32]>,
}

impl AtomicBuilder {
    /// Bind the builder to one Heap. Cross-Heap collection handles are refused.
    pub fn new(heap_id: HeapId, options: AtomicOptions) -> Result<Self, AtomicsError> {
        refuse_if_past_deadline(options.deadline, options.limits, Instant::now())?;
        if !AtomicProfile::LocalHeapV1.execution_supports_scope(options.scope) {
            return Err(AtomicsError::Refused(AtomicRefuseReason::ScopeUnavailable));
        }
        if !options.limits.is_within(options.scope.hard_limits()) {
            return Err(AtomicsError::Refused(AtomicRefuseReason::LimitExceeded));
        }
        Ok(Self {
            heap_id,
            options,
            started: Instant::now(),
            mutations: Vec::new(),
            predicates: Vec::new(),
            reads: Vec::new(),
            required_rights: CollectionRights::empty(),
            bound_authority: None,
            rule_revisions: Vec::new(),
        })
    }

    /// Heap this builder is bound to.
    pub const fn heap_id(&self) -> HeapId {
        self.heap_id
    }

    /// Union of rights required by operations admitted so far.
    pub const fn required_rights(&self) -> CollectionRights {
        self.required_rights
    }

    /// Create-if-absent.
    pub fn create(
        &mut self,
        collection: &BoundCollection,
        key: CanonicalKey,
        value: CanonicalValue,
    ) -> Result<&mut Self, AtomicsError> {
        let id = self.admit(
            collection,
            CollectionRights::CREATE,
            &key,
            Some(&value),
            true,
        )?;
        self.mutations.push(encode_create(id, key, value));
        Ok(self)
    }

    /// Explicit blind upsert (`put_unconditional` on the SDK).
    pub fn put_unconditional(
        &mut self,
        collection: &BoundCollection,
        key: CanonicalKey,
        value: CanonicalValue,
    ) -> Result<&mut Self, AtomicsError> {
        let id = self.admit(collection, CollectionRights::PUT, &key, Some(&value), true)?;
        self.mutations.push(encode_put(id, key, value));
        Ok(self)
    }

    /// Version-replace.
    pub fn replace(
        &mut self,
        collection: &BoundCollection,
        key: CanonicalKey,
        if_version: VersionId,
        value: CanonicalValue,
    ) -> Result<&mut Self, AtomicsError> {
        let id = self.admit(
            collection,
            CollectionRights::REPLACE,
            &key,
            Some(&value),
            true,
        )?;
        self.mutations
            .push(encode_replace(id, key, if_version, value));
        Ok(self)
    }

    /// Version-delete.
    pub fn delete(
        &mut self,
        collection: &BoundCollection,
        key: CanonicalKey,
        if_version: VersionId,
    ) -> Result<&mut Self, AtomicsError> {
        let id = self.admit(collection, CollectionRights::DELETE, &key, None, true)?;
        self.mutations.push(encode_delete(id, key, if_version));
        Ok(self)
    }

    /// `assert_absent`.
    pub fn assert_absent(
        &mut self,
        collection: &BoundCollection,
        key: CanonicalKey,
    ) -> Result<&mut Self, AtomicsError> {
        let id = self.admit(collection, CollectionRights::READ, &key, None, false)?;
        self.predicates.push(encode_assert_absent(id, key));
        Ok(self)
    }

    /// `assert_present`.
    pub fn assert_present(
        &mut self,
        collection: &BoundCollection,
        key: CanonicalKey,
    ) -> Result<&mut Self, AtomicsError> {
        let id = self.admit(collection, CollectionRights::READ, &key, None, false)?;
        self.predicates.push(encode_assert_present(id, key));
        Ok(self)
    }

    /// `assert_version`.
    pub fn assert_version(
        &mut self,
        collection: &BoundCollection,
        key: CanonicalKey,
        version: VersionId,
    ) -> Result<&mut Self, AtomicsError> {
        let id = self.admit(collection, CollectionRights::READ, &key, None, false)?;
        self.predicates
            .push(encode_assert_version(id, key, version));
        Ok(self)
    }

    /// Record an exact-version prior read (`ATOMICS_SPEC` §7).
    pub fn witness_version(
        &mut self,
        collection: &BoundCollection,
        key: CanonicalKey,
        version: VersionId,
        projection_hash: [u8; 32],
    ) -> Result<&mut Self, AtomicsError> {
        let id = self.admit(collection, CollectionRights::READ, &key, None, false)?;
        self.reads.push(ReadWitness {
            collection_id: id,
            key,
            observed_version: Some(version),
            projection_hash,
        });
        Ok(self)
    }

    /// Record an absence prior read (`ATOMICS_SPEC` §7).
    pub fn witness_absent(
        &mut self,
        collection: &BoundCollection,
        key: CanonicalKey,
        projection_hash: [u8; 32],
    ) -> Result<&mut Self, AtomicsError> {
        let id = self.admit(collection, CollectionRights::READ, &key, None, false)?;
        self.reads.push(ReadWitness {
            collection_id: id,
            key,
            observed_version: None,
            projection_hash,
        });
        Ok(self)
    }

    /// Bind an active RRE rule revision hash. Not a Heap authority revision.
    pub fn bind_rule_revision(&mut self, revision: [u8; 32]) -> &mut Self {
        if !self.rule_revisions.contains(&revision) {
            self.rule_revisions.push(revision);
        }
        self
    }

    /// Close, cost, and structurally validate. Refusals append no evidence.
    pub fn build(self) -> Result<AtomicPlan, AtomicsError> {
        refuse_if_past_deadline(self.options.deadline, self.options.limits, self.started)?;
        let mut predicates = self.predicates;
        if let Some(rev) = self.bound_authority {
            predicates.push(encode_heap_authority_revision(rev));
        }
        let plan = AtomicPlan::close(AtomicPlanParts {
            profile: AtomicProfile::LocalHeapV1,
            atomic_id: self.options.atomic_id,
            heap_id: self.heap_id,
            scope: self.options.scope,
            read_frontier: self.options.read_frontier,
            reads: self.reads,
            predicates,
            mutations: self.mutations,
            active_rule_revisions: self.rule_revisions,
            limits: self.options.limits,
        })?;
        validate_closed_plan(&plan, self.heap_id)?;
        Ok(plan)
    }

    fn admit(
        &mut self,
        collection: &BoundCollection,
        need: CollectionRights,
        key: &CanonicalKey,
        value: Option<&CanonicalValue>,
        as_mutation: bool,
    ) -> Result<CollectionId, AtomicsError> {
        refuse_if_past_deadline(self.options.deadline, self.options.limits, self.started)?;
        if collection.heap_id != self.heap_id {
            return Err(AtomicsError::Refused(
                AtomicRefuseReason::CrossHeapCollection,
            ));
        }
        if !collection.rights.contains(need) {
            return Err(AtomicsError::Refused(
                AtomicRefuseReason::AuthorizationFailure,
            ));
        }
        collection.encoding.admit_key(key)?;
        if let Some(value) = value {
            collection.encoding.admit_value_bytes(value.as_bytes())?;
        }
        match self.bound_authority {
            None => self.bound_authority = Some(collection.authority_revision),
            Some(rev) if rev != collection.authority_revision => {
                return Err(AtomicsError::Refused(
                    AtomicRefuseReason::StaleOrForeignCapability,
                ));
            }
            Some(_) => {}
        }
        if as_mutation {
            let kb = key_order_bytes(key);
            if self.mutations.iter().any(|m| {
                m.collection_id == collection.collection_id && key_order_bytes(&m.key) == kb
            }) {
                return Err(AtomicsError::Refused(AtomicRefuseReason::DuplicateTarget));
            }
        }
        self.required_rights = self.required_rights.union(need);
        Ok(collection.collection_id)
    }
}

/// Revalidate a closed plan against trusted current authority and grants.
///
/// Refuses with no prepare/evidence. Call before durable acceptance.
pub fn admit_closed_plan(
    plan: &AtomicPlan,
    trusted: &TrustedAuthorityView,
) -> Result<(), AtomicsError> {
    validate_closed_plan(plan, trusted.heap_id)?;
    let bound = extract_heap_authority(plan)?;
    if bound != trusted.revision {
        return Err(AtomicsError::Refused(
            AtomicRefuseReason::StaleOrForeignCapability,
        ));
    }
    for mutation in plan.mutations() {
        require_grant(
            trusted,
            mutation.collection_id,
            right_for_mutation(mutation),
        )?;
        require_encoding(
            trusted,
            mutation.collection_id,
            &mutation.key,
            mutation.encoded_value.as_deref(),
        )?;
    }
    for predicate in plan.predicates() {
        if let Some((cid, need)) = right_for_predicate(predicate) {
            require_grant(trusted, cid, need)?;
            if let Some(key) = &predicate.key {
                require_encoding(trusted, cid, key, None)?;
            }
        }
    }
    for read in plan.reads() {
        require_grant(trusted, read.collection_id, CollectionRights::READ)?;
        require_encoding(trusted, read.collection_id, &read.key, None)?;
    }
    Ok(())
}

fn extract_heap_authority(plan: &AtomicPlan) -> Result<[u8; 32], AtomicsError> {
    let mut found = None;
    for p in plan.predicates() {
        if p.kind != PredicateKind::HeapAuthorityRevision {
            continue;
        }
        let bytes = p
            .encoded
            .as_ref()
            .ok_or(AtomicsError::Refused(AtomicRefuseReason::MalformedInput))?;
        let rev: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| AtomicsError::Refused(AtomicRefuseReason::MalformedInput))?;
        if found.is_some() {
            return Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput));
        }
        found = Some(rev);
    }
    found.ok_or(AtomicsError::Refused(
        AtomicRefuseReason::StaleOrForeignCapability,
    ))
}

fn right_for_mutation(mutation: &PlanMutation) -> CollectionRights {
    match mutation.kind {
        MutationKind::Create => CollectionRights::CREATE,
        MutationKind::Put => CollectionRights::PUT,
        MutationKind::Replace => CollectionRights::REPLACE,
        MutationKind::Delete => CollectionRights::DELETE,
    }
}

fn right_for_predicate(predicate: &PlanPredicate) -> Option<(CollectionId, CollectionRights)> {
    if !predicate.kind.is_public_builder_assert() {
        return None;
    }
    predicate
        .collection_id
        .map(|cid| (cid, CollectionRights::READ))
}

fn require_grant(
    trusted: &TrustedAuthorityView,
    collection_id: CollectionId,
    need: CollectionRights,
) -> Result<(), AtomicsError> {
    if !trusted.granted(collection_id).contains(need) {
        return Err(AtomicsError::Refused(
            AtomicRefuseReason::AuthorizationFailure,
        ));
    }
    Ok(())
}

fn require_encoding(
    trusted: &TrustedAuthorityView,
    collection_id: CollectionId,
    key: &CanonicalKey,
    value: Option<&[u8]>,
) -> Result<(), AtomicsError> {
    let encoding = trusted
        .encoding(collection_id)
        .ok_or(AtomicsError::Refused(
            AtomicRefuseReason::AuthorizationFailure,
        ))?;
    encoding.admit_key(key)?;
    if let Some(bytes) = value {
        encoding.admit_value_bytes(bytes)?;
    }
    Ok(())
}

fn refuse_if_past_deadline(
    deadline: Option<Instant>,
    limits: ResourceLimits,
    started: Instant,
) -> Result<(), AtomicsError> {
    let limit = deadline.unwrap_or_else(|| started + limits.construction_deadline);
    if Instant::now() >= limit {
        Err(AtomicsError::Refused(AtomicRefuseReason::Deadline))
    } else {
        Ok(())
    }
}