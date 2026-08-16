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
    encode_delete, encode_put, encode_replace, CanonicalValue,
};
use crate::error::AtomicsError;
use crate::id::{AtomicId, CollectionId, HeapId, VersionId};
use crate::limits::ResourceLimits;
use crate::outcome::AtomicRefuseReason;
use crate::plan::{
    AtomicPlan, AtomicPlanParts, AtomicProfile, CanonicalKey, CoordinationScope, ReadWitness,
};
use crate::validate::validate_closed_plan;
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

/// Heap-bound collection identity presented to the builder.
///
/// The SDK constructs this from one `HeapClient`. A handle from another Heap
/// is refused as [`AtomicRefuseReason::CrossHeapCollection`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundCollection {
    heap_id: HeapId,
    collection_id: CollectionId,
    rights: CollectionRights,
    authority_revision: [u8; 32],
}

impl BoundCollection {
    /// Bind a collection ID to one Heap, its granted rights, and the authority
    /// revision observed when the handle was issued.
    pub fn bind(
        heap_id: HeapId,
        collection_id: CollectionId,
        rights: CollectionRights,
        authority_revision: [u8; 32],
    ) -> Self {
        Self {
            heap_id,
            collection_id,
            rights,
            authority_revision,
        }
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
    authority_revisions: Vec<[u8; 32]>,
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
            authority_revisions: Vec::new(),
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
        let id = self.admit(collection, CollectionRights::CREATE, Some(&key))?;
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
        let id = self.admit(collection, CollectionRights::PUT, Some(&key))?;
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
        let id = self.admit(collection, CollectionRights::REPLACE, Some(&key))?;
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
        let id = self.admit(collection, CollectionRights::DELETE, Some(&key))?;
        self.mutations.push(encode_delete(id, key, if_version));
        Ok(self)
    }

    /// `assert_absent`.
    pub fn assert_absent(
        &mut self,
        collection: &BoundCollection,
        key: CanonicalKey,
    ) -> Result<&mut Self, AtomicsError> {
        let id = self.admit(collection, CollectionRights::READ, None)?;
        self.predicates.push(encode_assert_absent(id, key));
        Ok(self)
    }

    /// `assert_present`.
    pub fn assert_present(
        &mut self,
        collection: &BoundCollection,
        key: CanonicalKey,
    ) -> Result<&mut Self, AtomicsError> {
        let id = self.admit(collection, CollectionRights::READ, None)?;
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
        let id = self.admit(collection, CollectionRights::READ, None)?;
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
        let id = self.admit(collection, CollectionRights::READ, None)?;
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
        let id = self.admit(collection, CollectionRights::READ, None)?;
        self.reads.push(ReadWitness {
            collection_id: id,
            key,
            observed_version: None,
            projection_hash,
        });
        Ok(self)
    }

    /// Close, cost, and structurally validate. Refusals append no evidence.
    pub fn build(self) -> Result<AtomicPlan, AtomicsError> {
        refuse_if_past_deadline(self.options.deadline, self.options.limits, self.started)?;
        let plan = AtomicPlan::close(AtomicPlanParts {
            profile: AtomicProfile::LocalHeapV1,
            atomic_id: self.options.atomic_id,
            heap_id: self.heap_id,
            scope: self.options.scope,
            read_frontier: self.options.read_frontier,
            reads: self.reads,
            predicates: self.predicates,
            mutations: self.mutations,
            active_rule_revisions: self.authority_revisions,
            limits: self.options.limits,
        })?;
        validate_closed_plan(&plan, self.heap_id)?;
        Ok(plan)
    }

    fn admit(
        &mut self,
        collection: &BoundCollection,
        need: CollectionRights,
        mutation_key: Option<&CanonicalKey>,
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
        match self.bound_authority {
            None => self.bound_authority = Some(collection.authority_revision),
            Some(rev) if rev != collection.authority_revision => {
                return Err(AtomicsError::Refused(
                    AtomicRefuseReason::StaleOrForeignCapability,
                ));
            }
            Some(_) => {}
        }
        if let Some(key) = mutation_key {
            let kb = key_order_bytes(key);
            if self.mutations.iter().any(|m| {
                m.collection_id == collection.collection_id && key_order_bytes(&m.key) == kb
            }) {
                return Err(AtomicsError::Refused(AtomicRefuseReason::DuplicateTarget));
            }
        }
        if !self
            .authority_revisions
            .contains(&collection.authority_revision)
        {
            self.authority_revisions.push(collection.authority_revision);
        }
        self.required_rights = self.required_rights.union(need);
        Ok(collection.collection_id)
    }
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
