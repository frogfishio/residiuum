//! Async-driver bridge for Heap-local multi-record Atomics.

use super::{
    encode_driver_json, mint_request_id, Collection, Error, ErrorClass, ErrorCode, HeapClient,
};
use residiuum_atomics::{
    BoundCollection, CanonicalKey, CanonicalValue, CollectionId, ConstructionRead, EncodingProfile,
    VersionId,
};
use serde::{de::DeserializeOwned, Serialize};

pub use residiuum_atomics::{
    AtomicAbortReason, AtomicId, AtomicMemberReceipt, AtomicOptions, AtomicOutcome, AtomicPlan,
    AtomicReceipt, AtomicResolutionHandle, AtomicStatus, ResourceLimits,
};

/// Value observed during Atomic construction.
#[derive(Debug, Clone, PartialEq)]
pub enum AtomicRead<T> {
    /// Present value. Planned values have no establishing version until commit.
    Present {
        /// Decoded value.
        value: T,
        /// Store version for an external observation; `None` for plan overlay.
        version: Option<[u8; 16]>,
    },
    /// Absent externally or because of an earlier planned delete.
    Absent,
}

/// Heap-bound construction session holding no store lock between calls.
pub struct AtomicBuilder {
    heap: HeapClient,
    inner: residiuum_atomics::AtomicBuilder,
}

impl AtomicBuilder {
    pub(super) fn new(heap: HeapClient, options: AtomicOptions) -> Result<Self, Error> {
        let heap_id = residiuum_atomics::HeapId::from_bytes(*heap.heap_id().as_bytes())
            .map_err(atomics_error)?;
        let inner =
            residiuum_atomics::AtomicBuilder::new(heap_id, options).map_err(atomics_error)?;
        Ok(Self { heap, inner })
    }

    fn bound<T>(&self, collection: &Collection<T>) -> Result<BoundCollection, Error> {
        if !self
            .heap
            .heap
            .capability()
            .same_instance(collection.inner.capability())
        {
            return Err(Error::local(
                ErrorCode::PermissionDenied,
                ErrorClass::Request,
                "Atomic collection belongs to a foreign capability instance",
            ));
        }
        let capability = self.heap.heap.capability();
        let slot = std::sync::Arc::clone(capability.slot());
        let guard = slot.lock_authority_frontier().map_err(|_| {
            Error::local(
                ErrorCode::PermissionDenied,
                ErrorClass::Service,
                "Heap authority frontier lock poisoned",
            )
        })?;
        residiuum_heap::refresh_capability_or_terminate(capability).map_err(|error| {
            Error::local(
                ErrorCode::PermissionDenied,
                ErrorClass::Service,
                error.to_string(),
            )
        })?;
        let mut rights = residiuum_atomics::CollectionRights::empty();
        if capability.rights().contains(residiuum_heap::Rights::READ) {
            rights = rights.union(residiuum_atomics::CollectionRights::READ);
        }
        if capability.rights().contains(residiuum_heap::Rights::WRITE) {
            rights = rights.union(residiuum_atomics::CollectionRights::CREATE);
            rights = rights.union(residiuum_atomics::CollectionRights::PUT);
            rights = rights.union(residiuum_atomics::CollectionRights::REPLACE);
            rights = rights.union(residiuum_atomics::CollectionRights::DELETE);
        }
        let heap_id = residiuum_atomics::HeapId::from_bytes(*capability.heap_id().as_bytes())
            .map_err(atomics_error)?;
        let collection_id =
            CollectionId::from_bytes(*collection.inner.id().as_bytes()).map_err(atomics_error)?;
        let revision = guard.load().atomic_authority_revision();
        // SAFETY: every field comes from this live HeapCap while its authority
        // frontier remains guarded; none is accepted from the application.
        unsafe {
            BoundCollection::from_verified_authority_parts(
                heap_id,
                collection_id,
                rights,
                revision,
                EncodingProfile::STRING_BYTES,
            )
        }
        .map_err(atomics_error)
    }

    /// Insert only when the key is absent.
    pub fn create<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
        value: &T,
    ) -> Result<&mut Self, Error>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let bound = self.bound(collection)?;
        self.inner
            .create(
                &bound,
                CanonicalKey::string(key.into()),
                CanonicalValue::from_bytes(&encode_driver_json(value)?),
            )
            .map_err(atomics_error)?;
        Ok(self)
    }

    /// Explicit unconditional upsert.
    pub fn put_unconditional<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
        value: &T,
    ) -> Result<&mut Self, Error>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let bound = self.bound(collection)?;
        self.inner
            .put_unconditional(
                &bound,
                CanonicalKey::string(key.into()),
                CanonicalValue::from_bytes(&encode_driver_json(value)?),
            )
            .map_err(atomics_error)?;
        Ok(self)
    }

    /// Replace only when the establishing version is still live.
    pub fn replace<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
        if_version: [u8; 16],
        value: &T,
    ) -> Result<&mut Self, Error>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let bound = self.bound(collection)?;
        self.inner
            .replace(
                &bound,
                CanonicalKey::string(key.into()),
                VersionId::from_bytes(if_version).map_err(atomics_error)?,
                CanonicalValue::from_bytes(&encode_driver_json(value)?),
            )
            .map_err(atomics_error)?;
        Ok(self)
    }

    /// Delete only when the establishing version is still live.
    pub fn delete<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
        if_version: [u8; 16],
    ) -> Result<&mut Self, Error> {
        let bound = self.bound(collection)?;
        self.inner
            .delete(
                &bound,
                CanonicalKey::string(key.into()),
                VersionId::from_bytes(if_version).map_err(atomics_error)?,
            )
            .map_err(atomics_error)?;
        Ok(self)
    }

    /// Require absence at the serialization frontier.
    pub fn assert_absent<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
    ) -> Result<&mut Self, Error> {
        let bound = self.bound(collection)?;
        self.inner
            .assert_absent(&bound, CanonicalKey::string(key.into()))
            .map_err(atomics_error)?;
        Ok(self)
    }

    /// Require presence at the serialization frontier.
    pub fn assert_present<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
    ) -> Result<&mut Self, Error> {
        let bound = self.bound(collection)?;
        self.inner
            .assert_present(&bound, CanonicalKey::string(key.into()))
            .map_err(atomics_error)?;
        Ok(self)
    }

    /// Require an exact establishing version.
    pub fn assert_version<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
        version: [u8; 16],
    ) -> Result<&mut Self, Error> {
        let bound = self.bound(collection)?;
        self.inner
            .assert_version(
                &bound,
                CanonicalKey::string(key.into()),
                VersionId::from_bytes(version).map_err(atomics_error)?,
            )
            .map_err(atomics_error)?;
        Ok(self)
    }

    /// Read through the plan overlay, or perform one bounded external read and
    /// record its exact version/absence witness.
    pub async fn read<T>(
        &mut self,
        collection: &Collection<T>,
        key: impl Into<String>,
    ) -> Result<AtomicRead<T>, Error>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let key = key.into();
        let bound = self.bound(collection)?;
        match self
            .inner
            .read_your_plan(&bound, &CanonicalKey::string(key.clone()))
            .map_err(atomics_error)?
        {
            ConstructionRead::Present { encoded_value, .. } => Ok(AtomicRead::Present {
                value: decode_value(&encoded_value)?,
                version: None,
            }),
            ConstructionRead::Absent { .. } => Ok(AtomicRead::Absent),
            ConstructionRead::External => {
                let inner = collection.inner.clone();
                let heap = std::sync::Arc::clone(&self.heap.heap);
                let read_key = key.clone();
                let request_id = mint_request_id()?;
                let (observed, frontier) = self
                    .heap
                    .connection
                    .scheduler
                    .dispatch(request_id, None, None, move || {
                        Ok((
                            inner.get_versioned_raw_body(&read_key)?,
                            heap.segment_fingerprint()?,
                        ))
                    })
                    .await?;
                self.inner
                    .bind_read_frontier(frontier)
                    .map_err(atomics_error)?;
                match observed {
                    None => {
                        self.inner
                            .witness_absent(
                                &bound,
                                CanonicalKey::string(key.clone()),
                                absent_hash(collection.inner.id(), key.as_bytes()),
                            )
                            .map_err(atomics_error)?;
                        Ok(AtomicRead::Absent)
                    }
                    Some((body, version)) => {
                        self.inner
                            .witness_version(
                                &bound,
                                CanonicalKey::string(key),
                                VersionId::from_bytes(version).map_err(atomics_error)?,
                                *blake3::hash(&body).as_bytes(),
                            )
                            .map_err(atomics_error)?;
                        Ok(AtomicRead::Present {
                            value: decode_value(&body)?,
                            version: Some(version),
                        })
                    }
                }
            }
        }
    }

    /// Close into one immutable canonical plan.
    pub fn build(self) -> Result<AtomicPlan, Error> {
        self.inner.build().map_err(atomics_error)
    }
}

pub(super) async fn commit(heap: &HeapClient, plan: AtomicPlan) -> Result<AtomicOutcome, Error> {
    let expected =
        residiuum_atomics::HeapId::from_bytes(*heap.heap_id().as_bytes()).map_err(atomics_error)?;
    if plan.heap_id() != expected {
        return Err(Error::local(
            ErrorCode::PermissionDenied,
            ErrorClass::Request,
            "Atomic plan belongs to a foreign Heap",
        ));
    }
    let bytes = residiuum_atomics::encode_canonical_plan(&plan)
        .map_err(atomics_error)?
        .len();
    let bound = std::sync::Arc::clone(&heap.heap);
    let request_id = mint_request_id()?;
    heap.connection
        .scheduler
        .dispatch_weighted(request_id, None, bytes, move || {
            bound.decide_atomic_plan_outcome(&plan)
        })
        .await
}

pub(super) async fn status(heap: &HeapClient, atomic_id: AtomicId) -> Result<AtomicStatus, Error> {
    let bound = std::sync::Arc::clone(&heap.heap);
    let request_id = mint_request_id()?;
    heap.connection
        .scheduler
        .dispatch(request_id, None, None, move || {
            bound.atomic_status(atomic_id)
        })
        .await
}

fn decode_value<T: DeserializeOwned>(body: &[u8]) -> Result<T, Error> {
    let json = crate::value::decode_json(body).map_err(|error| {
        Error::local(
            ErrorCode::DataDamaged,
            ErrorClass::Service,
            error.to_string(),
        )
    })?;
    serde_json::from_value(json).map_err(|error| {
        Error::local(
            ErrorCode::DataDamaged,
            ErrorClass::Service,
            error.to_string(),
        )
    })
}

fn absent_hash(collection: residiuum_heap::CollectionId, key: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"RESIDIUUM-ATOMIC-ABSENT-PROJECTION-V1");
    hash.update(collection.as_bytes());
    hash.update(key);
    *hash.finalize().as_bytes()
}

fn atomics_error(error: residiuum_atomics::AtomicsError) -> Error {
    use residiuum_atomics::AtomicRefuseReason;
    let code = match &error {
        residiuum_atomics::AtomicsError::Refused(
            AtomicRefuseReason::AuthorizationFailure
            | AtomicRefuseReason::CrossHeapCollection
            | AtomicRefuseReason::StaleOrForeignCapability,
        ) => ErrorCode::PermissionDenied,
        residiuum_atomics::AtomicsError::Refused(AtomicRefuseReason::LimitExceeded) => {
            ErrorCode::ResourceLimit
        }
        _ => ErrorCode::Validation,
    };
    Error::local(code, ErrorClass::Request, error.to_string())
}
