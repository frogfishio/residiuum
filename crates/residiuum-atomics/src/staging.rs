//! Per-Heap coordinator, placement manifest, and staged append (`ATOMICS_SPEC` §9).
//!
//! ATM-2.2. Pure in-memory kernel: no file, store, SDK, or index publication.
//! Staged members never enter the ordinary primary map. RQL, history, watch,
//! and secondary indexes MUST read only that ordinary map when a store adapter
//! is wired later. Chunking, failpoints, and crash matrices are later cards.

use crate::canonical::key_order_bytes;
use crate::error::AtomicsError;
use crate::evidence::{
    AtomicLifecycle, AtomicMember, DecisionPhase, MemberPhase, PreparePhase, PublicationPhase,
};
use crate::id::{AtomicId, CollectionId, ContentRoot, HeapId, VersionId};
use crate::outcome::AtomicRefuseReason;
use crate::plan::CanonicalKey;
use std::collections::BTreeMap;

/// Writer-shard identity in a placement manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardId(u32);

impl ShardId {
    /// Construct a shard identity.
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Raw shard number.
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

/// Monotonic coordinator sequence. Never zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CoordinatorSeq(u64);

impl CoordinatorSeq {
    /// Raw sequence.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// One coordinator-stream record (prepare only; decision is ATM-3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatorRecord {
    /// Allocated sequence.
    pub seq: CoordinatorSeq,
    /// Atomic identity.
    pub atomic_id: AtomicId,
    /// Plan content root named by the prepare.
    pub content_root: ContentRoot,
}

/// Per-Heap coordinator stream and allocator.
#[derive(Clone, Debug)]
pub struct CoordinatorStream {
    heap_id: HeapId,
    next: u64,
    records: Vec<CoordinatorRecord>,
}

impl CoordinatorStream {
    fn new(heap_id: HeapId) -> Self {
        Self {
            heap_id,
            next: 1,
            records: Vec::new(),
        }
    }

    /// Heap this stream belongs to.
    pub const fn heap_id(&self) -> HeapId {
        self.heap_id
    }

    /// Append-only coordinator records.
    pub fn records(&self) -> &[CoordinatorRecord] {
        &self.records
    }

    fn allocate(
        &mut self,
        atomic_id: AtomicId,
        content_root: ContentRoot,
    ) -> Result<CoordinatorSeq, AtomicsError> {
        if self.records.iter().any(|r| r.atomic_id == atomic_id) {
            return Err(AtomicsError::Refused(AtomicRefuseReason::AtomicIdConflict));
        }
        let seq = CoordinatorSeq(self.next);
        self.next += 1;
        self.records.push(CoordinatorRecord {
            seq,
            atomic_id,
            content_root,
        });
        Ok(seq)
    }
}

/// One intended member placement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementEntry {
    /// Manifest ordinal.
    pub ordinal: u32,
    /// Writer shard that must hold the member.
    pub shard: ShardId,
    /// Target collection.
    pub collection_id: CollectionId,
    /// Target key order bytes.
    pub key_bytes: Vec<u8>,
    /// Payload hash committed by the prepare (member `after_content_hash` or zeros on delete).
    pub payload_hash: [u8; 32],
}

/// Member placement across writer shards. Frozen before any staged append.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementManifest {
    heap_id: HeapId,
    atomic_id: AtomicId,
    content_root: ContentRoot,
    entries: Vec<PlacementEntry>,
}

impl PlacementManifest {
    /// Assign shards by `ordinal % shard_count`.
    pub fn assign(
        heap_id: HeapId,
        atomic_id: AtomicId,
        content_root: ContentRoot,
        shard_count: u32,
        members: &[AtomicMember],
    ) -> Result<Self, AtomicsError> {
        if shard_count == 0 {
            return Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput));
        }
        let mut entries = Vec::with_capacity(members.len());
        for m in members {
            if m.atomic_id != atomic_id {
                return Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput));
            }
            m.validate()?;
            entries.push(PlacementEntry {
                ordinal: m.ordinal,
                shard: ShardId(m.ordinal % shard_count),
                collection_id: m.object_identity.collection_id,
                key_bytes: key_order_bytes(&m.object_identity.key),
                payload_hash: m.after_content_hash.unwrap_or([0u8; 32]),
            });
        }
        entries.sort_by_key(|e| e.ordinal);
        Ok(Self {
            heap_id,
            atomic_id,
            content_root,
            entries,
        })
    }

    /// Heap this manifest is bound to.
    pub const fn heap_id(&self) -> HeapId {
        self.heap_id
    }

    /// Atomic identity.
    pub const fn atomic_id(&self) -> AtomicId {
        self.atomic_id
    }

    /// Plan content root.
    pub const fn content_root(&self) -> ContentRoot {
        self.content_root
    }

    /// Ordered placement entries.
    pub fn entries(&self) -> &[PlacementEntry] {
        &self.entries
    }
}

/// Ordinary published cell. Never holds staged material.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrdinaryCell {
    /// Visible version.
    pub version: VersionId,
    /// Visible value bytes.
    pub value: Vec<u8>,
}

/// Staged member plus the shard that holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedMember {
    /// Authoritative member record.
    pub member: AtomicMember,
    /// Canonical payload. Empty on delete.
    pub payload: Vec<u8>,
    /// Shard named by the placement manifest.
    pub shard: ShardId,
}

#[derive(Clone, Debug)]
struct StagedAtomic {
    seq: CoordinatorSeq,
    manifest: PlacementManifest,
    members: Vec<StagedMember>,
    lifecycle: AtomicLifecycle,
}

/// One Heap: coordinator stream, ordinary primary map, separate staged lane.
#[derive(Clone, Debug)]
pub struct StagingHeap {
    heap_id: HeapId,
    shard_count: u32,
    coordinator: CoordinatorStream,
    ordinary: BTreeMap<(CollectionId, Vec<u8>), (CanonicalKey, OrdinaryCell)>,
    staged: BTreeMap<AtomicId, StagedAtomic>,
}

impl StagingHeap {
    /// Bind a Heap and its writer-shard count.
    pub fn new(heap_id: HeapId, shard_count: u32) -> Result<Self, AtomicsError> {
        if shard_count == 0 {
            return Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput));
        }
        Ok(Self {
            heap_id,
            shard_count,
            coordinator: CoordinatorStream::new(heap_id),
            ordinary: BTreeMap::new(),
            staged: BTreeMap::new(),
        })
    }

    /// Heap this kernel models.
    pub const fn heap_id(&self) -> HeapId {
        self.heap_id
    }

    /// Coordinator stream.
    pub const fn coordinator(&self) -> &CoordinatorStream {
        &self.coordinator
    }

    /// Ordinary primary get. Staged members are never returned.
    pub fn get(&self, collection: CollectionId, key: &CanonicalKey) -> Option<&OrdinaryCell> {
        self.ordinary
            .get(&(collection, key_order_bytes(key)))
            .map(|(_, cell)| cell)
    }

    /// Ordinary primary scan. Staged members are never yielded.
    pub fn scan(&self) -> impl Iterator<Item = (CollectionId, &CanonicalKey, &OrdinaryCell)> {
        self.ordinary
            .iter()
            .map(|((cid, _), (key, cell))| (*cid, key, cell))
    }

    /// Seed or overwrite the ordinary primary map (not a staged append).
    pub fn publish_ordinary(
        &mut self,
        collection: CollectionId,
        key: CanonicalKey,
        cell: OrdinaryCell,
    ) {
        self.ordinary
            .insert((collection, key_order_bytes(&key)), (key, cell));
    }

    /// Append prepare to the coordinator and freeze placement. No ordinary update.
    pub fn begin_prepare(
        &mut self,
        atomic_id: AtomicId,
        content_root: ContentRoot,
        members: &[AtomicMember],
    ) -> Result<(CoordinatorSeq, PlacementManifest), AtomicsError> {
        let manifest = PlacementManifest::assign(
            self.heap_id,
            atomic_id,
            content_root,
            self.shard_count,
            members,
        )?;
        let seq = self.coordinator.allocate(atomic_id, content_root)?;
        self.staged.insert(
            atomic_id,
            StagedAtomic {
                seq,
                manifest: manifest.clone(),
                members: Vec::new(),
                lifecycle: AtomicLifecycle {
                    prepare: PreparePhase::Prepared,
                    members: MemberPhase::Absent,
                    decision: DecisionPhase::None,
                    publication: PublicationPhase::Unpublished,
                },
            },
        );
        Ok((seq, manifest))
    }

    /// Append a staged member. Does not update the ordinary primary map.
    pub fn append_staged(
        &mut self,
        member: AtomicMember,
        payload: Vec<u8>,
    ) -> Result<(), AtomicsError> {
        member.validate()?;
        let slot = self
            .staged
            .get_mut(&member.atomic_id)
            .ok_or(AtomicsError::Refused(AtomicRefuseReason::MalformedInput))?;
        let entry = slot
            .manifest
            .entries
            .iter()
            .find(|e| e.ordinal == member.ordinal)
            .ok_or(AtomicsError::Refused(AtomicRefuseReason::MalformedInput))?;
        let key_bytes = key_order_bytes(&member.object_identity.key);
        if entry.collection_id != member.object_identity.collection_id
            || entry.key_bytes != key_bytes
        {
            return Err(AtomicsError::Refused(AtomicRefuseReason::MalformedInput));
        }
        if slot
            .members
            .iter()
            .any(|s| s.member.ordinal == member.ordinal)
        {
            return Err(AtomicsError::Refused(AtomicRefuseReason::DuplicateTarget));
        }
        slot.members.push(StagedMember {
            shard: entry.shard,
            member,
            payload,
        });
        slot.lifecycle.members = MemberPhase::Staged;
        Ok(())
    }

    /// Examination-only staged lane. Not an ordinary read.
    pub fn inspect_staged(&self, atomic_id: AtomicId) -> Option<&[StagedMember]> {
        self.staged.get(&atomic_id).map(|s| s.members.as_slice())
    }

    /// Formal lifecycle for an Atomic, if prepare was accepted.
    pub fn lifecycle(&self, atomic_id: AtomicId) -> Option<AtomicLifecycle> {
        self.staged.get(&atomic_id).map(|s| s.lifecycle)
    }

    /// Placement frozen at prepare.
    pub fn placement(&self, atomic_id: AtomicId) -> Option<&PlacementManifest> {
        self.staged.get(&atomic_id).map(|s| &s.manifest)
    }

    /// Coordinator sequence allocated for this prepare.
    pub fn prepare_seq(&self, atomic_id: AtomicId) -> Option<CoordinatorSeq> {
        self.staged.get(&atomic_id).map(|s| s.seq)
    }
}
