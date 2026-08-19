//! Injected staging failpoints (`ATOMICS_SPEC` §9, plan §8).
//!
//! ATM-2.4. `before_prepare`, `after_prepare`, and `after_member_n` interrupt
//! the kernel. Reopen is a clone of the post-fault image: coordinator and
//! staged material that already applied survive for examination; the ordinary
//! map is unchanged. This is not a store I/O simulator.

use crate::error::AtomicsError;
use crate::evidence::AtomicMember;
use crate::id::{AtomicId, ContentRoot};
use crate::staging::{CoordinatorSeq, PlacementManifest, StagingHeap};

/// Named interrupt points on the prepare/member path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StagingFailpoint {
    /// Trip before the coordinator allocate. No prepare is recorded.
    BeforePrepare,
    /// Trip after prepare is recorded. Members are absent.
    AfterPrepare,
    /// Trip after staged member `n` has been appended.
    AfterMember(u32),
    /// Trip after chunk `index` of `ordinal` has been persisted.
    AfterChunk {
        /// Member ordinal whose chunk map this body belongs to.
        ordinal: u32,
        /// Chunk index inside that map.
        index: u32,
    },
}

/// Result of a failpoint-armed operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FaultError {
    /// The armed failpoint fired. Durable effects before the point remain.
    Injected(StagingFailpoint),
    /// The staging kernel refused or failed independently of the failpoint.
    Kernel(AtomicsError),
}

impl From<AtomicsError> for FaultError {
    fn from(err: AtomicsError) -> Self {
        Self::Kernel(err)
    }
}

/// Staging kernel with one optional armed failpoint.
#[derive(Clone, Debug)]
pub struct FaultSession {
    heap: StagingHeap,
    armed: Option<StagingFailpoint>,
}

impl FaultSession {
    /// Wrap a heap. No failpoint is armed.
    pub fn new(heap: StagingHeap) -> Self {
        Self { heap, armed: None }
    }

    /// Arm a single failpoint. Replaces any previous arm.
    pub fn arm(&mut self, point: StagingFailpoint) {
        self.armed = Some(point);
    }

    /// Disarm.
    pub fn disarm(&mut self) {
        self.armed = None;
    }

    /// Borrow the kernel (pre- or post-fault).
    pub const fn heap(&self) -> &StagingHeap {
        &self.heap
    }

    /// Recover the post-fault image. Ordinary cells are those from before staging.
    pub fn reopen(self) -> StagingHeap {
        self.heap
    }

    /// Prepare, honouring `BeforePrepare` / `AfterPrepare`.
    pub fn begin_prepare(
        &mut self,
        atomic_id: AtomicId,
        content_root: ContentRoot,
        members: &[AtomicMember],
    ) -> Result<(CoordinatorSeq, PlacementManifest), FaultError> {
        if self.armed == Some(StagingFailpoint::BeforePrepare) {
            return Err(FaultError::Injected(StagingFailpoint::BeforePrepare));
        }
        let out = self.heap.begin_prepare(atomic_id, content_root, members)?;
        if self.armed == Some(StagingFailpoint::AfterPrepare) {
            return Err(FaultError::Injected(StagingFailpoint::AfterPrepare));
        }
        Ok(out)
    }

    /// Staged append, honouring `AfterMember(n)` after a successful install.
    pub fn append_staged(
        &mut self,
        member: AtomicMember,
        payload: Vec<u8>,
    ) -> Result<(), FaultError> {
        let ordinal = member.ordinal;
        self.heap.append_staged(member, payload)?;
        if self.armed == Some(StagingFailpoint::AfterMember(ordinal)) {
            return Err(FaultError::Injected(StagingFailpoint::AfterMember(ordinal)));
        }
        Ok(())
    }
}
