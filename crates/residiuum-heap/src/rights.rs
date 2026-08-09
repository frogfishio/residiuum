//! Rights bitmap and operation registry (`HEAP_SPEC` §32).

use crate::error::{HeapError, HeapUnavailableCause};
use crate::generated_registry::{generated_op, GeneratedOp, GENERATED_OPS};

/// Private-field rights bitmap.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Rights(u64);

impl Rights {
    /// Empty rights (invalid on a certificate).
    pub const EMPTY: Self = Self(0);

    /// Named constants.
    pub const READ: Self = Self(crate::generated_registry::generated_rights::READ);
    /// History reads.
    pub const READ_HISTORY: Self = Self(crate::generated_registry::generated_rights::READHISTORY);
    /// Writes.
    pub const WRITE: Self = Self(crate::generated_registry::generated_rights::WRITE);
    /// Index administration.
    pub const INDEX_ADMIN: Self = Self(crate::generated_registry::generated_rights::INDEXADMIN);
    /// Export.
    pub const EXPORT: Self = Self(crate::generated_registry::generated_rights::EXPORT);
    /// Backup.
    pub const BACKUP: Self = Self(crate::generated_registry::generated_rights::BACKUP);
    /// Restore.
    pub const RESTORE: Self = Self(crate::generated_registry::generated_rights::RESTORE);
    /// Audit read.
    pub const AUDIT_READ: Self = Self(crate::generated_registry::generated_rights::AUDITREAD);
    /// Policy admin.
    pub const POLICY_ADMIN: Self = Self(crate::generated_registry::generated_rights::POLICYADMIN);
    /// Lifecycle admin.
    pub const LIFECYCLE_ADMIN: Self =
        Self(crate::generated_registry::generated_rights::LIFECYCLEADMIN);
    /// Hold admin.
    pub const HOLD_ADMIN: Self = Self(crate::generated_registry::generated_rights::HOLDADMIN);
    /// Recover.
    pub const RECOVER: Self = Self(crate::generated_registry::generated_rights::RECOVER);
    /// Heap admin.
    pub const HEAP_ADMIN: Self = Self(crate::generated_registry::generated_rights::HEAPADMIN);
    /// Retire.
    pub const RETIRE: Self = Self(crate::generated_registry::generated_rights::RETIRE);
    /// Purge.
    pub const PURGE: Self = Self(crate::generated_registry::generated_rights::PURGE);
    /// Data-key admin.
    pub const DATA_KEY_ADMIN: Self =
        Self(crate::generated_registry::generated_rights::DATAKEYADMIN);
    /// Placement admin.
    pub const PLACEMENT_ADMIN: Self =
        Self(crate::generated_registry::generated_rights::PLACEMENTADMIN);

    /// Mask of all defined v1 bits.
    pub const DEFINED_MASK: u64 = 0x1_ffff;

    /// Construct from bits; reserved bits and empty set are rejected for certificates.
    pub fn from_bits_certificate(bits: u64) -> Result<Self, HeapError> {
        if bits == 0 {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::MalformedOrBadSignature,
            ));
        }
        if bits & !Self::DEFINED_MASK != 0 {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::MalformedOrBadSignature,
            ));
        }
        Ok(Self(bits))
    }

    /// Construct an intersection / effective mask (may be empty).
    pub fn from_bits_effective(bits: u64) -> Result<Self, HeapError> {
        if bits & !Self::DEFINED_MASK != 0 {
            return Err(HeapError::unavailable(
                HeapUnavailableCause::MalformedOrBadSignature,
            ));
        }
        Ok(Self(bits))
    }

    /// Whether `self` contains all bits in `other`.
    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Bitwise intersection.
    pub fn intersection(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    /// Whether `self` is a subset of `other`.
    pub fn is_subset_of(self, other: Self) -> bool {
        self.0 & other.0 == self.0
    }

    /// Bits for audit (never used as authority alone).
    pub fn bits_for_audit(self) -> u64 {
        self.0
    }

    /// Raw bits.
    pub fn bits(self) -> u64 {
        self.0
    }
}

impl std::fmt::Debug for Rights {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Rights(0x{:x})", self.0)
    }
}

/// Registry status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationStatus {
    /// Dispatchable.
    Active,
    /// Allocated but not callable.
    Reserved,
}

/// Wire operation identity (`#[repr(u16)]` numeric IDs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum Operation {
    /// Process ping.
    Ping = 1,
    /// Liveness.
    HealthLive = 2,
    /// Readiness.
    HealthReady = 3,
    /// Catch-all for allocated-but-not-enum'd IDs via [`Operation::from_u16`].
    Other = 0,
}

impl Operation {
    /// Lookup registry row.
    pub fn lookup(id: u16) -> Result<&'static GeneratedOp, HeapError> {
        generated_op(id)
            .ok_or_else(|| HeapError::unavailable(HeapUnavailableCause::UnknownOperation))
    }

    /// Status for an ID.
    pub fn status(id: u16) -> Result<OperationStatus, HeapError> {
        let row = Self::lookup(id)?;
        Ok(match row.status {
            "active" => OperationStatus::Active,
            "reserved" => OperationStatus::Reserved,
            _ => {
                return Err(HeapError::unavailable(
                    HeapUnavailableCause::UnknownOperation,
                ))
            }
        })
    }

    /// Whether the operation may be dispatched (active only).
    pub fn is_callable(id: u16) -> bool {
        matches!(Self::status(id), Ok(OperationStatus::Active))
    }

    /// Required rights mask (0 for public process ops).
    pub fn required_rights(id: u16) -> Result<Rights, HeapError> {
        let row = Self::lookup(id)?;
        Rights::from_bits_effective(row.rights_mask)
    }

    /// Iterate the frozen registry.
    pub fn all() -> &'static [GeneratedOp] {
        GENERATED_OPS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn only_1_2_3_active() {
        let active: Vec<_> = Operation::all()
            .iter()
            .filter(|o| o.status == "active")
            .map(|o| o.id)
            .collect();
        assert!(active.contains(&1) && active.contains(&2) && active.contains(&3));
        // §32.4 data cuts (open/list + get/put/delete + keys/scan/find/history + indexes).
        for id in [
            105u16, 110, 111, 112, 114, 115, 116, 117, 120, 121, 122, 130, 131, 132, 133,
        ] {
            assert!(active.contains(&id), "missing active data op {id}");
        }
    }

    #[test]
    fn reserved_not_callable() {
        assert!(Operation::is_callable(1));
        assert!(Operation::is_callable(111)); // §32.4 data cut
        assert!(Operation::is_callable(116)); // find
        assert!(Operation::is_callable(117)); // history
        assert!(Operation::is_callable(130)); // index_list
        assert!(Operation::is_callable(131)); // index_create
        assert!(!Operation::is_callable(140)); // export still reserved
    }

    proptest! {
        #[test]
        fn intersection_commutative(a in 0u64..0x2_0000, b in 0u64..0x2_0000) {
            let a = a & Rights::DEFINED_MASK;
            let b = b & Rights::DEFINED_MASK;
            let ra = Rights::from_bits_effective(a).unwrap();
            let rb = Rights::from_bits_effective(b).unwrap();
            assert_eq!(ra.intersection(rb), rb.intersection(ra));
            assert!(ra.intersection(rb).is_subset_of(ra));
        }
    }
}
