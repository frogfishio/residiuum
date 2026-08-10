//! Comparison lanes — frozen by Q0 (`RQL_Q0_LANES_EXCLUSIONS.md`).
//!
//! Never score embedded Residiuum against MongoDB TCP as one contest.

use serde::{Deserialize, Serialize};

/// Gate-1 competitive lane id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaneId {
    /// Lane E — Residiuum embedded vs Couchbase Lite embedded.
    Embedded,
    /// Lane S — Residiuum server (loopback) vs local MongoDB.
    LocalClientServer,
}

impl LaneId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Embedded => "embedded",
            Self::LocalClientServer => "local_client_server",
        }
    }
}

/// Engine under test or comparator.
///
/// **F5:** [`LogicalHarness`] is the pure simulator — never a product Residiuum id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineId {
    ResidiuumEmbedded,
    ResidiuumServer,
    MongoLocal,
    CouchbaseLiteEmbedded,
    /// Test-only logical evaluator (not product; not competitive).
    LogicalHarness,
}

impl EngineId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ResidiuumEmbedded => "residiuum_embedded",
            Self::ResidiuumServer => "residiuum_server",
            Self::MongoLocal => "mongo_local",
            Self::CouchbaseLiteEmbedded => "cbl_embedded",
            Self::LogicalHarness => "logical_harness",
        }
    }

    /// True for product/comparator engines that may appear in Gate-1 competitive cells.
    pub fn is_competitive_product(self) -> bool {
        matches!(
            self,
            Self::ResidiuumEmbedded
                | Self::ResidiuumServer
                | Self::MongoLocal
                | Self::CouchbaseLiteEmbedded
        )
    }

    /// True only for the pure logical simulator.
    pub fn is_logical_simulator(self) -> bool {
        matches!(self, Self::LogicalHarness)
    }

    /// Lane this engine may participate in for Gate-1 cells.
    /// Logical harness scaffolds on Lane E only (not a competitive pairing).
    pub fn primary_lane(self) -> LaneId {
        match self {
            Self::ResidiuumEmbedded | Self::CouchbaseLiteEmbedded | Self::LogicalHarness => {
                LaneId::Embedded
            }
            Self::ResidiuumServer | Self::MongoLocal => LaneId::LocalClientServer,
        }
    }
}

/// One comparative pairing inside a single lane (side A vs side B).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LanePairing {
    pub lane: LaneId,
    pub side_a: EngineId,
    pub side_b: EngineId,
}

impl LanePairing {
    /// Competitive Lane E pairing (product Residiuum vs CBL).
    pub const EMBEDDED: Self = Self {
        lane: LaneId::Embedded,
        side_a: EngineId::ResidiuumEmbedded,
        side_b: EngineId::CouchbaseLiteEmbedded,
    };

    /// Scaffold Lane E: logical simulator vs CBL stub (not competitive).
    pub const SCAFFOLD_LOGICAL_VS_CBL: Self = Self {
        lane: LaneId::Embedded,
        side_a: EngineId::LogicalHarness,
        side_b: EngineId::CouchbaseLiteEmbedded,
    };

    pub const LOCAL_CS: Self = Self {
        lane: LaneId::LocalClientServer,
        side_a: EngineId::ResidiuumServer,
        side_b: EngineId::MongoLocal,
    };

    /// Reject cross-lane pairings (architecture invariant).
    pub fn validate(self) -> Result<(), String> {
        if self.side_a.primary_lane() != self.lane {
            return Err(format!(
                "side_a {} not eligible for lane {}",
                self.side_a.as_str(),
                self.lane.as_str()
            ));
        }
        if self.side_b.primary_lane() != self.lane {
            return Err(format!(
                "side_b {} not eligible for lane {}",
                self.side_b.as_str(),
                self.lane.as_str()
            ));
        }
        Ok(())
    }

    /// Competitive claim requires both sides to be product/comparator engines.
    pub fn is_competitive(self) -> bool {
        self.side_a.is_competitive_product() && self.side_b.is_competitive_product()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_pairings_validate() {
        LanePairing::EMBEDDED.validate().unwrap();
        LanePairing::LOCAL_CS.validate().unwrap();
        LanePairing::SCAFFOLD_LOGICAL_VS_CBL.validate().unwrap();
        assert!(LanePairing::EMBEDDED.is_competitive());
        assert!(!LanePairing::SCAFFOLD_LOGICAL_VS_CBL.is_competitive());
        assert!(EngineId::LogicalHarness.is_logical_simulator());
        assert!(!EngineId::LogicalHarness.is_competitive_product());
    }

    #[test]
    fn cross_lane_pairing_rejected() {
        let bad = LanePairing {
            lane: LaneId::Embedded,
            side_a: EngineId::ResidiuumEmbedded,
            side_b: EngineId::MongoLocal,
        };
        assert!(bad.validate().is_err());
    }
}
