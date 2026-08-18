//! Durable per-Heap Atomic coordinator and staged-member lane (CR-ATM2-001).
//!
//! Peer crate (Law 9): file-backed prepare/member append with `fsync`, not
//! `residiuum-store` / `residiuum-sdk` / `residiuum-perf`. Reconstructs the
//! in-memory [`residiuum_atomics::StagingHeap`] model from concatenated format
//! frames. Ordinary `get` / `scan` never observe staged members.
//! `Capabilities::atomics` stays false.

#![deny(missing_docs)]

mod error;
mod lane;
mod recover;
mod seal;

pub use error::LaneError;
pub use lane::DurableLane;

#[cfg(test)]
mod tests {
    #[test]
    fn crate_is_not_store_sdk_or_perf() {
        let manifest = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"));
        assert!(!manifest.contains("residiuum-store"));
        assert!(!manifest.contains("residiuum-sdk"));
        assert!(!manifest.contains("residiuum-perf"));
        assert!(!manifest.contains("residiuum-server"));
        assert!(!manifest.contains("residiuum-cluster"));
    }
}
