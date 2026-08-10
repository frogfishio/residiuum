//! Database handle entry points (`DX_SPEC` §4 / `HEAP_SPEC` §7.1 / CPR-001).
//!
//! - **Heap-bound (always):** [`Residiuum::open_deployment`], [`Residiuum::connect_heap`]
//! - **Legacy flat (feature `legacy-flat-sdk`, default on):** [`Residiuum::open`],
//!   [`Residiuum::collection`], token [`Residiuum::connect`], cluster open

#[cfg(feature = "legacy-flat-sdk")]
mod flat;

#[cfg(feature = "legacy-flat-sdk")]
pub(crate) use flat::Backend;
#[cfg(feature = "legacy-flat-sdk")]
pub use flat::Residiuum;

#[cfg(not(feature = "legacy-flat-sdk"))]
mod heap_only {
    use crate::error::Error;
    use crate::heap::ResidiuumDeployment;
    use std::path::Path;

    /// Namespace for heap-bound entry points when `legacy-flat-sdk` is disabled.
    pub struct Residiuum;

    impl Residiuum {
        /// Open a store directory as a **deployment host** (heap-bound).
        pub fn open_deployment(path: impl AsRef<Path>) -> Result<ResidiuumDeployment, Error> {
            ResidiuumDeployment::open(path)
        }

        /// Create a new store directory as a deployment host.
        pub fn create_deployment(path: impl AsRef<Path>) -> Result<ResidiuumDeployment, Error> {
            ResidiuumDeployment::create(path)
        }

        /// Connect a **qualified** remote heap via HeapKey.
        pub fn connect_heap(
            url: impl AsRef<str>,
            options: crate::RemoteHeapOptions,
        ) -> Result<crate::RemoteHeap, Error> {
            crate::remote_heap::connect_heap(url, options)
        }
    }
}

#[cfg(not(feature = "legacy-flat-sdk"))]
pub use heap_only::Residiuum;
