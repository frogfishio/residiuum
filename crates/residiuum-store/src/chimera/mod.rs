//! Chimera: workload-compiled value storage (INDEXING_STRATEGY_PROPOSAL FINAL DESIGN).
//!
//! Hydra chooses **how to find** a key in a sealed segment. Chimera chooses
//! **where the value lives** and how to fetch/decompress it:
//!
//! ```text
//! Hydra locator
//!     ↓
//! resident value
//! OR inline value
//! OR point container
//! OR scan extent
//! OR large-value log
//! OR segment frame (compact default)
//!     ↓
//! adaptive buffered/direct async I/O
//!     ↓
//! record-level decompression
//! ```
//!
//! Background compiler plans GC, relocation, reclustering, dictionary training,
//! hot/cold migration, and lifetime-aware placement.
//!
//! **Honesty:** codecs + planner + **seal/compaction layout sidecars**. Live
//! `Store::put` still writes segment frames + `PrimaryIndex` (frames remain
//! authoritative; do not omit bodies on put until FORMAT/profile says so).
//! At seal/compact, Chimera writes **compact** `indexes/chimera/*.cmr` layouts:
//! sorted key → [`ValueLocator::SegmentFrame`] (segment id + frame offset/len).
//! Payloads remain in authoritative segments. Full-payload embedding
//! (`build_materialized_layout`) is obsolete and non-default.
//! Hot `Store::get` uses PrimaryIndex locators first; `Store::get_via_chimera`
//! resolves compact sidecars via segment pread. Chimera is never authoritative
//! (Law 6). Dual-rep and ZNS stay deferred (see `INDEXING_STRATEGY_PROPOSAL.md`).

mod classify;
mod compiler;
mod container;
mod io_path;
mod layout;
mod value_log;

pub use classify::{
    classify_value, initial_locator_kind, ClassifyOptions, LifetimeClass, LocatorKind,
    PlacementHints, TemperatureClass, ValueClass, DEFAULT_MEDIUM_MAX, DEFAULT_TINY_MAX,
};
pub use compiler::{
    plan_compile, plan_recluster_range, CompilerOp, CompilerOptions, CompilerPlan, RecordStats,
};
pub use container::{
    read_slot, ContainerBuilder, ContainerSlot, PointContainer, CODEC_RAW, CONTAINER_MAGIC,
    CONTAINER_VERSION, DEFAULT_CONTAINER_TARGET,
};
pub use io_path::{select_io_path, IoHints, IoPath, IoSelectOptions};
pub use layout::{
    build_compact_layout, build_layout, build_materialized_layout, chimera_dir,
    chimera_layout_path, delete_chimera_layout, try_load_chimera_layout, write_chimera_layout,
    ChimeraKindCounts, ChimeraLayout, CompactFrameRef, CHIMERA_LAYOUT_VERSION,
    CHIMERA_LAYOUT_VERSION_LEGACY,
};
pub use value_log::{
    decode_record, ValueLog, ValueLogRecord, VALUE_LOG_HEADER_LEN, VALUE_LOG_MAGIC,
};

use crate::error::StoreError;

/// Hydra-facing value locator: physical placement of one live value.
///
/// Generation fields enable atomic relocation: old extents stay readable until
/// epoch reclamation completes (FINAL DESIGN §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueLocator {
    /// Memory-resident / hot working set (no I/O).
    Resident {
        /// Cache generation / epoch.
        generation: u32,
    },
    /// Value bytes stored with the index / envelope entry (tiny class).
    Inline {
        /// Payload bytes.
        bytes: Vec<u8>,
    },
    /// Slot in a sealed point-optimized micro-page container.
    PointContainer {
        /// Container identity (store-local).
        container_id: u64,
        /// Slot index within the container.
        slot: u32,
        /// Relocation generation.
        generation: u32,
    },
    /// Range inside a scan-optimized key-ordered extent.
    ScanExtent {
        /// Extent identity.
        extent_id: u64,
        /// Byte offset within the extent.
        offset: u32,
        /// Byte length.
        len: u32,
        /// Relocation generation.
        generation: u32,
    },
    /// Record in the large-value append log.
    LargeValueLog {
        /// Log file / extent identity.
        log_id: u64,
        /// Byte offset of the record header.
        offset: u64,
        /// Encoded record length (header + value).
        len: u64,
        /// Relocation generation.
        generation: u32,
    },
    /// Frame in an authoritative segment (default compact Chimera persistence).
    ///
    /// Payload is **not** embedded in the `.cmr`; resolve via segment pread at
    /// `frame_offset`. `body_len == 0` means unknown (validate by frame verify
    /// only); non-zero must match the verified item body length (fail-closed).
    SegmentFrame {
        /// Segment that holds the establishing item frame.
        segment_id: [u8; 16],
        /// Byte offset of the item frame within that segment.
        frame_offset: u64,
        /// Expected item body length, or 0 when unknown.
        body_len: u32,
        /// Relocation generation.
        generation: u32,
    },
}

impl ValueLocator {
    /// Locator kind discriminant.
    pub fn kind(&self) -> LocatorKind {
        LocatorKind::of(self)
    }

    /// Relocation generation when applicable (`Inline` returns 0).
    pub fn generation(&self) -> u32 {
        match self {
            Self::Resident { generation }
            | Self::PointContainer { generation, .. }
            | Self::ScanExtent { generation, .. }
            | Self::LargeValueLog { generation, .. }
            | Self::SegmentFrame { generation, .. } => *generation,
            Self::Inline { .. } => 0,
        }
    }

    /// Inline body when this locator is [`Self::Inline`].
    pub fn inline_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Inline { bytes } => Some(bytes.as_slice()),
            _ => None,
        }
    }
}

/// Materialized sources needed to resolve non-inline locators.
///
/// Callers supply only the backends required for the locator under resolution.
#[derive(Debug, Default)]
pub struct ResolveContext<'a> {
    /// Resident cache hit (key already looked up by caller).
    pub resident_value: Option<&'a [u8]>,
    /// Decoded point container when resolving [`ValueLocator::PointContainer`].
    pub point_container: Option<&'a PointContainer>,
    /// Scan extent bytes when resolving [`ValueLocator::ScanExtent`].
    pub scan_extent_bytes: Option<&'a [u8]>,
    /// Large-value log when resolving [`ValueLocator::LargeValueLog`].
    pub value_log: Option<&'a ValueLog>,
    /// Verified item body when resolving [`ValueLocator::SegmentFrame`].
    pub segment_frame_bytes: Option<&'a [u8]>,
}

/// Resolved logical value bytes (record-level decompression applied).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedValue {
    /// Logical payload.
    pub bytes: Vec<u8>,
    /// I/O path that would be selected for a cold fetch of this value.
    pub io_path: IoPath,
    /// Locator kind that supplied the bytes.
    pub source: LocatorKind,
}

/// Resolve a locator to logical bytes using the provided context.
///
/// Performs record-level “decompression” (today: raw codec only) and records
/// the adaptive I/O path for telemetry / future async submit.
pub fn resolve(
    locator: &ValueLocator,
    ctx: &ResolveContext<'_>,
    io_opts: &IoSelectOptions,
) -> Result<ResolvedValue, StoreError> {
    let (bytes, source, transfer_hint, cached) = match locator {
        ValueLocator::Resident { .. } => {
            let v = ctx.resident_value.ok_or_else(|| {
                StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "resident value missing from resolve context",
                ))
            })?;
            (v.to_vec(), LocatorKind::Resident, v.len() as u64, true)
        }
        ValueLocator::Inline { bytes } => {
            (bytes.clone(), LocatorKind::Inline, bytes.len() as u64, true)
        }
        ValueLocator::PointContainer { slot, .. } => {
            let c = ctx.point_container.ok_or_else(|| {
                StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "point container missing from resolve context",
                ))
            })?;
            let v = c.get(*slot).ok_or_else(|| {
                StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "point container slot missing or unsupported codec",
                ))
            })?;
            (v, LocatorKind::PointContainer, 4096, false)
        }
        ValueLocator::ScanExtent { offset, len, .. } => {
            let ext = ctx.scan_extent_bytes.ok_or_else(|| {
                StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "scan extent missing from resolve context",
                ))
            })?;
            let start = *offset as usize;
            let end = start
                .checked_add(*len as usize)
                .ok_or_else(|| StoreError::PayloadTooLarge)?;
            if end > ext.len() {
                return Err(StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "scan extent range out of bounds",
                )));
            }
            (
                ext[start..end].to_vec(),
                LocatorKind::ScanExtent,
                *len as u64,
                false,
            )
        }
        ValueLocator::LargeValueLog { offset, len, .. } => {
            let log = ctx.value_log.ok_or_else(|| {
                StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "value log missing from resolve context",
                ))
            })?;
            let rec = log.read_at(*offset, *len)?;
            let n = rec.value.len() as u64;
            (rec.value, LocatorKind::LargeValueLog, n, false)
        }
        ValueLocator::SegmentFrame { body_len, .. } => {
            // Segment frames resolve via store pread, not in-layout bytes.
            let v = ctx.segment_frame_bytes.ok_or_else(|| {
                StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "segment frame bytes missing from resolve context",
                ))
            })?;
            if *body_len != 0 && v.len() as u32 != *body_len {
                return Err(StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "chimera segment frame body_len mismatch",
                )));
            }
            (v.to_vec(), LocatorKind::SegmentFrame, v.len() as u64, false)
        }
    };

    let io_path = select_io_path(
        &IoHints {
            transfer_bytes: transfer_hint,
            likely_cached: cached,
            batchable: matches!(
                source,
                LocatorKind::PointContainer
                    | LocatorKind::LargeValueLog
                    | LocatorKind::ScanExtent
                    | LocatorKind::SegmentFrame
            ),
            async_available: false,
            direct_available: false,
        },
        io_opts,
    );

    Ok(ResolvedValue {
        bytes,
        io_path,
        source,
    })
}

/// Classify `value` and produce an initial locator (inline or needs placement ids).
///
/// For [`LocatorKind::Inline`], the locator is complete. For other kinds this
/// returns a **template** locator with zeroed ids; the write path must assign
/// container/log/extent identities after packing.
pub fn place_value(
    value: &[u8],
    classify_opts: &ClassifyOptions,
    hints: &PlacementHints,
    generation: u32,
) -> ValueLocator {
    match initial_locator_kind(value.len(), classify_opts, hints) {
        LocatorKind::Inline | LocatorKind::Resident => ValueLocator::Inline {
            bytes: value.to_vec(),
        },
        LocatorKind::PointContainer => ValueLocator::PointContainer {
            container_id: 0,
            slot: 0,
            generation,
        },
        LocatorKind::ScanExtent => ValueLocator::ScanExtent {
            extent_id: 0,
            offset: 0,
            len: value.len() as u32,
            generation,
        },
        LocatorKind::LargeValueLog => ValueLocator::LargeValueLog {
            log_id: 0,
            offset: 0,
            len: 0,
            generation,
        },
        LocatorKind::SegmentFrame => ValueLocator::SegmentFrame {
            segment_id: [0u8; 16],
            frame_offset: 0,
            body_len: value.len() as u32,
            generation,
        },
    }
}

/// Pack medium values into sealed point containers; returns `(containers, locators)`.
///
/// Locators use sequential `container_id`s starting at `container_id_start` and
/// per-container slot indices. Non-medium values get `None` in the parallel vec.
pub fn pack_point_containers(
    values: &[(Vec<u8>, Vec<u8>)], // (key, value) — key reserved for future clustering
    container_id_start: u64,
    generation: u32,
    target_bytes: usize,
    classify_opts: &ClassifyOptions,
) -> (Vec<PointContainer>, Vec<Option<ValueLocator>>) {
    let mut containers = Vec::new();
    let mut locators = Vec::with_capacity(values.len());
    let mut builder = ContainerBuilder::new(generation, Vec::new(), target_bytes);
    let mut current_id = container_id_start;
    let mut next_slot: u32 = 0;

    for (_k, v) in values {
        if classify_value(v.len(), classify_opts) != ValueClass::Medium {
            locators.push(None);
            continue;
        }
        if !builder.try_push(v.clone()) {
            // Seal full container and open the next id.
            debug_assert!(!builder.is_empty());
            let sealed = std::mem::replace(
                &mut builder,
                ContainerBuilder::new(generation, Vec::new(), target_bytes),
            )
            .seal();
            containers.push(sealed);
            current_id += 1;
            next_slot = 0;
            assert!(
                builder.try_push(v.clone()),
                "single medium value must fit in an empty container builder"
            );
        }
        locators.push(Some(ValueLocator::PointContainer {
            container_id: current_id,
            slot: next_slot,
            generation,
        }));
        next_slot += 1;
    }

    if !builder.is_empty() {
        containers.push(builder.seal());
    }

    (containers, locators)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_inline() {
        let loc = ValueLocator::Inline {
            bytes: b"hi".to_vec(),
        };
        let got = resolve(
            &loc,
            &ResolveContext::default(),
            &IoSelectOptions::default(),
        )
        .unwrap();
        assert_eq!(got.bytes, b"hi");
        assert_eq!(got.source, LocatorKind::Inline);
        assert_eq!(got.io_path, IoPath::Buffered);
    }

    #[test]
    fn resolve_point_container_slot() {
        let c = PointContainer::seal(1, Vec::new(), &[b"a".to_vec(), b"b".to_vec()]);
        let loc = ValueLocator::PointContainer {
            container_id: 9,
            slot: 1,
            generation: 1,
        };
        let ctx = ResolveContext {
            point_container: Some(&c),
            ..Default::default()
        };
        let got = resolve(&loc, &ctx, &IoSelectOptions::default()).unwrap();
        assert_eq!(got.bytes, b"b");
        assert_eq!(got.source, LocatorKind::PointContainer);
    }

    #[test]
    fn resolve_value_log() {
        let mut log = ValueLog::new();
        let (off, len) = log.append(&ValueLogRecord::new(3, b"big-payload".to_vec()));
        let loc = ValueLocator::LargeValueLog {
            log_id: 1,
            offset: off,
            len,
            generation: 3,
        };
        let ctx = ResolveContext {
            value_log: Some(&log),
            ..Default::default()
        };
        let got = resolve(&loc, &ctx, &IoSelectOptions::default()).unwrap();
        assert_eq!(got.bytes, b"big-payload");
    }

    #[test]
    fn resolve_scan_extent() {
        let ext = b"xxxxPAYLOADyyyy";
        let loc = ValueLocator::ScanExtent {
            extent_id: 1,
            offset: 4,
            len: 7,
            generation: 0,
        };
        let ctx = ResolveContext {
            scan_extent_bytes: Some(ext),
            ..Default::default()
        };
        let got = resolve(&loc, &ctx, &IoSelectOptions::default()).unwrap();
        assert_eq!(got.bytes, b"PAYLOAD");
    }

    #[test]
    fn segment_frame_fail_closed_on_len_mismatch_and_missing() {
        let loc = ValueLocator::SegmentFrame {
            segment_id: [1u8; 16],
            frame_offset: 64,
            body_len: 4,
            generation: 1,
        };
        assert!(resolve(
            &loc,
            &ResolveContext::default(),
            &IoSelectOptions::default()
        )
        .is_err());
        let wrong = b"toolong";
        let ctx = ResolveContext {
            segment_frame_bytes: Some(wrong.as_slice()),
            ..Default::default()
        };
        assert!(resolve(&loc, &ctx, &IoSelectOptions::default()).is_err());
        let ok = b"abcd";
        let ctx = ResolveContext {
            segment_frame_bytes: Some(ok.as_slice()),
            ..Default::default()
        };
        let got = resolve(&loc, &ctx, &IoSelectOptions::default()).unwrap();
        assert_eq!(got.bytes, b"abcd");
        assert_eq!(got.source, LocatorKind::SegmentFrame);
    }

    #[test]
    fn place_value_by_class() {
        let opts = ClassifyOptions::default();
        let hints = PlacementHints::default();
        assert!(matches!(
            place_value(b"tiny", &opts, &hints, 0),
            ValueLocator::Inline { .. }
        ));
        let med = vec![0u8; 200];
        assert!(matches!(
            place_value(&med, &opts, &hints, 1),
            ValueLocator::PointContainer { generation: 1, .. }
        ));
        let large = vec![0u8; 32 * 1024];
        assert!(matches!(
            place_value(&large, &opts, &hints, 2),
            ValueLocator::LargeValueLog { generation: 2, .. }
        ));
    }

    #[test]
    fn pack_medium_values() {
        let opts = ClassifyOptions::default();
        let values = vec![
            (b"a".to_vec(), vec![1u8; 100]),
            (b"b".to_vec(), b"tiny".to_vec()),
            (b"c".to_vec(), vec![2u8; 100]),
        ];
        let (containers, locs) = pack_point_containers(&values, 10, 1, 64 * 1024, &opts);
        assert_eq!(containers.len(), 1);
        assert_eq!(containers[0].len(), 2);
        assert!(locs[0].is_some());
        assert!(locs[1].is_none());
        assert!(locs[2].is_some());
        if let Some(ValueLocator::PointContainer {
            container_id, slot, ..
        }) = &locs[0]
        {
            assert_eq!(*container_id, 10);
            assert_eq!(*slot, 0);
        } else {
            panic!("expected point container for medium a");
        }
    }

    #[test]
    fn architecture_path_end_to_end() {
        // Hydra would hand us a locator; we resolve → I/O path → bytes.
        let mut log = ValueLog::new();
        let payload = vec![7u8; 40_000];
        let (off, len) = log.append(&ValueLogRecord::new(1, payload.clone()));
        let loc = ValueLocator::LargeValueLog {
            log_id: 1,
            offset: off,
            len,
            generation: 1,
        };
        assert_eq!(loc.kind(), LocatorKind::LargeValueLog);
        let ctx = ResolveContext {
            value_log: Some(&log),
            ..Default::default()
        };
        let resolved = resolve(&loc, &ctx, &IoSelectOptions::default()).unwrap();
        assert_eq!(resolved.bytes, payload);
        // Without async/direct flags, foundation selects buffered.
        assert_eq!(resolved.io_path, IoPath::Buffered);
    }
}
