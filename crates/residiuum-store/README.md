# residiuum-store

Single-node **authoritative store** for Residiuum: filesystem-backed append-only
segments, put/get/delete by subject, durability modes, catalog-independent
recovery, derived state (catalogs, secondary indexes, chunks, history,
compaction, checkpoints), inspect/salvage helpers, and tiering
(hot/warm/cold/archive) with offline coverage honesty.

Most applications should use [`residiuum-sdk`](https://crates.io/crates/residiuum-sdk)
(named collections, JSON, filters). Use **this crate** when you need the raw
store API, operator tools, or to embed storage without the collection layer.

## When to use this crate

| You want… | Use |
|-----------|-----|
| Named collections, JSON, filters, remote connect | [`residiuum-sdk`](https://crates.io/crates/residiuum-sdk) |
| Subject-keyed store, salvage, tiering, durability modes | **`residiuum-store`** (this crate) |
| Frame codec only | [`residiuum-format`](https://crates.io/crates/residiuum-format) |
| Multi-node partitions / Raft | [`residiuum-cluster`](https://crates.io/crates/residiuum-cluster) |

## Install

```toml
[dependencies]
residiuum-store = { version = "0.2.3", features = ["legacy-raw-store"] }
```

Or: `cargo add residiuum-store`

## Quick example

```rust
use residiuum_store::{DurabilityMode, Store};

# let dir = tempfile::tempdir().unwrap();
# let path = dir.path();
let mut store = Store::create(path)?;
store.put("user-42", br#"{"name":"Alice"}"#, DurabilityMode::Durable)?;
assert_eq!(
    store.get("user-42")?.as_deref(),
    Some(br#"{"name":"Alice"}"#.as_slice())
);
store.delete("user-42", DurabilityMode::Durable)?;
assert!(store.get("user-42")?.is_none());
# Ok::<(), residiuum_store::StoreError>(())
```

## Status

**Packaging 0.2.3** adds bounded clean-open diagnostics and exact smart-client
mutation recovery across restart and history-loss compaction.

**0.2.0 is unsafe for continued writes across reopen/rotation** — see
[SECURITY_ADVISORY_SEGID_0.2.0.md](../../doc/todo/performance-qualification/SECURITY_ADVISORY_SEGID_0.2.0.md).

**Shipped** (Stages 3 + 6 + 7 inspect/salvage + 9 tiering). Highlights:

| Area | What you get |
|------|----------------|
| Core | open/create, put/get/delete, durability modes (`Memory`, `Buffered`, `Durable`) |
| Ownership | exclusive writer lock; `open_inspect` is lock-free read-only |
| Recovery | rebuildable primary index, salvage after catalog wipe, evidence-preserving `salvage_to` |
| Derived | collection catalog, secondary indexes, subject history, checkpoints |
| Hydra | adaptive per-segment indexes at seal (Eytzinger / PGM·RadixSpline / compressed radix / MPHF); multithread rebuild |
| Async lifecycle | DEF-096 Axis A: auto-seal O(1) rotates to `active/pending/`; background worker finalizes sealed image + Hydra/Chimera; `seal_active` stays sync; `drain_lifecycle` / recover-on-open |
| Sharded writers | DEF-096 Axis B: `create_with_shards(N)` → N active segments by subject hash; `put_many` parallel appends; N=1 keeps legacy layout |
| Chimera | product seal embeds Materialized payloads (CSE-2R **safety rollback**, not Compact parity); Compact `SegmentFrame` remains ETQ-only via `build_compact_layout` until CSE-3 recovery code; hot `get` uses PrimaryIndex |
| Chunks | chunked payloads with partial maps; phased live compaction |
| Operator | `open_inspect` (doctor), `salvage_to`, `export_live_state`, `backup_to` / `restore_full_backup` (DEF-050), `scrub_once` / `scrub_status` (DEF-051), `migrate_to` (DEF-052) |
| Tiering | segment move/copy with stable identities; offline-tier coverage holes |
| Honesty | fail-closed logical scans; absence only proven when coverage is complete |

## Layout

```text
store/
  store-info/     # store_id + meta + descriptor + writer.lock
  active/         # open append segment (at most one live file)
    active.residiuum
    pending/      # rotated segments awaiting background seal finalize (DEF-096)
  segments/       # sealed hot segments
  tiers/          # warm/cold/archive media
  catalogs/       # derived only (rebuildable)
  indexes/        # derived only (rebuildable)
  snapshots/      # derived checkpoints
  recovery/       # operator scratch + compaction jobs + scrub + migration jobs
```

## Format migration (DEF-052)

Phased, evidence-preserving copy into a **new** store (never in-place rewrite):

```rust
use residiuum_store::{DurabilityMode, MigrateOptions, MigratePhase, Store};

# let dir = tempfile::tempdir().unwrap();
# let src = dir.path().join("src");
# let dst = dir.path().join("dst");
let mut store = Store::create(&src)?;
store.put("k", b"v", DurabilityMode::Durable)?;
let report = store.migrate_to(&dst, MigrateOptions::default())?;
assert_eq!(report.phase, MigratePhase::Done);
# Ok::<(), residiuum_store::StoreError>(())
```

Job documents live under `recovery/migration/job.v1.json`. Wire support is
declared in `residiuum-format` (`wire_compat_matrix` / `SUPPORTED_READER_MAJORS`).

## Integrity scrub (DEF-051)

Bounded verification of segments and chunks:

```rust
use residiuum_store::{DurabilityMode, ScrubOptions, Store};

# let dir = tempfile::tempdir().unwrap();
# let root = dir.path().join("s");
let mut store = Store::create(&root)?;
store.put("k", b"v", DurabilityMode::Durable)?;
let report = store.scrub_to_completion(ScrubOptions::default())?;
assert!(report.cycle_completed);
assert_eq!(report.status.open_findings, 0);
# Ok::<(), residiuum_store::StoreError>(())
```

State lives under `recovery/scrub/` (`state.v1.json`, `findings.v1.json`,
optional `quarantine/` copies). Scrub never deletes or rewrites authoritative
segment bytes. Pause with `pause_scrub` / resume with `resume_scrub`.

## Backup and restore (DEF-050)

Full backups are **packages**, not live stores:

```text
backup-package/
  backup-manifest.v1.json   # profile residiuum-backup-v1 + blake3 of files
  store/                    # authoritative trees only (no lock files)
```

```rust
use residiuum_store::{restore_full_backup, DurabilityMode, RestoreOptions, Store};

# let dir = tempfile::tempdir().unwrap();
# let src = dir.path().join("src");
# let bak = dir.path().join("bak");
# let dst = dir.path().join("dst");
let mut store = Store::create(&src)?;
store.put("k", b"v", DurabilityMode::Durable)?;
store.backup_to(&bak)?;
drop(store);
let report = restore_full_backup(&bak, &dst, RestoreOptions::default())?;
assert_eq!(report.live_subjects, 1);
# Ok::<(), residiuum_store::StoreError>(())
```

Salvage remains the damage-recovery path; export-live re-materializes current
values with new lineage. Neither produces a `residiuum-backup-v1` package.

Deleting `catalogs/`, `indexes/`, and `snapshots/` must not prevent recovery:
the store rebuilds current state by scanning `active/`, `segments/`, and online
tier media.

## API surface

| API | Role |
|-----|------|
| `Store::open` / `Store::create` | Create-or-open; exclusive writer lock |
| `Store::open_inspect` | Read-only open (no writer lock, no derived writes) |
| `put` / `get` / `delete` | Subject-keyed current-state operations |
| `get_payload` | Completeness-aware read (`PayloadResult`) |
| `history` | Per-subject event stream |
| `WriteReceipt` / `DurabilityMode` | Event identity + acknowledged durability |
| `rebuild_index` / `salvage` | Catalog-free scan of all segment files |
| `salvage_to` | Evidence-preserving frame copy + recovery manifest |
| `export_live_state` | Live-only re-put materialization (new lineage) |
| `backup_to` / `restore_full_backup` | Content-hashed full backup package (DEF-050) |
| `scrub_once` / `scrub_to_completion` / `scrub_status` | Bounded integrity scrub + findings (DEF-051) |
| `compact_live` / compact job helpers | Phased live projection; reclaim only with `allow_history_loss` |
| `scan_live_page` | Bounded page + continuation token |
| `transfer_segment_to_tier` | Copy/move sealed segment (stable id) |
| `get_with_tier_coverage` | Absence only proven when coverage complete |
| `examination_sources` | Ordered `(source_name, bytes)` for examination |

## Durability modes

| Mode | Meaning |
|------|---------|
| `Memory` | Acknowledged in process; may be lost on crash |
| `Buffered` | Written to OS buffers; may be lost on power failure |
| `Durable` | Flushed for the configured durable path |

## Design rule

> Durable truth must not depend on replaceable machinery.

Indexes and catalogs make access fast but are not the sole authority. They are
designed to be rebuilt from immutable, independently framed segments
([`residiuum-format`](https://crates.io/crates/residiuum-format)).

## Out of scope (this crate)

- Native SigV4 HTTP object SDK (use mirror / fuse mount)
- Erasure encode/decode codecs (manifest only)
- Background lifecycle scheduler (policy evaluate only)
- `replicated` durability — see [`residiuum-cluster`](https://crates.io/crates/residiuum-cluster)

## Related crates

| Crate | License | Role |
|-------|---------|------|
| [`residiuum-format`](https://crates.io/crates/residiuum-format) | MIT | Wire format this store writes |
| [`residiuum-sdk`](https://crates.io/crates/residiuum-sdk) | MPL-2.0 | Collection API over this store |
| [`residiuum-examine`](https://crates.io/crates/residiuum-examine) | MPL-2.0 | SDA examination over salvage |
| [`residiuum-cluster`](https://crates.io/crates/residiuum-cluster) | AGPL-3.0-or-later | Multi-node federation |

## Documentation

- Architecture: [OVERVIEW.md](https://github.com/frogfishio/dingodb/blob/main/doc/reference/product/OVERVIEW.md)
- Format: [FORMAT_SPEC.md](https://github.com/frogfishio/dingodb/blob/main/doc/reference/storage/FORMAT_SPEC.md)
- Crash consistency: [doc/reference/operations/CRASH_CONSISTENCY.md](https://github.com/frogfishio/dingodb/blob/main/doc/reference/operations/CRASH_CONSISTENCY.md)
- Retention runbook: [doc/reference/operations/RUNBOOK_RETENTION.md](https://github.com/frogfishio/dingodb/blob/main/doc/reference/operations/RUNBOOK_RETENTION.md)

## License

MPL-2.0 (file-level weak copyleft). Proprietary applications may embed the
store; modifications to MPL-covered files must be disclosed.

Part of [Residiuum](https://github.com/frogfishio/dingodb). Multi-tier license map:
[doc/reference/operations/LICENSING.md](https://github.com/frogfishio/dingodb/blob/main/doc/reference/operations/LICENSING.md).
