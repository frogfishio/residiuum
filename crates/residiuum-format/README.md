# residiuum-format

**Survival wire format** for Residiuum: frame encode/decode, structural integrity
(CRC32C + BLAKE3-256), deterministic CBOR envelopes, in-memory active segments
and seal, forward and reverse salvage scanning, event-id conflict analysis, and
chunk reassembly helpers.

This crate is pure format logic — **no durable storage IO**. The filesystem
store that writes and recovers segments is
[`residiuum-store`](https://crates.io/crates/residiuum-store).

## When to use this crate

| You want… | Use |
|-----------|-----|
| Open a database and put/get data | [`residiuum-sdk`](https://crates.io/crates/residiuum-sdk) |
| Single-node store (segments on disk) | [`residiuum-store`](https://crates.io/crates/residiuum-store) |
| Encode/decode/scan frames independently | **`residiuum-format`** (this crate) |
| Network RPC framing | [`residiuum-client`](https://crates.io/crates/residiuum-client) |

## Install

```toml
[dependencies]
residiuum-format = "0.1"
```

Or: `cargo add residiuum-format`

## Status

**Shipped** for Stage 2. Wire profile label `WIRE_PROFILE_LABEL` = `1.0-draft`
(`wire_major = 1`, `wire_minor = 0`). Not frozen as production wire major 1 —
freeze criteria and gaps: [WIRE_MAJOR1_FREEZE.md](../../doc/wip/format/WIRE_MAJOR1_FREEZE.md)
(DEF-053). Runtime honesty: `wire_is_frozen()` / `wire_freeze_summary()`. A
breaking on-disk change requires a major bump and dual-read support.

Implemented: frames, segment seal, scanners, the FORMAT_SPEC §13 destructive
corpus, and deterministic CBOR envelope validation.
ATM-2.1 adds Atomic `BatchPrepare` / `BatchCommit` envelopes, `ItemEvent`
linkage keys 37–40, and a byte-buffer recovery reader (no store write path).

## Quick example

```rust
use residiuum_format::{
    scan_forward, ActiveSegment, FrameKind, SafetyLimits, SegmentId, EMPTY_ENVELOPE,
};

let ids = SegmentId::new([1u8; 16], [2u8; 16]);
let mut seg = ActiveSegment::create(ids, SafetyLimits::default(), 0)?;
seg.append(FrameKind::ItemEvent, EMPTY_ENVELOPE, b"hello", [9u8; 16])?;
let sealed = seg.seal()?;

let report = scan_forward(sealed.as_bytes(), SafetyLimits::default());
assert!(report.verified_count() >= 3); // descriptor, item, summary
# Ok::<(), residiuum_format::SegmentError>(())
```

## API surface

| Area | Highlights |
|------|------------|
| Frame codec | `encode_frame`, `decode_frame`, `verify_frame_at` |
| Envelopes | `validate_deterministic_cbor_envelope`, `EMPTY_ENVELOPE`, uint-map encode/decode |
| Integrity | CRC32C prefix/envelope + suffix; BLAKE3-256 body |
| Segment | `ActiveSegment::create` / `append` / `seal` → `SealedSegment` |
| Scanner | `scan_forward` / `scan_reverse` → `ScanReport` (verified islands + holes) |
| Events | `group_by_event_id` → unique / replicas / conflicting |
| Chunks | `reassemble_chunks` → complete / partial / unavailable / conflicting |
| Atomics | `read_atomic_evidence` → valid / partial / corrupt / unsupported |
| Meta | Fixed descriptor/summary/chunk body layouts; `WIRE_PROFILE_LABEL` |

Envelopes must be a single definite-length CBOR map with unsigned integer keys,
shortest integer encodings, sorted unique keys, and valid UTF-8 text. The empty
map `0xa0` (`EMPTY_ENVELOPE`) is the minimal valid envelope.

## Design rule

> What is gone is gone. What remains still lives.

Frames are independently delimited and verified. Damage produces localized
holes; surviving verified islands remain recoverable without a global catalog.

## Out of scope (this crate)

- Durable storage IO / immutability enforcement — see
  [`residiuum-store`](https://crates.io/crates/residiuum-store)
- Compression or encryption transforms
- Full production chunk manifests (reassembly helpers only; the store owns
  chunked puts)
- Required-field checks per frame kind (partial)

## Related crates

| Crate | License | Role |
|-------|---------|------|
| [`residiuum-store`](https://crates.io/crates/residiuum-store) | MPL-2.0 | Filesystem store built on this format |
| [`residiuum-client`](https://crates.io/crates/residiuum-client) | MIT | Network RPC framing (separate from on-disk format) |
| [`residiuum-examine`](https://crates.io/crates/residiuum-examine) | MPL-2.0 | SDA examination over recovered frames |

## Documentation

- Format spec: [FORMAT_SPEC.md](https://github.com/frogfishio/dingodb/blob/main/doc/reference/storage/FORMAT_SPEC.md)
- Architecture: [OVERVIEW.md](https://github.com/frogfishio/dingodb/blob/main/doc/reference/product/OVERVIEW.md)

## License

MIT.

Part of [Residiuum](https://github.com/frogfishio/dingodb).