# Residiuum Survival Format

Status: Draft wire profile v0.1  
Wire major version: 1  
Byte order: little-endian

## 1. Scope

This document defines the first concrete physical profile for Residiuum frames,
segments, and salvage scanning.

Its purpose is not merely to serialize data. Its purpose is to ensure that a
valid frame after an arbitrary damaged region can be rediscovered and verified
without the original catalog, index, process, or segment header.

The architecture requirements in [OVERVIEW.md](../product/OVERVIEW.md) remain normative.
If this profile conflicts with an independent-survival invariant, the
invariant wins and the wire profile MUST be revised.

## 2. Primitive types

Wire integers are unsigned little-endian values:

- `u8` — 1 byte;
- `u16` — 2 bytes;
- `u32` — 4 bytes;
- `u64` — 8 bytes;
- `id128` — 16 uninterpreted bytes;
- `hash256` — 32 bytes.

Lengths count bytes, not characters or logical values.

All offsets are relative to the first byte of the containing object unless
explicitly stated otherwise.

## 3. Integrity algorithms

Wire version 1 uses:

- CRC32C for fast rejection of false headers and corrupt structural metadata;
- BLAKE3-256 for content and frame-body integrity.

CRC32C is damage detection, not an authenticity mechanism.

BLAKE3-256 is cryptographic integrity evidence, not proof of authorship.

Signed profiles MAY add signatures to the envelope. Signatures do not replace
the required structural checks.

## 4. Frame layout

Every frame has four regions:

```text
fixed prefix | envelope | body | fixed suffix
    64 B       variable   variable    56 B
```

The encoded frame length is:

```text
64 + envelope_len + body_len + 56
```

Frames are not required to be aligned. A storage profile MAY align frames by
emitting explicit padding frames. Scanners MUST NOT assume alignment.

### 4.1 Fixed prefix

The fixed prefix is exactly 64 bytes:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | `start_magic` |
| 8 | 1 | `wire_major` |
| 9 | 1 | `wire_minor` |
| 10 | 1 | `frame_kind` |
| 11 | 1 | `flags` |
| 12 | 4 | `envelope_len` |
| 16 | 8 | `body_len` |
| 24 | 8 | `logical_len` |
| 32 | 8 | `writer_sequence` |
| 40 | 16 | `event_id` |
| 56 | 4 | `prefix_crc32c` |
| 60 | 4 | `reserved` |

`start_magic` is the eight ASCII bytes:

```text
RESIDFRM
```

`wire_major` is `1` for this profile.

`wire_minor` is `0` for this draft.

`envelope_len` is the encoded envelope size.

`body_len` is the stored body size after compression or encryption.

`logical_len` is the payload size after reversing the declared storage
transforms. For frames without a logical payload, it MUST be zero.

`writer_sequence` is a writer-local diagnostic sequence. It MUST NOT be used
as global identity or sole evidence of ordering.

`event_id` is the stable identifier of the event represented by the frame.
For non-event structural frames it is the structural object identifier.

`prefix_crc32c` covers:

1. the complete 64-byte prefix with bytes 56 through 59 treated as zero; and
2. the complete encoded envelope.

`reserved` MUST be zero when written. Readers MUST ignore its value after
including it in prefix verification.

### 4.2 Flags

Wire version 1 assigns:

| Bit | Name | Meaning |
|---:|---|---|
| 0 | `compressed` | body uses the envelope-declared compression |
| 1 | `encrypted` | body uses the envelope-declared authenticated encryption |
| 2 | `chunked` | body is or references a chunked payload |
| 3 | `canonical` | structured body uses its declared canonical encoding |
| 4 | `repair` | frame was produced by an evidence-recorded repair |
| 5–7 | — | reserved |

Writers MUST set reserved bits to zero. Readers MUST NOT reject an otherwise
valid frame solely because an unknown flag bit is set; they report the
unsupported transform and preserve the frame bytes.

### 4.3 Frame kinds

Wire version 1 assigns:

| Value | Kind |
|---:|---|
| 0 | invalid/reserved |
| 1 | store descriptor |
| 2 | segment descriptor |
| 3 | item event |
| 4 | payload chunk |
| 5 | batch prepare |
| 6 | batch commit |
| 7 | segment summary |
| 8 | purge attestation |
| 9 | padding |
| 10 | heap descriptor (`residiuum-heap-v1`) |
| 11 | collection descriptor (`residiuum-heap-v1`) |
| 12 | stream descriptor (`residiuum-heap-v1`) |
| 13 | heap migration evidence (`residiuum-heap-v1`) |
| 14 | evidence record (`residiuum-evidence-ledger-v1`) |
| 15 | evidence checkpoint (`residiuum-evidence-ledger-v1`) |
| 16 | evidence retention cut (`residiuum-evidence-ledger-v1`) |
| 17–127 | reserved for core versions |
| 128–255 | application/profile extension |

An unknown kind remains recoverable as an opaque verified frame.

### 4.4 Envelope encoding

Wire version 1 envelopes use deterministic CBOR.

The encoded envelope MUST:

- be one definite-length map;
- use unsigned integer core field keys;
- contain no indefinite-length item;
- contain each key at most once;
- use shortest integer encodings;
- sort keys by their deterministic encoded byte order;
- remain within `envelope_len`;
- contain no framing information that contradicts the fixed prefix.

Text values MUST be valid UTF-8. Arbitrary data belongs in byte strings.

Unknown envelope keys MUST be retained by lossless tools and ignored by readers
that do not understand them. Heap-ownership parsers (profile v1) treat keys
37–40 as a reserved Atomic extension namespace and MUST ignore them; they MUST
still reject malformed keys 31–36. Other keys above 40 remain unknown to
ownership profile v1.

Core envelope keys are:

| Key | Name | Required |
|---:|---|---|
| 1 | `item_id` | item and chunk frames |
| 2 | `event_kind` | item event frames |
| 3 | `store_id` | yes |
| 4 | `segment_id` | yes |
| 5 | `created_ns` | no |
| 6 | `subject_id` | no |
| 7 | `media_type` | no |
| 8 | `schema_id` | no |
| 9 | `body_encoding` | yes when body is structured |
| 10 | `compression` | when compressed |
| 11 | `encryption` | when encrypted |
| 12 | `content_hash` | item and chunk payloads |
| 13 | `chunk_index` | chunk frames |
| 14 | `chunk_count` | chunked payloads |
| 15 | `chunk_ids` | chunk manifest frames |
| 16 | `batch_id` | batch frames |
| 17 | `causal_parents` | no |
| 18 | `source` | no |
| 19 | `labels` | no |
| 20 | `user_metadata` | no |
| 21 | `transform_order` | when transformed |
| 22 | `repair_evidence` | when repair flag is set |
| 23 | `signature` | signed profile |
| 24 | `signing_key_id` | signed profile |
| 25 | `cluster_id` | clustered store |
| 26 | `partition_id` | partitioned event |
| 27 | `placement_epoch` | partitioned event |
| 28 | `partition_term` | strong partition event |
| 29 | `partition_position` | ordered partition event |
| 30 | `commit_evidence` | when portable commit evidence is present |
| 31 | `heap_id` | every heap-aware frame (`bstr` 16) |
| 32 | `collection_id` | collection data and indexes (`bstr` 16) |
| 33 | `stream_id` | stream data and indexes (`bstr` 16) |
| 34 | `ownership_profile` | every heap-aware frame (uint `1`) |
| 35 | `source_heap_id` | import provenance only |
| 36 | `source_object_id` | import provenance only |
| 37 | `atomic_id` | Atomic frames (`bstr` 32) |
| 38 | `ordinal` | Atomic `ItemEvent` members (uint) |
| 39 | `content_root` | Atomic frames (`bstr` 32) |
| 40 | `commit_position` | Atomic committed decision (nonzero uint) |
| 41 | `operation_id` | idempotent client mutation frames (`bstr` 16) |
| 42 | `operation_content_hash` | when `operation_id` is present (`bstr` 32) |

**Amendment (CR-ATM2-002 / CR-R2-006):** landed HP-002 already used keys 31–36
for Heap ownership. The draft assignment of 31/32 to `operation_id` /
`operation_content_hash` is relocated to 41/42 so one registry covers FORMAT,
ownership, and Atomics. Atomic writers MUST emit keys 31 and 34 together with
37–40 so live-store `admit_frame_to_heap` can bind the frame. Compatibility:
old ownership readers reject keys 37–40 as unknown; new readers ignore that
namespace. Store item writers emit 41/42 only. Store item readers accept a
legacy 31/32 pair when key 32 is `bstr(32)` (operation content hash). A
`bstr(16)` at key 32 is collection ownership and MUST NOT be read as operation
identity. When 41/42 are present they win over a legacy 31/32 pair. Architect
acceptance of this amendment is still required before treating the namespace
as a published freeze. Keys 41 and 42 form one pair. Writers MUST emit both or
neither.
The operation ID is an opaque 16-byte string and the content hash is a 32-byte
canonical request digest. They make an accepted mutation outcome
reconstructable from authoritative media when a derived retry cache is absent
or interrupted. Before history-loss compaction reclaims frames carrying this
evidence, the implementation MUST durably materialize their decisions in the
retained operation ledger. An identity-reassigned clone establishes a new
operation-ID namespace and MUST NOT replay decisions belonging to the source
store ID.

The value types and exact event envelopes will be frozen before wire version 1
is declared stable. A draft reader MUST preserve unknown or not-yet-frozen
values losslessly.

### 4.5 Body

The body is exactly `body_len` bytes.

The fixed prefix does not require a body decoder. A scanner can skip or verify
an unsupported body using its length and suffix.

When compression and encryption are both present, `transform_order` MUST state
their order. An implementation MUST NOT guess.

Compression units and authenticated-encryption domains MUST be local to the
frame or an independently framed chunk.

### 4.6 Fixed suffix

The fixed suffix is exactly 56 bytes:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | `end_magic` |
| 8 | 8 | `frame_len` |
| 16 | 32 | `body_hash` |
| 48 | 4 | `suffix_crc32c` |
| 52 | 4 | `reserved` |

`end_magic` is the eight ASCII bytes:

```text
RESIDEND
```

`frame_len` MUST equal `64 + envelope_len + body_len + 56`.

`body_hash` is BLAKE3-256 over the stored body bytes. For an empty body it is
the BLAKE3-256 digest of the empty byte string.

`suffix_crc32c` covers the complete 56-byte suffix with bytes 48 through 51
treated as zero.

`reserved` MUST be zero when written and ignored after suffix verification.

### 4.7 Atomic staging records (CR-ATMR6-006)

These records are the shipped store-owned Atomic staging authority. They are
not ordinary indexed `ItemEvent` puts. Compaction live-projection, source
reclaim, and identity-reassign clone MUST NOT retire them. Until ATM-4
delivers copy-through preservation, implementations MUST fail those
operations closed while outstanding staging evidence exists.

Outstanding evidence is any of:

- `store-info/atomic-stage.ckpt` that authenticates and names prepares,
  members, seals, payload/chunk locators, blocked identities, findings,
  intended members, or coverage degradation; an acceleration-only frontier
  over ordinary media is not outstanding Atomic evidence;
- that checkpoint file present but unreadable;
- `store-info/atomic-coord.ckpt` that authenticates with one or more issued
  sequences;
- a verified Atomic `BatchPrepare` / member frame, or a sidecar magic, in the
  dirty active or pending-seal tail.

`BodyRef.rel_path` and covered-file paths are store-relative. After a file is
renamed or reclaimed they MUST NOT be trusted. This slice refuses rotation
instead of silently relocating locators.

Recovery Shadow publication of an Atomic-bearing active is produced only
during seal. Refusing seal therefore gates Recovery Shadow until ATM-4.

Backup profile `residiuum-backup-v1` copies `store-info/` (including
`atomic-stage.ckpt` and `atomic-coord.ckpt`) plus `active/`, `segments/`,
`chunks/`, `recovery/`, and `tiers/`. Same-identity restore preserves staging.
Identity-reassign clone MUST refuse while outstanding staging exists: prepare
`heap_id` is the source store id and becomes foreign after reassignment.

#### Checkpoint `ATCKP1` version 10

File: `store-info/atomic-stage.ckpt`. Domain separator
`RESIDIUUM-STORE-ATOMIC-STAGE-CKP-V10`. Layout, all integers big-endian:

| Field | Encoding |
|---|---|
| magic | `ATCKP1` |
| version | `u8` = 10 |
| covered files | `u32` count; each: `u16` path len, path UTF-8, `u8` atomic_evidence, `u64` covered_len, head `[32]`, tail `[32]`, `u32` block count, block hashes `[32]*`, leftover `u32` + hash `[32]`. Ordinary (`atomic_evidence=0`) entries carry no block hashes and are metadata-only; Atomic entries authenticate every byte. |
| prepares | `u32` count; each: `u32` len + `encode_prepare` bytes |
| members | `u32` count; each: `u32` len + `encode_member` bytes |
| payload refs | `u32` count; each: atomic_id `[32]`, ordinal `u32`, `BodyRef` |
| seals | `u32` count; each: atomic_id `[32]`, content_root `[32]` |
| blocked | `u32` count; each: atomic_id `[32]` |
| prepare_batch | `u32` count; each: atomic_id `[32]` |
| coord_next | `u64` |
| coord_seq | `u32` count; each: atomic_id `[32]`, seq `u64` |
| chunk plans | `u32` count; each: atomic_id `[32]`, ordinal `u32`, total `u32`, hash count `u32`, hashes `[32]*` |
| chunk refs | `u32` count; each: atomic_id `[32]`, ordinal `u32`, index `u32`, `BodyRef` |
| coverage_degraded | `u8` |
| findings | `u32` count; each: kind `u8`, class `u8`, has_id `u8`, optional atomic_id `[32]` |
| missing_covered | `u32` count; each: `u16` len + UTF-8 |
| intended_members | `u32` count; each: atomic_id `[32]`, `u32` |
| digest | BLAKE3-256(`domain` \\| body) |

`BodyRef` is `u16` path len, path UTF-8, `u64` offset, `u32` len, hash `[32]`.
Covered block size is 64 KiB. Finding kind 8 is global `Coverage` loss.
Version ≠ 10 or domain mismatch MUST NOT be interpreted; recovery rebuilds
from media. Older checkpoint versions are not readable by a v10 decoder.

#### Coordinator `ATCRD1` version 1

File: `store-info/atomic-coord.ckpt`. Domain
`RESIDIUUM-STORE-ATOMIC-COORD-V1`:

`ATCRD1` + `u8` version 1 + `u64` next + `u32` count + (`u64` seq + atomic_id
`[32]`)* + BLAKE3-256(domain \\| body). Sequences are unique and nonzero.

#### Sidecar bodies inside `PayloadChunk` frames

Ordinary recovery skips these bodies (`decode_piece_body` fail-closed). New
prepares are `BatchPrepare` frames, not `ATPREP1`.

| Magic | Body after magic |
|---|---|
| `ATPAY1` | atomic_id `[32]` + ordinal `u32` + payload bytes |
| `ATSEAL1` | atomic_id `[32]` + content_root `[32]` |
| `ATMAP1` | atomic_id `[32]` + ordinal `u32` + total `u32` + hashes `[32]*total` |
| `ATCHK1` | atomic_id `[32]` + ordinal `u32` + index `u32` + chunk bytes |
| `ATPREP1` | legacy `encode_prepare` bytes only; writers MUST NOT emit it |

## 5. Frame validity

A frame is `verified-complete` under wire version 1 only if:

1. start magic matches;
2. the major version is supported;
3. lengths are within the configured safety limits;
4. the complete candidate frame is physically available;
5. prefix and envelope CRC32C verifies;
6. envelope deterministic-CBOR rules verify;
7. suffix magic matches at the computed boundary;
8. suffix CRC32C verifies;
9. suffix `frame_len` matches the computed length;
10. BLAKE3-256 of the stored body matches `body_hash`;
11. required envelope fields for the frame kind are present and consistent.

A scanner MUST NOT report a frame as verified when any applicable condition
fails.

Unsupported transforms, codecs, payload schemas, or frame kinds do not make a
structurally valid frame corrupt. They produce a verified opaque or
format-unsupported result.

## 6. Segment format

A segment is a byte sequence containing frames.

The first ordinarily written frame is a segment descriptor. The final
ordinarily written frame of a sealed segment is a segment summary.

Neither frame is required for salvage of independent item or chunk frames.

### 6.1 Segment descriptor

A segment descriptor identifies:

- store identifier;
- segment identifier;
- creator implementation and version;
- creation time when available;
- configured safety limits;
- writer identity or shard;
- declared feature profile.

Repeated segment descriptors MAY appear as recovery anchors. They MUST use the
same segment identifier and MUST NOT contradict immutable properties.

### 6.2 Segment summary

A segment summary SHOULD contain:

- segment identifier;
- sealed byte length;
- verified frame count;
- frame-kind counts;
- minimum and maximum declared times;
- item and event identifier summary;
- whole-segment or region integrity roots;
- previous summary identifier when chained;
- summary creation implementation and version.

Summaries accelerate discovery and integrity checks. They are derived hints.
A missing or corrupt summary does not invalidate surviving frames.

### 6.3 Sealing

A segment is sealed only after:

1. every acknowledged durable frame has reached the stable-storage boundary;
2. a complete segment summary frame has been appended and made durable;
3. the segment has been marked immutable by the storage profile.

An interrupted seal leaves an active or ambiguously sealed segment. Recovery
still scans it frame by frame.

## 7. Salvage scanner

### 7.1 Safety limits

Before scanning, an implementation establishes limits for:

- maximum envelope length;
- maximum stored body length;
- maximum frame length;
- maximum CBOR nesting;
- maximum decoded allocation;
- maximum candidate verifications per byte range.

These limits are part of the scan report.

The default limits MUST permit bounded forward progress on adversarial bytes.

### 7.2 Forward scan

For a byte source of length `N`, the scanner performs:

```text
p := 0
while p + 64 <= N:
    q := find_next("RESIDFRM", p)
    if no q:
        emit hole for remaining unclassified bytes, when applicable
        stop

    read candidate prefix at q
    if version or lengths are implausible:
        record rejected candidate
        p := q + 1
        continue

    read candidate envelope
    if prefix/envelope CRC fails:
        record rejected candidate
        p := q + 1
        continue

    compute candidate suffix position using checked arithmetic
    if suffix is unavailable or invalid:
        record damaged candidate
        p := q + 1
        continue

    if body hash fails:
        record corrupt frame and its claimed range
        p := q + 1
        continue

    emit verified frame
    p := q + frame_len
```

After any failed candidate, the next search begins at `q + 1`, not at a length
claimed by the failed header. This prevents a corrupt length from hiding later
frames.

Implementations MAY optimize the search while preserving identical acceptance
semantics.

### 7.3 Hole construction

A scanner classifies byte ranges as:

- verified frame;
- verified padding;
- corrupt candidate frame;
- unclassified garbage;
- unreadable physical range;
- absent expected range.

Adjacent non-data ranges MAY be coalesced only when their distinct reasons and
evidence remain available.

A scanner MUST NOT infer the exact extent of missing bytes when the evidence
does not establish it.

### 7.4 Reverse assistance

When the storage medium supports reverse reads, suffix magic and `frame_len`
MAY assist reverse discovery.

A reverse-discovered frame is subject to the same complete validity rules as a
forward-discovered frame.

## 8. Chunk manifests and partial payloads

A chunked item has a manifest containing the ordered chunk identifiers,
individual logical lengths, and the complete logical content hash.

Each chunk remains independently verifiable.

Reassembly states are:

- `complete` — every required chunk verifies and the full content hash matches;
- `partial` — at least one chunk verifies and at least one required chunk is
  missing, corrupt, encrypted-unavailable, or unsupported;
- `unavailable` — no payload chunk is available;
- `conflicting` — more than one verified chunk claims the same manifest
  position with different content.

Partial reads MUST return an ordered extent map. Missing extents MUST never be
filled silently with zeros or neighboring bytes.

## 9. Duplicate and conflicting frames

Physical replicas may contain byte-identical frames with the same event
identifier. Deduplication is derived behavior.

If two verified frames share an event identifier but differ in envelope or
body, both survive and the recovery result is `conflicting`.

An implementation MUST NOT choose one conflicting frame based only on physical
encounter order.

## 10. False-positive resistance

Start magic is a candidate locator, not proof.

A candidate becomes verified only after independent structural and body checks.
For uniformly random damage, accidental acceptance requires a matching magic,
plausible bounded fields, valid structural CRCs, a matching end boundary, and
a matching 256-bit body digest.

Implementations MUST publish any recovery mode that weakens these checks.

## 11. Diagnostic projection

Every frame MUST have a deterministic lossless diagnostic representation.

The text projection MUST include:

- physical source and byte range;
- all fixed prefix and suffix fields;
- envelope keys, including unknown keys;
- body as decoded structure when supported;
- otherwise body bytes or a stable byte reference;
- every verification result;
- unsupported features;
- holes and conflicts.

JSON diagnostic output MUST encode arbitrary bytes using an explicitly named
encoding. It MUST NOT perform lossy replacement of invalid text.

SDA examination consumes the same evidence model; it is not allowed to hide
verification failures present in the diagnostic projection.

## 12. Versioning

Major versions may change framing semantics.

Minor versions may add frame kinds, flags, or envelope fields while preserving
the ability of an older same-major reader to locate, bound, verify, and retain
unknown frames.

The byte values and field assignments in this draft are not stable until the
project declares wire version 1 frozen. Test data written before that
declaration MUST identify itself as draft data.

## 13. Required wire-format tests

The wire implementation MUST test:

- every prefix and suffix field at boundary values;
- checked arithmetic for every length combination;
- all one-byte truncation positions;
- every one-byte corruption position in representative frames;
- false magic inside bodies;
- corrupt length followed by a valid frame;
- missing prefix followed by a valid suffix and later frame;
- valid prefix with missing suffix;
- valid suffix with missing prefix;
- unsupported kinds, flags, envelope keys, and codecs;
- duplicated and conflicting event identifiers;
- partial chunk maps;
- forward and reverse discovery agreement;
- scanning without a segment descriptor or summary;
- scanning random and adversarial garbage.

For every destructive test, later intact frames MUST remain discoverable.
