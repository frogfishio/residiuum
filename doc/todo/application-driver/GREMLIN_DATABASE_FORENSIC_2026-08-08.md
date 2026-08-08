# Gremlin database forensic review — 2026-08-08

Source examined read-only:
`tmp/com.koderra.tinker/store/tinker.residiuum`

The supplied source was not opened writable and no scrub state was written to
it. Writable-open measurements used an APFS copy-on-write clone under `/tmp`.

## Integrity result

- Store size: 2.9 GiB logical.
- Sealed segments: 309; active segments: 1.
- Recovery Shadows: 309 complete pairs.
- Verified frames: 1,209,590.
- Item events: 1,208,450.
- Live subjects reconstructed from authority: 1,203,046.
- Authoritative bytes covered by full scrub: 2,594,477,713.
- Scrub target coverage: 100%.
- Holes, damaged units, scrub failures, and open findings: all zero.

No Residiuum data damage was detected.

The source contains one orphan `*.rsh.dual.tmp` without a published segment or
final Shadow. This is recoverable interrupted staging, not authority. A normal
writable open of the clone removed it through protected-pair recovery.

## Derived-state finding

The source `primary.idx` is a 207,538,893-byte v3 checkpoint with a stale sealed
fingerprint. It contains 1,178,263 entries and cannot cover the 1,203,046 live
subjects reconstructed from authority. Rebuilding it is correct and does not
indicate data loss.

## Startup defect exposed by this database

After upgrading the clone to a valid 211,957,436-byte v4 checkpoint, a clean
development open took 45.07 seconds:

| Phase | Time |
|---|---:|
| Tier state | 39.10 s |
| Primary checkpoint | 5.78 s |
| Remaining phases | < 0.2 s |

Tier discovery hashed every immutable sealed segment before noticing that its
persisted placement was still valid. Segment-catalog reconstruction then
frame-scanned every segment despite a matching prior summary. This was an
unnecessary O(retained bytes) clean-open path.

The fix checks existing placements before hashing and reuses a prior segment
summary only when identity, tier, availability, file size, and verified content
hash agree. Explicit scrub remains responsible for deliberate whole-media
rehashing.

After the fix, clean development opens took 5.53–5.97 seconds and tier
state fell to 0.06–0.07 seconds. The remaining development-build cost was
measured as checkpoint decode (5.01 s), projection clone (0.25 s), and catalogue
derivation (0.17 s).

The latest client-qualification open took 5.53 seconds: 4.83 seconds decoding
the 211,957,436-byte checkpoint, 0.21 seconds installing the live/durable
projections, and 0.17 seconds deriving collection catalogues. It examined 310
segments but performed zero full-scan and active-replay bytes.

An optimized build opened the same clone in 692 ms:

| Phase | Time |
|---|---:|
| Tier state | 48 ms |
| Primary checkpoint total | 552 ms |
| Checkpoint read/decode | 392 ms |
| Projection clone | 81 ms |
| Catalogue derivation | 57 ms |

The report now exposes those three index subphases separately.

## Verification

- `cargo test -p residiuum-store --lib`: 253 passed.
- Regression: `clean_rebuild_reuses_matching_immutable_summary`.
- `cargo check -p residiuum-sdk`: passed (pre-existing warnings only).
- `cargo test -p residiuum-sdk --lib`: 163 passed.
- `cargo test -p residiuum-sdk --test driver_embedded`: 3 passed, including
  the large single-document Gremlin profile.
- `cargo test -p residiuum-store --features legacy-raw-store --test
  driver_operation_recovery`: 4 passed, including source-reclaim compaction.
