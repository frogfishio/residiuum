# ATM-1 / ATM-2 package handoff (ATMR4)

Date: 2026-08-19
Supersedes: `ATM1_ATM2_HANDOFF_2026-08-18.md` (ATMR3-era story; keep as history).
Source: `ATOMICS_IMPLEMENTATION_PLAN.md` §§13–14; `ATM1_ATM2_DEEP_REVIEW_CR_ATMR4_2026-08-18.md`.
Capability: `Capabilities::atomics` remains **false**.

Invoke the verifier as `bash scripts/verify-atomics.sh {quick,crash,model,full}`.
The script is also executable. Dirty-tree records are **diagnostic** only.

## Package and commit

| Field | Value |
| --- | --- |
| Packages | ATM-1 (compiler/validation), ATM-2 (staging/evidence prototype) |
| Verifier | `bash scripts/verify-atomics.sh {quick,crash,model,full}` |
| Suite | `atm-1-atm-2-atmr4-2026-08-19` |
| Manifests | `target/atomics-evidence/atm-1/manifest.json`, `atm-2/manifest.json`, `runs/<commit12>-<profile>.json` |
| HEAD at write | `86d179665088` (working tree is dirty with ATMR4 labor; not an accepted package record) |
| ATM-1 label | may become `acceptance_candidate` on a **clean** `full` run with ENC/ORA/AUT/RES + format `--all-targets` |
| ATM-2 label | **`partial` while `not_store = true`** or any mandatory deliverable is absent |
| Run-level label | worse of the two packages (cannot upgrade ATM-2) |

This handoff is written on a dirty tree. A later clean-commit `full` run is
required before any ATM-1 `acceptance_candidate` record is governance-usable.

## Architect order delivered (labor `in_review`, not architect-accepted)

| CR | Title | Labor disposition |
| --- | --- | --- |
| CR-ATMR4-001 | Exact closed member set at plan-to-prepare | `in_review` |
| CR-ATMR4-002 | Authenticated checkpoint; never invent sealed | `in_review` |
| CR-ATMR4-003 | Checkpoint open must not full-scan historical logs | `in_review` |
| CR-ATMR4-004 | Chunked members on the same member-log authority | `in_review` |
| CR-ATMR4-005 | One store-owned authority, not dual-write | `in_review` |
| CR-ATMR4-006 | Exclusive publish must not poison same-ID retry | `in_review` |
| CR-ATMR4-007 | Size-check every recovery sidecar before allocate | `in_review` |
| CR-ATMR4-008 | Deterministic I/O matrix with exact cell outcomes | `in_review` |
| CR-ATMR4-009 | Store visibility and independent examination matrix | `in_review` |
| CR-ATMR4-010 | Green full verifier and this regenerated handoff | this card |

Principal/human accepts `done`. Labor does not.

## Implemented requirements

### ATM-1

- Immutable closed `AtomicPlan`, canonical target order, typed encodings.
- Closed-plan validator shared with the serial oracle.
- Heap-bound builder: rights union, deadline/limit checks, read witnesses.
- `HeapAuthorityRevision` is not `active_rule_revisions`.
- `EncodingProfile` on the trusted collection handle; noncanonical integer/decimal refused.
- Prepare is derived from the admitted closed plan (`prepare_from_closed_plan`).
- **CR-ATMR4-001:** leftover unused members at `bind_members_to_plan` /
  `prepare_from_closed_plan` are `MalformedInput`; duplicate identity is
  `DuplicateTarget`. Durable closure is the exact admitted member set.

### ATM-2 (prototype / peer crate + store-owned first slice)

- Format envelope registry: ownership **31–36**, Atomic **37–40**, operation identity **41/42**.
- Store writers emit 41/42 only. Readers still accept a legacy 31/32 operation pair when key 32 is a 32-byte content hash.
- Recovery reader decodes frozen `AtomicPrepare` / `AtomicMember` / `AtomicDecision`.
- `StagingHeap` remains the reference model (member hash + payload; chunk maps).
- `residiuum-atomic-lane` (Law 9): plan sidecar, intent, payload, chunk-manifest/chunk, coordinator/shard logs, seals, checkpoint, exclusive writer lock.
- **CR-ATMR4-005:** store `StoreAtomicStage` no longer dual-writes a `DurableLane`. StagingHeap + store frames; prepare/payload/seal sidecars are `ATPREP1` / `ATPAY1` / `ATSEAL1`. Store open rebuild does not resurrect `BatchPrepare` from those frames — reopen uses the prepare sidecar.
- **CR-ATMR4-006:** exclusive publish is unique temp + `hard_link` (no-replace) + torn quarantine. Same-ID retry after a short write is not poisoned. Default parallel `cargo test` holds a process-global `io_fail::serial_guard` in `exclusive_publish` and `io_prefix_matrix`; no reviewer-only `--test-threads=1` flag.
- **CR-ATMR4-007:** every recovery sidecar is size-checked against `SidecarRole::max_bytes` / `RecoveryLimits.max_sidecar_bytes` before `fs::read`.
- **CR-ATMR4-002:** checkpoint v2 is BLAKE3 over prefix hashes. Seal is only from the seal sidecar, never invented from a checkpoint flag. Mutated intent on reopen is `Kernel` or `Corrupt`.
- **CR-ATMR4-003:** a covering v2 checkpoint opens from authenticated tails; it does not full-scan historical logs (`covered_prefix_larger_than_budget_opens_from_tails`). v1 checkpoints are `Legacy` and rebuild.
- **CR-ATMR4-004:** chunk-complete members persist an `ItemEvent` on the shard log. Seal requires the shard member frame. ChunkPlan `total` must be ≥ 2.
- **CR-ATMR4-008:** `io_prefix_matrix` names exact cell outcomes (presence / torn / retry / invisibility) and serializes failpoint tables.
- **CR-ATMR4-009:** store `atomic_stage_invisibility` covers get/scan/history/secondary emptiness before decision.

## Changed durable / public formats

- Envelope keys 37–40 reserved for Atomic linkage (`ENV_ATOMIC_*`).
- Client `operation_id` / `operation_content_hash` live at 41 / 42.
- Store-owned Atomic sidecars use `ATPREP1` / `ATPAY1` / `ATSEAL1` magic.
- Checkpoint file is v2 (checksum + prefix hashes). v1 is rebuilt, not trusted as covering.
- Exclusive sidecar publish is hard-link from a unique temp; a torn dest is quarantined.

No public SDK Atomic API. No capability advertisement.

## Tests and evidence matrix

`quick` (normal CI): ATM-ENC / ATM-ORA / ATM-AUT / ATM-ISO plus format admit/recovery.

`crash` (scheduled): lane crash-reopen + durable_chunks + honest_damage + exclusive_writer + exclusive_publish + io_prefix_matrix + in-memory failpoints.

`model` (scheduled): oracle / validator / ATM-0 evidence + staging kernel.

`full`: quick + crash + format `--all-targets` + store `envelope` lib tests + store `atomic_stage_invisibility` + all-targets on atomics/lane + fmt/clippy `-D warnings` on those crates.

Families **not** claimed: ATM-DMG / RET / MNT / APP / PERF as ATM-4/ATM-5 store material-truth families. Lane honest-damage is CRS, not ATM-4 salvage.

## Negative controls / mutants

| Family | Control |
| --- | --- |
| ATM-ENC | `hostile_corpus_covers_required_families_and_refuses` |
| ATM-ORA | `one_unit_over_limit_is_refused` |
| ATM-AUT | `cross_heap_collection_is_refused_and_produces_no_plan` |
| ATM-ISO | `second_heap_cannot_resolve_first_atomic` |
| ATM-CRS | `negative_control_detects_a_leaked_staged_member` |
| ATM-CRS | omit-file-sync / omit-rename / omit-dir-sync mutants in `io_prefix_matrix` |
| ATM-CRS | short-write then same-ID retry in `exclusive_publish` |

## Known residuals

- ATM-2 is **not** an accepted store durability contract. `not_store = true` forces `partial`.
- Store open still drops `BatchPrepare` from log rebuild; ATPREP1 is the reopen catalog. Not a dual-write restoration.
- RQL, watch, `residiuum-examine` projection, Recovery Shadow, backup/restore/clone, and segment rotation/cohort isolation remain open.
- FORMAT_SPEC 31–42 amendment is recommended, not architect-accepted.
- ATM-3 must not consume this lane or `StoreAtomicStage` as accepted storage.
- CR-ATMR4-001..010 are labor `in_review` (010 after this handoff + green `full`); principal/human accepts `done`.
- This handoff was written on a dirty tree at `86d179665088`. A clean-commit `full` run is required before any ATM-1 `acceptance_candidate` record is governance-usable.

## Performance change

None claimed. Lane uses per-append `sync_all` (correctness first). ATM-5 owns the “no per-member fsync / ≤5% ordinary-write regression” bar.

## Recovery / compatibility impact

- New Atomic frames carry ownership 31/34 plus Atomic 37–39 so `admit_frame_to_heap` can bind them.
- Store writers no longer emit operation identity at 31/32.
- Torn unacked log tail is not clean absence; acknowledged holes are `Coverage`/`Corrupt`.
- Checkpoint v2 + streamed tails bound reopen; mutated plan sidecar cannot reconstruct prepare; checkpoint cannot invent sealed.
- Chunked members recover from `chunk-manifest/` + `chunk/` **and** a completed shard `ItemEvent`, not assembled payload alone.
- Crash / injected I/O before decision leaves no ordinary-visible mutation on `get` / `scan` (lane) or store get/scan/history/secondary (first store slice).
- Oversized sidecars refuse before allocate. Exclusive publish retry after a torn dest does not require deleting a same-path poison file.

## Requested architecture decisions

1. Record acceptance of FORMAT_SPEC keys 31–36 / 37–40 / 41–42.
2. Keep `residiuum-atomic-lane` as mechanics/test-oracle; `residiuum-store` owns authoritative ATM-2 paths. Do not reintroduce dual-write.
3. Do not treat ATM-2 as accepted until 005/009 residuals (RQL/watch/examine/rotation) close and `not_store` can be lifted honestly.
4. Keep `Capabilities::atomics == false` until ATM-5 acceptance.
5. Commit the dirty ATMR4 tree, then re-run `bash scripts/verify-atomics.sh full` on that clean commit if a governance-usable ATM-1 record is wanted.
