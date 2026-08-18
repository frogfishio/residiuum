# ATM-1 / ATM-2 package handoff (ATMR3)

Date: 2026-08-18
Supersedes: `ATM1_ATM2_HANDOFF_2026-08-16.md` (stale envelope and verifier story).
Source: `ATOMICS_IMPLEMENTATION_PLAN.md` §§13–14; CR-ATMR3-009.
Capability: `Capabilities::atomics` remains **false**.

Invoke the verifier as `bash scripts/verify-atomics.sh {quick,crash,model,full}`.
The script is also executable. Dirty-tree records are **diagnostic** only.

## Package and commit

| Field | Value |
| --- | --- |
| Packages | ATM-1 (compiler/validation), ATM-2 (staging/evidence prototype) |
| Verifier | `bash scripts/verify-atomics.sh {quick,crash,model,full}` |
| Suite | `atm-1-atm-2-atmr3-2026-08-18` |
| Manifests | `target/atomics-evidence/atm-1/manifest.json`, `atm-2/manifest.json`, `runs/<commit12>-<profile>.json` |
| HEAD at write | `108cb293d017` (working tree is dirty; not the accepted package record) |
| ATM-1 label | may become `acceptance_candidate` on a **clean** `full` run with ENC/ORA/AUT/RES + format `--all-targets` |
| ATM-2 label | **`partial` while `not_store = true`** or any mandatory deliverable is absent |
| Run-level label | worse of the two packages (cannot upgrade ATM-2) |

## Implemented requirements

### ATM-1

- Immutable closed `AtomicPlan`, canonical target order, typed encodings.
- Closed-plan validator shared with the serial oracle.
- Heap-bound builder: rights union, deadline/limit checks, read witnesses.
- `HeapAuthorityRevision` is not `active_rule_revisions`.
- `EncodingProfile` on the trusted collection handle; noncanonical integer/decimal refused.
- Prepare is derived from the admitted closed plan (`prepare_from_closed_plan`).

### ATM-2 (prototype / peer crate + store-owned first slice)

- Format envelope registry: ownership **31–36**, Atomic **37–40**, operation identity **41/42**.
- **Store writers emit 41/42 only.** Readers still accept a legacy 31/32 operation pair when key 32 is a 32-byte content hash; 31/32 as 16-byte collection ownership is never operation identity.
- Recovery reader decodes frozen `AtomicPrepare` / `AtomicMember` / `AtomicDecision`.
- `StagingHeap` remains the reference model (member hash + payload; chunk maps).
- `residiuum-atomic-lane` (Law 9): plan sidecar, intent, payload, chunk-manifest/chunk, coordinator/shard logs, seals, checkpoint, exclusive writer lock.
- Store-owned `StoreAtomicStage` appends unindexed Atomic frames; `get` / scan / history / secondary stay empty for staged material.
- CR-ATMR3 labor (in_review, not architect-accepted): plan-derived prepare; not-committed classification; honest damage; bounded reopen; durable chunks; exclusive writer; I/O-phase prefix matrix.

## Changed durable / public formats

- Envelope keys 37–40 reserved for Atomic linkage (`ENV_ATOMIC_*`).
- Client `operation_id` / `operation_content_hash` live at 41 / 42.
- Ownership parser ignores the Atomic namespace; rejects malformed 31–36 and unknown keys above 42 that it does not understand.
- Architect recommendation (ATMR3): approve 31–36 / 37–40 / 41–42 in FORMAT_SPEC. **Not yet a recorded architect acceptance.**

No public SDK Atomic API. No capability advertisement.

## Tests and evidence matrix

`quick` (normal CI): ATM-ENC / ATM-ORA / ATM-AUT / ATM-ISO plus format admit/recovery.

`crash` (scheduled): lane crash-reopen + durable_chunks + honest_damage + exclusive_writer + io_prefix_matrix + in-memory failpoints.

`model` (scheduled): oracle / validator / ATM-0 evidence + staging kernel.

`full`: quick + crash + format `--all-targets` + store `envelope` lib tests + store `atomic_stage_invisibility` + all-targets on atomics/lane + fmt/clippy on those crates.

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

## Known residuals

- ATM-2 is **not** an accepted store durability contract. `not_store = true` forces `partial`.
- RQL, watch, `residiuum-examine` projection, and segment rotation/cohort isolation remain open (CR-ATMR3-006 residuals).
- FORMAT_SPEC 31–42 amendment is recommended, not architect-accepted.
- ATM-3 must not consume this lane as accepted storage.
- CR-ATMR3-001..009 are labor `in_review`; principal/human accepts `done`.
- This handoff was written on a dirty tree at `108cb293d017`. A clean-commit `full` run is required before any ATM-1 `acceptance_candidate` record is governance-usable.

## Performance change

None claimed. Lane uses per-append `sync_all` (correctness first). ATM-5 owns the “no per-member fsync / ≤5% ordinary-write regression” bar.

## Recovery / compatibility impact

- New Atomic frames carry ownership 31/34 plus Atomic 37–39 so `admit_frame_to_heap` can bind them.
- Store writers no longer emit operation identity at 31/32.
- Torn unacked log tail is not clean absence; acknowledged holes are `Coverage`/`Corrupt`.
- Checkpoint + streamed tails bound reopen; mutated plan sidecar cannot reconstruct prepare.
- Chunked members recover from `chunk-manifest/` + `chunk/`, not assembled payload alone.
- Crash / injected I/O before decision leaves no ordinary-visible mutation on `get` / `scan` (lane) or store get/scan/history/secondary (first store slice).

## Requested architecture decisions

1. Record acceptance of FORMAT_SPEC keys 31–36 / 37–40 / 41–42.
2. Keep `residiuum-atomic-lane` as mechanics/test-oracle; `residiuum-store` owns authoritative ATM-2 paths.
3. Do not treat ATM-2 as accepted until 006 residuals close and `not_store` can be lifted honestly.
4. Keep `Capabilities::atomics == false` until ATM-5 acceptance.
