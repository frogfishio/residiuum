# ATM-1 / ATM-2 package handoff (ATMR5)

Date: 2026-08-19
Supersedes: `ATM1_ATM2_HANDOFF_ATMR4_2026-08-19.md` (keep as history).
Source: `ATOMICS_IMPLEMENTATION_PLAN.md` §§13–14; `ATM1_ATM2_DEEP_REVIEW_CR_ATMR5_2026-08-19.md`.
Capability: `Capabilities::atomics` remains **false**.

Invoke the verifier as `bash scripts/verify-atomics.sh {quick,crash,model,full}`.
Dirty-tree records are **diagnostic** only.

## Package and commit

| Field | Value |
| --- | --- |
| Packages | ATM-1 (compiler/validation), ATM-2 (staging/evidence; **not accepted**) |
| Verifier | `bash scripts/verify-atomics.sh {quick,crash,model,full}` |
| Suite | `atm-1-atm-2-atmr5-2026-08-19` |
| Manifests | `target/atomics-evidence/atm-1/manifest.json`, `atm-2/manifest.json`, `runs/<commit12>-<profile>.json` |
| ATM-1 label | technically accepted at `00a06ae` for compiler/validation; `full` may record `acceptance_candidate` on a **clean** tree |
| ATM-2 label | **`partial`** while `not_store = true` **or** any assembler-listed mandatory/residual blocker remains |
| Run-level label | worse of the two packages (cannot upgrade ATM-2) |

ATM-2 is **not** an accepted store durability contract. ATM-3 must not consume
`StoreAtomicStage` or `residiuum-atomic-lane` as accepted storage.

## Architect order (labor `in_review`, not architect-accepted)

| CR | Title | Labor disposition |
| --- | --- | --- |
| CR-ATMR5-001 | Bounded store Atomic catalogue | `in_review` |
| CR-ATMR5-002 | Honest store damage/conflict classifier | `in_review` |
| CR-ATMR5-003 | Exact same-ID retry on store staging | `in_review` |
| CR-ATMR5-004 | Durable coordinator sequence | `in_review` |
| CR-ATMR5-005 | Durable partial chunks and frozen manifest | `in_review` |
| CR-ATMR5-006 | One authoritative prepare record | `in_review` |
| CR-ATMR5-007 | No prefix-guess exclusive overwrite | `in_review` |
| CR-ATMR5-008 | Incremental authenticated checkpoint frontier | `in_review` |
| CR-ATMR5-009 | Store-authority I/O and crash matrix | `in_review` |
| CR-ATMR5-010 | Honest verifier and this regenerated handoff | this card |

Principal/human accepts `done`. Labor does not.

## What is proven where

### Peer lane (`residiuum-atomic-lane`) — mechanics / test-oracle

Proven by lane tests and `crash`/`full` ATM-CRS commands:

- Exclusive persist: unique temp + no-replace. Empty or shorter-prefix finals
  are preserved as damage/conflict, not overwritten (CR-ATMR5-007).
- Checkpoint **v3**: incremental chain digest + head/tail marks + 64 KiB block
  hashes. Open verifies constant-size marks and **charges** those bytes.
  Persist hashes only `[old_offset, new_offset)`. v1/v2 images rebuild once.
  This **replaces** the ATMR4 claim that open rereads the full covered prefix
  to authenticate a hash.
- I/O prefix matrix generates **all** `Chunk` cells plus `Chunk`/`Checkpoint`
  (not a single sentinel).
- omit-file-sync / omit-dir-sync mutants still fail on **missing visit
  counters**, not by deleting unsynced media. That remains a residual.

### Store authority (`StoreAtomicStage`) — authoritative ATM-2 path

Proven by store tests now required on `crash`/`full`:

| Test | What it proves |
| --- | --- |
| `atomic_stage_bounded` | Catalogue open is checkpoint + tails, not a per-op full scan (001) |
| `atomic_stage_classify` | Damage/conflict/foreign evidence is classified, not last-wins (002) |
| `atomic_stage_retry` | Exact unchunked/chunked retry is idempotent; mutations refuse (003) |
| `atomic_stage_coordinator` | Coordinator sequence survives reopen opposite Atomic-ID order (004) |
| `atomic_stage_chunks` | `ATMAP1`/`ATCHK1` retain partial prefixes after reopen (005) |
| `atomic_stage_prepare_authority` | One `BatchPrepare`; no ATPREP1 dual-write (006) |
| `atomic_stage_io_matrix` | prepare/member/chunk/seal failpoint prefixes + reopen class (009) |
| `atomic_stage_invisibility` | get/scan/history/secondary stay empty |

Store exclusive prefix-guess is the **peer-lane** persist path (007). Store
frames append; they do not use `write_exclusive`.

### ATM-1

Unchanged: compiler/validator core accepted at `00a06ae`. Do not reopen unless
plan, authority, encoding, or limits change.

## Changed durable / public formats (store + lane)

- Store chunk map/body sidecars: `ATMAP1` / `ATCHK1` (plus existing `ATPAY1` /
  `ATSEAL1` / `BatchPrepare`).
- Store Atomic checkpoint is v5 (plans + chunk bodies + coordinator seq).
- Peer-lane checkpoint is v3 (incremental frontier). v2 is `Legacy`.
- Exclusive lane publish no longer quarantines a strict prefix as torn.

No public SDK Atomic API. No capability advertisement.

## Tests and evidence matrix

`quick`: ATM-ENC / ATM-ORA / ATM-AUT / ATM-ISO plus format admit/recovery.
Does **not** claim store ATMR5 proofs.

`crash`: lane crash-reopen, durable_chunks, honest_damage, exclusive_writer,
exclusive_publish, io_prefix_matrix, **plus** store retry / chunks / I/O matrix.

`model`: oracle / validator / ATM-0 evidence + staging kernel.

`full`: quick + crash + format `--all-targets` + store envelope lib tests +
store invisibility + **all ATMR5 store staging tests listed above** + scoped
`rustfmt --check` on store `atomic_stage*` sources + all-targets on
atomics/lane + fmt/clippy `-D warnings` on atomics/format/lane.

**Not** run as an Atomics gate: `cargo clippy -p residiuum-store -- -D warnings`
(pre-existing store warnings). That is an explicit ATM-2 residual blocker.

Families **not** claimed: ATM-DMG / RET / MNT / APP / PERF as ATM-4/ATM-5
store material-truth families.

## Negative controls / mutants

| Family | Control |
| --- | --- |
| ATM-ENC | `hostile_corpus_covers_required_families_and_refuses` |
| ATM-ORA | `one_unit_over_limit_is_refused` |
| ATM-AUT | `cross_heap_collection_is_refused_and_produces_no_plan` |
| ATM-ISO | `second_heap_cannot_resolve_first_atomic` |
| ATM-CRS | `negative_control_detects_a_leaked_staged_member` |
| ATM-CRS | `leak_negative_control_is_visible_on_each_surface` (store) |
| ATM-CRS | omit-file-sync / omit-rename / omit-dir-sync **visit** mutants (not media loss) |

## Known residuals (also ATM-2 assembler blockers)

- `not_store = true`. Store staging is prototype/authority-in-progress.
- ATM-3 must not consume this path.
- RQL, watch, `residiuum-examine`, Recovery Shadow, backup/restore/clone.
- Store rotation / compaction / pending-seal not in the Atomic I/O matrix.
- omit-sync mutants do not drop unsynced bytes.
- Store-wide Clippy `-D warnings` is not in the Atomics package gate.
- Checkpoint still rewrites the full Atomic summary list on each persist.
- CR-ATMR5-001..010 are labor `in_review`; principal/human accepts `done`.

## Performance change

None claimed. Lane still per-append `sync_all`. ATM-5 owns the regression bar.

## Requested architecture decisions

1. Keep ATM-1 accepted at `00a06ae` unless plan/authority/encoding/limits change.
2. Keep `residiuum-atomic-lane` as mechanics/test-oracle; store owns
   authoritative ATM-2 paths. Do not reintroduce dual-write.
3. Do not lift `not_store` or treat ATM-2 as accepted until the residuals
   above close and a clean-commit `full` run lists no mandatory gaps.
4. Keep `Capabilities::atomics == false` until ATM-5 acceptance.
5. A clean-commit `bash scripts/verify-atomics.sh full` is required before any
   governance-usable ATM-1 `acceptance_candidate` record.
