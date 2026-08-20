# ATM-1 / ATM-2 package handoff (ATMR6)

Date: 2026-08-20
Supersedes: `ATM1_ATM2_HANDOFF_ATMR5_2026-08-19.md` (keep as history).
Source: `ATOMICS_IMPLEMENTATION_PLAN.md` §§13–14; `ATM1_ATM2_DEEP_REVIEW_CR_ATMR6_2026-08-20.md`.
Capability: `Capabilities::atomics` remains **false**.

Invoke the verifier as `bash scripts/verify-atomics.sh {quick,crash,model,full}`.
Dirty-tree records are **diagnostic** only. Acceptance-candidate ATM-1 and a
truthful ATM-2 `partial` record require a **clean** tree (`git status` empty)
and `full` exit 0 on that commit.

## Package and commit

| Field | Value |
| --- | --- |
| Packages | ATM-1 (compiler/validation), ATM-2 (staging/evidence; **not accepted**) |
| Verifier | `bash scripts/verify-atomics.sh {quick,crash,model,full}` |
| Suite | `atm-1-atm-2-atmr6-2026-08-20` |
| Manifests | `target/atomics-evidence/atm-1/manifest.json`, `atm-2/manifest.json`, `runs/<commit12>-<profile>.json` |
| ATM-1 label | technically accepted at `00a06ae` for compiler/validation; `full` may record `acceptance_candidate` on a **clean** tree |
| ATM-2 label | **`partial`** while `not_store = true` **or** any assembler-listed residual remains |
| Run-level label | worse of the two packages (cannot upgrade ATM-2) |

ATM-2 is **not** an accepted store durability contract. ATM-3 must not consume
`StoreAtomicStage` or `residiuum-atomic-lane` as accepted storage.

## Architect order (labor `in_review`, not architect-accepted)

| CR | Title | Labor disposition |
| --- | --- | --- |
| CR-ATMR6-001 | Verify every covered prefix block | `in_review` |
| CR-ATMR6-002 | Persist damage, orphans, and seal conflicts | `in_review` |
| CR-ATMR6-003 | Persist seal before DurableInvisible | `in_review` |
| CR-ATMR6-004 | Operable limits and incremental catalogue | `in_review` |
| CR-ATMR6-005 | Project surviving prepare, not absence | `in_review` |
| CR-ATMR6-006 | Freeze staging format; fence maintenance | `in_review` |
| CR-ATMR6-007 | True crash-media store I/O matrix | `in_review` |
| CR-ATMR6-008 | Green clean-commit verifier and this handoff | this card |

Principal/human accepts `done`. Labor does not.

ATM-1 semantic acceptance at `00a06ae` is **not** reopened.

## Response matrix (CR-ATMR6-*)

| CR | Code | Tests | Durable format |
| --- | --- | --- | --- |
| 001 | `atomic_stage_recover.rs` block frontier verify; lane `verify_prefix_blocks` | `atomic_stage_bounded`, lane `honest_damage` (6/6) | checkpoint v6+ block hashes |
| 002 | `atomic_stage_classify.rs` persist findings/orphans/seal-block | `atomic_stage_classify` | checkpoint v7 findings |
| 003 | persist-then-apply `seal_member_boundary` | `atomic_stage_seal` | ATSEAL1 before kernel apply |
| 004 | `AtomicStageLimits::operable`, `BodyRef` locators | `atomic_stage_limits` | checkpoint v8 locators |
| 005 | `atomic_stage_status.rs` `examine` | `atomic_stage_status` | checkpoint v9 `intended_members` |
| 006 | fail-closed seal/compact/reclaim/clone; FORMAT_SPEC §4.7 | `atomic_stage_maintenance` | ATCKP1 v9 / ATCRD1 / sidecar magics frozen |
| 007 | crash-media Panic + abandon + mutants; member frame cells | `atomic_stage_io_matrix` | persist failpoints `checkpoint.*` / `coord.*` |
| 008 | `scripts/verify-atomics.sh`; this handoff | `full` exit 0 when commands pass | suite `atm-1-atm-2-atmr6-2026-08-20` |

## What is proven where

### Peer lane (`residiuum-atomic-lane`) — mechanics / test-oracle

- Exclusive persist, incremental checkpoint v3, I/O prefix matrix.
- Honest-damage 6/6 green (covered-prefix block verify; CR-ATMR6-001).
- `CheckpointLoad::Ready` is boxed (strict clippy `large_enum_variant`).
- `cargo fmt --check` includes `checkpoint.rs`.

### Store authority (`StoreAtomicStage`) — authoritative ATM-2 path

| Test | What it proves |
| --- | --- |
| `atomic_stage_bounded` | Covered prefixes re-hashed; interior flip is not healthy (001) |
| `atomic_stage_classify` | Damage/orphan/seal-block persist across reopen (002) |
| `atomic_stage_retry` | Exact same-ID retry (ATMR5-003) |
| `atomic_stage_coordinator` | Durable coordinator sequence (ATMR5-004) |
| `atomic_stage_chunks` | Durable chunk prefixes (ATMR5-005) |
| `atomic_stage_prepare_authority` | One BatchPrepare (ATMR5-006) |
| `atomic_stage_seal` | Persist seal before DurableInvisible (003) |
| `atomic_stage_limits` | Operable limits; locators not full payload rewrite (004) |
| `atomic_stage_status` | Surviving prepare is Prepared, not absence (005) |
| `atomic_stage_maintenance` | Fail-closed seal/compact/reclaim/clone; backup copies ckpt (006) |
| `atomic_stage_io_matrix` | Crash-media cells; Member is the member frame; one projection (007) |
| `atomic_stage_invisibility` | get/scan/history/secondary stay empty |

### ATM-1

Compiler/validation core unchanged. `full` ENC/ORA/AUT/RES + format
`--all-targets` may label ATM-1 `acceptance_candidate` on a **clean** tree.

## Residuals (truthful — only genuinely absent/deferred)

- ATM-2 remains `not_store=true`; not an accepted durability contract.
- ATM-3 must not consume `StoreAtomicStage` or the peer lane.
- RQL / watch / `residiuum-examine` store surfaces are untested.
- Recovery Shadow of Atomic-bearing actives is **fail-closed** (006) until ATM-4 copy-through.
- Multiprocess `crash_child` Abort is not an Atomic staging cell (007 is in-process crash-media).
- Store-wide `clippy -D warnings` is not an Atomics gate.

Removed as false “missing” claims (commands now exist and must pass on `full`):

- peer-lane honest-damage tests
- scoped store Atomic rustfmt
- backup/restore/clone (006 tests)
- omit-sync mutants that only checked visit counters (007 mutates media)

## How to regenerate evidence

```bash
bash scripts/verify-atomics.sh full
```

On a dirty tree the run exits 0 only if every recorded command exits 0; the
manifest is still labeled **diagnostic**. After the principal commits this
tree, re-run `full` on that SHA so artifact hashes name the accepted commit.
Do not treat a dirty-tree manifest as the acceptance record.

Labor `full` evidence (dirty, **diagnostic**, 31/31 commands pass):

| Artifact | Value |
| --- | --- |
| Run | `target/atomics-evidence/runs/11d6f2e5060e-full.json` |
| Sidecar SHA-256 | `dd8a99bafeba146005f409c628063f5587439318598fc93c49bbb01fee78525b` |
| HEAD at run | `11d6f2e5060e` (working tree dirty) |
| Suite | `atm-1-atm-2-atmr6-2026-08-20` |