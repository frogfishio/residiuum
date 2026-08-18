# ATM-1 / ATM-2 package handoff

**Superseded by `ATM1_ATM2_HANDOFF_2026-08-18.md`.** Store writers emit
operation identity at 41/42, not 31/32. Do not use this file as the live handoff.

Date: 2026-08-16  
Source: `ATOMICS_IMPLEMENTATION_PLAN.md` §§13–14; CR-ATM2-006  
Capability: `Capabilities::atomics` remains **false**.

Dirty-tree evidence produced by `scripts/verify-atomics.sh` is **diagnostic
only** and is not the accepted package record.

## Package and commit

| Field | Value |
| --- | --- |
| Packages | ATM-1 (compiler/validation), ATM-2 (staging/evidence prototype) |
| Verifier | `scripts/verify-atomics.sh {quick,crash,model,full}` |
| Manifests | `target/atomics-evidence/atm-1/manifest.json`, `atm-2/manifest.json`, `runs/<profile>.json` |
| Commit | recorded in those manifests at generation time |
| Acceptance | not requested; CRs are in_review |

## Implemented requirements

### ATM-1

- Immutable closed `AtomicPlan`, canonical target order, typed encodings.
- Closed-plan validator shared with the serial oracle.
- Heap-bound builder: rights union, deadline/limit checks, read witnesses.
- CR-ATM1-001: `HeapAuthorityRevision` is not `active_rule_revisions`.
- CR-ATM1-002: `EncodingProfile` on the trusted collection handle; noncanonical integer/decimal refused before prepare.

### ATM-2

- Format envelope registry: ownership 31–36, Atomic 37–40; proposed operation identity 41/42 (not emitted by Atomic/ownership helpers).
- Composable Atomic frames carry heap 31/34 so `admit_frame_to_heap` can bind them.
- Recovery reader decodes frozen `AtomicPrepare` / `AtomicMember` / `AtomicDecision` bodies.
- In-memory `StagingHeap` remains the reference model (member hash + payload bound; chunk limits; empty-value completeness).
- CR-ATM2-001: peer crate `residiuum-atomic-lane` (Law 9) — file-backed coordinator/member logs, `fsync`, directory-image crash prefixes. **Not** `residiuum-store` / sdk / perf.

## Changed durable / public formats

- Envelope keys 37–40 reserved for Atomic linkage (`ENV_ATOMIC_*`).
- Ownership parser ignores the Atomic namespace; still rejects malformed 31–36 and unknown keys above 40.
- Proposed relocation of client operation identity to 41/42. **Architect-unaccepted.** Store item frames may still emit 31/32 as operation identity.

No public SDK Atomic API. No capability advertisement.

## Tests and evidence manifest

`quick` (normal CI): ATM-ENC / ATM-ORA / ATM-AUT / ATM-ISO plus format admit/recovery.

`crash` (scheduled): `residiuum-atomic-lane` crash-reopen prefixes + in-memory failpoints.

`model` (scheduled): oracle / validator / ATM-0 evidence recompute + staging kernel.

`full`: quick + crash + all-targets on `residiuum-atomics` and `residiuum-atomic-lane`.

Families **not** claimed here: ATM-DMG, ATM-RET, ATM-MNT, ATM-APP, ATM-PERF (ATM-3+).

## Negative controls / mutants

| Family | Control (must fail if the rule is removed) |
| --- | --- |
| ATM-ENC | `hostile_corpus_covers_required_families_and_refuses`; noncanonical integer/decimal key refusal |
| ATM-ORA | `one_unit_over_limit_is_refused`; `validator_is_sensitive_to_single_field_flips` |
| ATM-AUT | `cross_heap_collection_is_refused_and_produces_no_plan` |
| ATM-ISO | `second_heap_cannot_resolve_first_atomic` |
| ATM-CRS | `negative_control_detects_a_leaked_staged_member` |

The verifier greps these needles and fails if they disappear.

## Known residuals

- ATM-2 is a **prototype / peer-crate lane**, not a delivered store package.
- No `residiuum-store` / `residiuum-sdk` / `residiuum-examine` integration.
- RQL, history, watch, and secondary-index invisibility are not proven on live store surfaces.
- FORMAT_SPEC amendment (31–42) is proposed, not architect-accepted.
- ATM-3 must not merge store publication on this staging contract until architects accept the CRs.
- ATM-DMG / RET / MNT / APP / PERF evidence families are out of scope.

## Performance change

None claimed. Lane uses per-append `sync_all` (correctness first). ATM-5 owns the “no per-member fsync / ≤5% ordinary-write regression” bar.

## Recovery / compatibility impact

- Old ownership readers that reject keys >36 cannot admit new Atomic frames.
- New readers ignore Atomic 37–40 on ordinary Heap-owned frames.
- Torn last log frame is a salvage hole; reopen reconstructs only verified prepare/member prefixes.
- First stable boundary: `sealed/<atomic_id>` after `sync_all` of `coordinator.log` and every `shard-*.log`.
- Crash before decision leaves no ordinary-visible mutation on the lane kernel (`get` / `scan`).

## Requested architecture decisions

1. Accept or reject the FORMAT_SPEC envelope-key amendment (ownership 31–36, Atomic 37–40, operation identity 41/42).
2. Accept Law 9 peel (`residiuum-atomic-lane`) as the ATM-2 durable-staging home, versus requiring the same work inside `residiuum-store`.
3. Do not treat ATM-2 as store-complete until store wiring, examine projection, and ordinary-surface negatives exist.
4. Keep `Capabilities::atomics == false` until ATM-5 acceptance.
