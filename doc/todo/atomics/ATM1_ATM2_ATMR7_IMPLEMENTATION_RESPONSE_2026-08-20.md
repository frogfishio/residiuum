# ATM-1 / ATM-2 ATMR7 implementation response

Date: 2026-08-20

Authority: `ATOMICS_SPEC.md`, `ATOMICS_IMPLEMENTATION_PLAN.md`, the two
identical `spec/atomics/cbor-v1.json` copies, and `FORMAT_SPEC.md` §4.7.

This response closes the six requests in
`ATM1_ATM2_DEEP_REVIEW_CR_ATMR7_2026-08-20.md`. It deliberately does not turn
on `Capabilities::atomics`; governance still controls that capability gate.

## Stable coverage architecture

The checkpoint now distinguishes two media classes:

1. **Ordinary settled media** has a metadata-only frontier. The store has
   already classified it while it was new, and the maintenance fence prevents
   a segment containing outstanding Atomic authority from being settled.
   Reopening therefore inventories path and length but does not reread its
   payload. Sealed files remain inside the Store's immutable-segment trust
   boundary; out-of-band media verification belongs to the Store scrub.
2. **Atomic-bearing/unsettled media** has a complete 64-KiB block frontier plus
   leftover hash. Every covered byte is authenticated, every verification byte
   is charged, and the total normal recovery budget is two 64-MiB segments:
   one unsettled Atomic prefix and one newly discovered/tail segment.

If an ordinary tail first acquires Atomic evidence, the complete file is
promoted to authenticated Atomic coverage once. Missing or truncated
Atomic-bearing media retains its original authenticated frontier and enters
`coverage_degraded`. Normal issuance cannot treat that state as proof of
absence.

`ATCKP1` is version 10. Its covered-file row adds `atomic_evidence: u8`; finding
kind 8 is the global `Coverage` marker. Version 9 is intentionally rebuilt from
media rather than ambiguously upgraded.

## CR response matrix

| Request | Result | Delivery and proof |
|---|---|---|
| CR-ATMR7-001 | Closed | Ordinary retained history is metadata-only after initial classification; Atomic media remains fully authenticated and charged. `settled_ordinary_history_adds_no_reopen_reads` proves that doubling settled history adds zero scan/verification bytes. Active Atomic interior mutation remains detected. |
| CR-ATMR7-002 | Closed | Missing/truncated Atomic coverage persists an authenticated frontier. New IDs are refused while degraded; exact already-known retries remain eligible. Scrub checks exact length and every frontier hash, refuses arbitrary replacement, and cannot clear an unbound damaged record. Tests cover missing media, unseen-ID refusal, arbitrary same-length replacement, exact restoration, and reopen. |
| CR-ATMR7-003 | Closed | Outstanding state is derived from Atomic facts/degradation, never from the acceleration frontier alone. Ordinary put → empty stage open/reopen → seal and compact is green. Unreadable checkpoints and real Atomic evidence remain fail-closed. |
| CR-ATMR7-004 | Closed | Every catalogue mutation is preflighted against the stage's own checkpoint/work limits before append. Chunk plans use exact candidate checkpoint encoding plus conservative frame-frontier growth. Checkpoint overflow is an error, never a silent success. Tests prove one-over refusal without active-media growth and visible forced capacity failure before append. |
| CR-ATMR7-005 | Closed | Data-frame cells use distinct lower-level after-write and after-file-sync failpoints. Checkpoint/coordinator writes expose role-specific after-write, after-file-sync, after-rename, and after-directory-sync boundaries. Unsupported member-scoped low-level cells were removed instead of aliased. A real child process abort after synced prepare reopens to the same projection as the model. |
| CR-ATMR7-006 | Closed by final handoff | The companion ATMR7 handoff records the clean implementation commit, run manifest, digest, command count, and acceptance labels after the clean verifier run. |

## Acceptance tests added or strengthened

- retained ordinary history has constant zero payload reads after qualification;
- empty stage checkpoints remain quiescent across process reopen;
- missing Atomic media blocks unseen identities;
- arbitrary same-length replacement fails authenticated scrub;
- exact media restoration passes scrub and persists healthy coverage;
- unbound Atomic damage cannot be scrubbed into healthy absence;
- custom checkpoint limits refuse chunk metadata before active media changes;
- injected checkpoint-capacity failure is returned before append;
- every retained matrix phase names a unique failpoint; and
- a subprocess `Abort` after prepare file sync recovers `Prepared`, with no
  ordinary visibility.

## Governance boundary

ATM-1 remains accepted. This package makes ATM-2 an acceptance candidate for
the delivered staging scope. It does not claim ATM-3 publication or ATM-4
retirement/compaction semantics, and it does not enable the public capability.
