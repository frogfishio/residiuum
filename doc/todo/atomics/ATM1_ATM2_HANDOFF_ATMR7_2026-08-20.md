# ATM-1 / ATM-2 handoff — ATMR7 stable baseline

Date: 2026-08-20

## Exact implementation baseline

- commit: `5f1c3c7168a66a5f7b5c7fdf95598cd564327bbb`
- subject: `stabilize atomic stage recovery and admission`
- worktree during verification: clean
- verifier: `bash scripts/verify-atomics.sh full`
- result: `pass`
- command count: 31
- failed commands: 0
- run: `target/atomics-evidence/runs/5f1c3c7168a6-full.json`
- run SHA-256:
  `b809c687362bd816bc4fade41ee0217f076c5c4cfceb30a3778048910e0afc75`

Generated manifests:

- `target/atomics-evidence/atm-1/manifest.json` —
  `acceptance_candidate`;
- `target/atomics-evidence/atm-2/manifest.json` — `partial` because the public
  Atomics capability remains deliberately disabled; and
- both manifests name the exact clean commit above.

The earlier dirty run
`target/atomics-evidence/runs/5b97c077a073-full.json` remains diagnostic
history only. It is not acceptance evidence.

## Delivery decision

All active `CR-ATMR7-*` implementation requests are addressed at the baseline
above. The detailed architecture and response matrix are in
`ATM1_ATM2_ATMR7_IMPLEMENTATION_RESPONSE_2026-08-20.md`.

The delivered ATM-2 staging scope is now a governance acceptance candidate:

- normal reopen cost is independent of settled ordinary payload size after
  initial classification;
- Atomic-bearing/unsettled coverage remains byte-authenticated and bounded;
- incomplete coverage closes unseen-ID issuance and cannot be cleared by path
  existence or arbitrary replacement;
- empty inspection remains quiescent and does not disable maintenance;
- all catalogue/checkpoint growth is admitted before authoritative append;
- checkpoint capacity failure is visible rather than silently stale;
- crash phases are no longer aliases; and
- a real process-abort sentinel agrees with the in-process crash model.

`Capabilities::atomics` remains `false`. Turning it on is a separate governance
decision and must not be inferred from this handoff. ATM-3 publication and ATM-4
qualified retirement/compaction remain outside this delivery.

## Compatibility note

`ATCKP1` is now version 10 with domain
`RESIDIUUM-STORE-ATOMIC-STAGE-CKP-V10`. Version 9 checkpoints are deliberately
treated as acceleration misses and rebuilt from authoritative media. The two
Atomics spec bundles are byte-identical and `FORMAT_SPEC.md` §4.7 freezes the
new row layout and finding code.
