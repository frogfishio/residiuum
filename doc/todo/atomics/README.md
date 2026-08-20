# Atomics programme

Status: **active critical-path implementation; ATM-5 qualification in progress**

Execution authority:

1. [ATOMICS_SPEC.md](./ATOMICS_SPEC.md) — normative semantics, storage
   protocol, async product API, failure model, and conformance contract.
2. [ATOMICS_IMPLEMENTATION_PLAN.md](./ATOMICS_IMPLEMENTATION_PLAN.md) — current
   baseline, package ownership, PR order, gates, evidence, and governance.

Current continuation point:

- [ATM5_ASYNC_SDK_BASELINE_2026-08-20.md](./ATM5_ASYNC_SDK_BASELINE_2026-08-20.md)
  — first public async construction/commit/status slice, evidence and exact
  remaining release delta. Capability advertisement remains closed.

Historical acceptance reviews:

- [ATM1_ATM2_DEEP_REVIEW_CR_ATMR7_2026-08-20.md](./ATM1_ATM2_DEEP_REVIEW_CR_ATMR7_2026-08-20.md)
  — ATM-1 is accepted; ATM-2 remains partial at clean `5f90d59`; active changes
  use only the `CR-ATMR7-*` namespace.
- [ATM1_ATM2_DEEP_REVIEW_CR_ATMR6_2026-08-20.md](./ATM1_ATM2_DEEP_REVIEW_CR_ATMR6_2026-08-20.md)
  — superseded review retained as historical evidence.
- [ATM1_ATM2_HANDOFF_ATMR6_2026-08-20.md](./ATM1_ATM2_HANDOFF_ATMR6_2026-08-20.md)
  — current labor handoff. Dirty-tree verifier records are diagnostic.
- [ATM1_ATM2_DEEP_REVIEW_CR_ATMR5_2026-08-19.md](./ATM1_ATM2_DEEP_REVIEW_CR_ATMR5_2026-08-19.md)
  — superseded review retained as historical evidence.

Compatibility note:
[TRANSACTIONS.md](./TRANSACTIONS.md) explains how transaction terminology may
later project the Atomic contract. It is subordinate and is not an
implementation authority.

Historical rationale:
[ATOMICS_PROPOSAL.md](../../done/proposals/ATOMICS_PROPOSAL.md) is archived.
Developers must not implement from it.

Current product truth:

- key-local conditional writes and durable operation replay exist;
- physical group commit exists but its members are logically independent;
- the Heap-local Atomic kernel, durable evidence, publication, recovery,
  maintenance and serial-history proofs exist;
- an embedded async smart-driver construction/commit/status surface now exists
  for qualification, including exact external read witnesses and indivisible
  weighted admission;
- `Capabilities::atomics` remains `false` until ATM-5 acceptance; and
- no cross-Heap, cross-partition, interactive, synchronous, or external-effect
  transaction is in scope.
