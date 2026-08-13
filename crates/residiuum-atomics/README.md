# residiuum-atomics

Pure Atomic protocol crate for Residiuum (`ATM-0` start package).

This crate freezes identity, coordination scope, resource limits, the closed
mutation/predicate vocabulary, outcomes, material status, abort reasons, and
the formal prepare / member / decision / publication state types.

It has no file, network, thread, store, or SDK dependency. Canonical CBOR,
fixtures, the hostile decoder corpus, and the serial in-memory oracle are
later ATM-0 cards — do not invent those fields here.

Normative: [`ATOMICS_SPEC.md`](../../doc/todo/atomics/ATOMICS_SPEC.md)
§§4–13. Programme: [`ATOMICS_IMPLEMENTATION_PLAN.md`](../../doc/todo/atomics/ATOMICS_IMPLEMENTATION_PLAN.md) §4 and §6.

## Modules

| Module | Owns |
|--------|------|
| `id` | `AtomicId`, `ContentRoot`, Heap/collection/version byte identities |
| `limits` | V1 hard ceilings and LocalHeap builder defaults |
| `plan` | Scope, profile, closed mutation/predicate vocabulary |
| `canonical` | Domain separators; codec deferred to ATM-0.2 |
| `evidence` | Prepare / member / decision records and lifecycle phases |
| `outcome` | Logical/material status, receipts, abort and refuse reasons |
| `oracle` | Reserved crate boundary; serial oracle is ATM-0.5 |

License: MIT.
