# residiuum-atomics

Pure Atomic protocol crate for Residiuum (`ATM-0` start package).

This crate freezes identity, coordination scope, resource limits, the closed
mutation/predicate vocabulary, outcomes, material status, abort reasons, and
the formal prepare / member / decision / publication state types.

It has no file, network, thread, store, or SDK dependency. ATM-0.2 freezes
[`spec/atomics/cbor-v1.json`](../../spec/atomics/cbor-v1.json) and the canonical
plan codec. Fixtures, the hostile corpus, and the serial oracle are later cards.

Normative: [`ATOMICS_SPEC.md`](../../doc/todo/atomics/ATOMICS_SPEC.md)
§§4–13. Programme: [`ATOMICS_IMPLEMENTATION_PLAN.md`](../../doc/todo/atomics/ATOMICS_IMPLEMENTATION_PLAN.md) §4 and §6.

## Modules

| Module | Owns |
|--------|------|
| `id` | `AtomicId`, `ContentRoot`, Heap/collection/version byte identities |
| `limits` | V1 hard ceilings and LocalHeap builder defaults |
| `plan` | Scope, profile, closed mutation/predicate vocabulary |
| `canonical` | Domain separators, CBOR codec, content root, target order |
| `evidence` | Prepare / member / decision records and lifecycle phases |
| `outcome` | Logical/material status, receipts, abort and refuse reasons |
| `oracle` | Reserved crate boundary; serial oracle is ATM-0.5 |

License: MIT.
