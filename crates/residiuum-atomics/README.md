# residiuum-atomics

Pure Atomic protocol crate for Residiuum (`ATM-0` start package).

This crate freezes identity, coordination scope, resource limits, the closed
mutation/predicate vocabulary, outcomes, material status, abort reasons, and
the formal prepare / member / decision / publication state types.

It has no file, network, thread, store, or SDK dependency. ATM-0.2 freezes
[`spec/atomics/cbor-v1.json`](../../spec/atomics/cbor-v1.json) and the canonical
plan codec. ATM-0.3 freezes accepted/rejected vectors in
[`spec/atomics/protocol-vectors.json`](../../spec/atomics/protocol-vectors.json).
ATM-0.4 is the hostile decoder corpus
([`spec/atomics/hostile-corpus.json`](../../spec/atomics/hostile-corpus.json)).
ATM-0.5 is the serial in-memory oracle and shared history format.
ATM-0.6 writes `target/atomics-evidence/atm-0/manifest.json` (semantic/byte freeze).
ATM-0.7 freezes prepare/member/decision/tombstone codecs, member
`object_identity`, not-committed abort-reason preservation, and recursive
canonical map validation ([`spec/atomics/evidence-vectors.json`](../../spec/atomics/evidence-vectors.json)).
ATM-0.8 makes plan close order-independent for reads, predicates, mutations, and
rule revisions, and requires `read_frontier` whenever prior-read witnesses exist.
ATM-0.9 seals `AtomicProfile::Unknown` so known wire codes cannot be constructed
as unknown and cannot alias `LocalHeapV1`.

Normative: [`ATOMICS_SPEC.md`](../../doc/todo/atomics/ATOMICS_SPEC.md)
§§4–13. Programme: [`ATOMICS_IMPLEMENTATION_PLAN.md`](../../doc/todo/atomics/ATOMICS_IMPLEMENTATION_PLAN.md) §4 and §6.

## Modules

| Module | Owns |
|--------|------|
| `id` | `AtomicId`, `ContentRoot`, Heap/collection/version byte identities |
| `limits` | V1 hard ceilings and LocalHeap builder defaults |
| `plan` | Scope, profile, closed mutation/predicate vocabulary |
| `canonical` | Domain separators, CBOR codec, content root, target order |
| `evidence` | Prepare / member / decision / tombstone records and lifecycle phases |
| `evidence_cbor` | Durable evidence encode/decode and domain hashes |
| `outcome` | Logical/material status, receipts, abort and refuse reasons |
| `oracle` | Serial in-memory oracle and shared history format |

License: MIT.
