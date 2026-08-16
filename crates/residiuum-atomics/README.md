# residiuum-atomics

Pure Atomic protocol crate for Residiuum (`ATM-0` start package).

This crate freezes identity, coordination scope, resource limits, the closed
mutation/predicate vocabulary, outcomes, material status, abort reasons, and
the formal prepare / member / decision / publication state types.

It has no file, network, thread, store, or SDK dependency. The packaged
conformance bundle is crate-local [`spec/`](spec/) (CBOR freeze, accepted and
rejected vectors, hostile corpus, evidence vectors). In the monorepo those
files stay byte-identical to [`spec/atomics/`](../../spec/atomics/). ATM-0.2
freezes [`spec/cbor-v1.json`](spec/cbor-v1.json) and the canonical
plan codec. ATM-0.3 freezes accepted/rejected vectors in
[`spec/protocol-vectors.json`](spec/protocol-vectors.json).
ATM-0.4 is the hostile decoder corpus
([`spec/hostile-corpus.json`](spec/hostile-corpus.json)).
ATM-0.5 is the serial in-memory oracle and shared history format.
ATM-0.6 writes `target/atomics-evidence/atm-0/manifest.json` (semantic/byte freeze).
ATM-0.7 freezes prepare/member/decision/tombstone codecs, member
`object_identity`, not-committed abort-reason preservation, and recursive
canonical map validation ([`spec/evidence-vectors.json`](spec/evidence-vectors.json)).
ATM-0.8 makes plan close order-independent for reads, predicates, mutations, and
rule revisions, and requires `read_frontier` whenever prior-read witnesses exist.
ATM-0.9 seals `AtomicProfile::Unknown` so known wire codes cannot be constructed
as unknown and cannot alias `LocalHeapV1`.
ATM-0.10 freezes `AtomicReceipt.durability = durable` and returns the committed
receipt on `AtomicStatus` when committed material permits it. `NotFound` stays off the logical decision axis.
ATM-0.11 replaces ceremonial model evidence with a finite lifecycle model and
derived proofs. One generator writes the ATM-0 evidence pack; a separate test
recomputes every manifest hash.
ATM-1.1 adds typed encodings for the closed mutation/predicate vocabulary and
requested vs worst-case generated-member accounting. The closed-plan validator is ATM-1.2.
ATM-1.2 is the pure closed-plan validator shared by the serial oracle and future
remote admission. Structural refusals append no evidence.
ATM-1.3 is the Heap-bound typed builder: collection IDs from one Heap, rights
union, authority-revision binding, deadline and hard-limit validation, and read
witnesses. The public SDK façade and `Capabilities::atomics` remain ATM-5.
ATM-2.2 is the per-Heap coordinator stream, shard placement manifest, and staged
append lane. Staged members are invisible to ordinary get/scan. Store adapters
must not publish them to RQL, history, watch, or secondary indexes.
ATM-2.3 commits a complete chunk map before any chunk is installed and seals a
first stable boundary covering prepare and every member (`DurableInvisible`).

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
| `encode` | Typed plan encodings, admitted values, closed-plan byte accounting |
| `outcome` | Logical/material status, receipts, abort and refuse reasons |
| `oracle` | Serial in-memory oracle and shared history format |
| `validate` | Closed-plan structural validator (profile, scope, Heap, limits) |
| `builder` | Heap-bound typed builder, rights union, authority binding |
| `staging` | Coordinator stream, placement manifest, invisible staged append |

License: MIT.
