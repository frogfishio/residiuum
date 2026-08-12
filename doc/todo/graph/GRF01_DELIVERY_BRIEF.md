# GRF-0 / GRF-1 delivery brief

Status: **ready to assign; capability unavailable until acceptance**

Date: 2026-08-12

Technical contract:
[GRF01_DEVELOPER_HANDOFF.md](./GRF01_DEVELOPER_HANDOFF.md)

Destination and staging:
[GRAPH_ENGINE_SPEC.md](./GRAPH_ENGINE_SPEC.md) and
[GRAPH_ENGINE_DELIVERY_PLAN.md](./GRAPH_ENGINE_DELIVERY_PLAN.md)

Machine contracts: [spec/graph/](../../../spec/graph/)

## Assignment

Deliver `GRF-0`, obtain architecture acceptance, then deliver `GRF-1.1` through
`GRF-1.7` in order. This assignment is limited to the model, embedded async
client, canonical generation loading, bounded record pages, point reads and
exact one-hop adjacency. It does not authorize recursion, paths, algorithms,
remote graph RPC, incremental active-generation mutation or a second query
runtime.

`GRF-0` is technically independent, but work starts only when the principal's
assignment explicitly admits it under
[CRITICAL_PATH.md](../../../CRITICAL_PATH.md). `GRF-1` must not start until
embedded LocalHeap Atomics has passed ATM-5 and the driver
truthfully reports `Capabilities::atomics`. If that gate has not passed, report
the dependency; do not replace it with locks, compensating writes or
graph-local transactions.

## Required delivery order

1. `GRF-0.1`: identities, names, generation physical-key codec and ordering.
2. `GRF-0.2`: deterministic definition/system-record CBOR and hostile decoder.
3. `GRF-0.3`: canonical record JSON, hashes and golden vectors.
4. `GRF-0.4`: independent point/scan/adjacency oracle and shared corpus.
5. Architecture review and GRF-0 evidence acceptance.
6. `GRF-1.1`: reserved namespace, bootstrap and collection ownership guard.
7. `GRF-1.2`: catalog, Atomic graph publication and immutable binding.
8. `GRF-1.3`: versioned point reads and bounded vertex/edge pages.
9. `GRF-1.4`: immutable generation-scoped adjacency codec, manifest and host
   page lookup.
10. `GRF-1.5`: exact one-hop neighbors, continuation and coverage.
11. `GRF-1.6`: resumable generation load, validation Atomics and activation
    Atomic.
12. `GRF-1.7`: crash/damage/rebuild/authority/performance qualification.
13. Architecture and evidence review.
14. Enable the four GRF-1 capability bits last.

Each numbered package is independently reviewable. Later packages may be
developed behind false capability flags, but no later package can be accepted
before all earlier exits pass.

## Non-negotiable review points

- One physical `driver::Client` remains the scheduler/writer/shutdown domain;
  graph handles are Heap-bound children.
- Logical graph keys use the frozen generation-prefixed physical-key codec, so
  equal logical keys in two generations never overwrite one another.
- Canonical records and graph system records are authority. `.gai`/`.gam`
  files are disposable derived state.
- Explicit adjacency rebuild reproduces the immutable binding's original
  identities; ordinary open/read paths never repair or rewrite storage.
- Active adjacency remains valid while another generation loads.
- Empty is an answer only with complete coverage; absence never comes from a
  missing, unpublished, partial or damaged artifact.
- Every adjacency candidate is revalidated against its authoritative edge
  record and immutable binding.
- Graph publication, validation publication and activation use qualified
  LocalHeap Atomics. No graph transaction coordinator is permitted.
- All inputs, queues, scans, pages, continuations, validation work and memory
  are bounded by the frozen contract.
- SDK reads expose establishing versions, binding and coverage. Point absence
  is carried in `GraphPointResult`, not a naked `Option<T>`.
- Recursive graph meaning is absent in GRF-1 and later belongs only to QVM.

## Developer evidence bundle

Run every command listed in
[acceptance-v1.json](../../../spec/graph/acceptance-v1.json) and deliver the six
artifacts under `out/graph/grf01/`. The manifest must record:

- repository commit and dirty-state refusal;
- Rust toolchain and target;
- Atomics profile/evidence manifest identity;
- every suite and negative-control result;
- fixture and machine-contract hashes;
- failpoint crash matrix;
- resource-limit results;
- performance measurements and host description; and
- the exact capability bits before and after the proposed gate.

No capability may be enabled from a developer assertion alone. Architecture
accepts the implementation shape; governance accepts the evidence; only then
is the profile advertised.

## Change control

If implementation discovers a conflict, missing case or impractical frozen
limit, stop only the affected package and submit a minimal amendment containing:

1. the exact contradictory clauses or machine fields;
2. a reproducer or measured evidence;
3. authority, recovery, compatibility and coverage consequences;
4. the proposed replacement text and fixture changes; and
5. why the change does not create a second model/runtime/transaction system.

Do not silently reinterpret durable bytes, public signatures, error strings,
coverage, publication order or package scope.
