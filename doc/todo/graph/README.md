# Residiuum Graph programme

Status: **gold-standard destination specified; implementation not admitted**

This directory defines the destination and staged delivery shape for native
graph capability in Residiuum. It does not change the active critical path.
Atomics remains the active programme; remaining RQL qualification and Cluster
retain their governing order in [CRITICAL_PATH.md](../../../CRITICAL_PATH.md).

Read in this order:

1. [GRAPH_ENGINE_SPEC.md](./GRAPH_ENGINE_SPEC.md) — normative destination:
   model, semantics, execution, storage, recovery, SDK, distribution and proof
   obligations.
2. [GRAPH_ENGINE_DELIVERY_PLAN.md](./GRAPH_ENGINE_DELIVERY_PLAN.md) — strict
   conformance profiles from an early source-analysis client to the complete
   engine.
3. [GRF01_DEVELOPER_HANDOFF.md](./GRF01_DEVELOPER_HANDOFF.md) — closed
   developer contract for GRF-0 and GRF-1, including exact schemas, API,
   physical-key/adjacency layout, fixtures and acceptance gates.
4. [GRF01_DELIVERY_BRIEF.md](./GRF01_DELIVERY_BRIEF.md) — assignable work order,
   dependency gate, review sequence and evidence handback.

The central rule is:

> Deliver less of one complete architecture; never deliver a temporary second
> graph model, query engine, authority system or client topology.

No capability is shipped merely because it appears in the destination spec.
Each delivery profile has an explicit entry gate, exit evidence and capability
claim. Until its profile is accepted, the capability is `planned`, not
`available`.
