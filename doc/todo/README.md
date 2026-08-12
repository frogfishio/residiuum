# TODO — admitted by the master plan

Documents here are sufficiently specified for development but are not
accepted. Their directory does not itself authorize work; dependencies and
priority come from [MASTER_DELIVERY_PLAN.md](../../MASTER_DELIVERY_PLAN.md).

Immediate order:

1. [Core Storage Qualification](core-storage/)
2. [Performance Qualification Harness](performance-qualification/) — first
   post-C0 measurement lane; may execute alongside M1
3. [Formal Assurance Spine](formal-assurance/) — begins post-C0 alongside PQH
   and M1; later theorem families travel with Atomics and cluster
4. [Application Baseline](application-baseline/)
5. [Heap Application Ready](heap-application-ready/)

Critical-path supporting infrastructure:

- [Application Driver Spine](application-driver/) — async Rust, bounded
  pooling, streamed RQL, cancellation/retry truth, and server read concurrency;
  only slices required by the active RQL gate are admitted

Active critical-path programme (principal amendment 2026-08-10):

- [Atomics](atomics/) — bounded serializable LocalHeap transitions, exact
  outcome evidence, recovery, async SDK, and cost qualification

Later programs:

- [RRE and collection contracts](rre/)
- [Direct Access](direct-access/)
- [Order Wavelets](order-wavelets/)
- [Graph Engine](graph/) — gold-standard destination and staged profiles;
  specified for future admission, not an active implementation lane
- [Kiku / COBOL / ISAM lateral rehosting](integrations/KIKU_COBOL_ISAM_REHOSTING_SPEC.md)
  — provisional codec/runtime architecture for behavior-preserving indexed-file
  rehosting; requires real-estate refinement before developer handoff
- Evidence, Telemetry, Studio, clustering (including the
  [Medusa Durability Fabric](cluster/MEDUSA_DURABILITY_FABRIC_SPEC.md)), and
  deferred expansion programs
