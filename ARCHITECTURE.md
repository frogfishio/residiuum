# Residiuum architecture map

This file points implementers at the **normative specs** and the **crate layout**.
It is not a second architecture document.

Document state and execution authority are indexed in
[doc/README.md](./doc/README.md).

## Product thesis

> Put anything in. Keep it at scale. Damage it. Find what survived.

Governing recovery rule: *What is gone is gone. What remains still lives.*

## Normative documents

| Concern | Document |
|---------|----------|
| Database identity, trust, security, encryption, lifecycle, ownership | [DATABASE_DOCTRINE.md](./doc/reference/product/DATABASE_DOCTRINE.md) |
| Logical heap identity, containment, and access isolation | [HEAP_SPEC.md](./doc/wip/heap/HEAP_SPEC.md) |
| System architecture, storage model, recovery, quality bars | [OVERVIEW.md](./doc/reference/product/OVERVIEW.md) |
| Survival wire format, frames, segments, scanner tests | [FORMAT_SPEC.md](./doc/reference/storage/FORMAT_SPEC.md) |
| Recovery Shadow (`.rsh`) P★ salvage artifact (CSE-3 Hybrid) | [CSE3_STAGE1_HYBRID_RECOVERY_SHADOW.md](./doc/todo/performance-qualification/CSE3_STAGE1_HYBRID_RECOVERY_SHADOW.md), [Stage 2 implement](./doc/todo/performance-qualification/CSE3_STAGE2_RECOVERY_SHADOW_IMPLEMENT.md) |
| Core storage invariants, failure model, and qualification suite | [CORE_STORAGE_QUALIFICATION_SPEC.md](./doc/todo/core-storage/CORE_STORAGE_QUALIFICATION_SPEC.md), [implementation plan](./doc/todo/core-storage/CORE_STORAGE_QUALIFICATION_IMPLEMENTATION_PLAN.md) |
| Everyday API, CLI, progressive disclosure | [DX_SPEC.md](./doc/reference/product/DX_SPEC.md) |
| First Heap-bound Rust application API and RQL delivery package | [doc/todo/application-baseline/CORE_APPLICATION_API_IMPLEMENTATION_PLAN.md](./doc/todo/application-baseline/CORE_APPLICATION_API_IMPLEMENTATION_PLAN.md) |
| Missing application APIs and product-capability closure | [PRODUCT_DEFICIENCIES.md](./doc/reference/product/PRODUCT_DEFICIENCIES.md) |
| Immediate post-qualification application baseline packages | [MUST_ADD.md](./doc/todo/application-baseline/MUST_ADD.md) |
| Structured Data Algebra (standalone language) | [SDA_SPEC.md](./doc/reference/query/SDA_SPEC.md) |
| Residiuum Query Language (v1 design; shipped parser is v0.1 subset) | [RQL_SPEC.md](./doc/wip/query/RQL_SPEC.md), current-subset guide [doc/RQL/USER_GUIDE.md](./doc/RQL/USER_GUIDE.md), path to full RQL + comprehensive tests [PATH_TO_FULL_RQL.md](./doc/todo/rql/PATH_TO_FULL_RQL.md) |
| Exact ranked query access and rank/select substrate | [DIRECT_ACCESS_SPEC.md](./doc/todo/direct-access/DIRECT_ACCESS_SPEC.md) |
| Filter-conditioned sorting without prefix enumeration | [ORDER_WAVELET_SPEC.md](./doc/todo/order-wavelets/ORDER_WAVELET_SPEC.md) |
| Shared total predicate semantics for RQL and RRE | [RESIDIUUM_PREDICATE_SPEC.md](./doc/reference/query/RESIDIUUM_PREDICATE_SPEC.md) |
| Residiuum Rule Expression (RRE) constraint language and Invariant Core | [RRE_SPEC.md](./doc/todo/rre/RRE_SPEC.md) |
| Collection-owned behaviour and default scope confinement | [COLLECTION_CONTRACT_SPEC.md](./doc/todo/rre/COLLECTION_CONTRACT_SPEC.md) |
| Bounded serializable state transitions and relationship integrity | [ATOMICS_SPEC.md](./doc/todo/atomics/ATOMICS_SPEC.md) |
| Native property graph model, traversal, paths, analytics and staged delivery | [GRAPH_ENGINE_SPEC.md](./doc/todo/graph/GRAPH_ENGINE_SPEC.md), [delivery plan](./doc/todo/graph/GRAPH_ENGINE_DELIVERY_PLAN.md), [GRF-0/1 developer handoff](./doc/todo/graph/GRF01_DEVELOPER_HANDOFF.md) |
| Provisional Kiku/COBOL indexed-file lateral rehosting | [KIKU_COBOL_ISAM_REHOSTING_SPEC.md](./doc/todo/integrations/KIKU_COBOL_ISAM_REHOSTING_SPEC.md) |
| Durable security and administrative evidence | [EVIDENCE_LEDGER_SPEC.md](./doc/todo/evidence/EVIDENCE_LEDGER_SPEC.md) |
| Operational telemetry collection and Ratatouille export | [TELEMETRY_SPEC.md](./doc/todo/telemetry/TELEMETRY_SPEC.md) |
| First-party desktop database IDE | [STUDIO_SPEC.md](./doc/todo/studio/STUDIO_SPEC.md), [implementation plan](./doc/todo/studio/STUDIO_IMPLEMENTATION_PLAN.md) |
| Testing, assurance levels, claim evidence, and release verification | [TESTING_STRATEGY.md](./doc/reference/engineering/TESTING_STRATEGY.md), [implementation plan](./doc/todo/verification/VERIFICATION_IMPLEMENTATION_PLAN.md), [status](./doc/wip/status/VERIFICATION_STATUS.md) |
| SQL-ish+ executable surface and SQL→RQL compiler | [SQL_TO_RQL_SPEC.md](./doc/todo/rql/SQL_TO_RQL_SPEC.md) |
| JSON Schema Draft 2020-12 import into RRE | [JSON_SCHEMA_TO_RRE_SPEC.md](./doc/todo/rre/JSON_SCHEMA_TO_RRE_SPEC.md) |
| Query dialects (rql / sda / json / mongo / sql / … → pure SDA) | [doc/SDA/DIALECTS.md](./doc/SDA/DIALECTS.md) |
| SDA examination of recovered Residiuum units | [SDA_PROFILE.md](./doc/reference/query/SDA_PROFILE.md) |
| Enrichment algebra (ENR1 kernel in `residiuum-sda`; ENR2 candidates design-only) | [crates/enr-core/README.md](./crates/enr-core/README.md), [ENR1.md](./crates/enr-core/ENR1.md), [ENR2.md](./crates/enr-core/ENR2.md); profile `sda-enr1-v0.1` |
| Cluster federation and coverage | [CLUSTER_SPEC.md](./doc/todo/cluster/CLUSTER_SPEC.md) |
| Product framing | [USP.md](./doc/reference/product/USP.md) |
| Public product website | [WEBSITE_SPEC.md](./doc/done/web/WEBSITE_SPEC.md) |
| Public documentation website | [DOCS_SITE_SPEC.md](./doc/done/web/DOCS_SITE_SPEC.md) |
| Three-stage competitive goals and exit gates | [COMPETITIVE_GOALS.md](./doc/reference/product/COMPETITIVE_GOALS.md) |
| Definitive execution priority, stages, and current starting queue | [MASTER_DELIVERY_PLAN.md](./MASTER_DELIVERY_PLAN.md) |
| Staged delivery and exit criteria | [DELIVERY_PLAN.md](./doc/done/programs/DELIVERY_PLAN.md) |
| Doctrine implementation gap map | [doc/wip/doctrine/DOCTRINE_GAPS.md](./doc/wip/doctrine/DOCTRINE_GAPS.md) |
| Post-Heap implementation sequence and package gates | [NEXT_BUILD_PLAN.md](./doc/done/programs/NEXT_BUILD_PLAN.md), [doc/wip/status/NEXT_BUILD_STATUS.md](./doc/wip/status/NEXT_BUILD_STATUS.md) |

Prefer amending a named section of a normative doc before inventing new behavior.

## Delivery stages (summary)

See [DELIVERY_PLAN.md](./doc/done/programs/DELIVERY_PLAN.md) for full exit criteria.

| Stage | Focus | Status |
|-------|--------|--------|
| 0 | Repo + CI harness | **done** (workspace, CI, language decision) |
| 1 | SDA standalone (pure) | **done** — full §14 MUST lock; corpus tag `sda-standalone-v1.0` |
| 2 | Wire format + salvage scanner | **2a–2d** — frames, seal, fwd/rev scan, §13 corpus, deterministic CBOR envelopes |
| 3 | Single-node store | **3a–3c** — put/get/delete, §16 suite, descriptor + index cache |
| 4 | Collection SDK | **4a–4d** — `residiuum-sdk` open, JSON/bytes, scan/stream, filters, `ErrorCode` |
| 5 | SDA examination profile | **done** — `residiuum-examine` ExaminationUnit + SDA over salvage |
| 6 | Indexes, catalogs, chunks | **done** — secondary indexes, history, chunks, compact, checkpoints |
| 7 | CLI doctor/salvage + server | **done** — `residiuum-cli`, connect options (auth/deadline/retry), nightly packaging |
| 8 | Cluster federation | **8a–8f done** — partitions, coverage, Raft, convergent-append, SDK routing, find coverage, rebalance |
| 9 | Tiering / archive | **done** — filesystem media roots, segment move/copy, hierarchical catalogs, offline coverage, retention runbook |

## Crate layout (current)

```text
dingodb/
  crates/
    sda-core/       # package name residiuum-sda; SDA+ENR1 hybrid pure eval (Stage 1) — MIT
    sda-cli/        # package name residiuum-sda-cli; `residiuum-sda` binary (Stage 1) — MIT
    enr-core/       # ENR1/ENR2 specs; ENR1 runtime lives in residiuum-sda (one compile path)
    residiuum-format/   # frames, CBOR envelopes, seal, scan, §13 corpus (Stage 2a–2d) — MIT
    residiuum-client/   # framed RPC + handshake only — MIT
    residiuum-heap/     # identity, capability, decide — MIT
    residiuum-atomics/  # pure Atomic protocol types (ATM-0 start) — MIT
    residiuum-store/    # single-node append store (Stages 3 + 6 + 7 inspect/salvage_to) — MPL-2.0
    residiuum-sdk/      # collection API + remote connect (Stages 4 + 6 + 7); cluster via feature — MPL-2.0
    residiuum-server/   # accept loop, authz, admission, Raft RPC glue, serve_* — AGPL
    residiuum-examine/  # ExaminationUnit + SDA over salvage (Stage 5) — MPL-2.0
    residiuum-cli/      # `residiuum` binary: put/get, doctor, salvage, backup/restore, scrub, migrate, serve (Stage 7) — AGPL
    residiuum-cluster/  # partitions, coverage, multi-node + Raft + find + rebalance (Stage 8a–8f) — AGPL
```

Crate ownership:

| Stage | Crate | Role |
|-------|-------|------|
| 2 | `residiuum-format` | **Present** — frames, deterministic CBOR envelopes, seal, scanner, §13 corpus (2a–2d) |
| — | `residiuum-client` | **Present** — MIT wire framing + handshake (`residiuum-rpc-v1`) |
| — | `residiuum-heap` | **Present** — heap identity, capability, pure `decide` |
| ATM-0 | `residiuum-atomics` | **Present (ATM-0.1)** — pure Atomic types; no store/SDK/IO |
| 3+6+7 | `residiuum-store` | **Present** — put/get/delete, salvage, open_inspect, salvage_to, backup_to/restore (DEF-050), scrub (DEF-051), migrate (DEF-052), catalogs, chunks, history, compact |
| 4+6+7+8d–8e | `residiuum-sdk` | **Present** — collections, filters, indexes, history, remote RPC; `cluster` feature for open_cluster |
| 5 | `residiuum-examine` | **Present** — ExaminationUnit projection, salvage stream, SDA filter/map, bounded pages |
| 7 | `residiuum-server` | **Present** — bounded serve, authz, admission, TLS bind policy, network Raft glue |
| 7 | `residiuum-cli` | **Present** — `residiuum` put/get/list/doctor/salvage/backup/restore/scrub/migrate/serve (serve via `residiuum-server`) |
| 8 | `residiuum-cluster` | **Present (8a–8f)** — partitions, coverage, Raft, convergent-append, find honesty, rebalance |

Rule of thumb from the delivery plan: **vertical slices over empty package trees.**

## Language decisions (Stage 0)

| Choice | Decision |
|--------|----------|
| Core implementation language | **Rust** |
| First embedded surface | Rust library API; TypeScript-like examples in DX_SPEC remain the product shape |
| First CLI | `residiuum-sda` (Stage 1) + `residiuum` (Stage 7) |
| SDA packaging | `residiuum-sda` (lib) + `residiuum-sda-cli` (`residiuum-sda` binary); SDA+ENR1 hybrid; no storage IO |
| Wire format versioning | Draft `1.0-draft`; reader/writer matrix + migrate phases (DEF-052); freeze is DEF-053 |
| Process configuration | Versioned `residiuum-config-v1` validate-before-serve (DEF-054); live reload follow-on |
| Operational telemetry | [Ratatouille-only bounded firehose](./doc/todo/telemetry/TELEMETRY_SPEC.md); no request-path file/stdout logging |
| Formal audit | Residiuum Evidence Ledger; durable, Heap-confined, independently verifiable |
| Metrics / health | Versioned `residiuum-metrics-v1` scrape + `residiuum-health-v1` live/ready/detail RPCs (DEF-061); store/cluster gauges follow-on |
| License | Multi-tier: MIT / MPL-2.0 / AGPL-3.0-or-later (see `doc/reference/operations/LICENSING.md`) |

## SDA import convention

- Package name on crates.io: **`residiuum-sda`** (never bare `sda` / `sda-lib`)
- CLI package: **`residiuum-sda-cli`**, binary **`residiuum-sda`**
- Workspace dependency key: `sda-core` → Rust path `sda_core::…` (dependents)
- Inside the library package / its integration tests: `residiuum_sda::…`
- Product shape: SDA + additive ENR1 hybrid for Residiuum, not a generic pure-SDA claim

## Product follow-ons (in-tree v0.23 — not production)

Stages **0–9** are implemented in-tree. Product follow-ons 1–4:

1. **S3/GCS filesystem mirrors** — `MediaLocator` + `CloudMirrorConfig`
   (`RESIDIUUM_S3_ROOT` / `RESIDIUUM_GS_ROOT`); `object:local:` stand-in unchanged.
   These are **mirrors**, not native cloud backends.
2. **Network multi-hop routing + experimental Raft** — `residiuum serve-cluster` +
   live `endpoints.json` reload; `RemoteClient` routes keyed ops and refreshes
   on transport failure; demo `scripts/demos/08_kill_a_node.sh`. Requires
   `--experimental-network-cluster`. When Raft attaches (default), collection
   put/delete use partition propose (DEF-037) and control-plane `raft_*` RPCs
   (DEF-036); acks report `committed` only after quorum + local apply.
   Directory-only fallback if attach fails. Deterministic multi-replica tests
   still prefer in-process `Residiuum::open_cluster`.
3. **Freeze / packaging labels** — `SDK_API_VERSION` (`1.0`),
   `CLUSTER_PROFILE_VERSION` (`v1` in-process), `WIRE_PROFILE_LABEL`
   (`1.0-draft`), plus `CLUSTER_COMMIT_PROFILE` (`residiuum-cluster-commit-v1`).
   Distinct from crate semver `0.2.0`.
4. **Nice-to-haves** — `LifecyclePolicy`, erasure manifest scaffold,
   [doc/reference/operations/BENCHMARK_DISCLOSURE.md](./doc/reference/operations/BENCHMARK_DISCLOSURE.md) (OVERVIEW §12.2).

Network Raft control plane, data-plane commit, durable rebalance jobs,
in-process anti-entropy repair, and seeded in-process verification are in-tree
on the experimental path (DEF-035–041). Production local-cluster gates
(multi-process Jepsen / long soak) remain DEF-041 follow-ons. Operator path today:
development `residiuum serve`, experimental `serve-cluster` with Raft when attached,
and offline node salvage. Maturity labels:
[doc/wip/status/CAPABILITY_MATRIX.md](./doc/wip/status/CAPABILITY_MATRIX.md), [DEFECTS.md](./doc/done/incidents/DEFECTS.md).
Work horizon (stage plan vs remaining gates):
[doc/done/programs/WORK_HORIZON.md](./doc/done/programs/WORK_HORIZON.md).

## Stage 9 (landed)

Filesystem hot/warm/cold/archive media roots, segment move/copy with stable
identities, hierarchical segment catalogs, offline-tier coverage honesty, and
[doc/reference/operations/RUNBOOK_RETENTION.md](./doc/reference/operations/RUNBOOK_RETENTION.md).

Object-style addressing: parse `MediaLocator` (`file` / `object:local` / `s3` /
`gs`); local object media and mirrored cloud roots work under the placement API.
