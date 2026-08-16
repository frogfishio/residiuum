# Residiuum Heap Specification

Status: Developer-ready implementation contract v0.9
Capability status: **Partial** — HP-000…HP-009 landed in-tree (with listed
gaps); HP-010 evidence advanced: **H3 Accept**, H0–H2/H4–H5 partial, H6 partial
with complete-path review + external-review brief + pure proof bundle + §32.4
remote data/list/scan cut (`qualified=false`); HP-011…HP-012 not started. HC1
not started. No `residiuum-heap-v1` qualified claim. See **Implementation progress**
below.
Scope: Logical heap identity, collection containment, authorization, isolation,
administration, recovery, and compatibility
Audience: SDK, server, cluster, storage, security, recovery, CLI, and test-rig
implementers
Companion contracts: `ATOMICS_SPEC.md`, `RRE_SPEC.md`,
`COLLECTION_CONTRACT_SPEC.md`, `DX_SPEC.md`, `CLUSTER_SPEC.md`,
`doc/todo/heap-application-ready/HEAP_APPLICATION_READY_PLAN.md`, and `doc/reference/operations/LICENSING.md`

### Normative language

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**,
**SHOULD**, **SHOULD NOT**, **RECOMMENDED**, **MAY**, and **OPTIONAL** are to be
interpreted as described by
[BCP 14](https://www.rfc-editor.org/rfc/rfc2119.html) when, and only when, they
appear in all capitals. Text marked illustrative is non-normative; frozen
tables, byte layouts, state transitions, and acceptance conditions are
normative.

### Implementation progress

Last updated: 2026-07-30. This subsection is **status only**; it does not
change normative requirements. Package acceptance still means the Accept
criteria in §40.

#### Summary

| Layer | State |
|-------|--------|
| Spec contract (`residiuum-heap-v1` prose §§30–41) | Frozen (this document) |
| Machine-readable artifacts (`spec/heap/`) | Present; HP-000 mostly landed |
| Isolation kernel crate (`crates/residiuum-heap`) | Present; HP-001 mostly landed |
| Durable ownership in `residiuum-format` | Present; HP-002 partial |
| Store façades / architecture check | Present; HP-003 partial |
| Heap/object catalogs (HP-004) | Present; staged genesis + rebuild Accept |
| Authority + local ceremony (HP-005) | Present; two-slot store + `residiuum-authority` |
| SDK heap API (HP-007) | Present; typed handles + isolation Accept |
| Qualified network (HP-008) | Present; live TLS accept-loop + HeapKey session |
| Legacy migration (HP-006) | Present; durable job engine + phase-6 gate Accept |
| Lifecycle / backup / DR (HP-009) | Present; purge + payload-restore + DR retain-ID + key destroy + media-domain incomplete purge + retention scheduler Accept |
| Single-node / cluster qualification | **In progress** (HP-010: H3 Accept; H6 partial; `qualified=false`) |
| Product claim level (§1.4 / Gate H6) | Still **Level 1 language only** — named namespaces / isolation in progress |

Before Gate H6, product language remains:

> Residiuum provides named heap namespaces; strong access-isolation qualification
> is in progress.

#### Work packages (§40)

| ID | Title | Status | Notes |
|----|-------|--------|-------|
| **HP-000** | Machine-readable contract | **Landed (format closed; ops expanding under §32.4)** | Baseline process ops **1–3** + data/list/scan/find/history/indexes cut **105/110–112/114–117/120–122/130–133** with schemas/fixtures; bootstrap cert/proof + **`format_vectors`**. Authority/RPC remainder of §38.1 still partial; remaining ops stay `reserved`. |
| **HP-001** | Isolation kernel | **Landed (gaps)** | `crates/residiuum-heap`: IDs, `Rights`, constraints, COSE cert + holder-proof verify, snapshot/`HeapSlot`, pure `decide`, unforgeable `HeapCap` (trybuild compile-fail). **Gap:** Verus/Kani paths under `verification/heap-verus/` and `formal/heap/` are scaffolds, not connected proofs. |
| **HP-002** | Durable ownership | **Landed (Accept corpus)** | Frame kinds **10–13**; envelope keys **31–36**; `SubjectV2`; ownership parse/agree (merge); descriptor encode/decode + `descriptor_hash`; `admit_frame_to_heap` / salvage; store `require_admit` + `HeapStore` SubjectV2 heap check; adversarial unit/corpus rejects wrong-heap. |
| **HP-003** | Store compilation firewall | **Landed (qualified path)** | `kernel::PhysicalStore` alias; façades; architecture checker. Public raw `Store` gated behind **opt-in** feature `legacy-raw-store`; package **default is façades-only** (A3). Stages 3–9 enable the feature explicitly. |
| **HP-004** | Heap and object catalogs | **Landed (Accept rebuild)** | `residiuum-store::heap::catalog`: non-discoverable staged genesis, descriptor-chain history, immutable collection/stream IDs, rename/retire, rebuildable `heap-catalog`/`collections`/`streams` CBOR, local admin receipts. Accept test deletes catalogs and reconstructs names/aliases/IDs/owner from chains. **Does not** bind authority (HP-005). |
| **HP-005** | Authority and local ceremony | **Landed (Accept core)** | `crates/residiuum-authority` (AGPL): two-slot head/time-floor store, anchor, root-event genesis binding staged descriptor hash, publish, HeapKey issue, reload notify (read-only apply). `residiuum-store/authority-provisioning` feature. Accept: genesis+issue, staged-invisible, fork fail-closed, reload non-mutating, server does not link authority. **Gaps:** full COSE transition/mutation event corpus, threshold recovery, Unix lock/peer-cred barrier, crash-matrix failpoints. |
| **HP-006** | Legacy migration | **Landed (Accept job/gate)** | `residiuum-store::heap::migration`: durable `MigrationStateV1`, inventory/assignment hashes (§34.7), phases 0–7, idempotent rewrite admit log, failpoint crash resume, phase-6 `CutoverGate` refuses `unlabelled_active_frames > 0`. Accept: crash injection converges without duplicate/lost frames; cutover blocked until unlabelled cleared. **Gaps:** physical segment rewrite against live `Store` trees, preflight backup verification, operator CLI/report, full quarantine filesystem moves. |
| **HP-007** | SDK capability surface | **Landed (Accept isolation + SubjectV2 + connect_heap data + CPR-001 heap-only default)** | Heap APIs + SubjectV2 put/get + `connect_heap` with remote put/get/delete/list/scan/find/history/indexes. Equality `find` accelerates via ready secondary indexes; put/delete marks indexes **stale**. **CPR-001:** package default **heap-only**; `legacy-flat-sdk` opt-in for Stages 3–9. Accept: isolation, SubjectV2, connect_heap data+list/scan/find/history/indexes (+ find-via-index + stale), `cpr001_legacy_opt_in`. **Gaps:** incremental rebuild (not only full); store `legacy-raw-store` default still on (A3). |
| **HP-008** | Qualified network protocol | **Landed (Accept TLS + §32.4 data/list/scan/find/history/indexes)** | Session/audit/exporter + accept-loop; **no token/RBAC**. **§32.4 active (18 ops):** process 1–3 + 105/110–112/114–117/120–122 + **130–133** indexes (**IndexAdmin** on 131–133; bootstrap cert rights_mask includes IndexAdmin). Accept: connect_heap put/get/delete + list/scan/find/history/indexes. **Gaps:** lifecycle still reserved, default qualified listener, RPC corpus expansion. |
| **HP-009** | Lifecycle, backup, recovery | **Landed (Accept + DR/key + media wipe + retention residual)** | `residiuum-store::heap::lifecycle`: suspend/resume/retire/purge on `HeapSlot`, hold-blocked purge, verifiable `PurgeReceipt`, heap-aware backup manifest, payload-only restore-to-new-id (no access), labelled-unit damage isolation, permanent identity tombstones, in-process data-key destruction receipts, disaster-recovery same-identity takeover, media-domain purge plans with unavailable-domain incomplete result that **stays `retired`**, **live multi-tier filesystem wipe** (`destroy_coverage_unit_on_media` / `wipe_heap_object_media`), `RetentionScheduler` minimum-retain window. Accept: receipt verifies; payload restore denied; damage isolation; key destroy; tombstone permanent; DR retain-ID; incomplete purge; retention; live FS multi-tier wipe. **Gaps:** HSM/provider data-key adapters, mixed-heap salvage drill, operator CLI. |
| HP-010 | Single-node qualification | **In progress (H3 Accept; H6 partial)** | Matrix stays `qualified=false`. **H3 Accept**. **H1 advanced** (SubjectV2 + remote data/list/scan/find/history/indexes + CPR-001 opt-in). H0/H1/H2/H4/H5 still partial. H6 still needs machine-checked Verus/Kani + **signed** external review + CPR residual close (default flat SDK). |
| HP-011 | Cluster control and placement | Not started | |
| HP-012 | Cluster qualification | Not started | |

#### Implementation gates (§27)

| Gate | Status |
|------|--------|
| H0 Vocabulary and identity | **In progress** — types/registry + CPR-001 heap-only SDK default; store `legacy-raw-store` still default-on (A3). |
| H1 Heap-bound SDK | **In progress** — HP-007 + SubjectV2 + `connect_heap` put/get/list/scan/find/history/indexes + stale maintain Accept; package default heap-only. |
| H2 HeapKey authority | **In progress** — HP-005/008 + §32.4 data/list/scan/find/history/indexes cut (18 active ops); lifecycle/export and other ops still reserved. |
| H3 Derived / operational coverage | **Accept** (single-node reference) — Derived paths, query escape, ops/health/bundle confinement, **named isolation profiles + closed declassification registry + metadata-hardened operational confinement**. Resource/physical profiles declared, not qualified. |
| H4 Backup and recovery | **In progress** — HP-009 payload-restore + DR retain-ID + purge/tombstone + media-domain incomplete purge + retention + **live multi-tier FS wipe** Accept; HSM adapters / mixed-heap salvage still open. |
| H5 Single-node lifecycle | **In progress** — HP-009 transitions + HP-010 key-loss / incomplete-purge / retention + **lifecycle crash-matrix** (peer unaffected) Accept; broader destructive crash cells still open. |
| HC1 Cluster extension | Not started. |
| H6 Isolation claim | **Partial** — Level 1 language; published limitations; TLA + connected models; executable §39 + `pure_proofs`; **Kani + Verus pure-kernel connected** (CI `kani-heap` / `verus-heap`); complete-path review + external review **brief** on file. **Signed** external report (CPR-005) still open — `may_advertise_qualified() == false`. |

#### Primary tree map (current)

```text
spec/heap/                     # HP-000 contract + format_vectors
crates/residiuum-heap/             # HP-001 kernel (MIT)
crates/residiuum-format/           # HP-002 ownership, SubjectV2, descriptors, admit
crates/residiuum-store/src/kernel/ # PhysicalStore alias (crate-private)
crates/residiuum-store/src/heap/   # HP-003 façades + HP-004 catalog
crates/residiuum-authority/        # HP-005 local ceremony (AGPL; not linked by server)
crates/residiuum-sdk/src/heap.rs   # HP-007 Heap / typed handles
crates/residiuum-client/src/heap_handshake.rs  # HP-008 wire types
crates/residiuum-server/src/heap_{registry,auth,dispatch,session,audit}.rs  # HP-008
crates/residiuum-store/src/heap/migration.rs  # HP-006 job engine + phase-6 gate
crates/residiuum-store/src/heap/lifecycle.rs  # HP-009 purge/backup/restore gates
spec/heap/qualification/                 # HP-010 evidence matrix
crates/residiuum-heap/src/qualification.rs   # claim surface (qualified=false)
crates/residiuum-heap/src/isolation.rs       # query-escape confinement (H3/H6)
crates/residiuum-heap/src/operational.rs     # metrics/logs/export/health/bundle (H3)
crates/residiuum-heap/src/isolation_model.rs # connected Rust ↔ HeapIsolation Inv (H6)
crates/residiuum-heap/src/authority_model.rs  # connected Rust ↔ HeapAuthority Inv (H6)
crates/residiuum-heap/src/isolation_profile.rs # §13 named profiles + registry
crates/residiuum-heap/src/decide_obligations.rs # executable §39 Verus stand-in (H6)
spec/heap/isolation-profiles-v1.json         # closed declassification registry
doc/wip/heap/RUNBOOK_HEAP_QUALIFICATION.md        # HP-010 operator runbook
doc/wip/heap/HEAP_COMPLETE_PATH_REVIEW.md         # Gate H6 complete-path review (CPR-*)
doc/wip/heap/HEAP_EXTERNAL_SECURITY_REVIEW_BRIEF.md  # External review engagement pack
crates/residiuum-heap/src/pure_proofs.rs     # Verus-oriented pure lemmas (executable)
                               # Store public only with legacy-raw-store (default)
scripts/check_heap_architecture.sh
scripts/verify-heap.sh
formal/heap/                   # TLA+ HeapIsolation + HeapAuthority sketches (+ MC cfg)
verification/heap-verus/       # Verus scaffold + H6 obligation checklist
fuzz/fuzz_targets/heap_ownership.rs
```

#### Self-check: are we “done with HEAP_SPEC”?

| Question | Answer |
|----------|--------|
| Is the **spec prose** a usable implementation contract? | **Yes** — §§30–41 are frozen developer-ready text (v0.9). |
| Is **implementation** of the full package tree complete? | **No** — HP-010 incomplete; HP-011/012 not started. |
| May we advertise `residiuum-heap-v1` **qualified**? | **No** — `qualified=false`; Level 1 claim language only. |
| Is the **hot data path** good enough for heap-bound apps? | **Mostly yes** for embedded + qualified remote put/get/list/scan/find/history/indexes (equality find index-accelerated); lifecycle RPC still reserved. |

**Bottom line:** we are **not done** with the *program* HEAP_SPEC describes. We **are** done writing the core *contract document*; remaining work is residual implementation + honest qualification evidence.

#### Next recommended package

Living task queue: **[doc/done/programs/HEAP_NEXT_TASKS.md](../../done/programs/HEAP_NEXT_TASKS.md)**.

Continue **HP-010** critical path: close H0/H1/H2/H4/H5 residuals → machine-checked
Verus/Kani → signed external review disposing CPR findings → only then consider
`qualified=true`. In parallel: CPR-001 default flip, default qualified listener,
HP-009 FS tier wipe / HSM, HP-006 physical rewrite, remaining reserved ops as matrix needs.

#### Remaining delivery sequence

```text
DONE   HP-000 → … → HP-008 (core + §32.4 data/list/scan/find/history/indexes) → HP-006(core) → HP-009(core+DR/key+media/retention)
NEXT   HP-010 single-node qualification  (H3 Accept; H6 partial; Verus + signed review open)
  or   residual closers (CPR-001 default flip, FS wipe/HSM, lifecycle ops as needed)
LATER  HP-011 → HP-012 cluster
```

**Critical path to a qualified single-node claim (`residiuum-heap-v1`):**
close qualification residuals as required → **HP-010** evidence matrices.

#### What's left (operator checklist)

**Must land before / during HP-010 (qualified claim):**

| Item | Package | Why it blocks qualification |
|------|---------|------------------------------|
| Full H-gate matrices, load/fuzz/restore/key-loss evidence | HP-010 | Profiles + AuthorityModel + §39 gen/blacklist Accept landed; H6 full Verus + external review still block `qualified=true` |
| Physical segment rewrite + operator migration tooling | HP-006 residual | Live-store rewrite still open |
| Activate remaining reserved heap ops (§32.4 schemas first) | HP-008 residual | Data + list/scan/find/history/indexes cut landed; lifecycle/export still reserved |
| Make qualified listener the default remote profile | HP-008 residual | Legacy token path still default-off |
| Live media purge across tiers/replicas / retention scheduler | HP-009 residual | Live multi-tier FS wipe Accept landed; HSM adapters / mixed-heap salvage still open |

**Landed but still gapped:**

| Item | Package |
|------|---------|
| Authority transition/mutation COSE corpus, threshold recovery, peer-cred barrier | HP-005 |
| Incremental secondary-index maintain after writes on SubjectV2 | HP-007 residual (equality find acceleration Accept landed) |
| Authority/RPC vector remainder in `spec/heap` | HP-000 |
| Signed external security review (CPR-005) | HP-010 / H6 |
| Flip default off for `legacy-raw-store` (SDK flat already opt-in) | HP-003 / A3 residual |

**After single-node qualification:** HP-011 → HP-012. Until then cluster remains
`qualified=false`.

**Verify today:** `./scripts/verify-heap.sh` (includes HP-006 + HP-009 + HP-010 matrix/drills).
Product language stays Level 1 until Gate H6.

## 1. Purpose

A Residiuum deployment may serve more than one independent body of application
data.

Residiuum calls each such body a **heap**.

```text
Deployment
├── Heap A
│   ├── Collection: users
│   └── Collection: orders
└── Heap B
    ├── Collection: users
    └── Stream: events
```

Collection names are unique only within a heap. `Heap A/users` and
`Heap B/users` are unrelated collections.

The central guarantee is:

> No ordinary data operation exists outside a heap context, and a system
> without a valid HeapKey for a heap cannot use an ordinary or derived access
> path to obtain that heap's data.

The stronger isolation rule is:

> Data belonging to two different heaps never enters one query, iterator,
> transaction, index, cache entry, result set, backup payload, or recovery
> view.

The query engine does not enforce this rule by remembering to add a heap
filter. It receives a capability whose visible universe is already one heap.

This specification defines logical behavior. It does not require heaps to map
to directories, files, segments, partitions, processes, or storage devices in
any particular way.

## 1.1 Defining property: heaps cannot meet

The defining property of a Residiuum heap is not that queries normally include a
heap filter.

It is:

> **Heaps are not filtered apart. They are incapable of meeting inside a
> Residiuum data operation.**

Heap separation MUST NOT depend on RQL, SDA, ENR, an optimizer, an index,
an SDK, or an RPC handler remembering to append:

```text
where heap_id = current_heap
```

Those components are complex, evolve frequently, and will contain defects.
They operate above the isolation boundary.

Instead, every operation receives an unforgeable heap capability whose visible
universe already contains exactly one heap:

```text
Untrusted and evolving database machinery
─────────────────────────────────────────
RQL / SDA / ENR / query planner / optimizer
indexes / SDK / server / cluster / tooling
─────────────────────────────────────────
        formally specified boundary
─────────────────────────────────────────
Heap isolation kernel
─────────────────────────────────────────
heap-owned authoritative and derived objects
```

The query and interpretation layers do not possess a deployment-global data
reader. They can ask only:

```text
scan(HeapCap<A>) -> Row<A>
```

They cannot ask:

```text
scan(GlobalStore) -> Row<AnyHeap>
```

Therefore a future defect equivalent to:

```text
SELECT * FROM everything
```

means:

```text
SELECT * FROM everything visible through HeapCap<A>
```

It does not mean every object in the deployment.

This is the central architectural constraint of the heap design. A feature
that requires bypassing it is incompatible with heap isolation and MUST be
redesigned or rejected.

## 1.2 Required mathematical result

The implementation target is machine-checked **non-interference**:

```text
same(Heap A) + arbitrary changes to all other heaps
    => exactly the same data observation from Heap A
```

For mutation:

```text
execute(write, Heap A)
    => every Heap B where B != A remains unchanged
```

For returned data:

```text
every returned object is owned by Heap A
```

Section 3.9 defines these properties formally. They are not aspirational
security language; they are proof obligations for the isolation kernel.

## 1.3 Defense against future defects

The design assumes that parsers, query planners, optimizers, indexes,
caches, protocol handlers, and administrative tools may contain bugs.

The isolation kernel MUST make defects in those components unable to expand an
operation's heap authority.

A defect above the kernel may:

- return an incorrect subset of Heap A;
- return an incorrect computation over Heap A;
- reject a valid query;
- consume excessive permitted resources;
- crash the operation.

It MUST NOT:

- return data owned by Heap B;
- modify Heap B;
- construct a mixed-heap iterator or result;
- create an index, cursor, cache entry, backup payload, or recovery view
  containing more than one heap.

The proof does not assume that ordinary database machinery is correct. It
assumes only the declared trusted computing base in §3.12.

## 1.4 Claim ladder

Residiuum distinguishes three maturity levels.

### Level 1 — Heap namespaces

The API names heaps and scopes collections beneath them.

Permitted claim:

> Residiuum provides named logical heap namespaces.

This is organisation, not qualified security isolation.

### Level 2 — Kernel-enforced isolation

All data paths use the heap isolation kernel, raw global access is unavailable
above it, capability and differential tests pass, and complete-path review
finds no bypass.

Permitted claim:

> Residiuum enforces logical access isolation between cryptographically
> authorized heaps for the qualified deployment profile.

This remains an implementation assurance supported by architecture and tests.

### Level 3 — Formally verified non-interference

The state-machine safety properties are mechanically proved, the executable
Rust isolation kernel is proved to refine that model, implementation-to-model
assumptions are published, deliberately faulty upper layers remain confined,
and an independent security review is complete.

Permitted claim:

> Residiuum provides formally verified non-interference between heap-bound data
> operations for the qualified deployment profile.

No release may use a higher-level claim based solely on the existence of APIs,
tests, types, a model, or an unconnected implementation proof.

## 1.5 Non-negotiable consequences

To preserve the defining property:

- no Residiuum data-plane operation spans two heaps;
- no cross-heap join or transaction will be added later;
- no query layer receives a global data iterator;
- no caller-provided `HeapId` is treated as authority;
- every data-bearing authoritative or derived object has exactly one heap
  owner;
- equal data in different heaps is not represented by one co-owned logical or
  cryptographic object;
- deployment-wide administration may aggregate explicitly declassified
  operational metadata, never application data from several heaps;
- movement between heaps is separate export and import, creating
  destination-owned objects;
- any raw administrative or recovery mechanism capable of seeing several
  heaps remains inside the trusted isolation boundary and emits separate
  heap-bound outputs.

These constraints are intentional product properties, not temporary v1
limitations.

## 1.6 Self-contained authority, not RBAC

Heap access uses the private-CA model:

```text
Heap master key
    ├── signs Admin HeapKey
    ├── signs CRUD HeapKey
    └── signs Read HeapKey
```

Each HeapKey is a self-contained, holder-bound access certificate for exactly
one heap. Its signed claims contain all permitted operations.

The access path is:

```text
present HeapKey
    -> verify cryptographic authority locally
    -> construct HeapCap<H>
    -> execute
```

It is not:

```text
identify user
    -> look up groups
    -> expand roles
    -> look up permissions
    -> evaluate resource policy
    -> execute
```

Residiuum stores no human RBAC state for heap access. Applications may implement
RBAC or any other human access model above Residiuum.

The governing rule is:

> **A HeapKey carries cryptographic proof of authority. Residiuum does not ask an
> authorization database whether that authority exists.**

The certificate is validated when establishing a channel. With an unchanged
security revision, the data hot path requires only resident capability,
revision, epoch, rights, constraint, time, and lease checks.

Strict cycling replaces the heap master public key and makes every previously
issued HeapKey cryptographically inert. The optional graceful profile keeps
only the immediately previous generation temporarily usable, minus an
always-resident blacklist.

## 2. Vocabulary

### 2.1 Deployment

A running embedded instance, server, or cluster exposing one administrative
and connection surface.

### 2.2 Heap

A named logical scope containing collections, streams, data history, derived
structures, and governing policy.

A heap has stable identity independent of its current name and physical
placement.

### 2.3 Collection

A named map of stable keys to current values with retained history underneath,
as defined by [DX_SPEC.md](../../reference/product/DX_SPEC.md).

### 2.4 Stream

A named append-oriented sequence within one heap.

### 2.5 System holder

An application, service, worker, agent, pipeline, or administrative tool that
holds a cryptographic keypair and presents a `HeapKey`.

Residiuum authorizes systems. Human users, organisational roles, groups, and
business policy belong to the application above Residiuum.

### 2.6 Heap master key

The private authority key created for one heap. Its public counterpart is
pinned to that heap.

The master key has no data permissions. It exists only to issue HeapKeys and
authorize local authority mutations and cycling through the local tool.

### 2.7 `HeapKey`

A self-contained, cryptographically signed access certificate for exactly one
heap. It contains the holder public key, permitted operations, constraints,
authority generation, validity bounds, and issuer signature.

### 2.8 `HeapCap<H>`

An unforgeable in-process capability constructed by the heap isolation kernel
after successful HeapKey and holder-proof validation. `H` brands exactly one
heap.

### 2.9 Authority snapshot

An immutable, always-resident view of one heap's current authority generation,
trusted master public key, optional grace generation and deadline, resident
blacklist, and authority revision.

### 2.10 Heap handle

An SDK or server-side handle bound to exactly one `HeapCap<H>`.

### 2.11 Physical store

An implementation durability and recovery boundary. A heap MAY occupy one
store, part of a store, or multiple stores. Store layout is not part of the
public heap contract.

### 2.12 `DeploymentId` and `AuthorityEpoch`

`DeploymentId` identifies one installation or cluster. `AuthorityEpoch`
identifies the one deployment incarnation currently permitted to serve a
particular heap identity.

`HeapId` is durable data identity. `DeploymentId` and `AuthorityEpoch` provide
fencing. Restoring the same `HeapId` elsewhere does not create a second valid
serving authority without an explicit takeover that advances the epoch.

### 2.13 Heap security snapshot

The immutable, always-resident decision state for one heap. It combines the
authority snapshot, heap administrative state, access-relevant policy, their
revisions, and the current serving epoch. Section 8.12 defines it.

## 3. Governing principles

### 3.1 Heap context is mandatory

Every collection, stream, query, index, history, watch, export, backup,
lifecycle, and recovery operation MUST have an unambiguous heap context.

There is no deployment-global collection namespace.

### 3.2 Identity is not a name

Heap names are mutable lookup labels. `HeapId` is authoritative identity.

Renaming a heap MUST NOT change the identity of its collections, data,
history, policies, backups, or audit records.

### 3.3 Default deny

The absence, corruption, ambiguity, staleness, or unavailability of the
resident heap security snapshot MUST deny access. It MUST NOT fall back to
deployment-wide read or write access.

### 3.4 Complete-path enforcement

Heap isolation is valid only if every path to data enforces it.

Securing CRUD while leaking through indexes, history, backup, salvage, query,
logs, or diagnostics is non-conforming.

### 3.5 Names are not security boundaries

Collection prefixes, heap-name prefixes, URL strings, directory names, and
client-side filtering do not establish isolation.

Authorization is evaluated against resolved immutable identity.

### 3.5.1 Systems are the database subjects

Residiuum does not maintain human users, groups, roles, role inheritance, or
principal-to-permission grants.

Applications MAY implement RBAC, ABAC, ACLs, relationship policy, subscription
policy, or any other human authorization model above Residiuum. The resulting
application operation reaches Residiuum through the application's HeapKey.

The governing separation is:

> Residiuum authorizes systems. Systems authorize people.

### 3.5.2 Authority travels with the channel

The authorization decision is encoded and cryptographically authenticated
inside the presented HeapKey.

Residiuum MUST NOT perform a user, role, group, grant, permission, or revocation
database lookup on the request path.

After channel establishment, ordinary operations use an in-memory
`HeapCap<H>`, a security-revision comparison, and rights/constraint checks.

### 3.6 Logical isolation is not physical isolation

Heap authorization protects against systems using Residiuum interfaces.

It does not by itself protect against:

- operating-system administrators;
- direct access to storage media;
- code executing inside the same trusted embedded process;
- process-memory compromise;
- a holder explicitly issued valid HeapKeys for multiple heaps;
- side channels inherent in shared physical resources.

A system legitimately issued separate HeapKeys for two heaps can read them
through two separate handles and combine the results in application memory.
Residiuum prevents one heap-bound operation from doing so; it cannot control
what an authorized caller does after separate results leave the database.

Deployments requiring protection against those threats need separate stores,
processes, operating-system identities, key domains, or hosts as defined by
[DATABASE_DOCTRINE.md](../../reference/product/DATABASE_DOCTRINE.md).

### 3.7 Physical organization remains private

The implementation MAY colocate or separate heap material. No API contract
depends on observable directory layout.

### 3.8 Recovery preserves scope

Salvage and recovery MUST preserve the heap identity associated with recovered
material or report that identity as unavailable. Recovery MUST NOT silently
assign material to another heap.

### 3.9 Formal non-interference contract

Let:

- `H` be the set of heap identities;
- `K` be the set of cryptographic holder identities;
- `O` be the set of data-bearing objects;
- `Ownership = Known(HeapId) | Unknown | Conflict`;
- `owner: O -> Ownership` be the total ownership-evidence function;
- `S[h]` be all authoritative and derived state owned by heap `h`;
- `D(S)` be the explicitly allowlisted declassification of deployment state;
- `Cap(k, h, r, g, v)` be an unforgeable capability for holder key `k`, rights
  `r`, heap `h`, authority generation `g`, and security revision `v`;
- `Exec(S, G, cap, op)` execute operation `op`;
- `Obs(...)` be the complete caller-visible functional observation: returned
  bytes, metadata, result cardinality and order, pagination, errors, protocol
  behavior, and termination, excluding only leakage explicitly permitted by
  the named isolation profile.
- `equiv_p(x, y)` mean equality after removing only the leakage fields
  explicitly permitted by named profile `p`.

For any heaps `a` and `b` where `a != b`, the following MUST hold.

#### Ownership disjointness

```text
S[a] intersection S[b] = empty
```

Every data-bearing authoritative or derived object has exactly one heap owner.
No object is co-owned by two heaps.

Cross-heap physical deduplication of data-bearing objects is prohibited in the
qualified isolation profile. Equal bytes in two heaps remain separately owned
logical and cryptographic objects.

#### Read confinement

For:

```text
cap = Cap(k, a, r, g, v)
result = Exec(S, G, cap, read_op)
```

every data-bearing object in `result` satisfies:

```text
owner(object) = Known(a)
```

An unlabeled object, an object with invalid ownership evidence, or an object
owned by another heap is rejected before it reaches the caller.

#### Write confinement

For:

```text
(S', result) = Exec(S, G, Cap(k, a, r, g, v), write_op)
```

the state of every other heap is unchanged:

```text
for all b in H where b != a: S'[b] = S[b]
```

This applies to authoritative data and all derived consequences, including
indexes, caches, history, replication queues, audit attribution, backup state,
and lifecycle work.

An operation MAY consume shared resources and append explicitly A-attributed
audit or aggregate counters as permitted by the named profile. It MUST NOT
write B-labeled metadata, change B policy/state, evict or delete B data as a
correctness action, or relabel a shared effect as belonging to B.

#### Data non-interference

Consider two deployment states `S1` and `S2` that are identical for heap `a`
but differ arbitrarily in every other heap:

```text
S1[a] = S2[a]
```

For every operation using a capability for heap `a`, complete functional
observations MUST satisfy:

```text
equiv_p(
  Obs(Exec(S1, D(S1), Cap(k, a, r, g, v), op)),
  Obs(Exec(S2, D(S2), Cap(k, a, r, g, v), op))
)
```

Changing the names, collections, keys, values, indexes, history, number, or
contents of other heaps cannot change functional output from heap `a`.

Timing, aggregate load, and availability leakage MAY be excluded only by an
explicitly named profile in §13. Exclusion MUST NOT silently remove errors,
result sizes, ordering, pagination, or other functional behavior from `Obs`.

`D` is a closed, reviewed allowlist. Calling metadata “operational” does not
declassify it. A field derived from another heap is invisible to an ordinary
heap holder unless the named profile explicitly permits that leakage.
Application data, functional errors, result size/order, and pagination are
never removable by `equiv_p`.

#### No composition

There is no data-plane operator:

```text
combine(S[a], S[b])
```

for `a != b`.

Export followed by explicit application-level import creates new objects owned
by the destination heap. It is not a cross-heap query and MUST record
provenance without retaining live authority to the source.

The checked state-machine model MUST include:

- capability creation and security-snapshot replacement;
- administrative-state and access-policy changes;
- concurrent operations and their authorization linearization points;
- authority creation, cycling, grace, expiry, and blacklist mutation;
- crash/restart and partial durable transitions;
- cluster leases, fencing, and stale-node rejection;
- restore, takeover, and permanent identity tombstones;
- ownership states `Known`, `Unknown`, and `Conflict`;
- caller-visible output and externally visible side effects.

### 3.10 Isolation kernel

The mathematical contract is enforced by a small **heap isolation kernel**
below RQL, SDA, ENR, query planning, indexes, and ordinary SDK logic.

The kernel is the only component permitted to convert a validated HeapKey,
holder proof, resident heap security snapshot, and durable heap identity into a
heap capability.

Its conceptual interfaces are:

```text
validate(HeapKey, holder_proof, HeapSecuritySnapshot) -> HeapCap<H>

open_collection(HeapCap<H>, name) -> Collection<H>
scan(Collection<H>)               -> Stream<Row<H>>
get(Collection<H>, key)           -> Option<Row<H>>
put(Collection<H>, key, value)    -> Receipt<H>
join(Stream<A, H>, Stream<B, H>)  -> Stream<C, H>
```

`H` denotes one bound heap identity. Query operators accept inputs carrying
the same `H` and return output carrying that `H`.

For dynamic heap IDs, `H` is a fresh generative brand for one capability, not
an ordinary shared Rust type parameter. The implementation uses an
unforgeable runtime capability plus Rust type/module privacy. A raw
user-supplied `HeapId` is not a capability.

Where an API shape cannot preserve the generative brand statically, the
isolation kernel MUST validate exact capability-instance identity at runtime.
Equality of heap names, public IDs supplied by a caller, or Rust concrete types
is insufficient. Query builders accept only collections produced by their own
bound capability.

The query engine receives only `Collection<H>` and `Stream<_, H>`. It does not
receive a raw deployment store, global iterator, or method capable of
enumerating other heaps.

Consequently, a query-planner defect equivalent to:

```text
SELECT * FROM everything
```

can enumerate only the universe exposed by `HeapCap<H>`. The planner cannot
forget a heap predicate because no heap predicate is added by the planner.

### 3.11 Defense in depth

The qualified isolation profile additionally requires:

- heap identity in every authoritative and derived object identity;
- heap identity in cache, cursor, resume, transaction, dedup, and operation
  keys;
- independent per-heap cryptographic ownership in the protected profile;
- an output boundary that validates `owner(object) == bound_heap`;
- fail-closed quarantine for missing, corrupt, or conflicting ownership;
- no raw-store access from query, SDK, index, export, or ordinary recovery
  modules;
- no `unsafe` code inside the isolation kernel unless separately justified and
  verified;
- differential non-interference tests;
- formal model checking of the kernel state machine;
- a narrowly enumerated trusted computing base.

The output ownership check is independent of the input scoping mechanism. A
single defect must not both select and disclose another heap's object.

### 3.11.1 Independently recoverable ownership evidence

Every independently recoverable data-bearing unit MUST carry or inherit
integrity-protected ownership evidence that remains available when unrelated
bytes are missing.

This includes:

- item frames and large-value chunks;
- segment and index pages;
- stream blocks and history records;
- manifests and replication items;
- backup units;
- temporary spills and materializations;
- recovery fragments.

Where encryption is used, `HeapId` and immutable subordinate object identity
MUST be authenticated as associated data or equivalently bound to the unit.
An ownership label stored only in a directory name, catalog, segment header,
or other single point whose loss makes healthy subordinate units ambiguous is
non-conforming for the damage-tolerant recovery profile.

Invalid, missing, or conflicting evidence yields `Unknown` or `Conflict` and
is quarantined. It is never repaired by guessing from a mutable name or nearby
material.

### 3.12 Trusted computing base and honest guarantee

No software can mathematically guarantee behavior in the presence of an
arbitrary defect in the compiler, kernel, hardware, cryptography, or the code
that implements the proved model.

Residiuum's guarantee is therefore precise:

> Assuming the heap isolation kernel, cryptographic primitives, compiler,
> runtime, and operating system behave according to their specified models, a
> defect outside that trusted computing base cannot cause data from one heap
> to be returned through another heap.

The isolation kernel MUST remain small enough to review, model, fuzz, and
eventually verify directly. Adding a new raw access path expands the trusted
computing base and requires an explicit security review.

The published trusted computing base enumerates exact crates/modules and
includes the capability constructor, heap-scoped storage adapter, ownership
codec/verifier, output guard, cryptographic verifier, security-snapshot
publisher, and the minimum runtime/codec dependencies they invoke.

The physical storage engine does not expose a raw locator-based read or global
iterator to upper layers. Physical keys and object references are derived or
validated inside the heap-scoped adapter from an unforgeable capability.
Serialization, `Debug`, cloning, deserialization, FFI, plugin, and `unsafe`
paths cannot manufacture `HeapCap`, `Collection<H>`, or raw object locators.

Build-time dependency rules reject imports of raw storage modules from query,
SDK, index, export, network, and ordinary recovery code. Any exception changes
the TCB manifest and blocks H6 pending proof and review.

## 4. Logical hierarchy

The normative hierarchy is:

```text
Deployment
└── Heap
    ├── Collection
    │   └── Key
    └── Stream
        └── Event
```

The fully qualified logical identities are:

```text
(HeapId, CollectionName, Key)
(HeapId, StreamName, EventId)
```

Every collection and stream has an immutable, never-reused `CollectionId` or
`StreamId`. Names are heap-local mutable labels for those identities.

Durable object identity is:

```text
(HeapId, CollectionId, Key)
(HeapId, StreamId, EventId)
```

Names MAY appear in APIs but MUST be resolved to immutable identity before an
operation begins. Deleting and recreating the same name creates a new
subordinate identity and cannot retarget stale handles.

The following are invalid:

```text
(CollectionName, Key)                    # missing heap
(HeapName, CollectionName, Key)          # mutable name used as durable identity
(HolderPublicKey, CollectionName, Key)   # holder is not data identity
```

## 5. Heap identity

### 5.1 `HeapId`

`HeapId` is an opaque UUIDv4 identifier in v1, encoded in canonical lowercase
hyphenated form. Its 122 random bits are generated by an operating-system
cryptographic random source.

Requirements:

- globally collision-resistant generation;
- immutable for the lifetime of a heap;
- not derived solely from the heap name;
- not derived from a filesystem path;
- represented canonically in APIs and evidence;
- preserved by rename and physical movement;
- covered by integrity protection wherever it participates in durable
  identity.

Human users normally use heap names. Tools and protocols expose `HeapId` for
diagnosis, audit, migration, recovery, and unambiguous automation.

### 5.2 Heap name

A heap name is a deployment-local mutable label.

The v1 portable name profile is:

- 1 to 63 UTF-8 bytes;
- lowercase ASCII letters, decimal digits, `-`, `_`, and `.`;
- begins with a lowercase ASCII letter or decimal digit;
- ends with a lowercase ASCII letter or decimal digit;
- no consecutive `..`;
- case-sensitive, with no case folding;
- no Unicode normalization because the portable profile is ASCII.

The reserved names are:

- `system`;
- `admin`;
- `default`;
- any name beginning with `_residiuum`.

`default` is reserved for compatibility use and MUST NOT be created as an
ordinary user heap.

Implementations MAY later support display names separately. Display names are
never lookup or security identity.

### 5.3 Name uniqueness

Two live heaps in one deployment MUST NOT have the same name.

Name lookup MUST resolve to either:

- exactly one `HeapId`; or
- no result.

Ambiguous name state is a readiness failure and denies access.

### 5.4 Rename

Rename changes only the lookup label.

Rename MUST:

- be atomic with respect to name resolution;
- preserve `HeapId`;
- preserve issued HeapKeys because their signed claims bind to `HeapId`;
- invalidate name-resolution caches;
- produce an audit record;
- reject collision with an existing or reserved name.

After rename, an old name MUST NOT silently resolve to a newly created heap
for an already-bound handle.

### 5.5 Reuse of old names

An old heap name MAY be reused only after the prior heap is retired and the
name-quarantine period has elapsed. v1 defaults to 30 days and permits only a
longer deployment setting.

Name reuse creates a new `HeapId`.

Clients MUST NOT treat `(deployment, heap name)` as permanent identity.

## 6. Heap states

A heap has one of the following administrative states:

Administrative state is part of `HeapSecuritySnapshot`. Every state change
atomically increments `security_revision` and therefore invalidates previously
validated capabilities before their next operation.

### 6.1 `active`

Authorized reads and writes are permitted.

### 6.2 `read_only`

Authorized reads are permitted. Ordinary mutations are rejected.

Maintenance operations explicitly valid in read-only state MAY continue.

### 6.3 `suspended`

Ordinary reads and writes are denied. HeapKeys with explicit administration,
audit, backup, or recovery rights MAY inspect or operate on the heap.

Suspension is reversible and is not deletion.

### 6.4 `retired`

The heap is removed from ordinary discovery and cannot return to `active`.
Data remains subject to retention, hold, backup, and purge policy.

Retirement is not purge.

### 6.4.1 `purging`

`purging` is a durable internal administrative state entered only from
`retired` after an authorized purge plan is fixed. It admits no ordinary data
access. A crash resumes the same plan and operation ID. Incomplete coverage
returns to `retired`; complete declared coverage advances to `purged`.

### 6.5 `purged`

The managed purge plan has completed to its declared coverage and the heap is
represented only by permitted audit and purge evidence.

If any managed domain was unavailable, the heap remains `retired` with an
incomplete-purge result. It MUST NOT be reported as `purged`.

### 6.6 State transitions

```text
             ┌──────────────┐
             │              ▼
active <-> read_only     suspended
   │             │          │
   └─────────────┴──────────┘
                 │
                 ▼
              retired
                 │
                 ▼
              purging
                 │
                 ▼
               purged
```

Transition to `retired` and any purge operation require separate high-impact
rights, confirmation, and durable audit.

`purged` is terminal. A purged `HeapId`, its authority epochs, and its
anti-rollback tombstone are never reused or silently removed. Restoring old
media cannot reactivate it.

The exact transition table is:

| From | Operation | To | Remembered resume state |
|---|---|---|---|
| `active` | set read-only | `read_only` | none |
| `read_only` | set active | `active` | none |
| `active` | suspend | `suspended` | `active` |
| `read_only` | suspend | `suspended` | `read_only` |
| `suspended` | resume | remembered state | cleared |
| `active`, `read_only`, `suspended` | retire | `retired` | cleared |
| `retired` | begin purge | `purging` internal substate | none |
| `purging` | abort/incomplete purge | `retired` | none |
| `purging` | complete purge | `purged` | none |

Every other pair is invalid. Repeating an operation with the same
`operation_id` returns its original receipt; repeating it with a new operation
ID against the already-reached state is `InvalidStateTransition`. `purging` is
durable but never grants ordinary data access and is reported externally as
`retired` plus purge progress.

Each operation has an authorization linearization point:

- a single request linearizes immediately before its first externally visible
  read or durable effect;
- a transaction linearizes at commit;
- a stream, watch, query, backup, recovery job, and background task rechecks at
  the interruption points and maximum interval defined in §8.16.

If state or security revision changes before that point, the operation
rechecks the current immutable snapshot or performs no further effect. A
mutation authorized before a transition but not yet committed does not retain
authority merely because it was queued earlier.

## 7. API model

### 7.1 Heap selection

Heap selection produces a heap-bound handle.

Qualified remote Rust:

```rust
let heap = Residiuum::connect_heap(
    "residiuum://localhost:7434/accounts",
    RemoteHeapOptions::new(tls, credential)
        .expected_heap_name("accounts"),
)?;
let users = heap.collection("users")?;
```

Trusted embedded multi-heap Rust:

```rust
let deployment = Residiuum::open_deployment("./app.residiuum")?;
let heap = deployment.heap("accounts")?;
let users = heap.collection("users")?;
```

The remote URL path is an expected human label, not authority. The signed
credential supplies the `HeapId`; the label is compared only after successful
authentication. Embedded name lookup occurs through the trusted deployment
handle and resolves to immutable identity before returning `Heap`.

For a qualified remote connection, the HeapKey's signed `HeapId` is the
selection identity. A supplied name or URL label is only an expected human
label and is checked after confidential authentication; it never causes the
server to test authority for a different heap. SDK configuration MAY map a
friendly name to the certificate and immutable ID locally.

### 7.2 Bound-handle invariant

A `Heap` handle contains:

- deployment connection or embedded backend;
- resolved `HeapId`;
- validated HeapKey fingerprint and holder-key identity;
- authority generation, serving epoch, and security revision;
- cryptographically encoded rights and constraints;
- no public method for changing its `HeapId` in place.

Changing heaps creates a new handle.

### 7.3 Collection access

Collection lookup occurs only through a heap handle:

```rust
heap.collection("users")
```

The preferred API does not expose:

```rust
deployment.collection("users")
```

except as a documented compatibility surface bound to the legacy default heap.

### 7.4 Request scope

Every qualified v1 remote data request uses an unforgeable server session
binding established by a successfully validated HeapKey exchange. It contains
no caller-selected `HeapId`. Other future profiles may use an explicitly
authenticated logical-channel identifier, but never an unauthenticated heap
field.

The server MUST NOT trust a collection name prefix, URL, request body, or
header to infer or replace heap identity.

Multiplexing requests for different heaps on the same bound transport channel
is prohibited in the qualified v1 profile.
Selecting another heap requires a separately authenticated logical channel and
fresh generative capability. Future transport multiplexing MAY carry several
such isolated logical channels but never one capability or operation spanning
them.

### 7.5 Stale handles

A bound handle remains bound to its original `HeapId` after rename.

If the heap is retired, suspended, cycled to a new authority generation, or
the presented key is blacklisted, the next operation fails according to the
new state. A `HeapCap` MUST NOT outlive its security revision, authority
generation, serving epoch, validity bound, or grace boundary.

### 7.6 Embedded operation

An embedded process is normally the system holder.

An embedded heap handle provides namespace correctness but does not isolate
mutually hostile code within the same process.

Applications requiring in-process human authorization implement it above the
heap handle. Residiuum MUST NOT describe HeapKeys as protection from arbitrary
code already running with the same process authority and access to holder key
material.

## 8. HeapKey security model

### 8.1 Governing model

Each heap is its own private certificate authority.

At heap creation:

```text
Heap H
└── pins MasterPublicKey(H, generation 1)
```

The corresponding `MasterPrivateKey` is generated and retained by the heap
owner, HSM, or protected key provider and MUST NOT be stored in plaintext by
Residiuum. The preferred ceremony never places it in Residiuum memory.

The master key is not a database login. It has no read, write, query, backup,
recovery, administration, or data-encryption permission.

It exists only to:

- issue HeapKeys through the local authority tool;
- authorize grace and blacklist mutations through the local authority tool;
- cycle the heap to a new master key through the local authority tool.

### 8.2 First-level and second-level access

Residiuum's first-level access subjects are systems holding HeapKeys.

```text
application / worker / agent / admin tool
                    │
                    │ HeapKey
                    ▼
                 Residiuum
```

Human access control is second-level application policy:

```text
human
  │
  ▼
application
  │ RBAC / ABAC / ACL / business policy
  ▼
application HeapKey
  │
  ▼
Residiuum
```

Residiuum MUST NOT implement human RBAC as part of heap authorization. An
external identity or role system MAY decide which system receives a HeapKey,
but it has no place in channel verification or the isolation kernel.

### 8.3 HeapKey certificate

A HeapKey credential has two deliberately separate parts:

- `HeapKeyCertificate`: public, self-contained signed authority;
- `HolderSecretKey`: private proof-of-possession material held by the system.

The certificate format MUST NOT contain, serialize, log, or export the holder
secret key. SDK types for the two parts are distinct and private-key export is
never an incidental consequence of serializing a certificate or handle.

The signed certificate contains at least:

```text
HeapKeyCertificate {
    protected_header { profile_version, algorithm },
    payload {
        deployment_id,
        heap_id,
        authority_epoch,
        authority_generation,
        certificate_id,
        holder_public_key,
        rights,
        constraints,
        not_before,
        expires_at,
        audience,
        issuer_master_key_id,
    },
    signature,
}
```

Every protected header and payload field is covered by the master
signature, including profile, deployment, issuer, algorithm context, heap,
epoch, generation, rights, constraints, holder key, validity, audience, and
certificate identity.

Signing authenticates the claims. The qualified v1 certificate is not
encrypted. A deployment may wrap credential files at rest, but it removes that
outer wrapping before protocol use; wrapping is not part of certificate or
wire encoding and is never a substitute for signature verification.

One certificate belongs to exactly one heap. Wildcard, heap-set, and
deployment-wide application-data certificates are prohibited.

The frozen v1 profile uses
[COSE Sign1](https://www.rfc-editor.org/rfc/rfc9052.html) and
[deterministic CBOR](https://www.rfc-editor.org/rfc/rfc8949.html):

- a COSE Sign1 structure using deterministic CBOR;
- Ed25519 master and holder signing keys;
- SHA-256 certificate, public-key, and transition fingerprints;
- exactly one accepted signature algorithm, with no algorithm chosen from
  untrusted certificate data;
- definite-length encoding, shortest integer encodings, sorted map keys, and
  rejection of duplicate keys;
- maximum encoded certificate size 16 KiB, maximum constraint count 64, and
  bounded string and collection lengths;
- a 128-bit cryptographically random `certificate_id`, unique within heap and
  generation; security decisions use the full certificate fingerprint when
  collision ambiguity would matter;
- unknown top-level fields rejected unless explicitly registered as
  non-critical for this profile;
- unknown rights, constraints, versions, algorithms, and critical fields
  rejected;
- no downgrade from an unsupported profile to a legacy or bearer profile.

Signature inputs use distinct fixed domain strings:

```text
RESIDIUUM-HEAPKEY-CERTIFICATE-V1
RESIDIUUM-HEAPKEY-HOLDER-PROOF-V1
RESIDIUUM-HEAP-AUTHORITY-TRANSITION-V1
RESIDIUUM-HEAP-AUTHORITY-MUTATION-V1
```

One signing key is used for one algorithm and purpose. Key identifiers only
confirm the expected pinned key; attacker-controlled identifiers never select
an alternate verification key.

For the qualified data service, `audience` is the fixed protocol value
`residiuum:data:v1`; `DeploymentId` and `AuthorityEpoch` provide installation and
incarnation binding. Individual node hostnames are not certificate audiences,
so ordinary failover within the same valid cluster does not require
reissuance. A certificate for another deployment, restored incarnation,
protocol service, or epoch is rejected.

Certificates are not confidential. Anyone obtaining one may learn its heap
identity, rights, constraints, holder public key, and validity. Deployments
requiring confidentiality of that metadata protect certificate storage and
transport; wrapping certificate bytes does not change authorization semantics.

### 8.4 Rights

The v1 right vocabulary is:

| Right | Meaning |
|---|---|
| `read` | Read live collection and stream data |
| `read_history` | Read retained versions and history |
| `write` | Put, append, and logically delete ordinary data |
| `index_admin` | Create, rebuild, and remove derived indexes |
| `export` | Export logical heap data |
| `backup` | Create and inspect heap-scoped backups |
| `restore` | Restore into a declared destination heap |
| `audit_read` | Read heap-scoped security and operation audit |
| `policy_admin` | Change heap access and governance policy within mandatory deployment limits |
| `lifecycle_admin` | Change ordinary lifecycle and tier policy |
| `hold_admin` | Place or release retention/legal holds |
| `recover` | Scrub, examine, salvage, and repair |
| `heap_admin` | Rename, suspend, resume, and inspect protected heap metadata |
| `retire` | Irreversibly retire the heap |
| `purge` | Execute an authorized purge plan |
| `data_key_admin` | Manage the heap's data-encryption policy |
| `placement_admin` | Change heap placement within mandatory deployment policy |

Rights MAY be represented as a fixed bitmap in the frozen profile.

`heap_admin` does not include issuing HeapKeys, cycling authority, reading
application data, or any other right not explicitly present.

No issued HeapKey can issue another HeapKey in v1. All issued certificates
are signed directly by the current heap master.

Rights are independent unless the frozen operation matrix explicitly requires
several. In particular:

- `backup`, `export`, and `recover` authorize their named data-disclosure paths
  without implicitly granting ordinary `read`;
- `data_key_admin` does not grant plaintext data access;
- `restore` never grants authority over a source heap;
- a compound operation checks every required right before its first read or
  effect;
- no right implies `purge`, `retire`, `policy_admin`, `hold_admin`, or another
  high-impact right.

The normative RPC/SDK operation-to-rights matrix is generated from one
machine-readable registry and tested for completeness. Only an operation with
registry status `active` is remotely callable.

### 8.4.1 Constraint profile

Constraints are a closed, deterministic narrowing language. v1 permits only:

- immutable `CollectionId` and `StreamId` allowlists;
- operation allowlists narrower than the rights bitmap;
- maximum request and result bytes;
- maximum query work and duration;
- source network constraints when the qualified transport exposes a trusted
  source identity.

Constraints can only reduce signed rights. They cannot consult mutable human,
group, or role state; execute SDA; call external services; perform network or
storage I/O; or expand authority.

All applicable constraints are decoded into the resident `HeapCap` during
validation. Unknown or malformed constraints fail closed.

### 8.5 Holder proof

A HeapKey certificate is bound to `holder_public_key`.

The holder proves possession of the corresponding private key when
establishing the channel. The proof MUST bind:

- certificate hash;
- `DeploymentId` and `AuthorityEpoch`;
- requested heap identity;
- server audience;
- a fresh 256-bit server nonce;
- the TLS 1.3
  [`tls-exporter`](https://www.rfc-editor.org/rfc/rfc9266.html) channel
  binding;
- protocol profile.

The canonical proof payload is:

```text
HolderProof {
    profile_version,
    proof_id,
    created_at,
    certificate_hash,
    deployment_id,
    heap_id,
    authority_epoch,
    audience,
    server_nonce,
    tls_exporter,
    signature,
}
```

Every payload field except the signature is signed. `proof_id` contains 128
cryptographically random bits.

The proof is an Ed25519 signature over the deterministic transcript with the
holder-proof domain string. It expires after 60 seconds and is accepted once
by the connection state machine. The server stores no deployment-wide proof-ID
set; §33.8's single-use connection nonce and TLS exporter make a replay invalid
on both the same and another connection. Nonces are issuer-bound, size-limited,
unpredictable, and never accepted by another deployment or logical channel.

In a cluster, the nonce identifies its issuing node and the proof returns to
that node; replay state is therefore node-local and needs no authorization
database. A retry on another node obtains and signs a fresh nonce.

The qualified network profile requires authenticated TLS 1.3 server identity.
TLS termination, proxying, resumption, and transport migration are conforming
only when the Residiuum endpoint that verifies the proof has access to the
correct exporter value for that exact logical channel. Otherwise the proof
exchange occurs inside a separately end-to-end protected channel.

Possession of a copied certificate without the holder private key grants no
access in the qualified profile.

Bearer-only HeapKeys, if ever supported, are a separately named weaker profile
and do not qualify for the strong isolation claim.

### 8.6 Network validation

The network service receives a HeapKey certificate and holder proof.

It validates:

```text
certificate.profile == qualified_profile
&& certificate.deployment_id == serving_deployment
&& certificate.heap_id == selected_heap
&& certificate.authority_epoch == current_serving_epoch
&& certificate.generation is accepted by HeapSecuritySnapshot
&& certificate signature verifies under accepted master public key
&& holder proof verifies under certificate.holder_public_key
&& requested operation is contained in certificate.rights
&& request satisfies certificate.constraints
&& certificate is within its validity interval
&& certificate audience matches this service
&& certificate is not rejected by the resident blacklist
```

Successful validation creates `HeapCap<H>`.

Capability construction computes:

```text
effective_rights = certificate.rights intersect resident_policy.allowed_rights
effective_constraints =
    deterministic_conjunction(certificate.constraints, resident_policy.constraints)
```

Administrative state may deny construction or remove state-prohibited rights.
Resident policy can only narrow the signed certificate.

Validation is local and deterministic. It performs no user, role, group,
grant, permission, or revocation database lookup.

The service necessarily resolves the selected heap and reads its already
resident heap security snapshot. That routing and memory access MUST NOT be
described as an authorization-policy lookup.

### 8.7 Established-channel hot path

Each `HeapCap<H>` records:

```text
deployment_id
heap_id
authority_epoch
certificate_id
holder_key_id
rights
constraints
authority_generation
validated_security_revision
validity_deadline
```

The ordinary request hot path checks:

```text
cap.heap_id == bound_heap
&& cap.deployment_id == serving_deployment
&& cap.authority_epoch == current_serving_epoch
&& cap.validated_security_revision == current_security_revision
&& requested_operation in cap.rights
&& request satisfies cap.constraints
&& now < cap.validity_deadline
```

When the security revision is unchanged, the request performs no certificate
verification, blacklist membership check, or authorization I/O.

The performance contract is:

> HeapKey authorization removes authorization-policy lookup and evaluation
> from the data hot path.

Quantitative latency claims require published benchmarks against declared
alternatives.

### 8.7.1 Trusted time

Certificate and grace decisions depend on security time, not an unchecked wall
clock.

Each ready node maintains:

- a monotonic runtime clock;
- a durable `not_before_time_floor` that never decreases across restart;
- bounded wall-clock uncertainty;
- for clusters, a quorum-issued authority lease with an absolute expiry and
  maximum permitted uncertainty.

The time floor advances durably at least every 30 seconds, before a clean
shutdown, and as part of every authority mutation. After a crash, uncertainty
includes the maximum floor-persistence interval. A profile backed by trusted
hardware time MAY use its stronger bound instead.

Security time never moves backward. Observing a wall clock below the durable
floor, uncertainty beyond the profile bound, an expired authority lease, or
inability to establish trustworthy time makes time-dependent access fail
closed.

v1 limits:

- maximum HeapKey lifetime: 90 days;
- maximum holder-proof lifetime: 60 seconds;
- maximum graceful-cycle interval: 48 hours;
- maximum accepted clock uncertainty: 30 seconds.

Shorter deployment limits MAY be configured. Longer limits require a
separately named profile. Expiry and grace comparisons include clock
uncertainty in the deny direction: a key is accepted only when the node can
prove it is valid, and a previous generation is accepted only when the node
can prove grace has not ended.

### 8.8 No master key over the network

The Residiuum network protocol MUST NOT define an operation for:

- presenting a master private key;
- issuing a HeapKey;
- cycling heap authority;
- starting grace or mutating the blacklist;
- replacing a heap master public key;
- resetting the authority generation;
- changing the master-loss recovery profile;
- recovering a lost master key.

The network server MUST NOT parse or accept master private-key material.

A network `admin` HeapKey cannot issue keys or cycle authority.

The precise guarantee is:

> The Residiuum data-service executable has no protocol operation,
> client/local-control parser for master-authority event bytes, concrete
> master-key provider, or linked local-authority implementation capable of
> accepting a master key or originating a change to the master generation,
> grace, blacklist, recovery policy, or authority epoch.

This does not prohibit the restricted operational-event writer in §31.5.1.
That writer can commit only HeapKey-authorized state, policy, and rename
events. It cannot parse or construct a master-signed event and has no signing
capability.

The cluster profile necessarily replicates already committed, master-signed
public events between mutually authenticated control peers. Its narrowly
typed peer-only decoder is the sole exception and obeys §37.2. It cannot
originate an event, accept one from a client or local-control frame, or invoke
a master key. Replicating a completed public transition is not exercising the
master key over the network.

### 8.9 Local authority plane

Heap creation, HeapKey issuance, and master-key cycling occur through the
separate `residiuum-authority` executable running with declared operating-system
access. The qualified data-server process neither links a master-key provider
implementation nor contains a client/local master-authority mutation
dispatcher. The cluster-only verified replication exception is §37.2.

Illustrative commands:

```text
residiuum-authority heap create <name>
residiuum-authority key issue <heap> <issuance-request>
residiuum-authority authority cycle <heap>
residiuum-authority authority blacklist <heap> <certificate-or-holder-fingerprint>
```

Use through SSH is still network use at the operating-system layer. It is not
use through the Residiuum network protocol.

“Local-only issuance” is an implementation and custody boundary, not a claim
that a signature reveals where it was created. Anyone who steals a current
master private key can mint cryptographically valid HeapKeys away from the
host until the owner completes a hard cycle. The design prevents use of the
master itself as a network credential; it cannot make stolen signing power
harmless.

The executable has no TCP, UDP, HTTP, Residiuum RPC, or other inbound listener. It
accepts authority inputs only from its local terminal and protected local
files. As a client, it may connect to the qualified data server's local
barrier/reload Unix-domain endpoint. That endpoint accepts only these fixed
messages:

```text
begin_security_barrier(
    profile_version,
    DeploymentId,
    HeapId,
    operation_id,
    expected_current_head_hash
)

apply_committed_head(
    profile_version,
    DeploymentId,
    HeapId,
    operation_id,
    barrier_id,
    expected_new_head_hash
)

abort_uncommitted_barrier(
    profile_version,
    DeploymentId,
    HeapId,
    operation_id,
    barrier_id,
    expected_unchanged_head_hash
)

publish_committed_genesis(
    profile_version,
    DeploymentId,
    HeapId,
    operation_id,
    expected_new_head_hash
)
```

It accepts no certificate, authority event, private key, signature request,
policy body, or replacement key.

The v1 local-control wire format is frozen. Each message is one unsigned
32-bit big-endian length followed by exactly that many deterministic-CBOR
bytes. The length MUST be `1..=1024`; trailing bytes, indefinite lengths,
duplicate labels, unknown labels, and non-canonical encodings are rejected
before any state change. A request is the exact map:

| Label | Field |
|---:|---|
| 1 | profile version, uint = 1 |
| 2 | verb: 1 begin, 2 apply, 3 abort, 4 publish genesis |
| 3 | deployment ID, bstr(16) |
| 4 | heap ID, bstr(16) |
| 5 | operation ID, UUIDv4 bstr(16) |
| 6 | barrier ID, null for begin/publish; bstr(16) for apply/abort |
| 7 | expected head hash: current/new/unchanged according to verb, bstr(32) |

A response is the exact map:

| Label | Field |
|---:|---|
| 1 | profile version, uint = 1 |
| 2 | status: 1 barrier-begun, 2 applied, 3 aborted, 4 genesis-published, 255 error |
| 3 | operation ID copied from request, bstr(16) |
| 4 | barrier ID, bstr(16) after a successful begin/apply/abort; otherwise null |
| 5 | observed head hash, bstr(32), or null when no valid head was observable |
| 6 | error code: 0 success, 1 malformed, 2 peer-denied, 3 wrong-deployment, 4 wrong-heap, 5 head-mismatch, 6 barrier-conflict, 7 no-barrier, 8 timeout, 9 unavailable, 10 internal, 11 genesis-conflict |

The socket has a configured absolute path outside `data_root`. Its parent is
owned by the server UID with mode 0700; the socket has mode 0600. Every path
component is opened without following symbolic links. The server obtains the
kernel-supplied peer process credentials and requires the exact configured
authority-operator UID; an application-supplied UID is never consulted. The
reference v1 filesystem profile uses the server UID as that operator UID, so
an administrator enters the account through a separately audited OS control
such as `sudo` or SSH. Other ACL or service-identity models require a named
profile amendment.

One heap has at most one active barrier. `operation_id` is the idempotency key:
repeating the byte-identical request returns the original bounded response;
reusing it with different bytes returns `barrier-conflict`. The server stores
the active request and response in the control journal defined below before
replying. A disconnected client does not release a barrier.

The journal lives at
`<authority_root>/<deployment>/<heap>/control/` and uses `barrier.a.cbor`,
`barrier.b.cbor`, and `barrier-current` with the same write/sync/selector
algorithm as §35.1. Its wrapped payload is exactly:

```text
{1: 1, 2: request_bstr, 3: response_bstr, 4: phase, 5: file_sequence}
```

`phase` is 0 intent, 1 begun, 2 head-applied, or 3 aborted. Before changing
admission, the server durably writes phase 0; after draining it durably writes
phase 1 and only then replies to `begin`. The complete payload is at most
4,096 bytes. A terminal copy is retained as
`control/receipts/<operation-id-hex>.cbor`; receipts are immutable and use the
same wrapper. After syncing a terminal receipt, the server removes both live
journal slots and the selector and directory-syncs `control/`; a crash during
cleanup resumes from the terminal receipt. On restart, an intact phase-0 or
phase-1 journal keeps the heap
unavailable. It may abort only if the anchored head still equals the begin
request's label 7. Otherwise it awaits an `apply` carrying the expected new
head and verifies exact equality, or enters protected recovery; it does not
guess that an arbitrary changed head belongs to this operation. Corrupt or
ambiguous control artifacts also keep that heap unavailable; complete absence
of both journal slots and selector means no barrier. The protected recovery
procedure must establish the anchored head and write a terminal receipt before
clearing damaged artifacts. The server never resumes merely because the
control client disconnected or restarted.

`begin_security_barrier` stops new admission for that heap, pauses autonomous
maintenance before its next durable effect, drains admitted mutations and
externally visible output for at most one second, and returns a random barrier
ID plus the still-current anchored head hash. The authority tool commits only
when that hash still matches.

`apply_committed_head` rereads and verifies already committed public authority
state from `authority_root`, publishes exactly the expected head, and resumes
admission. `abort_uncommitted_barrier` resumes only when the anchor still
equals the expected unchanged head; it can never discard a committed
transition. A tool or server crash leaves the heap unavailable until one of
those two facts is proved during restart.

`publish_committed_genesis` is valid only when the `HeapId` has no live slot.
The server rereads an epoch-1 creation root and head from `authority_root`,
requires head label 25 to match the staged sequence-1 descriptor under
`data_root`, verifies the creation tuple in §8.9.1, atomically publishes the
staged storage/catalog, constructs the first resident snapshot, and returns
success only after the heap is ready. The request supplies no name,
descriptor, event, policy, certificate, or key bytes. Repeating the same
operation after publication returns its receipt; a different genesis for an
existing `HeapId` is `genesis-conflict`.

Calling this endpoint cannot create, sign, alter, or roll back authority. A
hard cycle, blacklist/grace mutation, recovery-policy change, or epoch change
is reported complete only after the running single-node server acknowledges
the committed head. HeapKey-authorized suspension, retirement, purge,
read-only, rename, and access-policy changes instead use the restricted
operational path in §31.5.1. When no server is running, a local
master-authority mutation may complete because startup must load that head
before readiness.

The local authority implementation MUST:

- be separated from the network request dispatcher;
- obtain protected local authority access;
- verify the current master against the pinned public key;
- require proof that a proposed holder controls its private key before issuing
  a certificate;
- never accept master material through command-line arguments;
- avoid shell history, process-list, log, crash-dump, and telemetry exposure;
- write authority transitions atomically and durably;
- use a protected key provider, HSM, agent, protected descriptor, or
  interactive secret input for master-key operations;
- zeroize transient private material where the platform permits;
- produce a durable non-secret receipt for every creation, issuance,
  blacklist, grace, cycle, and recovery operation.

The qualified build fails architectural inspection if a network-reachable
binary contains a concrete `MasterKeyProvider`, an authority signing command,
or a parser capable of accepting master-transition, master-mutation, or
authority-root event bytes from a request.

The holder generates its own keypair. The authority tool receives only the
holder public key and a fresh proof of possession, then displays or records its
fingerprint for operator confirmation. Residiuum does not generate or deliver
holder private keys.

Issuance receipts are append-only forensic evidence, not an authorization
database. A missing receipt does not make a correctly signed certificate valid
or invalid; the network decision remains self-contained. The first successful
use of each certificate is also audited.

### 8.9.1 Heap creation ceremony

The preferred ceremony has the owner or HSM generate the generation-1 master
key outside Residiuum. Only its public key and proof of possession enter the
local tool.

Creation is a recoverable state machine:

```text
absent
  -> creating(HeapId, DeploymentId, AuthorityEpoch=1)
  -> authority_prepared(master_public_key, recovery_profile)
  -> owner_possession_confirmed
  -> storage_genesis_staged(descriptor_hash)
  -> authority_genesis_committed(descriptor_hash)
  -> storage_genesis_published(descriptor_hash)
  -> active
```

No state before `active` is network-discoverable or accepts data. A crash
resumes or aborts the same immutable `HeapId`; it never creates a second
active identity.

Creation is local-only but does not require deployment-wide downtime.
`residiuum-authority` obtains the deployment `creation.lock`, then uses
`residiuum-store`'s authority-provisioning interface to write and sync one
canonical active `HeapDescriptor` plus its segment descriptor into a
non-discoverable, freshly created staging path. The running server never scans
that path. The tool computes the §34.7 descriptor hash and commits the
authority-root event whose label 18 and head label 25 bind that exact hash.
That rollback-resistant authority commit is the logical activation point.

If a server owns `serving.lock`, the tool calls
`publish_committed_genesis`; the server verifies and publishes the staged
bytes. If no server owns it, the tool acquires `serving.lock` non-blockingly,
publishes those same bytes through the provisioning interface, and retains
the lock through receipt sync. Contention plus an unavailable control endpoint
is ambiguous and causes retry, never direct publication. The heap is ready
only when the published descriptor hash, authority head, creation event ID,
`HeapId`, and origin `DeploymentId` all agree.

There is deliberately no claim of one filesystem transaction across
`authority_root` and `data_root`. Crash recovery follows the commit point:

- before authority genesis commits, staged storage is invisible and may be
  resumed or quarantined; abort permanently tombstones the allocated ID;
- after authority genesis commits, creation cannot abort or choose new bytes;
  it must publish the byte-identical staged genesis;
- if those bytes are missing or disagree, the heap remains unavailable and
  protected recovery must restore that exact genesis or master-authorize a
  permanent failed-creation tombstone; it never generates a replacement
  descriptor under the same root;
- a catalog entry is only a publication hint and cannot activate a heap whose
  authority/genesis binding is absent.

`creation.lock` is owned by the deployment, not one heap. Heap creation,
heap-name mutation, alias release, and recovery rebuild of the name index take
it exclusively; data operations never take it. Its acquisition precedes
every per-heap `mutation.lock`. While holding it, the authority tool only
probes `serving.lock` non-blockingly; it never waits for that lock. The
server's genesis-publish handler relies on the peer-credentialed tool retaining
`creation.lock` and does not reacquire it. Before publication it uses the
kernel lock-owner query to require that the exclusive owner PID/UID equals the
Unix-socket peer credentials; unsupported platforms need a separately frozen
coordination profile. A server-side name mutation acquires only
`creation.lock` in addition to its lifetime serving lock, so no blocking lock
cycle exists.

An aborted creation permanently tombstones its allocated `HeapId` and destroys
or abandons any prepared key according to provider policy; the identifier is
never recycled.

If a local provider generates a master private key, activation is prohibited
until protected persistence and proof of recoverability succeed. Writing
secret bytes to a terminal or ordinary file does not count as delivery.

### 8.9.2 Master-loss recovery profiles

Each heap chooses one immutable recovery profile at creation:

- `no_master_recovery`: loss of the current master permanently prevents
  issuance and ordinary cycling; this consequence is confirmed explicitly;
- `threshold_master_recovery`: a threshold set of offline recovery public keys
  is pinned in the rollback-resistant authority head.

Threshold recovery is local-only, requires the configured quorum, advances
`AuthorityEpoch`, installs a new master public key, hard-invalidates every
existing HeapKey, and emits durable evidence. Recovery keys have no data
rights and are never accepted by the network service.

An epoch advance starts generation and revision counters at one under the new
epoch. Every comparison includes the epoch, so an older counter value can
never create an ABA match.

Changing the recovery profile itself requires the existing recovery quorum and
current master, advances the epoch, and hard-invalidates issued HeapKeys.

### 8.10 Hard authority cycle

The default revocation mechanism is whole-generation cycling.

Given:

```text
Heap H:
    generation = g
    master_public_key = PK[g]
```

the owner/HSM or protected provider prepares a new keypair and the local tool
commits:

```text
AuthorityTransition {
    profile_version,
    deployment_id,
    heap_id: H,
    authority_epoch,
    expected_security_revision,
    previous_transition_head_hash,
    from_generation: g,
    from_master_key_hash: Hash(PK[g]),
    to_generation: g + 1,
    to_master_public_key: PK[g + 1],
    transition_nonce,
    effective_at,
    optional_grace_deadline,
    initial_blacklist_commitment,
    authorization_by_old_master,
    proof_of_possession_by_new_master,
}
```

The new keypair is generated by the owner/HSM or a protected provider and
proved recoverable before commit. The transition is authorized by protected
local operating-system access plus the old master signature; possessing old
master bytes without local authority cannot commit it.

The cycle state machine is:

```text
current
  -> new_root_prepared
  -> new_root_possession_and_recovery_confirmed
  -> transition_committed(effective_at)
  -> new_generation_active
```

Each transition compares the expected generation, security revision, epoch,
and previous head hash. Concurrent, replayed, forked, or out-of-order
transitions fail.

At `effective_at`, only generation `g + 1` is accepted. In a cluster, the
operation is not reported complete until every lease capable of accepting the
old generation has expired or been fenced.

Every generation-`g` HeapKey becomes cryptographically inert, including keys
created after the cycle by an attacker holding the old master.

The old master key is not destroyed by this operation, but it no longer
matches the heap's accepted authority and cannot issue valid HeapKeys or
perform another valid cycle.

All legitimate systems must receive newly issued generation-`g + 1` HeapKeys.

### 8.11 Optional graceful cycle

Grace is permitted only when the old master remains trusted and an issued
HeapKey or holder key is being replaced. Suspected or confirmed compromise of
the master requires a hard cycle with no grace: an attacker holding the old
master could otherwise mint unlimited new previous-generation certificates
that are absent from any blacklist.

The optional graceful profile permits:

```text
current generation: g + 1
previous generation: g
accept previous until: grace_deadline
resident blacklist: rejected generation-g certificate/holder fingerprints
```

The grace deadline and initial blacklist are part of the same atomic authority
transition that activates generation `g + 1`. A compromised holder included in
that transition has no acceptance window between cycle and blacklist
publication. Later blacklist additions are separate atomic authority mutations
with their own effective barriers.

During grace:

```text
generation g + 1:
    accept when otherwise valid

generation g:
    accept only when
        now < grace_deadline
        && certificate/holder fingerprint not blacklisted
        && otherwise valid

all older generations:
    reject
```

After the deadline, generation `g` is rejected and its temporary blacklist may
be discarded.

Blacklist entries are typed:

```text
CertificateFingerprint(generation, sha256_certificate)
HolderFingerprint(generation, sha256_holder_public_key)
```

A certificate entry rejects one exact certificate. A holder entry rejects
every previous-generation certificate for that holder. Human names,
unqualified IDs, and ambiguous byte strings are not revocation identity.

Blacklist and grace mutations occur only through the local authority plane.
Each is signed by the current master, requires operating-system authority, and
binds heap, epoch, expected revision, previous authority-chain head hash, operation
ID, mutation type, and effective time.

The v1 blacklist contains at most 100,000 entries or 16 MiB of canonical
fingerprints, whichever is reached first. Exceeding the bound requires hard
cycling without grace; entries are never silently evicted.

The graceful profile adds one bounded negative-authority set. It does not add
users, roles, groups, grants, or permission evaluation.

Individual current-generation certificates cannot be revoked in v1.
Compromise of one requires hard cycling, optionally followed by grace that
blacklists the compromised holder only when the current master remains
trusted. This whole-generation cost is an explicit consequence of the
zero-lookup design.

### 8.12 Always-resident heap security snapshot

For every ready heap, the complete current decision state is held in one
immutable resident snapshot:

```text
HeapSecuritySnapshot {
    deployment_id,
    heap_id,
    authority_epoch,
    security_revision,
    authority_revision,
    administrative_state_revision,
    access_policy_revision,
    administrative_state,
    decoded_access_policy,
    current_generation,
    current_master_public_key,
    optional_previous_generation,
    optional_previous_master_public_key,
    optional_grace_deadline,
    blacklist,
    security_time_floor,
    optional_authority_lease,
    authority_chain_head_hash,
}
```

The complete access-relevant policy and blacklist are always resident while
applicable. They are never fetched on a request or cache miss.

Updates build a complete replacement snapshot and atomically publish it.
Readers see the old snapshot or the new snapshot, never a partial mixture.

An authority change increments both `authority_revision` and
`security_revision`. Administrative state and access-policy changes increment
their own revisions and `security_revision`.

The exact revision deltas within one epoch are:

| Event | Authority | Security | State | Policy |
|---|---:|---:|---:|---:|
| master transition or master-mutation kinds 1–3 | +1 | +1 | 0 | 0 |
| failed-creation mutation kind 4 | +1 | +1 | +1 | 0 |
| administrative-state transition | +1 | +1 | +1 | 0 |
| access-policy change | +1 | +1 | 0 | +1 |
| heap rename or alias release | +1 | +1 | 0 | 0 |
| time checkpoint | 0 | 0 | 0 | 0 |
| new epoch root | reset all to 1 in the new epoch |

One event cannot combine rows. Unchanged counters retain their prior value;
overflow invokes the epoch-exhaustion rule below.

Revisions are unsigned 64-bit counters scoped by `AuthorityEpoch` and backed
by the authority-chain head hash. They never wrap or reset within an epoch.
Exhaustion locks authority mutation pending threshold epoch recovery; it does
not wrap to an earlier observable value.

Established HeapCaps perform one atomic security-revision comparison. In the
frozen v1 profile, a mismatch terminates that capability instance. It is never
mutated or revalidated in place. A remote holder establishes a new channel and
holder proof; trusted embedded code explicitly reopens the heap.

### 8.13 Restart and readiness

Security state is durable even though request-path state is resident.

Startup order is:

```text
load durable identity, authority, state, policy, time floor, and tombstone
verify epoch, revisions, roots, transition chain, grace, and blacklist
obtain a valid serving lease where the cluster profile requires one
construct immutable resident security snapshot
publish security revision
only then mark heap ready
```

Missing, corrupt, ambiguous, rolled-back, or unavailable authority state keeps
the heap unavailable.

Residiuum MUST NOT become ready with an empty default blacklist, permissive
policy, active administrative state, expired lease, earlier epoch, or earlier
root as a fallback.

### 8.14 Authority rollback resistance

Restoring an old data backup MUST NOT restore acceptance of old HeapKeys.

The accepted authority head therefore requires a rollback-resistant anchor
outside ordinary heap-data restoration. The qualified profile MUST define one
of:

- protected host authority head;
- hardware-backed monotonic state;
- cluster-consensus authority head;
- another independently protected monotonic mechanism.

The anchor contains at least:

```text
DeploymentId
HeapId
AuthorityEpoch
authority generation
security revision floor
authority-chain head hash
security time floor
retired/purged tombstone
recovery profile and public keys
```

If restored heap state is below or conflicts with any protected value, network
access remains locked pending an authorized local recovery ceremony. A purged
or retired tombstone cannot be cleared by restoring data.

Backups preserve authority evidence but do not gain authority to reduce the
current trusted generation.

### 8.15 Cluster freshness

Authority transition and blacklist updates are strongly ordered control-plane
events.

The cluster profile stores security transitions in a linearizable,
quorum-committed log. A node may serve a heap only while holding a
quorum-issued lease binding:

```text
DeploymentId
HeapId
AuthorityEpoch
security_revision
authority_chain_head_hash
node_id
control_term
minimum_control_log_index
not_after
maximum_clock_uncertainty
```

The v1 maximum authority lease duration is five seconds. Each request checks
that its resident snapshot matches the lease and that the lease is provably
unexpired.

```text
cannot prove matching revision and unexpired quorum lease
    => reject heap access
```

A partitioned node MUST NOT continue accepting a stale master generation or
stale blacklist merely to preserve availability.

A security mutation chooses an `effective_at` no earlier than the expiry of
all previously issued conflicting leases. Reachable nodes apply it sooner,
but the local tool reports the mutation effective only at that barrier.
Therefore the published cluster revocation bound is at most the maximum lease
duration plus bounded clock uncertainty.

Lease issuance and the maximum outstanding lease horizon are quorum-committed
control state, so the transition coordinator does not infer the barrier from a
best-effort node list.

Node bootstrap and replacement obtain the rollback-resistant head and a
verified snapshot through quorum state transfer before receiving a serving
lease. A data backup alone can never bootstrap authority.

### 8.16 Existing channels and long operations

Cycling, blacklist updates, and grace expiry apply to already established
HeapCaps.

After a security-revision change:

- new requests on the old capability fail;
- streams and watches detect termination within the v1 bound below;
- queued writes and retries fail and reacquire a fresh capability before a
  retry;
- long-running queries check before every output chunk and at least once per
  second;
- cached capabilities from the old revision are not silently retained.

Transactions compare capability revision at commit. Mutations compare it
immediately before their first durable effect. Streams, watches, backups,
exports, recovery jobs, and request-created background tasks compare it before
each externally visible output or durable effect and at least once per second.
On mismatch the capability terminates and buffered data is discarded.
Autonomous maintenance reacquires a newly minted internal capability from the
current resident slot before continuing.

The v1 single-node enforcement bound is one second for ongoing non-atomic
work and immediate on the next atomic request. The cluster bound is the larger
of one second and the authority-lease bound. Both are normative, tested, and
published.

### 8.17 Unauthorized versus absent

An invalid HeapKey, unauthorized operation, nonexistent heap, hidden heap, and
retired heap return the same ordinary network error: `HeapUnavailable`.

Protected local authority and recovery tools MAY report precise internal
reasons.

Public health endpoints MUST NOT disclose heap-specific authority state.

### 8.18 No ambient heap or authority

Server internals MUST NOT rely on a mutable process-global current heap,
current system, or ambient administrative authority.

Heap context travels explicitly as `HeapCap<H>`. Background work is created
with immutable heap identity, explicit internal rights, serving epoch,
authority generation, and security revision.

Request-created jobs retain the initiating capability and stop when it becomes
invalid. Autonomous compaction, scrub, repair, replication, retention, and
similar maintenance use a non-serializable `HeapMaintenanceCap<H>` minted only
by the isolation kernel from current resident policy. It is scoped to one heap,
cannot be presented by a network caller, cannot emit application data except
through a separately authorized output path, and rechecks state, policy,
epoch, and security revision before every durable effect.

## 9. Complete-path isolation

The following operations MUST be heap-scoped and authorized.

### 9.1 Ordinary data

- collection and stream discovery;
- get, put, append, delete;
- batch operations;
- transactions;
- scan, find, sort, and pagination;
- large-payload and chunk retrieval.

### 9.2 Query and interpretation

- RQL;
- raw SDA;
- ENR;
- dialect compilation and execution;
- query plans;
- temporary materialization;
- result caches.

Host-supplied collection bindings MUST originate from the same heap. No
cross-heap query capability exists.

### 9.3 History and events

- item history;
- tombstones;
- retained prior values;
- change streams;
- resumable stream positions;
- transaction and commitment evidence.

### 9.4 Derived structures

- primary and secondary indexes;
- collection catalogs;
- statistics;
- projections;
- caches;
- checkpoints;
- search structures.

Every cache and index key that can collide across heaps MUST include `HeapId`
or be physically scoped equivalently.

### 9.5 Operational surfaces

- metrics;
- health detail;
- logs;
- traces;
- audit;
- support bundles;
- configuration inspection;
- capacity and quota reports.

Ordinary metrics SHOULD avoid free-form heap labels when doing so creates
unbounded cardinality or cross-heap disclosure. Authorized administrative
detail MAY provide heap-scoped views.

Deployment-wide operational fields are visible only when present in the
closed declassification registry for the named isolation profile. New fields
default to heap-local and confidential. Logs, errors, metrics, traces, audit,
support bundles, and query explanations are part of `Obs` and cannot escape
non-interference by being called diagnostics.

### 9.6 Data movement

- replication;
- anti-entropy;
- repair;
- tier movement;
- migration;
- import;
- export;
- backup;
- restore.

### 9.7 Destructive and recovery surfaces

- compaction;
- expiration;
- retention;
- holds;
- retirement;
- purge;
- key destruction;
- scrub;
- salvage;
- examination.

Administrative recovery that intentionally crosses heaps requires explicit
protected local recovery-plane authority and audit as defined in §18.3. It is
outside the ordinary heap data plane and cannot emit a mixed data-bearing
handle, result, or package.

## 10. Cross-heap behavior

### 10.1 Default rule

Cross-heap reads, writes, joins, transactions, indexes, streams, caches,
capabilities, backups, and recovery result sets are prohibited in every
qualified profile.

This is a permanent isolation invariant, not a v1 feature omission.

### 10.2 References

Applications MAY store opaque identifiers referring to another heap, but
Residiuum does not resolve or enforce those references in v1.

### 10.3 Data movement

Moving data between heaps requires two separate operations:

1. export from the source using a source-bound capability;
2. import into the destination using a destination-bound capability.

No live query, cursor, iterator, transaction, or capability spans both heaps.
The import creates destination-owned objects and records source provenance as
inert metadata.

There is no “same deployment means joinable” rule and no future compatibility
mode may introduce one.

### 10.4 Administrative aggregation

Deployment administrators MAY aggregate non-data-bearing operational metadata
that has been explicitly declassified for that purpose, such as:

- heap count;
- capacity totals;
- health state;
- backup job status;
- policy compliance status.

Administrative aggregation MUST NOT include application keys, values,
collection contents, query results, history, indexes, recovered payloads, or
other data-bearing material from more than one heap.

## 11. Heap administration

### 11.1 Create

Heap creation follows §8.9.1 and becomes ready only after the recoverable
genesis protocol has established:

- new `HeapId`;
- serving `DeploymentId` and `AuthorityEpoch`;
- unique initial name;
- initial state `active`;
- policy version;
- generation-1 master public key pinned to the heap;
- rollback-resistant authority head;
- permanent identity tombstone;
- creation audit record.

The preferred ceremony never delivers a master private key through Residiuum.
Failure MUST NOT leave a discoverable heap without authoritative policy or
leave an active heap whose master private key has not been proved controlled
and recoverable under the selected profile.

### 11.2 List

There is no ordinary network operation that enumerates heaps available to a
human identity, because Residiuum stores no human-to-heap grants.

A system already holding a HeapKey may describe only that key's heap.

The local deployment tool MAY list non-data-bearing heap names, IDs, states,
and authority status under operating-system authority.

Ordinary network selection sends `HeapId`, not a guessed name, after local SDK
configuration resolves the name. If a deployment supports remote name
resolution, it performs bounded work and returns the same response shape and
externally qualified timing class for absent, hidden, retired, and
unauthorized names. Authentication-failure and parsing admission occurs before
expensive heap-specific work.

### 11.3 Describe

Heap description is rights-filtered.

A valid HeapKey MAY expose:

- name;
- `HeapId`;
- state;
- certificate rights and constraints;
- accepted authority generation and certificate's generation;
- public capability profile.

Sensitive placement, data-encryption, recovery, and policy detail requires the
corresponding encoded HeapKey right.

### 11.4 Rename

Rename follows §5.4 and does not affect existing bound handles.

### 11.5 Suspend and resume

Suspension/resumption requires `heap_admin` and audit.

Emergency suspension SHOULD be possible without data-key destruction or
physical mutation.

### 11.6 Retire

Retirement requires:

- `retire`;
- high-friction confirmation naming immutable `HeapId`;
- policy and hold checks;
- durable audit;
- explicit statement that retirement is not purge.

### 11.7 Purge

Purge follows [DATABASE_DOCTRINE.md](../../reference/product/DATABASE_DOCTRINE.md).

The purge plan MUST be scoped by immutable `HeapId`, enumerate managed copies
and key dependencies, and report unavailable domains.

Heap name alone is insufficient purge authority.

Purge removes application payload, indexes, streams, history, data keys, and
managed copies according to policy. It MUST retain the non-payload authority
chain, permanent `HeapId` tombstone, creation/current `HeapDescriptor` chain,
operation and destruction receipts, and name-quarantine evidence. Those
records contain no application values and are required to prove that old
media, names, keys, or identifiers cannot revive or alias the purged heap.
They are stored in the protected administrative retention class and are not
returned by data operations.

## 12. Policy hierarchy

Policy resolution follows:

```text
deployment defaults
        ↓
heap policy
        ↓
collection/stream policy
        ↓
item policy or hold, where supported
```

More specific policy MAY strengthen protection.

It MUST NOT weaken:

- mandatory deployment safety controls;
- active legal holds;
- minimum retention;
- required encryption;
- required replication;
- prohibited placement;
- audit obligations.

Effective policy is evaluated against `HeapId`, never name prefix.

Policy is divided into:

- access-relevant policy, fully decoded into `HeapSecuritySnapshot`;
- operation-local governance policy, such as retention and placement, read
  through a heap-bound policy handle by the authorized operation.

Changing access-relevant policy atomically increments
`access_policy_revision` and `security_revision`. No request-time policy
provider, cache miss, or external identity lookup is permitted. Item policy
and holds govern lifecycle and destructive behavior; they are not a hidden
record-level human authorization system.

Remote access-policy mutation requires `policy_admin`; local recovery mutation
requires protected local authority. Policy cannot expand the rights in any
HeapKey or weaken mandatory deployment safety controls.

In v1, the only access-relevant resident policy is the heap policy encoded in
§35.1. Collection/stream narrowing is expressed by immutable-ID constraints
inside that policy or the certificate. Collection, stream, and item policy
layers are governance-only; they cannot grant or deny human/system data access
through a second authorization mechanism. Adding access-relevant subordinate
policy requires a new frozen policy profile and security-snapshot format.

## 13. Resource isolation

Logical data isolation does not automatically provide resource isolation.

A conforming multi-system server profile MUST support heap-attributed
budgets for:

- storage;
- retained history;
- request rate;
- concurrent operations;
- query work;
- result bytes;
- background maintenance;
- replication bandwidth;
- backup bandwidth;
- memory/cache use where practical.

Exhaustion in Heap A MUST NOT authorize deletion in Heap B.

Shared-resource interference and side channels MUST be documented. A profile
claiming hostile-tenant isolation requires stronger controls than basic heap
authorization.

### 13.1 Named isolation profiles

Claims always name one of these profiles:

| Profile | Guarantee | Permitted leakage |
|---|---|---|
| `heap-data-isolated` | No application data or functional heap metadata crosses heaps | Timing, aggregate load, and availability from shared resources |
| `heap-metadata-hardened` | Data isolation plus closed, tested metadata declassification | Coarsened timing and declared aggregate health only |
| `heap-resource-isolated` | Metadata-hardened plus enforced CPU, memory, I/O, cache, and bandwidth budgets | Residual hardware and network side channels documented by deployment |
| `heap-physical-isolated` | Separate process/store/key/host boundaries as declared | Only explicitly documented infrastructure channels |

The H6 logical claim requires at least `heap-data-isolated`. It never implies a
stronger profile. Disk-full, throttling, cache, quota, health, and admission
behavior are included in profile testing because they can disclose activity
in other heaps.

### 13.2 Base metadata declassification registry

The registry is closed. Under `heap-data-isolated`, an unauthenticated network
caller may observe only:

| Field | Rule |
|---|---|
| `protocol_versions` | fixed server capability, independent of stored heaps |
| `live` | process event loop and listener are alive |
| `ready` | protocol and deployment control catalog loaded; never depends on one named heap's existence, state, size, or authority health |
| `build_id` | fixed binary build identifier |

An authenticated Heap A capability may additionally observe Heap A's own
immutable ID, display metadata, state permitted by its operation, budgets,
usage, receipts, and operation results. It may not observe heap count, another
heap's names/IDs, global object counts, per-heap readiness, authority-provider
inventory, physical paths, placement topology, or whether a rejected heap
exists, except for the opaque heap-local routing material below.

`heap_directory` may return only Heap A's opaque routing endpoints, partition
tokens, placement epoch, and expiry needed for direct client routing. It does
not return node inventory, physical paths, replica membership, another heap's
assignments, or whether an endpoint is shared. Routing tokens are signed and
bound to deployment, heap, capability, placement epoch, and expiry.

The `heap-metadata-hardened` base profile additionally coarsens public timing
and exposes no aggregate load field. Availability of the whole endpoint
remains observable by definition. A deployment extension is a versioned,
reviewed JSON registry shipped with the server configuration; its SHA-256 hash
is reported in local qualification evidence. A field not present in the base
table or that exact extension is confidential. Runtime configuration cannot
add a field without restarting and loading a newly qualified registry.

## 14. Atomics, transaction compatibility, and acknowledgement

### 14.1 Atomic scope

Residiuum Atomics are heap-local in v1. A transaction-shaped compatibility API
is one possible client surface over Atomics; it does not weaken or replace the
`ATOMICS_SPEC.md` execution and recovery model.

Every atomic unit binds one `HeapId` at creation. Attempts to use collections
from another heap fail before mutation.

### 14.2 Batch identity

Batch, atomic-unit, transaction-compatibility, idempotency, and write-dedup identities MUST include
`HeapId` or be scoped by an equivalent heap-bound context.

The same operation ID used in two heaps MUST NOT collide.

The v1 deduplication key is:

```text
(HeapId, operation_id)
```

Its durable value binds operation ID to operation code, immutable subordinate
ID or null, request-binding hash, terminal status, and receipt/result hash.
Reuse with an identical binding returns the original outcome without repeating
effects. Reuse with any different binding is `OperationIdConflict`.
Collection, stream, and operation code are values, not extra key dimensions,
so changing one cannot evade conflict detection.

The request-binding hash is SHA-256 over:

```text
ASCII("RESIDIUUM-HEAP-RPC-REQUEST-V1") || 0x00
|| op_id_u16_be
|| operation_id_16_bytes
|| collection_present_u8 || collection_id_if_present
|| stream_present_u8 || stream_id_if_present
|| args_length_u32_be
|| exact_args_json_bytes
```

Presence is 0 or 1. `exact_args_json_bytes` is the exact UTF-8 byte range of
the validated `args` value in the received frame, including its internal
whitespace and key order. The bounded parser preserves that range before
deserialization. Correlation field `id` and whitespace or key order outside
`args` are excluded. A retry may change its correlation ID; changing the
encoded arguments, even to a semantically equivalent JSON value, is a safe
conflict rather than a risk of repeating different effects. The reference SDK
serializes `args` deterministically so ordinary retries remain identical.

For a mutation, the dedup record and durable effect commit in one storage
transaction. Where an operation spans several physical units, a durable
`Pending` record first fixes all event IDs and intended effects; replay applies
those IDs idempotently and then writes `Complete`. `Complete` stores the exact
bounded result/receipt bytes, not only a hash, so a retry can return the
original outcome. A crash at every boundary must converge to either no effect
with a retryable pending operation or one effect with one stable receipt—never
two effects.

### 14.3 Acknowledgement

Acknowledgement semantics apply within the selected heap's deployment,
durability, replication, and policy profile.

An acknowledgement for Heap A says nothing about Heap B.

## 15. Query semantics

### 15.1 Heap-local bindings

RQL, SDA, ENR, and fluent query bindings opened through one heap handle may
bind only collections from that heap.

Collection names in query text never select a heap.

### 15.2 Query compilation

Compiled plans and prepared queries MUST retain the bound `HeapId` or require
rebinding through an authorized heap handle before execution.

A plan compiled for Heap A MUST NOT execute against Heap B merely because the
same collection names exist.

Prepared plans contain immutable collection/stream identities and are rebound
through the same capability instance before execution. Serialized plans carry
no authority and cannot embed a raw physical locator.

### 15.3 Result provenance

Administrative and examination profiles SHOULD be able to attach `HeapId` to
result provenance without requiring it in ordinary application documents.

## 16. Indexes and collection identity

Index names are unique within:

```text
(HeapId, Collection, IndexName)
```

Index creation, lookup, rebuild, deletion, cache identity, and persisted
metadata MUST include heap context.

Rebuilding derived catalogs MUST preserve separate collection namespaces for
each recovered heap.

A collection catalog for Heap A MUST NOT reveal names from Heap B to a Heap A
holder.

## 17. Backup, export, and restore

### 17.1 Heap-scoped backup

A heap backup includes:

- immutable `HeapId`;
- heap name at backup time;
- collection/stream scope;
- policy and format versions required for interpretation;
- recovery frontier;
- encryption/key dependencies;
- integrity manifest;
- declared exclusions and holes.

Heap-scoped backup MUST NOT silently include data from another heap.

### 17.2 Deployment backup

A deployment backup is an explicit administrative operation containing a
manifest of included heap IDs plus separately sealed heap backup packages.

No backup data stream, encryption domain, index, or restore iterator combines
the contents of two heaps. The deployment manifest is non-data-bearing
inventory and is not implemented by pretending the deployment is one heap.

A deployment backup container MAY physically contain several independently
sealed heap packages. No plaintext decoder, iterator, compression stream,
encryption context, or package signature spans package boundaries.

### 17.3 Authorization filters do not define physical backup

Physical backup tooling operates on its declared heap or deployment scope.
Ordinary query visibility MUST NOT silently filter backup content and produce
an apparently complete package.

### 17.4 Restore

Restore MUST declare one of:

- restore the same heap identity into a replacement deployment;
- clone into a new `HeapId`;
- restore selected content into an existing destination heap.

The default safe mode is restore into a new destination with no name collision.

If restoring the same `HeapId` would create two concurrently writable
authorities, the operation MUST stop unless an explicit disaster-recovery
takeover protocol prevents split authority.

#### Same-identity takeover

Restoring the same identity requires the protected local recovery plane. It:

1. verifies the backup identity and current rollback-resistant authority head;
2. proves recovery authority under §8.9.2;
3. fences the old `DeploymentId` and advances `AuthorityEpoch`;
4. installs a fresh master public key and security snapshot;
5. waits for all prior serving leases to expire or be fenced;
6. reissues HeapKeys for the new deployment and epoch;
7. activates the restored heap only after durable takeover evidence commits.

The old deployment and all old HeapKeys remain invalid even if the old
machines later return.

#### Clone to a new identity

A clone creates a new heap through §8.9.1, with new `HeapId`, subordinate
collection/stream identities, master authority, data-key context, and
rollback-resistant head. Import then rewrites or regenerates ownership
evidence, encryption associated data, indexes, manifests, cursors, resume
tokens, and internal references. Source HeapKeys confer no destination
authority.

Reading the source requires its `backup` or `export` authority; creating and
mutating the destination requires separate local creation authority and a
destination `restore` HeapKey. No single credential spans both sides.

#### Restore into an existing heap

Selected-content restore is a destination-heap import requiring `restore`. It
uses an explicit collision policy, stages all rewritten objects under the
destination identity, validates provenance and integrity, and commits
atomically or reports a resumable partial import with no ambiguous visibility.
It never preserves source authority, live locators, capabilities, cursors, or
encryption ownership.

### 17.5 Export

Logical export includes heap provenance and is authorized by `export`.

An export format MAY omit `HeapId` from each item when one signed/verified
package manifest unambiguously binds every contained item to the heap.

## 18. Recovery and examination

### 18.1 Recovery identity

Recovered authoritative material MUST result in one of:

- known `HeapId`;
- cryptographically/integrity-bound heap identity reference;
- `heap_identity_unavailable`.

It MUST NOT be guessed from a directory name or collection name.

### 18.2 Mixed recovery input

Recovery tools MAY scan media containing multiple heaps.

They MUST:

- group material by proven `HeapId`;
- keep unknown-identity material separate;
- report conflicts where material claims incompatible identity;
- require authorization for each heap exposed;
- emit separate heap-bound result handles or packages;
- never merge equal collection names across heaps.

The scanner MAY classify several heaps internally inside the protected
recovery TCB. No ordinary or administrative query result may contain
data-bearing material from more than one heap.

The mixed-media classifier is not part of the ordinary heap isolation kernel.
It belongs to the separately qualified recovery TCB described below. This
distinction keeps the ordinary kernel small and makes the exception to
single-heap internal processing explicit.

### 18.3 Salvage authorization

Ordinary heap recovery exposes only the authorized heap.

Raw physical salvage capable of discovering multiple heaps is available only
through the protected local recovery plane. Output is separated only after
evidence is classified; it is not declared complete for an individual heap
unless scope coverage can be proven.

The protected recovery plane:

- is local/offline and has no ordinary network query endpoint;
- authenticates operating-system authority and, where a known heap is exposed,
  requires that heap's `recover` HeapKey or threshold recovery authority;
- uses a separate recovery capability that cannot enter the data-plane API;
- treats the mixed-media scanner and ownership verifier as a distinct,
  published recovery TCB;
- may classify mixed physical input internally;
- emits only independently sealed `Known(HeapId)`, `Unknown`, or `Conflict`
  packages and never a mixed data-bearing handle;
- records input media identity, ownership evidence, holes, conflicts,
  operator authority, and every emitted package in durable audit.

There is no remotely usable “deployment-wide recovery key.” Operating-system
access alone may inventory opaque units but cannot decrypt or expose a known
heap without the corresponding recovery authority.

### 18.4 SDA examination

SDA examination receives heap identity as recovery evidence or host context.
SDA programs do not gain cross-heap authority by referring to another name.

### 18.5 Missing identity

Material with unavailable heap identity may be preserved and examined as
unassigned recovery evidence by an authorized recovery operator.

It MUST NOT appear in an ordinary heap collection until an explicit,
evidence-recorded attribution/import operation occurs.

## 19. Replication and clustering

### 19.1 Routing identity

Heap identity participates in partition and routing identity:

```text
route(HeapId, CollectionOrStream, KeyOrEvent)
```

Equivalent implementation scoping is permitted, but routing or dedup state
from one heap MUST NOT affect another.

### 19.2 Replica authorization

Node-to-node replication uses cluster authority and declared placement policy.
Client HeapKeys do not authorize cluster peers.

Replication metadata and repair evidence retain `HeapId`.

Cluster credentials identify one `DeploymentId`, node identity, permitted
protocol role, and validity interval. Possessing a cluster credential does not
grant application read authority. A node receives heap material only when a
quorum-committed placement assignment names that node and heap; peers verify
that assignment before transfer.

Node removal, credential rotation, placement revocation, and serving-lease
expiry are fenced through the cluster control log. A node cannot request an
arbitrary `HeapId` and infer replication authority from knowing its name.

### 19.3 Placement

A heap MAY have independent:

- replication factor;
- write mode;
- region/zone constraints;
- tier policy;
- encryption profile;
- capacity budget.

Changing placement does not change `HeapId`.

### 19.4 Control-plane loss

Rebuilding placement or catalog state from node inventories MUST preserve heap
identity. Equal collection names are never sufficient evidence that data
belongs to the same heap.

## 20. Encryption and keys

Heap authorization and encryption are independent controls.

The heap master key and issued HeapKeys authorize access. They MUST NOT be used
as data-encryption keys, key-encryption keys, backup-encryption keys, or
crypto-erasure keys.

Compromise of access authority must not automatically disclose encrypted data,
and destruction of a data-encryption key must not rotate access authority.

A heap MAY have:

- a dedicated data-encryption key domain;
- a shared deployment key domain;
- client-confidential payloads;
- external volume encryption.

Profiles MUST state which applies.

If a heap-specific key domain is configured:

- wrapped data-key metadata identifies `HeapId`;
- key administration requires `data_key_admin`;
- key rotation reports heap coverage;
- backup records key dependencies;
- loss or destruction of keys reports encrypted-unavailable data;
- a key for Heap A MUST NOT decrypt Heap B unless the declared shared-key
  profile explicitly permits it.

Shared keys weaken blast-radius isolation and MUST be visible in policy.

## 21. Audit

The durable audit subsystem is named the **Residiuum Evidence Ledger** and is
specified normatively by
[EVIDENCE_LEDGER_SPEC.md](../../todo/evidence/EVIDENCE_LEDGER_SPEC.md). This section defines the
Heap-facing obligations; where it is less specific, the Evidence Ledger
specification controls.

Security-sensitive audit records include:

- HeapKey certificate ID and fingerprint;
- holder-public-key fingerprint;
- immutable `HeapId`;
- heap name observed at operation time when known;
- operation;
- result;
- authority generation, authority revision, security revision, and serving
  epoch;
- encoded right used for the operation;
- request/operation identity;
- timestamp and ordering evidence;
- confirmation evidence for high-impact actions without recording secrets.

Audit also records certificate issuance receipts, first successful certificate
use, grace and blacklist mutations, security-snapshot revisions, serving
leases, takeovers, recovery ceremonies, and rejected rollback attempts.

Rename history allows operators to understand old audit events after names
change.

Denials SHOULD be auditable without creating an attacker-controlled
denial-of-service path.

A HeapKey with `audit_read` sees only its own heap's audit projection.
Deployment-wide audit is available only through the protected local
administrative plane and keeps heap records logically separable. Audit output
is data-bearing for isolation purposes even when it contains only identifiers.

Certificate, proof, error, and audit inputs have fixed size limits. Signature
verification, nonce tracking, and denial logging are protected by pre-
authentication admission budgets so malformed certificates cannot create an
unbounded CPU, memory, log, or cardinality path.

## 22. Errors

The public error vocabulary includes:

| Error | Meaning |
|---|---|
| `HeapUnavailable` | Heap absent, undiscoverable, unauthorized, or retired for this caller |
| `HeapSuspended` | Authorized caller cannot operate because heap is suspended |
| `HeapReadOnly` | Mutation rejected by heap state |
| `HeapNameInvalid` | Proposed name violates portable profile |
| `HeapNameConflict` | Authorized administration encountered a live name collision |
| `HeapPolicyDenied` | Caller is authorized generally but effective heap policy denies this operation |
| `HeapKeyInvalid` | Protected diagnostic: certificate, holder proof, generation, signature, audience, or validity failed |
| `HeapKeyBlacklisted` | Protected diagnostic: certificate or holder fingerprint is in the active resident blacklist |
| `HeapAuthorityStale` | Node lacks a matching unexpired quorum serving lease for its resident security snapshot |
| `HeapAuthorityUnavailable` | Durable authority state cannot be loaded or verified |
| `CrossHeapOperation` | Operation attempted to mix heap identities |
| `HeapIdentityUnavailable` | Recovery material lacks provable heap identity |
| `HeapPurgeIncomplete` | Managed purge could not cover every required domain |
| `OperationIdConflict` | One heap-local operation ID was reused with a different binding |
| `InvalidStateTransition` | Authorized request named an edge absent from §6.6 |
| `OwnershipConflict` | Integrity-valid ownership evidence disagrees |
| `AuthorityCorrupt` | Protected diagnostic: authority head, policy, time, or event chain is invalid |
| `AuthorityFork` | Protected diagnostic: equal authority sequence has conflicting valid candidates |
| `IssuanceRequestConsumed` | Protected local diagnostic: issuance request was already committed |

Error text MUST NOT reveal hidden heap names, IDs, collection names, sizes, or
policy.

### 22.1 Qualified wire projection

The qualified listener emits only the following codes. The `retryable` value
is fixed; handlers cannot choose it dynamically.

| Wire code | Retryable | Eligible internal causes |
|---|---:|---|
| `heap_unavailable` | false | absent/hidden/unauthorized heap, invalid key/proof, blacklist, retired/purged state, cross-heap attempt |
| `heap_suspended` | true | `HeapSuspended`, after right validation |
| `heap_read_only` | false | `HeapReadOnly`, after right validation |
| `heap_name_invalid` | false | `HeapNameInvalid` |
| `heap_name_conflict` | false | `HeapNameConflict` |
| `heap_policy_denied` | false | `HeapPolicyDenied`, after right validation |
| `heap_authority_stale` | true | `HeapAuthorityStale` |
| `heap_authority_unavailable` | true | verified capability but serving authority became unavailable |
| `heap_purge_incomplete` | true | `HeapPurgeIncomplete` |
| `heap_identity_unavailable` | false | authorized recovery input lacks identity |
| `operation_id_conflict` | false | `OperationIdConflict` |
| `invalid_state_transition` | false | `InvalidStateTransition` |
| `ownership_conflict` | false | authorized recovery found conflicting valid evidence |
| `invalid_request` | false | bounded syntax/schema/type/range failure after framing |
| `unknown_operation` | false | unallocated, reserved, or unknown operation ID |
| `resource_limit` | true | admitted request exceeded a declared transient resource budget |
| `timeout` | true | declared operation deadline elapsed before an effect |
| `internal` | true | no safe narrower public code; no diagnostic text |

`HeapKeyInvalid`, `HeapKeyBlacklisted`, `AuthorityCorrupt`, and
`AuthorityFork` are protected diagnostics and never appear as qualified
network codes. Before a capability is established, every framed rejection is
exactly `heap_unavailable`. After establishment, an error other than
`heap_unavailable`, `heap_authority_stale`, or
`heap_authority_unavailable` may be returned only after the operation's right
and target scope validate. A security-revision mismatch closes the connection
without an application error because the capability instance has terminated.
The local authority CLI may report `issuance_request_consumed`; it is not a
network code.

## 23. Compatibility with current Residiuum

### 23.1 Current state

Before this specification, Residiuum has:

- one physical/logical store per embedded path or server process;
- a flat collection namespace within that store;
- subjects encoded as `(collection, key)`;
- a remote URL path label that is informational;
- deployment-wide read/write/admin privileges.

This is not multi-heap support.

### 23.2 Legacy embedded compatibility

`Residiuum::open(path)` MAY continue to return a handle whose collection methods
operate within one implicit compatibility heap.

The compatibility heap:

- has a durable generated `HeapId`;
- receives a generation-1 master public key through a one-time local migration
  ceremony before HeapKey network access is enabled;
- uses reserved display/lookup behavior rather than an ordinary user-created
  `default` name;
- does not expose deployment-level multi-heap administration through the
  legacy handle;
- can later be assigned a user heap name without changing data identity.

### 23.3 Legacy remote compatibility

Before the heap protocol feature is negotiated, the server MAY retain the
single-heap protocol.

After heap support is enabled:

- the URL path selects a heap rather than carrying an informational label;
- old clients without HeapKey support may connect only when the server
  explicitly exposes one unqualified compatibility heap;
- a compatibility endpoint cannot make the strong HeapKey isolation or
  zero-lookup claim;
- the server MUST NOT map arbitrary old URL labels to the same heap while
  presenting them as separate heaps;
- capability negotiation distinguishes legacy single-heap and HeapKey
  multi-heap semantics;
- the network protocol still exposes no master-key operation.

A qualified multi-heap server cannot expose a legacy remote endpoint against
the same process, raw storage handle, or physical store. Legacy remote mode is
disabled or isolated in a separate process and store containing exactly one
compatibility heap. Code retaining a deployment-global iterator is never
linked into the qualified multi-heap data plane.

### 23.4 Durable identity migration

Migration assigns the legacy store one `HeapId` and records it in authoritative
heap/store identity metadata.

The frozen `residiuum-heap-v1` profile rewrites every admitted legacy frame as
specified in §36. A future named profile may avoid rewriting only if it proves
all of the following with equally strong surviving ownership evidence:

- recovered legacy frames are unambiguously attributable through
  integrity-protected store/segment context;
- moved or detached recovery units do not become silently attributable to the
  wrong heap;
- mixed legacy and heap-aware material is reported honestly;
- future new material carries sufficient identity for the declared recovery
  profile.

Such a future profile is not wire- or recovery-compatible with
`residiuum-heap-v1` unless this specification explicitly says so.

### 23.5 SDK transition

Preferred new API:

```rust
deployment.heap("accounts")?.collection("users")?
```

Compatibility API:

```rust
db.collection("users")?
```

The compatibility API is valid only for a handle already bound to exactly one
heap. It MUST NOT select a process-global default heap.

## 24. Threat model

### 24.1 Protected adversary

For the HeapKey server profile, heap isolation protects against:

- a system holding a valid HeapKey for Heap A attempting to access Heap B;
- guessed heap names and IDs;
- request tampering that substitutes heap context;
- query text referencing collections outside the bound heap;
- stolen certificates without the bound holder private key;
- old HeapKeys after hard authority cycling;
- blacklisted HeapKeys during an active grace period;
- stale handles after security revision;
- stale administrative state or access policy;
- wall-clock rollback and excessive time uncertainty;
- stale cluster leases and returned fenced deployments;
- network attempts to present or exercise the master key;
- accidental administrative targeting by mutable name;
- derived-path and recovery-path authorization bypass.

### 24.2 Out of scope for logical heap isolation

Heap authorization alone does not protect against:

- server process compromise;
- Residiuum binary compromise;
- kernel or hypervisor compromise;
- storage administrator access;
- memory inspection;
- traffic analysis beyond declared transport protections;
- denial of service through shared resources;
- compromise of the local master private key;
- compromise of the local authority tool or rollback-resistant authority head;
- an application explicitly holding HeapKeys for two heaps combining separately
  obtained results outside Residiuum.

Those require other doctrine controls.

In particular, compromise of a current master permits the attacker to mint
valid HeapKeys until a hard cycle becomes effective. HeapKey holder proof does
not reduce that signing-authority consequence.

### 24.3 Confused-deputy prevention

Internal services, background jobs, query engines, and repair workers MUST
receive immutable heap context and the authority under which they act.

They MUST NOT accept a caller-provided collection path or HeapId and infer
authority from it. Authority exists only through a validated `HeapCap<H>`.

## 25. Normative invariants

A conforming implementation maintains all of the following:

1. Every ordinary data operation resolves exactly one `HeapId`.
2. No collection identity is deployment-global.
3. Every HeapKey certificate binds exactly one immutable `HeapId`, never a
   heap name or set of heaps.
4. A bound heap handle never changes identity after rename.
5. Missing, invalid, stale, or ambiguous authority state denies access.
6. Invalid HeapKeys and absent heaps are indistinguishable to ordinary network
   callers.
7. Cross-heap capabilities, queries, and transactions are unrepresentable and
   prohibited in every qualified profile.
8. Every derived data path preserves heap scope.
9. Cache, dedup, cursor, resume, index, and operation identities cannot collide
   across heaps.
10. Backup and export do not silently cross heap boundaries.
11. Recovery never merges equal collection names from different heaps.
12. Retirement is not purge.
13. Logical heap isolation is never advertised as physical isolation.
14. Administrative bypass is explicit and audited.
15. Physical placement may change without changing `HeapId`.
16. Every admitted authoritative or derived data-bearing object has exactly one
    `Known(HeapId)` owner; `Unknown` and `Conflict` remain quarantined recovery
    evidence.
17. No query, iterator, transaction, index, cache entry, result, backup
    payload, or recovery view contains objects from different heaps.
18. A write authorized for one heap cannot change another heap's state.
19. Varying all other heaps cannot vary the data observation of a heap-bound
    operation.
20. Query and interpretation engines cannot access a deployment-global data
    iterator.
21. Residiuum stores no human user, group, role, membership, grant, or
    permission database for heap access.
22. The network protocol cannot accept a master/recovery secret, issue
    HeapKeys, mutate grace/blacklist, recover a master, or cycle authority.
23. Issued HeapKeys carry all rights and constraints cryptographically and
    cannot issue other HeapKeys.
24. Network validation performs no authorization-policy I/O.
25. The ready heap's complete security snapshot, access policy, lease, and
    applicable blacklist are resident in memory.
26. An unchanged security revision reduces established-channel authorization
    to resident revision, epoch, rights, constraint, time, and lease checks.
27. Hard cycling invalidates every prior-generation HeapKey and prevents the
    prior master from authorizing future access or transitions; historical
    signatures remain verifiable evidence only.
28. Graceful cycling accepts at most the current and immediately previous
    generations, and the previous generation only before its deadline and
    outside its resident blacklist.
29. Restoring data cannot roll back the trusted authority head.
30. A node unable to prove sufficient authority freshness rejects heap access.
31. Administrative state and access-policy changes increment the security
    revision and invalidate established capabilities.
32. Grace is prohibited when the previous master may be compromised.
33. Individual current-generation revocation requires a generation cycle.
34. Security time never moves backward; untrusted time denies time-dependent
    access.
35. Every independently recoverable data unit carries integrity-protected
    immutable ownership evidence.
36. `CollectionId` and `StreamId` are immutable and never reused.
37. Same-identity restore advances `AuthorityEpoch` and fences the prior
    serving incarnation.
38. Purged heap identity and authority tombstones are permanent.
39. The mixed-media recovery TCB emits no mixed data-bearing output.
40. Qualified multi-heap service cannot share a raw store or process with a
    legacy remote endpoint.
41. Unknown certificate fields, algorithms, rights, and constraints fail
    closed under the frozen profile.
42. Master, recovery, and holder secret keys are never accepted by the network
    service as authority material.
43. A compound operation obtains every required right before its first read
    or effect.
44. Heap-visible operational output follows the closed declassification
    registry for the named isolation profile.
45. Data backups cannot bootstrap or lower cluster authority.
46. Every accepted authority head is connected through one contiguous,
    anchored event chain to its creation root and current epoch root.
47. Administrative-state admission is generated from the same closed
    operation registry as rights and dispatch.
48. Reusing one operation ID with different operation, target, or arguments
    cannot repeat, redirect, or hide an effect.
49. Access policy is present in and atomically authenticated with the authority
    head; a missing policy never means permissive default.
50. A cluster serving lease binds one node, one heap, one epoch, one security
    revision, and one authority-chain head.

## 26. Conformance test plan

### 26.1 Basic namespace tests

- create Heap A and Heap B with a collection named `users`;
- write the same key with different values;
- prove reads, scans, histories, and indexes return only the selected heap;
- rename Heap A and prove existing handles retain identity;
- reuse an eligible old name and prove stale handles do not switch identity.

### 26.2 HeapKey rights matrix

For no key, malformed key, wrong-heap key, wrong-holder proof, expired key,
future key, read-only, CRUD, index, backup, recovery, heap-admin, retire, and
purge HeapKeys:

- exercise every RPC and SDK operation;
- verify signed rights and constraints exactly;
- prove no right implies another right unless frozen by the profile;
- verify every compound operation checks the complete rights matrix before
  reading or mutating;
- fuzz unknown, duplicate, oversized, and critical constraints and prove
  fail-closed parsing;
- prove `backup`, `export`, `recover`, `audit_read`, and `data_key_admin`
  disclose only their explicitly defined surfaces;
- prove `heap_admin` cannot issue keys or cycle authority;
- verify invalid/absent error non-disclosure;
- verify audit contains certificate and holder fingerprints, not secrets;
- verify no hidden heap appears in metrics, logs, or support output;
- verify no user, role, group, grant, or permission lookup occurs.

### 26.2.1 Differential non-interference

For each heap-bound operation:

1. generate State A and State B with identical target-heap state;
2. populate every non-target heap with different random names, structures,
   values, histories, indexes, sizes, and corruption;
3. execute the same operation and capability against both states;
4. assert byte-equivalent complete functional observations, including errors,
   result order, cardinality, pagination, and termination;
5. for mutations, hash every non-target heap before and after and assert no
   change;
6. repeat across query plans, caches, restarts, compaction, backup, recovery,
   replication, and mixed versions.

The property test is:

```text
same(target_heap) + arbitrary(other_heaps)
    => same(target_functional_observation)
```

Timing is measured separately as a side-channel qualification and does not
relax the prohibition on data or metadata disclosure.

### 26.3 Query escape tests

- collection names containing separators and prefix-like material;
- RQL/SDA/ENR attempts to bind another heap;
- prepared plan compiled under A and executed under B;
- query-cache collision between equal collection/query text in A and B;
- nested and multi-collection queries;
- malicious dialect output;
- oversized and malformed heap identifiers;
- inject a deliberately faulty planner that requests an unconstrained scan
  and prove the isolation kernel still returns only the bound heap;
- attempt to construct or deserialize a forged `HeapCap`;
- attempt to combine two differently bound streams at every query operator;
- prove there is no cross-heap capability type, constructor, protocol field, or
  privileged query escape.

### 26.4 Derived-path tests

- secondary index create/read/drop;
- catalog rebuild;
- history and tombstone reads;
- change streams and resume tokens;
- caches and checkpoints;
- temporary files;
- query explain;
- statistics and metrics.

Every result remains heap-scoped.

### 26.5 Mutation identity tests

- same key, event ID, operation ID, idempotency token, and transaction ID in
  two heaps;
- retry and dedup independently;
- retry one operation ID with a changed operation, target, arguments,
  correlation ID, whitespace, and argument key order; only correlation and
  bytes outside `args` may vary without conflict;
- crash before pending record, after pending record, after each intended
  effect, and before completion receipt, then prove one effect and one receipt;
- prove acknowledgement and conflict state do not cross heaps.

### 26.6 Authority cycling and revocation tests

- hard-cycle the master and prove every old HeapKey fails;
- use the old master after cycling and prove newly signed old-generation keys
  fail;
- prove old established channels, scans, streams, long queries, backups,
  queues, and retries recheck or terminate within the normative profile
  bound;
- graceful-cycle with a deadline and prove non-blacklisted previous-generation
  keys temporarily work;
- blacklist one certificate and one holder fingerprint and prove bounded
  resident-set rejection after security-revision publication;
- prove current-generation keys continue while previous-generation blacklist
  changes;
- expire grace and prove every previous-generation key fails;
- restart during grace and prove the complete blacklist is loaded before
  readiness;
- corrupt or omit durable blacklist state and prove the heap remains
  unavailable;
- restore an old backup and prove old authority does not revive;
- roll wall time backward and forward, restart, and prove security time never
  extends a key or grace interval;
- exceed clock uncertainty and prove time-dependent access fails closed;
- suspect master compromise and prove graceful cycling is rejected;
- prove one current-generation compromise requires cycling and that no
  current-generation blacklist shortcut exists;
- measure security revision, epoch, lease, time, constraint, and rights-check
  hot-path cost.

### 26.6.1 Authority chain and state tests

- exercise every allowed and forbidden edge in §6.6;
- generate every operation/state pair and compare dispatch with §32.3.1;
- suspend from active and read-only, restart, and prove resume restores the
  remembered state;
- inject crashes before and after event publication, head-slot write, anchor
  advance, and selector publication;
- reject event gaps, duplicate revisions, mismatched embedded previous hashes,
  forked equal revisions, and an epoch without a valid root event;
- delete or alter access policy and prove readiness fails;
- restore a previous head, time floor, event directory, and selector
  independently and prove the anchor prevents rollback;
- threshold-recover into a new epoch and prove all generations and revisions
  reset only inside that epoch while the old event chain remains verifiable.

### 26.6.2 Local authority-plane tests

- prove the network protocol has no issue, cycle, master-present, or authority
  recovery operation;
- present master private-key bytes to every network field and prove they grant
  no authority;
- issue HeapKeys only through the local authority tool;
- prove holder possession before issuance and confirm holder fingerprint;
- prove issuance and first-use receipts are durable but never consulted on the
  request path;
- prove master private-key material never enters arguments, logs, telemetry,
  crash reports, heap data, or network messages;
- interrupt create, issue, and cycle at every durable boundary;
- interrupt every creation and cycle state transition and prove no active heap
  lacks a recoverable current master;
- create while the server is running and stopped; prove staged genesis is
  undiscoverable, lock contention cannot race a name mutation, and only the
  descriptor hash signed into root/head can publish;
- crash before and after genesis authority commit and before publication;
  prove recovery either publishes byte-identical staged bytes or leaves a
  permanent unavailable/tombstoned identity;
- stage a same-ID descriptor with a different name, event, deployment, or hash
  and prove `publish_committed_genesis` rejects it;
- prove crash recovery selects one complete authority generation;
- prove a new master key is recoverable before the old generation becomes
  inactive;
- prove an `admin` HeapKey cannot invoke local authority operations;
- prove concurrent cycles serialize by expected generation;
- test `no_master_recovery` loss semantics;
- exercise threshold master recovery, epoch advance, old-key invalidation, and
  recovery-profile change.

### 26.7 Administrative-state tests

- read-only transition during writes;
- suspend during streams and queries;
- change access policy during every operation class and prove the
  security-revision recheck;
- rename under concurrent lookup;
- crash a rename/state/alias release before descriptor staging, authority
  commit, selector publication, descriptor publication, and receipt; prove
  head label 26 and the visible descriptor always converge to one hash;
- retire with active handles;
- hold preventing purge;
- incomplete purge with unavailable replica/tier;
- name quarantine and reuse.

### 26.8 Backup and restore tests

- heap-scoped backup contains no other heap;
- deployment backup lists exact heap IDs;
- restore to new ID;
- disaster-recovery restore retaining ID;
- reject concurrent duplicate authority;
- prove takeover advances epoch, fences the old deployment, and invalidates
  every old HeapKey;
- clone and prove all ownership, subordinate IDs, encryption context, indexes,
  cursors, and authority are rewritten;
- restore package with missing or conflicting heap identity;
- authorization-filtered query cannot create a falsely complete backup.

### 26.9 Recovery tests

- destroy catalogs while two heaps contain equal collection names;
- mix segments or media from multiple heaps;
- remove heap-name registry;
- corrupt heap identity metadata;
- salvage as heap-scoped operator;
- salvage as deployment recovery operator;
- verify unknown identity remains separate;
- verify recovery coverage names holes without cross-attribution;
- destroy segment headers while leaving healthy frames and prove each
  independently labeled frame remains attributable;
- prove mixed-media recovery emits only separately sealed known, unknown, or
  conflict packages.

### 26.10 Cluster tests

- equal routes/keys in different heaps;
- partition movement;
- replica repair;
- control-plane rebuild;
- node replacement;
- split network;
- mixed policy revisions;
- placement and quota independence;
- expire serving leases under partition and prove rejection;
- commit a security transition and prove it is not reported effective before
  every conflicting lease is fenced or expired;
- bootstrap and replace nodes only from quorum authority state, never a data
  backup.

### 26.11 Security tests

- fuzz heap name and ID parsing;
- fuzz canonical HeapKey certificate and authority-transition parsing;
- mutate every signed HeapKey field and prove signature rejection;
- reject duplicate CBOR keys, non-deterministic encodings, wrong algorithms,
  unknown critical fields, excessive sizes, and downgrade attempts;
- swap certificates, holder proofs, audiences, channels, heaps, and
  generations;
- authorization checks at every dispatch entry;
- direct RPC attempts bypassing SDK heap handles;
- stale/crafted resume and cursor tokens;
- object reference substitution;
- timing and error comparison for absent versus unauthorized heaps;
- audit-log injection and cardinality attacks;
- prove heap security snapshots publish atomically under concurrent requests;
- prove established caps observe security-revision changes;
- prove state and policy changes update security revision atomically;
- prove a stale cluster node fails closed;
- prove TLS exporter, nonce, deployment, epoch, audience, and logical-channel
  binding across reconnect, resumption, proxy, and multiplexing cases;
- enforce signature-verification, nonce-memory, denial-log, and cardinality
  admission budgets;
- prove legacy remote mode cannot share process or raw storage with qualified
  multi-heap service;
- explicit administrative bypass review.

### 26.12 Embedded-boundary tests

- two heap handles in one trusted process remain namespace-correct;
- documentation does not claim hostile in-process isolation;
- an embedded compatibility handle cannot accidentally bind another heap;
- separate policy-provider mode, if present, fails closed.

## 27. Implementation gates

### Gate H0 — Vocabulary and identity

- `HeapId`, `CollectionId`, `StreamId`, `DeploymentId`, `AuthorityEpoch`, heap
  name profile, hierarchy, and errors frozen;
- capability and gap documents distinguish heap from store and database;
- no new public API treats collection names as deployment-global.

### Gate H1 — Heap-bound SDK

- heap handle exists;
- heap isolation kernel and unforgeable capability exist;
- all collection/query APIs operate through bound context;
- query and SDK modules have no raw deployment-store iterator;
- crate/module dependency rules prevent raw storage access outside the
  published TCB;
- legacy single-heap compatibility is explicit;
- namespace and collision tests pass.

### Gate H2 — HeapKey authority

- heap-aware protocol negotiation;
- canonical signed HeapKey profile;
- holder proof and channel binding;
- deterministic CBOR/COSE parser limits and algorithm pinning;
- per-heap pinned master public key and authority generation;
- local-only create, issue, and cycle tool;
- recoverable create/cycle state machines and declared master-loss profile;
- no master-key operation in the network protocol;
- always-resident immutable heap security snapshot;
- hard-cycle invalidation;
- trusted-time and security-time rollback enforcement;
- complete RPC rights matrix;
- zero authorization-policy I/O evidence;
- security-revision enforcement tests.

### Gate H3 — Derived and operational coverage

- indexes, history, streams, queries, metrics, logs, export, and background work
  are scoped;
- every admitted data-bearing object has exactly one validated known owner;
- established HeapCaps terminate after authority, state, or policy change;
- optional graceful profile has resident blacklist and bounded grace semantics;
- differential non-interference properties pass;
- no known bypass through non-CRUD paths;
- support tooling defaults to authorized heap scope;
- named isolation profile and closed metadata-declassification registry pass.

### Gate H4 — Backup and recovery

- heap-scoped backup/restore;
- same-identity epoch fencing and new-identity rewrite;
- mixed-heap salvage classification;
- separately qualified local recovery TCB;
- recovery identity survives loss of ordinary catalogs;
- administrative raw recovery is separately authorized and audited.

### Gate H5 — Single-node lifecycle

- quota, retention, hold, tier, purge, backup, and restore behavior include
  immutable heap identity;
- backup rollback cannot revive an old authority root;
- lifecycle barriers terminate stale capabilities within the single-node
  bound;
- single-node crash, rollback, migration, key-loss, and destructive-operation
  qualification passes.

### Gate HC1 — Cluster extension

This gate is required only for the `cluster` deployment profile:

- routing, replication, repair, placement, and quota behavior include heap
  identity;
- authority generations, revisions, grace, and blacklist updates are strongly
  ordered and stale nodes fail closed;
- quorum serving leases define the published stale-acceptance bound;
- backup rollback cannot revive an old authority root;
- multi-process adversarial qualification passes.

### Gate H6 — Isolation claim

Residiuum may claim for a named isolation and deployment profile:

> Cryptographically authorized systems are logically isolated between heaps.

only after:

- H0 through H5 pass for the single-node profile;
- HC1 also passes when the named profile is `cluster`;
- complete-path review finds no unscoped access surface;
- the isolation-kernel state machine has a checked formal model covering read
  confinement, write confinement, ownership disjointness, complete functional
  observations, revocation, concurrency, crash recovery, and restore fencing;
- lease and stale-node properties are included when the named profile is
  `cluster`;
- implementation-to-model proof obligations are documented and reviewed;
- an intentionally faulty query planner cannot escape its bound capability;
- adversarial tests run in CI;
- an external security review covers heap authorization;
- limitations concerning administrators, physical access, process compromise,
  and shared-resource side channels are published.

Before H6, product language is:

> Residiuum provides named heap namespaces; strong access-isolation qualification
> is in progress.

## 28. Deployment profile selections

The security semantics above are not open. Sections 30–40 freeze everything
needed to implement the reference profile. A deployment still publishes:

1. which `AuthorityAnchor` and `MasterKeyProvider` implementations it uses;
2. exact CPU, memory, I/O, cache, bandwidth, and storage budgets if it claims
   `heap-resource-isolated`;
3. any limits shorter than the normative maxima;
4. whether it enables the optional metadata-hardened profile and its closed
   deployment-specific declassification extension.

The reference choices are:

- two-slot authority persistence plus a monotonic `AuthorityAnchor`;
- development file keys for testing only and non-exportable provider keys for
  a server-secure claim;
- TLA+/TLC, Verus, Kani, differential property tests, and fuzzing for H6;
- the normative maximums in this document when no shorter limit is configured;
- the base metadata declassification registry in §13, with no extension.

The filesystem-only development anchor detects corruption and ordinary
partial writes but is not rollback-resistant. It cannot pass H2. The initial
server-secure adapter is a TPM 2.0 NV monotonic counter; an HSM or remote
quorum witness may qualify later by passing the same `AuthorityAnchor`
conformance suite. This distinction does not change formats or application
code.

Legacy material has one fixed rule: a detached unit that lacks sufficient
integrity-protected heap ownership evidence is `Unknown`, even if a directory,
nearby header, or old catalog suggests an owner. A migration profile may prove
ownership through intact store/segment context, but it cannot qualify detached
or damaged legacy units for attribution that their surviving evidence does
not support.

A deployment choice may not be described as harmless to a gate whose claim
depends on it. H2 requires a qualifying key/time/rollback provider, HC1
requires the frozen lease profile, resource claims require concrete budgets,
and H6 requires the connected verification stack in §39.

## 29. Definition of done

Heap support is not complete when `heap("name")` exists.

It is complete for a declared profile when:

- identity is durable and rename-safe;
- collection and stream identity is immutable and every independently
  recoverable unit has integrity-protected ownership;
- every operation is heap-bound;
- HeapKey authorization is self-contained, default-deny, and complete-path;
- Residiuum has no human RBAC or permission database for heap access;
- the network protocol cannot exercise the master key;
- the resident heap security snapshot is complete before readiness;
- administrative state and access policy participate in one resident security
  revision;
- security time is rollback-resistant and fails closed;
- ordinary established-channel access performs no authorization-policy I/O;
- hard cycling makes every prior-generation key cryptographically inert;
- graceful cycling is impossible under suspected master compromise;
- any graceful-cycle blacklist is resident, bounded, durable, and atomic;
- for the cluster profile, stale cluster authority fails closed;
- for the cluster profile, quorum leases and fencing define the cluster
  revocation bound;
- backup rollback cannot revive an old master generation;
- discovery does not leak;
- query, index, history, backup, recovery, and administration preserve scope;
- revocation is bounded;
- compatibility cannot redirect old handles into new heaps;
- destructive operations use immutable identity;
- recovery never guesses or merges scope;
- same-identity restore advances epoch and fences the prior deployment;
- qualified multi-heap service contains no co-resident legacy global access
  path;
- cryptographic encoding, algorithms, parser limits, proof transcript, and
  rights/constraint registry are frozen and adversarially tested;
- the non-interference properties are model-checked and differentially tested;
- query engines have no representable global data scan;
- the trusted computing base and implementation-to-model assumptions are
  published;
- adversarial conformance evidence is published;
- the product states honestly what logical heap isolation does and does not
  protect.

The governing rule is:

> Select one heap. Prove authority. Keep every consequence inside that heap.

## 30. Developer implementation contract

Sections 1–29 define product and security behavior. Sections 30 onward freeze
the first implementation profile so developers do not invent incompatible
types, module boundaries, identifiers, encodings, or migration behavior.

There is no “reasonable implementation choice” escape hatch between the two
parts. If a frozen implementation clause appears to contradict a governing
invariant, implementation stops and the specification is corrected first.
Developers do not choose whichever sentence is more convenient. External
documents, existing legacy code, and prototypes cannot silently override this
profile.

The implementation profile label is:

```text
residiuum-heap-v1
```

The profile targets the existing Rust 1.88 workspace and existing draft frame
and RPC formats. Any change to a numeric value, byte layout, signature input,
state transition, or module boundary in these sections is a specification
change requiring updated golden vectors and compatibility review.

### 30.1 Initial delivery scope

Development proceeds in this order:

1. single-node embedded namespace and storage confinement;
2. single-node TLS service with HeapKey authentication;
3. backup, restore, examination, and recovery confinement;
4. qualified single-node adversarial and formal checks;
5. cluster authority log, leases, placement, and multi-process qualification.

Cluster work does not block implementation of H0–H4. It MUST NOT cause
single-node code to contain a fake lease or silently accept an always-valid
cluster lease.

### 30.2 New crates and dependency graph

Add two workspace crates:

```text
crates/residiuum-heap/
crates/residiuum-authority/
```

Kernel package:

```toml
[package]
name = "residiuum-heap"
version.workspace = true
license = "MIT"
edition.workspace = true
rust-version.workspace = true
authors.workspace = true
repository.workspace = true
description = "Residiuum heap identity, capability, and authority kernel."

[dependencies]
residiuum-format.workspace = true
serde.workspace = true
thiserror.workspace = true
getrandom.workspace = true
sha2 = "0.10"
ed25519-dalek = { version = "3.0.0", default-features = false,
                  features = ["alloc", "fast", "zeroize"] }
arc-swap = "1.7"
zeroize = "1.8"
```

Add `"crates/residiuum-heap"` to workspace members and:

```toml
residiuum-heap = { path = "crates/residiuum-heap", version = "0.2.0" }
```

to `[workspace.dependencies]`, using the workspace release version rather than
copying a stale literal when the workspace version changes. Crates consume it
as `residiuum-heap.workspace = true`.

Local authority package:

```toml
[package]
name = "residiuum-authority"
version.workspace = true
license = "AGPL-3.0-or-later"
edition.workspace = true
rust-version.workspace = true
authors.workspace = true
repository.workspace = true
description = "Local-only Residiuum heap authority controller."

[dependencies]
residiuum-heap.workspace = true
residiuum-format.workspace = true
residiuum-store = { workspace = true, features = ["authority-provisioning"] }
clap.workspace = true
thiserror.workspace = true
zeroize = "1.8"
```

Add:

```toml
residiuum-authority = { path = "crates/residiuum-authority", version = "0.2.0" }
```

to workspace dependencies using the same workspace-version rule.
`residiuum-authority` produces a separate `residiuum-authority` executable. Neither
`residiuum-server` nor the qualified `residiuum` data-service executable depends on
or links this crate.

The workspace lockfile pins exact transitive versions. Dependency upgrades
that affect key parsing, signature verification, CBOR, zeroization, or MSRV
require golden-vector and negative-corpus reruns.

The permitted dependency direction is:

```text
residiuum-format
      ▲
      │
residiuum-heap
  ▲   ▲   ▲   ▲
  │   │   │   └──────── residiuum-authority (also uses store/authority-provisioning)
  │   │   └──────────── residiuum-client
  │   └──────────────── residiuum-store
  │                           ▲
  └──────────────────┬────────┼──────────────┐
                     │        │              │
                 residiuum-sdk  residiuum-cluster  residiuum-server
                     ▲          ▲              ▲
                     └──────────┴──── residiuum-cli
```

`residiuum-heap` MUST NOT depend on `residiuum-store`, `residiuum-sdk`, `residiuum-server`,
`residiuum-cluster`, SDA, RQL, ENR, a filesystem abstraction, or a network
runtime.

`residiuum-authority` MUST NOT depend on `residiuum-server`, `residiuum-sdk`,
`residiuum-client`, `residiuum-cluster`, SDA, RQL, ENR, or a TCP/HTTP runtime. Its
only IPC client is the bounded local barrier/reload protocol defined in §8.9.
The `residiuum-store/authority-provisioning` feature exposes only
non-discoverable staged genesis writes and offline publication under the
required locks; it exposes no live data read, write, scan, query, or server
operation and is never enabled in the qualified data-service target.

### 30.3 `residiuum-heap` module ownership

The crate contains exactly these public implementation areas:

```text
src/
  lib.rs
  ids.rs             HeapId, CollectionId, StreamId, DeploymentId, epochs
  rights.rs          Rights bitmap and Operation registry
  constraints.rs     Closed constraint decoder and evaluator
  certificate.rs     HeapKeyCertificate COSE codec and verification
  holder_proof.rs    Channel-bound proof codec and verification
  capability.rs      HeapCap and capability-instance rules
  authority.rs       Authority events and transition validation
  snapshot.rs        HeapSecuritySnapshot and atomic HeapSlot
  security_time.rs   Time floor and uncertainty decisions
  recovery_auth.rs   Threshold-recovery public data and verification
  wire.rs            Numeric CBOR/COSE labels and protocol constants
  error.rs           Closed fail-closed error type
```

Private-key provider, filesystem, TLS, server dispatch, storage, query,
backup, and recovery-media code do not belong in this crate.

### 30.4 Existing crate changes

The required module placement is:

```text
residiuum-format/src/canonical_cbor.rs
    generic bounded deterministic CBOR values and COSE array support

residiuum-store/src/heap/
    host.rs
    heap_store.rs
    maintenance_store.rs
    replica_store.rs
    recovery_store.rs
    ownership.rs
    subject_v2.rs
    catalog.rs
    authority_operational.rs
    migration.rs

residiuum-store/src/kernel/
    physical_store.rs
    physical_index.rs
    physical_segments.rs

residiuum-client/src/heap_handshake.rs
residiuum-sdk/src/heap.rs
residiuum-sdk/src/heap_collection.rs
residiuum-server/src/heap_auth.rs
residiuum-server/src/heap_dispatch.rs
residiuum-server/src/heap_registry.rs
residiuum-server/src/heap_authority_reload.rs
residiuum-cluster/src/heap_control.rs
residiuum-cluster/src/heap_lease.rs
residiuum-cli/src/heap.rs

residiuum-authority/src/
    main.rs
    command.rs
    filesystem_store.rs
    key_provider.rs
    ceremony.rs
    reload_notify.rs
```

The ordinary `residiuum` CLI may perform HeapKey-authorized data and heap
administration. Master issuance, cycling, blacklist, grace, epoch recovery,
and recovery-profile commands exist only in the separate
`residiuum-authority` executable.

The current `residiuum-server::authz` token/role implementation remains only for
the isolated legacy server profile. Qualified heap dispatch does not call it.

### 30.5 Compilation firewall

The current public `residiuum_store::Store` allows global scans and raw subjects.
It cannot remain reachable from qualified upper layers.

During H1:

- rename its implementation to crate-private `kernel::PhysicalStore`;
- expose `StoreHost` with no get, put, scan, index, or raw-path methods;
- expose capability-gated `HeapStore`, `MaintenanceStore`, `ReplicaStore`, and
  `RecoveryStore` façades;
- remove `Store` re-exports from `residiuum-store::lib`;
- update embedded, SDK, server, and cluster code to use the correct façade;
- keep any legacy raw wrapper in a separate legacy-only crate or binary that
  cannot be linked into `residiuum-server` with `residiuum-heap-v1`.

CI adds `scripts/check_heap_architecture.sh`. It fails when:

- `residiuum-sdk`, `residiuum-server`, query, SDA host, index host, or client modules
  import `kernel`, `PhysicalStore`, raw segment catalogs, or raw iterators;
- any type outside `residiuum-heap` implements or constructs a capability;
- `HeapCap`, `HeapMaintenanceCap`, `ReplicaCap`, or `RecoveryCap` implements
  `Serialize` or `Deserialize`;
- the qualified server links legacy role dispatch or diagnostic line protocol;
- `residiuum-server`, the qualified `residiuum` data-service target, or any
  client-facing dispatcher depends on `residiuum-authority`, implements
  `MasterKeyProvider`, stores a `dyn MasterKeyProvider`, contains
  `MasterAuthorityStore`, or accepts a raw authority event instead of
  `AuthorizedOperationalEvent`;
- the qualified data-service feature graph enables
  `residiuum-store/authority-provisioning` or links its provisioning symbols;
- a cluster component decodes `ReplicatedMasterAuthorityEvent` anywhere
  except the mTLS-authenticated security-control Raft peer module, or that
  module can construct/sign an event rather than verify and replicate one;
- a new RPC operation lacks an `Operation` registry entry;
- a new data-bearing frame kind lacks ownership requirements.

### 30.6 Concrete identity types

`residiuum-heap::ids` defines:

```rust
#[repr(transparent)]
pub struct HeapId([u8; 16]);

#[repr(transparent)]
pub struct CollectionId([u8; 16]);

#[repr(transparent)]
pub struct StreamId([u8; 16]);

#[repr(transparent)]
pub struct DeploymentId([u8; 16]);

#[repr(transparent)]
pub struct CertificateId([u8; 16]);

#[repr(transparent)]
pub struct CapabilityId([u8; 16]);

#[repr(transparent)]
pub struct AuthorityEpoch(u64);

#[repr(transparent)]
pub struct AuthorityGeneration(u64);

#[repr(transparent)]
pub struct SecurityRevision(u64);
```

Requirements:

- byte arrays are private;
- `from_bytes` validates semantic restrictions;
- `new_random` uses `getrandom`;
- IDs implement `Copy`, `Clone`, `Eq`, `Ord`, and `Hash`;
- IDs do not implement implicit conversion from names or paths;
- display uses lowercase canonical UUID form;
- parsing accepts only canonical lowercase hyphenated UUID text;
- zero UUIDs are invalid for durable identities;
- epoch, generation, and revision zero are invalid;
- IDs are never reused.

The RFC UUID variant and v4 version bits are set when generating `HeapId`,
`CollectionId`, `StreamId`, `DeploymentId`, `CertificateId`, and
`CapabilityId`.

### 30.7 Concrete capability shape

The conceptual generative `H` is implemented using an unforgeable
capability-instance object and pointer identity:

```rust
pub struct HeapCap {
    inner: Arc<CapInner>,
}

struct CapInner {
    capability_id: CapabilityId,
    slot: Arc<HeapSlot>,
    deployment_id: DeploymentId,
    certificate_id: CertificateId,
    certificate_fingerprint: [u8; 32],
    holder_fingerprint: [u8; 32],
    authority_epoch: AuthorityEpoch,
    authority_generation: AuthorityGeneration,
    validated_security_revision: SecurityRevision,
    validated_authority_chain_head_hash: [u8; 32],
    effective_rights: Rights,
    effective_constraints: Constraints,
    validity_deadline_unix_s: u64,
}
```

Rules:

- fields and constructors are private to `residiuum-heap`;
- `HeapCap` is cloneable inside the trusted process but not serializable;
- `Debug` prints only heap ID, generation, revision, and redacted
  fingerprints;
- equality for composition uses `Arc::ptr_eq(&a.inner, &b.inner)`;
- equal `HeapId` from distinct capability instances is insufficient for query
  composition;
- capability validation returns `HeapCap` only after snapshot, certificate,
  proof, time, state, policy, and lease checks succeed;
- `validity_deadline_unix_s` is the earlier of certificate expiry and an
  applicable previous-generation grace expiry; request-duration constraints
  are relative to request admission and cluster leases are checked through
  the current resident slot;
- an embedded trusted process obtains a capability from
  `StoreHost::open_embedded_heap`, never by constructing fields.

The frozen v1 behavior on security-revision, epoch, generation, or authority
chain-head mismatch is to terminate the capability, not mutate it in place. A
remote SDK reconnects and performs a new holder proof; embedded trusted code
reopens the heap. A refreshed cluster lease for the same security tuple does
not change capability identity; a missing, expired, or tuple-mismatched lease
denies the request and closes the remote connection. This avoids a permanently
stale handle revalidating on every request and preserves capability-instance
identity.

SDK handles are:

```rust
pub struct Heap {
    cap: HeapCap,
    backend: HeapBackend,
}

pub struct Collection {
    cap: HeapCap,
    id: CollectionId,
    name_at_open: String,
    backend: HeapBackend,
}
```

`Collection` has no setter for heap or collection identity. Query builders
reject inputs whose capabilities are not pointer-identical.

### 30.8 Capability-gated store façades

The storage boundary exposes these shapes:

```rust
pub struct StoreHost { /* no raw data methods */ }

impl StoreHost {
    pub fn bind(&self, cap: &HeapCap) -> Result<HeapStore, StoreError>;
    pub fn bind_maintenance(
        &self,
        cap: &HeapMaintenanceCap,
    ) -> Result<MaintenanceStore, StoreError>;
    pub fn bind_replica(&self, cap: &ReplicaCap)
        -> Result<ReplicaStore, StoreError>;
    pub fn bind_recovery(&self, cap: &RecoveryCap)
        -> Result<RecoveryStore, StoreError>;
}
```

`HeapStore` exposes heap-local collection lookup, reads, writes, history,
indexes, streams, queries, and heap-scoped backup/export. It accepts immutable
IDs, not physical subjects or paths.

`MaintenanceStore` exposes only the maintenance operations encoded in its
internal rights.

`ReplicaStore` accepts only frames whose verified ownership and committed
placement match its heap and replica assignment.

`RecoveryStore` emits `Known`, `Unknown`, or `Conflict` packages and has no
method returning a deployment-global application iterator.

Every façade method checks the capability against the current resident slot at
the security linearization point defined in §6.6 and §8.16. A mismatch returns
the terminal-capability error; the method never refreshes the instance.

### 30.9 Frozen Rust SDK entry surface

The qualified synchronous SDK adds:

```rust
pub trait HolderSigner: Send + Sync {
    fn public_key(&self) -> [u8; 32];
    fn sign_holder_proof(
        &self,
        message: &[u8],
    ) -> Result<[u8; 64], CredentialError>;
}

pub struct HeapCredential {
    certificate: HeapKeyCertificate,
    signer: Arc<dyn HolderSigner>,
}

impl HeapCredential {
    pub fn new(
        certificate_cose: &[u8],
        signer: Arc<dyn HolderSigner>,
    ) -> Result<Self, CredentialError>;
}

pub struct RemoteHeapOptions {
    pub tls: TlsClientOptions,
    pub credential: HeapCredential,
    pub expected_heap_name: Option<String>,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_connect_attempts: NonZeroU32,
    pub retry_backoff: Duration,
}

pub struct ResidiuumDeployment {
    host: StoreHost,
}

impl RemoteHeapOptions {
    pub fn new(tls: TlsClientOptions, credential: HeapCredential) -> Self;
    pub fn expected_heap_name(self, name: impl Into<String>) -> Self;
    pub fn connect_timeout(self, value: Duration) -> Self;
    pub fn request_timeout(self, value: Duration) -> Self;
    pub fn max_connect_attempts(self, value: NonZeroU32) -> Self;
    pub fn retry_backoff(self, value: Duration) -> Self;
}

impl Residiuum {
    pub fn connect_heap(
        url: impl AsRef<str>,
        options: RemoteHeapOptions,
    ) -> Result<Heap, Error>;

    pub fn open_compatibility_heap(
        path: impl AsRef<Path>,
    ) -> Result<Heap, Error>;

    pub fn open_deployment(
        path: impl AsRef<Path>,
    ) -> Result<ResidiuumDeployment, Error>;
}

impl ResidiuumDeployment {
    pub fn heap(&self, name: &str) -> Result<Heap, Error>;
    pub fn heap_by_id(&self, id: HeapId) -> Result<Heap, Error>;
}

impl Heap {
    pub fn id(&self) -> HeapId;
    pub fn collection(&self, name: &str) -> Result<Collection, Error>;
    pub fn collection_by_id(
        &self,
        id: CollectionId,
    ) -> Result<Collection, Error>;
    pub fn stream(&self, name: &str) -> Result<Stream, Error>;
    pub fn stream_by_id(&self, id: StreamId) -> Result<Stream, Error>;
}
```

`HeapCredential::new(certificate, signer)` parses and verifies structural
canonicality, rejects a signer whose public key differs from the certificate,
and does not contact a server. The master signature and current authority are
verified by the server. The type is cloneable only by explicitly cloning its
signer handle; it is not `Debug`, `Serialize`, or `Deserialize`.

`RemoteHeapOptions::new` sets no expected name, a 5-second connect timeout,
30-second request timeout, three total connect attempts, and 50-millisecond
retry backoff. Zero attempts are unrepresentable.

`HolderSigner` may wrap an in-memory Ed25519 key, OS keystore, agent, TPM, or
HSM. The SDK never asks it to export secret bytes. The reference
`InMemoryHolderKey` is feature-gated as `dangerous-key-export`; constructors
from a 32-byte seed are absent from default public Rustdoc.

`TlsClientOptions` is mandatory and must authenticate the deployment's server
identity. `RemoteHeapOptions` has no token, role, username, diagnostic-line,
plaintext, TLS-version, or caller-supplied heap-ID setting.

`ResidiuumDeployment` is embedded trusted-process authority, is not serializable,
and has no remote constructor. It can resolve heaps but exposes no
deployment-global application-data iterator, query, collection, transaction,
or backup. Heap creation, retirement, authority, and cross-heap import remain
explicit administrative APIs and never hide inside `heap(name)`.

The existing `Residiuum::connect`, `ConnectOptions::auth_token`, and name-based
`RpcRequest` remain legacy-only and are deprecated when the heap feature is
enabled. They cannot select the qualified listener. Existing
`Residiuum::open(path)` becomes a deprecated spelling of
`open_compatibility_heap(path)` and never opens a deployment-global
multi-heap handle.

## 31. Frozen HeapKey binary profile

### 31.1 Common encoding rules

All HeapKey security objects use untagged COSE Sign1:

```text
[
  protected : bstr,
  unprotected : {},
  payload : bstr,
  signature : bstr(64)
]
```

The protected header is deterministic CBOR:

| Label | Value |
|---:|---|
| 1 (`alg`) | `-8` (`EdDSA`) |
| 3 (`content type`) | exact profile content-type text |

The unprotected map MUST be empty. Receiving an unprotected field is an error.
CBOR tags are rejected. The payload is a deterministic integer-keyed map.

The signature input is the standard COSE `Sig_structure`:

```text
[
  "Signature1",
  protected_header_bytes,
  external_aad,
  payload_bytes
]
```

Verification uses `ed25519_dalek::VerifyingKey::verify_strict`. Public keys are
exactly 32 bytes and signatures exactly 64 bytes. Non-canonical public keys,
weak points, malformed signatures, trailing bytes, duplicate keys, unknown
critical fields, and non-deterministic encodings are rejected.

### 31.2 HeapKey certificate payload

Content type:

```text
application/residiuum.heap-key+cbor
```

External AAD:

```text
RESIDIUUM-HEAPKEY-CERTIFICATE-V1
```

Payload labels:

| Label | Field | Type | Rule |
|---:|---|---|---|
| 1 | `profile_version` | uint | exactly `1` |
| 2 | `deployment_id` | bstr(16) | non-zero UUIDv4 |
| 3 | `heap_id` | bstr(16) | non-zero UUIDv4 |
| 4 | `authority_epoch` | uint | `1..=u64::MAX` |
| 5 | `authority_generation` | uint | `1..=u64::MAX` |
| 6 | `certificate_id` | bstr(16) | non-zero UUIDv4 |
| 7 | `holder_public_key` | bstr(32) | strict Ed25519 key |
| 8 | `rights` | uint | non-zero; only bits defined in §32.1 |
| 9 | `constraints` | array | canonical §32.2 entries |
| 10 | `not_before` | uint | Unix seconds |
| 11 | `expires_at` | uint | Unix seconds; greater than label 10 |
| 12 | `audience` | text | exactly `residiuum:data:v1` |
| 13 | `issuer_master_key_id` | bstr(32) | SHA-256 of raw master public key |

Every label is required, appears once, and no other label is accepted in v1.
Certificate lifetime is at most 7,776,000 seconds.

### 31.3 Holder proof payload

Content type:

```text
application/residiuum.heap-proof+cbor
```

External AAD:

```text
RESIDIUUM-HEAPKEY-HOLDER-PROOF-V1
```

Payload labels:

| Label | Field | Type | Rule |
|---:|---|---|---|
| 1 | `profile_version` | uint | exactly `1` |
| 2 | `proof_id` | bstr(16) | cryptographically random UUIDv4 |
| 3 | `created_at` | uint | Unix seconds |
| 4 | `certificate_hash` | bstr(32) | SHA-256 of complete certificate COSE bytes |
| 5 | `deployment_id` | bstr(16) | equals certificate |
| 6 | `heap_id` | bstr(16) | equals certificate |
| 7 | `authority_epoch` | uint | equals certificate and snapshot |
| 8 | `audience` | text | exactly `residiuum:data:v1` |
| 9 | `server_nonce` | bstr(32) | challenge bytes |
| 10 | `tls_exporter` | bstr(32) | RFC 9266 value for this TLS channel |
| 11 | `protocol` | array(3) | `[heap_profile=1, rpc_major=1, rpc_minor=0]` |

Every label is required and no other label is accepted. The proof is signed by
the holder key in the referenced certificate.

### 31.4 Authority transition payload

Content type:

```text
application/residiuum.heap-authority-transition+cbor
```

External AAD:

```text
RESIDIUUM-HEAP-AUTHORITY-TRANSITION-V1
```

Payload labels:

| Label | Field | Type |
|---:|---|---|
| 1 | `profile_version` | uint, exactly 1 |
| 2 | `deployment_id` | bstr(16) |
| 3 | `heap_id` | bstr(16) |
| 4 | `authority_epoch` | uint |
| 5 | `expected_security_revision` | uint |
| 6 | `previous_transition_head_hash` | bstr(32) |
| 7 | `from_generation` | uint |
| 8 | `from_master_key_id` | bstr(32) |
| 9 | `to_generation` | uint; exactly from + 1 |
| 10 | `to_master_public_key` | bstr(32) |
| 11 | `transition_id` | bstr(16), random UUIDv4 |
| 12 | `effective_at` | uint, Unix seconds |
| 13 | `grace_deadline` | null or uint |
| 14 | `initial_blacklist` | array of §31.5 entries |
| 15 | `new_master_possession_proof` | bstr(64) |

The legacy field name `previous_transition_head_hash` means the SHA-256 of the
complete preceding authority-chain event, whether that event is a transition,
master-signed mutation, or operational authority event. The old master signs
the COSE object. Label 15 signs the SHA-256 digest of the deterministic CBOR
map containing labels 1–14 using the new master. Its exact signed message is:

```text
ASCII("RESIDIUUM-HEAP-NEW-MASTER-POSSESSION-V1")
|| 0x00
|| SHA-256(canonical_map(labels 1..14))
```

### 31.4.1 Authority root event

Creation and threshold recovery establish an epoch root using deterministic
CBOR:

| Label | Field | Type |
|---:|---|---|
| 1 | profile version | uint, exactly 1 |
| 2 | deployment ID | bstr(16) |
| 3 | heap ID | bstr(16) |
| 4 | from authority epoch | uint; zero only for creation |
| 5 | to authority epoch | uint; 1 for creation, otherwise from + 1 |
| 6 | reason | 1 creation, 2 master recovery, 3 recovery-policy replacement |
| 7 | new generation | uint, exactly 1 |
| 8 | new master public key | strict Ed25519 bstr(32) |
| 9 | new recovery profile | 1 no-master-recovery, 2 threshold-master-recovery |
| 10 | new recovery public keys | canonical bstr(32) array |
| 11 | new recovery threshold | uint |
| 12 | root event ID | random UUIDv4 bstr(16) |
| 13 | effective at | Unix seconds |
| 14 | previous authority-chain head | bstr(32), all zero only for creation |
| 15 | current-master signature | bstr(64) or null |
| 16 | recovery signatures | canonical array described below |
| 17 | new-master possession signature | bstr(64) |
| 18 | storage genesis descriptor hash | BLAKE3-256 bstr(32) |
| 19 | current heap-descriptor hash | BLAKE3-256 bstr(32) |

The common signing message is:

```text
ASCII("RESIDIUUM-HEAP-AUTHORITY-ROOT-V1")
|| 0x00
|| SHA-256(canonical_map(labels 1..14, 18, and 19))
```

Label 17 signs that message with label 8's new master. Label 15, when required,
signs the same message with the current master. Each label 16 entry is
`{1: recovery_public_key_hash, 2: signature}` and signs the same message with
a currently pinned recovery key; entries are sorted by key hash and duplicates
are rejected.

Authorization rules are exact:

- creation: from epoch zero, zero previous hash, null current-master
  signature, empty recovery signatures, valid new-master possession, and
  protected local creation authority; labels 18 and 19 both equal the staged
  sequence-1 `HeapDescriptor` hash;
- master recovery: reason 2, null current-master signature, signatures from at
  least the currently pinned threshold, valid new-master possession, and
  protected local recovery authority; label 18 equals the current head's
  immutable storage-genesis hash and label 19 equals the current head's
  descriptor hash;
- recovery-policy replacement: reason 3, valid current-master signature,
  signatures from at least the currently pinned threshold, valid new-master
  possession, protected local recovery authority, and unchanged labels 18–19.

New recovery-profile fields obey §35.1 head-validation rules. A
no-master-recovery heap therefore cannot replace its recovery policy. A root
event resets generation and all revision counters to 1 inside its new epoch,
clears grace and blacklist, preserves a retired/purged tombstone, and
invalidates every earlier certificate.

### 31.5 Authority mutation and blacklist entries

Authority mutation content type:

```text
application/residiuum.heap-authority-mutation+cbor
```

External AAD:

```text
RESIDIUUM-HEAP-AUTHORITY-MUTATION-V1
```

Payload labels:

| Label | Field | Type |
|---:|---|---|
| 1 | `profile_version` | uint, exactly 1 |
| 2 | `deployment_id` | bstr(16) |
| 3 | `heap_id` | bstr(16) |
| 4 | `authority_epoch` | uint |
| 5 | `authority_generation` | uint |
| 6 | `expected_security_revision` | uint |
| 7 | `previous_transition_head_hash` | bstr(32) |
| 8 | `mutation_id` | bstr(16), random UUIDv4 |
| 9 | `effective_at` | uint, Unix seconds |
| 10 | `mutation_kind` | uint |
| 11 | `mutation_value` | kind-specific value |

Mutation kinds:

| Value | Meaning | Label 11 |
|---:|---|---|
| 1 | add blacklist entry | one blacklist entry |
| 2 | remove blacklist entry | one blacklist entry |
| 3 | end grace | null |
| 4 | permanently fail committed creation | `{1: genesis_hash, 2: reason}` |

Values 5–255 are reserved. These four mutations are local-authority
operations signed by the current master. Ordinary administrative state and
policy changes are not master-signed and use §31.5.1; otherwise a network
`HeapAdmin` operation would require the network server to exercise the master
key, violating §8.8.

Kinds 1 and 2 are valid only while grace exists and only for the recorded
previous generation. Add of an existing entry and removal of a missing entry
are rejected unless the same mutation ID already committed, in which case the
original receipt is returned. Kind 3 atomically clears previous generation,
previous key, grace deadline, and the entire blacklist; it may end grace early
but can never extend it. Every kind increments authority and security
revisions exactly once at its effective barrier.

Kind 4 is valid only in epoch 1 after a creation root committed and before any
successful genesis-publication receipt exists. `genesis_hash` must equal head
labels 25 and 26. `reason` is 1 staged bytes lost, 2 staged bytes conflict, or
3 operator-declared unrecoverable provider/storage failure. It atomically sets
state and tombstone to purged, increments state revision, and permanently
prevents serving, restoration under the same ID, or name reuse except through
the ordinary quarantine record. It does not pretend payload was purged; no
payload was ever admitted. This is the sole valid authority state whose
committed descriptor bytes may be absent.

Blacklist entry encoding is:

```text
{ 1: kind, 2: generation, 3: fingerprint }
```

Kinds:

- `1` = certificate SHA-256;
- `2` = raw holder-public-key SHA-256.

`generation` is non-zero and `fingerprint` is bstr(32). Entries sort
lexicographically by canonical encoded bytes. Duplicates are rejected.

### 31.5.1 Operational authority event

An already authenticated HeapKey may authorize state or policy operations in
§32.3. Their durable event is deterministic CBOR, not COSE:

| Label | Field | Type |
|---:|---|---|
| 1 | profile version | uint, exactly 1 |
| 2 | deployment ID | bstr(16) |
| 3 | heap ID | bstr(16) |
| 4 | authority epoch | uint |
| 5 | expected security revision | uint |
| 6 | previous authority-chain head hash | bstr(32) |
| 7 | operation ID | UUIDv4 bstr(16) |
| 8 | operation | uint16 from §32.3 |
| 9 | capability ID | UUIDv4 bstr(16) |
| 10 | certificate hash | bstr(32) |
| 11 | request-binding hash | exact §14.2 hash, bstr(32) |
| 12 | effective at | Unix seconds |
| 13 | resulting administrative state | uint or null |
| 14 | resulting access policy | canonical §35.1 policy map or null |
| 15 | remembered resume state | uint or null |
| 16 | resulting heap-descriptor hash | BLAKE3-256 bstr(32) or null |

Exactly one committed `HeapCap` authorizes a public event. Before commit, the
server rechecks that capability, right, immutable target, request constraints,
expected revision, and current chain head. The only internal exception is
automatic alias release after the §5.5 deadline: operation `0x8001` requires a
non-serializable heap-bound `MaintenanceCap`, sets label 10 to 32 zero bytes,
and uses the ordinary request-binding algorithm over its exact alias argument.
It grants no general administrative path.

The authority store or security-control Raft group then durably authenticates
the event by committing its hash into the monotonic authority chain. A network
request cannot submit this event as bytes or choose labels 2–6, 9–12, or the
resulting values.

The kernel constructs a private-field `AuthorizedOperationalEvent` only from
that successful check. The storage writer accepts that type, never raw event
bytes or a public struct literal. Its constructor and fields are private to
`residiuum-heap`; serialization occurs only after construction. This is the
compile-time firewall between a network handler and arbitrary authority-state
mutation.

Committing the event terminates the initiating capability along with every
other capability at the old security revision. The commit atomically fixes
one bounded receipt, and the initiating request may return that receipt after
the revision change. It may return no heap data and perform no additional
effect. A lost response is recovered only by repeating the same operation ID
and request binding through a newly established capability.

Allowed operation IDs are public 151 and 160–167, including the internal
completion or abort step of 164, plus internal `0x8001` as defined above.
Other operation IDs are rejected. Blacklist, grace, master, epoch,
recovery-root, and key-issuance changes can never use this event type.

Operation 151 sets label 14 and leaves 13, 15, and 16 null. Operations
160–167 stage the exact next `HeapDescriptor` first and set label 16 to its
§34.7 hash; state operations also set labels 13 and 15 as applicable. The
authority head commits that hash before the descriptor is published. A crash
after authority commit resumes publication of only those staged bytes;
missing or different bytes keep the heap unavailable. No catalog or
descriptor write may become discoverable before its authorizing head.

### 31.6 Decoder limits

Before allocation:

- complete certificate: at most 16,384 bytes;
- complete proof: at most 4,096 bytes;
- authority object: at most 16,777,216 bytes;
- CBOR nesting depth: at most 8;
- map entries: at most 64;
- array entries: at most 100,000 only for blacklist; otherwise 64;
- text: at most 128 UTF-8 bytes;
- byte string: at most the field's exact or declared maximum;
- no floats, tags, indefinite lengths, duplicate keys, or trailing bytes.

`residiuum-format::canonical_cbor` performs structural bounds before allocating.
Semantic decoders then require the exact field set.

### 31.7 Holder issuance request

The holder creates its own key and sends the owner a self-signed issuance
request. It is an untagged COSE Sign1 with empty unprotected map, protected
content type `application/residiuum.heap-issuance-request+cbor`, and external AAD:

```text
RESIDIUUM-HEAPKEY-ISSUANCE-REQUEST-V1
```

Payload:

| Label | Field | Type |
|---:|---|---|
| 1 | profile version = 1 | uint |
| 2 | request ID | random UUIDv4 bstr(16) |
| 3 | deployment ID | UUIDv4 bstr(16) |
| 4 | heap ID | UUIDv4 bstr(16) |
| 5 | holder public key | strict Ed25519 bstr(32) |
| 6 | created at | Unix seconds |
| 7 | expires at | Unix seconds, at most 24 hours after creation |
| 8 | holder label | 1–64 printable UTF-8 bytes |

The request is signed by label 5's key. It requests no rights: the owner
chooses rights, constraints, and certificate lifetime locally and confirms
them before signing. The receipt binds request ID, request hash, certificate
hash, chosen claims, and authority revision. Replaying one request may only
return the byte-identical previously issued certificate or fail
`IssuanceRequestConsumed`; it cannot mint different claims.

## 32. Rights, constraints, and operation registry

### 32.1 Rights bitmap

The v1 `u64` bitmap is frozen:

| Bit | Mask | Rust variant |
|---:|---:|---|
| 0 | `0x0000_0000_0000_0001` | `Read` |
| 1 | `0x0000_0000_0000_0002` | `ReadHistory` |
| 2 | `0x0000_0000_0000_0004` | `Write` |
| 3 | `0x0000_0000_0000_0008` | `IndexAdmin` |
| 4 | `0x0000_0000_0000_0010` | `Export` |
| 5 | `0x0000_0000_0000_0020` | `Backup` |
| 6 | `0x0000_0000_0000_0040` | `Restore` |
| 7 | `0x0000_0000_0000_0080` | `AuditRead` |
| 8 | `0x0000_0000_0000_0100` | `PolicyAdmin` |
| 9 | `0x0000_0000_0000_0200` | `LifecycleAdmin` |
| 10 | `0x0000_0000_0000_0400` | `HoldAdmin` |
| 11 | `0x0000_0000_0000_0800` | `Recover` |
| 12 | `0x0000_0000_0000_1000` | `HeapAdmin` |
| 13 | `0x0000_0000_0000_2000` | `Retire` |
| 14 | `0x0000_0000_0000_4000` | `Purge` |
| 15 | `0x0000_0000_0000_8000` | `DataKeyAdmin` |
| 16 | `0x0000_0000_0001_0000` | `PlacementAdmin` |

Bits 17–63 are reserved and MUST be zero. A certificate with a reserved bit set
is rejected; it is not interpreted as a future right by an older server.

`Rights` is a private-field newtype:

```rust
pub struct Rights(u64);
```

It exposes named constants, `contains`, `intersection`, `is_subset_of`, and
`bits_for_audit`. It does not expose unchecked construction.

### 32.2 Constraint encoding

Each constraint is:

```text
{ 1: kind, 2: critical, 3: value }
```

`critical` MUST be `true` in v1. Constraints sort by `kind`; duplicate kinds
are rejected.

| Kind | Rust variant | Value |
|---:|---|---|
| 1 | `CollectionAllowlist` | sorted unique array of bstr(16) |
| 2 | `StreamAllowlist` | sorted unique array of bstr(16) |
| 3 | `OperationAllowlist` | sorted unique array of uint16 operation IDs |
| 4 | `MaxRequestBytes` | uint, `1..=16,777,216` |
| 5 | `MaxResultBytes` | uint, `1..=1,073,741,824` |
| 6 | `MaxQueryWork` | uint, `1..=u64::MAX` |
| 7 | `MaxDurationMs` | uint, `1..=86,400,000` |
| 8 | `SourceNetwork` | `{1: family, 2: prefix, 3: address}` |

For `SourceNetwork`, family 4 requires bstr(4) and prefix `0..=32`; family 6
requires bstr(16) and prefix `0..=128`. Host bits outside the prefix MUST be
zero. It is evaluated against the authenticated transport peer, not an
application header.

Multiple policy and certificate constraints combine as:

- allowlists: set intersection;
- byte/work/duration maxima: minimum;
- source networks: logical intersection;
- absent constraint: no additional narrowing;
- empty intersection: capability validation fails.

No constraint parser accepts regex, script, SDA, name glob, heap name,
filesystem path, external callback, or mutable user identity.

### 32.3 Operation registry

`residiuum-heap::rights::Operation` is `#[repr(u16)]`. The numeric IDs below are
the qualified wire representation and are permanently allocated by this
table. Allocation freezes identity and minimum rights but does not by itself
make an operation callable. Wire names are frozen SDK, audit, schema, and
diagnostic names; qualified request dispatch never parses them as authority.
Unknown or reserved numeric IDs return `UnknownOperation` and never fall back
to an administrator right. Legacy string dispatch exists only on the isolated
legacy listener.

Public operations contain no heap-derived detail:

| ID | Wire name | Right | Notes |
|---:|---|---|---|
| 1 | `ping` | public | fixed process response |
| 2 | `health_live` | public | fixed liveness only |
| 3 | `health_ready` | public | Boolean readiness; no heap reason |

Heap data and metadata operations:

| ID | Wire name | Required right |
|---:|---|---|
| 100 | `heap_describe` | `HeapAdmin` |
| 101 | `heap_directory` | `Read` |
| 105 | `collection_open` | `Read` |
| 106 | `collection_create` | `HeapAdmin` |
| 107 | `collection_rename` | `HeapAdmin` |
| 108 | `collection_retire` | `HeapAdmin` |
| 110 | `list_collections` | `Read` |
| 111 | `get` | `Read` |
| 112 | `get_bytes` | `Read` |
| 113 | `get_payload` | `Read` |
| 114 | `list_keys` | `Read` |
| 115 | `scan_json` | `Read` |
| 116 | `find` | `Read` |
| 117 | `history` | `ReadHistory` |
| 118 | `rql_query` | `Read` |
| 119 | `sda_query` | `Read` |
| 120 | `put` | `Write` |
| 121 | `put_bytes` | `Write` |
| 122 | `delete` | `Write` |
| 130 | `index_list` | `Read` |
| 131 | `index_create` | `IndexAdmin` |
| 132 | `index_drop` | `IndexAdmin` |
| 133 | `index_rebuild` | `IndexAdmin` |
| 140 | `export` | `Export` |
| 141 | `backup_create` | `Backup` |
| 142 | `restore_import` | `Restore` |
| 143 | `audit_read` | `AuditRead` |
| 150 | `policy_get` | `HeapAdmin` |
| 151 | `policy_set` | `PolicyAdmin` |
| 152 | `lifecycle_set` | `LifecycleAdmin` |
| 153 | `hold_place` | `HoldAdmin` |
| 154 | `hold_release` | `HoldAdmin` |
| 155 | `recover_examine` | `Recover` |
| 156 | `tier_move` | `LifecycleAdmin` |
| 160 | `heap_rename` | `HeapAdmin` |
| 161 | `heap_suspend` | `HeapAdmin` |
| 162 | `heap_resume` | `HeapAdmin` |
| 163 | `heap_retire` | `Retire` |
| 164 | `heap_purge` | `Purge` |
| 165 | `heap_set_read_only` | `HeapAdmin` |
| 166 | `heap_set_active` | `HeapAdmin` |
| 167 | `heap_alias_release` | `HeapAdmin` |
| 170 | `data_key_status` | `DataKeyAdmin` |
| 171 | `data_key_rotate` | `DataKeyAdmin` |
| 172 | `placement_get` | `HeapAdmin` |
| 173 | `placement_set` | `PlacementAdmin` |
| 180 | `heap_metrics` | `HeapAdmin` |
| 181 | `heap_health` | `Read` |
| 190 | `stream_open` | `Read` |
| 191 | `stream_create` | `HeapAdmin` |
| 192 | `stream_rename` | `HeapAdmin` |
| 193 | `stream_retire` | `HeapAdmin` |
| 194 | `stream_append` | `Write` |
| 195 | `stream_read` | `Read` |
| 196 | `stream_history` | `ReadHistory` |
| 200 | `batch` | union of every enclosed operation |

`batch` is rejected before execution unless every enclosed operation is
registered, uses the same capability instance, satisfies constraints, and has
all required rights. It is never a way to defer authorization until after a
partial mutation.

### 32.3.1 Administrative-state admission matrix

State admission is generated from active entries in the operation registry
and checked in addition to rights:

| State | Admitted public operation IDs |
|---|---|
| active | every active heap operation except 162, 164, and 166 |
| read-only | active set except 106–108, 120–122, 131–133, 142, 165, and 191–194; add 166 |
| suspended | 100, 141, 143, 150–160, 162–163, 167, 170–180 |
| retired | 100, 141, 143, 150–155, 164, 167, 170–171, 180 |
| purging | 100, 141, 143, 150, 153–155, 164, 167, 170, 180 |
| purged | 100, 143, 164, 167, 180 |

Ranges include only IDs whose registry status is `active`; an allocated or
unallocated number does not become callable. Public process operations 1–3
are outside heap state.
Internal purge completion/abort uses the same 164 authority path but is
available only to a non-serializable local `MaintenanceCap`.

Operation 163 moves active, read-only, or suspended to retired. Operation 164
is valid only after retirement: retired begins purge, purging resumes the same
operation ID, and purged returns the original completion receipt only for that
same operation ID. A new ID against purged is `InvalidStateTransition`. Purge
never retires a heap implicitly or creates a second purge.

The batch state set is the intersection of every child operation's state set.
State denial happens before a child read or effect. A state change increments
security revision, so a pre-existing capability cannot retain the old matrix.

Operation IDs `0x8000..=0xffff` are reserved for authenticated cluster-peer
and protected local control operations. They are never authorized by a
HeapKey.

Current legacy names map as follows during migration:

| Legacy name | Qualified result |
|---|---|
| `store_info` | removed; use `heap_describe` without physical path |
| `directory` | removed; use heap-filtered `heap_directory` |
| `health` | `heap_health` |
| `metrics` / `admin_stats` | `heap_metrics` |
| `salvage_export` | `recover_examine` or local recovery plane |
| `purge` | `heap_purge` |
| `force_reconfig` | protected cluster-control operation, never HeapKey |
| `raft_*` | authenticated peer protocol, never data dispatch |

### 32.4 Machine-readable registry

The source of truth is committed as:

```text
spec/heap/operations-v1.json
```

Each entry contains:

```json
{
  "id": 111,
  "wire_name": "get",
  "status": "active",
  "surface": "heap",
  "rights_mask": 1,
  "request_schema": "rpc-v1/get.request.json",
  "response_schema": "rpc-v1/get.response.json",
  "allowed_states": ["active", "read_only"],
  "high_impact_confirmation": null,
  "returns_data": true,
  "mutates_data": false,
  "authorization_checkpoint": "before_read"
}
```

`build.rs` in `residiuum-heap` generates the Rust enum, name parser, rights table,
and test cases from this file. CI rejects generated diffs and duplicate IDs,
names, or incomplete dispatch coverage.

`spec/heap/rpc-v1.schema.json` defines the common envelope and
`spec/heap/rpc-v1/` contains one request and response JSON Schema for every
active operation. An allocated operation begins as `reserved`. Changing it to
`active` is a contract change that MUST land before or in the same pull request
as both exact schemas, dispatch exhaustiveness tests, public error mapping,
resource bounds, and at least one accepted and one rejected fixture. Runtime
configuration cannot activate it.

HP-000 activated operations 1–3. Their schemas are exact:

| ID | Request `args` | Successful `result` |
|---:|---|---|
| 1 | `{}` | `{"pong":true}` |
| 2 | `{}` | `{"live":true}` |
| 3 | `{}` | `{"ready":boolean}` |

These objects reject additional fields. `health_live` returns success with
`live:true` whenever the process can parse and answer a frame; otherwise no
application response exists. `health_ready` always returns an `ok:true`
envelope with only the Boolean result and never a reason.

**§32.4 data plane cuts (post HP-000):** active heap data ops are **105, 110,
111, 112, 114, 115, 116, 117, 120, 121, 122** (`collection_open`,
`list_collections`, `get`/`get_bytes`, `list_keys`, `scan_json`, **`find`**,
**`history`**, `put`/`put_bytes`, `delete`) with committed `rpc-v1` schemas and
fixtures. Find first cut scans the collection and evaluates Mongo-style filters
(`Filter::from_json`); history rebuilds from segment frames on SubjectV2 keys.
History **rights first-cut** uses Read (1) so bootstrap certs with mask `0x5`
admit; dedicated ReadHistory (2) may be reasserted when issuance grants it by
default. Activation of any further operation still requires the exact
table/schema amendment, dispatch exhaustiveness, fixtures, and public error
mapping in the same change.

High-impact confirmation values are:

```text
heap_retire: RETIRE <canonical HeapId>
heap_purge:  PURGE <canonical HeapId>
```

Confirmation is not a credential. It is checked in addition to the signed
right and immutable target identity.

## 33. Qualified network session

### 33.1 Transport profile

The qualified remote profile is:

```text
residiuum-rpc-v1 + heap-key-v1 + TLS 1.3
```

It disables:

- plaintext transport;
- TLS 1.2;
- diagnostic newline JSON;
- shared `token`;
- legacy principal/role authorization;
- multiple heaps on one logical connection.

The existing `u32` big-endian length-prefixed UTF-8 JSON handshake remains.
Application RPC frames remain JSON during v1. HeapKey certificate and proof
bytes use unpadded base64url in JSON fields.

### 33.2 Feature negotiation

Add required feature:

```text
heap-key-v1
```

When this feature is granted, the handshake sequence is exactly:

```text
TLS 1.3 established
client -> hello
server -> heap_challenge
client -> heap_auth
server -> welcome | reject
client <-> heap-bound RPC requests
```

`welcome` is not sent before HeapKey validation.

### 33.3 `heap_challenge`

JSON fields:

```json
{
  "v": 1,
  "msg": "heap_challenge",
  "deployment_id": "canonical-uuid",
  "audience": "residiuum:data:v1",
  "server_nonce_b64u": "unpadded-base64url-32-bytes",
  "heap_profile": 1,
  "protocol_major": 1,
  "protocol_minor": 0
}
```

The nonce is random, single-use, node-local, and retained for at most 60
seconds. The server derives the 32-byte RFC 9266 exporter from the exact TLS
connection using label `EXPORTER-Channel-Binding` and empty context.

### 33.4 `heap_auth`

JSON fields:

```json
{
  "v": 1,
  "msg": "heap_auth",
  "heap_id": "canonical-uuid",
  "certificate_b64u": "COSE bytes",
  "holder_proof_b64u": "COSE bytes",
  "expected_heap_name": "optional-human-check"
}
```

The server:

1. enforces encoded-size limits;
2. parses deterministic COSE/CBOR;
3. resolves `HeapSlot` by the certificate's `HeapId`;
4. verifies deployment, epoch, generation, issuer key, signature, time,
   audience, blacklist, policy, state, and lease;
5. derives and checks the TLS exporter;
6. verifies holder proof, consumes the connection nonce, and records the proof
   ID only in bounded audit evidence;
7. creates `HeapCap`;
8. checks `expected_heap_name` after authority succeeds;
9. sends `welcome`.

Every failure before step 9 returns the same `heap_unavailable` reject shape.
The server writes exactly one framed object and closes the connection:

```json
{
  "v": 1,
  "msg": "reject",
  "code": "heap_unavailable",
  "retryable": false
}
```

No optional field, message text, heap identifier, challenge detail, or
provider error is included.

### 33.5 Heap-bound `welcome`

Additional JSON fields:

```json
{
  "session_id": "32-lowercase-hex",
  "heap_id": "canonical-uuid",
  "authority_epoch": 1,
  "authority_generation": 1,
  "security_revision": 1,
  "capability_expires_at": 1700003600,
  "heap_profile": "residiuum-heap-v1"
}
```

`session_id` is a random `CapabilityId` display value and is never accepted as
authority on another connection.

### 33.6 Application request changes

Under `heap-key-v1`:

- `RpcRequest.token` MUST be absent;
- `RpcRequest.collection` is replaced internally by immutable
  `collection_id`; the compatibility SDK may send a name only to
  `collection_open`;
- data requests carry no caller-selected `HeapId`;
- the server obtains heap identity solely from the channel capability;
- `operation_id` dedup records obey §14.2 and are scoped by
  `(HeapId, operation_id)`;
- cursors, resume tokens, and prepared-query IDs include a MAC or signature
  over `DeploymentId`, `HeapId`, `AuthorityEpoch`, capability ID, operation,
  immutable subordinate IDs, expiry, and position;
- a response never includes a filesystem path or deployment-global count.

The qualified common request envelope is exactly:

```json
{
  "v": 1,
  "id": 42,
  "operation_id": "canonical-uuid-or-null",
  "op_id": 111,
  "collection_id": "canonical-uuid-or-null",
  "stream_id": null,
  "args": {}
}
```

Rules:

- `id` is a connection-local unsigned 64-bit correlation value;
- `operation_id` is required for every mutation and absent for a pure read;
- `op_id` is the numeric registry value; qualified dispatch does not accept an
  operation string;
- exactly one immutable subordinate ID is present when its operation schema
  requires it;
- `args` is an object validated against that operation's committed schema;
- no additional common-envelope field is accepted;
- a JSON integer outside the exact schema range, duplicate object key,
  non-finite number, invalid UTF-8, or trailing frame byte rejects the request.

The response envelope is exactly one of:

```json
{"v":1,"id":42,"ok":true,"result":{}}
{"v":1,"id":42,"ok":false,"error":{"code":"heap_unavailable","retryable":false}}
```

Operation schemas define `result`. Public errors contain only registered
lowercase snake-case codes and `retryable`; diagnostic text and internal
causes go only to heap-scoped audit. An authorization, hidden-object, or
hidden-heap failure uses `heap_unavailable`. An authenticated caller may
receive state-specific errors only after the operation's right has been
validated and the error cannot disclose another heap.

Add one metadata operation:

```text
collection_open(name) -> { collection_id, canonical_name }
```

It requires `Read` for lookup, or the corresponding administrative right for
creation. Subsequent requests use `collection_id`.

### 33.7 Connection pooling and retries

An SDK pool key is:

```text
(endpoint, DeploymentId, HeapId, AuthorityEpoch, certificate_fingerprint)
```

Connections for two heaps are never pooled together. Reconnect performs a new
challenge and holder proof. A request retry reuses its data `operation_id` but
never reuses a holder proof or server nonce.

### 33.8 Pre-authentication admission bounds

The qualified listener freezes these defaults and hard maxima:

| Resource | Default | Hard maximum |
|---|---:|---:|
| concurrent unauthenticated connections per process | 256 | 1,024 |
| concurrent unauthenticated connections per kernel-observed source address | 8 | 32 |
| complete framed `heap_auth` JSON bytes | 32,768 | 32,768 |
| challenges per connection | 1 | 1 |
| handshake lifetime | 15 seconds | 60 seconds |
| concurrent Ed25519 verification jobs | `min(2 × logical_cpu, 32)`, at least 2 | 32 |
| failed-handshake audit records per source per second | 1, aggregated | 1, aggregated |

A deployment may configure smaller values, never larger ones under v1.
Source-address accounting uses the authenticated kernel transport peer, not a
forwarded application header. A deployment behind a proxy either authenticates
and qualifies that proxy binding or applies only the global limit.

The server stores one 32-byte nonce and deadline in the connection state; it
does not maintain a deployment-wide accepted-proof database. Protocol state
permits exactly one `heap_auth`, consumes the nonce before emitting either
`welcome` or `reject`, and closes after failure. Reuse on another connection
fails because both nonce and TLS exporter differ. `proof_id` remains signed
identity and audit evidence, not a lookup key on the request path.

Limit exhaustion returns the ordinary bounded reject or closes before a
challenge, according to whether a framed response can be emitted without
additional allocation. It never performs heap lookup, signature verification,
or detailed denial logging after the applicable budget is exhausted.

## 34. Frozen durable-storage profile

This section is normative for `residiuum-heap-v1`. A different layout is permitted
only under a new profile name and after requalification.

### 34.1 One heap per segment

A qualified segment contains data for exactly one `HeapId`. It may contain many
collections and streams from that heap. It MUST NOT contain frames owned by a
second heap.

Every heap-aware segment has:

1. one existing kind-2 `SegmentDescriptor` as frame zero;
2. envelope keys 31 and 34 on that descriptor, binding `HeapId` and ownership
   profile;
3. the existing descriptor body binding store ID, segment ID, creation time,
   and safety limits exactly as defined by `residiuum-format`;
4. heap identity on every independently recoverable frame;
5. keys 31 and 34 on the final `SegmentSummary`, when present.

This preserves the existing invariant that `SegmentDescriptor` is frame zero;
`HeapDescriptor` is heap catalog/history evidence and is not substituted for
the segment-lifecycle descriptor.

`residiuum-format` adds the profile-neutral constructor:

```rust
pub fn create_with_descriptor_envelope(
    ids: SegmentId,
    limits: SafetyLimits,
    created_ns: u64,
    envelope: &[u8],
) -> Result<ActiveSegment, SegmentError>;
```

It validates the envelope with the existing deterministic-CBOR limits and
writes the unchanged draft descriptor body. `residiuum-store::kernel` is the only
qualified caller and supplies canonical `{31: heap_id_bytes, 34: 1}` after
deriving the bytes from its bound capability. This avoids a dependency cycle:
`residiuum-format` does not import `HeapId` or depend on `residiuum-heap`.

The qualified store cannot call the legacy empty-envelope constructor.
Migration remains the only qualified module permitted to decode legacy
empty-envelope segment descriptors.

An ordinary live-store reader accepts a data frame only when the intact
segment descriptor, frame envelope, and subject agree. Missing or disagreeing
ownership prevents ordinary mounting. Damage to one copy does not authorize
reassignment.

Evidence-preserving salvage is more granular: it may recover an independently
verified frame when that frame's envelope and subject agree on one `HeapId`
even if the segment descriptor is missing or damaged. It records lost segment
context. Two conflicting integrity-valid ownership claims yield
`OwnershipConflict`; the bytes remain in a conflict package and do not enter
ordinary current state.

This duplication is deliberate. A directory name, catalog, index, or intact
segment header is useful evidence, but no single one is the source of truth.

### 34.2 Reference filesystem layout

The reference server uses:

```text
<data_root>/
  meta/
    heap-catalog.v1.cbor
    heaps/<heap-id-hex>/
      descriptor-head
      descriptor-chain/<20-digit-sequence>-<descriptor-hash-hex>.frame
      collections.v1.cbor
      streams.v1.cbor
  active/<heap-id-hex>/<shard-id>.residiuum
  segments/<heap-id-hex>/<segment-id>.residiuum
  chunks/<heap-id-hex>/<chunk-id>.residiuum
  indexes/<heap-id-hex>/<collection-id-hex>/<index-id>.residiuum
  migration/<job-id>/state.v1.cbor
  quarantine/<reason>/<opaque-file-name>
```

Hex directory names are lowercase, unhyphenated 32-character UUID bytes.
Directories are placement hints, not authority. Moving a segment into another
heap's directory cannot change its owner and causes quarantine on discovery.

Catalogs are rebuildable accelerators. The authoritative mapping is the
integrity-valid descriptor history in heap-owned storage. A catalog entry MUST
contain a descriptor hash and MUST be ignored if the descriptor does not match.
`descriptor-chain` contains exact committed kind-10 frame bytes, not a second
encoding; each frame retains its envelope and subject ownership evidence.
`descriptor-head` is a rebuildable ASCII hash hint. This protected
administrative chain survives payload purge.

### 34.3 Allocated frame kinds and envelope fields

The following `residiuum-format` values are frozen:

| Value | Frame kind |
|---:|---|
| 10 | `HeapDescriptor` |
| 11 | `CollectionDescriptor` |
| 12 | `StreamDescriptor` |
| 13 | `HeapMigrationEvidence` |
| 14 | `EvidenceRecord` |
| 15 | `EvidenceCheckpoint` |
| 16 | `EvidenceRetentionCut` |

Envelope map keys 1–30 retain their current meanings. The following keys are
allocated:

| Key | Name | Encoding | Requirement |
|---:|---|---|---|
| 31 | `heap_id` | 16-byte byte string | every heap-aware frame |
| 32 | `collection_id` | 16-byte byte string | collection data and indexes |
| 33 | `stream_id` | 16-byte byte string | stream data and indexes |
| 34 | `ownership_profile` | unsigned integer | `1` for every heap-aware frame; `2` only for protected deployment evidence |
| 35 | `source_heap_id` | 16-byte byte string | import provenance only |
| 36 | `source_object_id` | byte string, 1–64 bytes | import provenance only |
| 37 | `deployment_id` | 16-byte byte string | deployment evidence only; forbidden in a Heap-owned frame |

Every envelope key unknown to profile v1 rejects the frame, except the reserved
Atomic namespace 37–40 which ownership parsers MUST ignore (FORMAT_SPEC §4.4 /
CR-ATM2-002). On Heap-owned frames those keys are Atomic linkage, not
`deployment_id`. `deployment_id` remains deployment-evidence-only and is
forbidden on Heap-owned frames. Writers emit map keys in numeric order. UUID
fields use raw RFC 4122 network-order bytes, never text.

`DeploymentId` is deliberately absent from ordinary data-frame ownership.
`HeapId` is durable data identity; deployment and authority epoch are serving
fences held by the authority plane. Restoring the same heap into a replacement
deployment therefore does not require rewriting every surviving data frame.

### 34.4 Subject version 2

The current name-based subject version 1 remains readable only by migration
code. All new writes use:

```text
offset  size  field
0       1     version = 0x02
1       16    HeapId
17      1     object kind: 0x00 heap metadata, 0x01 collection, 0x02 stream
18      16    all zero for heap metadata; otherwise CollectionId or StreamId
34      2     key length, unsigned big-endian
36      N     key bytes
```

`N` is 0–2048 and the subject length MUST equal `36 + N`; trailing bytes are
invalid. For collection/stream kinds, the object ID MUST match envelope key 32
or 33 and the kind MUST match the frame kind. Heap metadata has neither key 32
nor 33, requires the all-zero object ID, and permits exactly:

```text
HeapDescriptor:       key = 0x01 || descriptor_sequence_u64_be
HeapMigrationEvidence key = 0x02 || migration_job_uuid_bytes
EvidenceRecord:       key = 0x03 || evidence_sequence_u64_be
EvidenceCheckpoint:   key = 0x04 || checkpoint_end_sequence_u64_be
EvidenceRetentionCut: key = 0x05 || first_retained_sequence_u64_be
```

No other heap-metadata subtype is valid in v1. Deployment-owned evidence uses
the SubjectV3 profile defined only by `EVIDENCE_LEDGER_SPEC.md` and is rejected
by `HeapStore`. Segment lifecycle descriptor
and summary frames retain their existing subjects; their heap binding is
envelope key 31 plus segment context, and they never carry application data.
For kind-11 `CollectionDescriptor` and kind-12 `StreamDescriptor`, key bytes
are exactly `0x00 || descriptor_sequence_u64_be`; frame kind distinguishes
this metadata subject from an ordinary item with the same application key.
Decoders return a borrowed `SubjectV2`; they do not normalize or reinterpret
application key bytes.

### 34.5 Descriptor schemas

Descriptors are deterministic CBOR maps embedded as frame bodies.

`HeapDescriptor`:

| Key | Field | Encoding |
|---:|---|---|
| 1 | version | `1` |
| 2 | origin deployment ID | bstr(16) |
| 3 | heap ID | bstr(16) |
| 4 | creation event ID | bstr(16) |
| 5 | created at | Unix seconds |
| 6 | profile | `"residiuum-heap-v1"` |
| 7 | predecessor descriptor hash | bstr(32) or null |
| 8 | descriptor sequence | uint, starting at 1 |
| 9 | state | 1 active, 2 read-only, 3 suspended, 4 retired, 5 purging, 6 purged |
| 10 | canonical heap name | UTF-8 text |
| 11 | aliases | sorted UTF-8 array |

`origin deployment ID` is immutable creation/migration provenance. It is not
the current serving `DeploymentId`, is not consulted for frame ownership or
network authorization, and is preserved by same-identity takeover. Current
deployment and epoch live only in verified authority state.

Heap names and aliases obey §5. Sequence 1 has a null predecessor, no aliases,
and its §34.7 hash equals authority-root label 18 and authority-head label 25.
Rename appends a descriptor with unchanged labels 1–6 and 9, the prior
descriptor hash in label 7, incremented sequence, the new name in label 10,
and prior names retained as aliases subject to quarantine policy. Recovery
chooses only the unique chain from authority-head label 25 to the exact tip in
label 26; an equal-sequence fork or missing committed tip is
`OwnershipConflict`.

Label 11 contains only aliases that currently resolve. Earlier descriptors
preserve complete name history without keeping every old name live forever.
After §5.5 quarantine expires, releasing an alias appends another descriptor
that removes it; name reuse cannot occur before that descriptor is durable and
the heap-name index is rebuilt from it. Administrative state changes likewise
append a descriptor with the new label 9 and unchanged identity/name fields.

`CollectionDescriptor` and `StreamDescriptor` use keys 1–8 above with keys
3–4 replaced by heap ID and immutable object ID, then:

| Key | Field |
|---:|---|
| 9 | canonical UTF-8 name |
| 10 | aliases, sorted UTF-8 array |
| 11 | state: 1 active, 2 retired |
| 12 | object-specific options map |

Names are NFC, 1–255 UTF-8 bytes, contain no NUL or control characters, and
are unique within one heap under exact byte comparison. Rename appends a new
descriptor; it never changes the immutable ID. An alias collision is a
transaction conflict. A descriptor body is at most 65,536 encoded bytes,
contains exactly the registered fields, and rejects an unknown option key.
V1 defines no object-specific descriptor option, so key 12 is an empty map.
Aliases obey the same name rules, are unique, and number at most 64.

For each collection or stream ID, ordinary recovery accepts only one
contiguous committed descriptor chain beginning at sequence 1 with a null
predecessor. Each successor increments sequence by one and names the exact
predecessor hash. Two integrity-valid successors of one predecessor,
conflicting sequence-1 descriptors, or an object ID appearing under two heaps
is `OwnershipConflict`; recovery never chooses the longer or newer-looking
branch. If later descriptors are physically missing, the unique surviving
contiguous tip is usable, subject to the global name/alias uniqueness check.
Any resulting name ambiguity keeps the affected heaps unavailable rather than
guessing which missing update was newer.

### 34.6 Write and recovery rules

A write is visible only after:

1. the body and envelope have been encoded;
2. all ownership copies agree;
3. the frame checksum/authenticator is complete;
4. the segment commit marker is durable according to the requested durability;
5. any catalog update is either durable or known to be rebuildable.

Ordinary recovery first establishes an intact one-heap segment descriptor and
then admits matching frames. Evidence-preserving salvage scans frames
independently and applies §34.1's per-frame ownership rule, so loss of frame
zero does not discard later healthy frames whose envelope and subject still
agree. A damaged frame is skipped or quarantined according to the existing
damage policy; it cannot cause later valid frames to inherit a different heap.
Index rebuilding is performed through `RecoveryStore` with an explicit
`HeapId` and emits output only into that heap's paths.

### 34.7 Storage digest registry

Security-object fingerprints and authority-chain hashes use SHA-256 exactly as
stated in §§31 and 35. Storage content identities use BLAKE3-256 with the
following ASCII domain, one zero byte, then the exact input:

| Name | Domain | Input |
|---|---|---|
| descriptor hash | `RESIDIUUM-HEAP-DESCRIPTOR-V1` | canonical descriptor body |
| migration inventory hash | `RESIDIUUM-HEAP-MIGRATION-INVENTORY-V1` | canonical ordered inventory |
| assignment-map hash | `RESIDIUUM-HEAP-MIGRATION-ASSIGNMENTS-V1` | canonical assignment map |
| rewritten-segment hash | `RESIDIUUM-HEAP-MIGRATION-SEGMENT-V1` | complete committed segment bytes |

“Canonical inventory” sorts entries by raw source `SegmentId` and records
segment ID, byte length, and BLAKE3-256 of the complete source bytes. Duplicate
segment IDs are a preflight conflict. Filesystem paths are diagnostic only and
are excluded from identity, so non-UTF-8 or platform-specific paths do not
change the inventory hash or prevent salvage. No field called `hash` or
`descriptor hash` may silently choose another algorithm. Frame CRC/checksum
fields retain their existing `residiuum-format` algorithms and are not object
identity.

## 35. Authority storage and key-provider contract

Authority state is not ordinary heap data and is not restored from a data
backup. In the qualified server profile it lives under a separately configured
`authority_root`, preferably a separately protected volume or service:

```text
<authority_root>/<deployment-id-hex>/<heap-id-hex>/
  head.a.cbor
  head.b.cbor
  current
  time-floor.a.cbor
  time-floor.b.cbor
  time-current
  events/<20-digit-epoch>/<20-digit-revision>.cbor
  receipts/<20-digit-epoch>/<operation-id>.cbor
```

The server refuses to start the qualified profile when `authority_root` is
absent, resolves inside `data_root`, is group/world writable, or any path
component is a symbolic link. Embedded local use may colocate roots but MUST
report `rollback_resistance=false` and is not qualified as server-secure.

The reference qualified filesystem implementation is Unix-only: directories
are mode 0700, files are mode 0600, ownership equals the configured server
UID, and all traversal/mutation is directory-descriptor-relative with
no-follow semantics. The data server holds an exclusive `serving.lock` for its
deployment from verification through shutdown, preventing a second serving
process. A separate per-heap `mutation.lock` serializes all authority-chain
commits. `residiuum-authority` holds it exclusively for a local master-authority
mutation; the server holds it exclusively only inside
`commit_operational`, and shared while loading a candidate snapshot or
checking time. The lock is never held while waiting for an admitted data
operation.

When `residiuum-authority` observes no running server, it MUST also acquire
`serving.lock` exclusively and retain both locks through the commit and
selector publication. This closes the race in which a server could start
after the tool skipped the barrier but before the new head became durable. If
the server holds `serving.lock`, the tool uses the barrier protocol and never
waits for `mutation.lock` until `begin_security_barrier` has drained old-head
operational commits. Lock acquisition order is therefore: `serving.lock`
probe or barrier, then `mutation.lock`; no code takes them in reverse order.
The probe is a non-blocking exclusive lock attempt. If it reports contention
but the control endpoint cannot complete `begin`, the tool retries the probe;
it never guesses whether the server is alive and never commits in the
ambiguous state.

Permission, owner, link-count, mount, and file type are rechecked on every
reopen; path strings are never reopened after validation. A Windows
implementation requires a separately frozen ACL and handle-opening profile
before qualification.

### 35.1 Authority head

Every head and time-floor slot file is deterministic CBOR:

```text
{
  1: payload_bstr,
  2: SHA-256(payload_bstr)
}
```

`current` and `time-current` contain exactly ASCII `a\n` or `b\n`; any other
bytes are invalid hints. A time-floor payload is:

| Key | Field |
|---:|---|
| 1 | profile version, uint = 1 |
| 2 | deployment ID, bstr(16) |
| 3 | heap ID, bstr(16) |
| 4 | trusted Unix-seconds floor, uint |
| 5 | file sequence, strictly increasing uint |
| 6 | monotonic-clock sample in nanoseconds, uint |
| 7 | wall-clock sample in Unix nanoseconds, signed integer |
| 8 | maximum uncertainty in nanoseconds, uint |
| 9 | provider reset/restart counters, array(2) of uint |

Fields 6–9 are evidence, not permission to move label 4 backward. A provider
without such counters writes `[0, 0]` in label 9. The head's label 16,
selected time-floor label 4, and `AnchorValue` floor must agree exactly.

The head payload is:

| Key | Field |
|---:|---|
| 1 | profile version = 1 |
| 2 | `DeploymentId` bstr(16) |
| 3 | `HeapId` bstr(16) |
| 4 | authority epoch |
| 5 | security revision |
| 6 | authority revision |
| 7 | state revision |
| 8 | policy revision |
| 9 | heap state: 1 active, 2 read-only, 3 suspended, 4 retired, 5 purging, 6 purged |
| 10 | current master generation |
| 11 | current Ed25519 public key bstr(32) |
| 12 | previous generation or null |
| 13 | previous public key bstr(32) or null |
| 14 | previous-generation grace deadline or null |
| 15 | blacklist entries in canonical encoded-byte order |
| 16 | trusted time floor, Unix seconds |
| 17 | authority-chain head hash bstr(32) |
| 18 | recovery profile: 1 no-master-recovery, 2 threshold-master-recovery |
| 19 | recovery public keys, lexicographically sorted bstr(32) array |
| 20 | recovery threshold |
| 21 | tombstone: 0 none, 1 retired, 2 purged |
| 22 | file sequence, strictly increasing |
| 23 | access policy, canonical map below |
| 24 | remembered resume state: 1 active, 2 read-only, or null |
| 25 | immutable storage genesis descriptor hash, BLAKE3-256 bstr(32) |
| 26 | current heap-descriptor hash, BLAKE3-256 bstr(32) |
| 27 | active EvidencePolicy root, BLAKE3-256 bstr(32), or null before DEL activation |
| 28 | active evidence-signer certificate hash, BLAKE3-256 bstr(32), or null before DEL activation |

The access-policy map is:

```text
{
  1: allowed_rights_mask,
  2: constraints,
  3: policy_profile_version
}
```

`allowed_rights_mask` contains only §32.1 bits, `constraints` is the canonical
§32.2 array, and `policy_profile_version` is exactly 1. The empty/default
policy is `{1: 0x1ffff, 2: [], 3: 1}`. It means “apply no additional
narrowing”; mandatory deployment controls still apply. Missing policy is
corruption, never interpreted as the default.

Head validation additionally requires:

- previous generation, previous key, and grace deadline are either all null or
  all present;
- a present previous generation is exactly current generation minus one;
- blacklist entries refer only to that previous generation;
- remembered resume state is non-null exactly while state is suspended;
- tombstone is retired for retired/purging and purged for purged, and is never
  cleared once set;
- no-master-recovery has an empty recovery-key array and threshold zero;
- threshold-master-recovery has 2–16 distinct strict Ed25519 public keys and a
  threshold from 2 through the number of keys;
- time floor equals the separately anchored time-floor value;
- each revision is non-zero and matches the transition implied by the last
  authority event;
- label 25 equals root-event label 18 in every epoch and never changes;
- label 26 equals root-event label 19 at an epoch root; creation requires
  labels 18 and 19 to match, and within the epoch label 26 changes only
  through a valid §31.5.1 event whose label 16 equals the new value.
- labels 27 and 28 are both null before Residiuum Evidence Ledger activation and
  both non-null afterward; activation and every later change is a valid
  authority event atomically bound to the corresponding ledger evidence.

Except for the terminal failed-creation case in §31.5 kind 4, the exact
descriptor frame named by label 26 MUST exist and validate before readiness.
Kind 4 is never ready and never creates a heap slot.

Failure of any condition is `AuthorityCorrupt` and prevents readiness.

Each `events/<epoch>/<revision>.cbor` file is:

```text
{
  1: 1,
  2: event_kind,
  3: event_body_bstr,
  4: previous_event_hash
}
```

Event kind 1 is a §31.4.1 authority-root event, 2 is a §31.4
authority-transition COSE object, 3 is a §31.5 master-signed mutation COSE
object, and 4 is a §31.5.1 operational event. Values 5–255 are reserved.
`previous_event_hash` is bstr(32), all zero only for the immutable creation
event. The embedded previous-head field MUST equal label 4. The event hash is
SHA-256 of the complete canonical event-file bytes. The path contains the
zero-padded epoch and authority revision. Revisions are contiguous within an
epoch; gaps, duplicate revisions, a missing preceding epoch root, or a body
kind mismatch fail closed.

Each mutation:

0. for a descriptor-bearing operational event, writes and syncs the exact
   descriptor frame in a non-discoverable staging path and verifies its hash
   equals event label 16;
1. writes and syncs the new event under a temporary name;
2. atomically publishes and directory-syncs the event;
3. writes and syncs the inactive head slot referring to that event;
4. advances and syncs `AuthorityAnchor` to the new head hash;
5. atomically replaces and directory-syncs the `current` selector.

After step 5, a descriptor-bearing mutation atomically publishes only the
staged descriptor whose hash equals head label 26, then updates the rebuildable
catalog. The authority selector is the logical commit point. A crash before it
leaves the descriptor invisible; a crash after it causes startup to finish
publishing the exact staged bytes before readiness. Missing or different staged
bytes fail closed. The mutation receipt is written only after publication, but
dedup replay derives the already-committed outcome from the event and head.

On startup the selector is a hint. The implementation validates the anchor,
both slots, and the append-only event chain, then selects the one head whose
hash equals the anchor. An event beyond the anchored head is an uncommitted
orphan and is quarantined. The other slot may be the direct predecessor or one
unanchored direct successor. No anchored match, a skipped sequence, or a chain
fork fails closed. Equal sequences with unequal payloads are `AuthorityFork`
and fail closed.

The time floor uses the same two-slot algorithm and is monotonically
nondecreasing. A clock below the stored floor suspends network authorization
until recovery procedure advances or validates time; it never rolls the floor
back.

### 35.2 Storage interface

`residiuum-heap` exposes the following split interfaces. The split is normative:
the data-service dependency graph may contain `AuthorityReader`,
`OperationalAuthorityWriter`, `AuthorityCheckpointWriter`, and
the opaque implementations of those traits. `AuthorityAnchor`,
`MasterAuthorityStore`, and their mutation types are private to the authority
storage implementation and `residiuum-authority`; none is callable from the data
service.

```rust
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct AuthorityVersion {
    pub epoch: AuthorityEpoch,
    pub security_revision: SecurityRevision,
    pub authority_revision: u64,
    pub file_sequence: u64,
    pub chain_head_hash: [u8; 32],
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct AnchorValue {
    pub monotonic_counter: u64,
    pub head_hash: [u8; 32],
    pub security_time_floor: u64,
    pub provider_reset_counter: u64,
    pub provider_restart_counter: u64,
}

pub struct TimeCheckpoint {
    pub floor_unix_s: u64,
    pub monotonic_ns: u64,
    pub wall_unix_ns: i64,
    pub uncertainty_ns: u64,
    pub provider_counters: [u64; 2],
}

pub struct AuthorizedOperationalEvent {
    /* private fields; constructed only by residiuum-heap authorization */
}

pub struct OperationalCommit {
    pub version: AuthorityVersion,
    pub receipt: OperationalReceipt,
}

pub trait AuthorityReader: Send + Sync {
    fn load(&self, heap: HeapId) -> Result<AuthorityHead, AuthorityError>;
}

pub trait OperationalAuthorityWriter: Send + Sync {
    fn commit_operational(
        &self,
        event: &AuthorizedOperationalEvent,
    ) -> Result<OperationalCommit, AuthorityError>;
}

pub trait AuthorityCheckpointWriter: Send + Sync {
    fn commit_time_checkpoint(
        &self,
        heap: HeapId,
        expected: AuthorityVersion,
        next: &TimeCheckpoint,
    ) -> Result<AuthorityVersion, AuthorityError>;
}

pub trait MasterKeyProvider: Send + Sync {
    fn generate(&self, heap: HeapId, generation: u64)
        -> Result<MasterPublicKey, KeyProviderError>;
    fn sign_certificate(
        &self,
        heap: HeapId,
        generation: u64,
        certificate_sig_structure: &[u8],
    ) -> Result<Ed25519Signature, KeyProviderError>;
    fn sign_authority_transition(
        &self,
        heap: HeapId,
        generation: u64,
        transition_sig_structure: &[u8],
    ) -> Result<Ed25519Signature, KeyProviderError>;
    fn sign_authority_mutation(
        &self,
        heap: HeapId,
        generation: u64,
        mutation_sig_structure: &[u8],
    ) -> Result<Ed25519Signature, KeyProviderError>;
    fn sign_authority_root(
        &self,
        heap: HeapId,
        generation: u64,
        root_signing_message: &[u8],
    ) -> Result<Ed25519Signature, KeyProviderError>;
    fn prove_new_master_possession(
        &self,
        heap: HeapId,
        generation: u64,
        possession_message: &[u8],
    ) -> Result<Ed25519Signature, KeyProviderError>;
    fn destroy(&self, heap: HeapId, generation: u64)
        -> Result<DestructionReceipt, KeyProviderError>;
}
```

The provider verifies that the supplied generation is the registered key for
the heap and purpose—the current key for an established epoch or the prepared
generation-1 key during creation/recovery—and that each byte string is a
canonical, correctly domain-separated structure of the named type before
signing. It refuses arbitrary-message signing. The first three signing inputs
are complete COSE `Sig_structure` values; the root input is the exact §31.4.1
domain-separated message, and possession input is one of the exact
domain-separated new-master messages in §31.4 or §31.4.1. The caller
independently verifies every returned signature against the pinned public key
before committing it.

`AnchorValue` contains a monotonic counter and the accepted authority-head
hash and security-time floor. Loading authority succeeds only when the selected
valid disk head and time-floor slot match the anchor. A mutation persists the
candidate disk head, advances the anchor, then publishes the selector. A crash
after anchor advance but before selector publication is recovered by selecting
the matching valid slot. The nonmatching slot may be the immediately preceding
valid value or one fully formed but unanchored successor; it is ignored and
later overwritten. No matching slot, two different matching slots, a skipped
sequence, or a fork from the anchored chain fails closed.

A provider without independently trusted hardware time uses periodic
checkpoints: it writes and syncs the inactive time-floor slot and an inactive
head with only labels 16 and 22 advanced, calls
`AuthorityAnchor::advance_time_floor`, then publishes both selectors. It does
not append an authority event or increment security, authority, state, or
policy revision. The anchor operation is atomic: failure leaves the earlier
floor authoritative and the heap includes the checkpoint interval in its
clock uncertainty. A provider unable to sustain the §8.7 checkpoint rate
cannot qualify for this profile. A reviewed hardware-time provider may instead
use §8.7's stronger bound and checkpoints only on its documented persistence
events; its reset/restart counters are part of `AnchorValue`.

Private master material is non-exportable in the server-secure profile.
`generate` returns only the public key. File-backed development keys are
permitted only under the visibly named `development-file-key-provider` and
zeroize secret buffers. Production adapters may use an OS keystore, TPM, HSM,
or remote signer, but must pass the same conformance suite.

Master rotation is a local-only `residiuum-authority` command. The data-service
listener has no operation ID, parser branch, crate dependency, concrete
provider, or trait object capable of invoking `MasterKeyProvider`.

For a security-barrier mutation, `residiuum-authority` first completes
`begin_security_barrier`, commits against the returned current head, and then
sends `apply_committed_head` as defined in §8.9. The server validates peer
credentials, rereads the anchored state, and acknowledges only the exact
applied head hash.

If the server was running but does not acknowledge within five seconds, the
authority mutation remains durably committed, the heap remains unavailable,
and the command reports `committed_not_observed`; it never rolls authority
backward or resumes the old snapshot. If no server held `serving.lock` when
the command began, no barrier exchange is required. Startup loads the committed
head before readiness.

## 36. Migration from raw storage

Migration is an explicit, crash-resumable job; opening a legacy store never
silently upgrades it.

### 36.1 Required phases

| Phase | Name | Completion condition |
|---:|---|---|
| 0 | preflight | exclusive migration lease, immutable source inventory, verified backup |
| 1 | establish | implicit compatibility heap and authority head created |
| 2 | identify | immutable IDs assigned to every collection/stream |
| 3 | dual-read | legacy v1 readable; every new write is v2 and heap-labelled |
| 4 | rewrite | each legacy segment rewritten into single-heap segments |
| 5 | verify | all source frames accounted for by content/event hash |
| 6 | cut over | v2 catalogs published; raw/global APIs disabled |
| 7 | quarantine | legacy files made read-only and moved from active discovery |

The job state records:

```rust
pub struct MigrationStateV1 {
    pub job_id: OperationId,
    pub source_store_id: StoreId,
    pub deployment_id: DeploymentId,
    pub destination_heap_id: HeapId,
    pub phase: u8,
    pub source_inventory_hash: [u8; 32],
    pub next_segment: Option<SegmentId>,
    pub completed_segments: Vec<(SegmentId, [u8; 32])>,
    pub assigned_objects_hash: [u8; 32],
    pub rewritten_frames: u64,
    pub quarantined_frames: u64,
    pub started_at: i64,
    pub updated_at: i64,
}
```

State is updated atomically after each segment. Object assignments are stored
once in `migration/<job-id>/assigned-objects.v1.cbor` as canonical source
identity to freshly generated UUIDv4 identity pairs; its hash is
`assigned_objects_hash`. Replaying a completed segment reads that immutable
mapping and must produce the same destination IDs and hashes. A missing,
changed, duplicate, or colliding assignment is a fatal verification error,
not regenerated or overwritten data.

Legacy subject-v1 data has no cryptographic heap label and is therefore
`Unknown` until the operator binds the whole source inventory to one
destination heap. A migration cannot split one legacy collection across heaps.