# residiuum-atomic-lane

Durable per-Heap Atomic coordinator stream and staged-member append lane
(CR-ATM2-001).

This is a **peer crate** (Law 9). It is not `residiuum-store`, `residiuum-sdk`,
or `residiuum-perf`. The in-memory kernel in `residiuum-atomics` remains the
reference model. `Capabilities::atomics` stays false.

## Authoritative files

| Path | Role |
| --- | --- |
| `heap.id` | 16-byte Heap identity |
| `meta` | `shard_count=` |
| `coordinator.log` | concatenated `BatchPrepare` frames |
| `shard-XXXXXXXX.log` | concatenated `ItemEvent` member frames |
| `plan/<atomic_id>` | closed plan + bound frontier (`ATMPLAN1`); prepare is derived from this |
| `intent/<atomic_id>` | length-prefixed frozen `AtomicMember` records |
| `payload/<atomic_id>-<ordinal>` | staged value bytes |
| `sealed/<atomic_id>` | versioned checksummed boundary (`R2SEAL1`) bound to Heap, Atomic ID, content root, manifest root, member count |

## Sync / first stable boundary

Write order: closed-plan sidecar + `sync_all` → intent file + `sync_all` →
prepare frame append + `sync_all` (`AfterPrepare`) → payload file +
`sync_all` → member frame append + `sync_all` (`AfterMember(n)`).
The prepare body is `prepare_from_closed_plan`, not synthetic empty roots.

`seal_member_boundary` `sync_all`s `coordinator.log` and every shard log, then
creates `sealed/<atomic_id>` and `sync_all`s that file and its directory. That
marker plus the preceding log syncs is the first durable-invisible boundary.

Reopen authenticates the plan sidecar, recomputes the prepare from that plan,
and refuses if the coordinator frame is not byte-identical. It also checks
intent members against the prepare's ordered manifest root, envelope linkage
against decoded bodies, payload hashes, shard placement, and the checksummed
seal. Placeholder / synthetic prepare roots are refused.
Ordinary `get` / `scan` never observe staged members. RQL, history, watch, and
secondary indexes are not implemented here.

Crash prefixes (`before_prepare`, `after_prepare`, `after_member_n`) are
directory-image reopens, not in-memory clones.