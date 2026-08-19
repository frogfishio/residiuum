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
| `coordinator.ack` | acknowledged coordinator length after the last `sync_all` |
| `shard-XXXXXXXX.log` | concatenated `ItemEvent` member frames |
| `shard-XXXXXXXX.ack` | acknowledged shard-log length after the last `sync_all` |
| `plan/<atomic_id>` | closed plan + bound frontier (`ATMPLAN1`); prepare is derived from this |
| `intent/<atomic_id>` | length-prefixed frozen `AtomicMember` records |
| `payload/<atomic_id>-<ordinal>` | staged value bytes |
| `sealed/<atomic_id>` | versioned checksummed boundary (`R2SEAL1`) bound to Heap, Atomic ID, content root, manifest root, member count |
| `checkpoint` | v2 checksummed summaries, prefix hashes, and ack tails (CR-ATMR4-002/003) |
| `writer.lock` | exclusive physical writer (`flock` + in-process table) |

## Sync / first stable boundary

Write order: closed-plan sidecar + `sync_all` → intent file + `sync_all` →
prepare frame append + `sync_all` (`AfterPrepare`) → payload file +
`sync_all` → member frame append + `sync_all` (`AfterMember(n)`).
The prepare body is `prepare_from_closed_plan`, not synthetic empty roots.
A completed chunked member appends the same shard `ItemEvent` as an unchunked
member before seal is allowed (CR-ATMR4-004).
The member slice must equal the closed plan mutations; leftover members are
refused before any sidecar or log append (CR-ATMR4-001). Exclusive sidecars
publish through a unique temp file and a no-replace link; a torn leftover is
quarantined so exact same-ID retry is not an identity conflict (CR-ATMR4-006).
Recovery size-checks every sidecar role from metadata before `read` and charges
those bytes to `max_sidecar_bytes` (CR-ATMR4-007).

`seal_member_boundary` `sync_all`s `coordinator.log` and every shard log, then
creates `sealed/<atomic_id>` and `sync_all`s that file and its directory. That
marker plus the preceding log syncs is the first durable-invisible boundary.

Reopen authenticates the plan sidecar, recomputes the prepare from that plan,
and refuses if the coordinator frame is not byte-identical. Scan holes inside
the acknowledged log prefix are coverage damage, even when no later frame
verifies. A torn tail past the last `.ack` is ignored and does not invent a
prepare. It also checks intent members against the prepare's ordered manifest
root, envelope linkage against decoded bodies, payload hashes, shard
placement, and the checksummed seal. Placeholder / synthetic prepare roots are
refused.
Ordinary `get` / `scan` never observe staged members. RQL, history, watch, and
secondary indexes are not implemented here.

Crash prefixes (`before_prepare`, `after_prepare`, `after_member_n`) are
directory-image reopens, not in-memory clones.