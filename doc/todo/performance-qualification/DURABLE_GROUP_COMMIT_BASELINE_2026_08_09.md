# Durable group commit baseline — 2026-08-09

Status: **10 GiB correctness PASS; single-boundary gathered-WAL and asynchronous smart-client mutation submission implemented and locally verified**

This note records the product write-path baseline created after the Bonzo run
showed approximately 5,500 durable writes/s and an implausibly high stable
boundary count. It does not replace PQH evidence and makes no throughput claim.

## Outcome

The smart embedded client now sends operation-bearing collection puts through
one deployment-wide commit coordinator. Concurrent logical writes retain their
own operation identity, condition, error and receipt, but share:

1. one physical-store ownership interval;
2. one gathered active-media write per touched active shard; and
3. one durable active-media boundary per touched active shard.

The coordinator is shared across every capability-bound Heap on the physical
connection. A connection therefore remains a connection, not a Heap/database.

Default cohort bounds are 1,024 operations, 16 MiB of subject/body bytes and a
250 microsecond maximum collection delay. The delay is a fixed deadline: a new
arrival does not restart it or release the cohort early.

## Acknowledgement contract

A successful smart-client receipt still means that the individual mutation is
durable. Group commit changes who shares the stable-storage work; it does not
turn a durable acknowledgement into a buffered acknowledgement.

For each new successful operation:

- the authoritative item-event contains the operation id and canonical request
  hash;
- all successful cohort frames cross the active-media durable boundary before
  any successful outcome is returned;
- the fixed-size checksummed outcome records are a derived lookup and do not
  create a second acknowledgement boundary;
- each receipt is upgraded to `Durable` only after the media boundary succeeds;
- request-level condition failures remain individual failures; and
- an infrastructure failure fails the cohort and prevents a clean-session
  certificate.

Exact retries return the original event id. Reuse of an operation id with a
different canonical request fails with `OperationIdentityConflict`.

## Recovery and normal-path cost

The previous smart operation path scanned authoritative segment media for every
previously unseen operation id. That made an ordinary write proportional to the
entire database size.

The new path uses a clean/dirty writer-session certificate:

- writer open marks the session dirty before returning to the application;
- orderly close marks it clean only when no failed boundary or unresolved
  reconciliation exists;
- the clean certificate records the outcome-journal length;
- open validates that length and the complete checksummed journal prefix; and
- a dirty, missing, truncated or corrupt journal triggers one authoritative
  reconciliation before the first operation-bearing mutation.

Reconciliation writes a complete checkpoint and replaces the journal with a
complete checksummed image. Consequently, normal clean sessions perform O(1)
in-memory outcome lookup and do not scan segment media per write. A crash in the
segment-durable/journal-durable window remains recoverable from the
authoritative item-event envelope.

## Executable evidence

The focused suites establish:

- two successful logical puts, including an 80 KiB chunked value, share exactly
  one active-media `FileSync` boundary;
- duplicate operation ids inside one cohort share the owner's receipt;
- client concurrency produces fewer cohorts than submitted durable operations;
- put and delete retry recover the original receipt after the
  append-before-outcome-journal crash window;
- a conflicting retry remains rejected after recovery;
- a same-length checksum-corrupt journal cannot be hidden by a clean marker;
- torn journal tails replay only their complete prefix; and
- compaction materialises outcome evidence before reclaiming source frames.

The client exposes redacted `operation_commits` counters through `inspect()`:
submitted, cohorts, committed, deduplicated, failed, maximum cohort entries and
maximum cohort bytes.

## Superseded baseline residuals

The 10 GiB result below predates the gathered-WAL correction. At that baseline:

1. a cohort performed an individual active-tail and Shadow write call per
   logical document; and
2. the authoritative media and outcome journal required two stable boundaries
   per cohort.
3. Superseded: operation-bearing puts and deletes now share the same
   coordinator and mixed cohorts retain individual receipts.
4. Secondary-index stale markers are updated after individual completions and
   are not yet coalesced per collection/cohort.
5. The R400 fixture loader's legacy synchronous writes do not exercise this
   smart-client path. Performance runs must identify the API path explicitly.
6. Cohort size/latency tuning and sustained performance on Bonzo remain
   unqualified. Debug numbers and a single machine are diagnostic only.

## Next measurement gate

Before another peer comparison, run the same smart-client durable workload with
the new counters and boundary probe enabled. The report must include logical
operations, cohorts, media syncs, journal syncs, maximum/mean cohort size,
payload bytes, acknowledgement latency, drain/close time and reopen validation.
Only then should the next engineering slice choose between gathered frame I/O,
outcome-boundary folding, delete integration or adaptive cohort tuning.

The first retained-media gate is intentionally 10 GiB, not 100 GiB. On a
dedicated empty Bonzo path it is launched with:

```bash
scripts/run-smart-durable-10g.sh /Users/rumpel/residiuum-campaigns/smart-durable-10g-20260809
```

The release-mode campaign uses 8 KiB deterministic pre-generated JSON payloads,
20 smart-client workers, a 30 GiB free-space floor, separate open/ack/close
timings, group-commit boundary counters, retained physical-size measurement,
then a fresh reopen and complete bounded-page scan of every record. It retains
the store, `report.json`, and a sibling `.campaign.log` for review.

## 10 GiB retained-media result

Bonzo completed the campaign at commit
`a1ce47c8c1af0b5a857b6939cb148909dc6d6d62`:

- 1,310,720 / 1,310,720 durable acknowledgements; zero failures;
- exact 10 GiB payload recovered by a fresh reopen and complete scan;
- 1,451.63 operations/s and 11.34 MiB/s acknowledged payload;
- p50 / p95 / p99 acknowledgement latency 9.90 / 38.86 / 60.97 ms;
- 71,443 cohorts, averaging 18.35 operations (maximum 20);
- 71,443 active-media syncs **and** 71,443 outcome-journal syncs;
- 1.80 s reopen and 127.41 s complete validation scan; and
- 22,945,603,584 allocated bytes for 10,737,418,240 payload bytes (2.14x).

The retained-media breakdown was approximately 10 GiB authoritative segments,
10 GiB Recovery Shadow, 381 MiB indexes and 208 MiB store metadata. Process
monitoring observed about 5,500 filesystem write operations/s while the
application acknowledged only 1,451.63 logical writes/s. This is physical-I/O
amplification, not database throughput.

## Corrected acknowledgement architecture

The checksummed append-only active segment is the write-ahead record for an
operation-bearing mutation. Its item-event envelope already contains the
operation id and canonical content hash required to reconstruct the exact
outcome after process loss. Consequently, durable acknowledgement requires one
authoritative boundary, not a second stable boundary for a derived lookup file.

The corrected contract is:

1. reserve/cook cohort frames and append them to the active segment;
2. write the gathered active tail and cross one active-media sync per touched
   active shard;
3. append outcome records to the derived dedup journal without another fsync;
4. publish receipts and acknowledge the cohort;
5. on orderly shutdown, sync that journal before publishing its clean-length
   certificate; and
6. after an unclean shutdown, reconstruct any missing/torn derived outcomes
   from the authoritative item-event frames.

Recovery Shadow remains a separate P★ salvage promise. The specification has
always stated `ack != P★`, so it must not introduce a second stable boundary
for acknowledgement. The current RSHD0004 stream receives the same gathered
tail as the authoritative active segment: one Shadow write per cohort/touched
shard, not one per record, and it becomes independently durable at protected
seal publication. Moving Shadow construction entirely behind bounded seal
backpressure remains a possible later experiment, not a prerequisite for this
correction. Its protected frontier may advance only after the Shadow artifact
is independently durable.

This follows the relevant WiredTiger separation: synchronous commit forces the
grouped WAL, not WAL plus data pages plus a second full-copy recovery image.
The 50 ms WiredTiger timer drains a partially filled asynchronous log slot; it
is not justification for adding acknowledgement delay. Residiuum already filled
18.35 of 20 available cohort positions on average, so the defect is redundant
physical work rather than insufficient waiting.

## First corrected-path verification

A 64 MiB retained-media SDK run after the correction completed 8,192 / 8,192
durable writes and recovered every record after a fresh reopen. It recorded:

- 472 cohorts for 8,192 operations (17.36 operations/cohort);
- 472 authoritative media-sync cohorts;
- 472 buffered derived-journal appends and **zero** derived-journal syncs;
- maximum gathered cohort payload of 165,320 bytes; and
- 3,419.23 operations/s / 26.71 MiB/s on the local development machine.

The throughput number is a smoke result, not a Bonzo comparison. The important
qualification fact is the boundary accounting: the former second stable
journal boundary is absent, and the active tail is emitted as one gathered
write per touched shard. CompactShadow still receives the same gathered tail;
there is no longer a per-document primary, Shadow, or journal write call on the
smart-client cohort path.

For the original Bonzo cohort shape, the source-level write-call model therefore
falls from approximately three calls per operation (authoritative tail, Shadow
tail, outcome record), or 3.93 million calls, to three calls per cohort, or
about 214 thousand calls. That is an estimated 18.35× reduction before any
filesystem splitting. Stable boundaries fall from two per cohort to one per
touched active shard. The repeat Bonzo campaign must verify the physical
process-monitor counters rather than treating this model as measured evidence.

## Positioned I/O and byte admission follow-up

The next native I/O slice applies the useful Seastar mechanics without adding
a framework or changing Residiuum's file format:

- authoritative active tails use explicit-offset writes rather than mutable
  `seek` + `write_all` cursor state;
- RSHD0004 staging, envelope patching and commitment publication use the same
  positioned-write primitive;
- the SDK scheduler retains its operation-count bound and additionally holds
  byte credits across queued **and running** mutations (64 MiB product default,
  configurable on `EmbeddedOptions`);
- temporary byte-window exhaustion is explicit retryable `Overloaded`
  admission, while a mutation larger than the configured window is a
  non-retryable `ResourceLimit`;
- the deployment commit coordinator independently bounds queued + installing
  payload credits to two maximum cohorts (32 MiB), applying backpressure to
  callers that do not enter through the SDK; and
- inspection reports current/peak byte credits, byte refusals, coordinator
  waits and oversized exclusive admissions.

This slice deliberately does **not** add `O_DIRECT`, alignment padding or
`io_uring`. Those are optional backend experiments after the grouped portable
path has a same-machine baseline. The durability boundary remains unchanged:
all positioned authoritative bytes are complete before the existing sync, and
the receipt is published only after that sync succeeds.

### Local 64 MiB retained-media smoke after the follow-up

Release-mode run on the same development machine, 8 KiB payloads and
concurrency 20:

| Result | Measurement |
|---|---:|
| acknowledged operations | 8,192 |
| acknowledged payload | 64 MiB |
| throughput | 3,711.4 ops/s; 29.00 MiB/s |
| operation latency | p50 4.14 ms; p95 9.27 ms; p99 27.55 ms |
| gathered cohorts / media syncs | 443 / 443 |
| maximum cohort | 20 entries; 165,320 bytes |
| journal append / sync cohorts | 443 / 0 |
| SDK peak admitted bytes / refusals | 172,830 / 0 |
| coordinator peak admitted bytes / waits | 165,320 / 0 |
| reopen | 1.352 s; all 8,192 payloads validated complete |

The preceding local gathered-write smoke was 3,419 ops/s and 26.71 MiB/s, so
this observation is about 8.6% higher. It is a small, non-isolated local run:
it proves the path remains healthy and the new bounds are not constraining this
workload, but does not attribute the difference solely to positioned I/O.

## Asynchronous smart-client mutation follow-up

The smart client's former mutation path submitted every durable write to a
bounded worker and occupied that worker until group commit returned its
receipt. That made the read/query worker count an accidental hard ceiling on
the number of operations available to a cohort. It was an asynchronous API
over a synchronous wait, not an asynchronous mutation pipeline.

The corrected path now:

- validates and admits each mutation without blocking;
- retains count and byte credits until its real commit result arrives;
- submits directly to the deployment commit coordinator;
- completes each future from its individual cohort result;
- reports `CommitOutcomeUnknown` if an admitted write crosses its deadline,
  while retaining OperationId recovery;
- drains admitted mutations before client close returns; and
- reserves the bounded worker pool for reads, queries, and administration.

Deletes use the same coordinator, so a cohort may contain puts and tombstones
without weakening independent CAS, OperationId, receipt, or recovery
semantics.

Two release-mode 64 MiB runs on the local development machine used identical
8 KiB documents and four read/query workers:

| Callers | Durable throughput | Cohorts / media syncs | Maximum cohort | Peak admitted mutations |
|---:|---:|---:|---:|---:|
| 20 | 2,342.9 ops/s; 18.30 MiB/s | 412 / 412 | 20; 165,320 bytes | 20 |
| 256 | 23,047.8 ops/s; 180.06 MiB/s | 32 / 32 | 256; 2,116,096 bytes | 256 |

Both runs acknowledged and recovered all 8,192 operations after a fresh reopen,
with zero failures, refusals, byte waits, or derived-journal syncs. At 256
callers, p50/p95/p99 acknowledgement latency was 10.08/14.11/15.85 ms and the
complete validation scan took 59.70 ms. The scheduler's mutation peak reached
256 while its synchronous running-work peak was one, directly demonstrating
that durable writes no longer consume read/query workers.

These remain local smoke measurements, not Bonzo qualification. They establish
the architectural effect: increasing mutation concurrency grows useful cohorts
without growing the blocking-worker pool, reduces one stable boundary to every
256 acknowledged operations in this workload, and restores the earlier
approximately 21–25k writes/s range. The remaining gap to a 500 MiB/s target is
inside cohort/storage execution and payload amplification, not a reason to
reintroduce synchronous mutation clients.

## 10 GiB asynchronous smart-client result on Bonzo

Bonzo completed the retained-media campaign at commit
`8e1e0a93810d2a4ef8c2ca5bb632215c14388d1b`, using 256 concurrent mutations and
four read/query workers:

- 1,310,720 / 1,310,720 durable acknowledgements; zero failures or refusals;
- exact 10 GiB payload recovered by a fresh reopen and complete scan;
- 14,252.48 operations/s and 111.35 MiB/s acknowledged payload;
- p50 / p95 / p99 acknowledgement latency 15.00 / 31.69 / 59.36 ms;
- 5,141 cohorts and media syncs, averaging 254.96 operations (maximum 256);
- 5,141 buffered derived-journal appends and zero derived-journal syncs;
- 91.96 s write phase, 0.46 s close, 912.98 s reopen, and 110.47 s validation
  scan; and
- 11,549,663,232 allocated bytes after close for 10,737,418,240 payload bytes
  (approximately 1.08x).

Compared with the earlier 20-worker Bonzo baseline, acknowledged throughput is
9.82x higher. Media-sync cohorts fell from 71,443 to 5,141 (13.89x fewer), and
the former 71,443 journal syncs are absent. This is a material write-path gain,
not a buffering change: every operation still receives its individual durable
receipt and the complete retained store reopens successfully.

The run also establishes a release-blocking startup defect. Reopen took 15.22
minutes, while the subsequent scan of every decoded document took only 1.84
minutes. Live stack samples placed normal reopen first in
`resume_or_start_all_actives` / `scan_forward`, verifying active frames through
small reads, and later in `attach_shadow_dual_to_actives` /
`ShadowDualStream::append_image_chunk`, issuing positioned writes while
reconstructing recovery-shadow state. With 1.31 million records, the behavior
matches the previously observed burst of roughly 2,500 small reads/s: startup
is dominated by per-frame syscall and recovery-shadow reconstruction work, not
sequential device bandwidth.

The retained final store is approximately 11 GiB (the 10.39 GiB active segment,
203 MiB outcome journal, and 159 MiB primary index dominate). The long shadow
attachment work is transient and did not leave a second 10 GiB artifact after
orderly close. `/usr/bin/time -l` reported a maximum resident set of roughly
7.41 GiB; sampled resident size during reopen was 2.5–2.9 GiB. Both startup time
and peak memory require focused qualification before this path is handed to an
application.

## Bounded-active and orderly-close correction

The 10 GiB active file was not an intentional segment size. Operation-cohort
gathering suppressed per-item auto-seal, but the cohort completion path failed
to perform the deferred rotation. Consequently the active recovery tail grew
with the full database. Normal close then synced the outcome journal and wrote
a clean-session certificate without first sealing that tail or checkpointing
the resulting authoritative frontier.

The corrected lifecycle now:

- performs deferred threshold checks after each durable cohort;
- uses 64 MiB as the normal active-segment target;
- rotates CompactShadow protected pairs through the zero-scan asynchronous
  publication path;
- carries the already-known segment summary through publication rather than
  rereading every newly sealed segment for catalog metadata;
- drains authoritative seals, checkpoints the primary index and catalogs,
  syncs the outcome journal, and only then publishes the clean certificate;
- invokes that barrier explicitly from `driver::Client::close`; and
- retains best-effort orderly close from direct `Store` drop, while panic,
  poisoned-writer, and unresolved-outcome paths remain deliberately unclean.

A 256 MiB release-mode smoke on the local development machine used the same
8 KiB documents, 256 mutation callers and four read/query workers. It completed
32,768 / 32,768 durable writes and recovered every record:

- 12,636.19 operations/s and 98.72 MiB/s;
- 128 cohorts/media syncs, exactly 256 operations per cohort;
- zero journal syncs, failures, refusals, or admission waits;
- 0.244 s orderly close;
- **0.171 s clean reopen**; and
- 0.734 s complete validation scan.

Allocated bytes after close were 572,530,688 for 268,435,456 payload bytes
(2.13x). Unlike the defective 10 GiB run, normal rotation now publishes and
retains CompactShadow protection rather than reconstructing a transient shadow
for the entire active file at startup. That recovery redundancy is real and
must be reported separately from the eliminated startup rewrite.

An upgraded legacy store with one giant active file still requires one bounded
migration open because it has no trustworthy precomputed active summary. Its
subsequent orderly close seals and checkpoints that tail. New stores and later
crash recovery are bounded by the active-segment target rather than total
database size.

The crash campaign exposed one necessary fail-closed refinement: an injected
error can occur after a durable media boundary but before index publication.
Such an append-path error now poisons orderly close. The process therefore
cannot checkpoint an older projection over newer authoritative media; reopen
uses the existing old/new/unknown recovery contract instead.

Orderly close is also armed only after create/open returns successfully. A
partially constructed handle from a failed open releases its resources without
checkpointing partial derived state or publishing a clean certificate.

## 10 GiB bounded-active result on Bonzo

Bonzo repeated the same retained-media campaign with 256 mutation callers and
four read/query workers using the bounded-active/orderly-close working tree over
commit `8e1e0a93810d2a4ef8c2ca5bb632215c14388d1b`:

- 1,310,720 / 1,310,720 durable acknowledgements; zero failures, refusals or
  admission waits;
- exact 10 GiB payload recovered by fresh reopen and complete scan;
- **19,978.73 operations/s and 156.08 MiB/s** acknowledged payload;
- p50 / p95 / p99 acknowledgement latency 10.16 / 28.06 / 59.87 ms;
- 5,136 cohorts/media syncs, averaging 255.20 operations (maximum 256);
- 5,136 buffered journal append cohorts and zero journal syncs;
- 65.61 s write phase and **0.94 s orderly close**;
- **1.08 s clean reopen**, down from 912.98 s (approximately 844x faster);
- 134.72 s complete validation scan; and
- 22,917,095,424 allocated bytes after close for 10,737,418,240 payload bytes
  (approximately 2.13x).

The store retained 165 sealed authoritative segments, normally approximately
64 MiB each, and a 181-byte active descriptor. Directory allocation was about
10 GiB authoritative segments, 10 GiB recovery protection, 368 MiB indexes and
209 MiB store metadata. The former 10.39 GiB active recovery tail and transient
full-shadow reconstruction are absent. Maximum resident size was approximately
2.23 GiB, down from roughly 7.41 GiB in the defective run.

This result establishes the intended distinction: ordinary clean reopen loads
bounded metadata and a tiny active descriptor; it does not verify and rewrite
the full database. CompactShadow's retained recovery copy remains a separate
space-amplification decision and is now reported honestly rather than paid as
surprise startup work.

## Post-startup write-path bisection

The bounded-active fix removed startup reconstruction but did not explain the
remaining gap to 500 MiB/s. A release phase bisection on Bonzo established:

- raw 8 KiB sequential writes: 1,626.72 MiB/s;
- one-record full store path: 512.97 MiB/s; and
- existing four-worker batch cook/install path: 1,090.02 MiB/s.

The device and frame format therefore do not impose the observed 156 MiB/s
ceiling. The product operation coordinator was serially cooking frames and the
smart client had no bounded pipelined bulk surface.

The next candidate connects up to four record cookers to eligible product
cohorts. Eligibility is deliberately narrow: single-shard inline puts with
unique subjects. Duplicate subjects, chunked values, deletes and mixed cohorts
retain their exact serial semantics. CAS failures remain individual. Product
inspection now reports the number of concurrently cooked records and aggregate
cohort phase time.

The client also exposes bounded async `Collection::put_many`. It admits all
entries before awaiting receipts, but does **not** claim transaction atomicity:
every entry retains its own operation identity, receipt and terminal result.
Pre-admission validation failure is the outer error; after admission the caller
must inspect every key-correlated outcome. This is the correct mechanism for an
application or bulk loader to offer enough independent work for a larger stable
boundary without creating thousands of threads.

Bonzo retained-media 1 GiB measurements on the candidate were:

| Client shape | Cohorts | Max entries | MiB/s |
|---|---:|---:|---:|
| 256 callers, one outstanding each | 515 | 256 | 202.51 |
| 256 callers, async bulk of four | 132 | 1,024 | **305.13** |

The bulk-of-four run recovered all 131,072 records after fresh reopen, with no
failures, refusals or admission waits. The 3.3560 s client write interval
contained 2.7177 s aggregate store-cohort time, split into:

- 1.5266 s authoritative + Shadow staging write / stable boundary;
- 0.7104 s parallel cook, ordered install and visibility publication;
- 0.3531 s deferred rotation/lifecycle work;
- 0.1017 s outcome journal/publication; and
- 0.0259 s preparation.

Two suspected optimisations were explicitly falsified. Syncing the unchanged
active directory on every cohort was redundant and has been reduced to one
directory-entry barrier per active-file creation, but macOS showed no material
throughput change. `sync_data` is the correct WAL append boundary once that
filename is durable, but APFS priced it approximately the same as `sync_all`.
Submitting authoritative and Shadow writes concurrently from ad-hoc threads was
rejected: throughput fell to 146.90 MiB/s and p99 latency rose sharply because
CPU and lifecycle contention increased.

The remaining delta to 500 MiB/s is now specific. CompactShadow writes a second
independent physical stream, while frame cooking, stable I/O and the next cohort
currently execute as serial stages. The next implementation push must use the
already-designed persistent cooker/pipeline machinery so cooking and admission
for cohort N+1 overlap stable I/O for cohort N, without changing the per-entry
durable acknowledgement boundary. Another 10 GiB campaign is justified only
after that pipeline changes the 1 GiB phase split.

## Rejected pre-hash approximation (2026-08-10)

An initial candidate moved only payload BLAKE3 calculation into four persistent
workers at admission. The immutable body and its bound digest were then consumed
by the existing full-frame cooker after event, segment and sequence identities
were assigned under the store lock. The full store suite, SDK bulk/CAS/retry
tests and fresh-reopen validation all passed.

This was intentionally rejected rather than merged. It was not the pipeline
specified in `ADAPTIVE_WRITE_OPTIMISER_IMPLEMENTATION_PLAN.md`: it overlapped a
body-hash substage, not complete frame cooking from a real reservation. Bonzo
did not show an end-to-end gain:

| Shape | Cohorts | MiB/s | Result |
|---|---:|---:|---|
| bulk four, first isolated run | 139 | 303.72 | equal to 305.13 baseline |
| bulk four, repeat under lower free space | 145 | 199.26 | device state dominated |
| bulk eight / two cohorts outstanding | 128 | 267.69 | more media-boundary time and latency |

The first bulk-four run did show that moving digest work reduced aggregate
`cook_install_publish` time from 0.7104 s to 0.5499 s and total store-cohort time
from 2.7177 s to 2.4499 s. That saving did not reach client throughput. Bulk
eight filled the intended 128 cohorts, but p99 rose to 138.75 ms and aggregate
media-boundary time rose to 1.9530 s.

Bonzo free space also fell from 15.84 GB before the first retained run to 13.55
GB before bulk eight. The large run-to-run media variance means no further
performance decision or 10 GiB campaign is valid until old retained campaign
media is deliberately reclaimed.

The next admissible implementation is the actual AWO reservation boundary:

1. reserve conditions, operation identity, segment/event/item identity, writer
   sequence and an exact active checkpoint under the sole writer;
2. release the store lock and cook the **complete** operation-bearing frames in
   the existing bounded `PersistentCookerPool`;
3. install ready frames strictly by lane ticket;
4. cross the stable boundary, publish visibility and then resolve each
   acknowledgement; and
5. permit depth two only when rotation and duplicate-subject rules prove that
   batch B cannot invalidate batch A.

Pre-hashing alone must not be reintroduced as a production optimisation without
new evidence.

## Full-frame reservation and depth-two overlap (2026-08-10)

The product coordinator now reserves an eligible cohort's conditions,
operation identities, segment/event/item identities, writer sequences and
exact active checkpoint under the sole writer. It then releases the physical
store lock and cooks complete operation-bearing frames in the persistent cooker
pool. Ordered install verifies the ticket sequence and exact checkpoint before
one gathered write and durability barrier; visibility and individual receipts
remain strictly post-durable.

Depth two is deliberately narrower than depth one. A follower may reserve only
when it is subject- and operation-id-disjoint from its predecessor and the
predecessor's already-cooked byte length proves that automatic rotation cannot
occur. The follower receives the deterministic post-predecessor checkpoint and
cooks while the predecessor crosses its media boundary. A third unresolved
reservation is impossible. Any dependency, rotation risk, mixed mutation,
chunked value, retry or failed condition retains the established serial path.

The smart-client default count window is 2,048 operations so two maximum
1,024-entry cohorts can be outstanding. This is a capacity limit, not a promise
that an application will create that concurrency: the original 256 callers ×
bulk four shape has only 1,024 outstanding operations and correctly reports no
overlap.

Bonzo retained-media 1 GiB results were:

| Commit / shape | Cohorts | Overlap ops | MiB/s | p99 |
|---|---:|---:|---:|---:|
| `61b3329`, 256 × bulk 4 historical baseline | 132 | — | 305.13 | — |
| `e8196e9`, 256 × bulk 4 depth-one comparison | 135 | 0 | 307.24 | 51.1 ms |
| `2655408`, 512 × bulk 4 depth-one control | 128 | 0 | 322.28 | 90.3 ms |
| `e8196e9`, 512 × bulk 4 premature follower take | 144 | 52,409 | 303.17 | 385.3 ms |
| `8d92c77`, 512 × bulk 4 bounded follower fill | 133 | 55,198 | 329.64 | 84.9 ms |
| `a738761`, 512 × bulk 4 credit-aware follower fill | 130 | 64,329 | **339.02** | **83.2 ms** |

Every cell committed and recovered all 131,072 records after a fresh reopen,
with zero mutation failures, refusals or byte-admission waits. The first
depth-two attempt is important negative evidence: taking whatever follower work
was immediately visible fragmented cohorts and paid 16 extra stable boundaries.
Giving followers the same bounded 250 µs fill opportunity removed that
regression. Using the admission-credit ledger to wait only when follower work
already exists reduced the accepted candidate to 130 cohorts without adding a
blind delay to the one-cohort workload.

The accepted comparison is the same 512 × bulk-four shape: 339.02 MiB/s versus
322.28 MiB/s, a 5.2% throughput improvement, while p99 improved from 90.3 ms to
83.2 ms. Relative to the original production bulk baseline it is an 11.1%
increase. Aggregate cohort phase values are no longer additive wall time when
overlap is active; reports mark this explicitly. The final 1 GiB run recorded
1.378 s aggregate media-boundary time and 0.368 s rotation time across 130
cohorts. Stable media and CompactShadow staging therefore remain the principal
delta to 500 MiB/s; additional CPU-side frame cooking work is not the next
priority.

### 10 GiB sustained gate: failed performance, passed correctness

The accepted 1 GiB candidate was advanced to the previously agreed retained
10 GiB gate on Bonzo with the same 512 callers × bulk-four shape. It did **not**
sustain the short-run rate:

| Metric | 1 GiB | 10 GiB |
|---|---:|---:|
| Payload throughput | 339.02 MiB/s | **168.28 MiB/s** |
| Durable 8 KiB writes/s | 43,395 | **21,540** |
| p99 | 83.2 ms | **275.4 ms** |
| Cohorts | 130 | 1,297 |
| Overlapped operations | 64,329 | 550,451 |
| Media-boundary time | 1.378 s | 31.540 s |
| Rotation time | 0.368 s | 5.995 s |

Correctness remained intact: all 1,310,720 operations committed, fresh reopen
recovered and exhaustively scanned all 10,737,418,240 payload bytes, and there
were zero mutation failures, refusals or byte-admission waits. The retained
deployment occupied 22.90 GB allocated. Orderly close took 3.313 s, reopen took
1.071 s, and full validation scan took 223.764 s.

This is not authority to proceed to 100 GiB and it is not evidence for a
sustained 339 MiB/s claim. The depth-two mechanism remains a valid bounded
short-run improvement, but the sustained qualification objective is unmet.
The next diagnostic campaign must record per-GiB throughput and phase deltas,
free-space samples, lifecycle queue/backpressure, Shadow finalize/enrichment
activity and media-boundary latency distribution. The current aggregate report
cannot distinguish device-state degradation from growing segment lifecycle
interference or reduced follower formation. No further CPU-side cooking
optimisation is justified before that sustained-path bisection.

## Per-GiB sustained bisection (2026-08-10)

Commit `1c3ef60` added constant-time client inspection of authoritative
rotation, Recovery Shadow and derived-enrichment state, plus per-GiB campaign
samples. The instrumentation was first verified by a complete local retained
write/reopen/scan cycle. Three matched 4 GiB runs were then executed on Bonzo;
each committed and freshly recovered all 524,288 records.

| Recovery / enrichment | GiB 1 | GiB 2 | GiB 3 | GiB 4 | Total |
|---|---:|---:|---:|---:|---:|
| CompactShadow / enabled | 299 | 280 | 174 | 163 | 213 MiB/s |
| CompactShadow / disabled | 341 | 319 | 215 | 139 | 223 MiB/s |
| Materialized / disabled | 400 | 331 | 161 | 147 | 216 MiB/s |

Disabling enrichment removed approximately one GiB of derived segment reads
per GiB of payload and improved the early intervals, but did not remove the
cliff. It also increased the complete validation scan from 29 seconds to 73
seconds, so removing enrichment is not a product answer. Materialized recovery
halved the retained footprint (4.61 GB versus approximately 9.1 GB for 4 GiB
payload) and raised the first interval to 400 MiB/s, proving that the second
Shadow stream is material short-run amplification. It still did not remove the
3–4 GiB cliff.

The phase transition isolated the root defect. In the Materialized control,
third-interval media-boundary time was only 1.415 s, while aggregate
`cook_install_publish` rose to 2.937 s and preparation to 0.358 s. Source audit
then found a fixed 65,536-operation trigger inside foreground durable
publication. At every trigger it cloned the entire growing `durable_index`
under the sole physical writer before delegating a full `primary.idx` rewrite
to the lifecycle worker. At 524,288 records this produced eight successively
larger full snapshots: an O(N²) ingestion curve disguised as asynchronous
checkpointing.

The fixed-count full checkpoint has therefore been removed from ingestion.
The write path now records checkpoint lag only. Orderly close still seals the
authoritative frontier and writes one complete primary checkpoint, preserving
fast clean restart. Explicit operator checkpointing remains available. After
an unclean stop, authority remains the segments and open may rebuild until an
incremental mid-run checkpoint format is implemented. A regression crosses the
former 65,536-operation boundary and proves `primary.idx` is byte-identical
until an explicit checkpoint.

This is a correctness-preserving removal of accidental quadratic work, not the
final checkpoint design. The next qualification run must repeat the ordinary
CompactShadow + enrichment 4 GiB shape with this fix. If the per-GiB curve is
flat, the remaining work separates bounded enrichment scheduling and Shadow
write amplification from the now-fixed retention-size cliff. A 10 GiB gate is
not justified until that 4 GiB comparison passes.

### First post-fix rerun: checkpoint defect real, sustained gate still failed

Commit `2941698` repeated the ordinary CompactShadow + enrichment 4 GiB shape
after removing fixed-count full checkpoints. It again recovered all 524,288
records, but it did not flatten the complete sustained curve:

| GiB interval | 1 | 2 | 3 | 4 | Total |
|---|---:|---:|---:|---:|---:|
| Payload MiB/s | 257 | 292 | 199 | 143 | 207 |
| Cook/install/publish seconds | 1.751 | 1.369 | 1.790 | 2.674 | — |
| Media-boundary seconds | 2.233 | 1.788 | 3.170 | 4.483 | — |

This run began after three retained controls on the same device and free space
fell from 37.1 GB to 28.0 GB. Its absolute throughput is therefore contaminated
by a much harsher APFS/device state and cannot quantify the checkpoint removal
against the earlier run. The phase data is nevertheless sufficient to reject
the claim that periodic index cloning was the only sustained defect. Removing
it is still required: the regression proves it performed repeated foreground
O(N) snapshots and the resulting lifetime curve was quadratic.

A read-only snapshot during validation found a 16 GiB machine, approximately
1.10 GiB campaign RSS at that point, active VM compression and historical swap
traffic. The next instrumentation adds per-GiB process RSS plus constant-time
counts for the two B-tree primary projections, operation-dedup table and
checkpoint lag. Before another comparison, the retained diagnostic stores must
be archived/reclaimed so both candidates begin with equivalent free space and
the device is not tested back-to-back under an exhausted cache/thermal state.
The 10 GiB gate remains blocked.
