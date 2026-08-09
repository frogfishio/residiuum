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

## Clean-device enrichment isolation and 10 GiB gate (2026-08-10)

All retained diagnostic stores were removed from Bonzo after their reports were
archived. Each comparison below consequently began with 88 GiB free on the same
APFS volume. The workload was held constant at 8 KiB documents, 512 callers,
four operations per client submission, durable acknowledgement, and 32 MiB of
writer byte admission.

### Clean 4 GiB matched controls

Clean reruns at commit `ceff7cf` removed the earlier low-free-space ambiguity:

| CompactShadow mode | GiB 1 | GiB 2 | GiB 3 | GiB 4 | Total | Peak sampled RSS |
|---|---:|---:|---:|---:|---:|---:|
| enrichment enabled | 349 | 329 | 325 | 198 | 286 MiB/s | 1.05 GB |
| enrichment disabled | 364 | 375 | 376 | 353 | 367 MiB/s | 745 MB |

Both runs recovered all 524,288 records. The enabled run's fourth interval
coincided with derived enrichment catching up aggressively: 19 segments and
1.32 GB of segment reads in that interval. The disabled control stayed flat.
This proves that foreground-unaware enrichment, rather than the removed primary
checkpoint alone, caused the remaining 4 GiB cliff. On this full logical scan,
enrichment also did not compensate for its write cost: validation was 50.9 s
enabled versus 42.3 s disabled.

Commit `f038ef5` changed derived work to yield until 250 ms of foreground quiet,
with at most one job admitted per two seconds under uninterrupted ingestion.
The same 4 GiB default-product run then produced 368, 358, 352 and 360 MiB/s,
359.5 MiB/s overall, while still completing five enrichment jobs during the
write phase. All records recovered and reopen took 0.44 s. This passes the
4 GiB sustained-shape gate.

That run also exposed an avoidable 10.9 s close: shutdown synchronously drained
59 rebuildable derived jobs. Commit `0392d84` now abandons queued derived work
after any currently executing job completes; authoritative seal work still
drains. A 1 GiB retained-media verification closed with 14 derived jobs queued
in 0.312 s, reopened in 0.153 s, and recovered all 131,072 records. Throughput
remained 363 MiB/s. Store and SDK suites passed (268 and 9 tests respectively).

### 10 GiB: two independent remaining boundaries

The final scheduler commit was then exercised at 10 GiB. Every run below
recovered all 1,310,720 records with no admission failure or swap:

| Recovery / enrichment | GiB 1–3 | GiB 4 | GiB 5 | GiB 6 | GiB 7 | GiB 8 | GiB 9 | GiB 10 | Total |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| CompactShadow / enabled | 372–376 | 212 | 178 | 174 | 178 | 177 | 177 | 221 | 219 MiB/s |
| CompactShadow / disabled | 341–359 | 375 | 351 | 353 | 305 | 255 | 179 | 179 | 283 MiB/s |
| Materialized / disabled | 453–460 | 442 | 466 | 305 | 287 | 239 | 277 | 285 | 345 MiB/s |

The enabled CompactShadow run accumulated 139 derived jobs while allowing only
21 to run. Its time-based bounded progress nevertheless creates positive
feedback after the media path slows: slower GiB intervals admit more derived
jobs, increasing segment reads from about 69 MB to 139–209 MB per interval.
The scheduler fixes the 4 GiB foreground competition defect, but a two-second
maximum deferral remains too aggressive for a continuously saturated store.

The disabled CompactShadow control proves enrichment is not the only 10 GiB
limit. It remains flat through 6 GiB, then its terminal rate reaches 179 MiB/s;
both `cook_install_publish` and `media_boundary` rise. The Materialized control
proves Recovery Shadow amplification is substantial: its first five intervals
are 442–466 MiB/s, its overall rate is 345 MiB/s and its terminal rate is about
285 MiB/s. It still changes regime between 5 and 7 GiB. Both disabled runs show
a marked RSS increase near the seventh GiB, consistent with (but not yet proof
of) a capacity transition in the retained operation-dedup `HashMap`. The next
campaign must expose table capacity and resize duration rather than inferring
them from RSS.

The exhaustive full logical scans are also a separate failed read-path signal:
168.9 s with sparse enrichment, 234.4 s with enrichment disabled, and 173.7 s
for Materialized without enrichment. Clean reopen itself remained about 1.05 s
and close was 0.50–1.02 s; the long duration belongs to row validation, not
startup or durability shutdown.

Accordingly, the current result is:

1. foreground-priority derived scheduling and non-blocking close are accepted;
2. the 10 GiB correctness gate passes;
3. the 10 GiB sustained-performance gate remains open;
4. next instrument and audit the retention-size/capacity transition in the
   primary and dedup projections, then reduce CompactShadow's physical write
   amplification;
5. do not quote cache-assisted GiB 1–5 rates as sustained device throughput.

### Exact I/O calibration (`c15cf1f`)

Commit `c15cf1f` added constant-time counters for authoritative and Shadow
exact-write submissions, bytes, write wall time, durability barriers and barrier
wall time. It also exposes the retained operation-dedup `HashMap` capacity. The
counters do not pretend to count kernel short-write retries; they describe the
calls Residiuum submits to its exact-write helper. A matched clean 4 GiB
CompactShadow/enrichment-off run recovered all 524,288 records and produced:

| GiB | Logical MiB/s | Auth writes / bytes | Auth syncs / time | Shadow writes / bytes | Shadow syncs / time | Dedup entries / capacity |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 285 | 132 / 1.133 GB | 164 / 1.209 s | 146 / 1.115 GB | 16 / 12.3 ms | 133,108 / 229,376 |
| 2 | 354 | 128 / 1.113 GB | 160 / 0.998 s | 144 / 1.113 GB | 16 / 0.04 ms | 263,900 / 458,752 |
| 3 | 365 | 128 / 1.111 GB | 160 / 0.895 s | 145 / 1.119 GB | 16 / 15.4 ms | 394,388 / 458,752 |
| 4 | 357 | 130 / 1.106 GB | 160 / 0.888 s | 137 / 1.055 GB | 15 / 10.9 ms | 524,288 / 917,504 |

The production cohort shape therefore already submits approximately 8.3 MiB
authoritative writes and 7.4 MiB Shadow staging writes on average. The nominal
1 MiB Shadow buffer is a minimum flush threshold, not the observed production
write size: an 8 MiB cohort is appended whole and then flushed once. Increasing
that threshold to 10 MiB cannot be the primary fix.

Bonzo `iostat -I` sampled the internal `disk0` once per second. During the
ingestion window, cumulative device transfer grew by approximately 8.83 GB over
14 seconds, about 630 MB/s including filesystem traffic, with one-second deltas
peaking around 807 MB/s. The device is not continuously at its nominal 1 GB/s
sequential ceiling, but it is materially busier than the logical payload rate
suggests. Residiuum itself submits approximately 2.1 bytes of authoritative plus
Shadow image data per logical payload byte, before filesystem metadata.

The dominant directly timed barrier is the authoritative `sync_data`: roughly
160 barriers per GiB consuming 0.89–1.21 seconds. Shadow performs only 15–16
file barriers per GiB and their direct latency is small because staging writes
have already entered the filesystem; Shadow still consumes a complete second
copy of device bandwidth and makes the authoritative barrier flush under a
heavier dirty-page workload.

Dedup capacity doubled during both GiB 2 and GiB 4 without causing a sustained
throughput collapse. The capacity evidence therefore weakens the earlier
single-cause rehash theory. The later GiB-7 resize may explain an RSS step and a
transient interval cost, but not the persistent terminal media slowdown by
itself.

The next optimization target is now narrower: reduce the approximately 160
authoritative barriers per GiB (128 cohort acknowledgements plus rotation/start
boundaries), and/or remove Shadow's full-byte duplicate from the foreground
dirty-page set while preserving honest P★ lag. Changing the 1 MiB threshold
alone is not justified by the measured write sizes.

### Cohort ceiling falsification and deferred Shadow candidate

Commit `bea6a9d` raised the entry ceiling from 1,024 to 2,048 while retaining
the 16 MiB byte ceiling and 32 MiB admission window. A clean matched 4 GiB run
recovered all 524,288 records and occasionally formed a 2,029-entry,
16,771,714-byte cohort. It nevertheless submitted 496 cohorts versus roughly
512 at baseline and delivered 337.24 MiB/s versus 336.50 MiB/s: +0.22%, inside
run noise. Product admission timing, not the old entry ceiling alone, closed
most cohorts. The candidate is therefore reverted; it is not credited as a
barrier optimization.

Commit `81cc4e0` first moved the complete duplicate Shadow image to a post-seal
worker:

1. Durable acknowledgement continues to require authoritative `sync_data`.
2. At rotation, a durable shard-tagged intent records the protection debt.
3. The authoritative image is published before Shadow work starts.
4. The protection worker copies that immutable sealed image through bounded
   1 MiB read/write buffers, syncs it, atomically publishes it, then advances
   P★. Sealed coverage is published before the copy so lag is observable.
5. Restart completes a pending or sealed deferred intent before claiming P★.
6. Deferred protection is bounded to 16 in-flight 64 MiB segments (about 1 GiB
   of protection lag); saturation backpressures writers rather than allowing a
   10 GiB benchmark to hide an unbounded Shadow debt.

The local 64 MiB smoke proved foreground separation, but the clean matched Bonzo
run rejected this implementation. Its GiB intervals were 368, 397, 258 and
194 MiB/s (280.45 MiB/s total), versus the 285, 354, 365 and 357 MiB/s baseline
(336.50 MiB/s total). The worker kept pace and published 15–16 Shadows per GiB,
so this was not hidden debt. Copying from sealed authority adds one full media
read before the duplicate write: approximately 3× payload traffic rather than
the write-time path's approximately 2×. The later authoritative `sync_data`
time rose to 1.73 and 2.61 seconds/GiB. Post-seal reread is rejected.

The replacement keeps single-pass write-time bytes but changes their scheduler:

1. Foreground authority copies each already-encoded cohort into a store-wide
   bounded memory queue; it performs no Shadow file I/O.
2. One Shadow staging worker owns all staging files and emits exact 1 MiB writes.
3. Four maximum-size 16 MiB cohorts may queue (64 MiB), plus the cohort being
   copied. Queue saturation applies protection-first backpressure.
4. At rotation, `Finish` is ordered behind every prior append for that segment.
   Authoritative rename cannot occur until the worker returns a fully encoded
   Shadow temp. This preserves the existing crash-recovery state machine: any
   Shadow temp beside pending/sealed authority is complete and publishable.
5. The protected-pair worker publishes authority, records visible P★ lag, syncs
   and atomically publishes Shadow, then advances durable coverage.

The explicit synchronous RSHD0004 path and its failpoint matrix remain available
as a qualified reference. CompactShadow product mode selects the bounded
off-thread stager. This replacement must repeat the clean 4 GiB comparison; no
performance claim is made from the failed `81cc4e0` candidate.

Commit `1d175f5` passed that clean matched comparison. Its four intervals were
360.99, 363.30, 363.47 and 352.41 MiB/s, with 359.84 MiB/s overall (46,059
durable operations/s). The exact-I/O baseline was 336.50 MiB/s, so the accepted
gain is 6.94%. All 64 rotations observed during the four sampled intervals also
published their Shadows; there was no protection backlog, byte-admission wait,
operation failure or swap. Shadow staging submitted 1,066–1,073 exact 1 MiB
writes per GiB on its worker. The last interval remained within 3% of the first,
so the sustained-shape gate passes.

Close took 0.267 s, clean reopen 0.460 s, and the exhaustive 4 GiB logical scan
43.33 s. All 524,288 records and all 4,294,967,296 payload bytes validated after
restart. Peak sampled ingestion RSS was approximately 750 MB. The report is
archived as `1d175f5-async-shadow-4g.report.json` under the 2026-08-10 sustained
bisection archive. Bonzo was returned to 88 GiB free.

This accepts bounded asynchronous single-pass Shadow staging as the product
path. It does not establish a 500 MB/s claim. The next performance step should
target the roughly 159–163 authoritative durability barriers per GiB; Shadow
scheduling is no longer the dominant foreground defect in this 4 GiB shape.

### Stable-prefix barrier elision (`81b4c47`)

The remaining 159–163 authoritative barriers per GiB separated cleanly into
roughly 127–132 cohort acknowledgement barriers and two redundant barriers per
64 MiB rotation. The old path called `sync_data` on the retiring active even
when its complete prefix had just crossed the cohort barrier, then called it
again on the replacement active immediately after creation had already
completed `sync_all` and the active-directory barrier.

Commit `81b4c47` records two distinct active-file watermarks:

- `durable_len` remains the historical write-through prefix (Buffered writes
  advance it, so its old name must not be interpreted as crash stability);
- `stable_len` advances only after a successful file durability barrier.

A Durable tail flush now skips `sync_data` only when `stable_len ==
durable_len`. This does not move the acknowledgement boundary. If Buffered
bytes follow a Durable prefix, `stable_len < durable_len` and rotate/seal still
performs the barrier. New tests prove both the already-stable skip and the
Buffered-tail negative case. Store unit tests (273 total), RSHD0004 (16/16),
default-flip (3/3), and product campaign (6/6) passed.

Two clean matched Bonzo runs recovered every one of 524,288 records and all
4,294,967,296 payload bytes. The first produced 344.96 MiB/s; the immediate
repeat produced 362.21 MiB/s (46,363 durable operations/s), with interval rates
361.23, 364.78, 362.68 and 360.57 MiB/s. The repeat's 515 cohorts produced
exactly 515 authoritative writes and 515 authoritative barriers. Against the
accepted `1d175f5` run (516 cohorts and approximately 644 barriers), this
removes about 129 barriers, or 20%, without weakening a single cohort ACK.
Rotation flush time fell from roughly 39–49 ms/GiB to 4–6 microseconds/GiB.

The throughput result is deliberately classified as neutral-to-small-positive,
not as a new large performance claim: 362.21 MiB/s is only 0.66% above the
accepted 359.84 MiB/s run, and the first replicate demonstrates device-latency
variance. The I/O cleanup itself is accepted because the eliminated calls were
provably redundant, the crash contract is unchanged, and the measured barrier
count matches the intended one-per-cohort model exactly.

The repeat closed in 0.268 s, reopened in 0.448 s, and completed its exhaustive
logical validation in 44.24 s. Its report is archived as
`81b4c47-stable-prefix-repeat-4g.report.json` beside the first replicate
`81b4c47-stable-prefix-4g.report.json`. The next throughput target is therefore
the 515 real cohort acknowledgement barriers and the work surrounding them,
not rotation/start re-syncs and not a larger entry ceiling.

### Gathered physical cohorts (`d13f741`)

The failed `bea6a9d` experiment established that merely raising the logical
entry ceiling consumed the entire 2,048-request client window, destroyed
follower cook/I/O overlap, and usually failed to fill 16 MiB before the 250 µs
collection deadline. A second local spike confirmed that holding a small
remainder for the next refill halves barriers but creates an acknowledgement-
gated refill bubble. Neither is the product design.

Commit `d13f741` instead divides the existing window into two bounded logical
halves (at most 1,024 entries / 8 MiB each). When both halves are already
present and their union fits 2,048 entries / 16 MiB, the coordinator treats the
union as one physical cohort: all frames are cooked and installed, one gathered
authoritative write crosses one `sync_data` boundary, and only then are all
individual outcomes returned. No acknowledgement is issued between the halves.
Oversized singleton and edge shapes retain the previously qualified depth-two
path rather than exceeding the gathered bound. Admission remains 32 MiB.

The clean matched 4 GiB Bonzo run sustained 413.41, 414.39, 414.23 and 416.55
MiB/s, for **414.46 MiB/s overall and 53,051 durable operations/s**. This is
14.43% above the accepted `81b4c47` repeat (362.21 MiB/s) and 23.17% above the
exact-I/O baseline (336.50 MiB/s). The last interval exceeded the first, so
there is no terminal throughput collapse in this run.

The accounting closes exactly:

- 524,288 operations formed 261 physical cohorts;
- those cohorts submitted 261 authoritative writes and 261 authoritative
  barriers (approximately 65/GiB, down from approximately 129/GiB);
- maximum observed cohort was 2,028 entries / 16,763,448 bytes, below the
  2,048-entry / 16 MiB bounds;
- all 524,288 operations used concurrent frame cooking;
- all 63 rotations published 63 Shadows, with no protection backlog;
- zero operation failures, admission waits, scheduler refusals or swaps.

Batching did not trade throughput for worse client latency in this saturated
shape. Compared with `81b4c47`, p50 fell from 36.48 to 33.06 ms, p95 from 69.87
to 57.74 ms, and p99 from 74.77 to 65.63 ms. Close took 0.258 s, clean reopen
0.436 s, and the exhaustive logical validation 44.25 s. Every record and all
4,294,967,296 payload bytes validated after restart.

The report is archived as `d13f741-gathered-cohort-4g.report.json` under the
2026-08-10 sustained-bisection archive, and Bonzo was restored to 88 GiB free.
Gathered physical cohorts are accepted as the product path. This still does not
establish 500 MiB/s; the remaining delta is approximately 85.5 MiB/s, with one
real stable barrier per approximately 16 MiB cohort plus frame/index/outcome
publication and rotation costs.

### Cohort hot-loop and smart-client CPU bisection (`696c6a2`–`2dca057`)

Commit `696c6a2` removed two provably redundant operations from the gathered
cohort hot loop: disabled boundary probes no longer take per-record clocks, and
the lifecycle completion queue is polled once per physical cohort rather than
once per record while the same writer lock prevents concurrent application.
The checkpoint-lag counter still advances by the exact record count. Store unit
tests (273/273), RSHD0004 (16/16), default-flip (3/3), and product campaign
(6/6) passed.

The matched 4 GiB run completed at **411.43 MiB/s**, versus 414.46 MiB/s for
`d13f741`. Its `cook_install_publish` aggregate fell by only 16.47 ms across all
524,288 records; two extra cohorts and media variance dominated the wall time.
This is accepted as hot-loop cleanup, not as a throughput improvement. The
report is `696c6a2-cohort-hot-loop-4g.report.json`.

An 8-second macOS CPU sample of the same saturated shape then identified the
smart-client preparation path as significant runnable CPU: `serde_json::Value`
was cloned into another complete JSON tree before serialization, request and
operation identities each entered the OS entropy path separately, and mutation
admission serialized callers on scheduler/drain mutexes. BLAKE3 frame/content
hashing and `pwrite` were the expected dominant storage-side runnable work;
primary B-tree insertion was comparatively small. The sampling run is
diagnostic and is not a throughput result. Its retained artifacts are
`696c6a2-cpu-profile-4g.sample.txt` and
`696c6a2-cpu-profile-4g.report.json`.

Commit `3eef0b4` therefore preserves the established durable JSON bytes while
avoiding the redundant tree clone when the typed collection already contains a
`serde_json::Value`. Arbitrary `Serialize` implementations retain the old
normalization path. `put_many` also obtains all absent request/operation IDs in
one bounded OS-CSPRNG fill; every ID remains OS-CSPRNG material, non-zero and
fail-closed. SDK unit tests (171/171) and embedded-driver integration tests
(9/9) passed. The matched run sustained **418.24 MiB/s / 53,535 ops/s**, a real
but modest **0.91%** improvement over `d13f741`, with interval rates 417.88,
414.92, 424.50 and 416.47 MiB/s. It retained 261 writes/barriers, committed and
revalidated every record, and had zero failures, waits, refusals or swaps. p95
was 56.42 ms and p99 59.31 ms. The report is
`3eef0b4-sdk-bulk-4g.report.json`.

Commit `2dca057` removes the remaining unnecessary scheduler lock convoy for
mutations. Mutation work never enters the read/query worker channel, so one
atomic state now combines bounded admitted count with a closed bit. Close first
closes that state, refuses late mutations, and waits until every earlier
admission releases it. A close-race test proves both halves. The same SDK unit
and embedded-driver suites passed. Its matched run sustained **415.85 MiB/s /
53,229 ops/s**, with interval rates 415.60, 424.11, 415.10 and 409.51 MiB/s,
again 261 writes/barriers and complete restart validation. This is classified
as concurrency-quality cleanup, not an additional throughput gain. The report
is `2dca057-atomic-admission-4g.report.json`.

These experiments narrow the next target. Client-side waste mattered, but the
accepted improvement remains only 3.78 MiB/s. At this shape the physical
cohort phases still spend approximately 0.83–0.88 s/GiB on authoritative
write+barrier and 0.34–0.37 s/GiB on rotation. Reaching 500 MiB/s now requires
overlapping safe rotation/publication work with the next cohort or otherwise
reducing those real physical costs. More per-record bookkeeping cleanup or a
larger logical batch is not supported by the evidence.
