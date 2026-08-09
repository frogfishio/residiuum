# Durable group commit baseline — 2026-08-09

Status: **10 GiB correctness PASS; single-boundary gathered-WAL correction implemented and locally verified**

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
3. Operation-bearing puts use the coordinator. Operation-bearing deletes still
   use the single-operation durable path.
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
