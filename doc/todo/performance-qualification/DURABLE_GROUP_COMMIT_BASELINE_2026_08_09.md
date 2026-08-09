# Durable group commit baseline — 2026-08-09

Status: **implemented correctness floor; performance qualification pending**

This note records the product write-path baseline created after the Bonzo run
showed approximately 5,500 durable writes/s and an implausibly high stable
boundary count. It does not replace PQH evidence and makes no throughput claim.

## Outcome

The smart embedded client now sends operation-bearing collection puts through
one deployment-wide commit coordinator. Concurrent logical writes retain their
own operation identity, condition, error and receipt, but share:

1. one physical-store ownership interval;
2. one durable active-media boundary; and
3. one durable operation-outcome journal boundary.

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
- the fixed-size checksummed outcome records cross one shared journal boundary;
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

## Deliberate residuals

This is not yet the final write engine.

1. A cohort still performs an individual frame append/write call per logical
   document. Stable boundaries are consolidated; data syscalls are not yet
   assembled into a WiredTiger-style reserved slot or one large gathered write.
2. The authoritative media and outcome journal currently require two stable
   boundaries per cohort. Folding retry evidence into a single authoritative
   commit record is a separate format/recovery change.
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
