# RQL versus MongoDB game dipstick — 2026-08-10

Status: **informal directional result** · not Q5 · not a competitive claim

## Verdict

**Residiuum is not yet in the MongoDB query-performance game.**

The implemented RQL core returned the same answers as MongoDB for all six
measured shapes, which is an important positive result. On this small warm
fixture, however, Residiuum was 10× slower in its best indexed case and roughly
28×–196× slower on the scans, top-k and aggregate cases.

This is a dipstick, not a stable performance ratio. It is sufficient to reject
“same ballpark” today and to identify where profiling should begin.

## Method

- Apple development host, same run and filesystem class.
- 5,000 identical deterministic documents, approximately 1 KiB each, deeply
  nested shape.
- Fixture content hash equal across engines.
- Equivalent equality and compound indexes created before measurement.
- One warm-up and three measured repetitions per query.
- Release-mode Residiuum through the bounded async smart client and QVM.
- MongoDB 8.3.3 through Node.js 26.5.0 and MongoDB driver 6.20.0 over localhost
  TCP.
- Load, index construction and connection/bootstrap excluded from query timing.
- Each timed operation consumes the complete result, not only cursor creation.
- Canonical result digests equal on all six cells.
- Mongo localhost `ping` p50: **0.099 ms**. It is reported but not subtracted.

The local Mongo version differs from the older Q4 pin; this is acceptable for
the directional question and not acceptable for formal qualification.

## Results

Median latency:

| Query shape | Rows | Residiuum | MongoDB incl. TCP | R/M ratio | Result |
|---|---:|---:|---:|---:|---|
| Indexed equality, 10% | 500 | 28.98 ms | 2.90 ms | **10.0×** | digest equal |
| Compound equality/range | 410 | 317.89 ms | 2.17 ms | **146.5×** | digest equal |
| Deep nested scan | 2,500 | 273.56 ms | 9.76 ms | **28.0×** | digest equal |
| Deterministic top-10 | 10 | 248.16 ms | 1.26 ms | **196.3×** | digest equal |
| Grouped count, 500 groups | 500 | 248.04 ms | 1.71 ms | **145.0×** | digest equal |
| Grouped count/sum/min/max/avg | 5 | 246.83 ms | 1.77 ms | **139.3×** | digest equal |

P95 was close to the median on both sides in this tiny run, so no single
outlier created the result.

## Shape of the game

1. **The algebraic core is credible.** Six different query/result shapes,
   including grouped aggregates, produced exact canonical equality.
2. **Localhost TCP is not the explanation.** Mongo's command floor is around
   0.1 ms and its headline includes transport and BSON materialisation.
3. **The admitted equality index works, but the path is still expensive.** It
   examines exactly 500 matching documents, yet costs about 29 ms for roughly
   500 KiB of result materialisation.
4. **The compound index is not selected.** Residiuum examined all 5,000
   documents despite the declared `(region, amount)` index. This accounts for
   part, but not all, of the 146× deficit.
5. **Top-k is not specialised enough.** Returning ten rows after examining
   5,000 costs about the same quarter-second as a full grouped scan. A bounded
   selection/order structure or top-k heap should avoid general full-result
   machinery.
6. **Scan/group fixed work dominates.** Nested scan, grouped count and the five
   accumulators all cluster near 247–274 ms despite very different result sizes.
   That points toward common decode/materialise/QVM phase overhead rather than
   the accumulator arithmetic itself.
7. **Source compile/plan work is in the product call.** Mongo benefits from its
   plan cache. Residiuum needs phase timings before deciding how much is query
   compilation, repeated JSON decoding, scan dispatch, sorting/group state,
   page materialisation or serialization.

## Small next move

Do not broaden this benchmark yet. Add per-query phase timing to the same six
cells:

- source parse/canonical plan/QVM encode or cache hit;
- index selection and candidate acquisition;
- record fetch/decode;
- predicate evaluation;
- sort/top-k or group/aggregate;
- page/result materialisation and serialization.

Then fix the largest common phase and rerun this exact dipstick. Bonzo is useful
only after local phase attribution, or as one larger confirmation that the
ordering of the deficits remains the same.

## First optimisation pass — bounded document pages

The opt-in query diagnostic showed that compilation was not the problem:
compile, lower, decode and VM verification together took only tens of
microseconds. The dominant cost was the storage boundary.

Before the change, every full scan was implemented as approximately 5,000
individual point reads (5,021 host calls including key pages). Each storage
page also cloned the entire remaining live-key tail before consuming only 256
keys, making repeated page enumeration quadratic in the number of keys.

The first production change therefore:

- adds opt-in phase diagnostics to `QueryRunOptions` / `QueryPage`;
- adds a coverage-aware bounded document-page host capability;
- makes embedded QVM scans consume that page instead of composing key-list +
  point-get;
- resolves each bounded storage page under one lock, preserving explicit hole
  evidence and coherent body/version pairs;
- bounds live-index enumeration to the requested page plus one look-ahead key.

Same 5,000-document fixture, one warm-up, three measured iterations:

| Query shape | Before | After | Improvement | Host calls after |
|---|---:|---:|---:|---:|
| Indexed equality, 10% | 28.98 ms | 28.91 ms | 1.00× | 501 |
| Compound equality/range | 317.89 ms | 193.23 ms | 1.64× | 21 |
| Deep nested scan | 273.56 ms | 151.66 ms | 1.80× | 22 |
| Deterministic top-10 | 248.16 ms | 124.24 ms | 2.00× | 21 |
| Grouped count | 248.04 ms | 124.43 ms | 1.99× | 21 |
| Five aggregates | 246.83 ms | 123.29 ms | 2.00× | 21 |

All six result digests remain equal. The pagination/coverage regression set is
also green (10/10). This is a real first gain, not parity: Mongo remains about
10× faster for indexed equality and roughly 16×–99× faster for these scan,
top-k and aggregate cells.

The next cliffs are now clearer:

1. compound equality-plus-range cannot use the declared compound index;
2. equality candidates still use one point read per result;
3. bounded top-k still materialises all documents;
4. a 5,000-document embedded page scan still spends about 90 ms in storage
   reads, before roughly 20–45 ms of decode/predicate/projection work.

## Second optimisation pass — compound prefixes and candidate batches

The original planner discarded all equality information when an AND predicate
also contained a range. The compound index encoding is not numerically
order-preserving, so a direct numeric range walk would be incorrect. The safe
implementation instead uses the longest equality prefix and re-evaluates the
complete predicate over that candidate superset.

Prefix use is admitted only when every trailing compound field is constrained.
This matters because a document missing a trailing indexed field has no
compound posting: using `(status, n)` for `status = "open"` alone could silently
lose an open document with no `n`. A regression test freezes both the admitted
and refused cases.

Index candidates were also still resolved as one point call per key. Candidate
sets are now materialised in bounded 256-key batches under one capability check
and store lock. Queries carrying document/byte budgets, or pages that need only
a prefix of the candidate list, retain the sequential path so batching cannot
weaken budget or continuation semantics.

Same fixture and measurement method:

| Query shape | After pass 1 | After pass 2 | Original | Host calls |
|---|---:|---:|---:|---:|
| Indexed equality, 10% | 28.91 ms | **18.07 ms** | 28.98 ms | 501 → 3 |
| Compound equality/range | 193.23 ms | **40.62 ms** | 317.89 ms | 5,021 → 5 |
| Deep nested scan | 151.66 ms | 154.97 ms | 273.56 ms | 22 |
| Deterministic top-10 | 124.24 ms | 124.61 ms | 248.16 ms | 21 |
| Grouped count | 124.43 ms | 125.56 ms | 248.04 ms | 21 |
| Five aggregates | 123.29 ms | 123.24 ms | 246.83 ms | 21 |

The compound cell now examines 1,000 documents rather than 5,000 and is 7.8×
faster than the original implementation. Indexed equality is approximately
6.2× Mongo and compound equality/range approximately 18.7× Mongo in this local
dipstick. These remain gaps, but they are materially different gaps from the
original 10× and 146× results.

The dominant common residual is now below the QVM planner: resolving 5,000
adjacent documents consumes about 90 ms. The next high-leverage investigation
is segment-aware/coalesced physical reads; top-k specialization can remove some
CPU and materialisation, but cannot remove that current 90 ms scan floor.

## Third optimisation pass — segment-grouped verified reads

The storage page and explicit candidate batch both had a second hidden
point-read pattern. Although the store lock was shared, every locator reopened
and statted the same segment before reading and verifying one frame.

The bounded reader now groups requested locators by segment and opens/stats each
named medium once per batch. It still verifies every frame independently:

- frame checksum and safety limits;
- envelope segment id;
- event id generation fence;
- item id lineage;
- exact subject identity.

Results remain in caller order. Absence and per-record holes remain distinct.
Chunk manifests use the existing generation-exact reassembler, while damaged,
renamed, or unusual media falls back to the established resolver and its error
taxonomy. A direct storage regression proves ordered body/absence results and
that one wrong subject expectation fails independently without contaminating a
valid neighbour.

Same fixture and measurement method:

| Query shape | After pass 2 | After pass 3 | Original | Original→current |
|---|---:|---:|---:|---:|
| Indexed equality, 10% | 18.07 ms | **11.06 ms** | 28.98 ms | 2.62× |
| Compound equality/range | 40.62 ms | **28.60 ms** | 317.89 ms | 11.11× |
| Deep nested scan | 154.97 ms | **89.01 ms** | 273.56 ms | 3.07× |
| Deterministic top-10 | 124.61 ms | **63.43 ms** | 248.16 ms | 3.91× |
| Grouped count | 125.56 ms | **60.26 ms** | 248.04 ms | 4.12× |
| Five aggregates | 123.24 ms | **59.58 ms** | 246.83 ms | 4.14× |

For 5,000-document scans, measured host-read time fell from approximately
90 ms to 26–29 ms. Against the Mongo dipstick this leaves approximate ratios of
3.8× indexed equality, 13.2× compound range, 9.1× nested scan, 50× top-k,
35× grouped count, and 34× five aggregates.

The bottleneck has shifted again. Plain scan/group cells now spend about
26–29 ms resolving/decoding storage and another 20–45 ms in predicate,
logical-key injection, grouping/projection, and result construction. Top-k has
the clearest algorithmic deficit because it still materialises all 5,000 full
documents for ten returned rows.

## Fourth optimisation pass — bounded streaming top-k

The first bounded-order implementation selected the best page after filtering.
It removed the full sort and full-page clone, reducing deterministic top-10
from 63.43 ms to 56.77 ms, but still retained every matching document until the
Order opcode.

The completed implementation moves non-aggregate field ordering across the
Scan/Filter boundary:

- Scan exposes either an indexed candidate stream or a full bounded document
  stream instead of first building a complete document bag;
- Filter still examines every required document, records identical byte/document
  budgets and hole evidence, and applies the complete predicate;
- field-order continuation is applied before a row enters the frontier;
- small K uses a permanently sorted frontier with binary insertion and never
  retains more than K documents;
- larger pages use bounded batched partitioning and keep one look-ahead row
  only when a continuation may legally exist;
- grouped/aggregate queries deliberately retain their full-bag path because
  dropping pre-aggregate rows would be incorrect.

Same 5,000-document fixture, three warm-ups and twelve measured iterations:

| Query shape | After pass 3 | After pass 4 | Original | Original→current |
|---|---:|---:|---:|---:|
| Indexed equality, 10% | 11.06 ms | **10.00 ms** | 28.98 ms | 2.90× |
| Compound equality/range | 28.60 ms | **27.08 ms** | 317.89 ms | 11.74× |
| Deep nested scan | 89.01 ms | **88.11 ms** | 273.56 ms | 3.10× |
| Deterministic top-10 | 63.43 ms | **51.47 ms** | 248.16 ms | 4.82× |
| Grouped count | 60.26 ms | **59.77 ms** | 248.04 ms | 4.15× |
| Five aggregates | 59.58 ms | **58.65 ms** | 246.83 ms | 4.21× |

All six row counts and canonical value digests remain exactly equal to Mongo.
The order unit oracle, page-concatenation laws, multipage oracle matrix,
coverage cases, and index-pushdown cases are green.

The top-10 cell now retains ten documents rather than 5,000 and spends
effectively nothing in the separate Order, Page, and Project phases. Its
remaining 51.47 ms is approximately 24.99 ms of verified host reads and 26.4 ms
inside the streaming Filter phase. Against Mongo's 1.26 ms this remains a
40.7× gap. The central missing mechanism is now explicit: Mongo can serve this
order from its `score` index and stop after ten winners, while Residiuum has no
order-serving index cursor and must still read and compare all 5,000 documents.
Further general top-k container tuning cannot remove that algorithmic deficit.

## Fifth optimisation pass — admitted order-serving index

The fixture now declares the same single-field `score` index used by Mongo.
Residiuum admits it as an exclusive ordered source only when all of the
following are proven:

- index lifecycle is Ready with complete coverage;
- the requested order contains exactly one non-key field followed only by the
  immutable key tie-break (`_key`/`$key` spellings are normalised);
- the index definition is exactly that one field;
- posting count equals the authoritative live collection count;
- every posting is unique, belongs to the exact heap/collection, and names a
  currently live subject;
- every encoded value decodes and can be ordered by the same JSON comparator
  as the query executor.

Current equality index bytes are not numerically order-preserving. The first
admitted query therefore builds a semantic ordered projection from lightweight
postings. That projection is cached on the shared physical store and invalidated
by every secondary-index write, stale transition, rebuild, or delete. Warm
queries walk the projection directly and fetch only enough ordered candidates
to fill the requested page. Missing values, incomplete postings, stale indexes,
compound definitions, or unsupported order shapes fall back to the bounded
streaming top-k oracle.

The invalidation regression exposed and fixed a separate correctness defect:
the non-adaptive direct embedded put path returned from inside the physical
store lock before the common `mark_indexes_stale` step. A successful direct put
could therefore leave an equality index falsely Ready. Direct, adaptive and
asynchronous collection writes now converge on index invalidation.

Same 5,000-document fixture, five warm-ups and twelve measured iterations:

| Query shape | After pass 4 | After pass 5 | Mongo | Residiuum/Mongo |
|---|---:|---:|---:|---:|
| Indexed equality, 10% | 10.00 ms | **11.29 ms** | 2.90 ms | 3.90× |
| Compound equality/range | 27.08 ms | **29.43 ms** | 2.17 ms | 13.56× |
| Deep nested scan | 88.11 ms | **89.66 ms** | 9.76 ms | 9.19× |
| Deterministic indexed top-10 | 51.47 ms | **0.618 ms** | 1.264 ms | **0.49×** |
| Grouped count | 59.77 ms | **60.39 ms** | 1.71 ms | 35.31× |
| Five aggregates | 58.65 ms | **59.08 ms** | 1.77 ms | 33.34× |

The indexed top-10 now examines exactly 10 documents instead of 5,000. Its
median phase attribution is 0.121 ms for ordered-index acquisition, 0.229 ms
for verified document reads, 0.252 ms for filtering and 0.130 ms for result
projection. It is approximately 83× faster than pass 4 and, in this narrow warm
embedded-vs-localhost cell, about 2.0× faster than Mongo end-to-end.

This is not a blanket performance-parity claim. The process-local projection
still has a cold-build cost because the durable equality encoding is not an
ordered numeric encoding. A future versioned order-preserving secondary format
can remove that cold bridge. The scan and aggregate cells remain the dominant
parity deficit.

## Sixth optimisation pass — streaming group accumulators

The group path retained every filtered input document in a bucket and then
rescanned each bucket once for every requested aggregate. It also serialized,
hashed and hex-encoded the group key for every input row. A five-aggregate
query over one numeric field therefore resolved that same field four times per
document and retained the complete 5,000-document working set until Project.

The group path now accumulates while Filter consumes storage pages:

- each group retains only its key values and scalar count/sum/min/max/average
  state;
- full input documents are released immediately after ingestion;
- shared aggregate source paths are resolved once per document;
- canonical group bytes identify the in-memory bucket, while BLAKE3 and hex
  rendering happen once per distinct output group;
- constant `true`/`false` predicates bypass SDA input cloning and evaluation;
- Project recognizes already-accumulated rows and cannot aggregate them twice.

Same 5,000-document fixture, five warm-ups and twelve measured iterations:

| Query shape | After pass 5 | After pass 6 | Change | Mongo | Residiuum/Mongo |
|---|---:|---:|---:|---:|---:|
| Indexed equality, 10% | 11.29 ms | **11.43 ms** | noise | 2.90 ms | 3.95× |
| Compound equality/range | 29.43 ms | **29.75 ms** | noise | 2.17 ms | 13.71× |
| Deep nested scan | 89.66 ms | **89.68 ms** | noise | 9.76 ms | 9.19× |
| Deterministic indexed top-10 | 0.618 ms | **0.592 ms** | 1.04× | 1.264 ms | **0.47×** |
| Grouped count | 60.39 ms | **38.10 ms** | **1.59×** | 1.71 ms | 22.28× |
| Five aggregates | 59.08 ms | **37.24 ms** | **1.59×** | 1.77 ms | 21.01× |

All six row counts and canonical value digests remain exactly equal to Mongo.
The grouped-count Project phase fell to 0.293 ms and the five-aggregate Project
phase to 0.013 ms; aggregation now occurs in the streaming Filter phase as
intended. Grouped count is 6.5× faster than the original 248.04 ms path, and
five aggregates is 6.6× faster than the original 246.83 ms path.

The remaining aggregate floor is now sharply attributed. Both workloads spend
about 26.25 ms in verified host reads and about 37 ms in the combined streaming
Filter phase. They are still roughly 21–22× behind Mongo, so this is not
aggregate parity. The next useful investigation is the per-row representation
boundary shared by scans and aggregates: JSON decode/materialisation,
predicate/path resolution and canonical group-key allocation. A follow-up
change also elides logical `_key` injection when a constant predicate and the
group/aggregate paths prove that the key cannot be observed; it is not credited
with a separate latency claim because storage variance obscured the micro-gain.
Optimising accumulator arithmetic further cannot remove the measured storage
floor.

## Seventh optimisation pass — bounded verified-body cache

Warm queries still reopened and reverified every immutable frame even after the
operating system had cached its pages. The physical store now retains a bounded
process-local cache of logical body bytes only after normal frame verification.
It is derived, not authority:

- lookup requires both the exact subject and the current primary-index event
  id, so a put or delete makes old bytes unreachable immediately;
- the cache is shared by point reads and segment-grouped query batches;
- payload plus subject and per-entry bookkeeping count against a hard 64 MiB
  bound, including empty values;
- admission is FIFO and stale queue records are compacted, preventing an
  update-heavy key from growing cache metadata without bound;
- cache loss or process restart merely returns reads to the verified media path.

Same 5,000-document fixture and exact result digests:

| Query shape | Before cache | With cache | Host read with cache | Mongo | Residiuum/Mongo |
|---|---:|---:|---:|---:|---:|
| Indexed equality, 10% | 11.43 ms | **9.84 ms** | 3.47 ms | 2.90 ms | 3.40× |
| Compound equality/range | 29.75 ms | **31.83 ms** | 5.41 ms | 2.17 ms | 14.67× |
| Deep nested scan | 89.68 ms | **114.50 ms** | 23.85 ms | 9.76 ms | 11.73× |
| Deterministic indexed top-10 | 0.592 ms | **0.890 ms** | 0.291 ms | 1.264 ms | **0.70×** |
| Grouped count | 38.10 ms | **36.08 ms** | 17.95 ms | 1.71 ms | 21.09× |
| Five aggregates | 37.24 ms | **34.98 ms** | 17.96 ms | 1.77 ms | 19.74× |

The aggregate cells improved modestly and verified host-read time fell from
about 26.25 ms to 17.95 ms. The mixed regressions are run-to-run storage and
host variance, not result differences; all six outputs remain exact. More
importantly, this experiment rejects the hypothesis that redundant physical
verification is the dominant remaining gap. The OS page cache was already
serving warm bytes cheaply. Repeated JSON decoding/cloning and row-oriented
predicate/group execution now outweigh the saved media work.

The next optimisation should therefore sit above the byte store: a bounded
decoded-value cache keyed by the same immutable version identity, followed by
phase measurement. If that still leaves a large aggregate gap, parity will
require covering/index-only aggregation rather than increasingly elaborate
full-document scans.

## Eighth optimisation pass — deployment-shared decoded JSON cache

The smart embedded host decoded the same tagged JSON body on every warm scan,
then discarded its tree. Batch candidate reads also discarded the establishing
event id even though scan pages and CAS point reads already preserved it.

The boundary now remains version-bearing end to end:

- bounded collection batches return coherent body/event-id pairs observed
  under one physical-store lock;
- one decoded cache is shared by every authorized heap opened through the same
  physical deployment, while its key includes heap id, collection id and
  application key;
- a hit additionally requires the current establishing event id, so replacement
  and deletion cannot expose a stale decoded value;
- callers receive a deep clone, preventing query-side mutation from changing
  cached state;
- a conservative 3× encoded-size charge plus key/entry overhead is bounded at
  64 MiB; oversize values bypass the cache and stale FIFO metadata is compacted;
- decode failures and coverage holes are never admitted.

Same fixed 5,000-document fixture, five warm-ups and twelve measured iterations:

| Query shape | Verified-body cache | Decoded JSON cache | Improvement | Mongo | Residiuum/Mongo |
|---|---:|---:|---:|---:|---:|
| Indexed equality, 10% | 9.84 ms | **9.83 ms** | noise | 2.90 ms | 3.40× |
| Compound equality/range | 31.83 ms | **26.39 ms** | 1.21× | 2.17 ms | 12.16× |
| Deep nested scan | 114.50 ms | **76.14 ms** | 1.50× | 9.76 ms | 7.80× |
| Deterministic indexed top-10 | 0.890 ms | **0.611 ms** | 1.46× | 1.264 ms | **0.48×** |
| Grouped count | 36.08 ms | **24.07 ms** | 1.50× | 1.71 ms | 14.07× |
| Five aggregates | 34.98 ms | **23.87 ms** | 1.47× | 1.77 ms | 13.47× |

All six row counts and canonical value digests remain exactly equal to Mongo.
An end-to-end regression warms a grouped scan twice, replaces the underlying
record, and proves the next scan observes the new aggregate rather than the
cached old document. Cache unit tests additionally freeze version mismatch,
clone isolation and bounded eviction.

This is a second material representation-boundary gain, but it also clarifies
the remaining problem. Warm aggregate host-read/decode time is now about
11.7–12.0 ms and total latency about 24 ms. Full row-oriented scanning is still
13–14× behind Mongo. The next large step is therefore covering/index-only
execution: group keys and numeric aggregate inputs must be consumable from a
versioned index projection without fetching and cloning complete JSON trees.

## Ninth optimisation pass — covering/index-only aggregation

A constant-predicate group/aggregate can now consume the fields of a complete
Ready secondary index without reading full documents. Admission is deliberately
strict and refusal falls back to the authoritative scan:

- the predicate must be constant `true`, no document budget may be active and
  the query must not have explicitly forced a scan;
- one complete Ready index must contain every group and numeric aggregate field;
- every posting is decoded and must identify one unique, currently-live subject
  in the correct heap and collection;
- posting count and proven subject count must both equal the authoritative live
  collection count;
- requested columns may be selected from a compound index, but duplicate or
  malformed index fields/values refuse admission;
- any collection write marks the index stale and invalidates the projection;
- decoded projections are derived, process-local, FIFO-evicted and charged
  against a hard 64 MiB bound. Restart or eviction only restores the cold index
  decode path.

The first uncached implementation reduced the five-aggregate cell from 23.87 ms
to 5.72 ms. Reusing the already validated decoded projection and resolving its
field slots once per query reduced it again to 1.39 ms. Same fixed
5,000-document fixture, five warm-ups and twelve measured iterations:

| Query shape | After pass 8 | After pass 9 | Improvement | Mongo | Residiuum/Mongo |
|---|---:|---:|---:|---:|---:|
| Indexed equality, 10% | 9.83 ms | **9.67 ms** | noise | 2.90 ms | 3.34× |
| Compound equality/range | 26.39 ms | **25.26 ms** | noise | 2.17 ms | 11.64× |
| Deep nested scan | 76.14 ms | **74.87 ms** | noise | 9.76 ms | 7.67× |
| Deterministic indexed top-10 | 0.611 ms | **0.535 ms** | 1.14× | 1.264 ms | **0.42×** |
| Grouped count | 24.07 ms | **23.86 ms** | noise | 1.71 ms | 13.95× |
| Five aggregates | 23.87 ms | **1.39 ms** | **17.17×** | 1.77 ms | **0.78×** |

All six row counts and canonical value digests remain exactly equal to Mongo.
The five-aggregate cell performs one host call and is approximately 1.28× faster
than Mongo end-to-end in this warm embedded-versus-localhost dipstick. Its
median host-read phase is 0.180 ms and streaming Filter phase 1.003 ms. The
grouped count fixture intentionally remains a full scan because neither product
was given an index on its `status` group field.

Regressions prove full count/sum/min/max/avg results through a compound covering
index, subset-column coverage, and immediate stale-index fallback after a
replacement. The complete group suite, independent semantic oracle, page
concatenation laws, multipage oracle matrix and coverage-grade tests remain
green. This is a narrow parity result, not general query parity: predicate scans
and uncovered aggregates still pay the row-materialisation path.

## Tenth optimisation pass — authoritative projected aggregation scan

An uncovered constant-predicate aggregate no longer has to clone every complete
JSON document into the query executor. The embedded host can now scan the
authoritative collection while returning only the group and numeric aggregate
paths:

- the optimization is admitted only for constant `true` aggregation without a
  document budget or forced-scan oracle;
- the normal covering index is attempted first; projected scanning is explicitly
  not reported as index use;
- pages remain version-bearing and every cache lookup requires the establishing
  event id;
- any storage hole refuses the projected path, allowing the ordinary coverage
  machinery to fail closed rather than silently omit a row;
- nested paths are resolved with the same object traversal, while missing group
  values retain the established null-group behavior;
- decoded-cache projection is performed under one lock per bounded page rather
  than cloning the complete cached tree or locking once per document.

On the first stable same-fixture run, unindexed grouped count fell from 23.86 ms
to **9.55 ms**, a **2.50×** improvement. Mongo remains 1.71 ms, so this cell is
still 5.58× behind and is not parity. The five-aggregate covering-index cell
remained exact at 1.31 ms in that run. Later repetitions experienced a
machine-wide slowdown across every query shape and are deliberately not used to
claim a second micro-improvement.

Regressions freeze two important boundaries: a warmed projected scan observes a
replacement through its new event id, and nested present/null/missing values
produce the same group identities and counts as the ordinary document path.
The remaining projected-scan host floor comes from retrieving full raw bodies
before checking the decoded cache. A safe next experiment is a version-only
collection inventory followed by body reads only for cache misses; it must not
weaken post-cache damage visibility beyond the existing verified-body-cache
contract.

## Eleventh optimisation pass — version-first projected scans

The projected scan previously resolved and copied every raw body before asking
whether the deployment cache already held the decoded value for that exact
event. The store now exposes a bounded key/version inventory that reads only the
live primary index. The SDK uses it as follows:

1. inventory `(key, establishing event id)` in pages of at most 4,096;
2. project version-matched decoded-cache entries directly;
3. batch-fetch bodies only for misses;
4. require every fetched key and version to match the inventory exactly;
5. refuse the optimization on a hole, disappearance or concurrent version
   mismatch so the ordinary coverage-aware scan remains the fallback.

The inventory is identity only, never payload authority. Cache entries still
exist only after a successful body resolution and JSON decode. A dedicated
multipage regression freezes strict ordering, continuation and exact event ids.
The 5,000-document dipstick itself crosses the 4,096-row page boundary.

On a stable fixed-fixture run, grouped count fell from 9.55 ms to **6.59 ms**
(1.45× for this pass and 3.62× versus pass 9). Median host time fell from
7.62 ms to **4.58 ms**. Mongo remains 1.71 ms, leaving this unindexed aggregate
3.85× behind. The covering five-aggregate cell remained exact at 1.43 ms, and
all six canonical result digests still equal Mongo.

## Twelfth optimisation pass — admitted direct comparison kernel

Common normalized comparisons no longer clone a complete JSON document into SDA
for every candidate row. The compiled kernel now admits a bounded direct form
for path-versus-bound-literal equality/inequality/range comparisons and
conjunctions of those comparisons. It traverses borrowed JSON values and keeps
SDA as the fallback for every unsupported predicate shape.

Admission is structural rather than text-pattern based. Parameters are bound
once at compile time; reversed literal/path comparisons reverse their operator;
absent paths remain false for every admitted comparison; numeric and string
ordering follows the predicate profile. Logical `_key` decoration is delayed
until after evaluation when the predicate cannot observe the key, while matching
result documents retain the established `_key` surface.

Stable 5,000-document run, with exact Mongo digests:

| Query shape | Before direct kernel | Direct kernel | Improvement | Mongo | Residiuum/Mongo |
|---|---:|---:|---:|---:|---:|
| Indexed equality, 10% | 9.96 ms | **6.72 ms** | 1.48× | 2.90 ms | 2.32× |
| Compound equality/range | 26.47 ms | **8.10 ms** | **3.27×** | 2.17 ms | 3.73× |
| Deep nested scan | 77.33 ms | **43.28 ms** | **1.79×** | 9.76 ms | 4.43× |
| Deterministic indexed top-10 | 0.583 ms | **0.528 ms** | noise | 1.264 ms | **0.42×** |
| Grouped count | 6.59 ms | **5.82 ms** | 1.13× | 1.71 ms | 3.40× |
| Five aggregates | 1.43 ms | **1.30 ms** | noise | 1.77 ms | **0.73×** |

Median Filter time fell from 4.59 to 1.41 ms for indexed equality, 21.37 to
3.27 ms for the compound query, and 56.86 to 22.93 ms for the deep nested scan.
The independent 144-case semantic oracle and direct-kernel unit matrix remain
green, including absent/null, reversed comparison, parameter and nested
conjunction cases. Later measurements suffered a simultaneous machine-wide
slowdown in every control cell and are not credited.

## Thirteenth optimisation pass — preserved logical byte accounting

Coverage and resource-governance accounting used to compact-serialize every
successfully decoded JSON value solely to rediscover its byte length. Embedded
reads now preserve the original compact JSON payload length beside the decoded
value and pass it through the host boundary. The typed-body tag is explicitly
excluded, so the reported and budgeted byte count remains identical to the
established `serde_json` payload measure. Hosts without this metadata retain an
exact allocation-free JSON length fallback.

Key-aware aggregation remains deliberately conservative: where execution adds
the logical `_key` field before applying the byte budget, it continues to
measure that decorated value. This avoids turning a performance change into a
resource-governance semantic change.

Two consecutive fixed-fixture runs produced the following ranges; all row
counts and canonical value digests remained exact:

| Query shape | Pass 12 | Pass 13 repeated range | Mongo | Status |
|---|---:|---:|---:|---|
| Indexed equality, 10% | 6.72 ms | 6.33–6.70 ms | 2.90 ms | flat/noise |
| Compound equality/range | 8.10 ms | 7.83–8.00 ms | 2.17 ms | flat/noise |
| Deep nested scan | 43.28 ms | **38.81–41.31 ms** | 9.76 ms | **1.05–1.12× faster** |
| Deterministic indexed top-10 | 0.528 ms | 0.525–0.684 ms | 1.264 ms | flat/noise |
| Grouped count | 5.82 ms | 7.61–7.67 ms | 1.71 ms | not credited; host-read variance |
| Five aggregates | 1.30 ms | 1.49–1.58 ms | 1.77 ms | still at parity; noise |

The deep-scan median Filter phase fell from 22.93 ms to 16.84–17.40 ms and
diagnostic byte-account time became zero. The remaining 38–41 ms floor is now
roughly split across host scan/decode, predicate execution and result
materialisation. The next optimisation should therefore target the repeated
full-result cloning/materialisation visible in the Project phase, not byte
accounting or accumulator arithmetic.

## Fourteenth optimisation pass — ownership-preserving result materialisation

The identity Project path was performing two deep clones per accepted row: one
to implement a no-op projection and another to retain a possible cursor source,
even though only the final accepted row can establish the continuation tuple.
Result construction now moves an identity document directly into its row. For
an explicit field projection it retains ownership of only the latest accepted
complete document, because projected output may omit an order field needed by
the cursor. Unbudgeted queries also stop traversing every result solely to
compute a `max_result_bytes` total that no caller requested; budgeted queries
retain the exact former enforcement path.

A new page-concatenation regression orders by an unprojected field over three
pages and proves that the cursor still reconstructs the exact unpaged order.
The existing hard result-byte budget regression remains green.

The least-contended post-change run reduced deep-scan Project time from the
pass-13 range of 15.89–17.26 ms to **4.72 ms**. That run completed in 38.34 ms,
but no end-to-end gain is credited yet: repeated measurements were affected by
large, simultaneous swings in host-read, top-K and covering-aggregate control
cells; all six result digests nevertheless remained exact. The structural clone
removal and its correctness evidence are accepted;
the latency claim remains provisional until a quiet-machine repetition.

## Fifteenth optimisation pass — shared decoded values through predicate admission

The deployment cache already stored decoded JSON as immutable `Arc<Value>`, but
every cache hit immediately deep-cloned the complete tree before the query
kernel could decide whether the row matched. The host boundary now distinguishes
owned and shared decoded values. Direct predicates evaluate the shared tree by
reference; rejected rows are dropped without a deep clone, while accepted rows
are cloned once into an independently owned result before logical `_key`
decoration. Ordinary point reads retain their existing clone-isolated owned
value contract.

Cache identity remains `(heap, collection, key, establishing event id)`. The
existing warmed-replacement regression proves that a new version cannot observe
the old shared tree. Cache mutation isolation, bounded eviction, coverage,
resource governance, driver, multipage and the independent semantic oracle
suites remain green.

Two consecutive quiet fixed-fixture runs, five warm-ups and twelve measured
iterations:

| Query shape | Previous accepted baseline | Shared-value range | Mongo | Residiuum/Mongo |
|---|---:|---:|---:|---:|
| Indexed equality, 10% | 6.72 ms | **3.92–4.25 ms** | 2.90 ms | 1.35–1.47× |
| Compound equality/range | 8.10 ms | **4.29–4.80 ms** | 2.17 ms | 1.97–2.21× |
| Deep nested scan | 43.28 ms | **17.24–17.94 ms** | 9.76 ms | 1.77–1.84× |
| Deterministic indexed top-10 | 0.528 ms | **0.486–0.521 ms** | 1.264 ms | **0.38–0.41×** |
| Grouped count | 5.82 ms | **5.34–5.68 ms** | 1.71 ms | 3.12–3.32× |
| Five aggregates | 1.30 ms | **1.35–1.38 ms** | 1.77 ms | **0.76–0.78×** |

All six row counts and canonical value digests remain exactly equal to Mongo.
Deep-scan host-read and Filter medians both fell to 9.44–9.78 ms, with Project
at 2.75–2.90 ms. This pass removes more than half of the previous deep-scan
latency and moves the general unindexed comparison from 4.43× behind Mongo after
pass 12 to less than 1.84× behind.

`HostCapabilities` is public and its coverage value now carries an ownership
form, so custom host-adapter source code must wrap owned JSON with `.into()`.
This must ship as the next minor pre-1.0 SDK line (0.4), not as a supposedly
source-compatible 0.3 patch.

## Sixteenth optimisation pass — version-first document and candidate pages

Warm document scans no longer resolve raw bodies before consulting the decoded
cache. The store supplies bounded `(key, establishing event id)` inventories;
the SDK returns exact version-matched shared values immediately and batch-fetches
only misses. Explicit index candidate batches use the same protocol through a
new bounded point-identity operation. A fetched key/version mismatch—such as a
concurrent replacement between inventory and miss resolution—abandons the fast
path and repeats the ordinary authoritative body scan. Explicit media holes
remain coverage evidence rather than becoming absence.

Large key-ordered queries also increase their inventory page adaptively up to
the store's 4,096-row bound, while small queries retain the established 256-row
over-read ceiling. Per-document cancellation and resource-governance checks are
unchanged.

On repeated fixed-fixture runs, deep scan fell from 17.24–17.94 ms to
**12.02–12.35 ms**. Compound range settled at **4.20–4.23 ms**; its Filter phase
fell below 0.80 ms, although identity inventory overhead made the end-to-end
candidate improvement modest. All semantic, pagination, coverage, stale-cache,
driver and resource-governance regressions remained green.

## Seventeenth optimisation pass — bounded validated secondary-index cache

The remaining ~2.48 ms Index phase was file-system work: every query listed,
read, authenticated and decoded the secondary-index files, including an empty
list for an unindexed path. The physical store now retains validated decoded
secondary indexes in a process-local FIFO cache:

- admission is charged by represented index-file bytes plus scope overhead and
  is capped at 64 MiB;
- oversize index sets bypass the cache;
- empty index sets carry a non-zero charge and are cached, so negative lookups
  cannot grow without bound;
- every secondary-index write or deletion clears the loaded, ordered and
  covering caches for that collection;
- a regression first caches index absence, creates an index, and proves the
  very next lookup observes it.

Two consecutive quiet fixed-fixture runs:

| Query shape | Pass 17 range | Mongo | Residiuum/Mongo | Result |
|---|---:|---:|---:|---|
| Indexed equality, 10% | **1.66–1.87 ms** | 2.90 ms | **0.57–0.64×** | faster |
| Compound equality/range | **1.77–1.84 ms** | 2.17 ms | **0.82–0.85×** | faster |
| Deep nested scan | **9.86–10.42 ms** | 9.76 ms | **1.01–1.07×** | parity |
| Deterministic indexed top-10 | **0.447–0.460 ms** | 1.264 ms | **0.35–0.36×** | faster |
| Grouped count | **2.99–3.02 ms** | 1.71 ms | 1.75–1.76× | remaining gap |
| Five aggregates | **1.29–1.30 ms** | 1.77 ms | **0.73–0.74×** | faster |

All six row counts and canonical value digests remain exactly equal to Mongo.
The Index phase is now 0.08–0.14 ms for the three comparison queries. On this
dipstick Residiuum has reached raw-data query parity: four shapes are faster,
the unindexed deep scan is within 0.66 ms (7%) at the slower repeated median,
and only the uncovered grouped count remains materially behind.

## Eighteenth optimisation pass — semantic group keys without row serialization

Grouping previously serialized every row's group tuple into canonical JSON,
used those bytes as an ordered-map key, and serialized the same tuple again to
form the final stable `g:` key. The accumulator now owns a recursively hashed
semantic JSON key whose equality is deliberately identical to canonical JSON
identity. Canonical bytes and the stable output hash are produced once per
finished group, and the small final group set is explicitly sorted by those
bytes to preserve deterministic output order.

Two consecutive quiet fixed-fixture runs:

| Query shape | Pass 17 range | Pass 18 range | Mongo | Residiuum/Mongo |
|---|---:|---:|---:|---:|
| Grouped count | 2.99–3.02 ms | **2.58–2.64 ms** | 1.71 ms | **1.51–1.54×** |
| Five aggregates | 1.29–1.30 ms | **1.14 ms** | 1.77 ms | **0.64×** |

Grouped count's Filter phase fell to 0.83–0.89 ms. The equality, compound,
deep-scan and top-K digests also remained exact; their measured ranges were at
least as fast as Pass 17, but are not attributed to this group-specific change.
A direct unit regression proves the semantic key's hash/equality contract
against canonical JSON identity, including nested objects, arrays and numeric
representations. The aggregate, semantic-oracle, pagination, driver, coverage
and governance suites remain green.

## Nineteenth optimisation pass — ownership and allocation-free projected accounting

Projected aggregate rows are owned at the VM boundary, but the accumulator was
borrowing each row and cloning its group fields again. It now consumes each row
and moves group values into the semantic key after inspecting numeric inputs.
The same Scan path also stopped compact-serializing every projected row merely
to calculate `examined_bytes`; it uses the exact allocation-free JSON length
calculator already proven against `serde_json` for ordinary documents.

Two quiet repetitions reduced grouped count to **2.39–2.40 ms** (1.40× Mongo),
with Filter at **0.662–0.672 ms**. All six value digests remained exact.

## Twentieth optimisation pass — authoritative aggregate host pushdown

For a constant-true, unbudgeted aggregate over an authoritative full scan, the
VM may now ask the host to accumulate directly. The embedded host walks bounded
key/version inventories, resolves only exact decoded-cache identities, and
feeds shared documents directly to the established semantic accumulator. It
returns only finished aggregate rows. Any cache miss, coverage hole or identity
drift refuses the pushdown and falls back to the existing projected or ordinary
coverage-aware path. A validated covering index still takes precedence.

The decoded cache now uses heterogeneous raw-key lookup on this hot path, so it
does not allocate 5,000 temporary `String` keys or materialize intermediate
host-document wrappers. `HostCapabilities` gains the optional
`scan_group_aggregate` operation and `HostGroupAggregate` result; custom hosts
remain source-compatible through the default `None`, while hosts that want the
optimization can implement it. Embedded `HeapClient`, `CollectionClient` and
the bounded driver forward the operation.

Two consecutive quiet fixed-fixture runs:

| Query shape | Pass 18 | Pass 20 range | Mongo | Residiuum/Mongo | Result |
|---|---:|---:|---:|---:|---|
| Grouped count | 2.58–2.64 ms | **1.95–2.01 ms** | 1.71 ms | **1.14–1.18×** | near parity |
| Five aggregates | 1.14 ms | **0.985–1.004 ms** | 1.77 ms | **0.56–0.57×** | faster |

Grouped-count Filter time is effectively eliminated (**0.0005 ms**); the work
is now one authoritative host operation at 1.71–1.77 ms plus about 0.10 ms of
final deterministic projection. The other four control shapes remained within
their accepted ranges and all six canonical digests remained exact.

## Artefacts

- Harness: `tools/rql-mongo-dipstick/`
- Residiuum entry: `crates/residiuum-rql-qual/examples/game_dipstick.rs`
- Local reports: `target/rql-mongo-dipstick/residiuum.json` and `mongo.json`
- Capability growth ledger: `RQL_MONGO_CAPABILITY_DELTA.md`
