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

## Artefacts

- Harness: `tools/rql-mongo-dipstick/`
- Residiuum entry: `crates/residiuum-rql-qual/examples/game_dipstick.rs`
- Local reports: `target/rql-mongo-dipstick/residiuum.json` and `mongo.json`
- Capability growth ledger: `RQL_MONGO_CAPABILITY_DELTA.md`
