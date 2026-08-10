# RQL versus MongoDB weak-cell matrix — 2026-08-10

Status: **warm local weak cells closed; store-reopen baseline complete with startup fix; cold-query, scale and remote deltas pending**

Parked-work restart instructions:
[`RQL_PERFORMANCE_PROOF_CONTINUATION_2026_08_10.md`](RQL_PERFORMANCE_PROOF_CONTINUATION_2026_08_10.md).

## Purpose

The six-shape game dipstick found the fast path. This matrix deliberately looks
for the opposite: ordinary query shapes where Residiuum is materially worse
than MongoDB or where a favorable small warm test can conceal an architectural
problem. It is diagnostic evidence, not a competitive qualification claim.

## Protocol

- Identical deterministic 5,000-document, approximately 1 KiB fixture.
- Identical companion customer collection and logical join answers.
- Three warm-ups and seven measured iterations.
- Residiuum embedded smart client measured without Mongo resident.
- MongoDB 8.3.3 through Node.js driver 6.20.0 over localhost, measured in a
  separate run.
- Load and index construction excluded.
- Exact canonical row counts and SHA-256 value digests required before a
  latency comparison is admitted.

## Warm local results

| Query shape | Residiuum | Mongo | R/M | Rows | Result |
|---|---:|---:|---:|---:|---|
| Indexed equality | **1.57 ms** | 2.84 ms | **0.55×** | 500 | faster |
| Compound equality/range | **1.68 ms** | 2.08 ms | **0.81×** | 410 | faster |
| Deep nested scan | **9.71 ms** | 11.03 ms | **0.88×** | 2,500 | faster |
| Indexed top-10 | **0.461 ms** | 1.410 ms | **0.33×** | 10 | faster |
| Plain grouped count | 2.21 ms | **2.05 ms** | 1.08× | 500 | parity |
| Five covered aggregates | **0.984 ms** | 2.17 ms | **0.45×** | 5 | faster |
| Full result materialisation | **16.91 ms** | 22.60 ms | **0.75×** | 5,000 | faster |
| Filtered grouped count (baseline) | 7.36 ms | **1.07 ms** | **6.90×** | 100 | weak |
| High-cardinality grouped count | 39.73 ms | **10.66 ms** | **3.73×** | 5,000 | weak |
| Indexed one-to-many enrichment | 137.41 ms | **57.83 ms** | **2.38×** | 5,000 | weak |

All ten row counts and canonical value digests are exact.

## Findings

### W1 — filtered aggregation

`where region = "r0" group by status` cannot use the constant-true aggregate
host pushdown. It scans 5,000 documents and spends a 7.23 ms median in the
combined Filter/accumulator phase. Mongo completes the equivalent `$match` +
`$group` pipeline in 1.07 ms.

**Closed in the first optimisation pass.** Predicate-aware aggregate pushdown
now passes the already-authoritative compiled kernel into the host rather than
introducing a second predicate algebra. When the planner has an admissible
index, it also passes a bounded candidate identity set. The heap validates each
key/version identity, resolves the exact decoded value and evaluates the same
compiled kernel before accumulation. A cache miss, version drift or absent
identity refuses the shortcut and returns to the ordinary execution path.

An equivalent single-field `region` index was installed in both systems for
the after measurement. This is necessary because Residiuum's compound index
does not contain documents missing a trailing indexed field and therefore
cannot safely pretend to be a complete prefix index.

| Measurement | Residiuum | Mongo | R/M | Documents examined | Exact result |
|---|---:|---:|---:|---:|---|
| Baseline | 7.36 ms | **1.07 ms** | 6.90× | 5,000 | yes |
| Predicate + candidate pushdown | **0.596 ms** | 0.659 ms | **0.91×** | 1,000 | yes |

The final Residiuum median attributes 0.491 ms to the host aggregate and
effectively zero to the outer Filter phase. The canonical 100-row digest is
identical across engines. On this warm local cell Residiuum moved from 6.9×
behind to approximately 9.5% faster than Mongo, including Mongo's localhost
command transport. This closes W1; it does not substitute for the pending
cold, scale or remote lanes.

### W2 — high-cardinality group pagination

The first matrix run exposed a correctness defect: a 5,000-group result stopped
at the 4,096 page boundary. Two bugs were fixed:

1. group exhaustion incorrectly probed the source key stream using a synthetic
   `g:` result key;
2. logical-`_key` group continuation was applied to the source scan before
   aggregation rather than to the finished group bag.

A regression now concatenates multiple logical-`_key` group pages and proves
all groups survive. The corrected query is still slow: page two recomputes the
entire aggregate, so 5,000 output groups examine 10,000 source documents and
spend 34.60 ms in Filter/aggregation.

**Closed in the third optimisation pass.** An unbudgeted, hole-free grouped
query now retains only the unreturned portion of its finished group bag in a
bounded process-local spool. The continuation cursor carries a MAC-protected
opaque spool identity and the authoritative heap frontier. Entries are
single-use, expire with the cursor lifetime and share a 64 MiB process bound.
A replay, eviction, restart, unavailable frontier or inter-page write never
trusts stale derived state: it discards/misses the spool entry and executes the
ordinary query again. Budgeted or incomplete queries do not use this shortcut.

The first execution was also made competitive rather than merely hiding it
behind pagination. Exact grouping by immutable logical `_key` bypasses the
general group hash table, count-only groups avoid per-group accumulator
allocation, and the heap aggregate path consumes the authoritative key beside
the borrowed decoded body. It no longer clones every approximately 1 KiB
document to inject `_key`, performs a redundant canonical-group sort, or reads
the unbudgeted source in 256-row fragments. A body field named `_key` is now
unconditionally overwritten by the store-authoritative logical key; a
regression prevents body data from spoofing group identity.

| Measurement | Residiuum | Mongo | R/M | Documents examined across pages | Exact result |
|---|---:|---:|---:|---:|---|
| Baseline | 39.73 ms | **10.66 ms** | 3.73× | 10,000 | yes |
| One execution + direct `_key` aggregate | **6.92 ms** | 7.01 ms | **0.99×** | 5,000 | yes |

The after measurement follows the same separate-process protocol with three
warm-ups and seven measured iterations. Both engines returned 5,000 groups and
the same canonical digest. Residiuum's median attributes 3.22 ms to the host
read/aggregate and 1.63 ms to final projection. The concatenation regression
requires seven source documents to be examined once across three pages; spool
hit, consumed-cursor replay and source-frontier drift are separately frozen.
This closes W2 for the warm local exact-`_key` cell. General high-cardinality
group shapes and cache-independent behavior remain subjects of the scale and
cold/restart lanes.

### W3 — indexed enrichment

The equality index gives the right answer, but 5,000 root rows still incur
102.46 ms of host reads. End-to-end enrichment is 137.41 ms versus Mongo
`$lookup` at 57.83 ms.

**Closed in the second optimisation pass.** Root join values are deduplicated,
probed as bounded batches against one Ready/complete single-field foreign
index inventory, and the candidate union is materialised through bounded
coherent body batches. Both the value batch and candidate-key working set are
hard bounded. Absent candidates remain legitimate concurrent deletions;
coverage holes fail closed. The ordinary scan remains the fallback whenever
the index cannot prove an exclusive candidate union.

Attachment now consumes root documents rather than cloning the entire root
page, borrows the query-local foreign lookup table and clones only documents
that are actually attached. Match-key construction uses a type-separated fast
path and hash lookup while foreign-key sorting retains deterministic `many`
order. Full execution also stopped repeating Core's already-enforced result
memory check before the first expanding opcode; every expansion still checks
its actual retained peak.

| Measurement | Residiuum | Mongo | R/M | Foreign host-read time | Exact result |
|---|---:|---:|---:|---:|---|
| Baseline | 137.41 ms | **57.83 ms** | 2.38× | 102.46 ms | yes |
| Batched lookup + owned attach | **26.91 ms** | 30.84 ms | **0.87×** | 3.41 ms | yes |

The after pair was refreshed under the documented separate-process protocol;
Mongo was not resident during the Residiuum run. Both engines returned the
same 5,000 rows and canonical digest. A 53-root regression with 52 distinct
join values also requires fewer than 20 total host calls, making restoration
of the old probe/read N+1 loop observable in CI. W3 is closed for this warm
local cell; fan-out curves remain part of the pending join-scale lane.

### W4 — controls

Full 5,000-document result materialisation is faster than Mongo, so large
result ownership alone is not the primary weakness. The plain group, scan,
top-K and covered aggregate controls also remain competitive. The weak cells
are pipeline composition and repeated work, not a general JSON-engine failure.

### W5 — store reopen and first-query penalty

The restart lane now has an explicit product harness rather than an inferred
"cold" number. For each query shape and repetition it orderly-closes the same
media, creates a fresh physical deployment and decoded cache, records driver
open, every structured store-open phase, heap/collection binding, the first
query, an immediate same-connection repetition and orderly close. It records
version-qualified decoded-cache hits/misses and retained charge around both
queries. Seven repetitions use the same 5,000-document fixture and all ten row
counts and value digests equal the established warm result.

This is a **store-reopen** lifecycle with uncontrolled OS page cache. It is not
a process-restart or device-cold claim. It is deliberately not scored against
a warm MongoDB server; a future competitive restart comparison must restart
the corresponding Mongo server state under a separately declared protocol.

The first report exposed needless clean-lifecycle storage work. Tier-state load
durably republished three unchanged derived control documents on every open,
and an unchanged active segment was rewritten and synced after recovery had
already proved its bytes canonical. A read-only connection also rewrote the
accepted primary checkpoint and derived catalogues during orderly close despite
having no new authoritative frontier. Atomic derived documents now skip only
byte-identical publishes; changed state retains the full crash-safe atomic
publish. Active media is rewritten only when recovery actually changes its
canonical bytes, and checkpoint/catalogue close work occurs only after durable
operations.

| Clean lifecycle phase | Before | After | Change |
|---|---:|---:|---:|
| Smart-client open | 49.68 ms | **17.22 ms** | **2.89× faster** |
| Store open | 40.62 ms | **8.36 ms** | **4.86× faster** |
| Tier state | 27.75 ms | **0.257 ms** | **108× faster** |
| Active resume | 4.42 ms | **0.040 ms** | **110× faster** |
| Orderly read-only close | 66.20 ms | **11.60 ms** | **5.71× faster** |

Every after-open accepted the v4 checkpoint (`Loaded` / `AcceptedV4`), decoded
zero full segment bytes, repaired zero pending seals and repaired zero protected
pairs. The remaining store-open median is principally identity/writer-lock
(4.20 ms) and primary checkpoint load (1.86 ms); smart-client construction adds
approximately 8.86 ms beyond the physical-store report.

The first-query result is less comfortable and is now the dominant cold delta.
The first host-aggregate implementation also refused on a cold decoded-cache
miss and restarted through the ordinary query pipeline. It now resolves the
exact versioned bodies in one bounded batch and continues the same aggregate.
For constant-true and direct-comparison filtered grouped aggregates, a
presence-aware streaming JSON projection consumes and validates the complete
payload but materializes only the required predicate, group and numeric fields.
Missing, explicit null and present values remain distinct during predicate
evaluation. A separate 16 MiB bounded scalar-projection cache is fenced by
heap, collection, key, record version and exact field list; it never advertises
an incomplete projection as a decoded document. Exact `_key` count grouping
uses the same fully validated, zero-field path.

Full RQL previously cloned every Core result before attach and retained both
the pre-attachment `base.rows` tree and the attached output. Remote execution
also serialized both `rows` and `base_rows`. Core row ownership now moves into
the Full pipeline; `base` retains continuation, coverage, consistency and
diagnostic evidence with an empty row vector plus explicit `base_row_count`.
The wire sends that count instead of a duplicate result set, while the client
accepts and discards legacy `base_rows` during rolling upgrades. Verified
get-many cache misses also decode directly after their authoritative version
check instead of probing the same cache twice.

Seven fresh store reopens after those changes produced:

| Query shape | First after reopen | Immediate repeat | First/repeat | First cache misses |
|---|---:|---:|---:|---:|
| Indexed equality | 6.43 ms | 1.45 ms | 4.42× | 500 |
| Compound equality/range | 9.25 ms | 1.67 ms | 5.53× | 1,000 |
| Deep nested scan | 35.18 ms | 8.93 ms | 3.94× | 5,000 |
| Indexed top-10 | 4.27 ms | 0.576 ms | 7.41× | 16 |
| Plain grouped count | **28.18 ms** | **2.67 ms** | 10.55× | **0** |
| Five covered aggregates | 4.75 ms | 1.06 ms | 4.49× | 0* |
| Full result materialisation | 39.23 ms | 13.97 ms | 2.81× | 5,000 |
| Filtered grouped count | **8.10 ms** | **0.811 ms** | 9.99× | **0** |
| High-cardinality grouped count | **28.63 ms** | **6.32 ms** | 4.53× | **0** |
| Indexed one-to-many enrichment | **43.64 ms** | **14.42 ms** | 3.03× | 6,252 |

Cache counts are decoded-document version-qualified lookup attempts, not unique
documents. Every immediate repeat recorded zero decoded-cache misses. `*` The
covered aggregate uses the secondary-index projection instead of document JSON,
so its first-execution penalty belongs to the separate process-local covering-
index decode cache and needs its own counter. Direct filtered aggregation now
uses presence-aware scalar projections; general SDA predicates still require
complete decoded documents.

Relative to the original reopen baseline, plain grouped count improved from
42.23/3.66 ms to 28.18/2.67 ms, filtered grouped count from 15.83/1.13 ms to
8.10/0.811 ms, high-cardinality grouped count from 53.49/9.44 ms to
28.63/6.32 ms, and indexed enrichment from 83.91/35.92 ms to 43.64/14.42 ms
(first/repeat). Enrichment is now only 4.41 ms slower cold and 0.44 ms slower
warm than materialising the same 5,000 root documents without the join.

Full result materialisation itself is now an ownership floor for the current
public API: returning 5,000 independently owned `serde_json::Value` trees
requires constructing them. The remaining broad cold penalty is concentrated
in predicates and result-producing queries that genuinely require complete
documents, plus the still-uninstrumented covering-index decode cache.

The next cold-path optimization target is durable/versioned scalar projection
for indexed and covering paths. The streaming projector avoids tree allocation
but must still tokenize every authoritative body to preserve damage visibility;
only a rebuildable projection tied to the record version can avoid that scan.
Cache loss must continue to fall back to authoritative media without weakening
damage visibility.

## Remaining matrix lanes

| Lane | Exact experiment | Status |
|---|---|---|
| Cold/restart | Reopen process-local state, measure open separately from first query, record index disposition and decoded-cache misses | baseline complete; clean-open no-op writes fixed; cache-independent first-query execution remains open; no device-cold or Mongo restart score claimed |
| Scale | 1,000,000 streamed ~1 KiB documents; full count/scan, selective predicate, low/high-cardinality group; load excluded | pending on Bonzo; constant-memory fixture required |
| Remote | Same warm cells through the real Residiuum server and smart remote client, with localhost command floor | pending; server-side QVM versus client-side fallback must be identified explicitly |
| Join scale | Root/foreign cardinality and fan-out curves, indexed and forced-scan controls | pending after W3 batching |

## Work order

1. ~~W1 predicate-aware aggregate pushdown.~~ Closed.
2. ~~W3 deduplicated batch enrichment.~~ Closed.
3. ~~W2 one-execution group result pagination.~~ Closed.
4. ~~Cold/restart measurement and clean-open attribution.~~ Baseline complete; startup no-op I/O fixed.
5. Cold-query durable semantic projection / parse-avoidance delta.
6. Million-document scale lane on Bonzo.
7. Remote server/client lane.
