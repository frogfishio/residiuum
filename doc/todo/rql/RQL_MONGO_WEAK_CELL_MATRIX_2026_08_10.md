# RQL versus MongoDB weak-cell matrix — 2026-08-10

Status: **first warm local baseline complete; W1 and W3 closed; cold, scale and remote lanes pending**

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

Required direction: one query execution must materialize or spool the bounded
group result once and page that result, or expose an equivalent continuation
state. Re-executing the source pipeline per result page is not acceptable.

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

## Remaining matrix lanes

| Lane | Exact experiment | Status |
|---|---|---|
| Cold/restart | Reopen process-local state, measure open separately from first query, record index disposition and decoded-cache misses | pending; must not compare Residiuum restart with a warm Mongo server |
| Scale | 1,000,000 streamed ~1 KiB documents; full count/scan, selective predicate, low/high-cardinality group; load excluded | pending on Bonzo; constant-memory fixture required |
| Remote | Same warm cells through the real Residiuum server and smart remote client, with localhost command floor | pending; server-side QVM versus client-side fallback must be identified explicitly |
| Join scale | Root/foreign cardinality and fan-out curves, indexed and forced-scan controls | pending after W3 batching |

## Work order

1. ~~W1 predicate-aware aggregate pushdown.~~ Closed.
2. ~~W3 deduplicated batch enrichment.~~ Closed.
3. W2 one-execution group result pagination.
4. Cold/restart lane.
5. Million-document scale lane on Bonzo.
6. Remote server/client lane.
