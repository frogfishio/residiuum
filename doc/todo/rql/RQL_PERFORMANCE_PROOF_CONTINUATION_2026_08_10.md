# RQL performance proof — parked-work continuation

Date parked: **2026-08-10**  
Status: **local 5,000-document weak-cell optimization complete; scale, remote and final Mongo proof pending**  
Primary evidence: [`RQL_MONGO_WEAK_CELL_MATRIX_2026_08_10.md`](RQL_MONGO_WEAK_CELL_MATRIX_2026_08_10.md)

This is the restart point for the RQL performance effort. Read this document
before changing query execution again. Do not reconstruct the work from chat
history and do not restart the already-closed local optimization cells.

## 1. State being parked

The working tree is on `main`, based on commit `6a80a2d`, with the RQL/store/
SDK/server changes from this tranche still uncommitted. They are intentional;
do not reset or discard the dirty tree. The exact changed-file inventory is
available from `git status --short`.

Feedback packages published from this tranche on 2026-08-10:
`residiuum-store 0.2.5`, `residiuum-sdk 0.4.0`, and
`residiuum-server 0.2.3`. A fresh external consumer resolved and compiled all
three solely from crates.io. The matching source tree still needs to be
committed and pushed for repository traceability.

The current product behavior includes:

- one-execution filtered and high-cardinality group aggregation;
- presence-aware partial JSON projection preserving missing versus null;
- a 16 MiB bounded, version- and exact-field-fenced scalar projection cache;
- clean store reopen without unchanged tier/catalog/active-segment rewrites;
- structured store-open phase and decoded-cache evidence;
- deduplicated, batched indexed enrichment;
- Full RQL ownership transfer: Core rows move into the attach pipeline rather
  than being cloned and retained twice;
- `RqlFullPage.base.rows` is intentionally empty and `base_row_count` records
  pre-attach cardinality;
- the Full wire sends `base_row_count`, not duplicate `base_rows`; the client
  accepts legacy `base_rows` during rolling upgrades and discards their bodies;
- verified get-many cache misses do not perform a redundant second probe.

Do not reintroduce `base_rows` or treat the empty `base.rows` as missing data.
It is metadata-only Core evidence by design.

## 2. Frozen local baseline

Fixture: deterministic 5,000 approximately 1 KiB documents, seven orderly
store reopens, uncontrolled OS page cache. These are store-reopen numbers, not
process-restart or device-cold claims.

| Query shape | First after reopen | Immediate repeat | First decoded misses |
|---|---:|---:|---:|
| Indexed equality | 6.43 ms | 1.45 ms | 500 |
| Compound equality/range | 9.25 ms | 1.67 ms | 1,000 |
| Deep nested scan | 35.18 ms | 8.93 ms | 5,000 |
| Indexed top-10 | 4.27 ms | 0.576 ms | 16 |
| Plain grouped count | 28.18 ms | 2.67 ms | 0 |
| Five covered aggregates | 4.75 ms | 1.06 ms | 0 |
| Full result materialisation | 39.23 ms | 13.97 ms | 5,000 |
| Filtered grouped count | 8.10 ms | 0.811 ms | 0 |
| High-cardinality grouped count | 28.63 ms | 6.32 ms | 0 |
| Indexed one-to-many enrichment | 43.64 ms | 14.42 ms | 6,252 |

Warm local comparison against Mongo is already competitive after the fixes.
Indexed enrichment improved from the original 83.91/35.92 ms reopen baseline
to 43.64/14.42 ms. Warm enrichment is only 0.44 ms above materialising the
same 5,000 root documents without the join.

Raw reopen evidence is generated at:

```text
target/rql-mongo-dipstick/residiuum-reopen.json
```

## 3. First actions when work resumes

1. Preserve and land the current dirty tranche before starting unrelated RQL
   changes. Review `git diff --check`, inspect the diff, then commit through the
   normal project workflow. Do not silently squash unrelated user changes.
2. Re-run the focused qualification set below. All result digests must remain
   identical before accepting a performance number.
3. Run the seven-reopen harness once and compare it with the frozen table.
   Treat machine-wide movement in all control cells as noise, not a regression.
4. Start the scale lane. Do not begin another speculative local micro-
   optimization unless a scale or remote profile identifies it.

Focused validation commands:

```sh
cargo test -p residiuum-sdk --lib
cargo test -p residiuum-sdk \
  --test rql_group_aggregate \
  --test rql_q3_adversarial \
  --test rql_q3_differential_matrix \
  --test rql_q3_page_concat \
  --test rql_q3_semantic_oracle
cargo test -p residiuum-sdk \
  --test rql_full_corpus \
  --test rql_full_enrich_index \
  --test rql_full_enrich_kickoff \
  --test rql_full_many_facade
cargo test -p residiuum-server --features dangerous-key-export \
  --test hp007_connect_heap
```

The server test binds loopback sockets and may require execution outside a
restricted sandbox. The most recent result was 252 relevant passing checks.

Reopen baseline command:

```sh
cargo run --release -p residiuum-rql-qual \
  --features residiuum-embedded \
  --example game_reopen_dipstick
```

## 4. Remaining work, in order

### P1 — million-document embedded scale lane

Build a dedicated streaming scale runner; do not enlarge the existing JSON
fixture in memory. Generate and insert one document at a time and exclude load
and index construction from query timings.

Run exactly these logical shapes over 1,000,000 approximately 1 KiB documents:

1. global count;
2. selective indexed equality;
3. selective compound equality/range;
4. low-cardinality grouped count;
5. filtered low-cardinality grouped count;
6. high-cardinality grouped count with complete pagination;
7. covered five-aggregate query;
8. bounded top-K;
9. bounded result-page scan (do not claim that returning one million owned
   documents is an aggregate benchmark).

For each shape record three warm-ups and seven measured repetitions, p50/p95,
rows, canonical digest, examined documents/bytes, host calls, decoded and
projection cache deltas, peak RSS and result bytes. The generator and expected
answers must use constant memory. Abort rather than swap.

Use Bonzo for this lane. The configured non-interactive target is:

```sh
ssh bonzo /bin/hostname
```

Expected response: `Bonzo.local`. Bonzo is an M2 MacBook Air with roughly
200 GB free space. Before a run, verify free space and remove only the explicit
prior benchmark directory—never a broad home/workspace path. The source commit
must be pushed before asking Bonzo to run it.

Deliverable:

```text
target/rql-mongo-dipstick/residiuum-scale-1m.json
```

and a summarized scale section in the weak-cell matrix.

### P2 — real remote smart-client lane

Run the same ten weak-cell shapes through the real Residiuum server and async
smart client on localhost. This is not the legacy synchronous/raw surface.

Record:

- client-observed p50/p95;
- a no-op/ping command floor without subtracting it from headlines;
- server-side execution time when available;
- request/response bytes;
- host-call count;
- confirmation that QVM executes server-side with no client scan fallback;
- row count and canonical digest parity with embedded execution;
- Full response evidence proving `base_row_count` and absence of duplicated
  `base_rows` on the new wire.

Run Mongo separately so the two engines do not compete for memory or CPU.
Never compare an embedded Residiuum number with a remote Mongo number without
labelling that topology difference.

Deliverables:

```text
target/rql-mongo-dipstick/residiuum-remote.json
target/rql-mongo-dipstick/mongo-remote-control.json
```

### P3 — join scale and fan-out curves

Use indexed and forced-scan controls with deterministic answers. At minimum run:

- root cardinality: 10,000; 100,000; 1,000,000;
- foreign cardinality: 1%, 10% and 100% of root cardinality;
- fan-out: 0, 1, 4 and 16;
- cardinality contracts: `optional`, `exactly_one`, `many`;
- indexed candidate loading and forced full-scan loading.

Page the output and hash the concatenated logical result. Record candidate-key
count/bytes, foreign documents loaded, attached document count, peak RSS,
host calls and p50/p95. Refuse configurations that exceed the declared result
budget rather than silently truncating them.

### P4 — cold projection follow-up, only if evidence justifies it

The streaming projector avoids JSON-tree allocation but still tokenizes the
whole authoritative payload to preserve corruption visibility. If the scale or
remote profile shows cold parsing is still decisive, design a rebuildable
durable scalar projection tied to exact record versions.

Required safety properties:

- projection is derived, never authoritative;
- exact heap/collection/key/version and field-list fencing;
- missing, explicit null and present remain distinct;
- cache/projection loss falls back to authoritative verified media;
- corruption or version drift refuses the shortcut;
- rebuild/recovery reason and bytes scanned are observable;
- no false-complete coverage claim.

Do not implement this merely to reduce the first/repeat ratio. Full result
materialisation must construct independently owned values under the current
public API and is therefore a legitimate ownership floor.

### P5 — final Mongo comparison and delta decision

Re-run the matching Mongo application with identical logical data, indexes,
query answers, warm-up/repetition counts and topology labels. Publish both raw
reports and a joined table. Do not subtract TCP/ping floors from headline
latency; show them separately.

Classify every cell as:

- faster (`R/M < 0.8`);
- parity (`0.8 <= R/M <= 1.25`);
- slower (`R/M > 1.25`);
- invalid/unscored (answer, lifecycle, topology or coverage mismatch).

Any slower cell needs phase attribution and either a concrete engine task or an
explicitly accepted product limitation. Do not optimize from a ratio alone.

## 5. Evidence admission rules

A number is not admitted unless all of the following are true:

- identical canonical row count and value digest;
- load/index construction excluded and declared;
- lifecycle declared: warm, store reopen, process restart or device cold;
- topology declared: embedded, localhost remote or other;
- coverage complete with zero unaccepted holes;
- three warm-ups and seven measured repetitions unless the document explicitly
  states a stronger campaign protocol;
- p50 and p95 retained, with every raw repetition available;
- no concurrent Mongo/Residiuum run on the same machine;
- no client fallback masquerading as server execution;
- peak memory and result ownership included for scale/join cells.

Never score a warm Mongo server against a reopened Residiuum store. Store
reopen does not claim process restart or device-cold execution.

## 6. Definition of done

This effort is complete only when:

1. the 1M embedded report is reproducible on Bonzo;
2. the real remote smart-client report is reproducible;
3. join fan-out curves have bounded-memory evidence;
4. Mongo and Residiuum answers/digests match for every scored cell;
5. no scored cell is slower than 1.25× without an accepted written delta;
6. cold/reopen claims are lifecycle-honest;
7. raw reports, commands, environment identity and commit hashes are recorded;
8. the weak-cell matrix and final RQL/Mongo delta document are updated.

## 7. Suggested restart prompt

> Resume the parked RQL performance proof from
> `doc/todo/rql/RQL_PERFORMANCE_PROOF_CONTINUATION_2026_08_10.md`.
> Verify the parked tranche first, then implement and run P1, the constant-
> memory one-million-document embedded scale lane on Bonzo. Do not redo the
> closed 5,000-document local optimizations.
