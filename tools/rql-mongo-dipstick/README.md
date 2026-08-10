# RQL/Mongo game dipstick

This is an intentionally disposable order-of-magnitude check, not Q5 evidence.
It measures the original six warm query shapes plus four deliberately weak
cells over one identical generated fixture. Load and index construction are
excluded. Mongo timings are client-observed and include localhost TCP/driver
overhead; the report records a separate ping floor but does not subtract it from
the headline. The weak-cell baseline and standalone lanes are recorded in
`doc/todo/rql/RQL_MONGO_WEAK_CELL_MATRIX_2026_08_10.md`.
The exact parked-work continuation order, admission rules and Bonzo procedure
are in `doc/todo/rql/RQL_PERFORMANCE_PROOF_CONTINUATION_2026_08_10.md`.

Run Residiuum first so it emits the fixture, then Mongo:

```sh
cargo run -p residiuum-rql-qual --features residiuum-embedded --example game_dipstick --release
npm --prefix tools/rql-mongo-dipstick install
node tools/rql-mongo-dipstick/mongo-dipstick.mjs \
  target/rql-mongo-dipstick/fixture.json \
  target/rql-mongo-dipstick/mongo.json
```

Defaults: 20,000 approximately 1 KiB documents, three warm-ups and twelve
measured iterations. Override with `RQL_DIPSTICK_DOCUMENTS`,
`RQL_DIPSTICK_WARMUPS` and `RQL_DIPSTICK_ITERATIONS`.

The separate store-reopen lane uses the same logical fixture and records
physical-store open phases, first query, an immediate same-connection repeat,
decoded-cache deltas and orderly close:

```sh
cargo run -p residiuum-rql-qual --features residiuum-embedded \
  --example game_reopen_dipstick --release
```

Its defaults are 5,000 documents and seven repetitions; override with
`RQL_DIPSTICK_DOCUMENTS` and `RQL_REOPEN_REPETITIONS`. The resulting
`target/rql-mongo-dipstick/residiuum-reopen.json` declares store reopen with an
uncontrolled OS page cache. It does not claim process restart or device-cold
execution and does not score a warm MongoDB server as a restart comparator.
