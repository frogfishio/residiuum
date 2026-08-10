# RQL/Mongo game dipstick

This is an intentionally disposable order-of-magnitude check, not Q5 evidence.
It measures six warm query shapes over one identical generated fixture. Load and
index construction are excluded. Mongo timings are client-observed and include
localhost TCP/driver overhead; the report records a separate ping floor but does
not subtract it from the headline.

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
