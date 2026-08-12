# Residiuum Product Thesis

Status: Draft v0.1

Designer-ready long-form product and landing-page source:
[TECHNICAL_SALES_MASTER_COPY.md](./TECHNICAL_SALES_MASTER_COPY.md).

## 1. The problem

Most databases behave as a single logical machine.

When their critical metadata, file structure, or storage engine is damaged,
healthy bytes may still exist but become inaccessible. Recovery commonly means
restoring an intact replica, accepting a valid prefix, or reconstructing the
database as a whole.

At the same time, storage systems tend to divide into separate categories:

- fast stores for active data;
- object stores for large data;
- archives for old data;
- search engines for finding data;
- specialist tools for recovery.

That division creates fragile handoffs and long-term dependencies. Fifteen
years later, the original application, schema, catalog, or vendor service may
be gone even though most of the underlying content survives.

## 2. The Residiuum thesis

Residiuum treats a database as a fabric of independently survivable data islands,
not one all-or-nothing object.

Its product promise is:

> Put anything in. Keep it at scale. Damage it. Find what survived.

Residiuum combines:

- a memory-store-class hot path;
- append-oriented durable ingestion;
- immutable, self-verifying storage segments;
- partition-local consensus without a global hot-path lock;
- massive tiered retention;
- catalog-independent salvage;
- SDA-based deterministic examination.

## 3. The defining difference

Other databases primarily ask:

> Can I reconstruct the database?

Residiuum first asks:

> Which pieces can I still prove are intact?

```text
ordinary failure model

critical damage → database unavailable

Residiuum failure model

DATA │ DATA │ HOLE │ DATA │ HOLE │ DATA
  ✓      ✓      ✗      ✓      ✗      ✓
```

Missing material is reported as a hole. It is not allowed to invalidate
unrelated material or to disappear from the recovery report.

## 4. Why arbitrary data matters

Valuable future data often looks worthless in the present:

- raw logs;
- abandoned application formats;
- device output;
- intermediate build artifacts;
- old documents;
- binary blobs;
- malformed imports;
- model inputs and outputs;
- data whose schema has been lost;
- data no one knows how to interpret yet.

Residiuum preserves the bytes and a small self-describing envelope first.
Understanding may be added later.

The database does not require every payload to become a document, row, fact,
or vector before it deserves durable storage.

## 5. Why massive retention matters

“Store everything” is useful only if storage remains economical and retrieval
remains possible.

Residiuum separates one logical namespace from physical location. Immutable
segments move between hot, warm, cold, and archival tiers without changing
their identities.

Hierarchical catalogs and indexes make large stores searchable. They are
replaceable accelerators, not the only map back to the data. If they disappear,
surviving segments can recreate them.

This supports two very different questions:

- “Give me this active item now.”
- “Search fifteen years of retained material for anything matching this new
  interpretation.”

## 6. Why speed matters

Dependability is not an excuse for a slow hot path.

Residiuum is built around:

- sequential appends;
- sharded writers;
- memory-resident indexes;
- immutable data structures;
- parallel readers;
- bounded independent compression and encryption;
- delayed extraction and secondary indexing;
- streaming queries.

The project targets Redis-class performance for memory-resident indexed reads,
not Oracle-style heavyweight coordination on every operation.

Claims are tied to explicit acknowledgement modes. A memory acknowledgement,
an `fsync`-equivalent durable acknowledgement, and a replicated
acknowledgement are not presented as the same benchmark.

## 7. Why SDA matters

Storage without a durable examination model merely moves the black box.

SDA gives Residiuum a small, deterministic algebra for filtering, projecting,
normalizing, validating, and transforming recovered material.

SDA can operate over streams and indexed candidates without loading the whole
store into memory. It can also represent the recovery evidence itself:

- verified data;
- partial data;
- missing data;
- corruption;
- unsupported encodings;
- uncertainty.

This allows future software to examine old data without inheriting the
behavior of the application that originally wrote it.

## 8. Why not just use another system?

### Relational databases

Relational databases excel at transactions, constraints, joins, and strongly
structured current state. They are not generally designed for catalog-free
salvage of arbitrary surviving byte islands.

### Document databases

Document databases handle flexible structured objects. Residiuum additionally
targets opaque data, independent physical survival, multi-decade tiering, and
explicit recovery evidence.

### Object storage

Object storage handles large scale and durable objects. Residiuum adds an
embedded hot path, event history, local recovery semantics, derived indexes,
and SDA examination across objects and holes.

### Redis

Redis is the performance reference for the hot working set. Residiuum adds a
storage format and recovery model intended for massive, damaged, long-lived
data. It does not claim archive reads have memory latency.

### Git

Git preserves versioned content and history. Residiuum is optimized for
high-volume ingestion, arbitrary payloads, indexed access, tiered retention,
partial physical recovery, and streaming examination.

### Backup systems

Backups restore known copies. Residiuum also salvages independently valid
material when no intact copy or catalog remains.

## 9. Honest limits

No system can recover data after every copy of its bytes has been destroyed.

Residiuum's “database that refuses to die” claim means:

- damage containment;
- independent verification;
- resynchronization after corruption;
- explicit holes;
- redundant retention;
- evidence-preserving repair;
- recovery of every surviving valid island.

It does not mean recovery from nonexistence.

## 10. Category and pitch

Category:

> Damage-tolerant universal data store

The staged competitive strategy is defined in
[COMPETITIVE_GOALS.md](./COMPETITIVE_GOALS.md): first SQLite plus loose files,
then Couchbase for edge data, then MongoDB for long-lived operational document
data.

One-sentence pitch:

> Residiuum is an extremely fast database for arbitrary, massive, long-lived
> data that recovers every intact piece after partial destruction and exposes
> the result through SDA.

Short form:

> Damage the database. Keep the data you did not destroy.

## 11. Clustering story

Residiuum does not turn a cluster into one larger fragile database.

It distributes independently meaningful partitions and immutable segments.
Consensus decides which node may order strong writes for a partition, while
the frames retain their own identity and integrity evidence.

This produces a clear failure story:

- lose a leader: its partition elects another from verified replicas;
- lose quorum: strong writes pause, surviving data remains readable or
  salvageable;
- split the network: only the quorum side commits strong writes;
- select convergent append: both sides retain uniquely identified events and
  merge explicitly;
- lose the catalog and control plane: rebuild placement from self-identifying
  node inventories and segments;
- delete half the cluster: every intact remaining frame still speaks for
  itself.

The clustering principle is:

> Consensus controls the right to write. The data remains able to speak for
> itself.

## 12. Everyday product

Damage tolerance earns trust, but it is not the only reason to install
Residiuum.

For ordinary work, Residiuum is a zero-configuration database for JSON, bytes,
events, and large retained datasets:

```text
open → put → get → find → stream
```

Collections are schemaless by default. Common filters are familiar and compile
to SDA internally. Indexes accelerate queries but do not define truth.
Embedded, server, and cluster deployments retain the same logical API.

History, verification, tiering, and salvage remain beneath the normal path
until the user asks for them or correctness requires surfacing a problem.

The DX principle is:

> Extraordinary internals. Ordinary database experience.
