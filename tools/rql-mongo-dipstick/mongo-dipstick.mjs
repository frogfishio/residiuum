import { readFile, writeFile, mkdir } from "node:fs/promises";
import { createHash } from "node:crypto";
import { MongoClient } from "mongodb";

const fixturePath = process.argv[2] ?? "target/rql-mongo-dipstick/fixture.json";
const reportPath = process.argv[3] ?? "target/rql-mongo-dipstick/mongo.json";
const uri = process.env.MONGO_URI ?? "mongodb://127.0.0.1:27017/?directConnection=true";
const warmups = Number(process.env.RQL_DIPSTICK_WARMUPS ?? 3);
const iterations = Number(process.env.RQL_DIPSTICK_ITERATIONS ?? 12);
const fixture = JSON.parse(await readFile(fixturePath, "utf8"));
const sourceDocs = Object.entries(fixture.collections.docs).map(([key, value]) => {
  const copy = structuredClone(value);
  delete copy._key;
  return { _id: key, ...copy };
});

function normalize(value) {
  if (Array.isArray(value)) return value.map(normalize);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).filter(k => k !== "$key").sort()
      .map(k => [k, normalize(value[k])]));
  }
  return value;
}

function canonical(rows, orderSensitive, groupKey) {
  let pairs = rows.map(row => {
    const copy = structuredClone(row);
    const mongoKey = copy._id;
    delete copy._id;
    const keyValue = groupKey ? copy[groupKey] : mongoKey;
    const key = groupKey ? JSON.stringify(keyValue) : String(keyValue);
    return [key, JSON.stringify(normalize(copy))];
  });
  if (!orderSensitive) pairs = pairs.sort((a, b) =>
    a[0].localeCompare(b[0]) || a[1].localeCompare(b[1]));
  const blob = pairs.map(([key, value]) => `${key}\t${value}\n`).join("");
  return {
    row_count: pairs.length,
    values_digest: createHash("sha256").update(blob).digest("hex"),
  };
}

function percentile(sorted, p) {
  if (!sorted.length) return 0;
  return sorted[Math.round((sorted.length - 1) * p)];
}

const client = new MongoClient(uri, { maxPoolSize: 4, minPoolSize: 1 });
await client.connect();
const db = client.db("residiuum_rql_game_dipstick");
await db.dropDatabase();
const docs = db.collection("docs");
for (let at = 0; at < sourceDocs.length; at += 1000) {
  await docs.insertMany(sourceDocs.slice(at, at + 1000), { ordered: true });
}
await docs.createIndex({ sel_bucket: 1 }, { name: "by_sel_bucket" });
await docs.createIndex({ region: 1, amount: 1 }, { name: "by_region_amount" });

const cases = [
  {
    name: "indexed_equality", orderSensitive: false, groupKey: null,
    run: () => docs.find({ sel_bucket: "HIT" }).sort({ _id: 1 }).toArray(),
  },
  {
    name: "compound_range", orderSensitive: false, groupKey: null,
    run: () => docs.find({ region: "r0", amount: { $gte: 100, $lt: 500 } }).sort({ _id: 1 }).toArray(),
  },
  {
    name: "nested_scan", orderSensitive: false, groupKey: null,
    run: () => docs.find({ "nested.l1.l2.l3.flag": true }).sort({ _id: 1 }).toArray(),
  },
  {
    name: "deterministic_topk", orderSensitive: true, groupKey: null,
    run: () => docs.find({}).sort({ score: -1, _id: 1 }).limit(10).toArray(),
  },
  {
    name: "group_count", orderSensitive: false, groupKey: "status",
    run: () => docs.aggregate([
      { $group: { _id: "$status", count: { $sum: 1 } } },
      { $project: { _id: 0, status: "$_id", count: 1 } },
    ]).toArray(),
  },
  {
    name: "aggregate_five", orderSensitive: false, groupKey: "region",
    run: () => docs.aggregate([
      { $group: { _id: "$region", count: { $sum: 1 }, sum: { $sum: "$amount" }, min: { $min: "$amount" }, max: { $max: "$amount" }, avg: { $avg: "$amount" } } },
      { $project: { _id: 0, region: "$_id", count: 1, sum: 1, min: 1, max: 1, avg: 1 } },
    ]).toArray(),
  },
];

const pingNs = [];
for (let i = 0; i < 50; i++) {
  const started = process.hrtime.bigint();
  await db.command({ ping: 1 });
  pingNs.push(Number(process.hrtime.bigint() - started));
}
pingNs.sort((a, b) => a - b);

const queries = [];
for (const query of cases) {
  for (let i = 0; i < warmups; i++) await query.run();
  const latencyNs = [];
  let rows = [];
  for (let i = 0; i < Math.max(1, iterations); i++) {
    const started = process.hrtime.bigint();
    rows = await query.run();
    latencyNs.push(Number(process.hrtime.bigint() - started));
  }
  const sorted = [...latencyNs].sort((a, b) => a - b);
  queries.push({
    name: query.name,
    warmups,
    iterations: Math.max(1, iterations),
    latency_ns: latencyNs,
    p50_ns: percentile(sorted, 0.50),
    p95_ns: percentile(sorted, 0.95),
    first_key: rows.length ? String(query.groupKey ? JSON.stringify(rows[0][query.groupKey]) : rows[0]._id) : null,
    first_value: rows.length ? normalize(Object.fromEntries(Object.entries(rows[0]).filter(([key]) => key !== "_id"))) : null,
    ...canonical(rows, query.orderSensitive, query.groupKey),
  });
}

const report = {
  format: "residiuum-rql-game-dipstick-v1",
  engine: "mongodb_local_node_driver",
  mongodb_version: (await db.admin().serverInfo()).version,
  node_version: process.version,
  document_count: sourceDocs.length,
  fixture_hash: fixture.content_hash,
  localhost_ping_p50_ns: percentile(pingNs, 0.50),
  localhost_ping_p95_ns: percentile(pingNs, 0.95),
  queries,
};
await mkdir(new URL("../../target/rql-mongo-dipstick/", import.meta.url), { recursive: true });
await writeFile(reportPath, JSON.stringify(report, null, 2));
await client.close();
console.log(reportPath);
