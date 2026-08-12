# Residiuum Graph machine contracts

Status: **GRF-0 / GRF-1 freeze v1**

Normative prose:
[GRF01_DEVELOPER_HANDOFF.md](../../doc/todo/graph/GRF01_DEVELOPER_HANDOFF.md)

Files:

- `cbor-v1.json` — deterministic-CBOR maps, enums, limits and hash domains;
- `adjacency-v1.json` — immutable adjacency file, manifest, validation and
  publication contract;
- `records-v1.schema.json` — canonical authoritative vertex/edge JSON schema;
- `source-analysis-v1.json` — shared oracle/SDK conformance corpus; and
- `acceptance-v1.json` — required claims, suites, artifacts and negative
  controls.

The files are normative inputs. Implementation-generated output never rewrites
them. A semantic change requires an architect-reviewed spec/profile amendment.
