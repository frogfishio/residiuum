#!/usr/bin/env bash
# RQL-Q4 structural verify: Q4.1 architecture + Q4.2 datasets + Q4.3 metrics/adapters.
# Exit 0 = scaffold labor green. Not package accept / not competitive.
#
# F8: tests write under target/rql-q4/ by default (do not rewrite checked-in spec/).
#      Publish snapshots: bash scripts/publish-rql-q4-evidence.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ok() { printf 'verify-rql-q4-harness: %s\n' "$*"; }
fail() { printf 'verify-rql-q4-harness: FAIL: %s\n' "$*" >&2; exit 1; }

need() {
  if [[ ! -f "$1" ]]; then
    fail "missing required file: $1"
  fi
}

resolve_report() {
  local name="$1"
  local target="$ROOT/target/rql-q4/$name"
  local spec="$ROOT/spec/rql/qualification/harness-v1/$name"
  if [[ -f "$target" ]]; then
    printf '%s' "$target"
  elif [[ -f "$spec" ]]; then
    printf '%s' "$spec"
  else
    fail "missing report: $name (neither target/rql-q4/ nor committed spec/)"
  fi
}

need "$ROOT/crates/residiuum-rql-qual/src/lib.rs"
need "$ROOT/crates/residiuum-rql-qual/src/dataset.rs"
need "$ROOT/crates/residiuum-rql-qual/src/generator.rs"
need "$ROOT/crates/residiuum-rql-qual/src/cell_plan.rs"
need "$ROOT/crates/residiuum-rql-qual/src/lifecycle.rs"
need "$ROOT/crates/residiuum-rql-qual/src/metrics.rs"
need "$ROOT/crates/residiuum-rql-qual/src/engine.rs"
need "$ROOT/crates/residiuum-rql-qual/src/run.rs"
need "$ROOT/crates/residiuum-rql-qual/src/shared_work.rs"
need "$ROOT/doc/todo/rql/RQL_Q4_1_HARNESS_ARCHITECTURE.md"
need "$ROOT/doc/todo/rql/RQL_Q4_2_DATASET_CELLS.md"
need "$ROOT/doc/todo/rql/RQL_Q4_3_METRICS_ADAPTERS.md"
need "$ROOT/spec/rql/qualification/harness-v1/evidence-bundle-v1.schema.json"
need "$ROOT/spec/rql/qualification/corpus-v1/corpus-v1.json"

command -v cargo >/dev/null 2>&1 || fail "cargo required"
grep -q 'residiuum-rql-qual' "$ROOT/Cargo.toml" || fail "workspace member missing"

SPEC_BEFORE=$(
  git -C "$ROOT" status --porcelain -- \
    'spec/rql/qualification/harness-v1/q4_*.json' 2>/dev/null || true
)

ok "unit tests (Q4.1–Q4.3 structural, default features)"
cargo test -p residiuum-rql-qual

# F1: product adapter must not silently rot — compile + tests with feature on.
ok "residiuum-embedded feature (product adapter compile + smoke)"
cargo test -p residiuum-rql-qual --features residiuum-embedded --lib

REPORT1="$(resolve_report q4_1_architecture_report.json)"
REPORT2="$(resolve_report q4_2_dataset_cells_report.json)"
REPORT3="$(resolve_report q4_3_metrics_adapters_report.json)"
BUNDLE3="$(resolve_report q4_3_smoke_evidence_bundle.json)"
PRODUCT_CONCURRENCY="$(resolve_report q4_product_concurrency_report.json)"
PRODUCT_SCALING="$(resolve_report q4_product_scaling_report.json)"
PRODUCT_LIFECYCLE="$(resolve_report q4_product_repetition_lifecycle_report.json)"
PRODUCT_MAINTENANCE_DAMAGE="$(resolve_report q4_product_maintenance_damage_report.json)"
PRODUCT_R400_COLD="$(resolve_report q4_product_r400_cold_report.json)"
ok "using reports from $(dirname "$REPORT3" | sed "s|$ROOT/||")"

python3 - "$REPORT1" "$REPORT2" "$REPORT3" "$BUNDLE3" "$PRODUCT_CONCURRENCY" "$PRODUCT_SCALING" "$PRODUCT_LIFECYCLE" "$PRODUCT_MAINTENANCE_DAMAGE" "$PRODUCT_R400_COLD" <<'PY'
import json, sys

def load(p):
    with open(p, encoding="utf-8") as f:
        return json.load(f)

r1, r2, r3, b, pc, ps, pl, pmd, prc = (load(path) for path in sys.argv[1:10])
if r1.get("format") != "residiuum-rql-q4-1-architecture-report-v1":
    raise SystemExit(f"bad q4.1 format {r1.get('format')!r}")
if r2.get("format") != "residiuum-rql-q4-2-dataset-cells-report-v1":
    raise SystemExit(f"bad q4.2 format {r2.get('format')!r}")
if r3.get("format") != "residiuum-rql-q4-3-metrics-adapters-report-v1":
    raise SystemExit(f"bad q4.3 format {r3.get('format')!r}")
if b.get("format") != "residiuum-rql-qual-evidence-bundle-v1":
    raise SystemExit(f"bad bundle format {b.get('format')!r}")
if pc.get("format") != "residiuum-rql-q4-product-concurrency-report-v1":
    raise SystemExit(f"bad product concurrency format {pc.get('format')!r}")
if ps.get("format") != "residiuum-rql-q4-product-scaling-report-v1":
    raise SystemExit(f"bad product scaling format {ps.get('format')!r}")
if pl.get("format") != "residiuum-rql-q4-product-repetition-lifecycle-report-v1":
    raise SystemExit(f"bad product lifecycle format {pl.get('format')!r}")
if pmd.get("format") != "residiuum-rql-q4-product-maintenance-damage-report-v1":
    raise SystemExit(f"bad product maintenance/damage format {pmd.get('format')!r}")

s2 = r2.get("summary") or {}
s3 = r3.get("summary") or {}
if int(s2.get("smoke_plans") or 0) != 12:
    raise SystemExit(f"q4.2 smoke_plans {s2}")
if int(s3.get("smoke_cells") or 0) != 12:
    raise SystemExit(f"q4.3 smoke_cells {s3}")
if int(s3.get("logical_ready_with_result") or 0) != 12:
    raise SystemExit(f"q4.3 logical ready {s3}")
if s3.get("lane_s_fixture_identity") is not True:
    raise SystemExit(f"q4.3 lane_s identity {s3}")
if not b.get("content_hash"):
    raise SystemExit("bundle missing content_hash")
protocol = b.get("protocol") or {}
if protocol.get("class") != "scaffold":
    raise SystemExit(f"Q4 smoke must identify as scaffold: {protocol}")
for key in ("seed", "warmup_operations", "minimum_repetitions", "minimum_duration_ms", "minimum_operations", "engine_order_policy"):
    if key not in protocol:
        raise SystemExit(f"bundle protocol missing {key}: {protocol}")
if len(b.get("cells") or []) != 12:
    raise SystemExit(f"bundle cells {len(b.get('cells') or [])}")
if any("raw_repetitions" not in cell for cell in b.get("cells") or []):
    raise SystemExit("cell missing F14 raw_repetitions field")
pcs = pc.get("summary") or {}
if int(pcs.get("mandatory_cells") or 0) != 12 or int(pcs.get("product_ready") or 0) != 12:
    raise SystemExit(f"product concurrency coverage incomplete {pcs}")
if int(pcs.get("requested_concurrency") or 0) < 2:
    raise SystemExit(f"product concurrency did not request parallel work {pcs}")
if int(pcs.get("exact_concurrency_matches") or 0) != 12:
    raise SystemExit(f"product concurrency was not achieved exactly {pcs}")
for cell in pc.get("cells") or []:
    if cell.get("requested_concurrency") != cell.get("achieved_concurrency"):
        raise SystemExit(f"product concurrency mismatch {cell.get('plan_id')}: {cell}")
    if cell.get("one_physical_connection") is not True or cell.get("product_ready") is not True:
        raise SystemExit(f"invalid product execution proof {cell.get('plan_id')}: {cell}")
    outcome = cell.get("outcome") or {}
    if not outcome.get("result") or not outcome.get("metrics"):
        raise SystemExit(f"product proof missing result/metrics {cell.get('plan_id')}")
pss = ps.get("summary") or {}
levels = pss.get("levels") or []
if levels[:4] != [1, 2, 4, 8] or len(levels) != 5:
    raise SystemExit(f"product scaling levels incomplete {levels}")
host = ps.get("host") or {}
oversub = int(host.get("oversubscribed_concurrency") or 0)
if oversub != levels[4] or oversub <= int(host.get("available_parallelism") or 0):
    raise SystemExit(f"product scaling oversubscription dishonest host={host} levels={levels}")
for key, expected in (("matrix_rows", 60), ("expected_matrix_rows", 60), ("product_ready", 60), ("exact_concurrency_matches", 60), ("oversubscribed_rows", 12)):
    if int(pss.get(key) or 0) != expected:
        raise SystemExit(f"product scaling {key} expected={expected} summary={pss}")
seen = {(cell.get("cell_id"), cell.get("requested_concurrency")) for cell in ps.get("cells") or []}
if len(seen) != 60:
    raise SystemExit(f"product scaling matrix has duplicate/missing coordinates: {len(seen)}")
for cell in ps.get("cells") or []:
    if cell.get("requested_concurrency") != cell.get("achieved_concurrency"):
        raise SystemExit(f"product scaling mismatch {cell.get('plan_id')}")
    if cell.get("product_ready") is not True or cell.get("one_physical_connection") is not True:
        raise SystemExit(f"invalid product scaling row {cell.get('plan_id')}")
    outcome = cell.get("outcome") or {}
    if not outcome.get("result") or not outcome.get("metrics"):
        raise SystemExit(f"product scaling row missing result/metrics {cell.get('plan_id')}")
    if int(cell.get("workload_wall_ns") or 0) <= 0 or int(cell.get("workload_operations") or 0) <= 0 or float(cell.get("aggregate_ops_per_s") or 0) <= 0:
        raise SystemExit(f"product scaling row missing aggregate throughput {cell.get('plan_id')}")
pls = pl.get("summary") or {}
for key, expected in (("mandatory_cells", 12), ("repetitions_per_cell", 7), ("raw_repetitions", 84), ("stable_identity_cells", 12), ("same_deployment_reopen_cells", 12), ("same_connection_warmup_cells", 12), ("larger_dataset_cells", 12), ("larger_dataset_document_count", 256), ("larger_dataset_scale_factor", 4)):
    if int(pls.get(key) or 0) != expected:
        raise SystemExit(f"product lifecycle {key} expected={expected} summary={pls}")
if pls.get("claims_device_cold") is not False or pls.get("claims_larger_than_memory") is not False:
    raise SystemExit(f"product lifecycle made false cold/memory claim {pls}")
if int(pls.get("resource_probe_rows") or 0) != 24:
    raise SystemExit(f"product lifecycle resource probe row count {pls}")
rss_present = int(pls.get("rss_snapshots_present") or 0)
if not 0 <= rss_present <= 24:
    raise SystemExit(f"product lifecycle invalid RSS count {pls}")
expected_rss_status = "best_effort_snapshot" if rss_present else "unavailable_in_campaign_environment"
if rss_present:
    expected_rss_status = "in_process_interval_end"
if pls.get("rss_probe_status") != expected_rss_status:
    raise SystemExit(f"product lifecycle resource probe honesty failure {pls}")
if int(pls.get("peak_rss_samples_present") or 0) != 24 or pls.get("peak_rss_probe_status") != "sampled_1ms_in_process":
    raise SystemExit(f"product lifecycle peak RSS sampling incomplete {pls}")
if int(pls.get("physical_io_deltas_present") or 0) != 24 or pls.get("physical_io_probe_status") != "in_process_interval_delta":
    raise SystemExit(f"product lifecycle physical I/O deltas incomplete {pls}")
if int(pls.get("cpu_time_samples_present") or 0) != 24 or pls.get("cpu_time_probe_status") != "process_cpu_clock_interval_including_sampler":
    raise SystemExit(f"product lifecycle CPU clock evidence incomplete {pls}")
if int(pls.get("logical_byte_counters_present") or 0) != 24 or int(pls.get("read_amplification_present") or 0) != 24 or pls.get("read_amplification_probe_status") != "physical_over_vm_logical_bytes":
    raise SystemExit(f"product lifecycle logical-byte/amplification evidence incomplete {pls}")
for cell in pl.get("repeated_cells") or []:
    reps = cell.get("repetitions") or []
    if len(reps) != 7 or any(not rep.get("valid") for rep in reps):
        raise SystemExit(f"invalid raw repetitions {cell.get('plan_id')}")
    for key in ("result_digest", "query_hash", "qvm_hash", "index_config_hash"):
        if len({rep.get(key) for rep in reps}) != 1 or not reps[0].get(key):
            raise SystemExit(f"raw identity drift/missing {key} {cell.get('plan_id')}")
    if any(int(rep.get("operations") or 0) <= 0 or int(rep.get("duration_ns") or 0) <= 0 for rep in reps):
        raise SystemExit(f"raw repetition missing operations/duration {cell.get('plan_id')}")
for cell in pl.get("larger_dataset_cells") or []:
    if cell.get("claims_larger_than_memory") is not False or int(cell.get("document_count") or 0) != 256:
        raise SystemExit(f"dishonest larger dataset row {cell.get('plan_id')}")
    outcome = cell.get("outcome") or {}
    if outcome.get("status") != "ready" or not outcome.get("result") or not outcome.get("metrics"):
        raise SystemExit(f"larger dataset row not product ready {cell.get('plan_id')}")
for outcome in [cell.get("last_outcome") or {} for cell in pl.get("repeated_cells") or []] + [cell.get("outcome") or {} for cell in pl.get("larger_dataset_cells") or []]:
    resource = ((outcome.get("metrics") or {}).get("resource") or {})
    if int(resource.get("rss_bytes") or 0) <= 0 or int(resource.get("peak_rss_bytes") or 0) < int(resource.get("rss_bytes") or 0):
        raise SystemExit(f"resource RSS/peak invalid {resource}")
    if resource.get("physical_bytes_read") is None or resource.get("physical_bytes_written") is None:
        raise SystemExit(f"resource physical I/O delta missing {resource}")
    path = ((outcome.get("metrics") or {}).get("path") or {})
    if int(path.get("logical_bytes_examined") or 0) <= 0:
        raise SystemExit(f"logical-byte counter missing {path}")
    if resource.get("read_amplification") is None or float(resource.get("read_amplification")) < 0:
        raise SystemExit(f"read amplification missing/invalid {resource}")
    if int(resource.get("cpu_time_ns") or 0) <= 0:
        raise SystemExit(f"resource CPU interval missing/invalid {resource}")

pmds = pmd.get("summary") or {}
for key, expected in (("maintenance_cells", 12), ("maintenance_digest_stable", 12), ("declared_damage_cells", 1), ("false_complete_damage_outcomes", 0)):
    if int(pmds.get(key) or 0) != expected:
        raise SystemExit(f"product maintenance/damage {key} expected={expected} summary={pmds}")
maintenance = pmd.get("maintenance_cells") or []
if len(maintenance) != 12:
    raise SystemExit(f"product maintenance cell count {len(maintenance)}")
for cell in maintenance:
    if cell.get("operator_boundary") != "public_residiuum_store_maintenance_setup":
        raise SystemExit(f"maintenance operator boundary missing {cell.get('plan_id')}")
    if cell.get("before_result_digest") != cell.get("after_result_digest"):
        raise SystemExit(f"maintenance result drift {cell.get('plan_id')}")
    compact = cell.get("compaction") or {}
    if compact.get("phase") == "inactive" or int(compact.get("bytes_read") or 0) <= 0 or int(compact.get("bytes_written") or 0) <= 0:
        raise SystemExit(f"maintenance compaction not exercised {cell.get('plan_id')}: {compact}")
damage = pmd.get("declared_damage") or {}
if int(damage.get("damaged_item_frames") or 0) <= 0 or int(damage.get("damaged_bytes") or 0) <= 0:
    raise SystemExit(f"declared damage injection missing {damage}")
execution = damage.get("execution") or {}
if execution.get("classification") != "partial_survivors_incomplete_coverage":
    raise SystemExit(f"declared damage did not return honest survivors {execution}")
outcome = execution.get("outcome") or {}
result = outcome.get("result") or {}
metrics = outcome.get("metrics") or {}
rows = int(result.get("row_count") or 0)
if outcome.get("status") != "ready" or rows <= 0 or rows >= int(damage.get("baseline_rows") or 0):
    raise SystemExit(f"declared damage survivor count invalid {execution}")
if result.get("coverage_complete") is not False or metrics.get("coverage_complete") is not False:
    raise SystemExit(f"declared damage falsely complete {execution}")
strict = damage.get("strict_coverage_execution") or {}
if strict.get("classification") not in ("fail_closed_error", "fail_closed_outcome"):
    raise SystemExit(f"strict damage query did not fail closed {strict}")
memory = pmd.get("memory_admission") or {}
if memory.get("executed") is not False or memory.get("claims_larger_than_memory") is not False:
    raise SystemExit(f"memory campaign made unsupported claim {memory}")
if memory.get("status") not in ("refused_host_memory_unavailable", "refused_resource_admission", "refused_filesystem_probe_unavailable", "admitted_external_campaign_required"):
    raise SystemExit(f"memory admission status invalid {memory}")

if prc.get("format") != "residiuum-rql-q4-product-r400-cold-report-v1":
    raise SystemExit(f"bad R400/cold report format {prc.get('format')!r}")
r400 = prc.get("r400") or {}
if r400.get("streaming_loader") is not True:
    raise SystemExit(f"R400 campaign lacks constant-memory loader {r400}")
if r400.get("executed") is True:
    if r400.get("claims_larger_than_memory") is not True or int(r400.get("scanned_logical_bytes") or 0) < int(r400.get("required_r400_bytes") or 0):
        raise SystemExit(f"executed R400 campaign lacks complete scan evidence {r400}")
elif r400.get("claims_larger_than_memory") is not False or r400.get("status") != "refused_resource_admission":
    raise SystemExit(f"R400 refusal/claim dishonest {r400}")
cold = prc.get("device_cold") or {}
if cold.get("executed") is True:
    if cold.get("claims_device_cold") is not True or cold.get("status") != "executed_after_successful_page_cache_drop" or int(cold.get("warmup_operations") or -1) != 0:
        raise SystemExit(f"device-cold execution dishonest {cold}")
elif cold.get("claims_device_cold") is not False or not str(cold.get("status", "")).startswith("refused_page_cache_drop_"):
    raise SystemExit(f"device-cold refusal/claim dishonest {cold}")

print(
    "verify-rql-q4-harness: report ok "
    f"q4.2 smoke={s2.get('smoke_plans')} "
    f"q4.3 cells={s3.get('smoke_cells')} ready={s3.get('logical_ready_with_result')} "
    f"lane_s={s3.get('lane_s_fixture_identity')} "
    f"product_concurrency={pcs.get('product_ready')}/{pcs.get('mandatory_cells')}@{pcs.get('requested_concurrency')} "
    f"product_scaling={pss.get('product_ready')}/{pss.get('matrix_rows')} levels={levels} "
    f"raw_repetitions={pls.get('raw_repetitions')} larger={pls.get('larger_dataset_cells')}x{pls.get('larger_dataset_scale_factor')} "
    f"maintenance={pmds.get('maintenance_digest_stable')}/{pmds.get('maintenance_cells')} damage_survivors={rows} "
    f"r400={r400.get('status')} cold={cold.get('status')} "
    f"bundle_hash={str(b.get('content_hash'))[:12]}…"
)
PY

SPEC_AFTER=$(
  git -C "$ROOT" status --porcelain -- \
    'spec/rql/qualification/harness-v1/q4_*.json' 2>/dev/null || true
)
if [[ "$SPEC_BEFORE" != "$SPEC_AFTER" ]]; then
  fail "default verify mutated tracked harness evidence under spec/ (F8).
  before: $SPEC_BEFORE
  after:  $SPEC_AFTER"
fi

ok "SCAFFOLD PASS / PACKAGE HOLD (no comparator campaign; not competitive; F8 no-spec-churn)"
