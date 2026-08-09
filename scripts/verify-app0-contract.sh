#!/usr/bin/env bash
# APP-0 — application contract lock checks (CORE plan §13–§14).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

fail() { echo "verify-app0-contract: FAIL: $*" >&2; exit 1; }
ok() { echo "verify-app0-contract: $*"; }

required=(
  spec/app/v1/README.md
  spec/app/v1/error_mapping_v1.json
  spec/app/v1/plan_vectors_v1.json
  spec/app/v1/cursor_vectors_v1.json
  spec/app/v1/residuals_v1.json
  spec/heap/rpc-v1/collection_create.request.json
  spec/heap/rpc-v1/collection_create.response.json
  spec/heap/rpc-v1/rql_query.request.json
  spec/heap/rpc-v1/rql_query.response.json
  spec/heap/fixtures/collection_create.accepted.json
  spec/heap/fixtures/collection_create.rejected.json
  spec/heap/fixtures/rql_query.accepted.json
  spec/heap/fixtures/rql_query.rejected.json
  crates/residiuum-sdk/src/app_v1.rs
  crates/residiuum-sdk/tests/app0_contract_lock.rs
  doc/todo/application-baseline/CORE_APPLICATION_API_IMPLEMENTATION_PLAN.md
)

for f in "${required[@]}"; do
  [[ -f "$f" ]] || fail "missing $f"
done
ok "required paths present (${#required[@]})"

command -v python3 >/dev/null 2>&1 || fail "python3 required"

python3 - <<'PY'
import json, sys
from pathlib import Path

def load(p):
    return json.loads(Path(p).read_text(encoding="utf-8"))

# JSON parses
for p in [
    "spec/app/v1/error_mapping_v1.json",
    "spec/app/v1/plan_vectors_v1.json",
    "spec/app/v1/cursor_vectors_v1.json",
    "spec/heap/rpc-v1/collection_create.request.json",
    "spec/heap/rpc-v1/collection_create.response.json",
    "spec/heap/rpc-v1/rql_query.request.json",
    "spec/heap/rpc-v1/rql_query.response.json",
    "spec/heap/fixtures/collection_create.accepted.json",
    "spec/heap/fixtures/collection_create.rejected.json",
    "spec/heap/fixtures/rql_query.accepted.json",
    "spec/heap/fixtures/rql_query.rejected.json",
]:
    load(p)

ops = load("spec/heap/operations-v1.json")
by_id = {o["id"]: o for o in ops["operations"]}
# APP-1 activates collection_create (106); APP-7 activates rql_query (118).
o106 = by_id.get(106)
if not o106:
    sys.exit("missing operation 106")
if o106.get("wire_name") != "collection_create":
    sys.exit(f"op 106 wire_name expected collection_create, got {o106.get('wire_name')}")
if o106.get("status") != "active":
    sys.exit(f"op 106 must be active after APP-1 (got {o106.get('status')})")
if not o106.get("request_schema") or not o106.get("response_schema"):
    sys.exit("active op 106 must have non-null schema pointers")
o118 = by_id.get(118)
if not o118:
    sys.exit("missing operation 118")
if o118.get("wire_name") != "rql_query":
    sys.exit(f"op 118 wire_name expected rql_query, got {o118.get('wire_name')}")
if o118.get("status") != "active":
    sys.exit(f"op 118 must be active after APP-7 (got {o118.get('status')})")
if not o118.get("request_schema") or not o118.get("response_schema"):
    sys.exit("active op 118 must have non-null schema pointers")

em = load("spec/app/v1/error_mapping_v1.json")
if not em.get("required_error_codes"):
    sys.exit("error_mapping missing required_error_codes")
if not em.get("mappings"):
    sys.exit("error_mapping missing mappings")

plans = load("spec/app/v1/plan_vectors_v1.json")
if plans.get("profile") != "rql-plan-v1":
    sys.exit("plan profile must be rql-plan-v1")
if len(plans.get("vectors") or []) < 3:
    sys.exit("need ≥3 plan vectors")

cursors = load("spec/app/v1/cursor_vectors_v1.json")
if cursors.get("profile") != "residiuum-cursor-v1":
    sys.exit("cursor profile must be residiuum-cursor-v1")
fields = set(cursors.get("fields_required") or [])
for req in ("plan_hash", "mac", "heap_id", "collection_id"):
    if req not in fields:
        sys.exit(f"cursor fields_required missing {req}")

create = load("spec/heap/fixtures/collection_create.accepted.json")
if create.get("op") != 106 or create.get("ok") is not True:
    sys.exit("collection_create.accepted malformed")
rql = load("spec/heap/fixtures/rql_query.accepted.json")
if rql.get("op") != 118 or rql.get("ok") is not True:
    sys.exit("rql_query.accepted malformed")

src = Path("crates/residiuum-sdk/src/app_v1.rs").read_text(encoding="utf-8")
for needle in (
    "pub struct HeapClient",
    "pub struct CollectionClient",
    "pub struct QueryPage",
    "residiuum-rust-app-v1",
    "rql-app-core-v1",
):
    if needle not in src:
        sys.exit(f"app_v1.rs missing {needle!r}")

print("verify-app0-contract: JSON + registry + surface OK")
PY

ok "APP-0 contract lock checks passed"
