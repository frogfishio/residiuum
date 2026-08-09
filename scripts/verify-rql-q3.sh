#!/usr/bin/env bash
# RQL-Q3 one-command green: Q3.1 oracle + Q3.2 differential + Q3.3 adversarial + Q3.4 page-concat.
# Exit 0 = labor evidence only. Does NOT accept the package (principal).
#
# F8: tests write under target/rql-q3/ by default (do not rewrite checked-in spec/).
#      Publish snapshots: bash scripts/publish-rql-q3-evidence.sh
#      or RESIDIUUM_WRITE_SPEC_EVIDENCE=1 cargo test ...
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ok() { printf 'verify-rql-q3: %s\n' "$*"; }
fail() { printf 'verify-rql-q3: FAIL: %s\n' "$*" >&2; exit 1; }

need() {
  if [[ ! -f "$1" ]]; then
    fail "missing required file: $1"
  fi
}

# Prefer fresh target/ report from this run; fall back to committed spec/ snapshot.
resolve_report() {
  local name="$1"
  local target="$ROOT/target/rql-q3/$name"
  local spec="$ROOT/spec/rql/qualification/corpus-v1/$name"
  if [[ -f "$target" ]]; then
    printf '%s' "$target"
  elif [[ -f "$spec" ]]; then
    printf '%s' "$spec"
  else
    fail "missing report: $name (neither target/rql-q3/ nor committed spec/)"
  fi
}

need "$ROOT/crates/residiuum-sdk/tests/rql_q3_semantic_oracle.rs"
need "$ROOT/crates/residiuum-sdk/tests/rql_q3_differential_matrix.rs"
need "$ROOT/crates/residiuum-sdk/tests/rql_q3_adversarial.rs"
need "$ROOT/crates/residiuum-sdk/tests/rql_q3_page_concat.rs"
need "$ROOT/doc/todo/rql/RQL_Q3_1_SEMANTIC_ORACLE.md"
need "$ROOT/doc/todo/rql/RQL_Q3_2_DIFFERENTIAL_MATRIX.md"
need "$ROOT/doc/todo/rql/RQL_Q3_3_ADVERSARIAL_SUITE.md"
need "$ROOT/doc/todo/rql/RQL_Q3_4_PAGE_CONCAT.md"
need "$ROOT/spec/rql/qualification/corpus-v1/corpus-v1.json"
need "$ROOT/tools/rql_q1/materialise_fixture.py"

command -v cargo >/dev/null 2>&1 || fail "cargo required"
command -v python3 >/dev/null 2>&1 || fail "python3 required"

# Snapshot tracked evidence before tests (F8: default must not mutate them).
SPEC_BEFORE=$(
  git -C "$ROOT" status --porcelain -- \
    'spec/rql/qualification/corpus-v1/q3_*.json' \
    'spec/rql/qualification/harness-v1/q4_*.json' 2>/dev/null || true
)

ok "Q3.1 independent semantic oracle"
cargo test -p residiuum-sdk --test rql_q3_semantic_oracle

ok "Q3.2 differential matrix + metamorphic laws"
cargo test -p residiuum-sdk --test rql_q3_differential_matrix

ok "Q3.3 adversarial + damage + property"
cargo test -p residiuum-sdk --test rql_q3_adversarial

ok "Q3.4 page-concat metamorphic law"
cargo test -p residiuum-sdk --test rql_q3_page_concat

REPORT1="$(resolve_report q3_1_oracle_report.json)"
REPORT2="$(resolve_report q3_2_differential_report.json)"
REPORT3="$(resolve_report q3_3_adversarial_report.json)"
REPORT4="$(resolve_report q3_4_page_concat_report.json)"
ok "using reports: $(basename "$REPORT1") from $(dirname "$REPORT1" | sed "s|$ROOT/||")"

python3 - "$REPORT1" "$REPORT2" "$REPORT3" "$REPORT4" <<'PY'
import json, sys

def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)

r1, r2, r3, r4 = load(sys.argv[1]), load(sys.argv[2]), load(sys.argv[3]), load(sys.argv[4])
if r1.get("format") != "residiuum-rql-q3-1-oracle-report-v1":
    raise SystemExit(f"bad q3.1 format {r1.get('format')!r}")
if r2.get("format") != "residiuum-rql-q3-2-differential-report-v1":
    raise SystemExit(f"bad q3.2 format {r2.get('format')!r}")
if r3.get("format") != "residiuum-rql-q3-3-adversarial-report-v1":
    raise SystemExit(f"bad q3.3 format {r3.get('format')!r}")
if r4.get("format") != "residiuum-rql-q3-4-page-concat-report-v1":
    raise SystemExit(f"bad q3.4 format {r4.get('format')!r}")

s1, s2, s3, s4 = r1.get("summary") or {}, r2.get("summary") or {}, r3.get("summary") or {}, r4.get("summary") or {}
if int(s1.get("digest_mismatch") or 0) or int(s1.get("oracle_eval_fail") or 0):
    raise SystemExit(f"q3.1 residual fail: {s1}")
if int(s1.get("oracle_ok") or 0) < 90:
    raise SystemExit(f"q3.1 oracle_ok floor: {s1}")
if int(s1.get("tier_a_total") or 0) != 147:
    raise SystemExit(f"q3.1 Tier-A denominator drift: {s1}")
if int(s1.get("qualification_green") or 0) + int(s1.get("qualification_residual") or 0) != int(s1.get("tier_a_total") or 0):
    raise SystemExit(f"q3.1 qualification accounting mismatch: {s1}")
if int(s2.get("matrix_diverge") or 0) or int(s2.get("errors") or 0) or int(s2.get("reopen_fail") or 0):
    raise SystemExit(f"q3.2 residual fail: {s2}")
if int(s2.get("matrix_equal") or 0) < 90:
    raise SystemExit(f"q3.2 matrix_equal floor: {s2}")
if int(s2.get("tier_a_total") or 0) != 147:
    raise SystemExit(f"q3.2 Tier-A denominator drift: {s2}")
if int(s2.get("qualification_green") or 0) + int(s2.get("qualification_residual") or 0) != int(s2.get("tier_a_total") or 0):
    raise SystemExit(f"q3.2 qualification accounting mismatch: {s2}")
if int(s3.get("false_absence_defects") or 0) or int(s3.get("false_completeness_defects") or 0):
    raise SystemExit(f"q3.3 false absence/completeness: {s3}")
if int(s3.get("unresolved_divergence") or 0):
    raise SystemExit(f"q3.3 unresolved divergence: {s3}")
if int(s3.get("dimensions_covered") or 0) < 10:
    raise SystemExit(f"q3.3 dimensions floor: {s3}")
if int(s4.get("law_count") or 0) < 4:
    raise SystemExit(f"q3.4 law_count floor: {s4}")
if int(s4.get("false_absence_defects") or 0):
    raise SystemExit(f"q3.4 false absence: {s4}")

print(
    "verify-rql-q3: report ok "
    f"oracle_ok={s1.get('oracle_ok')} matrix_equal={s2.get('matrix_equal')} "
    f"unsupported={s2.get('unsupported')} "
    f"adv_dims={s3.get('dimensions_covered')} "
    f"page_concat_laws={s4.get('law_count')} "
    f"source_after_residual={s4.get('source_after_cursor_residual')}"
)
denominator_green = bool(s1.get("package_exit_ready")) and bool(s2.get("package_exit_ready"))
state = "LABOR EXIT READY / PACKAGE REVIEW" if denominator_green else "PACKAGE HOLD"
print(
    f"verify-rql-q3: {state} "
    f"oracle_green={s1.get('qualification_green')}/{s1.get('tier_a_total')} "
    f"matrix_green={s2.get('qualification_green')}/{s2.get('tier_a_total')}"
)
PY

SPEC_AFTER=$(
  git -C "$ROOT" status --porcelain -- \
    'spec/rql/qualification/corpus-v1/q3_*.json' \
    'spec/rql/qualification/harness-v1/q4_*.json' 2>/dev/null || true
)
if [[ "$SPEC_BEFORE" != "$SPEC_AFTER" ]]; then
  fail "default verify mutated tracked evidence under spec/ (F8). Before/after porcelain differs.
  set RESIDIUUM_WRITE_SPEC_EVIDENCE=1 only for explicit publish.
  before: $SPEC_BEFORE
  after:  $SPEC_AFTER"
fi

ok "PASS (Tier-A denominator green; package awaits principal review; F8 no-spec-churn)"
