#!/usr/bin/env bash
# Atomics plan §13 — evidence/CI contract.
#
# Invoke (either form; file is executable, bash always works):
#   bash scripts/verify-atomics.sh {quick,crash,model,full}
#   scripts/verify-atomics.sh {quick,crash,model,full}
#
# Writes commit-scoped run records under target/atomics-evidence/runs/
# <commit12>-<profile>.json plus a detached .sha256 sidecar (CR-R2-007).
# Labels are package-specific (CR-ATMR4-010). A clean full matrix may make a
# completed package an acceptance candidate. ATM-3 remains partial while its
# lifecycle/authority-frontier deliverable is open.
# Run-level label is the worst package label.
# Dirty or failing runs are diagnostic. Capabilities::atomics must stay false.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PROFILE="${1:-}"
usage() {
  echo "usage: $0 {quick|crash|model|full}" >&2
  exit 2
}
case "$PROFILE" in
  quick|crash|model|full) ;;
  *) usage ;;
esac

OUT_ROOT="target/atomics-evidence"
RUN_DIR="$OUT_ROOT/runs"
mkdir -p "$OUT_ROOT/atm-1" "$OUT_ROOT/atm-2" "$OUT_ROOT/atm-3" "$RUN_DIR"

fail() { echo "verify-atomics ($PROFILE): FAIL: $*" >&2; exit 1; }
ok() { echo "verify-atomics ($PROFILE): $*"; }

COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
DIRTY=false
if [[ -n "$(git status --porcelain 2>/dev/null || true)" ]]; then
  DIRTY=true
fi
TOOLCHAIN="$(rustc --version 2>/dev/null || echo rustc-missing)"
CARGO_V="$(cargo --version 2>/dev/null || echo cargo-missing)"
PLATFORM="$(uname -srm)"
SEED="${ATOMICS_EVIDENCE_SEED:-0}"
SUITE_VERSION="atm-1-atm-2-atm-3-2026-08-20"
STARTED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
STARTED_UNIX="$(date +%s)"
# Label is decided after the run from dirty + family coverage (CR-R2-007).
ACCEPTANCE="pending"

COMMANDS_JSONL="$(mktemp)"
trap 'rm -f "$COMMANDS_JSONL"' EXIT

# Fail closed: public capability must stay off.
if ! grep -q 'atomics: false' crates/residiuum-sdk/src/driver.rs; then
  fail "Capabilities::atomics must remain false (driver.rs)"
fi

ok "capability false"

run_cmd() {
  local family="$1"
  local negative="$2"
  shift 2
  local cmd="$*"
  local start end rc
  start="$(date +%s)"
  echo "+ [$family] $cmd"
  set +e
  "$@"
  rc=$?
  set -e
  end="$(date +%s)"
  python3 - "$COMMANDS_JSONL" "$family" "$negative" "$cmd" "$rc" "$start" "$end" <<'PY'
import json, sys
path, family, negative, cmd, rc, start, end = sys.argv[1:8]
rc = int(rc); start = int(start); end = int(end)
rec = {
    "family": family,
    "negative_control": negative,
    "command": cmd,
    "exit_code": rc,
    "result": "pass" if rc == 0 else "fail",
    "started_unix": start,
    "duration_s": max(0, end - start),
}
with open(path, "a", encoding="utf-8") as f:
    f.write(json.dumps(rec, separators=(",", ":")) + "\n")
PY
}

# Profiles run only scoped crate tests. Never `cargo test --workspace`.
run_enc() {
  run_cmd ATM-ENC "hostile_corpus_covers_required_families_and_refuses" \
    cargo test -p residiuum-atomics --offline --test vectors --test hostile_decode \
    --test canonical_properties --test evidence_vectors
}

run_ora() {
  run_cmd ATM-ORA "one_unit_over_limit_is_refused" \
    cargo test -p residiuum-atomics --offline --test validator_oracle --test oracle_histories
}

run_aut() {
  run_cmd ATM-AUT "cross_heap_collection_is_refused_and_produces_no_plan" \
    cargo test -p residiuum-atomics --offline --lib builder_cases
}

run_iso() {
  run_cmd ATM-ISO "second_heap_cannot_resolve_first_atomic" \
    cargo test -p residiuum-atomics --offline --test staging --test failpoints --test chunked_boundary
}

run_fmt() {
  run_cmd ATM-ENC "atomic_recovery / admit mutants" \
    cargo test -p residiuum-format --offline --test atomic_admit --test atomic_recovery
}

run_crs() {
  run_cmd ATM-CRS "negative_control_detects_a_leaked_staged_member" \
    cargo test -p residiuum-atomic-lane --offline --test crash_reopen
}

run_model_kernel() {
  run_cmd ATM-ORA "validator_is_sensitive_to_single_field_flips" \
    cargo test -p residiuum-atomics --offline --test validator_oracle --test oracle_histories --test atm0_evidence
}

# Store-owned ATM-2 proofs (CR-ATMR5-010). Scoped to Atomic staging tests,
# not residiuum-store --all-targets (pre-existing store warnings are residual).
run_store_atmr5_crash() {
  run_cmd ATM-CRS "store atomic_stage_retry" \
    cargo test -p residiuum-store --offline --test atomic_stage_retry \
    --features legacy-raw-store
  run_cmd ATM-CRS "store atomic_stage_chunks" \
    cargo test -p residiuum-store --offline --test atomic_stage_chunks \
    --features legacy-raw-store
  run_cmd ATM-CRS "store atomic_stage_io_matrix" \
    cargo test -p residiuum-store --offline --test atomic_stage_io_matrix \
    --features legacy-raw-store
}

run_store_atmr5_full() {
  run_store_atmr5_crash
  run_cmd ATM-CRS "store atomic_stage_bounded" \
    cargo test -p residiuum-store --offline --test atomic_stage_bounded \
    --features legacy-raw-store
  run_cmd ATM-CRS "store atomic_stage_classify" \
    cargo test -p residiuum-store --offline --test atomic_stage_classify \
    --features legacy-raw-store
  run_cmd ATM-CRS "store atomic_stage_coordinator" \
    cargo test -p residiuum-store --offline --test atomic_stage_coordinator \
    --features legacy-raw-store
  run_cmd ATM-CRS "store atomic_stage_prepare_authority" \
    cargo test -p residiuum-store --offline --test atomic_stage_prepare_authority \
    --features legacy-raw-store
  run_cmd ATM-CRS "store atomic_stage_seal" \
    cargo test -p residiuum-store --offline --test atomic_stage_seal \
    --features legacy-raw-store
  run_cmd ATM-CRS "store atomic_stage_status" \
    cargo test -p residiuum-store --offline --test atomic_stage_status \
    --features legacy-raw-store
  run_cmd ATM-CRS "store atomic_stage_limits" \
    cargo test -p residiuum-store --offline --test atomic_stage_limits \
    --features legacy-raw-store
  run_cmd ATM-CRS "store atomic_stage_maintenance" \
    cargo test -p residiuum-store --offline --test atomic_stage_maintenance \
    --features legacy-raw-store
  run_cmd ATM-ENC "store atomic_stage rustfmt --check" \
    rustfmt --check \
    crates/residiuum-store/src/atomic_stage.rs \
    crates/residiuum-store/src/atomic_stage_media.rs \
    crates/residiuum-store/src/atomic_stage_classify.rs \
    crates/residiuum-store/src/atomic_stage_recover.rs \
    crates/residiuum-store/src/atomic_stage_status.rs \
    crates/residiuum-store/tests/atomic_stage_invisibility.rs \
    crates/residiuum-store/tests/atomic_stage_bounded.rs \
    crates/residiuum-store/tests/atomic_stage_classify.rs \
    crates/residiuum-store/tests/atomic_stage_coordinator.rs \
    crates/residiuum-store/tests/atomic_stage_retry.rs \
    crates/residiuum-store/tests/atomic_stage_chunks.rs \
    crates/residiuum-store/tests/atomic_stage_prepare_authority.rs \
    crates/residiuum-store/tests/atomic_stage_io_matrix.rs \
    crates/residiuum-store/tests/atomic_stage_seal.rs \
    crates/residiuum-store/tests/atomic_stage_status.rs \
    crates/residiuum-store/tests/atomic_stage_limits.rs \
    crates/residiuum-store/tests/atomic_stage_maintenance.rs
}

run_atm3_store() {
  run_cmd ATM-PUB "whole-generation publication, crash, receipt and limit proofs" \
    cargo test -p residiuum-store --offline --test atomic_frontier_decision \
    --features legacy-raw-store
}

run_atm3_sdk() {
  run_cmd ATM-RDR "guarded SDK/RQL Atomic generation and receipt" \
    cargo test -p residiuum-sdk --offline --test atomic_rql_generation
}

case "$PROFILE" in
  quick)
    run_enc
    run_ora
    run_aut
    run_iso
    run_fmt
    run_atm3_store
    run_atm3_sdk
    ;;
  crash)
    run_crs
    run_iso
    run_cmd ATM-CRS "durable_chunks" \
      cargo test -p residiuum-atomic-lane --offline --test durable_chunks
    run_cmd ATM-CRS "honest_damage" \
      cargo test -p residiuum-atomic-lane --offline --test honest_damage
    run_cmd ATM-CRS "exclusive_writer" \
      cargo test -p residiuum-atomic-lane --offline --test exclusive_writer
    run_cmd ATM-CRS "exclusive_publish" \
      cargo test -p residiuum-atomic-lane --offline --test exclusive_publish
    run_cmd ATM-CRS "io_prefix_matrix" \
      cargo test -p residiuum-atomic-lane --offline --test io_prefix_matrix
    run_store_atmr5_crash
    run_atm3_store
    run_atm3_sdk
    ;;
  model)
    run_model_kernel
    run_iso
    ;;
  full)
    run_enc
    run_ora
    run_aut
    run_iso
    run_fmt
    run_crs
    run_cmd ATM-ENC "residiuum-format --all-targets" \
      cargo test -p residiuum-format --offline --all-targets
    run_cmd ATM-CRS "store envelope key migration (41/42)" \
      cargo test -p residiuum-store --offline --lib legacy_31_32
    run_cmd ATM-CRS "store atomic_stage_invisibility" \
      cargo test -p residiuum-store --offline --test atomic_stage_invisibility \
      --features legacy-raw-store
    run_store_atmr5_full
    run_atm3_store
    run_atm3_sdk
    run_cmd ATM-RES "raised_limits_are_refused" \
      cargo test -p residiuum-atomics --offline --all-targets
    run_cmd ATM-CRS "residiuum-atomic-lane --all-targets" \
      cargo test -p residiuum-atomic-lane --offline --all-targets
    run_cmd ATM-ENC "cargo fmt --check atomics/format/lane" \
      cargo fmt -p residiuum-atomics -p residiuum-format -p residiuum-atomic-lane -- --check
    run_cmd ATM-ENC "clippy -D warnings --no-deps atomics/format/lane" \
      cargo clippy -p residiuum-atomics -p residiuum-format -p residiuum-atomic-lane \
      --offline --all-targets --no-deps -- -D warnings
    ;;
esac

# Execute named negative-control tests (not grep-for-symbol). Failures are
# recorded; the assembler treats them as required evidence.
run_negatives() {
  case "$PROFILE" in
    quick|full|model)
      run_cmd ATM-ENC "executed:hostile_corpus_covers_required_families_and_refuses" \
        cargo test -p residiuum-atomics --offline --test hostile_decode -- \
        hostile_corpus_covers_required_families_and_refuses --exact
      run_cmd ATM-ORA "executed:one_unit_over_limit_is_refused" \
        cargo test -p residiuum-atomics --offline --test validator_oracle -- \
        one_unit_over_limit_is_refused --exact
      run_cmd ATM-AUT "executed:cross_heap_collection_is_refused_and_produces_no_plan" \
        cargo test -p residiuum-atomics --offline --lib \
        builder_cases::cross_heap_collection_is_refused_and_produces_no_plan -- --exact
      ;;
  esac
  case "$PROFILE" in
    crash|full|quick)
      run_cmd ATM-ISO "executed:second_heap_cannot_resolve_first_atomic" \
        cargo test -p residiuum-atomics --offline --test failpoints -- \
        second_heap_cannot_resolve_first_atomic --exact
      ;;
  esac
  case "$PROFILE" in
    crash|full)
      run_cmd ATM-CRS "executed:negative_control_detects_a_leaked_staged_member" \
        cargo test -p residiuum-atomic-lane --offline --test crash_reopen -- \
        negative_control_detects_a_leaked_staged_member --exact
      run_cmd ATM-CRS "executed:leak_negative_control_is_visible_on_each_surface" \
        cargo test -p residiuum-store --offline --test atomic_stage_invisibility \
        --features legacy-raw-store -- \
        leak_negative_control_is_visible_on_each_surface --exact
      ;;
  esac
  case "$PROFILE" in
    quick|crash|full)
      run_cmd ATM-PUB "executed:one_over_maximum_caller_plan_is_refused_before_media_append" \
        cargo test -p residiuum-store --offline --test atomic_frontier_decision \
        --features legacy-raw-store -- \
        one_over_maximum_caller_plan_is_refused_before_media_append --exact
      run_cmd ATM-PUB "executed:unbound_heap_authority_predicate_fails_closed_and_replays_after_restart" \
        cargo test -p residiuum-store --offline --test atomic_frontier_decision \
        --features legacy-raw-store -- \
        unbound_heap_authority_predicate_fails_closed_and_replays_after_restart --exact
      run_cmd ATM-PUB "executed:atomic_cohort_serializes_conflicts_and_keeps_refusals_independent" \
        cargo test -p residiuum-store --offline --test atomic_frontier_decision \
        --features legacy-raw-store -- \
        atomic_cohort_serializes_conflicts_and_keeps_refusals_independent --exact
      run_cmd ATM-PUB "executed:atomic_cohort_crash_cuts_recover_only_legal_whole_decisions" \
        cargo test -p residiuum-store --offline --test atomic_frontier_decision \
        --features legacy-raw-store -- \
        atomic_cohort_crash_cuts_recover_only_legal_whole_decisions --exact
      ;;
  esac
}
run_negatives

ENDED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
ENDED_UNIX="$(date +%s)"
DURATION_S="$((ENDED_UNIX - STARTED_UNIX))"

HANDOFF="doc/todo/atomics/ATM1_ATM2_HANDOFF_ATMR6_2026-08-20.md"
[[ -f "$HANDOFF" ]] || fail "missing package handoff $HANDOFF"

python3 - "$COMMANDS_JSONL" "$OUT_ROOT" "$PROFILE" "$COMMIT" "$DIRTY" \
  "$TOOLCHAIN" "$CARGO_V" "$PLATFORM" "$SEED" "$SUITE_VERSION" \
  "$STARTED" "$ENDED" "$DURATION_S" "$ACCEPTANCE" "$HANDOFF" <<'PY'
import hashlib, json, sys
from pathlib import Path

(
    jsonl, out_root, profile, commit, dirty_s, toolchain, cargo_v, platform,
    seed, suite, started, ended, duration_s, acceptance, handoff,
) = sys.argv[1:16]
dirty = dirty_s.lower() == "true"
duration_s = int(duration_s)
out = Path(out_root)
cmds = []
if Path(jsonl).exists() and Path(jsonl).stat().st_size:
    for line in Path(jsonl).read_text(encoding="utf-8").splitlines():
        if line.strip():
            cmds.append(json.loads(line))

failed = [c for c in cmds if c["result"] != "pass"]
overall = "fail" if failed else "pass"

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()

def hash_existing(rel: str):
    p = Path(rel)
    if not p.is_file():
        return None
    return {"path": rel, "sha256": sha256_file(p)}

ATM1_FAMILIES = {"ATM-ENC", "ATM-ORA", "ATM-AUT"}
ATM1_FULL_FAMILIES = ATM1_FAMILIES | {"ATM-RES"}
ATM2_FAMILIES = {"ATM-ISO", "ATM-CRS"}
ATM3_FAMILIES = {"ATM-PUB", "ATM-RDR"}

def decide_acceptance(*, dirty, failed, required_families, passing, blockers):
    if dirty or failed:
        return "diagnostic"
    if blockers:
        return "partial"
    if not required_families.issubset(passing):
        return "partial"
    return "acceptance_candidate"

def worse(a, b):
    rank = {"diagnostic": 0, "partial": 1, "acceptance_candidate": 2}
    return a if rank[a] <= rank[b] else b

def cmd_passed(cmds, needle):
    return any(needle in c.get("command", "") and c.get("result") == "pass" for c in cmds)

atm1_required = ATM1_FULL_FAMILIES if profile == "full" else ATM1_FAMILIES
atm1_blockers = []
if profile != "full":
    atm1_blockers.append("acceptance requires the clean full profile")
if profile == "full" and not cmd_passed(cmds, "residiuum-format --offline --all-targets"):
    atm1_blockers.append("missing residiuum-format --all-targets")

atm2_blockers = []
if profile != "full":
    atm2_blockers.append("acceptance requires the clean full profile")
if profile in {"crash", "full"}:
    if not cmd_passed(cmds, "atomic_stage_retry"):
        atm2_blockers.append("missing store exact same-ID retry (CR-ATMR5-003)")
    if not cmd_passed(cmds, "atomic_stage_chunks"):
        atm2_blockers.append("missing store durable chunk prefixes (CR-ATMR5-005)")
    if not cmd_passed(cmds, "atomic_stage_io_matrix"):
        atm2_blockers.append("missing store-authority I/O prefix matrix (CR-ATMR5-009)")
if profile == "full":
    if not (cmd_passed(cmds, "durable_chunks") or cmd_passed(cmds, "residiuum-atomic-lane --offline --all-targets")):
        atm2_blockers.append("missing peer-lane durable chunk tests")
    if not (cmd_passed(cmds, "honest_damage") or cmd_passed(cmds, "residiuum-atomic-lane --offline --all-targets")):
        atm2_blockers.append("missing peer-lane honest damage tests")
    if not (cmd_passed(cmds, "exclusive_writer") or cmd_passed(cmds, "residiuum-atomic-lane --offline --all-targets")):
        atm2_blockers.append("missing writer-lock tests")
    if not (cmd_passed(cmds, "io_prefix_matrix") or cmd_passed(cmds, "residiuum-atomic-lane --offline --all-targets")):
        atm2_blockers.append("missing peer-lane I/O-phase prefix matrix")
    if not cmd_passed(cmds, "atomic_stage_invisibility"):
        atm2_blockers.append("missing store get/scan/history visibility")
    if not cmd_passed(cmds, "legacy_31_32"):
        atm2_blockers.append("missing store envelope key migration")
    if not cmd_passed(cmds, "atomic_stage_bounded"):
        atm2_blockers.append("missing store bounded catalogue (CR-ATMR5-001)")
    if not cmd_passed(cmds, "atomic_stage_classify"):
        atm2_blockers.append("missing store honest damage/conflict classifier (CR-ATMR5-002)")
    if not cmd_passed(cmds, "atomic_stage_coordinator"):
        atm2_blockers.append("missing store durable coordinator sequence (CR-ATMR5-004)")
    if not cmd_passed(cmds, "atomic_stage_prepare_authority"):
        atm2_blockers.append("missing store single prepare authority (CR-ATMR5-006)")
    if not cmd_passed(cmds, "atomic_stage_seal"):
        atm2_blockers.append("missing store persist-before-apply seal (CR-ATMR6-003)")
    if not cmd_passed(cmds, "atomic_stage_status"):
        atm2_blockers.append("missing store surviving-prepare examination (CR-ATMR6-005)")
    if not cmd_passed(cmds, "atomic_stage_limits"):
        atm2_blockers.append("missing store operable limits (CR-ATMR6-004)")
    if not cmd_passed(cmds, "atomic_stage_maintenance"):
        atm2_blockers.append("missing store maintenance fence (CR-ATMR6-006)")
    if not cmd_passed(cmds, "rustfmt --check crates/residiuum-store/src/atomic_stage.rs"):
        atm2_blockers.append("missing scoped store Atomic staging rustfmt --check")

atm3_blockers = [
    "Heap lifecycle/authority mutations and authority predicates are not yet integrated into the universal serialization frontier",
]
if not cmd_passed(cmds, "atomic_frontier_decision"):
    atm3_blockers.append("missing store ATM-3 publication/crash/receipt/resource suite")
if not cmd_passed(cmds, "atomic_rql_generation"):
    atm3_blockers.append("missing capability-bound SDK/RQL generation suite")

deferred = [
    {"family": "ATM-DMG", "result": "not_in_scope", "reason": "ATM-4 damage/material truth"},
    {"family": "ATM-RET", "result": "not_in_scope", "reason": "ATM-4 tombstone/retention"},
    {"family": "ATM-MNT", "result": "not_in_scope", "reason": "ATM-4 maintenance journeys"},
    {"family": "ATM-APP", "result": "not_in_scope", "reason": "ATM-5 async SDK / Gremlin journey"},
    {"family": "ATM-PERF", "result": "not_in_scope", "reason": "ATM-5 cost/regression disclosure"},
]

families = {}
executed_negatives = []
for c in cmds:
    fam = families.setdefault(c["family"], {
        "family": c["family"],
        "result": "pass",
        "duration_s": 0,
        "commands": [],
        "negative_controls": [],
    })
    fam["commands"].append(c["command"])
    fam["duration_s"] += c["duration_s"]
    if c["negative_control"] and c["negative_control"] not in fam["negative_controls"]:
        fam["negative_controls"].append(c["negative_control"])
    if str(c.get("negative_control", "")).startswith("executed:"):
        executed_negatives.append({
            "name": c["negative_control"],
            "command": c["command"],
            "result": c["result"],
            "exit_code": c["exit_code"],
        })
    if c["result"] != "pass":
        fam["result"] = "fail"

passing = {f for f, v in families.items() if v["result"] == "pass"}
failed = overall != "pass"
atm1_failed = any(
    c["result"] != "pass" and c["family"] in atm1_required for c in cmds
)
atm2_failed = any(
    c["result"] != "pass" and c["family"] in ATM2_FAMILIES for c in cmds
)
atm3_failed = any(
    c["result"] != "pass" and c["family"] in ATM3_FAMILIES for c in cmds
)
atm1_acceptance = decide_acceptance(
    dirty=dirty,
    failed=failed or atm1_failed,
    required_families=atm1_required,
    passing=passing,
    blockers=atm1_blockers,
)
atm2_acceptance = decide_acceptance(
    dirty=dirty,
    failed=failed or atm2_failed,
    required_families=ATM2_FAMILIES if profile in {"crash", "full", "quick"} else set(),
    passing=passing,
    blockers=atm2_blockers,
)
atm3_acceptance = decide_acceptance(
    dirty=dirty,
    failed=failed or atm3_failed,
    required_families=ATM3_FAMILIES,
    passing=passing,
    blockers=atm3_blockers,
)
acceptance = worse(worse(atm1_acceptance, atm2_acceptance), atm3_acceptance)

run = {
    "format": "residiuum-atomics-verify/2",
    "profile": profile,
    "package_suite": suite,
    "commit": commit,
    "dirty": dirty,
    "acceptance": acceptance,
    "acceptance_rule": (
        "Package-specific (CR-ATMR4-010). diagnostic = dirty or failing; "
        "ATM-1 acceptance_candidate = clean full ENC/ORA/AUT/RES + format all-targets; "
        "ATM-2 acceptance_candidate = clean full store/lane matrix; "
        "ATM-3 stays partial while lifecycle/authority integration remains; "
        "run-level label is the worst of the three packages. "
        "Run payload is hashed in a sidecar; this file never contains its own digest."
    ),
    "package_acceptance": {
        "ATM-1": atm1_acceptance,
        "ATM-2": atm2_acceptance,
        "ATM-3": atm3_acceptance,
    },
    "atm1_blockers": atm1_blockers,
    "atm2_blockers": atm2_blockers,
    "atm3_blockers": atm3_blockers,
    "toolchain": toolchain,
    "cargo": cargo_v,
    "platform": platform,
    "seed": seed,
    "started_utc": started,
    "ended_utc": ended,
    "duration_s": duration_s,
    "result": overall,
    "capabilities_atomics": False,
    "commands": cmds,
    "families": list(families.values()),
    "executed_negative_controls": executed_negatives,
    "deferred_families": deferred,
    "handoff": handoff,
}

artifacts = [
    hash_existing("scripts/verify-atomics.sh"),
    hash_existing(handoff),
    hash_existing("spec/atomics/cbor-v1.json"),
    hash_existing("crates/residiuum-format/src/envelope_keys.rs"),
    hash_existing("crates/residiuum-atomic-lane/src/lane.rs"),
    hash_existing("crates/residiuum-store/src/atomic_stage.rs"),
    hash_existing("crates/residiuum-atomics/src/outcome.rs"),
    hash_existing("crates/residiuum-store/src/heap/heap_store.rs"),
    hash_existing("crates/residiuum-sdk/tests/atomic_rql_generation.rs"),
    hash_existing("doc/todo/atomics/ATM3_PUBLICATION_ARCHITECTURE_2026-08-20.md"),
    hash_existing("crates/residiuum-atomics/src/builder.rs"),
    hash_existing("crates/residiuum-atomics/src/validate.rs"),
]
run["artifact_hashes"] = [a for a in artifacts if a]

commit12 = commit[:12] if commit != "unknown" else "unknown"
run_rel = f"runs/{commit12}-{profile}.json"
run_path = out / run_rel
run_path.write_text(json.dumps(run, indent=2) + "\n", encoding="utf-8")
sidecar = run_path.with_suffix(".sha256")
sidecar.write_text(sha256_file(run_path) + "\n", encoding="utf-8")

def same_scope(prev: dict) -> bool:
    return (
        prev.get("commit") == commit
        and prev.get("dirty") == dirty
        and prev.get("toolchain") == toolchain
        and prev.get("package_suite") == suite
    )

def merge_pack(path: Path, base: dict, new_fams: list) -> dict:
    prev = {}
    if path.is_file():
        try:
            prev = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            prev = {}
    inherit = same_scope(prev)
    by = {}
    if inherit:
        by = {f["family"]: f for f in prev.get("families", []) if "family" in f}
    for f in new_fams:
        by[f["family"]] = f
    base["families"] = list(by.values())
    profiles = list(prev.get("verify_profiles", [])) if inherit else []
    if profile not in profiles:
        profiles.append(profile)
    base["verify_profiles"] = profiles
    base["verify_result"] = overall if not inherit else (
        "fail" if prev.get("verify_result") == "fail" or overall == "fail" else overall
    )
    base["inherited_prior_run"] = inherit and bool(prev)
    return base

pack_scope = {
    "commit": commit,
    "dirty": dirty,
    "toolchain": toolchain,
    "package_suite": suite,
}

atm1 = merge_pack(out / "atm-1" / "manifest.json", {
    "package": "ATM-1",
    "title": "Canonical plan compiler and validation",
    "format": "residiuum-atomics-package/2",
    **pack_scope,
    "acceptance": atm1_acceptance,
    "acceptance_blockers": atm1_blockers,
    "capabilities_atomics": False,
    "implemented_requirements": [
        "immutable closed AtomicPlan + canonical order",
        "typed mutation/predicate encodings and accounting",
        "HeapAuthorityRevision distinct from active_rule_revisions (CR-ATM1-001)",
        "collection EncodingProfile; noncanonical integer/decimal refused (CR-ATM1-002)",
        "oracle agreement + one-unit-over limit refusals",
    ],
    "negative_controls": [
        n["name"] for n in executed_negatives if n["name"].startswith("executed:")
        and n["name"].split(":", 1)[-1] in {
            "cross_heap_collection_is_refused_and_produces_no_plan",
            "one_unit_over_limit_is_refused",
            "hostile_corpus_covers_required_families_and_refuses",
        }
    ],
    "verify_profile": profile,
    "source_run": run_rel,
    "artifact_hashes": [a for a in artifacts if a],
}, [f for f in families.values() if f["family"] in {"ATM-ENC", "ATM-ORA", "ATM-AUT", "ATM-RES"}])
(out / "atm-1" / "manifest.json").write_text(json.dumps(atm1, indent=2) + "\n", encoding="utf-8")

atm2 = merge_pack(out / "atm-2" / "manifest.json", {
    "package": "ATM-2",
    "title": "Store-owned durable evidence and invisible staging",
    "format": "residiuum-atomics-package/2",
    **pack_scope,
    "acceptance": atm2_acceptance,
    "acceptance_blockers": atm2_blockers,
    "capabilities_atomics": False,
    "implemented_requirements": [
        "format envelope registry 31–36 ownership, 37–40 Atomic (CR-ATM2-002)",
        "recovery decodes frozen AtomicPrepare/Member/Decision (CR-ATM2-003)",
        "in-memory StagingHeap binds member_hash + payload (CR-ATM2-004/005)",
        "residiuum-atomic-lane file-backed prepare/member + fsync reopen (CR-ATM2-001)",
        "crash prefixes before_prepare / after_prepare / after_member_n",
        "closed member set, one store authority, exclusive publish, sidecar limits, authenticated checkpoint, durable chunks, I/O + store invisibility (CR-ATMR4)",
        "store bounded catalogue, honest classifier, exact retry, durable chunks, coordinator seq, one prepare, exclusive no-prefix-guess, incremental frontier, store I/O matrix (CR-ATMR5 labor)",
        "covered-prefix block verify, persist-before-apply seal, operable limits, examine projection, format freeze + fail-closed maintenance, crash-media I/O matrix (CR-ATMR6 labor)",
    ],
    "authoritative_files": [
        "coordinator.log (BatchPrepare frames, sync_all after append)",
        "shard-XXXXXXXX.log (ItemEvent member frames, sync_all after append)",
        "intent/<atomic_id> (frozen members; synced before prepare)",
        "payload/<atomic_id>-<ord> (value bytes; synced before member frame)",
        "sealed/<atomic_id> (first stable boundary after log sync_all)",
    ],
    "negative_controls": [
        n["name"] for n in executed_negatives if n["name"].startswith("executed:")
        and n["name"].split(":", 1)[-1] in {
            "negative_control_detects_a_leaked_staged_member",
            "second_heap_cannot_resolve_first_atomic",
            "leak_negative_control_is_visible_on_each_surface",
        }
    ],
    "not_store": False,
    "verify_profile": profile,
    "source_run": run_rel,
    "artifact_hashes": [a for a in artifacts if a],
}, [f for f in families.values() if f["family"] in {"ATM-ISO", "ATM-CRS", "ATM-ENC"}])
(out / "atm-2" / "manifest.json").write_text(json.dumps(atm2, indent=2) + "\n", encoding="utf-8")

atm3 = merge_pack(out / "atm-3" / "manifest.json", {
    "package": "ATM-3",
    "title": "Durable decision and whole-generation publication",
    "format": "residiuum-atomics-package/2",
    **pack_scope,
    "acceptance": atm3_acceptance,
    "acceptance_blockers": atm3_blockers,
    "capabilities_atomics": False,
    "implemented_requirements": [
        "one monotonic per-Heap commit position per committed Atomic",
        "validation at the guarded live Heap frontier",
        "two authoritative stable boundaries independent of member count",
        "all-or-none point, scan, history and capability-bound RQL visibility",
        "committed-before-publish reconstruction and five-cut crash prefix proof",
        "universal ATORD1 ordering with later ordinary writes and deletes",
        "O(member-count) primary/history/locator publication",
        "exact committed/not-committed outcomes and per-member CAS versions",
        "serial independent-outcome cohorts sharing one member and one decision boundary",
        "maximum 256-caller-member execution plus one-over pre-media refusal",
    ],
    "negative_controls": [
        n["name"] for n in executed_negatives if n["name"].startswith("executed:")
        and n["name"].split(":", 1)[-1] in {
            "one_over_maximum_caller_plan_is_refused_before_media_append",
            "unbound_heap_authority_predicate_fails_closed_and_replays_after_restart",
            "atomic_cohort_serializes_conflicts_and_keeps_refusals_independent",
            "atomic_cohort_crash_cuts_recover_only_legal_whole_decisions",
        }
    ],
    "verify_profile": profile,
    "source_run": run_rel,
    "artifact_hashes": [a for a in artifacts if a],
}, [f for f in families.values() if f["family"] in ATM3_FAMILIES])
(out / "atm-3" / "manifest.json").write_text(json.dumps(atm3, indent=2) + "\n", encoding="utf-8")

print(f"wrote {run_path}")
print(f"wrote {sidecar}")
print(f"wrote {out / 'atm-1' / 'manifest.json'}")
print(f"wrote {out / 'atm-2' / 'manifest.json'}")
print(f"wrote {out / 'atm-3' / 'manifest.json'}")
print(f"acceptance={acceptance}")
# Sidecar must match the written payload; the payload must not list itself.
digest = sidecar.read_text(encoding="utf-8").strip()
if digest != sha256_file(run_path):
    sys.exit("run sidecar hash mismatch")
if digest in run_path.read_text(encoding="utf-8"):
    sys.exit("run payload must not contain its own digest")
if dirty:
    print("DIRTY TREE: evidence is diagnostic only; not an accepted package record.")
if overall != "pass":
    sys.exit(1)
PY

ok "manifests written (dirty=$DIRTY; label is in the run record)"
ok "OK"
