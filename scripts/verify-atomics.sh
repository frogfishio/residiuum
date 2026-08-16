#!/usr/bin/env bash
# Atomics plan §13 — evidence/CI contract.
#
#   scripts/verify-atomics.sh {quick,crash,model,full}
#
# Writes machine-readable run + package manifests under
# target/atomics-evidence/. A dirty working tree is diagnostic only and cannot
# be the accepted package record. Capabilities::atomics must stay false.
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
mkdir -p "$OUT_ROOT/atm-1" "$OUT_ROOT/atm-2" "$RUN_DIR"

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
SUITE_VERSION="atm-1-atm-2-cr-2026-08-16"
STARTED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
STARTED_UNIX="$(date +%s)"
ACCEPTANCE="accepted_candidate"
if [[ "$DIRTY" == true ]]; then
  ACCEPTANCE="diagnostic_only"
fi

COMMANDS_JSONL="$(mktemp)"
trap 'rm -f "$COMMANDS_JSONL"' EXIT

# Fail closed: public capability must stay off.
if ! grep -q 'atomics: false' crates/residiuum-sdk/src/driver.rs; then
  fail "Capabilities::atomics must remain false (driver.rs)"
fi

# Negative controls must remain in-tree (dead-control detector).
needles=(
  "fn hostile_corpus_covers_required_families_and_refuses"
  "fn one_unit_over_limit_is_refused"
  "fn validator_is_sensitive_to_single_field_flips"
  "fn cross_heap_collection_is_refused_and_produces_no_plan"
  "fn noncanonical_integer_key_refuses_before_prepare"
  "fn negative_control_detects_a_leaked_staged_member"
  "fn second_heap_cannot_resolve_first_atomic"
)
for n in "${needles[@]}"; do
  if ! grep -R -l --include='*.rs' -F "$n" crates/residiuum-atomics crates/residiuum-atomic-lane >/dev/null; then
    fail "missing negative control: $n"
  fi
done
ok "capability false + negative-control needles present"

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
    cargo test -p residiuum-atomics --offline --test builder
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

case "$PROFILE" in
  quick)
    run_enc
    run_ora
    run_aut
    run_iso
    run_fmt
    ;;
  crash)
    run_crs
    run_iso
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
    run_cmd ATM-RES "raised_limits_are_refused" \
      cargo test -p residiuum-atomics --offline --all-targets
    run_cmd ATM-CRS "residiuum-atomic-lane --all-targets" \
      cargo test -p residiuum-atomic-lane --offline --all-targets
    ;;
esac

ENDED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
ENDED_UNIX="$(date +%s)"
DURATION_S="$((ENDED_UNIX - STARTED_UNIX))"

HANDOFF="doc/todo/atomics/ATM1_ATM2_HANDOFF_2026-08-16.md"
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

deferred = [
    {"family": "ATM-DMG", "result": "not_in_scope", "reason": "ATM-4 damage/material truth"},
    {"family": "ATM-RET", "result": "not_in_scope", "reason": "ATM-4 tombstone/retention"},
    {"family": "ATM-MNT", "result": "not_in_scope", "reason": "ATM-4 maintenance journeys"},
    {"family": "ATM-APP", "result": "not_in_scope", "reason": "ATM-5 async SDK / Gremlin journey"},
    {"family": "ATM-PERF", "result": "not_in_scope", "reason": "ATM-5 cost/regression disclosure"},
]

families = {}
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
    if c["result"] != "pass":
        fam["result"] = "fail"

run = {
    "format": "residiuum-atomics-verify/1",
    "profile": profile,
    "package_suite": suite,
    "commit": commit,
    "dirty": dirty,
    "acceptance": acceptance,
    "acceptance_rule": (
        "Dirty-tree evidence is diagnostic only and cannot be the accepted package record."
    ),
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
    "deferred_families": deferred,
    "handoff": handoff,
}

artifacts = [
    hash_existing("scripts/verify-atomics.sh"),
    hash_existing(handoff),
    hash_existing("spec/atomics/cbor-v1.json"),
    hash_existing("crates/residiuum-format/src/envelope_keys.rs"),
    hash_existing("crates/residiuum-atomic-lane/src/lane.rs"),
    hash_existing("crates/residiuum-atomics/src/builder.rs"),
    hash_existing("crates/residiuum-atomics/src/validate.rs"),
]
run["artifact_hashes"] = [a for a in artifacts if a]

run_path = out / "runs" / f"{profile}.json"
run_path.write_text(json.dumps(run, indent=2) + "\n", encoding="utf-8")

def merge_pack(path: Path, base: dict, new_fams: list) -> dict:
    if path.is_file():
        try:
            prev = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            prev = {}
        by = {f["family"]: f for f in prev.get("families", []) if "family" in f}
        for f in new_fams:
            by[f["family"]] = f
        base["families"] = list(by.values())
        profiles = list(prev.get("verify_profiles", []))
        if prev.get("verify_profile") and prev["verify_profile"] not in profiles:
            profiles.append(prev["verify_profile"])
        if profile not in profiles:
            profiles.append(profile)
        base["verify_profiles"] = profiles
        if prev.get("verify_result") == "fail" or overall == "fail":
            base["verify_result"] = "fail"
        else:
            base["verify_result"] = overall
    else:
        base["families"] = new_fams
        base["verify_profiles"] = [profile]
        base["verify_result"] = overall
    return base

atm1 = merge_pack(out / "atm-1" / "manifest.json", {
    "package": "ATM-1",
    "title": "Canonical plan compiler and validation",
    "format": "residiuum-atomics-package/1",
    "commit": commit,
    "dirty": dirty,
    "acceptance": acceptance,
    "capabilities_atomics": False,
    "implemented_requirements": [
        "immutable closed AtomicPlan + canonical order",
        "typed mutation/predicate encodings and accounting",
        "HeapAuthorityRevision distinct from active_rule_revisions (CR-ATM1-001)",
        "collection EncodingProfile; noncanonical integer/decimal refused (CR-ATM1-002)",
        "oracle agreement + one-unit-over limit refusals",
    ],
    "negative_controls": [
        "cross_heap_collection_is_refused_and_produces_no_plan",
        "noncanonical_integer_key_refuses_before_prepare",
        "one_unit_over_limit_is_refused",
        "validator_is_sensitive_to_single_field_flips",
    ],
    "verify_profile": profile,
    "artifact_hashes": [a for a in artifacts if a],
}, [f for f in families.values() if f["family"] in {"ATM-ENC", "ATM-ORA", "ATM-AUT", "ATM-RES"}])
(out / "atm-1" / "manifest.json").write_text(json.dumps(atm1, indent=2) + "\n", encoding="utf-8")

atm2 = merge_pack(out / "atm-2" / "manifest.json", {
    "package": "ATM-2",
    "title": "Evidence and invisible staging (prototype / peer crate)",
    "format": "residiuum-atomics-package/1",
    "commit": commit,
    "dirty": dirty,
    "acceptance": acceptance,
    "capabilities_atomics": False,
    "implemented_requirements": [
        "format envelope registry 31–36 ownership, 37–40 Atomic (CR-ATM2-002)",
        "recovery decodes frozen AtomicPrepare/Member/Decision (CR-ATM2-003)",
        "in-memory StagingHeap binds member_hash + payload (CR-ATM2-004/005)",
        "residiuum-atomic-lane file-backed prepare/member + fsync reopen (CR-ATM2-001)",
        "crash prefixes before_prepare / after_prepare / after_member_n",
    ],
    "authoritative_files": [
        "coordinator.log (BatchPrepare frames, sync_all after append)",
        "shard-XXXXXXXX.log (ItemEvent member frames, sync_all after append)",
        "intent/<atomic_id> (frozen members; synced before prepare)",
        "payload/<atomic_id>-<ord> (value bytes; synced before member frame)",
        "sealed/<atomic_id> (first stable boundary after log sync_all)",
    ],
    "negative_controls": [
        "negative_control_detects_a_leaked_staged_member",
        "second_heap_cannot_resolve_first_atomic",
    ],
    "not_store": True,
    "verify_profile": profile,
    "artifact_hashes": [a for a in artifacts if a],
}, [f for f in families.values() if f["family"] in {"ATM-ISO", "ATM-CRS", "ATM-ENC"}])
(out / "atm-2" / "manifest.json").write_text(json.dumps(atm2, indent=2) + "\n", encoding="utf-8")

# Point the run at the written pack hashes.
for name in ("atm-1/manifest.json", "atm-2/manifest.json", f"runs/{profile}.json"):
    p = out / name
    run["artifact_hashes"].append({"path": str(p).replace("\\", "/"), "sha256": sha256_file(p)})
run_path.write_text(json.dumps(run, indent=2) + "\n", encoding="utf-8")

print(f"wrote {run_path}")
print(f"wrote {out / 'atm-1' / 'manifest.json'}")
print(f"wrote {out / 'atm-2' / 'manifest.json'}")
if dirty:
    print("DIRTY TREE: evidence is diagnostic only; not an accepted package record.")
if overall != "pass":
    sys.exit(1)
PY

ok "manifests written (acceptance=$ACCEPTANCE dirty=$DIRTY)"
ok "OK"