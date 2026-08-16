#!/usr/bin/env bash
# DEF-090 — local mirror of the required CI quality bar (.github/workflows/ci.yml).
# Run from the repository root: ./scripts/quality.sh
#
# Optional env:
#   RESIDIUUM_QUALITY_SKIP_DENY=1   skip cargo-deny when the binary is not installed
#   RESIDIUUM_QUALITY_SKIP_DOC=1    skip cargo doc
#   RESIDIUUM_RELEASE_ALLOW_DIRTY=1 pass through to release_content.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export RUSTFLAGS="${RUSTFLAGS:--D warnings}"
export RUSTDOCFLAGS="${RUSTDOCFLAGS:--D warnings}"

echo "== delivery scoreboard (M0-3) =="
bash ./scripts/verify-delivery-status.sh

echo "== documentation structure and links =="
node ./scripts/check-doc-links.mjs

echo "== Residiuum protocol identity reset =="
node ./scripts/check-residiuum-identity.mjs

echo "== APP-0 application contract lock =="
bash ./scripts/verify-app0-contract.sh

echo "== APB-0 application baseline contract freeze =="
bash ./scripts/verify-app-baseline-contract.sh --require-frozen

echo "== crash-and-recovery contract (DEF-104) =="
bash ./scripts/verify-crash-recovery-contract.sh

echo "== atomics evidence/CI contract (plan §13, quick) =="
bash ./scripts/verify-atomics.sh quick

# CSQ-0 core-storage registries (VFY-0 namespace)
bash ./scripts/verify-core-storage-registry.sh

# PQH-0 performance qualification registries
bash ./scripts/verify-performance-registry.sh

# PQH-1 safe runner (path guard / preflight / cancel artifacts)
bash ./scripts/verify-performance-runner.sh

# CSQ-1 oracle dependency firewall
bash ./scripts/verify-csq-oracle-firewall.sh

# CSQ-2 boundary/failpoint instrumentation
bash ./scripts/verify-csq-boundary-instrumentation.sh

# CSQ-3 format exhaustive corpus
bash ./scripts/verify-csq-format-corpus.sh

# CSQ-4 store model / state machine
bash ./scripts/verify-csq-state-machine.sh

# CSQ-5 crash / filesystem campaign
bash ./scripts/verify-csq-crash-campaign.sh

# CSQ-6 chunk / large-value qualification
bash ./scripts/verify-csq-chunk-large-value.sh

# CSQ-7 damage / salvage / recovery
bash ./scripts/verify-csq-damage-salvage.sh

# CSQ-8 derived / maintenance / backup / migration
bash ./scripts/verify-csq-derived-maintenance.sh

# CSQ-9 concurrency / resources
bash ./scripts/verify-csq-concurrency-resources.sh

# CSQ-10 mutation / fuzz ownership (includes DEF-091-F property bar)
bash ./scripts/verify-csq-mutation-fuzz.sh

# CSQ-11 compatibility / packaged journey / PR-safe scale+soak seed
bash ./scripts/verify-csq-compat-scale-soak.sh

# CSQ-12 evidence bundle builder + independent A2 verifier
bash ./scripts/verify-csq-evidence-bundle.sh


echo "== fuzz property bar (DEF-091-F, no cargo-fuzz required) =="
RESIDIUUM_FUZZ_SKIP_CARGO_FUZZ=1 bash ./scripts/fuzz-smoke.sh

echo "== security process docs (DEF-063-A) =="
bash ./scripts/verify-security-process.sh

echo "== fmt =="
cargo fmt --all -- --check

echo "== clippy (strict) =="
cargo clippy --workspace --all-targets -- -D warnings

echo "== build all targets =="
cargo build --workspace --all-targets

echo "== test =="
cargo test --workspace

if [[ "${RESIDIUUM_QUALITY_SKIP_DOC:-0}" != "1" ]]; then
  echo "== doc =="
  cargo doc --workspace --no-deps --document-private-items
fi

echo "== release content (DEF-003) =="
./scripts/release_content.sh

if [[ "${RESIDIUUM_QUALITY_SKIP_DENY:-0}" == "1" ]]; then
  echo "== cargo-deny skipped (RESIDIUUM_QUALITY_SKIP_DENY=1) =="
elif command -v cargo-deny >/dev/null 2>&1; then
  echo "== cargo-deny =="
  cargo deny check --all-features
else
  echo "warning: cargo-deny not installed; install with:" >&2
  echo "  cargo install --locked cargo-deny" >&2
  echo "or set RESIDIUUM_QUALITY_SKIP_DENY=1 for a local dry-run." >&2
  exit 1
fi

echo "== DEF-091 property tests (residiuum-format) =="
cargo test -p residiuum-format --test stage_def_091_properties

echo "quality bar OK (DEF-090); DEF-091 properties exercised"