#!/usr/bin/env bash
# Explicit publish: rewrite checked-in Q4 harness evidence under spec/.
# Default verify/tests do NOT do this (F8).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export RESIDIUUM_WRITE_SPEC_EVIDENCE=1
printf 'publish-rql-q4-evidence: writing target/ + spec/ (RESIDIUUM_WRITE_SPEC_EVIDENCE=1)\n'
cargo test -p residiuum-rql-qual fingerprint_capture_and_bundle_write --lib
cargo test -p residiuum-rql-qual write_q4_2_report --lib
cargo test -p residiuum-rql-qual publish_evidence_bundle --lib
cargo test -p residiuum-rql-qual --features residiuum-embedded product_concurrency_all_mandatory_cells_and_report --lib
cargo test -p residiuum-rql-qual --features residiuum-embedded publish_product_scaling_campaign --lib -- --ignored --nocapture
cargo test -p residiuum-rql-qual --features residiuum-embedded publish_product_repetition_lifecycle_campaign --lib -- --ignored --nocapture
cargo test -p residiuum-rql-qual --features residiuum-embedded publish_product_maintenance_damage_campaign --lib -- --ignored --nocapture
cargo test -p residiuum-rql-qual --features residiuum-embedded publish_product_r400_cold_campaign --lib -- --ignored --nocapture
printf 'publish-rql-q4-evidence: done — review git diff under spec/rql/qualification/harness-v1/\n'
