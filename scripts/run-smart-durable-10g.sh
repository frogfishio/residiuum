#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 /absolute/dedicated/empty/campaign-root" >&2
  exit 2
fi

campaign_root="$1"
if [[ "$campaign_root" != /* ]]; then
  echo "campaign root must be absolute" >&2
  exit 2
fi
if [[ -e "$campaign_root" ]] && [[ -n "$(find "$campaign_root" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "campaign root must be empty: $campaign_root" >&2
  exit 2
fi

workspace_root="$(cd "$(dirname "$0")/.." && pwd)"
campaign_commit="$(git -C "$workspace_root" rev-parse HEAD)"
campaign_log="${campaign_root}.campaign.log"

export RESIDIUUM_SMART_DURABLE_ROOT="$campaign_root"
export RESIDIUUM_SMART_DURABLE_LOGICAL_BYTES=10737418240
export RESIDIUUM_SMART_DURABLE_PAYLOAD_BYTES=8192
export RESIDIUUM_SMART_DURABLE_CONCURRENCY=20
export RESIDIUUM_SMART_DURABLE_MIN_FREE_BYTES=32212254720
export RESIDIUUM_CAMPAIGN_COMMIT="$campaign_commit"

echo "smart durable campaign"
echo "  root: $campaign_root"
echo "  logical payload: 10 GiB"
echo "  document payload: 8 KiB"
echo "  concurrency: 20"
echo "  commit: $campaign_commit"
echo "  log: $campaign_log"

cd "$workspace_root"
/usr/bin/time -l cargo test --release -p residiuum-sdk \
  --test smart_durable_campaign \
  smart_client_durable_retained_media_campaign \
  -- --ignored --exact --nocapture 2>&1 | tee "$campaign_log"

echo "report: $campaign_root/report.json"
