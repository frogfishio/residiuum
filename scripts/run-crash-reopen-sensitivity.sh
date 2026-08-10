#!/usr/bin/env bash
# Repeated real-SIGKILL qualification for embedded durable restart behavior.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 /absolute/dedicated/empty/campaign-root" >&2
  exit 2
fi

campaign_root="$1"
if [[ "$campaign_root" != /* ]] || [[ "$campaign_root" == "/" ]]; then
  echo "campaign root must be a narrow absolute path" >&2
  exit 2
fi
if [[ -e "$campaign_root" ]] && [[ -n "$(find "$campaign_root" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "campaign root must be empty: $campaign_root" >&2
  exit 2
fi

workspace_root="$(cd "$(dirname "$0")/.." && pwd)"
write_log="${campaign_root}.write.log"
summary="${campaign_root}.reopen.log"
store="$campaign_root/store"

cd "$workspace_root"
cargo test --release -p residiuum-sdk --test smart_durable_campaign --no-run
cargo build --release -p residiuum-store --features legacy-raw-store \
  --example crash_reopen_report

test_binary="$(find target/release/deps -type f -perm -111 \
  -name 'smart_durable_campaign-*' -print | head -n 1)"
if [[ -z "$test_binary" ]]; then
  echo "smart durable campaign executable not found" >&2
  exit 2
fi

export RESIDIUUM_SMART_DURABLE_ROOT="$campaign_root"
export RESIDIUUM_SMART_DURABLE_LOGICAL_BYTES=10737418240
export RESIDIUUM_SMART_DURABLE_PAYLOAD_BYTES=8192
export RESIDIUUM_SMART_DURABLE_CONCURRENCY=512
export RESIDIUUM_SMART_DURABLE_CLIENT_BATCH=8
export RESIDIUUM_SMART_DURABLE_QUEUE_CAPACITY=4096
export RESIDIUUM_SMART_DURABLE_QUEUE_BYTE_CAPACITY=67108864
export RESIDIUUM_SMART_DURABLE_MIN_FREE_BYTES=32212254720

"$test_binary" --ignored --exact smart_client_durable_retained_media_campaign \
  --nocapture >"$write_log" 2>&1 &
writer_pid=$!

reached=0
for _ in $(seq 1 6000); do
  if grep -q 'acknowledged_payload_gib=2' "$write_log" 2>/dev/null; then
    reached=1
    break
  fi
  if ! kill -0 "$writer_pid" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
if [[ "$reached" -ne 1 ]]; then
  echo "writer did not reach the 2 GiB kill boundary" >&2
  tail -n 40 "$write_log" >&2 || true
  kill -KILL "$writer_pid" 2>/dev/null || true
  wait "$writer_pid" 2>/dev/null || true
  exit 1
fi
kill -KILL "$writer_pid"
wait "$writer_pid" 2>/dev/null || true

: >"$summary"
run_probe() {
  local number="$1"
  local disposition="$2"
  local probe_log="${campaign_root}.probe-${number}.log"
  if [[ "$disposition" == "kill" ]]; then
    target/release/examples/crash_reopen_report "$store" "$number" --hold \
      >"$probe_log" 2>&1 &
    local probe_pid=$!
    for _ in $(seq 1 1200); do
      if grep -q '^open_ns=' "$probe_log" 2>/dev/null; then
        break
      fi
      if ! kill -0 "$probe_pid" 2>/dev/null; then
        break
      fi
      sleep 0.05
    done
    if ! grep -q '^open_ns=' "$probe_log" 2>/dev/null; then
      echo "probe $number did not report" >&2
      cat "$probe_log" >&2 || true
      kill -KILL "$probe_pid" 2>/dev/null || true
      wait "$probe_pid" 2>/dev/null || true
      exit 1
    fi
    kill -KILL "$probe_pid"
    wait "$probe_pid" 2>/dev/null || true
  else
    target/release/examples/crash_reopen_report "$store" "$number" \
      >"$probe_log" 2>&1
  fi
  printf 'probe_%s_%s ' "$number" "$disposition" >>"$summary"
  tr '\n' ' ' <"$probe_log" >>"$summary"
  printf '\n' >>"$summary"
}

# Two consecutive unclean recovery sessions, then one healing close and a
# final clean-open control.
run_probe 1 kill
run_probe 2 kill
run_probe 3 close
run_probe 4 close

echo "write_log=$write_log"
echo "summary=$summary"
cat "$summary"
