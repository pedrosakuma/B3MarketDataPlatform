#!/usr/bin/env bash
# Replay-driven staged late-join FIX vs fresh WS snapshot validation harness.
#
# Starts one long-lived ConsoleApp replay, then launches multiple sequential
# FIX validator connections at staggered offsets to prove late joiners receive
# a correct current MarketDataSnapshotFullRefresh and continue cleanly from
# that state.
#
# Usage:
#   tools/fix-conflated-late-join-validate.sh <pcap-prefix> [duration-seconds] [symbol] [start-delay-seconds]
#
# Optional env:
#   OUT=artifacts/fix-conflated-late-join-validate/<name>
#   WS_PORT=18080
#   FIX_PORT=19200
#   SPEED=1
#   CHECK_INTERVAL_MS=5000
#   JOIN_PERCENTAGES=10,50,90
#   PER_CLIENT_RUN_SECONDS=12
#   FRESH_WS_COMPARE=0
#   STARTUP_TIMEOUT_SECONDS=60
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PREFIX="${1:?usage: $0 <pcap-prefix> [duration-seconds] [symbol] [start-delay-seconds]}"
DURATION_SECONDS="${2:-60}"
REQUESTED_SYMBOL="${3:-${SYMBOL:-}}"
START_DELAY_SECONDS="${4:-0}"
OUT="${OUT:-artifacts/fix-conflated-late-join-validate/$(date +%Y%m%d-%H%M%S)}"
HOST="${HOST:-127.0.0.1}"
WS_PORT="${WS_PORT:-18080}"
FIX_PORT="${FIX_PORT:-19200}"
SPEED="${SPEED:-1}"
CHECK_INTERVAL_MS="${CHECK_INTERVAL_MS:-5000}"
JOIN_PERCENTAGES="${JOIN_PERCENTAGES:-10,50,90}"
PER_CLIENT_RUN_SECONDS="${PER_CLIENT_RUN_SECONDS:-12}"
STARTUP_TIMEOUT_SECONDS="${STARTUP_TIMEOUT_SECONDS:-60}"
FRESH_WS_COMPARE="${FRESH_WS_COMPARE:-0}"

APP_DLL="src/B3.Umdf.ConsoleApp/bin/Release/net10.0/B3.Umdf.ConsoleApp.dll"
BUILD_LOG="$OUT/build.log"
CONSUMER_LOG="$OUT/consumer.log"
SUMMARY_LOG="$OUT/summary.log"

CONSUMER_PID=""

mkdir -p "$OUT"

cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  if [ -n "${CONSUMER_PID:-}" ] && kill -0 "$CONSUMER_PID" 2>/dev/null; then
    kill -9 "$CONSUMER_PID" 2>/dev/null || true
    wait "$CONSUMER_PID" 2>/dev/null || true
  fi
  exit "$rc"
}
trap cleanup EXIT INT TERM

echo "## fix-conflated late-join validate prefix=$PREFIX duration=${DURATION_SECONDS}s out=$OUT" | tee "$SUMMARY_LOG"

if [ ! -f "$APP_DLL" ]; then
  echo "[build] $APP_DLL missing; building Release ConsoleApp" | tee -a "$SUMMARY_LOG"
  dotnet build src/B3.Umdf.ConsoleApp/B3.Umdf.ConsoleApp.csproj -c Release >"$BUILD_LOG" 2>&1
fi

if ! node -e "require.resolve('ws', { paths: ['tools/ws'] })" >/dev/null 2>&1; then
  echo "[error] missing Node dependency 'ws'; install with: npm install --prefix tools ws" | tee -a "$SUMMARY_LOG"
  exit 1
fi

wait_for_http() {
  local url="$1" timeout_s="$2" start_ts
  start_ts="$(date +%s)"
  while true; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    if [ $(( $(date +%s) - start_ts )) -ge "$timeout_s" ]; then
      return 1
    fi
    sleep 1
  done
}

wait_for_tcp() {
  local host="$1" port="$2" timeout_s="$3"
  python3 - "$host" "$port" "$timeout_s" <<'PY'
import socket, sys, time
host = sys.argv[1]
port = int(sys.argv[2])
timeout_s = int(sys.argv[3])
deadline = time.time() + timeout_s
while time.time() < deadline:
    try:
        with socket.create_connection((host, port), timeout=1):
            sys.exit(0)
    except OSError:
        time.sleep(1)
sys.exit(1)
PY
}

discover_symbol() {
  local response discovered
  response="$(curl -fsS "http://$HOST:$WS_PORT/symbols?limit=20")"
  discovered="$(printf '%s' "$response" | python3 -c 'import json, sys; data=json.load(sys.stdin); syms=data.get("symbols") or []; want="CPLE3"; print(want if want in syms else (syms[0] if syms else ""))')"
  printf '%s\n' "$discovered"
}

echo "[start] launching ConsoleApp (ws=$WS_PORT fix=$FIX_PORT speed=$SPEED)" | tee -a "$SUMMARY_LOG"
UMDF_FIX_CONFLATED_ENABLED=true \
UMDF_FIX_CONFLATED_PORT="$FIX_PORT" \
dotnet "$APP_DLL" \
  --pcap-prefix "$PREFIX" \
  --ws-port "$WS_PORT" \
  --speed "$SPEED" \
  >"$CONSUMER_LOG" 2>&1 &
CONSUMER_PID=$!

wait_for_http "http://$HOST:$WS_PORT/ready" "$STARTUP_TIMEOUT_SECONDS" || { echo "[error] HTTP not ready" | tee -a "$SUMMARY_LOG"; exit 1; }
wait_for_tcp "$HOST" "$FIX_PORT" "$STARTUP_TIMEOUT_SECONDS" || { echo "[error] FIX not ready" | tee -a "$SUMMARY_LOG"; exit 1; }

SYMBOL_TO_USE="$REQUESTED_SYMBOL"
if [ -z "$SYMBOL_TO_USE" ]; then
  SYMBOL_TO_USE="$(discover_symbol)"
fi
if [ -z "$SYMBOL_TO_USE" ]; then
  echo "[error] failed to discover symbol" | tee -a "$SUMMARY_LOG"
  exit 1
fi

echo "[ready] symbol=$SYMBOL_TO_USE joinPercentages=$JOIN_PERCENTAGES startDelay=${START_DELAY_SECONDS}s" | tee -a "$SUMMARY_LOG"
if [ "$FRESH_WS_COMPARE" = "1" ]; then
  echo "[mode] fresh WS comparison enabled" | tee -a "$SUMMARY_LOG"
else
  echo "[mode] snapshot/incremental-only validation (fresh WS compare disabled)" | tee -a "$SUMMARY_LOG"
fi

if [ "$START_DELAY_SECONDS" -gt 0 ]; then
  echo "[wait] initial start delay ${START_DELAY_SECONDS}s" | tee -a "$SUMMARY_LOG"
  sleep "$START_DELAY_SECONDS"
fi

IFS=',' read -r -a percentages <<< "$JOIN_PERCENTAGES"
last_offset=0
overall_rc=0

for pct in "${percentages[@]}"; do
  pct="${pct// /}"
  offset=$(( DURATION_SECONDS * pct / 100 ))
  sleep_seconds=$(( offset - last_offset ))
  if [ "$sleep_seconds" -gt 0 ]; then
    echo "[wait] sleeping ${sleep_seconds}s until ${pct}% join point" | tee -a "$SUMMARY_LOG"
    sleep "$sleep_seconds"
  fi
  last_offset="$offset"

  client_log="$OUT/client-${pct}.log"
  echo "[stage] join=${pct}% elapsed=${offset}s symbol=$SYMBOL_TO_USE" | tee -a "$SUMMARY_LOG"
  set +e
  RUN_SECONDS="$PER_CLIENT_RUN_SECONDS" \
  CHECK_INTERVAL_MS="$CHECK_INTERVAL_MS" \
  HTTP_BASE="http://$HOST:$WS_PORT" \
  WS_SNAPSHOT_COMPARE_URL="${FRESH_WS_COMPARE:+ws://$HOST:$WS_PORT/ws}" \
  WS_SNAPSHOT_TIMEOUT_MS=15000 \
  node tools/fix/fix-validate.mjs "$HOST" "$FIX_PORT" "$SYMBOL_TO_USE" >"$client_log" 2>&1
  rc=$?
  set -e

  result_line="$(grep -E '^(✅ MATCH|❌ MISMATCH) \| fresh-ws ' "$client_log" | tail -1 || true)"
  snapshot_line="$(grep -E '^Snapshot loaded for ' "$client_log" | tail -1 || true)"
  if [ "$FRESH_WS_COMPARE" = "1" ] && [ "$rc" -eq 0 ] && [ -n "$result_line" ]; then
    echo "[pass] join=${pct}% $result_line" | tee -a "$SUMMARY_LOG"
  elif [ "$FRESH_WS_COMPARE" != "1" ] && [ "$rc" -eq 0 ] && [ -n "$snapshot_line" ]; then
    echo "[pass] join=${pct}% ${snapshot_line#Snapshot loaded for }" | tee -a "$SUMMARY_LOG"
  else
    overall_rc=2
    echo "[fail] join=${pct}% rc=$rc ${result_line:-${snapshot_line:-'(no verdict)'}}" | tee -a "$SUMMARY_LOG"
  fi
done

if kill -0 "$CONSUMER_PID" 2>/dev/null; then
  kill -9 "$CONSUMER_PID" 2>/dev/null || true
  wait "$CONSUMER_PID" 2>/dev/null || true
fi

echo "Logs:" | tee -a "$SUMMARY_LOG"
echo "  consumer $CONSUMER_LOG" | tee -a "$SUMMARY_LOG"
echo "  summary  $SUMMARY_LOG" | tee -a "$SUMMARY_LOG"

exit "$overall_rc"
