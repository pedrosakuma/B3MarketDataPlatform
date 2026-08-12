#!/usr/bin/env bash
# Replay-driven FIX vs WireV2 validation harness.
#
# Starts the ConsoleApp directly against one PCAP prefix, enables the opt-in
# FIX Conflated listener, runs the FIX validator for the full replay window,
# then opens a fresh WebSocket Book subscribe for the same symbol and treats the
# resulting BookSnapshot/reset batch as the server's authoritative current book.
# The final verdict compares FIX-derived state against that fresh server-side
# snapshot pull (the closest available equivalent to a missing /book/{symbol}
# HTTP endpoint).
#
# Usage:
#   tools/fix-conflated-replay-validate.sh <pcap-prefix> [duration-seconds] [symbol]
# Optional env:
#   OUT=artifacts/fix-replay/<name>  override output directory
#   WS_PORT=8080                     WebSocket/HTTP port for the host
#   FIX_PORT=9200                    FIX Conflated TCP port
#   SPEED=0                          replay speed (0=max, 1=real-time)
#   CHECK_INTERVAL_MS=5000           validator summary/check interval
#
# Example:
#   tools/fix-conflated-replay-validate.sh pcap/20250331_MBO_084_EQT 25 PETR4
#
# Exit codes:
#   0 = FIX and fresh WS snapshot summaries match within tolerance
#   1 = harness / process startup failure
#   2 = validator mismatch or validator non-zero exit
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PREFIX="${1:?usage: $0 <pcap-prefix> [duration-seconds] [symbol]}"
DURATION_SECONDS="${2:-25}"
REQUESTED_SYMBOL="${3:-${SYMBOL:-}}"
OUT="${OUT:-artifacts/fix-conflated-replay-validate/$(date +%Y%m%d-%H%M%S)}"
HOST="${HOST:-127.0.0.1}"
WS_PORT="${WS_PORT:-8080}"
FIX_PORT="${FIX_PORT:-9200}"
SPEED="${SPEED:-0}"
CHECK_INTERVAL_MS="${CHECK_INTERVAL_MS:-5000}"
STARTUP_TIMEOUT_SECONDS="${STARTUP_TIMEOUT_SECONDS:-60}"
VALIDATOR_SHUTDOWN_TIMEOUT_SECONDS="${VALIDATOR_SHUTDOWN_TIMEOUT_SECONDS:-15}"

APP_DLL="src/B3.Umdf.ConsoleApp/bin/Release/net10.0/B3.Umdf.ConsoleApp.dll"
CONSUMER_LOG="$OUT/consumer.log"
FIX_LOG="$OUT/fix-validate.log"
BUILD_LOG="$OUT/build.log"
COMPARE_LOG="$OUT/compare.log"

CONSUMER_PID=""
FIX_PID=""

mkdir -p "$OUT"

cleanup() {
  local rc=$?
  trap - EXIT INT TERM

  for pid in "$FIX_PID" "$CONSUMER_PID"; do
    if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done

  exit "$rc"
}
trap cleanup EXIT INT TERM

echo "## fix-conflated replay validate  prefix=$PREFIX duration=${DURATION_SECONDS}s out=$OUT"

if [ ! -f "$APP_DLL" ]; then
  echo "[build] $APP_DLL missing; building Release ConsoleApp"
  dotnet build src/B3.Umdf.ConsoleApp/B3.Umdf.ConsoleApp.csproj -c Release >"$BUILD_LOG" 2>&1
fi

if ! node -e "require.resolve('ws', { paths: ['tools/ws'] })" >/dev/null 2>&1; then
  echo "[error] missing Node dependency 'ws' required by tools/ws/*.mjs"
  echo "        install it with: npm install --prefix tools ws"
  exit 1
fi

wait_for_http() {
  local url="$1"
  local timeout_s="$2"
  local start_ts
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
  local host="$1"
  local port="$2"
  local timeout_s="$3"
  python3 - "$host" "$port" "$timeout_s" <<'PY'
import socket
import sys
import time

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
  local url="$1"
  local timeout_s="$2"
  local start_ts
  start_ts="$(date +%s)"

  while true; do
    local response=""
    response="$(curl -fsS "$url" 2>/dev/null || true)"
    if [ -n "$response" ]; then
      local discovered=""
      discovered="$(printf '%s' "$response" | python3 -c 'import json, sys; data=json.load(sys.stdin); symbols=data.get("symbols") or []; print(symbols[0] if symbols else "")' 2>/dev/null || true)"
      if [ -n "$discovered" ]; then
        printf '%s\n' "$discovered"
        return 0
      fi
    fi

    if [ $(( $(date +%s) - start_ts )) -ge "$timeout_s" ]; then
      return 1
    fi

    sleep 1
  done
}

echo "[start] launching ConsoleApp (ws=$WS_PORT fix=$FIX_PORT speed=$SPEED)"
UMDF_FIX_CONFLATED_ENABLED=true \
UMDF_FIX_CONFLATED_PORT="$FIX_PORT" \
dotnet "$APP_DLL" \
  --pcap-prefix "$PREFIX" \
  --ws-port "$WS_PORT" \
  --speed "$SPEED" \
  >"$CONSUMER_LOG" 2>&1 &
CONSUMER_PID=$!

if ! wait_for_http "http://$HOST:$WS_PORT/ready" "$STARTUP_TIMEOUT_SECONDS"; then
  echo "[error] HTTP host did not become ready on $HOST:$WS_PORT"
  exit 1
fi

if ! wait_for_tcp "$HOST" "$FIX_PORT" "$STARTUP_TIMEOUT_SECONDS"; then
  echo "[error] FIX listener did not open on $HOST:$FIX_PORT"
  exit 1
fi

SYMBOL_TO_USE="$REQUESTED_SYMBOL"
if [ -z "$SYMBOL_TO_USE" ]; then
  if ! SYMBOL_TO_USE="$(discover_symbol "http://$HOST:$WS_PORT/symbols?limit=1" "$STARTUP_TIMEOUT_SECONDS")"; then
    echo "[error] failed to discover a symbol from /symbols"
    exit 1
  fi
fi

echo "[ready] symbol=$SYMBOL_TO_USE"

RUN_SECONDS="$DURATION_SECONDS" \
CHECK_INTERVAL_MS="$CHECK_INTERVAL_MS" \
HTTP_BASE="http://$HOST:$WS_PORT" \
WS_SNAPSHOT_COMPARE_URL="ws://$HOST:$WS_PORT/ws" \
WS_SNAPSHOT_TIMEOUT_MS="$((VALIDATOR_SHUTDOWN_TIMEOUT_SECONDS * 1000))" \
node tools/fix/fix-validate.mjs "$HOST" "$FIX_PORT" "$SYMBOL_TO_USE" >"$FIX_LOG" 2>&1 &
FIX_PID=$!

set +e
wait "$FIX_PID"
FIX_RC=$?
set -e

CONSUMER_RC=0
if kill -0 "$CONSUMER_PID" 2>/dev/null; then
  kill "$CONSUMER_PID" 2>/dev/null || true
  set +e
  wait "$CONSUMER_PID"
  killed_consumer_rc=$?
  set -e
  case "$killed_consumer_rc" in
    0|143) ;;
    *) CONSUMER_RC="$killed_consumer_rc" ;;
  esac
else
  set +e
  wait "$CONSUMER_PID"
  CONSUMER_RC=$?
  set -e
fi

echo "[validators] fix=$FIX_RC consumer=$CONSUMER_RC"

RESULT_LINE="$(grep -E '^(✅ MATCH|❌ MISMATCH) \| fresh-ws ' "$FIX_LOG" | tail -1 || true)"
if [ -z "$RESULT_LINE" ]; then
  printf 'RESULT: ERROR (no fresh-ws comparison line found in %s)\n' "$FIX_LOG" | tee "$COMPARE_LOG"
  COMPARE_RC=2
elif [ "$FIX_RC" -ne 0 ] || [ "$CONSUMER_RC" -ne 0 ]; then
  {
    printf '%s\n' "$RESULT_LINE"
    [ "$FIX_RC" -ne 0 ] && printf '  - fix validator exited %s\n' "$FIX_RC"
    [ "$CONSUMER_RC" -ne 0 ] && printf '  - consumer exited %s\n' "$CONSUMER_RC"
  } | tee "$COMPARE_LOG"
  COMPARE_RC=2
else
  printf '%s\n' "$RESULT_LINE" | tee "$COMPARE_LOG"
  COMPARE_RC=0
fi

echo "Logs:"
echo "  consumer  $CONSUMER_LOG"
echo "  fix       $FIX_LOG"
echo "  compare   $COMPARE_LOG"

exit "$COMPARE_RC"
