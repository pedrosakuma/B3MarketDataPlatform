#!/usr/bin/env bash
# Replay-driven FIX vs WireV2 validation harness.
#
# Starts the ConsoleApp directly against one PCAP prefix, enables the opt-in
# FIX Conflated listener, runs the existing WS and FIX validators in parallel,
# then compares their final client-side reconstructed book summaries.
#
# Usage:
#   tools/fix-conflated-replay-validate.sh <pcap-prefix> [duration-seconds] [symbol]
# Optional env:
#   OUT=artifacts/fix-replay/<name>  override output directory
#   WS_PORT=8080                     WebSocket/HTTP port for the host
#   FIX_PORT=9200                    FIX Conflated TCP port
#   SPEED=0                          replay speed (0=max, 1=real-time)
#   CHECK_INTERVAL_MS=5000           validator summary/check interval
#   ORDER_TOLERANCE=0                allowed abs delta for bid/ask order counts
#   LEVEL_TOLERANCE=0                allowed abs delta for bid/ask level counts
#   PRICE_TOLERANCE=0                allowed abs delta for best bid/ask prices
#
# Example:
#   tools/fix-conflated-replay-validate.sh pcap/20250331_MBO_084_EQT 25 PETR4
#
# Exit codes:
#   0 = WS and FIX final summaries match within tolerance
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
ORDER_TOLERANCE="${ORDER_TOLERANCE:-0}"
LEVEL_TOLERANCE="${LEVEL_TOLERANCE:-0}"
PRICE_TOLERANCE="${PRICE_TOLERANCE:-0}"
STARTUP_TIMEOUT_SECONDS="${STARTUP_TIMEOUT_SECONDS:-60}"
VALIDATOR_SHUTDOWN_TIMEOUT_SECONDS="${VALIDATOR_SHUTDOWN_TIMEOUT_SECONDS:-15}"

APP_DLL="src/B3.Umdf.ConsoleApp/bin/Release/net10.0/B3.Umdf.ConsoleApp.dll"
CONSUMER_LOG="$OUT/consumer.log"
WS_LOG="$OUT/ws-validate.log"
FIX_LOG="$OUT/fix-validate.log"
BUILD_LOG="$OUT/build.log"
COMPARE_LOG="$OUT/compare.log"

CONSUMER_PID=""
WS_PID=""
FIX_PID=""

mkdir -p "$OUT"

cleanup() {
  local rc=$?
  trap - EXIT INT TERM

  for pid in "$WS_PID" "$FIX_PID" "$CONSUMER_PID"; do
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
  echo "[error] missing Node dependency 'ws' required by tools/ws/ws-validate.mjs"
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

CHECK_INTERVAL_MS="$CHECK_INTERVAL_MS" \
node tools/ws/ws-validate.mjs "ws://$HOST:$WS_PORT/ws" "$SYMBOL_TO_USE" >"$WS_LOG" 2>&1 &
WS_PID=$!

CHECK_INTERVAL_MS="$CHECK_INTERVAL_MS" \
HTTP_BASE="http://$HOST:$WS_PORT" \
node tools/fix/fix-validate.mjs "$HOST" "$FIX_PORT" "$SYMBOL_TO_USE" >"$FIX_LOG" 2>&1 &
FIX_PID=$!

sleep "$DURATION_SECONDS"

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

validator_deadline=$(( $(date +%s) + VALIDATOR_SHUTDOWN_TIMEOUT_SECONDS ))
while { kill -0 "$WS_PID" 2>/dev/null || kill -0 "$FIX_PID" 2>/dev/null; } && [ "$(date +%s)" -lt "$validator_deadline" ]; do
  sleep 1
done

if kill -0 "$WS_PID" 2>/dev/null; then
  kill "$WS_PID" 2>/dev/null || true
fi
if kill -0 "$FIX_PID" 2>/dev/null; then
  kill "$FIX_PID" 2>/dev/null || true
fi

set +e
wait "$WS_PID"
WS_RC=$?
wait "$FIX_PID"
FIX_RC=$?
set -e

echo "[validators] ws=$WS_RC fix=$FIX_RC consumer=$CONSUMER_RC"

set +e
python3 - "$WS_LOG" "$FIX_LOG" "$ORDER_TOLERANCE" "$LEVEL_TOLERANCE" "$PRICE_TOLERANCE" "$WS_RC" "$FIX_RC" "$CONSUMER_RC" <<'PY' | tee "$COMPARE_LOG"
import math
import pathlib
import re
import sys

ws_path = pathlib.Path(sys.argv[1])
fix_path = pathlib.Path(sys.argv[2])
order_tol = float(sys.argv[3])
level_tol = float(sys.argv[4])
price_tol = float(sys.argv[5])
ws_rc = int(sys.argv[6])
fix_rc = int(sys.argv[7])
consumer_rc = int(sys.argv[8])

summary_re = re.compile(
    r"\[summary\] local=(?P<bidOrders>\d+)b/(?P<askOrders>\d+)a "
    r"(?P<bidLevels>\d+)lv/(?P<askLevels>\d+)lv "
    r"bid=(?P<bestBid>\S+) ask=(?P<bestAsk>\S+) crossed=(?P<crossed>\S+)"
)
symbol_re = re.compile(r"\[summary\].*?\| symbol=(?P<symbol>\S+)")
ws_activity_re = re.compile(r"\[summary\] local=.*? msgs=(?P<msgs>\d+) adds=(?P<adds>\d+) upd=(?P<upd>\d+) dels=(?P<dels>\d+) snaps=(?P<snaps>\d+)")
fix_activity_re = re.compile(r"\[summary\] local=.*? msgs=(?P<msgs>\d+) logons=(?P<logons>\d+) snaps=(?P<snaps>\d+) incr=(?P<incr>\d+) bookEntries=(?P<bookEntries>\d+)")

def parse(path: pathlib.Path):
    text = path.read_text(encoding="utf-8", errors="replace")
    summary_matches = summary_re.findall(text)
    if not summary_matches:
        raise RuntimeError(f"no summary line found in {path}")

    last_summary = summary_re.finditer(text)
    summary = None
    for match in last_summary:
        summary = match

    symbol = None
    for match in symbol_re.finditer(text):
        symbol = match.group("symbol")

    activity = None
    for pattern in (ws_activity_re, fix_activity_re):
        for match in pattern.finditer(text):
            activity = {k: int(v) for k, v in match.groupdict().items()}

    assert summary is not None
    return {
        "text": text,
        "symbol": symbol,
        "bidOrders": int(summary.group("bidOrders")),
        "askOrders": int(summary.group("askOrders")),
        "bidLevels": int(summary.group("bidLevels")),
        "askLevels": int(summary.group("askLevels")),
        "bestBid": float(summary.group("bestBid")),
        "bestAsk": float(summary.group("bestAsk")),
        "crossed": summary.group("crossed").lower() == "true",
        "activity": activity or {},
    }

def within(left: float, right: float, tolerance: float) -> bool:
    return math.fabs(left - right) <= tolerance

def printable(state):
    return {k: v for k, v in state.items() if k != "text"}

try:
    ws = parse(ws_path)
    fix = parse(fix_path)
except Exception as exc:
    print(f"RESULT: ERROR ({exc})")
    sys.exit(2)

print("WS :", printable(ws))
print("FIX:", printable(fix))

issues = []
if ws_rc != 0:
    issues.append(f"ws validator exited {ws_rc}")
if fix_rc != 0:
    issues.append(f"fix validator exited {fix_rc}")
if consumer_rc != 0:
    issues.append(f"consumer exited {consumer_rc}")
if ws.get("symbol") and fix.get("symbol") and ws["symbol"] != fix["symbol"]:
    issues.append(f"symbol ws={ws['symbol']} fix={fix['symbol']}")
if "SubscribeOk:" not in ws["text"]:
    issues.append("ws validator never received SubscribeOk")
if ws["activity"].get("snaps", 0) == 0 and sum(ws["activity"].get(k, 0) for k in ("adds", "upd", "dels")) == 0:
    issues.append("ws validator observed no book traffic")
if "Snapshot loaded" not in fix["text"] or fix["activity"].get("snaps", 0) == 0:
    issues.append("fix validator never loaded an initial snapshot")
if not within(ws["bidOrders"], fix["bidOrders"], order_tol):
    issues.append(f"bidOrders ws={ws['bidOrders']} fix={fix['bidOrders']}")
if not within(ws["askOrders"], fix["askOrders"], order_tol):
    issues.append(f"askOrders ws={ws['askOrders']} fix={fix['askOrders']}")
if not within(ws["bidLevels"], fix["bidLevels"], level_tol):
    issues.append(f"bidLevels ws={ws['bidLevels']} fix={fix['bidLevels']}")
if not within(ws["askLevels"], fix["askLevels"], level_tol):
    issues.append(f"askLevels ws={ws['askLevels']} fix={fix['askLevels']}")
if not within(ws["bestBid"], fix["bestBid"], price_tol):
    issues.append(f"bestBid ws={ws['bestBid']} fix={fix['bestBid']}")
if not within(ws["bestAsk"], fix["bestAsk"], price_tol):
    issues.append(f"bestAsk ws={ws['bestAsk']} fix={fix['bestAsk']}")
if ws["crossed"] != fix["crossed"]:
    issues.append(f"crossed ws={ws['crossed']} fix={fix['crossed']}")

if issues:
    print("RESULT: MISMATCH")
    for issue in issues:
        print(f"  - {issue}")
    sys.exit(2)

print("RESULT: MATCH")
sys.exit(0)
PY
COMPARE_RC=$?
set -e

echo "Logs:"
echo "  consumer  $CONSUMER_LOG"
echo "  ws        $WS_LOG"
echo "  fix       $FIX_LOG"
echo "  compare   $COMPARE_LOG"

exit "$COMPARE_RC"
