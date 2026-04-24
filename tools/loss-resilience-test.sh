#!/bin/bash
# Resilience test harness for the consumer's gap-detection / recovery paths.
# Drives the replayer with packet-loss injection scenarios and captures logs.
#
# Usage: tools/loss-resilience-test.sh <pcap-prefix> [<duration-seconds>]
# Optional env: OUT=/tmp/loss-validation (override output directory)
# Example: tools/loss-resilience-test.sh pcap/20250331_MBO_084_EQT 20
#
# Outputs to ${OUT}/{scenario}.log (default /tmp/loss-validation/).
# Summary printed to stdout.
set -u
PREFIX="${1:?usage: $0 <pcap-prefix> [<duration-seconds>]}"
DUR="${2:-20}"
OUT="${OUT:-/tmp/loss-validation}"
MODE="${MODE:-PerSymbol}"
BIN="dotnet src/B3.Umdf.ConsoleApp/bin/Release/net10.0/B3.Umdf.ConsoleApp.dll"
mkdir -p "$OUT"

echo "## per-symbol recovery (unified)  output=$OUT"

run() {
  local name="$1"; shift
  echo "=== $name ==="
  timeout -k 5 "$DUR" $BIN --pcap-prefix "$PREFIX" --speed 0 "$@" > "$OUT/$name.log" 2>&1 || true
  echo "  warnings:        $(grep -c 'warn:' "$OUT/$name.log" || true)"
  echo "  state transitions: $(grep -c '→' "$OUT/$name.log" || true)"
  echo "  gaps detected:   $(grep -c 'Gap detected' "$OUT/$name.log" || true)"
  echo "  recovery overflow: $(grep -c 'recovery queue overflow' "$OUT/$name.log" || true)"
  local last_overflow=$(grep 'recovery queue overflow' "$OUT/$name.log" | tail -1 | grep -oE 'dropped [0-9]+' || echo "")
  [ -n "$last_overflow" ] && echo "  total recovery drops: $last_overflow"
  local final_state=$(grep -E '→ (RealTime|Recovery|CatchUp|WaitSnapshot)' "$OUT/$name.log" | tail -1 | grep -oE '→ \w+' || echo "→ ?")
  echo "  final state:     $final_state"
  if [ "$MODE" = "PerSymbol" ]; then
    echo "  channel gaps absorbed: $(grep -c 'absorbed channel gap' "$OUT/$name.log" || true)"
    local last_per=$(grep 'PerSymbol:' "$OUT/$name.log" | tail -1)
    [ -n "$last_per" ] && echo "  last per-symbol stats:$(echo "$last_per" | sed -E 's/.*PerSymbol://')"
  fi
}

# Baseline + 6 loss scenarios. Seeds fixed for reproducibility.
run "00_baseline_no_loss"
run "01_loss_A_only_5pct"      --loss-targets A  --loss-rate 0.05  --loss-seed 42
run "02_loss_B_only_5pct"      --loss-targets B  --loss-rate 0.05  --loss-seed 42
run "03_loss_AB_indep_2pct"    --loss-targets AB --loss-rate 0.02  --loss-seed 42
run "04_loss_AB_corr_1pct"     --loss-targets AB --loss-rate 0.01  --loss-correlated --loss-seed 42
run "05_loss_burst50_corr"     --loss-targets AB --loss-rate 0.005 --loss-mode burst --loss-burst 50 --loss-correlated --loss-seed 42
run "06_loss_AB_corr_tiny"     --loss-targets AB --loss-rate 0.00001 --loss-correlated --loss-seed 42

echo
echo "Logs in $OUT/"
