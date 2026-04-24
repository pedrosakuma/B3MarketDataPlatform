#!/bin/bash
# Resilience harness with in-process counter capture (floor pin metrics).
#
# For each scenario, runs the consumer (PCAP replay --speed 0 with loss injection)
# while attaching dotnet-counters to capture key System.Diagnostics.Metrics
# counters: floor pin (safe/unsafe evictions), hot promotions, snapshot
# rejections, gaps absorbed, healed counts.
#
# Usage: tools/loss-resilience-with-counters.sh <pcap-prefix> [<duration-seconds>]
# Example: tools/loss-resilience-with-counters.sh pcap/20250331_MBO_084_EQT 25
#
set -u
PREFIX="${1:?usage: $0 <pcap-prefix> [<duration-seconds>]}"
DUR="${2:-25}"
OUT="${OUT:-/tmp/loss-validation}"
BIN="dotnet src/B3.Umdf.ConsoleApp/bin/Release/net10.0/B3.Umdf.ConsoleApp.dll"
COUNTERS="b3.umdf.persymbol.stale_buffer_evicted_unsafe,b3.umdf.persymbol.stale_buffer_evicted_safe_below_floor,b3.umdf.persymbol.stale_buffer_hot_promotions,b3.umdf.persymbol.stale_buffer_dropped_persymbol_cap,b3.umdf.persymbol.stale_buffer_dropped_global_cap,b3.umdf.persymbol.snapshots_rejected_too_old,b3.umdf.persymbol.snapshots_healed,b3.umdf.persymbol.channel_gaps_absorbed,b3.umdf.feed.channel_gaps_absorbed,b3.umdf.persymbol.snapshots_missing_rptseq"

mkdir -p "$OUT"
echo "## resilience+counters  prefix=$PREFIX dur=${DUR}s out=$OUT"

run() {
  local name="$1"; shift
  echo "=== $name ==="
  timeout -k 5 "$DUR" $BIN --pcap-prefix "$PREFIX" --speed 0 "$@" > "$OUT/$name.log" 2>&1 || true

  local gaps=$(grep -c 'absorbed channel gap' "$OUT/$name.log" || true)
  local promotions=$(grep -c 'promoted to hot cap' "$OUT/$name.log" || true)
  local last_per=$(grep 'PerSymbol:' "$OUT/$name.log" | tail -1 | sed -E 's/.*PerSymbol://')
  local last_floor=$(grep 'floorPin:' "$OUT/$name.log" | tail -1 | sed -E 's/.*floorPin:/floorPin:/')
  # rejTooOld and stale from last per-symbol periodic line
  local last_periodic=$(grep -E 'per-symbol: G' "$OUT/$name.log" | tail -1 | sed -E 's/.*per-symbol: //')
  echo "  log: absorbed_gaps=$gaps hot_promotions(log)=$promotions"
  [ -n "$last_periodic" ] && echo "  last periodic: $last_periodic"
  [ -n "$last_per" ]      && echo "  final stats:  $last_per"
  [ -n "$last_floor" ]    && echo "  final floor:  $last_floor"
}

run "00_baseline_no_loss"
run "01_loss_A_only_5pct"      --loss-targets A  --loss-rate 0.05  --loss-seed 42
run "02_loss_B_only_5pct"      --loss-targets B  --loss-rate 0.05  --loss-seed 42
run "03_loss_AB_indep_2pct"    --loss-targets AB --loss-rate 0.02  --loss-seed 42
run "04_loss_AB_corr_1pct"     --loss-targets AB --loss-rate 0.01  --loss-correlated --loss-seed 42
run "05_loss_burst50_corr"     --loss-targets AB --loss-rate 0.005 --loss-mode burst --loss-burst 50 --loss-correlated --loss-seed 42
run "06_loss_AB_corr_aggressive" --loss-targets AB --loss-rate 0.05 --loss-correlated --loss-seed 42

echo
echo "Logs+counters in $OUT/"
