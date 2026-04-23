#!/bin/bash
# Compare Channel vs PerSymbol loss-resilience runs.
#
# Run the harness twice first:
#   MODE=Channel   tools/loss-resilience-test.sh <pcap-prefix> [<duration>]
#   MODE=PerSymbol tools/loss-resilience-test.sh <pcap-prefix> [<duration>]
# Then:
#   tools/loss-resilience-compare.sh
set -u
CH="${CHANNEL_OUT:-/tmp/loss-validation-Channel}"
PS="${PERSYMBOL_OUT:-/tmp/loss-validation-PerSymbol}"

if [ ! -d "$CH" ] || [ ! -d "$PS" ]; then
  echo "Missing $CH or $PS — run loss-resilience-test.sh in both MODEs first." >&2
  exit 1
fi

g() { grep -c "$1" "$2" 2>/dev/null | tr -d '\n' || printf 0; }

printf "%-26s | %7s %7s %12s | %7s %9s %10s\n" "scenario" \
  "ch.gap" "ch.ovf" "ch.dropped" "ps.gap" "ps.absorb" "ps.final"
echo "---------------------------+------------------------------+------------------------------"
for f in "$CH"/*.log; do
  name=$(basename "$f" .log)
  ch_gap=$(g 'Gap detected' "$f")
  ch_ovf=$(g 'recovery queue overflow' "$f")
  ch_drop=$(grep 'recovery queue overflow' "$f" 2>/dev/null | tail -1 | grep -oE 'dropped [0-9]+' | grep -oE '[0-9]+' || echo 0)
  ps_gap=$(g 'Gap detected' "$PS/$name.log")
  ps_abs=$(g 'absorbed channel gap' "$PS/$name.log")
  ps_final=$(grep -E '→ (RealTime|Recovery|CatchUp|WaitSnapshot)' "$PS/$name.log" 2>/dev/null | tail -1 | grep -oE '\w+$' || echo "?")
  printf "%-26s | %7s %7s %12s | %7s %9s %10s\n" "$name" "$ch_gap" "$ch_ovf" "$ch_drop" "$ps_gap" "$ps_abs" "$ps_final"
done
