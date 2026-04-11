#!/usr/bin/env bash
# Profile the B3 UMDF consumer app using dotnet-trace.
#
# Usage:
#   ./tools/profile.sh [pcap-dir]
#
# Requirements:
#   - dotnet-trace global tool: dotnet tool install -g dotnet-trace
#
# Outputs:
#   /tmp/b3-trace.speedscope.json  — Open with https://speedscope.app or VS

set -euo pipefail

PCAP_DIR="${1:-pcap}"
DLL="src/B3.Umdf.ConsoleApp/bin/Release/net10.0/B3.Umdf.ConsoleApp.dll"
TRACE_OUT="/tmp/b3-trace.nettrace"
DURATION="${TRACE_DURATION:-00:00:20}"

# Locate PCAP files
INCR_A=$(find "$PCAP_DIR" -name '*Incremental_FeedA.pcap' | head -1)
INCR_B=$(find "$PCAP_DIR" -name '*Incremental_FeedB.pcap' | head -1)
INSTR=$(find "$PCAP_DIR" -name '*InstrumentDefinition.pcap' | head -1)
SNAP=$(find "$PCAP_DIR" -name '*SnapshotRecovery.pcap' | head -1)

if [[ -z "$INCR_A" || -z "$INSTR" || -z "$SNAP" ]]; then
    echo "ERROR: PCAP files not found in $PCAP_DIR"
    echo "Run ./tools/download-pcaps.sh first."
    exit 1
fi

echo "Building Release..."
dotnet build -c Release -q

echo "Starting app..."
dotnet "$DLL" "$INCR_A" ${INCR_B:+"$INCR_B"} "$INSTR" "$SNAP" &
APP_PID=$!
echo "  PID: $APP_PID"

sleep 5

echo "Collecting trace for $DURATION..."
dotnet-trace collect -p "$APP_PID" --format Speedscope \
    --providers "Microsoft-DotNETCore-SampleProfiler:0xF00000000000:4" \
    --duration "$DURATION" -o "$TRACE_OUT" 2>&1

wait "$APP_PID" 2>/dev/null || true

SPEEDSCOPE="${TRACE_OUT%.nettrace}.speedscope.json"
echo ""
echo "=== Trace collected ==="
echo "  Speedscope: $SPEEDSCOPE"
echo "  Open with:  https://speedscope.app (drag & drop the file)"
echo ""

# Quick analysis if python3 is available
if command -v python3 &>/dev/null; then
    echo "=== Quick hotspot analysis ==="
    python3 -c "
import json, sys
from collections import defaultdict

with open('$SPEEDSCOPE') as f:
    data = json.load(f)

frames = data['shared']['frames']

for prof in data['profiles']:
    events = prof.get('events', [])
    if len(events) < 200:
        continue
    stack = []
    inclusive = defaultdict(float)
    last = 0
    for ev in events:
        delta = ev['at'] - last
        if stack and delta > 0:
            seen = set()
            for fid in stack:
                n = frames[fid]['name']
                if n not in seen:
                    inclusive[n] += delta
                    seen.add(n)
        if ev['type'] == 'O': stack.append(ev['frame'])
        elif ev['type'] == 'C' and stack: stack.pop()
        last = ev['at']
    total = sum(inclusive.values()) / len(set(id(x) for x in [1]))  # avoid div0
    if total == 0: continue
    app = {k: v for k, v in inclusive.items() if 'B3.Umdf' in k}
    if not app: continue
    print(f'Thread: {prof[\"name\"]} ({len(events)} samples)')
    for name, t in sorted(app.items(), key=lambda x: -x[1])[:15]:
        print(f'  {t/total*100:5.1f}%  {name[:120]}')
    print()
"
fi
