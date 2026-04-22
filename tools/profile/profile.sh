#!/usr/bin/env bash
# Capture diagnostics from the consumer container.
# All .NET diagnostic tools ship inside the runtime image at /usr/local/bin
# (dotnet-trace, dotnet-counters, dotnet-dump, dotnet-gcdump, dotnet-stack).
#
# Usage:
#   tools/profile/profile.sh trace    [duration]   # CPU sample profile (default 30s)
#   tools/profile/profile.sh gc       [duration]   # GC + alloc trace
#   tools/profile/profile.sh counters              # live counter monitor (Ctrl-C to stop)
#   tools/profile/profile.sh gcdump                # heap object graph snapshot
#   tools/profile/profile.sh dump                  # full process dump
#   tools/profile/profile.sh stack                 # one-shot stack walk of all threads
#   tools/profile/profile.sh shell                 # interactive shell inside container
#
# Override container with CONTAINER=<service-name>. Captures land in ./profiles/.
set -euo pipefail

CONTAINER="${CONTAINER:-consumer}"
ACTION="${1:-trace}"
DURATION="${2:-00:00:30}"
HOST_OUT="$(pwd)/profiles"
mkdir -p "$HOST_OUT"

CID="$(docker compose ps -q "$CONTAINER" 2>/dev/null || true)"
[ -z "$CID" ] && CID="$CONTAINER"

run() { docker exec -i "$CID" "$@"; }
pid() { docker exec "$CID" sh -c 'pgrep -f B3.Umdf.ConsoleApp | head -n1'; }
ensure_dir() { docker exec "$CID" sh -c 'mkdir -p /profiles'; }
stamp() { date +%Y%m%d_%H%M%S; }
fetch() { docker cp "$CID:$1" "$HOST_OUT/" && echo "Saved → $HOST_OUT/$(basename "$1")"; }

case "$ACTION" in
  trace)
    PID=$(pid); ensure_dir
    OUT="/profiles/trace_$(stamp).nettrace"
    echo "CPU trace PID=$PID for $DURATION → $OUT"
    run dotnet-trace collect -p "$PID" --duration "$DURATION" \
        --providers Microsoft-DotNETCore-SampleProfiler -o "$OUT"
    fetch "$OUT"
    ;;
  gc)
    PID=$(pid); ensure_dir
    OUT="/profiles/gc_$(stamp).nettrace"
    echo "GC trace PID=$PID for $DURATION → $OUT"
    run dotnet-trace collect -p "$PID" --duration "$DURATION" \
        --profile gc-verbose -o "$OUT"
    fetch "$OUT"
    ;;
  counters)
    PID=$(pid)
    docker exec -it "$CID" dotnet-counters monitor -p "$PID" \
        --counters B3.Umdf.Consumer,System.Runtime
    ;;
  gcdump)
    PID=$(pid); ensure_dir
    OUT="/profiles/gcdump_$(stamp).gcdump"
    run dotnet-gcdump collect -p "$PID" -o "$OUT"
    fetch "$OUT"
    ;;
  dump)
    PID=$(pid); ensure_dir
    OUT="/profiles/dump_$(stamp).dmp"
    run dotnet-dump collect -p "$PID" -o "$OUT" --type Full
    fetch "$OUT"
    ;;
  stack)
    PID=$(pid)
    run dotnet-stack report -p "$PID"
    ;;
  shell)
    docker exec -it "$CID" bash || docker exec -it "$CID" sh
    ;;
  *)
    sed -n '2,15p' "$0" >&2
    exit 2
    ;;
esac
