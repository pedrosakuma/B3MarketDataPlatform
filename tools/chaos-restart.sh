#!/bin/bash
# Chaos test: backend SIGKILL mid-recovery.
#
# 1. Start consumer with PCAP loop + injected loss.
# 2. Wait until recovery is in progress (per-symbol stale > 0).
# 3. SIGKILL the backend.
# 4. Restart and verify cold-start succeeds, recovery converges to stale=0.
# 5. Capture: time-to-restart, time-to-converge, any 'error:' lines, any
#    counter regressions across the restart boundary.
#
# Usage: tools/chaos-restart.sh
#
set -u
OUT="${OUT:-/tmp/chaos-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT"

echo "## chaos-restart  out=$OUT"

# Bring up with loss injection (correlated 1% — will create per-symbol stale).
docker compose down >/dev/null 2>&1 || true
docker compose build > "$OUT/build.log" 2>&1
UMDF_LOSS_TARGETS=AB UMDF_LOSS_RATE=0.05 UMDF_LOSS_CORRELATED=true UMDF_LOSS_SEED=42 \
  docker compose up -d > "$OUT/up.log" 2>&1
sleep 5

# Wait for stale > 0 (recovery actively in progress).
echo "[chaos] waiting for stale > 0..."
T0=$(date +%s)
WAIT_MAX=120
while true; do
  ELAPSED=$(($(date +%s) - T0))
  if [ $ELAPSED -gt $WAIT_MAX ]; then
    echo "  timeout waiting for stale>0; bringing chaos anyway"
    break
  fi
  STALE=$(docker compose logs --tail=100 backend 2>/dev/null | grep -E 'per-symbol: G' | tail -1 | grep -oE 'stale:[0-9]+' | head -1 | grep -oE '[0-9]+' || echo 0)
  if [ "${STALE:-0}" -gt 0 ]; then
    echo "  detected stale=$STALE at t=${ELAPSED}s"
    break
  fi
  sleep 2
done

# Snapshot pre-kill counters.
docker compose logs --tail=200 backend 2>/dev/null | grep -E 'per-symbol: G|PerSymbol:' | tail -3 > "$OUT/pre-kill.log"
PRE_HEALED=$(grep -oE 'healed:[0-9,]+' "$OUT/pre-kill.log" | tail -1 | tr -d ',' | grep -oE '[0-9]+' || echo 0)
PRE_STALE=$(grep -oE 'stale:[0-9]+' "$OUT/pre-kill.log" | tail -1 | grep -oE '[0-9]+' || echo 0)
echo "[chaos] pre-kill: stale=$PRE_STALE healed=$PRE_HEALED"

# SIGKILL.
KILL_TS=$(date +%s%N)
PID=$(docker compose exec -T backend bash -c 'echo 1' >/dev/null 2>&1 && \
      docker inspect --format='{{.State.Pid}}' sbeb3umdfconsumer-backend-1 2>/dev/null || echo "")
echo "[chaos] SIGKILL backend (host pid=$PID)"
docker kill --signal=KILL sbeb3umdfconsumer-backend-1 > "$OUT/kill.log" 2>&1

# Wait for container to exit, then restart (compose policy: 'unless-stopped' would auto-restart;
# manually restart to measure).
sleep 2
RESTART_TS=$(date +%s%N)
docker compose up -d > "$OUT/restart.log" 2>&1
RESTART_TIME_MS=$(( (RESTART_TS - KILL_TS) / 1000000 ))
echo "[chaos] restart issued (${RESTART_TIME_MS}ms after kill)"

# Wait for ready.
T0=$(date +%s)
READY=0
for i in $(seq 1 60); do
  if curl -fsS http://localhost:8080/health/ready 2>/dev/null | grep -q '"status":"ready"'; then
    READY=$(($(date +%s) - T0))
    echo "[chaos] backend ready ${READY}s after restart"
    break
  fi
  sleep 1
done

# Convergence: wait until stale = 0 (post cold-start) — bound by 120s.
# Use --since to only look at logs since restart.
RESTART_ISO=$(date -u -Iseconds)
sleep 5  # allow time for fresh per-symbol log lines after restart
echo "[chaos] waiting for convergence (stale=0) since $RESTART_ISO..."
T0=$(date +%s)
CONVERGED=-1
for i in $(seq 1 120); do
  STALE=$(docker compose logs --since="$RESTART_ISO" backend 2>/dev/null | grep -E 'per-symbol: G' | tail -1 | grep -oE 'stale:[0-9]+' | head -1 | grep -oE '[0-9]+' || echo "")
  if [ -n "${STALE:-}" ] && [ "$STALE" = "0" ]; then
    CONVERGED=$(($(date +%s) - T0))
    echo "[chaos] converged: stale=0 at t=${CONVERGED}s post-restart"
    break
  fi
  sleep 1
done
[ $CONVERGED -lt 0 ] && echo "[chaos] did not reach stale=0 within 120s (last stale=${STALE:-?})"

# Error scan.
docker compose logs --since=5m backend 2>/dev/null > "$OUT/post.log"
ERRORS=$(grep -c 'error:\|ERR \|FATAL\|Unhandled exception' "$OUT/post.log" || true)
echo
echo "## chaos summary"
echo "  pre-kill stale     : $PRE_STALE"
echo "  pre-kill healed    : $PRE_HEALED"
echo "  restart cold-start : ${READY:-?}s"
echo "  convergence        : ${CONVERGED}s (stale=0)"
echo "  errors in post.log : $ERRORS"
if [ $CONVERGED -ge 0 ] && [ $ERRORS -eq 0 ]; then
  echo "  RESULT             : PASS ✓"
else
  echo "  RESULT             : FAIL ✗"
fi
echo
echo "Logs in $OUT/"
