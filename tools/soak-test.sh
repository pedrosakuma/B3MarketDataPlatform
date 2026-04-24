#!/bin/bash
# Long-running stability soak test.
#
# Runs the consumer in docker for ${DURATION_MIN} minutes, sampling RSS
# (resident set size, the runtime memory footprint), GC counts, and key
# per-symbol counters every 30 s into a CSV.
#
# Usage:
#   tools/soak-test.sh [DURATION_MIN]   # default 240 (4 h)
#   OUT=/tmp/soak-out tools/soak-test.sh 60
#
# Pass criteria (printed at end):
#   - RSS slope (last hour vs first hour post-warmup) < +10 %
#   - No monotonic gen2 GC pressure increase (>2x first-window rate)
#   - No 'evictUnsafe' growth without matching 'authReset' (would mean
#     stuck-stale escape isn't catching a leak)
#
set -u
DURATION_MIN="${1:-240}"
OUT="${OUT:-/tmp/soak-$(date +%Y%m%d-%H%M%S)}"
WARMUP_S=120
SAMPLE_INTERVAL=30

mkdir -p "$OUT"
CSV="$OUT/soak.csv"
echo "ts,uptime_s,rss_MB,gen0,gen1,gen2,heap_MB,workingSet_MB,threads,cpu_pct,stale,healed,authReset,evictSafe,evictUnsafe,hotProm,rejTooOld,absorbedGaps" > "$CSV"

echo "## soak-test  duration=${DURATION_MIN}m  out=$OUT"

# Bring up docker compose stack.
docker compose up -d --build > "$OUT/compose-up.log" 2>&1
sleep 5

# Wait for backend healthcheck to be ready.
for i in $(seq 1 60); do
  if curl -fsS http://localhost:8080/health/ready 2>/dev/null | grep -q '"status":"ready"'; then
    echo "[t=0] backend ready"
    break
  fi
  sleep 2
done

START_TS=$(date +%s)
END_TS=$((START_TS + DURATION_MIN * 60))

while [ $(date +%s) -lt $END_TS ]; do
  NOW=$(date +%s)
  UPTIME=$((NOW - START_TS))

  # Pull container RSS via cgroup.
  RSS_BYTES=$(docker compose exec -T backend cat /sys/fs/cgroup/memory.current 2>/dev/null || echo 0)
  RSS_MB=$((RSS_BYTES / 1024 / 1024))

  # docker stats for working set / cpu (one-shot).
  read CPU_PCT WS_MB THREADS <<< $(docker stats --no-stream --format '{{.CPUPerc}} {{.MemUsage}}' sbeb3umdfconsumer-backend-1 2>/dev/null \
    | awk '{gsub(/%/,"",$1); split($2,a,"/"); val=a[1]; gsub(/[^0-9.]/,"",val); printf "%s %s 0\n", $1, val}' || echo "0 0 0")

  # GC + heap from dotnet-counters one-shot inside container.
  COUNTERS=$(docker compose exec -T backend bash -c "timeout 6 dotnet-counters collect --process-id 1 --counters System.Runtime --refresh-interval 1 --format json -o /tmp/.gc.json >/dev/null 2>&1; cat /tmp/.gc.json 2>/dev/null" 2>/dev/null || echo '{}')
  GEN0=$(echo "$COUNTERS" | grep -oE '"gen-0-gc-count"[^,}]*' | grep -oE '[0-9]+(\.[0-9]+)?' | head -1 || echo 0)
  GEN1=$(echo "$COUNTERS" | grep -oE '"gen-1-gc-count"[^,}]*' | grep -oE '[0-9]+(\.[0-9]+)?' | head -1 || echo 0)
  GEN2=$(echo "$COUNTERS" | grep -oE '"gen-2-gc-count"[^,}]*' | grep -oE '[0-9]+(\.[0-9]+)?' | head -1 || echo 0)
  HEAP=$(echo "$COUNTERS" | grep -oE '"gc-heap-size"[^,}]*' | grep -oE '[0-9]+(\.[0-9]+)?' | head -1 || echo 0)

  # Floor pin + stale stats from backend stdout (StatsPrinter periodic line).
  LAST_LOG=$(docker compose logs --tail=200 backend 2>/dev/null | grep -E 'PerSymbol:|per-symbol: G' | tail -1)
  STALE=$(echo "$LAST_LOG" | grep -oE 'stale[=:][0-9]+' | head -1 | grep -oE '[0-9]+' || echo 0)
  HEALED=$(echo "$LAST_LOG" | grep -oE 'healed[=:][0-9,]+' | head -1 | tr -d ',' | grep -oE '[0-9]+' || echo 0)
  AUTHRESET=$(echo "$LAST_LOG" | grep -oE 'authReset[=:][0-9,]+' | tr -d ',' | grep -oE '[0-9]+' || echo 0)
  EVICTSAFE=$(echo "$LAST_LOG" | grep -oE 'evictSafe[=:][0-9,]+' | tr -d ',' | grep -oE '[0-9]+' || echo 0)
  EVICTUNSAFE=$(echo "$LAST_LOG" | grep -oE 'evictUnsafe[=:][0-9,]+' | tr -d ',' | grep -oE '[0-9]+' || echo 0)
  HOTPROM=$(echo "$LAST_LOG" | grep -oE 'hotProm[=:][0-9,]+' | tr -d ',' | grep -oE '[0-9]+' || echo 0)
  REJTOOOLD=$(echo "$LAST_LOG" | grep -oE 'rejTooOld:[0-9,]+' | tr -d ',' | grep -oE '[0-9]+' || echo 0)
  ABSORBED=$(echo "$LAST_LOG" | grep -oE 'channelGapsAbsorbed[=:][0-9,]+' | tr -d ',' | grep -oE '[0-9]+' || echo 0)

  echo "$NOW,$UPTIME,$RSS_MB,$GEN0,$GEN1,$GEN2,$HEAP,$WS_MB,$THREADS,$CPU_PCT,$STALE,$HEALED,$AUTHRESET,$EVICTSAFE,$EVICTUNSAFE,$HOTPROM,$REJTOOOLD,$ABSORBED" >> "$CSV"
  printf "[t=%5ds] RSS=%4dMB  gen2=%-4s  stale=%-4s  authReset=%-4s  evictUnsafe=%-10s  rejTooOld=%-4s\n" \
    "$UPTIME" "$RSS_MB" "$GEN2" "$STALE" "$AUTHRESET" "$EVICTUNSAFE" "$REJTOOOLD"

  sleep $SAMPLE_INTERVAL
done

echo
echo "## post-run analysis"
python3 - <<PY
import csv
rows = []
with open("$CSV") as f:
    for r in csv.DictReader(f):
        # Skip malformed rows (missing required fields).
        if r.get('rss_MB') and r.get('uptime_s') and r['rss_MB'].strip().isdigit():
            rows.append(r)
if len(rows) < 4:
    print("not enough samples to analyze")
    exit(0)

warmup = ${WARMUP_S} // ${SAMPLE_INTERVAL}
post = rows[warmup:]
if len(post) < 4:
    print("not enough post-warmup samples")
    exit(0)

def col(rs, k): return [float((r[k] or '0').strip() or 0) for r in rs]
def get(r, k, d=0.0):
    try: return float((r[k] or '0').strip() or 0)
    except (ValueError, TypeError): return d

half = len(post) // 2
rss_first = col(post[:half], 'rss_MB')
rss_last  = col(post[half:], 'rss_MB')
avg_first = sum(rss_first)/len(rss_first)
avg_last  = sum(rss_last)/len(rss_last)
slope_pct = (avg_last - avg_first) / max(avg_first, 1) * 100

gen2_total = get(post[-1], 'gen2') - get(post[0], 'gen2')
duration_s = get(post[-1], 'uptime_s') - get(post[0], 'uptime_s')
gen2_rate  = gen2_total / max(duration_s, 1)

evictUnsafe_growth = get(post[-1], 'evictUnsafe') - get(post[0], 'evictUnsafe')
authReset_growth  = get(post[-1], 'authReset')  - get(post[0], 'authReset')

print(f"  samples post-warmup: {len(post)}")
print(f"  RSS first-half avg : {avg_first:.0f} MB")
print(f"  RSS last-half avg  : {avg_last:.0f} MB")
print(f"  RSS slope          : {slope_pct:+.1f} %   { 'PASS' if abs(slope_pct) < 10 else 'FAIL (>10%)' }")
print(f"  gen2 GC rate       : {gen2_rate:.4f} /s")
print(f"  evictUnsafe growth : {evictUnsafe_growth:.0f}")
print(f"  authReset growth   : {authReset_growth:.0f}")
if evictUnsafe_growth > 0 and authReset_growth == 0:
    print(f"  ⚠ evictUnsafe growing but no authReset — investigate stuck-stale escape")
PY

echo
echo "CSV: $CSV"
