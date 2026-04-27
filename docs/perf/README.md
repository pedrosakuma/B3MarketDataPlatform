# UMDF Traffic Characterization (Phase 0)

Ground-truth per-channel statistics extracted from recorded B3 UMDF
PCAPs, used to design realistic dispatcher / ring benchmarks instead
of guessing synthetic rates.

## How to regenerate

```bash
dotnet run --project tools/PcapTrafficCharacterizer -c Release -- \
  --session DRV-072-20250929 \
  --feed-a pcap/20250929_MBO_072_DRV_Incremental_FeedA.pcap \
  --feed-b pcap/20250929_MBO_072_DRV_Incremental_FeedB.pcap \
  --snap   pcap/20250929_MBO_072_DRV_SnapshotRecovery.pcap \
  --instr  pcap/20250929_MBO_072_DRV_InstrumentDefinition.pcap \
  --bucket-ms 10 \
  --out-dir docs/perf
```

Outputs (per session):
- `<session>-buckets.csv` — per-channel × 10ms bucket time series
  (pkts, bytes, min_size, max_size). Large (~125 MB per session);
  gitignored — regenerate from PCAPs.
- `<session>-sizes.csv` — per-channel cumulative size histogram
  (8 bins from ≤80B to ≤1500B + overflow). Committed.

## Findings (DRV-072 2025-09-29 + EQT-084 2025-03-31)

### Per-channel rates

| Channel              | DRV mean kpps | DRV peak100ms kpps | EQT mean kpps | EQT peak100ms kpps |
|----------------------|---------------|--------------------|---------------|--------------------|
| IncrementalA         | 4.1           | **118.4**          | 4.7           | **99.7**           |
| IncrementalB         | 4.1           | **118.4**          | 4.7           | **97.0**           |
| SnapshotRecovery     | 0.1           | 1.1                | 0.1           | 1.1                |
| InstrumentDefinition | 0.1           | 1.1                | 0.1           | 1.1                |

### Burst factor (peak / mean)
- Inc: **21–29×** — extremely spiky (auctions, news, expiries)
- Snap / InstrDef: **~10×** but absolute rate is negligible

### Size distributions
All four channels are **bimodal**, not normally distributed:
- Inc: heavy at ≤128B (single-message updates), thin tail to 1500B
- Snap: ~99% at 1500B (book-dense snapshots), small head at ≤80B (CompleteSnapshot)
- InstrDef: ~99.9% bimodal between ≤80B (header/complete) and 1500B (entries)

→ Any benchmark using a fixed packet size produces unrealistic memory
and cache pressure.

## Architectural implications

The original P2 #5 hypothesis ("Snapshot bursts head-of-line block
Incremental") is **not supported by the data**:

- Snap peak load is **1.1 kpps** (~11 packets per 100ms). Even at a
  hypothetical 100µs dispatch cost per packet, Snap contributes
  <1.1 ms per second of dispatcher time — invisible against the
  ~118 kpps Incremental burst window.
- Inc itself bursts at **118 kpps** (29× its mean). This is the
  actual dispatcher pressure point.
- Per-channel ring isolation (Option B/B.1 from the prior plan) would
  not measurably improve Incremental latency under the recorded
  workloads.

### Decisions
- **Cancel Option B/B.1** (multi-ring + priority dispatch).
- **Keep Option A** (single ring + per-channel sub-counters) — gives
  drop attribution at zero structural risk.
- **Open P9** to investigate Inc burst absorption: is the per-group
  ring sized for 118 kpps bursts? Are SO_REUSEPORT sockets distributing
  fairly under that load? What's the dispatcher's sustained throughput
  ceiling?

### Caveats
- PCAPs are normal-day captures; no patological recovery storm is
  represented. Even hypothesizing 10× Snap burst during a forced-heal
  cascade still leaves Snap at ~1/10 the Inc rate.
- Forced-heal-every-6s for mini-index futures (observed in production)
  is most likely UDP loss in the network, not consumer-side contention.
- No opening-auction (~10:00 BRT) capture; Inc peaks could be higher.
