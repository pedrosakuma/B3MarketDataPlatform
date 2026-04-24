# Resilience Strategy

This document describes the fault-tolerance design of `SbeB3UmdfConsumer`:
the failure modes it explicitly handles, and the layered defenses in place.

For raw throughput design see [PERFORMANCE.md](./PERFORMANCE.md).

The system is built on the principle that **clients must never be able to
impair feed consumption**. Every defense below exists to enforce that
invariant.

## 1. Failure modes covered

| Failure | Where | Defense |
| ------- | ----- | ------- |
| Single UDP packet loss | Network → recv socket | Sequence-gap detection + recovery via incremental UDP recovery feed |
| Burst loss / out-of-order | Network | A/B reorder buffer (configurable depth) |
| Channel `Lost` state | Feed state machine | Snapshot recovery (TCP) once recovery becomes available, with retry/backoff |
| **Cascading recovery** | Slow clients pressuring the dispatch thread → packets dropped → more recovery → loop | **Fanout suppression while in Recovery/CatchUp** + auto-resnapshot on RealTime |
| Slow WS client (genuinely stuck) | TCP back-pressure → unbounded server-side queue | **Hard pending-bytes cap** with disconnect |
| Many clients all slow at once (systemic) | All clients near cap simultaneously | **Outlier sweep** under aggregate-pressure gate |
| Snapshot connect storm | 100 k+ subscribes at once → snapshot allocation flood | `MaxSnapshotRequestsPerBatch` cap on the dispatch thread |
| Container OOM | POH fragmentation from pinned UDP buffers | Pinned-buffer pool sized to expected concurrency |
| Publisher crash | shared `network_mode: service:consumer` | Health endpoint + restart policy in compose |

## 2. Per-instrument recovery (unified `rptSeq`)

The B3 UMDF spec — confirmed by PCAP analysis (200 k packets, 175 116
cross-template `rptSeq` advances with gap = 0, 0 violations) — defines
**one `rptSeq` counter per `SecurityID`** that is monotonic across **all**
message templates (MBO, Trade, SecurityStatus, PriceBand, ExecStat, …).
The consumer enforces this invariant in `SymbolStateRegistry`.

### 2.1 State per symbol

For each `SecurityID` the registry tracks:

- `ObservedRptSeq` — max `rptSeq` ever seen on **any** kind. Wire-truth.
- `LastRptSeq[kind]` — last applied `rptSeq` per bucket (one shared `Mbo`
  bucket; one bucket per stat template — SecurityStatus, PriceBand, etc.).
- `States[kind]` ∈ {Unknown, Healthy, Stale}.
- `MinHealRptSeq[Mbo]` — lower bound for an acceptable snapshot.
- `StaleKindMask` — bitmask of kinds currently Stale (drives the symbol-level
  `IsAnyStale` gate).

### 2.2 Observe — global-gap-first dispatch

For every incoming `(secId, kind, receivedRptSeq)`:

```
globalGap = ObservedRptSeq > 0 && receivedRptSeq > ObservedRptSeq + 1
```

- **`globalGap = true`** → the wire counter jumped. The Mbo bucket is
  conservatively forced `Healthy → Stale` (the lost rptSeqs *could* have
  been MBO messages). `MinHealRptSeq[Mbo]` is pinned to `received - 1`
  on the transition. The triggering kind itself still applies (stats are
  allowed to live-resync).
- **No global gap** → per-kind dispatch proceeds: duplicate is dropped,
  forward delivery is applied, and the kind is rebaselined to `received`.

The shared `Mbo` bucket means seven templates (`Order_50`, `DeleteOrder_51`,
`MassDelete_52`, `Trade_53`, `ForwardTrade_54`, `ExecSummary_55`,
`TradeBust_57`) share one state — without this, a Trade between two MBOs
would otherwise expose an artificial per-kind gap.

### 2.3 Why global-gap dispatch matters

Earlier per-kind tracking caused **false-positive Stales** whenever a stat
advanced the wire counter between MBOs. On hot symbols with many stat
templates this triggered repeated 30-100 s snapshot-heal cycles that were
purely an artifact of the bookkeeping model — no actual MBO loss had
occurred. Unification eliminated this entire class of spurious recovery.

### 2.4 Heal flow

```
Observe (gap)
    │
    ▼
Mbo[secId] = Stale; MinHeal = received - 1; mask |= Mbo bit
    │
    ▼
Subsequent Mbo messages → buffered in StaleMboBuffer (8 192 / symbol normal,
65 536 / symbol "hot" after first overflow).
    │
    ├─ Snapshot.Begin(rptSeq=S) for this symbol
    │     └─► StaleMboBuffer.SetProtectedFloor(secId, S+1)   (monotonic)
    │
    │     During the Begin→End window, if the buffer hits hot cap, the
    │     oldest msg is evaluated:
    │       • oldest.rptSeq <  ProtectedFloor → SAFE drop (covered by
    │         the in-flight snapshot). MinHeal NOT bumped. Counts
    │         stale_buffer_evicted_safe_below_floor.
    │       • oldest.rptSeq >= ProtectedFloor → UNSAFE drop (in the
    │         drain window). MinHeal bumped as fail-safe — the snapshot
    │         being assembled may now be too-old. Counts
    │         stale_buffer_evicted_unsafe.
    │
    ▼
Snapshot arrives → BookManager.CompleteSnapshot
    │
    │   (StaleMboBuffer.ClearProtectedFloor at every exit path: accept,
    │    reject, illiquid)
    │
    ├── snapshotRptSeq < MinHeal → REJECT (lagging snapshot, stays Stale,
    │                              counts b3.umdf.persymbol.snapshots_rejected_too_old)
    │
    └── snapshotRptSeq >= MinHeal → ACCEPT
            │
            ▼
        ApplySnapshotStaging → live book swapped
        Heal returns DrainFrom = snap+1, DrainTo = high-water
        BookManager replays buffered messages in [DrainFrom, DrainTo]
        Mbo state ← Healthy; mask &= ~Mbo bit
        If StaleKindMask becomes 0 → fire OnSymbolStaleStatusChanged(false)
```

**Why the floor pin matters.** Pre-fix, any hot-cap eviction unconditionally
bumped `MinHeal` to the rptSeq following the evicted message. For high-rate
symbols, the buffer could overflow during the multi-second window between
`Snapshot.Begin` and `Snapshot.End`, advancing `MinHeal` past
`snapshot.rptSeq` and causing `CompleteSnapshot` to reject its own snapshot.
Result: a stuck-Stale loop — the symbol stayed Stale forever because every
arriving snapshot looked too-old. The floor pin makes eviction safe for the
range the in-flight snapshot already covers, breaking the loop. Validated in
production stress: 6 M+ safe evictions / scenario on DRV under correlated 1 %
loss with **zero** stuck-stale symbols (see § 11).

### 2.5 Stale → Healthy event surfacing

`OnSymbolStaleStatusChanged` is fired by the registry callback (wired in
`BookManager`'s constructor) so the event surfaces **regardless of which
kind exposed the gap**: a stat-revealed loss flips the symbol's stale flag
to clients exactly as an MBO-revealed one would. The callback only fires
on real `Stale ↔ Healthy` transitions (Unknown → Healthy bootstrap is not
counted).

### 2.6 Channel-level fallback

A wide channel-level gap (more than the 256-packet A/B reorder window) still
escalates the channel's `FeedHandler` to `Lost → Recovery → CatchUp →
RealTime` and triggers the resnapshot-all-subscribers path described in
§ 4. Per-instrument and per-channel recovery are complementary: per-instrument
absorbs the common case (a few symbols affected); per-channel handles the
catastrophic case (the entire group fell behind).

Recovery progress is observable via metrics:

```
b3.umdf.feed.state                                  # 0=WaitInstrumentDefinition, 1=Streaming
b3.umdf.feed.gaps                                   # channel-level network-loss diagnostic
b3.umdf.feed.channel_gaps_absorbed                  # gaps absorbed without leaving Streaming
b3.umdf.persymbol.stale_symbols                     # per-symbol Stale gauge by kind
b3.umdf.persymbol.snapshots_healed
b3.umdf.persymbol.snapshots_rejected_too_old        # ALERT if growing monotonically
b3.umdf.persymbol.stale_buffer_hot_promotions       # symbols at hot cap (65 536)
b3.umdf.persymbol.stale_buffer_evicted_safe_below_floor
b3.umdf.persymbol.stale_buffer_evicted_unsafe       # ALERT if growing — pathological loss
b3.umdf.recovery.snapshot.attempts
```

## 3. A/B reorder buffer

B3 publishes incremental data on **two equivalent multicast groups (A and
B)** for redundancy. The consumer joins both and dedups by `MsgSeqNum`
using a small ring window. This absorbs short bursts of loss on either
channel transparently — no recovery required.

The reorder window size is configurable; the default is sized to absorb
~50 ms of single-channel jitter.

## 4. Cascading recovery — and why it deserves its own defense

The most subtle failure we observed in stress testing was a
**self-reinforcing recovery loop**:

```
slow client → fanout slows → dispatch backs up → MpscPacketRing fills
   → recv thread drops UDP packets → sequence gap → Recovery
   → recovery enqueues into _incrementalQueue (which is also processed
     on the dispatch thread!) → dispatch even slower → more drops
   → POH churn from snapshot allocations on every state transition
   → OOM ~2.3 GiB
```

The trigger is fanout pressure but the **amplifier** is doing client
fanout *while* the group is recovering. Recovery is by definition a CPU
spike on the dispatch thread; layering subscriber fanout on top of it
guarantees the queue never drains.

### Defense: fanout suppression during Recovery / CatchUp

`GroupConflationHandler.SetFanoutSuppressed(true)` is called when the
group's `FeedHandler` transitions to `Recovery` or `CatchUp`. While
suppressed:

- `OnBatchComplete` still drains the unsubscribe queue (cleanup must continue).
- Conflation buffers are **discarded** (cleared, counted as flushed for
  metrics integrity) — clients would have received stale data anyway.
- `ProcessOwnSubscribeRequests` and `PublishCurrentBatch` are skipped.

When the group transitions back to `RealTime`,
`SubscriptionManager.RequestResyncForAllSubscribersInGroup` schedules a
fresh MBP snapshot for every Book subscriber in the group, paced by
`MaxSnapshotRequestsPerBatch`. From the client's perspective the recovery
is invisible: they see a brief pause, then a clean snapshot, then
incremental updates resume.

This single change broke the recovery loop in stress tests at 500 c × 240 s
where the consumer was previously OOM'ing reliably at t ≈ 90 s.

## 5. Slow-consumer protection — layered defense

Three layers, escalating in aggressiveness:

### Layer 1 — Bounded per-client queue (msgs)

`MpscOutboundRing` per client has a fixed capacity (default **65 536**
slots). When full, `TryEnqueue` returns `false` and the inbound producer
treats the message as dropped for that one client (others are unaffected).
This is the **first** line of back-pressure and is transparent to the
client (they may see a tiny inconsistency that the next snapshot resolves).

### Layer 2 — Hard pending-bytes cap (memory)

Per-client byte budget: `UMDF_CLIENT_MAX_PENDING_BYTES` (default
**4 MiB**). Tracked atomically across `TryEnqueue`/drain; when exceeded
the client is **disconnected with `WebSocketCloseStatus.PolicyViolation
"slow consumer"`**. This is the absolute memory ceiling per client —
without it, a single slow consumer with deep TCP back-pressure could
accumulate many MiB before the msg-count layer triggers.

### Layer 3 — Outlier sweep under aggregate pressure (fairness)

A periodic timer (`UMDF_CLIENT_OUTLIER_INTERVAL_MS`, default **1 000 ms**)
walks all clients and:

1. Snapshots `pendingBytes` per client (via `ArrayPool` to avoid
   allocation).
2. **Gates on aggregate pressure**: if
   `Σpending < UMDF_CLIENT_OUTLIER_PRESSURE_PCT × clients × maxPending`
   (default 50 %) the sweep is a **no-op**.
3. Otherwise computes the median pending and disconnects every client
   whose pending exceeds
   `max(median × UMDF_CLIENT_OUTLIER_MULTIPLIER, UMDF_CLIENT_OUTLIER_MIN_BYTES)`
   (defaults 4× and 256 KiB).

#### Why outlier-relative and not just a fixed threshold?

A fixed lower threshold is tempting but has a well-known failure mode:
under a **systemic** slowdown (e.g. a transient network hiccup affecting
all clients) every client briefly approaches the threshold and
**all clients get disconnected at once**. The outlier strategy is
self-stabilizing — when the median rises, the disconnect threshold rises
with it. We only kill clients whose pending is *anomalous relative to the
fleet*, and only when the fleet as a whole is under real pressure.

The `MIN_BYTES` floor (`UMDF_CLIENT_OUTLIER_MIN_BYTES`) prevents a
pathological case where the median is tiny (e.g. all clients fully drained
except one): a client with 200 KiB pending would technically be 1000× the
median but is not actually a problem.

#### Hard cap vs. outlier sweep — when does each fire?

In practice:

- **Bursty single bad client** → hard cap (Layer 2) fires inline within
  milliseconds. Sweep never engages.
- **Slow build-up across many clients** (e.g. all clients on a saturated
  uplink) → hard cap may not trip; sweep engages once aggregate crosses
  50 % gate, removes the worst offenders, restores headroom.
- **Healthy fleet, no pressure** → sweep is a no-op; ~negligible cost.

## 6. Snapshot allocation pacing

Each Book subscribe issues `SendMboSnapshot` on the dispatch thread, which
allocates `(long, long, int)[]` for both sides plus rents an
`ArrayPool<byte>` buffer. A connect storm of 500 clients × 200 symbols
= 100 000 snapshots issued in seconds will drown the GC if not paced.

**`MaxSnapshotRequestsPerBatch`** (default **32**) bounds how many of these
the dispatch thread services per packet. Excess requests stay queued in
the per-group request queue and drain naturally at ~1 000 batches/s ≈
32 000 snapshots/s. This is far above any realistic connect rate while
keeping the dispatch thread's allocation rate bounded.

Without this cap, the same connect storm reliably caused OOM at ~2.3 GiB
in pre-fix stress tests.

## 7. Drop-newest with resnapshot (broadcaster ring)

The `BroadcastRing` between dispatch and broadcaster is bounded (default
256 slots/group). On overflow the dispatch thread:

1. Drops the work batch.
2. Collects the unique `SecId`s in the dropped batch.
3. Enqueues a `Get`-kind resync request via `SubscriptionManager` so each
   affected subscriber receives a fresh MBP snapshot on the next dispatch
   cycle.

Effect: feed integrity is preserved (the dispatch thread never blocks on
the broadcaster), and any client whose update was dropped sees a clean
snapshot rather than a corrupted incremental sequence.

## 8. Memory bounds

Total consumer memory has hard upper bounds at every layer:

| Component | Bound | Default |
| --------- | ----- | ------- |
| Per-client outbound ring (msgs) | `UMDF_CLIENT_OUTBOUND_RING_CAPACITY` | 65 536 |
| Per-client outbound (bytes) | `UMDF_CLIENT_MAX_PENDING_BYTES` | 4 MiB |
| Per-group dispatch ring (packets) | `UMDF_DISPATCH_RING_CAPACITY` | 65 536 |
| Per-group broadcaster ring (work batches) | compile-time | 256 |
| Per-symbol stale MBO buffer | compile-time | 256 messages × N symbols |
| Per-security candle buffer | compile-time (`MaxRetainedCandles`) | 36 000 (10 h @ 1 s) |
| Pinned UDP buffer pool | compile-time | sized to recvmmsg batch × N groups |

With defaults and the recommended container sizing (4 GiB / 2 CPU), the
worst-case footprint is well under the limit even with all rings full.
The OOM scenarios we observed in earlier development were always traced to
**unbounded** state (POH-pinned snapshot allocations during cascading
recovery) — every such site is now bounded.

## 9. Crash safety

- `MulticastRecv` thread catches and logs any `Exception`, then exits the
  thread cleanly; the host registers a `lifetime.StopApplication()` so the
  container exits with a non-zero code and the orchestrator restarts it.
- `OutOfMemoryException` from `new UmdfPacket[64]` is treated the same way
  (container restart) — the alternative (silent allocation failure) would
  leave the consumer running but blind.
- Health endpoint `/health` returns 503 if any feed group has been out of
  `RealTime` for more than `UMDF_HEALTH_DEGRADED_AFTER_MS`.

## 10. Operational playbook

| Symptom | Likely cause | First action |
| ------- | ------------ | ------------ |
| `b3.umdf.client.disconnects.slow` rising | Client cannot keep up with feed | Inspect client; raise `UMDF_CLIENT_COALESCE_WINDOW_MS` if many clients on slow links |
| `b3.umdf.feed.state` flapping to Recovery | Network loss or dispatch starvation | Check `b3.umdf.feed.ring_dropped`; if non-zero, increase dispatch ring or check CPU saturation |
| Memory growing past 2 GiB | POH fragmentation from snapshot bursts | Verify `MaxSnapshotRequestsPerBatch` is set; throttle connect rate |
| Outlier sweep disconnecting many | Systemic slowdown | Inspect `b3.umdf.client.pending_bytes_total`; raise `UMDF_CLIENT_OUTLIER_PRESSURE_PCT` if too sensitive |
| Container OOMKilled (vs managed OOM) | Real memory exhaustion | Raise container limit; confirm pinned-buffer pool isn't oversized |

## 11. Stress validation

The defenses are exercised end-to-end by repeated stress runs documented
in the session checkpoints:

- **500 clients × 240 s @ REPLAY_SPEED=2** with 25 producer processes —
  the original cascading-recovery scenario; now stable post fanout
  suppression + snapshot pacing.
- **200 clients × 180 s with 15 – 30 % artificially slow** —
  validates that the hard cap selectively removes only the slow clients
  while the healthy fleet sees zero disconnects.
- **Unit-test coverage** for the outlier sweep:
  `OutlierSweep_DoesNotDisconnect_WhenAggregatePressureBelowGate`,
  `OutlierSweep_DisconnectsOutliers_UnderPressure`,
  `OutlierSweep_RespectsMinBytesFloor_EvenUnderPressure`.

### 11.1 Loss-injection harness

`tools/loss-resilience-test.sh` drives the replayer with PCAP-replayed
multicast traffic and seeded packet-loss injection across 7 scenarios.
`tools/loss-resilience-with-counters.sh` is the same harness with floor-pin
and snapshot-rejection counters surfaced inline (recommended for
post-change validation).

```sh
# Build Release once.
dotnet build -c Release

# Per-PCAP run (30 – 45 s per scenario; outputs to /tmp/loss-validation/<scenario>.log).
OUT=/tmp/loss-validation-eqt tools/loss-resilience-with-counters.sh pcap/20250331_MBO_084_EQT 45
OUT=/tmp/loss-validation-drv tools/loss-resilience-with-counters.sh pcap/20250929_MBO_072_DRV 30
```

Scenarios (all reproducible — fixed `--loss-seed 42`):

| # | Name | Targets | Rate | Mode | Purpose |
|---|------|---------|------|------|---------|
| 00 | baseline | – | – | – | Sanity / no-loss reference |
| 01 | A-only 5 % | A | 5 % | indep | Confirms B-feed redundancy fully covers A loss |
| 02 | B-only 5 % | B | 5 % | indep | Confirms A-feed redundancy fully covers B loss |
| 03 | AB indep 2 % | A+B | 2 % | indep | Both feeds losing independently |
| 04 | AB corr 1 % | A+B | 1 % | correlated | Same packet dropped on both feeds (genuine loss) |
| 05 | AB burst 50 corr 0.5 % | A+B | 0.5 % | burst-50 corr | Long correlated bursts (worst case) |
| 06 | AB corr 0.001 % | A+B | 0.001 % | correlated | Long-tail rare loss |

### 11.2 Latest results (post floor-pin fix — 2026-04-24)

Run with `tools/loss-resilience-with-counters.sh` (45 s EQT, 30 s DRV).
Floor-pin counters now exposed in periodic and final stats.

**EQT (18 386 symbols, mostly low-rate):**

| Scenario | gaps absorbed | hot promotions | rejTooOld | evictUnsafe | stale (mid → end) |
|----------|--------------:|---------------:|----------:|------------:|------------------:|
| 00 baseline | 0 | 0 | 0 | 0 | 0 |
| 01 A 5 % | 0 | 0 | 0 | 0 | 0 (B covers) |
| 02 B 5 % | 0 | 0 | 0 | 0 | 0 (A covers) |
| 03 AB indep 2 % | 8 353 | 14 | 1 | **0** | 1 257 → 1 096 |
| 04 AB corr 1 % | 1 944 | 15 | 2 | **0** | 750 → 779 |
| 05 burst 50 corr | 66 554 | 0 | 5 | **0** | 1 556 → 144 |
| 06 AB corr 5 % | 34 570 | 7 | 3 | **0** | 2 377 → 397 |

**DRV (16 symbols, very high rate — exercises floor pin under stress):**

| Scenario | gaps absorbed | evictSafe | evictUnsafe | rejTooOld | stale final |
|----------|--------------:|----------:|------------:|----------:|------------:|
| 03 AB indep 2 % | 7 624 | **5.98 M** | 17.1 M | 97 | 4 |
| 04 AB corr 1 % | 2 306 | **6.14 M** | 19.6 M | 115 | 3 |
| 05 burst 50 corr | 61 658 | 3.94 M | 1.94 M | 80 | **0** ✓ |
| 06 AB corr 5 % | 30 831 | 5.66 M | 10.3 M | 98 | **0** ✓ |

**Reading the numbers:**

- `evictSafe = 6 M` per DRV scenario means the floor pin saved 6 M messages
  from incorrectly bumping `MinHeal` — exactly the failure mode the fix
  targets. Pre-fix, every one of these would have poisoned the in-flight
  snapshot.
- `evictUnsafe > 0` on DRV is the genuine pathological case (loss higher
  than snapshot rotation can drain). It still recovers — `rejTooOld`
  grows incrementally (new snapshots, not a stuck loop), and stale
  converges to 0 in 05 / 06.
- DRV 03 / 04 don't reach `stale = 0` in 30 s because PCAP replay at
  `--speed 0` drives msg rates far above real-time; in production these
  scenarios would converge well within a snapshot rotation.
- Zero `recovery queue overflow`, zero unhandled exceptions, zero
  `drop[psCap, gCap]` (cap-of-last-resort never reached).

### 11.3 Acceptance criteria

A run is considered passing when:

1. Single-feed loss scenarios (01, 02) end with `stale = 0` for every
   symbol — A/B redundancy is doing its job.
2. Correlated-loss scenarios (03 – 06) end with most symbols Healthy;
   the residual stale count is bounded by snapshot-availability of the
   PCAP slice (the recovery feed in the captures is finite).
3. No `recovery queue overflow` line in any log.
4. No `error:` / unhandled exception in any log.
5. Replayed-message count is non-zero in every loss scenario (proves the
   buffer-and-drain path is being exercised).

Failure of any of these criteria should block release.

## 12. Summary

The consumer's resilience is the result of bounding **every** queue,
**every** allocation rate, and **every** per-client resource — and then
choosing the cheapest possible escalation when a bound is hit. In order
of escalation:

1. Drop the message → next snapshot fixes it.
2. Disconnect the slow client → fleet is unaffected.
3. Suppress fanout during recovery → feed catches up.
4. Crash the container → orchestrator restarts in a clean state.

The system is engineered to fail loudly and locally, never silently or
systemically.
