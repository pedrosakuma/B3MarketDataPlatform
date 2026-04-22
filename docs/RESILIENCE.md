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

## 2. Gap detection and recovery

`FeedHandler` tracks `MsgSeqNum` per channel. On a sequence gap it
transitions:

```
RealTime → Lost → (recovery available?) → Recovery → CatchUp → RealTime
```

**Incremental recovery (UDP)** receives the missing packets via a separate
multicast group. Recovered packets are queued in an `_incrementalQueue` (cap
**`UMDF_INCREMENTAL_RECOVERY_QUEUE_CAPACITY`**, default 200 000) and replayed
in order on the dispatch thread.

**Snapshot recovery (TCP)** is used when the gap is too wide for
incremental recovery. The snapshot client has bounded retry with
exponential backoff and emits `b3.umdf.recovery.snapshot.attempts`.

Recovery progress is observable via metrics:

```
b3.umdf.feed.state                  # 0=Init,1=RealTime,2=Lost,3=Recovery,4=CatchUp
b3.umdf.feed.gaps                   # gauge of currently-tracked gaps
b3.umdf.recovery.incremental.queued
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
| Per-group recovery queue (packets) | `UMDF_INCREMENTAL_RECOVERY_QUEUE_CAPACITY` | 200 000 |
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
