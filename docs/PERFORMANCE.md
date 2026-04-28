# Performance Strategy

This document describes the performance design of `SbeB3UmdfConsumer`: which
hot paths matter, what they look like in code, and the engineering choices
that keep them fast.

For fault-tolerance/resilience design see [RESILIENCE.md](./RESILIENCE.md).

## 1. Hot paths

The consumer has three latency-sensitive pipelines that determine end-to-end
performance:

| Pipeline | Hot path | Owner |
| -------- | -------- | ----- |
| **Ingest** | UDP socket → SBE decode → state machine | `MulticastPacketSource` → `MultiFeedManager` → `FeedHandler` |
| **Apply** | Decoded message → order book / market data update | `BookManager`, `MarketDataManager` |
| **Fanout** | Update → conflation → coalesced WS write | `GroupConflationHandler` → broadcaster thread → `ClientSession` |

Every cross-pipeline boundary uses a **bounded, lock-free queue** so a slow
stage cannot stall a fast upstream. All three pipelines run in parallel.

## 2. Zero-copy SBE decoding

B3 wire messages are decoded with [`SbeSourceGenerator`][sbegen]. The
generator emits **blittable C# `struct`s** that overlay the wire bytes
directly — no per-field allocation, no boxing, no reflection.

```csharp
// inside FeedHandler hot path:
var msg = new Trade_53V15(packet.Data, offset, blockLength);
var price = msg.MdEntryPx.Mantissa;   // direct memory read
```

A 52M-packet PCAP replay completes in ~30 s on a single thread (~1.7M
packets/s) end-to-end through ingest + apply.

[sbegen]: https://github.com/pedrosakuma/SbeSourceGenerator

## 3. Per-group, single-threaded apply path

Each B3 channel group (EQT, DRV, …) owns its own:

- `BookManager` (the order books for that group's symbols)
- `MarketDataManager` (instrument info)
- `GroupConflationHandler` (event buffers + broadcaster ring)
- `FeedHandler` state machine

A dedicated **dispatch thread** drains the group's MPSC ring and calls into
all of the above. Because ownership is per-thread, the apply path never
takes a lock on the hot path:

- `BookSide` is a sorted `Dictionary<long, OrderBookEntry>` mutated only
  from the dispatch thread.
- `OrderBookEntry` is a **`struct`** (commit `ba4bc2b`) so adds/updates do
  not allocate.
- Subscription state is read lock-free from the dispatch thread — see
  the **copy-on-write** pattern below.

## 4. Lock-free MPSC dispatch ring

Receive threads push raw packets into a per-group **`MpscPacketRing`**
(commit `17f3b7e`):

- Fixed-size power-of-two ring (default 65 536 slots/group).
- Producers (recv threads) commit with a single `Interlocked.CompareExchange`
  on the head index.
- Consumer (dispatch thread) reads with plain volatile loads.
- Producer waking is amortized via a `consumer-waiting` flag (commit
  `feacf9d`) — only one wake per drain cycle, not one per push.

**Profile delta** (REPLAY_SPEED=2): the previous per-group `Monitor` lock
showed ~952 samples in `Monitor.Enter_Slowpath`; after the ring it dropped
to ~186 (-80 % slow-path contention).

On overflow the ring drops the **newest** packet and increments
`b3.umdf.feed.ring_dropped`; downstream gap detection then triggers
recovery — no producer is ever blocked.

## 5. Decoupled broadcaster thread

The broadcaster runs on a **separate thread per group** (commit `dc85a8c`).
The dispatch thread does only conflation + accumulation (`OnBatchComplete`),
then `BroadcastRing.TryEnqueue(batch)`. Per-client serialization,
`ArrayPool` rental, and `SendAsync` all happen on the broadcaster thread.

Why this matters: `SendAsync` is dominated by Kestrel pipe-lock acquisitions
proportional to client count. With 500 clients × ~5k events/s the dispatch
thread spent >70 % of CPU inside `SendAsync` before this change. After
decoupling, dispatch sustains 100 k+ events/s on a single core.

## 6. Coalesced per-client batching

Events are coalesced into a single WS frame via two complementary windows:

| Layer | Window | Purpose |
| ----- | ------ | ------- |
| **Server-side flush** | 1 packet (per `OnBatchComplete`) | Drop redundant updates; same-order add+delete cancels; same-price trades sum quantities |
| **Per-client write loop** | `UMDF_CLIENT_COALESCE_WINDOW_MS` (default 10 ms) | After the first item, wait *N* ms before draining → larger frames, fewer pipe-lock acquisitions |

Empirically, raising the per-client window from 0 → 10 ms reduces total
syscalls by ~6× without measurably increasing client-perceived latency for
hundreds of concurrent connections (commit `040cfa4`).

> The window is a **soft latency knob**. 0 = immediate (lowest latency,
> highest CPU). 10–20 ms = sweet spot for hundreds of clients. >50 ms
> increases per-client pending memory materially.

## 7. Snapshot rate-limit on the dispatch thread

Each `Subscribe`/`Get` request runs `SendMboSnapshot` on the dispatch
thread, which:

1. Snapshots the book side into a tuple array (allocation: `(long, long, int)[]`).
2. Rents an `ArrayPool<byte>` buffer sized proportionally to the book.

A connect storm (e.g. 500 clients × 200 symbols ≈ 100 000 snapshots) easily
exceeds the GC's ability to keep up. `MaxSnapshotRequestsPerBatch` (default
**32**, env `UMDF_MAX_SNAPSHOT_REQUESTS_PER_BATCH`) caps how many of these
the dispatch thread services per packet — excess requests stay queued and
drain on subsequent packets.

At 32/batch × ~1 000 batches/s = 32 000 snapshots/s steady-state capacity,
which is far above any realistic connect rate while keeping the dispatch
thread's allocation rate bounded (commit `d2099bb`).

## 8. Pooled buffers everywhere

Allocation is the most reliable path to GC pauses. The hot paths all use
pooled or reused buffers:

- **`ArrayPool<byte>`** for snapshot payloads, per-flush per-client
  broadcast buffers (commits `4027e82`, `2364ee7`).
- **`PinnedBufferPool`** for UDP receive (POH-resident, no GC compaction).
- **Reused `UmdfPacket[64]`** for `recvmmsg` batch receive.
- **`stackalloc Span<byte>`** for fixed-size wire encodings (e.g. order
  add/delete frames are ~40 bytes).
- **Conflation `Dictionary`s** (`_orderBuffer`, `_clearBuffer`,
  `_tradeBuffer`) reused across packets — only `.Clear()` between flushes.
- **No closure capture on the hot MBO route.** `RouteMbo` was rewritten
  in P11 to take an explicit `(BookManager bm, in MboMessage msg)` instead
  of capturing locals into a delegate; this removed a per-message display-class
  allocation that dominated MBO-heavy bursts.

## 9. Lock-free subscription reads

Subscription mutations (subscribe/unsubscribe) are rare and serialized
under a single light-weight `_subLock`. Reads on the hot path are
**lock-free**: the inner `Dictionary<string, DataFlags>` is a
**copy-on-write snapshot** and the outer `ConcurrentDictionary` provides
lock-free `TryGetValue`. Iteration during a fanout never blocks subscribe.

```csharp
// Hot path: per-event, ~10× per packet
internal void BroadcastToSubscribers(ulong securityId, ReadOnlyMemory<byte> payload)
{
    if (!_subscriptions.TryGetValue(securityId, out var clients)) return;
    foreach (var (clientId, flags) in clients)        // safe: copy-on-write
    { ... }
}
```

## 10. `recvmmsg` / `sendmmsg` batching

On Linux the consumer uses the kernel batch syscalls when available
(`MulticastPacketSource.IsBatchReceiveSupported`), pulling up to 64
datagrams per system call. The publisher uses the matching `sendmmsg`. At
B3 production rates (~30 k pkts/s/group) this reduces syscall overhead by
roughly 30–60×.

## 11. Periodic SymbolRegistry freeze

Symbol lookups are dominated by reads. The registry uses a
`ConcurrentDictionary` for live writes and **periodically promotes** the
view to a `FrozenDictionary` (lookup is ~2× faster for the read-mostly
workload). Mid-session new instruments still go through the concurrent
side and become visible at the next freeze.

## 12. Numbers

### PCAP replay (in-process)

52 M packets, EQT 2025-03-31, ~20 GB:

| Metric | Value |
| ------ | ----- |
| Total time | ~30 s |
| Throughput | ~1.7 M pkts/s |

### Live multicast (production-like, dual group, REPLAY_SPEED=2)

`docker-compose.multicast.yml`, EQT + DRV, single 2-CPU / 4-GiB container,
no clients:

| Metric | Value |
| ------ | ----- |
| Sustained throughput | ~18 k pkts/s/group |
| Consumer CPU | ~30 % of one core |
| Consumer RSS | ~550 MiB |
| Recovery cycles in steady state | 0 |

### High-fanout stress (200 clients, 15-30 % slow, REPLAY_SPEED=2)

200 WebSocket clients (200 symbols each = 40 k subscriptions) over a
180 s run on the same 2-CPU / 4-GiB container, with 15 – 30 % of clients
artificially slowed via TCP socket pause/resume:

| Metric | Value |
| ------ | ----- |
| Per-process throughput at the client | 150 k – 700 k msgs/s |
| Consumer CPU | ~200 % (both cores fully used) |
| Consumer RSS | ~2.0 GiB stable |
| Slow disconnects (matched to slow clients) | 100 % of `closes` |
| Healthy clients disconnected | 0 |

The hard pending-bytes cap surgically removes the genuinely-stuck
consumers; healthy clients never see degradation. See
[RESILIENCE.md](./RESILIENCE.md) for the slow-consumer protection
layers.

### Profiling

```bash
dotnet tool install -g dotnet-trace dotnet-counters
./tools/profile/profile.sh

# live counters during a run
dotnet-counters monitor --counters B3.Umdf.Consumer --process-id <pid>
```

Recommended trace types: `gc-collect`, `cpu-sampling`, `database-async`
(for the publisher only).

## 13. What we tried and rejected

| Approach | Why rejected |
| -------- | ------------ |
| Per-client `Channel<OutboundMessage>` | Channel allocates a node per message; replaced with custom MPSC ring (commit `57e1cc3`) |
| Pause/resume of the **kernel** UDP socket during recovery | Recovery already drops packets — pausing the socket adds latency without solving the cascading-recovery cause; instead we suppress fanout (see RESILIENCE.md) |
| `SO_REUSEPORT` with N receive sockets per channel | Linux multicast delivers a copy to each socket — multiplies CPU without enlarging effective `SO_RCVBUF` |
| AOT vs JIT for runtime perf | Both produce ~25 – 27 k msgs/s in the regime we tested; AOT used for startup time + container size, not throughput |

## 14. Profile snapshot (PCAP replay)

The numbers below come from `dotnet-trace` + `dotnet-counters` against the
real consumer pipeline (no synthetic micro-bench). Two PCAPs are used:

| Capture | Symbols | Duration | Pkts (max-burst) | Events | Time |
| ------- | ------- | -------- | ---------------- | ------ | ---- |
| `20250331_MBO_084_EQT*` | 18,380 | 5 min | 5.8 M | 5.2 M | 5 s |
| `20250929_MBO_072_DRV*` |     16 | full   | 27 M+ buffered | n/a | 5 s |

Reproduce with:

```sh
dotnet-trace collect --format speedscope -o /tmp/perf/trace-eqt.nettrace \
  --providers Microsoft-DotNETCore-SampleProfiler --duration 00:00:30 \
  -- dotnet src/B3.Umdf.ConsoleApp/bin/Release/net10.0/B3.Umdf.ConsoleApp.dll \
     --pcap-prefix pcap/20250331_MBO_084_EQT --speed 0
dotnet-trace report /tmp/perf/trace-eqt.nettrace topN -n 25
```

For runtime counters (alloc rate, GC, working-set):

```sh
nohup dotnet …ConsoleApp.dll --pcap-prefix pcap/…_EQT --speed 0 &
dotnet-counters collect --process-id $! --duration 00:00:08 --format csv \
  --counters B3.Umdf.Consumer,System.Runtime -o /tmp/perf/counters.csv
```

### 14.1 Top exclusive frames (CPU sample profile)

Exclusive percentages are of total trace wall-time. Active CPU was
≈ 14.5 % of wall (replayer finished in 5 s of a 30 s trace), so multiply by
≈ 7 to get share of *active* CPU.

| Frame | EQT base | EQT loss 1 % corr. | DRV base |
| ----- | --------:| ------------------:| --------:|
| Wait/semaphore (idle thread-pool)         | 85.6 % | 85.2 % | 85.4 % |
| `Thread.PollGCWorker`                     |  9.7 % |  9.0 % | 12.3 % |
| `MarketDataManager.HandleSecurityGroupPhase` | 2.6 % | 1.8 % | – |
| `Monitor.Enter_Slowpath`                  |  1.8 % |  3.2 % |  1.1 % |
| `TimestampMergedReplayer.TryReceive` (incl) | 8.5 % | 5.7 % | 13.2 % |

Observations:

- **GC poll dominates active CPU** (60 – 85 % of active depending on
  capture). PollGC fires at runtime safepoints in proportion to
  allocation pressure; the level here matches the 70 MB/s sustained
  allocation rate seen in counters (§14.2).
- **Lock contention nearly doubles under loss** (1.8 % → 3.2 %
  exclusive). The recovery path (`StaleMboBuffer.Drain`,
  per-symbol epoch handoff) takes additional locks per affected
  symbol. It is still small absolute but is a cheap optimisation
  target because the hot-path handlers are otherwise lock-free.
- **DRV is PCAP-read-bound**: `TryReceive` is 13 % wall = 91 % of
  active feed-thread time. With only 16 symbols there is very little
  per-symbol work, so the merged replayer’s timestamp arbitration and
  buffer copy dominate. Live UDP would not show this.

### 14.2 Runtime counters (8 s burst, EQT)

| Counter | Value |
| ------- | ----- |
| Packets/s                              | 1.2 – 1.3 M |
| Events/s                               | 0.87 M      |
| `feed.duplicates` (A/B redundancy)     | 0.5 – 0.65 M/s |
| Allocation rate (`gc.heap.total_allocated`) | ~70 MB/s |
| GC pause time                           | ~30 ms/s (3 % wall) |
| Gen0/Gen1/Gen2 collections              | 1 / 1 / 0 in 7 s |
| Working-set growth                      | 567 MB → 2.1 GB in 7 s (+220 MB/s) |
| `monitor.lock_contentions`              | 0 / s (no contended waits) |
| `snapshots_skipped_healthy_ahead`       | 30 – 90 k/s |
| `mbo_stale_transitions`                 | 0/s (no spurious stales) |

### 14.3 Bottlenecks identified

1. **Allocation pressure (~70 MB/s)** drives GC poll overhead. The
   working-set climbs ~220 MB/s during burst replay because every
   1-second candle is retained for 10 h × 18,380 symbols (§9 of this
   doc covers the candle store). This is by design for the dashboard
   but means a long live session needs ≥ 4 GB headroom.
2. **`MarketDataManager.HandleSecurityGroupPhase` 2.6 % exclusive** is
   the biggest non-runtime hot frame. It currently does a `switch` on
   the SBE template id and routes to the per-kind handler; opportunity
   to inline the small handlers and eliminate the boxing of the
   `IMarketDataEventHandler` callback (`OnMarketDataUpdated` shows up
   at 0.08 % excl as well).
3. **Recovery-path locks** (`Monitor.Enter_Slowpath` doubling under
   loss) point at `StaleMboBuffer` / `SymbolStateRegistry`; converting
   the per-symbol mutex to a copy-on-write epoch swap would eliminate
   the contention without changing semantics.
4. **DRV PCAP read path** is dominant in DRV measurements. Not a
   live-path concern (UDP source is different code) but worth fixing
   for replay-driven CI: `TimestampMergedReplayer` allocates per
   call (the 0.93 % exclusive in `TryReceive` includes
   `Buffer.MemmoveInternal`). A pre-warmed batch read would amortise.

### 14.4 What is *not* a problem

- **No spurious stales**, no replay overflow, no channel-gap absorbs
  in either capture — Phase A correctness work holds under load.
- **Thread-pool queue length stays at 0** — the pipeline never
  back-pressures the OS thread pool.
- **JIT cost is one-shot** (~0.5 s total in first 3 s) and falls to
  near zero after warmup; not worth AOT-ing for throughput.

## 15. Summary

The throughput envelope is achieved by a small set of repeated patterns:

1. Decode in place, never copy.
2. Single-threaded ownership per group, no locks on the hot path.
3. Bounded queues at every stage so back-pressure is local, not global.
4. Pooled buffers; allocations only on the cold paths (subscribe, snapshot).
5. Coalesce work to the largest unit consumers can tolerate.

Each of those patterns is enforced by a specific piece of code referenced
above; this document exists so reviewers can see *why* a change must
respect them.
