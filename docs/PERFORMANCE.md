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
packets/s) end-to-end through ingest + apply. See [README §
Performance](../README.md#performance) for the full benchmark.

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

## 14. Summary

The throughput envelope is achieved by a small set of repeated patterns:

1. Decode in place, never copy.
2. Single-threaded ownership per group, no locks on the hot path.
3. Bounded queues at every stage so back-pressure is local, not global.
4. Pooled buffers; allocations only on the cold paths (subscribe, snapshot).
5. Coalesce work to the largest unit consumers can tolerate.

Each of those patterns is enforced by a specific piece of code referenced
above; this document exists so reviewers can see *why* a change must
respect them.
