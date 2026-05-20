# BookFeed (Phase 1) — empirical baseline

Captured 2026-05-20 against `B3.Umdf.ConsoleApp --ws-port 8080 --speed 50` replaying
`pcap/20250929_MBO_072_DRV_*.pcap` (16 mini-index futures: WIN/WI1/TF0). Client side
was `tools/BookFeedLoadHarness` subscribing all 16 with `SubscribeFlags.Book`.

## Headline

The client-side `BookFeed` is **not the bottleneck under realistic B3 traffic** — by
3+ orders of magnitude. Under sustained 3,200 events/s the harness consumed **1.5% of
one CPU core** and our SDK code accounted for **<0.05% of total CPU samples**
(≤100 of 226,883 samples over 30s). Every measurable hotspot is `Monitor.Enter_Slowpath`
on internal channel locks; `BookView`'s apply paths register as 1–3 samples each.

## Measurement note

The first run used `--speed 50` which compresses inter-burst gaps but preserves them —
the client saw long idle stretches between hot windows (~58/s mean, 270/s peak). That
under-represents how the `BookView` data structures actually sit under sustained load.
The numbers below come from `--speed 5`, which keeps timestamps realistic but ensures
a continuous stream of MBO events during the measurement window.

## Server (DRV-072 pcap @ 5×, 1m of capture window)

Per-(secId, side, price) conflation in `GroupConflationHandler` flattens dispatcher
throughput (hundreds of thousands of order ops/s) into hundreds–thousands of WS frames/s
per group. The harness window saw 277k WS `Changed` events for 16 symbols (mini-index
futures: WIN/WI1/TF0).

## Client (120s harness window, 16 symbols subscribed)

| metric | value |
| --- | --- |
| `BookFeed.Changed` invocations | 277,065 total (~2,309/s mean, **~3,200/s sustained** for first 42s, then ~1,600/s) |
| `BookSnapshot` events | 16 (one per subscribed symbol) |
| unique symbols touched | 16 / 16 |
| process CPU (under 3,200/s load) | **1.5%** |
| working set | 74 MB |
| GC heap size | 1.17 MB (stable) |
| Gen0 collections in 30s | 8 (max pause 1.5ms, total **5.5ms** = 0.018% time-in-GC) |
| Gen1 / Gen2 collections | 0 / 0 |
| monitor lock contention | 0 |
| exceptions | 0 |
| alloc rate | 1.17 MB/s |

## CPU sample breakdown (30s window during sustained 3,200/s)

Sampled 226,883 frames. Every top-15 hotspot is runtime infrastructure (idle
threadpool wait, semaphore, epoll, TimerThread). Drilling into our code:

| stack | inclusive samples | % of total |
| --- | --- | --- |
| `MarketDataClient.ReceiveLoopAsync` (decode + enqueue) | 79 | 0.035% |
|   ↳ `BoundedChannel.TryWrite → Monitor.Enter_Slowpath` | 36 | 0.016% |
| `MarketDataClient.DispatchLoopAsync` (handler invoke) | 18 | 0.008% |
|   ↳ `BookView.ApplyCleared` | 3 | 0.001% |
|   ↳ `BookFeed.OnAdded → BookView.ApplyAdded → Dictionary.set_Item` | 1 | <0.001% |

The lone non-runtime hotspot is the SDK's internal `BoundedChannel` between receive
and dispatch — and even that is 0.016% of CPU.

## Implication for issue #43 Phase 2

The original planning comment asked for a "performance gate" (p50/p99 on hot
symbols) before shipping depth>1 + sorted structures. Empirically that gate is
trivially met by any reasonable implementation — even a `SortedDictionary<decimal,
SideBucket>` would be operating at <1% of headroom, since the existing
`Dictionary`-based `BookView` already takes 1–3 samples out of 226,883 (<0.001%).

Reframe Phase 2 as **API / functional completeness** (depth>1 views, zero-alloc
`CopyLevels(Span<L2Level>)`, eviction policy), not performance. Microbenchmarks in
`benchmarks/` should still cover the data-structure choice for documentation and
regression detection, but they're not blockers.

## How to reproduce

```sh
# 1) Build
dotnet build -c Release

# 2) Start server replaying DRV pcap at 5× speed with WS on 8080
#    (5× compresses the timeline enough to keep a continuous event stream
#     during the measurement window, without distorting the burst shape)
dotnet src/B3.Umdf.ConsoleApp/bin/Release/net10.0/B3.Umdf.ConsoleApp.dll \
  --ws-port 8080 --speed 5 \
  pcap/20250929_MBO_072_DRV_Incremental_FeedA.pcap \
  pcap/20250929_MBO_072_DRV_Incremental_FeedB.pcap \
  pcap/20250929_MBO_072_DRV_InstrumentDefinition.pcap \
  pcap/20250929_MBO_072_DRV_SnapshotRecovery.pcap &

# 3) Run harness — discovers symbols via GET /symbols, subscribes Book, counts
dotnet tools/BookFeedLoadHarness/bin/Release/net10.0/bookfeed-load-harness.dll \
  --duration 120 --warmup 5
```

Attach `dotnet-diagnostics` (`snapshot_counters` / `collect_cpu_sample` /
`collect_gc_events`) to the harness PID printed at startup. The CPU sample drill-down
that produced the breakdown above used `get_call_tree` with
`rootMethodFilter=MarketDataClient` and `rootMethodFilter=DispatchLoop`.
