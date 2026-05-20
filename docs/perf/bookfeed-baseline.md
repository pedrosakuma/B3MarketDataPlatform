# BookFeed (Phase 1) — empirical baseline

Captured 2026-05-20 against `B3.Umdf.ConsoleApp --ws-port 8080 --speed 50` replaying
`pcap/20250929_MBO_072_DRV_*.pcap` (16 mini-index futures: WIN/WI1/TF0). Client side
was `tools/BookFeedLoadHarness` subscribing all 16 with `SubscribeFlags.Book`.

## Headline

The client-side `BookFeed` is **not the bottleneck under realistic B3 traffic** — by
3 orders of magnitude. The server's per-`(secId, side, price)` conflation flattens
~22M raw MBO ops into ~125k WebSocket frames (≈175× ratio), so even hot symbols feed
the SDK at hundreds of events/sec, not tens of thousands.

## Server (1m48s of pcap @ 50×)

| metric | value |
| --- | --- |
| dispatcher throughput | 420k pkts/s avg, ~490k peak |
| order adds | 10,601,720 |
| order updates | 1,312,022 |
| order deletes | 10,390,417 |
| trades parsed (SBE) | 5,836,333 |
| trades emitted (WS) | 88,414 |
| **conflated frames emitted (WS, all groups)** | **125,473** |

## Client (60s harness window, 16 symbols subscribed)

| metric | value |
| --- | --- |
| `BookFeed.Changed` invocations | 3,494 total (~58/s mean, 270/s peak) |
| `BookSnapshot` events | 151 |
| unique symbols touched | 11 / 16 |
| process CPU | 0.9% |
| working set | 76 MB |
| GC heap size | 1.4 MB |
| Gen0 collections in 40s | 1 (1.4ms pause) |
| Gen1 / Gen2 collections | 0 / 0 |
| monitor lock contention | 0 |
| alloc rate | 235 KB/s |

## Implication for issue #43 Phase 2

The original planning comment asked for a "performance gate" (p50/p99 on hot
symbols) before shipping depth>1 + sorted structures. Empirically that gate is
trivially met by any reasonable implementation — even a `SortedDictionary<decimal,
SideBucket>` would be operating at <1% of headroom.

Reframe Phase 2 as **API / functional completeness** (depth>1 views, zero-alloc
`CopyLevels(Span<L2Level>)`, eviction policy), not performance. Microbenchmarks in
`benchmarks/` should still cover the data-structure choice for documentation and
regression detection, but they're not blockers.

## How to reproduce

```sh
# 1) Build
dotnet build -c Release

# 2) Start server replaying DRV pcap at 50× speed with WS on 8080
dotnet src/B3.Umdf.ConsoleApp/bin/Release/net10.0/B3.Umdf.ConsoleApp.dll \
  --ws-port 8080 --speed 50 \
  pcap/20250929_MBO_072_DRV_Incremental_FeedA.pcap \
  pcap/20250929_MBO_072_DRV_Incremental_FeedB.pcap \
  pcap/20250929_MBO_072_DRV_InstrumentDefinition.pcap \
  pcap/20250929_MBO_072_DRV_SnapshotRecovery.pcap &

# 3) Run harness — discovers symbols via GET /symbols, subscribes Book, counts
dotnet tools/BookFeedLoadHarness/bin/Release/net10.0/bookfeed-load-harness.dll \
  --duration 60 --warmup 5
```

Attach `dotnet-diagnostics` (snapshot_counters / collect_cpu_sample / collect_gc_events)
to the harness PID printed at startup for runtime metrics.
