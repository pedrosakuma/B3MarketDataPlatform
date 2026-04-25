# SbeB3UmdfConsumer

Open-source C# consumer for [B3](https://www.b3.com.br/) market data over
the **Binary UMDF** (Unified Market Data Feed) protocol, encoded with
[SBE (Simple Binary Encoding)](https://github.com/FIXTradingCommunity/fix-simple-binary-encoding).

Uses the [`SbeSourceGenerator`](https://www.nuget.org/packages/SbeSourceGenerator/)
Roslyn source generator to produce zero-allocation, blittable C# structs
straight from the B3 SBE XML schema.

![Subscribed view](docs/images/02-subscribed.png)

## Highlights

- **Zero-copy SBE decoding** — generated blittable structs, no heap
  allocation per packet.
- **PCAP replay with cross-channel sync** — timestamp-based priority
  queue merges the four UMDF channels (Incremental A/B, Instrument
  Definition, Snapshot Recovery) into one chronologically-correct stream.
- **Live multicast transport** — UDP multicast with ASM/SSM, configurable
  `SO_RCVBUF`, bounded internal queues, `recvmmsg`/`sendmmsg` batching.
- **Multi-channel** — process several channel groups (e.g. EQT + DRV)
  simultaneously, each with its own per-group hot path (zero locks).
- **Gap detection & snapshot recovery** — gaps trigger snapshot recovery,
  catch-up, and back to real-time without losing client state.
- **Per-instrument heal (differentiator)** — exploits B3's unified
  per-`SecurityID` `rptSeq` (validated against PCAP: 175 116 cross-template
  advances, zero violations) so a single-symbol gap stales **only that
  symbol**, not the whole channel. Per-instrument heal is universal
  (cold-start, normal gap, wide burst — all the same path); the
  channel-level state machine reduces to `WaitInstrumentDefinition →
  Streaming` with no Recovery state to fall back to. Eliminates the
  30 – 100 s blanket `Lost → Recovery → CatchUp` cycles imposed by
  conventional consumers on liquid feeds. See
  [RESILIENCE.md §2](./docs/RESILIENCE.md#2-per-instrument-recovery-unified-rptseq).
- **Cascading-recovery loop structurally impossible** — the historical
  slow-client → drop → Recovery → more drops feedback loop is gone:
  there is no Recovery state to enter (see RESILIENCE.md §4). Cold-start
  fanout suppression and a mass-stale fanout gate remain as residual
  defenses for analogous failure modes.
- **WebSocket subscription server** — compact binary protocol with `Book`
  and `Info` channels, unary `Get`, candle history (10 h of 1 s candles),
  and 2 s rankings broadcast.
- **Layered backpressure** — bounded per-client outbound ring, hard
  pending-bytes cap with disconnect, outlier sweep, fanout suppression
  during recovery; clients can never impair feed consumption.
- **Web Worker frontend** — vanilla JS; worker owns the WebSocket and
  state, main thread only updates a pre-allocated DOM pool.
- **Operations** — `/health`, `/ready`, `/live` endpoints, 26
  `System.Diagnostics.Metrics` instruments (AOT-safe, zero NuGet deps),
  graceful SIGTERM drain, Docker hardening.

## Architecture

### Backend

```
┌─────────────────┐     ┌──────────────────┐
│ PcapReplay      │     │ Multicast UDP    │
│ (TimestampMerge)│     │ (MulticastSource)│
└────────┬────────┘     └────────┬─────────┘
         │  IPacketSource        │
         └──────────┬────────────┘
                    │
          ┌─────────▼─────────┐
          │ MultiFeedManager  │  ← routes by ChannelGroup
          └──┬──────────────┬─┘
             │              │
     ┌───────▼──────┐ ┌─────▼────────┐
     │ FeedHandler  │ │ FeedHandler  │   ← one per group
     │ (G0 / EQT)   │ │ (G1 / DRV)   │
     └───────┬──────┘ └─────┬────────┘
             │              │
     ┌───────▼──────┐ ┌─────▼────────┐   ← per-group, single-threaded
     │ BookManager  │ │ BookManager  │
     │ MarketDataMgr│ │ MarketDataMgr│
     │ GroupHandler │ │ GroupHandler │   (conflation buffers)
     └───────┬──────┘ └─────┬────────┘
             └──────┬───────┘
                    │
          SymbolRegistry (shared, FrozenDictionary)
                    │
       SubscriptionManager (central registry)
        ├─ ConcurrentDictionary + copy-on-write subscriptions
        ├─ subscribe / get routed to owning group's queue
        ├─ rankings aggregated across all groups
        └─ symbol registry promote (periodic FrozenDictionary rebuild)
                    │
                    ▼
              WebSocketHost
              (Kestrel, binary frames)
```

### Frontend (Web Worker)

```
┌──────────────────────────────────────────────────┐
│ Worker thread (worker.js)                         │
│                                                   │
│ WebSocket → parse binary → update state           │
│ (orders, trades, info, rankings, subscriptions)   │
│ compute MBP (bid/ask levels from order map)       │
│                                                   │
│ setInterval(16ms) → if dirty: postMessage         │
│ (render-ready frame with arrays/objects)          │
└─────────────────────┬────────────────────────────┘
                      │ postMessage (structured clone)
                      ▼
┌──────────────────────────────────────────────────┐
│ Main thread (app.js + ui.js)                      │
│                                                   │
│ onmessage → store in view → rAF render            │
│ DOM pool updates (.textContent only)              │
│ event delegation for UI actions                   │
│ postMessage commands back to worker               │
└──────────────────────────────────────────────────┘
```

## Projects

| Project | Description |
|---------|-------------|
| `B3.Umdf.Sbe` | SBE schema + source generator (generates all B3 message types) |
| `B3.Umdf.Transport` | UMDF packet header, multicast transport, `IPacketSource` / `IPacketSink` |
| `B3.Umdf.Feed` | Feed handler, gap detection, A/B dedup, message dispatch |
| `B3.Umdf.Book` | MBO book + per-symbol heal: `OrderBook`, `BookSide`, `BookManager`, `BookStore`, `SnapshotApplier`, `SymbolStateRegistry`, `StaleMboBuffer`, `MarketDataManager`, `SymbolRegistry`, `CandleAggregator` |
| `B3.Umdf.PcapReplay` | PCAP reader, UDP extractor, timestamp-merged replayer |
| `B3.Umdf.Server` | WebSocket subscription server: `WireProtocol`, `SubscriptionManager`, `SnapshotEmitter`, `OutlierSweeper`, `RankingsPublisher`, `RecoveryProgressPublisher`, `GroupConflationHandler`, `ClientSession`, `WebSocketHost`, `AppSettings` |
| `B3.Umdf.ConsoleApp` | CLI application — PCAP replay + optional WebSocket server + `AppMetrics` |

## Tests

| Project | Tests | Description |
|---------|-------|-------------|
| `B3.Umdf.Book.Tests` | 108 | Order book operations, book side, snapshot apply, symbol-state registry, stale buffer, candle aggregator, concurrency stress |
| `B3.Umdf.Feed.Tests` | 20 | Feed handler, gap detection, A/B dedup, MultiFeedManager dispatch |
| `B3.Umdf.PcapReplay.Tests` | 18 | PCAP reader, UDP/VLAN/SLL extraction, timestamp-merge ordering |
| `B3.Umdf.Transport.Tests` | 14 | Packet source, multicast config, batch receive |
| `B3.Umdf.Server.Tests` | 57 | Subscription manager, snapshot emitter, outlier sweep, conflation, client session, settings, backpressure |
| `B3.Umdf.ConsoleApp.Tests` | 29 | CLI option parsing, env-var precedence, multicast config validation |

```bash
dotnet build
dotnet test
```

## Quick start (Docker)

```bash
docker compose up --build
```

- **Backend** on `:8080` (WebSocket + REST), **frontend** on `:3000`.
- Open <http://localhost:3000>, click **Connect**, then subscribe to a
  symbol shown in the rankings panel.

For the split publisher / consumer topology that exercises the live UDP
path, the full CLI / env-var reference, and the multicast JSON format,
see [docs/OPERATIONS.md](docs/OPERATIONS.md) and
[docs/CONFIGURATION.md](docs/CONFIGURATION.md).

## B3 schema

This project uses the [B3 Market Data Messages v2.2.0](https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-developers-and-vendors/binary-umdf/)
SBE XML schema. The schema's `<!DOCTYPE xml>` declaration is removed
because .NET's `XmlReader` prohibits DTD processing by default. This is
the only modification to the original B3 schema.

## Documentation

| Document | What's inside |
|----------|---------------|
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Running locally and in Docker, web viewer screenshots, health endpoints, metrics, backpressure summary, graceful shutdown, TLS, profiling |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | CLI options, full `UMDF_*` environment variable reference, multicast JSON config, host kernel tuning, replay-speed range |
| [docs/WEBSOCKET-PROTOCOL.md](docs/WEBSOCKET-PROTOCOL.md) | Wire framing, message catalog, hex examples, subscription / reconnect / slow-consumer flows, candle chunking |
| [docs/PERFORMANCE.md](docs/PERFORMANCE.md) | Hot-path design, zero-copy decoding, MPSC ring, broadcaster decoupling, coalescing, benchmarks |
| [docs/RESILIENCE.md](docs/RESILIENCE.md) | Failure modes, gap recovery, fanout suppression, slow-consumer layered defenses, memory bounds, operational playbook |
| [docs/NOISY-NEIGHBOUR.md](docs/NOISY-NEIGHBOUR.md) | Behaviour under host-level resource contention, scheduler jitter probe, deployment hardening (cpuset, k8s static CPU manager, NIC IRQ pinning, sysctls) |

## References

- [B3 Binary UMDF Developer Page](https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-developers-and-vendors/binary-umdf/)
- [B3 Binary Market Data — Client Portal (SBE specs, schemas, release notes)](https://clientes.b3.com.br/en/w/binary-market-data)
- [B3 SBE PCAP samples (public Azure storage)](https://mktdatabinario.z15.web.core.windows.net/PCAPS/BinaryUMDF/SiteB3/) — downloaded by `tools/pcap/download-pcaps.sh`
- [SbeSourceGenerator](https://github.com/pedrosakuma/SbeSourceGenerator)
- [FIX Simple Binary Encoding](https://github.com/FIXTradingCommunity/fix-simple-binary-encoding)

## License

[MIT](LICENSE)
