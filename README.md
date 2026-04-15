# SbeB3UmdfConsumer

Open-source C# application for consuming [B3](https://www.b3.com.br/) market data via the **Binary UMDF** (Unified Market Data Feed) protocol using [SBE (Simple Binary Encoding)](https://github.com/FIXTradingCommunity/fix-simple-binary-encoding).

Uses the [`SbeSourceGenerator`](https://www.nuget.org/packages/SbeSourceGenerator/) Roslyn source generator to produce zero-allocation, high-performance C# structs directly from the B3 SBE XML schema.

## Features

### Market Data Engine
- **Zero-copy SBE decoding** — generated blittable structs via `SbeSourceGenerator`
- **PCAP replay with cross-channel sync** — timestamp-based priority queue merge across all UMDF channels (Incremental A/B, Instrument Definition, Snapshot Recovery)
- **Live multicast transport** — UDP multicast with source-specific multicast (SSM) support
- **Multi-channel support** — process multiple channel groups simultaneously (e.g. EQT + DRV)
- **Per-group architecture** — each channel group owns its `BookManager`, `MarketDataManager`, and conflation buffers; single-threaded hot path with zero locks
- **Feed A/B deduplication** — automatic duplicate packet filtering
- **Gap detection & snapshot recovery** — detects missing packets, transitions to snapshot recovery, catch-up and back to real-time
- **Market-by-Order (MBO) book** — full order book maintenance per instrument
- **Market data aggregation** — instrument info with 22+ fields (prices, volumes, bands, status) plus SecurityDefinition repeating groups (underlyings, legs, attributes) and SecurityDesc
- **Symbol registry with periodic freeze** — `ConcurrentDictionary` for real-time writes, periodically promoted to `FrozenDictionary` for fast lookups; supports mid-session instrument listings (e.g. new options series)

### WebSocket Server
- **Binary subscription protocol** — compact framing header for real-time data streaming
- **Data channel filtering** — subscribe to Book, Info, or both via `DataFlags` bitmask
- **Unary Get** — one-shot snapshot without subscribing
- **Upstream conflation** — order add+delete within the same packet are cancelled; same-price trades aggregate quantities
- **Rankings** — top 10 by volume, gainers, and losers pushed every 2s
- **Server status broadcast** — sends ready state on connect; auto-resubscribes all instruments on reconnect
- **Backpressure** — bounded per-client outbound queue, slow-client detection and auto-disconnect

### Web Frontend
- **Web Worker architecture** — worker thread owns WebSocket connection, message parsing, state management, and MBP computation; main thread only renders DOM
- **DOM pooling** — pre-allocated DOM elements for book, trades, info, rankings, and subscriptions; updates via `.textContent` only, no `innerHTML` on hot paths
- **Dirty-flag render loop** — bitfield tracking which panels changed, single `requestAnimationFrame` per frame
- **Responsive layout** — CSS media queries for tablet (≤900px) and mobile (≤600px); sidebar collapses to slide-out drawer with hamburger toggle; panels stack to single column
- **Order book with depth bars** — 15 bid/ask levels with quantity visualization
- **Trade log** — 50 most recent trades with time, price, and quantity
- **Rankings panel** — volume, gainers, and losers tabs with click-to-subscribe
- **Instrument info grid** — 22 market data fields with configurable price decimal display
- **Instrument detail modal** — full instrument metadata via REST endpoint, including repeating groups (underlyings, legs, attributes), SecurityDesc, CFI Code interpretation (ISO 10962); keyboard shortcuts (Ctrl+I, Alt+↑/↓)
- **Event log** — subscription events, connection status, errors
- **Auto-reconnect** — exponential backoff with configurable toggle; automatic resubscription of all instruments when feed is ready

### Operations
- **Docker Compose** — one command to run backend + frontend with PCAP replay
- **Health endpoints** — `/health`, `/ready`, `/live` for Kubernetes probes
- **Feed queue monitoring** — per-group channel depth exposed in console output
- **OpenTelemetry metrics** — `System.Diagnostics.Metrics` with 26 observable instruments (counters/gauges) covering feed, book, market data, and server subsystems; zero dependencies, fully AOT-safe
- **Structured logging** — `ILogger<T>` throughout with structured log templates
- **Graceful shutdown** — SIGTERM handling with ordered drain
- **Configuration** — JSON + environment variable config (`UMDF_*` prefix)
- **Docker hardening** — non-root user, HEALTHCHECK, resource limits

## Architecture

### Backend

```
┌─────────────────┐     ┌──────────────────┐
│  PcapReplay      │     │  Multicast UDP    │
│  (TimestampMerge)│     │  (MulticastSource)│
└────────┬────────┘     └────────┬─────────┘
         │  IPacketSource        │
         └──────────┬────────────┘
                    │
          ┌─────────▼──────────┐
          │  MultiFeedManager   │   ← routes by ChannelGroup
          │  (multi-channel)    │
          └──┬──────────────┬──┘
             │              │
     ┌───────▼──────┐ ┌────▼─────────┐
     │ FeedHandler   │ │ FeedHandler   │  ← one per channel group
     │ (Group 0/EQT) │ │ (Group 1/DRV) │
     └───────┬──────┘ └────┬─────────┘
             │              │
     ┌───────▼──────┐ ┌────▼─────────┐   ← per-group, single-threaded
     │ BookManager   │ │ BookManager   │
     │ MarketDataMgr │ │ MarketDataMgr │
     │ GroupHandler   │ │ GroupHandler   │   (conflation buffers)
     └───────┬──────┘ └────┬─────────┘
             └──────┬───────┘
                    │
          SymbolRegistry (shared, FrozenDictionary)
                    │
      SubscriptionManager (central registry)
       ├── ConcurrentDictionary + copy-on-write subscriptions
       ├── subscribe/get routed to owning group's queue
       ├── rankings aggregated across all groups
       └── symbol registry promote (periodic FrozenDictionary rebuild)
                    │
                    ▼
              WebSocketHost
              (Kestrel, binary frames)
```

### Frontend (Web Worker)

```
┌─────────────────────────────────────────────────┐
│  Worker Thread (worker.js)                       │
│                                                  │
│  WebSocket ──► parse binary ──► update state     │
│  (orders, trades, info, rankings, subscriptions) │
│  compute MBP (bid/ask levels from order map)     │
│                                                  │
│  setInterval(16ms) ──► if dirty: postMessage     │
│  (render-ready frame with arrays/objects)         │
└────────────────────┬────────────────────────────┘
                     │ postMessage (structured clone)
                     ▼
┌────────────────────────────────────────────────┐
│  Main Thread (app.js + ui.js)                   │
│                                                 │
│  onmessage ──► store in view ──► rAF render     │
│  DOM pool updates (.textContent only)            │
│  event delegation for UI actions                 │
│  postMessage commands back to worker             │
└─────────────────────────────────────────────────┘
```

## Projects

| Project | Description |
|---------|-------------|
| `B3.Umdf.Sbe` | SBE schema + source generator (generates all B3 message types) |
| `B3.Umdf.Transport` | UMDF packet header, multicast transport, `IPacketSource`/`IPacketSink` |
| `B3.Umdf.Feed` | Feed handler, gap detection, A/B dedup, message dispatch |
| `B3.Umdf.Book` | Market-by-Order book: `OrderBook`, `BookSide`, `BookManager`, `MarketDataManager`, `SymbolRegistry` |
| `B3.Umdf.PcapReplay` | PCAP reader, UDP extractor, timestamp-merged replayer |
| `B3.Umdf.Server` | WebSocket subscription server: `WireProtocol`, `SubscriptionManager`, `ClientSession`, `WebSocketHost`, `AppSettings` |
| `B3.Umdf.ConsoleApp` | CLI application — PCAP replay + optional WebSocket server + `AppMetrics` (OTEL instruments) |

## Tests

| Project | Tests | Description |
|---------|-------|-------------|
| `B3.Umdf.Book.Tests` | 18 | Order book operations, book side, concurrency stress |
| `B3.Umdf.Feed.Tests` | 4 | Channel handler, gap detection |
| `B3.Umdf.PcapReplay.Tests` | 4 | PCAP reader, timestamp merge |
| `B3.Umdf.Transport.Tests` | 5 | Packet source, multicast config |
| `B3.Umdf.Server.Tests` | 7 | Subscription manager, client session, settings |

## Quick Start

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)

### Build & Test

```bash
dotnet build
dotnet test
```

### Download PCAP Examples

B3 provides sample PCAP files for development:

```bash
./tools/download-pcaps.sh
```

### Run with PCAP Replay (Single Channel)

```bash
dotnet run --project src/B3.Umdf.ConsoleApp -- \
  pcap/20250331_MBO_084_EQT_Incremental_FeedA.pcap \
  pcap/20250331_MBO_084_EQT_Incremental_FeedB.pcap \
  pcap/20250331_MBO_084_EQT_InstrumentDefinition.pcap \
  pcap/20250331_MBO_084_EQT_SnapshotRecovery.pcap
```

### Run with PCAP Replay (Multi-Channel)

Use `--pcap-prefix` to auto-discover the 4 PCAP files per channel group:

```bash
dotnet run --project src/B3.Umdf.ConsoleApp -- \
  --pcap-prefix pcap/20250331_MBO_084_EQT \
  --pcap-prefix pcap/20250929_MBO_072_DRV \
  --ws-port 8080 --speed 5
```

Each prefix expands to `{prefix}_Incremental_FeedA.pcap`, `_Incremental_FeedB.pcap`, `_InstrumentDefinition.pcap`, and `_SnapshotRecovery.pcap`.

### Run with Live Multicast (UDP)

For production or certification environments with B3 multicast feeds:

```bash
dotnet run --project src/B3.Umdf.ConsoleApp -- \
  --multicast-config config/multicast-sample.json \
  --ws-port 8080
```

The JSON config defines multicast group addresses and ports for each channel. See `config/multicast-sample.json` for the format.

### Run with WebSocket Server

```bash
dotnet run --project src/B3.Umdf.ConsoleApp -- \
  pcap/20250331_MBO_084_EQT_Incremental_FeedA.pcap \
  pcap/20250331_MBO_084_EQT_Incremental_FeedB.pcap \
  pcap/20250331_MBO_084_EQT_InstrumentDefinition.pcap \
  pcap/20250331_MBO_084_EQT_SnapshotRecovery.pcap \
  --ws-port 8080 --speed 5
```

Then open `frontend/index.html` in a browser and connect to `ws://localhost:8080/ws`.

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--pcap-prefix <path>` | — | PCAP file prefix (repeatable for multi-channel). Auto-discovers 4 files per prefix |
| `--multicast-config <file>` | — | JSON config with multicast group addresses/ports for live UDP |
| `--ws-port <port>` | *(off)* | Start WebSocket subscription server on the given port |
| `--speed <mult>` | `0` | Replay speed: `0` = max, `1` = real-time, `5` = 5× accelerated |

Positional arguments (4 PCAP file paths) are also supported for single-channel backward compatibility.

## Docker Compose

One command to run the full stack (backend + frontend):

```bash
docker compose up --build
```

- **Backend** (port 8080): .NET app replaying PCAPs with WebSocket server
- **Frontend** (port 3000): nginx serving the web viewer

Open http://localhost:3000, connect to `ws://localhost:8080/ws`, and subscribe to a symbol.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PCAP_PREFIX` | `20250331_MBO_084_EQT` | Comma-separated PCAP prefixes (multi-channel: `EQT,DRV`) |
| `WS_PORT` | `8080` | WebSocket server port |
| `REPLAY_SPEED` | `5` | Replay speed multiplier |
| `FRONTEND_PORT` | `3000` | Frontend HTTP port |

```bash
# Both equities and derivatives (default)
docker compose up --build

# Derivatives only at real-time speed
PCAP_PREFIX=20250929_MBO_072_DRV REPLAY_SPEED=1 docker compose up --build

# Multi-channel explicit
PCAP_PREFIX=20250331_MBO_084_EQT,20250929_MBO_072_DRV docker compose up --build
```

## WebSocket Binary Protocol

All messages use a 4-byte framing header: `[u16 messageLength][u16 messageType]`, little-endian.

### Client → Server

| Message | Type | Payload |
|---------|------|---------|
| **Subscribe** | `0x0001` | `[flags u8][symbolLen u8][symbol...]` |
| **Unsubscribe** | `0x0002` | `[securityId u64]` |
| **Get** | `0x0003` | `[flags u8][symbolLen u8][symbol...]` |

**DataFlags** bitmask: `Book = 0x01`, `Info = 0x02`, `All = 0x03`. Sending `0x00` is treated as `All`.

- **Subscribe**: returns `SubscribeOk` + snapshot(s) + incremental updates filtered by flags
- **Get**: returns snapshot(s) only (no subscription)

### Server → Client

| Message | Type | Payload |
|---------|------|---------|
| **SubscribeOk** | `0x0010` | `[securityId u64][flags u8][symbolLen u8][symbol...]` |
| **SubscribeError** | `0x0011` | `[errorCode u8][symbolLen u8][symbol...]` |
| **Unsubscribed** | `0x0012` | `[securityId u64]` |
| **BookSnapshot** | `0x0020` | `[securityId u64][rptSeq u32][bidCount u16][askCount u16][levels...]` |
| **InfoSnapshot** | `0x0021` | `[securityId u64][fieldMask u32][values i64...]` |
| **OrderAdded** | `0x0030` | `[securityId u64][orderId u64][side u8][price i64][qty i64]` |
| **OrderUpdated** | `0x0031` | *(same as OrderAdded)* |
| **OrderDeleted** | `0x0032` | `[securityId u64][orderId u64][side u8]` |
| **Trade** | `0x0033` | `[securityId u64][price i64][qty i64][tradeId i64]` |
| **BookCleared** | `0x0034` | `[securityId u64]` |
| **ServerStatus** | `0x0050` | `[ready u8]` |

**BookSnapshot levels**: each level is `[price i64][totalQty i64][orderCount u16]` (18 bytes).

**InfoSnapshot fields** (bit position in `fieldMask`):

| Bit | Field | Bit | Field |
|-----|-------|-----|-------|
| 0 | OpeningPrice | 11 | VwapPrice |
| 1 | ClosingPrice | 12 | NetChange |
| 2 | HighPrice | 13 | NumberOfTrades |
| 3 | LowPrice | 14 | OpenInterest |
| 4 | LastTradePrice | 15 | PriceBandLow |
| 5 | LastTradeSize | 16 | PriceBandHigh |
| 6 | SettlementPrice | 17 | TradingReferencePrice |
| 7 | TheoreticalOpeningPrice | 18 | AvgDailyTradedQty |
| 8 | TheoreticalOpeningSize | 19 | MaxTradeVol |
| 9 | AuctionImbalanceSize | 20 | TradingStatus |
| 10 | TradeVolume | 21 | TradingEvent |

Only fields with their bit set are present in the payload (as i64 in bit order). Max message size: 192 bytes.

### Subscription Flow

```
Client                          Server (feed thread)
  │                                │
  │─── Subscribe(PETR4, Book+Info) │
  │                                │── ProcessPendingRequests()
  │◄── SubscribeOk(id, flags)      │
  │◄── BookSnapshot(levels)        │   ← book is stable (feed thread)
  │◄── InfoSnapshot(fields)        │   ← no concurrent mutations
  │                                │── activate subscription
  │◄── OrderAdded / Trade / ...    │   ← incrementals start flowing
  │◄── InfoSnapshot (updates)      │
  │    ...                         │
  │─── Unsubscribe(id)             │
  │◄── Unsubscribed(id)            │
```

Snapshot delivery happens on the feed thread before activating the subscription, guaranteeing no race between snapshot and incrementals.

## PCAP Replay — Cross-Channel Synchronization

The main challenge with replaying UMDF data from PCAPs is that the four channels (Incremental A, Incremental B, Instrument Definition, Snapshot Recovery) are normally received simultaneously via multicast. A naive sequential replay would break message ordering.

**Solution: Timestamp-based Priority Queue Merge**

The `TimestampMergedReplayer` reads all PCAP files simultaneously and merges packets into a single stream ordered by their original capture timestamp using a `PriorityQueue`. This ensures:

- Packets arrive in the exact chronological order they were captured
- Cross-channel ordering is preserved (e.g., instrument definition before first incremental)
- Optional speed control via `--speed` (0 = burst, 1 = real-time, >1 = accelerated)

## Multi-Channel Support

The application supports processing multiple UMDF channel groups simultaneously (e.g. equities + derivatives). B3 uses the same SBE schema for all asset classes — the wire format is identical.

### How It Works

- Each channel group has 4 channels (Incremental A/B, Instrument Definition, Snapshot Recovery)
- `MultiFeedManager` routes packets by `ChannelGroup` to per-group `FeedHandler` instances
- Each `FeedHandler` has an independent state machine (WaitInstrDef → WaitSnapshot → CatchUp → RealTime)
- **Per-group architecture**: each group owns its own `BookManager`, `MarketDataManager`, and `GroupEventHandler` — single-threaded, zero locks on the hot path
- `SymbolRegistry` is shared across groups (security IDs are globally unique)
- `SubscriptionManager` aggregates across groups: subscribe/get requests are routed to the owning group's queue; rankings merge data from all per-group `MarketDataManager` instances
- The WebSocket server activates once **all** channel groups reach RealTime

### Transport Modes

| Mode | Source | Use Case |
|------|--------|----------|
| **PCAP Replay** | `TimestampMergedReplayer` (sync) | Development, testing, backtesting |
| **Live Multicast** | `MulticastChannelMerger` (async) | Production, B3 certification environment |

Both implement `IPacketSource` — the feed handler layer is fully transport-agnostic.

### Multicast Config Format

The `--multicast-config` option takes a JSON file defining channel groups:

```json
{
  "channelGroups": [
    {
      "name": "EQT",
      "channels": [
        { "channelId": 84, "type": "IncrementalA", "multicastGroup": "224.0.20.84", "port": 30084 },
        { "channelId": 84, "type": "IncrementalB", "multicastGroup": "224.0.20.85", "port": 30085 },
        { "channelId": 84, "type": "InstrumentDefinition", "multicastGroup": "224.0.20.86", "port": 30086 },
        { "channelId": 84, "type": "SnapshotRecovery", "multicastGroup": "224.0.20.87", "port": 30087 }
      ]
    }
  ]
}
```

Optional `sourceAddress` field enables source-specific multicast (SSM). See `config/multicast-sample.json` for a complete example.

### Timestamp Ordering Note

When replaying PCAPs from different dates (e.g. EQT 2025-03-31 and DRV 2025-09-29), the timestamp-based merge processes all packets from the earlier date first. This means one group reaches RealTime before the other starts its instrument definition phase. This is by design — the merge preserves chronological fidelity.

## Performance

Benchmark on 52M UMDF packets (EQT 2025-03-31 PCAPs, ~20GB):

| Metric | Value |
|--------|-------|
| Packets processed | 52,075,408 |
| Orders | 15,737,471 |
| Trades | 1,122,008 |
| Books | 3,639 |
| **Total time** | **~30s** |
| **Throughput** | **~1.7M pkts/s** |

Key optimizations:
- **Synchronous packet processing** (`ISyncPacketSource`) — eliminates async/await overhead per packet
- **ArrayPool\<byte\>** — reuses buffers instead of allocating per-packet (`new byte[]` × 52M)
- **1MB BufferedStream** — reduces I/O syscalls in `PcapReader`
- **Zero-copy SBE** — `SbeSourceGenerator` produces blittable structs, no deserialization

### Profiling

```bash
# Install dotnet-trace (one time)
dotnet tool install -g dotnet-trace

# Run profiling script
./tools/profile.sh
```

## B3 Schema

This project uses the [B3 Market Data Messages v2.2.0](https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-developers-and-vendors/binary-umdf/) SBE XML schema.

The schema's `<!DOCTYPE xml>` declaration is removed because .NET's `XmlReader` prohibits DTD processing by default. This is the only modification to the original B3 schema.

## Production Operations

### Health Endpoints

When the WebSocket server is active (`--ws-port`), HTTP endpoints are available on the same port:

| Endpoint | Description |
|----------|-------------|
| `GET /health` | JSON: status, uptime, feed group states, last packet timestamps |
| `GET /ready` | `200` when all groups RealTime, `503` otherwise (readiness probe) |
| `GET /live` | Always `200` (liveness probe) |
| `GET /symbols` | JSON: list of all known symbols |
| `GET /instrument/{symbol}` | JSON: full instrument metadata including SecurityDefinition groups |

```bash
curl http://localhost:8080/health | jq
# {"status":"ready","uptime":"00:05:32","feedGroups":{"G0":"RealTime","G1":"RealTime"},...}
```

### Metrics

OTEL-compatible metrics via `System.Diagnostics.Metrics` (zero NuGet dependencies, fully AOT-safe).

**Meter name:** `B3.Umdf.Consumer`

```bash
# Install dotnet-counters (one time)
dotnet tool install -g dotnet-counters

# Monitor all metrics
dotnet-counters monitor --counters B3.Umdf.Consumer --process-id <PID>

# Monitor specific metrics
dotnet-counters monitor --counters B3.Umdf.Consumer[b3.umdf.feed.packets,b3.umdf.book.trades] --process-id <PID>
```

#### Feed Instruments

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `b3.umdf.feed.packets` | Counter | `group` | Packets received from feed |
| `b3.umdf.feed.duplicates` | Counter | `group` | Duplicate packets skipped |
| `b3.umdf.feed.gaps` | Counter | `group` | Sequence gaps detected |
| `b3.umdf.feed.instrument_definitions` | Counter | `group` | Instrument definitions received |
| `b3.umdf.feed.state` | Gauge | `group` | Feed state (0=WaitInstrDef, 1=WaitSnapshot, 2=CatchUp, 3=RealTime, 4=Recovery) |
| `b3.umdf.feed.last_packet_age` | Gauge | `group` | Milliseconds since last packet (stale feed detection) |
| `b3.umdf.feed.queue_depth` | Gauge | `group` | Pending packets in feed queue (multi-channel only) |

#### Book Instruments

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `b3.umdf.book.orders_added` | Counter | `group` | Orders added to books |
| `b3.umdf.book.orders_updated` | Counter | `group` | Order updates applied |
| `b3.umdf.book.orders_deleted` | Counter | `group` | Orders deleted from books |
| `b3.umdf.book.trades` | Counter | — | Trades processed |
| `b3.umdf.book.parse_errors` | Counter | `group` | Book SBE parse errors |
| `b3.umdf.book.crossings` | Counter | `group` | Bid/ask crossing transitions |
| `b3.umdf.book.delete_not_found` | Counter | `group` | Delete operations on non-existent orders |
| `b3.umdf.book.null_price_skips` | Counter | `group` | New orders skipped due to null price |
| `b3.umdf.book.null_price_deletes` | Counter | `group` | Updates with null price converted to deletes |
| `b3.umdf.book.active` | Gauge | `group` | Active order books |

#### Market Data Instruments

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `b3.umdf.market_data.updates` | Counter | — | Market data updates |
| `b3.umdf.market_data.status_changes` | Counter | — | Security status changes |
| `b3.umdf.market_data.forward_trades` | Counter | — | Forward trades |
| `b3.umdf.market_data.trade_busts` | Counter | — | Trade busts |
| `b3.umdf.market_data.execution_summaries` | Counter | — | Execution summaries |
| `b3.umdf.market_data.parse_errors` | Counter | `group` | Market data SBE parse errors |
| `b3.umdf.instruments.active` | Gauge | `group` | Active instruments |
| `b3.umdf.symbols.registered` | Gauge | — | Total registered symbols |

#### Server Instruments

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `b3.umdf.server.clients` | Gauge | — | Connected WebSocket clients |
| `b3.umdf.server.upstream_conflated` | Gauge | — | Pending events in conflation buffers |
| `b3.umdf.server.client_queue_depth` | Gauge | `client` | Per-client outbound queue depth |
| `b3.umdf.server.messages_sent` | Counter | `client` | Messages sent to WebSocket clients |
| `b3.umdf.server.bytes_sent` | Counter | `client` | Bytes sent to WebSocket clients |
| `b3.umdf.server.events_received` | Counter | `group` | Events received into conflation |
| `b3.umdf.server.events_flushed` | Counter | `group` | Events flushed from conflation to clients |

All instruments are **pull-based** (ObservableCounter/ObservableGauge) — zero overhead on the hot path. `dotnet-counters` computes rates automatically from Counter instruments.

Later, adding a Prometheus `/metrics` endpoint or OTLP exporter requires only the corresponding NuGet package — no instrumentation changes.

### Configuration

Settings can be provided via JSON file, environment variables, or CLI arguments (highest priority wins):

| Environment Variable | CLI | Default | Description |
|---------------------|-----|---------|-------------|
| `UMDF_WS_PORT` | `--ws-port` | *(off)* | WebSocket server port |
| `UMDF_SPEED` | `--speed` | `0` | Replay speed multiplier |
| `UMDF_MAX_CONNECTIONS` | — | `0` (unlimited) | Max concurrent WebSocket connections |
| `UMDF_CLIENT_CHANNEL_CAPACITY` | — | `4096` | Per-client outbound queue size |
| `UMDF_SHUTDOWN_DRAIN_SECONDS` | — | `5` | Graceful shutdown drain timeout |
| `UMDF_LOG_LEVEL` | — | `Information` | Minimum log level |
| `UMDF_MULTICAST_CONFIG` | `--multicast-config` | — | Multicast JSON config path |

### Backpressure & Slow Clients

Each WebSocket client has a bounded outbound queue (default: 4096 messages). When a client can't keep up:
1. Messages are dropped (newest data replaces oldest)
2. Queue depth is monitored on every feed event
3. Clients with queue depth above 75% capacity for 100+ consecutive checks are automatically disconnected
4. Disconnected clients should reconnect and re-subscribe (will receive fresh snapshots)

### Graceful Shutdown

The application handles `SIGTERM` (containers) and `SIGINT` (Ctrl+C):
1. Stop accepting new connections
2. Stop the feed (no new data)
3. Drain in-flight WebSocket writes (2s)
4. Stop the server

### TLS (HTTPS / wss://)

Kestrel natively supports HTTPS. Configure via environment variables:

```bash
ASPNETCORE_URLS=https://0.0.0.0:8443
ASPNETCORE_Kestrel__Certificates__Default__Path=/certs/cert.pfx
ASPNETCORE_Kestrel__Certificates__Default__Password=changeit
```

## References

- [B3 Binary UMDF Developer Page](https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-developers-and-vendors/binary-umdf/)
- [SbeSourceGenerator](https://github.com/pedrosakuma/SbeSourceGenerator)
- [FIX Simple Binary Encoding](https://github.com/FIXTradingCommunity/fix-simple-binary-encoding)

## License

[MIT](LICENSE)
