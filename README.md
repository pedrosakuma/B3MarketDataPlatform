# SbeB3UmdfConsumer

Open-source C# application for consuming [B3](https://www.b3.com.br/) market data via the **Binary UMDF** (Unified Market Data Feed) protocol using [SBE (Simple Binary Encoding)](https://github.com/FIXTradingCommunity/fix-simple-binary-encoding).

Uses the [`SbeSourceGenerator`](https://www.nuget.org/packages/SbeSourceGenerator/) Roslyn source generator to produce zero-allocation, high-performance C# structs directly from the B3 SBE XML schema.

## Features

- **Zero-copy SBE decoding** — generated blittable structs via `SbeSourceGenerator`
- **PCAP replay with cross-channel sync** — timestamp-based priority queue merge across all UMDF channels (Incremental A/B, Instrument Definition, Snapshot Recovery)
- **Feed A/B deduplication** — automatic duplicate packet filtering
- **Gap detection & sequence tracking** — detects missing packets for snapshot recovery
- **Market-by-Order (MBO) book** — full order book maintenance per instrument
- **Market data aggregation** — instrument info with 22 fields (prices, volumes, bands, status)
- **WebSocket subscription server** — binary protocol for real-time data streaming
- **Data channel filtering** — subscribe to Book, Info, or both via `DataFlags` bitmask
- **Unary Get** — one-shot snapshot without subscribing
- **Web frontend** — single-file SPA for interactive testing
- **Docker Compose** — one command to run backend + frontend with PCAP replay
- **Pluggable transport** — `IPacketSource` abstraction with multicast and in-process implementations

## Architecture

```
┌─────────────────┐     ┌──────────────────┐
│  PcapReplay      │     │  Multicast UDP    │
│  (TimestampMerge)│     │  (MulticastSource)│
└────────┬────────┘     └────────┬─────────┘
         │  IPacketSource        │
         └──────────┬────────────┘
                    │
            ┌───────▼───────┐
            │  FeedHandler   │
            │  (ChannelHandler, GapDetector)
            └───────┬───────┘
                    │  IFeedEventHandler
        ┌───────────┼───────────┐
        ▼           ▼           ▼
  BookManager  MarketDataMgr  SymbolRegistry
  (OrderBook)  (InstrumentInfo) (symbol→id)
        │           │
        └─────┬─────┘
              ▼
    SubscriptionManager ◄── ConcurrentQueue ── WebSocket clients
     (feed thread)           (subscribe/get/unsub requests)
              │
              ▼
        WebSocketHost
        (Kestrel, binary frames)
              │
              ▼
        Browser / Client
```

## Projects

| Project | Description |
|---------|-------------|
| `B3.Umdf.Sbe` | SBE schema + source generator (generates all B3 message types) |
| `B3.Umdf.Transport` | UMDF packet header, multicast transport, `IPacketSource`/`IPacketSink` |
| `B3.Umdf.Feed` | Feed handler, gap detection, A/B dedup, message dispatch |
| `B3.Umdf.Book` | Market-by-Order book: `OrderBook`, `BookSide`, `BookManager`, `MarketDataManager`, `SymbolRegistry` |
| `B3.Umdf.PcapReplay` | PCAP reader, UDP extractor, timestamp-merged replayer |
| `B3.Umdf.Server` | WebSocket subscription server: `WireProtocol`, `SubscriptionManager`, `ClientSession`, `WebSocketHost` |
| `B3.Umdf.ConsoleApp` | CLI application — PCAP replay + optional WebSocket server |

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

### Run with PCAP Replay

```bash
dotnet run --project src/B3.Umdf.ConsoleApp -- \
  pcap/20250331_MBO_084_EQT_Incremental_FeedA.pcap \
  pcap/20250331_MBO_084_EQT_Incremental_FeedB.pcap \
  pcap/20250331_MBO_084_EQT_InstrumentDefinition.pcap \
  pcap/20250331_MBO_084_EQT_SnapshotRecovery.pcap
```

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
| `--ws-port <port>` | *(off)* | Start WebSocket subscription server on the given port |
| `--speed <mult>` | `0` | Replay speed: `0` = max, `1` = real-time, `5` = 5× accelerated |

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
| `PCAP_PREFIX` | `20250331_MBO_084_EQT` | PCAP file name prefix (files must be in `pcap/`) |
| `WS_PORT` | `8080` | WebSocket server port |
| `REPLAY_SPEED` | `5` | Replay speed multiplier |
| `FRONTEND_PORT` | `3000` | Frontend HTTP port |

```bash
# Use derivatives dataset at real-time speed
PCAP_PREFIX=20250929_MBO_072_DRV REPLAY_SPEED=1 docker compose up --build
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

## References

- [B3 Binary UMDF Developer Page](https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-developers-and-vendors/binary-umdf/)
- [SbeSourceGenerator](https://github.com/pedrosakuma/SbeSourceGenerator)
- [FIX Simple Binary Encoding](https://github.com/FIXTradingCommunity/fix-simple-binary-encoding)

## License

[MIT](LICENSE)
