# SbeB3UmdfConsumer

Open-source C# application for consuming [B3](https://www.b3.com.br/) market data via the **Binary UMDF** (Unified Market Data Feed) protocol using [SBE (Simple Binary Encoding)](https://github.com/FIXTradingCommunity/fix-simple-binary-encoding).

Uses the [`SbeSourceGenerator`](https://www.nuget.org/packages/SbeSourceGenerator/) Roslyn source generator to produce zero-allocation, high-performance C# structs directly from the B3 SBE XML schema.

## Features

- **Zero-copy SBE decoding** — generated blittable structs via `SbeSourceGenerator`
- **PCAP replay with cross-channel sync** — timestamp-based priority queue merge across all UMDF channels (Incremental A/B, Instrument Definition, Snapshot Recovery)
- **Feed A/B deduplication** — automatic duplicate packet filtering
- **Gap detection & sequence tracking** — detects missing packets for snapshot recovery
- **Market-by-Order (MBO) book** — full order book maintenance per instrument
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
            ┌───────▼───────┐
            │  BookManager   │
            │  (OrderBook, BookSide)
            └───────────────┘
```

## Projects

| Project | Description |
|---------|-------------|
| `B3.Umdf.Sbe` | SBE schema + source generator (generates all B3 message types) |
| `B3.Umdf.Transport` | UMDF packet header, multicast transport, `IPacketSource`/`IPacketSink` |
| `B3.Umdf.Feed` | Feed handler, gap detection, A/B dedup, message dispatch |
| `B3.Umdf.Book` | Market-by-Order book: `OrderBook`, `BookSide`, `BookManager` |
| `B3.Umdf.PcapReplay` | PCAP reader, UDP extractor, timestamp-merged replayer |
| `B3.Umdf.ConsoleApp` | Demo console application |

## Quick Start

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)

### Build

```bash
dotnet build
```

### Download PCAP Examples

B3 provides sample PCAP files for development:

```bash
./tools/download-pcaps.sh
```

### Run with PCAP Replay

```bash
dotnet run --project src/B3.Umdf.ConsoleApp -- \
  pcap/MBO_EQT_Incremental_FeedA.pcap \
  pcap/MBO_EQT_Incremental_FeedB.pcap \
  pcap/MBO_EQT_InstrumentDefinition.pcap \
  pcap/MBO_EQT_SnapshotRecovery.pcap
```

### Run Tests

```bash
dotnet test
```

## PCAP Replay — Cross-Channel Synchronization

The main challenge with replaying UMDF data from PCAPs is that the four channels (Incremental A, Incremental B, Instrument Definition, Snapshot Recovery) are normally received simultaneously via multicast. A naive sequential replay would break message ordering.

**Solution: Timestamp-based Priority Queue Merge**

The `TimestampMergedReplayer` reads all PCAP files simultaneously and merges packets into a single stream ordered by their original capture timestamp using a `PriorityQueue`. This ensures:

- Packets arrive in the exact chronological order they were captured
- Cross-channel ordering is preserved (e.g., instrument definition before first incremental)
- Optional speed control via `SpeedMultiplier` (0 = burst, 1.0 = real-time)

## B3 Schema

This project uses the [B3 Market Data Messages v2.2.0](https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-developers-and-vendors/binary-umdf/) SBE XML schema.

The schema's `<!DOCTYPE xml>` declaration is removed because .NET's `XmlReader` prohibits DTD processing by default. This is the only modification to the original B3 schema.

## References

- [B3 Binary UMDF Developer Page](https://www.b3.com.br/en_us/solutions/platforms/puma-trading-system/for-developers-and-vendors/binary-umdf/)
- [SbeSourceGenerator](https://github.com/pedrosakuma/SbeSourceGenerator)
- [FIX Simple Binary Encoding](https://github.com/FIXTradingCommunity/fix-simple-binary-encoding)

## License

[MIT](LICENSE)
