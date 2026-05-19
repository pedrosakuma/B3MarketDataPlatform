# B3.MarketData.WebSocketClient

Typed C# subscriber SDK for the
[B3MarketDataPlatform](https://github.com/pedrosakuma/B3MarketDataPlatform)
WebSocket distribution layer.

This v1 release covers the **reference-price use case** (trade prints,
info snapshots, server status) — the same surface documented in
[`docs/WEBSOCKET_API.md`](https://github.com/pedrosakuma/B3MarketDataPlatform/blob/main/docs/WEBSOCKET_API.md),
exposed as typed events with the SBE 4-decimal price exponent already
applied so consumers never see the raw `i64` mantissa.

## Install

```sh
dotnet add package B3.MarketData.WebSocketClient
```

```csharp
using B3.MarketData.WebSocketClient;

await using var client = new MarketDataClient(new MarketDataClientOptions
{
    Endpoint = new Uri("ws://localhost:8080/ws"),
});

client.Trade += t => Console.WriteLine($"{t.Symbol}  {t.Price:F4} x {t.Qty}");
client.LevelUpdate += l => Console.WriteLine($"{l.Symbol} {l.Side} {l.Price} qty={l.TotalQty}");

await client.ConnectAsync();
await client.SubscribeAsync("PETR4", SubscribeFlags.Trades | SubscribeFlags.Info | SubscribeFlags.Mbp);
```

Includes:

- Transparent reconnect with exponential back-off + automatic
  re-subscribe of every active subscription.
- Bounded back-pressure (`Channel<T>` with a configurable policy:
  `DropOldest` / `Block` / `Throw`).
- DI extension: `services.AddMarketDataClient(...)`.
- Full server parity: trades, info snapshots, server status, L3 / MBO
  (`BookSnapshot`, `OrderAdded/Updated/Deleted`, `BookCleared`,
  `MarketTierUpdate`), MBP (`LevelSnapshot`, `LevelUpdate`,
  `LevelDeleted`), candles (snapshot + update), rankings, per-symbol
  stale status, recovery progress, and reassembled news.
- **Opt-in materialized book layer (`IBookFeed`/`IBookView`)** —
  maintains an in-memory L3 book per symbol from the MBO event stream
  and exposes derived top-of-book. Stale-flag bridged from the
  server's `SymbolStaleStatus` (no client-side gap detection
  duplication). Construct via `client.CreateBookFeed()` or
  `services.AddMarketDataClient(...).WithBookFeed()`.

See [`docs/CLIENT-SDK.md`](https://github.com/pedrosakuma/B3MarketDataPlatform/blob/main/docs/CLIENT-SDK.md)
for the full guide.
