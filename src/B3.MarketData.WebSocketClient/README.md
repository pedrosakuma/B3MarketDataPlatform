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

await client.ConnectAsync();
await client.SubscribeAsync("PETR4");
```

Includes:

- Transparent reconnect with exponential back-off + automatic
  re-subscribe of every active subscription.
- Bounded back-pressure (`Channel<T>` with a configurable policy:
  `DropOldest` / `Block` / `Throw`).
- DI extension: `services.AddMarketDataClient(...)`.

Out of scope for v1: full L2/MBO subscription, recovery REST endpoints.

See [`docs/CLIENT-SDK.md`](https://github.com/pedrosakuma/B3MarketDataPlatform/blob/main/docs/CLIENT-SDK.md)
for the full guide.
