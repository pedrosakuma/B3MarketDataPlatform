# Client SDK — `B3.MarketData.WebSocketClient`

A typed, allocation-aware C# SDK for consuming the
B3MarketDataPlatform WebSocket distribution layer. The package wraps
the binary wire protocol described in
[`docs/WEBSOCKET-PROTOCOL.md`](WEBSOCKET-PROTOCOL.md), exposes the
"reference-price" surface from
[`docs/WEBSOCKET_API.md`](WEBSOCKET_API.md) as .NET events, and handles
reconnect + replay transparently.

| Item | Value |
|------|-------|
| Package id | `B3.MarketData.WebSocketClient` |
| Target | `net10.0` |
| License | MIT |
| Public surface | `MarketDataClient`, `MarketDataClientOptions`, event records, `SubscribeFlags` |
| Source | `src/B3.MarketData.WebSocketClient/` |

## Quickstart

```csharp
await using var client = new MarketDataClient(new MarketDataClientOptions
{
    Endpoint = new Uri("ws://localhost:8080/ws"),
});

client.Trade        += t => Console.WriteLine($"{t.Symbol} @ {t.Price} x {t.Qty}");
client.InfoSnapshot += i => Console.WriteLine($"{i.Symbol} last={i.LastTradePrice}");
client.ServerStatus += s => Console.WriteLine($"server ready={s.Ready}");
client.SubscribeError += e => Console.WriteLine($"{e.Symbol} -> {e.ErrorCode}");

await client.ConnectAsync();
await client.SubscribeAsync("PETR4", SubscribeFlags.Trades | SubscribeFlags.Info);
```

`Price` is delivered as `decimal` already scaled by the SBE exponent
(`-4` for trades and info fields) — no manual divide is needed.

## Dependency injection

```csharp
services.AddMarketDataClient(opt =>
{
    opt.Endpoint = new Uri(builder.Configuration["MarketData:Endpoint"]!);
    opt.BackPressure = BackPressurePolicy.DropOldest;
});
```

The client is registered as a singleton; resolve `MarketDataClient`
from your `IServiceProvider` and call `ConnectAsync` once during
startup.

## Reconnect & replay

- Background loop reconnects with exponential backoff
  (`ReconnectInitialDelay` → `ReconnectMaxDelay`, default 250 ms → 30 s),
  with up to ~25 % additive jitter.
- When the socket re-opens, every active subscription is re-issued
  automatically (`AutoResubscribeOnReconnect = true`). Consumers do
  **not** need to re-subscribe on `ConnectionStateChanged`.
- The cached `symbol → securityId` mapping is preserved across
  reconnects and refreshed by the new `SubscribeOk` frames.

## Back-pressure

Decoded events are queued through a bounded `Channel<T>`
(`EventChannelCapacity`, default 4096). When full, behaviour follows
`BackPressurePolicy`:

| Policy | Effect | When to use |
|--------|--------|-------------|
| `DropOldest` *(default)* | Discards the oldest queued event; increments `DroppedEventCount`. | Reference-price consumers that only need the latest tick. |
| `Block` | The receive loop awaits channel capacity. | Audit/persistence consumers that must not lose events but accept latency. |
| `Throw` | Drops the new event and raises `DroppedEventCount`. | Tests / strict consumers that want explicit detection. |

Monitor `client.DroppedEventCount` to detect slow handlers; the server
also enforces its own queue limits described in
[`docs/RESILIENCE.md`](RESILIENCE.md).

## Scope of v1

In: `Trade`, `TradeBust`, `InfoSnapshot`, `ServerStatus`,
`SubscribeError`, `ConnectionStateChanged`, reconnect+replay, DI
extension, bounded back-pressure.

Out (intentional): MBO/MBP order-book streams, recovery REST, auth
tokens. These remain accessible via the raw protocol described in
[`docs/WEBSOCKET-PROTOCOL.md`](WEBSOCKET-PROTOCOL.md).
