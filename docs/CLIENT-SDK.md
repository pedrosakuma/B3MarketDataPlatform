# Client SDK — `B3.MarketData.WebSocketClient`

[![NuGet](https://img.shields.io/nuget/v/B3.MarketData.WebSocketClient.svg)](https://www.nuget.org/packages/B3.MarketData.WebSocketClient)

A typed, allocation-aware C# SDK for consuming the
B3MarketDataPlatform WebSocket distribution layer. The package wraps
the binary wire protocol described in
[`docs/WEBSOCKET-PROTOCOL.md`](WEBSOCKET-PROTOCOL.md), exposes the
"reference-price" surface from
[`docs/WEBSOCKET_API.md`](WEBSOCKET_API.md) as .NET events, and handles
reconnect + replay transparently.

| Item | Value |
|------|-------|
| Package id | [`B3.MarketData.WebSocketClient`](https://www.nuget.org/packages/B3.MarketData.WebSocketClient) |
| Target | `net10.0` |
| License | MIT |
| Public surface | `MarketDataClient`, `MarketDataClientOptions`, event records, `SubscribeFlags` |
| Source | `src/B3.MarketData.WebSocketClient/` |

## Install

```sh
dotnet add package B3.MarketData.WebSocketClient
```

Or pin a specific version in your `csproj`:

```xml
<PackageReference Include="B3.MarketData.WebSocketClient" Version="0.1.0" />
```

## Quickstart

```csharp
await using var client = new MarketDataClient(new MarketDataClientOptions
{
    Endpoint = new Uri("ws://localhost:8080/ws"),
});

client.Trade        += t => Console.WriteLine($"{t.Symbol} @ {t.Price} x {t.Qty}{(t.Flags.HasFlag(TradeFlags.AuctionPrint) ? " (auction)" : "")}");
client.InfoSnapshot += i => Console.WriteLine($"{i.Symbol} last={i.LastTradePrice} top={i.TheoreticalOpeningPrice} imb={i.AuctionImbalanceCondition}");
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

In: `Trade`, `TradeBust`, `InfoSnapshot`, `SecurityDefinition`, `PriceBand`,
`Auction`, `ServerStatus`, `SubscribeError`, `ConnectionStateChanged`,
reconnect+replay, DI extension, bounded back-pressure.

Out (intentional): MBO/MBP order-book streams, recovery REST, auth
tokens. These remain accessible via the raw protocol described in
[`docs/WEBSOCKET-PROTOCOL.md`](WEBSOCKET-PROTOCOL.md).

## SecurityDefinition channel

Pre-trade guards typically need the static instrument metadata
(`MinPriceIncrement` = tick size, `MinTradeVolume` = lot size, ISIN,
currency, CFI code …). Set `SubscribeFlags.SecurityDefinition` (`0x20`)
on `SubscribeAsync` to receive:

* a bootstrap `SecurityDefinitionEvent` on subscribe (when the server
  already has a definition for the symbol), and
* a fresh `SecurityDefinitionEvent` whenever B3 publishes a real
  `SecurityDefinition_12` delta — idempotent re-broadcasts are
  suppressed server-side, so the event fires only on true changes.

```csharp
client.SecurityDefinition += sd =>
    Console.WriteLine($"{sd.Symbol} tick={sd.MinPriceIncrement} lot={sd.MinTradeVolume} isin={sd.IsinNumber}");
await client.SubscribeAsync("PETR4",
    SubscribeFlags.Trades | SubscribeFlags.Info | SubscribeFlags.SecurityDefinition);
// or just: SubscribeFlags.AllKnown
```

`MinPriceIncrement` is already scaled (raw SBE `Fixed8` mantissa divided by
`1e8`); all other numeric fields are unscaled. The event includes the
resolved `Symbol` directly from the wire frame, so it works even before the
`SubscribeOk` symbol cache is populated for first-sight securities.

## PriceBand channel

Pre-trade fat-finger guards (OPT-E `PriceBandCheck`, `QuantityBandCheck` and
friends) need the venue-authoritative dynamic bands. Set
`SubscribeFlags.PriceBand` (`0x40`) on `SubscribeAsync` to receive:

* a bootstrap `PriceBandEvent` on subscribe (when the server has already
  observed a `PriceBand_22` or `QuantityBand_21` for the symbol), and
* a fresh `PriceBandEvent` whenever B3 changes any of the band fields —
  idempotent re-broadcasts (the venue may emit the same band periodically)
  are suppressed server-side, so the event fires only on true band moves.

```csharp
client.PriceBand += pb =>
    Console.WriteLine(
        $"{pb.Symbol} price=[{pb.LowerBand}, {pb.UpperBand}] " +
        $"limitType={pb.PriceLimitType} " +
        $"avgQty={pb.AvgDailyTradedQty} maxQty={pb.MaxOrderQty}");
await client.SubscribeAsync("WINJ5",
    SubscribeFlags.Trades | SubscribeFlags.Info | SubscribeFlags.PriceBand);
// or just: SubscribeFlags.AllKnown
```

**Price fields** (from `PriceBand_22`):
- `LowerBand` / `UpperBand`: pre-scaled `decimal?` (raw `Price` mantissa / 1e4)
- `TradingReferencePrice`: pre-scaled `decimal?` (raw / 1e8)
- `PriceLimitType`: REQUIRED to interpret bands — `0` PRICE_UNIT (absolute),
  `1` TICKS (offsets vs. reference), `2` PERCENTAGE (offsets vs. reference)
- `PriceBandType`, `PriceBandMidpointPriceType`: discriminator enums

**Quantity fields** (from `QuantityBand_21`):
- `AvgDailyTradedQty`: average daily traded quantity (raw `i64`)
- `MaxOrderQty`: maximum order quantity allowed — use as venue-authoritative
  single-order qty ceiling for fat-finger guards

The event includes the resolved `Symbol` directly from the wire frame, so
it works even before the `SubscribeOk` symbol cache is populated.

Drop-in seam for consumers that already have an `IPriceBandSource`
interface (the B3TradingPlatform OPT-E pattern):

```csharp
public sealed class WebSocketPriceBandSource : IPriceBandSource
{
    private readonly ConcurrentDictionary<string, PriceBandEvent> _bands = new();
    public WebSocketPriceBandSource(MarketDataClient c) =>
        c.PriceBand += pb => _bands[pb.Symbol] = pb;
    public bool TryGet(string symbol, out PriceBandEvent band) =>
        _bands.TryGetValue(symbol, out band!);
}
```

## Auction channel

Aggregated auction state sourced from two UMDF templates:
`AuctionImbalance_19` (imbalance qty/side) and `SecurityGroupPhase_10`
(trading phase/pre-open time). Set `SubscribeFlags.Auction` (`0x80`)
to receive:

* a bootstrap `AuctionEvent` on subscribe (when either template has
  already been observed), and
* a fresh `AuctionEvent` whenever either template fires with a real
  delta — idempotent re-broadcasts upstream are suppressed.

```csharp
client.Auction += a =>
    Console.WriteLine(
        $"{a.Symbol} status={a.TradingStatus} imb={a.ImbalanceQty} " +
        $"side={a.ImbalanceSide} openTime={a.TradSesOpenTime}");
await client.SubscribeAsync("PETR4",
    SubscribeFlags.Trades | SubscribeFlags.Info | SubscribeFlags.Auction);
// or just: SubscribeFlags.AllKnown
```

`ImbalanceSide` is an enum (`Balanced`, `MoreBuyers`, `MoreSellers`)
decoded from the raw `ImbalanceCondition` bitfield (`0x0100` = MoreBuyers,
`0x0200` = MoreSellers). `TradingStatus` is the SBE `TradingSessionSubID`
enum (`2` = Pre-Open, `4` = Call, `17` = Continuous, …). `TradSesOpenTime`
is populated only when the status is Pre-Open and B3 publishes the
scheduled opening time (UTC epoch nanos); null otherwise.

The two UMDF templates can fire independently — each bump yields a push
with whatever is currently populated. Null fields mean "not yet received
from UMDF" or "not applicable to the current phase".

