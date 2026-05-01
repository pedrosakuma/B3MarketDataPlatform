# WebSocket API — consumer guide

**Status: v1, stable.** This page is the contract surface for downstream
apps that want to consume `B3MarketDataPlatform` over the network without
reverse-engineering the bundled JS frontend. The exhaustive wire spec
(framing, full message catalog, every field) lives in
[WEBSOCKET-PROTOCOL.md](WEBSOCKET-PROTOCOL.md); this file is a thin
landing page that highlights the parts a typical consumer needs.

The canonical downstream use case driving v1 is `B3TradingPlatform`'s
`IReferencePrice` (last-trade price feed for pre-trade risk).

---

## Endpoint

```
ws://<host>:<WS_PORT>/ws
```

- `WS_PORT` defaults to `8080` (env var, also CLI `--ws-port`).
- Plain WebSocket — no TLS at the edge by default; terminate TLS in your
  ingress / sidecar if needed.
- The same HTTP host also exposes `GET /health`, `/ready`, `/live`,
  `/symbols?q=…`, `/instrument/{symbol}`, `/api/recovery/recent` (REST,
  not covered here).

## Wire format at a glance

All messages — both directions — are length-prefixed little-endian
binary frames sent as **WebSocket binary messages**:

```
[length u16][type u16][payload …]   length includes the 4-byte header
```

**The protocol is binary, not JSON.** This is a deliberate performance
choice: each message decodes with zero allocations using
`BinaryPrimitives.ReadXxxLittleEndian`. A typed C# subscriber is ~50
lines.

`type` is a `u16` from the catalog in
[WEBSOCKET-PROTOCOL.md §Message types](WEBSOCKET-PROTOCOL.md#message-types).
The ones a price consumer cares about:

| Direction | Type | Name | Purpose |
|-----------|------|------|---------|
| C → S | `0x0001` | `Subscribe` | Start receiving frames for one symbol. |
| C → S | `0x0002` | `Unsubscribe` | Stop receiving. |
| S → C | `0x0010` | `SubscribeOk` | Subscription accepted; carries the resolved `securityId`. |
| S → C | `0x0011` | `SubscribeError` | `UnknownSymbol` (`0x01`) or `NotReady` (`0x02`). |
| S → C | `0x0021` | `InfoSnapshot` | Per-symbol fields (incl. `LastTradePrice`). |
| S → C | `0x0033` | `Trade` | Live trade print. |
| S → C | `0x0035` | `TradeBust` | Cancellation of a previously-broadcast trade. |
| S → C | `0x0050` | `ServerStatus` | `ready=1` once every feed group is in `RealTime`. |

## Quickstart — subscribe to last-trade prices

The minimal flow for an `IReferencePrice`-style consumer:

1. **Connect** to `ws://host:WS_PORT/ws`.
2. **Wait for `ServerStatus`** (`type=0x0050`, payload `[ready u8]`).
   Treat `ready=0` as "do not subscribe yet"; resubscribe on the rising
   edge to `ready=1`. The server emits one immediately on connect and
   again on each transition.
3. For each symbol, send a **`Subscribe`** frame with `flags = 0x10`
   (`Trades` only) — or `0x12` (`Info|Trades`) if you also want the
   periodic `InfoSnapshot` fields (which include `LastTradePrice` /
   `LastTradeSize` as canonical "last value seen by the server").
4. Read frames in a loop. For each `Trade` (`0x0033`), update your
   in-memory `lastPrice[securityId] = price * 1e-4`.
5. On `TradeBust` (`0x0035`), risk consumers usually **ignore** it (the
   cancelled print is not the *current* reference; the next live `Trade`
   will overwrite). Audit consumers may want to remove the print from
   their trade history.

### Subscribe frame layout

```
Subscribe (0x0001) — body: [flags u8][symLen u8][symbol UTF-8…]
```

Example — subscribe to `PETR4` for trades only (`flags=0x10`):

```
0c 00 01 00 10 05 50 45 54 52 34
└──┬──┘└──┬──┘ │  │  └────┬────┘
  len   type  fl ln    "PETR4"
   12  0x0001 Trades 5
```

Server replies with **`SubscribeOk`** (`0x0010`):

```
SubscribeOk — body: [securityId u64][flags u8][symLen u8][symbol UTF-8…]
```

Persist `securityId` — every subsequent server frame uses it as the
primary key, *not* the symbol string.

### `Trade` frame layout

```
Trade (0x0033) — body: [securityId u64][price i64][qty i64][tradeId i64]
Total: 4 + 8 + 8 + 8 + 8 = 36 bytes
```

| Field | Type | Notes |
|-------|------|-------|
| `securityId` | `u64` LE | Match against the `securityId` from `SubscribeOk`. |
| `price` | `i64` LE | Mantissa with **exponent `-4`** (B3 SBE `Price`). Display value = `price × 10⁻⁴`. |
| `qty` | `i64` LE | Trade size, integer units. |
| `tradeId` | `i64` LE | Server-assigned, monotonically increasing per security. |

### `Trade` hex example

A trade of **10.0000 @ 50** for `securityId = 0x00000000CAFEBABE`,
`tradeId = 9999`:

```
24 00 33 00  BE BA FE CA 00 00 00 00  A0 86 01 00 00 00 00 00  32 00 00 00 00 00 00 00  0F 27 00 00 00 00 00 00
└──┬──┘└──┬──┘└────────┬────────────┘└────────┬────────────┘└────────┬────────────┘└────────┬────────────┘
 len=36  type=0x33     securityId=             price=100000           qty=50                 tradeId=9999
                       0xCAFEBABE              (= 10.0000)
```

C# decode:

```csharp
ushort len  = BinaryPrimitives.ReadUInt16LittleEndian(buf);            // 36
ushort type = BinaryPrimitives.ReadUInt16LittleEndian(buf[2..]);       // 0x0033
ulong  sid  = BinaryPrimitives.ReadUInt64LittleEndian(buf[4..]);
long   pxM  = BinaryPrimitives.ReadInt64LittleEndian(buf[12..]);       // mantissa
long   qty  = BinaryPrimitives.ReadInt64LittleEndian(buf[20..]);
long   tid  = BinaryPrimitives.ReadInt64LittleEndian(buf[28..]);
decimal price = pxM / 10_000m;
```

### `InfoSnapshot` hex example (last-trade-price only)

If you subscribe with `Info` (`flags=0x02` or any superset), an
`InfoSnapshot` (`0x0021`) is delivered on subscribe and on every change.
It uses a `u32` field bitmask; only fields with their bit set are
present, written as `i64` in bit order.

A minimal snapshot with **only** `LastTradePrice = 10.0000` for
`securityId = 0x00000000CAFEBABE` (mask bit 4):

```
18 00 21 00  BE BA FE CA 00 00 00 00  10 00 00 00  A0 86 01 00 00 00 00 00
└──┬──┘└──┬──┘└────────┬────────────┘└─────┬─────┘└────────┬────────────┘
 len=24  type=0x21     securityId          mask=0x10        LastTradePrice
                       0xCAFEBABE          (bit 4)          = 100000 → 10.0000
```

For the full bit map (24 fields, including `LastTradeSize`,
`TradeVolume`, `NumberOfTrades`, `TradingStatus`), see
[WEBSOCKET-PROTOCOL.md §Snapshots](WEBSOCKET-PROTOCOL.md#snapshots).

> **Heads-up on `Info` vs `Trades`.** `LastTradePrice` in `InfoSnapshot`
> tracks the server's view of the last trade and is convenient if you
> only need the latest value. The live `Trade` tape (`flags=0x10`) is
> the source of truth for *every* print; pick one based on your latency
> and bandwidth budget. They can be combined freely (`flags=0x12`).

## Lifecycle

| Aspect | Behaviour |
|--------|-----------|
| **Heartbeat** | None at the application layer. The server sets the WebSocket `KeepAliveInterval` to **30 s** (RFC 6455 ping/pong frames managed by the WS layer). Most clients do this transparently. |
| **`ServerStatus.ready=0`** | At least one feed group is recovering / not in `RealTime`. Consumers should pause subscriptions and resume on the next `ready=1`. |
| **Reconnect** | Open a new connection. **There is no per-client replay or sequence number.** The server re-sends a fresh `InfoSnapshot` (and `BookSnapshot` if `Book` flag set) on subscribe; live frames resume cleanly. Subscriptions do **not** survive the disconnect — re-issue every `Subscribe`. |
| **Gap detection** | Not exposed at this layer. The server's per-instrument healing means a steady-state subscriber sees a coherent stream; transient gaps surface as `ServerStatus(ready=0)` for the affected group. |
| **Slow-consumer drop** | If a client buffers more than `UMDF_CLIENT_MAX_PENDING_BYTES` (default 4 MiB) of unsent data, the server closes with WS code **`1008` (PolicyViolation)** and reason `"slow consumer"`. Reconnect and re-subscribe. |
| **Server shutdown** | Standard WS close. Clients should reconnect with backoff. |

## Versioning & breaking-change policy

- **v1 is stable.** Existing message types and field layouts (the rows
  in the table above, plus everything else in
  [WEBSOCKET-PROTOCOL.md](WEBSOCKET-PROTOCOL.md)) will not change shape
  within v1.
- **Additive changes are non-breaking** and may ship in any release:
  - New `MessageType` values.
  - New bits in `DataFlags`.
  - New bits in `InfoSnapshot.fieldMask` (appended at the next free
    bit position; existing masks remain valid).
  - New optional REST endpoints alongside the WS layer.
- **Breaking changes require a v2 surface.** That includes: changing
  the size or order of fields in an existing message, removing a
  message type, repurposing a `DataFlags` bit, or changing the framing
  header. v2 will be advertised by a new `MessageType` (e.g. a
  versioned `Hello`) before the legacy v1 wire is removed; v1 will be
  supported for at least one major release after v2 ships.
- The published Docker image tag (`v1.x.y`) is the stability anchor.
  Pin to a specific `vX.Y.Z` (or `sha-<short>`) in production; `latest`
  tracks `main` and may include experimental aditive features behind
  flags.

## Out of scope here

- **Typed C# / TS SDK.** Not provided; the format is small enough that
  a hand-written subscriber is the right answer (see C# snippet above).
  If demand grows, an SDK can land in a separate repo.
- **UMDF wire details** (B3's upstream multicast). This document covers
  only the *distribution* WebSocket; the UMDF ingestion side is
  internal to the platform.
- **Frontend message helpers.** The bundled JS in `frontend/js/` is
  reference code for the demo UI, not the contract.
