# FIX "UMDF Conflated" sandbox channel

> **Status: experimental, non-official, non-certified.** This is not the
> B3 UMDF PUMA Conflated product, is not connected to B3, and is not
> reviewed or endorsed by B3. It is a local reproduction of the public
> wire *format* described in B3's *UMDF PUMA Conflated Market Data
> Specification* (v2.2.0, 2025-10-21), built for exploratory validation
> of downstream FIX-consuming code against this platform's own market
> data. See [docs/PROTOCOL-CONTRACTS.md](PROTOCOL-CONTRACTS.md) for why
> this distinction matters and must never be blurred.

## What this is

An additional, **opt-in** output channel, alongside the existing
`WireV2` WebSocket protocol (see
[docs/WEBSOCKET-PROTOCOL.md](WEBSOCKET-PROTOCOL.md)), through which this
platform emits live book, trade, instrument-status, and news data
encoded as **FIX 4.4 Tag=Value** messages, batching book deltas over a
configurable time window ("conflation"). The same project also models
additional UMDF-specific message shapes such as `SecurityList*` and
`MarketTotals*`. This platform acts as the **FIX session acceptor**
("server" role) — the inverse of connecting out to a real B3 UMDF
Conflated feed.

## What this is not

- **Not FAST-encoded.** Despite the "UMDF FIX/FAST" branding used for
  some older/other B3 products, the *Conflated* Market Data
  Specification explicitly uses "traditional Tag=Value FIX encoding"
  (§3.1), not FAST. This sandbox follows that: plain SOH-delimited
  `tag=value` frames.
- **Not authenticated.** Consistent with the existing `WireV2` WebSocket
  channel (no API keys / tokens today), the FIX acceptor here performs
  **no credential validation** on `Logon` (MsgType=A): any
  `SenderCompID`/`TargetCompID` pair is accepted. The real B3 product
  requires B3-assigned CompIDs and a certification process (§7.8.15) —
  this sandbox intentionally skips both.
- **Not a replay/resend engine.** Real B3 UMDF Conflated does not
  gap-fill across reconnects: a client reconnecting with a `MsgSeqNum`
  lower than expected is disconnected immediately, with recovery
  expected via a fresh `MarketDataSnapshotFullRefresh`, not persisted
  history. This sandbox mirrors that behavior instead of implementing a
  general-purpose FIX message store — see "Session behavior" below.
- **Not built on QuickFIX/n** or any other third-party FIX engine. The
  session and encoding are purpose-built and intentionally minimal, for
  three reasons: the outbound zlib compression layer (RFC 1950) doesn't
  fit typical FIX-engine transport models, this platform never needs to
  *answer* application-level resends (a pure market-data acceptor has no
  business messages to redeliver from a persistent store beyond the
  current session), and the real product's restricted recovery model
  (forced disconnect + snapshot, not engine-managed gap-fill) would
  fight a general-purpose engine's assumptions rather than benefit from
  them.
- **Not tied to the B3-documented 380ms conflation interval.** The spec
  cites ~380ms as B3's chosen cadence; this sandbox exposes conflation
  cadence as a **configurable** parameter with a documented default, not
  a hard-coded constant reproducing that exact figure.

## Vendored schema

`schemas/fix-conflated/FIX44_UMDFConflated.xml` is the FIX 4.4 data
dictionary published alongside the *UMDF PUMA Conflated Market Data
Specification*. Like the SBE schema under `schemas/`, it is vendored
verbatim and covered by the CI "Vendored schema guard" — any PR touching
it needs the `schema-upgrade` label.

## Session behavior (summary)

- `Logon` (A) / `Heartbeat` (0) / `TestRequest` (1) / `Logout` (5) are
  supported per standard FIX 4.4 session semantics, with no credential
  check.
- `ResendRequest` (2) is honored only for messages still available in
  the **current session's** bounded in-memory buffer — there is no
  cross-session/cross-day persisted message store.
- `MsgSeqNum` is tracked per connection; a reconnect presenting a
  sequence number lower than expected is disconnected immediately (no
  `SequenceReset`-based gap-fill across reconnects), matching B3's
  documented behavior. Recovery after such a disconnect happens via a
  fresh `MarketDataSnapshotFullRefresh` on the new session.

## Message catalog

MsgType codes below are verified against both the vendored dictionary
(`schemas/fix-conflated/FIX44_UMDFConflated.xml`) and the in-repo
constants in `FixMsgTypes` / `FixApplicationMsgTypes`.

### Session/admin messages

| Message | MsgType | Direction in this sandbox | Current behavior |
|---|---|---|---|
| `Logon` | `A` | inbound + outbound | First inbound message must be `Logon`; the sandbox always accepts it and replies with a `Logon` ack |
| `Heartbeat` | `0` | inbound + outbound | Periodic server heartbeat; also the response to `TestRequest` |
| `TestRequest` | `1` | inbound | Accepted; answered with a `Heartbeat` carrying the same `TestReqID` |
| `ResendRequest` | `2` | inbound | Replays only application messages still retained in the current session's bounded in-memory resend buffer |
| `SequenceReset` | `4` | outbound | Emitted only as in-session gap-fill during `ResendRequest` handling |
| `Logout` | `5` | inbound + outbound | Used for orderly shutdown and validation/sequence failures after logon |

### Application messages emitted by the current TCP listener wiring

| Message | MsgType | Trigger / source |
|---|---|---|
| `MarketDataSnapshotFullRefresh` | `W` | `FixInitialSnapshotProvider` sends one full snapshot per known book immediately after logon / reconnect recovery |
| `MarketDataIncrementalRefresh` | `X` | `FixConflatedMarketDataPublisher` batches book deltas per instrument/side over the configured conflation window; trade entries (`MDEntryType=2`) bypass that window and flush immediately |
| `SecurityStatus` | `f` | `FixConflatedChannelHandler.OnSecurityStatusChanged` |
| `News` | `B` | `FixConflatedChannelHandler.OnNews`, fed by the existing `NewsReassembler` pipeline |

### Additional UMDF-specific message definitions modeled in code/schema

These message shapes are implemented by builders in
`src/B3.Umdf.FixConflated`, but the current listener wiring does **not**
yet auto-publish them on its own:

| Message | MsgType | Current status |
|---|---|---|
| `SecurityListRequest` | `x` | Request/builder shape implemented by `SecurityListMessageBuilder.BuildRequest`; no automatic request/response flow is wired today |
| `SecurityList` | `y` | Builder implemented by `SecurityListMessageBuilder.Build`; not automatically broadcast by the current TCP server wiring |
| `MarketTotalsBroadcast` | `UTOT` | Builder implemented; not automatically broadcast by the current TCP server wiring |
| `MarketTotalsComposition` | `UTOTC` | Builder implemented; not automatically broadcast by the current TCP server wiring |
| `MarketTotalsRequest` | `UTOTQ` | Request/builder shape implemented; no automatic request/response flow is wired today |
| `MarketTotalsResponse` | `UTOTP` | Builder implemented; not automatically broadcast by the current TCP server wiring |

## Conflation model

Within each conflation window, book deltas (`MDUpdateAction` =
add/change/delete/delete-thru, indexed by `MDEntryPx`/`OrderID` per B3's
price/priority model) accumulate and are flushed as a single batched
`MarketDataIncrementalRefresh` per instrument/side — this is a *batch of
occurred deltas*, not a last-value-wins collapse. Trades and
statistical/status data are never conflated.

## Hot-path isolation

The channel plugs into the existing `IBookEventHandler` /
`IMarketDataEventHandler` fan-out
(`CompositeBookEventHandler`/`CompositeMarketDataEventHandler`, wired in
`B3.Umdf.ConsoleApp/Program.cs`) as an additional handler, following the
same discipline as `GroupConflationHandler`: the synchronous hot-path
callback only performs a cheap, allocation-free enqueue into its own
ring buffer; FIX encoding, conflation-window flushing, and socket I/O all
run on a dedicated background thread, never blocking or allocating on
the shared per-group hot path.

## Configuration reference

All FIX sandbox knobs are environment-variable only; there are no
dedicated CLI switches.

| Environment Variable | Default | Valid values | Effect |
|---|---|---|---|
| `UMDF_FIX_CONFLATED_ENABLED` | `false` | `true` / `false` | Enables the opt-in FIX conflated TCP listener. When `false`, the other FIX knobs are ignored |
| `UMDF_FIX_CONFLATED_PORT` | *(off)* | integer `1..65535`; required when enabled; must differ from `UMDF_WS_PORT` | TCP listen port for the FIX acceptor |
| `UMDF_FIX_CONFLATED_CONFLATION_MS` | `380` | integer `> 0` | Book-delta conflation window in milliseconds. The sandbox default matches the real product's documented ~380 ms cadence, but remains configurable for experiments |
| `UMDF_FIX_CONFLATED_RESEND_BUFFER_CAPACITY` | `10000` | integer `>= 0` | Per-connection in-memory application resend buffer size. `0` disables in-session replay retention |
| `UMDF_FIX_CONFLATED_OUTBOUND_QUEUE_CAPACITY` | `4096` | integer `> 0` | Per-connection bounded outbound queue. Slow clients are disconnected if it fills |
| `UMDF_FIX_CONFLATED_EVENT_QUEUE_CAPACITY` | `65536` | integer `> 0` | Per-group hot-path queue feeding the background FIX encoder. When full, new FIX events are dropped instead of blocking the shared UMDF group thread |

See [docs/CONFIGURATION.md](CONFIGURATION.md) for the full configuration
matrix alongside the existing WebSocket and transport knobs.

## Explicit deviations from the real B3 product

- **Logon always succeeds.** There is no password, certificate, CompID
  whitelist, or session-level authentication step.
- **No B3 certification / onboarding process.** This repo is an
  exploratory local sandbox, not an official access path.
- **Reconnect recovery is snapshot-based.** `ResendRequest` only replays
  application messages still buffered inside the current TCP session;
  there is no persisted cross-session or cross-day gap-fill store.
- **Conflation cadence is configurable.** Real B3 documentation cites an
  approximately 380 ms cadence; this sandbox uses
  `UMDF_FIX_CONFLATED_CONFLATION_MS` with a default of `380`.
- **Role relationship is inverted.** This platform is the FIX session
  **acceptor/server**; downstream clients connect *to it*, rather than
  this repo acting as a FIX client connecting out to B3.
- **This remains a non-official sandbox only.** It is not certified for,
  or suitable for, production connectivity to B3.

## Enabling

Disabled by default. Enable with `UMDF_FIX_CONFLATED_ENABLED=true` plus
at least `UMDF_FIX_CONFLATED_PORT=<port>`. Optional transport tuning
includes `UMDF_FIX_CONFLATED_CONFLATION_MS`,
`UMDF_FIX_CONFLATED_RESEND_BUFFER_CAPACITY`,
`UMDF_FIX_CONFLATED_OUTBOUND_QUEUE_CAPACITY`, and
`UMDF_FIX_CONFLATED_EVENT_QUEUE_CAPACITY`; see
[docs/CONFIGURATION.md](CONFIGURATION.md).
