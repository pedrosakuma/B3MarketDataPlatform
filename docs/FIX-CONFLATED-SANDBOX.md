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
platform emits book, trade, instrument-status, news, security-list, and
market-totals data encoded as **FIX 4.4 Tag=Value** messages, batching
book deltas over a configurable time window ("conflation"). This
platform acts as the **FIX session acceptor** ("server" role) — the
inverse of connecting out to a real B3 UMDF Conflated feed.

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
  `MsgSeqNum` is tracked per connection; a reconnect presenting a
  sequence number lower than expected is disconnected immediately (no
  `SequenceReset`-based gap-fill across reconnects), matching B3's
  documented behavior. Recovery after such a disconnect happens via a
  fresh `MarketDataSnapshotFullRefresh` on the new session.

## Message scope

| Message | MsgType | Source in this platform |
|---|---|---|
| `MarketDataSnapshotFullRefresh` | W | Book state on subscribe / post-reconnect recovery |
| `MarketDataIncrementalRefresh` | X | Book deltas, batched per configurable conflation window; trades sent unthrottled in the same message family |
| `SecurityStatus` | f | `OnInstrumentStatusChanged` |
| `News` | B | Existing `NewsReassembler` pipeline |
| `SecurityList` / `SecurityListRequest` | y / x | `SymbolRegistry` |
| `MarketTotals*` | UTOT/UTOTC/UTOTQ/UTOTP | Best-effort, from existing rankings/aggregates |

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

## Enabling

Disabled by default. Enable with `UMDF_FIX_CONFLATED_ENABLED=true` and
configure the listening port and conflation window per
[docs/CONFIGURATION.md](CONFIGURATION.md) (reference added once the
transport lands).
