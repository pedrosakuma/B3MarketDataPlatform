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
encoded as **FIX 4.4 Tag=Value** messages inside a **single continuous per-session RFC 1950 ZLIB stream**, batching book deltas over a
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

- Outbound bytes are wrapped once per TCP connection in a **single continuous ZLIB stream** (`System.IO.Compression.ZLibStream`). The sandbox does **not** compress each FIX frame independently; clients must inflate the whole socket stream and then parse tag=value FIX messages out of the resulting plaintext byte stream. This is always on, because that matches the real B3 product and keeps the sandbox wire contract faithful by default.
- The write loop flushes the shared connection-lifetime compressor after each encoded FIX message so logon, heartbeats, trades, and conflated book updates become readable to the client promptly instead of waiting for additional deflater buffering.
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
| `MarketDataRequest` | `V` | inbound only — parsed by `FixMarketDataRequestHandler` (`FixMarketDataSubscriptionSupport.cs`); drives per-session `SecurityID` subscription filtering (issue #116) |
| `MarketDataRequestReject` | `Y` | outbound only — emitted by `FixMarketDataRequestHandler` for malformed/unknown-instrument `MarketDataRequest`s |

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

### Schema messages with no implementation at all

These MsgTypes exist in the vendored dictionary
(`schemas/fix-conflated/FIX44_UMDFConflated.xml`) but have **no builder and
no handling** anywhere in `src/B3.Umdf.FixConflated` today:

| Message | MsgType | Notes |
|---|---|---|
| `BusinessMessageReject` | `j` | No generic session-level business-reject path; malformed application messages other than `MarketDataRequest` (`V`) have no reject response at all |
| `LicenseKeyRequest` | `ULRQ` | Not modeled |
| `LicenseKeyResponse` | `ULRP` | Not modeled |
| `LicenseLogoutReport` | `ULRL` | Not modeled |
| `NetworkStatusResponse` | `BD` | Not modeled |
| `TradeHistoryRequest` | `UTHQ` | Not modeled |
| `TradeHistoryResponse` | `UTHP` | Not modeled |

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

## Validation & tooling

The FIX Conflated channel has validation/observability tooling parity with
the existing `WireV2` WebSocket path (tracked by issue #103). All listed tooling now inflates the continuous ZLIB transport stream before FIX frame parsing:

- **Standalone FIX validator** — `tools/fix/fix-validate.mjs` is a
  dependency-free Node.js TCP client mirroring `tools/ws/ws-validate.mjs`. It
  performs `Logon`, consumes the automatic `MarketDataSnapshotFullRefresh` and
  subsequent `MarketDataIncrementalRefresh` messages, reconstructs the book
  locally, tracks trades, and answers `TestRequest` with `Heartbeat`. See
  [tools/fix/README.md](../tools/fix/README.md) for usage and environment
  variables.
- **Server-truth comparison via fresh WS snapshot.** The current server does
  **not** expose an HTTP `/book/{symbol}` route (a pre-existing gap that also
  affects `tools/ws/ws-validate.mjs`, unrelated to this channel). Rather than
  adding a new HTTP endpoint or comparing two independently-accumulated
  client-side books (which could drift identically on a shared conceptual
  bug), validation instead opens a **fresh WebSocket connection and sends a
  plain `Subscribe`** for the tracked symbol at the end of a run. The server
  always answers a fresh subscribe with a full, server-computed
  `BookSnapshot` reset marker followed by the MBO `OrderAdded` burst — the
  validator waits for that burst to go briefly idle before computing the book.
  This is genuine server-side truth, the closest available equivalent to what
  `/book/{symbol}` would have provided. This is wired via
  `WS_SNAPSHOT_COMPARE_URL` in `fix-validate.mjs`.
- **Real-socket reconnect/recovery test** —
  `tests/B3.Umdf.FixConflated.Tests/FixConflatedReconnectEndToEndTests.cs`
  proves the reconnect rule (stale `MsgSeqNum` disconnects with no gap-fill;
  a fresh session with `MsgSeqNum=1` recovers via a new snapshot) through the
  real `FixConflatedTcpServer`/socket layer, not just the in-process session
  state machine.
- **Pcap-replay harness** — `tools/fix-conflated-replay-validate.sh` starts
  the console app against a PCAP prefix with the FIX listener enabled, runs
  `fix-validate.mjs` for the replay window, and reports the final fresh-WS
  `BookSnapshot` comparison as the pass/fail verdict. Mirrors the conventions
  of `tools/loss-resilience-test.sh`.
- **Staged late-join replay harness** — `tools/fix-conflated-late-join-validate.sh`
  keeps one replay running, then launches multiple fresh FIX validator clients
  at staggered offsets (default `10,50,90%` of the run) so each connection gets
  its own late-join `MarketDataSnapshotFullRefresh` cross-checked against a
  simultaneous fresh WS subscribe snapshot. Example:
  `WS_PORT=18080 FIX_PORT=19200 SPEED=1 FRESH_WS_COMPARE=0 tools/fix-conflated-late-join-validate.sh pcap/20250331_MBO_084_EQT 60 CPLE3 45`.
  The optional fourth argument adds an initial delay before the staged joins,
  useful when a replay needs time to leave instrument-definition bootstrap and
  build a non-trivial book before the first "late join" probe. Set
  `FRESH_WS_COMPARE=1` to require the extra fresh-WS cross-check when the
  replay/window being used is known to answer WS subscribe snapshots promptly.
- **Soak test coverage** — `tools/soak-test.sh` optionally starts the FIX
  listener alongside the WebSocket host (`ENABLE_FIX_CONFLATED=true`) and
  samples `FixConflatedMetrics` via the Prometheus `/metrics` endpoint into
  the same long-running RSS/GC/counter stability CSV used for the WireV2
  path.
- **Perf-smoke benchmark coverage** — not yet added (deliberately deferred,
  best-effort/optional item in issue #103).

### Message-content validation against real B3 sample captures

Issue #108 added a manual/offline comparison pass against the local B3 sample
 kit (`prodreplay-equities-23apr2014.zip`, with a secondary spot-check against
 `prodreplay-derivatives-28apr2014.zip`). The comparison intentionally stayed
 at **message-content level**: which tags a real capture actually populates,
 their order, and repeating-group shape. The sample files are proprietary and
 remain outside this repo; only tiny single-message snippets are reproduced
 here for illustration.

#### Confirmed matches

- **Session Logon shape matches closely.** Real captures start with the same
  minimal `A` pattern the sandbox emits today: `35=A`, `34`, `49`, `52`, `56`,
  `98=0`, `108=30`, optional `141=Y`, then trailer. Example real snippet:
  `35=A|34=1|49=UMDFTCP5|52=...|56=LUX00|98=0|108=30|141=Y`.
- **News (`B`) core mapping is aligned.** Real messages consistently populate
  `33`/`58`/`42` plus one `146=1` related instrument block containing
  `48`/`22`/`207`, then `148`/`149`/`1474` and a trailing status-like code
  (`6940`). Our builder already models the same headline/orig-time/related
  instrument/url/language structure.
- **SecurityList (`y`) is really present in production-style captures.** The
  replay starts with `35=A` and then many `35=y` messages, confirming the doc's
  earlier note that `SecurityList` is part of the observed catalog, not just a
  schema-only possibility.
- **Nested `SecurityList` feed groups look right.** Real `y` messages populate
  `146` followed by repeated per-instrument blocks and nested feed-type tuples
  like `1141=2|1022=STD|264=0|1021=3|1022=STD|264=10|1021=2`, which matches the
  sandbox's choice to model `NoRelatedSym` with nested `NoMDFeedTypes`.

#### Observed discrepancies / gaps

- **Snapshot (`W`) instrument identity is partially aligned now.** Real captures use
  an instrument-only header such as
  `35=W|...|128=LUX00|22=8|48=910000|207=BVMF|262=...|268=0|911=33943`,
  typically **without `55=Symbol`**, and include optional header `128` plus
  `911=TotNumReports`. The sandbox now emits `22`, `207`, optional `128`, and
  `911`, but still keeps `55=Symbol` for compatibility and does not yet mirror
  the full production header shape exactly.
- **Snapshot entry content is still thinner than production, but less so.** The derivatives
  sample shows populated snapshot entries like
  `269=0|270=14|271=200|272=...|273=...|37017=...|37=...|288=88|290=1|1021=3`
  and status-style entries like `269=c|...|336=1|625=21|342=...`. The sandbox
  now also emits `37016/37017` and `290`, but still does not cover the broader
  production entry set.
- **Incremental (`X`) messages are closer to production, but still simplified.** Real captures consistently include message-level `75=TradeDate`,
  sometimes `1021=MDBookType`, and entry-level `22`, `207`, frequent `276`,
  `286`, `289`, `290`, `346`, `1500`, `9325`, and timestamp/order metadata
  such as `37016`/`37017`. The sandbox now writes `75`, `1021`, `22`, `207`,
  `276`, `286`, `290`, `346`, `1500`, `9325`, `37016`, and `37017`, but some
  values remain fixed placeholders where the current in-memory model has no
  richer source of truth (for example constant position/order-count style
  metadata).
- **Incremental tag order differs.** Real captures lead each entry with
  `279`, then usually instrument identity (`22/48/207`) before or around
  `269`, whereas the sandbox now follows that pattern more closely but still
  prioritizes parser stability over exact byte-for-byte ordering parity.
- **SecurityStatus (`f`) coverage is now intentionally compact.** Real
  samples are compact, typically
  `35=f|...|60=...|75=...|336=1|625=18|1151=69`, with no broad instrument block
  or descriptive text. The sandbox builder now centers on `60/75/336/625/1151`
  plus routing identity, instead of the previous richer status payload.
- **News (`B`) production tag `6940` is now modeled.** Real samples end
  with `6940=17` (equities) or `6940=3` (derivatives); the sandbox builder now
  exposes that field directly.
- **SecurityList (`y`) coverage is expanded, but still partial.** The real capture repeatedly includes tags such as `15`,
  `63`, `64`, `107`, `120`, `200`, `225`, `231`, `454/455/456`, `470`, `541`,
  `667`, `762`, `870/871/872`, `969`, `980`, `1151`, `1231`, `1234`, `1300`,
  `5151`, `6937`, `6938`, `7595`, `9748`, `9749`, and sometimes `320/322/393/560`.
  The sandbox now emits a meaningful subset derived from the existing reference
  model (`15`, `63`, `64`, `107`, `120`, `200`, `225`, `231`, `541`, `762`,
  `969`, `1151`, `320/322/393/560`, `6937`, `9749`), but many rarer fields
  remain unimplemented because no natural source exists in the current metadata.

#### Secondary schema sanity check

- The older sample-kit dictionary (`FIX44UMDFConflated-016.xml`) still shows
  the same broad structures observed in the real captures: `W` with optional
  `TotNumReports`/`LastFragment`, `X` with rich `NoMDEntries` instrument and
  market-data fields, `B` with `NoRelatedSym`, and `y` with nested
  `NoMDFeedTypes`.
- No obvious case was found where the older dictionary contradicted the real
  capture in a way that suggests the vendored v1.0.0.37 schema is wrong.
  The actionable differences observed here are therefore implementation/content
  gaps in the sandbox, not evidence that the vendored schema should regress.

#### Recommended follow-up

- Treat the current sandbox as **transport-faithful but content-simplified**.
  It is already good for parser/session/conflation experiments, but not yet a
  close field-for-field reproduction of observed B3 production payloads.
- If stricter production mirroring becomes important, the highest-value follow-up
  items are:
  1. replace fixed placeholder values in `X` (`276`, `286`, `290`, `346`,
     `1500`, `9325`) with richer per-entry sources if the upstream book/trade
     model is extended;
  2. continue expanding `SecurityList (y)` for fields still absent from the
     current in-memory reference model;
  3. decide whether exact production ordering / optional omission of `55` in
     snapshots is worth the compatibility trade-off.

## Consolidated drift map vs. the real B3 UMDF PUMA Conflated spec (issue #115)

This section is the single authoritative inventory of every known
behavioral/content divergence from the real B3 UMDF PUMA Conflated Market
Data Specification (v2.2.0, 2025-10-21). It exists to stop re-discovering the
same drifts piecemeal (compression wiring #107, content gaps #108/#111/#114,
WS snapshot bug #110, subscription model #116, late-join timing #117) — every
new drift found from here on should be added as a row here, not just fixed
silently. Rows that duplicate detail already covered elsewhere in this
document link out instead of repeating it.

| Area | Spec expectation | Sandbox behavior | Impact | Tracking issue |
|---|---|---|---|---|
| Instrument subscription | Clients scope their feed via inbound `MarketDataRequest` (`V`) with `NoRelatedSym`/`SecurityID`/`SecurityIDSource`/`SecurityExchange`, `SubscriptionRequestType`, optional `MDBookType`/`MarketDepth`/`SecurityType`/`CFICode`/`Product`/`NoSecurityGroups`; server may reply `MarketDataRequestReject` (`Y`) | **Resolved.** `V` is parsed by `FixMarketDataRequestHandler`, drives per-session `SecurityID` subscription state, gates snapshot/incremental fan-out per session, and rejects malformed/unknown instruments with `Y`. `NoSecurityGroups` and `MDBookType`/`MarketDepth`-based filtering are still deferred (only `SecurityID`-level filtering exists). Sessions that never send an explicit `V` still fall back to the legacy full-broadcast (every known instrument) for compatibility. | Functional — was previously full-broadcast-only (no way to scope a feed); now closer to spec but `NoSecurityGroups`/book-type/depth filters remain unimplemented | #116 (closed via PR #118); residual `NoSecurityGroups`/`MDBookType` filtering not separately tracked yet |
| Late-join snapshot correctness | A client connecting mid-session at any point during an active feed must receive a correct, current full snapshot via `MarketDataSnapshotFullRefresh`, then continue cleanly from subsequent incrementals with no gap | Automated end-to-end coverage now exists (`FixConflatedLateJoinSnapshotEndToEndTests`) proving two clients joining at different points each get a correct current snapshot and correct subsequent incrementals. Manual staged-replay validation (`tools/fix-conflated-late-join-validate.sh`) remains inconclusive due to test-harness timing/instability, not a proven server-side bug. | Functional (core guarantee) — now proven at the unit/integration level; real-replay timing behavior still not conclusively demonstrated | #117 (closed via PR #119) |
| `BusinessMessageReject` (`j`) | Generic session-level reject for malformed/unsupported application messages | Not modeled at all; only `MarketDataRequest` has a dedicated reject (`MarketDataRequestReject`, `Y`) | Cosmetic/protocol — malformed messages of other types are silently ignored rather than explicitly rejected | Not yet tracked — candidate for a new issue if this becomes relevant |
| `LicenseKeyRequest`/`LicenseKeyResponse`/`LicenseLogoutReport` (`ULRQ`/`ULRP`/`ULRL`) | B3 entitlement/licensing handshake messages | Not modeled at all | Cosmetic — this sandbox has no authentication/entitlement layer at all (see "Explicit deviations" below) | Not tracked (out of scope; sandbox has no licensing model) |
| `NetworkStatusResponse` (`BD`) | Network/connectivity status reporting message | Not modeled at all | Cosmetic | Not tracked |
| `TradeHistoryRequest`/`TradeHistoryResponse` (`UTHQ`/`UTHP`) | On-demand historical trade query/response | Not modeled at all | Functional gap only if historical trade replay/query becomes a requirement; current channel is live-feed only | Not tracked |
| `SecurityListRequest`/`SecurityList` (`x`/`y`) | Request/response flow for instrument reference data | Builders exist but are not wired to any automatic trigger; no request/response flow | Functional — `y` content itself is now reasonably close to production per #111/#114, but there is no way for a client to actually request it | Not separately tracked; content gaps covered by #111/#114 |
| `MarketTotalsBroadcast`/`MarketTotalsComposition`/`MarketTotalsRequest`/`MarketTotalsResponse` (`UTOT`/`UTOTC`/`UTOTQ`/`UTOTP`) | Market-totals broadcast/request-response messages | Builders exist but nothing triggers them automatically | Functional gap if market-totals data becomes a requirement | Not tracked |
| `W`/`X`/`f`/`y`/`B` content-level field coverage | See "Message-content validation against real B3 sample captures" below for the full per-tag breakdown | Substantially improved by #111/#114 but still not a byte-for-byte reproduction (fixed placeholders in some `X` entry-level fields, partial `y` field set, `55=Symbol` retained in `W` for compatibility, incremental tag ordering not exact) | Cosmetic to moderate — content is parseable and close to production shape, some fields still synthetic | #108 (initial gap analysis), #111/#114 (majority closed) |
| Explicit deviations (auth, certification, reconnect model, conflation cadence, acceptor/initiator role) | See spec sections on session authentication, cross-session gap-fill, and connectivity roles | Already fully documented in "Explicit deviations from the real B3 product" below — intentional sandbox simplifications, not accidental drift | By design — not a bug | See "Explicit deviations" section below |

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
at least `UMDF_FIX_CONFLATED_PORT=<port>`. Once enabled, transport compression is always on and mirrors the real B3 conflated feed: downstream clients must wrap the TCP socket in a ZLIB inflater before parsing FIX tag=value frames. Optional transport tuning
includes `UMDF_FIX_CONFLATED_CONFLATION_MS`,
`UMDF_FIX_CONFLATED_RESEND_BUFFER_CAPACITY`,
`UMDF_FIX_CONFLATED_OUTBOUND_QUEUE_CAPACITY`, and
`UMDF_FIX_CONFLATED_EVENT_QUEUE_CAPACITY`; see
[docs/CONFIGURATION.md](CONFIGURATION.md).
