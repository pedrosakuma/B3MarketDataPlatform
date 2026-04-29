# MBP (Market-By-Price) wire stream

Status: server-side, wire protocol, tests, and frontend complete (Phase 1 of the
MBP epic). This document captures the design, the wire format additions, the
bandwidth measurements that motivated it, and the operator-facing implications.

## Why MBP

The legacy `Book` (MBO) stream emits one frame per order mutation
(`OrderAdded`, `OrderUpdated`, `OrderDeleted`). For a hot symbol like PETR4 in
a busy minute, the same price level can be touched 50–200× by add/cancel churn
inside a single B3 packet. A typical UI consumer doesn't care about the
individual orders — it only renders the per-level aggregate (totalQty,
orderCount). All those frames boil down to a single net change.

`Mbp` is a parallel stream keyed by `(securityId, side, price)`. Within a
conflation window, every touch to a level collapses to **one** frame. Empirical
measurements on a synthetic hot-symbol trace (see below) show the ratio
quickly drops below 10% of the MBO byte volume as the conflation window grows.

The two streams are independent — subscribers pick via the `DataFlags` bitmask
on `Subscribe`. MBO remains supported for HFT/order-tracking clients that need
the per-order detail. The default frontend toggle now selects MBP.

## Wire protocol additions

Three new message types (identifiers chosen to slot into existing ranges):

| ID       | Name           | Size               | Purpose                       |
|----------|----------------|--------------------|-------------------------------|
| `0x0022` | `LevelSnapshot`| variable           | Connect-time aggregate replay |
| `0x0037` | `LevelUpdate`  | 33 bytes           | Live-level mutation           |
| `0x0038` | `LevelDeleted` | 21 bytes           | Drained-level deletion        |

Layouts (little-endian):

```
LevelUpdate  (33B): hdr(4) | secId(8) | side(1) | price(8) | totalQty(8) | orderCount(4)
LevelDeleted (21B): hdr(4) | secId(8) | side(1) | price(8)
LevelSnapshot:      hdr(4) | secId(8) | bidCount(2) | askCount(2) |
                    bid entries × 20B | ask entries × 20B
   entry (20B):     price(8) | totalQty(8) | orderCount(4)
```

A new flag bit was added: `DataFlags.Mbp = 0x08`. `Everything = Book | Info |
News | Mbp`.

## Server architecture

* `IBookEventHandler.OnPriceLevelChanged(book, side, price)` — new callback
  with a default no-op so existing handlers (tests, ConsoleApp Stats, etc.)
  remain unaffected. `BookManager` invokes it after every mutation that touches
  a priced level: `ADD`, `UPDATE` (one or two prices on a move), `DELETE`
  (priced and priced→market-downgrade paths). The deletion path now does a
  `TryGetOrder` lookup before `Remove` so the price can be captured.

* `BookSide.TryGetLevelAggregate(price, out qty, out count)` and
  `PriceLevelAggregates` enumerator — both backed by the V2 cached aggregates
  introduced in the previous epic, so flush is **O(1)** per dirty level and
  snapshot construction is O(levels) with no per-order scan.

* `GroupConflationHandler._levelBuffer` (`Dictionary<(secId, side, price),
  bool>`) — per-batch dirty set. `OnPriceLevelChanged` only marks; `OnBatchComplete`
  reads `BookSide.TryGetLevelAggregate` and emits either `LevelUpdate` (qty>0)
  or `LevelDeleted` (drained). `OnBookCleared` purges any dirty entries on the
  cleared side to avoid stale emissions.

* **Three routing kinds** (`BroadcastWorkBatch.EventKind`):
  * `BookSubscribers` (unchanged): MBO-only frames.
  * `MbpSubscribers` (new): level frames.
  * `BookOrMbpSubscribers` (new): shared frames — `Trade`, `BookCleared`,
    `MarketTier`, `StaleStatus`, `TradeBust`, `CandleUpdate`. Without this split
    an Mbp-only subscriber would starve on trades and clears.

* `SubscriptionManager.SendSnapshots` sends `LevelSnapshot` for any client
  subscribing with `Mbp`. When `Mbp` is set without `Book`, trade history and
  candles are also replayed (they are L2-view-relevant).

## Frontend changes

* `protocol.js`: new decoders for the three frames; `DATA_FLAGS.MBP = 0x08`.
* `worker.js`: handles `LevelSnapshot`/`LevelUpdate`/`LevelDeleted` directly
  against the per-symbol `bidLevels`/`askLevels` maps. When the subscription
  carries `Mbp`, the worker skips the legacy MBO-derived `levelAdd`/`levelRemove`
  on `OrderAdded`/`Updated`/`Deleted` to avoid double-mutation.
* `app.js` + `index.html`: the `MBP` checkbox is checked by default, `MBO` is
  unchecked. The `Subscribe` button sends `DataFlags.Mbp` unless the operator
  ticks `MBO` (intended for order-tracking / HFT use cases).

## Measured bandwidth

`benchmarks/B3.Umdf.Book.Benchmarks/MbpBandwidthBenchmarks.cs` drives 100k MBO
SBE messages through the full `BookManager.OnPacket` path against a
single hot symbol (PETR4-shaped: 50/50 bid/ask, 80% of activity within the top
3 ticks). A `BandwidthCounter : IBookEventHandler` accumulates the bytes each
route would emit. The conflation window is parametrized.

| BatchSize (events / `OnPacketProcessed`) | MBO bytes  | MBP bytes  | Ratio | Savings |
|------------------------------------------|------------|------------|-------|---------|
| 1   (no conflation)                      | 3,221,792  | 4,049,700  | 1.257 | -25.7%  |
| 16                                       | 3,221,792  | 1,749,078  | 0.543 |  45.7%  |
| 64                                       | 3,221,792  |   720,780  | 0.224 |  77.6%  |
| 256                                      | 3,221,792  |   250,839  | 0.078 |  92.2%  |
| 1024                                     | 3,221,792  |    64,548  | 0.020 |  98.0%  |

Reproduce with:

```
dotnet run -c Release --project benchmarks/B3.Umdf.Book.Benchmarks -- mbp-bandwidth-probe
```

### Reading the table

* **BatchSize=1** is the worst case for MBP: every event flushes immediately,
  and a single `UPDATE` with a price move triggers two `OnPriceLevelChanged`
  emissions (old price + new price). MBP is ~26% larger than MBO here. This
  case never occurs in production because B3 packets carry many SBE messages.
* **BatchSize=16** roughly approximates a B3 incremental packet during normal
  trading. MBP already cuts wire bytes ~half.
* **BatchSize≥64** approximates burst/replay scenarios (back-to-back packets,
  recovery). The asymptote is set by the number of *distinct* levels touched
  per window — with B3's typical depth concentration that's a small set, so
  MBP stabilizes near 1–10% of the MBO volume.

In real B3 traffic (DRV-072 / EQT-084 captures, mean ~4 kpps with 100ms peaks
to ~118 kpps — see `docs/perf/README.md`), each broadcaster batch covers many
events. Expect MBP to deliver order-of-magnitude bandwidth reduction on
hot symbols, with the savings tracking the ratio of *level touches* to
*distinct levels* per batch.

## Testing

* `tests/B3.Umdf.Server.Tests/WireProtocolMbpTests.cs` — round-trip the three
  new frames and the `DataFlags.Mbp` bit.
* `tests/B3.Umdf.Server.Tests/GroupConflationHandlerMbpTests.cs` — fan-out
  invariants:
  * subscribe-time `LevelSnapshot` delivery
  * live-level mutation emits `LevelUpdate` with conflated `(qty, count)`
  * drained level emits `LevelDeleted` (not `LevelUpdate qty=0`)
  * MBO-only subscribers must not receive any level frame
  * MBP-only subscribers receive shared frames (`Trade`) but never raw
    `OrderAdded`/`Updated`/`Deleted`
  * `Book | Mbp` subscribers receive both streams

The full server suite runs at 114 tests, total repository at 422.

## Open follow-ups

* **Observability**: emit a counter for level-buffer depth + flushed-frame
  counts per group, useful to confirm conflation behaviour in production.
* **Snapshot bandwidth on full ring resync**: current path enqueues a
  `LevelSnapshot` per affected security. For 5000 symbols × ~100 levels at
  20B/entry, that's ~12 MB of one-shot replay. If this becomes a problem,
  consider per-symbol lazy snapshot on first packet.
* **Replay validation**: end-to-end PCAP replay with a real frontend
  comparing MBO-derived vs MBP-driven ladders side by side, to confirm
  semantic equivalence on an entire trading session.
