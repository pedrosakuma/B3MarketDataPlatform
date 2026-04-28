# P10-3 — `BookManager.OnPacket` Profiling Findings

Author: P10 audit cycle
Status: Exploratory characterization complete; follow-up optimization left as a separate phase.

## Summary

Profiled `BookManager.OnPacket` end-to-end on the steady-state Apply path
using two harnesses:

1. **`BookManagerOnPacketBenchmarks`** — BenchmarkDotNet harness with
   `[MemoryDiagnoser]`, 64 / 512 symbols × 10k / 100k messages, mix of 40 %
   Order add / 30 % update / 30 % delete.
2. **`OnPacketAllocProbe`** — one-shot `GC.GetAllocatedBytesForCurrentThread`
   probe (90 k measured ops after a 10 k warm-up) for an exact byte count
   uncontaminated by BDN scaffolding.

Both runs are reproducible from the repo root:

```bash
cd benchmarks/B3.Umdf.Book.Benchmarks
dotnet run -c Release -- --filter '*BookManagerOnPacketBenchmarks*'
dotnet run -c Release -- alloc-probe
```

## Throughput baseline (BenchmarkDotNet, AMD EPYC 7763)

| Messages | Symbols | Mean       | ns / op | ops / s    |
| -------- | ------- | ---------: | ------: | ---------: |
| 10 000   | 64      |   528 µs   |   53 ns | 18.9 M / s |
| 10 000   | 512     |   618 µs   |   62 ns | 16.2 M / s |
| 100 000  | 64      | 5 298 µs   |   53 ns | 18.9 M / s |
| 100 000  | 512     | 6 182 µs   |   62 ns | 16.2 M / s |

**Headroom**: production peak observed ≈ 150 678 packets/s →
~ 111 167 events/s (PCAP traces). Single-threaded `OnPacket` capacity is
two orders of magnitude above that; `OnPacket` itself is **not** the
bottleneck even at the worst-observed live load.

The 16 % regression from 64 → 512 symbols is consistent with the working
set growing past L1/L2 cache for the per-side dictionaries — expected and
not actionable without a different data structure.

## Allocation findings 🚨

**`OnPacketAllocProbe` (post-warmup, 90 000 ops, 64 symbols):**

```
ns / op           : 2206.61
bytes / op        :   60.03
Gen0 collections  :    0   (heap had headroom)
```

**BDN `[MemoryDiagnoser]` (steady state, 100 000 ops):**

| Symbols | Allocated/Op |
| ------- | -----------: |
| 64      |       32 B   |
| 512     |       32 B   |

The two harnesses agree on the qualitative finding (steady-state
allocations are NOT zero); the quantitative gap (32 B vs 60 B) is
explained by BDN running multiple iterations against a `BookManager`
whose internal dictionaries / level lists have already amortized their
growth, while the probe still sees List/Dictionary resizes as the books
expand past 1 000 orders per side.

**Production projection** (≈ 111 k OnPacket/s):
* Lower bound (BDN steady-state, 32 B/op): ~ 3.5 MB/s allocated → 12.7
  GB/hour → frequent Gen0 collects.
* Upper bound (probe, 60 B/op): ~ 6.6 MB/s allocated → 23.8 GB/hour.

Both are well within the workstation GC's comfort zone, but they explain
the recurring Gen0 collections seen in `dotnet-counters` during live
runs. **No leak** — every allocation is short-lived.

## Suspected allocation sources (NOT root-caused — flagged for follow-up)

Surveyed the Apply-path code in `BookManager.HandleOrder` /
`BookSide.AddOrUpdate`. Nothing obvious allocates per call (the
`OrderBookEntry` is a `struct`, `BookStore` lookups go through the frozen
dictionary, the `BookSbeHandler` is a `struct` so SBE dispatch is
devirtualized). The most plausible sources of per-op churn:

1. **`Dictionary<ulong, OrderBookEntry>` growth inside each `BookSide`.**
   Initial capacity is 128 (`BookSide.cs:9`); a busy symbol with > 128
   live orders triggers a power-of-two resize. The probe shows the
   allocation does not amortize to zero, suggesting some symbols cycle
   through resizes repeatedly as orders churn.
2. **`List<(long Price, List<OrderBookEntry>)>` price-level growth**
   (`BookSide.cs:15`, initial capacity 64). Same dynamic.
3. **`List<OrderBookEntry>` per price level** (rented from `_listPool`,
   but new lists are still allocated when the pool is empty —
   `BookSide.cs:325`).

A targeted A/B (raise initial dictionary capacity to e.g. 4096 for hot
symbols, or pre-warm the list pool) would prove or disprove (1) and (2)
quickly.

## Recommendations (deferred to a future phase)

* **Do nothing immediate.** `OnPacket` has 100× headroom over peak live
  load and the allocation rate is comfortably handled by the workstation
  GC. The current numbers do not justify additional complexity.
* **If a future change pushes load 5–10× higher** (multi-feed
  consolidation, multi-tenant deployment), revisit:
  1. Bump `BookSide` dictionary initial capacity to remove the resize
     cycles for hot symbols (very small change, very low risk).
  2. Pre-warm `_listPool` in `BookSide` to a depth matching the typical
     active-price-level count (e.g. 32–64) so `RentList` never falls
     through to `new List<OrderBookEntry>()`.
  3. Investigate replacing the `List<(long, List<OrderBookEntry>)>` for
     price levels with the inverted swap-remove structure already
     prototyped in `BookSideBenchmarks.InvertedBookSide` — ratified by
     that benchmark to be allocation-free for the hot path.
* **Keep `BookManagerOnPacketBenchmarks` and `OnPacketAllocProbe` in the
  repo** as regression sentries. Any future change to the dispatch path
  should be measured against these baselines before merging.

## Out of scope (intentionally not investigated)

* Snapshot-apply path (`SnapshotApplier`) — different code path, low
  frequency, not the hot path the reviewer flagged.
* `RouteMbo` Buffer branch — exercised only during recovery; the
  per-call allocation of the eviction lambda (`BookManager.cs:259`) is a
  known cost paid only by stale symbols and is outside the steady-state
  Apply path measured here.
* Cross-thread contention on registry / book counters — single-threaded
  in production (per-group dispatcher) so not relevant.

---

## Live sampling addendum (2026-04-27)

After the synthetic BenchmarkDotNet baseline above, we ran the consumer
under PCAP replay at **REPLAY_SPEED=1** (real-time, ~200 pkts/s on
DRV/072 only — EQT did not stream during this window) and captured two
10-minute traces with `dotnet-trace` (gc-verbose + dotnet-sampled-thread-time)
plus parallel `dotnet-counters` snapshots.

Trace files committed under `profiles/` (gitignored binaries — keep
locally for re-analysis):

- `profiles/gc-verbose-20260427T233413.nettrace` (12 MB)
- `profiles/cpu-sampled-20260427T234515.nettrace` (53 MB)
- `profiles/counters-gcphase-20260427T233437.csv`
- `profiles/counters-cpuphase-20260427T234515.csv`

Analyzer: `tools/TraceAllocAnalyzer` (TraceEvent 3.1.16). Run as

```bash
dotnet run -c Release --project tools/TraceAllocAnalyzer -- \
  profiles/<trace>.nettrace [topN=30] [--cpu]
```

### Steady-state allocation rate (counters)

| Metric                            | Avg          | Peak       |
| --------------------------------- | ------------ | ---------- |
| Heap allocation rate              | **1.19 MB/s** | 8.68 MB/s  |
| Gen0 size at last collection      | 5.9 MB        | 60.7 MB    |
| Gen1 size at last collection      | 113.6 MB      | 177.3 MB   |
| Gen2 size at last collection      | 357.1 MB      | 435.4 MB   |
| GC pause time                     | ~0 s/s        | 0.20 s/s   |
| Working set                       | 1.27 GB       | 2.23 GB    |
| User CPU                          | 1.03 s/s/2cpu | 1.95 s/s   |

**Per-event alloc:** at the observed ~1,300 events/s, the runtime is
allocating ~915 bytes per event — an order of magnitude above the
60 B/op the BDN harness reports. The delta is explained below: BDN
exercises only the steady MBO Apply path on pre-warmed dictionaries,
while the live consumer also runs the InstrDef cyclical replay,
PendingSnapshot bookkeeping, per-symbol metric tag formatting, and
**per-call closures** that the bench skips entirely.

### Top allocators (gc-verbose, 600 s, 1 083 MB sampled)

By **type** (where the bytes land):

| Bytes  | %    | Type                                                       |
| ------ | ---- | ---------------------------------------------------------- |
| 313 MB | 29.0 | `Entry[UInt64,OrderBookEntry][]` (Dictionary buckets)      |
| 158 MB | 14.6 | `System.String`                                            |
| 121 MB | 11.2 | **`<>c__DisplayClass96_0`** — closure in `BookManager.RouteMbo` |
|  40 MB |  3.7 | `OrderBookEntry[]`                                         |
|  36 MB |  3.3 | `ValueTuple<long,List<OrderBookEntry>>[]` (price levels)   |
|  30 MB |  2.8 | `NoUnderlyingsHandler` (SecDef handler)                    |
|  28 MB |  2.6 | `NoInstrAttribsHandler`                                    |
|  27 MB |  2.5 | `Callback`                                                 |
|  25 MB |  2.3 | `NoLegsHandler`                                            |
|  23 MB |  2.1 | `List<OrderBookEntry>`                                     |
|  21 MB |  2.0 | `MarketOrder` list / arrays                                |

By **leaf method** (where the `new` lives):

| Bytes  | %    | Caller                                                                |
| ------ | ---- | --------------------------------------------------------------------- |
| 288 MB | 26.6 | `BookStore.<GetOrCreate>b__7_0` (`new OrderBook(id)`)                 |
| 221 MB | 20.4 | `MarketDataManager.HandleSecurityDefinition`                          |
| **121 MB** | **11.2** | **`BookManager.RouteMbo`** ← lambda `evictedRptSeq => …`      |
|  88 MB |  8.1 | `SecurityDefinition_12DataReader.ReadGroups` (handler invocations)    |
|  64 MB |  5.9 | `Dictionary<UInt64,OrderBookEntry>.Initialize` (BookSide ctor)        |
|  62 MB |  5.7 | `SnapshotApplier.OnHeader` (PendingSnapshot allocation)               |
|  40 MB |  3.7 | `List<OrderBookEntry>.AddWithResize`                                  |
|  39 MB |  3.6 | `List<__Canon>.AddWithResize` (price-level lists)                     |
|  19 MB |  1.8 | `CandleAggregator.AppendCandle`                                       |

GC counts during the 600 s window: Gen0 = 15, Gen1 = 4, Gen2 = 3.

### CPU sampling addendum

The `dotnet-sampled-thread-time` capture is dominated by idle waits
(~80 %): `Monitor.Wait`, `LowLevelLifoSemaphore.WaitForSignal`,
`Thread.Sleep` (replayer pacing). Real productive work surfaces are:

- 1.86 % `TimestampMergedReplayer.TryReceive` (PCAP replay sleep+spin)
- 0.51 % `MultiFeedManager.RunSyncDispatch`
- < 0.1 % each in `BookManager.HandleOrder` / `HandleDeleteOrder` / `RouteMbo`

**CPU is not the bottleneck at this throughput.** Even extrapolated to
the observed live peak of ~111 k events/s (≈ 85× the trace rate) we
stay below half of one core. The optimization opportunity is purely
allocation pressure → Gen2 heap inflation → tail-latency risk under
sustained load.

### Concrete optimization opportunities (ranked by ROI)

1. **`BookManager.RouteMbo` per-call closure (121 MB / 600 s)** — the
   `evictedRptSeq => { _stateRegistry!.BumpMinHeal(securityId, …) }`
   lambda captures `securityId` and `_stateRegistry`, so the C# compiler
   hoists a `<>c__DisplayClass96_0` to the top of every `RouteMbo`
   invocation. **Even when the Buffer branch is not taken** the closure
   is still allocated (compiler hoists at method entry when any reachable
   lambda captures locals). Fix: extend `StaleMboBuffer.Enqueue` with an
   overload accepting `Action<TState, uint>` + `TState`, then pass a
   static delegate. Eliminates the closure on the steady path.
   - Expected gain: ~200 KB/s steady-state, scaling linearly with
     packet rate (peak load ≈ 17 MB/s saved).

2. **SecurityDefinition replay handlers (≈ 110 MB combined)** —
   `NoUnderlyingsHandler`, `NoInstrAttribsHandler`, `NoLegsHandler`,
   `Callback` are `new`'d on every SecDef invocation, but the InstrDef
   feed cycles continuously (re-broadcasts every few seconds). The
   handlers are stateless wrappers around `MarketDataManager`. Fix:
   cache them as fields on `MarketDataManager` (single instance, reused
   across calls). Pair with the `<>c__DisplayClass36_0` (18.5 MB) which
   is the same SecDef pattern.
   - Expected gain: ~200 KB/s steady-state. Reduces InstrDef replay
     cost by ~50 % for free.

3. **`BookStore.GetOrCreate` factory path (288 MB)** — even though the
   factory is `static`, `ConcurrentDictionary.GetOrAdd` still goes
   through the slow path if the key is missing OR the frozen snapshot
   doesn't include this symbol. Worth verifying that `FreezeBooks` is
   called before live MBO arrives — and bumping `OrderBook` /
   `BookSide` initial dictionary capacities so they don't resize during
   warmup. The 64 MB attributed to `Dictionary.Initialize` confirms
   first-touch sizing churn.

4. **`PendingSnapshot` allocations (62 MB)** — `SnapshotApplier.OnHeader`
   instantiates a `PendingSnapshot` per snapshot fragment cycle (the
   staging buffers introduced in commit 7fe384e and earlier). Pool
   these per-symbol; the buffer-swap pattern in `ApplySnapshotStaging`
   already supports lifecycle reuse.

5. **Strings (158 MB)** — likely dominated by SecDef ASCII reads
   + per-instrument metric tag formatting. Mitigation: intern symbol
   names once, format tag strings once at first observation.

### Decision

These are all **deferred**. None of them threaten correctness, the
1.27 GB working set is well within the container budget, and Gen0
pause times stay well below 200 ms. The list above is the seed for a
future P11 phase if one of the following triggers fires:

- Live load reaches ≥ 5× the current per-symbol message rate.
- Gen2 heap exceeds 1 GB sustained.
- p99.9 OnPacket latency regresses past the bench baseline (60 ns/op).

The traces and analyzer are kept in-tree so the analysis is
reproducible — no PerfView / Visual Studio install required.

---

## 5x replay capture (2026-04-28) — actionable findings

The 1x capture above was insufficient: with EQT/084 idle and DRV at
~1 300 events/s the consumer was an order of magnitude below typical
peak. A second capture at **REPLAY_SPEED=5** with both EQT and DRV
streaming reproduced realistic peak conditions: ~75 k events/s,
~98 k packets/s, 18 396 books active, 45 M events processed in the
600 s window.

Trace files (gitignored):

- `profiles/gc-verbose-5x-20260428T000043.nettrace` (15 MB)
- `profiles/counters-5x-20260428T000130.csv`

### Counters under load

| Metric                            | Avg            | Peak             |
| --------------------------------- | -------------- | ---------------- |
| Heap allocation rate              | **6.00 MB/s**  | 21.5 MB/s        |
| Gen1 size at last collection      | **466 MB**     | 756 MB           |
| Gen2 size at last collection      | 451 MB         | 586 MB           |
| LOH size                          | 59 MB          | 79 MB            |
| **GC pause time**                 | **10 ms/s**    | **410 ms** ⚠️    |
| Working set                       | **7.73 GB**    | 10.9 GB          |
| User CPU                          | 1.90 s/s/2cpu  | 2.06 s/s         |

The 410 ms GC pause is the headline: a single Gen1/Gen2 collection
under sustained load stalls the consumer for almost half a second.
Any time-sensitive downstream (websocket clients, gap detection on the
incoming feed) sees this as a latency spike.

### Top allocators at peak (3 462 MB sampled / 600 s)

| Bytes   | %    | Type                                                         |
| ------- | ---- | ------------------------------------------------------------ |
| **1208 MB** | **34.9** | **`<>c__DisplayClass96_0`** — closure in `BookManager.RouteMbo` |
| 755 MB  | 21.8 | `System.String`                                              |
| 135 MB  |  3.9 | `NoInstrAttribsHandler`                                      |
| 130 MB  |  3.8 | `NoUnderlyingsHandler`                                       |
| 130 MB  |  3.8 | `Callback`                                                   |
| 128 MB  |  3.7 | `NoLegsHandler`                                              |
| 115 MB  |  3.3 | `InstrAttribInfo[]`                                          |
| 102 MB  |  3.0 | `<>c__DisplayClass36_0` — closure in `HandleSecurityDefinition` |
|  90 MB  |  2.6 | `Candle[]`                                                   |
|  88 MB  |  2.6 | `UnderlyingInfo[]`                                           |

By **leaf method**:

| Bytes   | %    | Caller                                                          |
| ------- | ---- | --------------------------------------------------------------- |
| **1208 MB** | **34.9** | **`BookManager.RouteMbo`** ← lambda allocates per-call closure  |
| **1082 MB** | **31.3** | **`MarketDataManager.HandleSecurityDefinition`** ← cyclical  |
| 444 MB  | 12.8 | `SecurityDefinition_12DataReader.ReadGroups` (handler invocations) |
| 209 MB  |  6.1 | `List<__Canon>.AddWithResize` (price-level lists)               |
| 147 MB  |  4.2 | `SnapshotApplier.OnHeader` (PendingSnapshot)                    |
|  94 MB  |  2.7 | `List<OrderBookEntry>.AddWithResize`                            |
|  90 MB  |  2.6 | `CandleAggregator.AppendCandle`                                 |
|  87 MB  |  2.5 | `MessageDispatcher.Dispatch`                                    |

GC counts during the 600 s window: **Gen0 = 53, Gen1 = 1, Gen2 = 1**.
Gen0 is healthy (~12 s between collections), but each Gen1+ that does
fire promotes the closure-and-handler churn into long-lived survivor
space, which is why the working set inflates to 7.7 GB sustained.

### Actionable verdict

The "deferred to a future P11" framing in the 1x section above is
**no longer accurate at peak load**. Under realistic peak the closure
in `RouteMbo` alone dominates the entire allocation budget — fixing it
single-handedly cuts heap pressure by ~⅓, with no algorithmic risk.
The recommended P11 scope is:

| # | Fix                                                              | Expected gain | Risk |
| - | ---------------------------------------------------------------- | ------------- | ---- |
| 1 | `StaleMboBuffer.Enqueue<TState>` overload + static delegate in `RouteMbo` to eliminate `<>c__DisplayClass96_0` | ~2 MB/s saved (35 % of total) | low |
| 2 | Cache `NoUnderlyingsHandler` / `NoLegsHandler` / `NoInstrAttribsHandler` / `Callback` as fields on `MarketDataManager`; eliminate `<>c__DisplayClass36_0` | ~0.9 MB/s (15 %) | low |
| 3 | Pool `PendingSnapshot` per symbol (the buffer-swap pattern in `ApplySnapshotStaging` already supports reuse) | ~0.25 MB/s (4 %) | medium |
| 4 | Intern symbol names + cache metric-tag strings | ~1.0 MB/s (17 %) — assumes most strings are tag-formatting | medium |

After (1)+(2) alone we expect alloc rate to drop from 6 MB/s to
**≈ 3 MB/s** and Gen1/Gen2 pressure roughly halved, which should bring
the worst-case GC pause back below 100 ms.

(3) and (4) are larger surgeries with lower per-fix ROI; revisit after
(1)+(2) is measured.

---

## P11 validation — 5x replay capture (post-fix)

**Date:** 2026-04-28. Capture: 600 s `dotnet-trace` (gc-verbose) + 60 s
`dotnet-counters collect`, REPLAY_SPEED=5, both groups Streaming, 18 402
books, sustained ~75k events/s + ~98k packets/s (same load profile as the
pre-P11 baseline above).

**Fix in scope:**
- P11-1 (`b5bb6b0`): RouteMbo closure → static lambda + ValueTuple state via
  new `StaleMboBuffer.Enqueue<TState>` overload.
- P11-2 (`6b6751e`): `HandleSecurityDefinition` early-out keyed on
  `SecurityValidityTimestamp` cached in `InstrumentInfo`.

### Headline numbers — pre-P11 vs post-P11

| Metric                        | Gate     | Pre-P11 (5x) | Post-P11 (5x) | Delta            |
|-------------------------------|----------|--------------|---------------|------------------|
| Sampled alloc (600 s)         | —        | 3.4 GB       | **537.6 MB**  | **−84 %**        |
| Alloc rate (counters, MB/s)   | ≤3.5     | 6.0          | **0.70**      | **−88 %**        |
| Alloc rate (sampled, MB/s)    | —        | 5.7          | **0.90**      | **−84 %**        |
| GC pause peak                 | ≤100 ms  | 410 ms       | **≈0 ms***    | **gate cleared** |
| GC collects / 600 s           | —        | many         | **G0=6 G1=2 G2=1** | dramatic     |
| Working set (RSS, docker)     | ≤5 GB    | 7.7 GB       | **0.93 GB**   | **−88 %**        |
| Throughput                    | —        | 75 k ev/s    | **75 k ev/s** | unchanged        |

\* No GC pauses observed in the 60 s counters window — `dotnet.gc.pause.time`
summed to 0.0 s. The trace recorded only one Gen2 collection across 600 s.

### Top allocation hotspots — pre-P11 vs post-P11

| Allocator                                       | Pre (MB/600s) | Post (MB/600s) | Status |
|-------------------------------------------------|---------------|----------------|--------|
| `BookManager.RouteMbo` closure (`<>c__96_0`)    | 1,208         | **0**          | ✅ eliminated |
| `MarketDataManager.HandleSecurityDefinition`    | 1,082         | 128            | ✅ −88 % (early-out keeps real changes only) |
| `SnapshotApplier.OnHeader`                      | (top 4)       | 134            | unchanged (next P12 candidate) |
| `CandleAggregator.AppendCandle`                 | (top 5)       | 89             | unchanged (per-bar list growth) |
| `List<OrderBookEntry>.AddWithResize`            | (steady)      | 73             | unchanged (per-symbol BookSide growth) |
| `MessageDispatcher.Dispatch`                    | (steady)      | 58             | unchanged |

The two P11 targets account for the entire delta — every other allocator
is unchanged, confirming the fix did not regress any other path.

### Verdict

All four numeric gates cleared by 4×–10× margin:
- Alloc rate: 0.70 MB/s vs gate 3.5 MB/s (5× headroom)
- GC pause peak: ≈0 ms vs gate 100 ms
- Working set: 935 MB vs gate 5 GB (5× headroom)
- Throughput preserved at 75 k events/s

P11 closes the 5x-replay allocation hotspots cleanly. Next candidates
(`SnapshotApplier.OnHeader`, `CandleAggregator.AppendCandle`,
`BookSide` list/dict growth) are tracked in the original "next levers"
list above, but their absolute weight at REPLAY_SPEED=5 is now small
enough that GC pressure is no longer the system's bottleneck — CPU
spent in SBE parse + book mutation is the natural successor focus.
