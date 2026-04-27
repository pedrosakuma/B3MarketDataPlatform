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
