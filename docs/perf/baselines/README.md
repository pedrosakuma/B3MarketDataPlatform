# Perf baselines (machine-readable)

This directory holds one JSON file per `(benchmark, method, parameter set)`
that we want the opt-in perf-smoke CI workflow
(`.github/workflows/perf-smoke.yml`) to compare BenchmarkDotNet runs
against. The comparison tool lives at
[`tools/PerfBaselineCompare/`](../../../tools/PerfBaselineCompare/) and the
broader rationale is tracked in
[issue #19](https://github.com/pedrosakuma/B3MarketDataPlatform/issues/19).

## File format

Each `*.json` file in this directory is treated as a baseline. Shape:

```jsonc
{
  "benchmark": "BookManagerOnPacketBenchmarks",   // BDN class (short name)
  "method":    "OnPacket_DispatchLoop",           // [Benchmark] method name
  "params":    { "MessageCount": 10000, "SymbolCount": 64 }, // BDN [Params] subset
  "ops_per_invocation": 10000,                    // optional; divides BDN per-call numbers
  "captured_at": "2026-04-27",                    // ISO date
  "captured_on": "AMD EPYC 7763",                 // human-readable hardware tag
  "runtime":     "net10.0 Release",
  "metrics": {
    "mean_ns_per_op": 53.0,                       // optional — omit to skip the gate
    "alloc_b_per_op": 32.0                        // optional — omit to skip the gate
  },
  "tolerance": { "mean_pct": 15, "alloc_pct": 25 },
  "notes": "Free-form text. Always note the source of the numbers."
}
```

Notes:

* `params` is a **subset match** against BenchmarkDotNet's `Parameters`
  field, so a baseline only needs to list the params it cares about.
* If a metric is omitted under `metrics`, that metric is **not enforced** for
  this row. We use this for benchmarks where the committed prose docs
  don't include a defensible number — better to ship a schema-only
  placeholder than to fabricate one.
* `ops_per_invocation` is required when a `[Benchmark]` method runs an
  inner loop. BDN reports per-invocation numbers; the comparer divides
  by `ops_per_invocation` so the JSON stays in human-friendly per-item
  units.
* Tolerances are intentionally loose. Treat regressions as **advisory**
  unless they reproduce on stable hardware (see "Hardware caveat" below).

## How to refresh a baseline

1. On a quiet machine (no other CPU/GC pressure, fixed CPU governor),
   build Release and run the relevant suite, e.g.:

   ```bash
   cd benchmarks/B3.Umdf.Book.Benchmarks
   dotnet run -c Release -- --filter '*BookManagerOnPacketBenchmarks*'
   ```

2. Find the per-benchmark report under
   `BenchmarkDotNet.Artifacts/results/*-report-full.json`.
3. Copy the `Mean` (ns) and `Memory.BytesAllocatedPerOperation` values
   for each parameter row you have a baseline for. Divide both by
   `ops_per_invocation` if the benchmark loops internally.
4. Update `metrics.mean_ns_per_op` / `metrics.alloc_b_per_op` and bump
   `captured_at` / `captured_on`.
5. Commit the JSON change in the **same PR** as the perf-affecting code
   change. PRs that move perf MUST update the affected baseline JSON;
   reviewers should reject perf-affecting PRs that don't.

## Hardware caveat

The current numbers for `BookManagerOnPacketBenchmarks` come from the
prose tables in [`docs/perf/onpacket-profiling.md`](../onpacket-profiling.md)
(captured on AMD EPYC 7763). They are committed so the comparison tool
and CI workflow have something to chew on, **not** because we trust them
as a long-term gate. Two follow-up steps before treating them as a hard
gate:

1. Re-capture on the same stable runner that CI will use.
2. Tighten the tolerances once we know the run-to-run noise on that
   runner.

Until that's done, `.github/workflows/perf-smoke.yml` is opt-in (label
`run-perf` or manual dispatch) and explicitly **not** wired into the
required-checks list.

## Why some files are schema-only

`BookSideBenchmarks.*.json` and `MpscPacketRingBenchmarks.*.json` ship
without populated `metrics`. The committed prose docs don't include a
single defensible number for those benchmarks in isolation, so we'd
rather commit a no-metric placeholder (which the comparer treats as
"matched but nothing to gate on") than fabricate numbers that quietly
drift. Populating those numbers is a follow-up to issue #19 and should
happen the first time the perf-smoke workflow runs on a stable runner.
