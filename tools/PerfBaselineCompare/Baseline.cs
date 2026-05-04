using System.Text.Json;
using System.Text.Json.Serialization;

namespace B3.Umdf.Tools.PerfBaselineCompare;

/// <summary>
/// Strongly-typed shape for our committed baseline JSONs under
/// <c>docs/perf/baselines/</c>. Designed to be hand-edited; missing
/// metrics are treated as "not enforced".
/// </summary>
public sealed class Baseline
{
    [JsonPropertyName("benchmark")]
    public string Benchmark { get; set; } = "";

    [JsonPropertyName("method")]
    public string Method { get; set; } = "";

    /// <summary>
    /// Subset match against BDN's <c>Parameters</c> field
    /// (e.g. <c>{"MessageCount":10000, "SymbolCount":64}</c>).
    /// All listed keys must match; extras in the BDN row are ignored.
    /// </summary>
    [JsonPropertyName("params")]
    public Dictionary<string, JsonElement> Params { get; set; } = new();

    /// <summary>
    /// BenchmarkDotNet reports per-invocation numbers. When a benchmark
    /// processes N items in a single Benchmark call (e.g. a tight inner
    /// loop), set this to N so the comparer reports per-item metrics
    /// matching the doc baselines.
    /// </summary>
    [JsonPropertyName("ops_per_invocation")]
    public long? OpsPerInvocation { get; set; }

    [JsonPropertyName("metrics")]
    public BaselineMetrics Metrics { get; set; } = new();

    [JsonPropertyName("tolerance")]
    public BaselineTolerance Tolerance { get; set; } = new();

    [JsonPropertyName("notes")]
    public string? Notes { get; set; }
}

public sealed class BaselineMetrics
{
    [JsonPropertyName("mean_ns_per_op")]
    public double? MeanNsPerOp { get; set; }

    [JsonPropertyName("alloc_b_per_op")]
    public double? AllocBPerOp { get; set; }
}

public sealed class BaselineTolerance
{
    /// <summary>Allowed positive % delta of mean_ns_per_op vs baseline.</summary>
    [JsonPropertyName("mean_pct")]
    public double MeanPct { get; set; } = 10;

    /// <summary>Allowed positive % delta of alloc_b_per_op vs baseline.</summary>
    [JsonPropertyName("alloc_pct")]
    public double AllocPct { get; set; } = 20;
}
