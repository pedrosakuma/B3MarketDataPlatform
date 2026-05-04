using System.Text.Json;
using B3.Umdf.Tools.PerfBaselineCompare;

namespace B3.Umdf.Tools.PerfBaselineCompare.Tests;

/// <summary>
/// Exercises <see cref="Comparer.Compare"/> against a small canned BDN
/// report so the regression / pass / missing branches are all locked in
/// without spinning up the real benchmark suite.
/// </summary>
public class ComparerTests
{
    // Canned BDN-shaped report: one benchmark with two parameter sets.
    // Mean is in nanoseconds-per-invocation (BDN's native unit); the
    // BookManager benchmark loops MessageCount times per invocation, so
    // for OpsPerInvocation=10000 a Mean of 530000 ns ≈ 53 ns/op.
    private const string SampleReport = """
      {
        "Title": "sample",
        "Benchmarks": [
          {
            "Type": "B3.Umdf.Book.Benchmarks.BookManagerOnPacketBenchmarks",
            "Method": "OnPacket_DispatchLoop",
            "Parameters": "MessageCount=10000, SymbolCount=64",
            "Statistics": { "Mean": 530000.0 },
            "Memory": { "BytesAllocatedPerOperation": 320000 }
          },
          {
            "Type": "B3.Umdf.Book.Benchmarks.BookManagerOnPacketBenchmarks",
            "Method": "OnPacket_DispatchLoop",
            "Parameters": "MessageCount=10000, SymbolCount=512",
            "Statistics": { "Mean": 800000.0 },
            "Memory": { "BytesAllocatedPerOperation": 320000 }
          }
        ]
      }
      """;

    private static JsonDocument Report() => JsonDocument.Parse(SampleReport);

    private static Baseline MakeBaseline(
        double? meanNs = 53.0,
        double? allocB = 32.0,
        double meanTol = 10,
        double allocTol = 20,
        int symbolCount = 64,
        string method = "OnPacket_DispatchLoop",
        string benchmark = "BookManagerOnPacketBenchmarks")
    {
        return new Baseline
        {
            Benchmark = benchmark,
            Method = method,
            Params = new Dictionary<string, JsonElement>
            {
                ["MessageCount"] = JsonDocument.Parse("10000").RootElement.Clone(),
                ["SymbolCount"] = JsonDocument.Parse(symbolCount.ToString()).RootElement.Clone(),
            },
            OpsPerInvocation = 10000,
            Metrics = new BaselineMetrics { MeanNsPerOp = meanNs, AllocBPerOp = allocB },
            Tolerance = new BaselineTolerance { MeanPct = meanTol, AllocPct = allocTol },
        };
    }

    [Fact]
    public void WithinTolerance_Passes()
    {
        var results = Comparer.Compare(new[] { MakeBaseline() }, Report());

        var r = Assert.Single(results);
        Assert.Equal("PASS", r.Status);
        Assert.NotNull(r.ObservedMeanNsPerOp);
        Assert.Equal(53.0, r.ObservedMeanNsPerOp!.Value, 1);
        Assert.Equal(32.0, r.ObservedAllocBPerOp!.Value, 1);
    }

    [Fact]
    public void MeanOverTolerance_Fails()
    {
        // Observed for SymbolCount=512 row is 80 ns/op; baseline 62 ns/op,
        // tolerance 10% → ~28% delta should trip the gate.
        var baseline = MakeBaseline(meanNs: 62.0, allocB: 32.0, symbolCount: 512);

        var r = Assert.Single(Comparer.Compare(new[] { baseline }, Report()));
        Assert.Equal("FAIL", r.Status);
        Assert.True(r.MeanPctDelta is > 10);
        Assert.Contains("mean", r.Message);
    }

    [Fact]
    public void AllocOverTolerance_Fails()
    {
        // Observed alloc is 32 B/op; baseline 20 B/op, tol 20% → +60% delta.
        var baseline = MakeBaseline(meanNs: 53.0, allocB: 20.0, allocTol: 20);

        var r = Assert.Single(Comparer.Compare(new[] { baseline }, Report()));
        Assert.Equal("FAIL", r.Status);
        Assert.True(r.AllocPctDelta is > 20);
        Assert.Contains("alloc", r.Message);
    }

    [Fact]
    public void MissingBenchmark_ReportsClearMessage()
    {
        var baseline = MakeBaseline(method: "DoesNotExist");

        var r = Assert.Single(Comparer.Compare(new[] { baseline }, Report()));
        Assert.Equal("MISSING", r.Status);
        Assert.Contains("No matching benchmark", r.Message);
        Assert.Contains("DoesNotExist", r.Message);
    }

    [Fact]
    public void MissingMetric_IsNotEnforced()
    {
        // Baseline declares only mean; alloc is unset and must not trigger a
        // failure even if the BDN row reports allocations.
        var baseline = MakeBaseline(meanNs: 53.0, allocB: null);

        var r = Assert.Single(Comparer.Compare(new[] { baseline }, Report()));
        Assert.Equal("PASS", r.Status);
        Assert.Null(r.AllocPctDelta);
    }

    [Fact]
    public void ParamsMatch_RequiresAllListedKeys()
    {
        var parsed = Comparer.ParseBdnParams("MessageCount=10000, SymbolCount=64");
        Assert.Equal("10000", parsed["MessageCount"]);
        Assert.Equal("64", parsed["SymbolCount"]);

        var want = new Dictionary<string, JsonElement>
        {
            ["MessageCount"] = JsonDocument.Parse("10000").RootElement.Clone(),
            ["SymbolCount"] = JsonDocument.Parse("64").RootElement.Clone(),
        };
        Assert.True(Comparer.ParamsMatch("MessageCount=10000, SymbolCount=64", want));
        Assert.False(Comparer.ParamsMatch("MessageCount=10000, SymbolCount=512", want));
        Assert.False(Comparer.ParamsMatch("MessageCount=10000", want));
    }
}
