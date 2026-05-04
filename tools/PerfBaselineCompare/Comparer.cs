using System.Globalization;
using System.Text.Json;

namespace B3.Umdf.Tools.PerfBaselineCompare;

/// <summary>
/// Result of comparing one baseline against a BDN report.
/// Status indicates whether the baseline was satisfied, exceeded the
/// tolerance, or could not be matched against any benchmark in the report.
/// </summary>
public sealed record ComparisonResult(
    Baseline Baseline,
    string Status, // "PASS", "FAIL", "MISSING"
    double? ObservedMeanNsPerOp,
    double? ObservedAllocBPerOp,
    double? MeanPctDelta,
    double? AllocPctDelta,
    string Message);

public static class Comparer
{
    /// <summary>
    /// Compares <paramref name="baselines"/> against benchmark rows in a
    /// BenchmarkDotNet report JSON document. Returns one result per baseline.
    /// </summary>
    public static List<ComparisonResult> Compare(IReadOnlyList<Baseline> baselines, JsonDocument bdnReport)
    {
        var benchmarks = bdnReport.RootElement.TryGetProperty("Benchmarks", out var b)
            ? b
            : throw new InvalidDataException("BDN report missing 'Benchmarks' array.");

        var results = new List<ComparisonResult>(baselines.Count);
        foreach (var baseline in baselines)
        {
            results.Add(CompareOne(baseline, benchmarks));
        }
        return results;
    }

    private static ComparisonResult CompareOne(Baseline baseline, JsonElement benchmarks)
    {
        JsonElement? match = null;
        foreach (var bench in benchmarks.EnumerateArray())
        {
            if (!TryGetString(bench, "Type", out var type) ||
                !TryGetString(bench, "Method", out var method))
            {
                continue;
            }

            if (!TypeMatches(type, baseline.Benchmark)) continue;
            if (!string.Equals(method, baseline.Method, StringComparison.Ordinal)) continue;

            var paramString = TryGetString(bench, "Parameters", out var p) ? p : "";
            if (!ParamsMatch(paramString, baseline.Params)) continue;

            match = bench;
            break;
        }

        if (match is null)
        {
            return new ComparisonResult(
                baseline,
                "MISSING",
                null, null, null, null,
                $"No matching benchmark in report for {baseline.Benchmark}.{baseline.Method} {ParamsToString(baseline.Params)}");
        }

        double divisor = baseline.OpsPerInvocation is { } n && n > 0 ? n : 1;

        double? observedMeanPerOp = null;
        if (match.Value.TryGetProperty("Statistics", out var stats) &&
            stats.TryGetProperty("Mean", out var mean) &&
            mean.ValueKind == JsonValueKind.Number)
        {
            observedMeanPerOp = mean.GetDouble() / divisor;
        }

        double? observedAllocPerOp = null;
        if (match.Value.TryGetProperty("Memory", out var mem) &&
            mem.ValueKind == JsonValueKind.Object &&
            mem.TryGetProperty("BytesAllocatedPerOperation", out var bytes) &&
            bytes.ValueKind == JsonValueKind.Number)
        {
            observedAllocPerOp = bytes.GetDouble() / divisor;
        }

        double? meanDelta = null;
        bool meanFail = false;
        if (baseline.Metrics.MeanNsPerOp is { } baseMean && baseMean > 0 && observedMeanPerOp is { } obsMean)
        {
            meanDelta = (obsMean - baseMean) / baseMean * 100.0;
            if (meanDelta > baseline.Tolerance.MeanPct) meanFail = true;
        }

        double? allocDelta = null;
        bool allocFail = false;
        if (baseline.Metrics.AllocBPerOp is { } baseAlloc && observedAllocPerOp is { } obsAlloc)
        {
            // Avoid divide-by-zero; if baseline alloc is 0, treat any observed >0 as a regression.
            if (baseAlloc > 0)
            {
                allocDelta = (obsAlloc - baseAlloc) / baseAlloc * 100.0;
                if (allocDelta > baseline.Tolerance.AllocPct) allocFail = true;
            }
            else
            {
                allocDelta = obsAlloc > 0 ? double.PositiveInfinity : 0;
                if (obsAlloc > 0) allocFail = true;
            }
        }

        var status = (meanFail || allocFail) ? "FAIL" : "PASS";
        var msg = status == "PASS"
            ? "within tolerance"
            : $"regression: " +
              (meanFail ? $"mean +{meanDelta:F1}% > {baseline.Tolerance.MeanPct}% " : "") +
              (allocFail ? $"alloc +{allocDelta:F1}% > {baseline.Tolerance.AllocPct}%" : "");

        return new ComparisonResult(
            baseline,
            status,
            observedMeanPerOp,
            observedAllocPerOp,
            meanDelta,
            allocDelta,
            msg.Trim());
    }

    private static bool TypeMatches(string bdnType, string baselineBenchmark)
    {
        // Match the unqualified class name; baseline files use the short name.
        if (string.Equals(bdnType, baselineBenchmark, StringComparison.Ordinal)) return true;
        var lastDot = bdnType.LastIndexOf('.');
        var simple = lastDot >= 0 ? bdnType[(lastDot + 1)..] : bdnType;
        return string.Equals(simple, baselineBenchmark, StringComparison.Ordinal);
    }

    internal static bool ParamsMatch(string bdnParams, IReadOnlyDictionary<string, JsonElement> wanted)
    {
        if (wanted.Count == 0) return true;
        var parsed = ParseBdnParams(bdnParams);
        foreach (var (k, v) in wanted)
        {
            if (!parsed.TryGetValue(k, out var actual)) return false;
            if (!ParamValueEquals(v, actual)) return false;
        }
        return true;
    }

    internal static Dictionary<string, string> ParseBdnParams(string parameters)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        if (string.IsNullOrWhiteSpace(parameters)) return result;
        foreach (var part in parameters.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var eq = part.IndexOf('=');
            if (eq <= 0) continue;
            result[part[..eq].Trim()] = part[(eq + 1)..].Trim();
        }
        return result;
    }

    private static bool ParamValueEquals(JsonElement want, string actual)
    {
        return want.ValueKind switch
        {
            JsonValueKind.Number => want.GetDouble().ToString("R", CultureInfo.InvariantCulture)
                                        == ParseNumber(actual)?.ToString("R", CultureInfo.InvariantCulture)
                                    || want.ToString() == actual,
            JsonValueKind.String => string.Equals(want.GetString(), actual, StringComparison.Ordinal),
            JsonValueKind.True => actual.Equals("True", StringComparison.OrdinalIgnoreCase),
            JsonValueKind.False => actual.Equals("False", StringComparison.OrdinalIgnoreCase),
            _ => string.Equals(want.ToString(), actual, StringComparison.Ordinal),
        };

        static double? ParseNumber(string s) =>
            double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var d) ? d : null;
    }

    private static bool TryGetString(JsonElement element, string name, out string value)
    {
        if (element.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String)
        {
            value = v.GetString() ?? "";
            return true;
        }
        value = "";
        return false;
    }

    private static string ParamsToString(IReadOnlyDictionary<string, JsonElement> p)
    {
        if (p.Count == 0) return "(no params)";
        return "{" + string.Join(", ", p.Select(kv => $"{kv.Key}={kv.Value}")) + "}";
    }
}
