using System.Text.Json;
using B3.Umdf.Tools.PerfBaselineCompare;

// Usage:
//   perf-baseline-compare --baselines <dir> --report <bdn-report.json> [--report <other.json> ...]
//
// Exits 0 if every baseline that has a metric stays within tolerance.
// Exits 1 if any baseline regresses or cannot be matched.
// Exits 2 on argument / IO errors.

string? baselineDir = null;
var reports = new List<string>();

for (int i = 0; i < args.Length; i++)
{
    switch (args[i])
    {
        case "--baselines":
        case "-b":
            if (++i >= args.Length) return Fail("--baselines requires a path");
            baselineDir = args[i];
            break;
        case "--report":
        case "-r":
            if (++i >= args.Length) return Fail("--report requires a path");
            reports.Add(args[i]);
            break;
        case "--help":
        case "-h":
            Console.WriteLine("Usage: perf-baseline-compare --baselines <dir> --report <file> [--report <file> ...]");
            return 0;
        default:
            return Fail($"Unknown argument: {args[i]}");
    }
}

if (baselineDir is null) return Fail("Missing --baselines <dir>");
if (reports.Count == 0) return Fail("Missing --report <file>");
if (!Directory.Exists(baselineDir)) return Fail($"Baselines directory not found: {baselineDir}");

var baselineFiles = Directory.GetFiles(baselineDir, "*.json", SearchOption.TopDirectoryOnly)
    .OrderBy(f => f, StringComparer.Ordinal)
    .ToList();
if (baselineFiles.Count == 0)
{
    Console.Error.WriteLine($"No baseline JSON files in {baselineDir}");
    return 2;
}

var baselines = new List<Baseline>(baselineFiles.Count);
foreach (var path in baselineFiles)
{
    try
    {
        var json = File.ReadAllText(path);
        var b = JsonSerializer.Deserialize<Baseline>(json,
            new JsonSerializerOptions { ReadCommentHandling = JsonCommentHandling.Skip });
        if (b is null) { Console.Error.WriteLine($"Skipping empty baseline: {path}"); continue; }
        baselines.Add(b);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Failed to parse baseline {path}: {ex.Message}");
        return 2;
    }
}

// Combine all benchmark rows from every supplied report so a single
// invocation can validate the full perf-smoke suite.
var combined = new List<JsonElement>();
foreach (var r in reports)
{
    if (!File.Exists(r)) return Fail($"Report not found: {r}");
    try
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(r));
        if (!doc.RootElement.TryGetProperty("Benchmarks", out var arr))
        {
            Console.Error.WriteLine($"Report missing 'Benchmarks' array: {r}");
            return 2;
        }
        foreach (var el in arr.EnumerateArray())
        {
            // Clone so we can keep references after the JsonDocument is disposed.
            combined.Add(el.Clone());
        }
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Failed to parse report {r}: {ex.Message}");
        return 2;
    }
}

var combinedJson = JsonSerializer.SerializeToDocument(new { Benchmarks = combined });
var results = Comparer.Compare(baselines, combinedJson);

PrintTable(results);

bool failed = results.Any(r => r.Status != "PASS");
return failed ? 1 : 0;

static int Fail(string message)
{
    Console.Error.WriteLine(message);
    return 2;
}

static void PrintTable(List<ComparisonResult> results)
{
    Console.WriteLine();
    Console.WriteLine("Perf baseline comparison");
    Console.WriteLine(new string('=', 96));
    Console.WriteLine($"{"STATUS",-7} {"BENCHMARK.METHOD",-55} {"MEAN Δ%",10} {"ALLOC Δ%",12}");
    Console.WriteLine(new string('-', 96));
    foreach (var r in results)
    {
        var name = $"{r.Baseline.Benchmark}.{r.Baseline.Method}";
        if (name.Length > 55) name = name[..55];
        var mean = r.MeanPctDelta.HasValue ? $"{r.MeanPctDelta.Value:+0.0;-0.0;0.0}" : "n/a";
        var alloc = r.AllocPctDelta.HasValue ? $"{r.AllocPctDelta.Value:+0.0;-0.0;0.0}" : "n/a";
        Console.WriteLine($"{r.Status,-7} {name,-55} {mean,10} {alloc,12}");
        if (r.Status != "PASS") Console.WriteLine($"        → {r.Message}");
    }
    Console.WriteLine(new string('=', 96));
    int pass = results.Count(r => r.Status == "PASS");
    int fail = results.Count(r => r.Status == "FAIL");
    int miss = results.Count(r => r.Status == "MISSING");
    Console.WriteLine($"Total: {results.Count}  Pass: {pass}  Fail: {fail}  Missing: {miss}");
}

// Make Program a partial class so the test project can reference internals via
// InternalsVisibleTo if ever needed; today the comparer logic is in its own type.
public partial class Program { }
