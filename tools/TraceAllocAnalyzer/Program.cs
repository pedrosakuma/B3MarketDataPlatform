using Microsoft.Diagnostics.Tracing;
using Microsoft.Diagnostics.Tracing.Etlx;
using Microsoft.Diagnostics.Tracing.Parsers.Clr;
using Microsoft.Diagnostics.Tracing.Stacks;
using Microsoft.Diagnostics.Symbols;

if (args.Length < 1)
{
    Console.Error.WriteLine("usage: TraceAllocAnalyzer <input.nettrace> [topN=30] [--cpu]");
    return 2;
}
string input = args[0];
int topN = 30;
bool cpuMode = false;
foreach (var a in args.Skip(1))
{
    if (a == "--cpu") cpuMode = true;
    else if (int.TryParse(a, out var n)) topN = n;
}

string etlx = TraceLog.CreateFromEventPipeDataFile(input);
using var traceLog = new TraceLog(etlx);

if (cpuMode)
{
    // Aggregate SampleProfiler events by leaf method
    var byLeafCpu = new Dictionary<string, long>(StringComparer.Ordinal);
    long totalSamples = 0;
    var stacks = new MutableTraceEventStackSource(traceLog) { ShowUnknownAddresses = false };
    var sampleCpu = new StackSourceSample(stacks);
    foreach (var e in traceLog.Events)
    {
        if (e.ProviderName != "Microsoft-DotNETCore-SampleProfiler") continue;
        if (e.EventName != "Thread/Sample" && e.EventName != "Thread/StackSample") continue;
        var csIdx = e.CallStackIndex();
        if (csIdx == CallStackIndex.Invalid) continue;
        sampleCpu.Metric = 1;
        sampleCpu.TimeRelativeMSec = e.TimeStampRelativeMSec;
        sampleCpu.StackIndex = stacks.GetCallStack(csIdx, null);
        stacks.AddSample(sampleCpu);
        totalSamples++;
    }
    stacks.DoneAddingSamples();
    stacks.ForEach(s =>
    {
        var stk = s.StackIndex;
        if (stk == StackSourceCallStackIndex.Invalid) return;
        var leaf = stacks.GetFrameIndex(stk);
        var name = stacks.GetFrameName(leaf, false);
        if (!byLeafCpu.TryGetValue(name, out var v)) v = 0;
        byLeafCpu[name] = v + 1;
    });
    Console.WriteLine($"=== CPU sampling: {input}");
    Console.WriteLine($"Total samples : {totalSamples:N0}  ({(traceLog.SessionEndTime - traceLog.SessionStartTime).TotalSeconds:F0} s)");
    Console.WriteLine($"--- Top {topN} leaf methods (inclusive of inlined work attributed to leaf) ---");
    Console.WriteLine($"{"Samples",10}  {"Pct",6}  Method");
    foreach (var kv in byLeafCpu.OrderByDescending(k => k.Value).Take(topN))
    {
        double pct = totalSamples == 0 ? 0 : 100.0 * kv.Value / totalSamples;
        Console.WriteLine($"{kv.Value,10:N0}  {pct,6:F2}  {kv.Key}");
    }
    return 0;
}

// 1) Aggregate AllocationTick (sampled allocations) by type name
var byType = new Dictionary<string, (long count, long bytes)>(StringComparer.Ordinal);
long totalSampledBytes = 0;
long allocTickCount = 0;
foreach (var e in traceLog.Events)
{
    if (e is GCAllocationTickTraceData a)
    {
        allocTickCount++;
        var name = a.TypeName ?? "<unknown>";
        if (!byType.TryGetValue(name, out var v)) v = (0, 0);
        v.count += 1;
        v.bytes += a.AllocationAmount64;
        byType[name] = v;
        totalSampledBytes += a.AllocationAmount64;
    }
}

Console.WriteLine($"=== Trace: {input}");
Console.WriteLine($"Process count   : {traceLog.Processes.Count}");
Console.WriteLine($"Trace duration  : {(traceLog.SessionEndTime - traceLog.SessionStartTime).TotalSeconds:F1} s");
Console.WriteLine($"AllocationTicks : {allocTickCount:N0}  (each ~100 KB sampled)");
Console.WriteLine($"Sampled alloc   : {totalSampledBytes / 1024.0 / 1024.0:F1} MB");
Console.WriteLine();

Console.WriteLine($"--- Top {topN} types by sampled allocation bytes ---");
Console.WriteLine($"{"Bytes(MB)",10}  {"Pct",6}  {"Ticks",8}  Type");
foreach (var kv in byType.OrderByDescending(k => k.Value.bytes).Take(topN))
{
    double mb = kv.Value.bytes / 1024.0 / 1024.0;
    double pct = totalSampledBytes == 0 ? 0 : 100.0 * kv.Value.bytes / totalSampledBytes;
    Console.WriteLine($"{mb,10:F2}  {pct,6:F1}  {kv.Value.count,8:N0}  {kv.Key}");
}
Console.WriteLine();

// 2) Aggregate stacks for AllocationTick
Console.WriteLine($"--- Top {topN} call stacks for sampled allocations (leaf-rooted) ---");
var stackSource = new MutableTraceEventStackSource(traceLog)
{
    OnlyManagedCodeStacks = true,
};
var computer = new SampleProfilerThreadTimeComputer(traceLog, new SymbolReader(TextWriter.Null));
// Build a custom stack source from AllocationTick events
var allocStacks = new MutableTraceEventStackSource(traceLog);
allocStacks.ShowUnknownAddresses = false;
var sample = new StackSourceSample(allocStacks);
foreach (var e in traceLog.Events)
{
    if (e is GCAllocationTickTraceData a)
    {
        var csIdx = a.CallStackIndex();
        if (csIdx == CallStackIndex.Invalid) continue;
        sample.Metric = a.AllocationAmount64;
        sample.TimeRelativeMSec = a.TimeStampRelativeMSec;
        sample.StackIndex = allocStacks.GetCallStack(csIdx, null);
        var nameIdx = allocStacks.Interner.FrameIntern("Type: " + (a.TypeName ?? "<unknown>"));
        sample.StackIndex = allocStacks.Interner.CallStackIntern(nameIdx, sample.StackIndex);
        allocStacks.AddSample(sample);
    }
}
allocStacks.DoneAddingSamples();

// Aggregate by leaf method (caller of new)
var byLeaf = new Dictionary<string, double>(StringComparer.Ordinal);
allocStacks.ForEach(s =>
{
    var stk = s.StackIndex;
    if (stk == StackSourceCallStackIndex.Invalid) return;
    // Skip the synthetic "Type: ..." leaf
    var first = allocStacks.GetFrameIndex(stk);
    var firstName = allocStacks.GetFrameName(first, false);
    var caller = allocStacks.GetCallerIndex(stk);
    if (caller == StackSourceCallStackIndex.Invalid) return;
    var leafFrame = allocStacks.GetFrameIndex(caller);
    var leafName = allocStacks.GetFrameName(leafFrame, false);
    if (!byLeaf.TryGetValue(leafName, out var v)) v = 0;
    byLeaf[leafName] = v + s.Metric;
});

Console.WriteLine($"{"Bytes(MB)",10}  {"Pct",6}  Leaf method (allocator caller)");
foreach (var kv in byLeaf.OrderByDescending(k => k.Value).Take(topN))
{
    double mb = kv.Value / 1024.0 / 1024.0;
    double pct = totalSampledBytes == 0 ? 0 : 100.0 * kv.Value / totalSampledBytes;
    Console.WriteLine($"{mb,10:F2}  {pct,6:F1}  {kv.Key}");
}
Console.WriteLine();

// 3) GC summary
int gen0 = 0, gen1 = 0, gen2 = 0;
foreach (var e in traceLog.Events)
{
    if (e is GCStartTraceData s)
    {
        if (s.Depth == 0) gen0++;
        else if (s.Depth == 1) gen1++;
        else if (s.Depth == 2) gen2++;
    }
    else if (e is GCEndTraceData) { /* no-op */ }
}
// Pause times via GCStats parser would need GCProcess; skip for brevity.
Console.WriteLine($"--- GC counts during trace ---");
Console.WriteLine($"Gen0: {gen0}   Gen1: {gen1}   Gen2: {gen2}");

return 0;
