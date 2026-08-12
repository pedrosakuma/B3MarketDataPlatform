using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace B3.Umdf.FixConflated;

public static class FixConflatedMetrics
{
    public static readonly Meter Meter = new("B3.Umdf.FixConflated", "1.0.0");
    public static readonly UpDownCounter<int> ActiveConnections = Meter.CreateUpDownCounter<int>(
        "b3.umdf.fix_conflated.connections.active",
        unit: "{connections}",
        description: "Active FIX conflated TCP sessions");
    public static readonly Counter<long> MessagesSent = Meter.CreateCounter<long>(
        "b3.umdf.fix_conflated.messages.sent",
        unit: "{messages}",
        description: "FIX session/application messages written to TCP sessions");
    public static readonly Counter<long> BytesSent = Meter.CreateCounter<long>(
        "b3.umdf.fix_conflated.bytes.sent",
        unit: "By",
        description: "Bytes written to FIX conflated TCP sessions");

    private static readonly ConcurrentDictionary<int, IFixConflatedQueueMetricsSource> s_sources = new();

    static FixConflatedMetrics()
    {
        Meter.CreateObservableGauge(
            "b3.umdf.fix_conflated.queue.depth",
            ObserveQueueDepth,
            unit: "{events}",
            description: "Pending queued hot-path events awaiting FIX encode per group");
        Meter.CreateObservableCounter(
            "b3.umdf.fix_conflated.queue.dropped",
            ObserveQueueDrops,
            unit: "{events}",
            description: "Hot-path FIX conflated events dropped because the queue was full");
    }

    public static void RegisterGroup(int groupId, IFixConflatedQueueMetricsSource source)
    {
        ArgumentNullException.ThrowIfNull(source);
        s_sources[groupId] = source;
    }

    public static void UnregisterGroup(int groupId)
    {
        s_sources.TryRemove(groupId, out _);
    }

    private static IEnumerable<Measurement<int>> ObserveQueueDepth()
    {
        foreach (KeyValuePair<int, IFixConflatedQueueMetricsSource> entry in s_sources)
            yield return new Measurement<int>(entry.Value.PendingQueueDepth, new KeyValuePair<string, object?>("group", $"G{entry.Key}"));
    }

    private static IEnumerable<Measurement<long>> ObserveQueueDrops()
    {
        foreach (KeyValuePair<int, IFixConflatedQueueMetricsSource> entry in s_sources)
            yield return new Measurement<long>(entry.Value.DroppedQueueEntries, new KeyValuePair<string, object?>("group", $"G{entry.Key}"));
    }
}

public interface IFixConflatedQueueMetricsSource
{
    int PendingQueueDepth { get; }
    long DroppedQueueEntries { get; }
}
