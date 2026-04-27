using System.Collections.Concurrent;
using B3.Umdf.Transport;
using B3.Umdf.Feed;

namespace B3.Umdf.Tools.InboundLatencyProbe;

/// <summary>
/// IFeedEventHandler that measures dispatcher latency without doing any
/// real work (book mutation, market-data fanout). Replaces BookManager so
/// the probe measures ring + FeedHandler state-machine cost only.
///
/// Latency = Stopwatch.GetTimestamp() at OnPacket - packet.ReceivedTimestampTicks.
/// Samples are bucketed by wall-clock 100ms window so we can correlate
/// latency spikes with real-time packet rate (which we know from Phase 0
/// bursts to ~118 kpps for 100ms).
/// </summary>
internal sealed class LatencyRecorderHandler : IFeedEventHandler
{
    private readonly long _runStartTicks;
    private readonly long _bucketTicks;
    private readonly long _warmupTicks;
    private readonly ConcurrentDictionary<long, BucketSamples> _buckets = new();

    public LatencyRecorderHandler(long runStartTicks, long bucketTicks, long warmupTicks)
    {
        _runStartTicks = runStartTicks;
        _bucketTicks = bucketTicks;
        _warmupTicks = warmupTicks;
    }

    public IReadOnlyDictionary<long, BucketSamples> Buckets => _buckets;

    public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId)
    {
        long now = System.Diagnostics.Stopwatch.GetTimestamp();
        long elapsed = now - _runStartTicks;
        if (elapsed < _warmupTicks) return;

        long latency = now - packet.ReceivedTimestampTicks;
        if (latency < 0) latency = 0;

        long bucketIdx = elapsed / _bucketTicks;
        var bucket = _buckets.GetOrAdd(bucketIdx, static _ => new BucketSamples());
        bucket.Add(latency);
    }

    public void OnSequenceReset() { }
    public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
}

/// <summary>
/// Per-bucket lock-protected sample buffer. Lock contention is acceptable:
/// only one dispatch thread per group writes, and we avoid Volatile on each
/// add by serializing through Add().
/// </summary>
internal sealed class BucketSamples
{
    private readonly object _lock = new();
    private long[] _samples = new long[256];
    private int _count;

    public void Add(long sample)
    {
        lock (_lock)
        {
            if (_count == _samples.Length)
            {
                var bigger = new long[_samples.Length * 2];
                Array.Copy(_samples, bigger, _count);
                _samples = bigger;
            }
            _samples[_count++] = sample;
        }
    }

    public long[] Snapshot()
    {
        lock (_lock)
        {
            var copy = new long[_count];
            Array.Copy(_samples, copy, _count);
            return copy;
        }
    }
}
