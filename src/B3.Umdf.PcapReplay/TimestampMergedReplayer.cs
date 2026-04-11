using B3.Umdf.Transport;

namespace B3.Umdf.PcapReplay;

/// <summary>
/// Merges multiple PCAP files by timestamp using a priority queue.
/// Uses memory-mapped readers for zero-copy I/O — packet data points directly
/// into mmap'd pages, eliminating per-packet allocations and copies.
/// UDP payload offset is computed once per reader (constant per PCAP file).
/// Implements ISyncPacketSource for zero-overhead synchronous consumption,
/// and IPacketSource for async compatibility.
/// </summary>
public sealed class TimestampMergedReplayer : ISyncPacketSource, IPacketSource
{
    private readonly PriorityQueue<(PcapPacket Packet, int ReaderIndex), long> _pq = new();
    private readonly List<(MmapPcapReader Reader, ChannelType Channel)> _readers = new();
    private readonly int[] _cachedUdpOffset;
    private readonly ReplayOptions _options;
    private long? _firstTimestamp;
    private long _startTicks;
    private bool _disposed;

    public TimestampMergedReplayer(IReadOnlyList<PcapChannelSource> sources, ReplayOptions? options = null)
    {
        _options = options ?? new ReplayOptions();
        _startTicks = Environment.TickCount64;
        _cachedUdpOffset = new int[sources.Count];

        for (int i = 0; i < sources.Count; i++)
        {
            var src = sources[i];
            var reader = new MmapPcapReader(src.FilePath);
            _readers.Add((reader, src.Channel));
            if (reader.TryReadNext(out var pkt))
            {
                _cachedUdpOffset[i] = UdpExtractor.ComputeUdpPayloadOffset(pkt.Data.Span, reader.LinkType);
                _pq.Enqueue((pkt, i), pkt.TimestampMicros);
            }
        }
    }

    /// <summary>
    /// Synchronous receive — no async overhead, no Channel&lt;T&gt;, no thread pool.
    /// With mmap, the returned packet's Data points directly into the mmap'd region
    /// and remains valid for the lifetime of the replayer.
    /// </summary>
    public bool TryReceive(out UmdfPacket packet)
    {
        if (!_pq.TryDequeue(out var item, out long timestampMicros))
        {
            packet = default;
            return false;
        }

        if (_options.SpeedMultiplier > 0)
        {
            _firstTimestamp ??= timestampMicros;
            long elapsedTargetMs = (long)((timestampMicros - _firstTimestamp.Value) / 1000.0 / _options.SpeedMultiplier);
            long elapsedActualMs = Environment.TickCount64 - _startTicks;
            long delayMs = elapsedTargetMs - elapsedActualMs;
            if (delayMs > 1)
                Thread.Sleep((int)delayMs);
        }

        var (reader, channel) = _readers[item.ReaderIndex];
        var payload = item.Packet.Data.Slice(_cachedUdpOffset[item.ReaderIndex]);

        packet = new UmdfPacket
        {
            Data = payload,
            Channel = channel,
            ReceivedTimestampTicks = Environment.TickCount64
        };

        if (reader.TryReadNext(out var next))
            _pq.Enqueue((next, item.ReaderIndex), next.TimestampMicros);

        return true;
    }

    /// <summary>
    /// Async wrapper for IPacketSource compatibility (e.g. tests, multicast bridge).
    /// Runs the sync path on the calling context.
    /// </summary>
    public ValueTask<UmdfPacket> ReceiveAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (TryReceive(out var packet))
            return new ValueTask<UmdfPacket>(packet);
        throw new System.Threading.Channels.ChannelClosedException();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        foreach (var (reader, _) in _readers)
            reader.Dispose();
    }
}
