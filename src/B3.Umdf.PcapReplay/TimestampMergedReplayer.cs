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
    private readonly List<(MmapPcapReader Reader, ChannelType Channel, int Group)> _readers = new();
    private readonly int[] _cachedUdpOffset;
    private readonly long[] _groupTimeOffset;
    private readonly ReplayOptions _options;
    private readonly LossInjector? _loss;
    private long? _firstTimestampMicros;
    private readonly System.Diagnostics.Stopwatch _stopwatch = System.Diagnostics.Stopwatch.StartNew();
    private bool _disposed;
    private long _droppedPackets;

    /// <summary>Number of packets suppressed by the loss policy. 0 when no policy.</summary>
    public long DroppedPackets => System.Threading.Volatile.Read(ref _droppedPackets);

    public TimestampMergedReplayer(IReadOnlyList<PcapChannelSource> sources, ReplayOptions? options = null)
    {
        _options = options ?? new ReplayOptions();
        _loss = _options.Loss is { } p ? new LossInjector(p) : null;
        _cachedUdpOffset = new int[sources.Count];
        _groupTimeOffset = new long[sources.Count];

        // First pass: read first packet from each reader and find min timestamp per group
        var firstPackets = new PcapPacket?[sources.Count];
        var groupMinTimestamp = new Dictionary<int, long>();

        for (int i = 0; i < sources.Count; i++)
        {
            var src = sources[i];
            var reader = new MmapPcapReader(src.FilePath);
            _readers.Add((reader, src.Channel, src.Group));
            if (reader.TryReadNext(out var pkt))
            {
                _cachedUdpOffset[i] = UdpExtractor.ComputeUdpPayloadOffset(pkt.Data.Span, reader.LinkType);
                firstPackets[i] = pkt;
                if (!groupMinTimestamp.TryGetValue(src.Group, out var existing) || pkt.TimestampMicros < existing)
                    groupMinTimestamp[src.Group] = pkt.TimestampMicros;
            }
        }

        // Compute per-group offset so all groups start at the same logical time
        long globalMin = groupMinTimestamp.Count > 0 ? groupMinTimestamp.Values.Min() : 0;
        for (int i = 0; i < sources.Count; i++)
        {
            var group = sources[i].Group;
            _groupTimeOffset[i] = groupMinTimestamp.TryGetValue(group, out var gmin) ? gmin - globalMin : 0;

            if (firstPackets[i] is { } pkt)
                _pq.Enqueue((pkt, i), pkt.TimestampMicros - _groupTimeOffset[i]);
        }
    }

    /// <summary>
    /// Synchronous receive — no async overhead, no Channel&lt;T&gt;, no thread pool.
    /// With mmap, the returned packet's Data points directly into the mmap'd region
    /// and remains valid for the lifetime of the replayer.
    /// </summary>
    public bool TryReceive(out UmdfPacket packet)
    {
        // Loop so that a dropped packet immediately advances to the next without
        // returning false (which the caller interprets as end-of-stream).
        while (true)
        {
            if (!_pq.TryDequeue(out var item, out long normalizedTimestamp))
            {
                packet = default;
                return false;
            }

            if (_options.SpeedMultiplier > 0)
            {
                _firstTimestampMicros ??= normalizedTimestamp;
                // Sub-millisecond pacing: target elapsed time using Stopwatch ticks (typically 100 ns resolution).
                // Coarse Thread.Sleep is replaced by a 3-tier wait that preserves microbursts of inter-packet gaps:
                //   - long delay (>= 2 ms): Thread.Sleep((int)(delayMs - 1)) for OS-scheduled wait
                //   - medium delay (>= 50 us): Thread.SpinWait + Yield to avoid kernel scheduling jitter
                //   - tiny delay: tight Thread.SpinWait (sub-microsecond)
                double targetElapsedMicros = (normalizedTimestamp - _firstTimestampMicros.Value) / _options.SpeedMultiplier;
                long targetElapsedTicks = (long)(targetElapsedMicros * System.Diagnostics.Stopwatch.Frequency / 1_000_000.0);
                long delayTicks = targetElapsedTicks - _stopwatch.ElapsedTicks;

                if (delayTicks > 0)
                {
                    long ticksPerMs = System.Diagnostics.Stopwatch.Frequency / 1000;
                    if (delayTicks > ticksPerMs * 2)
                    {
                        int sleepMs = (int)(delayTicks / ticksPerMs) - 1;
                        if (sleepMs > 0) Thread.Sleep(sleepMs);
                    }
                    while (_stopwatch.ElapsedTicks < targetElapsedTicks)
                    {
                        long remaining = targetElapsedTicks - _stopwatch.ElapsedTicks;
                        if (remaining <= 0) break;
                        if (remaining > ticksPerMs / 20) Thread.Yield();
                        else Thread.SpinWait(20);
                    }
                }
            }

            var (reader, channel, group) = _readers[item.ReaderIndex];
            var payload = item.Packet.Data.Slice(_cachedUdpOffset[item.ReaderIndex]);

            // Always advance the reader regardless of drop decision so the
            // priority queue stays full and per-reader ordering is preserved.
            if (reader.TryReadNext(out var next))
                _pq.Enqueue((next, item.ReaderIndex), next.TimestampMicros - _groupTimeOffset[item.ReaderIndex]);

            if (_loss is not null && _loss.ShouldDrop(channel, payload.Span))
            {
                System.Threading.Interlocked.Increment(ref _droppedPackets);
                continue; // simulate UDP loss: skip without delivering
            }

            packet = new UmdfPacket
            {
                Data = payload,
                Channel = channel,
                ChannelGroup = group,
                ReceivedTimestampTicks = Environment.TickCount64
            };
            return true;
        }
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
        foreach (var (reader, _, _) in _readers)
            reader.Dispose();
    }
}
