using System.Threading.Channels;
using B3.Umdf.Transport;

namespace B3.Umdf.PcapReplay;

public sealed class TimestampMergedReplayer : IPacketSource
{
    private readonly Channel<UmdfPacket> _output;
    private readonly Task _replayTask;

    public TimestampMergedReplayer(IReadOnlyList<PcapChannelSource> sources, ReplayOptions? options = null)
    {
        options ??= new ReplayOptions();
        _output = Channel.CreateBounded<UmdfPacket>(new BoundedChannelOptions(4096)
        {
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });
        _replayTask = Task.Run(() => ReplayLoop(sources, options));
    }

    public async ValueTask<UmdfPacket> ReceiveAsync(CancellationToken ct = default)
    {
        return await _output.Reader.ReadAsync(ct);
    }

    private async Task ReplayLoop(IReadOnlyList<PcapChannelSource> sources, ReplayOptions options)
    {
        var readers = new List<(PcapReader Reader, ChannelType Channel, uint LinkType)>();
        try
        {
            var pq = new PriorityQueue<(PcapPacket Packet, int ReaderIndex), long>();

            for (int i = 0; i < sources.Count; i++)
            {
                var src = sources[i];
                var reader = new PcapReader(src.FilePath);
                readers.Add((reader, src.Channel, reader.LinkType));
                if (reader.TryReadNext(out var pkt))
                    pq.Enqueue((pkt, i), pkt.TimestampMicros);
            }

            long? firstTimestamp = null;
            long startTicks = Environment.TickCount64;

            while (pq.TryDequeue(out var item, out long timestampMicros))
            {
                if (options.SpeedMultiplier > 0)
                {
                    firstTimestamp ??= timestampMicros;
                    long elapsedTargetMs = (long)((timestampMicros - firstTimestamp.Value) / 1000.0 / options.SpeedMultiplier);
                    long elapsedActualMs = Environment.TickCount64 - startTicks;
                    long delayMs = elapsedTargetMs - elapsedActualMs;
                    if (delayMs > 1)
                        await Task.Delay((int)delayMs);
                }

                var (reader, channel, linkType) = readers[item.ReaderIndex];
                var payload = UdpExtractor.ExtractUdpPayload(item.Packet.Data, linkType);

                var packet = new UmdfPacket
                {
                    Data = payload,
                    Channel = channel,
                    ReceivedTimestampTicks = Environment.TickCount64
                };

                await _output.Writer.WriteAsync(packet);

                if (reader.TryReadNext(out var next))
                    pq.Enqueue((next, item.ReaderIndex), next.TimestampMicros);
            }
        }
        finally
        {
            _output.Writer.Complete();
            foreach (var (reader, _, _) in readers)
                reader.Dispose();
        }
    }

    public void Dispose()
    {
        _output.Writer.TryComplete();
        _replayTask.GetAwaiter().GetResult();
    }
}
