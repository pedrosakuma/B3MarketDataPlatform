using System.Diagnostics;
using System.Runtime.InteropServices;
using B3.Umdf.PcapReplay;
using B3.Umdf.Transport;
using B3.Umdf.Feed;

namespace B3.Umdf.Tools.InboundLatencyProbe;

/// <summary>
/// Streams a single PCAP, paces packets according to the original capture
/// timestamps (scaled by <c>speed</c>), tags each packet with the live
/// Stopwatch.GetTimestamp() at push time, and submits to the supplied
/// MultiFeedManager via PushPacket. Threads are independent across multiple
/// PCAPs but share a common wall-clock origin so cross-channel timing is
/// consistent.
/// </summary>
internal sealed class PcapReplayProducer
{
    private readonly string _path;
    private readonly ChannelType _channel;
    private readonly int _channelGroup;
    private readonly double _speed;
    private readonly long _runStartTicks;
    private readonly long _firstPcapMicros;        // anchor (earliest across all PCAPs)
    private readonly MultiFeedManager _manager;
    private readonly long _ticksPerSecond;

    public long PacketsPushed;

    public PcapReplayProducer(
        string path,
        ChannelType channel,
        int channelGroup,
        double speed,
        long runStartTicks,
        long firstPcapMicros,
        MultiFeedManager manager)
    {
        _path = path;
        _channel = channel;
        _channelGroup = channelGroup;
        _speed = speed;
        _runStartTicks = runStartTicks;
        _firstPcapMicros = firstPcapMicros;
        _manager = manager;
        _ticksPerSecond = Stopwatch.Frequency;
    }

    public void Run(CancellationToken ct)
    {
        const int packetHeaderSize = 16;
        using var reader = new MmapPcapReader(_path);
        int udpOffset = -1;

        while (!ct.IsCancellationRequested && reader.TryReadNext(out var pkt))
        {
            var frame = pkt.Data.Span;
            if (udpOffset < 0)
                udpOffset = UdpExtractor.ComputeUdpPayloadOffset(frame, reader.LinkType);
            if (udpOffset >= frame.Length) continue;
            var payload = frame[udpOffset..];
            if (payload.Length < packetHeaderSize) continue;

            // Pacing: target time relative to run start = (pcap_offset / speed).
            long pcapOffsetMicros = pkt.TimestampMicros - _firstPcapMicros;
            long targetTicks = _runStartTicks + (long)(pcapOffsetMicros * _ticksPerSecond / 1_000_000.0 / _speed);

            long now = Stopwatch.GetTimestamp();
            long delta = targetTicks - now;
            if (delta > 0)
            {
                // Coarse sleep for >1ms, spin-wait for the tail. Avoids burning
                // a CPU when the feed is idle but keeps sub-ms pacing precise.
                long oneMsTicks = _ticksPerSecond / 1000;
                if (delta > oneMsTicks * 2)
                {
                    int sleepMs = (int)((delta - oneMsTicks) * 1000 / _ticksPerSecond);
                    if (sleepMs > 0) Thread.Sleep(sleepMs);
                }
                while ((targetTicks - Stopwatch.GetTimestamp()) > 0)
                {
                    if (ct.IsCancellationRequested) return;
                    Thread.SpinWait(64);
                }
            }
            // If delta is negative we're already late; deliver immediately
            // (mirrors what would happen under real overload).

            // Copy payload into a fresh buffer so the PCAP mmap can advance.
            // ArrayPool would be lower-allocation; for v1 we accept the alloc
            // cost — it's identical for all strategies under comparison.
            var buf = new byte[payload.Length];
            payload.CopyTo(buf);

            var umdf = new UmdfPacket
            {
                Data = buf,
                Channel = _channel,
                ChannelGroup = _channelGroup,
                ReceivedTimestampTicks = Stopwatch.GetTimestamp(),
            };

            _manager.PushPacket(in umdf);
            PacketsPushed++;
        }
    }
}
