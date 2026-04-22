using System.Buffers.Binary;
using B3.Umdf.PcapReplay;
using B3.Umdf.Transport;

namespace B3.Umdf.PcapReplay.Tests;

/// <summary>
/// End-to-end ordering tests for <see cref="TimestampMergedReplayer"/>.
/// We write tiny synthetic PCAP files to a temp directory, then verify the
/// replayer drains packets in monotonic timestamp order across multiple
/// channels and channel groups.
/// </summary>
public class TimestampMergedReplayerTests : IDisposable
{
    private readonly string _tempDir;

    public TimestampMergedReplayerTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "umdf-replay-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* ignore */ }
    }

    [Fact]
    public void Replayer_DrainsTwoChannels_InMonotonicTimestampOrder()
    {
        // Channel A timestamps (microseconds since epoch): 100, 300, 500
        // Channel B timestamps:                            200, 400
        // Expected drain order (by ts):                    100A, 200B, 300A, 400B, 500A
        var pathA = WritePcap("a.pcap", new long[] { 100, 300, 500 });
        var pathB = WritePcap("b.pcap", new long[] { 200, 400 });

        var sources = new List<PcapChannelSource>
        {
            new(pathA, ChannelType.IncrementalA, Group: 0),
            new(pathB, ChannelType.IncrementalB, Group: 0),
        };

        using var replayer = new TimestampMergedReplayer(sources, new ReplayOptions { SpeedMultiplier = 0 });

        var seen = new List<(long Ts, ChannelType Ch)>();
        while (replayer.TryReceive(out var pkt))
        {
            // We encoded the timestamp in the first 8 bytes of the synthetic UDP payload.
            long ts = BinaryPrimitives.ReadInt64LittleEndian(pkt.Data.Span);
            seen.Add((ts, pkt.Channel));
        }

        Assert.Equal(5, seen.Count);
        Assert.Equal(new[] { 100L, 200L, 300L, 400L, 500L }, seen.Select(s => s.Ts).ToArray());
        Assert.Equal(
            new[] { ChannelType.IncrementalA, ChannelType.IncrementalB, ChannelType.IncrementalA, ChannelType.IncrementalB, ChannelType.IncrementalA },
            seen.Select(s => s.Ch).ToArray());
    }

    [Fact]
    public void Replayer_AlignsGroupsToCommonStart_PreservesIntraGroupOrder()
    {
        // Group 0 starts at ts=1000; Group 1 starts at ts=5000.
        // After per-group offset alignment both groups start at the same logical t=0;
        // intra-group ordering must be preserved and the two groups must be interleaved
        // by their normalised timestamps.
        var g0a = WritePcap("g0a.pcap", new long[] { 1000, 1002 });
        var g1a = WritePcap("g1a.pcap", new long[] { 5000, 5001, 5003 });

        var sources = new List<PcapChannelSource>
        {
            new(g0a, ChannelType.IncrementalA, Group: 0),
            new(g1a, ChannelType.IncrementalA, Group: 1),
        };

        using var replayer = new TimestampMergedReplayer(sources, new ReplayOptions { SpeedMultiplier = 0 });

        var seen = new List<(int Group, long RawTs)>();
        while (replayer.TryReceive(out var pkt))
        {
            long rawTs = BinaryPrimitives.ReadInt64LittleEndian(pkt.Data.Span);
            seen.Add((pkt.ChannelGroup, rawTs));
        }

        Assert.Equal(5, seen.Count);
        // Both groups were drained.
        Assert.Contains(seen, s => s.Group == 0);
        Assert.Contains(seen, s => s.Group == 1);
        // Intra-group order preserved.
        var g0Order = seen.Where(s => s.Group == 0).Select(s => s.RawTs).ToArray();
        var g1Order = seen.Where(s => s.Group == 1).Select(s => s.RawTs).ToArray();
        Assert.Equal(new[] { 1000L, 1002L }, g0Order);
        Assert.Equal(new[] { 5000L, 5001L, 5003L }, g1Order);
    }

    // ── PCAP writer helper ──

    private string WritePcap(string name, long[] timestampMicros)
    {
        var path = Path.Combine(_tempDir, name);
        using var fs = File.Create(path);
        WriteGlobalHeader(fs);
        foreach (var ts in timestampMicros)
            WritePacket(fs, ts);
        return path;
    }

    private static void WriteGlobalHeader(Stream s)
    {
        Span<byte> hdr = stackalloc byte[24];
        BinaryPrimitives.WriteUInt32LittleEndian(hdr[..4], 0xa1b2c3d4u); // microsecond magic, no swap
        BinaryPrimitives.WriteUInt16LittleEndian(hdr[4..6], 2);          // version major
        BinaryPrimitives.WriteUInt16LittleEndian(hdr[6..8], 4);          // version minor
        // thiszone=0 (8..12), sigfigs=0 (12..16) — already zero
        BinaryPrimitives.WriteUInt32LittleEndian(hdr[16..20], 65535);    // snaplen
        BinaryPrimitives.WriteUInt32LittleEndian(hdr[20..24], 1);        // linktype = Ethernet
        s.Write(hdr);
    }

    private static void WritePacket(Stream s, long timestampMicros)
    {
        // Frame: Ethernet (14) + IPv4 (20) + UDP (8) + 16B payload (8B ts marker + 8B padding).
        const int frameLen = 14 + 20 + 8 + 16;
        Span<byte> rec = stackalloc byte[16 + frameLen];

        long secs = timestampMicros / 1_000_000;
        long usecs = timestampMicros % 1_000_000;
        BinaryPrimitives.WriteUInt32LittleEndian(rec[0..4], (uint)secs);
        BinaryPrimitives.WriteUInt32LittleEndian(rec[4..8], (uint)usecs);
        BinaryPrimitives.WriteUInt32LittleEndian(rec[8..12], (uint)frameLen);  // captured len
        BinaryPrimitives.WriteUInt32LittleEndian(rec[12..16], (uint)frameLen); // original len

        var frame = rec.Slice(16, frameLen);
        // Ethernet header (zeros); IPv4 IHL=5
        frame[14] = 0x45;
        // UDP at 34, payload at 42 — write the timestamp into the first 8 bytes of payload.
        BinaryPrimitives.WriteInt64LittleEndian(frame.Slice(42, 8), timestampMicros);

        s.Write(rec);
    }
}
