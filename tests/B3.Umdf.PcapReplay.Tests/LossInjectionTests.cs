using System.Buffers.Binary;
using B3.Umdf.PcapReplay;
using B3.Umdf.Transport;

namespace B3.Umdf.PcapReplay.Tests;

/// <summary>
/// Tests the loss-injection policy of <see cref="TimestampMergedReplayer"/>.
/// We write synthetic PCAPs with deterministic SeqNums in the SBE PacketHeader,
/// then verify drop counts and per-channel/correlated semantics.
/// </summary>
public class LossInjectionTests : IDisposable
{
    private readonly string _tempDir;

    public LossInjectionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "umdf-loss-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* ignore */ }
    }

    [Fact]
    public void Loss_RandomRate_DropsApproximatelyExpectedFraction()
    {
        const int n = 1000;
        var path = WritePcap("a.pcap", Enumerable.Range(1, n).Select(i => (long)i).ToArray());
        var sources = new List<PcapChannelSource>
        {
            new(path, ChannelType.IncrementalA, Group: 0),
        };
        var loss = new LossPolicy(LossTargets.IncrementalA, LossMode.Random, Rate: 0.30, Seed: 42);
        using var replayer = new TimestampMergedReplayer(sources, new ReplayOptions { SpeedMultiplier = 0, Loss = loss });

        int delivered = 0;
        while (replayer.TryReceive(out _)) delivered++;

        // 30% target ± 5% tolerance for n=1000
        int expectedDelivered = (int)(n * 0.70);
        Assert.InRange(delivered, expectedDelivered - 50, expectedDelivered + 50);
        Assert.Equal(n - delivered, replayer.DroppedPackets);
    }

    [Fact]
    public void Loss_TargetsAOnly_LeavesBIntact()
    {
        var pathA = WritePcap("a.pcap", Enumerable.Range(1, 200).Select(i => (long)(i * 2)).ToArray());
        var pathB = WritePcap("b.pcap", Enumerable.Range(1, 200).Select(i => (long)(i * 2 + 1)).ToArray());
        var sources = new List<PcapChannelSource>
        {
            new(pathA, ChannelType.IncrementalA, Group: 0),
            new(pathB, ChannelType.IncrementalB, Group: 0),
        };
        var loss = new LossPolicy(LossTargets.IncrementalA, LossMode.Random, Rate: 0.50, Seed: 7);
        using var replayer = new TimestampMergedReplayer(sources, new ReplayOptions { SpeedMultiplier = 0, Loss = loss });

        int aDelivered = 0, bDelivered = 0;
        while (replayer.TryReceive(out var pkt))
        {
            if (pkt.Channel == ChannelType.IncrementalA) aDelivered++;
            else if (pkt.Channel == ChannelType.IncrementalB) bDelivered++;
        }

        Assert.Equal(200, bDelivered);             // B untouched
        Assert.InRange(aDelivered, 80, 120);        // A around 50%
    }

    [Fact]
    public void Loss_BurstMode_DropsConsecutivePackets()
    {
        var path = WritePcap("a.pcap", Enumerable.Range(1, 500).Select(i => (long)i).ToArray());
        var sources = new List<PcapChannelSource>
        {
            new(path, ChannelType.IncrementalA, Group: 0),
        };
        // Low trigger rate but burst of 10. Total drops should be ~ trigger_count * 10.
        var loss = new LossPolicy(LossTargets.IncrementalA, LossMode.Burst, Rate: 0.02, BurstSize: 10, Seed: 13);
        using var replayer = new TimestampMergedReplayer(sources, new ReplayOptions { SpeedMultiplier = 0, Loss = loss });

        var deliveredSeqs = new List<uint>();
        while (replayer.TryReceive(out var pkt))
        {
            uint seq = BinaryPrimitives.ReadUInt32LittleEndian(pkt.Data.Span.Slice(8, 4));
            deliveredSeqs.Add(seq);
        }

        // Find largest gap between consecutive delivered seqs — should be >= burst size at least once.
        int maxGap = 0;
        for (int i = 1; i < deliveredSeqs.Count; i++)
            maxGap = Math.Max(maxGap, (int)(deliveredSeqs[i] - deliveredSeqs[i - 1]));
        Assert.True(maxGap >= 10, $"Expected at least one burst-sized gap, max observed = {maxGap}");
    }

    [Fact]
    public void Loss_Correlated_DropsSameSeqOnAandB()
    {
        // Both feeds carry seqs 1..200 with identical SeqNums in the header.
        var seqs = Enumerable.Range(1, 200).Select(i => (long)i).ToArray();
        var pathA = WritePcap("a.pcap", seqs);
        var pathB = WritePcap("b.pcap", seqs);
        var sources = new List<PcapChannelSource>
        {
            new(pathA, ChannelType.IncrementalA, Group: 0),
            new(pathB, ChannelType.IncrementalB, Group: 0),
        };
        var loss = new LossPolicy(LossTargets.Incrementals, LossMode.Random, Rate: 0.20, Correlated: true, Seed: 99);
        using var replayer = new TimestampMergedReplayer(sources, new ReplayOptions { SpeedMultiplier = 0, Loss = loss });

        var aSeqs = new HashSet<uint>();
        var bSeqs = new HashSet<uint>();
        while (replayer.TryReceive(out var pkt))
        {
            uint seq = BinaryPrimitives.ReadUInt32LittleEndian(pkt.Data.Span.Slice(8, 4));
            if (pkt.Channel == ChannelType.IncrementalA) aSeqs.Add(seq);
            else bSeqs.Add(seq);
        }

        // For every SeqNum, A and B must agree on whether it was delivered or dropped.
        var allSeqs = aSeqs.Union(bSeqs).ToHashSet();
        foreach (var seq in Enumerable.Range(1, 200).Select(i => (uint)i))
        {
            bool inA = aSeqs.Contains(seq);
            bool inB = bSeqs.Contains(seq);
            Assert.True(inA == inB, $"Seq {seq} delivery diverged: A={inA} B={inB}");
        }
        // Some non-trivial number was dropped.
        Assert.True(aSeqs.Count < 200);
    }

    [Fact]
    public void Loss_Disabled_DeliversEverything()
    {
        var path = WritePcap("a.pcap", Enumerable.Range(1, 50).Select(i => (long)i).ToArray());
        var sources = new List<PcapChannelSource>
        {
            new(path, ChannelType.IncrementalA, Group: 0),
        };
        using var replayer = new TimestampMergedReplayer(sources, new ReplayOptions { SpeedMultiplier = 0, Loss = null });

        int n = 0;
        while (replayer.TryReceive(out _)) n++;
        Assert.Equal(50, n);
        Assert.Equal(0, replayer.DroppedPackets);
    }

    // ── PCAP writer helpers (with valid SBE PacketHeader so SeqNum can be parsed) ──

    private string WritePcap(string name, long[] sequenceNumbers)
    {
        var path = Path.Combine(_tempDir, name);
        using var fs = File.Create(path);
        WriteGlobalHeader(fs);
        long ts = 1000;
        foreach (var seq in sequenceNumbers)
        {
            WritePacket(fs, ts, (uint)seq);
            ts += 10;
        }
        return path;
    }

    private static void WriteGlobalHeader(Stream s)
    {
        Span<byte> hdr = stackalloc byte[24];
        BinaryPrimitives.WriteUInt32LittleEndian(hdr[..4], 0xa1b2c3d4u);
        BinaryPrimitives.WriteUInt16LittleEndian(hdr[4..6], 2);
        BinaryPrimitives.WriteUInt16LittleEndian(hdr[6..8], 4);
        BinaryPrimitives.WriteUInt32LittleEndian(hdr[16..20], 65535);
        BinaryPrimitives.WriteUInt32LittleEndian(hdr[20..24], 1);
        s.Write(hdr);
    }

    private static void WritePacket(Stream s, long timestampMicros, uint seqNum)
    {
        // Layout: Ethernet(14) + IPv4(20) + UDP(8) + UMDF PacketHeader(16, populated)
        // PacketHeader: ChannelNumber(1) + Reserved(1) + SequenceVersion(2) + SequenceNumber(4) + SendingTime(8)
        const int payloadLen = 16;
        const int frameLen = 14 + 20 + 8 + payloadLen;
        Span<byte> rec = stackalloc byte[16 + frameLen];

        long secs = timestampMicros / 1_000_000;
        long usecs = timestampMicros % 1_000_000;
        BinaryPrimitives.WriteUInt32LittleEndian(rec[0..4], (uint)secs);
        BinaryPrimitives.WriteUInt32LittleEndian(rec[4..8], (uint)usecs);
        BinaryPrimitives.WriteUInt32LittleEndian(rec[8..12], (uint)frameLen);
        BinaryPrimitives.WriteUInt32LittleEndian(rec[12..16], (uint)frameLen);

        var frame = rec.Slice(16, frameLen);
        frame[14] = 0x45;                  // IPv4 IHL=5
        var udpPayload = frame.Slice(42, payloadLen);
        // PacketHeader bytes: SeqNum at offset 4..8 (after ChannelNumber+Reserved+SequenceVersion).
        udpPayload[0] = 0;                                       // ChannelNumber
        udpPayload[1] = 0;                                       // Reserved
        BinaryPrimitives.WriteUInt16LittleEndian(udpPayload.Slice(2, 2), 0); // SequenceVersion
        BinaryPrimitives.WriteUInt32LittleEndian(udpPayload.Slice(4, 4), seqNum);
        BinaryPrimitives.WriteInt64LittleEndian(udpPayload.Slice(8, 8), timestampMicros);
        s.Write(rec);
    }
}
