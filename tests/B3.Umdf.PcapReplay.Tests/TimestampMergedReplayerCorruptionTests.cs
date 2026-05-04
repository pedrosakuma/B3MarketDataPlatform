using System.Buffers.Binary;
using B3.Umdf.PcapReplay;
using B3.Umdf.Transport;

namespace B3.Umdf.PcapReplay.Tests;

/// <summary>
/// Verifies that <see cref="TimestampMergedReplayer"/> tolerates malformed PCAP
/// frames by dropping them and continuing the merge.
/// </summary>
public class TimestampMergedReplayerCorruptionTests : IDisposable
{
    private readonly string _tempDir;

    public TimestampMergedReplayerCorruptionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "umdf-replay-corruption-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* ignore */ }
    }

    [Fact]
    public void TryComputeUdpPayloadOffset_TooShortFrame_ReturnsFalse()
    {
        Assert.False(UdpExtractor.TryComputeUdpPayloadOffset(new byte[10], 1, out int off));
        Assert.Equal(-1, off);
    }

    [Fact]
    public void TryComputeUdpPayloadOffset_UnsupportedLinkType_ReturnsFalse()
    {
        Assert.False(UdpExtractor.TryComputeUdpPayloadOffset(new byte[100], 99, out _));
    }

    [Fact]
    public void TryComputeUdpPayloadOffset_ValidEthernetFrame_ReturnsTrue()
    {
        byte[] frame = new byte[14 + 20 + 8];
        frame[14] = 0x45; // IPv4 IHL=5
        Assert.True(UdpExtractor.TryComputeUdpPayloadOffset(frame, 1, out int off));
        Assert.Equal(42, off);
    }

    [Fact]
    public void Replayer_MalformedFrameBetweenValidFrames_DropsAndContinuesMerge()
    {
        var path = Path.Combine(_tempDir, "mixed.pcap");
        using (var fs = File.Create(path))
        {
            WriteGlobalHeader(fs);
            WriteValidPacket(fs, 100, payloadMarker: 100);
            WriteRawRecord(fs, timestampMicros: 200, payload: new byte[8]); // way too short for L2+IP+UDP
            WriteValidPacket(fs, 300, payloadMarker: 300);
        }

        var sources = new List<PcapChannelSource>
        {
            new(path, ChannelType.IncrementalA, Group: 0),
        };

        using var replayer = new TimestampMergedReplayer(sources, new ReplayOptions { SpeedMultiplier = 0 });

        var seen = new List<long>();
        while (replayer.TryReceive(out var pkt))
        {
            seen.Add(BinaryPrimitives.ReadInt64LittleEndian(pkt.Data.Span));
        }

        Assert.Equal(new[] { 100L, 300L }, seen);
        Assert.Equal(1, replayer.MalformedPcapFrames);
    }

    [Fact]
    public void Replayer_LeadingMalformedFrames_SkipsToFirstValidAndCachesOffset()
    {
        var path = Path.Combine(_tempDir, "lead-bad.pcap");
        using (var fs = File.Create(path))
        {
            WriteGlobalHeader(fs);
            WriteRawRecord(fs, timestampMicros: 50, payload: new byte[5]);
            WriteRawRecord(fs, timestampMicros: 75, payload: new byte[10]);
            WriteValidPacket(fs, 100, payloadMarker: 100);
            WriteValidPacket(fs, 200, payloadMarker: 200);
        }

        var sources = new List<PcapChannelSource>
        {
            new(path, ChannelType.IncrementalA, Group: 0),
        };

        using var replayer = new TimestampMergedReplayer(sources, new ReplayOptions { SpeedMultiplier = 0 });

        var seen = new List<long>();
        while (replayer.TryReceive(out var pkt))
            seen.Add(BinaryPrimitives.ReadInt64LittleEndian(pkt.Data.Span));

        Assert.Equal(new[] { 100L, 200L }, seen);
        Assert.Equal(2, replayer.MalformedPcapFrames);
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

    private static void WriteValidPacket(Stream s, long timestampMicros, long payloadMarker)
    {
        const int frameLen = 14 + 20 + 8 + 16;
        Span<byte> rec = stackalloc byte[16 + frameLen];

        long secs = timestampMicros / 1_000_000;
        long usecs = timestampMicros % 1_000_000;
        BinaryPrimitives.WriteUInt32LittleEndian(rec[0..4], (uint)secs);
        BinaryPrimitives.WriteUInt32LittleEndian(rec[4..8], (uint)usecs);
        BinaryPrimitives.WriteUInt32LittleEndian(rec[8..12], (uint)frameLen);
        BinaryPrimitives.WriteUInt32LittleEndian(rec[12..16], (uint)frameLen);

        var frame = rec.Slice(16, frameLen);
        frame[14] = 0x45; // IPv4 IHL=5
        BinaryPrimitives.WriteInt64LittleEndian(frame.Slice(42, 8), payloadMarker);
        s.Write(rec);
    }

    private static void WriteRawRecord(Stream s, long timestampMicros, ReadOnlySpan<byte> payload)
    {
        Span<byte> hdr = stackalloc byte[16];
        long secs = timestampMicros / 1_000_000;
        long usecs = timestampMicros % 1_000_000;
        BinaryPrimitives.WriteUInt32LittleEndian(hdr[0..4], (uint)secs);
        BinaryPrimitives.WriteUInt32LittleEndian(hdr[4..8], (uint)usecs);
        BinaryPrimitives.WriteUInt32LittleEndian(hdr[8..12], (uint)payload.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(hdr[12..16], (uint)payload.Length);
        s.Write(hdr);
        s.Write(payload);
    }
}
