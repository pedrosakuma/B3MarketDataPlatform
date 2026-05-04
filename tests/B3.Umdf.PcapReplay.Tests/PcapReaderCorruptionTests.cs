using System.Buffers.Binary;
using B3.Umdf.PcapReplay;

namespace B3.Umdf.PcapReplay.Tests;

/// <summary>
/// Corruption-tolerance tests for <see cref="PcapReader"/>. Verifies that
/// truncated and oversized records are surfaced as observable counters
/// instead of bubbling up as exceptions or OOM-causing allocations.
/// </summary>
public class PcapReaderCorruptionTests
{
    [Fact]
    public void TryReadNext_TruncatedRecordPayloadAtEof_ReturnsFalseAndIncrementsCounter()
    {
        using var ms = new MemoryStream();
        WriteGlobalHeader(ms);
        // Write a record header claiming a 100-byte payload, then only supply 40 bytes.
        WriteRecordHeader(ms, timestampMicros: 1, inclLen: 100);
        ms.Write(new byte[40]);
        ms.Position = 0;

        using var reader = new PcapReader(ms);
        Assert.False(reader.TryReadNext(out _));
        Assert.Equal(1, reader.TruncatedPcapRecords);
        Assert.Equal(0, reader.MalformedPcapRecords);
    }

    [Fact]
    public void TryReadNext_TruncatedRecordHeaderAtEof_ReturnsFalseAndIncrementsCounter()
    {
        using var ms = new MemoryStream();
        WriteGlobalHeader(ms);
        // Only 8 bytes of a 16-byte record header.
        ms.Write(new byte[8]);
        ms.Position = 0;

        using var reader = new PcapReader(ms);
        Assert.False(reader.TryReadNext(out _));
        Assert.Equal(1, reader.TruncatedPcapRecords);
    }

    [Fact]
    public void TryReadNext_CleanEofOnRecordBoundary_ReturnsFalseWithoutIncrementingCounters()
    {
        using var ms = new MemoryStream();
        WriteGlobalHeader(ms);
        ms.Position = 0;

        using var reader = new PcapReader(ms);
        Assert.False(reader.TryReadNext(out _));
        Assert.Equal(0, reader.TruncatedPcapRecords);
        Assert.Equal(0, reader.MalformedPcapRecords);
    }

    [Fact]
    public void TryReadNext_OversizedInclLen_DropsRecordAndIncrementsCounter()
    {
        using var ms = new MemoryStream();
        WriteGlobalHeader(ms);
        // inclLen = 1 MiB, well above MaxPcapRecordBytes (65535).
        WriteRecordHeader(ms, timestampMicros: 1, inclLen: 1024 * 1024);
        ms.Position = 0;

        using var reader = new PcapReader(ms);
        Assert.False(reader.TryReadNext(out _));
        Assert.Equal(1, reader.MalformedPcapRecords);
        Assert.Equal(0, reader.TruncatedPcapRecords);
    }

    [Fact]
    public void TryReadNext_GoodRecordThenTruncated_ReturnsFirstThenStops()
    {
        using var ms = new MemoryStream();
        WriteGlobalHeader(ms);
        WriteRecordHeader(ms, timestampMicros: 100, inclLen: 16);
        ms.Write(new byte[16]);
        WriteRecordHeader(ms, timestampMicros: 200, inclLen: 32);
        ms.Write(new byte[4]);
        ms.Position = 0;

        using var reader = new PcapReader(ms);
        Assert.True(reader.TryReadNext(out var first));
        Assert.Equal(100, first.TimestampMicros);

        Assert.False(reader.TryReadNext(out _));
        Assert.Equal(1, reader.TruncatedPcapRecords);
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

    private static void WriteRecordHeader(Stream s, long timestampMicros, uint inclLen)
    {
        Span<byte> rec = stackalloc byte[16];
        long secs = timestampMicros / 1_000_000;
        long usecs = timestampMicros % 1_000_000;
        BinaryPrimitives.WriteUInt32LittleEndian(rec[0..4], (uint)secs);
        BinaryPrimitives.WriteUInt32LittleEndian(rec[4..8], (uint)usecs);
        BinaryPrimitives.WriteUInt32LittleEndian(rec[8..12], inclLen);
        BinaryPrimitives.WriteUInt32LittleEndian(rec[12..16], inclLen);
        s.Write(rec);
    }
}
