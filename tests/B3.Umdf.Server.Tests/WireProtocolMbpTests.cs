using System.Buffers.Binary;

namespace B3.Umdf.Server.Tests;

public class WireProtocolMbpTests
{
    private static (int length, MessageType type) ReadFraming(Span<byte> buf) =>
        ((int)BinaryPrimitives.ReadUInt32LittleEndian(buf), (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(buf[4..]));

    [Fact]
    public void WriteLevelUpdate_RoundTrip()
    {
        var buf = new byte[64];
        int len = WireProtocol.WriteLevelUpdate(buf, securityId: 42UL, side: 1, price: 1234567L, totalQty: 9876L, orderCount: 7);

        Assert.Equal(WireProtocol.LevelUpdateSize, len);
        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(len, msgLen);
        Assert.Equal(MessageType.LevelUpdate, type);

        Assert.Equal(42UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(8)));
        Assert.Equal(1, buf[36]);
        Assert.Equal(1234567L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(16)));
        Assert.Equal(9876L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(24)));
        Assert.Equal(7u, BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(32)));
    }

    [Fact]
    public void WriteLevelDeleted_RoundTrip()
    {
        var buf = new byte[64];
        int len = WireProtocol.WriteLevelDeleted(buf, securityId: 99UL, side: 0, price: -5L);

        Assert.Equal(WireProtocol.LevelDeletedSize, len);
        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(len, msgLen);
        Assert.Equal(MessageType.LevelDeleted, type);

        Assert.Equal(99UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(8)));
        Assert.Equal(0, buf[24]);
        Assert.Equal(-5L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(16)));
    }

    [Fact]
    public void WriteLevelSnapshot_HeaderAndEntries_RoundTrip()
    {
        const int bidCount = 2;
        const int askCount = 1;
        var buf = new byte[WireProtocol.LevelSnapshotSize(bidCount, askCount)];

        int offset = WireProtocol.WriteLevelSnapshotHeader(buf, securityId: 7UL, (ushort)bidCount, (ushort)askCount);
        offset = WireProtocol.WriteLevelSnapshotEntry(buf, offset, price: 1000, totalQty: 50, orderCount: 3);
        offset = WireProtocol.WriteLevelSnapshotEntry(buf, offset, price: 999, totalQty: 20, orderCount: 1);
        offset = WireProtocol.WriteLevelSnapshotEntry(buf, offset, price: 1010, totalQty: 100, orderCount: 4);
        Assert.Equal(buf.Length, offset);

        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(buf.Length, msgLen);
        Assert.Equal(MessageType.LevelSnapshot, type);
        Assert.Equal(7UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(8)));
        Assert.Equal((ushort)bidCount, BinaryPrimitives.ReadUInt16LittleEndian(buf.AsSpan(16)));
        Assert.Equal((ushort)askCount, BinaryPrimitives.ReadUInt16LittleEndian(buf.AsSpan(18)));

        // First bid entry begins at offset 20.
        Assert.Equal(1000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(20)));
        Assert.Equal(50L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(28)));
        Assert.Equal(3u, BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(36)));
    }

    [Fact]
    public void WriteLevelSnapshot_EmptyBook_OnlyHeader()
    {
        var buf = new byte[WireProtocol.LevelSnapshotSize(0, 0)];
        int offset = WireProtocol.WriteLevelSnapshotHeader(buf, securityId: 1UL, 0, 0);
        Assert.Equal(buf.Length, offset);
        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(buf.Length, msgLen);
        Assert.Equal(MessageType.LevelSnapshot, type);
        Assert.Equal(0, BinaryPrimitives.ReadUInt16LittleEndian(buf.AsSpan(16)));
        Assert.Equal(0, BinaryPrimitives.ReadUInt16LittleEndian(buf.AsSpan(18)));
    }

    [Fact]
    public void DataFlags_MbpBitMatchesProtocol()
    {
        Assert.Equal((DataFlags)0x08, DataFlags.Mbp);
        Assert.True((DataFlags.AllKnown & DataFlags.Mbp) == DataFlags.Mbp);
        Assert.True((DataFlags.Book & DataFlags.Mbp) == 0);
    }
}
