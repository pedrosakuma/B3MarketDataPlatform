using System.Buffers.Binary;
using System.Text;
using B3.Umdf.Book;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Wire-format tests for the Auction channel (issue #57). Pins the
/// single-bitmask layout, bit-position contract, and MessageType code so
/// changes that would silently break the SDK decoder fail loudly here first.
/// </summary>
public class WireProtocolAuctionTests
{
    private static (ushort length, MessageType type) ReadFraming(Span<byte> buf) =>
        (BinaryPrimitives.ReadUInt16LittleEndian(buf), (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(buf[2..]));

    [Fact]
    public void WriteAuction_MessageTypeIs0x00B2()
    {
        var buf = new byte[WireProtocol.AuctionMaxSize];
        var info = new InstrumentInfo { Symbol = "PETR4", AuctionTimestamp = 1 };
        WireProtocol.WriteAuction(buf, securityId: 1, info);
        var (_, type) = ReadFraming(buf);
        Assert.Equal(MessageType.Auction, type);
        Assert.Equal((ushort)0x00B2, (ushort)type);
    }

    [Fact]
    public void WriteAuction_OnlySymbol_MaskIsZero()
    {
        var buf = new byte[WireProtocol.AuctionMaxSize];
        var info = new InstrumentInfo { Symbol = "PETR4" };
        int len = WireProtocol.WriteAuction(buf, securityId: 99, info);

        var (msgLen, _) = ReadFraming(buf);
        Assert.Equal(len, msgLen);

        int off = WireProtocol.FramingHeaderSize;
        Assert.Equal(99UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(off))); off += 8;
        int symLen = buf[off++];
        Assert.Equal(5, symLen);
        Assert.Equal("PETR4", Encoding.UTF8.GetString(buf.AsSpan(off, symLen))); off += symLen;
        Assert.Equal(0, buf[off]); off += 1;
        Assert.Equal(len, off);
    }

    [Fact]
    public void WriteAuction_ImbalanceAndPhase_MaskAndValuesCorrect()
    {
        var buf = new byte[WireProtocol.AuctionMaxSize];
        var info = new InstrumentInfo
        {
            Symbol = "VALE3",
            AuctionImbalanceSize = 50_000,
            AuctionImbalanceCondition = 0x0100, // MoreBuyers
            TradingStatus = 2,           // Pre-Open
            TradSesOpenTime = 1_700_000_000_000_000_000UL,
            AuctionTimestamp = 1_700_000_000_000_001_000L,
        };
        int len = WireProtocol.WriteAuction(buf, securityId: 42, info);

        int off = WireProtocol.FramingHeaderSize;
        Assert.Equal(42UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(off))); off += 8;
        int symLen = buf[off++]; Assert.Equal(5, symLen);
        Assert.Equal("VALE3", Encoding.UTF8.GetString(buf.AsSpan(off, symLen))); off += symLen;

        byte mask = buf[off++];
        byte expected =
            (1 << WireProtocol.AuctionFieldImbalanceQty) |
            (1 << WireProtocol.AuctionFieldImbalanceCondition) |
            (1 << WireProtocol.AuctionFieldTradingStatus) |
            (1 << WireProtocol.AuctionFieldTradSesOpenTime) |
            (1 << WireProtocol.AuctionFieldAsOfTimestampNanos);
        Assert.Equal(expected, mask);

        // Slots are written in bit order
        Assert.Equal(50_000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(0x0100L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(2L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(1_700_000_000_000_000_000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(1_700_000_000_000_001_000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(len, off);
    }

    [Fact]
    public void WriteAuction_RptSeq_OnlyWhenNonZero()
    {
        var buf = new byte[WireProtocol.AuctionMaxSize];
        var info = new InstrumentInfo
        {
            Symbol = "ABEV3",
            AuctionImbalanceSize = 10_000,
            LastRptSeqAuctionImbalance = 12345,
        };
        WireProtocol.WriteAuction(buf, securityId: 7, info);

        int off = WireProtocol.FramingHeaderSize + 8 + 1 + 5;
        byte mask = buf[off++];
        Assert.NotEqual(0, mask & (1 << WireProtocol.AuctionFieldRptSeq));
        Assert.NotEqual(0, mask & (1 << WireProtocol.AuctionFieldImbalanceQty));

        // Slots: ImbalanceQty first (bit 0), RptSeq last (bit 5).
        Assert.Equal(10_000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(12345L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off)));
    }

    [Fact]
    public void WriteAuction_ImbalanceMoreSellers_ConditionPreserved()
    {
        var buf = new byte[WireProtocol.AuctionMaxSize];
        var info = new InstrumentInfo
        {
            Symbol = "ITUB4",
            AuctionImbalanceCondition = 0x0200, // MoreSellers
        };
        WireProtocol.WriteAuction(buf, securityId: 1, info);

        int off = WireProtocol.FramingHeaderSize + 8 + 1 + 5;
        byte mask = buf[off++];
        Assert.NotEqual(0, mask & (1 << WireProtocol.AuctionFieldImbalanceCondition));

        // Find ImbalanceCondition slot
        Assert.Equal(0x0200L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off)));
    }

    [Fact]
    public void DataFlags_Auction_IsInEverythingButNotAll()
    {
        Assert.True(DataFlags.Everything.HasFlag(DataFlags.Auction));
        Assert.False(DataFlags.All.HasFlag(DataFlags.Auction));
        Assert.Equal((byte)0x80, (byte)DataFlags.Auction);
    }

    [Fact]
    public void DataFlags_Everything_Is0xFF()
    {
        // After adding Auction (0x80), Everything should be all bits set.
        Assert.Equal((byte)0xFF, (byte)DataFlags.Everything);
    }
}
