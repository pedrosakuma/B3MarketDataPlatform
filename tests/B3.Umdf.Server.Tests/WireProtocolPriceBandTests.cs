using System.Buffers.Binary;
using System.Text;
using B3.Umdf.Book;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Wire-format tests for the PriceBand channel (issue #56). Pins the
/// single-bitmask layout, bit-position contract, and MessageType code so
/// changes that would silently break the SDK decoder fail loudly here first.
/// </summary>
public class WireProtocolPriceBandTests
{
    private static (ushort length, MessageType type) ReadFraming(Span<byte> buf) =>
        (BinaryPrimitives.ReadUInt16LittleEndian(buf), (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(buf[2..]));

    [Fact]
    public void WritePriceBand_MessageTypeIs0x00B1()
    {
        var buf = new byte[WireProtocol.PriceBandMaxSize];
        var info = new InstrumentInfo { Symbol = "PETR4", PriceBandTimestamp = 1 };
        WireProtocol.WritePriceBand(buf, securityId: 1, info);
        var (_, type) = ReadFraming(buf);
        Assert.Equal(MessageType.PriceBand, type);
        Assert.Equal((ushort)0x00B1, (ushort)type);
    }

    [Fact]
    public void WritePriceBand_OnlySymbol_MaskIsZero()
    {
        var buf = new byte[WireProtocol.PriceBandMaxSize];
        var info = new InstrumentInfo { Symbol = "PETR4" };
        int len = WireProtocol.WritePriceBand(buf, securityId: 99, info);

        var (msgLen, _) = ReadFraming(buf);
        Assert.Equal(len, msgLen);

        int off = WireProtocol.FramingHeaderSize;
        Assert.Equal(99UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(off))); off += 8;
        int symLen = buf[off++];
        Assert.Equal(5, symLen);
        Assert.Equal("PETR4", Encoding.UTF8.GetString(buf.AsSpan(off, symLen))); off += symLen;
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(off))); off += 4;
        Assert.Equal(len, off);
    }

    [Fact]
    public void WritePriceBand_LowHighRefAndDiscriminators_MaskAndValuesCorrect()
    {
        var buf = new byte[WireProtocol.PriceBandMaxSize];
        // Futures-style PERCENTAGE band: ±10% around a reference price.
        var info = new InstrumentInfo
        {
            Symbol = "WINJ5",
            PriceBandLow = -1000,                 // raw mantissa, /1e4 = -0.10 (= -10%)
            PriceBandHigh = 1000,                 // raw mantissa, /1e4 = +0.10 (= +10%)
            TradingReferencePrice = 13_500_000_000, // /1e8 = 135.00
            PriceLimitType = 2,                   // PERCENTAGE
            PriceBandType = 4,
            PriceBandMidpointPriceType = 1,
            PriceBandTimestamp = 1_700_000_000_000_000_000L,
        };
        int len = WireProtocol.WritePriceBand(buf, securityId: 42, info);

        int off = WireProtocol.FramingHeaderSize;
        Assert.Equal(42UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(off))); off += 8;
        int symLen = buf[off++]; Assert.Equal(5, symLen);
        Assert.Equal("WINJ5", Encoding.UTF8.GetString(buf.AsSpan(off, symLen))); off += symLen;

        uint mask = BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(off)); off += 4;
        uint expected =
            (1u << WireProtocol.PriceBandFieldLowerBand) |
            (1u << WireProtocol.PriceBandFieldUpperBand) |
            (1u << WireProtocol.PriceBandFieldTradingReferencePrice) |
            (1u << WireProtocol.PriceBandFieldPriceLimitType) |
            (1u << WireProtocol.PriceBandFieldPriceBandType) |
            (1u << WireProtocol.PriceBandFieldPriceBandMidpointPriceType) |
            (1u << WireProtocol.PriceBandFieldAsOfTimestampNanos);
        Assert.Equal(expected, mask);

        // Slots are written in bit order; verify each value.
        Assert.Equal(-1000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(1000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(13_500_000_000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(2L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(4L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(1L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(1_700_000_000_000_000_000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(len, off);
    }

    [Fact]
    public void WritePriceBand_RptSeq_OnlyWhenNonZero()
    {
        var buf = new byte[WireProtocol.PriceBandMaxSize];
        var info = new InstrumentInfo
        {
            Symbol = "ABEV3",
            PriceBandLow = 17_000,
            LastRptSeqPriceBand = 12345,
        };
        WireProtocol.WritePriceBand(buf, securityId: 7, info);

        int off = WireProtocol.FramingHeaderSize + 8 + 1 + 5;
        uint mask = BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(off)); off += 4;
        Assert.NotEqual(0u, mask & (1u << WireProtocol.PriceBandFieldRptSeq));
        Assert.NotEqual(0u, mask & (1u << WireProtocol.PriceBandFieldLowerBand));

        // Slots: LowerBand first (bit 0), RptSeq last (bit 7).
        Assert.Equal(17_000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(12345L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off)));
    }

    [Fact]
    public void DataFlags_PriceBand_IsInEverythingButNotAll()
    {
        Assert.True(DataFlags.Everything.HasFlag(DataFlags.PriceBand));
        Assert.False(DataFlags.All.HasFlag(DataFlags.PriceBand));
        Assert.Equal((byte)0x40, (byte)DataFlags.PriceBand);
    }

    [Fact]
    public void WritePriceBand_QuantityBandFields_MaskAndValuesCorrect()
    {
        var buf = new byte[WireProtocol.PriceBandMaxSize];
        var info = new InstrumentInfo
        {
            Symbol = "PETR4",
            AvgDailyTradedQty = 1_000_000,
            MaxTradeVol = 500_000,
        };
        int len = WireProtocol.WritePriceBand(buf, securityId: 99, info);

        int off = WireProtocol.FramingHeaderSize + 8 + 1 + 5;
        uint mask = BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(off)); off += 4;
        Assert.NotEqual(0u, mask & (1u << WireProtocol.PriceBandFieldAvgDailyTradedQty));
        Assert.NotEqual(0u, mask & (1u << WireProtocol.PriceBandFieldMaxOrderQty));

        // Slots: AvgDailyTradedQty (bit 8), MaxOrderQty (bit 9).
        Assert.Equal(1_000_000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(500_000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off)));
    }
}
