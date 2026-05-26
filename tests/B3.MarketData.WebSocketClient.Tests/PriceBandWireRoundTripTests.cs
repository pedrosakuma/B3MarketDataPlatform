using System.Buffers.Binary;
using B3.Umdf.Book;
using B3.Umdf.Server;

namespace B3.MarketData.WebSocketClient.Tests;

/// <summary>
/// End-to-end wire roundtrip for the PriceBand channel (issue #56):
/// server-side <c>WireProtocol.WritePriceBand</c> ⇆ SDK
/// <c>WireFormat.ReadPriceBand</c>. Pins the byte-for-byte contract between
/// the two halves of the codebase, including the dual Price-vs-Fixed8 scale
/// boundary (Low/High limits at 1e-4, TradingReferencePrice at 1e-8).
/// </summary>
public class PriceBandWireRoundTripTests
{
    [Fact]
    public void Roundtrip_FullyPopulated_AllFieldsPreserved()
    {
        var info = new InstrumentInfo
        {
            Symbol = "WINJ5",
            PriceBandLow = -1000L,                   // /1e4 = -0.10
            PriceBandHigh = 1000L,                   // /1e4 = +0.10
            TradingReferencePrice = 13_500_000_000L, // /1e8 = 135.00
            PriceLimitType = 2,
            PriceBandType = 4,
            PriceBandMidpointPriceType = 1,
            PriceBandTimestamp = 1_700_000_000_000_000_000L,
            LastRptSeqPriceBand = 999,
        };

        var buf = new byte[B3.Umdf.Server.WireProtocol.PriceBandMaxSize];
        int len = B3.Umdf.Server.WireProtocol.WritePriceBand(buf, securityId: 42UL, info);

        Assert.True(WireFormat.TryReadHeader(buf, out var hLen, out var type));
        Assert.Equal(len, hLen);
        Assert.Equal(WireFormat.MessageType.PriceBand, type);

        var ev = WireFormat.ReadPriceBand(
            buf.AsSpan(WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize),
            receivedUtc: DateTime.UtcNow);

        Assert.Equal(42UL, ev.SecurityId);
        Assert.Equal("WINJ5", ev.Symbol);
        Assert.Equal(-0.1000m, ev.LowerBand);
        Assert.Equal(0.1000m, ev.UpperBand);
        Assert.Equal(135.00m, ev.TradingReferencePrice);
        Assert.Equal((byte)2, ev.PriceLimitType);
        Assert.Equal((byte)4, ev.PriceBandType);
        Assert.Equal((byte)1, ev.PriceBandMidpointPriceType);
        Assert.Equal(1_700_000_000_000_000_000L, ev.AsOfTimestamp);
        Assert.Equal(999L, ev.RptSeq);
    }

    [Fact]
    public void Roundtrip_SymbolOnly_AllNumericsNull()
    {
        var info = new InstrumentInfo { Symbol = "ABEV3" };

        var buf = new byte[B3.Umdf.Server.WireProtocol.PriceBandMaxSize];
        int len = B3.Umdf.Server.WireProtocol.WritePriceBand(buf, securityId: 1UL, info);

        var ev = WireFormat.ReadPriceBand(
            buf.AsSpan(WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize),
            receivedUtc: DateTime.UtcNow);

        Assert.Equal(1UL, ev.SecurityId);
        Assert.Equal("ABEV3", ev.Symbol);
        Assert.Null(ev.LowerBand);
        Assert.Null(ev.UpperBand);
        Assert.Null(ev.TradingReferencePrice);
        Assert.Null(ev.PriceLimitType);
        Assert.Null(ev.PriceBandType);
        Assert.Null(ev.PriceBandMidpointPriceType);
        Assert.Null(ev.AsOfTimestamp);
        Assert.Null(ev.RptSeq);
        Assert.Null(ev.AvgDailyTradedQty);
        Assert.Null(ev.MaxOrderQty);
    }

    [Fact]
    public void Roundtrip_QuantityBandFields_Preserved()
    {
        var info = new InstrumentInfo
        {
            Symbol = "VALE3",
            AvgDailyTradedQty = 2_500_000L,
            MaxTradeVol = 750_000L,
        };

        var buf = new byte[B3.Umdf.Server.WireProtocol.PriceBandMaxSize];
        int len = B3.Umdf.Server.WireProtocol.WritePriceBand(buf, securityId: 11UL, info);

        var ev = WireFormat.ReadPriceBand(
            buf.AsSpan(WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize),
            receivedUtc: DateTime.UtcNow);

        Assert.Equal(11UL, ev.SecurityId);
        Assert.Equal("VALE3", ev.Symbol);
        Assert.Equal(2_500_000L, ev.AvgDailyTradedQty);
        Assert.Equal(750_000L, ev.MaxOrderQty);
    }

    [Fact]
    public void SubscribeFlags_PriceBand_IsInEverythingButNotAll()
    {
        Assert.True(SubscribeFlags.Everything.HasFlag(SubscribeFlags.PriceBand));
        Assert.False(SubscribeFlags.All.HasFlag(SubscribeFlags.PriceBand));
        Assert.Equal((byte)0x40, (byte)SubscribeFlags.PriceBand);
    }
}
