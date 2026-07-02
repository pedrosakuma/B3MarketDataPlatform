using B3.MarketData.Wire;
using B3.Umdf.Book;
using B3.Umdf.Server;

namespace B3.MarketData.WebSocketClient.Tests;

/// <summary>
/// End-to-end wire roundtrip for the Auction channel (issue #57):
/// server-side <c>WireProtocol.WriteAuction</c> ⇆ SDK
/// <c>WireFormat.ReadAuction</c>. Pins the byte-for-byte contract between
/// the two halves of the codebase, including the imbalance side decoding.
/// </summary>
public class AuctionWireRoundTripTests
{
    [Fact]
    public void Roundtrip_FullyPopulated_AllFieldsPreserved()
    {
        var info = new InstrumentInfo
        {
            Symbol = "WINJ5",
            AuctionImbalanceSize = 50_000L,
            AuctionImbalanceCondition = 0x0100, // MoreBuyers
            TradingStatus = 2,           // Pre-Open
            TradSesOpenTime = 1_700_000_000_000_000_000UL,
            AuctionTimestamp = 1_700_000_000_000_001_000L,
            LastRptSeqAuctionImbalance = 999,
        };

        var buf = new byte[B3.Umdf.Server.WireProtocol.AuctionMaxSize];
        int len = B3.Umdf.Server.WireProtocol.WriteAuction(buf, securityId: 42UL, info);

        Assert.True(WireFormat.TryReadHeader(buf, out var hLen, out var type));
        Assert.Equal((uint)len, hLen);
        Assert.Equal(MessageType.Auction, type);

        var ev = WireFormat.ReadAuction(
            buf.AsSpan(WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize),
            receivedUtc: DateTime.UtcNow);

        Assert.Equal(42UL, ev.SecurityId);
        Assert.Equal("WINJ5", ev.Symbol);
        Assert.Equal(50_000L, ev.ImbalanceQty);
        Assert.Equal(ImbalanceSide.MoreBuyers, ev.ImbalanceSide);
        Assert.Equal((ushort)0x0100, ev.ImbalanceConditionRaw);
        Assert.Equal(2, ev.TradingStatus);
        Assert.Equal(1_700_000_000_000_000_000L, ev.TradSesOpenTime);
        Assert.Equal(1_700_000_000_000_001_000L, ev.AsOfTimestamp);
        Assert.Equal(999L, ev.RptSeq);
    }

    [Fact]
    public void Roundtrip_MoreSellers_ImbalanceSideCorrect()
    {
        var info = new InstrumentInfo
        {
            Symbol = "VALE3",
            AuctionImbalanceCondition = 0x0200, // MoreSellers
        };

        var buf = new byte[B3.Umdf.Server.WireProtocol.AuctionMaxSize];
        int len = B3.Umdf.Server.WireProtocol.WriteAuction(buf, securityId: 1UL, info);

        var ev = WireFormat.ReadAuction(
            buf.AsSpan(WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize),
            receivedUtc: DateTime.UtcNow);

        Assert.Equal(ImbalanceSide.MoreSellers, ev.ImbalanceSide);
        Assert.Equal((ushort)0x0200, ev.ImbalanceConditionRaw);
    }

    [Fact]
    public void Roundtrip_Balanced_ImbalanceSideCorrect()
    {
        var info = new InstrumentInfo
        {
            Symbol = "PETR4",
            AuctionImbalanceCondition = 0x0000, // Balanced
        };

        var buf = new byte[B3.Umdf.Server.WireProtocol.AuctionMaxSize];
        int len = B3.Umdf.Server.WireProtocol.WriteAuction(buf, securityId: 1UL, info);

        var ev = WireFormat.ReadAuction(
            buf.AsSpan(WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize),
            receivedUtc: DateTime.UtcNow);

        Assert.Equal(ImbalanceSide.Balanced, ev.ImbalanceSide);
        Assert.Equal((ushort)0x0000, ev.ImbalanceConditionRaw);
    }

    [Fact]
    public void Roundtrip_SymbolOnly_AllNumericsNull()
    {
        var info = new InstrumentInfo { Symbol = "ABEV3" };

        var buf = new byte[B3.Umdf.Server.WireProtocol.AuctionMaxSize];
        int len = B3.Umdf.Server.WireProtocol.WriteAuction(buf, securityId: 1UL, info);

        var ev = WireFormat.ReadAuction(
            buf.AsSpan(WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize),
            receivedUtc: DateTime.UtcNow);

        Assert.Equal(1UL, ev.SecurityId);
        Assert.Equal("ABEV3", ev.Symbol);
        Assert.Null(ev.ImbalanceQty);
        Assert.Equal(ImbalanceSide.Balanced, ev.ImbalanceSide); // Default when no condition
        Assert.Null(ev.ImbalanceConditionRaw);
        Assert.Null(ev.TradingStatus);
        Assert.Null(ev.TradSesOpenTime);
        Assert.Null(ev.AsOfTimestamp);
        Assert.Null(ev.RptSeq);
    }

    [Fact]
    public void SubscribeFlags_Auction_IsInEverythingButNotAll()
    {
        Assert.True(SubscribeFlags.Everything.HasFlag(SubscribeFlags.Auction));
        Assert.False(SubscribeFlags.All.HasFlag(SubscribeFlags.Auction));
        Assert.Equal((byte)0x80, (byte)SubscribeFlags.Auction);
    }

    [Fact]
    public void SubscribeFlags_Everything_Is0xFF()
    {
        // After adding Auction (0x80), Everything should cover all channels.
        Assert.Equal((byte)0xFF, (byte)SubscribeFlags.Everything);
    }
}
