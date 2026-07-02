using System.Buffers.Binary;
using B3.Umdf.Book;

namespace B3.Umdf.Server.Tests;

public class WireProtocolTests
{
    private static (int length, MessageType type) ReadFraming(Span<byte> buf) =>
        ((int)BinaryPrimitives.ReadUInt32LittleEndian(buf), (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(buf[4..]));

    [Fact]
    public void WriteServerStatus_Ready_EncodesCorrectly()
    {
        var buf = new byte[64];
        int len = WireProtocol.WriteServerStatus(buf, ready: true);

        Assert.Equal(9, len);
        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(9, msgLen);
        Assert.Equal(MessageType.ServerStatus, type);
        Assert.Equal(1, buf[8]);
    }

    [Fact]
    public void WriteServerStatus_NotReady_EncodesCorrectly()
    {
        var buf = new byte[64];
        WireProtocol.WriteServerStatus(buf, ready: false);
        Assert.Equal(0, buf[8]);
    }

    [Fact]
    public void WriteSubscribeOk_RoundTrip()
    {
        var buf = new byte[128];
        int len = WireProtocol.WriteSubscribeOk(buf, securityId: 12345, flags: DataFlags.All, symbol: "PETR4");

        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(len, msgLen);
        Assert.Equal(MessageType.SubscribeOk, type);

        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(8));
        Assert.Equal(12345UL, secId);
        Assert.Equal((uint)DataFlags.All, BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(16)));
        int symLen = buf[20];
        Assert.Equal("PETR4", System.Text.Encoding.UTF8.GetString(buf, 21, symLen));
    }

    [Fact]
    public void WriteSubscribeError_RoundTrip()
    {
        var buf = new byte[128];
        int len = WireProtocol.WriteSubscribeError(buf, SubscribeErrorCode.UnknownSymbol, "UNKNOWN");

        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(len, msgLen);
        Assert.Equal(MessageType.SubscribeError, type);
        Assert.Equal((byte)SubscribeErrorCode.UnknownSymbol, buf[8]);
        int symLen = buf[9];
        Assert.Equal("UNKNOWN", System.Text.Encoding.UTF8.GetString(buf, 10, symLen));
    }

    [Fact]
    public void WriteOrderEvent_Added_RoundTrip()
    {
        var buf = new byte[64];
        int len = WireProtocol.WriteOrderEvent(buf, MessageType.OrderAdded,
            securityId: 99, orderId: 42, side: 1, price: 10_000L, qty: 500L);

        Assert.Equal(41, len);
        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(41, msgLen);
        Assert.Equal(MessageType.OrderAdded, type);

        int off = 8;
        Assert.Equal(99UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(42UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(10_000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(500L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(1, buf[off]);
    }

    [Fact]
    public void WriteOrderDeleted_RoundTrip()
    {
        var buf = new byte[64];
        int len = WireProtocol.WriteOrderDeleted(buf, securityId: 7, orderId: 3, side: 2);

        Assert.Equal(25, len);
        var (_, type) = ReadFraming(buf);
        Assert.Equal(MessageType.OrderDeleted, type);
        Assert.Equal(7UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(8)));
        Assert.Equal(3UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(16)));
        Assert.Equal(2, buf[24]);
    }

    [Fact]
    public void WriteTrade_RoundTrip()
    {
        var buf = new byte[64];
        int len = WireProtocol.WriteTrade(buf, securityId: 1001, price: 55_000L, qty: 100L, tradeId: 9876L);

        Assert.Equal(41, len);
        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(41, msgLen);
        Assert.Equal(MessageType.Trade, type);

        int off = 8;
        Assert.Equal(1001UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(55_000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(100L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(9876L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(0, buf[off]); // flags default
    }

    [Fact]
    public void WriteTrade_WithAuctionFlag_RoundTrip()
    {
        var buf = new byte[64];
        int len = WireProtocol.WriteTrade(buf, securityId: 1001, price: 55_000L, qty: 100L, tradeId: 9876L,
            flags: (byte)TradeFlags.AuctionPrint);

        Assert.Equal(41, len);
        Assert.Equal((byte)TradeFlags.AuctionPrint, buf[40]);
    }

    [Fact]
    public void WriteBookCleared_RoundTrip()
    {
        var buf = new byte[64];
        int len = WireProtocol.WriteBookCleared(buf, securityId: 500, clearSide: 3);

        Assert.Equal(17, len);
        var (_, type) = ReadFraming(buf);
        Assert.Equal(MessageType.BookCleared, type);
        Assert.Equal(500UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(8)));
        Assert.Equal(3, buf[16]);
    }

    [Fact]
    public void WriteMarketTierUpdate_RoundTrip()
    {
        var buf = new byte[64];
        int len = WireProtocol.WriteMarketTierUpdate(buf, securityId: 501, side: 0, totalQty: 12345, orderCount: 7);

        Assert.Equal(29, len);
        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(29, msgLen);
        Assert.Equal(MessageType.MarketTierUpdate, type);

        int off = 8;
        Assert.Equal(501UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(12345L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(7u, BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(off))); off += 4;
        Assert.Equal(0, buf[off]);
    }

    [Fact]
    public void WriteInfoSnapshot_EmptyInfo_ContainsOnlyHeader()
    {
        var buf = new byte[WireProtocol.InfoSnapshotMaxSize];
        var info = new InstrumentInfo();
        int len = WireProtocol.WriteInfoSnapshot(buf, securityId: 42, info);

        // framing(8) + secId(8) + mask(4) = 20 bytes minimum (no fields set)
        Assert.Equal(20, len);
        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(20, msgLen);
        Assert.Equal(MessageType.InfoSnapshot, type);
        Assert.Equal(42UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(8)));
        uint mask = BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(16));
        Assert.Equal(0u, mask);
    }

    [Fact]
    public void WriteInfoSnapshot_WithSomeFields_MaskAndValuesCorrect()
    {
        var buf = new byte[WireProtocol.InfoSnapshotMaxSize];
        var info = new InstrumentInfo
        {
            OpeningPrice = 10_000L,
            HighPrice = 12_000L,
            LowPrice = 9_500L,
            TradingStatus = 1,
        };
        int len = WireProtocol.WriteInfoSnapshot(buf, securityId: 77, info);

        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(len, msgLen);
        Assert.Equal(MessageType.InfoSnapshot, type);

        uint mask = BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(16));
        Assert.NotEqual(0u, mask);
        Assert.True((mask & (1u << WireProtocol.FieldOpeningPrice)) != 0);
        Assert.True((mask & (1u << WireProtocol.FieldHighPrice)) != 0);
        Assert.True((mask & (1u << WireProtocol.FieldLowPrice)) != 0);
        Assert.True((mask & (1u << WireProtocol.FieldTradingStatus)) != 0);

        // ClosingPrice not set
        Assert.True((mask & (1u << WireProtocol.FieldClosingPrice)) == 0);

        // Verify values (in bit order: OpeningPrice first)
        int off = 20; // after framing(8)+secId(8)+mask(4)
        Assert.Equal(10_000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(12_000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8; // High
        Assert.Equal(9_500L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off)));           // Low
    }

    [Fact]
    public void WriteInfoSnapshot_AuctionFields_RoundTrip()
    {
        var buf = new byte[WireProtocol.InfoSnapshotMaxSize];
        var info = new InstrumentInfo
        {
            TheoreticalOpeningPrice = 12_3450L, // 12.3450
            TheoreticalOpeningSize = 7_500L,
            AuctionImbalanceSize = 250L,
            // SBE ImbalanceCondition bit 8 = MoreBuyers
            AuctionImbalanceCondition = 0x0100,
        };
        int len = WireProtocol.WriteInfoSnapshot(buf, securityId: 99, info);

        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(len, msgLen);
        Assert.Equal(MessageType.InfoSnapshot, type);

        uint mask = BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(16));
        Assert.True((mask & (1u << WireProtocol.FieldTheoreticalOpeningPrice)) != 0);
        Assert.True((mask & (1u << WireProtocol.FieldTheoreticalOpeningSize)) != 0);
        Assert.True((mask & (1u << WireProtocol.FieldAuctionImbalanceSize)) != 0);
        Assert.True((mask & (1u << WireProtocol.FieldAuctionImbalanceCondition)) != 0);

        // Values appear in bit order. The set bits here are 7,8,9,24 → that's
        // the order on the wire. (24 is set last, after the four lower bits.)
        int off = 20;
        Assert.Equal(12_3450L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8; // TheoreticalOpeningPrice
        Assert.Equal(7_500L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;   // TheoreticalOpeningSize
        Assert.Equal(250L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;     // AuctionImbalanceSize
        Assert.Equal(0x0100L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off)));            // AuctionImbalanceCondition
    }

    [Fact]
    public void WriteCandleUpdate_RoundTrip()
    {
        var buf = new byte[128];
        var candle = new Candle(Time: 1000L, Open: 100L, High: 120L, Low: 90L, Close: 110L, Volume: 500L, Avg: 108L);
        int len = WireProtocol.WriteCandleUpdate(buf, securityId: 333, resolution: 1, in candle);

        Assert.Equal(WireProtocol.FramingHeaderSize + 8 + 2 + WireProtocol.CandleSize, len);
        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(len, msgLen);
        Assert.Equal(MessageType.CandleUpdate, type);

        int off = WireProtocol.FramingHeaderSize;
        Assert.Equal(333UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(1, BinaryPrimitives.ReadUInt16LittleEndian(buf.AsSpan(off))); off += 2;
        Assert.Equal(1000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8; // Time
        Assert.Equal(100L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;  // Open
        Assert.Equal(120L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;  // High
        Assert.Equal(90L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;   // Low
        Assert.Equal(110L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;  // Close
        Assert.Equal(500L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;  // Volume
        Assert.Equal(108L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off)));            // Avg
    }

    [Fact]
    public void WriteCandleSnapshot_SingleBatch_FlagsAndCountCorrect()
    {
        var buf = new byte[4096];
        var candles = new[]
        {
            new Candle(1L, 100L, 110L, 90L, 105L, 200L, 100L),
            new Candle(2L, 105L, 115L, 95L, 108L, 150L, 105L),
        };
        int len = WireProtocol.WriteCandleSnapshot(buf, securityId: 55, resolution: 1,
            flags: (byte)(WireProtocol.CandleFlagFirst | WireProtocol.CandleFlagLast), candles);

        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(len, msgLen);
        Assert.Equal(MessageType.CandleSnapshot, type);

        int off = WireProtocol.FramingHeaderSize;
        Assert.Equal(55UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(1, BinaryPrimitives.ReadUInt16LittleEndian(buf.AsSpan(off))); off += 2; // resolution
        Assert.Equal(WireProtocol.CandleFlagFirst | WireProtocol.CandleFlagLast, buf[off++]);
        Assert.Equal(2, BinaryPrimitives.ReadUInt16LittleEndian(buf.AsSpan(off))); // count = 2
    }

    [Fact]
    public void WriteRankingsUpdate_ThreeCategories_RoundTrip()
    {
        var buf = new byte[WireProtocol.RankingsUpdateMaxSize];
        var volume = new[] { new RankingEntry(10UL, 100L, "PETR4") };
        var gainers = new[] { new RankingEntry(20UL, 5L, "VALE3") };
        var losers = ReadOnlySpan<RankingEntry>.Empty;

        int len = WireProtocol.WriteRankingsUpdate(buf, volume, gainers, losers);

        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(len, msgLen);
        Assert.Equal(MessageType.RankingsUpdate, type);

        int off = WireProtocol.FramingHeaderSize;

        // Volume category: count=1
        Assert.Equal(1, buf[off++]);
        Assert.Equal(10UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(100L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        int volSymLen = buf[off++];
        Assert.Equal("PETR4", System.Text.Encoding.UTF8.GetString(buf, off, volSymLen));
        off += volSymLen;

        // Gainers category: count=1
        Assert.Equal(1, buf[off++]);
        Assert.Equal(20UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(5L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        int gainSymLen = buf[off++];
        Assert.Equal("VALE3", System.Text.Encoding.UTF8.GetString(buf, off, gainSymLen));
        off += gainSymLen;

        // Losers category: count=0
        Assert.Equal(0, buf[off]);
        Assert.Equal(len, off + 1); // message ends here
    }

    [Fact]
    public void TryReadFramingHeader_ValidBuffer_ReturnsTrue()
    {
        var buf = new byte[64];
        WireProtocol.WriteServerStatus(buf, ready: true);
        bool ok = WireProtocol.TryReadFramingHeader(buf, out uint length, out MessageType type);
        Assert.True(ok);
        Assert.Equal(9u, length);
        Assert.Equal(MessageType.ServerStatus, type);
    }

    [Fact]
    public void TryReadFramingHeader_TooShort_ReturnsFalse()
    {
        var buf = new byte[3];
        bool ok = WireProtocol.TryReadFramingHeader(buf, out _, out _);
        Assert.False(ok);
    }

    [Fact]
    public void WriteRecoveryProgress_NoStaleKinds_OnlyTotalsAndZeroKindCount()
    {
        var buf = new byte[WireProtocol.RecoveryProgressMaxSize];
        int len = WireProtocol.WriteRecoveryProgress(buf, totalSymbols: 18000, totalStaleSymbols: 0,
            staleByKind: ReadOnlySpan<int>.Empty);

        Assert.Equal(17, len); // 8 framing + 4 + 4 + 1
        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(17, msgLen);
        Assert.Equal(MessageType.RecoveryProgress, type);
        Assert.Equal(18000u, BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(8)));
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(12)));
        Assert.Equal(0, buf[16]); // kindCount
    }

    [Fact]
    public void WriteRecoveryProgress_MixedKinds_OnlyEmitsNonZero()
    {
        var buf = new byte[WireProtocol.RecoveryProgressMaxSize];
        Span<int> perKind = stackalloc int[14];
        perKind[0] = 7;     // MBO
        perKind[5] = 0;     // skipped
        perKind[10] = 13;   // SettlementPrice

        int len = WireProtocol.WriteRecoveryProgress(buf, 18000, 20, perKind);

        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(len, msgLen);
        Assert.Equal(MessageType.RecoveryProgress, type);
        Assert.Equal(18000u, BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(8)));
        Assert.Equal(20u, BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(12)));
        Assert.Equal(2, buf[16]); // kindCount = 2

        int off = 17;
        Assert.Equal(0, buf[off++]);
        Assert.Equal(7u, BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(off))); off += 4;
        Assert.Equal(10, buf[off++]);
        Assert.Equal(13u, BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(off))); off += 4;
        Assert.Equal(len, off);
    }

    [Fact]
    public void WriteRecoveryProgress_AllKindsStale_FitsInMaxSize()
    {
        var buf = new byte[WireProtocol.RecoveryProgressMaxSize];
        Span<int> perKind = stackalloc int[14];
        for (int i = 0; i < 14; i++) perKind[i] = i + 1;

        int len = WireProtocol.WriteRecoveryProgress(buf, 18000, 99, perKind);

        Assert.Equal(WireProtocol.RecoveryProgressMaxSize, len);
        var (msgLen, type) = ReadFraming(buf);
        Assert.Equal(len, msgLen);
        Assert.Equal(MessageType.RecoveryProgress, type);
        Assert.Equal(14, buf[16]);
    }
}
