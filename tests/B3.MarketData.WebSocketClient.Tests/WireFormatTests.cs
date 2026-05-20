using B3.MarketData.WebSocketClient;

namespace B3.MarketData.WebSocketClient.Tests;

/// <summary>
/// Pure-decode tests for <see cref="WireFormat"/>. These don't open a
/// socket — they encode bytes the same way the server's
/// <c>WireProtocol</c> does and assert the SDK decodes them into the
/// typed events.
/// </summary>
public class WireFormatTests
{
    [Fact]
    public void Trade_RoundTrip_ScalesPriceWithSbeExponent()
    {
        // Encode a Trade frame the way the server would.
        Span<byte> buf = stackalloc byte[36];
        WriteServerTradeFrame(buf, securityId: 42, price: 12_3456, qty: 100, tradeId: 7);

        Assert.True(WireFormat.TryReadHeader(buf, out var len, out var type));
        Assert.Equal(36, len);
        Assert.Equal(WireFormat.MessageType.Trade, type);

        var (secId, price, qty, tradeId) = WireFormat.ReadTrade(buf[WireFormat.FramingHeaderSize..]);
        Assert.Equal(42UL, secId);
        Assert.Equal(12_3456L, price);
        Assert.Equal(100L, qty);
        Assert.Equal(7L, tradeId);

        // Scaling: raw 12_3456 with exponent -4 == 12.3456
        decimal scaled = price / WireFormat.PriceScale;
        Assert.Equal(12.3456m, scaled);
    }

    [Fact]
    public void Subscribe_FramesContainFlagsAndSymbol()
    {
        Span<byte> buf = stackalloc byte[64];
        int len = WireFormat.WriteSubscribe(buf, SubscribeFlags.Trades | SubscribeFlags.Info, "PETR4");

        Assert.True(WireFormat.TryReadHeader(buf, out var totalLen, out var type));
        Assert.Equal(len, totalLen);
        Assert.Equal(WireFormat.MessageType.Subscribe, type);
        // [flags=0x12][symLen=5][PETR4]
        Assert.Equal((byte)0x12, buf[4]);
        Assert.Equal((byte)5, buf[5]);
        Assert.Equal((byte)'P', buf[6]);
        Assert.Equal((byte)'4', buf[10]);
    }

    [Fact]
    public void InfoSnapshot_DecodesOnlySetFields_AndAppliesPriceScale()
    {
        // Set bits: LastTradePrice (4) and LastTradeSize (5).
        uint mask = (1u << WireFormat.FieldLastTradePrice) | (1u << WireFormat.FieldLastTradeSize);
        Span<byte> buf = stackalloc byte[256];
        // header(4) + secId(8) + mask(4) + 2 * i64
        ushort total = 4 + 8 + 4 + 16;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(buf, total);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(buf[2..], (ushort)WireFormat.MessageType.InfoSnapshot);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(buf[4..], 99UL);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(buf[12..], mask);
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(buf[16..], 25_5000L); // 25.5000
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(buf[24..], 200L);

        var ev = WireFormat.ReadInfoSnapshot(buf[4..total], "PETR4", DateTime.UtcNow);
        Assert.Equal(99UL, ev.SecurityId);
        Assert.Equal("PETR4", ev.Symbol);
        Assert.Equal(25.5m, ev.LastTradePrice);
        Assert.Equal(200L, ev.LastTradeSize);
        // Unset fields stay null.
        Assert.Null(ev.OpeningPrice);
        Assert.Null(ev.VwapPrice);
        Assert.Null(ev.TheoreticalOpeningPrice);
        Assert.Null(ev.AuctionImbalanceSize);
        Assert.Null(ev.AuctionImbalanceCondition);
    }

    [Theory]
    [InlineData(0x0000, AuctionImbalanceCondition.Balanced)]
    [InlineData(0x0100, AuctionImbalanceCondition.MoreBuyers)]
    [InlineData(0x0200, AuctionImbalanceCondition.MoreSellers)]
    [InlineData(0x0300, AuctionImbalanceCondition.Unknown)] // both bits set
    public void InfoSnapshot_DecodesAuctionFields(int rawImbalance, AuctionImbalanceCondition expected)
    {
        // Set bits: TheoreticalOpeningPrice(7), TheoreticalOpeningSize(8),
        //           AuctionImbalanceSize(9), AuctionImbalanceCondition(24).
        uint mask = (1u << WireFormat.FieldTheoreticalOpeningPrice)
                  | (1u << WireFormat.FieldTheoreticalOpeningSize)
                  | (1u << WireFormat.FieldAuctionImbalanceSize)
                  | (1u << WireFormat.FieldAuctionImbalanceCondition);
        Span<byte> buf = stackalloc byte[256];
        // header(4) + secId(8) + mask(4) + 4 * i64
        ushort total = 4 + 8 + 4 + 32;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(buf, total);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(buf[2..], (ushort)WireFormat.MessageType.InfoSnapshot);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(buf[4..], 7UL);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(buf[12..], mask);
        // Bit-order on wire: 7, 8, 9, 24.
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(buf[16..], 30_2500L); // 30.25 TOP
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(buf[24..], 1_500L);   // TOP size
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(buf[32..], 800L);     // imbalance size
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(buf[40..], rawImbalance);

        var ev = WireFormat.ReadInfoSnapshot(buf[4..total], "VALE3", DateTime.UtcNow);
        Assert.Equal(30.25m, ev.TheoreticalOpeningPrice);
        Assert.Equal(1_500L, ev.TheoreticalOpeningSize);
        Assert.Equal(800L, ev.AuctionImbalanceSize);
        Assert.Equal(expected, ev.AuctionImbalanceCondition);
    }

    private static void WriteServerTradeFrame(Span<byte> dest, ulong securityId, long price, long qty, long tradeId)
    {
        const ushort totalLen = 4 + 8 + 8 + 8 + 8;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(dest, totalLen);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(dest[2..], (ushort)WireFormat.MessageType.Trade);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(dest[4..], securityId);
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(dest[12..], price);
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(dest[20..], qty);
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(dest[28..], tradeId);
    }
}
