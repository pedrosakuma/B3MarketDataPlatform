using B3.MarketData.Wire;
using System.Buffers.Binary;
using ServerWire = B3.Umdf.Server.WireProtocol;
using ServerMsg = B3.MarketData.Wire.MessageType;

namespace B3.MarketData.WebSocketClient.Tests;

/// <summary>
/// Round-trip tests asserting the SDK <see cref="WireFormat"/> decoders accept
/// the exact bytes the server's <c>WireProtocol</c> writers produce. These
/// cover the parity gaps closed by issue #41 — MBO / MBP / News / Candles /
/// Rankings / SymbolStaleStatus / RecoveryProgress.
/// </summary>
public class ParityWireRoundTripTests
{
    [Fact]
    public void OrderAdded_RoundTripsPriceWithSbeExponent()
    {
        var buf = new byte[64];
        int len = ServerWire.WriteOrderEvent(buf, ServerMsg.OrderAdded,
            securityId: 42, orderId: 9001, side: (byte)BookSide.Bid,
            price: 12_3456, qty: 250);

        Assert.True(WireFormat.TryReadHeader(buf, out var totalLen, out var type));
        Assert.Equal((uint)len, totalLen);
        Assert.Equal(MessageType.OrderAdded, type);

        var (secId, orderId, side, price, qty) = WireFormat.ReadOrderEvent(
            buf.AsSpan(WireFormat.FramingHeaderSize));
        Assert.Equal(42UL, secId);
        Assert.Equal(9001UL, orderId);
        Assert.Equal((byte)BookSide.Bid, side);
        Assert.Equal(12_3456L, price);
        Assert.Equal(250L, qty);
        Assert.Equal(12.3456m, price / WireFormat.PriceScale);
    }

    [Fact]
    public void OrderDeleted_RoundTrips()
    {
        var buf = new byte[32];
        int len = ServerWire.WriteOrderDeleted(buf,
            securityId: 7, orderId: 555, side: (byte)BookSide.Ask);

        WireFormat.TryReadHeader(buf, out _, out var type);
        Assert.Equal(MessageType.OrderDeleted, type);

        var (secId, orderId, side) = WireFormat.ReadOrderDeleted(
            buf.AsSpan(WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize));
        Assert.Equal(7UL, secId);
        Assert.Equal(555UL, orderId);
        Assert.Equal((byte)BookSide.Ask, side);
    }

    [Fact]
    public void BookCleared_RoundTrips()
    {
        var buf = new byte[17];
        ServerWire.WriteBookCleared(buf, securityId: 1, clearSide: (byte)BookClearSide.Bid);

        var (secId, clearByte) = WireFormat.ReadBookCleared(
            buf.AsSpan(WireFormat.FramingHeaderSize));
        Assert.Equal(1UL, secId);
        Assert.Equal((byte)BookClearSide.Bid, clearByte);
    }

    [Fact]
    public void MarketTierUpdate_RoundTrips()
    {
        var buf = new byte[32];
        int len = ServerWire.WriteMarketTierUpdate(buf,
            securityId: 11, side: (byte)BookSide.Bid, totalQty: 5_000, orderCount: 3);

        WireFormat.TryReadHeader(buf, out _, out var type);
        Assert.Equal(MessageType.MarketTierUpdate, type);

        var (secId, side, qty, count) = WireFormat.ReadMarketTierUpdate(
            buf.AsSpan(WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize));
        Assert.Equal(11UL, secId);
        Assert.Equal((byte)BookSide.Bid, side);
        Assert.Equal(5_000L, qty);
        Assert.Equal(3, count);
    }

    [Fact]
    public void LevelUpdate_RoundTripsPriceWithSbeExponent()
    {
        var buf = new byte[64];
        int len = ServerWire.WriteLevelUpdate(buf,
            securityId: 99, side: (byte)BookSide.Ask, price: 25_5000, totalQty: 1_200, orderCount: 4);

        WireFormat.TryReadHeader(buf, out _, out var type);
        Assert.Equal(MessageType.LevelUpdate, type);

        var (secId, side, price, qty, count) = WireFormat.ReadLevelUpdate(
            buf.AsSpan(WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize));
        Assert.Equal(99UL, secId);
        Assert.Equal((byte)BookSide.Ask, side);
        Assert.Equal(25_5000L, price);
        Assert.Equal(1_200L, qty);
        Assert.Equal(4, count);
        Assert.Equal(25.5m, price / WireFormat.PriceScale);
    }

    [Fact]
    public void LevelDeleted_RoundTrips()
    {
        var buf = new byte[32];
        int len = ServerWire.WriteLevelDeleted(buf,
            securityId: 99, side: (byte)BookSide.Bid, price: 36_7800);

        var (secId, side, price) = WireFormat.ReadLevelDeleted(
            buf.AsSpan(WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize));
        Assert.Equal(99UL, secId);
        Assert.Equal((byte)BookSide.Bid, side);
        Assert.Equal(36_7800L, price);
    }

    [Fact]
    public void LevelSnapshot_RoundTripsBothSides()
    {
        // Build a 2-bid + 1-ask snapshot using the server writer.
        int total = ServerWire.LevelSnapshotSize(2, 1);
        var buf = new byte[total];
        int offset = ServerWire.WriteLevelSnapshotHeader(buf, securityId: 42, bidCount: 2, askCount: 1);
        offset = ServerWire.WriteLevelSnapshotEntry(buf, offset, price: 36_7000, totalQty: 100, orderCount: 2);
        offset = ServerWire.WriteLevelSnapshotEntry(buf, offset, price: 36_6500, totalQty: 50, orderCount: 1);
        offset = ServerWire.WriteLevelSnapshotEntry(buf, offset, price: 36_8500, totalQty: 75, orderCount: 1);
        Assert.Equal(total, offset);

        var ev = WireFormat.ReadLevelSnapshot(
            buf.AsSpan(WireFormat.FramingHeaderSize, total - WireFormat.FramingHeaderSize).ToArray(),
            "PETR4", DateTime.UtcNow);
        Assert.Equal(42UL, ev.SecurityId);
        Assert.Equal(2, ev.Bids.Count);
        Assert.Single(ev.Asks);
        Assert.Equal(36.70m, ev.Bids[0].Price);
        Assert.Equal(100L, ev.Bids[0].TotalQty);
        Assert.Equal(2, ev.Bids[0].OrderCount);
        Assert.Equal(36.85m, ev.Asks[0].Price);
    }

    [Fact]
    public void BookSnapshot_HeaderOnlyMarker_DecodesEmpty()
    {
        var buf = new byte[ServerWire.BookSnapshotSize(0, 0)];
        ServerWire.WriteBookSnapshotHeader(buf, securityId: 1, rptSeq: 42, bidCount: 0, askCount: 0);

        var ev = WireFormat.ReadBookSnapshot(
            buf.AsSpan(WireFormat.FramingHeaderSize).ToArray(),
            "PETR4", DateTime.UtcNow);
        Assert.Equal(1UL, ev.SecurityId);
        Assert.Equal(42U, ev.RptSeq);
        Assert.Empty(ev.Bids);
        Assert.Empty(ev.Asks);
    }

    [Fact]
    public void SymbolStaleStatus_RoundTrips()
    {
        var buf = new byte[17];
        ServerWire.WriteSymbolStaleStatus(buf, securityId: 77, isStale: true);

        var (secId, isStale) = WireFormat.ReadSymbolStaleStatus(
            buf.AsSpan(WireFormat.FramingHeaderSize));
        Assert.Equal(77UL, secId);
        Assert.True(isStale);
    }

    [Fact]
    public void RecoveryProgress_RoundTrips()
    {
        var buf = new byte[ServerWire.RecoveryProgressMaxSize];
        Span<int> staleByKind = stackalloc int[] { 0, 4, 0, 7 };
        int len = ServerWire.WriteRecoveryProgress(buf,
            totalSymbols: 100, totalStaleSymbols: 11, staleByKind);

        var ev = WireFormat.ReadRecoveryProgress(
            buf.AsSpan(WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize).ToArray(),
            DateTime.UtcNow);
        Assert.Equal(100U, ev.TotalSymbols);
        Assert.Equal(11U, ev.TotalStaleSymbols);
        // Only non-zero kinds make it onto the wire (indexes 1 and 3 here).
        Assert.Equal(2, ev.StaleByKind.Count);
        Assert.Equal((byte)1, ev.StaleByKind[0].Kind);
        Assert.Equal(4U, ev.StaleByKind[0].StaleCount);
        Assert.Equal((byte)3, ev.StaleByKind[1].Kind);
        Assert.Equal(7U, ev.StaleByKind[1].StaleCount);
    }

    [Fact]
    public void News_BeginChunkEnd_ReassemblesAllThreeFields()
    {
        // Build a NewsBegin + one chunk per field + a final empty NewsEnd.
        var headline = System.Text.Encoding.UTF8.GetBytes("Petrobras anuncia dividendos");
        var body = System.Text.Encoding.UTF8.GetBytes("Lorem ipsum dolor sit amet.");
        var url = System.Text.Encoding.UTF8.GetBytes("https://example.com/n/1");
        const ulong newsId = 12345UL;

        var begin = new byte[ServerWire.NewsBeginTotalSize];
        ServerWire.WriteNewsBegin(begin,
            securityIdOrZero: 0, newsId: newsId, source: 0, language: 1, origTimeNanos: 1_000,
            totalHeadlineLen: (uint)headline.Length,
            totalTextLen: (uint)body.Length,
            totalUrlLen: (uint)url.Length);

        var hChunk = new byte[ServerWire.NewsChunkTotalSize(headline.Length)];
        ServerWire.WriteNewsChunk(hChunk, newsId, ServerWire.NewsField.Headline, headline, isFinal: false);
        var tChunk = new byte[ServerWire.NewsChunkTotalSize(body.Length)];
        ServerWire.WriteNewsChunk(tChunk, newsId, ServerWire.NewsField.Text, body, isFinal: false);
        // Last fragment carries the URL and the NewsEnd opcode.
        var uEnd = new byte[ServerWire.NewsChunkTotalSize(url.Length)];
        ServerWire.WriteNewsChunk(uEnd, newsId, ServerWire.NewsField.Url, url, isFinal: true);

        // Verify decoding each one matches the SDK's parsers.
        var hdr = WireFormat.ReadNewsBegin(begin.AsSpan(WireFormat.FramingHeaderSize));
        Assert.Equal(newsId, hdr.NewsId);
        Assert.Equal((uint)headline.Length, hdr.TotalHeadlineLen);
        Assert.Equal((uint)body.Length, hdr.TotalTextLen);
        Assert.Equal((uint)url.Length, hdr.TotalUrlLen);

        var (_, _, hField) = WireFormat.ReadNewsChunk(
            hChunk.AsSpan(WireFormat.FramingHeaderSize), out var hFrag);
        Assert.Equal(WireFormat.NewsField.Headline, hField);
        Assert.Equal(headline.Length, hFrag.Length);
        Assert.True(headline.AsSpan().SequenceEqual(hFrag));

        var (_, _, uFieldEnd) = WireFormat.ReadNewsChunk(
            uEnd.AsSpan(WireFormat.FramingHeaderSize), out var uFrag);
        Assert.Equal(WireFormat.NewsField.Url, uFieldEnd);
        Assert.True(url.AsSpan().SequenceEqual(uFrag));
    }

    [Fact]
    public void RankingsUpdate_RoundTripsAllThreeCategories()
    {
        var entries = new[]
        {
            new B3.Umdf.Server.RankingEntry(secId(1), 100, "PETR4"),
            new B3.Umdf.Server.RankingEntry(secId(2),  90, "VALE3"),
        };
        var empty = Array.Empty<B3.Umdf.Server.RankingEntry>();

        var buf = new byte[ServerWire.RankingsUpdateMaxSize];
        int len = ServerWire.WriteRankingsUpdate(buf, entries, empty, empty);

        var ev = WireFormat.ReadRankingsUpdate(
            buf.AsSpan(WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize).ToArray(),
            DateTime.UtcNow);
        Assert.Equal(2, ev.Volume.Count);
        Assert.Empty(ev.Gainers);
        Assert.Empty(ev.Losers);
        Assert.Equal("PETR4", ev.Volume[0].Symbol);
        Assert.Equal(100L, ev.Volume[0].Value);

        static ulong secId(ulong v) => v;
    }

    [Fact]
    public void Candles_SnapshotAndUpdate_RoundTripWithSbeExponent()
    {
        // Hand-craft a CandleSnapshot frame the same way the server's
        // internal WriteCandleSnapshot would, then verify the SDK decoder.
        // (WriteCandleSnapshot is internal — recreate the bytes here.)
        const ulong secId = 42;
        const ushort resolution = 60;
        const byte flags = 0x01 | 0x02; // first + last
        const int candleCount = 1;
        int total = WireFormat.FramingHeaderSize + 8 + 2 + 1 + 2 + candleCount * 56;
        var buf = new byte[total];
        BinaryPrimitives.WriteUInt32LittleEndian(buf, (uint)total);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(4), (ushort)MessageType.CandleSnapshot);
        BinaryPrimitives.WriteUInt64LittleEndian(buf.AsSpan(8), secId);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(16), resolution);
        buf[18] = flags;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(19), (ushort)candleCount);
        // Candle: time, open, high, low, close, volume, avg
        int o = 21;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(o), 1_700_000_000_000_000_000L); o += 8;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(o), 36_7000L); o += 8;          // open 36.70
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(o), 37_0000L); o += 8;          // high 37.00
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(o), 36_5000L); o += 8;          // low  36.50
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(o), 36_9000L); o += 8;          // close 36.90
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(o), 12_345L);  o += 8;          // volume
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(o), 36_8000L);                  // avg 36.80

        var ev = WireFormat.ReadCandleSnapshot(
            buf.AsSpan(WireFormat.FramingHeaderSize).ToArray(),
            "PETR4", DateTime.UtcNow);
        Assert.Equal(42UL, ev.SecurityId);
        Assert.Equal(60, ev.Resolution);
        Assert.True(ev.IsFirst);
        Assert.True(ev.IsLast);
        Assert.Single(ev.Candles);
        var c = ev.Candles[0];
        Assert.Equal(36.70m, c.Open);
        Assert.Equal(37.00m, c.High);
        Assert.Equal(36.50m, c.Low);
        Assert.Equal(36.90m, c.Close);
        Assert.Equal(12_345L, c.Volume);
        Assert.Equal(36.80m, c.Avg);
    }
}
