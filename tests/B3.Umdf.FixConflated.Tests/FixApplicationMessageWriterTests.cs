using B3.Umdf.Book;
using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Tests;

public sealed class FixApplicationMessageWriterTests
{
    [Fact]
    public void IncrementalRefresh_Writes_DirectFixFrame_ThatDecodesCorrectly()
    {
        using var writer = new FixApplicationMessageWriter();
        var header = new FixApplicationSessionHeader(
            "SERVER",
            "CLIENT",
            7,
            new DateTimeOffset(2026, 8, 12, 19, 10, 11, 123, TimeSpan.Zero));
        var instrument = new FixMarketDataInstrument("PETR4", 1234, priceScale: 2, deliverToCompId: "LUX00");
        FixMarketDataIncrementalEntry[] entries =
        [
            new(
                FixMdUpdateAction.New,
                FixMdEntryType.Bid,
                new DateTimeOffset(2026, 8, 12, 19, 10, 11, 123, TimeSpan.Zero),
                FixMarketDataEntryFields.Price | FixMarketDataEntryFields.Size | FixMarketDataEntryFields.OrderId,
                Price: 2810,
                Size: 100,
                OrderId: 501),
            new(
                FixMdUpdateAction.Change,
                FixMdEntryType.Bid,
                new DateTimeOffset(2026, 8, 12, 19, 10, 11, 456, TimeSpan.Zero),
                FixMarketDataEntryFields.Price | FixMarketDataEntryFields.Size | FixMarketDataEntryFields.OrderId,
                Price: 2811,
                Size: 120,
                OrderId: 501),
        ];

        ReadOnlyMemory<byte> frame = writer.WriteIncrementalRefresh(header, instrument, entries);

        FixMessage decoded = Decode(frame);
        Assert.Equal(FixMsgTypes.MarketDataIncrementalRefresh, GetRequired(decoded, FixTags.MsgType));
        Assert.Equal("SERVER", GetRequired(decoded, FixTags.SenderCompId));
        Assert.Equal("CLIENT", GetRequired(decoded, FixTags.TargetCompId));
        Assert.Equal("7", GetRequired(decoded, FixTags.MsgSeqNum));
        Assert.Equal("2", GetRequired(decoded, FixTags.NoMDEntries));
        Assert.Equal("20260812", GetRequired(decoded, FixTags.TradeDate));
        Assert.Equal("3", GetRequired(decoded, FixTags.MDBookType));

        IReadOnlyList<IReadOnlyDictionary<int, string>> parsedEntries = ParseIncrementalEntries(decoded);
        Assert.Equal(2, parsedEntries.Count);

        Assert.Equal("0", parsedEntries[0][FixTags.MDUpdateAction]);
        Assert.Equal("8", parsedEntries[0][FixTags.SecurityIdSource]);
        Assert.Equal("BVMF", parsedEntries[0][FixTags.SecurityExchange]);
        Assert.Equal("0", parsedEntries[0][FixTags.MDEntryType]);
        Assert.Equal("PETR4", parsedEntries[0][FixTags.Symbol]);
        Assert.Equal("1234", parsedEntries[0][FixTags.SecurityId]);
        Assert.Equal("28.10", parsedEntries[0][FixTags.MDEntryPx]);
        Assert.Equal("100", parsedEntries[0][FixTags.MDEntrySize]);
        Assert.Equal("3", parsedEntries[0][FixTags.MDStreamId]);
        Assert.Equal("1", parsedEntries[0][FixTags.NumberOfOrders]);
        Assert.Equal("501", parsedEntries[0][FixTags.OrderId]);

        Assert.Equal("1", parsedEntries[1][FixTags.MDUpdateAction]);
        Assert.Equal("28.11", parsedEntries[1][FixTags.MDEntryPx]);
        Assert.Equal("120", parsedEntries[1][FixTags.MDEntrySize]);
    }

    [Fact]
    public void SnapshotFullRefresh_Writes_All_Current_Prices_In_One_Message()
    {
        using var writer = new FixApplicationMessageWriter();
        var header = new FixApplicationSessionHeader(
            "SERVER",
            "CLIENT",
            11,
            new DateTimeOffset(2026, 8, 12, 19, 15, 00, TimeSpan.Zero));
        var request = new FixMarketDataSnapshotRequest(
            "snap-1",
            new FixMarketDataInstrument("PETR4", 1234, priceScale: 2, deliverToCompId: "LUX00"));
        OrderBook book = CreateBook(
            (1UL, BookSideType.Bid, 2810L, 100L),
            (2UL, BookSideType.Bid, 2809L, 150L),
            (3UL, BookSideType.Ask, 2812L, 90L),
            (4UL, BookSideType.Ask, 2813L, 110L));

        ReadOnlyMemory<byte> frame = writer.WriteSnapshotFullRefresh(header, request, book);

        FixMessage decoded = Decode(frame);
        Assert.Equal(FixMsgTypes.MarketDataSnapshotFullRefresh, GetRequired(decoded, FixTags.MsgType));
        Assert.Equal("snap-1", GetRequired(decoded, FixTags.MDReqId));
        Assert.Equal("PETR4", GetRequired(decoded, FixTags.Symbol));
        Assert.Equal("1234", GetRequired(decoded, FixTags.SecurityId));
        Assert.Equal("8", GetRequired(decoded, FixTags.SecurityIdSource));
        Assert.Equal("BVMF", GetRequired(decoded, FixTags.SecurityExchange));
        Assert.Equal("LUX00", GetRequired(decoded, FixTags.DeliverToCompID));
        Assert.Equal("4", GetRequired(decoded, FixTags.NoMDEntries));
        Assert.Equal("4", GetRequired(decoded, FixTags.TotNumReports));

        IReadOnlyList<IReadOnlyDictionary<int, string>> entries = ParseSnapshotEntries(decoded);
        Assert.Equal(4, entries.Count);
        Assert.Equal("0", entries[0][FixTags.MDEntryType]);
        Assert.Equal("28.10", entries[0][FixTags.MDEntryPx]);
        Assert.Equal("1", entries[0][FixTags.MDEntryPositionNo]);
        Assert.Equal("1", entries[0][FixTags.OrderId]);
        Assert.Equal("1", entries[2][FixTags.MDEntryType]);
        Assert.Equal("28.12", entries[2][FixTags.MDEntryPx]);
        Assert.Equal("3", entries[2][FixTags.OrderId]);
    }

    private static OrderBook CreateBook(params (ulong OrderId, BookSideType Side, long Price, long Quantity)[] orders)
    {
        var book = new OrderBook(1234);
        foreach (var order in orders)
        {
            var entry = new OrderBookEntry
            {
                OrderId = order.OrderId,
                Side = order.Side,
                SecurityId = book.SecurityId,
                Price = order.Price,
                Quantity = order.Quantity,
            };

            book.GetSide(order.Side).Add(in entry);
        }

        return book;
    }

    private static FixMessage Decode(ReadOnlyMemory<byte> frame)
    {
        FixDecodeResult decoded = FixMessageCodec.Decode(frame.Span);
        Assert.True(decoded.Success, decoded.Error.ToString());
        return decoded.Message!;
    }

    private static IReadOnlyList<IReadOnlyDictionary<int, string>> ParseIncrementalEntries(FixMessage message)
    {
        List<IReadOnlyDictionary<int, string>> entries = [];
        Dictionary<int, string>? current = null;
        bool insideGroup = false;

        foreach (FixField field in message.Fields)
        {
            if (field.Tag == FixTags.NoMDEntries)
            {
                insideGroup = true;
                continue;
            }

            if (!insideGroup || field.Tag == FixTags.CheckSum)
                continue;

            if (field.Tag == FixTags.MDUpdateAction)
            {
                current = [];
                entries.Add(current);
            }

            Assert.NotNull(current);
            current[field.Tag] = field.Value;
        }

        return entries;
    }

    private static IReadOnlyList<IReadOnlyDictionary<int, string>> ParseSnapshotEntries(FixMessage message)
    {
        List<IReadOnlyDictionary<int, string>> entries = [];
        Dictionary<int, string>? current = null;
        bool insideGroup = false;

        foreach (FixField field in message.Fields)
        {
            if (field.Tag == FixTags.NoMDEntries)
            {
                insideGroup = true;
                continue;
            }

            if (!insideGroup || field.Tag == FixTags.CheckSum)
                continue;

            if (field.Tag == FixTags.MDEntryType)
            {
                current = [];
                entries.Add(current);
            }

            Assert.NotNull(current);
            current[field.Tag] = field.Value;
        }

        return entries;
    }

    private static string GetRequired(FixMessage message, int tag)
    {
        Assert.True(message.TryGetString(tag, out string? value));
        return value!;
    }
}
