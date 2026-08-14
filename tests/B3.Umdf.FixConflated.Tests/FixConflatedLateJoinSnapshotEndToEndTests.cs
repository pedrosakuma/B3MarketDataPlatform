using System.Net;
using System.Net.Sockets;
using B3.Umdf.Book;
using B3.Umdf.FixConflated;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.FixConflated.Tests;

public sealed class FixConflatedLateJoinSnapshotEndToEndTests
{
    [Fact]
    public async Task Late_Joining_Clients_Receive_Current_Full_Snapshot_And_Clean_Follow_On_Incrementals()
    {
        const ulong securityId = 1234;

        var stateRegistry = new SymbolStateRegistry(NullLogger.Instance);
        var staleBuffer = new StaleMboBuffer(NullLogger.Instance);
        var bookManager = new BookManager(
            logger: NullLogger<BookManager>.Instance,
            stateRegistry: stateRegistry,
            staleBuffer: staleBuffer);
        var marketDataManager = new MarketDataManager(
            logger: NullLogger<MarketDataManager>.Instance,
            stateRegistry: stateRegistry);

        InstrumentInfo info = marketDataManager.GetOrCreateInfo(securityId);
        info.Symbol = "CPLE3";
        info.PriceDivisor = 100;

        OrderBook book = bookManager.GetOrCreateBook(securityId);
        SeedInitialBook(book, securityId);

        var resolver = new FixLiveInstrumentResolver(new SymbolRegistry(), [marketDataManager]);
        var hub = new FixConflatedSessionHub();
        using var channelHandler = new FixConflatedChannelHandler(
            0,
            hub,
            resolver,
            new FixConflatedMarketDataOptions
            {
                ConflationInterval = TimeSpan.FromMilliseconds(25),
                PendingEventCapacity = 512,
            });

        int port = GetFreeTcpPort();
        await using var server = new FixConflatedTcpServer(
            hub,
            new FixConflatedTcpServerOptions
            {
                OutboundQueueCapacity = 64,
                SessionOptions = new FixSessionOptions
                {
                    ApplicationResendBufferCapacity = 32,
                },
            },
            initialMessagesProvider: new FixInitialSnapshotProvider([bookManager], resolver).CreateMessages);
        await server.StartAsync(port);

        using var firstClient = new TcpClient { NoDelay = true };
        await firstClient.ConnectAsync(IPAddress.Loopback, port);
        using NetworkStream firstStream = firstClient.GetStream();
        await using var firstFixClient = new FixSocketClientTestHelpers.InflatingFixClient(firstStream);

        await firstFixClient.SendAsync(CreateLogon("CLIENT-LATE-1", "SANDBOX", 1));
        await ReadAndAssertLogonAckAsync(firstFixClient, expectedSeqNum: "1");

        FixMessage firstSnapshot = await firstFixClient.ReadMessageAsync();
        AssertSnapshotMatchesBook(firstSnapshot, book, securityId, "2");

        OrderBookEntry firstDeltaAdd = CreateEntry(securityId, 51001, BookSideType.Bid, 10025, 450);
        book.Bids.AddOrUpdate(firstDeltaAdd);
        channelHandler.OnOrderAdded(book, in firstDeltaAdd);

        OrderBookEntry firstDeltaUpdate = CreateEntry(securityId, 11005, BookSideType.Bid, 10020, 990);
        book.Bids.AddOrUpdate(firstDeltaUpdate);
        channelHandler.OnOrderUpdated(book, in firstDeltaUpdate);

        book.Asks.Remove(21004);
        channelHandler.OnOrderDeleted(book, 21004, BookSideType.Ask);

        FixMessage firstIncremental = await firstFixClient.ReadMessageAsync();
        Assert.Equal("3", FixApplicationMessageTestHelpers.GetRequired(firstIncremental, FixTags.MsgSeqNum));
        AssertIncrementalContains(
            firstIncremental,
            CreateExpectedIncremental("0", firstDeltaAdd),
            CreateExpectedIncremental("1", firstDeltaUpdate));

        using var secondClient = new TcpClient { NoDelay = true };
        await secondClient.ConnectAsync(IPAddress.Loopback, port);
        using NetworkStream secondStream = secondClient.GetStream();
        await using var secondFixClient = new FixSocketClientTestHelpers.InflatingFixClient(secondStream);

        await secondFixClient.SendAsync(CreateLogon("CLIENT-LATE-2", "SANDBOX", 1));
        await ReadAndAssertLogonAckAsync(secondFixClient, expectedSeqNum: "1");

        FixMessage secondSnapshot = await secondFixClient.ReadMessageAsync();
        AssertSnapshotMatchesBook(secondSnapshot, book, securityId, "2");

        FixMessage firstDeleteIncremental = await firstFixClient.ReadMessageAsync();
        Assert.Equal("4", FixApplicationMessageTestHelpers.GetRequired(firstDeleteIncremental, FixTags.MsgSeqNum));
        AssertIncrementalContains(
            firstDeleteIncremental,
            CreateExpectedIncremental("2", CreateEntry(securityId, 21004, BookSideType.Ask, 10050, 0)));

        OrderBookEntry secondDeltaAdd = CreateEntry(securityId, 61001, BookSideType.Ask, 10065, 720);
        book.Asks.AddOrUpdate(secondDeltaAdd);
        channelHandler.OnOrderAdded(book, in secondDeltaAdd);

        FixMessage firstSecondIncremental = await firstFixClient.ReadMessageAsync();
        Assert.Equal("5", FixApplicationMessageTestHelpers.GetRequired(firstSecondIncremental, FixTags.MsgSeqNum));
        AssertIncrementalContains(
            firstSecondIncremental,
            CreateExpectedIncremental("0", secondDeltaAdd));

        FixMessage secondIncremental = await secondFixClient.ReadMessageAsync();
        Assert.Equal("3", FixApplicationMessageTestHelpers.GetRequired(secondIncremental, FixTags.MsgSeqNum));
        AssertIncrementalContains(
            secondIncremental,
            CreateExpectedIncremental("0", secondDeltaAdd));

        OrderBookEntry secondDeltaUpdate = CreateEntry(securityId, 21003, BookSideType.Ask, 10055, 1230);
        book.Asks.AddOrUpdate(secondDeltaUpdate);
        channelHandler.OnOrderUpdated(book, in secondDeltaUpdate);

        book.Bids.Remove(11001);
        channelHandler.OnOrderDeleted(book, 11001, BookSideType.Bid);

        FixMessage firstThirdIncremental = await firstFixClient.ReadMessageAsync();
        Assert.Equal("6", FixApplicationMessageTestHelpers.GetRequired(firstThirdIncremental, FixTags.MsgSeqNum));
        AssertIncrementalContains(
            firstThirdIncremental,
            CreateExpectedIncremental("1", secondDeltaUpdate));

        FixMessage secondThirdIncremental = await secondFixClient.ReadMessageAsync();
        Assert.Equal("4", FixApplicationMessageTestHelpers.GetRequired(secondThirdIncremental, FixTags.MsgSeqNum));
        AssertIncrementalContains(
            secondThirdIncremental,
            CreateExpectedIncremental("1", secondDeltaUpdate));
    }

    private static void SeedInitialBook(OrderBook book, ulong securityId)
    {
        foreach (OrderBookEntry bid in new[]
                 {
                     CreateEntry(securityId, 11001, BookSideType.Bid, 10000, 1000),
                     CreateEntry(securityId, 11002, BookSideType.Bid, 10000, 700),
                     CreateEntry(securityId, 11003, BookSideType.Bid, 10010, 800),
                     CreateEntry(securityId, 11004, BookSideType.Bid, 10010, 650),
                     CreateEntry(securityId, 11005, BookSideType.Bid, 10020, 900),
                     CreateEntry(securityId, 11006, BookSideType.Bid, 10020, 600),
                     CreateEntry(securityId, 11007, BookSideType.Bid, 10030, 500),
                     CreateEntry(securityId, 11008, BookSideType.Bid, 10030, 400),
                 })
        {
            book.Bids.Add(bid);
        }

        foreach (OrderBookEntry ask in new[]
                 {
                     CreateEntry(securityId, 21001, BookSideType.Ask, 10040, 750),
                     CreateEntry(securityId, 21002, BookSideType.Ask, 10040, 500),
                     CreateEntry(securityId, 21003, BookSideType.Ask, 10050, 1250),
                     CreateEntry(securityId, 21004, BookSideType.Ask, 10050, 300),
                     CreateEntry(securityId, 21005, BookSideType.Ask, 10060, 800),
                     CreateEntry(securityId, 21006, BookSideType.Ask, 10060, 450),
                     CreateEntry(securityId, 21007, BookSideType.Ask, 10070, 900),
                     CreateEntry(securityId, 21008, BookSideType.Ask, 10070, 550),
                 })
        {
            book.Asks.Add(ask);
        }
    }

    private static async Task ReadAndAssertLogonAckAsync(FixSocketClientTestHelpers.InflatingFixClient client, string expectedSeqNum)
    {
        FixMessage logonAck = await client.ReadMessageAsync();
        Assert.Equal(FixMsgTypes.Logon, FixApplicationMessageTestHelpers.GetRequired(logonAck, FixTags.MsgType));
        Assert.Equal(expectedSeqNum, FixApplicationMessageTestHelpers.GetRequired(logonAck, FixTags.MsgSeqNum));
    }

    private static void AssertSnapshotMatchesBook(FixMessage snapshot, OrderBook book, ulong securityId, string expectedSeqNum)
    {
        Assert.Equal(FixMsgTypes.MarketDataSnapshotFullRefresh, FixApplicationMessageTestHelpers.GetRequired(snapshot, FixTags.MsgType));
        Assert.Equal(expectedSeqNum, FixApplicationMessageTestHelpers.GetRequired(snapshot, FixTags.MsgSeqNum));
        Assert.Equal($"SNAP-{securityId}", FixApplicationMessageTestHelpers.GetRequired(snapshot, FixTags.MDReqId));
        Assert.Equal("CPLE3", FixApplicationMessageTestHelpers.GetRequired(snapshot, FixTags.Symbol));
        Assert.Equal(securityId.ToString(), FixApplicationMessageTestHelpers.GetRequired(snapshot, FixTags.SecurityId));

        IReadOnlyList<IReadOnlyDictionary<int, string>> snapshotEntries = ParseSnapshotEntries(snapshot);
        List<ExpectedSnapshotEntry> expected = BuildExpectedSnapshotEntries(book);
        Assert.Equal(expected.Count, snapshotEntries.Count);

        List<ExpectedSnapshotEntry> actual = snapshotEntries
            .Select(static entry => new ExpectedSnapshotEntry(
                Type: entry[FixTags.MDEntryType],
                OrderId: entry[FixTags.OrderId],
                Price: entry[FixTags.MDEntryPx],
                Size: entry[FixTags.MDEntrySize]))
            .ToList();

        Assert.Equal(expected, actual);
    }

    private static void AssertIncrementalContains(FixMessage message, params ExpectedIncrementalEntry[] expectedEntries)
    {
        IReadOnlyList<IReadOnlyDictionary<int, string>> entries = ParseIncrementalEntries(message);
        List<ExpectedIncrementalEntry> actual = entries
            .Select(static entry => new ExpectedIncrementalEntry(
                UpdateAction: entry[FixTags.MDUpdateAction],
                Type: entry[FixTags.MDEntryType],
                OrderId: entry[FixTags.OrderId],
                Price: entry.TryGetValue(FixTags.MDEntryPx, out string? px) ? px : null,
                Size: entry.TryGetValue(FixTags.MDEntrySize, out string? size) ? size : null))
            .ToList();

        Assert.Equal(expectedEntries.Length, actual.Count);
        Assert.Equal(expectedEntries, actual);
    }

    private static List<ExpectedSnapshotEntry> BuildExpectedSnapshotEntries(OrderBook book)
    {
        return book.Bids.PriceLevels
            .SelectMany(static level => level.Value.Select(order => new ExpectedSnapshotEntry("0", order.OrderId.ToString(), FormatPrice(order.Price), order.Quantity.ToString())))
            .Concat(book.Asks.PriceLevels
                .SelectMany(static level => level.Value.Select(order => new ExpectedSnapshotEntry("1", order.OrderId.ToString(), FormatPrice(order.Price), order.Quantity.ToString()))))
            .ToList();
    }

    private static ExpectedIncrementalEntry CreateExpectedIncremental(string updateAction, OrderBookEntry entry)
        => new(
            UpdateAction: updateAction,
            Type: entry.Side == BookSideType.Bid ? "0" : "1",
            OrderId: entry.OrderId.ToString(),
            Price: updateAction == "2" ? null : FormatPrice(entry.Price),
            Size: updateAction == "2" ? null : entry.Quantity.ToString());

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

    private static FixMessage CreateLogon(string senderCompId, string targetCompId, int seqNum)
    {
        var message = new FixMessage();
        message.Add(FixTags.BeginString, FixMessageCodec.BeginString);
        message.Add(FixTags.MsgType, FixMsgTypes.Logon);
        message.Add(FixTags.SenderCompId, senderCompId);
        message.Add(FixTags.TargetCompId, targetCompId);
        message.Add(FixTags.MsgSeqNum, seqNum);
        message.Add(FixTags.SendingTime, "20260814-15:00:00.000");
        message.Add(FixTags.EncryptMethod, 0);
        message.Add(FixTags.HeartBtInt, 30);
        return message;
    }

    private static OrderBookEntry CreateEntry(ulong securityId, ulong orderId, BookSideType side, long price, long quantity)
    {
        return new OrderBookEntry
        {
            SecurityId = securityId,
            OrderId = orderId,
            Side = side,
            Price = price,
            Quantity = quantity,
        };
    }

    private static string FormatPrice(long rawPrice) => (rawPrice / 100m).ToString("0.00");

    private static int GetFreeTcpPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    private sealed record ExpectedSnapshotEntry(string Type, string OrderId, string Price, string Size);

    private sealed record ExpectedIncrementalEntry(string UpdateAction, string Type, string OrderId, string? Price, string? Size);
}
