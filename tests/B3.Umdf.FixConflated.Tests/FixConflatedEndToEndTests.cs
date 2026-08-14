using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using B3.Umdf.Book;
using B3.Umdf.FixConflated;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.FixConflated.Tests;

public sealed class FixConflatedEndToEndTests
{
    [Fact]
    public async Task Tcp_Server_Delivers_Snapshot_Conflated_Book_Deltas_And_Prompt_Trade()
    {
        const ulong securityId = 1234;
        TimeSpan conflationWindow = TimeSpan.FromMilliseconds(300);

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
        info.Symbol = "PETR4";
        info.PriceDivisor = 100;

        var resolver = new FixLiveInstrumentResolver(new SymbolRegistry(), [marketDataManager]);
        var hub = new FixConflatedSessionHub();
        using var channelHandler = new FixConflatedChannelHandler(
            0,
            hub,
            resolver,
            new FixConflatedMarketDataOptions
            {
                ConflationInterval = conflationWindow,
                PendingEventCapacity = 512,
            });

        OrderBook book = bookManager.GetOrCreateBook(securityId);
        book.Bids.Add(CreateEntry(securityId, 7001, BookSideType.Bid, 2810, 100));
        book.Asks.Add(CreateEntry(securityId, 8001, BookSideType.Ask, 2815, 120));

        int port = GetFreeTcpPort();
        await using var server = new FixConflatedTcpServer(
            hub,
            new FixConflatedTcpServerOptions
            {
                OutboundQueueCapacity = 64,
                SessionOptions = new FixSessionOptions
                {
                    ApplicationResendBufferCapacity = 16,
                },
            },
            initialMessagesProvider: new FixInitialSnapshotProvider([bookManager], resolver).CreateMessages);
        await server.StartAsync(port);

        using var client = new TcpClient { NoDelay = true };
        await client.ConnectAsync(IPAddress.Loopback, port);
        using NetworkStream stream = client.GetStream();
        await using var fixClient = new FixSocketClientTestHelpers.InflatingFixClient(stream);

        await fixClient.SendAsync(CreateLogon("CLIENT-A", "SANDBOX", 1));

        FixMessage logonAck = await fixClient.ReadMessageAsync();
        Assert.Equal(FixMsgTypes.Logon, FixApplicationMessageTestHelpers.GetRequired(logonAck, FixTags.MsgType));
        Assert.Equal("1", FixApplicationMessageTestHelpers.GetRequired(logonAck, FixTags.MsgSeqNum));
        Assert.Equal("SANDBOX", FixApplicationMessageTestHelpers.GetRequired(logonAck, FixTags.SenderCompId));
        Assert.Equal("CLIENT-A", FixApplicationMessageTestHelpers.GetRequired(logonAck, FixTags.TargetCompId));

        FixMessage snapshot = await fixClient.ReadMessageAsync();
        Assert.Equal(FixMsgTypes.MarketDataSnapshotFullRefresh, FixApplicationMessageTestHelpers.GetRequired(snapshot, FixTags.MsgType));
        Assert.Equal("2", FixApplicationMessageTestHelpers.GetRequired(snapshot, FixTags.MsgSeqNum));
        Assert.Equal("SNAP-1234", FixApplicationMessageTestHelpers.GetRequired(snapshot, FixTags.MDReqId));
        Assert.Equal("PETR4", FixApplicationMessageTestHelpers.GetRequired(snapshot, FixTags.Symbol));
        Assert.Equal("1234", FixApplicationMessageTestHelpers.GetRequired(snapshot, FixTags.SecurityId));

        IReadOnlyList<IReadOnlyDictionary<int, string>> snapshotEntries = ParseSnapshotEntries(snapshot);
        Assert.Equal(2, snapshotEntries.Count);
        Assert.Equal(["0", "1"], snapshotEntries.Select(static entry => entry[FixTags.MDEntryType]).ToArray());
        Assert.Equal(["28.10", "28.15"], snapshotEntries.Select(static entry => entry[FixTags.MDEntryPx]).ToArray());

        OrderBookEntry add = CreateEntry(securityId, 9001, BookSideType.Bid, 2811, 40);
        book.Bids.AddOrUpdate(add);
        channelHandler.OnOrderAdded(book, in add);

        OrderBookEntry update = CreateEntry(securityId, 9001, BookSideType.Bid, 2812, 35);
        book.Bids.AddOrUpdate(update);
        channelHandler.OnOrderUpdated(book, in update);

        DateTimeOffset tradeTime = DateTimeOffset.UtcNow;
        Stopwatch tradeStopwatch = Stopwatch.StartNew();
        channelHandler.OnTrade(securityId, 2813, 12, 99001, tradeTime.ToUnixTimeMilliseconds() * 1_000_000);

        FixMessage trade = await fixClient.ReadMessageAsync();
        tradeStopwatch.Stop();

        Assert.True(tradeStopwatch.Elapsed < conflationWindow,
            $"Expected trade delivery before the {conflationWindow.TotalMilliseconds:F0} ms conflation window, but it took {tradeStopwatch.Elapsed.TotalMilliseconds:F1} ms.");
        Assert.Equal(FixMsgTypes.MarketDataIncrementalRefresh, FixApplicationMessageTestHelpers.GetRequired(trade, FixTags.MsgType));
        Assert.Equal("3", FixApplicationMessageTestHelpers.GetRequired(trade, FixTags.MsgSeqNum));

        IReadOnlyList<IReadOnlyDictionary<int, string>> tradeEntries = ParseIncrementalEntries(trade);
        IReadOnlyDictionary<int, string> tradeEntry = Assert.Single(tradeEntries);
        Assert.Equal("0", tradeEntry[FixTags.MDUpdateAction]);
        Assert.Equal("2", tradeEntry[FixTags.MDEntryType]);
        Assert.Equal("28.13", tradeEntry[FixTags.MDEntryPx]);
        Assert.Equal("12", tradeEntry[FixTags.MDEntrySize]);
        Assert.Equal("99001", tradeEntry[FixTags.TradeId]);

        FixMessage conflatedBook = await fixClient.ReadMessageAsync();
        Assert.Equal(FixMsgTypes.MarketDataIncrementalRefresh, FixApplicationMessageTestHelpers.GetRequired(conflatedBook, FixTags.MsgType));
        Assert.Equal("4", FixApplicationMessageTestHelpers.GetRequired(conflatedBook, FixTags.MsgSeqNum));

        IReadOnlyList<IReadOnlyDictionary<int, string>> bookEntries = ParseIncrementalEntries(conflatedBook);
        Assert.Equal(2, bookEntries.Count);
        Assert.Equal(["0", "1"], bookEntries.Select(static entry => entry[FixTags.MDUpdateAction]).ToArray());
        Assert.All(bookEntries, static entry => Assert.Equal("0", entry[FixTags.MDEntryType]));
        Assert.Equal(["28.11", "28.12"], bookEntries.Select(static entry => entry[FixTags.MDEntryPx]).ToArray());
        Assert.Equal(["9001", "9001"], bookEntries.Select(static entry => entry[FixTags.OrderId]).ToArray());
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
        message.Add(FixTags.SendingTime, "20260812-19:30:00.000");
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
}