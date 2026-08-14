using System.Net;
using System.Net.Sockets;
using B3.Umdf.Book;
using B3.Umdf.FixConflated;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.FixConflated.Tests;

public sealed class FixSubscriptionTests
{
    [Fact]
    public async Task Subscribe_Receives_Snapshot_And_Subsequent_Incrementals_For_Requested_Instrument_Only()
    {
        await using var harness = await SubscriptionHarness.CreateAsync();
        await harness.ClientA.SendAsync(CreateLogon("CLIENT-A", "SANDBOX", 1));
        Assert.Equal(FixMsgTypes.Logon, FixApplicationMessageTestHelpers.GetRequired(await harness.ClientA.ReadMessageAsync(), FixTags.MsgType));

        await harness.ClientA.SendAsync(CreateMarketDataRequest("req-1", '1', 1234));

        FixMessage snapshot = await harness.ClientA.ReadMessageAsync();
        Assert.Equal(FixMsgTypes.MarketDataSnapshotFullRefresh, FixApplicationMessageTestHelpers.GetRequired(snapshot, FixTags.MsgType));
        Assert.Equal("1234", FixApplicationMessageTestHelpers.GetRequired(snapshot, FixTags.SecurityId));
        Assert.Equal("req-1", FixApplicationMessageTestHelpers.GetRequired(snapshot, FixTags.MDReqId));

        harness.PublishIncremental(1234, "req-ignored");
        FixMessage incremental = await harness.ClientA.ReadMessageAsync();
        Assert.Equal(FixMsgTypes.MarketDataIncrementalRefresh, FixApplicationMessageTestHelpers.GetRequired(incremental, FixTags.MsgType));
        Assert.Equal("1234", FixApplicationMessageTestHelpers.GetRequired(incremental, FixTags.SecurityId));
    }

    [Fact]
    public async Task Unsubscribe_Stops_Future_Incrementals()
    {
        await using var harness = await SubscriptionHarness.CreateAsync();
        await harness.ClientA.SendAsync(CreateLogon("CLIENT-A", "SANDBOX", 1));
        _ = await harness.ClientA.ReadMessageAsync();
        await harness.ClientA.SendAsync(CreateMarketDataRequest("req-sub", '1', 1234));
        _ = await harness.ClientA.ReadMessageAsync();

        await harness.ClientA.SendAsync(CreateMarketDataRequest("req-unsub", '2', 1234, 3));
        await AssertNoMessageAsync(harness.ClientA, TimeSpan.FromMilliseconds(250));

        harness.PublishIncremental(1234, "after-unsub");
        await AssertNoMessageAsync(harness.ClientA, TimeSpan.FromMilliseconds(600));
    }

    [Fact]
    public async Task Unknown_SecurityId_Is_Rejected()
    {
        await using var harness = await SubscriptionHarness.CreateAsync();
        await harness.ClientA.SendAsync(CreateLogon("CLIENT-A", "SANDBOX", 1));
        _ = await harness.ClientA.ReadMessageAsync();

        await harness.ClientA.SendAsync(CreateMarketDataRequest("req-bad", '1', 999999));

        FixMessage reject = await harness.ClientA.ReadMessageAsync();
        Assert.Equal(FixMsgTypes.MarketDataRequestReject, FixApplicationMessageTestHelpers.GetRequired(reject, FixTags.MsgType));
        Assert.Equal("req-bad", FixApplicationMessageTestHelpers.GetRequired(reject, FixTags.MDReqId));
        Assert.Equal("0", FixApplicationMessageTestHelpers.GetRequired(reject, FixApplicationTags.MdReqRejReason));
    }

    [Fact]
    public async Task Different_Sessions_Only_Receive_Their_Own_Subscriptions()
    {
        await using var harness = await SubscriptionHarness.CreateAsync();
        await harness.ClientA.SendAsync(CreateLogon("CLIENT-A", "SANDBOX", 1));
        _ = await harness.ClientA.ReadMessageAsync();
        await harness.ClientB.SendAsync(CreateLogon("CLIENT-B", "SANDBOX", 1));
        _ = await harness.ClientB.ReadMessageAsync();

        await harness.ClientA.SendAsync(CreateMarketDataRequest("req-a", '1', 1234));
        await harness.ClientB.SendAsync(CreateMarketDataRequest("req-b", '1', 5678));
        Assert.Equal("1234", FixApplicationMessageTestHelpers.GetRequired(await harness.ClientA.ReadMessageAsync(), FixTags.SecurityId));
        Assert.Equal("5678", FixApplicationMessageTestHelpers.GetRequired(await harness.ClientB.ReadMessageAsync(), FixTags.SecurityId));

        harness.PublishIncremental(1234, "inc-a");
        harness.PublishIncremental(5678, "inc-b");

        FixMessage clientAIncremental = await harness.ClientA.ReadMessageAsync();
        FixMessage clientBIncremental = await harness.ClientB.ReadMessageAsync();
        Assert.Equal("1234", FixApplicationMessageTestHelpers.GetRequired(clientAIncremental, FixTags.SecurityId));
        Assert.Equal("5678", FixApplicationMessageTestHelpers.GetRequired(clientBIncremental, FixTags.SecurityId));
        await AssertNoMessageAsync(harness.ClientA, TimeSpan.FromMilliseconds(400));
        await AssertNoMessageAsync(harness.ClientB, TimeSpan.FromMilliseconds(400));
    }

    private static async Task AssertNoMessageAsync(FixSocketClientTestHelpers.InflatingFixClient client, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.ReadMessageAsync(cts.Token));
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

    private static FixMessage CreateMarketDataRequest(string mdReqId, char subscriptionRequestType, ulong securityId, int seqNum = 2)
    {
        var message = new FixMessage();
        message.Add(FixTags.BeginString, FixMessageCodec.BeginString);
        message.Add(FixTags.MsgType, FixMsgTypes.MarketDataRequest);
        message.Add(FixTags.SenderCompId, "CLIENT");
        message.Add(FixTags.TargetCompId, "SANDBOX");
        message.Add(FixTags.MsgSeqNum, seqNum);
        message.Add(FixTags.SendingTime, "20260812-19:30:01.000");
        message.Add(FixTags.MDReqId, mdReqId);
        message.Add(FixApplicationTags.SubscriptionRequestType, subscriptionRequestType.ToString());
        message.Add(FixApplicationTags.NoRelatedSym, 1);
        message.Add(FixTags.SecurityId, securityId.ToString());
        message.Add(FixTags.SecurityIdSource, "8");
        message.Add(FixTags.SecurityExchange, "BVMF");
        return message;
    }

    private sealed class SubscriptionHarness : IAsyncDisposable
    {
        private readonly OrderBook _book1234;
        private readonly OrderBook _book5678;
        private readonly FixConflatedSessionHub _hub;
        public FixSocketClientTestHelpers.InflatingFixClient ClientA { get; }
        public FixSocketClientTestHelpers.InflatingFixClient ClientB { get; }
        public FixConflatedTcpServer Server { get; }
        public TcpClient TcpClientA { get; }
        public TcpClient TcpClientB { get; }

        private SubscriptionHarness(FixConflatedSessionHub hub, FixConflatedTcpServer server, TcpClient tcpClientA, TcpClient tcpClientB, FixSocketClientTestHelpers.InflatingFixClient clientA, FixSocketClientTestHelpers.InflatingFixClient clientB, OrderBook book1234, OrderBook book5678)
        {
            _hub = hub;
            Server = server;
            TcpClientA = tcpClientA;
            TcpClientB = tcpClientB;
            ClientA = clientA;
            ClientB = clientB;
            _book1234 = book1234;
            _book5678 = book5678;
        }

        public static async Task<SubscriptionHarness> CreateAsync()
        {
            var stateRegistry = new SymbolStateRegistry(NullLogger.Instance);
            var staleBuffer = new StaleMboBuffer(NullLogger.Instance);
            var bookManager = new BookManager(logger: NullLogger<BookManager>.Instance, stateRegistry: stateRegistry, staleBuffer: staleBuffer);
            var marketDataManager = new MarketDataManager(logger: NullLogger<MarketDataManager>.Instance, stateRegistry: stateRegistry);
            marketDataManager.GetOrCreateInfo(1234).Symbol = "PETR4";
            marketDataManager.GetOrCreateInfo(1234).PriceDivisor = 100;
            marketDataManager.GetOrCreateInfo(5678).Symbol = "VALE3";
            marketDataManager.GetOrCreateInfo(5678).PriceDivisor = 100;

            OrderBook book1234 = bookManager.GetOrCreateBook(1234);
            book1234.Bids.Add(new OrderBookEntry { SecurityId = 1234, OrderId = 1, Side = BookSideType.Bid, Price = 2810, Quantity = 100 });
            OrderBook book5678 = bookManager.GetOrCreateBook(5678);
            book5678.Bids.Add(new OrderBookEntry { SecurityId = 5678, OrderId = 2, Side = BookSideType.Bid, Price = 5510, Quantity = 100 });

            var resolver = new FixLiveInstrumentResolver(new SymbolRegistry(), [marketDataManager]);
            var snapshotProvider = new FixInitialSnapshotProvider([bookManager], resolver);
            var requestHandler = new FixMarketDataRequestHandler(snapshotProvider, resolver);
            var hub = new FixConflatedSessionHub();
            int port = GetFreeTcpPort();
            var server = new FixConflatedTcpServer(hub, new FixConflatedTcpServerOptions { OutboundQueueCapacity = 64 }, null, requestHandler);
            await server.StartAsync(port);

            var tcpClientA = new TcpClient { NoDelay = true };
            await tcpClientA.ConnectAsync(IPAddress.Loopback, port);
            var tcpClientB = new TcpClient { NoDelay = true };
            await tcpClientB.ConnectAsync(IPAddress.Loopback, port);

            var clientA = new FixSocketClientTestHelpers.InflatingFixClient(tcpClientA.GetStream());
            var clientB = new FixSocketClientTestHelpers.InflatingFixClient(tcpClientB.GetStream());
            return new SubscriptionHarness(hub, server, tcpClientA, tcpClientB, clientA, clientB, book1234, book5678);
        }

        public void PublishIncremental(ulong securityId, string mdReqId)
        {
            var message = new FixMessage();
            message.Add(FixTags.MsgType, FixMsgTypes.MarketDataIncrementalRefresh);
            message.Add(FixTags.MDReqId, mdReqId);
            message.Add(FixTags.Symbol, securityId == 1234 ? "PETR4" : "VALE3");
            message.Add(FixTags.SecurityId, securityId.ToString());
            message.Add(FixTags.NoMDEntries, 1);
            message.Add(FixTags.MDUpdateAction, 0);
            message.Add(FixTags.MDEntryType, 0);
            message.Add(FixTags.MDEntryPx, securityId == 1234 ? "28.11" : "55.11");
            message.Add(FixTags.MDEntrySize, 10);
            message.Add(FixTags.OrderId, securityId == 1234 ? 1001 : 2001);
            _hub.BroadcastApplication(message);
        }

        public async ValueTask DisposeAsync()
        {
            await ClientA.DisposeAsync();
            await ClientB.DisposeAsync();
            TcpClientA.Dispose();
            TcpClientB.Dispose();
            await Server.DisposeAsync();
        }

        private static int GetFreeTcpPort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            try { return ((IPEndPoint)listener.LocalEndpoint).Port; }
            finally { listener.Stop(); }
        }
    }
}
