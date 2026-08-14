using System.Net;
using System.Net.Sockets;
using B3.Umdf.Book;
using B3.Umdf.FixConflated;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit.Sdk;

namespace B3.Umdf.FixConflated.Tests;

public sealed class FixConflatedReconnectEndToEndTests
{
    [Fact]
    public async Task Stale_Reconnect_Is_Dropped_And_Fresh_Session_Recovers_Via_New_Snapshot()
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
        info.Symbol = "PETR4";
        info.PriceDivisor = 100;

        OrderBook book = bookManager.GetOrCreateBook(securityId);
        book.Bids.Add(CreateEntry(securityId, 7001, BookSideType.Bid, 2810, 100));
        book.Asks.Add(CreateEntry(securityId, 8001, BookSideType.Ask, 2815, 120));

        var resolver = new FixLiveInstrumentResolver(new SymbolRegistry(), [marketDataManager]);
        var hub = new FixConflatedSessionHub();
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

        await RunFirstSessionAsync(port);

        using (var staleClient = new TcpClient { NoDelay = true })
        {
            await staleClient.ConnectAsync(IPAddress.Loopback, port);
            using NetworkStream staleStream = staleClient.GetStream();
            await using var staleFixClient = new FixSocketClientTestHelpers.InflatingFixClient(staleStream);

            await staleFixClient.SendAsync(CreateLogon("CLIENT-A", "SANDBOX", 3));
            await staleFixClient.AssertClosedWithoutFrameAsync();
        }

        using (var freshClient = new TcpClient { NoDelay = true })
        {
            await freshClient.ConnectAsync(IPAddress.Loopback, port);
            using NetworkStream freshStream = freshClient.GetStream();
            await using var freshFixClient = new FixSocketClientTestHelpers.InflatingFixClient(freshStream);

            await freshFixClient.SendAsync(CreateLogon("CLIENT-B", "SANDBOX", 1));

            FixMessage freshLogonAck = await freshFixClient.ReadMessageAsync();
            Assert.Equal(FixMsgTypes.Logon, FixApplicationMessageTestHelpers.GetRequired(freshLogonAck, FixTags.MsgType));
            Assert.Equal("1", FixApplicationMessageTestHelpers.GetRequired(freshLogonAck, FixTags.MsgSeqNum));

            FixMessage freshSnapshot = await freshFixClient.ReadMessageAsync();
            Assert.Equal(FixMsgTypes.MarketDataSnapshotFullRefresh, FixApplicationMessageTestHelpers.GetRequired(freshSnapshot, FixTags.MsgType));
            Assert.Equal("2", FixApplicationMessageTestHelpers.GetRequired(freshSnapshot, FixTags.MsgSeqNum));
            Assert.Equal("SNAP-1234", FixApplicationMessageTestHelpers.GetRequired(freshSnapshot, FixTags.MDReqId));
            Assert.Equal("PETR4", FixApplicationMessageTestHelpers.GetRequired(freshSnapshot, FixTags.Symbol));
            Assert.Equal("1234", FixApplicationMessageTestHelpers.GetRequired(freshSnapshot, FixTags.SecurityId));
        }
    }

    private static async Task RunFirstSessionAsync(int port)
    {
        using var client = new TcpClient { NoDelay = true };
        await client.ConnectAsync(IPAddress.Loopback, port);
        client.Client.LingerState = new LingerOption(true, 0);

        using NetworkStream stream = client.GetStream();
        await using var fixClient = new FixSocketClientTestHelpers.InflatingFixClient(stream);

        await fixClient.SendAsync(CreateLogon("CLIENT-A", "SANDBOX", 1));

        FixMessage logonAck = await fixClient.ReadMessageAsync();
        Assert.Equal(FixMsgTypes.Logon, FixApplicationMessageTestHelpers.GetRequired(logonAck, FixTags.MsgType));
        Assert.Equal("1", FixApplicationMessageTestHelpers.GetRequired(logonAck, FixTags.MsgSeqNum));

        FixMessage snapshot = await fixClient.ReadMessageAsync();
        Assert.Equal(FixMsgTypes.MarketDataSnapshotFullRefresh, FixApplicationMessageTestHelpers.GetRequired(snapshot, FixTags.MsgType));
        Assert.Equal("2", FixApplicationMessageTestHelpers.GetRequired(snapshot, FixTags.MsgSeqNum));

        await fixClient.SendAsync(CreateHeartbeat("CLIENT-A", "SANDBOX", 2));
        await fixClient.SendAsync(CreateTestRequest("CLIENT-A", "SANDBOX", 3, "reconnect-probe"));

        FixMessage heartbeat = await fixClient.ReadMessageAsync();
        Assert.Equal(FixMsgTypes.Heartbeat, FixApplicationMessageTestHelpers.GetRequired(heartbeat, FixTags.MsgType));
        Assert.Equal("3", FixApplicationMessageTestHelpers.GetRequired(heartbeat, FixTags.MsgSeqNum));
        Assert.Equal("reconnect-probe", FixApplicationMessageTestHelpers.GetRequired(heartbeat, FixTags.TestReqId));
    }

    private static FixMessage CreateLogon(string senderCompId, string targetCompId, int seqNum)
    {
        var message = CreateSessionMessage(FixMsgTypes.Logon, senderCompId, targetCompId, seqNum);
        message.Add(FixTags.EncryptMethod, 0);
        message.Add(FixTags.HeartBtInt, 30);
        return message;
    }

    private static FixMessage CreateHeartbeat(string senderCompId, string targetCompId, int seqNum)
        => CreateSessionMessage(FixMsgTypes.Heartbeat, senderCompId, targetCompId, seqNum);

    private static FixMessage CreateTestRequest(string senderCompId, string targetCompId, int seqNum, string testReqId)
    {
        var message = CreateSessionMessage(FixMsgTypes.TestRequest, senderCompId, targetCompId, seqNum);
        message.Add(FixTags.TestReqId, testReqId);
        return message;
    }

    private static FixMessage CreateSessionMessage(string msgType, string senderCompId, string targetCompId, int seqNum)
    {
        var message = new FixMessage();
        message.Add(FixTags.BeginString, FixMessageCodec.BeginString);
        message.Add(FixTags.MsgType, msgType);
        message.Add(FixTags.SenderCompId, senderCompId);
        message.Add(FixTags.TargetCompId, targetCompId);
        message.Add(FixTags.MsgSeqNum, seqNum);
        message.Add(FixTags.SendingTime, "20260812-19:30:00.000");
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