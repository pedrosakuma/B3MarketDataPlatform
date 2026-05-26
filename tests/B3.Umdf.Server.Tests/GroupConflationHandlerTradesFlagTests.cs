using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Reflection;
using B3.Umdf.Book;
using B3.Umdf.Server;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Pins the opt-in routing of <see cref="DataFlags.Trades"/>:
/// <list type="bullet">
///   <item>Subscribers WITHOUT <c>Trades</c> never receive <see cref="MessageType.Trade"/>
///   nor <see cref="MessageType.TradeBust"/>.</item>
///   <item>Subscribers WITH <c>Trades</c> receive both, regardless of whether they also
///   asked for <c>Book</c> or <c>Mbp</c>.</item>
///   <item>Trade history snapshot (per-symbol ring) is sent only when <c>Trades</c> is set.</item>
///   <item>Trades opt-in is independent: a Trades-only subscriber gets prints + history
///   without book or level frames.</item>
/// </list>
/// Phase D of the trade-opt-in plan.
/// </summary>
public class GroupConflationHandlerTradesFlagTests
{
    private const ulong SecurityId = 4001;
    private const string Symbol = "TFLG";

    [Fact]
    public async Task Subscriber_WithoutTradesFlag_DoesNotReceiveTradeOrBust()
    {
        var w = NewWiring();
        w.Group.StartBroadcaster(0);
        try
        {
            var rec = new RecordingWebSocket();
            var session = new ClientSession(rec, channelCapacity: 64);
            w.Manager.RegisterClient(session);
            _ = Task.Run(() => session.RunWriteLoopAsync());
            // Book only — explicitly no Trades.
            w.Manager.HandleSubscribe(session.Id, Symbol, DataFlags.Book,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);

            await WaitUntil(() => rec.HasMessageType(MessageType.BookSnapshot), TimeSpan.FromSeconds(2));

            w.Group.OnTrade(SecurityId, price: 100, quantity: 5, tradeId: 1, sendingTimeNs: 0);
            w.Group.OnTradeBust(SecurityId, price: 100, quantity: 5, tradeId: 1);
            w.Group.OnBatchComplete();

            // Give the broadcaster a chance.
            await Task.Delay(150);

            Assert.False(rec.HasMessageType(MessageType.Trade));
            Assert.False(rec.HasMessageType(MessageType.TradeBust));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task Subscriber_WithTradesFlag_ReceivesTradeAndBust()
    {
        var w = NewWiring();
        w.Group.StartBroadcaster(0);
        try
        {
            var rec = new RecordingWebSocket();
            var session = new ClientSession(rec, channelCapacity: 64);
            w.Manager.RegisterClient(session);
            _ = Task.Run(() => session.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(session.Id, Symbol, DataFlags.Book | DataFlags.Trades,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);

            await WaitUntil(() => rec.HasMessageType(MessageType.BookSnapshot), TimeSpan.FromSeconds(2));

            w.Group.OnTrade(SecurityId, price: 100, quantity: 5, tradeId: 1, sendingTimeNs: 0);
            w.Group.OnBatchComplete();

            await WaitUntil(() => rec.HasMessageType(MessageType.Trade), TimeSpan.FromSeconds(2));

            w.Group.OnTradeBust(SecurityId, price: 100, quantity: 5, tradeId: 1);
            w.Group.OnBatchComplete();

            await WaitUntil(() => rec.HasMessageType(MessageType.TradeBust), TimeSpan.FromSeconds(2));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task TradesOnlySubscriber_ReceivesTrades_NoBookOrLevels()
    {
        var w = NewWiring();
        w.Group.StartBroadcaster(0);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);

            var rec = new RecordingWebSocket();
            var session = new ClientSession(rec, channelCapacity: 64);
            w.Manager.RegisterClient(session);
            _ = Task.Run(() => session.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(session.Id, Symbol, DataFlags.Trades,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);

            // Order events should NOT reach the subscriber.
            var entry = NewEntry(orderId: 5, price: 1000, qty: 1);
            w.Group.OnOrderAdded(book, in entry);
            w.Group.OnPriceLevelChanged(book, BookSideType.Bid, 1000);

            // Trade SHOULD reach the subscriber.
            w.Group.OnTrade(SecurityId, price: 1000, quantity: 1, tradeId: 7, sendingTimeNs: 0);
            w.Group.OnBatchComplete();

            await WaitUntil(() => rec.HasMessageType(MessageType.Trade), TimeSpan.FromSeconds(2));
            Assert.False(rec.HasMessageType(MessageType.OrderAdded));
            Assert.False(rec.HasMessageType(MessageType.LevelUpdate));
            Assert.False(rec.HasMessageType(MessageType.LevelSnapshot));
            Assert.False(rec.HasMessageType(MessageType.BookSnapshot));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task TradesOnlySubscriber_ReceivesCandleSnapshot()
    {
        var w = NewWiring();
        w.Group.StartBroadcaster(0);
        try
        {
            // Pre-seed candles by subscribing a "warm" client that requests
            // Trades, then firing a trade. After Phase C the ring requires an
            // active subscriber to hydrate.
            var warmRec = new RecordingWebSocket();
            var warmSession = new ClientSession(warmRec, channelCapacity: 64);
            w.Manager.RegisterClient(warmSession);
            _ = Task.Run(() => warmSession.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(warmSession.Id, Symbol, DataFlags.Trades,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);
            await WaitUntil(() => w.Manager.IsSubscribedFor(SecurityId), TimeSpan.FromSeconds(1));

            w.Group.OnTrade(SecurityId, price: 100, quantity: 5, tradeId: 1, sendingTimeNs: 0);
            w.Group.OnBatchComplete();

            // Wait for trade ring to populate.
            await WaitUntil(() => w.Group.RecentTrades.ContainsKey(SecurityId)
                                  && w.Group.RecentTrades[SecurityId].AsSpan().Length > 0,
                            TimeSpan.FromSeconds(1));

            // Now subscribe a NEW client with only Trades flag (no Book/Mbp).
            // Must receive CandleSnapshot so the chart panel works.
            var tradesOnlyRec = new RecordingWebSocket();
            var tradesOnlySession = new ClientSession(tradesOnlyRec, channelCapacity: 64);
            w.Manager.RegisterClient(tradesOnlySession);
            _ = Task.Run(() => tradesOnlySession.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(tradesOnlySession.Id, Symbol, DataFlags.Trades,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);

            await WaitUntil(() => tradesOnlyRec.HasMessageType(MessageType.CandleSnapshot), TimeSpan.FromSeconds(2));

            // Verify Trades-only subscriber also gets the Trade history snapshot.
            Assert.True(tradesOnlyRec.CountByType(MessageType.Trade) >= 1,
                "Trades-only subscriber should receive trade history snapshot");
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task TradeHistorySnapshot_OnlySentWithTradesFlag()
    {
        var w = NewWiring();
        w.Group.StartBroadcaster(0);
        try
        {
            // Pre-seed the trade ring by subscribing a "warm" client first that
            // requests Trades. After Phase C the ring requires an active
            // subscriber to hydrate.
            var warmRec = new RecordingWebSocket();
            var warmSession = new ClientSession(warmRec, channelCapacity: 64);
            w.Manager.RegisterClient(warmSession);
            _ = Task.Run(() => warmSession.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(warmSession.Id, Symbol, DataFlags.Trades,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);
            await WaitUntil(() => w.Manager.IsSubscribedFor(SecurityId), TimeSpan.FromSeconds(1));

            w.Group.OnTrade(SecurityId, price: 100, quantity: 5, tradeId: 1, sendingTimeNs: 0);
            w.Group.OnBatchComplete();
            await WaitUntil(() => w.Group.RecentTrades.ContainsKey(SecurityId)
                                  && w.Group.RecentTrades[SecurityId].AsSpan().Length > 0,
                            TimeSpan.FromSeconds(1));

            // Now subscribe a NEW client without the Trades flag — must NOT get
            // Trade frames during snapshot replay (BookSnapshot still arrives).
            var noTradesRec = new RecordingWebSocket();
            var noTradesSession = new ClientSession(noTradesRec, channelCapacity: 64);
            w.Manager.RegisterClient(noTradesSession);
            _ = Task.Run(() => noTradesSession.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(noTradesSession.Id, Symbol, DataFlags.Book,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);

            await WaitUntil(() => noTradesRec.HasMessageType(MessageType.BookSnapshot), TimeSpan.FromSeconds(2));
            await Task.Delay(100); // allow any rogue Trade frames a chance to arrive
            Assert.Equal(0, noTradesRec.CountByType(MessageType.Trade));

            // Subscribe a third client WITH Trades — MUST receive the historical
            // Trade frame from the snapshot replay.
            var withTradesRec = new RecordingWebSocket();
            var withTradesSession = new ClientSession(withTradesRec, channelCapacity: 64);
            w.Manager.RegisterClient(withTradesSession);
            _ = Task.Run(() => withTradesSession.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(withTradesSession.Id, Symbol, DataFlags.Book | DataFlags.Trades,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);

            await WaitUntil(() => withTradesRec.CountByType(MessageType.Trade) >= 1, TimeSpan.FromSeconds(2));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    // --- helpers ---

    private static (SubscriptionManager Manager, GroupConflationHandler Group, BookManager BookManager) NewWiring()
    {
        var manager = new SubscriptionManager();
        var group = manager.CreateGroupHandler();
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var staleBuffer = new StaleMboBuffer(NullLogger.Instance);
        var book = new BookManager(stateRegistry: registry, staleBuffer: staleBuffer);
        group.SetBookManager(book);

        var symbols = new SymbolRegistry();
        RegisterSymbol(symbols, Symbol, SecurityId);

        manager.SetDataSources(
            new[] { book },
            new[] { new MarketDataManager(stateRegistry: registry) },
            symbols,
            new[] { group });
        manager.SetReady();

        return (manager, group, book);
    }

    private static OrderBookEntry NewEntry(ulong orderId, long price, long qty, BookSideType side = BookSideType.Bid) => new()
    {
        OrderId = orderId,
        Price = price,
        Quantity = qty,
        Side = side,
    };

    private static void RegisterSymbol(SymbolRegistry registry, string symbol, ulong securityId)
    {
        var bySymbol = (ConcurrentDictionary<string, ulong>)typeof(SymbolRegistry)
            .GetField("_bySymbol", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(registry)!;
        var byId = (ConcurrentDictionary<ulong, string>)typeof(SymbolRegistry)
            .GetField("_byId", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(registry)!;
        bySymbol[symbol] = securityId;
        byId[securityId] = symbol;
    }

    private static async Task WaitUntil(Func<bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate()) return;
            await Task.Delay(20);
        }
        Assert.True(predicate(), "Timed out waiting for condition.");
    }
}

internal static class SubscriptionManagerTestExtensions
{
    public static bool IsSubscribedFor(this SubscriptionManager m, ulong secId)
        => (bool)typeof(SubscriptionManager)
            .GetMethod("IsSubscribed", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(m, new object[] { secId })!;
}
