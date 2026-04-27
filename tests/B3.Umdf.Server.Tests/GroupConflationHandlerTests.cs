using System.Collections.Concurrent;
using System.Reflection;
using B3.Umdf.Book;
using B3.Umdf.Server;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server.Tests;

public class GroupConflationHandlerTests
{
    private const ulong SecurityId = 1001;
    private const string Symbol = "TEST1";

    [Fact]
    public async Task Fanout_SkipsBatchesAtOrBelowSubscriptionCutoff()
    {
        var wiring = NewWiring(channelCapacity: 64);
        wiring.Group.StartBroadcaster(groupId: 0);
        try
        {
            wiring.Manager.HandleSubscribe(
                wiring.Session.Id,
                Symbol,
                DataFlags.Book,
                wiring.BookManager,
                wiring.Group,
                bookBatchCutoffSequence: 1);

            int afterSnapshotDepth = wiring.Session.QueueDepth;
            var book = new OrderBook(SecurityId);

            var staleForSubscriber = NewEntry(orderId: 10, price: 1000, qty: 10);
            wiring.Group.OnOrderAdded(book, in staleForSubscriber);
            wiring.Group.OnBatchComplete();

            await Task.Delay(100);
            Assert.Equal(afterSnapshotDepth, wiring.Session.QueueDepth);

            var liveForSubscriber = NewEntry(orderId: 10, price: 1000, qty: 20);
            wiring.Group.OnOrderUpdated(book, in liveForSubscriber);
            wiring.Group.OnBatchComplete();

            await WaitUntil(() => wiring.Session.QueueDepth > afterSnapshotDepth, TimeSpan.FromSeconds(2));
        }
        finally
        {
            wiring.Group.StopBroadcaster();
            wiring.Manager.Dispose();
        }
    }

    [Fact]
    public void HandleSubscribe_DoesNotActivate_WhenSubscribeOkCannotEnqueue()
    {
        var wiring = NewWiring(channelCapacity: 64, maxPendingBytes: 20);
        try
        {
            wiring.Manager.HandleSubscribe(
                wiring.Session.Id,
                Symbol,
                DataFlags.Book,
                wiring.BookManager,
                wiring.Group,
                bookBatchCutoffSequence: 0);

            Assert.Null(wiring.Manager.GetSubscribers(SecurityId));
            Assert.True(wiring.Session.CancellationToken.IsCancellationRequested);
        }
        finally
        {
            wiring.Manager.Dispose();
        }
    }

    [Fact]
    public void HandleSubscribe_RollsBackActivation_WhenSnapshotCannotEnqueue()
    {
        var wiring = NewWiring(channelCapacity: 64, maxPendingBytes: 24);
        try
        {
            wiring.Manager.HandleSubscribe(
                wiring.Session.Id,
                Symbol,
                DataFlags.Book,
                wiring.BookManager,
                wiring.Group,
                bookBatchCutoffSequence: 0);

            Assert.Null(wiring.Manager.GetSubscribers(SecurityId));
            Assert.True(wiring.Session.CancellationToken.IsCancellationRequested);
        }
        finally
        {
            wiring.Manager.Dispose();
        }
    }

    private static (SubscriptionManager Manager, GroupConflationHandler Group, BookManager BookManager, ClientSession Session)
        NewWiring(int channelCapacity, long maxPendingBytes = 0)
    {
        var manager = new SubscriptionManager();
        var group = manager.CreateGroupHandler();
        var stateRegistry = new SymbolStateRegistry(NullLogger.Instance);
        var bookManager = NewBookManager(stateRegistry);
        group.SetBookManager(bookManager);

        var symbolRegistry = new SymbolRegistry();
        RegisterSymbol(symbolRegistry, Symbol, SecurityId);
        manager.SetDataSources(
            new[] { bookManager },
            new[] { new MarketDataManager(stateRegistry: stateRegistry) },
            symbolRegistry,
            new[] { group });
        manager.SetReady();

        var session = new ClientSession(
            new FakeWebSocket(),
            channelCapacity: channelCapacity,
            maxPendingBytes: maxPendingBytes);
        manager.RegisterClient(session);

        return (manager, group, bookManager, session);
    }

    private static BookManager NewBookManager(SymbolStateRegistry registry)
    {
        var staleBuffer = new StaleMboBuffer(NullLogger.Instance);
        return new BookManager(stateRegistry: registry, staleBuffer: staleBuffer);
    }

    private static OrderBookEntry NewEntry(ulong orderId, long price, long qty) => new()
    {
        OrderId = orderId,
        Price = price,
        Quantity = qty,
        SecurityId = SecurityId,
        Side = BookSideType.Bid,
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
