using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Reflection;
using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Pins behavior of the configurable server-side temporal flush window
/// (<c>UMDF_SERVER_FLUSH_WINDOW_MS</c>) inside <see cref="GroupConflationHandler"/>.
/// Default 0 = legacy per-packet flush (no behavior change). > 0 = debounce-style
/// deferral until WindowMs has elapsed since the first dirty event.
/// </summary>
public class GroupConflationHandlerFlushWindowTests
{
    private const ulong SecurityId = 7001;
    private const string Symbol = "WIN1";

    [Fact]
    public async Task WindowZero_FlushesEveryPacket_LegacyBehavior()
    {
        var w = NewWiring(flushWindowMs: 0);
        w.Group.StartBroadcaster(0);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);
            var (session, rec) = AddMboSubscriber(w);

            await WaitUntil(() => rec.HasMessageType(MessageType.BookSnapshot), TimeSpan.FromSeconds(2));
            int baseAdds = rec.CountByType(MessageType.OrderAdded);

            for (int i = 0; i < 3; i++)
            {
                w.Group.OnOrderAdded(book, NewEntry(orderId: (ulong)(100 + i), price: 1000, qty: 1));
                w.Group.OnBatchComplete();
            }

            await WaitUntil(() => rec.CountByType(MessageType.OrderAdded) == baseAdds + 3, TimeSpan.FromSeconds(2));
            Assert.Equal(0, w.Group.BuffersDeferredFlushCount);
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task Window_DefersFlushAcrossPackets_WithinWindow()
    {
        var w = NewWiring(flushWindowMs: 5_000);
        w.Group.StartBroadcaster(0);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);
            var (session, rec) = AddMboSubscriber(w);
            await WaitUntil(() => rec.HasMessageType(MessageType.BookSnapshot), TimeSpan.FromSeconds(2));

            // Multiple packets within the window: first dirty starts the timer,
            // subsequent OnBatchComplete invocations must defer (no flush).
            for (int i = 0; i < 5; i++)
            {
                w.Group.OnOrderAdded(book, NewEntry(orderId: (ulong)(200 + i), price: 1000, qty: 1));
                w.Group.OnBatchComplete();
            }

            Assert.True(w.Group.BuffersDeferredFlushCount >= 1,
                $"Expected deferred flushes, got {w.Group.BuffersDeferredFlushCount}");
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task Window_FlushesAfterWindowElapses_OnNextPacket()
    {
        var w = NewWiring(flushWindowMs: 30);
        w.Group.StartBroadcaster(0);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);
            var (session, rec) = AddMboSubscriber(w);
            await WaitUntil(() => rec.HasMessageType(MessageType.BookSnapshot), TimeSpan.FromSeconds(2));

            // Add an order; first OnBatchComplete defers (window not elapsed).
            w.Group.OnOrderAdded(book, NewEntry(orderId: 300, price: 1000, qty: 1));
            w.Group.OnBatchComplete();

            int beforeAdds = rec.CountByType(MessageType.OrderAdded);

            // Wait past the window, then trigger another OnBatchComplete to flush.
            Thread.Sleep(60);
            w.Group.OnBatchComplete();

            await WaitUntil(() => rec.CountByType(MessageType.OrderAdded) > beforeAdds, TimeSpan.FromSeconds(2));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task FlushIfDue_FlushesBufferedEvents_WhenWindowElapsed()
    {
        var w = NewWiring(flushWindowMs: 30);
        w.Group.StartBroadcaster(0);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);
            var (session, rec) = AddMboSubscriber(w);
            await WaitUntil(() => rec.HasMessageType(MessageType.BookSnapshot), TimeSpan.FromSeconds(2));

            int beforeAdds = rec.CountByType(MessageType.OrderAdded);

            w.Group.OnOrderAdded(book, NewEntry(orderId: 400, price: 1000, qty: 1));
            w.Group.OnBatchComplete(); // deferred

            // Idle path: no FlushIfDue effect yet (window not elapsed).
            ((IBookEventHandler)w.Group).FlushIfDue();
            Assert.Equal(beforeAdds, rec.CountByType(MessageType.OrderAdded));

            // After window elapses, FlushIfDue must publish.
            Thread.Sleep(60);
            ((IBookEventHandler)w.Group).FlushIfDue();
            await WaitUntil(() => rec.CountByType(MessageType.OrderAdded) > beforeAdds, TimeSpan.FromSeconds(2));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task Window_ConflatesLevelUpdates_LastWriteWinsAcrossPackets()
    {
        var w = NewWiring(flushWindowMs: 5_000);
        w.Group.StartBroadcaster(0);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);
            var (session, rec) = AddMbpSubscriber(w);
            await WaitUntil(() => rec.HasMessageType(MessageType.LevelSnapshot), TimeSpan.FromSeconds(2));
            int baseLU = rec.CountByType(MessageType.LevelUpdate);

            // 3 packets, each mutating the same level. Only one LevelUpdate should be
            // emitted at the forced flush below.
            for (int i = 0; i < 3; i++)
            {
                book.Bids.Add(NewEntry(orderId: (ulong)(500 + i), price: 1000, qty: 1));
                w.Group.OnPriceLevelChanged(book, BookSideType.Bid, 1000);
                w.Group.OnBatchComplete();
            }

            // Force flush via shutdown drain.
            ((IBookEventHandler)w.Group).FlushNow();

            await WaitUntil(() => rec.CountByType(MessageType.LevelUpdate) > baseLU, TimeSpan.FromSeconds(2));
            Assert.Equal(baseLU + 1, rec.CountByType(MessageType.LevelUpdate));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task FlushNow_DrainsBufferedEvents_OnShutdown()
    {
        var w = NewWiring(flushWindowMs: 5_000);
        w.Group.StartBroadcaster(0);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);
            var (session, rec) = AddMboSubscriber(w);
            await WaitUntil(() => rec.HasMessageType(MessageType.BookSnapshot), TimeSpan.FromSeconds(2));
            int baseAdds = rec.CountByType(MessageType.OrderAdded);

            w.Group.OnOrderAdded(book, NewEntry(orderId: 600, price: 1000, qty: 1));
            w.Group.OnBatchComplete(); // deferred

            ((IBookEventHandler)w.Group).FlushNow();
            await WaitUntil(() => rec.CountByType(MessageType.OrderAdded) > baseAdds, TimeSpan.FromSeconds(2));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task PreSnapshot_ForcesFlush_BeforeSubscribeRequestProcessing()
    {
        // Snapshot cutoff invariant: a deferred batch of deltas MUST flush BEFORE
        // ProcessOwnSubscribeRequests so the snapshot cutoff sequence excludes
        // the in-flight deltas. Otherwise a new subscriber would receive both the
        // snapshot reflecting state AFTER the deltas AND the deltas themselves
        // in a future batch.
        var w = NewWiring(flushWindowMs: 5_000);
        w.Group.StartBroadcaster(0);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);

            // Existing subscriber observes the deltas (no force-flush triggered
            // because no NEW pending subscribe at this point).
            var (oldSession, oldRec) = AddMboSubscriber(w);
            await WaitUntil(() => oldRec.HasMessageType(MessageType.BookSnapshot), TimeSpan.FromSeconds(2));
            int oldBaseAdds = oldRec.CountByType(MessageType.OrderAdded);

            w.Group.OnOrderAdded(book, NewEntry(orderId: 700, price: 1000, qty: 1));
            w.Group.OnBatchComplete(); // deferred (no pending subscribe yet)
            Assert.True(w.Group.BuffersDeferredFlushCount >= 1);

            long beforePublished = w.Group.LastPublishedBatchSequence;

            // Simulate a NEW subscriber routed via the per-group queue (production
            // path). Register the client first so HandleSubscribe finds it when
            // ProcessOwnSubscribeRequests drains the queue.
            var newRec = new RecordingWebSocket();
            var newSession = new ClientSession(newRec, channelCapacity: 256);
            w.Manager.RegisterClient(newSession);
            _ = Task.Run(() => newSession.RunWriteLoopAsync());
            w.Group.EnqueueRequest(newSession.Id, Symbol, DataFlags.Book, isGet: false);

            // OnBatchComplete now sees a pending subscribe + dirty buffer → forced flush.
            w.Group.OnBatchComplete();

            await WaitUntil(() => newRec.HasMessageType(MessageType.BookSnapshot), TimeSpan.FromSeconds(2));
            await WaitUntil(() => oldRec.CountByType(MessageType.OrderAdded) > oldBaseAdds, TimeSpan.FromSeconds(2));

            // Forced flush must have advanced the published sequence BEFORE the
            // snapshot was sent: any future delta (sequence > current) is the only
            // thing the new subscriber will see post-snapshot.
            Assert.True(w.Group.LastPublishedBatchSequence > beforePublished,
                $"Expected sequence to advance from {beforePublished}, got {w.Group.LastPublishedBatchSequence}");
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public void NegativeFlushWindow_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SubscriptionManager(serverFlushWindowMs: -1));
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private static (RecordingWebSocket Rec, ClientSession Session) AddMboSubscriberInternal(Wiring w)
    {
        var rec = new RecordingWebSocket();
        var session = new ClientSession(rec, channelCapacity: 256);
        w.Manager.RegisterClient(session);
        _ = Task.Run(() => session.RunWriteLoopAsync());
        w.Manager.HandleSubscribe(session.Id, Symbol, DataFlags.Book,
            w.BookManager, w.Group, bookBatchCutoffSequence: 0);
        return (rec, session);
    }

    private static (ClientSession, RecordingWebSocket) AddMboSubscriber(Wiring w)
    {
        var (rec, session) = AddMboSubscriberInternal(w);
        return (session, rec);
    }

    private static (ClientSession, RecordingWebSocket) AddMbpSubscriber(Wiring w)
    {
        var rec = new RecordingWebSocket();
        var session = new ClientSession(rec, channelCapacity: 256);
        w.Manager.RegisterClient(session);
        _ = Task.Run(() => session.RunWriteLoopAsync());
        w.Manager.HandleSubscribe(session.Id, Symbol, DataFlags.Mbp,
            w.BookManager, w.Group, bookBatchCutoffSequence: 0);
        return (session, rec);
    }

    private record Wiring(SubscriptionManager Manager, GroupConflationHandler Group, BookManager BookManager);

    private static Wiring NewWiring(int flushWindowMs)
    {
        var manager = new SubscriptionManager(serverFlushWindowMs: flushWindowMs);
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
        return new Wiring(manager, group, book);
    }

    private static OrderBookEntry NewEntry(ulong orderId, long price, long qty, BookSideType side = BookSideType.Bid) => new()
    {
        OrderId = orderId,
        Price = price,
        Quantity = qty,
        SecurityId = SecurityId,
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
