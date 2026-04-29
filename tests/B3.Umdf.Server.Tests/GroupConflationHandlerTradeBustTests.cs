using B3.Umdf.Book;
using B3.Umdf.Server;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// P12-7 — pins the documented behaviour of TradeBust_57 (B3 BinaryUMDF
/// v2.2.0 §10) end-to-end through <see cref="GroupConflationHandler"/>:
/// the busted trade is annotated in the per-security recent-trades ring,
/// candle aggregates are intentionally NOT rolled back, missing/stale
/// references are silent no-ops, and snapshot history skips busted slots
/// while live subscribers still receive a TradeBust frame.
/// </summary>
public class GroupConflationHandlerTradeBustTests
{
    private const ulong SecurityId = 9001;
    private const string Symbol = "TBUST";
    private const long Price = 100_000;
    private const long Qty = 5;

    [Fact]
    public void OnTradeBust_FlagsMatchingTradeInRing()
    {
        var w = NewWiringWithSubscriber();

        w.Group.OnTrade(SecurityId, Price, Qty, tradeId: 1, sendingTimeNs: 0);
        w.Group.OnTrade(SecurityId, Price + 1, Qty, tradeId: 2, sendingTimeNs: 0);
        w.Group.OnTrade(SecurityId, Price + 2, Qty, tradeId: 3, sendingTimeNs: 0);

        w.Group.OnTradeBust(SecurityId, Price + 1, Qty, tradeId: 2);

        Assert.True(w.Group.RecentTrades.TryGetValue(SecurityId, out var ring));
        var slots = ring!.AsSpan();
        Assert.Equal(3, slots.Length);
        Assert.Equal(0, slots[0].Busted);   // tradeId 1
        Assert.Equal(1, slots[1].Busted);   // tradeId 2 — busted
        Assert.Equal(0, slots[2].Busted);   // tradeId 3

        w.Manager.Dispose();
    }

    [Fact]
    public void OnTradeBust_UnknownSecurityId_IsSilentNoOp()
    {
        var manager = new SubscriptionManager();
        var group = manager.CreateGroupHandler();

        // No prior OnTrade for this securityId. Bust should not throw and
        // must not lazily allocate a ring (otherwise we'd leak an empty
        // ring per spurious bust).
        group.OnTradeBust(securityId: 12345, price: 0, quantity: 0, tradeId: 99);

        Assert.False(group.RecentTrades.ContainsKey(12345));

        manager.Dispose();
    }

    [Fact]
    public void OnTradeBust_UnknownTradeId_LeavesRingUntouched()
    {
        var w = NewWiringWithSubscriber();

        w.Group.OnTrade(SecurityId, Price, Qty, tradeId: 7, sendingTimeNs: 0);
        w.Group.OnTradeBust(SecurityId, Price, Qty, tradeId: 9_999);

        Assert.True(w.Group.RecentTrades.TryGetValue(SecurityId, out var ring));
        var slots = ring!.AsSpan();
        Assert.Single(slots);
        Assert.Equal(0, slots[0].Busted);

        w.Manager.Dispose();
    }

    [Fact]
    public void OnTradeBust_DoesNotAdjustCandleAggregator()
    {
        // Pins the explicit comment in GroupConflationHandler.OnTradeBust:
        // "Candle volumes are intentionally NOT adjusted (would require
        //  per-trade history we don't retain; a volume-only adjustment would
        //  still leave OHLC distorted)". Stat refresh is the responsibility
        // of the next ExecutionStatistics_55 message.
        var w = NewWiringWithSubscriber();

        long ts = 1_700_000_000L * 1_000_000_000L;
        w.Group.OnTrade(SecurityId, Price, quantity: 10, tradeId: 1, sendingTimeNs: ts);
        w.Group.OnTrade(SecurityId, Price, quantity:  4, tradeId: 2, sendingTimeNs: ts);

        Assert.True(w.Group.Candles.TryGetValue(SecurityId, out var agg));
        var beforeBust = agg!.GetCandles();
        long volumeBefore = beforeBust[^1].Volume;
        Assert.Equal(14L, volumeBefore);

        w.Group.OnTradeBust(SecurityId, Price, quantity: 4, tradeId: 2);

        var afterBust = agg!.GetCandles();
        Assert.Equal(beforeBust.Length, afterBust.Length);
        Assert.Equal(volumeBefore, afterBust[^1].Volume); // unchanged

        w.Manager.Dispose();
    }

    [Fact]
    public void SnapshotEmitter_SendTradeHistory_SkipsBustedSlots()
    {
        // Verifies the §10 snapshot guarantee: new subscribers must not see
        // busted trades in their initial history snapshot.
        var w = NewWiringWithSubscriber();

        w.Group.OnTrade(SecurityId, Price,     Qty, tradeId: 1, sendingTimeNs: 0);
        w.Group.OnTrade(SecurityId, Price + 1, Qty, tradeId: 2, sendingTimeNs: 0);
        w.Group.OnTrade(SecurityId, Price + 2, Qty, tradeId: 3, sendingTimeNs: 0);

        w.Group.OnTradeBust(SecurityId, Price + 1, Qty, tradeId: 2);

        var session = new ClientSession(
            new FakeWebSocket(),
            channelCapacity: 16,
            maxPendingBytes: 0);

        Assert.True(w.Group.RecentTrades.TryGetValue(SecurityId, out var ring));
        int depthBefore = session.QueueDepth;
        Assert.True(SnapshotEmitter.SendTradeHistory(session, SecurityId, ring!));

        // 3 trades captured, 1 busted → 2 frames enqueued (busted skipped).
        Assert.Equal(depthBefore + 2, session.QueueDepth);

        w.Manager.Dispose();
    }

    /// <summary>
    /// Builds a SubscriptionManager + GroupConflationHandler with one client
    /// already subscribed to <see cref="SecurityId"/>. Required after the
    /// Phase C optimization in <c>OnTrade</c> which short-circuits hydration
    /// of <c>RecentTrades</c>/<c>Candles</c> for unsubscribed symbols.
    /// </summary>
    private static (SubscriptionManager Manager, GroupConflationHandler Group, BookManager BookManager) NewWiringWithSubscriber()
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

        var session = new ClientSession(new FakeWebSocket(), channelCapacity: 64);
        manager.RegisterClient(session);
        _ = Task.Run(() => session.RunWriteLoopAsync());
        manager.HandleSubscribe(session.Id, Symbol, DataFlags.Book,
            book, group, bookBatchCutoffSequence: 0);

        return (manager, group, book);
    }

    private static void RegisterSymbol(SymbolRegistry registry, string symbol, ulong securityId)
    {
        var bySymbol = (System.Collections.Concurrent.ConcurrentDictionary<string, ulong>)typeof(SymbolRegistry)
            .GetField("_bySymbol", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(registry)!;
        var byId = (System.Collections.Concurrent.ConcurrentDictionary<ulong, string>)typeof(SymbolRegistry)
            .GetField("_byId", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(registry)!;
        bySymbol[symbol] = securityId;
        byId[securityId] = symbol;
    }
}
