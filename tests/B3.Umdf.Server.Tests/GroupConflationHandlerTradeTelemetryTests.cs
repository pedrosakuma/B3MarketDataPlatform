using B3.Umdf.Book;
using B3.Umdf.Server;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Pins the trade-conflation telemetry counters surfaced by
/// <see cref="GroupConflationHandler.TradesReceivedTotal"/> /
/// <see cref="GroupConflationHandler.TradesEmittedTotal"/>. These exist so
/// operators can compute the (secId,price) coalescing hit rate in production:
/// hitRate = 1 − Emitted/Received. They drive whether the dictionary-based
/// trade conflation is paying for itself in a given workload.
/// </summary>
public class GroupConflationHandlerTradeTelemetryTests
{
    private const ulong SecurityId = 7777;
    private const string Symbol = "TRDTL";

    [Fact]
    public void OnTrade_OnlyIncrementsReceived_WhenSubscribed()
    {
        var manager = new SubscriptionManager();
        var group = manager.CreateGroupHandler();

        // No subscriber → BufferTrade short-circuits via IsSubscribed; counter stays 0.
        group.OnTrade(SecurityId, price: 100, quantity: 5, tradeId: 1, sendingTimeNs: 0);
        Assert.Equal(0, group.TradesReceivedTotal);
        Assert.Equal(0, group.TradesEmittedTotal);

        manager.Dispose();
    }

    [Fact]
    public void Conflation_DistinctPrices_EmitsOnePerPrice_NoCoalescing()
    {
        var w = NewWiring();
        try
        {
            // Need a subscriber so OnTrade reaches BufferTrade.
            SubscribeOne(w);

            // 3 trades, 3 distinct prices → 3 received, 3 emitted (no coalescing).
            w.Group.OnTrade(SecurityId, price: 100, quantity: 5, tradeId: 1, sendingTimeNs: 0);
            w.Group.OnTrade(SecurityId, price: 101, quantity: 6, tradeId: 2, sendingTimeNs: 0);
            w.Group.OnTrade(SecurityId, price: 102, quantity: 7, tradeId: 3, sendingTimeNs: 0);
            w.Group.OnBatchComplete();

            Assert.Equal(3, w.Group.TradesReceivedTotal);
            Assert.Equal(3, w.Group.TradesEmittedTotal);
        }
        finally
        {
            w.Manager.Dispose();
        }
    }

    [Fact]
    public void Conflation_SamePrice_CoalescesToSingleEmitted()
    {
        var w = NewWiring();
        try
        {
            SubscribeOne(w);

            // 5 trades, all at the same (secId, price) → 5 received, 1 emitted.
            for (int i = 0; i < 5; i++)
                w.Group.OnTrade(SecurityId, price: 100, quantity: 1, tradeId: 100 + i, sendingTimeNs: 0);
            w.Group.OnBatchComplete();

            Assert.Equal(5, w.Group.TradesReceivedTotal);
            Assert.Equal(1, w.Group.TradesEmittedTotal);
        }
        finally
        {
            w.Manager.Dispose();
        }
    }

    [Fact]
    public void Conflation_AccumulatesAcrossBatches()
    {
        var w = NewWiring();
        try
        {
            SubscribeOne(w);

            // Batch 1: 2 trades at price 100 (1 emit), 1 at price 101 (1 emit) → +3 recv, +2 emit
            w.Group.OnTrade(SecurityId, 100, 1, 1, 0);
            w.Group.OnTrade(SecurityId, 100, 1, 2, 0);
            w.Group.OnTrade(SecurityId, 101, 1, 3, 0);
            w.Group.OnBatchComplete();

            // Batch 2: 1 trade at price 100 (1 emit) → +1 recv, +1 emit
            w.Group.OnTrade(SecurityId, 100, 1, 4, 0);
            w.Group.OnBatchComplete();

            Assert.Equal(4, w.Group.TradesReceivedTotal);
            Assert.Equal(3, w.Group.TradesEmittedTotal);
        }
        finally
        {
            w.Manager.Dispose();
        }
    }

    [Fact]
    public void OnForwardTrade_AlsoIncrementsCounters()
    {
        var w = NewWiring();
        try
        {
            SubscribeOne(w);

            w.Group.OnForwardTrade(SecurityId, price: 100, quantity: 5, tradeId: 1, sendingTimeNs: 0);
            w.Group.OnBatchComplete();

            Assert.Equal(1, w.Group.TradesReceivedTotal);
            Assert.Equal(1, w.Group.TradesEmittedTotal);
        }
        finally
        {
            w.Manager.Dispose();
        }
    }

    [Fact]
    public void OnTrade_ColdSymbol_DoesNotHydrateRing_ButStillAggregatesCandle()
    {
        // Phase C invariant (post-revision): when no client is subscribed, OnTrade
        // skips the live-tape ring + buffer fan-out, but STILL aggregates candles
        // and updates InfoSnapshot.LastTradePrice. Late subscribers must not see
        // gaps in chart history nor a stale last-print, even on cold symbols.
        var w = NewWiring();
        try
        {
            // Trade arrives before any subscribe.
            w.Group.OnTrade(SecurityId, price: 100, quantity: 5, tradeId: 1, sendingTimeNs: 0);
            w.Group.OnBatchComplete();

            // Live-tape side: skipped.
            Assert.False(w.Group.RecentTrades.ContainsKey(SecurityId));
            Assert.Equal(0, w.Group.TradesReceivedTotal);
            Assert.Equal(0, w.Group.TradesEmittedTotal);

            // Aggregation side: still happens.
            Assert.True(w.Group.Candles.ContainsKey(SecurityId),
                "Candles must aggregate for cold symbols so chart history has no gaps for late subscribers.");
        }
        finally
        {
            w.Manager.Dispose();
        }
    }

    [Fact]
    public void OnForwardTrade_ColdSymbol_DoesNotHydrateRing_ButStillAggregatesCandle()
    {
        // Symmetric to OnTrade_ColdSymbol_DoesNotHydrateRing_ButStillAggregatesCandle:
        // Forward trades (replayed during snapshot recovery / mid-session catch-up)
        // also feed the candle aggregator + InfoSnapshot last-price even when no
        // client is subscribed. The ring + buffer fan-out are skipped (best-effort
        // history). Late subscribers must not see chart gaps regardless of which
        // path delivered the trade.
        var w = NewWiring();
        try
        {
            w.Group.OnForwardTrade(SecurityId, price: 100, quantity: 5, tradeId: 1, sendingTimeNs: 0);
            w.Group.OnBatchComplete();

            // Live-tape side: skipped.
            Assert.False(w.Group.RecentTrades.ContainsKey(SecurityId));
            Assert.Equal(0, w.Group.TradesReceivedTotal);
            Assert.Equal(0, w.Group.TradesEmittedTotal);

            // Aggregation side: still happens.
            Assert.True(w.Group.Candles.ContainsKey(SecurityId),
                "Candles must aggregate for cold symbols on the forward-trade path too.");
        }
        finally
        {
            w.Manager.Dispose();
        }
    }

    [Fact]
    public void OnTrade_WarmSymbol_HydratesRingFromFirstSubscriber()
    {
        // Documents the Phase C trade-off: a future subscriber to a previously
        // cold symbol sees no trade history until the FIRST trade after subscribe.
        var w = NewWiring();
        try
        {
            // 5 trades on a cold symbol → no hydration.
            for (int i = 0; i < 5; i++)
                w.Group.OnTrade(SecurityId, price: 100, quantity: 1, tradeId: i, sendingTimeNs: 0);
            w.Group.OnBatchComplete();
            Assert.False(w.Group.RecentTrades.ContainsKey(SecurityId));

            // Subscribe.
            SubscribeOne(w);

            // Next trade hydrates the ring.
            w.Group.OnTrade(SecurityId, price: 100, quantity: 1, tradeId: 99, sendingTimeNs: 0);
            w.Group.OnBatchComplete();

            Assert.True(w.Group.RecentTrades.TryGetValue(SecurityId, out var ring));
            Assert.Single(ring!.AsSpan().ToArray()); // only the post-subscribe trade
        }
        finally
        {
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

    private static void SubscribeOne((SubscriptionManager Manager, GroupConflationHandler Group, BookManager BookManager) w)
    {
        var rec = new RecordingWebSocket();
        var session = new ClientSession(rec, channelCapacity: 64);
        w.Manager.RegisterClient(session);
        _ = Task.Run(() => session.RunWriteLoopAsync());
        w.Manager.HandleSubscribe(session.Id, Symbol, DataFlags.Book,
            w.BookManager, w.Group, bookBatchCutoffSequence: 0);
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
