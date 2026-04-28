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
    private const long Price = 100_000;
    private const long Qty = 5;

    [Fact]
    public void OnTradeBust_FlagsMatchingTradeInRing()
    {
        var manager = new SubscriptionManager();
        var group = manager.CreateGroupHandler();

        group.OnTrade(SecurityId, Price, Qty, tradeId: 1, sendingTimeNs: 0);
        group.OnTrade(SecurityId, Price + 1, Qty, tradeId: 2, sendingTimeNs: 0);
        group.OnTrade(SecurityId, Price + 2, Qty, tradeId: 3, sendingTimeNs: 0);

        group.OnTradeBust(SecurityId, Price + 1, Qty, tradeId: 2);

        Assert.True(group.RecentTrades.TryGetValue(SecurityId, out var ring));
        var slots = ring!.AsSpan();
        Assert.Equal(3, slots.Length);
        Assert.Equal(0, slots[0].Busted);   // tradeId 1
        Assert.Equal(1, slots[1].Busted);   // tradeId 2 — busted
        Assert.Equal(0, slots[2].Busted);   // tradeId 3

        manager.Dispose();
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
        var manager = new SubscriptionManager();
        var group = manager.CreateGroupHandler();

        group.OnTrade(SecurityId, Price, Qty, tradeId: 7, sendingTimeNs: 0);
        group.OnTradeBust(SecurityId, Price, Qty, tradeId: 9_999);

        Assert.True(group.RecentTrades.TryGetValue(SecurityId, out var ring));
        var slots = ring!.AsSpan();
        Assert.Single(slots);
        Assert.Equal(0, slots[0].Busted);

        manager.Dispose();
    }

    [Fact]
    public void OnTradeBust_DoesNotAdjustCandleAggregator()
    {
        // Pins the explicit comment in GroupConflationHandler.OnTradeBust:
        // "Candle volumes are intentionally NOT adjusted (would require
        //  per-trade history we don't retain; a volume-only adjustment would
        //  still leave OHLC distorted)". Stat refresh is the responsibility
        // of the next ExecutionStatistics_55 message.
        var manager = new SubscriptionManager();
        var group = manager.CreateGroupHandler();

        long ts = 1_700_000_000L * 1_000_000_000L;
        group.OnTrade(SecurityId, Price, quantity: 10, tradeId: 1, sendingTimeNs: ts);
        group.OnTrade(SecurityId, Price, quantity:  4, tradeId: 2, sendingTimeNs: ts);

        Assert.True(group.Candles.TryGetValue(SecurityId, out var agg));
        var beforeBust = agg!.GetCandles();
        long volumeBefore = beforeBust[^1].Volume;
        Assert.Equal(14L, volumeBefore);

        group.OnTradeBust(SecurityId, Price, quantity: 4, tradeId: 2);

        var afterBust = agg!.GetCandles();
        Assert.Equal(beforeBust.Length, afterBust.Length);
        Assert.Equal(volumeBefore, afterBust[^1].Volume); // unchanged

        manager.Dispose();
    }

    [Fact]
    public void SnapshotEmitter_SendTradeHistory_SkipsBustedSlots()
    {
        // Verifies the §10 snapshot guarantee: new subscribers must not see
        // busted trades in their initial history snapshot.
        var manager = new SubscriptionManager();
        var group = manager.CreateGroupHandler();

        group.OnTrade(SecurityId, Price,     Qty, tradeId: 1, sendingTimeNs: 0);
        group.OnTrade(SecurityId, Price + 1, Qty, tradeId: 2, sendingTimeNs: 0);
        group.OnTrade(SecurityId, Price + 2, Qty, tradeId: 3, sendingTimeNs: 0);

        group.OnTradeBust(SecurityId, Price + 1, Qty, tradeId: 2);

        var session = new ClientSession(
            new FakeWebSocket(),
            channelCapacity: 16,
            maxPendingBytes: 0);

        Assert.True(group.RecentTrades.TryGetValue(SecurityId, out var ring));
        int depthBefore = session.QueueDepth;
        Assert.True(SnapshotEmitter.SendTradeHistory(session, SecurityId, ring!));

        // 3 trades captured, 1 busted → 2 frames enqueued (busted skipped).
        Assert.Equal(depthBefore + 2, session.QueueDepth);

        manager.Dispose();
    }
}
