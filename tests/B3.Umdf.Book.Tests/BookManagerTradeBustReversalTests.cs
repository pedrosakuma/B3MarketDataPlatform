using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// P1 — Trade bust state reversal (B3 BinaryUMDF v2.2.0 §10).
///
/// Pins the BookManager-side reversal of <c>TradeBust_57</c>: the busted
/// trade is removed from the per-symbol recent-trades index, the
/// 1-second OHLCV bucket containing it is recomputed (or popped when the
/// bust was the only contributor), and the cached LastTradePrice falls
/// back to the most recent surviving trade. Bust-fanout via
/// <see cref="IBookEventHandler.OnTradeBust"/> is preserved.
/// </summary>
public class BookManagerTradeBustReversalTests
{
    private const ulong SecurityId = 9001;
    private const long Ts = 1_700_000_000L * 1_000_000_000L; // ns @ 2023-11-14T22:13:20Z

    private sealed class Recorder : IBookEventHandler
    {
        public int TradeCount;
        public int BustCount;
        public (ulong sec, long price, long qty, long id) LastBust;

        public void OnOrderAdded(OrderBook book, in OrderBookEntry entry) { }
        public void OnOrderUpdated(OrderBook book, in OrderBookEntry entry) { }
        public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side) { }
        public void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs) => TradeCount++;
        public void OnBookCleared(ulong securityId, BookClearSide side) { }
        public void OnTradeBust(ulong securityId, long price, long quantity, long tradeId)
        {
            BustCount++;
            LastBust = (securityId, price, quantity, tradeId);
        }
    }

    private static (BookManager bm, Recorder rec) NewBookManager()
    {
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var staleBuffer = new StaleMboBuffer(NullLogger.Instance);
        var rec = new Recorder();
        var bm = new BookManager(eventHandler: rec, stateRegistry: registry, staleBuffer: staleBuffer);
        return (bm, rec);
    }

    [Fact]
    public void Bust_RecentTrade_RestoresLastPriceToPriorTrade()
    {
        var (bm, _) = NewBookManager();

        bm.RecordTrade(SecurityId, price: 100_0000, quantity: 10, tradeId: 1, sendingTimeNs: Ts);
        bm.RecordTrade(SecurityId, price: 101_0000, quantity:  5, tradeId: 2, sendingTimeNs: Ts);

        Assert.True(bm.TryGetTradeState(SecurityId, out var state));
        Assert.Equal(101_0000L, state!.LastTradePrice);

        bm.ApplyTradeBustToState(SecurityId, tradeId: 2);

        Assert.Equal(100_0000L, state.LastTradePrice);
        Assert.Equal(1, bm.TradeBustsApplied);
        Assert.Equal(0, bm.UnknownTradeBusts);

        // Ring entry for the busted trade is flagged so snapshot history skips it.
        var slots = state.Ring.AsSpan();
        Assert.Equal(2, slots.Length);
        Assert.Equal(0, slots[0].Busted);
        Assert.Equal(1, slots[1].Busted);
    }

    [Fact]
    public void Bust_AdjustsCurrentMinuteCandle_OHLCV()
    {
        var (bm, _) = NewBookManager();

        // Three trades in the same 1-second bucket: 100, 105 (high), 99 (low → busted).
        bm.RecordTrade(SecurityId, price: 100_0000, quantity: 10, tradeId: 1, sendingTimeNs: Ts);
        bm.RecordTrade(SecurityId, price: 105_0000, quantity:  4, tradeId: 2, sendingTimeNs: Ts);
        bm.RecordTrade(SecurityId, price:  99_0000, quantity:  6, tradeId: 3, sendingTimeNs: Ts);

        Assert.True(bm.TryGetTradeState(SecurityId, out var state));
        var before = state!.Candles.GetLatest()!.Value;
        Assert.Equal(20L, before.Volume);
        Assert.Equal(105_0000L, before.High);
        Assert.Equal( 99_0000L, before.Low);

        bm.ApplyTradeBustToState(SecurityId, tradeId: 3);

        var after = state.Candles.GetLatest()!.Value;
        Assert.Equal(14L, after.Volume);             // 99-print volume removed
        Assert.Equal(105_0000L, after.High);
        Assert.Equal(100_0000L, after.Low);          // low rebuilt from {100, 105}
        Assert.Equal(100_0000L, after.Open);         // first remaining trade is open (no prior bucket)
        Assert.Equal(105_0000L, after.Close);        // last surviving trade in bucket
        Assert.Equal(1, bm.TradeBustsApplied);
    }

    [Fact]
    public void Bust_OfOnlyTradeInLatestBucket_PopsBucket()
    {
        var (bm, _) = NewBookManager();
        long ts2 = Ts + 1_000_000_000L; // next second

        bm.RecordTrade(SecurityId, price: 100_0000, quantity: 10, tradeId: 1, sendingTimeNs: Ts);
        bm.RecordTrade(SecurityId, price: 105_0000, quantity:  4, tradeId: 2, sendingTimeNs: ts2);

        Assert.True(bm.TryGetTradeState(SecurityId, out var state));
        Assert.Equal(2, state!.Candles.Count);

        bm.ApplyTradeBustToState(SecurityId, tradeId: 2);

        Assert.Equal(1, state.Candles.Count);                // latest bucket popped
        Assert.Equal(100_0000L, state.LastTradePrice);       // falls back to prior trade
        Assert.Equal(100_0000L, state.Candles.LastClose);
    }

    [Fact]
    public void Bust_UnknownTradeId_IncrementsCounter_AndDoesNotThrow()
    {
        var (bm, _) = NewBookManager();
        bm.RecordTrade(SecurityId, price: 100_0000, quantity: 10, tradeId: 1, sendingTimeNs: Ts);

        bm.ApplyTradeBustToState(SecurityId, tradeId: 9_999); // never seen

        Assert.Equal(1, bm.UnknownTradeBusts);
        Assert.Equal(0, bm.TradeBustsApplied);
        Assert.Equal(0, bm.OutOfRetentionTradeBusts);

        Assert.True(bm.TryGetTradeState(SecurityId, out var state));
        Assert.Equal(100_0000L, state!.LastTradePrice);      // unchanged
        Assert.Equal(0, state.Ring.AsSpan()[0].Busted);
    }

    [Fact]
    public void Bust_UnknownSecurityId_IncrementsUnknown_AndIsNoOp()
    {
        var (bm, _) = NewBookManager();

        bm.ApplyTradeBustToState(securityId: 4242, tradeId: 1);

        Assert.Equal(1, bm.UnknownTradeBusts);
        Assert.Equal(0, bm.TradeBustsApplied);
        Assert.False(bm.TryGetTradeState(4242, out _));
    }

    [Fact]
    public void Bust_StillFires_OnTradeBust_ExactlyOnce()
    {
        var (bm, rec) = NewBookManager();
        bm.RecordTrade(SecurityId, price: 100_0000, quantity: 10, tradeId: 1, sendingTimeNs: Ts);

        // DispatchTradeBust is the single source of truth for the
        // post-routing bust path (applied by HandleTradeBust after wire
        // decode + RouteMbo). Verifies the OnTradeBust fanout still fires
        // exactly once per bust alongside the new state-reversal work.
        bm.DispatchTradeBust(SecurityId, price: 100_0000, quantity: 10, tradeId: 1);

        Assert.Equal(1, rec.BustCount);
        Assert.Equal((SecurityId, 100_0000L, 10L, 1L), rec.LastBust);
        Assert.Equal(1, bm.TradeBustsApplied);

        Assert.True(bm.TryGetTradeState(SecurityId, out var state));
        Assert.Equal(1, state!.Ring.AsSpan()[0].Busted);  // state was actually mutated
    }
}
