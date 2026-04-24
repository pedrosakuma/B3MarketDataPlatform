using B3.Umdf.Book;
using B3.Umdf.Mbo.Sbe.V16;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Tests for B3 BinaryUMDF v2.2.0 spec-compliance fixes:
///   - §14.3 TRADING_SESSION_CHANGE (event=4) end-of-day stats reset
///   - §6.5.5.1 SequenceVersion change resets per-symbol epoch
///   - §7.2 stale-version snapshots are silently skipped
/// </summary>
public class SpecComplianceTests
{
    // ── §14.3 — TRADING_SESSION_CHANGE (eventCode=4) ──────────────────────────

    [Fact]
    public void ResetSessionStatistics_ClearsStatFieldsAndWatermarks()
    {
        var info = new InstrumentInfo
        {
            Symbol = "PETR4",
            SecurityGroup = "EQT",
            LastTradePrice = 123450,
            LastTradeSize = 100,
            OpeningPrice = 120000,
            TheoreticalOpeningPrice = 121000,
            ClosingPrice = 122000,
            HighPrice = 125000,
            LowPrice = 119000,
            TradeVolume = 1_000_000,
            VwapPrice = 122500,
            NumberOfTrades = 500,
            NetChangeFromPrevDay = 1500,
            AuctionImbalanceSize = 250,
        };
        info.LastRptSeqLastTradePrice = 42;
        info.LastRptSeqOpeningPrice = 5;
        info.LastRptSeqHighPrice = 7;
        info.LastRptSeqExecutionStatistics = 10;

        info.ResetSessionStatistics();

        Assert.Null(info.LastTradePrice);
        Assert.Null(info.LastTradeSize);
        Assert.Null(info.OpeningPrice);
        Assert.Null(info.TheoreticalOpeningPrice);
        Assert.Null(info.ClosingPrice);
        Assert.Null(info.HighPrice);
        Assert.Null(info.LowPrice);
        Assert.Null(info.TradeVolume);
        Assert.Null(info.VwapPrice);
        Assert.Null(info.NumberOfTrades);
        Assert.Null(info.NetChangeFromPrevDay);
        Assert.Null(info.AuctionImbalanceSize);
        // Watermarks must be zeroed so post-reset rptSeq=1 is accepted.
        Assert.Equal(0u, info.LastRptSeqLastTradePrice);
        Assert.Equal(0u, info.LastRptSeqOpeningPrice);
        Assert.Equal(0u, info.LastRptSeqHighPrice);
        Assert.Equal(0u, info.LastRptSeqExecutionStatistics);
        // Identity / metadata MUST be preserved.
        Assert.Equal("PETR4", info.Symbol);
        Assert.Equal("EQT", info.SecurityGroup);
    }

    // ── §6.5.5.1 — SequenceVersion change ─────────────────────────────────────

    [Fact]
    public void OnSequenceVersionChanged_ZeroesAllStatWatermarks()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var manager = new MarketDataManager(stateRegistry: reg);
        var info = manager.GetOrCreateInfo(123);
        info.LastRptSeqOpeningPrice = 5;
        info.LastRptSeqHighPrice = 6;
        info.LastRptSeqLowPrice = 7;
        info.LastRptSeqLastTradePrice = 42;
        info.LastRptSeqExecutionStatistics = 10;
        info.LastRptSeqSecurityStatus = 11;
        // Stat values are preserved (mirror of OnSequenceReset rationale).
        info.LastTradePrice = 99000;

        manager.OnSequenceVersionChanged(newVersion: 2);

        Assert.Equal(0u, info.LastRptSeqOpeningPrice);
        Assert.Equal(0u, info.LastRptSeqHighPrice);
        Assert.Equal(0u, info.LastRptSeqLowPrice);
        Assert.Equal(0u, info.LastRptSeqLastTradePrice);
        Assert.Equal(0u, info.LastRptSeqExecutionStatistics);
        Assert.Equal(0u, info.LastRptSeqSecurityStatus);
        Assert.Equal(99000, info.LastTradePrice);
    }

    [Fact]
    public void BookManager_OnSequenceVersionChanged_TracksVersionAndResetsEpoch()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf);

        // Bootstrap a symbol to Healthy via a snapshot baseline.
        bm.BeginSnapshotHeader(secId: 100, lastRptSeq: 50, hasRptSeq: true, ordersExpected: 0);
        Assert.Equal(SymbolState.Healthy, reg.GetState(100, SymbolGapKind.Mbo));

        bm.OnSequenceVersionChanged(newVersion: 7);

        // Books cleared, registry epoch reset → symbol back to Unknown.
        Assert.Equal(SymbolState.Unknown, reg.GetState(100, SymbolGapKind.Mbo));
        // Internal version-tracking accessor exposed for SnapshotApplier gating.
        // (No public surface; verified indirectly via stale-version skip test below.)
    }

    // ── §7.2 — Stale-version snapshots skipped ────────────────────────────────

    [Fact]
    public void Snapshot_OlderSequenceVersion_IsSilentlySkipped()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf);

        // Advance the channel to SequenceVersion=5.
        bm.OnSequenceVersionChanged(newVersion: 5);
        Assert.Equal(0L, bm.SnapshotsRejectedStaleVersion);

        // A snapshot for the same symbol arriving with LastSequenceVersion=4
        // (older) must be skipped — the symbol must NOT transition to Healthy.
        bm.OnSnapshotHeaderForTest(securityId: 200, lastRptSeq: 100, ordersExpected: 0, lastSequenceVersion: 4);
        Assert.Equal(SymbolState.Unknown, reg.GetState(200, SymbolGapKind.Mbo));
        Assert.Equal(1L, bm.SnapshotsRejectedStaleVersion);

        // Same-version snapshot is accepted (heals immediately because ordersExpected=0).
        bm.OnSnapshotHeaderForTest(securityId: 200, lastRptSeq: 100, ordersExpected: 0, lastSequenceVersion: 5);
        Assert.Equal(SymbolState.Healthy, reg.GetState(200, SymbolGapKind.Mbo));
        Assert.Equal(1L, bm.SnapshotsRejectedStaleVersion);
    }

    [Fact]
    public void Snapshot_NoCurrentVersionTracked_SkipsVersionGate()
    {
        // When the channel has not yet observed a SequenceVersion (initial
        // bootstrap / tests that don't drive ChannelHandler), the version
        // gate must NOT reject snapshots — it has no baseline to compare to.
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf);

        bm.OnSnapshotHeaderForTest(securityId: 300, lastRptSeq: 50, ordersExpected: 0, lastSequenceVersion: 1);
        Assert.Equal(SymbolState.Healthy, reg.GetState(300, SymbolGapKind.Mbo));
        Assert.Equal(0L, bm.SnapshotsRejectedStaleVersion);
    }

    // ── §18 — TradeCondition / TrdSubType filter ──────────────────────────────

    [Fact]
    public void IsReportableTrade_FiltersOutOfSequenceAndLegTrades()
    {
        // Regular trade → reported.
        Assert.True(BookManager.IsReportableTrade((TradeCondition)0, subType: null));
        Assert.True(BookManager.IsReportableTrade(TradeCondition.RegularTrade, subType: null));

        // OutOfSequence flag → filtered.
        Assert.False(BookManager.IsReportableTrade(TradeCondition.OutOfSequence, subType: null));
        // OutOfSequence combined with another flag → still filtered.
        Assert.False(BookManager.IsReportableTrade(TradeCondition.OutOfSequence | TradeCondition.OpeningPrice, null));

        // LEG_TRADE sub-type → filtered (multi-leg synthetic, not a venue trade).
        Assert.False(BookManager.IsReportableTrade((TradeCondition)0, TrdSubType.LEG_TRADE));
    }

    // ── §10 — TradeBust_57 marks ring entries; snapshot skips them ───────────

    [Fact]
    public void TradeRingBuffer_MarkBust_FlagsMatchingTradeOnly()
    {
        var ring = new TradeRingBuffer(5);
        ring.Add(price: 100_0000, qty: 10, tradeId: 1001);
        ring.Add(price: 101_0000, qty: 20, tradeId: 1002);
        ring.Add(price: 102_0000, qty: 30, tradeId: 1003);

        Assert.True(ring.MarkBust(1002));

        var snap = ring.AsSpan();
        Assert.Equal(3, snap.Length);
        Assert.Equal(0, snap[0].Busted);
        Assert.Equal(1, snap[1].Busted);
        Assert.Equal(0, snap[2].Busted);
        Assert.Equal(1002L, snap[1].TradeId);
    }

    [Fact]
    public void TradeRingBuffer_MarkBust_UnknownTradeReturnsFalse()
    {
        var ring = new TradeRingBuffer(3);
        ring.Add(100, 1, tradeId: 1);
        Assert.False(ring.MarkBust(99999));
    }

    [Fact]
    public void TradeRingBuffer_MarkBust_IsIdempotent()
    {
        var ring = new TradeRingBuffer(3);
        ring.Add(100, 1, tradeId: 7);
        Assert.True(ring.MarkBust(7));
        Assert.True(ring.MarkBust(7)); // second call still finds it; remains busted
        Assert.Equal(1, ring.AsSpan()[0].Busted);
    }

    [Fact]
    public void TradeRingBuffer_MarkBust_BustedTradeEvictedAfterRingFills()
    {
        // Ring of 2 — once a 3rd trade arrives, oldest evicted and bust no longer found.
        var ring = new TradeRingBuffer(2);
        ring.Add(100, 1, tradeId: 1);
        ring.Add(101, 1, tradeId: 2);
        ring.Add(102, 1, tradeId: 3); // evicts tradeId=1
        Assert.False(ring.MarkBust(1));
        Assert.True(ring.MarkBust(2));
    }

    // ── §12.1 — MOA/MOC null-price orders go to per-side market tier ─────────

    [Fact]
    public void OrderBook_UpsertMarketOrder_AddsThenUpdates()
    {
        var book = new OrderBook(securityId: 42);
        Assert.True(book.UpsertMarketOrder(orderId: 1, BookSideType.Bid, quantity: 100, enteringFirm: 8));
        Assert.False(book.UpsertMarketOrder(orderId: 1, BookSideType.Bid, quantity: 250, enteringFirm: 8));
        Assert.Equal(1, book.MarketOrderCount(BookSideType.Bid));
        Assert.True(book.HasMarketOrders(BookSideType.Bid));
        Assert.False(book.HasMarketOrders(BookSideType.Ask));
        Assert.True(book.TryGetMarketOrder(1, BookSideType.Bid, out var got));
        Assert.Equal(250, got.Quantity);
    }

    [Fact]
    public void OrderBook_TryRemoveMarketOrderAnySide_ResolvesSide()
    {
        var book = new OrderBook(99);
        book.UpsertMarketOrder(orderId: 5, BookSideType.Ask, quantity: 50, enteringFirm: 1);
        Assert.True(book.TryRemoveMarketOrderAnySide(5, out var side));
        Assert.Equal(BookSideType.Ask, side);
        Assert.Equal(0, book.MarketOrderCount(BookSideType.Ask));
        Assert.False(book.TryRemoveMarketOrderAnySide(5, out _));
    }

    [Fact]
    public void OrderBook_Clear_AlsoClearsMarketTier()
    {
        var book = new OrderBook(7);
        book.UpsertMarketOrder(orderId: 1, BookSideType.Bid, quantity: 10, enteringFirm: 1);
        book.UpsertMarketOrder(orderId: 2, BookSideType.Ask, quantity: 20, enteringFirm: 1);
        book.Clear();
        Assert.Equal(0, book.MarketOrderCount(BookSideType.Bid));
        Assert.Equal(0, book.MarketOrderCount(BookSideType.Ask));
    }
}
