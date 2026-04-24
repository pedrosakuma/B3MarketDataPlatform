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
}
