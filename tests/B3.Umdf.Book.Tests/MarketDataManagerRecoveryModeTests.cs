using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

public class MarketDataManagerRecoveryModeTests
{
    [Fact]
    public void Channel_DefaultMode_NoRegistry()
    {
        var mdm = new MarketDataManager();
        Assert.Equal(RecoveryMode.Channel, mdm.RecoveryMode);
        Assert.Null(mdm.StateRegistry);
    }

    [Fact]
    public void PerSymbol_RequiresRegistry()
    {
        var ex = Assert.Throws<ArgumentNullException>(() =>
            new MarketDataManager(recoveryMode: RecoveryMode.PerSymbol));
        Assert.Equal("stateRegistry", ex.ParamName);
    }

    [Fact]
    public void PerSymbol_FullyConstructed()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var mdm = new MarketDataManager(stateRegistry: reg, recoveryMode: RecoveryMode.PerSymbol);
        Assert.Equal(RecoveryMode.PerSymbol, mdm.RecoveryMode);
        Assert.Same(reg, mdm.StateRegistry);
        Assert.Equal(0, mdm.DroppedDuplicateStats);
        Assert.Equal(0, mdm.LiveResyncs);
    }

    [Fact]
    public void StatsRegistry_FirstObserve_AcceptsAsBaseline()
    {
        // AcceptFirst policy: cold-start applies immediately as baseline.
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var r = reg.Observe(securityId: 1, SymbolGapKind.OpeningPrice, 100);
        Assert.Equal(SymbolStateRegistry.ObserveAction.Apply, r.Action);
        Assert.Equal(SymbolState.Healthy, r.NewState);
    }

    [Fact]
    public void StatsRegistry_GapInHealthy_AppliesAsLiveResync()
    {
        // NextMessage policy: gap on Healthy → still Apply (with GapSize > 0).
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        reg.Observe(1, SymbolGapKind.HighPrice, 10); // baseline
        var r = reg.Observe(1, SymbolGapKind.HighPrice, 15); // gap of 4
        Assert.Equal(SymbolStateRegistry.ObserveAction.Apply, r.Action);
        Assert.Equal(SymbolState.Healthy, r.NewState);
        Assert.Equal(4u, r.GapSize);
    }

    [Fact]
    public void StatsRegistry_DuplicateRptSeq_Drops()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        reg.Observe(1, SymbolGapKind.LowPrice, 50);
        var r = reg.Observe(1, SymbolGapKind.LowPrice, 50); // duplicate
        Assert.Equal(SymbolStateRegistry.ObserveAction.Drop, r.Action);
    }

    [Fact]
    public void StatsRegistry_LowerRptSeq_Drops()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        reg.Observe(1, SymbolGapKind.LastTradePrice, 50);
        var r = reg.Observe(1, SymbolGapKind.LastTradePrice, 40); // older
        Assert.Equal(SymbolStateRegistry.ObserveAction.Drop, r.Action);
    }

    [Fact]
    public void DifferentKinds_PerSymbol_AreIndependent()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        // OpeningPrice baseline for sec 1
        reg.Observe(1, SymbolGapKind.OpeningPrice, 100);
        // HighPrice for SAME security is independent — first observe is also baseline
        var r = reg.Observe(1, SymbolGapKind.HighPrice, 5);
        Assert.Equal(SymbolStateRegistry.ObserveAction.Apply, r.Action);
    }
}
