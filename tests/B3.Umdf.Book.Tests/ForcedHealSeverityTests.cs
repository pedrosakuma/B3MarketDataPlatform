using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Coverage for the operator-visible severity gauges around the stuck-Stale
/// escape valve in <see cref="SymbolStateRegistry.HealFromSnapshot"/>. The
/// existing <c>StaleAuthoritativeResetCount</c> only counts events; these
/// gauges quantify <i>how bad</i> each event was so dashboards can alert on
/// magnitude rather than mere occurrence.
/// </summary>
public class ForcedHealSeverityTests
{
    private static SymbolStateRegistry NewRegistry()
        => new(NullLogger<SymbolStateRegistry>.Instance);

    [Fact]
    public void NoForcedHeal_GaugesAreZero()
    {
        var r = NewRegistry();
        Assert.Equal(0u, r.LastAuthoritativeResetUnsafeDelta);
        Assert.Equal(0u, r.MaxAuthoritativeResetUnsafeDelta);
        Assert.Equal(0UL, r.SumAuthoritativeResetUnsafeDelta);
        Assert.Equal(0u, r.LastAuthoritativeResetDiscardedTailDelta);
        Assert.Equal(0u, r.MaxAuthoritativeResetDiscardedTailDelta);
        Assert.Equal(0UL, r.SumAuthoritativeResetDiscardedTailDelta);
    }

    [Fact]
    public void ForcedHeal_PopulatesUnsafeAndDiscardedTailDeltas()
    {
        var r = NewRegistry();
        r.StaleEscapeTimeoutMs = 30;
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 100);
        r.Observe(1, SymbolGapKind.Mbo, 101);
        // Gap to 200 → MinHeal becomes 199, priorHighWater becomes 200.
        r.Observe(1, SymbolGapKind.Mbo, 200);
        Thread.Sleep(80);

        // Snapshot at 50 forces escape: unsafe = MinHeal(199)-snap(50)=149,
        // discarded tail = highWater(200)-snap(50)=150.
        var heal = r.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 50);
        Assert.True(heal.Accepted);

        Assert.Equal(1, r.StaleAuthoritativeResetCount);
        Assert.Equal(149u, r.LastAuthoritativeResetUnsafeDelta);
        Assert.Equal(149u, r.MaxAuthoritativeResetUnsafeDelta);
        Assert.Equal(149UL, r.SumAuthoritativeResetUnsafeDelta);
        Assert.Equal(150u, r.LastAuthoritativeResetDiscardedTailDelta);
        Assert.Equal(150u, r.MaxAuthoritativeResetDiscardedTailDelta);
        Assert.Equal(150UL, r.SumAuthoritativeResetDiscardedTailDelta);
    }

    [Fact]
    public void ForcedHeal_MaxRetainsLargestSpike_SumAccumulates()
    {
        var r = NewRegistry();
        r.StaleEscapeTimeoutMs = 30;

        // Symbol A: large delta.
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 100);
        r.Observe(1, SymbolGapKind.Mbo, 101);
        r.Observe(1, SymbolGapKind.Mbo, 1_001); // MinHeal=1000, high=1001
        Thread.Sleep(80);
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 1); // unsafe=999, tail=1000

        // Symbol B: small delta.
        r.HealFromSnapshot(2, SymbolGapKind.Mbo, snapshotRptSeq: 100);
        r.Observe(2, SymbolGapKind.Mbo, 101);
        r.Observe(2, SymbolGapKind.Mbo, 110); // MinHeal=109, high=110
        Thread.Sleep(80);
        r.HealFromSnapshot(2, SymbolGapKind.Mbo, snapshotRptSeq: 50); // unsafe=59, tail=60

        Assert.Equal(2, r.StaleAuthoritativeResetCount);
        // "Last" reflects the most recent (smaller) event.
        Assert.Equal(59u, r.LastAuthoritativeResetUnsafeDelta);
        Assert.Equal(60u, r.LastAuthoritativeResetDiscardedTailDelta);
        // Max retains the largest historical spike (so a "last" gauge cannot hide it).
        Assert.Equal(999u, r.MaxAuthoritativeResetUnsafeDelta);
        Assert.Equal(1000u, r.MaxAuthoritativeResetDiscardedTailDelta);
        // Sum accumulates total severity over time.
        Assert.Equal(1058UL, r.SumAuthoritativeResetUnsafeDelta);
        Assert.Equal(1060UL, r.SumAuthoritativeResetDiscardedTailDelta);
    }

    [Fact]
    public void RejectedSnapshot_DoesNotMoveSeverityGauges()
    {
        // Lagging snapshot rejected before timeout → counters stay clean.
        var r = NewRegistry();
        r.StaleEscapeTimeoutMs = 5_000;
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 100);
        r.Observe(1, SymbolGapKind.Mbo, 101);
        r.Observe(1, SymbolGapKind.Mbo, 200); // MinHeal=199

        var heal = r.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 50);
        Assert.False(heal.Accepted);
        Assert.Equal(0, r.StaleAuthoritativeResetCount);
        Assert.Equal(0u, r.LastAuthoritativeResetUnsafeDelta);
        Assert.Equal(0u, r.MaxAuthoritativeResetUnsafeDelta);
        Assert.Equal(0UL, r.SumAuthoritativeResetUnsafeDelta);
    }
}
