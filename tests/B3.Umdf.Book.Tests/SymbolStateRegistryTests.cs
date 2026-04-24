using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

public class SymbolStateRegistryTests
{
    private static SymbolStateRegistry NewRegistry() => new(NullLogger.Instance);

    // ---- BootstrapPolicy.AcceptFirst (stats) ----

    [Fact]
    public void Stat_FirstObservation_BecomesHealthyAndApplies()
    {
        var r = NewRegistry();
        var result = r.Observe(securityId: 1, SymbolGapKind.PriceBand, receivedRptSeq: 12345);

        Assert.Equal(SymbolStateRegistry.ObserveAction.Apply, result.Action);
        Assert.Equal(SymbolState.Healthy, result.NewState);
        Assert.Equal(SymbolState.Healthy, r.GetState(1, SymbolGapKind.PriceBand));
    }

    [Fact]
    public void Stat_GapDetected_AppliesAndRebaselines_LiveResync()
    {
        var r = NewRegistry();
        r.Observe(1, SymbolGapKind.PriceBand, 10);
        var result = r.Observe(1, SymbolGapKind.PriceBand, 13); // gap of 2

        Assert.Equal(SymbolStateRegistry.ObserveAction.Apply, result.Action);
        Assert.Equal(SymbolState.Healthy, result.NewState);
        Assert.Equal(2u, result.GapSize);
        // Next contiguous still works.
        var next = r.Observe(1, SymbolGapKind.PriceBand, 14);
        Assert.Equal(SymbolStateRegistry.ObserveAction.Apply, next.Action);
        Assert.Equal(0u, next.GapSize);
    }

    // ---- BootstrapPolicy.RequireSnapshot (MBO) ----

    [Fact]
    public void Mbo_FirstObservation_BuffersUntilSnapshot()
    {
        var r = NewRegistry();
        var result = r.Observe(1, SymbolGapKind.Mbo, 5);

        Assert.Equal(SymbolStateRegistry.ObserveAction.Buffer, result.Action);
        Assert.Equal(SymbolState.Unknown, result.NewState);
    }

    [Fact]
    public void Mbo_GapWhileHealthy_TransitionsToStaleAndBuffers()
    {
        var r = NewRegistry();
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 100);
        r.Observe(1, SymbolGapKind.Mbo, 101); // contiguous, applies
        var gap = r.Observe(1, SymbolGapKind.Mbo, 105); // gap

        Assert.Equal(SymbolStateRegistry.ObserveAction.Buffer, gap.Action);
        Assert.Equal(SymbolState.Stale, gap.NewState);
        Assert.True(gap.TransitionedToStale);
        Assert.Equal(3u, gap.GapSize); // expected 102, got 105 → 3 missing

        // Subsequent live messages also buffer.
        var next = r.Observe(1, SymbolGapKind.Mbo, 106);
        Assert.Equal(SymbolStateRegistry.ObserveAction.Buffer, next.Action);
        Assert.False(next.TransitionedToStale); // already Stale

        // O(1) any-stale reflects state.
        Assert.True(r.IsAnyStale(1));
    }

    [Fact]
    public void Mbo_HealFromSnapshot_ReturnsDrainWindowFromHighWater()
    {
        var r = NewRegistry();
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, 100);
        r.Observe(1, SymbolGapKind.Mbo, 101);  // healthy
        r.Observe(1, SymbolGapKind.Mbo, 110);  // gap → Stale
        r.Observe(1, SymbolGapKind.Mbo, 115);  // buffered, advances high-water

        var heal = r.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 112);

        Assert.True(heal.TransitionedToHealthy);
        Assert.Equal(112u, heal.SnapshotRptSeq);
        Assert.Equal(113u, heal.DrainFrom);
        Assert.Equal(115u, heal.DrainTo);
        Assert.False(r.IsAnyStale(1));
    }

    [Fact]
    public void Mbo_HealFromSnapshot_NoBufferedMessages_DrainWindowIsEmpty()
    {
        var r = NewRegistry();
        var heal = r.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 50);

        Assert.True(heal.TransitionedToHealthy);
        Assert.Equal(51u, heal.DrainFrom);
        Assert.Equal(50u, heal.DrainTo); // DrainTo < DrainFrom signals nothing to drain
    }

    [Fact]
    public void Mbo_LaggingSnapshot_BelowMinHeal_Rejected()
    {
        var r = NewRegistry();
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, 100);
        r.Observe(1, SymbolGapKind.Mbo, 105);  // gap → Stale, MinHealRptSeq=104, high-water=105
        r.Observe(1, SymbolGapKind.Mbo, 108);  // buffered, high-water=108

        // Snapshot at 103 cannot bridge: drain would be [104, 108] but buffer has only 105+,
        // leaving rptSeq 104 as a hole. Must be rejected; symbol stays Stale.
        var heal = r.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 103);

        Assert.False(heal.Accepted);
        Assert.False(heal.TransitionedToHealthy);
        Assert.Equal(SymbolState.Stale, r.GetState(1, SymbolGapKind.Mbo));
        Assert.Equal(1, r.LaggingSnapshotCount);
    }

    [Fact]
    public void Mbo_LaggingSnapshot_AtMinHeal_Accepted()
    {
        var r = NewRegistry();
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, 100);
        r.Observe(1, SymbolGapKind.Mbo, 105);  // gap → Stale, MinHealRptSeq=104
        r.Observe(1, SymbolGapKind.Mbo, 108);

        // Snapshot at exactly MinHealRptSeq (104): drain [105, 108] aligns with first
        // buffered entry. Accepted.
        var heal = r.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 104);

        Assert.True(heal.Accepted);
        Assert.True(heal.TransitionedToHealthy);
        Assert.Equal(105u, heal.DrainFrom);
        Assert.Equal(108u, heal.DrainTo);
    }

    [Fact]
    public void Mbo_ColdStart_LaggingSnapshot_Rejected()
    {
        var r = NewRegistry();
        // Cold start: first observation at 5005 (Unknown→Buffer). MinHealRptSeq=5004.
        r.Observe(1, SymbolGapKind.Mbo, 5005);
        r.Observe(1, SymbolGapKind.Mbo, 5006);

        // Snapshot at 4999 (older than first observed - 1) cannot bridge.
        var heal = r.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 4999);

        Assert.False(heal.Accepted);
        Assert.Equal(SymbolState.Unknown, r.GetState(1, SymbolGapKind.Mbo));

        // Snapshot at 5004 (= first observed - 1) bridges cleanly.
        heal = r.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 5004);
        Assert.True(heal.Accepted);
        Assert.True(heal.TransitionedToHealthy);
        Assert.Equal(5005u, heal.DrainFrom);
        Assert.Equal(5006u, heal.DrainTo);
    }

    [Fact]
    public void Mbo_AcceptedHeal_ClearsMinHeal_NextHealAtAnyRptSeqAccepted()
    {
        var r = NewRegistry();
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, 100);
        r.Observe(1, SymbolGapKind.Mbo, 105);  // Stale, MinHealRptSeq=104
        var first = r.HealFromSnapshot(1, SymbolGapKind.Mbo, 104);
        Assert.True(first.Accepted);

        // After accepted heal, MinHealRptSeq must be cleared. A subsequent heal with
        // any (even older) rptSeq before another stale event should be accepted.
        var second = r.HealFromSnapshot(1, SymbolGapKind.Mbo, 50);
        Assert.True(second.Accepted);
    }

    // ---- ResetEpoch (catastrophic, lower-numbered rptSeq accepted) ----

    [Fact]
    public void ResetEpoch_AllowsNewLowerEpoch()
    {
        var r = NewRegistry();
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, 1000);
        r.Observe(1, SymbolGapKind.Mbo, 1001);
        r.Observe(2, SymbolGapKind.PriceBand, 999);

        r.ResetEpoch("ChannelReset_11");

        Assert.Equal(SymbolState.Unknown, r.GetState(1, SymbolGapKind.Mbo));
        Assert.Equal(SymbolState.Unknown, r.GetState(2, SymbolGapKind.PriceBand));

        // New epoch with low rptSeq must be accepted.
        var snap = r.HealFromSnapshot(1, SymbolGapKind.Mbo, 5);
        Assert.True(snap.TransitionedToHealthy);
        var live = r.Observe(1, SymbolGapKind.Mbo, 6);
        Assert.Equal(SymbolStateRegistry.ObserveAction.Apply, live.Action);

        var stat = r.Observe(2, SymbolGapKind.PriceBand, 3);
        Assert.Equal(SymbolStateRegistry.ObserveAction.Apply, stat.Action);
    }

    // ---- MarkAllStale (non-epoch invalidation) ----

    [Fact]
    public void MarkAllStale_KeepsBaselines_OnlyFlipsHealthyToStale()
    {
        var r = NewRegistry();
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, 100);
        r.Observe(2, SymbolGapKind.PriceBand, 50);

        r.MarkAllStale("operator");

        Assert.Equal(SymbolState.Stale, r.GetState(1, SymbolGapKind.Mbo));
        Assert.Equal(SymbolState.Stale, r.GetState(2, SymbolGapKind.PriceBand));

        // Subsequent MBO heal preserves baseline ordering.
        var heal = r.HealFromSnapshot(1, SymbolGapKind.Mbo, 150);
        Assert.True(heal.TransitionedToHealthy);
        Assert.Equal(150u, heal.SnapshotRptSeq);
    }

    // ---- Duplicate / reorder ----

    [Fact]
    public void DuplicateRptSeq_Dropped()
    {
        var r = NewRegistry();
        r.Observe(1, SymbolGapKind.PriceBand, 10);
        r.Observe(1, SymbolGapKind.PriceBand, 11);
        var dup = r.Observe(1, SymbolGapKind.PriceBand, 11);

        Assert.Equal(SymbolStateRegistry.ObserveAction.Drop, dup.Action);
        Assert.Equal(SymbolState.Healthy, dup.NewState);
    }

    [Fact]
    public void ZeroRptSeq_Dropped()
    {
        var r = NewRegistry();
        var result = r.Observe(1, SymbolGapKind.Mbo, 0);
        Assert.Equal(SymbolStateRegistry.ObserveAction.Drop, result.Action);
    }

    // ---- Aggregate snapshot ----

    [Fact]
    public void AggregateSnapshot_CountsStaleSymbolsAndKinds()
    {
        var r = NewRegistry();
        // Symbol 1: Mbo Stale + PriceBand healthy
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, 100);
        r.Observe(1, SymbolGapKind.Mbo, 101);
        r.Observe(1, SymbolGapKind.Mbo, 110); // → Stale
        r.Observe(1, SymbolGapKind.PriceBand, 5);
        // Symbol 2: only PriceBand observed (Healthy)
        r.Observe(2, SymbolGapKind.PriceBand, 5);
        // Symbol 3: Mbo Stale via MarkAllStale
        r.HealFromSnapshot(3, SymbolGapKind.Mbo, 1);
        r.MarkAllStale("test");

        var agg = r.GetAggregateSnapshot();

        // After MarkAllStale: every Healthy kind flips to Stale.
        // Symbol 1: Mbo Stale (was already), PriceBand Stale (was Healthy → flipped)
        // Symbol 2: PriceBand Stale (was Healthy → flipped)
        // Symbol 3: Mbo Stale (flipped)
        Assert.Equal(3, agg.TotalStaleSymbols);
        Assert.Equal(3, agg.TotalSymbols);
        Assert.Equal(2, agg.StaleOf(SymbolGapKind.Mbo));        // symbols 1, 3
        Assert.Equal(2, agg.StaleOf(SymbolGapKind.PriceBand));  // symbols 1, 2
    }

    // ---- IsAnyStale ----

    [Fact]
    public void IsAnyStale_FalseForUnknownSymbol()
    {
        var r = NewRegistry();
        Assert.False(r.IsAnyStale(999));
    }

    [Fact]
    public void IsAnyStale_TrueWhenAnyKindStale()
    {
        var r = NewRegistry();
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, 100);
        r.Observe(1, SymbolGapKind.Mbo, 101);
        Assert.False(r.IsAnyStale(1));
        r.Observe(1, SymbolGapKind.Mbo, 110); // Stale
        Assert.True(r.IsAnyStale(1));
    }

    // ---- KnownSymbolCount / StaleSymbolCount (cheap aggregates for fanout gate) ----

    [Fact]
    public void KnownSymbolCount_IncrementsOnFirstObservationAndEnsureRegistered()
    {
        var r = NewRegistry();
        Assert.Equal(0, r.KnownSymbolCount);

        r.EnsureRegistered(1);
        Assert.Equal(1, r.KnownSymbolCount);

        r.EnsureRegistered(1); // idempotent
        Assert.Equal(1, r.KnownSymbolCount);

        r.Observe(2, SymbolGapKind.PriceBand, 5);
        Assert.Equal(2, r.KnownSymbolCount);

        r.HealFromSnapshot(3, SymbolGapKind.Mbo, 100);
        Assert.Equal(3, r.KnownSymbolCount);
    }

    [Fact]
    public void StaleSymbolCount_TracksHealthyToStaleTransitions()
    {
        var r = NewRegistry();
        // Bring two MBO symbols Healthy.
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, 100);
        r.Observe(1, SymbolGapKind.Mbo, 101);
        r.HealFromSnapshot(2, SymbolGapKind.Mbo, 200);
        r.Observe(2, SymbolGapKind.Mbo, 201);
        Assert.Equal(0, r.StaleSymbolCount);

        // Gap → Stale on symbol 1.
        r.Observe(1, SymbolGapKind.Mbo, 110);
        Assert.Equal(1, r.StaleSymbolCount);

        // Heal symbol 1 → back to 0.
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, 120);
        Assert.Equal(0, r.StaleSymbolCount);
    }

    [Fact]
    public void StaleSymbolCount_DoesNotDoubleCount_PerSymbolAcrossKinds()
    {
        // Symbol with both MBO Stale and another kind Stale should count once.
        var r = NewRegistry();
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, 100);
        r.Observe(1, SymbolGapKind.Mbo, 101);
        // Force MBO Stale.
        r.Observe(1, SymbolGapKind.Mbo, 110);
        Assert.Equal(1, r.StaleSymbolCount);

        // Heal MBO; now symbol back to Healthy → 0.
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, 120);
        Assert.Equal(0, r.StaleSymbolCount);
    }

    [Fact]
    public void MarkAllStale_IncrementsCountForEachAffectedSymbol()
    {
        var r = NewRegistry();
        for (ulong s = 1; s <= 5; s++)
        {
            r.HealFromSnapshot(s, SymbolGapKind.Mbo, 100);
            r.Observe(s, SymbolGapKind.Mbo, 101);
        }
        Assert.Equal(0, r.StaleSymbolCount);

        r.MarkAllStale("test");
        Assert.Equal(5, r.StaleSymbolCount);
        Assert.Equal(5, r.KnownSymbolCount);
    }

    [Fact]
    public void ResetEpoch_ClearsStaleCount()
    {
        var r = NewRegistry();
        r.HealFromSnapshot(1, SymbolGapKind.Mbo, 100);
        r.Observe(1, SymbolGapKind.Mbo, 110); // Stale
        Assert.Equal(1, r.StaleSymbolCount);

        r.ResetEpoch("test");
        Assert.Equal(0, r.StaleSymbolCount);
        Assert.Equal(1, r.KnownSymbolCount);
    }
}
