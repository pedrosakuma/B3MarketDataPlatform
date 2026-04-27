using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Pins <see cref="IClock"/> injection into <see cref="SymbolStateRegistry"/>
/// (recovery improvement): time-based gauges (forced-heal escape, stale-since
/// latency) MUST be driven by the injected clock, not by the wall clock —
/// otherwise tests must <c>Thread.Sleep</c> for real time.
/// </summary>
public class SymbolStateRegistryClockTests
{
    private sealed class FakeClock : IClock
    {
        // Start non-zero so StaleSinceTicks != 0 (the escape guard requires it).
        private long _ticks = 1_000_000;
        public long NowTicks => _ticks;
        public void Advance(long ms) => _ticks += ms;
    }

    [Fact]
    public void StaleSinceTicks_UsesInjectedClock()
    {
        var clock = new FakeClock();
        var reg = new SymbolStateRegistry(NullLogger.Instance, clock);

        // Heal Mbo via snapshot at t=0, then trigger gap-Stale at t=5000ms.
        reg.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 100);
        reg.Observe(1, SymbolGapKind.Mbo, 100); // in-sequence (already at 100)
        Assert.Equal(SymbolState.Healthy, reg.GetState(1, SymbolGapKind.Mbo));

        clock.Advance(5_000);
        reg.Observe(1, SymbolGapKind.Mbo, 200); // gap → Stale

        Assert.Equal(SymbolState.Stale, reg.GetState(1, SymbolGapKind.Mbo));
        Assert.Equal(1, reg.StaleSymbolCount);
    }

    [Fact]
    public void ForcedHealEscape_UsesInjectedClock_NoWallClockDependency()
    {
        var clock = new FakeClock();
        var reg = new SymbolStateRegistry(NullLogger.Instance, clock)
        {
            StaleEscapeTimeoutMs = 60_000,
        };

        // Heal Mbo at rpt=100, then create a global gap to force Stale with
        // MinHeal=199 — this is the exact path forced-heal escape protects.
        reg.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 100);
        reg.Observe(1, SymbolGapKind.Mbo, 100);
        reg.Observe(1, SymbolGapKind.Mbo, 200); // gap → Stale, MinHeal=199

        // Stale snapshot at rpt=150 (< MinHeal=199) — too-old normally.
        var resultEarly = reg.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 150);
        Assert.False(resultEarly.Accepted);
        Assert.Equal(0, reg.StaleAuthoritativeResetCount);

        // Advance the FAKE clock past the escape timeout.
        clock.Advance(60_001);

        // Same too-old snapshot now triggers the authoritative-reset escape.
        var resultEscape = reg.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 150);
        Assert.True(resultEscape.Accepted);
        Assert.Equal(1, reg.StaleAuthoritativeResetCount);
    }

    [Fact]
    public void DefaultCtor_UsesSystemClock_NoNullRef()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        reg.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 100);
        reg.Observe(1, SymbolGapKind.Mbo, 100);
        Assert.Equal(SymbolState.Healthy, reg.GetState(1, SymbolGapKind.Mbo));
    }
}
