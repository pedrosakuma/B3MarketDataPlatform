using B3.Umdf.Book;
using B3.Umdf.Server;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server.Tests;

public class PerSymbolFanoutGateTests
{
    private static (SymbolStateRegistry registry, SubscriptionManager sm, GroupConflationHandler gh) NewWiring()
    {
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var sm = new SubscriptionManager();
        var gh = sm.CreateGroupHandler();
        return (registry, sm, gh);
    }

    [Fact]
    public void Gate_DisabledWhenHighRatioNegative()
    {
        var (registry, _, gh) = NewWiring();
        var gate = new PerSymbolFanoutGate(registry, gh, highRatio: -1.0, lowRatio: -1.0);
        Assert.False(gate.Enabled);

        // Even with all symbols stale, evaluator must not engage when disabled.
        registry.HealFromSnapshot(1, SymbolGapKind.Mbo, 100);
        registry.Observe(1, SymbolGapKind.Mbo, 110); // → Stale
        gate.Evaluate();
        Assert.False(gate.IsEngaged);
        Assert.False(gh.IsFanoutSuppressed);
    }

    [Fact]
    public void Gate_NoOpWhenKnownSymbolCountZero()
    {
        var (registry, _, gh) = NewWiring();
        var gate = new PerSymbolFanoutGate(registry, gh, highRatio: 0.50, lowRatio: 0.10);
        gate.Evaluate(); // no symbols registered yet
        Assert.False(gate.IsEngaged);
        Assert.False(gh.IsFanoutSuppressed);
    }

    [Fact]
    public void Gate_EngagesAtHighWatermark_ReleasesAtLowWatermark()
    {
        var (registry, _, gh) = NewWiring();
        // Register 4 symbols, all Healthy.
        for (ulong s = 1; s <= 4; s++)
        {
            registry.HealFromSnapshot(s, SymbolGapKind.Mbo, 100);
            registry.Observe(s, SymbolGapKind.Mbo, 101);
        }

        var gate = new PerSymbolFanoutGate(registry, gh, highRatio: 0.50, lowRatio: 0.25);

        // 1/4 = 0.25 — below high, gate stays off.
        registry.Observe(1, SymbolGapKind.Mbo, 110);
        gate.Evaluate();
        Assert.False(gate.IsEngaged);

        // 2/4 = 0.50 — at high, gate engages.
        registry.Observe(2, SymbolGapKind.Mbo, 210);
        gate.Evaluate();
        Assert.True(gate.IsEngaged);
        Assert.True(gh.IsFanoutSuppressed);

        // Heal symbol 2 → 1/4 = 0.25 (== low, releases).
        registry.HealFromSnapshot(2, SymbolGapKind.Mbo, 220);
        gate.Evaluate();
        Assert.False(gate.IsEngaged);
        Assert.False(gh.IsFanoutSuppressed);
    }

    [Fact]
    public void Gate_HysteresisPreventsFlapping()
    {
        var (registry, _, gh) = NewWiring();
        for (ulong s = 1; s <= 10; s++)
        {
            registry.HealFromSnapshot(s, SymbolGapKind.Mbo, 100);
            registry.Observe(s, SymbolGapKind.Mbo, 101);
        }

        var gate = new PerSymbolFanoutGate(registry, gh, highRatio: 0.50, lowRatio: 0.10);

        // Push to 6/10 = 0.60 → engage.
        for (ulong s = 1; s <= 6; s++) registry.Observe(s, SymbolGapKind.Mbo, 110);
        gate.Evaluate();
        Assert.True(gate.IsEngaged);

        // Heal three → 3/10 = 0.30. Above low (0.10), still engaged.
        for (ulong s = 1; s <= 3; s++) registry.HealFromSnapshot(s, SymbolGapKind.Mbo, 120);
        gate.Evaluate();
        Assert.True(gate.IsEngaged);

        // Heal two more → 1/10 = 0.10 (== low, releases).
        registry.HealFromSnapshot(4, SymbolGapKind.Mbo, 130);
        registry.HealFromSnapshot(5, SymbolGapKind.Mbo, 140);
        gate.Evaluate();
        Assert.False(gate.IsEngaged);
    }

    [Fact]
    public void Gate_RejectsLowGreaterThanHigh()
    {
        var (registry, _, gh) = NewWiring();
        Assert.Throws<ArgumentException>(() =>
            new PerSymbolFanoutGate(registry, gh, highRatio: 0.10, lowRatio: 0.50));
    }

    [Fact]
    public void DualSourceMask_BothSourcesMustReleaseBeforeResync()
    {
        var (_, _, gh) = NewWiring();
        Assert.False(gh.IsFanoutSuppressed);

        gh.SetSuppressionSource(GroupConflationHandler.SuppressionSource.ChannelState, true);
        Assert.True(gh.IsFanoutSuppressed);

        // Second source overlaps; suppressed remains true.
        gh.SetSuppressionSource(GroupConflationHandler.SuppressionSource.StaleRatio, true);
        Assert.True(gh.IsFanoutSuppressed);

        // Drop one source: still suppressed because the other is active.
        gh.SetSuppressionSource(GroupConflationHandler.SuppressionSource.ChannelState, false);
        Assert.True(gh.IsFanoutSuppressed);

        // Drop the last source: suppression released.
        gh.SetSuppressionSource(GroupConflationHandler.SuppressionSource.StaleRatio, false);
        Assert.False(gh.IsFanoutSuppressed);
    }

    [Fact]
    public void SetFanoutSuppressed_LegacyApi_RoutesToChannelStateSource()
    {
        var (_, _, gh) = NewWiring();

        gh.SetFanoutSuppressed(true);
        Assert.True(gh.IsFanoutSuppressed);

        // PerSymbol gate raised independently; releasing channel-state alone keeps suppression.
        gh.SetSuppressionSource(GroupConflationHandler.SuppressionSource.StaleRatio, true);
        gh.SetFanoutSuppressed(false);
        Assert.True(gh.IsFanoutSuppressed);

        gh.SetSuppressionSource(GroupConflationHandler.SuppressionSource.StaleRatio, false);
        Assert.False(gh.IsFanoutSuppressed);
    }
}
