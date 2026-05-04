using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Boundary-condition tests for <see cref="SymbolStateRegistry"/> covering the
/// stuck-Stale forced-heal escape valve at the exact threshold, monotonic-clock
/// edge cases, and per-kind bootstrap policy injection via <see cref="SymbolStatePolicy"/>.
/// All time advances use a deterministic <see cref="FakeClock"/> — never the
/// wall clock — so failures point unambiguously at the registry's threshold
/// arithmetic rather than scheduler jitter.
/// </summary>
public class SymbolStateRegistryBoundaryTests
{
    /// <remarks>
    /// Starts at 1_000_000 so <c>StaleSinceTicks</c> is non-zero on every
    /// transition into Stale. The forced-heal escape guard requires
    /// <c>StaleSinceTicks != 0</c> — a clock that begins at 0 silently
    /// disables it.
    /// </remarks>
    private sealed class FakeClock : IClock
    {
        private long _ticks = 1_000_000;
        public long NowTicks => _ticks;
        public void Advance(long ms) => _ticks += ms;
        public void SetTicks(long ticks) => _ticks = ticks;
    }

    private static (SymbolStateRegistry Registry, FakeClock Clock) NewRegistry(SymbolStatePolicy? policy = null)
    {
        var clock = new FakeClock();
        var reg = policy is null
            ? new SymbolStateRegistry(NullLogger.Instance, clock)
            : new SymbolStateRegistry(NullLogger.Instance, clock, policy);
        return (reg, clock);
    }

    /// <summary>
    /// Pin: the escape valve is gated on <c>(now - staleSince) &gt; timeout</c>
    /// (strictly greater). At <c>now - staleSince == timeout</c> the snapshot
    /// must still be rejected; at <c>timeout + 1</c> it is accepted.
    /// </summary>
    [Fact]
    public void ForcedHealEscape_AtExactlyTimeout_NotTriggered_Plus1Triggers()
    {
        const long timeout = 10_000;
        var (reg, clock) = NewRegistry();
        reg.StaleEscapeTimeoutMs = timeout;

        // Heal Mbo at rpt=100, then create a global gap → Stale, MinHeal=199.
        reg.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 100);
        reg.Observe(1, SymbolGapKind.Mbo, 100);
        reg.Observe(1, SymbolGapKind.Mbo, 200);
        Assert.Equal(SymbolState.Stale, reg.GetState(1, SymbolGapKind.Mbo));

        // Exactly at the threshold: still rejected (strict ">" guard).
        clock.Advance(timeout);
        var atThreshold = reg.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 150);
        Assert.False(atThreshold.Accepted);
        Assert.Equal(0, reg.StaleAuthoritativeResetCount);
        Assert.Equal(SymbolState.Stale, reg.GetState(1, SymbolGapKind.Mbo));

        // One ms past the threshold: escape engages.
        clock.Advance(1);
        var pastThreshold = reg.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 150);
        Assert.True(pastThreshold.Accepted);
        Assert.Equal(1, reg.StaleAuthoritativeResetCount);
        Assert.Equal(SymbolState.Healthy, reg.GetState(1, SymbolGapKind.Mbo));
    }

    /// <summary>Just below threshold: never engages, no matter how close.</summary>
    [Fact]
    public void ForcedHealEscape_OneMsBelowTimeout_NotTriggered()
    {
        const long timeout = 10_000;
        var (reg, clock) = NewRegistry();
        reg.StaleEscapeTimeoutMs = timeout;

        reg.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 100);
        reg.Observe(1, SymbolGapKind.Mbo, 100);
        reg.Observe(1, SymbolGapKind.Mbo, 200);

        clock.Advance(timeout - 1);
        var result = reg.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 150);
        Assert.False(result.Accepted);
        Assert.Equal(0, reg.StaleAuthoritativeResetCount);
    }

    /// <summary>
    /// Verifies policy injection: a custom shorter timeout is honored. Builds the
    /// registry through the policy-aware constructor (no post-construction property
    /// mutation) so the path proves <see cref="SymbolStatePolicy.StaleEscapeTimeoutMs"/>
    /// flows into the registry's escape arithmetic.
    /// </summary>
    [Fact]
    public void ForcedHealEscape_HonorsCustomShorterTimeoutFromPolicy()
    {
        const long shortTimeout = 250;
        var policy = SymbolStatePolicy.Default.WithStaleEscapeTimeoutMs(shortTimeout);
        var (reg, clock) = NewRegistry(policy);

        reg.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 100);
        reg.Observe(1, SymbolGapKind.Mbo, 100);
        reg.Observe(1, SymbolGapKind.Mbo, 200);

        // Below: rejected.
        clock.Advance(shortTimeout);
        Assert.False(reg.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 150).Accepted);

        // Past: accepted.
        clock.Advance(1);
        Assert.True(reg.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 150).Accepted);
        Assert.Equal(1, reg.StaleAuthoritativeResetCount);
    }

    /// <summary>
    /// Default policy: Mbo bootstrap requires snapshot — first incremental
    /// for an Unknown symbol is buffered, never applied. Pins the legacy
    /// behavior the policy split must preserve.
    /// </summary>
    [Fact]
    public void DefaultPolicy_MboBootstrap_RequiresSnapshot_BuffersFirstIncremental()
    {
        var (reg, _) = NewRegistry();
        var result = reg.Observe(1, SymbolGapKind.Mbo, receivedRptSeq: 42);

        Assert.Equal(SymbolStateRegistry.ObserveAction.Buffer, result.Action);
        Assert.Equal(SymbolState.Unknown, reg.GetState(1, SymbolGapKind.Mbo));
    }

    /// <summary>
    /// Override Mbo bootstrap to <see cref="BootstrapPolicy.AcceptFirst"/> via
    /// the policy and verify the first incremental is applied (Unknown→Healthy)
    /// — the inverse of <see cref="DefaultPolicy_MboBootstrap_RequiresSnapshot_BuffersFirstIncremental"/>.
    /// Concretely proves the bootstrap knob is wired through the policy.
    /// </summary>
    [Fact]
    public void PolicyOverride_MboBootstrap_AcceptFirst_AppliesFirstIncremental()
    {
        var policy = SymbolStatePolicy.Default.WithBootstrap(SymbolGapKind.Mbo, BootstrapPolicy.AcceptFirst);
        var (reg, _) = NewRegistry(policy);

        var result = reg.Observe(1, SymbolGapKind.Mbo, receivedRptSeq: 42);

        Assert.Equal(SymbolStateRegistry.ObserveAction.Apply, result.Action);
        Assert.Equal(SymbolState.Healthy, reg.GetState(1, SymbolGapKind.Mbo));
    }

    /// <summary>
    /// Heavy clock skew: the injected clock jumps backwards (e.g. ntpd slewed,
    /// or a buggy fake) AFTER a symbol enters Stale. The registry must NOT
    /// throw and must NOT spuriously trigger the escape valve from the
    /// resulting negative <c>(now - staleSince)</c>.
    /// </summary>
    [Fact]
    public void Stale_UnderClockSkewBackwards_DoesNotThrowAndDoesNotForceHeal()
    {
        const long timeout = 10_000;
        var (reg, clock) = NewRegistry();
        reg.StaleEscapeTimeoutMs = timeout;

        reg.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 100);
        reg.Observe(1, SymbolGapKind.Mbo, 100);
        reg.Observe(1, SymbolGapKind.Mbo, 200);
        Assert.Equal(SymbolState.Stale, reg.GetState(1, SymbolGapKind.Mbo));

        // Wind the clock backwards by way more than the timeout. The escape
        // guard is `(now - staleSince) > timeout` — with now < staleSince the
        // subtraction is negative and must not engage.
        clock.SetTicks(0);

        var result = reg.HealFromSnapshot(1, SymbolGapKind.Mbo, snapshotRptSeq: 150);
        Assert.False(result.Accepted);
        Assert.Equal(0, reg.StaleAuthoritativeResetCount);
        Assert.Equal(SymbolState.Stale, reg.GetState(1, SymbolGapKind.Mbo));

        // Subsequent observation under skewed clock also does not throw.
        var observe = reg.Observe(1, SymbolGapKind.Mbo, 250);
        Assert.Equal(SymbolStateRegistry.ObserveAction.Buffer, observe.Action);
    }

    /// <summary>
    /// Sanity: a fresh <see cref="SymbolStatePolicy.Default"/> has the escape
    /// disabled (0 ms). This is the contract the registry's parameterless ctor
    /// relies on for backwards compatibility — production wiring opts in via
    /// <c>AppSettings.StaleEscapeTimeoutMs = 60_000</c>.
    /// </summary>
    [Fact]
    public void DefaultPolicy_StaleEscapeTimeoutMs_IsZeroDisabled()
    {
        Assert.Equal(0, SymbolStatePolicy.Default.StaleEscapeTimeoutMs);

        var reg = new SymbolStateRegistry(NullLogger.Instance);
        Assert.Equal(0, reg.StaleEscapeTimeoutMs);
    }
}
