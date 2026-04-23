using B3.Umdf.Book;

namespace B3.Umdf.Server;

/// <summary>
/// Per-group hysteretic gate that suppresses client fanout in PerSymbol
/// recovery mode when a market-wide event leaves a substantial fraction of
/// symbols Stale (e.g. ChannelReset_11, mass loss, prolonged outage).
/// </summary>
/// <remarks>
/// <para><b>Rationale.</b> In Channel mode the FeedHandler enters Recovery
/// on a single channel-level gap and that already drives
/// <see cref="GroupConflationHandler.SetFanoutSuppressed"/>. PerSymbol mode
/// keeps the channel in RealTime and absorbs gaps as per-symbol Stale
/// transitions, so the channel-state gate never fires past cold-start.
/// Without this helper a market-wide stale event would still publish a
/// flood of <c>SymbolStaleStatus</c> flips and partial book updates
/// to clients with no useful aggregate view.</para>
///
/// <para><b>Hysteresis.</b> Engages when <c>StaleSymbolCount/KnownSymbolCount</c>
/// crosses the configured high-watermark; releases only when the ratio
/// drops at or below the low-watermark. Set the high-watermark to a
/// negative value to disable.</para>
///
/// <para><b>Threading.</b> <see cref="Evaluate"/> must be called on the
/// owning group's dispatch thread (typically from
/// <see cref="GroupConflationHandler.PreBatchEvaluator"/>). The Registry
/// counters it reads are <see cref="Volatile.Read{T}(ref T)"/>-safe so
/// occasional skew during catastrophic-reset iteration is acceptable for
/// a ratio gate.</para>
/// </remarks>
public sealed class PerSymbolFanoutGate
{
    private readonly SymbolStateRegistry _registry;
    private readonly GroupConflationHandler _conflation;
    private readonly double _highRatio;
    private readonly double _lowRatio;
    private bool _engaged;

    public PerSymbolFanoutGate(
        SymbolStateRegistry registry,
        GroupConflationHandler conflation,
        double highRatio,
        double lowRatio)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(conflation);
        if (lowRatio > highRatio)
            throw new ArgumentException(
                $"PerSymbolFanoutGate low-watermark ({lowRatio}) must not exceed high-watermark ({highRatio}).",
                nameof(lowRatio));
        _registry = registry;
        _conflation = conflation;
        _highRatio = highRatio;
        _lowRatio = lowRatio;
    }

    /// <summary>True iff the gate is configured to engage at all (high-watermark non-negative).</summary>
    public bool Enabled => _highRatio >= 0.0;

    /// <summary>True iff the gate is currently engaged (suppressing fanout).</summary>
    public bool IsEngaged => _engaged;

    /// <summary>Called once per batch on the dispatch thread; transitions on threshold crossings.</summary>
    public void Evaluate()
    {
        if (!Enabled) return;
        int known = _registry.KnownSymbolCount;
        if (known <= 0)
        {
            // No symbols registered yet (cold-start before any SecurityDefinition);
            // ratio is undefined — keep current state.
            return;
        }
        double ratio = (double)_registry.StaleSymbolCount / known;
        if (!_engaged && ratio >= _highRatio)
        {
            _engaged = true;
            _conflation.SetSuppressionSource(GroupConflationHandler.SuppressionSource.StaleRatio, true);
        }
        else if (_engaged && ratio <= _lowRatio)
        {
            _engaged = false;
            _conflation.SetSuppressionSource(GroupConflationHandler.SuppressionSource.StaleRatio, false);
        }
    }
}
