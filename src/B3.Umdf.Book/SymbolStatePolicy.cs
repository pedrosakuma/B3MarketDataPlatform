namespace B3.Umdf.Book;

/// <summary>
/// Immutable policy bundle consumed by <see cref="SymbolStateRegistry"/>. Splits
/// every tunable knob (per-kind bootstrap/live-resync defaults plus the stuck-Stale
/// escape timeout) out of the state-machine core so the registry's mutating code
/// paths only read configuration, never own it.
/// </summary>
/// <remarks>
/// <para><b>Backwards compatibility.</b> The registry's legacy parameterless
/// constructors and its mutable <see cref="SymbolStateRegistry.StaleEscapeTimeoutMs"/>
/// property are preserved; both delegate to <see cref="Default"/> on construction.
/// Production wiring (CLI, server) and existing tests therefore require zero
/// changes.</para>
/// <para><b>Per-kind defaults.</b> Mbo uses <see cref="BootstrapPolicy.RequireSnapshot"/>
/// + <see cref="LiveResyncPolicy.SnapshotOnly"/> (book is stateful — applying a
/// delete or trade without a built book corrupts state silently). Every stat kind
/// uses <see cref="BootstrapPolicy.AcceptFirst"/> + <see cref="LiveResyncPolicy.NextMessage"/>
/// (each stat update fully replaces the field; partial loss is preferable to
/// perpetual stale-ness).</para>
/// </remarks>
public sealed class SymbolStatePolicy
{
    private const int KindCount = (int)SymbolGapKind.SecurityStatus + 1;

    private readonly (BootstrapPolicy Boot, LiveResyncPolicy Live)[] _perKind;

    /// <summary>
    /// Stuck-Stale escape valve (milliseconds). When a (secId, kind) has been
    /// Stale longer than this AND a snapshot arrives that would be rejected as
    /// too-old (snapshotRptSeq &lt; MinHeal), the snapshot is accepted as
    /// authoritative reset instead. <c>0</c> disables the escape (legacy
    /// strict-reject behavior). The default policy uses <c>0</c> to preserve
    /// the registry's original out-of-the-box behavior; production wiring
    /// (<c>AppSettings.StaleEscapeTimeoutMs</c>) sets <c>60_000</c>.
    /// </summary>
    public long StaleEscapeTimeoutMs { get; }

    /// <summary>The shared default instance: stat-friendly per-kind policies, escape disabled.</summary>
    public static SymbolStatePolicy Default { get; } = new SymbolStatePolicy();

    /// <summary>Build the default policy: standard per-kind defaults, escape disabled (0 ms).</summary>
    public SymbolStatePolicy() : this(BuildDefaultPerKind(), staleEscapeTimeoutMs: 0)
    {
    }

    private SymbolStatePolicy(
        (BootstrapPolicy Boot, LiveResyncPolicy Live)[] perKind,
        long staleEscapeTimeoutMs)
    {
        if (perKind is null) throw new ArgumentNullException(nameof(perKind));
        if (perKind.Length != KindCount)
            throw new ArgumentException($"perKind length must be {KindCount}", nameof(perKind));
        if (staleEscapeTimeoutMs < 0)
            throw new ArgumentOutOfRangeException(nameof(staleEscapeTimeoutMs), "must be >= 0 (0 disables)");
        _perKind = ((BootstrapPolicy, LiveResyncPolicy)[])perKind.Clone();
        StaleEscapeTimeoutMs = staleEscapeTimeoutMs;
    }

    /// <summary>Per-kind bootstrap policy (Unknown→first message handling).</summary>
    public BootstrapPolicy GetBootstrap(SymbolGapKind kind) => _perKind[(int)kind].Boot;

    /// <summary>Per-kind live-resync policy (Stale→Healthy without snapshot).</summary>
    public LiveResyncPolicy GetLiveResync(SymbolGapKind kind) => _perKind[(int)kind].Live;

    /// <summary>Returns a new policy with the bootstrap policy of <paramref name="kind"/> overridden.</summary>
    public SymbolStatePolicy WithBootstrap(SymbolGapKind kind, BootstrapPolicy bootstrap)
    {
        var copy = ((BootstrapPolicy, LiveResyncPolicy)[])_perKind.Clone();
        copy[(int)kind] = (bootstrap, _perKind[(int)kind].Live);
        return new SymbolStatePolicy(copy, StaleEscapeTimeoutMs);
    }

    /// <summary>Returns a new policy with the live-resync policy of <paramref name="kind"/> overridden.</summary>
    public SymbolStatePolicy WithLiveResync(SymbolGapKind kind, LiveResyncPolicy live)
    {
        var copy = ((BootstrapPolicy, LiveResyncPolicy)[])_perKind.Clone();
        copy[(int)kind] = (_perKind[(int)kind].Boot, live);
        return new SymbolStatePolicy(copy, StaleEscapeTimeoutMs);
    }

    /// <summary>Returns a new policy with <see cref="StaleEscapeTimeoutMs"/> overridden.</summary>
    public SymbolStatePolicy WithStaleEscapeTimeoutMs(long staleEscapeTimeoutMs)
        => new SymbolStatePolicy(_perKind, staleEscapeTimeoutMs);

    private static (BootstrapPolicy Boot, LiveResyncPolicy Live)[] BuildDefaultPerKind()
    {
        var p = new (BootstrapPolicy, LiveResyncPolicy)[KindCount];
        // Stats: self-contained, accept first, resync on next message.
        for (int i = 0; i < KindCount; i++)
            p[i] = (BootstrapPolicy.AcceptFirst, LiveResyncPolicy.NextMessage);
        // MBO is the only stateful kind that requires snapshot for both bootstrap and recovery.
        p[(int)SymbolGapKind.Mbo] = (BootstrapPolicy.RequireSnapshot, LiveResyncPolicy.SnapshotOnly);
        return p;
    }
}
