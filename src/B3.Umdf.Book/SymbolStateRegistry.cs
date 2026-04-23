using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace B3.Umdf.Book;

/// <summary>
/// Per-kind recovery policy. Different B3 message families have different
/// recovery contracts: MBO requires a snapshot to rebuild book state safely,
/// while statistics are self-contained and can resync on the next live
/// message. SecurityStatus carries cumulative state that benefits from
/// snapshot but can also be re-baselined from any incoming update.
/// </summary>
public enum BootstrapPolicy : byte
{
    /// <summary>
    /// Bootstrap requires snapshot: an Unknown→Healthy transition can only
    /// happen via <see cref="SymbolStateRegistry.HealFromSnapshot"/>. The
    /// first incremental for an Unknown symbol is buffered (caller must hold
    /// it for replay). Used for MBO/Trade messages where applying a delete
    /// or trade without a built book causes silent corruption.
    /// </summary>
    RequireSnapshot,

    /// <summary>
    /// Bootstrap accepts first observation as baseline: the first incremental
    /// for an Unknown symbol becomes the rptSeq baseline and the message is
    /// applied. Used for self-contained statistic messages where each update
    /// fully replaces the field value.
    /// </summary>
    AcceptFirst,
}

/// <summary>
/// Live-recovery policy: how does a Stale (secId, kind) heal without a
/// snapshot? B3 publishes snapshots only for MBO; statistic messages have
/// no snapshot stream, so a Stale stat would be stuck forever without a
/// live resync path.
/// </summary>
public enum LiveResyncPolicy : byte
{
    /// <summary>
    /// Stale only heals via snapshot. Live messages while Stale are
    /// buffered (caller responsibility) and applied on heal. Used for MBO.
    /// </summary>
    SnapshotOnly,

    /// <summary>
    /// Stale heals on the next live message: the message is applied, becomes
    /// the new baseline, and any intermediate gap is accepted as data loss.
    /// Used for statistic messages where partial loss of intermediate values
    /// is preferable to perpetual stale-ness (each stat update is self-contained).
    /// </summary>
    NextMessage,
}

/// <summary>
/// Centralized per-(SecurityID, <see cref="SymbolGapKind"/>) state machine.
/// Single source of truth for per-symbol recovery state across BookManager,
/// MarketDataManager, fanout, and metrics — replaces the channel-level
/// Recovery state machine for the per-symbol unified path.
/// </summary>
/// <remarks>
/// <para><b>Per-kind policies.</b> Bootstrap and live-resync policies are
/// distinct per <see cref="SymbolGapKind"/>. MBO uses
/// <see cref="BootstrapPolicy.RequireSnapshot"/> +
/// <see cref="LiveResyncPolicy.SnapshotOnly"/> because the book is stateful.
/// Stats use <see cref="BootstrapPolicy.AcceptFirst"/> +
/// <see cref="LiveResyncPolicy.NextMessage"/> because each message fully
/// replaces the field. Defaults are encoded in <see cref="DefaultPolicies"/>.</para>
///
/// <para><b>Concurrency.</b> One <see cref="SymbolEntry"/> per security; the
/// outer <see cref="ConcurrentDictionary{TKey,TValue}"/> handles concurrent
/// inserts. Per-entry mutations are guarded by an entry-local lock so that
/// hot-path observations contend only with other observations of the same
/// symbol — which on the feed thread serialize naturally.</para>
///
/// <para><b>Aggregate queries.</b> <see cref="GetAggregateSnapshot"/>
/// performs a single-pass scan of the registry. Callers that need the result
/// at high frequency (backpressure gate, frontend RecoveryProgress) should
/// cache the value with a short TTL.</para>
/// </remarks>
public sealed class SymbolStateRegistry
{
    private const int KindCount = (int)SymbolGapKind.SecurityStatus + 1;

    private static readonly (BootstrapPolicy Boot, LiveResyncPolicy Live)[] DefaultPolicies = BuildDefaultPolicies();

    private static (BootstrapPolicy, LiveResyncPolicy)[] BuildDefaultPolicies()
    {
        var p = new (BootstrapPolicy, LiveResyncPolicy)[KindCount];
        // Default for stats: self-contained, accept first, resync on next message.
        for (int i = 0; i < KindCount; i++)
            p[i] = (BootstrapPolicy.AcceptFirst, LiveResyncPolicy.NextMessage);
        // MBO is the one stateful kind that requires snapshot for both bootstrap and recovery.
        p[(int)SymbolGapKind.Mbo] = (BootstrapPolicy.RequireSnapshot, LiveResyncPolicy.SnapshotOnly);
        return p;
    }

    private readonly ConcurrentDictionary<ulong, SymbolEntry> _entries = new();
    private readonly ILogger _logger;
    private long _staleSnapshotIgnored;

    public SymbolStateRegistry(ILogger logger)
    {
        _logger = logger;
    }

    /// <summary>Per-symbol state holder. Mutations require holding <see cref="Sync"/>.</summary>
    public sealed class SymbolEntry
    {
        internal readonly Lock Sync = new();
        internal readonly SymbolState[] States = new SymbolState[KindCount];
        internal readonly uint[] LastRptSeq = new uint[KindCount];
        internal readonly long[] StaleSinceTicks = new long[KindCount];

        // Mask of kinds currently Stale — maintained inside the lock for
        // O(1) IsAnyStale lookups and aggregate counting without scanning
        // all 14 slots.
        internal int StaleKindMask;
    }

    /// <summary>
    /// Outcome of <see cref="Observe"/>: tells the caller whether to apply,
    /// buffer, or drop the message, and signals state transitions for
    /// metrics emission.
    /// </summary>
    public readonly record struct ObserveResult(
        SymbolState NewState,
        ObserveAction Action,
        bool TransitionedToStale,
        bool TransitionedToHealthy,
        uint GapSize)
    {
        public static readonly ObserveResult ApplyHealthy =
            new(SymbolState.Healthy, ObserveAction.Apply, false, false, 0);
    }

    /// <summary>What the caller should do with the message that triggered <see cref="Observe"/>.</summary>
    public enum ObserveAction : byte
    {
        /// <summary>Apply the message to the book/info as usual.</summary>
        Apply,
        /// <summary>Drop the message: duplicate, A/B reorder leftover, or absent rptSeq.</summary>
        Drop,
        /// <summary>Buffer the message — symbol is Stale awaiting snapshot heal.</summary>
        Buffer,
    }

    /// <summary>
    /// Apply an incoming incremental message's <c>rptSeq</c> against the
    /// stored last-seen value. Returns the action the caller should take.
    /// </summary>
    /// <param name="securityId">Security identifier.</param>
    /// <param name="kind">Message family — distinguishes independent rptSeq streams per symbol.</param>
    /// <param name="receivedRptSeq">The rptSeq carried by the incoming message. Caller
    /// must not pass 0 (sentinel for "absent" in SBE) — guard at the call site.</param>
    public ObserveResult Observe(ulong securityId, SymbolGapKind kind, uint receivedRptSeq)
    {
        if (receivedRptSeq == 0)
            return new ObserveResult(GetState(securityId, kind), ObserveAction.Drop, false, false, 0);

        var entry = _entries.GetOrAdd(securityId, _ => new SymbolEntry());
        int idx = (int)kind;
        var (bootPolicy, livePolicy) = DefaultPolicies[idx];

        lock (entry.Sync)
        {
            var prev = entry.States[idx];
            uint lastSeen = entry.LastRptSeq[idx];

            switch (prev)
            {
                case SymbolState.Unknown:
                    if (bootPolicy == BootstrapPolicy.AcceptFirst)
                    {
                        // Self-contained kinds: first message is the baseline.
                        entry.LastRptSeq[idx] = receivedRptSeq;
                        entry.States[idx] = SymbolState.Healthy;
                        return ObserveResult.ApplyHealthy;
                    }
                    // RequireSnapshot: track high-water for post-heal drain, buffer the message.
                    if (receivedRptSeq > lastSeen)
                        entry.LastRptSeq[idx] = receivedRptSeq;
                    return new ObserveResult(SymbolState.Unknown, ObserveAction.Buffer, false, false, 0);

                case SymbolState.Healthy:
                    if (receivedRptSeq <= lastSeen)
                        return new ObserveResult(SymbolState.Healthy, ObserveAction.Drop, false, false, 0);
                    uint expected = lastSeen + 1;
                    if (receivedRptSeq == expected)
                    {
                        entry.LastRptSeq[idx] = receivedRptSeq;
                        return ObserveResult.ApplyHealthy;
                    }
                    // Gap detected.
                    uint gapSize = receivedRptSeq - expected;
                    entry.LastRptSeq[idx] = receivedRptSeq;
                    if (livePolicy == LiveResyncPolicy.NextMessage)
                    {
                        // Self-contained kind: accept the gap; this message is the new baseline.
                        if (_logger.IsEnabled(LogLevel.Debug))
                            _logger.LogDebug(
                                "SymbolState: secId={SecurityId} kind={Kind} live-resync (expected={Expected} received={Received} gap={Gap})",
                                securityId, kind, expected, receivedRptSeq, gapSize);
                        return new ObserveResult(SymbolState.Healthy, ObserveAction.Apply, false, false, gapSize);
                    }
                    // SnapshotOnly: transition to Stale.
                    entry.States[idx] = SymbolState.Stale;
                    entry.StaleSinceTicks[idx] = Environment.TickCount64;
                    entry.StaleKindMask |= 1 << idx;
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug(
                            "SymbolState: secId={SecurityId} kind={Kind} Healthy→Stale (expected={Expected} received={Received} gap={Gap})",
                            securityId, kind, expected, receivedRptSeq, gapSize);
                    return new ObserveResult(SymbolState.Stale, ObserveAction.Buffer, true, false, gapSize);

                case SymbolState.Stale:
                    // Stale only heals via HealFromSnapshot (SnapshotOnly kinds reach Stale).
                    // Track the high-water mark so the post-heal drain knows the upper bound.
                    if (receivedRptSeq > lastSeen)
                        entry.LastRptSeq[idx] = receivedRptSeq;
                    return new ObserveResult(SymbolState.Stale, ObserveAction.Buffer, false, false, 0);

                default:
                    return new ObserveResult(prev, ObserveAction.Drop, false, false, 0);
            }
        }
    }

    /// <summary>
    /// Outcome of <see cref="HealFromSnapshot"/>. <see cref="DrainFrom"/> is
    /// the rptSeq immediately after the snapshot baseline; <see cref="DrainTo"/>
    /// is the highest rptSeq observed while Stale/Unknown. Caller drains its
    /// per-symbol buffer for entries with <c>rptSeq ∈ (DrainFrom-1, DrainTo]</c>
    /// — i.e. <c>rptSeq &gt; SnapshotRptSeq</c>.
    /// </summary>
    public readonly record struct HealResult(
        bool TransitionedToHealthy,
        uint SnapshotRptSeq,
        uint DrainFrom,
        uint DrainTo);

    /// <summary>
    /// Snapshot path: forces (secId, kind) to <see cref="SymbolState.Healthy"/>
    /// at the given rptSeq. Caller must drain its per-symbol buffer using
    /// the returned <see cref="HealResult"/>.
    /// </summary>
    public HealResult HealFromSnapshot(ulong securityId, SymbolGapKind kind, uint snapshotRptSeq)
    {
        var entry = _entries.GetOrAdd(securityId, _ => new SymbolEntry());
        int idx = (int)kind;
        lock (entry.Sync)
        {
            var prev = entry.States[idx];
            uint priorHighWater = entry.LastRptSeq[idx];

            // Snapshot is authoritative. If snapshot rptSeq is older than what
            // we've already seen live, log it (could indicate a lagging snapshot
            // stream) but accept the heal — the snapshot rebuilds book state
            // and live messages > snapshot will replay from the buffer.
            if (snapshotRptSeq < priorHighWater)
            {
                Interlocked.Increment(ref _staleSnapshotIgnored);
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug(
                        "SymbolState: lagging snapshot secId={SecurityId} kind={Kind} snapshotRptSeq={Snap} priorHighWater={High}",
                        securityId, kind, snapshotRptSeq, priorHighWater);
            }

            uint newBaseline = Math.Max(snapshotRptSeq, priorHighWater);
            entry.LastRptSeq[idx] = newBaseline;
            entry.States[idx] = SymbolState.Healthy;

            bool transitioned = prev != SymbolState.Healthy;
            if (transitioned)
            {
                entry.StaleKindMask &= ~(1 << idx);
                entry.StaleSinceTicks[idx] = 0;
            }

            // Drain window: anything strictly above snapshotRptSeq up to the
            // observed high-water mark must be replayed by the caller.
            uint drainFrom = snapshotRptSeq + 1;
            uint drainTo = priorHighWater > snapshotRptSeq ? priorHighWater : snapshotRptSeq;
            return new HealResult(transitioned, snapshotRptSeq, drainFrom, drainTo);
        }
    }

    /// <summary>
    /// Catastrophic-reset path (SequenceReset_1, ChannelReset_11). Resets
    /// every (symbol, kind) to <see cref="SymbolState.Unknown"/> and clears
    /// the rptSeq baseline so a new epoch with lower-numbered rptSeq is
    /// accepted. Caller is responsible for flushing per-symbol buffers held
    /// outside the registry (BookManager._staleBuffers, etc).
    /// </summary>
    public void ResetEpoch(string reason)
    {
        int affected = 0;
        foreach (var kv in _entries)
        {
            var entry = kv.Value;
            lock (entry.Sync)
            {
                for (int i = 0; i < KindCount; i++)
                {
                    if (entry.States[i] != SymbolState.Unknown) affected++;
                    entry.States[i] = SymbolState.Unknown;
                    entry.LastRptSeq[i] = 0;
                    entry.StaleSinceTicks[i] = 0;
                }
                entry.StaleKindMask = 0;
            }
        }
        _logger.LogWarning("SymbolStateRegistry.ResetEpoch: reason={Reason} kindsCleared={Count}", reason, affected);
    }

    /// <summary>
    /// Marks every healthy (symbol, kind) as Stale without clearing the
    /// rptSeq baseline. Used for non-epoch-changing events that nonetheless
    /// invalidate the in-memory state (e.g. operator-triggered re-snapshot).
    /// Catastrophic resets should call <see cref="ResetEpoch"/> instead.
    /// </summary>
    public void MarkAllStale(string reason)
    {
        long now = Environment.TickCount64;
        int affected = 0;
        foreach (var kv in _entries)
        {
            var entry = kv.Value;
            lock (entry.Sync)
            {
                for (int i = 0; i < KindCount; i++)
                {
                    if (entry.States[i] == SymbolState.Healthy)
                    {
                        entry.States[i] = SymbolState.Stale;
                        entry.StaleSinceTicks[i] = now;
                        entry.StaleKindMask |= 1 << i;
                        affected++;
                    }
                }
            }
        }
        _logger.LogWarning("SymbolStateRegistry.MarkAllStale: reason={Reason} kindsAffected={Count}", reason, affected);
    }

    /// <summary>
    /// Pre-populate an entry on SecurityDefinition so the cold-start path
    /// avoids racing GetOrAdd on the feed thread. Idempotent.
    /// </summary>
    public void EnsureRegistered(ulong securityId)
    {
        _entries.GetOrAdd(securityId, _ => new SymbolEntry());
    }

    public SymbolState GetState(ulong securityId, SymbolGapKind kind)
    {
        if (!_entries.TryGetValue(securityId, out var entry)) return SymbolState.Unknown;
        lock (entry.Sync) return entry.States[(int)kind];
    }

    /// <summary>
    /// O(1) check used by frontend per-row dim logic: is the symbol Stale
    /// in any kind?
    /// </summary>
    public bool IsAnyStale(ulong securityId)
    {
        return _entries.TryGetValue(securityId, out var entry) && Volatile.Read(ref entry.StaleKindMask) != 0;
    }

    /// <summary>Counter exposed to metrics: snapshot heals where snapshotRptSeq lagged the live high-water.</summary>
    public long LaggingSnapshotCount => Volatile.Read(ref _staleSnapshotIgnored);

    /// <summary>
    /// Single-pass aggregate: total symbols with at least one Stale kind, plus per-kind counts.
    /// Callers needing this at high frequency should cache externally with a short TTL.
    /// </summary>
    public AggregateSnapshot GetAggregateSnapshot()
    {
        var perKind = new int[KindCount];
        int totalStaleSymbols = 0;
        foreach (var kv in _entries)
        {
            int mask = Volatile.Read(ref kv.Value.StaleKindMask);
            if (mask == 0) continue;
            totalStaleSymbols++;
            for (int i = 0; i < KindCount; i++)
                if ((mask & (1 << i)) != 0) perKind[i]++;
        }
        return new AggregateSnapshot(totalStaleSymbols, _entries.Count, perKind);
    }

    public readonly record struct AggregateSnapshot(int TotalStaleSymbols, int TotalSymbols, int[] StaleByKind)
    {
        public int StaleOf(SymbolGapKind kind) => StaleByKind[(int)kind];
    }
}
