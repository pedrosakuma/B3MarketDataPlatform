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
    private readonly IClock _clock;
    private Action<ulong, bool>? _onMboStaleStatusChanged;
    private long _staleSnapshotIgnored;
    private long _staleAuthoritativeReset;
    private long _lastAuthResetUnsafeDelta;
    private long _maxAuthResetUnsafeDelta;
    private long _sumAuthResetUnsafeDelta;
    private long _lastAuthResetDiscardedTailDelta;
    private long _maxAuthResetDiscardedTailDelta;
    private long _sumAuthResetDiscardedTailDelta;

    /// <summary>
    /// Stuck-Stale escape valve: when a symbol has been Stale longer than this
    /// many milliseconds AND a snapshot arrives that would be rejected as
    /// too-old (snapshotRptSeq &lt; MinHeal), accept it as authoritative reset
    /// instead — discard the buffered tail (caller's drain window will be
    /// empty) and rebuild from the snapshot. 0 disables the escape (legacy
    /// behavior: always reject too-old snapshots). Default 60 000 ms.
    /// </summary>
    public long StaleEscapeTimeoutMs { get; set; }

    // O(1) cheap aggregates for the fanout backpressure gate. Maintained
    // atomically on every (symbol, kind) state transition so callers on the
    // hot path (GroupConflationHandler.OnBatchComplete) avoid scanning
    // _entries on every batch (ConcurrentDictionary.Count is O(N) under
    // table lock).
    private int _knownSymbolCount;
    private int _staleSymbolCount;

    public SymbolStateRegistry(ILogger logger)
        : this(logger, SystemClock.Instance) { }

    public SymbolStateRegistry(ILogger logger, IClock clock)
    {
        _logger = logger;
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
    }

    /// <summary>
    /// Optional callback fired on Mbo state transitions Healthy↔Stale. Invoked
    /// OUTSIDE the per-symbol lock (after the mutating critical section completes)
    /// so the handler is free to do non-trivial work without risk of feed-thread
    /// contention. Used by BookManager to emit
    /// <see cref="IBookEventHandler.OnSymbolStaleStatusChanged"/> regardless of
    /// which kind triggered the transition (e.g. a stat exposing a global gap
    /// must surface the Mbo stale status to the frontend just as an MBO gap would).
    /// Set once during wiring; safe to leave null in tests that don't need the signal.
    /// </summary>
    public void SetMboStaleStatusCallback(Action<ulong, bool>? callback)
    {
        _onMboStaleStatusChanged = callback;
    }

    /// <summary>Per-symbol state holder. Mutations require holding <see cref="Sync"/>.</summary>
    public sealed class SymbolEntry
    {
        internal readonly Lock Sync = new();
        internal readonly SymbolState[] States = new SymbolState[KindCount];
        internal readonly uint[] LastRptSeq = new uint[KindCount];
        internal readonly long[] StaleSinceTicks = new long[KindCount];

        // Lower bound for snapshot rptSeq accepted by HealFromSnapshot. Set whenever
        // we transition into a state where our buffer cannot reconstruct history
        // before some rptSeq:
        //   - Unknown→Buffer (cold-start): set to (firstObserved - 1). A snapshot
        //     older than this would leave a gap between snapshotRptSeq+1 and our
        //     first observed rptSeq that we never saw and cannot replay.
        //   - Healthy→Stale (mid-session gap): set to lastSeenBeforeStale. A snapshot
        //     older than this would re-discard already-applied state and leave a gap
        //     in the pre-stale rptSeq range.
        // 0 means "no constraint" (any snapshot accepted).
        internal readonly uint[] MinHealRptSeq = new uint[KindCount];

        // Mask of kinds currently Stale — maintained inside the lock for
        // O(1) IsAnyStale lookups and aggregate counting without scanning
        // all 14 slots.
        internal int StaleKindMask;

        // Maximum rptSeq observed across ANY kind for this symbol. Per B3 UMDF spec
        // (and confirmed by PCAP analysis: 175k cross-template advances, 0 violations
        // in 200k packets), rptSeq is ONE counter per SecurityID shared across all
        // message families (MBO, Trade, all stats). This field is the wire-truth
        // baseline used for global-gap detection — when received > Observed+1 we know
        // SOMEONE was lost (we don't know which kind), so the conservative response is
        // to stale the Mbo bucket because the loss MAY have been a book-mutating
        // message. This eliminates false-positive Stales caused by per-kind tracking
        // (where a stat advancing the global counter made the next MBO look like a gap).
        internal uint ObservedRptSeq;
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

        var entry = GetOrAddEntry(securityId);
        int idx = (int)kind;
        var (bootPolicy, _) = DefaultPolicies[idx];

        ObserveResult result;
        bool fireMboStaleCallback;
        lock (entry.Sync)
        {
            var prev = entry.States[idx];
            uint lastSeen = entry.LastRptSeq[idx];
            uint observed = entry.ObservedRptSeq;

            // Step 1 — global-gap detection (kind-agnostic). May force Mbo Stale.
            var gap = DetectAndApplyGlobalGap(entry, kind, receivedRptSeq, observed);
            fireMboStaleCallback = gap.MboForcedStale;
            if (gap.MboForcedStale && kind == SymbolGapKind.Mbo)
                prev = SymbolState.Stale;

            // Update observed watermark monotonically.
            if (receivedRptSeq > observed) entry.ObservedRptSeq = receivedRptSeq;

            // Step 2 — per-kind dispatch. Per-kind gap detection is intentionally
            // absent: the global watermark above is the single source of truth.
            // The Healthy branch only handles duplicate suppression and forward apply.
            result = DispatchByState(entry, idx, kind, receivedRptSeq, lastSeen, prev, bootPolicy, gap);
        }

        // Invoke the (Mbo) stale-status callback OUTSIDE the per-symbol lock to
        // honor the "callback must be cheap and non-blocking" contract without
        // holding the hot-path lock if a future handler ever does I/O.
        if (fireMboStaleCallback) _onMboStaleStatusChanged?.Invoke(securityId, true);

        return result;
    }

    /// <summary>
    /// rptSeq is shared per-instrument across all templates (PCAP-confirmed:
    /// 175k cross-template advances, 0 violations in 200k packets). When any
    /// message arrives with <c>received &gt; observed+1</c>, SOME message was
    /// lost — but we don't know which kind. Since the loss MAY have been a
    /// book-mutating MBO, conservatively force the Mbo bucket Stale (if
    /// currently Healthy) regardless of which kind exposed the gap. Snapshot
    /// heal is the only path back.
    /// <para>This eliminates false-positive Stales caused by per-kind tracking:
    /// an interleaved stat advancing the wire counter no longer makes the next
    /// MBO look like a per-kind gap.</para>
    /// <para>MUST be called with <c>entry.Sync</c> held.</para>
    /// </summary>
    private GapInfo DetectAndApplyGlobalGap(SymbolEntry entry, SymbolGapKind triggerKind, uint receivedRptSeq, uint observed)
    {
        bool globalGap = observed > 0 && receivedRptSeq > observed + 1;
        if (!globalGap) return default;

        uint gapSize = receivedRptSeq - observed - 1;
        const int mboIdx = (int)SymbolGapKind.Mbo;
        if (entry.States[mboIdx] != SymbolState.Healthy)
            return new GapInfo(true, gapSize, false);

        // Tighten MinHeal[Mbo] to (received - 1) on the Healthy→Stale transition.
        // Subsequent gaps while already Stale do NOT bump MinHeal — the
        // StaleMboBuffer eviction callback (BumpMinHeal) is responsible for
        // advancing it when buffered entries are dropped.
        uint mboMin = receivedRptSeq - 1;
        if (mboMin > entry.MinHealRptSeq[mboIdx])
            entry.MinHealRptSeq[mboIdx] = mboMin;

        int prevMask = entry.StaleKindMask;
        entry.States[mboIdx] = SymbolState.Stale;
        entry.StaleSinceTicks[mboIdx] = _clock.NowTicks;
        entry.StaleKindMask |= 1 << mboIdx;
        if (prevMask == 0) Interlocked.Increment(ref _staleSymbolCount);

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug(
                "SymbolState: GLOBAL gap → Mbo Healthy→Stale viaKind={TriggerKind} observed={Observed} received={Received} gap={Gap}",
                triggerKind, observed, receivedRptSeq, gapSize);

        return new GapInfo(true, gapSize, true);
    }

    /// <summary>
    /// Per-state dispatch. Returns the action the caller should take with the
    /// triggering message. MUST be called with <c>entry.Sync</c> held.
    /// </summary>
    private static ObserveResult DispatchByState(
        SymbolEntry entry, int idx, SymbolGapKind kind, uint receivedRptSeq, uint lastSeen,
        SymbolState prev, BootstrapPolicy bootPolicy, GapInfo gap)
    {
        switch (prev)
        {
            case SymbolState.Unknown:
                if (bootPolicy == BootstrapPolicy.AcceptFirst)
                {
                    entry.LastRptSeq[idx] = receivedRptSeq;
                    entry.States[idx] = SymbolState.Healthy;
                    return ObserveResult.ApplyHealthy;
                }
                // RequireSnapshot: track high-water for post-heal drain, buffer the message.
                if (receivedRptSeq > lastSeen)
                    entry.LastRptSeq[idx] = receivedRptSeq;
                // Anchor the minimum acceptable snapshot baseline to (firstObserved - 1)
                // so a snapshot older than our first live observation is rejected
                // (we cannot replay messages we never saw).
                if (entry.MinHealRptSeq[idx] == 0 && receivedRptSeq > 0)
                    entry.MinHealRptSeq[idx] = receivedRptSeq - 1;
                return new ObserveResult(SymbolState.Unknown, ObserveAction.Buffer, false, false, 0);

            case SymbolState.Healthy:
                if (receivedRptSeq <= lastSeen)
                    return new ObserveResult(SymbolState.Healthy, ObserveAction.Drop, false, false, 0);
                entry.LastRptSeq[idx] = receivedRptSeq;
                return new ObserveResult(SymbolState.Healthy, ObserveAction.Apply,
                    TransitionedToStale: false, TransitionedToHealthy: false,
                    GapSize: gap.GlobalGap ? gap.Size : 0);

            case SymbolState.Stale:
                if (receivedRptSeq > lastSeen)
                    entry.LastRptSeq[idx] = receivedRptSeq;
                bool surfaceStaleTransition = gap.MboForcedStale && kind == SymbolGapKind.Mbo;
                return new ObserveResult(SymbolState.Stale, ObserveAction.Buffer,
                    TransitionedToStale: surfaceStaleTransition,
                    TransitionedToHealthy: false,
                    GapSize: surfaceStaleTransition ? gap.Size : 0);

            default:
                return new ObserveResult(prev, ObserveAction.Drop, false, false, 0);
        }
    }

    /// <summary>Output of <see cref="DetectAndApplyGlobalGap"/>.</summary>
    private readonly record struct GapInfo(bool GlobalGap, uint Size, bool MboForcedStale);

    /// <summary>
    /// Outcome of <see cref="HealFromSnapshot"/>.
    /// <see cref="Accepted"/> is false when the snapshot was rejected because its
    /// <see cref="SnapshotRptSeq"/> is older than the symbol's
    /// <c>MinHealRptSeq</c> (caller must not transition state nor drain buffer in
    /// that case — symbol stays Stale/Unknown awaiting a fresher snapshot).
    /// When accepted, <see cref="DrainFrom"/> is the rptSeq immediately after the
    /// snapshot baseline; <see cref="DrainTo"/> is the highest rptSeq observed
    /// while Stale/Unknown. Caller drains its per-symbol buffer for entries with
    /// <c>rptSeq ∈ [DrainFrom, DrainTo]</c>.
    /// </summary>
    public readonly record struct HealResult(
        bool Accepted,
        bool TransitionedToHealthy,
        uint SnapshotRptSeq,
        uint DrainFrom,
        uint DrainTo);

    /// <summary>
    /// Snapshot path: forces (secId, kind) to <see cref="SymbolState.Healthy"/>
    /// at the given rptSeq when the snapshot is fresh enough to bridge our
    /// buffered window. Caller must drain its per-symbol buffer using the
    /// returned <see cref="HealResult"/> when <see cref="HealResult.Accepted"/>
    /// is true. When rejected, the symbol stays Stale and the caller waits
    /// for the next snapshot rotation — never accepting a snapshot that would
    /// leave a hole in the reconstructed book state.
    /// </summary>
    public HealResult HealFromSnapshot(ulong securityId, SymbolGapKind kind, uint snapshotRptSeq)
    {
        var entry = GetOrAddEntry(securityId);
        int idx = (int)kind;
        HealResult result;
        bool fireMboHealedCallback = false;
        lock (entry.Sync)
        {
            var prev = entry.States[idx];
            uint priorHighWater = entry.LastRptSeq[idx];
            uint minHeal = entry.MinHealRptSeq[idx];
            int prevMask = entry.StaleKindMask;

            // Reject snapshots too old to bridge the gap between the snapshot's
            // last covered rptSeq and our first live observation (Unknown bootstrap)
            // or our last good rptSeq before going Stale (mid-session gap). Healing
            // anyway would leave a hole in the book — corrupt state that surfaces
            // as crossed BBOs and ghost orders. Symbol stays in its current state;
            // caller leaves the snapshot bytes already applied to the book in place
            // (book state == snapshot state) and continues buffering live
            // incrementals until a fresher snapshot arrives.
            //
            // Escape valve: if the symbol has been Stale longer than
            // StaleEscapeTimeoutMs (default 60 s), accept the snapshot as an
            // authoritative reset instead. The buffered tail is presumed
            // un-bridgeable (the floor pin's evict-unsafe path proved we lost
            // data in the drain window) — drop it and rebuild from the
            // snapshot. This breaks the stuck-Stale loop at the cost of
            // discarding the buffered messages between snapshotRptSeq+1 and
            // the live high-water (they will arrive again as live increments
            // and re-establish the book naturally; book state may briefly
            // appear thin until those increments catch up).
            bool authoritativeReset = false;
            if (minHeal > 0 && snapshotRptSeq < minHeal)
            {
                bool eligible = StaleEscapeTimeoutMs > 0
                    && prev == SymbolState.Stale
                    && entry.StaleSinceTicks[idx] != 0
                    && (_clock.NowTicks - entry.StaleSinceTicks[idx]) > StaleEscapeTimeoutMs;

                if (!eligible)
                {
                    Interlocked.Increment(ref _staleSnapshotIgnored);
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug(
                            "SymbolState: rejected lagging snapshot secId={SecurityId} kind={Kind} snapshotRptSeq={Snap} minHeal={MinHeal} priorHighWater={High}",
                            securityId, kind, snapshotRptSeq, minHeal, priorHighWater);
                    return new HealResult(Accepted: false, TransitionedToHealthy: false,
                        SnapshotRptSeq: snapshotRptSeq, DrainFrom: 1, DrainTo: 0);
                }

                authoritativeReset = true;
                Interlocked.Increment(ref _staleAuthoritativeReset);

                // Severity instrumentation. Two distinct deltas:
                //   unsafeDelta        = MinHeal - snap   (proven-unbridgeable gap)
                //   discardedTailDelta = highWater - snap (live tail abandoned)
                // Both are exposed as last/max/sum so dashboards can alert on
                // magnitude (a lone "last" gauge can hide spikes between scrapes).
                long unsafeDelta = (long)minHeal - (long)snapshotRptSeq;
                long discardedTailDelta = (long)priorHighWater - (long)snapshotRptSeq;
                if (unsafeDelta < 0) unsafeDelta = 0;
                if (discardedTailDelta < 0) discardedTailDelta = 0;
                Volatile.Write(ref _lastAuthResetUnsafeDelta, unsafeDelta);
                Volatile.Write(ref _lastAuthResetDiscardedTailDelta, discardedTailDelta);
                Interlocked.Add(ref _sumAuthResetUnsafeDelta, unsafeDelta);
                Interlocked.Add(ref _sumAuthResetDiscardedTailDelta, discardedTailDelta);
                UpdateMaxIfGreater(ref _maxAuthResetUnsafeDelta, unsafeDelta);
                UpdateMaxIfGreater(ref _maxAuthResetDiscardedTailDelta, discardedTailDelta);

                long staleForMs = _clock.NowTicks - entry.StaleSinceTicks[idx];
                _logger.LogWarning(
                    "SymbolState: stuck-Stale escape — accepting too-old snapshot as authoritative reset. secId={SecurityId} kind={Kind} snapshotRptSeq={Snap} minHeal={MinHeal} priorHighWater={High} staleForMs={StaleMs}",
                    securityId, kind, snapshotRptSeq, minHeal, priorHighWater, staleForMs);
            }

            // Defensive: never regress a Healthy symbol whose live stream is already
            // ahead of the snapshot. The BeginSnapshotHeader fast-path (Skipped guard)
            // normally catches this before the snapshot bytes touch the book; this
            // double-check ensures the registry baseline cannot regress even if a
            // future caller forgets the fast-path. Without it, setting baseline=snap
            // (below) would silently drop every subsequent live message in
            // [priorHighWater+1 .. snap] until the live stream catches up.
            if (prev == SymbolState.Healthy && snapshotRptSeq <= priorHighWater)
            {
                Interlocked.Increment(ref _staleSnapshotIgnored);
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug(
                        "SymbolState: ignored Healthy-ahead snapshot secId={SecurityId} kind={Kind} snapshotRptSeq={Snap} priorHighWater={High}",
                        securityId, kind, snapshotRptSeq, priorHighWater);
                return new HealResult(Accepted: false, TransitionedToHealthy: false,
                    SnapshotRptSeq: snapshotRptSeq, DrainFrom: 1, DrainTo: 0);
            }

            // CRITICAL: baseline = snapshotRptSeq (NOT max(snap, highWater)).
            //
            // The snapshot rebuilt the book at state-as-of-snap. The caller now drains
            // the per-symbol buffer for [snap+1 .. highWater]. Each replayed message
            // re-enters Observe via the dispatch path (HandleOrder → RouteMbo →
            // Observe). For replay to reach the Apply branch, baseline must equal
            // snap — then snap+1 is contiguous, snap+2 is contiguous, and so on,
            // advancing the baseline naturally to the highest replayed rptSeq.
            //
            // The previous Math.Max(snap, highWater) implementation set baseline to
            // highWater immediately, causing every replayed message to satisfy
            // received <= lastSeen → DROP path in Observe. The drain became a silent
            // no-op: book stayed at snapshot state, all buffered operations
            // [snap+1 .. highWater] were lost. This corrupted the book whenever a
            // symbol healed from Stale (the most common heal scenario).
            entry.LastRptSeq[idx] = snapshotRptSeq;
            entry.States[idx] = SymbolState.Healthy;
            entry.MinHealRptSeq[idx] = 0; // baseline restored — no constraint until next stale event

            // Bump observed to at least snapshotRptSeq. Snapshots are an authoritative
            // "instrument is at rptSeq=N" signal from the wire (lower bound — live may
            // be ahead). Without this bump, a heal followed by a live message at
            // lastSeen+ε would bypass global-gap detection because observed lagged.
            if (snapshotRptSeq > entry.ObservedRptSeq) entry.ObservedRptSeq = snapshotRptSeq;

            bool transitioned = prev != SymbolState.Healthy;
            if (transitioned)
            {
                entry.StaleKindMask &= ~(1 << idx);
                entry.StaleSinceTicks[idx] = 0;
                if (prevMask != 0 && entry.StaleKindMask == 0)
                    Interlocked.Decrement(ref _staleSymbolCount);
                // Surface Stale→Healthy for the Mbo bucket only when the symbol becomes
                // fully Healthy (StaleKindMask == 0) AND prev was actually Stale (not
                // Unknown). Unknown→Healthy is bootstrap, not a stale-recovery event.
                if (kind == SymbolGapKind.Mbo && prev == SymbolState.Stale && entry.StaleKindMask == 0)
                    fireMboHealedCallback = true;
            }

            // Drain window: anything strictly above snapshotRptSeq up to the
            // observed high-water mark must be replayed by the caller. When
            // taking the authoritative-reset escape, the buffered tail is
            // un-bridgeable (we lost data inside [snapshotRptSeq+1, highWater]
            // — that is precisely why minHeal moved past snapshotRptSeq) so
            // we signal an empty drain (DrainFrom > DrainTo) and the caller
            // discards its per-symbol buffer.
            //
            // CONTRACT: when DrainFrom > DrainTo, the caller MUST clear its
            // per-symbol stale buffer (no entries are bridgeable). The empty
            // drain is currently produced by two paths: (a) authoritative
            // reset, and (b) a snapshot whose rptSeq is at-or-above the live
            // high-water (no buffered tail to apply). BookManager honors this
            // by calling _staleBuffer.Clear(securityId) in the empty-drain
            // branch of its caller.
            uint drainFrom = snapshotRptSeq + 1;
            uint drainTo = (!authoritativeReset && priorHighWater > snapshotRptSeq)
                ? priorHighWater
                : snapshotRptSeq;
            result = new HealResult(Accepted: true, TransitionedToHealthy: transitioned,
                SnapshotRptSeq: snapshotRptSeq, DrainFrom: drainFrom, DrainTo: drainTo);
        }

        // Invoke the (Mbo) healed callback OUTSIDE the per-symbol lock — same
        // discipline as Observe's stale-status callback path.
        if (fireMboHealedCallback) _onMboStaleStatusChanged?.Invoke(securityId, false);

        return result;
    }

    /// <summary>
    /// Authoritative empty-book heal for the illiquid case (B3 spec §7.4):
    /// the wire publishes a snapshot with <c>LastRptSeq</c> absent AND
    /// <c>TotNumBids+TotNumOffers == 0</c>, asserting the instrument is empty.
    /// Always accepted regardless of <see cref="SymbolEntry.MinHealRptSeq"/> —
    /// the empty payload is itself proof that any cross-kind global gap that
    /// flipped this kind Stale did NOT leave persistent book state behind
    /// (whatever orders may have been lost are gone from the venue too).
    ///
    /// <para>The post-heal baseline is the maximum of (per-kind LastRptSeq,
    /// per-kind MinHealRptSeq, per-symbol ObservedRptSeq) so any late
    /// pre-snapshot MBO packet with a smaller rptSeq is dropped via the normal
    /// <see cref="SymbolState.Healthy"/> drop branch (received &lt;= lastSeen)
    /// rather than resurrecting ghost orders into the empty book.</para>
    ///
    /// <para>Caller MUST clear its per-symbol buffer for <paramref name="securityId"/>
    /// — the empty drain window <see cref="HealResult.DrainFrom"/> &gt;
    /// <see cref="HealResult.DrainTo"/> communicates this.</para>
    /// </summary>
    public HealResult HealFromIlliquidEmptySnapshot(ulong securityId, SymbolGapKind kind)
    {
        var entry = GetOrAddEntry(securityId);
        int idx = (int)kind;
        bool fireMboHealedCallback = false;
        HealResult result;
        lock (entry.Sync)
        {
            var prev = entry.States[idx];
            int prevMask = entry.StaleKindMask;

            // Anchor baseline above any prior observation so late pre-snapshot
            // MBO packets are dropped rather than applied to the empty book.
            uint anchor = entry.LastRptSeq[idx];
            if (entry.MinHealRptSeq[idx] > anchor) anchor = entry.MinHealRptSeq[idx];
            if (entry.ObservedRptSeq > anchor) anchor = entry.ObservedRptSeq;

            entry.LastRptSeq[idx] = anchor;
            entry.States[idx] = SymbolState.Healthy;
            entry.MinHealRptSeq[idx] = 0;
            if (anchor > entry.ObservedRptSeq) entry.ObservedRptSeq = anchor;

            bool transitioned = prev != SymbolState.Healthy;
            if (transitioned)
            {
                entry.StaleKindMask &= ~(1 << idx);
                entry.StaleSinceTicks[idx] = 0;
                if (prevMask != 0 && entry.StaleKindMask == 0)
                    Interlocked.Decrement(ref _staleSymbolCount);
                if (kind == SymbolGapKind.Mbo && prev == SymbolState.Stale && entry.StaleKindMask == 0)
                    fireMboHealedCallback = true;
            }

            // Empty drain window — caller drops any buffered tail.
            result = new HealResult(Accepted: true, TransitionedToHealthy: transitioned,
                SnapshotRptSeq: anchor, DrainFrom: 1, DrainTo: 0);
        }

        if (fireMboHealedCallback) _onMboStaleStatusChanged?.Invoke(securityId, false);

        return result;
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
                int prevMask = entry.StaleKindMask;
                for (int i = 0; i < KindCount; i++)
                {
                    if (entry.States[i] != SymbolState.Unknown) affected++;
                    entry.States[i] = SymbolState.Unknown;
                    entry.LastRptSeq[i] = 0;
                    entry.StaleSinceTicks[i] = 0;
                    entry.MinHealRptSeq[i] = 0;
                }
                entry.StaleKindMask = 0;
                // Wire counter restarts on a catastrophic reset (SequenceReset_1 /
                // ChannelReset_11) — clear the observed-rptSeq watermark so the next
                // message at lower rptSeq doesn't trigger a spurious global gap.
                entry.ObservedRptSeq = 0;
                if (prevMask != 0)
                    Interlocked.Decrement(ref _staleSymbolCount);
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
        long now = _clock.NowTicks;
        int affected = 0;
        foreach (var kv in _entries)
        {
            var entry = kv.Value;
            lock (entry.Sync)
            {
                int prevMask = entry.StaleKindMask;
                for (int i = 0; i < KindCount; i++)
                {
                    if (entry.States[i] == SymbolState.Healthy)
                    {
                        entry.States[i] = SymbolState.Stale;
                        entry.StaleSinceTicks[i] = now;
                        entry.StaleKindMask |= 1 << i;
                        // Pin the heal threshold to the current LastRptSeq so a stale
                        // snapshot from before this MarkAllStale event won't re-heal
                        // with a hole.
                        entry.MinHealRptSeq[i] = entry.LastRptSeq[i];
                        affected++;
                    }
                }
                if (prevMask == 0 && entry.StaleKindMask != 0)
                    Interlocked.Increment(ref _staleSymbolCount);
            }
        }
        _logger.LogWarning("SymbolStateRegistry.MarkAllStale: reason={Reason} kindsAffected={Count}", reason, affected);
    }

    /// <summary>
    /// Per-symbol epoch reset, triggered by EmptyBook_9 on the live wire.
    /// EmptyBook is a per-instrument provable empty-state event after which
    /// the B3 wire restarts that instrument's RptSeq counter at 1 (per spec
    /// "EmptyBook resets RptSeq to 1"). Without this reset, the registry
    /// would still hold lastRptSeq=N and the next live message at rptSeq=1
    /// would hit the Healthy.Drop branch (received &lt;= lastSeen), silently
    /// losing every subsequent update for that symbol.
    ///
    /// Sets the symbol/kind to Healthy at baseline=0 so the next message
    /// (rptSeq=1) is contiguous via expected = lastSeen + 1 = 1.
    /// </summary>
    public void ResetSymbolEpoch(ulong securityId, SymbolGapKind kind)
    {
        if (!_entries.TryGetValue(securityId, out var entry))
            return;
        int idx = (int)kind;
        lock (entry.Sync)
        {
            int prevMask = entry.StaleKindMask;
            entry.States[idx] = SymbolState.Healthy;
            entry.LastRptSeq[idx] = 0;
            entry.StaleSinceTicks[idx] = 0;
            entry.MinHealRptSeq[idx] = 0;
            entry.StaleKindMask &= ~(1 << idx);
            if (prevMask != 0 && entry.StaleKindMask == 0)
                Interlocked.Decrement(ref _staleSymbolCount);
            // EmptyBook resets the wire-level rptSeq counter for this instrument
            // (per spec). Clear ObservedRptSeq so the next incremental at rpt=1
            // doesn't trigger a spurious global gap against the pre-reset watermark.
            entry.ObservedRptSeq = 0;
        }
    }

    /// <summary>
    /// Advance the minimum-acceptable-heal baseline for a symbol because the
    /// Stale buffer evicted older messages (per-symbol cap exceeded).
    /// After this call, snapshots with <c>lastRptSeq &lt; newMin</c> are
    /// rejected — they would leave a hole between the snapshot and the
    /// buffer's earliest retained message. Idempotent and monotonic
    /// (never lowers MinHeal). Returns true if MinHeal advanced.
    /// </summary>
    public bool BumpMinHeal(ulong securityId, SymbolGapKind kind, uint newMin)
    {
        if (!_entries.TryGetValue(securityId, out var entry)) return false;
        int idx = (int)kind;
        lock (entry.Sync)
        {
            if (newMin > entry.MinHealRptSeq[idx])
            {
                entry.MinHealRptSeq[idx] = newMin;
                return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Pre-populate an entry on SecurityDefinition so the cold-start path
    /// avoids racing GetOrAdd on the feed thread. Idempotent.
    /// </summary>
    public void EnsureRegistered(ulong securityId)
    {
        GetOrAddEntry(securityId);
    }

    /// <summary>Centralized entry creator that maintains the cheap <see cref="KnownSymbolCount"/> counter.</summary>
    private SymbolEntry GetOrAddEntry(ulong securityId)
    {
        if (_entries.TryGetValue(securityId, out var existing)) return existing;
        var fresh = new SymbolEntry();
        if (_entries.TryAdd(securityId, fresh))
        {
            Interlocked.Increment(ref _knownSymbolCount);
            return fresh;
        }
        return _entries[securityId];
    }

    /// <summary>O(1) symbol count maintained on insert. Safe on the hot path
    /// (unlike <c>ConcurrentDictionary.Count</c> which acquires the table lock).</summary>
    public int KnownSymbolCount => Volatile.Read(ref _knownSymbolCount);

    /// <summary>O(1) count of symbols with at least one Stale kind. Maintained
    /// on every entry's <c>StaleKindMask</c> transition between 0 and non-zero.</summary>
    public int StaleSymbolCount => Volatile.Read(ref _staleSymbolCount);

    public SymbolState GetState(ulong securityId, SymbolGapKind kind)
    {
        if (!_entries.TryGetValue(securityId, out var entry)) return SymbolState.Unknown;
        // Single-byte read of an enum-typed array slot: atomic on all supported
        // platforms (.NET memory model guarantees atomicity for ≤ word-sized
        // reads). Avoiding the per-symbol lock here keeps hot-path callers
        // (frontend dimming, Observe's receivedRptSeq=0 fast path) from
        // contending with feed-thread state mutations of the same symbol.
        // Worst case: caller observes a slightly stale value across a
        // concurrent transition — never a torn one.
        return entry.States[(int)kind];
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
    /// Counter exposed to metrics: snapshots accepted as authoritative reset
    /// because the symbol was stuck Stale longer than <see cref="StaleEscapeTimeoutMs"/>.
    /// Sustained growth indicates pathological loss the floor pin alone cannot
    /// absorb; review hot cap sizing or upstream loss rate.
    /// </summary>
    public long StaleAuthoritativeResetCount => Volatile.Read(ref _staleAuthoritativeReset);

    /// <summary>
    /// Severity gauge for the most recent forced heal: <c>MinHealRptSeq - snapshotRptSeq</c>.
    /// Quantifies the proven-unbridgeable gap that the escape valve silenced.
    /// Use alongside <see cref="MaxAuthoritativeResetUnsafeDelta"/> /
    /// <see cref="SumAuthoritativeResetUnsafeDelta"/> — a "last" gauge alone
    /// can hide spikes between metric scrapes.
    /// </summary>
    public uint LastAuthoritativeResetUnsafeDelta => (uint)Volatile.Read(ref _lastAuthResetUnsafeDelta);
    /// <summary>Largest historical <see cref="LastAuthoritativeResetUnsafeDelta"/>; never decreases.</summary>
    public uint MaxAuthoritativeResetUnsafeDelta => (uint)Volatile.Read(ref _maxAuthResetUnsafeDelta);
    /// <summary>Cumulative sum of unsafe deltas across every forced heal.</summary>
    public ulong SumAuthoritativeResetUnsafeDelta => (ulong)Volatile.Read(ref _sumAuthResetUnsafeDelta);

    /// <summary>
    /// Severity gauge for the most recent forced heal: <c>priorHighWater - snapshotRptSeq</c>.
    /// Quantifies the live tail abandoned by the empty-drain signal (caller
    /// drops everything in <c>(snap, priorHighWater]</c>).
    /// </summary>
    public uint LastAuthoritativeResetDiscardedTailDelta => (uint)Volatile.Read(ref _lastAuthResetDiscardedTailDelta);
    /// <summary>Largest historical <see cref="LastAuthoritativeResetDiscardedTailDelta"/>; never decreases.</summary>
    public uint MaxAuthoritativeResetDiscardedTailDelta => (uint)Volatile.Read(ref _maxAuthResetDiscardedTailDelta);
    /// <summary>Cumulative sum of discarded-tail deltas across every forced heal.</summary>
    public ulong SumAuthoritativeResetDiscardedTailDelta => (ulong)Volatile.Read(ref _sumAuthResetDiscardedTailDelta);

    private static void UpdateMaxIfGreater(ref long target, long candidate)
    {
        long observed = Volatile.Read(ref target);
        while (candidate > observed)
        {
            long previous = Interlocked.CompareExchange(ref target, candidate, observed);
            if (previous == observed) return;
            observed = previous;
        }
    }

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
