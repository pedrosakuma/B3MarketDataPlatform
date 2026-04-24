using Microsoft.Extensions.Logging;

namespace B3.Umdf.Book;

/// <summary>
/// Per-symbol gap kind. Each value corresponds to an SBE message family that
/// carries its own independent <c>rptSeq</c> counter per security in the B3
/// UMDF v16 schema.
/// </summary>
public enum SymbolGapKind
{
    Mbo = 0,                       // Order_MBO_50, DeleteOrder_MBO_51, MassDeleteOrders_MBO_52
                                   // + Trade_53/ForwardTrade_54/ExecutionSummary_55/TradeBust_57
                                   // (B3 shares one rptSeq stream between book + trade)
    OpeningPrice,                  // tpl 15
    TheoreticalOpeningPrice,       // tpl 16
    ClosingPrice,                  // tpl 17
    AuctionImbalance,              // tpl 19
    QuantityBand,                  // tpl 21
    PriceBand,                     // tpl 22
    HighPrice,                     // tpl 24
    LowPrice,                      // tpl 25
    LastTradePrice,                // tpl 27
    SettlementPrice,               // tpl 28
    OpenInterest,                  // tpl 29
    ExecutionStatistics,           // tpl 56
    SecurityStatus,                // tpl 3
}

/// <summary>
/// Per-symbol gap tracker. Compares the received per-symbol <c>rptSeq</c>
/// against a stored "last seen" value per (secId, kind) and records
/// gap counters / sizes / affected-symbol sets. Pure observability:
/// emits metrics and exposes affected-symbol sets consumed by
/// <see cref="SymbolStateRegistry"/> (which owns the actual state machine
/// and stale transitions).
/// </summary>
public sealed class SymbolGapTracker
{
    private const int KindCount = (int)SymbolGapKind.SecurityStatus + 1;

    private readonly long[] _gapCount = new long[KindCount];
    private readonly long[] _gapSizeSum = new long[KindCount];
    private readonly HashSet<ulong>[] _affectedSymbols;
    private readonly Lock _setsLock = new();
    private readonly ILogger _logger;

    public SymbolGapTracker(ILogger logger)
    {
        _logger = logger;
        _affectedSymbols = new HashSet<ulong>[KindCount];
        for (int i = 0; i < KindCount; i++)
            _affectedSymbols[i] = new HashSet<ulong>();
    }

    /// <summary>
    /// Returns true if a gap was detected (received &gt; expected). The first
    /// observation per (symbol,kind) — when <paramref name="lastSeen"/> is 0
    /// — is treated as a baseline establishment, not a gap.
    /// </summary>
    public bool Observe(ulong securityId, uint received, uint lastSeen, SymbolGapKind kind)
    {
        if (received == 0) return false;          // RptSeq optional/zeroed → ignore
        if (lastSeen == 0) return false;          // first message of this kind for this symbol
        uint expected = lastSeen + 1;
        if (received <= lastSeen) return false;   // duplicate/reordered (handled elsewhere)
        if (received == expected) return false;   // healthy

        int skipped = (int)(received - expected);
        int idx = (int)kind;
        Interlocked.Increment(ref _gapCount[idx]);
        Interlocked.Add(ref _gapSizeSum[idx], skipped);

        bool firstForThisSymbol;
        lock (_setsLock)
        {
            firstForThisSymbol = _affectedSymbols[idx].Add(securityId);
        }

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug(
                "Per-symbol gap: secId={SecurityId} kind={Kind} expected={Expected} received={Received} skipped={Skipped} firstHit={First}",
                securityId, kind, expected, received, skipped, firstForThisSymbol);
        }
        return true;
    }

    public long GapCount(SymbolGapKind kind) => Volatile.Read(ref _gapCount[(int)kind]);

    public long GapSizeSum(SymbolGapKind kind) => Volatile.Read(ref _gapSizeSum[(int)kind]);

    public int AffectedSymbolCount(SymbolGapKind kind)
    {
        lock (_setsLock) return _affectedSymbols[(int)kind].Count;
    }

    public long TotalGapCount()
    {
        long sum = 0;
        for (int i = 0; i < KindCount; i++) sum += Volatile.Read(ref _gapCount[i]);
        return sum;
    }

    public int TotalAffectedSymbolCount()
    {
        lock (_setsLock)
        {
            var union = new HashSet<ulong>();
            for (int i = 0; i < KindCount; i++) union.UnionWith(_affectedSymbols[i]);
            return union.Count;
        }
    }

    /// <summary>
    /// Snapshot the currently-affected-symbol counts per kind (for correlating
    /// with channel-level Recovery events).
    /// </summary>
    public int SnapshotAffectedAtRecoveryStart()
    {
        return TotalAffectedSymbolCount();
    }
}
