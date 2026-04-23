namespace B3.Umdf.Book;

/// <summary>
/// Per-symbol per-kind recovery state. Source of truth lives in
/// <see cref="SymbolStateRegistry"/>; consumers (BookManager,
/// MarketDataManager, fanout, frontend) query the registry rather than
/// holding shadow copies.
/// </summary>
/// <remarks>
/// State machine (per (SecurityID, <see cref="SymbolGapKind"/>)):
/// <list type="bullet">
/// <item><description><b>Unknown</b>: initial state. No message of this kind ever applied.
/// First incremental establishes baseline (no gap claim) and transitions to Healthy.
/// Snapshot also transitions Unknown → Healthy.</description></item>
/// <item><description><b>Stale</b>: gap detected (received rptSeq &gt; lastSeen + 1) or
/// <see cref="SymbolStateRegistry.MarkAllStale(string)"/> called (catastrophic reset).
/// Incremental messages are buffered (MBO) or dropped-with-resync-on-next (stats)
/// until a snapshot or fresh contiguous stream re-establishes the baseline.</description></item>
/// <item><description><b>Healthy</b>: contiguous stream confirmed. Messages applied
/// directly to book/info.</description></item>
/// </list>
/// </remarks>
public enum SymbolState : byte
{
    Unknown = 0,
    Stale = 1,
    Healthy = 2,
}
