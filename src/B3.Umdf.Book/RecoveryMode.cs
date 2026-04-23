namespace B3.Umdf.Book;

/// <summary>
/// Recovery state-machine selection. <c>Channel</c> is the legacy
/// channel-level Recovery (snapshot cycle bridges full group on any gap).
/// <c>PerSymbol</c> is the Phase 2 unified per-symbol Recovery driven by
/// <see cref="SymbolStateRegistry"/>: a gap only marks affected symbols
/// Stale; book/info applies for every other symbol continue uninterrupted.
/// </summary>
public enum RecoveryMode
{
    Channel = 0,
    PerSymbol = 1,
}
