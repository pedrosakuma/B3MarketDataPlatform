namespace B3.MarketData.WebSocketClient;

/// <summary>
/// Phase 1 (issue #43). Opt-in materialized book layer that maintains an
/// in-memory L3 (MBO) book per symbol and exposes a derived L2 top-of-book
/// via <see cref="IBookView"/>. Construct with
/// <see cref="MarketDataClient.CreateBookFeed"/> or the
/// <c>AddMarketDataClient().WithBookFeed()</c> DI extension.
/// </summary>
public interface IBookFeed
{
    /// <summary>
    /// Return the live view for <paramref name="symbol"/>, or <c>null</c>
    /// if no snapshot / order frame has been observed for it yet. The
    /// returned view stays attached to the live state — subsequent calls
    /// to its read methods reflect updates without requiring a re-fetch.
    /// </summary>
    IBookView? GetBook(string symbol);

    /// <summary>
    /// Zero-allocation convenience for the hot path. Equivalent to
    /// <c>GetBook(symbol)?.TryGetTop(out top)</c> with <c>top</c> defaulted
    /// when the book is unknown. Useful inside pegging / risk loops.
    /// </summary>
    bool TryGetTop(string symbol, out L2TopOfBook top);

    /// <summary>
    /// Raised after any state change for the named symbol (snapshot, add,
    /// update, delete, clear, or stale transition). Coarse-grained on
    /// purpose so consumers can decide whether to recompute derived state;
    /// pair with <see cref="GetBook"/> to read the resulting state. Fires
    /// on the client's receive loop — keep handlers fast.
    /// </summary>
    event Action<string>? Changed;
}

/// <summary>
/// Phase 1 (issue #43). Read-only view over the materialized book for one
/// symbol. Reads are thread-safe and lock-only-briefly; do not cache the
/// returned <see cref="L2TopOfBook"/> value across event boundaries —
/// re-read each time you need a fresh quote.
/// </summary>
public interface IBookView
{
    /// <summary>Symbol this view tracks (case as first observed).</summary>
    string Symbol { get; }

    /// <summary>Server security id from the first observed frame.</summary>
    ulong SecurityId { get; }

    /// <summary>
    /// <c>true</c> after the server flagged this symbol stale via
    /// <see cref="SymbolStaleStatusEvent"/>. Cleared by the next
    /// <see cref="BookSnapshotEvent"/> the server emits as recovery completes.
    /// Consumers SHOULD NOT route off-of stale top-of-book.
    /// </summary>
    bool IsStale { get; }

    /// <summary>UTC of the last applied event (snapshot or incremental).</summary>
    DateTime UpdatedUtc { get; }

    /// <summary>
    /// Returns the aggregate top-of-book derived from the live MBO state, or
    /// <c>false</c> when both sides are empty. The returned value is a
    /// snapshot — safe to keep on the stack while the live state advances.
    /// </summary>
    bool TryGetTop(out L2TopOfBook top);
}

/// <summary>
/// Top-of-book aggregate derived from the per-order MBO book. Either side
/// may be a "missing" tuple (price = 0, qty = 0, count = 0) when that side
/// is empty — callers should check <see cref="L2Side.OrderCount"/> &gt; 0
/// before consuming the price.
/// </summary>
public readonly record struct L2TopOfBook(
    string Symbol,
    L2Side Bid,
    L2Side Ask,
    DateTime UpdatedUtc);

/// <summary>Aggregate of one side at one price level.</summary>
public readonly record struct L2Side(
    decimal Price,
    long TotalQty,
    int OrderCount);
