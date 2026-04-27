namespace B3.Umdf.Book;

/// <summary>Which side(s) were cleared by a MassDelete or EmptyBook event.</summary>
public enum BookClearSide : byte
{
    Both = 0,
    Bid = 1,
    Ask = 2,
}

public interface IBookEventHandler
{
    void OnOrderAdded(OrderBook book, in OrderBookEntry entry);
    void OnOrderUpdated(OrderBook book, in OrderBookEntry entry);
    void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side);
    void OnMarketTierChanged(OrderBook book, BookSideType side, long totalQuantity, int orderCount) { }
    void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs);
    void OnBookCleared(ulong securityId, BookClearSide side);

    void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs) { }
    void OnTradeBust(ulong securityId, long price, long quantity, long tradeId) { }
    void OnExecutionSummary(ulong securityId, long lastPx, long fillQty) { }

    /// <summary>
    /// Per-symbol heal-state transition. Emitted when a security flips
    /// between Healthy and Stale (any-kind aggregated). Fanout
    /// implementations should buffer and coalesce per security so multiple
    /// flips within a packet collapse to the latest value.
    /// </summary>
    void OnSymbolStaleStatusChanged(ulong securityId, bool isStale) { }

    /// <summary>
    /// Atomic event boundary signal (B3 spec §10 — MatchEventIndicator
    /// EndOfEvent bit). Fired after the last message of an exchange-side
    /// matching event has been applied to the book. Default implementation
    /// is a no-op; consumers MAY use this to flush per-event coalescing
    /// buffers. Currently informational — buffer flushing is still
    /// driven by <see cref="OnBatchComplete"/> at packet boundaries.
    /// </summary>
    void OnEndOfEvent(ulong securityId) { }

    /// <summary>
    /// Called after all messages in a packet batch have been processed.
    /// Used as flush signal for upstream conflation buffers.
    /// </summary>
    void OnBatchComplete() { }
}
