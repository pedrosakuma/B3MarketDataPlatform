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
    /// Called after all messages in a packet batch have been processed.
    /// Used as flush signal for upstream conflation buffers.
    /// </summary>
    void OnBatchComplete() { }
}
