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

    /// <summary>
    /// Catastrophic per-channel reset: every (symbol, kind) is being moved
    /// to <see cref="SymbolState.Unknown"/> and all books have been cleared
    /// (per-book <see cref="OnBookCleared"/> notifications still fire for
    /// each affected symbol). Fired after the cleanup completes.
    /// Triggered by <see cref="SnapshotClearReason.SequenceVersionChanged"/>
    /// (B3 spec §6.5.5.1 weekly rollover / failover),
    /// <see cref="SnapshotClearReason.ChannelReset"/> (ChannelReset_11), or
    /// <see cref="SnapshotClearReason.SequenceReset"/> (SequenceReset_1).
    /// Default implementation is a no-op; consumers that maintain per-symbol
    /// derived state outside the order book (stats, conflation queues,
    /// caches) SHOULD use this to invalidate that state.
    /// </summary>
    void OnEpochReset(SnapshotClearReason reason) { }
}
