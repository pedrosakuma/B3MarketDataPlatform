namespace B3.Umdf.Book;

/// <summary>Which side(s) were cleared by a MassDelete or EmptyBook event.</summary>
public enum BookClearSide : byte
{
    Both = 0,
    Bid = 1,
    Ask = 2,
}

/// <summary>
/// Per-trade flag bitset emitted alongside <see cref="IBookEventHandler.OnTrade"/>
/// and <see cref="IBookEventHandler.OnForwardTrade"/>. Currently signals whether
/// the print was executed during an auction phase (opening cross or pre-open /
/// final-closing-call). Reserved bits are kept 0 for forward compatibility.
/// </summary>
[System.Flags]
public enum TradeFlags : byte
{
    None = 0,
    /// <summary>
    /// Trade is an auction print: either <c>TradeCondition.OpeningPrice</c>
    /// (opening / reopening cross) or the security's <c>TradingStatus</c> is in
    /// an auction phase (<c>RESERVED</c> / <c>FINAL_CLOSING_CALL</c>) at the
    /// time of the trade.
    /// </summary>
    AuctionPrint = 1,
}

public interface IBookEventHandler
{
    void OnOrderAdded(OrderBook book, in OrderBookEntry entry);
    void OnOrderUpdated(OrderBook book, in OrderBookEntry entry);
    void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side);

    /// <summary>
    /// Per-price-level dirty notification. Fired by <c>BookManager</c> after every
    /// mutation that touches a level (Add, Update same-price, Update moved — twice
    /// for old+new — and Delete). Consumers MAY consult
    /// <see cref="BookSide.TryGetLevelAggregate"/> at flush time to read the
    /// post-mutation aggregate (TotalQty/Count) and emit MBP frames; if the level
    /// no longer exists, the level was drained and consumers should emit a delete.
    /// Default implementation is a no-op so MBO-only handlers stay unchanged.
    /// </summary>
    void OnPriceLevelChanged(OrderBook book, BookSideType side, long price) { }

    void OnMarketTierChanged(OrderBook book, BookSideType side, long totalQuantity, int orderCount) { }
    void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs);

    /// <summary>
    /// Flag-aware overload called by <c>BookManager</c>. Default implementation
    /// forwards to the legacy <see cref="OnTrade(ulong,long,long,long,long)"/>
    /// (dropping <paramref name="flags"/>) so existing implementers stay source-
    /// and binary-compatible. Implementers that want to surface AuctionPrint
    /// (or any future <see cref="TradeFlags"/> bit) override this overload.
    /// </summary>
    void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs, TradeFlags flags)
        => OnTrade(securityId, price, quantity, tradeId, sendingTimeNs);
    void OnBookCleared(ulong securityId, BookClearSide side);

    void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs) { }

    /// <summary>
    /// Flag-aware overload for <see cref="OnForwardTrade(ulong,long,long,long,long)"/>.
    /// Default implementation forwards to the legacy method, dropping
    /// <paramref name="flags"/>.
    /// </summary>
    void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs, TradeFlags flags)
        => OnForwardTrade(securityId, price, quantity, tradeId, sendingTimeNs);
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

    /// <summary>
    /// Optional hook for handlers that defer wire fan-out (server-side temporal
    /// flush window). Forwarded by the dispatch loop on idle wakeups; default
    /// no-op preserves legacy behavior.
    /// </summary>
    void FlushIfDue() { }

    /// <summary>
    /// Unconditional shutdown drain. Default no-op preserves legacy behavior.
    /// </summary>
    void FlushNow() { }
}
