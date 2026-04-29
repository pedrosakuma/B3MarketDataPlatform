namespace B3.Umdf.Book;

public sealed class CompositeBookEventHandler : IBookEventHandler
{
    private readonly IBookEventHandler[] _handlers;

    public CompositeBookEventHandler(params IBookEventHandler[] handlers)
    {
        _handlers = handlers;
    }

    public void OnOrderAdded(OrderBook book, in OrderBookEntry entry)
    {
        foreach (var h in _handlers) h.OnOrderAdded(book, in entry);
    }

    public void OnOrderUpdated(OrderBook book, in OrderBookEntry entry)
    {
        foreach (var h in _handlers) h.OnOrderUpdated(book, in entry);
    }

    public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side)
    {
        foreach (var h in _handlers) h.OnOrderDeleted(book, orderId, side);
    }

    public void OnPriceLevelChanged(OrderBook book, BookSideType side, long price)
    {
        foreach (var h in _handlers) h.OnPriceLevelChanged(book, side, price);
    }

    public void OnMarketTierChanged(OrderBook book, BookSideType side, long totalQuantity, int orderCount)
    {
        foreach (var h in _handlers) h.OnMarketTierChanged(book, side, totalQuantity, orderCount);
    }

    public void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs)
    {
        foreach (var h in _handlers) h.OnTrade(securityId, price, quantity, tradeId, sendingTimeNs);
    }

    public void OnBookCleared(ulong securityId, BookClearSide side)
    {
        foreach (var h in _handlers) h.OnBookCleared(securityId, side);
    }

    public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs)
    {
        foreach (var h in _handlers) h.OnForwardTrade(securityId, price, quantity, tradeId, sendingTimeNs);
    }

    public void OnTradeBust(ulong securityId, long price, long quantity, long tradeId)
    {
        foreach (var h in _handlers) h.OnTradeBust(securityId, price, quantity, tradeId);
    }

    public void OnExecutionSummary(ulong securityId, long lastPx, long fillQty)
    {
        foreach (var h in _handlers) h.OnExecutionSummary(securityId, lastPx, fillQty);
    }

    public void OnSymbolStaleStatusChanged(ulong securityId, bool isStale)
    {
        foreach (var h in _handlers) h.OnSymbolStaleStatusChanged(securityId, isStale);
    }

    public void OnEndOfEvent(ulong securityId)
    {
        foreach (var h in _handlers) h.OnEndOfEvent(securityId);
    }

    public void OnBatchComplete()
    {
        foreach (var h in _handlers) h.OnBatchComplete();
    }

    public void OnEpochReset(SnapshotClearReason reason)
    {
        foreach (var h in _handlers) h.OnEpochReset(reason);
    }

    public void FlushIfDue()
    {
        foreach (var h in _handlers) h.FlushIfDue();
    }

    public void FlushNow()
    {
        foreach (var h in _handlers) h.FlushNow();
    }
}
