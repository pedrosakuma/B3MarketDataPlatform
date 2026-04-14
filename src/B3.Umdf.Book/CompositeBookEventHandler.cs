namespace B3.Umdf.Book;

public sealed class CompositeBookEventHandler : IBookEventHandler
{
    private readonly IBookEventHandler[] _handlers;

    public CompositeBookEventHandler(params IBookEventHandler[] handlers)
    {
        _handlers = handlers;
    }

    public void OnOrderAdded(OrderBook book, OrderBookEntry entry)
    {
        foreach (var h in _handlers) h.OnOrderAdded(book, entry);
    }

    public void OnOrderUpdated(OrderBook book, OrderBookEntry entry)
    {
        foreach (var h in _handlers) h.OnOrderUpdated(book, entry);
    }

    public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side)
    {
        foreach (var h in _handlers) h.OnOrderDeleted(book, orderId, side);
    }

    public void OnTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        foreach (var h in _handlers) h.OnTrade(securityId, price, quantity, tradeId);
    }

    public void OnBookCleared(ulong securityId, BookClearSide side)
    {
        foreach (var h in _handlers) h.OnBookCleared(securityId, side);
    }

    public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        foreach (var h in _handlers) h.OnForwardTrade(securityId, price, quantity, tradeId);
    }

    public void OnTradeBust(ulong securityId, long price, long quantity, long tradeId)
    {
        foreach (var h in _handlers) h.OnTradeBust(securityId, price, quantity, tradeId);
    }

    public void OnExecutionSummary(ulong securityId, long lastPx, long fillQty)
    {
        foreach (var h in _handlers) h.OnExecutionSummary(securityId, lastPx, fillQty);
    }

    public void OnBatchComplete()
    {
        foreach (var h in _handlers) h.OnBatchComplete();
    }
}
