namespace B3.Umdf.Book;

public interface IBookEventHandler
{
    void OnOrderAdded(OrderBook book, OrderBookEntry entry);
    void OnOrderUpdated(OrderBook book, OrderBookEntry entry);
    void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side);
    void OnTrade(ulong securityId, long price, long quantity, long tradeId);
    void OnBookCleared(ulong securityId);

    void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId) { }
    void OnTradeBust(ulong securityId, long price, long quantity, long tradeId) { }
    void OnExecutionSummary(ulong securityId, long lastPx, long fillQty) { }
}
