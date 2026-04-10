namespace B3.Umdf.Book;

public interface IBookEventHandler
{
    void OnOrderAdded(OrderBook book, OrderBookEntry entry);
    void OnOrderUpdated(OrderBook book, OrderBookEntry entry);
    void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side);
    void OnTrade(ulong securityId, long price, long quantity, long tradeId);
    void OnBookCleared(ulong securityId);
}
