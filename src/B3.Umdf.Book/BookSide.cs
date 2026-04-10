namespace B3.Umdf.Book;

public sealed class BookSide
{
    private readonly BookSideType _side;
    private readonly Dictionary<ulong, OrderBookEntry> _orders = new();
    private readonly SortedDictionary<long, List<OrderBookEntry>> _priceLevels;

    public BookSideType Side => _side;
    public int OrderCount => _orders.Count;
    public IReadOnlyDictionary<ulong, OrderBookEntry> Orders => _orders;
    public SortedDictionary<long, List<OrderBookEntry>> PriceLevels => _priceLevels;

    public BookSide(BookSideType side)
    {
        _side = side;
        _priceLevels = side == BookSideType.Bid
            ? new SortedDictionary<long, List<OrderBookEntry>>(Comparer<long>.Create((a, b) => b.CompareTo(a)))
            : new SortedDictionary<long, List<OrderBookEntry>>();
    }

    public void AddOrUpdate(OrderBookEntry entry)
    {
        if (_orders.TryGetValue(entry.OrderId, out var existing))
            RemoveFromPriceLevels(existing);

        _orders[entry.OrderId] = entry;
        AddToPriceLevels(entry);
    }

    public bool Remove(ulong orderId)
    {
        if (!_orders.Remove(orderId, out var entry))
            return false;
        RemoveFromPriceLevels(entry);
        return true;
    }

    public void Clear()
    {
        _orders.Clear();
        _priceLevels.Clear();
    }

    public (long Price, long TotalQty)? BestPrice()
    {
        foreach (var kvp in _priceLevels)
        {
            long totalQty = 0;
            foreach (var o in kvp.Value)
                totalQty += o.Quantity;
            return (kvp.Key, totalQty);
        }
        return null;
    }

    private void AddToPriceLevels(OrderBookEntry entry)
    {
        if (!_priceLevels.TryGetValue(entry.Price, out var list))
        {
            list = new List<OrderBookEntry>();
            _priceLevels[entry.Price] = list;
        }
        list.Add(entry);
    }

    private void RemoveFromPriceLevels(OrderBookEntry entry)
    {
        if (_priceLevels.TryGetValue(entry.Price, out var list))
        {
            list.Remove(entry);
            if (list.Count == 0)
                _priceLevels.Remove(entry.Price);
        }
    }
}
