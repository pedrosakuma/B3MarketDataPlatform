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

    /// <summary>
    /// Validates internal consistency between _orders and _priceLevels.
    /// Returns a list of errors (empty if valid).
    /// </summary>
    public List<string> Validate()
    {
        var errors = new List<string>();

        // 1. No empty price levels
        foreach (var (price, list) in _priceLevels)
        {
            if (list.Count == 0)
                errors.Add($"Empty price level at {price}");
        }

        // 2. Every order in _orders must be in exactly one price level at the correct price
        var ordersInLevels = new Dictionary<ulong, long>();
        foreach (var (price, list) in _priceLevels)
        {
            foreach (var entry in list)
            {
                if (entry.Price != price)
                    errors.Add($"Order {entry.OrderId} at price level {price} has entry.Price={entry.Price}");

                if (!ordersInLevels.TryAdd(entry.OrderId, price))
                    errors.Add($"Order {entry.OrderId} appears in multiple price levels ({ordersInLevels[entry.OrderId]} and {price})");
            }
        }

        // 3. Counts must match
        if (_orders.Count != ordersInLevels.Count)
            errors.Add($"Order count mismatch: _orders={_orders.Count}, priceLevels total={ordersInLevels.Count}");

        // 4. Every order in _orders must exist in price levels
        foreach (var (orderId, entry) in _orders)
        {
            if (!ordersInLevels.ContainsKey(orderId))
                errors.Add($"Order {orderId} in _orders but missing from price levels");
        }

        // 5. Every order in price levels must exist in _orders
        foreach (var orderId in ordersInLevels.Keys)
        {
            if (!_orders.ContainsKey(orderId))
                errors.Add($"Order {orderId} in price levels but missing from _orders");
        }

        return errors;
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
