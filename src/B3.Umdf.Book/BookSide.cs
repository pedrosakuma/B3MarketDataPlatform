using System.Diagnostics;

namespace B3.Umdf.Book;

public sealed class BookSide
{
    private readonly BookSideType _side;
    private readonly Dictionary<ulong, OrderBookEntry> _orders = new();

    // Price levels sorted with BEST at the END of the list.
    // Bids: ascending (lowest→highest, best=highest at end)
    // Asks: descending (highest→lowest, best=lowest at end)
    // Most activity is near top-of-book → inserts/removes near the end = O(1) amortized.
    private readonly List<(long Price, List<OrderBookEntry> Orders)> _levels = new();
    private readonly bool _ascending;

    public BookSideType Side => _side;
    public int OrderCount => _orders.Count;
    public IReadOnlyDictionary<ulong, OrderBookEntry> Orders => _orders;
    public int LevelCount => _levels.Count;

    /// <summary>
    /// Iterates price levels in best→worst order (from end of array).
    /// </summary>
    public IEnumerable<KeyValuePair<long, List<OrderBookEntry>>> PriceLevels
    {
        get
        {
            for (int i = _levels.Count - 1; i >= 0; i--)
                yield return new(_levels[i].Price, _levels[i].Orders);
        }
    }

    /// <summary>
    /// Direct access to orders at a given price level via binary search. O(log n).
    /// </summary>
    public List<OrderBookEntry>? GetOrdersAtPrice(long price)
    {
        int idx = BinarySearchPrice(price);
        return idx >= 0 ? _levels[idx].Orders : null;
    }

    public BookSide(BookSideType side)
    {
        _side = side;
        _ascending = side == BookSideType.Bid;
    }

    /// <returns>true if the order already existed (update); false if new (add).</returns>
    public bool AddOrUpdate(OrderBookEntry entry)
    {
        bool isUpdate = _orders.TryGetValue(entry.OrderId, out var existing);
        if (isUpdate)
            RemoveFromPriceLevels(existing!);

        _orders[entry.OrderId] = entry;
        AddToPriceLevels(entry);
        AssertValid();
        return isUpdate;
    }

    public bool Remove(ulong orderId)
    {
        if (!_orders.Remove(orderId, out var entry))
            return false;
        RemoveFromPriceLevels(entry);
        AssertValid();
        return true;
    }

    public void Clear()
    {
        _orders.Clear();
        _levels.Clear();
    }

    public (long Price, long TotalQty)? BestPrice()
    {
        if (_levels.Count == 0)
            return null;
        var best = _levels[^1];
        long totalQty = 0;
        foreach (var o in best.Orders)
            totalQty += o.Quantity;
        return (best.Price, totalQty);
    }

    /// <summary>
    /// Validates internal consistency between _orders and _levels.
    /// Returns a list of errors (empty if valid).
    /// </summary>
    public List<string> Validate()
    {
        var errors = new List<string>();

        // 1. No empty price levels
        foreach (var (price, list) in _levels)
        {
            if (list.Count == 0)
                errors.Add($"Empty price level at {price}");
        }

        // 2. Price levels must be sorted (worst→best)
        for (int i = 1; i < _levels.Count; i++)
        {
            int cmp = _ascending
                ? _levels[i - 1].Price.CompareTo(_levels[i].Price)
                : _levels[i].Price.CompareTo(_levels[i - 1].Price);
            if (cmp >= 0)
                errors.Add($"Price levels not sorted: [{i - 1}]={_levels[i - 1].Price}, [{i}]={_levels[i].Price}");
        }

        // 3. Every order in _orders must be in exactly one price level at the correct price
        var ordersInLevels = new Dictionary<ulong, long>();
        foreach (var (price, list) in _levels)
        {
            for (int i = 0; i < list.Count; i++)
            {
                var entry = list[i];

                if (entry.Price != price)
                    errors.Add($"Order {entry.OrderId} at price level {price} has entry.Price={entry.Price}");

                if (entry.PriceLevelIndex != i)
                    errors.Add($"Order {entry.OrderId} has PriceLevelIndex={entry.PriceLevelIndex} but is at position {i}");

                if (!ordersInLevels.TryAdd(entry.OrderId, price))
                    errors.Add($"Order {entry.OrderId} appears in multiple price levels ({ordersInLevels[entry.OrderId]} and {price})");
            }
        }

        // 4. Counts must match
        if (_orders.Count != ordersInLevels.Count)
            errors.Add($"Order count mismatch: _orders={_orders.Count}, priceLevels total={ordersInLevels.Count}");

        // 5. Every order in _orders must exist in price levels
        foreach (var (orderId, entry) in _orders)
        {
            if (!ordersInLevels.ContainsKey(orderId))
                errors.Add($"Order {orderId} in _orders but missing from price levels");
        }

        // 6. Every order in price levels must exist in _orders
        foreach (var orderId in ordersInLevels.Keys)
        {
            if (!_orders.ContainsKey(orderId))
                errors.Add($"Order {orderId} in price levels but missing from _orders");
        }

        return errors;
    }

    [Conditional("DEBUG")]
    private void AssertValid()
    {
        var errors = Validate();
        if (errors.Count > 0)
            throw new InvalidOperationException($"BookSide({_side}) invalid: {string.Join("; ", errors)}");
    }

    private void AddToPriceLevels(OrderBookEntry entry)
    {
        int idx = BinarySearchPrice(entry.Price);
        if (idx >= 0)
        {
            var orders = _levels[idx].Orders;
            entry.PriceLevelIndex = orders.Count;
            orders.Add(entry);
        }
        else
        {
            int insertIdx = ~idx;
            var orders = new List<OrderBookEntry> { entry };
            entry.PriceLevelIndex = 0;
            _levels.Insert(insertIdx, (entry.Price, orders));
        }
    }

    private void RemoveFromPriceLevels(OrderBookEntry entry)
    {
        int levelIdx = BinarySearchPrice(entry.Price);
        if (levelIdx < 0) return;

        var orders = _levels[levelIdx].Orders;
        int orderIdx = entry.PriceLevelIndex;

        // Swap-remove: O(1) — swap with last, remove last
        int lastIdx = orders.Count - 1;
        if (orderIdx < lastIdx)
        {
            orders[orderIdx] = orders[lastIdx];
            orders[orderIdx].PriceLevelIndex = orderIdx;
        }
        orders.RemoveAt(lastIdx);

        if (orders.Count == 0)
            _levels.RemoveAt(levelIdx);
    }

    private int BinarySearchPrice(long price)
    {
        int lo = 0, hi = _levels.Count - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            int cmp = _ascending
                ? _levels[mid].Price.CompareTo(price)
                : price.CompareTo(_levels[mid].Price);
            if (cmp == 0) return mid;
            if (cmp < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        return ~lo;
    }
}
