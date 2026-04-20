using System.Diagnostics;
using System.Runtime.InteropServices;

namespace B3.Umdf.Book;

public sealed class BookSide
{
    private readonly BookSideType _side;
    private readonly Dictionary<ulong, OrderBookEntry> _orders = new(128);

    // Price levels sorted with BEST at the END of the list.
    // Bids: ascending (lowest→highest, best=highest at end)
    // Asks: descending (highest→lowest, best=lowest at end)
    // Most activity is near top-of-book → inserts/removes near the end = O(1) amortized.
    private readonly List<(long Price, List<OrderBookEntry> Orders)> _levels = new(64);
    private readonly bool _ascending;
    private readonly Stack<List<OrderBookEntry>> _listPool = new(32);

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
    /// Direct access to orders at a given price level. O(1) amortized for top-of-book.
    /// </summary>
    public List<OrderBookEntry>? GetOrdersAtPrice(long price)
    {
        int idx = FindPriceLevel(price);
        return idx >= 0 ? _levels[idx].Orders : null;
    }

    public bool TryGetOrder(ulong orderId, out OrderBookEntry entry)
        => _orders.TryGetValue(orderId, out entry);

    /// <summary>
    /// Returns a ref to the stored order so callers can mutate fields in-place without copying.
    /// Throws if the order does not exist.
    /// </summary>
    public ref OrderBookEntry GetOrderRef(ulong orderId)
    {
        ref var slot = ref CollectionsMarshal.GetValueRefOrNullRef(_orders, orderId);
        if (System.Runtime.CompilerServices.Unsafe.IsNullRef(ref slot))
            throw new KeyNotFoundException($"OrderId {orderId} not found in BookSide({_side})");
        return ref slot;
    }

    public BookSide(BookSideType side)
    {
        _side = side;
        _ascending = side == BookSideType.Bid;
    }

    /// <summary>
    /// Add a new order (caller must ensure orderId doesn't exist yet).
    /// Uses single-hash dictionary insert via CollectionsMarshal.
    /// </summary>
    public void Add(in OrderBookEntry entry)
    {
        ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(_orders, entry.OrderId, out _);
        slot = entry;
        AddToPriceLevels(ref slot);
        AssertValid();
    }

    /// <summary>
    /// Update an existing order's price level placement after its fields were mutated.
    /// Only call when the price has changed.
    /// </summary>
    public void MoveOrder(ulong orderId, long oldPrice)
    {
        ref var slot = ref CollectionsMarshal.GetValueRefOrNullRef(_orders, orderId);
        if (System.Runtime.CompilerServices.Unsafe.IsNullRef(ref slot))
            return;
        RemoveFromPriceLevelsByPrice(slot.PriceLevelIndex, oldPrice);
        AddToPriceLevels(ref slot);
        AssertValid();
    }

    /// <returns>true if the order already existed (update); false if new (add).</returns>
    public bool AddOrUpdate(in OrderBookEntry entry)
    {
        ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(_orders, entry.OrderId, out bool existed);

        if (existed)
        {
            long existingPrice = slot.Price;
            int existingIdx = slot.PriceLevelIndex;
            if (existingPrice == entry.Price)
            {
                // Same-price: swap entry in-place, no level restructuring
                slot = entry;
                slot.PriceLevelIndex = existingIdx;
                int levelIdx = FindPriceLevel(entry.Price);
                if (levelIdx >= 0)
                    CollectionsMarshal.AsSpan(_levels[levelIdx].Orders)[existingIdx] = slot;
                AssertValid();
                return true;
            }
            RemoveFromPriceLevelsByPrice(existingIdx, existingPrice);
        }

        slot = entry;
        AddToPriceLevels(ref slot);
        AssertValid();
        return existed;
    }

    public bool Remove(ulong orderId)
    {
        if (!_orders.TryGetValue(orderId, out var entry))
            return false;
        RemoveFromPriceLevelsByPrice(entry.PriceLevelIndex, entry.Price);
        _orders.Remove(orderId);
        AssertValid();
        return true;
    }

    /// <summary>
    /// Re-writes the order's per-level list copy from the authoritative dictionary slot.
    /// Called by callers that mutate fields in-place via <see cref="GetOrderRef"/> when the
    /// price has not changed (so the level placement is unchanged but the list copy is stale).
    /// </summary>
    public void SyncPriceLevelCopy(ulong orderId)
    {
        ref var slot = ref CollectionsMarshal.GetValueRefOrNullRef(_orders, orderId);
        if (System.Runtime.CompilerServices.Unsafe.IsNullRef(ref slot))
            return;
        int levelIdx = FindPriceLevel(slot.Price);
        if (levelIdx < 0)
            return;
        CollectionsMarshal.AsSpan(_levels[levelIdx].Orders)[slot.PriceLevelIndex] = slot;
    }

    public void Clear()
    {
        _orders.Clear();
        foreach (var (_, list) in _levels)
            ReturnList(list);
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

    private void AddToPriceLevels(ref OrderBookEntry entry)
    {
        int idx = FindPriceLevel(entry.Price);
        if (idx >= 0)
        {
            var orders = _levels[idx].Orders;
            entry.PriceLevelIndex = orders.Count;
            orders.Add(entry);
        }
        else
        {
            int insertIdx = ~idx;
            var orders = RentList();
            entry.PriceLevelIndex = 0;
            orders.Add(entry);
            _levels.Insert(insertIdx, (entry.Price, orders));
        }
    }

    private void RemoveFromPriceLevelsByPrice(int orderIdx, long price)
    {
        int levelIdx = FindPriceLevel(price);
        if (levelIdx < 0) return;

        var orders = _levels[levelIdx].Orders;

        // Swap-remove: O(1) — swap with last, remove last
        int lastIdx = orders.Count - 1;
        if (orderIdx < lastIdx)
        {
            var moved = orders[lastIdx];
            moved.PriceLevelIndex = orderIdx;
            orders[orderIdx] = moved;

            // Sync the moved order's PriceLevelIndex inside _orders
            ref var slot = ref CollectionsMarshal.GetValueRefOrNullRef(_orders, moved.OrderId);
            if (!System.Runtime.CompilerServices.Unsafe.IsNullRef(ref slot))
                slot.PriceLevelIndex = orderIdx;
        }
        orders.RemoveAt(lastIdx);

        if (orders.Count == 0)
        {
            ReturnList(orders);
            _levels.RemoveAt(levelIdx);
        }
    }

    /// <summary>
    /// Reverse linear search from end of list (best price).
    /// 82% of real market data operations hit the top 5 price levels (avg distance 3.6),
    /// making this faster than binary search (which always does log2(L) comparisons).
    /// Returns index if found, or ~insertionPoint if not found.
    /// </summary>
    private int FindPriceLevel(long price)
    {
        for (int i = _levels.Count - 1; i >= 0; i--)
        {
            long levelPrice = _levels[i].Price;
            if (levelPrice == price)
                return i;

            int cmp = _ascending
                ? levelPrice.CompareTo(price)
                : price.CompareTo(levelPrice);
            if (cmp < 0)
                return ~(i + 1);
        }

        return ~0;
    }

    private List<OrderBookEntry> RentList()
    {
        if (_listPool.TryPop(out var list))
        {
            list.Clear();
            return list;
        }
        return new List<OrderBookEntry>();
    }

    private void ReturnList(List<OrderBookEntry> list) => _listPool.Push(list);
}
