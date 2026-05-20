namespace B3.MarketData.WebSocketClient;

/// <summary>
/// Phase 1/2 (issue #43). Per-symbol L3 / MBO state + derived top-of-book
/// and depth-N L2 ladder. Mutated only by <see cref="BookFeed"/> (single
/// writer = receive loop); read methods take the per-symbol lock to publish
/// a consistent aggregate.
/// <para>
/// State is kept as two parallel structures per side: a flat
/// <see cref="Dictionary{TKey,TValue}"/> keyed by <c>OrderId</c> for O(1)
/// update/delete-by-id, and a <see cref="SortedDictionary{TKey,TValue}"/>
/// keyed by price (descending for bids, ascending for asks) that aggregates
/// total qty and order count per price level for O(1) top reads and
/// O(depth) ladder copies.
/// </para>
/// </summary>
internal sealed class BookView : IBookView
{
    private readonly object _gate = new();
    private readonly Dictionary<ulong, OrderEntry> _bidOrders = new();
    private readonly Dictionary<ulong, OrderEntry> _askOrders = new();
    private readonly SortedDictionary<decimal, PriceLevel> _bidLevels = new(DescendingDecimalComparer.Instance);
    private readonly SortedDictionary<decimal, PriceLevel> _askLevels = new();
    private bool _isStale;
    private DateTime _updatedUtc;
    private long _sequence;

    public BookView(string symbol, ulong securityId)
    {
        Symbol = symbol;
        SecurityId = securityId;
    }

    public string Symbol { get; }
    public ulong SecurityId { get; }
    public bool IsStale { get { lock (_gate) return _isStale; } }
    public DateTime UpdatedUtc { get { lock (_gate) return _updatedUtc; } }
    public long Sequence { get { lock (_gate) return _sequence; } }

    public bool TryGetTop(out L2TopOfBook top)
    {
        lock (_gate)
        {
            var bid = FirstLevel(_bidLevels);
            var ask = FirstLevel(_askLevels);
            if (bid.OrderCount == 0 && ask.OrderCount == 0)
            {
                top = default;
                return false;
            }
            top = new L2TopOfBook(Symbol, bid, ask, _updatedUtc);
            return true;
        }
    }

    public int CopyBidLevels(Span<L2Level> destination, int depth)
    {
        ValidateDepth(destination, depth);
        lock (_gate) return CopyLevels(_bidLevels, destination, depth);
    }

    public int CopyAskLevels(Span<L2Level> destination, int depth)
    {
        ValidateDepth(destination, depth);
        lock (_gate) return CopyLevels(_askLevels, destination, depth);
    }

    internal void ApplySnapshot(BookSnapshotEvent ev)
    {
        lock (_gate)
        {
            _bidOrders.Clear();
            _askOrders.Clear();
            _bidLevels.Clear();
            _askLevels.Clear();
            // BookSnapshotEvent today carries only the phase-marker header;
            // OrderAdded frames follow in the same packet and are applied
            // separately. We also support the wire form's optional aggregated
            // entries (orderId=0) by skipping them — the live state will
            // rebuild from the order stream.
            foreach (var o in ev.Bids)
            {
                if (o.OrderId == 0) continue;
                _bidOrders[o.OrderId] = new OrderEntry(o.Price, o.Qty);
                AddToLevel(_bidLevels, o.Price, o.Qty);
            }
            foreach (var o in ev.Asks)
            {
                if (o.OrderId == 0) continue;
                _askOrders[o.OrderId] = new OrderEntry(o.Price, o.Qty);
                AddToLevel(_askLevels, o.Price, o.Qty);
            }
            _isStale = false;
            _updatedUtc = ev.ReceivedUtc;
            _sequence = ev.RptSeq;
        }
    }

    internal void ApplyAdded(OrderAddedEvent ev)
    {
        lock (_gate)
        {
            var orders = SideOrders(ev.Side);
            var levels = SideLevels(ev.Side);
            // Defensive: an Add for a known id is treated as a replace —
            // remove the old contribution from its level before adding the new.
            if (orders.TryGetValue(ev.OrderId, out var prior))
            {
                RemoveFromLevel(levels, prior.Price, prior.Qty);
            }
            orders[ev.OrderId] = new OrderEntry(ev.Price, ev.Qty);
            AddToLevel(levels, ev.Price, ev.Qty);
            _updatedUtc = ev.ReceivedUtc;
        }
    }

    internal void ApplyUpdated(OrderUpdatedEvent ev)
    {
        lock (_gate)
        {
            var orders = SideOrders(ev.Side);
            var levels = SideLevels(ev.Side);
            bool hadPrior = orders.TryGetValue(ev.OrderId, out var prior);
            if (ev.Qty <= 0)
            {
                if (orders.Remove(ev.OrderId, out var removed))
                {
                    RemoveFromLevel(levels, removed.Price, removed.Qty);
                }
            }
            else
            {
                if (hadPrior)
                {
                    if (prior.Price == ev.Price)
                    {
                        long delta = ev.Qty - prior.Qty;
                        if (delta != 0)
                        {
                            // adjust level qty without changing order count
                            AdjustLevelQty(levels, ev.Price, delta);
                        }
                    }
                    else
                    {
                        RemoveFromLevel(levels, prior.Price, prior.Qty);
                        AddToLevel(levels, ev.Price, ev.Qty);
                    }
                }
                else
                {
                    AddToLevel(levels, ev.Price, ev.Qty);
                }
                orders[ev.OrderId] = new OrderEntry(ev.Price, ev.Qty);
            }
            _updatedUtc = ev.ReceivedUtc;
        }
    }

    internal void ApplyDeleted(OrderDeletedEvent ev)
    {
        lock (_gate)
        {
            var orders = SideOrders(ev.Side);
            var levels = SideLevels(ev.Side);
            if (orders.Remove(ev.OrderId, out var removed))
            {
                RemoveFromLevel(levels, removed.Price, removed.Qty);
            }
            _updatedUtc = ev.ReceivedUtc;
        }
    }

    internal void ApplyCleared(BookClearedEvent ev)
    {
        lock (_gate)
        {
            switch (ev.ClearSide)
            {
                case BookClearSide.Both:
                    _bidOrders.Clear();
                    _askOrders.Clear();
                    _bidLevels.Clear();
                    _askLevels.Clear();
                    break;
                case BookClearSide.Bid:
                    _bidOrders.Clear();
                    _bidLevels.Clear();
                    break;
                case BookClearSide.Ask:
                    _askOrders.Clear();
                    _askLevels.Clear();
                    break;
            }
            _updatedUtc = ev.ReceivedUtc;
        }
    }

    internal void MarkStale(bool stale, DateTime updatedUtc)
    {
        lock (_gate)
        {
            _isStale = stale;
            _updatedUtc = updatedUtc;
        }
    }

    /// <summary>Test/diagnostic hook: per-side live order count.</summary>
    internal (int Bids, int Asks) GetOrderCounts()
    {
        lock (_gate) return (_bidOrders.Count, _askOrders.Count);
    }

    /// <summary>Test/diagnostic hook: per-side live price-level count.</summary>
    internal (int Bids, int Asks) GetLevelCounts()
    {
        lock (_gate) return (_bidLevels.Count, _askLevels.Count);
    }

    private Dictionary<ulong, OrderEntry> SideOrders(BookSide side) =>
        side == BookSide.Bid ? _bidOrders : _askOrders;

    private SortedDictionary<decimal, PriceLevel> SideLevels(BookSide side) =>
        side == BookSide.Bid ? _bidLevels : _askLevels;

    private static L2Side FirstLevel(SortedDictionary<decimal, PriceLevel> levels)
    {
        if (levels.Count == 0) return new L2Side(0m, 0, 0);
        // SortedDictionary enumerator is in key order — first element is the
        // best price under the configured comparer (descending for bids,
        // ascending for asks).
        using var e = levels.GetEnumerator();
        e.MoveNext();
        var kv = e.Current;
        return new L2Side(kv.Key, kv.Value.TotalQty, kv.Value.OrderCount);
    }

    private static int CopyLevels(SortedDictionary<decimal, PriceLevel> levels, Span<L2Level> dst, int depth)
    {
        int n = Math.Min(depth, levels.Count);
        if (n == 0) return 0;
        int i = 0;
        foreach (var kv in levels)
        {
            if (i >= n) break;
            dst[i++] = new L2Level(kv.Key, kv.Value.TotalQty, kv.Value.OrderCount);
        }
        return i;
    }

    private static void AddToLevel(SortedDictionary<decimal, PriceLevel> levels, decimal price, long qty)
    {
        if (levels.TryGetValue(price, out var lvl))
        {
            levels[price] = new PriceLevel(lvl.TotalQty + qty, lvl.OrderCount + 1);
        }
        else
        {
            levels.Add(price, new PriceLevel(qty, 1));
        }
    }

    private static void RemoveFromLevel(SortedDictionary<decimal, PriceLevel> levels, decimal price, long qty)
    {
        if (!levels.TryGetValue(price, out var lvl)) return;
        int newCount = lvl.OrderCount - 1;
        long newQty = lvl.TotalQty - qty;
        if (newCount <= 0 || newQty <= 0)
        {
            levels.Remove(price);
        }
        else
        {
            levels[price] = new PriceLevel(newQty, newCount);
        }
    }

    private static void AdjustLevelQty(SortedDictionary<decimal, PriceLevel> levels, decimal price, long delta)
    {
        if (!levels.TryGetValue(price, out var lvl)) return;
        long newQty = lvl.TotalQty + delta;
        if (newQty <= 0)
        {
            // Shouldn't happen in well-formed streams, but stay defensive.
            levels.Remove(price);
        }
        else
        {
            levels[price] = new PriceLevel(newQty, lvl.OrderCount);
        }
    }

    private static void ValidateDepth(Span<L2Level> destination, int depth)
    {
        if (depth < 0) throw new ArgumentOutOfRangeException(nameof(depth), "depth must be >= 0");
        if (depth > destination.Length)
            throw new ArgumentOutOfRangeException(nameof(depth), "depth exceeds destination length");
    }

    private readonly record struct OrderEntry(decimal Price, long Qty);

    private readonly record struct PriceLevel(long TotalQty, int OrderCount);

    private sealed class DescendingDecimalComparer : IComparer<decimal>
    {
        public static readonly DescendingDecimalComparer Instance = new();
        public int Compare(decimal x, decimal y) => y.CompareTo(x);
    }
}
