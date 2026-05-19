namespace B3.MarketData.WebSocketClient;

/// <summary>
/// Phase 1 (issue #43). Per-symbol L3 / MBO state + derived top-of-book.
/// Mutated only by <see cref="BookFeed"/> (single writer = receive loop);
/// read methods take the per-symbol lock to publish a consistent aggregate.
/// </summary>
internal sealed class BookView : IBookView
{
    private readonly object _gate = new();
    private readonly Dictionary<ulong, OrderEntry> _bids = new();
    private readonly Dictionary<ulong, OrderEntry> _asks = new();
    private bool _isStale;
    private DateTime _updatedUtc;

    public BookView(string symbol, ulong securityId)
    {
        Symbol = symbol;
        SecurityId = securityId;
    }

    public string Symbol { get; }
    public ulong SecurityId { get; }
    public bool IsStale { get { lock (_gate) return _isStale; } }
    public DateTime UpdatedUtc { get { lock (_gate) return _updatedUtc; } }

    public bool TryGetTop(out L2TopOfBook top)
    {
        lock (_gate)
        {
            // Bid side = highest price wins; Ask side = lowest.
            var bid = TopOfSide(_bids, ascending: false);
            var ask = TopOfSide(_asks, ascending: true);
            if (bid.OrderCount == 0 && ask.OrderCount == 0)
            {
                top = default;
                return false;
            }
            top = new L2TopOfBook(Symbol, bid, ask, _updatedUtc);
            return true;
        }
    }

    internal void ApplySnapshot(BookSnapshotEvent ev)
    {
        lock (_gate)
        {
            _bids.Clear();
            _asks.Clear();
            // BookSnapshotEvent today carries only the phase-marker header;
            // OrderAdded frames follow in the same packet and are applied
            // separately. But we also support the wire form's optional
            // aggregated entries (orderId=0) by skipping them — the live
            // state will rebuild from the order stream.
            foreach (var o in ev.Bids)
            {
                if (o.OrderId == 0) continue;
                _bids[o.OrderId] = new OrderEntry(o.Price, o.Qty);
            }
            foreach (var o in ev.Asks)
            {
                if (o.OrderId == 0) continue;
                _asks[o.OrderId] = new OrderEntry(o.Price, o.Qty);
            }
            _isStale = false;
            _updatedUtc = ev.ReceivedUtc;
        }
    }

    internal void ApplyAdded(OrderAddedEvent ev)
    {
        lock (_gate)
        {
            SideMap(ev.Side)[ev.OrderId] = new OrderEntry(ev.Price, ev.Qty);
            _updatedUtc = ev.ReceivedUtc;
        }
    }

    internal void ApplyUpdated(OrderUpdatedEvent ev)
    {
        lock (_gate)
        {
            var map = SideMap(ev.Side);
            if (ev.Qty <= 0)
            {
                map.Remove(ev.OrderId);
            }
            else
            {
                map[ev.OrderId] = new OrderEntry(ev.Price, ev.Qty);
            }
            _updatedUtc = ev.ReceivedUtc;
        }
    }

    internal void ApplyDeleted(OrderDeletedEvent ev)
    {
        lock (_gate)
        {
            SideMap(ev.Side).Remove(ev.OrderId);
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
                    _bids.Clear();
                    _asks.Clear();
                    break;
                case BookClearSide.Bid:
                    _bids.Clear();
                    break;
                case BookClearSide.Ask:
                    _asks.Clear();
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
        lock (_gate) return (_bids.Count, _asks.Count);
    }

    private static L2Side TopOfSide(Dictionary<ulong, OrderEntry> side, bool ascending)
    {
        if (side.Count == 0) return new L2Side(0m, 0, 0);
        decimal best = ascending ? decimal.MaxValue : decimal.MinValue;
        foreach (var e in side.Values)
        {
            if (ascending ? e.Price < best : e.Price > best)
                best = e.Price;
        }
        long qty = 0;
        int count = 0;
        foreach (var e in side.Values)
        {
            if (e.Price == best)
            {
                qty += e.Qty;
                count++;
            }
        }
        return new L2Side(best, qty, count);
    }

    private Dictionary<ulong, OrderEntry> SideMap(BookSide side) =>
        side == BookSide.Bid ? _bids : _asks;

    private readonly record struct OrderEntry(decimal Price, long Qty);
}
