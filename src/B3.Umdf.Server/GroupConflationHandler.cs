using System.Collections.Concurrent;
using B3.Umdf.Book;

namespace B3.Umdf.Server;

/// <summary>
/// Per-group book event handler. Owns upstream conflation buffers and trade ring buffers.
/// All methods run on the owning group's single thread — no locks needed.
/// </summary>
public sealed class GroupConflationHandler : IBookEventHandler
{
    private readonly SubscriptionManager _parent;
    internal BookManager BookManager { get; private set; } = null!;

    /// <summary>Bind the BookManager for this group. Must be called before feed processing starts.</summary>
    public void SetBookManager(BookManager bm) => BookManager = bm;

    // Per-group conflation buffers (single-threaded, no locks)
    private readonly Dictionary<ulong, BufferedOrder> _orderBuffer = new();
    private readonly List<(ulong SecurityId, byte Side)> _clearBuffer = new();
    private readonly Dictionary<(ulong SecurityId, long Price), (long Qty, long TradeId)> _tradeBuffer = new();
    private byte[] _flushBuf = new byte[4096];
    private long _eventsReceived;
    private long _eventsFlushed;

    // Recent trades per security.
    // ConcurrentDictionary because HandleSubscribe may read from another group's thread
    // via SendSnapshots, while OnTrade writes on the owning thread.
    internal readonly ConcurrentDictionary<ulong, TradeRingBuffer> RecentTrades = new();

    // Subscribe/Get requests routed to this group by the SubscriptionManager
    private readonly ConcurrentQueue<SubscriptionRequest> _pendingSubscribeRequests = new();

    /// <summary>Enqueue a subscribe/get request routed to this group.</summary>
    internal void EnqueueRequest(string clientId, string? symbol, DataFlags flags, bool isGet)
    {
        var req = isGet
            ? SubscriptionRequest.Get(clientId, symbol!, flags)
            : SubscriptionRequest.Subscribe(clientId, symbol!, flags);
        _pendingSubscribeRequests.Enqueue(req);
    }

    internal long UpstreamConflated =>
        Volatile.Read(ref _eventsReceived) - Volatile.Read(ref _eventsFlushed);

    public long EventsReceived => Volatile.Read(ref _eventsReceived);
    public long EventsFlushed => Volatile.Read(ref _eventsFlushed);

    internal GroupConflationHandler(SubscriptionManager parent)
    {
        _parent = parent;
    }

    // ── IBookEventHandler ──

    public void OnOrderAdded(OrderBook book, OrderBookEntry entry)
        => BufferOrder(PendingOrderKind.Added, book.SecurityId, entry.OrderId,
            (byte)entry.Side, entry.Price, entry.Quantity);

    public void OnOrderUpdated(OrderBook book, OrderBookEntry entry)
        => BufferOrder(PendingOrderKind.Updated, book.SecurityId, entry.OrderId,
            (byte)entry.Side, entry.Price, entry.Quantity);

    public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side)
    {
        if (!_parent.IsSubscribed(book.SecurityId)) return;
        BufferOrderDelete(book.SecurityId, orderId, (byte)side);
    }

    public void OnTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        // Always capture trades (regardless of subscription)
        var ring = RecentTrades.GetOrAdd(securityId,
            static _ => new TradeRingBuffer(SubscriptionManager.MaxRecentTrades));
        ring.Add(price, quantity, tradeId);

        if (!_parent.IsSubscribed(securityId)) return;
        _eventsReceived++;
        _parent.UpdateLastTradeFromEvent(securityId, price, quantity);
        BufferTrade(securityId, price, quantity, tradeId);
    }

    public void OnBookCleared(ulong securityId, BookClearSide side)
    {
        if (!_parent.IsSubscribed(securityId)) return;
        PurgeBufferedOrders(securityId, (byte)side);
        _clearBuffer.Add((securityId, (byte)side));
    }

    public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        var ring = RecentTrades.GetOrAdd(securityId,
            static _ => new TradeRingBuffer(SubscriptionManager.MaxRecentTrades));
        ring.Add(price, quantity, tradeId);

        if (!_parent.IsSubscribed(securityId)) return;
        _eventsReceived++;
        _parent.UpdateLastTradeFromEvent(securityId, price, quantity);
        BufferTrade(securityId, price, quantity, tradeId);
    }

    public void OnTradeBust(ulong securityId, long price, long quantity, long tradeId) { }
    public void OnExecutionSummary(ulong securityId, long lastPx, long fillQty) { }

    /// <summary>
    /// Called after each packet is fully processed on the owning group's thread.
    /// Drains pending requests and flushes conflation buffers.
    /// </summary>
    public void OnBatchComplete()
    {
        // 1. Process shared unsubscribe queue (any group, under _subLock)
        _parent.ProcessUnsubscribes();

        // 2. Process this group's routed subscribe/get requests (single-threaded, safe book access)
        ProcessOwnSubscribeRequests();

        // 3. Flush conflation buffers
        if (_orderBuffer.Count == 0 && _clearBuffer.Count == 0 && _tradeBuffer.Count == 0)
            return;
        FlushBuffers();
    }

    // ── Request processing ──

    private void ProcessOwnSubscribeRequests()
    {
        while (_pendingSubscribeRequests.TryDequeue(out var req))
        {
            switch (req.Kind)
            {
                case SubscriptionRequestKind.Subscribe:
                    _parent.HandleSubscribe(req.ClientId, req.Symbol!, req.Flags, BookManager, this);
                    break;
                case SubscriptionRequestKind.Get:
                    _parent.HandleGet(req.ClientId, req.Symbol!, req.Flags, BookManager, this);
                    break;
            }
        }
    }

    // ── Upstream conflation ──

    private enum PendingOrderKind : byte { Added, Updated, Deleted }

    private readonly struct BufferedOrder
    {
        public readonly PendingOrderKind Kind;
        public readonly ulong SecurityId;
        public readonly byte Side;
        public readonly long Price;
        public readonly long Quantity;

        public BufferedOrder(PendingOrderKind kind, ulong secId, byte side, long price, long qty)
        { Kind = kind; SecurityId = secId; Side = side; Price = price; Quantity = qty; }
    }

    private void BufferOrder(PendingOrderKind kind, ulong securityId, ulong orderId, byte side, long price, long qty)
    {
        if (!_parent.IsSubscribed(securityId)) return;
        _eventsReceived++;

        if (_orderBuffer.TryGetValue(orderId, out var existing))
        {
            if (kind == PendingOrderKind.Deleted)
            {
                if (existing.Kind == PendingOrderKind.Added)
                    _orderBuffer.Remove(orderId);
                else
                    _orderBuffer[orderId] = new BufferedOrder(PendingOrderKind.Deleted, securityId, side, 0, 0);
            }
            else
            {
                var mergedKind = existing.Kind == PendingOrderKind.Added ? PendingOrderKind.Added : kind;
                _orderBuffer[orderId] = new BufferedOrder(mergedKind, securityId, side, price, qty);
            }
        }
        else
        {
            _orderBuffer[orderId] = new BufferedOrder(kind, securityId, side, price, qty);
        }
    }

    private void BufferOrderDelete(ulong securityId, ulong orderId, byte side)
    {
        _eventsReceived++;
        if (_orderBuffer.TryGetValue(orderId, out var existing))
        {
            if (existing.Kind == PendingOrderKind.Added)
                _orderBuffer.Remove(orderId);
            else
                _orderBuffer[orderId] = new BufferedOrder(PendingOrderKind.Deleted, securityId, side, 0, 0);
        }
        else
        {
            _orderBuffer[orderId] = new BufferedOrder(PendingOrderKind.Deleted, securityId, side, 0, 0);
        }
    }

    private void BufferTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        var key = (securityId, price);
        if (_tradeBuffer.TryGetValue(key, out var existing))
            _tradeBuffer[key] = (existing.Qty + quantity, tradeId);
        else
            _tradeBuffer[key] = (quantity, tradeId);
    }

    private void PurgeBufferedOrders(ulong securityId, byte clearSide)
    {
        List<ulong>? toRemove = null;
        foreach (var (orderId, order) in _orderBuffer)
        {
            if (order.SecurityId != securityId) continue;
            if (clearSide != 0 && order.Side != (clearSide - 1)) continue;
            toRemove ??= new();
            toRemove.Add(orderId);
        }
        if (toRemove is not null)
            foreach (var id in toRemove)
                _orderBuffer.Remove(id);
    }

    // ── Buffer flushing ──

    private void FlushBuffers()
    {
        int flushed = 0;

        foreach (var (secId, side) in _clearBuffer)
        {
            if (!_parent.IsSubscribed(secId)) continue;
            var buf = new byte[13];
            WireProtocol.WriteBookCleared(buf, secId, side);
            _parent.BroadcastToSubscribers(secId, buf);
            flushed++;
        }
        _clearBuffer.Clear();

        if (_orderBuffer.Count > 0)
            flushed += FlushOrderBuffer();

        if (_tradeBuffer.Count > 0)
            flushed += FlushTradeBuffer();

        _eventsFlushed += flushed;
    }

    private int FlushOrderBuffer()
    {
        int flushed = _orderBuffer.Count <= 4
            ? FlushOrdersIndividually()
            : FlushOrdersBatched();

        _orderBuffer.Clear();
        return flushed;
    }

    /// <summary>Small batch: serialize each order individually (avoids sort overhead).</summary>
    private int FlushOrdersIndividually()
    {
        int flushed = 0;
        foreach (var (orderId, order) in _orderBuffer)
        {
            if (!_parent.IsSubscribed(order.SecurityId)) { flushed++; continue; }

            byte[] buf;
            int len;
            if (order.Kind == PendingOrderKind.Deleted)
            {
                buf = new byte[21];
                len = WireProtocol.WriteOrderDeleted(buf, order.SecurityId, orderId, order.Side);
            }
            else
            {
                var msgType = order.Kind == PendingOrderKind.Added ? MessageType.OrderAdded : MessageType.OrderUpdated;
                buf = new byte[37];
                len = WireProtocol.WriteOrderEvent(buf, msgType, order.SecurityId, orderId, order.Side, order.Price, order.Quantity);
            }
            _parent.BroadcastToSubscribers(order.SecurityId, new ReadOnlyMemory<byte>(buf, 0, len));
            flushed++;
        }
        return flushed;
    }

    /// <summary>Large batch: sort by security ID, coalesce writes per security into shared buffer.</summary>
    private int FlushOrdersBatched()
    {
        int flushed = 0;
        const int maxPerEvent = 37;
        int totalBufSize = _orderBuffer.Count * maxPerEvent;
        if (_flushBuf.Length < totalBufSize)
            _flushBuf = new byte[Math.Max(totalBufSize, _flushBuf.Length * 2)];

        Span<(ulong OrderId, BufferedOrder Order)> sorted = _orderBuffer.Count <= 256
            ? stackalloc (ulong, BufferedOrder)[_orderBuffer.Count]
            : new (ulong, BufferedOrder)[_orderBuffer.Count];
        int idx = 0;
        foreach (var kv in _orderBuffer)
            sorted[idx++] = (kv.Key, kv.Value);
        sorted.Sort((a, b) => a.Order.SecurityId.CompareTo(b.Order.SecurityId));

        ulong currentSecId = 0;
        int segmentStart = 0;
        int offset = 0;

        for (int i = 0; i < sorted.Length; i++)
        {
            var (orderId, order) = sorted[i];

            // Flush previous security's segment when switching to a new security
            if (order.SecurityId != currentSecId && offset > segmentStart)
            {
                BroadcastSegment(currentSecId, segmentStart, offset);
                segmentStart = offset;
            }
            currentSecId = order.SecurityId;

            if (order.Kind == PendingOrderKind.Deleted)
                offset += WireProtocol.WriteOrderDeleted(_flushBuf.AsSpan(offset), order.SecurityId, orderId, order.Side);
            else
            {
                var msgType = order.Kind == PendingOrderKind.Added ? MessageType.OrderAdded : MessageType.OrderUpdated;
                offset += WireProtocol.WriteOrderEvent(_flushBuf.AsSpan(offset), msgType, order.SecurityId, orderId, order.Side, order.Price, order.Quantity);
            }
            flushed++;
        }

        // Flush final segment
        if (offset > segmentStart)
            BroadcastSegment(currentSecId, segmentStart, offset);

        return flushed;
    }

    private void BroadcastSegment(ulong securityId, int start, int end)
    {
        if (!_parent.IsSubscribed(securityId)) return;
        var copy = new byte[end - start];
        _flushBuf.AsSpan(start, end - start).CopyTo(copy);
        _parent.BroadcastToSubscribers(securityId, copy);
    }

    private int FlushTradeBuffer()
    {
        int flushed = 0;
        foreach (var ((secId, price), (qty, tradeId)) in _tradeBuffer)
        {
            if (!_parent.IsSubscribed(secId)) { flushed++; continue; }
            var buf = new byte[36];
            int len = WireProtocol.WriteTrade(buf, secId, price, qty, tradeId);
            _parent.BroadcastToSubscribers(secId, new ReadOnlyMemory<byte>(buf, 0, len));
            flushed++;
        }
        _tradeBuffer.Clear();
        return flushed;
    }
}
