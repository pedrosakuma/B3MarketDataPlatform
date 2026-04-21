using System.Buffers;
using System.Collections.Concurrent;
using B3.Umdf.Book;
using B3.Umdf.Mbo.Sbe.V16;

namespace B3.Umdf.Server;

/// <summary>
/// Per-group book event handler. Owns upstream conflation buffers and trade ring buffers.
/// All methods run on the owning group's single thread — no locks needed.
/// </summary>
public sealed class GroupConflationHandler : IBookEventHandler, IMarketDataEventHandler
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

    // Per-flush per-client coalesced output buffer. Reused across flushes; the byte[]
    // payload is freshly allocated per flush so it can be handed off to the client's
    // outbound channel without copy. Coalescing N (events × subscribers) Channel.TryWrite
    // calls into 1 per client amortizes the dominant cost in the WS broadcast path.
    private readonly Dictionary<ClientSession, ClientBatchAccumulator> _clientBatches = new();

    private struct ClientBatchAccumulator
    {
        public byte[] Buffer;
        public int Offset;
        public int LogicalCount;
    }

    private long _eventsReceived;
    private long _eventsFlushed;

    // Recent trades per security.
    // ConcurrentDictionary because HandleSubscribe may read from another group's thread
    // via SendSnapshots, while OnTrade writes on the owning thread.
    internal readonly ConcurrentDictionary<ulong, TradeRingBuffer> RecentTrades = new();

    // Per-security candle aggregators (read via GetCandles from subscribe thread)
    internal readonly ConcurrentDictionary<ulong, CandleAggregator> Candles = new();

    // Per-security trading phase (updated via IMarketDataEventHandler, read on same feed thread)
    private readonly Dictionary<ulong, int> _tradingStatus = new();

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

    public void OnOrderAdded(OrderBook book, in OrderBookEntry entry)
        => BufferOrder(PendingOrderKind.Added, book.SecurityId, entry.OrderId,
            (byte)entry.Side, entry.Price, entry.Quantity);

    public void OnOrderUpdated(OrderBook book, in OrderBookEntry entry)
        => BufferOrder(PendingOrderKind.Updated, book.SecurityId, entry.OrderId,
            (byte)entry.Side, entry.Price, entry.Quantity);

    public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side)
    {
        if (!_parent.IsSubscribed(book.SecurityId)) return;
        BufferOrderDelete(book.SecurityId, orderId, (byte)side);
    }

    public void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs)
    {
        // Always capture trades (regardless of subscription)
        var ring = RecentTrades.GetOrAdd(securityId,
            static _ => new TradeRingBuffer(SubscriptionManager.MaxRecentTrades));
        ring.Add(price, quantity, tradeId);

        // Only aggregate trades into candles during Open trading phase
        if (IsOpenPhase(securityId))
        {
            var candle = Candles.GetOrAdd(securityId, static _ => new CandleAggregator());
            long timestampSeconds = sendingTimeNs > 0
                ? sendingTimeNs / 1_000_000_000
                : DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            candle.Add(price, quantity, timestampSeconds);
        }

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

    public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs)
    {
        var ring = RecentTrades.GetOrAdd(securityId,
            static _ => new TradeRingBuffer(SubscriptionManager.MaxRecentTrades));
        ring.Add(price, quantity, tradeId);

        if (IsOpenPhase(securityId))
        {
            var candle = Candles.GetOrAdd(securityId, static _ => new CandleAggregator());
            long timestampSeconds = sendingTimeNs > 0
                ? sendingTimeNs / 1_000_000_000
                : DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            candle.Add(price, quantity, timestampSeconds);
        }

        if (!_parent.IsSubscribed(securityId)) return;
        _eventsReceived++;
        _parent.UpdateLastTradeFromEvent(securityId, price, quantity);
        BufferTrade(securityId, price, quantity, tradeId);
    }

    public void OnTradeBust(ulong securityId, long price, long quantity, long tradeId) { }
    public void OnExecutionSummary(ulong securityId, long lastPx, long fillQty) { }

    /// <summary>
    /// Returns true if the instrument is in the Open trading phase (or if the phase is unknown).
    /// Trades from auction/pre-open/closing call should not be plotted on the chart.
    /// </summary>
    private bool IsOpenPhase(ulong securityId)
    {
        if (!_tradingStatus.TryGetValue(securityId, out int status)) return true;
        return status == (int)TradingSessionSubID.OPEN;
    }

    // ── IMarketDataEventHandler ──

    public void OnSecurityStatusChanged(ulong securityId, InstrumentInfo info)
    {
        if (info.TradingStatus is { } status)
            _tradingStatus[securityId] = status;

        _parent.NotifyInfoUpdated(securityId);
    }

    public void OnMarketDataUpdated(ulong securityId, InstrumentInfo info)
    {
        if (info.TradingStatus is { } status)
            _tradingStatus[securityId] = status;

        _parent.NotifyInfoUpdated(securityId);
    }

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

        Span<byte> tmp = stackalloc byte[64];

        foreach (var (secId, side) in _clearBuffer)
        {
            if (!_parent.IsSubscribed(secId)) continue;
            int len = WireProtocol.WriteBookCleared(tmp, secId, side);
            AppendForBookSubscribers(secId, tmp[..len], logicalCount: 1);
            flushed++;
        }
        _clearBuffer.Clear();

        if (_orderBuffer.Count > 0)
            flushed += FlushOrderBuffer();

        if (_tradeBuffer.Count > 0)
            flushed += FlushTradeBuffer();

        // Coalesced flush: one Channel.TryWrite per client per cycle (instead of one
        // per event × subscriber). This is the single biggest WS-side optimization;
        // see CHECKPOINT 014 for profile evidence.
        FlushClientBatches();

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
        Span<byte> tmp = stackalloc byte[64];
        foreach (var (orderId, order) in _orderBuffer)
        {
            if (!_parent.IsSubscribed(order.SecurityId)) { flushed++; continue; }

            int len;
            if (order.Kind == PendingOrderKind.Deleted)
            {
                len = WireProtocol.WriteOrderDeleted(tmp, order.SecurityId, orderId, order.Side);
            }
            else
            {
                var msgType = order.Kind == PendingOrderKind.Added ? MessageType.OrderAdded : MessageType.OrderUpdated;
                len = WireProtocol.WriteOrderEvent(tmp, msgType, order.SecurityId, orderId, order.Side, order.Price, order.Quantity);
            }
            AppendForBookSubscribers(order.SecurityId, tmp[..len], logicalCount: 1);
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
        int segmentCount = 0;
        int offset = 0;

        for (int i = 0; i < sorted.Length; i++)
        {
            var (orderId, order) = sorted[i];

            // Flush previous security's segment when switching to a new security
            if (order.SecurityId != currentSecId && offset > segmentStart)
            {
                AppendForBookSubscribers(currentSecId, _flushBuf.AsSpan(segmentStart, offset - segmentStart), segmentCount);
                segmentStart = offset;
                segmentCount = 0;
            }
            currentSecId = order.SecurityId;

            if (order.Kind == PendingOrderKind.Deleted)
                offset += WireProtocol.WriteOrderDeleted(_flushBuf.AsSpan(offset), order.SecurityId, orderId, order.Side);
            else
            {
                var msgType = order.Kind == PendingOrderKind.Added ? MessageType.OrderAdded : MessageType.OrderUpdated;
                offset += WireProtocol.WriteOrderEvent(_flushBuf.AsSpan(offset), msgType, order.SecurityId, orderId, order.Side, order.Price, order.Quantity);
            }
            segmentCount++;
            flushed++;
        }

        // Flush final segment
        if (offset > segmentStart)
            AppendForBookSubscribers(currentSecId, _flushBuf.AsSpan(segmentStart, offset - segmentStart), segmentCount);

        return flushed;
    }

    private int FlushTradeBuffer()
    {
        int flushed = 0;
        Span<byte> tmp = stackalloc byte[64];

        // Collect securities that need candle broadcasts (deduplicated)
        HashSet<ulong>? candleSecIds = null;
        foreach (var ((secId, _), _) in _tradeBuffer)
        {
            if (!_parent.IsSubscribed(secId)) continue;
            candleSecIds ??= new();
            candleSecIds.Add(secId);
        }

        // Flush trades
        foreach (var ((secId, price), (qty, tradeId)) in _tradeBuffer)
        {
            if (!_parent.IsSubscribed(secId)) { flushed++; continue; }
            int len = WireProtocol.WriteTrade(tmp, secId, price, qty, tradeId);
            AppendForBookSubscribers(secId, tmp[..len], logicalCount: 1);
            flushed++;
        }
        _tradeBuffer.Clear();

        // Broadcast candle updates (one per security, after all trades)
        if (candleSecIds is not null)
        {
            Span<byte> cbuf = stackalloc byte[64]; // CandleUpdate fits in 62 bytes
            foreach (var secId in candleSecIds)
            {
                if (!Candles.TryGetValue(secId, out var agg)) continue;
                var latest = agg.GetLatest();
                if (latest is { } c)
                {
                    int clen = WireProtocol.WriteCandleUpdate(cbuf, secId, agg.Resolution, c);
                    AppendForBookSubscribers(secId, cbuf[..clen], logicalCount: 1);
                }
            }
        }

        return flushed;
    }

    /// <summary>
    /// Append <paramref name="bytes"/> to every Book-flag subscriber's per-client coalesced
    /// buffer. <paramref name="logicalCount"/> is the number of pre-serialized wire messages
    /// packed back-to-back in <paramref name="bytes"/> (1 for single events, N for batched
    /// segments). Buffers are flushed in <see cref="FlushClientBatches"/> at the end of the
    /// per-packet flush cycle, so each client receives at most one Channel.TryWrite per
    /// cycle even when many securities/events fan out to it.
    /// </summary>
    private void AppendForBookSubscribers(ulong securityId, ReadOnlySpan<byte> bytes, int logicalCount)
    {
        var subs = _parent.GetSubscribers(securityId);
        if (subs is null || bytes.Length == 0) return;

        foreach (var (clientId, flags) in subs)
        {
            if (!flags.HasFlag(DataFlags.Book)) continue;
            var session = _parent.GetClient(clientId);
            if (session is null) continue;

            if (!_clientBatches.TryGetValue(session, out var acc))
            {
                acc = new ClientBatchAccumulator
                {
                    Buffer = ArrayPool<byte>.Shared.Rent(Math.Max(bytes.Length * 4, 1024)),
                    Offset = 0,
                    LogicalCount = 0,
                };
            }
            if (acc.Offset + bytes.Length > acc.Buffer.Length)
            {
                int newSize = Math.Max(acc.Buffer.Length * 2, acc.Offset + bytes.Length);
                var newBuf = ArrayPool<byte>.Shared.Rent(newSize);
                acc.Buffer.AsSpan(0, acc.Offset).CopyTo(newBuf);
                ArrayPool<byte>.Shared.Return(acc.Buffer);
                acc.Buffer = newBuf;
            }
            bytes.CopyTo(acc.Buffer.AsSpan(acc.Offset));
            acc.Offset += bytes.Length;
            acc.LogicalCount += logicalCount;
            _clientBatches[session] = acc;
        }
    }

    /// <summary>
    /// Hand each accumulated per-client buffer to its outbound channel as a single
    /// coalesced batch and transfer ownership of the pooled byte[] to the session's
    /// write loop, which returns it to <see cref="ArrayPool{T}.Shared"/> after the WS
    /// frame is sent. <see cref="ClientSession.TryEnqueueBatch"/> is responsible for
    /// returning the array if the channel is full or already cancelled.
    /// </summary>
    private void FlushClientBatches()
    {
        if (_clientBatches.Count == 0) return;
        foreach (var kv in _clientBatches)
        {
            var acc = kv.Value;
            if (acc.Offset == 0)
            {
                ArrayPool<byte>.Shared.Return(acc.Buffer);
                continue;
            }
            kv.Key.TryEnqueueBatch(
                new ReadOnlyMemory<byte>(acc.Buffer, 0, acc.Offset),
                acc.LogicalCount,
                pooledArray: acc.Buffer);
        }
        _clientBatches.Clear();
    }
}
