using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
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

    // Per-packet work batch. Dispatch thread rents on first event in a packet, appends
    // all per-event wire-bytes into it, and publishes to _broadcastRing on
    // OnBatchComplete. The broadcaster thread drains _broadcastRing and performs the
    // subscriber fan-out there, keeping the dispatch thread independent of client count.
    private BroadcastWorkBatch? _currentBatch;
    private readonly BroadcastRing _broadcastRing;
    private Thread? _broadcasterThread;
    private CancellationTokenSource? _broadcasterCts;

    // Broadcaster-thread local: per-client coalesced output buffer. Accumulates across
    // events within one batch (one packet) so each client receives at most one
    // Channel.TryWrite per packet even when many securities fan out to it. Reused
    // across batches (Dictionary instance) but the byte[] payloads are rented per flush
    // and handed off to the session's write loop.
    private readonly Dictionary<ClientSession, ClientBatchAccumulator> _clientBatches = new();

    private struct ClientBatchAccumulator
    {
        public byte[] Buffer;
        public int Offset;
        public int LogicalCount;
    }

    // Metrics (all written/read via Volatile from different threads)
    private long _broadcastBatchesPublished;
    private long _broadcastBatchesDroppedFull;
    private long _broadcastResyncRequests;

    public long BroadcastBatchesPublished => Volatile.Read(ref _broadcastBatchesPublished);
    public long BroadcastBatchesDroppedFull => Volatile.Read(ref _broadcastBatchesDroppedFull);
    public long BroadcastResyncRequests => Volatile.Read(ref _broadcastResyncRequests);
    public int BroadcastRingDepth => _broadcastRing.ApproximateDepth;
    public int BroadcastRingCapacity => _broadcastRing.Capacity;

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
    // Latest authoritative session VWAP from B3 (InfoSnapshot.VwapPrice). Stamped on every
    // candle so the chart line matches what B3 publishes — no parallel computation drift.
    private readonly Dictionary<ulong, long> _vwapBySecurity = new();

    // Subscribe/Get requests routed to this group by the SubscriptionManager
    private readonly ConcurrentQueue<SubscriptionRequest> _pendingSubscribeRequests = new();
    // Cap of snapshot requests serviced per OnBatchComplete invocation. See AppSettings.
    private readonly int _maxSnapshotRequestsPerBatch;
    // When true, OnBatchComplete drops accumulated wire events instead of flushing+publishing.
    // Set during FeedHandler Recovery/CatchUp; cleared on RealTime entry, which also
    // schedules a fresh book snapshot (Get) for every current Book-flag subscriber so
    // they recover any state that was suppressed.
    private volatile bool _suppressFanout;

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

    internal GroupConflationHandler(SubscriptionManager parent, int broadcastRingCapacity = 256, int maxSnapshotRequestsPerBatch = 32)
    {
        _parent = parent;
        _broadcastRing = new BroadcastRing(broadcastRingCapacity);
        _maxSnapshotRequestsPerBatch = maxSnapshotRequestsPerBatch;
    }

    /// <summary>
    /// Start the per-group broadcaster thread. Must be called once after construction,
    /// before feed processing begins. Idempotent.
    /// </summary>
    public void StartBroadcaster(int groupId)
    {
        if (_broadcasterThread is not null) return;
        _broadcasterCts = new CancellationTokenSource();
        var t = new Thread(() => RunBroadcaster(_broadcasterCts.Token))
        {
            IsBackground = true,
            Name = $"UmdfBroadcaster-G{groupId}",
        };
        _broadcasterThread = t;
        t.Start();
    }

    /// <summary>Signals the broadcaster thread to stop and waits for it to exit.</summary>
    public void StopBroadcaster()
    {
        if (_broadcasterCts is null) return;
        _broadcasterCts.Cancel();
        _broadcastRing.SignalShutdown();
        _broadcasterThread?.Join(TimeSpan.FromSeconds(2));
        _broadcastRing.Dispose();
        _broadcasterCts.Dispose();
        _broadcasterCts = null;
        _broadcasterThread = null;
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
            long sessionVwap = _vwapBySecurity.TryGetValue(securityId, out var v) ? v : price;
            candle.Add(price, quantity, timestampSeconds, sessionVwap);
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
            long sessionVwap = _vwapBySecurity.TryGetValue(securityId, out var v) ? v : price;
            candle.Add(price, quantity, timestampSeconds, sessionVwap);
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
        if (info.VwapPrice is { } vwap)
            _vwapBySecurity[securityId] = vwap;

        _parent.NotifyInfoUpdated(securityId);
    }

    public void OnMarketDataUpdated(ulong securityId, InstrumentInfo info)
    {
        if (info.TradingStatus is { } status)
            _tradingStatus[securityId] = status;
        if (info.VwapPrice is { } vwap)
            _vwapBySecurity[securityId] = vwap;

        _parent.NotifyInfoUpdated(securityId);
    }

    /// <summary>
    /// Called after each packet is fully processed on the owning group's thread.
    /// Drains pending requests, flushes conflation buffers into the current broadcast
    /// batch, then publishes the batch to the broadcaster thread.
    /// </summary>
    public void OnBatchComplete()
    {
        // 1. Process shared unsubscribe queue (any group, under _subLock)
        _parent.ProcessUnsubscribes();

        if (_suppressFanout)
        {
            // Feed is in Recovery/CatchUp: the book is being rebuilt and broadcasting
            // partial state to clients would (a) push large catch-up backlogs through
            // the per-client outbound rings and (b) ship interim/inconsistent updates.
            // Discard the wire-event buffers; SetFanoutSuppressed(false) (called on
            // RealTime entry) schedules a fresh snapshot for every Book subscriber.
            // Subscribe/Get requests stay queued and drain when fanout resumes.
            DiscardConflationBuffers();
            return;
        }

        // 2. Process this group's routed subscribe/get requests (single-threaded, safe book access)
        ProcessOwnSubscribeRequests();

        // 3. Flush conflation buffers into _currentBatch (dispatch-side work only —
        //    no subscriber fan-out, no ArrayPool rentals for client buffers).
        if (_orderBuffer.Count != 0 || _clearBuffer.Count != 0 || _tradeBuffer.Count != 0)
            FlushBuffers();

        // 4. Publish the batch to the broadcaster thread. On full ring, drop + schedule
        //    a resnapshot for each affected security so subscribers recover state.
        PublishCurrentBatch();
    }

    /// <summary>
    /// Toggle fanout suppression. When transitioning from suppressed to live, schedules
    /// a fresh book snapshot for every current Book-flag subscriber owned by this group.
    /// Must be called from the dispatch thread (or via FeedHandler StateChanged hook
    /// that runs on the dispatch thread).
    /// </summary>
    public void SetFanoutSuppressed(bool suppressed)
    {
        bool wasSuppressed = _suppressFanout;
        _suppressFanout = suppressed;
        if (wasSuppressed && !suppressed)
            _parent.RequestResyncForAllSubscribersInGroup(this);
    }

    /// <summary>True while OnBatchComplete is dropping wire events instead of fanning out.</summary>
    public bool IsFanoutSuppressed => _suppressFanout;

    private void DiscardConflationBuffers()
    {
        // Equivalent of FlushBuffers without producing wire bytes — keep the upstream
        // event counter accurate so UpstreamConflated reflects discarded work too.
        int discarded = _orderBuffer.Count + _clearBuffer.Count + _tradeBuffer.Count;
        _orderBuffer.Clear();
        _clearBuffer.Clear();
        _tradeBuffer.Clear();
        _eventsFlushed += discarded;
    }

    // ── Request processing ──

    private void ProcessOwnSubscribeRequests()
    {
        // Cap snapshots per batch: each Get/Subscribe runs SendMboSnapshot which allocates
        // a tuple array per side + rents an ArrayPool buffer (book-size proportional).
        // Without this cap, an initial flood (e.g. 500 clients × 200 syms = 100k requests)
        // can saturate the dispatch thread allocator faster than the GC + write loops
        // drain — observed OOM in CopyOrderData under that load. Excess requests stay in
        // the queue and drain on subsequent packets.
        int budget = _maxSnapshotRequestsPerBatch > 0 ? _maxSnapshotRequestsPerBatch : int.MaxValue;
        while (budget-- > 0 && _pendingSubscribeRequests.TryDequeue(out var req))
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

    // ── Buffer flushing (dispatch thread) ──────────────────────────────────────
    //
    // These methods now only *append events to the current broadcast batch*. They do
    // NOT perform subscriber fan-out — that moved to the broadcaster thread
    // (see FanoutBatch). The goal is to keep the dispatch thread's per-packet work
    // bounded and independent of the number of subscribers.

    private void FlushBuffers()
    {
        int flushed = 0;

        Span<byte> tmp = stackalloc byte[64];

        foreach (var (secId, side) in _clearBuffer)
        {
            if (!_parent.IsSubscribed(secId)) continue;
            int len = WireProtocol.WriteBookCleared(tmp, secId, side);
            AppendEventToBatch(secId, tmp[..len], logicalCount: 1);
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
            AppendEventToBatch(order.SecurityId, tmp[..len], logicalCount: 1);
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
                AppendEventToBatch(currentSecId, _flushBuf.AsSpan(segmentStart, offset - segmentStart), segmentCount);
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
            AppendEventToBatch(currentSecId, _flushBuf.AsSpan(segmentStart, offset - segmentStart), segmentCount);

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
            AppendEventToBatch(secId, tmp[..len], logicalCount: 1);
            flushed++;
        }
        _tradeBuffer.Clear();

        // Broadcast candle updates (one per security, after all trades)
        if (candleSecIds is not null)
        {
            Span<byte> cbuf = stackalloc byte[WireProtocol.CandleUpdateMessageSize];
            foreach (var secId in candleSecIds)
            {
                if (!Candles.TryGetValue(secId, out var agg)) continue;
                var latest = agg.GetLatest();
                if (latest is { } c)
                {
                    int clen = WireProtocol.WriteCandleUpdate(cbuf, secId, agg.Resolution, c);
                    AppendEventToBatch(secId, cbuf[..clen], logicalCount: 1);
                }
            }
        }

        return flushed;
    }

    /// <summary>
    /// Dispatch thread: append <paramref name="bytes"/> (one or more pre-serialized wire
    /// messages packed back-to-back) to the current packet's broadcast batch. No
    /// subscriber lookup happens here — that runs on the broadcaster thread when the
    /// batch is fanned out, so dispatch-side work is bounded by event count, not
    /// subscriber count.
    /// </summary>
    private void AppendEventToBatch(ulong securityId, ReadOnlySpan<byte> bytes, int logicalCount)
    {
        if (bytes.Length == 0) return;
        // Skip events for securities nobody is subscribed to. This check is cheap
        // (ConcurrentDictionary lookup) and lets us avoid enqueueing dead bytes into
        // the batch — matches old behaviour which the call sites also guarded against.
        if (!_parent.HasAnyBookSubscriber(securityId)) return;

        _currentBatch ??= BroadcastWorkBatch.Rent();
        _currentBatch.Append(securityId, bytes, logicalCount);
    }

    /// <summary>
    /// Dispatch thread: hand the current batch to the broadcaster thread. On ring full,
    /// drop the batch and enqueue resnapshot (Get) requests for each affected security
    /// so every Book-flag subscriber receives a fresh MBP snapshot to recover state.
    /// </summary>
    private void PublishCurrentBatch()
    {
        var batch = _currentBatch;
        _currentBatch = null;
        if (batch is null) return;
        if (batch.EventCount == 0)
        {
            BroadcastWorkBatch.Return(batch);
            return;
        }

        if (_broadcastRing.TryEnqueue(batch))
        {
            Volatile.Write(ref _broadcastBatchesPublished, _broadcastBatchesPublished + 1);
            return;
        }

        // Ring full: drop + schedule resync for every security in the dropped batch.
        Volatile.Write(ref _broadcastBatchesDroppedFull, _broadcastBatchesDroppedFull + 1);
        EnqueueResyncForDroppedBatch(batch);
        BroadcastWorkBatch.Return(batch);
    }

    private void EnqueueResyncForDroppedBatch(BroadcastWorkBatch batch)
    {
        // Dedupe SecIds (the same security often contributes multiple events per packet).
        // Using a reused scratch HashSet would be nicer but a temporary allocation here
        // is rare (only on ring full) and keeps the method simple.
        var seen = new HashSet<ulong>();
        for (int i = 0; i < batch.EventCount; i++)
        {
            ulong secId = batch.Events[i].SecId;
            if (!seen.Add(secId)) continue;
            if (!_parent.RequestResyncForBookSubscribers(secId)) continue;
            Volatile.Write(ref _broadcastResyncRequests, _broadcastResyncRequests + 1);
        }
    }

    // ── Broadcaster thread ─────────────────────────────────────────────────────
    //
    // Dedicated thread per group drains _broadcastRing and performs subscriber
    // fan-out. Runs entirely off the feed/dispatch thread so client scaling (N=200+)
    // never back-pressures UDP receive.

    private void RunBroadcaster(CancellationToken ct)
    {
        const int spinIterations = 32;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (_broadcastRing.TryDequeue(out var batch))
                {
                    FanoutBatch(batch!);
                    BroadcastWorkBatch.Return(batch!);
                    continue;
                }

                bool gotItem = false;
                for (int i = 0; i < spinIterations; i++)
                {
                    if (_broadcastRing.TryDequeue(out batch))
                    {
                        FanoutBatch(batch!);
                        BroadcastWorkBatch.Return(batch!);
                        gotItem = true;
                        break;
                    }
                    Thread.SpinWait(1 << Math.Min(i, 6));
                }

                if (!gotItem)
                {
                    try { _broadcastRing.WaitForItems(ct); }
                    catch (OperationCanceledException) { break; }
                }
            }
        }
        finally
        {
            // Drain remaining batches on shutdown so pool doesn't leak.
            while (_broadcastRing.TryDequeue(out var batch))
                BroadcastWorkBatch.Return(batch!);
        }
    }

    /// <summary>
    /// Broadcaster thread: for each event in the batch, resolve subscribers and
    /// accumulate wire-bytes into per-session buffers; at end of batch hand each buffer
    /// to its session's outbound channel as one coalesced write. This preserves the
    /// "one Channel.TryWrite per client per packet" coalescing property.
    /// </summary>
    private void FanoutBatch(BroadcastWorkBatch batch)
    {
        for (int i = 0; i < batch.EventCount; i++)
        {
            ref var ev = ref batch.Events[i];
            var bytes = batch.Buffer.AsSpan(ev.Offset, ev.Len);
            AppendForBookSubscribers(ev.SecId, bytes, ev.LogicalCount);
        }
        FlushClientBatches();
    }

    // Hard cap on per-client per-batch accumulator size. Beyond this, the partial
    // buffer is flushed to the client immediately (as its own enqueue) and a fresh
    // one is rented for the remainder of the batch. Bounds peak memory pinned in
    // ArrayPool at high fan-out (e.g. 500 clients × snapshot bursts).
    private const int MaxAccumulatorBytes = 256 * 1024;

    private void AppendForBookSubscribers(ulong securityId, ReadOnlySpan<byte> bytes, int logicalCount)
    {
        var subs = _parent.GetSubscribers(securityId);
        if (subs is null || bytes.Length == 0) return;

        foreach (var (clientId, flags) in subs)
        {
            if (!flags.HasFlag(DataFlags.Book)) continue;
            var session = _parent.GetClient(clientId);
            if (session is null) continue;

            ref var acc = ref CollectionsMarshal.GetValueRefOrAddDefault(_clientBatches, session, out bool exists);
            if (!exists)
            {
                acc.Buffer = ArrayPool<byte>.Shared.Rent(Math.Max(bytes.Length * 4, 1024));
                acc.Offset = 0;
                acc.LogicalCount = 0;
            }
            if (acc.Offset + bytes.Length > acc.Buffer.Length)
            {
                int desired = Math.Max(acc.Buffer.Length * 2, acc.Offset + bytes.Length);
                if (desired <= MaxAccumulatorBytes)
                {
                    var newBuf = ArrayPool<byte>.Shared.Rent(desired);
                    acc.Buffer.AsSpan(0, acc.Offset).CopyTo(newBuf);
                    ArrayPool<byte>.Shared.Return(acc.Buffer);
                    acc.Buffer = newBuf;
                }
                else
                {
                    // Cap reached: flush what we already have to the client and
                    // start a fresh accumulator for the current event.
                    if (acc.Offset > 0)
                    {
                        session.TryEnqueueBatch(
                            new ReadOnlyMemory<byte>(acc.Buffer, 0, acc.Offset),
                            acc.LogicalCount,
                            pooledArray: acc.Buffer);
                    }
                    else
                    {
                        ArrayPool<byte>.Shared.Return(acc.Buffer);
                    }
                    int initial = Math.Max(bytes.Length * 2, 1024);
                    acc.Buffer = ArrayPool<byte>.Shared.Rent(initial);
                    acc.Offset = 0;
                    acc.LogicalCount = 0;
                }
            }
            bytes.CopyTo(acc.Buffer.AsSpan(acc.Offset));
            acc.Offset += bytes.Length;
            acc.LogicalCount += logicalCount;
        }
    }

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
