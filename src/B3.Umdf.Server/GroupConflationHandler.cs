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
    private readonly Dictionary<(ulong SecurityId, byte Side), (long TotalQty, int OrderCount)> _marketTierBuffer = new();
    private readonly List<(ulong SecurityId, byte Side)> _clearBuffer = new();
    private readonly Dictionary<(ulong SecurityId, long Price), (long Qty, long TradeId)> _tradeBuffer = new();
    // Per-symbol stale-status flips. Coalesced per security:
    // multiple flips in the same batch collapse to the latest value.
    private readonly Dictionary<ulong, bool> _staleStatusBuffer = new();
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
    private long _lastPublishedBatchSequence;
    private long _epochResetsObserved;
    private SnapshotClearReason _lastEpochResetReason;

    public long BroadcastBatchesPublished => Volatile.Read(ref _broadcastBatchesPublished);
    public long BroadcastBatchesDroppedFull => Volatile.Read(ref _broadcastBatchesDroppedFull);
    public long BroadcastResyncRequests => Volatile.Read(ref _broadcastResyncRequests);
    /// <summary>
    /// Count of <see cref="IBookEventHandler.OnEpochReset"/> notifications
    /// received from the BookManager (one per ChannelReset_11 / SequenceReset_1
    /// / SequenceVersion change). Sustained growth indicates an unhealthy
    /// upstream (frequent failovers / weekly rollover triggering more often
    /// than expected).
    /// </summary>
    public long EpochResetsObserved => Volatile.Read(ref _epochResetsObserved);
    /// <summary>Reason of the most recently observed epoch reset.</summary>
    public SnapshotClearReason LastEpochResetReason => _lastEpochResetReason;
    public int BroadcastRingDepth => _broadcastRing.ApproximateDepth;
    public int BroadcastRingCapacity => _broadcastRing.Capacity;
    internal long LastPublishedBatchSequence => Volatile.Read(ref _lastPublishedBatchSequence);

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
    // Sources that may independently demand fanout suppression. The
    // effective suppression state is `_suppressionMask != 0`. Keeping each
    // source separate means a transient stale-ratio spike doesn't release
    // suppression that was set by the channel still being in Recovery,
    // and vice-versa.
    [Flags]
    public enum SuppressionSource
    {
        None = 0,
        /// <summary>Channel-level FeedState is not RealTime (cold-start, channel Recovery).</summary>
        ChannelState = 1,
        /// <summary>Too many symbols are Stale relative to the configured threshold.</summary>
        StaleRatio = 2,
    }

    private volatile SuppressionSource _suppressionMask;
    private volatile bool _suppressFanout;

    /// <summary>Evaluator invoked at the start of <see cref="OnBatchComplete"/> (after
    /// pending unsubscribes drain) to refresh dynamic suppression sources such as the
    /// per-symbol Stale ratio. Null by default. Wired in <c>Bootstrap</c>.</summary>
    public Action? PreBatchEvaluator { get; set; }

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

    public void OnMarketTierChanged(OrderBook book, BookSideType side, long totalQuantity, int orderCount)
    {
        if (!_parent.IsSubscribed(book.SecurityId)) return;
        _eventsReceived++;
        _marketTierBuffer[(book.SecurityId, (byte)side)] = (totalQuantity, orderCount);
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
        _eventsReceived++;
        PurgeBufferedOrders(securityId, (byte)side);
        PurgeBufferedMarketTiers(securityId, side);
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

    public void OnTradeBust(ulong securityId, long price, long quantity, long tradeId)
    {
        // Mark the trade in the per-security ring so future subscribers don't
        // see the busted trade in their initial history snapshot. Best-effort:
        // ring is bounded (50 entries) — busts of older trades return false
        // and are silently ignored. Candle volumes are intentionally NOT
        // adjusted (would require per-trade history we don't retain; a
        // volume-only adjustment would still leave OHLC distorted, giving
        // false confidence in recomputed candles). Spec §10 / TradeBust_57.
        if (RecentTrades.TryGetValue(securityId, out var ring))
            ring.MarkBust(tradeId);

        if (!_parent.IsSubscribed(securityId)) return;
        Span<byte> tmp = stackalloc byte[20];
        int len = WireProtocol.WriteTradeBust(tmp, securityId, tradeId);
        AppendEventToBatch(securityId, tmp[..len], logicalCount: 1);
        _eventsReceived++;
    }
    public void OnExecutionSummary(ulong securityId, long lastPx, long fillQty) { }

    public void OnSymbolStaleStatusChanged(ulong securityId, bool isStale)
    {
        if (!_parent.IsSubscribed(securityId)) return;
        _eventsReceived++;
        _staleStatusBuffer[securityId] = isStale;

        // On heal (Stale→Healthy), trigger a fresh book snapshot to all
        // subscribers. The backend book was rebuilt from the always-on snapshot
        // chunks WITHOUT firing per-order events to subscribers, so the
        // frontend's view diverges from the backend state. Re-snapshotting
        // synchronizes them. (For the Stale entry direction we just buffer the
        // status flag — no resnap needed since live updates will keep flowing
        // once heal completes.)
        if (!isStale) _parent.RequestResyncForBookSubscribers(securityId);
    }

    /// <summary>
    /// Catastrophic per-channel reset notification from <see cref="BookManager"/>.
    /// At this point every subscribed book has already received a per-symbol
    /// <see cref="OnBookCleared"/> (via <c>ClearAllBooks</c>) so the in-flight
    /// conflation buffers contain the correct cleanup events for clients —
    /// they will flush normally on the next <see cref="OnBatchComplete"/>.
    /// <para>
    /// Per-symbol session attributes (<c>_tradingStatus</c>, <c>_vwapBySecurity</c>)
    /// and history (<c>RecentTrades</c>, <c>Candles</c>) are NOT invalidated:
    /// they survive across SequenceVersion change (weekly rollover or failover)
    /// because they reflect physical session state, not the volatile incremental
    /// stream's seq space. ChannelReset_11 / SequenceReset_1 are also non-invalidating
    /// for these.
    /// </para>
    /// <para>
    /// Recovery flow continues per-symbol via <see cref="OnSymbolStaleStatusChanged"/>:
    /// when each symbol heals (Stale→Healthy via snapshot), the existing path
    /// fires <see cref="SubscriptionManager.RequestResyncForBookSubscribers"/>.
    /// </para>
    /// </summary>
    public void OnEpochReset(SnapshotClearReason reason)
    {
        Interlocked.Increment(ref _epochResetsObserved);
        _lastEpochResetReason = reason;
    }

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
    /// P13: handle a fully-reassembled News_5 delivery from MarketDataManager.
    /// Runs on the dispatch (feed) thread; spans are valid only for the call.
    /// We synchronously serialize NewsBegin + NewsChunk(s) + NewsEnd wire frames
    /// into the current batch buffer (rented byte storage), and tag each frame
    /// with the routing kind so the broadcaster thread can fan out to the
    /// right subscriber set without re-touching the source spans.
    ///
    /// <para>Routing:</para>
    /// <list type="bullet">
    ///   <item><description><c>securityId != 0</c>: only clients subscribed to
    ///   that securityId AND with <see cref="DataFlags.News"/>.</description></item>
    ///   <item><description><c>securityId == 0</c> (global): every connected
    ///   client with <see cref="DataFlags.News"/>, regardless of per-symbol
    ///   subscriptions.</description></item>
    /// </list>
    ///
    /// <para>Recovery suppression intentionally does NOT apply to news — news
    /// is independent of book consistency.</para>
    /// </summary>
    public void OnNews(
        ulong securityIdOrZero,
        ulong newsId,
        byte source,
        ushort language,
        long origTimeNanos,
        ReadOnlySpan<byte> headline,
        ReadOnlySpan<byte> text,
        ReadOnlySpan<byte> url)
    {
        bool isGlobal = securityIdOrZero == 0;

        // Skip serialization entirely if no candidate subscriber exists.
        if (isGlobal)
        {
            if (!_parent.HasAnyNewsSubscriberAnywhere()) return;
        }
        else
        {
            if (!_parent.HasAnyNewsSubscriberFor(securityIdOrZero)) return;
        }

        var kind = isGlobal
            ? BroadcastWorkBatch.EventKind.NewsGlobal
            : BroadcastWorkBatch.EventKind.NewsForSecurity;

        _currentBatch ??= BroadcastWorkBatch.Rent();
        _eventsReceived++;

        // 1. NewsBegin
        Span<byte> beginBuf = stackalloc byte[WireProtocol.NewsBeginTotalSize];
        int beginLen = WireProtocol.WriteNewsBegin(
            beginBuf, securityIdOrZero, newsId, source, language, origTimeNanos,
            (uint)headline.Length, (uint)text.Length, (uint)url.Length);
        _currentBatch.Append(securityIdOrZero, beginBuf[..beginLen], logicalCount: 1, kind);

        // 2. Per-field chunks
        AppendNewsField(newsId, WireProtocol.NewsField.Headline, headline, kind, securityIdOrZero, isLastField: false);
        AppendNewsField(newsId, WireProtocol.NewsField.Text, text, kind, securityIdOrZero, isLastField: false);
        AppendNewsField(newsId, WireProtocol.NewsField.Url, url, kind, securityIdOrZero, isLastField: true);
    }

    private void AppendNewsField(
        ulong newsId,
        WireProtocol.NewsField field,
        ReadOnlySpan<byte> data,
        BroadcastWorkBatch.EventKind kind,
        ulong secIdRoute,
        bool isLastField)
    {
        const int maxFrag = WireProtocol.NewsChunkMaxFragment;
        const int stackThreshold = 4096;
        int offset = 0;

        if (data.Length == 0)
        {
            int frameSize = WireProtocol.NewsChunkTotalSize(0);
            Span<byte> frame = stackalloc byte[frameSize];
            int len = WireProtocol.WriteNewsChunk(frame, newsId, field, ReadOnlySpan<byte>.Empty,
                isFinal: isLastField);
            _currentBatch!.Append(secIdRoute, frame[..len], logicalCount: 1, kind);
            return;
        }

        Span<byte> stackFrame = stackalloc byte[stackThreshold];
        while (offset < data.Length)
        {
            int take = Math.Min(maxFrag, data.Length - offset);
            int frameSize = WireProtocol.NewsChunkTotalSize(take);
            bool last = isLastField && (offset + take == data.Length);
            if (frameSize <= stackThreshold)
            {
                int len = WireProtocol.WriteNewsChunk(stackFrame, newsId, field, data.Slice(offset, take), last);
                _currentBatch!.Append(secIdRoute, stackFrame[..len], logicalCount: 1, kind);
            }
            else
            {
                byte[] rented = ArrayPool<byte>.Shared.Rent(frameSize);
                try
                {
                    int len = WireProtocol.WriteNewsChunk(rented.AsSpan(), newsId, field, data.Slice(offset, take), last);
                    _currentBatch!.Append(secIdRoute, rented.AsSpan(0, len), logicalCount: 1, kind);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rented);
                }
            }
            offset += take;
        }
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

        // 2. Refresh dynamic suppression sources (e.g. per-symbol stale ratio)
        //    AFTER unsubscribes drain so a suppression release does not enqueue
        //    resync requests for clients that just unsubscribed.
        PreBatchEvaluator?.Invoke();

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

        // 3. Flush conflation buffers into _currentBatch (dispatch-side work only —
        //    no subscriber fan-out, no ArrayPool rentals for client buffers).
        if (_orderBuffer.Count != 0 || _marketTierBuffer.Count != 0 || _clearBuffer.Count != 0 || _tradeBuffer.Count != 0 || _staleStatusBuffer.Count != 0)
            FlushBuffers();

        // 4. Process routed subscribe/get requests after current-packet events have
        //    been serialized, but before the batch is visible to the broadcaster.
        //    Snapshots use the pending batch sequence as a barrier so clients do not
        //    receive deltas already included in their snapshot.
        long snapshotCutoffSequence = PendingBatchSequence;
        ProcessOwnSubscribeRequests(snapshotCutoffSequence);

        // 5. Publish the batch to the broadcaster thread. On full ring, drop + schedule
        //    a resnapshot for each affected security so subscribers recover state.
        PublishCurrentBatch();
    }

    /// <summary>
    /// Toggle fanout suppression. When transitioning from suppressed to live, schedules
    /// a fresh book snapshot for every current Book-flag subscriber owned by this group.
    /// Must be called from the dispatch thread (or via FeedHandler StateChanged hook
    /// that runs on the dispatch thread). Routes to the <see cref="SuppressionSource.ChannelState"/>
    /// source for backward compatibility.
    /// </summary>
    public void SetFanoutSuppressed(bool suppressed)
        => SetSuppressionSource(SuppressionSource.ChannelState, suppressed);

    /// <summary>
    /// Set/clear an individual suppression source. Suppression is the OR of
    /// all sources; the resync hook fires only when the combined mask
    /// transitions from non-zero to zero (i.e. ALL sources released).
    /// </summary>
    public void SetSuppressionSource(SuppressionSource source, bool active)
    {
        var prev = _suppressionMask;
        var next = active ? (prev | source) : (prev & ~source);
        if (next == prev) return;
        _suppressionMask = next;
        _suppressFanout = next != SuppressionSource.None;
        if (prev != SuppressionSource.None && next == SuppressionSource.None)
            _parent.RequestResyncForAllSubscribersInGroup(this);
    }

    /// <summary>True while OnBatchComplete is dropping wire events instead of fanning out.</summary>
    public bool IsFanoutSuppressed => _suppressFanout;

    private long PendingBatchSequence =>
        _currentBatch is { EventCount: > 0 }
            ? LastPublishedBatchSequence + 1
            : LastPublishedBatchSequence;

    private void DiscardConflationBuffers()
    {
        // Equivalent of FlushBuffers without producing wire bytes — keep the upstream
        // event counter accurate so UpstreamConflated reflects discarded work too.
        int discarded = _orderBuffer.Count + _marketTierBuffer.Count + _clearBuffer.Count + _tradeBuffer.Count + _staleStatusBuffer.Count;
        _orderBuffer.Clear();
        _marketTierBuffer.Clear();
        _clearBuffer.Clear();
        _tradeBuffer.Clear();
        _staleStatusBuffer.Clear();
        if (_currentBatch is not null)
        {
            discarded += _currentBatch.EventCount;
            BroadcastWorkBatch.Return(_currentBatch);
            _currentBatch = null;
        }
        _eventsFlushed += discarded;
    }

    // ── Request processing ──

    private void ProcessOwnSubscribeRequests(long bookBatchCutoffSequence)
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
                    _parent.HandleSubscribe(req.ClientId, req.Symbol!, req.Flags, BookManager, this, bookBatchCutoffSequence);
                    break;
                case SubscriptionRequestKind.Get:
                    _parent.HandleGet(req.ClientId, req.Symbol!, req.Flags, BookManager, this, bookBatchCutoffSequence);
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

        ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(_orderBuffer, orderId, out bool existed);
        if (existed)
        {
            if (kind == PendingOrderKind.Deleted)
            {
                if (slot.Kind == PendingOrderKind.Added)
                    _orderBuffer.Remove(orderId); // Add+Delete → drop entirely
                else
                    slot = new BufferedOrder(PendingOrderKind.Deleted, securityId, side, 0, 0);
            }
            else
            {
                var mergedKind = slot.Kind == PendingOrderKind.Added ? PendingOrderKind.Added : kind;
                slot = new BufferedOrder(mergedKind, securityId, side, price, qty);
            }
        }
        else
        {
            slot = new BufferedOrder(kind, securityId, side, price, qty);
        }
    }

    private void BufferOrderDelete(ulong securityId, ulong orderId, byte side)
    {
        _eventsReceived++;
        ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(_orderBuffer, orderId, out bool existed);
        if (existed && slot.Kind == PendingOrderKind.Added)
        {
            _orderBuffer.Remove(orderId); // Add+Delete → drop entirely
            return;
        }
        slot = new BufferedOrder(PendingOrderKind.Deleted, securityId, side, 0, 0);
    }

    private void BufferTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(_tradeBuffer, (securityId, price), out bool existed);
        if (existed)
            slot = (slot.Qty + quantity, tradeId);
        else
            slot = (quantity, tradeId);
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

    private void PurgeBufferedMarketTiers(ulong securityId, BookClearSide clearSide)
    {
        if (clearSide == BookClearSide.Both)
        {
            _marketTierBuffer.Remove((securityId, (byte)BookSideType.Bid));
            _marketTierBuffer.Remove((securityId, (byte)BookSideType.Ask));
            return;
        }

        var side = clearSide == BookClearSide.Bid ? BookSideType.Bid : BookSideType.Ask;
        _marketTierBuffer.Remove((securityId, (byte)side));
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
            int len = WireProtocol.WriteBookCleared(tmp, secId, side);
            AppendEventToBatch(secId, tmp[..len], logicalCount: 1);
            flushed++;
        }
        _clearBuffer.Clear();

        if (_orderBuffer.Count > 0)
            flushed += FlushOrderBuffer();

        if (_marketTierBuffer.Count > 0)
            flushed += FlushMarketTierBuffer();

        if (_tradeBuffer.Count > 0)
            flushed += FlushTradeBuffer();

        if (_staleStatusBuffer.Count > 0)
        {
            Span<byte> staleTmp = stackalloc byte[16];
            foreach (var (secId, isStale) in _staleStatusBuffer)
            {
                int len = WireProtocol.WriteSymbolStaleStatus(staleTmp, secId, isStale);
                AppendEventToBatch(secId, staleTmp[..len], logicalCount: 1);
                flushed++;
            }
            _staleStatusBuffer.Clear();
        }

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

    private int FlushMarketTierBuffer()
    {
        int flushed = 0;
        Span<byte> tmp = stackalloc byte[32];
        foreach (var ((secId, side), (totalQty, orderCount)) in _marketTierBuffer)
        {
            int len = WireProtocol.WriteMarketTierUpdate(tmp, secId, side, totalQty, orderCount);
            AppendEventToBatch(secId, tmp[..len], logicalCount: 1);
            flushed++;
        }
        _marketTierBuffer.Clear();
        return flushed;
    }

    private int FlushTradeBuffer()
    {
        int flushed = 0;
        Span<byte> tmp = stackalloc byte[64];

        // Collect securities that need candle broadcasts (deduplicated). All entries
        // already came in via subscribed callers so no additional filter is needed.
        HashSet<ulong>? candleSecIds = null;
        foreach (var ((secId, _), _) in _tradeBuffer)
        {
            candleSecIds ??= new();
            candleSecIds.Add(secId);
        }

        // Flush trades
        foreach (var ((secId, price), (qty, tradeId)) in _tradeBuffer)
        {
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

        long sequence = LastPublishedBatchSequence + 1;
        batch.Sequence = sequence;
        Volatile.Write(ref _lastPublishedBatchSequence, sequence);

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
            switch (ev.Kind)
            {
                case BroadcastWorkBatch.EventKind.BookSubscribers:
                    AppendForBookSubscribers(ev.SecId, bytes, ev.LogicalCount, batch.Sequence);
                    break;
                case BroadcastWorkBatch.EventKind.NewsForSecurity:
                    AppendForNewsSubscribers(ev.SecId, bytes, ev.LogicalCount);
                    break;
                case BroadcastWorkBatch.EventKind.NewsGlobal:
                    AppendForGlobalNewsSubscribers(bytes, ev.LogicalCount);
                    break;
            }
        }
        FlushClientBatches();
    }

    /// <summary>Per-symbol news fan-out: clients subscribed to <paramref name="securityId"/>
    /// who have <see cref="DataFlags.News"/> set. Mirrors
    /// <see cref="AppendForBookSubscribers"/> but skips the broadcast-sequence gating
    /// (news is independent of book-snapshot epochs).</summary>
    private void AppendForNewsSubscribers(ulong securityId, ReadOnlySpan<byte> bytes, int logicalCount)
    {
        var subs = _parent.GetSubscribers(securityId);
        if (subs is null || bytes.Length == 0) return;
        foreach (var (clientId, state) in subs)
        {
            if (!state.WantsNews) continue;
            var session = _parent.GetClient(clientId);
            if (session is null) continue;
            AppendToClient(session, bytes, logicalCount);
        }
    }

    /// <summary>Global news fan-out: every connected client whose state on any
    /// security has <see cref="DataFlags.News"/> set. Each client receives the
    /// frame at most once even if subscribed to many securities.</summary>
    private void AppendForGlobalNewsSubscribers(ReadOnlySpan<byte> bytes, int logicalCount)
    {
        if (bytes.Length == 0) return;
        foreach (var (_, session) in _parent.EnumerateAllClients())
        {
            if (!session.HasAnyNewsSubscription) continue;
            AppendToClient(session, bytes, logicalCount);
        }
    }

    /// <summary>Shared accumulator append used by per-symbol and global news fan-out.
    /// Extracted so the buffer-grow / flush-on-cap policy stays consistent with
    /// <see cref="AppendForBookSubscribers"/>.</summary>
    private void AppendToClient(ClientSession session, ReadOnlySpan<byte> bytes, int logicalCount)
    {
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
                acc.Buffer = ArrayPool<byte>.Shared.Rent(Math.Max(bytes.Length, 1024));
                acc.Offset = 0;
                acc.LogicalCount = 0;
            }
        }
        bytes.CopyTo(acc.Buffer.AsSpan(acc.Offset));
        acc.Offset += bytes.Length;
        acc.LogicalCount += logicalCount;
    }

    // Hard cap on per-client per-batch accumulator size. Beyond this, the partial
    // buffer is flushed to the client immediately (as its own enqueue) and a fresh
    // one is rented for the remainder of the batch. Bounds peak memory pinned in
    // ArrayPool at high fan-out (e.g. 500 clients × snapshot bursts).
    private const int MaxAccumulatorBytes = 256 * 1024;

    private void AppendForBookSubscribers(ulong securityId, ReadOnlySpan<byte> bytes, int logicalCount, long batchSequence)
    {
        var subs = _parent.GetSubscribers(securityId);
        if (subs is null || bytes.Length == 0) return;

        foreach (var (clientId, state) in subs)
        {
            if (!state.WantsBookBatch(batchSequence)) continue;
            var session = _parent.GetClient(clientId);
            if (session is null) continue;
            AppendToClient(session, bytes, logicalCount);
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
