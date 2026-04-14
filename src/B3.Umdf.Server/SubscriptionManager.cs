using System.Collections.Concurrent;
using System.Diagnostics;
using B3.Umdf.Book;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server;

/// <summary>
/// Central subscription registry. Manages client connections, symbol resolution,
/// snapshot delivery, and rankings broadcast.
///
/// Per-group <see cref="GroupEventHandler"/> instances handle order/trade buffering
/// and upstream conflation on their own group thread (single-threaded, no locks).
/// Subscription state uses <see cref="ConcurrentDictionary{TKey,TValue}"/> with
/// copy-on-write inner dictionaries for lock-free reads on the hot path.
/// A lightweight <see cref="_subLock"/> serialises rare subscription mutations.
/// </summary>
public sealed class SubscriptionManager
{
    private volatile BookManager[]? _bookManagers;
    private volatile MarketDataManager[]? _marketDataManagers;
    private SymbolRegistry? _symbolRegistry;
    private readonly ILogger<SubscriptionManager> _logger;

    // Serialises subscription mutations (subscribe/unsubscribe).
    // NOT taken on the hot path (order/trade buffering/flush).
    private readonly object _subLock = new();

    private readonly ConcurrentDictionary<string, ClientSession> _clients = new();

    // Per-security: clientId → DataFlags.
    // Outer dict is ConcurrentDictionary (lock-free reads).
    // Inner dicts use copy-on-write under _subLock for safe concurrent iteration.
    private readonly ConcurrentDictionary<ulong, Dictionary<string, DataFlags>> _subscriptions = new();

    // Pending unsubscribe requests from WebSocket threads (processed by any group)
    private readonly ConcurrentQueue<SubscriptionRequest> _pendingUnsubscribes = new();

    private const int RankingsTopN = 10;
    internal const int MaxRecentTrades = 50;

    private volatile GroupEventHandler[]? _groupHandlers;

    /// <summary>Number of events eliminated by upstream conflation across all groups.</summary>
    public long UpstreamConflated
    {
        get
        {
            long total = 0;
            if (_groupHandlers is { } handlers)
                foreach (var gh in handlers)
                    total += gh.UpstreamConflated;
            return total;
        }
    }

    private volatile bool _ready;

    public SubscriptionManager(ILogger<SubscriptionManager>? logger = null)
    {
        _logger = logger ?? NullLogger<SubscriptionManager>.Instance;
    }

    public bool IsReady => _ready;

    /// <summary>Current number of connected clients.</summary>
    public int ClientCount => _clients.Count;

    /// <summary>Get queue depth for all connected clients.</summary>
    public IEnumerable<(string Id, int QueueDepth)> GetClientStats()
    {
        foreach (var (_, session) in _clients)
            yield return (session.Id, session.QueueDepth);
    }

    /// <summary>
    /// Creates a per-group event handler. Each handler owns its conflation buffers
    /// and trade ring buffers. Call <see cref="GroupEventHandler.SetBookManager"/>
    /// after construction to bind the handler to its BookManager.
    /// </summary>
    public GroupEventHandler CreateGroupHandler()
    {
        return new GroupEventHandler(this);
    }

    public void SetDataSources(
        BookManager[] bookManagers,
        MarketDataManager[] marketDataManagers,
        SymbolRegistry symbolRegistry,
        GroupEventHandler[] groupHandlers)
    {
        _bookManagers = bookManagers;
        _marketDataManagers = marketDataManagers;
        _symbolRegistry = symbolRegistry;
        _groupHandlers = groupHandlers;
    }

    /// <summary>Expose symbol registry for diagnostic endpoints.</summary>
    public SymbolRegistry? SymbolRegistry => _symbolRegistry;

    /// <summary>All per-group book managers.</summary>
    public BookManager[]? BookManagers => _bookManagers;

    /// <summary>All per-group market data managers.</summary>
    public MarketDataManager[]? MarketDataManagers => _marketDataManagers;

    /// <summary>Register a client session.</summary>
    public void RegisterClient(ClientSession session)
    {
        _clients[session.Id] = session;
        if (_marketDataManagers is { Length: > 0 } managers)
            session.SetMarketDataManagers(managers);
        AppMetrics.WsConnectionsActive.Add(1);
    }

    /// <summary>Unregister a client and remove all its subscriptions.</summary>
    public void UnregisterClient(string clientId)
    {
        _clients.TryRemove(clientId, out _);
        _pendingUnsubscribes.Enqueue(SubscriptionRequest.UnsubscribeAll(clientId));
        AppMetrics.WsConnectionsActive.Add(-1);
    }

    /// <summary>Called from WebSocket read thread to request a subscription.</summary>
    public void RequestSubscribe(string clientId, string symbol, DataFlags flags)
    {
        var req = SubscriptionRequest.Subscribe(clientId, symbol, flags);
        if (!RouteToGroup(req))
        {
            // Could not route — send error directly (safe: TryEnqueue is multi-writer)
            if (_clients.TryGetValue(clientId, out var session))
            {
                if (!_ready)
                    SendError(session, SubscribeErrorCode.NotReady, symbol);
                else
                    SendError(session, SubscribeErrorCode.UnknownSymbol, symbol);
            }
        }
    }

    /// <summary>Called from WebSocket read thread to request a one-shot snapshot.</summary>
    public void RequestGet(string clientId, string symbol, DataFlags flags)
    {
        var req = SubscriptionRequest.Get(clientId, symbol, flags);
        if (!RouteToGroup(req))
        {
            if (_clients.TryGetValue(clientId, out var session))
            {
                if (!_ready)
                    SendError(session, SubscribeErrorCode.NotReady, symbol);
                else
                    SendError(session, SubscribeErrorCode.UnknownSymbol, symbol);
            }
        }
    }

    /// <summary>Called from WebSocket read thread to unsubscribe.</summary>
    public void RequestUnsubscribe(string clientId, ulong securityId)
    {
        _pendingUnsubscribes.Enqueue(SubscriptionRequest.Unsubscribe(clientId, securityId));
    }

    /// <summary>Route a subscribe/get request to the correct group's queue.</summary>
    private bool RouteToGroup(SubscriptionRequest req)
    {
        if (_groupHandlers is null || _symbolRegistry is null || !_ready) return false;
        if (!_symbolRegistry.TryResolve(req.Symbol!, out var secId)) return false;

        foreach (var gh in _groupHandlers)
        {
            if (gh.BookManager.Books.ContainsKey(secId))
            {
                gh.EnqueueRequest(req.ClientId, req.Symbol, req.Flags,
                    req.Kind == SubscriptionRequestKind.Get);
                return true;
            }
        }
        return false;
    }

    // --- Called by GroupEventHandler from the owning group's thread ---

    /// <summary>Process pending unsubscribes. Called from any group's OnBatchComplete.</summary>
    internal void ProcessUnsubscribes()
    {
        while (_pendingUnsubscribes.TryDequeue(out var req))
        {
            lock (_subLock)
            {
                switch (req.Kind)
                {
                    case SubscriptionRequestKind.Unsubscribe:
                        HandleUnsubscribe(req.ClientId, req.SecurityId);
                        break;
                    case SubscriptionRequestKind.UnsubscribeAll:
                        HandleUnsubscribeAll(req.ClientId);
                        break;
                }
            }
        }
    }

    /// <summary>Lock-free check whether any client is subscribed to a security.</summary>
    internal bool IsSubscribed(ulong securityId) => _subscriptions.ContainsKey(securityId);

    /// <summary>Broadcast pre-serialized bytes to all Book subscribers for a security.</summary>
    internal void BroadcastToSubscribers(ulong securityId, ReadOnlyMemory<byte> payload)
    {
        // Lock-free read: inner dict is a copy-on-write snapshot, safe to iterate.
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return;
        foreach (var (clientId, flags) in clients)
        {
            if (!flags.HasFlag(DataFlags.Book)) continue;
            if (_clients.TryGetValue(clientId, out var session))
            {
                session.TryEnqueue(payload);
                AppMetrics.WsMessagesSent.Add(1);
            }
        }
    }

    // --- Subscribe handling (called on owning group's thread) ---

    internal void HandleSubscribe(string clientId, string symbol, DataFlags flags,
        BookManager bookManager, GroupEventHandler group)
    {
        if (!TryValidateAndResolve(clientId, symbol, out var session, out var securityId))
            return;

        // Send SubscribeOk
        var okBuf = new byte[WireProtocol.FramingHeaderSize + 8 + 1 + 1 + System.Text.Encoding.UTF8.GetMaxByteCount(symbol.Length)];
        int okLen = WireProtocol.WriteSubscribeOk(okBuf, securityId, flags, symbol);
        session.TryEnqueue(new ReadOnlyMemory<byte>(okBuf, 0, okLen));

        // Send snapshots BEFORE activating incremental forwarding.
        // Safe: we're on the owning group's thread — no concurrent book mutations.
        SendSnapshots(session, securityId, flags, bookManager, group);

        // Activate subscription — incrementals start flowing after this point
        lock (_subLock)
        {
            session.AddSubscription(securityId);
            if (flags.HasFlag(DataFlags.Info))
                session.AddInfoSubscription(securityId);

            // Copy-on-write: create new inner dict to avoid mutating a dict being iterated
            _subscriptions.TryGetValue(securityId, out var existing);
            var newClients = existing is not null ? new Dictionary<string, DataFlags>(existing) : new();
            newClients[clientId] = flags;
            _subscriptions[securityId] = newClients;
        }
        AppMetrics.WsSubscriptions.Add(1);
    }

    internal void HandleGet(string clientId, string symbol, DataFlags flags,
        BookManager bookManager, GroupEventHandler group)
    {
        if (!TryValidateAndResolve(clientId, symbol, out var session, out var securityId))
            return;

        SendSnapshots(session, securityId, flags, bookManager, group);
    }

    /// <summary>Validate client, readiness, and symbol resolution. Sends error responses on failure.</summary>
    private bool TryValidateAndResolve(string clientId, string symbol, out ClientSession session, out ulong securityId)
    {
        securityId = 0;
        if (!_clients.TryGetValue(clientId, out session!)) return false;
        if (_symbolRegistry is null) return false;

        if (!_ready)
        {
            SendError(session, SubscribeErrorCode.NotReady, symbol);
            return false;
        }

        if (!_symbolRegistry.TryResolve(symbol, out securityId))
        {
            SendError(session, SubscribeErrorCode.UnknownSymbol, symbol);
            return false;
        }

        return true;
    }

    private static void SendError(ClientSession session, SubscribeErrorCode code, string symbol)
    {
        var buf = new byte[WireProtocol.FramingHeaderSize + 1 + 1 + System.Text.Encoding.UTF8.GetMaxByteCount(symbol.Length)];
        int len = WireProtocol.WriteSubscribeError(buf, code, symbol);
        session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len));
    }

    private void SendSnapshots(ClientSession session, ulong securityId, DataFlags flags,
        BookManager bookManager, GroupEventHandler group)
    {
        if (flags.HasFlag(DataFlags.Book))
        {
            if (bookManager.Books.TryGetValue(securityId, out var book))
                SendMboSnapshot(session, book);
            else
            {
                var emptyBuf = new byte[WireProtocol.BookSnapshotSize(0, 0)];
                WireProtocol.WriteBookSnapshotHeader(emptyBuf, securityId, 0, 0, 0);
                session.TryEnqueue(new ReadOnlyMemory<byte>(emptyBuf));
            }

            // Send recent trade history from the owning group's ring buffer
            if (group.RecentTrades.TryGetValue(securityId, out var trades))
                SendTradeHistory(session, securityId, trades);
        }

        if (flags.HasFlag(DataFlags.Info))
        {
            // Search across all MarketDataManagers for the instrument
            if (_marketDataManagers is { } managers)
            {
                foreach (var mdm in managers)
                {
                    if (mdm.InstrumentData.TryGetValue(securityId, out var info))
                    {
                        SendInfoSnapshot(session, securityId, info);
                        break;
                    }
                }
            }
        }
    }

    private void HandleUnsubscribe(string clientId, ulong securityId)
    {
        // Must be called under _subLock
        if (!_clients.TryGetValue(clientId, out var session)) return;

        session.RemoveSubscription(securityId);

        // Copy-on-write
        if (_subscriptions.TryGetValue(securityId, out var existing))
        {
            var newClients = new Dictionary<string, DataFlags>(existing);
            newClients.Remove(clientId);
            if (newClients.Count == 0)
                _subscriptions.TryRemove(securityId, out _);
            else
                _subscriptions[securityId] = newClients;
        }

        var buf = new byte[12];
        int len = WireProtocol.WriteUnsubscribed(buf, securityId);
        session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len));
    }

    private void HandleUnsubscribeAll(string clientId)
    {
        // Must be called under _subLock.
        // Copy-on-write: build list of security IDs that need updating.
        List<ulong>? toRemove = null;
        List<ulong>? toUpdate = null;

        foreach (var (secId, clients) in _subscriptions)
        {
            if (!clients.ContainsKey(clientId)) continue;

            if (clients.Count == 1)
            {
                toRemove ??= new();
                toRemove.Add(secId);
            }
            else
            {
                toUpdate ??= new();
                toUpdate.Add(secId);
            }
        }

        if (toRemove is not null)
            foreach (var key in toRemove)
                _subscriptions.TryRemove(key, out _);

        if (toUpdate is not null)
        {
            foreach (var secId in toUpdate)
            {
                if (!_subscriptions.TryGetValue(secId, out var existing)) continue;
                var newClients = new Dictionary<string, DataFlags>(existing);
                newClients.Remove(clientId);
                _subscriptions[secId] = newClients;
            }
        }
    }

    // --- Snapshot serialization ---

    private static void SendMboSnapshot(ClientSession session, OrderBook book)
    {
        ulong securityId = book.SecurityId;
        uint lastRptSeq = book.LastRptSeq;
        var bidOrders = CopyOrderData(book.Bids);
        var askOrders = CopyOrderData(book.Asks);

        int headerSize = WireProtocol.BookSnapshotSize(0, 0);
        int totalOrders = bidOrders.Length + askOrders.Length;
        var buf = new byte[headerSize + totalOrders * 37];

        WireProtocol.WriteBookSnapshotHeader(buf, securityId, lastRptSeq, 0, 0);
        int offset = headerSize;

        SerializeOrderArray(buf, ref offset, securityId, bidOrders);
        SerializeOrderArray(buf, ref offset, securityId, askOrders);

        session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, offset));
    }

    private static (ulong OrderId, byte Side, long Price, long Quantity)[] CopyOrderData(BookSide side)
    {
        var orders = side.Orders;
        var result = new (ulong, byte, long, long)[orders.Count];
        int i = 0;
        foreach (var entry in orders.Values)
            result[i++] = (entry.OrderId, (byte)entry.Side, entry.Price, entry.Quantity);
        return result;
    }

    private static void SerializeOrderArray(byte[] buf, ref int offset, ulong securityId,
        (ulong OrderId, byte Side, long Price, long Quantity)[] orders)
    {
        foreach (var (orderId, side, price, quantity) in orders)
        {
            int len = WireProtocol.WriteOrderEvent(buf.AsSpan(offset), MessageType.OrderAdded,
                securityId, orderId, side, price, quantity);
            offset += len;
        }
    }

    private static void SendInfoSnapshot(ClientSession session, ulong securityId, InstrumentInfo info)
    {
        var buf = new byte[WireProtocol.InfoSnapshotMaxSize];
        int len = WireProtocol.WriteInfoSnapshot(buf, securityId, info);
        session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len));
    }

    // --- Rankings ---

    private void PushRankings()
    {
        if (_marketDataManagers is null || _symbolRegistry is null) return;

        var volumeList = new List<RankingEntry>();
        var gainerList = new List<RankingEntry>();
        var loserList = new List<RankingEntry>();

        // Aggregate across all per-group MarketDataManagers
        foreach (var mdm in _marketDataManagers)
        {
            foreach (var (secId, info) in mdm.InstrumentData)
            {
                if (!_symbolRegistry.TryGetSymbol(secId, out var sym)) continue;

                if (info.TradeVolume is { } vol and > 0)
                    volumeList.Add(new RankingEntry(secId, vol, sym));

                if (info.NetChangeFromPrevDay is { } chg)
                {
                    if (chg > 0) gainerList.Add(new RankingEntry(secId, chg, sym));
                    else if (chg < 0) loserList.Add(new RankingEntry(secId, chg, sym));
                }
            }
        }

        volumeList.Sort((a, b) => b.Value.CompareTo(a.Value));
        gainerList.Sort((a, b) => b.Value.CompareTo(a.Value));
        loserList.Sort((a, b) => a.Value.CompareTo(b.Value));

        var volume = volumeList.Count > RankingsTopN
            ? volumeList.GetRange(0, RankingsTopN).ToArray()
            : volumeList.ToArray();
        var gainers = gainerList.Count > RankingsTopN
            ? gainerList.GetRange(0, RankingsTopN).ToArray()
            : gainerList.ToArray();
        var losers = loserList.Count > RankingsTopN
            ? loserList.GetRange(0, RankingsTopN).ToArray()
            : loserList.ToArray();

        var buf = new byte[WireProtocol.RankingsUpdateMaxSize];
        int len = WireProtocol.WriteRankingsUpdate(buf, volume, gainers, losers);
        var payload = new ReadOnlyMemory<byte>(buf, 0, len);

        foreach (var (_, client) in _clients)
            client.TryEnqueue(payload);
    }

    /// <summary>Called when feed enters RealTime state. Enables subscriptions and starts background rankings.</summary>
    public void SetReady()
    {
        _ready = true;
        StartRankingsTimer();
    }

    private Timer? _rankingsTimer;
    private const long RankingsIntervalMs = 300;
    private int _rankingsTick;
    private const int PromoteEveryNTicks = 100; // ~30s at 300ms interval

    private void StartRankingsTimer()
    {
        _rankingsTimer = new Timer(_ =>
        {
            if (++_rankingsTick % PromoteEveryNTicks == 0)
                _symbolRegistry?.TryPromote();

            if (_clients.Count > 0)
                PushRankings();
        }, null, RankingsIntervalMs, RankingsIntervalMs);
    }

    /// <summary>Stop the background rankings timer.</summary>
    public void StopRankingsTimer() => _rankingsTimer?.Dispose();

    /// <summary>Find an instrument across all per-group MarketDataManagers.</summary>
    public InstrumentInfo? FindInstrumentInfo(ulong securityId)
    {
        if (_marketDataManagers is not { } managers) return null;
        foreach (var mdm in managers)
            if (mdm.InstrumentData.TryGetValue(securityId, out var info))
                return info;
        return null;
    }

    // --- Internal types ---

    private enum SubscriptionRequestKind : byte
    {
        Subscribe,
        Get,
        Unsubscribe,
        UnsubscribeAll,
    }

    private readonly struct SubscriptionRequest
    {
        public SubscriptionRequestKind Kind { get; }
        public string ClientId { get; }
        public string? Symbol { get; }
        public ulong SecurityId { get; }
        public DataFlags Flags { get; }

        private SubscriptionRequest(SubscriptionRequestKind kind, string clientId, string? symbol, ulong securityId, DataFlags flags)
        {
            Kind = kind;
            ClientId = clientId;
            Symbol = symbol;
            SecurityId = securityId;
            Flags = flags;
        }

        public static SubscriptionRequest Subscribe(string clientId, string symbol, DataFlags flags)
            => new(SubscriptionRequestKind.Subscribe, clientId, symbol, 0, flags);

        public static SubscriptionRequest Get(string clientId, string symbol, DataFlags flags)
            => new(SubscriptionRequestKind.Get, clientId, symbol, 0, flags);

        public static SubscriptionRequest Unsubscribe(string clientId, ulong securityId)
            => new(SubscriptionRequestKind.Unsubscribe, clientId, null, securityId, DataFlags.None);

        public static SubscriptionRequest UnsubscribeAll(string clientId)
            => new(SubscriptionRequestKind.UnsubscribeAll, clientId, null, 0, DataFlags.None);
    }

    private static void SendTradeHistory(ClientSession session, ulong securityId, TradeRingBuffer ring)
    {
        foreach (var (price, qty, tradeId) in ring.AsSpan())
        {
            var buf = new byte[36];
            int len = WireProtocol.WriteTrade(buf, securityId, price, qty, tradeId);
            session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len));
        }
    }

    /// <summary>Fixed-capacity ring buffer of recent trades for a single security.</summary>
    internal sealed class TradeRingBuffer
    {
        private readonly (long Price, long Qty, long TradeId)[] _buf;
        private volatile int _head; // next write position
        private volatile int _count;

        public TradeRingBuffer(int capacity) => _buf = new (long, long, long)[capacity];

        public void Add(long price, long qty, long tradeId)
        {
            _buf[_head] = (price, qty, tradeId);
            _head = (_head + 1) % _buf.Length;
            if (_count < _buf.Length) _count++;
        }

        /// <summary>Snapshot oldest → newest. Safe for concurrent reads.</summary>
        public IEnumerable<(long Price, long Qty, long TradeId)> AsSpan()
        {
            int count = _count;
            int head = _head;
            int start = count < _buf.Length ? 0 : head;
            for (int i = 0; i < count; i++)
                yield return _buf[(start + i) % _buf.Length];
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Per-group event handler — owns conflation buffers & trade rings
    // ═══════════════════════════════════════════════════════════════

    /// <summary>
    /// Per-group book event handler. Owns conflation buffers and trade ring buffers.
    /// All methods run on the owning group's single thread — no locks needed.
    /// </summary>
    public sealed class GroupEventHandler : IBookEventHandler
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

        internal GroupEventHandler(SubscriptionManager parent)
        {
            _parent = parent;
        }

        // --- IBookEventHandler ---

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
                static _ => new TradeRingBuffer(MaxRecentTrades));
            ring.Add(price, quantity, tradeId);

            if (!_parent.IsSubscribed(securityId)) return;
            _eventsReceived++;
            var key = (securityId, price);
            if (_tradeBuffer.TryGetValue(key, out var existing))
                _tradeBuffer[key] = (existing.Qty + quantity, tradeId);
            else
                _tradeBuffer[key] = (quantity, tradeId);
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
                static _ => new TradeRingBuffer(MaxRecentTrades));
            ring.Add(price, quantity, tradeId);

            if (!_parent.IsSubscribed(securityId)) return;
            _eventsReceived++;
            var key = (securityId, price);
            if (_tradeBuffer.TryGetValue(key, out var existing))
                _tradeBuffer[key] = (existing.Qty + quantity, tradeId);
            else
                _tradeBuffer[key] = (quantity, tradeId);
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

        // --- Upstream conflation ---

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
            int flushed = 0;

            if (_orderBuffer.Count <= 4)
            {
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
            }
            else
            {
                int maxPerEvent = 37;
                int totalBufSize = _orderBuffer.Count * maxPerEvent;
                if (_flushBuf.Length < totalBufSize)
                    _flushBuf = new byte[Math.Max(totalBufSize, _flushBuf.Length * 2)];

                ulong currentSecId = 0;
                int segmentStart = 0;
                int offset = 0;

                Span<(ulong OrderId, BufferedOrder Order)> sorted = _orderBuffer.Count <= 256
                    ? stackalloc (ulong, BufferedOrder)[_orderBuffer.Count]
                    : new (ulong, BufferedOrder)[_orderBuffer.Count];
                int idx = 0;
                foreach (var kv in _orderBuffer)
                    sorted[idx++] = (kv.Key, kv.Value);
                sorted.Sort((a, b) => a.Order.SecurityId.CompareTo(b.Order.SecurityId));

                for (int i = 0; i < sorted.Length; i++)
                {
                    var (orderId, order) = sorted[i];
                    if (order.SecurityId != currentSecId && offset > segmentStart)
                    {
                        if (_parent.IsSubscribed(currentSecId))
                        {
                            var copy = new byte[offset - segmentStart];
                            _flushBuf.AsSpan(segmentStart, offset - segmentStart).CopyTo(copy);
                            _parent.BroadcastToSubscribers(currentSecId, copy);
                        }
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
                if (offset > segmentStart && _parent.IsSubscribed(currentSecId))
                {
                    var copy = new byte[offset - segmentStart];
                    _flushBuf.AsSpan(segmentStart, offset - segmentStart).CopyTo(copy);
                    _parent.BroadcastToSubscribers(currentSecId, copy);
                }
            }

            _orderBuffer.Clear();
            return flushed;
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
}
