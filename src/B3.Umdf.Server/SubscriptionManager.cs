using System.Collections.Concurrent;
using System.Diagnostics;
using B3.Umdf.Book;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server;

/// <summary>
/// Manages WebSocket subscriptions and forwards market data to clients.
/// All subscription management (including snapshot delivery) runs on the feed thread,
/// which guarantees snapshot-then-incremental ordering without locks or buffering.
/// </summary>
public sealed class SubscriptionManager : IBookEventHandler, IMarketDataEventHandler
{
    private BookManager? _bookManager;
    private MarketDataManager? _marketDataManager;
    private SymbolRegistry? _symbolRegistry;
    private readonly ILogger<SubscriptionManager> _logger;

    private readonly ConcurrentDictionary<string, ClientSession> _clients = new();
    // Per-security: clientId → DataFlags — only accessed on feed thread
    private readonly Dictionary<ulong, Dictionary<string, DataFlags>> _subscriptions = new();
    // Recent trades per security — circular buffer, only accessed on feed thread
    private readonly Dictionary<ulong, TradeRingBuffer> _recentTrades = new();
    // Pending subscription requests from WebSocket threads
    private readonly ConcurrentQueue<SubscriptionRequest> _pendingRequests = new();
    // Slow-client detection: clientId → consecutive high-watermark ticks
    private readonly Dictionary<string, int> _slowClientTicks = new();
    // Clients needing MBO resync due to dropped events — clientId → set of securityIds
    private readonly Dictionary<string, HashSet<ulong>> _dirtyClients = new();
    private int _resyncThrottle;
    private readonly Stopwatch _rankingsTimer = Stopwatch.StartNew();
    private const long RankingsIntervalMs = 2000;
    private const int RankingsTopN = 10;

    private const double SlowClientThreshold = 0.75; // 75% of channel capacity
    private const int SlowClientMaxTicks = 100; // consecutive overloaded ticks before disconnect
    private const int MaxRecentTrades = 50;
    private const int ResyncThrottleInterval = 1000; // check dirty clients every N events

    private volatile bool _ready;
    private long _slowClientDisconnects;
    private long _resyncCount;

    public SubscriptionManager(ILogger<SubscriptionManager>? logger = null)
    {
        _logger = logger ?? NullLogger<SubscriptionManager>.Instance;
    }

    public bool IsReady => _ready;

    /// <summary>Count of clients disconnected for being slow.</summary>
    public long SlowClientDisconnects => Volatile.Read(ref _slowClientDisconnects);

    /// <summary>Count of MBO resync snapshots sent due to dropped events.</summary>
    public long ResyncCount => Volatile.Read(ref _resyncCount);

    /// <summary>Current number of connected clients.</summary>
    public int ClientCount => _clients.Count;

    public void SetDataSources(BookManager bookManager, MarketDataManager marketDataManager, SymbolRegistry symbolRegistry)
    {
        _bookManager = bookManager;
        _marketDataManager = marketDataManager;
        _symbolRegistry = symbolRegistry;
    }

    /// <summary>Expose symbol registry for diagnostic endpoints.</summary>
    public SymbolRegistry? SymbolRegistry => _symbolRegistry;

    /// <summary>Expose book manager for diagnostic endpoints.</summary>
    public BookManager? BookManager => _bookManager;

    /// <summary>Expose market data manager for diagnostic endpoints.</summary>
    public MarketDataManager? MarketDataManager => _marketDataManager;

    /// <summary>Register a client session.</summary>
    public void RegisterClient(ClientSession session)
    {
        _clients[session.Id] = session;
        AppMetrics.WsConnectionsActive.Add(1);
    }

    /// <summary>Unregister a client and remove all its subscriptions.</summary>
    public void UnregisterClient(string clientId)
    {
        _clients.TryRemove(clientId, out _);
        _pendingRequests.Enqueue(SubscriptionRequest.UnsubscribeAll(clientId));
        AppMetrics.WsConnectionsActive.Add(-1);
    }

    /// <summary>Called from WebSocket read thread to request a subscription.</summary>
    public void RequestSubscribe(string clientId, string symbol, DataFlags flags)
    {
        _pendingRequests.Enqueue(SubscriptionRequest.Subscribe(clientId, symbol, flags));
    }

    /// <summary>Called from WebSocket read thread to request a one-shot snapshot.</summary>
    public void RequestGet(string clientId, string symbol, DataFlags flags)
    {
        _pendingRequests.Enqueue(SubscriptionRequest.Get(clientId, symbol, flags));
    }

    /// <summary>Called from WebSocket read thread to unsubscribe.</summary>
    public void RequestUnsubscribe(string clientId, ulong securityId)
    {
        _pendingRequests.Enqueue(SubscriptionRequest.Unsubscribe(clientId, securityId));
    }

    // --- Feed thread: process pending requests ---

    private void ProcessPendingRequests()
    {
        while (_pendingRequests.TryDequeue(out var req))
        {
            switch (req.Kind)
            {
                case SubscriptionRequestKind.Subscribe:
                    HandleSubscribe(req.ClientId, req.Symbol!, req.Flags);
                    break;
                case SubscriptionRequestKind.Get:
                    HandleGet(req.ClientId, req.Symbol!, req.Flags);
                    break;
                case SubscriptionRequestKind.Unsubscribe:
                    HandleUnsubscribe(req.ClientId, req.SecurityId);
                    break;
                case SubscriptionRequestKind.UnsubscribeAll:
                    HandleUnsubscribeAll(req.ClientId);
                    break;
            }
        }

        // Throttled: resync dirty clients and detect slow clients periodically
        if (++_resyncThrottle >= ResyncThrottleInterval)
        {
            _resyncThrottle = 0;
            ProcessDirtyClients();
            DetectSlowClients();
        }

        // Time-based: push rankings to all clients every ~2s
        if (_clients.Count > 0 && _rankingsTimer.ElapsedMilliseconds >= RankingsIntervalMs)
        {
            _rankingsTimer.Restart();
            PushRankings();
        }
    }

    /// <summary>
    /// Mark a client's subscription as needing MBO resync due to dropped events.
    /// Called on the feed thread when enqueue fails.
    /// </summary>
    private void MarkDirty(string clientId, ulong securityId)
    {
        if (!_dirtyClients.TryGetValue(clientId, out var set))
        {
            set = new HashSet<ulong>();
            _dirtyClients[clientId] = set;
        }
        set.Add(securityId);
    }

    /// <summary>
    /// Sends fresh MBO snapshots to clients with dropped events.
    /// Runs on the feed thread — book state is stable during this call.
    /// </summary>
    private void ProcessDirtyClients()
    {
        if (_dirtyClients.Count == 0) return;

        foreach (var (clientId, securityIds) in _dirtyClients)
        {
            if (!_clients.TryGetValue(clientId, out var session)) continue;

            foreach (var securityId in securityIds)
            {
                if (_bookManager is not null && _bookManager.Books.TryGetValue(securityId, out var book))
                {
                    SendMboSnapshot(session, book);
                    Interlocked.Increment(ref _resyncCount);
                    var sym = _symbolRegistry is not null && _symbolRegistry.TryGetSymbol(securityId, out var s) ? s : securityId.ToString();
                    _logger.LogWarning("Resync {Symbol} for {ClientId} due to dropped events", sym, clientId);
                }
            }
        }
        _dirtyClients.Clear();
    }

    /// <summary>
    /// Checks each client's outbound queue depth. Clients that stay above 75% capacity
    /// for too many consecutive ticks are disconnected to prevent data buildup.
    /// </summary>
    private void DetectSlowClients()
    {
        List<string>? toDisconnect = null;

        foreach (var (clientId, session) in _clients)
        {
            int capacity = session.ChannelCapacity;
            int depth = session.QueueDepth;

            if (depth > capacity * SlowClientThreshold)
            {
                _slowClientTicks.TryGetValue(clientId, out int ticks);
                _slowClientTicks[clientId] = ticks + 1;

                if (ticks + 1 >= SlowClientMaxTicks)
                {
                    toDisconnect ??= new();
                    toDisconnect.Add(clientId);
                }
            }
            else
            {
                _slowClientTicks.Remove(clientId);
            }
        }

        if (toDisconnect is null) return;

        foreach (var clientId in toDisconnect)
        {
            _logger.LogWarning("Disconnecting slow client {ClientId} (queue full for {Ticks} ticks, dropped: {Dropped})",
                clientId, SlowClientMaxTicks, _clients.TryGetValue(clientId, out var s) ? s.DroppedMessages : 0);
            if (_clients.TryGetValue(clientId, out var session))
                session.Cancel();
            _slowClientTicks.Remove(clientId);
            Interlocked.Increment(ref _slowClientDisconnects);
            AppMetrics.WsSlowDisconnects.Add(1);
        }
    }

    private void HandleSubscribe(string clientId, string symbol, DataFlags flags)
    {
        if (!TryValidateAndResolve(clientId, symbol, out var session, out var securityId))
            return;

        // Send SubscribeOk with the active flags
        var okBuf = new byte[WireProtocol.FramingHeaderSize + 8 + 1 + 1 + System.Text.Encoding.UTF8.GetMaxByteCount(symbol.Length)];
        int okLen = WireProtocol.WriteSubscribeOk(okBuf, securityId, flags, symbol);
        session.TryEnqueue(new ReadOnlyMemory<byte>(okBuf, 0, okLen));

        // Send snapshots BEFORE activating incremental forwarding.
        // Since we're on the feed thread, the book is stable — no concurrent mutations.
        SendSnapshots(session, securityId, flags);

        // NOW activate subscription — incrementals start flowing after this point
        session.AddSubscription(securityId);
        if (!_subscriptions.TryGetValue(securityId, out var clients))
        {
            clients = new Dictionary<string, DataFlags>();
            _subscriptions[securityId] = clients;
        }
        clients[clientId] = flags;
        AppMetrics.WsSubscriptions.Add(1);
    }

    private void HandleGet(string clientId, string symbol, DataFlags flags)
    {
        if (!TryValidateAndResolve(clientId, symbol, out var session, out var securityId))
            return;

        // Send snapshots only — no subscription activation
        SendSnapshots(session, securityId, flags);
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

    private void SendSnapshots(ClientSession session, ulong securityId, DataFlags flags)
    {
        if (flags.HasFlag(DataFlags.Book) && _bookManager is not null)
        {
            if (_bookManager.Books.TryGetValue(securityId, out var book))
                SendMboSnapshot(session, book);
            else
            {
                // Book not yet created — send empty reset so client initializes an empty book
                var emptyBuf = new byte[WireProtocol.BookSnapshotSize(0, 0)];
                WireProtocol.WriteBookSnapshotHeader(emptyBuf, securityId, 0, 0, 0);
                session.TryEnqueue(new ReadOnlyMemory<byte>(emptyBuf));
            }

            // Send recent trade history
            if (_recentTrades.TryGetValue(securityId, out var trades))
                SendTradeHistory(session, securityId, trades);
        }

        if (flags.HasFlag(DataFlags.Info) && _marketDataManager is not null && _marketDataManager.InstrumentData.TryGetValue(securityId, out var info))
            SendInfoSnapshot(session, securityId, info);
    }

    private void HandleUnsubscribe(string clientId, ulong securityId)
    {
        if (!_clients.TryGetValue(clientId, out var session)) return;

        session.RemoveSubscription(securityId);
        if (_subscriptions.TryGetValue(securityId, out var clients))
        {
            clients.Remove(clientId);
            if (clients.Count == 0)
                _subscriptions.Remove(securityId);
        }

        var buf = new byte[12];
        int len = WireProtocol.WriteUnsubscribed(buf, securityId);
        session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len));
    }

    private void HandleUnsubscribeAll(string clientId)
    {
        foreach (var (_, clients) in _subscriptions)
        {
            clients.Remove(clientId);
        }
        var empty = _subscriptions.Where(kv => kv.Value.Count == 0).Select(kv => kv.Key).ToList();
        foreach (var key in empty) _subscriptions.Remove(key);
    }

    // --- Snapshot serialization (runs on feed thread — book is stable) ---

    /// <summary>
    /// Sends MBO (Market-by-Order) snapshot: a reset marker (empty BookSnapshot)
    /// followed by individual OrderAdded events for every order in the book.
    /// This gives the client full MBO state for correct incremental tracking.
    /// </summary>
    private static void SendMboSnapshot(ClientSession session, OrderBook book)
    {
        // 1. Reset marker — tells client to clear local book state
        var resetBuf = new byte[WireProtocol.BookSnapshotSize(0, 0)];
        WireProtocol.WriteBookSnapshotHeader(resetBuf, book.SecurityId, book.LastRptSeq, 0, 0);
        session.TryEnqueue(new ReadOnlyMemory<byte>(resetBuf));

        // 2. Send every individual order as OrderAdded
        SendSideOrders(session, book.SecurityId, book.Bids);
        SendSideOrders(session, book.SecurityId, book.Asks);
    }

    private static void SendSideOrders(ClientSession session, ulong securityId, BookSide side)
    {
        // Snapshot to avoid "collection modified during enumeration" —
        // ProcessPendingRequests can trigger this mid-HandleDeleteOrder.
        foreach (var entry in side.Orders.Values.ToArray())
        {
            var buf = new byte[37];
            int len = WireProtocol.WriteOrderEvent(buf, MessageType.OrderAdded, securityId,
                entry.OrderId, (byte)entry.Side, entry.Price, entry.Quantity);
            session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len));
        }
    }

    private static void SendInfoSnapshot(ClientSession session, ulong securityId, InstrumentInfo info)
    {
        var buf = new byte[WireProtocol.InfoSnapshotMaxSize];
        int len = WireProtocol.WriteInfoSnapshot(buf, securityId, info);
        session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len));
    }

    // --- IBookEventHandler (called on feed thread) ---

    public void OnOrderAdded(OrderBook book, OrderBookEntry entry)
    {
        ProcessPendingRequests();
        ForwardOrderEvent(MessageType.OrderAdded, book.SecurityId, entry);
    }

    public void OnOrderUpdated(OrderBook book, OrderBookEntry entry)
    {
        ProcessPendingRequests();
        ForwardOrderEvent(MessageType.OrderUpdated, book.SecurityId, entry);
    }

    public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side)
    {
        ProcessPendingRequests();
        if (!_subscriptions.ContainsKey(book.SecurityId)) return;
        SendOrderToSubscribers(MessageType.OrderDeleted, book.SecurityId, orderId, (byte)side, 0, 0);
    }

    public void OnTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        ProcessPendingRequests();
        ForwardTradeEvent(securityId, price, quantity, tradeId);
    }

    public void OnBookCleared(ulong securityId, BookClearSide side)
    {
        ProcessPendingRequests();
        if (!_subscriptions.ContainsKey(securityId)) return;

        SendBookClearedToSubscribers(securityId, (byte)side);
    }

    public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        ProcessPendingRequests();
        ForwardTradeEvent(securityId, price, quantity, tradeId);
    }

    public void OnTradeBust(ulong securityId, long price, long quantity, long tradeId)
    {
        ProcessPendingRequests();
    }

    public void OnExecutionSummary(ulong securityId, long lastPx, long fillQty)
    {
        ProcessPendingRequests();
    }

    // --- IMarketDataEventHandler (called on feed thread) ---

    public void OnSecurityStatusChanged(ulong securityId, InstrumentInfo info)
    {
        ProcessPendingRequests();
        SendInfoUpdate(securityId, info);
    }

    public void OnMarketDataUpdated(ulong securityId, InstrumentInfo info)
    {
        ProcessPendingRequests();
        SendInfoUpdate(securityId, info);
    }

    // --- Helpers ---

    private void PushRankings()
    {
        if (_marketDataManager is null || _symbolRegistry is null) return;

        var data = _marketDataManager.InstrumentData;
        if (data.Count == 0) return;

        // Compute top N for each category
        var volumeList = new List<RankingEntry>();
        var gainerList = new List<RankingEntry>();
        var loserList = new List<RankingEntry>();

        foreach (var (secId, info) in data)
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

        // Serialize
        var buf = new byte[WireProtocol.RankingsUpdateMaxSize];
        int len = WireProtocol.WriteRankingsUpdate(buf, volume, gainers, losers);
        var payload = new ReadOnlyMemory<byte>(buf, 0, len);

        // Broadcast to all connected clients
        foreach (var (_, client) in _clients)
        {
            client.TryEnqueue(payload);
        }
    }

    private void ForwardOrderEvent(MessageType type, ulong securityId, OrderBookEntry entry)
    {
        if (!_subscriptions.ContainsKey(securityId)) return;
        SendOrderToSubscribers(type, securityId, entry.OrderId, (byte)entry.Side, entry.Price, entry.Quantity);
    }

    private void ForwardTradeEvent(ulong securityId, long price, long quantity, long tradeId)
    {
        // Always store — even if no subscribers, so late subscribers get history
        if (!_recentTrades.TryGetValue(securityId, out var ring))
        {
            ring = new TradeRingBuffer(MaxRecentTrades);
            _recentTrades[securityId] = ring;
        }
        ring.Add(price, quantity, tradeId);

        if (!_subscriptions.ContainsKey(securityId)) return;

        var buf = new byte[36];
        int len = WireProtocol.WriteTrade(buf, securityId, price, quantity, tradeId);
        SendToSubscribers(securityId, new ReadOnlyMemory<byte>(buf, 0, len), DataFlags.Book);
    }

    private void SendToSubscribers(ulong securityId, ReadOnlyMemory<byte> message, DataFlags requiredFlag)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return;
        foreach (var (clientId, flags) in clients)
        {
            if (!flags.HasFlag(requiredFlag)) continue;
            if (_clients.TryGetValue(clientId, out var session))
            {
                if (session.TryEnqueue(message))
                    AppMetrics.WsMessagesSent.Add(1);
                else
                {
                    AppMetrics.WsMessagesDropped.Add(1);
                    if (requiredFlag == DataFlags.Book)
                        MarkDirty(clientId, securityId);
                }
            }
        }
    }

    /// <summary>Send a typed order event to all subscribers for conflation in the write loop.</summary>
    private void SendOrderToSubscribers(MessageType type, ulong securityId, ulong orderId, byte side, long price, long qty)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return;
        foreach (var (clientId, flags) in clients)
        {
            if (!flags.HasFlag(DataFlags.Book)) continue;
            if (_clients.TryGetValue(clientId, out var session))
            {
                if (session.TryEnqueueOrder(type, securityId, orderId, side, price, qty))
                    AppMetrics.WsMessagesSent.Add(1);
                else
                {
                    AppMetrics.WsMessagesDropped.Add(1);
                    MarkDirty(clientId, securityId);
                }
            }
        }
    }

    /// <summary>Send a typed info update to all subscribers for last-state-wins conflation.</summary>
    private void SendInfoToSubscribers(ulong securityId, ReadOnlyMemory<byte> serializedInfo)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return;
        foreach (var (clientId, flags) in clients)
        {
            if (!flags.HasFlag(DataFlags.Info)) continue;
            if (_clients.TryGetValue(clientId, out var session))
            {
                if (session.TryEnqueueInfo(securityId, serializedInfo))
                    AppMetrics.WsMessagesSent.Add(1);
                else
                    AppMetrics.WsMessagesDropped.Add(1);
            }
        }
    }

    /// <summary>Send a typed BookCleared event for correct conflation ordering.</summary>
    private void SendBookClearedToSubscribers(ulong securityId, byte clearSide)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return;
        foreach (var (clientId, flags) in clients)
        {
            if (!flags.HasFlag(DataFlags.Book)) continue;
            if (_clients.TryGetValue(clientId, out var session))
            {
                if (session.TryEnqueueBookCleared(securityId, clearSide))
                    AppMetrics.WsMessagesSent.Add(1);
                else
                {
                    AppMetrics.WsMessagesDropped.Add(1);
                    MarkDirty(clientId, securityId);
                }
            }
        }
    }

    private void SendInfoUpdate(ulong securityId, InstrumentInfo info)
    {
        if (!_subscriptions.ContainsKey(securityId)) return;

        var buf = new byte[WireProtocol.InfoSnapshotMaxSize];
        int len = WireProtocol.WriteInfoSnapshot(buf, securityId, info);
        SendInfoToSubscribers(securityId, new ReadOnlyMemory<byte>(buf, 0, len));
    }

    /// <summary>Called when feed enters RealTime state. Enables subscriptions.</summary>
    public void SetReady() => _ready = true;

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
        private int _head; // next write position
        private int _count;

        public TradeRingBuffer(int capacity) => _buf = new (long, long, long)[capacity];

        public void Add(long price, long qty, long tradeId)
        {
            _buf[_head] = (price, qty, tradeId);
            _head = (_head + 1) % _buf.Length;
            if (_count < _buf.Length) _count++;
        }

        /// <summary>Iterates oldest → newest.</summary>
        public IEnumerable<(long Price, long Qty, long TradeId)> AsSpan()
        {
            int start = _count < _buf.Length ? 0 : _head;
            for (int i = 0; i < _count; i++)
                yield return _buf[(start + i) % _buf.Length];
        }
    }
}
