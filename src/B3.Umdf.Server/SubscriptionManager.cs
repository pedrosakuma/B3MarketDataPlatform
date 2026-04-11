using System.Collections.Concurrent;
using B3.Umdf.Book;

namespace B3.Umdf.Server;

public sealed class SubscriptionManager : IBookEventHandler, IMarketDataEventHandler, IDisposable
{
    private BookManager? _bookManager;
    private MarketDataManager? _marketDataManager;
    private SymbolRegistry? _symbolRegistry;

    private readonly ConcurrentDictionary<string, ClientSession> _clients = new();
    // Per-security: set of client IDs — only accessed on feed thread
    private readonly Dictionary<ulong, HashSet<string>> _subscriptions = new();
    // Pending subscription requests from WebSocket threads
    private readonly ConcurrentQueue<SubscriptionRequest> _pendingRequests = new();
    // Pending snapshot requests for the snapshot thread
    private readonly ConcurrentQueue<SnapshotRequest> _snapshotQueue = new();

    private readonly Thread _snapshotThread;
    private readonly CancellationTokenSource _cts = new();
    private volatile bool _ready;

    public bool IsReady => _ready;

    public SubscriptionManager()
    {
        _snapshotThread = new Thread(SnapshotThreadLoop) { IsBackground = true, Name = "SnapshotWorker" };
    }

    public void SetDataSources(BookManager bookManager, MarketDataManager marketDataManager, SymbolRegistry symbolRegistry)
    {
        _bookManager = bookManager;
        _marketDataManager = marketDataManager;
        _symbolRegistry = symbolRegistry;
    }

    public void Start()
    {
        _snapshotThread.Start();
    }

    /// <summary>Register a client session.</summary>
    public void RegisterClient(ClientSession session)
    {
        _clients[session.Id] = session;
    }

    /// <summary>Unregister a client and remove all its subscriptions.</summary>
    public void UnregisterClient(string clientId)
    {
        _clients.TryRemove(clientId, out _);
        // Schedule cleanup of subscriptions on feed thread
        _pendingRequests.Enqueue(SubscriptionRequest.UnsubscribeAll(clientId));
    }

    /// <summary>Called from WebSocket read thread to request a subscription.</summary>
    public void RequestSubscribe(string clientId, string symbol)
    {
        _pendingRequests.Enqueue(SubscriptionRequest.Subscribe(clientId, symbol));
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
                    HandleSubscribe(req.ClientId, req.Symbol!);
                    break;
                case SubscriptionRequestKind.Unsubscribe:
                    HandleUnsubscribe(req.ClientId, req.SecurityId);
                    break;
                case SubscriptionRequestKind.UnsubscribeAll:
                    HandleUnsubscribeAll(req.ClientId);
                    break;
            }
        }
    }

    private void HandleSubscribe(string clientId, string symbol)
    {
        if (!_clients.TryGetValue(clientId, out var session)) return;
        if (_symbolRegistry is null) return;

        if (!_ready)
        {
            var buf = new byte[64];
            int len = WireProtocol.WriteSubscribeError(buf, SubscribeErrorCode.NotReady, symbol);
            session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len));
            return;
        }

        if (!_symbolRegistry.TryResolve(symbol, out var securityId))
        {
            var buf = new byte[64];
            int len = WireProtocol.WriteSubscribeError(buf, SubscribeErrorCode.UnknownSymbol, symbol);
            session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len));
            return;
        }

        // Activate subscription
        session.AddSubscription(securityId);
        if (!_subscriptions.TryGetValue(securityId, out var clients))
        {
            clients = new HashSet<string>();
            _subscriptions[securityId] = clients;
        }
        clients.Add(clientId);

        // Send SubscribeOk
        var okBuf = new byte[64];
        int okLen = WireProtocol.WriteSubscribeOk(okBuf, securityId, symbol);
        session.TryEnqueue(new ReadOnlyMemory<byte>(okBuf, 0, okLen));

        // Queue snapshot for the snapshot thread
        _snapshotQueue.Enqueue(new SnapshotRequest(clientId, securityId));
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
        // Clean up empty sets
        var empty = _subscriptions.Where(kv => kv.Value.Count == 0).Select(kv => kv.Key).ToList();
        foreach (var key in empty) _subscriptions.Remove(key);
    }

    // --- Snapshot thread ---

    private void SnapshotThreadLoop()
    {
        var spinWait = new SpinWait();
        while (!_cts.IsCancellationRequested)
        {
            if (_snapshotQueue.TryDequeue(out var req))
            {
                ProcessSnapshot(req);
                spinWait.Reset();
            }
            else
            {
                spinWait.SpinOnce();
                if (spinWait.NextSpinWillYield)
                    Thread.Sleep(1);
            }
        }
    }

    private void ProcessSnapshot(SnapshotRequest req)
    {
        if (!_clients.TryGetValue(req.ClientId, out var session)) return;
        if (_bookManager is null || _marketDataManager is null) return;

        // Read book via seqlock
        if (_bookManager.Books.TryGetValue(req.SecurityId, out var book))
        {
            TrySendBookSnapshot(session, book);
        }

        // Read instrument info (no seqlock needed — simple fields)
        if (_marketDataManager.InstrumentData.TryGetValue(req.SecurityId, out var info))
        {
            SendInfoSnapshot(session, req.SecurityId, info);
        }
    }

    private void TrySendBookSnapshot(ClientSession session, OrderBook book)
    {
        const int maxRetries = 10;
        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            int v1 = book.Version;
            if ((v1 & 1) != 0)
            {
                // Mutation in progress, spin
                Thread.SpinWait(10);
                continue;
            }

            // Try to copy price levels
            try
            {
                var bidLevels = new List<(long price, long totalQty, int count)>();
                var askLevels = new List<(long price, long totalQty, int count)>();

                foreach (var kv in book.Bids.PriceLevels)
                    bidLevels.Add((kv.Key, kv.Value.Sum(o => o.Quantity), kv.Value.Count));

                foreach (var kv in book.Asks.PriceLevels)
                    askLevels.Add((kv.Key, kv.Value.Sum(o => o.Quantity), kv.Value.Count));

                uint rptSeq = book.LastRptSeq;

                Thread.MemoryBarrier();
                int v2 = book.Version;
                if (v1 != v2) continue; // Version changed, retry

                // Serialize
                int size = WireProtocol.BookSnapshotSize(bidLevels.Count, askLevels.Count);
                var buf = new byte[size];
                int offset = WireProtocol.WriteBookSnapshotHeader(buf, book.SecurityId, rptSeq,
                    (ushort)bidLevels.Count, (ushort)askLevels.Count);

                foreach (var (price, qty, count) in bidLevels)
                    offset = WireProtocol.WritePriceLevel(buf, offset, price, qty, (ushort)count);
                foreach (var (price, qty, count) in askLevels)
                    offset = WireProtocol.WritePriceLevel(buf, offset, price, qty, (ushort)count);

                session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, size));
                return;
            }
            catch (InvalidOperationException)
            {
                // Collection modified during enumeration — retry
                continue;
            }
        }
        // Failed after max retries — skip snapshot (client will get incrementals)
    }

    private void SendInfoSnapshot(ClientSession session, ulong securityId, InstrumentInfo info)
    {
        void Send(byte fieldId, long? value)
        {
            if (value is null) return;
            var buf = new byte[21];
            int len = WireProtocol.WriteMarketData(buf, securityId, fieldId, value.Value);
            session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len));
        }

        Send(WireProtocol.FieldOpeningPrice, info.OpeningPrice);
        Send(WireProtocol.FieldClosingPrice, info.ClosingPrice);
        Send(WireProtocol.FieldHighPrice, info.HighPrice);
        Send(WireProtocol.FieldLowPrice, info.LowPrice);
        Send(WireProtocol.FieldLastTradePrice, info.LastTradePrice);
        Send(WireProtocol.FieldLastTradeSize, info.LastTradeSize);
        Send(WireProtocol.FieldSettlementPrice, info.SettlementPrice);
        Send(WireProtocol.FieldTheoreticalOpeningPrice, info.TheoreticalOpeningPrice);
        Send(WireProtocol.FieldTradeVolume, info.TradeVolume);
        Send(WireProtocol.FieldVwapPrice, info.VwapPrice);
        Send(WireProtocol.FieldNetChange, info.NetChangeFromPrevDay);
        Send(WireProtocol.FieldNumberOfTrades, info.NumberOfTrades);
        Send(WireProtocol.FieldOpenInterest, info.OpenInterest);
        Send(WireProtocol.FieldPriceBandLow, info.PriceBandLow);
        Send(WireProtocol.FieldPriceBandHigh, info.PriceBandHigh);
        Send(WireProtocol.FieldTradingReferencePrice, info.TradingReferencePrice);

        if (info.TradingStatus is not null)
        {
            var statusBuf = new byte[20];
            int len = WireProtocol.WriteSecurityStatus(statusBuf, securityId,
                info.TradingStatus.Value, info.TradingEvent ?? 0);
            session.TryEnqueue(new ReadOnlyMemory<byte>(statusBuf, 0, len));
        }
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

        var buf = new byte[21];
        int len = WireProtocol.WriteOrderDeleted(buf, book.SecurityId, orderId, (byte)side);
        var msg = new ReadOnlyMemory<byte>(buf, 0, len);
        SendToSubscribers(book.SecurityId, msg);
    }

    public void OnTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        ProcessPendingRequests();
        if (!_subscriptions.ContainsKey(securityId)) return;

        var buf = new byte[36];
        int len = WireProtocol.WriteTrade(buf, securityId, price, quantity, tradeId);
        var msg = new ReadOnlyMemory<byte>(buf, 0, len);
        SendToSubscribers(securityId, msg);
    }

    public void OnBookCleared(ulong securityId)
    {
        ProcessPendingRequests();
        if (!_subscriptions.ContainsKey(securityId)) return;

        var buf = new byte[12];
        int len = WireProtocol.WriteBookCleared(buf, securityId);
        var msg = new ReadOnlyMemory<byte>(buf, 0, len);
        SendToSubscribers(securityId, msg);
    }

    public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        if (!_subscriptions.ContainsKey(securityId)) return;
        var buf = new byte[36];
        int len = WireProtocol.WriteTrade(buf, securityId, price, quantity, tradeId);
        SendToSubscribers(securityId, new ReadOnlyMemory<byte>(buf, 0, len));
    }

    public void OnTradeBust(ulong securityId, long price, long quantity, long tradeId)
    {
        // Reuse Trade message type — client can distinguish by context if needed
    }

    public void OnExecutionSummary(ulong securityId, long lastPx, long fillQty)
    {
        // Execution summaries not forwarded to WS clients for now
    }

    // --- IMarketDataEventHandler (called on feed thread) ---

    public void OnSecurityStatusChanged(ulong securityId, InstrumentInfo info)
    {
        ProcessPendingRequests();
        if (!_subscriptions.ContainsKey(securityId)) return;

        var buf = new byte[20];
        int len = WireProtocol.WriteSecurityStatus(buf, securityId,
            info.TradingStatus ?? 0, info.TradingEvent ?? 0);
        SendToSubscribers(securityId, new ReadOnlyMemory<byte>(buf, 0, len));
    }

    public void OnMarketDataUpdated(ulong securityId, InstrumentInfo info)
    {
        ProcessPendingRequests();
        if (!_subscriptions.ContainsKey(securityId)) return;

        SendInfoFields(securityId, info);
    }

    // --- Helpers ---

    private void ForwardOrderEvent(MessageType type, ulong securityId, OrderBookEntry entry)
    {
        if (!_subscriptions.ContainsKey(securityId)) return;

        var buf = new byte[37];
        int len = WireProtocol.WriteOrderEvent(buf, type, securityId,
            entry.OrderId, (byte)entry.Side, entry.Price, entry.Quantity);
        var msg = new ReadOnlyMemory<byte>(buf, 0, len);
        SendToSubscribers(securityId, msg);
    }

    private void SendToSubscribers(ulong securityId, ReadOnlyMemory<byte> message)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clientIds)) return;
        foreach (var clientId in clientIds)
        {
            if (_clients.TryGetValue(clientId, out var session))
                session.TryEnqueue(message);
        }
    }

    private void SendInfoFields(ulong securityId, InstrumentInfo info)
    {
        if (info.LastTradePrice is { } ltp)
        {
            var buf = new byte[21];
            int len = WireProtocol.WriteMarketData(buf, securityId, WireProtocol.FieldLastTradePrice, ltp);
            SendToSubscribers(securityId, new ReadOnlyMemory<byte>(buf, 0, len));
        }
        if (info.HighPrice is { } hp)
        {
            var buf = new byte[21];
            int len = WireProtocol.WriteMarketData(buf, securityId, WireProtocol.FieldHighPrice, hp);
            SendToSubscribers(securityId, new ReadOnlyMemory<byte>(buf, 0, len));
        }
        if (info.LowPrice is { } lp)
        {
            var buf = new byte[21];
            int len = WireProtocol.WriteMarketData(buf, securityId, WireProtocol.FieldLowPrice, lp);
            SendToSubscribers(securityId, new ReadOnlyMemory<byte>(buf, 0, len));
        }
        if (info.TradeVolume is { } tv)
        {
            var buf = new byte[21];
            int len = WireProtocol.WriteMarketData(buf, securityId, WireProtocol.FieldTradeVolume, tv);
            SendToSubscribers(securityId, new ReadOnlyMemory<byte>(buf, 0, len));
        }
    }

    /// <summary>Called when feed enters RealTime state. Enables subscriptions.</summary>
    public void SetReady() => _ready = true;

    public void Dispose()
    {
        _cts.Cancel();
        if (_snapshotThread.IsAlive)
            _snapshotThread.Join(TimeSpan.FromSeconds(2));
        _cts.Dispose();
    }

    // --- Internal types ---

    private enum SubscriptionRequestKind : byte
    {
        Subscribe,
        Unsubscribe,
        UnsubscribeAll,
    }

    private readonly struct SubscriptionRequest
    {
        public SubscriptionRequestKind Kind { get; }
        public string ClientId { get; }
        public string? Symbol { get; }
        public ulong SecurityId { get; }

        private SubscriptionRequest(SubscriptionRequestKind kind, string clientId, string? symbol, ulong securityId)
        {
            Kind = kind;
            ClientId = clientId;
            Symbol = symbol;
            SecurityId = securityId;
        }

        public static SubscriptionRequest Subscribe(string clientId, string symbol)
            => new(SubscriptionRequestKind.Subscribe, clientId, symbol, 0);

        public static SubscriptionRequest Unsubscribe(string clientId, ulong securityId)
            => new(SubscriptionRequestKind.Unsubscribe, clientId, null, securityId);

        public static SubscriptionRequest UnsubscribeAll(string clientId)
            => new(SubscriptionRequestKind.UnsubscribeAll, clientId, null, 0);
    }

    private readonly record struct SnapshotRequest(string ClientId, ulong SecurityId);
}
