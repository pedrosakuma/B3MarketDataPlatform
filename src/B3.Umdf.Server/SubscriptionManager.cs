using System.Collections.Concurrent;
using B3.Umdf.Book;

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

    private readonly ConcurrentDictionary<string, ClientSession> _clients = new();
    // Per-security: set of client IDs — only accessed on feed thread
    private readonly Dictionary<ulong, HashSet<string>> _subscriptions = new();
    // Pending subscription requests from WebSocket threads
    private readonly ConcurrentQueue<SubscriptionRequest> _pendingRequests = new();

    private volatile bool _ready;

    public bool IsReady => _ready;

    public void SetDataSources(BookManager bookManager, MarketDataManager marketDataManager, SymbolRegistry symbolRegistry)
    {
        _bookManager = bookManager;
        _marketDataManager = marketDataManager;
        _symbolRegistry = symbolRegistry;
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

        // Send SubscribeOk
        var okBuf = new byte[64];
        int okLen = WireProtocol.WriteSubscribeOk(okBuf, securityId, symbol);
        session.TryEnqueue(new ReadOnlyMemory<byte>(okBuf, 0, okLen));

        // Send snapshot BEFORE activating incremental forwarding.
        // Since we're on the feed thread, the book is stable — no concurrent mutations.
        if (_bookManager is not null && _bookManager.Books.TryGetValue(securityId, out var book))
            SendBookSnapshot(session, book);

        if (_marketDataManager is not null && _marketDataManager.InstrumentData.TryGetValue(securityId, out var info))
            SendInfoSnapshot(session, securityId, info);

        // NOW activate subscription — incrementals start flowing after this point
        session.AddSubscription(securityId);
        if (!_subscriptions.TryGetValue(securityId, out var clients))
        {
            clients = new HashSet<string>();
            _subscriptions[securityId] = clients;
        }
        clients.Add(clientId);
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

    private static void SendBookSnapshot(ClientSession session, OrderBook book)
    {
        var bidLevels = new List<(long price, long totalQty, int count)>();
        var askLevels = new List<(long price, long totalQty, int count)>();

        foreach (var kv in book.Bids.PriceLevels)
            bidLevels.Add((kv.Key, kv.Value.Sum(o => o.Quantity), kv.Value.Count));

        foreach (var kv in book.Asks.PriceLevels)
            askLevels.Add((kv.Key, kv.Value.Sum(o => o.Quantity), kv.Value.Count));

        int size = WireProtocol.BookSnapshotSize(bidLevels.Count, askLevels.Count);
        var buf = new byte[size];
        int offset = WireProtocol.WriteBookSnapshotHeader(buf, book.SecurityId, book.LastRptSeq,
            (ushort)bidLevels.Count, (ushort)askLevels.Count);

        foreach (var (price, qty, count) in bidLevels)
            offset = WireProtocol.WritePriceLevel(buf, offset, price, qty, (ushort)count);
        foreach (var (price, qty, count) in askLevels)
            offset = WireProtocol.WritePriceLevel(buf, offset, price, qty, (ushort)count);

        session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, size));
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

        var buf = new byte[21];
        int len = WireProtocol.WriteOrderDeleted(buf, book.SecurityId, orderId, (byte)side);
        SendToSubscribers(book.SecurityId, new ReadOnlyMemory<byte>(buf, 0, len));
    }

    public void OnTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        ProcessPendingRequests();
        if (!_subscriptions.ContainsKey(securityId)) return;

        var buf = new byte[36];
        int len = WireProtocol.WriteTrade(buf, securityId, price, quantity, tradeId);
        SendToSubscribers(securityId, new ReadOnlyMemory<byte>(buf, 0, len));
    }

    public void OnBookCleared(ulong securityId)
    {
        ProcessPendingRequests();
        if (!_subscriptions.ContainsKey(securityId)) return;

        var buf = new byte[12];
        int len = WireProtocol.WriteBookCleared(buf, securityId);
        SendToSubscribers(securityId, new ReadOnlyMemory<byte>(buf, 0, len));
    }

    public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        ProcessPendingRequests();
        if (!_subscriptions.ContainsKey(securityId)) return;

        var buf = new byte[36];
        int len = WireProtocol.WriteTrade(buf, securityId, price, quantity, tradeId);
        SendToSubscribers(securityId, new ReadOnlyMemory<byte>(buf, 0, len));
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

    private void ForwardOrderEvent(MessageType type, ulong securityId, OrderBookEntry entry)
    {
        if (!_subscriptions.ContainsKey(securityId)) return;

        var buf = new byte[37];
        int len = WireProtocol.WriteOrderEvent(buf, type, securityId,
            entry.OrderId, (byte)entry.Side, entry.Price, entry.Quantity);
        SendToSubscribers(securityId, new ReadOnlyMemory<byte>(buf, 0, len));
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

    private void SendInfoUpdate(ulong securityId, InstrumentInfo info)
    {
        if (!_subscriptions.ContainsKey(securityId)) return;

        var buf = new byte[WireProtocol.InfoSnapshotMaxSize];
        int len = WireProtocol.WriteInfoSnapshot(buf, securityId, info);
        SendToSubscribers(securityId, new ReadOnlyMemory<byte>(buf, 0, len));
    }

    /// <summary>Called when feed enters RealTime state. Enables subscriptions.</summary>
    public void SetReady() => _ready = true;

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
}
