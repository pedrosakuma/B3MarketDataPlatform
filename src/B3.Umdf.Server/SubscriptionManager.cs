using System.Collections.Concurrent;
using B3.Umdf.Book;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server;

/// <summary>
/// Central subscription registry. Manages client connections, symbol resolution,
/// snapshot delivery, and rankings broadcast.
///
/// Per-group <see cref="GroupConflationHandler"/> instances handle order/trade buffering
/// and upstream conflation on their own group thread (single-threaded, no locks).
/// Subscription state uses <see cref="ConcurrentDictionary{TKey,TValue}"/> with
/// copy-on-write inner dictionaries for lock-free reads on the hot path.
/// A lightweight <see cref="_subLock"/> serialises rare subscription mutations.
/// </summary>
public sealed class SubscriptionManager : IDisposable
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

    private volatile GroupConflationHandler[]? _groupHandlers;

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
    private readonly int _maxSnapshotRequestsPerBatch;

    private readonly long _clientMaxPendingBytes;
    private readonly OutlierSweeper _outlierSweeper;

    public SubscriptionManager(
        ILogger<SubscriptionManager>? logger = null,
        int maxSnapshotRequestsPerBatch = 32,
        long clientMaxPendingBytes = 0,
        double outlierMultiplier = 4.0,
        long outlierMinBytes = 256L * 1024,
        double outlierPressurePct = 0.50,
        int outlierIntervalMs = 1000)
    {
        _logger = logger ?? NullLogger<SubscriptionManager>.Instance;
        _maxSnapshotRequestsPerBatch = maxSnapshotRequestsPerBatch;
        _clientMaxPendingBytes = clientMaxPendingBytes;
        _outlierSweeper = new OutlierSweeper(
            _clients,
            clientMaxPendingBytes,
            outlierMultiplier,
            outlierMinBytes,
            outlierPressurePct,
            outlierIntervalMs,
            _logger);
    }

    public bool IsReady => _ready;

    /// <summary>Current number of connected clients.</summary>
    public int ClientCount => _clients.Count;

    /// <summary>Get stats for all connected clients.</summary>
    public IEnumerable<(string Id, int QueueDepth, long PendingBytes, long MessagesSent, long BytesSent)> GetClientStats()
    {
        foreach (var (_, session) in _clients)
            yield return (session.Id, session.QueueDepth, session.PendingBytes, session.MessagesSent, session.BytesSent);
    }

    /// <summary>
    /// Creates a per-group event handler. Each handler owns its conflation buffers
    /// and trade ring buffers. Call <see cref="GroupConflationHandler.SetBookManager"/>
    /// after construction to bind the handler to its BookManager.
    /// </summary>
    public GroupConflationHandler CreateGroupHandler()
    {
        return new GroupConflationHandler(this, maxSnapshotRequestsPerBatch: _maxSnapshotRequestsPerBatch);
    }

    public void SetDataSources(
        BookManager[] bookManagers,
        MarketDataManager[] marketDataManagers,
        SymbolRegistry symbolRegistry,
        GroupConflationHandler[] groupHandlers)
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
        MetricsRegistry.WsConnectionsActive.Add(1);

        // Immediately tell the client whether the server is ready
        SnapshotEmitter.SendServerStatus(session, _ready);
    }

    /// <summary>Unregister a client and remove all its subscriptions.</summary>
    public void UnregisterClient(string clientId)
    {
        _clients.TryRemove(clientId, out _);
        _pendingUnsubscribes.Enqueue(SubscriptionRequest.UnsubscribeAll(clientId));
        MetricsRegistry.WsConnectionsActive.Add(-1);
    }

    /// <summary>Called from WebSocket read thread to request a subscription.</summary>
    public void RequestSubscribe(string clientId, string symbol, DataFlags flags)
    {
        var req = SubscriptionRequest.Subscribe(clientId, symbol, flags);
        if (!RouteToGroup(req))
            SendRouteFailureError(clientId, symbol);
    }

    /// <summary>Called from WebSocket read thread to request a one-shot snapshot.</summary>
    public void RequestGet(string clientId, string symbol, DataFlags flags)
    {
        var req = SubscriptionRequest.Get(clientId, symbol, flags);
        if (!RouteToGroup(req))
            SendRouteFailureError(clientId, symbol);
    }

    /// <summary>
    /// Common failure response for Subscribe/Get when routing returns false.
    /// Distinguishes NotReady (still loading instrument definitions) from
    /// UnknownSymbol (no group owns it) so the client can react accordingly.
    /// Safe to call from the WebSocket read thread because <see cref="ClientSession.TryEnqueue"/>
    /// is multi-writer.
    /// </summary>
    private void SendRouteFailureError(string clientId, string symbol)
    {
        if (!_clients.TryGetValue(clientId, out var session)) return;
        var code = _ready ? SubscribeErrorCode.UnknownSymbol : SubscribeErrorCode.NotReady;
        SendError(session, code, symbol);
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

    // --- Called by GroupConflationHandler from the owning group's thread ---

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
                session.TryEnqueue(payload);
        }
    }

    /// <summary>Lock-free accessor for the inner per-security subscriber dict (copy-on-write under
    /// _subLock). Used by hot-path coalesced broadcast in <see cref="GroupConflationHandler"/>
    /// to amortize the per-event Channel.TryWrite cost across an entire flush cycle.
    /// </summary>
    internal IReadOnlyDictionary<string, DataFlags>? GetSubscribers(ulong securityId) =>
        _subscriptions.TryGetValue(securityId, out var clients) ? clients : null;

    /// <summary>
    /// Lock-free quick check used by the dispatch thread to skip buffering wire bytes
    /// for securities that have no Book-flag subscriber. Returns true if at least one
    /// current subscriber has <see cref="DataFlags.Book"/> set.
    /// </summary>
    internal bool HasAnyBookSubscriber(ulong securityId)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return false;
        foreach (var (_, f) in clients)
            if (f.HasFlag(DataFlags.Book)) return true;
        return false;
    }

    /// <summary>
    /// Invoked from the dispatch thread when a broadcast batch had to be dropped
    /// (broadcaster ring full). Schedules a fresh snapshot (Get) request for every
    /// current Book-flag subscriber of <paramref name="securityId"/> so they can
    /// recover the state they missed. Returns true if at least one resync request
    /// was enqueued.
    /// </summary>
    internal bool RequestResyncForBookSubscribers(ulong securityId)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return false;
        if (_symbolRegistry is null) return false;
        if (!_symbolRegistry.TryGetSymbol(securityId, out var symbol)) return false;

        bool any = false;
        var group = GetOwningGroup(securityId);
        if (group is null) return false;
        foreach (var (clientId, flags) in clients)
        {
            if (!flags.HasFlag(DataFlags.Book)) continue;
            group.EnqueueRequest(clientId, symbol, DataFlags.Book, isGet: true);
            any = true;
        }
        return any;
    }

    /// <summary>
    /// Schedule a fresh book snapshot (Get) for every Book-flag subscriber whose security
    /// is owned by <paramref name="group"/>. Used when a feed group exits Recovery/CatchUp
    /// and resumes fanout: clients receive a clean snapshot to recover any state that
    /// was suppressed during the recovery window. Pacing is enforced by the per-batch
    /// snapshot budget in <see cref="GroupConflationHandler.OnBatchComplete"/>.
    /// </summary>
    internal void RequestResyncForAllSubscribersInGroup(GroupConflationHandler group)
    {
        // _subscriptions is a ConcurrentDictionary; enumeration is safe under concurrent mutation.
        foreach (var kv in _subscriptions)
        {
            if (GetOwningGroup(kv.Key) != group) continue;
            RequestResyncForBookSubscribers(kv.Key);
        }
    }

    private GroupConflationHandler? GetOwningGroup(ulong securityId)
    {
        var handlers = _groupHandlers;
        if (handlers is null) return null;
        foreach (var gh in handlers)
        {
            if (gh.BookManager is not null && gh.BookManager.Books.ContainsKey(securityId))
                return gh;
        }
        return null;
    }

    /// <summary>Lock-free lookup of a connected client session by id.</summary>
    internal ClientSession? GetClient(string clientId) =>
        _clients.TryGetValue(clientId, out var session) ? session : null;

    /// <summary>
    /// Wake Info subscribers for a security so their latest InstrumentInfo version is flushed
    /// even when there is no concurrent book/rankings traffic.
    /// </summary>
    internal void NotifyInfoUpdated(ulong securityId)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return;
        foreach (var (clientId, flags) in clients)
        {
            if (!flags.HasFlag(DataFlags.Info)) continue;
            if (_clients.TryGetValue(clientId, out var session))
                session.NotifyInfoAvailable();
        }
    }

    // --- Subscribe handling (called on owning group's thread) ---

    internal void HandleSubscribe(string clientId, string symbol, DataFlags flags,
        BookManager bookManager, GroupConflationHandler group)
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
        MetricsRegistry.WsSubscriptions.Add(1);
    }

    internal void HandleGet(string clientId, string symbol, DataFlags flags,
        BookManager bookManager, GroupConflationHandler group)
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
        => SnapshotEmitter.SendError(session, code, symbol);

    private void SendSnapshots(ClientSession session, ulong securityId, DataFlags flags,
        BookManager bookManager, GroupConflationHandler group)
    {
        if (flags.HasFlag(DataFlags.Book))
        {
            if (bookManager.Books.TryGetValue(securityId, out var book))
                SnapshotEmitter.SendMboSnapshot(session, book);
            else
                SnapshotEmitter.SendEmptyBookSnapshot(session, securityId);

            // Send recent trade history from the owning group's ring buffer
            if (group.RecentTrades.TryGetValue(securityId, out var trades))
                SnapshotEmitter.SendTradeHistory(session, securityId, trades);

            // Send candle history from the owning group's aggregator.
            // Always send a CandleSnapshot (even empty) so the frontend knows the snapshot phase is complete.
            if (group.Candles.TryGetValue(securityId, out var agg))
                SnapshotEmitter.SendCandleHistory(session, securityId, agg);
            else
                SnapshotEmitter.SendEmptyCandleSnapshot(session, securityId);
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
                        SnapshotEmitter.SendInfoSnapshot(session, securityId, info);
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
            if (newClients.Remove(clientId))
            {
                MetricsRegistry.WsSubscriptions.Add(-1);
                if (newClients.Count == 0)
                    _subscriptions.TryRemove(securityId, out _);
                else
                    _subscriptions[securityId] = newClients;
            }
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
        int removedSubscriptions = 0;

        foreach (var (secId, clients) in _subscriptions)
        {
            if (!clients.ContainsKey(clientId)) continue;
            removedSubscriptions++;

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
        {
            foreach (var key in toRemove)
                _subscriptions.TryRemove(key, out _);
        }

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

        if (removedSubscriptions > 0)
            MetricsRegistry.WsSubscriptions.Add(-removedSubscriptions);
    }

    // --- Snapshot serialization moved to SnapshotEmitter ---

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

        var volume = TopN(volumeList, RankingsTopN);
        var gainers = TopN(gainerList, RankingsTopN);
        var losers = TopN(loserList, RankingsTopN);

        var buf = new byte[WireProtocol.RankingsUpdateMaxSize];
        int len = WireProtocol.WriteRankingsUpdate(buf, volume, gainers, losers);
        var payload = new ReadOnlyMemory<byte>(buf, 0, len);

        foreach (var (_, client) in _clients)
            client.TryEnqueue(payload);
    }

    /// <summary>Take the first <paramref name="n"/> elements (caller has pre-sorted).</summary>
    private static RankingEntry[] TopN(List<RankingEntry> sorted, int n)
        => sorted.Count > n ? sorted.GetRange(0, n).ToArray() : sorted.ToArray();

    /// <summary>Called when feed enters RealTime state. Enables subscriptions and starts background rankings.</summary>
    public void SetReady()
    {
        if (_ready) return;

        _ready = true;
        BroadcastServerStatus(true);
        StartRankingsTimer();
    }

    private Timer? _rankingsTimer;
    private const long RankingsIntervalMs = 2000;
    private int _rankingsTick;
    private const int PromoteEveryNTicks = 15; // ~30s at 2000ms interval

    private void StartRankingsTimer()
    {
        _rankingsTimer = new Timer(_ =>
        {
            if (++_rankingsTick % PromoteEveryNTicks == 0)
                _symbolRegistry?.TryPromote();

            if (_clients.Count > 0)
                PushRankings();
        }, null, RankingsIntervalMs, RankingsIntervalMs);

        StartRecoveryProgressTimer();
    }

    /// <summary>Stop the background rankings timer.</summary>
    public void StopRankingsTimer()
    {
        _rankingsTimer?.Dispose();
        _recoveryProgressTimer?.Dispose();
    }

    // ─── Recovery progress broadcast ────────────────────────────────────────
    // Aggregates SymbolStateRegistry counters across every BookManager every
    // 250ms and emits a RecoveryProgress message whenever stale symbols exist.
    // Sends one last "all clear" message after stale drops to 0 so the UI can
    // close the recovering banner; then idles until stale > 0 again.

    private Timer? _recoveryProgressTimer;
    private const long RecoveryProgressIntervalMs = 250;
    private bool _recoveryProgressLastNonZero;

    private void StartRecoveryProgressTimer()
    {
        _recoveryProgressTimer = new Timer(
            _ => PushRecoveryProgress(),
            null,
            RecoveryProgressIntervalMs,
            RecoveryProgressIntervalMs);
    }

    private void PushRecoveryProgress()
    {
        if (_clients.IsEmpty) return;
        var managers = _bookManagers;
        if (managers is null || managers.Length == 0) return;

        Span<int> perKind = stackalloc int[14];
        perKind.Clear();
        int totalStale = 0;
        int totalKnown = 0;

        foreach (var bm in managers)
        {
            if (bm?.StateRegistry is not { } reg) continue;
            var snap = reg.GetAggregateSnapshot();
            totalStale += snap.TotalStaleSymbols;
            totalKnown += snap.TotalSymbols;
            int n = Math.Min(perKind.Length, snap.StaleByKind.Length);
            for (int i = 0; i < n; i++) perKind[i] += snap.StaleByKind[i];
        }

        // Edge-triggered idle: when stale falls from >0 to 0 emit one final
        // all-clear message, then suppress until stale becomes >0 again.
        bool isNonZero = totalStale > 0;
        if (!isNonZero && !_recoveryProgressLastNonZero) return;
        _recoveryProgressLastNonZero = isNonZero;

        Span<byte> buf = stackalloc byte[WireProtocol.RecoveryProgressMaxSize];
        int len = WireProtocol.WriteRecoveryProgress(buf, (uint)totalKnown, (uint)totalStale, perKind);
        var payload = new ReadOnlyMemory<byte>(buf[..len].ToArray());
        foreach (var (_, client) in _clients)
            client.TryEnqueue(payload);
    }

    public void Dispose()
    {
        _rankingsTimer?.Dispose();
        _recoveryProgressTimer?.Dispose();
        _outlierSweeper.Dispose();
    }

    /// <summary>Find an instrument across all per-group MarketDataManagers.</summary>
    public InstrumentInfo? FindInstrumentInfo(ulong securityId)
    {
        if (_marketDataManagers is not { } managers) return null;
        foreach (var mdm in managers)
            if (mdm.InstrumentData.TryGetValue(securityId, out var info))
                return info;
        return null;
    }

    /// <summary>
    /// Update LastTradePrice/Size from trade events.
    /// Called from GroupConflationHandler.OnTrade so that LastTradePrice is populated
    /// even when the feed does not carry LastTradePrice_27 messages.
    /// </summary>
    internal void UpdateLastTradeFromEvent(ulong securityId, long price, long quantity)
    {
        if (_marketDataManagers is not { } managers) return;
        foreach (var mdm in managers)
        {
            if (mdm.InstrumentData.TryGetValue(securityId, out var info))
            {
                info.LastTradePrice = price;
                info.LastTradeSize = quantity;
                info.BumpVersion();
                NotifyInfoUpdated(securityId);
                return;
            }
        }
    }

    private void BroadcastServerStatus(bool ready)
    {
        var buf = new byte[5];
        WireProtocol.WriteServerStatus(buf, ready);
        var payload = new ReadOnlyMemory<byte>(buf);
        foreach (var (_, client) in _clients)
            client.TryEnqueue(payload);
    }
}
