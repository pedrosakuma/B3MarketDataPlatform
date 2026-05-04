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

    // Per-security: clientId → flags + batch sequence barrier.
    // Outer dict is ConcurrentDictionary (lock-free reads).
    // Inner dicts use copy-on-write under _subLock for safe concurrent iteration.
    private readonly ConcurrentDictionary<ulong, Dictionary<string, SubscriptionState>> _subscriptions = new();

    // Pending unsubscribe requests from WebSocket threads (processed by any group)
    private readonly ConcurrentQueue<SubscriptionRequest> _pendingUnsubscribes = new();

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
    private readonly int _serverFlushWindowMs;

    private readonly long _clientMaxPendingBytes;
    private readonly OutlierSweeper _outlierSweeper;
    private readonly RankingsPublisher _rankingsPublisher;
    private readonly RecoveryProgressPublisher _recoveryProgressPublisher;

    public SubscriptionManager(
        ILogger<SubscriptionManager>? logger = null,
        int maxSnapshotRequestsPerBatch = 32,
        long clientMaxPendingBytes = 0,
        double outlierMultiplier = 4.0,
        long outlierMinBytes = 256L * 1024,
        double outlierPressurePct = 0.50,
        int outlierIntervalMs = 1000,
        int serverFlushWindowMs = 0)
    {
        if (serverFlushWindowMs < 0) throw new ArgumentOutOfRangeException(nameof(serverFlushWindowMs));
        _logger = logger ?? NullLogger<SubscriptionManager>.Instance;
        _maxSnapshotRequestsPerBatch = maxSnapshotRequestsPerBatch;
        _serverFlushWindowMs = serverFlushWindowMs;
        _clientMaxPendingBytes = clientMaxPendingBytes;
        _outlierSweeper = new OutlierSweeper(
            _clients,
            clientMaxPendingBytes,
            outlierMultiplier,
            outlierMinBytes,
            outlierPressurePct,
            outlierIntervalMs,
            _logger);
        _rankingsPublisher = new RankingsPublisher(
            () => _marketDataManagers,
            () => _symbolRegistry,
            _clients);
        _recoveryProgressPublisher = new RecoveryProgressPublisher(
            () => _bookManagers,
            _clients);
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
        return new GroupConflationHandler(this, maxSnapshotRequestsPerBatch: _maxSnapshotRequestsPerBatch, flushWindowMs: _serverFlushWindowMs);
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

        // ServerHello MUST be the first server-initiated frame so clients can negotiate
        // protocol version + capabilities before interpreting any other message.
        SnapshotEmitter.SendServerHello(session);

        // Immediately tell the client whether the server is ready
        SnapshotEmitter.SendServerStatus(session, _ready);
    }

    /// <summary>
    /// Terminal notification hook: deliver a <see cref="MessageType.SymbolDelisted"/>
    /// frame to every current subscriber of <paramref name="securityId"/> and tear
    /// down the per-symbol subscription map so the symbol stops fanning out.
    /// <para>Today this hook has no built-in upstream trigger — integration with the
    /// real SBE delisting code path (e.g. <c>SecurityStatus_3</c> in a terminal state)
    /// is a follow-up. The hook is exercised by the unit test in
    /// <c>SubscriptionManagerTests.NotifyDelisted_NotifiesOnlySubscribers</c>.</para>
    /// <para>Thread-safety: the snapshot of subscriber ids is taken under
    /// <c>_subLock</c>; per-subscriber <c>TryEnqueue</c> is multi-writer-safe.
    /// Cleanup runs under the same lock to avoid racing a fresh subscribe.</para>
    /// </summary>
    public void NotifyDelisted(ulong securityId)
    {
        string[] clientIds;
        lock (_subLock)
        {
            if (!_subscriptions.TryGetValue(securityId, out var subs) || subs.Count == 0)
                return;
            clientIds = new string[subs.Count];
            int i = 0;
            foreach (var k in subs.Keys) clientIds[i++] = k;
        }

        var buf = new byte[12];
        int len = WireProtocol.WriteSymbolDelisted(buf, securityId);
        var payload = new ReadOnlyMemory<byte>(buf, 0, len);

        foreach (var clientId in clientIds)
        {
            if (_clients.TryGetValue(clientId, out var session))
                session.TryEnqueue(payload);
        }

        // Tear down the per-symbol subscription map last so any concurrent
        // broadcast that has already snapshotted the inner dict still gets
        // through, but no future broadcast will find subscribers for it.
        lock (_subLock)
        {
            foreach (var clientId in clientIds)
                RemoveSubscriptionCore(clientId, securityId, enqueueAck: false, adjustMetric: true);
        }
    }

    /// <summary>
    /// Test-only seam: register a synthetic subscription without going through the
    /// full snapshot/registry pipeline. Used by <c>SubscriptionManagerTests</c> to
    /// exercise <see cref="NotifyDelisted"/> in isolation.
    /// </summary>
    internal void AddSubscriptionForTest(string clientId, ulong securityId, DataFlags flags)
    {
        lock (_subLock)
        {
            var newClients = _subscriptions.TryGetValue(securityId, out var existing)
                ? new Dictionary<string, SubscriptionState>(existing)
                : new Dictionary<string, SubscriptionState>();
            newClients[clientId] = new SubscriptionState(flags, 0);
            _subscriptions[securityId] = newClients;
        }
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

    /// <summary>
    /// Count of distinct securities with at least one active subscriber.
    /// Lock-free; intended for observability gauges (low-cardinality, scrape-time only).
    /// </summary>
    public int ActiveSymbolCount => _subscriptions.Count;

    /// <summary>Broadcast pre-serialized bytes to all Book subscribers for a security.</summary>
    internal void BroadcastToSubscribers(ulong securityId, ReadOnlyMemory<byte> payload)
    {
        // Lock-free read: inner dict is a copy-on-write snapshot, safe to iterate.
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return;
        foreach (var (clientId, state) in clients)
        {
            if ((state.Flags & DataFlags.Book) == 0) continue;
            if (_clients.TryGetValue(clientId, out var session))
                session.TryEnqueue(payload);
        }
    }

    /// <summary>Lock-free accessor for the inner per-security subscriber dict (copy-on-write under
    /// _subLock). Used by hot-path coalesced broadcast in <see cref="GroupConflationHandler"/>
    /// to amortize the per-event Channel.TryWrite cost across an entire flush cycle.
    ///
    /// Returns the concrete <see cref="Dictionary{TKey, TValue}"/> (not the interface) so the
    /// broadcaster's foreach uses the struct enumerator — avoids boxing one IEnumerator per
    /// per-event call inside the fan-out loop.
    /// </summary>
    internal Dictionary<string, SubscriptionState>? GetSubscribers(ulong securityId) =>
        _subscriptions.TryGetValue(securityId, out var clients) ? clients : null;

    /// <summary>
    /// Lock-free quick check used by the dispatch thread to skip serializing
    /// instrument-scoped news bytes for securities with no News-flag subscriber.
    /// </summary>
    internal bool HasAnyNewsSubscriberFor(ulong securityId)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return false;
        foreach (var (_, state) in clients)
            if (state.WantsNews) return true;
        return false;
    }

    /// <summary>
    /// Lock-free check: any connected client has the News flag on at least one
    /// subscription? Used to skip serializing global news entirely when no one
    /// is listening. Iterates all subscription buckets — cheap when most
    /// clients use News (early-return) and bounded by symbol count otherwise.
    /// </summary>
    internal bool HasAnyNewsSubscriberAnywhere()
    {
        foreach (var kv in _subscriptions)
        {
            foreach (var (_, state) in kv.Value)
                if (state.WantsNews) return true;
        }
        return false;
    }

    /// <summary>Enumerate all currently connected clients (broadcaster thread use).
    /// Returns the live ConcurrentDictionary view; safe to enumerate under concurrent mutation.</summary>
    internal IEnumerable<KeyValuePair<string, ClientSession>> EnumerateAllClients() => _clients;

    /// <summary>Lock-free quick check used by the dispatch thread to skip buffering wire bytes
    /// for securities that have no Book-flag subscriber. Returns true if at least one
    /// current subscriber has <see cref="DataFlags.Book"/> set.
    /// </summary>
    internal bool HasAnyBookSubscriber(ulong securityId)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return false;
        foreach (var (_, state) in clients)
            if ((state.Flags & DataFlags.Book) != 0) return true;
        return false;
    }

    /// <summary>Lock-free quick check used by the dispatch thread to skip MBP buffering
    /// for securities with no MBP-flag subscriber.</summary>
    internal bool HasAnyMbpSubscriber(ulong securityId)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return false;
        foreach (var (_, state) in clients)
            if ((state.Flags & DataFlags.Mbp) != 0) return true;
        return false;
    }

    /// <summary>Lock-free quick check used by the dispatch thread to skip serializing
    /// trade frames (Trade, TradeBust) for securities with no Trades-flag subscriber.</summary>
    internal bool HasAnyTradesSubscriber(ulong securityId)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return false;
        foreach (var (_, state) in clients)
            if ((state.Flags & DataFlags.Trades) != 0) return true;
        return false;
    }

    /// <summary>
    /// Invoked from the dispatch thread when a broadcast batch had to be dropped
    /// (broadcaster ring full). Schedules a fresh snapshot (Get) request for every
    /// current Book- or Mbp-flag subscriber of <paramref name="securityId"/> so they can
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
        foreach (var (clientId, state) in clients)
        {
            var resyncFlags = state.Flags & (DataFlags.Book | DataFlags.Mbp);
            if (resyncFlags == 0) continue;
            group.EnqueueRequest(clientId, symbol, resyncFlags, isGet: true);
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
        foreach (var (clientId, state) in clients)
        {
            if (!state.WantsInfo) continue;
            if (_clients.TryGetValue(clientId, out var session))
                session.NotifyInfoAvailable();
        }
    }

    // --- Subscribe handling (called on owning group's thread) ---

    internal void HandleSubscribe(string clientId, string symbol, DataFlags flags,
        BookManager bookManager, GroupConflationHandler group, long bookBatchCutoffSequence)
    {
        if (!TryValidateAndResolve(clientId, symbol, out var session, out var securityId))
            return;

        // Send SubscribeOk
        var okBuf = new byte[WireProtocol.FramingHeaderSize + 8 + 1 + 1 + System.Text.Encoding.UTF8.GetMaxByteCount(symbol.Length)];
        int okLen = WireProtocol.WriteSubscribeOk(okBuf, securityId, flags, symbol);
        if (!session.TryEnqueue(new ReadOnlyMemory<byte>(okBuf, 0, okLen)))
            return;

        // Activate before publishing the already-serialized current batch, but with
        // a sequence barrier. The broadcaster will skip every queued/current batch
        // at or below bookBatchCutoffSequence for this subscription, so the snapshot
        // remains the client's baseline and future incrementals start after it.
        if (!ActivateSubscription(session, clientId, securityId, flags, bookBatchCutoffSequence, out var activation))
            return;

        if (!SendSnapshots(session, securityId, flags, bookManager, group))
        {
            RollbackSubscriptionActivation(session, clientId, securityId, activation);
            return;
        }

        if (activation.IsNew)
            MetricsRegistry.WsSubscriptions.Add(1);
    }

    internal void HandleGet(string clientId, string symbol, DataFlags flags,
        BookManager bookManager, GroupConflationHandler group, long bookBatchCutoffSequence)
    {
        if (!TryValidateAndResolve(clientId, symbol, out var session, out var securityId))
            return;

        if (flags.HasFlag(DataFlags.Book) || flags.HasFlag(DataFlags.Mbp))
            UpdateBookCutoffIfSubscribed(clientId, securityId, bookBatchCutoffSequence);

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

    private bool SendSnapshots(ClientSession session, ulong securityId, DataFlags flags,
        BookManager bookManager, GroupConflationHandler group)
    {
        if (flags.HasFlag(DataFlags.Book))
        {
            if (bookManager.Books.TryGetValue(securityId, out var book))
            {
                if (!SnapshotEmitter.SendMboSnapshot(session, book))
                    return false;
            }
            else
            {
                if (!SnapshotEmitter.SendEmptyBookSnapshot(session, securityId))
                    return false;
            }

            // Send candle history from the owning group's aggregator.
            // Always send a CandleSnapshot (even empty) so the frontend knows the snapshot phase is complete.
            if (group.Candles.TryGetValue(securityId, out var agg))
            {
                if (!SnapshotEmitter.SendCandleHistory(session, securityId, agg))
                    return false;
            }
            else
            {
                if (!SnapshotEmitter.SendEmptyCandleSnapshot(session, securityId))
                    return false;
            }
        }

        if (flags.HasFlag(DataFlags.Mbp))
        {
            if (bookManager.Books.TryGetValue(securityId, out var mbpBook))
            {
                if (!SnapshotEmitter.SendMbpSnapshot(session, mbpBook))
                    return false;
            }
            else
            {
                if (!SnapshotEmitter.SendEmptyMbpSnapshot(session, securityId))
                    return false;
            }

            // MBP-only subscribers (no Book flag) still need candles for the chart
            // panel which is part of the L2 view.
            if (!flags.HasFlag(DataFlags.Book))
            {
                if (group.Candles.TryGetValue(securityId, out var agg))
                {
                    if (!SnapshotEmitter.SendCandleHistory(session, securityId, agg))
                        return false;
                }
                else
                {
                    if (!SnapshotEmitter.SendEmptyCandleSnapshot(session, securityId))
                        return false;
                }
            }
        }

        if (flags.HasFlag(DataFlags.Trades))
        {
            // Trade history snapshot is gated on the opt-in Trades flag. Sent
            // independently of Book/Mbp so a client requesting only Trades still
            // gets recent prints. The ring may be empty for cold symbols
            // (Phase C optimization) — that's fine, no frame is emitted.
            if (group.RecentTrades.TryGetValue(securityId, out var trades))
            {
                if (!SnapshotEmitter.SendTradeHistory(session, securityId, trades))
                    return false;
            }
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
                        if (!SnapshotEmitter.SendInfoSnapshot(session, securityId, info))
                            return false;
                        break;
                    }
                }
            }
        }

        return true;
    }

    private void HandleUnsubscribe(string clientId, ulong securityId)
    {
        // Must be called under _subLock
        RemoveSubscriptionCore(clientId, securityId, enqueueAck: true, adjustMetric: true);
    }

    private bool ActivateSubscription(
        ClientSession session,
        string clientId,
        ulong securityId,
        DataFlags flags,
        long bookBatchCutoffSequence,
        out SubscriptionActivation activation)
    {
        activation = default;
        lock (_subLock)
        {
            _subscriptions.TryGetValue(securityId, out var existing);
            SubscriptionState? previous = null;
            bool hadPrevious = existing is not null && existing.TryGetValue(clientId, out previous!);
            bool wantsInfo = flags.HasFlag(DataFlags.Info);
            bool hadInfo = hadPrevious && previous!.WantsInfo;

            if (wantsInfo && !hadInfo && !session.AddInfoSubscription(securityId))
                return false;
            if (!wantsInfo && hadInfo && !session.RemoveInfoSubscription(securityId))
                return false;

            session.AddSubscription(securityId);

            bool wantsNews = (flags & DataFlags.News) != 0;
            bool hadNews = hadPrevious && previous!.WantsNews;
            if (wantsNews && !hadNews) session.IncrementNewsSubscriptions();
            else if (!wantsNews && hadNews) session.DecrementNewsSubscriptions();

            // Copy-on-write: create new inner dict to avoid mutating a dict being iterated.
            var newClients = existing is not null ? new Dictionary<string, SubscriptionState>(existing) : new();
            newClients[clientId] = new SubscriptionState(flags, bookBatchCutoffSequence);
            _subscriptions[securityId] = newClients;
            activation = new SubscriptionActivation(
                !hadPrevious,
                hadPrevious,
                previous,
                AddedInfoSubscription: wantsInfo && !hadInfo,
                RemovedInfoSubscription: !wantsInfo && hadInfo,
                AddedNewsSubscription: wantsNews && !hadNews,
                RemovedNewsSubscription: !wantsNews && hadNews);
        }

        return true;
    }

    private readonly record struct SubscriptionActivation(
        bool IsNew,
        bool HadPrevious,
        SubscriptionState? PreviousState,
        bool AddedInfoSubscription,
        bool RemovedInfoSubscription,
        bool AddedNewsSubscription,
        bool RemovedNewsSubscription);

    private void RollbackSubscriptionActivation(
        ClientSession session,
        string clientId,
        ulong securityId,
        SubscriptionActivation activation)
    {
        lock (_subLock)
        {
            if (!_subscriptions.TryGetValue(securityId, out var existing)) return;
            var newClients = new Dictionary<string, SubscriptionState>(existing);
            if (activation.HadPrevious)
                newClients[clientId] = activation.PreviousState!;
            else
                newClients.Remove(clientId);

            if (newClients.Count == 0)
                _subscriptions.TryRemove(securityId, out _);
            else
                _subscriptions[securityId] = newClients;
        }

        if (activation.IsNew)
            session.RemoveSubscription(securityId);
        else if (activation.AddedInfoSubscription)
            session.RemoveInfoSubscription(securityId);
        else if (activation.RemovedInfoSubscription)
            session.AddInfoSubscription(securityId);

        // Mirror the news-counter delta applied during activation.
        if (activation.AddedNewsSubscription) session.DecrementNewsSubscriptions();
        else if (activation.RemovedNewsSubscription) session.IncrementNewsSubscriptions();
    }

    /// <summary>
    /// Advance the per-client book broadcast cutoff after a Get snapshot. Lock-free:
    /// reads the lock-free outer <see cref="_subscriptions"/> and the lock-free inner
    /// dictionary snapshot, then performs a CAS-max on the mutable cutoff cell of the
    /// shared <see cref="SubscriptionState"/>. CoW snapshots share the state reference,
    /// so the new cutoff is immediately visible to broadcasters iterating any snapshot.
    /// Safe against concurrent Subscribe/Unsubscribe (worst case: a stale state object
    /// no longer reachable from the current snapshot is updated harmlessly).
    /// </summary>
    private void UpdateBookCutoffIfSubscribed(string clientId, ulong securityId, long bookBatchCutoffSequence)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return;
        if (!clients.TryGetValue(clientId, out var state)) return;
        if ((state.Flags & DataFlags.Book) == 0) return;
        state.AdvanceMinBroadcastSequence(bookBatchCutoffSequence);
    }

    private void RemoveSubscriptionCore(string clientId, ulong securityId, bool enqueueAck, bool adjustMetric)
    {
        if (!_clients.TryGetValue(clientId, out var session)) return;

        session.RemoveSubscription(securityId);

        // Copy-on-write
        if (_subscriptions.TryGetValue(securityId, out var existing))
        {
            bool removedHadNews = existing.TryGetValue(clientId, out var prev) && prev.WantsNews;
            var newClients = new Dictionary<string, SubscriptionState>(existing);
            if (newClients.Remove(clientId))
            {
                if (adjustMetric)
                    MetricsRegistry.WsSubscriptions.Add(-1);
                if (removedHadNews) session.DecrementNewsSubscriptions();
                if (newClients.Count == 0)
                    _subscriptions.TryRemove(securityId, out _);
                else
                    _subscriptions[securityId] = newClients;
            }
        }

        if (!enqueueAck) return;
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
            if (!clients.TryGetValue(clientId, out var prev)) continue;
            removedSubscriptions++;
            if (prev.WantsNews && _clients.TryGetValue(clientId, out var s))
                s.DecrementNewsSubscriptions();

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
                var newClients = new Dictionary<string, SubscriptionState>(existing);
                newClients.Remove(clientId);
                _subscriptions[secId] = newClients;
            }
        }

        if (removedSubscriptions > 0)
            MetricsRegistry.WsSubscriptions.Add(-removedSubscriptions);
    }

    // --- Snapshot serialization moved to SnapshotEmitter ---
    // --- Rankings broadcast moved to RankingsPublisher ---
    // --- Recovery progress broadcast moved to RecoveryProgressPublisher ---

    /// <summary>Called when feed enters RealTime state. Enables subscriptions and starts background broadcasters.</summary>
    public void SetReady()
    {
        if (_ready) return;

        _ready = true;
        BroadcastServerStatus(true);
        _rankingsPublisher.Start();
        _recoveryProgressPublisher.Start();
    }

    /// <summary>Stop background broadcasters (rankings + recovery progress).</summary>
    public void StopRankingsTimer()
    {
        _rankingsPublisher.Dispose();
        _recoveryProgressPublisher.Dispose();
    }

    public void Dispose()
    {
        _rankingsPublisher.Dispose();
        _recoveryProgressPublisher.Dispose();
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
