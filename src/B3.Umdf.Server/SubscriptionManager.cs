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
    public const int MinimumConflatedCadenceMs = 100;
    private static readonly int[] DefaultConflatedCadences = [100, 250, 500];
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
    // Per-security routing indexes derived from the same immutable subscription
    // snapshot. Rebuilt only on subscription mutation so event-time fanout never
    // scans conflated-only consumers to find immediate recipients, nor scans
    // consumers to decide which cadence buckets are active.
    private readonly ConcurrentDictionary<ulong, SubscriptionRoutingIndex> _routingIndexes = new();

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
    private readonly int[] _allowedConflatedCadencesMs;

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
        int serverFlushWindowMs = 0,
        IReadOnlyCollection<int>? allowedConflatedCadencesMs = null)
    {
        if (serverFlushWindowMs < 0) throw new ArgumentOutOfRangeException(nameof(serverFlushWindowMs));
        _logger = logger ?? NullLogger<SubscriptionManager>.Instance;
        _maxSnapshotRequestsPerBatch = maxSnapshotRequestsPerBatch;
        _serverFlushWindowMs = serverFlushWindowMs;
        _allowedConflatedCadencesMs = ValidateConflatedCadences(allowedConflatedCadencesMs);
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
            _clients,
            _logger);
        _recoveryProgressPublisher = new RecoveryProgressPublisher(
            () => _bookManagers,
            _clients,
            _logger);
    }

    public IReadOnlyList<int> AllowedConflatedCadencesMs => _allowedConflatedCadencesMs;

    private static int[] ValidateConflatedCadences(IReadOnlyCollection<int>? configured)
    {
        var values = configured is null
            ? (int[])DefaultConflatedCadences.Clone()
            : configured.Distinct().Order().ToArray();
        if (values.Length == 0)
            throw new ArgumentException("At least one conflated cadence must be configured.", nameof(configured));
        foreach (int cadence in values)
        {
            if (cadence < MinimumConflatedCadenceMs || cadence > ushort.MaxValue)
                throw new ArgumentOutOfRangeException(
                    nameof(configured),
                    $"Conflated cadences must be between {MinimumConflatedCadenceMs} and {ushort.MaxValue} ms.");
        }
        return values;
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

        var buf = new byte[16];
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
    /// Centralised copy-on-write writer for <see cref="_subscriptions"/>. Acquires
    /// <see cref="_subLock"/>, snapshots the current per-security inner map, hands it
    /// to <paramref name="mutator"/>, and publishes the result plus its derived
    /// routing indexes (removes both entries if the mutator returns null or an empty map).
    ///
    /// ALL writes that bump the per-security snapshot MUST go through this helper so
    /// the COW invariant ("readers always see a frozen Dictionary, writers always
    /// build a fresh one") is impossible to violate from a future contributor.
    /// The mutator receives <c>null</c> when the security is not currently tracked;
    /// it MUST NOT mutate the existing dictionary in place — return a new instance.
    /// The lock is reentrant so callers already inside <c>lock (_subLock)</c> for a
    /// broader sequence (e.g. <see cref="ActivateSubscription"/>) can still use it.
    /// </summary>
    private void UpdateSubscriptionSnapshot(
        ulong securityId,
        Func<Dictionary<string, SubscriptionState>?, Dictionary<string, SubscriptionState>?> mutator)
    {
        lock (_subLock)
        {
            _subscriptions.TryGetValue(securityId, out var existing);
            var next = mutator(existing);
            if (next is null || next.Count == 0)
            {
                _subscriptions.TryRemove(securityId, out _);
                _routingIndexes.TryRemove(securityId, out _);
            }
            else if (!ReferenceEquals(next, existing))
            {
                _subscriptions[securityId] = next;
                _routingIndexes[securityId] = SubscriptionRoutingIndex.Create(next);
            }
        }
    }

    private sealed class SubscriptionRoutingIndex
    {
        public Dictionary<string, SubscriptionState> Book { get; }
        public Dictionary<string, SubscriptionState> ImmediateMbp { get; }
        public Dictionary<string, SubscriptionState> BookOrImmediateMbp { get; }
        public Dictionary<string, SubscriptionState> Trades { get; }
        public Dictionary<ushort, Dictionary<string, SubscriptionState>> ConflatedMbpByCadence { get; }

        private SubscriptionRoutingIndex(
            Dictionary<string, SubscriptionState> book,
            Dictionary<string, SubscriptionState> immediateMbp,
            Dictionary<string, SubscriptionState> bookOrImmediateMbp,
            Dictionary<string, SubscriptionState> trades,
            Dictionary<ushort, Dictionary<string, SubscriptionState>> conflatedMbpByCadence)
        {
            Book = book;
            ImmediateMbp = immediateMbp;
            BookOrImmediateMbp = bookOrImmediateMbp;
            Trades = trades;
            ConflatedMbpByCadence = conflatedMbpByCadence;
        }

        public static SubscriptionRoutingIndex Create(
            Dictionary<string, SubscriptionState> subscriptions)
        {
            var book = new Dictionary<string, SubscriptionState>();
            var immediateMbp = new Dictionary<string, SubscriptionState>();
            var bookOrImmediateMbp = new Dictionary<string, SubscriptionState>();
            var trades = new Dictionary<string, SubscriptionState>();
            var conflated = new Dictionary<ushort, Dictionary<string, SubscriptionState>>();

            foreach (var (clientId, state) in subscriptions)
            {
                bool wantsBook = (state.Flags & DataFlags.Book) != 0;
                if (wantsBook)
                    book[clientId] = state;

                if (state.WantsMbp)
                    immediateMbp[clientId] = state;

                if (wantsBook || state.WantsMbp)
                    bookOrImmediateMbp[clientId] = state;

                if (state.WantsTrades)
                    trades[clientId] = state;

                if (state.WantsConflatedMbp)
                {
                    if (!conflated.TryGetValue(state.ConflationIntervalMs, out var cadence))
                    {
                        cadence = new Dictionary<string, SubscriptionState>();
                        conflated[state.ConflationIntervalMs] = cadence;
                    }
                    cadence[clientId] = state;
                }
            }

            return new SubscriptionRoutingIndex(
                book,
                immediateMbp,
                bookOrImmediateMbp,
                trades,
                conflated);
        }
    }

    /// <summary>
    /// Test-only seam: register a synthetic subscription without going through the
    /// full snapshot/registry pipeline. Used by <c>SubscriptionManagerTests</c> to
    /// exercise <see cref="NotifyDelisted"/> in isolation.
    /// </summary>
    internal void AddSubscriptionForTest(
        string clientId,
        ulong securityId,
        DataFlags flags,
        ushort conflationIntervalMs = 0)
    {
        UpdateSubscriptionSnapshot(securityId, existing =>
        {
            var next = existing is not null
                ? new Dictionary<string, SubscriptionState>(existing)
                : new Dictionary<string, SubscriptionState>();
            next[clientId] = new SubscriptionState(
                flags,
                minBookBroadcastSequenceExclusive: 0,
                minTradeBroadcastSequenceExclusive: 0,
                conflationIntervalMs: conflationIntervalMs);
            return next;
        });
    }

    /// <summary>Unregister a client and remove all its subscriptions.</summary>
    public void UnregisterClient(string clientId)
    {
        _clients.TryRemove(clientId, out _);
        _pendingUnsubscribes.Enqueue(SubscriptionRequest.UnsubscribeAll(clientId));
        MetricsRegistry.WsConnectionsActive.Add(-1);
    }

    /// <summary>Called from WebSocket read thread to request a subscription.</summary>
    public void RequestSubscribe(
        string clientId,
        string symbol,
        DataFlags flags,
        ushort conflationIntervalMs = 0)
    {
        var req = SubscriptionRequest.Subscribe(clientId, symbol, flags, conflationIntervalMs);
        if (!RouteToGroup(req))
            SendRouteFailureError(clientId, symbol);
    }

    /// <summary>Called from WebSocket read thread to request a one-shot snapshot.</summary>
    public void RequestGet(
        string clientId,
        string symbol,
        DataFlags flags,
        ushort conflationIntervalMs = 0)
    {
        var req = SubscriptionRequest.Get(clientId, symbol, flags, conflationIntervalMs);
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
                gh.EnqueueRequest(
                    req.ClientId,
                    req.Symbol,
                    req.Flags,
                    req.Kind == SubscriptionRequestKind.Get,
                    req.ConflationIntervalMs);
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

    public int ActiveConflatedSubscriptionCount
    {
        get
        {
            int count = 0;
            foreach (var subscriptions in _subscriptions.Values)
                foreach (var state in subscriptions.Values)
                    if (state.WantsConflatedMbp) count++;
            return count;
        }
    }

    /// <summary>Broadcast pre-serialized bytes to all Book subscribers for a security.</summary>
    internal void BroadcastToSubscribers(ulong securityId, ReadOnlyMemory<byte> payload)
    {
        if (GetBookSubscribers(securityId) is not { } clients) return;
        foreach (var (clientId, _) in clients)
        {
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

    internal Dictionary<string, SubscriptionState>? GetBookSubscribers(ulong securityId) =>
        _routingIndexes.TryGetValue(securityId, out var routes) && routes.Book.Count != 0
            ? routes.Book
            : null;

    internal Dictionary<string, SubscriptionState>? GetImmediateMbpSubscribers(ulong securityId) =>
        _routingIndexes.TryGetValue(securityId, out var routes) && routes.ImmediateMbp.Count != 0
            ? routes.ImmediateMbp
            : null;

    internal Dictionary<string, SubscriptionState>? GetBookOrImmediateMbpSubscribers(ulong securityId) =>
        _routingIndexes.TryGetValue(securityId, out var routes) && routes.BookOrImmediateMbp.Count != 0
            ? routes.BookOrImmediateMbp
            : null;

    internal Dictionary<string, SubscriptionState>? GetTradeSubscribers(ulong securityId) =>
        _routingIndexes.TryGetValue(securityId, out var routes) && routes.Trades.Count != 0
            ? routes.Trades
            : null;

    internal Dictionary<string, SubscriptionState>? GetConflatedMbpSubscribers(
        ulong securityId,
        int cadenceMs) =>
        _routingIndexes.TryGetValue(securityId, out var routes) &&
        routes.ConflatedMbpByCadence.TryGetValue(checked((ushort)cadenceMs), out var cadence) &&
        cadence.Count != 0
            ? cadence
            : null;

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
        => _routingIndexes.TryGetValue(securityId, out var routes) && routes.Book.Count != 0;

    /// <summary>Lock-free quick check used by the dispatch thread to skip MBP buffering
    /// for securities with no MBP-flag subscriber.</summary>
    internal bool HasAnyMbpSubscriber(ulong securityId)
        => _routingIndexes.TryGetValue(securityId, out var routes) &&
           (routes.ImmediateMbp.Count != 0 || routes.ConflatedMbpByCadence.Count != 0);

    internal bool HasAnyConflatedMbpSubscriber(ulong securityId, int cadenceMs)
        => GetConflatedMbpSubscribers(securityId, cadenceMs) is not null;

    /// <summary>Lock-free quick check used by the dispatch thread to skip serializing
    /// trade frames (Trade, TradeBust) for securities with no Trades-flag subscriber.</summary>
    internal bool HasAnyTradesSubscriber(ulong securityId)
        => _routingIndexes.TryGetValue(securityId, out var routes) && routes.Trades.Count != 0;

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
            var resyncFlags = state.Flags & (DataFlags.Book | DataFlags.Mbp | DataFlags.ConflatedMbp);
            if (resyncFlags == 0) continue;
            group.EnqueueRequest(
                clientId,
                symbol,
                resyncFlags,
                isGet: true,
                state.ConflationIntervalMs);
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

    /// <summary>
    /// Publishes a non-conflated halt/resume transition to existing Info
    /// subscribers. The dedicated frame is additive: legacy clients skip its
    /// unknown opcode and continue receiving the normal InfoSnapshot update.
    /// </summary>
    internal void PublishInstrumentStatus(
        ulong securityId,
        string? symbol,
        in InstrumentStatusUpdate update)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return;

        byte[]? buffer = null;
        int length = 0;
        foreach (var (clientId, state) in clients)
        {
            if (!state.WantsInfo || !_clients.TryGetValue(clientId, out var session))
                continue;

            if (buffer is null)
            {
                buffer = new byte[WireProtocol.InstrumentStatusMaxSize];
                length = WireProtocol.WriteInstrumentStatus(
                    buffer, securityId, symbol, in update);
            }

            session.TryEnqueue(new ReadOnlyMemory<byte>(buffer, 0, length));
        }
    }

    /// <summary>
    /// Wake <see cref="DataFlags.SecurityDefinition"/> subscribers for a security
    /// so their write loop emits a fresh <see cref="MessageType.SecurityDefinition"/>
    /// frame on the next cycle. Called by <c>GroupConflationHandler.OnSecurityDefinitionChanged</c>
    /// — itself fired only when <c>MarketDataManager.HandleSecurityDefinition</c>
    /// actually applied the payload (idempotent re-broadcasts short-circuit upstream).
    /// </summary>
    internal void NotifySecurityDefinitionUpdated(ulong securityId)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return;
        foreach (var (clientId, state) in clients)
        {
            if (!state.WantsSecurityDefinition) continue;
            if (_clients.TryGetValue(clientId, out var session))
                session.NotifySecurityDefinitionAvailable();
        }
    }

    /// <summary>
    /// Wake <see cref="DataFlags.PriceBand"/> subscribers for a security so
    /// their write loop emits a fresh <see cref="MessageType.PriceBand"/>
    /// frame on the next cycle. Called by <c>GroupConflationHandler.OnPriceBandChanged</c>
    /// — itself fired only when <c>MarketDataManager.HandlePriceBand</c>
    /// detected a real delta (idempotent re-broadcasts short-circuit upstream).
    /// </summary>
    internal void NotifyPriceBandUpdated(ulong securityId)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return;
        foreach (var (clientId, state) in clients)
        {
            if (!state.WantsPriceBand) continue;
            if (_clients.TryGetValue(clientId, out var session))
                session.NotifyPriceBandAvailable();
        }
    }

    /// <summary>
    /// Wake <see cref="DataFlags.Auction"/> subscribers for a security so
    /// their write loop emits a fresh <see cref="MessageType.Auction"/>
    /// frame on the next cycle. Called by <c>GroupConflationHandler.OnAuctionChanged</c>
    /// — itself fired only when imbalance or group-phase fields actually changed.
    /// </summary>
    internal void NotifyAuctionUpdated(ulong securityId)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return;
        foreach (var (clientId, state) in clients)
        {
            if (!state.WantsAuction) continue;
            if (_clients.TryGetValue(clientId, out var session))
                session.NotifyAuctionAvailable();
        }
    }

    // --- Subscribe handling (called on owning group's thread) ---

    internal void HandleSubscribe(string clientId, string symbol, DataFlags flags,
        BookManager bookManager, GroupConflationHandler group, long bookBatchCutoffSequence)
        => HandleSubscribe(clientId, symbol, flags, 0, bookManager, group, bookBatchCutoffSequence);

    internal void HandleSubscribe(
        string clientId,
        string symbol,
        DataFlags flags,
        ushort conflationIntervalMs,
        BookManager bookManager,
        GroupConflationHandler group,
        long bookBatchCutoffSequence)
    {
        if (!TryValidateAndResolve(clientId, symbol, out var session, out var securityId))
            return;
        if (!ValidateSubscriptionOptions(session, symbol, flags, conflationIntervalMs))
            return;

        // Send SubscribeOk
        var okBuf = new byte[
            WireProtocol.FramingHeaderSize + 8 + 4 + 1 +
            System.Text.Encoding.UTF8.GetMaxByteCount(symbol.Length) + 2];
        int okLen = WireProtocol.WriteSubscribeOk(okBuf, securityId, flags, symbol, conflationIntervalMs);
        if (!session.TryEnqueue(new ReadOnlyMemory<byte>(okBuf, 0, okLen)))
            return;

        // Activate before publishing the already-serialized current batch, but with
        // a sequence barrier. The broadcaster will skip every queued/current batch
        // at or below bookBatchCutoffSequence for this subscription, so the snapshot
        // remains the client's baseline and future incrementals start after it.
        if (!ActivateSubscription(
                session,
                clientId,
                securityId,
                flags,
                conflationIntervalMs,
                bookBatchCutoffSequence,
                out var activation))
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
        => HandleGet(clientId, symbol, flags, 0, bookManager, group, bookBatchCutoffSequence);

    internal void HandleGet(
        string clientId,
        string symbol,
        DataFlags flags,
        ushort conflationIntervalMs,
        BookManager bookManager,
        GroupConflationHandler group,
        long bookBatchCutoffSequence)
    {
        if (!TryValidateAndResolve(clientId, symbol, out var session, out var securityId))
            return;
        if (!ValidateSubscriptionOptions(session, symbol, flags, conflationIntervalMs))
            return;

        UpdateSnapshotCutoffsIfSubscribed(
            clientId,
            securityId,
            flags,
            bookBatchCutoffSequence);

        SendSnapshots(session, securityId, flags, bookManager, group);
    }

    private bool ValidateSubscriptionOptions(
        ClientSession session,
        string symbol,
        DataFlags flags,
        ushort conflationIntervalMs)
    {
        bool conflated = (flags & DataFlags.ConflatedMbp) != 0;
        if (conflated && (flags & DataFlags.Mbp) != 0)
        {
            SendError(session, SubscribeErrorCode.InvalidFlags, symbol);
            return false;
        }
        if (!conflated && conflationIntervalMs != 0)
        {
            SendError(session, SubscribeErrorCode.InvalidFlags, symbol);
            return false;
        }
        if (conflated && Array.BinarySearch(_allowedConflatedCadencesMs, (int)conflationIntervalMs) < 0)
        {
            SendError(session, SubscribeErrorCode.InvalidCadence, symbol);
            return false;
        }
        return true;
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

        if ((flags & (DataFlags.Mbp | DataFlags.ConflatedMbp)) != 0)
        {
            bool isStale = bookManager.StateRegistry.IsAnyStale(securityId);
            if (bookManager.Books.TryGetValue(securityId, out var mbpBook))
            {
                if (!SnapshotEmitter.SendMbpSnapshot(
                        session,
                        mbpBook,
                        isStale))
                    return false;
            }
            else
            {
                if (!SnapshotEmitter.SendEmptyMbpSnapshot(session, securityId, isStale))
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

            // Trades-only subscribers (no Book/Mbp) still need candles for the
            // chart panel. Book and Mbp already send candles above, so only
            // emit here if neither is set.
            if (!flags.HasFlag(DataFlags.Book) &&
                (flags & (DataFlags.Mbp | DataFlags.ConflatedMbp)) == 0)
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
                        if (!SnapshotEmitter.SendInstrumentStatusSnapshot(session, securityId, info))
                            return false;
                        break;
                    }
                }
            }
        }

        if (flags.HasFlag(DataFlags.SecurityDefinition))
        {
            // Same MDM search as Info — the static metadata lives on the same InstrumentInfo.
            // Skip emission when no Symbol is cached yet (HandleSecurityDefinition was never
            // entered for this securityId); the next real definition will push it via
            // the delta path.
            if (_marketDataManagers is { } managers)
            {
                foreach (var mdm in managers)
                {
                    if (mdm.InstrumentData.TryGetValue(securityId, out var info)
                        && !string.IsNullOrEmpty(info.Symbol))
                    {
                        if (!SnapshotEmitter.SendSecurityDefinitionSnapshot(session, securityId, info))
                            return false;
                        break;
                    }
                }
            }
        }

        if (flags.HasFlag(DataFlags.PriceBand))
        {
            // Same MDM search as SecurityDefinition. SnapshotEmitter.SendPriceBandSnapshot
            // is itself a no-op when the band hasn't been observed yet; the next real
            // PriceBand_22 will push it via the delta path.
            if (_marketDataManagers is { } managers)
            {
                foreach (var mdm in managers)
                {
                    if (mdm.InstrumentData.TryGetValue(securityId, out var info))
                    {
                        if (!SnapshotEmitter.SendPriceBandSnapshot(session, securityId, info))
                            return false;
                        break;
                    }
                }
            }
        }

        if (flags.HasFlag(DataFlags.Auction))
        {
            // Same MDM search as PriceBand. SnapshotEmitter.SendAuctionSnapshot
            // is a no-op when no imbalance or trading status has been observed yet.
            if (_marketDataManagers is { } managers)
            {
                foreach (var mdm in managers)
                {
                    if (mdm.InstrumentData.TryGetValue(securityId, out var info))
                    {
                        if (!SnapshotEmitter.SendAuctionSnapshot(session, securityId, info))
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
        ushort conflationIntervalMs,
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

            bool wantsSecDef = flags.HasFlag(DataFlags.SecurityDefinition);
            bool hadSecDef = hadPrevious && previous!.WantsSecurityDefinition;
            if (wantsSecDef && !hadSecDef && !session.AddSecurityDefinitionSubscription(securityId))
                return false;
            if (!wantsSecDef && hadSecDef && !session.RemoveSecurityDefinitionSubscription(securityId))
                return false;

            bool wantsPriceBand = flags.HasFlag(DataFlags.PriceBand);
            bool hadPriceBand = hadPrevious && previous!.WantsPriceBand;
            if (wantsPriceBand && !hadPriceBand && !session.AddPriceBandSubscription(securityId))
                return false;
            if (!wantsPriceBand && hadPriceBand && !session.RemovePriceBandSubscription(securityId))
                return false;

            bool wantsAuction = flags.HasFlag(DataFlags.Auction);
            bool hadAuction = hadPrevious && previous!.WantsAuction;
            if (wantsAuction && !hadAuction && !session.AddAuctionSubscription(securityId))
                return false;
            if (!wantsAuction && hadAuction && !session.RemoveAuctionSubscription(securityId))
                return false;

            session.AddSubscription(securityId);

            bool wantsNews = (flags & DataFlags.News) != 0;
            bool hadNews = hadPrevious && previous!.WantsNews;
            if (wantsNews && !hadNews) session.IncrementNewsSubscriptions();
            else if (!wantsNews && hadNews) session.DecrementNewsSubscriptions();

            // Copy-on-write through the centralised helper (reentrant on _subLock).
            UpdateSubscriptionSnapshot(securityId, current =>
            {
                var next = current is not null
                    ? new Dictionary<string, SubscriptionState>(current)
                    : new Dictionary<string, SubscriptionState>();
                bool includesBookSnapshot =
                    (flags & (DataFlags.Book | DataFlags.Mbp | DataFlags.ConflatedMbp)) != 0;
                bool includesTradeHistory = (flags & DataFlags.Trades) != 0;
                next[clientId] = new SubscriptionState(
                    flags,
                    minBookBroadcastSequenceExclusive:
                        includesBookSnapshot ? bookBatchCutoffSequence : 0,
                    minTradeBroadcastSequenceExclusive:
                        includesTradeHistory ? bookBatchCutoffSequence : 0,
                    conflationIntervalMs: conflationIntervalMs);
                return next;
            });
            activation = new SubscriptionActivation(
                !hadPrevious,
                hadPrevious,
                previous,
                AddedInfoSubscription: wantsInfo && !hadInfo,
                RemovedInfoSubscription: !wantsInfo && hadInfo,
                AddedNewsSubscription: wantsNews && !hadNews,
                RemovedNewsSubscription: !wantsNews && hadNews,
                AddedSecurityDefinitionSubscription: wantsSecDef && !hadSecDef,
                RemovedSecurityDefinitionSubscription: !wantsSecDef && hadSecDef,
                AddedPriceBandSubscription: wantsPriceBand && !hadPriceBand,
                RemovedPriceBandSubscription: !wantsPriceBand && hadPriceBand,
                AddedAuctionSubscription: wantsAuction && !hadAuction,
                RemovedAuctionSubscription: !wantsAuction && hadAuction);
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
        bool RemovedNewsSubscription,
        bool AddedSecurityDefinitionSubscription,
        bool RemovedSecurityDefinitionSubscription,
        bool AddedPriceBandSubscription,
        bool RemovedPriceBandSubscription,
        bool AddedAuctionSubscription,
        bool RemovedAuctionSubscription);

    private void RollbackSubscriptionActivation(
        ClientSession session,
        string clientId,
        ulong securityId,
        SubscriptionActivation activation)
    {
        UpdateSubscriptionSnapshot(securityId, existing =>
        {
            if (existing is null) return null;
            var next = new Dictionary<string, SubscriptionState>(existing);
            if (activation.HadPrevious)
                next[clientId] = activation.PreviousState!;
            else
                next.Remove(clientId);
            return next;
        });

        if (activation.IsNew)
            session.RemoveSubscription(securityId);
        else if (activation.AddedInfoSubscription)
            session.RemoveInfoSubscription(securityId);
        else if (activation.RemovedInfoSubscription)
            session.AddInfoSubscription(securityId);

        // Mirror the news-counter delta applied during activation.
        if (activation.AddedNewsSubscription) session.DecrementNewsSubscriptions();
        else if (activation.RemovedNewsSubscription) session.IncrementNewsSubscriptions();

        // Mirror the SecurityDefinition subscription delta.
        if (activation.AddedSecurityDefinitionSubscription)
            session.RemoveSecurityDefinitionSubscription(securityId);
        else if (activation.RemovedSecurityDefinitionSubscription)
            session.AddSecurityDefinitionSubscription(securityId);

        // Mirror the PriceBand subscription delta.
        if (activation.AddedPriceBandSubscription)
            session.RemovePriceBandSubscription(securityId);
        else if (activation.RemovedPriceBandSubscription)
            session.AddPriceBandSubscription(securityId);

        // Mirror the Auction subscription delta.
        if (activation.AddedAuctionSubscription)
            session.RemoveAuctionSubscription(securityId);
        else if (activation.RemovedAuctionSubscription)
            session.AddAuctionSubscription(securityId);
    }

    /// <summary>
    /// Advance only the per-channel cutoffs covered by a Get snapshot. Lock-free:
    /// reads the lock-free outer <see cref="_subscriptions"/> and the lock-free inner
    /// dictionary snapshot, then performs a CAS-max on the mutable cutoff cell of the
    /// shared <see cref="SubscriptionState"/>. CoW snapshots share the state reference,
    /// so the new cutoff is immediately visible to broadcasters iterating any snapshot.
    /// Safe against concurrent Subscribe/Unsubscribe (worst case: a stale state object
    /// no longer reachable from the current snapshot is updated harmlessly).
    /// </summary>
    private void UpdateSnapshotCutoffsIfSubscribed(
        string clientId,
        ulong securityId,
        DataFlags snapshotFlags,
        long batchCutoffSequence)
    {
        if (!_subscriptions.TryGetValue(securityId, out var clients)) return;
        if (!clients.TryGetValue(clientId, out var state)) return;

        if ((snapshotFlags & (DataFlags.Book | DataFlags.Mbp | DataFlags.ConflatedMbp)) != 0 &&
            (state.Flags & (DataFlags.Book | DataFlags.Mbp | DataFlags.ConflatedMbp)) != 0)
        {
            state.AdvanceBookMinBroadcastSequence(batchCutoffSequence);
        }

        if ((snapshotFlags & DataFlags.Trades) != 0 && state.WantsTrades)
            state.AdvanceTradeMinBroadcastSequence(batchCutoffSequence);
    }

    private void RemoveSubscriptionCore(string clientId, ulong securityId, bool enqueueAck, bool adjustMetric)
    {
        if (!_clients.TryGetValue(clientId, out var session)) return;

        session.RemoveSubscription(securityId);

        bool removed = false;
        bool removedHadNews = false;
        UpdateSubscriptionSnapshot(securityId, existing =>
        {
            if (existing is null) return null;
            removedHadNews = existing.TryGetValue(clientId, out var prev) && prev.WantsNews;
            var next = new Dictionary<string, SubscriptionState>(existing);
            removed = next.Remove(clientId);
            return next;
        });

        if (removed)
        {
            if (adjustMetric)
                MetricsRegistry.WsSubscriptions.Add(-1);
            if (removedHadNews) session.DecrementNewsSubscriptions();
        }

        if (!enqueueAck) return;
        var buf = new byte[16];
        int len = WireProtocol.WriteUnsubscribed(buf, securityId);
        session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len));
    }

    private void HandleUnsubscribeAll(string clientId)
    {
        // Must be called under _subLock.
        // Two-phase to avoid mutating _subscriptions while iterating it: first
        // collect the set of securities the client touches, then fan out per-security
        // COW writes through the centralised helper.
        List<ulong>? affected = null;
        foreach (var (secId, clients) in _subscriptions)
        {
            if (!clients.ContainsKey(clientId)) continue;
            (affected ??= new()).Add(secId);
        }

        if (affected is null) return;

        int removedSubscriptions = 0;
        foreach (var secId in affected)
        {
            UpdateSubscriptionSnapshot(secId, existing =>
            {
                if (existing is null || !existing.TryGetValue(clientId, out var prev))
                    return existing;
                removedSubscriptions++;
                if (prev.WantsNews && _clients.TryGetValue(clientId, out var session) && session is not null)
                    session.DecrementNewsSubscriptions();
                if (existing.Count == 1) return null;
                var next = new Dictionary<string, SubscriptionState>(existing);
                next.Remove(clientId);
                return next;
            });
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
        var buf = new byte[9];
        WireProtocol.WriteServerStatus(buf, ready);
        var payload = new ReadOnlyMemory<byte>(buf);
        foreach (var (_, client) in _clients)
            client.TryEnqueue(payload);
    }
}
